"""ChimeraX + ArtiaX curation sessions (VNC over a SLURM job): submit, poll,
reconnect, and round-trip manual picks (`.coords` ↔ centered-Å star) between ArtiaX
and the pipeline.

A session's scope (which species, which tomogram) is declared at launch and written into
the scoped directory's `manifest.json` and the session's `scope.json`, so it survives a
server restart. Every `.coords` in a scoped dir becomes its own pick list; anything saved
outside one lands in the unattributed inbox for explicit assignment
(`assign_unattributed_coords`), never a guess.

The only command that drives a running session is the confirmed scope switch
(`switch_session_scope`). It re-points the viewer over the REST channel and rewrites
`scope.json` + the target manifest in the same call, so the scope on disk always matches
the viewer. It never saves for the user; the UI confirms first because `close session`
drops unsaved ArtiaX lists."""

from __future__ import annotations
import asyncio
import json
import logging
import os
import re
import shlex
import socket
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from services.computing.processes import run_process
from services.computing.slurm_service import SlurmConfig, SlurmService, avoided_nodes, check_constraint
from services.configs.config_service import get_config_service
from services.curation import napari_link
from services.event_log import events
from services.result import ErrorCode, err, ok
from services.visualization.napari_bundle import tomogram_bundle, write_bundle

logger = logging.getLogger(__name__)


def _model_id_in_probe(res: dict[str, Any], model_name: str) -> str | None:
    """The ``#…`` id of the model called ``model_name`` in an ``info models`` REST reply,
    or None. ChimeraX prints one ``#id, name, shown`` line per model into the log, which
    the JSON body carries under ``log messages`` keyed by level. A non-JSON body (server
    not in json mode) has no ``data`` and yields None too — the caller must not read that
    as "absent"."""
    log = (res.get("data") or {}).get("log messages") or {}
    text = "\n".join(str(x) for v in log.values() if isinstance(v, (list, tuple)) for x in v)
    m = re.search(r"(#[\d.]+),\s*" + re.escape(model_name) + r"\s*,", text)
    return m.group(1) if m else None


class CurationSessionService:
    """Curation-session lifecycle + REST command channel. The backend facade
    delegates its same-named curation methods here; UI code never imports this
    directly. Needs no facade — only the SLURM service, the server dir (worker
    script location), and the username (pick provenance)."""

    def __init__(self, server_dir: Path, username: str, slurm_service: SlurmService):
        self.server_dir = server_dir
        self.username = username
        self.config_service = get_config_service()
        self.slurm_service = slurm_service
        # Serializes the user-level session registry's append/prune.
        self._curation_registry_lock = asyncio.Lock()
        # Serializes the scope switch, the one thing that drives a running session, so two
        # clicks cannot interleave their `close session` chains, and the scope written to
        # disk is the scope of the chain that ran last.
        self._switch_lock = asyncio.Lock()
        # job id -> (monotonic time asked, SLURM's start estimate) for queued napari sessions.
        self._start_estimates: dict[str, tuple[float, str | None]] = {}
        # job id -> monotonic time a napari job was asked to stop (stop_napari_session).
        self._stop_requests: dict[str, float] = {}

    async def launch_curation_session(
        self, project_path: Path | None = None, cxc_path: Path | None = None, scope: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Submit a ChimeraX+ArtiaX VNC desktop as a SLURM job on `curation.partition`
        (a CPU partition will do — software GL is enough for slice-based picking).

        `cxc_path`, when given, is a crboost-generated `.cxc` (see
        services/visualization/artiax_bridge.prepare_curation_bundle) passed to the
        worker as CB_CXC so the session opens with the tomogram + picks preloaded
        instead of blank.

        `scope` is that bundle's declared identity — `{species_id, species_label,
        tomo_name, curation_dir, project_path}`. It is the only moment the session's
        meaning is fixed: it is written to `<session_dir>/scope.json` so a
        reconnecting UI — or this process after a restart — can say what the running
        viewer was launched on, and stamped into the scoped dir's `manifest.json` as
        `launched_at`, which is what tells the curation watcher that dir is hot.

        Returns the SLURM job id and the shared-FS session dir that the compute
        node writes `session.json` into; poll it with get_curation_session_info().
        See containers/chimerax_artiax/curation_session.sh.
        """
        cur = self.config_service.curation
        sif = cur.sif_path
        if not sif or not Path(sif).exists():
            return err(
                f"ChimeraX SIF not found (curation.sif_path={sif!r}). Build it under "
                "containers/chimerax_artiax/ and set curation.sif_path in conf.yaml."
            )
        if not cur.partition:
            return err("curation.partition is not set in conf.yaml.")

        worker = self._vnc_worker()
        if not worker.exists():
            return err(f"Worker script missing: {worker}")

        # Housekeeping: scancel any zombie curation jobs this project left running
        # (repeated Start clicks / crashed sessions) before launching a fresh one.
        # One session per project, and a stale one hogs a GPU + an X display/port.
        if project_path:
            try:
                stale = await self._live_project_curation_job_ids(Path(project_path))
                if stale:
                    logger.info("Curation housekeeping: scancel stale jobs %s", stale)
                    await self.slurm_service.scancel_jobs(stale)
            except Exception as e:
                logger.warning("Curation housekeeping failed (continuing): %s", e)

        login_host = cur.login_host or socket.getfqdn()

        # Session dir on shared FS visible to BOTH the compute node (writer) and
        # this headnode (reader). Keep it inside the project when we have one.
        base = (Path(project_path) / ".curation_sessions") if project_path else (Path.home() / ".crboost" / "curation")
        session_id = uuid.uuid4().hex[:8]
        session_dir = base / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        # Unique-ish X display per session → rfb port 5900+N, so concurrent sessions
        # and the user's LOCAL tunnels don't all collide on :1 / 5901. The worker
        # treats this as a PREFERENCE and falls back to a free display if it's taken
        # on the node (a zombie VNC server or another curation job there).
        display_num = 2 + (int(session_id[:6], 16) % 88)

        # GPU one-click: --gres + the VirtualGL switch mirror what `launch_curation_vnc.sh g`
        # does on the manual path — `export CX_VGL=1` flips the worker to `vglrun -d egl chimerax`
        # under apptainer --nv, so the _GL.sif renders on the GPU instead of software-GL llvmpipe.
        # Where the GPU may come from: --constraint, --prefer (tried first) and the nodes `avoid` names (sinfo).
        placement = SlurmConfig(partition=cur.partition, constraint=cur.constraint, prefer=cur.prefer, avoid=cur.avoid)
        # The worker binds /tmp and $HOME itself; the site's roots and the project come from here.
        binds = [*self.config_service.config.container_binds, *([str(project_path)] if project_path else [])]
        env = {
            "CX_SIF": str(sif),
            "CX_BINDS": ":".join(binds),
            "CX_BIN": cur.chimerax_bin,
            "CX_GEOMETRY": cur.geometry,
            "CX_DISPLAY": str(display_num),
            "CX_LOGIN_HOST": login_host,
            "CB_SESSION_DIR": str(session_dir),
        }
        if cur.vgl:
            env["CX_VGL"] = "1"
        # Opt-in only: the desktop then runs `-SecurityTypes None`, so anyone who can reach the rfb
        # port on that node drives it. Off by default; the control center states the risk.
        if cur.passwordless_vnc:
            env["CX_VNC_NOPASS"] = "1"
        if cxc_path:
            env["CB_CXC"] = str(cxc_path)
        submitted = await self._submit_vnc_job(
            session_dir=session_dir,
            job_name=self._CURATION_JOB,
            placement=placement,
            gres=cur.gres,
            qos=cur.qos,
            cpus=cur.cpus,
            mem=cur.mem,
            walltime=cur.time,
            env=env,
            conf_section="curation",
        )
        if not submitted.get("success"):
            return submitted
        slurm_job_id = submitted["slurm_job_id"]

        # Persist the SLURM id into the session dir so a fresh UI render (or a
        # different browser tab) can recover + reconnect a session it didn't
        # launch — the id otherwise lives only in the dialog's local state.
        # find_active_curation_session() reads these back.
        try:
            (session_dir / "job.json").write_text(
                json.dumps(
                    {"slurm_job_id": slurm_job_id, "session_id": session_id, "cxc": str(cxc_path) if cxc_path else None}
                )
            )
        except Exception as e:
            logger.warning("Could not persist job.json for curation session %s: %s", session_id, e)

        self._record_scope(session_dir, slurm_job_id, scope)

        # Index in the USER-level registry so the session can be found + reused across
        # projects (a curation session is a per-user viewer, not per-project).
        await self._register_curation_session(
            slurm_job_id=slurm_job_id, session_id=session_id, session_dir=session_dir, project_path=project_path
        )

        logger.info("Curation session submitted: SLURM job %s (session %s)", slurm_job_id, session_id)
        return ok(slurm_job_id=slurm_job_id, session_dir=str(session_dir), session_id=session_id)

    def _vnc_worker(self) -> Path:
        """The node-side worker a ChimeraX curation session runs."""
        return self.server_dir / "containers" / "chimerax_artiax" / "curation_session.sh"

    def _napari_worker(self) -> Path:
        """The node-side worker a napari session runs: EisGreis on the site's remote desktop."""
        return self.server_dir / "containers" / "napari" / "desktop_session.sh"

    async def _submit_vnc_job(
        self,
        *,
        session_dir: Path,
        job_name: str,
        placement: SlurmConfig,
        gres: str | None,
        qos: str,
        cpus: int,
        mem: str,
        walltime: str,
        env: dict[str, str],
        conf_section: str,
        worker: Path | None = None,
    ) -> dict[str, Any]:
        """Write `<session_dir>/submit.sh` (the SLURM lines, `env` exported, then `worker`, the curation worker by
        default) and sbatch it. `ok(slurm_job_id=...)`, or `err` naming the conf.yaml section when the placement is
        wrong."""
        try:
            await asyncio.to_thread(check_constraint, placement)
            excluded = await asyncio.to_thread(avoided_nodes, placement)
        except ValueError as e:
            return err(f"conf.yaml {conf_section}: {e}")
        log = session_dir / "slurm.log"
        # One task on one node, said outright: a site's submit filter may turn --gres=gpu into per-task GPUs
        # and then refuse a job with no task count (CBE.next does).
        lines = [
            "#!/usr/bin/env bash",
            f"#SBATCH -p {placement.partition}",
            "#SBATCH --nodes=1",
            "#SBATCH --ntasks-per-node=1",
        ]
        if gres:
            lines.append(f"#SBATCH --gres={gres}")
        if placement.constraint:
            lines.append(f'#SBATCH --constraint="{placement.constraint}"')
        if placement.prefer:
            lines.append(f'#SBATCH --prefer="{placement.prefer}"')
        if excluded:
            lines.append(f"#SBATCH --exclude={','.join(excluded)}")
        if qos:
            lines.append(f"#SBATCH --qos={qos}")
        lines += [
            f"#SBATCH --cpus-per-task={cpus}",
            f"#SBATCH --mem={mem}",
            f"#SBATCH --time={walltime}",
            f"#SBATCH -J {job_name}",
            f"#SBATCH -o {log}",
            f"#SBATCH -e {log}",
        ]
        ood = self.config_service.ood
        if ood.enabled:
            # The browser route: websockify beside Xvnc for the portal's relay, Xvnc on localhost (roadmap 55).
            env = {**env, "CB_WS_SIF": ood.websockify_sif or env["CX_SIF"]}
        lines += [f"export {k}={shlex.quote(v)}" for k, v in env.items()]
        lines.append(f"exec {shlex.quote(str(worker or self._vnc_worker()))}")
        sbatch_script = session_dir / "submit.sh"
        await asyncio.to_thread(sbatch_script.write_text, "\n".join(lines) + "\n")
        await asyncio.to_thread(sbatch_script.chmod, 0o755)

        # Strip SLURM_*/SBATCH_* so submission doesn't inherit a parent job context.
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith(("SLURM_", "SBATCH_"))}
        try:
            done = await run_process(["sbatch", str(sbatch_script)], cwd=session_dir, env=clean_env)
        except OSError as e:
            logger.exception("sbatch could not be run for %s", sbatch_script)
            return err(f"sbatch could not be run: {e}")
        if done.returncode != 0:
            return err(f"sbatch failed: {done.stderr.decode().strip()}")
        out = done.stdout.decode().strip()
        return ok(slurm_job_id=out.split()[-1] if out else None)

    def browser_url(self, info: dict[str, Any]) -> str | None:
        """The portal link that opens a live session's desktop in a browser tab, built from the current `ood:`
        config and the session's `session.json` (`node`, `ws_port`, `password`), never from a string stored when
        the session started. Everything rides in the fragment, which the browser does not send to the portal, so
        the password stays out of its logs; noVNC 1.3 reads its settings from the fragment first. None when the
        portal is not configured or the session serves no websocket (it started before the portal was set)."""
        ood = self.config_service.ood
        node, ws_port = info.get("node"), info.get("ws_port")
        if not ood.enabled or not node or not ws_port:
            return None
        params = {
            "autoconnect": "true",
            "resize": "remote",
            "path": f"rnode/{str(node).split('.')[0]}/{ws_port}/websockify",
        }
        if info.get("password"):
            params["password"] = str(info["password"])
        # A query naming the session makes the browser fetch the page from the portal instead of its cache, so an
        # expired sign-in shows the portal's sign-in page; from a cached page the websocket is refused, and noVNC
        # only says "Failed to connect to server". Redirects keep the fragment.
        session = f"{str(node).split('.')[0]}-{ws_port}"
        return f"{ood.url.rstrip('/')}{ood.novnc_path}?session={session}#{urlencode(params, safe='/')}"

    def _with_browser_url(self, info: dict[str, Any]) -> dict[str, Any]:
        url = self.browser_url(info)
        if url:
            info["browser_url"] = url
        return info

    # ── declared scope ─────────────────────────────────────────────────────────

    @staticmethod
    def _record_scope(
        session_dir: Path, slurm_job_id: str | None, scope: dict[str, Any] | None, *, how: str = "launch"
    ) -> None:
        """Persist the session's scope beside it (`scope.json`), and stamp it into the
        scoped dir's manifest. Best-effort: a session whose scope could not be written
        still runs — its saves are attributed by the manifest the bundle already wrote,
        and worst case they reach the unattributed inbox. Never blocks a launch.

        `how` is `launch` or `switch`, recorded as `scope_set_by`. `launched_at` means
        "became this session's scope at": it is the watcher's hot-dir key (`watcher.py`,
        the newest `launched_at` across manifests is rescanned every tick), so a switch
        must move it to the new dir."""
        if not scope:
            return
        from services.visualization import artiax_bridge

        payload = {
            **scope,
            "slurm_job_id": slurm_job_id,
            "launched_at": datetime.now().isoformat(timespec="seconds"),
            "scope_set_by": how,
        }
        try:
            (Path(session_dir) / "scope.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        except OSError as e:
            logger.warning("Could not persist scope.json for curation session %s: %s", session_dir, e)
        cur_dir = scope.get("curation_dir")
        if not cur_dir:
            return
        try:
            artiax_bridge.write_manifest(
                Path(cur_dir),
                launched_at=payload["launched_at"],
                slurm_job_id=slurm_job_id,
                session_dir=str(session_dir),
                scope_set_by=how,
            )
        except OSError as e:
            logger.warning("Could not stamp %s onto the manifest in %s: %s", how, cur_dir, e)

    async def switch_session_scope(
        self,
        session_info: dict[str, Any],
        *,
        open_recon: str | None,
        auto_coords: str | None,
        seed_coords: str | None,
        scope: dict[str, Any],
        features: list[dict] | None = None,
    ) -> dict[str, Any]:
        """Re-point a live session at another (species, tomogram). `session_info` is what
        `find_active_curation_session*` returned (`node`, `rest_port`, `session_dir`,
        `slurm_job_id`); the three paths and the segmentation volumes (`features`) are the target
        bundle's; `scope` is `curation_scope(...)` for the target.

        Order matters: (1) the swap chain over REST; failure here means ArtiaX still has
        the old tomogram and the scope on disk stays the old one;
        (2) `cd` as its own call, best-effort (`cd_error`); (3) the scope written to
        `scope.json` and the target manifest via `_record_scope(how="switch")`, so the
        watcher's hot dir and every reconnecting UI follow the viewer. Never saves for the
        user — the UI confirms first. `ok(switched, scope_recorded, cd_error, commands)` /
        `err(...)`; a scope that could not be recorded is `ok(scope_recorded=False,
        warning=…)`, loud, never silent.
        """
        from services.visualization import artiax_bridge

        if not self.config_service.curation.rest_enabled:
            return err("curation.rest_enabled is off — use Restart on this tomogram")
        if not open_recon or not seed_coords:
            return err("the target bundle has no recon / seed to open — prepare it again (Curate picks)")
        chain = artiax_bridge.swap_chimerax_commands(
            open_recon, Path(auto_coords) if auto_coords else None, Path(seed_coords), features or ()
        )
        cd_cmd = artiax_bridge.cd_chimerax_command(scope["curation_dir"])
        seed_name = Path(seed_coords).name
        async with self._switch_lock:
            res = await self.send_chimerax_command(session_info, chain, timeout=120)
            if not res.get("success"):
                return err(f"ArtiaX did not switch: {res.get('error')}", raw=res.get("raw"))
            # A chain that returned ok is not proof the seed is open and selected. `info models` lists
            # `#id, name, shown` per model; the seed's model is named after its file. Absent → the switch
            # failed, whatever the chain said.
            probe = await self.send_chimerax_command(session_info, "info models", timeout=15)
            seed_model_id = _model_id_in_probe(probe, seed_name)
            if seed_model_id is None and probe.get("success") and probe.get("data") is not None:
                return err(
                    f"ArtiaX switched the tomogram but did not open {seed_name} — use Restart on this tomogram",
                    raw=probe.get("raw"),
                )
            cd_res = await self.send_chimerax_command(session_info, cd_cmd, timeout=15)
            cd_error = None if cd_res.get("success") else str(cd_res.get("error"))
            # Point ChimeraX's save dialog at the new folder: Qt opens every fresh dialog in
            # its LAST-visited dir and the cwd only seeds the first one of the process, so
            # `cd` alone leaves the dialog in the previous scope's folder. A hidden
            # QFileDialog.setDirectory() from the runscript moves it. Own call, best-effort.
            script = artiax_bridge.write_save_dir_script(Path(scope["curation_dir"]))
            rs_res = await self.send_chimerax_command(
                session_info, artiax_bridge.runscript_chimerax_command(script), timeout=15
            )
            save_dir_error = None if rs_res.get("success") else str(rs_res.get("error"))
            # The dialog-proof save path regardless: `save <seed> partlist #id`, for the user
            # to run in ChimeraX's command line. crboost never sends it.
            save_command = artiax_bridge.save_chimerax_command(seed_coords, seed_model_id) if seed_model_id else None
            sdir = session_info.get("session_dir")
            if not sdir:
                logger.warning("Switched ArtiaX to %s but the session has no session_dir — scope NOT recorded", scope)
                return ok(
                    switched=True,
                    scope_recorded=False,
                    cd_error=cd_error,
                    save_dir_error=save_dir_error,
                    seed_model_id=seed_model_id,
                    save_command=save_command,
                    commands=[chain, cd_cmd],
                    warning="ArtiaX switched, but this session has no session dir — its scope on disk was not "
                    "updated, so the watcher and a reconnect still name the previous tomogram",
                )
            self._record_scope(Path(sdir), session_info.get("slurm_job_id"), scope, how="switch")
        logger.info(
            "Curation session switched to %s/%s (seed model %s)",
            scope.get("species_id"),
            scope.get("tomo_name"),
            seed_model_id or "unverified",
        )
        return ok(
            switched=True,
            scope_recorded=True,
            cd_error=cd_error,
            save_dir_error=save_dir_error,
            seed_model_id=seed_model_id,
            save_command=save_command,
            commands=[chain, cd_cmd],
        )

    @staticmethod
    def _read_scope(session_dir: Path) -> dict[str, Any]:
        """``{"scope": {...}}`` for a session dir, or ``{}``. Merged into what the two
        find_active_* methods return, so every consumer of a live session also learns what
        it was launched on, including after a crboost restart."""
        try:
            data = json.loads((Path(session_dir) / "scope.json").read_text())
        except (OSError, ValueError):
            return {}
        return {"scope": data} if isinstance(data, dict) else {}

    @classmethod
    def _session_files(cls, sdir: Path) -> dict[str, Any]:
        """What a live session's dir says of it: its session.json, once the worker has published it, and its scope.
        Called in a thread: session dirs live on NFS, where a read can take seconds."""
        out: dict[str, Any] = {}
        info_file = sdir / "session.json"
        if info_file.exists():
            try:
                out.update(json.loads(info_file.read_text()))
            except (OSError, ValueError):
                pass  # the worker is still writing it; the next poll reads it whole
        out.update(cls._read_scope(sdir))
        return out

    @staticmethod
    def _read_session_json(sdir: Path) -> dict[str, Any] | None:
        """A session dir's session.json, None while the worker has not published it. Called in a thread (NFS)."""
        info_file = sdir / "session.json"
        return json.loads(info_file.read_text()) if info_file.exists() else None

    @staticmethod
    def _read_log_tail(log_file: Path) -> str | None:
        """The last 1200 characters of a session's slurm.log, None when the job has not written one. In a thread."""
        return log_file.read_text()[-1200:].strip() if log_file.exists() else None

    async def get_curation_session_info(self, session_dir: str, slurm_job_id: str | None = None) -> dict[str, Any]:
        """Poll a launched curation session. Once the job is RUNNING and has
        published session.json, returns its connection info (node/port/password/
        tunnel_cmd); otherwise reports pending/starting/ended."""
        sdir = Path(session_dir)
        try:
            data = await asyncio.to_thread(self._read_session_json, sdir)
        except (OSError, ValueError) as e:
            return ok(status="starting", detail=f"session.json not readable yet: {e}")
        if data is not None:
            data.update({"success": True, "status": "ready"})
            return self._with_browser_url(data)

        # No session.json yet — ask SLURM why. query_jobs_by_ids distinguishes the two
        # cases get_user_jobs conflates (get_user_jobs returns [] on squeue failure, which
        # is indistinguishable from "no jobs"): None = squeue itself failed (a transient, not
        # "job gone"); {} = squeue is healthy and the job genuinely left.
        state = reason = None
        squeue_ok = True
        if slurm_job_id:
            q = await self.slurm_service.query_jobs_by_ids([str(slurm_job_id)])
            if q is None:
                squeue_ok = False
            else:
                hit = q.get(str(slurm_job_id).split("_", 1)[0]) or q.get(str(slurm_job_id))
                state, reason = hit if hit else (None, None)

        if state in ("PENDING", "CONFIGURING", "SCHEDULED"):
            return ok(status="pending", slurm_state=state, slurm_reason=reason)
        if state is not None:
            # RUNNING (or similar) but session.json not visible yet — just started / NFS lag.
            return ok(status="starting", slurm_state=state)
        if not squeue_ok:
            # Couldn't reach the scheduler this tick; keep the spinner up rather than
            # report "ended" for a job that may well be alive. The next 3 s poll re-checks.
            return ok(status="pending", slurm_state="scheduler unreachable — retrying")

        # squeue is healthy and the job is genuinely gone. Only "ended" if the job actually
        # ran (its log exists); otherwise this is the brief post-sbatch window before it
        # registers, so report pending. If it ran but wrote nothing (0-byte log), it died in
        # the node-side container/VNC bring-up before logging; say so, so the user retries.
        tail = await asyncio.to_thread(self._read_log_tail, sdir / "slurm.log")
        if tail is not None:
            return ok(
                status="ended",
                detail=tail
                or (
                    "The session process exited immediately without writing any log — usually a transient "
                    "node-side container/VNC startup failure. Press Try again; it typically lands on a healthy node."
                ),
            )
        return ok(status="pending", slurm_state="submitting")

    async def stop_curation_session(self, slurm_job_id: str | None, project_path: Path | None = None) -> dict[str, Any]:
        """scancel a curation session's SLURM job. With `project_path`, ALSO scancel
        every other live cb-curation job recorded for that project — so one Stop
        clears the whole zombie pile (the user may have started several), and the
        next entry sees a clean 'no session' instead of reconnecting to a wedged one."""
        ids: list[str] = [str(slurm_job_id)] if slurm_job_id else []
        if project_path:
            try:
                ids.extend(await self._live_project_curation_job_ids(Path(project_path)))
            except Exception as e:
                logger.warning("stop_curation_session: project sweep failed: %s", e)
        ids = sorted(set(ids))
        if not ids:
            return err("no SLURM job id")
        return await self.slurm_service.scancel_jobs(ids)

    @staticmethod
    def _recorded_project_sessions(project_path: Path) -> list[tuple[str, Path]]:
        """(SLURM job id, session dir) of each session recorded under the project's `.curation_sessions/*/job.json`,
        in name order; a job.json that cannot be read is left out. Called in a thread: the project is on NFS."""
        base = project_path / ".curation_sessions"
        if not base.is_dir():
            return []
        recorded: list[tuple[str, Path]] = []
        for job_file in sorted(base.glob("*/job.json")):
            try:
                jid = json.loads(job_file.read_text()).get("slurm_job_id")
            except (OSError, ValueError, AttributeError):
                continue  # half-written or not a session's record: it names no job to look for
            if jid:
                recorded.append((str(jid), job_file.parent))
        return recorded

    async def _live_project_curation_job_ids(self, project_path: Path) -> list[str]:
        """Live cb-curation SLURM job ids recorded under this project's
        `.curation_sessions/*/job.json` (squeue-derived liveness, never a stored
        bool). Shared by launch + stop housekeeping to clear zombie sessions."""
        recorded = {jid for jid, _sdir in await asyncio.to_thread(self._recorded_project_sessions, Path(project_path))}
        if not recorded:
            return []
        jobs = await self.slurm_service.get_user_jobs(force_refresh=True)
        live = {j.job_id for j in jobs if j.name == self._CURATION_JOB and j.state in self._CURATION_LIVE_STATES}
        live_bases = {x.split("_", 1)[0] for x in live}
        return sorted({jid for jid in recorded if jid in live or jid.split("_", 1)[0] in live_bases})

    # Curation jobs are submitted with `-J cb-curation`, napari sessions with `-J cb-napari`; a
    # session is "live" iff its SLURM job is in one of these states (squeue is the source of
    # truth, never a cached is-running flag).
    _CURATION_JOB = "cb-curation"
    _NAPARI_JOB = "cb-napari"
    _CURATION_LIVE_STATES = ("RUNNING", "PENDING", "CONFIGURING", "SCHEDULED", "COMPLETING")
    # How long a napari job asked to stop counts as gone while it closes its viewer. One that still runs after this is
    # shown again, and a Stop then cancels it outright.
    _STOP_GRACE_S = 60.0

    async def find_active_curation_session(self, project_path: Path) -> dict[str, Any] | None:
        """Return the project's one live curation session, or None.

        Scans `<project>/.curation_sessions/*/job.json`, then makes a single
        squeue call and returns the first session whose SLURM job is still live —
        merged with its `session.json` connection details when already published.
        Liveness is derived from squeue, never a stored boolean.
        """
        recorded = await asyncio.to_thread(self._recorded_project_sessions, Path(project_path))
        if not recorded:
            return None

        jobs = await self.slurm_service.get_user_jobs(force_refresh=True)
        live = {j.job_id: j for j in jobs if j.name == self._CURATION_JOB and j.state in self._CURATION_LIVE_STATES}
        for jid, sdir in recorded:
            match = live.get(jid) or next(
                (j for j in live.values() if j.job_id.split("_", 1)[0] == jid.split("_", 1)[0]), None
            )
            if match is None:
                continue
            out: dict[str, Any] = {
                "session_dir": str(sdir),
                "slurm_job_id": match.job_id,
                "slurm_state": match.state,
                "node": match.nodelist or None,
            }
            out.update(await asyncio.to_thread(self._session_files, sdir))
            return self._with_browser_url(out)
        return None

    # ── per-user session registry + REST command channel ───────────────────────
    #
    # A curation session is one ChimeraX/ArtiaX VNC viewer per USER, reused across
    # tomograms / species / PROJECTS — the session is just a viewer, every file we
    # send it is an absolute path on the shared FS. The worker starts a REST server
    # on the node's loopback (`remotecontrol rest start port <rest_port> json true`)
    # and records `rest_port` in session.json; crboost drives it by ssh-hopping to the
    # node and curling localhost (the REST bind is 127.0.0.1, no auth — never routable).

    def _curation_registry_file(self) -> Path:
        return Path.home() / ".crboost" / "curation" / "registry.jsonl"

    async def _register_curation_session(
        self,
        *,
        slurm_job_id: str | None,
        session_id: str,
        session_dir: Path,
        project_path: Path | None,
        registry: Path | None = None,
    ) -> None:
        """Append a launched session to the user-level registry (`registry`, the curation one
        by default) so find-or-reuse can locate it from ANY project. Best-effort — never blocks
        a launch. Lock-guarded against the prune-rewrite in _find_live_registered so a
        concurrent prune can't drop this just-appended line."""
        try:
            reg = registry or self._curation_registry_file()
            reg.parent.mkdir(parents=True, exist_ok=True)
            entry = {
                "slurm_job_id": slurm_job_id,
                "session_id": session_id,
                "session_dir": str(session_dir),
                "project_path": str(project_path) if project_path else None,
            }
            async with self._curation_registry_lock:
                with reg.open("a") as f:
                    f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.warning("Could not register curation session %s: %s", session_id, e)

    async def find_active_curation_session_any(self) -> dict[str, Any] | None:
        """The user's one live curation session across ALL projects (the per-user
        reuse model), or None. See _find_live_registered."""
        return await self._find_live_registered(self._curation_registry_file(), self._CURATION_JOB)

    async def _find_live_registered(self, reg: Path, job_name: str) -> dict[str, Any] | None:
        """The newest live session in the user-level registry `reg` whose SLURM job is
        `job_name`, or None. Reads the registry, makes a single squeue call, returns the
        session merged with its session.json, and self-prunes dead/missing entries.
        Liveness is squeue-derived."""
        try:
            entries = await asyncio.to_thread(self._registered_sessions, reg)
        except OSError:
            # An unreadable registry names no session; the next poll reads it again.
            return None
        if not entries:
            return None

        jobs = await self.slurm_service.get_user_jobs(force_refresh=True)
        live = {j.job_id: j for j in jobs if j.name == job_name and j.state in self._CURATION_LIVE_STATES}
        live_bases = {jid.split("_", 1)[0]: j for jid, j in live.items()}

        # Pick the NEWEST live session: the registry is append-ordered (oldest→newest),
        # so the last live match is the one the user most recently started — reusing an
        # older session from another project would swap/close the wrong viewer.
        chosen = None
        dead_dirs = set()
        for d in entries:
            jid = str(d["slurm_job_id"])
            match = live.get(jid) or live_bases.get(jid.split("_", 1)[0])
            if match is None:
                dead_dirs.add(d.get("session_dir"))
                continue
            chosen = (d, match)

        # Prune dead entries, but only when squeue actually returned data. An empty
        # result is indistinguishable from a squeue failure (get_user_jobs returns []
        # on error without caching it), and pruning then would wipe still-live sessions
        # from the store the cross-project reuse path reads. Re-read fresh under the
        # lock so a concurrent launch's append isn't lost to a stale rewrite, and drop
        # only entries confirmed dead.
        if jobs and dead_dirs:
            async with self._curation_registry_lock:
                try:
                    await asyncio.to_thread(self._prune_registry, reg, dead_dirs)
                except OSError:
                    # Pruning only tidies: the dead entries are skipped on every read until a later prune lands.
                    logger.warning("Could not prune dead sessions from %s", reg, exc_info=True)
        if chosen is None:
            return None

        d, match = chosen
        sdir = Path(d["session_dir"])
        out: dict[str, Any] = {
            "session_dir": str(sdir),
            "slurm_job_id": match.job_id,
            "slurm_state": match.state,
            "node": match.nodelist or None,
        }
        out.update(await asyncio.to_thread(self._session_files, sdir))
        return self._with_browser_url(out)

    @staticmethod
    def _registry_lines(reg: Path) -> list[dict[str, Any]]:
        """Each entry of a user-level session registry (one JSON object a line), unreadable lines left out."""
        entries: list[dict[str, Any]] = []
        for ln in reg.read_text().splitlines():
            ln = ln.strip()
            if not ln:
                continue
            try:
                d = json.loads(ln)
            except ValueError:
                continue  # a line cut short by a crash mid-append: it names no session
            if isinstance(d, dict):
                entries.append(d)
        return entries

    @classmethod
    def _registered_sessions(cls, reg: Path) -> list[dict[str, Any]]:
        """The registry's entries whose session dir is still there, oldest first. Called in a thread: ~/.crboost is
        on NFS, where these reads took the event loop for seconds."""
        if not reg.is_file():
            return []
        return [
            d
            for d in cls._registry_lines(reg)
            if d.get("slurm_job_id") and d.get("session_dir") and Path(d["session_dir"]).is_dir()
        ]

    @classmethod
    def _prune_registry(cls, reg: Path, dead_dirs: set[Any]) -> None:
        """Rewrite the registry without the entries of `dead_dirs`, read afresh so a launch's append made meanwhile
        stays. Called in a thread, under the registry lock."""
        fresh = [json.dumps(d) for d in cls._registry_lines(reg) if d.get("session_dir") not in dead_dirs]
        reg.write_text("".join(s + "\n" for s in fresh))

    # ── napari sessions ───────────────────────────────────────────────────────
    #
    # A user's napari is EisGreis (conf.yaml napari.eisgreis). It runs in the user's own remote desktop, where the
    # viewer registered itself (a registry line of kind "desktop"), or as a job of ours: the site's remote desktop
    # started by containers/napari/desktop_session.sh, `-J cb-napari`, registered here when submitted. Either way
    # crboost writes what to show into the session dir's `open.json` and the viewer answers in `opened.json`; the
    # viewer's `desktop.json` says which protocol versions it reads (services/curation/napari_link.py).

    _NAPARI_REQUEST = "open.json"
    _NAPARI_ACK = "opened.json"
    _NAPARI_BUNDLE = "bundle.json"

    @staticmethod
    def _napari_base() -> Path:
        return Path.home() / ".crboost" / "napari"

    @staticmethod
    def _napari_outputs(volume: str) -> Path | None:
        """Where the viewer saves for `volume`: `<project>/Curation/napari/` of the crboost project the volume lies in
        (the nearest directory above it holding `project_params.json`), created here. None for a volume outside every
        project; the viewer then saves into its session dir's `outputs/` and says so."""
        for parent in Path(volume).parents:
            if (parent / "project_params.json").is_file():
                out = parent / "Curation" / "napari"
                out.mkdir(parents=True, exist_ok=True)
                return out
        return None

    @classmethod
    def _write_napari_request(
        cls,
        session_dir: Path,
        volumes: list[dict[str, str]],
        title: str,
        bundle: dict | None = None,
        protocol: int = napari_link.PROTOCOLS[-1],
    ) -> int:
        """Write `open.json` (renamed into place, so the viewer never reads half of it) in `protocol`, a version the
        viewer reads, and return its `seq`: a clock in ms, later than any request before it, a server restart
        included, which is all the viewer compares. `outputs` is the first volume's project folder for the viewer's
        saves; `bundle`, the tomogram's picks and segmentations, goes to `bundle.json` first, which the request
        names. EisGreis's docs/crboost-protocol.md is the format's reference."""
        seq = time.time_ns() // 1_000_000
        outputs = cls._napari_outputs(volumes[0]["path"]) if volumes else None
        bundle_path = None
        if bundle is not None:
            bundle_path = session_dir / cls._NAPARI_BUNDLE
            write_bundle(bundle, bundle_path)
        body = {
            "protocol": protocol,
            "seq": seq,
            "title": title,
            "layers": [{"name": v["name"], "path": v["path"]} for v in volumes],
            "outputs": str(outputs) if outputs else None,
            "bundle": str(bundle_path) if bundle_path else None,
        }
        tmp = session_dir / (cls._NAPARI_REQUEST + ".tmp")
        tmp.write_text(json.dumps(body))
        os.replace(tmp, session_dir / cls._NAPARI_REQUEST)
        return seq

    @classmethod
    def _read_napari_state(cls, session_dir: Path) -> dict[str, Any]:
        """`{"requested": open.json, "opened": opened.json}`, each None when absent. The viewer writes
        `opened.json` by rename too, so a file that does not parse is a fault on one side, and says so."""
        out: dict[str, Any] = {}
        for key, name in (("requested", cls._NAPARI_REQUEST), ("opened", cls._NAPARI_ACK)):
            try:
                out[key] = json.loads((session_dir / name).read_text())
            except FileNotFoundError:
                out[key] = None
            except (OSError, ValueError) as e:
                out[key] = {"ok": False, "error": f"{name} unreadable: {e}"}
        return out

    @staticmethod
    def _missing_volumes(volumes: list[dict[str, str]]) -> list[str]:
        return [v["path"] for v in volumes if not Path(v["path"]).is_file()]

    def _eisgreis_install(self) -> dict[str, Any]:
        """`ok(install=…)` when napari.eisgreis names an install with its command, else `err` saying what to do.
        Blocking (a stat)."""
        install = self.config_service.napari.eisgreis
        if not install:
            return err(
                "napari.eisgreis is not set in conf.yaml: install EisGreis (./crboost eisgreis-install prints the "
                "line) and name the install there."
            )
        if not os.access(napari_link.eisgreis_command(install), os.X_OK):
            return err(
                f"napari.eisgreis is {install}, which has no {napari_link.COMMAND}: point it at an EisGreis install."
            )
        return ok(install=str(Path(install).expanduser()))

    async def _napari_protocol(self, session_dir: Path | None) -> dict[str, Any]:
        """`ok(protocol=…)`, the version to write into `session_dir`: the newest the viewer running there reads (its
        `desktop.json`), else the newest the configured install reads; `err` naming the side to update when the two
        share none."""
        desktop = await asyncio.to_thread(napari_link.read_desktop, session_dir) if session_dir is not None else None
        if desktop is not None and desktop.get("protocol"):
            return napari_link.choose_protocol(desktop["protocol"], eisgreis=desktop.get("eisgreis"), install=None)
        install = self.config_service.napari.eisgreis
        versions = await asyncio.to_thread(napari_link.install_versions, install)
        if not versions.get("success"):
            return versions
        return napari_link.choose_protocol(versions["protocol"], eisgreis=versions["eisgreis"], install=install)

    async def launch_napari_session(
        self, volumes: list[dict[str, str]], title: str, *, software: bool = False
    ) -> dict[str, Any]:
        """Submit EisGreis on the site's remote desktop (napari.sif_path, the image its Open OnDemand desktop runs)
        showing `volumes` (`[{"name", "path"}]`, absolute paths). On a full GPU (VirtualGL) the placement is
        `napari:`'s where set, else `curation:`'s; `software` runs it with software rendering where `napari.software`
        says, which starts at once. Returns the SLURM job id, the session dir (in
        ~/.crboost/napari) and the request's `seq`; poll with get_napari_session_info()."""
        cfg = self.config_service.napari
        cur = self.config_service.curation
        sw = cfg.software
        installed = await asyncio.to_thread(self._eisgreis_install)
        if not installed.get("success"):
            return installed
        if software and not sw.partition:
            return err("napari.software.partition is not set in conf.yaml, so napari runs on a full GPU only.")
        sif = cfg.sif_path
        if not sif or not Path(sif).exists():
            return err(
                f"napari image not found (napari.sif_path={sif!r}). Build container_defs/napari_GL.def and set "
                "napari.sif_path in conf.yaml."
            )
        partition = sw.partition if software else (cfg.partition if cfg.partition is not None else cur.partition)
        if not partition:
            return err("Neither napari.partition nor curation.partition is set in conf.yaml.")
        if not self._napari_worker().exists():
            return err(f"Missing from the server's checkout: {self._napari_worker()}")
        if not volumes:
            return err("Nothing to open.")
        missing = await asyncio.to_thread(self._missing_volumes, volumes)
        if missing:
            return err(f"Not on disk: {', '.join(missing)}")
        version = await self._napari_protocol(None)
        if not version.get("success"):
            return version

        session_id = uuid.uuid4().hex[:8]
        session_dir = self._napari_base() / session_id
        await asyncio.to_thread(session_dir.mkdir, parents=True, exist_ok=True)
        bundle = await self._napari_bundle(volumes[0]["path"])
        seq = await asyncio.to_thread(
            self._write_napari_request, session_dir, volumes, title, bundle, version["protocol"]
        )

        if software:
            placement = SlurmConfig(partition=partition)
            gres = sw.gres
            qos = sw.qos if sw.qos is not None else cur.qos
            cpus = sw.cpus
        else:
            placement = SlurmConfig(
                partition=partition,
                constraint=cfg.constraint if cfg.constraint is not None else cur.constraint,
                prefer=cfg.prefer if cfg.prefer is not None else cur.prefer,
                avoid=cfg.avoid if cfg.avoid is not None else cur.avoid,
            )
            gres = cfg.gres if cfg.gres is not None else cur.gres
            qos = cfg.qos if cfg.qos is not None else cur.qos
            cpus = cfg.cpus
        env = {
            "CX_SIF": str(sif),
            # The worker binds the session folder and the EisGreis install itself; volumes outside the site's roots
            # are not visible in the session, and the viewer reports them as unreadable.
            "CX_BINDS": ":".join(self.config_service.config.container_binds),
            "CX_GEOMETRY": cfg.geometry,
            "CX_LOGIN_HOST": cur.login_host or socket.getfqdn(),
            "CB_SESSION_DIR": str(session_dir),
            "CB_EISGREIS": installed["install"],
        }
        if cur.vgl and not software:
            env["CX_VGL"] = "1"
        if cur.passwordless_vnc:
            env["CX_VNC_NOPASS"] = "1"
        submitted = await self._submit_vnc_job(
            session_dir=session_dir,
            job_name=self._NAPARI_JOB,
            placement=placement,
            gres=gres,
            qos=qos,
            cpus=cpus,
            mem=cfg.mem,
            walltime=cfg.time,
            env=env,
            conf_section="napari (placement from curation)",
            worker=self._napari_worker(),
        )
        if not submitted.get("success"):
            return submitted
        slurm_job_id = submitted["slurm_job_id"]
        where = "software" if software else "gpu"
        await asyncio.to_thread(
            (session_dir / "job.json").write_text,
            json.dumps({"slurm_job_id": slurm_job_id, "session_id": session_id, "where": where}),
        )
        await self._register_curation_session(
            slurm_job_id=slurm_job_id,
            session_id=session_id,
            session_dir=session_dir,
            project_path=None,
            registry=self._napari_base() / "registry.jsonl",
        )
        logger.info(
            "napari session submitted on %s: SLURM job %s (session %s), %s", where, slurm_job_id, session_id, title
        )
        return ok(slurm_job_id=slurm_job_id, session_dir=str(session_dir), session_id=session_id, seq=seq, where=where)

    async def open_in_napari_session(
        self, session_dir: str, volumes: list[dict[str, str]], title: str
    ) -> dict[str, Any]:
        """Ask a running viewer (in the user's desktop or on a node of ours) to show `volumes` instead of what it
        shows now. The answer arrives in get_napari_session_info()'s `opened` once the window has it (within about a
        second; a viewer waiting with no window opens one first)."""
        if not volumes:
            return err("Nothing to open.")
        missing = await asyncio.to_thread(self._missing_volumes, volumes)
        if missing:
            return err(f"Not on disk: {', '.join(missing)}")
        sdir = Path(session_dir)
        if not await asyncio.to_thread(sdir.is_dir):
            return err(f"The napari session's directory is gone: {sdir}")
        version = await self._napari_protocol(sdir)
        if not version.get("success"):
            return version
        bundle = await self._napari_bundle(volumes[0]["path"])
        seq = await asyncio.to_thread(self._write_napari_request, sdir, volumes, title, bundle, version["protocol"])
        return ok(seq=seq)

    async def _napari_bundle(self, volume: str) -> dict | None:
        """The picks and segmentations crboost holds for the tomogram at `volume`; None for a volume of no project,
        or when they cannot be read: the tomogram opens regardless, and the bundle's absence says why in the log."""
        try:
            return await tomogram_bundle(volume)
        except (OSError, ValueError, KeyError) as e:
            logger.exception("napari bundle for %s not built", volume)
            events.warning("napari: picks and segmentations of %s not shown: %s", Path(volume).name, e)
            return None

    @staticmethod
    def _desktop_session_info(session_dir: Path) -> dict[str, Any]:
        """A viewer in the user's own desktop: ready while its heartbeat is fresh, ended once it stopped. Blocking."""
        desktop = napari_link.read_desktop(session_dir)
        if desktop is None or not napari_link.is_alive(session_dir):
            return ok(
                status="ended",
                route="desktop",
                detail="EisGreis in your desktop stopped: its window was closed, or the desktop ended.",
            )
        return ok(
            status="ready",
            route="desktop",
            node=desktop.get("node"),
            display=desktop.get("display"),
            slurm_job_id=desktop.get("slurm_job_id"),
            desktop=desktop,
        )

    async def get_napari_session_info(self, session_dir: str, slurm_job_id: str | None = None) -> dict[str, Any]:
        """Where a session stands, plus what was asked for (`requested`) and what the window shows (`opened`). A job
        of ours is get_curation_session_info(), and while it is queued SLURM's estimate of its start
        (`expected_start`): a GPU session can wait hours when the cards are taken, and the user should see that. A
        viewer in the user's own desktop (a session dir crboost did not submit, so with no `job.json`) is ready
        while its heartbeat is fresh."""
        sdir = Path(session_dir)
        if not await asyncio.to_thread((sdir / "job.json").is_file):
            info = await asyncio.to_thread(self._desktop_session_info, sdir)
        else:
            info = await self.get_curation_session_info(session_dir, slurm_job_id)
            info["route"] = "job"
            if info.get("status") == "pending" and slurm_job_id:
                info["expected_start"] = await self._expected_start(str(slurm_job_id))
        info.update(await asyncio.to_thread(self._read_napari_state, sdir))
        return info

    async def _expected_start(self, slurm_job_id: str) -> str | None:
        """`squeue --start` for one job, asked at most once a minute per job (the dialog polls every 3 s)."""
        cached = self._start_estimates.get(slurm_job_id)
        now = time.monotonic()
        if cached is None or now - cached[0] > 60:
            cached = (now, await self.slurm_service.expected_start(slurm_job_id))
            self._start_estimates[slurm_job_id] = cached
        return cached[1]

    @staticmethod
    def _registry_entries(reg: Path) -> list[dict[str, Any]]:
        """The registry's lines whose session dir still exists, oldest first. Blocking."""
        if not reg.is_file():
            return []
        entries = []
        for ln in reg.read_text().splitlines():
            try:
                d = json.loads(ln) if ln.strip() else None
            except ValueError:  # a line torn by a crash: the others still count
                continue
            if isinstance(d, dict) and d.get("session_dir") and Path(d["session_dir"]).is_dir():
                entries.append(d)
        return entries

    async def find_active_napari_session(self) -> dict[str, Any] | None:
        """The user's live napari, or None; the newest one started when several run. A job of ours is live while its
        `cb-napari` job is, unless it was asked to stop less than `_STOP_GRACE_S` ago (it is closing its viewer); a
        viewer in the user's desktop (registry kind "desktop") while its SLURM job runs (any name: the portal's
        desktop job) and its heartbeat is fresh. The answer carries `route` ("job" or "desktop"),
        and `requested` and `opened` when the session has them. Dead lines are pruned when squeue answered."""
        reg = self._napari_base() / "registry.jsonl"
        entries = await asyncio.to_thread(self._registry_entries, reg)
        if not entries:
            return None
        jobs = await self.slurm_service.get_user_jobs(force_refresh=True)
        live = {j.job_id.split("_", 1)[0]: j for j in jobs if j.state in self._CURATION_LIVE_STATES}
        chosen = None
        dead: set[str] = set()
        for d in entries:
            sdir = Path(d["session_dir"])
            jid = str(d.get("slurm_job_id") or "").split("_", 1)[0]
            job = live.get(jid) if jid else None
            if d.get("kind") == "desktop":
                alive = await asyncio.to_thread(napari_link.is_alive, sdir)
                if alive and (job is not None or not jid):
                    chosen = (d, job, "desktop")
                elif (jid and job is None) or not await asyncio.to_thread((sdir / napari_link.DESKTOP).exists):
                    dead.add(d["session_dir"])
            elif job is not None and job.name == self._NAPARI_JOB:
                if not self._stopping(jid):
                    chosen = (d, job, "job")
            else:
                dead.add(d["session_dir"])

        # Prune only when squeue answered: an empty answer is indistinguishable from a failed one, and pruning then
        # would drop sessions that still run. Re-read under the lock so a concurrent launch's line is kept.
        if jobs and dead:
            try:
                async with self._curation_registry_lock:
                    kept = [
                        json.dumps(e)
                        for e in await asyncio.to_thread(self._registry_entries, reg)
                        if e.get("session_dir") not in dead
                    ]
                    await asyncio.to_thread(reg.write_text, "".join(k + "\n" for k in kept))
            except OSError:
                logger.exception("could not prune %s", reg)
        if chosen is None:
            return None
        d, job, route = chosen
        sdir = Path(d["session_dir"])
        if route == "desktop":
            out = await asyncio.to_thread(self._desktop_session_info, sdir)
            out.pop("success", None)
            out["session_dir"] = str(sdir)
            out["slurm_state"] = job.state if job is not None else None
        else:
            out = {
                "session_dir": str(sdir),
                "slurm_job_id": job.job_id,
                "slurm_state": job.state,
                "node": job.nodelist or None,
                "route": "job",
            }
            info_file = sdir / "session.json"
            try:
                out.update(json.loads(await asyncio.to_thread(info_file.read_text)))
            except FileNotFoundError:  # the desktop is not up yet
                pass
            except ValueError as e:  # written by the worker with a heredoc: torn while it writes
                logger.warning("%s not readable yet: %s", info_file, e)
            out = self._with_browser_url(out)
        out.update(await asyncio.to_thread(self._read_napari_state, sdir))
        return out

    def _stopping(self, job_id: str) -> bool:
        """The napari job was asked to stop less than `_STOP_GRACE_S` ago: it is closing its viewer, and ends."""
        asked = self._stop_requests.get(job_id)
        return asked is not None and time.monotonic() - asked < self._STOP_GRACE_S

    async def stop_napari_session(self, slurm_job_id: str | None) -> dict[str, Any]:
        """Stop a napari job of ours. A running one is asked: its worker alone gets SIGTERM and closes the viewer
        while the desktop is still up, which saves the comments and particle STARs, then ends the job; meanwhile the
        session counts as gone (`_stopping`). A pending job, or one that still runs `_STOP_GRACE_S` after it was
        asked, is cancelled outright. A viewer in the user's own desktop is never stopped from here."""
        if not slurm_job_id:
            return err("no SLURM job id")
        job_id = str(slurm_job_id)
        if self._stopping(job_id):  # asked already: it is closing its viewer
            return ok(cancelled=[job_id], graceful=True)
        if job_id not in self._stop_requests:
            signalled = await self.slurm_service.signal_batch_script(job_id, "TERM")
            if signalled.get("success"):
                self._stop_requests[job_id] = time.monotonic()
                return ok(cancelled=[job_id], graceful=True)
        self._stop_requests.pop(job_id, None)
        return await self.slurm_service.scancel_jobs([job_id])

    async def send_chimerax_command(
        self, session_info: dict[str, Any], command: str, *, timeout: float = 60.0
    ) -> dict[str, Any]:
        """Run a ChimeraX/ArtiaX command string in a LIVE curation session.

        A session driven from outside means what crboost thinks it means only if the
        scope on disk moves with it. This channel therefore carries exactly two things,
        both gated on `curation.rest_enabled`: launch-time health checks, and
        `switch_session_scope`, which records the new scope in the same call. Any further
        caller must do the same. Saving on the user's behalf is out of bounds.

        Reaches the node's loopback REST server by ssh-hopping (the headnode can ssh
        to a node where the user has a running job — the same access VNC relies on).
        The command MUST go in the GET query (`curl -G --data-urlencode`); a plain
        urlencoded POST body is ignored by `/run` ("command parameter missing").
        Returns `{success, error, raw, data}` — `success` is False if ssh/curl fails
        OR ChimeraX reported an error (json-true `error` field / log error messages).
        """
        node = (session_info or {}).get("node")
        rest_port = (session_info or {}).get("rest_port")
        if not node or not rest_port:
            return err("session has no REST endpoint (no rest_port) — relaunch the session")
        remote = (
            f"curl -s -G --max-time {int(timeout)} "
            f"--data-urlencode {shlex.quote('command=' + command)} "
            f"http://127.0.0.1:{int(rest_port)}/run"
        )
        # StrictHostKeyChecking=no (NOT accept-new): the CBE headnode runs OpenSSH 7.4
        # (el7), which predates accept-new (OpenSSH 7.6) and errors "unsupported option".
        # `no` is the el7-safe auto-accept for trusted intra-cluster headnode→node hops
        # (BatchMode=yes can't prompt, so an unknown node would otherwise fail). ServerAlive*
        # bounds a post-connect hang (channel stalls after TCP/auth, so ConnectTimeout no
        # longer applies) at ~24 s instead of forever.
        cmd = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=8",
            "-o",
            "ServerAliveCountMax=3",
            str(node),
            remote,
        ]
        try:
            # On the timeout run_process kills the ssh before it raises.
            proc = await run_process(cmd, timeout=timeout + 15)
        except subprocess.TimeoutExpired:
            return err(f"ChimeraX command timed out after {timeout}s")
        except OSError as e:
            logger.exception("ssh to %s could not be started", node)
            return err(str(e))
        out, err_out = proc.stdout, proc.stderr
        if proc.returncode != 0:
            detail = err_out.decode(errors="replace").strip() or out.decode(errors="replace").strip()
            return err(f"ssh/curl to {node} failed: {detail}")
        raw = out.decode(errors="replace").strip()
        try:
            data = json.loads(raw)
        except Exception:
            return ok(raw=raw)  # non-JSON body (server not in json mode) → treat as ok
        cx_err = data.get("error")
        log = data.get("log messages") or {}
        log_err = log.get("error") if isinstance(log, dict) else None
        log_err_txt = (log_err[0] if isinstance(log_err, (list, tuple)) else str(log_err)) if log_err else None
        # ChimeraX's REST `error` is {"type","message"} (sometimes a bare string), and it carries
        # both real failures and benign internal noise. A real command failure is a UserError
        # (bad args / missing file, must surface); an internal trigger bug is some other exception
        # type. ArtiaX/ChimeraX's command-history save() raises AttributeError ("'ParticleList'
        # object has no attribute 'string'") on every command once a particle list is open; the
        # command still ran, so it must not fail the call. Heuristic: fail only on a UserError
        # dict or a clean non-traceback string; log everything else as noise.
        real_err = None
        benign_err = None
        if isinstance(cx_err, dict):
            etype, emsg = cx_err.get("type"), (cx_err.get("message") or "")
            if etype and etype != "UserError":
                benign_err = f"{etype}: {emsg}".strip(": ")
            elif emsg:
                real_err = emsg
        elif isinstance(cx_err, str) and cx_err:
            if "Traceback (most recent call last)" in cx_err:
                benign_err = cx_err
            else:
                real_err = cx_err
        if real_err:
            logger.info("ChimeraX REST command error: %s — raw: %s", real_err, raw[:1500])
            return err(real_err, log_error=log_err_txt or benign_err, raw=raw, data=data)
        if benign_err or log_err_txt:
            logger.debug("ChimeraX REST benign diagnostics: %s", raw[:1500])
        return ok(log_error=log_err_txt or benign_err, raw=raw, data=data)

    async def prepare_curation_bundle(
        self,
        project_path: Path,
        candidates_star: Path | None,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str = "",
        *,
        species_id: str = "",
        source_star: Path | None = None,
        coords_label: str = "auto",
    ) -> dict[str, Any]:
        """Export one tomogram's picks → `.coords`, write an `open_<tomo>.cxc` that
        preloads them in ArtiaX, and DECLARE the scope in the dir's `manifest.json`.
        Returns the resolved paths + the copyable ChimeraX command lines (`commands`);
        pass `cxc_path` + `scope` to launch_curation_session(). Disk I/O + numpy run off
        the event loop, including the one-time display-recon downsample, which is why
        the caller should say it is preparing before awaiting this.

        Defaults to the PyTOM auto list (`candidates_star`). Pass `source_star` +
        `coords_label` to open a SPECIFIC workbench list instead (its centered-Å
        star as the export source, labelled so its reference `.coords` is named
        apart from the user's own save) — the per-list "Open in ArtiaX" path.
        With neither (a species picked de novo), the session opens the bare
        tomogram and the user picks into an empty ArtiaX list.
        """
        from services.project_state import get_project_state_for
        from services.segmentation.chimerax import session_volumes
        from services.tilt_series import get_registry_for
        from services.visualization import artiax_bridge

        out_dir = artiax_bridge.curation_dir(
            Path(project_path), tomo_name, species_id=species_id, species_label=species_label
        )
        export_source = source_star or candidates_star
        # The tomogram's segmentations, as the views show them; read here, on the event loop, where
        # the cached registry is shared.
        segmentations = session_volumes(
            get_project_state_for(Path(project_path)),
            get_registry_for(Path(project_path)),
            Path(project_path),
            tomo_name,
            Path(tomograms_star),
        )
        try:
            info = await asyncio.to_thread(
                artiax_bridge.prepare_curation_bundle,
                Path(export_source) if export_source is not None else None,
                Path(tomograms_star),
                tomo_name,
                out_dir,
                species=species_label,
                species_id=species_id,
                project_path=Path(project_path),
                coords_label=coords_label,
                project_root=Path(project_path),
                display_bin=int(getattr(self.config_service.curation, "display_bin", 1) or 1),
                segmentations=segmentations,
                # What a session binds (launch_curation_session): the site roots, the project, and
                # the home directory its worker binds itself.
                visible_roots=[*map(Path, self.config_service.config.container_binds), Path(project_path), Path.home()],
            )
        except Exception as e:
            logger.warning("prepare_curation_bundle failed for %s: %s", tomo_name, e)
            return err(str(e))
        if info.get("seed_coords"):
            # The seed is registered here, at the Curate click, as a 0-pick row, not left
            # for the watcher, which would only meet it after the first save. The
            # bundle stays ok on a registration failure: the `.cxc` opens the seed
            # regardless, and the watcher registers it on the first save.
            reg = await self._ensure_seed_registered(
                Path(project_path),
                Path(tomograms_star),
                tomo_name,
                species_label,
                species_id,
                Path(info["seed_coords"]),
                created=bool(info.get("seed_created")),
            )
            info["seed_slug"] = reg.get("slug")
            info["seed_registered"] = bool(reg.get("registered"))
            info["seed_error"] = reg.get("error") if not reg.get("success") else None
        return ok(**info)

    async def _ensure_seed_registered(
        self,
        project_path: Path,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str,
        species_id: str,
        seed: Path,
        *,
        created: bool,
    ) -> dict[str, Any]:
        """Make sure the seeded default list is a registered ``PickList``.

        Idempotent: a seed that already existed and is already registered is left alone
        (count and label untouched, no re-ingest, no watcher event). A just-created seed is
        always registered from the file (0 rows → a header-only star, count 0); an existing
        seed with no registered list (the list was deleted but the file survived, or the
        dir predates seeding) is registered from whatever it holds. ``ok(slug, registered)`` /
        ``err(...)``.
        """
        from services.particles.ingest import default_slug_for, register_manual_pick_list
        from services.project_state import get_project_state_for, get_state_service

        slug = default_slug_for(species_id, tomo_name)
        state = get_project_state_for(project_path)
        if not created and state.get_pick_list(slug, species_id, tomo_name) is not None:
            return ok(slug=slug, registered=False)
        try:
            res = await self.import_curation_picks(
                project_path, tomograms_star, tomo_name, species_label, species_id, coords_path=seed, archive=False
            )
            if not res.get("success"):
                return err(f"seed list not registered: {res.get('error')}", slug=slug)
            register_manual_pick_list(state, res, species_id, tomo_name)
            # Explicit path, force=True: this can run from a click handler whose client
            # context a bare save would silently no-op on (cf. list_admin.delete_pick_list).
            await get_state_service().save_project(project_path=project_path, force=True)
        except Exception as e:
            logger.exception("Could not register the seed list %s for %s/%s", seed.name, species_id, tomo_name)
            return err(f"seed list not registered: {e}", slug=slug)
        return ok(slug=slug, registered=True)

    def curation_scope(
        self, project_path: Path, species_id: str, species_label: str, tomo_name: str, curation_dir: str = ""
    ) -> dict[str, Any]:
        """The scope payload a launch declares — what `launch_curation_session(scope=...)`
        persists and what a reconnecting control center reads back. One builder so the
        keys can't drift between the writer and the readers."""
        from services.visualization import artiax_bridge

        cur = curation_dir or str(
            artiax_bridge.curation_dir(
                Path(project_path), tomo_name, species_id=species_id, species_label=species_label
            )
        )
        return {
            "project_path": str(project_path),
            "species_id": species_id,
            "species_label": species_label,
            "tomo_name": tomo_name,
            "curation_dir": cur,
        }

    def _discover_manual_coords(
        self, project_path: Path, tomo_name: str, *, species_id: str = "", species_label: str = ""
    ) -> list[Path]:
        """Saved ArtiaX `.coords` for one (species, tomo), newest first.

        Scans only that tomogram's `curation_dir`: every `.coords` under it is this
        tomogram's by construction, so newest-wins cannot pick up another tomogram's save
        (`.coords` are physical Å tied to one volume). Excludes crboost's own exports
        (`auto.coords`, `*_ref.coords`); the non-recursive glob skips the `imports/`
        archive. Match is by extension + mtime, not a fixed name, since the user may name
        the save anything (e.g. `particles.coords`)."""
        from services.visualization import artiax_bridge

        return artiax_bridge.user_coords_saves(
            artiax_bridge.curation_dir(
                Path(project_path), tomo_name, species_id=species_id, species_label=species_label
            )
        )

    async def import_curation_picks(
        self,
        project_path: Path,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str = "",
        species_id: str = "",
        *,
        coords_path: Path | None = None,
        archive: bool = True,
    ) -> dict[str, Any]:
        """Ingest a manually-saved ArtiaX `.coords` for one (species, tomo) back
        into the pipeline. `archive=False` skips the `imports/` provenance copy; the
        seed registration uses it, because a 0-byte file nobody saved is not an import
        worth archiving.

        Converts the `.coords` (physical Å from the volume corner) → a RELION-5
        centered-Å particles star at `Curation/<species>/<tomo>/manual__<stem>.star`
        (the same `TomogramGeometry` as export, so the round trip is parity-exact) and
        archives the raw `.coords` under that tomogram's `imports/<stamp>.coords` for
        provenance. Returns the count + resolved paths; the caller registers the
        matching `manual__<stem>` PickList on ProjectState (this method owns only file
        I/O, off the event loop).

        One star per source file: the file stem is the list identity, so N lists saved in
        a session stay N lists, and re-saving under the same name updates that list.

        The dir's `manifest.json` supplies `corner_offset_angst`, the display-binning
        term, so a pick placed on the binned display volume maps back into the full-res
        frame exactly. Absent manifest ⇒ 0.0 (no display binning).

        When `coords_path` is None, auto-discovers the newest non-export `.coords` for
        this (species, tomo) — the explicit-click fallback; the watcher always names one.
        """
        from services.visualization import artiax_bridge

        project_path = Path(project_path)
        cur_dir = artiax_bridge.curation_dir(
            project_path, tomo_name, species_id=species_id, species_label=species_label
        )

        if coords_path is not None:
            chosen = Path(coords_path)
            if not chosen.exists():
                return err(f"No such .coords file: {chosen}")
            discovered: list[str] = [str(chosen)]
        else:
            cands = self._discover_manual_coords(
                project_path, tomo_name, species_id=species_id, species_label=species_label
            )
            discovered = [str(c) for c in cands]
            if not cands:
                return err(
                    f"No .coords files found under {cur_dir}", code=ErrorCode.NO_COORDS_FOUND, searched=str(cur_dir)
                )
            chosen = cands[0]

        from services.particles.ingest import manual_slug_for

        slug = manual_slug_for(chosen)
        out_star = cur_dir / f"{slug}.star"
        # The offset belongs to the dir the file was SAVED in (that dir's session declared
        # what volume ArtiaX opened) — not to the destination, which for an external import
        # by path is a different dir entirely and never had a display copy.
        manifest = artiax_bridge.read_manifest(chosen.parent) or {}
        offset = float(manifest.get("corner_offset_angst") or 0.0)
        try:
            count = await asyncio.to_thread(
                artiax_bridge.import_coords_to_centered_star,
                chosen,
                Path(tomograms_star),
                tomo_name,
                out_star,
                project_path,
                corner_offset_angst=offset,
            )
        except Exception as e:
            logger.warning("import_curation_picks failed for %s: %s", tomo_name, e)
            return err(str(e))

        # Archive the raw .coords for provenance (ArtiaX files carry no author).
        raw_copy = chosen
        if archive:
            try:
                raw_dir = cur_dir / "imports"
                raw_dir.mkdir(parents=True, exist_ok=True)
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                dest = raw_dir / f"{stamp}__{chosen.stem}.coords"
                await asyncio.to_thread(lambda: dest.write_bytes(Path(chosen).read_bytes()))
                raw_copy = dest
            except Exception as e:
                logger.warning("Could not archive raw import %s: %s", chosen, e)

        return ok(
            count=int(count),
            slug=slug,
            out_star=str(out_star),
            coords_source=str(chosen),
            raw_import=str(raw_copy),
            corner_offset_angst=offset,
            discovered=discovered,
            created_by=self.username,
        )

    async def assign_unattributed_coords(
        self, project_path: Path, source: Path, species_id: str, species_label: str, tomo_name: str
    ) -> dict[str, Any]:
        """The staging step: the user assigns a list ArtiaX produced to a species, so its
        attribution is never ambiguous.

        `source` is a `.coords` file the watcher could not attribute — or the directory
        holding several. Every user save under it is MOVED into
        `Curation/<species>/<tomo>/`, a manifest declaring that identity is written, and
        the watcher ingests them on its next tick. Nothing is guessed and nothing is
        copied-and-left: after this the file lives in exactly one place, whose meaning is
        written down. Returns `moved` (destination paths) + `skipped` (what could not be
        moved, and why).

        A destination name already taken is not overwritten: the incoming file gets a
        `__2`, `__3` … suffix, because same name means same PickList and would silently
        replace an earlier list.
        """
        from services.visualization import artiax_bridge

        src = Path(source)
        files = artiax_bridge.user_coords_saves(src) if src.is_dir() else ([src] if src.is_file() else [])
        if not files:
            return err(f"No user .coords to assign under {src}")
        dest_dir = artiax_bridge.curation_dir(
            Path(project_path), tomo_name, species_id=species_id, species_label=species_label
        )
        moved: list[str] = []
        skipped: list[str] = []

        def _move_all() -> None:
            dest_dir.mkdir(parents=True, exist_ok=True)
            for f in files:
                target = dest_dir / f.name
                n = 2
                while target.exists():
                    target = dest_dir / f"{f.stem}__{n}{f.suffix}"
                    n += 1
                try:
                    f.replace(target)  # same FS: atomic rename; across FS it raises and we say so
                except OSError as e:
                    skipped.append(f"{f.name}: {e}")
                    continue
                moved.append(str(target))

        await asyncio.to_thread(_move_all)
        if moved:
            try:
                await asyncio.to_thread(
                    artiax_bridge.write_manifest,
                    dest_dir,
                    species_id=species_id,
                    species_label=species_label,
                    tomo_name=tomo_name,
                    project_path=str(project_path),
                    assigned_at=datetime.now().isoformat(timespec="seconds"),
                    assigned_by=self.username,
                )
            except OSError as e:
                # The move landed; only the declaration didn't. Attribution then falls back
                # to the (correct) directory slugs, so report it rather than fail the assign.
                logger.warning("Assigned %d file(s) to %s but its manifest failed: %s", len(moved), dest_dir, e)
                skipped.append(f"manifest not written: {e}")
        if not moved:
            return err("; ".join(skipped) or f"Nothing could be moved out of {src}", skipped=skipped)
        logger.info("Assigned %d .coords to %s/%s -> %s", len(moved), species_id, tomo_name, dest_dir)
        return ok(moved=moved, skipped=skipped, dest_dir=str(dest_dir), count=len(moved))

    async def merge_pick_lists(
        self,
        project_path: Path,
        species_id: str,
        species_label: str,
        tomo_name: str,
        sources: list[dict[str, Any]],
        out_slug: str = "merged",
    ) -> dict[str, Any]:
        """Union 2+ pick lists into one `merged` centered-Å star — NO dedup (that's
        a separate, user-triggered action). `sources` = ``[{"path", "type"}]``; the
        rows are ordered by list-type priority (curated/human before machine `auto`)
        so a later greedy dedup keeps manual over auto. Writes
        `Curation/<species>/<tomo>/<slug>.star`; the caller registers a `merged`
        PickList. Disk I/O + numpy off the event loop.
        """
        from services.particles import pick_merge
        from services.visualization import artiax_bridge

        out_star = (
            artiax_bridge.curation_dir(
                Path(project_path), tomo_name, species_id=species_id, species_label=species_label
            )
            / f"{artiax_bridge._safe_slug(out_slug)}.star"
        )
        srcs = [{"path": s["path"], "priority": pick_merge.type_priority(s.get("type", ""))} for s in sources]
        try:
            info = await asyncio.to_thread(pick_merge.merge_lists_to_star, srcs, tomo_name, out_star)
        except Exception as e:
            logger.warning("merge_pick_lists failed for %s: %s", tomo_name, e)
            return err(str(e))
        return ok(**info)

    async def list_clash_stats(self, star_path: Path, tomo_name: str, radius_ang: float) -> dict[str, Any]:
        """Overlap overview for a list at a chosen radius (Å): how many picks clash
        and how many a dedup would remove/keep. Read-only — never mutates the list."""
        from services.particles import pick_merge

        try:
            stats = await asyncio.to_thread(pick_merge.clash_stats_star, Path(star_path), tomo_name, float(radius_ang))
        except Exception as e:
            logger.warning("list_clash_stats failed for %s: %s", star_path, e)
            return err(str(e))
        return ok(**stats)

    async def deduplicate_pick_list(self, star_path: Path, tomo_name: str, radius_ang: float) -> dict[str, Any]:
        """Greedy radius-dedup a list's star in place — drop every pick within
        `radius_ang` Å of a higher-priority (earlier) pick. Rewrites the star; the
        caller updates the PickList count + (it becomes stale → re-extract)."""
        from services.particles import pick_merge

        try:
            info = await asyncio.to_thread(pick_merge.deduplicate_star, Path(star_path), tomo_name, float(radius_ang))
        except Exception as e:
            logger.warning("deduplicate_pick_list failed for %s: %s", star_path, e)
            return err(str(e))
        return ok(**info)

"""ChimeraX + ArtiaX curation sessions (VNC over a SLURM job): submit, poll,
reconnect, and round-trip manual picks (`.coords` ↔ centered-Å star) between ArtiaX
and the pipeline.

Model B (roadmap 10, maintainer decision 2026-08-21): a session's SCOPE — which species,
which tomogram — is declared at launch and written into the scoped directory's
`manifest.json` and the session's `scope.json`. The old outbound driving
(`load_into_session` / `save_curation_picks` / `save_session_particle_lists`) is gone: it
inferred the session's meaning from an in-memory dict that a restart emptied, silently
filed a second species' picks under the first, and ingested only the newest of N saved
lists. What replaces it is the staging contract — every `.coords` in a scoped dir becomes
its own pick list, and anything saved outside one lands in the unattributed inbox for
explicit assignment (`assign_unattributed_coords`), never a guess.

Roadmap 13-S2 admits exactly ONE outbound command after launch: the confirmed scope switch
(`switch_session_scope`). It re-points the running viewer over the same REST channel AND
rewrites `scope.json` + the target manifest in the same call, so the scope on disk is
always the viewer's scope — the invariant the old swap broke. It never saves for the
user; the UI confirms first because `close session` drops unsaved ArtiaX lists."""

from __future__ import annotations
import asyncio
import json
import logging
import os
import re
import shlex
import socket
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from services.computing.slurm_service import SlurmService
from services.configs.config_service import get_config_service
from services.result import ErrorCode, err, ok

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
        # Serializes the ONE thing that drives a running session — the scope switch
        # (13-S2) — so two clicks cannot interleave their `close session` chains, and the
        # scope written to disk is the scope of the chain that ran last.
        self._switch_lock = asyncio.Lock()

    async def launch_curation_session(
        self, project_path: Path | None = None, cxc_path: Path | None = None, scope: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Submit a ChimeraX+ArtiaX VNC desktop as a SLURM job (partition 'c' by
        default — software GL is enough for slice-based picking).

        `cxc_path`, when given, is a crboost-generated `.cxc` (see
        services/visualization/artiax_bridge.prepare_curation_bundle) passed to the
        worker as CB_CXC so the session opens with the tomogram + picks preloaded
        instead of blank.

        `scope` is that bundle's declared identity — `{species_id, species_label,
        tomo_name, curation_dir, project_path}`. It is the ONLY moment the session's
        meaning is fixed (Model B): it is written to `<session_dir>/scope.json` so a
        reconnecting UI — or this process after a restart — can say what the running
        viewer was launched on, and stamped into the scoped dir's `manifest.json` as
        `launched_at`, which is what tells the curation watcher that dir is hot.

        Returns the SLURM job id and the shared-FS session dir that the compute
        node writes `session.json` into; poll it with get_curation_session_info().
        See containers/chimerax_artiax/curation_session.sh.
        """
        cur = self.config_service.curation
        sif = os.environ.get("CX_SIF") or cur.sif_path
        if not sif or not Path(sif).exists():
            return err(
                f"ChimeraX SIF not found (curation.sif_path={cur.sif_path!r}, "
                f"CX_SIF={os.environ.get('CX_SIF')!r}). Build it under "
                "containers/chimerax_artiax/ and set curation.sif_path in conf.yaml."
            )

        worker = self.server_dir / "containers" / "chimerax_artiax" / "curation_session.sh"
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

        sbatch_script = session_dir / "submit.sh"
        # GPU one-click: --gres + the VirtualGL switch mirror what `launch_curation_vnc.sh g`
        # does on the manual path — `export CX_VGL=1` flips the worker to `vglrun -d egl chimerax`
        # under apptainer --nv, so the _GL.sif renders on the GPU instead of software-GL llvmpipe.
        gres_line = f"#SBATCH --gres={cur.gres}\n" if cur.gres else ""
        vgl_export = "export CX_VGL=1\n" if cur.vgl else ""
        # Opt-in only (roadmap 10-S1, the maintainer's "can we make it passwordless?"):
        # the desktop then runs `-SecurityTypes None`, so anyone who can reach the rfb
        # port on that node drives it. Off by default; the control center states the risk.
        nopass_export = "export CX_VNC_NOPASS=1\n" if cur.passwordless_vnc else ""
        sbatch_script.write_text(
            "#!/usr/bin/env bash\n"
            f"#SBATCH -p {cur.partition}\n"
            f"{gres_line}"
            f"#SBATCH --cpus-per-task={cur.cpus}\n"
            f"#SBATCH --mem={cur.mem}\n"
            f"#SBATCH --time={cur.time}\n"
            "#SBATCH -J cb-curation\n"
            f"#SBATCH -o {session_dir / 'slurm.log'}\n"
            f"#SBATCH -e {session_dir / 'slurm.log'}\n"
            f"export CX_SIF={shlex.quote(str(sif))}\n"
            f"export CX_BIN={shlex.quote(cur.chimerax_bin)}\n"
            f"export CX_GEOMETRY={shlex.quote(cur.geometry)}\n"
            f"export CX_DISPLAY={display_num}\n"
            f"export CX_LOGIN_HOST={shlex.quote(login_host)}\n"
            f"export CB_SESSION_DIR={shlex.quote(str(session_dir))}\n"
            f"{vgl_export}"
            f"{nopass_export}"
            + (f"export CB_CXC={shlex.quote(str(cxc_path))}\n" if cxc_path else "")
            + f"exec {shlex.quote(str(worker))}\n"
        )
        sbatch_script.chmod(0o755)

        # Strip SLURM_*/SBATCH_* so submission doesn't inherit a parent job context.
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith(("SLURM_", "SBATCH_"))}
        try:
            proc = await asyncio.create_subprocess_exec(
                "sbatch",
                str(sbatch_script),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(session_dir),
                env=clean_env,
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                return err(f"sbatch failed: {stderr.decode().strip()}")
            out = stdout.decode().strip()
            slurm_job_id = out.split()[-1] if out else None
        except Exception as e:
            return err(str(e))

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

    # ── declared scope (Model B) ───────────────────────────────────────────────

    @staticmethod
    def _record_scope(
        session_dir: Path, slurm_job_id: str | None, scope: dict[str, Any] | None, *, how: str = "launch"
    ) -> None:
        """Persist the session's scope beside it (`scope.json`), and stamp it into the
        scoped dir's manifest. Best-effort: a session whose scope could not be written
        still runs — its saves are attributed by the manifest the bundle already wrote,
        and worst case they reach the unattributed inbox. Never blocks a launch.

        `how` is `launch` or `switch` (13-S2), recorded as `scope_set_by`. `launched_at`
        now means "became this session's scope at" — it is the watcher's hot-dir key
        (`watcher.py`, the newest `launched_at` across manifests is rescanned every tick),
        which is exactly what a switch must move to the new dir."""
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
    ) -> dict[str, Any]:
        """Re-point a LIVE session at another (species, tomogram) — the one outbound command
        13-S2 admits after launch. `session_info` is what `find_active_curation_session*`
        returned (`node`, `rest_port`, `session_dir`, `slurm_job_id`); the three paths are
        the target bundle's; `scope` is `curation_scope(...)` for the target.

        Order matters and is the whole point: (1) the swap chain over REST — failure here
        means ArtiaX still has the OLD tomogram and the scope on disk stays the old one;
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
            open_recon, Path(auto_coords) if auto_coords else None, Path(seed_coords)
        )
        cd_cmd = artiax_bridge.cd_chimerax_command(scope["curation_dir"])
        seed_name = Path(seed_coords).name
        async with self._switch_lock:
            res = await self.send_chimerax_command(session_info, chain, timeout=120)
            if not res.get("success"):
                return err(f"ArtiaX did not switch: {res.get('error')}", raw=res.get("raw"))
            # VERIFY, don't assume (2026-09-06): a chain that returned ok is not proof the seed
            # is open and selected. `info models` lists `#id, name, shown` per model; the seed's
            # model is named after its file. Absent → the switch failed, whatever the chain said.
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
        it was launched on — including after a crboost restart, which is precisely what
        the old in-memory `_curation_loaded` could not survive."""
        try:
            data = json.loads((Path(session_dir) / "scope.json").read_text())
        except (OSError, ValueError):
            return {}
        return {"scope": data} if isinstance(data, dict) else {}

    async def get_curation_session_info(self, session_dir: str, slurm_job_id: str | None = None) -> dict[str, Any]:
        """Poll a launched curation session. Once the job is RUNNING and has
        published session.json, returns its connection info (node/port/password/
        tunnel_cmd); otherwise reports pending/starting/ended."""
        sdir = Path(session_dir)
        info_file = sdir / "session.json"
        if info_file.exists():
            try:
                data = json.loads(info_file.read_text())
                data.update({"success": True, "status": "ready"})
                return data
            except Exception as e:
                return ok(status="starting", detail=f"session.json not readable yet: {e}")

        # No session.json yet — ask SLURM why. query_jobs_by_ids distinguishes the two
        # cases get_user_jobs conflates (get_user_jobs returns [] on squeue failure, which
        # is indistinguishable from "no jobs"): None = squeue ITSELF failed (a transient we
        # must NOT read as "job gone"); {} = squeue is healthy and the job genuinely left.
        state = None
        squeue_ok = True
        if slurm_job_id:
            q = await self.slurm_service.query_jobs_by_ids([str(slurm_job_id)])
            if q is None:
                squeue_ok = False
            else:
                hit = q.get(str(slurm_job_id).split("_", 1)[0]) or q.get(str(slurm_job_id))
                state = hit[0] if hit else None

        if state in ("PENDING", "CONFIGURING", "SCHEDULED"):
            return ok(status="pending", slurm_state=state)
        if state is not None:
            # RUNNING (or similar) but session.json not visible yet — just started / NFS lag.
            return ok(status="starting", slurm_state=state)
        if not squeue_ok:
            # Couldn't reach the scheduler this tick — keep the spinner up rather than
            # fabricate an "ended" for a job that may well be alive (the transient-squeue
            # false-ended bug). The next 3 s poll re-checks.
            return ok(status="pending", slurm_state="scheduler unreachable — retrying")

        # squeue is healthy and the job is genuinely gone. Only "ended" if the job actually
        # ran (its log exists) — otherwise this is the brief post-sbatch window before it
        # registers, so report pending. If it ran but wrote NOTHING (0-byte log), it died in
        # the node-side container/VNC bring-up before logging — surface an actionable message
        # instead of a blank so the user retries rather than facing a mystery "session exited".
        log_file = sdir / "slurm.log"
        if log_file.exists():
            tail = log_file.read_text()[-1200:].strip()
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

    async def _live_project_curation_job_ids(self, project_path: Path) -> list[str]:
        """Live cb-curation SLURM job ids recorded under this project's
        `.curation_sessions/*/job.json` (squeue-derived liveness, never a stored
        bool). Shared by launch + stop housekeeping to clear zombie sessions."""
        base = Path(project_path) / ".curation_sessions"
        if not base.is_dir():
            return []
        recorded: set = set()
        for jf in base.glob("*/job.json"):
            try:
                jid = json.loads(jf.read_text()).get("slurm_job_id")
            except Exception:
                jid = None
            if jid:
                recorded.add(str(jid))
        if not recorded:
            return []
        jobs = await self.slurm_service.get_user_jobs(force_refresh=True)
        live = {j.job_id for j in jobs if j.name == "cb-curation" and j.state in self._CURATION_LIVE_STATES}
        live_bases = {x.split("_", 1)[0] for x in live}
        return sorted({jid for jid in recorded if jid in live or jid.split("_", 1)[0] in live_bases})

    # Curation jobs are submitted with `-J cb-curation`; a session is "live" iff
    # its SLURM job is in one of these states (squeue is the source of truth — we
    # never trust a cached is-running flag; see project memory on the stuck-yellow bug).
    _CURATION_LIVE_STATES = ("RUNNING", "PENDING", "CONFIGURING", "SCHEDULED", "COMPLETING")

    async def find_active_curation_session(self, project_path: Path) -> dict[str, Any] | None:
        """Return the project's one live curation session, or None.

        Scans `<project>/.curation_sessions/*/job.json`, then makes a single
        squeue call and returns the first session whose SLURM job is still live —
        merged with its `session.json` connection details when already published.
        Liveness is derived from squeue, never a stored boolean.
        """
        base = Path(project_path) / ".curation_sessions"
        if not base.is_dir():
            return None
        recorded = []
        for job_file in sorted(base.glob("*/job.json")):
            try:
                data = json.loads(job_file.read_text())
            except Exception:
                continue
            jid = data.get("slurm_job_id")
            if jid:
                recorded.append((jid, job_file.parent))
        if not recorded:
            return None

        jobs = await self.slurm_service.get_user_jobs(force_refresh=True)
        live = {j.job_id: j for j in jobs if j.name == "cb-curation" and j.state in self._CURATION_LIVE_STATES}
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
            info_file = sdir / "session.json"
            if info_file.exists():
                try:
                    out.update(json.loads(info_file.read_text()))
                except Exception:
                    pass
            out.update(self._read_scope(sdir))
            return out
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
        self, *, slurm_job_id: str | None, session_id: str, session_dir: Path, project_path: Path | None
    ) -> None:
        """Append a launched session to the user-level registry so find-or-reuse can
        locate it from ANY project. Best-effort — never blocks a launch. Lock-guarded
        against the prune-rewrite in find_active_curation_session_any so a concurrent
        prune can't drop this just-appended line."""
        try:
            reg = self._curation_registry_file()
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
        reuse model), or None. Reads the registry, makes a single squeue call,
        returns the first live `cb-curation` session merged with its session.json,
        and self-prunes dead/missing entries. Liveness is squeue-derived."""
        reg = self._curation_registry_file()
        if not reg.is_file():
            return None
        entries: list[dict[str, Any]] = []
        try:
            for ln in reg.read_text().splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    d = json.loads(ln)
                except Exception:
                    continue
                if d.get("slurm_job_id") and d.get("session_dir") and Path(d["session_dir"]).is_dir():
                    entries.append(d)
        except Exception:
            return None
        if not entries:
            return None

        jobs = await self.slurm_service.get_user_jobs(force_refresh=True)
        live = {j.job_id: j for j in jobs if j.name == "cb-curation" and j.state in self._CURATION_LIVE_STATES}
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

        # Prune dead entries — but ONLY when squeue actually returned data. An empty
        # result is indistinguishable from a squeue failure (get_user_jobs returns []
        # on error without caching it), and pruning then would wipe still-live sessions
        # — the one backing store the cross-project reuse path reads. Re-read fresh
        # under the lock so a concurrent launch's append isn't lost to a stale rewrite,
        # and drop only entries we CONFIRMED dead (preserving any new lines).
        if jobs and dead_dirs:
            try:
                async with self._curation_registry_lock:
                    fresh: list[str] = []
                    for ln in reg.read_text().splitlines():
                        ln = ln.strip()
                        if not ln:
                            continue
                        try:
                            dd = json.loads(ln)
                        except Exception:
                            continue
                        if dd.get("session_dir") in dead_dirs:
                            continue
                        fresh.append(json.dumps(dd))
                    reg.write_text("".join(s + "\n" for s in fresh))
            except Exception:
                pass
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
        info_file = sdir / "session.json"
        if info_file.exists():
            try:
                out.update(json.loads(info_file.read_text()))
            except Exception:
                pass
        out.update(self._read_scope(sdir))
        return out

    async def send_chimerax_command(
        self, session_info: dict[str, Any], command: str, *, timeout: float = 60.0
    ) -> dict[str, Any]:
        """Run a ChimeraX/ArtiaX command string in a LIVE curation session.

        QUARANTINED (roadmap 10-S1, relaxed by 13-S2). A session driven from outside
        cannot be trusted to mean what crboost thinks it means unless the scope on disk
        moves with it (10-external-picker-contract.md §1). This channel therefore carries
        exactly two things, both gated on `curation.rest_enabled`: launch-time health
        checks, and `switch_session_scope` — which records the new scope in the SAME
        call. Any further caller must do the same, or it reopens the bug class Model B
        closed; a caller that saves on the user's behalf is still out of bounds (that
        path was never verified and is what the old swap pretended to do).

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
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            out, err_out = await asyncio.wait_for(proc.communicate(), timeout=timeout + 15)
        except TimeoutError:
            # wait_for cancels the await but not the OS process — reap the orphaned ssh.
            if proc is not None:
                try:
                    proc.kill()
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except Exception:
                    pass
            return err(f"ChimeraX command timed out after {timeout}s")
        except Exception as e:
            return err(str(e))
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
        # BOTH real failures and benign internal noise. Distinguish them: a real command failure is
        # a UserError (bad args / missing file — we MUST surface it); an internal trigger bug is some
        # other exception type. Concretely, ArtiaX/ChimeraX's command-history save() raises
        # AttributeError ("'ParticleList' object has no attribute 'string'") on EVERY command once a
        # particle list is open — the command still ran and the tomo/picks loaded, so it must not
        # fail the call (it was toasting "Load failed" on every successful swap). Heuristic: fail
        # only on a UserError dict or a clean non-traceback string; log everything else as noise.
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
        the event loop — including the one-time display-recon downsample (10-S3), which
        is why the caller should say it is preparing before awaiting this.

        Defaults to the PyTOM auto list (`candidates_star`). Pass `source_star` +
        `coords_label` to open a SPECIFIC workbench list instead (its centered-Å
        star as the export source, labelled so its reference `.coords` is named
        apart from the user's own save) — the per-list "Open in ArtiaX" path.
        With neither (a species picked de novo), the session opens the bare
        tomogram and the user picks into an empty ArtiaX list.
        """
        from services.visualization import artiax_bridge

        out_dir = artiax_bridge.curation_dir(
            Path(project_path), tomo_name, species_id=species_id, species_label=species_label
        )
        export_source = source_star or candidates_star
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
            )
        except Exception as e:
            logger.warning("prepare_curation_bundle failed for %s: %s", tomo_name, e)
            return err(str(e))
        if info.get("seed_coords"):
            # The seed is registered HERE, at the Curate click, as a 0-pick row (13-S1) —
            # not left for the watcher, which would only meet it after the first save. The
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
        """Make sure the seeded default list is a registered ``PickList`` (13-S1).

        Idempotent: a seed that already existed AND is already registered is left alone —
        count and label untouched, no re-ingest, no watcher event. A just-created seed is
        always registered from the file (0 rows → a header-only star, count 0); an existing
        seed with no registered list (the list was deleted but the file survived, or a
        pre-13 dir) is registered from whatever it holds. ``ok(slug, registered)`` /
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

        Scans ONLY that tomogram's `curation_dir` — every `.coords` under it is THIS
        tomogram's by construction, so newest-wins is bleed-proof (the old per-species
        scan could grab a different tomo's save, and `.coords` are physical-Å tied to
        one volume → geometric garbage). EXCLUDES crboost's own exports (`auto.coords`,
        `*_ref.coords`); the non-recursive glob skips the `imports/` archive. Match is
        by extension + mtime, NOT a fixed name — the user may name the save anything
        (e.g. `particles.coords`)."""
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
        into the pipeline. `archive=False` skips the `imports/` provenance copy — the
        seed registration (13-S1) uses it, because a 0-byte file nobody saved is not an
        import worth archiving.

        Converts the `.coords` (physical Å from the volume corner) → a RELION-5
        centered-Å particles star at `Curation/<species>/<tomo>/manual__<stem>.star`
        (the same `TomogramGeometry` as export, so the round trip is parity-exact) and
        archives the raw `.coords` under that tomogram's `imports/<stamp>.coords` for
        provenance. Returns the count + resolved paths; the caller registers the
        matching `manual__<stem>` PickList on ProjectState (this method owns only file
        I/O, off the event loop).

        ONE STAR PER SOURCE FILE (roadmap 10-S2). It used to be one `manual.star` per
        (species, tomo), so of N lists saved in a session N−1 were silently overwritten;
        the file stem is now the identity, which also makes re-saving under the same name
        an UPDATE of that list (W1) rather than a new one.

        The dir's `manifest.json` supplies `corner_offset_angst` — the display-binning
        term (10-S3) — so a pick placed on the binned display volume maps back into the
        full-res frame exactly. Absent manifest ⇒ 0.0, which is what every pre-S3 dir is.

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
        """THE staging step (roadmap 10-S2, the maintainer's "the user assigns the list
        ArtiaX just produced to a particular species so there is absolutely no ambiguity").

        `source` is a `.coords` file the watcher could not attribute — or the directory
        holding several. Every user save under it is MOVED into
        `Curation/<species>/<tomo>/`, a manifest declaring that identity is written, and
        the watcher ingests them on its next tick. Nothing is guessed and nothing is
        copied-and-left: after this the file lives in exactly one place, whose meaning is
        written down. Returns `moved` (destination paths) + `skipped` (what could not be
        moved, and why).

        A destination name already taken is NOT overwritten — the incoming file gets a
        `__2`, `__3` … suffix, because same-name means same PickList and silently
        replacing someone's earlier list is the failure mode this whole stage exists to
        remove.
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

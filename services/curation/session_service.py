"""ChimeraX + ArtiaX curation sessions (VNC over a SLURM job): submit, poll,
reconnect, drive a live viewer over its REST channel, and round-trip manual
picks (`.coords` ↔ centered-Å star) between ArtiaX and the pipeline."""

from __future__ import annotations
import asyncio
import json
import logging
import os
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
        # `_curation_loaded` maps a live session's job id → the (species, tomo) it
        # currently has open (for save-on-swap). `_curation_registry_lock` serializes
        # the user-level registry's append/prune; `_curation_swap_locks` serializes
        # overlapping swaps into the SAME session.
        self._curation_loaded: dict[str, Any] = {}
        self._curation_registry_lock = asyncio.Lock()
        self._curation_swap_locks: dict[str, asyncio.Lock] = {}

    async def launch_curation_session(
        self, project_path: Path | None = None, cxc_path: Path | None = None
    ) -> dict[str, Any]:
        """Submit a ChimeraX+ArtiaX VNC desktop as a SLURM job (partition 'c' by
        default — software GL is enough for slice-based picking).

        `cxc_path`, when given, is a crboost-generated `.cxc` (see
        services/visualization/artiax_bridge.prepare_curation_bundle) passed to the
        worker as CB_CXC so the session opens with the tomogram + picks preloaded
        instead of blank.

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

        # Index in the USER-level registry so the session can be found + reused across
        # projects (a curation session is a per-user viewer, not per-project).
        await self._register_curation_session(
            slurm_job_id=slurm_job_id, session_id=session_id, session_dir=session_dir, project_path=project_path
        )

        logger.info("Curation session submitted: SLURM job %s (session %s)", slurm_job_id, session_id)
        return ok(slurm_job_id=slurm_job_id, session_dir=str(session_dir), session_id=session_id)

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
        return out

    async def send_chimerax_command(
        self, session_info: dict[str, Any], command: str, *, timeout: float = 60.0
    ) -> dict[str, Any]:
        """Run a ChimeraX/ArtiaX command string in a LIVE curation session.

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

    async def save_session_particle_lists(self, session_info: dict[str, Any], dest_dir: Path) -> dict[str, Any]:
        """Best-effort save of every ArtiaX ParticleList currently open in the live
        session to `dest_dir` as `.coords` (positions-only), so a swap's `close
        session` can't silently drop unsaved manual picks. Skips crboost's own
        reference exports (`auto.coords` / `*_ref.coords`). Defensive: any parse
        hiccup reports what it managed, never raises."""
        from services.visualization.artiax_bridge import _cxc_quote, _safe_slug

        info = await self.send_chimerax_command(session_info, "info models")
        if not info.get("success"):
            return err(info.get("error") or "could not query session models")
        try:
            jv = (info.get("data") or {}).get("json values") or []
            models = json.loads(jv[0]) if jv and isinstance(jv[0], str) else (jv[0] if jv else [])
        except Exception as e:
            return err(f"could not parse model list: {e}")
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        model_list = models if isinstance(models, list) else []
        if not model_list:
            # `info models` parsed to nothing — this ChimeraX build may not surface the
            # model tree in "json values" over REST. Log the raw envelope so a save that
            # finds no lists ("nothing open" though picks exist) is diagnosable without a
            # blind round-trip; this is the one REST path not yet runtime-verified.
            logger.info(
                "save_session_particle_lists: no models from 'info models' — raw: %s", (info.get("raw") or "")[:1500]
            )
        saved: list[str] = []
        skipped: list[str] = []
        for m in model_list:
            if not isinstance(m, dict) or m.get("class") != "ParticleList":
                continue
            spec = m.get("spec")
            name = str(m.get("value") or "").strip()
            if not spec or name == "auto.coords" or name.endswith("_ref.coords"):
                continue
            out = dest / f"{_safe_slug(name.rsplit('.', 1)[0]) or 'list'}.coords"
            res = await self.send_chimerax_command(session_info, f"save {_cxc_quote(out)} partlist {spec}")
            if res.get("success"):
                saved.append(str(out))
            else:
                # Best-effort sweep: one failed list must not abort the rest, but callers see it.
                logger.warning("save_session_particle_lists: failed to save list %r: %s", name, res.get("error"))
                skipped.append(name)
        return ok(saved=saved, skipped=skipped)

    async def save_curation_picks(
        self,
        session_info: dict[str, Any],
        *,
        project_path: Path | None = None,
        tomo_name: str = "",
        species_id: str = "",
        species_label: str = "",
    ) -> dict[str, Any]:
        """Forefront save: write every manual ParticleList open in the live session to
        the LOADED tomogram's curation dir as ``.coords``, where the dashboard prescan
        (`_auto_kick_coords_ingest`) auto-imports it as a ``manual`` pick list. crboost
        chooses the path, so the user never touches ArtiaX's Save dialog.

        The destination is the tomogram the session actually has open (tracked in
        ``_curation_loaded``); ``(project_path, tomo_name, species_*)`` is only a
        fallback for a session whose load this process didn't record (e.g. a
        ``.cxc``-preloaded start). Returns ``count`` (lists saved) so the UI can tell
        "saved N" from "nothing open to save"."""
        from services.visualization import artiax_bridge

        job_id = str((session_info or {}).get("slurm_job_id") or (session_info or {}).get("session_dir") or "")
        loaded = self._curation_loaded.get(job_id) or {}
        tomo = loaded.get("tomo_name") or tomo_name
        dest: Path | None = None
        if loaded.get("curation_dir"):
            dest = Path(loaded["curation_dir"])
        elif project_path and tomo_name:
            dest = artiax_bridge.curation_dir(
                Path(project_path), tomo_name, species_id=species_id, species_label=species_label
            )
        if dest is None:
            return err("No tomogram is loaded in the session yet — load one first.")
        res = await self.save_session_particle_lists(session_info, dest)
        saved = res.get("saved") or []
        if res.get("success"):
            return ok(saved=saved, count=len(saved), tomo=tomo, dest=str(dest))
        return err(
            res.get("error") or "could not save the session's particle lists",
            saved=saved,
            count=len(saved),
            tomo=tomo,
            dest=str(dest),
        )

    async def load_into_session(
        self,
        session_info: dict[str, Any],
        project_path: Path,
        candidates_star: Path | None,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str = "",
        *,
        species_id: str = "",
        source_star: Path | None = None,
        coords_label: str = "auto",
        save_first: bool = False,
    ) -> dict[str, Any]:
        """Swap a LIVE curation session to a new (species, tomo): clear what's open,
        then load the tomogram + its reference picks — the one-click alternative to
        copy-pasting into ChimeraX. With `save_first`, save whatever lists are
        currently open into the PREVIOUSLY-loaded tomo's dir before the clear."""
        from services.visualization import artiax_bridge

        if not (session_info or {}).get("rest_port"):
            return err("no live REST session — start a session first")

        job_id = str(session_info.get("slurm_job_id") or session_info.get("session_dir") or "")
        # Serialize swaps into the SAME session: two overlapping loads of different
        # tomos would race on _curation_loaded and on the single ChimeraX REST endpoint,
        # desyncing the recorded tomo from what ArtiaX actually has open.
        lock = self._curation_swap_locks.setdefault(job_id, asyncio.Lock())
        async with lock:
            saved = None
            if save_first:
                prev = self._curation_loaded.get(job_id) or {}
                if prev.get("curation_dir"):
                    saved = await self.save_session_particle_lists(session_info, Path(prev["curation_dir"]))

            bundle = await self.prepare_curation_bundle(
                project_path,
                candidates_star,
                tomograms_star,
                tomo_name,
                species_label,
                species_id=species_id,
                source_star=source_star,
                coords_label=coords_label,
            )
            if not bundle.get("success"):
                return err(bundle.get("error") or "could not prepare picks")

            # Point ChimeraX's cwd at this tomo's curation dir so ArtiaX's "Save particle
            # list" dialog defaults there (the worker only sets cwd at launch — it goes
            # stale after a swap, scattering saves into the wrong tomogram's folder).
            out_dir = artiax_bridge.curation_dir(
                Path(project_path), tomo_name, species_id=species_id, species_label=species_label
            )
            swap = " ; ".join(
                artiax_bridge.swap_chimerax_commands(bundle["recon"], bundle.get("auto_coords"), cwd=out_dir)
            )
            res = await self.send_chimerax_command(session_info, swap)
            if res.get("success"):
                self._curation_loaded[job_id] = {
                    "project_path": str(project_path),
                    "species_id": species_id,
                    "species_label": species_label,
                    "tomo_name": tomo_name,
                    "curation_dir": str(out_dir),
                }
                return ok(loaded=tomo_name, auto_count=bundle.get("auto_count"), saved=saved)
            return err(
                res.get("error") or "ChimeraX load command failed",
                loaded=tomo_name,
                auto_count=bundle.get("auto_count"),
                saved=saved,
            )

    def get_curation_loaded(self, session_info: dict[str, Any]) -> dict[str, Any] | None:
        """What (species, tomo) the live session currently has open via the REST swap,
        or None. Keyed exactly as load_into_session records it, so a reconnecting dialog
        can show a "Currently loaded" indicator. The session is shared per-user, so this
        reflects whatever the last swap put in ArtiaX — even a tomogram loaded from a
        different project's dashboard. In-memory only: empty until this backend process
        has done at least one load_into_session (a preloaded-via-.cxc start is not tracked)."""
        job_id = str((session_info or {}).get("slurm_job_id") or (session_info or {}).get("session_dir") or "")
        return self._curation_loaded.get(job_id) if job_id else None

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
        """Export one tomogram's picks → `.coords` and write an `open_<tomo>.cxc`
        that preloads them in ArtiaX. Returns the resolved paths + the copyable
        ChimeraX command lines (`commands`); pass `cxc_path` to
        launch_curation_session() for a preloaded session, or surface `commands`
        for an already-running one. Disk I/O + numpy run off the event loop.

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
                coords_label=coords_label,
                project_root=Path(project_path),
            )
        except Exception as e:
            logger.warning("prepare_curation_bundle failed for %s: %s", tomo_name, e)
            return err(str(e))
        return ok(**info)

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

        d = artiax_bridge.curation_dir(
            Path(project_path), tomo_name, species_id=species_id, species_label=species_label
        )
        if not d.is_dir():
            return []
        found: list[Path] = []
        for c in d.glob("*.coords"):
            if c.name == "auto.coords" or c.name.endswith("_ref.coords"):
                continue  # crboost's reference exports, not the user's save
            found.append(c)
        found.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return found

    async def import_curation_picks(
        self,
        project_path: Path,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str = "",
        species_id: str = "",
        *,
        coords_path: Path | None = None,
    ) -> dict[str, Any]:
        """Ingest a manually-saved ArtiaX `.coords` for one (species, tomo) back
        into the pipeline.

        Converts the `.coords` (physical Å from the volume corner) → a RELION-5
        centered-Å particles star at `Curation/<species>/<tomo>/manual.star` (the
        same `TomogramGeometry` as export, so the round trip is parity-exact), and archives
        the raw `.coords` under that tomogram's `imports/<stamp>.coords` for
        provenance. Returns the count + resolved paths; the caller registers a
        `manual` PickList on ProjectState (this method owns only file I/O, off the
        event loop). When `coords_path` is None, auto-discovers the newest non-export
        `.coords` for this (species, tomo).
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

        out_star = cur_dir / "manual.star"
        try:
            count = await asyncio.to_thread(
                artiax_bridge.import_coords_to_centered_star,
                chosen,
                Path(tomograms_star),
                tomo_name,
                out_star,
                project_path,
            )
        except Exception as e:
            logger.warning("import_curation_picks failed for %s: %s", tomo_name, e)
            return err(str(e))

        # Archive the raw .coords for provenance (ArtiaX files carry no author).
        raw_copy = chosen
        try:
            raw_dir = cur_dir / "imports"
            raw_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest = raw_dir / f"{stamp}.coords"
            await asyncio.to_thread(lambda: dest.write_bytes(Path(chosen).read_bytes()))
            raw_copy = dest
        except Exception as e:
            logger.warning("Could not archive raw import %s: %s", chosen, e)

        return ok(
            count=int(count),
            out_star=str(out_star),
            coords_source=str(chosen),
            raw_import=str(raw_copy),
            discovered=discovered,
            created_by=self.username,
        )

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
        from services.visualization import artiax_bridge, pick_merge

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
        from services.visualization import pick_merge

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
        from services.visualization import pick_merge

        try:
            info = await asyncio.to_thread(pick_merge.deduplicate_star, Path(star_path), tomo_name, float(radius_ang))
        except Exception as e:
            logger.warning("deduplicate_pick_list failed for %s: %s", star_path, e)
            return err(str(e))
        return ok(**info)

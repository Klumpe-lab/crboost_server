from __future__ import annotations
import asyncio
import getpass
import json
import logging
import os
import pwd
import shlex
import socket
import traceback
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

from services.templating.template_service import TemplateService
from services.templating.pdb_service import PDBService
from services.scheduling_and_orchestration.project_service import ProjectService
from services.scheduling_and_orchestration.pipeline_orchestrator_service import PipelineOrchestratorService
from services.computing.container_service import get_container_service
from services.scheduling_and_orchestration.pipeline_runner import PipelineRunnerService
from services.scheduling_and_orchestration.pipeline_monitor import PipelineMonitor
from services.project_state import JobType, get_state_service
from services.computing.slurm_service import SlurmService
from services.configs.config_service import get_config_service
from services.tilt_series import TiltSeriesRegistry, get_registry_for

logger = logging.getLogger(__name__)

# The process-wide backend. main.py constructs one at startup; UI surfaces opened
# without a threaded-through reference (e.g. the tomo dashboard, which is a function
# module) reach it via get_backend(). Same idiom as get_config_service()/get_state_service().
_backend_instance: Optional["CryoBoostBackend"] = None


def get_backend() -> Optional["CryoBoostBackend"]:
    """The process-wide backend instance, or None before one is constructed."""
    return _backend_instance


class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        global _backend_instance
        _backend_instance = self
        self.username = getpass.getuser()
        self.server_dir = server_dir
        self.config_service = get_config_service()
        self.project_service = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)
        self.container_service = get_container_service()
        self.slurm_service = SlurmService(self.username)
        self.pipeline_runner = PipelineRunnerService(self)
        self.state_service = get_state_service()
        self.template_service = TemplateService(self)
        self.pdb_service = PDBService(self)
        # PipelineMonitor is the single server-side observer of pipeline
        # status across all projects. Started in main.py's FastAPI startup
        # hook; not auto-started here so unit-test / one-shot driver paths
        # that construct a backend don't spawn a background task.
        self.pipeline_monitor = PipelineMonitor(self)

    def registry_for(self, project_path: Path) -> TiltSeriesRegistry:
        """TiltSeriesRegistry for a project. Lazily loaded from sidecar JSON
        at `{project_path}/registry/`. Empty for legacy projects without a
        registry on disk — populate via project_service on import/load."""
        return get_registry_for(project_path)

    async def start_pipeline(
        self, project_path: str, scheme_name: str, selected_jobs: List[str], required_paths: List[str]
    ):
        """
        Unified pipeline start method.
        selected_jobs is a list of instance_id strings (e.g. ['tsReconstruct', 'templatematching__ribosome']).
        """
        return await self.pipeline_orchestrator.deploy_and_run_scheme(
            project_dir=Path(project_path), selected_instance_ids=selected_jobs
        )

    async def delete_job(self, job_name: str, instance_id: Optional[str] = None) -> Dict[str, Any]:
        return await self.project_service.delete_job(job_name, instance_id=instance_id)

    async def submit_tilt_filter_dl(self, project_path: Path, instance_id: str) -> Dict[str, Any]:
        """Submit the tilt filter DL driver as a standalone SLURM job."""
        from services.path_resolution_service import PathResolutionService
        from services.models_base import JobStatus

        state = self.state_service.state_for(project_path)
        job_model = state.jobs.get(instance_id)
        if not job_model:
            return {"success": False, "error": f"Job '{instance_id}' not found"}

        # Resolve input paths
        resolver = PathResolutionService(state)
        try:
            io_paths = resolver.resolve_all_paths(
                job_model.job_type, job_model, project_path / "TiltFilter", instance_id=instance_id
            )
            job_model.paths.update({k: str(v) for k, v in io_paths.items() if v is not None})
        except Exception as e:
            return {"success": False, "error": f"Path resolution failed: {e}"}

        # Create job directory and clean up stale markers from previous runs
        job_dir = project_path / "TiltFilter" / "dl_run"
        job_dir.mkdir(parents=True, exist_ok=True)
        for marker in ("RELION_JOB_EXIT_SUCCESS", "RELION_JOB_EXIT_FAILURE"):
            (job_dir / marker).unlink(missing_ok=True)

        # Save state so the driver can read it
        job_model.execution_status = JobStatus.RUNNING
        state.mark_dirty()
        await self.state_service.save_project(project_path=project_path, force=True)

        # Build driver command
        python_exe = self.server_dir / "venv" / "bin" / "python3"
        if not python_exe.exists():
            python_exe = "python3"
        script_path = self.server_dir / "drivers" / "tilt_filter.py"
        driver_cmd = (
            f"export PYTHONPATH={self.server_dir}:${{PYTHONPATH}}; "
            f"{python_exe} {script_path} "
            f"--instance_id {instance_id} "
            f"--project_path {project_path}"
        )

        # Build sbatch script from template
        qsub_template = self.server_dir / "config" / "qsub.sh"
        template_text = qsub_template.read_text()

        slurm_cfg = job_model.get_effective_slurm_config()
        # Strip surrounding quotes that may be stored in config values
        constraint = slurm_cfg.constraint.strip("'\"")
        replacements = {
            "XXXextra1XXX": slurm_cfg.partition,
            "XXXextra2XXX": constraint,
            "XXXextra3XXX": str(slurm_cfg.nodes),
            "XXXextra4XXX": str(slurm_cfg.ntasks_per_node),
            "XXXextra5XXX": str(slurm_cfg.cpus_per_task),
            "XXXextra6XXX": slurm_cfg.gres,
            "XXXextra7XXX": slurm_cfg.mem,
            "XXXextra8XXX": slurm_cfg.time,
            "XXXoutfileXXX": str(job_dir / "run.out"),
            "XXXerrfileXXX": str(job_dir / "run.err"),
            "XXXcommandXXX": driver_cmd,
        }
        script = template_text
        for placeholder, value in replacements.items():
            script = script.replace(placeholder, value)

        sbatch_path = job_dir / "run_tilt_filter.sh"
        sbatch_path.write_text(script)
        sbatch_path.chmod(0o755)

        # Submit via sbatch
        try:
            proc = await asyncio.create_subprocess_exec(
                "sbatch",
                str(sbatch_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(job_dir),
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                job_model.execution_status = JobStatus.FAILED
                state.mark_dirty()
                await self.state_service.save_project(project_path=project_path, force=True)
                return {"success": False, "error": f"sbatch failed: {stderr.decode().strip()}"}

            # Parse job ID from "Submitted batch job 12345"
            output = stdout.decode().strip()
            slurm_job_id = output.split()[-1] if output else None
            job_model.slurm_job_id = slurm_job_id
            state.mark_dirty()
            await self.state_service.save_project(project_path=project_path, force=True)

            logger.info("Tilt filter DL submitted: SLURM job %s", slurm_job_id)
            return {"success": True, "slurm_job_id": slurm_job_id, "job_dir": str(job_dir)}

        except Exception as e:
            job_model.execution_status = JobStatus.FAILED
            state.mark_dirty()
            await self.state_service.save_project(project_path=project_path, force=True)
            return {"success": False, "error": str(e)}

    # ── ChimeraX + ArtiaX curation session (VNC over a SLURM job) ───────────────

    async def launch_curation_session(
        self, project_path: Optional[Path] = None, cxc_path: Optional[Path] = None
    ) -> Dict[str, Any]:
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
            return {
                "success": False,
                "error": (
                    f"ChimeraX SIF not found (curation.sif_path={cur.sif_path!r}, "
                    f"CX_SIF={os.environ.get('CX_SIF')!r}). Build it under "
                    "containers/chimerax_artiax/ and set curation.sif_path in conf.yaml."
                ),
            }

        worker = self.server_dir / "containers" / "chimerax_artiax" / "curation_session.sh"
        if not worker.exists():
            return {"success": False, "error": f"Worker script missing: {worker}"}

        login_host = cur.login_host or socket.getfqdn()

        # Session dir on shared FS visible to BOTH the compute node (writer) and
        # this headnode (reader). Keep it inside the project when we have one.
        base = (Path(project_path) / ".curation_sessions") if project_path else (Path.home() / ".crboost" / "curation")
        session_id = uuid.uuid4().hex[:8]
        session_dir = base / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

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
                return {"success": False, "error": f"sbatch failed: {stderr.decode().strip()}"}
            out = stdout.decode().strip()
            slurm_job_id = out.split()[-1] if out else None
        except Exception as e:
            return {"success": False, "error": str(e)}

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

        logger.info("Curation session submitted: SLURM job %s (session %s)", slurm_job_id, session_id)
        return {
            "success": True,
            "slurm_job_id": slurm_job_id,
            "session_dir": str(session_dir),
            "session_id": session_id,
        }

    async def get_curation_session_info(self, session_dir: str, slurm_job_id: Optional[str] = None) -> Dict[str, Any]:
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
                return {"success": True, "status": "starting", "detail": f"session.json not readable yet: {e}"}

        # No session.json yet — ask SLURM why.
        state = None
        if slurm_job_id:
            jobs = await self.slurm_service.get_user_jobs(force_refresh=True)
            for j in jobs:
                if j.job_id == slurm_job_id or j.job_id.split("_", 1)[0] == slurm_job_id:
                    state = j.state
                    break

        if state in ("PENDING", "CONFIGURING", "SCHEDULED"):
            return {"success": True, "status": "pending", "slurm_state": state}
        if state is not None:
            # RUNNING (or similar) but session.json not visible yet — just started / NFS lag.
            return {"success": True, "status": "starting", "slurm_state": state}

        # Not in the queue and no session.json. Only call it "ended" if the job
        # actually ran (its log exists) — otherwise this is the brief window right
        # after sbatch before the job registers in squeue, so report pending.
        log_file = sdir / "slurm.log"
        if log_file.exists():
            return {"success": True, "status": "ended", "detail": log_file.read_text()[-1200:]}
        return {"success": True, "status": "pending", "slurm_state": "submitting"}

    async def stop_curation_session(self, slurm_job_id: Optional[str]) -> Dict[str, Any]:
        """scancel a curation session's SLURM job."""
        if not slurm_job_id:
            return {"success": False, "error": "no SLURM job id"}
        return await self.slurm_service.scancel_jobs([slurm_job_id])

    # Curation jobs are submitted with `-J cb-curation`; a session is "live" iff
    # its SLURM job is in one of these states (squeue is the source of truth — we
    # never trust a cached is-running flag; see project memory on the stuck-yellow bug).
    _CURATION_LIVE_STATES = ("RUNNING", "PENDING", "CONFIGURING", "SCHEDULED", "COMPLETING")

    async def find_active_curation_session(self, project_path: Path) -> Optional[Dict[str, Any]]:
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
            out: Dict[str, Any] = {
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

    async def prepare_curation_bundle(
        self,
        project_path: Path,
        candidates_star: Path,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str = "",
        *,
        source_star: Optional[Path] = None,
        coords_label: str = "auto",
    ) -> Dict[str, Any]:
        """Export one tomogram's picks → `.coords` and write an `open_<tomo>.cxc`
        that preloads them in ArtiaX. Returns the resolved paths + the copyable
        ChimeraX command lines (`commands`); pass `cxc_path` to
        launch_curation_session() for a preloaded session, or surface `commands`
        for an already-running one. Disk I/O + numpy run off the event loop.

        Defaults to the PyTOM auto list (`candidates_star`). Pass `source_star` +
        `coords_label` to open a SPECIFIC workbench list instead (its centered-Å
        star as the export source, labelled so its reference `.coords` is named
        apart from the user's own save) — the per-list "Open in ArtiaX" path.
        """
        from services.visualization import artiax_bridge

        out_dir = Path(project_path) / ".curation_sessions" / "bundles" / (artiax_bridge._safe_slug(species_label))
        try:
            info = await asyncio.to_thread(
                artiax_bridge.prepare_curation_bundle,
                Path(source_star or candidates_star),
                Path(tomograms_star),
                tomo_name,
                out_dir,
                species=species_label,
                coords_label=coords_label,
                project_root=Path(project_path),
            )
        except Exception as e:
            logger.warning("prepare_curation_bundle failed for %s: %s", tomo_name, e)
            return {"success": False, "error": str(e)}
        return {"success": True, **info}

    def _discover_manual_coords(self, project_path: Path, species_slug: str) -> List[Path]:
        """Saved ArtiaX `.coords` candidates for a species, newest first.

        Prefers the species curation bundle dir (unambiguously this species); only
        if it holds nothing does it widen to the session dirs (where ArtiaX's
        default-save may land in the cwd or a `manual/` subdir) — so a save for a
        different species sitting in a shared session dir doesn't get grabbed when
        this species has its own. EXCLUDES our own `*__auto.coords` exports and
        archived provenance copies. Match is by extension + mtime, NOT a fixed
        name — the user may name the save anything (e.g. `particles.coords`)."""
        base = Path(project_path) / ".curation_sessions"
        if not base.is_dir():
            return []

        def _scan(dirs: List[Path]) -> List[Path]:
            seen: set = set()
            found: List[Path] = []
            for d in dirs:
                if not d.is_dir():
                    continue
                for c in d.glob("*.coords"):
                    # Skip crboost's own reference exports (auto list + per-list
                    # *_ref re-curation seeds) and archived provenance copies —
                    # we want the user's SAVE, not what we handed them to load.
                    if c.name.endswith("__auto.coords") or c.name.endswith("_ref.coords") or "imports" in c.parts:
                        continue
                    rp = c.resolve()
                    if rp in seen:
                        continue
                    seen.add(rp)
                    found.append(c)
            found.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            return found

        bundle_hits = _scan([base / "bundles" / species_slug])
        if bundle_hits:
            return bundle_hits
        session_dirs: List[Path] = []
        for sd in base.glob("*"):
            if sd.is_dir() and sd.name != "bundles":
                session_dirs.append(sd)
                session_dirs.append(sd / "manual")
        return _scan(session_dirs)

    async def import_curation_picks(
        self,
        project_path: Path,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str = "",
        species_id: str = "",
        *,
        coords_path: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """Ingest a manually-saved ArtiaX `.coords` for one (species, tomo) back
        into the pipeline.

        Converts the `.coords` (physical Å from the volume corner) → a RELION-5
        centered-Å particles star at `ManualPicks/<species>/<tomo>.star` (the same
        `TomoFrame` as export, so the round trip is parity-exact), and archives the
        raw `.coords` under `ManualPicks/<species>/imports/<tomo>__<stamp>.coords`
        for provenance. Returns the count + resolved paths; the caller registers a
        `manual` PickList on ProjectState (this method owns only file I/O, off the
        event loop). When `coords_path` is None, auto-discovers the newest non-auto
        `.coords` for this species.
        """
        from services.visualization import artiax_bridge

        project_path = Path(project_path)
        bundle_slug = artiax_bridge._safe_slug(species_label or species_id or tomo_name)
        store_slug = artiax_bridge._safe_slug(species_id or species_label or tomo_name)
        tomo_slug = artiax_bridge._safe_slug(tomo_name)

        if coords_path is not None:
            chosen = Path(coords_path)
            if not chosen.exists():
                return {"success": False, "error": f"No such .coords file: {chosen}"}
            discovered: List[str] = [str(chosen)]
        else:
            cands = self._discover_manual_coords(project_path, bundle_slug)
            discovered = [str(c) for c in cands]
            if not cands:
                return {
                    "success": False,
                    "error": "no_coords_found",
                    "searched": str(project_path / ".curation_sessions"),
                }
            chosen = cands[0]

        out_star = project_path / "ManualPicks" / store_slug / f"{tomo_slug}.star"
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
            return {"success": False, "error": str(e)}

        # Archive the raw .coords for provenance (ArtiaX files carry no author).
        raw_copy = chosen
        try:
            raw_dir = project_path / "ManualPicks" / store_slug / "imports"
            raw_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest = raw_dir / f"{tomo_slug}__{stamp}.coords"
            await asyncio.to_thread(lambda: dest.write_bytes(Path(chosen).read_bytes()))
            raw_copy = dest
        except Exception as e:
            logger.warning("Could not archive raw import %s: %s", chosen, e)

        return {
            "success": True,
            "count": int(count),
            "out_star": str(out_star),
            "coords_source": str(chosen),
            "raw_import": str(raw_copy),
            "discovered": discovered,
            "created_by": self.username,
        }

    async def merge_pick_lists(
        self,
        project_path: Path,
        species_id: str,
        species_label: str,
        tomo_name: str,
        sources: List[Dict[str, Any]],
        out_slug: str = "merged",
    ) -> Dict[str, Any]:
        """Union 2+ pick lists into one `merged` centered-Å star — NO dedup (that's
        a separate, user-triggered action). `sources` = ``[{"path", "type"}]``; the
        rows are ordered by list-type priority (curated/human before machine `auto`)
        so a later greedy dedup keeps manual over auto. Writes
        `ManualPicks/<species>/<tomo>__<slug>.star`; the caller registers a `merged`
        PickList. Disk I/O + numpy off the event loop.
        """
        from services.visualization import artiax_bridge, pick_merge

        store_slug = artiax_bridge._safe_slug(species_id or species_label or tomo_name)
        out_star = (
            Path(project_path)
            / "ManualPicks"
            / store_slug
            / f"{artiax_bridge._safe_slug(tomo_name)}__{artiax_bridge._safe_slug(out_slug)}.star"
        )
        srcs = [{"path": s["path"], "priority": pick_merge.type_priority(s.get("type", ""))} for s in sources]
        try:
            info = await asyncio.to_thread(pick_merge.merge_lists_to_star, srcs, tomo_name, out_star)
        except Exception as e:
            logger.warning("merge_pick_lists failed for %s: %s", tomo_name, e)
            return {"success": False, "error": str(e)}
        return {"success": True, **info}

    async def list_clash_stats(self, star_path: Path, tomo_name: str, radius_ang: float) -> Dict[str, Any]:
        """Overlap overview for a list at a chosen radius (Å): how many picks clash
        and how many a dedup would remove/keep. Read-only — never mutates the list."""
        from services.visualization import pick_merge

        try:
            stats = await asyncio.to_thread(pick_merge.clash_stats_star, Path(star_path), tomo_name, float(radius_ang))
        except Exception as e:
            logger.warning("list_clash_stats failed for %s: %s", star_path, e)
            return {"success": False, "error": str(e)}
        return {"success": True, **stats}

    async def deduplicate_pick_list(self, star_path: Path, tomo_name: str, radius_ang: float) -> Dict[str, Any]:
        """Greedy radius-dedup a list's star in place — drop every pick within
        `radius_ang` Å of a higher-priority (earlier) pick. Rewrites the star; the
        caller updates the PickList count + (it becomes stale → re-extract)."""
        from services.visualization import pick_merge

        try:
            info = await asyncio.to_thread(pick_merge.deduplicate_star, Path(star_path), tomo_name, float(radius_ang))
        except Exception as e:
            logger.warning("deduplicate_pick_list failed for %s: %s", star_path, e)
            return {"success": False, "error": str(e)}
        return {"success": True, **info}

    async def get_default_data_globs(self) -> Dict[str, str]:
        """Get default glob patterns from config."""
        config_service = get_config_service()
        movies, mdocs = config_service.default_data_globs
        # Return empty strings if not set, to avoid UI errors
        return {"movies": movies if movies else "", "mdocs": mdocs if mdocs else ""}

    async def get_default_project_base(self) -> str:
        """Retrieves the default project base path from config."""
        config_service = get_config_service()
        configured_path = config_service.default_project_base
        if configured_path:
            return configured_path

        return str(Path.home())

    async def scan_for_projects(self, base_path: str) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._scan_for_projects_sync, base_path)

    def _scan_for_projects_sync(self, base_path: str) -> List[Dict[str, Any]]:
        projects = []
        path = Path(base_path)

        logger.info("Scanning directory: %s", path)

        if not path.exists():
            logger.info("Path does not exist: %s", path)
            return []
        if not path.is_dir():
            logger.info("Path is not a directory: %s", path)
            return []

        try:
            for item in path.iterdir():
                if not item.is_dir():
                    continue
                params_file = item / "project_params.json"
                if not params_file.exists():
                    if not item.name.startswith("."):
                        logger.info("Skipped '%s' - missing project_params.json", item.name)
                    continue
                try:
                    stats = params_file.stat()
                    mod_time = datetime.fromtimestamp(stats.st_mtime)

                    created_at = None
                    created_ts = None
                    creator = None
                    pipeline_active = False
                    total_jobs_planned = 0
                    ts_count = 0
                    mnemonic = ""
                    source_directory = ""
                    try:
                        with open(params_file) as f:
                            data = json.load(f)
                        raw_created = data.get("created_at")
                        if raw_created:
                            created_at = str(raw_created)[:16]
                            try:
                                created_ts = datetime.fromisoformat(str(raw_created)).timestamp()
                            except Exception:
                                created_ts = None
                        creator = data.get("created_by")
                        pipeline_active = bool(data.get("pipeline_active", False))
                        jobs_dict = data.get("jobs") or {}
                        total_jobs_planned = len(jobs_dict)
                        ts_count = data.get("import_selected_tilt_series") or data.get("import_total_tilt_series") or 0
                        mnemonic = data.get("mnemonic") or ""
                        # Where the raw data came from. Prefer the resolved
                        # frames dir; fall back to the movies glob's parent.
                        source_directory = data.get("import_source_directory") or ""
                        if not source_directory:
                            mg = data.get("movies_glob") or ""
                            if mg:
                                source_directory = str(Path(mg).parent) if "*" in mg else mg
                    except Exception:
                        pass

                    # Legacy projects (no `mnemonic` persisted) get a
                    # deterministic fallback so the UI always has something
                    # to show. Seed is the resolved project dir — stable
                    # across reloads, independent of project_name renames.
                    if not mnemonic:
                        try:
                            from services.project_nickname import nickname_for

                            mnemonic = nickname_for(str(item.resolve()))
                        except Exception:
                            mnemonic = ""

                    if creator is None:
                        try:
                            creator = pwd.getpwuid(stats.st_uid).pw_name
                        except Exception:
                            creator = None

                    if created_ts is None:
                        # Stable fallback: directory ctime (creation on most filesystems).
                        try:
                            created_ts = item.stat().st_ctime
                        except Exception:
                            created_ts = stats.st_mtime

                    derived = self._derive_live_status(item, jobs_dict)
                    derived["pipeline_active_flag"] = pipeline_active

                    last_activity_ts = max(stats.st_mtime, derived.get("last_activity_ts", 0.0))

                    projects.append(
                        {
                            "name": item.name,
                            "path": str(item),
                            "mnemonic": mnemonic,
                            "modified": mod_time.strftime("%Y-%m-%d %H:%M"),
                            "modified_timestamp": stats.st_mtime,
                            "created_at": created_at,
                            "created_timestamp": created_ts,
                            "creator": creator,
                            "pipeline_active": pipeline_active,
                            "total_jobs_planned": total_jobs_planned,
                            "ts_count": ts_count,
                            "source_directory": source_directory,
                            "last_activity_ts": last_activity_ts,
                            "last_activity": datetime.fromtimestamp(last_activity_ts).strftime("%Y-%m-%d %H:%M"),
                            **derived,
                        }
                    )
                except Exception as e:
                    logger.info("Error reading %s: %s", item.name, e)

        except Exception as e:
            logger.error("Error scanning projects: %s", e)
            return []

        # Stable order: oldest-first by created_at (or ctime fallback). Newly
        # created projects always go to the end -- positions never shuffle on
        # in-place edits to project_params.json.
        projects.sort(key=lambda x: (x["created_timestamp"], x["name"]))
        for idx, proj in enumerate(projects, start=1):
            proj["stable_index"] = idx
        logger.info("Found %d valid projects.", len(projects))
        return projects

    @staticmethod
    def _derive_live_status(project_dir: Path, jobs_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Derive pipeline status for the roster from project_params.json's
        `jobs` dict (canonical: includes tiltFilter and other non-RELION
        jobs that aren't in default_pipeline.star). Disk RELION_JOB_EXIT_*
        markers are only used to reconcile in-memory `Running` statuses
        that the schemer crashed before persisting -- otherwise the project
        state is the source of truth."""
        out: Dict[str, Any] = {
            "executed_jobs": 0,
            "succeeded": 0,
            "running_live": 0,
            "failed": 0,
            "scheduled": 0,
            "live_status": "idle",
            "last_activity_ts": 0.0,
        }
        pipeline_star = project_dir / "default_pipeline.star"
        if pipeline_star.exists():
            try:
                out["last_activity_ts"] = pipeline_star.stat().st_mtime
            except Exception:
                pass

        if not isinstance(jobs_dict, dict) or not jobs_dict:
            return out

        for _iid, job in jobs_dict.items():
            if not isinstance(job, dict):
                continue
            status = (job.get("execution_status") or "").strip()
            relion_name = job.get("relion_job_name")
            # Reconcile Running against on-disk exit markers so a crashed
            # schemer doesn't leave us perpetually showing a dead job as
            # "live".
            if status == "Running" and relion_name:
                job_dir = project_dir / relion_name.rstrip("/")
                success_marker = job_dir / "RELION_JOB_EXIT_SUCCESS"
                failure_marker = job_dir / "RELION_JOB_EXIT_FAILURE"
                if success_marker.exists():
                    status = "Succeeded"
                    try:
                        out["last_activity_ts"] = max(out["last_activity_ts"], success_marker.stat().st_mtime)
                    except Exception:
                        pass
                elif failure_marker.exists():
                    status = "Failed"
                    try:
                        out["last_activity_ts"] = max(out["last_activity_ts"], failure_marker.stat().st_mtime)
                    except Exception:
                        pass

            if status == "Succeeded":
                out["succeeded"] += 1
                out["executed_jobs"] += 1
            elif status == "Running":
                out["running_live"] += 1
                out["executed_jobs"] += 1
            elif status == "Failed":
                out["failed"] += 1
                out["executed_jobs"] += 1
            elif status == "Scheduled":
                out["scheduled"] += 1
            # Anything else (Idle / Pending / unknown) is ignored.

        if out["running_live"] > 0:
            out["live_status"] = "running"
        elif out["failed"] > 0:
            out["live_status"] = "failed"
        elif out["succeeded"] > 0 and out["scheduled"] == 0 and out["failed"] == 0 and out["running_live"] == 0:
            out["live_status"] = "done"
        else:
            out["live_status"] = "idle"

        return out

    async def get_job_parameters(self, job_name: str) -> Dict[str, Any]:
        """Get parameters for a specific job instance, initializing if not present."""
        try:
            state = self.state_service.state

            job_model = state.jobs.get(job_name)
            if not job_model:
                # instance_id not found — try to initialize as a singleton job
                try:
                    job_type = JobType.from_string(job_name)
                except ValueError:
                    return {"success": False, "error": f"Unknown job instance: {job_name}"}

                logger.info("Job %s not in state, initializing from template.", job_name)
                template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
                job_star_path = template_base / job_type.value / "job.star"
                state.ensure_job_initialized(
                    job_type, instance_id=job_name, template_path=job_star_path if job_star_path.exists() else None
                )
                job_model = state.jobs.get(job_name)

            if job_model:
                return {"success": True, "params": job_model.model_dump()}
            else:
                return {"success": False, "error": f"Failed to initialize job {job_name}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def update_job_parameters(self, job_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates parameters for a specific job instance and persists to disk.
        """
        try:
            state = self.state_service.state
            job_model = state.jobs.get(job_name)

            if not job_model:
                return {"success": False, "error": f"Job {job_name} not initialized"}

            for key, value in params.items():
                if hasattr(job_model, key):
                    setattr(job_model, key, value)

            await self.state_service.save_project()

            return {"success": True, "params": job_model.model_dump()}

        except Exception as e:
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    async def get_available_jobs(self) -> List[str]:
        # --- FIX: Use config root instead of cwd ---
        template_path = self.config_service.crboost_root / "config" / "Schemes" / "warp_tomo_prep"
        if not template_path.is_dir():
            return []
        jobs = sorted([p.name for p in template_path.iterdir() if p.is_dir()])
        return jobs

    async def create_project_and_scheme(
        self,
        project_name: str,
        project_base_path: str,
        selected_jobs: List[str],
        movies_glob: str,
        mdocs_glob: str,
        selected_mdoc_paths: Optional[List[str]] = None,
        import_summary: Optional[Dict[str, Any]] = None,
        detected_params: Optional[Dict[str, Any]] = None,
        is_aggregation: bool = False,
    ):
        return await self.project_service.initialize_new_project(
            project_name=project_name,
            project_base_path=project_base_path,
            selected_jobs=selected_jobs,
            movies_glob=movies_glob,
            mdocs_glob=mdocs_glob,
            selected_mdoc_paths=selected_mdoc_paths,
            import_summary=import_summary,
            detected_params=detected_params,
            is_aggregation=is_aggregation,
        )

    async def get_initial_parameters(self) -> Dict[str, Any]:
        """Returns a dump of the current project state."""
        return self.state_service.state.model_dump(mode="json", exclude={"project_path"})

    async def autodetect_parameters(self, mdocs_glob: str) -> Dict[str, Any]:
        from services.configs.mdoc_service import get_mdoc_service

        mdoc_data = await asyncio.to_thread(get_mdoc_service().get_autodetect_params, mdocs_glob)
        return {
            "microscope": {
                "pixel_size_angstrom": mdoc_data.get("pixel_spacing"),
                "acceleration_voltage_kv": mdoc_data.get("voltage"),
                "spherical_aberration_mm": None,
            },
            "acquisition": {
                "dose_per_tilt": mdoc_data.get("dose_per_tilt"),
                "tilt_axis_degrees": mdoc_data.get("tilt_axis_angle"),
            },
        }

    async def parse_dataset_overview(self, mdocs_glob: str, frames_dir: Optional[str] = None, progress_cb=None):
        from services.configs.dataset_parsing_service import get_dataset_parsing_service

        service = get_dataset_parsing_service()
        overview = await asyncio.to_thread(service.parse_dataset, mdocs_glob, frames_dir, progress_cb)
        return overview

    async def run_shell_command(
        self, command: str, cwd: Path = None, tool_name: str = None, additional_binds: List[str] = None
    ):
        """Runs a shell command, optionally using specified tool's container."""
        try:
            if tool_name:
                logger.debug("Running command with tool: %s", tool_name)
                final_command = self.container_service.wrap_command_for_tool(
                    command=command,
                    cwd=cwd or self.server_dir,
                    tool_name=tool_name,
                    additional_binds=additional_binds or [],
                )
            else:
                final_command = command
                logger.info("Running natively: %s", final_command)

            process = await asyncio.create_subprocess_shell(
                final_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd or self.server_dir,
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120.0)

                logger.debug("Process completed with return code: %s", process.returncode)
                if process.returncode == 0:
                    return {"success": True, "output": stdout.decode(), "error": None}
                else:
                    return {"success": False, "output": stdout.decode(), "error": stderr.decode()}

            except asyncio.TimeoutError:
                logger.error("Command timed out after 120 seconds: %s", final_command)
                process.terminate()
                await process.wait()
                return {"success": False, "output": "", "error": "Command execution timed out"}

        except Exception as e:
            logger.error("Exception in run_shell_command: %s", e)
            return {"success": False, "output": "", "error": str(e)}

    async def get_pipeline_overview(self, project_path: str):
        """Gets a high-level overview and detailed statuses of all jobs."""
        return await self.pipeline_runner.get_pipeline_overview(project_path)

    async def get_job_logs(self, project_path: str, job_name: str) -> Dict[str, str]:
        """Gets logs for a specific job *path* (e.g., "External/job003/")."""
        return await self.pipeline_runner.get_job_logs(project_path, job_name)

    async def get_eer_frames_per_tilt(self, eer_file_path: str) -> int:
        try:
            command = f"header {eer_file_path}"
            result = await self.run_shell_command(command)

            if result["success"]:
                output = result["output"]
                for line in output.split("\n"):
                    if "Number of columns, rows, sections" in line:
                        parts = line.split(".")[-1].strip().split()
                        if len(parts) >= 3:
                            return int(parts[2])
            return None
        except Exception as e:
            print(f"Error getting EER frames: {e}")
            return None

    async def load_existing_project(self, project_path: str) -> Dict[str, Any]:
        return await self.project_service.load_project_state(project_path)

    async def debug_pipeline_status(self, project_path: str):
        """Debug method to check pipeline status directly"""
        pipeline_star = Path(project_path) / "default_pipeline.star"

        if not pipeline_star.exists():
            return {"error": "Pipeline file not found"}

        try:
            with open(pipeline_star, "r") as f:
                content = f.read()

            return {"success": True, "content": content, "exists": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

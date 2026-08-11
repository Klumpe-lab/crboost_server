from __future__ import annotations
import asyncio
import getpass
import json
import logging
import pwd
import shlex
import shutil
from pathlib import Path
from typing import Any
from datetime import datetime

from services.templating.template_service import TemplateService
from services.templating.pdb_service import PDBService
from services.scheduling_and_orchestration.project_service import ProjectService
from services.scheduling_and_orchestration.pipeline_orchestrator_service import PipelineOrchestratorService
from services.computing.container_service import get_container_service
from services.scheduling_and_orchestration.pipeline_runner import PipelineRunnerService
from services.scheduling_and_orchestration.pipeline_monitor import PipelineMonitor
from services.project_state import get_state_service
from services.computing.slurm_service import SlurmService
from services.configs.config_service import get_config_service
from services.curation.session_service import CurationSessionService
from services.tilt_series import TiltSeriesRegistry, get_registry_for

logger = logging.getLogger(__name__)

# The process-wide backend. main.py constructs one at startup; UI surfaces opened
# without a threaded-through reference (e.g. the tomo dashboard, which is a function
# module) reach it via get_backend(). Same idiom as get_config_service()/get_state_service().
_backend_instance: CryoBoostBackend | None = None


def get_backend() -> CryoBoostBackend | None:
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
        self.curation_service = CurationSessionService(
            server_dir=self.server_dir, username=self.username, slurm_service=self.slurm_service
        )

    def registry_for(self, project_path: Path) -> TiltSeriesRegistry:
        """TiltSeriesRegistry for a project. Lazily loaded from sidecar JSON
        at `{project_path}/registry/`. Empty for legacy projects without a
        registry on disk — populate via project_service on import/load."""
        return get_registry_for(project_path)

    async def start_pipeline(
        self, project_path: str, scheme_name: str, selected_jobs: list[str], required_paths: list[str]
    ):
        """
        Unified pipeline start method.
        selected_jobs is a list of instance_id strings (e.g. ['tsReconstruct', 'templatematching__ribosome']).
        """
        return await self.pipeline_orchestrator.deploy_and_run_scheme(
            project_dir=Path(project_path), selected_instance_ids=selected_jobs
        )

    async def delete_job(self, job_name: str, project_path: Path, instance_id: str | None = None) -> dict[str, Any]:
        return await self.project_service.delete_job(job_name, project_path=project_path, instance_id=instance_id)

    async def submit_tilt_filter_dl(self, project_path: Path, instance_id: str) -> dict[str, Any]:
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

    async def extract_pick_list(
        self,
        project_path: Path,
        candidate_optset: Path,
        list_star: Path,
        tomo_name: str,
        species_id: str,
        slug: str,
        *,
        box_size: int = 384,
        binning: float = 1.0,
        crop_size: int = 224,
        max_dose: float = -1.0,
        min_frames: int = 1,
        do_stack2d: bool = True,
        do_float16: bool = True,
    ) -> dict[str, Any]:
        """Subtomo-extract ONE curation pick list (Slice C): submit
        ``drivers/extract_pick_list.py`` as a one-off SLURM job via ``config/qsub.sh``
        (same mechanism as ``submit_tilt_filter_dl``). Output lands in
        ``<list_star dir>/<slug>/`` so lists extract independently. Returns the SLURM
        job id + the dir to watch (``RELION_JOB_EXIT_SUCCESS/FAILURE`` + ``result.json``
        appear there; the caller records ``PickList.mark_extracted`` on success).

        Box/bin/crop default to the RELION subtomo defaults but the caller passes the
        species' subtomo-job params so a list extracts compatibly with the auto set."""
        project_path = Path(project_path)
        out_dir = Path(list_star).parent / slug
        out_dir.mkdir(parents=True, exist_ok=True)
        for marker in ("RELION_JOB_EXIT_SUCCESS", "RELION_JOB_EXIT_FAILURE", "result.json"):
            (out_dir / marker).unlink(missing_ok=True)
        # A user-initiated (re-)extract must re-cut from scratch: clear any prior
        # extraction output so drivers/extract_pick_list.py does NOT hit its idempotency
        # skip (out/particles.star + out/Subtomograms/) and silently reuse stale
        # subtomograms for a now-curated pick set. A genuine SLURM requeue reruns
        # run_extract.sh directly (bypassing this) and still benefits from that skip.
        shutil.rmtree(out_dir / "out", ignore_errors=True)

        python_exe = self.server_dir / "venv" / "bin" / "python3"
        if not python_exe.exists():
            python_exe = "python3"
        script_path = self.server_dir / "drivers" / "extract_pick_list.py"
        flags = ""
        if do_stack2d:
            flags += " --stack2d"
        if do_float16:
            flags += " --float16"
        driver_cmd = (
            f"export PYTHONPATH={self.server_dir}:${{PYTHONPATH}}; "
            f"{python_exe} {script_path} "
            f"--candidate-optset {shlex.quote(str(candidate_optset))} "
            f"--list-star {shlex.quote(str(list_star))} "
            f"--tomo {shlex.quote(str(tomo_name))} "
            f"--out-dir {shlex.quote(str(out_dir))} "
            f"--project-root {shlex.quote(str(project_path))} "
            f"--box {int(box_size)} --binning {float(binning)} --crop {int(crop_size)} "
            f"--max-dose {float(max_dose)} --min-frames {int(min_frames)}"
            f"{flags}"
        )

        # SLURM resources = the project's defaults (proven to run relion_tomo_subtomo;
        # over-provisioned for one tomo but consistent with the pipeline extraction).
        slurm_cfg = self.state_service.state_for(project_path).slurm_defaults
        constraint = slurm_cfg.constraint.strip("'\"")
        template_text = (self.server_dir / "config" / "qsub.sh").read_text()
        replacements = {
            "XXXextra1XXX": slurm_cfg.partition,
            "XXXextra2XXX": constraint,
            "XXXextra3XXX": str(slurm_cfg.nodes),
            "XXXextra4XXX": str(slurm_cfg.ntasks_per_node),
            "XXXextra5XXX": str(slurm_cfg.cpus_per_task),
            "XXXextra6XXX": slurm_cfg.gres,
            "XXXextra7XXX": slurm_cfg.mem,
            "XXXextra8XXX": slurm_cfg.time,
            "XXXoutfileXXX": str(out_dir / "run.out"),
            "XXXerrfileXXX": str(out_dir / "run.err"),
            "XXXcommandXXX": driver_cmd,
        }
        script = template_text
        for placeholder, value in replacements.items():
            script = script.replace(placeholder, value)
        sbatch_path = out_dir / "run_extract.sh"
        sbatch_path.write_text(script)
        sbatch_path.chmod(0o755)

        try:
            proc = await asyncio.create_subprocess_exec(
                "sbatch",
                str(sbatch_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(out_dir),
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                return {"success": False, "error": f"sbatch failed: {stderr.decode().strip()}"}
            output = stdout.decode().strip()
            slurm_job_id = output.split()[-1] if output else None
            logger.info("Per-list extraction submitted: SLURM job %s (%s/%s)", slurm_job_id, species_id, slug)
            return {"success": True, "slurm_job_id": slurm_job_id, "out_dir": str(out_dir)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def get_authoritative_extraction_status(self, project_path: Path, species_id: str) -> list[dict[str, Any]]:
        """Read-only: per-(species, tomo) authoritative-list extraction status — the
        aggregation gate's input (see docs/LIST_EXTRACTION_AND_AGGREGATION.md
        §8.1). Returns one dict per tomo: {species_id, tomo_name, slug, kind, extraction_state,
        optset_path, kept, total, notes}. Disk reads run off the event loop."""
        from dataclasses import asdict
        from services.aggregation_authoritative import enumerate_authoritative

        state = self.state_service.state_for(Path(project_path))
        handles = await asyncio.to_thread(enumerate_authoritative, state, Path(project_path), species_id)
        return [{**asdict(h), "extraction_state": h.extraction_state.value} for h in handles]

    async def get_authoritative_gate_report(self, project_path: Path, species_id: str) -> dict[str, Any]:
        """Read-only §8.3 gate: classify each (species, tomo) authoritative list as
        ready / pending (workbench NOT_EXTRACTED|STALE — extractable here) / blocked, so the
        UI/user sees exactly what must be extracted before a species roll-up. No side effects.
        See docs/LIST_EXTRACTION_AND_AGGREGATION.md §8.3."""
        from services.aggregation_authoritative import compute_gate_report, enumerate_authoritative

        state = self.state_service.state_for(Path(project_path))
        handles = await asyncio.to_thread(enumerate_authoritative, state, Path(project_path), species_id)
        return compute_gate_report(handles).to_dict()

    # ── Tomogram import (PARTICLES-header utility; a project-level artifact, not a job) ──

    async def list_tomogram_candidates(self, directory_or_glob: str) -> list[str]:
        """Resolve a directory OR a glob to a sorted list of .mrc file paths (off the
        event loop). A bare directory lists its ``*.mrc``; a glob is expanded directly."""

        def _list() -> list[str]:
            import glob as _glob

            s = str(Path(directory_or_glob).expanduser()) if directory_or_glob else ""
            if not s:
                return []
            if Path(s).is_dir():
                return sorted(str(p) for p in Path(s).glob("*.mrc") if p.is_file())
            return sorted(p for p in _glob.glob(s) if Path(p).is_file())

        return await asyncio.to_thread(_list)

    async def probe_tomogram_metadata(self, paths: list[str]) -> list[dict[str, Any]]:
        """MRC header metadata (dims, voxel size, has_voxel_size) for the import preview,
        read off the event loop. Surfaces a missing voxel size so the widget can flag it
        before commit (never silently defaults apix)."""
        from services.tomogram_import import probe_mrc_metadata

        return await asyncio.to_thread(probe_mrc_metadata, [Path(p) for p in paths])

    async def preview_reference_tomograms(self, reference_star: str) -> list[dict[str, Any]]:
        """Read an existing ``tomograms.star`` (off the event loop) and project its rows
        into the same preview-dict shape ``probe_tomogram_metadata`` returns, so the import
        dialog can show a review table before referencing it. Propagates the
        FileNotFoundError / 'no rlnTomoName block' guards from ``_reference_rows`` to the UI."""
        from services.tomogram_import import preview_reference_metadata

        return await asyncio.to_thread(preview_reference_metadata, reference_star)

    async def commit_imported_tomograms(
        self,
        project_path: Path,
        *,
        mode: str = "synthesize",
        mrc_paths: list[str] | None = None,
        reference_star: str = "",
        pixel_size_angstrom: float = 0.0,
        tomogram_binning: float = 1.0,
        optics_group_name: str = "opticsGroup1",
    ) -> dict[str, Any]:
        """Write ``Tomograms/tomograms.star`` from the selection + record it on
        ``ProjectState.imported_tomograms``. Raises (propagated to the UI) if a selected
        MRC has no voxel size and no pixel-size override — never silently defaults apix.
        Microscope/optics values come from the project's microscope + acquisition params."""
        from services.tomogram_import import write_tomograms_star
        from services.project_state import ImportedTomograms

        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        out_rel = Path("Tomograms") / "tomograms.star"
        out_abs = project_path / out_rel

        count = await asyncio.to_thread(
            write_tomograms_star,
            out_abs,
            mode=mode,
            mrc_paths=[Path(p) for p in (mrc_paths or [])],
            reference_star=reference_star,
            pixel_size_angstrom=pixel_size_angstrom,
            tomogram_binning=tomogram_binning,
            optics_group_name=optics_group_name,
            voltage=state.microscope.acceleration_voltage_kv,
            spherical_aberration=state.microscope.spherical_aberration_mm,
            amplitude_contrast=state.microscope.amplitude_contrast,
            invert_defocus_hand=state.acquisition.invert_defocus_hand,
            project_tag=project_path.name,
        )

        state.set_imported_tomograms(
            ImportedTomograms(
                star_path=str(out_rel),
                source_mode=mode,
                source_paths=[str(p) for p in (mrc_paths or [])],
                reference_star=reference_star,
                pixel_size_angstrom=pixel_size_angstrom,
                tomogram_binning=tomogram_binning,
                optics_group_name=optics_group_name,
                count=int(count),
            )
        )
        await self.state_service.save_project(project_path=project_path, force=True)
        return {"count": int(count), "star_path": str(out_abs)}

    async def _await_extraction_outdirs(self, out_dirs: list[str], timeout_s: int) -> dict[str, tuple]:
        """Poll each per-list extraction out dir (RELION_JOB_EXIT_* + result.json) until all
        resolve or ``timeout_s`` elapses. Returns {out_dir: ("done"|"failed", data)} for the
        resolved ones (an out dir absent from the result = still running). Disk scans run off
        the event loop. Mirrors the per-list watcher in tomo_dashboard_dialog._handle_extract_list."""
        import json as _json

        pending = set(out_dirs)
        results: dict[str, tuple] = {}
        if not pending:
            return results

        def _scan(dirs: set) -> dict[str, tuple]:
            done: dict[str, tuple] = {}
            for od in dirs:
                p = Path(od)
                rj = p / "result.json"
                if (p / "RELION_JOB_EXIT_FAILURE").exists():
                    try:
                        done[od] = ("failed", _json.loads(rj.read_text()) if rj.exists() else {})
                    except Exception:
                        done[od] = ("failed", {})
                elif (p / "RELION_JOB_EXIT_SUCCESS").exists() or rj.exists():
                    try:
                        done[od] = ("done", _json.loads(rj.read_text()) if rj.exists() else {})
                    except Exception:
                        done[od] = ("done", {})
            return done

        for _ in range(max(1, timeout_s // 10)):
            if not pending:
                break
            done = await asyncio.to_thread(_scan, set(pending))
            for od, r in done.items():
                results[od] = r
                pending.discard(od)
            if not pending:
                break
            await asyncio.sleep(10)
        return results

    async def extract_authoritative_pending(
        self, project_path: Path, species_id: str, *, only_tomos: list[str] | None = None, timeout_s: int = 3600
    ) -> dict[str, Any]:
        """§8.3 auto-extract: submit per-list subtomo extraction for every WORKBENCH
        authoritative list that is NOT_EXTRACTED/STALE (optionally limited to ``only_tomos``),
        wait for completion, record ``mark_extracted``, and persist. Never touches
        'auto'/'filtered' or already-ready lists; idempotent (re-running acts only on
        still-pending lists). Reuses ``extract_pick_list`` (which clears ``out/`` for a clean
        re-cut). RUN INSIDE A BackgroundTask — it polls up to ``timeout_s``.

        Returns {submitted, succeeded:[{tomo,slug,count}], failed:[{tomo,slug,error}],
        blocked:[{tomo,slug,reason}], still_running:[{tomo,slug}], can_proceed}."""
        from services.aggregation_authoritative import (
            compute_gate_report,
            enumerate_authoritative,
            extract_inputs_for_list,
        )

        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        handles = await asyncio.to_thread(enumerate_authoritative, state, project_path, species_id)
        pending = compute_gate_report(handles).pending
        if only_tomos is not None:
            keep = set(only_tomos)
            pending = [h for h in pending if h.tomo_name in keep]

        submitted: list[dict] = []
        failed: list[dict] = []
        blocked: list[dict] = []
        watching: list[dict] = []  # {tomo, slug, out_dir}
        for h in pending:
            pl = state.get_pick_list(h.slug, species_id, h.tomo_name)
            inputs = extract_inputs_for_list(state, project_path, species_id, pl) if pl is not None else None
            if inputs is None:
                blocked.append({"tomo": h.tomo_name, "slug": h.slug, "reason": "no candidate/subtomo job or list star"})
                continue
            res = await self.extract_pick_list(
                project_path,
                Path(inputs["candidate_optset"]),
                Path(inputs["list_star"]),
                h.tomo_name,
                species_id,
                h.slug,
                **inputs["params"],
            )
            if not res.get("success"):
                failed.append({"tomo": h.tomo_name, "slug": h.slug, "error": res.get("error")})
                continue
            submitted.append({"tomo": h.tomo_name, "slug": h.slug, "slurm_job_id": res.get("slurm_job_id")})
            watching.append({"tomo": h.tomo_name, "slug": h.slug, "out_dir": res["out_dir"]})

        results = await self._await_extraction_outdirs([w["out_dir"] for w in watching], timeout_s)

        succeeded: list[dict] = []
        still_running: list[dict] = []
        state = self.state_service.state_for(project_path)
        changed = False
        for w in watching:
            r = results.get(w["out_dir"])
            if r is None:
                still_running.append({"tomo": w["tomo"], "slug": w["slug"]})
                continue
            status, data = r
            if status == "done" and data.get("ok"):
                pl = state.get_pick_list(w["slug"], species_id, w["tomo"])
                if pl is not None:
                    pl.mark_extracted(data["optimisation_set"], int(data.get("count", 0)))
                    changed = True
                succeeded.append({"tomo": w["tomo"], "slug": w["slug"], "count": int(data.get("count", 0))})
            else:
                failed.append(
                    {"tomo": w["tomo"], "slug": w["slug"], "error": data.get("error") or "extraction job failed"}
                )
        if changed:
            state.mark_dirty()
            await self.state_service.save_project(project_path=project_path, force=True)

        handles2 = await asyncio.to_thread(enumerate_authoritative, state, project_path, species_id)
        return {
            "submitted": len(submitted),
            "succeeded": succeeded,
            "failed": failed,
            "blocked": blocked,
            "still_running": still_running,
            "can_proceed": compute_gate_report(handles2).can_proceed,
        }

    # ── ChimeraX + ArtiaX curation — thin delegation; logic + docs live in
    # services/curation/session_service.py (CurationSessionService) ─────────────

    async def launch_curation_session(
        self, project_path: Path | None = None, cxc_path: Path | None = None
    ) -> dict[str, Any]:
        return await self.curation_service.launch_curation_session(project_path, cxc_path=cxc_path)

    async def get_curation_session_info(self, session_dir: str, slurm_job_id: str | None = None) -> dict[str, Any]:
        return await self.curation_service.get_curation_session_info(session_dir, slurm_job_id)

    async def stop_curation_session(self, slurm_job_id: str | None, project_path: Path | None = None) -> dict[str, Any]:
        return await self.curation_service.stop_curation_session(slurm_job_id, project_path=project_path)

    async def find_active_curation_session(self, project_path: Path) -> dict[str, Any] | None:
        return await self.curation_service.find_active_curation_session(project_path)

    async def find_active_curation_session_any(self) -> dict[str, Any] | None:
        return await self.curation_service.find_active_curation_session_any()

    async def send_chimerax_command(
        self, session_info: dict[str, Any], command: str, *, timeout: float = 60.0
    ) -> dict[str, Any]:
        return await self.curation_service.send_chimerax_command(session_info, command, timeout=timeout)

    async def save_session_particle_lists(self, session_info: dict[str, Any], dest_dir: Path) -> dict[str, Any]:
        return await self.curation_service.save_session_particle_lists(session_info, dest_dir)

    async def save_curation_picks(
        self,
        session_info: dict[str, Any],
        *,
        project_path: Path | None = None,
        tomo_name: str = "",
        species_id: str = "",
        species_label: str = "",
    ) -> dict[str, Any]:
        return await self.curation_service.save_curation_picks(
            session_info,
            project_path=project_path,
            tomo_name=tomo_name,
            species_id=species_id,
            species_label=species_label,
        )

    async def load_into_session(
        self,
        session_info: dict[str, Any],
        project_path: Path,
        candidates_star: Path,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str = "",
        *,
        species_id: str = "",
        source_star: Path | None = None,
        coords_label: str = "auto",
        save_first: bool = False,
    ) -> dict[str, Any]:
        return await self.curation_service.load_into_session(
            session_info,
            project_path,
            candidates_star,
            tomograms_star,
            tomo_name,
            species_label,
            species_id=species_id,
            source_star=source_star,
            coords_label=coords_label,
            save_first=save_first,
        )

    def get_curation_loaded(self, session_info: dict[str, Any]) -> dict[str, Any] | None:
        return self.curation_service.get_curation_loaded(session_info)

    async def prepare_curation_bundle(
        self,
        project_path: Path,
        candidates_star: Path,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str = "",
        *,
        species_id: str = "",
        source_star: Path | None = None,
        coords_label: str = "auto",
    ) -> dict[str, Any]:
        return await self.curation_service.prepare_curation_bundle(
            project_path,
            candidates_star,
            tomograms_star,
            tomo_name,
            species_label,
            species_id=species_id,
            source_star=source_star,
            coords_label=coords_label,
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
    ) -> dict[str, Any]:
        return await self.curation_service.import_curation_picks(
            project_path, tomograms_star, tomo_name, species_label, species_id, coords_path=coords_path
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
        return await self.curation_service.merge_pick_lists(
            project_path, species_id, species_label, tomo_name, sources, out_slug
        )

    async def list_clash_stats(self, star_path: Path, tomo_name: str, radius_ang: float) -> dict[str, Any]:
        return await self.curation_service.list_clash_stats(star_path, tomo_name, radius_ang)

    async def deduplicate_pick_list(self, star_path: Path, tomo_name: str, radius_ang: float) -> dict[str, Any]:
        return await self.curation_service.deduplicate_pick_list(star_path, tomo_name, radius_ang)

    async def get_default_data_globs(self) -> dict[str, str]:
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

    async def scan_for_projects(self, base_path: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._scan_for_projects_sync, base_path)

    async def read_project_state_detached(self, project_path: str):
        """Load a project's ProjectState from disk for read-only preview WITHOUT
        registering it in the shared in-memory registry (so the central pipeline
        observer / sync never picks up a merely-previewed project). Used by the
        project hub's left-hand params panel."""
        from services.project_state import ProjectState

        params_file = Path(project_path) / "project_params.json"
        return await asyncio.to_thread(ProjectState.load, params_file)

    def _scan_for_projects_sync(self, base_path: str) -> list[dict[str, Any]]:
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
                    owner_raw = None
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
                        owner_raw = data.get("owner")
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
                            "owner": owner_raw,
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
    def _derive_live_status(project_dir: Path, jobs_dict: dict[str, Any]) -> dict[str, Any]:
        """Derive pipeline status for the roster from project_params.json's
        `jobs` dict (canonical: includes tiltFilter and other non-RELION
        jobs that aren't in default_pipeline.star). Disk RELION_JOB_EXIT_*
        markers are only used to reconcile in-memory `Running` statuses
        that the schemer crashed before persisting -- otherwise the project
        state is the source of truth."""
        out: dict[str, Any] = {
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

    async def get_available_jobs(self) -> list[str]:
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
        selected_jobs: list[str],
        movies_glob: str,
        mdocs_glob: str,
        selected_mdoc_paths: list[str] | None = None,
        import_summary: dict[str, Any] | None = None,
        detected_params: dict[str, Any] | None = None,
        is_aggregation: bool = False,
        shared: bool = False,
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
            shared=shared,
        )

    async def transfer_project_ownership(self, project_path: Path, new_owner: str | None) -> dict[str, Any]:
        """Reassign a project's owner WITHOUT moving it on disk.

        `owner` is pure attribution/grouping metadata; the on-disk location and
        the path-keyed state registry are untouched. We resolve via state_for()
        so the live in-memory state is mutated when the project is open in this
        process (a blind JSON rewrite would be clobbered on its next save).

        new_owner: a username, SHARED_OWNER ("@lab") for the shared/lab area, or
        None to revert to the original creator. There is no access control
        anywhere (identity is the OS user of each per-user process on a shared
        filesystem), so this is honor-system attribution; cross-process
        concurrency is the same unhandled race as any other concurrent edit.
        """
        try:
            path = Path(project_path)
            state = self.state_service.state_for(path)
            state.owner = new_owner or None
            state.update_modified()
            state.mark_dirty()
            await self.state_service.save_project(project_path=path, force=True)
            return {"success": True, "owner": state.owner}
        except Exception as e:
            logger.error("Failed to transfer ownership of %s: %s", project_path, e)
            return {"success": False, "error": str(e)}

    async def autodetect_parameters(self, mdocs_glob: str) -> dict[str, Any]:
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

    async def parse_dataset_overview(self, mdocs_glob: str, frames_dir: str | None = None, progress_cb=None):
        from services.configs.dataset_parsing_service import get_dataset_parsing_service

        service = get_dataset_parsing_service()
        overview = await asyncio.to_thread(service.parse_dataset, mdocs_glob, frames_dir, progress_cb)
        return overview

    async def run_shell_command(
        self,
        command: str,
        cwd: Path | None = None,
        tool_name: str | None = None,
        additional_binds: list[str] | None = None,
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

            except TimeoutError:
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

    async def get_job_logs(self, project_path: str, job_name: str) -> dict[str, str]:
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

    async def load_existing_project(self, project_path: str) -> dict[str, Any]:
        return await self.project_service.load_project_state(project_path)

    async def debug_pipeline_status(self, project_path: str):
        """Debug method to check pipeline status directly"""
        pipeline_star = Path(project_path) / "default_pipeline.star"

        if not pipeline_star.exists():
            return {"error": "Pipeline file not found"}

        try:
            with open(pipeline_star) as f:
                content = f.read()

            return {"success": True, "content": content, "exists": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

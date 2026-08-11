import logging
import os
import pandas as pd
from pathlib import Path
from typing import Any, ClassVar
from datetime import datetime

from services.computing.slurm_service import normalize_slurm_ids
from services.configs.config_service import get_config_service
from services.configs.starfile_service import StarfileService
from services.job_models import ImportMoviesParams
from services.path_resolution_service import PathResolutionError, PathResolutionService, get_context_paths
from services.project_state import AbstractJobParams, JobCategory, JobType, JobStatus
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend import CryoBoostBackend

logger = logging.getLogger(__name__)


def _toposort_submit_order(nodes: list[str], edges: list[tuple]) -> tuple:
    """Kahn's topological sort over (producer, consumer) edges restricted to ``nodes``.

    Returns ``(ordered_nodes, predecessors)`` where ``predecessors[c]`` is the set of
    producer instance_ids of ``c`` within ``nodes``. Raises ``ValueError`` on a cycle.
    Seeds the queue in ``nodes`` order so independent jobs keep their pipeline/UI order.
    """
    from collections import deque

    nodeset = set(nodes)
    indeg = {n: 0 for n in nodes}
    succ: dict[str, list[str]] = {n: [] for n in nodes}
    preds: dict[str, set] = {n: set() for n in nodes}
    for producer, consumer in edges:
        if producer not in nodeset or consumer not in nodeset or producer == consumer:
            continue
        if producer in preds[consumer]:
            continue  # dedup (resolve_edges already dedups; defensive)
        preds[consumer].add(producer)
        succ[producer].append(consumer)
        indeg[consumer] += 1

    queue = deque([n for n in nodes if indeg[n] == 0])
    order: list[str] = []
    while queue:
        n = queue.popleft()
        order.append(n)
        for m in succ[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)

    if len(order) != len(nodes):
        stuck = nodeset - set(order)
        raise ValueError(f"cycle among {sorted(stuck)}")
    return order, preds


class PipelineOrchestratorService:
    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()
        self.job_resolver = JobTypeResolver(self.star_handler)

    async def deploy_and_run_scheme(self, project_dir: Path, selected_instance_ids: list[str]) -> dict[str, Any]:
        if not selected_instance_ids:
            return {"success": False, "error": "No jobs selected."}

        state = self.backend.state_service.state_for(project_dir)

        # Guard FIRST -- before any state mutation.
        # _run_relion_schemer has the same guard but it fires too late:
        # by the time it's reached, relion_job_name and job_path_mapping
        # have already been clobbered, which breaks sync_all_jobs path resolution.
        if state.pipeline_active:
            return {
                "success": False,
                "error": "Pipeline is already running. Wait for it to complete or cancel it first.",
            }

        # Persist the participating set + order for this run (P1.0 of the orchestrator
        # rework) so a tab-less submitter/reconciler has the pipeline membership
        # without UIState. The FULL selected set, in UI order (incl. already-finished
        # upstreams, which the afterok DAG needs as producers). The save on the
        # retry/fresh paths below persists it.
        state.pipeline_order = list(selected_instance_ids)
        state.mark_dirty()

        instances_to_run: list[str] = []
        for instance_id in selected_instance_ids:
            job_model = state.jobs.get(instance_id)
            if not job_model or job_model.execution_status != JobStatus.SUCCEEDED:
                # Interactive jobs are never dispatched to the cluster.
                if job_model and getattr(job_model, "IS_INTERACTIVE", False):
                    continue
                instances_to_run.append(instance_id)

        if not instances_to_run:
            return {
                "success": True,
                "already_complete": True,
                "message": "All selected jobs are already finished.",
                "pid": 0,
            }

        # Orchestrator rework (P1.A): when the project opts into the afterok DAG, submit the WHOLE
        # remaining set (FAILED + fresh) directly via SLURM dependencies. _submit_chain allocates
        # fresh External/jobNNN dirs and rewires the afterok edges, so afterok re-runs do NOT use the
        # schemer in-place retry path below; edges to already-SUCCEEDED producers are dropped (their
        # outputs exist on disk). Dormant unless the flag is set; the schemer path is unchanged when off.
        if state.use_afterok_orchestrator:
            return await self._submit_chain(project_dir=project_dir, instances_to_run=instances_to_run, state=state)

        # Schemer path -- Partition: jobs that FAILED with an existing External/jobNNN dir get
        # re-sbatched in place (preserves .task_status/*.ok for per-TS skip). Fresh jobs go through
        # the schemer as before. If both exist, retries run first; the monitor hands off on success.
        retry_ids: list[str] = []
        fresh_ids: list[str] = []
        for iid in instances_to_run:
            job_model = state.jobs.get(iid)
            if (
                job_model
                and getattr(job_model, "relion_job_name", None)
                and job_model.execution_status == JobStatus.FAILED
            ):
                retry_ids.append(iid)
            else:
                fresh_ids.append(iid)

        if retry_ids:
            return await self.backend.pipeline_runner.launch_retries(
                project_dir=project_dir, retry_instance_ids=retry_ids, on_success_fresh_ids=fresh_ids
            )

        instances_to_run = fresh_ids

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        scheme_name = f"run_{timestamp}"
        scheme_dir = project_dir / "Schemes" / scheme_name
        scheme_dir.mkdir(parents=True, exist_ok=True)

        current_counter = self._get_current_relion_counter(project_dir)
        server_dir = Path(__file__).parent.parent.parent.resolve()

        resolver = PathResolutionService(state, active_instance_ids=set(instances_to_run))

        report_lines = [f"Scheme: {scheme_name}", f"Instances_to_run: {instances_to_run}", ""]

        next_job_num = current_counter
        for _i, instance_id in enumerate(instances_to_run):
            job_model = state.jobs.get(instance_id)
            if not job_model:
                base_type_str = instance_id.split("__")[0]
                try:
                    job_type = JobType(base_type_str)
                except ValueError:
                    report_lines.append(f"[{instance_id}] UNKNOWN JOB TYPE, skipping")
                    continue
                template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep" / job_type.value / "job.star"
                state.ensure_job_initialized(job_type, instance_id=instance_id, template_path=template_base)
                job_model = state.jobs.get(instance_id)

            job_type = job_model.job_type
            category = JobCategory.IMPORT if job_type == JobType.IMPORT_MOVIES else JobCategory.EXTERNAL

            # Reuse the existing job directory on re-run (e.g. after partial failure)
            # so that .task_status/*.ok files from the previous run are preserved
            # and already-succeeded array tasks can be skipped.
            existing_rjn = getattr(job_model, "relion_job_name", None)
            if existing_rjn:
                predicted_job_dir = project_dir / existing_rjn.rstrip("/")
                # Clean stale exit markers so the schemer doesn't see old results
                for marker in ["RELION_JOB_EXIT_SUCCESS", "RELION_JOB_EXIT_FAILURE"]:
                    (predicted_job_dir / marker).unlink(missing_ok=True)
            else:
                predicted_job_dir = project_dir / category.value / f"job{next_job_num:03d}"
                next_job_num += 1

            try:
                io_paths = resolver.resolve_all_paths(job_type, job_model, predicted_job_dir, instance_id=instance_id)
                context_paths = get_context_paths(job_type, job_model, predicted_job_dir)
                resolved_paths = {**context_paths, **io_paths}
            except PathResolutionError as e:
                job_model.is_orphaned = True
                job_model.missing_inputs = [str(e)]
                report_lines.append(f"[{instance_id}] RESOLUTION FAILED: {e}")
                report_lines.append("")
                logger.info("Resolution failed for %s: %s", instance_id, e)
                continue

            job_model.paths = {k: str(v) for k, v in resolved_paths.items() if v is not None}
            job_model.is_orphaned = False
            job_model.missing_inputs = []

            # Write predicted path into job_path_mapping immediately at deploy time.
            # This makes Pass 2 of sync_all_jobs authoritative before the job has run,
            # which is the only reliable disambiguation when multiple instances of the
            # same job type exist.
            predicted_rel = str(predicted_job_dir.relative_to(project_dir))
            state.job_path_mapping[instance_id] = predicted_rel

            resolver.invalidate_cache()

            report_lines.append(f"[{instance_id}] predicted_dir={predicted_job_dir}")
            for k, v in sorted(job_model.paths.items()):
                report_lines.append(f"  {k}: {v}")
            report_lines.append("")

            scheme_job_dir = scheme_dir / instance_id
            scheme_job_dir.mkdir(parents=True, exist_ok=True)

            self._write_job_star(
                scheme_job_dir=scheme_job_dir,
                instance_id=instance_id,
                job_type=job_type,
                job_model=job_model,
                server_dir=server_dir,
                project_dir=project_dir,
            )

        report_path = scheme_dir / "resolution_report.txt"
        report_path.write_text("\n".join(report_lines))
        logger.info("Wrote %s", report_path)

        self._write_scheme_star(scheme_dir, scheme_name, instances_to_run)

        os.sync()

        bind_paths = [str(project_dir.parent.resolve()), str(server_dir.resolve())]
        if state.movies_glob:
            bind_paths.append(str(Path(state.movies_glob).parent.resolve()))
        if state.mdocs_glob:
            bind_paths.append(str(Path(state.mdocs_glob).parent.resolve()))

        for instance_id in instances_to_run:
            job_model = state.jobs.get(instance_id)
            if job_model:
                job_model.execution_status = JobStatus.SCHEDULED
                job_model.relion_job_name = None
                job_model.relion_job_number = None

        return await self.backend.pipeline_runner.run_generated_scheme(
            project_dir=project_dir, scheme_name=scheme_name, bind_paths=list(set(bind_paths))
        )

    async def _submit_chain(self, project_dir: Path, instances_to_run: list[str], state) -> dict[str, Any]:
        """P1.A: submit the pipeline as a SLURM afterok DAG instead of via relion_schemer.

        For each fresh job: allocate a stable External/jobNNN dir from the ProjectState
        counter (monotonic, no reuse -> no off-by-one), resolve paths, write the RELION-compat
        job.star, and render run_submit.script. A FAILED job that already owns a dir is the one
        exception: it is re-submitted in place on that dir (no counter slot consumed) so its
        `.task_status/*.ok` survives and the supervisor reruns only the failed/missing items.
        Then toposort resolve_edges() and sbatch each supervisor with --dependency=afterok on
        its producers' supervisor job ids.

        This method SUBMITS and persists slurm_job_id + QUEUED + pipeline_active=True. The monitor
        dispatches afterok projects to reconcile_afterok (P1.B) -- which owns live status and winds
        pipeline_active back down on completion -- while sync_all_jobs stays guarded off for them.
        The chain executes in SLURM regardless of the headnode process (the durability win).

        Failure handling: job-dir allocation uses a LOCAL counter committed to ProjectState only
        after a clean prepare + toposort, so an early failure leaks neither the counter nor a
        persisted dir number; whatever reaches SLURM is always persisted, so no slurm_job_id handle
        is lost on a mid-chain sbatch error.
        """
        server_dir = Path(__file__).parent.parent.parent.resolve()

        # Seed the sole job-dir allocator once from the existing RELION counter, then own it.
        # (P1.C removes the star read entirely.)
        if not state.job_dir_counter:
            state.job_dir_counter = max(self._get_current_relion_counter(project_dir), 1)
        # Allocate from a LOCAL counter during prepare; commit to state.job_dir_counter only after
        # the whole prepare + toposort succeeds, so an early failure leaks neither the counter nor a
        # persisted dir number (mirrors the schemer path's discardable local next_job_num).
        next_job_num = state.job_dir_counter

        resolver = PathResolutionService(state, active_instance_ids=set(instances_to_run))

        prepared: dict[str, tuple] = {}  # instance_id -> (job_dir, script_path)
        for instance_id in instances_to_run:
            job_model = state.jobs.get(instance_id)
            if not job_model:
                base_type_str = instance_id.split("__")[0]
                try:
                    job_type = JobType(base_type_str)
                except ValueError:
                    return {"success": False, "error": f"Unknown job type for instance '{instance_id}'"}
                template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep" / job_type.value / "job.star"
                state.ensure_job_initialized(job_type, instance_id=instance_id, template_path=template_base)
                job_model = state.jobs.get(instance_id)

            job_type = job_model.job_type
            category = JobCategory.IMPORT if job_type == JobType.IMPORT_MOVIES else JobCategory.EXTERNAL

            # Dir allocation. Fresh jobs consume a slot from the sole monotonic allocator
            # (job_dir_counter), never reused -- so crboost's numbers can't drift off-by-one.
            # The ONE exception: a FAILED job that already owns a dir is re-submitted IN PLACE on
            # that same dir, so the supervisor's submit_array_job sees the prior `.task_status/*.ok`
            # and reruns only the failed/missing tilt-series instead of all of them. This reuses a
            # dir crboost itself allocated and recorded (relion_job_number) and consumes no counter
            # slot -- categorically unlike the schemer-era off-by-one (H1), which came from a SECOND
            # allocator (RELION's schemer) assigning the next number while we reused a stale one.
            # The afterok path has no second allocator, so in-place reuse here cannot reproduce H1.
            existing_rel = (job_model.relion_job_name or "").rstrip("/")
            reuse_dir = (
                job_model.execution_status == JobStatus.FAILED
                and bool(existing_rel)
                and job_model.relion_job_number is not None
                and (project_dir / existing_rel).is_dir()
            )
            if reuse_dir:
                rel = existing_rel
                job_num = job_model.relion_job_number
                job_dir = project_dir / rel
                logger.info("Reusing FAILED job dir %s for in-place retry of %s", rel, instance_id)
            else:
                job_num = next_job_num
                next_job_num += 1
                rel = f"{category.value}/job{job_num:03d}"
                job_dir = project_dir / rel
            job_dir.mkdir(parents=True, exist_ok=True)
            # Clear stale exit sentinels so the reconciler's pass-1 can't latch a prior run's
            # terminal status. On a reused FAILED dir this is what lets the rerun proceed;
            # `.task_status/*.ok` is deliberately left intact so the supervisor (clean_status_dir
            # keep_ok=True) skips already-done items and resubmits only what failed.
            for _marker in ("RELION_JOB_EXIT_SUCCESS", "RELION_JOB_EXIT_FAILURE"):
                (job_dir / _marker).unlink(missing_ok=True)

            try:
                io_paths = resolver.resolve_all_paths(job_type, job_model, job_dir, instance_id=instance_id)
                context_paths = get_context_paths(job_type, job_model, job_dir)
                resolved_paths = {**context_paths, **io_paths}
            except PathResolutionError as e:
                job_model.is_orphaned = True
                job_model.missing_inputs = [str(e)]
                return {"success": False, "error": f"Path resolution failed for {instance_id}: {e}"}

            job_model.paths = {k: str(v) for k, v in resolved_paths.items() if v is not None}
            job_model.is_orphaned = False
            job_model.missing_inputs = []
            job_model.relion_job_name = rel + "/"
            job_model.relion_job_number = job_num
            state.job_path_mapping[instance_id] = rel
            resolver.invalidate_cache()

            # RELION-compat job.star into the real job dir (P1.C builds the full export on this).
            self._write_job_star(
                scheme_job_dir=job_dir,
                instance_id=instance_id,
                job_type=job_type,
                job_model=job_model,
                server_dir=server_dir,
                project_dir=project_dir,
            )

            # Inline-completed providers (RUNS_INLINE): metadata-only jobs whose output star is
            # written here in-process -- no relion binary, no supervisor, no afterok SLURM job.
            # Mark Succeeded + drop the exit sentinel so the roster/DAG show them and their consumers
            # submit with no afterok producer (the star is on disk before the chain runs). Excluded
            # from `prepared`, hence from the submit set/DAG below. Currently: ImportMovies
            # (tilt_series.star from the registry).
            if getattr(job_model, "RUNS_INLINE", False):
                try:
                    if job_type == JobType.IMPORT_MOVIES:
                        self._write_import_stars_inline(job_model, job_dir, project_dir)
                    else:
                        raise ValueError(f"RUNS_INLINE set but no inline writer for {job_type}")
                except Exception as e:
                    logger.exception("inline writer failed for %s", instance_id)
                    return {"success": False, "error": f"Inline writer failed for {instance_id}: {e}"}
                (job_dir / "RELION_JOB_EXIT_SUCCESS").touch()
                job_model.execution_status = JobStatus.SUCCEEDED
                job_model.slurm_job_id = None
            else:
                fn_exe = self._build_fn_exe(instance_id, job_type, job_model, project_dir, server_dir)
                script_path = self._render_supervisor_script(job_dir, job_model, fn_exe, server_dir)
                prepared[instance_id] = (job_dir, script_path)

        # Derive the dependency DAG, restrict to the SUBMITTED set, toposort (cycle-checked).
        # `prepared` excludes inline-completed jobs (e.g. IMPORT); an edge from such a producer to a
        # submitted consumer drops out here, so the consumer submits with no afterok dep -- correct,
        # since the producer's output is already on disk.
        resolver.invalidate_cache()
        edges = resolver.resolve_edges(instances_to_run)
        submit_ids = list(prepared)
        submit_set = set(submit_ids)
        edges = [(p, c) for (p, c) in edges if p in submit_set and c in submit_set]
        try:
            order, preds = _toposort_submit_order(submit_ids, edges)
        except ValueError as e:
            return {"success": False, "error": f"Pipeline DAG is not acyclic ({e}); cannot submit afterok chain."}

        # Prepare + toposort succeeded -> commit the allocator (these job dirs are now owned).
        state.job_dir_counter = next_job_num

        # Submit each supervisor in topo order, gating on its producers' supervisor ids. On a
        # mid-chain sbatch error, stop the loop but still persist what already reached SLURM below.
        instance_to_slurm: dict[str, str] = {}
        submitted: list[dict[str, Any]] = []
        submit_error: tuple | None = None
        for instance_id in order:
            _job_dir, script_path = prepared[instance_id]
            after_ids = normalize_slurm_ids(
                [instance_to_slurm[p] for p in preds.get(instance_id, set()) if p in instance_to_slurm]
            )
            try:
                slurm_id = await self.backend.pipeline_runner.submit_supervisor(
                    script_path=script_path, cwd=project_dir, after_ids=after_ids or None
                )
            except Exception as e:
                logger.exception("afterok submit failed for %s", instance_id)
                submit_error = (instance_id, e)
                break

            instance_to_slurm[instance_id] = slurm_id
            job_model = state.jobs[instance_id]
            job_model.slurm_job_id = slurm_id
            job_model.execution_status = JobStatus.QUEUED
            submitted.append({"instance_id": instance_id, "slurm_job_id": slurm_id, "afterok": after_ids})
            logger.info("submit_chain: %s -> slurm %s (afterok=%s)", instance_id, slurm_id, after_ids)

        # Mark the run active (so the monitor ticks it -> reconcile_afterok) and persist the
        # committed counter + every submitted handle (on success OR partial failure) so no live
        # SLURM job is left unrecorded and a restart can re-observe the chain. reconcile_afterok
        # owns winding pipeline_active back down when every job reaches a terminal state.
        if submitted:
            state.pipeline_active = True
        state.mark_dirty()
        await self.backend.state_service.save_project(project_path=project_dir, force=True)

        if submit_error is not None:
            failed_iid, err = submit_error
            return {
                "success": False,
                "error": (
                    f"sbatch failed for {failed_iid}: {err}. {len(submitted)} earlier job(s) are queued "
                    f"in SLURM and recorded; cancel them manually until the P1.B reconciler lands."
                ),
                "afterok": True,
                "submitted": submitted,
            }

        return {
            "success": True,
            "message": f"Submitted {len(submitted)} job(s) as a SLURM afterok DAG (schemer-free).",
            "pid": 0,
            "afterok": True,
            "submitted": submitted,
        }

    def _render_supervisor_script(
        self, job_dir: Path, job_model: AbstractJobParams, fn_exe: str, server_dir: Path
    ) -> Path:
        """Render config/qsub.sh -> <job_dir>/run_submit.script for a direct (schemer-free)
        supervisor submit (P1.A).

        Resources come from job_model._get_queue_options() -- the same per-job-type routing the
        schemer reads from job.star (array jobs -> lightweight supervisor_slurm_defaults; single
        jobs -> get_effective_slurm_config). Unlike the array renderer, the RELION_JOB_EXIT marker
        block is KEPT: for a single supervisor job that marker is the completion signal.
        """
        template = (server_dir / "config" / "qsub.sh").read_text()
        opts = dict(job_model._get_queue_options())  # qsub_extra1..8 already routed per job type

        replacements = {
            "XXXextra1XXX": opts.get("qsub_extra1", ""),  # partition
            "XXXextra2XXX": opts.get("qsub_extra2", "").strip("'\""),  # constraint (template wraps it in quotes)
            "XXXextra3XXX": opts.get("qsub_extra3", "1"),  # nodes
            "XXXextra4XXX": opts.get("qsub_extra4", "1"),  # ntasks-per-node
            "XXXextra5XXX": opts.get("qsub_extra5", "1"),  # cpus-per-task
            "XXXextra6XXX": opts.get("qsub_extra6", ""),  # gres
            "XXXextra7XXX": opts.get("qsub_extra7", ""),  # mem
            "XXXextra8XXX": opts.get("qsub_extra8", ""),  # time
            "XXXoutfileXXX": str(job_dir / "run.out"),
            "XXXerrfileXXX": str(job_dir / "run.err"),
            "XXXcommandXXX": fn_exe,
        }
        script = template
        for placeholder, value in replacements.items():
            script = script.replace(placeholder, value)

        out_path = job_dir / "run_submit.script"
        out_path.write_text(script)
        out_path.chmod(0o755)
        return out_path

    def _write_job_star(
        self,
        scheme_job_dir: Path,
        instance_id: str,
        job_type: JobType,
        job_model: AbstractJobParams,
        server_dir: Path,
        project_dir: Path,
        **kwargs,
    ):
        fn_exe = self._build_fn_exe(instance_id, job_type, job_model, project_dir, server_dir)
        job_model.generate_job_star(job_dir=scheme_job_dir, fn_exe=fn_exe, star_handler=self.star_handler)

    def _build_fn_exe(
        self, instance_id: str, job_type: JobType, job_model: AbstractJobParams, project_dir: Path, server_dir: Path
    ) -> str:
        if job_type == JobType.IMPORT_MOVIES:
            return self._build_import_command(job_model)

        driver_map = {
            JobType.FS_MOTION_CTF: "fs_motion_and_ctf.py",
            JobType.TS_IMPORT: "ts_import.py",
            JobType.TS_ALIGNMENT: "ts_alignment.py",
            JobType.MISS_ALIGN: "miss_align.py",
            JobType.TS_CTF: "ts_ctf.py",
            JobType.TILT_FILTER: "tilt_filter.py",
            JobType.TS_RECONSTRUCT: "ts_reconstruct.py",
            JobType.DENOISE_TRAIN: "denoise_train.py",
            JobType.DENOISE_PREDICT: "denoise_predict.py",
            JobType.TEMPLATE_MATCH_PYTOM: "template_match_pytom.py",
            JobType.TEMPLATE_EXTRACT_PYTOM: "extract_candidates_pytom.py",
            JobType.SUBTOMO_EXTRACTION: "subtomo_extraction.py",
            JobType.RECONSTRUCT_PARTICLE: "reconstruct_particle.py",
            JobType.CLASS3D: "class3d.py",
        }

        script = driver_map.get(job_type)
        if not script:
            return "echo 'Unknown Driver'; exit 1"

        python_exe = server_dir / "venv" / "bin" / "python3"
        if not python_exe.exists():
            python_exe = "python3"

        script_path = server_dir / "drivers" / script

        return (
            f"export PYTHONPATH={server_dir}:${{PYTHONPATH}}; "
            f"{python_exe} {script_path} "
            f"--instance_id {instance_id} "
            f"--project_path {project_dir}"
        )

    def _get_current_relion_counter(self, project_dir: Path) -> int:
        """
        Reads the current job counter from default_pipeline.star.
        Raises if the file exists but can't be parsed -- silent failures cause
        job path collisions.
        """
        pipeline_star = project_dir / "default_pipeline.star"

        if not pipeline_star.exists():
            return 0

        data = self.star_handler.read(pipeline_star)
        general = data.get("pipeline_general")

        if general is None:
            raise ValueError(f"pipeline_general block missing from {pipeline_star}")

        if isinstance(general, dict):
            counter = general.get("rlnPipeLineJobCounter")
        elif isinstance(general, pd.DataFrame) and not general.empty:
            if "rlnPipeLineJobCounter" not in general.columns:
                raise ValueError(f"rlnPipeLineJobCounter column missing from {pipeline_star}")
            counter = general["rlnPipeLineJobCounter"].values[0]
        else:
            raise ValueError(f"Unexpected pipeline_general format in {pipeline_star}: {type(general)}")

        if counter is None:
            raise ValueError(f"rlnPipeLineJobCounter not found in {pipeline_star}")

        return int(counter)

    def _write_import_stars_inline(self, job_model: ImportMoviesParams, job_dir: Path, project_dir: Path) -> None:
        """Write ``Import/jobNNN/tilt_series.star`` + per-TS ``tilt_series/<TS>.star`` from the
        TiltSeriesRegistry, reproducing what relion_python_tomo_import would emit (Option B; see
        ORCHESTRATOR_REPLACEMENT_PLAN.md §6a). Runs inline in the server process -- pure metadata,
        no relion binary, no container, no SLURM job. Columns mirror a known-good schemer-produced
        import (the ``post_handedness_fix`` oracle): an 8-col global block + a 6-col per-TS block,
        per-tilt rows in acquisition (tilt_index) order.
        """
        from services.tilt_series import get_registry_for
        from services.configs.mdoc_service import get_mdoc_service

        registry = get_registry_for(project_dir)
        mdoc_service = get_mdoc_service()
        rel = job_model.relion_job_name  # "Import/jobNNN/"
        hand = -1 if job_model.acquisition.invert_defocus_hand else 1

        per_ts_dir = job_dir / "tilt_series"
        per_ts_dir.mkdir(parents=True, exist_ok=True)

        global_rows: list[dict[str, Any]] = []
        for ts in sorted(registry.all_tilt_series(), key=lambda t: t.id):
            # rlnTomoNominalDefocus (mdoc TargetDefocus) is not persisted in the registry; source it
            # from the project mdoc, matched by movie basename. Non-fatal if absent (informational
            # column, not consumed by our pipeline) -> defaults to 0.0.
            target_defocus: dict[str, float] = {}
            try:
                for sec in mdoc_service.parse_mdoc_file(ts.mdoc_path).get("data", []):
                    sub = sec.get("SubFramePath", "").replace("\\", "/")
                    if sub and "TargetDefocus" in sec:
                        try:
                            target_defocus[Path(sub).name] = float(sec["TargetDefocus"])
                        except (TypeError, ValueError):
                            pass
            except Exception:
                logger.warning("import writer: could not read mdoc %s for TS %s", ts.mdoc_path, ts.id)

            frames = sorted(ts.frames, key=lambda f: f.tilt_index)
            per_ts_df = pd.DataFrame(
                {
                    "rlnMicrographMovieName": [f"frames/{f.raw_filename}" for f in frames],
                    "rlnTomoTiltMovieFrameCount": [1 for _ in frames],
                    "rlnTomoNominalStageTiltAngle": [f.nominal_tilt_angle_deg for f in frames],
                    "rlnTomoNominalTiltAxisAngle": [job_model.tilt_axis_angle for _ in frames],
                    "rlnMicrographPreExposure": [f.pre_exposure_e_per_a2 for f in frames],
                    "rlnTomoNominalDefocus": [target_defocus.get(f.raw_filename, 0.0) for f in frames],
                }
            )
            self.star_handler.write({ts.id: per_ts_df}, per_ts_dir / f"{ts.id}.star")

            global_rows.append(
                {
                    "rlnTomoName": ts.id,
                    "rlnTomoTiltSeriesStarFile": f"{rel}tilt_series/{ts.id}.star",
                    "rlnVoltage": job_model.voltage,
                    "rlnSphericalAberration": job_model.spherical_aberration,
                    "rlnAmplitudeContrast": job_model.amplitude_contrast,
                    "rlnMicrographOriginalPixelSize": job_model.pixel_size,
                    "rlnTomoHand": hand,
                    "rlnOpticsGroupName": job_model.optics_group_name,
                }
            )

        if not global_rows:
            raise RuntimeError(f"TiltSeriesRegistry for {project_dir} is empty -- cannot write import tilt_series.star")

        self.star_handler.write({"global": pd.DataFrame(global_rows)}, job_dir / "tilt_series.star")
        logger.info("import writer: wrote tilt_series.star + %d per-TS stars into %s", len(global_rows), job_dir)

    def _build_import_command(self, params: ImportMoviesParams) -> str:
        # IMPORT is written inline by _write_import_stars_inline (Option B, §6a). This fn_exe is never
        # executed -- the afterok path writes the star inline and does not submit an import supervisor;
        # the schemer's relion.importtomo job.star ignores fn_exe and runs relion's native importer.
        # Kept as a harmless no-op so a stray execution can't run a wrong relion_import command.
        return "true  # crboost writes Import/tilt_series.star inline; relion import is not run"

    def _write_scheme_star(self, scheme_dir: Path, scheme_name: str, job_names: list[str]):
        general_df = pd.DataFrame(
            {"rlnSchemeName": [f"Schemes/{scheme_name}/"], "rlnSchemeCurrentNodeName": [job_names[0]]}
        )

        jobs_data = []
        for name in job_names:
            jobs_data.append(
                {
                    "rlnSchemeJobNameOriginal": name,
                    "rlnSchemeJobName": name,
                    "rlnSchemeJobMode": "new",
                    "rlnSchemeJobHasStarted": 0,
                }
            )
        jobs_df = pd.DataFrame(jobs_data)

        edges_data = []
        for i in range(len(job_names) - 1):
            edges_data.append(
                {
                    "rlnSchemeEdgeInputNodeName": job_names[i],
                    "rlnSchemeEdgeOutputNodeName": job_names[i + 1],
                    "rlnSchemeEdgeIsFork": 0,
                    "rlnSchemeEdgeOutputNodeNameIfTrue": "undefined",
                    "rlnSchemeEdgeBooleanVariable": "undefined",
                }
            )

        if job_names:
            edges_data.append(
                {
                    "rlnSchemeEdgeInputNodeName": job_names[-1],
                    "rlnSchemeEdgeOutputNodeName": "EXIT",
                    "rlnSchemeEdgeIsFork": 0,
                    "rlnSchemeEdgeOutputNodeNameIfTrue": "undefined",
                    "rlnSchemeEdgeBooleanVariable": "undefined",
                }
            )

        edges_df = pd.DataFrame(edges_data)

        floats_df = pd.DataFrame(
            {
                "rlnSchemeFloatVariableName": ["do_at_most", "wait_sec"],
                "rlnSchemeFloatVariableValue": [500.0, 10.0],
                "rlnSchemeFloatVariableResetValue": [500.0, 10.0],
            }
        )
        ops_df = pd.DataFrame(
            {
                "rlnSchemeOperatorName": ["EXIT", "WAIT"],
                "rlnSchemeOperatorType": ["exit", "wait"],
                "rlnSchemeOperatorOutput": ["undefined", "undefined"],
                "rlnSchemeOperatorInput1": ["undefined", "wait_sec"],
                "rlnSchemeOperatorInput2": ["undefined", "undefined"],
            }
        )

        data = {
            "scheme_general": general_df,
            "scheme_jobs": jobs_df,
            "scheme_edges": edges_df,
            "scheme_floats": floats_df,
            "scheme_operators": ops_df,
        }

        self.star_handler.write(data, scheme_dir / "scheme.star")

    async def delete_job(self, project_dir: Path, job_type: JobType, harsh: bool = False) -> dict[str, Any]:
        job_numbers = self._get_all_job_numbers_for_type(project_dir, job_type)

        if not job_numbers:
            return {"success": False, "error": f"No instances of {job_type.value} found to delete."}

        logger.info("Found %d instances of %s to delete: %s", len(job_numbers), job_type.value, job_numbers)

        flag = "--harsh_clean" if harsh else "--gentle_clean"
        success_count = 0
        errors = []

        for job_num_str in reversed(job_numbers):
            cmd = f"relion_pipeliner {flag} {job_num_str}"
            result = await self.backend.run_shell_command(cmd, cwd=project_dir, tool_name="relion")

            if result["success"]:
                logger.info("Deleted job %s", job_num_str)
                success_count += 1
            else:
                logger.info("Failed to delete job %s: %s", job_num_str, result.get("error"))
                errors.append(f"Job {job_num_str}: {result.get('error')}")

        if success_count == 0 and errors:
            return {"success": False, "error": f"Failed to delete jobs: {'; '.join(errors)}"}

        return {"success": True, "message": f"Deleted {success_count} job instances.", "deleted_aliases": job_numbers}

    def _get_all_job_numbers_for_type(self, project_dir: Path, target_job_type: JobType) -> list[str]:
        """
        Scans default_pipeline.star to find ALL job numbers matching the type.
        Returns list of strings like ["6", "7", "8"].
        """
        pipeline_star = project_dir / "default_pipeline.star"
        if not pipeline_star.exists():
            return []

        try:
            data = self.star_handler.read(pipeline_star)
            processes = data.get("pipeline_processes", pd.DataFrame())

            if processes.empty:
                return []

            job_numbers = []
            for _, row in processes.iterrows():
                job_path = row["rlnPipeLineProcessName"]
                detected_type_str = self.job_resolver.get_job_type_from_path(project_dir, job_path)

                if detected_type_str == target_job_type.value:
                    try:
                        folder_name = job_path.strip("/").split("/")[-1]
                        number_str = folder_name.replace("job", "")
                        job_numbers.append(str(int(number_str)))
                    except ValueError:
                        logger.info("Could not parse number from %s", job_path)

            return job_numbers

        except Exception as e:
            logger.error("Error scanning pipeline for deletion: %s", e)
            return []

    def _dryrun_compare_schema_paths(
        self, resolver: PathResolutionService, job_type: JobType, job_model: AbstractJobParams, predicted_job_dir: Path
    ) -> None:
        """
        Dry-run path resolution check for debugging.
        Enable via env var: CRBOOST_SCHEMA_RESOLVE_DRYRUN=1
        """
        if os.environ.get("CRBOOST_SCHEMA_RESOLVE_DRYRUN", "0") != "1":
            return

        try:
            schema_paths = resolver.resolve_all_paths(job_type, job_model, predicted_job_dir)
        except PathResolutionError as e:
            logger.info("%s: cannot resolve: %s", job_type.value, e)
            return
        except Exception as e:
            logger.info("%s: unexpected error: %s", job_type.value, e)
            return

        legacy_paths = job_model.paths or {}
        keys = sorted(set(legacy_paths.keys()) | set(schema_paths.keys()))
        diffs = []
        for k in keys:
            if str(legacy_paths.get(k)) != str(schema_paths.get(k)):
                diffs.append((k, legacy_paths.get(k), schema_paths.get(k)))

        if diffs:
            logger.info("%s: %d path diffs", job_type.value, len(diffs))
            for k, oldv, newv in diffs:
                logger.info("  - %s\n      legacy: %s\n      schema : %s", k, oldv, newv)


class JobTypeResolver:
    DRIVER_TO_JOBTYPE: ClassVar[dict[str, str]] = {
        "fs_motion_and_ctf.py": "fsMotionAndCtf",
        "ts_import.py": "tsImport",
        "ts_alignment.py": "aligntiltsWarp",
        "miss_align.py": "missAlign",
        "ts_ctf.py": "tsCtf",
        "ts_reconstruct.py": "tsReconstruct",
        "denoise_train.py": "denoisetrain",
        "denoise_predict.py": "denoisepredict",
        "template_match_pytom.py": "templatematching",
        "extract_candidates_pytom.py": "tmextractcand",
        "subtomo_extraction.py": "subtomoExtraction",
        "reconstruct_particle.py": "reconstructParticle",
        "class3d.py": "class3d",
    }

    def __init__(self, star_handler: StarfileService):
        self.star_handler = star_handler

    def get_job_type_from_path(self, project_dir: Path, job_path: str) -> str | None:
        if "Import/job" in job_path:
            return "importmovies"

        job_star_path = project_dir / job_path.rstrip("/") / "job.star"
        if not job_star_path.exists():
            return None

        try:
            data = self.star_handler.read(job_star_path)
            joboptions = data.get("joboptions_values")

            if joboptions is None or not isinstance(joboptions, pd.DataFrame):
                return None

            fn_exe_rows = joboptions[joboptions["rlnJobOptionVariable"] == "fn_exe"]
            if fn_exe_rows.empty:
                return None

            fn_exe = fn_exe_rows["rlnJobOptionValue"].values[0]

            for driver_name, job_type in self.DRIVER_TO_JOBTYPE.items():
                if driver_name in fn_exe:
                    return job_type

            if "relion_import" in fn_exe:
                return "importmovies"

            return None

        except Exception as e:
            logger.warning("Could not read job type from %s: %s", job_star_path, e)
            return None

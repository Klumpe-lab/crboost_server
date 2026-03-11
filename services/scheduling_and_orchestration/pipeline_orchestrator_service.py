import os
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from services.configs.config_service import get_config_service
from services.configs.starfile_service import StarfileService
from services.job_models import ImportMoviesParams
from services.path_resolution_service import PathResolutionError, PathResolutionService, get_context_paths
from services.project_state import AbstractJobParams, JobCategory, JobType, JobStatus
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend import CryoBoostBackend


class PipelineOrchestratorService:
    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()
        self.job_resolver = JobTypeResolver(self.star_handler)

    async def deploy_and_run_scheme(self, project_dir: Path, selected_instance_ids: List[str]) -> Dict[str, Any]:
        if not selected_instance_ids:
            return {"success": False, "message": "No jobs selected."}

        state = self.backend.state_service.state_for(project_dir)

        # Guard FIRST -- before any state mutation.
        # _run_relion_schemer has the same guard but it fires too late:
        # by the time it's reached, relion_job_name and job_path_mapping
        # have already been clobbered, which breaks sync_all_jobs path resolution.
        if state.pipeline_active:
            return {
                "success": False,
                "message": "Pipeline is already running. Wait for it to complete or cancel it first.",
            }

        instances_to_run: List[str] = []
        for instance_id in selected_instance_ids:
            job_model = state.jobs.get(instance_id)
            if not job_model or job_model.execution_status != JobStatus.SUCCEEDED:
                instances_to_run.append(instance_id)

        if not instances_to_run:
            return {
                "success": True,
                "already_complete": True,
                "message": "All selected jobs are already finished.",
                "pid": 0,
            }

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        scheme_name = f"run_{timestamp}"
        scheme_dir = project_dir / "Schemes" / scheme_name
        scheme_dir.mkdir(parents=True, exist_ok=True)

        current_counter = self._get_current_relion_counter(project_dir)
        server_dir = Path(__file__).parent.parent.parent.resolve()

        resolver = PathResolutionService(state, active_instance_ids=set(instances_to_run))

        report_lines = [f"Scheme: {scheme_name}", f"Instances_to_run: {instances_to_run}", ""]

        for i, instance_id in enumerate(instances_to_run):
            job_num = current_counter + i

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
            predicted_job_dir = project_dir / category.value / f"job{job_num:03d}"

            try:
                io_paths = resolver.resolve_all_paths(job_type, job_model, predicted_job_dir, instance_id=instance_id)
                context_paths = get_context_paths(job_type, job_model, predicted_job_dir)
                resolved_paths = {**context_paths, **io_paths}
            except PathResolutionError as e:
                job_model.is_orphaned = True
                job_model.missing_inputs = [str(e)]
                report_lines.append(f"[{instance_id}] RESOLUTION FAILED: {e}")
                report_lines.append("")
                print(f"[RESOLUTION ERROR] {instance_id}: {e}")
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
        print(f"[RESOLUTION] wrote {report_path}")

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
            JobType.TS_ALIGNMENT: "ts_alignment.py",
            JobType.TS_CTF: "ts_ctf.py",
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

    def _build_import_command(self, params: ImportMoviesParams) -> str:
        mdoc_glob = str(params.paths.get("mdoc_glob", ""))

        cmd = [
            "relion_import",
            "--do_movies",
            "--optics_group_name",
            params.optics_group_name,
            "--angpix",
            str(params.pixel_size),
            "--kV",
            str(params.voltage),
            "--Cs",
            str(params.spherical_aberration),
            "--Q0",
            str(params.amplitude_contrast),
            "--dose_per_tilt_image",
            str(params.dose_per_tilt),
            "--nominal_tilt_axis_angle",
            str(params.tilt_axis_angle),
        ]

        if params.acquisition.invert_defocus_hand:
            cmd.append("--invert_defocus_hand")

        if params.do_at_most > 0:
            cmd.extend(["--do_at_most", str(params.do_at_most)])

        if mdoc_glob:
            cmd.extend(["--i", mdoc_glob])

        return " ".join(cmd)

    def _write_scheme_star(self, scheme_dir: Path, scheme_name: str, job_names: List[str]):
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

    async def delete_job(self, project_dir: Path, job_type: JobType, harsh: bool = False) -> Dict[str, Any]:
        job_numbers = self._get_all_job_numbers_for_type(project_dir, job_type)

        if not job_numbers:
            return {"success": False, "error": f"No instances of {job_type.value} found to delete."}

        print(f"[ORCHESTRATOR] Found {len(job_numbers)} instances of {job_type.value} to delete: {job_numbers}")

        flag = "--harsh_clean" if harsh else "--gentle_clean"
        success_count = 0
        errors = []

        for job_num_str in reversed(job_numbers):
            cmd = f"relion_pipeliner {flag} {job_num_str}"
            result = await self.backend.run_shell_command(cmd, cwd=project_dir, tool_name="relion")

            if result["success"]:
                print(f"[ORCHESTRATOR] Deleted job {job_num_str}")
                success_count += 1
            else:
                print(f"[ORCHESTRATOR] Failed to delete job {job_num_str}: {result.get('error')}")
                errors.append(f"Job {job_num_str}: {result.get('error')}")

        if success_count == 0 and errors:
            return {"success": False, "error": f"Failed to delete jobs: {'; '.join(errors)}"}

        return {"success": True, "message": f"Deleted {success_count} job instances.", "deleted_aliases": job_numbers}

    def _get_all_job_numbers_for_type(self, project_dir: Path, target_job_type: JobType) -> List[str]:
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
                        print(f"[ORCHESTRATOR] Could not parse number from {job_path}")

            return job_numbers

        except Exception as e:
            print(f"[ORCHESTRATOR] Error scanning pipeline for deletion: {e}")
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
            print(f"[SCHEMA DRYRUN] {job_type.value}: cannot resolve: {e}")
            return
        except Exception as e:
            print(f"[SCHEMA DRYRUN] {job_type.value}: unexpected error: {e}")
            return

        legacy_paths = job_model.paths or {}
        keys = sorted(set(legacy_paths.keys()) | set(schema_paths.keys()))
        diffs = []
        for k in keys:
            if str(legacy_paths.get(k)) != str(schema_paths.get(k)):
                diffs.append((k, legacy_paths.get(k), schema_paths.get(k)))

        if diffs:
            print(f"[SCHEMA DRYRUN] {job_type.value}: {len(diffs)} path diffs")
            for k, oldv, newv in diffs:
                print(f"  - {k}\n      legacy: {oldv}\n      schema : {newv}")


class JobTypeResolver:
    DRIVER_TO_JOBTYPE = {
        "fs_motion_and_ctf.py": "fsMotionAndCtf",
        "ts_alignment.py": "aligntiltsWarp",
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

    def get_job_type_from_path(self, project_dir: Path, job_path: str) -> Optional[str]:
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
            print(f"[WARN] Could not read job type from {job_star_path}: {e}")
            return None

# services/pipeline_orchestrator_service.py
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from services.config_service import get_config_service
from services.project_state import (
    AbstractJobParams, 
    JobCategory, 
    JobType, 
    JobStatus,
    ImportMoviesParams,
)
from .starfile_service import StarfileService
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend import CryoBoostBackend

class PipelineOrchestratorService:
    def __init__(self, backend_instance: "CryoBoostBackend"):

        self.backend        = backend_instance
        self.star_handler   = StarfileService()
        self.config_service = get_config_service()
        self.job_resolver   = JobTypeResolver(self.star_handler)

    async def deploy_and_run_scheme(
        self,
        project_dir: Path,
        selected_job_types: List[JobType],
    ) -> Dict[str, Any]:
        """
        Main entry point for "Just-In-Time" scheme generation.
        """
        if not selected_job_types:
            return {"success": False, "message": "No jobs selected."}

        state = self.backend.state_service.state

        # --- FIX: FILTER ALREADY COMPLETED JOBS ---
        # We only want to run jobs that are NOT Succeeded.
        # If the user wants to re-run a succeeded job, they must explicitly reset/delete it first.
        jobs_to_run = []
        for job_type in selected_job_types:
            job_model = state.jobs.get(job_type)
            # If model missing, assume it needs running. If status is not SUCCEEDED, run it.
            if not job_model or job_model.execution_status != JobStatus.SUCCEEDED:
                jobs_to_run.append(job_type)
        
        if not jobs_to_run:
            print("[ORCHESTRATOR] All selected jobs are already completed. Nothing to run.")
            return {"success": True, "message": "All selected jobs are already finished.", "pid": 0}

        print(f"[ORCHESTRATOR] Full selection: {[j.value for j in selected_job_types]}")
        print(f"[ORCHESTRATOR] Actual execution list: {[j.value for j in jobs_to_run]}")

        # 1. Prepare Scheme Directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        scheme_name = f"run_{timestamp}"
        scheme_dir = project_dir / "Schemes" / scheme_name
        scheme_dir.mkdir(parents=True, exist_ok=True)

        # 2. Predict Job Numbers
        current_counter = self._get_current_relion_counter(project_dir)

        # 3. Process Each Job
        server_dir = Path(__file__).parent.parent.resolve()

        previous_job_output_dir_in_batch: Optional[Path] = None

        for i, job_type in enumerate(jobs_to_run):
            # Calculate predicted directory - counter IS the next job number
            job_num = current_counter + i  # NOT current_counter + 1 + i
            
            category = JobCategory.IMPORT if job_type == JobType.IMPORT_MOVIES else JobCategory.EXTERNAL
            predicted_job_dir = project_dir / category.value / f"job{job_num:03d}"
            
            # Ensure Model exists
            job_model = state.jobs.get(job_type)
            if not job_model:
                template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep" / job_type.value / "job.star"
                state.ensure_job_initialized(job_type, template_base)
                job_model = state.jobs.get(job_type)

            # --- UPSTREAM RESOLUTION ---
            upstream_path_for_resolution = None

            if i == 0:
                # First job in THIS batch. 
                # Its input must come from the Project History (a job that finished in a previous run).
                reqs = job_model.get_input_requirements()
                if reqs:
                    # We look for the required job type in the state's path mapping.
                    # This mapping was populated by SyncStatus when we loaded the project.
                    required_type = list(reqs.values())[0] # e.g. "tsReconstruct"
                    
                    if required_type in state.job_path_mapping:
                        rel_path = state.job_path_mapping[required_type] # e.g. "External/job005"
                        upstream_path_for_resolution = project_dir / rel_path
                        print(f"[ORCHESTRATOR] {job_type.value} depends on historical: {upstream_path_for_resolution}")
                    else:
                        print(f"[ORCHESTRATOR] WARNING: Upstream {required_type} not found in history for {job_type.value}")
            else:
                # Subsequent job in THIS batch.
                # Its input comes from the PREVIOUS job in this batch (which we just predicted).
                upstream_path_for_resolution = previous_job_output_dir_in_batch
                print(f"[ORCHESTRATOR] {job_type.value} depends on batch predecessor: {upstream_path_for_resolution}")

            # --- RESOLVE PATHS ---
            resolved_paths = job_model.resolve_paths(
                job_dir=predicted_job_dir, 
                upstream_job_dir=upstream_path_for_resolution
            )
            
            # Update the State (Persist these absolute paths for the Driver)
            job_model.paths = {k: str(v) for k, v in resolved_paths.items() if v}
            
            # Prepare temporary job.star
            scheme_job_dir = scheme_dir / job_type.value
            scheme_job_dir.mkdir(parents=True, exist_ok=True)
            
            self._write_job_star(
                scheme_job_dir = scheme_job_dir,
                job_type       = job_type,
                job_model      = job_model,
                server_dir     = server_dir,
                project_dir    = project_dir
            )
            
            # Update tracking for next iteration
            previous_job_output_dir_in_batch = predicted_job_dir

        # 4. Generate scheme.star
        self._write_scheme_star(scheme_dir, scheme_name, [j.value for j in jobs_to_run])

        # 5. Save Project State
        await self.backend.state_service.save_project()

        # 6. Run
        bind_paths = [str(project_dir.parent.resolve()), str(server_dir.resolve())]
        if state.movies_glob:
             bind_paths.append(str(Path(state.movies_glob).parent.resolve()))
        if state.mdocs_glob:
             bind_paths.append(str(Path(state.mdocs_glob).parent.resolve()))

        return await self.backend.pipeline_runner.run_generated_scheme(
            project_dir=project_dir,
            scheme_name=scheme_name,
            bind_paths=list(set(bind_paths))
        )

    def _get_current_relion_counter(self, project_dir: Path) -> int:
        """
        Reads the current job counter from default_pipeline.star.
        Raises if the file exists but can't be parsed - silent failures cause job path collisions.
        """
        pipeline_star = project_dir / "default_pipeline.star"
        
        if not pipeline_star.exists():
            # This is fine - new project, no jobs yet
            return 0
        
        # File exists, so we MUST be able to read it
        data = self.star_handler.read(pipeline_star)
        general = data.get("pipeline_general")
        
        if general is None:
            raise ValueError(f"pipeline_general block missing from {pipeline_star}")
        
        # starfile returns dict for single-row blocks, DataFrame for multi-row
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

    def _write_job_star(
        self,
    scheme_job_dir: Path,
    job_type      : JobType,
    job_model     : AbstractJobParams,
    server_dir    : Path,
    project_dir   : Path
    )             : 
        template_source = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep" / job_type.value / "job.star"
        dest_star = scheme_job_dir / "job.star"
        
        if template_source.exists():
            shutil.copy(template_source, dest_star)
        else:
            print(f"[ERROR] Template missing for {job_type.value}")
            return

        data = self.star_handler.read(dest_star)
        if "joboptions_values" not in data:
            return

        df = data["joboptions_values"]

        # --- Patch Scheme Name References ---
        template_scheme_name = template_source.parent.parent.name
        new_scheme_name = scheme_job_dir.parent.name
        
        for key, block in data.items():
            if isinstance(block, pd.DataFrame):
                for col in block.select_dtypes(include=['object']):
                    if block[col].astype(str).str.contains(template_scheme_name).any():
                        block[col] = block[col].astype(str).str.replace(
                            template_scheme_name, new_scheme_name, regex=False
                        )

        # Inject fn_exe
        fn_exe = self._build_fn_exe(job_type, job_model, project_dir, server_dir)
        df.loc[df["rlnJobOptionVariable"] == "fn_exe", "rlnJobOptionValue"] = fn_exe
        
        self.star_handler.write(data, dest_star)

    def _build_fn_exe(self, job_type: JobType, job_model: AbstractJobParams, project_dir: Path, server_dir: Path) -> str:
        if job_type == JobType.IMPORT_MOVIES:
            return self._build_import_command(job_model)

        driver_map = {
            JobType.FS_MOTION_CTF  : "fs_motion_and_ctf.py",
            JobType.TS_ALIGNMENT   : "ts_alignment.py",
            JobType.TS_CTF         : "ts_ctf.py",
            JobType.TS_RECONSTRUCT : "ts_reconstruct.py",
            JobType.DENOISE_TRAIN  : "denoise_train.py",
            JobType.DENOISE_PREDICT: "denoise_predict.py",
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
            f"--job_type {job_type.value} "
            f"--project_path {project_dir}"
        )

    def _build_import_command(self, params: ImportMoviesParams) -> str:
        mdoc_glob = str(params.paths.get("mdoc_glob", ""))
        
        cmd = [
            "relion_import",
            "--do_movies",
            "--optics_group_name", params.optics_group_name,
            "--angpix", str(params.pixel_size),
            "--kV", str(params.voltage),
            "--Cs", str(params.spherical_aberration),
            "--Q0", str(params.amplitude_contrast),
            "--dose_per_tilt_image", str(params.dose_per_tilt),
            "--nominal_tilt_axis_angle", str(params.tilt_axis_angle),
        ]

        if params.acquisition.invert_defocus_hand:
            cmd.append("--invert_defocus_hand")

        if params.do_at_most > 0:
            cmd.extend(["--do_at_most", str(params.do_at_most)])

        if mdoc_glob:
             cmd.extend(["--i", mdoc_glob])
        
        return " ".join(cmd)

    def _write_scheme_star(self, scheme_dir: Path, scheme_name: str, job_names: List[str]):
        general_df = pd.DataFrame({
            "rlnSchemeName": [f"Schemes/{scheme_name}/"], 
            "rlnSchemeCurrentNodeName": [job_names[0]] 
        })
        
        jobs_data = []
        for name in job_names:
            jobs_data.append({
                "rlnSchemeJobNameOriginal": name,
                "rlnSchemeJobName": name, 
                "rlnSchemeJobMode": "new",
                "rlnSchemeJobHasStarted": 0
            })
        jobs_df = pd.DataFrame(jobs_data)

        edges_data = []
        for i in range(len(job_names) - 1):
            edges_data.append({
                "rlnSchemeEdgeInputNodeName": job_names[i],
                "rlnSchemeEdgeOutputNodeName": job_names[i+1],
                "rlnSchemeEdgeIsFork": 0,
                "rlnSchemeEdgeOutputNodeNameIfTrue": "undefined",
                "rlnSchemeEdgeBooleanVariable": "undefined"
            })
            
        if job_names:
            edges_data.append({
                "rlnSchemeEdgeInputNodeName": job_names[-1],
                "rlnSchemeEdgeOutputNodeName": "EXIT",
                "rlnSchemeEdgeIsFork": 0,
                "rlnSchemeEdgeOutputNodeNameIfTrue": "undefined",
                "rlnSchemeEdgeBooleanVariable": "undefined"
            })
        
        edges_df = pd.DataFrame(edges_data)

        floats_df = pd.DataFrame({
            "rlnSchemeFloatVariableName": ["do_at_most", "wait_sec"], 
            "rlnSchemeFloatVariableValue": [500.0, 10.0], 
            "rlnSchemeFloatVariableResetValue": [500.0, 10.0]
        })
        ops_df = pd.DataFrame({
            "rlnSchemeOperatorName": ["EXIT", "WAIT"], 
            "rlnSchemeOperatorType": ["exit", "wait"], 
            "rlnSchemeOperatorOutput": ["undefined", "undefined"], 
            "rlnSchemeOperatorInput1": ["undefined", "wait_sec"], 
            "rlnSchemeOperatorInput2": ["undefined", "undefined"]
        })

        data = {
            "scheme_general": general_df,
            "scheme_jobs": jobs_df,
            "scheme_edges": edges_df,
            "scheme_floats": floats_df,
            "scheme_operators": ops_df
        }
        
        self.star_handler.write(data, scheme_dir / "scheme.star")

    async def delete_job(self, project_dir: Path, job_type: JobType, harsh: bool = False) -> Dict[str, Any]:
        # 1. Find ALL job NUMBERS for this type (e.g. ['6', '7', '8', '9'])
        # Changed variable name from 'aliases' to 'job_numbers' for clarity
        job_numbers = self._get_all_job_numbers_for_type(project_dir, job_type)
        
        if not job_numbers:
            return {"success": False, "error": f"No instances of {job_type.value} found to delete."}

        print(f"[ORCHESTRATOR] Found {len(job_numbers)} instances of {job_type.value} to delete: {job_numbers}")

        flag = "--harsh_clean" if harsh else "--gentle_clean"
        success_count = 0
        errors = []

        # 2. Iterate and Nuke
        # We process in reverse order (newest first)
        for job_num_str in reversed(job_numbers):
            # FIX: Pass the bare number "9", not "job009"
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

        return {
            "success": True, 
            "message": f"Deleted {success_count} job instances.", 
            "deleted_aliases": job_numbers
        }

    def _get_all_job_numbers_for_type(self, project_dir: Path, target_job_type: JobType) -> List[str]:
        """
        Scans default_pipeline.star to find ALL job numbers matching the type.
        Returns list of strings: ["6", "7", "8"]
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
                job_path = row["rlnPipeLineProcessName"] # e.g. "External/job006/"
                
                detected_type_str = self.job_resolver.get_job_type_from_path(project_dir, job_path)
                
                if detected_type_str == target_job_type.value:
                    try:
                        # Extract "job006" -> 6
                        # Handle "External/job006/"
                        folder_name = job_path.strip("/").split("/")[-1] # "job006"
                        number_str = folder_name.replace("job", "")      # "006"
                        clean_number = str(int(number_str))              # "6"
                        job_numbers.append(clean_number)
                    except ValueError:
                        print(f"[ORCHESTRATOR] Could not parse number from {job_path}")
            
            return job_numbers

        except Exception as e:
            print(f"[ORCHESTRATOR] Error scanning pipeline for deletion: {e}")
            return []

class JobTypeResolver:

    DRIVER_TO_JOBTYPE = {
        "fs_motion_and_ctf.py": "fsMotionAndCtf",
        "ts_alignment.py"     : "aligntiltsWarp",
        "ts_ctf.py"           : "tsCtf",
        "ts_reconstruct.py"   : "tsReconstruct",
        "denoise_train.py"    : "denoisetrain",
        "denoise_predict.py"  : "denoisepredict",
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
    
    def get_all_completed_jobs(self, project_dir: Path) -> Dict[str, str]:
        pipeline_star = project_dir / "default_pipeline.star"
        if not pipeline_star.exists():
            return {}
        try:
            data = self.star_handler.read(pipeline_star)
            processes = data.get("pipeline_processes", pd.DataFrame())
            if processes.empty: return {}
            result = {}
            for _, row in processes.iterrows():
                job_path = row["rlnPipeLineProcessName"]
                status = row["rlnPipeLineProcessStatusLabel"]
                if status in ["Succeeded", "Running"]:
                    job_type = self.get_job_type_from_path(project_dir, job_path)
                    if job_type: result[job_type] = job_path
            return result
        except Exception as e:
            print(f"[WARN] Could not parse pipeline: {e}")
            return {}

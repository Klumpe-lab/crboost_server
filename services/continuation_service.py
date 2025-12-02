# services/continuation_service.py

import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional

from services.starfile_service import StarfileService
from services.project_state import JobType


class JobTypeResolver:
    """Resolves job types from job directories - no hardcoded mappings."""

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
        """Determine job type by inspecting the job.star file."""
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

    def get_all_pipeline_jobs(self, project_dir: Path) -> Dict[str, str]:
        """Return {job_type: job_path} for all jobs in pipeline."""
        pipeline_star = project_dir / "default_pipeline.star"
        if not pipeline_star.exists():
            return {}

        try:
            data = self.star_handler.read(pipeline_star)
            processes = data.get("pipeline_processes", pd.DataFrame())

            if processes.empty:
                return {}

            result = {}
            for _, row in processes.iterrows():
                job_path = row["rlnPipeLineProcessName"]
                job_type = self.get_job_type_from_path(project_dir, job_path)
                if job_type:
                    result[job_type] = job_path

            return result

        except Exception as e:
            print(f"[WARN] Could not parse pipeline: {e}")
            return {}


class ContinuationService:
    """Handles adding jobs to existing pipelines."""

    def __init__(self, backend):
        self.backend = backend
        self.star_handler = StarfileService()
        self.job_resolver = JobTypeResolver(self.star_handler)

    def is_pipeline_complete(self, project_dir: Path, scheme_name: str) -> bool:
        """Check if all jobs in the scheme have completed (current_node is EXIT)."""
        scheme_star = project_dir / "Schemes" / scheme_name / "scheme.star"
        if not scheme_star.exists():
            return False

        try:
            data = self.star_handler.read(scheme_star)
            general = data.get("scheme_general")
            if isinstance(general, pd.DataFrame) and not general.empty:
                current_node = general["rlnSchemeCurrentNodeName"].values[0]
                return current_node == "EXIT"
            return False
        except:
            return False

    async def add_job_to_existing_pipeline(
        self, project_dir: Path, scheme_name: str, job_type: JobType
    ) -> Dict[str, Any]:
        """
        Add a new job to an existing completed pipeline.

        1. Find the last job in the current pipeline (predecessor)
        2. Prepare job.star with correct fn_exe
        3. Use relion_pipeliner to add to default_pipeline.star
        4. Update scheme.star with new job and edges
        """
        scheme_dir = project_dir / "Schemes" / scheme_name

        # 1. Find predecessor - get the CONCRETE path from scheme_jobs
        predecessor_info = self._get_last_completed_job(scheme_dir)
        if not predecessor_info:
            return {"success": False, "error": "Could not find last completed job in scheme"}

        predecessor_original_name, predecessor_concrete_path = predecessor_info
        print(
            f"[CONTINUATION] Adding {job_type.value} after {predecessor_concrete_path} (original: {predecessor_original_name})"
        )

        # 2. Prepare job.star
        job_star_path = self._prepare_job_star(project_dir, scheme_dir, job_type)
        if not job_star_path:
            return {"success": False, "error": "Failed to prepare job.star"}

        # 3. Add via relion_pipeliner
        add_result = await self._add_job_via_pipeliner(project_dir, job_star_path, job_type.value)
        if not add_result["success"]:
            return add_result

        # 4. Find the new job path
        new_job_path = self._find_new_job_path(project_dir, job_type)
        if not new_job_path:
            return {"success": False, "error": "Job added but path not found"}

        print(f"[CONTINUATION] New job created at: {new_job_path}")

        # 5. Update scheme.star - use the original name for edge matching
        self._update_scheme(
            scheme_dir, job_type.value, new_job_path, predecessor_original_name, predecessor_concrete_path
        )

        # 6. Create symlink in scheme
        self._create_scheme_symlink(scheme_dir, project_dir, new_job_path)

        return {"success": True, "new_job_path": new_job_path}

    def _get_last_completed_job(self, scheme_dir: Path) -> Optional[tuple[str, str]]:
        """
        Find the last job that points to EXIT.
        Returns (edge_name, concrete_path) tuple.
        """
        scheme_star = scheme_dir / "scheme.star"
        if not scheme_star.exists():
            return None
        
        data = self.star_handler.read(scheme_star)
        edges_df = data.get("scheme_edges")
        jobs_df = data.get("scheme_jobs")
        
        if edges_df is None or jobs_df is None:
            return None
        
        # Find the node that points to EXIT
        exit_edge = edges_df[edges_df["rlnSchemeEdgeOutputNodeName"] == "EXIT"]
        if exit_edge.empty:
            return None
        
        # This is the name used in edges - could be symbolic (tsReconstruct) or concrete (External/job005/)
        last_node_name = exit_edge.iloc[0]["rlnSchemeEdgeInputNodeName"]
        print(f"[CONTINUATION] Last node pointing to EXIT: {last_node_name}")
        
        # Try to find in scheme_jobs
        # First: check if edges use rlnSchemeJobNameOriginal (symbolic name like "tsReconstruct")
        job_match = jobs_df[jobs_df["rlnSchemeJobNameOriginal"] == last_node_name]
        
        if not job_match.empty:
            concrete_path = job_match.iloc[0]["rlnSchemeJobName"]
            print(f"[CONTINUATION] Found by original name: {last_node_name} -> {concrete_path}")
            return (last_node_name, concrete_path)
        
        # Second: check if edges use rlnSchemeJobName (concrete path like "External/job005/")
        job_match = jobs_df[jobs_df["rlnSchemeJobName"] == last_node_name]
        
        if not job_match.empty:
            concrete_path = last_node_name  # It's already concrete
            print(f"[CONTINUATION] Found by job name (already concrete): {concrete_path}")
            return (last_node_name, concrete_path)
        
        print(f"[CONTINUATION] Could not find job matching: {last_node_name}")
        return None

    def _resolve_symbolic_to_concrete(self, project_dir: Path, job_type_name: str) -> Optional[str]:
        """Resolve a symbolic job name to its concrete path using default_pipeline.star."""
        pipeline_jobs = self.job_resolver.get_all_pipeline_jobs(project_dir)
        return pipeline_jobs.get(job_type_name)

    def _prepare_job_star(self, project_dir: Path, scheme_dir: Path, job_type: JobType) -> Optional[Path]:
        """Copy template and set fn_exe to our driver."""
        template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        source_job_dir = template_base / job_type.value
        dest_job_dir = scheme_dir / job_type.value

        if not source_job_dir.exists():
            print(f"[ERROR] Template not found: {source_job_dir}")
            return None

        if dest_job_dir.exists():
            shutil.rmtree(dest_job_dir)
        shutil.copytree(source_job_dir, dest_job_dir)

        job_star_path = dest_job_dir / "job.star"

        # Build fn_exe command
        server_dir = Path(__file__).parent.parent.resolve()
        driver_scripts = {
            JobType.DENOISE_TRAIN: "denoise_train.py",
            JobType.DENOISE_PREDICT: "denoise_predict.py",
            JobType.FS_MOTION_CTF: "fs_motion_and_ctf.py",
            JobType.TS_ALIGNMENT: "ts_alignment.py",
            JobType.TS_CTF: "ts_ctf.py",
            JobType.TS_RECONSTRUCT: "ts_reconstruct.py",
        }

        driver_name = driver_scripts.get(job_type)
        if not driver_name:
            print(f"[ERROR] No driver for job type: {job_type}")
            return None

        python_exe = server_dir / "venv" / "bin" / "python3"
        driver_path = server_dir / "drivers" / driver_name

        fn_exe = (
            f"export PYTHONPATH={server_dir}:${{PYTHONPATH}}; "
            f"{python_exe} {driver_path} "
            f"--job_type {job_type.value} "
            f"--project_path {project_dir}"
        )

        # Update job.star
        data = self.star_handler.read(job_star_path)

        # Force External job type
        if "job" in data and isinstance(data["job"], pd.DataFrame):
            if "_rlnJobTypeLabel" in data["job"].columns:
                data["job"]["_rlnJobTypeLabel"] = "relion.external"

        joboptions = data.get("joboptions_values")
        if joboptions is not None:
            joboptions.loc[joboptions["rlnJobOptionVariable"] == "fn_exe", "rlnJobOptionValue"] = fn_exe

        self.star_handler.write(data, job_star_path)
        print(f"[CONTINUATION] Prepared job.star at {job_star_path}")

        return job_star_path

    async def _add_job_via_pipeliner(self, project_dir: Path, job_star_path: Path, alias: str) -> Dict[str, Any]:
        """Use relion_pipeliner --addJobFromStar."""
        rel_path = job_star_path.relative_to(project_dir)

        cmd = f"relion_pipeliner --addJobFromStar {rel_path} --setJobAlias {alias}"
        print(f"[CONTINUATION] Running: {cmd}")

        result = await self.backend.run_shell_command(cmd, cwd=project_dir, tool_name="relion")

        if result["success"]:
            print(f"[CONTINUATION] relion_pipeliner succeeded")
        else:
            print(f"[CONTINUATION] relion_pipeliner failed: {result.get('error')}")

        return result

    def _find_new_job_path(self, project_dir: Path, job_type: JobType) -> Optional[str]:
        """Find the newly created job in default_pipeline.star."""
        pipeline_star = project_dir / "default_pipeline.star"
        data = self.star_handler.read(pipeline_star)
        processes = data.get("pipeline_processes", pd.DataFrame())

        if processes.empty:
            return None

        # Check by alias first
        alias_match = processes[processes["rlnPipeLineProcessAlias"].str.contains(job_type.value, na=False)]
        if not alias_match.empty:
            return alias_match.iloc[-1]["rlnPipeLineProcessName"]

        # Fallback: get the last External job (highest number)
        external_jobs = processes[processes["rlnPipeLineProcessName"].str.contains("External/job")]
        if not external_jobs.empty:
            return external_jobs.iloc[-1]["rlnPipeLineProcessName"]

        return None

    def _update_scheme(
        self,
        scheme_dir: Path,
        job_name: str,
        new_job_path: str,
        predecessor_edge_name: str,
        predecessor_concrete_path: str,
    ):
        """Update scheme.star with new job and rewired edges."""
        scheme_star = scheme_dir / "scheme.star"
        data = self.star_handler.read(scheme_star)

        # 1. Add to scheme_jobs
        jobs_df = data.get("scheme_jobs")
        new_row = pd.DataFrame(
            [
                {
                    "rlnSchemeJobNameOriginal": job_name,
                    "rlnSchemeJobName": new_job_path,
                    "rlnSchemeJobMode": "new",
                    "rlnSchemeJobHasStarted": 0,
                }
            ]
        )
        data["scheme_jobs"] = pd.concat([jobs_df, new_row], ignore_index=True)

        # 2. Update edges: predecessor -> new_job -> EXIT
        edges_df = data.get("scheme_edges")

        # Remove old: predecessor -> EXIT (match by the edge name, could be symbolic)
        mask = (edges_df["rlnSchemeEdgeInputNodeName"] == predecessor_edge_name) & (
            edges_df["rlnSchemeEdgeOutputNodeName"] == "EXIT"
        )
        edges_df = edges_df[~mask]

        # Add: predecessor -> new_job (use the same name that was in edges)
        edge1 = pd.DataFrame(
            [
                {
                    "rlnSchemeEdgeInputNodeName": predecessor_edge_name,
                    "rlnSchemeEdgeOutputNodeName": new_job_path,
                    "rlnSchemeEdgeIsFork": 0,
                    "rlnSchemeEdgeOutputNodeNameIfTrue": "undefined",
                    "rlnSchemeEdgeBooleanVariable": "undefined",
                }
            ]
        )

        # Add: new_job -> EXIT
        edge2 = pd.DataFrame(
            [
                {
                    "rlnSchemeEdgeInputNodeName": new_job_path,
                    "rlnSchemeEdgeOutputNodeName": "EXIT",
                    "rlnSchemeEdgeIsFork": 0,
                    "rlnSchemeEdgeOutputNodeNameIfTrue": "undefined",
                    "rlnSchemeEdgeBooleanVariable": "undefined",
                }
            ]
        )

        data["scheme_edges"] = pd.concat([edges_df, edge1, edge2], ignore_index=True)

        # 3. Set current_node to predecessor CONCRETE path (schemer will advance to new job)
        general_df = data.get("scheme_general")
        general_df["rlnSchemeCurrentNodeName"] = predecessor_concrete_path
        data["scheme_general"] = general_df

        self.star_handler.write(data, scheme_star)
        print(f"[CONTINUATION] Updated scheme.star: predecessor={predecessor_edge_name}, new_job={new_job_path}")

    def _create_scheme_symlink(self, scheme_dir: Path, project_dir: Path, job_path: str):
        """Create symlink in scheme directory pointing to actual job."""
        clean_path = job_path.rstrip("/")
        link_location = scheme_dir / clean_path
        real_target = project_dir / clean_path

        link_location.parent.mkdir(parents=True, exist_ok=True)

        if not link_location.exists() and real_target.exists():
            link_location.symlink_to(real_target.resolve())
            print(f"[CONTINUATION] Created symlink: {link_location} -> {real_target}")

# services/continuation_service.py (FIXED)

from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
import shutil
from services.project_state import JobStatus
from services.starfile_service import StarfileService
import json

class SchemeManipulationService:

    def __init__(self, backend):
        self.backend = backend
        self.star_handler = StarfileService()

    def reset_job_in_scheme(
        self,
        project_path: Path,
        scheme_name: str,
        job_type: str  # Make sure this is a string, not a number
    ) -> Dict[str, Any]:
        # ADD DEBUGGING
        print(f"[DEBUG SCHEME_RESET] Resetting job_type: '{job_type}' (type: {type(job_type)})")
        
        # Rest of your existing code...
        """
        Reset a job in scheme.star to allow re-running.
        
        Sets:
        - rlnSchemeJobName = job_type (back to template name)
        - rlnSchemeJobHasStarted = 0
        - rlnSchemeCurrentNodeName = job_type (so Relion starts here)
        """
        try:
            scheme_star_path = project_path / "Schemes" / scheme_name / "scheme.star"
            
            if not scheme_star_path.exists():
                return {"success": False, "error": f"Scheme file not found: {scheme_star_path}"}
            
            # Read scheme
            scheme_data = self.star_handler.read(scheme_star_path)
            
            # Find and update the job in scheme_jobs
            jobs_df = scheme_data.get("scheme_jobs")
            if jobs_df is None:
                return {"success": False, "error": "No jobs found in scheme"}
            
            if not isinstance(jobs_df, pd.DataFrame) or jobs_df.empty:
                 return {"success": False, "error": "scheme_jobs is not in expected format"}
            
            job_found = False
            previous_job_name = None
            
            for idx, row in jobs_df.iterrows():
                if row["rlnSchemeJobNameOriginal"] == job_type:
                    previous_job_name = row["rlnSchemeJobName"]
                    
                    # Reset to not started
                    jobs_df.at[idx, "rlnSchemeJobName"] = job_type
                    jobs_df.at[idx, "rlnSchemeJobHasStarted"] = 0
                    
                    job_found = True
                    print(f"[SCHEME] Reset {job_type}: JobName '{previous_job_name}' -> '{job_type}', HasStarted 1 -> 0")
                    break
            
            if not job_found:
                return {"success": False, "error": f"Job type '{job_type}' not found in scheme"}
            
            scheme_data["scheme_jobs"] = jobs_df
            
            # --- THIS IS THE FIX ---
            # Set CurrentNodeName to the job we just reset, not the previous one.
            general_data = scheme_data.get("scheme_general")
            if general_data is not None:
                print(f"[SCHEME] Setting CurrentNodeName to '{job_type}'")
                if isinstance(general_data, pd.DataFrame):
                    general_data.at[0, "rlnSchemeCurrentNodeName"] = job_type
                elif isinstance(general_data, dict):
                    general_data["rlnSchemeCurrentNodeName"] = job_type
                
                scheme_data["scheme_general"] = general_data
            # --- END FIX ---
            
            # Write scheme back
            self.star_handler.write(scheme_data, scheme_star_path)
            print(f"[SCHEME] Updated {scheme_star_path}")
            
            return {
                "success": True,
                "job_type": job_type,
                "previous_job_name": previous_job_name
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def find_job_type_from_job_name(
            self,
            project_path: Path,
            scheme_name: str,
            job_name_full: str
        ) -> Optional[str]:
            """
            Find the original job type (e.g., "tsCtf") from the run-time job name
            (e.g., "External/job005/").
            """
            try:
                scheme_star_path = project_path / "Schemes" / scheme_name / "scheme.star"
                if not scheme_star_path.exists():
                    print(f"[SCHEME] Scheme file not found: {scheme_star_path}")
                    return None
                
                scheme_data = self.star_handler.read(scheme_star_path)
                jobs_df = scheme_data.get("scheme_jobs")

                if jobs_df is None or not isinstance(jobs_df, pd.DataFrame) or jobs_df.empty:
                    print(f"[SCHEME] No valid scheme_jobs table found in {scheme_star_path}")
                    return None

                job_name_clean = job_name_full.rstrip("/")
                
                for _, row in jobs_df.iterrows():
                    if row["rlnSchemeJobName"].rstrip("/") == job_name_clean:
                        job_type = row["rlnSchemeJobNameOriginal"]
                        return job_type
                
                print(f"[SCHEME] Could not find job '{job_name_full}' in scheme_jobs table.")
                return None

            except Exception as e:
                print(f"[SCHEME] Error in find_job_type_from_job_name: {e}")
                return None

    def mark_jobs_as_completed(
        self,
        project_path: Path,
        scheme_name: str,
        job_types: List[str]
    ) -> Dict[str, Any]:
        """
        Mark multiple jobs as completed in scheme to skip them on next run.
        """
        try:
            scheme_star_path = project_path / "Schemes" / scheme_name / "scheme.star"
            
            if not scheme_star_path.exists():
                return {"success": False, "error": f"Scheme file not found: {scheme_star_path}"}
            
            scheme_data = self.star_handler.read(scheme_star_path)
            jobs_df = scheme_data.get("scheme_jobs")
            
            if jobs_df is None:
                return {"success": False, "error": "No jobs found in scheme"}
            
            if not isinstance(jobs_df, pd.DataFrame) or jobs_df.empty:
                return {"success": False, "error": "scheme_jobs is not in expected format"}
            
            marked_jobs = []
            
            for idx, row in jobs_df.iterrows():
                if row["rlnSchemeJobNameOriginal"] in job_types:
                    jobs_df.at[idx, "rlnSchemeJobHasStarted"] = 1
                    marked_jobs.append(row["rlnSchemeJobNameOriginal"])
                    print(f"[SCHEME] Marked {row['rlnSchemeJobNameOriginal']} as HasStarted=1")
            
            scheme_data["scheme_jobs"] = jobs_df
            self.star_handler.write(scheme_data, scheme_star_path)
            
            return {
                "success": True,
                "marked_jobs": marked_jobs
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def get_scheme_state(
        self,
        project_path: Path,
        scheme_name: str
    ) -> Dict[str, Any]:
        """
        Get current state of all jobs in the scheme.
        """
        try:
            scheme_star_path = project_path / "Schemes" / scheme_name / "scheme.star"
            
            if not scheme_star_path.exists():
                return {"success": False, "error": f"Scheme file not found: {scheme_star_path}"}
            
            scheme_data = self.star_handler.read(scheme_star_path)
            
            # Get current node
            general_data = scheme_data.get("scheme_general")
            current_node = None
            if general_data is not None:
                if isinstance(general_data, pd.DataFrame):
                    current_node = general_data.at[0, "rlnSchemeCurrentNodeName"]
                elif isinstance(general_data, dict):
                    current_node = general_data.get("rlnSchemeCurrentNodeName")
            
            # Get jobs list
            jobs_df = scheme_data.get("scheme_jobs")
            jobs_list = []
            
            if jobs_df is not None:
                if isinstance(jobs_df, pd.DataFrame) and not jobs_df.empty:
                    for _, row in jobs_df.iterrows():
                        jobs_list.append({
                            "original_name": row["rlnSchemeJobNameOriginal"],
                            "job_name": row["rlnSchemeJobName"],
                            "has_started": int(row["rlnSchemeJobHasStarted"]),
                            "mode": row["rlnSchemeJobMode"]
                        })
                elif isinstance(jobs_df, dict):
                    jobs_list.append({
                        "original_name": jobs_df.get("rlnSchemeJobNameOriginal"),
                        "job_name": jobs_df.get("rlnSchemeJobName"),
                        "has_started": int(jobs_df.get("rlnSchemeJobHasStarted", 0)),
                        "mode": jobs_df.get("rlnSchemeJobMode")
                    })
            
            return {
                "success": True,
                "current_node": current_node,
                "jobs": jobs_list
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

class PipelineManipulationService:

    def __init__(self, backend):
        self.backend = backend
        self.star_handler = StarfileService()

    def delete_job_from_pipeline(
        self,
        project_path: Path,
        job_number: int,
        move_to_trash: bool = True
    ) -> Dict[str, Any]:
        """
        Delete a job from default_pipeline.star and optionally move to Trash/.
        """
        try:
            pipeline_star_path = project_path / "default_pipeline.star"
            
            if not pipeline_star_path.exists():
                return {"success": False, "error": f"Pipeline file not found: {pipeline_star_path}"}
            
            # Read pipeline
            pipeline_data = self.star_handler.read(pipeline_star_path)
            
            # Find the job to delete
            processes_df = pipeline_data.get("pipeline_processes")
            if processes_df is None or processes_df.empty:
                return {"success": False, "error": "No processes found in pipeline"}
            
            # Find job by number (e.g., job004 -> 4)
            job_name = None
            job_index = None
            
            for idx, row in processes_df.iterrows():
                process_name = row["rlnPipeLineProcessName"]
                # Extract job number from "External/job004/" or "Import/job001/"
                if f"job{job_number:03d}/" in process_name:
                    job_name = process_name
                    job_index = idx
                    break
            
            if job_name is None:
                return {"success": False, "error": f"Job {job_number} not found in pipeline"}
            
            print(f"[PIPELINE] Found job to delete: {job_name}")
            
            # Check for downstream dependencies
            dependency_check = self._check_downstream_dependencies(pipeline_data, job_name)
            if not dependency_check["safe_to_delete"]:
                return {
                    "success": False,
                    "error": f"Cannot delete {job_name}: it has downstream dependencies",
                    "dependencies": dependency_check["dependent_jobs"]
                }
            
            # Remove from pipeline_processes
            processes_df = processes_df.drop(index=job_index).reset_index(drop=True)
            pipeline_data["pipeline_processes"] = processes_df
            
            # Remove from pipeline_nodes (output nodes of this job)
            nodes_df = pipeline_data.get("pipeline_nodes")
            if nodes_df is not None and not nodes_df.empty:
                # Remove nodes that belong to this job (start with job_name)
                nodes_df = nodes_df[~nodes_df["rlnPipeLineNodeName"].str.startswith(job_name)]
                pipeline_data["pipeline_nodes"] = nodes_df.reset_index(drop=True)
            
            # Remove from pipeline_input_edges (where this job is the consumer)
            input_edges_df = pipeline_data.get("pipeline_input_edges")
            if input_edges_df is not None and not input_edges_df.empty:
                input_edges_df = input_edges_df[input_edges_df["rlnPipeLineEdgeProcess"] != job_name]
                pipeline_data["pipeline_input_edges"] = input_edges_df.reset_index(drop=True)
            
            # Remove from pipeline_output_edges (where this job is the producer)
            output_edges_df = pipeline_data.get("pipeline_output_edges")
            if output_edges_df is not None and not output_edges_df.empty:
                output_edges_df = output_edges_df[output_edges_df["rlnPipeLineEdgeProcess"] != job_name]
                pipeline_data["pipeline_output_edges"] = output_edges_df.reset_index(drop=True)
            
            # Write modified pipeline back
            self.star_handler.write(pipeline_data, pipeline_star_path)
            print(f"[PIPELINE] Removed {job_name} from default_pipeline.star")
            
            # Move job directory to Trash/
            moved_to_trash = False
            if move_to_trash:
                job_dir = project_path / job_name.rstrip("/")
                if job_dir.exists():
                    trash_dir = project_path / "Trash" / job_name.rstrip("/")
                    trash_dir.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Remove existing trash copy if it exists
                    if trash_dir.exists():
                        shutil.rmtree(trash_dir)
                    
                    shutil.move(str(job_dir), str(trash_dir))
                    print(f"[PIPELINE] Moved {job_dir} to {trash_dir}")
                    moved_to_trash = True
                else:
                    print(f"[PIPELINE] Warning: Job directory {job_dir} does not exist")
            
            return {
                "success": True,
                "deleted_job": job_name,
                "moved_to_trash": moved_to_trash
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _check_downstream_dependencies(
        self, 
        pipeline_data: Dict[str, pd.DataFrame], 
        job_name: str
    ) -> Dict[str, Any]:
        dependent_jobs = []
        
        output_edges_df = pipeline_data.get("pipeline_output_edges")
        if output_edges_df is None or output_edges_df.empty:
            return {"safe_to_delete": True, "dependent_jobs": []}
        
        output_nodes = output_edges_df[
            output_edges_df["rlnPipeLineEdgeProcess"] == job_name
        ]["rlnPipeLineEdgeToNode"].tolist()
        
        if not output_nodes:
            return {"safe_to_delete": True, "dependent_jobs": []}
        
        # Check if any other jobs use these outputs as inputs
        input_edges_df = pipeline_data.get("pipeline_input_edges")
        if input_edges_df is None or input_edges_df.empty:
            return {"safe_to_delete": True, "dependent_jobs": []}
        
        for output_node in output_nodes:
            dependent = input_edges_df[
                input_edges_df["rlnPipeLineEdgeFromNode"] == output_node
            ]["rlnPipeLineEdgeProcess"].tolist()
            
            dependent_jobs.extend(dependent)
        
        # Remove duplicates and the job itself
        dependent_jobs = list(set(dependent_jobs))
        if job_name in dependent_jobs:
            dependent_jobs.remove(job_name)
        
        safe_to_delete = len(dependent_jobs) == 0
        
        return {
            "safe_to_delete": safe_to_delete,
            "dependent_jobs": dependent_jobs
        }

    def get_job_info_by_number(
        self, 
        project_path: Path, 
        job_number: int,
        scheme_name: str = None  
    ) -> Optional[Dict[str, Any]]:
        try:
            pipeline_star_path = project_path / "default_pipeline.star"
            pipeline_data = self.star_handler.read(pipeline_star_path)
            
            processes_df = pipeline_data.get("pipeline_processes")
            if processes_df is None or processes_df.empty:
                return None
            
            for _, row in processes_df.iterrows():
                process_name = row["rlnPipeLineProcessName"]
                if f"job{job_number:03d}/" in process_name:
                    job_dir = project_path / process_name.rstrip("/")
                    job_params_file = job_dir / "job_params.json"
                    
                    job_type = None
                    if job_params_file.exists():
                        try:
                            with open(job_params_file, 'r') as f:
                                params = json.load(f)
                                job_type = params.get("job_type")
                        except Exception as e:
                            print(f"[PIPELINE] Warning: Could not read {job_params_file}: {e}")
                    
                    if not job_type and scheme_name:
                        print(f"[PIPELINE] No job_type in params. Falling back to scheme '{scheme_name}'...")
                        job_type = self._find_job_type_from_scheme(
                            project_path,
                            scheme_name,
                            process_name # e.g., "External/job005/"
                        )
                        if job_type:
                             print(f"[PIPELINE] Found job type via scheme: {job_type}")

                    return {
                        "job_name": process_name,
                        "job_type": job_type,                                   
                        "status"  : row["rlnPipeLineProcessStatusLabel"],
                        "alias"   : row.get("rlnPipeLineProcessAlias", "None")
                    }
            
            return None
            
        except Exception as e:
            print(f"[PIPELINE] Error getting job info: {e}")
            return None

    def _find_job_type_from_scheme(
        self,
        project_path: Path,
        scheme_name: str,
        job_name_full: str
    ) -> Optional[str]:
        """
        Helper to find job type from scheme.star, isolated in this service.
        """
        try:
            scheme_star_path = project_path / "Schemes" / scheme_name / "scheme.star"
            if not scheme_star_path.exists():
                print(f"[PIPELINE-HELPER] Scheme file not found: {scheme_star_path}")
                return None
            
            scheme_data = self.star_handler.read(scheme_star_path)
            jobs_df = scheme_data.get("scheme_jobs")

            if jobs_df is None or not isinstance(jobs_df, pd.DataFrame) or jobs_df.empty:
                print(f"[PIPELINE-HELPER] No valid scheme_jobs table found in {scheme_star_path}")
                return None

            job_name_clean = job_name_full.rstrip("/")
            
            for _, row in jobs_df.iterrows():
                if row["rlnSchemeJobName"].rstrip("/") == job_name_clean:
                    return row["rlnSchemeJobNameOriginal"]
            
            print(f"[PIPELINE-HELPER] Could not find job '{job_name_full}' in scheme_jobs table.")
            return None

        except Exception as e:
            print(f"[PIPELINE-HELPER] Error in _find_job_type_from_scheme: {e}")
            return None

class ContinuationService:
    """
    Orchestrates pipeline continuation operations.
    Ensures consistency between default_pipeline.star and scheme.star.
    """

    def __init__(self, backend):
        self.backend = backend
        self.pipeline_service = PipelineManipulationService(backend)
        self.scheme_service = SchemeManipulationService(backend)

    def delete_and_reset_job(
            self,
            project_path: str,
            job_number: int,
            scheme_name: str
        ) -> Dict[str, Any]:
            """
            Complete operation to delete a job and reset it for re-running.
            """
            project_dir = Path(project_path)
            
            try:
                # Step 1: Get job info from pipeline.star
                print(f"[CONTINUATION] Getting info for job{job_number:03d}")
                job_info = self.pipeline_service.get_job_info_by_number(
                    project_dir, 
                    job_number, 
                    scheme_name
                )
                
                if not job_info:
                    return {
                        "success": False,
                        "error": f"Job {job_number} not found in pipeline"
                    }
                
                job_type = job_info.get("job_type")
                job_name_full = job_info.get("job_name")

                if not job_type:
                    return {
                        "success": False,
                        "error": f"Could not determine job type for {job_name_full} from scheme or job_params.json"
                    }
                
                print(f"[CONTINUATION] Found job type: {job_type}")
                
                # Step 1.5: Get the job model BEFORE deletion so we can reset it
                from services.state_old.parameter_models import JobType
                job_type_enum = JobType.from_string(job_type)
                job_model = self.backend.app_state.jobs.get(job_type)
                
                # Step 2: Delete from pipeline
                print(f"[CONTINUATION] Deleting {job_info['job_name']} from pipeline")
                pipeline_result = self.pipeline_service.delete_job_from_pipeline(
                    project_dir,
                    job_number,
                    move_to_trash=True
                )
                
                if not pipeline_result["success"]:
                    return {
                        "success": False,
                        "error": f"Failed to delete from pipeline: {pipeline_result.get('error')}",
                        "pipeline_result": pipeline_result
                    }
                
                print(f"[CONTINUATION] Deleted from pipeline: {pipeline_result}")
                
                # Step 3: Reset in scheme
                print(f"[CONTINUATION] Resetting {job_type} in scheme")
                scheme_result = self.scheme_service.reset_job_in_scheme(
                    project_dir,
                    scheme_name,
                    job_type
                )
                
                if not scheme_result["success"]:
                    return {
                        "success": False,
                        "error": f"Failed to reset in scheme: {scheme_result.get('error')}",
                        "pipeline_result": pipeline_result,
                        "scheme_result": scheme_result
                    }
                
                print(f"[CONTINUATION] Reset in scheme: {scheme_result}")
                
                # === CRITICAL: Reset the job model in app_state ===
                if job_model:
                    print(f"[CONTINUATION] Resetting job model for {job_type}")
                    # COMPLETELY reset the job model
                    job_model.execution_status = JobStatus.SCHEDULED
                    job_model.relion_job_name = None
                    job_model.relion_job_number = None
                    
                    # Re-sync from pipeline state to get default parameters
                    # job_model.sync_from_pipeline_state(app_state)
                    
                    print(f"[CONTINUATION] Job model reset: status={job_model.execution_status}, job_name={job_model.relion_job_name}")
                else:
                    print(f"[CONTINUATION] Warning: No job model found for {job_type} in app_state")
                
                # Step 4: Get next job number (read current counter)
                pipeline_star_path = project_dir / "default_pipeline.star"
                from services.starfile_service import StarfileService
                star_handler = StarfileService()
                pipeline_data = star_handler.read(pipeline_star_path)
                general_data = pipeline_data.get("pipeline_general")
                
                current_counter = None
                if general_data is not None:
                    if isinstance(general_data, pd.DataFrame) and not general_data.empty:
                        current_counter = int(general_data.at[0, "rlnPipeLineJobCounter"])
                    elif isinstance(general_data, dict):
                        current_counter = int(general_data.get("rlnPipeLineJobCounter", 0))
                
                if current_counter is None:
                    current_counter = job_number + 1  # Fallback
                
                return {
                    "success": True,
                    "job_number": job_number,
                    "job_type": job_type,
                    "next_job_number": current_counter,
                    "pipeline_result": pipeline_result,
                    "scheme_result": scheme_result,
                    "message": f"Successfully deleted job{job_number:03d} ({job_type}). Next job will be job{current_counter:03d}."
                }
                
            except Exception as e:
                import traceback
                traceback.print_exc()
                return {
                    "success": False,
                    "error": f"Continuation operation failed: {str(e)}"
                }

    def prepare_for_continuation(
        self,
        project_path: str,
        scheme_name: str,
        jobs_to_skip: List[str]
    ) -> Dict[str, Any]:
        """
        Prepare a scheme for continuation by marking upstream jobs as completed.
        
        Use this when you want to restart a pipeline but skip jobs that have
        already succeeded.
        
        Args:
            project_path: Root project directory
            scheme_name: Scheme name
            jobs_to_skip: List of job types that should be marked as completed
                         (e.g., ["importmovies", "fsMotionAndCtf"])
            
        Returns:
            {
                "success": bool,
                "marked_jobs": List[str],
                "error": str (optional)
            }
        """
        project_dir = Path(project_path)
        
        return self.scheme_service.mark_jobs_as_completed(
            project_dir,
            scheme_name,
            jobs_to_skip
        )
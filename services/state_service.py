# services/state_service.py
"""
Minimal state service - just handles loading/saving and mdoc updates.
No sync tracking, no user modification tracking.
"""
from pathlib import Path
from typing import Optional, Dict, Any, List
import sys
from datetime import datetime

from services.project_state import JobType, ProjectState, get_project_state, set_project_state
from services.mdoc_service import get_mdoc_service
from services.config_service import get_config_service # Added this


class StateService:
    """Just handles persistence and mdoc updates"""
    
    def __init__(self):
        self._project_state = get_project_state()
    
    @property
    def state(self) -> ProjectState:
        return self._project_state
    
    async def update_from_mdoc(self, mdocs_glob: str):
        """
        Update global state from mdoc files.
        This directly mutates the single source of truth.
        """
        mdoc_service = get_mdoc_service()
        mdoc_data = mdoc_service.get_autodetect_params(mdocs_glob)
        
        if not mdoc_data:
            print(f"[WARN] No mdoc data found or parsed from: {mdocs_glob}")
            return
            
        print(f"[STATE] Updating from mdoc: {mdoc_data}")
        
        try:
            # Update microscope params directly
            if "pixel_spacing" in mdoc_data:
                self._project_state.microscope.pixel_size_angstrom = mdoc_data["pixel_spacing"]
            if "voltage" in mdoc_data:
                self._project_state.microscope.acceleration_voltage_kv = mdoc_data["voltage"]

            # Update acquisition params directly
            if "dose_per_tilt" in mdoc_data:
                 self._project_state.acquisition.dose_per_tilt = mdoc_data["dose_per_tilt"]
            if "frame_dose" in mdoc_data:
                self._project_state.acquisition.frame_dose = mdoc_data["frame_dose"]
            if "tilt_axis_angle" in mdoc_data:
                self._project_state.acquisition.tilt_axis_degrees = mdoc_data["tilt_axis_angle"]
            if "acquisition_software" in mdoc_data:
                self._project_state.acquisition.acquisition_software = mdoc_data["acquisition_software"]
            if "invert_tilt_angles" in mdoc_data:
                self._project_state.acquisition.invert_tilt_angles = mdoc_data["invert_tilt_angles"]
            if "detector_dimensions" in mdoc_data:
                self._project_state.acquisition.detector_dimensions = mdoc_data["detector_dimensions"]
            if "eer_fractions_per_frame" in mdoc_data:
                self._project_state.acquisition.eer_fractions_per_frame = mdoc_data["eer_fractions_per_frame"]
            if "nominal_magnification" in mdoc_data:
                self._project_state.acquisition.nominal_magnification = mdoc_data["nominal_magnification"]
            if "spot_size" in mdoc_data:
                self._project_state.acquisition.spot_size = mdoc_data["spot_size"]
            if "binning" in mdoc_data:
                self._project_state.acquisition.binning = mdoc_data["binning"]
                
            self._project_state.update_modified()
            print("[STATE] Global parameters updated from mdoc.")

        except Exception as e:
            print(f"[ERROR] Failed to update state from mdoc data: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
    
    async def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        """Ensure job exists in the state"""
        self._project_state.ensure_job_initialized(job_type, template_path)
    
    async def load_project(self, project_json_path: Path):
        """Load project, replacing the current global state"""
        try:
            self._project_state = ProjectState.load(project_json_path)
            set_project_state(self._project_state) # IMPORTANT: Set the global singleton
            return True
        except Exception as e:
            print(f"[ERROR] Failed to load project state from {project_json_path}: {e}", file=sys.stderr)
            return False
    
    async def save_project(self, save_path: Optional[Path] = None):
        """
        Save the current ProjectState model to its designated file.
        This is a direct serialization of the state object.
        """
        target_path = save_path or self._project_state.project_path / "project_params.json"
        
        if not target_path:
            raise ValueError("Cannot save project, project_path is not set and no save_path provided.")
            
        self._project_state.save(target_path)

    async def export_for_project(
        self, movies_glob: str, mdocs_glob: str, selected_jobs_str: List[str]
    ) -> Dict[str, Any]:
        """
        Gathers all state and config data into a single dictionary
        for project creation and serialization, similar to the old export_for_project.
        """
        print("[STATE] Exporting comprehensive project config")
        
        # Get services
        mdoc_service = get_mdoc_service()
        config_service = get_config_service()
        state = self._project_state
        
        # 1. Analyze Mdocs
        mdoc_stats = mdoc_service.parse_all_mdoc_files(mdocs_glob)

        # 2. Ensure all selected jobs are in the state
        template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        for job_str in selected_jobs_str:
            try:
                job_type = JobType(job_str)
                job_star_path = template_base / job_type.value / "job.star"
                state.ensure_job_initialized(
                    job_type,
                    job_star_path if job_star_path.exists() else None
                )
            except ValueError:
                print(f"[WARN] Skipping unknown job '{job_str}' during export.")

        # 3. Get container config
        containers = config_service.containers

        # 4. Build the export dictionary
        export = {
            "metadata": {
                "config_version": "3.0", # Bumped version for new state model
                "created_by": "CryoBoost Parameter Manager",
                "created_at": datetime.now().isoformat(),
                "project_name": state.project_name,
                "mdoc_analysis": mdoc_stats, 
            },
            "data_sources": {
                "frames_glob": movies_glob,
                "mdocs_glob": mdocs_glob,
                "gain_reference": state.acquisition.gain_reference_path,
            },
            "containers": containers,
            "microscope": state.microscope.model_dump(),
            "acquisition": state.acquisition.model_dump(),
            "computing": state.computing.model_dump(),
            "jobs": {
                job_str: state.jobs[JobType(job_str)].model_dump()
                for job_str in selected_jobs_str
                if JobType(job_str) in state.jobs
            },
        }

        return export


# Singleton
_state_service_instance = None

def get_state_service():
    global _state_service_instance
    if _state_service_instance is None:
        _state_service_instance = StateService()
    return _state_service_instance
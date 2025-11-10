# # app_state.py
# """
# Centralized application state management.
# This is the single source of truth for all application state.
# """

# import glob
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Type
# from datetime import datetime
# import json

# from pydantic import BaseModel, Field, ConfigDict

# from services.state_old.parameter_models import (
#     JobType,  
#     MicroscopeParams,
#     AcquisitionParams,
#     ComputingParams,
#     AbstractJobParams,
#     ImportMoviesParams,
#     FsMotionCtfParams,
#     TsAlignmentParams,
#     jobtype_paramclass,
# )

# from services.state_old.mdoc_service import get_mdoc_service

# class PipelineState(BaseModel):
#     """Central state with hierarchical organization"""

#     model_config = ConfigDict(validate_assignment=True)

#     microscope : MicroscopeParams             = Field(default_factory=MicroscopeParams)
#     acquisition: AcquisitionParams            = Field(default_factory=AcquisitionParams)
#     computing  : ComputingParams              = Field(default_factory=ComputingParams)
#     jobs       : Dict[str, AbstractJobParams] = Field(default_factory=dict)

#     # Metadata
#     created_at: datetime = Field(default_factory=datetime.now)
#     modified_at: datetime = Field(default_factory=datetime.now)

#     def populate_job(self, job_type: JobType, job_star_path: Optional[Path] = None):
#         param_classes = jobtype_paramclass()
#         param_class = param_classes.get(job_type)

#         if not param_class:
#             raise ValueError(f"Unknown job type: {job_type}")

#         # Try loading template defaults first
#         job_params = param_class.from_job_star(job_star_path) if job_star_path else None

#         # Create new from state OR sync existing with current state
#         if job_params is None:
#             job_params = param_class.from_pipeline_state(self)
#             print(f"[STATE] Created {job_type.value} from pipeline state")
#         else:
#             job_params.sync_from_pipeline_state(self)
#             print(f"[STATE] Loaded {job_type.value} from job.star and synced with pipeline state")

#         # Store in jobs dict (UI binds to this)
#         self.jobs[job_type.value] = job_params
#         self.update_modified()

#     def update_modified(self):
#         """Update the modified timestamp"""
#         self.modified_at = datetime.now()


# def prepare_job_params(job_name_or_type):
#     """
#     Prepare job parameters - ensure they're properly synced with global state
#     """
#     if isinstance(job_name_or_type, str):
#         job_type = JobType.from_string(job_name_or_type)
#     else:
#         job_type = job_name_or_type

#     template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
#     job_star_path = template_base / job_type.value / "job.star"

#     state.populate_job(job_type, job_star_path if job_star_path.exists() else None)

#     job_model = state.jobs.get(job_type.value)
#     if job_model and hasattr(job_model, "sync_from_pipeline_state"):
#         job_model.sync_from_pipeline_state(state)
#         print(f"[STATE] Force-synced {job_type.value} with current global state")

#     return state.jobs.get(job_type.value)

# def update_from_mdoc(mdocs_glob: str):
#     """
#     Parse first mdoc file and update microscope/acquisition params.
#     This mutates state.microscope and state.acquisition.
#     Enhanced to match old CryoBoost logic.
#     """
#     mdoc_service = get_mdoc_service()
#     mdoc_data = mdoc_service.get_autodetect_params(mdocs_glob)

#     if not mdoc_data:
#         print(f"[WARN] No mdoc data found or parsed from: {mdocs_glob}")
#         return

#     try:
#         # Update microscope params
#         if "pixel_spacing" in mdoc_data:
#             state.microscope.pixel_size_angstrom = mdoc_data["pixel_spacing"]
#         if "voltage" in mdoc_data:
#             state.microscope.acceleration_voltage_kv = mdoc_data["voltage"]

#         # Update acquisition params with NEW fields
#         if "exposure_dose" in mdoc_data:
#             # OLD LOGIC: Use the scaled value that mdoc_service now calculates
#             state.acquisition.dose_per_tilt = mdoc_data.get("dose_per_tilt", mdoc_data["exposure_dose"] * 1.5)
#             # Also store frame dose if available
#             if "frame_dose" in mdoc_data:
#                 state.acquisition.frame_dose = mdoc_data["frame_dose"]

#         if "tilt_axis_angle" in mdoc_data:
#             state.acquisition.tilt_axis_degrees = mdoc_data["tilt_axis_angle"]

#         # NEW: Set acquisition software and related parameters
#         if "acquisition_software" in mdoc_data:
#             state.acquisition.acquisition_software = mdoc_data["acquisition_software"]
            
#         # NEW: Set inversion logic based on acquisition software
#         if "invert_tilt_angles" in mdoc_data:
#             state.acquisition.invert_tilt_angles = mdoc_data["invert_tilt_angles"]

#         # Parse detector dimensions
#         if "image_size" in mdoc_data:
#             dims = mdoc_data["image_size"].split("x")
#             if len(dims) == 2:
#                 state.acquisition.detector_dimensions = (int(dims[0]), int(dims[1]))

#         # NEW: Enhanced EER detection
#         if "eer_fractions_per_frame" in mdoc_data:
#             state.acquisition.eer_fractions_per_frame = mdoc_data["eer_fractions_per_frame"]
#         elif "5760" in str(mdoc_data.get("image_size", "")) or "11520" in str(mdoc_data.get("image_size", "")):
#             state.acquisition.eer_fractions_per_frame = 32
#             print("[STATE] Detected K3/EER camera, set fractions to 32")

#         # NEW: Set additional acquisition parameters
#         if "nominal_magnification" in mdoc_data:
#             state.acquisition.nominal_magnification = mdoc_data["nominal_magnification"]
#         if "spot_size" in mdoc_data:
#             state.acquisition.spot_size = mdoc_data["spot_size"]
#         if "binning" in mdoc_data:
#             state.acquisition.binning = mdoc_data["binning"]

#         state.update_modified()

#         # Sync all jobs with updated global parameters
#         for job_name in list(state.jobs.keys()):
#             _sync_job_with_global_params(job_name)

#         print(f"[STATE] Updated from mdoc: {mdoc_data}")

#     except Exception as e:
#         print(f"[ERROR] Failed to update state from mdoc data: {e}")
#         import traceback
#         traceback.print_exc()

# def export_for_project(
#     project_name: str, movies_glob: str, mdocs_glob: str, selected_jobs: List[str]
# ) -> Dict[str, Any]:
#     print("[STATE] Exporting project config")

#     mdoc_stats = analyze_all_mdocs(mdocs_glob)

#     for job in selected_jobs:
#         if job not in state.jobs:
#             prepare_job_params(job)

#     containers = {}
#     try:
#         import yaml
#         with open("config/conf.yaml") as f:
#             conf = yaml.safe_load(f)
#             containers = conf.get("containers", {})
#     except Exception as e:
#         print(f"[WARN] Could not load containers from conf.yaml: {e}")

#     export = {
#         "metadata": {
#             "config_version": "2.0",
#             "created_by": "CryoBoost Parameter Manager",
#             "created_at": datetime.now().isoformat(),
#             "project_name": project_name,
#             "mdoc_analysis": mdoc_stats, 
#         },
#         "data_sources": {
#             "frames_glob": movies_glob,
#             "mdocs_glob": mdocs_glob,
#             "gain_reference": state.acquisition.gain_reference_path,
#         },
#         "containers": containers,
#         "microscope": state.microscope.model_dump(),
#         "acquisition": state.acquisition.model_dump(),
#         "computing": state.computing.model_dump(),
#         "jobs": {job: state.jobs[job].model_dump() for job in selected_jobs if job in state.jobs},
#     }

#     return export

# def save_state_to_file(path: Path):
#     """Save current state to JSON file"""
#     try:
#         state_dict = {
#             "microscope": state.microscope.model_dump(),
#             "acquisition": state.acquisition.model_dump(),
#             "computing": state.computing.model_dump(),
#             "jobs": {name: params.model_dump() for name, params in state.jobs.items()},
#             "metadata": {"created_at": state.created_at.isoformat(), "modified_at": state.modified_at.isoformat()},
#         }

#         with open(path, "w") as f:
#             json.dump(state_dict, f, indent=2)

#         print(f"[STATE] Saved to {path}")

#     except Exception as e:
#         print(f"[ERROR] Failed to save state to {path}: {e}")

# def load_state_from_file(path: Path):
#     """Load state from JSON file - mutates global state"""
#     try:
#         with open(path, "r") as f:
#             data = json.load(f)

#         if "microscope" in data:
#             state.microscope = MicroscopeParams(**data["microscope"])
#         if "acquisition" in data:
#             state.acquisition = AcquisitionParams(**data["acquisition"])
#         if "computing" in data:
#             state.computing = ComputingParams(**data["computing"])

#         # Load jobs
#         if "jobs" in data:
#             for job_name, job_data in data["jobs"].items():
#                 if job_name == "importmovies":
#                     state.jobs[job_name] = ImportMoviesParams(**job_data)
#                 elif job_name == "fsMotionAndCtf":
#                     state.jobs[job_name] = FsMotionCtfParams(**job_data)
#                 elif job_name == "tsAlignment":
#                     state.jobs[job_name] = TsAlignmentParams(**job_data)

#         print(f"[STATE] Loaded from {path}")

#     except Exception as e:
#         print(f"[ERROR] Failed to load state from {path}: {e}")

# def _sync_job_with_global_params(job_name: str):
#     """
#     Sync a job's params with current global state values.
#     Enhanced to handle new acquisition fields.
#     """
#     if job_name not in state.jobs:
#         return

#     job = state.jobs[job_name]

#     # Update common parameters that jobs inherit from global state
#     if hasattr(job, "pixel_size"):
#         job.pixel_size = state.microscope.pixel_size_angstrom
#     if hasattr(job, "voltage"):
#         job.voltage = state.microscope.acceleration_voltage_kv
#     if hasattr(job, "spherical_aberration"):
#         job.spherical_aberration = state.microscope.spherical_aberration_mm
#     if hasattr(job, "amplitude_contrast"):
#         job.amplitude_contrast = state.microscope.amplitude_contrast
#     if hasattr(job, "cs"):
#         job.cs = state.microscope.spherical_aberration_mm
#     if hasattr(job, "amplitude"):
#         job.amplitude = state.microscope.amplitude_contrast
#     if hasattr(job, "dose_per_tilt_image"):
#         job.dose_per_tilt_image = state.acquisition.dose_per_tilt
#     if hasattr(job, "tilt_axis_angle"):
#         job.tilt_axis_angle = state.acquisition.tilt_axis_degrees
#     if hasattr(job, "eer_ngroups"):
#         job.eer_ngroups = state.acquisition.eer_fractions_per_frame or 32
        
#     # NEW: Sync additional acquisition fields for alignment jobs
#     if job_name == "tsAlignment":
#         if hasattr(job, "thickness_nm"):
#             job.thickness_nm = state.acquisition.sample_thickness_nm
#         if hasattr(job, "invert_tilt_angles"):
#             job.invert_tilt_angles = state.acquisition.invert_tilt_angles
#         if hasattr(job, "gain_path"):
#             job.gain_path = state.acquisition.gain_reference_path

#     print(f"[STATE] Synced {job_name} with global params")

# def is_job_synced_with_global(job_type: JobType) -> bool:
#     """Check if job params match global params - with proper comparison"""
#     job_model = state.jobs.get(job_type.value)
#     if not job_model:
#         return True

#     # Define sync mappings for each job type with tolerance for floating point
#     sync_mappings = {
#         JobType.IMPORT_MOVIES: {
#             "pixel_size": state.microscope.pixel_size_angstrom,
#             "voltage": state.microscope.acceleration_voltage_kv,
#             "spherical_aberration": state.microscope.spherical_aberration_mm,
#             "amplitude_contrast": state.microscope.amplitude_contrast,
#             "dose_per_tilt_image": state.acquisition.dose_per_tilt,
#             "tilt_axis_angle": state.acquisition.tilt_axis_degrees,
#             "invert_defocus_hand": state.acquisition.invert_defocus_hand,
#         },
#         JobType.FS_MOTION_CTF: {
#             "pixel_size": state.microscope.pixel_size_angstrom,
#             "voltage": state.microscope.acceleration_voltage_kv,
#             "cs": state.microscope.spherical_aberration_mm,
#             "amplitude": state.microscope.amplitude_contrast,
#             "eer_ngroups": state.acquisition.eer_fractions_per_frame or 32,
#         },
#         JobType.TS_ALIGNMENT: {
#             "thickness_nm": state.acquisition.sample_thickness_nm,
#             "pixel_size": state.microscope.pixel_size_angstrom,
#             "dose_per_tilt": state.acquisition.dose_per_tilt,
#             "tilt_axis_angle": state.acquisition.tilt_axis_degrees,
#             "invert_tilt_angles": state.acquisition.invert_tilt_angles,
#             "gain_path": state.acquisition.gain_reference_path,
#         },
#     }

#     mapping = sync_mappings.get(job_type, {})

#     for field, global_value in mapping.items():
#         job_value = getattr(job_model, field, None)

#         # Handle None comparisons
#         if global_value is None and job_value is None:
#             continue
#         if global_value is None or job_value is None:
#             print(f"[SYNC CHECK] {job_type.value}.{field}: job={job_value}, global={global_value} → OUT OF SYNC (None mismatch)")
#             return False

#         if isinstance(global_value, float) and isinstance(job_value, float):
#             if abs(job_value - global_value) > 1e-6:
#                 print(f"[SYNC CHECK] {job_type.value}.{field}: job={job_value}, global={global_value} → OUT OF SYNC")
#                 return False
#         elif job_value != global_value:
#             print(f"[SYNC CHECK] {job_type.value}.{field}: job={job_value}, global={global_value} → OUT OF SYNC")
#             return False

#     return True

# def analyze_all_mdocs(mdocs_glob: str) -> Dict[str, Any]:
#     """
#     Comprehensive analysis of all mdoc files.
#     Returns statistics and consistency checks.
#     """
#     mdoc_service = get_mdoc_service()
#     mdoc_stats = mdoc_service.parse_all_mdoc_files(mdocs_glob)
    
#     if not mdoc_stats:
#         return {"error": "No mdoc files found or could not be parsed"}
    
#     # Update state with comprehensive findings
#     if mdoc_stats.get("consistent_params"):
#         print(f"[STATE] All {mdoc_stats['tilt_series_count']} tilt series have consistent parameters")
#     else:
#         print(f"[WARN] Inconsistent parameters detected across {mdoc_stats['tilt_series_count']} tilt series")
    
#     # Update tilt range information
#     if "tilt_range" in mdoc_stats:
#         min_tilt, max_tilt = mdoc_stats["tilt_range"]
#         print(f"[STATE] Tilt range: {min_tilt:.1f}° to {max_tilt:.1f}°")
    
#     return mdoc_stats

# def sync_job_with_global(job_type: JobType):
#     """Syncs a specific job's model with the current global state."""
#     job_model = state.jobs.get(job_type.value)
#     if job_model and hasattr(job_model, "sync_from_pipeline_state"):
#         job_model.sync_from_pipeline_state(state)
#         print(f"[STATE] Synced {job_type.value} from global state")


# state = PipelineState(computing=ComputingParams.from_conf_yaml(Path("config/conf.yaml")))
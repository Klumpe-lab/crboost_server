# app_state.py
"""
Centralized application state management.
This is the single source of truth for all application state.
"""

import glob
from pathlib import Path
from typing import Dict, Any, List, Optional, Type
from datetime import datetime
import json

# --- NEW: Imports for moved PipelineState ---
from pydantic import BaseModel, Field, ConfigDict

# --- UPDATED: Imports from parameter_models ---
from services.parameter_models import (
    JobType,
    MicroscopeParams,
    AcquisitionParams,
    ComputingParams,
    AbstractJobParams,
    ImportMoviesParams,
    FsMotionCtfParams,
    TsAlignmentParams,
    jobtype_paramclass,  # <-- NEW
)

# --- NEW: Import the MdocService ---
from services.mdoc_service import get_mdoc_service


# -----------------------------------------------------------------
# --- PipelineState class MOVED HERE from parameter_models.py ---
# -----------------------------------------------------------------
class PipelineState(BaseModel):
    """Central state with hierarchical organization"""

    model_config = ConfigDict(validate_assignment=True)

    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams = Field(default_factory=AcquisitionParams)
    computing: ComputingParams = Field(default_factory=ComputingParams)
    jobs: Dict[str, AbstractJobParams] = Field(default_factory=dict)

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    modified_at: datetime = Field(default_factory=datetime.now)

    def populate_job(self, job_type: JobType, job_star_path: Optional[Path] = None):
        param_classes = jobtype_paramclass()
        param_class = param_classes.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_type}")

        # Try loading template defaults first
        job_params = param_class.from_job_star(job_star_path) if job_star_path else None

        # Create new from state OR sync existing with current state
        if job_params is None:
            job_params = param_class.from_pipeline_state(self)
            print(f"[STATE] Created {job_type.value} from pipeline state")
        else:
            job_params.sync_from_pipeline_state(self)
            print(f"[STATE] Loaded {job_type.value} from job.star and synced with pipeline state")

        # Store in jobs dict (UI binds to this)
        self.jobs[job_type.value] = job_params
        self.update_modified()

    def update_modified(self):
        """Update the modified timestamp"""
        self.modified_at = datetime.now()


# -----------------------------------------------------------------
# --- Global State Initialization ---
# -----------------------------------------------------------------

state = PipelineState(computing=ComputingParams.from_conf_yaml(Path("config/conf.yaml")))

# --- UPDATED: Use .model_dump() ---
print(f"[APP STATE] Initialized with computing: {state.computing.model_dump()}")

# --- DELETED: Redundant state initialization ---
# state = PipelineState()


def prepare_job_params(job_name_or_type):
    """
    Prepare job parameters - ensure they're properly synced with global state
    """
    if isinstance(job_name_or_type, str):
        job_type = JobType.from_string(job_name_or_type)
    else:
        job_type = job_name_or_type

    template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
    job_star_path = template_base / job_type.value / "job.star"

    # Use populate_job but then force sync with current global state
    state.populate_job(job_type, job_star_path if job_star_path.exists() else None)

    # Ensure the job is synced with current global state
    job_model = state.jobs.get(job_type.value)
    if job_model and hasattr(job_model, "sync_from_pipeline_state"):
        job_model.sync_from_pipeline_state(state)
        print(f"[STATE] Force-synced {job_type.value} with current global state")

    return state.jobs.get(job_type.value)


def update_from_mdoc(mdocs_glob: str):
    """
    Parse first mdoc file and update microscope/acquisition params.
    This mutates state.microscope and state.acquisition.
    """
    # --- UPDATED: Use MdocService ---
    print(f"[STATE] Parsing mdoc glob: {mdocs_glob}")
    mdoc_service = get_mdoc_service()
    mdoc_data = mdoc_service.get_autodetect_params(mdocs_glob)

    if not mdoc_data:
        print(f"[WARN] No mdoc data found or parsed from: {mdocs_glob}")
        return

    try:
        # Update microscope params
        if "pixel_spacing" in mdoc_data:
            state.microscope.pixel_size_angstrom = mdoc_data["pixel_spacing"]
        if "voltage" in mdoc_data:
            state.microscope.acceleration_voltage_kv = mdoc_data["voltage"]

        # Update acquisition params
        if "exposure_dose" in mdoc_data:
            dose = mdoc_data["exposure_dose"] * 1.5  # Scale as per original logic
            dose = max(0.1, min(9.0, dose))  # Clamp
            state.acquisition.dose_per_tilt = dose

        if "tilt_axis_angle" in mdoc_data:
            state.acquisition.tilt_axis_degrees = mdoc_data["tilt_axis_angle"]

        # Parse detector dimensions
        if "image_size" in mdoc_data:
            dims = mdoc_data["image_size"].split("x")
            if len(dims) == 2:
                state.acquisition.detector_dimensions = (int(dims[0]), int(dims[1]))

                # Detect K3/EER based on dimensions
                if "5760" in mdoc_data["image_size"] or "11520" in mdoc_data["image_size"]:
                    state.acquisition.eer_fractions_per_frame = 32
                    print("[STATE] Detected K3/EER camera, set fractions to 32")

        state.update_modified()

        # Update any existing jobs with new global values
        for job_name in list(state.jobs.keys()):
            _sync_job_with_global_params(job_name)

        print(f"[STATE] Updated from mdoc")

    except Exception as e:
        print(f"[ERROR] Failed to update state from mdoc data: {e}")
        import traceback

        traceback.print_exc()


def export_for_project(
    project_name: str, movies_glob: str, mdocs_glob: str, selected_jobs: List[str]
) -> Dict[str, Any]:
    """
    Export clean configuration for project creation.
    This reads from state but doesn't mutate it.
    """
    print("[STATE] Exporting project config")

    # Ensure all selected jobs have params
    for job in selected_jobs:
        if job not in state.jobs:
            # Note: prepare_job_params takes job_name_or_type, not template_path
            prepare_job_params(job)

    # Read containers from config
    containers = {}
    try:
        import yaml

        with open("config/conf.yaml") as f:
            conf = yaml.safe_load(f)
            containers = conf.get("containers", {})
    except Exception as e:
        print(f"[WARN] Could not load containers from conf.yaml: {e}")

    # --- UPDATED: Use .model_dump() ---
    export = {
        "metadata": {
            "config_version": "2.0",
            "created_by": "CryoBoost Parameter Manager",
            "created_at": datetime.now().isoformat(),
            "project_name": project_name,
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
        "jobs": {job: state.jobs[job].model_dump() for job in selected_jobs if job in state.jobs},
    }

    return export


def save_state_to_file(path: Path):
    """Save current state to JSON file"""
    try:
        # --- UPDATED: Use .model_dump() ---
        state_dict = {
            "microscope": state.microscope.model_dump(),
            "acquisition": state.acquisition.model_dump(),
            "computing": state.computing.model_dump(),
            "jobs": {name: params.model_dump() for name, params in state.jobs.items()},
            "metadata": {"created_at": state.created_at.isoformat(), "modified_at": state.modified_at.isoformat()},
        }

        with open(path, "w") as f:
            json.dump(state_dict, f, indent=2)

        print(f"[STATE] Saved to {path}")

    except Exception as e:
        print(f"[ERROR] Failed to save state to {path}: {e}")


def load_state_from_file(path: Path):
    """Load state from JSON file - mutates global state"""
    try:
        with open(path, "r") as f:
            data = json.load(f)

        if "microscope" in data:
            state.microscope = MicroscopeParams(**data["microscope"])
        if "acquisition" in data:
            state.acquisition = AcquisitionParams(**data["acquisition"])
        if "computing" in data:
            state.computing = ComputingParams(**data["computing"])

        # Load jobs
        if "jobs" in data:
            for job_name, job_data in data["jobs"].items():
                if job_name == "importmovies":
                    state.jobs[job_name] = ImportMoviesParams(**job_data)
                elif job_name == "fsMotionAndCtf":
                    state.jobs[job_name] = FsMotionCtfParams(**job_data)
                elif job_name == "tsAlignment":
                    state.jobs[job_name] = TsAlignmentParams(**job_data)

        print(f"[STATE] Loaded from {path}")

    except Exception as e:
        print(f"[ERROR] Failed to load state from {path}: {e}")


# ============= HELPER FUNCTIONS =============


def _sync_job_with_global_params(job_name: str):
    """
    Sync a job's params with current global state values.
    Called after mdoc detection or when job is first created.
    """
    if job_name not in state.jobs:
        return

    job = state.jobs[job_name]

    # Update common parameters that jobs inherit from global state
    if hasattr(job, "pixel_size"):
        job.pixel_size = state.microscope.pixel_size_angstrom
    if hasattr(job, "voltage"):
        job.voltage = state.microscope.acceleration_voltage_kv
    if hasattr(job, "spherical_aberration"):
        job.spherical_aberration = state.microscope.spherical_aberration_mm
    if hasattr(job, "amplitude_contrast"):
        job.amplitude_contrast = state.microscope.amplitude_contrast
    if hasattr(job, "cs"):
        job.cs = state.microscope.spherical_aberration_mm
    if hasattr(job, "amplitude"):
        job.amplitude = state.microscope.amplitude_contrast
    if hasattr(job, "dose_per_tilt_image"):
        job.dose_per_tilt_image = state.acquisition.dose_per_tilt
    if hasattr(job, "tilt_axis_angle"):
        job.tilt_axis_angle = state.acquisition.tilt_axis_degrees
    if hasattr(job, "eer_ngroups"):
        job.eer_ngroups = state.acquisition.eer_fractions_per_frame or 32

    print(f"[STATE] Synced {job_name} with global params")


# --- DELETED: _parse_mdoc function is now in MdocService ---


# --- DELETED: get_ui_state_legacy function ---

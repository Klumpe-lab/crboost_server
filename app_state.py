# app_state.py
"""
Centralized application state management.
This is the single source of truth for all application state.
"""

import glob
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import json

from services.parameter_models import (
    PipelineState,
    ComputingParams,
    MicroscopeParams,
    AcquisitionParams,
    ImportMoviesParams,
    FsMotionCtfParams,
    TsAlignmentParams,
)

# ============= GLOBAL STATE INSTANCE =============
# This is the single source of truth - import this in any module that needs state
state = PipelineState(
    computing=ComputingParams.from_conf_yaml(Path("config/conf.yaml"))
)

print(f"[APP STATE] Initialized with computing: {state.computing.dict()}")


# ============= STATE MUTATORS (aka Reducers) =============
# These are the ONLY functions that should modify the global state


def prepare_job_params(job_name: str, job_star_path: Optional[Path] = None):
    """
    Populate job parameters in the state, loading from job.star if available.
    This mutates state.jobs[job_name].
    """
    if job_name not in state.jobs:
        state.populate_job(job_name, job_star_path)
        print(f"[STATE] Prepared job: {job_name}")
    return state.jobs.get(job_name)


def update_from_mdoc(mdocs_glob: str):
    """
    Parse first mdoc file and update microscope/acquisition params.
    This mutates state.microscope and state.acquisition.
    """
    mdoc_files = glob.glob(mdocs_glob)
    if not mdoc_files:
        print(f"[WARN] No mdoc files found at: {mdocs_glob}")
        return

    try:
        mdoc_path = Path(mdoc_files[0])
        print(f"[STATE] Parsing mdoc: {mdoc_path}")
        mdoc_data = _parse_mdoc(mdoc_path)

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
                if (
                    "5760" in mdoc_data["image_size"]
                    or "11520" in mdoc_data["image_size"]
                ):
                    state.acquisition.eer_fractions_per_frame = 32
                    print("[STATE] Detected K3/EER camera, set fractions to 32")

        state.update_modified()

        # Update any existing jobs with new global values
        for job_name in list(state.jobs.keys()):
            _sync_job_with_global_params(job_name)

        print(f"[STATE] Updated from mdoc: {len(mdoc_files)} files found")

    except Exception as e:
        print(f"[ERROR] Failed to parse mdoc {mdoc_files[0]}: {e}")
        import traceback

        traceback.print_exc()


def export_for_project(
    project_name: str,
    movies_glob: str,
    mdocs_glob: str,
    selected_jobs: List[str],
) -> Dict[str, Any]:
    """
    Export clean configuration for project creation.
    This reads from state but doesn't mutate it.
    """
    print("[STATE] Exporting project config")

    # Ensure all selected jobs have params
    for job in selected_jobs:
        if job not in state.jobs:
            template_path = Path("config/Schemes/warp_tomo_prep") / job / "job.star"
            prepare_job_params(job, template_path if template_path.exists() else None)

    # Read containers from config
    containers = {}
    try:
        import yaml

        with open("config/conf.yaml") as f:
            conf = yaml.safe_load(f)
            containers = conf.get("containers", {})
    except Exception as e:
        print(f"[WARN] Could not load containers from conf.yaml: {e}")

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
        "microscope": state.microscope.dict(),
        "acquisition": state.acquisition.dict(),
        "computing": state.computing.dict(),
        "jobs": {
            job: state.jobs[job].dict() for job in selected_jobs if job in state.jobs
        },
    }

    return export


def save_state_to_file(path: Path):
    """Save current state to JSON file"""
    try:
        state_dict = {
            "microscope": state.microscope.dict(),
            "acquisition": state.acquisition.dict(),
            "computing": state.computing.dict(),
            "jobs": {name: params.dict() for name, params in state.jobs.items()},
            "metadata": {
                "created_at": state.created_at.isoformat(),
                "modified_at": state.modified_at.isoformat(),
            },
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


def _parse_mdoc(mdoc_path: Path) -> Dict[str, Any]:
    """Parse mdoc file for key metadata"""
    result = {}
    header_data = {}
    first_section = {}
    in_zvalue_section = False

    with open(mdoc_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if line.startswith("[ZValue"):
                in_zvalue_section = True
            elif in_zvalue_section and "=" in line:
                key, value = [x.strip() for x in line.split("=", 1)]
                first_section[key] = value
            elif not in_zvalue_section and "=" in line:
                key, value = [x.strip() for x in line.split("=", 1)]
                header_data[key] = value

    # Extract values, preferring header, falling back to first section
    if "PixelSpacing" in header_data:
        result["pixel_spacing"] = float(header_data["PixelSpacing"])
    elif "PixelSpacing" in first_section:
        result["pixel_spacing"] = float(first_section["PixelSpacing"])

    if "Voltage" in header_data:
        result["voltage"] = float(header_data["Voltage"])
    elif "Voltage" in first_section:
        result["voltage"] = float(first_section["Voltage"])

    if "ImageSize" in header_data:
        result["image_size"] = header_data["ImageSize"].replace(" ", "x")
    elif "ImageSize" in first_section:
        result["image_size"] = first_section["ImageSize"].replace(" ", "x")

    if "ExposureDose" in first_section:
        result["exposure_dose"] = float(first_section["ExposureDose"])
    elif "ExposureDose" in header_data:
        result["exposure_dose"] = float(header_data["ExposureDose"])

    if "TiltAxisAngle" in first_section:
        result["tilt_axis_angle"] = float(first_section["TiltAxisAngle"])
    elif "Tilt axis angle" in header_data:
        result["tilt_axis_angle"] = float(header_data["Tilt axis angle"])

    return result


def get_ui_state_legacy() -> Dict[str, Any]:
    """
    Get state in legacy flat format for backward compatibility.
    This is a READ operation, doesn't mutate state.
    """
    ui_state = {
        # Flat parameters for backward compatibility
        "pixel_size_angstrom": {
            "value": state.microscope.pixel_size_angstrom,
            "source": "user",
        },
        "acceleration_voltage_kv": {
            "value": state.microscope.acceleration_voltage_kv,
            "source": "user",
        },
        "spherical_aberration_mm": {
            "value": state.microscope.spherical_aberration_mm,
            "source": "user",
        },
        "amplitude_contrast": {
            "value": state.microscope.amplitude_contrast,
            "source": "user",
        },
        "dose_per_tilt": {
            "value": state.acquisition.dose_per_tilt,
            "source": "user",
        },
        "detector_dimensions": {
            "value": state.acquisition.detector_dimensions,
            "source": "user",
        },
        "tilt_axis_degrees": {
            "value": state.acquisition.tilt_axis_degrees,
            "source": "user",
        },
        "eer_fractions_per_frame": {
            "value": state.acquisition.eer_fractions_per_frame,
            "source": "user",
        }
        if state.acquisition.eer_fractions_per_frame
        else None,
        # Hierarchical format
        "microscope": state.microscope.dict(),
        "acquisition": state.acquisition.dict(),
        "computing": state.computing.dict(),
        "jobs": {name: params.dict() for name, params in state.jobs.items()},
    }

    return {k: v for k, v in ui_state.items() if v is not None}

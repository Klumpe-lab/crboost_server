# services/project_state.py
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Type, List

from pydantic import BaseModel, Field

from services.models_base import (
    JobStatus, MicroscopeType, AlignmentMethod, JobCategory, 
    JobType, MicroscopeParams, AcquisitionParams
)
from services.computing.slurm_service import SlurmConfig
from services.job_models import (
    AbstractJobParams, CandidateExtractPytomParams, DenoisePredictParams, 
    DenoiseTrainParams, FsMotionCtfParams, ImportMoviesParams, 
    SubtomoExtractionParams, TemplateMatchPytomParams, TsAlignmentParams, 
    TsCtfParams, TsReconstructParams
)

class ProjectState(BaseModel):
    """Complete project state with direct global parameter access"""

    project_name    : str            = "Untitled"
    project_path    : Optional[Path] = None
    created_at      : datetime        = Field(default_factory=datetime.now)
    modified_at     : datetime        = Field(default_factory=datetime.now)
    job_path_mapping: Dict[str, str] = Field(default_factory=dict)

    movies_glob: str = ""
    mdocs_glob: str = ""

    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams = Field(default_factory=AcquisitionParams)
    slurm_defaults: SlurmConfig = Field(default_factory=SlurmConfig.from_config_defaults)

    jobs: Dict[JobType, AbstractJobParams] = Field(default_factory=dict)

    def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        if job_type in self.jobs:
            return

        from services.project_state import jobtype_paramclass
        param_class_map = jobtype_paramclass()
        param_class = param_class_map.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_type}")

        job_params = param_class()
        job_params._project_state = self
        self.jobs[job_type] = job_params
        self.update_modified()

    def update_modified(self):
        self.modified_at = datetime.now()

    def save(self, path: Optional[Path] = None):
        save_path = path or (
            self.project_path / "project_params.json" if self.project_path else Path("project_params.json")
        )
        save_path.parent.mkdir(parents=True, exist_ok=True)

        data = self.model_dump(exclude={"project_path"})
        data["project_path"] = str(self.project_path) if self.project_path else None
        
        with open(save_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    @classmethod
    def load(cls, path: Path):
        if not path.exists():
            raise FileNotFoundError(f"Project params file not found: {path}")

        with open(path, "r") as f:
            data = json.load(f)

        project_state = cls(
            project_name=data.get("project_name", "Untitled"),
            project_path=Path(data["project_path"]) if data.get("project_path") else None,
            created_at=datetime.fromisoformat(data.get("created_at", datetime.now().isoformat())),
            modified_at=datetime.fromisoformat(data.get("modified_at", datetime.now().isoformat())),
            movies_glob=data.get("movies_glob", ""),
            mdocs_glob=data.get("mdocs_glob", ""),
            microscope=MicroscopeParams(**data.get("microscope", {})),
            acquisition=AcquisitionParams(**data.get("acquisition", {})),
        )

        from services.project_state import jobtype_paramclass
        param_class_map = jobtype_paramclass()
        for job_type_str, job_data in data.get("jobs", {}).items():
            try:
                job_type = JobType(job_type_str)
                param_class = param_class_map.get(job_type)
                if param_class:
                    job_params = param_class(**job_data)
                    job_params._project_state = project_state
                    project_state.jobs[job_type] = job_params
            except ValueError:
                pass

        return project_state

def jobtype_paramclass() -> Dict[JobType, Type[AbstractJobParams]]:
    return {
        JobType.IMPORT_MOVIES         : ImportMoviesParams,
        JobType.FS_MOTION_CTF         : FsMotionCtfParams,
        JobType.TS_ALIGNMENT          : TsAlignmentParams,
        JobType.TS_CTF                : TsCtfParams,
        JobType.TS_RECONSTRUCT        : TsReconstructParams,
        JobType.DENOISE_TRAIN         : DenoiseTrainParams,
        JobType.DENOISE_PREDICT       : DenoisePredictParams,
        JobType.TEMPLATE_MATCH_PYTOM  : TemplateMatchPytomParams,
        JobType.TEMPLATE_EXTRACT_PYTOM: CandidateExtractPytomParams,
        JobType.SUBTOMO_EXTRACTION     : SubtomoExtractionParams,
    }

# --- Global Singleton Management ---

_project_state = None

def get_project_state() -> ProjectState:
    global _project_state
    if _project_state is None:
        _project_state = ProjectState()
    return _project_state

def set_project_state(new_state: ProjectState):
    global _project_state
    _project_state = new_state

def reset_project_state():
    """Forces the creation of a fresh ProjectState instance."""
    global _project_state
    print("[STATE] Resetting Global Project State to defaults.")
    _project_state = ProjectState()
    return _project_state

class StateService:
    @property
    def state(self) -> ProjectState:
        return get_project_state()

    async def update_from_mdoc(self, mdocs_glob: str):
        from services.configs.mdoc_service import get_mdoc_service
        mdoc_service = get_mdoc_service()
        mdoc_data = mdoc_service.get_autodetect_params(mdocs_glob)
        if not mdoc_data: return

        s = self.state
        if "pixel_spacing" in mdoc_data: s.microscope.pixel_size_angstrom = mdoc_data["pixel_spacing"]
        if "voltage" in mdoc_data: s.microscope.acceleration_voltage_kv = mdoc_data["voltage"]
        if "dose_per_tilt" in mdoc_data: s.acquisition.dose_per_tilt = mdoc_data["dose_per_tilt"]
        if "tilt_axis_angle" in mdoc_data: s.acquisition.tilt_axis_degrees = mdoc_data["tilt_axis_angle"]
        s.update_modified()

    async def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        self.state.ensure_job_initialized(job_type, template_path)

    async def load_project(self, project_json_path: Path):
        try:
            new_state = ProjectState.load(project_json_path)
            set_project_state(new_state)
            return True
        except Exception:
            return False

    async def save_project(self, save_path: Optional[Path] = None):
        if save_path:
            target_path = save_path
        elif self.state.project_path:
            target_path = self.state.project_path / "project_params.json"
        else:
            return
        self.state.save(target_path)

_state_service_instance = None

def get_state_service():
    global _state_service_instance
    if _state_service_instance is None:
        _state_service_instance = StateService()
    return _state_service_instance

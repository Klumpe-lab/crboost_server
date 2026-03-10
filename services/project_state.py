# services/project_state.py
from __future__ import annotations
import asyncio
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Type, List

from pydantic import BaseModel, Field, PrivateAttr, SerializeAsAny

from services.models_base import (
    JobStatus,
    MicroscopeType,
    AlignmentMethod,
    JobCategory,
    JobType,
    MicroscopeParams,
    AcquisitionParams,
)
from services.computing.slurm_service import SlurmConfig
from services.job_models import (
    AbstractJobParams,
    CandidateExtractPytomParams,
    Class3DParams,
    DenoisePredictParams,
    DenoiseTrainParams,
    FsMotionCtfParams,
    ImportMoviesParams,
    ReconstructParticleParams,
    SubtomoExtractionParams,
    TemplateMatchPytomParams,
    TemplateWorkbenchState,  
    TsAlignmentParams,
    TsCtfParams,
    TsReconstructParams,
    jobtype_paramclass,
)

def slugify(name: str) -> str:
    """Convert a display name to a filesystem-safe species id."""
    s = name.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s-]+", "_", s)
    return s.strip("_") or "species"


class ParticleSpecies(BaseModel):
    id: str                  # slug, used as folder name and instance suffix
    name: str                # display label
    color: str = "#3b82f6"
    template_path: str = ""
    mask_path: str = ""
    workbench: TemplateWorkbenchState = Field(default_factory=TemplateWorkbenchState)

class ProjectState(BaseModel):
    """Complete project state with direct global parameter access"""

    project_name: str = "Untitled"
    project_path: Optional[Path] = None
    created_at: datetime = Field(default_factory=datetime.now)
    modified_at: datetime = Field(default_factory=datetime.now)
    job_path_mapping: Dict[str, str] = Field(default_factory=dict)

    movies_glob: str = ""
    mdocs_glob: str = ""

    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams = Field(default_factory=AcquisitionParams)
    slurm_defaults: SlurmConfig = Field(default_factory=SlurmConfig.from_config_defaults)

    jobs: Dict[str, SerializeAsAny[AbstractJobParams]] = Field(default_factory=dict)
    species_registry: List[ParticleSpecies] = Field(default_factory=list)
    pipeline_active: bool = Field(default=False)

    _dirty: bool = PrivateAttr(default=False)


    def mark_dirty(self):
        self._dirty = True



    @property
    def is_dirty(self) -> bool:
        return self._dirty


    def save_if_dirty(self, path: Optional[Path] = None):
        if self.is_dirty:
            self.save(path)

    def get_species(self, species_id: str) -> Optional[ParticleSpecies]:
        return next((s for s in self.species_registry if s.id == species_id), None)

    def add_species(self, name: str, color: str = "#3b82f6") -> ParticleSpecies:
        """Create a new species entry from a display name. Caller is responsible
        for ensuring the name is not blank before calling."""
        sid = slugify(name)
        # Avoid id collisions by appending a counter if needed
        existing_ids = {s.id for s in self.species_registry}
        base = sid
        n = 2
        while sid in existing_ids:
            sid = f"{base}_{n}"
            n += 1
        species = ParticleSpecies(id=sid, name=name, color=color)
        self.species_registry.append(species)
        self.update_modified()
        return species

    def ensure_job_initialized(
        self,
        job_type: JobType,
        instance_id: Optional[str] = None,
        template_path: Optional[Path] = None,
    ):
        if instance_id is None:
            instance_id = job_type.value

        if instance_id in self.jobs:
            return

        from services.configs.config_service import get_config_service

        param_class_map = jobtype_paramclass()
        param_class = param_class_map.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_type}")

        job_params = param_class()
        job_params._project_state = self

        if hasattr(job_params, "rescale_angpixs") and self.microscope.pixel_size_angstrom > 0:
            binning = get_config_service().processing_defaults.reconstruction_binning
            computed = round(self.microscope.pixel_size_angstrom * binning, 2)
            job_params.rescale_angpixs = computed
            print(f"[STATE] Auto-set rescale_angpixs = {computed} ({self.microscope.pixel_size_angstrom} * {binning})")

        self.jobs[instance_id] = job_params
        self.update_modified()

    def update_modified(self):
        self.modified_at = datetime.now()

    def save(self, path: Optional[Path] = None):
        """Atomic file write via tempfile + rename."""
        save_path = path or (
            self.project_path / "project_params.json" if self.project_path else Path("project_params.json")
        )
        save_path.parent.mkdir(parents=True, exist_ok=True)

        data = self.model_dump(exclude={"project_path"})
        data["project_path"] = str(self.project_path) if self.project_path else None
        fd, tmp_path = tempfile.mkstemp(dir=str(save_path.parent), suffix=".tmp", prefix=".project_params_")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2, default=str)
            os.rename(tmp_path, str(save_path))
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        self._dirty = False

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
            slurm_defaults=(
                SlurmConfig(**data["slurm_defaults"])
                if "slurm_defaults" in data
                else SlurmConfig.from_config_defaults()
            ),
            pipeline_active=data.get("pipeline_active", False),
        )

        try:
            project_state.species_registry = [
                ParticleSpecies(**s) for s in data.get("species_registry", [])
            ]
        except Exception as e:
            print(f"[WARN] Could not load species registry: {e}")
            project_state.species_registry = []

        param_class_map = jobtype_paramclass()

        for instance_id, job_data in data.get("jobs", {}).items():
            try:
                job_type_value = job_data.get("job_type") or instance_id
                job_type = JobType(job_type_value)
                param_class = param_class_map.get(job_type)
                if param_class:
                    job_params = param_class(**job_data)
                    job_params._project_state = project_state
                    project_state.jobs[instance_id] = job_params
                else:
                    print(f"[WARN] No param class for job type '{job_type_value}' (instance '{instance_id}'), skipping")
            except (ValueError, Exception) as e:
                print(f"[WARN] Skipping job instance '{instance_id}' - failed to deserialize: {e}")

        return project_state




# =========================================================================
# Path-keyed ProjectState registry
#
# Replaces the old module-level _project_state singleton.
# Each project directory gets exactly one ProjectState instance.
# Two browser tabs on the same project share the same instance.
# Two tabs on different projects get different instances.
# Two server processes (different users/ports) have completely
# separate registries (separate Python processes, separate memory).
# =========================================================================

_project_states: Dict[Path, ProjectState] = {}


def get_project_state_for(project_path: Path) -> ProjectState:
    """Get or create ProjectState for a specific project directory.

    Backend/service code that has a project_path available should use
    this directly (via StateService.state_for(path)).
    """
    resolved = project_path.resolve()
    if resolved not in _project_states:
        params_file = resolved / "project_params.json"
        if params_file.exists():
            _project_states[resolved] = ProjectState.load(params_file)
        else:
            state = ProjectState()
            state.project_path = resolved
            _project_states[resolved] = state
    return _project_states[resolved]


def set_project_state_for(project_path: Path, state: ProjectState):
    """Insert or replace a ProjectState in the registry."""
    _project_states[project_path.resolve()] = state


def remove_project_state(project_path: Path):
    """Remove from registry (e.g. when closing a project)."""
    _project_states.pop(project_path.resolve(), None)


def get_project_state() -> ProjectState:
    """Convenience for UI code: resolves project_path from the current
    browser tab's UIStateManager.

    Falls back to a detached blank ProjectState if no project is loaded
    yet (landing page before create/load). This means all existing
    get_project_state() callsites in UI code work unchanged.
    """
    try:
        from ui.ui_state import get_ui_state_manager
        ui_mgr = get_ui_state_manager()
        if ui_mgr.project_path:
            return get_project_state_for(ui_mgr.project_path)
    except RuntimeError:
        # No client connection (background task, server startup, etc.)
        pass
    return ProjectState()


def set_project_state(new_state: ProjectState):
    """Legacy setter -- routes into the registry if the state has a project_path,
    otherwise falls back to replacing the tab-context entry."""
    if new_state.project_path:
        set_project_state_for(new_state.project_path, new_state)
    else:
        # Pre-creation state (landing page). Just park it in the registry
        # under a sentinel key; get_project_state() won't find it via
        # tab context anyway, and it'll be replaced once a real path exists.
        pass


def reset_project_state() -> ProjectState:
    """No-op when called from the landing page (the blank fallback in
    get_project_state() handles the 'no project' case).

    Kept as a function so existing imports don't break.
    """
    return ProjectState()


class StateService:
    """Manages persistence of ProjectState to disk.

    - UI code accesses .state (resolves via tab context)
    - Backend code with an explicit path uses .state_for(path)
    - save_project is serialized with an asyncio.Lock
    """

    def __init__(self):
        self._save_lock = asyncio.Lock()

    def state_for(self, project_path: Path) -> ProjectState:
        """Explicit accessor for backend/service code that has a path."""
        return get_project_state_for(project_path)

    @property
    def state(self) -> ProjectState:
        """Tab-context accessor. Backend code should prefer state_for(path)."""
        return get_project_state()

    async def update_from_mdoc(self, mdocs_glob: str, project_path: Optional[Path] = None):
        from services.configs.mdoc_service import get_mdoc_service

        mdoc_service = get_mdoc_service()
        print(f"[MDOC_UPDATE] Parsing mdocs from: {mdocs_glob}")
        mdoc_data = mdoc_service.get_autodetect_params(mdocs_glob)
        print(f"[MDOC_UPDATE] Result: {mdoc_data}")
        if not mdoc_data:
            return

        # CHANGED: explicit path when available (initialize_new_project
        # calls this before the UI tab has a project_path set)
        if project_path:
            s = self.state_for(project_path)
        else:
            s = self.state

        if "dose_per_tilt" in mdoc_data:
            s.acquisition.dose_per_tilt = mdoc_data["dose_per_tilt"]
            print(f"[MDOC_UPDATE] Set dose_per_tilt = {mdoc_data['dose_per_tilt']}")
        if "pixel_spacing" in mdoc_data:
            s.microscope.pixel_size_angstrom = mdoc_data["pixel_spacing"]
        if "voltage" in mdoc_data:
            s.microscope.acceleration_voltage_kv = mdoc_data["voltage"]
        if "tilt_axis_angle" in mdoc_data:
            s.acquisition.tilt_axis_degrees = mdoc_data["tilt_axis_angle"]
        s.update_modified()

    async def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        self.state.ensure_job_initialized(job_type, template_path)

    async def load_project(self, project_json_path: Path):
        try:
            new_state = ProjectState.load(project_json_path)
            project_path = new_state.project_path or project_json_path.parent
            new_state.project_path = project_path          # <-- fix
            set_project_state_for(project_path, new_state)
            return True
        except Exception:
            return False

    async def save_project(self, save_path: Optional[Path] = None, project_path: Optional[Path] = None, force: bool = False):
        """Save project state to disk.

        Args:
            save_path: Explicit file path to write to.
            project_path: Explicit project dir (for backend code without tab context).
            force: If True, write even if dirty flag is not set.
        """
        async with self._save_lock:
            if project_path:
                state = get_project_state_for(project_path)
            else:
                state = get_project_state()

            if save_path:
                target_path = save_path
            elif state.project_path:
                target_path = state.project_path / "project_params.json"
            else:
                return

            if force:
                state.save(target_path)
            else:
                state.save_if_dirty(target_path)


_state_service_instance = None


def get_state_service():
    global _state_service_instance
    if _state_service_instance is None:
        _state_service_instance = StateService()
    return _state_service_instance

## Final Architecture Proposal

### 1. Simplified Project State (`services/project_state.py`)

```python
# services/project_state.py
"""
Unified project state - single source of truth for all parameters.
No more job_params.json files, no sync tracking, no user modification nonsense.
"""

from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime
import json

from services.parameter_models import (
    MicroscopeParams, AcquisitionParams, ComputingParams,
    JobType, ImportMoviesParams, FsMotionCtfParams, 
    TsAlignmentParams, TsCtfParams, TsReconstructParams
)

class ProjectState(BaseModel):
    """Complete project state with direct global parameter access"""
    
    # Metadata
    project_name: str            = "Untitled"
    project_path: Optional[Path] = None
    created_at  : datetime       = Field(default_factory=datetime.now)
    modified_at : datetime       = Field(default_factory=datetime.now)
    
    # Global experimental parameters - THE single source of truth
    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams = Field(default_factory=AcquisitionParams)
    computing: ComputingParams = Field(default_factory=ComputingParams)
    
    # Job configurations - these access global params directly via properties
    jobs: Dict[JobType, Any] = Field(default_factory=dict)
    
    def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        """Ensure job exists and can access global parameters"""
        if job_type in self.jobs:
            return
            
        param_classes = {
            JobType.IMPORT_MOVIES: ImportMoviesParams,
            JobType.FS_MOTION_CTF: FsMotionCtfParams,
            JobType.TS_ALIGNMENT: TsAlignmentParams,
            JobType.TS_CTF: TsCtfParams,
            JobType.TS_RECONSTRUCT: TsReconstructParams,
        }
        
        param_class = param_classes.get(job_type)
        if not param_class:
            raise ValueError(f"Unknown job type: {job_type}")
        
        # Try template first
        if template_path and template_path.exists():
            job_params = param_class.from_job_star(template_path)
        else:
            job_params = param_class()
        
        # Attach project state for global parameter access
        if job_params:
            job_params._project_state = self
            self.jobs[job_type] = job_params
    
    # Serialization
    def save(self, path: Optional[Path] = None):
        save_path = path or self.project_path / "project_params.json"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            "project_name": self.project_name,
            "project_path": str(self.project_path) if self.project_path else None,
            "created_at": self.created_at.isoformat(),
            "modified_at": self.modified_at.isoformat(),
            "microscope": self.microscope.model_dump(),
            "acquisition": self.acquisition.model_dump(), 
            "computing": self.computing.model_dump(),
            "jobs": {
                job_type.value: job_params.model_dump()
                for job_type, job_params in self.jobs.items()
            }
        }
        
        with open(save_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    @classmethod
    def load(cls, path: Path):
        with open(path, 'r') as f:
            data = json.load(f)
        
        # Recreate project state
        project_state = cls(
            project_name=data.get("project_name", "Untitled"),
            project_path=Path(data["project_path"]) if data.get("project_path") else None,
            microscope=MicroscopeParams(**data.get("microscope", {})),
            acquisition=AcquisitionParams(**data.get("acquisition", {})),
            computing=ComputingParams(**data.get("computing", {})),
        )
        
        # Reattach project state to jobs
        job_type_mapping = {
            JobType.IMPORT_MOVIES: ImportMoviesParams,
            JobType.FS_MOTION_CTF: FsMotionCtfParams,
            JobType.TS_ALIGNMENT: TsAlignmentParams,
            JobType.TS_CTF: TsCtfParams,
            JobType.TS_RECONSTRUCT: TsReconstructParams,
        }
        
        for job_type_str, job_data in data.get("jobs", {}).items():
            job_type = JobType(job_type_str)
            param_class = job_type_mapping.get(job_type)
            if param_class:
                job_params = param_class(**job_data)
                job_params._project_state = project_state
                project_state.jobs[job_type] = job_params
        
        return project_state

# Global instance
_project_state = None

def get_project_state():
    global _project_state
    if _project_state is None:
        _project_state = ProjectState()
    return _project_state
```

### 2. Simplified Parameter Models (`services/parameter_models.py`)

```python
# services/parameter_models.py
"""
Clean parameter models where jobs directly access global experimental parameters.
No more sync methods, no duplicate fields.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import ClassVar, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from services.project_state import ProjectState

class AbstractJobParams(BaseModel):
    """Base class with direct access to global experimental parameters"""
    
    model_config = ConfigDict(validate_assignment=True)
    
    # Job execution metadata only
    execution_status: JobStatus = Field(default=JobStatus.SCHEDULED)
    relion_job_name: Optional[str] = None
    relion_job_number: Optional[int] = None
    
    # Private reference to project state for global parameter access
    _project_state: Optional[ProjectState] = Field(default=None, exclude=True)
    
    # Global parameter access via properties - THE KEY INNOVATION
    @property
    def microscope(self):
        if self._project_state is None:
            raise RuntimeError("Job not attached to project state")
        return self._project_state.microscope
    
    @property
    def acquisition(self):
        if self._project_state is None:
            raise RuntimeError("Job not attached to project state")  
        return self._project_state.acquisition
    
    @property
    def pixel_size(self) -> float:
        return self.microscope.pixel_size_angstrom
    
    @property
    def voltage(self) -> float:
        return self.microscope.acceleration_voltage_kv
    
    @property
    def dose_per_tilt(self) -> float:
        return self.acquisition.dose_per_tilt
    
    @property
    def tilt_axis_angle(self) -> float:
        return self.acquisition.tilt_axis_degrees
    
    # Remove all sync_from_pipeline_state methods!
    # Remove all duplicate pixel_size, voltage, etc. fields!

class ImportMoviesParams(AbstractJobParams):
    """Import movies - only job-specific parameters"""
    
    # JOB-SPECIFIC PARAMETERS ONLY
    optics_group_name: str = "opticsGroup1"
    do_at_most: int = Field(default=-1)
    
    # NO pixel_size, voltage, etc. - accessed via properties above
    
    def get_tool_name(self) -> str:
        return "relion_import"
    
    @classmethod
    def from_job_star(cls, star_path: Path):
        """Load only job-specific parameters from template"""
        if not star_path.exists():
            return None
        
        try:
            data = starfile.read(star_path, always_dict=True)
            job_data = data.get("job")
            if not job_data:
                return None
            
            if isinstance(job_data, pd.DataFrame):
                job_params = job_data.to_dict("records")[0]
            else:
                job_params = job_data
            
            return cls(
                optics_group_name=job_params.get("optics_group_name", "opticsGroup1"),
                do_at_most=int(job_params.get("do_at_most", -1)),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star: {e}")
            return None

class FsMotionCtfParams(AbstractJobParams):
    """Motion correction and CTF - only job-specific parameters"""
    
    # JOB-SPECIFIC PARAMETERS ONLY
    eer_ngroups: int = Field(default=32, ge=1)
    m_range_min_max: str = "500:10"
    m_bfac: int = Field(default=-500)
    m_grid: str = "1x1x3"
    c_range_min_max: str = "30:6.0"
    c_defocus_min_max: str = "1.1:8"
    c_grid: str = "2x2x1"
    c_window: int = Field(default=512, ge=128)
    c_use_sum: bool = False
    perdevice: int = Field(default=1, ge=0, le=8)
    do_at_most: int = Field(default=-1)
    
    # NO pixel_size, voltage, cs, amplitude - accessed via properties
    
    def is_driver_job(self) -> bool:
        return True
    
    def get_tool_name(self) -> str:
        return "warptools"
    
    @classmethod
    def from_job_star(cls, star_path: Path):
        """Load only job-specific parameters"""
        if not star_path.exists():
            return None
        
        try:
            data = starfile.read(star_path, always_dict=True)
            joboptions = data.get("joboptions_values")
            if not isinstance(joboptions, pd.DataFrame):
                return None
            
            param_dict = pd.Series(
                joboptions["rlnJobOptionValue"].values,
                index=joboptions["rlnJobOptionVariable"].values
            ).to_dict()
            
            return cls(
                eer_ngroups=int(param_dict.get("param1_value", "32")),
                m_range_min_max=param_dict.get("param4_value", "500:10"),
                m_bfac=int(param_dict.get("param5_value", "-500")),
                m_grid=param_dict.get("param6_value", "1x1x3"),
                c_range_min_max=param_dict.get("param7_value", "30:6.0"),
                c_defocus_min_max=param_dict.get("param8_value", "1.1:8"),
                c_grid=param_dict.get("param9_value", "2x2x1"),
                perdevice=int(param_dict.get("param10_value", "1")),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star: {e}")
            return None
```

### 3. Ultra-Simple State Service (`services/state_service.py`)

```python
# services/state_service.py
"""
Minimal state service - just handles loading/saving.
No sync tracking, no user modification tracking, no source tracking.
"""

from services.project_state import ProjectState, get_project_state, set_project_state
from services.parameter_models import JobType
from services.mdoc_service import get_mdoc_service

class StateService:
    """Just handles persistence and mdoc updates"""
    
    def __init__(self):
        self._project_state = get_project_state()
    
    @property
    def state(self) -> ProjectState:
        return self._project_state
    
    async def update_from_mdoc(self, mdocs_glob: str):
        """Update global state from mdoc files"""
        mdoc_service = get_mdoc_service()
        mdoc_data = mdoc_service.get_autodetect_params(mdocs_glob)
        
        if not mdoc_data:
            return
        
        # Update global parameters directly
        if "pixel_spacing" in mdoc_data:
            self._project_state.microscope.pixel_size_angstrom = mdoc_data["pixel_spacing"]
        if "voltage" in mdoc_data:
            self._project_state.microscope.acceleration_voltage_kv = mdoc_data["voltage"]
        if "dose_per_tilt" in mdoc_data:
            self._project_state.acquisition.dose_per_tilt = mdoc_data["dose_per_tilt"]
        # ... etc
    
    async def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        """Ensure job exists"""
        self._project_state.ensure_job_initialized(job_type, template_path)
    
    async def load_project(self, project_path: Path):
        """Load project"""
        self._project_state = ProjectState.load(project_path)
        set_project_state(self._project_state)
    
    async def save_project(self, save_path: Optional[Path] = None):
        """Save project"""
        self._project_state.save(save_path)

# Singleton
_state_service_instance = None

def get_state_service():
    global _state_service_instance
    if _state_service_instance is None:
        _state_service_instance = StateService()
    return _state_service_instance
```

### 4. Direct UI Binding Examples

#### Obvious Example: Global Parameters
```python
# ui/data_import_panel.py - BEFORE (complex)
def build_data_import_panel(backend, shared_state, callbacks):
    # Complex state-bound inputs
    pixel_size_input = create_global_input("Pixel Size", "microscope.pixel_size_angstrom")
    voltage_input = create_global_input("Voltage", "microscope.acceleration_voltage_kv")
    
    # Manual sync tracking
    sync_indicator = create_sync_indicator(job_type)

# ui/data_import_panel.py - AFTER (simple)
def build_data_import_panel(backend, shared_state, callbacks):
    state_service = get_state_service()
    
    with ui.column().classes("w-full"):
        # Direct NiceGUI binding - no wrapper needed
        ui.input('Pixel Size (Å)').bind_value(
            state_service.state.microscope, 
            'pixel_size_angstrom'
        ).props("dense outlined")
        
        ui.input('Voltage (kV)').bind_value(
            state_service.state.microscope,
            'acceleration_voltage_kv'
        ).props("dense outlined")
        
        ui.input('Dose per Tilt').bind_value(
            state_service.state.acquisition, 
            'dose_per_tilt'
        ).props("dense outlined")
```

#### Less Obvious Example: Job Parameters Accessing Global State
```python
# ui/pipeline_builder/job_tab_component.py - BEFORE (with sync complexity)
def _render_config_tab(job_type, job_model, is_frozen, shared_state, callbacks):
    # Job-specific parameters
    ui.input('Optics Group').bind_value(job_model, 'optics_group_name')
    
    # Global parameters duplicated in job model (WITH SYNC ISSUES)
    ui.input('Pixel Size').bind_value(job_model, 'pixel_size')
    ui.input('Voltage').bind_value(job_model, 'voltage')
    
    # Sync indicator needed
    with ui.row():
        create_sync_indicator(job_type)
        ui.button("Reset to Global", on_click=reset_job)

# ui/pipeline_builder/job_tab_component.py - AFTER (simple and correct)
def _render_config_tab(job_type, job_model, is_frozen, shared_state, callbacks):
    # Job-specific parameters
    ui.input('Optics Group').bind_value(job_model, 'optics_group_name')
    
    # Global parameters accessed via job properties (ALWAYS IN SYNC)
    ui.input('Pixel Size').bind_value(
        job_model.microscope,  # Direct access to global state!
        'pixel_size_angstrom'
    ).props("dense outlined").tooltip("Global experimental parameter")
    
    ui.input('Voltage').bind_value(
        job_model.microscope,  # Direct access to global state!
        'acceleration_voltage_kv'  
    ).props("dense outlined").tooltip("Global experimental parameter")
    
    # NO sync indicator needed - it's always in sync by design!
```

#### Powerful Example: Driver Code
```python
# drivers/ts_alignment.py - BEFORE (needs param generator)
def main():
    # Complex bootstrap to get job_params.json
    params_data, job_dir, project_path, job_type = get_driver_context()
    params = TsAlignmentParams(**params_data['job_model'])
    
    # Use parameters
    pixel_size = params.pixel_size  # Could be out of sync with global state
    thickness = params.thickness_nm

# drivers/ts_alignment.py - AFTER (direct access)
def main():
    # Simple project state loading
    project_path = get_project_path_from_args() 
    project_state = ProjectState.load(project_path / "project_params.json")
    
    # Get job model with direct global access
    job_model = project_state.jobs[JobType.TS_ALIGNMENT]
    
    # Parameters are always correct - no sync issues!
    pixel_size = job_model.pixel_size  # From global state via property
    thickness = job_model.thickness_nm  # From global state via property
    
    # Job-specific parameters work the same
    alignment_method = job_model.alignment_method
```

## Actionable Changes Needed

### 1. **ELIMINATE** These Files/Concepts:
- ❌ `ui/state_binding.py` - Entire file
- ❌ `StateBoundInput` class - All usage
- ❌ `create_global_input`/`create_job_input` functions  
- ❌ All "user_modified_jobs" tracking
- ❌ All "sync_from_pipeline_state" methods
- ❌ All per-job `job_params.json` files

### 2. **SIMPLIFY** Parameter Models:
- Remove duplicate fields (`pixel_size`, `voltage`, etc.) from job models
- Remove all `sync_from_pipeline_state` methods
- Add `_project_state` reference and property accessors
- Keep only job-specific parameters in each model

### 3. **CONSOLIDATE** State Management:
- Replace `app_state.py` with `project_state.py`
- Use single `project_params.json` instead of scattered job files
- Remove all sync tracking logic from state service

### 4. **USE DIRECT BINDING** in UI:
- Replace all `StateBoundInput` with direct `ui.input().bind_value()`
- Bind job global parameters to `job_model.microscope.field_name`
- Remove all sync indicators from UI

### 5. **UPDATE** Drivers and Services:
- Update param generator to use project state
- Update pipeline orchestrator to use project state
- Simplify driver bootstrap logic

## Why This Architecture Wins

1. **No Sync Bugs**: Global parameters are truly global - no duplication
2. **Dramatically Less Code**: Remove hundreds of lines of sync logic
3. **True Single Source**: One `project_params.json` for everything
4. **Natural UI Binding**: Direct NiceGUI binding just works
5. **Type Safety**: IDE can follow property chains
6. **Maintainable**: Clear separation of global vs job-specific parameters
7. **Performance**: No complex listener networks, no sync calculations

The key insight: **Jobs don't "have" experimental parameters - they access the single source of truth.** This eliminates the entire category of sync-related complexity that was plaguing your system.
# services/parameter_models.py
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, Dict, Tuple, Self, Union, Any, TYPE_CHECKING
from enum import Enum
from pathlib import Path
import pandas as pd
import starfile
from datetime import datetime

if TYPE_CHECKING:
    from services.parameter_models import PipelineState

# ============= BASE & PROTOCOL =============

class JobParamsProtocol:
    """
    Protocol defining the interface all job parameters must implement.
    This is documentation + type hints, not enforced inheritance.
    """
    
    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        """Load default values from job.star template"""
        ...
    
    @classmethod
    def from_pipeline_state(cls, state: 'PipelineState') -> Self:
        """Create fresh instance using current global state values"""
        ...
    
    def sync_from_pipeline_state(self, state: 'PipelineState') -> Self:
        """
        Update microscope/acquisition fields from global state IN-PLACE.
        This is called when global params change and user wants to sync.
        IMPORTANT: Modifies self and returns self (for chaining).
        """
        ...


# ============= ENUMS =============

class Partition(str, Enum):
    CPU = "c"
    GPU = "g"
    GPU_V100 = "g-v100"
    GPU_A100 = "g-a100"
    MEMORY = "m"


class MicroscopeType(str, Enum):
    KRIOS_G3 = "Krios_G3"
    KRIOS_G4 = "Krios_G4"
    GLACIOS = "Glacios"
    TALOS = "Talos"
    CUSTOM = "Custom"


class AlignmentMethod(str, Enum):
    ARETOMO = "AreTomo"
    IMOD = "IMOD"
    RELION = "Relion"


# ============= CORE PARAMETER GROUPS =============

class MicroscopeParams(BaseModel):
    """Microscope-specific parameters"""
    model_config = ConfigDict(validate_assignment=True)  # Enable validation on assignment
    
    microscope_type: MicroscopeType = MicroscopeType.CUSTOM
    pixel_size_angstrom: float = Field(default=1.35, ge=0.5, le=10.0)
    acceleration_voltage_kv: float = Field(default=300.0)
    spherical_aberration_mm: float = Field(default=2.7, ge=0.0, le=10.0)
    amplitude_contrast: float = Field(default=0.1, ge=0.0, le=1.0)

    @field_validator("acceleration_voltage_kv")
    @classmethod
    def validate_voltage(cls, v: float) -> float:
        allowed = [200.0, 300.0]
        if v not in allowed:
            print(f"[WARN] Voltage {v} not in standard values {allowed}")
        return v


class AcquisitionParams(BaseModel):
    """Data acquisition parameters"""
    model_config = ConfigDict(validate_assignment=True)
    
    dose_per_tilt: float = Field(default=3.0, ge=0.1, le=9.0)
    detector_dimensions: Tuple[int, int] = (4096, 4096)
    tilt_axis_degrees: float = Field(default=-95.0, ge=-180.0, le=180.0)
    eer_fractions_per_frame: Optional[int] = Field(default=None, ge=1, le=100)
    sample_thickness_nm: float = Field(default=300.0, ge=50.0, le=2000.0)
    gain_reference_path: Optional[str] = None
    invert_tilt_angles: bool = False
    invert_defocus_hand: bool = False


class ComputingParams(BaseModel):
    """Computing resource parameters"""
    model_config = ConfigDict(validate_assignment=True)
    
    partition: Partition = Partition.GPU
    gpu_count: int = Field(default=1, ge=0, le=8)
    memory_gb: int = Field(default=32, ge=4, le=512)
    threads: int = Field(default=8, ge=1, le=128)

    @classmethod
    def from_conf_yaml(cls, config_path: Path) -> "ComputingParams":
        """Extract computing params from conf.yaml"""
        from services.config_service import get_config_service

        try:
            config_service = get_config_service(str(config_path))
            gpu_partition = config_service.find_gpu_partition()

            if gpu_partition:
                partition_key, partition = gpu_partition
                memory_gb = int(partition.RAM.replace("G", "").replace("g", ""))

                return cls(
                    partition=Partition(partition_key),
                    gpu_count=partition.NrGPU,
                    memory_gb=memory_gb,
                    threads=partition.NrCPU,
                )

            return cls()

        except Exception as e:
            print(f"[ERROR] Failed to parse computing config: {e}")
            return cls()
    
    def get_qsub_replacements(self) -> Dict[str, str]:
        """
        Generate qsub template replacements from computing params.
        This replaces hardcoded values in project_service.
        """
        return {
            "XXXextra1XXX": "1",  # nodes (could make configurable later)
            "XXXextra2XXX": "",   # mpi_per_node (empty = let relion handle)
            "XXXextra3XXX": self.partition.value,
            "XXXextra4XXX": str(self.gpu_count),
            "XXXextra5XXX": f"{self.memory_gb}G",
            "XXXthreadsXXX": str(self.threads),
        }


# ============= JOB-SPECIFIC PARAMETER MODELS =============

class ImportMoviesParams(BaseModel):
    """Parameters for import movies job - implements JobParamsProtocol"""
    model_config = ConfigDict(validate_assignment=True)
    
    # From microscope
    pixel_size: float = Field(ge=0.5, le=10.0)
    voltage: float = Field(ge=50.0)
    spherical_aberration: float = Field(ge=0.0)
    amplitude_contrast: float = Field(ge=0.0, le=1.0)

    # From acquisition
    dose_per_tilt_image: float = Field(ge=0.1)
    tilt_axis_angle: float = Field(ge=-180.0, le=180.0)

    # Job-specific
    optics_group_name: str = "opticsGroup1"
    do_at_most: int = Field(default=-1)
    invert_defocus_hand: bool = False

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        """Load defaults from job.star template"""
        if not star_path or not star_path.exists():
            return None

        try:
            data: Dict[str, Union[pd.DataFrame, Dict[str, Any]]] = starfile.read(
                star_path, always_dict=True
            )
            
            job_data = data.get('job')
            if job_data is None:
                return None
                
            # Convert DataFrame to dict if needed
            if isinstance(job_data, pd.DataFrame):
                if len(job_data) == 0:
                    return None
                job_params: Dict[str, Any] = job_data.to_dict('records')[0]
            else:
                job_params: Dict[str, Any] = job_data

            return cls(
                pixel_size=float(job_params.get("nominal_pixel_size", 1.35)),
                voltage=float(job_params.get("voltage", 300)),
                spherical_aberration=float(job_params.get("spherical_aberration", 2.7)),
                amplitude_contrast=float(job_params.get("amplitude_contrast", 0.1)),
                dose_per_tilt_image=float(job_params.get("dose_per_tilt_image", 3.0)),
                tilt_axis_angle=float(job_params.get("nominal_tilt_axis_angle", -95.0)),
                optics_group_name=job_params.get("optics_group_name", "opticsGroup1"),
                invert_defocus_hand=bool(job_params.get("invert_defocus_hand", False)),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None
    
    @classmethod
    def from_pipeline_state(cls, state: 'PipelineState') -> Self:
        """Create from global pipeline state"""
        return cls(
            pixel_size=state.microscope.pixel_size_angstrom,
            voltage=state.microscope.acceleration_voltage_kv,
            spherical_aberration=state.microscope.spherical_aberration_mm,
            amplitude_contrast=state.microscope.amplitude_contrast,
            dose_per_tilt_image=state.acquisition.dose_per_tilt,
            tilt_axis_angle=state.acquisition.tilt_axis_degrees,
            invert_defocus_hand=state.acquisition.invert_defocus_hand,
        )
    
    def sync_from_pipeline_state(self, state: 'PipelineState') -> Self:
        """Update microscope/acquisition params from global state IN-PLACE"""
        self.pixel_size = state.microscope.pixel_size_angstrom
        self.voltage = state.microscope.acceleration_voltage_kv
        self.spherical_aberration = state.microscope.spherical_aberration_mm
        self.amplitude_contrast = state.microscope.amplitude_contrast
        self.dose_per_tilt_image = state.acquisition.dose_per_tilt
        self.tilt_axis_angle = state.acquisition.tilt_axis_degrees
        self.invert_defocus_hand = state.acquisition.invert_defocus_hand
        return self


class FsMotionCtfParams(BaseModel):
    """Parameters for WarpTools motion correction and CTF"""
    model_config = ConfigDict(validate_assignment=True)
    
    # From microscope (synced from global)
    pixel_size: float = Field(ge=0.5, le=10.0)
    voltage: float = Field(ge=50.0)
    cs: float = Field(ge=0.0)
    amplitude: float = Field(ge=0.0, le=1.0)

    # EER specific (from acquisition)
    eer_ngroups: int = Field(default=32, ge=1)

    # Motion correction parameters (from job.star)
    m_range_min_max: str = "500:10"
    m_bfac: int = Field(default=-500)
    m_grid: str = "1x1x3"

    # CTF parameters
    c_range_min_max: str = "30:6.0"
    c_defocus_min_max: str = "1.1:8"  # microns
    c_grid: str = "2x2x1"
    c_window: int = Field(default=512, ge=128)

    # Processing control
    perdevice: int = Field(default=1, ge=0, le=8)
    do_at_most: int = Field(default=-1)

    # Optional gain reference
    gain_path: Optional[str] = None
    gain_operations: Optional[str] = None

    # Helper properties
    @property
    def m_range_min(self) -> int:
        return int(self.m_range_min_max.split(":")[0])

    @property
    def m_range_max(self) -> int:
        return int(self.m_range_min_max.split(":")[1])

    @property
    def c_range_min(self) -> float:
        return float(self.c_range_min_max.split(":")[0])

    @property
    def c_range_max(self) -> float:
        return float(self.c_range_min_max.split(":")[1])

    @property
    def defocus_min_microns(self) -> float:
        return float(self.c_defocus_min_max.split(":")[0])

    @property
    def defocus_max_microns(self) -> float:
        return float(self.c_defocus_min_max.split(":")[1])

    @property
    def defocus_min_angstroms(self) -> float:
        return self.defocus_min_microns * 10000.0

    @property
    def defocus_max_angstroms(self) -> float:
        return self.defocus_max_microns * 10000.0

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        """Load from job.star template"""
        if not star_path or not star_path.exists():
            return None

        try:
            data: Dict[str, Union[pd.DataFrame, dict]] = starfile.read(
                star_path, always_dict=True
            )
            
            joboptions = data.get('joboptions_values')
            if joboptions is None or not isinstance(joboptions, pd.DataFrame):
                return None
            
            df: pd.DataFrame = joboptions
            
            # Create parameter dictionary - safely access DataFrame columns
            param_dict: Dict[str, str] = pd.Series(
                df['rlnJobOptionValue'].values,
                index=df['rlnJobOptionVariable'].values
            ).to_dict()

            return cls(
                pixel_size=1.35,  # Will be synced from pipeline state
                voltage=300.0,
                cs=2.7,
                amplitude=0.1,
                eer_ngroups=int(param_dict.get("param1_value", "32")),
                gain_path=param_dict.get("param2_value"),
                gain_operations=param_dict.get("param3_value"),
                m_range_min_max=param_dict.get("param4_value", "500:10"),
                m_bfac=int(param_dict.get("param5_value", "-500")),
                m_grid=param_dict.get("param6_value", "1x1x3"),
                c_range_min_max=param_dict.get("param7_value", "30:6.0"),
                c_defocus_min_max=param_dict.get("param8_value", "1.1:8"),
                c_grid=param_dict.get("param9_value", "2x2x1"),
                perdevice=int(param_dict.get("param10_value", "1")),
                c_window=512,
            )

        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None
    
    @classmethod
    def from_pipeline_state(cls, state: 'PipelineState') -> Self:
        """Create from global pipeline state"""
        return cls(
            pixel_size=state.microscope.pixel_size_angstrom,
            voltage=state.microscope.acceleration_voltage_kv,
            cs=state.microscope.spherical_aberration_mm,
            amplitude=state.microscope.amplitude_contrast,
            eer_ngroups=state.acquisition.eer_fractions_per_frame or 32,
        )
    
    def sync_from_pipeline_state(self, state: 'PipelineState') -> Self:
        """Update microscope/acquisition params from global state IN-PLACE"""
        self.pixel_size = state.microscope.pixel_size_angstrom
        self.voltage = state.microscope.acceleration_voltage_kv
        self.cs = state.microscope.spherical_aberration_mm
        self.amplitude = state.microscope.amplitude_contrast
        self.eer_ngroups = state.acquisition.eer_fractions_per_frame or 32
        return self


class TsAlignmentParams(BaseModel):
    """Parameters for tilt series alignment"""
    model_config = ConfigDict(validate_assignment=True)
    
    alignment_method: AlignmentMethod = AlignmentMethod.ARETOMO
    binning: int = Field(default=4, ge=1, le=16)
    thickness_nm: float = Field(default=300.0, ge=50.0, le=2000.0)
    do_at_most: int = Field(default=-1)

    # AreTomo specific
    tilt_cor: int = Field(default=1)
    out_imod: int = Field(default=0)
    patch_x: int = Field(default=5, ge=1)
    patch_y: int = Field(default=5, ge=1)

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        """Load from job.star template"""
        if not star_path or not star_path.exists():
            return None

        try:
            data: Dict[str, Union[pd.DataFrame, dict]] = starfile.read(
                star_path, always_dict=True
            )
            
            job_data = data.get('job')
            if job_data is None:
                return None
                
            if isinstance(job_data, pd.DataFrame):
                if len(job_data) == 0:
                    return None
                job_params: Dict[str, Any] = job_data.to_dict('records')[0]
            else:
                job_params: Dict[str, Any] = job_data

            method_str = job_params.get("alignment_method", "AreTomo")
            try:
                method = AlignmentMethod(method_str)
            except ValueError:
                method = AlignmentMethod.ARETOMO

            return cls(
                alignment_method=method,
                binning=int(job_params.get("binning", 4)),
                thickness_nm=float(job_params.get("thickness", 300.0)),
                tilt_cor=int(job_params.get("tilt_cor", 1)),
                out_imod=int(job_params.get("out_imod", 0)),
                patch_x=int(job_params.get("patch_x", 5)),
                patch_y=int(job_params.get("patch_y", 5)),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None
    
    @classmethod
    def from_pipeline_state(cls, state: 'PipelineState') -> Self:
        """Create from global pipeline state"""
        return cls(
            thickness_nm=state.acquisition.sample_thickness_nm
        )
    
    def sync_from_pipeline_state(self, state: 'PipelineState') -> Self:
        """Update acquisition params from global state IN-PLACE"""
        self.thickness_nm = state.acquisition.sample_thickness_nm
        return self


# ============= MAIN PIPELINE STATE =============

class PipelineState(BaseModel):
    """Central state with hierarchical organization"""
    model_config = ConfigDict(validate_assignment=True)
    
    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams = Field(default_factory=AcquisitionParams)
    computing: ComputingParams = Field(default_factory=ComputingParams)
    jobs: Dict[str, BaseModel] = Field(default_factory=dict)

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    modified_at: datetime = Field(default_factory=datetime.now)

    def populate_job(self, job_type: 'JobType', job_star_path: Optional[Path] = None):
        """
        Generic job population using the param class's factory methods.
        This replaces the giant if/elif chain.
        """
        from services.job_types import get_job_param_classes
        
        param_classes = get_job_param_classes()
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

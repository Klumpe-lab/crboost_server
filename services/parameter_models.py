# services/parameter_models.py
from __future__ import annotations
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import ClassVar, Optional, Dict, Tuple, Self, Union, Any, TYPE_CHECKING, Type
from enum import Enum
from pathlib import Path
import pandas as pd
import starfile
from datetime import datetime
import sys

if TYPE_CHECKING:
    from app_state import PipelineState

class JobStatus(str, Enum):
    SUCCEEDED = "Succeeded"
    FAILED    = "Failed"
    RUNNING   = "Running"
    SCHEDULED = "Scheduled"  # We'll use this for jobs in scheme but not pipeline
    UNKNOWN   = "Unknown"

class Partition(str, Enum):
    CPU = "c"
    GPU = "g"

class MicroscopeType(str, Enum):
    KRIOS_G3 = "Krios_G3"
    KRIOS_G4 = "Krios_G4"
    GLACIOS  = "Glacios"
    TALOS    = "Talos"
    CUSTOM   = "Custom"

class AlignmentMethod(str, Enum):
    ARETOMO = "AreTomo"
    IMOD    = "IMOD"
    RELION  = "Relion"

class MicroscopeParams(BaseModel):
    """Microscope-specific parameters"""

    model_config = ConfigDict(validate_assignment=True)

    microscope_type         : MicroscopeType = MicroscopeType.CUSTOM
    pixel_size_angstrom     : float          = Field(default=1.35, ge=0.5, le=10.0)
    acceleration_voltage_kv: float           = Field(default=300.0)
    spherical_aberration_mm: float           = Field(default=2.7, ge=0.0, le=10.0)
    amplitude_contrast      : float          = Field(default=0.1, ge=0.0, le=1.0)

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

    dose_per_tilt          : float           = Field(default=3.0, ge=0.1, le=9.0)
    detector_dimensions    : Tuple[int, int] = (4096, 4096)
    tilt_axis_degrees      : float           = Field(default=-95.0, ge=-180.0, le=180.0)
    eer_fractions_per_frame: Optional[int]   = Field(default=None, ge=1, le=100)
    sample_thickness_nm    : float           = Field(default=300.0, ge=50.0, le=2000.0)
    gain_reference_path    : Optional[str]   = None
    invert_tilt_angles     : bool            = False
    invert_defocus_hand    : bool            = False
    
    acquisition_software   : str             = Field(default="SerialEM") 
    nominal_magnification  : Optional[int]   = None
    spot_size              : Optional[int]   = None
    camera_name            : Optional[str]   = None
    binning                : Optional[int]   = Field(default=1, ge=1)
    frame_dose             : Optional[float] = None  

class ComputingParams(BaseModel):
    """Computing resource parameters"""
    model_config = ConfigDict(validate_assignment=True)

    partition: Partition = Partition.GPU
    gpu_count: int       = Field(default=1, ge=0, le=8)
    memory_gb: int       = Field(default=32, ge=4, le=512)
    threads  : int       = Field(default=8, ge=1, le=128)

    @classmethod
    def from_conf_yaml(cls, config_path: Path) -> Self:
        """Extract computing params from conf.yaml"""
        from services.config_service import get_config_service

        try:
            config_service = get_config_service()
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
            print(f"[ERROR] Failed to parse computing config: {e}", file=sys.stderr)
            return cls()

class JobCategory(str, Enum):
    """Where different job types live in the project"""

    IMPORT     = "Import"
    EXTERNAL   = "External"
    MOTIONCORR = "MotionCorr"
    CTFFIND    = "CtfFind"

class JobType(str, Enum):
    """Enumeration of all pipeline job types"""

    IMPORT_MOVIES       = "importmovies"
    FS_MOTION_CTF       = "fsMotionAndCtf"
    TS_ALIGNMENT        = "aligntiltsWarp"
    TS_CTF              = "tsCtf"
    TS_RECONSTRUCT      = "tsReconstruct"
    DENOISE_TRAIN       = "denoiseTrain"
    TEMPLATE_MATCH      = "templateMatching"
    SUBTOMO_RECONSTRUCT = "sta"

    @classmethod
    def from_string(cls, value: str) -> Self:
        try:
            return cls(value)
        except ValueError:
            valid = [e.value for e in cls]
            raise ValueError(f"Unknown job type '{value}'. Valid types: {valid}")

    @property
    def display_name(self) -> str:
        """Human-readable name"""
        return self.value.replace("_", " ").title()

class AbstractJobParams(BaseModel):
    """Abstract base class for all job parameter models."""

    model_config = ConfigDict(validate_assignment=True)
    JOB_CATEGORY: ClassVar[JobCategory]

    execution_status : JobStatus     = Field(default=JobStatus.SCHEDULED)
    relion_job_name  : Optional[str] = None                                
    relion_job_number: Optional[int] = None

    def __setattr__(self, name: str, value: Any) -> None:
        """Enforce immutability for started/completed jobs"""
        # Always allow these metadata fields to update
        if name in ['execution_status', 'relion_job_name', 'relion_job_number']:
            super().__setattr__(name, value)
            return
        
        # Allow internal Pydantic machinery (fields that start with underscore)
        if name.startswith('_'):
            super().__setattr__(name, value)
            return
        
        # Check if we're in initialization (object doesn't have status yet)
        try:
            current_status = object.__getattribute__(self, 'execution_status')
        except AttributeError:
            # We're in __init__, allow everything
            super().__setattr__(name, value)
            return
        
        # Only allow param changes if job hasn't started
        if current_status != JobStatus.SCHEDULED:
            print(f"[IMMUTABLE] Blocked change to '{name}' on {current_status.value} job")
            return
        
        # Job is SCHEDULED, allow the change
        super().__setattr__(name, value)

    @property
    def display_status(self) -> str:
        """Human-readable status"""
        return self.execution_status.value

    @property 
    def has_succeeded(self) -> bool:
        return self.execution_status == JobStatus.SUCCEEDED

    @property
    def is_running(self) -> bool:
        return self.execution_status == JobStatus.RUNNING

    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        raise NotImplementedError("Subclass must implement get_output_assets()")

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {}  

    @staticmethod
    def get_input_assets(
        job_dir: Path, project_root: Path, upstream_outputs: Dict[str, Dict[str, Path]]
    ) -> Dict[str, Path]:
        return {"job_dir": job_dir, "project_root": project_root}  

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        return None  

    @classmethod
    def from_pipeline_state(cls, state: "PipelineState") -> Self:
        raise NotImplementedError("Subclass must implement from_pipeline_state()")

    def sync_from_pipeline_state(self, state: "PipelineState") -> Self:
        return self  

    def is_driver_job(self) -> bool:
        """Returns True if this job uses a Python driver, False if it's a direct command."""
        return False  

    def get_tool_name(self) -> str:
        """Returns the container tool name (e.g., 'relion_import', 'warptools')."""
        raise NotImplementedError("Subclass must implement get_tool_name()")

class ImportMoviesParams(AbstractJobParams):
    """Parameters for import movies job - implements JobParamsProtocol"""

    model_config = ConfigDict(validate_assignment=True)

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.IMPORT

    pixel_size          : float = Field(ge=0.5, le=10.0)
    voltage             : float = Field(ge=50.0)
    spherical_aberration: float = Field(ge=0.0)
    amplitude_contrast  : float = Field(ge=0.0, le=1.0)

    dose_per_tilt_image: float = Field(ge=0.1)
    tilt_axis_angle    : float = Field(ge=-180.0, le=180.0)

    optics_group_name  : str  = "opticsGroup1"
    do_at_most         : int  = Field(default=-1)
    invert_defocus_hand: bool = False

    def get_tool_name(self) -> str:
        return "relion_import"


    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        """Load defaults from job.star template"""
        if not star_path or not star_path.exists():
            return None

        try:
            data: Dict[str, Union[pd.DataFrame, Dict[str, Any]]] = starfile.read(star_path, always_dict=True)

            job_data = data.get("job")
            if job_data is None:
                return None

            if isinstance(job_data, pd.DataFrame):
                if len(job_data) == 0:
                    return None
                job_params: Dict[str, Any] = job_data.to_dict("records")[0]
            else:
                job_params: Dict[str, Any] = job_data

            return cls(
                pixel_size           = float(job_params.get("nominal_pixel_size", 1.35)),
                voltage              = float(job_params.get("voltage", 300)),
                spherical_aberration = float(job_params.get("spherical_aberration", 2.7)),
                amplitude_contrast   = float(job_params.get("amplitude_contrast", 0.1)),
                dose_per_tilt_image  = float(job_params.get("dose_per_tilt_image", 3.0)),
                tilt_axis_angle      = float(job_params.get("nominal_tilt_axis_angle", -95.0)),
                optics_group_name    = job_params.get("optics_group_name", "opticsGroup1"),
                invert_defocus_hand  = bool(job_params.get("invert_defocus_hand", False)),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    @classmethod
    def from_pipeline_state(cls, state: "PipelineState") -> Self:
        """Create from global pipeline state"""
        return cls(
            pixel_size           = state.microscope.pixel_size_angstrom,
            voltage              = state.microscope.acceleration_voltage_kv,
            spherical_aberration = state.microscope.spherical_aberration_mm,
            amplitude_contrast   = state.microscope.amplitude_contrast,
            dose_per_tilt_image  = state.acquisition.dose_per_tilt,
            tilt_axis_angle      = state.acquisition.tilt_axis_degrees,
            invert_defocus_hand  = state.acquisition.invert_defocus_hand,
        )

    def sync_from_pipeline_state(self, state: "PipelineState") -> Self:
        """Update params from global state - only works for SCHEDULED jobs"""
        if self.execution_status != JobStatus.SCHEDULED:
            return self
        """Update microscope/acquisition params from global state IN-PLACE"""
        self.pixel_size           = state.microscope.pixel_size_angstrom
        self.voltage              = state.microscope.acceleration_voltage_kv
        self.spherical_aberration = state.microscope.spherical_aberration_mm
        self.amplitude_contrast   = state.microscope.amplitude_contrast
        self.dose_per_tilt_image  = state.acquisition.dose_per_tilt
        self.tilt_axis_angle      = state.acquisition.tilt_axis_degrees
        self.invert_defocus_hand  = state.acquisition.invert_defocus_hand
        return self

    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        return {
            "job_dir"         : job_dir,
            "tilt_series_star": job_dir / "tilt_series.star",
            "tilt_series_dir" : job_dir / "tilt_series",
            "log"             : job_dir / "log.txt",
        }

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {}  

    @staticmethod
    def get_input_assets(
        job_dir: Path, project_root: Path, upstream_outputs: Dict[str, Dict[str, Path]]
    ) -> Dict[str, Path]:
        return {"job_dir": job_dir, "frames_dir": project_root / "frames", "mdoc_dir": project_root / "mdoc"}

class FsMotionCtfParams(AbstractJobParams):
    """Parameters for WarpTools motion correction and CTF"""

    model_config = ConfigDict(validate_assignment=True)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    pixel_size: float = Field(ge=0.5, le=10.0)
    voltage   : float = Field(ge=50.0)
    cs        : float = Field(ge=0.0)
    amplitude : float = Field(ge=0.0, le=1.0)

    eer_ngroups: int = Field(default=32, ge=1)

    # Motion correction
    m_range_min_max   : str = "500:10"
    m_bfac            : int = Field(default=-500)
    m_grid            : str = "1x1x3"
    
    # CTF estimation
    c_range_min_max   : str = "30:6.0"
    c_defocus_min_max : str = "1.1:8"
    c_grid            : str = "2x2x1"
    c_window          : int = Field(default=512, ge=128)
    c_use_sum         : bool = False  # NEW: Use average spectrum instead of individual frames
    
    # Output options
    out_average_halves: bool = True
    out_skip_first    : int = 0       # Skip first N frames
    out_skip_last     : int = 0       # Skip last N frames

    perdevice : int = Field(default=1, ge=0, le=8)
    do_at_most: int = Field(default=-1)

    gain_path      : Optional[str] = None
    gain_operations: Optional[str] = None

    def is_driver_job(self) -> bool:
        return True  

    def get_tool_name(self) -> str:
        return "warptools"  

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

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        """Load from job.star template"""
        if not star_path or not star_path.exists():
            return None

        try:
            data: Dict[str, Union[pd.DataFrame, dict]] = starfile.read(star_path, always_dict=True)

            joboptions = data.get("joboptions_values")
            if joboptions is None or not isinstance(joboptions, pd.DataFrame):
                return None

            df: pd.DataFrame = joboptions

            param_dict: Dict[str, str] = pd.Series(
                df["rlnJobOptionValue"].values, index=df["rlnJobOptionVariable"].values
            ).to_dict()

            return cls(
                pixel_size        = 1.35,
                voltage           = 300.0,
                cs                = 2.7,
                amplitude         = 0.1,
                eer_ngroups       = int(param_dict.get("param1_value", "32")),
                gain_path         = param_dict.get("param2_value"),
                gain_operations   = param_dict.get("param3_value"),
                m_range_min_max   = param_dict.get("param4_value", "500:10"),
                m_bfac            = int(param_dict.get("param5_value", "-500")),
                m_grid            = param_dict.get("param6_value", "1x1x3"),
                c_range_min_max   = param_dict.get("param7_value", "30:6.0"),
                c_defocus_min_max = param_dict.get("param8_value", "1.1:8"),
                c_grid            = param_dict.get("param9_value", "2x2x1"),
                perdevice         = int(param_dict.get("param10_value", "1")),
                c_window          = 512,
                # NEW: Set defaults for new parameters
                c_use_sum         = param_dict.get("param11_value", "False").lower() == "true",
                out_average_halves= param_dict.get("param12_value", "False").lower() == "true",
                out_skip_first    = int(param_dict.get("param13_value", "0")),
                out_skip_last     = int(param_dict.get("param14_value", "0")),
            )

        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    @classmethod
    def from_pipeline_state(cls, state: "PipelineState") -> Self:
        """Create from global pipeline state"""
        return cls(
            pixel_size  = state.microscope.pixel_size_angstrom,
            voltage     = state.microscope.acceleration_voltage_kv,
            cs          = state.microscope.spherical_aberration_mm,
            amplitude   = state.microscope.amplitude_contrast,
            eer_ngroups = state.acquisition.eer_fractions_per_frame or 32,
            gain_path   = state.acquisition.gain_reference_path,
        )

    def sync_from_pipeline_state(self, state: "PipelineState") -> Self:
        """Update params from global state - only works for SCHEDULED jobs"""
        if self.execution_status != JobStatus.SCHEDULED:
            return self
        self.pixel_size  = state.microscope.pixel_size_angstrom
        self.voltage     = state.microscope.acceleration_voltage_kv
        self.cs          = state.microscope.spherical_aberration_mm
        self.amplitude   = state.microscope.amplitude_contrast
        self.eer_ngroups = state.acquisition.eer_fractions_per_frame or 32
        self.gain_path   = state.acquisition.gain_reference_path
        return self

    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        """Define all outputs this job produces"""
        return {
            "job_dir"        : job_dir,
            "output_star"    : job_dir / "fs_motion_and_ctf.star",
            "tilt_series_dir": job_dir / "tilt_series",
            "warp_dir"       : job_dir / "warp_frameseries",
            "warp_settings"  : job_dir / "warp_frameseries.settings",
            "xml_pattern"    : str(job_dir / "warp_frameseries" / "*.xml"),
        }

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        """This job needs outputs from importmovies"""
        return {
            "import": "importmovies"  # Key is logical name, value is job type
        }

    @staticmethod
    def get_input_assets(
        job_dir: Path, project_root: Path, upstream_outputs: Dict[str, Dict[str, Path]]
    ) -> Dict[str, Path]:
        """Map upstream outputs to this job's inputs"""
        import_outputs = upstream_outputs.get("importmovies", {})

        return {
            "job_dir"    : job_dir,
            "input_star" : import_outputs.get("tilt_series_star"),
            "output_star": job_dir / "fs_motion_and_ctf.star",
            "warp_dir"   : job_dir / "warp_frameseries",
            "frames_dir" : project_root / "frames",
            "mdoc_dir"   : project_root / "mdoc",
        }

class TsAlignmentParams(AbstractJobParams):
    """Parameters for tilt series alignment"""

    model_config = ConfigDict(validate_assignment=True)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    # Synced from global state
    pixel_size        : float = Field(default=1.35)                     
    dose_per_tilt     : float = Field(default=3.0)
    tilt_axis_angle   : float = Field(default=-95.0)
    invert_tilt_angles: bool  = False
    thickness_nm      : float = Field(default=300.0, ge=50.0, le=2000.0) 

    # Job-specific
    alignment_method: AlignmentMethod = AlignmentMethod.ARETOMO
    rescale_angpixs : float           = Field(default=12.0, ge=2.0, le=50.0)  
    tomo_dimensions : str             = Field(default="4096x4096x2048")      
    do_at_most      : int             = Field(default=-1)
    perdevice       : int             = Field(default=1, ge=0, le=8)
    mdoc_pattern    : str             = Field(default="*.mdoc")  # NEW: Make pattern configurable

    # Optional gain
    gain_path: Optional[str] = None
    gain_operations: Optional[str] = None

    patch_x   : int = Field(default=2, ge=0)  #  default to 2 to match old code
    patch_y   : int = Field(default=2, ge=0)  #  default to 2 to match old code
    axis_iter : int = Field(default=1, ge=0)
    axis_batch: int = Field(default=5, ge=1)

    imod_patch_size: int = Field(default=200)
    imod_overlap   : int = Field(default=50)

    def is_driver_job(self) -> bool:
        return True  # This job also uses a Python driver

    def get_tool_name(self) -> str:
        return "warptools"  # The driver will use this tool (or 'warptools')

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        """Load from job.star template"""
        if not star_path or not star_path.exists():
            return None

        try:
            data: Dict[str, Union[pd.DataFrame, dict]] = starfile.read(star_path, always_dict=True)

            job_data = data.get("job")
            if job_data is None:
                return None

            if isinstance(job_data, pd.DataFrame):
                if len(job_data) == 0:
                    return None
                job_params: Dict[str, Any] = job_data.to_dict("records")[0]
            else:
                job_params: Dict[str, Any] = job_data

            method_str = job_params.get("alignment_method", "AreTomo")
            try:
                method = AlignmentMethod(method_str)
            except ValueError:
                method = AlignmentMethod.ARETOMO

            # Handle patch parameters - old code used string like "2x2"
            patch_str = job_params.get("aretomo_patches", "2x2")
            patch_x, patch_y = 2, 2
            if "x" in patch_str:
                patch_x, patch_y = map(int, patch_str.split("x"))
            
            # Handle axis refinement - old code used string like "3:5"
            axis_str = job_params.get("refineTiltAxis_iter_and_batch", "3:5")
            axis_iter, axis_batch = 3, 5
            if ":" in axis_str:
                axis_iter, axis_batch = map(int, axis_str.split(":"))

            return cls(
                alignment_method=method,
                rescale_angpixs=float(job_params.get("rescale_angpixs", 12.0)),
                thickness_nm    = float(job_params.get("aretomo_sample_thickness", 300.0)),
                tomo_dimensions = job_params.get("tomo_dimensions", "4096x4096x2048"),
                gain_path       = job_params.get("gain_path"),
                gain_operations = job_params.get("gain_operations"),
                perdevice       = int(job_params.get("perdevice", 1)),
                mdoc_pattern    = job_params.get("mdoc_pattern", "*.mdoc"),
                patch_x         = patch_x,
                patch_y         = patch_y,
                axis_iter       = axis_iter,
                axis_batch      = axis_batch,
                imod_patch_size = int(job_params.get("imod_patch_size", 200)),
                imod_overlap    = int(job_params.get("imod_overlap", 50)),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    @classmethod
    def from_pipeline_state(cls, state: "PipelineState") -> Self:
        """Create from global pipeline state"""
        return cls(
            thickness_nm       = state.acquisition.sample_thickness_nm,
            pixel_size         = state.microscope.pixel_size_angstrom,
            dose_per_tilt      = state.acquisition.dose_per_tilt,
            tilt_axis_angle    = state.acquisition.tilt_axis_degrees,
            invert_tilt_angles = state.acquisition.invert_tilt_angles,
            gain_path          = state.acquisition.gain_reference_path,
        )

    def sync_from_pipeline_state(self, state: "PipelineState") -> Self:
        """Update params from global state - only works for SCHEDULED jobs"""
        if self.execution_status != JobStatus.SCHEDULED:
            return self
        self.thickness_nm       = state.acquisition.sample_thickness_nm
        self.pixel_size         = state.microscope.pixel_size_angstrom
        self.dose_per_tilt      = state.acquisition.dose_per_tilt
        self.tilt_axis_angle    = state.acquisition.tilt_axis_degrees
        self.invert_tilt_angles = state.acquisition.invert_tilt_angles
        self.gain_path          = state.acquisition.gain_reference_path
        return self

    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        return {
            "job_dir"        : job_dir,
            "output_star"    : job_dir / "aligned_tilt_series.star",
            "tilt_series_dir": job_dir / "tilt_series",
            "warp_dir"       : job_dir / "warp_tiltseries",
            "warp_settings"  : job_dir / "warp_tiltseries.settings",
            "tomostar_dir"   : job_dir / "tomostar",
        }

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"motion": "fsMotionAndCtf"}

    @staticmethod
    def get_input_assets(
        job_dir: Path, project_root: Path, upstream_outputs: Dict[str, Dict[str, Path]]
    ) -> Dict[str, Path]:
        motion_outputs = upstream_outputs.get("fsMotionAndCtf", {})
        return {
            "job_dir"        : job_dir,
            "input_star"     : motion_outputs.get("output_star"),
            "frameseries_dir": motion_outputs.get("warp_dir"),
            "output_star"    : job_dir / "aligned_tilt_series.star",
            "mdoc_dir"       : project_root / "mdoc",
            "tomostar_dir"   : job_dir / "tomostar",
            "warp_dir"       : job_dir / "warp_tiltseries",
        }

class TsCtfParams(AbstractJobParams):
    """Parameters for tilt series CTF estimation"""

    model_config = ConfigDict(validate_assignment=True)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    # CTF parameters
    window         : int = Field(default=512, ge=128, le=2048)
    range_min_max  : str = Field(default="30:6.0")                # Resolution range in Angstrom
    defocus_min_max: str = Field(default="0.5:8")               # Defocus range in microns
    defocus_hand   : str = Field(default="set_flip")            # set_flip or set_normal
    
    # GPU settings
    perdevice: int   = Field(default=1, ge=0, le=8)
    voltage  : float = Field(default=300.0)
    cs       : float = Field(default=2.7)
    amplitude: float = Field(default=0.1)

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "warptools"

    @property
    def range_min(self) -> float:
        return float(self.range_min_max.split(":")[0])

    @property
    def range_max(self) -> float:
        return float(self.range_min_max.split(":")[1])

    @property
    def defocus_min(self) -> float:
        return float(self.defocus_min_max.split(":")[0])

    @property
    def defocus_max(self) -> float:
        return float(self.defocus_min_max.split(":")[1])

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        """Load from job.star template"""
        if not star_path or not star_path.exists():
            return None

        try:
            data: Dict[str, Union[pd.DataFrame, dict]] = starfile.read(star_path, always_dict=True)

            joboptions = data.get("joboptions_values")
            if joboptions is None or not isinstance(joboptions, pd.DataFrame):
                return None

            df: pd.DataFrame = joboptions
            param_dict: Dict[str, str] = pd.Series(
                df["rlnJobOptionValue"].values, index=df["rlnJobOptionVariable"].values
            ).to_dict()

            return cls(
                window          = int(param_dict.get("param1_value", "512")),
                range_min_max   = param_dict.get("param2_value", "30:4"),
                defocus_min_max = param_dict.get("param3_value", "0.5:8"),
                defocus_hand    = param_dict.get("param4_value", "set_flip"),
                perdevice       = int(param_dict.get("param5_value", "1")),
            )

        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    @classmethod
    def from_pipeline_state(cls, state: "PipelineState") -> Self:
        """Create from global pipeline state"""
        return cls(
            voltage=state.microscope.acceleration_voltage_kv,
            cs=state.microscope.spherical_aberration_mm,
            amplitude=state.microscope.amplitude_contrast,
        )

    def sync_from_pipeline_state(self, state: "PipelineState") -> Self:
        """Update params from global state - only works for SCHEDULED jobs"""
        if self.execution_status != JobStatus.SCHEDULED:
            return self
        self.voltage = state.microscope.acceleration_voltage_kv
        self.cs = state.microscope.spherical_aberration_mm
        self.amplitude = state.microscope.amplitude_contrast
        return self

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"alignment": "aligntiltsWarp"}

    # In parameter_models.py - TsCtfParams class

    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        """Output paths for this job"""
        return {
            "job_dir"        : job_dir,
            "output_star"    : job_dir / "ts_ctf_tilt_series.star",
            "tilt_series_dir": job_dir / "tilt_series",
            "warp_dir"       : job_dir / "warp_tiltseries",
            "warp_settings"  : job_dir / "warp_tiltseries.settings",
            "tomostar_dir"   : job_dir / "tomostar",
            "xml_pattern"    : str(job_dir / "warp_tiltseries" / "*.xml"),
        }

    @staticmethod
    def get_input_assets(
        job_dir: Path, project_root: Path, upstream_outputs: Dict[str, Dict[str, Path]]
    ) -> Dict[str, Path]:
        """Input paths from upstream jobs"""
        alignment_outputs = upstream_outputs.get("aligntiltsWarp", {})
        return {
            "input_star"      : alignment_outputs.get("output_star"),  
            "frameseries_dir" : alignment_outputs.get("warp_dir"),
            "warp_settings_in": alignment_outputs.get("warp_settings"),
            "tomostar_dir_in" : alignment_outputs.get("tomostar_dir"),
        }

class TsReconstructParams(AbstractJobParams):
    """Parameters for tomogram reconstruction"""

    model_config = ConfigDict(validate_assignment=True)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    # Reconstruction parameters
    rescale_angpixs: float = Field(default=12.0, ge=2.0, le=50.0)
    halfmap_frames : int   = Field(default=1, ge=0, le=1)          # Boolean as int
    deconv         : int   = Field(default=1, ge=0, le=1)          # Boolean as int
    perdevice      : int   = Field(default=1, ge=0, le=8)
    
    # Synced from global state
    pixel_size: float = Field(default=1.35)

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "warptools"

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        """Load from job.star template"""
        if not star_path or not star_path.exists():
            return None

        try:
            data: Dict[str, Union[pd.DataFrame, dict]] = starfile.read(star_path, always_dict=True)

            joboptions = data.get("joboptions_values")
            if joboptions is None or not isinstance(joboptions, pd.DataFrame):
                return None

            df: pd.DataFrame = joboptions
            param_dict: Dict[str, str] = pd.Series(
                df["rlnJobOptionValue"].values, index=df["rlnJobOptionVariable"].values
            ).to_dict()

            return cls(
                rescale_angpixs = float(param_dict.get("param1_value", "12.0")),
                halfmap_frames  = int(param_dict.get("param2_value", "1")),
                deconv          = int(param_dict.get("param3_value", "1")),
                perdevice       = int(param_dict.get("param4_value", "1")),
            )

        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    @classmethod
    def from_pipeline_state(cls, state: "PipelineState") -> Self:
        """Create from global pipeline state"""
        return cls(
            pixel_size=state.microscope.pixel_size_angstrom,
        )

    def sync_from_pipeline_state(self, state: "PipelineState") -> Self:
        """Update params from global state - only works for SCHEDULED jobs"""
        if self.execution_status != JobStatus.SCHEDULED:
            return self
        self.pixel_size = state.microscope.pixel_size_angstrom
        return self

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"ctf": "tsCtf"}

    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        """Output paths for this job"""
        return {
            "job_dir"           : job_dir,
            "output_star"       : job_dir / "tomograms.star",
            "tilt_series_dir"   : job_dir / "tilt_series",
            "warp_dir"          : job_dir / "warp_tiltseries",
            "warp_settings"     : job_dir / "warp_tiltseries.settings",
            "tomostar_dir"      : job_dir / "tomostar",
            "reconstruction_dir": job_dir / "warp_tiltseries" / "reconstruction",
        }

    @staticmethod
    def get_input_assets(
        job_dir: Path, project_root: Path, upstream_outputs: Dict[str, Dict[str, Path]]
    ) -> Dict[str, Path]:
        """Input paths from upstream jobs"""
        ctf_outputs = upstream_outputs.get("tsCtf", {})
        return {
            "input_star"      : ctf_outputs.get("output_star"),
            "warp_dir_in"     : ctf_outputs.get("warp_dir"),
            "warp_settings_in": ctf_outputs.get("warp_settings"),
            "tomostar_dir_in" : ctf_outputs.get("tomostar_dir"),
        }


def jobtype_paramclass() -> Dict[JobType, Type[AbstractJobParams]]:
    return {
        JobType.IMPORT_MOVIES: ImportMoviesParams,
        JobType.FS_MOTION_CTF: FsMotionCtfParams,
        JobType.TS_ALIGNMENT: TsAlignmentParams,
        JobType.TS_CTF: TsCtfParams,
        JobType.TS_RECONSTRUCT: TsReconstructParams,
    }
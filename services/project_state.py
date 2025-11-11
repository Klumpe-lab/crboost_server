# services/project_state.py
"""
Unified project state - single source of truth for all parameters.
Based on the new architecture proposal.
"""

from __future__ import annotations
from enum import Enum
from pathlib import Path
from typing import Dict, Any, Optional, Type
from pydantic import BaseModel, Field
from datetime import datetime
import json
import sys
from pydantic import field_validator, ConfigDict
from typing import ClassVar, Tuple, Self, Union, TYPE_CHECKING
import pandas as pd
import starfile


if TYPE_CHECKING:
    from services.project_state import ProjectState 

class JobStatus(str, Enum):
    SUCCEEDED = "Succeeded"
    FAILED    = "Failed"
    RUNNING   = "Running"
    SCHEDULED = "Scheduled"
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

class JobCategory(str, Enum):
    IMPORT     = "Import"
    EXTERNAL   = "External"
    MOTIONCORR = "MotionCorr"
    CTFFIND    = "CtfFind"

class JobType(str, Enum):
    IMPORT_MOVIES    = "importmovies"
    FS_MOTION_CTF    = "fsMotionAndCtf"
    TS_ALIGNMENT     = "aligntiltsWarp"
    TS_CTF           = "tsCtf"
    TS_RECONSTRUCT   = "tsReconstruct"
    DENOISE_TRAIN    = "denoiseTrain"
    TEMPLATE_MATCH   = "templateMatching"
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
        return self.value.replace("_", " ").title()

class MicroscopeParams(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    microscope_type        : MicroscopeType = MicroscopeType.CUSTOM
    pixel_size_angstrom    : float          = Field(default=1.35, ge=0.5, le=10.0)
    acceleration_voltage_kv: float          = Field(default=300.0)
    spherical_aberration_mm: float          = Field(default=2.7, ge=0.0, le=10.0)
    amplitude_contrast     : float          = Field(default=0.1, ge=0.0, le=1.0)

    @field_validator("acceleration_voltage_kv")
    @classmethod
    def validate_voltage(cls, v: float) -> float:
        allowed = [200.0, 300.0]
        if v not in allowed:
            print(f"[WARN] Voltage {v} not in standard values {allowed}")
        return v

class AcquisitionParams(BaseModel):
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
    model_config = ConfigDict(validate_assignment=True)

    partition: Partition = Partition.GPU
    gpu_count: int       = Field(default=1, ge=0, le=8)
    memory_gb: int       = Field(default=32, ge=4, le=512)
    threads  : int       = Field(default=8, ge=1, le=128)
    @classmethod
    def from_conf_yaml(cls, config_path: Path) -> Self:
        try:
            from services.config_service import get_config_service
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

# --- REFACTORED JOB MODELS ---

class AbstractJobParams(BaseModel):
    """
    Abstract base class for job parameters.
    Contains NO global parameters, only accessors.
    """
    
    # model_config = ConfigDict(validate_assignment=True, private_attributes_allowed=True)
    
    JOB_CATEGORY: ClassVar[JobCategory]
    
    # Job execution metadata only
    execution_status: JobStatus = Field(default=JobStatus.SCHEDULED)
    relion_job_name: Optional[str] = None
    relion_job_number: Optional[int] = None
    
    # This is now a private attribute, not a Pydantic model field.
    _project_state: Optional["ProjectState"] = None
    
    
    # --- Global parameter access via properties - THE KEY INNOVATION ---
    
    @property
    def microscope(self) -> MicroscopeParams:
        if self._project_state is None:
            raise RuntimeError(f"Job {type(self).__name__} not attached to project state")
        return self._project_state.microscope
        
    @property
    def acquisition(self) -> AcquisitionParams:
        if self._project_state is None:
            raise RuntimeError(f"Job {type(self).__name__} not attached to project state")
        return self._project_state.acquisition
        
    # Example properties for direct access
    @property
    def pixel_size(self) -> float:
        return self.microscope.pixel_size_angstrom
        
    @property
    def voltage(self) -> float:
        return self.microscope.acceleration_voltage_kv
        
    @property
    def spherical_aberration(self) -> float:
        return self.microscope.spherical_aberration_mm
        
    @property
    def amplitude_contrast(self) -> float:
        return self.microscope.amplitude_contrast
        
    @property
    def dose_per_tilt(self) -> float:
        return self.acquisition.dose_per_tilt
        
    @property
    def tilt_axis_angle(self) -> float:
        return self.acquisition.tilt_axis_degrees
        
    @property
    def thickness_nm(self) -> float:
        return self.acquisition.sample_thickness_nm
        
    @property
    def eer_ngroups(self) -> int:
        return self.acquisition.eer_fractions_per_frame or 32
        
    @property
    def gain_path(self) -> Optional[str]:
        return self.acquisition.gain_reference_path
        
    @property
    def invert_tilt_angles(self) -> bool:
        return self.acquisition.invert_tilt_angles

    # --- End of Properties ---
    
    def __setattr__(self, name: str, value: Any) -> None:
        """Enforce immutability for started/completed jobs"""
        if name in ['execution_status', 'relion_job_name', 'relion_job_number', '_project_state']:
            super().__setattr__(name, value)
            return
        
        if name.startswith('_'):
            super().__setattr__(name, value)
            return
            
        try:
            current_status = object.__getattribute__(self, 'execution_status')
        except AttributeError:
            super().__setattr__(name, value)
            return
            
        if current_status != JobStatus.SCHEDULED:
            print(f"[IMMUTABLE] Blocked change to '{name}' on {current_status.value} job")
            return
            
        super().__setattr__(name, value)

    @property
    def display_status(self) -> str:
        return self.execution_status.value
    @property 
    def has_succeeded(self) -> bool:
        return self.execution_status == JobStatus.SUCCEEDED
    @property
    def is_running(self) -> bool:
        return self.execution_status == JobStatus.RUNNING

    # --- Abstract methods ---
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
        return None # Default implementation
    def is_driver_job(self) -> bool:
        return False
    def get_tool_name(self) -> str:
        raise NotImplementedError("Subclass must implement get_tool_name()")
        
    # --- ELIMINATED METHODS ---
    # NO from_pipeline_state
    # NO sync_from_pipeline_state


class ImportMoviesParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.IMPORT

    # JOB-SPECIFIC PARAMETERS ONLY
    optics_group_name: str = "opticsGroup1"
    do_at_most: int = Field(default=-1)
    
    # NO pixel_size, voltage, spherical_aberration, etc.
    # They are accessed via properties from AbstractJobParams

    def get_tool_name(self) -> str:
        return "relion_import"

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        if not star_path or not star_path.exists():
            return None
        try:
            data: Dict[str, Union[pd.DataFrame, Dict[str, Any]]] = starfile.read(star_path, always_dict=True)
            job_data = data.get("job")
            if job_data is None: return None
            
            if isinstance(job_data, pd.DataFrame):
                if len(job_data) == 0: return None
                job_params: Dict[str, Any] = job_data.to_dict("records")[0]
            else:
                job_params: Dict[str, Any] = job_data
            
            # Load ONLY job-specific parameters
            return cls(
                optics_group_name=job_params.get("optics_group_name", "opticsGroup1"),
                do_at_most=int(job_params.get("do_at_most", -1)), # Example if this was in the star
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None
            
    # Asset definition methods
    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        return {
            "job_dir": job_dir,
            "tilt_series_star": job_dir / "tilt_series.star",
            "tilt_series_dir": job_dir / "tilt_series",
            "log": job_dir / "log.txt",
        }
    @staticmethod
    def get_input_assets(
        job_dir: Path, project_root: Path, upstream_outputs: Dict[str, Dict[str, Path]]
    ) -> Dict[str, Path]:
        return {"job_dir": job_dir, "frames_dir": project_root / "frames", "mdoc_dir": project_root / "mdoc"}

class FsMotionCtfParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    m_range_min_max   : str           = "500:10"
    m_bfac            : int           = Field(default=-500)
    m_grid            : str           = "1x1x3"
    c_range_min_max   : str           = "30:6.0"
    c_defocus_min_max : str           = "1.1:8"
    c_grid            : str           = "2x2x1"
    c_window          : int           = Field(default=512, ge=128)
    c_use_sum         : bool          = False
    out_average_halves: bool          = True
    out_skip_first    : int           = 0
    out_skip_last     : int           = 0
    perdevice         : int           = Field(default=1, ge=0, le=8)
    do_at_most        : int           = Field(default=-1)
    gain_operations   : Optional[str] = None
    

    def is_driver_job(self) -> bool: return True
    def get_tool_name(self) -> str: return "warptools"

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
    # --- PROPERTIES END HERE ---

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        if not star_path or not star_path.exists(): return None
        try:
            data: Dict[str, Union[pd.DataFrame, dict]] = starfile.read(star_path, always_dict=True)
            joboptions = data.get("joboptions_values")
            if joboptions is None or not isinstance(joboptions, pd.DataFrame): return None
            
            df: pd.DataFrame = joboptions
            param_dict: Dict[str, str] = pd.Series(
                df["rlnJobOptionValue"].values, index=df["rlnJobOptionVariable"].values
            ).to_dict()
            
            # Load ONLY job-specific parameters
            return cls(
                gain_operations    = param_dict.get("param3_value") ,
                m_range_min_max    = param_dict.get("param4_value", "500:10") ,
                m_bfac             = int(param_dict.get("param5_value", "-500")) ,
                m_grid             = param_dict.get("param6_value", "1x1x3") ,
                c_range_min_max    = param_dict.get("param7_value", "30:6.0") ,
                c_defocus_min_max  = param_dict.get("param8_value", "1.1:8") ,
                c_grid             = param_dict.get("param9_value", "2x2x1") ,
                perdevice          = int(param_dict.get("param10_value", "1")) ,
                c_window           = 512 ,
                c_use_sum          = param_dict.get("param11_value", "False").lower() == "true",
                out_average_halves = True,
                out_skip_first     = int(param_dict.get("param13_value", "0")) ,
                out_skip_last      = int(param_dict.get("param14_value", "0")) ,
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    # Asset definitions
    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        return {
            "job_dir": job_dir,
            "output_star": job_dir / "fs_motion_and_ctf.star",
            "tilt_series_dir": job_dir / "tilt_series",
            "warp_dir": job_dir / "warp_frameseries",
            "warp_settings": job_dir / "warp_frameseries.settings",
            "xml_pattern": str(job_dir / "warp_frameseries" / "*.xml"),
        }
    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"import": "importmovies"}
    @staticmethod
    def get_input_assets(
        job_dir: Path, project_root: Path, upstream_outputs: Dict[str, Dict[str, Path]]
    ) -> Dict[str, Path]:
        import_outputs = upstream_outputs.get("importmovies", {})
        return {
            "job_dir": job_dir,
            "input_star": import_outputs.get("tilt_series_star"),
            "output_star": job_dir / "fs_motion_and_ctf.star",
            "warp_dir": job_dir / "warp_frameseries",
            "frames_dir": project_root / "frames",
            "mdoc_dir": project_root / "mdoc",
        }

class TsAlignmentParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    # JOB-SPECIFIC PARAMETERS ONLY
    alignment_method: AlignmentMethod = AlignmentMethod.ARETOMO
    rescale_angpixs  : float          = Field(default=12.0, ge=2.0, le=50.0)
    tomo_dimensions : str             = Field(default="4096x4096x2048")
    do_at_most      : int             = Field(default=-1)
    perdevice       : int             = Field(default=1, ge=0, le=8)
    mdoc_pattern    : str             = Field(default="*.mdoc")
    gain_operations : Optional[str]   = None
    patch_x         : int             = Field(default=2, ge=0)
    patch_y         : int             = Field(default=2, ge=0)
    axis_iter       : int             = Field(default=1, ge=0)
    axis_batch      : int             = Field(default=5, ge=1)
    imod_patch_size : int             = Field(default=200)
    imod_overlap    : int             = Field(default=50)

    # NO pixel_size, dose_per_tilt, tilt_axis_angle, invert_tilt_angles, thickness_nm, gain_path
    # They are accessed via properties.

    def is_driver_job(self) -> bool: return True
    def get_tool_name(self) -> str: return "warptools"

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        if not star_path or not star_path.exists(): return None
        try:
            data: Dict[str, Union[pd.DataFrame, dict]] = starfile.read(star_path, always_dict=True)
            job_data = data.get("job")
            if job_data is None: return None
            if isinstance(job_data, pd.DataFrame):
                if len(job_data) == 0: return None
                job_params: Dict[str, Any] = job_data.to_dict("records")[0]
            else:
                job_params: Dict[str, Any] = job_data
            
            method = AlignmentMethod(job_params.get("alignment_method", "AreTomo"))
            patch_str = job_params.get("aretomo_patches", "2x2")
            patch_x, patch_y = map(int, patch_str.split("x")) if "x" in patch_str else (2, 2)
            axis_str = job_params.get("refineTiltAxis_iter_and_batch", "3:5")
            axis_iter, axis_batch = map(int, axis_str.split(":")) if ":" in axis_str else (3, 5)

            return cls(
                alignment_method=method,
                rescale_angpixs=float(job_params.get("rescale_angpixs", 12.0)),
                tomo_dimensions=job_params.get("tomo_dimensions", "4096x4096x2048"),
                gain_operations=job_params.get("gain_operations"),
                perdevice=int(job_params.get("perdevice", 1)),
                mdoc_pattern=job_params.get("mdoc_pattern", "*.mdoc"),
                patch_x=patch_x,
                patch_y=patch_y,
                axis_iter=axis_iter,
                axis_batch=axis_batch,
                imod_patch_size=int(job_params.get("imod_patch_size", 200)),
                imod_overlap=int(job_params.get("imod_overlap", 50)),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None
            
    # Asset definitions
    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        return {
            "job_dir": job_dir,
            "output_star": job_dir / "aligned_tilt_series.star",
            "tilt_series_dir": job_dir / "tilt_series",
            "warp_dir": job_dir / "warp_tiltseries",
            "warp_settings": job_dir / "warp_tiltseries.settings",
            "tomostar_dir": job_dir / "tomostar",
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
            "job_dir": job_dir,
            "input_star": motion_outputs.get("output_star"),
            "frameseries_dir": motion_outputs.get("warp_dir"),
            "output_star": job_dir / "aligned_tilt_series.star",
            "mdoc_dir": project_root / "mdoc",
            "tomostar_dir": job_dir / "tomostar",
            "warp_dir": job_dir / "warp_tiltseries",
        }

class TsCtfParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    # JOB-SPECIFIC PARAMETERS ONLY
    window: int = Field(default=512, ge=128, le=2048)
    range_min_max: str = Field(default="30:6.0")
    defocus_min_max: str = Field(default="0.5:8")
    defocus_hand: str = Field(default="set_flip")
    perdevice: int = Field(default=1, ge=0, le=8)
    
    # NO voltage, cs, amplitude
    # They are accessed via properties

    def is_driver_job(self) -> bool: return True
    def get_tool_name(self) -> str: return "warptools"

    # --- PROPERTIES START HERE ---
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
    # --- PROPERTIES END HERE ---

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        if not star_path or not star_path.exists(): return None
        try:
            data: Dict[str, Union[pd.DataFrame, dict]] = starfile.read(star_path, always_dict=True)
            joboptions = data.get("joboptions_values")
            if joboptions is None or not isinstance(joboptions, pd.DataFrame): return None
            
            df: pd.DataFrame = joboptions
            param_dict: Dict[str, str] = pd.Series(
                df["rlnJobOptionValue"].values, index=df["rlnJobOptionVariable"].values
            ).to_dict()
            
            return cls(
                window=int(param_dict.get("param1_value", "512")),
                range_min_max=param_dict.get("param2_value", "30:4"),
                defocus_min_max=param_dict.get("param3_value", "0.5:8"),
                defocus_hand=param_dict.get("param4_value", "set_flip"),
                perdevice=int(param_dict.get("param5_value", "1")),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    # Asset definitions
    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"alignment": "aligntiltsWarp"}
    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        return {
            "job_dir": job_dir,
            "output_star": job_dir / "ts_ctf_tilt_series.star",
            "tilt_series_dir": job_dir / "tilt_series",
            "warp_dir": job_dir / "warp_tiltseries",
            "warp_settings": job_dir / "warp_tiltseries.settings",
            "tomostar_dir": job_dir / "tomostar",
            "xml_pattern": str(job_dir / "warp_tiltseries" / "*.xml"),
        }
    @staticmethod
    def get_input_assets(
        job_dir: Path, project_root: Path, upstream_outputs: Dict[str, Dict[str, Path]]
    ) -> Dict[str, Path]:
        alignment_outputs = upstream_outputs.get("aligntiltsWarp", {})
        return {
            "input_star": alignment_outputs.get("output_star"),
            "frameseries_dir": alignment_outputs.get("warp_dir"),
            "warp_settings_in": alignment_outputs.get("warp_settings"),
            "tomostar_dir_in": alignment_outputs.get("tomostar_dir"),
        }

class TsReconstructParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    # JOB-SPECIFIC PARAMETERS ONLY
    rescale_angpixs: float = Field(default=12.0, ge=2.0, le=50.0)
    halfmap_frames: int = Field(default=1, ge=0, le=1)
    deconv: int = Field(default=1, ge=0, le=1)
    perdevice: int = Field(default=1, ge=0, le=8)
    
    # NO pixel_size
    # Accessed via property

    def is_driver_job(self) -> bool: return True
    def get_tool_name(self) -> str: return "warptools"

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        if not star_path or not star_path.exists(): return None
        try:
            data: Dict[str, Union[pd.DataFrame, dict]] = starfile.read(star_path, always_dict=True)
            joboptions = data.get("joboptions_values")
            if joboptions is None or not isinstance(joboptions, pd.DataFrame): return None
            
            df: pd.DataFrame = joboptions
            param_dict: Dict[str, str] = pd.Series(
                df["rlnJobOptionValue"].values, index=df["rlnJobOptionVariable"].values
            ).to_dict()
            
            return cls(
                rescale_angpixs=float(param_dict.get("param1_value", "12.0")),
                halfmap_frames=int(param_dict.get("param2_value", "1")),
                deconv=int(param_dict.get("param3_value", "1")),
                perdevice=int(param_dict.get("param4_value", "1")),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    # Asset definitions
    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"ctf": "tsCtf"}
    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        return {
            "job_dir": job_dir,
            "output_star": job_dir / "tomograms.star",
            "tilt_series_dir": job_dir / "tilt_series",
            "warp_dir": job_dir / "warp_tiltseries",
            "warp_settings": job_dir / "warp_tiltseries.settings",
            "tomostar_dir": job_dir / "tomostar",
            "reconstruction_dir": job_dir / "warp_tiltseries" / "reconstruction",
        }
    @staticmethod
    def get_input_assets(
        job_dir: Path, project_root: Path, upstream_outputs: Dict[str, Dict[str, Path]]
    ) -> Dict[str, Path]:
        ctf_outputs = upstream_outputs.get("tsCtf", {})
        return {
            "input_star": ctf_outputs.get("output_star"),
            "warp_dir_in": ctf_outputs.get("warp_dir"),
            "warp_settings_in": ctf_outputs.get("warp_settings"),
            "tomostar_dir_in": ctf_outputs.get("tomostar_dir"),
        }

# --- Mapping Function (Still useful) ---

def jobtype_paramclass() -> Dict[JobType, Type[AbstractJobParams]]:
    return {
        JobType.IMPORT_MOVIES: ImportMoviesParams,
        JobType.FS_MOTION_CTF: FsMotionCtfParams,
        JobType.TS_ALIGNMENT: TsAlignmentParams,
        JobType.TS_CTF: TsCtfParams,
        JobType.TS_RECONSTRUCT: TsReconstructParams,
    }

class ProjectState(BaseModel):
    """Complete project state with direct global parameter access"""

    # Metadata
    project_name: str = "Untitled"
    project_path: Optional[Path] = None
    created_at: datetime = Field(default_factory=datetime.now)
    modified_at: datetime = Field(default_factory=datetime.now)

    # Global experimental parameters - THE single source of truth
    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams = Field(default_factory=AcquisitionParams)
    computing: ComputingParams = Field(default_factory=lambda: ComputingParams.from_conf_yaml(Path("config/conf.yaml")))

    # Job configurations - these access global params directly via properties
    jobs: Dict[JobType, AbstractJobParams] = Field(default_factory=dict)

    def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        """Ensure job exists and can access global parameters"""
        if job_type in self.jobs:
            return

        param_class_map = jobtype_paramclass()
        param_class = param_class_map.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_type}")

        # Try template first
        job_params = None
        if template_path and template_path.exists():
            job_params = param_class.from_job_star(template_path)

        if job_params is None:
            # Create new, empty job-specific params
            job_params = param_class()
            print(f"[STATE] Initialized {job_type.value} with defaults")
        else:
            print(f"[STATE] Initialized {job_type.value} from job.star template")

        # Attach project state for global parameter access
        if job_params:
            job_params._project_state = self
            self.jobs[job_type] = job_params
            self.update_modified()

    def update_modified(self):
        """Update the modified timestamp"""
        self.modified_at = datetime.now()

    # Serialization
    def save(self, path: Optional[Path] = None):
        """Save the entire project state to a single JSON file."""
        save_path = path or (
            self.project_path / "project_params.json" if self.project_path else Path("project_params.json")
        )
        save_path.parent.mkdir(parents=True, exist_ok=True)

        # Use Pydantic's model_dump for cleaner serialization
        data = self.model_dump(exclude={"project_path"})

        # Handle non-serializable types manually
        data["project_path"] = str(self.project_path) if self.project_path else None

        # Ensure datetimes are ISO format
        data["created_at"] = self.created_at.isoformat()
        data["modified_at"] = self.modified_at.isoformat()

        # Ensure job keys are strings
        data["jobs"] = {job_type.value: job_params.model_dump() for job_type, job_params in self.jobs.items()}

        with open(save_path, "w") as f:
            json.dump(data, f, indent=2, default=str)  # Add default=str for safety
        print(f"[STATE] Project state saved to {save_path}")

    @classmethod
    def load(cls, path: Path):
        """Load the entire project state from a single JSON file."""
        if not path.exists():
            raise FileNotFoundError(f"Project params file not found: {path}")

        with open(path, "r") as f:
            data = json.load(f)

        # Recreate project state
        project_state = cls(
            project_name=data.get("project_name", "Untitled"),
            project_path=Path(data["project_path"]) if data.get("project_path") else None,
            created_at=datetime.fromisoformat(data.get("created_at", datetime.now().isoformat())),
            modified_at=datetime.fromisoformat(data.get("modified_at", datetime.now().isoformat())),
            microscope=MicroscopeParams(**data.get("microscope", {})),
            acquisition=AcquisitionParams(**data.get("acquisition", {})),
            computing=ComputingParams(**data.get("computing", {})),
        )

        # Re-initialize jobs and attach project state
        param_class_map = jobtype_paramclass()

        for job_type_str, job_data in data.get("jobs", {}).items():
            try:
                job_type = JobType(job_type_str)
                param_class = param_class_map.get(job_type)
                if param_class:
                    job_params = param_class(**job_data)
                    job_params._project_state = project_state  # THE KEY: Re-attach state
                    project_state.jobs[job_type] = job_params
            except ValueError:
                print(f"[WARN] Skipping unknown job type '{job_type_str}' during load.")

        print(f"[STATE] Project state loaded from {path}")
        return project_state


# Global instance logic
_project_state = None


def get_project_state():
    global _project_state
    if _project_state is None:
        _project_state = ProjectState()
    return _project_state


def set_project_state(new_state: ProjectState):
    """Override the global state, e.g., when loading a project."""
    global _project_state
    _project_state = new_state

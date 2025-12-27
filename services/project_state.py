# services/project_state.py
"""
Unified project state - single source of truth for all parameters.
"""

from __future__ import annotations
from enum import Enum
from pathlib import Path
from typing import Dict, Any, Optional, Type, List
from pydantic import BaseModel, Field, ConfigDict, field_validator
from datetime import datetime
import json
import sys
from typing import ClassVar, Tuple, Self, Union, TYPE_CHECKING
import pandas as pd
import starfile
from services.mdoc_service import get_mdoc_service
from services.config_service import get_config_service

if TYPE_CHECKING:
    from services.project_state import ProjectState


class JobStatus(str, Enum):
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    RUNNING = "Running"
    SCHEDULED = "Scheduled"
    UNKNOWN = "Unknown"


class Partition(str, Enum):
    CPU = "c"
    GPU = "g"


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


class JobCategory(str, Enum):
    IMPORT = "Import"
    EXTERNAL = "External"
    MOTIONCORR = "MotionCorr"
    CTFFIND = "CtfFind"


class JobType(str, Enum):
    IMPORT_MOVIES = "importmovies"
    FS_MOTION_CTF = "fsMotionAndCtf"
    TS_ALIGNMENT = "aligntiltsWarp"
    TS_CTF = "tsCtf"
    TS_RECONSTRUCT = "tsReconstruct"
    DENOISE_TRAIN = "denoisetrain"
    DENOISE_PREDICT = "denoisepredict"

    TEMPLATE_MATCH_PYTOM = "templatematching"
    TEMPLATE_EXTRACT_PYTOM = "tmextractcand"
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
    gpu_count: int = Field(default=1, ge=0, le=8)
    memory_gb: int = Field(default=32, ge=4, le=512)
    threads: int = Field(default=8, ge=1, le=128)

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


# --- JOB MODELS ---


class AbstractJobParams(BaseModel):
    """
    Abstract base class for job parameters.
    Contains NO global parameters, only accessors.
    """

    JOB_CATEGORY: ClassVar[JobCategory]

    # Job execution metadata only
    execution_status: JobStatus = Field(default=JobStatus.SCHEDULED)
    relion_job_name: Optional[str] = None
    relion_job_number: Optional[int] = None

    # We store the resolved paths and binds here to persist them in project_params.json
    paths: Dict[str, str] = Field(default_factory=dict)
    additional_binds: List[str] = Field(default_factory=list)

    # This is now a private attribute, not a Pydantic model field.
    _project_state: Optional["ProjectState"] = None

    # --- Global parameter access via properties ---

    @classmethod
    def get_jobstar_field_mapping(cls) -> Dict[str, str]:
        """
        Maps paths dict keys to job.star rlnJobOptionVariable names.
        Override in subclasses that need custom mappings.
        Default handles the common 'in_mic' case.
        """
        return {"input_star": "in_mic"}

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

    @property
    def project_root(self) -> Path:
        if self._project_state is None or self._project_state.project_path is None:
            raise RuntimeError("Project path not set in state")
        return self._project_state.project_path

    @property
    def master_tomostar_dir(self) -> Path:
        return self.project_root / "tomostar"

    @property
    def master_warp_frameseries_settings(self) -> Path:
        return self.project_root / "warp_frameseries.settings"

    @property
    def master_warp_tiltseries_settings(self) -> Path:
        return self.project_root / "warp_tiltseries.settings"

    @property
    def frames_dir(self) -> Path:
        return self.project_root / "frames"

    @property
    def mdoc_dir(self) -> Path:
        return self.project_root / "mdoc"

    # --- LOGIC MIGRATION ---

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        """
        Calculates the exact paths required for this job.
        Replaces the old _resolve_job_paths_standardized in the orchestrator.
        """
        raise NotImplementedError("Subclasses must implement resolve_paths")

    # --- End of Properties ---

    def __setattr__(self, name: str, value: Any) -> None:
        """Enforce immutability for started/completed jobs AND Auto-Save changes"""
        # 1. Bypass logic for private/internal fields
        if name in [
            "execution_status",
            "relion_job_name",
            "relion_job_number",
            "_project_state",
            "paths",
            "additional_binds",
        ]:
            super().__setattr__(name, value)
            return

        if name.startswith("_"):
            super().__setattr__(name, value)
            return

        # 2. Immutability Check
        try:
            current_status = object.__getattribute__(self, "execution_status")
        except AttributeError:
            super().__setattr__(name, value)
            return

        if current_status != JobStatus.SCHEDULED:
            print(f"[IMMUTABLE] Blocked change to '{name}' on {current_status.value} job")
            return

        # 3. Apply Change
        super().__setattr__(name, value)

        # 4. AUTO-SAVE: If attached to state, persist to disk immediately.
        # This fixes the UI desync issue.
        if self._project_state is not None:
            try:
                self._project_state.save()
            except Exception as e:
                print(f"[WARN] Auto-save failed for {name}: {e}")

    @property
    def display_status(self) -> str:
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
        return None  # Default implementation

    def is_driver_job(self) -> bool:
        return False

    def get_tool_name(self) -> str:
        raise NotImplementedError("Subclass must implement get_tool_name()")


class ImportMoviesParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.IMPORT

    # JOB-SPECIFIC PARAMETERS ONLY
    optics_group_name: str = "opticsGroup1"
    do_at_most: int = Field(default=-1)

    def get_tool_name(self) -> str:
        return "relion_import"

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
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
                optics_group_name=job_params.get("optics_group_name", "opticsGroup1"),
                do_at_most=int(job_params.get("do_at_most", -1)),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        return {
            "job_dir"        : job_dir,
            "project_root"   : self.project_root,
            "frames_dir"     : self.frames_dir,
            "mdoc_dir"       : self.mdoc_dir,
            "tilt_series_dir": job_dir / "tilt_series",
            "output_star"    : job_dir / "tilt_series.star",
            "tomostar_dir"   : self.master_tomostar_dir,
        }


class FsMotionCtfParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    m_range_min_max: str = "500:10"
    m_bfac: int = Field(default=-500)
    m_grid: str = "1x1x3"
    c_range_min_max: str = "30:6.0"
    c_defocus_min_max: str = "1.1:8"
    c_grid: str = "2x2x1"
    c_window: int = Field(default=512, ge=128)
    c_use_sum: bool = False
    out_average_halves: bool = True
    out_skip_first: int = 0
    out_skip_last: int = 0
    perdevice: int = Field(default=1, ge=0, le=8)
    do_at_most: int = Field(default=-1)
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
                out_average_halves = True ,
                out_skip_first     = int(param_dict.get("param13_value", "0")) ,
                out_skip_last      = int(param_dict.get("param14_value", "0")) ,
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"import": "importmovies"}

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        if not upstream_job_dir:
            raise ValueError("FsMotionCtf requires an upstream job directory (Import)")

        return {
            "job_dir"                  : job_dir,
            "project_root"             : self.project_root,
            "frames_dir"               : self.frames_dir,
            "mdoc_dir"                 : self.mdoc_dir,
            "warp_frameseries_settings": self.master_warp_frameseries_settings,
            "warp_tiltseries_settings" : self.master_warp_tiltseries_settings,
            "tomostar_dir"             : self.master_tomostar_dir,
            "input_star"               : upstream_job_dir / "tilt_series.star",
            "output_star"              : job_dir / "fs_motion_and_ctf.star",
            "warp_dir"                 : job_dir / "warp_frameseries",
            "input_processing"         : None,
            "output_processing"        : job_dir / "warp_frameseries",
        }


class TsAlignmentParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    alignment_method: AlignmentMethod = AlignmentMethod.ARETOMO
    rescale_angpixs : float           = Field(default=12.0, ge=2.0, le=50.0)
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

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "warptools"

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
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

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        if not upstream_job_dir:
            raise ValueError("TsAlignment requires an upstream job directory (FsMotion)")

        return {
            "job_dir"                  : job_dir,
            "project_root"             : self.project_root,
            "mdoc_dir"                 : self.mdoc_dir,
            "warp_frameseries_settings": self.master_warp_frameseries_settings,
            "warp_tiltseries_settings" : self.master_warp_tiltseries_settings,
            "tomostar_dir"             : self.master_tomostar_dir,
            "input_star"               : upstream_job_dir / "fs_motion_and_ctf.star",
            "output_star"              : job_dir / "aligned_tilt_series.star",
            "warp_dir"                 : job_dir / "warp_tiltseries",
            "input_processing"         : upstream_job_dir / "warp_frameseries",
            "output_processing"        : job_dir / "warp_tiltseries",
        }

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"motion": "fsMotionAndCtf"}


class TsCtfParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    window: int = Field(default=512, ge=128, le=2048)
    range_min_max: str = Field(default="30:6.0")
    defocus_min_max: str = Field(default="0.5:8")
    defocus_hand: str = Field(default="set_flip")
    perdevice: int = Field(default=1, ge=0, le=8)

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
                window=int(param_dict.get("param1_value", "512")),
                range_min_max=param_dict.get("param2_value", "30:4"),
                defocus_min_max=param_dict.get("param3_value", "0.5:8"),
                defocus_hand=param_dict.get("param4_value", "set_flip"),
                perdevice=int(param_dict.get("param5_value", "1")),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"alignment": "aligntiltsWarp"}

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        if not upstream_job_dir:
            raise ValueError("TsCtf requires an upstream job directory (Alignment)")

        return {
            "job_dir": job_dir,
            "project_root": self.project_root,
            "warp_tiltseries_settings": self.master_warp_tiltseries_settings,
            "tomostar_dir": self.master_tomostar_dir,
            "input_star"       : upstream_job_dir / "aligned_tilt_series.star",
            "output_star"      : job_dir / "ts_ctf_tilt_series.star",
            "warp_dir"         : job_dir / "warp_tiltseries",
            "input_processing" : upstream_job_dir / "warp_tiltseries",        
            "output_processing": job_dir / "warp_tiltseries",                  
        }


class TsReconstructParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    rescale_angpixs: float = Field(default=12.0, ge=2.0, le=50.0)
    halfmap_frames : int   = Field(default=1, ge=0, le=1)
    deconv         : int   = Field(default=1, ge=0, le=1)
    perdevice      : int   = Field(default=1, ge=0, le=8)

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "warptools"

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
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
                rescale_angpixs=float(param_dict.get("param1_value", "12.0")),
                halfmap_frames=int(param_dict.get("param2_value", "1")),
                deconv=int(param_dict.get("param3_value", "1")),
                perdevice=int(param_dict.get("param4_value", "1")),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"ctf": "tsCtf"}

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        if not upstream_job_dir:
            raise ValueError("TsReconstruct requires an upstream job directory (Ctf)")

        return {
            "job_dir": job_dir,
            "project_root": self.project_root,
            "warp_tiltseries_settings": self.master_warp_tiltseries_settings,
            "tomostar_dir": self.master_tomostar_dir,
            "input_star"       : upstream_job_dir / "ts_ctf_tilt_series.star",
            "output_star"      : job_dir / "tomograms.star",
            "warp_dir"         : job_dir / "warp_tiltseries",
            "input_processing" : upstream_job_dir / "warp_tiltseries",          
            "output_processing": job_dir / "warp_tiltseries",                    
        }


class DenoiseTrainParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    tomograms_for_training    : str = Field(default="Position_1")
    number_training_subvolumes: int = Field(default=600, ge=100)
    subvolume_dimensions      : int = Field(default=64, ge=32)
    perdevice                 : int = Field(default=1)

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "cryocare"  

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
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
                tomograms_for_training=param_dict.get("tomograms_for_training", "Position_1"),
                number_training_subvolumes=int(param_dict.get("number_training_subvolumes", "600")),
                subvolume_dimensions=int(param_dict.get("subvolume_dimensions", "64")),
                perdevice=int(param_dict.get("min_dedicated", "1")),
            )
        except Exception:
            return None

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"reconstruct": "tsReconstruct"}

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:

        if not upstream_job_dir:
            print("[WARN] DenoiseTrain resolved with no upstream job! Defaulting to project root (likely to fail).")
            fallback_star = self.project_root / "tomograms.star"
        else:
            fallback_star = upstream_job_dir / "tomograms.star"

        return {
            "job_dir"     : job_dir,
            "project_root": self.project_root,
            "input_star"  : fallback_star,
            "output_model": job_dir / "denoising_model.tar.gz",
        }



class DenoisePredictParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    ntiles_x: int = Field(default=2, ge=1)
    ntiles_y: int = Field(default=2, ge=1)
    ntiles_z: int = Field(default=2, ge=1)
    denoising_tomo_name: str = "" 
    perdevice: int = Field(default=1)

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "cryocare" 

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
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
                ntiles_x=int(param_dict.get("ntiles_x", "2")),
                ntiles_y=int(param_dict.get("ntiles_y", "2")),
                ntiles_z=int(param_dict.get("ntiles_z", "2")),
                denoising_tomo_name=param_dict.get("denoising_tomo_name", ""),
                perdevice=int(param_dict.get("min_dedicated", "1")),
            )
        except Exception:
            return None

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"train": "denoisetrain"}

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
            """
            Kitchen sink resolution:
            1. Grabs the stored path dictionary from the historical TsReconstruct job.
            2. Grabs the stored path dictionary from the DenoiseTrain job.
            3. Packages them for the driver.
            """
            if not self._project_state:
                raise RuntimeError("DenoisePredict detached from ProjectState")

            # --- 1. GET DATA (From TsReconstruct) ---
            # We assume the user has a successful reconstruction job in the project
            ts_job = self._project_state.jobs.get(JobType.TS_RECONSTRUCT)
            
            if not ts_job or ts_job.execution_status != JobStatus.SUCCEEDED:
                raise ValueError("DenoisePredict requires a successfully completed TsReconstruct job.")
            
            # Access the persisted paths from that job
            ts_paths = ts_job.paths 
            if "output_star" not in ts_paths or "warp_dir" not in ts_paths:
                raise ValueError("TsReconstruct state is missing output paths. Did it run correctly?")

            # --- 2. GET MODEL (From DenoiseTrain) ---
            # If we are in a pipeline, upstream_job_dir might be the train job.
            # Otherwise, we look at the last successful train job in the state.
            model_path = None
            
            if upstream_job_dir:
                model_path = upstream_job_dir / "denoising_model.tar.gz"
            else:
                train_job = self._project_state.jobs.get(JobType.DENOISE_TRAIN)
                if train_job and train_job.execution_status == JobStatus.SUCCEEDED:
                    # Use the path explicitly saved by the train job
                    model_path = Path(train_job.paths.get("output_model"))
            
            if not model_path or not model_path.exists():
                raise FileNotFoundError("Denoising model not found.")

            # --- 3. PACKAGE FOR DRIVER ---
            return {
                "job_dir": job_dir,
                "project_root": self.project_root,
                
                # Pass the precise absolute paths to the driver
                "model_path": model_path,
                "input_star": Path(ts_paths["output_star"]),
                "reconstruct_base": Path(ts_paths["warp_dir"]), # The specific Warp dir containing even/odd
                "output_dir": job_dir / "denoised",
            }


class TemplateMatchPytomParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    # Inputs (Strings here, resolved to Paths in resolve_paths)
    template_path: str = Field(default="")
    mask_path: str = Field(default="")

    # Algorithm Params
    angular_search: str = Field(default="12.0")
    symmetry: str = Field(default="C1")
    
    # Flags
    defocus_weight         : bool = True
    dose_weight            : bool = True
    spectral_whitening     : bool = True
    random_phase_correction: bool = False
    non_spherical_mask     : bool = False
    
    bandpass_filter: str = Field(default="None")  # Format "low:high"
    gpu_split      : str = Field(default="auto")  # "auto" or "4:4:2"
    perdevice      : int = Field(default=1)

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "pytom"

    @classmethod
    def get_jobstar_field_mapping(cls) -> Dict[str, str]:
        return {
            "input_tomograms": "in_mic",
            "template_path": "in_3dref",
            "mask_path": "in_mask",
        }

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        if not star_path or not star_path.exists():
            return None
        try:
            data = starfile.read(star_path, always_dict=True)
            df = data.get("joboptions_values")
            if df is None or not isinstance(df, pd.DataFrame): return None
            
            param_dict = pd.Series(
                df["rlnJobOptionValue"].values, index=df["rlnJobOptionVariable"].values
            ).to_dict()

            return cls(
                template_path=param_dict.get("in_3dref", ""),
                mask_path=param_dict.get("in_mask", ""),
                angular_search=param_dict.get("angular_search", "12.0"),
                symmetry=param_dict.get("symmetry", "C1"),
                # Parse booleans
                defocus_weight=param_dict.get("ctf_weight", "True") == "True",
                dose_weight=param_dict.get("dose_weight", "True") == "True",
                spectral_whitening=param_dict.get("spectral_whitening", "True") == "True",
                non_spherical_mask=param_dict.get("non_spherical_mask", "False") == "True",
                bandpass_filter=param_dict.get("bandpass_filter", "None"),
                gpu_split=param_dict.get("split", "auto"),
            )
        except Exception:
            return None

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        # We prefer denoised tomograms, but reconstruct works too
        return {"tomograms": "denoisepredict"}

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        """
        Complex resolution:
        1. Tomograms come from 'upstream_job_dir' (usually Denoise or Reconstruct).
        2. Tilt Angles/Metadata come from a historical TS_CTF job in the project state.
        """
        if not self._project_state:
            raise RuntimeError("TemplateMatchPytomParams detached from ProjectState")

        # 1. Resolve Tomograms (Primary Upstream)
        if upstream_job_dir:
            input_tomograms = upstream_job_dir / "tomograms.star"
        else:
            # Fallback to TS Reconstruct if no upstream provided
            rec_job = self._project_state.jobs.get(JobType.TS_RECONSTRUCT)
            if rec_job and rec_job.execution_status == JobStatus.SUCCEEDED:
                 input_tomograms = Path(rec_job.paths.get("output_star"))
            else:
                 raise ValueError("No input tomograms found for Template Matching")

        # 2. Resolve Tilt Series (For Angles/Defocus/Dose)
        # We assume TS_CTF has run. 
        ctf_job = self._project_state.jobs.get(JobType.TS_CTF)
        if not ctf_job or ctf_job.execution_status != JobStatus.SUCCEEDED:
             raise ValueError("Template Matching requires a completed CtfFind job (tsCtf) to generate angle files.")
        
        input_tiltseries = Path(ctf_job.paths.get("output_star"))

        return {
            "job_dir": job_dir,
            "project_root": self.project_root,
            # Inputs
            "input_tomograms": input_tomograms,
            "input_tiltseries": input_tiltseries,
            "template_path": Path(self.template_path) if self.template_path else None,
            "mask_path": Path(self.mask_path) if self.mask_path else None,
            # Outputs
            "output_dir": job_dir / "tmResults"
        }


class CandidateExtractPytomParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    # Particle Params
    particle_diameter_ang: float = Field(default=150.0)
    max_num_particles: int = Field(default=1000)
    
    # Thresholding
    cutoff_method: str = Field(default="NumberOfFalsePositives") # "NumberOfFalsePositives" or "ManualCutOff"
    cutoff_value: float = Field(default=1.0)
    
    # Score Map
    apix_score_map: str = Field(default="auto") 
    
    # Filtering
    score_filter_method: str = Field(default="None")
    score_filter_value: str = Field(default="None")

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "pytom"

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        if not star_path or not star_path.exists():
            return None
        try:
            data = starfile.read(star_path, always_dict=True)
            df = data.get("joboptions_values")
            if df is None or not isinstance(df, pd.DataFrame): return None
            
            param_dict = pd.Series(
                df["rlnJobOptionValue"].values, index=df["rlnJobOptionVariable"].values
            ).to_dict()

            return cls(
                particle_diameter_ang=float(param_dict.get("particle_diameter", "150.0")),
                max_num_particles=int(param_dict.get("max_num_particles", "1000")),
                cutoff_method=param_dict.get("cutoff_method", "NumberOfFalsePositives"),
                cutoff_value=float(param_dict.get("cutoff_value", "1.0")),
                apix_score_map=param_dict.get("apix_score_map", "auto")
            )
        except Exception:
            return None

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"tm_job": "tmMatchPytom"}

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        if not upstream_job_dir:
             raise ValueError("Extraction requires an upstream Template Matching job")

        return {
            "job_dir": job_dir,
            "project_root": self.project_root,
            "input_tm_job": upstream_job_dir,
            "input_tomograms": upstream_job_dir.parent / "tomograms.star", # Inferring original tomo list location
            "output_star": job_dir / "candidates.star",
            "optimisation_set": job_dir / "optimisation_set.star"
        }

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
    }

class ProjectState(BaseModel):
    """Complete project state with direct global parameter access"""

    project_name    : str            = "Untitled"
    project_path    : Optional[Path] = None
    created_at      : datetime       = Field(default_factory=datetime.now)
    modified_at     : datetime       = Field(default_factory=datetime.now)
    job_path_mapping: Dict[str, str] = Field(default_factory=dict)          # job_type -> relion_path

    movies_glob: str = ""
    mdocs_glob: str = ""

    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams = Field(default_factory=AcquisitionParams)
    computing: ComputingParams = Field(default_factory=lambda: ComputingParams.from_conf_yaml(Path("config/conf.yaml")))

    jobs: Dict[JobType, AbstractJobParams] = Field(default_factory=dict)

    def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        if job_type in self.jobs:
            return

        param_class_map = jobtype_paramclass()
        param_class = param_class_map.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_type}")

        job_params = None
        if template_path and template_path.exists():
            job_params = param_class.from_job_star(template_path)

        if job_params is None:
            job_params = param_class()
            print(f"[STATE] Initialized {job_type.value} with defaults")
        else:
            print(f"[STATE] Initialized {job_type.value} from job.star template")

        if job_params:
            job_params._project_state = self
            self.jobs[job_type] = job_params
            self.update_modified()

    def update_modified(self):
        self.modified_at = datetime.now()

    def save(self, path: Optional[Path] = None):
        """Save the entire project state to a single JSON file."""
        save_path = path or (
            self.project_path / "project_params.json" if self.project_path else Path("project_params.json")
        )
        save_path.parent.mkdir(parents=True, exist_ok=True)

        data = self.model_dump(exclude={"project_path"})
        data["project_path"] = str(self.project_path) if self.project_path else None
        data["created_at"] = self.created_at.isoformat()
        data["modified_at"] = self.modified_at.isoformat()
        data["jobs"] = {job_type.value: job_params.model_dump() for job_type, job_params in self.jobs.items()}

        with open(save_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        print(f"[STATE] Project state saved to {save_path}")

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
            microscope=MicroscopeParams(**data.get("microscope", {})),
            acquisition=AcquisitionParams(**data.get("acquisition", {})),
            computing=ComputingParams(**data.get("computing", {})),
        )

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
                print(f"[WARN] Skipping unknown job type '{job_type_str}' during load.")

        print(f"[STATE] Project state loaded from {path}")
        return project_state


def get_project_state():
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


_project_state = None


class StateService:
    """Just handles persistence and mdoc updates"""

    def __init__(self):
        self._project_state = get_project_state()

    @property
    def state(self) -> ProjectState:
        # ALWAYS fetch the current global, do not cache inside __init__
        return get_project_state()

    async def update_from_mdoc(self, mdocs_glob: str):
        mdoc_service = get_mdoc_service()
        mdoc_data = mdoc_service.get_autodetect_params(mdocs_glob)

        if not mdoc_data:
            print(f"[WARN] No mdoc data found or parsed from: {mdocs_glob}")
            return

        print(f"[STATE] Updating from mdoc: {mdoc_data}")
        state = self.state
        try:
            if "pixel_spacing" in mdoc_data:
                state.microscope.pixel_size_angstrom = mdoc_data["pixel_spacing"]
            if "voltage" in mdoc_data:
                state.microscope.acceleration_voltage_kv = mdoc_data["voltage"]

            if "dose_per_tilt" in mdoc_data:
                state.acquisition.dose_per_tilt = mdoc_data["dose_per_tilt"]
            if "frame_dose" in mdoc_data:
                state.acquisition.frame_dose = mdoc_data["frame_dose"]
            if "tilt_axis_angle" in mdoc_data:
                state.acquisition.tilt_axis_degrees = mdoc_data["tilt_axis_angle"]
            if "acquisition_software" in mdoc_data:
                state.acquisition.acquisition_software = mdoc_data["acquisition_software"]
            if "invert_tilt_angles" in mdoc_data:
                state.acquisition.invert_tilt_angles = mdoc_data["invert_tilt_angles"]
            if "detector_dimensions" in mdoc_data:
                state.acquisition.detector_dimensions = mdoc_data["detector_dimensions"]
            if "eer_fractions_per_frame" in mdoc_data:
                state.acquisition.eer_fractions_per_frame = mdoc_data["eer_fractions_per_frame"]
            if "nominal_magnification" in mdoc_data:
                state.acquisition.nominal_magnification = mdoc_data["nominal_magnification"]
            if "spot_size" in mdoc_data:
                state.acquisition.spot_size = mdoc_data["spot_size"]
            if "binning" in mdoc_data:
                state.acquisition.binning = mdoc_data["binning"]

            state.update_modified()
            print("[STATE] Global parameters updated from mdoc.")

        except Exception as e:
            print(f"[ERROR] Failed to update state from mdoc data: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc()

    async def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        self.state.ensure_job_initialized(job_type, template_path)

    async def load_project(self, project_json_path: Path):
        try:
            new_state = ProjectState.load(project_json_path)
            set_project_state(new_state)
            return True
        except Exception as e:
            print(f"[ERROR] Failed to load project state from {project_json_path}: {e}", file=sys.stderr)
            return False

    async def save_project(self, save_path: Optional[Path] = None):
        state = self.state
        # Determine target path safely
        if save_path:
            target_path = save_path
        elif state.project_path:
            target_path = state.project_path / "project_params.json"
        else:
            # No path to save to - expected during autodetect before project creation
            print("[STATE] Skipping save - no project path set yet")
            return

        state.save(target_path)

    async def export_for_project(
        self, movies_glob: str, mdocs_glob: str, selected_jobs_str: List[str]
    ) -> Dict[str, Any]:
        print("[STATE] Exporting comprehensive project config")

        mdoc_service   = get_mdoc_service()
        config_service = get_config_service()
        state          = self.state

        mdoc_stats = mdoc_service.parse_all_mdoc_files(mdocs_glob)

        template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        for job_str in selected_jobs_str:
            try:
                job_type = JobType(job_str)
                job_star_path = template_base / job_type.value / "job.star"
                state.ensure_job_initialized(job_type, job_star_path if job_star_path.exists() else None)
            except ValueError:
                print(f"[WARN] Skipping unknown job '{job_str}' during export.")

        containers = config_service.containers
        export = {
            "metadata": {
                "config_version": "3.0",
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
            "containers" : containers,
            "microscope" : state.microscope.model_dump(),
            "acquisition": state.acquisition.model_dump(),
            "computing"  : state.computing.model_dump(),
            "jobs"       : {
                job_str: state.jobs[JobType(job_str)].model_dump()
                for job_str in selected_jobs_str
                if JobType(job_str) in state.jobs
            },
        }

        return export


_state_service_instance = None


def get_state_service():
    global _state_service_instance
    if _state_service_instance is None:
        _state_service_instance = StateService()
    return _state_service_instance

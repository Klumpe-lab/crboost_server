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
    IMPORT     = "Import"
    EXTERNAL   = "External"
    MOTIONCORR = "MotionCorr"
    CTFFIND    = "CtfFind"


class JobType(str, Enum):
    IMPORT_MOVIES       = "importmovies"
    FS_MOTION_CTF       = "fsMotionAndCtf"
    TS_ALIGNMENT        = "aligntiltsWarp"
    TS_CTF              = "tsCtf"
    TS_RECONSTRUCT      = "tsReconstruct"
    DENOISE_TRAIN       = "denoisetrain"
    DENOISE_PREDICT     = "denoisepredict"
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

    dose_per_tilt: float = Field(default=3.0, ge=0.1, le=9.0)
    detector_dimensions: Tuple[int, int] = (4096, 4096)
    tilt_axis_degrees: float = Field(default=-95.0, ge=-180.0, le=180.0)
    eer_fractions_per_frame: Optional[int] = Field(default=None, ge=1, le=100)
    sample_thickness_nm: float = Field(default=300.0, ge=50.0, le=2000.0)
    gain_reference_path: Optional[str] = None
    invert_tilt_angles: bool = False
    invert_defocus_hand: bool = False
    acquisition_software: str = Field(default="SerialEM")
    nominal_magnification: Optional[int] = None
    spot_size: Optional[int] = None
    camera_name: Optional[str] = None
    binning: Optional[int] = Field(default=1, ge=1)
    frame_dose: Optional[float] = None


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


# --- REFACTORED JOB MODELS ---


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
                # We call save() on the project state.
                # This is safe because _project_state is the global singleton.
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
        # Import has no upstream job in the pipeline sense
        return {
            "job_dir": job_dir,
            "project_root": self.project_root,
            "frames_dir": self.frames_dir,
            "mdoc_dir": self.mdoc_dir,
            "tilt_series_dir": job_dir / "tilt_series",
            "output_star": job_dir / "tilt_series.star",
            "tomostar_dir": self.master_tomostar_dir,  # Use master tomostar
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
                gain_operations=param_dict.get("param3_value"),
                m_range_min_max=param_dict.get("param4_value", "500:10"),
                m_bfac=int(param_dict.get("param5_value", "-500")),
                m_grid=param_dict.get("param6_value", "1x1x3"),
                c_range_min_max=param_dict.get("param7_value", "30:6.0"),
                c_defocus_min_max=param_dict.get("param8_value", "1.1:8"),
                c_grid=param_dict.get("param9_value", "2x2x1"),
                perdevice=int(param_dict.get("param10_value", "1")),
                c_window=512,
                c_use_sum=param_dict.get("param11_value", "False").lower() == "true",
                out_average_halves=True,
                out_skip_first=int(param_dict.get("param13_value", "0")),
                out_skip_last=int(param_dict.get("param14_value", "0")),
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
            "job_dir": job_dir,
            "project_root": self.project_root,
            "frames_dir": self.frames_dir,
            "mdoc_dir": self.mdoc_dir,
            "warp_frameseries_settings": self.master_warp_frameseries_settings,
            "warp_tiltseries_settings": self.master_warp_tiltseries_settings,
            "tomostar_dir": self.master_tomostar_dir,
            # Logic from Orchestrator:
            "input_star": upstream_job_dir / "tilt_series.star",
            "output_star": job_dir / "fs_motion_and_ctf.star",
            "warp_dir": job_dir / "warp_frameseries",
            "input_processing": None,
            "output_processing": job_dir / "warp_frameseries",
        }


class TsAlignmentParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    alignment_method: AlignmentMethod = AlignmentMethod.ARETOMO
    rescale_angpixs: float = Field(default=12.0, ge=2.0, le=50.0)
    tomo_dimensions: str = Field(default="4096x4096x2048")
    do_at_most: int = Field(default=-1)
    perdevice: int = Field(default=1, ge=0, le=8)
    mdoc_pattern: str = Field(default="*.mdoc")
    gain_operations: Optional[str] = None
    patch_x: int = Field(default=2, ge=0)
    patch_y: int = Field(default=2, ge=0)
    axis_iter: int = Field(default=1, ge=0)
    axis_batch: int = Field(default=5, ge=1)
    imod_patch_size: int = Field(default=200)
    imod_overlap: int = Field(default=50)

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
            "job_dir": job_dir,
            "project_root": self.project_root,
            "mdoc_dir": self.mdoc_dir,
            "warp_frameseries_settings": self.master_warp_frameseries_settings,
            "warp_tiltseries_settings": self.master_warp_tiltseries_settings,
            "tomostar_dir": self.master_tomostar_dir,
            "input_star": upstream_job_dir / "fs_motion_and_ctf.star",
            "output_star": job_dir / "aligned_tilt_series.star",
            "warp_dir": job_dir / "warp_tiltseries",
            "input_processing": upstream_job_dir / "warp_frameseries",  # Read from FsMotion
            "output_processing": job_dir / "warp_tiltseries",  # Write to current
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
            # Logic from Orchestrator:
            "input_star": upstream_job_dir / "aligned_tilt_series.star",
            "output_star": job_dir / "ts_ctf_tilt_series.star",
            "warp_dir": job_dir / "warp_tiltseries",
            "input_processing": upstream_job_dir / "warp_tiltseries",  # Read from Alignment
            "output_processing": job_dir / "warp_tiltseries",  # Write to current
        }


class TsReconstructParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    rescale_angpixs: float = Field(default=12.0, ge=2.0, le=50.0)
    halfmap_frames: int = Field(default=1, ge=0, le=1)
    deconv: int = Field(default=1, ge=0, le=1)
    perdevice: int = Field(default=1, ge=0, le=8)

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
            # Logic from Orchestrator:
            "input_star": upstream_job_dir / "ts_ctf_tilt_series.star",
            "output_star": job_dir / "tomograms.star",
            "warp_dir": job_dir / "warp_tiltseries",
            "input_processing": upstream_job_dir / "warp_tiltseries",  # Read from CTF
            "output_processing": job_dir / "warp_tiltseries",  # Write to current
        }


class DenoiseTrainParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    tomograms_for_training: str = Field(default="Position_1")
    number_training_subvolumes: int = Field(default=600, ge=100)
    subvolume_dimensions: int = Field(default=64, ge=32)
    perdevice: int = Field(default=1)

    def is_driver_job(self) -> bool:
        return True


    def get_tool_name(self) -> str:
        return "cryocare"  # Changed from "cryocare_train"

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

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        # Upstream is typically reconstruction (providing tomograms)
        if not upstream_job_dir:
            # Fallback for when there's no clear linear upstream (rare)
            # Try to guess reconstruction folder
            reconstruction_dir = job_dir.parent / "tsReconstruct"
            if not reconstruction_dir.exists():
                # Try finding any previous job that outputted tomograms.star
                 pass # Logic handled by orchestrator usually passing valid upstream
        
        return {
            "job_dir": job_dir,
            "project_root": self.project_root,
            "input_star": upstream_job_dir / "tomograms.star" if upstream_job_dir else self.project_root / "tomograms.star",
            "output_model": job_dir / "denoising_model.tar.gz",
        }


class DenoisePredictParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL

    ntiles_x: int = Field(default=2, ge=1)
    ntiles_y: int = Field(default=2, ge=1)
    ntiles_z: int = Field(default=2, ge=1)
    denoising_tomo_name: str = "" # Filter, empty means all
    perdevice: int = Field(default=1)

    def is_driver_job(self) -> bool:
        return True


    def get_tool_name(self) -> str:
        return "cryocare"  # Changed from "cryocare_predict"

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

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        # Denoise Predict has TWO dependencies:
        # 1. The Model (from upstream Denoise Train)
        # 2. The Tomograms (from Reconstruction - which is upstream of Train)
        
        # Assumption: upstream_job_dir is the Denoise Train job directory
        model_path = upstream_job_dir / "denoising_model.tar.gz"
        
        # We need to find the tomograms.star. Usually in 'tsReconstruct' folder at same level,
        # or we can inspect the Train job input (if we had access to full state here, but we don't fully).
        # Safe heuristic: Check if there is a 'tomograms.star' in the project root or standard location,
        # OR assume the user linked it correctly in the previous steps.
        
        # Better heuristic used in Orchestrator: The 'input_star' passed to Train is likely what we want.
        # But here we only see 'upstream_job_dir'.
        # We will assume a standard directory structure: project/External/jobXXX
        # We can try to look for tomograms.star in typical reconstruction output folders.
        
        # However, specifically for `denoisepredict`, the Orchestrator (old system)
        # sets `in_tomoset` pointing to `tsReconstruct/tomograms.star`.
        
        # For now, we will try to resolve it relative to project root if possible, or look back 2 jobs.
        # Since we can't look back 2 jobs here easily, we will look for 'tomograms.star' in the
        # expected reconstruction folder location.
        
        reconstruct_guess = job_dir.parent / "tsReconstruct" / "tomograms.star"
        # If we are using job numbers (job005), this guess fails.
        # Let's rely on finding it in the project scope or assume the driver can verify it.
        # We will default to a path compatible with the standard pipeline.
        
        # NOTE: In the `_build_job_command` inside orchestrator, we can actually pass specific paths.
        # But here we define what keys are available.
        
        return {
            "job_dir": job_dir,
            "project_root": self.project_root,
            "model_path": model_path,
            # This is a bit weak, relies on standard naming or user ensuring it exists:
            "input_star": self.project_root / "External" / "tsReconstruct" / "tomograms.star", 
             "output_dir": job_dir / "denoised"
        }


def jobtype_paramclass() -> Dict[JobType, Type[AbstractJobParams]]:
    return {
        JobType.IMPORT_MOVIES: ImportMoviesParams,
        JobType.FS_MOTION_CTF: FsMotionCtfParams,
        JobType.TS_ALIGNMENT: TsAlignmentParams,
        JobType.TS_CTF: TsCtfParams,
        JobType.TS_RECONSTRUCT: TsReconstructParams,
        JobType.DENOISE_TRAIN: DenoiseTrainParams,
        JobType.DENOISE_PREDICT: DenoisePredictParams,
    }


class ProjectState(BaseModel):
    """Complete project state with direct global parameter access"""

    project_name: str = "Untitled"
    project_path: Optional[Path] = None
    created_at: datetime = Field(default_factory=datetime.now)
    modified_at: datetime = Field(default_factory=datetime.now)


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


# --- NEW: Function to reset global state (Fixes the ghost project issue) ---
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

        mdoc_service = get_mdoc_service()
        config_service = get_config_service()
        state = self.state

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
            "containers": containers,
            "microscope": state.microscope.model_dump(),
            "acquisition": state.acquisition.model_dump(),
            "computing": state.computing.model_dump(),
            "jobs": {
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

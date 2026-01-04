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
from services.config_service import get_config_service, SlurmDefaultsConfig

from typing import ClassVar, Dict, Any, Optional, Type, List

if TYPE_CHECKING:
    from services.project_state import ProjectState

class SlurmPreset(str, Enum):
    CUSTOM = "Custom"
    SMALL  = "1gpu:16GB"
    MEDIUM = "2gpu:32GB"
    LARGE  = "4gpu:64gb"

# Descriptive mapping for UI pills and snapping values
SLURM_PRESET_MAP = {
    SlurmPreset.SMALL: {
        "label": "1 GPU · 16GB · 30m",
        "values": {
            "gres": "gpu:1",
            "mem": "16G",
            "cpus_per_task": 2,
            "time": "0:30:00",
            "nodes": 1
        }
    },
    SlurmPreset.MEDIUM: {
        "label": "2 GPUs · 32GB · 2h",
        "values": {
            "gres": "gpu:2",
            "mem": "32G",
            "cpus_per_task": 4,
            "time": "2:00:00",
            "nodes":2
        }
    },
    SlurmPreset.LARGE: {
        "label": "4 GPUs · 64GB · 4h",
        "values": {
            "gres": "gpu:4",
            "mem": "64G",
            "cpus_per_task": 8,
            "time": "4:00:00",
            "nodes":4
        }
    }
}

class SlurmConfig(BaseModel):
    """SLURM submission parameters for a job"""
    model_config = ConfigDict(validate_assignment=True)

    preset: SlurmPreset = Field(default=SlurmPreset.CUSTOM)
    partition: str = "g"
    constraint: str = "g2|g3|g4"
    nodes: int = Field(default=1, ge=1)
    ntasks_per_node: int = Field(default=1, ge=1)
    cpus_per_task: int = Field(default=4, ge=1)
    gres: str = "gpu:4"
    mem: str = "64G"
    time: str = "3:30:00"

    # Standard Relion Tomography aliases for XXXextra1XXX through XXXextra8XXX
    QSUB_EXTRA_MAPPING: ClassVar[Dict[str, str]] = {
        "partition": "qsub_extra1",
        "constraint": "qsub_extra2",
        "nodes": "qsub_extra3",
        "ntasks_per_node": "qsub_extra4",
        "cpus_per_task": "qsub_extra5",
        "gres": "qsub_extra6",
        "mem": "qsub_extra7",
        "time": "qsub_extra8",
    }
    def to_qsub_extra_dict(self) -> Dict[str, str]:
        return {
            self.QSUB_EXTRA_MAPPING[field]: str(getattr(self, field))
            for field in self.QSUB_EXTRA_MAPPING
        }

    @classmethod
    def from_config_defaults(cls) -> "SlurmConfig":
        try:
            config_service = get_config_service()
            defaults = config_service.slurm_defaults
            return cls(**defaults.model_dump())
        except Exception:
            return cls()

class JobStatus(str, Enum):
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    RUNNING = "Running"
    SCHEDULED = "Scheduled"
    UNKNOWN = "Unknown"




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
    SUBTOMO_EXTRACTION     = "subtomoExtraction"  


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




# --- JOB MODELS ---


class AbstractJobParams(BaseModel):
    """
    Abstract base class for job parameters.
    Contains NO global parameters, only accessors.
    """

    JOB_CATEGORY: ClassVar[JobCategory]
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"  # Override for native jobs
    IS_TOMO_JOB: ClassVar[bool] = True
    IS_CONTINUE: ClassVar[bool] = False

    # Job execution metadata only
    execution_status: JobStatus = Field(default=JobStatus.SCHEDULED)
    relion_job_name: Optional[str] = None
    relion_job_number: Optional[int] = None

    is_orphaned: bool = Field(default=False)
    missing_inputs: List[str] = Field(default_factory=list)

    # We store the resolved paths and binds here to persist them in project_params.json
    paths: Dict[str, str] = Field(default_factory=dict)
    additional_binds: List[str] = Field(default_factory=list)
    
    slurm_overrides: Dict[str, Any] = Field(default_factory=dict)


    # This is now a private attribute, not a Pydantic model field.
    _project_state: Optional["ProjectState"] = None

    def get_effective_slurm_config(self) -> SlurmConfig:
        """
        Returns the effective SLURM config for this job.
        Merges project defaults with any per-job overrides.
        """
        if self._project_state is not None:
            defaults = self._project_state.slurm_defaults
        else:
            defaults = SlurmConfig.from_config_defaults()
        
        if not self.slurm_overrides:
            return defaults
        
        merged_data = defaults.model_dump()
        merged_data.update(self.slurm_overrides)
        return SlurmConfig(**merged_data)
    
    def apply_slurm_preset(self, preset: SlurmPreset) -> None:
        """Flatten preset values into the overrides dictionary."""
        if preset == SlurmPreset.CUSTOM:
            self.slurm_overrides["preset"] = SlurmPreset.CUSTOM.value
            return

        preset_data = SLURM_PRESET_MAP.get(preset)
        if not preset_data:
            return

        # 1. Clear specific resource overrides to ensure a clean snap
        for key in ["gres", "mem", "cpus_per_task", "time"]:
            self.slurm_overrides.pop(key, None)

        # 2. Inject ONLY the resource values, not the metadata
        self.slurm_overrides.update(preset_data["values"])
        
        # 3. Store the preset enum value
        self.slurm_overrides["preset"] = preset.value
        
        # Manually trigger save since we modified a dict in-place
        if self._project_state:
            self._project_state.save()

    def set_slurm_override(self, field: str, value: Any) -> None:
        """Update a single field and ensure we flip to Custom."""
        self.slurm_overrides[field] = value
        # If we change a resource, we are no longer strictly on a preset
        if field != "preset":
            self.slurm_overrides["preset"] = SlurmPreset.CUSTOM.value
        
        if self._project_state:
            self._project_state.save()
    
    def clear_slurm_overrides(self) -> None:
        """Clear all per-job SLURM overrides, reverting to project defaults"""
        self.slurm_overrides = {}

    def generate_job_star(
        self, 
        job_dir: Path, 
        fn_exe: str,
        star_handler, 
    ) -> None:
        """Generate job.star entirely from this model's state."""
        
        # 1. Job metadata block
        job_data = {
            "rlnJobTypeLabel": self.RELION_JOB_TYPE,
            "rlnJobIsContinue": 1 if self.IS_CONTINUE else 0,
            "rlnJobIsTomo": 1 if self.IS_TOMO_JOB else 0,
        }
        
        # 2. Build options list
        options: List[Tuple[str, str]] = []
        
        if self.RELION_JOB_TYPE == "relion.external":
            options.append(("fn_exe", fn_exe))
        
        # Add job-specific options (in_mic, in_tomoset, etc.)
        options.extend(self._get_job_specific_options())
        
        # CRITICAL: Add actual path values for validation
        # This helps relion_schemer understand dependencies
        for key, path_value in self.paths.items():
            if key in ["input_star", "model_path", "input_tomoset"]:
                # Convert to relative path for RELION
                try:
                    rel_path = Path(path_value).relative_to(self.project_root)
                    options.append((key, f"./{rel_path}"))
                except ValueError:
                    options.append((key, str(path_value)))
        
        if self.RELION_JOB_TYPE == "relion.external":
            options.append(("other_args", ""))
        
        # Add Queue/Slurm options
        options.extend(self._get_queue_options())
        
        # 3. Create DataFrame and Write
        joboptions_df = pd.DataFrame(options, columns=["rlnJobOptionVariable", "rlnJobOptionValue"])
        
        data = {
            "job": job_data,
            "joboptions_values": joboptions_df,
        }
        
        job_dir.mkdir(parents=True, exist_ok=True)
        star_path = job_dir / "job.star"
        star_handler.write(data, star_path)
        
        # 4. Mandatory RELION version header
        content = star_path.read_text()
        with open(star_path, 'w') as f:
            f.write("# version 50001\n\n")
            f.write(content)
        
        print(f"[JOBSTAR] Generated {star_path}")

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        """
        Override in subclasses to provide job-specific joboptions.
        Default: single input as in_mic.
        """
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def _get_queue_options(self) -> List[Tuple[str, str]]:
        """Generate SLURM/queue options using paramN_value slots."""
        slurm = self.get_effective_slurm_config()
        
        # Basic Queue setup
        options = [
            ("do_queue", "Yes"),
            ("queuename", slurm.partition),
            ("qsub", "sbatch"),
            ("qsubscript", "qsub/qsub.sh"),
            ("min_dedicated", "1"),
        ]
        
        # Add ONLY the qsub_extra values - NO labels needed!
        slurm_dict = slurm.to_qsub_extra_dict()
        for var_name, value in slurm_dict.items():
            options.append((var_name, value))
        
        return options

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
            "is_orphaned",      
            "missing_inputs",  
            "slurm_overrides",
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
    RELION_JOB_TYPE: ClassVar[str] = "relion.importtomo"  # Native RELION job!
    IS_CONTINUE: ClassVar[bool] = True  # From your template

    # Job-specific parameters
    optics_group_name: str = "opticsGroup1"
    do_at_most: int = Field(default=-1)

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
            """Import uses relative paths - RELION runs from project root."""
            # Detect frame extension from the actual files
            frames_dir = self.frames_dir
            if frames_dir.exists():
                eer_files = list(frames_dir.glob("*.eer"))
                mrc_files = list(frames_dir.glob("*.mrc"))
                tiff_files = list(frames_dir.glob("*.tiff")) + list(frames_dir.glob("*.tif"))
                
                if eer_files:
                    frame_ext = "*.eer"
                elif mrc_files:
                    frame_ext = "*.mrc"
                elif tiff_files:
                    frame_ext = "*.tiff"
                else:
                    frame_ext = "*.eer"  # Default fallback
            else:
                frame_ext = "*.eer"
            
            frames_pattern = f"./frames/{frame_ext}"
            mdoc_pattern = "./mdoc/*.mdoc"
            
            return [
                ("movie_files", frames_pattern),
                ("images_are_motion_corrected", "No"),
                ("mdoc_files", mdoc_pattern),
                ("optics_group_name", self.optics_group_name),
                ("prefix", ""),
                ("angpix", str(self.pixel_size)),
                ("kV", str(int(self.voltage))),
                ("Cs", str(self.spherical_aberration)),
                ("Q0", str(self.amplitude_contrast)),
                ("dose_rate", str(self.dose_per_tilt)),
                ("dose_is_per_movie_frame", "No"),
                ("tilt_axis_angle", str(self.tilt_axis_angle)),
                ("mtf_file", ""),
                ("flip_tiltseries_hand", "Yes" if self.acquisition.invert_defocus_hand else "No"),
            ]

    def _get_queue_options(self) -> List[Tuple[str, str]]:
        """Import jobs defaults to local run, but includes correct keys for consistency."""
        slurm_config = self.get_effective_slurm_config()
        
        options = [
            ("do_queue", "No"),
            ("queuename", slurm_config.partition),
            ("qsub", "sbatch"),
            ("qsubscript", "qsub/qsub.sh"),
            ("min_dedicated", "1"),
            ("other_args", ""),
        ]
        
        # Add Slurm placeholders even if do_queue is No
        options.extend(list(slurm_config.to_qsub_extra_dict().items()))
        return options

    def get_tool_name(self) -> str:
        return "relion_import"

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
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    

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

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [
            ("in_mic", str(input_star)),
            # Could add other in_* fields if needed for RELION GUI display
        ]


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
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

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

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
            input_star = self.paths.get("input_star", "")
            return [("in_mic", str(input_star))]


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
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    

    window: int = Field(default=512, ge=128, le=2048)
    range_min_max: str = Field(default="30:6.0")
    defocus_min_max: str = Field(default="0.5:8")
    defocus_hand: str = Field(default="set_flip")
    perdevice: int = Field(default=1, ge=0, le=8)


    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

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
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    

    rescale_angpixs: float = Field(default=12.0, ge=2.0, le=50.0)
    halfmap_frames : int   = Field(default=1, ge=0, le=1)
    deconv         : int   = Field(default=1, ge=0, le=1)
    perdevice      : int   = Field(default=1, ge=0, le=8)

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "warptools"


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
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = True
    

    tomograms_for_training    : str = Field(default="Position_1")
    number_training_subvolumes: int = Field(default=600, ge=100)
    subvolume_dimensions      : int = Field(default=64, ge=32)
    perdevice                 : int = Field(default=1)

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        # This job uses in_tomoset, not in_mic
        input_star = self.paths.get("input_star", "")
        return [("in_tomoset", str(input_star))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "cryocare"  


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
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = True


    ntiles_x: int = Field(default=4, ge=1)
    ntiles_y: int = Field(default=4, ge=1)
    ntiles_z: int = Field(default=4, ge=1)
    denoising_tomo_name: str = "" 
    perdevice: int = Field(default=1)

    def __init__(self, **data):
        super().__init__(**data)
        # Set cryoCARE-specific SLURM defaults
        if "slurm_overrides" not in data:
            self.slurm_overrides = {
                "gres": "gpu:1",  # cryoCARE prediction is single-GPU only!
                "mem": "64G",
                "cpus_per_task": 4,
                "time": "4:00:00",
                "preset": SlurmPreset.CUSTOM.value
            }

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "cryocare" 

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        model_path = self.paths.get("model_path", "")
        return [
            ("in_tomoset", str(input_star)),
            ("in_model", str(model_path)),
        ]

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"train": "denoisetrain"}


    # In DenoisePredictParams

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        """
        DenoisePredict needs:
        1. Model from DenoiseTrain (upstream_job_dir - linear predecessor)
        2. Tomograms from TsReconstruct (looked up from state)
        """
        if not self._project_state:
            raise RuntimeError("DenoisePredictParams not attached to ProjectState")

        # --- 1. MODEL PATH (from DenoiseTrain) ---
        if upstream_job_dir:
            model_path = upstream_job_dir / "denoising_model.tar.gz"
        else:
            # Fallback: look for historical train job
            train_job = self._project_state.jobs.get(JobType.DENOISE_TRAIN)
            if train_job and train_job.paths.get("output_model"):
                model_path = Path(train_job.paths["output_model"])
            else:
                # Try to find any denoising_model.tar.gz in the project
                potential_models = list(self.project_root.glob("**/denoising_model.tar.gz"))
                if potential_models:
                    model_path = potential_models[0]
                else:
                    raise ValueError("DenoisePredict requires DenoiseTrain (no model path found)")

        # --- 2. TOMOGRAMS (from TsReconstruct) ---
        ts_job = self._project_state.jobs.get(JobType.TS_RECONSTRUCT)
        
        # Multiple ways to find input_star
        input_star = None
        reconstruct_base = None
        
        # Option 1: From ts_job paths
        if ts_job and ts_job.paths.get("output_star"):
            input_star = Path(ts_job.paths["output_star"])
            reconstruct_base = Path(ts_job.paths.get("warp_dir") or 
                                ts_job.paths.get("output_processing", ""))
        
        # Option 2: Search for tomograms.star
        if not input_star or not input_star.exists():
            potential_stars = list(self.project_root.glob("**/tomograms.star"))
            if potential_stars:
                # Prefer External/job006/ over others
                external_stars = [s for s in potential_stars if "External/job" in str(s)]
                input_star = external_stars[0] if external_stars else potential_stars[0]
                
                # Infer reconstruct_base from tomograms.star location
                reconstruct_base = input_star.parent / "warp_tiltseries"
        
        if not input_star or not input_star.exists():
            raise ValueError(
                f"Cannot find tomograms.star. Searched in: {list(self.project_root.glob('**/tomograms.star'))}"
            )
        
        if not reconstruct_base or not reconstruct_base.exists():
            # Try to infer from common locations
            possible_bases = [
                input_star.parent / "warp_tiltseries",
                self.project_root / "External" / f"job{input_star.parent.name.replace('job', '')}" / "warp_tiltseries",
                self.project_root / "External" / input_star.parent.name / "warp_tiltseries",
            ]
            for base in possible_bases:
                if base.exists():
                    reconstruct_base = base
                    break
        
        return {
            "job_dir": job_dir,
            "project_root": self.project_root,
            "model_path": model_path,
            "input_star": input_star,
            "reconstruct_base": reconstruct_base,
            "output_dir": job_dir / "denoised",
        }


class TemplateMatchPytomParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = False  # From your template
    

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

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        return [
            ("in_mic", str(self.paths.get("input_tomograms", ""))),
            ("in_3dref", str(self.paths.get("template_path", ""))),
            ("in_mask", str(self.paths.get("mask_path", ""))),
            ("in_coords", ""),
            ("in_mov", ""),
            ("in_part", ""),
        ]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "pytom"



    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        # We prefer denoised tomograms, but reconstruct works too
        return {"tomograms": "denoisepredict"}

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        """
        TemplateMatch needs:
        1. Tomograms (from upstream - either DenoisePredict or TsReconstruct)
        2. Tilt series metadata (from TsCtf - for angles/defocus)
        3. Template and mask (user-provided paths)
        """
        if not self._project_state:
            raise RuntimeError("TemplateMatchPytomParams not attached to ProjectState")

        # --- 1. TOMOGRAMS (from upstream or fallback) ---
        if upstream_job_dir:
            # Could be DenoisePredict or TsReconstruct depending on pipeline
            input_tomograms = upstream_job_dir / "tomograms.star"
            if not input_tomograms.exists():
                # DenoisePredict might output differently
                input_tomograms = upstream_job_dir / "denoised" / "tomograms.star"
        else:
            # Fallback: look for reconstruct output
            rec_job = self._project_state.jobs.get(JobType.TS_RECONSTRUCT)
            if rec_job and rec_job.paths.get("output_star"):
                input_tomograms = Path(rec_job.paths["output_star"])
            else:
                raise ValueError("No tomogram source found for TemplateMatching")

        # --- 2. TILT SERIES (from TsCtf - for angle/defocus files) ---
        ctf_job = self._project_state.jobs.get(JobType.TS_CTF)
        
        # DON'T check execution_status - just check if paths exist
        if ctf_job and ctf_job.paths.get("output_star"):
            input_tiltseries = Path(ctf_job.paths["output_star"])
        else:
            raise ValueError(
                "TemplateMatching requires TsCtf job with resolved paths "
                "(for tilt angle and defocus information)"
            )

        return {
            "job_dir": job_dir,
            "project_root": self.project_root,
            "input_tomograms": input_tomograms,
            "input_tiltseries": input_tiltseries,
            "template_path": Path(self.template_path) if self.template_path else None,
            "mask_path": Path(self.mask_path) if self.mask_path else None,
            "output_dir": job_dir / "tmResults",
        }


class CandidateExtractPytomParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    


    # Particle Params (defaults from original job.star)
    particle_diameter_ang: float = Field(default=200.0)  # param3
    max_num_particles: int = Field(default=1500)         # param4
    
    # Thresholding
    cutoff_method: str = Field(default="NumberOfFalsePositives")  # param1: "NumberOfFalsePositives" or "ManualCutOff"
    cutoff_value: float = Field(default=1.0)                      # param2
    
    # Score Map
    apix_score_map: str = Field(default="auto")  # param5
    
    # Filtering
    score_filter_method: str = Field(default="None")  # param6: "None" or "tophat"
    score_filter_value: str = Field(default="None")   # param7: "connectivity:bins" e.g. "1:10"
    
    # Optional mask folder for excluding regions
    mask_fold_path: str = Field(default="None")  # param8


    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_tm_job = self.paths.get("input_tm_job", "")
        return [("in_mic", str(input_tm_job))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "pytom"


    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"tm_job": "templatematching"}

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        """
        Resolves paths for candidate extraction.
        upstream_job_dir: The template matching job directory (e.g., External/job007/)
        """
        if not upstream_job_dir:
            raise ValueError("Extraction requires an upstream Template Matching job directory")

        # Find tomograms.star - check multiple sources
        input_tomograms = None
        
        # 1. Check if TM job has tomograms.star
        tm_tomograms = upstream_job_dir / "tomograms.star"
        if tm_tomograms.exists():
            input_tomograms = tm_tomograms
        
        # 2. Look in project state for reconstruct/denoise output
        if not input_tomograms and self._project_state:
            for source_job in [JobType.DENOISE_PREDICT, JobType.TS_RECONSTRUCT]:
                source_model = self._project_state.jobs.get(source_job)
                if source_model and source_model.execution_status == JobStatus.SUCCEEDED:
                    source_star = source_model.paths.get("output_star")
                    if source_star and Path(source_star).exists():
                        input_tomograms = Path(source_star)
                        break
        
        # 3. Fallback
        if not input_tomograms:
            print(f"[WARN] Could not resolve input_tomograms, will attempt runtime resolution")
            input_tomograms = upstream_job_dir / "tomograms.star"

        # Resolve mask folder if provided
        mask_fold = None
        if self.mask_fold_path and self.mask_fold_path != "None":
            mask_fold = Path(self.mask_fold_path)

        return {
            "job_dir"         : job_dir,
            "project_root"    : self.project_root,
            "input_tm_job"    : upstream_job_dir,
            "input_tomograms" : input_tomograms,
            "mask_fold"       : mask_fold,
            "output_star"     : job_dir / "candidates.star",
            "output_tomograms": job_dir / "tomograms.star",
            "optimisation_set": job_dir / "optimisation_set.star",
        }


class SubtomoExtractionParams(AbstractJobParams):
    """
    Subtomogram extraction using RELION's relion_tomo_subtomo.
    Creates pseudo-subtomograms from tilt series for downstream averaging/classification.
    """

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    # Extraction parameters
    binning: float = Field(default=1.0, description="Binning factor relative to unbinned data")
    box_size: int = Field(default=512, description="Box size in binned pixels")
    crop_size: int = Field(default=256, description="Cropped box size (-1 = no cropping)")
    
    # Output format
    do_float16: bool = Field(default=True, description="Write output in float16 to save space")
    do_stack2d: bool = Field(default=True, description="Write as 2D stacks (preferred for RELION 4.1+)")
    
    # Filtering
    max_dose: float = Field(default=-1.0, description="Max dose to include (-1 = all)")
    min_frames: int = Field(default=1, description="Min frames per tilt to include")


    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_opt = self.paths.get("input_optimisation", "")
        return [("in_optimisation", str(input_opt))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"


    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        # Depends on candidate extraction output
        return {"optimisation_set": "tmextractcand"}

    def resolve_paths(self, job_dir: Path, upstream_job_dir: Optional[Path] = None) -> Dict[str, Path]:
        """
        Resolve input/output paths for subtomo extraction.
        Input: optimisation_set.star from candidate extraction
        Output: particles.star + new optimisation_set.star
        """
        if not upstream_job_dir:
            raise ValueError("SubtomoExtraction requires upstream candidate extraction job (tmextractcand)")

        input_opt = upstream_job_dir / "optimisation_set.star"
        
        return {
            "job_dir": job_dir,
            "project_root": self.project_root,
            # Input
            "input_optimisation": input_opt,
            # Outputs (RELION generates these)
            "output_particles": job_dir / "particles.star",
            "output_optimisation": job_dir / "optimisation_set.star",
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
    JobType.SUBTOMO_EXTRACTION    : SubtomoExtractionParams,  

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


    slurm_defaults: SlurmConfig = Field(default_factory=SlurmConfig.from_config_defaults)

    jobs: Dict[JobType, AbstractJobParams] = Field(default_factory=dict)

    def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        if job_type in self.jobs:
            return

        param_class_map = jobtype_paramclass()
        param_class = param_class_map.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_type}")

        # Just instantiate with defaults - no more template loading
        job_params = param_class()
        job_params._project_state = self
        self.jobs[job_type] = job_params
        self.update_modified()
        print(f"[STATE] Initialized {job_type.value} with defaults")

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
        data["slurm_defaults"] = self.slurm_defaults.model_dump()
        data["jobs"] = {
            job_type.value: job_params.model_dump()
            for job_type, job_params in self.jobs.items()
        }

        with open(save_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        print(f"[STATE] Project state saved to {save_path}")

    @classmethod
    def load(cls, path: Path):
        if not path.exists():
            raise FileNotFoundError(f"Project params file not found: {path}")

        with open(path, "r") as f:
            data = json.load(f)

        slurm_data = data.get("slurm_defaults", {})
        slurm_defaults = SlurmConfig(**slurm_data) if slurm_data else SlurmConfig.from_config_defaults()

        project_state = cls(
            project_name=data.get("project_name", "Untitled"),
            project_path=Path(data["project_path"]) if data.get("project_path") else None,
            created_at=datetime.fromisoformat(data.get("created_at", datetime.now().isoformat())),
            modified_at=datetime.fromisoformat(data.get("modified_at", datetime.now().isoformat())),
            movies_glob=data.get("movies_glob", ""),
            mdocs_glob=data.get("mdocs_glob", ""),
            microscope=MicroscopeParams(**data.get("microscope", {})),
            acquisition=AcquisitionParams(**data.get("acquisition", {})),
            slurm_defaults=slurm_defaults,
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

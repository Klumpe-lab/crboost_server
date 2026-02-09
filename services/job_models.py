from __future__ import annotations
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Self, Tuple, TYPE_CHECKING
from pydantic import BaseModel, Field, PrivateAttr
import pandas as pd

from services.computing.slurm_service import SLURM_PRESET_MAP, SlurmConfig, SlurmPreset

# IMPORT FROM BASE, NOT PROJECT_STATE
from services.models_base import JobType, AcquisitionParams, AlignmentMethod, JobCategory, JobStatus, MicroscopeParams
from services.io_slots import InputSlot, OutputSlot, JobFileType


# This prevents the circular import error at runtime
if TYPE_CHECKING:
    from services.project_state import ProjectState


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
    
    # NEW: User overrides for input slot sources
    # Maps input_slot_key -> source specification
    # Format: "jobtype:instance_path" e.g. "tsReconstruct:External/job005"
    #         or "manual:/absolute/path/to/file.star"
    source_overrides: Dict[str, str] = Field(default_factory=dict)



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

    def generate_job_star(self, job_dir: Path, fn_exe: str, star_handler) -> None:
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

        data = {"job": job_data, "joboptions_values": joboptions_df}

        job_dir.mkdir(parents=True, exist_ok=True)
        star_path = job_dir / "job.star"
        star_handler.write(data, star_path)

        # 4. Mandatory RELION version header
        content = star_path.read_text()
        with open(star_path, "w") as f:
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
            ("qsubscript", "qsub.sh"),
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
            "source_overrides", 
            "additional_sources",
            "merge_only",

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

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = []
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.TILT_SERIES_STAR, path_template="tilt_series.star")
    ]

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
            ("qsubscript", "qsub.sh"),
            ("min_dedicated", "1"),
            ("other_args", ""),
        ]

        # Add Slurm placeholders even if do_queue is No
        options.extend(list(slurm_config.to_qsub_extra_dict().items()))
        return options

    def get_tool_name(self) -> str:
        return "relion_import"



class FsMotionCtfParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.TILT_SERIES_STAR], preferred_source="importmovies")
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.FS_MOTION_CTF_STAR, path_template="fs_motion_and_ctf.star"),
        OutputSlot(
            key="output_processing",
            produces=JobFileType.WARP_FRAMESERIES_DIR,
            path_template="warp_frameseries/",
            is_dir=True,
        ),
    ]

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
            ("in_mic", str(input_star))
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



class TsAlignmentParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.FS_MOTION_CTF_STAR], preferred_source="fsMotionAndCtf"),
        InputSlot(
            key="input_processing", accepts=[JobFileType.WARP_FRAMESERIES_DIR], preferred_source="fsMotionAndCtf"
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(
            key="output_star", produces=JobFileType.ALIGNED_TILT_SERIES_STAR, path_template="aligned_tilt_series.star"
        ),
        OutputSlot(
            key="output_processing",
            produces=JobFileType.WARP_TILTSERIES_DIR,
            path_template="warp_tiltseries/",
            is_dir=True,
        ),
    ]

    alignment_method: AlignmentMethod = AlignmentMethod.ARETOMO
    rescale_angpixs: float = Field(default=12.0, ge=2.0, le=50.0)
    tomo_dimensions: str = Field(default="4096x4096x2048")
    sample_thickness_nm: float = Field(default=200.0, ge=50.0, le=1000.0)  # <-- ADD THIS
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

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]


    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"motion": "fsMotionAndCtf"}


class TsCtfParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.ALIGNED_TILT_SERIES_STAR], preferred_source="aligntiltsWarp"),
        InputSlot(key="input_processing", accepts=[JobFileType.WARP_TILTSERIES_DIR], preferred_source="aligntiltsWarp"),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(
            key="output_star", produces=JobFileType.TS_CTF_TILT_SERIES_STAR, path_template="ts_ctf_tilt_series.star"
        ),
        OutputSlot(
            key="output_processing",
            produces=JobFileType.WARP_TILTSERIES_DIR,
            path_template="warp_tiltseries/",
            is_dir=True,
        ),
    ]

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


class TsReconstructParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.TS_CTF_TILT_SERIES_STAR], preferred_source="tsCtf"),
        InputSlot(key="input_processing", accepts=[JobFileType.WARP_TILTSERIES_DIR], preferred_source="tsCtf"),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.TOMOGRAMS_STAR, path_template="tomograms.star"),
        OutputSlot(
            key="output_processing",
            produces=JobFileType.WARP_TILTSERIES_DIR,
            path_template="warp_tiltseries/",
            is_dir=True,
        ),
    ]

    rescale_angpixs: float = Field(default=12.0, ge=2.0, le=50.0)
    halfmap_frames: int = Field(default=1, ge=0, le=1)
    deconv: int = Field(default=1, ge=0, le=1)
    perdevice: int = Field(default=1, ge=0, le=8)

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


class DenoiseTrainParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = True

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.TOMOGRAMS_STAR], preferred_source="tsReconstruct")
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_model", produces=JobFileType.DENOISE_MODEL_TAR, path_template="denoising_model.tar.gz")
    ]

    tomograms_for_training: str = Field(default="Position_1")
    number_training_subvolumes: int = Field(default=600, ge=100)
    subvolume_dimensions: int = Field(default=64, ge=32)
    perdevice: int = Field(default=1)

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



class DenoisePredictParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = True

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="model_path", accepts=[JobFileType.DENOISE_MODEL_TAR], preferred_source="denoisetrain"),
        InputSlot(key="input_star", accepts=[JobFileType.TOMOGRAMS_STAR], preferred_source="tsReconstruct"),
        InputSlot(key="reconstruct_base", accepts=[JobFileType.WARP_TILTSERIES_DIR], preferred_source="tsReconstruct"),
    ]

    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(
            key="output_star",
            produces=JobFileType.DENOISED_TOMOGRAMS_STAR,
            path_template="tomograms.star",  # NOT "denoised/tomograms.star"
        ),
    ]

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
                "preset": SlurmPreset.CUSTOM.value,
            }

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "cryocare"

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        model_path = self.paths.get("model_path", "")
        return [("in_tomoset", str(input_star)), ("in_model", str(model_path))]

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"train": "denoisetrain"}



class TemplateMatchPytomParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = False  # From your template

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(
            key="input_tomograms",
            accepts=[JobFileType.DENOISED_TOMOGRAMS_STAR, JobFileType.TOMOGRAMS_STAR],
            preferred_source="denoisepredict",
        ),
        InputSlot(key="input_tiltseries", accepts=[JobFileType.TS_CTF_TILT_SERIES_STAR], preferred_source="tsCtf"),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
            OutputSlot(key="output_dir", produces=JobFileType.TM_RESULTS_DIR, path_template="tmResults/", is_dir=True),
            OutputSlot(key="output_tomograms", produces=JobFileType.TOMOGRAMS_STAR, path_template="tomograms.star"),
    ]

    # Inputs (Strings here, resolved to Paths in resolve_paths)
    template_path: str = Field(default="")
    mask_path: str = Field(default="")

    # Algorithm Params
    angular_search: str = Field(default="12.0")
    symmetry: str = Field(default="C1")

    # Flags
    defocus_weight: bool = True
    dose_weight: bool = True
    spectral_whitening: bool = True
    random_phase_correction: bool = False
    non_spherical_mask: bool = False

    bandpass_filter: str = Field(default="None")  # Format "low:high"
    gpu_split: str = Field(default="auto")  # "auto" or "4:4:2"
    perdevice: int = Field(default=1)

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        return [
            ("in_mic", str(self.paths.get("input_tomograms", ""))),
            ("in_3dref", str(self.template_path or "")),
            ("in_mask",  str(self.mask_path or "")),
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


class CandidateExtractPytomParams(AbstractJobParams):
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    # In CandidateExtractPytomParams:
    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_tm_job", accepts=[JobFileType.TM_RESULTS_DIR], preferred_source="templatematching"),
        InputSlot(
            key="input_tomograms",
            accepts=[JobFileType.TOMOGRAMS_STAR, JobFileType.DENOISED_TOMOGRAMS_STAR],
            preferred_source="templatematching",  # changed from denoisepredict
            required=True,  # changed from False -- we actually need this
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.CANDIDATES_STAR, path_template="candidates.star"),
        OutputSlot(
            key="optimisation_set", produces=JobFileType.OPTIMISATION_SET_STAR, path_template="optimisation_set.star"
        ),
    ]

    # Particle Params (defaults from original job.star)
    particle_diameter_ang: float = Field(default=200.0)  # param3
    max_num_particles: int = Field(default=1500)  # param4

    # Thresholding
    cutoff_method: str = Field(default="NumberOfFalsePositives")  # param1: "NumberOfFalsePositives" or "ManualCutOff"
    cutoff_value: float = Field(default=1.0)  # param2

    # Score Map
    apix_score_map: str = Field(default="auto")  # param5

    # Filtering
    score_filter_method: str = Field(default="None")  # param6: "None" or "tophat"
    score_filter_value: str = Field(default="None")  # param7: "connectivity:bins" e.g. "1:10"

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



class SubtomoExtractionParams(AbstractJobParams):
    """
    Subtomogram extraction using RELION's relion_tomo_subtomo.
    Creates pseudo-subtomograms from tilt series for downstream averaging/classification.
    """

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(
            key="input_optimisation", accepts=[JobFileType.OPTIMISATION_SET_STAR], preferred_source="tmextractcand"
        )
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_particles", produces=JobFileType.PARTICLES_STAR, path_template="particles.star"),
        OutputSlot(
            key="output_optimisation", produces=JobFileType.OPTIMISATION_SET_STAR, path_template="optimisation_set.star"
        ),
    ]

    additional_sources: List[str] = Field(default_factory=list, description="Extra optimisation_set.star files or job dirs to merge")
    merge_only: bool = Field(default=False, description="If true, skip relion_tomo_subtomo and only merge")

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
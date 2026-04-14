from __future__ import annotations
import logging
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Self, Set, Tuple, TYPE_CHECKING
from pydantic import BaseModel, Field
import pandas as pd

from services.computing.slurm_service import SLURM_PRESET_MAP, SlurmConfig, SlurmPreset
from services.configs.config_service import get_config_service
from services.models_base import JobType, AcquisitionParams, JobCategory, JobStatus, MicroscopeParams

if TYPE_CHECKING:
    from services.project_state import ProjectState

logger = logging.getLogger(__name__)


class SymmetryGroup(str, Enum):
    """RELION point group symmetry designations."""

    C1 = "C1"
    C2 = "C2"
    C3 = "C3"
    C4 = "C4"
    C5 = "C5"
    C6 = "C6"
    D2 = "D2"
    D3 = "D3"
    D4 = "D4"
    D5 = "D5"
    D6 = "D6"
    T = "T"
    O = "O"
    I1 = "I1"
    I2 = "I2"


class TemplateWorkbenchState(BaseModel):
    pixel_size: float = 0.0
    box_size: int = 96
    auto_box: bool = True
    apply_lowpass: bool = False
    template_resolution: Optional[float] = None
    basic_shape_def: str = "550:550:550"
    auto_infer_seed: bool = True


class ExtractionCutoffMethod(str, Enum):
    """How to threshold template matching scores for candidate extraction."""

    FALSE_POSITIVES = "NumberOfFalsePositives"
    MANUAL = "ManualCutOff"


class AbstractJobParams(BaseModel):
    """
    Abstract base class for job parameters.
    Contains NO global parameters, only accessors.
    """

    # Human-readable label shown in the UI roster and tab strip.
    # Backend code (orchestrator, path resolution, drivers) never reads this.
    # Set by the user or auto-generated; purely cosmetic.
    display_label: Optional[str] = None

    species_id: Optional[str] = None
    JOB_CATEGORY: ClassVar[JobCategory]
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"  # Override for native jobs
    IS_TOMO_JOB: ClassVar[bool] = True
    IS_CONTINUE: ClassVar[bool] = False
    IS_INTERACTIVE: ClassVar[bool] = False  # Interactive tools manage their own status

    # ------------------------------------------------------------------
    # Phase 1c: USER_PARAMS whitelist.
    #
    # Each subclass declares which of its fields are user-tunable
    # parameters (i.e. things the user edits in the config tab).
    #
    # Only these fields get:
    #   - Immutability enforcement (blocked on running/completed jobs)
    #   - Dirty-marking on change (triggers persistence at next save point)
    #
    # Everything NOT listed here is internal metadata (execution_status,
    # paths, slurm_overrides, etc.) that can always be written freely
    # by backend code regardless of job status.
    #
    # When you add a new user-facing parameter to a job subclass,
    # add it to that subclass's USER_PARAMS set. If you forget,
    # the field will behave as metadata (no immutability, no dirty mark)
    # which is safe but means edits won't auto-persist -- you'll notice
    # quickly in testing.
    # ------------------------------------------------------------------
    USER_PARAMS: ClassVar[Set[str]] = set()

    # Job execution metadata only
    execution_status: JobStatus = Field(default=JobStatus.SCHEDULED)
    relion_job_name: Optional[str] = None
    relion_job_number: Optional[int] = None
    slurm_job_id: Optional[str] = None  # set when sbatch accepts the job

    is_orphaned: bool = Field(default=False)
    missing_inputs: List[str] = Field(default_factory=list)

    # We store the resolved paths and binds here to persist them in project_params.json
    paths: Dict[str, str] = Field(default_factory=dict)
    additional_binds: List[str] = Field(default_factory=list)
    slurm_overrides: Dict[str, Any] = Field(default_factory=dict)

    # User overrides for input slot sources
    # Maps input_slot_key -> source specification
    # Format: "jobtype:instance_path" e.g. "tsReconstruct:External/job005"
    #         or "manual:/absolute/path/to/file.star"
    source_overrides: Dict[str, str] = Field(default_factory=dict)
    # After the ClassVar declarations, before execution_status:
    job_type: Optional[JobType] = None

    # This is now a private attribute, not a Pydantic model field.
    _project_state: Optional["ProjectState"] = None

    def get_effective_slurm_config(self) -> SlurmConfig:
        """
        Returns the effective SLURM config for this job.
        Three-layer merge: slurm_defaults <- job_resource_profiles[jobtype] <- slurm_overrides
        """
        if self._project_state is not None:
            defaults = self._project_state.slurm_defaults
        else:
            defaults = SlurmConfig.from_config_defaults()

        merged_data = defaults.model_dump()

        # Layer 2: per-job-type resource profile from conf.yaml
        if self.job_type is not None:
            try:
                profile = get_config_service().get_job_resource_profile(self.job_type.value)
                if profile is not None:
                    for field, value in profile.model_dump(exclude_none=True).items():
                        merged_data[field] = value
            except Exception:
                pass  # Config not loaded yet (e.g. during testing)

        # Layer 3: per-job user overrides
        if self.slurm_overrides:
            merged_data.update(self.slurm_overrides)

        return SlurmConfig(**merged_data)

    def get_profile_slurm_config(self) -> SlurmConfig:
        """
        Returns the SLURM config with profile applied but WITHOUT user overrides.
        Used by the UI "Reset to profile" button.
        """
        if self._project_state is not None:
            defaults = self._project_state.slurm_defaults
        else:
            defaults = SlurmConfig.from_config_defaults()

        merged_data = defaults.model_dump()

        if self.job_type is not None:
            try:
                profile = get_config_service().get_job_resource_profile(self.job_type.value)
                if profile is not None:
                    for field, value in profile.model_dump(exclude_none=True).items():
                        merged_data[field] = value
            except Exception:
                pass

        return SlurmConfig(**merged_data)

    def has_resource_profile(self) -> bool:
        """Whether this job type has a resource profile in conf.yaml."""
        if self.job_type is None:
            return False
        try:
            return get_config_service().get_job_resource_profile(self.job_type.value) is not None
        except Exception:
            return False

    def apply_slurm_preset(self, preset: SlurmPreset) -> None:
        """Flatten preset values into the overrides dictionary."""
        if preset == SlurmPreset.CUSTOM:
            self.slurm_overrides["preset"] = SlurmPreset.CUSTOM.value
            if self._project_state:
                self._project_state.mark_dirty()
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

        # Phase 1b: mark dirty instead of saving immediately.
        # The UI save_handler (called right after this) triggers the actual write.
        if self._project_state:
            self._project_state.mark_dirty()

    def set_slurm_override(self, field: str, value: Any) -> None:
        """Update a single field and ensure we flip to Custom."""
        self.slurm_overrides[field] = value
        # If we change a resource, we are no longer strictly on a preset
        if field != "preset":
            self.slurm_overrides["preset"] = SlurmPreset.CUSTOM.value

        # Phase 1b: mark dirty instead of saving immediately.
        if self._project_state:
            self._project_state.mark_dirty()

    def clear_slurm_overrides(self) -> None:
        """Clear all per-job SLURM overrides, reverting to project defaults"""
        self.slurm_overrides = {}
        if self._project_state:
            self._project_state.mark_dirty()

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

        logger.info("Generated %s", star_path)

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

    # ------------------------------------------------------------------
    # Phase 1b + 1c: Rewritten __setattr__
    #
    # Uses USER_PARAMS whitelist instead of a fragile bypass blacklist.
    # Only user-tunable params get immutability enforcement and dirty-marking.
    # Everything else (metadata fields) is written freely.
    # ------------------------------------------------------------------
    def __setattr__(self, name: str, value: Any) -> None:
        # Private/internal attributes always bypass (Pydantic internals, _project_state, etc.)
        if name.startswith("_"):
            super().__setattr__(name, value)
            return

        # Only user-tunable parameters get immutability checks and dirty-marking.
        # All other fields (execution_status, paths, slurm_overrides, etc.) are
        # internal metadata that backend code can always write freely.
        if name not in self.USER_PARAMS:
            super().__setattr__(name, value)
            return

        # --- From here on, we're dealing with a user-tunable parameter ---

        # Immutability check: block edits on non-SCHEDULED jobs
        try:
            current_status = object.__getattribute__(self, "execution_status")
        except AttributeError:
            # During __init__, execution_status may not exist yet
            super().__setattr__(name, value)
            return

        if current_status not in (JobStatus.SCHEDULED, JobStatus.FAILED):
            logger.info("Blocked change to '%s' on %s job", name, current_status.value)
            return

        # Apply the change
        super().__setattr__(name, value)

        # Mark the project dirty (actual disk write happens at explicit save points:
        # save_project(), pipeline start, etc.)
        if self._project_state is not None:
            self._project_state.mark_dirty()

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

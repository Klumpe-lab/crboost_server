from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from services.computing.slurm_service import SlurmConfig
from services.configs.config_service import get_config_service
from services.io_slots import InputSlot, JobFileType, OutputSlot
from services.jobs._base import AbstractJobParams
from services.models_base import JobCategory, JobType, SegmentFormat, SegmentMethod


class SegmentParams(AbstractJobParams):
    """Segment every tomogram of the input into feature probability volumes, one GPU per tomogram:
    easymode's pretrained networks for the features chosen, or one Ais model trained by the user.
    The volumes are written on the tomogram's own grid as segmentations/<tomogram>__<feature>.mrc,
    and segmentations.json lists them with their provenance."""

    job_type: JobType = Field(default=JobType.SEGMENT)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = True

    USER_PARAMS: ClassVar[set[str]] = {
        "method",
        "features",
        "ais_model_path",
        "tta",
        "output_format",
        "match_training_voxel",
        "smooth_z",
        "array_throttle",
    }

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        # easymode was trained on raw, even/odd and noise2noise reconstructions; a denoised input is
        # accepted, and the job tab marks an IsoNet one (missing-wedge corrected).
        InputSlot(
            key="input_tomograms",
            accepts=[JobFileType.TOMOGRAMS_STAR, JobFileType.DENOISED_TOMOGRAMS_STAR],
            preferred_source="tsReconstruct",
        )
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(key="output_segmentations", produces=JobFileType.SEGMENTATIONS, path_template="segmentations.json")
    ]

    CONFIG_PREAMBLE: ClassVar[str] = (
        "Each tomogram is one SLURM array task on one GPU. **easymode** runs its pretrained network for every "
        "feature chosen, from the weights cache in `tools.easymode.cache_dir` and never from the network, so a "
        "run always uses the same weights; **Ais model** applies one model (`.scnm`) you trained in Ais, and "
        "the model's title names the feature.\n\n"
        "easymode's networks were trained on 10 Å reconstructions, so by default each tomogram is resampled to "
        "10 Å for them, and every volume is mapped back onto the tomogram's grid.\n\n"
        "Every volume has the tomogram's shape and voxel size: `segmentations/<tomogram>__<feature>.mrc`, "
        "probability × 127 (int8) or × 255. The Tomograms view shows each feature as a transparent fill "
        "where p ≥ 0.5, on the reconstruction the input traces back to."
    )

    SLURM_NOTE: ClassVar[tuple[str, str]] = (
        "One GPU per tomogram, with at least 24 GB: easymode peaked at 22 GB",
        "On a 264 × 530 × 530 tomogram, membrane, void, cytoplasm and ribosome together peaked at 22.2 GB of GPU "
        "memory, on a 24 GB card. A smaller card runs out of memory, so set the constraint to the nodes whose "
        "cards have 24 GB or more (on CBE: g3 = RTX 6000, g4 = A100).\n\n"
        "• On 1024 × 1024 × 512 tomograms at 6.2 Å with TTA 2, cytoplasm and nucleus took about 1.5 min each and "
        "ribosome 4.7 min, plus a minute of start-up per task. The networks' time grows with TTA: 8 takes four "
        "times as long as 2. At 10 Å the 2.5D networks have fewer planes to go through.\n"
        "• The time limit is per tomogram.",
    )

    method: SegmentMethod = Field(
        default=SegmentMethod.EASYMODE,
        description="easymode: its pretrained networks for the features below. Ais model: one model you "
        "trained in Ais (.scnm).",
    )
    features: list[str] = Field(
        default_factory=list, description="easymode features to segment, from the weights cache's registry"
    )
    ais_model_path: str = Field(
        default="", description="The Ais model (.scnm) to apply; its title becomes the feature's name"
    )
    tta: int = Field(
        default=8,
        ge=1,
        le=8,
        title="TTA",
        description="Test-time augmentation: how many rotated and flipped copies of each tomogram are segmented "
        "and averaged. More is smoother and slower: the networks take TTA times as long as one pass. Ais takes "
        "at most 8.",
    )
    output_format: SegmentFormat = Field(
        default=SegmentFormat.INT8,
        description="How easymode's own 3D networks store p: int8 (p × 127, the smallest), uint16 (p × 255) "
        "or float32 (p). Ais-engine features are always p × 255.",
    )
    match_training_voxel: bool = Field(
        default=True,
        title="Segment at 10 Å",
        description="Resample each tomogram to 10 Å, the voxel size easymode's networks were trained on (its "
        "spectrum cropped, as a 10 Å reconstruction holds it), segment that, and map every volume back onto the "
        "tomogram's grid. Off, the tomogram is segmented as it is: easymode's 3D networks resample it themselves, "
        "the Ais-engine ones (cytoplasm, nucleus, void, membrane, …) only in X/Y, so their Z context and contrast "
        "differ from training. A tomogram within 5 % of 10 Å, or coarser, is segmented as it is either way.",
    )
    smooth_z: bool = Field(
        default=True,
        title="Smooth along Z",
        description="Networks that decide each Z plane on its own (2D and 2.5D: cytoplasm, nucleus, void, "
        "mitochondrion, …) see X/Y at a coarse pixel (50 or 30 Å). Smooth their volumes along Z to that same "
        "resolution (a Gaussian whose FWHM is the network's X/Y pixel), which damps their plane-to-plane flips.",
    )
    array_throttle: int = Field(default=8, ge=1, le=64, description="Max concurrent SLURM array tasks (one GPU each)")

    def _get_queue_options(self) -> list[tuple[str, str]]:
        """The parent sbatch is a CPU-only supervisor that submits the per-tomogram array, waits for it
        and aggregates; the job's SLURM settings describe one array task (see TsReconstructParams)."""
        sup = get_config_service().supervisor_slurm_defaults
        options = [
            ("do_queue", "Yes"),
            ("queuename", sup.partition),
            ("qsub", "sbatch"),
            ("qsubscript", "qsub.sh"),
            ("min_dedicated", "1"),
        ]
        for field_name, var_name in SlurmConfig.QSUB_EXTRA_MAPPING.items():
            options.append((var_name, str(getattr(sup, field_name))))
        return options

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        return [("in_mic", str(self.paths.get("input_tomograms", "")))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        # One image for both methods: `ais` ships beside easymode.
        return "easymode"

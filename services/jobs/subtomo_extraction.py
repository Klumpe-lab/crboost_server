from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.computing.slurm_service import SlurmConfig
from services.configs.config_service import get_config_service
from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class SubtomoExtractionParams(AbstractJobParams):
    """
    Subtomogram extraction using RELION's relion_tomo_subtomo.
    Creates pseudo-subtomograms from tilt series for downstream averaging/classification.

    Per-TS parallelization: the supervisor slices the upstream
    optimisation_set.star into per-TS slices (one staging dir per TS),
    submits a SLURM array, and merges per-TS particles.star files into
    one canonical job_dir/particles.star. Mirrors the array pattern used
    by templatematching and candidate-extract.
    """

    job_type: JobType = Field(default=JobType.SUBTOMO_EXTRACTION)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[Set[str]] = {
        "binning",
        "box_size",
        "crop_size",
        "do_float16",
        "do_stack2d",
        "max_dose",
        "min_frames",
        "array_throttle",
    }

    # NOTE: additional_sources and merge_only are intentionally NOT in
    # USER_PARAMS. They are widget-managed state (the merge panel sets
    # them directly and triggers its own save). Keeping them out means:
    #   - No immutability enforcement (the merge panel can write them
    #     even after the job has run, which is correct — merging is a
    #     post-hoc operation on existing outputs)
    #   - No automatic dirty-marking (the merge panel calls save_handler
    #     explicitly after changing them)

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
        # Curator-saved subset of the picks above. Written post-hoc by the
        # dashboard's "Save picks" action (services/visualization/picks_filter.py);
        # the driver itself never touches this file. prefer_if_exists=True makes
        # the resolver admit this candidate only when the file is on disk and,
        # when admitted, rank it above the original within the same producer —
        # so downstream reconstruct_particle silently picks it up.
        OutputSlot(
            key="output_optimisation_filtered",
            produces=JobFileType.OPTIMISATION_SET_STAR,
            path_template="optimisation_set_filtered.star",
            prefer_if_exists=True,
        ),
        OutputSlot(
            key="output_particles_filtered",
            produces=JobFileType.PARTICLES_STAR,
            path_template="particles_filtered.star",
            prefer_if_exists=True,
        ),
    ]

    additional_sources: List[str] = Field(
        default_factory=list, description="Extra optimisation_set.star files or job dirs to merge"
    )
    merge_only: bool = Field(default=False, description="If true, skip relion_tomo_subtomo and only merge")

    # Extraction parameters
    binning: float = Field(default=1.0, description="Binning factor relative to unbinned data")
    box_size: int = Field(default=384, description="Box size in binned pixels")
    crop_size: int = Field(default=224, description="Cropped box size (-1 = no cropping)")
    # Output format
    do_float16: bool = Field(default=True, description="Write output in float16 to save space")
    do_stack2d: bool = Field(default=True, description="Write as 2D stacks (preferred for RELION 4.1+)")

    # Filtering
    max_dose: float = Field(default=-1.0, description="Max dose to include (-1 = all)")
    min_frames: int = Field(default=1, description="Min frames per tilt to include")

    array_throttle: int = Field(
        default=16, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tilt-series subtomo extraction"
    )

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_opt = self.paths.get("input_optimisation", "")
        return [("in_optimisation", str(input_opt))]

    def _get_queue_options(self) -> List[Tuple[str, str]]:
        """
        Override: extract's parent sbatch is a lightweight CPU-only supervisor
        that slices the upstream optimisation set per TS, submits an array,
        and merges results. Per-task resources (the actual relion_tomo_subtomo
        invocations — these are the GPU/memory-heavy ones) are consumed by the
        supervisor when it builds the array sbatch, NOT by this supervisor's
        own sbatch. Mirrors candidate_extract.py:_get_queue_options.
        """
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

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"optimisation_set": "tmextractcand"}

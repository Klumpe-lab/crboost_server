from __future__ import annotations
from typing import ClassVar
from pydantic import Field

from services.computing.slurm_service import SlurmConfig
from services.configs.config_service import get_config_service
from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class WarpExportParams(AbstractJobParams):
    """
    Particle extraction with `WarpTools ts_export_particles --2d`.

    Reads the same optimisation set as subtomoExtraction and writes the same envelope
    (particles.star / tomograms.star / optimisation_set.star) of 2D particle series that
    Class3D/Refine3D read. Warp cuts each tilt image with its full alignment model, local
    image warps included; RELION extraction applies only the rigid per-tilt geometry.

    The particle series come from Warp; the tilt geometry RELION refines against is the
    tsCtf job's per-tilt star, reordered into Warp's frame order, so the CTF, dose and hand
    are the ones the RELION extraction path uses.
    """

    job_type: JobType = Field(default=JobType.WARP_EXPORT)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[set[str]] = {
        "binning",
        "box_size",
        "diameter_ang",
        "max_missing_tilts",
        "dont_premultiply",
        "perdevice",
        "array_throttle",
    }

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(
            key="input_optimisation", accepts=[JobFileType.OPTIMISATION_SET_STAR], preferred_source="tmextractcand"
        )
    ]

    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(key="output_particles", produces=JobFileType.PARTICLES_STAR, path_template="particles.star"),
        OutputSlot(
            key="output_optimisation", produces=JobFileType.OPTIMISATION_SET_STAR, path_template="optimisation_set.star"
        ),
    ]

    binning: float = Field(
        default=1.0,
        description=(
            "Output pixel size = tilt-series pixel size × this factor (Warp's --output_angpix). "
            "Same meaning as subtomoExtraction's binning, so both paths can cut the same particles "
            "at the same voxel size."
        ),
    )
    box_size: int = Field(
        default=224,
        description=(
            "Particle image size in output pixels (even). No larger box is cropped afterwards: "
            "the box must hold the particle plus the signal defocus spreads outward, so aim "
            "~2× the particle diameter."
        ),
    )
    diameter_ang: float = Field(
        default=200.0,
        description=(
            "Particle diameter in Å. Warp marks a tilt as not visible when the particle is "
            "closer than half of this to the image edge."
        ),
    )
    max_missing_tilts: int = Field(
        default=999,
        ge=0,
        description=(
            "Drop particles not visible in more than this many tilts. The default keeps every "
            "particle visible in at least one tilt, as subtomoExtraction's min_frames 1 does."
        ),
    )
    dont_premultiply: bool = Field(
        default=True,
        description=(
            "Write raw particle images and let RELION apply the CTF and dose weighting, with "
            "the same hand as the RELION extraction path. Off = Warp premultiplies them."
        ),
    )
    perdevice: int = Field(default=1, ge=1, description="Warp worker processes per GPU")

    array_throttle: int = Field(
        default=16, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tilt-series export"
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_opt = self.paths.get("input_optimisation", "")
        return [("in_optimisation", str(input_opt))]

    def _get_queue_options(self) -> list[tuple[str, str]]:
        """The parent sbatch is the CPU-only supervisor; the GPU resources of the job's
        profile go to the per-TS array tasks (same split as subtomoExtraction)."""
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
        return "warp_aretomo"

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"optimisation_set": "tmextractcand"}

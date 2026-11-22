from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from services.io_slots import InputSlot, JobFileType, OutputSlot
from services.jobs._base import L, S, AbstractJobParams, form
from services.models_base import JobCategory, JobType


class FrameAlignParams(AbstractJobParams):
    """
    relion_tomo_align: each tilt's rigid alignment (angles and shifts) refined per tilt series, the particles'
    positions, and their motion over the tilts, by comparing the tilt images with a reference projected through the
    particles' poses (roadmap 53 §15.10 T3, §15.11).

    Inputs as CTF refine's: a Refine 3D's optimisation set, and the reference as two half maps at the tilt series' own
    pixel size with their mask and their FSC. The output is RELION's whole model: refined tilt-series stars, the
    particles placed in them, and RELION's motion, which replaces Warp's local geometry (RELION drops the trajectories
    it reads: they only centre its search windows). The tomograms star is marked so the bridge hands the set on as it
    is. The particles name no images: the stacks were cut with the old model, so the set goes through Subtomo
    Extraction before a Refine 3D. A tilt series with fewer than `min_particles` keeps what it came with (its star,
    its particles, their trajectories); by default every tilt series is refined, as RELION does.
    """

    job_type: JobType = Field(default=JobType.FRAME_ALIGN)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    USER_PARAMS: ClassVar[set[str]] = {
        "refine_angles",
        "refine_shifts",
        "refine_positions",
        "max_shift_px",
        "iterations",
        "motion",
        "sigma_velocity",
        "sigma_divergence",
        "square_exp_kernel",
        "per_frame_noise",
        "box_size",
        "min_particles",
        "threads",
        "other_args",
    }
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = ("Alignment", "Motion", "Box & checks", "Compute")
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_optimisation", accepts=[JobFileType.OPTIMISATION_SET_STAR], preferred_source="refine3d"),
        InputSlot(key="input_half_map", accepts=[JobFileType.HALF_MAP], preferred_source="reconstructParticle"),
        InputSlot(key="input_mask", accepts=[JobFileType.MASK], preferred_source="maskCreate"),
        InputSlot(key="input_fsc", accepts=[JobFileType.FSC_STAR], preferred_source="postprocess"),
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(
            key="output_optimisation",
            produces=JobFileType.OPTIMISATION_SET_STAR,
            path_template="aligned_optimisation_set.star",
        )
    ]

    # What is refined
    refine_angles: bool = Field(
        default=True,
        description="Refine each tilt's angles (off: --const_a)",
        title="Tilt angles",
        json_schema_extra=form("Alignment"),
    )
    refine_shifts: bool = Field(
        default=True,
        description="Refine each tilt's shift (off: --const_s)",
        title="Tilt shifts",
        json_schema_extra=form("Alignment"),
    )
    refine_positions: bool = Field(
        default=True,
        description="Refine the particles' positions (off: --const_p)",
        title="Particle positions",
        json_schema_extra=form("Alignment"),
    )
    max_shift_px: int = Field(
        default=30,
        ge=1,
        description="Largest shift of a particle allowed, in tilt-image pixels (--r; RELION's default 20). On the "
        "1.55 Å, 30: missAlign's warps on Grid2 reach 41 Å",
        title="Largest shift",
        json_schema_extra=form("Alignment", S, "px"),
    )
    iterations: int = Field(
        default=10000,
        ge=1,
        description="Most optimiser iterations per tilt series (--it). The capsid's four large tilt series stopped at "
        "10,000 without converging (roadmap 53 §15.10 T3)",
        title="Iterations",
        json_schema_extra=form("Alignment", S),
    )

    # Motion
    motion: bool = Field(
        default=True,
        description="Refine each particle's motion over the tilts (--motion): what replaces Warp's local image warps",
        title="Particle motion",
        json_schema_extra=form("Motion"),
    )
    sigma_velocity: float = Field(
        default=0.5,
        gt=0,
        description="Expected speed of the motion (--s_vel, Å per e/Å² of dose)",
        title="Velocity σ",
        json_schema_extra=form("Motion", S, "Å/dose"),
    )
    sigma_divergence: float = Field(
        default=5000.0,
        gt=0,
        description="Distance over which neighbouring particles move alike (--s_div)",
        title="Divergence σ",
        json_schema_extra=form("Motion", S, "Å"),
    )
    square_exp_kernel: bool = Field(
        default=False,
        description="A square-exponential kernel for the motion instead of the exponential one (--sq_exp_ker)",
        title="Square-exp. kernel",
        json_schema_extra=form("Motion"),
    )
    per_frame_noise: bool = Field(
        default=False,
        description="A noise spectrum of its own for each tilt (--per_frame_noise)",
        title="Noise per tilt",
        json_schema_extra=form("Motion"),
    )

    # Box and checks
    box_size: int = Field(
        default=0,
        ge=0,
        description="Box cut around each particle in the tilt images, in pixels at the tilt series' pixel size (--b); "
        "0 = the reference's box",
        title="Box",
        json_schema_extra=form("Box & checks", S, "px"),
    )
    min_particles: int = Field(
        default=1,
        ge=1,
        description="A tilt series with fewer particles keeps what it came with (its star, its particles' positions "
        "and their trajectories); 1 = refine every one, as RELION does. On the capsid, keeping the eight tilt series "
        "under 10 (17 capsids, whose fitted angles and shifts ran up to 6° and 81 Å) changed the map little (roadmap 53 §15.11)",
        title="Fewest particles",
        json_schema_extra=form("Box & checks", S),
    )

    threads: int = Field(
        default=8, ge=1, description="OpenMP threads (--j)", title="Threads", json_schema_extra=form("Compute", S)
    )
    other_args: str = Field(
        default="",
        description="Extra relion_tomo_align flags, appended verbatim (the deformation options among them)",
        title="Extra arguments",
        json_schema_extra=form("Compute", L),
    )

    MOTION_ONLY: ClassVar[frozenset[str]] = frozenset({"sigma_velocity", "sigma_divergence", "square_exp_kernel"})

    def run_problems(self) -> list[str]:
        # What drivers/frame_align.py refuses before RELION runs, said before the submit.
        if not (self.refine_angles or self.refine_shifts or self.refine_positions or self.motion):
            return ["nothing to refine: turn on tilt angles, tilt shifts, particle positions or motion"]
        return []

    def shows_param(self, name: str) -> bool:
        if name in self.MOTION_ONLY:
            return self.motion
        return True

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        return [
            ("in_optimisation", str(self.paths.get("input_optimisation", ""))),
            ("fn_half1", str(self.paths.get("input_half_map", ""))),
            ("fn_mask", str(self.paths.get("input_mask", ""))),
            ("fn_fsc", str(self.paths.get("input_fsc", ""))),
        ]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"

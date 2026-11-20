from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from services.io_slots import InputSlot, JobFileType, OutputSlot
from services.jobs._base import L, S, AbstractJobParams, form
from services.models_base import JobCategory, JobType


class CtfRefineParams(AbstractJobParams):
    """
    relion_tomo_refine_ctf: each tilt's defocus and astigmatism (and, if asked, its contrast scale and higher-order
    aberrations) refined by comparing the tilt images with a reference projected through the particles' poses.

    It reads the particles' poses and tomograms from an optimisation set (a Refine 3D's), and the reference as two half
    maps at the tilt series' own pixel size, with their mask and their FSC (a Reconstruct particle at binning 1, a Mask
    and a PostProcess made from it): RELION compares the reference with the tilt images voxel for voxel and does not
    rescale it. It writes an optimisation set with the same particles and refined tomograms, which a Refine 3D reads
    as it is: the images stay, since a Warp export's carry no CTF and RELION applies it from the tomograms at refine.
    The driver reports every tilt's defocus and astigmatism before and after, and fails on a defocus at or below zero,
    a move beyond `max_defocus_change_um` or a negative contrast scale.
    """

    job_type: JobType = Field(default=JobType.CTF_REFINE)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    USER_PARAMS: ClassVar[set[str]] = {
        "do_defocus",
        "defocus_range_ang",
        "defocus_steps",
        "kmin_ang",
        "regularise",
        "reg_lambda",
        "reset_to_common",
        "do_scale",
        "per_frame_scale",
        "per_tomogram_scale",
        "even_aberrations",
        "even_order",
        "odd_aberrations",
        "odd_order",
        "box_size",
        "max_defocus_change_um",
        "threads",
        "other_args",
    }
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = ("Defocus", "Scale", "Aberrations", "Box & checks", "Compute")
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_optimisation", accepts=[JobFileType.OPTIMISATION_SET_STAR], preferred_source="refine3d"),
        InputSlot(key="input_half_map", accepts=[JobFileType.HALF_MAP], preferred_source="reconstructParticle"),
        InputSlot(key="input_mask", accepts=[JobFileType.MASK], preferred_source="maskCreate"),
        InputSlot(key="input_fsc", accepts=[JobFileType.FSC_STAR], preferred_source="postprocess"),
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(
            key="output_optimisation", produces=JobFileType.OPTIMISATION_SET_STAR, path_template="optimisation_set.star"
        )
    ]

    # Defocus
    do_defocus: bool = Field(
        default=True,
        description="Refine each tilt's defocus and astigmatism (--do_defocus)",
        title="Refine defocus",
        json_schema_extra=form("Defocus"),
    )
    defocus_range_ang: float = Field(
        default=3000.0,
        gt=0,
        description="Defocus offsets searched around each tilt's current value: ± this (Å; --d0, --d1)",
        title="Search range ±",
        json_schema_extra=form("Defocus", S, "Å"),
    )
    defocus_steps: int = Field(
        default=100,
        ge=2,
        description="Defocus offsets tested across the range (--ds)",
        title="Steps",
        json_schema_extra=form("Defocus", S),
    )
    kmin_ang: float = Field(
        default=30.0,
        gt=0,
        description="Lowest spatial frequency the fit uses (Å; --kmin)",
        title="Lowest frequency",
        json_schema_extra=form("Defocus", S, "Å"),
    )
    regularise: bool = Field(
        default=False,
        description="Hold the tilts' defoci close to each other, weighted by lambda (--do_reg_defocus): late, "
        "low-dose tilts then cannot run off on their own",
        title="Regularise across tilts",
        json_schema_extra=form("Defocus"),
    )
    reg_lambda: float = Field(
        default=0.001,
        gt=0,
        description="Weight of the regularisation across tilts (--lambda)",
        title="Lambda",
        json_schema_extra=form("Defocus", S),
    )
    reset_to_common: bool = Field(
        default=False,
        description="Start every tilt of a tilt series from one common CTF before the local search (--reset_to_common)",
        title="Reset to common",
        json_schema_extra=form("Defocus"),
    )

    # Contrast scale
    do_scale: bool = Field(
        default=False,
        description="Refine the contrast scale (--do_scale)",
        title="Refine scale",
        json_schema_extra=form("Scale"),
    )
    per_frame_scale: bool = Field(
        default=False,
        description="One scale per tilt instead of a fit over the dose (--per_frame_scale)",
        title="Per tilt",
        json_schema_extra=form("Scale"),
    )
    per_tomogram_scale: bool = Field(
        default=False,
        description="One scale per tomogram (--per_tomogram_scale; the luminance may become unstable)",
        title="Per tomogram",
        json_schema_extra=form("Scale"),
    )

    # Higher-order aberrations
    even_aberrations: bool = Field(
        default=False,
        description="Refine even higher-order aberrations (--do_even_aberrations)",
        title="Even aberrations",
        json_schema_extra=form("Aberrations"),
    )
    even_order: int = Field(
        default=4,
        ge=2,
        description="Highest order of the even aberrations (--ne)",
        title="Even up to",
        json_schema_extra=form("Aberrations", S),
    )
    odd_aberrations: bool = Field(
        default=False,
        description="Refine odd higher-order aberrations (--do_odd_aberrations)",
        title="Odd aberrations",
        json_schema_extra=form("Aberrations"),
    )
    odd_order: int = Field(
        default=3,
        ge=1,
        description="Highest order of the odd aberrations (--no)",
        title="Odd up to",
        json_schema_extra=form("Aberrations", S),
    )

    # Box and checks
    box_size: int = Field(
        default=0,
        ge=0,
        description="Box cut around each particle in the tilt images, in pixels at the tilt series' pixel size (--b); "
        "0 = the reference's box. A box larger than the particle keeps the signal the CTF spreads out of it",
        title="Box",
        json_schema_extra=form("Box & checks", S, "px"),
    )
    max_defocus_change_um: float = Field(
        default=1.0,
        ge=0,
        description="Fail when a tilt's defocus moves more than this from its input value (µm); 0 = no limit",
        title="Largest move",
        json_schema_extra=form("Box & checks", S, "µm"),
    )

    threads: int = Field(
        default=8, ge=1, description="OpenMP threads (--j)", title="Threads", json_schema_extra=form("Compute", S)
    )
    other_args: str = Field(
        default="",
        description="Extra relion_tomo_refine_ctf flags, appended verbatim",
        title="Extra arguments",
        json_schema_extra=form("Compute", L),
    )

    DEFOCUS_ONLY: ClassVar[frozenset[str]] = frozenset(
        {"defocus_range_ang", "defocus_steps", "kmin_ang", "regularise", "reset_to_common"}
    )

    def run_problems(self) -> list[str]:
        # What drivers/ctf_refine.py refuses before RELION runs, said before the submit.
        if not (self.do_defocus or self.do_scale or self.even_aberrations or self.odd_aberrations):
            return ["nothing to refine: turn on defocus, scale or aberrations"]
        return []

    def shows_param(self, name: str) -> bool:
        if name in self.DEFOCUS_ONLY:
            return self.do_defocus
        if name == "reg_lambda":
            return self.do_defocus and self.regularise
        if name in ("per_frame_scale", "per_tomogram_scale"):
            return self.do_scale
        if name == "even_order":
            return self.even_aberrations
        if name == "odd_order":
            return self.odd_aberrations
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

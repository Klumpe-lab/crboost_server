from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from services.io_slots import InputSlot, JobFileType, OutputSlot
from services.jobs._base import L, S, AbstractJobParams, form
from services.models_base import JobCategory, JobType


class PostProcessParams(AbstractJobParams):
    """
    relion_postprocess on a refinement's unfiltered half maps and a solvent mask: the gold-standard FSC with the
    mask (corrected for its effect by phase randomisation), the unmasked FSC beside it, and the sum of the half maps
    sharpened by a B-factor fitted from the Guinier plot or given. The resolution it reports is the masked one; the
    driver says so in its last line.
    """

    job_type: JobType = Field(default=JobType.POSTPROCESS)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    USER_PARAMS: ClassVar[set[str]] = {
        "auto_bfac",
        "autob_lowres",
        "adhoc_bfac",
        "skip_fsc_weighting",
        "low_pass",
        "other_args",
    }
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = ("Sharpening", "Filtering", "Extra")
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_half_map", accepts=[JobFileType.HALF_MAP], preferred_source="refine3d"),
        InputSlot(key="input_mask", accepts=[JobFileType.MASK], preferred_source="maskCreate"),
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(key="output_map", produces=JobFileType.REFERENCE_MAP, path_template="postprocess.mrc"),
        # The FSC of these half maps in this mask: how far CTF refinement trusts them as its reference.
        OutputSlot(key="output_fsc", produces=JobFileType.FSC_STAR, path_template="postprocess.star"),
    ]

    auto_bfac: bool = Field(
        default=True,
        description="Fit the sharpening B-factor from the Guinier plot",
        title="Fit B-factor",
        json_schema_extra=form("Sharpening"),
    )
    autob_lowres: float = Field(
        default=10.0,
        gt=0,
        description="Lowest resolution used in the B-factor fit (Å); higher frequencies only",
        title="Fit from",
        json_schema_extra=form("Sharpening", S, "Å"),
    )
    adhoc_bfac: float = Field(
        default=0.0,
        le=0,
        description="B-factor to sharpen with when auto_bfac is off (Å², negative; 0 = none)",
        title="B-factor",
        json_schema_extra=form("Sharpening", S, "Å²"),
    )
    skip_fsc_weighting: bool = Field(
        default=False,
        description="No FSC weighting of the map; low-pass it at low_pass instead",
        title="Skip FSC weighting",
        json_schema_extra=form("Filtering"),
    )
    low_pass: float = Field(
        default=0.0,
        ge=0,
        description="Low-pass filter (Å) with skip_fsc_weighting; 0 = none",
        title="Low-pass",
        json_schema_extra=form("Filtering", S, "Å"),
    )
    other_args: str = Field(
        default="",
        description="Extra relion_postprocess flags, appended verbatim",
        title="Extra arguments",
        json_schema_extra=form("Extra", L),
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        return [
            ("fn_in", str(self.paths.get("input_half_map", ""))),
            ("fn_mask", str(self.paths.get("input_mask", ""))),
        ]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"

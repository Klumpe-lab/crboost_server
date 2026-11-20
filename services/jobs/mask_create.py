from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from services.io_slots import InputSlot, JobFileType, OutputSlot
from services.jobs._base import L, S, AbstractJobParams, form
from services.models_base import JobCategory, JobType


class MaskCreateParams(AbstractJobParams):
    """
    A soft solvent mask from a map: the map low-pass filtered, thresholded, the binary mask grown by `extend_px`
    and given a cosine edge `soft_edge_px` wide (relion_mask_create). The threshold is the low-passed map's own
    value: a fraction of its maximum unless an absolute threshold is given, because maps from different jobs and
    scales share no greyscale (the 0.15 of the tutorials selects nothing on ours). The driver writes the map's
    range, the threshold it used and the fraction of the box the mask encloses into `mask_stats.json`.

    Radial limits keep a shell of the mask (a capsid's outer layer): between the radii, measured from the box centre,
    the mask is RELION's; beyond each limit it falls to zero over the soft edge, as RELION grows its own edge outwards.
    """

    job_type: JobType = Field(default=JobType.MASK_CREATE)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    USER_PARAMS: ClassVar[set[str]] = {
        "lowpass_ang",
        "threshold",
        "threshold_fraction",
        "extend_px",
        "soft_edge_px",
        "inner_radius_ang",
        "outer_radius_ang",
        "other_args",
    }
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = ("Threshold", "Edge", "Radius", "Extra")
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_map", accepts=[JobFileType.REFERENCE_MAP], preferred_source="refine3d")
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(key="output_mask", produces=JobFileType.MASK, path_template="mask.mrc")
    ]

    lowpass_ang: float = Field(
        default=15.0,
        gt=0,
        description="Low-pass filter on the map before thresholding (Å)",
        title="Low-pass",
        json_schema_extra=form("Threshold", S, "Å"),
    )
    threshold: float = Field(
        default=0.0,
        description="Absolute threshold on the low-passed map; 0 = threshold_fraction of its maximum",
        title="Threshold",
        json_schema_extra=form("Threshold", S),
    )
    threshold_fraction: float = Field(
        default=0.4,
        gt=0,
        lt=1,
        description="Threshold as a fraction of the low-passed map's maximum (used while threshold is 0)",
        title="Threshold fraction",
        json_schema_extra=form("Threshold", S),
    )
    extend_px: int = Field(
        default=3,
        ge=0,
        description="Grow the binary mask by this many pixels",
        title="Extend",
        json_schema_extra=form("Edge", S, "px"),
    )
    soft_edge_px: int = Field(
        default=6,
        ge=0,
        description="Width of the cosine soft edge (pixels)",
        title="Soft edge",
        json_schema_extra=form("Edge", S, "px"),
    )
    inner_radius_ang: float = Field(
        default=0.0,
        ge=0,
        description="Zero the mask inside this radius from the box centre (Å); 0 = no inner limit",
        title="Inner radius",
        json_schema_extra=form("Radius", S, "Å"),
    )
    outer_radius_ang: float = Field(
        default=0.0,
        ge=0,
        description="Zero the mask outside this radius from the box centre (Å); 0 = no outer limit",
        title="Outer radius",
        json_schema_extra=form("Radius", S, "Å"),
    )
    other_args: str = Field(
        default="",
        description="Extra relion_mask_create flags, appended verbatim",
        title="Extra arguments",
        json_schema_extra=form("Extra", L),
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        return [("in_3dref", str(self.paths.get("input_map", "")))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"

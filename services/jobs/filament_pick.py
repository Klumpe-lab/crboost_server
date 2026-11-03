from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from services.io_slots import InputSlot, JobFileType, OutputSlot
from services.jobs._base import AbstractJobParams
from services.models_base import JobCategory, JobType


class FilamentPickParams(AbstractJobParams):
    """Points along the filaments of one feature of a Segment job (Ais `pick -filament`), each oriented
    along its filament, as a candidate set Warp Export reads."""

    job_type: JobType = Field(default=JobType.FILAMENT_PICK)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = True

    USER_PARAMS: ClassVar[set[str]] = {
        "feature",
        "spacing_ang",
        "binning",
        "min_volume_nm3",
        "min_length_ang",
        "min_curvature_radius_ang",
        "min_angle_from_z_deg",
    }

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_segmentations", accepts=[JobFileType.SEGMENTATIONS], preferred_source="segment")
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.CANDIDATES_STAR, path_template="candidates.star"),
        OutputSlot(
            key="optimisation_set",
            produces=JobFileType.OPTIMISATION_SET_STAR,
            path_template="optimisation_set.star",
            coordinates_only=True,
        ),
    ]

    CONFIG_PREAMBLE: ClassVar[str] = (
        "Ais traces the feature's volumes where p > 0.5: each connected piece is thinned to its centre line, cut "
        "where lines meet, and resampled every *spacing* Å. Ais drops pieces smaller than the minimum volume and "
        "pieces (and side branches) shorter than the minimum length.\n\n"
        "Each point is oriented along its filament: tilt and psi lay the reference's Z axis along the line and are "
        "also written as priors; rot is random (searched later); the polarity is unknown (psi flip ratio 0.5). "
        "Each filament is a helical tube, and a tube's points all go to the same half-set.\n\n"
        "A filament is **marked**, not deleted, when it is shorter than the minimum length (the pieces left where "
        "Ais cut at a junction), bends tighter than the minimum radius (a blob), runs within the minimum angle of "
        "Z (missing-wedge streaks), or its points are unevenly spaced. `filaments_all.star` keeps every point with "
        "its `cbMarkReason`; `candidates.star` holds the unmarked ones. The points sit on the reconstruction the "
        "segmented tomograms trace to."
    )

    SLURM_NOTE: ClassVar[tuple[str, str]] = (
        "One CPU job for all tomograms: no GPU needed",
        "Ais runs one process per CPU of the job. 139 tomograms of 1024 × 1024 × 512 took 6.5 min with 4 "
        "processes at binning 2. Clear the GPU field: the job never uses one.",
    )

    feature: str = Field(
        default="microtubule",
        title="Feature",
        description="The Segment job's feature to trace (a name its segmentations.json lists)",
    )
    spacing_ang: float = Field(
        default=82.0,
        gt=0,
        title="Spacing (Å)",
        description="Distance between points along a filament. 82 Å is one αβ-tubulin dimer.",
    )
    binning: int = Field(
        default=2,
        ge=1,
        le=8,
        title="Binning",
        description="Ais traces the volumes binned by this factor (mean of each block): 2 is about four times "
        "faster than 1 and plenty for a 25 nm tube.",
    )
    min_volume_nm3: float = Field(
        default=10000.0,
        ge=0,
        title="Min volume (nm³)",
        description="Ais drops connected pieces smaller than this before tracing. 10 000 nm³ is about 20–30 nm "
        "of microtubule.",
    )
    min_length_ang: float = Field(
        default=1000.0,
        ge=0,
        title="Min length (Å)",
        description="Ais drops pieces and prunes side branches shorter than this; filaments still shorter after "
        "the cut at junctions are marked.",
    )
    min_curvature_radius_ang: float = Field(
        default=1000.0,
        ge=0,
        title="Min bend radius (Å)",
        description="A filament whose median radius of curvature is smaller is marked: a blob, not a tube.",
    )
    min_angle_from_z_deg: float = Field(
        default=30.0,
        ge=0,
        le=90,
        title="Min angle from Z (°)",
        description="A filament whose median direction lies closer to Z than this is marked: missing-wedge "
        "streaks and lamella edges run along Z.",
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        return [("in_mic", str(self.paths.get("input_segmentations", "")))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        # `ais` ships in the easymode image.
        return "easymode"

"""Subset: a finished per-tilt-series output cut down to the tilt series chosen in its tab.

Written at submit (RUNS_INLINE; the inline writer is services.subsets.write_subset_star): the cut
star's rows for the chosen tilt series, its path columns made absolute, so the jobs after it
process only those. No SLURM job and no driver; the output it cuts has finished, so its consumers
wait for nothing. Made from the tab of the job it cuts, which sets its input; automatic wiring
never chooses one.
"""

from __future__ import annotations

from typing import ClassVar, Final

from pydantic import Field

from services.io_slots import InputSlot, JobFileType, OutputSlot
from services.jobs._base import AbstractJobParams
from services.models_base import JobCategory, JobType

# The outputs a Subset can cut: per-tilt-series stars whose rows the jobs after them take their
# tilt series from. Alignment and what comes before it take theirs from the registry instead.
CUT_TYPES: Final[tuple[JobFileType, ...]] = (
    JobFileType.ALIGNED_TILT_SERIES_STAR,
    JobFileType.TS_CTF_TILT_SERIES_STAR,
    JobFileType.TOMOGRAMS_STAR,
    JobFileType.DENOISED_TOMOGRAMS_STAR,
)


class SubsetParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.SUBSET)

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    RUNS_INLINE: ClassVar[bool] = True  # the star is written at submit; see _submit_chain

    USER_PARAMS: ClassVar[set[str]] = {"ts_names"}

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [InputSlot(key="input_star", accepts=list(CUT_TYPES))]
    # Its output has the type of what it cuts, so the resolver takes it from the instance
    # (PathResolutionService._get_output_schema), not from the class.
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = []

    ts_names: list[str] = Field(default_factory=list, description="The tilt series it keeps, by rlnTomoName")

    def get_tool_name(self) -> str:
        return "crboost"

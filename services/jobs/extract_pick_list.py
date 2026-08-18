from __future__ import annotations
from typing import ClassVar
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobCategory, JobType


class ExtractPickListParams(AbstractJobParams):
    """Subtomogram extraction of ONE pick list (roadmap 07).

    Identity: one instance per pick list, keyed on the `(species_id, tomo_name, slug)`
    triple through `services.particles.list_ref.extract_pick_list_instance_id`. A slug
    alone does NOT identify a list -- every hand-picked list is minted `slug="manual"` --
    so the triple is the key here exactly as it is for `pick_list_producer_id`.

    NOT a scheme/roster job. Two things keep it out of the pipeline machinery, and both
    are load-bearing rather than cosmetic:

      * `IS_INTERACTIVE = True` -- nothing filters `state.jobs` by pipeline membership,
        so without this flag `sync_all_jobs`' orphan sweep would reset the instance to
        SCHEDULED and null its `slurm_job_id` on every 3 s tick, `pipeline_monitor`'s
        crash recovery would hand it to `deploy_and_run_scheme` as a real scheme job,
        `get_pipeline_overview` would count it into the roster's done/total forever, and
        the STOP-ON-FAIL heuristic would see a live job that is not part of any run.
      * `phase=None` on its JobSpec row -- keeps it out of the roster palette and the
        Species page's Jobs tab.

    No INPUT_SCHEMA: `get_driver_context` re-resolves every declared slot and exits the
    driver on failure, so the two source stars ride as plain fields instead. No
    OUTPUT_SCHEMA: otherwise each per-list instance would join the resolver's output
    index as a producer candidate for other jobs' input slots.

    Geometry defaults are deliberately "unset" sentinels, not guesses: `box_size` /
    `binning` are 0 until a submit writes the values the user (or the species'
    committed geometry) supplied. Nothing may extract with a zero box.
    """

    job_type: JobType = Field(default=JobType.EXTRACT_PICK_LIST)

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_INTERACTIVE: ClassVar[bool] = True

    USER_PARAMS: ClassVar[set[str]] = {
        "box_size",
        "binning",
        "crop_size",
        "max_dose",
        "min_frames",
        "do_stack2d",
        "do_float16",
    }

    # ── which list this instance cuts (species_id lives on AbstractJobParams) ──
    tomo_name: str = Field(default="", description="Tomogram the pick list belongs to")
    list_slug: str = Field(default="", description="Pick-list slug; unique only within (species, tomogram)")

    # ── source: exactly one of these is set, enforced at the submit site ──
    candidate_optset: str = Field(default="", description="Candidate-extract optimisation set to cut from")
    tomograms_star: str = Field(default="", description="tomograms.star to cut from when the species has no CE job")
    list_star: str = Field(default="", description="The pick list's star file (the positions to extract)")

    # ── geometry (mirrors backend.extract_pick_list's signature exactly) ──
    box_size: int = Field(default=0, ge=0, description="Extraction box size in pixels (0 = not yet submitted)")
    binning: float = Field(default=0.0, ge=0.0, description="Extraction binning (0 = not yet submitted)")
    crop_size: int = Field(default=0, ge=0, description="Cropped box size in pixels; 0 = no crop")
    max_dose: float = Field(default=-1.0, description="Dose cutoff; <= 0 = no cutoff")
    min_frames: int = Field(default=1, ge=1, description="Minimum contributing frames per particle")
    do_stack2d: bool = Field(default=True, description="Write 2D stacks")
    do_float16: bool = Field(default=True, description="Write float16 output")

    # ── failure text (roadmap 07 S3) ──
    # Deliberately OUTSIDE USER_PARAMS: USER_PARAMS fields are frozen once the job
    # leaves SCHEDULED/FAILED, and the whole point of this field is to be written the
    # moment an extraction fails, so a failure survives the dialog that launched it.
    last_error: str = Field(default="", description="Failure text from the driver's result.json, if any")

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"

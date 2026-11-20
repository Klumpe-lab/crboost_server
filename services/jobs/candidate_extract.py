from __future__ import annotations
from typing import Any, ClassVar
from pydantic import Field, model_validator

from services.computing.slurm_service import SlurmConfig
from services.jobs._base import L, M, S, SPECIES_GROUP, AbstractJobParams, ExtractionCutoffMethod, form
from services.models_base import JobType, JobCategory, PickSource
from services.io_slots import InputSlot, OutputSlot, JobFileType

# The parameters each source reads; the other source's are hidden on the job page (`shows_param`).
TM_PARAMS = frozenset(
    {
        "max_num_particles",
        "cutoff_method",
        "cc_threshold",
        "expected_false_positives",
        "apix_score_map",
        "score_filter_method",
        "score_filter_value",
    }
)
SEGMENTATION_PARAMS = frozenset({"feature"})


class CandidateExtractPytomParams(AbstractJobParams):
    """One species' picks from evidence over the tomograms: the peaks of a Template Match job's score maps (source
    template matching), or the pieces of one feature's probability volumes (source segmentation). Both write
    candidates.star, tomograms.star and a coordinates-only optimisation set."""

    job_type: JobType = Field(default=JobType.TEMPLATE_EXTRACT_PYTOM)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[set[str]] = {
        "source",
        "feature",
        "particle_diameter_ang",
        "max_num_particles",
        "cutoff_method",
        "cc_threshold",
        "expected_false_positives",
        "apix_score_map",
        "score_filter_method",
        "score_filter_value",
        "array_throttle",
    }
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = (
        "Source",
        "Segmentation",
        "Detection",
        "Orientation",
        "Pick threshold",
        SPECIES_GROUP,
        "Score map",
    )

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(
            key="input_tm_job",
            accepts=[JobFileType.TM_RESULTS_DIR],
            preferred_source="templatematching",
            when="from_template_matching",
        ),
        InputSlot(
            key="input_tomograms",
            accepts=[JobFileType.TOMOGRAMS_STAR, JobFileType.DENOISED_TOMOGRAMS_STAR],
            preferred_source="templatematching",
            required=True,
            when="from_template_matching",
        ),
        # A Segment job's volumes, or imported ones: the evidence of the segmentation source.
        InputSlot(
            key="input_segmentations",
            accepts=[JobFileType.SEGMENTATIONS],
            preferred_source="segment",
            when="from_segmentation",
            beside=True,
        ),
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

    source: PickSource = Field(
        default=PickSource.TEMPLATE_MATCHING,
        description="Template matching: the peaks of a Template Match job's score maps, at least a particle apart. "
        "Segmentation: the pieces of one feature's probability volumes (a Segment job's, or imported segmentations), "
        "with no template and no GPU.",
        title="Source", json_schema_extra=form("Source", M),
    )
    # Species-derived (the species' segmentation feature); empty is absent and stated on the page, never guessed.
    feature: str = Field(
        default="", description="The feature to pick, one of those the segmentations hold.",
        title="Feature", json_schema_extra=form("Segmentation", L),
    )

    # Particle params
    particle_diameter_ang: float = Field(
        default=200.0,
        description="Particle diameter; also the least distance between two picks (non-maximum suppression).",
        title="Diameter",
        json_schema_extra=form(SPECIES_GROUP, S, "Å"),
    )
    # ge=1: PyTOM's `-n` is a required, strictly-positive cap ("error: -n must be larger
    # than 0", argparse exit 2); without the bound a stored 0 only fails on the GPU node.
    # There is no "0 = uncapped" spelling; leave the cap high instead.
    max_num_particles: int = Field(
        default=1500, ge=1, description="Hard cap on candidates per tilt-series after the cutoff is applied.",
        title="Max picks / TS", json_schema_extra=form("Pick threshold", S),
    )

    # Thresholding strategy + per-strategy values.
    #
    # The two strategies have incomparable scales: under FALSE_POSITIVES the value is
    # "expected FPs per tomogram" (e.g. 1.0 = strict), under MANUAL it is a raw LCC
    # threshold (e.g. 0.1). A shared field would carry 1.0 over to MANUAL on a dropdown
    # flip (CC ≥ 1.0 → 0 picks), so each strategy has its own backing field and the UI
    # shows only the one matching `cutoff_method`. Older project files carry a single
    # `cutoff_value`; `_migrate_legacy_cutoff_value` below routes it on load.
    cutoff_method: ExtractionCutoffMethod = Field(
        default=ExtractionCutoffMethod.FALSE_POSITIVES,
        description="ManualCutOff: a raw LCC threshold (e.g. 0.10). NumberOfFalsePositives: PyTOM fits the score "
        "map's noise and takes the threshold giving that many false positives per tomogram.",
        title="Strategy",
        json_schema_extra=form("Pick threshold", M),
    )
    cc_threshold: float = Field(
        default=0.1, description="Manual LCC threshold; peaks with score ≥ this are kept. Typical 0.05–0.20.",
        title="LCC threshold", json_schema_extra=form("Pick threshold", S),
    )
    expected_false_positives: float = Field(
        default=1.0,
        description="Expected false positives per tomogram (FALSE_POSITIVES strategy). Strict=1, moderate=10, "
        "loose=100.", title="Expected FPs / TS", json_schema_extra=form("Pick threshold", S),
    )

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_cutoff_value(cls, data: Any) -> Any:
        """Migrate the legacy `cutoff_value` field into the appropriate
        strategy-specific field. Runs before field validation, so the raw
        dict from JSON load is what we mutate.

        Behavior:
          - If the JSON has `cutoff_value` and neither `cc_threshold` nor
            `expected_false_positives` is set, route the value into the
            field that matches `cutoff_method`.
          - If both old and new keys are present, the explicit new field wins.
          - The legacy key is always removed from the dict (Pydantic v2
            BaseModel already defaults to extra='ignore').
        """
        if not isinstance(data, dict):
            return data
        if "cutoff_value" not in data:
            return data
        legacy = data.pop("cutoff_value", None)
        if legacy is None:
            return data
        try:
            legacy_f = float(legacy)
        except (TypeError, ValueError):
            return data
        method_raw = data.get("cutoff_method") or ExtractionCutoffMethod.FALSE_POSITIVES.value
        method_str = method_raw.value if hasattr(method_raw, "value") else str(method_raw)
        if method_str == ExtractionCutoffMethod.MANUAL.value:
            data.setdefault("cc_threshold", legacy_f)
        else:
            data.setdefault("expected_false_positives", legacy_f)
        return data

    # Score map pixel size
    apix_score_map: str = Field(
        default="auto",
        description="Pixel size of the score map. 'auto' reads it from tomograms.star (recommended).",
        title="Pixel size",
        json_schema_extra=form("Score map", M, "Å/px"),
    )

    # Filtering
    score_filter_method: str = Field(default="None", title="Filter", json_schema_extra=form("Score map", M))
    score_filter_value: str = Field(
        default="None",
        description="For tophat: '<connectivity>:<bins>' (e.g. '1:5'). Leave 'None' otherwise.",
        title="Filter value",
        json_schema_extra=form("Score map", M),
    )

    array_throttle: int = Field(
        default=40, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tomogram candidate extraction"
    )

    @property
    def from_segmentation(self) -> bool:
        """Whether the picks come from a feature's segmentations: the job then reads them, and nothing of
        template matching."""
        return PickSource(self.source) == PickSource.SEGMENTATION

    @property
    def from_template_matching(self) -> bool:
        return not self.from_segmentation

    def shows_param(self, name: str) -> bool:
        if name in TM_PARAMS and self.from_segmentation:
            return False
        if name in SEGMENTATION_PARAMS and self.from_template_matching:
            return False
        # The two strategies keep their thresholds apart (incomparable scales): only the chosen one's shows.
        manual = ExtractionCutoffMethod(self.cutoff_method) == ExtractionCutoffMethod.MANUAL
        if name == "cc_threshold":
            return manual
        if name == "expected_false_positives":
            return not manual
        return True

    def settings_reading(self, slots: set[str]) -> dict[str, Any]:
        # Made reading segmentations and no template-matching output: it picks from the segmentations.
        if "input_segmentations" in slots and not slots & {"input_tm_job", "input_tomograms"}:
            return {"source": PickSource.SEGMENTATION}
        return {}

    def run_problems(self) -> list[str]:
        if self.from_segmentation and not self.feature.strip():
            return ["no feature chosen to pick from the segmentations"]
        return []

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        key = "input_segmentations" if self.from_segmentation else "input_tm_job"
        return [("in_mic", str(self.paths.get(key, "")))]

    def _get_queue_options(self) -> list[tuple[str, str]]:
        """
        Override: extract's parent sbatch is a lightweight CPU-only supervisor
        that enumerates tomograms, submits a per-tomogram SLURM array, polls,
        merges per-tomogram particle lists. Per-task resources (also CPU-only,
        small) are consumed by the supervisor when it builds the array sbatch
        -- NOT by this supervisor's own sbatch.
        """
        sup = self.supervisor_slurm()
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
        # Ais picks the segmentation source's pieces; it ships in the easymode image.
        return "easymode" if self.from_segmentation else "pytom"

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"tm_job": "templatematching"}

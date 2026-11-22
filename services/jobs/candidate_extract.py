from __future__ import annotations
from typing import Any, ClassVar
from pydantic import Field, model_validator

from services.computing.slurm_service import SlurmConfig
from services.jobs._base import L, M, S, SPECIES_GROUP, AbstractJobParams, ExtractionCutoffMethod, form
from services.models_base import JobType, JobCategory, PickOrientation, PickSource, SpeciesKind
from services.io_slots import InputSlot, OutputSlot, JobFileType

# The parameters each source reads; the other source's are hidden on the job page (`shows_param`).
TM_PARAMS = frozenset(
    {
        "context",
        "max_num_particles",
        "cutoff_method",
        "cc_threshold",
        "expected_false_positives",
        "apix_score_map",
        "score_filter_method",
        "score_filter_value",
    }
)
# Of the segmentation source's, what only a particle's picks read and what only a filament's do; the feature, the p
# threshold and the binning are both shapes'.
PARTICLE_PARAMS = frozenset({"min_piece", "one_per_piece", "orientation"})
FILAMENT_PARAMS = frozenset(
    {"spacing_ang", "min_volume_nm3", "min_length_ang", "min_curvature_radius_ang", "min_angle_from_z_deg"}
)
SEGMENTATION_PARAMS = frozenset({"feature", "shape", "p_threshold", "binning"}) | PARTICLE_PARAMS | FILAMENT_PARAMS


class CandidateExtractPytomParams(AbstractJobParams):
    """One species' picks from evidence over the tomograms: the peaks of a Template Match job's score maps (source
    template matching), or one feature's probability volumes (source segmentation): their pieces for a particle,
    points along their traced lines for a filament (the species' shape). All write candidates.star, tomograms.star
    and a coordinates-only optimisation set."""

    job_type: JobType = Field(default=JobType.TEMPLATE_EXTRACT_PYTOM)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[set[str]] = {
        "source",
        "context",
        "feature",
        "p_threshold",
        "min_piece",
        "one_per_piece",
        "binning",
        "orientation",
        "shape",
        "spacing_ang",
        "min_volume_nm3",
        "min_length_ang",
        "min_curvature_radius_ang",
        "min_angle_from_z_deg",
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
        "Filaments",
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
        # A Segment job's volumes, or imported ones: the evidence of the segmentation source, and the context every
        # pick records (of template matching's, while Context is on).
        InputSlot(
            key="input_segmentations",
            accepts=[JobFileType.SEGMENTATIONS],
            preferred_source="segment",
            when="reads_segmentations",
            required_when="from_segmentation",
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
        title="Source",
        json_schema_extra=form("Source", M),
    )
    # A job submitted before Context existed ran without it, and loads with it off (ProjectState.from_data).
    context: bool = Field(
        default=True,
        description="Record at every pick what the segmentations input says around it: each feature's mean p "
        "inside the particle, and its distance to the feature's surface, as columns the kitchen filters on. Only "
        "segmentations of the picks' own reconstruction are read. No: the input is not read.",
        title="Context",
        json_schema_extra=form("Source"),
    )
    # Species-derived (the species' segmentation feature); empty is absent and stated on the page, never guessed.
    feature: str = Field(
        default="",
        description="The feature to pick, one of those the segmentations hold.",
        title="Feature",
        json_schema_extra=form("Segmentation", L),
    )

    # Detection on the feature's volumes (Ais's blob pick), and the picks' starting angles.
    p_threshold: float = Field(
        default=0.5,
        gt=0.0,
        lt=1.0,
        description="A voxel is the feature where its p is at least this; on a binned volume, the mean p of its "
        "block. 0.5 is the network's own decision, and the fill the wall shows.",
        title="p threshold",
        json_schema_extra=form("Detection", S),
    )
    min_piece: float = Field(
        default=0.24,
        ge=0.0,
        description="Pieces smaller than this fraction of one particle (a sphere of the diameter) are left out. "
        "easymode's ribosome example keeps pieces from 0.24 of one.",
        title="Min piece",
        json_schema_extra=form("Detection", S, "× Ø"),
    )
    one_per_piece: bool = Field(
        default=False,
        description="One pick at the centre of each piece, instead of the points deepest inside the pieces, at "
        "least a diameter apart. For particles that never touch. Only these picks can take a piece's axis as their "
        "angles; the others start from random angles.",
        title="One per piece",
        json_schema_extra=form("Detection"),
    )
    binning: int = Field(
        default=2,
        ge=1,
        le=8,
        description="The volumes are binned by this (the mean of each block) before detection: faster, and "
        "coarser by as much. The score is read on the volume as it is.",
        title="Binning",
        json_schema_extra=form("Detection", S),
    )
    orientation: PickOrientation = Field(
        default=PickOrientation.RANDOM,
        description="Random: uniform over all orientations, seeded per job; refinement then searches globally. Long "
        "or short axis: the reference's Z along the piece's longest axis (rods) or shortest (discs), rot random; "
        "a piece too round for its axis takes random angles.",
        title="Angles",
        json_schema_extra=form("Orientation", M),
    )

    # A filament's picks (Ais's tracer): which traced lines are kept. The spacing is the species' repeat.
    min_volume_nm3: float = Field(
        default=10000.0,
        ge=0,
        description="Connected pieces of the feature smaller than this are not traced. 10 000 nm³ is about 20–30 nm "
        "of microtubule.",
        title="Min volume",
        json_schema_extra=form("Filaments", S, "nm³"),
    )
    min_length_ang: float = Field(
        default=1000.0,
        ge=0,
        description="Pieces and side branches shorter than this are not traced; filaments still shorter after the "
        "cut where lines meet are marked.",
        title="Min length",
        json_schema_extra=form("Filaments", S, "Å"),
    )
    min_curvature_radius_ang: float = Field(
        default=1000.0,
        ge=0,
        description="A filament whose median radius of curvature is smaller is marked: a blob, not a tube.",
        title="Min bend radius",
        json_schema_extra=form("Filaments", S, "Å"),
    )
    min_angle_from_z_deg: float = Field(
        default=30.0,
        ge=0,
        le=90,
        description="A filament whose median direction lies closer to Z than this is marked: missing-wedge streaks "
        "and lamella edges run along Z.",
        title="Min angle from Z",
        json_schema_extra=form("Filaments", S, "°"),
    )

    # Species-derived: the shape, a filament's spacing, the diameter.
    shape: SpeciesKind = Field(
        default=SpeciesKind.PARTICLE,
        description="Particle: a pick in each piece of the feature (Detection). Filament: the feature's pieces are "
        "thinned to their centre lines, cut where lines meet, and a pick placed every spacing along each; a pick "
        "lies along its line (tilt and psi, also written as priors), its spin about the line is random and its "
        "direction along it unknown; a filament's picks share a half-set, and a filament failing the Filaments "
        "rules is marked whole (filaments_all.star keeps every point, candidates.star the unmarked).",
        title="Shape",
        json_schema_extra=form(SPECIES_GROUP, M),
    )
    spacing_ang: float = Field(
        default=0.0,
        ge=0,
        description="The distance between two picks along a filament: the lattice's repeat, so no pick sits "
        "between two units (82 Å is one αβ-tubulin dimer). 0: not set, and Run refuses.",
        title="Spacing",
        json_schema_extra=form(SPECIES_GROUP, S, "Å"),
    )
    particle_diameter_ang: float = Field(
        default=200.0,
        description="Particle diameter, or a filament's tube; for particles also the least distance between two "
        "picks (non-maximum suppression).",
        title="Diameter",
        json_schema_extra=form(SPECIES_GROUP, S, "Å"),
    )
    # ge=1: PyTOM's `-n` is a required, strictly-positive cap ("error: -n must be larger
    # than 0", argparse exit 2); without the bound a stored 0 only fails on the GPU node.
    # There is no "0 = uncapped" spelling; leave the cap high instead.
    max_num_particles: int = Field(
        default=1500,
        ge=1,
        description="Hard cap on candidates per tilt-series after the cutoff is applied.",
        title="Max picks / TS",
        json_schema_extra=form("Pick threshold", S),
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
        default=0.1,
        description="Manual LCC threshold; peaks with score ≥ this are kept. Typical 0.05–0.20.",
        title="LCC threshold",
        json_schema_extra=form("Pick threshold", S),
    )
    expected_false_positives: float = Field(
        default=1.0,
        description="Expected false positives per tomogram (FALSE_POSITIVES strategy). Strict=1, moderate=10, "
        "loose=100.",
        title="Expected FPs / TS",
        json_schema_extra=form("Pick threshold", S),
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

    @property
    def reads_segmentations(self) -> bool:
        """Whether the job reads its segmentations input: the evidence it picks from, or template matching's
        context while Context is on."""
        return self.from_segmentation or bool(self.context)

    @property
    def traces_filaments(self) -> bool:
        """Whether the picks are points along a feature's traced lines (the segmentation source, a filament)."""
        return self.from_segmentation and SpeciesKind(self.shape) == SpeciesKind.FILAMENT

    @property
    def effective_orientation(self) -> PickOrientation:
        """The angles the segmentation source gives its picks: a piece's axis needs one pick per piece, so the
        distance-transform placement always takes random angles."""
        return PickOrientation(self.orientation) if self.one_per_piece else PickOrientation.RANDOM

    def shows_param(self, name: str) -> bool:
        if name in TM_PARAMS and self.from_segmentation:
            return False
        if name in SEGMENTATION_PARAMS and self.from_template_matching:
            return False
        if name in FILAMENT_PARAMS:
            return self.traces_filaments
        if name in PARTICLE_PARAMS and self.traces_filaments:
            return False
        if name == "orientation":
            return self.one_per_piece
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
        problems = []
        if self.from_segmentation and not self.feature.strip():
            problems.append("no feature chosen to pick from the segmentations")
        if self.traces_filaments and self.spacing_ang <= 0:
            problems.append(
                "no spacing between a filament's picks: set it here, or the repeat on the Species page for new jobs"
            )
        return problems

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

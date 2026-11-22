"""The verbs' inputs, one pydantic model each (FastAPI and the MCP tool list read their JSON schemas), and the
shape of an open decision a result carries (`Need`)."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema

_PROJECT = "The project: its absolute path, or its folder name under one of the user's project bases."
_JOB = "The job's instance id on the roster, e.g. `aligntiltsWarp` or `templatematching__2` (get_project lists them)."


class _In(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NoInput(_In):
    pass


class ProjectIn(_In):
    project: str = Field(description=_PROJECT)


class JobIn(ProjectIn):
    job: str = Field(description=_JOB)


# ── Needs ─────────────────────────────────────────────────────────────────────


class _NeedBase(BaseModel):
    what: str = Field(description="The value, choice or input the call needs")
    why: str


class NeedValue(_NeedBase):
    kind: Literal["value"]


class NeedChoice(_NeedBase):
    kind: Literal["choice"]
    options: list[Any] = Field(default_factory=list)


class NeedPin(_NeedBase):
    kind: Literal["pin"]
    options: list[Any] = Field(default_factory=list)


# An open decision the user has to make; the call refuses until it is made.
Need = Annotated[NeedValue | NeedChoice | NeedPin, Field(discriminator="kind")]


# ── Reads ─────────────────────────────────────────────────────────────────────


class ListProjectsIn(_In):
    base: str | None = Field(None, description="A folder of projects; the server's default project base when omitted")


class JobTasksIn(JobIn):
    state: Literal["ok", "fail", "running", "pending", "skip"] | None = Field(
        None, description="Only the tilt series in this state"
    )
    offset: int = Field(0, ge=0)
    limit: int = Field(50, ge=1, le=500)


class JobLogsIn(JobIn):
    ts: str | None = Field(None, description="A tilt series of an array job: its own task log instead of the job's")
    lines: int = Field(200, ge=1, le=2000, description="Lines from the end of each log")


class JobTypeIn(_In):
    job_type: str = Field(description="A job type, e.g. `aligntiltsWarp` (list_job_types lists them)")


class InputCandidatesIn(JobIn):
    slot: str = Field(description="The input slot, e.g. `input_star` (get_job lists a job's inputs)")


class SpeciesIn(ProjectIn):
    species: str = Field(description="The species' id in this project (list_species lists them)")


class KitchenSubsetIn(SpeciesIn):
    subset: str = Field(description="The subset, by its id or name (list_kitchen_subsets lists them)")


# ── The kitchen: recipes, sources, subsets, sets ──


class KitchenRangeIn(_In):
    kind: Literal["range"] = "range"
    col: str = Field(description="A number column (get_kitchen's number_columns), e.g. sigma")
    lo: float | None = Field(None, description="Keeps values ≥ lo; no lower bound when omitted")
    hi: float | None = Field(None, description="Keeps values ≤ hi; no upper bound when omitted")
    keep_missing: bool = Field(True, description="A pick with no value passes")


class KitchenCategoryIn(_In):
    kind: Literal["category"] = "category"
    col: str = Field(description="A category column (get_kitchen's category_columns), e.g. project")
    allowed: list[str] = Field(description="The values kept, as get_kitchen lists them")


class KitchenSetFilterIn(_In):
    kind: Literal["set"] = "set"
    set_id: str = Field(description="A particle set of the home project, e.g. s003_ribosome")
    mode: Literal["in", "out"] = Field("in", description="in: only its picks; out: none of them")


class KitchenTomogramsIn(_In):
    kind: Literal["tomograms"] = "tomograms"
    excluded: list[str] = Field(description="Reconstructions (frame keys) whose picks fail")


KitchenFilterIn = Annotated[
    KitchenRangeIn | KitchenCategoryIn | KitchenSetFilterIn | KitchenTomogramsIn, Field(discriminator="kind")
]


class _Recipe(_In):
    """Which picks: the filters (all must pass), then per tomogram the post-ops, then the hand verdicts and the
    kitchen's source choices, as the kitchen's filter strip reads them. The user's own strip is not changed."""

    filters: list[KitchenFilterIn] = Field(default_factory=list, description="Every one must pass; none: every pick")
    min_distance_a: float | None = Field(None, gt=0, description="No two picks closer than this (Å), the better σ kept")
    top_n_per_tomogram: int | None = Field(None, ge=1, description="At most this many per tomogram, best σ first")
    in_subsets: list[str] = Field(default_factory=list, description="Only picks in at least one of these subsets")
    not_in_subsets: list[str] = Field(default_factory=list, description="No pick of these subsets")


class GetKitchenIn(SpeciesIn, _Recipe):
    pass


class KitchenSourcesIn(SpeciesIn):
    projects: list[str] | None = Field(
        None,
        description="The projects to look in (paths or folder names); omitted: this one, the projects under the "
        "user's recent project roots, and those the pool already reads, as Add sources lists them",
    )


class KitchenSourceIn(_In):
    project: str = Field(description="The project the picks are in: its absolute path")
    job: str = Field(description="Its template-match picks (a Pick candidates job's instance id) or a pick list")
    species: str | None = Field(
        None, description="The species there; omitted: the one from the same lab-catalog entry (never by name)"
    )


class AddKitchenSourcesIn(SpeciesIn):
    sources: list[KitchenSourceIn] = Field(min_length=1, description="list_kitchen_sources lists them")


class RemoveKitchenSourceIn(SpeciesIn):
    source: str = Field(description="The source, by its key or as <project folder>:<job> (get_kitchen lists them)")


class CreateKitchenSubsetIn(SpeciesIn, _Recipe):
    name: str = Field(description="The new subset's name, unique in this kitchen")
    group: str | None = Field(
        None, description="A group (by name or id): its subsets never share a pick, so adding moves picks into it"
    )


class EditKitchenSubsetIn(SpeciesIn):
    subset: str = Field(description="The subset, by its id or name")
    name: str | None = Field(None, description="A new name")
    group: str | None = Field(None, description="Move it into this group (refused when it shares picks there)")
    ungroup: bool = Field(False, description="Take it out of its group")


class KitchenGroupIn(SpeciesIn):
    group: str = Field(description="The group, by its id or name")


class CreateKitchenGroupIn(SpeciesIn):
    name: str = Field(description="The new group's name, unique in this kitchen")


class RenameKitchenGroupIn(KitchenGroupIn):
    name: str = Field(description="Its new name")


class MakeKitchenSetIn(SpeciesIn, _Recipe):
    name: str | None = Field(
        None, description="The set's name; omitted: the subset's name. Ignored with group (each set takes its subset's)"
    )
    subset: str | None = Field(None, description="Narrow the recipe to this subset, as a subset's Make set does")
    group: str | None = Field(None, description="A set of each subset of this group, the recipe narrowed to each")


class MakeClassSetIn(JobIn):
    classes: list[int] = Field(min_length=1, description="The Class 3D's classes whose particles the set holds")
    name: str = Field(description="The set's name")


class TaskIn(_In):
    task_id: str = Field(description="The id a verb that runs in the background returned")


class InspectDatasetIn(_In):
    directory: str | None = Field(
        None, description="A data folder: its movies, the mdocs beside them or one level down"
    )
    frames_glob: str | None = Field(None, description="The movies, as a glob, instead of a folder")
    mdocs_glob: str | None = Field(None, description="The mdocs, as a glob, instead of a folder")
    mdocs_directory: str | None = Field(None, description="Where the mdocs are, when not beside the movies")
    offset: int = Field(0, ge=0, description="First series listed")
    limit: int = Field(100, ge=1, le=1000, description="Series listed")


# ── Creation ──────────────────────────────────────────────────────────────────


class TomogramSourceIn(_In):
    project: str = Field(description="The project the tomograms come from: " + _PROJECT[len("The project: ") :])
    job: str = Field(description="Its Reconstruct or Denoise Predict job, by instance id (get_project lists them)")
    names: list[str] | None = Field(None, description="The tomograms, by their names there; every one when omitted")


class CreateProjectIn(_In):
    name: str = Field(description="The new project's folder name")
    base: str = Field(description="The folder it is made in; it must not hold a folder of that name")
    directory: str | None = Field(
        None, description="The data folder, as inspect_dataset took it; omit, with no globs, for no raw data"
    )
    frames_glob: str | None = Field(None, description="The movies, as inspect_dataset reported them")
    mdocs_glob: str | None = Field(None, description="The mdocs, as inspect_dataset reported them")
    mdocs_directory: str | None = None
    series: list[str] | None = Field(None, description="The series to import, by name or mdoc path; all when omitted")
    ts_names_by_folder: bool = Field(
        False, description="Name each series after its folder too: folders that reuse series names, in one project"
    )
    source_layer: Literal["stack", "movies"] | None = Field(
        None,
        description="Where every series comes from when the selected series have both SerialEM's stacks and their "
        "movies (inspect_dataset's `layers`): they differ in orientation, so the tilt axis, the hand and the gain "
        "follow the choice. Omit when the data holds one layer",
    )
    shared: bool = Field(False, description="A lab project rather than the user's own")
    gain_reference: str | None = Field(description="The gain reference file, or null for frames already gain-corrected")
    pixel_size_angstrom: float | None = Field(None, description="Å per pixel of the movies")
    acceleration_voltage_kv: float | None = None
    spherical_aberration_mm: float | None = None
    amplitude_contrast: float | None = None
    dose_per_tilt: float | None = Field(None, description="e/Å² per tilt")
    dose_per_tilt_source: Literal["mdoc", "estimated", "user"] = Field(
        "user", description="Where the dose came from: the mdocs, the scan's estimate, or the user"
    )
    tilt_axis_degrees: float | None = None
    detector_dimensions: tuple[int, int] | None = Field(None, description="Detector width and height in pixels")
    invert_tilt_angles: bool | None = Field(None, description="The tilt-angle hand: invert the mdoc tilt angles")
    invert_defocus_hand: bool | None = Field(None, description="The defocus hand")
    eer_fractions_per_frame: int | None = Field(None, description="Raw EER frames per fraction (EER movies only)")
    protocol: str | None = Field(
        None, description="A protocol bundle (name or folder): its species and stages come with the project"
    )
    tomograms_from: list[TomogramSourceIn] | None = Field(
        None,
        description="A project without raw data made from other projects' tomogram jobs, ready for template "
        "matching; their acquisition values come with them (give none)",
    )


# ── Composition ───────────────────────────────────────────────────────────────


class AddJobIn(ProjectIn):
    job_type: str = Field(description="The job type to add (list_job_types)")
    species: str | None = Field(None, description="The species, for a particle job")
    after: str | None = Field(
        None, description="A Subset of this finished job's output (job_type subset), or a job reading this Subset"
    )
    reading: dict[str, str] | None = Field(
        None,
        description="Inputs of the new job set from the start: {slot: source}, sources as get_input_candidates gives",
    )
    clone_of: str | None = Field(None, description="A job of the same type whose settings the new one takes")


class ExcludeTiltSeriesIn(ProjectIn):
    ts: str = Field(description="The tilt series")
    excluded: bool = Field(True, description="False brings it back")


class SetParamsIn(JobIn):
    values: dict[str, Any] = Field(description="{parameter: value}; get_job and get_job_type list each one")


class SetTiltFilterIn(JobIn):
    mode: Literal["manual", "dl_review", "dl_auto"] | None = None
    model: str | None = Field(None, description="One of conf.yaml's tilt_filter.models")
    threshold: float | None = Field(None, ge=0.0, le=1.0)
    keep_alignable: bool | None = Field(
        None, description="Keep tilts the filter calls bad where excluding them leaves the series unalignable"
    )
    keep_max_gap_deg: float | None = Field(None, description="Widest gap between kept tilts the filter may leave")
    keep_min_tilts: int | None = Field(None, description="A series left with fewer tilts keeps all it can")


class SetSlurmIn(JobIn):
    values: dict[str, Any] | None = Field(
        None,
        description="{setting: value} among partition, constraint, nodes, ntasks_per_node, cpus_per_task, gres, "
        "mem, time, qos, prefer (node features tried first), avoid (node features to stay off)",
    )
    reset: bool = Field(False, description="Drop every setting made by hand: back to the defaults and the profile")


class SetInputIn(JobIn):
    slot: str
    source: str | None = Field(None, description="One of get_input_candidates' sources")
    path: str | None = Field(None, description="A file instead of a source")
    auto: bool = Field(False, description="Back to automatic selection")
    facet: dict[str, str] | None = Field(
        None, description="For a kitchen set: {project path or *: source} per facet choice; {} forgets them"
    )


class SetGainIn(ProjectIn):
    path: str | None = Field(description="The gain reference file, or null for none")


class ImportSpeciesIn(ProjectIn):
    catalog_id: str = Field(description="The lab catalog's species (list_species lists the catalog)")
    version: int | None = Field(None, description="A published version; the latest when omitted")


class AddTemplateIn(SpeciesIn):
    path: str = Field(description="An MRC file; it is copied into the project's templates/<species>/")
    polarity: Literal["white", "black"] = Field(description="Particles white on black, or black on white")
    lowpass_ang: float | None = None
    source: str = Field("agent", description="Where the template came from, kept with it")


class AddMaskIn(SpeciesIn):
    path: str = Field(description="An MRC file; it is copied into the project's templates/<species>/")
    method: Literal["spherical", "cylindrical", "relion", "manual", "imported"] = "imported"


class SelectTemplateIn(SpeciesIn):
    template_id: str


class SelectMaskIn(SpeciesIn):
    mask_id: str


_FROM_TEMPLATE = "The template it starts from; the species' selected one when omitted"


class MakeTemplateIn(SpeciesIn):
    kind: Literal["shape", "pdb", "emdb", "resample", "lowpass", "flip", "extend"] = Field(
        description="shape: an ellipsoid; pdb: a density simulated from a PDB entry; emdb: an EMDB map resampled; "
        "resample: a template resampled; lowpass: a template filtered at its own pixel size and box; flip: a "
        "template's other polarity; extend: a filament's template (along Z) made to fill its box with copies of "
        "itself shifted by whole repeats"
    )
    shape: str | None = Field(None, description="shape: the ellipsoid's axes, x:y:z in Å")
    pdb_id: str | None = Field(None, description="pdb: the PDB entry")
    emdb_id: str | None = Field(None, description="emdb: the EMDB entry")
    template_id: str | None = Field(None, description=f"resample, lowpass, flip, extend: {_FROM_TEMPLATE}")
    repeat_ang: float | None = Field(
        None, gt=0, description="extend: the filament's repeat along its axis, Å; the species' when omitted"
    )
    pixel_size: float | None = Field(
        None, gt=0, description="Å; the tomograms' (a Reconstruct job's, or the imported ones') when omitted"
    )
    box_size: int | None = Field(
        None, gt=0, description="Pixels; for a shape the smallest box (its own is its longest axis plus 30 %)"
    )
    lowpass: float | None = Field(
        None, gt=0, description="Å; a shape is filtered at 45 Å and a PDB entry simulated at 10 Å when omitted"
    )


class MakeMaskIn(SpeciesIn):
    kind: Literal["threshold", "sphere"] = Field(
        description="threshold: RELION's mask of the template's density at a level; sphere: a soft sphere on the "
        "template's grid"
    )
    template_id: str | None = Field(None, description=_FROM_TEMPLATE)
    threshold: float | None = Field(None, description="threshold: the density level")
    threshold_method: Literal["flexible_bounds", "otsu", "isodata", "li", "yen"] | None = Field(
        None, description="threshold: find the level this way instead"
    )
    extend: float = Field(5.0, ge=0, description="threshold: pixels the mask grows by")
    soft_edge: float = Field(5.0, ge=0, description="Pixels of soft edge")
    lowpass: float = Field(20.0, gt=0, description="threshold: Å the density is filtered at before thresholding")
    diameter_ang: float | None = Field(None, gt=0, description="sphere: Å; the species' diameter when omitted")


class CancelLeftoversIn(ProjectIn):
    slurm_ids: list[str] = Field(description="The SLURM ids a refused run named as still live")


class CreateSpeciesIn(ProjectIn):
    name: str
    origin: Literal["workbench", "manual"] = Field(
        "workbench", description="workbench: a species with templates; manual: one picked by hand, no template"
    )
    color: str = Field("", description="A hex colour; a palette colour when empty")
    diameter_ang: float | None = Field(None, gt=0)
    symmetry: str = "C1"
    notes: str = ""
    mask_path: str | None = Field(None, description="A mask registered as it is (not copied)")


class Extraction(BaseModel):
    box_size: int = Field(gt=0)
    binning: float = Field(gt=0)
    crop_size: int = Field(gt=0)


class SetSpeciesIn(SpeciesIn):
    name: str | None = None
    color: str | None = None
    diameter_ang: float | None = Field(None, description="null clears it")
    symmetry: str | None = None
    notes: str | None = None
    glyph: str | None = Field(
        None,
        description="The glyph the app's chrome draws the species with: capsid (the default), ribosome, enveloped, "
        "spike, membrane, filament, tube, barrel, pore, lattice; empty or null for the default",
    )
    glyph_hue: str | None = Field(
        None,
        description="The glyph's muted hue: grey (the default), slate, steel, verdigris, olive, ochre, sand, rust, "
        "oxide; empty or null for the default. The overlay colour (color) stays on the canvases",
    )
    extraction: Extraction | None = Field(None, description="The geometry new extraction jobs of the species take")
    segmentation_feature: str | None = Field(
        None,
        description="The segmentation feature its Pick candidates jobs pick from (a Segment job writes ribosome_ais "
        "for easymode's ribosome); empty or null clears it",
    )
    kind: Literal["particle", "filament"] | None = Field(
        None,
        description="The species' shape, which the jobs added for it later take: particle (a pick is a point on its "
        "own), or filament (Pick candidates traces lines and places a pick every repeat along them; Refine 3D "
        "refines a helical tube of the diameter)",
    )
    repeat_ang: float | None = Field(
        None, description="A filament's lattice repeat along its axis, in Å: the spacing of its picks; null clears it"
    )


class ImportTomogramsIn(ProjectIn):
    mode: Literal["synthesize", "reference", "jobs"] = Field(
        description="synthesize: from MRC volumes; reference: an existing tomograms.star; jobs: other projects' "
        "tomogram jobs, with their tilt series, ready for template matching"
    )
    mrc_paths: list[str] = Field(default_factory=list, description="The volumes, for synthesize")
    reference_star: str = Field("", description="The tomograms.star, for reference")
    sources: list[TomogramSourceIn] = Field(default_factory=list, description="The tomogram jobs, for jobs")
    pixel_size_angstrom: float = Field(
        0.0, ge=0.0, description="Overrides the volumes' header voxel size; 0 keeps each header's"
    )
    tomogram_binning: float = Field(1.0, gt=0.0)
    replace: bool = Field(False, description="Drop the batches imported before instead of adding to them")


# ── Deletions ─────────────────────────────────────────────────────────────────


class ClearTrashIn(ProjectIn):
    names: list[str] | None = Field(
        None, description="The Trash/ entries to delete for good (job folder names); all of them when omitted"
    )


# ── Change plans ──────────────────────────────────────────────────────────────


class PlanStepIn(_In):
    verb: str = Field(description="A write verb, e.g. add_job, set_params, delete_job, run")
    args: dict[str, Any] = Field(
        default_factory=dict, description="Its arguments, exactly as the verb takes them when called alone"
    )


class PlanChangesIn(_In):
    title: str = Field(description="What the plan does, in a few words for the user")
    steps: list[PlanStepIn] = Field(min_length=1, description="The writes, in the order they are to run")


class PlanIn(_In):
    plan_id: str = Field(description="The id plan_changes returned")


class ApplyIn(PlanIn):
    revision: int | None = Field(
        None,
        description="The revision the user was shown (plan_changes or get_plan); refused when the plan was edited on "
        "its page since",
    )


class ApplyNowIn(ApplyIn):
    # The one-time answer the crboost MCP forwarder sends after the user confirmed the plan in its terminal
    # dialog. Not in the tool's schema: the agent never sees it, so it cannot confirm for the user.
    confirmation: SkipJsonSchema[str | None] = None
    # Set by a forwarder that shows the dialog (it drops one an agent sends). Without it no token is handed out:
    # an older forwarder would pass the token on to the agent.
    dialog: SkipJsonSchema[bool] = False

"""The project behind the branch map: its jobs, what each reads and makes, its tilt series.

`project_from_state` reads the workspace's live `ProjectState` (never a second load): the jobs, the inputs each
finished job recorded when it ran (`paths`), the resolver's answer for the ones still scheduled, the per-tilt-series
statuses the array status cache holds, and the tilt-series registry in memory. Nothing here touches the disk; what a
large card shows of a job's results comes from `ui/branch_map/facts.py`, read off the event loop.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from services import array_status_cache
from services.array_tasks import sort_ts_by_position
from services.jobs import jobtype_paramclass
from services.jobs.spec import JOB_SPEC_BY_TYPE, PIPELINE_ORDER, instance_order
from services.models_base import JobType
from services.path_resolution_service import PathResolutionService
from services.tilt_series import get_registry_for

logger = logging.getLogger(__name__)

NEW = "new"  # the node id of the job being added
TRUNK = "trunk"  # the node id of the shared start, folded


# ── the model ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Slot:
    """One input of a job type. `bundle`: normally read from the same job as the first (main) slot, so it rides
    that choice. `unless_method`: the methods under which the input is not read (easymode needs no model).
    `quiet`: a settings file that rides along: a row on the card, no cable on the map."""

    key: str
    label: str
    accepts: tuple[str, ...]
    required: bool = True
    bundle: bool = False
    unless_method: tuple[str, ...] = ()
    quiet: bool = False


@dataclass(frozen=True)
class FieldSpec:
    """One parameter of a job: how the form asks for it. `group` is the job tab's grouping
    (ui/job_plugins/default_renderer._classify_fields): paths get a full-width row each, numeric / enum / text
    share the grid, toggles wrap into a row of their own beneath."""

    name: str
    label: str
    kind: str  # number / select / text
    default: object
    options: tuple[str, ...] = ()
    hint: str = ""
    group: str = "numeric"


@dataclass
class Plot:
    """One plot of a job's numbers: `kind` dots / bars / curve / hist / stack."""

    kind: str
    values: list[float]
    caption: str = ""
    flags: set[int] = field(default_factory=set)
    lo: float | None = None
    hi: float | None = None
    cut: float | None = None  # a threshold line, in the values' units
    kept: int = 0  # stack: the two parts
    out: int = 0


@dataclass
class Facts:
    """What a large card shows of a finished job: pictures (urls with a hover each), a plot, lines, tiles."""

    pictures: list[tuple[str, str]] = field(default_factory=list)
    picture_caption: str = ""
    arrow: bool = False  # the pictures are a before → after pair
    plot: Plot | None = None
    lines: list[str] = field(default_factory=list)
    tiles: list[tuple[str, str]] = field(default_factory=list)  # headline numbers, the Journey's stat tiles


@dataclass
class TiltPlot:
    """One per-tilt plot of a tilt series, the Journey's way: a value per tilt over the stage angle, dropped tilts
    hollow. `kind` dots / bars. `over_is_bad`: bars above `cut` in the dropped-tilt red (a residual), or not (a
    score, a peak)."""

    label: str
    tilts: list[float]
    values: list[float]
    unit: str = ""
    kind: str = "dots"
    dropped: set[int] = field(default_factory=set)  # indices into `tilts`
    lo: float | None = None
    hi: float | None = None
    cut: float | None = None
    over_is_bad: bool = True


@dataclass
class TsFacts:
    """What a large card shows of one job for one tilt series: the Journey's section for that stage, compacted."""

    tiles: list[tuple[str, str]] = field(default_factory=list)
    pictures: list[tuple[str, str]] = field(default_factory=list)
    arrow: bool = False
    plots: list[TiltPlot] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class TsStatic:
    """What the picker says of one tilt series besides how it fared: its tilts, the dropped ones, the mdoc's
    defocus."""

    tilts: int
    dropped: int
    defocus_um: float | None = None


@dataclass
class InputRow:
    """What one input of an existing job reads: the job (`source`) or a named thing that is not a job (`value`),
    and how it was settled."""

    slot: Slot
    source: str | None
    how: str
    value: str | None = None


@dataclass
class Job:
    id: str  # the instance id: the node id on the map
    kind: str
    job_type: JobType
    number: str  # the RELION job number, or nothing for a job that has none (not run yet, or run inline)
    parent: str | None
    status: str  # done / running / failed / queued / scheduled
    ok: int
    total: int
    side: tuple[str, ...] = ()
    method: str = ""  # the method parameter's value, raw
    tool: str = ""
    identity: list[str] = field(default_factory=list)
    inputs: list[InputRow] = field(default_factory=list)
    statuses: list[str] | None = None  # per tilt series in project order: ok / fail / running / pending / skip / none
    param_values: dict[str, object] = field(default_factory=dict)  # the job's parameters, by field name
    species: str | None = None
    job_dir: Path | None = None
    on_roster: bool = True
    frozen: bool = False  # its inputs were fixed at submit: the rows are what it ran with


@dataclass(frozen=True)
class Vocabulary:
    """What every job type is, from the param classes: shared by every project."""

    names: dict[str, str]
    stages: list[str]
    slots: dict[str, tuple[Slot, ...]]
    produces: dict[str, str]  # kind → the type token of its main output
    makes: dict[str, str]  # kind → what its output row says
    methods: dict[str, tuple[tuple[str, str], ...]]  # kind → ((value, label), …)
    tool: dict[str, str]
    param_fields: dict[str, tuple[FieldSpec, ...]]
    job_types: dict[str, JobType]
    addable: tuple[str, ...]


@dataclass
class Project:
    name: str
    path: Path
    jobs: list[Job]
    vocab: Vocabulary
    ts_ids: list[str] = field(default_factory=list)
    ts_titles: dict[str, str] = field(default_factory=dict)  # tilt series id → Pos 3 · Beam 2
    ts_static: dict[str, TsStatic] = field(default_factory=dict)
    species_names: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.by_id = {j.id: j for j in self.jobs}
        self.names = self.vocab.names
        self.stages = self.vocab.stages
        self.slots = self.vocab.slots
        self.produces = self.vocab.produces
        self.makes = self.vocab.makes
        self.methods = self.vocab.methods
        self.tool = self.vocab.tool
        self.param_fields = self.vocab.param_fields
        self.addable = self.vocab.addable
        self.words = WORDS
        self.gloss = GLOSS
        self.slot_gloss = SLOT_GLOSS
        self.icon = ICON

    # ── one tilt series ──
    def ts_title(self, ts: str) -> str:
        return self.ts_titles.get(ts, ts)

    def ts_state(self, j: Job, ts: str) -> str:
        """How one tilt series fared in one job: its task marker, else the job's status."""
        if j.statuses is not None and ts in self.ts_ids:
            i = self.ts_ids.index(ts)
            return j.statuses[i] if i < len(j.statuses) else "none"
        return {"done": "ok", "failed": "fail", "running": "running"}.get(j.status, "pending")

    def trail(self, ts: str) -> list[tuple[str, str]]:
        """How one tilt series fared at every job, in map order."""
        return [(j.id, self.ts_state(j, ts)) for j in self.jobs]

    # ── lineage ──
    def path_of(self, j: Job) -> list[Job]:
        """Root first, `j` last, along main inputs."""
        out = []
        seen: set[str] = set()
        cur: Job | None = j
        while cur is not None and cur.id not in seen:
            out.append(cur)
            seen.add(cur.id)
            cur = self.by_id.get(cur.parent) if cur.parent else None
        return out[::-1]

    def ancestor_at(self, j: Job, stage: str) -> Job | None:
        return next((a for a in self.path_of(j) if a.kind == stage), None)

    def main_children(self, j: Job) -> list[Job]:
        return [c for c in self.jobs if c.parent == j.id]

    def readers(self, j: Job) -> list[Job]:
        return [c for c in self.jobs if c.parent == j.id or j.id in c.side]

    def trunk(self) -> list[str]:
        """The shared start of every chain: from the root while a job has one reader."""
        roots = [j for j in self.jobs if j.parent is None]
        if not roots:
            return []
        out = []
        cur = roots[0]
        while True:
            out.append(cur.id)
            kids = self.main_children(cur)
            if len(kids) != 1:
                return out
            cur = kids[0]

    def stage_index(self, kind: str) -> int:
        return self.stages.index(kind) if kind in self.stages else len(self.stages)

    # ── naming ──
    def num(self, j: Job) -> str:
        return j.number

    def ident(self, j: Job) -> str:
        return f"{self.names.get(j.kind, j.kind)} {j.number or j.id}"

    def method_label(self, raw: str) -> str:
        return METHOD_WORDS.get(raw, raw)

    def chips(self, j: Job) -> list[str]:
        """The job's own parameters, the way a user names them: the method, then what sets it apart."""
        method = self.method_label(j.method) if j.method else ""
        return ([method] if method and method != self.tool.get(j.kind, "") else []) + list(j.identity)

    def label(self, j: Job) -> str:
        return " · ".join(self.chips(j)) or self.names.get(j.kind, j.kind)

    def method_of(self, j: Job) -> str:
        """The badge on the header: the method's words, else the tool."""
        return self.method_label(j.method) if j.method else j.tool or self.tool.get(j.kind, "")

    def same_kind(self, j: Job) -> list[Job]:
        return [x for x in self.jobs if x.kind == j.kind]

    # ── inputs ──
    def slots_of(self, kind: str, method: str | None = None) -> tuple[Slot, ...]:
        """A type's inputs, given its method: an easymode denoising needs no trained model."""
        return tuple(s for s in self.slots.get(kind, ()) if method is None or method not in s.unless_method)

    def candidates_for(self, slot: Slot) -> list[Job]:
        """The jobs whose main output a new job's input could read: by type, not scheduled."""
        return [j for j in self.jobs if self.produces.get(j.kind) in slot.accepts and j.status != "scheduled"]

    def rows_of(self, j: Job) -> list[InputRow]:
        return j.inputs

    def offered_after(self, j: Job) -> list[tuple[str, Slot | None, str]]:
        """The job types a card's add button offers: (kind, the slot that would read it, or None with why not)."""
        out = []
        produced = self.produces.get(j.kind)
        for kind in self.addable:
            if self.stage_index(kind) <= self.stage_index(j.kind) and kind != "subset":
                continue
            slot = next((s for s in self.slots_of(kind) if produced in s.accepts), None)
            if slot is None:
                first = self.slots_of(kind)
                wants = self.words.get(first[0].accepts[0], "something else") if first else "nothing"
                out.append((kind, None, f"reads {wants}, not {self.words.get(produced, produced)}"))
            else:
                out.append((kind, slot, ""))
        return out

    # ── tilt series ──
    def series_count(self, j: Job) -> int:
        return j.total

    @property
    def max_series(self) -> int:
        return max((self.series_count(j) for j in self.jobs), default=1)

    def ts_text(self, j: Job) -> tuple[str, str]:
        """The tilt-series tally and its colour, the roster's way."""
        from ui.branch_map.style import AMBER, BLUE, GREEN, MUTED, RED

        if j.status == "running":
            return f"{j.ok}/{j.total} ▸", BLUE
        if j.status in ("queued", "scheduled"):
            return f"{j.ok}/{j.total}", MUTED
        if j.status == "failed":
            return f"{j.ok}/{j.total} {j.total - j.ok}!", RED
        if j.ok < j.total:
            return f"{j.ok}/{j.total} {j.total - j.ok}!", AMBER
        return f"{j.ok}/{j.total}", GREEN

    def tally_words(self, j: Job) -> str:
        left = j.total - j.ok
        if j.status == "running":
            return f"running: {j.ok} of {j.total} tilt series done"
        if j.status in ("queued", "scheduled"):
            return f"{j.status}: {j.total} tilt series to do"
        if j.status == "failed":
            return f"failed on {left} of {j.total} tilt series"
        if left:
            return f"{j.ok} of {j.total} tilt series done, {left} failed"
        return f"all {j.total} tilt series done"

    # ── the pedigree: the choices that tell a candidate from the others ──
    def _aid(self, j: Job | None) -> str | None:
        return None if j is None else j.id

    def differing_stages(self, cands: list[Job]) -> list[str]:
        """The stages at which the candidates' ancestries split, in pipeline order. A stage counts only when two
        candidates equal at every earlier splitting stage differ there."""
        kept: list[str] = []
        for s in self.stages:
            groups: dict[tuple, set] = {}
            for c in cands:
                key = tuple(self._aid(self.ancestor_at(c, k)) for k in kept)
                groups.setdefault(key, set()).add(self._aid(self.ancestor_at(c, s)))
            if any(len(v) > 1 for v in groups.values()):
                kept.append(s)
        return kept

    def _stage_name(self, a: Job, distinct: list[Job]) -> str:
        base = self.label(a)
        clash = [d for d in distinct if self.label(d) == base]
        return base if len(clash) <= 1 else f"{base} {a.number or a.id}"

    def _distinct_at(self, cands: list[Job], s: str) -> list[Job]:
        seen: dict[str, Job] = {}
        for c in cands:
            a = self.ancestor_at(c, s)
            if a is not None:
                seen.setdefault(a.id, a)
        return list(seen.values())

    def _peers(self, j: Job, cands: list[Job], kept: list[str]) -> list[Job]:
        key = tuple(self._aid(self.ancestor_at(j, k)) for k in kept)
        return [c for c in cands if tuple(self._aid(self.ancestor_at(c, k)) for k in kept) == key]

    def pedigree(self, j: Job, cands: list[Job]) -> list[str]:
        """The choices that tell `j` from the other candidates: its ancestor at each splitting stage, then its own
        parameters."""
        out = []
        kept: list[str] = []
        for s in self.differing_stages(cands):
            peers = self._peers(j, cands, kept)
            kept.append(s)
            if s == j.kind:
                continue
            a = self.ancestor_at(j, s)
            if a is not None:
                out.append(self._stage_name(a, self._distinct_at(peers, s)))
        out.extend(self.chips(j))
        return out


# ── words ────────────────────────────────────────────────────────────────────

# Words for the artifact types of services/io_slots.JobFileType.
WORDS = {
    "tilt_series_star": "tilt series",
    "fs_motion_and_ctf_star": "motion-corrected tilt series",
    "warp_frameseries_dir": "warp frames dir",
    "warp_frameseries_settings": "warp frame settings",
    "warp_tiltseries_settings": "settings",
    "tomostar_dir": "tomostars",
    "aligned_tilt_series_star": "aligned tilt series",
    "ts_ctf_tilt_series_star": "CTF'd tilt series",
    "filtered_tilt_series_star": "filtered tilt series",
    "warp_tiltseries_dir": "warp dir",
    "tomograms_star": "tomograms",
    "denoised_tomograms_star": "denoised tomograms",
    "denoise_model_tar": "denoising model",
    "segmentations": "segmentations",
    "tm_results_dir": "TM results",
    "candidates_star": "candidate picks",
    "optimisation_set_star": "particle set",
    "particles_star": "particles",
    "reference_map": "reference map",
    "half_map": "half map",
}
# The input row's label per slot key; input_star depends on what it accepts.
SLOT_LABELS = {
    "input_processing": "warp dir",
    "reconstruct_base": "warp dir",
    "warp_tiltseries_settings": "settings",
    "tomostar_dir": "tomostars",
    "model_path": "model",
    "input_tomograms": "tomograms",
    "input_tiltseries": "tilt series",
    "input_segmentations": "segmentations",
    "input_tm_job": "TM results",
    "input_optimisation": "particle set",
    "input_reference": "reference",
}
STAR_LABELS = {
    "tilt_series_star": "tilt series",
    "fs_motion_and_ctf_star": "tilt series",
    "aligned_tilt_series_star": "alignment",
    "ts_ctf_tilt_series_star": "tilt series",
    "tomograms_star": "tomograms",
}
NAMES = {
    "importmovies": "Import",
    "fsMotionAndCtf": "Motion & CTF",
    "tsImport": "Tilt series import",
    "tiltFilter": "Tilt filter",
    "aligntiltsWarp": "Alignment",
    "missAlign": "missAlign",
    "tsCtf": "CTF",
    "subset": "Subset",
    "tsReconstruct": "Reconstruct",
    "denoisetrain": "Denoise train",
    "denoisepredict": "Denoise predict",
    "segment": "Segment",
    "templatematching": "Template matching",
    "tmextractcand": "Pick candidates",
    "filamentPick": "Filament pick",
    "subtomoExtraction": "Subtomo extraction",
    "warpExport": "Warp export",
    "reconstructParticle": "Reconstruct particle",
    "class3d": "Class 3D",
    "refine3d": "Refine 3D",
}
TOOLS = {
    "warp_aretomo": "WarpTools",
    "relion": "RELION",
    "pytom": "PyTOM",
    "miss_alignment": "miss-alignment",
    "easymode": "easymode",
    "cryocare": "cryoCARE",
    "isonet": "IsoNet",
    "crboost": "CryoBoost",
    "imod": "IMOD",
    "tilt_filter": "DL model",
}
# The parameter that is a type's method, and how its values read.
METHOD_FIELD = {
    "aligntiltsWarp": "alignment_method",
    "denoisetrain": "denoise_method",
    "denoisepredict": "denoise_method",
    "segment": "method",
    "tiltFilter": "mode",
}
METHOD_WORDS = {
    "dl_review": "DL review",
    "dl_auto": "DL auto",
    "dl": "DL model",
    "manual": "by hand",
    "Relion": "RELION",
}
GLOSS = {
    "RELION": "RELION's import: the tilt series and their metadata into the project",
    "WarpTools": "Warp's command-line tools: motion, CTF and tomogram reconstruction",
    "DL model": "the deep-learning tilt classifier scores each tilt and the bad ones are taken out",
    "DL review": "the deep-learning tilt classifier scores each tilt; the verdicts are reviewed by hand",
    "DL auto": "the deep-learning tilt classifier scores each tilt and its verdicts stand",
    "by hand": "tilts reviewed and taken out by hand",
    "AreTomo": "AreTomo: marker-free tilt-series alignment on the GPU",
    "IMOD": "IMOD: fiducial or patch-tracking alignment",
    "miss-alignment": "a refinement of the alignment, tilt by tilt",
    "cryoCARE": "cryoCARE: noise2noise denoising trained on this project's even and odd tomograms",
    "IsoNet": "IsoNet: denoising with missing-wedge correction",
    "easymode": "easymode: pretrained networks for denoising and segmentation, no training job needed",
    "Ais": "Ais: segmentation of membranes and other features",
    "MemBrain-seg": "MemBrain-seg: a membrane segmentation network",
    "PyTOM": "pytom-match-pick: template matching on the GPU",
    "CryoBoost": "a step CryoBoost runs itself, no external tool",
}
SLOT_GLOSS = {
    "frames": "the raw frames of every tilt",
    "tilt series": "the tilt series, one stack per tomogram",
    "alignment": "the aligned tilt series: tilt angles and shifts",
    "warp dir": "the Warp folder the tilt series are processed in; it comes with them",
    "settings": "the Warp tilt-series settings file; it comes with the alignment",
    "tomostars": "the per-tilt-series star files Warp works from",
    "tomograms": "the reconstructed tomograms",
    "model": "a trained denoising model",
    "segmentations": "segmentations of the tomograms",
    "TM results": "the template-matching score maps",
    "particle set": "a particle set: picks with their tomograms and tilt series",
    "reference": "a reference map to start from",
}
ICON = {
    "importmovies": "download",
    "fsMotionAndCtf": "motion_photos_on",
    "tsImport": "download",
    "tiltFilter": "filter_alt",
    "aligntiltsWarp": "center_focus_weak",
    "missAlign": "tune",
    "tsCtf": "adjust",
    "subset": "content_cut",
    "tsReconstruct": "view_in_ar",
    "denoisetrain": "model_training",
    "denoisepredict": "auto_fix_high",
    "segment": "texture",
    "templatematching": "my_location",
    "tmextractcand": "scatter_plot",
    "filamentPick": "timeline",
    "subtomoExtraction": "grid_on",
    "warpExport": "output",
    "reconstructParticle": "blur_circular",
    "class3d": "category",
    "refine3d": "auto_awesome",
}
ADDABLE = (
    "aligntiltsWarp",
    "missAlign",
    "tsCtf",
    "subset",
    "tsReconstruct",
    "denoisetrain",
    "denoisepredict",
    "segment",
    "templatematching",
    "tmextractcand",
    "warpExport",
    "subtomoExtraction",
    "reconstructParticle",
    "class3d",
    "refine3d",
)
# Parameters the card's form leaves to the SLURM tab or never asks.
HIDDEN_PARAMS = {"array_throttle", "perdevice", "do_at_most"}
STATUS_WORD = {
    "Succeeded": "done",
    "Failed": "failed",
    "Running": "running",
    "Queued": "queued",
    "Scheduled": "scheduled",
}


def value_words(v) -> str:
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, Enum):
        return str(v.value)
    if isinstance(v, float):
        return f"{v:g}"
    if isinstance(v, (list, tuple)):
        return ", ".join(str(x) for x in v) if v else "none"
    return "" if v is None else str(v)


def param_words(jm, name: str, default="") -> str:
    """A job parameter in words (an enum by its value)."""
    return value_words(getattr(jm, name, default))


def job_dir_of(jm, project: Path) -> Path | None:
    p = jm.paths.get("job_dir") if jm.paths else None
    if p:
        return Path(p)
    if jm.relion_job_name:
        return project / jm.relion_job_name.rstrip("/")
    return None


def _tool_of(jm, fallback: str) -> str:
    try:
        return TOOLS.get(jm.get_tool_name(), jm.get_tool_name())
    except Exception:  # a type with no tool of its own (the inline ones)
        return fallback


def field_spec(jm, name: str) -> FieldSpec:
    from ui.job_plugins.default_renderer import _is_pathlike
    from ui.utils import snake_to_title

    info = jm.model_fields.get(name)
    label = (info.title if info and info.title else None) or snake_to_title(name)
    hint = (info.description or "") if info else ""
    value = getattr(jm, name)
    ann = info.annotation if info else type(value)
    # The groups are the job tab's (default_renderer._classify_fields), so the card lays the form out as the tab does.
    if isinstance(value, bool) or ann is bool:
        return FieldSpec(name, label, "select", "yes" if value else "no", ("no", "yes"), hint, "toggle")
    if _is_pathlike(name):
        return FieldSpec(name, label, "text", "" if value is None else str(value), (), hint, "paths")
    if isinstance(value, Enum):
        opts = tuple(e.value for e in type(value))
        return FieldSpec(name, label, "select", value.value, opts, hint, "enum")
    if isinstance(ann, type) and issubclass(ann, Enum):
        opts = tuple(e.value for e in ann)
        return FieldSpec(name, label, "select", value if value in opts else opts[0], opts, hint, "enum")
    if isinstance(value, (int, float)) or value is None:
        return FieldSpec(name, label, "number", value, (), hint, "numeric")
    if isinstance(value, (list, tuple)):
        return FieldSpec(name, label, "text", ", ".join(str(v) for v in value), (), hint, "text")
    return FieldSpec(name, label, "text", str(value), (), hint, "text")


_vocab: Vocabulary | None = None


def vocabulary() -> Vocabulary:
    """The job types' inputs, outputs, methods, tools and parameter forms, from the param classes, once per
    process. A form's defaults are the class's own: what `add_job` makes before the species' values and the
    typed ones go on."""
    global _vocab
    if _vocab is not None:
        return _vocab
    classes = jobtype_paramclass()
    stages = [jt.value for jt in PIPELINE_ORDER]
    if "tsImport" in stages:
        # Shown between Motion & CTF and the tilt filter, where the roster keeps it.
        stages.remove("tsImport")
        stages.insert(stages.index("fsMotionAndCtf") + 1, "tsImport")
    names: dict[str, str] = {}
    slots: dict[str, tuple[Slot, ...]] = {}
    produces: dict[str, str] = {}
    makes: dict[str, str] = {}
    methods: dict[str, tuple[tuple[str, str], ...]] = {}
    tool: dict[str, str] = {}
    param_fields: dict[str, tuple[FieldSpec, ...]] = {}
    job_types: dict[str, JobType] = {}
    for jt, cls in classes.items():
        kind = jt.value
        spec = JOB_SPEC_BY_TYPE.get(jt)
        names[kind] = NAMES.get(kind, spec.display_name if spec else kind)
        job_types[kind] = jt
        try:
            proto = cls()
        except Exception:  # a class whose defaults need the project (none known); shown without a form
            logger.exception("branch map: defaults of %s", kind)
            continue
        in_schema = getattr(cls, "INPUT_SCHEMA", [])
        out_schema = getattr(cls, "OUTPUT_SCHEMA", [])
        main_key = (spec.items_from if spec else None) or (in_schema[0].key if in_schema else None)
        main_pref = next((s.preferred_source for s in in_schema if s.key == main_key), None)
        rows = []
        for s in in_schema:
            accepts = tuple(a.value for a in s.accepts)
            label = SLOT_LABELS.get(s.key) or STAR_LABELS.get(
                accepts[0], s.key.removeprefix("input_").replace("_", " ")
            )
            when = getattr(s, "when", None)
            unless = ("easymode",) if when == "uses_trained_model" else ()
            bundle = s.key != main_key and s.preferred_source == main_pref and main_pref is not None
            quiet = s.key == "warp_tiltseries_settings" and s.key != main_key
            # A slot read only under a condition (the search restricted to segmentations) is optional here.
            required = bool(s.required) and not (when and when != "uses_trained_model")
            rows.append(Slot(s.key, label, accepts, required, bundle, unless, quiet))
        # The main slot first: the card's first row is the lineage.
        rows.sort(key=lambda r: 0 if r.key == main_key else 1)
        slots[kind] = tuple(rows)
        if out_schema:
            produces[kind] = out_schema[0].produces.value
            makes[kind] = WORDS.get(produces[kind], produces[kind])
        tool[kind] = _tool_of(proto, "CryoBoost")
        mf = METHOD_FIELD.get(kind)
        if mf and mf in proto.model_fields:
            ann = proto.model_fields[mf].annotation
            if isinstance(ann, type) and issubclass(ann, Enum):
                methods[kind] = tuple((e.value, METHOD_WORDS.get(e.value, e.value)) for e in ann)
        fields = [field_spec(proto, n) for n in sorted(proto.USER_PARAMS - HIDDEN_PARAMS - {mf})]
        param_fields[kind] = tuple(fields)
    produces.setdefault("tiltFilter", "fs_motion_and_ctf_star")
    makes["tiltFilter"] = "tilt series, bad tilts out"
    makes["importmovies"] = "tilt series"
    makes["subset"] = "a subset of tilt series"
    _vocab = Vocabulary(
        names,
        stages,
        slots,
        produces,
        makes,
        methods,
        tool,
        param_fields,
        job_types,
        tuple(k for k in ADDABLE if k in job_types),
    )
    return _vocab


def _identity(kind: str, jm) -> list[str]:
    identity: list[str] = []
    if kind in ("aligntiltsWarp", "tsReconstruct") and getattr(jm, "rescale_angpixs", None):
        identity.append(f"{jm.rescale_angpixs:g} Å")
    if getattr(jm, "deconv", 0):
        identity.append("deconvolved")
    if kind == "segment":
        identity.append(f"{len(getattr(jm, 'features', []) or [])} features")
    if kind == "templatematching":
        identity.append(f"{param_words(jm, 'angular_search')}° · {param_words(jm, 'symmetry')}")
    if kind == "tmextractcand":
        identity.append(f"cut at {getattr(jm, 'expected_false_positives', 0):g} false positive")
    if kind == "reconstructParticle":
        identity.append(f"box {param_words(jm, 'box_size')} · {param_words(jm, 'symmetry')}")
    if kind == "class3d":
        n_cl = getattr(jm, "n_classes", 1)
        identity.append(f"{n_cl} class{'es' if n_cl != 1 else ''} · {param_words(jm, 'n_iterations')} iterations")
    return identity


def project_from_state(state, project_path: Path, *, set_parents: dict[str, tuple[str | None, str]]) -> Project:
    """The project as the map sees it, from the live state: every job in `state.jobs` (the roster's and the
    prerequisite the roster hides), its inputs as recorded or as the resolver will choose them, its per-tilt-series
    statuses from the array status cache, the tilt series from the registry in memory. `set_parents`: the particle
    sets some inputs read, by set id, as `ui/branch_map/facts.py` has read them (the job it pooled from, its
    name); a set not read yet is a named value without a cable."""
    vocab = vocabulary()
    try:
        registry = get_registry_for(project_path)
        ts_order = sort_ts_by_position(registry.tilt_series_ids())
        series = {ts.id: ts for ts in registry.all_tilt_series()}
    except Exception:  # a project without a registry: the map has no tilt series, the jobs stand
        logger.exception("branch map: the registry of %s", project_path)
        ts_order, series = [], {}
    ts_titles = {
        t: f"Pos {series[t].stage_position} · Beam {series[t].beam_position}" if t in series else t for t in ts_order
    }
    ts_static = {}
    for t in ts_order:
        ts = series.get(t)
        if ts is None:
            continue
        frames = ts.frames
        dose = [abs(fr.defocus_um) for fr in frames if fr.defocus_um]
        ts_static[t] = TsStatic(
            len(frames), sum(1 for fr in frames if fr.is_filtered_out), sorted(dose)[len(dose) // 2] if dose else None
        )

    dirs: dict[str, str] = {}  # job dir → instance id
    for iid, jm in state.jobs.items():
        jd = job_dir_of(jm, project_path)
        if jd is not None:
            dirs[str(jd)] = iid
    live_edges: dict[tuple[str, str], str] = {}
    try:
        for e in PathResolutionService(state).resolve_slot_edges(list(state.jobs)):
            live_edges[(e.consumer, e.slot)] = e.producer
    except Exception:
        logger.exception("branch map: resolving the edges of %s", project_path)
    roster = set(state.pipeline_order or ())
    jobs: dict[str, Job] = {}
    for iid, jm in state.jobs.items():
        if jm.job_type is None:
            continue
        kind = jm.job_type.value
        jd = job_dir_of(jm, project_path)
        number = jm.relion_job_number or (jd.name.removeprefix("job") if jd and jd.name.startswith("job") else None)
        try:
            number_txt = f"{int(number):03d}" if number else ""
        except (TypeError, ValueError):
            number_txt = str(number)
        status = STATUS_WORD.get(jm.execution_status.value, "scheduled")
        statuses: list[str] | None = None
        ok = total = len(ts_order)
        arr = array_status_cache.get(project_path, iid) if status != "scheduled" else None
        if arr is not None:
            statuses = [arr.statuses.get(t, "none") for t in ts_order]
            total = len(arr.items)
            ok = sum(1 for v in arr.statuses.values() if v == "ok")
        elif status in ("scheduled", "failed"):
            ok = 0
        mf = METHOD_FIELD.get(kind)
        method_raw = getattr(jm, mf, None) if mf else None
        method = value_words(method_raw) if method_raw is not None else ""
        rows: list[InputRow] = []
        for slot in (s for s in vocab.slots.get(kind, ()) if method not in s.unless_method):
            src = None
            how = "fixed when the job ran"
            value = None
            recorded = jm.paths.get(slot.key) if jm.paths else None
            if recorded:
                hit = next((d for d in dirs if recorded.startswith(d + "/") or recorded == d), None)
                src = dirs[hit] if hit else None
                if src is None and "/Analytics/sets/" in recorded:
                    set_id = recorded.split("/Analytics/sets/", 1)[1].split("/", 1)[0]
                    parent_iid, value = set_parents.get(set_id, (None, f"set {set_id}"))
                    src = parent_iid if parent_iid in state.jobs else None
                    how = "a particle set pooled from that job's picks" if src else "a particle set, not a job"
                elif src is None:
                    value = Path(recorded).name
            if src is None and value is None:
                override = (jm.source_overrides or {}).get(slot.key, "")
                if override.startswith("mergedSources:"):
                    set_id = override.split(":", 1)[1].removeprefix("particle_set__")
                    parent_iid, value = set_parents.get(set_id, (None, f"set {set_id}"))
                    src = parent_iid if parent_iid in state.jobs else None
                    how = "a particle set pooled from that job's picks" if src else "a particle set, not a job"
                elif (iid, slot.key) in live_edges:
                    src = live_edges[(iid, slot.key)]
                    how = (
                        "set by hand"
                        if slot.key in (jm.source_overrides or {})
                        else "the resolver's choice, until it runs"
                    )
                elif status == "scheduled":
                    how = "to choose"
            if slot.bundle and src is not None:
                how = f"comes with the {vocab.slots[kind][0].label}"
            rows.append(InputRow(slot, src, how, value))
        main_src = rows[0].source if rows else None
        side = tuple(r.source for r in rows[1:] if r.source and r.source != main_src and not r.slot.quiet)
        identity = _identity(kind, jm)
        if kind == "tiltFilter":
            out = sum(1 for t in ts_order if t in series for fr in series[t].frames if fr.is_filtered_out)
            identity.append(f"{out} tilts taken out")
        param_values = {n: getattr(jm, n) for n in sorted(jm.USER_PARAMS - HIDDEN_PARAMS - ({mf} if mf else set()))}
        jobs[iid] = Job(
            iid,
            kind,
            jm.job_type,
            number_txt,
            main_src,
            status,
            ok,
            total,
            side,
            method=method,
            tool=_tool_of(jm, vocab.tool.get(kind, "")),
            identity=identity,
            inputs=rows,
            statuses=statuses,
            param_values=param_values,
            species=jm.species_id,
            job_dir=jd,
            on_roster=iid in roster or kind == "tsImport",
            frozen=bool(jm.inputs_fixed),
        )
    # The shown lineage of the shared start: the tilt filter trims the tomostars tsImport made, and the alignment
    # reads those, so the roster keeps Motion & CTF → tilt series import → tilt filter → alignment in that order.
    ts_import = next((j for j in jobs.values() if j.kind == "tsImport"), None)
    filt = next((j for j in jobs.values() if j.kind == "tiltFilter"), None)
    if ts_import is not None and filt is not None:
        filt.parent = ts_import.id
    for j in jobs.values():
        if j.kind == "aligntiltsWarp":
            if filt is not None:
                j.parent = filt.id
            elif ts_import is not None:
                j.parent = ts_import.id
    ordered = sorted(jobs.values(), key=lambda j: (0, j.number, j.id) if j.number else (1, "", instance_order(j.id)))
    return Project(
        state.project_name or project_path.name,
        project_path,
        ordered,
        vocab,
        ts_ids=ts_order,
        ts_titles=ts_titles,
        ts_static=ts_static,
        species_names={s.id: s.name for s in state.species_registry},
    )

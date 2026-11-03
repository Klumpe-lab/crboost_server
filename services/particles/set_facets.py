"""How a job reads a set the STA kitchen made: each facet of its picks, from the job that produced it.

A kitchen set holds picks: their positions as picked, their uids (``cbPickUid``), their halves and the tomograms they
lie on (`services/sta_kitchen/export.py`). Whatever else a job needs of a pick is a facet some job made of it, and the
job reading the set takes it from there at Run:

- **images** (Class 3D, Refine 3D): the rows of an extraction that cut the pick's particle images: a Warp export or
  Subtomo extraction in the pick's own project reading its template-match picks (or a pick list's extraction), or one
  in the home project reading a set of the species;
- **poses** (Reconstruct particle, Warp export, Subtomo extraction): the set's own rows as picked, or the rows of a
  Class 3D or Refine 3D of the home project that read the picks.

A source is one job and its rows are taken whole: nothing combines the columns of two jobs. Each project of the set
takes the images of its picks from one job: the only one holding them, or the one chosen on the job reading the set
(`facet_choices`); poses come from one source for the whole set. Two candidates and no choice, none, or images from
jobs that would not make one image size and scale refuse Run. Images of several projects combine only from jobs whose
parameters (but inputs and how they run) are the same and whose stars agree on the images' size and scale.

A pick is found in a job's rows by the uid they carry, else by tomogram and centred position to 0.1 Å (the rows of an
extraction of template-match picks carry no uid: a stand-in, said so). Every star read here is a tool's output: what
is relied on is checked as read, and a row that fails is counted and left out, never matched some other way.

`build_input` builds what the job will read and writes nothing; the orchestrator checks it and writes it into the job's
own directory (`write_input`), and the job reads that frozen copy.

Blocking (star reads, project snapshots): call `resolve` and `build_input` in a thread.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from services.models_base import PARTICLE_EXTRACTION_TYPES, JobStatus, JobType
from services.particles.pick_uids import HALF_COL, UID_COL, half_of, read_uid_column, uid_cell

IMAGES, POSES = "images", "poses"
PICKED = "picked"  # the poses facet's own source: the set's rows as picked
ALL = "*"  # the key of a facet one source gives the whole set (poses)
INPUT_DIR = "input_set"  # in a job's directory: the set as the job reads it, written at Run
# How a job runs, not what it writes: left out when two jobs' parameters are compared.
_RESOURCE_PARAMS = frozenset({"array_throttle", "perdevice"})
# The images' size and scale as an extraction's star states them, and how each is named on screen.
GEOMETRY = {
    "rlnImageSize": "box",
    "rlnImagePixelSize": "pixel (Å)",
    "rlnTomoSubtomogramBinning": "binning",
    "rlnImageDimensionality": "dimensions",
    "rlnCtfDataAreCtfPremultiplied": "CTF-premultiplied",
}
_STACKS = "rlnTomoSubTomosAre2DStacks"
# Per pick, a value that belongs to its tomogram and optics group: template matching writes the binned tomogram's pixel
# size there, which a reader taking it for the tilt series' would scale the pick by. Left out of every row a set or a
# job reading one writes; tomograms.star (and an optics table) state the right one.
PER_ROW_DROPPED = ("rlnTomoTiltSeriesPixelSize",)
_RESULT_TYPES = frozenset({JobType.CLASS3D, JobType.REFINE3D})
_RESULT_OPTSET = "run_optimisation_set.star"
SHOWN = 3


def facet_of(slot) -> str:
    """What an input reading a kitchen set takes of its picks: images where it needs them, else poses."""
    return IMAGES if getattr(slot, "needs_extracted", False) else POSES


@dataclass(frozen=True)
class Source:
    """A job holding a facet of some of the set's picks."""

    ref: str  # "<project>|<instance id>", "<project>|list:<list ref>", or PICKED
    project: str  # the project the job lives in
    label: str  # "Warp Export job044"
    job_type: str
    star: str  # its particles star
    tomograms: str  # the tomograms star its rows refer to
    by: str  # "uid", or "position" (its rows carry no uid: a stand-in)
    rows: dict[str, int]  # uid of a pick of the set -> its row in `star`
    params: tuple = ()  # (name, value) of what decides its rows: its parameters but inputs and how it runs
    geometry: tuple = ()  # (star label, value) of its images' size and scale
    notes: tuple[str, ...] = ()

    @property
    def project_label(self) -> str:
        return Path(self.project).name


@dataclass
class ProjectFacet:
    """One project's picks (or, for poses, the whole set's) and the job they take the facet from."""

    key: str  # the project's path, or ALL
    label: str
    picks: int
    candidates: list[str]  # refs of the sources holding the facet of some of these picks
    choice: str | None = None  # the source the job chose, if it chose one
    chosen: str | None = None  # the source read
    how: str = ""  # "only" | "chosen" | "ask" | "none" | "gone" (the choice holds none of the picks now)
    held_by: dict[str, int] = field(default_factory=dict)  # candidate -> how many of the picks it holds

    @property
    def held(self) -> int:
        """Of the picks, those the source read holds."""
        return self.held_by.get(self.chosen, 0) if self.chosen else 0


@dataclass
class FacetPlan:
    set_id: str
    facet: str
    n_picks: int
    projects: list[ProjectFacet] = field(default_factory=list)
    sources: dict[str, Source] = field(default_factory=dict)
    differ: list[str] = field(default_factory=list)  # why the chosen sources' images are not one size and scale
    problems: list[str] = field(default_factory=list)  # what refuses Run
    notes: list[str] = field(default_factory=list)
    halves: tuple[int, int] = (0, 0)  # of the picks read

    @property
    def n_held(self) -> int:
        return sum(p.held for p in self.projects)

    @property
    def record(self) -> dict[str, str]:
        """Every project's source, as a job keeps what it read at its submit; empty while anything is open."""
        if self.problems:
            return {}
        return {p.key: p.chosen for p in self.projects if p.chosen}

    def chosen_sources(self) -> list[Source]:
        """The sources read, each once, in the order of the projects reading them."""
        refs = list(dict.fromkeys(p.chosen for p in self.projects if p.chosen))
        return [self.sources[r] for r in refs]


# ── Reading, kept per file and mtime ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Star:
    particles: pd.DataFrame
    optics: pd.DataFrame | None
    general: dict


_STARS: dict[str, tuple[int, _Star]] = {}
_PICKS: dict[str, tuple[tuple, _Picks]] = {}


def _mtime(path: str | Path) -> int:
    return os.stat(path).st_mtime_ns


def _read_star(path: str | Path) -> _Star:
    """A particles star's tables: its particles (the block with rlnTomoName), optics and general block. Kept per mtime;
    the frames returned are shared, so callers copy before they change one."""
    import starfile

    p = str(path)
    stamp = _mtime(p)
    hit = _STARS.get(p)
    if hit is not None and hit[0] == stamp:
        return hit[1]
    blocks = starfile.read(p, always_dict=True)
    particles = next((v for v in blocks.values() if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns), None)
    if particles is None:
        raise ValueError(f"{p} has no particle table (no rlnTomoName)")
    optics = next((v for v in blocks.values() if isinstance(v, pd.DataFrame) and "rlnVoltage" in v.columns), None)
    general = next((dict(v) for v in blocks.values() if isinstance(v, dict) and _STACKS in v), {})
    star = _Star(particles.reset_index(drop=True), optics, general)
    _STARS[p] = (stamp, star)
    return star


@dataclass(frozen=True)
class _Picks:
    """The set's picks, one per row of its particles star."""

    uids: list[str]
    names: list[str]  # its tomogram's name in the set
    projects: list[str]  # the project its tomogram lives in
    keys: pd.DataFrame  # its tomogram's name in that project and its centred position at 0.1 Å
    origins: dict  # name in the set -> `set_origins.Origin`
    index: dict[str, int]  # uid -> row


def _set_picks(pset) -> _Picks:
    """The set's picks, checked: a set the kitchen made carries a uid on every row and an origin on every tomogram."""
    from services.particles.set_origins import origins_of
    from services.sta_kitchen.table import _coord_keys
    from services.subtomo_merge import _read_tomograms_star

    stamp = (_mtime(pset.particles_star), _mtime(pset.tomograms_star))
    hit = _PICKS.get(pset.particles_star)
    if hit is not None and hit[0] == stamp:
        return hit[1]
    df = _read_star(pset.particles_star).particles
    col = read_uid_column(df)
    if col is None or col.n_valid != len(df):
        problems = col.problems() if col is not None else [f"its rows carry no {UID_COL}"]
        raise ValueError(f"the set {pset.id} cannot be read by its picks: " + "; ".join(problems))
    origins = origins_of(_read_tomograms_star(Path(pset.tomograms_star)))
    if origins is None:
        raise ValueError(f"the set {pset.id} records no origin for its tomograms")
    names = df["rlnTomoName"].astype(str).tolist()
    unknown = sorted(set(names) - set(origins))
    if unknown:
        raise ValueError(f"the set {pset.id} names tomograms it has no row for: {', '.join(unknown[:SHOWN])}")
    there = df.copy()
    there["rlnTomoName"] = [origins[n].tomo for n in names]
    picks = _Picks(
        uids=list(col.uids),
        names=names,
        projects=[str(Path(origins[n].project).resolve()) for n in names],
        keys=_coord_keys(there),
        origins=origins,
        index={u: i for i, u in enumerate(col.uids)},
    )
    _PICKS[pset.particles_star] = (stamp, picks)
    return picks


def _uid_rows(df: pd.DataFrame, picks: _Picks) -> tuple[dict[str, int], list[str]]:
    """{uid: row} of the set's picks in a star whose rows carry their uid, and what the column check left out."""
    col = read_uid_column(df)
    if col is None:
        return {}, [f"its rows carry no {UID_COL}"]
    rows = {u: i for i, u in enumerate(col.uids) if u is not None and u in picks.index}
    return rows, [f"{p}: left out" for p in col.problems()]


def _position_rows(df: pd.DataFrame, picks: _Picks, project: str) -> tuple[dict[str, int], list[str]]:
    """{uid: row} of the set's picks in `project` found in `df` by tomogram and centred position at 0.1 Å, and what was
    left out: rows sharing a position with another (no single pick to give them)."""
    from services.sta_kitchen.table import _coord_keys

    try:
        keys = _coord_keys(df)
    except KeyError:
        return {}, ["its rows carry no centred positions"]
    index = pd.MultiIndex.from_frame(keys)
    twice = index.duplicated(keep=False)
    lookup = pd.Series(np.arange(len(keys)), index=index)[~twice]
    mine = [i for i, p in enumerate(picks.projects) if p == project]
    if not mine:
        return {}, []
    hit = lookup.reindex(pd.MultiIndex.from_frame(picks.keys.iloc[mine])).to_numpy()
    rows = {picks.uids[i]: int(h) for i, h in zip(mine, hit, strict=True) if not np.isnan(h)}
    notes = [f"{int(twice.sum()):,} rows share a tomogram and position with another: left out"] if twice.any() else []
    return rows, notes


def _params(job_model) -> tuple:
    """What decides a job's rows: its parameters, but how it runs, as comparable text."""
    keys = sorted(k for k in type(job_model).USER_PARAMS if k not in _RESOURCE_PARAMS)
    dumped = job_model.model_dump(mode="json", include=set(keys))
    return tuple((k, json.dumps(dumped.get(k), sort_keys=True)) for k in keys)


def _geometry(star: _Star) -> tuple:
    """The images' size and scale an extraction's star states: each optics column once (several values joined by "/",
    which no other source shares), and whether they are 2D stacks."""
    out = []
    optics = star.optics if star.optics is not None else pd.DataFrame()
    for c in GEOMETRY:
        if c in optics.columns:
            out.append((c, "/".join(sorted({_plain(v) for v in optics[c]}))))
    if _STACKS in star.general:
        out.append((_STACKS, _plain(star.general[_STACKS])))
    return tuple(out)


def _plain(v) -> str:
    try:
        return f"{float(v):g}"
    except (TypeError, ValueError):
        return str(v)


# ── Where the facets are ────────────────────────────────────────────────────────────────────────


def _image_sources(home: Path, pset, picks: _Picks) -> tuple[list[Source], list[str]]:
    """The jobs holding particle images of the set's picks: each member's own extractions (by position) and the home
    project's extractions of sets of the species (by uid). Only finished jobs."""
    from services.jobs.spec import display_name
    from services.particles.analytics_store import job_dir_of, subtomo_instances_for_ce
    from services.particles.set_origins import project_of
    from services.sta_kitchen.snapshots import project_view
    from services.sta_kitchen.uid_join import uid_extractions
    from services.subtomo_merge import _parse_optimisation_set

    found: list[tuple[dict, str]] = []  # (what the source is, how it is joined)
    notes: list[str] = []
    for parent in pset.recipe.parents:
        if parent.kind not in ("job", "list"):
            continue
        project = Path(parent.project_path or home)
        try:
            view = project_view(project)
        except (OSError, ValueError) as e:
            notes.append(f"{project.name} cannot be read ({e}): its extractions are not looked at")
            continue
        state = view.state
        if parent.kind == "job":
            for iid in subtomo_instances_for_ce(state, parent.instance_id, view.path, types=PARTICLE_EXTRACTION_TYPES):
                jm = state.jobs[iid]
                jd = job_dir_of(state, view.path, iid)
                if jm.execution_status != JobStatus.SUCCEEDED or jd is None or not (jd / "particles.star").exists():
                    continue
                found.append(
                    (
                        {
                            "ref": f"{view.path}|{iid}",
                            "project": str(view.path),
                            "label": f"{display_name(jm.job_type)} {jd.name}",
                            "job_type": jm.job_type.value,
                            "star": str(jd / "particles.star"),
                            "tomograms": str(jd / "tomograms.star"),
                            "params": _params(jm),
                        },
                        "position",
                    )
                )
        else:
            species, tomo, slug = parent.list_ref.split("/", 2)
            pl = state.get_pick_list(slug, species, tomo)
            if pl is None or not pl.extracted_path or not Path(pl.extracted_path).exists():
                continue
            particles, tomograms = _parse_optimisation_set(Path(pl.extracted_path))
            found.append(
                (
                    {
                        "ref": f"{view.path}|list:{parent.list_ref}",
                        "project": str(view.path),
                        "label": f"list extraction {pl.label or pl.slug} · {tomo}",
                        "job_type": JobType.EXTRACT_PICK_LIST.value,
                        "star": str(particles),
                        "tomograms": str(tomograms),
                        # Not recorded for a list's extraction: it combines with no other job.
                        "params": (("extraction", json.dumps(pl.extracted_path)),),
                    },
                    "position",
                )
            )
    home_view = project_view(home)
    for e in uid_extractions(home_view, pset.species_id):
        jm = home_view.state.jobs.get(e["job"])
        if jm is None or jm.execution_status != JobStatus.SUCCEEDED:
            continue
        found.append(
            (
                {
                    "ref": f"{home_view.path}|{e['job']}",
                    "project": str(home_view.path),
                    "label": f"{display_name(jm.job_type)} {Path(e['star']).parent.name}",
                    "job_type": jm.job_type.value,
                    "star": e["star"],
                    "tomograms": str(Path(e["star"]).with_name("tomograms.star")),
                    "params": _params(jm),
                },
                "uid",
            )
        )
    out = []
    for info, by in found:
        # The project its rows name tomograms of: the one its star lies in, as the set's origins name projects.
        info["project"] = str(project_of(Path(info["star"]), Path(info["project"])).resolve())
        try:
            star = _read_star(info["star"])
        except (OSError, ValueError) as ex:
            notes.append(f"{info['label']} ({Path(info['project']).name}) cannot be read: {ex}")
            continue
        if "rlnImageName" not in star.particles.columns:
            notes.append(f"{info['label']} ({Path(info['project']).name}): its rows carry no particle images")
            continue
        if by == "uid":
            rows, faults = _uid_rows(star.particles, picks)
        else:
            rows, faults = _position_rows(star.particles, picks, info["project"])
        if rows:
            out.append(Source(**info, by=by, rows=rows, geometry=_geometry(star), notes=tuple(faults)))
    return out, notes


def _pose_sources(home: Path, pset, picks: _Picks) -> tuple[list[Source], list[str]]:
    """The set's own rows, and the home project's Class 3D and Refine 3D results whose rows carry the set's picks."""
    from services.jobs.spec import display_name
    from services.particles.analytics_store import job_dir_of
    from services.particles.star_columns import star_labels
    from services.sta_kitchen.snapshots import project_view
    from services.subtomo_merge import _parse_optimisation_set

    out = [
        Source(
            ref=PICKED,
            project=str(Path(home).resolve()),
            label="as picked",
            job_type="",
            star=pset.particles_star,
            tomograms=pset.tomograms_star,
            by="uid",
            rows=dict(picks.index),
        )
    ]
    notes: list[str] = []
    view = project_view(home)
    for iid, jm in view.state.jobs.items():
        if jm.job_type not in _RESULT_TYPES or jm.execution_status != JobStatus.SUCCEEDED:
            continue
        jd = job_dir_of(view.state, view.path, iid)
        optset = jd / _RESULT_OPTSET if jd is not None else None
        if optset is None or not optset.exists():
            continue
        try:
            particles, tomograms = _parse_optimisation_set(optset)
            if UID_COL not in star_labels(particles):
                continue
            rows, faults = _uid_rows(_read_star(particles).particles, picks)
        except (OSError, ValueError) as e:
            notes.append(f"{display_name(jm.job_type)} {jd.name} cannot be read: {e}")
            continue
        if rows:
            out.append(
                Source(
                    ref=f"{view.path}|{iid}",
                    project=str(view.path),
                    label=f"{display_name(jm.job_type)} {jd.name}",
                    job_type=jm.job_type.value,
                    star=str(particles),
                    tomograms=str(tomograms),
                    by="uid",
                    rows=rows,
                    notes=tuple(faults),
                )
            )
    return out, notes


# ── Resolving ───────────────────────────────────────────────────────────────────────────────────


def resolve(home: Path, pset, facet: str, choices: dict[str, str] | None = None) -> FacetPlan:
    """Where a job reading `pset` takes `facet` of its picks from, per project (images) or for the whole set (poses),
    with `choices` ({project path or ALL: source ref}) where the job chose. Raises ValueError for a set that cannot be
    read by its picks."""
    choices = choices or {}
    picks = _set_picks(pset)
    plan = FacetPlan(set_id=pset.id, facet=facet, n_picks=len(picks.uids))
    found, plan.notes = (_image_sources if facet == IMAGES else _pose_sources)(Path(home), pset, picks)
    plan.sources = {s.ref: s for s in found}
    if facet == IMAGES:
        groups: dict[str, list[int]] = {}
        for i, p in enumerate(picks.projects):
            groups.setdefault(p, []).append(i)
        keyed = [(p, Path(p).name, rows) for p, rows in sorted(groups.items(), key=lambda kv: Path(kv[0]).name)]
    else:
        keyed = [(ALL, "the set", list(range(len(picks.uids))))]
    for key, label, rows in keyed:
        uids = [picks.uids[i] for i in rows]
        held_by = {s.ref: n for s in found if (n := sum(u in s.rows for u in uids))}
        pf = ProjectFacet(key, label, len(rows), list(held_by), choice=choices.get(key), held_by=held_by)
        if pf.choice:
            pf.how = "chosen" if pf.choice in held_by else "gone"
            pf.chosen = pf.choice if pf.how == "chosen" else None
        elif len(held_by) == 1:
            pf.how, pf.chosen = "only", pf.candidates[0]
        else:
            pf.how = "ask" if held_by else "none"
        plan.projects.append(pf)
    if facet == IMAGES:
        plan.differ = _differences(plan)
    _say(plan, picks)
    return plan


def _differences(plan: FacetPlan) -> list[str]:
    """Why the images of the chosen sources are not one job's: per source unlike the sources holding most of the
    picks, what differs (the job, its parameters, else what its star says of the images)."""
    chosen = plan.chosen_sources()
    if len(chosen) < 2:
        return []
    held: dict[str, int] = {}
    for p in plan.projects:
        if p.chosen:
            held[p.chosen] = held.get(p.chosen, 0) + p.held
    groups: dict[tuple, list[Source]] = {}
    for s in chosen:
        groups.setdefault((s.job_type, s.params, s.geometry), []).append(s)
    if len(groups) == 1:
        return []
    main = max(groups, key=lambda g: sum(held[s.ref] for s in groups[g]))
    out = []
    for sig, sources in groups.items():
        if sig == main:
            continue
        for s in sources:
            out.append(f"{s.label} ({s.project_label}): {_unlike(sig, main)}")
    return out


def _unlike(sig: tuple, main: tuple) -> str:
    job_type, params, geometry = sig
    if job_type != main[0]:
        return f"a {job_type} job where the others are {main[0]}"
    theirs = dict(main[1])
    parts = [f"{k} {_shown(v)} against {_shown(theirs.get(k))}" for k, v in params if theirs.get(k) != v]
    if not parts:
        theirs = dict(main[2])
        mine = dict(geometry)
        parts = [
            f"{GEOMETRY.get(k, k)} {mine.get(k, '—')} against {theirs.get(k, '—')}"
            for k in dict.fromkeys([*mine, *theirs])
            if mine.get(k) != theirs.get(k)
        ]
    return "; ".join(parts) or "another job"


def _shown(v: str | None) -> str:
    if v is None:
        return "—"
    try:
        return _plain(json.loads(v))
    except ValueError:
        return v


def _say(plan: FacetPlan, picks: _Picks) -> None:
    what = "particle images" if plan.facet == IMAGES else "poses"
    for p in plan.projects:
        names = ", ".join(plan.sources[r].label for r in p.candidates[:SHOWN])
        if p.how == "ask" and plan.facet == POSES:
            plan.problems.append(f"its picks' poses: {poses_choice(plan, p)}; pick one")
        elif p.how == "ask":
            plan.problems.append(
                f"{p.label}: {len(p.candidates)} jobs hold {what} of its {p.picks:,} picks ({names}); pick one"
            )
        elif p.how == "none":
            plan.problems.append(
                f"{p.label}: no job holds {what} of its {p.picks:,} picks yet: add a Warp export reading the set"
            )
        elif p.how == "gone":
            plan.problems.append(
                f"{p.label}: the job chosen for it ({_ref_label(p.choice)}) holds none of its picks now"
            )
    if plan.differ:
        plan.problems.append(
            "its images would come from jobs that made them differently ("
            + "; ".join(plan.differ[:SHOWN])
            + "): run one Warp export on the set"
        )
    if not plan.problems and not plan.n_held:
        plan.problems.append(f"none of its picks has {what} in the chosen jobs")
    read: set[str] = set()
    for p in plan.projects:
        if p.chosen:
            rows = plan.sources[p.chosen].rows
            read.update(
                u for u, proj in zip(picks.uids, picks.projects, strict=True) if u in rows and p.key in (ALL, proj)
            )
    halves = [half_of(u) for u in read]
    plan.halves = (halves.count(1), halves.count(2))
    dropped = plan.n_picks - len(read)
    if dropped and not plan.problems:
        plan.notes.append(f"{dropped:,} of {plan.n_picks:,} picks have no {what} in the jobs read: left out")
    for s in plan.chosen_sources():
        if s.by == "position":
            plan.notes.append(
                f"{s.label} ({s.project_label}): matched to the picks by position (to 0.1 Å), as its rows carry no "
                "pick uid"
            )
        plan.notes += [f"{s.label} ({s.project_label}): {n}" for n in s.notes]


def poses_choice(plan: FacetPlan, p: ProjectFacet) -> str:
    """The poses a job may take, in words: "as picked, or as Class 3D job049 refined them"."""
    refined = [plan.sources[r].label for r in p.candidates if r != PICKED]
    return "as picked, or as " + " or ".join(refined) + " refined them"


def _ref_label(ref: str | None) -> str:
    if not ref:
        return "—"
    if ref == PICKED:
        return "as picked"
    project, _, job = ref.partition("|")
    return f"{job} of {Path(project).name}"


# ── What the job reads ──────────────────────────────────────────────────────────────────────────


@dataclass
class BuiltInput:
    """The set as one job reads it, before it is written."""

    particles: pd.DataFrame
    optics: pd.DataFrame | None
    general: dict
    tomograms: pd.DataFrame
    # name in the set -> (the per-tilt star it is read from, its block there, the project that star was written in), for
    # a tomogram the source names otherwise: the job gets a copy under the set's name (RELION reads the block by name)
    tilt_copies: dict[str, tuple[str, str, str]]
    notes: list[str] = field(default_factory=list)


def build_input(plan: FacetPlan, pset) -> BuiltInput:
    """The rows and tomograms the job reads: each chosen source's rows for the picks it gives, copied whole, stamped
    with the pick's uid and half, their tomograms renamed as the set names them and recorded with their origin; optics
    groups numbered anew across sources. Raises ValueError where a source's rows contradict the join."""
    from services.particles.coord_merge import _absolutize_paths
    from services.particles.set_origins import ORIGIN_PROJECT, ORIGIN_TILT_STAR, ORIGIN_TOMO, project_of
    from services.subtomo_merge import _read_tomograms_star

    if plan.problems:
        raise ValueError("; ".join(plan.problems))
    picks = _set_picks(pset)
    parts: list[tuple[Source, pd.DataFrame, pd.DataFrame | None]] = []
    tomo_parts: list[pd.DataFrame] = []
    tilt_copies: dict[str, tuple[str, str, str]] = {}
    general: dict = {}
    notes: list[str] = []
    named: dict[str, str] = {}  # name in the set -> the source that gave its tomogram row
    for src in plan.chosen_sources():
        keys = {p.key for p in plan.projects if p.chosen == src.ref}
        uids = [
            u
            for u, proj in zip(picks.uids, picks.projects, strict=True)
            if u in src.rows and (ALL in keys or proj in keys)
        ]
        star = _read_star(src.star)
        df = star.particles.iloc[[src.rows[u] for u in uids]].copy().reset_index(drop=True)
        col = read_uid_column(df)
        if col is not None:
            wrong = sum(u != want for u, want in zip(col.uids, uids, strict=True))
            if wrong:
                raise ValueError(f"{src.label}: {wrong:,} of the rows found for the picks carry another pick's uid")
        df[UID_COL] = [uid_cell(u) for u in uids]
        df[HALF_COL] = [half_of(u) for u in uids]
        df = df.drop(columns=[c for c in PER_ROW_DROPPED if c in df.columns])
        renames = _rename_rows(df, [picks.names[picks.index[u]] for u in uids], src)
        tomograms = _read_tomograms_star(Path(src.tomograms))
        by_name = {str(n): i for i, n in enumerate(tomograms["rlnTomoName"].astype(str))}
        project = Path(src.project)
        rows = []
        for old, new in renames.items():
            if old not in by_name:
                raise ValueError(
                    f"{src.label}: its rows name tomogram {old}, which {Path(src.tomograms).name} has no row for"
                )
            if new in named:
                raise ValueError(f"tomogram {new} would come from two jobs ({named[new]} and {src.label})")
            named[new] = src.label
            row = _absolutize_paths(tomograms.iloc[by_name[old]], project)
            row["rlnTomoName"] = new
            if old != new:
                tilt = str(row.get("rlnTomoTiltSeriesStarFile") or "")
                if not tilt or not Path(tilt).is_absolute():
                    raise ValueError(f"{src.label}: tomogram {old} names no per-tilt star to copy under the name {new}")
                tilt_copies[new] = (tilt, old, str(project_of(Path(tilt), project)))
            o = picks.origins[new]
            row[ORIGIN_PROJECT], row[ORIGIN_TOMO], row[ORIGIN_TILT_STAR] = o.project, o.tomo, o.tilt_star
            rows.append(row)
        tomo_parts.append(pd.DataFrame(rows))
        if star.general:
            if general and _plain(general.get(_STACKS)) != _plain(star.general.get(_STACKS)):
                raise ValueError(f"{src.label}: its images are 2D stacks where the other jobs' are not, or the reverse")
            general = dict(star.general)
        parts.append((src, df, star.optics))
    particles, optics = _merge_optics(parts, notes)
    tomograms = _common(tomo_parts, "tomogram", notes)
    if plan.facet == IMAGES:
        need = ["rlnImageName", "rlnOpticsGroup"] + (
            ["rlnTomoVisibleFrames"] if _plain(general.get(_STACKS)) == "1" else []
        )
        missing = [c for c in need if c not in particles.columns]
        if missing:
            raise ValueError(f"the image rows lack {', '.join(missing)} in some chosen job")
    return BuiltInput(particles, optics, general, tomograms, tilt_copies, notes)


def _rename_rows(df: pd.DataFrame, names: list[str], src: Source) -> dict[str, str]:
    """The rows' tomograms renamed to the set's names (and their particle names' prefix), in place; returns {name in
    the source: name in the set}. One name in the source must stand for one tomogram of the set, and the reverse."""
    out: dict[str, str] = {}
    for old, new in zip(df["rlnTomoName"].astype(str), names, strict=True):
        if out.setdefault(old, new) != new:
            raise ValueError(f"{src.label}: its tomogram {old} holds picks of two tomograms of the set")
    if len(set(out.values())) != len(out):
        raise ValueError(f"{src.label}: one tomogram of the set under two names in its rows")
    if "rlnTomoParticleName" in df.columns:
        df["rlnTomoParticleName"] = [
            f"{new}/{p[len(old) + 1 :]}" if old != new and p.startswith(f"{old}/") else p
            for p, old, new in zip(
                df["rlnTomoParticleName"].astype(str), df["rlnTomoName"].astype(str), names, strict=True
            )
        ]
    df["rlnTomoName"] = names
    return out


def _merge_optics(parts: list[tuple[Source, pd.DataFrame, pd.DataFrame | None]], notes: list[str]):
    """The sources' rows in one table and their optics groups in one, numbered anew: groups alike in every column
    become one; two unlike groups of one name refuse (RELION would take them for one)."""
    with_optics = [o for _s, _d, o in parts if o is not None]
    if not with_optics:
        return _common([d for _s, d, _o in parts], "particle", notes), None
    if len(with_optics) != len(parts):
        raise ValueError("some chosen jobs' rows come with an optics table and others without")
    cols = [c for c in with_optics[0].columns if all(c in o.columns for o in with_optics)]
    if "rlnOpticsGroup" not in cols:
        raise ValueError("an optics table of the chosen jobs has no rlnOpticsGroup")
    groups: dict[tuple, int] = {}
    by_name: dict[str, tuple] = {}
    rows: list[dict] = []
    frames = []
    for src, df, optics in parts:
        mapping: dict[int, int] = {}
        for _, row in optics[cols].iterrows():
            sig = tuple((c, _plain(row[c])) for c in cols if c != "rlnOpticsGroup")
            name = str(row.get("rlnOpticsGroupName", ""))
            if sig not in groups:
                if name and by_name.get(name, sig) != sig:
                    raise ValueError(
                        f"{src.label}: its optics group {name} differs from another chosen job's of that name"
                    )
                groups[sig] = len(groups) + 1
                by_name.setdefault(name, sig)
                rows.append({**row.to_dict(), "rlnOpticsGroup": groups[sig]})
            mapping[int(row["rlnOpticsGroup"])] = groups[sig]
        mapped = pd.to_numeric(df["rlnOpticsGroup"], errors="coerce").map(mapping)
        if mapped.isna().any():
            raise ValueError(f"{src.label}: {int(mapped.isna().sum()):,} rows name an optics group its table lacks")
        df["rlnOpticsGroup"] = mapped.astype(int)
        frames.append(df)
    return _common(frames, "particle", notes), pd.DataFrame(rows)[cols]


def _common(frames: list[pd.DataFrame], what: str, notes: list[str]) -> pd.DataFrame:
    """The frames in one, with the columns every one has: a cell no source states stays out rather than blank."""
    if len(frames) == 1:
        return frames[0].reset_index(drop=True)
    cols = [c for c in frames[0].columns if all(c in f.columns for f in frames)]
    dropped = sorted({c for f in frames for c in f.columns} - set(cols))
    if dropped:
        notes.append(f"{what} columns not every chosen job writes, left out: {', '.join(dropped)}")
    return pd.concat([f[cols] for f in frames], ignore_index=True)


def write_input(built: BuiltInput, out_dir: Path) -> Path:
    """Write what `build_input` built into `out_dir` (a job's ``input_set``): particles, tomograms, the per-tilt stars
    of renamed tomograms and the optimisation set naming them. Returns the optimisation set."""
    from services.particles.set_origins import TILT_DIR, write_renamed_tilt_star
    from services.subtomo_merge import _write_particles_star, _write_tomograms_star, write_optimisation_set

    out_dir.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(out_dir / TILT_DIR, ignore_errors=True)
    tomograms = built.tomograms.copy()
    for new, (src, old, project) in built.tilt_copies.items():
        dst = out_dir / TILT_DIR / f"{new}.star"
        write_renamed_tilt_star(Path(src), dst, old, new, Path(project))
        tomograms.loc[tomograms["rlnTomoName"].astype(str) == new, "rlnTomoTiltSeriesStarFile"] = str(dst.resolve())
    particles = out_dir / "particles.star"
    _write_particles_star(particles, optics_df=built.optics, particles_df=built.particles, general_kv=built.general)
    _write_tomograms_star(out_dir / "tomograms.star", tomograms)
    optset = out_dir / "optimisation_set.star"
    write_optimisation_set(optset, particles_star=particles, tomograms_star=out_dir / "tomograms.star")
    return optset

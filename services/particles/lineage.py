"""What only the disk knows about a project's particles, for the particle map to draw: the particle sets as nodes
(their recipe's parents, the classes a class subset took, stale parents), the class rows of each Class 3D (count,
share, Class 3D's own resolution estimate), and how many particles a job or a set holds per project and per
tomogram. Which job reads which job is not here: the map takes that from the same place the Jobs view does.

The star reads (up to hundreds of thousands of rows) run in worker processes (`WorkKind.LINEAGE`); `LineageStore`
keeps what they returned per project, keyed by the files' mtimes, which it stats in a thread. A reader asks the
store and gets what is there; `version` moves when a read lands.

Every RELION / Warp star is checked where it is relied on: a column missing, a class number out of range or a
tomogram the tomograms star does not name is reported on the node (`problems`), never filled in.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from services.jobs import jobtype_paramclass
from services.jobs.spec import JOB_SPEC_BY_TYPE, PHASE_PARTICLES
from services.io_slots import JobFileType
from services.models_base import JobStatus
from services.particles.particle_sets import SetOp, list_sets, set_read_by, sets_root
from services.particles.set_origins import ORIGIN_PROJECT
from services.workers import WorkKind, run_in_worker

logger = logging.getLogger(__name__)

SET_NODE_PREFIX = "set:"  # a set's node id on the map: `set:<set_id>`
_SETS_DIR_MARK = "/Analytics/sets/"
_IT_DATA = re.compile(r"run_it(\d{3})_data\.star$")
_IT_OPTSET = re.compile(r"run_it(\d{3})_optimisation_set\.star$")
NOT_RECORDED = "(origin not recorded)"
NOT_LISTED = "(not in the tomograms star)"


# ── what a node holds ────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Composition:
    """How many particles a node holds, by project and by tomogram (most first). `source`: the star counted."""

    total: int
    by_project: tuple[tuple[str, int], ...]
    by_tomo: tuple[tuple[str, str, int], ...]  # (tomogram, its project, particles)
    source: str
    problems: tuple[str, ...] = ()


@dataclass(frozen=True)
class ClassRow:
    """One class of a Class 3D's last iteration: its members, its share as RELION's model star gives it
    (`rlnClassDistribution`, the probability-weighted share, not members / total), and Class 3D's own resolution
    estimate (not a gold-standard FSC)."""

    number: int
    n: int
    share: float | None
    estimate_ang: float | None
    map_path: str


@dataclass(frozen=True)
class ClassRead:
    iteration: int
    classes: tuple[ClassRow, ...]
    data_star: str
    problems: tuple[str, ...] = ()


@dataclass(frozen=True)
class JobParticles:
    """A job's particles: the ones it made (`role` "makes") or, for a job that makes a map, the ones it read
    ("reads"). `classes` for a Class 3D."""

    role: str
    composition: Composition | None
    classes: ClassRead | None = None
    problem: str = ""


@dataclass(frozen=True)
class SetParentRef:
    """One parent of a set: a job of this project (`instance_id`), another set (`set_id`), or something in another
    project (`project` names it). `classes`: the classes a class subset took of its Class 3D."""

    kind: str
    instance_id: str
    set_id: str
    project: str
    label: str
    classes: tuple[int, ...]
    stale: bool


@dataclass(frozen=True)
class SetNode:
    id: str
    name: str
    species_id: str
    level: str
    op: str
    how: str
    n: int
    n_tomos: int
    parents: tuple[SetParentRef, ...]
    composition: Composition | None
    parent_stars: tuple[str, ...] = ()
    problems: tuple[str, ...] = ()

    @property
    def node_id(self) -> str:
        return SET_NODE_PREFIX + self.id

    @property
    def number(self) -> str:
        return self.id.split("_", 1)[0]


@dataclass(frozen=True)
class JobRef:
    """What the store needs of one particle job, taken from the live state on the loop (no disk)."""

    iid: str
    kind: str
    status: str
    job_dir: str  # as recorded; "" when the job has none yet
    rel_dir: str
    output_optsets: tuple[str, ...]  # path templates of the optimisation sets it makes
    input_optset: str  # the optimisation set it read, as recorded


# ── which input reads a set ──────────────────────────────────────────────────────────────────────


def set_of_input(job_model, slot: str = "input_optimisation") -> str | None:
    """The id of the particle set one input of a job reads: the set path it recorded when it ran, else the one its
    inputs name (`particle_sets.set_read_by`); None when it reads no set."""
    recorded = str((getattr(job_model, "paths", None) or {}).get(slot) or "")
    if _SETS_DIR_MARK in recorded:
        return recorded.split(_SETS_DIR_MARK, 1)[1].split("/", 1)[0]
    return set_read_by(job_model, slot)


def is_particle_job(job_type) -> bool:
    spec = JOB_SPEC_BY_TYPE.get(job_type)
    return spec is not None and spec.phase == PHASE_PARTICLES


def job_refs(state) -> list[JobRef]:
    """Every particle job of the project that has run or is running, as the store reads it."""
    classes = jobtype_paramclass()
    out = []
    for iid, jm in state.jobs.items():
        if jm.job_type is None or not is_particle_job(jm.job_type):
            continue
        if jm.execution_status in (JobStatus.SCHEDULED, JobStatus.UNKNOWN):
            continue
        cls = classes.get(jm.job_type)
        optsets = tuple(
            s.path_template
            for s in getattr(cls, "OUTPUT_SCHEMA", [])
            if s.produces == JobFileType.OPTIMISATION_SET_STAR and s.path_template
        )
        paths = jm.paths or {}
        out.append(
            JobRef(
                iid,
                jm.job_type.value,
                jm.execution_status.value,
                str(paths.get("job_dir") or ""),
                str(jm.relion_job_name or (state.job_path_mapping or {}).get(iid) or ""),
                optsets,
                str(paths.get("input_optimisation") or ""),
            )
        )
    return out


# ── the star reads (worker side) ─────────────────────────────────────────────────────────────────


def _blocks(path: str | Path) -> dict:
    import starfile

    return starfile.read(path, always_dict=True)


def _table_with(path: str | Path, column: str):
    import pandas as pd

    return next((v for v in _blocks(path).values() if isinstance(v, pd.DataFrame) and column in v.columns), None)


def parse_optimisation_set(path: str | Path) -> tuple[str, str]:
    """(particles star, tomograms star) an optimisation set names, absolute."""
    import pandas as pd

    base = Path(path).parent
    for block in _blocks(path).values():
        if isinstance(block, pd.DataFrame) and {"rlnTomoParticlesFile", "rlnTomoTomogramsFile"} <= set(block.columns):
            pair = (block["rlnTomoParticlesFile"].iloc[0], block["rlnTomoTomogramsFile"].iloc[0])
        elif isinstance(block, dict) and {"rlnTomoParticlesFile", "rlnTomoTomogramsFile"} <= set(block):
            pair = (block["rlnTomoParticlesFile"], block["rlnTomoTomogramsFile"])
        else:
            continue
        return tuple(str(p if Path(str(p)).is_absolute() else base / str(p)) for p in pair)  # type: ignore[return-value]
    raise ValueError(f"{path} names no particles and tomograms stars")


def _origin_projects(tomograms_star: str, home: str) -> tuple[dict[str, str], list[str]]:
    """{tomogram: the project it lives in} from a tomograms star: its `cbOriginProject` where the star records
    origins, else the home project. A row without an origin in a star that records them is named as such."""
    problems: list[str] = []
    if not tomograms_star or not Path(tomograms_star).exists():
        return {}, [f"tomograms star missing: {tomograms_star or 'none named'}"]
    df = _table_with(tomograms_star, "rlnTomoName")
    if df is None:
        return {}, [f"{tomograms_star} has no rlnTomoName column"]
    names = df["rlnTomoName"].astype(str)
    if ORIGIN_PROJECT not in df.columns:
        return dict.fromkeys(names, home), problems
    out = {}
    blank = 0
    for name, origin in zip(names, df[ORIGIN_PROJECT], strict=True):
        o = "" if origin is None or (isinstance(origin, float) and origin != origin) else str(origin).strip()
        if not o:
            blank += 1
        out[name] = Path(o).name if o else NOT_RECORDED
    if blank:
        problems.append(f"{blank} tomogram rows of {Path(tomograms_star).name} record no origin project")
    return out, problems


def _compose(tomos: list[str], tomograms_star: str, home: str, source: str, problems: list[str]) -> Composition:
    project_of, more = _origin_projects(tomograms_star, home)
    problems = problems + more
    per_tomo = Counter(tomos)
    unlisted = [t for t in per_tomo if project_of and t not in project_of]
    if unlisted:
        problems.append(f"{len(unlisted)} tomograms the particles name are not in the tomograms star")
    by_tomo = sorted(
        ((t, project_of.get(t, NOT_LISTED if project_of else home), n) for t, n in per_tomo.items()),
        key=lambda r: (-r[2], r[0]),
    )
    by_project: Counter = Counter()
    for _t, p, n in by_tomo:
        by_project[p] += n
    return Composition(
        sum(per_tomo.values()),
        tuple(sorted(by_project.items(), key=lambda kv: (-kv[1], kv[0]))),
        tuple(by_tomo),
        source,
        tuple(problems),
    )


def read_composition(particles_star: str, tomograms_star: str, home: str) -> Composition:
    """Particles per project and per tomogram of one particles star (worker)."""
    df = _table_with(particles_star, "rlnTomoName")
    if df is None:
        return Composition(0, (), (), particles_star, (f"{Path(particles_star).name} has no rlnTomoName column",))
    return _compose(df["rlnTomoName"].astype(str).tolist(), tomograms_star, Path(home).name, particles_star, [])


def read_optset_composition(optset: str, home: str) -> Composition:
    particles, tomograms = parse_optimisation_set(optset)
    return read_composition(particles, tomograms, home)


def read_class3d(job_dir: str, home: str) -> tuple[ClassRead, Composition]:
    """A Class 3D's last iteration: each class's members (`rlnClassNumber` of its data star), share and estimate
    (its model star), and the particles' composition (worker)."""
    from services.particles.class_readout import _model_classes

    jd = Path(job_dir)
    its = sorted(int(m.group(1)) for p in jd.glob("run_it*_data.star") if (m := _IT_DATA.search(p.name)))
    if not its:
        raise FileNotFoundError(f"{job_dir} has written no iteration yet")
    it = its[-1]
    data = jd / f"run_it{it:03d}_data.star"
    problems: list[str] = []
    df = _table_with(data, "rlnClassNumber")
    if df is None:
        raise ValueError(f"{data.name} has no rlnClassNumber column")
    numbers = df["rlnClassNumber"]
    try:
        numbers = numbers.astype(int)
    except (TypeError, ValueError) as e:
        raise ValueError(f"{data.name}: rlnClassNumber is not a whole number on every row") from e
    model = _model_classes(jd / f"run_it{it:03d}_model.star") if (jd / f"run_it{it:03d}_model.star").exists() else []
    if not model:
        problems.append(f"run_it{it:03d}_model.star missing or without classes: no share or estimate")
    k = max(len(model), int(numbers.max()) if len(numbers) else 0)
    outside = int(((numbers < 1) | (numbers > k)).sum()) if model else 0
    if outside:
        problems.append(f"{outside} rows name a class the model star does not have")
    counts = Counter(numbers.tolist())
    by_number = {c["class"]: c for c in model}
    rows = tuple(
        ClassRow(
            c,
            counts.get(c, 0),
            by_number[c]["distribution"] if c in by_number else None,
            by_number[c]["resolution_ang"] if c in by_number else None,
            str(jd / f"run_it{it:03d}_class{c:03d}.mrc"),
        )
        for c in range(1, k + 1)
    )
    optset = jd / f"run_it{it:03d}_optimisation_set.star"
    tomograms = ""
    if optset.exists():
        tomograms = parse_optimisation_set(optset)[1]
    else:
        problems.append(f"{optset.name} missing: the tomograms these particles live in are not named")
    tomos = df["rlnTomoName"].astype(str).tolist() if "rlnTomoName" in df.columns else []
    comp_problems = [] if tomos or not len(df) else [f"{data.name} has no rlnTomoName column"]
    comp = _compose(tomos, tomograms, Path(home).name, str(data), comp_problems)
    return ClassRead(it, rows, str(data), tuple(problems)), comp


def read_sets(project_path: str) -> list[SetNode]:
    """Every particle set of the project as a node: its parents, how it was chosen, its composition from the
    counts per tomogram its record holds and the origins its tomograms star records (worker)."""
    from services.particles.particle_sets import set_lineage

    home = Path(project_path)
    out = []
    for ps in list_sets(home):
        stale = {p.key for p in ps.stale_parents()}
        parents = []
        for p in ps.recipe.parents:
            foreign = bool(p.project_path) and Path(p.project_path) != home
            classes: tuple[int, ...] = ()
            if ps.recipe.op == SetOp.CLASS_SUBSET and p.kind == "job":
                classes = tuple(int(c) for c in ps.recipe.params.get("classes") or ())
            parents.append(
                SetParentRef(
                    p.kind,
                    p.instance_id,
                    p.set_id,
                    Path(p.project_path).name if foreign else "",
                    p.label,
                    classes,
                    p.key in stale,
                )
            )
        problems: list[str] = []
        comp = None
        if ps.per_tomo:
            tomos = [t for t, n in ps.per_tomo.items() for _ in range(int(n))]
            comp = _compose(tomos, ps.tomograms_star, home.name, ps.particles_star, problems)
        else:
            problems.append("the set's record holds no counts per tomogram")
        out.append(
            SetNode(
                ps.id,
                ps.name,
                ps.species_id,
                ps.level.value,
                ps.recipe.op.value,
                set_lineage(ps),
                ps.n_particles,
                ps.n_tomograms,
                tuple(parents),
                comp,
                tuple(p.particles_star for p in ps.recipe.parents),
                tuple(problems),
            )
        )
    return out


# ── the store (server side) ──────────────────────────────────────────────────────────────────────


def _mtime(path: str | Path) -> float | None:
    try:
        return os.stat(path).st_mtime
    except OSError:  # gone or never written: the key says so
        return None


def _job_dir(project: Path, ref: JobRef) -> Path | None:
    for d in (ref.job_dir, str(project / ref.rel_dir.rstrip("/")) if ref.rel_dir else ""):
        if d and Path(d).is_dir():
            return Path(d)
    return None


def _newest(jd: Path, pattern: re.Pattern, glob: str) -> Path | None:
    found = sorted((int(m.group(1)), p) for p in jd.glob(glob) if (m := pattern.search(p.name)))
    return found[-1][1] if found else None


@dataclass
class _Work:
    """One read the poll found due: its cache key and the worker call that answers it."""

    key: tuple
    fn: object
    args: tuple
    role: str = "makes"


def _plan(project: Path, refs: list[JobRef], parent_stars: tuple[str, ...]) -> tuple[tuple, dict[str, _Work | str]]:
    """In a thread: the sets' key, and per job the read its files call for (or why there is none)."""
    root = sets_root(project)
    sets_key = (_mtime(root), _mtime(root / "operations.jsonl"), tuple(_mtime(p) for p in parent_stars))
    home = str(project)
    jobs: dict[str, _Work | str] = {}
    for ref in refs:
        jd = _job_dir(project, ref)
        if jd is None:
            jobs[ref.iid] = "no job directory yet"
            continue
        if ref.kind == "class3d":
            data = _newest(jd, _IT_DATA, "run_it*_data.star")
            if data is None:
                jobs[ref.iid] = "no iteration written yet"
                continue
            model = data.with_name(data.name.replace("_data.star", "_model.star"))
            jobs[ref.iid] = _Work(("class3d", str(data), _mtime(data), _mtime(model)), read_class3d, (str(jd), home))
            continue
        optset = next((jd / t for t in ref.output_optsets if (jd / t).exists()), None)
        if optset is None and ref.output_optsets:
            optset = _newest(jd, _IT_OPTSET, "run_it*_optimisation_set.star")
        role = "makes"
        if optset is None and ref.input_optset and not ref.output_optsets:
            optset, role = Path(ref.input_optset), "reads"
        if optset is None or not optset.exists():
            jobs[ref.iid] = "no particles written yet" if ref.output_optsets else "a map: no particles of its own"
            continue
        key = ("optset", str(optset), _mtime(optset))
        jobs[ref.iid] = _Work(key, read_optset_composition, (str(optset), home), role)
    return sets_key, jobs


class LineageStore:
    """One project's particle lineage as read so far, shared by every tab's particle map."""

    def __init__(self, project_path: Path) -> None:
        self.path = Path(project_path)
        self.version = 0
        self.sets: list[SetNode] | None = None
        self.set_problem = ""
        self.jobs: dict[str, JobParticles] = {}
        self._keys: dict[str, tuple] = {}
        self._sets_key: tuple | None = None
        self._polling = False

    def set_by_id(self, set_id: str) -> SetNode | None:
        return next((s for s in self.sets or () if s.id == set_id), None)

    async def poll(self, refs: list[JobRef]) -> None:
        """Stat what the nodes come from (in a thread) and read again, in workers, what changed."""
        if self._polling:
            return
        self._polling = True
        try:
            parent_stars = tuple(p for s in self.sets or () for p in s.parent_stars)
            sets_key, plan = await asyncio.to_thread(_plan, self.path, refs, parent_stars)
            changed = False
            calls = []
            if sets_key != self._sets_key:
                calls.append(("sets", sets_key, _Work(sets_key, read_sets, (str(self.path),))))
            for iid, work in plan.items():
                if isinstance(work, str):
                    entry = JobParticles("", None, problem=work)
                    if self.jobs.get(iid) != entry:
                        self.jobs[iid] = entry
                        self._keys.pop(iid, None)
                        changed = True
                elif self._keys.get(iid) != work.key:
                    calls.append((iid, work.key, work))
            for gone in [i for i in self.jobs if i not in plan]:
                self.jobs.pop(gone)
                self._keys.pop(gone, None)
                changed = True
            if calls:
                results = await asyncio.gather(
                    *(run_in_worker(WorkKind.LINEAGE, w.fn, *w.args) for _, _, w in calls), return_exceptions=True
                )
                for (what, key, work), res in zip(calls, results, strict=True):
                    self._land(what, key, work, res)
                changed = True
            if changed:
                self.version += 1
        finally:
            self._polling = False

    def _land(self, what: str, key: tuple, work: _Work, res) -> None:
        if isinstance(res, BaseException):
            logger.error("particle map: reading %s of %s failed: %s", what, self.path.name, res, exc_info=res)
            words = f"{type(res).__name__}: {res}"
            if what == "sets":
                self.set_problem = f"the particle sets could not be read · {words}"
                self._sets_key = key
            else:
                self.jobs[what] = JobParticles(work.role, None, problem=words)
                self._keys[what] = key
            return
        if what == "sets":
            self.sets, self.set_problem, self._sets_key = res, "", key
            return
        if isinstance(res, tuple):
            classes, comp = res
            self.jobs[what] = JobParticles("makes", comp, classes)
        else:
            self.jobs[what] = JobParticles(work.role, res)
        self._keys[what] = key


_stores: dict[str, LineageStore] = {}


def lineage_store_for(project_path: Path) -> LineageStore:
    key = str(project_path)
    if key not in _stores:
        _stores[key] = LineageStore(Path(project_path))
    return _stores[key]

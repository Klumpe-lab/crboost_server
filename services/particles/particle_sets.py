"""Particle sets: every cut, split or class subset of particles is a new set with its recipe.

A set never changes after it is written and never changes its parent. It records:

- its **recipe** — the parent(s), the operation and its parameters (`Recipe`), enough to replay it;
- what the operation **produced** — counts per tomogram, the rule funnel, operation facts
  (`details`);
- its **provenance** — who made it, when, with which module and schema, and each parent file's
  mtime at the time (a parent that changed later makes the set *stale*: marked, never rewritten).

Layout, inside the project (one directory per set, written under a temporary name and renamed into
place, so a reader never sees half a set)::

    Analytics/sets/<set_id>/set.json               the ParticleSet record
    Analytics/sets/<set_id>/particles.star         the rows
    Analytics/sets/<set_id>/optimisation_set.star  particles + the tomograms they live in
    Analytics/sets/operations.jsonl                one line per operation, append-only

The store is the directory, not ``project_params.json``: a set is self-describing on disk, an older
server saving the project cannot drop it, and a driver reads one set by path. Downstream jobs see
each set as a synthetic producer of an optimisation set (`services/path_resolution_service.py`).
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import uuid
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from services.particles.records import analytics_root, current_user, file_mtime, now_iso, write_json_atomic

logger = logging.getLogger(__name__)

SETS_SUBDIR = "sets"
SET_FILE = "set.json"
PARTICLES_FILE = "particles.star"
OPTSET_FILE = "optimisation_set.star"
LOG_FILE = "operations.jsonl"
SET_SCHEMA = 1
# Synthetic-producer id of a set in the path resolver: `<prefix><set_id>`.
SET_PRODUCER_PREFIX = "particle_set__"

_ID_RE = re.compile(r"^s(\d{3,})_")


class SetLevel(StrEnum):
    COORDINATES = "coordinates"  # positions only: subtomo extraction (or the Warp export) makes particles of them
    EXTRACTED = "extracted"  # particles with images: Reconstruct / Class3D read them directly


class SetOp(StrEnum):
    RULE_FILTER = "rule_filter"
    CLASS_SUBSET = "class_subset"
    SPLIT = "split"
    DIFFERENCE = "difference"
    INTERSECTION = "intersection"
    DECOY_POSITIONS = "decoy_positions"
    UNION = "union"


class ParentRef(BaseModel):
    """A set's parent: a job's output, another set or a cross-project merge, with the file actually
    read."""

    kind: Literal["job", "set", "merge"]
    instance_id: str = ""  # kind "job": the producing instance
    output: str = ""  # kind "job": candidates | particles | particles_filtered | class3d_data
    set_id: str = ""  # kind "set"
    merge_slug: str = ""  # kind "merge"
    label: str = ""
    particles_star: str
    tomograms_star: str
    level: SetLevel
    species_id: str = ""
    mtime: float | None = None  # particles_star's mtime when the set was made

    @property
    def key(self) -> str:
        if self.kind == "set":
            return f"set:{self.set_id}"
        if self.kind == "merge":
            return f"merge:{self.merge_slug}"
        return f"job:{self.instance_id}:{self.output}"


class RuleKind(StrEnum):
    MIN_SIGMA = "min_sigma"
    MIN_LCC = "min_lcc"
    TOP_N = "top_n"
    DEPTH_BAND = "depth_band"
    FACE_DISTANCE = "face_distance"
    MIN_NN = "min_nn"
    EXCLUDE_TOMOS = "exclude_tomos"
    INCLUDE_TOMOS = "include_tomos"
    AWAY_FROM = "away_from"


class Rule(BaseModel):
    kind: RuleKind
    value: float | None = None  # σ, LCC, N, Å — per kind
    upper: float | None = None  # depth band: upper edge (Å above the pick plane)
    tomos: list[str] = Field(default_factory=list)
    other: ParentRef | None = None  # away_from: the other species' set
    other_min_sigma: float | None = None

    def describe(self) -> str:
        v = self.value
        match self.kind:
            case RuleKind.MIN_SIGMA:
                return f"σ ≥ {v:g}"
            case RuleKind.MIN_LCC:
                return f"LCC ≥ {v:g}"
            case RuleKind.TOP_N:
                return f"top {int(v or 0)} per tomogram"
            case RuleKind.DEPTH_BAND:
                return f"{v:g} Å ≤ depth from the pick plane ≤ {self.upper:g} Å"
            case RuleKind.FACE_DISTANCE:
                return f"≥ {v:g} Å from the volume faces"
            case RuleKind.MIN_NN:
                return f"no two closer than {v:g} Å (higher score kept)"
            case RuleKind.EXCLUDE_TOMOS:
                return f"without {len(self.tomos)} tomogram(s)"
            case RuleKind.INCLUDE_TOMOS:
                return f"only {len(self.tomos)} tomogram(s)"
            case RuleKind.AWAY_FROM:
                other = self.other.label if self.other else "?"
                floor = f" ≥ {self.other_min_sigma:g}σ" if self.other_min_sigma is not None else ""
                return f"farther than {v:g} Å from {other}{floor}"
        return self.kind.value


class Recipe(BaseModel):
    op: SetOp
    parents: list[ParentRef]
    rules: list[Rule] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)

    def describe(self) -> str:
        if self.op == SetOp.RULE_FILTER:
            return " · ".join(r.describe() for r in self.rules) or "all rows"
        if self.params.get("description"):
            return str(self.params["description"])
        return self.op.value


class FunnelStep(BaseModel):
    label: str
    kept: int
    removed: int
    note: str = ""


class ParticleSet(BaseModel):
    id: str
    name: str
    species_id: str = ""
    level: SetLevel
    recipe: Recipe
    n_particles: int
    n_tomograms: int
    per_tomo: dict[str, int] = Field(default_factory=dict)
    funnel: list[FunnelStep] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)
    sibling_group: str = ""
    created_at: str = Field(default_factory=now_iso)
    created_by: str = Field(default_factory=current_user)
    module: str = ""
    schema_version: int = SET_SCHEMA
    particles_star: str = ""
    optimisation_set: str = ""
    tomograms_star: str = ""

    @property
    def producer_id(self) -> str:
        return f"{SET_PRODUCER_PREFIX}{self.id}"

    def stale_parents(self) -> list[ParentRef]:
        """Parents whose file changed (or vanished) after this set was made."""
        out = []
        for p in self.recipe.parents:
            now = file_mtime(p.particles_star)
            if now is None or (p.mtime is not None and abs(now - p.mtime) > 1.0):
                out.append(p)
        return out

    def as_parent(self) -> ParentRef:
        return ParentRef(
            kind="set",
            set_id=self.id,
            label=self.name,
            particles_star=self.particles_star,
            tomograms_star=self.tomograms_star,
            level=self.level,
            species_id=self.species_id,
            mtime=file_mtime(self.particles_star),
        )


# ── The store ────────────────────────────────────────────────────────────────────────────────────


def sets_root(project_path: Path) -> Path:
    return analytics_root(project_path) / SETS_SUBDIR


def set_dir(project_path: Path, set_id: str) -> Path:
    return sets_root(project_path) / set_id


_LIST_MEMO: dict[str, tuple[int, list[ParticleSet]]] = {}


def list_sets(project_path: Path) -> list[ParticleSet]:
    """Every set of the project, oldest first. Re-read only when the sets directory changed (a set
    is published by renaming its directory into place and removed by renaming it away)."""
    root = sets_root(project_path)
    try:
        mtime = root.stat().st_mtime_ns
    except FileNotFoundError:
        return []
    hit = _LIST_MEMO.get(str(root))
    if hit is not None and hit[0] == mtime:
        return list(hit[1])
    out: list[ParticleSet] = []
    for d in sorted(root.iterdir()):
        if not d.is_dir() or d.name.startswith("."):
            continue
        ps = _read_set_file(d / SET_FILE)
        if ps is not None:
            out.append(ps)
    _LIST_MEMO[str(root)] = (mtime, out)
    return list(out)


def get_set(project_path: Path, set_id: str) -> ParticleSet | None:
    """One set by id, read directly (current even where a directory listing would be cached)."""
    return _read_set_file(set_dir(project_path, set_id) / SET_FILE)


def _read_set_file(path: Path) -> ParticleSet | None:
    try:
        return ParticleSet.model_validate_json(path.read_text())
    except FileNotFoundError:
        return None
    except ValueError as e:
        # A damaged record names a set that cannot be used; it is logged, not silently skipped.
        logger.error("Unreadable particle set %s: %s", path, e)
        return None


def allocate_id(project_path: Path, name: str) -> str:
    """`s<NNN>_<slug>`: the next free sequence number and the name in filesystem-safe form."""
    root = sets_root(project_path)
    numbers = [0]
    if root.is_dir():
        for d in root.iterdir():
            m = _ID_RE.match(d.name.lstrip("."))
            if m:
                numbers.append(int(m.group(1)))
    slug = re.sub(r"[^A-Za-z0-9]+", "_", name.replace("≥", "ge").replace("σ", "sigma")).strip("_").lower()[:40]
    return f"s{max(numbers) + 1:03d}_{slug or 'set'}"


def publish_set(project_path: Path, pset: ParticleSet, write_rows) -> ParticleSet:
    """Materialize `pset` and publish it: `write_rows(particles_path)` writes the rows into a
    temporary directory, which gets the optimisation set and set.json and is then renamed into
    place. Logs the operation. Returns the set with its final paths."""
    from services.subtomo_merge import write_optimisation_set

    root = sets_root(project_path)
    root.mkdir(parents=True, exist_ok=True)
    final = root / pset.id
    if final.exists():
        raise FileExistsError(f"particle set {pset.id} already exists")
    tmp = root / f".tmp-{pset.id}-{uuid.uuid4().hex[:6]}"
    tmp.mkdir()
    try:
        write_rows(tmp / PARTICLES_FILE)
        pset = pset.model_copy(
            update={"particles_star": str(final / PARTICLES_FILE), "optimisation_set": str(final / OPTSET_FILE)}
        )
        # The optimisation set names the final paths, which exist once the directory is renamed.
        write_optimisation_set(
            tmp / OPTSET_FILE, particles_star=final / PARTICLES_FILE, tomograms_star=Path(pset.tomograms_star)
        )
        write_json_atomic(tmp / SET_FILE, json.loads(pset.model_dump_json()))
        tmp.rename(final)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    log_operation(
        project_path,
        {
            "event": "create",
            "set_id": pset.id,
            "name": pset.name,
            "species_id": pset.species_id,
            "op": pset.recipe.op.value,
            "recipe": json.loads(pset.recipe.model_dump_json()),
            "n_particles": pset.n_particles,
            "n_tomograms": pset.n_tomograms,
            "level": pset.level.value,
        },
    )
    logger.info("Particle set %s created (%d particles)", pset.id, pset.n_particles)
    return pset


def delete_set(project_path: Path, set_id: str, *, reason: str = "") -> None:
    """Remove a set's directory and log it. The log keeps the recipe, so it can be made again."""
    d = set_dir(project_path, set_id)
    ps = get_set(project_path, set_id)
    if ps is None and not d.exists():
        raise FileNotFoundError(f"no particle set {set_id}")
    trash = d.with_name(f".deleted-{set_id}-{uuid.uuid4().hex[:6]}")
    d.rename(trash)
    shutil.rmtree(trash, ignore_errors=True)
    log_operation(
        project_path,
        {
            "event": "delete",
            "set_id": set_id,
            "name": ps.name if ps else "",
            "recipe": json.loads(ps.recipe.model_dump_json()) if ps else None,
            "reason": reason,
        },
    )


def log_operation(project_path: Path, event: dict[str, Any]) -> None:
    """Append one operation to the project's set log. Only the server writes it."""
    root = sets_root(project_path)
    root.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"at": now_iso(), "user": current_user(), **event}, default=str)
    with open(root / LOG_FILE, "a") as f:
        f.write(line + "\n")


def read_log(project_path: Path, limit: int = 200) -> list[dict[str, Any]]:
    """The newest `limit` operations, newest first."""
    path = sets_root(project_path) / LOG_FILE
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        return []
    out = []
    for line in reversed(lines[-limit:]):
        try:
            out.append(json.loads(line))
        except ValueError:
            logger.warning("Skipping a damaged line in %s", path)
    return out


def jobs_reading_set(state, set_id: str) -> list[str]:
    """Instance ids of jobs whose input overrides name this set."""
    needle = f"{SET_PRODUCER_PREFIX}{set_id}"
    out = []
    for iid, jm in (state.jobs or {}).items():
        overrides = getattr(jm, "source_overrides", None) or {}
        if any(needle == str(v).split(":", 1)[-1] for v in overrides.values()):
            out.append(iid)
    return out

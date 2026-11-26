"""Operations that make particle sets: pure functions over star tables, and the one place each
operation turns into a published `ParticleSet` (`services/particles/particle_sets.py`).

Rule filter (`apply_rules`): each rule narrows the rows the previous rules kept, and the funnel
records how many each one removed. Rules about positions use the tomograms' own geometry: the pick
plane (a robust plane fit through the tomogram's picks — where the lamella holds them), the volume
faces (the reconstruction's extent), the nearest neighbour, another set's picks.
"""

from __future__ import annotations

import logging
import math
from collections import OrderedDict
from dataclasses import dataclass, field
from itertools import pairwise
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import starfile

from services.models_base import JobType
from services.particles import analytics, filament_polarity, filament_seam, sites, subparticles
from services.particles.analytics import fit_pick_plane, min_distances, plane_depth, tomogram_extents
from services.particles.analytics_store import job_dir_of
from services.particles.coords import CENTERED_COLS
from services.particles.particle_sets import (
    FunnelStep,
    ParentRef,
    ParticleSet,
    Recipe,
    Rule,
    RuleKind,
    SetOp,
    SetLevel,
    allocate_id,
    get_set,
    list_sets,
    publish_set,
    set_read_by,
)
from services.particles.pick_merge import _greedy_drop_mask
from services.particles.records import file_mtime
from services.particles.species_jobs import jobs_for_species
from services.project_state import MERGED_DIR_NAME

logger = logging.getLogger(__name__)

# ── Star tables ──────────────────────────────────────────────────────────────────────────────────


@dataclass
class StarTable:
    """A particles or candidates star as read: every block, and which one holds the rows."""

    path: Path
    blocks: dict[str, Any]
    key: str

    @property
    def df(self) -> pd.DataFrame:
        return self.blocks[self.key]

    def write(self, rows: pd.DataFrame, out: Path) -> None:
        """The same star with `rows` in place of its particle table: optics and general blocks as read."""
        blocks = dict(self.blocks)
        blocks[self.key] = rows.reset_index(drop=True)
        starfile.write(blocks, out, overwrite=True)


_TABLE_CACHE: OrderedDict[tuple[str, float | None], StarTable] = OrderedDict()
_TABLE_CACHE_SIZE = 4


def read_table(path: str | Path) -> StarTable:
    """A star's particle table (the block carrying rlnTomoName). Recently read tables are reused
    while their file is unchanged, so a live preview re-reads nothing."""
    path = Path(path)
    key = (str(path), file_mtime(path))
    hit = _TABLE_CACHE.get(key)
    if hit is not None:
        _TABLE_CACHE.move_to_end(key)
        return hit
    blocks = starfile.read(path, always_dict=True)
    block = next(
        (k for k, v in blocks.items() if isinstance(v, pd.DataFrame) and analytics.TOMO_COL in v.columns), None
    )
    if block is None:
        raise ValueError(f"{path} has no particle table (no rlnTomoName column)")
    missing = [c for c in CENTERED_COLS if c not in blocks[block].columns]
    if missing:
        raise ValueError(f"{path} has no centred coordinates ({', '.join(missing)})")
    table = StarTable(path, blocks, block)
    _TABLE_CACHE[key] = table
    while len(_TABLE_CACHE) > _TABLE_CACHE_SIZE:
        _TABLE_CACHE.popitem(last=False)
    return table


def greedy_keep(xyz: np.ndarray, order: np.ndarray, radius: float, groups: np.ndarray | None = None) -> np.ndarray:
    """Boolean mask over rows: walking `order` (best first), a row is kept unless a kept row lies
    within `radius` Å (a kept row of its own group, when `groups` gives each row one)."""
    keep = np.zeros(len(xyz), dtype=bool)
    if len(order) == 0:
        return keep
    dropped = _greedy_drop_mask(xyz[order], radius, None if groups is None else np.asarray(groups)[order])
    keep[order[~dropped]] = True
    return keep


# ── Rule filter ──────────────────────────────────────────────────────────────────────────────────


@dataclass
class RuleContext:
    """What rules need beyond the table: the tomograms' extents and other sets' tables, loaded only
    for the rules that use them."""

    tomograms_star: str
    _extents: dict[str, np.ndarray] | None = None
    _others: dict[str, StarTable] = field(default_factory=dict)

    def extents(self) -> dict[str, np.ndarray]:
        if self._extents is None:
            self._extents = tomogram_extents(self.tomograms_star)
        return self._extents

    def other(self, ref: ParentRef) -> StarTable:
        if ref.particles_star not in self._others:
            self._others[ref.particles_star] = read_table(ref.particles_star)
        return self._others[ref.particles_star]


def _ranking(df: pd.DataFrame) -> np.ndarray:
    """Score each row ranks by: σ when the table has it, else LCC, else its row order."""
    sig = analytics.particle_sigma_scores(df)
    if sig is not None:
        return np.nan_to_num(sig, nan=-np.inf)
    if analytics.SCORE_COL in df.columns:
        return df[analytics.SCORE_COL].astype(float).to_numpy()
    return -np.arange(len(df), dtype=float)


def apply_rules(df: pd.DataFrame, rules: list[Rule], ctx: RuleContext) -> tuple[np.ndarray, list[FunnelStep]]:
    """Which rows survive `rules`, applied in order, and one funnel step per rule."""
    keep = np.ones(len(df), dtype=bool)
    tomo = df[analytics.TOMO_COL].astype(str).to_numpy()
    xyz = df[CENTERED_COLS].to_numpy(dtype=float)
    rank = _ranking(df)
    sig = analytics.particle_sigma_scores(df)
    steps: list[FunnelStep] = []
    for rule in rules:
        before = int(keep.sum())
        note = ""
        v = rule.value
        match rule.kind:
            case RuleKind.MIN_SIGMA:
                if sig is None:
                    raise ValueError("these particles carry no rlnLCCmax / rlnSearchStd, so they have no score in σ")
                keep &= np.nan_to_num(sig, nan=-np.inf) >= float(v)
            case RuleKind.MIN_LCC:
                if analytics.SCORE_COL not in df.columns:
                    raise ValueError("these particles carry no rlnLCCmax")
                keep &= df[analytics.SCORE_COL].astype(float).to_numpy() >= float(v)
            case RuleKind.TOP_N:
                n = int(v or 0)
                for t in np.unique(tomo[keep]):
                    idx = np.flatnonzero(keep & (tomo == t))
                    if len(idx) > n:
                        keep[idx[np.argsort(-rank[idx], kind="stable")[n:]]] = False
            case RuleKind.DEPTH_BAND:
                lo, hi = float(v), float(rule.upper if rule.upper is not None else np.inf)
                unfit = 0
                for t in np.unique(tomo[keep]):
                    in_tomo = tomo == t
                    coef = fit_pick_plane(xyz[in_tomo])
                    if coef is None:
                        unfit += 1
                        continue
                    depth = plane_depth(xyz, coef)
                    keep &= ~in_tomo | ((depth >= lo) & (depth <= hi))
                if unfit:
                    note = f"{unfit} tomogram(s) under {analytics.PLANE_MIN_PICKS} picks have no plane; kept whole"
            case RuleKind.FACE_DISTANCE:
                extents = ctx.extents()
                unknown = sorted({t for t in np.unique(tomo[keep]) if t not in extents})
                for t, ext in extents.items():
                    in_tomo = tomo == t
                    if not in_tomo.any():
                        continue
                    margin = (ext / 2.0)[None, :] - np.abs(xyz[in_tomo])
                    ok = margin.min(axis=1) >= float(v)
                    keep[np.flatnonzero(in_tomo)[~ok]] = False
                if unknown:
                    note = f"{len(unknown)} tomogram(s) missing from the tomograms star; kept whole"
            case RuleKind.MIN_NN:
                for t in np.unique(tomo[keep]):
                    idx = np.flatnonzero(keep & (tomo == t))
                    order = np.argsort(-rank[idx], kind="stable")
                    kept_local = greedy_keep(xyz[idx], order, float(v))
                    keep[idx[~kept_local]] = False
            case RuleKind.EXCLUDE_TOMOS:
                keep &= ~np.isin(tomo, list(rule.tomos))
            case RuleKind.INCLUDE_TOMOS:
                keep &= np.isin(tomo, list(rule.tomos))
            case RuleKind.AWAY_FROM:
                if rule.other is None:
                    raise ValueError("away-from rule names no other set")
                other = ctx.other(rule.other).df
                other_xyz = other[CENTERED_COLS].to_numpy(dtype=float)
                other_tomo = other[analytics.TOMO_COL].astype(str).to_numpy()
                if rule.other_min_sigma is not None:
                    osig = analytics.particle_sigma_scores(other)
                    if osig is None:
                        raise ValueError(f"{rule.other.label} carries no σ, so it cannot be cut at a σ")
                    sel = np.nan_to_num(osig, nan=-np.inf) >= float(rule.other_min_sigma)
                    other_xyz, other_tomo = other_xyz[sel], other_tomo[sel]
                for t in np.unique(tomo[keep]):
                    idx = np.flatnonzero(keep & (tomo == t))
                    d = min_distances(xyz[idx], other_xyz[other_tomo == t])
                    keep[idx[d <= float(v)]] = False
        after = int(keep.sum())
        steps.append(FunnelStep(label=rule.describe(), kept=after, removed=before - after, note=note))
    return keep, steps


def per_tomo_counts(df: pd.DataFrame) -> dict[str, int]:
    return {str(k): int(v) for k, v in df[analytics.TOMO_COL].astype(str).value_counts().sort_index().items()}


def preview_rule_filter(parent: ParentRef, rules: list[Rule]) -> list[FunnelStep]:
    """The funnel `rules` would give on `parent`, without writing anything."""
    table = read_table(parent.particles_star)
    _keep, steps = apply_rules(table.df, rules, RuleContext(parent.tomograms_star))
    return [FunnelStep(label=f"{parent.label}", kept=len(table.df), removed=0), *steps]


def create_rule_filter_set(
    project_path: Path, *, parent: ParentRef, rules: list[Rule], name: str, species_id: str = ""
) -> ParticleSet:
    """A new set: the rows of `parent` that pass `rules`. A row subset of an extracted parent is
    itself extracted (same images, same optics), so it needs no new extraction."""
    table = read_table(parent.particles_star)
    keep, steps = apply_rules(table.df, rules, RuleContext(parent.tomograms_star))
    subset = table.df[keep]
    if subset.empty:
        raise ValueError("the rules keep no particle; nothing to make a set of")
    parent = parent.model_copy(update={"mtime": file_mtime(parent.particles_star)})
    pset = ParticleSet(
        id=allocate_id(project_path, name),
        name=name,
        species_id=species_id or parent.species_id,
        level=parent.level,
        recipe=Recipe(op=SetOp.RULE_FILTER, parents=[parent], rules=rules),
        n_particles=len(subset),
        n_tomograms=subset[analytics.TOMO_COL].nunique(),
        per_tomo=per_tomo_counts(subset),
        funnel=[FunnelStep(label=parent.label, kept=len(table.df), removed=0), *steps],
        notes=[s.note for s in steps if s.note],
        module=__name__,
        tomograms_star=parent.tomograms_star,
    )
    return publish_set(project_path, pset, lambda out: table.write(subset, out))


# ── What a set can be made from ──────────────────────────────────────────────────────────────────


def parent_options(state, project_path: Path, species_id: str) -> list[ParentRef]:
    """Every table of one species a set can be derived from: each extraction job's candidates
    (coordinates), each subtomo job's particles and its curated subset (extracted), the species'
    existing sets, and the project's cross-project merges. Disk checks only; no star is read."""
    out: list[ParentRef] = []
    for iid, jm in jobs_for_species(state, species_id):
        jd = job_dir_of(state, project_path, iid)
        if jd is None:
            continue
        if jm.job_type == JobType.TEMPLATE_EXTRACT_PYTOM and (jd / "candidates.star").exists():
            out.append(
                ParentRef(
                    kind="job",
                    instance_id=iid,
                    output="candidates",
                    label=f"candidates {jd.name}",
                    particles_star=str(jd / "candidates.star"),
                    tomograms_star=str(jd / "tomograms.star"),
                    level=SetLevel.COORDINATES,
                    species_id=species_id,
                )
            )
        elif jm.job_type == JobType.SUBTOMO_EXTRACTION and (jd / "particles.star").exists():
            for output, star, what in (
                ("particles_filtered", "particles_filtered.star", "curated"),
                ("particles", "particles.star", "all"),
            ):
                if (jd / star).exists():
                    out.append(
                        ParentRef(
                            kind="job",
                            instance_id=iid,
                            output=output,
                            label=f"subtomo {jd.name} ({what})",
                            particles_star=str(jd / star),
                            tomograms_star=str(jd / "tomograms.star"),
                            level=SetLevel.EXTRACTED,
                            species_id=species_id,
                        )
                    )
    out.extend(ps.as_parent() for ps in list_sets(project_path) if ps.species_id == species_id)
    for m in state.aggregation_merges or []:
        merged = Path(project_path) / MERGED_DIR_NAME / m.slug
        if (merged / "particles.star").exists():
            out.append(
                ParentRef(
                    kind="merge",
                    merge_slug=m.slug,
                    label=f"merge {m.name or m.slug}",
                    particles_star=str(merged / "particles.star"),
                    tomograms_star=str(merged / "tomograms.star"),
                    level=SetLevel.EXTRACTED,
                    species_id=species_id,
                )
            )
    return out


def parent_options_all_species(state, project_path: Path) -> list[ParentRef]:
    """`parent_options` of every registered species."""
    out: list[ParentRef] = []
    for sp in state.species_registry or []:
        out.extend(p for p in parent_options(state, project_path, sp.id) if p.kind != "merge")
    return out


def overlap_counts(
    a: ParentRef, cut_a: float | None, b: ParentRef, min_sigma_b: float | None, radius: float
) -> dict[str, Any]:
    """How many of `a`'s picks (at or above `cut_a` σ, when given) lie within `radius` Å of one of
    `b`'s (at or above `min_sigma_b`): one position claimed by two species. Per tomogram and pooled."""

    def selected(ref: ParentRef, floor: float | None) -> pd.DataFrame:
        df = read_table(ref.particles_star).df
        if floor is None:
            return df
        sig = analytics.particle_sigma_scores(df)
        if sig is None:
            raise ValueError(f"{ref.label} carries no σ, so it cannot be cut at {floor:g}σ")
        return df[np.nan_to_num(sig, nan=-np.inf) >= floor]

    da, db = selected(a, cut_a), selected(b, min_sigma_b)
    ta, tb = da[analytics.TOMO_COL].astype(str).to_numpy(), db[analytics.TOMO_COL].astype(str).to_numpy()
    xa, xb = da[CENTERED_COLS].to_numpy(dtype=float), db[CENTERED_COLS].to_numpy(dtype=float)
    per: dict[str, int] = {}
    for t in np.unique(ta):
        n = int(np.count_nonzero(min_distances(xa[ta == t], xb[tb == t]) <= radius))
        if n:
            per[str(t)] = n
    return {"n_a": len(da), "n_b": len(db), "n_overlap": sum(per.values()), "per_tomo": per}


# ── Class subset ─────────────────────────────────────────────────────────────────────────────────


def create_class_subset_set(
    project_path: Path, state, *, class3d_instance: str, classes: list[int], name: str, species_id: str = ""
) -> ParticleSet:
    """A new set: the particles a Class 3D job's last iteration put in `classes`, with their refined
    poses (the rows of run_itNNN_data.star). Extracted, so Reconstruct particle and Class 3D read it
    directly."""
    from services.particles.class_readout import class3d_iterations
    from services.subtomo_merge import _parse_optimisation_set

    job_dir = job_dir_of(state, project_path, class3d_instance)
    if job_dir is None:
        raise FileNotFoundError(f"{class3d_instance} has no job directory")
    its = class3d_iterations(job_dir)
    if not its:
        raise FileNotFoundError(f"{class3d_instance} has written no iteration yet")
    final = its[-1]
    data_star = job_dir / f"run_it{final:03d}_data.star"
    optset = job_dir / f"run_it{final:03d}_optimisation_set.star"
    if not optset.exists():
        raise FileNotFoundError(f"{optset} is missing; it names the tomograms these particles live in")
    _particles, tomograms = _parse_optimisation_set(optset)
    table = read_table(data_star)
    if "rlnClassNumber" not in table.df.columns:
        raise ValueError(f"{data_star} has no rlnClassNumber column")
    keep = table.df["rlnClassNumber"].astype(int).isin([int(c) for c in classes])
    subset = table.df[keep]
    if subset.empty:
        raise ValueError(f"classes {classes} hold no particle in iteration {final}")
    parent = ParentRef(
        kind="job",
        instance_id=class3d_instance,
        output="class3d_data",
        label=f"Class 3D {job_dir.name} it{final:03d}",
        particles_star=str(data_star),
        tomograms_star=str(tomograms),
        level=SetLevel.EXTRACTED,
        species_id=species_id,
        mtime=file_mtime(data_star),
    )
    shown = ", ".join(str(c) for c in sorted(classes))
    pset = ParticleSet(
        id=allocate_id(project_path, name),
        name=name,
        species_id=species_id,
        level=SetLevel.EXTRACTED,
        recipe=Recipe(
            op=SetOp.CLASS_SUBSET,
            parents=[parent],
            params={
                "classes": sorted(int(c) for c in classes),
                "iteration": final,
                "description": f"classes {shown} of {job_dir.name}, iteration {final}",
            },
        ),
        n_particles=len(subset),
        n_tomograms=subset[analytics.TOMO_COL].nunique() if analytics.TOMO_COL in subset.columns else 0,
        per_tomo=per_tomo_counts(subset) if analytics.TOMO_COL in subset.columns else {},
        funnel=[
            FunnelStep(label=parent.label, kept=len(table.df), removed=0),
            FunnelStep(label=f"classes {shown}", kept=len(subset), removed=len(table.df) - len(subset)),
        ],
        module=__name__,
        tomograms_star=str(tomograms),
    )
    return publish_set(project_path, pset, lambda out: table.write(subset, out))


# ── Polarity of filaments ────────────────────────────────────────────────────────────────────────


def polarity_vote(job_dir: Path, *, min_majority: float = filament_polarity.MIN_MAJORITY):
    """The vote of a helical Refine 3D's segments (`filament_polarity.vote`), with the files it read."""
    files = filament_polarity.refine3d_files(job_dir)
    table = read_table(files["data"])
    if filament_polarity.TUBE_COL not in table.df.columns:
        raise ValueError(f"{Path(job_dir).name} refined no filaments: its particles carry no rlnHelicalTubeID")
    started = read_table(files["started"]).df
    box = filament_polarity.particle_box_ang(table.blocks)
    return filament_polarity.vote(table.df, started, min_majority=min_majority, box_ang=box), files, table


def polarity_readout(job_dir: Path, *, min_majority: float = filament_polarity.MIN_MAJORITY) -> dict[str, Any]:
    """What the vote over a helical Refine 3D would decide, for its Results tab and `get_job`: the counts, the
    pieces test and one row per filament. Writes nothing."""
    v, files, _table = polarity_vote(job_dir, min_majority=min_majority)
    fil = v.filaments
    return {
        **v.counts,
        "iteration": files["iteration"],
        "converged": files["iteration"] is None,
        "min_majority": v.min_majority,
        "pieces": v.pieces,
        "per_filament": [
            {
                "tomo": str(r[filament_polarity.TOMO_COL]),
                "tube": int(r[filament_polarity.TUBE_COL]),
                "segments": int(r["segments"]),
                "reversed": int(r["n_reversed"]),
                "majority": round(float(r["majority"]), 3),
                "verdict": str(r["verdict"]),
            }
            for _, r in fil.iterrows()
        ],
    }


def create_polarity_set(
    project_path: Path,
    state,
    *,
    refine3d_instance: str,
    name: str,
    species_id: str = "",
    min_majority: float = filament_polarity.MIN_MAJORITY,
    keep_undecided: bool = False,
) -> ParticleSet:
    """A new set: a helical Refine 3D's segments with each filament's direction decided by the vote of its
    segments (`filament_polarity`): the minority turned round, the priors along the decided direction, the flip
    ratio 0 so the next refinement keeps it. Filaments without a `min_majority` are left out, or kept as they
    went in with `keep_undecided`. Extracted: the segments' images are the refinement's."""
    from services.subtomo_merge import _parse_optimisation_set

    job_dir = job_dir_of(state, project_path, refine3d_instance)
    if job_dir is None:
        raise FileNotFoundError(f"{refine3d_instance} has no job directory")
    v, files, table = polarity_vote(job_dir, min_majority=min_majority)
    if not files["optset"].exists():
        raise FileNotFoundError(f"{files['optset'].name} is missing; it names the tomograms these particles live in")
    _particles, tomograms = _parse_optimisation_set(files["optset"])
    rows = filament_polarity.decided_rows(v, keep_undecided=keep_undecided)
    if rows.empty:
        raise ValueError(f"no filament of {job_dir.name} has a {min_majority:.0%} majority")
    c = v.counts
    when = "converged" if files["iteration"] is None else f"iteration {files['iteration']}, not converged"
    parent = ParentRef(
        kind="job",
        instance_id=refine3d_instance,
        output="refine3d_data",
        label=f"Refine 3D {job_dir.name} ({when})",
        particles_star=str(files["data"]),
        tomograms_star=str(tomograms),
        level=SetLevel.EXTRACTED,
        species_id=species_id,
        mtime=file_mtime(files["data"]),
    )
    funnel = [FunnelStep(label=parent.label, kept=c["segments"], removed=0)]
    turned = f"{c['segments_turned']:,} segments turned round to their filament's direction"
    if keep_undecided:
        funnel.append(FunnelStep(label="decided", kept=c["segments"], removed=0, note=turned))
    else:
        funnel.append(
            FunnelStep(
                label=f"{c['kept'] + c['reversed']} of {c['filaments']} filaments decided",
                kept=len(rows),
                removed=c["segments_undecided"],
                note=turned,
            )
        )
    undecided = v.filaments[v.filaments["verdict"] == filament_polarity.UNDECIDED]
    pieces = v.pieces or {}
    notes = []
    if pieces.get("decided"):
        notes.append(
            f"two pieces of one filament more than a box apart chose the same direction in {pieces['agree']} of "
            f"{pieces['decided']} filaments (about half when the map cannot tell the ends apart)"
        )
    pset = ParticleSet(
        id=allocate_id(project_path, name),
        name=name,
        species_id=species_id,
        level=SetLevel.EXTRACTED,
        recipe=Recipe(
            op=SetOp.POLARITY,
            parents=[parent],
            params={
                "min_majority": float(min_majority),
                "keep_undecided": bool(keep_undecided),
                "iteration": files["iteration"],
                "description": f"the filaments of {job_dir.name} turned to their segments' majority "
                f"({min_majority:.0%}), {when}",
            },
        ),
        n_particles=len(rows),
        n_tomograms=rows[analytics.TOMO_COL].nunique(),
        per_tomo=per_tomo_counts(rows),
        funnel=funnel,
        details={
            **c,
            "pieces": pieces,
            "undecided_filaments": [
                {
                    "tomo": str(r[filament_polarity.TOMO_COL]),
                    "tube": int(r[filament_polarity.TUBE_COL]),
                    "segments": int(r["segments"]),
                    "majority": round(float(r["majority"]), 3),
                }
                for _, r in undecided.iterrows()
            ],
        },
        notes=notes,
        module=__name__,
        tomograms_star=str(tomograms),
    )
    return publish_set(project_path, pset, lambda out: table.write(rows, out))


# ── Seam and register of filaments ───────────────────────────────────────────────────────────────


def _seam_inputs(state, project_path: Path, class3d_instance: str):
    """What a seam vote reads of a Class 3D that scored an expansion set: its last data star, the tomograms it
    names, the expansion set it read and that set's lattice."""
    from services.particles.class_readout import class3d_iterations
    from services.subtomo_merge import _parse_optimisation_set

    jm = (state.jobs or {}).get(class3d_instance)
    if jm is None:
        raise ValueError(f"no job {class3d_instance}")
    if jm.job_type != JobType.CLASS3D:
        raise ValueError(
            f"{class3d_instance} is a {jm.job_type.value} job: a seam is voted on a Class 3D that scored an "
            "expansion set (Skip alignment)"
        )
    set_id = set_read_by(jm)
    scored_set = get_set(project_path, set_id) if set_id else None
    if scored_set is None or scored_set.recipe.op != SetOp.EXPANSION:
        raise ValueError(
            f"{class3d_instance} did not read an expansion set: a seam is voted on the scores of every registration "
            "of every segment (Make expansion set on the helical Refine 3D's Results, then a Class 3D with Skip "
            "alignment on that set)"
        )
    expansion = sites.expansion_of(dict(scored_set.recipe.params.get("expansion") or {}))
    job_dir = job_dir_of(state, project_path, class3d_instance)
    if job_dir is None:
        raise FileNotFoundError(f"{class3d_instance} has not run yet")
    its = class3d_iterations(job_dir)
    if not its:
        raise FileNotFoundError(f"{class3d_instance} has written no iteration yet")
    final = its[-1]
    data = job_dir / f"run_it{final:03d}_data.star"
    optset = job_dir / f"run_it{final:03d}_optimisation_set.star"
    if not optset.exists():
        raise FileNotFoundError(f"{optset.name} is missing; it names the tomograms these particles live in")
    _particles, tomograms = _parse_optimisation_set(optset)
    notes = []
    if getattr(jm, "skip_align", None) is False:
        notes.append(
            f"{job_dir.name} aligned its particles: its scores are of poses it moved, not of the registrations"
        )
    if getattr(jm, "firstiter_cc", None) is True and final == 1:
        notes.append(
            f"{job_dir.name} scored by cross-correlation (First iteration CC on); the log-likelihood (off) weighs "
            "each frequency by its noise and carries more of what tells the units apart"
        )
    return {
        "job_dir": job_dir,
        "data": data,
        "tomograms": tomograms,
        "iteration": final,
        "scored_set": scored_set,
        "expansion": expansion,
        "notes": notes,
        "species_id": getattr(jm, "species_id", "") or scored_set.species_id,
    }


def seam_start(project_path: Path, state, class3d_instance: str) -> dict[str, Any]:
    """Whether a seam can be voted on this job (a Class 3D that read an expansion set of a lattice), and the
    values its block starts from: those of the last seam set made of it, else the radius of the last helix site
    given for a sub-unit of its species. A value nobody gave stays None."""
    out: dict[str, Any] = {"applies": False, "radius_ang": None, "min_segments": None, "block_segments": None}
    jm = (state.jobs or {}).get(class3d_instance)
    if jm is None or jm.job_type != JobType.CLASS3D:
        return out
    set_id = set_read_by(jm)
    scored_set = get_set(project_path, set_id) if set_id else None
    if scored_set is None or scored_set.recipe.op != SetOp.EXPANSION:
        return out
    if (scored_set.recipe.params.get("expansion") or {}).get("kind") != sites.LATTICE:
        return out
    out.update(applies=True, scored_set=scored_set.id, scored_set_name=scored_set.name)
    made = list_sets(project_path)
    mine = [
        ps
        for ps in made
        if ps.recipe.op == SetOp.SEAM and ps.recipe.parents and ps.recipe.parents[0].instance_id == class3d_instance
    ]
    if mine:
        last = mine[-1].recipe.params
        out.update({k: last.get(k) for k in ("radius_ang", "min_segments", "block_segments")})
        return out
    for ps in reversed(made):
        site = ps.recipe.params.get("site") or {}
        same_species = ps.recipe.params.get("parent_species") == scored_set.species_id
        if ps.recipe.op == SetOp.SUBPARTICLES and same_species and site.get("kind") == sites.HELIX:
            out["radius_ang"] = site.get("radius_ang")
            break
    return out


def seam_vote(state, project_path: Path, class3d_instance: str, *, values: dict[str, Any]):
    """The vote (`filament_seam.vote`) over a Class 3D's scores of an expansion set, with what it read. `values`:
    `radius_ang` (where the units sit from the axis, the person's), and optionally `min_segments` and
    `block_segments`."""
    inputs = _seam_inputs(state, project_path, class3d_instance)
    table = read_table(inputs["data"])
    v = dict(values or {})
    kwargs: dict[str, Any] = {"radius_ang": v.get("radius_ang"), "min_segments": v.get("min_segments")}
    if v.get("block_segments") is not None:
        kwargs["block_segments"] = v["block_segments"]
    return filament_seam.vote(table.df, inputs["expansion"], **kwargs), inputs, table


def _seam_pieces(pieces: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "tomo": str(r[filament_seam.TOMO_COL]),
            "tube": str(r[filament_seam.TUBE_COL]),
            "piece": str(r["piece"]).rsplit("#", 1)[-1],
            "segments": int(r["segments"]),
            "registration": int(r["k"]),
            "halves_steps_apart": None if pd.isna(r["halves"]) else int(r["halves"]),
            "own_vs_rest_steps": None if pd.isna(r["own_vs_rest"]) else float(r["own_vs_rest"]),
            "verdict": str(r["verdict"]),
            "why": str(r["why"]),
        }
        for _, r in pieces.iterrows()
    ]


def seam_readout(project_path: Path, state, class3d_instance: str, *, values: dict[str, Any]) -> dict[str, Any]:
    """What the seam vote over a Class 3D's scores decides, for its Results and `get_seam`: the counts, whether
    the vote means anything (`checks`), one row per piece of filament and the sets already made. Writes nothing."""
    v, inputs, _table = seam_vote(state, project_path, class3d_instance, values=values)
    made = [
        ps
        for ps in list_sets(project_path)
        if ps.recipe.op == SetOp.SEAM and ps.recipe.parents and ps.recipe.parents[0].instance_id == class3d_instance
    ]
    return {
        **v.counts,
        "registrations": v.lattice.n,
        "scored_set": inputs["scored_set"].id,
        "lattice": inputs["expansion"].description,
        "iteration": inputs["iteration"],
        **v.params,
        "checks": v.checks,
        "notes": list(inputs["notes"]),
        "per_piece": _seam_pieces(v.pieces),
        "sets": [
            {
                "set": ps.id,
                "name": ps.name,
                "segments": ps.n_particles,
                "held_out": bool(ps.recipe.params.get("held_out")),
                "min_segments": ps.recipe.params.get("min_segments"),
            }
            for ps in made
        ],
    }


def create_seam_set(
    project_path: Path, state, *, class3d_instance: str, name: str, values: dict[str, Any], held_out: bool = True
) -> ParticleSet:
    """A new extracted set: the segments of every filament piece long enough to vote, each on the registration of
    its lattice the vote chose (`filament_seam`): the same images, the pose and origin of that copy. With
    `held_out` a segment takes the registration its piece voted without the segment's own block and that block's
    neighbours, so an average of the set holds nothing its own noise chose; without it, the whole piece's. Pieces
    shorter than `min_segments` are left out and counted. The vote's values are kept in the recipe."""
    v, inputs, table = seam_vote(state, project_path, class3d_instance, values=values)
    c = v.counts
    if not c["decided"]:
        raise ValueError(
            f"no piece of filament in {inputs['job_dir'].name} has {v.params['min_segments']} segments on one lattice"
        )
    rows = filament_seam.registered_rows(v, table.df, held_out=held_out)
    job_dir = inputs["job_dir"]
    parent = ParentRef(
        kind="job",
        instance_id=class3d_instance,
        output="class3d_data",
        label=f"Class 3D {job_dir.name} it{inputs['iteration']:03d}",
        particles_star=str(inputs["data"]),
        tomograms_star=str(inputs["tomograms"]),
        level=SetLevel.EXTRACTED,
        species_id=inputs["species_id"],
        mtime=file_mtime(inputs["data"]),
    )
    how = (
        "each segment on the registration the rest of its piece voted"
        if held_out
        else "each segment on its piece's registration"
    )
    funnel = [
        FunnelStep(
            label=parent.label,
            kept=c["segments"],
            removed=0,
            note=f"{len(table.df):,} scored copies, {v.lattice.n} to a segment",
        ),
        FunnelStep(
            label="on its neighbour's lattice",
            kept=c["segments"] - c["segments_left_out"],
            removed=c["segments_left_out"],
        ),
        FunnelStep(
            label=f"{c['decided']} of {c['pieces']} pieces of {v.params['min_segments']} segments or more",
            kept=len(rows),
            removed=c["segments_undecided"],
            note=how,
        ),
    ]
    by_length = {b["min_segments"]: b for b in v.checks.get("by_length", [])}
    notes = list(inputs["notes"])
    nearest = max((m for m in by_length if m <= v.params["min_segments"]), default=None)
    if nearest is not None:
        b = by_length[nearest]
        notes.append(
            f"the two halves of a piece of {nearest} segments or more, which share no data, label "
            f"{b['sites_alike']:.0%} of the lattice's sites alike ({b['pieces']} pieces; 50 % when nothing is known)"
        )
    undecided = v.pieces[v.pieces["verdict"] != filament_seam.DECIDED]
    pset = ParticleSet(
        id=allocate_id(project_path, name),
        name=name,
        species_id=inputs["species_id"],
        level=SetLevel.EXTRACTED,
        recipe=Recipe(
            op=SetOp.SEAM,
            parents=[parent],
            params={
                **v.params,
                "held_out": bool(held_out),
                "scored_set": inputs["scored_set"].id,
                "lattice": inputs["expansion"].description,
                "iteration": inputs["iteration"],
                "description": f"the segments of {job_dir.name} on their lattice's registration · {how}",
            },
        ),
        n_particles=len(rows),
        n_tomograms=rows[analytics.TOMO_COL].nunique(),
        per_tomo=per_tomo_counts(rows),
        funnel=funnel,
        details={**c, "checks": v.checks, "undecided_pieces": _seam_pieces(undecided)},
        notes=notes,
        module=__name__,
        tomograms_star=str(inputs["tomograms"]),
    )
    return publish_set(project_path, pset, lambda out: table.write(rows, out))


# ── Sub-particles ────────────────────────────────────────────────────────────────────────────────

# The jobs whose particles carry refined poses: what a sub-particle set is made of.
REFINED_JOB_TYPES = (JobType.REFINE3D, JobType.M_REFINE)


@dataclass
class RefinedParticles:
    """A job's refined particles as a set's parent, with what its site follows from: the symmetry it refined with,
    and whether its particles are segments of filaments (then the lattice it imposed, when it imposed one)."""

    parent: ParentRef
    job_type: JobType
    symmetry: str
    helix: bool
    helical: dict[str, float] | None
    species_id: str


def _registered_particles(state, project_path: Path, instance_id: str, set_id: str) -> RefinedParticles:
    """A registered set (a seam vote's) as a sub-particle set's parent: segments of filaments, each on the
    registration of its lattice the vote gave it, so a unit's site is the same in every one."""
    pset = get_set(project_path, set_id)
    if pset is None:
        raise ValueError(f"no particle set {set_id}")
    if pset.recipe.op != SetOp.SEAM:
        raise ValueError(
            f"{pset.name} is a {pset.recipe.op.value} set: sub-particles are made of a job's refined particles, or "
            "of a set a seam vote registered"
        )
    made_of = pset.recipe.parents[0].instance_id if pset.recipe.parents else ""
    if made_of != instance_id:
        raise ValueError(f"{pset.name} was made of {made_of}'s scores, not of {instance_id}'s")
    if not Path(pset.particles_star).exists():
        raise FileNotFoundError(f"{pset.name}'s particles star is missing")
    jm = (state.jobs or {}).get(instance_id)
    job_type = getattr(jm, "job_type", JobType.CLASS3D)
    return RefinedParticles(pset.as_parent(), job_type, "C1", True, None, pset.species_id)


def refined_particles(state, project_path: Path, instance_id: str, *, from_set: str = "") -> RefinedParticles:
    """The refined particles of a Refine 3D (its last data star) or an M refine; with `from_set`, the registered
    set a seam vote over `instance_id`'s scores made. Raises FileNotFoundError when the job has written none yet,
    ValueError for any other job."""
    from services.models_base import resolve_species
    from services.subtomo_merge import _parse_optimisation_set

    if from_set:
        return _registered_particles(state, project_path, instance_id, from_set)
    jm = (state.jobs or {}).get(instance_id)
    if jm is None:
        raise ValueError(f"no job {instance_id}")
    if jm.job_type not in REFINED_JOB_TYPES:
        raise ValueError(
            f"{instance_id} is a {jm.job_type.value} job: sub-particles are made of a Refine 3D or an M refine"
        )
    job_dir = job_dir_of(state, project_path, instance_id)
    if job_dir is None:
        raise FileNotFoundError(f"{instance_id} has not run yet")
    _species, species_id = resolve_species(state, jm, instance_id)
    helical = None
    if jm.job_type == JobType.REFINE3D:
        files = filament_polarity.refine3d_files(job_dir)
        if not files["optset"].exists():
            raise FileNotFoundError(
                f"{files['optset'].name} is missing; it names the tomograms these particles live in"
            )
        _particles, tomograms = _parse_optimisation_set(files["optset"])
        when = "converged" if files["iteration"] is None else f"iteration {files['iteration']}, not converged"
        data, label, output = files["data"], f"Refine 3D {job_dir.name} ({when})", "refine3d_data"
        helix = bool(getattr(jm, "helix", False))
        if helix and jm.imposes_helical_symmetry:
            helical = {"twist_deg": float(jm.helical_twist), "rise_ang": float(jm.helical_rise)}
    else:
        data, tomograms = job_dir / "particles.star", job_dir / "tomograms.star"
        for f in (data, tomograms):
            if not f.exists():
                raise FileNotFoundError(f"{f.name} is missing from {job_dir.name}")
        label, output, helix = f"M refine {job_dir.name}", "particles", False
    symmetry = getattr(jm.symmetry, "value", jm.symmetry)
    parent = ParentRef(
        kind="job",
        instance_id=instance_id,
        output=output,
        label=label,
        particles_star=str(data),
        tomograms_star=str(tomograms),
        level=SetLevel.EXTRACTED,
        species_id=species_id or "",
        mtime=file_mtime(data),
    )
    return RefinedParticles(parent, jm.job_type, sites.group_name(str(symmetry)), helix, helical, species_id or "")


def site_description(refined: RefinedParticles, values: dict[str, Any]) -> dict[str, Any]:
    """The site a job's particles take, from the values a person gives. Its kind follows from the job: segments of
    filaments take a helix, particles the point group they were refined with. Nothing is filled in: a value not
    given is refused where the site is built. A whole description (a recipe's, with its kind) is taken as it is
    when it still fits the job, so a set made again sits where it sat."""
    v = dict(values or {})
    kind = sites.HELIX if refined.helix else sites.POINT_GROUP
    if not refined.helix and refined.symmetry in ("", "C1"):
        raise ValueError(
            f"{refined.parent.instance_id} refined without symmetry (C1): its particles hold no copies of a unit that "
            "a point group could place"
        )
    if v.get("kind"):
        if v["kind"] != kind:
            raise ValueError(f"the site is a {v['kind']}, and {refined.parent.instance_id}'s particles take a {kind}")
        if kind == sites.POINT_GROUP and sites.group_name(v.get("group", "")) != refined.symmetry:
            raise ValueError(
                f"the site is of {v.get('group')}, and {refined.parent.instance_id} refined with {refined.symmetry}"
            )
        return v
    if refined.helix:
        return {
            "kind": sites.HELIX,
            "twist_deg": v.get("twist_deg"),
            "rise_ang": v.get("rise_ang"),
            "radius_ang": v.get("radius_ang"),
            "axial_repeat_ang": v.get("axial_repeat_ang"),
            "every": v.get("every") or 1,
            "azimuth_deg": v.get("azimuth_deg") or 0.0,
            "axial_ang": v.get("axial_ang") or 0.0,
        }
    axis = str(v.get("axis") or "").strip()
    named = axis not in ("", sites.DIRECTION)
    if not named and v.get("direction") is None:
        raise ValueError("where the unit sits is not given: an axis of the group, or a direction (three numbers)")
    return {
        "kind": sites.POINT_GROUP,
        "group": refined.symmetry,
        "axis": axis,
        "direction": None if named else v.get("direction"),
        "radius_ang": v.get("radius_ang"),
    }


def _sub_unit(state, species_id: str):
    species = state.get_species(species_id) if species_id else None
    if species is None:
        raise ValueError(
            f"no species {species_id!r}: a sub-unit is a species of its own (its symmetry and Ø are what its jobs "
            "start from). Make it on the Species page first"
        )
    return species


def _place_subparticles(
    state, project_path: Path, instance_id: str, species_id: str, values: dict[str, Any], from_set: str = ""
):
    refined = refined_particles(state, project_path, instance_id, from_set=from_set)
    species = _sub_unit(state, species_id)
    if species_id == refined.species_id:
        raise ValueError(f"{species.name} is the species these particles are of: its sub-unit is another species")
    description = site_description(refined, values)
    table = read_table(refined.parent.particles_star)
    margin = float(species.diameter_ang) / 2.0 if species.diameter_ang else 0.0
    placement = subparticles.place(
        table.df, description, extents=tomogram_extents(refined.parent.tomograms_star), margin_ang=margin
    )
    if not species.diameter_ang:
        placement.notes.append(
            f"{species.name} has no Ø: a copy is kept when its centre is inside its tomogram, however close to a face"
        )
    return refined, species, placement


def _subparticle_funnel(refined: RefinedParticles, placement) -> list[FunnelStep]:
    site = placement.site
    per = "segment" if refined.helix else "particle"
    steps = [
        FunnelStep(label=refined.parent.label, kept=placement.n_parents, removed=0),
        FunnelStep(label=f"{placement.per_parent} per {per}: {site.text()}", kept=placement.n_placed, removed=0),
    ]
    kept = placement.n_placed - placement.n_outside
    inside = (
        f"inside its tomogram by {placement.margin_ang:g} Å (Ø/2)" if placement.margin_ang else "inside its tomogram"
    )
    steps.append(FunnelStep(label=inside, kept=kept, removed=placement.n_outside))
    if placement.doubles_radius_ang is not None:
        steps.append(
            FunnelStep(
                label=f"one of each unit (the same from two segments, within {placement.doubles_radius_ang:.0f} Å)",
                kept=kept - placement.n_doubles,
                removed=placement.n_doubles,
            )
        )
    return steps


def subparticle_plan(
    project_path: Path, state, *, instance_id: str, species_id: str, values: dict[str, Any], from_set: str = ""
) -> dict[str, Any]:
    """What `create_subparticle_set` would make, without writing: the site as it was understood, the counts and
    the funnel."""
    refined, species, placement = _place_subparticles(state, project_path, instance_id, species_id, values, from_set)
    return {
        "site": placement.site.description,
        "site_text": placement.site.text(),
        "own_symmetry": placement.site.own_symmetry,
        "species_symmetry": species.symmetry,
        "per_parent": placement.per_parent,
        "parents": placement.n_parents,
        "picks": len(placement.rows),
        "spacing_ang": placement.site.spacing_ang,
        "funnel": [f.model_dump() for f in _subparticle_funnel(refined, placement)],
        "notes": list(placement.notes),
    }


def create_subparticle_set(
    project_path: Path,
    state,
    *,
    instance_id: str,
    species_id: str,
    name: str,
    values: dict[str, Any],
    measured_on: str = "",
    from_set: str = "",
) -> ParticleSet:
    """A new coordinates set: the copies of one unit in every refined particle of `instance_id` (a Refine 3D or an
    M refine), each with its pose composed from its parent's, its parent's uid and its assembly's half-set
    (`subparticles.place`). `species_id` is the sub-unit's own species, whose symmetry and Ø the jobs on the set
    start from; `values` are the site's (`site_description`), `measured_on` the job whose map they were read
    from. With `from_set` the parents are the segments of a registered set (a seam vote over `instance_id`'s
    scores): on one registration each, a site that holds a unit of the true repeat sits on the same unit in
    every one. The whole site is kept in the recipe, so the set is made again exactly."""
    refined, species, placement = _place_subparticles(state, project_path, instance_id, species_id, values, from_set)
    rows = placement.rows
    if rows.empty:
        raise ValueError("no copy of the unit is inside its tomogram; nothing to make a set of")
    site = placement.site
    parent_species = state.get_species(refined.species_id) if refined.species_id else None
    parent_name = parent_species.name if parent_species is not None else "parent"
    job_dir = Path(refined.parent.particles_star).parent.name
    per = f"{placement.per_parent} per {parent_name} {'segment' if refined.helix else 'particle'}"
    notes = list(placement.notes)
    own = site.own_symmetry
    if own and sites.group_name(species.symmetry) != own:
        notes.append(f"the unit's own symmetry where it sits is {own}; {species.name} has {species.symmetry}")
    pset = ParticleSet(
        id=allocate_id(project_path, name),
        name=name,
        species_id=species_id,
        level=SetLevel.COORDINATES,
        recipe=Recipe(
            op=SetOp.SUBPARTICLES,
            parents=[refined.parent],
            params={
                "site": site.description,
                "measured_on": measured_on,
                "parent_species": refined.species_id,
                "job": instance_id,
                "from_set": from_set,
                "description": f"{species.name} of {job_dir} · {per}",
            },
        ),
        n_particles=len(rows),
        n_tomograms=rows[analytics.TOMO_COL].nunique(),
        per_tomo=per_tomo_counts(rows),
        funnel=_subparticle_funnel(refined, placement),
        details={
            "site_text": site.text(),
            "own_symmetry": own,
            "per_parent": placement.per_parent,
            "n_parents": placement.n_parents,
            "n_outside": placement.n_outside,
            "n_doubles": placement.n_doubles,
            "n_close": placement.n_close,
            "spacing_ang": site.spacing_ang,
            "doubles_radius_ang": placement.doubles_radius_ang,
            "half_window_ang": placement.half_window_ang,
        },
        notes=notes,
        module=__name__,
        tomograms_star=refined.parent.tomograms_star,
    )
    return publish_set(project_path, pset, lambda out: starfile.write({"particles": rows}, out, overwrite=True))


def subparticle_readout(project_path: Path, state, instance_id: str) -> dict[str, Any]:
    """What a job's Sub-units block and `get_subparticles` show, without reading a star: whether the job's
    particles can have sub-units and of which kind, the axes or the lattice the site starts from, the species a
    sub-unit can be, the jobs a site's values can have been measured on, the sets already made of this job and,
    per sub-unit species, the values of the last set made from this parent species."""
    from services.models_base import resolve_species

    jm = (state.jobs or {}).get(instance_id)
    if jm is None:
        raise ValueError(f"no job {instance_id}")
    if jm.job_type not in REFINED_JOB_TYPES:
        raise ValueError(
            f"{instance_id} is a {jm.job_type.value} job: sub-particles are made of a Refine 3D or an M refine"
        )
    _species, parent_species = resolve_species(state, jm, instance_id)
    parent_species = parent_species or ""
    helix = jm.job_type == JobType.REFINE3D and bool(getattr(jm, "helix", False))
    symmetry = sites.group_name(str(getattr(jm.symmetry, "value", jm.symmetry)))
    out: dict[str, Any] = {
        "job": instance_id,
        "parent_species": parent_species,
        "kind": None,
        "reason": "",
        "group": symmetry,
        "axes": [],
        "helical": None,
    }
    if helix:
        out["kind"] = sites.HELIX
        if jm.imposes_helical_symmetry:
            out["helical"] = {"twist_deg": float(jm.helical_twist), "rise_ang": float(jm.helical_rise)}
    elif symmetry in ("", "C1"):
        out["reason"] = "refined without symmetry (C1): no point group places a unit in its particles"
    elif not sites.known_group(symmetry):
        out["reason"] = f"the operators of {symmetry} have not been read from RELION ({sites.READ_SCRIPT})"
    else:
        out["kind"] = sites.POINT_GROUP
        out["axes"] = [
            {"axis": c.label, "per_particle": c.copies, "own_symmetry": f"C{c.order}", "direction": list(c.direction)}
            for c in sites.axis_classes(symmetry)
        ]
    out["species"] = [
        {"id": sp.id, "name": sp.name, "symmetry": sp.symmetry, "diameter_ang": sp.diameter_ang}
        for sp in state.species_registry or []
        if sp.id != parent_species
    ]
    out["measured_on"] = [
        {"job": iid, "label": f"{'Refine 3D' if m.job_type == JobType.REFINE3D else 'M refine'} ({d.name})"}
        for iid, m in jobs_for_species(state, parent_species)
        if m.job_type in REFINED_JOB_TYPES and (d := job_dir_of(state, project_path, iid)) is not None
    ]
    made = [ps for ps in list_sets(project_path) if ps.recipe.op == SetOp.SUBPARTICLES]
    out["sets"] = [
        {
            "set": ps.id,
            "name": ps.name,
            "species": ps.species_id,
            "picks": ps.n_particles,
            "site": ps.details.get("site_text", ""),
        }
        for ps in made
        if ps.recipe.parents and ps.recipe.parents[0].instance_id == instance_id
    ]
    last: dict[str, Any] = {}
    for ps in made:
        params = ps.recipe.params
        if params.get("parent_species") == parent_species and params.get("site", {}).get("kind") == out["kind"]:
            last[ps.species_id] = {
                "set": ps.id,
                "name": ps.name,
                "site": ps.recipe.params["site"],
                "measured_on": ps.recipe.params.get("measured_on", ""),
            }
    out["last"] = last
    out["expansion"] = _expansion_readout(project_path, state, instance_id, out)
    return out


# ── Expansions: every pose a particle's symmetry or lattice makes equal, the same images ─────────


def _expansion_readout(project_path: Path, state, instance_id: str, sub: dict[str, Any]) -> dict[str, Any]:
    """The Sub-units block's expansion part: which kind the job's particles take, the sets already made, and the
    lattice values a site of this species was last given (the helix the expansion starts from)."""
    made = [
        ps
        for ps in list_sets(project_path)
        if ps.recipe.op == SetOp.EXPANSION and ps.recipe.parents and ps.recipe.parents[0].instance_id == instance_id
    ]
    out: dict[str, Any] = {
        "kind": None,
        "sets": [
            {"set": ps.id, "name": ps.name, "picks": ps.n_particles, "expansion": ps.details.get("text", "")}
            for ps in made
        ],
        "last": None,
    }
    if sub["kind"] == sites.HELIX:
        out["kind"] = sites.LATTICE
        for ps in reversed(made):
            out["last"] = {"set": ps.id, "name": ps.name, "values": ps.recipe.params.get("expansion")}
            break
        else:
            helices = [v["site"] for v in sub["last"].values() if v["site"].get("axial_repeat_ang")]
            if helices:
                h = helices[0]
                out["last"] = {
                    "set": "",
                    "name": "",
                    "values": {
                        "kind": sites.LATTICE,
                        "twist_deg": h["twist_deg"],
                        "rise_ang": h["rise_ang"],
                        "axial_repeat_ang": h["axial_repeat_ang"],
                        "units_per_repeat": None,
                    },
                }
    elif sub["kind"] == sites.POINT_GROUP:
        out["kind"] = sites.POINT_GROUP
        out["per_particle"] = len(sites.operators(sub["group"]))
    return out


def expansion_description(refined: RefinedParticles, values: dict[str, Any]) -> dict[str, Any]:
    """The expansion a job's particles take, from the values a person gives. Segments of filaments take a lattice's
    registrations (its helix, the unit's repeat along the axis, how many units make the true repeat: all typed,
    none filled in); particles the operators of the point group they were refined with."""
    v = dict(values or {})
    if refined.helix:
        if v.get("kind") not in (None, sites.LATTICE):
            raise ValueError(
                f"{refined.parent.instance_id}'s particles are segments: they take a lattice's registrations"
            )
        return {
            "kind": sites.LATTICE,
            "twist_deg": v.get("twist_deg"),
            "rise_ang": v.get("rise_ang"),
            "axial_repeat_ang": v.get("axial_repeat_ang"),
            "units_per_repeat": v.get("units_per_repeat"),
        }
    if refined.symmetry in ("", "C1"):
        raise ValueError(f"{refined.parent.instance_id} refined without symmetry (C1): nothing to expand by")
    if v.get("kind") not in (None, sites.POINT_GROUP) or v.get("group") not in (None, refined.symmetry):
        raise ValueError(
            f"{refined.parent.instance_id} refined with {refined.symmetry}: its operators are the expansion"
        )
    return {"kind": sites.POINT_GROUP, "group": refined.symmetry}


def _expand(state, project_path: Path, instance_id: str, values: dict[str, Any]):
    refined = refined_particles(state, project_path, instance_id)
    description = expansion_description(refined, values)
    table = read_table(refined.parent.particles_star)
    return refined, table, subparticles.expand(table.df, description)


def _expansion_funnel(refined: RefinedParticles, expanded) -> list[FunnelStep]:
    per = "segment" if refined.helix else "particle"
    e = expanded.expansion
    return [
        FunnelStep(label=refined.parent.label, kept=expanded.n_parents, removed=0),
        FunnelStep(
            label=f"{e.n} per {per}: {e.text()}",
            kept=len(expanded.rows),
            removed=0,
            note=f"the same images; each copy moved at most {e.max_shift_ang:.1f} Å along its particle's axis",
        ),
    ]


def expansion_plan(project_path: Path, state, *, instance_id: str, values: dict[str, Any]) -> dict[str, Any]:
    """What `create_expansion_set` would make, without writing: the expansion as understood, its copies (turn and
    move each), the counts and the funnel."""
    refined, _table, expanded = _expand(state, project_path, instance_id, values)
    e = expanded.expansion
    return {
        "expansion": e.description,
        "text": e.text(),
        "per_parent": e.n,
        "parents": expanded.n_parents,
        "picks": len(expanded.rows),
        "max_shift_ang": round(e.max_shift_ang, 2),
        "labels_count_steps": e.by_steps,
        "copies": e.table(),
        "funnel": [f.model_dump() for f in _expansion_funnel(refined, expanded)],
        "notes": list(expanded.notes),
    }


def create_expansion_set(
    project_path: Path, state, *, instance_id: str, name: str, values: dict[str, Any]
) -> ParticleSet:
    """A new extracted set: every refined particle of `instance_id` (a Refine 3D or an M refine) in each pose of
    its expansion (`subparticles.expand`): the same images, the pose and the origin of each copy changed, the
    copy's label in cbSubIndex and its particle's uid in cbParentUid. A job that classifies the set without
    aligning (Class 3D, Skip alignment) scores every copy against one reference: which registration of a lattice,
    which copy under a pseudo-symmetry, each particle fits best. The expansion is kept in the recipe."""
    refined, table, expanded = _expand(state, project_path, instance_id, values)
    e = expanded.expansion
    rows = expanded.rows
    job_dir = Path(refined.parent.particles_star).parent.name
    per = f"{e.n} per {'segment' if refined.helix else 'particle'}"
    pset = ParticleSet(
        id=allocate_id(project_path, name),
        name=name,
        species_id=refined.species_id,
        level=SetLevel.EXTRACTED,
        recipe=Recipe(
            op=SetOp.EXPANSION,
            parents=[refined.parent],
            params={"expansion": e.description, "description": f"{job_dir} expanded · {per}: {e.text()}"},
        ),
        n_particles=len(rows),
        n_tomograms=rows[analytics.TOMO_COL].nunique(),
        per_tomo=per_tomo_counts(rows),
        funnel=_expansion_funnel(refined, expanded),
        details={
            "text": e.text(),
            "per_parent": e.n,
            "n_parents": expanded.n_parents,
            "max_shift_ang": round(e.max_shift_ang, 2),
            "labels_count_steps": e.by_steps,
            "copies": e.table(),
        },
        notes=list(expanded.notes),
        module=__name__,
        tomograms_star=refined.parent.tomograms_star,
    )
    return publish_set(project_path, pset, lambda out: table.write(rows, out))


# ── Random-position decoys ───────────────────────────────────────────────────────────────────────

DECOYS_FILE = "decoys.star"
# Draws per wanted decoy before a tomogram gives up (crowded slabs reject most draws).
DECOY_ATTEMPTS = 200
# Columns a coordinates set drops from extracted parents: they name images of a previous extraction.
_EXTRACTION_COLUMNS = ("rlnImageName", "rlnTomoParticleName", "rlnTomoVisibleFrames", "rlnOpticsGroup")


def _random_angles(rng: np.random.Generator, n: int) -> np.ndarray:
    """Orientations uniform over SO(3), as RELION ZYZ (rot, tilt, psi) in degrees."""
    rot = rng.uniform(-180.0, 180.0, n)
    tilt = np.degrees(np.arccos(rng.uniform(-1.0, 1.0, n)))
    psi = rng.uniform(-180.0, 180.0, n)
    return np.column_stack([rot, tilt, psi])


def create_decoy_position_set(
    project_path: Path,
    *,
    parent: ParentRef,
    fraction: float,
    min_distance_ang: float,
    seed: int,
    name: str,
    species_id: str = "",
) -> ParticleSet:
    """A new coordinates set: `parent`'s picks plus, per tomogram, `fraction` × its picks random
    positions inside its pick slab (between the 5th and 95th percentile of the picks' depth around
    the pick plane, within the volume), each at least `min_distance_ang` from every pick and every
    other decoy, with random orientations and a score of 0σ. The decoys are also listed in the set's
    decoys.star, which is how a classification of the set later finds them. Extracted parents give
    coordinates (their image columns are dropped)."""
    table = read_table(parent.particles_star)
    df = table.df.drop(columns=[c for c in _EXTRACTION_COLUMNS if c in table.df.columns]).reset_index(drop=True)
    extents = tomogram_extents(parent.tomograms_star)
    rng = np.random.default_rng(seed)
    tomo = df[analytics.TOMO_COL].astype(str).to_numpy()
    xyz = df[CENTERED_COLS].to_numpy(dtype=float)
    decoy_rows = []
    skipped: list[str] = []
    for t in np.unique(tomo):
        rows = np.flatnonzero(tomo == t)
        coef = analytics.fit_pick_plane(xyz[rows])
        ext = extents.get(t)
        if coef is None or ext is None:
            skipped.append(t)
            continue
        depth = analytics.plane_depth(xyz[rows], coef)
        d_lo, d_hi = (float(v) for v in np.quantile(depth, [0.05, 0.95]))
        half = ext / 2.0 - min_distance_ang / 2.0
        want = round(fraction * len(rows))
        placed: list[np.ndarray] = []
        occupied = xyz[rows]
        for _ in range(want * DECOY_ATTEMPTS):
            if len(placed) >= want:
                break
            x, y = rng.uniform(-half[0], half[0]), rng.uniform(-half[1], half[1])
            z = coef[0] * x + coef[1] * y + coef[2] + rng.uniform(d_lo, d_hi)
            p = np.array([x, y, z])
            if abs(z) > half[2]:
                continue
            others = np.vstack([occupied, *placed]) if placed else occupied
            if analytics.min_distances(p[None, :], others)[0] < min_distance_ang:
                continue
            placed.append(p)
        if len(placed) < want:
            skipped.append(f"{t} ({len(placed)} of {want})")
        if not placed:
            continue
        template = df.iloc[rows[0]]
        block = pd.DataFrame([template] * len(placed)).reset_index(drop=True)
        block[CENTERED_COLS] = np.array(placed)
        angles = _random_angles(rng, len(placed))
        for i, col in enumerate(("rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi")):
            if col in block.columns:
                block[col] = angles[:, i]
        if analytics.SCORE_COL in block.columns:
            block[analytics.SCORE_COL] = 0.0
        for col in ("rlnOriginXAngst", "rlnOriginYAngst", "rlnOriginZAngst"):
            if col in block.columns:
                block[col] = 0.0
        decoy_rows.append(block)
    if not decoy_rows:
        raise ValueError("no tomogram had a pick plane and room for a decoy; nothing to make")
    decoys = pd.concat(decoy_rows, ignore_index=True)
    union = pd.concat([df, decoys], ignore_index=True)
    parent = parent.model_copy(update={"mtime": file_mtime(parent.particles_star)})
    pset = ParticleSet(
        id=allocate_id(project_path, name),
        name=name,
        species_id=species_id or parent.species_id,
        level=SetLevel.COORDINATES,
        recipe=Recipe(
            op=SetOp.DECOY_POSITIONS,
            parents=[parent],
            params={
                "fraction": fraction,
                "min_distance_ang": min_distance_ang,
                "seed": seed,
                "description": (
                    f"+{len(decoys):,} random in-slab decoys ({fraction:g} per pick, ≥ {min_distance_ang:g} Å apart)"
                ),
            },
        ),
        n_particles=len(union),
        n_tomograms=union[analytics.TOMO_COL].nunique(),
        per_tomo=per_tomo_counts(union),
        details={"n_decoys": len(decoys), "decoys_star": DECOYS_FILE, "n_real": len(df)},
        notes=[f"{len(skipped)} tomogram(s) got fewer decoys or none: {', '.join(skipped)}"] if skipped else [],
        module=__name__,
        tomograms_star=parent.tomograms_star,
    )

    def write(out: Path) -> None:
        starfile.write({"particles": union}, out, overwrite=True)
        starfile.write({"particles": decoys}, out.with_name(DECOYS_FILE), overwrite=True)

    return publish_set(project_path, pset, write)


def decoy_keys(project_path: Path) -> set[tuple[str, tuple[int, int, int]]]:
    """(tomogram, coordinate key) of every random-position decoy of the project's sets."""
    from services.particles.subtomo_link import _coord_key

    keys: set[tuple[str, tuple[int, int, int]]] = set()
    for ps in list_sets(project_path):
        if ps.recipe.op != SetOp.DECOY_POSITIONS:
            continue
        star = Path(ps.particles_star).with_name(ps.details.get("decoys_star") or DECOYS_FILE)
        if not star.exists():
            continue
        df = read_table(star).df
        for t, x, y, z in zip(
            df[analytics.TOMO_COL].astype(str), *(df[c].astype(float) for c in CENTERED_COLS), strict=True
        ):
            keys.add((t, _coord_key(x, y, z)))
    return keys


def decoy_class_counts(data_star: Path, keys: set) -> dict[int, int]:
    """{class: decoys in it} of a Class 3D data star: the classifier's false acceptances."""
    from services.particles.subtomo_link import _coord_key

    if not keys:
        return {}
    df = read_table(data_star).df
    if "rlnClassNumber" not in df.columns:
        return {}
    out: dict[int, int] = {}
    for t, x, y, z, k in zip(
        df[analytics.TOMO_COL].astype(str),
        *(df[c].astype(float) for c in CENTERED_COLS),
        df["rlnClassNumber"].astype(int),
        strict=True,
    ):
        if (t, _coord_key(x, y, z)) in keys:
            out[int(k)] = out.get(int(k), 0) + 1
    return out


# ── Recall against a reference list ──────────────────────────────────────────────────────────────


def reference_table(state, project_path: Path, species_id: str, key: str) -> pd.DataFrame:
    """The reference picks: a parent table (`key` of `parent_options`), or "picklists" — the union of
    the species' hand-placed and imported pick lists."""
    if key != "picklists":
        ref = next((p for p in parent_options(state, project_path, species_id) if p.key == key), None)
        if ref is None:
            raise ValueError(f"no reference {key}")
        return read_table(ref.particles_star).df
    from services.models_base import PickListType
    from services.particles.pick_merge import read_centered_coords

    parts = []
    for pl in state.pick_lists or []:
        if pl.species_id != species_id or pl.list_type not in (PickListType.MANUAL, PickListType.IMPORTED):
            continue
        if not pl.path or not Path(pl.path).exists():
            continue
        c = read_centered_coords(Path(pl.path), pl.tomo_name)
        if len(c):
            block = pd.DataFrame(c, columns=CENTERED_COLS)
            block.insert(0, analytics.TOMO_COL, pl.tomo_name)
            parts.append(block)
    if not parts:
        raise ValueError("this species has no hand-placed or imported pick lists")
    return pd.concat(parts, ignore_index=True)


def recall_curve(candidates: pd.DataFrame, reference: pd.DataFrame, radius: float, *, step: float = 0.25) -> dict:
    """Against a reference list, over the tomograms the reference covers: the share of reference
    picks with a candidate at or above z within `radius` (recall), and the share of candidates at or
    above z within `radius` of a reference pick (precision), per z."""
    sig = analytics.particle_sigma_scores(candidates)
    if sig is None:
        raise ValueError("the candidates carry no σ")
    ct = candidates[analytics.TOMO_COL].astype(str).to_numpy()
    cx = candidates[CENTERED_COLS].to_numpy(dtype=float)
    rt = reference[analytics.TOMO_COL].astype(str).to_numpy()
    rx = reference[CENTERED_COLS].to_numpy(dtype=float)
    covered = np.isin(ct, np.unique(rt))
    best_for_ref = np.full(len(rx), -np.inf)
    cand_matched = np.zeros(len(cx), dtype=bool)
    for t in np.unique(rt):
        ri, ci = np.flatnonzero(rt == t), np.flatnonzero(ct == t)
        if len(ci) == 0:
            continue
        d = np.sqrt(((rx[ri][:, None, :] - cx[ci][None, :, :]) ** 2).sum(axis=2))
        near = d <= radius
        scores = np.where(near, np.nan_to_num(sig[ci], nan=-np.inf)[None, :], -np.inf)
        best_for_ref[ri] = scores.max(axis=1)
        cand_matched[ci] = near.any(axis=0)
    s_cov = np.nan_to_num(sig[covered], nan=-np.inf)
    if s_cov.size == 0:
        return {"z": [], "recall": [], "precision": [], "n_reference": len(rx), "n_candidates": 0}
    lo = math.floor(float(s_cov[np.isfinite(s_cov)].min()) / step) * step
    hi = math.ceil(float(s_cov.max()) / step) * step
    z = [round(lo + i * step, 3) for i in range(round((hi - lo) / step) + 1)]
    matched = cand_matched[covered]
    recall, precision = [], []
    for zz in z:
        above = s_cov >= zz
        recall.append(round(float(np.mean(best_for_ref >= zz)), 4))
        precision.append(round(float(matched[above].mean()), 4) if above.any() else None)
    return {
        "z": z,
        "recall": recall,
        "precision": precision,
        "n_reference": len(rx),
        "n_candidates": int(covered.sum()),
        "n_tomograms": len(np.unique(rt)),
    }


# ── Half-set independence ────────────────────────────────────────────────────────────────────────


def half_set_check(table: pd.DataFrame, diameter_ang: float) -> dict[str, Any]:
    """Pairs of particles closer than a diameter that sit in opposite halves (rlnRandomSubset 1 and 2):
    the same particle, or overlapping density, in both halves inflates the FSC."""
    if "rlnRandomSubset" not in table.columns:
        return {"has_halves": False}
    t = table[analytics.TOMO_COL].astype(str).to_numpy()
    x = table[CENTERED_COLS].to_numpy(dtype=float)
    h = table["rlnRandomSubset"].astype(int).to_numpy()
    pairs, involved = 0, set()
    for tomo in np.unique(t):
        a, b = np.flatnonzero((t == tomo) & (h == 1)), np.flatnonzero((t == tomo) & (h == 2))
        if not len(a) or not len(b):
            continue
        d = np.sqrt(((x[a][:, None, :] - x[b][None, :, :]) ** 2).sum(axis=2))
        ia, ib = np.nonzero(d < diameter_ang)
        pairs += len(ia)
        involved.update(a[ia].tolist())
        involved.update(b[ib].tolist())
    return {"has_halves": True, "n": len(table), "cross_pairs": pairs, "n_involved": len(involved)}


# ── Class membership of two classifications ──────────────────────────────────────────────────────


def class_agreement(data_star_a: Path, data_star_b: Path) -> dict[str, Any]:
    """How two classifications of the same particles agree: for each class of A, the class of B most
    of its particles went to and that share; overall, the share of particles in their class's
    best-matching class. Particles are matched by rlnTomoParticleName, else by coordinates."""
    from services.particles.subtomo_link import _coord_key

    def keyed(path: Path) -> dict[Any, int]:
        df = read_table(path).df
        if "rlnClassNumber" not in df.columns:
            raise ValueError(f"{path} has no rlnClassNumber")
        cls = df["rlnClassNumber"].astype(int).to_numpy()
        if "rlnTomoParticleName" in df.columns:
            keys = df["rlnTomoParticleName"].astype(str).tolist()
        else:
            keys = [
                (t, _coord_key(x, y, z))
                for t, x, y, z in zip(
                    df[analytics.TOMO_COL].astype(str), *(df[c].astype(float) for c in CENTERED_COLS), strict=True
                )
            ]
        return dict(zip(keys, cls.tolist(), strict=True))

    a, b = keyed(data_star_a), keyed(data_star_b)
    common = [k for k in a if k in b]
    if not common:
        raise ValueError("the two classifications share no particle")
    pairs: dict[tuple[int, int], int] = {}
    for k in common:
        pairs[(a[k], b[k])] = pairs.get((a[k], b[k]), 0) + 1
    per_class = []
    for ka in sorted({p[0] for p in pairs}):
        row = {kb: n for (x, kb), n in pairs.items() if x == ka}
        kb, n = max(row.items(), key=lambda kv: kv[1])
        per_class.append({"class": ka, "n": sum(row.values()), "best": kb, "share": round(n / sum(row.values()), 3)})
    agree = sum(max(n for (x, _), n in pairs.items() if x == ka) for ka in {p[0] for p in pairs})
    return {"n_common": len(common), "agreement": round(agree / len(common), 3), "classes": per_class}


# ── Splits ───────────────────────────────────────────────────────────────────────────────────────

SPLIT_MODES: dict[str, str] = {
    "halves": "random halves of the particles",
    "tomogram_halves": "halves by tomogram (every particle of a tomogram in the same half)",
    "projects": "one set per project (a cross-project merge)",
    "sigma_bins": "one set per σ bin",
    "classes": "one set per class (a classification's particles)",
}


def _split_parts(df: pd.DataFrame, mode: str, *, seed: int, edges: list[float], projects: dict[str, str]):
    """[(part label, boolean mask)] of one split."""
    n = len(df)
    tomo = df[analytics.TOMO_COL].astype(str).to_numpy()
    rng = np.random.default_rng(seed)
    if mode == "halves":
        half = np.zeros(n, dtype=bool)
        half[rng.permutation(n)[: n // 2]] = True
        return [("half 1", half), ("half 2", ~half)]
    if mode == "tomogram_halves":
        names = np.unique(tomo)
        first = set(rng.permutation(names)[: len(names) // 2].tolist())
        mask = np.isin(tomo, list(first))
        return [("tomogram half 1", mask), ("tomogram half 2", ~mask)]
    if mode == "projects":
        if not projects:
            raise ValueError("these particles carry no project of origin (not a cross-project merge)")
        proj = np.array([projects.get(t, "(unknown project)") for t in tomo])
        return [(p, proj == p) for p in sorted(set(proj.tolist()))]
    if mode == "sigma_bins":
        sig = analytics.particle_sigma_scores(df)
        if sig is None:
            raise ValueError("these particles carry no σ")
        bounds = sorted(float(e) for e in edges)
        if not bounds:
            raise ValueError("give at least one σ edge")
        sig = np.nan_to_num(sig, nan=-np.inf)
        cuts = [-np.inf, *bounds, np.inf]
        parts = []
        for lo, hi in pairwise(cuts):
            label = f"σ < {hi:g}" if lo == -np.inf else (f"σ ≥ {lo:g}" if hi == np.inf else f"{lo:g} ≤ σ < {hi:g}")
            parts.append((label, (sig >= lo) & (sig < hi)))
        return parts
    if mode == "classes":
        if "rlnClassNumber" not in df.columns:
            raise ValueError("these particles carry no class (rlnClassNumber)")
        cls = df["rlnClassNumber"].astype(int).to_numpy()
        return [(f"class {k}", cls == k) for k in sorted(set(cls.tolist()))]
    raise ValueError(f"unknown split {mode!r}")


def create_split_sets(
    project_path: Path,
    *,
    parent: ParentRef,
    mode: str,
    name: str,
    seed: int = 1,
    edges: list[float] | None = None,
    species_id: str = "",
) -> list[ParticleSet]:
    """Sibling sets that partition `parent`: one per part of the split, sharing a sibling group.
    Empty parts are not written (and named in the first set's notes)."""
    import uuid

    table = read_table(parent.particles_star)
    df = table.df
    projects = {}
    summary = Path(parent.particles_star).parent / "merge_summary.json"
    if summary.exists():
        from services.particles.analytics_store import _merge_projects

        projects = _merge_projects(summary)
    parts = _split_parts(df, mode, seed=seed, edges=list(edges or []), projects=projects)
    parent = parent.model_copy(update={"mtime": file_mtime(parent.particles_star)})
    group = uuid.uuid4().hex[:8]
    empty = [label for label, mask in parts if not mask.any()]
    out: list[ParticleSet] = []
    for label, mask in parts:
        if not mask.any():
            continue
        subset = df[mask]
        set_name = f"{name} · {label}"
        pset = ParticleSet(
            id=allocate_id(project_path, set_name),
            name=set_name,
            species_id=species_id or parent.species_id,
            level=parent.level,
            recipe=Recipe(
                op=SetOp.SPLIT,
                parents=[parent],
                params={
                    "mode": mode,
                    "part": label,
                    "seed": seed,
                    "edges": list(edges or []),
                    "description": f"{label} of a split into {SPLIT_MODES.get(mode, mode)}",
                },
            ),
            n_particles=len(subset),
            n_tomograms=subset[analytics.TOMO_COL].nunique(),
            per_tomo=per_tomo_counts(subset),
            funnel=[
                FunnelStep(label=parent.label, kept=len(df), removed=0),
                FunnelStep(label=label, kept=len(subset), removed=len(df) - len(subset)),
            ],
            notes=[f"empty parts not written: {', '.join(empty)}"] if empty and not out else [],
            sibling_group=group,
            module=__name__,
            tomograms_star=parent.tomograms_star,
        )
        out.append(publish_set(project_path, pset, lambda o, s=subset: table.write(s, o)))
    if not out:
        raise ValueError("every part of the split is empty")
    return out


# ── Set algebra ──────────────────────────────────────────────────────────────────────────────────


def create_proximity_set(
    project_path: Path, *, a: ParentRef, b: ParentRef, op: SetOp, radius_ang: float, name: str, species_id: str = ""
) -> ParticleSet:
    """A − B (`op` DIFFERENCE: A's rows with no B row within `radius_ang` in the same tomogram) or
    A ∩ B (INTERSECTION: A's rows with one), as a new set of A's rows."""
    if op not in (SetOp.DIFFERENCE, SetOp.INTERSECTION):
        raise ValueError(f"not a proximity operation: {op}")
    ta, tb = read_table(a.particles_star), read_table(b.particles_star).df
    da = ta.df
    at, bt = da[analytics.TOMO_COL].astype(str).to_numpy(), tb[analytics.TOMO_COL].astype(str).to_numpy()
    ax, bx = da[CENTERED_COLS].to_numpy(dtype=float), tb[CENTERED_COLS].to_numpy(dtype=float)
    near = np.zeros(len(da), dtype=bool)
    for t in np.unique(at):
        rows = np.flatnonzero(at == t)
        near[rows] = analytics.min_distances(ax[rows], bx[bt == t]) <= radius_ang
    keep = near if op == SetOp.INTERSECTION else ~near
    subset = da[keep]
    if subset.empty:
        raise ValueError("the operation keeps no particle")
    a = a.model_copy(update={"mtime": file_mtime(a.particles_star)})
    b = b.model_copy(update={"mtime": file_mtime(b.particles_star)})
    sign = "∩" if op == SetOp.INTERSECTION else "−"
    pset = ParticleSet(
        id=allocate_id(project_path, name),
        name=name,
        species_id=species_id or a.species_id,
        level=a.level,
        recipe=Recipe(
            op=op,
            parents=[a, b],
            params={"radius_ang": radius_ang, "description": f"{a.label} {sign} {b.label} within {radius_ang:g} Å"},
        ),
        n_particles=len(subset),
        n_tomograms=subset[analytics.TOMO_COL].nunique(),
        per_tomo=per_tomo_counts(subset),
        funnel=[
            FunnelStep(label=a.label, kept=len(da), removed=0),
            FunnelStep(
                label=f"{sign} {b.label} within {radius_ang:g} Å", kept=len(subset), removed=len(da) - len(subset)
            ),
        ],
        module=__name__,
        tomograms_star=a.tomograms_star,
    )
    return publish_set(project_path, pset, lambda out: ta.write(subset, out))


# ── Replay ───────────────────────────────────────────────────────────────────────────────────────


def replay_set(project_path: Path, state, pset: ParticleSet) -> list[ParticleSet]:
    """Make `pset` again from its recipe, against its parents as they are now (a parent job that ran
    again, a curated subset that changed). The new set is named after the old one."""
    r = pset.recipe
    name = f"{pset.name} (replayed)"
    if r.op == SetOp.RULE_FILTER:
        return [
            create_rule_filter_set(
                project_path, parent=r.parents[0], rules=r.rules, name=name, species_id=pset.species_id
            )
        ]
    if r.op == SetOp.CLASS_SUBSET:
        return [
            create_class_subset_set(
                project_path,
                state,
                class3d_instance=r.parents[0].instance_id,
                classes=list(r.params.get("classes") or []),
                name=name,
                species_id=pset.species_id,
            )
        ]
    if r.op == SetOp.POLARITY:
        return [
            create_polarity_set(
                project_path,
                state,
                refine3d_instance=r.parents[0].instance_id,
                name=name,
                species_id=pset.species_id,
                min_majority=float(r.params.get("min_majority", filament_polarity.MIN_MAJORITY)),
                keep_undecided=bool(r.params.get("keep_undecided", False)),
            )
        ]
    if r.op == SetOp.SUBPARTICLES:
        site = dict(r.params.get("site") or {})
        return [
            create_subparticle_set(
                project_path,
                state,
                instance_id=r.parents[0].instance_id or str(r.params.get("job") or ""),
                species_id=pset.species_id,
                name=name,
                values=site,
                measured_on=str(r.params.get("measured_on") or ""),
                from_set=str(r.params.get("from_set") or ""),
            )
        ]
    if r.op == SetOp.SEAM:
        keys = ("radius_ang", "min_segments", "block_segments")
        return [
            create_seam_set(
                project_path,
                state,
                class3d_instance=r.parents[0].instance_id,
                name=name,
                values={k: r.params[k] for k in keys if r.params.get(k) is not None},
                held_out=bool(r.params.get("held_out", True)),
            )
        ]
    if r.op == SetOp.EXPANSION:
        return [
            create_expansion_set(
                project_path,
                state,
                instance_id=r.parents[0].instance_id,
                name=name,
                values=dict(r.params.get("expansion") or {}),
            )
        ]
    if r.op in (SetOp.DIFFERENCE, SetOp.INTERSECTION):
        return [
            create_proximity_set(
                project_path,
                a=r.parents[0],
                b=r.parents[1],
                op=r.op,
                radius_ang=float(r.params["radius_ang"]),
                name=name,
                species_id=pset.species_id,
            )
        ]
    if r.op == SetOp.DECOY_POSITIONS:
        return [
            create_decoy_position_set(
                project_path,
                parent=r.parents[0],
                fraction=float(r.params["fraction"]),
                min_distance_ang=float(r.params["min_distance_ang"]),
                seed=int(r.params["seed"]),
                name=name,
                species_id=pset.species_id,
            )
        ]
    if r.op == SetOp.SPLIT:
        base = pset.name.split(" · ", 1)[0]
        return create_split_sets(
            project_path,
            parent=r.parents[0],
            mode=str(r.params["mode"]),
            name=f"{base} (replayed)",
            seed=int(r.params.get("seed", 1)),
            edges=list(r.params.get("edges") or []),
            species_id=pset.species_id,
        )
    raise ValueError(f"a {r.op.value} set is made again with its own action, not replayed")

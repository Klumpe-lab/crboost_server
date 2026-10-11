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
from services.particles import analytics
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
    list_sets,
    publish_set,
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


def greedy_keep(xyz: np.ndarray, order: np.ndarray, radius: float) -> np.ndarray:
    """Boolean mask over rows: walking `order` (best first), a row is kept unless a kept row lies
    within `radius` Å."""
    keep = np.zeros(len(xyz), dtype=bool)
    if len(order) == 0:
        return keep
    dropped = _greedy_drop_mask(xyz[order], radius)
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

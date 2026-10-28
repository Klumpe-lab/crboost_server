"""Make set: the kept rows of a pool become an ordinary particle set of the home project.

`plan(lp, name, level)` (server, in a thread) reads the selection and states before anything is
written: the blockers (an acquisition in frames that cannot be pooled and no frame chosen; extracted
members whose optics disagree), the unverified lines (handedness nobody states), the level, the clashes
deduplicated (picks of one pooled tomogram within Ø/2, a hand pick winning, then the higher σ), and the
counts. `write_set(plan)` (a worker) writes it: one member is a row subset of its own star; several
are merged, coordinates through `merge_coordinate_sources`, extracted particles through
`merge_optimisation_sets_into_jobdir`. The set records its members as parents, the recipe, the verdict
counts and the frozen uid list (``kitchen_uids.txt``), so it can be compared and replayed exactly.
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any

import numpy as np

from services.kitchen.models import SOURCE_PRIORITY, MemberKind, PickSource
from services.kitchen.pool import UNKNOWN_ACQ, PoolTable, Selection, _poolable
from services.particles.set_ops import greedy_keep

logger = logging.getLogger(__name__)

UIDS_FILE = "kitchen_uids.txt"
COORDINATES, EXTRACTED, EXTRACTED_ONLY = "coordinates", "extracted", "extracted_only"
# The optics columns extracted particles of one set must agree on.
OPTICS_COLS = ("rlnImagePixelSize", "rlnImageSize", "rlnTomoSubtomogramBinning")


def pooled_groups(table: PoolTable) -> np.ndarray:
    """Per frame, the frame its picks are pooled under: itself, or the first frame of its acquisition
    it can be pooled with (`_poolable`)."""
    out = np.arange(len(table.frames))
    by_acq: dict[str, list[int]] = {}
    for i, fr in enumerate(table.frames):
        if fr.get("acq") and fr["acq"] != UNKNOWN_ACQ:
            by_acq.setdefault(fr["acq"], []).append(i)
    for idx in by_acq.values():
        heads: list[int] = []
        for i in idx:
            head = next((h for h in heads if _poolable(table.frames[h], table.frames[i])), None)
            if head is None:
                heads.append(i)
            else:
                out[i] = head
    return out


def dedup(table: PoolTable, rows: np.ndarray, radius: float | None) -> np.ndarray:
    """The rows kept after removing clashes within `radius` Å in each pooled tomogram: a hand pick
    first, then imported, merged, template match; within a source the higher σ."""
    if not radius or not len(rows):
        return rows
    group = pooled_groups(table)[np.maximum(table.cat["frame"][rows], 0)]
    group = np.where(table.cat["frame"][rows] >= 0, group, -1 - rows)
    sources = table.categories["source"]
    prio = np.array([SOURCE_PRIORITY.get(PickSource(s), 9) for s in sources])[table.cat["source"][rows]]
    sig = table.num["sigma"][rows]
    order = np.lexsort((np.arange(len(rows)), np.where(np.isnan(sig), np.inf, -sig), prio, group))
    xyz = np.column_stack([table.num[c][rows] for c in ("x_a", "y_a", "z_a")]).astype(float)
    keep = np.zeros(len(rows), dtype=bool)
    bounds = np.flatnonzero(np.diff(group[order])) + 1
    for part in np.split(order, bounds):
        keep[part[greedy_keep(xyz[part], np.arange(len(part)), radius)]] = True
    return rows[keep]


def _optics(star: str) -> dict[str, Any] | None:
    import pandas as pd
    import starfile

    blocks = starfile.read(star, always_dict=True)
    optics = blocks.get("optics")
    if not isinstance(optics, pd.DataFrame) or optics.empty:
        return None
    return {c: str(optics[c].iloc[0]) for c in OPTICS_COLS if c in optics.columns}


def plan(lp, sel: Selection, *, name: str, level: str | None, diameter_a: float | None) -> dict[str, Any]:
    """What Make set would write, and what stops it. Blocking reads: call in a thread."""
    table = lp.table
    rows = np.flatnonzero(sel.final)
    blockers: list[str] = []
    unverified: list[str] = []
    notes: list[str] = []
    fcode = table.cat["frame"][rows]
    for groups in table.conflicts.values():
        hit = [g for g in groups if np.isin(fcode, g).any()]
        if len(hit) > 1:
            tomo = table.frames[hit[0][0]]["tomo"]
            blockers.append(
                f"{tomo}: one acquisition in {len(hit)} frames that cannot be pooled (another alignment, geometry "
                "or hand). Choose one in the Doctor."
            )
    unstated = sorted(
        {table.frames[f]["tomo"] for f in set(fcode.tolist()) if f >= 0 and table.frames[f].get("hand") is None}
    )
    if unstated:
        unverified.append(
            f"handedness is not stated for {len(unstated)} tomogram(s) (e.g. {unstated[0]}): a flip would mirror Z"
        )
    extracted = table.cat["extracted"][rows] > 0
    n_ext = int(extracted.sum())
    if level is None:
        level = EXTRACTED if n_ext == len(rows) and len(rows) else COORDINATES
    if level == EXTRACTED and n_ext < len(rows):
        blockers.append(
            f"{len(rows) - n_ext:,} kept rows are not extracted: make a coordinates set or take only the extracted"
        )
    if level == EXTRACTED_ONLY:
        rows = rows[extracted]
    radius = diameter_a / 2.0 if diameter_a else None
    if radius is None:
        notes.append("the species has no diameter, so clashes were not deduplicated")
    kept = dedup(table, rows, radius)
    members: list[dict[str, Any]] = []
    optics: dict[str, dict[str, Any]] = {}
    for key in table.members:
        sl = table.member_slices[key]
        mine = kept[(kept >= sl.start) & (kept < sl.stop)]
        if not len(mine):
            continue
        m = lp.pool.member(key)
        meta = table.member_meta[key]
        entry = {
            "key": key,
            "kind": m.kind.value,
            "ref": m.ref,
            "project": m.project_path,
            "project_label": m.project_label,
            "species_id": m.species_id,
            "label": m.label or m.ref,
            "star": meta["star"],
            "tomograms_star": meta["tomograms_star"],
            "rows": table.src_row[mine].tolist(),
            "uids": [table.uid_hex(int(r)) for r in mine],
        }
        if level in (EXTRACTED, EXTRACTED_ONLY):
            labels = table.categories["extracted"]
            jobs = {j["label"]: j["star"] for j in meta.get("subtomo") or []}
            codes = table.cat["extracted"][mine]
            stars = {jobs.get(labels[c]) for c in set(codes.tolist())}
            if len(stars) != 1 or None in stars:
                blockers.append(f"{entry['label']}: its kept rows come from {len(stars)} extractions; one per member")
            else:
                entry["ext_star"] = stars.pop()
                entry["ext_rows"] = table.ext_row[mine].tolist()
                o = _optics(entry["ext_star"])
                if o:
                    optics[entry["label"]] = o
        members.append(entry)
    if len({json.dumps(o, sort_keys=True) for o in optics.values()}) > 1:
        blockers.append(
            "extracted members disagree on optics: "
            + "; ".join(f"{k}: {', '.join(f'{c}={v}' for c, v in o.items())}" for k, o in optics.items())
        )
    projects = sorted({m["project"] for m in members})
    per_project: dict[str, int] = {}
    for m in members:
        per_project[m["project_label"]] = per_project.get(m["project_label"], 0) + len(m["rows"])
    return {
        "home": str(lp.home),
        "species_id": lp.species_id,
        "name": name,
        "level": COORDINATES if level == COORDINATES else EXTRACTED,
        "members": members,
        "blockers": blockers,
        "unverified": unverified,
        "notes": notes,
        "n": len(kept),
        "n_before_dedup": len(rows),
        "n_extracted": n_ext,
        "n_selected": int(sel.n),
        "n_frames": len(np.unique(table.cat["frame"][kept])) if len(kept) else 0,
        "n_projects": len(projects),
        "per_project": per_project,
        "dedup_radius_a": radius,
        "draft": json.loads(lp.pool.draft.model_dump_json()),
        "frame_choice": dict(lp.pool.frame_choice),
        "kept_by_hand": sel.kept_by_hand,
        "dropped_by_hand": sel.dropped_by_hand,
        "pool_version": table.version,
    }


# ── Worker side ──────────────────────────────────────────────────────────────────────────────────


def _absolutized_tomograms(src: str, out: Path, project: str, tomos: set[str] | None = None) -> None:
    """A member's tomograms.star copied with its paths made absolute (and only `tomos`, when given), so
    the set does not depend on where it was written from."""
    import pandas as pd
    import starfile

    from services.particles.coord_merge import _absolutize_paths

    blocks = starfile.read(src, always_dict=True)
    key = next(k for k, v in blocks.items() if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns)
    df = blocks[key]
    if tomos is not None:
        df = df[df["rlnTomoName"].astype(str).isin(tomos)]
    blocks[key] = pd.DataFrame([_absolutize_paths(r, Path(project)) for _, r in df.iterrows()]).reset_index(drop=True)
    starfile.write(blocks, out, overwrite=True)


def _tomograms_of(m: dict[str, Any], level) -> str:
    """The tomograms.star a member's rows of `level` refer to: its extraction's, beside the extracted
    particles, else the member's own."""
    if level == "extracted" and m.get("ext_star"):
        beside = Path(m["ext_star"]).with_name("tomograms.star")
        if beside.exists():
            return str(beside)
    return m["tomograms_star"]


def write_set(plan: dict[str, Any]) -> dict[str, Any]:
    """Write and publish the planned set; worker-safe. Returns the published set as JSON."""
    import pandas as pd

    from services.kitchen.snapshots import project_view
    from services.particles.particle_sets import (
        ParentRef,
        ParticleSet,
        Recipe,
        SetLevel,
        SetOp,
        allocate_id,
        publish_set,
    )
    from services.particles.set_ops import read_table

    home = Path(plan["home"])
    level = SetLevel(plan["level"])
    members = plan["members"]
    staging = Path(tempfile.mkdtemp(prefix="kitchen-set-"))
    try:
        frames: list[pd.DataFrame] = []
        parents = []
        for k, m in enumerate(members):
            src = m.get("ext_star") if level == SetLevel.EXTRACTED else m["star"]
            idx = m["ext_rows"] if level == SetLevel.EXTRACTED else m["rows"]
            table = read_table(src)
            subset = table.df.iloc[idx]
            out = staging / f"m{k}.star"
            table.write(subset, out)
            frames.append(subset)
            m["subset"] = str(out)
            m["tomos"] = sorted(subset["rlnTomoName"].astype(str).unique())
            same = Path(m["project"]).resolve() == home.resolve()
            parents.append(
                ParentRef(
                    kind="list" if m["kind"] == MemberKind.LIST.value else "job",
                    instance_id=m["ref"] if m["kind"] == MemberKind.TM.value else "",
                    output="particles" if level == SetLevel.EXTRACTED else "candidates",
                    list_ref=m["ref"] if m["kind"] == MemberKind.LIST.value else "",
                    project_path="" if same else m["project"],
                    label=f"{m['project_label']} · {m['label']}",
                    particles_star=src,
                    tomograms_star=m["tomograms_star"],
                    level=level,
                    species_id=m["species_id"],
                    mtime=Path(src).stat().st_mtime,
                )
            )
        extras: dict[str, Path] = {}
        if len(members) == 1:
            m = members[0]
            particles = Path(m["subset"])
            tomograms = staging / "tomograms.star"
            _absolutized_tomograms(_tomograms_of(m, level), tomograms, m["project"], set(m["tomos"]))
        elif level == SetLevel.COORDINATES:
            from services.particles.coord_merge import CoordSource, merge_coordinate_sources

            def lookup(project_path, tomo_name):
                try:
                    return project_view(Path(project_path)).registry.get_tilt_series(str(tomo_name))
                except KeyError:  # a tomogram the registry does not know: no acquisition key, kept apart
                    return None

            sources = [
                CoordSource(
                    star_path=Path(m["subset"]),
                    tomograms_star=Path(m["tomograms_star"]),
                    tomo_name=t,
                    project_path=Path(m["project"]),
                    list_type="auto" if m["kind"] == MemberKind.TM.value else "manual",
                    label=f"{m['project_label']}:{m['label']}:{t}",
                )
                for m in members
                for t in m["tomos"]
            ]
            merged = staging / "merged"
            merge_coordinate_sources(sources, out_dir=merged, registry_lookup=lookup, name=plan["name"])
            particles, tomograms = merged / "particles.star", merged / "tomograms.star"
            extras = {
                "provenance.star": merged / "provenance.star",
                "merge_summary.json": merged / "merge_summary.json",
            }
        else:
            from services.subtomo_merge import merge_optimisation_sets_into_jobdir, write_optimisation_set

            optsets = []
            for k, m in enumerate(members):
                d = staging / f"src{k}"
                d.mkdir()
                tstar = d / "tomograms.star"
                _absolutized_tomograms(_tomograms_of(m, level), tstar, m["project"], set(m["tomos"]))
                optset = d / "optimisation_set.star"
                write_optimisation_set(optset, particles_star=Path(m["subset"]), tomograms_star=tstar)
                optsets.append(str(optset))
            merged = staging / "merged"
            merged.mkdir()
            merge_optimisation_sets_into_jobdir(job_dir=merged, additional_sources=optsets, allow_no_primary=True)
            particles, tomograms = merged / "particles.star", merged / "tomograms.star"
            extras = {"merge_summary.json": merged / "merge_summary.json"}

        n_rows = sum(len(f) for f in frames)
        per_tomo = {}
        for f in frames:
            for t, c in f["rlnTomoName"].astype(str).value_counts().items():
                per_tomo[t] = per_tomo.get(t, 0) + int(c)
        uids = [u for m in members for u in m["uids"]]
        recipe = Recipe(
            op=SetOp.POOL,
            parents=parents,
            params={
                "description": f"STA kitchen pool · {len(members)} source(s) · {plan['n_projects']} project(s)",
                "species": plan["species_id"],
                "draft": plan["draft"],
                "frame_choice": plan["frame_choice"],
                "dedup_radius_a": plan["dedup_radius_a"],
            },
        )
        pset = ParticleSet(
            id=allocate_id(home, plan["name"]),
            name=plan["name"],
            species_id=plan["species_id"],
            level=level,
            recipe=recipe,
            n_particles=n_rows,
            n_tomograms=len(per_tomo),
            per_tomo=per_tomo,
            details={
                "n_projects": plan["n_projects"],
                "per_project": plan["per_project"],
                "kept_by_hand": plan["kept_by_hand"],
                "dropped_by_hand": plan["dropped_by_hand"],
                "deduplicated": plan["n_before_dedup"] - plan["n"],
                "pool_version": plan["pool_version"],
                "uids_file": UIDS_FILE,
            },
            notes=plan["unverified"] + plan["notes"],
            module=__name__,
        )

        def write_rows(out: Path) -> None:
            shutil.copyfile(particles, out)
            (out.parent / UIDS_FILE).write_text("\n".join(uids) + "\n")
            for name, path in extras.items():
                if path.exists():
                    shutil.copyfile(path, out.parent / name)

        published = publish_set(home, pset, write_rows, write_tomograms=lambda out: shutil.copyfile(tomograms, out))
        return json.loads(published.model_dump_json())
    finally:
        shutil.rmtree(staging, ignore_errors=True)

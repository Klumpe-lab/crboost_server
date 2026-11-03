"""Make set: the kept picks of a pool become an ordinary particle set of the home project.

A set holds picks: their positions as picked, with their uids and halves. A job reading it takes each facet it
needs of them (the images an extraction cut, poses a refinement found) from the jobs that made them
(`services/particles/set_facets.py`), so one set serves every job.

`plan(lp, sel, name=…)` (server, in a thread) reads the selection and states before anything is written: the
blockers (an acquisition in frames that cannot be pooled and no frame chosen; tomograms the merge cannot equate),
the unverified lines (handedness nobody states), the clashes deduplicated (picks of one pooled tomogram within Ø/2, a
hand pick winning, then the higher σ), the tomograms renamed, and the counts. `write_set(plan)` (a worker) writes it:
one member is a row subset of its own star; several are merged through `merge_coordinate_sources`.

Every row carries its pick's pool uid (``cbPickUid``) and a half from a hash of it (``rlnRandomSubset``;
`services/particles/pick_uids.py`). Every tomogram row records where its tomogram lives (project, name there,
per-tilt star there; `services/particles/set_origins.py`). A tomogram whose name another project of the set
also uses is renamed ``<name>__<project>`` and gets a copy of its per-tilt star under that name in the set's
``tilt_series/``. The set records its members as parents, the recipe (with the subsets it was made in and their uids
then, ``kitchen_subsets/``), the verdict counts and the frozen uid list (``kitchen_uids.txt``), so it can be compared
and replayed exactly.
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any

import numpy as np

from services.sta_kitchen.models import SOURCE_PRIORITY, MemberKind, PickSource
from services.sta_kitchen.pool import UNKNOWN_ACQ, PoolTable, Selection, acq_key, pooled_groups, row_members
from services.particles.set_ops import greedy_keep

logger = logging.getLogger(__name__)

UIDS_FILE = "kitchen_uids.txt"
# The uids each subset the set was made in held then, one file per subset: the rail compares them with now.
SUBSETS_DIR = "kitchen_subsets"


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
    return rows[greedy_keep(xyz, order, radius, group)]


def _member_label(lp, key: str) -> str:
    m = lp.pool.member(key)
    return f"{m.project_label} · {m.label or m.ref}" if m is not None else key


def _coord_sources(members: list[dict[str, Any]]) -> list:
    """One merge source per member and tomogram, in the order the plan and the merge both use (so they name
    the tomograms alike). Labelled by member key: two projects may share a directory name."""
    from services.particles.coord_merge import CoordSource

    return [
        CoordSource(
            star_path=Path(m.get("subset") or m["star"]),
            tomograms_star=Path(m["tomograms_star"]),
            tomo_name=t,
            project_path=Path(m["project"]),
            list_type="auto" if m["kind"] == MemberKind.TM.value else "manual",
            label=f"{m['key']}:{t}",
        )
        for m in members
        for t in m["tomos"]
    ]


def _acq_lookup(members: list[dict[str, Any]]):
    """The merge's acquisition lookup from the table's facts; a tomogram without one is kept apart."""
    acq = {(m["project"], t): a for m in members for t, a in (m.get("acq") or {}).items()}
    return lambda project, tomo: acq.get((str(project), str(tomo)))


def _plan_names(members: list[dict[str, Any]]) -> tuple[dict[str, str], dict[str, dict], list[str], list[str]]:
    """Each tomogram's name in the set ({"<project>|<name there>": name in the set}), each set name's origin
    ({name: {project, tomo, tilt_star}}), and what stops or doubts the naming (`plan_merge`: tomograms
    equated as one acquisition whose geometry disagrees; handedness nobody states). A name stays unless
    another project of the set uses it too."""
    from services.particles.coord_merge import plan_merge

    sources = _coord_sources(members)
    stars = {(m["project"], t): ts for m in members for t, ts in (m.get("tilt_stars") or {}).items()}
    if len(members) == 1:
        assign = {f"{s.project_path}|{s.tomo_name}": s.tomo_name for s in sources}
        blocking, unverified, named = [], [], {s.display(): s.tomo_name for s in sources}
    else:
        mp = plan_merge(sources, acquisition_lookup=_acq_lookup(members))
        assign = {f"{s.project_path}|{s.tomo_name}": mp.assignment[s.display()] for s in sources}
        blocking, unverified, named = mp.blocking, mp.unverified, mp.assignment
    origins: dict[str, dict] = {}
    for s in sources:
        origins.setdefault(
            named[s.display()],
            {
                "project": str(s.project_path),
                "tomo": s.tomo_name,
                "tilt_star": stars.get((str(s.project_path), s.tomo_name), ""),
            },
        )
    return assign, origins, list(blocking), list(unverified)


def plan(
    lp,
    sel: Selection,
    *,
    name: str,
    diameter_a: float | None,
    draft=None,
    scope_uids: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """What Make set would write, and what stops it. Blocking reads: call in a thread. `draft`: the recipe
    read (the pool's, or it with the scope a rail command set); `scope_uids`: the uids of the subsets that
    scope names, kept with the set."""
    table = lp.table
    draft = draft if draft is not None else lp.pool.draft
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
                "or hand). Choose its frame below."
            )
    if table.source_conflicts:
        codes: dict[str, list[int]] = {}
        for f, fr in enumerate(table.frames):
            codes.setdefault(acq_key(fr), []).append(f)
        owner = row_members(table)[rows]
        for acq in table.source_conflicts:
            fed = np.unique(owner[np.isin(fcode, codes.get(acq, []))])
            if len(fed) > 1:
                tomo = table.frames[codes[acq][0]]["tomo"]
                labels = ", ".join(_member_label(lp, table.members[m]) for m in fed)
                blockers.append(f"{tomo}: fed by {len(fed)} sources ({labels}). Choose one in Sources.")
    unstated = sorted(
        {table.frames[f]["tomo"] for f in set(fcode.tolist()) if f >= 0 and table.frames[f].get("hand") is None}
    )
    if unstated:
        unverified.append(
            f"handedness is not stated for {len(unstated)} tomogram(s) (e.g. {unstated[0]}): a flip would mirror Z"
        )
    radius = diameter_a / 2.0 if diameter_a else None
    if radius is None:
        notes.append("the species has no diameter, so clashes were not deduplicated")
    kept = dedup(table, rows, radius)
    tomo_names = table.categories["tomo"]
    members: list[dict[str, Any]] = []
    for key in table.members:
        sl = table.member_slices[key]
        mine = kept[(kept >= sl.start) & (kept < sl.stop)]
        if not len(mine):
            continue
        m = lp.pool.member(key)
        meta = table.member_meta[key]
        frames = table.cat["frame"][mine]
        tomo_of_row = [tomo_names[t] for t in table.cat["tomo"][mine].tolist()]
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
            # each row's tomogram by its name in the member's project, and the member's tomograms in order
            "tomo_of_row": tomo_of_row,
            "tomos": sorted(set(tomo_of_row)),
            # tomogram -> its acquisition, as the table knows it: the merge equates tomograms by it
            "acq": {
                table.frames[f]["tomo"]: table.frames[f]["acq"]
                for f in set(frames.tolist())
                if f >= 0 and table.frames[f].get("acq") not in (None, "", UNKNOWN_ACQ)
            },
            # tomogram -> its per-tilt star in the member's project (the set's tomograms record it)
            "tilt_stars": {table.frames[f]["tomo"]: table.frames[f].get("ts_star") or "" for f in set(frames.tolist())},
        }
        members.append(entry)
    assign, origins, merge_blocking, merge_unverified = _plan_names(members) if members else ({}, {}, [], [])
    blockers += merge_blocking
    unverified += [u for u in merge_unverified if u not in unverified]
    by_label = {m["project"]: m["project_label"] for m in members}
    renamed = sorted(
        (
            {"project": by_label.get(key.split("|", 1)[0], key.split("|", 1)[0]), "tomo": key.split("|", 1)[1], "as": n}
            for key, n in assign.items()
            if key.split("|", 1)[1] != n
        ),
        key=lambda r: (r["tomo"], r["project"]),
    )
    excluded = sorted(
        (table.frames[f]["project"], table.frames[f]["tomo"])
        for f in set(table.cat["frame"][kept].tolist())
        if f >= 0 and table.frames[f].get("excluded")
    )
    if excluded:
        named = ", ".join(f"{t} in {p}" for p, t in excluded[:3]) + (", …" if len(excluded) > 3 else "")
        notes.append(
            f"{len(excluded)} tomogram(s) are excluded in their own project ({named}): the set takes their "
            "picks, as a job reading a set applies no exclusions"
        )
    projects = sorted({m["project"] for m in members})
    per_project: dict[str, int] = {}
    for m in members:
        per_project[m["project_label"]] = per_project.get(m["project_label"], 0) + len(m["rows"])
    per_project_tomos = tomograms_per_project(members, assign)
    scope = draft.scope
    in_scope = [*scope.include, *scope.exclude]
    return {
        "home": str(lp.home),
        "species_id": lp.species_id,
        "name": name,
        "members": members,
        "blockers": blockers,
        "unverified": unverified,
        "notes": notes,
        "n": len(kept),
        "n_before_dedup": len(rows),
        "n_selected": int(sel.n),
        "n_frames": len(np.unique(table.cat["frame"][kept])) if len(kept) else 0,
        "n_projects": len(projects),
        "per_project": per_project,
        "per_project_tomograms": per_project_tomos,
        "dedup_radius_a": radius,
        "draft": json.loads(draft.model_dump_json()),
        "frame_choice": dict(lp.pool.frame_choice),
        "source_choice": dict(lp.pool.source_choice),
        "subsets": {x.id: x.name for x in lp.pool.subsets},
        "groups": {
            x.id: lp.pool.group(x.group).name
            for x in lp.pool.subsets
            if x.id in in_scope and x.group and lp.pool.group(x.group)
        },
        "scope_uids": {sid: list(uids) for sid, uids in (scope_uids or {}).items()},
        "assign": assign,
        "origins": origins,
        "renamed": renamed,
        "kept_by_hand": sel.kept_by_hand,
        "dropped_by_hand": sel.dropped_by_hand,
        "pool_version": table.version,
    }


def tomograms_per_project(members: list[dict[str, Any]], assign: dict[str, str]) -> dict[str, int]:
    """Tomograms of the set per project, by their names in the set: two projects' Position_1 are two, and one
    tomogram two members share is one."""
    names: dict[str, set[str]] = {}
    for m in members:
        names.setdefault(m["project_label"], set()).update(assign[f"{m['project']}|{t}"] for t in m["tomos"])
    return {label: len(n) for label, n in names.items()}


# ── Worker side ──────────────────────────────────────────────────────────────────────────────────


def _tomogram_rows(
    src: str,
    names: dict[str, str],
    origins: dict[str, dict],
    *,
    fallback_project: str,
    tilt_staging: Path,
    tilt_final: Path,
):
    """The set's tomogram rows from tomograms star `src`: for each {name in src: name in the set}, its row with
    paths absolute (against the project `src` belongs to), renamed, its origin recorded; a renamed one reads a
    copy of its per-tilt star with the block renamed (written to `tilt_staging`, named in `tilt_final`)."""
    import pandas as pd
    import starfile

    from services.particles.coord_merge import _absolutize_paths
    from services.particles.set_origins import (
        ORIGIN_PROJECT,
        ORIGIN_TILT_STAR,
        ORIGIN_TOMO,
        project_of,
        write_renamed_tilt_star,
    )

    blocks = starfile.read(src, always_dict=True)
    df = next(v for v in blocks.values() if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns)
    by_name = {str(n): i for i, n in enumerate(df["rlnTomoName"].astype(str))}
    project = project_of(Path(src), Path(fallback_project))
    out = []
    for old, new in names.items():
        if old not in by_name:
            raise ValueError(f"tomogram {old} has no row in {src}")
        row = _absolutize_paths(df.iloc[by_name[old]], project)
        row["rlnTomoName"] = new
        if new != old:
            tilt = Path(str(row.get("rlnTomoTiltSeriesStarFile") or ""))
            if not str(tilt) or not tilt.is_absolute():
                raise ValueError(f"tomogram {old} in {src} names no per-tilt star to copy for {new}")
            write_renamed_tilt_star(tilt, tilt_staging / f"{new}.star", old, new, project_of(tilt, project))
            row["rlnTomoTiltSeriesStarFile"] = str(tilt_final / f"{new}.star")
        o = origins[new]
        row[ORIGIN_PROJECT], row[ORIGIN_TOMO], row[ORIGIN_TILT_STAR] = o["project"], o["tomo"], o["tilt_star"]
        out.append(row)
    return pd.DataFrame(out).reset_index(drop=True)


def _write_tomograms(df, out: Path) -> None:
    from services.subtomo_merge import _write_tomograms_star

    out.parent.mkdir(parents=True, exist_ok=True)
    _write_tomograms_star(out, df)


def _stamp_rows(subset, m: dict[str, Any], src: str, notes: list[str]) -> None:
    """Every row's pick uid and half, in place. A member's own rows are its picks: a uid they carry from elsewhere
    is replaced, and said."""
    from services.particles.pick_uids import HALF_COL, UID_COL, half_of, read_uid_column, uid_cell

    col = read_uid_column(subset)
    if col is not None:
        wrong = sum(u != want for u, want in zip(col.uids, m["uids"], strict=True))
        if wrong:
            notes.append(f"{wrong:,} rows of {Path(src).name} carried another {UID_COL}; replaced by their pool uid")
    halves = [half_of(u) for u in m["uids"]]
    if HALF_COL in subset.columns:
        moved = int((subset[HALF_COL].astype(str) != [str(h) for h in halves]).sum())
        if moved:
            notes.append(f"{moved:,} rows of {Path(src).parent.name} had another {HALF_COL}; set from their uid")
    subset[UID_COL] = [uid_cell(u) for u in m["uids"]]
    subset[HALF_COL] = halves


def write_set(plan: dict[str, Any]) -> dict[str, Any]:
    """Write and publish the planned set; worker-safe. Returns the published set as JSON."""
    import pandas as pd

    from services.particles.particle_sets import (
        ParentRef,
        ParticleSet,
        Recipe,
        SetLevel,
        SetOp,
        allocate_id,
        publish_set,
        set_dir,
    )
    from services.particles.set_facets import PER_ROW_DROPPED
    from services.particles.set_origins import TILT_DIR
    from services.particles.set_ops import read_table

    home = Path(plan["home"])
    level = SetLevel.COORDINATES
    members = plan["members"]
    assign, origins = plan["assign"], plan["origins"]
    set_id = allocate_id(home, plan["name"])
    tilt_final = set_dir(home, set_id) / TILT_DIR
    staging = Path(tempfile.mkdtemp(prefix="kitchen-set-"))
    tilt_staging = staging / TILT_DIR
    notes: list[str] = []
    try:
        parents = []
        tomo_names: list[dict[str, str]] = []  # per member: {name in its rows' star: name in the set}
        for k, m in enumerate(members):
            src = m["star"]
            table = read_table(src)
            subset = table.df.iloc[m["rows"]].copy()
            _stamp_rows(subset, m, src, notes)
            subset = subset.drop(columns=[c for c in PER_ROW_DROPPED if c in subset.columns])
            tomo_names.append({t: assign[f"{m['project']}|{t}"] for t in m["tomos"]})
            out = staging / f"m{k}.star"
            table.write(subset, out)
            m["subset"] = str(out)
            same = Path(m["project"]).resolve() == home.resolve()
            parents.append(
                ParentRef(
                    kind="list" if m["kind"] == MemberKind.LIST.value else "job",
                    instance_id=m["ref"] if m["kind"] == MemberKind.TM.value else "",
                    output="candidates",
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
        tomograms = staging / "tomograms.star"
        rows_kw = {"origins": origins, "tilt_staging": tilt_staging, "tilt_final": tilt_final}
        if len(members) > 1:
            from services.particles.coord_merge import merge_coordinate_sources

            merged = staging / "merged"
            merge_coordinate_sources(
                _coord_sources(members), out_dir=merged, acquisition_lookup=_acq_lookup(members), name=plan["name"]
            )
            particles = merged / "particles.star"
            # The set's tomograms as the merge named them, each row from the first member with a tomogram of that
            # name (the row the merge kept), with its origin, a renamed one with its own per-tilt star.
            first: dict[str, tuple[dict, str]] = {}
            for m, names in zip(members, tomo_names, strict=True):
                for old, new in names.items():
                    first.setdefault(new, (m, old))
            parts = [
                _tomogram_rows(m["tomograms_star"], {old: new}, fallback_project=m["project"], **rows_kw)
                for new, (m, old) in sorted(first.items())
            ]
            _write_tomograms(pd.concat(parts, ignore_index=True), tomograms)
            extras = {
                "provenance.star": merged / "provenance.star",
                "merge_summary.json": merged / "merge_summary.json",
            }
        else:
            m, names = members[0], tomo_names[0]
            particles = Path(m["subset"])
            df = _tomogram_rows(m["tomograms_star"], names, fallback_project=m["project"], **rows_kw)
            _write_tomograms(df, tomograms)

        # Counted by the names in the set: two projects' Position_1 are two tomograms.
        per_tomo = {
            str(t): int(c) for t, c in read_table(particles).df["rlnTomoName"].astype(str).value_counts().items()
        }
        n_rows = sum(per_tomo.values())
        per_project_tomos = tomograms_per_project(members, assign)
        uids = [u for m in members for u in m["uids"]]
        scope = plan["draft"].get("scope", {})
        in_scope = [*scope.get("include", []), *scope.get("exclude", [])]
        recipe = Recipe(
            op=SetOp.POOL,
            parents=parents,
            params={
                "description": f"STA kitchen pool · {len(members)} source(s) · {plan['n_projects']} project(s)",
                "species": plan["species_id"],
                "draft": plan["draft"],
                "frame_choice": plan["frame_choice"],
                "source_choice": plan.get("source_choice", {}),
                "subsets": {i: plan.get("subsets", {}).get(i, i) for i in in_scope},
                "scope": {"include": list(scope.get("include", [])), "exclude": list(scope.get("exclude", []))},
                "groups": {i: g for i, g in (plan.get("groups") or {}).items() if i in in_scope},
                "dedup_radius_a": plan["dedup_radius_a"],
            },
        )
        pset = ParticleSet(
            id=set_id,
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
                "per_project_tomograms": per_project_tomos,
                "renamed": plan.get("renamed", []),
                "kept_by_hand": plan["kept_by_hand"],
                "dropped_by_hand": plan["dropped_by_hand"],
                "deduplicated": plan["n_before_dedup"] - plan["n"],
                "pool_version": plan["pool_version"],
                "uids_file": UIDS_FILE,
            },
            notes=plan["unverified"] + plan["notes"] + notes,
            module=__name__,
        )

        def write_rows(out: Path) -> None:
            shutil.copyfile(particles, out)
            (out.parent / UIDS_FILE).write_text("\n".join(uids) + "\n")
            for name, path in extras.items():
                if path.exists():
                    shutil.copyfile(path, out.parent / name)
            snapshots = plan.get("scope_uids") or {}
            if snapshots:
                (out.parent / SUBSETS_DIR).mkdir()
                for sid, held in snapshots.items():
                    (out.parent / SUBSETS_DIR / f"{sid}.txt").write_text("".join(f"{u}\n" for u in held))

        def write_tomograms(out: Path) -> None:
            shutil.copyfile(tomograms, out)
            if tilt_staging.is_dir():
                shutil.copytree(tilt_staging, out.parent / TILT_DIR)

        published = publish_set(home, pset, write_rows, write_tomograms=write_tomograms)
        return json.loads(published.model_dump_json())
    finally:
        shutil.rmtree(staging, ignore_errors=True)

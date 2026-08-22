"""Coordinate-grade union of one species' pick lists, across tomograms and projects
(roadmap `picking_ui/12-S2`).

The stage-③ merge in ``services/subtomo_merge.py`` unites EXTRACTED particles: it needs an
optics block and refuses a candidates star by name. This module is its counterpart one
stage earlier, uniting POSITIONS — a manual list, a ``candidates.star`` and an imported
list are the same kind of thing here, differing only in how many columns they carry
(``docs/particle-data-flow.md`` §2).

Output is the same directory shape the post-extraction merge writes, so one consumer
serves both grades::

    <out_dir>/
        particles.star          coordinate grade: no optics, no rlnImageName
        tomograms.star          union of the contributing frames
        optimisation_set.star   the envelope pointing at the two above
        merge_summary.json      counts, sources, per-column coverage
        provenance.star         per-row values the main star cannot honestly carry

``relion_tomo_subtomo`` consumes this directly: ``subtomo_merge`` reads an input particles
file leniently ("Only ``rlnTomoName`` is required"), and ``list_extraction`` synthesizes
the optics block at extraction time from ``tomograms.star`` — raising rather than inventing
when a required optics column is absent. Nothing here fabricates one either.

**One species per merge.** Enforced by the caller, not here: this module never reads a
species. Uniting two species into one particle set is not a workflow (roadmap 12, D2).

**Column policy** (roadmap 12, D5). Sources contribute different column sets, and a
placeholder must never be indistinguishable from a measurement:

  * ``rlnAngleRot/Tilt/Psi`` — filled with 0 for sources that lack them. This is not an
    invented value: 0,0,0 is RELION's own encoding of "no orientation prior", and
    ``list_extraction`` already writes exactly that for hand-picked lists.
  * score columns — 0 is a LEGAL (bad) LCC value, so it can never stand for "absent". A
    ragged score column is DROPPED from the main star and preserved per-row in
    ``provenance.star``; the caller states the coverage wherever it offers a score filter.
  * anything else ragged — same treatment as score: sidecar only, coverage in the summary.
    Promoted into the main star only when every source carries it.

numpy/pandas/starfile are hard deps (top-level, as everywhere in ``services/particles``).
Compile-checked in Claude's bare venv; runtime-exercised by the aggregation flow.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import starfile

from services.particles.coords import CENTERED_COLS
from services.particles.pick_merge import clash_stats_coords, type_priority
from services.particles.tomo_identity import acquisition_key, check_transferable, read_tomo_hand
from services.subtomo_merge import write_optimisation_set

logger = logging.getLogger(__name__)

# Orientation columns. Absent → 0, which is RELION's real "no prior", not a placeholder.
ANGLE_COLS = ("rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi")

# Score columns, in the priority order the rest of the codebase already uses
# (`picks_filter`, `preview_orchestrator`). 0 is a legal value for every one of them, so
# none may ever be filled in.
SCORE_COLS = ("rlnLCCmax", "rlnAutopickFigureOfMerit", "rlnMaxValueProbDistribution")

# Always in the main star.
_SPINE_COLS = ("rlnTomoName", *CENTERED_COLS, "rlnTomoParticleName")


@dataclass
class CoordSource:
    """One contributing pick list, already resolved to files on disk.

    ``tomo_name`` is the ``rlnTomoName`` this list uses INSIDE ``project_path``. It may
    collide with a different tomogram of the same name in another project; resolving that
    is this module's job, not the caller's.
    """

    star_path: Path
    tomograms_star: Path
    tomo_name: str
    project_path: Path
    list_type: str = "auto"
    label: str = ""

    @property
    def priority(self) -> int:
        return type_priority(self.list_type)

    def display(self) -> str:
        return self.label or f"{Path(self.project_path).name}:{self.tomo_name}:{Path(self.star_path).stem}"


@dataclass
class MergePlan:
    """What a merge WOULD do, computed before anything is written.

    ``blocking`` is fatal — same acquisition, incompatible reconstruction. ``unverified``
    is for the user to see, not for this module to resolve (handedness that no star
    states is the usual one). ``renamed`` records every ``rlnTomoName`` that had to be
    disambiguated so the caller can show it rather than let a silent rename surprise
    someone downstream.
    """

    blocking: list[str] = field(default_factory=list)
    unverified: list[str] = field(default_factory=list)
    renamed: dict[str, str] = field(default_factory=dict)
    tomo_rows: dict[str, pd.Series] = field(default_factory=dict)
    # source display -> merged rlnTomoName
    assignment: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.blocking


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def _coord_block(star_path: Path) -> pd.DataFrame:
    """The block carrying ``rlnTomoName`` + the three centred-Å columns, with EVERY column
    it happens to have. Raises rather than returning an empty frame: a list star with no
    coordinate block is a broken input, not an empty one."""
    data = starfile.read(Path(star_path), always_dict=True)
    for block in data.values():
        if isinstance(block, pd.DataFrame) and all(c in block.columns for c in CENTERED_COLS):
            return block.copy()
    raise ValueError(f"No centred-coordinate block (needs {list(CENTERED_COLS)}) in {star_path}")


def _tomo_row(tomograms_star: Path, tomo_name: str) -> pd.Series:
    data = starfile.read(Path(tomograms_star), always_dict=True)
    for block in data.values():
        if isinstance(block, pd.DataFrame) and "rlnTomoName" in block.columns:
            hit = block[block["rlnTomoName"].astype(str) == str(tomo_name)]
            if len(hit):
                return hit.iloc[0]
    raise ValueError(f"Tomogram {tomo_name!r} has no row in {tomograms_star}")


def _hand_for(tomo_row: pd.Series, project_path: Path) -> int | None:
    """Handedness for one tomogram, or None when nothing on disk states it.

    ``rlnTomoHand`` is not a ``tomograms.star`` column — it is written into the Import
    job's tilt star and reached through ``rlnTomoTiltSeriesStarFile``.
    """
    ref = tomo_row.get("rlnTomoTiltSeriesStarFile")
    if ref is None or (isinstance(ref, float) and pd.isna(ref)):
        return None
    path = Path(str(ref))
    if not path.is_absolute():
        path = Path(project_path) / path
    return read_tomo_hand(path)


def _absolutize_paths(row: pd.Series, project_path: Path) -> pd.Series:
    """Resolve a tomogram row's path-like values against the project that wrote them.

    A source project's ``tomograms.star`` may hold paths relative to ITS root. Copied
    verbatim into a merged star that lives somewhere else, they resolve to nothing -- and
    a tomogram whose reconstruction path does not exist is a per-TS extraction that
    silently produces no particles, not an error.
    """
    row = row.copy()
    for col in row.index:
        if not (str(col).endswith(("File", "Dir")) or str(col) == "rlnTomoReconstructedTomogram"):
            continue
        val = row[col]
        if not isinstance(val, str) or not val:
            continue
        p = Path(val)
        if not p.is_absolute():
            row[col] = str((Path(project_path) / p).resolve())
    return row


def _project_token(project_path: Path) -> str:
    """Short, filesystem-safe tag for a project, used only to break tomogram-name ties."""
    return re.sub(r"[^A-Za-z0-9]+", "-", Path(project_path).name).strip("-") or "proj"


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def plan_merge(sources: list[CoordSource], *, registry_lookup=None) -> MergePlan:
    """Resolve tomogram identity across the sources WITHOUT writing anything.

    Two tomograms are treated as one iff they resolve to the same acquisition —
    ``registry_lookup(project_path, tomo_name) -> TiltSeries | None`` supplies the
    ``TiltSeries`` whose mdoc key answers that. Pass None (the default) and no two
    tomograms are ever equated across projects: they are kept apart under disambiguated
    names, which is the conservative reading and never silently pools unrelated picks.

    Equated tomograms then face the transferability gate: same acquisition, different
    reconstruction geometry means the centred-Å origin is a different physical point, so
    the coordinates may not be pooled. That lands in ``blocking``.
    """
    plan = MergePlan()
    # merged name -> (row, project_path, acquisition key or None, handedness)
    accepted: dict[str, tuple[pd.Series, Path, tuple[str, str] | None, int | None]] = {}
    by_key: dict[tuple[str, str], str] = {}
    used_names: set[str] = set()

    for src in sources:
        row = _tomo_row(src.tomograms_star, src.tomo_name)
        hand = _hand_for(row, src.project_path)
        key = None
        if registry_lookup is not None:
            ts = registry_lookup(src.project_path, src.tomo_name)
            if ts is not None:
                key = acquisition_key(ts)

        if key is not None and key in by_key:
            merged_name = by_key[key]
            kept_row, kept_proj, _, kept_hand = accepted[merged_name]
            report = check_transferable(kept_row, row, hand_a=kept_hand, hand_b=hand)
            plan.unverified.extend(report.unverified)
            if not report.ok:
                plan.blocking.extend(report.blocking)
            if str(kept_proj) != str(src.project_path):
                plan.renamed.setdefault(f"{_project_token(src.project_path)}:{src.tomo_name}", merged_name)
            plan.assignment[src.display()] = merged_name
            continue

        merged_name = str(src.tomo_name)
        if merged_name in used_names:
            merged_name = f"{src.tomo_name}__{_project_token(src.project_path)}"
            suffix = 2
            while merged_name in used_names:
                merged_name = f"{src.tomo_name}__{_project_token(src.project_path)}-{suffix}"
                suffix += 1
            plan.renamed[f"{_project_token(src.project_path)}:{src.tomo_name}"] = merged_name
            if key is None:
                plan.unverified.append(
                    f"{src.tomo_name} exists in more than one selected project and no acquisition key "
                    f"was available — kept apart as {merged_name}; if they are the same tilt series, "
                    "their picks are NOT pooled"
                )

        used_names.add(merged_name)
        row = _absolutize_paths(row, src.project_path)
        row["rlnTomoName"] = merged_name
        accepted[merged_name] = (row, Path(src.project_path), key, hand)
        if key is not None:
            by_key[key] = merged_name
        plan.tomo_rows[merged_name] = row
        plan.assignment[src.display()] = merged_name

    # De-duplicate the advisory lines; the same missing handedness repeats per pair.
    plan.unverified = list(dict.fromkeys(plan.unverified))
    plan.blocking = list(dict.fromkeys(plan.blocking))
    return plan


# ---------------------------------------------------------------------------
# Column policy
# ---------------------------------------------------------------------------


def _classify_columns(frames: list[pd.DataFrame]) -> tuple[list[str], list[str], dict[str, int]]:
    """Split the union of every source's columns into (main-star, sidecar-only, coverage).

    Coverage counts ROWS that actually state the column, which is what the UI shows next
    to a filter ("applies to 1,204 of 3,890").
    """
    total_rows = sum(len(f) for f in frames)
    union: list[str] = []
    for f in frames:
        for c in f.columns:
            if c not in union:
                union.append(c)

    coverage: dict[str, int] = {}
    for col in union:
        n = 0
        for f in frames:
            if col in f.columns:
                n += int(f[col].notna().sum())
        coverage[col] = n

    main: list[str] = []
    sidecar: list[str] = []
    for col in union:
        if col in _SPINE_COLS:
            continue  # handled explicitly
        if col in ANGLE_COLS:
            main.append(col)  # 0 is RELION's real "no prior"
            continue
        # Score columns and everything else share one rule, and deliberately: a column
        # every row states is honest in the main star, a ragged one is not. Score is
        # called out in SCORE_COLS only so the caller can name it in the UI ("filter
        # applies to N of M") -- there is no separate policy for it here.
        (main if coverage[col] == total_rows else sidecar).append(col)
    return main, sidecar, coverage


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------


def merge_coordinate_sources(
    sources: list[CoordSource], *, out_dir: Path, registry_lookup=None, name: str = ""
) -> dict[str, Any]:
    """Union `sources` into a coordinate-grade merged set under `out_dir`.

    Rows are concatenated in ascending list-type priority (``merged`` < ``manual`` <
    ``imported`` < ``filtered`` < ``auto``), and that ORDER is the only thing encoding
    "a hand placement wins a clash" — the dedup in ``pick_merge`` is a keep-first walk.
    Nothing is deduplicated here: the union is meant to contain clashers so the caller can
    report them and let the user decide (roadmap 12, D4).

    Raises when the plan is blocking. Returns the summary dict, which is also written to
    ``merge_summary.json``.
    """
    if len(sources) < 2:
        raise ValueError(f"A merge needs at least 2 sources; got {len(sources)}")

    plan = plan_merge(sources, registry_lookup=registry_lookup)
    if not plan.ok:
        raise ValueError("Cannot merge these sources:\n  " + "\n  ".join(plan.blocking))

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ordered = sorted(sources, key=lambda s: s.priority)
    frames: list[pd.DataFrame] = []
    provenance_frames: list[pd.DataFrame] = []
    per_source: list[dict[str, Any]] = []

    for src in ordered:
        df = _coord_block(src.star_path)
        if "rlnTomoName" in df.columns:
            df = df[df["rlnTomoName"].astype(str) == str(src.tomo_name)].reset_index(drop=True)
        merged_name = plan.assignment[src.display()]
        df["rlnTomoName"] = merged_name
        prov = pd.DataFrame(index=df.index)
        prov["cbSourceLabel"] = src.display()
        prov["cbSourceProject"] = str(src.project_path)
        prov["cbSourceList"] = str(src.star_path)
        prov["cbSourceType"] = src.list_type
        prov["cbSourceParticleName"] = (
            df["rlnTomoParticleName"].astype(str) if "rlnTomoParticleName" in df.columns else ""
        )
        frames.append(df)
        provenance_frames.append(prov)
        per_source.append(
            {
                "label": src.display(),
                "list_star": str(src.star_path),
                "project": str(src.project_path),
                "tomo_name": src.tomo_name,
                "merged_tomo_name": merged_name,
                "list_type": src.list_type,
                "n_picks": len(df),
            }
        )

    main_cols, sidecar_cols, coverage = _classify_columns(frames)
    merged = pd.concat(frames, ignore_index=True, sort=False)
    prov = pd.concat(provenance_frames, ignore_index=True, sort=False)

    # rlnTomoParticleName is reassigned merged-wide: the sources' own names are unique only
    # within their own set, and a merged set needs one join key. The originals ride along
    # in the sidecar rather than being lost.
    merged["rlnTomoParticleName"] = (
        merged["rlnTomoName"].astype(str) + "/" + (merged.groupby("rlnTomoName").cumcount() + 1).astype(str)
    )
    prov.insert(0, "rlnTomoParticleName", merged["rlnTomoParticleName"].values)

    for col in ANGLE_COLS:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0.0)

    for col in sidecar_cols:
        if col in merged.columns:
            prov[col] = merged[col].values
    dropped = [c for c in sidecar_cols if c in merged.columns]
    keep = [c for c in _SPINE_COLS if c in merged.columns] + [c for c in main_cols if c in merged.columns]
    particles = merged[keep].copy()

    out_particles = out_dir / "particles.star"
    out_tomograms = out_dir / "tomograms.star"
    out_optset = out_dir / "optimisation_set.star"
    out_prov = out_dir / "provenance.star"
    out_summary = out_dir / "merge_summary.json"

    starfile.write({"particles": particles}, out_particles, overwrite=True)
    tomos = pd.DataFrame([plan.tomo_rows[n] for n in sorted(plan.tomo_rows)]).reset_index(drop=True)
    # Contributing projects can carry different tomograms.star schemas. Stacking them fills
    # the gaps with NaN, and a NaN reconstruction path or tilt-star reference is a tomogram
    # that extraction skips without complaining. Say so instead of writing it.
    ragged = [c for c in tomos.columns if tomos[c].isna().any() and tomos[c].notna().any()]
    if ragged:
        raise ValueError(
            "The selected projects write different tomograms.star schemas; these columns are "
            f"stated for some tomograms and not others: {sorted(ragged)}. A merged star with "
            "gaps here would be skipped silently downstream."
        )
    starfile.write({"global": tomos}, out_tomograms, overwrite=True)
    starfile.write({"provenance": prov}, out_prov, overwrite=True)
    write_optimisation_set(out_optset, particles_star=out_particles, tomograms_star=out_tomograms)

    summary: dict[str, Any] = {
        "grade": "coordinate",
        "name": name or out_dir.name,
        "n_picks": len(particles),
        "n_tomograms": len(tomos),
        "n_sources": len(ordered),
        "sources": per_source,
        "outputs": {
            "particles_star": str(out_particles),
            "tomograms_star": str(out_tomograms),
            "optimisation_set": str(out_optset),
            "provenance_star": str(out_prov),
        },
        "columns": {
            "in_particles_star": list(particles.columns),
            "sidecar_only": dropped,
            "coverage": {c: int(n) for c, n in coverage.items()},
        },
        "tomo_renames": plan.renamed,
        "unverified": plan.unverified,
    }
    out_summary.write_text(json.dumps(summary, indent=2))
    logger.info(
        "Coordinate merge -> %s: %d picks, %d tomograms, %d sources (sidecar-only: %s)",
        out_dir,
        len(particles),
        len(tomos),
        len(ordered),
        dropped or "none",
    )
    return summary


# ---------------------------------------------------------------------------
# Collision report (roadmap 12-S3)
# ---------------------------------------------------------------------------


def clash_report(particles_star: Path, radius_angst: float) -> dict[str, Any]:
    """Per-tomogram and total clash counts for a merged coordinate set at `radius_angst`.

    Computed AFTER the union and never applied: the union is meant to contain clashers,
    and dedup is the user's call (roadmap 12, D4). Row order carries priority, so the
    ``n_removed`` here is exactly what ``pick_merge.deduplicate_star`` would drop.
    """
    df = _coord_block(Path(particles_star))
    per_tomo: dict[str, dict[str, int]] = {}
    totals = {"n_total": 0, "n_clashing": 0, "n_removed": 0, "n_after": 0}
    names = df["rlnTomoName"].astype(str).unique().tolist() if "rlnTomoName" in df.columns else ["?"]
    for name in names:
        rows = df[df["rlnTomoName"].astype(str) == name] if "rlnTomoName" in df.columns else df
        stats = clash_stats_coords(rows[list(CENTERED_COLS)].to_numpy(dtype=float), radius_angst)
        per_tomo[name] = stats
        for k in totals:
            totals[k] += int(stats[k])
    return {"radius_angst": float(radius_angst), "total": totals, "per_tomogram": per_tomo}


# ---------------------------------------------------------------------------
# Headless CLI
# ---------------------------------------------------------------------------
#
# Same precedent as `list_extraction`: build the artifacts WITHOUT touching SLURM so the
# star format can be eyeballed before the dialog exists, and so the merge is verifiable in
# the module env where numpy/pandas/starfile actually import.
#
#   python -m services.particles.coord_merge \
#       --source <list.star>:<tomograms.star>:<tomo>:<project>:<type> \
#       --source ... --out <dir> [--radius 180]


def _parse_source(spec: str) -> CoordSource:
    parts = spec.split(":")
    if len(parts) < 4:
        raise SystemExit(f"--source needs list:tomograms:tomo:project[:type], got {spec!r}")
    return CoordSource(
        star_path=Path(parts[0]),
        tomograms_star=Path(parts[1]),
        tomo_name=parts[2],
        project_path=Path(parts[3]),
        list_type=parts[4] if len(parts) > 4 else "auto",
    )


def _main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Union coordinate-grade pick lists into a merged set.")
    ap.add_argument("--source", action="append", required=True, help="list:tomograms:tomo:project[:type]")
    ap.add_argument("--out", required=True, help="output directory")
    ap.add_argument("--radius", type=float, default=0.0, help="clash-report radius in Angstrom (0 = skip)")
    args = ap.parse_args()

    summary = merge_coordinate_sources([_parse_source(s) for s in args.source], out_dir=Path(args.out))
    print(json.dumps(summary, indent=2))
    for line in summary["unverified"]:
        print(f"[UNVERIFIED] {line}")
    if args.radius > 0:
        print(json.dumps(clash_report(Path(summary["outputs"]["particles_star"]), args.radius), indent=2))


if __name__ == "__main__":
    _main()

"""Per-pick-list subtomo-extraction prep.

A pick list's star (manual / imported / merged) carries only ``rlnTomoName`` +
the three ``rlnCenteredCoordinate*Angst`` columns. ``relion_tomo_subtomo`` (the
extraction driver) instead consumes an ``optimisation_set.star`` → a full
``particles.star`` (it reads ``rlnOpticsGroup`` and slices by ``rlnTomoName``) +
a ``tomograms.star``.

There are two ways to build that input, and they differ only in where the particle
schema and the optics block come from:

``build_list_optset`` — the species HAS a candidate-extract job. MIRROR its
``candidates.star`` (the known-good extraction input for this species): reuse its
exact column set + any non-particle blocks (e.g. data_optics) verbatim.

``build_list_optset_from_tomograms`` — the species has NO candidate-extract job (a
de-novo species picked by hand, possibly on imported tomograms). There is nothing to
mirror, so the particle schema is declared here and a minimal ``data_optics`` is read
off the ``tomograms.star`` global block. Any required optics column that is absent
raises — a synthesized optics block may carry no invented values.

Both synthesise one particle row per list coordinate with orientations at 0, since
manual picks are positions only. The output lands in the list's OWN dir so lists
extract independently and never overwrite each other.

Output (under ``Curation/<species>/<tomo>/<slug>/``):
  - ``particles.star``        — the list's coordinates in the chosen schema
  - ``optimisation_set.star`` — points at the above + the shared tomograms.star

The optset read/write primitives come from ``services.subtomo_merge`` so this and
the in-job extraction driver read and emit the identical format.

NOTE: this is py_compile + ruff only in Claude's venv (no numpy/pandas there); it
is runtime-tested by the user in the module env. The CLI below builds the artifacts
WITHOUT running extraction so the star format can be eyeballed before a SLURM job is
ever submitted.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import starfile

from services.subtomo_merge import _parse_optimisation_set, write_optimisation_set

CENTERED_COLS = ["rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnCenteredCoordinateZAngst"]


def write_extracted_optset(out_run_dir: Path, tomograms_star: Path) -> Path:
    """Write the ``optimisation_set.star`` that points at a COMPLETED extraction's
    ``particles.star`` (in ``out_run_dir``, produced by relion_tomo_subtomo) + the
    shared ``tomograms.star``. This is the per-list extraction's canonical output —
    recorded as ``PickList.extracted_path`` and (later) forwarded downstream. Returns
    the optset path."""
    out_run_dir = Path(out_run_dir)
    optset = out_run_dir / "optimisation_set.star"
    write_optimisation_set(optset, particles_star=out_run_dir / "particles.star", tomograms_star=Path(tomograms_star))
    return optset


def _find_particles_block(star_dict: dict) -> str | None:
    """Key of the block carrying per-particle rows (``rlnTomoName`` + a centered
    coord), preferring one that also has coords; never the data_optics block
    (optics groups have no ``rlnTomoName``)."""

    def _is_particles(v) -> bool:
        return isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns

    for key, val in star_dict.items():
        if _is_particles(val) and any(c in val.columns for c in CENTERED_COLS):
            return key
    for key, val in star_dict.items():
        if _is_particles(val):
            return key
    return None


def _default_for(series: pd.Series):
    """Dtype-appropriate fill for a synthesised row: 0 for numeric columns
    (incl. zeroed orientations), '' for strings."""
    return 0 if pd.api.types.is_numeric_dtype(series) else ""


def _coords_for_tomo(list_star: Path, tomo_name: str) -> pd.DataFrame:
    """The pick list's rows for ONE tomogram, validated to carry all three centered
    coordinate columns. Raises when the list has no picks there — an empty extraction
    is a user error worth surfacing, not an empty output."""
    lst = starfile.read(list_star, always_dict=True)
    lkey = _find_particles_block(lst)
    if lkey is None:
        raise ValueError(f"No coord block (rlnTomoName + centered coords) in list star: {list_star}")
    coords = lst[lkey]
    if "rlnTomoName" in coords.columns:
        coords = coords[coords["rlnTomoName"].astype(str) == str(tomo_name)].reset_index(drop=True)
    if len(coords) == 0:
        raise ValueError(f"List {list_star} has no picks for tomogram {tomo_name}")
    missing = [c for c in CENTERED_COLS if c not in coords.columns]
    if missing:
        raise ValueError(f"List star {list_star} missing coord columns {missing}")
    return coords


def _synthesize_particle_rows(coords: pd.DataFrame, tomo_name: str, columns: list[str], fill, optics_group):
    """One particle row per list coordinate, in ``columns`` order.

    ``fill(col)`` supplies each column's non-coordinate value — dtype-derived from a
    candidate template on the mirroring path, a declared constant on the candidate-free
    one. Coordinates, tomo name, optics group and particle name are then overwritten
    from the list; any centered column the schema lacked is appended.
    """
    n = len(coords)
    rows = pd.DataFrame(index=range(n))
    for col in columns:
        rows[col] = fill(col)
    rows["rlnTomoName"] = str(tomo_name)
    for c in CENTERED_COLS:
        rows[c] = coords[c].to_numpy()
    if "rlnOpticsGroup" in rows.columns:
        rows["rlnOpticsGroup"] = optics_group
    if "rlnTomoParticleName" in rows.columns:
        rows["rlnTomoParticleName"] = [f"{tomo_name}/{i + 1}" for i in range(n)]
    return rows[list(columns) + [c for c in CENTERED_COLS if c not in columns]]


def _write_list_optset(out_dir: Path, blocks: dict, tomograms_star: Path, count: int) -> dict:
    """Write ``particles.star`` (all blocks) + the ``optimisation_set.star`` pointing at
    it and at ``tomograms_star``. Returns the descriptor both builders hand back."""
    out_dir.mkdir(parents=True, exist_ok=True)
    particles_out = out_dir / "particles.star"
    starfile.write(blocks, particles_out, overwrite=True)
    optset_out = out_dir / "optimisation_set.star"
    write_optimisation_set(optset_out, particles_star=particles_out, tomograms_star=tomograms_star)
    return {
        "optimisation_set": str(optset_out),
        "particles": str(particles_out),
        "tomograms": str(tomograms_star),
        "count": int(count),
    }


def build_list_optset(candidate_optset: Path, list_star: Path, tomo_name: str, out_dir: Path) -> dict:
    """Mirror the candidate particles schema with ``list_star``'s coordinates for
    ``tomo_name`` and write a per-list ``optimisation_set.star`` (+ ``particles.star``)
    into ``out_dir``. Returns {optimisation_set, particles, tomograms, count}.

    Raises on unreadable candidate optset/particles, a list with no coords for the
    tomo, or missing coord columns."""
    candidate_optset, list_star, out_dir = Path(candidate_optset), Path(list_star), Path(out_dir)

    cand_particles_path, tomograms_path = _parse_optimisation_set(candidate_optset)
    cand = starfile.read(cand_particles_path, always_dict=True)
    pkey = _find_particles_block(cand)
    if pkey is None:
        raise ValueError(f"No particle block (rlnTomoName + coords) in candidate particles: {cand_particles_path}")
    template = cand[pkey]

    coords = _coords_for_tomo(list_star, tomo_name)

    # Optics group for the synthesised rows: the value the candidate uses for THIS
    # tomo (its first row), else the candidate's first row, else 1.
    optics_group = 1
    if "rlnOpticsGroup" in template.columns and len(template):
        this_tomo = template[template["rlnTomoName"].astype(str) == str(tomo_name)]
        src = this_tomo if len(this_tomo) else template
        optics_group = src["rlnOpticsGroup"].iloc[0]

    rows = _synthesize_particle_rows(
        coords, tomo_name, list(template.columns), lambda col: _default_for(template[col]), optics_group
    )

    # Keep every non-particle block (e.g. data_optics) verbatim; swap the particle block.
    out = dict(cand)
    out[pkey] = rows
    return _write_list_optset(out_dir, out, tomograms_path, len(coords))


# ── Candidate-free path (de-novo species: nothing to mirror) ────────────────────

# The particle schema relion_tomo_subtomo consumes: identity, position, orientation,
# optics group. Manual picks are positions only, so the three Euler angles are written
# as explicit zeros — "unoriented" IS zero here, it is not a stand-in for a value we
# failed to look up. Deliberately omits the PyTOM score columns a mirrored
# candidates.star carries: the tool never reads them and inventing scores would be a lie.
SYNTHESIZED_PARTICLE_COLS = [
    "rlnTomoName",
    "rlnTomoParticleName",
    "rlnCenteredCoordinateXAngst",
    "rlnCenteredCoordinateYAngst",
    "rlnCenteredCoordinateZAngst",
    "rlnAngleRot",
    "rlnAngleTilt",
    "rlnAnglePsi",
    "rlnOpticsGroup",
]

# Physical optics a synthesized data_optics block must carry, sourced from the
# tomograms.star global block. Both writers that can produce that star emit all four:
# the pipeline import (services/scheduling_and_orchestration/pipeline_orchestrator_service
# ._write_import_stars_inline, carried forward by every adapter) and the tomogram import
# (services/tomogram_import.py). A missing one raises rather than defaulting.
REQUIRED_OPTICS_COLS = ["rlnVoltage", "rlnSphericalAberration", "rlnAmplitudeContrast", "rlnTomoTiltSeriesPixelSize"]

_SYNTHESIZED_OPTICS_GROUP = 1


def _tomogram_row(tomograms_star: Path, tomo_name: str) -> tuple[pd.DataFrame, pd.Series]:
    """(table, row) for one tomogram in a tomograms.star. Raises when the star has no
    tomogram block or does not list this tomogram."""
    data = starfile.read(tomograms_star, always_dict=True)
    table = next((b for b in data.values() if isinstance(b, pd.DataFrame) and "rlnTomoName" in b.columns), None)
    if table is None:
        raise ValueError(f"No tomogram block (rlnTomoName) in {tomograms_star}")
    match = table[table["rlnTomoName"].astype(str) == str(tomo_name)]
    if len(match) == 0:
        raise ValueError(f"Tomogram {tomo_name!r} is not listed in {tomograms_star}")
    return table, match.iloc[0]


def _optics_from_tomograms(tomograms_star: Path, tomo_name: str) -> pd.DataFrame:
    """A one-row ``data_optics`` block for ``tomo_name``, read off the tomograms.star.

    Every value is copied, never derived: a missing OR blank required column raises so
    the failure names the star and the column instead of extracting at a wrong scale.
    ``rlnOpticsGroup`` is ours to assign (it is the join key to the particle rows we
    synthesize), and ``rlnOpticsGroupName`` rides along when the star carries it.
    """
    table, row = _tomogram_row(tomograms_star, tomo_name)
    missing = [c for c in REQUIRED_OPTICS_COLS if c not in table.columns]
    if missing:
        raise ValueError(
            f"{tomograms_star} is missing optics column(s) {missing} — cannot synthesize a data_optics "
            f"block for {tomo_name!r} without inventing values."
        )
    blank = [c for c in REQUIRED_OPTICS_COLS if pd.isna(row[c])]
    if blank:
        raise ValueError(f"{tomograms_star} has no value for {blank} on tomogram {tomo_name!r}.")

    optics: dict = {"rlnOpticsGroup": _SYNTHESIZED_OPTICS_GROUP}
    for col in REQUIRED_OPTICS_COLS:
        optics[col] = row[col]
    if "rlnOpticsGroupName" in table.columns and not pd.isna(row["rlnOpticsGroupName"]):
        optics["rlnOpticsGroupName"] = row["rlnOpticsGroupName"]
    return pd.DataFrame([optics])


def build_list_optset_from_tomograms(list_star: Path, tomo_name: str, out_dir: Path, tomograms_star: Path) -> dict:
    """Per-list ``optimisation_set.star`` for a species with NO candidate-extract job.

    Same contract as ``build_list_optset`` — returns
    {optimisation_set, particles, tomograms, count} — but the particle schema is
    ``SYNTHESIZED_PARTICLE_COLS`` and the optics block is read off ``tomograms_star``
    instead of mirrored from a candidates.star. Raises on an unreadable list star, a
    list with no picks for the tomo, a tomogram absent from the star, or any missing
    required optics column.
    """
    list_star, out_dir, tomograms_star = Path(list_star), Path(out_dir), Path(tomograms_star)

    coords = _coords_for_tomo(list_star, tomo_name)
    optics = _optics_from_tomograms(tomograms_star, tomo_name)
    rows = _synthesize_particle_rows(
        coords,
        tomo_name,
        list(SYNTHESIZED_PARTICLE_COLS),
        # Every non-coordinate column in this schema is numeric-and-zero
        # (the Euler angles) or overwritten below by _synthesize_particle_rows.
        lambda col: "" if col in ("rlnTomoName", "rlnTomoParticleName") else 0,
        _SYNTHESIZED_OPTICS_GROUP,
    )
    return _write_list_optset(out_dir, {"optics": optics, "particles": rows}, tomograms_star, len(coords))


def _main() -> None:
    ap = argparse.ArgumentParser(
        description="Build a per-pick-list optimisation_set for relion_tomo_subtomo (no extraction is run)."
    )
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--candidate-optset", help="candidate-extract optimisation_set.star OR its job dir")
    src.add_argument("--tomograms-star", help="tomograms.star to synthesize optics from (no candidate-extract job)")
    ap.add_argument("--list-star", required=True, help="the pick list's coords star (rlnTomoName + centered coords)")
    ap.add_argument("--tomo", required=True, help="rlnTomoName to extract")
    ap.add_argument("--out-dir", required=True, help="output dir, e.g. Curation/<species>/<tomo>/<slug>/")
    args = ap.parse_args()

    if args.tomograms_star:
        res = build_list_optset_from_tomograms(
            Path(args.list_star), args.tomo, Path(args.out_dir), Path(args.tomograms_star)
        )
    else:
        cand = Path(args.candidate_optset)
        if cand.is_dir():
            cand = cand / "optimisation_set.star"
        res = build_list_optset(cand, Path(args.list_star), args.tomo, Path(args.out_dir))
    print(f"Wrote {res['count']} particles")
    print(f"  particles:        {res['particles']}")
    print(f"  optimisation_set: {res['optimisation_set']}")
    print(f"  tomograms (ref):  {res['tomograms']}")


if __name__ == "__main__":
    _main()

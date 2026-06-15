"""Per-pick-list subtomo-extraction prep.

A pick list's star (manual / imported / merged) carries only ``rlnTomoName`` +
the three ``rlnCenteredCoordinate*Angst`` columns. ``relion_tomo_subtomo`` (the
extraction driver) instead consumes an ``optimisation_set.star`` → a full
``particles.star`` (it reads ``rlnOpticsGroup`` and slices by ``rlnTomoName``) +
a ``tomograms.star``.

So to extract ONE list we build a per-list ``optimisation_set`` by MIRRORING the
candidate-extract ``candidates.star`` (the known-good extraction input for this
species): reuse its exact column set + any non-particle blocks (e.g. data_optics)
verbatim, and synthesise one particle row per list coordinate — orientations
defaulted to 0, since manual picks are positions only. The output lands in the
list's OWN dir so lists extract independently and never overwrite each other.

Output (under ``Curation/<species>/<tomo>/<slug>/``):
  - ``particles.star``        — candidates.star schema, the list's coordinates
  - ``optimisation_set.star`` — points at the above + the shared tomograms.star

NOTE: like ``pick_merge.py``, this is py_compile + ruff only in Claude's venv
(no numpy/pandas there); it is runtime-tested by the user in the module env. The
CLI below builds the artifacts WITHOUT running extraction so the star format can
be eyeballed before a SLURM job is ever submitted.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import starfile

CENTERED_COLS = ["rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnCenteredCoordinateZAngst"]


def _parse_optimisation_set(opt_path: Path) -> Tuple[Path, Path]:
    """(particles_star, tomograms_star) from an optimisation_set.star, resolving
    relative paths against the optset's dir. Mirrors drivers/subtomo_merge.py so
    we read the candidate optset the same way the driver does."""
    base = opt_path.parent

    def _resolve(p: str) -> Path:
        pp = Path(str(p).strip())
        return pp if pp.is_absolute() else (base / pp)

    try:
        data = starfile.read(opt_path, always_dict=True)
        for block in data.values():
            if isinstance(block, pd.DataFrame) and {"rlnTomoParticlesFile", "rlnTomoTomogramsFile"}.issubset(
                block.columns
            ):
                return _resolve(block["rlnTomoParticlesFile"].iloc[0]), _resolve(block["rlnTomoTomogramsFile"].iloc[0])
            if isinstance(block, dict) and "rlnTomoParticlesFile" in block and "rlnTomoTomogramsFile" in block:
                return _resolve(block["rlnTomoParticlesFile"]), _resolve(block["rlnTomoTomogramsFile"])
    except Exception:
        pass

    particles = tomograms = None
    for raw in opt_path.read_text().splitlines():
        line = raw.strip()
        if line.startswith("_rlnTomoParticlesFile"):
            bits = line.split(None, 1)
            particles = bits[1].strip() if len(bits) == 2 else None
        elif line.startswith("_rlnTomoTomogramsFile"):
            bits = line.split(None, 1)
            tomograms = bits[1].strip() if len(bits) == 2 else None
    if not particles or not tomograms:
        raise ValueError(f"Cannot parse optimisation_set.star (missing particles/tomograms keys): {opt_path}")
    return _resolve(particles), _resolve(tomograms)


def _write_optimisation_set(path: Path, *, particles_star: Path, tomograms_star: Path) -> None:
    """RELION key-value optimisation_set.star with absolute paths (mirrors
    drivers/subtomo_merge.write_optimisation_set — kept here to avoid a
    services→drivers import)."""
    path.write_text(
        "\n".join(
            [
                "# version 50001",
                "",
                "data_",
                "",
                f"_rlnTomoParticlesFile            {particles_star.resolve()}",
                f"_rlnTomoTomogramsFile            {tomograms_star.resolve()}",
                "",
            ]
        )
    )


def write_extracted_optset(out_run_dir: Path, tomograms_star: Path) -> Path:
    """Write the ``optimisation_set.star`` that points at a COMPLETED extraction's
    ``particles.star`` (in ``out_run_dir``, produced by relion_tomo_subtomo) + the
    shared ``tomograms.star``. This is the per-list extraction's canonical output —
    recorded as ``PickList.extracted_path`` and (later) forwarded downstream. Returns
    the optset path. Mirrors ``_write_optimisation_set`` (key-value, absolute paths)."""
    out_run_dir = Path(out_run_dir)
    optset = out_run_dir / "optimisation_set.star"
    _write_optimisation_set(optset, particles_star=out_run_dir / "particles.star", tomograms_star=Path(tomograms_star))
    return optset


def _find_particles_block(star_dict: dict) -> Optional[str]:
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

    lst = starfile.read(list_star, always_dict=True)
    lkey = _find_particles_block(lst)
    if lkey is None:
        raise ValueError(f"No coord block (rlnTomoName + centered coords) in list star: {list_star}")
    coords = lst[lkey]
    if "rlnTomoName" in coords.columns:
        coords = coords[coords["rlnTomoName"].astype(str) == str(tomo_name)].reset_index(drop=True)
    n = len(coords)
    if n == 0:
        raise ValueError(f"List {list_star} has no picks for tomogram {tomo_name}")
    missing = [c for c in CENTERED_COLS if c not in coords.columns]
    if missing:
        raise ValueError(f"List star {list_star} missing coord columns {missing}")

    # Optics group for the synthesised rows: the value the candidate uses for THIS
    # tomo (its first row), else the candidate's first row, else 1.
    optics_group = 1
    if "rlnOpticsGroup" in template.columns and len(template):
        this_tomo = template[template["rlnTomoName"].astype(str) == str(tomo_name)]
        src = this_tomo if len(this_tomo) else template
        optics_group = src["rlnOpticsGroup"].iloc[0]

    rows = pd.DataFrame(index=range(n))
    for col in template.columns:
        rows[col] = _default_for(template[col])
    rows["rlnTomoName"] = str(tomo_name)
    for c in CENTERED_COLS:
        rows[c] = coords[c].to_numpy()
    if "rlnOpticsGroup" in rows.columns:
        rows["rlnOpticsGroup"] = optics_group
    if "rlnTomoParticleName" in rows.columns:
        rows["rlnTomoParticleName"] = [f"{tomo_name}/{i + 1}" for i in range(n)]

    # Column order = template's, with any centered cols the template lacked appended.
    rows = rows[list(template.columns) + [c for c in CENTERED_COLS if c not in template.columns]]

    # Keep every non-particle block (e.g. data_optics) verbatim; swap the particle block.
    out = dict(cand)
    out[pkey] = rows

    out_dir.mkdir(parents=True, exist_ok=True)
    particles_out = out_dir / "particles.star"
    starfile.write(out, particles_out, overwrite=True)
    optset_out = out_dir / "optimisation_set.star"
    _write_optimisation_set(optset_out, particles_star=particles_out, tomograms_star=tomograms_path)

    return {
        "optimisation_set": str(optset_out),
        "particles": str(particles_out),
        "tomograms": str(tomograms_path),
        "count": int(n),
    }


def _main() -> None:
    ap = argparse.ArgumentParser(
        description="Build a per-pick-list optimisation_set for relion_tomo_subtomo (no extraction is run)."
    )
    ap.add_argument("--candidate-optset", required=True, help="candidate-extract optimisation_set.star OR its job dir")
    ap.add_argument("--list-star", required=True, help="the pick list's coords star (rlnTomoName + centered coords)")
    ap.add_argument("--tomo", required=True, help="rlnTomoName to extract")
    ap.add_argument("--out-dir", required=True, help="output dir, e.g. Curation/<species>/<tomo>/<slug>/")
    args = ap.parse_args()

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

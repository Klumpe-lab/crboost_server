"""Bridge between crboost picks and ChimeraX/ArtiaX ``.coords`` files.

ArtiaX ``.coords`` format (decoded from a real ArtiaX 0.6 save, 2026-06-02):
    - Plain text, one particle per line, three whitespace-separated floats: ``X Y Z``.
    - No header, no orientation columns (positions only — matches our positions-only design).
    - Values are PHYSICAL ÅNGSTRÖM from the volume CORNER origin, i.e. ``voxel_index * pixel_size``,
      using the tomogram's pixel size (ArtiaX reads it from the MRC header — confirmed 6.2 Å). A
      mid-slice click on a 512-voxel-thick tomogram saved ``Z = 1584.1 Å = 255.5 * 6.2``.

Mapping to our RELION-5 centered-Å convention (see :mod:`services.visualization.coords`)::

    voxel               = artiax_corner_angst / pixel_size
    centered_angst      = (voxel - N / 2) * pixel_size          # = artiax_corner_angst - (N/2)*pixel_size
    artiax_corner_angst = voxel * pixel_size,  voxel = centered_angst / pixel_size + N / 2

Because we own both directions and exchange in ArtiaX's corner-Å (= voxel*px) space, the ``N/2``
centering term cancels in any our→ArtiaX→our round trip regardless of the half-voxel parity
ambiguity. See ``services/visualization/ARTIAX_BRIDGE_PLAN.md``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import starfile

from services.visualization.coords import TomoFrame, picks_centered_angst

logger = logging.getLogger(__name__)


def read_coords_file(path: Path) -> np.ndarray:
    """Read an ArtiaX ``.coords`` file → ``(N, 3)`` array of corner-Å ``X Y Z``.

    Lenient: skips blank/comment lines and any line without three float-parseable tokens.
    """
    rows = []
    for line in Path(path).read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        toks = s.split()
        try:
            rows.append([float(toks[0]), float(toks[1]), float(toks[2])])
        except (IndexError, ValueError):
            continue
    return np.asarray(rows, dtype=float).reshape(-1, 3)


def write_coords_file(coords_corner_angst: np.ndarray, path: Path) -> None:
    """Write ``(N, 3)`` corner-Å coordinates as an ArtiaX ``.coords`` file (``X Y Z``, no header)."""
    arr = np.asarray(coords_corner_angst, dtype=float).reshape(-1, 3)
    np.savetxt(str(path), arr, fmt="%.4f", delimiter=" ")


def artiax_to_centered_angst(coords_corner_angst: np.ndarray, frame: TomoFrame) -> np.ndarray:
    """ArtiaX corner-Å → RELION-5 centered-Å for one tomogram."""
    voxel = np.asarray(coords_corner_angst, dtype=float).reshape(-1, 3) / frame.pixel_size
    return frame.to_centered_angst(voxel)


def centered_angst_to_artiax(centered_angst: np.ndarray, frame: TomoFrame) -> np.ndarray:
    """RELION-5 centered-Å → ArtiaX corner-Å for one tomogram."""
    voxel = frame.to_voxel(np.asarray(centered_angst, dtype=float).reshape(-1, 3))
    return voxel * frame.pixel_size


def _find_table(star_path: Path, required_col: str) -> pd.DataFrame:
    d = starfile.read(star_path, always_dict=True)
    for v in d.values():
        if isinstance(v, pd.DataFrame) and required_col in v.columns:
            return v
    raise ValueError(f"No table with column {required_col!r} in {star_path}")


def frame_for_tomo(tomograms_star: Path, tomo_name: str, project_root: Optional[Path] = None) -> TomoFrame:
    tomo_df = _find_table(Path(tomograms_star), "rlnTomoName")
    rows = tomo_df[tomo_df["rlnTomoName"] == tomo_name]
    if rows.empty:
        raise ValueError(f"Tomogram {tomo_name!r} not found in {tomograms_star}")
    return TomoFrame.from_tomo_row(rows.iloc[0], project_root=project_root)


def export_tomo_picks_to_coords(
    candidates_star: Path, tomograms_star: Path, tomo_name: str, out_path: Path, project_root: Optional[Path] = None
) -> int:
    """Write our picks for one tomogram to an ArtiaX ``.coords`` file. Returns the pick count."""
    parts = _find_table(Path(candidates_star), "rlnTomoName")
    parts = parts[parts["rlnTomoName"] == tomo_name]
    frame = frame_for_tomo(tomograms_star, tomo_name, project_root=project_root)
    coords = centered_angst_to_artiax(picks_centered_angst(parts), frame)
    write_coords_file(coords, Path(out_path))
    logger.info("Wrote %d picks for %s -> %s", len(coords), tomo_name, out_path)
    return len(coords)


def _cli(argv=None) -> int:
    ap = argparse.ArgumentParser(description="crboost <-> ArtiaX .coords bridge")
    sub = ap.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("export", help="write our picks for a tomogram as an ArtiaX .coords file")
    e.add_argument("--candidates", required=True, type=Path)
    e.add_argument("--tomograms", required=True, type=Path)
    e.add_argument("--tomo", required=True)
    e.add_argument("--out", required=True, type=Path)
    e.add_argument("--project-root", type=Path, default=None)

    r = sub.add_parser("selftest", help="pure round-trip check (no ArtiaX): centered->coords->centered")
    r.add_argument("--candidates", required=True, type=Path)
    r.add_argument("--tomograms", required=True, type=Path)
    r.add_argument("--tomo", required=True)
    r.add_argument("--project-root", type=Path, default=None)

    args = ap.parse_args(argv)

    if args.cmd == "export":
        n = export_tomo_picks_to_coords(args.candidates, args.tomograms, args.tomo, args.out, args.project_root)
        print(f"wrote {n} picks -> {args.out}")
        return 0

    parts = _find_table(Path(args.candidates), "rlnTomoName")
    parts = parts[parts["rlnTomoName"] == args.tomo]
    frame = frame_for_tomo(args.tomograms, args.tomo, project_root=args.project_root)
    centered = picks_centered_angst(parts)
    back = artiax_to_centered_angst(centered_angst_to_artiax(centered, frame), frame)
    max_err = float(np.max(np.abs(centered - back))) if len(centered) else 0.0
    print(f"tomo={args.tomo} n={len(centered)} pixel_size={frame.pixel_size} size={frame.size.tolist()}")
    print(f"max round-trip error (A): {max_err:.3e}")
    return 0 if max_err < 1e-6 else 1


if __name__ == "__main__":
    raise SystemExit(_cli())

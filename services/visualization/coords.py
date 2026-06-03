"""Canonical coordinate transforms between RELION-5 centered-Ångström picks and
tomogram voxel space.

Single source of truth for the centered-Å ↔ voxel mapping. Used by the gallery
visualization (``imod_vis``) and by the ArtiaX bridge so both share one definition
of the convention. See ``services/visualization/ARTIAX_BRIDGE_PLAN.md``.

Convention (RELION-5)::

    pixel_size      = rlnTomoTiltSeriesPixelSize * rlnTomoTomogramBinning   # binned recon Å/px
    N               = binned tomogram dims, read from the reconstruction MRC header
    voxel           = centered_angst / pixel_size + N / 2
    centered_angst  = (voxel - N / 2) * pixel_size

Applying the same ``N / 2`` term in both directions makes the round trip exact
regardless of RELION's half-voxel parity ambiguity (3dem/relion#1280): it cancels.
The only external inputs are ``N`` (deterministic from the MRC header) and
``pixel_size`` (deterministic from the star metadata).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

CENTERED_COLS = ["rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnCenteredCoordinateZAngst"]


def pixel_size_from_tomo_row(tomo_row: pd.Series) -> float:
    """Binned reconstruction pixel size (Å/px) = tilt-series pixel size × tomogram binning."""
    ts_pixs = float(tomo_row["rlnTomoTiltSeriesPixelSize"])
    binning = float(tomo_row.get("rlnTomoTomogramBinning", 1))
    return ts_pixs * binning


def binned_tomo_size_from_tomo_row(tomo_row: pd.Series, project_root: Optional[Path] = None) -> np.ndarray:
    """Binned (nx, ny, nz) for a tomogram.

    Reads the dimensions from the reconstruction MRC header (the authoritative
    source — ``rlnTomoSize*`` is unbinned). Falls back to rounded
    ``unbinned / binning`` only when no reconstruction path is recorded.
    """
    import mrcfile

    mrc_col = "rlnTomoReconstructedTomogram"
    if mrc_col not in tomo_row.index:
        unbinned = np.array(
            [float(tomo_row["rlnTomoSizeX"]), float(tomo_row["rlnTomoSizeY"]), float(tomo_row["rlnTomoSizeZ"])]
        )
        binning = float(tomo_row.get("rlnTomoTomogramBinning", 1.0))
        return np.round(unbinned / binning).astype(int)

    mrc_path = Path(tomo_row[mrc_col])
    if not mrc_path.is_absolute() and project_root is not None:
        mrc_path = project_root / mrc_path

    if not mrc_path.exists():
        raise FileNotFoundError(
            f"Reconstructed tomogram not found: {mrc_path}. "
            "Cannot determine actual dimensions for coordinate transform."
        )
    with mrcfile.open(str(mrc_path), header_only=True, mode="r") as m:
        return np.array([int(m.header.nx), int(m.header.ny), int(m.header.nz)])


def centered_angst_to_voxel(coords_angst: np.ndarray, tomo_size: np.ndarray, pixel_size: float) -> np.ndarray:
    """RELION-5 centered Ångström → tomogram voxel coordinates (float, no rounding)."""
    coords_angst = np.asarray(coords_angst, dtype=float)
    return coords_angst / pixel_size + np.asarray(tomo_size, dtype=float) / 2.0


def voxel_to_centered_angst(voxel: np.ndarray, tomo_size: np.ndarray, pixel_size: float) -> np.ndarray:
    """Tomogram voxel coordinates → RELION-5 centered Ångström. Exact inverse of ``centered_angst_to_voxel``."""
    voxel = np.asarray(voxel, dtype=float)
    return (voxel - np.asarray(tomo_size, dtype=float) / 2.0) * pixel_size


def picks_centered_angst(particles: pd.DataFrame) -> np.ndarray:
    """Extract the N×3 centered-Ångström coordinate array from a particles/candidates table."""
    missing = [c for c in CENTERED_COLS if c not in particles.columns]
    if missing:
        raise ValueError(f"Particle table missing centered-coordinate columns: {missing}")
    return particles[CENTERED_COLS].to_numpy(dtype=float)


def centered_angst_dataframe(coords_angst: np.ndarray) -> pd.DataFrame:
    """Build a DataFrame with the three RELION-5 centered-coordinate columns from an N×3 array."""
    arr = np.asarray(coords_angst, dtype=float).reshape(-1, 3)
    return pd.DataFrame({CENTERED_COLS[i]: arr[:, i] for i in range(3)})


@dataclass(frozen=True)
class TomoFrame:
    """Everything needed to map one tomogram's picks ↔ voxel space.

    Build with :meth:`from_tomo_row` from a row of ``tomograms.star``; then
    ``to_voxel`` / ``to_centered_angst`` apply the canonical transform.
    """

    tomo_name: str
    pixel_size: float
    size: np.ndarray  # binned (nx, ny, nz)
    recon_path: Optional[Path] = None

    @classmethod
    def from_tomo_row(cls, tomo_row: pd.Series, project_root: Optional[Path] = None) -> "TomoFrame":
        recon = tomo_row.get("rlnTomoReconstructedTomogram")
        recon_path = Path(recon) if isinstance(recon, str) and recon else None
        if recon_path is not None and not recon_path.is_absolute() and project_root is not None:
            recon_path = project_root / recon_path
        return cls(
            tomo_name=str(tomo_row["rlnTomoName"]),
            pixel_size=pixel_size_from_tomo_row(tomo_row),
            size=binned_tomo_size_from_tomo_row(tomo_row, project_root=project_root),
            recon_path=recon_path,
        )

    def to_voxel(self, coords_angst: np.ndarray) -> np.ndarray:
        return centered_angst_to_voxel(coords_angst, self.size, self.pixel_size)

    def to_centered_angst(self, voxel: np.ndarray) -> np.ndarray:
        return voxel_to_centered_angst(voxel, self.size, self.pixel_size)

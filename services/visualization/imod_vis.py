"""
Visualization utilities for candidate extraction results using IMOD.

Generates IMOD models (.mod) for particle visualization and Warp-compatible
coordinate files from RELION5/pytom candidate star files.

Usage:
    from services.visualization.imod_vis import generate_candidate_vis, view_volume

    generate_candidate_vis(
        candidates_star=Path("External/job012/candidates.star"),
        tomograms_star=Path("External/job012/tomograms.star"),
        particle_diameter_ang=300.0,
        output_dir=Path("External/job012"),
    )
"""

import subprocess
import os
import numpy as np
import starfile
import pandas as pd
from pathlib import Path
from typing import Callable, Optional, Tuple


def _read_tomogram_info(tomograms_star: Path) -> pd.DataFrame:
    data = starfile.read(tomograms_star, always_dict=True)
    for key, val in data.items():
        if isinstance(val, pd.DataFrame) and "rlnTomoName" in val.columns:
            return val
    raise ValueError(f"No tomogram table with rlnTomoName found in {tomograms_star}")


def _read_particles(candidates_star: Path) -> pd.DataFrame:
    data = starfile.read(candidates_star, always_dict=True)
    for key, val in data.items():
        if isinstance(val, pd.DataFrame) and "rlnTomoName" in val.columns:
            return val
    raise ValueError(f"No particle table found in {candidates_star}")


def _get_pixel_size(tomo_row: pd.Series) -> float:
    ts_pixs = float(tomo_row["rlnTomoTiltSeriesPixelSize"])
    binning = float(tomo_row.get("rlnTomoTomogramBinning", 1))
    return ts_pixs * binning



def _get_unbinned_tomo_size(tomo_row: pd.Series) -> np.ndarray:
    """Return the unbinned tomogram dimensions from STAR metadata."""
    return np.array([
        float(tomo_row["rlnTomoSizeX"]),
        float(tomo_row["rlnTomoSizeY"]),
        float(tomo_row["rlnTomoSizeZ"]),
    ])


def _get_binned_tomo_size(tomo_row: pd.Series) -> np.ndarray:
    """Read actual tomogram dimensions from the reconstructed MRC file header."""
    import mrcfile

    mrc_col = "rlnTomoReconstructedTomogram"
    if mrc_col not in tomo_row.index:
        # Fallback: compute from STAR metadata
        unbinned = np.array([
            float(tomo_row["rlnTomoSizeX"]),
            float(tomo_row["rlnTomoSizeY"]),
            float(tomo_row["rlnTomoSizeZ"]),
        ])
        binning = float(tomo_row.get("rlnTomoTomogramBinning", 1.0))
        return np.round(unbinned / binning).astype(int)

    mrc_path = Path(tomo_row[mrc_col])
    if not mrc_path.exists():
        raise FileNotFoundError(
            f"Reconstructed tomogram not found: {mrc_path}. "
            f"Cannot determine actual dimensions for coordinate transform."
        )
    with mrcfile.open(str(mrc_path), header_only=True, mode='r') as m:
        return np.array([int(m.header.nx), int(m.header.ny), int(m.header.nz)])


def _centered_angst_to_imod_px(
    coords_angst: np.ndarray, tomo_size: np.ndarray, pixel_size: float
) -> np.ndarray:
    return np.int32(coords_angst / pixel_size + tomo_size / 2)


def _get_imod_coords(particles: pd.DataFrame, tomo_size: np.ndarray, pixel_size: float) -> np.ndarray:
    centered_cols = [
        "rlnCenteredCoordinateXAngst",
        "rlnCenteredCoordinateYAngst",
        "rlnCenteredCoordinateZAngst",
    ]
    abs_cols = ["rlnCoordinateX", "rlnCoordinateY", "rlnCoordinateZ"]

    if all(c in particles.columns for c in centered_cols):
        coords = particles[centered_cols].values.astype(float)
        return _centered_angst_to_imod_px(coords, tomo_size, pixel_size)
    elif all(c in particles.columns for c in abs_cols):
        return np.int32(particles[abs_cols].values.astype(float))
    else:
        raise ValueError(
            f"Particle table has neither centered angstrom nor absolute pixel coordinates. "
            f"Columns: {list(particles.columns)}"
        )


def _default_command_runner(cmd: str, cwd: Path) -> None:
    """Run a shell command directly (assumes tools are on PATH)."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed (rc={result.returncode}): {result.stderr.strip()}")


def _write_imod_model(
    coords: np.ndarray,
    output_txt: Path,
    output_mod: Path,
    radius_px: int,
    command_runner: Callable[[str, Path], None],
    color: Tuple[int, int, int] = (0, 255, 0),
    thickness: int = 2,
) -> None:
    output_txt.parent.mkdir(parents=True, exist_ok=True)
    np.savetxt(str(output_txt), coords, delimiter="\t", fmt="%.0f")

    cmd = (
        f"point2model {output_txt} {output_mod} "
        f"-sphere {radius_px} -scat "
        f"-color {color[0]},{color[1]},{color[2]} -thick {thickness}"
    )
    try:
        command_runner(cmd, output_txt.parent)
        print(f"[VIS] Created {output_mod}")
    except RuntimeError as e:
        print(f"[VIS WARN] point2model failed for {output_mod}: {e}")


def _write_warp_coords(
    particles: pd.DataFrame,
    tomo_name: str,
    tomo_size: np.ndarray,
    pixel_size: float,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    coords = _get_imod_coords(particles, tomo_size, pixel_size)
    angle_cols = ["rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi"]

    df = pd.DataFrame()
    df["rlnCoordinateX"] = coords[:, 0]
    df["rlnCoordinateY"] = coords[:, 1]
    df["rlnCoordinateZ"] = coords[:, 2]

    for col in angle_cols:
        if col in particles.columns:
            df[col] = particles[col].values

    df["rlnMicrographName"] = f"{tomo_name}.tomostar"

    score_col = next(
        (c for c in ["rlnLCCmax", "rlnMaxValueProbDistribution"] if c in particles.columns),
        None,
    )
    if score_col:
        df["rlnAutopickFigureOfMerit"] = particles[score_col].values

    starfile.write(df, output_dir / f"{tomo_name}.star", overwrite=True)
    print(f"[VIS] Wrote warp coords: {output_dir / tomo_name}.star")


def generate_candidate_vis(
    candidates_star: Path,
    tomograms_star: Path,
    particle_diameter_ang: float,
    output_dir: Path,
    command_runner: Optional[Callable[[str, Path], None]] = None,
) -> None:
    """
    Generate IMOD visualization models and Warp-compatible coordinates.

    Creates:
        output_dir/vis/imodPartRad/  -- particle radius spheres (green)
        output_dir/vis/imodCenter/   -- center markers (red, small)
        output_dir/candidatesWarp/   -- Warp-compatible star files

    Args:
        candidates_star: Path to candidates.star
        tomograms_star: Path to tomograms.star
        particle_diameter_ang: Particle diameter in Angstroms
        output_dir: Base output directory (typically the job dir)
        command_runner: Callable(cmd_string, cwd) to execute shell commands.
                        Defaults to bare subprocess.run. Pass a container-wrapped
                        version for execution via apptainer.
    """
    if command_runner is None:
        command_runner = _default_command_runner

    candidates_star = Path(candidates_star)
    tomograms_star = Path(tomograms_star)
    output_dir = Path(output_dir)

    particles_df = _read_particles(candidates_star)
    tomo_df = _read_tomogram_info(tomograms_star)

    tomo_lookup = {row["rlnTomoName"]: row for _, row in tomo_df.iterrows()}
    tomo_names = particles_df["rlnTomoName"].unique()

    dir_part_rad = output_dir / "vis" / "imodPartRad"
    dir_center = output_dir / "vis" / "imodCenter"
    dir_warp = output_dir / "candidatesWarp"

    for tomo_name in tomo_names:
        tomo_row = tomo_lookup.get(tomo_name)
        if tomo_row is None:
            print(f"[VIS WARN] Tomogram '{tomo_name}' not found in tomograms.star, skipping")
            continue

        pixel_size = _get_pixel_size(tomo_row)
        tomo_size = _get_binned_tomo_size(tomo_row)
        tomo_particles = particles_df[particles_df["rlnTomoName"] == tomo_name]
        coords = _get_imod_coords(tomo_particles, tomo_size, pixel_size)

        # Particle radius model (green spheres)
        radius_px = int(particle_diameter_ang / (2.0 * pixel_size))
        _write_imod_model(
            coords,
            output_txt=dir_part_rad / f"coords_{tomo_name}.txt",
            output_mod=dir_part_rad / f"coords_{tomo_name}.mod",
            radius_px=radius_px,
            command_runner=command_runner,
            color=(0, 255, 0),
            thickness=2,
        )

        # Center marker model (red, small)
        # Old code: diameterInAng = 8 * pixs, then radius = diameterInAng / (pixs * 2) = 4px
        _write_imod_model(
            coords,
            output_txt=dir_center / f"coords_{tomo_name}.txt",
            output_mod=dir_center / f"coords_{tomo_name}.mod",
            radius_px=4,
            command_runner=command_runner,
            color=(255, 0, 0),
            thickness=4,
        )

        # Warp-compatible coordinates
        _write_warp_coords(tomo_particles, tomo_name, tomo_size, pixel_size, dir_warp)

    print(f"[VIS] Candidate visualization complete for {len(tomo_names)} tomogram(s)")


def view_volume(mrc_path: str, command_runner: Optional[Callable[[str, Path], None]] = None) -> None:
    """
    Launch 3dmod to view a volume.

    Args:
        mrc_path: Path to the MRC file
        command_runner: Optional command runner (for container execution).
                        If None, launches 3dmod directly via Popen (non-blocking).
    """
    mrc_path = str(mrc_path)
    if not os.path.isfile(mrc_path):
        raise FileNotFoundError(f"Volume not found: {mrc_path}")

    if command_runner is not None:
        # Container mode -- run 3dmod through container (blocking)
        command_runner(f"3dmod {mrc_path}", Path(mrc_path).parent)
    else:
        print(f"[VIS] Launching 3dmod for {mrc_path}")
        subprocess.Popen(["3dmod", mrc_path])
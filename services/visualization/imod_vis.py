"""
Visualization utilities for candidate extraction results, for IMOD's 3dmod.

Writes IMOD models (.mod) for particle visualization, in IMOD's binary format and without IMOD, and Warp-compatible
coordinate files from RELION5/pytom candidate star files.

Usage:
    from services.visualization.imod_vis import generate_candidate_vis

    generate_candidate_vis(
        candidates_star=Path("External/job012/candidates.star"),
        tomograms_star=Path("External/job012/tomograms.star"),
        particle_diameter_ang=300.0,
        output_dir=Path("External/job012"),
    )
"""

import logging
import struct
import numpy as np
import starfile
import pandas as pd
from pathlib import Path

from services.particles.coords import binned_tomo_size_from_tomo_row, centered_angst_to_voxel, pixel_size_from_tomo_row

logger = logging.getLogger(__name__)


def _read_tomogram_info(tomograms_star: Path) -> pd.DataFrame:
    data = starfile.read(tomograms_star, always_dict=True)
    for _key, val in data.items():
        if isinstance(val, pd.DataFrame) and "rlnTomoName" in val.columns:
            return val
    raise ValueError(f"No tomogram table with rlnTomoName found in {tomograms_star}")


def _read_particles(candidates_star: Path) -> pd.DataFrame:
    data = starfile.read(candidates_star, always_dict=True)
    for _key, val in data.items():
        if isinstance(val, pd.DataFrame) and "rlnTomoName" in val.columns:
            return val
    raise ValueError(f"No particle table found in {candidates_star}")


def _get_pixel_size(tomo_row: pd.Series) -> float:
    return pixel_size_from_tomo_row(tomo_row)


def _get_unbinned_tomo_size(tomo_row: pd.Series) -> np.ndarray:
    """Return the unbinned tomogram dimensions from STAR metadata."""
    return np.array([float(tomo_row["rlnTomoSizeX"]), float(tomo_row["rlnTomoSizeY"]), float(tomo_row["rlnTomoSizeZ"])])


def _get_binned_tomo_size(tomo_row: pd.Series, project_root: Path | None = None) -> np.ndarray:
    return binned_tomo_size_from_tomo_row(tomo_row, project_root=project_root)


def _centered_angst_to_imod_px(coords_angst: np.ndarray, tomo_size: np.ndarray, pixel_size: float) -> np.ndarray:
    return np.int32(centered_angst_to_voxel(coords_angst, tomo_size, pixel_size))


def _get_imod_coords(particles: pd.DataFrame, tomo_size: np.ndarray, pixel_size: float) -> np.ndarray:
    centered_cols = ["rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnCenteredCoordinateZAngst"]
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


# IMOD's binary model format (IMOD's binspec), as `point2model -sphere R -scat -color r,g,b -thick T` writes it:
# big-endian, one object holding one contour of scattered points. Checked byte for byte against IMOD 5.1.3's
# point2model (roadmap 63 §7); only the name field differs past its NUL, where point2model leaves stray memory.
_MODEL_NAME = b"IMOD-NewModel"
_MODEL_FLAGS = 0x3E00  # point2model's model flags
_OBJECT_FLAGS = 0x18000208  # open, scattered, as point2model sets them for -scat
_CONTOUR_FLAGS = 16
_MATERIAL = bytes([102, 255, 127, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0])  # IMAT: IMOD's default material


def write_point_model(
    path: Path, coords: np.ndarray, radius_px: int, color: tuple[int, int, int], thickness: int
) -> None:
    """Write `coords` (voxels, one row per point) as an IMOD model of scattered points drawn as spheres of
    `radius_px`, in `color` (0-255), with lines `thickness` wide."""
    points = np.asarray(coords).reshape(-1, 3)
    if not len(points):
        raise ValueError(f"no points for {path.name}")
    top = points.max(axis=0)
    # As point2model sizes a model made without an image: 10 past the last point in X and Y, 1 in Z.
    size = (int(top[0]) + 10, int(top[1]) + 10, int(top[2]) + 1)
    model = struct.pack(
        ">3i i I 4i 3f 3f 5i f i i 3f",
        *size,
        1,  # objects
        _MODEL_FLAGS,
        1,  # drawmode
        2,  # mousemode
        0,  # black level
        255,  # white level
        *(0.0, 0.0, 0.0),  # offsets
        *(1.0, 1.0, 1.0),  # scales
        0,  # current object
        0,  # current contour
        -1,  # current point
        3,  # resolution
        128,  # threshold
        1.0,  # pixel size
        0,  # units: pixels
        0,  # checksum
        *(0.0, 0.0, 0.0),  # alpha, beta, gamma
    )
    obj = struct.pack(
        ">i I i i 3f i 8B i i",
        1,  # contours
        _OBJECT_FLAGS,
        0,  # axis
        1,  # drawmode
        *(c / 255.0 for c in color),
        int(radius_px),  # sphere radius, the -sphere value
        1,  # symbol
        3,  # symbol size
        1,  # 2-D line width
        int(thickness),  # 3-D line width, the -thick value
        *(0, 0, 0, 0),  # line style, symbol flags, pad, transparency
        0,  # meshes
        0,  # surfaces
    )
    contour = struct.pack(">i I i i", len(points), _CONTOUR_FLAGS, 0, 0) + points.astype(">f4").tobytes()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        b"IMODV1.2"
        + _MODEL_NAME.ljust(128, b"\0")
        + model
        + b"OBJT"
        + bytes(128)  # object name and its 16 spare words
        + obj
        + b"CONT"
        + contour
        + b"IMAT"
        + struct.pack(">i", len(_MATERIAL))
        + _MATERIAL
        + b"IEOF"
    )
    logger.info("Created %s", path)


def _write_warp_coords(
    particles: pd.DataFrame, tomo_name: str, tomo_size: np.ndarray, pixel_size: float, output_dir: Path
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

    score_col = next((c for c in ["rlnLCCmax", "rlnMaxValueProbDistribution"] if c in particles.columns), None)
    if score_col:
        df["rlnAutopickFigureOfMerit"] = particles[score_col].values

    starfile.write(df, output_dir / f"{tomo_name}.star", overwrite=True)
    logger.info("Wrote warp coords: %s.star", output_dir / tomo_name)


def generate_candidate_vis(
    candidates_star: Path,
    tomograms_star: Path,
    particle_diameter_ang: float,
    output_dir: Path,
    project_root: Path | None = None,
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
    """
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
            logger.warning("Tomogram '%s' not found in tomograms.star, skipping", tomo_name)
            continue

        pixel_size = _get_pixel_size(tomo_row)
        tomo_size = _get_binned_tomo_size(tomo_row, project_root=project_root)
        tomo_particles = particles_df[particles_df["rlnTomoName"] == tomo_name]
        coords = _get_imod_coords(tomo_particles, tomo_size, pixel_size)

        # Particle radius model (green spheres)
        radius_px = int(particle_diameter_ang / (2.0 * pixel_size))
        write_point_model(dir_part_rad / f"coords_{tomo_name}.mod", coords, radius_px, (0, 255, 0), thickness=2)

        # Center marker model (red, small): 4 px radius, as in CryoBoost v1
        write_point_model(dir_center / f"coords_{tomo_name}.mod", coords, 4, (255, 0, 0), thickness=4)

        # Warp-compatible coordinates
        _write_warp_coords(tomo_particles, tomo_name, tomo_size, pixel_size, dir_warp)

    logger.info("Candidate visualization complete for %d tomogram(s)", len(tomo_names))

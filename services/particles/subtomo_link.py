"""Cross-job linkage from candidate-extract picks to subtomo-extract `.mrcs` files.

The candidate-preview dialog is anchored on a TEMPLATE_EXTRACT_PYTOM job; the
2D image stacks that show what each pick *looks like* are produced by a
downstream extraction job (Subtomo Extraction, `relion_tomo_subtomo` with 2D output,
or Warp Export, `WarpTools ts_export_particles --2d`).
There is no explicit foreign key between the two — RELION just dumps a
`particles.star` per job — so we join by tomogram name + per-particle
Cartesian Å coordinates.

The subtomo job's `particles.star` exposes:
  - rlnTomoName              (tomogram identifier)
  - rlnCenteredCoordinateXAngst / Y / Z   (centered Å coords)
  - rlnImageName              (path to the per-particle .mrcs)
  - rlnTomoVisibleFrames      (e.g. "[0,1,1,1,...]" — bit-vector of which
                                 tilt-frames in the .mrcs are usable)

We index those rows by (tomo, rounded-Å triplet) so a candidate pick (also
in centered Å) finds its `.mrcs` in O(1). Picks without a match (e.g., the
candidate set was filtered before subtomo extraction) just don't get a tile.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from services.models_base import PARTICLE_EXTRACTION_TYPES, JobType

logger = logging.getLogger(__name__)


# 0.1 Å rounding — Å coords from RELION are written with one decimal in the
# .star file, and the candidate-extract pipeline doesn't introduce drift, so
# bit-exact match would also work. Round-to-tenths is a defensive margin.
_COORD_DECIMALS = 1


def _coord_key(x: float, y: float, z: float) -> tuple[int, int, int]:
    """A position in tenths of an Å, rounded half to even: `coord_keys` of one pick."""
    s = 10**_COORD_DECIMALS
    return (round(float(x) * s), round(float(y) * s), round(float(z) * s))


def coord_keys(xyz: np.ndarray) -> np.ndarray:
    """`_coord_key` of every row of an (n, 3) array of centred Å, as (n, 3) int64."""
    return np.rint(np.asarray(xyz, dtype=np.float64) * 10**_COORD_DECIMALS).astype(np.int64)


def _parse_visible_frames(raw) -> list[int] | None:
    """Parse a `rlnTomoVisibleFrames` cell like "[0,1,1,...]" into a list of ints."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s == "nan":
        return None
    if s.startswith("["):
        s = s[1:]
    if s.endswith("]"):
        s = s[:-1]
    parts = [p.strip() for p in s.split(",") if p.strip()]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None


def _read_subtomo_particles(particles_star: Path, job_dir: Path) -> pd.DataFrame | None:
    """Load the data_particles table from a subtomo-extract job's particles.star."""
    if not particles_star.exists():
        return None
    try:
        import starfile

        data = starfile.read(particles_star, always_dict=True)
    except Exception as e:
        logger.warning("Could not read %s: %s", particles_star, e)
        return None

    parts = data.get("particles")
    if parts is None:
        # Some versions emit a single-block .star — fall back to the first DataFrame.
        for v in data.values():
            if isinstance(v, pd.DataFrame) and "rlnImageName" in v.columns:
                parts = v
                break
    if parts is None or not isinstance(parts, pd.DataFrame):
        return None
    return parts


def _job_key(path) -> tuple[str, ...]:
    """A job dir's last two parts ("External", "jobNNN"): the same job however the project root
    is spelled."""
    return Path(str(path).rstrip("/")).parts[-2:]


def serves_candidate_job(project_state, job_model, candidate_extract_job_dir: Path) -> bool:
    """Whether an extraction job may hold the cutouts of `candidate_extract_job_dir`'s picks: it
    read that job's optimisation set, or one no candidate-extract job wrote (a merged or
    filtered set, or no recorded input; the coordinate join is then the only link). One that
    read another candidate-extract job belongs to another branch of the species."""
    src = (getattr(job_model, "paths", None) or {}).get("input_optimisation")
    if not src:
        return True
    src_key = _job_key(Path(src).parent)
    if src_key == _job_key(candidate_extract_job_dir):
        return True
    candidate_keys = {
        _job_key(jm.relion_job_name)
        for jm in (project_state.jobs or {}).values()
        if getattr(jm, "job_type", None) == JobType.TEMPLATE_EXTRACT_PYTOM and getattr(jm, "relion_job_name", None)
    }
    return src_key not in candidate_keys


def _subtomo_job_dirs(project_state, project_path, candidate_extract_job_dir: Path | None = None) -> list[Path]:
    """On-disk dirs for every particle-extraction job (Subtomo Extraction, Warp Export), sorted
    by relion job name (so newer / lex-greater jobs win on later coord-collision use). With
    `candidate_extract_job_dir`, only the ones that serve it (`serves_candidate_job`)."""
    dirs: list[Path] = []
    if project_state is None or project_path is None:
        return dirs
    for instance_id, job_model in (project_state.jobs or {}).items():
        if getattr(job_model, "job_type", None) not in PARTICLE_EXTRACTION_TYPES:
            continue
        if candidate_extract_job_dir is not None and not serves_candidate_job(
            project_state, job_model, candidate_extract_job_dir
        ):
            continue
        relion_name = getattr(job_model, "relion_job_name", None)
        if not relion_name:
            relion_name = (project_state.job_path_mapping or {}).get(instance_id)
            if not relion_name:
                continue
        d = Path(project_path) / relion_name.rstrip("/")
        if d.is_dir():
            dirs.append(d)
    dirs.sort()
    return dirs


def subtomo_particles_stars(project_state, project_path, candidate_extract_job_dir: Path) -> list[Path]:
    """particles.star path for every extraction job on disk that serves
    `candidate_extract_job_dir`.

    The candidate-preview cutout atlas is a JOIN between candidate picks and
    these files, so the preview orchestrator must include them in its cache
    staleness sources — otherwise a preview generated while a subtomo job is
    still running freezes "no subtomo match" failures that never rebuild once
    the job finishes (the candidates.star it normally watches is unchanged)."""
    stars = (d / "particles.star" for d in _subtomo_job_dirs(project_state, project_path, candidate_extract_job_dir))
    return [s for s in stars if s.exists()]


def build_pick_to_mrcs_index(
    candidate_extract_job_dir: Path, project_state, project_path: Path
) -> dict[tuple[str, tuple[int, int, int]], dict]:
    """Build a {(tomo_name, coord_key): {mrcs, visible_frames, src_job_dir}} lookup.

    Walks every extraction job that serves `candidate_extract_job_dir`, reads its
    particles.star, and indexes by (tomo, coord_key). Later subtomo jobs win on key
    collision — this is a heuristic but matches the user expectation that "the most
    recent extraction is the one I care about."
    """
    index: dict[tuple[str, tuple[int, int, int]], dict] = {}
    root = Path(project_path)
    coord_cols = ("rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnCenteredCoordinateZAngst")

    for job_dir in _subtomo_job_dirs(project_state, project_path, candidate_extract_job_dir):
        df = _read_subtomo_particles(job_dir / "particles.star", job_dir)
        if df is None:
            continue
        if "rlnImageName" not in df.columns or "rlnTomoName" not in df.columns:
            continue
        if not all(c in df.columns for c in coord_cols):
            continue

        xyz = np.column_stack([pd.to_numeric(df[c], errors="coerce").to_numpy(dtype=np.float64) for c in coord_cols])
        rows = np.flatnonzero(np.isfinite(xyz).all(axis=1))  # a row without a numeric position cannot be joined
        keys = coord_keys(xyz[rows]).tolist()
        tomos = df["rlnTomoName"].astype(str).to_numpy()[rows]
        images = df["rlnImageName"].astype(str).to_numpy()[rows]
        visible = None
        if "rlnTomoVisibleFrames" in df.columns:
            visible = df["rlnTomoVisibleFrames"].astype(str).to_numpy()[rows]
        parsed: dict[str, list[int] | None] = {}  # the rows of one tomogram mostly share a visible-frames string
        for i, (tomo, key, image) in enumerate(zip(tomos, keys, images, strict=True)):
            frames = None
            if visible is not None:
                raw = visible[i]
                if raw not in parsed:
                    parsed[raw] = _parse_visible_frames(raw)
                frames = list(parsed[raw]) if parsed[raw] is not None else None
            index[(tomo, tuple(key))] = {"mrcs": root / image, "visible_frames": frames, "src_job_dir": job_dir}

    return index


def lookup_for_pick(pick_index: dict, tomo_name: str, x_ang: float, y_ang: float, z_ang: float) -> dict | None:
    """Look up a candidate pick's matching subtomo entry; returns None on miss."""
    return pick_index.get((tomo_name, _coord_key(x_ang, y_ang, z_ang)))

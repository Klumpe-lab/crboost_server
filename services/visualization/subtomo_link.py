"""Cross-job linkage from candidate-extract picks to subtomo-extract `.mrcs` files.

The candidate-preview dialog is anchored on a TEMPLATE_EXTRACT_PYTOM job; the
2D image stacks that show what each pick *looks like* are produced by a
downstream SUBTOMO_EXTRACTION job (`relion_tomo_subtomo` with 2D output).
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
from typing import Optional

import pandas as pd

from services.models_base import JobType

logger = logging.getLogger(__name__)


# 0.1 Å rounding — Å coords from RELION are written with one decimal in the
# .star file, and the candidate-extract pipeline doesn't introduce drift, so
# bit-exact match would also work. Round-to-tenths is a defensive margin.
_COORD_DECIMALS = 1


def _coord_key(x: float, y: float, z: float) -> tuple[int, int, int]:
    return (
        int(round(float(x), _COORD_DECIMALS) * 10**_COORD_DECIMALS),
        int(round(float(y), _COORD_DECIMALS) * 10**_COORD_DECIMALS),
        int(round(float(z), _COORD_DECIMALS) * 10**_COORD_DECIMALS),
    )


def _parse_visible_frames(raw) -> Optional[list[int]]:
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


def _read_subtomo_particles(particles_star: Path, job_dir: Path) -> Optional[pd.DataFrame]:
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


def build_pick_to_mrcs_index(
    candidate_extract_job_dir: Path, project_state, project_path: Path
) -> dict[tuple[str, tuple[int, int, int]], dict]:
    """Build a {(tomo_name, coord_key): {mrcs, visible_frames, src_job_dir}} lookup.

    Walks every SUBTOMO_EXTRACTION job in the project, reads its particles.star,
    and indexes by (tomo, coord_key). Later subtomo jobs win on key collision —
    this is a heuristic but matches the user expectation that "the most recent
    extraction is the one I care about."
    """
    index: dict[tuple[str, tuple[int, int, int]], dict] = {}

    if project_state is None or project_path is None:
        return index

    subtomo_jobs: list[Path] = []
    for instance_id, job_model in (project_state.jobs or {}).items():
        if getattr(job_model, "job_type", None) != JobType.SUBTOMO_EXTRACTION:
            continue
        relion_name = getattr(job_model, "relion_job_name", None)
        if not relion_name:
            mapped = (project_state.job_path_mapping or {}).get(instance_id)
            if not mapped:
                continue
            relion_name = mapped
        d = Path(project_path) / relion_name.rstrip("/")
        if d.is_dir():
            subtomo_jobs.append(d)

    # Sort by relion-job name so newer (lex-greater) jobs win — gives the
    # "most recent extraction takes precedence" semantics on coord collision.
    subtomo_jobs.sort()

    for job_dir in subtomo_jobs:
        df = _read_subtomo_particles(job_dir / "particles.star", job_dir)
        if df is None:
            continue
        if "rlnImageName" not in df.columns or "rlnTomoName" not in df.columns:
            continue
        coord_cols = ("rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnCenteredCoordinateZAngst")
        if not all(c in df.columns for c in coord_cols):
            continue

        for _, row in df.iterrows():
            tomo = str(row["rlnTomoName"])
            try:
                key = _coord_key(row[coord_cols[0]], row[coord_cols[1]], row[coord_cols[2]])
            except (TypeError, ValueError):
                continue
            mrcs_path = Path(str(row["rlnImageName"]))
            if not mrcs_path.is_absolute():
                mrcs_path = Path(project_path) / mrcs_path
            visible = (
                _parse_visible_frames(row.get("rlnTomoVisibleFrames")) if "rlnTomoVisibleFrames" in df.columns else None
            )
            index[(tomo, key)] = {"mrcs": mrcs_path, "visible_frames": visible, "src_job_dir": job_dir}

    return index


def lookup_for_pick(pick_index: dict, tomo_name: str, x_ang: float, y_ang: float, z_ang: float) -> Optional[dict]:
    """Look up a candidate pick's matching subtomo entry; returns None on miss."""
    return pick_index.get((tomo_name, _coord_key(x_ang, y_ang, z_ang)))

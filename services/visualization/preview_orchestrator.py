"""
Per-candidate-extract-job orchestrator: render a Z-MIP preview PNG with
candidate-circle overlays for each tomogram in a candidate_extract job dir.

Reuses the coord-transform helpers from imod_vis so the preview circles land at
exactly the same pixel positions as the IMOD .mod sphere centers — single source
of truth.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Callable, Optional

import numpy as np

from services.visualization.imod_vis import (
    _get_binned_tomo_size,
    _get_imod_coords,
    _get_pixel_size,
    _read_particles,
    _read_tomogram_info,
)
from services.visualization.preview_render import (
    is_preview_stale,
    render_candidate_preview,
)

logger = logging.getLogger(__name__)

PREVIEW_SUBDIR = Path("vis") / "preview"
MANIFEST_NAME = "manifest.json"


def _resolve_tomo_mrc(tomo_row, project_root: Optional[Path]) -> Optional[Path]:
    """Resolve the on-disk path to the reconstructed tomogram MRC, preferring the
    f32 sibling (mrcfile-friendly, also IMOD4-friendly) when present."""
    if "rlnTomoReconstructedTomogram" not in tomo_row.index:
        return None
    p = Path(str(tomo_row["rlnTomoReconstructedTomogram"]))
    if not p.is_absolute() and project_root is not None:
        p = project_root / p
    f32 = p.with_name(p.stem + "_f32.mrc")
    if f32.exists():
        return f32
    if p.exists():
        return p
    return None


def generate_candidate_previews(
    candidates_star: Path,
    tomograms_star: Path,
    particle_diameter_ang: float,
    output_dir: Path,
    project_root: Optional[Path] = None,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    force: bool = False,
    slab_mode: str = "central",
) -> dict:
    """For each tomogram referenced by candidates.star, write a Z-MIP PNG with
    candidate-circle overlays under <output_dir>/vis/preview/.

    Args:
        candidates_star: Job dir's candidates.star
        tomograms_star: Job dir's tomograms.star (same one consumed by IMOD vis)
        particle_diameter_ang: Particle diameter in Angstroms (for circle radius).
        output_dir: Job directory. Previews land under output_dir/vis/preview/.
        project_root: Project root for resolving relative MRC paths.
        progress_cb: Optional (i, total, tomo_name) callable for UI progress.
        force: If True, re-render even if the PNG is newer than the source.

    Returns:
        dict with keys: ok (list of tomo names), skipped_cached (list), missing_volume
        (list), errored (list of {tomo, error}), manifest_path (str).
    """
    candidates_star = Path(candidates_star)
    tomograms_star = Path(tomograms_star)
    output_dir = Path(output_dir)
    preview_dir = output_dir / PREVIEW_SUBDIR
    preview_dir.mkdir(parents=True, exist_ok=True)

    particles_df = _read_particles(candidates_star)
    tomo_df = _read_tomogram_info(tomograms_star)
    tomo_lookup = {row["rlnTomoName"]: row for _, row in tomo_df.iterrows()}
    tomo_names = list(particles_df["rlnTomoName"].unique())

    ok: list[str] = []
    skipped_cached: list[str] = []
    missing_volume: list[str] = []
    errored: list[dict] = []
    entries: dict[str, dict] = {}

    score_col = next(
        (c for c in ["rlnLCCmax", "rlnAutopickFigureOfMerit", "rlnMaxValueProbDistribution"]
         if c in particles_df.columns),
        None,
    )

    total = len(tomo_names)
    for i, tomo_name in enumerate(tomo_names):
        if progress_cb is not None:
            try:
                progress_cb(i, total, tomo_name)
            except Exception:
                pass

        tomo_row = tomo_lookup.get(tomo_name)
        if tomo_row is None:
            errored.append({"tomo": tomo_name, "error": "missing from tomograms.star"})
            continue

        mrc_path = _resolve_tomo_mrc(tomo_row, project_root)
        if mrc_path is None:
            missing_volume.append(tomo_name)
            continue

        # PNG path encodes slab_mode so changing render mode busts the cache
        # naturally and old "_mip.png" thumbnails from a prior render don't
        # silently win over a fresh "_central.png".
        out_png = preview_dir / f"{tomo_name}_{slab_mode}.png"
        # Cache: only sources that affect render output gate staleness.
        sources = [mrc_path, candidates_star, tomograms_star]
        if not force and not is_preview_stale(out_png, sources):
            skipped_cached.append(tomo_name)
            entries[tomo_name] = {"png": str(out_png), "cached": True}
            continue

        try:
            pixel_size = _get_pixel_size(tomo_row)
            tomo_size = _get_binned_tomo_size(tomo_row, project_root=project_root)
            tomo_particles = particles_df[particles_df["rlnTomoName"] == tomo_name]
            coords = _get_imod_coords(tomo_particles, tomo_size, pixel_size)
            radius_px = float(particle_diameter_ang) / (2.0 * float(pixel_size))

            scores = tomo_particles[score_col].values.astype(float) if score_col else None
            title = f"{tomo_name} · N={len(coords)}"
            if scores is not None and len(scores):
                title += f" · score {float(np.nanmin(scores)):.3f}–{float(np.nanmax(scores)):.3f}"

            info = render_candidate_preview(
                tomo_path=mrc_path,
                coords_xyz_px=np.asarray(coords),
                radius_px=radius_px,
                out_png=out_png,
                scores=scores,
                title=title,
                slab_mode=slab_mode,
            )
            entries[tomo_name] = {
                "png": info["png_path"],
                "n_candidates": info["n_candidates"],
                "score_range": list(info["score_range"]) if info["score_range"] else None,
                "dims": list(info["dims"]),
                "tomo_mrc": str(mrc_path),
                "cached": False,
            }
            ok.append(tomo_name)
        except Exception as e:
            logger.warning("Preview render failed for %s: %s", tomo_name, e)
            errored.append({"tomo": tomo_name, "error": str(e)})

    if progress_cb is not None:
        try:
            progress_cb(total, total, "")
        except Exception:
            pass

    manifest_path = preview_dir / MANIFEST_NAME
    manifest = {
        "score_field": score_col,
        "particle_diameter_ang": float(particle_diameter_ang),
        "slab_mode": slab_mode,
        "entries": entries,
        "summary": {
            "ok": ok,
            "skipped_cached": skipped_cached,
            "missing_volume": missing_volume,
            "errored": errored,
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))

    logger.info(
        "Previews: %d rendered, %d cached, %d missing volume, %d errored",
        len(ok), len(skipped_cached), len(missing_volume), len(errored),
    )

    return {
        "ok": ok,
        "skipped_cached": skipped_cached,
        "missing_volume": missing_volume,
        "errored": errored,
        "manifest_path": str(manifest_path),
    }


def read_preview_manifest(job_dir: Path) -> Optional[dict]:
    """Read the preview manifest if it exists. Returns None on any failure."""
    p = Path(job_dir) / PREVIEW_SUBDIR / MANIFEST_NAME
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        logger.warning("Could not parse preview manifest %s: %s", p, e)
        return None

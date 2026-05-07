"""
Per-job orchestrator for candidate-pick previews.

For each tomogram in a candidate-extract job, writes a per-tomo picks.json
plus one job-level manifest.json (v4). Also resolves the WarpTools-rendered
tomogram preview PNG (produced during ts_reconstruct) so the UI can use it
as a backdrop for the X/Y pick scatter.

NO volume reads. NO server-side plotting (Plotly does it client-side from the
picks.json). Scales linearly in #tomograms × #picks; runs in seconds even
for projects with hundreds of tomograms.
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
    is_output_stale,
    render_pick_cutouts_atlas,
    render_xz_slab_preview,
    write_picks_data,
)
from services.visualization.subtomo_link import build_pick_to_mrcs_index, lookup_for_pick

logger = logging.getLogger(__name__)

PREVIEW_SUBDIR = Path("vis") / "preview"
MANIFEST_NAME = "manifest.json"
MANIFEST_VERSION = 9  # v9: 192px sprites + per-pick failure reasons; drop imodup-flipped Warp PNG
# v5: per-pick z_pct + nn_px; entry.pixel_size_ang
# v6: fixes v4-v5 bug where glob "<tomo_name>.png" never matched real WarpTools
#     filenames "<tomo_name>_<res>Apx.png" — entry.warp_tomo_preview was always
#     None. Cached v5 manifests carry that wrong value; bump to invalidate.
# v7: render an X/Z slab preview PNG ourselves (central Y-slab average) so the
#     X/Z scatter has the same WarpTools-style backdrop as X/Y. Bounded MRC
#     read (~50 MB / tomogram, mmap), so a 100-tomo project adds <30 s of
#     preview-gen time.
# v8: per-tomogram sprite-atlas of subtomo cutouts (entry.cutout_atlas /
#     cutout_index) joining the picks to the SUBTOMO_EXTRACTION job's .mrcs
#     stacks via Å-coord lookup.
# v9: drops the Plotly image-overlay scatter (and its imodup-flipped Warp PNG
#     hack) in favor of rendering the original WarpTools PNG directly via
#     ui.image, matching the project-wide tomogram-preview widget. Bumps
#     sprite tile size 96→192 so the gallery is actually readable. Cutout
#     failures (no subtomo match, mrcs missing, etc.) are now surfaced in
#     entry.cutout_failures so the UI can explain "why is tile X missing?"

SCORE_COL_PRIORITY = ("rlnLCCmax", "rlnAutopickFigureOfMerit", "rlnMaxValueProbDistribution")


def _resolve_tomo_mrc(tomo_row, project_root: Optional[Path]) -> Optional[Path]:
    """Resolve the on-disk reconstructed-tomogram path. Used for the 3dmod
    copy-command — the orchestrator never reads the MRC bytes itself."""
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


def _find_warp_tomo_preview(
    project_root: Optional[Path], tomo_name: str, mrc_path: Optional[Path] = None
) -> Optional[Path]:
    """Locate the WarpTools tomogram-preview PNG for a given tomogram.

    WarpTools writes one PNG per reconstructed tomogram into
    <reconstruct_job>/warp_tiltseries/reconstruction/<tomo_name>_<resApx>.png
    as a side effect of ts_reconstruct, where <resApx> is e.g. "6.20Apx".
    The PNG sits next to the .mrc with the same stem, so when we already
    have the resolved MRC path we just swap the suffix — much more reliable
    than guessing the resolution from the tomo name.

    The fallback glob (when mrc_path isn't available, e.g. missing-volume
    case) anchors on `<tomo>_*Apx.png` so we don't accidentally match a
    longer-named neighbor.
    """
    if mrc_path is not None:
        stem = mrc_path.stem
        # _f32 variant lives next to the canonical "_<res>Apx.mrc" — strip
        # the suffix so we land on the same stem WarpTools uses for the PNG.
        if stem.endswith("_f32"):
            stem = stem[:-4]
        png = mrc_path.parent / f"{stem}.png"
        if png.exists():
            return png
    if project_root is None:
        return None
    pat = f"**/warp_tiltseries/reconstruction/{tomo_name}_*Apx.png"
    candidates = sorted(Path(project_root).glob(pat))
    return candidates[-1] if candidates else None


def _render_one_tomogram(
    pick_coords_xyz: np.ndarray,
    scores: Optional[np.ndarray],
    score_field: Optional[str],
    tomo_dims_xyz: tuple,
    pixel_size_ang: Optional[float],
    out_dir: Path,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    info = write_picks_data(
        pick_coords_xyz=pick_coords_xyz,
        scores=scores,
        score_field=score_field,
        tomo_dims_xyz=tomo_dims_xyz,
        out_path=out_dir / "picks.json",
    )
    return {
        "picks_json": info["json_path"],
        "n_picks": info["n"],
        "score_range": ([float(np.min(scores)), float(np.max(scores))] if scores is not None and len(scores) else None),
        "score_mean": float(np.mean(scores)) if scores is not None and len(scores) else None,
        "tomo_dims_xyz_px": [int(v) for v in tomo_dims_xyz],
        "pixel_size_ang": float(pixel_size_ang) if pixel_size_ang else None,
    }


def _entry_outputs(entry: dict) -> list:
    out = []
    if entry.get("picks_json"):
        out.append(Path(entry["picks_json"]))
    if entry.get("xz_preview"):
        out.append(Path(entry["xz_preview"]))
    if entry.get("cutout_atlas"):
        out.append(Path(entry["cutout_atlas"]))
    return out


def generate_candidate_previews(
    candidates_star: Path,
    tomograms_star: Path,
    particle_diameter_ang: float,
    output_dir: Path,
    project_root: Optional[Path] = None,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    force: bool = False,
    project_state=None,
) -> dict:
    """Build per-tomo picks.json + sprite-atlas + manifest for one extract job.

    `project_state` (optional) lets us walk the project's SUBTOMO_EXTRACTION
    jobs to build per-pick cutout atlases. Drivers that haven't been updated
    just don't get atlases — the manifest still produces correctly without it.
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

    score_col = next((c for c in SCORE_COL_PRIORITY if c in particles_df.columns), None)

    # Cross-job pick → .mrcs index (built once, reused per tomo). Cheap miss
    # if no SUBTOMO_EXTRACTION jobs exist.
    subtomo_index: dict = {}
    if project_state is not None and project_root is not None:
        try:
            subtomo_index = build_pick_to_mrcs_index(output_dir, project_state, project_root)
            logger.info("Subtomo index built: %d (tomo, coord) entries", len(subtomo_index))
        except Exception as e:
            logger.warning("Subtomo index build failed: %s", e)

    ok: list[str] = []
    skipped_cached: list[str] = []
    missing_volume: list[str] = []
    errored: list[dict] = []
    tomo_entries: dict[str, dict] = {}

    prior = read_preview_manifest(output_dir) or {}
    prior_entries = (prior.get("tomograms") or {}) if prior.get("version") == MANIFEST_VERSION else {}

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

        tomo_out_dir = preview_dir / tomo_name
        sources = [candidates_star, tomograms_star]
        prior_entry = prior_entries.get(tomo_name)
        if not force and prior_entry is not None:
            prior_outputs = _entry_outputs(prior_entry)
            if prior_outputs and not any(is_output_stale(o, sources) for o in prior_outputs):
                skipped_cached.append(tomo_name)
                tomo_entries[tomo_name] = prior_entry
                continue

        try:
            pixel_size = _get_pixel_size(tomo_row)
            tomo_size = _get_binned_tomo_size(tomo_row, project_root=project_root)
            tomo_particles = particles_df[particles_df["rlnTomoName"] == tomo_name]
            coords = _get_imod_coords(tomo_particles, tomo_size, pixel_size)
            scores = tomo_particles[score_col].values.astype(float) if score_col else None

            # Pull centered-Å coords for the subtomo lookup. Same column names
            # as the SUBTOMO_EXTRACTION job uses, so we get exact-match keys.
            ang_cols = ("rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnCenteredCoordinateZAngst")
            coords_ang = None
            if all(c in tomo_particles.columns for c in ang_cols):
                coords_ang = tomo_particles[list(ang_cols)].values.astype(float)

            # Score-desc sort applied here so the sprite-atlas index aligns
            # with the post-sort pick indices in picks.json — write_picks_data
            # repeats the same sort internally, producing the same order.
            if scores is not None and len(scores):
                order = np.argsort(-np.asarray(scores, dtype=float))
            else:
                order = np.arange(len(coords))
            coords_ang_sorted = coords_ang[order] if coords_ang is not None else None

            entry = _render_one_tomogram(
                pick_coords_xyz=np.asarray(coords),
                scores=scores,
                score_field=score_col,
                tomo_dims_xyz=tuple(int(v) for v in tomo_size),
                pixel_size_ang=pixel_size,
                out_dir=tomo_out_dir,
            )
            entry["tomo_mrc"] = str(mrc_path) if mrc_path else None
            warp_png = _find_warp_tomo_preview(project_root, tomo_name, mrc_path)
            entry["warp_tomo_preview"] = str(warp_png) if warp_png else None
            entry["xz_preview"] = None
            if mrc_path is not None:
                xz_png = render_xz_slab_preview(mrc_path, tomo_out_dir / "xz_preview.png")
                if xz_png is not None:
                    entry["xz_preview"] = str(xz_png)

            # Per-pick cutout sprite atlas (joined to subtomo .mrcs via Å coords).
            entry["cutout_atlas"] = None
            entry["cutout_index"] = None
            entry["cutout_n_ok"] = 0
            entry["cutout_failures"] = []
            if coords_ang_sorted is not None and subtomo_index:
                pick_to_mrcs = []
                for x_a, y_a, z_a in coords_ang_sorted:
                    info = lookup_for_pick(subtomo_index, tomo_name, x_a, y_a, z_a)
                    pick_to_mrcs.append(info)
                meta = render_pick_cutouts_atlas(
                    pick_to_mrcs, tomo_out_dir / "cutout_atlas.png", tomo_out_dir / "cutout_index.json"
                )
                if meta is not None:
                    entry["cutout_atlas"] = meta["atlas_path"]
                    entry["cutout_index"] = meta["index_path"]
                    entry["cutout_n_ok"] = meta["n_ok"]
                    entry["cutout_failures"] = meta.get("failures") or []

            tomo_entries[tomo_name] = entry
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
        "version": MANIFEST_VERSION,
        "score_field": score_col,
        "particle_diameter_ang": float(particle_diameter_ang),
        "tomograms": tomo_entries,
        "summary": {"ok": ok, "skipped_cached": skipped_cached, "missing_volume": missing_volume, "errored": errored},
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))

    logger.info(
        "Previews v4: %d rendered, %d cached, %d missing volume, %d errored",
        len(ok),
        len(skipped_cached),
        len(missing_volume),
        len(errored),
    )

    return {
        "ok": ok,
        "skipped_cached": skipped_cached,
        "missing_volume": missing_volume,
        "errored": errored,
        "manifest_path": str(manifest_path),
    }


def read_preview_manifest(job_dir: Path) -> Optional[dict]:
    p = Path(job_dir) / PREVIEW_SUBDIR / MANIFEST_NAME
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        logger.warning("Could not parse preview manifest %s: %s", p, e)
        return None

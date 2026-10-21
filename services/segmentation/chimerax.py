"""The segmentations "Open in ChimeraX" loads with a tomogram: every layer the views show
(`services.segmentation.display`), computed on the reconstruction the session's tomogram traces to
(or on one not known), as a surface at p = 0.5 in its feature's colour. The ChimeraX commands, and
the binned copies a session on a binned tomogram opens, are services/visualization/artiax_bridge.py's.
"""

from __future__ import annotations

from pathlib import Path

from services.job_lineage import job_dirs, recon_of
from services.models_base import species_palette_color
from services.segmentation import display
from services.segmentation.volumes import DISPLAY_THRESHOLD


def session_volumes(state, registry, project_path, tomo_name: str, tomograms_star) -> list[dict]:
    """``[{name, path, level, color, cache_dir}]`` for the session on `tomo_name` whose tomogram
    `tomograms_star` lists. `level` is p = 0.5 in the volume's stored values; `cache_dir` is the
    producer's directory (a Segment job's, an import batch's), where a binned copy may go: never
    beside a volume an import only references."""
    if not registry.has_tilt_series(tomo_name):
        return []
    tomogram = registry.get_tilt_series(tomo_name).tomogram
    if tomogram is None:
        return []
    dirs = job_dirs(state)
    star_job = dirs.get(Path(tomograms_star).parent)
    shows = recon_of(state, star_job, dirs) if star_job else None
    out = []
    for producer, output in tomogram.outputs.items():
        if output.output_type != "segmentation":
            continue
        recon = output.recon_instance or (recon_of(state, producer, dirs) if producer in state.jobs else None)
        if recon is not None and shows is not None and recon != shows:
            continue
        for feature, vol in sorted(output.volumes.items()):
            if not display.is_shown(project_path, display.layer_id(producer, feature)):
                continue
            entry = state.get_feature(feature)
            out.append(
                {
                    "name": feature,
                    "path": str(vol.path),
                    "level": DISPLAY_THRESHOLD * vol.full_scale,
                    "color": entry.color if entry is not None else species_palette_color(feature),
                    "cache_dir": str(output.job_dir),
                }
            )
    return out

"""One tomogram's picks and segmentations from crboost, as a bundle for the colleague's napari viewer.

A napari session opens a volume by path (``open.json``); this finds what the project holds for the tomogram that
volume is, so the viewer can show it beside the colleague's own particle layers:

- ``picks``: per species, its candidate-extract job's candidates (one layer per job) and every pick list the
  workbench holds for the tomogram (manual, imported, merged), in the species' colour and diameter.
- ``segmentations``: every feature volume the registry records for the tomogram on the reconstruction the volume
  traces to, as the views draw it (p = 0.5), shown or hidden as the views have it.

Coordinates are voxels of the volume the session opens, ZYX (napari's order): RELION-5 centered Å through that
volume's own header, ``voxel = centered / pixel + N / 2`` (``services.particles.coords``), so a denoised volume on
the reconstruction's grid takes the same picks. A segmentation keeps its own grid; ``voxel_size`` says how to place
it (both grids share the volume's corner). What is left out, and why, is listed in ``notes``: nothing is dropped
without a line there.

The bundle file's format is the viewer's (``crboost_bundle.py`` in the colleague's checkout reads it):

    {"format": "crboost-napari-bundle", "version": 1, "tomogram": "/abs/vol.mrc", "tomo_name": "...",
     "project": "/abs/project", "shape_zyx": [nz, ny, nx], "voxel_size": 10.0,
     "picks": [{"name", "species", "source", "color", "box_size", "coordinates_zyx": [[z, y, x], ...]}],
     "segmentations": [{"name", "path", "voxel_size", "shape_zyx", "level", "full_scale", "color", "visible"}],
     "notes": ["..."]}
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import mrcfile
import numpy as np

from services.dashboard_data import ce_instances_by_species
from services.job_lineage import job_dirs, recon_of
from services.models_base import species_palette_color
from services.particles.coords import CENTERED_COLS
from services.segmentation import display
from services.segmentation.volumes import DISPLAY_THRESHOLD

logger = logging.getLogger(__name__)

BUNDLE_FORMAT = "crboost-napari-bundle"
BUNDLE_VERSION = 1


@dataclass
class _PickSource:
    name: str  # the layer's name in the viewer
    species: str
    source: str  # "candidates <instance>" or "<list type> list <slug>"
    color: str
    box_size: float | None
    star: Path | None  # the star holding centered-Å picks (several tomograms; filtered by rlnTomoName)


@dataclass
class _Plan:
    """What the project's state says, read on the event loop (in-memory); the disk work follows in a thread."""

    project: Path
    volume: Path
    tomo_name: str
    picks: list[_PickSource] = field(default_factory=list)
    segmentations: list[dict] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def find_project(volume: Path) -> Path | None:
    """The crboost project `volume` lies in: the nearest directory above it holding ``project_params.json``."""
    for parent in volume.parents:
        if (parent / "project_params.json").is_file():
            return parent
    return None


def _tomogram_paths(registry) -> list[tuple[str, str, Path]]:
    """``(tomo_name, producer, volume path)`` for every volume the registry records for a tomogram: the
    reconstructions, the denoised volumes, and the densities segmentations were computed on."""
    out = []
    for ts in registry.all_tilt_series():
        tomogram = ts.tomogram
        if tomogram is None:
            continue
        for producer, output in tomogram.outputs.items():
            for attr in ("reconstructed_mrc", "denoised_mrc", "source_mrc"):
                p = getattr(output, attr, None)
                if p:
                    out.append((ts.id, producer, Path(p)))
    return out


def _match(volume: Path, project: Path, candidates: list[tuple[str, str, Path]]) -> tuple[str, str] | None:
    """The (tomo_name, producer) whose volume is `volume`. Disk: resolves paths (symlinks, relative ones)."""
    target = volume.resolve()
    for tomo, producer, p in candidates:
        q = p if p.is_absolute() else project / p
        if q == volume or q.resolve() == target:
            return tomo, producer
    return None


def _plan(state, registry, project: Path, volume: Path, tomo_name: str, producer: str) -> _Plan:
    """The bundle's sources from the project's state and registry (in memory)."""
    plan = _Plan(project=project, volume=volume, tomo_name=tomo_name)
    dirs = job_dirs(state)
    shows = recon_of(state, producer, dirs) if producer in state.jobs else None

    tomogram = registry.get_tilt_series(tomo_name).tomogram
    for seg_producer, output in (tomogram.outputs if tomogram else {}).items():
        if output.output_type != "segmentation":
            continue
        recon = output.recon_instance or (recon_of(state, seg_producer, dirs) if seg_producer in state.jobs else None)
        if not display.on_source(recon, shows):
            plan.notes.append(f"{seg_producer}: computed on {recon}, not on {shows}, which this volume comes from")
            continue
        for feature, vol in sorted(output.volumes.items()):
            entry = state.get_feature(feature)
            plan.segmentations.append(
                {
                    "name": f"{feature} ({seg_producer})",
                    "path": str(vol.path),
                    "voxel_size": float(output.voxel_angstrom),
                    "shape_zyx": list(output.shape_zyx),
                    "level": DISPLAY_THRESHOLD * vol.full_scale,
                    "full_scale": float(vol.full_scale),
                    "color": entry.color if entry is not None else species_palette_color(feature),
                    "visible": display.is_shown(project, display.layer_id(seg_producer, feature)),
                }
            )

    ce_by_species, unclaimed = ce_instances_by_species(state)
    if unclaimed:
        plan.notes.append(
            "candidates of " + ", ".join(iid for iid, _ in unclaimed) + " belong to no registered species: not shown"
        )
    for sp in state.species_registry:
        label = sp.name or sp.id
        box = float(sp.diameter_ang) if sp.diameter_ang else None
        for iid, jm in ce_by_species.get(sp.id, []):
            rel = jm.relion_job_name or (state.job_path_mapping or {}).get(iid)
            plan.picks.append(
                _PickSource(
                    name=f"{label} · candidates ({iid})",
                    species=label,
                    source=f"candidates {iid}",
                    color=sp.color,
                    box_size=box,
                    star=project / rel.rstrip("/") / "candidates.star" if rel else None,
                )
            )
        for pl in state.get_pick_lists(sp.id, tomo_name):
            plan.picks.append(
                _PickSource(
                    name=f"{label} · {pl.label or pl.slug}",
                    species=label,
                    source=f"{pl.list_type.value} list {pl.slug}",
                    color=sp.color,
                    box_size=box,
                    star=Path(pl.path) if pl.path else None,
                )
            )
    return plan


def _volume_geometry(volume: Path) -> tuple[np.ndarray, float]:
    """``(N as x, y, z, voxel size in Å)`` from the volume's header. A header without a voxel size has no frame for
    centered-Å picks, so it raises rather than guess one."""
    with mrcfile.open(str(volume), header_only=True, permissive=True) as m:
        n = np.array([int(m.header.nx), int(m.header.ny), int(m.header.nz)], dtype=float)
        vs = m.voxel_size
        sizes = {float(vs.x), float(vs.y), float(vs.z)}
    px = sizes.pop() if len(sizes) == 1 else 0.0
    if px <= 0:
        raise ValueError(f"{volume.name}: the header carries no single voxel size, so picks have no frame there")
    return n, px


def _read_tomo_picks(star: Path, tomo_name: str) -> np.ndarray:
    """The centered-Å XYZ picks of `tomo_name` in `star` (a candidates or particles table)."""
    import starfile

    for table in starfile.read(star, always_dict=True).values():
        if hasattr(table, "columns") and "rlnTomoName" in table.columns and CENTERED_COLS[0] in table.columns:
            rows = table[table["rlnTomoName"].astype(str) == tomo_name]
            return rows[CENTERED_COLS].to_numpy(dtype=float)
    raise ValueError(f"{star.name} has no table with rlnTomoName and centered coordinates")


def _build(plan: _Plan) -> dict:
    """The bundle (disk: the volume's header and the stars)."""
    n_xyz, px = _volume_geometry(plan.volume)
    notes = list(plan.notes)
    picks = []
    for src in plan.picks:
        star = src.star
        if star is None or not star.is_file():
            notes.append(f"{src.name}: no file on disk ({star or 'none recorded'})")
            continue
        if star.suffix != ".star":
            notes.append(f"{src.name}: {star.name} is not a STAR file; only centered-Å STARs are read")
            continue
        try:
            xyz = _read_tomo_picks(star, plan.tomo_name)
        except (OSError, ValueError, KeyError) as e:
            notes.append(f"{src.name}: {e}")
            continue
        if len(xyz) == 0:
            continue
        voxel_xyz = xyz / px + n_xyz / 2.0
        picks.append(
            {
                "name": f"{src.name} ({len(xyz)})",
                "species": src.species,
                "source": src.source,
                "color": src.color,
                "box_size": src.box_size,
                "coordinates_zyx": np.round(voxel_xyz[:, ::-1], 2).tolist(),
            }
        )
    segs = []
    for seg in plan.segmentations:
        if not Path(seg["path"]).is_file():
            notes.append(f"{seg['name']}: {seg['path']} is not on disk")
            continue
        segs.append(seg)
    return {
        "format": BUNDLE_FORMAT,
        "version": BUNDLE_VERSION,
        "tomogram": str(plan.volume),
        "tomo_name": plan.tomo_name,
        "project": str(plan.project),
        "shape_zyx": [int(v) for v in n_xyz[::-1]],
        "voxel_size": px,
        "picks": picks,
        "segmentations": segs,
        "notes": notes,
    }


def write_bundle(bundle: dict, path: Path) -> None:
    """Write by rename, so the viewer never reads half a bundle."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(bundle))
    os.replace(tmp, path)


async def tomogram_bundle(volume: str | Path) -> dict | None:
    """The bundle for the tomogram at `volume`, or None when the volume is no tomogram of a crboost project (a
    file opened from elsewhere): nothing to add. Raises on a project whose files cannot be read, which the caller
    reports beside an otherwise working open."""
    from services.project_state import get_project_state_for, preload_project_state
    from services.tilt_series import get_registry_for

    volume = Path(volume)
    project = await asyncio.to_thread(find_project, volume)
    if project is None:
        return None
    await preload_project_state(project)
    state = get_project_state_for(project)
    registry = get_registry_for(project)
    candidates = _tomogram_paths(registry)
    hit = await asyncio.to_thread(_match, volume, project, candidates)
    if hit is None:
        logger.info("napari bundle: %s is no tomogram of %s's registry", volume, project.name)
        return None
    plan = _plan(state, registry, project, volume, *hit)
    return await asyncio.to_thread(_build, plan)

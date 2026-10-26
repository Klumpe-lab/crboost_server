"""Tomograms — the birds-eye wall of reconstructions, and every tilt beside it.

One tile per tomogram the project knows about (the reconstruct job's
``tomograms.star`` first, then imported volumes — see
``services.visualization.tomo_geometry.all_geometries``), laid out as a grid the
way the tilt-filter overview lays out tilts: everything at once.
The Journey answers "how did THIS tilt-series go"; this answers "how do they all
look", which is the question you ask before you pick one.

Two tabs, switched on the view's title line: Tomograms (the wall) and Tilts (every tilt preview,
grouped by tilt series; ``ui/tilt_previews.py``). A project with tilts and no tomograms opens on Tilts.

The wall's tools sit on the title line too, each a glyph whose dropdown opens under the pointer, and
all of them just re-render from the already-collected rows:

  · **version** — which version of the tomograms every tile shows, by reconstruction (one per
    reconstruct job when there are several, named by the alignment it traces back to): the
    reconstruction itself (``raw``: the WarpTools PNG) and each denoised X/Y slab made from it
    (cryoCARE / IsoNet / any other denoisepredict job that has a volume for the tomogram). Only
    versions that actually produced something appear.
  · **layers** — the pick layers, by species, and the segmentations, in one list
    (``ui/components/layer_menu.py``, the same list as the Journey's and the pick viewer's). Each
    candidate-extract job's picks, and each species' registered lists, are a pick layer with its own
    dot color (pickable); each segmentation (a Segment job's membranes, void, …) is a transparent fill
    where p ≥ 0.5 over the central slab, in the feature's colour, under the dots. Every layer follows
    the version: it shows over the versions of the reconstruction it was made on, so two picking arms
    on two alignments each sit on their own. Switching a layer by hand sets it apart from that; a
    layer shown over another reconstruction is flagged. The same menu imports segmentations made
    outside crboost.
  · **size** — how many tiles (and tilt cards) fit across.

Tiles show a downscaled JPEG of each picture, and the browser fetches the other sources'
while idle, so a source switch re-points images it already holds.

One tilt series is the selection, shared with the Journey: a click on a tile selects it (on the
Tilts tab, a click on a series), the Journey beside the wall follows, and a tilt series the Journey
switches to is selected here. The selection holds across the two tabs, and whenever it moves, the
tab changes or the view comes back, the wall scrolls to it if it is out of view.

A click on the selected tile's image opens the zoom view: the slice as large as the screen allows,
its pick count and 3dmod chip in its corners, and beside it a column with the tilts summed up (how
many are in the tomogram, the stage-tilt range in use, the series' CTF fit, the dark exposures in
use) and the wall's two lists for this tomogram (its versions, ↑ ↓; its layers, matched to the zoom's
own version, with its picks of each and each segmentation's share of the volume) and the Journey button.
The zoom's version never changes the wall's; its layers are the wall's; ← → step the selection
through the wall. The toggle in a tile's top-left corner swaps the slice for a mosaic of that
series' tilts, in the same box; a tilt opens the full-size tilt viewer. The caption under a tile is
its position and the Journey glyph, which opens the Journey on that tilt series, beside the wall.

Images come from the same renderers the Journey uses (the WarpTools PNG
written by ts_reconstruct, else an X/Y slab we render and cache), so a tile and
the Journey's Reconstruct card can never show different pictures — and a missing
slab is auto-kicked here exactly as it is there, capped per render so opening the
wall on a 100-tomogram project doesn't submit 100 renders at once.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from nicegui import ui

from services.dashboard_data import job_dir_for, position_label, tilt_thumb_dir, vis_asset_url
from services.models_base import InstanceId, JobType
from services.project_state import get_project_state_for
from services.segmentation import display
from services.tilt_series_service import ensure_tilt_thumbnails
from services.visualization.preview_orchestrator import read_preview_manifest
from services.visualization.preview_render import is_output_stale
from services.visualization.tomo_geometry import SOURCE_IMPORTED, TomoGeometry, all_geometries
from ui.components.buttons import house_button, icon_button
from ui.components.copyable import copy_3dmod_chip
from ui.components import feature_layers as fl
from ui.components.dialogs import dialog_host
from ui.components.hover_menu import Choice, ChoiceRows, hover_menu, tool_trigger
from ui.components.layer_menu import LayerRow, render_layer_list
from ui.components.reactive import SingleFlight, owned_timer
from services.job_lineage import job_dirs, recon_of
from ui.components.recon_compare import recon_version_labels
from ui.components.segmented import render_segmented
from ui.components.svg_icon import load_icon_svg, render_glyph
from ui.dashboard.css import ensure_assets_loaded
from ui.segmentation_import_dialog import open_segmentation_import_dialog
from ui.styles import WAIT_SPINNER
from ui.tilt_previews import (
    TILT_CLICK_JS,
    TiltGallery,
    collect_tilt_groups,
    mosaic_html,
    open_tilt_viewer,
    registry_tilt_series,
    scroll_into_view,
    thumbnail_task_key,
    tilt_context,
    tomogram_caption_html,
)

logger = logging.getLogger(__name__)

RECON_SOURCE = "recon"
# A reconstruction's own picture in the version menu, beside the denoised versions made from it.
RAW_VARIANT = "raw"
TOMOGRAMS_TAB, TILTS_TAB = "tomograms", "tilts"

# Tile column width per size step (CSS grid minmax floor).
_SIZES: tuple[tuple[str, str, int], ...] = (("s", "S", 150), ("m", "M", 240), ("l", "L", 380))
DEFAULT_SIZE = "m"

# The mosaic toggle's glyph: four squares, in currentColor.
_MOSAIC_ICON = (
    '<svg width="10" height="10" viewBox="0 0 10 10" fill="currentColor" style="display: block;">'
    '<rect width="4" height="4"/><rect x="6" width="4" height="4"/>'
    '<rect y="6" width="4" height="4"/><rect x="6" y="6" width="4" height="4"/></svg>'
)

# Placeholder tiles while the tomograms are being listed, before their number is known.
_SKELETON_TILES = 12

# Dots kept per (tomogram, species). picks.json is sorted score-descending, so the
# stride sample the wall draws from this is spread across the whole score range
# rather than being the top-N. The zoom view draws the whole stored set.
_MAX_DOTS = 1200
_WALL_DOTS = 260
_DOT_R_WALL = 1.1
_DOT_R_ZOOM = 2.0

# A reconstruct job's volume or its WarpTools preview: `<tomogram>_<apix>Apx.mrc|png`.
_RECON_FILE = re.compile(r"(.+)_[\d.]+Apx\.(mrc|png)")

# Threads for the per-tomogram reads: each is one small file or a few stats, and one after the
# other on NFS they add up to seconds for a hundred tomograms.
_IO_THREADS = 16

# Tomograms whose sources one pass reads before their tiles fill in.
_FILL_CHUNK = 24

# Tiles show a JPEG at most this wide (the zoom shows the PNG itself): a WarpTools PNG is ~1 MB,
# and a wall of them is tens of MB to fetch and decode on every source switch.
_TILE_THUMB_PX = 640

# Missing X/Y slabs auto-kicked per render. The wall asks for every tomogram at
# once; each render reads ~50 MB of MRC, so submit a few and let the next refresh
# (or the completion callback) take the rest.
_MAX_KICKS_PER_RENDER = 6

# The zoom's side column, and the room the dialog's padding takes around the slice.
_ZOOM_SIDE_PX = 300
_ZOOM_CHROME_PX = 28

# The zoom dialog's keys (← → step through the wall, ↑ ↓ switch versions), filtered in the
# browser so no other key is sent.
_ZOOM_KEYS_JS = """(event) => {
    if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(event.key)) return;
    event.preventDefault();
    emit({key: event.key});
}"""


# ---------------------------------------------------------------------------
# Collection — pure disk reads, runs in a thread (no NiceGUI here)
# ---------------------------------------------------------------------------


def _pmap(fn, items: list) -> list:
    """`fn` over `items` on `_IO_THREADS` threads, results in order."""
    if len(items) < 2:
        return [fn(x) for x in items]
    with ThreadPoolExecutor(max_workers=_IO_THREADS) as pool:
        return list(pool.map(fn, items))


def _stride_sample(items: list, cap: int) -> list:
    """At most ``cap`` items, evenly spread across the list (not the head)."""
    n = len(items)
    if n <= cap:
        return items
    step = n / cap
    return [items[int(i * step)] for i in range(cap)]


def _normalized_dots(picks: list, dims) -> list[tuple[float, float]]:
    """Picks → (x%, y%) of the tile frame, Y flipped like the shared canvas
    (``pick_viewer._render_pick_layer``). Unknown extent ⇒ no dots: a dot placed
    against a stand-in size is how picks end up quietly in the wrong corner."""
    if not dims:
        return []
    x_dim = max(int(dims[0]), 1)
    y_dim = max(int(dims[1]), 1)
    out: list[tuple[float, float]] = []
    for p in _stride_sample(picks, _MAX_DOTS):
        try:
            fx = min(max(float(p.get("x", 0)) / x_dim, 0.0), 1.0)
            fy = 1.0 - min(max(float(p.get("y", 0)) / y_dim, 0.0), 1.0)
        except (TypeError, ValueError):
            continue
        out.append((fx * 100.0, fy * 100.0))
    return out


def _pick_layers(project_state, project_path: Path, geoms: list[TomoGeometry]) -> tuple[list[dict], dict]:
    """The pick overlays: ``(layers, {ts: [{id, n, dots, dims}, ...]})``.

    A layer is one set of picks with one origin: each candidate-extract job's picks, and each
    species' registered lists (hand-picked, imported, merged) per reconstruction they trace back
    to. Two picking arms of one species are two layers. ``layers`` =
    ``[{id, species_id, species, label, recon, recon_label, color, n}]`` in the render plan's
    species order. ``recon`` is the reconstruct instance the picks were made on, which the toolbar
    matches against the source on show; None when it is not known (a list picked in ArtiaX).
    Each layer gets its own overlay color, so two layers on one tile can be told apart."""
    # The Journey's own readers, so the wall's dots and the viewer's dots can never
    # disagree about where a pick is.
    from services.dashboard_data import SPECIES_OVERLAY_COLORS, job_dir_for, species_render_plan
    from services.models_base import PickSourceKind
    from ui.particles.pick_viewer import _read_pick_list_voxels, _read_picks_json

    geom_by_ts = {g.tomo_name: g for g in geoms}
    dirs = job_dirs(project_state)
    recon_labels = recon_version_labels(project_state)
    layers: list[dict] = []
    per_ts: dict[str, list[dict]] = {}

    def _recon_label(recon: str | None) -> str:
        return recon_labels.get(recon, recon or "") if len(recon_labels) > 1 else ""

    def _add(layer: dict, entries: dict[str, dict]) -> None:
        entries = {ts: e for ts, e in entries.items() if e["n"]}
        if not entries:
            return
        layer["color"] = SPECIES_OVERLAY_COLORS[len(layers) % len(SPECIES_OVERLAY_COLORS)]
        layer["n"] = sum(e["n"] for e in entries.values())
        layer["recon_label"] = _recon_label(layer["recon"])
        layers.append(layer)
        for ts, e in entries.items():
            per_ts.setdefault(ts, []).append({"id": layer["id"], **e})

    lists_done: set[str] = set()
    for species, species_id, ce in species_render_plan(project_state):
        name = str(getattr(species, "name", "") or species_id or "")
        if ce is not None:
            iid, jm = ce
            recon = recon_of(project_state, iid, dirs)
            entries: dict[str, dict] = {}
            job_dir = job_dir_for(project_state, iid, jm, project_path)
            manifest = read_preview_manifest(job_dir) if job_dir else None
            todo = [(ts, e) for ts, e in ((manifest or {}).get("tomograms") or {}).items() if e.get("picks_json")]
            # The first read fills the job's σ cache, which the others then share.
            datas = [_read_picks_json(Path(e["picks_json"])) for _ts, e in todo[:1]]
            datas += _pmap(lambda te: _read_picks_json(Path(te[1]["picks_json"])), todo[1:])
            for (ts, entry), data in zip(todo, datas, strict=True):
                dims = data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px")
                entries[ts] = {
                    "n": int(data.get("n") or len(data.get("picks") or [])),
                    "dots": _normalized_dots(data.get("picks") or [], dims),
                    # The extent the dots were normalized against — the tile's frame
                    # falls back to it when the geometry provider has no dims, so the
                    # box the dots are placed in stays the box they were computed for.
                    "dims": [int(dims[0]), int(dims[1])] if dims else None,
                }
            label = f"TM picks · {_recon_label(recon) or iid}"
            _add({"id": iid, "species_id": species_id, "species": name or iid, "label": label, "recon": recon}, entries)
        if not species_id or species_id in lists_done:
            continue
        lists_done.add(species_id)
        # Hand-authored / imported / merged lists live in ProjectState, mapped into the
        # tomogram's voxel frame by the geometry provider — no MRC re-read per tile. A list
        # cut from a candidate-extract job's picks traces back through that job.
        by_recon: dict[str | None, dict[str, list]] = {}
        for pl in getattr(project_state, "pick_lists", None) or []:
            if pl.species_id != species_id:
                continue
            from_tm = pl.source_kind == PickSourceKind.TM and pl.source_ref in project_state.jobs
            recon = recon_of(project_state, pl.source_ref, dirs) if from_tm else None
            by_recon.setdefault(recon, {}).setdefault(pl.tomo_name, []).append(pl)

        def _list_entry(item: tuple[str, list]) -> tuple[str, dict]:
            ts, lists = item
            geom = geom_by_ts.get(ts)
            if geom is not None and geom.is_usable:
                picks: list[dict] = []
                for pl in lists:
                    # None = unreadable (already logged by the reader); the wall draws what it can.
                    picks.extend(_read_pick_list_voxels(Path(pl.path), list(geom.dims_xyz_px), geom.binned_apix) or [])
                n, dots = len(picks), _normalized_dots(picks, geom.dims_xyz_px)
                dims = [int(geom.dims_xyz_px[0]), int(geom.dims_xyz_px[1])]
            else:
                # No resolved frame ⇒ nothing can be placed. Carry the registered
                # count anyway so the tile can say "picks here, not drawable" —
                # dropping the species would read as "nothing picked".
                n, dots, dims = sum(int(pl.count or 0) for pl in lists), [], None
            return ts, {"n": n, "dots": dots, "dims": dims}

        for recon, by_ts in by_recon.items():
            entries = dict(_pmap(_list_entry, list(by_ts.items())))
            label = "pick lists" + (f" · {_recon_label(recon)}" if _recon_label(recon) else "")
            layer_id = f"lists:{species_id}:{recon or ''}"
            _add({"id": layer_id, "species_id": species_id, "species": name, "label": label, "recon": recon}, entries)
    # Two candidate-extract jobs of one species on one reconstruction: name them by instance.
    counts: dict[tuple, int] = {}
    for layer in layers:
        counts[(layer["species_id"], layer["label"])] = counts.get((layer["species_id"], layer["label"]), 0) + 1
    for layer in layers:
        if counts[(layer["species_id"], layer["label"])] > 1:
            layer["label"] = f"TM picks · {layer['id']}"
    return layers, per_ts


def _recon_source(slab_dir: Path, ts_name: str, mrc: Path | None, warp_png: Path | None) -> dict:
    """One reconstruction's tile picture: the WarpTools PNG ts_reconstruct wrote next to the volume,
    else the X/Y slab we render ourselves under `slab_dir` (the only option for an imported
    tomogram, which has no WarpTools output)."""
    from ui.particles.pick_viewer import _recon_slab_paths

    xy_png, _xz = _recon_slab_paths(slab_dir, ts_name)
    if warp_png is not None:
        src = {"png": warp_png, "kick": None, "caption": f"WarpTools · {warp_png.name}"}
    elif xy_png.exists() and not (mrc and is_output_stale(xy_png, [mrc])):
        src = {"png": xy_png, "kick": None, "caption": f"X/Y slab · {xy_png.name}"}
    else:
        src = {"png": None, "kick": (slab_dir, mrc) if mrc else None, "caption": "X/Y slab"}
    src["mrc"] = mrc
    src["url"], src["thumb"] = _png_urls(src["png"])
    return src


def _png_urls(png: Path | None) -> tuple[str | None, str | None]:
    """The picture's URLs, full size (the zoom) and as a tile thumbnail (the wall), made here in
    the collection thread: each stats the PNG, and a stat per tile on the event loop is what froze
    the page on a source switch."""
    if png is None:
        return None, None
    return vis_asset_url(str(png)), vis_asset_url(str(png), thumb=_TILE_THUMB_PX)


def _has_png(src: dict | None) -> bool:
    return src is not None and src["png"] is not None


def _meta_of(sources: dict[str, dict]) -> dict[str, dict]:
    """``{source key: {recon, group, variant}}`` of one tomogram's sources (`_sources_for`)."""
    return {
        key: {"recon": src.get("recon"), "group": src.get("group", ""), "variant": src.get("variant", key)}
        for key, src in sources.items()
    }


def recon_source_key(label: str) -> str:
    """Toolbar key of one reconstruct job's version when the project has several."""
    return f"{RECON_SOURCE} · {label}"


def source_prep(project_state, project_path: Path, tilt_series: list) -> dict:
    """What every tomogram's sources are looked up in, read once for the whole wall: each
    reconstruct job's reconstruction directory, listed once (a glob per tomogram lists the same
    directory again for every tile), the denoisepredict jobs, the registry's tilt series by id and
    the job dirs.

    ``versions`` = ``{tomogram: [{iid, job, dir, label, mrc, png}]}`` by job dir, the first volume
    by name per job (as ``recon_versions_for_ts``); ``pngs`` = ``{tomogram: [WarpTools PNG]}``;
    ``denoise`` = ``[{iid, label, dir}]``; ``labels`` = ``recon_version_labels``. Disk-only."""
    from ui.tomo_dashboard_dialog import _denoise_method_label

    project_path = Path(project_path)
    labels = recon_version_labels(project_state)
    versions: dict[str, list[dict]] = {}
    pngs: dict[str, list[Path]] = {}
    for iid, jm in project_state.jobs.items():
        if jm.job_type != JobType.TS_RECONSTRUCT or not jm.relion_job_name:
            continue
        job = jm.relion_job_name.rstrip("/")
        rec_dir = project_path / job / "warp_tiltseries" / "reconstruction"
        try:
            names = sorted(os.listdir(rec_dir))
        except OSError:
            # Planned, deleted or unreadable: the job offers no volume, as the glob it stands in for found none.
            continue
        present = set(names)
        for name in names:
            m = _RECON_FILE.fullmatch(name)
            if m is None:
                continue
            ts = m.group(1)
            if m.group(2) == "png":
                pngs.setdefault(ts, []).append(rec_dir / name)
            elif not any(v["iid"] == iid for v in versions.get(ts, [])):
                png = f"{name[: -len('.mrc')]}.png"
                versions.setdefault(ts, []).append(
                    {
                        "iid": iid,
                        "job": job,
                        "dir": project_path / job,
                        "label": labels.get(iid, iid),
                        "mrc": rec_dir / name,
                        "png": rec_dir / png if png in present else None,
                    }
                )
    for vs in versions.values():
        vs.sort(key=lambda v: v["job"])

    denoise = []
    for iid, jm in (project_state.jobs or {}).items():
        if getattr(jm, "job_type", None) != JobType.DENOISE_PREDICT and not InstanceId.matches(
            iid, JobType.DENOISE_PREDICT
        ):
            continue
        job_dir = job_dir_for(project_state, iid, jm, project_path)
        if job_dir is not None:
            denoise.append({"iid": iid, "label": _denoise_method_label(jm, iid, project_state), "dir": job_dir})

    return {
        "labels": labels,
        "versions": versions,
        "pngs": pngs,
        "denoise": denoise,
        "tilt_series": {ts.id: ts for ts in tilt_series},
        "dirs": job_dirs(project_state),
    }


def _warp_png(prep: dict, geom: TomoGeometry) -> Path | None:
    """The WarpTools PNG of a tomogram (``_find_warp_tomo_preview``): beside its volume, else the
    newest-named one any reconstruct job wrote for it."""
    if geom.recon_mrc is not None:
        png = geom.recon_mrc.parent / f"{geom.recon_mrc.stem}.png"
        if png.exists():
            return png
    found = prep["pngs"].get(geom.tomo_name)
    return max(found) if found else None


def _denoised_for_ts(prep: dict, ts_name: str) -> list[tuple[str, str, Path, Path]]:
    """(source label, method label, job dir, denoised volume) per denoisepredict job with a volume of
    this tomogram on disk, by label (``_available_denoise_methods_for_ts``, from the prep). Two jobs
    of one method are told apart by instance in the source label."""
    ts = prep["tilt_series"].get(ts_name)
    outputs = ts.tomogram.outputs if ts is not None and ts.tomogram is not None else {}
    found: list[tuple[str, str, Path, Path]] = []
    for job in prep["denoise"]:
        out = outputs.get(job["iid"])
        if out is None or getattr(out, "output_type", "") != "denoise_predict":
            continue
        # The registry can outlive a deleted volume; such a method has nothing to show.
        mrc = Path(out.denoised_mrc)
        if mrc.exists():
            found.append((job["label"], job["iid"], job["dir"], mrc))
    label_counts: dict[str, int] = {}
    for lbl, *_ in found:
        label_counts[lbl] = label_counts.get(lbl, 0) + 1
    out_list = [
        (lbl if label_counts[lbl] == 1 else f"{lbl} ({iid})", lbl, job_dir, mrc) for lbl, iid, job_dir, mrc in found
    ]
    out_list.sort(key=lambda t: t[0].lower())
    return out_list


def _sources_for(project_state, geom: TomoGeometry, prep: dict) -> dict[str, dict]:
    """Per-tomogram image sources, keyed by the toolbar's source id.

    With one reconstruct job, ``recon`` is its picture. With several (e.g. one per alignment),
    each job with a volume of this tomogram is its own version, keyed ``recon · <alignment>``
    by the project-wide label, so one toolbar choice shows the same job on every tile; a tomogram
    a job did not reconstruct says so instead of showing another job's. Each denoisepredict job
    with a volume for this tomogram adds one entry under its method label. ``kick`` carries the
    (cache dir, source MRC) the renderer needs when the PNG isn't on disk yet; ``mrc`` is the
    volume the picture shows, for the 3dmod chip; ``recon`` the reconstruct instance it is (or
    was denoised from), which pick layers are matched against. ``group`` names that reconstruction
    and ``variant`` the picture of it (``raw``, or the denoising method), for the version menu.
    """
    from ui.tomo_dashboard_dialog import _denoise_slab_path

    out: dict[str, dict] = {}
    recon_labels = prep["labels"]
    dirs = prep["dirs"]
    if geom.source != SOURCE_IMPORTED and len(recon_labels) > 1:
        for v in prep["versions"].get(geom.tomo_name, []):
            src = _recon_source(v["dir"], geom.tomo_name, v["mrc"], v["png"])
            src["caption"] = f"{v['label']} · {src['caption']}"
            src["recon"] = v["iid"]
            src["group"], src["variant"] = v["label"], RAW_VARIANT
            out[recon_source_key(v["label"])] = src
    else:
        src = _recon_source(geom.tomograms_star.parent, geom.tomo_name, geom.recon_mrc, _warp_png(prep, geom))
        src["recon"] = next(iter(recon_labels)) if geom.source != SOURCE_IMPORTED and recon_labels else None
        src["group"] = "imported" if geom.source == SOURCE_IMPORTED else recon_labels.get(src["recon"], "")
        src["variant"] = RAW_VARIANT
        out[RECON_SOURCE] = src

    for label, method, job_dir, mrc in _denoised_for_ts(prep, geom.tomo_name):
        png = _denoise_slab_path(job_dir, geom.tomo_name)
        fresh = png.exists() and not is_output_stale(png, [mrc])
        url, thumb = _png_urls(png if fresh else None)
        denoise_iid = dirs.get(Path(job_dir))
        recon = recon_of(project_state, denoise_iid, dirs) if denoise_iid else None
        out[label] = {
            "png": png if fresh else None,
            "kick": None if fresh else (job_dir, mrc),
            "caption": f"{label} · denoised X/Y slab",
            "mrc": mrc,
            "url": url,
            "thumb": thumb,
            "recon": recon,
            "group": recon_labels.get(recon) or "reconstruction not known",
            "variant": method,
        }
    return out


def sources_by_ts(project_state, geoms: list[TomoGeometry], prep: dict) -> dict[str, dict[str, dict]]:
    """``{tomogram: _sources_for(...)}`` for `geoms`, read on a few threads. Disk-only."""
    found = _pmap(lambda g: _sources_for(project_state, g, prep), geoms)
    return {g.tomo_name: sources for g, sources in zip(geoms, found, strict=True)}


def _row_aspect(geom: TomoGeometry, picks: list[dict]) -> tuple | None:
    # Frame aspect = the tomogram's own X/Y extent, else the extent the picks on it
    # were normalized against (the candidate-extract job read it when it wrote
    # picks.json, and can have it when the volume is no longer on disk). Neither ⇒
    # the frame is not the tomogram's box, so the tile lets the image size itself
    # and reports its picks as counted-but-unplaced rather than scattering them
    # against a guessed extent.
    if geom.dims_xyz_px:
        return tuple(geom.dims_xyz_px[:2])
    return next((tuple(p["dims"]) for p in picks if p.get("dims")), None)


def compose_row(geom: TomoGeometry, sources: dict, picks: list[dict], features: list[dict], *, ready: bool) -> dict:
    """One tile's row. ``ready`` is False while its sources are still being read: the tile
    shows its frame and label and says it is reading."""
    label, order = position_label(geom.tomo_name)
    return {
        "ts": geom.tomo_name,
        "label": label,
        "order": order,
        "aspect": _row_aspect(geom, picks),
        "volume": str(geom.recon_mrc) if geom.recon_mrc else None,
        "source_label": geom.source,
        "sources": sources,
        "picks": picks,
        "features": features,
        "ready": ready,
    }


def collect_rows(project_state, project_path: Path, tilt_series: list) -> tuple[list[dict], list[dict], list[dict]]:
    """``(rows, pick layers, feature layers)``: one row per tomogram, ordered by acquisition
    position, and the layers its ``picks`` and ``features`` entries name (`_pick_layers`,
    `feature_layers.collect_layers`, the latter read from the registry's `tilt_series`). Disk-only — safe to call
    in a thread, which is where the page calls it from."""
    geoms = all_geometries(project_state, project_path)
    layers, picks_by_ts = _pick_layers(project_state, project_path, geoms)
    prep = source_prep(project_state, project_path, tilt_series)
    feature_layers, features_by_ts = fl.collect_layers(project_state, tilt_series, prep["dirs"])
    sources = sources_by_ts(project_state, geoms, prep)
    rows = [
        compose_row(
            g, sources[g.tomo_name], picks_by_ts.get(g.tomo_name, []), features_by_ts.get(g.tomo_name, []), ready=True
        )
        for g in geoms
    ]
    rows.sort(key=lambda r: (r["order"], r["ts"]))
    return rows, layers, feature_layers


# ---------------------------------------------------------------------------
# The page
# ---------------------------------------------------------------------------


class TomoGalleryPage:
    """The wall, swapped into the workspace main area like the Journey.

    Built once (lazily, on the first click of the Tomograms icon) and refreshed on
    demand: collection touches disk for every tomogram, so the toolbar switches
    re-render from the cached rows and only an explicit refresh — or landing on the
    view again after a background render finished — re-collects."""

    def __init__(self, container, project_path: Path, callbacks: dict | None = None) -> None:
        self.container = container
        self.project_path = Path(project_path)
        self.callbacks = callbacks or {}
        self.rows: list[dict] = []
        self.source: str = RECON_SOURCE
        self.size: str = DEFAULT_SIZE
        # Pick overlays (`_pick_layers`) and segmentation overlays (`feature_layers.collect_layers`). A layer
        # follows the version, showing on the sources of the reconstruction it was made on, unless switched
        # by hand (`services.segmentation.display`, shared with the Journey and the pick viewer). The
        # segmentations' colours and opacity are CSS variables on the page and the zoom (`_feature_vars`):
        # changing them repaints no tile.
        self.layers: list[dict] = []
        self._layer_by_id: dict[str, dict] = {}
        # source key -> {recon, group, variant}: the reconstruct instance it shows, and its place in the version menu.
        self._source_meta: dict[str, dict] = {}
        self.feature_layers: list[dict] = []
        self._feature_layer_by_id: dict[str, dict] = {}
        # Live layers lists, "wall" (the title line's menu) and "zoom": (body, source getter).
        self._layer_menus: dict[str, tuple[Any, Any]] = {}
        self._overlays: dict[str, Any] = {}  # each tile's feature fills and dot layers
        self._overlay_keys: dict[str, tuple] = {}  # the layer ids each tile's overlay draws
        self._imgs: dict[str, Any] = {}  # each tile's <img>, re-pointed on a source switch
        self._prefetched: set[str] = set()
        # The selected tilt series, shared with the Journey (the workspace's `select_ts`): whichever view moves
        # it, the other follows.
        selected = self.callbacks.get("selected_ts")
        self.selected_ts: str | None = selected() if selected is not None else None
        self.tab: str = TOMOGRAMS_TAB
        self._tab_chosen = False
        # Tilt previews (ui/tilt_previews.py): one group per tilt series.
        self.tilt_groups: list[dict] = []
        self.tilt_facts: dict = {}  # the project-wide facts collect_tilt_groups returns
        self.tilt_error: str | None = None
        self.tilt_awaiting = 0  # tilts with an average but no preview PNG yet
        self._png_dir: Path | None = None
        self._thumbs_live = False  # the thumbnail pass was running at the last collection
        self._group_by_ts: dict[str, dict] = {}
        self._tilt_by_key: dict[str, dict] = {}  # tilts that show: a preview or an average
        self._frame_keys: set[str] = set()  # every tilt, as the header strips draw them
        # The Tilts tab's gallery; it keeps its open groups, sort, show and metrics across renders. A click on a
        # series selects it.
        self.tilt_gallery = TiltGallery(self.project_path, size=self.size, on_select=self._select)
        self.tilt_gallery.selected = self.selected_ts
        # Kept on the page, so a size switch, a Refresh or the pending poll keeps them.
        self.open_mosaics: set[str] = set()  # tiles showing their tilts instead of the slice
        self._tiles: dict[str, Any] = {}
        self._pictures: dict[str, Any] = {}  # each tile's image-or-note, swapped on a source switch
        self._zoom_ts: str | None = None  # the tomogram the zoom dialog shows
        self._zoom_source: str = RECON_SOURCE  # the zoom's own version, the wall's when the zoom opens
        self._flight = SingleFlight()
        self._refs: dict[str, Any] = {}
        self._collected = False
        self._active = False
        self._skeleton = 0  # placeholder tiles the grid shows until the tomograms are listed
        # Bumped by every reload: a pass that finds it moved has been superseded and drops its result.
        self._gen = 0
        self._geom_by_ts: dict[str, TomoGeometry] = {}
        self._tilts_pending = False  # the tilts of the running reload have not landed yet
        self._kicks_left = _MAX_KICKS_PER_RENDER  # slab renders the running reload may still submit
        self._controls_key: tuple = ()  # what the title line's tools were last built from
        self._head: dict | None = None  # the view's title line (`callbacks["view_head"]`), filled on the first render
        # Slabs kicked off by this page land minutes later. Poll while the wall is the
        # visible view AND something is still pending — disk work, so 15 s (CLAUDE.md),
        # gated on a signature so a tick that changes nothing never touches the DOM.
        self._timer = owned_timer(15.0, self._tick, container)
        self._timer.deactivate()
        # The Journey moving the selection moves it here.
        self.callbacks.setdefault("follow_ts", {})["gallery"] = self._follow_ts

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def show(self) -> None:
        """Point the workspace at the wall. The first call collects (`reload`); later ones
        show what's cached (the pending-tile poll and the Refresh button bring in previews
        that landed meanwhile)."""
        async with self._flight("show") as acquired:
            if not acquired:
                return
            if self._collected:
                return
            await self.reload()

    async def reload(self) -> None:
        """Re-read the wall. The tiles show as soon as the tomograms are known, each its frame
        and label; their pictures fill in a batch at a time, the pick and segmentation layers
        and the tilts as each pass lands."""
        self._gen += 1
        gen = self._gen
        self._collected = False
        self._skeleton = len(self.rows) or _SKELETON_TILES
        self.rows = []
        self._tilts_pending = True
        self.render()
        await asyncio.sleep(0.03)  # flush the skeleton before the disk pass
        started = time.monotonic()
        state = get_project_state_for(self.project_path)
        tilt_series, self.tilt_error = registry_tilt_series(self.project_path)
        # The tilt filter's review is live job state: snapshot it here, derive in the thread.
        ctx = tilt_context(state)
        png_dir = self._png_dir = tilt_thumb_dir(state, self.project_path)
        geoms = await asyncio.to_thread(all_geometries, state, self.project_path)
        if self._stale(gen):
            return
        # In wall order, so the tiles fill in from the top.
        geoms.sort(key=lambda g: (position_label(g.tomo_name)[1], g.tomo_name))
        self._geom_by_ts = {g.tomo_name: g for g in geoms}
        self._skeleton = 0
        self.rows = [compose_row(g, {}, [], [], ready=False) for g in geoms]
        self._source_meta = {}
        self.tilt_groups, self.tilt_facts = [], {}
        self._group_by_ts, self._tilt_by_key, self._frame_keys = {}, {}, set()
        self._tilts_pending = True
        self._kicks_left = _MAX_KICKS_PER_RENDER
        if not self._tab_chosen and not self.rows:
            # A project with tilts and no tomograms opens on Tilts; which it is waits for the tilts.
            groups, facts = await asyncio.to_thread(collect_tilt_groups, tilt_series, png_dir, ctx)
            if self._stale(gen):
                return
            self._apply_tilts(state, groups, facts)
        self.render()
        shown = time.monotonic()
        await asyncio.gather(
            self._fill_sources(gen, state, geoms, tilt_series),
            self._fill_layers(gen, state, geoms, tilt_series),
            self._fill_tilts(gen, state, tilt_series, png_dir, ctx),
        )
        if self._stale(gen):
            return
        self._collected = True
        with self.container:
            self._prefetch_other_sources()
        logger.info(
            "%s · tomograms: wall shown in %.1f s, filled in %.1f s",
            self.project_path.name,
            shown - started,
            time.monotonic() - started,
        )

    def _stale(self, gen: int) -> bool:
        """A newer reload started, or the page went away, while this one awaited."""
        return gen != self._gen or self.container.is_deleted

    async def _fill_sources(self, gen: int, state, geoms: list[TomoGeometry], tilt_series: list) -> None:
        """The tiles' pictures, a batch of tomograms at a time, each batch filled in as it lands."""
        prep = await asyncio.to_thread(source_prep, state, self.project_path, tilt_series)
        for i in range(0, len(geoms), _FILL_CHUNK):
            sources = await asyncio.to_thread(sources_by_ts, state, geoms[i : i + _FILL_CHUNK], prep)
            if self._stale(gen):
                return
            # A task of its own (gather) starts with no NiceGUI slot; the wall's container gives it the client.
            with self.container:
                self._land_sources(sources)

    async def _fill_layers(self, gen: int, state, geoms: list[TomoGeometry], tilt_series: list) -> None:
        dirs = job_dirs(state)
        (layers, picks_by_ts), (feature_layers, features_by_ts) = await asyncio.to_thread(
            lambda: (_pick_layers(state, self.project_path, geoms), fl.collect_layers(state, tilt_series, dirs))
        )
        if self._stale(gen):
            return
        with self.container:
            self._land_layers(layers, picks_by_ts, feature_layers, features_by_ts)

    async def _fill_tilts(self, gen: int, state, tilt_series: list, png_dir: Path, ctx: dict) -> None:
        if not self._tilts_pending:
            return  # read before the first render
        groups, facts = await asyncio.to_thread(collect_tilt_groups, tilt_series, png_dir, ctx)
        if self._stale(gen):
            return
        with self.container:
            self._apply_tilts(state, groups, facts)
            self._land_tilts()

    def _land_sources(self, sources: dict[str, dict]) -> None:
        for row in self.rows:
            if row["ts"] in sources:
                row["sources"] = sources[row["ts"]]
                row["ready"] = True
        for found in sources.values():
            self._source_meta.update(_meta_of(found))
        before = self.source
        self._sync_controls()
        # The first batch can move the wall's source (the toolbar picks the first one there is):
        # the tiles filled before it follow.
        ready = [r["ts"] for r in self.rows if r["ready"]] if self.source != before else list(sources)
        for ts in ready:
            self._refill(ts)

    def _land_layers(self, layers: list[dict], picks_by_ts: dict, feature_layers: list[dict], features_by_ts: dict):
        self._apply_layers(layers, feature_layers)
        reshaped = []
        for row in self.rows:
            ts = row["ts"]
            row["picks"] = picks_by_ts.get(ts, [])
            row["features"] = features_by_ts.get(ts, [])
            aspect = _row_aspect(self._geom_by_ts[ts], row["picks"])
            if aspect != row["aspect"]:
                row["aspect"] = aspect
                reshaped.append(ts)
        self._apply_feature_vars()
        self._sync_controls()
        for ts in reshaped:
            self._refill(ts)
        self._refresh_overlays()

    def _land_tilts(self) -> None:
        tabs = self._refs.get("seg_tab")
        if tabs is not None:
            tabs.set_badge(TILTS_TAB, str(len(self._tilt_by_key)))
        self._sync_controls()
        if self.tab == TILTS_TAB:
            self._rerender_body()
            return
        # Mosaic toggles; a tile still reading gets its toggle with its picture.
        for row in self.rows:
            if row["ready"]:
                self._refill(row["ts"])

    def _refill(self, ts: str) -> None:
        """Rebuild one tile in place (the grid and the other tiles stay)."""
        tile = self._tiles.get(ts)
        row = next((r for r in self.rows if r["ts"] == ts), None)
        if tile is None or tile.is_deleted or row is None:
            return
        tile.clear()
        with tile:
            self._kicks_left -= self._fill_tile(row, kicked=self._kicks_left > 0)

    async def _collect(self, gen: int) -> None:
        """Everything at once, for the pending-preview poll (which re-renders only on a change)."""
        # Explicit-path resolution, not the tab accessor: this runs from a timer tick,
        # where a bare lookup can hand back a blank throwaway state.
        state = get_project_state_for(self.project_path)
        tilt_series, tilt_error = registry_tilt_series(self.project_path)
        ctx = tilt_context(state)
        png_dir = self._png_dir = tilt_thumb_dir(state, self.project_path)
        (rows, layers, feature_layers), (groups, facts) = await asyncio.to_thread(
            lambda: (
                collect_rows(state, self.project_path, tilt_series),
                collect_tilt_groups(tilt_series, png_dir, ctx),
            )
        )
        if self._stale(gen):
            return
        self.rows, self.tilt_error = rows, tilt_error
        self._apply_layers(layers, feature_layers)
        self._source_meta = {}
        for row in self.rows:
            self._source_meta.update(_meta_of(row["sources"]))
        self._apply_tilts(state, groups, facts)

    def _apply_layers(self, layers: list[dict], feature_layers: list[dict]) -> None:
        # Colors picked in this session outlive a re-collection.
        for layer in layers:
            if layer["id"] in self._layer_by_id:
                layer["color"] = self._layer_by_id[layer["id"]]["color"]
        self.layers = layers
        self._layer_by_id = {layer["id"]: layer for layer in layers}
        self.feature_layers = feature_layers
        self._feature_layer_by_id = {layer["id"]: layer for layer in feature_layers}

    def _apply_tilts(self, state, groups: list[dict], facts: dict) -> None:
        self.tilt_groups, self.tilt_facts = groups, facts
        self._tilts_pending = False
        self._group_by_ts = {g["ts"]: g for g in self.tilt_groups}
        self._tilt_by_key = {t["key"]: t for g in self.tilt_groups for t in g["tilts"]}
        self._frame_keys = {s.frame_id for g in self.tilt_groups for s, _title in g["ticks"]}
        self.tilt_gallery.set_data(self.tilt_groups, self.tilt_facts)
        self.tilt_awaiting = sum(1 for t in self._tilt_by_key.values() if t["png"] is None and t["mrc"])
        if self.tilt_awaiting and not any(g["n_png"] for g in self.tilt_groups):
            # No previews at all: the pass that makes them normally starts when fsMotion lands.
            ensure_tilt_thumbnails(self.project_path, state)
        self._thumbs_live = self._thumbs_running()
        if not self._tab_chosen:
            self._tab_chosen = True
            if not self.rows and self.tilt_groups:
                self.tab = TILTS_TAB

    async def _tick(self) -> None:
        if not self._active or not self._collected or not self._pending_count():
            return
        gen = self._gen
        before = self._grid_signature()
        await self._collect(gen)
        if not self._stale(gen) and self._grid_signature() != before:
            self.render()

    def invalidate(self) -> None:
        """Drop the cached rows so the next visit re-collects.

        ``show()`` is a no-op once the wall has been collected — which is right for a
        view swap, and wrong after something ADDS tomograms to the project. A tomogram
        import while the wall was already built would otherwise leave the user staring
        at the pre-import wall with no hint that it is stale."""
        self._collected = False

    def set_active(self, on: bool) -> None:
        """The workspace's _apply_views hook: the pending-preview poll runs only while
        the wall is on screen. On screen (again, or narrowed by a view opening beside it), the wall
        brings the selection into view."""
        self._active = on
        try:
            self._timer.activate() if on else self._timer.deactivate()
        except Exception:
            pass  # timer torn down with the client
        if on:
            self._scroll_to_selected()

    # ── Derived state ─────────────────────────────────────────────────────────

    def _shows(self, source: str) -> str | None:
        """The reconstruct instance `source` shows (a denoised version: the one it was denoised from)."""
        return self._source_meta.get(source, {}).get("recon")

    def _layer_shown(self, layer: dict, source: str) -> bool:
        """Whether a layer (picks, or a segmentation) draws over `source`: switched by hand, or following
        the version, made on the reconstruction it shows (or either side's reconstruction not known)."""
        following = display.on_source(layer["recon"], self._shows(source))
        return display.is_shown(self.project_path, layer["id"], following)

    def _visible_picks(self, row: dict, source: str) -> list[dict]:
        """The row's pick entries that draw over `source`, each with its layer's meta."""
        out = []
        for entry in row["picks"]:
            layer = self._layer_by_id.get(entry["id"])
            if layer is not None and self._layer_shown(layer, source):
                out.append({**layer, **entry})
        return out

    def _visible_features(self, row: dict, source: str) -> list[dict]:
        """The row's segmentation entries that fill over `source`."""
        return [
            entry
            for entry in row["features"]
            if (layer := self._feature_layer_by_id.get(entry["id"])) is not None and self._layer_shown(layer, source)
        ]

    def _pending_count(self) -> int:
        """Tiles whose current source has no PNG yet but has a volume to render from (a
        tile showing its tilts kicks no render), plus tilts waiting for a preview while
        the thumbnail pass runs, or ran at the last collection (its last batch)."""
        slabs = sum(
            1
            for row in self.rows
            if row["ts"] not in self.open_mosaics
            and (src := row["sources"].get(self.source))
            and src["png"] is None
            and src["kick"] is not None
        )
        tilts = self.tilt_awaiting if self._thumbs_live or self._thumbs_running() else 0
        return slabs + tilts

    def _thumbs_running(self) -> bool:
        from ui.background_task import BackgroundTask

        if self._png_dir is None:
            return False
        return BackgroundTask.existing(thumbnail_task_key(self.project_path, self._png_dir)) is not None

    def _grid_signature(self) -> tuple:
        """What the wall draws, cheaply: which tiles have an image and how many picks
        each carries, and how many tilts each series shows and has previews for. A poll
        that doesn't move this must not rebuild the DOM."""
        sig = []
        for row in self.rows:
            src = row["sources"].get(self.source)
            sig.append(
                (
                    row["ts"],
                    src is not None and src["png"] is not None,
                    tuple(p["n"] for p in row["picks"]),
                    tuple(f["id"] for f in row["features"]),
                )
            )
        tilts = tuple((g["ts"], len(g["tilts"]), g["n_png"]) for g in self.tilt_groups)
        return tuple(sig), tilts, self.tilt_error

    def _source_keys(self) -> list[str]:
        """Every source at least one tomogram can show, in the version menu's order: by reconstruction
        (in the order the tomograms list them, by job dir), each one's own picture first, then the
        versions denoised from it by name."""
        keys: list[str] = []
        for row in self.rows:
            for key in row["sources"]:
                if key not in keys:
                    keys.append(key)
        meta = self._source_meta
        groups: list[str] = []
        # The reconstructions' own pictures first, in the order listed: they order the groups.
        for key in sorted(keys, key=lambda k: not k.startswith(RECON_SOURCE)):
            group = meta.get(key, {}).get("group", "")
            if group not in groups:
                groups.append(group)
        return sorted(
            keys,
            key=lambda k: (groups.index(meta.get(k, {}).get("group", "")), not k.startswith(RECON_SOURCE), k.lower()),
        )

    def _source_groups(self) -> set[str]:
        return {self._source_meta.get(k, {}).get("group", "") for k in self._source_keys()}

    def _source_text(self, key: str) -> str:
        """A version's name on the tool: its picture (`raw`, a method), after its reconstruction when
        the project has several."""
        meta = self._source_meta.get(key)
        if meta is None:
            return key
        return meta["variant"] if len(self._source_groups()) <= 1 else f"{meta['group']} · {meta['variant']}"

    def _source_choices(self, row: dict | None = None) -> list[Choice]:
        """The version menu's rows, grouped by reconstruction. On the wall, a version only some
        tomograms have says how many; for one tomogram (`row`, the zoom), a version it lacks is grey."""
        keys = self._source_keys()
        named: dict[tuple, int] = {}
        for k in keys:
            meta = self._source_meta.get(k, {})
            named[(meta.get("group"), meta.get("variant"))] = named.get((meta.get("group"), meta.get("variant")), 0) + 1
        out = []
        for k in keys:
            meta = self._source_meta.get(k, {})
            # Two jobs of one method on one reconstruction keep the key, which names the instance.
            label = meta.get("variant", k) if named[(meta.get("group"), meta.get("variant"))] == 1 else k
            if row is None:
                have = sum(1 for r in self.rows if k in r["sources"])
                note = f"{have}/{len(self.rows)}" if have < len(self.rows) else ""
                out.append(Choice(k, label, meta.get("group", ""), note))
            else:
                src = row["sources"].get(k)
                tip = src["caption"] if src else "This version has no volume of this tomogram"
                out.append(Choice(k, label, meta.get("group", ""), enabled=src is not None, tip=tip))
        return out

    # ── Render ────────────────────────────────────────────────────────────────

    def render(self) -> None:
        ensure_assets_loaded()
        self._render_head()
        tabs = self._refs["seg_tab"]
        tabs.set_badge(TOMOGRAMS_TAB, str(len(self.rows)))
        tabs.set_badge(TILTS_TAB, "" if self._tilts_pending else str(len(self._tilt_by_key)))
        self._settle_source()
        self.container.clear()
        with self.container, ui.element("div").classes("cb-gal-page").style(self._feature_vars()) as page:
            self._refs["page"] = page
            self._refs["tilt_strip"] = ui.element("div").classes("cb-gal-toolbar")
            body = ui.element("div").classes("cb-gal-body")
            self._refs["body"] = body
            with body:
                self._render_body()
        self._render_tilt_strip()
        self._sync_controls()
        # A new page starts at the top: the selection is brought back into view.
        self._scroll_to_selected()

    def _render_body(self) -> None:
        if self.tab == TILTS_TAB:
            self._render_tilts()
        else:
            self._render_grid()

    def _rerender_body(self) -> None:
        body = self._refs.get("body")
        if body is None:
            return
        body.clear()
        with body:
            self._render_body()
        self._scroll_to_selected()

    def _render_head(self) -> None:
        """The view's title line, built once: the Tomograms / Tilts switch in place of its name, and at
        its right end the wall's tools (`_render_wall_tools`), the size and Refresh."""
        if self._head is not None:
            return
        head = self._head = self.callbacks["view_head"]("gallery")
        head["title"].set_visibility(False)
        with head["lead"]:
            self._refs["seg_tab"] = render_segmented(
                [(TOMOGRAMS_TAB, "Tomograms"), (TILTS_TAB, "Tilts")], self.tab, self._select_tab, classes="cb-seg-head"
            )
        with head["tools"]:
            self._refs["tab_controls"] = ui.element("div").classes("cb-head-tools")
            self._refs["seg_size"] = render_segmented(
                [(k, lbl) for k, lbl, _w in _SIZES], self.size, self._select_size, classes="cb-seg-sm"
            )
            icon_button("refresh", self._reload_click, tooltip="Re-read the tomograms, tilts and previews from disk")

    def _controls_signature(self) -> tuple:
        return (
            self.tab,
            tuple(self._source_keys()),
            tuple(layer["id"] for layer in self.layers),
            tuple(layer["id"] for layer in self.feature_layers),
            bool(self.rows),
            bool(self.tilt_groups),
        )

    def _settle_source(self) -> None:
        """A source gone from the project (or not landed yet) gives way to the first there is."""
        keys = self._source_keys()
        if keys and self.source not in keys:
            self.source = keys[0]

    def _sync_controls(self) -> None:
        """Rebuild the title line's tools and the Tilts strip when what they offer changed (a source,
        a layer or the tilts landed, the tab switched), and only then."""
        self._settle_source()
        key = self._controls_signature()
        if key == self._controls_key:
            return
        self._controls_key = key
        self._render_wall_tools()
        self._render_tilt_strip()

    def _render_tilt_strip(self) -> None:
        """The Tilts tab's controls (sort, show, group, metrics, the legend) on a strip under the title line."""
        strip = self._refs.get("tilt_strip")
        if strip is None or strip.is_deleted:
            return
        strip.clear()
        on = self.tab == TILTS_TAB and bool(self.tilt_groups)
        strip.set_visibility(on)
        if on:
            with strip:
                self.tilt_gallery.render_controls()

    def _render_wall_tools(self) -> None:
        """The Tomograms tab's tools on the title line: the version every tile shows and the layers (the
        picks and the segmentations), each a glyph whose dropdown opens under the pointer. The Tilts tab
        has none there."""
        tools = self._refs.get("tab_controls")
        if tools is None or tools.is_deleted:
            return
        tools.clear()
        self._layer_menus.pop("wall", None)
        for ref in ("source_rows", "source_text", "layers_menu"):
            self._refs.pop(ref, None)
        if self.tab != TOMOGRAMS_TAB:
            return
        with tools:
            if len(self._source_keys()) > 1:
                self._render_source_tool()
            if self.rows:
                # Without a layer yet it still holds the import.
                self._render_layers_tool()

    def _render_source_tool(self) -> None:
        trigger, label = tool_trigger("filter_none", self._source_text(self.source))
        self._refs["source_text"] = label
        _menu, body = hover_menu(trigger, auto_close=True)
        with body:
            ui.label("Version on every tile").classes("cb-hmenu-title")
            self._refs["source_rows"] = ChoiceRows(self._source_choices(), self.source, self._select_source)

    def _render_layers_tool(self) -> None:
        trigger, _ = tool_trigger("layers.svg")
        menu, body = hover_menu(trigger, align="right")
        self._refs["layers_menu"] = menu
        self._layer_menus["wall"] = (body, lambda: self.source)
        self._refresh_layer_lists()

    # ── Layers ────────────────────────────────────────────────────────────────

    def _feature_vars(self) -> str:
        """The CSS variables the segmentation fills and badges read (`feature_layers.css_vars`). Set on
        the page and on the zoom, which the dialog puts outside the page."""
        return fl.css_vars(self.project_path, (layer["feature"] for layer in self.feature_layers))

    def _apply_feature_vars(self) -> None:
        style = self._feature_vars()
        for key in ("page", "zoom_body"):
            el = self._refs.get(key)
            if el is not None and not el.is_deleted:
                el.style(style)

    def _refresh_layer_lists(self) -> None:
        """Repaint every live layers list (the title line's menu, with the import; the zoom's side column);
        they are a few rows each."""
        for name, (body, source_of) in list(self._layer_menus.items()):
            if body.is_deleted:
                del self._layer_menus[name]
                continue
            row = self._zoom_row() if name == "zoom" else None
            if name == "zoom" and row is None:
                continue
            body.clear()
            with body:
                if name == "wall":
                    ui.label("Layers on the wall").classes("cb-hmenu-title")
                self._render_layer_list(source_of(), row)
                if name == "wall":
                    self._render_import_row()

    def _render_import_row(self) -> None:
        item = ui.element("div").classes("cb-hmenu-item cb-hmenu-action")
        item.props["title"] = (
            "Register volumes segmented outside crboost (MemBrain, easymode or Ais run by hand) on these tomograms"
        )
        item.on("click", lambda _e: self._import_from_menu())
        with item:
            ui.icon("add", size="12px")
            ui.label("Import segmentations…")

    def _import_from_menu(self) -> None:
        menu = self._refs.get("layers_menu")
        if menu is not None and not menu.is_deleted:
            menu.close()
        self._import_segmentations()

    def _render_layer_list(self, source: str, row: dict | None = None) -> None:
        """The layers (`layer_menu.render_layer_list`) read against `source`: the pick layers, by species,
        and the segmentations. On the wall a count is a layer's picks, or the tomograms a segmentation
        covers; for one tomogram (`row`, the zoom), the layers on it, with its picks of each and each
        feature's share of its volume."""
        picks_n = {e["id"]: e["n"] for e in row["picks"]} if row is not None else None
        fractions = {e["id"]: e["fraction"] for e in row["features"]} if row is not None else None
        picks = [
            LayerRow(
                id=layer["id"],
                name=layer["label"],
                count=str(layer["n"] if picks_n is None else picks_n[layer["id"]]),
                color=layer["color"],
                recon=layer["recon"],
                recon_label=layer["recon_label"],
                group=layer["species"],
                on_color=lambda c, lid=layer["id"]: self._set_color(lid, c),
            )
            for layer in self.layers
            if picks_n is None or layer["id"] in picks_n
        ]
        features = fl.feature_rows(
            self.project_path,
            [layer for layer in self.feature_layers if fractions is None or layer["id"] in fractions],
            lambda layer: str(layer["n"]) if fractions is None else fl.fraction_text(fractions[layer["id"]]),
            self._layers_changed,
        )
        render_layer_list(self.project_path, picks, features, shows=self._shows(source), on_change=self._layers_changed)

    def _set_color(self, layer_id: str, color: str) -> None:
        layer = self._layer_by_id.get(layer_id)
        if layer is None or layer["color"] == color:
            return
        layer["color"] = color
        self._layers_changed()

    def _layers_changed(self) -> None:
        """A layer shown or hidden, a colour picked, the opacity switched."""
        self._apply_feature_vars()
        self._refresh_layer_lists()
        self._refresh_overlays()
        if self._zoom_ts is not None and self._refs.get("zoom") is not None and self._refs["zoom"].value:
            self._render_zoom_overlay()

    def _render_grid(self) -> None:
        if not self.rows:
            if self._skeleton:
                self._render_skeleton()
            else:
                self._render_empty()
            return
        kicks = 0
        self._tiles, self._pictures, self._imgs, self._overlays, self._overlay_keys = {}, {}, {}, {}, {}
        grid = ui.element("div").classes("cb-gal-grid").style(self._grid_columns())
        self._refs["grid"] = grid
        with grid:
            for row in self.rows:
                kicks += self._render_tile(row, kicked=kicks < _MAX_KICKS_PER_RENDER)
        self._prefetch_other_sources()

    def _grid_columns(self) -> str:
        width = next(w for k, _lbl, w in _SIZES if k == self.size)
        return f"grid-template-columns: repeat(auto-fill, minmax({width}px, 1fr));"

    def _render_skeleton(self) -> None:
        """Placeholder tiles while the tomograms are listed: a breathing square frame and a caption bar."""
        with ui.element("div").classes("cb-gal-grid").style(self._grid_columns()):
            for _ in range(self._skeleton):
                with ui.element("div").classes("cb-gal-tile"):
                    with ui.element("div").classes("cb-gal-frame").style("aspect-ratio: 1/1; cursor: default;"):
                        self._render_wait_note()
                    with ui.element("div").classes("cb-gal-cap").style("cursor: default;"):
                        ui.element("div").style(
                            "width: 45%; height: 8px; margin: 3.5px 0; border-radius: 2px; background: #e2e8f0;"
                        )

    def _render_empty(self) -> None:
        with ui.element("div").classes("cb-empty"):
            ui.icon("grid_view", size="40px").classes("text-gray-400")
            ui.label("No tomograms yet.").classes("text-sm text-gray-500")
            ui.label(
                "Run Reconstruct on a tilt-series, or import reconstructed volumes from the "
                "PARTICLES header of the job roster — both land here."
            ).classes("text-[11px] italic text-gray-400 text-center").style("max-width: 460px;")

    def _render_tile(self, row: dict, *, kicked: bool) -> int:
        """One tile. Returns 1 when it submitted a slab render, so the caller can cap
        how many a single pass kicks off."""
        tile = ui.element("div").classes("cb-gal-tile" + (" selected" if row["ts"] == self.selected_ts else ""))
        self._tiles[row["ts"]] = tile
        with tile:
            return self._fill_tile(row, kicked=kicked)

    def _fill_tile(self, row: dict, *, kicked: bool) -> int:
        """A tile's frame and caption row: the slice, or, toggled, a mosaic of the series'
        tilts in the same box. The caption row is the position label and at its end the Journey
        glyph; a click on it selects the tile."""
        group = self._group_by_ts.get(row["ts"])
        spent = 0
        if group is not None and row["ts"] in self.open_mosaics:
            self._render_mosaic_frame(row, group)
        else:
            spent = self._render_slice_frame(row, group, kicked=kicked)
        cap = ui.element("div").classes("cb-gal-cap")
        cap.on("click", lambda _e, t=row["ts"]: self._select(t))
        with cap:
            ui.label(row["label"]).classes("cb-gal-name").tooltip(row["ts"])
            journey = ui.html(load_icon_svg("journey.svg", size=11), sanitize=False).classes("cb-gal-jlink")
            journey.props["title"] = "Open this tilt series in the Journey, beside the wall"
            journey.on("click.stop", lambda _e, t=row["ts"]: self._open_journey(t))
        return spent

    def _render_slice_frame(self, row: dict, group: dict | None, *, kicked: bool) -> int:
        src = row["sources"].get(self.source)
        frame = ui.element("div").classes("cb-gal-frame" + ("" if row["aspect"] else " cb-gal-frame-auto"))
        if row["aspect"]:
            frame.style(f"aspect-ratio: {row['aspect'][0]}/{row['aspect'][1]};")
        frame.on("click", lambda _e, r=row: self._click_tile(r))
        with frame:
            # `display: contents`: a box of its own would come between the frame and the image.
            picture = ui.element("div").style("display: contents;")
            self._pictures[row["ts"]] = picture
            with picture:
                spent = self._render_picture(row, src, kicked=kicked)
            overlay = ui.element("div").style("display: contents;")
            self._overlays[row["ts"]] = overlay
            with overlay:
                self._render_tile_overlay(row)
            if group is not None:
                self._render_mosaic_toggle(row["ts"], len(group["tilts"]), on=False)
        return spent

    def _render_picture(self, row: dict, src: dict | None, *, kicked: bool) -> int:
        """What a tile shows for one source: its image (the thumbnail), or a note saying why there
        is none. Returns 1 when it submitted a slab render."""
        spent = 0
        self._imgs.pop(row["ts"], None)
        if src is None and not row["ready"]:
            self._render_wait_note()
        elif src is None:
            self._render_frame_note("not produced by this source")
        elif src["png"] is not None:
            img = ui.element("img").classes("cb-gal-img")
            img.props.update(src=src["thumb"], alt=row["ts"], loading="lazy", decoding="async")
            self._imgs[row["ts"]] = img
        elif src["kick"] is not None:
            if kicked:
                spent = 1
                self._kick_slab(row, src)
            self._render_frame_note("rendering slice…", spinner=True, wait=True)
        else:
            self._render_frame_note("no volume on disk")
        return spent

    def _overlay_key(self, row: dict, source: str) -> tuple:
        """What a tile's overlay draws over `source`: its image or not, the pick layers with their
        colors, and the segmentation layers (their colours are CSS variables, so not part of it)."""
        has_png = _has_png(row["sources"].get(source))
        picks = tuple((p["id"], p["color"]) for p in self._visible_picks(row, source))
        return has_png, picks, tuple(f["id"] for f in self._visible_features(row, source))

    def _render_tile_overlay(self, row: dict) -> None:
        """A tile's segmentation fills and dot layers, over an image only, for the wall's source."""
        key = self._overlay_key(row, self.source)
        self._overlay_keys[row["ts"]] = key
        if key[0]:
            self._render_feature_fills(row, self.source)
            self._render_dot_layers(row, self._visible_picks(row, self.source), _DOT_R_WALL)

    def _render_feature_fills(self, row: dict, source: str) -> None:
        """One fill per segmentation layer over the frame, beneath the dots. Without the tomogram's
        extent the frame is not its box, so nothing is drawn (as for the dots)."""
        if not row["aspect"]:
            return
        for f in self._visible_features(row, source):
            ui.element("div").style(fl.fill_style(f["feature"], f["xy"]))

    def _refresh_overlays(self) -> None:
        """Repaint the overlay of each tile whose drawn layers changed (a source switch while
        following the source, a layer switched, a color picked)."""
        for row in self.rows:
            ts = row["ts"]
            overlay = self._overlays.get(ts)
            if overlay is None or overlay.is_deleted or ts in self.open_mosaics:
                continue
            if self._overlay_keys.get(ts) == self._overlay_key(row, self.source):
                continue
            overlay.clear()
            with overlay:
                self._render_tile_overlay(row)

    def _prefetch_other_sources(self) -> None:
        """Have the browser fetch every other source's tile thumbnails while idle, so a source
        switch shows pictures it already holds instead of pulling a wall of them."""
        urls = []
        for row in self.rows:
            for key, src in row["sources"].items():
                url = src.get("thumb")
                if key != self.source and url and url not in self._prefetched:
                    urls.append(url)
                    self._prefetched.add(url)
        if urls:
            ui.run_javascript(
                "(window.requestIdleCallback || setTimeout)(() => { for (const u of "
                f"{json.dumps(urls)}) {{ const i = new Image(); i.decoding = 'async'; i.src = u; }} }});"
            )

    def _render_mosaic_frame(self, row: dict, group: dict) -> None:
        """The series' tilts in the box the slice occupies, with no zoom click or dots. A tilt
        selects the tile and opens the full-size tilt viewer."""
        aspect = row["aspect"]
        cells, own_aspect = mosaic_html(group["tilts"], aspect[0] / max(aspect[1], 1) if aspect else None)
        ratio = f"{aspect[0]}/{aspect[1]}" if aspect else own_aspect
        frame = ui.element("div").classes("cb-gal-frame").style(f"aspect-ratio: {ratio}; cursor: default;")
        with frame:
            mosaic = ui.html(cells, sanitize=False).style("position: absolute; inset: 0;")
            mosaic.on("click", handler=lambda e, t=row["ts"]: self._on_tilt_click(e, t), js_handler=TILT_CLICK_JS)
            self._render_mosaic_toggle(row["ts"], len(group["tilts"]), on=True)

    def _render_mosaic_toggle(self, ts: str, n: int, *, on: bool) -> None:
        """The top-left corner switch between a tile's slice and its tilts."""
        toggle = (
            ui.element("div")
            .classes("cb-gal-mosaic-toggle" + (" on" if on else ""))
            .style(
                "position: absolute; top: 3px; left: 3px; z-index: 2; width: 18px; height: 18px; "
                "border-radius: 3px; display: flex; align-items: center; justify-content: center; cursor: pointer;"
            )
        )
        # .stop: in slice mode the toggle sits inside the frame, whose click zooms.
        toggle.on("click.stop", lambda _e, t=ts: self._toggle_mosaic(t))
        toggle.tooltip(
            "Back to the tomogram's slice" if on else f"Show this tilt series' {n} tilts in place of the slice"
        )
        with toggle:
            ui.html(_MOSAIC_ICON, sanitize=False)

    def _render_pick_badge(self, visible: list[dict]) -> None:
        """Pick count in the zoom's top-right corner. Counted-but-not-drawn is stated, not
        rounded away: without the tomogram's extent (or with picks written against a
        different one) there is no honest place to put a dot, and a bare count over an
        empty slice would read as a rendering bug."""
        total = sum(sp["n"] for sp in visible)
        if not total:
            return
        unplaced = sum(sp["n"] for sp in visible if not sp["dots"])
        if not unplaced:
            ui.label(str(total)).classes("cb-gal-badge").tooltip(
                " · ".join(f"{sp['species']} {sp['label']}: {sp['n']}" for sp in visible if sp["n"])
            )
            return
        ui.label(f"{total} ⚠").classes("cb-gal-badge cb-gal-badge-warn").tooltip(
            f"{unplaced} of {total} picks can't be placed on this slice — the tomogram's binned extent "
            "is unresolved (no volume on disk and no size in tomograms.star), so no dot position is "
            "trustworthy. Open the Journey's Reconstruct card for this tilt-series."
        )

    def _render_frame_note(self, text: str, *, spinner: bool = False, wait: bool = False) -> None:
        """`wait`: the picture is on its way, so the frame shows the loading look (.cb-gal-note-wait)."""
        with ui.element("div").classes("cb-gal-note" + (" cb-gal-note-wait" if wait else "")):
            if spinner:
                ui.spinner(size="18px", color="indigo-400")
            ui.label(text)

    def _render_wait_note(self) -> None:
        """A frame still reading: the loading look and a small spinner, no words."""
        with ui.element("div").classes("cb-gal-note cb-gal-note-wait"):
            ui.html(WAIT_SPINNER, sanitize=False)

    def _render_dot_layers(
        self, row: dict, visible: list[dict], radius: float, *, cap: int | None = _WALL_DOTS
    ) -> None:
        # No frame aspect means the frame box isn't the tomogram box, so a percentage
        # position in it means nothing. The badge says so; nothing is drawn.
        if not row["aspect"]:
            return
        for sp in visible:
            dots = _stride_sample(sp["dots"], cap) if cap else sp["dots"]
            if not dots:
                continue
            ui.html(_dots_svg(dots, sp["color"], radius), sanitize=False).classes("cb-gal-dotlayer")

    # ── Tilts tab ─────────────────────────────────────────────────────────────

    def _render_tilts(self) -> None:
        """Every tilt preview, one collapsible group per tilt series (TiltGallery), under the
        notes only this page knows: the registry error and the previews still being made."""
        if self._tilts_pending:
            with ui.element("div").classes("cb-empty"):
                ui.spinner(size="lg", color="indigo")
                ui.label("Reading tilts…").classes("text-xs")
            return
        notes: list[tuple[str, str, bool]] = []
        if self.tilt_error:
            notes.append((self.tilt_error, "#b45309", False))
        if self.tilt_awaiting:
            running = self._thumbs_running()
            tail = "; they are being rendered in the background." if running else "."
            notes.append((f"{self.tilt_awaiting} tilts have no preview image yet{tail}", "#94a3b8", running))
        self.tilt_gallery.render(notes)

    # ── Selection ─────────────────────────────────────────────────────────────

    def _click_tile(self, row: dict) -> None:
        """A click on a tile's slice selects the tile; on the selected tile it opens the zoom."""
        if row["ts"] == self.selected_ts:
            self._zoom(row)
        else:
            self._select(row["ts"])

    def _select(self, ts: str, *, scroll: bool = False) -> None:
        """Make `ts` the selection, which the Journey follows. `scroll`: bring it into view (the zoom's
        steps; a click is on what is on screen)."""
        self._mark_selected(ts)
        publish = self.callbacks.get("select_ts")
        if publish is not None:
            publish(ts, "gallery")
        if scroll:
            self._scroll_to_selected()

    def _follow_ts(self, ts: str) -> None:
        """The Journey moved the selection: mark it here and bring it into view."""
        self._mark_selected(ts)
        self._scroll_to_selected()

    def _mark_selected(self, ts: str) -> None:
        """Move the selection's mark: the ring on its tile, and its series' on the Tilts tab."""
        prev, self.selected_ts = self.selected_ts, ts
        if prev == ts:
            return
        for t, on in ((prev, False), (ts, True)):
            tile = self._tiles.get(t)
            if tile is None or tile.is_deleted:
                continue
            if on:
                tile.classes(add="selected")
            else:
                tile.classes(remove="selected")
        self.tilt_gallery.set_selected(ts)

    def _scroll_to_selected(self) -> None:
        """Bring the selection into view when it is out of it: its tile, or on the Tilts tab its series.
        With nothing of it on screen yet (the view hidden, the wall still being listed), the view turning
        on or the render that builds it does."""
        if self.selected_ts is None or not self._active:
            return
        if self.tab == TILTS_TAB:
            self.tilt_gallery.scroll_to_selected()
            return
        tile = self._tiles.get(self.selected_ts)
        if tile is not None and not tile.is_deleted:
            scroll_into_view(tile)

    # ── Actions ───────────────────────────────────────────────────────────────

    def _select_source(self, key: str) -> None:
        """Point every tile at another source. A tile with an image under both only re-points its
        <img> (whose thumbnail the browser prefetched); one gaining or losing its image swaps its
        picture slot. The dot layers repaint only where the layers drawn change. Nothing else of a
        tile is rebuilt: rebuilding the whole grid re-sent every tile's dot layers and froze the
        page."""
        self.source = key
        # The version menu moves its check, and the tool its name, only when told to (neither rebuilds).
        rows, label = self._refs.get("source_rows"), self._refs.get("source_text")
        if rows is not None:
            rows.set_active(key)
        if label is not None and not label.is_deleted:
            label.set_text(self._source_text(key))
        kicks = 0
        for row in self.rows:
            ts = row["ts"]
            picture = self._pictures.get(ts)
            if picture is None or picture.is_deleted or ts in self.open_mosaics:
                continue  # a tile showing its tilts draws the current source when toggled back
            now = row["sources"].get(key)
            img = self._imgs.get(ts)
            if _has_png(now) and img is not None and not img.is_deleted:
                img.props["src"] = now["thumb"]
                img.update()
            else:
                picture.clear()
                with picture:
                    kicks += self._render_picture(row, now, kicked=kicks < _MAX_KICKS_PER_RENDER)
        self._refresh_overlays()
        self._refresh_layer_lists()
        self._prefetch_other_sources()

    def _select_size(self, key: str) -> None:
        self.size = key
        self._refs["seg_size"].set_active(key)
        # The tilt cards follow by one CSS variable, the wall by its column width, set in place: a
        # re-render would rebuild every tile. The reflow can move the selection out of view.
        self.tilt_gallery.set_size(key)
        grid = self._refs.get("grid")
        if grid is not None and not grid.is_deleted:
            grid.style(self._grid_columns())
        self._scroll_to_selected()

    def _select_tab(self, key: str) -> None:
        self.tab = key
        self._refs["seg_tab"].set_active(key)
        # The tab's own tools first: they settle the source the wall then draws.
        self._sync_controls()
        self._rerender_body()

    def _toggle_mosaic(self, ts: str) -> None:
        """Swap one tile between its slice and its tilts; the rest of the wall stays as it is."""
        if ts in self.open_mosaics:
            self.open_mosaics.discard(ts)
        else:
            self.open_mosaics.add(ts)
        tile = self._tiles.get(ts)
        row = next((r for r in self.rows if r["ts"] == ts), None)
        if tile is None or tile.is_deleted or row is None:
            return
        tile.clear()
        with tile:
            self._fill_tile(row, kicked=True)

    def _on_tilt_click(self, e, ts: str) -> None:
        """A tilt of a tile's mosaic: the tile is selected and the tilt opens in the viewer."""
        self._select(ts)
        key = (e.args or {}).get("key", "")
        tilt = self._tilt_by_key.get(key)
        if tilt is None:
            if key in self._frame_keys:
                ui.notify("That tilt has no motion-corrected average to show yet.", type="warning")
            else:
                ui.notify("That tilt is no longer in the registry; Refresh the view.", type="warning")
            return
        open_tilt_viewer(tilt, self.project_path)

    async def _reload_click(self) -> None:
        async with self._flight("reload") as acquired:
            if not acquired:
                return
            await self.reload()

    def _import_segmentations(self) -> None:
        """The import dialog; the wall re-collects once a batch has registered or been removed."""
        from backend import get_backend

        backend = get_backend()
        if backend is None:
            ui.notify("The server's backend is not running; nothing can be imported.", type="negative")
            return

        async def _reload() -> None:
            # A task of its own starts with no NiceGUI slot; the wall's container gives it the client.
            with self.container:
                await self._reload_click()

        open_segmentation_import_dialog(backend, self.project_path, on_done=lambda: asyncio.create_task(_reload()))

    def _kick_slab(self, row: dict, src: dict) -> None:
        """Render this tile's missing X/Y slab in the background. Same cache paths and
        dedup keys the Journey uses, so a slab rendered from either surface serves
        both and neither renders it twice."""
        from ui.background_task import BackgroundTask
        from ui.particles.pick_viewer import _recon_slab_paths
        from ui.tomo_dashboard_dialog import _denoise_slab_path
        from services.visualization.preview_render import render_xy_slab_preview
        from services.workers import WorkKind, run_in_worker

        cache_dir, mrc = src["kick"]
        is_recon = self.source.startswith(RECON_SOURCE)
        out_png = _recon_slab_paths(cache_dir, row["ts"])[0] if is_recon else _denoise_slab_path(cache_dir, row["ts"])

        async def _run(progress_cb):
            progress_cb(0, 0, "rendering X/Y slab…")
            return await run_in_worker(WorkKind.RENDER, render_xy_slab_preview, Path(mrc), out_png)

        BackgroundTask(
            title=f"Render tomogram slab · {row['ts']}",
            subtitle="Tomogram gallery preview",
            project_path=str(self.project_path),
            dedup_key=f"{'recon-slabs' if is_recon else 'denoise-slab'}:{cache_dir}:{row['ts']}",
        ).submit(_run, show_start_toast=False)

    async def _open_journey(self, ts: str, section: str | None = None) -> None:
        """Select this tilt series and turn the Journey on beside the wall, on it (and scrolled to
        `section`)."""
        self._select(ts)
        async with self._flight("journey") as acquired:
            if not acquired:
                return
            toggle = self.callbacks.get("show_journey")
            if toggle is None:
                ui.notify("Journey not available in this view", type="warning")
                return
            await toggle()
            show = self.callbacks.get("journey_show_ts")
            if show is not None:
                await show(ts, section=section)

    def _zoom(self, row: dict) -> None:
        """Full-size view of one tile — same image, same dots, as tall as the screen allows —
        stepping the selection through the wall in its order with ←/→ or the side chevrons. A
        dialog and not a mode: zooming is a look, and the routes onward (Journey, 3dmod) are one
        click away either way. One dialog per page, re-filled per tomogram, so a step never closes
        it."""
        if not row["ready"]:
            return  # its pictures are still being read
        self._select(row["ts"])
        dlg = self._refs.get("zoom")
        if dlg is None or dlg.is_deleted:
            with dialog_host():
                dlg = ui.dialog().classes("cb-gal-zoom-dialog")
                with dlg:
                    self._refs["zoom_body"] = ui.element("div").classes("cb-gal-zoom")
            # On the dialog's own root, which exists only while it shows: the arrows step the
            # zoom and nothing else, and no other key crosses the socket.
            dlg.on("keydown", self._on_zoom_key, js_handler=_ZOOM_KEYS_JS)
            self._refs["zoom"] = dlg
        self._zoom_ts = row["ts"]
        self._zoom_source = self.source
        self._fill_zoom()
        dlg.open()

    def _step_zoom(self, delta: int) -> None:
        order = [r["ts"] for r in self.rows if r["ready"]]
        if self._zoom_ts not in order:
            return
        i = order.index(self._zoom_ts) + delta
        if 0 <= i < len(order):
            self._zoom_ts = order[i]
            self._select(order[i], scroll=True)
            self._fill_zoom()

    def _on_zoom_key(self, e) -> None:
        key = (e.args or {}).get("key")
        if key in ("ArrowUp", "ArrowDown"):
            # The versions this tomogram has, in the menu's order (the others are grey there).
            row = self._zoom_row()
            keys = [k for k in self._source_keys() if row is not None and k in row["sources"]]
            if keys:
                step = -1 if key == "ArrowUp" else 1
                at = keys.index(self._zoom_source) + step if self._zoom_source in keys else (0 if step > 0 else -1)
                self._select_zoom_source(keys[at % len(keys)])
            return
        self._step_zoom(-1 if key == "ArrowLeft" else 1)

    def _zoom_row(self) -> dict | None:
        return next((r for r in self.rows if r["ts"] == self._zoom_ts), None)

    def _fill_zoom(self) -> None:
        """The zoom dialog: the slice as large as the screen holds on the left, with its pick count and
        3dmod chip, and on its right a column with the tomogram's name, its tilts summed up, its
        versions, the pick layers, the segmentations and the routes onward."""
        body = self._refs.get("zoom_body")
        dlg = self._refs.get("zoom")
        order = [r["ts"] for r in self.rows]
        if body is None or dlg is None or self._zoom_ts not in order:
            return
        i = order.index(self._zoom_ts)
        row = self.rows[i]
        src = row["sources"].get(self._zoom_source)
        body.clear()
        # The dialog sits outside the page, so it carries the feature colours itself.
        body.style(self._feature_vars())
        for key in ("zoom_img", "zoom_overlay"):
            self._refs.pop(key, None)
        with body:
            frame = ui.element("div").classes("cb-gal-frame cb-gal-zoom-frame")
            if row["aspect"]:
                x, y = row["aspect"]
                # As tall as the screen, unless the width left of the side column runs out first.
                height = f"min(calc(100vh - {_ZOOM_CHROME_PX}px), calc((100vw - {_ZOOM_SIDE_PX}px) * {y} / {x}))"
                frame.style(f"height: {height}; width: calc({height} * {x} / {y});")
            else:
                # Unknown extent: the image sizes the frame, so no dots can be placed on it.
                frame.style("width: fit-content;")
            with frame:
                if _has_png(src):
                    # A plain <img> whose src the version switch changes in place: the browser keeps
                    # the old picture up until the new one has loaded, so a flip doesn't blink.
                    img = ui.element("img").classes("cb-gal-zoom-img" + ("" if row["aspect"] else " auto"))
                    img.props.update(src=src["url"], alt=row["ts"])
                    self._refs["zoom_img"] = img
                else:
                    if not row["aspect"]:
                        frame.style("width: 60vw; height: 60vh;")
                    self._render_frame_note(
                        "not produced by this source" if src is None else "no preview on disk for this source yet"
                    )
                self._refs["zoom_overlay"] = ui.element("div").style("display: contents;")
                self._render_zoom_overlay()
                chip = self._refs["zoom_chip"] = ui.element("div").style("display: contents;")
                with chip:
                    if src is not None and src.get("mrc"):
                        copy_3dmod_chip(src["mrc"])
                if i > 0:
                    self._render_zoom_chevron("left")
                if i < len(order) - 1:
                    self._render_zoom_chevron("right")
            with ui.element("div").classes("cb-gal-zoom-side").style(f"width: {_ZOOM_SIDE_PX - 32}px;"):
                with ui.element("div").classes("cb-gal-zoom-headline"):
                    ui.label(row["label"]).classes("cb-gal-zoom-title")
                    ui.label(f"{i + 1} / {len(order)}").classes("cb-gal-zoom-src").tooltip("← → step through the wall")
                ui.label(row["ts"]).classes("cb-gal-zoom-ts")
                group = self._group_by_ts.get(row["ts"])
                if group is not None:
                    ui.html(tomogram_caption_html(group), sanitize=False)
                if row["source_label"] == SOURCE_IMPORTED:
                    ui.label("imported").classes("cb-gal-zoom-src").tooltip(
                        "Volume imported into this project rather than reconstructed by its own pipeline"
                    )
                # The wall's three tools, as lists: the same rows, for this tomogram.
                if len(self._source_keys()) > 1:
                    _zoom_section("filter_none", "Version", "↑ ↓ step through this tomogram's versions")
                    self._refs["zoom_versions"] = ui.element("div").classes("cb-gal-zoom-list")
                    self._render_zoom_versions()
                self._refs["zoom_caption"] = ui.label(src["caption"] if src else "").classes("cb-gal-zoom-src")
                if row["picks"] or row["features"]:
                    _zoom_section(
                        "layers.svg",
                        "Layers",
                        "Beside each pick layer, its picks on this tomogram; beside each segmentation, its share of "
                        "the volume: the mean probability over the whole tomogram.",
                    )
                    layers = ui.element("div").classes("cb-gal-zoom-list")
                    self._layer_menus["zoom"] = (layers, lambda: self._zoom_source)
                    with layers:
                        self._render_layer_list(self._zoom_source, row)
                else:
                    self._layer_menus.pop("zoom", None)

                async def _go(ts: str = row["ts"]) -> None:
                    dlg.close()
                    await self._open_journey(ts, "reconstruct")

                with ui.element("div").classes("cb-gal-zoom-actions"):
                    house_button(
                        "Journey ↗",
                        _go,
                        tooltip="This tomogram's whole pipeline — motion, CTF, alignment, reconstruction, picks",
                    )
                    house_button("Close", dlg.close)
        # The other versions of this tomogram, fetched while the user looks, so a switch is instant.
        others = [s["url"] for k, s in row["sources"].items() if k != self._zoom_source and s.get("url")]
        if others:
            # The body's client, not the context's: a chevron click runs in the chevron's slot, gone with body.clear().
            body.client.run_javascript(
                f"for (const u of {json.dumps(others)}) {{ const i = new Image(); i.decoding = 'async'; i.src = u; }}"
            )

    def _render_zoom_versions(self) -> None:
        box = self._refs.get("zoom_versions")
        row = self._zoom_row()
        if box is None or box.is_deleted or row is None:
            return
        box.clear()
        with box:
            self._refs["zoom_version_rows"] = ChoiceRows(
                self._source_choices(row), self._zoom_source, self._select_zoom_source
            )

    def _render_zoom_overlay(self) -> None:
        """The zoom's segmentation fills and dot layers (over an image only) and its pick count, for the
        zoom's own version: every stored dot, not the wall's sample."""
        overlay = self._refs.get("zoom_overlay")
        row = self._zoom_row()
        if overlay is None or overlay.is_deleted or row is None:
            return
        overlay.clear()
        visible = self._visible_picks(row, self._zoom_source)
        with overlay:
            if _has_png(row["sources"].get(self._zoom_source)):
                self._render_feature_fills(row, self._zoom_source)
                self._render_dot_layers(row, visible, _DOT_R_ZOOM, cap=None)
            self._render_pick_badge(visible)

    def _select_zoom_source(self, key: str) -> None:
        """Show the zoomed tomogram under another version; the wall keeps its own. With an image
        under both, the image is re-pointed in place and the caption, 3dmod chip, dots and lists
        follow; otherwise the zoom is re-filled. The choice holds while stepping with ← →."""
        row = self._zoom_row()
        if row is None or key == self._zoom_source:
            return
        before, now = row["sources"].get(self._zoom_source), row["sources"].get(key)
        self._zoom_source = key
        img = self._refs.get("zoom_img")
        if not (_has_png(before) and _has_png(now) and img is not None and not img.is_deleted):
            self._fill_zoom()
            return
        img.props["src"] = now["url"]
        img.update()
        self._refs["zoom_caption"].set_text(now["caption"])
        chip = self._refs["zoom_chip"]
        chip.clear()
        with chip:
            if now.get("mrc"):
                copy_3dmod_chip(now["mrc"])
        rows = self._refs.get("zoom_version_rows")
        if rows is not None:
            rows.set_active(key)
        self._render_zoom_overlay()
        self._refresh_layer_lists()

    def _render_zoom_chevron(self, side: str) -> None:
        chev = (
            ui.element("div")
            .classes("cb-gal-zoom-chev")
            .style(
                f"position: absolute; top: 50%; {side}: 6px; transform: translateY(-50%); z-index: 3; "
                "width: 36px; height: 72px; border-radius: 6px; display: flex; align-items: center; "
                "justify-content: center; cursor: pointer;"
            )
        )
        chev.on("click.stop", lambda: self._step_zoom(-1 if side == "left" else 1))
        with chev:
            ui.icon(f"chevron_{side}", size="34px")


def _zoom_section(glyph: str, title: str, tip: str | None = None) -> None:
    """A list's head in the zoom's side column: the glyph of its tool on the wall's title line, and its name."""
    head = ui.element("div").classes("cb-gal-zoom-sec")
    with head:
        render_glyph(glyph, size=12)
        ui.label(title)
    if tip:
        head.props["title"] = tip


def _dots_svg(dots: list[tuple[float, float]], color: str, radius: float) -> str:
    """One pick layer's dots as a single inline SVG.

    Percentage coordinates against the frame (which is the image box exactly), and a
    radius in user units — with no viewBox those are CSS pixels, so a dot keeps its
    size whether it sits on a 150 px thumbnail or the zoom view. One element per
    layer instead of one per pick: a 40-tomogram wall is thousands of dots.
    """
    circles = "".join(f'<circle cx="{fx:.2f}%" cy="{fy:.2f}%" r="{radius}"/>' for fx, fy in dots)
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none">'
        f'<g fill="{color}" stroke="rgba(255,255,255,0.8)" stroke-width="0.5">{circles}</g>'
        "</svg>"
    )

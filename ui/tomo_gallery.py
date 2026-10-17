"""Tomograms — the birds-eye wall of reconstructions, and every tilt beside it.

One tile per tomogram the project knows about (the reconstruct job's
``tomograms.star`` first, then imported volumes — see
``services.visualization.tomo_geometry.all_geometries``), laid out as a grid the
way the tilt-filter overview lays out tilts: everything at once, nothing selected.
The Journey answers "how did THIS tilt-series go"; this answers "how do they all
look", which is the question you ask before you pick one.

Two tabs: Tomograms (the wall) and Tilts (every tilt preview, grouped by tilt series;
``ui/tilt_previews.py``). A project with tilts and no tomograms opens on Tilts.

Three switches, all of them just re-render from the already-collected rows:

  · **source** — which version of the tomogram: the WarpTools recon PNG (one entry
    per reconstruct job when there are several, named by the alignment it traces back
    to), or a denoised X/Y slab (cryoCARE / IsoNet / any other denoisepredict job that
    has a volume for the tomogram). Only versions that actually produced something appear.
  · **picks** — a menu of pick layers, by species: each candidate-extract job's picks, and
    each species' registered lists, are a layer with its own dot color (pickable). By default
    the layers follow the source: a layer shows over the version that shows the reconstruction
    its picks were made on, so two picking arms on two alignments each sit on their own.
    Switching a layer by hand leaves that mode; a layer shown over another reconstruction is
    flagged. Nothing picked yet ⇒ no menu and plain slices, the de-novo project's normal state.
  · **size** — how many tiles (and tilt cards) fit across.

Tiles show a downscaled JPEG of each picture, and the browser fetches the other sources'
while idle, so a source switch re-points images it already holds.

A tile's image opens the zoom view: the slice as large as the screen allows, and beside it a
column with the tomogram's versions (↑ ↓), its pick layers (the same ones the wall uses, matched
to the zoom's own version) and the Journey button. The zoom's version never changes the wall's.
The toggle in a tile's top-left corner swaps the slice for a mosaic of that
series' tilts, in the same box; a tilt opens the full-size tilt viewer. The caption row
under a tile sums its tilts up: how many are in the tomogram, the stage-tilt range in use,
the series' CTF fit, and the dark exposures in use.

Images come from the same renderers the Journey uses (the WarpTools PNG
written by ts_reconstruct, else an X/Y slab we render and cache), so a tile and
the Journey's Reconstruct card can never show different pictures — and a missing
slab is auto-kicked here exactly as it is there, capped per render so opening the
wall on a 100-tomogram project doesn't submit 100 renders at once.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from nicegui import ui

from services.dashboard_data import position_label, tilt_thumb_dir, vis_asset_url
from services.models_base import SPECIES_OVERLAY_COLORS
from services.project_state import get_project_state_for
from services.tilt_series_service import ensure_tilt_thumbnails
from services.visualization.preview_orchestrator import _find_warp_tomo_preview, read_preview_manifest
from services.visualization.preview_render import is_output_stale
from services.visualization.tomo_geometry import SOURCE_IMPORTED, TomoGeometry, all_geometries
from ui.components.buttons import house_button
from ui.components.copyable import copy_3dmod_chip
from ui.components.dialogs import dialog_host
from ui.components.reactive import SingleFlight, owned_timer
from ui.components.recon_compare import job_dirs, recon_of, recon_version_labels, recon_versions_for_ts
from ui.components.segmented import render_segmented
from ui.components.svg_icon import load_icon_svg
from ui.dashboard.css import ensure_assets_loaded
from ui.tilt_previews import (
    TILT_CLICK_JS,
    TiltGallery,
    collect_tilt_groups,
    mosaic_html,
    open_tilt_viewer,
    registry_tilt_series,
    thumbnail_task_key,
    tilt_context,
    tomogram_caption_html,
)

RECON_SOURCE = "recon"
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

# Dots kept per (tomogram, species). picks.json is sorted score-descending, so the
# stride sample the wall draws from this is spread across the whole score range
# rather than being the top-N. The zoom view draws the whole stored set.
_MAX_DOTS = 1200
_WALL_DOTS = 260
_DOT_R_WALL = 1.1
_DOT_R_ZOOM = 2.0

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
            for ts, entry in ((manifest or {}).get("tomograms") or {}).items():
                if not entry.get("picks_json"):
                    continue
                data = _read_picks_json(Path(entry["picks_json"]))
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
        for recon, by_ts in by_recon.items():
            entries = {}
            for ts, lists in by_ts.items():
                geom = geom_by_ts.get(ts)
                if geom is not None and geom.is_usable:
                    picks: list[dict] = []
                    for pl in lists:
                        # None = unreadable (already logged by the reader); the wall draws what it can.
                        picks.extend(
                            _read_pick_list_voxels(Path(pl.path), list(geom.dims_xyz_px), geom.binned_apix) or []
                        )
                    n, dots = len(picks), _normalized_dots(picks, geom.dims_xyz_px)
                    dims = [int(geom.dims_xyz_px[0]), int(geom.dims_xyz_px[1])]
                else:
                    # No resolved frame ⇒ nothing can be placed. Carry the registered
                    # count anyway so the tile can say "picks here, not drawable" —
                    # dropping the species would read as "nothing picked".
                    n, dots, dims = sum(int(pl.count or 0) for pl in lists), [], None
                entries[ts] = {"n": n, "dots": dots, "dims": dims}
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


def recon_source_key(label: str) -> str:
    """Toolbar key of one reconstruct job's version when the project has several."""
    return f"{RECON_SOURCE} · {label}"


def _sources_for(project_state, project_path: Path, geom: TomoGeometry, dirs: dict[Path, str]) -> dict[str, dict]:
    """Per-tomogram image sources, keyed by the toolbar's source id.

    With one reconstruct job, ``recon`` is its picture. With several (e.g. one per alignment),
    each job with a volume of this tomogram is its own version, keyed ``recon · <alignment>``
    by the project-wide label, so one toolbar choice shows the same job on every tile; a tomogram
    a job did not reconstruct says so instead of showing another job's. Each denoisepredict job
    with a volume for this tomogram adds one entry under its method label. ``kick`` carries the
    (cache dir, source MRC) the renderer needs when the PNG isn't on disk yet; ``mrc`` is the
    volume the picture shows, for the 3dmod chip; ``recon`` the reconstruct instance it is (or
    was denoised from), which pick layers are matched against.
    """
    from ui.tomo_dashboard_dialog import _available_denoise_methods_for_ts, _denoise_slab_path

    out: dict[str, dict] = {}
    recon_labels = recon_version_labels(project_state)
    if geom.source != SOURCE_IMPORTED and len(recon_labels) > 1:
        for v in recon_versions_for_ts(project_state, geom.tomo_name):
            warp_png = v["mrc"].with_suffix(".png")
            src = _recon_source(v["dir"], geom.tomo_name, v["mrc"], warp_png if warp_png.exists() else None)
            src["caption"] = f"{v['label']} · {src['caption']}"
            src["recon"] = v["iid"]
            out[recon_source_key(v["label"])] = src
    else:
        warp_png = _find_warp_tomo_preview(project_path, geom.tomo_name, geom.recon_mrc)
        src = _recon_source(geom.tomograms_star.parent, geom.tomo_name, geom.recon_mrc, warp_png)
        src["recon"] = next(iter(recon_labels)) if geom.source != SOURCE_IMPORTED and recon_labels else None
        out[RECON_SOURCE] = src

    for label, job_dir, mrc in _available_denoise_methods_for_ts(project_state, project_path, geom.tomo_name):
        png = _denoise_slab_path(job_dir, geom.tomo_name)
        fresh = png.exists() and not is_output_stale(png, [mrc])
        url, thumb = _png_urls(png if fresh else None)
        denoise_iid = dirs.get(Path(job_dir))
        out[label] = {
            "png": png if fresh else None,
            "kick": None if fresh else (job_dir, mrc),
            "caption": f"{label} · denoised X/Y slab",
            "mrc": mrc,
            "url": url,
            "thumb": thumb,
            "recon": recon_of(project_state, denoise_iid, dirs) if denoise_iid else None,
        }
    return out


def collect_rows(project_state, project_path: Path) -> tuple[list[dict], list[dict]]:
    """``(rows, pick layers)``: one row per tomogram, ordered by acquisition position, and the
    layers its ``picks`` entries name (`_pick_layers`). Disk-only — safe to call in a thread,
    which is where the page calls it from."""
    geoms = all_geometries(project_state, project_path)
    layers, picks_by_ts = _pick_layers(project_state, project_path, geoms)
    dirs = job_dirs(project_state)
    rows: list[dict] = []
    for geom in geoms:
        picks = picks_by_ts.get(geom.tomo_name, [])
        # Frame aspect = the tomogram's own X/Y extent, else the extent the picks on it
        # were normalized against (the candidate-extract job read it when it wrote
        # picks.json, and can have it when the volume is no longer on disk). Neither ⇒
        # the frame is not the tomogram's box, so the tile lets the image size itself
        # and reports its picks as counted-but-unplaced rather than scattering them
        # against a guessed extent.
        aspect = tuple(geom.dims_xyz_px[:2]) if geom.dims_xyz_px else None
        if aspect is None:
            aspect = next((tuple(p["dims"]) for p in picks if p.get("dims")), None)
        rows.append(
            {
                "ts": geom.tomo_name,
                "label": position_label(geom.tomo_name)[0],
                "order": position_label(geom.tomo_name)[1],
                "aspect": aspect,
                "volume": str(geom.recon_mrc) if geom.recon_mrc else None,
                "source_label": geom.source,
                "sources": _sources_for(project_state, project_path, geom, dirs),
                "picks": picks,
            }
        )
    rows.sort(key=lambda r: (r["order"], r["ts"]))
    return rows, layers


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
        # Pick overlays (`_pick_layers`). Following the source, a layer shows on the source that
        # shows the reconstruction its picks were made on; once a layer is switched by hand,
        # `layer_on` decides.
        self.layers: list[dict] = []
        self._layer_by_id: dict[str, dict] = {}
        self._source_recon: dict[str, str | None] = {}  # source key -> the reconstruct instance it shows
        self.picks_follow = True
        self.layer_on: dict[str, bool] = {}
        # Live pick-layer lists, "wall" (the toolbar menu) and "zoom": (body, summary label, source getter).
        self._picks_menus: dict[str, tuple[Any, Any, Any]] = {}
        self._overlays: dict[str, Any] = {}  # each tile's dot layers + pick badge
        self._overlay_keys: dict[str, tuple] = {}  # the layer ids each tile's overlay draws
        self._imgs: dict[str, Any] = {}  # each tile's <img>, re-pointed on a source switch
        self._chips: dict[str, Any] = {}  # each tile's 3dmod chip slot
        self._prefetched: set[str] = set()
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
        # The Tilts tab's gallery; it keeps its open groups, sort, show and metrics across renders.
        self.tilt_gallery = TiltGallery(self.project_path, size=self.size)
        # Kept on the page, so a size switch, a Refresh or the pending poll keeps them.
        self.open_mosaics: set[str] = set()  # tiles showing their tilts instead of the slice
        self._tiles: dict[str, Any] = {}
        self._pictures: dict[str, Any] = {}  # each tile's image-or-note and 3dmod chip, swapped on a source switch
        self._zoom_ts: str | None = None  # the tomogram the zoom dialog shows
        self._zoom_source: str = RECON_SOURCE  # the zoom's own version, the wall's when the zoom opens
        self._flight = SingleFlight()
        self._refs: dict[str, Any] = {}
        self._collected = False
        self._active = False
        # Slabs kicked off by this page land minutes later. Poll while the wall is the
        # visible view AND something is still pending — disk work, so 15 s (CLAUDE.md),
        # gated on a signature so a tick that changes nothing never touches the DOM.
        self._timer = owned_timer(15.0, self._tick, container)
        self._timer.deactivate()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def show(self) -> None:
        """Point the workspace at the wall. The first call collects behind a spinner;
        later ones show what's cached (the pending-tile poll and the Refresh button
        bring in previews that landed meanwhile)."""
        async with self._flight("show") as acquired:
            if not acquired:
                return
            if self._collected:
                return
            await self.reload()

    async def reload(self) -> None:
        self.container.clear()
        with self.container, ui.element("div").classes("cb-empty"):
            ui.spinner(size="lg", color="indigo")
            ui.label("Reading tomograms…").classes("text-xs")
        await asyncio.sleep(0.03)  # flush the spinner before the disk pass
        await self._collect()
        self.render()

    async def _collect(self) -> None:
        # Explicit-path resolution, not the tab accessor: this also runs from a timer
        # tick, where a bare lookup can hand back a blank throwaway state.
        state = get_project_state_for(self.project_path)
        tilt_series, self.tilt_error = registry_tilt_series(self.project_path)
        # The tilt filter's review is live job state: snapshot it here, derive in the thread.
        ctx = tilt_context(state)
        png_dir = self._png_dir = tilt_thumb_dir(state, self.project_path)
        (self.rows, layers), (self.tilt_groups, self.tilt_facts) = await asyncio.to_thread(
            lambda: (collect_rows(state, self.project_path), collect_tilt_groups(tilt_series, png_dir, ctx))
        )
        self._collected = True
        # Colors picked in this session outlive a re-collection.
        for layer in layers:
            if layer["id"] in self._layer_by_id:
                layer["color"] = self._layer_by_id[layer["id"]]["color"]
        self.layers = layers
        self._layer_by_id = {layer["id"]: layer for layer in layers}
        self._source_recon = {key: src.get("recon") for row in self.rows for key, src in row["sources"].items()}
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
        before = self._grid_signature()
        await self._collect()
        if self._grid_signature() != before:
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
        the wall is on screen."""
        self._active = on
        try:
            self._timer.activate() if on else self._timer.deactivate()
        except Exception:
            pass  # timer torn down with the client

    # ── Derived state ─────────────────────────────────────────────────────────

    def _layer_shown(self, layer: dict, source: str) -> bool:
        """Whether a pick layer draws over `source`. Following the source: when the picks were
        made on the reconstruction it shows, or when either side's reconstruction is unknown."""
        if not self.picks_follow:
            return self.layer_on.get(layer["id"], True)
        shows = self._source_recon.get(source)
        return layer["recon"] is None or shows is None or layer["recon"] == shows

    def _off_source(self, layer: dict, source: str) -> bool:
        """The layer's picks were made on another reconstruction than `source` shows."""
        shows = self._source_recon.get(source)
        return layer["recon"] is not None and shows is not None and layer["recon"] != shows

    def _visible_picks(self, row: dict, source: str) -> list[dict]:
        """The row's pick entries that draw over `source`, each with its layer's meta."""
        out = []
        for entry in row["picks"]:
            layer = self._layer_by_id.get(entry["id"])
            if layer is not None and self._layer_shown(layer, source):
                out.append({**layer, **entry})
        return out

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
            sig.append((row["ts"], src is not None and src["png"] is not None, tuple(p["n"] for p in row["picks"])))
        tilts = tuple((g["ts"], len(g["tilts"]), g["n_png"]) for g in self.tilt_groups)
        return tuple(sig), tilts, self.tilt_error

    def _source_options(self) -> list[tuple[str, str]]:
        """(key, label) for every source at least one tomogram can show."""
        keys: list[str] = []
        for row in self.rows:
            for key in row["sources"]:
                if key not in keys:
                    keys.append(key)
        # Reconstructions first, in the order the tomograms list them (by job dir), then the rest.
        keys.sort(key=lambda k: (not k.startswith(RECON_SOURCE), "" if k.startswith(RECON_SOURCE) else k.lower()))
        return [(k, k) for k in keys]

    # ── Render ────────────────────────────────────────────────────────────────

    def render(self) -> None:
        ensure_assets_loaded()
        self.container.clear()
        with self.container, ui.element("div").classes("cb-gal-page"):
            self._render_toolbar()
            body = ui.element("div").classes("cb-gal-body")
            self._refs["body"] = body
            with body:
                self._render_body()

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

    def _render_toolbar(self) -> None:
        with ui.element("div").classes("cb-gal-toolbar"):
            tabs = render_segmented([(TOMOGRAMS_TAB, "Tomograms"), (TILTS_TAB, "Tilts")], self.tab, self._select_tab)
            tabs.set_badge(TOMOGRAMS_TAB, str(len(self.rows)))
            tabs.set_badge(TILTS_TAB, str(len(self._tilt_by_key)))
            self._refs["seg_tab"] = tabs
            # The active tab's own controls: a tab switch rebuilds them, never the strip that was clicked.
            controls = ui.element("div").style("display: flex; align-items: center; gap: 8px; flex-wrap: wrap;")
            self._refs["tab_controls"] = controls
            with controls:
                self._render_tab_controls()
            ui.element("div").style("flex: 1;")
            ui.label("size").classes("cb-gal-toolbar-label")
            self._refs["seg_size"] = render_segmented([(k, lbl) for k, lbl, _w in _SIZES], self.size, self._select_size)
            house_button("Refresh", self._reload_click, tooltip="Re-read the tomograms, tilts and previews from disk")

    def _render_tab_controls(self) -> None:
        if self.tab == TILTS_TAB:
            if self.tilt_groups:
                self.tilt_gallery.render_controls()
            return
        sources = self._source_options()
        if sources and self.source not in dict(sources):
            self.source = sources[0][0]
        if len(sources) > 1:
            ui.label("source").classes("cb-gal-toolbar-label")
            self._refs["seg_source"] = render_segmented(sources, self.source, self._select_source)
        if self.layers:
            ui.label("picks").classes("cb-gal-toolbar-label")
            trigger = ui.element("div").classes("cb-gal-picks-btn")
            with trigger:
                summary = ui.label("")
                ui.icon("expand_more", size="13px")
                with ui.menu().props("anchor='bottom left' self='top left'"):
                    body = ui.element("div").classes("cb-gal-picks-menu")
            self._picks_menus["wall"] = (body, summary, lambda: self.source)
            self._refresh_picks_lists()

    # ── Pick layers ───────────────────────────────────────────────────────────

    def _picks_summary(self, source: str) -> str:
        shown = [layer for layer in self.layers if self._layer_shown(layer, source)]
        if len(shown) == 1:
            return shown[0]["label"]
        return f"{len(shown)} of {len(self.layers)} layers"

    def _refresh_picks_lists(self) -> None:
        """Repaint every live pick-layer list (the toolbar's menu, the zoom's side column) and
        its summary; they are a few rows each."""
        for name, (body, summary, source_of) in list(self._picks_menus.items()):
            if body.is_deleted:
                del self._picks_menus[name]
                continue
            if summary is not None:
                summary.set_text(self._picks_summary(source_of()))
            body.clear()
            with body:
                self._render_picks_list(source_of())

    def _render_picks_list(self, source: str) -> None:
        """The pick layers, by species: a click on a row shows or hides it (and stops following the
        source), a click on its marker picks its color. "Follow the source" puts the layers back
        on what the source shows."""
        follow = ui.element("div").classes("cb-gal-pk-row cb-gal-pk-follow")
        follow.on("click", lambda _e: self._set_follow(not self.picks_follow))
        follow.tooltip(
            "Show the picks made on the reconstruction the source shows (a denoised version counts as the "
            "reconstruction it was denoised from). Lists whose reconstruction is not known show on every source."
        )
        with follow:
            ui.icon("check_box" if self.picks_follow else "check_box_outline_blank", size="14px")
            ui.label("Follow the source")
        species = None
        for layer in self.layers:
            if layer["species_id"] != species:
                species = layer["species_id"]
                with ui.element("div").classes("cb-gal-pk-species"):
                    ui.html(load_icon_svg("particle.svg", "#94a3b8", size=11), sanitize=False).style("display: flex;")
                    ui.label(layer["species"])
            shown = self._layer_shown(layer, source)
            row = ui.element("div").classes("cb-gal-pk-row" + ("" if shown else " off"))
            row.on("click", lambda _e, lid=layer["id"]: self._toggle_layer(lid, source))
            with row:
                ui.icon("check_box" if shown else "check_box_outline_blank", size="14px")
                # A box around the drawn dot: an html element's content would replace a menu inside it.
                marker = ui.element("div").classes("cb-gal-pk-marker")
                marker.on("click.stop", lambda: None)
                with marker:
                    ui.html(_marker_svg(layer["color"]), sanitize=False)
                    ui.tooltip("Pick this layer's color")
                    with ui.menu().props("anchor='bottom left' self='top left'"):
                        with ui.element("div").classes("cb-gal-pk-palette"):
                            for color in SPECIES_OVERLAY_COLORS:
                                swatch = ui.html(_marker_svg(color, 14), sanitize=False).classes("cb-gal-pk-marker")
                                swatch.on("click.stop", lambda _e, lid=layer["id"], c=color: self._set_color(lid, c))
                ui.label(layer["label"]).classes("cb-gal-pk-name")
                ui.label(str(layer["n"])).classes("cb-gal-pk-n")
                if shown and self._off_source(layer, source):
                    ui.icon("warning", size="12px").classes("cb-gal-pk-warn").tooltip(
                        f"Picked on {layer['recon_label'] or layer['recon']}, a different reconstruction from the "
                        "one this source shows: another alignment puts the specimen elsewhere in the volume, so "
                        "these dots can sit off their particles."
                    )

    def _set_follow(self, on: bool) -> None:
        self.picks_follow = on
        self._picks_changed()

    def _toggle_layer(self, layer_id: str, source: str) -> None:
        if self.picks_follow:
            # Switching one layer by hand starts from what was showing.
            self.layer_on = {layer["id"]: self._layer_shown(layer, source) for layer in self.layers}
            self.picks_follow = False
        self.layer_on[layer_id] = not self.layer_on.get(layer_id, True)
        self._picks_changed()

    def _set_color(self, layer_id: str, color: str) -> None:
        layer = self._layer_by_id.get(layer_id)
        if layer is None or layer["color"] == color:
            return
        layer["color"] = color
        self._picks_changed()

    def _picks_changed(self) -> None:
        self._refresh_picks_lists()
        self._refresh_overlays()
        if self._zoom_ts is not None and self._refs.get("zoom") is not None and self._refs["zoom"].value:
            self._render_zoom_overlay()

    def _render_grid(self) -> None:
        if not self.rows:
            self._render_empty()
            return
        width = next(w for k, _lbl, w in _SIZES if k == self.size)
        kicks = 0
        self._tiles, self._pictures, self._imgs, self._chips = {}, {}, {}, {}
        self._overlays, self._overlay_keys = {}, {}
        with (
            ui.element("div")
            .classes("cb-gal-grid")
            .style(f"grid-template-columns: repeat(auto-fill, minmax({width}px, 1fr));")
        ):
            for row in self.rows:
                kicks += self._render_tile(row, kicked=kicks < _MAX_KICKS_PER_RENDER)
        self._prefetch_other_sources()

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
        tile = ui.element("div").classes("cb-gal-tile")
        self._tiles[row["ts"]] = tile
        with tile:
            return self._fill_tile(row, kicked=kicked)

    def _fill_tile(self, row: dict, *, kicked: bool) -> int:
        """A tile's frame and caption row: the slice, or, toggled, a mosaic of the series'
        tilts in the same box. The caption row has the position label and, for a tomogram
        with tilts (an imported one has none), its tilts summed up."""
        group = self._group_by_ts.get(row["ts"])
        spent = 0
        if group is not None and row["ts"] in self.open_mosaics:
            self._render_mosaic_frame(row, group)
        else:
            spent = self._render_slice_frame(row, group, kicked=kicked)
        with ui.element("div").classes("cb-gal-cap"):
            ui.label(row["label"]).classes("cb-gal-name").tooltip(row["ts"])
            if group is not None:
                ui.html(tomogram_caption_html(group, compact=self.size == "s"), sanitize=False).style("flex: 0 0 auto;")
        return spent

    def _render_slice_frame(self, row: dict, group: dict | None, *, kicked: bool) -> int:
        src = row["sources"].get(self.source)
        frame = ui.element("div").classes("cb-gal-frame" + ("" if row["aspect"] else " cb-gal-frame-auto"))
        if row["aspect"]:
            frame.style(f"aspect-ratio: {row['aspect'][0]}/{row['aspect'][1]};")
        frame.on("click", lambda _e, r=row: self._zoom(r))
        frame.tooltip("Click to zoom")
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
            chip = ui.element("div").style("display: contents;")
            self._chips[row["ts"]] = chip
            with chip:
                if src is not None and src.get("mrc"):
                    copy_3dmod_chip(src["mrc"])
            if group is not None:
                self._render_mosaic_toggle(row["ts"], len(group["tilts"]), on=False)
        return spent

    def _render_picture(self, row: dict, src: dict | None, *, kicked: bool) -> int:
        """What a tile shows for one source: its image (the thumbnail), or a note saying why there
        is none. Returns 1 when it submitted a slab render."""
        spent = 0
        self._imgs.pop(row["ts"], None)
        if src is None:
            self._render_frame_note("not produced by this source")
        elif src["png"] is not None:
            img = ui.element("img").classes("cb-gal-img")
            img.props.update(src=src["thumb"], alt=row["ts"], loading="lazy", decoding="async")
            self._imgs[row["ts"]] = img
        elif src["kick"] is not None:
            if kicked:
                spent = 1
                self._kick_slab(row, src)
            self._render_frame_note("rendering slice…", spinner=True)
        else:
            self._render_frame_note("no volume on disk")
        return spent

    def _overlay_key(self, row: dict, source: str) -> tuple:
        """What a tile's overlay draws over `source`: its image or not, and the layers with their colors."""
        has_png = _has_png(row["sources"].get(source))
        return has_png, tuple((p["id"], p["color"]) for p in self._visible_picks(row, source))

    def _render_tile_overlay(self, row: dict) -> None:
        """A tile's dot layers (over an image only) and its pick badge, for the wall's source."""
        key = self._overlay_key(row, self.source)
        self._overlay_keys[row["ts"]] = key
        visible = self._visible_picks(row, self.source)
        if key[0]:
            self._render_dot_layers(row, visible, _DOT_R_WALL)
        self._render_pick_badge(visible)

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
        """The series' tilts in the box the slice occupies, with no zoom click, dots or pick
        badge. A tilt opens the full-size tilt viewer."""
        aspect = row["aspect"]
        cells, own_aspect = mosaic_html(group["tilts"], aspect[0] / max(aspect[1], 1) if aspect else None)
        ratio = f"{aspect[0]}/{aspect[1]}" if aspect else own_aspect
        frame = ui.element("div").classes("cb-gal-frame").style(f"aspect-ratio: {ratio}; cursor: default;")
        with frame:
            mosaic = ui.html(cells, sanitize=False).style("position: absolute; inset: 0;")
            mosaic.on("click", handler=self._on_tilt_click, js_handler=TILT_CLICK_JS)
            self._render_mosaic_toggle(row["ts"], len(group["tilts"]), on=True)

    def _render_mosaic_toggle(self, ts: str, n: int, *, on: bool) -> None:
        """The top-left corner switch between a tile's slice and its tilts (the pick badge
        holds the top-right)."""
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
        """Pick count in the tile's corner. Counted-but-not-drawn is stated, not
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
            f"{unplaced} of {total} picks can't be placed on this tile — the tomogram's binned extent "
            "is unresolved (no volume on disk and no size in tomograms.star), so no dot position is "
            "trustworthy. Open the Journey's Reconstruct card for this tilt-series."
        )

    def _render_frame_note(self, text: str, *, spinner: bool = False) -> None:
        with ui.element("div").classes("cb-gal-note"):
            if spinner:
                ui.spinner(size="18px", color="indigo-400")
            ui.label(text)

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
        notes: list[tuple[str, str, bool]] = []
        if self.tilt_error:
            notes.append((self.tilt_error, "#b45309", False))
        if self.tilt_awaiting:
            running = self._thumbs_running()
            tail = "; they are being rendered in the background." if running else "."
            notes.append((f"{self.tilt_awaiting} tilts have no preview image yet{tail}", "#94a3b8", running))
        self.tilt_gallery.render(notes)

    # ── Actions ───────────────────────────────────────────────────────────────

    def _select_source(self, key: str) -> None:
        """Point every tile at another source. A tile with an image under both only re-points its
        <img> (whose thumbnail the browser prefetched); one gaining or losing its image swaps its
        picture slot. The 3dmod chip follows, and the dot layers and badge repaint only where
        the layers drawn change. Nothing else of a tile is rebuilt: rebuilding the whole grid
        re-sent every tile's dot layers and froze the page."""
        self.source = key
        # The segmented control flips its own highlight only when told to (it never rebuilds itself).
        self._refs["seg_source"].set_active(key)
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
            chip = self._chips.get(ts)
            if chip is not None and not chip.is_deleted:
                chip.clear()
                with chip:
                    if now is not None and now.get("mrc"):
                        copy_3dmod_chip(now["mrc"])
        self._refresh_overlays()
        self._refresh_picks_lists()
        self._prefetch_other_sources()

    def _select_size(self, key: str) -> None:
        self.size = key
        self._refs["seg_size"].set_active(key)
        # The tilt cards follow by one CSS variable; only the wall re-renders.
        self.tilt_gallery.set_size(key)
        if self.tab != TILTS_TAB:
            self._rerender_body()

    def _select_tab(self, key: str) -> None:
        self.tab = key
        self._refs["seg_tab"].set_active(key)
        controls = self._refs["tab_controls"]
        controls.clear()
        with controls:
            self._render_tab_controls()
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

    def _on_tilt_click(self, e) -> None:
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

    async def _open_journey(self, ts: str) -> None:
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
                await show(ts, section="reconstruct")

    def _zoom(self, row: dict) -> None:
        """Full-size view of one tile — same image, same dots, as tall as the screen allows —
        stepping through the wall in its order with ←/→ or the side chevrons. A dialog and not
        a mode: zooming is a look, and the routes onward (Journey, 3dmod) are one click away
        either way. One dialog per page, re-filled per tomogram, so a step never closes it."""
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
        order = [r["ts"] for r in self.rows]
        if self._zoom_ts not in order:
            return
        i = order.index(self._zoom_ts) + delta
        if 0 <= i < len(order):
            self._zoom_ts = order[i]
            self._fill_zoom()

    def _on_zoom_key(self, e) -> None:
        key = (e.args or {}).get("key")
        if key in ("ArrowUp", "ArrowDown"):
            keys = [k for k, _label in self._source_options()]
            if len(keys) > 1 and self._zoom_source in keys:
                step = -1 if key == "ArrowUp" else 1
                self._select_zoom_source(keys[(keys.index(self._zoom_source) + step) % len(keys)])
            return
        self._step_zoom(-1 if key == "ArrowLeft" else 1)

    def _zoom_row(self) -> dict | None:
        return next((r for r in self.rows if r["ts"] == self._zoom_ts), None)

    def _fill_zoom(self) -> None:
        """The zoom dialog: the slice as large as the screen holds on the left, and on its right a
        column with the tomogram's name, its versions, the pick layers and the routes onward."""
        body = self._refs.get("zoom_body")
        dlg = self._refs.get("zoom")
        order = [r["ts"] for r in self.rows]
        if body is None or dlg is None or self._zoom_ts not in order:
            return
        i = order.index(self._zoom_ts)
        row = self.rows[i]
        src = row["sources"].get(self._zoom_source)
        body.clear()
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
                if row["source_label"] == SOURCE_IMPORTED:
                    ui.label("imported").classes("cb-gal-zoom-src").tooltip(
                        "Volume imported into this project rather than reconstructed by its own pipeline"
                    )
                sources = self._source_options()
                if len(sources) > 1:
                    ui.label("version").classes("cb-gal-zoom-sec").tooltip("↑ ↓ switch versions")
                    self._refs["zoom_versions"] = ui.element("div").classes("cb-gal-zoom-list")
                    self._render_zoom_versions()
                self._refs["zoom_caption"] = ui.label(src["caption"] if src else "").classes("cb-gal-zoom-src")
                if self.layers:
                    ui.label("picks").classes("cb-gal-zoom-sec")
                    picks = ui.element("div").classes("cb-gal-zoom-list")
                    self._picks_menus["zoom"] = (picks, None, lambda: self._zoom_source)
                    with picks:
                        self._render_picks_list(self._zoom_source)

                async def _go(ts: str = row["ts"]) -> None:
                    dlg.close()
                    await self._open_journey(ts)

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
            ui.run_javascript(
                f"for (const u of {json.dumps(others)}) {{ const i = new Image(); i.decoding = 'async'; i.src = u; }}"
            )

    def _render_zoom_versions(self) -> None:
        box = self._refs.get("zoom_versions")
        row = self._zoom_row()
        if box is None or box.is_deleted or row is None:
            return
        box.clear()
        with box:
            for key, label in self._source_options():
                src = row["sources"].get(key)
                on = key == self._zoom_source
                item = ui.element("div").classes("cb-gal-pk-row" + ("" if src else " off"))
                item.on("click", lambda _e, k=key: self._select_zoom_source(k))
                with item:
                    ui.icon("radio_button_checked" if on else "radio_button_unchecked", size="14px")
                    ui.label(label).classes("cb-gal-pk-name")
                item.tooltip(src["caption"] if src else "This version has no volume of this tomogram")

    def _render_zoom_overlay(self) -> None:
        """The zoom's dot layers, for the zoom's own version: every stored dot, not the wall's sample."""
        overlay = self._refs.get("zoom_overlay")
        row = self._zoom_row()
        if overlay is None or overlay.is_deleted or row is None:
            return
        overlay.clear()
        if not _has_png(row["sources"].get(self._zoom_source)):
            return
        with overlay:
            self._render_dot_layers(row, self._visible_picks(row, self._zoom_source), _DOT_R_ZOOM, cap=None)

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
        self._render_zoom_versions()
        self._render_zoom_overlay()
        self._refresh_picks_lists()

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


def _marker_svg(color: str, size: int = 10) -> str:
    """A layer's dot as its key in the pick lists: the dot the overlay draws, at list size."""
    r = size / 2 - 1
    return (
        f'<svg width="{size}" height="{size}" style="display: block;"><circle cx="{size / 2}" cy="{size / 2}" '
        f'r="{r}" fill="{color}" stroke="rgba(15,23,42,0.35)" stroke-width="1"/></svg>'
    )


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

"""Tomograms — the birds-eye wall of reconstructions.

One tile per tomogram the project knows about (the reconstruct job's
``tomograms.star`` first, then imported volumes — see
``services.visualization.tomo_geometry.all_geometries``), laid out as a grid the
way the tilt-filter overview lays out tilts: everything at once, nothing selected.
The Journey answers "how did THIS tilt-series go"; this answers "how do they all
look", which is the question you ask before you pick one.

Three switches, all of them just re-render from the already-collected rows:

  · **source** — the WarpTools recon PNG, or a denoised X/Y slab (cryoCARE /
    IsoNet / any other denoisepredict job that has a volume for the tomogram).
    Only methods that actually produced something appear.
  · **picks** — one toggle per species with picks on any tomogram; its dots are
    projected onto every tile that has them. Nothing picked yet ⇒ no toggles and
    plain slices, which is the de-novo project's normal state.
  · **size** — how many tiles fit across.

A tile's image opens the zoom view; its caption opens that tomogram in the
Journey. Images come from the same renderers the Journey uses (the WarpTools PNG
written by ts_reconstruct, else an X/Y slab we render and cache), so a tile and
the Journey's Reconstruct card can never show different pictures — and a missing
slab is auto-kicked here exactly as it is there, capped per render so opening the
wall on a 100-tomogram project doesn't submit 100 renders at once.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from nicegui import ui

from services.dashboard_data import position_label, vis_asset_url
from services.project_state import get_project_state_for
from services.visualization.preview_orchestrator import _find_warp_tomo_preview, read_preview_manifest
from services.visualization.preview_render import is_output_stale
from services.visualization.tomo_geometry import SOURCE_IMPORTED, TomoGeometry, all_geometries
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.reactive import SingleFlight
from ui.components.segmented import render_segmented
from ui.dashboard.css import ensure_assets_loaded

RECON_SOURCE = "recon"

# Tile column width per size step (CSS grid minmax floor).
_SIZES: tuple[tuple[str, str, int], ...] = (("s", "S", 150), ("m", "M", 240), ("l", "L", 380))
DEFAULT_SIZE = "m"

# Dots kept per (tomogram, species). picks.json is sorted score-descending, so the
# stride sample the wall draws from this is spread across the whole score range
# rather than being the top-N. The zoom view draws the whole stored set.
_MAX_DOTS = 1200
_WALL_DOTS = 260
_DOT_R_WALL = 1.6
_DOT_R_ZOOM = 2.8

# Missing X/Y slabs auto-kicked per render. The wall asks for every tomogram at
# once; each render reads ~50 MB of MRC, so submit a few and let the next refresh
# (or the completion callback) take the rest.
_MAX_KICKS_PER_RENDER = 6


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


def _species_rows(project_state, project_path: Path, geoms: list[TomoGeometry]) -> dict[str, list[dict]]:
    """``{ts: [{id, label, color, n, dots}, ...]}`` — every species' picks on every
    tomogram, auto (candidate-extract picks.json) and hand-authored (the pick-list
    registry's centered-Å stars) alike, in the render plan's species order."""
    # The Journey's own readers, so the wall's dots and the viewer's dots can never
    # disagree about where a pick is.
    from services.dashboard_data import job_dir_for, species_render_plan, SPECIES_OVERLAY_COLORS
    from ui.particles.pick_viewer import _read_pick_list_voxels, _read_picks_json

    geom_by_ts = {g.tomo_name: g for g in geoms}
    out: dict[str, list[dict]] = {}

    for idx, (species, species_id, ce) in enumerate(species_render_plan(project_state)):
        color = getattr(species, "color", "") or SPECIES_OVERLAY_COLORS[idx % len(SPECIES_OVERLAY_COLORS)]
        label = str(getattr(species, "name", "") or species_id or "")
        entries: dict[str, dict] = {}  # ts -> this species' single dot layer
        if ce is not None:
            iid, jm = ce
            job_dir = job_dir_for(project_state, iid, jm, project_path)
            manifest = read_preview_manifest(job_dir) if job_dir else None
            for ts, entry in ((manifest or {}).get("tomograms") or {}).items():
                if not entry.get("picks_json"):
                    continue
                data = _read_picks_json(Path(entry["picks_json"]))
                dims = data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px")
                entries[ts] = {
                    "id": species_id or iid,
                    "label": label or iid,
                    "color": color,
                    "n": int(data.get("n") or len(data.get("picks") or [])),
                    "dots": _normalized_dots(data.get("picks") or [], dims),
                    # The extent the dots were normalized against — the tile's frame
                    # falls back to it when the geometry provider has no dims, so the
                    # box the dots are placed in stays the box they were computed for.
                    "dims": [int(dims[0]), int(dims[1])] if dims else None,
                }
        if species_id:
            # Hand-authored / imported / merged lists live in ProjectState, mapped into
            # the tomogram's voxel frame by the geometry provider — no MRC re-read per
            # tile. They fold into the species' existing layer rather than drawing the
            # same species twice on one tile.
            by_ts: dict[str, list] = {}
            for pl in getattr(project_state, "pick_lists", None) or []:
                if pl.species_id == species_id:
                    by_ts.setdefault(pl.tomo_name, []).append(pl)
            for ts, lists in by_ts.items():
                geom = geom_by_ts.get(ts)
                if geom is not None and geom.is_usable:
                    picks: list[dict] = []
                    for pl in lists:
                        picks.extend(_read_pick_list_voxels(Path(pl.path), list(geom.dims_xyz_px), geom.binned_apix))
                    n, dots = len(picks), _normalized_dots(picks, geom.dims_xyz_px)
                    dims = [int(geom.dims_xyz_px[0]), int(geom.dims_xyz_px[1])]
                else:
                    # No resolved frame ⇒ nothing can be placed. Carry the registered
                    # count anyway so the tile can say "picks here, not drawable" —
                    # dropping the species would read as "nothing picked".
                    n, dots, dims = sum(int(pl.count or 0) for pl in lists), [], None
                existing = entries.get(ts)
                if existing is not None:
                    existing["n"] += n
                    existing["dots"] = _stride_sample(existing["dots"] + dots, _MAX_DOTS)
                else:
                    entries[ts] = {
                        "id": species_id,
                        "label": label or species_id,
                        "color": color,
                        "n": n,
                        "dots": dots,
                        "dims": dims,
                    }
        for ts, entry in entries.items():
            if entry["n"]:
                out.setdefault(ts, []).append(entry)
    return out


def _sources_for(project_state, project_path: Path, geom: TomoGeometry) -> dict[str, dict]:
    """Per-tomogram image sources, keyed by the toolbar's source id.

    ``recon`` is the WarpTools PNG ts_reconstruct wrote next to the volume, falling
    back to the X/Y slab we render ourselves (the only option for an imported
    tomogram, which has no WarpTools output). Each denoisepredict job with a volume
    for this tomogram adds one entry under its method label. ``kick`` carries the
    (cache dir, source MRC) the renderer needs when the PNG isn't on disk yet.
    """
    from ui.particles.pick_viewer import _recon_slab_paths
    from ui.tomo_dashboard_dialog import _available_denoise_methods_for_ts, _denoise_slab_path

    out: dict[str, dict] = {}
    slab_dir = geom.tomograms_star.parent
    xy_png, _xz = _recon_slab_paths(slab_dir, geom.tomo_name)
    warp_png = _find_warp_tomo_preview(project_path, geom.tomo_name, geom.recon_mrc)
    if warp_png is not None:
        out[RECON_SOURCE] = {"png": warp_png, "kick": None, "caption": f"WarpTools · {warp_png.name}"}
    elif xy_png.exists() and not (geom.recon_mrc and is_output_stale(xy_png, [geom.recon_mrc])):
        out[RECON_SOURCE] = {"png": xy_png, "kick": None, "caption": f"X/Y slab · {xy_png.name}"}
    else:
        out[RECON_SOURCE] = {
            "png": None,
            "kick": (slab_dir, geom.recon_mrc) if geom.recon_mrc else None,
            "caption": "X/Y slab",
        }

    for label, job_dir, mrc in _available_denoise_methods_for_ts(project_state, project_path, geom.tomo_name):
        png = _denoise_slab_path(job_dir, geom.tomo_name)
        fresh = png.exists() and not is_output_stale(png, [mrc])
        out[label] = {
            "png": png if fresh else None,
            "kick": None if fresh else (job_dir, mrc),
            "caption": f"{label} · denoised X/Y slab",
        }
    return out


def collect_rows(project_state, project_path: Path) -> list[dict]:
    """One dict per tomogram, ordered by acquisition position. Disk-only — safe to
    call in a thread, which is where the page calls it from."""
    geoms = all_geometries(project_state, project_path)
    species_by_ts = _species_rows(project_state, project_path, geoms)
    rows: list[dict] = []
    for geom in geoms:
        species = species_by_ts.get(geom.tomo_name, [])
        # Frame aspect = the tomogram's own X/Y extent, else the extent the picks on it
        # were normalized against (the candidate-extract job read it when it wrote
        # picks.json, and can have it when the volume is no longer on disk). Neither ⇒
        # the frame is not the tomogram's box, so the tile lets the image size itself
        # and reports its picks as counted-but-unplaced rather than scattering them
        # against a guessed extent.
        aspect = tuple(geom.dims_xyz_px[:2]) if geom.dims_xyz_px else None
        if aspect is None:
            aspect = next((tuple(sp["dims"]) for sp in species if sp.get("dims")), None)
        rows.append(
            {
                "ts": geom.tomo_name,
                "label": position_label(geom.tomo_name)[0],
                "order": position_label(geom.tomo_name)[1],
                "aspect": aspect,
                "volume": str(geom.recon_mrc) if geom.recon_mrc else None,
                "source_label": geom.source,
                "sources": _sources_for(project_state, project_path, geom),
                "species": species,
            }
        )
    rows.sort(key=lambda r: (r["order"], r["ts"]))
    return rows


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
        self.species_on: dict[str, bool] = {}
        self._flight = SingleFlight()
        self._refs: dict[str, Any] = {}
        self._collected = False
        self._active = False
        # Slabs kicked off by this page land minutes later. Poll while the wall is the
        # visible view AND something is still pending — disk work, so 15 s (CLAUDE.md),
        # gated on a signature so a tick that changes nothing never touches the DOM.
        self._timer = ui.timer(15.0, self._tick)
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
        self.rows = await asyncio.to_thread(collect_rows, state, self.project_path)
        self._collected = True
        for sp in self._all_species():
            self.species_on.setdefault(sp["id"], True)

    async def _tick(self) -> None:
        if not self._active or not self._collected or not self._pending_count():
            return
        before = self._grid_signature()
        await self._collect()
        if self._grid_signature() != before:
            self.render()

    def set_active(self, on: bool) -> None:
        """The workspace's _switch_to hook: the pending-preview poll runs only while
        the wall is the visible view."""
        self._active = on
        try:
            self._timer.activate() if on else self._timer.deactivate()
        except Exception:
            pass  # timer torn down with the client

    # ── Derived state ─────────────────────────────────────────────────────────

    def _all_species(self) -> list[dict]:
        """Deduplicated species across all tiles, in first-seen (render-plan) order."""
        seen: dict[str, dict] = {}
        for row in self.rows:
            for sp in row["species"]:
                cur = seen.setdefault(sp["id"], {"id": sp["id"], "label": sp["label"], "color": sp["color"], "n": 0})
                cur["n"] += sp["n"]
        return list(seen.values())

    def _pending_count(self) -> int:
        """Tiles whose current source has no PNG yet but has a volume to render from."""
        return sum(
            1
            for row in self.rows
            if (src := row["sources"].get(self.source)) and src["png"] is None and src["kick"] is not None
        )

    def _grid_signature(self) -> tuple:
        """What the wall draws, cheaply: which tiles have an image and how many picks
        each carries. A poll that doesn't move this must not rebuild the DOM."""
        sig = []
        for row in self.rows:
            src = row["sources"].get(self.source)
            sig.append((row["ts"], src is not None and src["png"] is not None, tuple(sp["n"] for sp in row["species"])))
        return tuple(sig)

    def _source_options(self) -> list[tuple[str, str]]:
        """(key, label) for every source at least one tomogram can show."""
        keys: list[str] = []
        for row in self.rows:
            for key in row["sources"]:
                if key not in keys:
                    keys.append(key)
        keys.sort(key=lambda k: (k != RECON_SOURCE, k.lower()))
        return [(k, "recon" if k == RECON_SOURCE else k) for k in keys]

    # ── Render ────────────────────────────────────────────────────────────────

    def render(self) -> None:
        ensure_assets_loaded()
        self.container.clear()
        with self.container, ui.element("div").classes("cb-gal-page"):
            self._render_toolbar()
            body = ui.element("div").classes("cb-gal-body")
            self._refs["body"] = body
            with body:
                self._render_grid()

    def _rerender_grid(self) -> None:
        body = self._refs.get("body")
        if body is None:
            return
        body.clear()
        with body:
            self._render_grid()

    def _render_toolbar(self) -> None:
        sources = self._source_options()
        species = self._all_species()
        with ui.element("div").classes("cb-gal-toolbar"):
            ui.label("Tomograms").classes("cb-gal-title")
            ui.label(f"{len(self.rows)}").classes("cb-gal-count").tooltip(
                "Tomograms described by the reconstruct job's tomograms.star or imported into this project"
            )
            if len(sources) > 1:
                ui.label("source").classes("cb-gal-toolbar-label")
                if self.source not in dict(sources):
                    self.source = sources[0][0]
                self._refs["seg_source"] = render_segmented(sources, self.source, self._select_source)
            if species:
                ui.label("picks").classes("cb-gal-toolbar-label")
                for sp in species:
                    self._render_species_toggle(sp)
            ui.element("div").style("flex: 1;")
            ui.label("size").classes("cb-gal-toolbar-label")
            self._refs["seg_size"] = render_segmented([(k, lbl) for k, lbl, _w in _SIZES], self.size, self._select_size)
            house_button("Refresh", self._reload_click, tooltip="Re-read the tomograms and previews from disk")

    def _render_species_toggle(self, sp: dict) -> None:
        on = self.species_on.get(sp["id"], True)
        chip = ui.element("div").classes("cb-gal-sp" + ("" if on else " off"))
        chip.on("click", lambda _e, sid=sp["id"]: self._toggle_species(sid))
        chip.tooltip(f"{sp['label']} — {sp['n']} picks across the project. Click to show/hide its dots.")
        with chip:
            ui.element("div").classes("cb-gal-sp-dot").style(f"background: {sp['color']};")
            ui.label(sp["label"]).classes("cb-gal-sp-name")

    def _render_grid(self) -> None:
        if not self.rows:
            self._render_empty()
            return
        width = next(w for k, _lbl, w in _SIZES if k == self.size)
        kicks = 0
        with (
            ui.element("div")
            .classes("cb-gal-grid")
            .style(f"grid-template-columns: repeat(auto-fill, minmax({width}px, 1fr));")
        ):
            for row in self.rows:
                kicks += self._render_tile(row, kicked=kicks < _MAX_KICKS_PER_RENDER)

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
        src = row["sources"].get(self.source)
        spent = 0
        with ui.element("div").classes("cb-gal-tile"):
            frame = ui.element("div").classes("cb-gal-frame" + ("" if row["aspect"] else " cb-gal-frame-auto"))
            if row["aspect"]:
                frame.style(f"aspect-ratio: {row['aspect'][0]}/{row['aspect'][1]};")
            frame.on("click", lambda _e, r=row: self._zoom(r))
            frame.tooltip("Click to zoom")
            with frame:
                if src is None:
                    self._render_frame_note("not produced by this source")
                elif src["png"] is not None:
                    ui.html(f"<img src='{vis_asset_url(str(src['png']))}' alt='{row['ts']}' />", sanitize=False)
                    self._render_dot_layers(row, _DOT_R_WALL)
                elif src["kick"] is not None:
                    if kicked:
                        spent = 1
                        self._kick_slab(row, src)
                    self._render_frame_note("rendering slice…", spinner=True)
                else:
                    self._render_frame_note("no volume on disk")
                self._render_pick_badge(row)
            cap = ui.element("div").classes("cb-gal-cap")
            cap.on("click", lambda _e, ts=row["ts"]: self._open_journey(ts))
            cap.tooltip(f"{row['ts']} — open in the Journey")
            with cap:
                ui.label(row["label"]).classes("cb-gal-name")
                ui.label("↗").classes("cb-gal-go")
        return spent

    def _render_pick_badge(self, row: dict) -> None:
        """Pick count in the tile's corner. Counted-but-not-drawn is stated, not
        rounded away: without the tomogram's extent (or with picks written against a
        different one) there is no honest place to put a dot, and a bare count over an
        empty slice would read as a rendering bug."""
        visible = self._visible_species(row)
        total = sum(sp["n"] for sp in visible)
        if not total:
            return
        unplaced = sum(sp["n"] for sp in visible if not sp["dots"])
        if not unplaced:
            ui.label(str(total)).classes("cb-gal-badge").tooltip(
                " · ".join(f"{sp['label']}: {sp['n']}" for sp in visible if sp["n"])
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

    def _visible_species(self, row: dict) -> list[dict]:
        return [sp for sp in row["species"] if self.species_on.get(sp["id"], True)]

    def _render_dot_layers(self, row: dict, radius: float, *, cap: int | None = _WALL_DOTS) -> None:
        # No frame aspect means the frame box isn't the tomogram box, so a percentage
        # position in it means nothing. The badge says so; nothing is drawn.
        if not row["aspect"]:
            return
        for sp in self._visible_species(row):
            dots = _stride_sample(sp["dots"], cap) if cap else sp["dots"]
            if not dots:
                continue
            ui.html(_dots_svg(dots, sp["color"], radius), sanitize=False).classes("cb-gal-dotlayer")

    # ── Actions ───────────────────────────────────────────────────────────────

    def _select_source(self, key: str) -> None:
        # The segmented control flips its own highlight only when told to (it never
        # rebuilds itself), and only the grid below it needs re-rendering.
        self.source = key
        self._refs["seg_source"].set_active(key)
        self._rerender_grid()

    def _select_size(self, key: str) -> None:
        self.size = key
        self._refs["seg_size"].set_active(key)
        self._rerender_grid()

    def _toggle_species(self, species_id: str) -> None:
        self.species_on[species_id] = not self.species_on.get(species_id, True)
        self.render()  # the toolbar chip's own state changes too

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

        cache_dir, mrc = src["kick"]
        is_recon = self.source == RECON_SOURCE
        out_png = _recon_slab_paths(cache_dir, row["ts"])[0] if is_recon else _denoise_slab_path(cache_dir, row["ts"])

        async def _run(progress_cb):
            progress_cb(0, 0, "rendering X/Y slab…")
            return await asyncio.to_thread(render_xy_slab_preview, Path(mrc), out_png)

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
            toggle = self.callbacks.get("toggle_journey")
            if toggle is None:
                ui.notify("Journey not available in this view", type="warning")
                return
            await toggle()
            show = self.callbacks.get("journey_show_ts")
            if show is not None:
                await show(ts, section="reconstruct")

    def _zoom(self, row: dict) -> None:
        """Full-size view of one tile — same image, same dots, bigger. A dialog and not
        a mode: zooming is a look, and the routes onward (Journey) are one click away
        in the header either way."""
        src = row["sources"].get(self.source)
        with dialog_host(), ui.dialog() as dlg, ui.element("div").classes("cb-gal-zoom"):
            with ui.element("div").classes("cb-gal-zoom-head"):
                ui.label(row["label"]).classes("cb-gal-zoom-title")
                ui.label(row["ts"]).classes("cb-gal-zoom-ts")
                if row["source_label"] == SOURCE_IMPORTED:
                    ui.label("imported").classes("cb-gal-zoom-src").tooltip(
                        "Volume imported into this project rather than reconstructed by its own pipeline"
                    )
                if src is not None:
                    ui.label(src["caption"]).classes("cb-gal-zoom-src")
                ui.element("div").style("flex: 1;")
                for sp in self._visible_species(row):
                    with ui.element("div").classes("cb-gal-sp"):
                        ui.element("div").classes("cb-gal-sp-dot").style(f"background: {sp['color']};")
                        ui.label(f"{sp['label']} · {sp['n']}").classes("cb-gal-sp-name")

                async def _go(ts: str = row["ts"]) -> None:
                    dlg.close()
                    await self._open_journey(ts)

                house_button(
                    "Journey ↗",
                    _go,
                    tooltip="This tomogram's whole pipeline — motion, CTF, alignment, reconstruction, picks",
                )
                house_button("Close", dlg.close)
            frame = ui.element("div").classes(
                "cb-gal-frame cb-gal-zoom-frame" + ("" if row["aspect"] else " cb-gal-frame-auto")
            )
            if row["aspect"]:
                x, y = row["aspect"]
                # Cap the WIDTH so the height lands under the viewport at the tomogram's
                # own ratio (see .cb-gal-zoom-frame).
                frame.style(f"aspect-ratio: {x}/{y}; max-width: calc(74vh * {x} / {y});")
            with frame:
                if src is not None and src["png"] is not None:
                    ui.html(f"<img src='{vis_asset_url(str(src['png']))}' alt='{row['ts']}' />", sanitize=False)
                    self._render_dot_layers(row, _DOT_R_ZOOM, cap=None)
                else:
                    self._render_frame_note("no preview on disk for this source yet")
            if row["volume"]:
                ui.label(row["volume"]).classes("cb-gal-zoom-path").tooltip("Reconstructed volume on disk")
        dlg.open()


def _dots_svg(dots: list[tuple[float, float]], color: str, radius: float) -> str:
    """One species' dots as a single inline SVG.

    Percentage coordinates against the frame (which is the image box exactly), and a
    radius in user units — with no viewBox those are CSS pixels, so a dot keeps its
    size whether it sits on a 150 px thumbnail or the zoom view. One element per
    layer instead of one per pick: a 40-tomogram wall is thousands of dots.
    """
    circles = "".join(f'<circle cx="{fx:.2f}%" cy="{fy:.2f}%" r="{radius}"/>' for fx, fy in dots)
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none">'
        f'<g fill="{color}" stroke="rgba(255,255,255,0.85)" stroke-width="0.7">{circles}</g>'
        "</svg>"
    )

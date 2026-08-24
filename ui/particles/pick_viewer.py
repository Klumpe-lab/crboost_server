"""The pick viewer — slabs + pick lists + the cutout gallery for one tomogram.

Carved out of ``ui/tomo_dashboard_dialog.py`` (picking-UI roadmap 11-S1), which had grown
to 5.8k lines with this component welded into its middle. It is ONE component with TWO
mounts:

- ``mode="slim"`` — the Journey's Particles section, where it has always lived: look,
  filter, cross-link. No curation, no extraction verbs.
- ``mode="full"`` — the full page reached from the Particles registry's Picks & curation
  tab, filling the workspace main area: bigger slabs, curation mode, the per-list
  extraction verb, fullscreen.

Layout (both mounts): the LISTS STRIP runs full width across the top, and below it one row
with the slabs LEFT and the gallery RIGHT, tops aligned. The rail used to sit at the top of
the right column, which is what pushed the gallery a rail's-height below the slabs.

Two gallery backends share one chrome: the auto (PyTOM) list renders the subtomo atlas with
a Save/Reset dirty model, a workbench list (manual/imported/merged) renders a contact sheet
cut from the binned recon that auto-commits per click. Everything else — the slab canvas,
the ghost-dot layers, the hover bridge, the reference strip — is shared.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from pathlib import Path

from nicegui import ui

from services.dashboard_data import (
    SPECIES_OVERLAY_COLORS,
    glyph_for,
    job_dir_for,
    matching_subtomo_instance,
    position_label,
    read_tomograms_table,
    resolve_species,
    resolve_volume_for_3dmod,
    species_render_plan,
    split_species_id,
    template_match_instances,
    vis_asset_url,
)
from services.models_base import JobStatus, ListExtractionState, PickListType, PickSourceKind
from services.particles.list_ref import fs_slug
from services.project_state import get_project_state_for
from services.visualization.imod_vis import generate_candidate_vis
from services.visualization.preview_orchestrator import generate_candidate_previews, read_preview_manifest
from services.visualization.preview_render import is_output_stale, render_xy_slab_preview, render_xz_slab_preview
from services.visualization.tomo_geometry import APIX_MRC_HEADER, TomoGeometry, geometry_for_ts
from ui.components.buttons import house_button
from ui.components.reactive import SingleFlight
from ui.current_project import current_project_state
from ui.dashboard.figures import _build_score_hist_fig, _build_xy_scatter_fig, _build_xz_scatter_fig
from ui.particles.list_actions import extraction_badge

logger = logging.getLogger(__name__)


# Guards the Journey's own click handlers (new-species prompt, open-list-in-ArtiaX)
# against re-entry: a button can be destroyed + rebuilt mid-click by a dashboard
# refresh, so several clicks may land before one does — without this each would open a
# dialog. The shared pick-list actions (⚡ load / curate) carry their own guard in
# ui/particles/list_actions.py.
# See ui/components/reactive.py and CLAUDE.md "UI reactivity patterns".
_curation_flight = SingleFlight()


# Sticky per-(species_id, tomo) selected list slug so a background-render refresh
# (which rebuilds the whole tab body) doesn't bounce the user back to the auto list.
_SELECTED_LIST_SLUG: dict[tuple[str, str], str] = {}

# Rendered edge of one cutout tile, in px. It is also the sprite step used to read the
# atlas PNG (`background-position: -col*TILE -row*TILE`), so the atlases and every tile
# CSS rule (.cb-gallery-tile / .cb-noise-tile / .cb-template-tile in ui/dashboard/css.py)
# are locked to this number together — it is not a free display knob.
_DISPLAY_TILE_PX = 96

# Box-select arms ONLY in curation mode (11-S4): the grid carries `.cb-curating`
# while the toolbelt toggle is on, and the marquee refuses to start without it —
# so a drag across a look-only gallery scrolls/selects text as the user expects.
# A movement threshold separates click from drag, and the synthetic click the
# browser fires at the end of a drag is swallowed so it doesn't also toggle the
# tile under the cursor. Esc anywhere clears the selection; so does a click on
# empty gallery space.
# One global delegated handler per client (guarded) + lazy grid resolution so
# it survives lazily-mounted panels (not in the DOM at render time).
_MARQUEE_JS = """
(function() {
    if (window.__cbLassoInit) return;
    window.__cbLassoInit = true;
    var THRESH = 5;
    var rect = null, grid = null, sx = 0, sy = 0, shiftKey = false, pending = false, dragging = false;
    function place(cx, cy) {
        var gb = grid.getBoundingClientRect();
        rect.style.left = (Math.min(sx, cx) - gb.left + grid.scrollLeft) + 'px';
        rect.style.top = (Math.min(sy, cy) - gb.top + grid.scrollTop) + 'px';
        rect.style.width = Math.abs(cx - sx) + 'px';
        rect.style.height = Math.abs(cy - sy) + 'px';
    }
    document.addEventListener('keydown', function(e) {
        if (e.key !== 'Escape') return;
        document.querySelectorAll('.cb-gallery-scroll').forEach(function(g) {
            if (g.querySelector('.cb-gallery-tile.selected'))
                g.dispatchEvent(new CustomEvent('cbpickdeselect'));
        });
    });
    document.addEventListener('click', function(e) {
        // Click-away INSIDE a gallery (empty space between tiles) deselects.
        // Tile clicks are handled by their own listener and stop here.
        if (!e.target.closest) return;
        var g = e.target.closest('.cb-gallery-scroll');
        if (!g || e.target.closest('.cb-gallery-tile')) return;
        g.dispatchEvent(new CustomEvent('cbpickdeselect'));
    });
    document.addEventListener('mousedown', function(e) {
        if (e.button !== 0 || !e.target.closest) return;
        var g = e.target.closest('.cb-gallery-scroll.cb-curating');
        if (!g) return;
        grid = g; sx = e.clientX; sy = e.clientY; shiftKey = e.shiftKey;
        pending = true; dragging = false;  // don't preventDefault — keep clicks alive
    });
    document.addEventListener('mousemove', function(e) {
        if (!pending && !dragging) return;
        if (pending && (Math.abs(e.clientX - sx) > THRESH || Math.abs(e.clientY - sy) > THRESH)) {
            pending = false; dragging = true;
            rect = document.createElement('div');
            rect.className = 'cb-lasso-rect';
            grid.appendChild(rect);
            grid.classList.add('cb-lasso-dragging');
        }
        if (dragging) { e.preventDefault(); place(e.clientX, e.clientY); }
    });
    document.addEventListener('mouseup', function(e) {
        if (!dragging) { pending = false; grid = null; return; }  // plain click — let it through
        var rb = rect.getBoundingClientRect();
        var idxs = [], dropped = 0;
        grid.querySelectorAll('.cb-gallery-tile[data-pick-idx]').forEach(function(t) {
            if (t.classList.contains('cb-tile-degenerate')) return;
            var tb = t.getBoundingClientRect();
            var cx = tb.left + tb.width / 2, cy = tb.top + tb.height / 2;
            if (cx >= rb.left && cx <= rb.right && cy >= rb.top && cy <= rb.bottom) {
                idxs.push(parseInt(t.getAttribute('data-pick-idx'), 10));
                if (t.classList.contains('cb-tile-dropped')) dropped++;
            }
        });
        rect.remove(); rect = null;
        grid.classList.remove('cb-lasso-dragging');
        if (idxs.length) {
            // Symmetric: unify the box toward the opposite of its current
            // majority — mostly-kept → drop, mostly-dropped → restore (keep).
            // Shift forces keep (explicit restore). Ties drop.
            var keep = shiftKey ? true : (dropped > (idxs.length - dropped));
            grid.dispatchEvent(new CustomEvent('cbpicklasso', {detail: {idxs: idxs, keep: keep}}));
        }
        // Swallow the click the browser synthesizes from this drag so it
        // doesn't also toggle the tile under the pointer. Capture-phase, and
        // self-removing on the next tick whether or not a click fires.
        function sw(ev) { ev.stopPropagation(); ev.preventDefault(); }
        window.addEventListener('click', sw, true);
        setTimeout(function() { window.removeEventListener('click', sw, true); }, 0);
        dragging = false; pending = false; grid = null;
    });
})();
"""

# Slab lightbox (11-S5, full page only): click a slab → a full-viewport overlay of THAT
# slab with its pick markers, wheel to zoom, drag to pan, Esc/backdrop-click to close.
# The overlay is a DOM clone of the slab host, so the dots come along for free and stay
# in register with the image; every id is stripped from the clone so the hover bridge and
# the keep/drop dot-sync keep targeting the real layers and never the copy.
#
# Deviation from the stage text, recorded in the roadmap log: it says "wheel = Z". There
# is no Z stack to scroll — `render_xy_slab_preview` writes ONE central-Z-average PNG per
# tomogram, and rendering a slice per wheel tick would be a background render per tick
# (and browser volume rendering is a decision of record against). Wheel zooms instead,
# which is what a user reaching for a lightbox on a 1024-px projection actually wants.
_SLAB_LIGHTBOX_JS = """
(function() {
    if (window.__cbSlabLightbox) return;
    window.__cbSlabLightbox = true;
    var ov = null, inner = null, scale = 1, tx = 0, ty = 0, dragging = false, px = 0, py = 0;
    function apply() {
        if (inner) inner.style.transform = 'translate(' + tx + 'px,' + ty + 'px) scale(' + scale + ')';
    }
    function close() { if (ov) ov.remove(); ov = null; inner = null; dragging = false; }
    document.addEventListener('click', function(e) {
        if (!e.target || !e.target.closest) return;
        if (ov) { if (e.target === ov) close(); return; }
        if (e.target.closest('.cb-pick-ghost')) return;   // a dot click keeps its meaning
        var host = e.target.closest('.cb-viewer-full .cb-tomo-preview');
        if (!host) return;
        ov = document.createElement('div');
        ov.className = 'cb-slab-lightbox';
        inner = host.cloneNode(true);
        inner.classList.add('cb-slab-lightbox-inner');
        inner.removeAttribute('id');
        inner.querySelectorAll('[id]').forEach(function(n) { n.removeAttribute('id'); });
        scale = 1; tx = 0; ty = 0; apply();
        ov.appendChild(inner);
        document.body.appendChild(ov);
    });
    document.addEventListener('keydown', function(e) { if (e.key === 'Escape') close(); });
    document.addEventListener('wheel', function(e) {
        if (!ov) return;
        e.preventDefault();
        scale = Math.min(12, Math.max(1, scale * (e.deltaY < 0 ? 1.12 : 1 / 1.12)));
        if (scale === 1) { tx = 0; ty = 0; }
        apply();
    }, {passive: false});
    document.addEventListener('mousedown', function(e) {
        if (!ov || !inner) return;
        dragging = true; px = e.clientX; py = e.clientY; e.preventDefault();
    });
    document.addEventListener('mousemove', function(e) {
        if (!dragging) return;
        tx += e.clientX - px; ty += e.clientY - py; px = e.clientX; py = e.clientY; apply();
    });
    document.addEventListener('mouseup', function() { dragging = false; });
})();
"""


def _read_picks_json(path: Path) -> dict:
    if not path or not Path(path).exists():
        return {"picks": [], "tomo_dims_xyz_px": [0, 0, 0], "score_field": None, "n": 0}
    try:
        return json.loads(Path(path).read_text())
    except Exception as e:
        logger.warning("Failed to load picks.json %s: %s", path, e)
        return {"picks": [], "tomo_dims_xyz_px": [0, 0, 0], "score_field": None, "n": 0}


# Atlas-index parse cache (P3): path -> (mtime, meta). The cutout sheet re-reads
# the same index JSON on every visit; memoizing by mtime skips the parse on a warm
# revisit AND lets the sheet skip its loading spinner when the index is already in
# memory. Invalidated automatically when the index file is rewritten (new mtime).
_ATLAS_INDEX_MEMO: dict[str, tuple[float, dict]] = {}


def _read_atlas_index(index_path: Path) -> dict | None:
    if not index_path or not Path(index_path).exists():
        return None
    key = str(index_path)
    try:
        mtime = Path(index_path).stat().st_mtime
    except OSError:
        mtime = 0.0
    hit = _ATLAS_INDEX_MEMO.get(key)
    if hit is not None and hit[0] == mtime:
        return hit[1]
    try:
        meta = json.loads(Path(index_path).read_text())
        if not meta.get("index"):
            return None
        _ATLAS_INDEX_MEMO[key] = (mtime, meta)
        return meta
    except Exception as e:
        logger.warning("Could not parse cutout index %s: %s", index_path, e)
        return None


# Keep/drop derive cache (P2 + P3): derive_keep_state_for_list reads TWO stars
# (source + <slug>_filtered.star) to recover which rows survived curation. Both the
# rail count (P2) and the cutout sheet's keep overlay (P3) need it, and collect runs
# on every 4s refresh — so memoize by (source mtime, filtered mtime) to avoid
# re-reading two stars per list per tick. None = no filter committed (all kept).
_KEEP_STATE_MEMO: dict[str, tuple[tuple, set[int] | None]] = {}


def _memoized_keep_state(source_star: Path) -> set[int] | None:
    """``picks_filter.derive_keep_state_for_list`` memoized by the two stars' mtimes
    so the table count and the cutout keep overlay share one read. Returns the kept
    ROW indices of ``source_star`` (None when no ``_filtered`` star exists)."""
    from services.particles import picks_filter

    src = Path(source_star)
    filt = picks_filter.filtered_list_path(src)
    try:
        fmt = filt.stat().st_mtime if filt.exists() else None
    except OSError:
        fmt = None
    if fmt is None:
        return None  # no filtered star → all rows kept; nothing to read or cache
    try:
        smt = src.stat().st_mtime if src.exists() else 0.0
    except OSError:
        smt = 0.0
    key = str(src)
    sig = (smt, fmt)
    hit = _KEEP_STATE_MEMO.get(key)
    if hit is not None and hit[0] == sig:
        return hit[1]
    try:
        keep = picks_filter.derive_keep_state_for_list(src)
    except Exception:
        keep = None
    _KEEP_STATE_MEMO[key] = (sig, keep)
    return keep


def _render_invert_switch(root) -> None:
    """Polarity-invert toggle wired to add/remove `cb-invert-polarity` on
    `root`. The CSS flips every `.cb-tomo-preview > img` (and cutout tiles)
    under that root together, so density convention stays consistent."""

    def _on_invert(e, _root=root):
        if e.value:
            _root.classes(add="cb-invert-polarity")
        else:
            _root.classes(remove="cb-invert-polarity")

    (
        ui.switch("Invert", value=False, on_change=_on_invert)
        .props("dense color=indigo-6 left-label")
        .classes("text-[10px]")
        .tooltip(
            "Flip the apparent intensity of the tomogram slabs (and cutout tiles). "
            "The picker uses raw densities — this is viewer-only."
        )
    )


# ---------------------------------------------------------------------------
# Shared recon-stage tomogram canvas + multi-species pick overlay
#
# One X/Y + X/Z slab rendered from the reconstructed MRC (same percentile +
# flip pipeline as the cutout tiles, so Invert flips everything together),
# reused as the backdrop for every species' picks. Each candidate-extract
# instance's picks are drawn as a color-coded ghost-dot layer the user can
# toggle by species. Progressive: recon-only shows just the slabs; picks
# appear once candidate-extract has run.
# ---------------------------------------------------------------------------


_AUTO_KICKED_RECON_SLABS: set[str] = set()


def _recon_slab_paths(recon_job_dir: Path, ts_name: str) -> tuple[Path, Path]:
    base = recon_job_dir / "vis" / "slabs"
    return base / f"{ts_name}_xy.png", base / f"{ts_name}_xz.png"


def _render_recon_slabs_sync(mrc_path: Path, xy_png: Path, xz_png: Path) -> str:
    render_xy_slab_preview(Path(mrc_path), xy_png)
    render_xz_slab_preview(Path(mrc_path), xz_png)
    return "recon slabs rendered"


def _auto_kick_recon_slabs(recon_job_dir: Path, ts_name: str, mrc_path: Path, project_path: Path, refresh) -> None:
    """Render the shared X/Y + X/Z slabs in the background if missing/stale.
    Mirrors the candidate-extract preview auto-kick: module-level dedup set
    plus BackgroundTask dedup_key, refresh-on-complete so the canvas fills
    when the PNGs land."""
    key = f"{recon_job_dir}:{ts_name}"
    if key in _AUTO_KICKED_RECON_SLABS:
        return
    xy_png, xz_png = _recon_slab_paths(recon_job_dir, ts_name)
    fresh = (
        xy_png.exists()
        and xz_png.exists()
        and not is_output_stale(xy_png, [mrc_path])
        and not is_output_stale(xz_png, [mrc_path])
    )
    if fresh:
        return
    _AUTO_KICKED_RECON_SLABS.add(key)

    async def _run(progress_cb):
        import asyncio as _asyncio

        progress_cb(0, 0, "rendering tomogram slabs…")
        return await _asyncio.to_thread(_render_recon_slabs_sync, mrc_path, xy_png, xz_png)

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title=f"Render tomogram slabs · {ts_name}",
        subtitle="Shared canvas for the pick overlay",
        project_path=str(project_path),
        dedup_key=f"recon-slabs:{recon_job_dir}:{ts_name}",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)


# ── Per-list recon cutouts (the workbench contact sheet) ───────────────────────
# Workbench lists (manual/imported/merged) were never subtomo-extracted, so they
# have no subtomo atlas. We cut tiles straight from the binned recon at each pick
# voxel via services.visualization.recon_cutouts (same atlas PNG + index schema as
# the subtomo gallery), background-built like the recon slabs and shown read-only.
_AUTO_KICKED_LIST_CUTOUTS: set[str] = set()


def _list_cutout_paths(project_path: Path, species_id: str, tomo_name: str, slug: str) -> tuple[Path, Path]:
    """(atlas PNG, index JSON) cache paths for one workbench list's recon cutouts."""
    base = Path(project_path) / ".curation_sessions" / "cutouts" / fs_slug(species_id)
    stem = f"{fs_slug(tomo_name)}__{fs_slug(slug)}"
    return base / f"{stem}.png", base / f"{stem}.json"


def _list_cutout_box_px(sp: dict) -> int:
    """Cutout box edge in binned-recon px ≈ 2× particle diameter (context around
    the pick), clamped. Falls back to 48 when diameter/pixel size is unknown — this
    is a display window, not extraction geometry, so a generic one is honest."""
    species = current_project_state().get_species(sp.get("species_id") or "")
    diameter = float(getattr(sp.get("jm"), "particle_diameter_ang", 0.0) or 0.0) or float(
        getattr(species, "diameter_ang", 0.0) or 0.0
    )
    px = (sp.get("entry") or {}).get("pixel_size_ang")
    if diameter and px and px > 0:
        half = max(16, min(96, round(diameter / px)))
        return half * 2
    return 48


# ── Cutout display-filter UI (shared by the auto gallery + list sheet) ──
# Presets live in services/visualization/cutout_filters.py. The control is two
# compact selects: TYPE (None/Denoise/Local contrast/Bandpass/Lowpass) and, when
# the type has more than one option, PARAM (its intensity/cutoff). `on_select(key)`
# fires with the chosen preset key whenever either changes.
def _cutout_filter_tooltip(apix, apix_source: str) -> str:
    base = (
        "Display filter only — never changes the data, the picks, or any star file. "
        "Denoise / Local contrast usually read best on noisy tomograms; "
        "Lowpass / Bandpass are frequency cutoffs. "
    )
    if apix and apix > 0:
        return base + f"Frequency cutoffs use {apix:.2f} Å/px (from {apix_source}; e.g. 30 Å ≈ {30.0 / apix:.0f}px)."
    return base + "Pixel size is unknown (absent from the cutout header), so the frequency filters are hidden."


def _render_cutout_filter_controls(presets: list, apix, apix_source: str, on_select) -> None:
    """Render the compact TYPE (+ conditional PARAM) selects into the current row.
    `presets` are the preset dicts that have a variant atlas on disk (ordered).
    Calls `on_select(preset_key)` whenever the selection changes."""
    if len(presets) < 2:
        return
    types: list[tuple[str, str]] = []
    seen: set[str] = set()
    by_type: dict[str, list[dict]] = {}
    for p in presets:
        t = p.get("type", "raw")
        by_type.setdefault(t, []).append(p)
        if t not in seen:
            seen.add(t)
            types.append((t, p.get("type_label", t)))

    def _params(t: str) -> dict:
        return {p["key"]: (p.get("param_label") or "—") for p in by_type.get(t, [])}

    st = {"type": types[0][0], "key": by_type[types[0][0]][0]["key"]}

    type_sel = (
        ui.select({t: lbl for t, lbl in types}, value=st["type"])
        .props("dense outlined options-dense")
        .classes("text-xs")
        .style("min-width: 116px;")
    )
    param_sel = (
        ui.select(_params(st["type"]), value=st["key"])
        .props("dense outlined options-dense")
        .classes("text-xs")
        .style("min-width: 84px;")
    )
    param_sel.set_visibility(len(by_type[st["type"]]) > 1)

    def _on_type(e) -> None:
        t = e.value or types[0][0]
        st["type"] = t
        opts = _params(t)
        keys = list(opts)
        st["key"] = keys[0] if keys else t
        multi = len(keys) > 1
        param_sel.set_visibility(multi)
        if multi:
            param_sel.set_options(opts, value=st["key"])
        on_select(st["key"])

    def _on_param(e) -> None:
        st["key"] = e.value or st["key"]
        on_select(st["key"])

    type_sel.on_value_change(_on_type)
    param_sel.on_value_change(_on_param)
    with ui.icon("help_outline", size="13px").style("color: #94a3b8; cursor: help;"):
        ui.tooltip(_cutout_filter_tooltip(apix, apix_source)).style("font-size: 10px; max-width: 320px;")


def _atlas_index_is_current(index_path: Path) -> bool:
    """A cutout index from an older filter-preset set has a stale (or missing)
    `filter_schema`; treat those as stale so the current variant atlases get
    (re)generated."""
    from services.visualization.cutout_filters import FILTER_SCHEMA_VERSION

    meta = _read_atlas_index(index_path)
    return bool(meta) and meta.get("filter_schema") == FILTER_SCHEMA_VERSION


def _auto_kick_list_cutouts(
    recon_mrc: Path,
    picks: list,
    star_path: str | None,
    atlas_path: Path,
    index_path: Path,
    box_px: int,
    project_path: Path,
    refresh,
    dedup_key: str,
    apix_hint: float | None = None,
) -> bool:
    """True if the list's recon-cutout atlas is on disk + fresh (render now); else
    kick ONE background build (mirrors `_auto_kick_recon_slabs`) and return False
    so the caller shows a placeholder. `dedup_key` carries the star mtime, so a
    re-import (new mtime) rebuilds without needing a dashboard reopen."""
    sources = [Path(recon_mrc)] + ([Path(star_path)] if star_path else [])
    if (
        atlas_path.exists()
        and index_path.exists()
        and not is_output_stale(atlas_path, sources)
        and _atlas_index_is_current(index_path)
    ):
        return True
    if dedup_key in _AUTO_KICKED_LIST_CUTOUTS:
        return False
    _AUTO_KICKED_LIST_CUTOUTS.add(dedup_key)

    async def _run(progress_cb):
        import asyncio as _asyncio

        from services.visualization.recon_cutouts import render_recon_cutouts_atlas

        progress_cb(0, 0, "cutting tiles from the recon…")
        return await _asyncio.to_thread(
            render_recon_cutouts_atlas,
            Path(recon_mrc),
            picks,
            Path(atlas_path),
            Path(index_path),
            box_px=box_px,
            apix_hint=apix_hint,
        )

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title="Render list cutouts",
        subtitle="Recon-sourced tiles for a curation list",
        project_path=str(project_path),
        dedup_key=f"list-cutouts:{dedup_key}",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)
    return False


def _render_list_header(lst: dict, sp: dict, project_path: Path) -> None:
    """Swatch + label (+ merge provenance) for one workbench list, rendered inside a
    caller-provided row so the contact sheet and the building/empty states share one
    header. The 'Open in ArtiaX' action lived here too but was REDUNDANT with the rail
    toolbox's ⚡ (both open this tomo in ArtiaX) — removed per the user; its handler was
    kept as the W1 round-trip-edit foundation until roadmap 10-S2 closed W1 a different
    way (re-saving a `.coords` under the same name updates that list), so it is gone."""
    ui.element("div").classes(f"cb-species-swatch cb-swatch-{lst['shape']}").style(f"background: {lst['color']};")
    ui.label(lst["label"]).classes("cb-section-title")
    parents = lst.get("parent_slugs") or []
    if parents:
        ui.label("⋃ " + ", ".join(parents)).classes("cb-detail-meta").tooltip(
            "Merged from these lists (row order = type priority: manual/imported before auto)"
        )


def _render_list_cutouts_status(lst: dict, sp: dict, project_path: Path, *, building: bool) -> None:
    """Slim header row shown while a list's cutouts build (spinner) or when the
    build produced nothing (picks outside the recon / recon unreadable)."""
    with ui.element("div").classes("w-full").style("margin-top: 10px;"), ui.row().classes("items-center gap-2"):
        _render_list_header(lst, sp, project_path)
        ui.label(f"· {len(lst.get('picks', []))} picks").classes("cb-detail-meta")
        if building:
            ui.spinner(size="16px", color="indigo-500")
            ui.label("rendering cutouts from the recon…").style("font-size: 10px; color: #94a3b8;")
        else:
            ui.label("no cutouts (picks outside the recon, or recon unreadable)").style(
                "font-size: 10px; color: #94a3b8;"
            )


def _render_list_cutout_sheet(
    lst: dict,
    sp: dict,
    project_path: Path,
    atlas_meta: dict,
    atlas_path: str,
    initial_keep,
    refresh,
    *,
    mode: str = "slim",
) -> None:
    """Interactive contact sheet for one workbench list (manual/imported/merged):
    CSS-sprite tiles cut from the recon, each click-toggleable keep/drop. A dropped
    tile greys out AND greys its matching slab ghost-dot (the dot↔tile sync, via the
    list's own `_lid_xy/_lid_xz` canvas layers). Each toggle AUTO-COMMITS the kept
    subset to `<slug>_filtered.star` (no Save click) — so the selection persists across
    navigation and the merge + per-list extraction consume exactly the kept picks; the
    rail table's count cell live-updates to kept/total. "Reset" clears the filter (all
    kept). These lists are scoreless, so keep/discard IS the filter — there is no score
    threshold ([[feedback_per_list_extraction]])."""
    from services.particles import picks_filter

    index = atlas_meta.get("index", {})
    if not index:
        return
    cols = int(atlas_meta.get("cols", 8))
    rows = int(atlas_meta.get("rows", 1))
    atlas_url = vis_asset_url(atlas_path)
    tile = _DISPLAY_TILE_PX
    bg_w, bg_h = cols * tile, rows * tile
    all_idx = sorted(int(k) for k in index.keys())
    star_path = lst.get("path")
    # The list's own slab dot-layers (stashed by the canvas renderer) — used to
    # mirror keep/drop onto the ghost dots. Empty when the list isn't on the canvas.
    layer_ids = [lid for lid in (lst.get("_lid_xy"), lst.get("_lid_xz")) if lid]
    grid_id = f"cb-list-grid-{uuid.uuid4().hex[:8]}"
    client = ui.context.client
    tiles: dict[int, object] = {}
    # keep_set == None means "no curation yet — all kept implicitly"; it materializes
    # to a concrete set on the first toggle. Every toggle AUTO-COMMITS to
    # <slug>_filtered.star (serialized via `commit`) — so the selection persists across
    # navigation and the merge/extraction consume exactly the kept picks, no Save click.
    state = {"keep_set": set(initial_keep) if initial_keep is not None else None, "curating": False}
    sel: dict = {"idx": None}  # 11-S4: the selected tile, outside curation mode
    commit = {"running": False, "dirty": False}
    species_id = sp.get("species_id") or ""
    tomo_name = sp["row"]["tomo_name"]

    def _is_kept(i: int) -> bool:
        ks = state["keep_set"]
        return True if ks is None else i in ks

    def _dropped_now() -> list[int]:
        ks = state["keep_set"]
        return [] if ks is None else [i for i in all_idx if i not in ks]

    def _run_js(js: str) -> None:
        try:
            client.run_javascript(js)
        except Exception:
            pass  # client gone / slot torn down — the dot sync is best-effort cosmetic

    def _sync_all_dots() -> None:
        if not layer_ids:
            return
        _run_js(
            f"(function(){{var ls={json.dumps(layer_ids)};"
            f"var d=new Set({json.dumps([str(i) for i in _dropped_now()])});ls.forEach(function(lid){{"
            "var h=document.getElementById(lid);if(!h)return;"
            "h.querySelectorAll('.cb-pick-ghost[data-pick-idx]').forEach(function(g){"
            "if(d.has(g.getAttribute('data-pick-idx')))g.classList.add('cb-pick-ghost-dropped');"
            "else g.classList.remove('cb-pick-ghost-dropped');});});})();"
        )

    def _counter_txt() -> str:
        # Count kept among the VISIBLE tiles only — keep_set may carry tile-less
        # row indices (out-of-bounds picks kept in a prior filter), which must not
        # inflate the counter past the tile count.
        ks = state["keep_set"]
        kept = len(all_idx) if ks is None else sum(1 for i in all_idx if i in ks)
        return f"kept {kept}/{len(all_idx)}"

    def _update_count_cell(total: int, fc) -> None:
        # Live-update this list's count cell in the rail table (shared `lst` dict).
        el = lst.get("_count_el")
        if el is None:
            return
        try:
            el.set_text(_list_count_text(total, fc))
        except Exception:
            pass  # rail torn down — best-effort cosmetic

    async def _commit_loop() -> None:
        """Auto-commit the current keep/drop to <slug>_filtered.star, serialized so
        rapid toggles can't race or hammer Lustre: a toggle arriving mid-write just
        flags `dirty` and the running loop re-writes the final state. Drops empty →
        the filtered star is discarded (revert to all-kept). Persists the kept count
        on the PickList so the table survives navigation."""
        if not star_path:
            return
        if commit["running"]:
            commit["dirty"] = True
            return
        commit["running"] = True
        try:
            while True:
                commit["dirty"] = False
                ks = state["keep_set"]
                dropped = (set(all_idx) - ks) if ks is not None else set()
                try:
                    if dropped:
                        res = await asyncio.to_thread(picks_filter.save_filtered_list, Path(star_path), set(dropped))
                        kept, total = int(res["kept"]), int(res["total"])
                    else:
                        await asyncio.to_thread(picks_filter.discard_filtered_list, Path(star_path))
                        kept = total = len(lst.get("picks") or [])
                except Exception:
                    logger.exception("keep/drop auto-commit failed for %s", star_path)
                    break
                fc = None if kept == total else kept
                st = current_project_state()
                pl = st.get_pick_list(lst["slug"], species_id, tomo_name)
                if pl is not None and pl.filtered_count != fc:
                    pl.filtered_count = fc
                    from backend import get_backend

                    await get_backend().save_project(st.project_path, force=True)
                lst["filtered_count"] = fc
                _update_count_cell(total, fc)
                if not commit["dirty"]:
                    break
        finally:
            commit["running"] = False

    async def _toggle(i: int) -> None:
        ks = state["keep_set"]
        if ks is None:
            ks = set(all_idx)  # materialize keep-all, then flip this one
            state["keep_set"] = ks
        kept = i not in ks  # state AFTER the toggle
        if kept:
            ks.add(i)
        else:
            ks.discard(i)
        t = tiles.get(i)
        if t is not None:
            if kept:
                t.classes(remove="cb-tile-dropped")
            else:
                t.classes(add="cb-tile-dropped")
        if layer_ids:
            _run_js(
                "(function(){{var ls={ls};var idx='{i}';var drop={drop};ls.forEach(function(lid){{"
                "var h=document.getElementById(lid);if(!h)return;"
                "h.querySelectorAll('.cb-pick-ghost[data-pick-idx=\"'+idx+'\"]').forEach(function(g){{"
                "if(drop)g.classList.add('cb-pick-ghost-dropped');else g.classList.remove('cb-pick-ghost-dropped');"
                "}});}});}})();".format(ls=json.dumps(layer_ids), i=i, drop="false" if kept else "true")
            )
        counter.set_text(_counter_txt())
        await _commit_loop()  # auto-save the keep/drop → persists + feeds merge/extraction

    async def _on_lasso(e) -> None:
        """Box-select over this sheet, same contract as the subtomo gallery's: one
        uniform action over the enclosed tiles (mostly-kept → drop, mostly-dropped →
        restore; ⇧ forces keep), committed once at the end rather than per tile."""
        if not state["curating"]:
            return  # the marquee only arms in curation mode; belt-and-braces server-side
        args = e.args or {}
        keep = bool(args.get("keep"))
        ks = state["keep_set"]
        if ks is None:
            ks = set(all_idx)
            state["keep_set"] = ks
        changed = False
        for raw in args.get("idxs") or []:
            try:
                i = int(raw)
            except (TypeError, ValueError):
                continue
            if keep and i not in ks:
                ks.add(i)
            elif not keep and i in ks:
                ks.discard(i)
            else:
                continue
            changed = True
            t = tiles.get(i)
            if t is not None:
                if keep:
                    t.classes(remove="cb-tile-dropped")
                else:
                    t.classes(add="cb-tile-dropped")
        if not changed:
            return
        _sync_all_dots()
        counter.set_text(_counter_txt())
        await _commit_loop()

    async def _on_reset() -> None:
        state["keep_set"] = None
        for t in tiles.values():
            t.classes(remove="cb-tile-dropped")
        _sync_all_dots()
        counter.set_text(_counter_txt())
        await _commit_loop()  # discards the filtered star + clears the kept count
        ui.notify("Reset — all picks kept", type="positive", timeout=1500)

    def _install_hover_bridge() -> None:
        """Bidirectional hover brushing for THIS list: hover a tile → glow its slab
        ghost-dot(s); hover a ghost-dot → glow it + highlight/scroll-to its tile.
        Delegated on the Particles section card (the stable ancestor of both the
        canvas and this sheet), grid re-resolved lazily — mirrors the auto gallery
        bridge, but the handler pair is stashed on the card and the prior one removed
        first, so re-renders don't stack duplicate listeners. No-op without the
        list's canvas dot-layers."""
        if not layer_ids:
            return
        _run_js(
            f"""
            setTimeout(function() {{
                var gid = {json.dumps(grid_id)};
                var lids = {json.dumps(layer_ids)};
                if (!document.getElementById(gid)) return;
                var root = document.getElementById(gid).closest('.cb-section-card') || document.body;
                function getGrid() {{ return document.getElementById(gid); }}
                function layers() {{
                    return lids.map(function(id) {{ return document.getElementById(id); }}).filter(Boolean);
                }}
                function ourGhost(el) {{
                    var l = el.closest && el.closest('.cb-pick-layer');
                    return !!l && lids.indexOf(l.id) !== -1;
                }}
                function ghostsFor(idx) {{
                    var out = [];
                    layers().forEach(function(h) {{
                        h.querySelectorAll('.cb-pick-ghost[data-pick-idx="' + idx + '"]')
                         .forEach(function(g) {{ out.push(g); }});
                    }});
                    return out;
                }}
                function clearActive() {{
                    layers().forEach(function(h) {{
                        h.querySelectorAll('.cb-pick-ghost.cb-ghost-active')
                         .forEach(function(g) {{ g.classList.remove('cb-ghost-active'); }});
                    }});
                    var gr = getGrid();
                    if (gr) gr.querySelectorAll('.cb-tile-highlight')
                            .forEach(function(t) {{ t.classList.remove('cb-tile-highlight'); }});
                }}
                function isOurs(el) {{
                    if (!el || !el.closest) return false;
                    var gr = getGrid();
                    var t = el.closest('.cb-gallery-tile[data-pick-idx]');
                    if (t && gr && gr.contains(t)) return true;
                    var gh = el.closest('.cb-pick-ghost[data-pick-idx]');
                    return !!(gh && ourGhost(gh));
                }}
                if (root._cbListBridge) {{
                    root.removeEventListener('mouseover', root._cbListBridge.over);
                    root.removeEventListener('mouseout', root._cbListBridge.out);
                }}
                var over = function(e) {{
                    if (!e.target.closest) return;
                    var gr = getGrid();
                    var t = e.target.closest('.cb-gallery-tile[data-pick-idx]');
                    if (t && gr && gr.contains(t)) {{
                        var idx = t.getAttribute('data-pick-idx');
                        clearActive();
                        ghostsFor(idx).forEach(function(g) {{ g.classList.add('cb-ghost-active'); }});
                        return;
                    }}
                    var gh = e.target.closest('.cb-pick-ghost[data-pick-idx]');
                    if (gh && ourGhost(gh)) {{
                        var gi = gh.getAttribute('data-pick-idx');
                        clearActive();
                        ghostsFor(gi).forEach(function(g) {{ g.classList.add('cb-ghost-active'); }});
                        var g2 = getGrid();
                        if (g2) {{
                            var tl = g2.querySelector('.cb-gallery-tile[data-pick-idx="' + gi + '"]');
                            if (tl) {{
                                tl.classList.add('cb-tile-highlight');
                                var tr = tl.getBoundingClientRect(), gb = g2.getBoundingClientRect();
                                if (tr.top < gb.top || tr.bottom > gb.bottom)
                                    tl.scrollIntoView({{block: 'nearest', behavior: 'smooth'}});
                            }}
                        }}
                    }}
                }};
                var out = function(e) {{
                    if (!e.target.closest) return;
                    var lv = e.target.closest('.cb-gallery-tile[data-pick-idx]') ||
                             e.target.closest('.cb-pick-ghost[data-pick-idx]');
                    if (!lv || !isOurs(lv)) return;
                    if (!isOurs(e.relatedTarget)) clearActive();
                }};
                root._cbListBridge = {{over: over, out: out}};
                root.addEventListener('mouseover', over);
                root.addEventListener('mouseout', out);
            }}, 60);
            """
        )

    from services.visualization.cutout_filters import get_filter_presets, keyed_path

    _apix = atlas_meta.get("apix")
    _apix_source = atlas_meta.get("apix_source") or "unknown"
    _list_filter_presets = [
        p for p in get_filter_presets() if p["key"] == "raw" or keyed_path(Path(atlas_path), p["key"]).exists()
    ]

    def _on_list_select(key: str) -> None:
        # Swap each tile's background-image to the chosen variant atlas, in place —
        # no rebuild, so the keep/drop handlers and dot sync survive the switch.
        url = vis_asset_url(str(keyed_path(Path(atlas_path), key)))
        for t in tiles.values():
            try:
                t.style(add=f"background-image: url({url});")
            except Exception:
                pass  # tile torn down — best-effort

    def _select(i: int | None) -> None:
        """Selection outside curation mode — the same click model the subtomo gallery
        uses (11-S4). One tile at a time; its slab dots light with it."""
        prev = sel["idx"]
        if prev is not None and prev in tiles:
            tiles[prev].classes(remove="selected")
        sel["idx"] = i
        if i is not None and i in tiles:
            tiles[i].classes(add="selected")
        if not layer_ids:
            return
        _run_js(
            f"(function(){{var ls={json.dumps(layer_ids)};var sel={json.dumps('' if i is None else str(i))};"
            "ls.forEach(function(lid){var h=document.getElementById(lid);if(!h)return;"
            "h.querySelectorAll('.cb-pick-ghost.cb-ghost-selected').forEach(function(d){"
            "d.classList.remove('cb-ghost-selected');});"
            "if(sel)h.querySelectorAll('.cb-pick-ghost[data-pick-idx=\"'+sel+'\"]').forEach(function(d){"
            "d.classList.add('cb-ghost-selected');});});})();"
        )

    def _on_tile_click(i: int):
        """Curation mode ON → keep/drop (async: it auto-commits the filtered star).
        OFF → select. Returning the coroutine lets NiceGUI await it."""
        if state["curating"]:
            return _toggle(i)
        _select(None if sel["idx"] == i else i)
        return None

    def _set_curating(on: bool) -> None:
        state["curating"] = bool(on)
        if curate_btn is not None:
            if on:
                curate_btn.classes(add="cb-tb-on")
            else:
                curate_btn.classes(remove="cb-tb-on")
        if on:
            grid_el.classes(add="cb-curating")
        else:
            grid_el.classes(remove="cb-curating")
        curation_bar.set_visibility(bool(on))
        hint.set_text(_hint_text())

    def _hint_text() -> str:
        if state["curating"]:
            return "click a tile to keep/drop — saved automatically · hover to find it on the slab"
        if mode == "full":
            return "click a tile to select it · hover to find it on the slab · arm ✎ to keep/drop"
        return "click a tile to select it · hover to find it on the slab · curate on the full viewer"

    with ui.element("div").classes("cb-cutouts-box w-full").style("margin-top: 10px;"):
        with ui.row().classes("cb-gallery-head w-full items-center gap-2 no-wrap"):
            _render_list_header(lst, sp, project_path)
            counter = ui.label(_counter_txt()).classes("cb-filter-counter")
            ui.space()
            with ui.row().classes("cb-toolbelt items-center gap-0 no-wrap"):
                if len(_list_filter_presets) >= 2:
                    fbtn = _toolbelt_button("tune", "Display filter — how the cutouts are rendered, never the data")
                    with fbtn, ui.menu().props("anchor='bottom right' self='top right'"):
                        with ui.element("div").classes("cb-info-card cb-tb-card"):
                            with ui.row().classes("items-center gap-2 no-wrap"):
                                _render_cutout_filter_controls(
                                    _list_filter_presets, _apix, _apix_source, _on_list_select
                                )
                # A workbench list has no per-pick peek (no subtomo extraction behind
                # it), so the popover carries the per-tomogram command only.
                _render_3dmod_popover(sp["row"], with_peek=False)
                curate_btn = None
                if mode == "full":
                    # Curation belongs to the full page (11-S6): the Journey's mount is
                    # look-and-filter, and a keep/drop there would be a second place to
                    # change what downstream consumes.
                    curate_btn = _toolbelt_button(
                        "edit",
                        "Curation mode — while armed, clicking a tile or a slab dot keeps/drops it and the "
                        "kept subset auto-saves to this list's filtered star. Off, a click only selects.",
                    )
                    curate_btn.on("click", lambda: _set_curating(not state["curating"]))
                    _toolbelt_button("fullscreen", "Fill the browser window with the viewer (Esc exits)").on(
                        "click", js_handler=_FULLSCREEN_JS
                    )
        curation_bar = ui.row().classes("cb-curation-bar w-full items-center gap-2")
        curation_bar.set_visibility(False)
        with curation_bar:
            ui.space()
            house_button("Reset", _on_reset, tooltip="Clear this list's keep/drop — revert to all picks kept")
        hint = ui.label(_hint_text()).style("font-size: 9px; color: #94a3b8; margin-bottom: 4px;")
        grid_el = ui.element("div").classes("cb-gallery-scroll").style("display: flex; flex-wrap: wrap; gap: 4px;")
        grid_el._props["id"] = grid_id
        with grid_el:
            for i in all_idx:
                pos = index.get(str(i))
                if not pos:
                    continue
                r, c = pos
                cls = "cb-gallery-tile" if _is_kept(i) else "cb-gallery-tile cb-tile-dropped"
                t = (
                    ui.element("div")
                    .classes(cls)
                    .style(
                        f"background-image: url({atlas_url}); background-size: {bg_w}px {bg_h}px; "
                        f"background-position: {-c * tile}px {-r * tile}px;"
                    )
                )
                t._props["title"] = f"#{i}"
                t._props["data-pick-idx"] = str(i)
                t.on("click", lambda _e, i=i: _on_tile_click(i))
                tiles[i] = t
    # Same two grid-scoped custom events the subtomo gallery listens for, dispatched
    # by the one global marquee/deselect handler (see `_MARQUEE_JS`).
    grid_el.on("cbpicklasso", _on_lasso, js_handler="(e) => emit(e.detail)")
    grid_el.on("cbpickdeselect", lambda _e: _select(None), js_handler="(e) => emit({})")
    _sync_all_dots()
    _install_hover_bridge()
    ui.run_javascript(_MARQUEE_JS)


def _render_list_extraction_line(sp: dict, lst: dict, project_path: Path, refresh) -> None:
    """One line above a workbench list's contact sheet naming its subtomo-extraction state
    and, in the full viewer, offering the verb (11-S5).

    The tiles below are cut from the binned RECON, so a sheet full of them says nothing
    about whether subtomograms were ever extracted — the two are unrelated, and that is
    exactly the confusion this line closes. The button calls the same
    ``list_actions.extract_list`` the registry row does; the ref is built at click time
    because ``tomograms_star_for`` reads stars."""
    species_id = sp.get("species_id") or ""
    tomo_name = sp["row"]["tomo_name"]
    pl = current_project_state().get_pick_list(lst["slug"], species_id, tomo_name)
    est = pl.extraction_state() if pl is not None else ListExtractionState.NOT_EXTRACTED
    text, cls = extraction_badge(est)

    async def _extract() -> None:
        from backend import get_backend
        from services.particles.list_ref import list_ref_for, species_anchors, tomograms_star_for
        from ui.particles import list_actions

        st = current_project_state()
        anchors = await asyncio.to_thread(species_anchors, st, project_path, species_id)
        tstar = await asyncio.to_thread(tomograms_star_for, st, project_path, species_id, tomo_name)
        ref = list_ref_for(
            project_path,
            anchors,
            species_id,
            tomo_name,
            lst["slug"],
            label=lst.get("label") or lst["slug"],
            list_type=lst["list_type"],
            star_path=lst.get("path"),
            tomograms_star=tstar,
        )
        await list_actions.extract_list(get_backend(), ref, on_done=refresh)

    with ui.row().classes("w-full items-center gap-2").style("margin-top: 8px;"):
        if text:
            ui.label(text).classes(f"cb-ltable-badge {cls}")
        if est == ListExtractionState.EXTRACTED:
            ui.label("subtomograms extracted for this list").classes("cb-detail-meta")
        else:
            ui.label("no subtomograms extracted for this list").classes("cb-detail-meta")
        ui.space()
        house_button(
            "Re-extract" if est != ListExtractionState.NOT_EXTRACTED else "Extract",
            _extract,
            tooltip=(
                "Cut this list's subtomograms with the species' committed extraction geometry — the same "
                "action as the Particles registry's per-row ⚗, on the kept subset when a keep/drop filter exists."
            ),
        )


async def _render_single_list_cutouts(sp: dict, lst: dict, project_path: Path, refresh, *, mode: str = "slim") -> None:
    """Detail pane for ONE workbench list (manual/imported/merged): a read-only
    recon-sourced cutout sheet (these lists were never subtomo-extracted, so tiles
    are cut from the binned recon at each pick voxel). Carved from the old per-list
    loop so the rail's detail pane can show a single selected list.

    The merged-list overlap/dedup panel moved to the Particles registry's Picks & curation
    tab (which acts on every tomogram at once) and stays there. The per-list EXTRACT verb
    comes back in ``mode="full"`` only (11-S5) — it is the same `list_actions.extract_list`
    the registry row fires, not a second implementation, and it is the answer to standing
    in front of a list and seeing that no subtomograms were ever cut from it.

    The read-only disk probes (recon/star stat, atlas staleness, atlas-index read)
    run OFF the event loop via ``asyncio.to_thread`` so selecting a list doesn't
    freeze the whole UI on Lustre latency — a spinner shows until they return."""
    recon = (sp.get("row") or {}).get("vol_path")

    def _no_recon() -> None:
        with ui.element("div").classes("cb-empty"):
            ui.icon("image_not_supported", size="24px").classes("text-gray-400")
            ui.label("No reconstructed tomogram on disk — cutouts unavailable.").classes("text-xs")
            ui.label("This list's dots still overlay the slab canvas on the left.").classes(
                "text-[11px] italic text-gray-500"
            )

    if not recon:
        _no_recon()
        return
    species_id = sp.get("species_id") or ""
    tomo_name = sp["row"]["tomo_name"]
    if mode == "full":
        _render_list_extraction_line(sp, lst, project_path, refresh)
    box_px = _list_cutout_box_px(sp)
    atlas_path, index_path = _list_cutout_paths(project_path, species_id, tomo_name, lst["slug"])
    star_path = lst.get("path")

    # Spinner while the disk probes run off-loop (Lustre stat/read latency was the
    # "laggy on switch" freeze — it blocked the event loop mid-click). P3: on a warm
    # REVISIT the atlas index is already in memory and the keep-state derive is
    # memoized, so the probe returns near-instantly — skip the "loading cutouts…"
    # spinner that otherwise flashes on every open and reads as a full re-render.
    warm = str(index_path) in _ATLAS_INDEX_MEMO
    pending_box = None
    if warm:
        logger.info("list-cutouts[%s/%s]: atlas cache HIT — rendering without spinner", species_id, lst["slug"])
    else:
        with ui.element("div").classes("w-full").style("margin-top: 10px;") as pending_box:
            with ui.row().classes("items-center gap-2"):
                ui.spinner(size="16px", color="indigo-500")
                ui.label("loading cutouts…").style("font-size: 10px; color: #94a3b8;")

    def _probe() -> dict:
        recon_exists = Path(recon).exists()
        try:
            star_sig = int(Path(star_path).stat().st_mtime) if star_path and Path(star_path).exists() else 0
        except OSError:
            star_sig = 0
        sources = [Path(recon)] + ([Path(star_path)] if star_path else [])
        fresh = (
            atlas_path.exists()
            and index_path.exists()
            and not is_output_stale(atlas_path, sources)
            and _atlas_index_is_current(index_path)
        )
        # Existing keep/drop curation for this list (centered-Å match against
        # <slug>_filtered.star), derived off-loop here so the interactive sheet opens
        # already reflecting saved drops. Memoized by mtime (shared with the rail
        # count). None = no filter yet (all kept).
        keep = _memoized_keep_state(Path(star_path)) if star_path else None
        return {
            "recon_exists": recon_exists,
            "star_sig": star_sig,
            "atlas_meta": _read_atlas_index(index_path) if fresh else None,
            "index_exists": index_path.exists(),
            "keep": keep,
        }

    io = await asyncio.to_thread(_probe)
    if pending_box is not None:
        pending_box.delete()

    if not io["recon_exists"]:
        _no_recon()
        return
    atlas_meta = io["atlas_meta"]
    if atlas_meta is None:
        # Cold path: kick ONE background build (stays on-loop — it installs a
        # ui.timer) and fall through to the building/empty status.
        dedup_key = f"{species_id}:{tomo_name}:{lst['slug']}:{io['star_sig']}"
        if _auto_kick_list_cutouts(
            Path(recon),
            lst["picks"],
            star_path,
            atlas_path,
            index_path,
            box_px,
            project_path,
            refresh,
            dedup_key,
            apix_hint=(sp.get("entry") or {}).get("pixel_size_ang"),
        ):
            atlas_meta = _read_atlas_index(index_path)  # rare: built between probe and now
    if atlas_meta:
        _render_list_cutout_sheet(
            lst, sp, project_path, atlas_meta, str(atlas_path), io.get("keep"), refresh, mode=mode
        )
    else:
        # No tiles yet: a written index means the build ran (empty result);
        # no index means it's still in flight.
        _render_list_cutouts_status(lst, sp, project_path, building=not io["index_exists"])


# Merging and the merged-list overlap/dedup panel left the Journey in 11-S3: both are
# per-list ACTIONS, and the Picks & curation tab owns those across every tomogram
# (`ui/species/picks_tab.py` → `list_actions.merge_lists` / `open_dedup_dialog`). The
# popup that preceded the inline merge bar died 2026-06-11; see W3 in
# docs/ARTIAX_BRIDGE_PLAN.md.


def _render_pick_layer(picks: list, color: str, dims: list | None, axis: str, layer_id: str, shape: str = "circle"):
    """Render one pick list's ghost-dot layer over a shared slab canvas.

    Carries a stable DOM `layer_id` and per-dot `data-pick-idx` plus a
    `.cb-pick-marker` so the gallery↔canvas hover bridge in
    `_render_gallery_body` can target this exact list's dots (the bridge
    does `getElementById(layer_id)` then scopes its querySelector to it).
    `shape` (circle/diamond/square/triangle) is the per-list glyph, applied on
    the layer so it cascades to every child dot. Returns the layer element so a
    checkbox can toggle its visibility. With unknown ``dims`` the layer renders
    empty — a dot needs the binned extent to be placed, and placing it against a
    stand-in extent is how picks end up quietly in the wrong corner."""
    x_dim = max(int(dims[0]), 1) if dims else 0
    y_dim = max(int(dims[1]), 1) if dims else 0
    z_dim = max(int(dims[2]), 1) if dims else 0
    if not (x_dim and y_dim and z_dim):
        picks = []
    # `--sp-color` cascades to every child .cb-pick-ghost (the restyle rule
    # reads it via var()), so the species color survives the !important dot
    # styling without per-dot inline overrides.
    layer = ui.element("div").classes(f"cb-pick-layer cb-shape-{shape}").style(f"--sp-color: {color};")
    layer._props["id"] = layer_id
    with layer:
        for p in picks:
            try:
                idx = int(p.get("i"))
                fx = max(0.0, min(1.0, float(p.get("x", 0)) / x_dim))
                if axis == "xy":
                    fv = 1.0 - max(0.0, min(1.0, float(p.get("y", 0)) / y_dim))
                else:
                    fv = 1.0 - max(0.0, min(1.0, float(p.get("z", 0)) / z_dim))
            except (TypeError, ValueError):
                continue
            dot = ui.element("div").classes("cb-pick-ghost")
            dot._props["data-pick-idx"] = str(idx)
            dot.style(f"left: {fx * 100:.3f}%; top: {fv * 100:.3f}%;")
        # Hover marker the bridge moves to the hovered pick (one per layer).
        ui.element("div").classes("cb-pick-marker")
    return layer


# ---------------------------------------------------------------------------
# Template Match size chips — reused by the per-species tab sanity strip.
# ---------------------------------------------------------------------------


def _read_pick_list_voxels(star_path: Path, dims: list | None, pixel_size: float | None) -> list[dict]:
    """Read a centered-Å pick star → voxel-space picks ``[{i, x, y, z}]`` for the
    canvas overlay, using the binned ``dims`` + ``pixel_size`` already resolved in
    the render context (no MRC re-read per render). Returns ``[]`` if the file,
    its deps, or the centered-coord columns are unavailable — so a missing/changed
    list degrades to 'nothing drawn' rather than breaking the dashboard render.
    Unknown dims/pixel size land here too: no overlay beats an overlay drawn at a
    guessed scale (the geometry chip in the section header says why)."""
    if not star_path or not Path(star_path).exists() or not pixel_size or pixel_size <= 0 or not dims:
        return []
    try:
        import starfile

        from services.particles.coords import CENTERED_COLS, centered_angst_to_voxel

        data = starfile.read(star_path, always_dict=True)
        df = None
        for v in data.values():
            if hasattr(v, "columns") and all(c in v.columns for c in CENTERED_COLS):
                df = v
                break
        if df is None or len(df) == 0:
            return []
        coords = df[CENTERED_COLS].to_numpy(dtype=float)
        vox = centered_angst_to_voxel(coords, [int(dims[0]), int(dims[1]), int(dims[2])], float(pixel_size))
        return [{"i": i, "x": float(vox[i][0]), "y": float(vox[i][1]), "z": float(vox[i][2])} for i in range(len(vox))]
    except Exception as e:
        logger.warning("Could not read pick list %s: %s", star_path, e)
        return []


def _collect_pick_lists_for_species(sp: dict, project_state, ts_name: str) -> list[dict]:
    """The pick lists overlaid for one species on this TS, each render-ready:
    {slug, label, list_type, color, shape, picks, dims, visible}.

    The `auto` PyTOM list comes from picks.json (always present when picked).
    Workbench-authored lists (manual/imported/merged) come from the ProjectState
    registry — each backed by a centered-Å star whose coords are mapped into the
    same voxel space as the auto picks so they overlay on the shared canvas. The
    canvas/toggle/cutout machinery already iterates this list, so a new type is
    purely another entry here."""
    lists: list[dict] = []
    auto_picks = sp.get("picks") or []
    if auto_picks:
        lists.append(
            {
                "slug": "auto",
                "label": sp["label"],
                "list_type": PickListType.AUTO,
                "color": sp["color"],
                "shape": glyph_for(PickListType.AUTO),
                "picks": auto_picks,
                "dims": sp["dims"],
                "visible": True,
                "filtered_count": sp.get("auto_kept_count"),
                "source_kind": PickSourceKind.TM.value,
                "source_ref": sp.get("iid") or "",
            }
        )
    species_id = sp.get("species_id") or ""
    if species_id:
        # dims / pixel_size may be None (geometry unresolved) — `_read_pick_list_voxels`
        # then draws nothing rather than mapping picks through an invented scale.
        dims = sp.get("dims")
        pixel_size = (sp.get("entry") or {}).get("pixel_size_ang")
        geometry_ok = bool(dims) and bool(pixel_size and pixel_size > 0)
        for pl in project_state.get_pick_lists(species_id, ts_name):
            picks = _read_pick_list_voxels(Path(pl.path), dims, pixel_size)
            if not picks:
                # P4: a PERSISTED list that reads back as 0 picks must NOT be silently
                # dropped — that is exactly how a merged/manual list could vanish from
                # the rail (a coord/dims/apix regression making its star unreadable
                # looked identical to "no list"). Keep it in the rail (visible,
                # selectable, debuggable) and log the cause instead of skipping it.
                logger.warning(
                    "pick list %r (%s) for %s/%s read back 0 picks from %s — rendering empty",
                    pl.slug,
                    pl.list_type,
                    species_id,
                    ts_name,
                    pl.path,
                )
            # P2: the table count must match the cutout sheet, which derives kept/total
            # live from <slug>_filtered.star. pl.filtered_count is a cache that goes
            # stale (None) when the filter was committed in a prior session, so source
            # the count from the same star the sheet reads whenever it exists.
            filtered_count = pl.filtered_count
            if picks:
                keep = _memoized_keep_state(Path(pl.path))
                if keep is not None:
                    filtered_count = len(keep)
            else:
                filtered_count = None
            # Sync the persisted cache so PickList.extraction_state() (which reads
            # pl.filtered_count, not this local) compares the extracted count against the
            # SAME kept count the sheet + table show. Without this, a list filtered in a
            # prior session (filtered_count=None on disk) reads falsely STALE right after a
            # correct extraction of its kept subset. mark_dirty so the corrected count
            # persists for cross-session / aggregation reads that never re-render this panel.
            # Only when the geometry actually resolved: with unknown dims/apix EVERY list
            # reads back empty, and writing that through would wipe a real cached count on
            # a render that never looked at the picks.
            if geometry_ok and pl.filtered_count != filtered_count:
                pl.filtered_count = filtered_count
                project_state.mark_dirty()
            lists.append(
                {
                    "slug": pl.slug,
                    "label": pl.label or pl.slug,
                    "list_type": pl.list_type,
                    # Species color for every list (09-S2): PickList.color is legacy; the
                    # glyph (shape) tells the list types apart on the shared canvas.
                    "color": sp["color"],
                    "shape": glyph_for(pl.list_type),
                    "picks": picks,
                    "dims": dims,
                    "visible": pl.visible,
                    "path": pl.path,
                    "parent_slugs": pl.parent_slugs,
                    "filtered_count": filtered_count,
                    "source_kind": pl.source_kind,
                    "source_ref": pl.source_ref,
                }
            )
    return lists


def _denovo_species_entry(species, species_id: str, idx: int, color: str, geom: TomoGeometry, ts_name: str) -> dict:
    """Species-section entry for a species with NO candidate-extract job — picked de
    novo, so everything comes from the tomogram's geometry plus the user's own pick
    lists. `row` mirrors the shape `_collect_tomo_rows_for_instance` produces and
    `entry` carries the geometry under the keys the preview manifest would have used,
    so the whole render path below runs unchanged. No manifest, no auto picks, no job
    dir: `jm`/`job_dir` are None and the CE-only branches skip themselves."""
    label, (stage, beam) = position_label(ts_name)
    row = {
        "tomo_name": ts_name,
        "position_label": label,
        "stage": stage,
        "beam": beam,
        "vol_path": str(geom.recon_mrc) if geom.recon_mrc else None,
        "mod_path": None,  # IMOD overlays are a candidate-extract artifact
        "mod_exists": False,
        "n_picks": None,
        "score_range": None,
        "status": "ok" if geom.recon_mrc else "missing-volume",
        "error": None,
    }
    return {
        "idx": idx,
        # D-1: an internal key for tabs / canvas layers, never a roster job.
        "iid": f"pick__{species_id}",
        "jm": None,
        "job_dir": None,
        "species_id": species_id,
        "subtomo_job_dir": None,
        "subtomo_jm": None,
        "subtomo_iid": None,
        "auto_kept_count": None,
        "row": row,
        "manifest": {},
        "entry": {
            "pixel_size_ang": geom.binned_apix,
            "tomo_dims_xyz_px": list(geom.dims_xyz_px) if geom.dims_xyz_px else None,
        },
        "label": str(getattr(species, "name", "") or species_id or "species"),
        "color": color,
        "picks": [],
        "dims": list(geom.dims_xyz_px) if geom.dims_xyz_px else None,
        "tomograms_star": geom.tomograms_star,
    }


def _ce_species_entry(
    ce, species_id, idx: int, color: str, geom, project_state, project_path: Path, ts_name: str, refresh
) -> dict | None:
    """Species-section entry backed by a candidate-extract instance — the pre-inversion
    path, unchanged except for the color source and the dims fallback."""
    iid, jm = ce
    job_dir = job_dir_for(project_state, iid, jm, project_path)
    if not job_dir:
        return None
    ce_rows = _collect_tomo_rows_for_instance(job_dir, project_path)
    row = next((r for r in ce_rows if r["tomo_name"] == ts_name), None)
    if row is None:
        return None
    # Lazy-generate previews + IMOD overlays for this species (idempotent;
    # refresh re-renders the dashboard when the background job lands).
    _auto_kick_preview_generation(iid, jm, job_dir, project_path, refresh)
    _auto_kick_imod_generation(iid, jm, job_dir, project_path, refresh)
    manifest = read_preview_manifest(job_dir) or {}
    entry = (manifest.get("tomograms") or {}).get(ts_name) or {}
    picks_data = _read_picks_json(Path(entry["picks_json"])) if entry.get("picks_json") else {}
    label = (manifest.get("template") or {}).get("species_name") or split_species_id(iid) or iid
    # Resolve the subtomo job for THIS species (by species_id) so the
    # gallery's save-filter writes into the right job — the old lex-greatest
    # heuristic mis-targeted every species at one subtomo job.
    sub_match = matching_subtomo_instance(project_state, species_id)
    subtomo_job_dir = job_dir_for(project_state, sub_match[0], sub_match[1], project_path) if sub_match else None
    # Auto list's curated kept count (the subtomo-gallery keep/drop), read off the
    # cheap reviewed sidecar so the table shows kept/total for auto like the rest.
    auto_kept_count = None
    if subtomo_job_dir:
        try:
            from services.particles import picks_filter

            auto_kept_count = picks_filter.read_reviewed_counts(subtomo_job_dir).get(ts_name)
        except Exception:
            auto_kept_count = None
    # The candidate-extract job's own tomograms.star stays authoritative for this
    # species: on a denoised chain it repoints rlnTomoReconstructedTomogram at the
    # denoised volume, so sourcing the recon job's star instead would silently swap
    # which volume ArtiaX opens. The geometry star is the fallback when the job copy
    # is absent (and the only source on the de-novo path, which has no job dir).
    ce_star = job_dir / "tomograms.star"
    tomograms_star = ce_star if ce_star.exists() else (geom.tomograms_star if geom else ce_star)
    return {
        "idx": idx,
        "iid": iid,
        "jm": jm,
        "job_dir": job_dir,
        "species_id": species_id,
        "subtomo_job_dir": subtomo_job_dir,
        "subtomo_jm": sub_match[1] if sub_match else None,
        "subtomo_iid": sub_match[0] if sub_match else None,
        "auto_kept_count": auto_kept_count,
        "row": row,
        "manifest": manifest,
        "entry": entry,
        "label": str(label),
        "color": color,
        "picks": picks_data.get("picks") or [],
        # picks.json / manifest first (parity), then the geometry provider. The old
        # `[1, 1, 1]` tail is gone: dims we don't know disable the overlay instead of
        # collapsing every dot into the corner.
        "dims": picks_data.get("tomo_dims_xyz_px")
        or entry.get("tomo_dims_xyz_px")
        or (list(geom.dims_xyz_px) if geom and geom.dims_xyz_px else None),
        "tomograms_star": tomograms_star,
    }


def _collect_species_data_for_ts(project_state, project_path: Path, ts_name: str, refresh) -> list[dict]:
    """One entry per species the Particles section renders for this TS, with
    everything it needs (row, manifest, entry, picks, color, lists).
    Drives both the shared canvas overlay and the per-species tabs.

    Enumerates `species_render_plan` (the registry, not the candidate-extract jobs):
    a species with a CE instance keeps the pre-inversion entry exactly; a species
    without one gets a geometry-backed entry so it can be picked into de novo."""
    geom = geometry_for_ts(project_state, project_path, ts_name)
    out: list[dict] = []
    for idx, (species, species_id, ce) in enumerate(species_render_plan(project_state)):
        color = getattr(species, "color", "") or SPECIES_OVERLAY_COLORS[idx % len(SPECIES_OVERLAY_COLORS)]
        if ce is None:
            # Nothing has reconstructed or imported this tomogram, so there is no
            # frame to pick in — the species simply doesn't appear on this TS.
            if geom is None:
                continue
            sp_entry = _denovo_species_entry(species, species_id or "", idx, color, geom, ts_name)
        else:
            sp_entry = _ce_species_entry(
                ce, species_id, idx, color, geom, project_state, project_path, ts_name, refresh
            )
            if sp_entry is None:
                continue
        sp_entry["lists"] = _collect_pick_lists_for_species(sp_entry, project_state, ts_name)
        out.append(sp_entry)
    return out


def _eye_name(visible: bool) -> str:
    return "visibility" if visible else "visibility_off"


def _apply_pick_list_visibility(lst: dict) -> None:
    """Show/hide one pick list's canvas dot layers per its ``visible`` flag and
    keep that list's rail-chip eye glyph in sync. The dot layers (``_layer_els``)
    and the chip eye (``_eye_el``) are stashed on the list dict by the canvas /
    rail renderers, so this reaches both from either eye."""
    vis = lst.get("visible", True)
    for layer in lst.get("_layer_els") or []:
        if vis:
            layer.classes(remove="cb-pick-layer-hidden")
        else:
            layer.classes(add="cb-pick-layer-hidden")
    eye = lst.get("_eye_el")
    if eye is not None:
        eye.name = _eye_name(vis)


def _sync_species_master_eye(sp: dict) -> None:
    """Re-point a species' tab master-eye at whether ANY of its lists is visible,
    so toggling an individual chip eye keeps the master glyph honest."""
    eye = sp.get("_master_eye_el")
    if eye is None:
        return
    vis = any(lst.get("visible", True) for lst in (sp.get("lists") or []))
    sp["_master_visible"] = vis
    eye.name = _eye_name(vis)


def _render_species_master_eye(sp: dict, tab) -> None:
    """A master visibility eye inside a species tab: one click toggles ALL that
    species' canvas overlay layers. It lives on the tab strip (not the rail) so a
    species' overlay can be toggled even while another species' tab is open —
    what the retired cross-species 'Show picks' row gave. ``click.stop`` keeps the
    click from also switching tabs."""
    vis0 = any(lst.get("visible", True) for lst in (sp.get("lists") or []))
    sp["_master_visible"] = vis0
    with tab:
        eye = ui.icon(_eye_name(vis0), size="14px").classes("cb-eye cb-tab-eye")
        eye.tooltip("Show / hide all of this species' picks on the canvas")
    sp["_master_eye_el"] = eye

    def _toggle(_e, _sp=sp, _eye=eye):
        vis = not _sp.get("_master_visible", True)
        _sp["_master_visible"] = vis
        for lst in _sp.get("lists") or []:
            lst["visible"] = vis
            _apply_pick_list_visibility(lst)
        _eye.name = _eye_name(vis)

    eye.on("click.stop", _toggle)


# Slab height cap (vh) for the Particles canvas: the X/Y slab is capped at this
# fraction of viewport height, so the slab column's width = its vh cap · aspect.
# Lower → narrower slabs → more width for the gallery column beside them.
#
# The second number is a hard ceiling on the slab column's share of the row WIDTH.
# The vh cap alone sets the slab width to vh·aspect, which on a typical monitor lands
# at ~half the row — so the gallery column beside it only ever got the OTHER half,
# regardless of flex (this was the "gallery is half-width" bug: the two prior fixes
# tweaked vh + flex but never bounded the horizontal split). Capping the slab's width
# as a % of the row bounds that split directly. Lower → wider gallery.
#
# Mode-dependent since 11-S2: the Journey's slim mount shares its column with six other
# section cards and keeps the old 60vh/34%; the full page IS the viewer, so its slabs
# get the height and the width the maintainer asked for.
_SLAB_CAPS = {"slim": (60, 34), "full": (74, 46)}


def _slab_caps(mode: str) -> tuple[int, int]:
    """(max height in vh, max share of the row in %) for the slab column."""
    return _SLAB_CAPS.get(mode, _SLAB_CAPS["slim"])


def _imported_wip_marker(text: str, tip: str) -> None:
    """A subtle red 'unverified / missing' marker with an explanatory tooltip.
    Per CLAUDE.md ('Surfacing uncertainty'): show a missing/ambiguous param, never
    silently default it."""
    with ui.row().classes("items-center gap-1").style("display:inline-flex;"):
        ui.icon("error_outline", size="13px").classes("text-red-500")
        ui.label(text).classes("font-mono text-red-600").tooltip(tip)


def _render_imported_species_row(sp_obj, project_state, ts_name: str) -> None:
    """One registered species' manual pick lists on this imported tomogram.

    Counts come straight from the PickList registry (pl.count) — independent of the
    tomogram's apix/dims, which are surfaced separately with their own markers (so a
    suspect/missing geometry never silently mis-states a pick count). P2.1 lists picks;
    P2.2 adds the canvas overlay, which is what actually needs apix + dims."""
    color = (
        getattr(sp_obj, "color", "")
        or SPECIES_OVERLAY_COLORS[sum(map(ord, str(sp_obj.id))) % len(SPECIES_OVERLAY_COLORS)]
    )
    lists = project_state.get_pick_lists(sp_obj.id, ts_name)
    with ui.element("div").style("display:flex; gap:10px; align-items:center; padding:3px 2px; font-size:11px;"):
        ui.icon("circle", size="10px").style(f"color:{color};")
        ui.label(str(sp_obj.name)).style("font-weight:600;")
        if lists:
            total = sum(int(getattr(pl, "count", 0) or 0) for pl in lists)
            ui.label(f"{len(lists)} list(s) · {total} picks").classes("font-mono text-gray-600")
        else:
            ui.label("no picks yet").classes("italic text-gray-400")


def render_imported_particles_section(
    ts_name: str, project_state, project_path: Path, refresh, refresh_roster=None
) -> bool:
    """Particles section for imported tomograms (data-less / particle-only projects).

    A separate, deliberately-simple renderer: the candidate-extract canvas + gallery
    machinery assumes auto-pick data this source has none of, so we don't drive it
    blind. Shows the imported tomogram's geometry — with provenance markers, never a
    silent apix default (CLAUDE.md) — plus each registered species' manual pick lists.
    The 'register species & pick' launcher (P2.2) will mount here."""
    imported_star = project_state.imported_tomograms_star_path()
    if not imported_star:
        return False
    tomo_df = read_tomograms_table(Path(imported_star))
    if tomo_df is None or "rlnTomoName" not in tomo_df.columns:
        return False
    match = tomo_df[tomo_df["rlnTomoName"].astype(str) == ts_name]
    if match.empty:
        return False
    row = match.iloc[0]

    # Geometry from the star (cheap — no MRC re-read on the event loop). apix is
    # surfaced WITH provenance: if it looks like the synthesize fallback (the recon
    # MRC header had no voxel_size and no override was set), flag it red rather than
    # presenting 1.0 A/px as if it were measured.
    # binning follows RELION's convention (absent ⇒ 1.0); a corrupt non-positive /
    # NaN value also falls back to 1 rather than poisoning the apix + dims math.
    try:
        binning = float(row.get("rlnTomoTomogramBinning", 1.0))
    except (TypeError, ValueError):
        binning = 1.0
    if not (binning > 0):
        binning = 1.0
    try:
        ts_px = float(row.get("rlnTomoTiltSeriesPixelSize", 0.0))
    except (TypeError, ValueError):
        ts_px = 0.0
    binned_apix = ts_px * binning if ts_px > 0 else 0.0

    def _binned_dim(col: str):
        try:
            return round(float(row[col]) / binning) if col in row.index else None
        except (TypeError, ValueError):
            return None

    bx, by, bz = _binned_dim("rlnTomoSizeX"), _binned_dim("rlnTomoSizeY"), _binned_dim("rlnTomoSizeZ")
    dims_known = None not in (bx, by, bz)
    species = list(getattr(project_state, "species_registry", []) or [])

    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "particles"
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("scatter_plot", size="14px").classes("text-indigo-600")
            ui.label("Particles").classes("cb-section-title")
            ui.label("imported tomogram").classes("text-[10px] font-mono text-gray-500")
            ui.space()
            ui.label("manual picking · WIP").classes("text-[10px] font-mono text-amber-600").tooltip(
                "Particle-only project: pick manually in ArtiaX. Automated template matching on imported "
                "tomograms is not wired yet (it needs a per-tilt-series star)."
            )

        with ui.element("div").style(
            "display:flex; gap:16px; align-items:center; flex-wrap:wrap; padding:6px 2px; font-size:11px;"
        ):
            ui.label(ts_name).classes("font-mono").style("font-weight:600;")
            if dims_known:
                ui.label(f"{bx} x {by} x {bz} px").classes("font-mono text-gray-600")
            else:
                _imported_wip_marker(
                    "dimensions unavailable", "tomograms.star has no rlnTomoSizeX/Y/Z for this tomogram."
                )
            if abs(binning - 1.0) > 1e-6:
                ui.label(f"bin x{binning:g}").classes("font-mono text-gray-500")
            # `not (ts_px > 0)` catches 0 / negative / NaN — definitely missing.
            # A bare 1.0 A/px (binning 1) is AMBIGUOUS: it's the writer's fallback
            # when an MRC header lacks voxel_size, but could also be genuine — so we
            # surface it for confirmation rather than asserting either way.
            if not (ts_px > 0):
                _imported_wip_marker(
                    "pixel size unset",
                    "tomograms.star has no usable rlnTomoTiltSeriesPixelSize for this tomogram — set "
                    "'Pixel size' on the Import Tomograms job.",
                )
            elif abs(ts_px - 1.0) < 1e-6 and abs(binning - 1.0) < 1e-6:
                _imported_wip_marker(
                    f"{binned_apix:g} A/px · verify",
                    "Pixel size is exactly 1.0 A/px. If the recon MRC header lacked voxel_size this is the "
                    "import fallback and picks would be mis-scaled — set 'Pixel size' on the Import Tomograms "
                    "job. If 1.0 A/px is genuinely correct, ignore this.",
                )
            else:
                ui.label(f"{binned_apix:g} A/px").classes("font-mono text-gray-600")

        if not species:
            ui.label("No species registered yet — register one to start picking.").classes(
                "italic text-gray-500"
            ).style("padding:6px 2px; font-size:11px;")
        else:
            for sp_obj in species:
                _render_imported_species_row(sp_obj, project_state, ts_name)
    return True


def _render_geometry_chip(geom: TomoGeometry | None) -> None:
    """The tomogram's binned pixel size + dims in the Particles header, WITH the
    provenance of the pixel size. A missing apix (or missing dims) is shown red and
    disables every overlay downstream — the one thing we must never do is present a
    stand-in scale as if it were measured (CLAUDE.md, 'Surfacing uncertainty')."""
    if geom is None:
        return
    if geom.dims_xyz_px:
        ui.label("×".join(str(int(v)) for v in geom.dims_xyz_px)).classes("text-[10px] font-mono text-gray-500")
    else:
        _imported_wip_marker(
            "dimensions unknown",
            "Neither the reconstruction MRC header nor tomograms.star gives this tomogram's size, "
            "so picks cannot be placed on the canvas.",
        )
    if geom.binned_apix is None:
        _imported_wip_marker(
            "pixel size unset",
            "tomograms.star has no usable rlnTomoTiltSeriesPixelSize and the reconstruction MRC header "
            "carries no voxel size. Pick overlays are disabled rather than drawn at a guessed scale — "
            "set 'Pixel size' on the Import Tomograms job.",
        )
    elif geom.apix_provenance == APIX_MRC_HEADER:
        ui.label(f"{geom.binned_apix:g} Å/px").classes("text-[10px] font-mono text-amber-600").tooltip(
            "Pixel size read from the reconstruction MRC header — tomograms.star does not carry one."
        )
    else:
        ui.label(f"{geom.binned_apix:g} Å/px").classes("text-[10px] font-mono text-gray-500").tooltip(
            "Binned pixel size from tomograms.star (rlnTomoTiltSeriesPixelSize × rlnTomoTomogramBinning)."
        )


async def _prompt_new_species(project_path: Path, refresh) -> None:
    """Create a label-only species from the Particles empty state (D-5's second
    entry point; the roster's PARTICLES header carries the first). ``origin="manual"``
    — no template directory, because a de-novo species may never have a template.
    SingleFlight-guarded: this button sits in a poll-refreshed container and can be
    rebuilt mid-click."""
    from backend import get_backend
    from ui.species.prompt import create_species

    async with _curation_flight(f"new_species:{project_path}") as acquired:
        if not acquired:
            return
        species = await create_species(get_backend(), project_path, origin="manual")
        if species is None:
            return
        ui.notify(
            f"Created species '{species.name}' — pick into it with 'curate' on this tomogram, in the Particles "
            "registry's Picks & curation tab",
            type="positive",
            timeout=5000,
        )
        refresh()


def _render_no_species_empty_state(project_path: Path, refresh) -> None:
    """Particles section for a tomogram nobody has declared a species for yet. The
    de-novo entry point (D-5): picking needs a species, and this is where the user
    is standing when they realize that."""
    with ui.element("div").classes("cb-empty"):
        ui.icon("scatter_plot", size="28px").classes("text-gray-400")
        ui.label("No particle species yet.").classes("text-xs")
        ui.label(
            "A species is the label picks hang off. Create one to pick particles by hand in ArtiaX — "
            "no template or template-matching job needed."
        ).classes("text-[11px] italic text-center text-gray-500").style("max-width: 460px;")
        house_button("New species", lambda: _prompt_new_species(project_path, refresh), kind="accent")


def render_particles_section(
    ts_name: str,
    project_state,
    project_path: Path,
    refresh,
    refresh_roster=None,
    manage_species=None,
    *,
    mode: str = "slim",
    initial_species_id: str | None = None,
    open_viewer=None,
) -> bool:
    """The pick viewer, in either mount: a shared tomogram canvas with every species'
    picks overlaid (toggleable), the lists strip across the top, and the selected list's
    gallery beside the slabs.

    `mode="slim"` is the Journey's Particles section — look, filter, select, cross-link.
    `mode="full"` is the page reached from the Particles registry: bigger slabs, curation
    mode, the per-list extraction verb, fullscreen and the slab lightbox.

    Merge / dedup / import / delete live on the Species page
    (11-S3), so the header carries the route there (``manage_species``); a removed action
    the user cannot navigate to reads as a lost feature rather than a moved one.
    `open_viewer(species_id, tomo_name)` is the slim mount's route INTO the full page;
    `initial_species_id` preselects a species tab when the full page was opened on one."""
    geom = geometry_for_ts(project_state, project_path, ts_name)
    species_data = _collect_species_data_for_ts(project_state, project_path, ts_name, refresh)
    if not species_data:
        # A tomogram exists here but no species does — offer to create one rather
        # than leaving the section (and the whole picking path) undiscoverable.
        if geom is None:
            return False
        with ui.element("div").classes("cb-section-card w-full") as empty_card:
            empty_card._props["data-section"] = "particles"
            with ui.element("div").classes("cb-section-card-header"):
                ui.icon("scatter_plot", size="14px").classes("text-indigo-600")
                ui.label("Particles").classes("cb-section-title")
                _render_geometry_chip(geom)
            _render_no_species_empty_state(project_path, refresh)
        return True

    mrc_path = geom.recon_mrc if geom else None

    root_cls = "cb-section-card cb-particles-root w-full" + (" cb-viewer-full" if mode == "full" else "")
    with ui.element("div").classes(root_cls) as card:
        card._props["data-section"] = "particles"
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("scatter_plot", size="14px").classes("text-indigo-600")
            ui.label("Particles").classes("cb-section-title")
            ui.label(f"{len(species_data)} species").classes("text-[10px] font-mono text-gray-500")
            _render_geometry_chip(geom)
            # Route to the tab that owns this species' pick-list actions; follows the
            # active species tab, like the admin buttons.
            manage_host = ui.row().classes("items-center gap-0")
            viewer_host = ui.row().classes("items-center gap-0")
            ui.space()
            # Per-species generate controls (Render previews · Re-render · IMOD)
            # live in the panel toolbar, following the active tab; the canvas-wide
            # Invert switch is section-level (shared across species).
            admin_host = ui.row().classes("items-center gap-0 cb-section-admin")
            if mrc_path is not None:
                _render_invert_switch(card)

        def _show_manage_link_for(sp: dict) -> None:
            """'manage in Particles registry ↗' for the active species (11-S3). The Journey no longer
            merges / dedups / extracts / imports — this is
            the one-click route to where those now live (the Picks & curation tab, selected by
            `manage_species`), so their removal reads as a move. Absent when the workspace
            gave us no route (a standalone journey mount) or the species has no id."""
            manage_host.clear()
            sid = sp.get("species_id")
            if manage_species is None or not sid:
                return
            with manage_host:
                ui.label("manage in Particles registry ↗").classes(
                    "text-[10px] text-indigo-500 cursor-pointer underline decoration-dotted"
                ).on("click", lambda _e, s=sid: manage_species(s)).tooltip(
                    "Curate in ArtiaX · import a .coords · extract · dedup · delete — opens this species' "
                    "Picks & curation tab, where all of them live in one place."
                )

        def _show_viewer_link_for(sp: dict) -> None:
            """`full viewer ↗` — the slim mount's route into the full page (11-S6), on the
            active species + this tomogram. Absent in the full page (already there) and
            when the workspace gave us no route."""
            viewer_host.clear()
            sid = sp.get("species_id")
            if mode != "slim" or open_viewer is None or not sid:
                return
            with viewer_host:
                ui.label("full viewer ↗").classes(
                    "text-[10px] text-indigo-500 cursor-pointer underline decoration-dotted"
                ).style("margin-left: 8px;").on(
                    "click", lambda _e, s=sid, t=ts_name: open_viewer(s, t)
                ).tooltip(
                    "Open this species + tomogram in the full-page pick viewer — bigger slabs, curation mode, "
                    "the per-list extraction verb and fullscreen."
                )

        def _show_admin_for(sp: dict) -> None:
            admin_host.clear()
            with admin_host:
                _render_species_admin_buttons(sp, project_path, refresh)
            _show_manage_link_for(sp)
            _show_viewer_link_for(sp)

        # Layout (11-S2): species tabs, then the LISTS STRIP full width across the
        # top, then ONE row with the slabs LEFT and the gallery RIGHT. The strip used
        # to sit at the top of the right column, which is exactly what pushed the
        # gallery a rail's-height below the slabs — with it hoisted out, the two
        # columns start at the same y.
        tabs_host = ui.element("div").classes("w-full")
        lists_host = ui.element("div").classes("cb-lists-strip w-full")
        with ui.element("div").classes("cb-particles-split"):
            canvas_col = ui.element("div").classes("cb-particles-canvas-col")
            detail_host = ui.element("div").classes("cb-particles-detail-col")
        # Width = the slab's natural height-capped width, but never more than the
        # mode's % of the row — so the gallery column beside it always gets the
        # majority instead of the slab eating ~half the row by its vh·aspect width.
        # All species share the TS's reconstructed tomogram → first real dims win.
        slab_vh, slab_pct = _slab_caps(mode)
        cdims = next((sp["dims"] for sp in species_data if sp.get("dims")), None)
        cx, cy = (max(int(cdims[0]), 1), max(int(cdims[1]), 1)) if cdims else (1, 1)
        canvas_col.style(f"width: min(1400px, calc({slab_vh}vh * {cx} / {cy})); max-width: {slab_pct}%")
        with canvas_col:
            # Shared canvas: lazily renders the recon slab and overlays EVERY species'
            # picks. Rendered before the tab strip is built (though it sits below it in
            # the DOM) because the master-eye on each tab only appears once that
            # species' dot layers exist. Returns {iid: {"xy": lid, "xz": lid}} so the
            # active species' gallery cross-links to its own dots.
            canvas_layers = _render_particles_canvas(species_data, geom, ts_name, project_path, refresh, slab_vh)

        # Species-switch generation. The two hosts are now SHARED by every species (they
        # were per-`ui.tab_panel` before), and the detail pane renders one tick late off a
        # once-timer — so without this, switching species twice quickly lets the first
        # species' pending timer paint its gallery into the second's pane.
        gen = {"n": 0}

        def _show_species(sp: dict) -> None:
            """Swap the whole per-species half of the surface: toolbar, lists strip,
            detail. Replaces the old `ui.tab_panels`, which built every species' rail
            AND gallery up front and kept the inactive ones out of the DOM — the reason
            the hover bridge had to re-resolve its grid lazily."""
            gen["n"] += 1
            mine = gen["n"]
            _show_admin_for(sp)
            lists_host.clear()
            detail_host.clear()
            _render_species_tab_body(
                sp,
                canvas_layers.get(sp["iid"]),
                project_path,
                refresh,
                refresh_roster,
                manage_species,
                mode=mode,
                lists_host=lists_host,
                detail_host=detail_host,
                is_current=lambda: gen["n"] == mine,
            )

        sp_by_iid = {s["iid"]: s for s in species_data}
        # The full page opens ON the species whose registry row was clicked; the Journey
        # (and a full page opened on a species that isn't on this tomogram) starts on the
        # first tab rather than an empty one.
        active_sp = next((s for s in species_data if s.get("species_id") == initial_species_id), species_data[0])

        def _on_species_tab(e) -> None:
            sp = sp_by_iid.get(e.value)
            if sp is not None:
                _show_species(sp)

        with tabs_host:
            tabs = (
                ui.tabs(value=active_sp["iid"])
                .props("dense align=left indicator-color=indigo")
                .classes("cb-species-tabs")
                .on_value_change(_on_species_tab)
            )
            with tabs:
                for sp in species_data:
                    # Name + a master-eye toggling all of this species' canvas overlays
                    # at once (replacing the retired cross-species "Show picks" row). A
                    # CSS ::before dot (driven by the inline --sp-color) ties each tab
                    # to its canvas overlay color.
                    tab = ui.tab(sp["iid"], label=sp["label"]).classes("cb-species-tab")
                    tab.style(f"--sp-color: {sp['color']};")
                    if any(lst.get("_layer_els") for lst in sp.get("lists") or []):
                        _render_species_master_eye(sp, tab)

        # `ui.tabs(value=…)` does not fire on_value_change, so paint the active species.
        _show_species(active_sp)
    if mode == "full":
        ui.run_javascript(_SLAB_LIGHTBOX_JS)
    return True


def _render_particles_canvas(
    species_data: list[dict], geom: TomoGeometry | None, ts_name: str, project_path: Path, refresh, slab_vh: int
) -> dict:
    """Maximized shared slab canvas with each species' picks overlaid as a
    color-coded, toggleable layer. Returns {iid: {"xy": layer_id, "xz":
    layer_id|None}} so each tab's gallery can cross-link to its dots."""
    layer_ids: dict[str, dict] = {}

    if geom is None or geom.recon_mrc is None:
        ui.label("No reconstructed tomogram on disk — canvas unavailable; per-species galleries below.").classes(
            "cb-section-placeholder"
        )
        return layer_ids

    # The slabs cache next to the star that declared the volume — the recon job dir
    # for a pipeline tomogram, the Tomograms dir for an imported one.
    slab_dir, mrc_path = geom.tomograms_star.parent, geom.recon_mrc
    _auto_kick_recon_slabs(slab_dir, ts_name, mrc_path, project_path, refresh)
    xy_png, xz_png = _recon_slab_paths(slab_dir, ts_name)
    if not xy_png.exists():
        with ui.element("div").classes("cb-empty"):
            ui.spinner(size="26px", color="indigo-500")
            ui.label("Rendering tomogram slices…").classes("text-xs")
            ui.label("Auto-kicked in the background — the canvas fills when the slab PNG lands.").classes(
                "text-[11px] italic text-gray-500"
            )
        return layer_ids

    with_picks = [sp for sp in species_data if sp.get("lists")]
    if not with_picks:
        # Recon slab exists but nothing picked yet — clean slab, no overlay.
        with ui.element("div").classes("cb-recon-preview cb-recon-canvas").style(f"max-height: {slab_vh}vh;"):
            ui.image(vis_asset_url(str(xy_png)))
        return layer_ids

    # All species on this TS share the one volume, so any species that resolved dims
    # gives the canvas its aspect; the geometry provider is the backstop. Unknown
    # extent ⇒ bare slab, no overlay (never dots placed against a stand-in size).
    dims = next((sp["dims"] for sp in with_picks if sp.get("dims")), None) or (
        list(geom.dims_xyz_px) if geom.dims_xyz_px else None
    )
    if dims is None:
        with ui.element("div").classes("cb-recon-preview cb-recon-canvas").style(f"max-height: {slab_vh}vh;"):
            ui.image(vis_asset_url(str(xy_png)))
        return layer_ids
    x_dim = max(int(dims[0]), 1)
    y_dim = max(int(dims[1]), 1)
    z_dim = max(int(dims[2]), 1)
    nonce = uuid.uuid4().hex[:8]

    # Visibility is driven by the per-tab master-eye + per-chip eyes (Slice B
    # item 1) — the old per-list "Show picks" checkbox row was retired. Both eyes
    # toggle each list's `_layer_els` (populated just below) through
    # `_apply_pick_list_visibility`.

    # X/Y and X/Z share ONE stack that fills the (per-tomo width-capped) canvas
    # column — each child is width:100% of the stack and derives its height from
    # its own aspect-ratio. The COLUMN's max-width (set in render_particles_section
    # to min(1400px, slab_vh·x/y), R1) caps the X/Y at ~slab_vh tall while preserving the
    # tomogram aspect AND hugging the previews (no whitespace before the gallery);
    # the X/Z then reads as a proportional strip below it (height = width · z/x).
    stack = ui.element("div").classes("cb-canvas-stack")
    with stack:
        xy_host = ui.element("div").classes("cb-tomo-preview cb-recon-canvas")
        xy_host.style(f"aspect-ratio: {x_dim}/{y_dim};")
        with xy_host:
            ui.image(vis_asset_url(str(xy_png)))
            for sp in with_picks:
                for lst in sp["lists"]:
                    lid = f"cb-pl-{nonce}-{sp['idx']}-{lst['slug']}-xy"
                    lst.setdefault("_layer_els", []).append(
                        _render_pick_layer(lst["picks"], lst["color"], lst["dims"], "xy", lid, lst["shape"])
                    )
                    lst["_lid_xy"] = lid
                    # Gallery↔canvas bridge targets the species' primary list (auto
                    # if present, else the first) — what the cutout gallery mirrors.
                    if lst["slug"] == "auto" or "xy" not in layer_ids.get(sp["iid"], {}):
                        layer_ids.setdefault(sp["iid"], {})["xy"] = lid

        if xz_png.exists():
            xz_host = ui.element("div").classes("cb-tomo-preview cb-recon-canvas")
            xz_host.style(f"aspect-ratio: {x_dim}/{z_dim}; margin-top: 6px;")
            with xz_host:
                ui.image(vis_asset_url(str(xz_png)))
                for sp in with_picks:
                    for lst in sp["lists"]:
                        lid = f"cb-pl-{nonce}-{sp['idx']}-{lst['slug']}-xz"
                        lst.setdefault("_layer_els", []).append(
                            _render_pick_layer(lst["picks"], lst["color"], lst["dims"], "xz", lid, lst["shape"])
                        )
                        lst["_lid_xz"] = lid
                        if lst["slug"] == "auto" or "xz" not in layer_ids.get(sp["iid"], {}):
                            layer_ids.setdefault(sp["iid"], {})["xz"] = lid

    return layer_ids


def _tm_essentials_for_species(sp: dict) -> dict:
    """Resolve the Template-Match instance matched to this candidate-extract
    species (by species_id) and pull the interpretive essentials shown in the
    tab header: matched TM instance id, angular search θ, and symmetry."""
    state = current_project_state()
    # The collector already ran the resolve chain; re-running it here would have to
    # cope with a de-novo species' synthetic instance id and its `jm=None`.
    species_id = sp.get("species_id")
    species = state.get_species(species_id) if species_id else None
    info: dict = {
        "species": species,
        "species_id": species_id,
        "tm_iid": None,
        "tm_jm": None,
        "theta": None,
        "sym": None,
    }
    for tm_iid, tm_jm in template_match_instances(state):
        _, tm_sid = resolve_species(state, tm_jm, tm_iid)
        if tm_sid == species_id:
            info["tm_iid"], info["tm_jm"] = tm_iid, tm_jm
            info["theta"] = getattr(tm_jm, "angular_search", None)
            info["sym"] = (getattr(species, "symmetry", None) if species else None) or getattr(tm_jm, "symmetry", None)
            break
    return info


def _render_species_admin_buttons(sp: dict, project_path: Path, refresh) -> None:
    """The three per-species preview/overlay generate controls as icon buttons —
    Render previews (gen-missing) · Re-render all (cache-bypass) · (Re)generate
    IMOD overlays. Rendered into the Particles section title bar (next to the
    canvas-wide Invert switch), following the active species tab — so they sit in
    the panel toolbar, not crammed into the tab body.

    All three act on a candidate-extract job; a species picked de novo has none, so
    the toolbar is simply empty for it."""
    iid, jm, job_dir = sp["iid"], sp["jm"], sp.get("job_dir")
    if job_dir is None:
        return
    imod_dir = job_dir / "vis" / "imodPartRad"
    has_imod_models = imod_dir.exists() and any(imod_dir.glob("*.mod"))
    gen_missing_btn = (
        ui.button(
            icon="auto_fix_high",
            on_click=lambda: _handle_generate_for_instance(
                iid, jm, job_dir, project_path, False, gen_missing_btn, refresh
            ),
        )
        .props("flat dense round size=sm")
        .classes("text-purple-600")
        .tooltip("Render previews for tomograms whose manifest entry is missing or stale.")
    )
    regen_btn = (
        ui.button(
            icon="refresh",
            on_click=lambda: _handle_generate_for_instance(iid, jm, job_dir, project_path, True, regen_btn, refresh),
        )
        .props("flat dense round size=sm")
        .classes("text-gray-500")
        .tooltip("Re-render all: bypass cache and regenerate every tomogram's preview from scratch.")
    )
    imod_btn = (
        ui.button(
            icon="scatter_plot",
            on_click=lambda: _handle_generate_imod_for_instance(iid, jm, job_dir, project_path, imod_btn, refresh),
        )
        .props("flat dense round size=sm")
        .classes("text-blue-600")
        .tooltip("Regenerate IMOD .mod overlays" if has_imod_models else "Generate IMOD .mod overlays")
    )


def _render_species_tab_body(
    sp: dict,
    layer_ids: dict | None,
    project_path: Path,
    refresh,
    refresh_roster=None,
    manage_species=None,
    *,
    mode: str = "slim",
    lists_host,
    detail_host,
    is_current=lambda: True,
) -> None:
    """One species' half of the viewer, painted into two caller-owned hosts: the LISTS
    STRIP (`lists_host`, full width across the top) and the DETAIL pane (`detail_host`,
    the right column of the slabs/gallery row).

    The strip is a table — one row per pick list (auto + manual/imported/merged): swatch ·
    name · count · auth · extraction badge · path · eye. Clicking a row drives the detail
    below it: the auto list → the subtomo gallery/scatter cross-linked to this species'
    canvas dots via `layer_ids`; a workbench list → its recon cutout sheet.

    Both hosts belong to the section (not to a `ui.tab_panel`) since 11-S2, which is what
    lets the strip span the full width while the gallery's top edge lines up with the
    slabs'."""
    tm_info = _tm_essentials_for_species(sp)
    if sp["row"]["status"] == "missing-volume":
        with lists_host:
            ui.label("No reconstructed tomogram on disk for 3dmod — picks plot still works.").classes(
                "text-[11px] text-amber-700 italic"
            )

    # Always present an `auto` entry so the PyTOM section (incl. its zero-picks /
    # errored / generating states) stays reachable even when there are no auto
    # picks (the collector omits a 0-pick auto list to keep the canvas overlay clean).
    # Only for a species that HAS a candidate-extract job — a de-novo species has no
    # PyTOM section behind the chip, and an empty one would read as a failed run.
    lists = list(sp.get("lists") or [])
    if sp.get("job_dir") is not None and not any(lst["slug"] == "auto" for lst in lists):
        lists.insert(
            0,
            {
                "slug": "auto",
                "label": sp.get("label") or "auto",
                "list_type": PickListType.AUTO,
                "color": sp.get("color") or "#6366f1",
                "shape": glyph_for(PickListType.AUTO),
                "picks": sp.get("picks") or [],
                "dims": sp.get("dims"),
                "visible": True,
                "filtered_count": sp.get("auto_kept_count"),
            },
        )

    key = (sp.get("species_id") or "", sp["row"]["tomo_name"])
    slugs = {lst["slug"] for lst in lists}
    default_slug = "auto" if "auto" in slugs else (lists[0]["slug"] if lists else None)
    cur = _SELECTED_LIST_SLUG.get(key, default_slug)
    if cur not in slugs:
        cur = default_slug
    sel = {"slug": cur}
    chip_els: dict[str, object] = {}

    async def _render_detail() -> None:
        # The species may have been switched out from under this once-timer / click.
        if not is_current():
            return
        detail_host.clear()
        lst = next((x for x in lists if x["slug"] == sel["slug"]), None)
        with detail_host:
            await _render_list_detail(sp, lst, layer_ids, project_path, refresh, refresh_roster, mode=mode)

    async def _select(slug: str) -> None:
        if slug == sel["slug"] or slug not in slugs:
            return
        if sel["slug"] in chip_els:
            chip_els[sel["slug"]].classes(remove="selected")
        sel["slug"] = slug
        _SELECTED_LIST_SLUG[key] = slug
        if slug in chip_els:
            chip_els[slug].classes(add="selected")
        await _render_detail()

    with lists_host:
        _render_list_rail(
            sp,
            lists,
            project_path,
            tm_info=tm_info,
            selected_slug=sel["slug"],
            on_select=_select,
            chip_els=chip_els,
            manage_species=manage_species,
        )
    # The detail pane renders one tick later via a once-timer: _render_detail is
    # async now (its workbench-list branch probes disk off-loop), so it can't be
    # called inline from this sync builder — schedule it onto the event loop.
    ui.timer(0.05, _render_detail, once=True)


def _render_list_eye(lst: dict, sp: dict) -> None:
    """Per-list visibility eye on a rail chip: toggles just this list's canvas dot
    layers. ``click.stop`` so toggling visibility doesn't also select the chip;
    the species tab's master-eye is re-synced after each toggle."""
    vis0 = lst.get("visible", True)
    eye = ui.icon(_eye_name(vis0), size="14px").classes("cb-eye")
    eye.tooltip("Show / hide this list on the canvas")
    lst["_eye_el"] = eye

    def _toggle(_e, _lst=lst, _sp=sp):
        _lst["visible"] = not _lst.get("visible", True)
        _apply_pick_list_visibility(_lst)
        _sync_species_master_eye(_sp)

    eye.on("click.stop", _toggle)


def _attach_auto_chip_tooltip(el, sp: dict, tm_info: dict) -> None:
    """Rich hover tooltip for the pytom (auto) chip's type tag: the per-tomo
    auto-pick stats + the template-match run params that used to clutter the tab
    header inline (Slice B item 3). Two labeled sections so it's clear the stats
    describe the auto pick SET and the TM line the template-match RUN."""
    row = sp["row"]
    entry = sp.get("entry") or {}
    diameter = float(getattr(sp["jm"], "particle_diameter_ang", 0.0) or 0.0)
    score_field = (sp.get("manifest") or {}).get("score_field")
    tm_bits: list[str] = []
    if tm_info.get("tm_iid"):
        tm_bits.append(str(tm_info["tm_iid"]))
    if tm_info.get("theta"):
        tm_bits.append(f"θ {tm_info['theta']}°")
    if tm_info.get("sym"):
        tm_bits.append(f"sym {tm_info['sym']}")
    if diameter:
        tm_bits.append(f"Ø {diameter:.0f} Å")
    with el:
        with ui.tooltip().classes("cb-chip-tooltip"):
            ui.label("Auto pick set (PyTOM)").classes("cb-tt-head")
            ui.label(f"{row['position_label']} · {row['tomo_name']}").classes("cb-tt-line")
            stat = f"N = {row['n_picks']}"
            if row.get("score_range"):
                stat += f" · CC {row['score_range'][0]:.3f}–{row['score_range'][1]:.3f}"
            if entry.get("score_mean") is not None:
                stat += f" · mean {entry['score_mean']:.3f}"
            ui.label(stat).classes("cb-tt-line")
            if score_field:
                ui.label(f"score field: {score_field}").classes("cb-tt-sub")
            if tm_bits:
                ui.separator().classes("cb-tt-sep")
                ui.label("Template-match run").classes("cb-tt-head")
                ui.label("  ·  ".join(tm_bits)).classes("cb-tt-line")


def _list_count_text(total: int, filtered_count: int | None) -> str:
    """Table count cell text: 'kept/total' when a keep/drop filter is committed for
    this list, else just the total. `filtered_count` is None (no filter) or the kept
    count; equal-to-total is treated as no effective filter."""
    if filtered_count is not None and filtered_count != total:
        return f"{filtered_count}/{total}"
    return str(total)


def _render_list_rail(
    sp: dict,
    lists: list[dict],
    project_path: Path,
    *,
    tm_info: dict,
    selected_slug,
    on_select,
    chip_els: dict,
    manage_species=None,
) -> None:
    """The lists strip: a compact aligned TABLE (header + one row per
    list: swatch · name · count(kept/total) · extracted-mark · copy-path · visibility eye) on
    the left + the `curate ↗` route into the registry on the right.
    Every row shares one grid template so the columns line up under the header. The auto
    (pytom) row's name carries a hover tooltip with its pick stats + template-match
    essentials. Clicking a row selects it → drives the detail; the copy button and the
    eye use click.stop so they don't also select. `chip_els` is filled {slug:
    row-element} so selection can re-highlight without rebuilding the table.

    Look only (11-S3, tightened by 09-S2): curate / dedup / extract / import / delete live on
    the Particles registry's Picks & curation tab, where they act across every tomogram at
    once. A read-only `auth` radio used to sit between `picks` and `ext`, naming the one list
    downstream consumed; the authoritative model is gone, so the column went with it."""
    state_obj = current_project_state()
    species_id = sp.get("species_id") or ""
    tomo_name = sp["row"]["tomo_name"]

    with ui.element("div").classes("cb-list-top"):
        # The list TABLE: one aligned row per pick list — [swatch · name ·
        # count(kept/total) · extracted-mark · path · eye]. A row click selects it → drives
        # the detail gallery; the copy button and the eye use click.stop so they don't also
        # select. Every row shares the .cb-ltable-row grid template, so the columns line up
        # under the header.
        with ui.element("div").classes("cb-ltable"):
            with ui.element("div").classes("cb-ltable-row cb-ltable-head"):
                ui.element("div")  # swatch col
                ui.label("list").classes("cb-ltable-h-name")
                ui.label("picks").classes("cb-ltable-h-num")
                ui.label("ext").classes("cb-ltable-h-cell").tooltip("Subtomo-extracted state")
                ui.label("path").classes("cb-ltable-h-cell").tooltip("Copy the full path to this list's backing file")
                ui.element("div")  # eye col
            for lst in lists:
                slug = lst["slug"]
                row = ui.element("div").classes("cb-ltable-row")
                chip_els[slug] = row
                if slug == selected_slug:
                    row.classes(add="selected")
                row.on("click", lambda e, s=slug: on_select(s))
                with row:
                    ui.element("div").classes(f"cb-ltable-swatch cb-swatch-{lst['shape']}").style(
                        f"background: {lst['color']};"
                    )
                    name = ui.label(lst["label"]).classes("cb-ltable-name")
                    if slug == "auto":
                        name.style("cursor: help;")
                        _attach_auto_chip_tooltip(name, sp, tm_info)
                    total = len(lst.get("picks") or [])
                    cnt = ui.label(_list_count_text(total, lst.get("filtered_count"))).classes("cb-ltable-count")
                    cnt.tooltip("kept / total picks after keep-drop curation")
                    lst["_count_el"] = cnt  # so the cutout sheet can live-update it on keep/drop
                    with ui.element("div").classes("cb-ltable-cell"):
                        if slug != "auto":
                            pl = state_obj.get_pick_list(slug, species_id, tomo_name)
                            est = pl.extraction_state() if pl is not None else ListExtractionState.NOT_EXTRACTED
                            text, cls = extraction_badge(est)
                            if text:
                                # Symbol only in the table (○/✓/⚠); full label on hover.
                                ui.label(text.split(" ", 1)[0]).classes(f"cb-ltable-badge {cls}").tooltip(text)
                    with ui.element("div").classes("cb-ltable-cell"):
                        # P6: copy the full path to this list's backing file (auto →
                        # candidates.star; workbench → its star). Tooltip shows it; the
                        # click copies. click.stop so copying doesn't also select the row.
                        copy_path = (
                            str(Path(sp["job_dir"]) / "candidates.star") if slug == "auto" else (lst.get("path") or "")
                        )
                        if copy_path:
                            cbtn = (
                                ui.button(icon="content_copy")
                                .props("flat dense round size=sm")
                                .classes("cb-info-copy")
                                .tooltip(copy_path)
                            )
                            cbtn.on(
                                "click.stop",
                                lambda e, v=copy_path: (
                                    ui.clipboard.write(v),
                                    ui.notify("Copied path", type="positive", timeout=800),
                                ),
                            )
                    with ui.element("div").classes("cb-ltable-cell"):
                        if lst.get("_layer_els"):
                            _render_list_eye(lst, sp)
        # The toolbox NAVIGATES, it no longer launches (picking-UI 09-S2). The ⚡ that
        # started/swapped an ArtiaX session from here is gone: the app has exactly one
        # launch affordance, 'curate' on a tomogram group of the Particles registry's
        # "Picks & curation" tab, which is also where the save contract, the import path
        # and the watcher log live. This link is the route to it, beside the lists it acts
        # on; absent when the workspace gave us no route (a standalone journey mount).
        with ui.element("div").classes("cb-list-toolbox"):
            if manage_species is not None and species_id:
                ui.label("curate ↗").classes("cb-toolbox-link").on(
                    "click", lambda _e, s=species_id: manage_species(s)
                ).tooltip(
                    "Open this species in the Particles registry's Picks & curation tab — start or swap an "
                    "ArtiaX session on a tomogram, import a .coords, extract, dedup, delete"
                )


async def _render_list_detail(
    sp: dict,
    lst: dict | None,
    layer_ids: dict | None,
    project_path: Path,
    refresh,
    refresh_roster,
    *,
    mode: str = "slim",
) -> None:
    """Detail pane for the selected rail chip: the auto list shows the status-aware
    subtomo gallery / scatter; a workbench list shows its recon cutout sheet (whose
    disk probes run off-loop, hence async)."""
    if lst is None:
        with ui.element("div").classes("cb-empty"):
            ui.icon("inbox", size="28px").classes("text-gray-400")
            ui.label("No pick lists yet for this species.").classes("text-xs")
        return
    if lst["slug"] == "auto":
        _render_species_auto_section(sp, layer_ids, project_path, refresh, refresh_roster, mode=mode)
    else:
        await _render_single_list_cutouts(sp, lst, project_path, refresh, mode=mode)


def _render_species_auto_section(
    sp: dict, layer_ids: dict | None, project_path: Path, refresh, refresh_roster=None, *, mode: str = "slim"
) -> None:
    """The status-aware AUTO (PyTOM) gallery for a species tab: the subtomo
    cutout gallery when extracted, else the scatter fallback, or an
    error/empty/spinner state. Factored out of `_render_species_tab_body` so the
    workbench lists' cutouts render after it without being skipped by its early
    returns."""
    row, manifest, entry = sp["row"], sp["manifest"], sp["entry"]
    status = row["status"]
    if status == "errored":
        with ui.element("div").classes("cb-empty"):
            ui.icon("error_outline", size="28px").classes("text-red-500")
            ui.label("Render error: " + (row.get("error") or "unknown")).classes("text-xs text-red-600")
        return
    if status == "zero-picks":
        _render_zero_picks_empty_state(manifest, sp["label"])
        return
    if status not in ("ok", "missing-volume"):
        with ui.element("div").classes("cb-empty"):
            ui.spinner(size="28px", color="indigo-500")
            ui.label("Generating preview for this tilt-series…").classes("text-xs")
            ui.label("Auto-kicked in the background — the page refreshes when the manifest lands.").classes(
                "text-[11px] italic text-gray-500"
            )
        return

    has_atlas = bool(entry.get("cutout_atlas") and entry.get("cutout_index"))
    if has_atlas:
        atlas_meta = _read_atlas_index(Path(entry["cutout_index"]))
        if atlas_meta is not None:
            xy_id = (layer_ids or {}).get("xy") or ""
            xz_id = (layer_ids or {}).get("xz") or ""
            gallery_id = f"cb-gallery-{uuid.uuid4().hex[:8]}"
            _render_gallery_body(
                row,
                entry,
                manifest,
                atlas_meta,
                xy_id,
                xz_id,
                gallery_id,
                bool(xy_id),
                bool(xz_id),
                sp["job_dir"],
                sp.get("subtomo_job_dir"),
                refresh_roster,
                mode=mode,
            )
            return
    # No subtomo cutouts in the manifest — scatter fallback (its own mini X/Y +
    # X/Z plots). Whether an extraction job exists decides which of two very
    # different situations this is, so the fallback says which.
    _render_picks_scatter_section(row, entry, manifest, sp.get("subtomo_job_dir") is not None)


def _render_zero_picks_empty_state(manifest: dict, species_name: str | None) -> None:
    """Empty state for tomograms PyTOM processed but where no candidate
    exceeded the cutoff. Shows the species template + score field so users
    can tell at a glance "this species got nothing here" — distinct from the
    generic "still generating" spinner the no-preview branch falls through to.
    """
    template_block = manifest.get("template") or {}
    thumb_path = template_block.get("thumb_path")
    species_label = species_name or template_block.get("species_name") or "this species"
    score_field = manifest.get("score_field") or "rlnLCCmax"
    summary = manifest.get("summary") or {}
    zero_n = len(summary.get("zero_picks") or [])
    ok_n = len(summary.get("ok") or [])
    total = zero_n + ok_n
    with ui.element("div").classes("cb-empty"):
        with ui.row().classes("items-center gap-4 justify-center w-full flex-wrap"):
            if thumb_path:
                ui.image(vis_asset_url(thumb_path)).style("width: 96px; height: 96px;").classes(
                    "rounded border border-gray-200 bg-gray-50 object-contain"
                )
            with ui.column().classes("gap-1 items-start"):
                ui.label(f"0 picks above cutoff for {species_label}").classes("text-sm font-semibold text-amber-700")
                ui.label(f"PyTOM processed this tilt-series but no {score_field} value cleared the cutoff.").classes(
                    "text-[11px] text-gray-700"
                )
                if total:
                    ui.label(f"{zero_n} of {total} TS in this job came back empty.").classes(
                        "text-[11px] italic text-gray-500"
                    )
                ui.label(
                    "Check the picking cutoff, template chirality (TomoHand), "
                    "or template appropriateness for this species."
                ).classes("text-[10px] italic text-gray-500").style("max-width: 480px;")


# ── The gallery toolbelt (11-S3) ──────────────────────────────────────────────
# One icon row on the gallery header replaces the strip of controls that had
# accumulated under the grid: a sort select, a display-filter pair, a per-tomogram
# 3dmod block, a per-pick 3dmod block, a hovered-pick stats row and a filtered-set
# path row. Each icon opens a popover holding the controls it used to show inline;
# the two 3dmod blocks and the outputs row are now ONE popover, so 3dmod exists in
# exactly one place per gallery instead of two plus two hint strings.


# Client-side only: fullscreen the viewer ROOT (the nearest .cb-particles-root
# ancestor of the button), not the document — so the workspace chrome drops away and
# the slabs + gallery get the whole window. Falls back to the page when the root is
# missing, and toggles back out on a second click (Esc also exits, per the browser).
_FULLSCREEN_JS = """(e) => {
    const t = (e.target.closest && e.target.closest('.cb-particles-root')) || document.documentElement;
    if (document.fullscreenElement) { document.exitFullscreen(); }
    else if (t.requestFullscreen) { t.requestFullscreen(); }
}"""


def _toolbelt_button(icon: str, tooltip: str) -> ui.button:
    """One toolbelt icon. Popover-opening icons wrap this in `with btn, ui.menu()`;
    stateful toggles flip `.cb-tb-on` on the returned button."""
    return ui.button(icon=icon).props("flat dense round size=sm").classes("cb-tb-btn").tooltip(tooltip)


def _cmd_row(label: str, value: str, *, status: tuple[str, str, str] | None = None) -> dict:
    """A readonly command line + status dot + copy button, as the 3dmod popover shows
    them. Returns the input/icon so a lazily-computed command (the per-pick peek) can
    fill them in later."""
    with ui.row().classes("w-full items-center gap-1 no-wrap"):
        ui.label(label).classes("text-[9px] uppercase font-bold text-gray-400").style("width: 62px;")
        cmd = (
            ui.input(value=value)
            .props("dense outlined readonly hide-bottom-space")
            .classes("text-xs font-mono flex-1")
            .style("min-width: 0;")
        )
        icon_name, icon_color, icon_tip = status or ("info", "text-gray-400", "")
        icon = ui.icon(icon_name, size="14px").classes(icon_color)
        if icon_tip:
            icon.tooltip(icon_tip)
        (
            ui.button(
                icon="content_copy",
                on_click=lambda: (ui.clipboard.write(cmd.value), ui.notify("Copied", type="positive", timeout=800)),
            )
            .props("flat dense round size=sm")
            .classes("text-gray-500 hover:text-gray-800")
            .tooltip("Copy to clipboard")
        )
    return {"cmd_input": cmd, "status_icon": icon}


def _tomo_3dmod_command(row: dict) -> tuple[str, tuple[str, str, str]] | None:
    """`3dmod <vol> [<mod>]` for this tomogram plus its status triple, or None when no
    volume is on disk. The .mod overlay may still be generating — the command is
    copyable already, and by the time the user pastes it the file is usually there."""
    if not row.get("vol_path"):
        return None
    mod = row.get("mod_path")
    if not mod:
        return f"3dmod {row['vol_path']}", ("info", "text-gray-400", "Open volume only — no IMOD overlay for it.")
    if row.get("mod_exists"):
        return f"3dmod {row['vol_path']} {mod}", ("check_circle", "text-emerald-600", "IMOD overlay ready.")
    return f"3dmod {row['vol_path']} {mod}", (
        "hourglass_top",
        "text-amber-600",
        "IMOD overlay being generated in the background. The command is copyable already — by the time you "
        "paste, the .mod file should be in place. Picks appear as circles over the tomogram.",
    )


def _render_3dmod_popover(row: dict, *, with_peek: bool) -> dict:
    """The ONE 3dmod affordance for a gallery: the per-tomogram command, the per-pick
    subvolume command (auto lists only — a workbench list has no peek extraction), and
    an `outputs` host the gallery fills with the filtered-set path once one exists.

    Returns the peek refs (`cmd_input` / `status_icon`) plus `outputs_host`; empty of
    peek keys when `with_peek` is False."""
    refs: dict = {}
    btn = _toolbelt_button("view_in_ar", "3dmod commands + output paths for this tomogram")
    tomo_cmd = _tomo_3dmod_command(row)
    with btn, ui.menu().props("anchor='bottom right' self='top right'"):
        with ui.element("div").classes("cb-info-card cb-3dmod-card"):
            if tomo_cmd is None:
                ui.label("No reconstructed tomogram on disk for this tilt-series.").classes("cb-info-meta")
            else:
                cmd, status = tomo_cmd
                _cmd_row("tomogram", cmd, status=status)
            if with_peek:
                refs.update(
                    _cmd_row(
                        "pick",
                        "(shift-click a tile to extract its subvolume)",
                        status=(
                            "touch_app",
                            "text-gray-400",
                            "Shift-click a cutout to cut its 3D subvolume out of the tomogram, then copy this "
                            "command and run it in a terminal with X11. In 3dmod: Image → Slicer gives the "
                            "X/Y, X/Z, Y/Z triptych centered on the pick.",
                        ),
                    )
                )
            refs["outputs_host"] = ui.element("div").classes("w-full")
    return refs


def _render_anchor_tiles(
    indices: list[int], picks: list, cutout_index: dict, atlas_url: str, cols: int, rows: int, tag: str, note: str
) -> None:
    """One row of calibration tiles cut from the same atlas as the grid. `tag` is the
    per-tile rank prefix (H1…/L1…) and `note` the tooltip's role word."""
    tile_px = _DISPLAY_TILE_PX
    bg_w, bg_h = cols * tile_px, rows * tile_px
    with ui.element("div").classes("cb-ref-tiles"):
        for n, pick_idx in enumerate(indices):
            pos = cutout_index.get(str(pick_idx))
            if not pos:
                continue
            r, c = pos
            pick = picks[pick_idx] if 0 <= pick_idx < len(picks) else None
            tile = ui.element("div").classes("cb-noise-tile")
            tile.style(
                f"background-image: url({atlas_url}); background-size: {bg_w}px {bg_h}px; "
                f"background-position: {-c * tile_px}px {-r * tile_px}px;"
            )
            tip = [f"#{pick_idx} ({note})"]
            if pick and pick.get("score") is not None:
                tip.append(f"score={pick['score']:.4f}")
            tile._props["title"] = " · ".join(tip)
            with tile:
                ui.html(f"{tag}{n + 1}", sanitize=False).classes("cb-tile-rank")
                if pick and pick.get("score") is not None:
                    ui.html(f"{pick['score']:.3f}", sanitize=False).classes("cb-tile-score")


def _render_reference_strip(
    picks: list, cutout_index: dict, atlas_url: str, cols: int, rows: int, manifest: dict, ref_state: dict
) -> None:
    """Calibration anchors for the gallery: template tile · best-4 · worst-4.

    Three pedagogical anchors: what the picker thinks "particle" looks like (the
    template's central X/Y slice through the same percentile pipeline), the highest-
    scoring picks it found here, and the worst-scoring ones that still cleared the
    cutoff — the practical noise floor. The user evaluates the ambiguous middle band
    against all three ([[feedback_gallery_calibration_anchors]]).

    Collapsible and CLOSED by default since 11-S2: three tile rows permanently above
    the grid cost more vertical space than they earn once the user has calibrated. The
    open/closed flag lives in `ref_state` (owned by the gallery) so a display-filter
    switch — which re-renders this strip so the anchors match the grid's filter —
    doesn't slam it shut again.
    """
    template_block = (manifest or {}).get("template") if isinstance(manifest, dict) else None
    template_thumb_path = template_block.get("thumb_path") if template_block else None
    template_url = vis_asset_url(template_thumb_path) if template_thumb_path else None

    # The pick index IS the score rank (the picker writes candidates best-first), so
    # the two extremes are the two ends of the sorted index list.
    ranked = sorted(int(k) for k in cutout_index.keys())
    n_anchor = min(4, len(ranked))
    best_indices = ranked[:n_anchor]
    worst_indices = list(reversed(ranked[-n_anchor:]))

    if not template_url and not ranked:
        return

    caret = ui.label("references ▸").classes("cb-ref-toggle")
    caret.tooltip("Template · highest-scoring · lowest-scoring picks — the calibration anchors for the grid below")
    body = ui.element("div").classes("cb-reference-strip")

    def _sync() -> None:
        on = bool(ref_state.get("refs_open"))
        caret.set_text("references ▾" if on else "references ▸")
        body.set_visibility(on)

    def _toggle(_e=None) -> None:
        ref_state["refs_open"] = not ref_state.get("refs_open")
        _sync()

    caret.on("click", _toggle)

    with body:
        if template_url:
            with ui.element("div").classes("cb-ref-group"):
                ui.label("Template").classes("cb-ref-label")
                with ui.element("div").classes("cb-ref-tiles"):
                    tile = ui.element("div").classes("cb-template-tile")
                    tile.style(f"background-image: url({template_url});")
                    apix = template_block.get("pixel_size_ang") if template_block else None
                    name = template_block.get("species_name") if template_block else None
                    tip_parts = [str(name or "template")]
                    if apix:
                        tip_parts.append(f"{apix:.2f} Å/px")
                    box = template_block.get("box_px") if template_block else None
                    if box:
                        tip_parts.append(f"box {box}")
                    tile._props["title"] = " · ".join(tip_parts)
                    with tile:
                        ui.html("REF", sanitize=False).classes("cb-tile-rank")
        if template_url and ranked:
            ui.element("div").classes("cb-ref-divider")
        if best_indices:
            with ui.element("div").classes("cb-ref-group"):
                ui.label(f"Highest scores ({len(best_indices)})").classes("cb-ref-label")
                _render_anchor_tiles(
                    best_indices, picks, cutout_index, atlas_url, cols, rows, "H", "high score reference"
                )
        if best_indices and worst_indices:
            ui.element("div").classes("cb-ref-divider")
        if worst_indices:
            with ui.element("div").classes("cb-ref-group"):
                ui.label(f"Lowest scores · noise floor ({len(worst_indices)})").classes("cb-ref-label")
                _render_anchor_tiles(
                    worst_indices, picks, cutout_index, atlas_url, cols, rows, "L", "low score reference"
                )
    _sync()


async def _trigger_peek_for_pick(
    pick_idx: int,
    picks: list,
    tomo_mrc: str | None,
    peek_dir: Path | None,
    pixel_size_ang: float | None,
    particle_diameter_ang: float | None,
    peek_refs: dict,
    state: dict,
) -> None:
    """Extract the subvolume around the clicked pick, populate the 3dmod cmd.

    Stale-result guard: if the user clicks several tiles in quick succession,
    only the latest selection's extraction populates the UI — we track
    `state["selected_idx"]` and skip the update if it changed by the time
    extraction finishes."""
    cmd = peek_refs.get("cmd_input")
    status = peek_refs.get("status_icon")
    if cmd is None or status is None:
        return
    if not tomo_mrc or peek_dir is None or not (0 <= pick_idx < len(picks)):
        cmd.set_value("(no tomogram volume on disk)")
        status.props("name=error_outline").classes(replace="text-red-500")
        return
    pick = picks[pick_idx]
    try:
        x = int(pick.get("x"))
        y = int(pick.get("y"))
        z = int(pick.get("z"))
    except (TypeError, ValueError):
        cmd.set_value("(pick coords missing)")
        return

    # Box ≈ 3× particle diameter in tomogram px; clamp so user can navigate
    # context without dragging gigabytes around. Fallback to 48 px half (≈96
    # cube) when we can't compute — works for most particles at bin 4 / bin 8.
    if pixel_size_ang and particle_diameter_ang and pixel_size_ang > 0:
        diameter_px = particle_diameter_ang / pixel_size_ang
        half_box = round(diameter_px * 1.5)
    else:
        half_box = 48
    half_box = max(24, min(128, half_box))

    out_path = peek_dir / f"pick_{pick_idx:05d}.mrc"
    cmd.set_value(f"extracting subvolume for pick #{pick_idx}…")
    status.props("name=hourglass_top").classes(replace="text-amber-600")

    import asyncio as _asyncio

    from services.visualization.preview_render import extract_pick_subvolume

    if not out_path.exists():
        result = await _asyncio.to_thread(extract_pick_subvolume, Path(tomo_mrc), x, y, z, half_box, out_path)
    else:
        result = out_path  # cache hit

    if state.get("selected_idx") != pick_idx:
        return  # user clicked elsewhere — discard stale result

    if result is None:
        cmd.set_value(f"(extraction failed for pick #{pick_idx})")
        status.props("name=error_outline").classes(replace="text-red-500")
        status.tooltip("Subvolume extraction failed; check server logs.")
        return
    cmd.set_value(f"3dmod {out_path}")
    status.props("name=check_circle").classes(replace="text-emerald-600")
    status.tooltip(
        f"Subvolume ready: half_box={half_box} px around (x={x}, y={y}, z={z}). "
        "Copy the command and run in a terminal with X11 forwarding."
    )


def _render_gallery_body(
    row: dict,
    entry: dict,
    manifest: dict,
    atlas_meta: dict,
    xy_host_id: str,
    xz_host_id: str,
    gallery_id: str,
    has_xy: bool,
    has_xz: bool,
    ce_job_dir: Path | None,
    subtomo_job_dir: Path | None,
    refresh_roster=None,
    *,
    mode: str = "slim",
) -> None:
    picks_json_path = entry.get("picks_json")
    picks_data = (
        _read_picks_json(Path(picks_json_path)) if picks_json_path else {"picks": [], "tomo_dims_xyz_px": [0, 0, 0]}
    )
    picks = picks_data.get("picks", [])
    tomo_dims = picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1]
    pixel_size_ang = entry.get("pixel_size_ang")

    atlas_url = vis_asset_url(entry["cutout_atlas"])
    cols = int(atlas_meta.get("cols", 8))
    rows = int(atlas_meta.get("rows", 1))
    cutout_index = atlas_meta.get("index", {})
    failures = entry.get("cutout_failures") or atlas_meta.get("failures") or []

    # Display-filter variants (raw + lowpass/bandpass). Each variant is a sibling
    # atlas PNG that shares this exact tile index, so switching filters just swaps
    # the atlas URL the tiles + noise-reference tiles point at — no re-layout. apix
    # is read from the cutout file's OWN header (subtomo and recon use different
    # bins), so the Å→px conversion in the unit toggle/tooltip is always correct.
    from services.visualization.cutout_filters import get_filter_presets

    cutout_variants = entry.get("cutout_variants") or {}
    cutout_apix = entry.get("cutout_apix")
    cutout_apix_source = entry.get("cutout_apix_source") or "unknown"
    _filter_presets = [p for p in get_filter_presets() if p["key"] in cutout_variants]

    def _variant_atlas_url(key: str) -> str:
        v = cutout_variants.get(key)
        if v and v.get("atlas"):
            return vis_asset_url(v["atlas"])
        return vis_asset_url(entry["cutout_atlas"])

    def _on_filter_select(key: str) -> None:
        # Swap the atlas the tiles + noise-reference tiles point at, then re-render
        # (positions are identical across filters — only the pixels differ).
        nonlocal atlas_url
        atlas_url = _variant_atlas_url(key)
        _refresh_reference_strip()
        _refresh_grid()

    x_dim = max(int(tomo_dims[0]), 1)
    y_dim = max(int(tomo_dims[1]), 1)
    z_dim = max(int(tomo_dims[2]), 1)
    pick_xy_frac: dict[int, list[float]] = {}
    pick_xz_frac: dict[int, list[float]] = {}
    for k in cutout_index:
        try:
            i = int(k)
        except (TypeError, ValueError):
            continue
        if 0 <= i < len(picks):
            p = picks[i]
            fx = max(0.0, min(1.0, float(p.get("x", 0)) / x_dim))
            fy_top = max(0.0, min(1.0, 1.0 - float(p.get("y", 0)) / y_dim))
            fz_top = max(0.0, min(1.0, 1.0 - float(p.get("z", 0)) / z_dim))
            pick_xy_frac[i] = [fx, fy_top]
            pick_xz_frac[i] = [fx, fz_top]

    # ce_job_dir + subtomo_job_dir are passed in from the species data, resolved
    # per-species. They used to be guessed here — ce via path arithmetic that
    # landed on <ce>/vis (the "Could not read candidates … from …/vis" bug), and
    # subtomo via a lex-greatest heuristic that mis-targeted every species at one
    # job. Both are now correct for any number of registered species.
    ts_name = row.get("tomo_name") or ""
    from services.particles import picks_filter

    # Degenerate picks: candidates that produced no cutout (no subtomo match /
    # render failed). Surfaced as greyed tiles at the end of the grid so the
    # gallery's tile count vs pick index is transparent to the user.
    degenerate_indices: list[int] = []
    for f in failures:
        try:
            degenerate_indices.append(int(f.get("i")))
        except (TypeError, ValueError):
            continue
    n_picks_total = len(picks)
    all_pick_indices: set[int] = set()
    for k in cutout_index.keys():
        try:
            all_pick_indices.add(int(k))
        except (TypeError, ValueError):
            continue

    # Load any existing curation. derive_keep_state_for_ts returns None when
    # there's no filtered file at all (= "all kept implicitly"); we materialize
    # that to a None sentinel in state so the user sees an unmarked starting
    # point, and only mutate to a concrete set once they actually click a tile.
    initial_keep_set: set[int] | None = None
    if subtomo_job_dir is not None and ce_job_dir is not None and ts_name:
        try:
            initial_keep_set = picks_filter.derive_keep_state_for_ts(
                subtomo_job_dir, ce_job_dir, ts_name, n_picks_total
            )
        except Exception as e:
            logger.warning("Could not load existing filter for %s: %s", ts_name, e)

    state = {
        "selected_idx": None,  # shift-click peek target (stale-result guard)
        # 11-S4: the SELECTED pick. Clicking a tile or a slab dot selects it — a
        # persistent highlight on both — and that is ALL a click does unless curation
        # mode is armed. Esc / a click on empty gallery space clears it.
        "selected": None,
        # 11-S4: curation mode. OFF by default and unavailable in the slim mount: the
        # Journey is look-and-filter, the full page is where picks are curated. While
        # ON, a click means keep/drop, the lasso arms, and Save/Reset appear.
        "curating": False,
        "sort_mode": "best",
        # Filter state. keep_set == None means "no curation yet — every pick
        # implicitly kept". Once the user toggles anything we materialize it
        # to a concrete set so subsequent toggles can flip membership cleanly.
        "keep_set": initial_keep_set,
        "saved_baseline": (set(initial_keep_set) if initial_keep_set is not None else None),
        # Tracks the saved state of the "exclude degenerate" checkbox so toggling
        # it alone counts as a dirty change worth saving.
        "saved_exclude_degen": True,
        # Reference strip open/closed — survives the re-render a filter switch forces.
        "refs_open": False,
    }
    can_filter = bool(subtomo_job_dir is not None and ce_job_dir is not None and ts_name)
    can_curate = can_filter and mode == "full"

    # Layout (11-S2/S3): header row with the toolbelt, a curation bar that exists only
    # while curation mode is armed, the collapsible reference strip, then the grid.
    counter_label = None
    save_btn = None
    discard_btn = None
    exclude_degen_cb = None  # set in the curation bar when there are degenerate picks

    def _exclude_degen_on() -> bool:
        return bool(exclude_degen_cb.value) if exclude_degen_cb is not None else True

    def _effective_keep_set() -> set[int]:
        # The set actually written on save: kept OK picks, plus degenerate picks
        # only when the user unchecked "exclude". Degenerate "no subtomo match"
        # picks have no subtomo row so save_filtered_picks_for_ts drops them
        # regardless; the checkbox only changes the fate of render-failed picks
        # that DO have a subtomo.
        ks = state["keep_set"]
        base = set(all_pick_indices) if ks is None else set(ks)
        if not _exclude_degen_on():
            base |= set(degenerate_indices)
        return base

    def _is_dirty() -> bool:
        if state["keep_set"] != state["saved_baseline"]:
            return True
        return _exclude_degen_on() != state["saved_exclude_degen"]

    def _refresh_counter():
        if counter_label is None:
            return
        ks = state["keep_set"]
        kept = len(ks) if ks is not None else len(all_pick_indices)
        total = len(all_pick_indices)
        txt = f"kept {kept}/{total} (TS)"
        if degenerate_indices:
            txt += f" · {len(degenerate_indices)} not extracted"
        counter_label.set_text(txt)
        if _is_dirty():
            counter_label.classes(add="cb-filter-dirty")
        else:
            counter_label.classes(remove="cb-filter-dirty")
        if save_btn is not None:
            if _is_dirty():
                save_btn.props(remove="disable")
            else:
                save_btn.props("disable")
        if discard_btn is not None:
            has_saved = (
                subtomo_job_dir is not None and (subtomo_job_dir / picks_filter.PARTICLES_FILTERED_NAME).exists()
            )
            if has_saved:
                discard_btn.props(remove="disable")
            else:
                discard_btn.props("disable")

    def _on_save():
        try:
            if not _is_dirty():
                ui.notify("No changes to save", type="info", timeout=1500)
                return
            effective = _effective_keep_set()
            result = picks_filter.save_filtered_picks_for_ts(subtomo_job_dir, ce_job_dir, ts_name, effective)
            ks = state["keep_set"]
            state["saved_baseline"] = set(ks) if ks is not None else None
            state["saved_exclude_degen"] = _exclude_degen_on()
            ui.notify(f"Saved {result['kept_for_ts']} picks for {ts_name}", type="positive", timeout=2000)
            _refresh_counter()
            _refresh_outputs()
            if refresh_roster:
                refresh_roster()  # roster review column updates now, not on next switch
        except Exception as e:
            logger.exception("Save filter failed for %s", ts_name)
            ui.notify(f"Save failed: {e}", type="negative", timeout=4000)

    def _on_discard():
        try:
            outcome = picks_filter.discard_ts_filter(subtomo_job_dir, ts_name)
            if outcome == "noop":
                ui.notify("No filter to reset for this tomogram", type="info", timeout=1500)
                return
            state["keep_set"] = None
            state["saved_baseline"] = None
            state["saved_exclude_degen"] = True
            if outcome == "removed_all":
                ui.notify(
                    f"Reset {ts_name} — no curation left for this species, downstream uses original picks",
                    type="info",
                    timeout=2800,
                )
            else:
                ui.notify(
                    f"Reset {ts_name} to original picks — other tomograms keep their curation",
                    type="info",
                    timeout=2800,
                )
            _refresh_grid()
            _refresh_counter()
            _refresh_outputs()
            if refresh_roster:
                refresh_roster()
        except Exception as e:
            logger.exception("Discard filter failed")
            ui.notify(f"Discard failed: {e}", type="negative", timeout=4000)

    gallery_box = ui.element("div").classes("cb-cutouts-box w-full")
    with gallery_box:
        with ui.row().classes("cb-gallery-head w-full items-center gap-2 no-wrap"):
            ui.label("Subtomo gallery").classes("cb-section-title")
            ui.label(f"{len(all_pick_indices)} cutouts").classes("cb-gallery-count")
            ui.space()
            toolbelt = ui.row().classes("cb-toolbelt items-center gap-0 no-wrap")
        # The curation bar exists only while curation mode is armed: counter, the
        # degenerate-picks toggle, Reset and Save. With the mode off there is nothing
        # to save, so showing a disabled Save pair would only be noise.
        curation_bar = ui.row().classes("cb-curation-bar w-full items-center gap-2")
        curation_bar.set_visibility(False)
        if can_curate:
            with curation_bar:
                counter_label = ui.label("kept …/…").classes("cb-filter-counter")
                if degenerate_indices:
                    exclude_degen_cb = (
                        ui.checkbox(f"exclude {len(degenerate_indices)} not-extracted", value=True)
                        .props("dense")
                        .classes("cb-degen-toggle")
                        .tooltip(
                            "Picks that produced no subtomo cutout (no match / render failed). On (default) keeps "
                            "them out of the saved set; uncheck to include any that do have a subtomo."
                        )
                        .on_value_change(lambda: (_refresh_grid(), _refresh_counter()))
                    )
                ui.space()
                discard_btn = house_button(
                    "Reset this tomo",
                    _on_discard,
                    kind="danger",
                    tooltip=(
                        "Revert THIS tomogram's picks to the original (all kept). Other tomograms in this "
                        "species keep their curation. Removing the last curated tomogram deletes the filter."
                    ),
                )
                save_btn = house_button(
                    "Save picks",
                    _on_save,
                    kind="accent",
                    tooltip=(
                        "Write optimisation_set_filtered.star + particles_filtered.star next to the subtomo "
                        "job's outputs. Downstream consumers (reconstruct_particle, class3d) auto-prefer the "
                        "filtered file via the IO-slot resolver when it exists."
                    ),
                )

        # Wrapped so a filter switch re-renders the noise-floor reference tiles
        # under the same filter as the grid (the template thumb is its own clean
        # reference and stays raw).
        ref_strip_container = ui.element("div").classes("w-full")

        def _refresh_reference_strip():
            ref_strip_container.clear()
            with ref_strip_container:
                _render_reference_strip(picks, cutout_index, atlas_url, cols, rows, manifest, state)

        _refresh_reference_strip()

        grid_container = ui.element("div").classes("cb-gallery-scroll w-full")
        grid_container._props["id"] = gallery_id

    # ── Toolbelt (11-S3): sort · display filter · 3dmod · curate · fullscreen ──
    with toolbelt:
        sort_btn = _toolbelt_button("sort", "Sort order")
        with sort_btn, ui.menu().props("anchor='bottom right' self='top right'"):
            with ui.element("div").classes("cb-info-card cb-tb-card"):
                sort_select = (
                    ui.select(options={"best": "Best", "worst": "Worst", "z": "Z depth"}, value="best")
                    .props("dense outlined options-dense")
                    .classes("text-xs")
                    .style("min-width: 108px;")
                )
        if len(_filter_presets) >= 2:
            filter_btn = _toolbelt_button("tune", "Display filter — how the cutouts are rendered, never the data")
            with filter_btn, ui.menu().props("anchor='bottom right' self='top right'"):
                with ui.element("div").classes("cb-info-card cb-tb-card"):
                    with ui.row().classes("items-center gap-2 no-wrap"):
                        _render_cutout_filter_controls(
                            _filter_presets, cutout_apix, cutout_apix_source, _on_filter_select
                        )
        peek_refs = _render_3dmod_popover(row, with_peek=True)
        curate_btn = None
        if can_curate:
            curate_btn = _toolbelt_button(
                "edit",
                "Curation mode — while armed, clicking a cutout or a slab dot keeps/drops it and "
                "drag draws a box-select. Off, a click only selects.",
            )
            curate_btn.on("click", lambda: _set_curating(not state["curating"]))
        if mode == "full":
            _toolbelt_button("fullscreen", "Fill the browser window with the viewer (Esc exits)").on(
                "click", js_handler=_FULLSCREEN_JS
            )

    def _refresh_outputs() -> None:
        """The saved filtered-set path, inside the 3dmod popover's `outputs` block —
        present only once a filter exists (after Save), gone after Reset. It is where
        the user goes to confirm the file actually landed."""
        host = peek_refs.get("outputs_host")
        if host is None:
            return
        host.clear()
        p = subtomo_job_dir / picks_filter.OPTIMISATION_SET_FILTERED_NAME if subtomo_job_dir is not None else None
        if p is None or not p.exists():
            host.set_visibility(False)
            return
        host.set_visibility(True)
        fp = str(p)
        with host:
            _cmd_row(
                "filtered set",
                fp,
                status=("check_circle", "text-emerald-600", "Downstream consumers auto-prefer this file."),
            )

    def _set_curating(on: bool) -> None:
        """Arm / disarm curation mode: the toolbelt icon tints, the Save/Reset bar
        appears, the grid gains `cb-curating` (which is what the marquee JS checks
        before starting a drag), and the tiles re-render with the matching hint."""
        state["curating"] = bool(on)
        if curate_btn is not None:
            if on:
                curate_btn.classes(add="cb-tb-on")
            else:
                curate_btn.classes(remove="cb-tb-on")
        curation_bar.set_visibility(bool(on))
        if on:
            grid_container.classes(add="cb-curating")
        else:
            grid_container.classes(remove="cb-curating")
        _refresh_grid()
        _refresh_counter()

    _refresh_outputs()

    def _sorted_pick_indices() -> list:
        sort_mode = state["sort_mode"]
        available = [int(k) for k in cutout_index.keys()]
        if sort_mode == "worst":
            return sorted(available, reverse=True)
        if sort_mode == "z":

            def _z(i):
                return picks[i].get("z", 0) if 0 <= i < len(picks) else 0

            return sorted(available, key=_z, reverse=True)
        return sorted(available)

    tomo_mrc = entry.get("tomo_mrc")
    particle_diameter_ang = (manifest or {}).get("particle_diameter_ang") if isinstance(manifest, dict) else None
    picks_json_path = entry.get("picks_json")
    peek_dir = (Path(picks_json_path).parent / "peeks") if picks_json_path else None

    def _is_kept(pick_idx: int) -> bool:
        ks = state["keep_set"]
        if ks is None:
            return True  # implicit keep-all
        return pick_idx in ks

    def _toggle_keep(pick_idx: int) -> None:
        if pick_idx in degenerate_indices:
            return  # degenerate picks aren't individually keepable (no cutout)
        ks = state["keep_set"]
        if ks is None:
            # First click materializes the keep_set with everything as a
            # baseline so subsequent toggles can remove individual entries.
            ks = set(all_pick_indices)
            state["keep_set"] = ks
        if pick_idx in ks:
            ks.discard(pick_idx)
        else:
            ks.add(pick_idx)
        _refresh_grid()
        _refresh_counter()

    def _on_dot_click(e) -> None:
        # A ghost-dot click on the slab does exactly what a tile click does: select,
        # or keep/drop while curation mode is armed. Dispatched from the hover bridge
        # via a CustomEvent on grid_container.
        try:
            idx = int((e.args or {}).get("idx"))
        except (TypeError, ValueError):
            return
        if state["curating"]:
            _toggle_keep(idx)
        else:
            _select_pick(idx)

    # Captured at render time (valid context). _sync_dropped_dots uses this
    # instead of resolving the client via context.slot — which, inside a
    # tile-click handler, is the tile that _refresh_grid's grid_container.clear()
    # just deleted (the "parent element this slot belongs to has been deleted"
    # crash that also left the Save button stuck disabled because the exception
    # aborted _toggle_keep before _refresh_counter ran).
    _gallery_client = ui.context.client

    def _sync_dropped_dots() -> None:
        # Reflect keep/drop onto the canvas ghost dots: dropped picks (and, when
        # the exclude box is on, degenerate picks) get greyed via a class toggle
        # on the matching dots in THIS species' layers. Keeps the slab and the
        # gallery in agreement whichever side the user clicked.
        ks = state["keep_set"]
        dropped = [] if ks is None else [i for i in all_pick_indices if i not in ks]
        if _exclude_degen_on():
            dropped = list(dropped) + list(degenerate_indices)
        layer_ids = [lid for lid in (xy_host_id, xz_host_id) if lid]
        if not layer_ids:
            return
        js = (
            f"(function(){{const ls={json.dumps(layer_ids)};const d=new Set({json.dumps([str(i) for i in dropped])});"
            "ls.forEach(function(lid){const h=document.getElementById(lid);if(!h)return;"
            "h.querySelectorAll('.cb-pick-ghost[data-pick-idx]').forEach(function(g){"
            "if(d.has(g.getAttribute('data-pick-idx')))g.classList.add('cb-pick-ghost-dropped');"
            "else g.classList.remove('cb-pick-ghost-dropped');});});})();"
        )
        try:
            _gallery_client.run_javascript(js)
        except Exception:
            pass  # client / elements gone (e.g. TS switched) — dots resync on next render

    def _trigger_peek(pick_idx: int) -> None:
        state["selected_idx"] = pick_idx
        import asyncio as _asyncio

        _asyncio.create_task(
            _trigger_peek_for_pick(
                pick_idx, picks, tomo_mrc, peek_dir, pixel_size_ang, particle_diameter_ang, peek_refs, state
            )
        )

    def _select_pick(pick_idx: int | None) -> None:
        """11-S4: the click model outside curation mode. One pick is selected at a
        time; the tile keeps `.selected` and its slab dots `.cb-ghost-selected` until
        another is picked or Esc/click-away clears it. Nothing about keep/drop moves —
        selecting is how you point at a pick, not how you judge it."""
        state["selected"] = None if pick_idx is None else int(pick_idx)
        _sync_selection()

    def _sync_selection() -> None:
        sel = state["selected"]
        layer_ids = [lid for lid in (xy_host_id, xz_host_id) if lid]
        js = (
            f"(function(){{var gid={json.dumps(gallery_id)};var sel={json.dumps('' if sel is None else str(sel))};"
            f"var ls={json.dumps(layer_ids)};"
            "var g=document.getElementById(gid);"
            "if(g){g.querySelectorAll('.cb-gallery-tile.selected').forEach(function(t){"
            "t.classList.remove('selected');});"
            "if(sel){var t=g.querySelector('.cb-gallery-tile[data-pick-idx=\"'+sel+'\"]');"
            "if(t)t.classList.add('selected');}}"
            "ls.forEach(function(lid){var h=document.getElementById(lid);if(!h)return;"
            "h.querySelectorAll('.cb-pick-ghost.cb-ghost-selected').forEach(function(d){"
            "d.classList.remove('cb-ghost-selected');});"
            "if(sel)h.querySelectorAll('.cb-pick-ghost[data-pick-idx=\"'+sel+'\"]').forEach(function(d){"
            "d.classList.add('cb-ghost-selected');});});})();"
        )
        try:
            _gallery_client.run_javascript(js)
        except Exception:
            pass  # client / elements gone (e.g. TS switched) — selection repaints on next render

    def _on_tile_click(pick_idx: int, shift: bool) -> None:
        if shift:
            # Power-user path in BOTH modes: peek-extract the subvolume so the 3dmod
            # popover's per-pick command populates. Doesn't affect keep/drop state.
            _trigger_peek(pick_idx)
            _select_pick(pick_idx)
            return
        if state["curating"]:
            _toggle_keep(pick_idx)
            return
        _select_pick(None if state["selected"] == pick_idx else pick_idx)

    bg_w = cols * _DISPLAY_TILE_PX
    bg_h = rows * _DISPLAY_TILE_PX
    bg_size_css = f"{bg_w}px {bg_h}px"

    def _refresh_grid():
        click_hint = (
            "click: keep/drop · shift-click: extract for 3dmod"
            if state["curating"]
            else "click: select · shift-click: extract for 3dmod"
        )
        grid_container.clear()
        with grid_container, ui.element("div").classes("cb-gallery-grid"):
            for rank, pick_idx in enumerate(_sorted_pick_indices()):
                pos = cutout_index.get(str(pick_idx))
                if not pos:
                    continue
                r, c = pos
                bg_x = -c * _DISPLAY_TILE_PX
                bg_y = -r * _DISPLAY_TILE_PX
                pick = picks[pick_idx] if 0 <= pick_idx < len(picks) else None
                cls = "cb-gallery-tile"
                if not _is_kept(pick_idx):
                    cls += " cb-tile-dropped"
                style = (
                    f"background-image: url({atlas_url}); "
                    f"background-size: {bg_size_css}; "
                    f"background-position: {bg_x}px {bg_y}px;"
                )
                if pick_idx == state["selected"]:
                    cls += " selected"
                tile = ui.element("div").classes(cls).style(style)
                tile._props["data-pick-idx"] = str(pick_idx)
                if pick is not None:
                    # The tile's own tooltip carries the pick's numbers — it is where
                    # score/coords live now that the hover info row is gone (11-S3).
                    parts = [f"#{pick_idx}"]
                    if pick.get("score") is not None:
                        parts.append(f"score={pick['score']:.4f}")
                    parts.append(f"x={int(pick.get('x', 0))}")
                    parts.append(f"y={int(pick.get('y', 0))}")
                    parts.append(f"z={int(pick.get('z', 0))}")
                    parts.append(f"({click_hint})")
                    tile._props["title"] = "  ".join(parts)
                # JS handler captures the shift modifier so we can route to
                # toggle vs. peek-extract; emit returns the dict that becomes
                # e.args on the Python side.
                tile.on(
                    "click",
                    lambda e, i=pick_idx: _on_tile_click(i, bool((e.args or {}).get("shift", False))),
                    js_handler="(event) => { emit({shift: event.shiftKey}); }",
                )
                with tile:
                    ui.html(f"#{rank + 1}", sanitize=False).classes("cb-tile-rank")
                    ui.html(f"{pick_idx}", sanitize=False).classes("cb-tile-idx")
                    if pick is not None and pick.get("z") is not None:
                        ui.html(f"z{int(pick['z'])}", sanitize=False).classes("cb-tile-z")
                    if pick and pick.get("score") is not None:
                        ui.html(f"{pick['score']:.3f}", sanitize=False).classes("cb-tile-score")

            # Degenerate tiles at the end: candidates that produced no cutout.
            # Greyed + non-interactive, index + reason in tooltip — so it's clear
            # why the rendered-tile count is below the pick-index range.
            for f in failures:
                try:
                    fi = int(f.get("i"))
                except (TypeError, ValueError):
                    continue
                reason = str(f.get("reason", "unknown"))
                dcls = "cb-gallery-tile cb-tile-degenerate"
                if _exclude_degen_on():
                    dcls += " cb-tile-dropped"  # same red-orange excluded look as a dropped pick
                dtile = ui.element("div").classes(dcls)
                dtile._props["title"] = f"#{fi} · {reason} — no cutout rendered" + (
                    " · excluded from saved picks" if _exclude_degen_on() else " · kept if it has a subtomo"
                )
                with dtile:
                    ui.html(f"{fi}", sanitize=False).classes("cb-tile-idx")
                    ui.html("✕", sanitize=False).classes("cb-tile-degen-mark")
        _sync_dropped_dots()
        _sync_selection()

    sort_select.on_value_change(lambda e: (state.update(sort_mode=e.value or "best"), _refresh_grid()))

    # The filter TYPE/PARAM selects (rendered above) wire their own handlers via
    # `_on_filter_select`, which swaps `atlas_url` and re-renders.

    # Ghost-dot clicks on the slab act like tile clicks — the bridge dispatches a
    # CustomEvent on grid_container, scoped to it so it's cleaned up with the gallery.
    grid_container.on("cbpickclick", _on_dot_click, js_handler="(e) => emit(e.detail)")
    # Esc / a click on empty gallery space clears the selection (11-S4).
    grid_container.on("cbpickdeselect", lambda _e: _select_pick(None), js_handler="(e) => emit({})")

    # Box-select (lasso): the marquee JS collects the enclosed tiles' pick idxs
    # and dispatches `cbpicklasso` with {idxs, keep}. Default = drop them all;
    # Shift-drag keeps/restores them. One uniform action over the box (not a
    # per-tile toggle, which would be ambiguous over a mixed selection).
    def _on_lasso(e) -> None:
        if not state["curating"]:
            return  # the marquee only arms in curation mode; belt-and-braces on the server too
        args = e.args or {}
        keep = bool(args.get("keep"))
        ks = state["keep_set"]
        if ks is None:
            ks = set(all_pick_indices)
            state["keep_set"] = ks
        changed = False
        for raw in args.get("idxs") or []:
            try:
                i = int(raw)
            except (TypeError, ValueError):
                continue
            if i in degenerate_indices:
                continue
            if keep and i not in ks:
                ks.add(i)
                changed = True
            elif not keep and i in ks:
                ks.discard(i)
                changed = True
        if changed:
            _refresh_grid()
            _refresh_counter()

    grid_container.on("cbpicklasso", _on_lasso, js_handler="(e) => emit(e.detail)")

    ui.run_javascript(_MARQUEE_JS)

    _refresh_grid()
    _refresh_counter()

    if (has_xy and pick_xy_frac) or (has_xz and pick_xz_frac):
        slices = []
        if has_xy and pick_xy_frac:
            slices.append({"id": xy_host_id, "picks": pick_xy_frac})
        if has_xz and pick_xz_frac:
            slices.append({"id": xz_host_id, "picks": pick_xz_frac})
        # Bidirectional hover bridge:
        #   gallery tile mouseover → place marker on each preview, mark
        #     matching ghost dots active.
        #   ghost-dot mouseover     → same, plus add .cb-tile-highlight on
        #     the matching gallery tile and scroll it into view if hidden.
        # The horizontal hover info row this also used to fill is gone (11-S3): the
        # numbers it showed live on each tile's own tooltip, and clicking a tile now
        # SELECTS it, which is the persistent readout.
        bridge_js = f"""
        setTimeout(function() {{
            const galleryId = {json.dumps(gallery_id)};
            const slices = {json.dumps(slices)};
            // Layer hosts live in the always-present left canvas column, so they
            // resolve now and stay valid across tab switches. `s.id` is the
            // .cb-pick-layer id; the marker + ghost dots are its children.
            const wired = slices.map(function(s) {{
                const host = document.getElementById(s.id);
                if (!host) return null;
                return {{
                    host: host,
                    marker: host.querySelector('.cb-pick-marker'),
                    picks: s.picks,
                    layerId: s.id
                }};
            }}).filter(function(x) {{ return x && x.marker; }});
            if (!wired.length) return;

            // Delegate on the Particles card — the common ancestor of BOTH the
            // canvas (left) and every species' gallery (right, in lazily-mounted
            // tab panels). The gallery grid is resolved LAZILY at event time, so
            // the cross-link works no matter which tab was active when this ran.
            // (Fix for the dead-hover bug on non-default tabs: q-tab-panels only
            // mount the active panel, so the old one-shot getElementById(galleryId)
            // bailed for inactive tabs and never wired their listeners.)
            const root = wired[0].host.closest('.cb-section-card') || document.body;
            const ourLayerIds = wired.map(function(w) {{ return w.layerId; }});
            function getGrid() {{ return document.getElementById(galleryId); }}
            function ourGhost(el) {{
                const layer = el.closest && el.closest('.cb-pick-layer');
                return !!layer && ourLayerIds.indexOf(layer.id) !== -1;
            }}

            function placeMarkers(idx) {{
                wired.forEach(function(w) {{
                    const xy = w.picks[idx];
                    if (!xy) {{ w.marker.style.opacity = '0'; return; }}
                    w.marker.style.left = (xy[0] * 100).toFixed(3) + '%';
                    w.marker.style.top = (xy[1] * 100).toFixed(3) + '%';
                    w.marker.style.opacity = '1';
                }});
            }}
            function hideMarkers() {{
                wired.forEach(function(w) {{ w.marker.style.opacity = '0'; }});
            }}
            function setGhostActive(idx, on) {{
                wired.forEach(function(w) {{
                    w.host.querySelectorAll('.cb-pick-ghost[data-pick-idx="' + idx + '"]').forEach(function(g) {{
                        if (on) g.classList.add('cb-ghost-active');
                        else g.classList.remove('cb-ghost-active');
                    }});
                }});
            }}
            function setTileHighlight(idx, on, scrollIntoView) {{
                const grid = getGrid();
                if (!grid) return;
                const tile = grid.querySelector('.cb-gallery-tile[data-pick-idx="' + idx + '"]');
                if (!tile) return;
                if (on) tile.classList.add('cb-tile-highlight');
                else tile.classList.remove('cb-tile-highlight');
                if (on && scrollIntoView) {{
                    const tr = tile.getBoundingClientRect();
                    const gr = grid.getBoundingClientRect();
                    if (tr.top < gr.top || tr.bottom > gr.bottom) {{
                        tile.scrollIntoView({{block: 'nearest', behavior: 'smooth'}});
                    }}
                }}
            }}
            // Clear ALL active state in our scope (every active ghost + every
            // highlighted tile). Called at the start of each hover so only one
            // pick is ever active — moving tile→tile no longer accumulates
            // stuck dots (the bug: mouseout only fired on full grid-exit, so a
            // tile→tile move never deactivated the one you left).
            function clearActive() {{
                wired.forEach(function(w) {{
                    w.host.querySelectorAll('.cb-pick-ghost.cb-ghost-active').forEach(function(g) {{
                        g.classList.remove('cb-ghost-active');
                    }});
                }});
                const grid = getGrid();
                if (grid) grid.querySelectorAll('.cb-tile-highlight').forEach(function(t) {{
                    t.classList.remove('cb-tile-highlight');
                }});
            }}
            function isOurs(el) {{
                if (!el || !el.closest) return false;
                const grid = getGrid();
                const tile = el.closest('.cb-gallery-tile[data-pick-idx]');
                if (tile && grid && grid.contains(tile)) return true;
                const gh = el.closest('.cb-pick-ghost[data-pick-idx]');
                return !!(gh && ourGhost(gh));
            }}

            // Single delegated mouseover/mouseout on the card handles BOTH
            // directions: a gallery tile (filtered to OUR grid) and a ghost dot
            // (filtered to OUR species' layers). Filtering keeps the N per-species
            // bridges on the shared card from cross-firing. Every mouseover
            // resets first so exactly one pick is active at a time.
            root.addEventListener('mouseover', function(e) {{
                if (!e.target.closest) return;
                const grid = getGrid();
                const tile = e.target.closest('.cb-gallery-tile[data-pick-idx]');
                if (tile && grid && grid.contains(tile)) {{
                    const idx = tile.getAttribute('data-pick-idx');
                    clearActive();
                    placeMarkers(idx);
                    setGhostActive(idx, true);
                    return;
                }}
                const ghost = e.target.closest('.cb-pick-ghost[data-pick-idx]');
                if (ghost && ourGhost(ghost)) {{
                    const idx = ghost.getAttribute('data-pick-idx');
                    clearActive();
                    placeMarkers(idx);
                    setGhostActive(idx, true);
                    setTileHighlight(idx, true, true);
                }}
            }});
            root.addEventListener('mouseout', function(e) {{
                if (!e.target.closest) return;
                const leaving = e.target.closest('.cb-gallery-tile[data-pick-idx]') ||
                    e.target.closest('.cb-pick-ghost[data-pick-idx]');
                if (!leaving || !isOurs(leaving)) return;
                // Only tear down when the pointer leaves our interactive area
                // entirely; tile→tile / tile→ghost moves are handled by the next
                // mouseover's clearActive().
                if (!isOurs(e.relatedTarget)) {{
                    clearActive();
                    hideMarkers();
                }}
            }});
            // Ghost-dot click → the same action a tile click takes: select, or
            // keep/drop while curation mode is armed. Dispatched as a CustomEvent on
            // our grid so the NiceGUI handler bound to grid_container picks it up
            // (scoped to the gallery, auto-cleaned).
            root.addEventListener('click', function(e) {{
                if (!e.target.closest) return;
                const ghost = e.target.closest('.cb-pick-ghost[data-pick-idx]');
                if (!ghost || !ourGhost(ghost)) return;
                const grid = getGrid();
                if (grid) grid.dispatchEvent(new CustomEvent('cbpickclick',
                    {{detail: {{idx: ghost.getAttribute('data-pick-idx')}}}}));
            }});
        }}, 80);
        """
        ui.run_javascript(bridge_js)


def _render_picks_scatter_section(row: dict, entry: dict, manifest: dict, has_subtomo_job: bool = False) -> None:
    """Scatter-only fallback: X/Y + X/Z + score histogram. Renders only when
    the gallery isn't available.

    `has_subtomo_job` separates "extraction hasn't run" from "it ran but this
    tomogram's manifest entry carries no cutout atlas" — the second is a stale
    manifest, not a missing job, and the header must not claim otherwise."""
    picks_json_path = entry.get("picks_json")
    picks_data = (
        _read_picks_json(Path(picks_json_path)) if picks_json_path else {"picks": [], "tomo_dims_xyz_px": [0, 0, 0]}
    )
    picks = picks_data.get("picks", [])
    tomo_dims = tuple(picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1])
    score_field = manifest.get("score_field")
    pixel_size_ang = entry.get("pixel_size_ang")
    xz_url = vis_asset_url(entry["xz_preview"]) if entry.get("xz_preview") else None

    x_dim, y_dim, z_dim = (max(int(d), 1) for d in tomo_dims)
    xy_aspect = min(2.5, max(0.5, x_dim / y_dim))
    xz_aspect = min(5.0, max(1.5, x_dim / z_dim))
    xy_target_h = 380
    xy_max_w = min(640, max(280, int(xy_target_h * xy_aspect)))
    xz_target_h = 220
    xz_max_w = min(720, max(280, int(xz_target_h * xz_aspect)))

    with ui.element("div").classes("cb-section-card w-full"):
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("scatter_plot", size="14px").classes("text-indigo-600")
            ui.label("Pick distribution").classes("cb-section-title")
            ui.label(f"  ({row['n_picks']} picks)").classes("text-[10px] text-gray-400")
            ui.space()
            if has_subtomo_job:
                ui.label("· cutouts not in the preview manifest — gallery view unavailable").classes(
                    "text-[10px] text-amber-700 italic"
                ).tooltip(
                    "Subtomo extraction HAS run for this species, but this tomogram's preview entry carries no "
                    "cutout atlas — the manifest predates the extraction job or a preview pass was interrupted "
                    "before it recorded them. Re-run 'Render missing' from the Journey preview menu to rebuild it."
                )
            else:
                ui.label("· no subtomo extraction yet — gallery view unavailable").classes(
                    "text-[10px] text-amber-700 italic"
                )

        with ui.row().classes("w-full gap-3 items-stretch flex-wrap"):
            with ui.column().classes("gap-1").style(f"flex: 1 1 320px; min-width: 280px; max-width: {xy_max_w}px;"):
                ui.label("X / Y top-down").classes("cb-section-title")
                with ui.element("div").classes("cb-aspect").style(f"aspect-ratio: {xy_aspect};"):
                    xy_plot = ui.plotly(_build_xy_scatter_fig(picks, tomo_dims, score_field)).style(
                        "width: 100%; height: 100%;"
                    )
            with (
                ui.column()
                .classes("gap-2 cb-picks-right")
                .style(f"flex: 1 1 320px; min-width: 280px; max-width: {xz_max_w}px;")
            ):
                ui.label("X / Z side").classes("cb-section-title")
                with ui.element("div").classes("cb-aspect").style(f"aspect-ratio: {xz_aspect};"):
                    xz_plot = ui.plotly(_build_xz_scatter_fig(picks, tomo_dims, score_field, xz_url)).style(
                        "width: 100%; height: 100%;"
                    )
                ui.label("Score distribution").classes("cb-section-title")
                ui.plotly(_build_score_hist_fig(picks, score_field)).style("width: 100%; height: 160px;")
                ui.label("Hovered pick").classes("cb-section-title")
                hover_labels = _render_hover_card_skeleton()

                def on_hover(e, _picks=picks, _ps=pixel_size_ang, _labels=hover_labels):
                    _update_hover_card(e, _picks, _ps, _labels)

                xy_plot.on("plotly_hover", on_hover, throttle=0.08)
                xz_plot.on("plotly_hover", on_hover, throttle=0.08)


def _render_hover_card_skeleton() -> dict:
    """The scatter fallback's "Hovered pick" readout — a 2-column key/value grid the
    Plotly hover handler writes into.

    The gallery's horizontal variant of this (an info row above the grid, filled by the
    hover bridge in JS) is gone since 11-S3: it was the top item of the kitchen sink,
    and the same numbers now sit on each tile's own tooltip. The scatter fallback keeps
    it because a scatter point has nowhere else to say what it is."""
    labels: dict = {}
    with ui.element("div").classes("cb-hover-card") as card:
        for key in ("idx", "px", "ang", "score", "z%-tile", "nn"):
            ui.label(key).classes("cb-hover-key")
            v_el = ui.label("—").classes("cb-hover-val")
            v_el._props["data-hover-key"] = key
            labels[key] = v_el
    labels["__card"] = card
    return labels


def _update_hover_card(e, picks: list, pixel_size_ang, labels: dict) -> None:
    args = getattr(e, "args", None) or {}
    points = args.get("points") or []
    if not points:
        return
    p = points[0] or {}
    cd = p.get("customdata") or []
    if not cd:
        return
    try:
        idx = int(cd[0])
    except (TypeError, ValueError):
        return
    if idx < 0 or idx >= len(picks):
        return
    pick = picks[idx]

    if labels.get("__card") is None:
        return

    labels["idx"].set_text(f"#{pick['i']}")
    labels["px"].set_text(f"{pick['x']}, {pick['y']}, {pick['z']}")
    if pixel_size_ang:
        ax = pick["x"] * pixel_size_ang
        ay = pick["y"] * pixel_size_ang
        az = pick["z"] * pixel_size_ang
        labels["ang"].set_text(f"{ax:.0f}, {ay:.0f}, {az:.0f}")
    else:
        labels["ang"].set_text("(no pixel size)")
    if pick.get("score") is not None:
        labels["score"].set_text(f"{pick['score']:.4f}")
    else:
        labels["score"].set_text("—")
    if pick.get("z_pct") is not None:
        labels["z%-tile"].set_text(f"{pick['z_pct']:.0f}")
    else:
        labels["z%-tile"].set_text("—")
    if pick.get("nn_px") is not None:
        nn_px = pick["nn_px"]
        if pixel_size_ang:
            labels["nn"].set_text(f"{nn_px:.1f} px ({nn_px * pixel_size_ang:.0f} Å)")
        else:
            labels["nn"].set_text(f"{nn_px:.1f} px")
    else:
        labels["nn"].set_text("—")


# ---------------------------------------------------------------------------
# Per-instance row collection: builds row dicts for every tomogram in a
# candidate-extract job, merging tomograms.star and the preview manifest.
# ---------------------------------------------------------------------------


def _collect_tomo_rows_for_instance(job_dir: Path, project_path: Path) -> list[dict]:
    tomograms_star = job_dir / "tomograms.star"
    tomo_df = read_tomograms_table(tomograms_star)
    manifest = read_preview_manifest(job_dir) or {}
    tomo_entries = manifest.get("tomograms") or {}
    summary = manifest.get("summary") or {}
    missing_volume = set(summary.get("missing_volume") or [])
    errored_map = {e["tomo"]: e.get("error", "") for e in (summary.get("errored") or [])}
    # "zero-picks": orchestrator processed this TS but PyTOM produced no
    # candidates above cutoff. Without this bucket the per-TS card falls into
    # the generic "no-preview" spinner branch and lies to the user about being
    # "still generating" indefinitely.
    zero_picks = set(summary.get("zero_picks") or [])

    rows: list[dict] = []
    if tomo_df is None:
        for tomo_name, entry in tomo_entries.items():
            label, (stage, beam) = position_label(tomo_name)
            mod_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
            rows.append(
                {
                    "tomo_name": tomo_name,
                    "position_label": label,
                    "stage": stage,
                    "beam": beam,
                    "vol_path": entry.get("tomo_mrc"),
                    "mod_path": str(mod_path),
                    "mod_exists": mod_path.exists(),
                    "n_picks": entry.get("n_picks"),
                    "score_range": entry.get("score_range"),
                    "status": "ok" if entry.get("picks_json") else "no-preview",
                    "error": None,
                }
            )
        for tomo_name in zero_picks - set(tomo_entries.keys()):
            label, (stage, beam) = position_label(tomo_name)
            mod_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
            rows.append(
                {
                    "tomo_name": tomo_name,
                    "position_label": label,
                    "stage": stage,
                    "beam": beam,
                    "vol_path": None,
                    "mod_path": str(mod_path),
                    "mod_exists": False,
                    "n_picks": 0,
                    "score_range": None,
                    "status": "zero-picks",
                    "error": None,
                }
            )
    else:
        for _, tomo_row in tomo_df.iterrows():
            tomo_name = str(tomo_row["rlnTomoName"])
            label, (stage, beam) = position_label(tomo_name)
            entry = tomo_entries.get(tomo_name) or {}
            vol_path = resolve_volume_for_3dmod(tomo_row, project_path)
            mod_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
            if tomo_name in missing_volume and not entry.get("picks_json"):
                status = "missing-volume"
            elif tomo_name in errored_map:
                status = "errored"
            elif entry.get("picks_json"):
                status = "ok"
            elif tomo_name in zero_picks:
                status = "zero-picks"
            else:
                status = "no-preview"
            rows.append(
                {
                    "tomo_name": tomo_name,
                    "position_label": label,
                    "stage": stage,
                    "beam": beam,
                    "vol_path": str(vol_path) if vol_path else None,
                    "mod_path": str(mod_path),
                    "mod_exists": mod_path.exists(),
                    "n_picks": entry.get("n_picks"),
                    "score_range": entry.get("score_range"),
                    "status": status,
                    "error": errored_map.get(tomo_name),
                }
            )
    rows.sort(key=lambda r: (r["stage"], r["beam"], r["tomo_name"]))
    return rows


# ---------------------------------------------------------------------------
# Generation handlers (regen previews / IMOD models from per-card buttons)
# ---------------------------------------------------------------------------


def _make_imod_command_runner():
    from services.computing.container_service import get_container_service

    container_service = get_container_service()

    def runner(cmd: str, cwd: Path) -> None:
        import subprocess

        wrapped = container_service.wrap_command_for_tool(cmd, cwd=cwd, tool_name="imod", additional_binds=[str(cwd)])
        result = subprocess.run(wrapped, shell=True, capture_output=True, text=True, cwd=cwd)
        if result.returncode != 0:
            raise RuntimeError(
                f"Container command failed (rc={result.returncode}): {result.stderr.strip() or result.stdout.strip()}"
            )

    return runner


def _generate_imod_sync(
    candidates_star: Path, tomograms_star: Path, diameter: float, job_dir: Path, project_path: Path
) -> None:
    generate_candidate_vis(
        candidates_star=candidates_star,
        tomograms_star=tomograms_star,
        particle_diameter_ang=diameter,
        output_dir=job_dir,
        command_runner=_make_imod_command_runner(),
        project_root=project_path,
    )


# Per-mount dedup so the auto-kick helpers don't pile up completion timers
# when the user clicks between tilt-series in the sidebar. BackgroundTask's
# own dedup_key prevents redundant *work*, but we still want to avoid
# installing multiple ui.timer pollers per (job_dir).
_AUTO_KICKED_PREVIEWS: set[str] = set()
_AUTO_KICKED_IMOD: set[str] = set()


def reset_auto_kick_state() -> None:
    """Clear the auto-kick dedup sets — used by `open_tomo_dashboard` so each
    fresh dashboard mount can re-trigger generation if the page is reloaded."""
    _AUTO_KICKED_PREVIEWS.clear()
    _AUTO_KICKED_IMOD.clear()
    _AUTO_KICKED_RECON_SLABS.clear()
    _AUTO_KICKED_LIST_CUTOUTS.clear()


def _auto_kick_preview_generation(instance_id: str, job_model, job_dir: Path, project_path: Path, refresh) -> bool:
    """If the candidate-extract job has succeeded but some tomograms are
    missing from the preview manifest, kick off a background 'Render
    missing' with completion handler that refreshes the page when done.
    Returns True iff a kickoff was submitted (or one was already in
    flight). Safe to call on every render — both module-level set and
    BackgroundTask dedup_key prevent re-submission."""
    key = str(job_dir)
    if key in _AUTO_KICKED_PREVIEWS:
        return False
    if getattr(job_model, "execution_status", None) != JobStatus.SUCCEEDED:
        return False
    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        return False
    _AUTO_KICKED_PREVIEWS.add(key)

    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
    state = current_project_state()

    async def _run(progress_cb):
        import asyncio as _asyncio

        return await _asyncio.to_thread(
            generate_candidate_previews,
            candidates_star,
            tomograms_star,
            diameter,
            job_dir,
            project_path,
            progress_cb,
            False,
            state,
            instance_id,
            job_model,
        )

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title=f"Auto-render previews · {instance_id}",
        subtitle="Filling missing tomogram entries",
        project_path=str(project_path),
        dedup_key=f"render-previews:{job_dir}:no-force",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)
    return True


def _auto_kick_imod_generation(instance_id: str, job_model, job_dir: Path, project_path: Path, refresh) -> bool:
    """If candidates.star exists for a succeeded extract but the IMOD .mod
    overlays aren't on disk, kick off background generation that auto-
    refreshes the 3dmod command lines on completion. Same dedup semantics
    as the preview kickoff above."""
    key = str(job_dir)
    if key in _AUTO_KICKED_IMOD:
        return False
    if getattr(job_model, "execution_status", None) != JobStatus.SUCCEEDED:
        return False
    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        return False
    imod_dir = job_dir / "vis" / "imodPartRad"
    if imod_dir.exists() and any(imod_dir.glob("*.mod")):
        return False  # already have overlays
    _AUTO_KICKED_IMOD.add(key)

    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))

    async def _run(progress_cb):
        import asyncio as _asyncio

        progress_cb(0, 0, "generating IMOD .mod overlays…")
        await _asyncio.to_thread(_generate_imod_sync, candidates_star, tomograms_star, diameter, job_dir, project_path)
        return "IMOD overlays ready; 3dmod commands now include them"

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title=f"Auto-generate IMOD overlays · {instance_id}",
        subtitle="Per-tomogram .mod files for 3dmod",
        project_path=str(project_path),
        dedup_key=f"imod-models:{job_dir}",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)
    return True


async def _handle_generate_imod_for_instance(
    instance_id: str, job_model, job_dir: Path, project_path: Path, btn, refresh
) -> None:
    """Submit IMOD model generation to the background task registry. Same
    pattern as the preview-render handler: fire-and-forget, user tracks
    via the workspace tray."""
    import asyncio as _asyncio

    from ui.background_task import BackgroundTask

    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        ui.notify(
            "candidates.star or tomograms.star missing — cannot generate IMOD models", type="negative", timeout=4000
        )
        return
    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))

    async def _run(progress_cb) -> str:
        # IMOD generation has no per-tomogram progress hook today; treat as
        # indeterminate (total=0).
        progress_cb(0, 0, "generating IMOD .mod overlays…")
        await _asyncio.to_thread(_generate_imod_sync, candidates_star, tomograms_star, diameter, job_dir, project_path)
        return "IMOD overlays ready; 3dmod commands now include them"

    BackgroundTask(
        title=f"IMOD models · {instance_id}",
        subtitle="Generate .mod overlays for 3dmod",
        project_path=str(project_path),
        dedup_key=f"imod-models:{job_dir}",
    ).submit(_run)
    refresh()


async def _handle_generate_for_instance(
    instance_id: str, job_model, job_dir: Path, project_path: Path, force: bool, btn, refresh
) -> None:
    """Submit a preview-render to the BackgroundTaskRegistry. Returns
    immediately; user tracks progress via the workspace tray. Multiple
    clicks on the same (job_dir, force) combo dedupe to a single task to
    keep concurrent writes to manifest.json from racing each other."""
    import asyncio as _asyncio

    from ui.background_task import BackgroundTask

    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        ui.notify("candidates.star or tomograms.star missing — cannot render previews", type="negative", timeout=4000)
        return
    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
    state = current_project_state()

    action = "Re-render all" if force else "Render missing"
    subtitle = "Bypass cache; regenerate every tomogram" if force else "Skip tomograms with a fresh manifest entry"

    async def _run(progress_cb) -> str:
        # The orchestrator is sync; off-thread it so the event loop stays
        # responsive. progress_cb signature matches:
        #   preview_orchestrator.py: progress_cb(i, total, tomo_name)
        # which lines up with our registry's (current, total, message).
        summary = await _asyncio.to_thread(
            generate_candidate_previews,
            candidates_star,
            tomograms_star,
            diameter,
            job_dir,
            project_path,
            progress_cb,
            force,
            state,
            instance_id,
            job_model,
        )
        n_new = len(summary["ok"])
        n_cached = len(summary["skipped_cached"])
        n_missing = len(summary["missing_volume"])
        n_err = len(summary["errored"])
        # Pull zero-pick count from the manifest summary if the orchestrator
        # recorded it (manifest v10+).
        try:
            from services.visualization.preview_orchestrator import read_preview_manifest

            manifest = read_preview_manifest(job_dir) or {}
            n_zero = len((manifest.get("summary") or {}).get("zero_picks") or [])
        except Exception:
            n_zero = 0

        parts = [f"{n_new} rendered", f"{n_cached} cached"]
        if n_zero:
            parts.append(f"{n_zero} zero-picks")
        if n_missing:
            parts.append(f"{n_missing} missing volume")
        if n_err:
            parts.append(f"{n_err} errored")
        return ", ".join(parts)

    BackgroundTask(
        title=f"Journey previews · {instance_id}",
        subtitle=f"{action} — {subtitle}",
        project_path=str(project_path),
        # Dedup keyed by job_dir + force flag so concurrent clicks coalesce
        # rather than racing on writes to the same manifest.json.
        dedup_key=f"preview-render:{job_dir}:force={force}",
    ).submit(_run)
    refresh()


# ---------------------------------------------------------------------------
# The full page (11-S5): the viewer filling the workspace main area
# ---------------------------------------------------------------------------


class PickViewerPage:
    """The `mode="full"` mount, swapped into the workspace main area like the Journey.

    Built once (lazily, on the first `viewer ↗`) and re-pointed at a (species, tomogram)
    by `show()`, so switching tomograms costs one render instead of a page rebuild. It is
    NOT a dialog: the whole point of the full page is the width and height a dialog can't
    give the slabs.

    `callbacks` is the workspace's dict; the page uses `species_select`/`toggle_journey`/
    `journey_show_ts` for its back-links and nothing else."""

    def __init__(self, container, project_path: Path, callbacks: dict | None = None) -> None:
        self.container = container
        self.project_path = Path(project_path)
        self.callbacks = callbacks or {}
        self.species_id: str | None = None
        self.tomo_name: str | None = None
        self._flight = SingleFlight()
        # Coalesced rebuild, same shape as the Journey's: the auto-kick background
        # renders (slabs, cutout atlases, previews) each call `refresh` on completion,
        # and those callbacks run with NO client context — building UI there would
        # explode. They raise a flag; this timer, created in the client's context, does
        # the one rebuild once the burst goes quiet.
        self._req = {"pending": False, "quiet": 0}
        self._timer = ui.timer(0.2, self._flush)

    async def show(self, species_id: str | None, tomo_name: str | None) -> None:
        """Point the page at a (species, tomogram) and render it. Guarded: the registry
        row's link sits in a poll-refreshed container, so several clicks can land."""
        async with self._flight("show") as acquired:
            if not acquired:
                return
            if species_id:
                self.species_id = species_id
            if tomo_name:
                self.tomo_name = tomo_name
            self.render()

    def set_active(self, on: bool) -> None:
        """Pause the coalesce timer while the page isn't the visible view — driven by
        the workspace's `_switch_to`, like the Journey's."""
        try:
            self._timer.activate() if on else self._timer.deactivate()
        except Exception:
            pass  # timer torn down with the client

    def render(self) -> None:
        self.container.clear()
        with self.container:
            self._render_body()

    def _render_body(self) -> None:
        # Explicit-path resolution, not the tab accessor: `refresh` reaches here from
        # BackgroundTask completions too, where a bare tab lookup yields a blank
        # throwaway state (ui/current_project.py's W2 warning).
        state = get_project_state_for(self.project_path)
        with ui.element("div").classes("cb-viewer-page"):
            self._render_header(state)
            if not self.tomo_name:
                with ui.element("div").classes("cb-empty"):
                    ui.icon("scatter_plot", size="28px").classes("text-gray-400")
                    ui.label("No tomogram selected.").classes("text-xs")
                    ui.label(
                        "Open the viewer from a tomogram group on the Particles registry's "
                        "Picks & curation tab."
                    ).classes("text-[11px] italic text-gray-500")
                return
            rendered = render_particles_section(
                self.tomo_name,
                state,
                self.project_path,
                self.refresh,
                None,
                self._manage_species,
                mode="full",
                initial_species_id=self.species_id,
            )
            if not rendered:
                with ui.element("div").classes("cb-empty"):
                    ui.icon("hourglass_empty", size="28px").classes("text-gray-400")
                    ui.label(f"Nothing to show for {self.tomo_name} yet.").classes("text-xs")
                    ui.label(
                        "The viewer needs a reconstructed tomogram and at least one registered species."
                    ).classes("text-[11px] italic text-gray-500")

    def _manage_species(self, species_id: str) -> None:
        """The route back into the registry, composed the way the Journey composes it:
        select the species AND land on Picks & curation. Without the second half the page
        reuses its last tab, and none of the verbs this link promises are on Overview."""
        open_species = self.callbacks.get("open_species")
        if open_species is None:
            return
        open_species(species_id)
        select_tab = self.callbacks.get("species_select_tab")
        if select_tab is not None:
            select_tab("picks")

    def _render_header(self, state) -> None:
        """Tomogram identity + the two back-links (registry row, Journey). The viewer is
        a leaf view with no nav icon of its own, so both routes out live here."""
        species = state.get_species(self.species_id) if self.species_id else None
        with ui.row().classes("cb-viewer-page-head w-full items-center gap-3 no-wrap"):
            ui.icon("scatter_plot", size="16px").classes("text-indigo-600")
            ui.label("Pick viewer").classes("cb-section-title")
            if self.tomo_name:
                ui.label(self.tomo_name).classes("text-[11px] font-mono text-gray-700")
            if species is not None:
                ui.label(str(getattr(species, "name", "") or self.species_id)).classes(
                    "text-[10px] font-mono text-gray-500"
                )
            ui.space()
            if self.species_id and self.callbacks.get("open_species") is not None:
                ui.label("← Particles registry").classes(
                    "text-[10px] text-indigo-500 cursor-pointer underline decoration-dotted"
                ).on("click", lambda _e: self._manage_species(self.species_id)).tooltip(
                    "Back to this species' Picks & curation tab — the row this viewer was opened from"
                )
            if self.tomo_name and self.callbacks.get("toggle_journey") is not None:
                ui.label("journey ↗").classes(
                    "text-[10px] text-indigo-500 cursor-pointer underline decoration-dotted"
                ).on("click", lambda _e: self._open_journey()).tooltip(
                    "This tomogram's whole pipeline — motion, CTF, alignment, reconstruction, then these picks"
                )

    async def _open_journey(self) -> None:
        toggle = self.callbacks.get("toggle_journey")
        if toggle is None:
            return
        await toggle()
        show = self.callbacks.get("journey_show_ts")
        if show is not None and self.tomo_name:
            await show(self.tomo_name, section="particles")

    def refresh(self) -> None:
        """What the viewer's background renders call when their work lands. Raises the
        rebuild flag rather than rendering: the caller may have no client context."""
        self._req["pending"] = True
        self._req["quiet"] = 0

    def _flush(self) -> None:
        if not self._req["pending"]:
            return
        self._req["quiet"] += 1
        if self._req["quiet"] < 2:
            return  # still inside a burst — wait one more tick
        self._req["pending"] = False
        self._req["quiet"] = 0
        try:
            self.render()
        except Exception:
            logger.exception("pick viewer refresh failed")

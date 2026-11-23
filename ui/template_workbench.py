"""Template Workbench — the Templates & masks tab of the Particles registry.

  ┌ TEMPLATES ─────────── New template… ┐ ┌ VIEWER ──────┐   (the viewer beside the tables
  │ table, as tall as its rows          │ │ molstar, once │    in a wide pane, under them in
  ├ MASKS ─────────────── New mask… ────┤ │ loaded        │    a narrow one: `.cb-tm-split`)
  │ table                               │ └───────────────┘
  ▸ ACTIVITY (folded)

The tables are `.cb-tw-table` grids (CSS in ui/dashboard/css.py) in the job page's Table
look: file (`path_text`), polarity or method, header, σ, size, then the eye and the ×. The
creation forms sit in two dialogs (New template…: Shape, PDB, EMDB, Import, Edit current;
New mask…: From template, Sphere, Import), built once.

Design rules:
  - Inks and hairlines only. The accent marks state (the selected row's ring); an amber
    dot marks something to look at (a large file, an unusual σ), red only a broken volume.
    Polarity and method are words (`ui/components/chip.py`).
  - Selection is a class flip across kept row handles; a table is rebuilt only when
    its membership changes (register / delete). Clicking a row must not destroy it.
  - All template-producing actions (shape, pdb, emdb, import, resample,
    apply-lowpass, flip-polarity) write to canonical paths and skip the
    write if a registered entry already exists at that path.
  - Delete is confirmed in a modal; removes the file + sidecar from disk.
  - One workbench per species, never rebuilt: the molstar iframe (mounted on the first
    load) and its window `message` listener live as long as the page.

Schema reference: services/project_state.py — species.templates and
species.masks are sibling collections with UUID identity.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path

from fastapi.responses import FileResponse, HTMLResponse
from nicegui import app, context, ui

from services import species_admin, species_edit
from services.http_responses import NO_GZIP
from services.models_base import SPECIES_OVERLAY_COLORS
from services.project_state import (
    ParticleSpecies,
    ParticleTemplate,
    TemplateMask,
    get_project_state_for,
    is_in_loaded_project,
    is_registered_volume,
)
from services.templating.template_metadata import read_template_header
from ui.components.check import house_check
from ui.components.chip import render_method_chip, render_polarity_chip
from ui.components.dialogs import dialog_host
from ui.components.path_text import path_text
from ui.components.reactive import SingleFlight
from ui.components.segmented import render_segmented
from ui.components.buttons import house_button, icon_button
from ui.components.fields import house_field as _field
from ui.job_plugins._field_styles import PANEL_TITLE_STYLE, SECTION_STYLE, section_head, voice
from ui.components.template_viewer import TemplateViewerController, render_template_viewer
from ui.local_file_picker import local_file_picker
from ui.template_import_dialog import open_template_import_dialog

logger = logging.getLogger(__name__)


# ─── FastAPI routes for the molstar embed ────────────────────────────────

_MOLSTAR_EMBED_JS = Path(__file__).resolve().parent.parent / "static" / "molstar" / "embed.js"


def _molstar_cache_buster() -> str:
    """Use embed.js's mtime as a cache-buster query string so a rebuild
    automatically forces the browser to re-fetch. Otherwise the browser
    caches the old bundle and a deploy looks like nothing changed."""
    try:
        return str(int(_MOLSTAR_EMBED_JS.stat().st_mtime))
    except OSError:
        return "0"


def _build_molstar_embed_html() -> str:
    return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        html, body, #app {{ width: 100%; height: 100%; overflow: hidden; background: #1a1a1a; }}
    </style>
</head>
<body>
    <div id="app"></div>
    <script type="module" src="/static/molstar/embed.js?v={_molstar_cache_buster()}"></script>
</body>
</html>
"""


@app.get("/molstar-workbench")
def molstar_workbench_viewer():
    return HTMLResponse(_build_molstar_embed_html())


_VOLUME_SUFFIXES = (".mrc", ".map", ".rec", ".ccp4")


@app.get("/api/file")
def serve_file(path: str):
    # Only what the viewer loads: a volume inside a loaded project, or a registered template/mask.
    try:
        p = Path(path).resolve(strict=True)
    except (FileNotFoundError, RuntimeError):
        return {"error": "not found"}
    if p.suffix.lower() not in _VOLUME_SUFFIXES or not p.is_file():
        return {"error": "not a volume"}
    if not (is_in_loaded_project(p) or is_registered_volume(p)):
        return {"error": "outside project roots"}
    # NO_GZIP: a volume compressed by the middleware would hold the event loop for seconds.
    return FileResponse(p, media_type="application/octet-stream", headers=NO_GZIP)


# ─── Shared style chrome ────────────────────────────────────────────────
# The page's blocks are sections (a Rule head over its content), one box deep. Inside the
# dialogs' forms: TOOL (`_TOOL_CLS`, 10 px semibold) names a tool that owns inputs, a field
# label (`house_field`) one input, HINT a sentence.
_TITLE_CLS = "text-sm font-semibold text-gray-800"
_TOOL_CLS = "text-[10px] font-semibold text-slate-600"
_HINT_CLS = "text-[10px] text-gray-400"
_MONO_CLS = "text-[10px] font-mono text-gray-600"
# The creation dialogs' forms. Sentence case, matching the registry's tab labels. PDB and
# EMDB are two forms so each has one action in the footer. "From template" says what the
# tool does; which tool it is (relion_mask_create) is named in the form's own hint.
_SOURCE_TABS = (("shape", "Shape"), ("pdb", "PDB"), ("emdb", "EMDB"), ("import", "Import"), ("edit", "Edit current"))
# "From template" first and the default: most particles are not spherical, so the
# threshold-derived mask is the normal case and the sphere is the special one.
_MASK_TABS = (("relion", "From template"), ("sphere", "Sphere"), ("import", "Import"))
# One column shape for both tables: in use, file (the only one that flexes), polarity or
# method, header, σ, size, actions.
_TW_COLS = "10px minmax(0, 1fr) 84px 150px 64px 58px 46px"

_SANS = "font-family: 'IBM Plex Sans', sans-serif;"
_ROW_MONO = "font-family: 'IBM Plex Mono', monospace; font-size: 9.5px; font-variant-numeric: tabular-nums;"
_BODY = "#1e293b"
_SOFT = "#5b7089"
_CAPTION = f"{_SANS} font-size: 9.5px; color: {_SOFT}; line-height: 1.45;"
_TH = voice("th", font="'IBM Plex Sans', sans-serif", size="9px", weight=600, ink="#7d93ab") + " white-space: nowrap;"
# The two marks a row may carry: amber for something to look at, red only for a broken volume.
_MARK_COLORS = {"warn": "#d97706", "bad": "#b91c1c"}


def _mark_dot(kind: str) -> None:
    ui.element("span").style(
        f"width: 5px; height: 5px; border-radius: 50%; background: {_MARK_COLORS[kind]}; "
        "display: inline-block; margin-right: 4px; flex-shrink: 0;"
    )


def _stacked_panels(holder: ui.element, panels: dict[str, ui.element]):
    """`_panel`/`_show` pair over a grid-stacked holder: all panels live in the same
    grid cell, inactive ones are visibility-hidden (not display-none), so the stack
    keeps the height of its tallest panel and switching tabs never shoves the content
    below it around.

    Styles are inline, not classes: the page-shell stylesheet is served stale on this
    deployment, and a class-based version renders all panels at once with a dead
    switch until the shell refreshes. Inline styles ride the socket-delivered DOM and
    cannot go stale (same as _field_styles.py)."""
    holder.style("display: grid; width: 100%;")

    def _panel(key: str) -> ui.element:
        with holder:
            el = ui.column().classes("w-full gap-2 px-2 py-2").style("grid-area: 1 / 1; min-width: 0;")
        panels[key] = el
        return el

    def _show(key: str) -> None:
        for k, el in panels.items():
            if k == key:
                el.style("visibility: visible; pointer-events: auto;")
            else:
                el.style("visibility: hidden; pointer-events: none;")

    return _panel, _show


def _flip_selection(rows: dict, selected_id: str) -> None:
    """Move the `selected` class to one row. No DOM is rebuilt, so a click landing while
    another is in flight still hits a live element."""
    for entry_id, row in rows.items():
        if entry_id == selected_id:
            row.classes(add="cb-sel")
        else:
            row.classes(remove="cb-sel")


def _membership_moved(rows: dict, species, attr: str) -> bool:
    """Whether the entries of `species.<attr>` differ from what is currently rendered —
    the only thing that justifies clearing and rebuilding a list."""
    if species is None:
        return True
    ids = [getattr(e, "id", "") for e in getattr(species, attr, []) or []]
    return ids != list(rows.keys())


# The generators' pixel size until the project's is known (`_load_default_pixel_size`), and when it has none.
_UNSET_APIX = 10.0


def _scales(scale_xy, scale_z) -> dict:
    """The two scales as `plan_template` takes them: none when both are 1 (RELION's own resample)."""
    sxy, sz = float(scale_xy or 1.0), float(scale_z or 1.0)
    return {} if (sxy, sz) == (1.0, 1.0) else {"scale_xy": sxy, "scale_z": sz}


def _scale_fields(owner, attr_xy: str, attr_z: str) -> None:
    """A filament's lattice measured against the data's map: the map scaled across Z and along it before it is
    sampled (1 and 1: as deposited)."""
    for label, attr, hint in (
        ("scale ⊥", attr_xy, "The map scaled across Z by this before it is sampled (1: as deposited)"),
        ("scale ∥", attr_z, "The map scaled along Z by this before it is sampled (1: as deposited)"),
    ):
        _field(
            label,
            lambda attr=attr: ui.number(
                value=getattr(owner, attr), step=0.001, min=0.5, max=2.0, format="%.4f"
            ).bind_value(owner, attr),
            width="w-20",
            hint=hint,
        )


def _values_line(values: dict) -> str:
    """A made template's or mask's settled values, for the activity log and the task tray."""
    return " ".join(f"{k}={v:g}" if isinstance(v, float) else f"{k}={v}" for k, v in values.items() if k != "kind")


class TemplateWorkbench:
    """Per-species workbench — templates, masks and the viewer of one species. Mounted
    by the registry's Templates & masks tab (`ui/species/templates_tab.py`); the
    species' identity (name, color, Ø, symmetry, notes) and its delete cascade live on
    the Species tab."""

    def __init__(self, backend, project_path: str, species_id: str):
        self.backend = backend
        self.project_path = project_path
        self.species_id = species_id
        self.output_folder = os.path.join(project_path, "templates", species_id)
        os.makedirs(self.output_folder, exist_ok=True)

        # Scope iframe + event name per species so multiple workbench tabs
        # don't cross-talk: with a shared id, document.getElementById hits the
        # first iframe only, load_volume posts go to the wrong iframe, and the
        # postMessage listeners fire into the wrong workbench's Activity Log.
        # `species_id` is already a slug; safe in a DOM id and event name.
        self._iframe_id = f"molstar-frame-{species_id}"
        self._molstar_event_name = f"molstar_event_{species_id}"

        self.project_raw_apix: float | None = None
        self.project_tomo_apix: float | None = None

        # Persisted on species.workbench_ui
        self.auto_box: bool = True
        self.basic_shape_def: str = "550:550:550"

        # Per-flow generation form state (not persisted)
        self.shape_pixel_size: float = _UNSET_APIX
        self.shape_box_size: int = 96
        self.shape_lowpass: float | None = None

        self.pdb_pixel_size: float = _UNSET_APIX
        self.pdb_box_size: int = 96
        self.pdb_lowpass: float | None = None
        self.pdb_input_val: str = ""

        self.emdb_pixel_size: float = _UNSET_APIX
        self.emdb_box_size: int = 96
        self.emdb_lowpass: float | None = None
        self.emdb_input_val: str = ""
        self.emdb_scale_xy: float = 1.0
        self.emdb_scale_z: float = 1.0

        # Edit-current action forms
        self.resample_target_apix: float = _UNSET_APIX
        self.resample_target_box: int = 96
        self.resample_lowpass: float | None = None
        self.resample_scale_xy: float = 1.0
        self.resample_scale_z: float = 1.0
        self.lowpass_target: float = 30.0

        # Mask form
        self.mask_threshold: float = 0.5
        self.mask_extend: float = species_edit.MASK_EXTEND
        self.mask_soft_edge: float = species_edit.MASK_SOFT_EDGE
        self.mask_lowpass: float = species_edit.MASK_LOWPASS
        self.threshold_method: str = "flexible_bounds"
        self.masking_active: bool = False

        # Spherical-mask form. Diameter defaulted from species.diameter_ang
        # at render time (UI can still override). Soft edge in pixels.
        self.sphere_diameter_ang: float | None = None
        self.sphere_soft_edge: float = species_edit.MASK_SOFT_EDGE

        # Viewer state
        self.viewer_mode: str = "molstar"
        self.viewer_ready: bool = False
        self.loaded_items: list = []
        self.session_item_containers: dict = {}

        # Bridge robustness state:
        #   _pending_commands: posts buffered until iframe emits `ready`.
        #   _optimistic_visibility: client-side override per itemId; the
        #     icon flips immediately on click and gets reconciled on the
        #     next itemsChanged.
        #   _inflight_timestamps: per-(itemId, action) last send time;
        #     a duplicate within DEDUP_WINDOW_S is dropped — guards
        #     molstar's internal state against double-click spam.
        self._pending_commands: list[dict] = []
        self._optimistic_visibility: dict[str, bool] = {}
        self._inflight_timestamps: dict[tuple[str, str], float] = {}

        # Load spinners:
        #   _pending_loads: optimistic spinner entries for in-flight
        #     load_volume requests. FIFO-correlated with itemsChanged
        #     growth — we can't track per-item since molstar generates
        #     IDs, so we just pop the oldest 'loading' when count rises.
        #     Entries flip to 'error' after PENDING_ERROR_S without
        #     reconcile.
        self._pending_loads: list[dict] = []
        self._pending_load_seq: int = 0
        self._pending_loads_container: ui.element | None = None

        # Dialog openers reachable from INSIDE the lists (the by-path import rows): an
        # import moves list membership, which re-renders that list, so the row the click
        # came from is destroyed under it — without this a user's second click opens a
        # second dialog.
        self._flight = SingleFlight()

        # UI refs
        self._templates_table: ui.element | None = None
        self._masks_table: ui.element | None = None
        self._templates_count: ui.label | None = None
        self._masks_count: ui.label | None = None
        self._template_dialog: ui.dialog | None = None
        self._mask_dialog: ui.dialog | None = None
        # entry id -> row element, kept by the render so a selection change is a class
        # flip rather than a rebuild of the list the user just clicked.
        self._template_rows: dict[str, ui.element] = {}
        self._mask_rows: dict[str, ui.element] = {}
        self._edit_container: ui.element | None = None
        self._mask_source_label = None
        self._molstar_panel: ui.element | None = None
        self._molstar_canvas: ui.element | None = None
        self._iframe_mounted = False
        self._viewer_empty: ui.label | None = None
        self._slice_panel: ui.element | None = None
        self._slice_controller: TemplateViewerController | None = None
        self._session_list_container: ui.element | None = None
        self._log_container: ui.element | None = None
        self._log_meta: ui.label | None = None
        self._log_n = 0
        self.client = None

        self._load_project_parameters()
        self._render()
        self.client = context.client

        ui.timer(1.0, self._post_initial_items, once=True)
        ui.timer(5.0, self._prune_pending_loads)

    # ==================================================================
    # SPECIES / STATE ACCESS
    # ==================================================================

    def _get_species(self) -> ParticleSpecies | None:
        state = get_project_state_for(Path(self.project_path))
        return state.get_species(self.species_id)

    def _state(self):
        return get_project_state_for(Path(self.project_path))

    def _mutate_species(self, fn) -> None:
        # Model-level mutation: marks dirty + bumps registry_rev so the roster /
        # journey / workbench-strip gates see the edit.
        get_project_state_for(Path(self.project_path)).mutate_species(self.species_id, fn)

    async def _save_state(self) -> None:
        await self.backend.save_project(Path(self.project_path))

    def _save_workbench_ui(self) -> None:
        def _apply(sp: ParticleSpecies) -> None:
            sp.workbench_ui.auto_box = self.auto_box
            sp.workbench_ui.basic_shape_def = self.basic_shape_def

        self._mutate_species(_apply)
        asyncio.create_task(self._save_state())

    def _load_project_parameters(self) -> None:
        state = get_project_state_for(Path(self.project_path))
        raw = state.microscope.pixel_size_angstrom if state.microscope else None
        if raw and raw > 0:
            self.project_raw_apix = float(raw)

        sp = state.get_species(self.species_id)
        if sp is not None:
            wb_ui = getattr(sp, "workbench_ui", None)
            if wb_ui is not None:
                self.auto_box = bool(getattr(wb_ui, "auto_box", True))
                bsd = getattr(wb_ui, "basic_shape_def", None)
                if bsd:
                    self.basic_shape_def = bsd

            sel_mask = sp.get_selected_mask()
            if sel_mask is not None:
                if sel_mask.threshold is not None:
                    self.mask_threshold = float(sel_mask.threshold)
                if sel_mask.extend_pixels is not None:
                    self.mask_extend = float(sel_mask.extend_pixels)
                if sel_mask.soft_edge_pixels is not None:
                    self.mask_soft_edge = float(sel_mask.soft_edge_pixels)
                if sel_mask.lowpass_ang is not None:
                    self.mask_lowpass = float(sel_mask.lowpass_ang)
        asyncio.create_task(self._load_default_pixel_size())

    async def _load_default_pixel_size(self) -> None:
        """Offer the tomograms' pixel size in the generators' forms (`species_edit.template_pixel_size`), else the
        movies', else 10 Å. A form the user has typed in keeps its value."""
        found, _why = await species_edit.template_pixel_size(self._state())
        offered = found or self.project_raw_apix or _UNSET_APIX
        for attr in ("shape_pixel_size", "pdb_pixel_size", "emdb_pixel_size", "resample_target_apix"):
            if getattr(self, attr) == _UNSET_APIX:
                setattr(self, attr, offered)

    # ==================================================================
    # WRITE-THROUGH (append, dedup-aware)
    # ==================================================================

    def _append_template(
        self,
        template_path: str,
        polarity: str,
        source: str,
        *,
        lowpass: float | None = None,
        imported_from: str | None = None,
        notes: str = "",
    ) -> str:
        """Append a ParticleTemplate (or replace in-place if a registered
        entry already exists at this path). Auto-selects only when this
        is the first entry. Returns the entry's UUID."""
        res = species_admin.register_template(
            self._state(),
            self.species_id,
            template_path,
            polarity=polarity,
            source=source,
            lowpass=lowpass,
            imported_from=imported_from,
            notes=notes,
        )
        asyncio.create_task(self._after_register())
        return res.get("template_id", "")

    def _append_mask(self, mask: TemplateMask) -> str:
        res = species_admin.register_mask(self._state(), self.species_id, mask)
        asyncio.create_task(self._after_register())
        return res.get("mask_id", "")

    async def _after_register(self) -> None:
        await self._save_state()
        self._refresh_after_change()

    def _select_template(self, template_id: str) -> None:
        if not species_admin.select_template(self._state(), self.species_id, template_id)["success"]:
            return
        # The click that selects a row must not destroy that row: flip the class across
        # the kept handles and refresh only what depends on the selection.
        _flip_selection(self._template_rows, template_id)
        asyncio.create_task(self._after_select())

    def _select_mask(self, mask_id: str) -> None:
        if not species_admin.select_mask(self._state(), self.species_id, mask_id)["success"]:
            return
        _flip_selection(self._mask_rows, mask_id)
        asyncio.create_task(self._after_select())

    async def _after_select(self) -> None:
        """Saves, then repaints the surfaces that read the selection — the Edit Current
        form and the masks column's derived-from line. The two lists are untouched."""
        await self._save_state()
        self._refresh_edit_current()
        self._update_mask_source_label()
        self._refresh_viewer()

    def _refresh_after_change(self) -> None:
        """After a register / delete. A list is rebuilt only when its membership changed;
        a selection change alone goes through `_select_*`, which flips a class."""
        sp = self._get_species()
        if self._templates_table is not None and _membership_moved(self._template_rows, sp, "templates"):
            self._templates_table.clear()
            with self._templates_table:
                self._render_template_rows()
        if self._masks_table is not None and _membership_moved(self._mask_rows, sp, "masks"):
            self._masks_table.clear()
            with self._masks_table:
                self._render_mask_rows()
        self._refresh_edit_current()
        self._update_mask_source_label()
        self._refresh_viewer()

    def _refresh_edit_current(self) -> None:
        if self._edit_container is None:
            return
        self._edit_container.clear()
        with self._edit_container:
            self._render_edit_current_form()

    # ==================================================================
    # DELETE (with confirmation + on-disk removal)
    # ==================================================================

    def _request_delete_template(self, template_id: str) -> None:
        sp = self._get_species()
        tpl = sp.get_template_by_id(template_id) if sp else None
        if tpl is None:
            return
        self._open_delete_confirmation(
            kind="template", file_path=tpl.template_path, on_confirm=lambda: self._do_delete_template(template_id)
        )

    def _request_delete_mask(self, mask_id: str) -> None:
        sp = self._get_species()
        mask = sp.get_mask_by_id(mask_id) if sp else None
        if mask is None:
            return
        self._open_delete_confirmation(
            kind="mask", file_path=mask.mask_path, on_confirm=lambda: self._do_delete_mask(mask_id)
        )

    def _open_delete_confirmation(self, *, kind: str, file_path: str, on_confirm) -> None:
        fname = os.path.basename(file_path)
        with ui.dialog() as dialog, ui.card().classes("p-4 gap-2"):
            ui.label(f"Delete this {kind}?").classes(_TITLE_CLS)
            ui.label(fname).classes(_MONO_CLS)
            ui.label("Removes the file from disk (and its sidecar) and unregisters it from the species.").classes(
                _HINT_CLS
            )
            with ui.row().classes("w-full justify-end gap-2 mt-2"):
                house_button("Cancel", dialog.close)

                def _confirm():
                    dialog.close()
                    on_confirm()

                house_button("Delete", _confirm, kind="danger")
        dialog.open()

    def _do_delete_template(self, template_id: str) -> None:
        self._after_delete(species_admin.delete_template(self._state(), self.species_id, template_id), "template")

    def _do_delete_mask(self, mask_id: str) -> None:
        self._after_delete(species_admin.delete_mask(self._state(), self.species_id, mask_id), "mask")

    def _after_delete(self, res: dict, kind: str) -> None:
        if not res["success"]:
            ui.notify(res["error"], type="warning")
            return
        if res["problem"] is not None:
            ui.notify(res["problem"], type="warning")
        asyncio.create_task(self._after_register())
        self._log(f"Deleted {kind}: {os.path.basename(res['path'])}")

    # ==================================================================
    # MOLSTAR BRIDGE
    #
    # The iframe is a black box: postMessage out, events back. Three
    # robustness layers sit between the UI and the raw bridge:
    #
    #   1. READY GATE — commands posted before the iframe emits `ready`
    #      get queued and replayed on `ready`; otherwise they are dropped
    #      silently and session state looks stuck.
    #   2. IN-FLIGHT DEDUP — per-(itemId, action) timestamp; identical
    #      sends within DEDUP_WINDOW_S are dropped. Double-click spam on
    #      visibility corrupts molstar's internal state.
    #   3. OPTIMISTIC UI — visibility toggles flip the icon immediately
    #      and reconcile against itemsChanged. The user sees an instant
    #      response even when the iframe is mid-load on a big volume.
    #
    # `_hard_reset_viewer` is the escape hatch: it remounts the iframe
    # entirely. Used when the iframe's internal queue jams up.
    # ==================================================================

    DEDUP_WINDOW_S = 0.2
    _DEDUP_ACTIONS = frozenset({"setVisibility", "setColor", "setIsoValue", "deleteItem"})
    # Files above this on disk are likely to choke molstar (a ~345 MB
    # volume at 1.55 Å/px stalls it). The row's size carries an amber mark
    # from half this threshold up, as a pre-load warning.
    MOLSTAR_WARN_MB = 300
    # A pending load_volume that hasn't been reconciled by itemsChanged
    # within PENDING_ERROR_S is flipped to error; pruned PENDING_PRUNE_S
    # later if the user doesn't dismiss it.
    PENDING_ERROR_S = 60.0
    PENDING_PRUNE_S = 90.0

    def _send_to_iframe(self, payload: dict) -> None:
        if not self.client:
            return
        try:
            self.client.run_javascript(
                f"const f=document.getElementById({json.dumps(self._iframe_id)});"
                f"if(f&&f.contentWindow)f.contentWindow.postMessage({json.dumps(payload)},'*');"
            )
        except Exception as e:
            logger.warning("post_to_viewer failed: %s", e)

    def _post_to_viewer(self, action: str, **kwargs) -> bool:
        """Send (or queue) a message to the molstar iframe.

        Returns True if the post was accepted (sent or queued), False if
        the dedup window dropped it. Callers that maintain optimistic
        state (e.g. visibility toggle) should only update on True so the
        UI never diverges from the iframe."""
        payload = {"action": action, **kwargs}

        iid = kwargs.get("itemId")
        if iid is not None and action in self._DEDUP_ACTIONS:
            now = time.monotonic()
            key = (str(iid), action)
            if now - self._inflight_timestamps.get(key, 0.0) < self.DEDUP_WINDOW_S:
                logger.debug("Dedup: dropping %s for %s", action, iid)
                return False
            self._inflight_timestamps[key] = now

        # `getItems` is a sync request used during init bootstrap and is
        # idempotent — don't gate it on `ready`. Everything else queues
        # until the iframe is ready to receive.
        if not self.viewer_ready and action != "getItems":
            self._pending_commands.append(payload)
            return True
        self._send_to_iframe(payload)
        return True

    def _handle_viewer_event(self, e) -> None:
        try:
            event_type = e.args.get("type")
        except Exception:
            return
        if event_type == "ready":
            self.viewer_ready = True
            # Flush queued posts in arrival order before requesting sync.
            queued, self._pending_commands = self._pending_commands, []
            for payload in queued:
                self._send_to_iframe(payload)
            self._post_to_viewer("getItems")
        elif event_type == "itemsChanged":
            prev_count = len(self.loaded_items)
            items = e.args.get("items", [])
            self.loaded_items = items
            # FIFO-reconcile pending loads: on count growth, pop the
            # oldest 'loading' entries up to the delta. We can't track
            # per-item because molstar generates its own ids, so this
            # assumes new items correspond to recent load_volume posts.
            delta = len(items) - prev_count
            if delta > 0 and self._pending_loads:
                popped = 0
                for entry in list(self._pending_loads):
                    if popped >= delta:
                        break
                    if entry.get("status") == "loading":
                        self._pending_loads.remove(entry)
                        popped += 1
                if popped:
                    self._render_pending_loads()
            # Reconcile optimistic visibility: drop entries that now match
            # ground truth so subsequent renders fall back to the real state.
            if self._optimistic_visibility:
                ground = {it.get("id"): it.get("visible", True) for it in items}
                for iid in list(self._optimistic_visibility.keys()):
                    if iid in ground and ground[iid] == self._optimistic_visibility[iid]:
                        del self._optimistic_visibility[iid]
                    elif iid not in ground:
                        # Item disappeared (deleted / cleared) — drop override.
                        del self._optimistic_visibility[iid]
            self._update_session_tray()
        elif event_type == "error":
            msg = e.args.get("message", "")
            action = e.args.get("action") or ""
            iid = e.args.get("itemId") or ""
            ctx = " · ".join(f"{k}={v}" for k, v in (("action", action), ("item", iid)) if v)
            self._log(f"Viewer error: {msg}{f' ({ctx})' if ctx else ''}")
        elif event_type == "loadProgress":
            # Per-stage progress from AlignmentViewer.loadLocalVolume.
            # download → parse → stats → represent → done. Logged so we
            # can see which step silently fails on tricky volumes.
            iid = e.args.get("itemId", "?")
            stage = e.args.get("stage", "?")
            detail = e.args.get("detail")
            self._log(f"[viewer] {iid}: {stage}{f' ({detail})' if detail else ''}")
        elif event_type == "volumeStats":
            # Raw molstar-side stats + the inversion heuristic outcome.
            # Reveals when the heuristic mis-detects black-polarity
            # templates or when the parser falls back to default
            # {-1, 1, 0, 0.1} stats.
            iid = e.args.get("itemId", "?")
            stats = e.args.get("stats", {}) or {}
            inv = e.args.get("isInverted", False)
            self._log(
                f"[viewer] {iid}: stats min={stats.get('min', 'n/a')} "
                f"max={stats.get('max', 'n/a')} mean={stats.get('mean', 'n/a')} "
                f"σ={stats.get('sigma', 'n/a')} inverted={inv}"
            )

    async def _post_initial_items(self) -> None:
        self._post_to_viewer("getItems")

    def _sweep_viewer(self) -> None:
        """Soft clear: ask molstar to drop all items and discard any
        pending-load spinners (treat them as user-aborted). Bridge state
        is preserved — use _hard_reset_viewer for the nuclear option."""
        self._post_to_viewer("clear")
        if self._pending_loads:
            self._pending_loads.clear()
            self._render_pending_loads()

    def _hard_reset_viewer(self) -> None:
        """Nuclear reset: remount the iframe entirely.

        molstar's internal load queue can jam on a partially-loaded large
        volume; a soft `clear` then never gets processed. Remounting the
        iframe is the equivalent of a page reload without re-rendering the
        rest of the workbench. Clears all bridge state so a fresh `ready`
        flushes against an empty queue."""
        self.viewer_ready = False
        self.loaded_items = []
        self._pending_commands.clear()
        self._optimistic_visibility.clear()
        self._inflight_timestamps.clear()
        self._pending_loads.clear()
        self._render_pending_loads()
        self._update_session_tray()
        if self.client:
            try:
                self.client.run_javascript(
                    f"const f=document.getElementById({json.dumps(self._iframe_id)});"
                    "if(f){const s=f.src; f.src='about:blank'; setTimeout(()=>{f.src=s;}, 50);}"
                )
            except Exception as e:
                logger.warning("iframe remount failed: %s", e)
        self._log("Viewer reset")

    def _load_to_viewer(self, file_path: str, *, polarity: str | None = None, kind: str = "template") -> None:
        """Explicit user-requested load. Triggered by the eye icon on
        template / mask cards.

        `polarity` ('white'/'black') and `kind` ('template'/'mask') are
        plumbed to molstar so it can pick the right iso interpretation:
        - black templates need negative iso (signal lives at -|iso|).
        - masks are binary 0/1 with soft edges; absolute iso=0.5 is the
          natural boundary, relative iso lands beyond max=1 and renders
          nothing."""
        if not file_path or not os.path.exists(file_path):
            ui.notify("File missing on disk", type="warning")
            return
        if self.viewer_mode == "molstar":
            self._ensure_molstar()
            self._pending_load_seq += 1
            self._pending_loads.append(
                {
                    "seq": self._pending_load_seq,
                    "filename": os.path.basename(file_path),
                    "started_at": time.monotonic(),
                    "status": "loading",
                }
            )
            self._render_pending_loads()
            extra: dict = {"kind": kind}
            if polarity in ("white", "black"):
                extra["polarity"] = polarity
            self._post_to_viewer("load_volume", url=f"/api/file?path={file_path}", **extra)
            self._log(f"Loading into viewer: {os.path.basename(file_path)}")
        elif self._slice_controller is not None:
            # Slice fallback: replace the displayed volume with this one.
            try:
                self._slice_controller.update_paths(file_path, None)
            except Exception as e:
                logger.warning("slice viewer update_paths failed: %s", e)

    def _refresh_viewer(self) -> None:
        """Called after register/select. Molstar mode does not auto-load
        selected entries (the user controls what's in the viewer via the
        per-row eye icon), so this only updates the slice fallback."""
        if self.viewer_mode == "slice" and self._slice_controller is not None:
            # Slice viewer is Python-side and cheap; reflect current
            # selection there for the fallback case.
            sp = self._get_species()
            sel_tpl = sp.get_selected_template() if sp else None
            sel_mask = sp.get_selected_mask() if sp else None
            tpath = sel_tpl.template_path if sel_tpl else ""
            mpath = sel_mask.mask_path if sel_mask else None
            try:
                self._slice_controller.update_paths(tpath, mpath)
            except Exception as e:
                logger.warning("slice viewer update_paths failed: %s", e)

    def _update_session_tray(self) -> None:
        self._apply_viewer_mode_visibility()
        if not self._session_list_container:
            return
        current_ids = {item.get("id") for item in self.loaded_items}
        for iid in list(self.session_item_containers.keys()):
            if iid not in current_ids:
                refs = self.session_item_containers[iid]
                self._session_list_container.remove(refs["container"])
                del self.session_item_containers[iid]
        for item in self.loaded_items:
            iid = item.get("id", "unknown")
            if iid in self.session_item_containers:
                self._update_session_item(iid, item)
            else:
                self._create_session_item(item)

    def _create_session_item(self, item) -> None:
        iid = item.get("id", "unknown")
        item_type = item.get("type", "unknown")
        visible = item.get("visible", True)
        color = item.get("color", 0xCCCCCC)
        color_hex = f"#{color:06x}" if isinstance(color, int) else "#CCCCCC"

        with self._session_list_container:
            # Flat hover-highlighted row, not a nested card: per-item borders add
            # gray-line noise to the tray.
            container = ui.column().classes("w-full gap-0 px-1 rounded hover:bg-slate-100")
            with container:
                with ui.row().classes("items-center gap-1 no-wrap w-full").style("height: 22px;"):
                    color_btn = ui.button(icon="circle").props("flat round dense size=xs").style(f"color: {color_hex}")
                    with ui.menu().props("auto-close") as color_menu:
                        with ui.grid(columns=6).classes("gap-0.5 p-1"):
                            for pc in SPECIES_OVERLAY_COLORS:
                                ui.button().props("flat dense").style(
                                    f"background:{pc};width:16px;height:16px;min-width:16px;"
                                ).on("click", lambda c=pc, i=iid: self._set_item_color(i, c))
                    color_btn.on("click", color_menu.open)
                    ui.label(iid).classes(_MONO_CLS + " max-w-[120px] truncate")
                    vis_btn = icon_button(
                        "visibility" if visible else "visibility_off",
                        lambda i=iid: self._toggle_visibility_from_ui(i),
                        tooltip="Show or hide in the viewer",
                    )
                    icon_button(
                        "close",
                        lambda i=iid: self._post_to_viewer("deleteItem", itemId=i),
                        tooltip="Remove from the viewer",
                        kind="danger",
                    )

                iso_slider, iso_label = None, None
                if item_type == "map":
                    iso_value = item.get("isoValue", 1.5)
                    is_inv = item.get("isInverted", False)
                    with ui.row().classes("items-center gap-1 mt-0.5"):
                        ui.label("ISO").classes(_HINT_CLS + " shrink-0")
                        iso_slider = ui.slider(min=0.5, max=5.0, step=0.1, value=abs(iso_value)).classes("w-20")
                        iso_slider.on(
                            "change",
                            lambda e, i=iid, inv=is_inv: self._post_to_viewer(
                                "setIsoValue", itemId=i, isoValue=-abs(e.args) if inv else e.args
                            ),
                        )
                        iso_label = ui.label(f"{iso_value:.1f}σ").classes(_MONO_CLS)

        self.session_item_containers[iid] = {
            "container": container,
            "vis_btn": vis_btn,
            "color_btn": color_btn,
            "iso_label": iso_label,
            "iso_slider": iso_slider,
        }

    def _set_item_color(self, iid: str, color: str) -> None:
        """One of the overlay palette's colours on a loaded item; molstar takes it as an int."""
        self._post_to_viewer("setColor", itemId=iid, color=int(color.lstrip("#"), 16))

    def _update_session_item(self, iid: str, item) -> None:
        refs = self.session_item_containers.get(iid)
        if not refs:
            return
        # Optimistic override wins so the icon doesn't snap back to a
        # stale ground-truth `visible` while a setVisibility is in flight.
        visible = self._optimistic_visibility.get(iid, item.get("visible", True))
        color = item.get("color", 0xCCCCCC)
        refs["vis_btn"].props(f"icon={'visibility' if visible else 'visibility_off'}")
        refs["color_btn"].style(f"color: #{color:06x}")
        if item.get("type") == "map" and refs.get("iso_label"):
            iso_value = item.get("isoValue", 1.5)
            refs["iso_label"].set_text(f"{iso_value:.1f}σ")
            if refs.get("iso_slider"):
                refs["iso_slider"].value = abs(iso_value)

    def _toggle_visibility_from_ui(self, iid: str) -> None:
        item = next((i for i in self.loaded_items if i.get("id") == iid), None)
        if item is None:
            return
        # Optimistic: prefer any pending override over ground truth so
        # rapid clicks toggle off the user's last requested state, not
        # the iframe's stale `visible`.
        cur = self._optimistic_visibility.get(iid, item.get("visible", True))
        new_val = not cur
        # Only flip the icon if the post is actually going through.
        # A dedup-dropped click leaves the icon at the prior state so
        # the UI never claims a state the iframe doesn't know about.
        if not self._post_to_viewer("setVisibility", itemId=iid, visible=new_val):
            return
        self._optimistic_visibility[iid] = new_val
        refs = self.session_item_containers.get(iid)
        if refs and refs.get("vis_btn"):
            refs["vis_btn"].props(f"icon={'visibility' if new_val else 'visibility_off'}")

    def _render_pending_loads(self) -> None:
        """Draw the optimistic load spinners above the session list.
        Called whenever the pending list changes — clears and re-renders
        the small container so reorderings stay coherent with the model."""
        self._apply_viewer_mode_visibility()
        if self._pending_loads_container is None:
            return
        self._pending_loads_container.clear()
        with self._pending_loads_container:
            for entry in self._pending_loads:
                status = entry.get("status", "loading")
                # Inline row, matching the flat session items — no card chrome.
                with ui.row().classes("items-center gap-1 w-full min-w-0 px-1"):
                    if status == "loading":
                        ui.spinner("dots", size="xs").classes("text-slate-500 shrink-0")
                    else:
                        ui.icon("error_outline", size="12px").classes("text-red-600 shrink-0")
                    ui.label(entry["filename"]).classes(_MONO_CLS + " truncate flex-1 min-w-0")
                    if status == "error":
                        icon_button(
                            "close", lambda _e, seq=entry["seq"]: self._dismiss_pending_load(seq), tooltip="Dismiss"
                        )

    def _dismiss_pending_load(self, seq: int) -> None:
        self._pending_loads = [e for e in self._pending_loads if e.get("seq") != seq]
        self._render_pending_loads()

    def _prune_pending_loads(self) -> None:
        """Timer-driven: flip stale 'loading' entries to 'error', then
        evict 'error' entries the user never dismissed. Both thresholds
        are wallclock-relative to `started_at`."""
        now = time.monotonic()
        changed = False
        for entry in list(self._pending_loads):
            age = now - entry.get("started_at", now)
            if entry.get("status") == "loading" and age > self.PENDING_ERROR_S:
                entry["status"] = "error"
                changed = True
            elif entry.get("status") == "error" and age > self.PENDING_PRUNE_S:
                self._pending_loads.remove(entry)
                changed = True
        if changed:
            self._render_pending_loads()

    # ==================================================================
    # RENDER
    # ==================================================================

    def _render(self) -> None:
        # The tables over each other, the viewer beside them in a wide pane and under them in a narrow one
        # (`.cb-tm-split`, a container query on the registry's detail column in ui/dashboard/css.py), the log
        # folded under everything. The creation forms live in the two dialogs, built once here so their
        # handles (the Edit current form, the masks' derived-from line) stay valid.
        with ui.column().classes("w-full").style("gap: 16px; padding: 12px 14px 18px;"):
            with ui.element("div").classes("cb-tm-split"):
                with ui.element("div").style("display: flex; flex-direction: column; gap: 18px; min-width: 0;"):
                    self._render_templates_section()
                    self._render_masks_section()
                with ui.element("div").style("display: flex; flex-direction: column; min-width: 0;"):
                    self._render_viewer_panel()
            self._render_log_panel()
        self._template_dialog = self._build_template_dialog()
        self._mask_dialog = self._build_mask_dialog()

        # Per-species event channel: filter messages by THIS iframe's
        # contentWindow and route them to a species-scoped event name so
        # other workbench tabs don't react to our events.
        ui.on(self._molstar_event_name, self._handle_viewer_event)
        iframe_id_js = json.dumps(self._iframe_id)
        event_name_js = json.dumps(self._molstar_event_name)
        ui.run_javascript(
            f"""
            window.addEventListener('message', function(event) {{
                const iframe = document.getElementById({iframe_id_js});
                if (iframe && event.source === iframe.contentWindow && event.data && event.data.type) {{
                    emitEvent({event_name_js}, event.data);
                }}
            }});
            """
        )

    # ------------------------------------------------------------------
    # 2. TABLES — templates over masks, each as tall as its rows
    # ------------------------------------------------------------------

    def _render_templates_section(self) -> None:
        with ui.column().style(SECTION_STYLE):
            head = section_head(
                "Templates",
                right=" ",
                tooltip="The template template matching uses is the filled dot; a click on a row selects it",
            )
            self._templates_count = head.meta_label
            with head:
                house_button("New template…", lambda: self._open_dialog(self._template_dialog))
            self._templates_table = ui.element("div").classes("cb-tw-table").style(f"--cb-tw-cols: {_TW_COLS};")
            with self._templates_table:
                self._render_template_rows()

    def _render_template_rows(self) -> None:
        self._template_rows = {}
        sp = self._get_species()
        if sp is None:
            return
        self._render_table_head(("", "file", "polarity", "header", "stats", "size", ""))
        templates = list(sp.templates)
        if not templates:
            self._render_empty_state(
                "No template yet: New template… makes one from a shape, a PDB or EMDB entry, or a file."
            )
        else:
            sid = sp.selected_template_id
            for t in templates:
                self._render_template_row(t, selected=(t.id == sid))
        self._update_counts()

    def _render_template_row(self, tpl: ParticleTemplate, *, selected: bool) -> None:
        h = read_template_header(tpl.template_path)
        row = ui.element("div").classes("cb-tw-row" + (" cb-sel" if selected else ""))
        row.on("click", lambda i=tpl.id: self._select_template(i))
        self._template_rows[tpl.id] = row
        with row:
            self._render_select_cell("template")
            self._render_file_cell(tpl.template_path, tpl.source or tpl.imported_from)
            with ui.element("div").classes("cb-tw-cell"):
                self._polarity_chip(tpl.polarity)
            with ui.element("div").classes("cb-tw-cell"):
                ui.label(self._format_header_summary(h, tpl.lowpass_resolution_ang)).style(
                    f"{_ROW_MONO} color: {_BODY};"
                )
            with ui.element("div").classes("cb-tw-cell"):
                self._render_stats_row(h)
            with ui.element("div").classes("cb-tw-cell").style("justify-content: flex-end;"):
                self._render_size_badge(tpl.template_path)
            self._render_row_actions(
                on_load_to_viewer=lambda p=tpl.template_path, pol=tpl.polarity: self._load_to_viewer(
                    p, polarity=pol, kind="template"
                ),
                on_delete=lambda i=tpl.id: self._request_delete_template(i),
            )

    # ── Table pieces shared by both tables ───────────────────────────

    def _render_table_head(self, labels: tuple[str, ...]) -> None:
        """Rendered even when the table is empty, so the columns' shapes are legible
        before anything exists."""
        with ui.element("div").classes("cb-tw-head"):
            for i, text in enumerate(labels):
                ui.label(text).style(_TH + (" text-align: right;" if i == 5 else ""))

    def _render_select_cell(self, what: str) -> None:
        """The in-use dot: both are rendered and the row's `cb-sel` class decides which shows (CSS in
        `ui/dashboard/css.py`), so selecting costs a class flip, not a re-render."""
        with ui.element("div").classes("cb-tw-cell"):
            ui.element("span").classes("cb-tw-dot-on").props["title"] = f"The {what} template matching uses"
            ui.element("span").classes("cb-tw-dot-off").props["title"] = f"Use this {what}"

    def _render_file_cell(self, path: str, note: str | None) -> None:
        """The file cut in its middle (hover unrolls it, a click copies it), its provenance on hover."""
        with ui.element("div").classes("cb-tw-cell"):
            p = path_text(path, self.project_path).style("flex: 1 1 0; min-width: 0;")
            if note:
                p.tooltip(note)

    def _render_row_actions(self, *, on_load_to_viewer, on_delete) -> None:
        """Eye (load into the viewer) + delete ×. `click.stop` so an action never selects the
        row as a side effect."""
        with ui.element("div").classes("cb-tw-cell cb-tw-acts").style("justify-content: flex-end;"):
            icon_button("visibility", tooltip="Load into the viewer").on("click.stop", lambda _e: on_load_to_viewer())
            icon_button("close", tooltip="Delete", kind="danger").on("click.stop", lambda _e: on_delete())

    def _update_counts(self) -> None:
        sp = self._get_species()
        for label, n in (
            (self._templates_count, len(sp.templates) if sp else 0),
            (self._masks_count, len(sp.masks) if sp else 0),
        ):
            if label is not None and label.text != str(n):
                label.set_text(str(n))

    def _stats_cell(self, h) -> tuple[str, str, str] | None:
        """(text, mark, hover) for a volume's statistics: σ alone in the cell, min and max on the
        hover. `mark` flags the normalization state: "" (σ about 1, post-normalize), "warn" (σ far
        from 1: an un-normalized RELION class, say), "bad" (min = max: a constant volume, likely an
        empty mask or a failed run). None when the header carries no statistics."""
        if h.dmin is None or h.dmax is None:
            return None
        std = h.rms or 0.0
        hover = f"min {h.dmin:.3g} · max {h.dmax:.3g} · σ {std:.3g}"
        if h.dmin == h.dmax:
            return "constant", "bad", f"{hover}. A constant volume: likely an empty mask or an unsuccessful TM run"
        if std > 0 and (std < 0.1 or std > 5.0):
            return f"σ {std:.2g}", "warn", f"{hover}. Unusual σ: the values may not be σ-normalized"
        return f"σ {std:.2g}", "", hover

    def _render_stats_row(self, h) -> None:
        res = self._stats_cell(h)
        if res is None:
            ui.label("—").style(f"{_ROW_MONO} color: {_SOFT};").tooltip("The header carries no statistics")
            return
        text, mark, hover = res
        with ui.element("span").style(
            f"display: inline-flex; align-items: center; {_ROW_MONO} color: {_BODY if mark else _SOFT};"
        ) as cell:
            if mark:
                _mark_dot(mark)
            ui.label(text)
        cell.tooltip(hover)

    def _format_header_summary(self, h, lowpass: float | None) -> str:
        parts: list[str] = []
        if h.apix_ang:
            parts.append(f"{h.apix_ang:.3g} Å/px")
        if h.box_px:
            parts.append(f"{h.box_px} px")
        if lowpass:
            parts.append(f"lp {lowpass:g} Å")
        return " · ".join(parts) if parts else "header unreadable"

    def _format_file_size(self, file_path: str) -> tuple[str, bool] | None:
        """(human-readable size, large). Large is from half the warn threshold up: a pre-load
        warning before the user asks molstar for a volume it may stall on."""
        try:
            size_bytes = Path(file_path).stat().st_size
        except OSError:
            return None
        mb = size_bytes / (1024 * 1024)
        if mb >= 1024:
            label = f"{mb / 1024:.1f} GB"
        elif mb >= 1:
            label = f"{mb:.0f} MB"
        else:
            label = f"{size_bytes / 1024:.0f} KB"
        return label, mb >= self.MOLSTAR_WARN_MB / 2

    def _render_size_badge(self, file_path: str) -> None:
        res = self._format_file_size(file_path)
        if res is None:
            ui.label("missing").style(f"{_ROW_MONO} color: #b91c1c;").tooltip("The file is not on disk")
            return
        label, large = res
        with ui.element("span").style(
            f"display: inline-flex; align-items: center; {_ROW_MONO} color: {_SOFT}; flex-shrink: 0;"
        ) as cell:
            if large:
                _mark_dot("warn")
            ui.label(label)
        if large:
            cell.tooltip("Large file: molstar may stall loading it")

    def _polarity_chip(self, polarity: str) -> None:
        render_polarity_chip(polarity)

    def _render_empty_state(self, text: str) -> None:
        ui.label(text).style(f"{_CAPTION} padding: 6px;")

    # ------------------------------------------------------------------
    # 3. THE CREATION FORMS — in the New template… / New mask… dialogs
    # ------------------------------------------------------------------

    def _open_dialog(self, dialog: ui.dialog | None) -> None:
        if dialog is not None:
            dialog.open()

    def _creation_dialog(self, title: str, tabs, actions: dict, run, *, subtitle=None):
        """A dialog of segmented forms, grid-stacked so it keeps the tallest form's height on a switch, over the
        `[Cancel] [action]` footer; the action follows the form shown, and a form whose tools carry their own
        buttons (`actions[key]` None) shows none. `run(key)` returns True when the dialog may close: a refusal
        keeps it open with what was typed. `subtitle` builds a line under the title. Returns the dialog, the
        stacked panels to fill and the action button."""
        state = {"tab": tabs[0][0]}
        with (
            ui.dialog() as dialog,
            ui.card().style(
                "width: 600px; max-width: 92vw; padding: 14px 16px; gap: 10px; border-radius: 3px; "
                "box-shadow: 0 6px 18px rgba(16, 42, 67, .14);"
            ),
        ):
            ui.label(title).style(PANEL_TITLE_STYLE)
            if subtitle is not None:
                subtitle()
            panels: dict[str, ui.element] = {}
            seg = render_segmented(tabs, tabs[0][0], lambda k: _show(k))
            holder = ui.element("div")
            _panel, _stack_show = _stacked_panels(holder, panels)
            for key, _label in tabs:
                _panel(key)
            with ui.element("div").style(
                "display: flex; justify-content: flex-end; gap: 8px; margin-top: 4px; align-self: stretch;"
            ):
                house_button("Cancel", dialog.close)

                async def _act() -> None:
                    if await run(state["tab"]):
                        dialog.close()

                action = house_button(actions[tabs[0][0]] or "", _act, kind="accent")

            def _show(key: str) -> None:
                state["tab"] = key
                seg.set_active(key)
                _stack_show(key)
                label = actions.get(key)
                action.set_visibility(bool(label))
                if label:
                    action.set_text(label)

            _show(tabs[0][0])
        return dialog, panels, action

    def _build_template_dialog(self) -> ui.dialog:
        actions = {
            "shape": "Generate",
            "pdb": "Fetch & simulate",
            "emdb": "Fetch & resample",
            "import": "Choose file…",
            "edit": None,
        }
        dialog, panels, _action = self._creation_dialog(
            f"New template · {self._species_name()}", _SOURCE_TABS, actions, self._run_template_form
        )
        with panels["shape"]:
            self._render_basic_shape_form()
        with panels["pdb"]:
            self._render_pdb_form()
        with panels["emdb"]:
            self._render_emdb_form()
        with panels["import"]:
            ui.label(
                "Choose an .mrc: the header is read, you confirm its metadata, and a copy is registered in the "
                "project as a new template."
            ).classes(_HINT_CLS)
        with panels["edit"]:
            self._edit_container = ui.column().classes("w-full gap-2")
            with self._edit_container:
                self._render_edit_current_form()
        return dialog

    async def _run_template_form(self, key: str) -> bool:
        if key == "import":
            # The import dialog is a dialog of its own: this one closes first, and the next is parented at the
            # page's layout, not inside a dialog that is no longer shown.
            self._template_dialog.close()
            with dialog_host():
                await self._open_import_dialog()
            return False
        run = {"shape": self._gen_shape, "pdb": self._fetch_and_simulate_pdb, "emdb": self._fetch_and_resample_emdb}
        return bool(await run[key]())

    def _build_mask_dialog(self) -> ui.dialog:
        actions = {"relion": "Create mask", "sphere": "Create sphere", "import": "Choose file…"}

        def _source_line() -> None:
            # Which template new masks derive from: state, not a hint, so it follows the selection.
            self._mask_source_label = ui.label("").classes(_HINT_CLS)

        dialog, panels, action = self._creation_dialog(
            f"New mask · {self._species_name()}", _MASK_TABS, actions, self._run_mask_form, subtitle=_source_line
        )
        action.bind_enabled_from(self, "masking_active", backward=lambda x: not x)
        self._update_mask_source_label()
        with panels["relion"]:
            self._render_relion_mask_form()
        with panels["sphere"]:
            self._render_spherical_mask_form()
        with panels["import"]:
            ui.label("Choose an existing mask .mrc; it is registered as an imported mask, as it is.").classes(_HINT_CLS)
        return dialog

    async def _run_mask_form(self, key: str) -> bool:
        if key == "import":
            self._mask_dialog.close()
            with dialog_host():
                await self._import_mask()
            return False
        run = {"relion": self._create_relion_mask, "sphere": self._create_spherical_mask}
        return bool(await run[key]())

    def _species_name(self) -> str:
        sp = self._get_species()
        return sp.name if sp is not None else self.species_id

    def _render_basic_shape_form(self) -> None:
        with ui.row().classes("w-full gap-3 items-center").style("flex-wrap: wrap;"):
            _field(
                "ellipsoid",
                lambda: (
                    ui.input(placeholder="550:550:550")
                    .bind_value(self, "basic_shape_def")
                    .on("update:model-value", self._on_shape_changed)
                ),
                width="w-32",
                hint="Ellipsoid x:y:z in Å",
            )
            _field(
                "apix",
                lambda: ui.number(value=self.shape_pixel_size, step=0.1, min=0).bind_value(self, "shape_pixel_size"),
                hint="Pixel size of the generated volume (Å)",
            )
            _field(
                "box",
                lambda: ui.number(value=self.shape_box_size, step=32, min=32).bind_value(self, "shape_box_size"),
                hint="Box edge in pixels",
            )
            auto = house_check("auto box", value=self.auto_box).bind_value(self, "auto_box")
            auto.style(f"{_SANS} font-size: 10px; color: #475569;")
            auto.on_value_change(self._on_auto_box_toggle)
            _field(
                "lowpass",
                lambda: ui.number(value=self.shape_lowpass, step=5, min=0, placeholder="—").bind_value(
                    self, "shape_lowpass"
                ),
                width="w-24",
                hint="Optional filter applied to the generated shape (Å)",
            )
        ui.label("Writes _white.mrc + _black.mrc; registers both polarities as new entries.").classes(_HINT_CLS)

    def _render_pdb_form(self) -> None:
        with ui.row().classes("w-full gap-3 items-center").style("flex-wrap: wrap;"):
            _field("PDB", lambda: ui.input(placeholder="6Z6J").bind_value(self, "pdb_input_val"), width="w-20")
            _field(
                "apix",
                lambda: ui.number(value=self.pdb_pixel_size, step=0.1, min=0).bind_value(self, "pdb_pixel_size"),
                hint="Pixel size of the simulated map (Å)",
            )
            _field(
                "box",
                lambda: ui.number(value=self.pdb_box_size, step=32, min=32).bind_value(self, "pdb_box_size"),
                hint="Box edge in pixels",
            )
            _field(
                "resolution",
                lambda: ui.number(value=self.pdb_lowpass, step=2, min=0, placeholder="10").bind_value(
                    self, "pdb_lowpass"
                ),
                hint="Simulated map resolution (Å)",
            )
        ui.label("Simulates a density map from the model at this sampling.").classes(_HINT_CLS)

    def _render_emdb_form(self) -> None:
        with ui.row().classes("w-full gap-3 items-center").style("flex-wrap: wrap;"):
            _field("EMDB", lambda: ui.input(placeholder="30210").bind_value(self, "emdb_input_val"), width="w-20")
            _field(
                "apix",
                lambda: ui.number(value=self.emdb_pixel_size, step=0.1, min=0).bind_value(self, "emdb_pixel_size"),
                hint="Resample the fetched map to this pixel size (Å)",
            )
            _field(
                "box",
                lambda: ui.number(value=self.emdb_box_size, step=32, min=32).bind_value(self, "emdb_box_size"),
                hint="Box edge in pixels",
            )
            _field(
                "lowpass",
                lambda: ui.number(value=self.emdb_lowpass, step=5, min=0, placeholder="—").bind_value(
                    self, "emdb_lowpass"
                ),
                width="w-24",
                hint="Optional filter (Å)",
            )
            _scale_fields(self, "emdb_scale_xy", "emdb_scale_z")
        ui.label("Fetches the map from EMDB and resamples it.").classes(_HINT_CLS)

    # ── Edit Current — discrete action sections ──────────────────────

    def _render_edit_current_form(self) -> None:
        """The chip row and all three tool sections always render; with no selection they
        are disabled behind one hint instead of collapsing to a single line, so the dialog
        keeps its height whatever is selected."""
        sp = self._get_species()
        sel = sp.get_selected_template() if sp else None

        # Selected-template chip (read-only); same row shape when nothing is selected.
        with ui.row().classes("w-full items-center gap-2"):
            if sel is not None:
                ui.label("operating on").classes(_HINT_CLS)
                ui.label(os.path.basename(sel.template_path)).classes(_MONO_CLS)
                self._polarity_chip(sel.polarity)
            else:
                ui.label("select a template first — these tools act on the selected row").classes(_HINT_CLS)

        tools = ui.column().classes("w-full gap-1")
        if sel is None:
            tools.classes("opacity-50")
            tools.style("pointer-events: none;")
        with tools:
            self._render_edit_current_tools()

    def _render_edit_current_tools(self) -> None:
        # ── Action: Resample ──
        self._render_action_section(
            title="Resample",
            description="Rewrite at a new pixel size / box. Useful when the template apix doesn't match your tomos.",
            inputs_builder=lambda: self._render_resample_inputs(),
            on_click=self._resample_current,
            button_label="Resample",
        )

        # ── Action: Apply lowpass ──
        self._render_action_section(
            title="Apply lowpass",
            description="Re-filter at a new resolution (Å). Keeps the same apix / box.",
            inputs_builder=lambda: self._render_lowpass_inputs(),
            on_click=self._apply_lowpass_to_current,
            button_label="Apply",
        )

        # ── Action: Flip polarity ──
        self._render_action_section(
            title="Flip polarity",
            description=("Write a negated copy (white → black or vice versa). Skipped if the flipped sibling exists."),
            inputs_builder=None,
            on_click=self._flip_polarity,
            button_label="Flip",
        )

    def _render_action_section(
        self, *, title: str, description: str, inputs_builder, on_click, button_label: str
    ) -> None:
        """One tool: title, hint, inputs row, button. The three tools are separated by
        whitespace and the TOOL-rank header, not by boxes."""
        with ui.column().classes("w-full gap-1 mt-2"):
            with ui.row().classes("w-full items-baseline gap-2"):
                # `_TOOL_CLS`, not a field label: this names a tool that owns the inputs
                # under it, so it must not read at the same rank as "target apix".
                ui.label(title).classes(_TOOL_CLS)
                ui.label(description).classes(_HINT_CLS)
            with ui.row().classes("w-full gap-2 items-center").style("flex-wrap: wrap;"):
                if inputs_builder is not None:
                    inputs_builder()
                house_button(button_label, on_click).classes("ml-auto")

    def _render_resample_inputs(self) -> None:
        _field(
            "target apix",
            lambda: ui.number(value=self.resample_target_apix, step=0.1, min=0).bind_value(
                self, "resample_target_apix"
            ),
            hint="New pixel size in Å",
        )
        _field(
            "target box",
            lambda: ui.number(value=self.resample_target_box, step=32, min=32).bind_value(self, "resample_target_box"),
            hint="New box edge in pixels",
        )
        _field(
            "lowpass",
            lambda: ui.number(value=self.resample_lowpass, step=5, min=0, placeholder="—").bind_value(
                self, "resample_lowpass"
            ),
            hint="Optional lowpass (Å)",
        )
        _scale_fields(self, "resample_scale_xy", "resample_scale_z")

    def _render_lowpass_inputs(self) -> None:
        _field(
            "target lowpass",
            lambda: ui.number(value=self.lowpass_target, step=5, min=0).bind_value(self, "lowpass_target"),
            width="w-24",
            hint="Resolution to re-filter to (Å)",
        )

    # ------------------------------------------------------------------
    # 4. MASKS — the same table
    # ------------------------------------------------------------------

    def _render_masks_section(self) -> None:
        with ui.column().style(SECTION_STYLE):
            head = section_head(
                "Masks",
                right=" ",
                tooltip="The mask template matching uses is the filled dot; a click on a row selects it",
            )
            self._masks_count = head.meta_label
            with head:
                house_button("New mask…", lambda: self._open_dialog(self._mask_dialog))
            self._masks_table = ui.element("div").classes("cb-tw-table").style(f"--cb-tw-cols: {_TW_COLS};")
            with self._masks_table:
                self._render_mask_rows()

    def _render_mask_rows(self) -> None:
        self._mask_rows = {}
        sp = self._get_species()
        if sp is None:
            return
        self._render_table_head(("", "file", "method", "header", "stats", "size", ""))
        masks = list(sp.masks)
        if not masks:
            self._render_empty_state("No mask yet: New mask… makes one from a template, a sphere, or a file.")
        else:
            sid = sp.selected_mask_id
            for m in masks:
                self._render_mask_row(m, selected=(m.id == sid))
        self._update_counts()

    def _render_mask_row(self, mask: TemplateMask, *, selected: bool) -> None:
        h = read_template_header(mask.mask_path)
        row = ui.element("div").classes("cb-tw-row" + (" cb-sel" if selected else ""))
        row.on("click", lambda i=mask.id: self._select_mask(i))
        self._mask_rows[mask.id] = row
        with row:
            self._render_select_cell("mask")
            self._render_file_cell(mask.mask_path, self._mask_note(mask))
            with ui.element("div").classes("cb-tw-cell"):
                # The relion_mask_create knobs are provenance, not a scanning column:
                # they exist for one method only, so they ride the method's hover.
                self._method_chip_with_knobs(mask)
            with ui.element("div").classes("cb-tw-cell"):
                ui.label(self._format_header_summary(h, None)).style(f"{_ROW_MONO} color: {_BODY};")
            with ui.element("div").classes("cb-tw-cell"):
                self._render_stats_row(h)
            with ui.element("div").classes("cb-tw-cell").style("justify-content: flex-end;"):
                self._render_size_badge(mask.mask_path)
            self._render_row_actions(
                on_load_to_viewer=lambda p=mask.mask_path: self._load_to_viewer(p, kind="mask"),
                on_delete=lambda i=mask.id: self._request_delete_mask(i),
            )

    def _mask_note(self, mask: TemplateMask) -> str | None:
        """A mask has no `imported_from` — that field belongs to `ParticleTemplate`. A
        mask's provenance is the template it was derived from (`derived_from_template_id`,
        a soft link); the method has its own column. None when neither is recorded,
        so the row shows only the path rather than implying a source it does not have."""
        if mask.derived_from_template_id:
            sp = self._get_species()
            tpl = sp.get_template_by_id(mask.derived_from_template_id) if sp else None
            if tpl is None:
                return "derived from a template that is no longer registered"
            return f"derived from {os.path.basename(tpl.template_path)}"
        return mask.notes or None

    def _method_chip_with_knobs(self, mask: TemplateMask) -> None:
        knob_parts: list[str] = []
        if mask.threshold is not None:
            knob_parts.append(f"thr {mask.threshold:g}")
        if mask.extend_pixels is not None:
            knob_parts.append(f"ext {mask.extend_pixels:g}")
        if mask.soft_edge_pixels is not None:
            knob_parts.append(f"soft {mask.soft_edge_pixels:g}")
        render_method_chip(mask.method, tooltip=" · ".join(knob_parts) if knob_parts else None)

    def _update_mask_source_label(self) -> None:
        if self._mask_source_label is None:
            return
        sp = self._get_species()
        sel_tpl = sp.get_selected_template() if sp else None
        if sel_tpl is None:
            self._mask_source_label.set_text("Select a template first: From template derives the mask from it.")
        else:
            self._mask_source_label.set_text(f"From template derives it from {os.path.basename(sel_tpl.template_path)}")

    def _render_spherical_mask_form(self) -> None:
        # Default diameter from species; user can still override per-mask.
        sp = self._get_species()
        if self.sphere_diameter_ang is None:
            self.sphere_diameter_ang = float(getattr(sp, "diameter_ang", None) or 0.0) or None

        with ui.row().classes("w-full gap-3 items-center").style("flex-wrap: wrap;"):
            _field(
                "diameter",
                lambda: ui.number(
                    value=self.sphere_diameter_ang,
                    step=10,
                    min=0,
                    placeholder="from species" if sp and sp.diameter_ang else "e.g. 250",
                ).bind_value(self, "sphere_diameter_ang"),
                width="w-28",
                hint="Sphere diameter in Å",
            )
            _field(
                "soft edge",
                lambda: ui.number(value=self.sphere_soft_edge, step=1, min=0).bind_value(self, "sphere_soft_edge"),
                hint="Soft edge width in pixels",
            )
        ui.label(
            "Solid soft-edged sphere centered in the box, sized to the species's particle "
            "diameter. Recommended for VLPs and globular particles where a threshold-derived "
            "mask leaves a hollow lumen."
        ).classes(_HINT_CLS)

    def _render_relion_mask_form(self) -> None:
        with ui.row().classes("w-full gap-3 items-center").style("flex-wrap: wrap;"):
            method = _field(
                "method",
                lambda: ui.select(["flexible_bounds", "otsu", "isodata", "li", "yen"], value=self.threshold_method),
                width="w-36",
                hint="Threshold method",
            )
            method.props('popup-content-class="cb-select-popup"').on_value_change(self._on_threshold_method_changed)
            _field("threshold", lambda: ui.number(format="%.4f").bind_value(self, "mask_threshold"))
            _field("ext", lambda: ui.number().bind_value(self, "mask_extend"), width="w-20", hint="Extend (px)")
            _field("soft", lambda: ui.number().bind_value(self, "mask_soft_edge"), width="w-20", hint="Soft edge (px)")
            _field("lowpass", lambda: ui.number().bind_value(self, "mask_lowpass"), hint="Lowpass (Å)")
        ui.label(
            "Built from the currently selected template via relion_mask_create. "
            "If a black template is selected, its white sibling is used for thresholding."
        ).classes(_HINT_CLS)

    # ------------------------------------------------------------------
    # 5. VIEWER — drawn once something is loaded
    # ------------------------------------------------------------------

    def _render_viewer_panel(self) -> None:
        """The viewer's head (mode, clear, reset) over a "no volume loaded" line until the eye on a row loads
        something: molstar's iframe is mounted on the first load (`_ensure_molstar`), never before, and its box
        shows while it holds or is loading a volume. The slice fallback is built the first time it is chosen."""
        with ui.column().style(SECTION_STYLE):
            head = section_head(
                "Viewer",
                tooltip="molstar draws the volume in 3-D; slices is the fallback for a volume molstar will not load",
            )
            with head:
                toggle = (
                    ui.select(options={"molstar": "molstar", "slice": "slices"}, value=self.viewer_mode)
                    .props('dense popup-content-class="cb-select-popup"')
                    .classes("cb-field")
                    .style("width: 84px;")
                )
                toggle.on_value_change(self._on_viewer_mode_changed)
                icon_button("delete_sweep", self._sweep_viewer, tooltip="Clear the loaded volumes")
                icon_button(
                    "restart_alt", self._hard_reset_viewer, tooltip="Reset the viewer (remount — use if loads jam)"
                )
            self._viewer_empty = ui.label("No volume loaded: the eye on a row loads it.").style(_CAPTION)
            self._molstar_panel = ui.element("div").style(
                "display: flex; flex-direction: column; gap: 6px; width: 100%; min-width: 0;"
            )
            with self._molstar_panel:
                self._molstar_canvas = ui.element("div").style(
                    "position: relative; width: 100%; height: 320px; background: #1a1a1a; overflow: hidden; "
                    "border-radius: 2px;"
                )
                self._pending_loads_container = ui.column().classes("w-full gap-1")
                self._session_list_container = ui.column().classes("w-full gap-1")
            self._slice_panel = ui.element("div").style("width: 100%; min-width: 0;")
        self._apply_viewer_mode_visibility()

    def _ensure_molstar(self) -> None:
        """Mount molstar's iframe, once: its `ready` flushes whatever was queued meanwhile."""
        if self._iframe_mounted or self._molstar_canvas is None:
            return
        with self._molstar_canvas:
            ui.element("iframe").props(f'src="/molstar-workbench" id="{self._iframe_id}"').classes(
                "absolute inset-0 w-full h-full border-none"
            )
        self._iframe_mounted = True

    def _on_viewer_mode_changed(self, e) -> None:
        self.viewer_mode = e.value or "molstar"
        if self.viewer_mode == "slice" and self._slice_controller is None and self._slice_panel is not None:
            sp = self._get_species()
            sel_t = sp.get_selected_template() if sp else None
            sel_m = sp.get_selected_mask() if sp else None
            with self._slice_panel:
                self._slice_controller = render_template_viewer(
                    sel_t.template_path if sel_t else "", sel_m.mask_path if sel_m else None, height_px=260
                )
        self._apply_viewer_mode_visibility()
        # The slice viewer reflects the selection (it's cheap, Python-side); molstar does
        # NOT auto-load — the user clicks the eye.
        if self.viewer_mode == "slice":
            self._refresh_viewer()

    def _apply_viewer_mode_visibility(self) -> None:
        if self._molstar_panel is None or self._viewer_empty is None or self._slice_panel is None:
            return
        molstar = self.viewer_mode == "molstar"
        shown = molstar and self._iframe_mounted and bool(self.loaded_items or self._pending_loads)
        self._molstar_panel.set_visibility(shown)
        self._viewer_empty.set_visibility(molstar and not shown)
        self._slice_panel.set_visibility(not molstar)

    # ------------------------------------------------------------------
    # LOG — one folded line under everything
    # ------------------------------------------------------------------

    def _render_log_panel(self) -> None:
        state = {"open": False}
        with ui.column().classes("w-full").style("gap: 4px;"):
            header = section_head("Activity", right="nothing this session", caret="▸").style(
                "cursor: pointer; user-select: none;"
            )
            self._log_meta = header.meta_label
            self._log_container = ui.column().classes("w-full gap-0.5 px-2 py-1 max-h-40 overflow-y-auto")
        self._log_container.set_visibility(False)

        def _toggle() -> None:
            state["open"] = not state["open"]
            self._log_container.set_visibility(state["open"])
            header.caret.set_text("▾" if state["open"] else "▸")

        header.on("click", _toggle)

    def _log(self, msg: str) -> None:
        if self._log_container is not None:
            with self._log_container:
                ui.label(f"• {msg}").classes(_MONO_CLS + " leading-tight")
            self._log_n += 1
            if self._log_meta is not None:
                self._log_meta.set_text(f"{self._log_n} this session")

    # ==================================================================
    # FORM HANDLERS
    # ==================================================================

    def _on_shape_changed(self) -> None:
        self._recalculate_auto_box_for_shape()
        self._save_workbench_ui()

    def _on_auto_box_toggle(self, e) -> None:
        self.auto_box = bool(e.value)
        if self.auto_box:
            self._recalculate_auto_box_for_shape()
        self._save_workbench_ui()

    def _recalculate_auto_box_for_shape(self) -> None:
        if not self.auto_box:
            return
        try:
            self.shape_box_size = species_edit.auto_box(self.basic_shape_def, self.shape_pixel_size)
        except (ValueError, ZeroDivisionError):
            # A shape still being typed is not numbers yet, or the pixel size is 0: the box waits for both.
            pass

    # ==================================================================
    # GENERATION FLOWS (idempotent — file paths are canonical)
    # ==================================================================

    async def _gen_shape(self) -> bool:
        if self.auto_box:
            self._recalculate_auto_box_for_shape()
        self._save_workbench_ui()
        return await self._make_template(
            "shape",
            f"Generate shape · {self.basic_shape_def}",
            shape=self.basic_shape_def,
            pixel_size=self.shape_pixel_size,
            box_size=int(self.shape_box_size),
            lowpass=self.shape_lowpass if (self.shape_lowpass and self.shape_lowpass > 0) else None,
        )

    async def _fetch_and_simulate_pdb(self) -> bool:
        if not self.pdb_input_val:
            ui.notify("Enter a PDB ID first", type="warning")
            return False
        return await self._make_template(
            "pdb",
            f"PDB → density · {self.pdb_input_val.strip().lower()}",
            pdb_id=self.pdb_input_val,
            pixel_size=self.pdb_pixel_size,
            box_size=int(self.pdb_box_size),
            lowpass=self.pdb_lowpass if (self.pdb_lowpass and self.pdb_lowpass > 0) else None,
        )

    async def _fetch_and_resample_emdb(self) -> bool:
        if not self.emdb_input_val:
            ui.notify("Enter an EMDB ID first", type="warning")
            return False
        return await self._make_template(
            "emdb",
            f"EMDB → resampled · {self.emdb_input_val.strip()}",
            emdb_id=self.emdb_input_val,
            pixel_size=self.emdb_pixel_size,
            box_size=int(self.emdb_box_size),
            lowpass=self.emdb_lowpass if (self.emdb_lowpass and self.emdb_lowpass > 0) else None,
            **_scales(self.emdb_scale_xy, self.emdb_scale_z),
        )

    async def _make_template(self, kind: str, title: str, **values) -> bool:
        """Settle the template (a refusal or an open value shows at once), then make it in the background. The
        door registers and selects what it made and the task saves it, whether or not this page is still open;
        the page repaints when it is. False when it was refused, True once submitted."""
        from ui.background_task import BackgroundTask

        state = self._state()
        planned = await species_edit.plan_template(state, self.species_id, kind, **values)
        if not planned["success"]:
            self._log(f"{title}: {planned['error']}")
            ui.notify(planned["error"], type="warning")
            return False
        plan = planned["plan"]
        if plan.source is not None:
            title = f"{title} · {os.path.basename(plan.source.template_path)}"
        self._log(f"{title}: {_values_line(plan.values())}")
        backend, project_path = self.backend, Path(self.project_path)

        async def _run(progress_cb):
            res = await species_edit.make_template(backend, state, plan, lambda text: progress_cb(0, 0, text))
            if not res["success"]:
                raise RuntimeError(res["error"])
            await backend.save_project(project_path)
            return "registered " + ", ".join(os.path.basename(path) for path in res["paths"])

        BackgroundTask(
            title=title, subtitle=_values_line(plan.values()), dedup_key=plan.key, project_path=str(project_path)
        ).submit(_run, on_complete=self._after_make)
        return True

    def _after_make(self, task) -> None:
        """Repaint once a make has settled, also one another click started (the same make asked for twice runs
        once)."""
        if task.status != "succeeded":
            why = task.error or task.status
            self._log(f"{task.title} failed: {why}")
            ui.notify(f"{task.title} failed: {why}", type="negative")
            return
        self._log(task.result_message or task.title)
        self._refresh_after_change()
        sp = self._get_species()
        if sp is not None:
            _flip_selection(self._template_rows, sp.selected_template_id or "")

    async def _open_import_dialog(self) -> None:
        async with self._flight("import_template") as acquired:
            if not acquired:
                return
            sp = self._get_species()
            if sp is None:
                ui.notify("Species not found", type="warning")
                return
            tpl = await open_template_import_dialog(self.project_path, sp)
            if tpl is None:
                return

            self._append_template(
                tpl.template_path,
                tpl.polarity,
                tpl.source,
                lowpass=tpl.lowpass_resolution_ang,
                imported_from=tpl.imported_from,
                notes=tpl.notes,
            )
            self._log(f"Imported: {os.path.basename(tpl.template_path)}")

    # ── Edit-Current actions (idempotent) ─────────────────────────────

    async def _resample_current(self) -> None:
        await self._make_template(
            "resample",
            "Resample",
            pixel_size=self.resample_target_apix,
            box_size=int(self.resample_target_box),
            lowpass=self.resample_lowpass if (self.resample_lowpass and self.resample_lowpass > 0) else None,
            **_scales(self.resample_scale_xy, self.resample_scale_z),
        )

    async def _apply_lowpass_to_current(self) -> None:
        if not self.lowpass_target or self.lowpass_target <= 0:
            ui.notify("Set a positive lowpass target", type="warning")
            return
        # The template keeps its own pixel size and box; the shape form's stand in when its header has none.
        await self._make_template(
            "lowpass",
            f"Lowpass {float(self.lowpass_target):g}Å",
            lowpass=float(self.lowpass_target),
            pixel_size=self.shape_pixel_size,
            box_size=int(self.shape_box_size),
        )

    async def _flip_polarity(self) -> None:
        await self._make_template("flip", "Flip polarity")

    # ==================================================================
    # MASK FLOWS
    # ==================================================================

    async def _on_threshold_method_changed(self, e) -> None:
        self.threshold_method = e.value
        sp = self._get_species()
        sel = sp.get_selected_template() if sp else None
        if sel is None or not os.path.exists(sel.template_path):
            return
        white = sel.template_path
        if sel.template_path.endswith("_black.mrc"):
            cand = sel.template_path[: -len("_black.mrc")] + "_white.mrc"
            if os.path.exists(cand):
                white = cand
        thresholds = await self.backend.template_service.calculate_thresholds_async(white, self.mask_lowpass)
        if e.value in thresholds:
            self.mask_threshold = round(thresholds[e.value], 4)

    async def _create_relion_mask(self) -> bool:
        return await self._make_mask(
            "threshold",
            "RELION mask",
            threshold=float(self.mask_threshold),
            extend=float(self.mask_extend),
            soft_edge=float(self.mask_soft_edge),
            lowpass=float(self.mask_lowpass),
        )

    async def _create_spherical_mask(self) -> bool:
        """A soft-edged sphere on the selected template's grid; its diameter from this form, else the species'."""
        return await self._make_mask(
            "sphere",
            "Spherical mask",
            diameter_ang=float(self.sphere_diameter_ang or 0.0) or None,
            soft_edge=float(self.sphere_soft_edge or 0.0),
        )

    async def _make_mask(self, kind: str, title: str, **values) -> bool:
        """As `_make_template`, for a mask."""
        from ui.background_task import BackgroundTask

        state = self._state()
        planned = await species_edit.plan_mask(self.backend, state, self.species_id, kind, **values)
        if not planned["success"]:
            self._log(f"{title}: {planned['error']}")
            ui.notify(planned["error"], type="warning")
            return False
        plan = planned["plan"]
        title = f"{title} · {os.path.basename(plan.input_path)}"
        self._log(f"{title}: {_values_line(plan.values())}")
        self.masking_active = True
        backend, project_path = self.backend, Path(self.project_path)

        async def _run(progress_cb):
            progress_cb(0, 0, f"Writing {os.path.basename(plan.out_path)}…")
            res = await species_edit.make_mask(backend, state, plan)
            if not res["success"]:
                raise RuntimeError(res["error"])
            await backend.save_project(project_path)
            return f"mask {os.path.basename(plan.out_path)}"

        def _done(task) -> None:
            self.masking_active = False
            self._after_make(task)

        BackgroundTask(
            title=title, subtitle=_values_line(plan.values()), dedup_key=plan.key, project_path=str(project_path)
        ).submit(_run, on_complete=_done)
        return True

    async def _import_mask(self) -> None:
        async with self._flight("import_mask") as acquired:
            if not acquired:
                return
            picker = local_file_picker(None, mode="file", glob="*.mrc,*.map,*.rec,*.ccp4")
            result = await picker
            if not result or not result[0]:
                return
            picked = result[0]
            if Path(picked).suffix.lower() not in (".mrc", ".map", ".rec", ".ccp4"):
                ui.notify("Mask must be an MRC family file", type="warning")
                return
            self._append_mask(TemplateMask(mask_path=picked, method="imported"))
            self._log(f"Mask imported: {picked}")

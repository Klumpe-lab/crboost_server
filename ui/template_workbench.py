"""Template Workbench — v3 layout, density pass.

Visual structure (top → bottom):
  1. Species header strip — particle metadata inline.
  2. Templates section — selectable cards with file-icon copy affordance.
  3. Source — text-only tabs (shape / pdb-emdb / import / edit current).
  4. Masks section — selectable cards (parallel visual to templates).
  5. Mask creation — text-only tabs (relion / import).
  6. Viewer — molstar (3D), with a slice fallback toggle.
  7. Activity log (collapsed).

Design rules enforced here:
  - Color palette is gray + indigo (templates) + purple (masks) +
    white/black polarity chips. No blue/emerald/amber tints elsewhere.
  - Font scale collapsed to three: text-sm (section titles),
    text-xs (body / form labels), text-[10px] (captions / chips / mono).
  - Outer cards reserved for items in a list (one card per template / mask),
    the species identity, and the viewer container. Section panels are
    drawn with a header row + spacing — no nested borders.
  - All template-producing actions (shape, pdb, emdb, import, resample,
    apply-lowpass, flip-polarity) write to canonical paths and skip the
    write if a registered entry already exists at that path — no
    accidental dup spam.
  - Delete is confirmed in a modal; removes the file + sidecar from disk.

Schema reference: services/project_state.py v3 — species.templates and
species.masks are sibling collections with UUID identity.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import mrcfile
from fastapi.responses import FileResponse, HTMLResponse
from nicegui import app, context, ui

from services.jobs._base import SymmetryGroup
from services.project_state import (
    ParticleSpecies,
    ParticleTemplate,
    TemplateMask,
    get_project_state_for,
    get_state_service,
    sidecar_ensure,
)
from services.templating.template_metadata import read_template_header
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


@app.get("/api/file")
def serve_file(path: str):
    p = Path(path)
    if p.exists() and p.is_file():
        return FileResponse(p, media_type="application/octet-stream")
    return {"error": "not found"}


COLOR_PALETTE = [
    0x5C6BC0, 0x7986CB, 0x9FA8DA, 0x42A5F5, 0x64B5F6, 0x90CAF9,
    0x26C6DA, 0x4DD0E1, 0x80DEEA, 0x26A69A, 0x4DB6AC, 0x80CBC4,
    0x66BB6A, 0x81C784, 0xA5D6A7, 0x9CCC65, 0xAED581, 0xC5E1A5,
    0xFFA726, 0xFFB74D, 0xFFCC80, 0xFFEE58, 0xFFF176, 0xFFF59D,
    0xEF5350, 0xE57373, 0xEF9A9A, 0xEC407A, 0xF06292, 0xF48FB1,
    0xAB47BC, 0xBA68C8, 0xCE93D8, 0x7E57C2, 0x9575CD, 0xB39DDB,
]


# ─── Shared style chrome ────────────────────────────────────────────────
# Three font sizes total. Two accent colors total. Section panels use
# a thin gray underline rather than full card borders.

_TITLE_CLS = "text-sm font-semibold text-gray-800"
_LABEL_CLS = "text-[10px] font-bold text-gray-500 uppercase tracking-wider"
_HINT_CLS = "text-[10px] text-gray-400"
_BODY_CLS = "text-xs text-gray-700"
_MONO_CLS = "text-[10px] font-mono text-gray-600"
_INDIGO = "#6366f1"
_PURPLE = "#a855f7"
# Tabs use Quasar defaults — small but properly tab-shaped, not flat text.
# `inline-label` keeps the tab content compact when no icon is set.
_TAB_PROPS = "dense align=left indicator-color=indigo inline-label"
_TAB_PROPS_PURPLE = "dense align=left indicator-color=purple inline-label"
_CARD_W = 260


class TemplateWorkbench:
    """Per-species workbench. Public constructor signature preserved."""

    def __init__(self, backend, project_path: str, species_id: str, *, on_species_deleted=None):
        """The workbench owns one species. `on_species_deleted` is called
        after the user confirms deletion (and the model+files are gone);
        the surrounding panel uses it to drop the workbench container and
        switch the active tab."""
        self.backend = backend
        self.project_path = project_path
        self.species_id = species_id
        self.on_species_deleted = on_species_deleted
        self.output_folder = os.path.join(project_path, "templates", species_id)
        os.makedirs(self.output_folder, exist_ok=True)

        # Scope iframe + event name per-species so multiple workbench tabs
        # don't cross-leak. Before this, all iframes shared id='molstar-frame'
        # so document.getElementById hit the FIRST one only — load_volume
        # posts went to the wrong iframe, and the postMessage listeners
        # cross-fired into the wrong workbench's Activity Log.
        # `species_id` is already a slug; safe in a DOM id and event name.
        self._iframe_id = f"molstar-frame-{species_id}"
        self._molstar_event_name = f"molstar_event_{species_id}"

        self.project_raw_apix: Optional[float] = None
        self.project_tomo_apix: Optional[float] = None

        # Persisted on species.workbench_ui
        self.auto_box: bool = True
        self.basic_shape_def: str = "550:550:550"

        # Per-flow generation form state (not persisted)
        self.shape_pixel_size: float = 10.0
        self.shape_box_size: int = 96
        self.shape_lowpass: Optional[float] = None

        self.pdb_pixel_size: float = 10.0
        self.pdb_box_size: int = 96
        self.pdb_lowpass: Optional[float] = None
        self.pdb_input_val: str = ""

        self.emdb_pixel_size: float = 10.0
        self.emdb_box_size: int = 96
        self.emdb_lowpass: Optional[float] = None
        self.emdb_input_val: str = ""

        # Edit-current action forms
        self.resample_target_apix: float = 10.0
        self.resample_target_box: int = 96
        self.resample_lowpass: Optional[float] = None
        self.lowpass_target: float = 30.0

        # Mask form
        self.mask_threshold: float = 0.5
        self.mask_extend: float = 5
        self.mask_soft_edge: float = 5
        self.mask_lowpass: float = 20
        self.threshold_method: str = "flexible_bounds"
        self.masking_active: bool = False

        # Spherical-mask form. Diameter defaulted from species.diameter_ang
        # at render time (UI can still override). Soft edge in pixels.
        self.sphere_diameter_ang: Optional[float] = None
        self.sphere_soft_edge: float = 5.0

        # Viewer state
        self.viewer_mode: str = "molstar"
        self.viewer_ready: bool = False
        self.loaded_items: list = []
        self.session_item_containers: dict = {}

        # Bridge robustness state (see MOLSTAR_VIEWER_PLAN.md Slice A):
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

        # Slice C UI affordances:
        #   _pending_loads: optimistic spinner entries for in-flight
        #     load_volume requests. FIFO-correlated with itemsChanged
        #     growth — we can't track per-item since molstar generates
        #     IDs, so we just pop the oldest 'loading' when count rises.
        #     Entries flip to 'error' after PENDING_ERROR_S without
        #     reconcile.
        self._pending_loads: list[dict] = []
        self._pending_load_seq: int = 0
        self._pending_loads_container: Optional[ui.element] = None

        # UI refs
        self._templates_card_container: Optional[ui.element] = None
        self._masks_card_container: Optional[ui.element] = None
        self._edit_container: Optional[ui.element] = None
        self._mask_source_label = None
        self._molstar_panel: Optional[ui.element] = None
        self._slice_panel: Optional[ui.element] = None
        self._slice_controller: Optional[TemplateViewerController] = None
        self._session_list_container: Optional[ui.element] = None
        self._log_container: Optional[ui.element] = None
        self.client = None

        self._load_project_parameters()
        self._render()
        self.client = context.client

        ui.timer(1.0, self._post_initial_items, once=True)
        ui.timer(5.0, self._prune_pending_loads)

    # ==================================================================
    # SPECIES / STATE ACCESS
    # ==================================================================

    def _get_species(self) -> Optional[ParticleSpecies]:
        state = get_project_state_for(Path(self.project_path))
        return state.get_species(self.species_id)

    def _mutate_species(self, fn) -> None:
        state = get_project_state_for(Path(self.project_path))
        sp = state.get_species(self.species_id)
        if sp is None:
            return
        fn(sp)
        state.mark_dirty()

    async def _save_state(self) -> None:
        await get_state_service().save_project(project_path=Path(self.project_path))

    def _save_workbench_ui(self) -> None:
        def _apply(sp: ParticleSpecies) -> None:
            sp.workbench_ui.auto_box = self.auto_box
            sp.workbench_ui.basic_shape_def = self.basic_shape_def

        self._mutate_species(_apply)
        asyncio.create_task(self._save_state())

    def _load_project_parameters(self) -> None:
        try:
            state = get_project_state_for(Path(self.project_path))
            if hasattr(state, "microscope") and state.microscope:
                raw = getattr(state.microscope, "pixel_size_angstrom", None)
                if raw and raw > 0:
                    self.project_raw_apix = float(raw)
            if hasattr(state, "jobs") and state.jobs:
                for _, jp in state.jobs.items():
                    if "reconstruct" in str(getattr(jp, "job_type", "")).lower():
                        for fld in ("rescale_angpixs", "binned_angpix", "output_angpix"):
                            v = getattr(jp, fld, None)
                            if v and float(v) > 0:
                                self.project_tomo_apix = float(v)
                                break
                        break

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

            default_apix = self.project_tomo_apix or self.project_raw_apix or 10.0
            self.shape_pixel_size = default_apix
            self.pdb_pixel_size = default_apix
            self.emdb_pixel_size = default_apix
            self.resample_target_apix = default_apix
        except Exception as e:
            logger.info("Load project params error: %s", e)

    # ==================================================================
    # WRITE-THROUGH (append, dedup-aware)
    # ==================================================================

    def _append_template(
        self,
        template_path: str,
        polarity: str,
        source: str,
        *,
        lowpass: Optional[float] = None,
        imported_from: Optional[str] = None,
        notes: str = "",
    ) -> str:
        """Append a ParticleTemplate (or replace in-place if a registered
        entry already exists at this path). Auto-selects only when this
        is the first entry. Returns the entry's UUID."""
        new_id = sidecar_ensure(template_path, "template")
        tpl = ParticleTemplate(
            id=new_id,
            template_path=template_path,
            polarity=polarity if polarity in ("white", "black") else "black",
            lowpass_resolution_ang=lowpass,
            source=source,
            imported_from=imported_from,
            created_at=datetime.now(),
            notes=notes,
        )

        def _apply(sp: ParticleSpecies) -> None:
            existing_idx = next(
                (i for i, t in enumerate(sp.templates) if t.template_path == template_path), None
            )
            if existing_idx is not None:
                # Preserve the existing id so dropdowns elsewhere stay stable.
                tpl.id = sp.templates[existing_idx].id
                sp.templates[existing_idx] = tpl
            else:
                sp.templates.append(tpl)
            if not sp.selected_template_id:
                sp.selected_template_id = tpl.id

        self._mutate_species(_apply)
        asyncio.create_task(self._after_register())
        return new_id

    def _append_mask(self, mask: TemplateMask) -> str:
        mid = sidecar_ensure(mask.mask_path, "mask")
        mask = mask.model_copy(update={"id": mid, "created_at": datetime.now()})

        def _apply(sp: ParticleSpecies) -> None:
            existing_idx = next((i for i, m in enumerate(sp.masks) if m.mask_path == mask.mask_path), None)
            if existing_idx is not None:
                mask.id = sp.masks[existing_idx].id
                sp.masks[existing_idx] = mask
            else:
                sp.masks.append(mask)
            if not sp.selected_mask_id:
                sp.selected_mask_id = mask.id

        self._mutate_species(_apply)
        asyncio.create_task(self._after_register())
        return mid

    async def _after_register(self) -> None:
        await self._save_state()
        self._refresh_after_change()

    def _select_template(self, template_id: str) -> None:
        def _apply(sp: ParticleSpecies) -> None:
            if any(t.id == template_id for t in sp.templates):
                sp.selected_template_id = template_id

        self._mutate_species(_apply)
        asyncio.create_task(self._after_register())

    def _select_mask(self, mask_id: str) -> None:
        def _apply(sp: ParticleSpecies) -> None:
            if any(m.id == mask_id for m in sp.masks):
                sp.selected_mask_id = mask_id

        self._mutate_species(_apply)
        asyncio.create_task(self._after_register())

    def _refresh_after_change(self) -> None:
        if self._templates_card_container is not None:
            self._templates_card_container.clear()
            with self._templates_card_container:
                self._render_template_cards()
        if self._masks_card_container is not None:
            self._masks_card_container.clear()
            with self._masks_card_container:
                self._render_mask_cards()
        if self._edit_container is not None:
            self._edit_container.clear()
            with self._edit_container:
                self._render_edit_current_form()
        self._update_mask_source_label()
        self._refresh_viewer()

    # ==================================================================
    # DELETE (with confirmation + on-disk removal)
    # ==================================================================

    def _request_delete_template(self, template_id: str) -> None:
        sp = self._get_species()
        tpl = sp.get_template_by_id(template_id) if sp else None
        if tpl is None:
            return
        self._open_delete_confirmation(
            kind="template",
            file_path=tpl.template_path,
            on_confirm=lambda: self._do_delete_template(template_id),
        )

    def _request_delete_mask(self, mask_id: str) -> None:
        sp = self._get_species()
        mask = sp.get_mask_by_id(mask_id) if sp else None
        if mask is None:
            return
        self._open_delete_confirmation(
            kind="mask",
            file_path=mask.mask_path,
            on_confirm=lambda: self._do_delete_mask(mask_id),
        )

    def _open_delete_confirmation(self, *, kind: str, file_path: str, on_confirm) -> None:
        fname = os.path.basename(file_path)
        with ui.dialog() as dialog, ui.card().classes("p-4 gap-2"):
            ui.label(f"Delete this {kind}?").classes(_TITLE_CLS)
            ui.label(fname).classes(_MONO_CLS)
            ui.label(
                "Removes the file from disk (and its sidecar) and unregisters it from the species."
            ).classes(_HINT_CLS)
            with ui.row().classes("w-full justify-end gap-2 mt-2"):
                ui.button("Cancel", on_click=dialog.close).props("flat dense no-caps")

                def _confirm():
                    dialog.close()
                    on_confirm()

                ui.button("Delete", on_click=_confirm).props("unelevated dense color=negative no-caps")
        dialog.open()

    def _do_delete_template(self, template_id: str) -> None:
        sp = self._get_species()
        if sp is None:
            return
        tpl = sp.get_template_by_id(template_id)
        if tpl is None:
            return
        path = tpl.template_path
        self._delete_file_with_sidecar(path)

        def _apply(s: ParticleSpecies) -> None:
            s.templates = [t for t in s.templates if t.id != template_id]
            if s.selected_template_id == template_id:
                s.selected_template_id = s.templates[0].id if s.templates else ""
            # Also drop masks that were derived from this template
            s.masks = [m for m in s.masks if m.derived_from_template_id != template_id]
            if s.selected_mask_id and not any(m.id == s.selected_mask_id for m in s.masks):
                s.selected_mask_id = s.masks[0].id if s.masks else ""

        self._mutate_species(_apply)
        asyncio.create_task(self._after_register())
        self._log(f"Deleted template: {os.path.basename(path)}")

    def _do_delete_mask(self, mask_id: str) -> None:
        sp = self._get_species()
        if sp is None:
            return
        mask = sp.get_mask_by_id(mask_id)
        if mask is None:
            return
        path = mask.mask_path
        self._delete_file_with_sidecar(path)

        def _apply(s: ParticleSpecies) -> None:
            s.masks = [m for m in s.masks if m.id != mask_id]
            if s.selected_mask_id == mask_id:
                s.selected_mask_id = s.masks[0].id if s.masks else ""

        self._mutate_species(_apply)
        asyncio.create_task(self._after_register())
        self._log(f"Deleted mask: {os.path.basename(path)}")

    # ── Species-level delete (cascade through templates + masks + folder) ──

    def _request_delete_species(self) -> None:
        sp = self._get_species()
        if sp is None:
            return
        n_tpl = len(sp.templates)
        n_mask = len(sp.masks)
        with ui.dialog() as dialog, ui.card().classes("p-4 gap-2"):
            ui.label(f"Delete species '{sp.name}'?").classes(_TITLE_CLS)
            with ui.column().classes("gap-0 mt-1"):
                ui.label(f"• {n_tpl} template{'s' if n_tpl != 1 else ''} on disk").classes(_BODY_CLS)
                ui.label(f"• {n_mask} mask{'es' if n_mask != 1 else ''} on disk").classes(_BODY_CLS)
                ui.label(f"• Folder: {self.output_folder}").classes(_MONO_CLS)
            ui.label(
                "All registered files (+ sidecars) get removed. The folder is "
                "removed only if empty afterwards (manual drops are preserved)."
            ).classes(_HINT_CLS + " mt-1")
            ui.label("This cannot be undone.").classes(_HINT_CLS + " text-red-600")
            with ui.row().classes("w-full justify-end gap-2 mt-2"):
                ui.button("Cancel", on_click=dialog.close).props("flat dense no-caps")

                def _confirm():
                    dialog.close()
                    self._do_delete_species()

                ui.button("Delete species", on_click=_confirm).props(
                    "unelevated dense color=negative no-caps"
                )
        dialog.open()

    def _do_delete_species(self) -> None:
        sp = self._get_species()
        if sp is None:
            return
        sid = sp.id
        # Cascade: delete each template's + mask's file + sidecar.
        for tpl in list(sp.templates):
            self._delete_file_with_sidecar(tpl.template_path)
        for mask in list(sp.masks):
            self._delete_file_with_sidecar(mask.mask_path)
        # Try to remove the folder. rmdir only succeeds when empty —
        # leftover files (manually-dropped MRCs, RELION run logs) keep
        # the folder around. That's intentional; user can rm -rf later.
        try:
            os.rmdir(self.output_folder)
        except OSError:
            logger.info("Species folder %s not empty after cascade; left in place", self.output_folder)

        state = get_project_state_for(Path(self.project_path))
        state.remove_species(sid)
        asyncio.create_task(self._save_state())
        self._log(f"Deleted species: {sid}")

        if callable(self.on_species_deleted):
            try:
                self.on_species_deleted(sid)
            except Exception as e:
                logger.warning("on_species_deleted callback failed: %s", e)

    def _delete_file_with_sidecar(self, file_path: str) -> None:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except OSError as e:
            logger.warning("Could not remove %s: %s", file_path, e)
            ui.notify(f"Could not remove {os.path.basename(file_path)}", type="warning")
        sidecar = Path(file_path).with_name(Path(file_path).name + ".meta.json")
        try:
            if sidecar.exists():
                sidecar.unlink()
        except OSError as e:
            logger.warning("Could not remove sidecar %s: %s", sidecar, e)

    # ==================================================================
    # MOLSTAR BRIDGE (Slice A — see MOLSTAR_VIEWER_PLAN.md)
    #
    # The iframe is a black box: postMessage out, events back. Three
    # robustness layers sit between the UI and the raw bridge:
    #
    #   1. READY GATE — commands posted before the iframe emits `ready`
    #      get queued and replayed on `ready`. Prevents the silent
    #      "post-before-ready" drop that used to make session state
    #      look stuck.
    #   2. IN-FLIGHT DEDUP — per-(itemId, action) timestamp; identical
    #      sends within DEDUP_WINDOW_S are dropped. Double-click spam on
    #      visibility used to corrupt molstar's internal state.
    #   3. OPTIMISTIC UI — visibility toggles flip the icon immediately
    #      and reconcile against itemsChanged. The user sees an instant
    #      response even when the iframe is mid-load on a big volume.
    #
    # `_hard_reset_viewer` is the escape hatch: it remounts the iframe
    # entirely. Used when the iframe's internal queue jams up.
    # ==================================================================

    DEDUP_WINDOW_S = 0.2
    _DEDUP_ACTIONS = frozenset({"setVisibility", "setColor", "setIsoValue", "deleteItem"})
    # Files above this on disk are likely to choke molstar (the 1.55 Å/px
    # ~345 MB report). Card size badge turns red above this and orange
    # at half-threshold so the user has a pre-load warning.
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
            # templates (Slice B2 issue) or when the parser falls back
            # to default {-1, 1, 0, 0.1} stats.
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

        Why: molstar's internal load queue can jam on a partially-loaded
        large volume; soft `clear` then never gets processed. Remounting
        the iframe is the Python-side equivalent of a page reload, but
        without re-rendering the rest of the workbench.

        How to apply: callable from a UI button. Clears all bridge state
        so a fresh `ready` flushes against an empty queue."""
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

    def _load_to_viewer(
        self, file_path: str, *, polarity: Optional[str] = None, kind: str = "template"
    ) -> None:
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
            self._pending_load_seq += 1
            self._pending_loads.append({
                "seq": self._pending_load_seq,
                "filename": os.path.basename(file_path),
                "started_at": time.monotonic(),
                "status": "loading",
            })
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
        """Called after register/select. In v3 we deliberately do NOT
        auto-load selected entries — the user controls what's in the
        viewer via the per-card eye icon. This method exists for legacy
        callers and is now a no-op for molstar mode."""
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
            container = ui.card().tight().classes("p-1 bg-white border border-gray-200 shadow-none")
            with container:
                with ui.row().classes("items-center gap-1"):
                    color_btn = (
                        ui.button(icon="circle").props("flat round dense size=xs").style(f"color: {color_hex}")
                    )
                    with ui.menu().props("auto-close") as color_menu:
                        with ui.grid(columns=6).classes("gap-0.5 p-1"):
                            for pc in COLOR_PALETTE:
                                ui.button().props("flat dense").style(
                                    f"background:{f'#{pc:06x}'};width:16px;height:16px;min-width:16px;"
                                ).on("click", lambda c=pc, i=iid: self._post_to_viewer("setColor", itemId=i, color=c))
                    color_btn.on("click", color_menu.open)
                    ui.label(iid).classes(_MONO_CLS + " max-w-[120px] truncate")
                    vis_btn = ui.button(
                        icon="visibility" if visible else "visibility_off",
                        on_click=lambda i=iid: self._toggle_visibility_from_ui(i),
                    ).props("flat round dense size=xs")
                    ui.button(
                        icon="close", on_click=lambda i=iid: self._post_to_viewer("deleteItem", itemId=i)
                    ).props("flat round dense size=xs color=red")

                iso_slider, iso_label = None, None
                if item_type == "map":
                    iso_value = item.get("isoValue", 1.5)
                    is_inv = item.get("isInverted", False)
                    with ui.row().classes("items-center gap-1 mt-0.5"):
                        ui.label("ISO").classes(_HINT_CLS + " shrink-0")
                        iso_slider = (
                            ui.slider(min=0.5, max=5.0, step=0.1, value=abs(iso_value)).props("dense").classes("w-20")
                        )
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
        if self._pending_loads_container is None:
            return
        self._pending_loads_container.clear()
        with self._pending_loads_container:
            for entry in self._pending_loads:
                status = entry.get("status", "loading")
                with ui.card().tight().classes("p-1 bg-gray-50 border border-gray-200 shadow-none w-full"):
                    with ui.row().classes("items-center gap-1 w-full min-w-0"):
                        if status == "loading":
                            ui.spinner("dots", size="xs").classes("text-indigo-500 shrink-0")
                        else:
                            ui.icon("error_outline", size="12px").classes("text-red-600 shrink-0")
                        ui.label(entry["filename"]).classes(_MONO_CLS + " truncate flex-1 min-w-0")
                        if status == "error":
                            dismiss = ui.button(icon="close").props("flat round dense size=xs color=grey")
                            dismiss.on(
                                "click",
                                lambda _e, seq=entry["seq"]: self._dismiss_pending_load(seq),
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
        # Outer gap-5 between major groups; inner gap-2 inside a group
        # to keep paired sections (templates+source, masks+mask-tabs)
        # visually unified.
        with ui.column().classes("w-full gap-5 p-2"):
            self._render_species_header()
            with ui.column().classes("w-full gap-2"):
                self._render_templates_section()
                self._render_source_panel()
            with ui.column().classes("w-full gap-2"):
                self._render_masks_section()
            self._render_viewer_panel()
            self._render_log_panel()

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
    # 1. SPECIES HEADER
    # ------------------------------------------------------------------

    def _render_species_header(self) -> None:
        sp = self._get_species()
        if sp is None:
            return
        species_color = getattr(sp, "color", "#3b82f6") or "#3b82f6"
        species_name = getattr(sp, "name", None) or self.species_id
        diameter = getattr(sp, "diameter_ang", None)
        symmetry = getattr(sp, "symmetry", "C1") or "C1"
        notes = getattr(sp, "notes", "") or ""

        with ui.card().tight().classes("w-full overflow-hidden").style(
            f"border: 1px solid #e5e7eb; border-left: 4px solid {_INDIGO}; box-shadow: none;"
        ):
            with ui.row().classes("w-full items-center px-3 py-1 gap-3"):
                ui.element("div").style(
                    f"width: 10px; height: 10px; border-radius: 50%; "
                    f"background: {species_color}; flex-shrink: 0;"
                )
                ui.label(species_name).classes(_TITLE_CLS)
                ui.label("species particle metadata").classes(_HINT_CLS)

                with ui.row().classes("items-center gap-1 ml-3"):
                    ui.label("Ø").classes(_BODY_CLS)
                    diam_input = (
                        ui.number(value=diameter, placeholder="e.g. 250", step=10, min=0, suffix="Å")
                        .props("dense outlined")
                        .classes("w-24")
                    )

                    def _on_diam(e):
                        try:
                            new_val = float(e.value) if e.value not in (None, "") else None
                        except (TypeError, ValueError):
                            return

                        def _apply(s: ParticleSpecies) -> None:
                            s.diameter_ang = new_val

                        self._mutate_species(_apply)
                        asyncio.create_task(self._save_state())

                    diam_input.on_value_change(_on_diam)

                with ui.row().classes("items-center gap-1"):
                    ui.label("sym").classes(_BODY_CLS)
                    sym_select = (
                        ui.select(options=[g.value for g in SymmetryGroup], value=symmetry)
                        .props("dense outlined")
                        .classes("w-20")
                    )

                    def _on_sym(e):
                        new_val = e.value or "C1"

                        def _apply(s: ParticleSpecies) -> None:
                            s.symmetry = new_val

                        self._mutate_species(_apply)
                        asyncio.create_task(self._save_state())

                    sym_select.on_value_change(_on_sym)

                with ui.row().classes("items-center gap-1 flex-1"):
                    notes_input = (
                        ui.input(value=notes, placeholder="notes (free-form, optional)")
                        .props("dense outlined")
                        .classes("flex-1")
                    )

                    def _on_notes(e):
                        v = e.value or ""

                        def _apply(s: ParticleSpecies) -> None:
                            s.notes = v

                        self._mutate_species(_apply)
                        asyncio.create_task(self._save_state())

                    notes_input.on_value_change(_on_notes)

                delete_btn = ui.button(icon="delete_forever", on_click=self._request_delete_species).props(
                    "flat round dense size=sm color=grey-7"
                )
                delete_btn.tooltip("Delete this species (registry + files)")

            ui.label(
                "Defaults for new TM (symmetry) and candidate-extract (diameter) jobs. "
                "Existing jobs aren't auto-updated."
            ).classes(_HINT_CLS + " px-3 pb-1")

    # ------------------------------------------------------------------
    # 2. TEMPLATES — selectable cards
    # ------------------------------------------------------------------

    def _render_templates_section(self) -> None:
        with ui.column().classes("w-full gap-1"):
            with ui.row().classes("w-full items-baseline gap-2 px-1"):
                ui.label("TEMPLATES").classes(_LABEL_CLS)
                ui.label("click a card to select").classes(_HINT_CLS)
            self._templates_card_container = ui.row().classes("w-full gap-2 flex-wrap")
            with self._templates_card_container:
                self._render_template_cards()

    def _render_template_cards(self) -> None:
        sp = self._get_species()
        if sp is None:
            return
        templates = list(sp.templates)
        if not templates:
            self._render_empty_state("No templates yet — use Source below to add one.")
            return
        sid = sp.selected_template_id
        for t in templates:
            self._render_template_card(t, selected=(t.id == sid))

    def _render_template_card(self, tpl: ParticleTemplate, *, selected: bool) -> None:
        h = read_template_header(tpl.template_path)
        border_color = _INDIGO if selected else "#e5e7eb"
        with ui.card().tight().classes("overflow-hidden cursor-pointer").style(
            f"border: 1px solid #e5e7eb; border-left: 3px solid {border_color}; "
            f"width: {_CARD_W}px; background: white; box-shadow: none;"
        ).on("click", lambda i=tpl.id: self._select_template(i)):
            with ui.column().classes("p-2 gap-1 w-full min-w-0"):
                self._render_card_header_row(
                    file_path=tpl.template_path,
                    on_delete=lambda i=tpl.id: self._request_delete_template(i),
                    on_load_to_viewer=lambda p=tpl.template_path, pol=tpl.polarity: self._load_to_viewer(
                        p, polarity=pol, kind="template"
                    ),
                )
                with ui.row().classes("items-center gap-1 w-full min-w-0"):
                    self._polarity_chip(tpl.polarity)
                    ui.label(self._format_header_summary(h, tpl.lowpass_resolution_ang)).classes(
                        _MONO_CLS + " truncate flex-1 min-w-0"
                    )
                    self._render_size_badge(tpl.template_path)
                with ui.row().classes("items-center gap-1 w-full min-w-0"):
                    self._render_stats_row(h)
                if tpl.source:
                    ui.label(tpl.source).classes(_HINT_CLS + " truncate w-full min-w-0")

    def _render_card_header_row(self, *, file_path: str, on_delete, on_load_to_viewer) -> None:
        """The top row of a card: file icon (tooltip+copy) + filename +
        eye (load to viewer) + delete X. Buttons use click.stop so the
        outer card click handler (which selects the entry) doesn't fire
        — otherwise the X click would re-render the card mid-modal-open."""
        with ui.row().classes("items-center gap-1 w-full min-w-0"):
            icon = ui.icon("description", size="13px").classes("text-gray-400 shrink-0 cursor-pointer")
            icon.tooltip(file_path)
            icon.on("click.stop", lambda _e, p=file_path: self._copy_to_clipboard(p))

            # min-w-0 + flex-1 + truncate — required so the label gets
            # ellipsis'd instead of pushing the buttons off-screen.
            ui.label(os.path.basename(file_path)).classes(_BODY_CLS + " truncate flex-1 min-w-0")

            eye = ui.button(icon="visibility").props("flat round dense size=xs color=grey")
            eye.tooltip("Load into viewer")
            eye.on("click.stop", lambda _e: on_load_to_viewer())

            close = ui.button(icon="close").props("flat round dense size=xs color=grey")
            close.tooltip("Delete")
            close.on("click.stop", lambda _e: on_delete())

    def _copy_to_clipboard(self, value: str) -> None:
        ui.run_javascript(f"navigator.clipboard.writeText({json.dumps(value)})")
        ui.notify("Path copied", type="info", position="bottom", timeout=1200)

    def _format_stats_line(self, h) -> Optional[tuple[str, str]]:
        """Return (label, color) for a min/max/σ chip line. None if the
        header doesn't carry stats (file unreadable / pre-v3 file).

        Color flags the normalization state:
          - std around 1 (post-normalize): gray, looks good.
          - std much smaller than 0.1 (un-normalized RELION class): orange.
          - dmin == dmax (constant volume / mask-likely-empty): red.
        """
        if h.dmin is None or h.dmax is None:
            return None
        if h.dmin == h.dmax:
            # Constant volume — likely an empty mask or a TM sentinel.
            return f"min {h.dmin:.3g} · max {h.dmax:.3g} · constant", "#dc2626"
        std = h.rms or 0.0
        if std > 0 and (std < 0.1 or std > 5.0):
            color = "#ea580c"
        else:
            color = "#6b7280"
        return f"min {h.dmin:.3g} · max {h.dmax:.3g} · σ {std:.3g}", color

    def _render_stats_row(self, h) -> None:
        res = self._format_stats_line(h)
        if res is None:
            return
        label, color = res
        chip = ui.label(label).style(
            f"color: {color}; font-size: 9px; font-variant-numeric: tabular-nums;"
        )
        if color != "#6b7280":
            if color == "#dc2626":
                chip.tooltip("Constant volume — likely an empty mask or unsuccessful TM run")
            else:
                chip.tooltip("Unusual σ — values may not be σ-normalized")

    def _format_header_summary(self, h, lowpass: Optional[float]) -> str:
        parts: list[str] = []
        if h.apix_ang:
            parts.append(f"{h.apix_ang:.3g} Å/px")
        if h.box_px:
            parts.append(f"box {h.box_px}")
        if lowpass:
            parts.append(f"lp {lowpass:g}Å")
        return " · ".join(parts) if parts else "header unreadable"

    def _format_file_size(self, file_path: str) -> Optional[tuple[str, str]]:
        """Return (human-readable size, css color). Gray under half the
        warn threshold, orange between half and full, red above. Used to
        warn the user before they ask molstar to load a giant volume."""
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
        if mb >= self.MOLSTAR_WARN_MB:
            color = "#dc2626"
        elif mb >= self.MOLSTAR_WARN_MB / 2:
            color = "#ea580c"
        else:
            color = "#6b7280"
        return label, color

    def _render_size_badge(self, file_path: str) -> None:
        res = self._format_file_size(file_path)
        if res is None:
            return
        label, color = res
        badge = ui.label(label).style(
            f"color: {color}; font-size: 9px; font-weight: 700; "
            f"font-variant-numeric: tabular-nums; "
            f"padding: 1px 4px; border-radius: 3px; "
            f"background: {color}1a; flex-shrink: 0;"
        )
        if "#dc2626" in color or "#ea580c" in color:
            badge.tooltip("Large file — molstar may stall on load")

    def _polarity_chip(self, polarity: str) -> None:
        if polarity == "white":
            bg, fg, label = "#fff7ed", "#9a3412", "white"
        else:
            bg, fg, label = "#1f2937", "#f9fafb", polarity or "black"
        ui.label(label).style(
            f"background: {bg}; color: {fg}; "
            f"font-size: 9px; font-weight: 700; text-transform: uppercase; "
            f"padding: 1px 5px; border-radius: 3px; letter-spacing: 0.5px;"
        )

    def _method_chip(self, method: Optional[str]) -> None:
        palette = {
            "spherical": ("#e9d5ff", "#581c87"),
            "cylindrical": ("#e9d5ff", "#581c87"),
            "relion": ("#e9d5ff", "#581c87"),
            "manual": ("#e5e7eb", "#374151"),
            "imported": ("#e5e7eb", "#374151"),
        }
        bg, fg = palette.get(method or "", ("#e5e7eb", "#374151"))
        ui.label(method or "—").style(
            f"background: {bg}; color: {fg}; "
            f"font-size: 9px; font-weight: 700; text-transform: uppercase; "
            f"padding: 1px 5px; border-radius: 3px; letter-spacing: 0.5px;"
        )

    def _render_empty_state(self, text: str) -> None:
        with ui.row().classes("items-center gap-2 px-3 py-2 text-gray-400"):
            ui.icon("hourglass_empty", size="12px")
            ui.label(text).classes(_HINT_CLS)

    # ------------------------------------------------------------------
    # 3. SOURCE — text-only tabs
    # ------------------------------------------------------------------

    def _render_source_panel(self) -> None:
        with ui.column().classes("w-full gap-1"):
            with ui.row().classes("w-full items-baseline gap-2 px-1"):
                ui.label("SOURCE").classes(_LABEL_CLS)
                ui.label("generate, fetch, import; new entries append above").classes(_HINT_CLS)

            with ui.tabs().props(_TAB_PROPS).classes("w-full") as tabs:
                tab_shape = ui.tab(name="shape", label="Basic Shape")
                tab_pdb = ui.tab(name="pdb", label="PDB / EMDB")
                tab_import = ui.tab(name="import", label="Import")
                tab_edit = ui.tab(name="edit", label="Edit Current")

            with ui.tab_panels(tabs, value=tab_shape).classes("w-full"):
                with ui.tab_panel(tab_shape).classes("px-2 py-2"):
                    self._render_basic_shape_form()
                with ui.tab_panel(tab_pdb).classes("px-2 py-2"):
                    self._render_pdb_emdb_form()
                with ui.tab_panel(tab_import).classes("px-2 py-2"):
                    self._render_import_form()
                with ui.tab_panel(tab_edit).classes("px-2 py-2"):
                    self._edit_container = ui.column().classes("w-full gap-2")
                    with self._edit_container:
                        self._render_edit_current_form()

    def _render_basic_shape_form(self) -> None:
        with ui.row().classes("w-full gap-2 items-end"):
            ui.input(label="ellipsoid x:y:z (Å)", placeholder="550:550:550").bind_value(
                self, "basic_shape_def"
            ).props("dense outlined").classes("w-44").on("update:model-value", self._on_shape_changed)
            ui.number("apix (Å)", value=self.shape_pixel_size, step=0.1, min=0).bind_value(
                self, "shape_pixel_size"
            ).props("dense outlined").classes("w-24")
            ui.number("box (px)", value=self.shape_box_size, step=32, min=32).bind_value(
                self, "shape_box_size"
            ).props("dense outlined").classes("w-24")
            ui.checkbox("auto box", value=self.auto_box).props("dense").bind_value(
                self, "auto_box"
            ).on_value_change(self._on_auto_box_toggle)
            ui.number("lowpass (Å)", value=self.shape_lowpass, step=5, min=0, placeholder="—").bind_value(
                self, "shape_lowpass"
            ).props("dense outlined").classes("w-28")
            ui.button("generate", on_click=self._gen_shape).props(
                "unelevated dense color=primary no-caps"
            )
        ui.label("Writes _white.mrc + _black.mrc; registers both polarities as new entries.").classes(_HINT_CLS)

    def _render_pdb_emdb_form(self) -> None:
        with ui.column().classes("w-full gap-2"):
            with ui.row().classes("w-full gap-2 items-end"):
                ui.label("PDB").classes(_LABEL_CLS + " w-10 shrink-0")
                ui.input(placeholder="6Z6J").bind_value(self, "pdb_input_val").props("dense outlined").classes("w-24")
                ui.number("apix (Å)", value=self.pdb_pixel_size, step=0.1, min=0).bind_value(
                    self, "pdb_pixel_size"
                ).props("dense outlined").classes("w-24")
                ui.number("box (px)", value=self.pdb_box_size, step=32, min=32).bind_value(
                    self, "pdb_box_size"
                ).props("dense outlined").classes("w-24")
                ui.number("resolution (Å)", value=self.pdb_lowpass, step=2, min=0, placeholder="10").bind_value(
                    self, "pdb_lowpass"
                ).props("dense outlined").classes("w-32")
                ui.button("fetch & simulate", on_click=self._fetch_and_simulate_pdb).props(
                    "unelevated dense color=primary no-caps"
                )

            with ui.row().classes("w-full gap-2 items-end"):
                ui.label("EMDB").classes(_LABEL_CLS + " w-10 shrink-0")
                ui.input(placeholder="30210").bind_value(self, "emdb_input_val").props("dense outlined").classes(
                    "w-24"
                )
                ui.number("apix (Å)", value=self.emdb_pixel_size, step=0.1, min=0).bind_value(
                    self, "emdb_pixel_size"
                ).props("dense outlined").classes("w-24")
                ui.number("box (px)", value=self.emdb_box_size, step=32, min=32).bind_value(
                    self, "emdb_box_size"
                ).props("dense outlined").classes("w-24")
                ui.number("lowpass (Å)", value=self.emdb_lowpass, step=5, min=0, placeholder="—").bind_value(
                    self, "emdb_lowpass"
                ).props("dense outlined").classes("w-28")
                ui.button("fetch & resample", on_click=self._fetch_and_resample_emdb).props(
                    "unelevated dense color=primary no-caps"
                )

    def _render_import_form(self) -> None:
        with ui.row().classes("w-full gap-2 items-center"):
            ui.label(
                "Pick an existing .mrc. The inspection dialog reads the header, asks you to confirm "
                "metadata, copies the file into the project, and appends it as a new template."
            ).classes(_HINT_CLS + " flex-1")
            ui.button("open import dialog", on_click=self._open_import_dialog).props(
                "unelevated dense color=primary no-caps"
            )

    # ── Edit Current — discrete action sections ──────────────────────

    def _render_edit_current_form(self) -> None:
        sp = self._get_species()
        sel = sp.get_selected_template() if sp else None
        if sel is None:
            with ui.row().classes("w-full items-center gap-2"):
                ui.icon("info_outline", size="12px").classes("text-gray-400")
                ui.label("Select a template above first.").classes(_HINT_CLS)
            return

        # Selected-template chip (read-only)
        with ui.row().classes("w-full items-center gap-2"):
            ui.icon("category", size="12px").classes("text-indigo-500")
            ui.label("operating on").classes(_HINT_CLS)
            ui.label(os.path.basename(sel.template_path)).classes(_MONO_CLS)
            self._polarity_chip(sel.polarity)

        # ── Action: Resample ──
        self._render_action_section(
            title="Resample",
            description="Rewrite at a new pixel size / box. Useful when the template apix doesn't match your tomos.",
            inputs_builder=lambda: self._render_resample_inputs(),
            on_click=self._resample_current,
            button_label="resample",
        )

        # ── Action: Apply lowpass ──
        self._render_action_section(
            title="Apply lowpass",
            description="Re-filter at a new resolution (Å). Keeps the same apix / box.",
            inputs_builder=lambda: self._render_lowpass_inputs(),
            on_click=self._apply_lowpass_to_current,
            button_label="apply",
        )

        # ── Action: Flip polarity ──
        self._render_action_section(
            title="Flip polarity",
            description=(
                "Write a negated copy (white → black or vice versa). Skipped if the flipped sibling exists."
            ),
            inputs_builder=None,
            on_click=self._flip_polarity,
            button_label="flip",
        )

    def _render_action_section(
        self,
        *,
        title: str,
        description: str,
        inputs_builder,
        on_click,
        button_label: str,
    ) -> None:
        """A simple action block: title, hint, inputs row, button. No
        nested cards / borders — just a horizontal rule + spacing."""
        ui.separator().classes("my-1 opacity-40")
        with ui.column().classes("w-full gap-1"):
            with ui.row().classes("w-full items-baseline gap-2"):
                ui.label(title).classes(_LABEL_CLS)
                ui.label(description).classes(_HINT_CLS)
            with ui.row().classes("w-full gap-2 items-end"):
                if inputs_builder is not None:
                    inputs_builder()
                ui.button(button_label, on_click=on_click).props(
                    "unelevated dense color=primary no-caps"
                )

    def _render_resample_inputs(self) -> None:
        ui.number("target apix (Å)", value=self.resample_target_apix, step=0.1, min=0).bind_value(
            self, "resample_target_apix"
        ).props("dense outlined").classes("w-32")
        ui.number("target box (px)", value=self.resample_target_box, step=32, min=32).bind_value(
            self, "resample_target_box"
        ).props("dense outlined").classes("w-32")
        ui.number("lowpass (Å)", value=self.resample_lowpass, step=5, min=0, placeholder="—").bind_value(
            self, "resample_lowpass"
        ).props("dense outlined").classes("w-28")

    def _render_lowpass_inputs(self) -> None:
        ui.number("target lowpass (Å)", value=self.lowpass_target, step=5, min=0).bind_value(
            self, "lowpass_target"
        ).props("dense outlined").classes("w-32")

    # ------------------------------------------------------------------
    # 4. MASKS — selectable cards (same visual rules, purple accent)
    # ------------------------------------------------------------------

    def _render_masks_section(self) -> None:
        with ui.column().classes("w-full gap-1"):
            with ui.row().classes("w-full items-baseline gap-2 px-1"):
                ui.label("MASKS").classes(_LABEL_CLS)
                self._mask_source_label = ui.label("").classes(_HINT_CLS)
                self._update_mask_source_label()
            self._masks_card_container = ui.row().classes("w-full gap-2 flex-wrap")
            with self._masks_card_container:
                self._render_mask_cards()

            with ui.tabs().props(_TAB_PROPS_PURPLE).classes("w-full") as mtabs:
                tab_sphere = ui.tab(name="sphere", label="Sphere")
                tab_relion = ui.tab(name="relion", label="RELION Mask")
                tab_import = ui.tab(name="import", label="Import")
            with ui.tab_panels(mtabs, value=tab_sphere).classes("w-full"):
                with ui.tab_panel(tab_sphere).classes("px-2 py-2"):
                    self._render_spherical_mask_form()
                with ui.tab_panel(tab_relion).classes("px-2 py-2"):
                    self._render_relion_mask_form()
                with ui.tab_panel(tab_import).classes("px-2 py-2"):
                    self._render_mask_import_form()

    def _render_mask_cards(self) -> None:
        sp = self._get_species()
        if sp is None:
            return
        masks = list(sp.masks)
        if not masks:
            self._render_empty_state("No masks yet — create one below.")
            return
        sid = sp.selected_mask_id
        for m in masks:
            self._render_mask_card(m, selected=(m.id == sid))

    def _render_mask_card(self, mask: TemplateMask, *, selected: bool) -> None:
        h = read_template_header(mask.mask_path)
        border_color = _PURPLE if selected else "#e5e7eb"
        with ui.card().tight().classes("overflow-hidden cursor-pointer").style(
            f"border: 1px solid #e5e7eb; border-left: 3px solid {border_color}; "
            f"width: {_CARD_W}px; background: white; box-shadow: none;"
        ).on("click", lambda i=mask.id: self._select_mask(i)):
            with ui.column().classes("p-2 gap-1 w-full min-w-0"):
                self._render_card_header_row(
                    file_path=mask.mask_path,
                    on_delete=lambda i=mask.id: self._request_delete_mask(i),
                    on_load_to_viewer=lambda p=mask.mask_path: self._load_to_viewer(p, kind="mask"),
                )
                with ui.row().classes("items-center gap-1 w-full min-w-0"):
                    self._method_chip(mask.method)
                    ui.label(self._format_header_summary(h, None)).classes(_MONO_CLS + " truncate flex-1 min-w-0")
                    self._render_size_badge(mask.mask_path)
                with ui.row().classes("items-center gap-1 w-full min-w-0"):
                    self._render_stats_row(h)
                knob_parts: list[str] = []
                if mask.threshold is not None:
                    knob_parts.append(f"thr {mask.threshold:g}")
                if mask.extend_pixels is not None:
                    knob_parts.append(f"ext {mask.extend_pixels:g}")
                if mask.soft_edge_pixels is not None:
                    knob_parts.append(f"soft {mask.soft_edge_pixels:g}")
                if knob_parts:
                    ui.label(" · ".join(knob_parts)).classes(_HINT_CLS + " truncate w-full min-w-0")

    def _update_mask_source_label(self) -> None:
        if self._mask_source_label is None:
            return
        sp = self._get_species()
        sel_tpl = sp.get_selected_template() if sp else None
        if sel_tpl is None:
            self._mask_source_label.set_text("(select a template first — masks derive from it)")
        else:
            self._mask_source_label.set_text(f"derive new masks from {os.path.basename(sel_tpl.template_path)}")

    def _render_spherical_mask_form(self) -> None:
        # Default diameter from species; user can still override per-mask.
        sp = self._get_species()
        if self.sphere_diameter_ang is None:
            self.sphere_diameter_ang = float(getattr(sp, "diameter_ang", None) or 0.0) or None

        with ui.row().classes("w-full gap-2 items-end"):
            ui.number(
                "diameter (Å)", value=self.sphere_diameter_ang, step=10, min=0,
                placeholder="from species" if sp and sp.diameter_ang else "e.g. 250",
            ).bind_value(self, "sphere_diameter_ang").props("dense outlined").classes("w-32")
            ui.number("soft edge (px)", value=self.sphere_soft_edge, step=1, min=0).bind_value(
                self, "sphere_soft_edge"
            ).props("dense outlined").classes("w-28")
            ui.button("create sphere", on_click=self._create_spherical_mask).bind_enabled_from(
                self, "masking_active", backward=lambda x: not x
            ).props("unelevated dense color=primary no-caps")
        ui.label(
            "Solid soft-edged sphere centered in the box, sized to the species's particle "
            "diameter. Recommended for VLPs and globular particles where a threshold-derived "
            "mask leaves a hollow lumen."
        ).classes(_HINT_CLS)

    def _render_relion_mask_form(self) -> None:
        with ui.row().classes("w-full gap-2 items-end"):
            ui.select(
                ["flexible_bounds", "otsu", "isodata", "li", "yen"],
                value=self.threshold_method,
                label="threshold method",
            ).props("dense outlined").classes("w-40").on_value_change(self._on_threshold_method_changed)
            ui.number("threshold", format="%.4f").bind_value(self, "mask_threshold").props(
                "dense outlined"
            ).classes("w-24")
            ui.number("ext (px)").bind_value(self, "mask_extend").props("dense outlined").classes("w-20")
            ui.number("soft (px)").bind_value(self, "mask_soft_edge").props("dense outlined").classes("w-20")
            ui.number("lowpass (Å)").bind_value(self, "mask_lowpass").props("dense outlined").classes("w-24")
            ui.button("create mask", on_click=self._create_relion_mask).bind_enabled_from(
                self, "masking_active", backward=lambda x: not x
            ).props("unelevated dense color=primary no-caps")
        ui.label(
            "Built from the currently selected template via relion_mask_create. "
            "If a black template is selected, its white sibling is used for thresholding."
        ).classes(_HINT_CLS)

    def _render_mask_import_form(self) -> None:
        with ui.row().classes("w-full gap-2 items-center"):
            ui.label("Pick an existing mask MRC; appended as a new mask entry.").classes(_HINT_CLS + " flex-1")
            ui.button("pick mask file", on_click=self._import_mask).props(
                "unelevated dense color=primary no-caps"
            )

    # ------------------------------------------------------------------
    # 5. VIEWER
    # ------------------------------------------------------------------

    def _render_viewer_panel(self) -> None:
        with ui.column().classes("w-full gap-1"):
            with ui.row().classes("w-full items-baseline gap-2 px-1"):
                ui.label("VIEWER").classes(_LABEL_CLS)
                ui.element("div").classes("flex-1")
                ui.label("mode").classes(_HINT_CLS)
                toggle = (
                    ui.select(
                        options={"molstar": "molstar (3D)", "slice": "slice (fallback)"},
                        value=self.viewer_mode,
                    )
                    .props("dense outlined")
                    .classes("w-40")
                )
                toggle.on_value_change(self._on_viewer_mode_changed)

            with ui.card().tight().classes("w-full overflow-hidden").style(
                "border: 1px solid #e5e7eb; box-shadow: none;"
            ):
                self._molstar_panel = ui.row().classes("w-full gap-0").style("height: 380px;")
                with self._molstar_panel:
                    with ui.column().classes("w-44 p-2 border-r bg-white h-full"):
                        with ui.row().classes("items-center gap-1 mb-1"):
                            ui.icon("layers", size="11px").classes("text-gray-400")
                            ui.label("in viewer").classes(_LABEL_CLS)
                            ui.element("div").classes("flex-1")
                            sweep_btn = ui.button(
                                icon="delete_sweep", on_click=self._sweep_viewer
                            ).props("flat round dense size=xs color=grey")
                            sweep_btn.tooltip("Clear loaded items")
                            reset_btn = ui.button(
                                icon="restart_alt", on_click=self._hard_reset_viewer
                            ).props("flat round dense size=xs color=grey")
                            reset_btn.tooltip("Reset viewer (remount — use if loads jam)")
                        self._pending_loads_container = ui.column().classes("w-full gap-1")
                        self._session_list_container = ui.column().classes("w-full gap-1 overflow-y-auto flex-1")
                    with ui.column().classes("flex-1 bg-black relative overflow-hidden h-full"):
                        ui.element("iframe").props(
                            f'src="/molstar-workbench" id="{self._iframe_id}"'
                        ).classes("absolute inset-0 w-full h-full border-none")

                self._slice_panel = ui.column().classes("w-full p-2")
                with self._slice_panel:
                    sp = self._get_species()
                    sel_t = sp.get_selected_template() if sp else None
                    sel_m = sp.get_selected_mask() if sp else None
                    tpath = sel_t.template_path if sel_t else ""
                    mpath = sel_m.mask_path if sel_m else None
                    self._slice_controller = render_template_viewer(tpath, mpath, height_px=260)

                self._apply_viewer_mode_visibility()

    def _on_viewer_mode_changed(self, e) -> None:
        self.viewer_mode = e.value or "molstar"
        self._apply_viewer_mode_visibility()
        # Slice viewer reflects selection (it's cheap, Python-side).
        # Molstar mode does NOT auto-load — user clicks the eye icon.
        if self.viewer_mode == "slice":
            self._refresh_viewer()

    def _apply_viewer_mode_visibility(self) -> None:
        if self._molstar_panel:
            self._molstar_panel.set_visibility(self.viewer_mode == "molstar")
        if self._slice_panel:
            self._slice_panel.set_visibility(self.viewer_mode == "slice")

    # ------------------------------------------------------------------
    # LOG
    # ------------------------------------------------------------------

    def _render_log_panel(self) -> None:
        with ui.expansion("Activity log", icon="terminal").classes("w-full bg-gray-50 rounded").props("dense"):
            self._log_container = ui.column().classes("w-full gap-0.5 px-2 py-1 max-h-40 overflow-y-auto")

    def _log(self, msg: str) -> None:
        if self._log_container is not None:
            with self._log_container:
                ui.label(f"• {msg}").classes(_MONO_CLS + " leading-tight")

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
            dims = [float(x) for x in self.basic_shape_def.split(":")]
            new_box = max(int(((max(dims) / self.shape_pixel_size) * 1.3 + 31) // 32) * 32, 96)
            self.shape_box_size = new_box
        except Exception:
            pass

    # ==================================================================
    # GENERATION FLOWS (idempotent — file paths are canonical)
    # ==================================================================

    async def _gen_shape(self) -> None:
        if self.auto_box:
            self._recalculate_auto_box_for_shape()
        self._save_workbench_ui()
        lp = self.shape_lowpass if (self.shape_lowpass and self.shape_lowpass > 0) else None
        self._log(f"Ellipsoid {self.basic_shape_def} @ {self.shape_pixel_size}Å/px lp={lp}")
        res = await self.backend.template_service.generate_basic_shape_async(
            self.basic_shape_def,
            self.shape_pixel_size,
            self.output_folder,
            int(self.shape_box_size),
            lp,
        )
        if not res.get("success"):
            self._log(f"Generation failed: {res.get('error')}")
            ui.notify(f"Generation failed: {res.get('error')}", type="negative")
            return
        self._register_polarity_pair(res, source=f"basic_shape:{self.basic_shape_def}", lowpass=lp)

    async def _fetch_and_simulate_pdb(self) -> None:
        if not self.pdb_input_val:
            ui.notify("Enter a PDB ID first", type="warning")
            return
        pdb_id = self.pdb_input_val.strip().lower()
        self._log(f"Fetching PDB: {pdb_id}")
        fetch = await self.backend.template_service.fetch_pdb_async(pdb_id, self.output_folder)
        if not fetch.get("success"):
            self._log(f"Fetch failed: {fetch.get('error')}")
            ui.notify(f"PDB fetch failed: {fetch.get('error')}", type="negative")
            return
        pdb_path = fetch.get("path", "")
        self._log(f"Simulating from {os.path.basename(pdb_path)}…")
        lp = self.pdb_lowpass if (self.pdb_lowpass and self.pdb_lowpass > 0) else 10.0
        n = ui.notification("Simulating density…", type="ongoing", spinner=True, timeout=None)
        try:
            sim = await self.backend.pdb_service.simulate_map_from_pdb(
                pdb_path=pdb_path,
                output_folder=self.output_folder,
                target_apix=self.pdb_pixel_size,
                target_box=int(self.pdb_box_size),
                resolution=lp,
            )
        finally:
            n.dismiss()
        if not sim.get("success"):
            self._log(f"Simulation failed: {sim.get('error')}")
            ui.notify("Simulation failed (see log)", type="negative", timeout=8000)
            return
        self._register_polarity_pair(sim, source=f"PDB:{pdb_id}", lowpass=lp)

    async def _fetch_and_resample_emdb(self) -> None:
        if not self.emdb_input_val:
            ui.notify("Enter an EMDB ID first", type="warning")
            return
        emdb_id = self.emdb_input_val.strip()
        self._log(f"Fetching EMDB: {emdb_id}")
        fetch = await self.backend.template_service.fetch_emdb_map_async(emdb_id, self.output_folder)
        if not fetch.get("success"):
            self._log(f"Fetch failed: {fetch.get('error')}")
            ui.notify(f"EMDB fetch failed: {fetch.get('error')}", type="negative")
            return
        map_path = fetch.get("path", "")
        self._log(f"Resampling {os.path.basename(map_path)}…")
        lp = self.emdb_lowpass if (self.emdb_lowpass and self.emdb_lowpass > 0) else None
        res = await self.backend.template_service.process_volume_async(
            map_path,
            self.output_folder,
            self.emdb_pixel_size,
            int(self.emdb_box_size),
            lp,
        )
        if not res.get("success"):
            self._log(f"Resample failed: {res.get('error')}")
            ui.notify("Resample failed (see log)", type="negative")
            return
        self._register_polarity_pair(res, source=f"EMDB-{emdb_id}", lowpass=lp)

    def _register_polarity_pair(
        self, res: dict, *, source: str, lowpass: Optional[float], select_new_white: bool = True
    ) -> None:
        """Register a (white, black) pair from a generation result.

        `select_new_white=True` (default) flips the species's selection to
        the newly-registered white entry. This is the right behavior for
        Edit-Current → Resample / Apply lowpass — the user just produced
        a new template *intending* it to be the active one, and the next
        op (mask creation, TM, etc.) must run against it, not the stale
        previously-selected entry. Pass False if the caller really wants
        to preserve selection (no current use case)."""
        white = res.get("path_white")
        black = res.get("path_black")
        appended: list[str] = []
        new_white_id: Optional[str] = None
        for path, pol in ((white, "white"), (black, "black")):
            if path and os.path.exists(path):
                tid = self._append_template(path, pol, source, lowpass=lowpass)
                if pol == "white":
                    new_white_id = tid
                appended.append(os.path.basename(path))
        self._log(f"Registered: {', '.join(appended) if appended else '(nothing)'}")
        if select_new_white and new_white_id:
            self._select_template(new_white_id)

    async def _open_import_dialog(self) -> None:
        sp = self._get_species()
        if sp is None:
            ui.notify("Species not found", type="warning")
            return
        tpl = await open_template_import_dialog(self.project_path, sp)
        if tpl is None:
            return

        def _apply(s: ParticleSpecies) -> None:
            existing_idx = next(
                (i for i, t in enumerate(s.templates) if t.template_path == tpl.template_path), None
            )
            if existing_idx is not None:
                tpl.id = s.templates[existing_idx].id
                s.templates[existing_idx] = tpl
            else:
                s.templates.append(tpl)
            if not s.selected_template_id:
                s.selected_template_id = tpl.id

        self._mutate_species(_apply)
        asyncio.create_task(self._after_register())
        self._log(f"Imported: {os.path.basename(tpl.template_path)}")

    # ── Edit-Current actions (idempotent) ─────────────────────────────

    async def _resample_current(self) -> None:
        sp = self._get_species()
        sel = sp.get_selected_template() if sp else None
        if sel is None:
            ui.notify("Select a template first", type="warning")
            return
        target_apix = self.resample_target_apix
        target_box = int(self.resample_target_box)
        lp = self.resample_lowpass if (self.resample_lowpass and self.resample_lowpass > 0) else None
        self._log(f"Resampling {os.path.basename(sel.template_path)} → apix {target_apix}Å box {target_box}…")
        n = ui.notification("Resampling template…", type="ongoing", spinner=True, timeout=None)
        try:
            res = await self.backend.template_service.process_volume_async(
                sel.template_path,
                self.output_folder,
                target_apix,
                target_box,
                lp,
            )
        finally:
            n.dismiss()
        if not res.get("success"):
            self._log(f"Resample failed: {res.get('error')}")
            ui.notify("Resample failed (see log)", type="negative")
            return
        self._register_polarity_pair(res, source=f"resampled from {os.path.basename(sel.template_path)}", lowpass=lp)

    async def _apply_lowpass_to_current(self) -> None:
        sp = self._get_species()
        sel = sp.get_selected_template() if sp else None
        if sel is None:
            ui.notify("Select a template first", type="warning")
            return
        if not self.lowpass_target or self.lowpass_target <= 0:
            ui.notify("Set a positive lowpass target", type="warning")
            return
        h = read_template_header(sel.template_path)
        # Keep the original apix/box; only the lowpass changes.
        target_apix = h.apix_ang or self.shape_pixel_size
        target_box = h.box_px or int(self.shape_box_size)
        target_lp = float(self.lowpass_target)
        self._log(f"Applying lowpass {target_lp}Å to {os.path.basename(sel.template_path)}…")
        n = ui.notification("Applying lowpass…", type="ongoing", spinner=True, timeout=None)
        try:
            res = await self.backend.template_service.process_volume_async(
                sel.template_path,
                self.output_folder,
                target_apix,
                target_box,
                target_lp,
            )
        finally:
            n.dismiss()
        if not res.get("success"):
            self._log(f"Lowpass failed: {res.get('error')}")
            ui.notify("Lowpass failed (see log)", type="negative")
            return
        self._register_polarity_pair(
            res, source=f"lowpass({target_lp:g}Å) from {os.path.basename(sel.template_path)}", lowpass=target_lp
        )

    async def _flip_polarity(self) -> None:
        sp = self._get_species()
        sel = sp.get_selected_template() if sp else None
        if sel is None:
            ui.notify("Select a template first", type="warning")
            return
        target_polarity = "black" if sel.polarity == "white" else "white"
        target_path = self._canonical_flipped_path(sel.template_path, sel.polarity, target_polarity)

        # Dedup: if a registered entry already exists at the canonical
        # flipped path, just select it. If the file exists on disk
        # without a registry entry, register and select. Otherwise write.
        existing_entry = next((t for t in sp.templates if t.template_path == target_path), None)
        if existing_entry is not None:
            self._select_template(existing_entry.id)
            self._log(f"Already exists — selected {os.path.basename(target_path)}")
            return
        if os.path.exists(target_path):
            new_id = self._append_template(
                target_path,
                target_polarity,
                sel.source or f"flipped from {os.path.basename(sel.template_path)}",
                lowpass=sel.lowpass_resolution_ang,
            )
            self._select_template(new_id)
            self._log(f"Registered existing sibling {os.path.basename(target_path)}")
            return

        success = await asyncio.to_thread(self._negate_volume_to_disk_at, sel.template_path, target_path)
        if not success:
            ui.notify("Polarity flip failed (see log)", type="negative")
            return
        new_id = self._append_template(
            target_path,
            target_polarity,
            f"flipped from {os.path.basename(sel.template_path)}",
            lowpass=sel.lowpass_resolution_ang,
        )
        self._select_template(new_id)
        self._log(f"Flipped → {os.path.basename(target_path)}")

    def _canonical_flipped_path(self, src_path: str, cur: str, target: str) -> str:
        """Return the canonical path for the flipped sibling. If the source
        ends in _white/_black, swap. Otherwise append the target polarity
        as a suffix. Used for dedup."""
        if cur in ("white", "black") and target in ("white", "black"):
            from_token = f"_{cur}.mrc"
            to_token = f"_{target}.mrc"
            if src_path.endswith(from_token):
                return src_path[: -len(from_token)] + to_token
        stem = Path(src_path).stem
        for tok in ("_white", "_black"):
            if stem.endswith(tok):
                stem = stem[: -len(tok)]
                break
        return os.path.join(os.path.dirname(src_path), f"{stem}_{target}.mrc")

    def _negate_volume_to_disk_at(self, src_path: str, out_path: str) -> bool:
        """Write -1×src to out_path. Refuses to overwrite an existing
        file (dedup happens in the caller). Returns True on success."""
        if os.path.exists(out_path):
            logger.info("Refusing to overwrite existing %s", out_path)
            return False
        try:
            import numpy as np

            with mrcfile.open(src_path, mode="r", permissive=True) as m:
                data = np.array(m.data, copy=True)
                vsize = float(getattr(m.voxel_size, "x", 0.0) or 0.0)
            with mrcfile.new(out_path, overwrite=False) as m:
                m.set_data((-data).astype(np.float32))
                if vsize > 0:
                    m.voxel_size = vsize
            return True
        except Exception as e:
            logger.exception("Negate volume failed: %s", e)
            return False

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

    async def _create_relion_mask(self) -> None:
        sp = self._get_species()
        sel = sp.get_selected_template() if sp else None
        if sel is None or not sel.template_path:
            ui.notify("Select a template first", type="warning")
            return

        self.masking_active = True
        n = ui.notification("Creating mask…", type="ongoing", spinner=True, timeout=None)
        try:
            input_vol = sel.template_path
            if sel.template_path.endswith("_black.mrc"):
                cand = sel.template_path[: -len("_black.mrc")] + "_white.mrc"
                if os.path.exists(cand):
                    input_vol = cand
            threshold = float(self.mask_threshold)
            self._log(f"Mask from {os.path.basename(input_vol)} threshold={threshold}")

            base = Path(sel.template_path).stem.replace("_white", "").replace("_black", "")
            # Canonical mask path includes threshold knobs so identical-knob
            # runs are idempotent on disk (same path, gets overwritten).
            output_path = os.path.join(
                self.output_folder,
                f"{base}_mask_t{threshold:.3f}_e{int(self.mask_extend)}_s{int(self.mask_soft_edge)}.mrc",
            )

            res = await self.backend.template_service.create_mask_relion(
                input_vol,
                output_path,
                threshold,
                float(self.mask_extend),
                float(self.mask_soft_edge),
                float(self.mask_lowpass),
            )
            if not res.get("success"):
                self._log(f"Mask failed: {res.get('error')}")
                ui.notify(f"Mask failed: {res.get('error')}", type="negative")
                return

            self._log(f"Mask: {os.path.basename(output_path)}")
            self._append_mask(
                TemplateMask(
                    mask_path=output_path,
                    method="relion",
                    threshold=threshold,
                    extend_pixels=float(self.mask_extend),
                    soft_edge_pixels=float(self.mask_soft_edge),
                    lowpass_ang=float(self.mask_lowpass),
                    derived_from_template_id=sel.id,
                )
            )
        finally:
            n.dismiss()
            self.masking_active = False

    async def _create_spherical_mask(self) -> None:
        """Write a soft-edged sphere matching the selected template's
        grid (apix + box). Diameter from the species field (or the form
        if user overrode). The mask is then registered as method='spherical'."""
        sp = self._get_species()
        sel = sp.get_selected_template() if sp else None
        if sel is None or not sel.template_path or not os.path.exists(sel.template_path):
            ui.notify("Select a template first — sphere matches its grid", type="warning")
            return

        diameter = float(self.sphere_diameter_ang or 0.0)
        if diameter <= 0:
            sp_diam = float(getattr(sp, "diameter_ang", None) or 0.0)
            if sp_diam > 0:
                diameter = sp_diam
        if diameter <= 0:
            ui.notify("Set a particle diameter in the species header (or in this form)", type="warning")
            return

        header = read_template_header(sel.template_path)
        if not header.apix_ang or not header.box_px:
            ui.notify("Selected template has no apix/box in header", type="warning")
            return
        soft = float(self.sphere_soft_edge or 0.0)

        # Canonical mask path: <stem>_sphere_d<diameter>_s<soft>.mrc. Idempotent
        # for identical inputs (overwritten by template_service).
        base = Path(sel.template_path).stem.replace("_white", "").replace("_black", "")
        out_name = f"{base}_sphere_d{int(round(diameter))}_s{int(round(soft))}.mrc"
        output_path = os.path.join(self.output_folder, out_name)

        self.masking_active = True
        n = ui.notification("Creating spherical mask…", type="ongoing", spinner=True, timeout=None)
        try:
            self._log(f"Sphere d={diameter:g}Å soft={soft:g}px apix={header.apix_ang:.3g} box={header.box_px}")
            res = await self.backend.template_service.create_spherical_mask_async(
                output_path=output_path,
                apix_ang=float(header.apix_ang),
                box_px=int(header.box_px),
                diameter_ang=diameter,
                soft_edge_pixels=soft,
            )
            if not res.get("success"):
                self._log(f"Sphere failed: {res.get('error')}")
                ui.notify(f"Sphere failed: {res.get('error')}", type="negative")
                return
            self._log(f"Sphere: {os.path.basename(output_path)}")
            self._append_mask(
                TemplateMask(
                    mask_path=output_path,
                    method="spherical",
                    soft_edge_pixels=soft,
                    derived_from_template_id=sel.id,
                )
            )
        finally:
            n.dismiss()
            self.masking_active = False

    async def _import_mask(self) -> None:
        picker = local_file_picker("/", upper_limit=None, mode="file")
        result = await picker
        if not result or not result[0]:
            return
        picked = result[0]
        if Path(picked).suffix.lower() not in (".mrc", ".map", ".rec", ".ccp4"):
            ui.notify("Mask must be an MRC family file", type="warning")
            return
        self._append_mask(TemplateMask(mask_path=picked, method="imported"))
        self._log(f"Mask imported: {picked}")

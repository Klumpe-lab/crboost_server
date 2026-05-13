"""Import an external .mrc file as the species's template.

The "boss handed me an .mrc and said use this" workflow. Implements the
register-external flow per the v2 plan: a file is not a template until we
have inspected it, presented the metadata to the user, accepted their
confirmation/edits, and ingested it into the project's
templates/<species_id>/ directory with a populated `ParticleTemplate`
record.

Returns the new `ParticleTemplate` (caller writes it to the species and
saves project state). Returns None on cancel.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from nicegui import ui

from services.project_state import ParticleTemplate, ParticleSpecies, sidecar_ensure
from services.templating.mrc_inspection import MrcInspection, inspect_mrc_for_import
from ui.local_file_picker import local_file_picker

logger = logging.getLogger(__name__)


async def open_template_import_dialog(
    project_path: str,
    species: ParticleSpecies,
    initial_path: Optional[str] = None,
) -> Optional[ParticleTemplate]:
    """Open the import dialog. Returns a populated ParticleTemplate on
    confirm (the file has already been copied into the project), or None
    on cancel. The caller writes the result to species.template and
    saves project state."""

    dialog = ui.dialog().props("persistent")
    state = {
        "selected_path": initial_path or "",
        "inspection": None,  # MrcInspection or None
        # Editable form fields (pre-filled from inspection on first load):
        "polarity": "black",
        "source": "",
        "notes": "",
        "lowpass_ang": "",  # string for ui.input; float-or-empty
        "result": None,  # ParticleTemplate set on confirm
    }

    refs: dict = {}

    def _build_dialog():
        dialog.clear()
        with dialog, ui.card().classes("p-0").style("width: 720px; max-width: 95vw;"):
            # Header
            with ui.row().classes("w-full items-center px-4 py-3 bg-gray-50 border-b gap-2"):
                ui.icon("file_upload", size="20px").classes("text-gray-500")
                ui.label("Import template").classes("text-base font-semibold text-gray-800")
                ui.label(f"→ species: {species.name}").classes("text-xs text-gray-500 ml-2")

            # Info banner — imports always ADD a new entry to species.templates
            # in v3 (selection unchanged unless the user opts in below).
            existing_count = len(getattr(species, "templates", []) or [])
            if existing_count > 0:
                with ui.row().classes("w-full items-center px-4 py-2 bg-blue-50 border-b border-blue-100 gap-2"):
                    ui.icon("info", size="16px").classes("text-blue-600")
                    ui.label(
                        f"This will be added as a new entry alongside the {existing_count} template"
                        f"{'s' if existing_count != 1 else ''} already registered for this species."
                    ).classes("text-xs text-blue-800")

            # File path row
            with ui.row().classes("w-full items-center px-4 py-3 gap-2 border-b"):
                ui.label("File").classes("text-xs font-bold text-gray-500 uppercase w-12 shrink-0")
                refs["path_input"] = (
                    ui.input(value=state["selected_path"], placeholder="/path/to/template.mrc")
                    .props("dense outlined")
                    .classes("flex-1 font-mono text-xs")
                )
                refs["path_input"].on(
                    "blur",
                    lambda e: _try_inspect(refs["path_input"].value or ""),
                )
                ui.button("Browse…", icon="folder_open", on_click=_browse).props("dense no-caps")

            # Analysis section
            refs["analysis"] = ui.column().classes("w-full px-4 py-3 gap-3")
            _render_analysis()

            # Footer buttons
            with ui.row().classes("w-full justify-end items-center px-4 py-3 bg-gray-50 border-t gap-2"):
                ui.button("Cancel", on_click=lambda: dialog.submit(None)).props("flat no-caps")
                btn = ui.button("Import", icon="check", on_click=_confirm_import)
                btn.props("unelevated no-caps color=primary")
                refs["import_btn"] = btn
                if state["inspection"] is None:
                    refs["import_btn"].disable()

    async def _browse():
        # The picker lives one project-root level up so the user can grab
        # files from anywhere on the cluster they have read access to.
        # No upper_limit — the user might point at someone else's project.
        picker = local_file_picker("/", upper_limit=None, mode="file")
        result = await picker
        if result and result[0]:
            picked = result[0]
            refs["path_input"].value = picked
            state["selected_path"] = picked
            await _try_inspect_async(picked)

    def _try_inspect(path: str) -> None:
        """Synchronous wrapper kept for the blur handler — schedules
        the async version so the UI can show a spinner during the read."""
        asyncio.create_task(_try_inspect_async(path))

    async def _try_inspect_async(path: str) -> None:
        path = (path or "").strip()
        state["selected_path"] = path
        if not path:
            state["inspection"] = None
            _render_analysis()
            return
        if not Path(path).exists():
            ui.notify(f"File not found: {path}", type="warning", timeout=2500)
            state["inspection"] = None
            _render_analysis()
            return
        if Path(path).suffix.lower() not in (".mrc", ".map", ".rec", ".ccp4"):
            ui.notify(
                "Unsupported file extension. MRC family expected (.mrc, .map, .rec, .ccp4).",
                type="warning",
                timeout=2500,
            )

        # Show the spinner immediately; offload the inspection to a thread
        # so the UI loop can paint it. inspect_mrc_for_import loads the full
        # volume and computes statistics — a few seconds on large MRCs.
        _render_inspecting_state()
        ins = await asyncio.to_thread(inspect_mrc_for_import, path)
        if ins is None:
            ui.notify("Could not read MRC header / data.", type="negative", timeout=3000)
            state["inspection"] = None
        else:
            state["inspection"] = ins
            # Pre-fill editable form fields from inspection
            if ins.inferred_polarity in ("white", "black"):
                state["polarity"] = ins.inferred_polarity
            # Source: prefer PDB > EMDB > tool > "imported"
            if ins.inferred_pdb_id:
                state["source"] = f"PDB:{ins.inferred_pdb_id}"
            elif ins.inferred_emdb_id:
                state["source"] = f"EMDB-{ins.inferred_emdb_id}"
            elif ins.inferred_tool:
                state["source"] = f"imported (from {ins.inferred_tool})"
            else:
                state["source"] = "imported"
            state["notes"] = ""
            state["lowpass_ang"] = ""
        _render_analysis()

    def _render_inspecting_state() -> None:
        """Loading state shown while inspect_mrc_for_import runs."""
        if "analysis" not in refs:
            return
        refs["analysis"].clear()
        with refs["analysis"]:
            with ui.row().classes("w-full items-center gap-2 px-3 py-3"):
                ui.spinner("dots", size="sm").classes("text-indigo-500")
                ui.label("Reading MRC header & analyzing volume…").classes("text-xs text-gray-600")
        if "import_btn" in refs:
            refs["import_btn"].disable()

    def _render_analysis() -> None:
        refs["analysis"].clear()
        ins: Optional[MrcInspection] = state["inspection"]
        with refs["analysis"]:
            if ins is None:
                with ui.row().classes("w-full px-2 py-3 items-center gap-2"):
                    ui.icon("info_outline", size="16px").classes("text-gray-400")
                    ui.label("Pick an .mrc to inspect.").classes("text-sm text-gray-400 italic")
                if "import_btn" in refs:
                    refs["import_btn"].disable()
                return

            # Section: file analysis (read-only)
            _render_inspection_facts(ins)

            # Mask-likeness warning (this looks like a binary mask — they
            # probably don't want to register it as a template)
            if ins.looks_like_mask:
                with ui.row().classes("w-full px-3 py-2 bg-amber-50 border border-amber-200 rounded gap-2 items-start"):
                    ui.icon("warning", size="16px").classes("text-amber-600 mt-0.5")
                    with ui.column().classes("gap-0.5"):
                        ui.label("Looks like a mask, not a template.").classes("text-xs font-semibold text-amber-800")
                        ui.label(
                            f"Bimodal distribution: ~{int(ins.mask_confidence * 100)}% of voxels are at 0 or 1. "
                            "Templates are usually density volumes, not binary masks."
                        ).classes("text-[11px] text-amber-700")

            # Non-cube hint
            if not ins.is_cube:
                with ui.row().classes("w-full px-3 py-1 bg-gray-50 rounded gap-2 items-center"):
                    ui.icon("info", size="14px").classes("text-gray-500")
                    ui.label(
                        f"Non-cube volume ({ins.nx}×{ins.ny}×{ins.nz}). PyTOM templates are typically cubic."
                    ).classes("text-[11px] text-gray-600")

            # Section: editable metadata form. These are project-bookkeeping
            # fields we attach to this template — NOT MRC header values.
            # The header values are in the "Read from header" block above.
            ui.separator().classes("my-1")
            with ui.column().classes("w-full gap-0"):
                ui.label("Template record (your project bookkeeping)").classes(
                    "text-xs font-bold text-gray-500 uppercase tracking-wider"
                )
                ui.label(
                    "Free-form fields we store alongside the template so you can recall later what "
                    "this template is, where it came from, and at what resolution it was filtered. "
                    "Doesn't affect job execution or modify the .mrc file."
                ).classes("text-[11px] text-gray-500 italic")

            # Polarity — pre-filled from inference, user can override
            with ui.row().classes("w-full items-center gap-3"):
                ui.label("Polarity").classes("text-xs text-gray-600 w-20 shrink-0 text-right")
                pol_select = (
                    ui.select(
                        options={"black": "black (protein dark)", "white": "white (protein bright)"},
                        value=state["polarity"],
                    )
                    .props("dense outlined")
                    .classes("w-72")
                )
                pol_select.on_value_change(lambda e: state.update(polarity=e.value))
                if ins.inferred_polarity == "ambiguous":
                    ui.label("(auto-detect inconclusive)").classes("text-[11px] text-amber-600 italic")
                else:
                    ui.label(
                        f"(auto-detected: {ins.inferred_polarity}, conf {ins.polarity_confidence:.2f})"
                    ).classes("text-[11px] text-gray-400 italic")

            # Source
            with ui.row().classes("w-full items-center gap-3"):
                ui.label("Source").classes("text-xs text-gray-600 w-20 shrink-0 text-right")
                src_input = (
                    ui.input(value=state["source"], placeholder="e.g. PDB:6Z6J, EMDB-1234, custom")
                    .props("dense outlined")
                    .classes("flex-1")
                )
                src_input.on_value_change(lambda e: state.update(source=e.value or ""))

            # Lowpass status
            with ui.row().classes("w-full items-center gap-3"):
                ui.label("Lowpass").classes("text-xs text-gray-600 w-20 shrink-0 text-right")
                lp_input = (
                    ui.input(value=state["lowpass_ang"], placeholder="e.g. 30 (Å) — leave blank if unfiltered")
                    .props("dense outlined")
                    .classes("w-72")
                )
                lp_input.on_value_change(lambda e: state.update(lowpass_ang=e.value or ""))
                ui.label("(Å) — what resolution is this template filtered to?").classes("text-[11px] text-gray-400")

            # Notes
            with ui.row().classes("w-full items-start gap-3"):
                ui.label("Notes").classes("text-xs text-gray-600 w-20 shrink-0 text-right pt-2")
                notes_input = (
                    ui.textarea(value=state["notes"], placeholder="Free-form notes about this template…")
                    .props("dense outlined autogrow")
                    .classes("flex-1")
                )
                notes_input.on_value_change(lambda e: state.update(notes=e.value or ""))

            # Show MRC labels if any (provenance crumbs from generating tool)
            if ins.labels:
                ui.separator().classes("my-1")
                ui.label(f"MRC header labels ({len(ins.labels)})").classes(
                    "text-xs font-bold text-gray-500 uppercase tracking-wider"
                )
                for lbl in ins.labels:
                    ui.label(lbl).classes("text-[11px] text-gray-500 font-mono pl-2")

            if "import_btn" in refs:
                refs["import_btn"].enable()

    def _confirm_import() -> None:
        ins: Optional[MrcInspection] = state["inspection"]
        if ins is None:
            return
        try:
            tpl = _ingest(project_path, species.id, ins, state)
        except Exception as e:
            logger.exception("Template import failed")
            ui.notify(f"Import failed: {e}", type="negative", timeout=4000)
            return
        state["result"] = tpl
        dialog.submit(tpl)

    _build_dialog()
    if state["selected_path"]:
        _try_inspect(state["selected_path"])
    return await dialog


def _ingest(project_path: str, species_id: str, ins: MrcInspection, state: dict) -> ParticleTemplate:
    """Copy the inspected file into templates/<species_id>/, build a
    ParticleTemplate from the inspection + user-edited form. Caller writes
    the result to species.template."""
    project_root = Path(project_path)
    species_dir = project_root / "templates" / species_id
    species_dir.mkdir(parents=True, exist_ok=True)

    src = Path(ins.path)
    dst = species_dir / src.name
    # Avoid clobbering an existing file with the same name. _2.mrc, _3.mrc, ...
    if dst.exists() and dst.resolve() != src.resolve():
        stem = dst.stem
        suffix = dst.suffix
        n = 2
        while (species_dir / f"{stem}_{n}{suffix}").exists():
            n += 1
        dst = species_dir / f"{stem}_{n}{suffix}"

    if dst.resolve() != src.resolve():
        shutil.copy2(src, dst)

    # Parse user-supplied lowpass (free-form input — accept "30", "30.0", "30 Å", "")
    lowpass_str = (state.get("lowpass_ang") or "").strip().rstrip("Å").strip()
    lowpass_val: Optional[float] = None
    if lowpass_str:
        try:
            lowpass_val = float(lowpass_str)
        except ValueError:
            lowpass_val = None

    # Ensure a sidecar exists so future filesystem scans recognise the
    # copied file as already registered with this UUID.
    entry_id = sidecar_ensure(str(dst), "template")
    return ParticleTemplate(
        id=entry_id,
        template_path=str(dst),
        polarity=state.get("polarity") or "black",
        lowpass_resolution_ang=lowpass_val,
        source=state.get("source") or "imported",
        imported_from=str(src),
        created_at=datetime.now(),
        notes=state.get("notes") or "",
    )


def _render_inspection_facts(ins: MrcInspection) -> None:
    """Read-only summary of what `inspect_mrc_for_import` extracted."""
    rows: list[tuple[str, str]] = []
    apix_str = f"{ins.apix_ang:.3g} Å/px" if ins.apix_ang else "?"
    rows.append(("apix", apix_str))
    rows.append(("dims", f"{ins.nx} × {ins.ny} × {ins.nz} ({ins.mode_name})"))
    rows.append(("box", f"{ins.box_px} px"))
    rows.append(("data range", f"min {ins.data_min:.3g} • max {ins.data_max:.3g} • mean {ins.data_mean:.3g}"))
    if ins.inferred_pdb_id:
        rows.append(("pdb", ins.inferred_pdb_id))
    if ins.inferred_emdb_id:
        rows.append(("emdb", ins.inferred_emdb_id))
    if ins.inferred_tool:
        rows.append(("tool", ins.inferred_tool))

    with ui.column().classes("w-full px-3 py-2 bg-gray-50 rounded gap-1"):
        ui.label("Read from header").classes("text-[10px] font-bold text-gray-400 uppercase tracking-wider")
        for k, v in rows:
            with ui.row().classes("w-full gap-3 items-baseline"):
                ui.label(k).classes("text-xs text-gray-500 w-20 shrink-0 text-right")
                ui.label(v).classes("text-xs text-gray-800 font-mono")

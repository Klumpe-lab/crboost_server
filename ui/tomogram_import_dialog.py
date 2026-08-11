"""PARTICLES-header tomogram-import dialog — a project-level utility, NOT a pipeline job.

Two modes, both with a metadata preview the user confirms BEFORE committing:
- **mrcs** — browse/scan a directory of reconstructed ``.mrc`` files, multi-select which
  to import; each row shows dims + pixel size, or a red chip when the MRC header has no
  voxel size (so apix is never silently defaulted to 1.0 — CLAUDE.md 'Surfacing uncertainty').
- **reference** — point at an existing ``tomograms.star``; its rows are read back into the
  same review table so the user sees what they're referencing instead of ingesting blind.

Browsing reuses the shared ``local_file_picker`` (multi-select + glob-filtered). Styling
follows the Journey/dashboard ``cb-*`` chrome (ui/dashboard/css.py). See services/tomogram_import.py.
"""

from __future__ import annotations

from pathlib import Path
from collections.abc import Callable

from nicegui import ui

from ui.dashboard.css import ensure_assets_loaded
from ui.glob_directory_input import GlobDirectoryInput
from ui.local_file_picker import local_file_picker

_MONO = "font-family: ui-monospace, monospace;"
_ACT = "color: #4f46e5;"  # indigo, for flat text actions


def _chip(label: str, value: str, *, status: str = "neutral", tooltip: str | None = None) -> None:
    """One compact status chip (mirrors the dashboard ``_render_chip``; kept local so this
    dialog doesn't pull in the heavy tomo_dashboard_dialog module). ``status`` ∈
    {ok, warn, error, info, neutral}."""
    with ui.element("span").classes(f"cb-chip cb-chip-{status}") as chip:
        ui.label(label).classes("cb-chip-label")
        ui.label(value).classes("cb-chip-value")
        if tooltip:
            chip.tooltip(tooltip)


def _start_dir(glob_or_path: str) -> str | None:
    """Best-effort starting directory for the picker, derived from a glob/path field."""
    if not glob_or_path:
        return None
    p = Path(glob_or_path)
    d = p if p.is_dir() else p.parent
    s = str(d)
    return s if s and s != "." else None


def open_tomogram_import_dialog(backend, project_path, on_done: Callable[[], None] | None = None) -> None:
    """Open the tomogram-import dialog for ``project_path``. Commits via
    ``backend.commit_imported_tomograms`` and calls ``on_done`` after a successful import."""
    ensure_assets_loaded()  # the dialog can open on pages that never mounted the dashboard
    project_path = Path(project_path)

    # The pixel-size field is a deliberate opt-in OVERRIDE, left BLANK by default: each
    # recon MRC's own header voxel size is authoritative (ts_px = recon_apix / binning), so
    # prefilling would silently win over a present, valid header. We can't prefill from the
    # project microscope apix anyway — for a particle-only project it's the model default
    # (1.35, ge=0.5, never 0), i.e. meaningless here. A file whose header truly lacks a
    # voxel size is flagged per-row (warn chip) and the commit RAISES unless the user fills
    # this in — surfaced, never silently defaulted (CLAUDE.md 'Surfacing uncertainty').

    files: list[dict] = []  # mrcs probed metadata (backend.probe_tomogram_metadata)
    selected: dict[str, bool] = {}  # mrcs path -> checked
    ref_files: list[dict] = []  # reference preview rows (backend.preview_reference_tomograms)
    row_checkboxes: dict[str, ui.checkbox] = {}  # current mrcs table checkboxes
    select_all_cb = None
    _sync = {"v": False}  # guard: programmatic select-all set must not re-fire _toggle_all

    # ── Preview table (shared by mrcs + reference) ──────────────────────────────

    def _row(f: dict, selectable: bool) -> None:
        is_err = bool(f.get("error"))
        has_dims = bool(f.get("nx"))
        tmpl = "22px minmax(0,1fr) 110px 96px" if selectable else "minmax(0,1fr) 110px 96px"
        with (
            ui.element("div")
            .classes("cb-ltable-row")
            .style(f"grid-template-columns: {tmpl}; cursor: default;") as row_el
        ):
            if selectable:
                is_on = selected.get(f["path"], True)
                cb = ui.checkbox(
                    value=is_on, on_change=lambda e, p=f["path"], r=row_el: _on_toggle(p, bool(e.value), r)
                ).props("dense")
                row_checkboxes[f["path"]] = cb
                if is_on:
                    row_el.classes("selected")
            ui.label(f["name"]).classes("cb-ltable-name").tooltip(f.get("path") or f["name"])
            if has_dims:
                ui.label(f"{f['nx']}×{f['ny']}×{f['nz']}").classes("cb-ltable-cell").style(
                    f"{_MONO} font-size: 11px; color: #64748b;"
                )
            else:
                ui.label("—").classes("cb-ltable-cell").style(f"{_MONO} font-size: 11px; color: #cbd5e1;")
            with ui.element("div").classes("cb-ltable-cell"):
                if is_err:
                    _chip("file", "unreadable", status="error", tooltip=f.get("error") or "Not a readable MRC.")
                elif f.get("has_voxel_size"):
                    # Pixel size is independent of dims — a reference star may carry apix but
                    # no rlnTomoSize* columns, so don't gate this on has_dims.
                    ui.label(f"{f['voxel_size']:.3g} Å/px").style(f"{_MONO} font-size: 11px; color: #64748b;")
                elif has_dims or f.get("path"):
                    _chip(
                        "apix",
                        "missing",
                        status="warn",
                        tooltip="No pixel size — set 'Pixel size (Å)' below before importing, "
                        "or picks on this tomogram will be mis-scaled.",
                    )
                else:
                    ui.label("—").style(f"{_MONO} font-size: 11px; color: #cbd5e1;")

    def _render_table(box, items: list[dict], selectable: bool) -> None:
        nonlocal select_all_cb
        box.clear()
        if selectable:
            row_checkboxes.clear()
        with box:
            if not items:
                ui.label("Nothing to preview yet — scan or browse for tomograms.").style(
                    "font-size: 11px; color: #9ca3af; padding: 10px;"
                )
                return
            tmpl = "22px minmax(0,1fr) 110px 96px" if selectable else "minmax(0,1fr) 110px 96px"
            with ui.element("div").classes("cb-ltable-row cb-ltable-head").style(f"grid-template-columns: {tmpl};"):
                if selectable:
                    n_sel = sum(1 for v in selected.values() if v)
                    select_all_cb = ui.checkbox(
                        value=bool(items) and n_sel == len(items),
                        on_change=lambda e: None if _sync["v"] else _toggle_all(bool(e.value)),
                    ).props("dense")
                ui.label("Tomogram").classes("cb-ltable-h-name")
                ui.label("Dims (px)").classes("cb-ltable-h-cell")
                ui.label("Å/px").classes("cb-ltable-h-cell")
            for f in items:
                _row(f, selectable)

    def _update_counts() -> None:
        n_sel = sum(1 for v in selected.values() if v)
        missing = sum(1 for f in files if f.get("nx") and not f.get("has_voxel_size") and selected.get(f["path"]))
        mrcs_status.text = f"{n_sel}/{len(files)} selected" + (f"  ·  ⚠ {missing} need a pixel size" if missing else "")
        if select_all_cb is not None:
            _sync["v"] = True
            select_all_cb.value = bool(files) and n_sel == len(files)
            _sync["v"] = False

    def _on_toggle(path: str, value: bool, row_el) -> None:
        selected[path] = value
        row_el.classes(add="selected") if value else row_el.classes(remove="selected")
        _update_counts()

    def _toggle_all(value: bool) -> None:
        for f in files:
            cb = row_checkboxes.get(f["path"])
            if cb is not None:
                cb.value = value  # fires _on_toggle per row (updates state + row + counts)
            else:
                selected[f["path"]] = value
        _update_counts()

    # ── mrcs: scan / browse / probe ─────────────────────────────────────────────

    async def _ingest(paths) -> None:
        files.clear()
        selected.clear()
        row_checkboxes.clear()
        if not paths:
            mrcs_status.text = "No .mrc files found for that path."
            _render_table(mrcs_table, files, True)
            return
        mrcs_status.text = "Probing…"
        meta = await backend.probe_tomogram_metadata(list(paths))
        files.extend(meta)
        for f in meta:
            selected[f["path"]] = True
        _render_table(mrcs_table, files, True)
        _update_counts()

    async def _scan() -> None:
        pattern = glob_in.value
        if not pattern:
            ui.notify("Enter a directory or glob first, or use Browse…", type="warning")
            return
        mrcs_status.text = "Scanning…"
        paths = await backend.list_tomogram_candidates(pattern)
        await _ingest(paths)

    async def _browse_mrcs() -> None:
        start = _start_dir(glob_in.value) or str(project_path)
        result = await local_file_picker(start, upper_limit=None, mode="file", multiple=True, glob="*.mrc")
        if result:
            glob_in.set_directory(str(Path(result[0]).parent))
            await _ingest(result)

    # ── reference: browse / load preview ────────────────────────────────────────

    async def _load_reference(ref: str | None = None) -> None:
        ref = (ref if ref is not None else (ref_in.value or "")).strip()
        if not ref:
            ui.notify("Enter or browse to a tomograms.star first", type="warning")
            return
        ref_in.value = ref
        ref_status.text = "Reading…"
        ref_files.clear()
        try:
            rows = await backend.preview_reference_tomograms(ref)
        except Exception as e:
            ref_status.text = ""
            _render_table(ref_table, ref_files, False)
            ui.notify(f"Could not read that tomograms.star: {e}", type="negative", timeout=8000)
            return
        ref_files.extend(rows)
        missing = sum(1 for f in rows if not f.get("has_voxel_size"))
        ref_status.text = f"{len(rows)} tomogram(s)" + (f"  ·  ⚠ {missing} lack a pixel size" if missing else "")
        _render_table(ref_table, ref_files, False)

    async def _browse_reference() -> None:
        start = _start_dir(ref_in.value) or str(project_path)
        result = await local_file_picker(start, upper_limit=None, mode="file", glob="*.star")
        if result:
            await _load_reference(result[0])

    # ── commit ──────────────────────────────────────────────────────────────────

    async def _do_import() -> None:
        import_btn.props("loading")
        try:
            if tabs.value == "reference":
                ref = (ref_in.value or "").strip()
                if not ref:
                    ui.notify("Enter a path to an existing tomograms.star", type="warning")
                    return
                res = await backend.commit_imported_tomograms(project_path, mode="reference", reference_star=ref)
            else:
                chosen = [p for p, v in selected.items() if v]
                if not chosen:
                    ui.notify("Select at least one tomogram", type="warning")
                    return
                res = await backend.commit_imported_tomograms(
                    project_path,
                    mode="synthesize",
                    mrc_paths=chosen,
                    pixel_size_angstrom=float(apix_in.value or 0.0),
                    tomogram_binning=float(bin_in.value or 1.0),
                )
            ui.notify(f"Imported {res['count']} tomogram(s)", type="positive")
            dialog.close()
            if on_done:
                on_done()
        except Exception as e:
            # Surfaced, not swallowed — e.g. a selected MRC with no voxel size + no override.
            ui.notify(f"Import failed: {e}", type="negative", timeout=8000)
        finally:
            import_btn.props(remove="loading")

    # ── build ───────────────────────────────────────────────────────────────────

    dialog = ui.dialog().props("persistent")
    with dialog, ui.card().style("min-width: 700px; max-width: 880px; gap: 8px; padding: 14px;"):
        ui.label("Import tomograms").style("font-size: 14px; font-weight: 600; color: #1e293b;")
        ui.label(
            "Supply reconstructed tomograms directly — synthesize a tomograms.star from recon "
            "MRCs, or reference an existing one."
        ).classes("cb-detail-meta")

        tabs = ui.tabs().props("dense align=left indicator-color=indigo").classes("cb-species-tabs w-full")
        with tabs:
            t_mrcs = ui.tab("mrcs", label="mrcs")
            t_ref = ui.tab("reference", label="reference")
        with ui.tab_panels(tabs, value=t_mrcs).classes("w-full cb-species-panels"):
            with ui.tab_panel(t_mrcs).classes("p-0"):
                with ui.element("div").classes("cb-section-card w-full"):
                    with ui.element("div").classes("cb-section-card-header"):
                        ui.label("Source").classes("cb-section-title")
                    with ui.row().classes("items-center w-full gap-2 no-wrap"):
                        glob_in = GlobDirectoryInput(extension="*.mrc", placeholder="/path/to/reconstructions")
                        ui.button("Scan", on_click=_scan).props("flat dense no-caps size=sm").style(_ACT)
                        ui.button("Browse…", icon="folder_open", on_click=_browse_mrcs).props(
                            "flat dense no-caps size=sm"
                        ).style(_ACT)
                with ui.element("div").classes("cb-section-card w-full"):
                    with ui.element("div").classes("cb-section-card-header"):
                        ui.label("Tomograms").classes("cb-section-title")
                        mrcs_status = ui.label("").classes("cb-detail-meta").style("margin-left: auto;")
                    mrcs_table = (
                        ui.element("div").classes("cb-ltable w-full").style("max-height: 240px; overflow-y: auto;")
                    )
                with ui.row().classes("items-center w-full gap-3"):
                    apix_in = (
                        ui.number("Pixel size (Å)", value=None, format="%.4g").props("dense").style("width: 150px;")
                    )
                    apix_in.tooltip(
                        "Unbinned tilt-series pixel size — an OVERRIDE. Leave blank to use each file's own "
                        "header voxel size (the default); required only for files flagged 'apix missing'."
                    )
                    bin_in = ui.number("Binning", value=1.0, format="%g").props("dense").style("width: 110px;")
            with ui.tab_panel(t_ref).classes("p-0"):
                with ui.element("div").classes("cb-section-card w-full"):
                    with ui.element("div").classes("cb-section-card-header"):
                        ui.label("Reference tomograms.star").classes("cb-section-title")
                    with ui.row().classes("items-center w-full gap-2 no-wrap"):
                        ref_in = (
                            ui.input(placeholder="/path/to/existing/tomograms.star")
                            .props("dense")
                            .classes("flex-1")
                            .style(f"{_MONO} font-size: 12px;")
                        )
                        ui.button("Load", on_click=lambda: _load_reference()).props("flat dense no-caps size=sm").style(
                            _ACT
                        )
                        ui.button("Browse…", icon="folder_open", on_click=_browse_reference).props(
                            "flat dense no-caps size=sm"
                        ).style(_ACT)
                    ui.label("Its tomogram paths are absolutized; nothing is copied or recomputed.").classes(
                        "cb-detail-meta"
                    )
                with ui.element("div").classes("cb-section-card w-full"):
                    with ui.element("div").classes("cb-section-card-header"):
                        ui.label("Tomograms").classes("cb-section-title")
                        ref_status = ui.label("").classes("cb-detail-meta").style("margin-left: auto;")
                    ref_table = (
                        ui.element("div").classes("cb-ltable w-full").style("max-height: 240px; overflow-y: auto;")
                    )

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat dense no-caps")
            import_btn = ui.button("Import", on_click=_do_import).props("dense no-caps unelevated color=indigo")

    _render_table(mrcs_table, files, True)
    _render_table(ref_table, ref_files, False)
    dialog.open()

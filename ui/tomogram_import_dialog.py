"""PARTICLES-header tomogram-import dialog — a project-level utility, NOT a pipeline job.

Two modes, both with a metadata preview the user confirms BEFORE committing:
- **mrcs** — browse/scan a directory of reconstructed tomograms (``.mrc`` or etomo ``.rec``),
  multi-select which to import; each row shows dims + pixel size, or a red chip when the MRC
  header has no voxel size (so apix is never silently defaulted to 1.0 — CLAUDE.md
  'Surfacing uncertainty').
- **reference** — point at an existing ``tomograms.star``; its rows are read back into the
  same review table so the user sees what they're referencing instead of ingesting blind.

Imports ACCUMULATE (de-novo S5): each commit is a batch, the committed star is rebuilt from
all of them, and the prior batches are listed at the top with their collision report.
"Replace all" is still available — as a choice, not as the silent default it used to be.

Browsing reuses the shared ``local_file_picker`` (multi-select + glob-filtered). Styling
follows the Journey/dashboard ``cb-*`` chrome (ui/dashboard/css.py). See services/tomogram_import.py.
"""

from __future__ import annotations

from pathlib import Path
from collections.abc import Callable

from nicegui import ui

from services.project_state import get_project_state_for
from ui.components.buttons import house_button
from ui.components.chip import render_chip
from ui.components.fields import house_number
from ui.dashboard.css import ensure_assets_loaded
from ui.glob_directory_input import GlobDirectoryInput
from ui.local_file_picker import local_file_picker

_MONO = "font-family: ui-monospace, monospace;"
_PICKER_GLOB = "*.mrc,*.rec"
# Display cap only — every scanned file stays selected and gets imported. A Lustre recon
# directory can hold thousands of tomograms, and one DOM row each is what made this dialog
# "laggy, terrible"; the table says how many it is not showing rather than quietly eliding them.
_MAX_PREVIEW_ROWS = 300
_PROBE_CHUNK = 100  # header probes per await, so the status line moves during a long scan


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
                    render_chip("file", "unreadable", status="error", tooltip=f.get("error") or "Not a readable MRC.")
                elif f.get("has_voxel_size"):
                    # Pixel size is independent of dims — a reference star may carry apix but
                    # no rlnTomoSize* columns, so don't gate this on has_dims.
                    ui.label(f"{f['voxel_size']:.3g} Å/px").style(f"{_MONO} font-size: 11px; color: #64748b;")
                elif has_dims or f.get("path"):
                    render_chip(
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
            for f in items[:_MAX_PREVIEW_ROWS]:
                _row(f, selectable)
            if len(items) > _MAX_PREVIEW_ROWS:
                ui.label(
                    f"… and {len(items) - _MAX_PREVIEW_ROWS} more, not drawn. They are still "
                    f"{'selected and will be imported' if selectable else 'part of this star'} — "
                    "only the table is capped. Narrow the directory/glob to review them individually."
                ).style("font-size: 11px; color: #b45309; padding: 6px 10px;")

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
        paths = list(paths)
        if not paths:
            mrcs_status.text = "No .mrc / .rec files found for that path."
            _render_table(mrcs_table, files, True)
            return
        # Probed in chunks, not one 3000-file await: each probe opens an MRC header off the
        # event loop, and on Lustre that is seconds — a single await left the dialog showing
        # a bare "Probing…" with no sign of progress (the "laggy, terrible" report). The
        # table is drawn ONCE at the end; drawing per chunk would rebuild it O(n²) times.
        mrcs_status.text = f"Probing… 0/{len(paths)}"
        for start in range(0, len(paths), _PROBE_CHUNK):
            meta = await backend.probe_tomogram_metadata(paths[start : start + _PROBE_CHUNK])
            files.extend(meta)
            for f in meta:
                selected[f["path"]] = True
            mrcs_status.text = f"Probing… {len(files)}/{len(paths)}"
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
        result = await local_file_picker(start, upper_limit=None, mode="file", multiple=True, glob=_PICKER_GLOB)
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

    def _report_collisions(res: dict) -> None:
        """Say out loud what the rebuild changed: a renamed tomogram and a skipped duplicate
        are both decisions the user has to know about — a rename means downstream picks made
        under that name belong to the OTHER tomogram, and a skip means a file they selected
        is not in the star. Never folded into the success count."""
        for entry in res.get("renamed") or []:
            ui.notify(
                f"'{entry['name']}' already existed in this project — imported as '{entry['renamed_to']}'",
                type="warning",
                timeout=8000,
            )
        skipped = res.get("skipped") or []
        if skipped:
            ui.notify(
                f"{len(skipped)} file(s) skipped — already imported by an earlier batch (e.g. {skipped[0]['name']})",
                type="warning",
                timeout=8000,
            )
        for entry in res.get("batch_errors") or []:
            ui.notify(
                f"An earlier import batch ({entry['batch']}) could not be re-read and contributed "
                f"nothing: {entry['error']}",
                type="warning",
                timeout=10000,
            )

    async def _do_import() -> None:
        import_btn.props("loading")
        try:
            replace = mode_radio is not None and mode_radio.value == "replace"
            if tabs.value == "reference":
                ref = (ref_in.value or "").strip()
                if not ref:
                    ui.notify("Enter a path to an existing tomograms.star", type="warning")
                    return
                res = await backend.commit_imported_tomograms(
                    project_path, mode="reference", reference_star=ref, replace=replace
                )
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
                    replace=replace,
                )
            ui.notify(
                f"Imported {res.get('added', res['count'])} tomogram(s) — {res['count']} in the project",
                type="positive",
            )
            _report_collisions(res)
            dialog.close()
            if on_done:
                on_done()
        except Exception as e:
            # Surfaced, not swallowed — e.g. a selected MRC with no voxel size + no override.
            ui.notify(f"Import failed: {e}", type="negative", timeout=8000)
        finally:
            import_btn.props(remove="loading")

    # ── build ───────────────────────────────────────────────────────────────────

    rec = get_project_state_for(project_path).imported_tomograms
    prior_batches = rec.effective_batches() if rec else []
    mode_radio = None

    dialog = ui.dialog().props("persistent")
    with dialog, ui.card().style("min-width: 700px; max-width: 880px; gap: 8px; padding: 14px;"):
        ui.label("Import tomograms").style("font-size: 14px; font-weight: 600; color: #1e293b;")
        ui.label(
            "Supply reconstructed tomograms directly — synthesize a tomograms.star from recon "
            "MRC/REC files, or reference an existing one."
        ).classes("cb-detail-meta")

        if prior_batches:
            with ui.element("div").classes("cb-section-card w-full"):
                with ui.element("div").classes("cb-section-card-header"):
                    ui.label("Already imported").classes("cb-section-title")
                    ui.label(f"{rec.count} tomogram(s) · {len(prior_batches)} batch(es)").classes(
                        "cb-detail-meta"
                    ).style("margin-left: auto;")
                for b in prior_batches:
                    with ui.row().classes("items-center w-full gap-2 no-wrap").style("padding: 2px 8px;"):
                        ui.label(b.imported_at.strftime("%Y-%m-%d %H:%M")).classes("cb-detail-meta")
                        ui.label(b.label).style(f"{_MONO} font-size: 11px; color: #475569;").tooltip(
                            b.reference_star or "\n".join(b.source_paths[:12]) or "—"
                        )
                        ui.space()
                        ui.label(f"{b.count} tomo").classes("cb-detail-meta")
                        if b.renamed:
                            render_chip(
                                "renamed",
                                str(len(b.renamed)),
                                status="warn",
                                tooltip="\n".join(f"{e['name']} → {e['renamed_to']}" for e in b.renamed[:12]),
                            )
                        if b.skipped:
                            render_chip(
                                "skipped",
                                str(len(b.skipped)),
                                status="warn",
                                tooltip="Already imported by an earlier batch:\n"
                                + "\n".join(e["name"] for e in b.skipped[:12]),
                            )
                mode_radio = ui.radio({"add": "Add to these", "replace": "Replace all"}, value="add").props(
                    "inline dense"
                )
                mode_radio.tooltip(
                    "Add: the committed tomograms.star is rebuilt from every batch, with name "
                    "collisions renamed and already-imported files skipped (both reported). "
                    "Replace: the earlier batches are dropped — picks made on tomograms only they "
                    "described lose the tomogram they refer to."
                )

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
                        # extension "*" (not "*.mrc"): the backend filters the expansion to
                        # TOMOGRAM_SUFFIXES, so one field finds .mrc AND etomo .rec — a glob
                        # can express only one suffix, and a .rec directory used to read empty.
                        glob_in = GlobDirectoryInput(extension="*", placeholder="/path/to/reconstructions")
                        house_button("Scan", _scan)
                        house_button("Browse…", _browse_mrcs)
                with ui.element("div").classes("cb-section-card w-full"):
                    with ui.element("div").classes("cb-section-card-header"):
                        ui.label("Tomograms").classes("cb-section-title")
                        mrcs_status = ui.label("").classes("cb-detail-meta").style("margin-left: auto;")
                    mrcs_table = (
                        ui.element("div").classes("cb-ltable w-full").style("max-height: 240px; overflow-y: auto;")
                    )
                with ui.row().classes("items-center w-full gap-3"):
                    apix_in = house_number(
                        "Pixel size (Å)",
                        value=None,
                        format="%.4g",
                        width="w-24",
                        hint="Unbinned tilt-series pixel size — an OVERRIDE. Leave blank to use each file's own "
                        "header voxel size (the default); required only for files flagged 'apix missing'.",
                    )
                    bin_in = house_number("Binning", value=1.0, format="%g", width="w-20")
            with ui.tab_panel(t_ref).classes("p-0"):
                with ui.element("div").classes("cb-section-card w-full"):
                    with ui.element("div").classes("cb-section-card-header"):
                        ui.label("Reference tomograms.star").classes("cb-section-title")
                    with ui.row().classes("items-center w-full gap-2 no-wrap"):
                        ref_in = (
                            ui.input(placeholder="/path/to/existing/tomograms.star")
                            .props("dense")
                            .classes("cb-field flex-1")
                        )
                        house_button("Load", lambda: _load_reference())
                        house_button("Browse…", _browse_reference)
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
            house_button("Cancel", dialog.close)
            import_btn = house_button("Import", _do_import, kind="accent")

    _render_table(mrcs_table, files, True)
    _render_table(ref_table, ref_files, False)
    dialog.open()

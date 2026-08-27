"""PARTICLES-header tomogram-import dialog — a project-level utility, NOT a pipeline job.

ONE source field, and three things it accepts:

  · a **folder** of reconstructed volumes (``.mrc`` / etomo ``.rec``) — every volume in it
    is scanned and listed, tick off the ones you don't want;
  · **one volume**;
  · an existing **tomograms.star** — its rows are read back and referenced, nothing is
    copied or recomputed.

The mode is READ OFF the path instead of picked on a tab. The file browser already asks
"which of these do you have"; a tab that must be chosen before browsing asks it a second
time, and the two tabs it used to have carried two source fields, two status lines and two
copies of the same review table. Whatever the path resolves to lands in the SAME table,
and that table is the confirmation — nothing is committed until the user has seen it.

Every row shows dims + pixel size, or a red chip when the MRC header has no voxel size (so
apix is never silently defaulted to 1.0 — CLAUDE.md 'Surfacing uncertainty').

Imports ACCUMULATE (de-novo S5): each commit is a batch, the committed star is rebuilt from
all of them, and the prior batches are listed at the bottom with their collision report.
"Replace all" is still available — as a choice, not as the silent default it used to be.

Chrome: the house vocabulary (``house_button`` / ``house_field`` / ``PAGE_SECTION_STYLE``),
sections separated by whitespace rather than boxed in bordered cards. See
services/tomogram_import.py.
"""

from __future__ import annotations

from pathlib import Path
from collections.abc import Callable

from nicegui import ui

from services.project_state import get_project_state_for
from services.tomogram_import import TOMOGRAM_SUFFIXES
from ui.components.buttons import house_button
from ui.components.chip import render_chip
from ui.components.fields import house_field, house_number
from ui.dashboard.css import ensure_assets_loaded
from ui.job_plugins._field_styles import PAGE_SECTION_STYLE
from ui.local_file_picker import local_file_picker

_MONO = "font-family: ui-monospace, monospace;"
_HINT = "font-size: 10px; color: #94a3b8; line-height: 1.35;"
_STATUS = "font-size: 10px; color: #475569; line-height: 1.35;"
_STATUS_BAD = "font-size: 10px; color: #b45309; line-height: 1.35;"
# One browse dialog for all three source shapes — folders are always listed, and these are
# the files worth showing beside them.
_PICKER_GLOB = "*.mrc,*.rec,*.star"
# Display cap only — every scanned file stays selected and gets imported. A Lustre recon
# directory can hold thousands of tomograms, and one DOM row each is what made this dialog
# "laggy, terrible"; the table says how many it is not showing rather than quietly eliding them.
_MAX_PREVIEW_ROWS = 300
_PROBE_CHUNK = 100  # header probes per await, so the status line moves during a long scan

# What the resolved path turned out to be. Drives the commit mode and which of the two
# option lines is shown; nothing else in the dialog branches on it.
_KIND_NONE = ""
_KIND_VOLUMES = "volumes"  # a folder of volumes, or one volume -> synthesize
_KIND_STAR = "star"  # an existing tomograms.star -> reference


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

    # `typed` is the last string _resolve was handed — a blur whose text hasn't moved is not
    # a new request (clicking Browse… blurs the field, and re-scanning Lustre for that would
    # be a free directory walk per click).
    src = {"kind": _KIND_NONE, "path": "", "folder": "", "typed": ""}
    files: list[dict] = []  # preview rows (probed MRC headers, or reference star rows)
    selected: dict[str, bool] = {}  # volumes mode: path -> checked
    row_checkboxes: dict[str, ui.checkbox] = {}
    select_all_cb = None
    _sync = {"v": False}  # guard: programmatic select-all set must not re-fire _toggle_all

    # ── Preview table ───────────────────────────────────────────────────────────

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

    def _render_table() -> None:
        nonlocal select_all_cb
        selectable = src["kind"] == _KIND_VOLUMES
        select_all_cb = None
        table.clear()
        row_checkboxes.clear()
        with table:
            if not files:
                ui.label(
                    "Point the field above at a folder of volumes, one volume, or a tomograms.star."
                    if src["kind"] == _KIND_NONE
                    else "Nothing to import from that path."
                ).style("font-size: 11px; color: #9ca3af; padding: 10px;")
                return
            tmpl = "22px minmax(0,1fr) 110px 96px" if selectable else "minmax(0,1fr) 110px 96px"
            with ui.element("div").classes("cb-ltable-row cb-ltable-head").style(f"grid-template-columns: {tmpl};"):
                if selectable:
                    n_sel = sum(1 for v in selected.values() if v)
                    select_all_cb = ui.checkbox(
                        value=bool(files) and n_sel == len(files),
                        on_change=lambda e: None if _sync["v"] else _toggle_all(bool(e.value)),
                    ).props("dense")
                ui.label("Tomogram").classes("cb-ltable-h-name")
                ui.label("Dims (px)").classes("cb-ltable-h-cell")
                ui.label("Å/px").classes("cb-ltable-h-cell")
            for f in files[:_MAX_PREVIEW_ROWS]:
                _row(f, selectable)
            if len(files) > _MAX_PREVIEW_ROWS:
                ui.label(
                    f"… and {len(files) - _MAX_PREVIEW_ROWS} more, not drawn. They are still "
                    f"{'selected and will be imported' if selectable else 'part of this star'} — "
                    "only the table is capped. Narrow the folder to review them individually."
                ).style("font-size: 11px; color: #b45309; padding: 6px 10px;")

    def _update_counts() -> None:
        if src["kind"] == _KIND_STAR:
            missing = sum(1 for f in files if not f.get("has_voxel_size"))
            counts_lbl.text = f"{len(files)} in this star" + (f"  ·  ⚠ {missing} lack a pixel size" if missing else "")
        elif src["kind"] == _KIND_VOLUMES:
            n_sel = sum(1 for v in selected.values() if v)
            missing = sum(1 for f in files if f.get("nx") and not f.get("has_voxel_size") and selected.get(f["path"]))
            counts_lbl.text = f"{n_sel}/{len(files)} selected" + (
                f"  ·  ⚠ {missing} need a pixel size" if missing else ""
            )
        else:
            counts_lbl.text = ""
        if select_all_cb is not None:
            _sync["v"] = True
            select_all_cb.value = bool(files) and sum(1 for v in selected.values() if v) == len(files)
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

    # ── Source resolution ───────────────────────────────────────────────────────

    def _set_kind(kind: str, *, folder: str = "", path: str = "") -> None:
        src.update(kind=kind, folder=folder, path=path)
        synth_row.set_visibility(kind == _KIND_VOLUMES)
        ref_note.set_visibility(kind == _KIND_STAR)
        folder_btn.set_visibility(bool(folder))

    def _clear(message: str = "", *, bad: bool = False) -> None:
        files.clear()
        selected.clear()
        _set_kind(_KIND_NONE)
        status.text = message
        status.style(_STATUS_BAD if bad else _STATUS)
        _render_table()
        _update_counts()

    async def _probe(paths: list[Path], *, folder: str = "") -> None:
        """Read every candidate's MRC header into the review table (synthesize mode)."""
        files.clear()
        selected.clear()
        row_checkboxes.clear()
        # Probed in chunks, not one 3000-file await: each probe opens an MRC header off the
        # event loop, and on Lustre that is seconds — a single await left the dialog showing
        # a bare "Probing…" with no sign of progress (the "laggy, terrible" report). The
        # table is drawn ONCE at the end; drawing per chunk would rebuild it O(n²) times.
        status.style(_STATUS)
        status.text = f"Reading headers… 0/{len(paths)}"
        for start in range(0, len(paths), _PROBE_CHUNK):
            meta = await backend.probe_tomogram_metadata([str(p) for p in paths[start : start + _PROBE_CHUNK]])
            files.extend(meta)
            for f in meta:
                selected[f["path"]] = True
            status.text = f"Reading headers… {len(files)}/{len(paths)}"
        _set_kind(_KIND_VOLUMES, folder=folder)
        where = f"in {Path(folder).name or folder}/" if folder else f"— {Path(paths[0]).name}" if paths else ""
        status.text = (
            f"{len(files)} volume(s) {where}. A tomograms.star is synthesized from their headers "
            "when you import; the files themselves are not copied or moved."
        )
        _render_table()
        _update_counts()

    async def _load_star(star: Path) -> None:
        status.style(_STATUS)
        status.text = "Reading tomograms.star…"
        files.clear()
        selected.clear()
        try:
            rows = await backend.preview_reference_tomograms(str(star))
        except Exception as e:
            # Surfaced on the line under the field, not only as a toast that scrolls away.
            _clear(f"Could not read {star.name}: {e}", bad=True)
            return
        files.extend(rows)
        _set_kind(_KIND_STAR, path=str(star))
        status.text = f"{len(files)} tomogram(s) referenced from {star.name}."
        _render_table()
        _update_counts()

    async def _resolve(raw: str) -> None:
        """Turn whatever is in the path field into a preview. The ONE entry point —
        Browse…, Enter in the field and the 'whole folder' shortcut all land here, so
        there is exactly one place that decides what a path means."""
        raw = (raw or "").strip()
        path_in.value = raw
        src["typed"] = raw
        if not raw:
            _clear()
            return
        p = Path(raw).expanduser()
        if not p.exists():
            _clear(f"Nothing at {p}", bad=True)
            return
        if p.is_dir():
            status.style(_STATUS)
            status.text = "Scanning…"
            found = await backend.list_tomogram_candidates(str(p))
            if not found:
                _clear(f"No .mrc / .rec volumes directly in {p.name or p}/", bad=True)
                return
            await _probe([Path(x) for x in found], folder=str(p))
            return
        if p.suffix.lower() == ".star":
            await _load_star(p)
            return
        if p.suffix.lower() in TOMOGRAM_SUFFIXES:
            # Probed with no folder so the status line says "one volume", then the parent is
            # attached afterwards — it arms "Take the whole folder" without claiming the
            # folder is what was selected.
            await _probe([p])
            _set_kind(_KIND_VOLUMES, folder=str(p.parent), path=str(p))
            return
        _clear(
            f"{p.name} is not a tomogram source — point this at a folder of .mrc/.rec volumes, "
            "one volume, or a tomograms.star.",
            bad=True,
        )

    async def _on_blur() -> None:
        """Typing a path and clicking away reads it — one less button to find. Unchanged
        text is a no-op (see ``src["typed"]``)."""
        raw = (path_in.value or "").strip()
        if raw != src["typed"]:
            await _resolve(raw)

    async def _browse() -> None:
        start = src["folder"] or _start_dir(path_in.value) or str(project_path)
        result = await local_file_picker(start, upper_limit=None, mode="any", glob=_PICKER_GLOB)
        if result:
            await _resolve(result[0])

    async def _take_folder() -> None:
        """One volume was picked; take everything beside it. The common correction after
        double-clicking a file in the browser, and one click instead of a re-browse."""
        if src["folder"]:
            await _resolve(src["folder"])

    def _start_dir(glob_or_path: str) -> str | None:
        if not glob_or_path:
            return None
        p = Path(glob_or_path).expanduser()
        d = p if p.is_dir() else p.parent
        s = str(d)
        return s if s and s != "." else None

    # ── Commit ──────────────────────────────────────────────────────────────────

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
        # A pasted path that was never read (Import clicked before the blur scan landed)
        # resolves here rather than being refused as "no source".
        if src["kind"] == _KIND_NONE and (path_in.value or "").strip():
            await _resolve(path_in.value)
        if src["kind"] == _KIND_NONE:
            ui.notify("Choose a source first — Browse… or type a path", type="warning")
            return
        import_btn.props("loading")
        try:
            replace = mode_radio is not None and mode_radio.value == "replace"
            if src["kind"] == _KIND_STAR:
                res = await backend.commit_imported_tomograms(
                    project_path, mode="reference", reference_star=src["path"], replace=replace
                )
            else:
                chosen = [p for p, v in selected.items() if v]
                if not chosen:
                    ui.notify("Tick at least one tomogram", type="warning")
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
                f"Imported {res.get('added', res['count'])} tomogram(s) — {res['count']} in the project. "
                "Open the Tomograms view to see them.",
                type="positive",
                timeout=6000,
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

    # ── Build ───────────────────────────────────────────────────────────────────

    rec = get_project_state_for(project_path).imported_tomograms
    prior_batches = rec.effective_batches() if rec else []
    mode_radio = None

    dialog = ui.dialog().props("persistent")
    with dialog, ui.card().style("min-width: 720px; max-width: 880px; gap: 0; padding: 16px 18px 14px;"):
        ui.label("Import tomograms").style("font-size: 14px; font-weight: 600; color: #1e293b;")
        ui.label("Reconstructed volumes for a project with no preprocessing pipeline of its own.").classes(
            "cb-detail-meta"
        )

        # ── Source ──
        ui.element("div").style("height: 14px;")
        ui.label("Source").style(PAGE_SECTION_STYLE)
        ui.element("div").style("height: 5px;")
        with ui.row().classes("items-center w-full gap-2 no-wrap"):
            path_in = house_field(
                "",
                lambda: ui.input(placeholder="/path/to/reconstructions  ·  volume.mrc  ·  tomograms.star"),
                width="w-full",
            )
            path_in.on("keydown.enter", lambda: _resolve(path_in.value))
            path_in.on("blur", _on_blur)
            house_button("Browse…", _browse)
        ui.element("div").style("height: 3px;")
        ui.label(
            "A folder of .mrc / .rec volumes · one volume · an existing tomograms.star — "
            "whichever you point at is read straight away."
        ).style(_HINT)
        with ui.row().classes("items-start w-full gap-2 no-wrap").style("margin-top: 4px;"):
            status = ui.label("").style(_STATUS + " flex: 1 1 auto; min-width: 0;")
            folder_btn = house_button(
                "Take the whole folder",
                _take_folder,
                tooltip="Import every .mrc / .rec volume sitting beside the one you picked",
            )
            folder_btn.set_visibility(False)

        # ── Tomograms ──
        ui.element("div").style("height: 14px;")
        with ui.row().classes("items-center w-full gap-2 no-wrap"):
            ui.label("Tomograms").style(PAGE_SECTION_STYLE)
            ui.space()
            counts_lbl = ui.label("").classes("cb-detail-meta")
        ui.element("div").style("height: 5px;")
        table = ui.element("div").classes("cb-ltable w-full").style("max-height: 260px; overflow-y: auto;")

        synth_row = ui.row().classes("items-center w-full gap-4").style("margin-top: 8px;")
        with synth_row:
            apix_in = house_number(
                "Pixel size (Å)",
                value=None,
                format="%.4g",
                width="w-24",
                hint="Unbinned tilt-series pixel size — an OVERRIDE. Leave blank to use each file's own "
                "header voxel size (the default); required only for files flagged 'apix missing'.",
            )
            bin_in = house_number(
                "Binning",
                value=1.0,
                format="%g",
                width="w-20",
                hint="How much the reconstruction is binned relative to the unbinned tilt series.",
            )
        synth_row.set_visibility(False)
        ref_note = ui.label("The star's tomogram paths are absolutized; nothing is copied, moved or recomputed.").style(
            _HINT + " margin-top: 8px;"
        )
        ref_note.set_visibility(False)

        # ── Already imported ──
        if prior_batches:
            ui.element("div").style("height: 14px;")
            with ui.row().classes("items-center w-full gap-2 no-wrap"):
                ui.label("Already imported").style(PAGE_SECTION_STYLE)
                ui.space()
                ui.label(f"{rec.count} tomogram(s) · {len(prior_batches)} batch(es)").classes("cb-detail-meta")
            ui.element("div").style("height: 3px;")
            for b in prior_batches:
                with ui.row().classes("items-center w-full gap-2 no-wrap").style("padding: 1px 0;"):
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
            mode_radio = (
                ui.radio({"add": "Add to these", "replace": "Replace all"}, value="add")
                .props("inline dense")
                .style("margin-top: 2px;")
            )
            mode_radio.tooltip(
                "Add: the committed tomograms.star is rebuilt from every batch, with name "
                "collisions renamed and already-imported files skipped (both reported). "
                "Replace: the earlier batches are dropped — picks made on tomograms only they "
                "described lose the tomogram they refer to."
            )

        ui.element("div").style("height: 16px;")
        with ui.row().classes("w-full justify-end gap-2"):
            house_button("Cancel", dialog.close)
            import_btn = house_button("Import", _do_import, kind="accent")

    _render_table()
    dialog.open()

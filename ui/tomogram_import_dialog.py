"""PARTICLES-header tomogram-import dialog — a project-level utility, NOT a pipeline job.

Pick a directory or specific reconstructed ``.mrc`` files; preview per-file metadata
(dims + pixel size, or a red flag when the MRC header has no voxel size) BEFORE
committing, so the user confirms the selection and supplies a pixel size if needed;
then write ``Tomograms/tomograms.star`` + record it on ``ProjectState``. The metadata
preview surfaces a missing voxel size up front rather than letting apix silently fall
back to 1.0 (CLAUDE.md 'Surfacing uncertainty'). See services/tomogram_import.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional

from nicegui import ui

from ui.glob_directory_input import GlobDirectoryInput

_MONO = "font-family: ui-monospace, monospace;"


def open_tomogram_import_dialog(backend, project_path, on_done: Optional[Callable[[], None]] = None) -> None:
    """Open the tomogram-import dialog for ``project_path``. Commits via
    ``backend.commit_imported_tomograms`` and calls ``on_done`` after a successful import."""
    project_path = Path(project_path)

    # Pre-fill the pixel-size override from the project's microscope params (the unbinned
    # tilt-series pixel size) when known — surfaced + editable, never silently applied.
    default_apix = 0.0
    try:
        from services.project_state import get_project_state_for

        st = get_project_state_for(project_path)
        default_apix = float(getattr(st.microscope, "pixel_size_angstrom", 0.0) or 0.0)
    except Exception:
        default_apix = 0.0

    files: list[dict] = []  # probed metadata dicts (from backend.probe_tomogram_metadata)
    selected: dict[str, bool] = {}  # path -> checked

    def _marker(text: str, tip: str) -> None:
        with ui.row().classes("items-center gap-1").style("display: inline-flex;"):
            ui.icon("error_outline", size="13px").classes("text-red-500")
            ui.label(text).style(f"{_MONO} color: #dc2626; font-size: 11px;").tooltip(tip)

    def _render_files() -> None:
        files_box.clear()
        with files_box:
            if not files:
                ui.label("Scan a directory or glob to list reconstructed tomograms.").style(
                    "font-size: 11px; color: #9ca3af; padding: 8px;"
                )
                return
            n_sel = sum(1 for v in selected.values() if v)
            with ui.row().classes("items-center w-full gap-2").style("padding: 4px 8px; background: #f8fafc;"):
                ui.checkbox(value=(n_sel == len(files)), on_change=lambda e: _toggle_all(bool(e.value))).props("dense")
                ui.label(f"{n_sel}/{len(files)} selected").style(f"{_MONO} font-size: 10px; color: #6b7280;")
            for f in files:
                with (
                    ui.row()
                    .classes("items-center w-full gap-2")
                    .style("padding: 2px 8px; border-top: 1px solid #f1f5f9;")
                ):
                    ui.checkbox(
                        value=selected.get(f["path"], True),
                        on_change=lambda e, p=f["path"]: selected.__setitem__(p, bool(e.value)),
                    ).props("dense")
                    ui.label(f["name"]).style(
                        f"{_MONO} font-size: 11px; flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis;"
                    )
                    if f.get("error"):
                        _marker("unreadable", f["error"])
                    elif f.get("nx"):
                        ui.label(f"{f['nx']}×{f['ny']}×{f['nz']}").style(f"{_MONO} font-size: 11px; color: #6b7280;")
                        if f.get("has_voxel_size"):
                            ui.label(f"{f['voxel_size']:.3g} Å/px").style(f"{_MONO} font-size: 11px; color: #6b7280;")
                        else:
                            _marker(
                                "no voxel size",
                                "This MRC header carries no pixel size — set 'Pixel size (Å)' below, "
                                "or picks on this tomogram will be mis-scaled.",
                            )

    def _toggle_all(v: bool) -> None:
        for k in list(selected.keys()):
            selected[k] = v
        _render_files()

    async def _scan() -> None:
        pattern = glob_in.value
        if not pattern:
            ui.notify("Enter a directory or glob first", type="warning")
            return
        status.text = "Scanning…"
        paths = await backend.list_tomogram_candidates(pattern)
        files.clear()
        selected.clear()
        if not paths:
            status.text = "No .mrc files found for that path."
            _render_files()
            return
        meta = await backend.probe_tomogram_metadata(paths)
        files.extend(meta)
        for f in meta:
            selected[f["path"]] = True
        missing = sum(1 for f in meta if f.get("nx") and not f.get("has_voxel_size"))
        status.text = f"{len(meta)} file(s)." + (
            f"  ⚠ {missing} lack a voxel size — set a pixel size." if missing else ""
        )
        _render_files()

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

    dialog = ui.dialog().props("persistent")
    with dialog, ui.card().style("min-width: 680px; max-width: 860px; gap: 8px;"):
        ui.label("Import tomograms").style("font-size: 15px; font-weight: 600;")
        ui.label(
            "Particle-only project: supply reconstructed tomograms directly — synthesize a "
            "tomograms.star from recon MRCs, or reference an existing one."
        ).style("font-size: 11px; color: #6b7280;")

        tabs = ui.tabs().props("dense align=left").classes("w-full")
        with tabs:
            t_syn = ui.tab("synthesize", label="From recon MRCs")
            t_ref = ui.tab("reference", label="Reference a tomograms.star")
        with ui.tab_panels(tabs, value=t_syn).classes("w-full"):
            with ui.tab_panel(t_syn):
                with ui.row().classes("items-center w-full gap-2"):
                    ui.label("Directory / glob").style(f"{_MONO} font-size: 11px; width: 92px; flex-shrink: 0;")
                    glob_in = GlobDirectoryInput(extension="*.mrc", placeholder="/path/to/reconstructions")
                    ui.button("Scan", on_click=_scan).props("dense flat color=primary")
                files_box = (
                    ui.column()
                    .classes("w-full gap-0")
                    .style("max-height: 260px; overflow-y: auto; border: 1px solid #e5e7eb; border-radius: 4px;")
                )
                status = ui.label("").style("font-size: 11px; color: #6b7280;")
                with ui.row().classes("items-center w-full gap-3"):
                    apix_in = (
                        ui.number("Pixel size (Å)", value=(default_apix or None), format="%.4g")
                        .props("dense")
                        .style("width: 150px;")
                    )
                    apix_in.tooltip(
                        "Unbinned tilt-series pixel size. Required when an MRC header lacks a voxel size; "
                        "otherwise leave blank to use each file's own header value."
                    )
                    bin_in = ui.number("Binning", value=1.0, format="%g").props("dense").style("width: 110px;")
                    if default_apix > 0:
                        ui.label("apix prefilled from project params").style("font-size: 10px; color: #9333ea;")
            with ui.tab_panel(t_ref):
                ref_in = ui.input("Path to an existing tomograms.star").props("dense").classes("w-full")
                ui.label("Its tomogram paths are absolutized; nothing is copied or recomputed.").style(
                    "font-size: 10px; color: #9ca3af;"
                )

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat dense")
            import_btn = ui.button("Import", on_click=_do_import).props("dense color=primary")

    _render_files()
    dialog.open()

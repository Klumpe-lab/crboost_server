"""Tomograms-view dialog: register segmentations made outside crboost (MemBrain, a hand-run easymode
or Ais, …) on this project's tomograms. A project-level artifact, not a job.

One source field (a folder of .mrc volumes, or a glob); the reconstruction the files were computed
on, asked only when the project has several; the feature, for files whose name does not end in
`__<feature>`; and the encoding, read off each file unless chosen. The table is the confirmation:
what each file registers as, or why it is skipped. Nothing is copied. Import reads every accepted
volume once more for its overlay masks and fraction, in the background (the tray), and the
Tomograms view reloads when it lands. Earlier batches are listed with Remove.

Matching, encodings and what is skipped: services/segmentation/ingest.py.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from nicegui import ui

from services.project_state import get_project_state_for
from services.segmentation.ingest import split_name
from ui.background_task import BackgroundTask
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_field, house_select
from ui.components.recon_compare import recon_version_labels
from ui.dashboard.css import ensure_assets_loaded
from ui.job_plugins._field_styles import PAGE_SECTION_STYLE
from ui.local_file_picker import local_file_picker

_MONO = "font-family: ui-monospace, monospace;"
_HINT = "font-size: 10px; color: #94a3b8; line-height: 1.35;"
_STATUS = "font-size: 10px; color: #475569; line-height: 1.35;"
_STATUS_BAD = "font-size: 10px; color: #b45309; line-height: 1.35;"
_ROW_TMPL = "minmax(0, 1.5fr) minmax(0, 1fr) 84px 96px minmax(0, 1.4fr)"
_MAX_ROWS = 300  # drawn; every row still counts and imports
# The stored value of p = 1, as the import can name it; "" reads it off each file.
_ENCODINGS = {
    "": "read from each file",
    "1": "p (0–1), or a 0 / 1 mask",
    "127": "p × 127 (easymode int8)",
    "255": "p × 255 (Ais)",
}


def open_segmentation_import_dialog(backend, project_path, on_done: Callable[[], None] | None = None) -> None:
    """Open the dialog for `project_path`. Import runs as a background task; `on_done` follows a
    batch that registered, and a removed one."""
    ensure_assets_loaded()
    project_path = Path(project_path)
    state = get_project_state_for(project_path)
    recons = recon_version_labels(state)
    if not recons:
        ui.notify(
            "No reconstruct job in this project: segmentations are registered on its reconstructions.", type="warning"
        )
        return

    # With several reconstructions the user names the one the files were computed on: never guessed.
    opts = {"source": "", "recon": next(iter(recons)) if len(recons) == 1 else "", "feature": "", "encoding": ""}
    files: list[str] = []
    probes: dict[str, dict] = {}  # by path; kept while the dialog is open, across option changes
    rows: list[dict] = []
    # Each change of source or option starts a new plan (`gen`); a plan that lands after a newer one started is
    # dropped. `busy` counts the reads in flight; Import takes only the rows of the latest plan, once it landed.
    gen = {"n": 0, "rows": -1, "busy": 0}
    dedup_key = f"segmentation-import:{project_path}"

    # ── Table ────────────────────────────────────────────────────────────────

    def _render_table() -> None:
        table.clear()
        with table:
            if not rows:
                ui.label(
                    "Point the field above at a folder of .mrc volumes, or a glob."
                    if not files
                    else "Nothing to register from that source."
                ).style("font-size: 11px; color: #9ca3af; padding: 10px;")
                return
            with (
                ui.element("div").classes("cb-ltable-row cb-ltable-head").style(f"grid-template-columns: {_ROW_TMPL};")
            ):
                for title in ("File", "Tomogram", "Feature", "Encoding", "Status"):
                    ui.label(title).classes("cb-ltable-h-name")
            for r in rows[:_MAX_ROWS]:
                with (
                    ui.element("div")
                    .classes("cb-ltable-row")
                    .style(f"grid-template-columns: {_ROW_TMPL}; cursor: default;")
                ):
                    ui.label(r["name"]).classes("cb-ltable-name").tooltip(r["path"])
                    ui.label(r.get("ts") or "—").style(f"{_MONO} font-size: 10px; color: #64748b;")
                    ui.label(r.get("feature") or "—").style(f"{_MONO} font-size: 10px; color: #475569;")
                    enc = r.get("encoding") or "—"
                    if r.get("dtype"):
                        enc_tip = f"{r['dtype']}, values {r['vmin']:g} … {r['vmax']:g}"
                    else:
                        enc_tip = "not read"
                    ui.label(enc).style(f"{_MONO} font-size: 10px; color: #64748b;").tooltip(enc_tip)
                    if r["ok"]:
                        ui.label("registers").style(_STATUS)
                    else:
                        ui.label(r["reason"]).style(_STATUS_BAD + " white-space: normal;")
            if len(rows) > _MAX_ROWS:
                ui.label(f"… and {len(rows) - _MAX_ROWS} more, not drawn; they count all the same.").style(
                    "font-size: 11px; color: #b45309; padding: 6px 10px;"
                )
        n_ok = sum(1 for r in rows if r["ok"])
        counts.set_text(f"{n_ok} to register · {len(rows) - n_ok} skipped" if rows else "")

    # ── Planning ─────────────────────────────────────────────────────────────

    def _full_scale() -> float | None:
        return float(opts["encoding"]) if opts["encoding"] else None

    async def _replan() -> None:
        gen["n"] += 1
        mine = gen["n"]
        rows.clear()
        if not files or not opts["recon"]:
            _render_table()
            if files:
                status.style(replace=_STATUS_BAD)
                status.set_text("Choose the reconstruction the files were computed on.")
            return
        gen["busy"] += 1
        try:
            names = backend.segmentation_import_names(project_path, opts["recon"])
            matched = [p for p in files if split_name(p, names, opts["feature"])[1] is not None]
            todo = [p for p in matched if p not in probes]
            if todo:
                status.style(replace=_STATUS)
                status.set_text(f"Reading {len(todo)} volume(s)…")

                def _progress(current: int, total: int, message: str) -> None:
                    if gen["n"] == mine:
                        status.set_text(f"Reading volumes… {current}/{total} · {message}")

                probes.update(await backend.probe_segmentation_volumes(todo, progress_cb=_progress))
            planned = await backend.plan_segmentation_import(
                project_path, files, names, opts["feature"], _full_scale(), probes
            )
        finally:
            gen["busy"] -= 1
        if gen["n"] != mine:
            return
        rows[:] = planned
        gen["rows"] = mine
        status.style(replace=_STATUS)
        status.set_text(f"{len(files)} .mrc file(s) in {opts['source']}")
        _render_table()

    async def _resolve(raw: str) -> None:
        raw = (raw or "").strip()
        source_in.value = raw
        if raw == opts["source"]:
            return  # read already, or being read (Enter, then the blur that follows it)
        opts["source"] = raw
        files.clear()
        rows.clear()
        _render_table()
        found: list[str] = []
        if raw:
            status.style(replace=_STATUS)
            status.set_text("Scanning…")
            gen["busy"] += 1
            try:
                found = await backend.list_segmentation_files(raw)
            finally:
                gen["busy"] -= 1
            if opts["source"] != raw:
                return  # another source was entered meanwhile
            if not found:
                status.style(replace=_STATUS_BAD)
                status.set_text(f"No .mrc files at {raw}")
        else:
            status.set_text("")
        files[:] = found
        await _replan()

    async def _browse() -> None:
        start = opts["source"] if opts["source"] and Path(opts["source"]).is_dir() else str(project_path)
        result = await local_file_picker(start, mode="directory")
        if result:
            await _resolve(result[0])

    async def _set_option(key: str, value) -> None:
        if opts[key] == (value or ""):
            return
        opts[key] = value or ""
        await _replan()

    # ── Import / remove ──────────────────────────────────────────────────────

    async def _import() -> None:
        if gen["busy"] or gen["rows"] != gen["n"]:
            ui.notify("The table is still being made; Import once it shows.", type="warning")
            return
        accepted = [r for r in rows if r["ok"]]
        if not accepted:
            ui.notify("Nothing to register: every file is skipped (see the table).", type="warning")
            return
        if BackgroundTask.existing(dedup_key) is not None:
            ui.notify("An import of this project is still running; Import again once it has landed.", type="warning")
            return
        snapshot = dict(opts)
        planned = list(rows)

        async def _run(progress_cb):
            res = await backend.commit_segmentation_import(
                project_path,
                planned,
                recon_instance=snapshot["recon"],
                source=snapshot["source"],
                feature=snapshot["feature"],
                full_scale=float(snapshot["encoding"]) if snapshot["encoding"] else None,
                progress_cb=progress_cb,
            )
            if not res["success"]:
                raise RuntimeError(res["error"])
            late = res.get("skipped") or []
            tail = f"; {len(late)} skipped" if late else ""
            return f"{res['n_volumes']} volume(s) of {', '.join(res['features'])}{tail}"

        def _landed(task) -> None:
            if task.status == "succeeded":
                ui.notify(f"Segmentations registered: {task.result_message}", type="positive", timeout=6000)
                if on_done is not None:
                    on_done()
            else:
                ui.notify(f"Segmentation import failed: {task.error}", type="negative", timeout=10000)

        dialog.close()
        # At the page layout: the dialog's own slot goes with it, and the completion poll with that.
        with dialog_host():
            BackgroundTask(
                title=f"Register segmentations · {len(accepted)} volume(s)",
                subtitle=snapshot["source"],
                project_path=str(project_path),
                dedup_key=dedup_key,
            ).submit(_run, on_complete=_landed)

    def _confirm_remove(batch) -> None:
        with dialog_host(), ui.dialog() as confirm, ui.card().classes("w-[26rem] max-w-full gap-2"):
            ui.label(f"Remove the segmentation {batch.label}?").classes("text-sm font-bold")
            ui.label(
                f"Its {batch.n_volumes} volume(s) leave the views and template matching, and its overlays in "
                f"{batch.dir} are deleted. The volumes in {batch.source or 'their folder'} are not touched."
            ).classes("text-[12px] text-gray-600")

            async def _go() -> None:
                confirm.close()
                res = await backend.remove_segmentation_import(project_path, batch.batch)
                if not res["success"]:
                    ui.notify(res["error"], type="negative")
                    return
                ui.notify(
                    res.get("warning") or f"Removed {batch.label}", type="warning" if res.get("warning") else "info"
                )
                _render_batches()
                if on_done is not None:
                    on_done()

            with ui.row().classes("w-full justify-end gap-2"):
                house_button("Cancel", confirm.close)
                house_button("Remove", _go, kind="danger")
        confirm.open()

    def _render_batches() -> None:
        earlier.clear()
        batches = get_project_state_for(project_path).segmentation_imports
        with earlier:
            if not batches:
                return
            ui.element("div").style("height: 14px;")
            ui.label("Registered earlier").style(PAGE_SECTION_STYLE)
            ui.element("div").style("height: 3px;")
            for b in batches:
                with ui.row().classes("items-center w-full gap-2 no-wrap").style("padding: 1px 0;"):
                    ui.label(b.label).classes("cb-detail-meta")
                    ui.label(", ".join(b.features)).style(f"{_MONO} font-size: 11px; color: #475569;")
                    ui.label(f"{b.n_volumes} volume(s)").classes("cb-detail-meta").tooltip(b.source or "—")
                    if len(recons) > 1 and b.recon_instance:
                        ui.label(recons.get(b.recon_instance, b.recon_instance)).classes("cb-detail-meta")
                    ui.space()
                    house_button("Remove", lambda _e=None, b=b: _confirm_remove(b), kind="danger")

    # ── Build ────────────────────────────────────────────────────────────────

    with dialog_host(), ui.dialog().props("persistent") as dialog:
        with ui.card().style("min-width: 760px; max-width: 920px; gap: 0; padding: 16px 18px 14px;"):
            ui.label("Import segmentations").style("font-size: 14px; font-weight: 600; color: #1e293b;")
            ui.label(
                "Volumes segmented outside crboost, registered on this project's tomograms. They are read where "
                "they are; nothing is copied."
            ).classes("cb-detail-meta")

            ui.element("div").style("height: 14px;")
            ui.label("Source").style(PAGE_SECTION_STYLE)
            ui.element("div").style("height: 5px;")
            with ui.row().classes("items-center w-full gap-2 no-wrap"):
                source_in = house_field(
                    "", lambda: ui.input(placeholder="/path/to/segmentations  ·  /path/*_segmented.mrc"), width="w-full"
                )
                source_in.on("keydown.enter", lambda: _resolve(source_in.value))
                source_in.on("blur", lambda: _resolve(source_in.value))
                house_button("Browse…", _browse)
            ui.element("div").style("height: 3px;")
            ui.label(
                "A folder of .mrc volumes or a glob. Each file names its tomogram: <tomogram>__<feature>.mrc, as "
                "easymode and Ais write them, or any name that starts with the tomogram's (MemBrain's "
                "<tomogram>_…_segmented.mrc), which takes the feature given below."
            ).style(_HINT)
            status = ui.label("").style(_STATUS + " margin-top: 4px;")

            ui.element("div").style("height: 10px;")
            with ui.row().classes("items-center w-full gap-4"):
                if len(recons) > 1:
                    house_select(
                        "Computed on",
                        recons,
                        value=None,
                        width="w-48",
                        hint="The reconstruction the files were segmented from. Two reconstructions of one tilt "
                        "series share names and grids, but another alignment puts the specimen elsewhere in the "
                        "volume, so the import cannot tell them apart.",
                        on_change=lambda e: _set_option("recon", e.value),
                    )
                feature_in = house_field(
                    "Feature",
                    lambda: ui.input(placeholder="e.g. membrane"),
                    width="w-32",
                    hint="The feature of files whose name does not end in __<feature> (MemBrain: membrane).",
                )
                feature_in.on("keydown.enter", lambda: _set_option("feature", feature_in.value.strip()))
                feature_in.on("blur", lambda: _set_option("feature", feature_in.value.strip()))
                house_select(
                    "Encoding",
                    _ENCODINGS,
                    value="",
                    width="w-48",
                    hint="The stored value of probability 1. Read from each file: integers up to 1 are a mask, "
                    "int8 is p × 127, larger unsigned integers p × 255, floats in 0 … 1 are p; anything else "
                    "(label maps, scores, logits) is skipped until an encoding is chosen here.",
                    on_change=lambda e: _set_option("encoding", e.value),
                )

            ui.element("div").style("height: 14px;")
            with ui.row().classes("items-center w-full gap-2 no-wrap"):
                ui.label("Volumes").style(PAGE_SECTION_STYLE)
                ui.space()
                counts = ui.label("").classes("cb-detail-meta")
            ui.element("div").style("height: 5px;")
            table = ui.element("div").classes("cb-ltable w-full").style("max-height: 300px; overflow-y: auto;")

            earlier = ui.element("div").classes("w-full")

            ui.element("div").style("height: 16px;")
            with ui.row().classes("w-full justify-end gap-2"):
                house_button("Cancel", dialog.close)
                house_button("Import", _import, kind="accent")

    _render_table()
    _render_batches()
    dialog.open()

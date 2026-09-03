"""Shared rendering for protocol bundles (roadmap 16 S4 — hoisted from ui/protocols_dialog.py so
the landing dialog and the workspace Protocols view draw a species, a stage and its parameters
the same way). Plus the download-input submission both surfaces offer."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from nicegui import ui

from services.jobs.spec import display_name
from services.protocols.discovery import ProtocolInfo
from services.protocols.schema import ProtocolSpecies, ProtocolStage
from services.regress.inputs import download_input
from ui.background_task import BackgroundTask
from ui.components.buttons import house_button
from ui.components.copyable import copyable_path
from ui.components.dialogs import dialog_host
from ui.open_project import open_project_in_workspace
from ui.styles import MONO, SANS as FONT

TITLE = f"{FONT} font-size: 12px; font-weight: 700; color: #0f172a;"
LABEL = f"{FONT} font-size: 9px; font-weight: 700; color: #94a3b8; letter-spacing: 0.06em; text-transform: uppercase;"
TEXT = f"{FONT} font-size: 10px; color: #475569; line-height: 1.4;"
VAL = f"{MONO} font-size: 10px; color: #0f172a;"
CHIP = f"{MONO} font-size: 9px; border-radius: 4px; padding: 1px 6px; border: 1px solid #e2e8f0;"
FOOTER = "padding: 10px 16px; background: #f8fafc; border-top: 1px solid #e2e8f0;"
KV_GRID = "display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 2px 14px; width: 100%;"
KEY = f"{FONT} font-size: 10px; color: #94a3b8; white-space: nowrap;"


def fmt_value(v: Any) -> str:
    if v is None or v == "":
        return "—"
    if isinstance(v, bool):
        return "yes" if v else "no"
    if hasattr(v, "value") and not isinstance(v, (str, int, float)):
        return str(v.value)  # Enum members read as their value, like the protocol yaml does
    if isinstance(v, (list, tuple)):
        return ", ".join(fmt_value(x) for x in v)
    if isinstance(v, dict):
        return ", ".join(f"{k}: {fmt_value(x)}" for k, x in v.items())
    return str(v)


def kv_grid(rows: list[tuple[str, Any]]) -> None:
    with ui.element("div").style(KV_GRID):
        for k, v in rows:
            ui.label(k).style(KEY)
            ui.label(fmt_value(v)).style(VAL + " overflow-wrap: anywhere;")


def block_title(text: str, meta: str = "") -> None:
    with ui.row().classes("w-full items-baseline gap-2 no-wrap").style("margin: 12px 0 5px;"):
        ui.label(text).style(f"{FONT} font-size: 11px; font-weight: 600; color: #1e293b;")
        if meta:
            ui.label(meta).style(VAL + " color: #94a3b8;")


def state_chip(text: str, *, ok: bool, tip: str) -> None:
    color = (
        "color: #047857; background: #ecfdf5; border-color: #a7f3d0;"
        if ok
        else "color: #92400e; background: #fffbeb; border-color: #fde68a;"
    )
    ui.label(text).style(CHIP + color).tooltip(tip)


def species_block(sp: ProtocolSpecies) -> None:
    block_title(sp.name, f"species {sp.id}")
    rows: list[tuple[str, Any]] = [("diameter", f"{sp.diameter_ang:g} Å" if sp.diameter_ang else None)]
    rows.append(("symmetry", sp.symmetry))
    if sp.notes:
        rows.append(("notes", sp.notes))
    if sp.template is not None:
        t = sp.template
        rows.append(("template", t.asset))
        rows.append(("template polarity", t.polarity))
        if t.source:
            rows.append(("template source", t.source))
        if t.lowpass_ang:
            rows.append(("template low-pass", f"{t.lowpass_ang:g} Å"))
        if t.notes:
            rows.append(("template notes", t.notes))
    if sp.mask is not None:
        m = sp.mask
        rows.append(("mask", m.asset))
        if m.method:
            rows.append(("mask method", m.method))
        for label, val in (
            ("mask threshold", m.threshold),
            ("mask extend", m.extend_pixels),
            ("mask soft edge", m.soft_edge_pixels),
            ("mask low-pass", m.lowpass_ang),
        ):
            if val is not None:
                rows.append((label, val))
        if m.notes:
            rows.append(("mask notes", m.notes))
    if sp.extraction is not None:
        e = sp.extraction
        rows.append(("extraction box / crop", f"{e.box_size} / {e.crop_size} px"))
        rows.append(("extraction binning", e.binning))
    kv_grid(rows)


def stage_title(st: ProtocolStage) -> tuple[str, str]:
    """(display title, instance id) — a hand-edited bundle can name a job this build doesn't
    know; show it raw rather than hiding the stage."""
    try:
        return display_name(st.job_type), st.instance_id
    except ValueError:
        return st.job, (f"{st.job}__{st.species}" if st.species else st.job)


def stage_block(index: int, st: ProtocolStage) -> None:
    title, instance_id = stage_title(st)
    block_title(f"{index}. {title}", instance_id)
    rows: list[tuple[str, Any]] = []
    if st.species:
        rows.append(("species", st.species))
    rows.extend(st.params.items())
    for slot, producer in st.inputs.items():
        rows.append((f"input {slot}", f"from {producer}"))
    if not rows:
        rows.append(("parameters", "none — code defaults"))
    kv_grid(rows)


def submit_download(info: ProtocolInfo, root: Path) -> None:
    """Fetch the frozen input from EMPIAR into `local_data/input/<protocol>/` as a tray task."""
    protocol = info.protocol
    if protocol is None:
        return

    async def work(progress_cb):
        res = await asyncio.to_thread(download_input, protocol, root, progress_cb=progress_cb)
        if not res["success"]:
            raise RuntimeError(res["error"])
        frozen = " · test/input.yaml frozen (commit it)" if res["frozen_now"] else " · checksums verified"
        return f"{res['n_files']} files, {res['bytes'] / 2**30:.1f} GB → {res['input_dir']}{frozen}"

    def done(task) -> None:
        if task.status == "succeeded":
            ui.notify(task.result_message or "downloaded", type="positive", timeout=10000)
        else:
            ui.notify(f"Download failed: {task.error}", type="negative", timeout=10000)

    BackgroundTask(
        title=f"Download test data · {info.name}", subtitle="EMPIAR → input root", dedup_key=f"fetch:{info.name}"
    ).submit(work, on_complete=done, poll_interval=3.0)


def launched_dialog(backend, ui_mgr, project_path: str, mode: str) -> None:
    """After `launch_run` succeeded (a BackgroundTask's completion callback, so it has a client):
    say where the run project is and offer to open it."""
    with dialog_host(), ui.dialog() as dialog, ui.card().classes("p-0").style("width: 520px; max-width: 95vw;"):
        with ui.column().classes("w-full gap-1").style("padding: 14px 16px 10px;"):
            ui.label(f"Protocol {mode} submitted").style(TITLE)
            ui.label("The chain is in SLURM and the server watches it. Open the run project to follow it.").style(TEXT)
            copyable_path(project_path, font_size="10px")
        with ui.row().classes("w-full justify-end items-center gap-2").style(FOOTER):
            house_button("Stay here", dialog.close)

            async def _open() -> None:
                dialog.close()
                await open_project_in_workspace(backend, ui_mgr, Path(project_path))

            house_button("Open run project", _open, kind="accent")
    dialog.open()

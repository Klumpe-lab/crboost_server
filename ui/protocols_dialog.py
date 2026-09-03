"""Protocols dialog — the landing-page LAUNCHER for protocol bundles and the regression harness
(roadmap 14 S6, refit by 16 S5).

Opened from the status strip's "Protocols" action beside the settings gear. One row per
discovered bundle: name (click → every stage with its parameters) · frozen-input state under
`local_data.root` · baseline state · [Runs] [Record] [Run]. Run / Record create a run PROJECT
under the project base and submit its chain (`launch_run`, a seconds-long BackgroundTask); the
server's PipelineMonitor watches it from there, and the completion dialog offers to open it. The
workspace Protocols view (`ui/protocols_view.py`) is where a run is followed — status, verdicts,
metrics, logs; this dialog only launches and lists.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from nicegui import ui

from services.configs.config_service import get_config_service
from services.protocols.discovery import ProtocolInfo, list_protocols
from services.protocols.schema import Protocol
from services.regress.bands import BANDS_FILE
from services.regress.inputs import check_input, input_dir_for, read_manifest
from services.regress.report import list_runs
from services.regress.runner import cli_runs_root, launch_run
from ui.background_task import BackgroundTask
from ui.components.buttons import house_button
from ui.components.chip import render_chip
from ui.components.copyable import copyable_path
from ui.components.dialogs import dialog_host
from ui.components.reactive import SingleFlight
from ui.dashboard.css import ensure_assets_loaded
from ui.open_project import open_project_in_workspace
from ui.protocols_blocks import (
    FOOTER,
    LABEL,
    TEXT,
    TITLE,
    VAL,
    block_title,
    kv_grid,
    launched_dialog,
    species_block,
    stage_block,
    state_chip,
    submit_download,
)
from ui.protocols_view import VERDICT_STATUS, launch_project_base
from ui.styles import MONO, SANS as FONT
from ui.ui_state import get_ui_state_manager

logger = logging.getLogger(__name__)

_flight = SingleFlight()

# A grid row is a plain div inside a ui.column (align-items: flex-start): without an
# explicit width it shrink-wraps, and the header labels bunched up on the left.
_ROW = (
    "display: grid; grid-template-columns: minmax(0, 1fr) 176px 80px auto; gap: 10px; align-items: center; "
    "padding: 6px 0; width: 100%; box-sizing: border-box;"
)
_LINK = f"{FONT} font-size: 10px; color: #6366f1; cursor: pointer; text-decoration: underline dotted;"


def _facts(info: ProtocolInfo, root: Path | None) -> dict[str, Any]:
    """Disk facts for one row (called off the event loop): frozen input present? how many files
    would a download fetch? is there a baseline?"""
    p = info.protocol
    if p is None or not info.has_test_bundle:
        return {}
    chk = check_input(p, root, verify_checksums=False) if root else None
    return {
        "input": bool(chk and chk.ok),
        "input_dir": str(input_dir_for(p, root)) if root else "",
        "n_files": len(read_manifest(p).get("files") or []),
        "baseline": (info.bundle_dir / "test" / BANDS_FILE).exists(),
    }


async def open_protocols_dialog(backend) -> None:
    async with _flight("protocols") as acquired:
        if not acquired:
            return
        ensure_assets_loaded()
        root = get_config_service().local_data_root
        base = await launch_project_base(backend)
        infos = await asyncio.to_thread(list_protocols)
        facts = await asyncio.to_thread(lambda: {i.name: _facts(i, root) for i in infos})
        with dialog_host(), ui.dialog() as dialog, ui.card().classes("p-0").style("width: 800px; max-width: 95vw;"):
            with ui.column().classes("w-full gap-0").style("padding: 14px 16px 10px;"):
                ui.label("Protocols").style(TITLE)
                ui.label(
                    "Declarative workflows under config/protocols/ and ~/.crboost/protocols/. Click a protocol to "
                    "see every stage and its parameters. A protocol with a test bundle is a regression case: Run "
                    "creates a run project from the frozen input under your project base and submits its chain; "
                    "Record also keeps the settled run as a baseline record. Follow a run in its workspace "
                    "(the protocol light at the foot of the rail)."
                ).style(TEXT)
                with ui.row().classes("items-center gap-2").style("margin-top: 4px;"):
                    ui.label("local data").style(LABEL)
                    ui.label(str(root) if root else "not configured (local_data.root / DefaultProjectBase)").style(
                        VAL if root else TEXT + " color: #b91c1c;"
                    )
                    ui.label("runs go to").style(LABEL)
                    ui.label(str(base)).style(VAL)
            ui.element("div").style("height: 1px; background: #e2e8f0; margin: 0 16px;")
            with ui.column().classes("w-full gap-0").style("padding: 4px 16px 8px;"):
                with ui.element("div").style(_ROW + " border-bottom: 1px solid #f1f5f9;"):
                    for head in ("protocol", "input", "baseline", ""):
                        ui.label(head).style(LABEL)
                if not infos:
                    ui.label("No protocol bundles found.").style(TEXT + " padding: 8px 0;")
                for info in infos:
                    _protocol_row(backend, dialog, info, root, base, facts.get(info.name) or {})
            with ui.row().classes("w-full justify-end items-center gap-2").style(FOOTER):
                house_button("Close", dialog.close)
        dialog.open()
        await dialog


def _protocol_row(backend, dialog, info: ProtocolInfo, root: Path | None, base: Path, facts: dict[str, Any]) -> None:
    with ui.element("div").style(_ROW):
        with ui.column().classes("gap-0").style("min-width: 0;"):
            name = ui.label(info.name).style(VAL + " font-weight: 600;")
            if info.protocol is None:
                ui.label(f"broken: {info.error}").style(TEXT + " color: #b91c1c;")
            else:
                p = info.protocol
                name.style("cursor: pointer; text-decoration: underline dotted #cbd5e1; text-underline-offset: 3px;")
                name.tooltip("Open — every stage and its parameters")
                name.on("click", lambda i=info: _open_protocol_detail(i))
                ui.label(
                    f"version {p.version}, {len(p.stages)} stages, {len(p.species)} species"
                    + (f" — {p.description}" if p.description else "")
                ).style(TEXT).tooltip(str(info.bundle_dir))
        if info.protocol is None or not info.has_test_bundle:
            ui.label("no test bundle" if info.protocol else "").style(TEXT)
            ui.label("").style(TEXT)
            ui.label("").style(TEXT)
            return
        has_input = bool(facts.get("input"))
        with ui.row().classes("items-center gap-1 no-wrap"):
            state_chip(
                "present" if has_input else "missing",
                ok=has_input,
                tip=facts.get("input_dir") or "no input root configured",
            )
            if root and not has_input:
                (
                    ui.button(icon="download", on_click=lambda i=info: submit_download(i, root))
                    .props("flat dense round size=sm")
                    .style("color: #64748b;")
                    .tooltip(
                        f"Download the {facts.get('n_files', '?')} frozen input files from EMPIAR into "
                        f"{facts.get('input_dir')}, then freeze test/input.yaml (resumable; progress in the tray)"
                    )
                )
        has_baseline = bool(facts.get("baseline"))
        state_chip(
            "recorded" if has_baseline else "none",
            ok=has_baseline,
            tip="test/bands.yaml"
            if has_baseline
            else "No baseline yet — record three runs, then derive the bands from the records "
            "(`crboost_regress.py bless`)",
        )
        with ui.row().classes("items-center gap-1 no-wrap"):
            house_button(
                "Runs", lambda i=info: _open_runs(backend, dialog, i, root, base), tooltip="Every run of this protocol"
            )
            house_button(
                "Record",
                lambda i=info: _submit(backend, i, "record", base),
                tooltip="Run, then keep the settled run's metrics + artifacts as a baseline record",
            )
            house_button(
                "Run",
                lambda i=info: _submit(backend, i, "run", base),
                kind="accent",
                tooltip=f"Create a run project under {base}, submit its chain, and evaluate every stage "
                "against the recorded bands when it settles",
            )


# ── Protocol detail: every species and every stage with its full parameter set ──


def _open_protocol_detail(info: ProtocolInfo) -> None:
    p: Protocol | None = info.protocol
    if p is None:
        return
    with dialog_host(), ui.dialog() as dialog, ui.card().classes("p-0").style("width: 860px; max-width: 95vw;"):
        with ui.column().classes("w-full gap-0").style("padding: 14px 16px 8px;"):
            with ui.row().classes("w-full items-baseline gap-2 no-wrap"):
                ui.label(p.name).style(TITLE)
                ui.label(f"version {p.version}").style(VAL + " color: #94a3b8;")
            if p.description:
                ui.label(p.description).style(TEXT)
            copyable_path(str(info.bundle_dir), font_size="9px", color="#94a3b8")
        ui.element("div").style("height: 1px; background: #e2e8f0; margin: 0 16px;")
        with ui.scroll_area().classes("w-full cb-scroll-tight").style("height: 70vh;"):
            with ui.column().classes("w-full gap-0").style("padding: 2px 16px 14px;"):
                if p.expects:
                    block_title("Expected dataset facts", "warned at apply, never blocking")
                    kv_grid(
                        [(k, f"about {e.about:g}" + (f" ± {e.tol:g}" if e.tol else "")) for k, e in p.expects.items()]
                    )
                for sp in p.species:
                    species_block(sp)
                for i, st in enumerate(p.stages, start=1):
                    stage_block(i, st)
        with ui.row().classes("w-full justify-end items-center gap-2").style(FOOTER):
            house_button("Close", dialog.close)
    dialog.open()


# ── Run / record / runs ──


def _submit(backend, info: ProtocolInfo, mode: str, base: Path) -> None:
    """Launch = apply + submit (seconds) as a tray task; the run itself is a project the
    server's monitor watches. `base` was resolved on the page — a background task has no
    client context to read preferences from."""
    protocol = info.protocol
    if protocol is None:
        return
    ui_mgr = get_ui_state_manager()

    async def work(progress_cb):
        res = await launch_run(backend, protocol, mode=mode, project_base=base, progress_cb=progress_cb)
        if not res["success"]:
            raise RuntimeError(res["error"])
        return res["project_path"]

    def done(task) -> None:
        if task.status == "succeeded" and task.result_message:
            launched_dialog(backend, ui_mgr, task.result_message, mode)
        elif task.status != "succeeded":
            ui.notify(f"Protocol {mode} failed: {task.error}", type="negative", timeout=10000)

    BackgroundTask(
        title=f"Protocol {mode} · {info.name}",
        subtitle=f"apply + submit under {base}",
        dedup_key=f"regress:{info.name}",
    ).submit(work, on_complete=done, poll_interval=2.0)


def _open_runs(backend, parent_dialog, info: ProtocolInfo, root: Path | None, base: Path) -> None:
    roots = [base] + ([cli_runs_root(root)] if root else [])
    rows = list_runs(roots, info.name)
    ui_mgr = get_ui_state_manager()
    with dialog_host(), ui.dialog() as dialog, ui.card().classes("p-0").style("width: 760px; max-width: 95vw;"):
        with ui.column().classes("w-full gap-0").style("padding: 14px 16px 10px;"):
            ui.label(f"Runs of {info.name}").style(TITLE)
            ui.label("Under the project base (UI launches) and local_data/runs (CLI launches).").style(TEXT)
            if not rows:
                ui.label("No runs yet.").style(TEXT + " padding: 6px 0;")
            for r in rows:
                project_dir = str(r.get("project_dir") or "")
                with ui.row().classes("w-full items-center gap-3 no-wrap").style("padding: 3px 0;"):
                    ui.label(str(r.get("started_at") or "?")[:19]).style(VAL)
                    ui.label(str(r.get("mode") or "?")).style(TEXT)
                    ui.label(str(r.get("driver") or "?")).style(f"{MONO} font-size: 9px; color: #94a3b8;")
                    if r.get("error"):
                        render_chip("run.json", "unreadable", status="error", tooltip=str(r["error"]))
                    else:
                        v = str(r.get("verdict") or "?")
                        render_chip(
                            "verdict",
                            v,
                            status=VERDICT_STATUS.get(v, "warn"),
                            tooltip=f"{r.get('n_pass', '?')}/{r.get('n_stages', '?')} stages PASS or unbanded",
                        )
                    ui.label(Path(project_dir).name if project_dir else "?").style(VAL + " font-weight: 600;").tooltip(
                        project_dir
                    )
                    ui.space()
                    if project_dir:

                        async def _open(_e=None, target=project_dir) -> None:
                            dialog.close()
                            parent_dialog.close()
                            await open_project_in_workspace(backend, ui_mgr, Path(target))

                        ui.label("open ↗").style(_LINK).on("click", _open).tooltip("Open this run project")
        with ui.row().classes("w-full justify-end items-center gap-2").style(FOOTER):
            house_button("Close", dialog.close)
    dialog.open()

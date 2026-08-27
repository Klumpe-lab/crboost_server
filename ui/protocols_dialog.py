"""Protocols dialog — the landing-page entry to protocol bundles and the regression harness
(roadmap 14 S6).

Opened from the status strip's "Protocols" action beside the settings gear. One row per
discovered protocol bundle: name (click to open it — every stage with its parameters) ·
input state under `local_data.root` · bands state · [Report] [Record] [Run]. Run/Record
submit `run_protocol` through the BackgroundTask registry (`dedup_key="regress:<name>"`),
so the tray carries stage progress and the toast links the report; the run itself lives in
SLURM either way.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from nicegui import ui

from services.configs.config_service import get_config_service
from services.jobs.spec import display_name
from services.protocols.discovery import ProtocolInfo, list_protocols
from services.protocols.schema import Protocol, ProtocolSpecies, ProtocolStage
from services.regress.bands import BANDS_FILE
from services.regress.inputs import check_input, download_input, input_dir_for, read_manifest
from services.regress.report import list_runs
from services.regress.runner import run_protocol
from ui.background_task import BackgroundTask
from ui.components.buttons import house_button
from ui.components.copyable import copyable_path
from ui.components.dialogs import dialog_host
from ui.components.reactive import SingleFlight
from ui.dashboard.css import ensure_assets_loaded
from ui.styles import MONO, SANS as FONT

logger = logging.getLogger(__name__)

_flight = SingleFlight()

_TITLE = f"{FONT} font-size: 12px; font-weight: 700; color: #0f172a;"
_LABEL = f"{FONT} font-size: 9px; font-weight: 700; color: #94a3b8; letter-spacing: 0.06em; text-transform: uppercase;"
_TEXT = f"{FONT} font-size: 10px; color: #475569; line-height: 1.4;"
_VAL = f"{MONO} font-size: 10px; color: #0f172a;"
_CHIP = f"{MONO} font-size: 9px; border-radius: 4px; padding: 1px 6px; border: 1px solid #e2e8f0;"
# A grid row is a plain div inside a ui.column (align-items: flex-start): without an
# explicit width it shrink-wraps, and the header labels bunched up on the left.
_ROW = (
    "display: grid; grid-template-columns: minmax(0, 1fr) 176px 80px auto; gap: 10px; align-items: center; "
    "padding: 6px 0; width: 100%; box-sizing: border-box;"
)
_FOOTER = "padding: 10px 16px; background: #f8fafc; border-top: 1px solid #e2e8f0;"
_KV_GRID = "display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 2px 14px; width: 100%;"
_KEY = f"{FONT} font-size: 10px; color: #94a3b8; white-space: nowrap;"


async def open_protocols_dialog(backend) -> None:
    async with _flight("protocols") as acquired:
        if not acquired:
            return
        ensure_assets_loaded()
        root = get_config_service().local_data_root
        infos = list_protocols()
        with dialog_host(), ui.dialog() as dialog, ui.card().classes("p-0").style("width: 800px; max-width: 95vw;"):
            with ui.column().classes("w-full gap-0").style("padding: 14px 16px 10px;"):
                ui.label("Protocols").style(_TITLE)
                ui.label(
                    "Declarative workflows under config/protocols/ and ~/.crboost/protocols/. Click a protocol to "
                    "see every stage and its parameters. A protocol with a test bundle is a regression case: Run "
                    "applies it to the frozen input, submits the chain and checks every stage against the recorded "
                    "bands; Record also keeps the run as a baseline record."
                ).style(_TEXT)
                with ui.row().classes("items-center gap-2").style("margin-top: 4px;"):
                    ui.label("local data").style(_LABEL)
                    ui.label(str(root) if root else "not configured (local_data.root / DefaultProjectBase)").style(
                        _VAL if root else _TEXT + " color: #b91c1c;"
                    )
            ui.element("div").style("height: 1px; background: #e2e8f0; margin: 0 16px;")
            with ui.column().classes("w-full gap-0").style("padding: 4px 16px 8px;"):
                with ui.element("div").style(_ROW + " border-bottom: 1px solid #f1f5f9;"):
                    for head in ("protocol", "input", "bands", ""):
                        ui.label(head).style(_LABEL)
                if not infos:
                    ui.label("No protocol bundles found.").style(_TEXT + " padding: 8px 0;")
                for info in infos:
                    _protocol_row(backend, info, root)
            with ui.row().classes("w-full justify-end items-center gap-2").style(_FOOTER):
                house_button("Close", dialog.close)
        dialog.open()
        await dialog


def _protocol_row(backend, info: ProtocolInfo, root: Path | None) -> None:
    with ui.element("div").style(_ROW):
        with ui.column().classes("gap-0").style("min-width: 0;"):
            name = ui.label(info.name).style(_VAL + " font-weight: 600;")
            if info.protocol is None:
                ui.label(f"broken: {info.error}").style(_TEXT + " color: #b91c1c;")
            else:
                p = info.protocol
                name.style("cursor: pointer; text-decoration: underline dotted #cbd5e1; text-underline-offset: 3px;")
                name.tooltip("Open — every stage and its parameters")
                name.on("click", lambda i=info: _open_protocol_detail(i))
                ui.label(
                    f"version {p.version}, {len(p.stages)} stages, {len(p.species)} species"
                    + (f" — {p.description}" if p.description else "")
                ).style(_TEXT).tooltip(str(info.bundle_dir))
        if info.protocol is None or not info.has_test_bundle:
            ui.label("no test bundle" if info.protocol else "").style(_TEXT)
            ui.label("").style(_TEXT)
            ui.label("").style(_TEXT)
            return
        input_dir = input_dir_for(info.protocol, root) if root else None
        chk = check_input(info.protocol, root, verify_checksums=False) if root else None
        has_input = bool(chk and chk.ok)
        with ui.row().classes("items-center gap-1 no-wrap"):
            _chip(
                "present" if has_input else "missing",
                ok=has_input,
                tip=str(input_dir) if input_dir else "no input root configured",
            )
            if root and not has_input:
                n_files = len(read_manifest(info.protocol).get("files") or [])
                (
                    ui.button(icon="download", on_click=lambda i=info: _submit_download(i, root))
                    .props("flat dense round size=sm")
                    .style("color: #64748b;")
                    .tooltip(
                        f"Download the {n_files} frozen input files from EMPIAR into {input_dir}, then freeze "
                        "test/input.yaml (resumable; progress in the tray)"
                    )
                )
        has_bands = (info.bundle_dir / "test" / BANDS_FILE).exists()
        _chip(
            "recorded" if has_bands else "none",
            ok=has_bands,
            tip="test/bands.yaml" if has_bands else "No bands yet — record three runs, then derive the bands from them",
        )
        with ui.row().classes("items-center gap-1 no-wrap"):
            house_button("Report", lambda i=info: _open_report(i, root), tooltip="Past runs of this protocol")
            house_button(
                "Record",
                lambda i=info: _submit(backend, i, "record"),
                tooltip="Run, then keep the metrics + artifacts as a baseline record",
            )
            house_button(
                "Run",
                lambda i=info: _submit(backend, i, "run"),
                kind="accent",
                tooltip="Apply to the frozen input, submit the chain, check every stage — progress in the tray",
            )


def _chip(text: str, *, ok: bool, tip: str) -> None:
    color = (
        "color: #047857; background: #ecfdf5; border-color: #a7f3d0;"
        if ok
        else "color: #92400e; background: #fffbeb; border-color: #fde68a;"
    )
    ui.label(text).style(_CHIP + color).tooltip(tip)


# ── Protocol detail: every species and every stage with its full parameter set ──


def _fmt_value(v: Any) -> str:
    if v is None or v == "":
        return "—"
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, (list, tuple)):
        return ", ".join(_fmt_value(x) for x in v)
    if isinstance(v, dict):
        return ", ".join(f"{k}: {_fmt_value(x)}" for k, x in v.items())
    return str(v)


def _kv_grid(rows: list[tuple[str, Any]]) -> None:
    with ui.element("div").style(_KV_GRID):
        for k, v in rows:
            ui.label(k).style(_KEY)
            ui.label(_fmt_value(v)).style(_VAL + " overflow-wrap: anywhere;")


def _block_title(text: str, meta: str = "") -> None:
    with ui.row().classes("w-full items-baseline gap-2 no-wrap").style("margin: 12px 0 5px;"):
        ui.label(text).style(f"{FONT} font-size: 11px; font-weight: 600; color: #1e293b;")
        if meta:
            ui.label(meta).style(_VAL + " color: #94a3b8;")


def _species_block(sp: ProtocolSpecies) -> None:
    _block_title(sp.name, f"species {sp.id}")
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
    _kv_grid(rows)


def _stage_block(index: int, st: ProtocolStage) -> None:
    try:
        title = display_name(st.job_type)
        instance_id = st.instance_id
    except ValueError:
        # A hand-edited bundle can name a job this build doesn't know; show it raw
        # rather than hiding the stage.
        title = st.job
        instance_id = f"{st.job}__{st.species}" if st.species else st.job
    _block_title(f"{index}. {title}", instance_id)
    rows: list[tuple[str, Any]] = []
    if st.species:
        rows.append(("species", st.species))
    rows.extend(st.params.items())
    for slot, producer in st.inputs.items():
        rows.append((f"input {slot}", f"from {producer}"))
    if not rows:
        rows.append(("parameters", "none — code defaults"))
    _kv_grid(rows)


def _open_protocol_detail(info: ProtocolInfo) -> None:
    p: Protocol | None = info.protocol
    if p is None:
        return
    with dialog_host(), ui.dialog() as dialog, ui.card().classes("p-0").style("width: 860px; max-width: 95vw;"):
        with ui.column().classes("w-full gap-0").style("padding: 14px 16px 8px;"):
            with ui.row().classes("w-full items-baseline gap-2 no-wrap"):
                ui.label(p.name).style(_TITLE)
                ui.label(f"version {p.version}").style(_VAL + " color: #94a3b8;")
            if p.description:
                ui.label(p.description).style(_TEXT)
            copyable_path(str(info.bundle_dir), font_size="9px", color="#94a3b8")
        ui.element("div").style("height: 1px; background: #e2e8f0; margin: 0 16px;")
        with ui.scroll_area().classes("w-full cb-scroll-tight").style("height: 70vh;"):
            with ui.column().classes("w-full gap-0").style("padding: 2px 16px 14px;"):
                if p.expects:
                    _block_title("Expected dataset facts", "warned at apply, never blocking")
                    _kv_grid(
                        [(k, f"about {e.about:g}" + (f" ± {e.tol:g}" if e.tol else "")) for k, e in p.expects.items()]
                    )
                for sp in p.species:
                    _species_block(sp)
                for i, st in enumerate(p.stages, start=1):
                    _stage_block(i, st)
        with ui.row().classes("w-full justify-end items-center gap-2").style(_FOOTER):
            house_button("Close", dialog.close)
    dialog.open()


# ── Run / record / download / report ──


def _submit(backend, info: ProtocolInfo, mode: str) -> None:
    protocol = info.protocol
    if protocol is None:
        return

    async def work(progress_cb):
        rec = await run_protocol(backend, protocol, mode=mode, progress_cb=progress_cb)
        return f"{rec.summary()} — report.md"

    def done(task) -> None:
        if task.status == "succeeded":
            ui.notify(task.result_message or "finished", type="positive", timeout=8000)
        else:
            ui.notify(f"Protocol {mode} failed: {task.error}", type="negative", timeout=8000)

    BackgroundTask(
        title=f"Protocol {mode} · {info.name}", subtitle="regression harness", dedup_key=f"regress:{info.name}"
    ).submit(work, on_complete=done, poll_interval=5.0)


def _submit_download(info: ProtocolInfo, root: Path) -> None:
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


def _open_report(info: ProtocolInfo, root: Path | None) -> None:
    rows = list_runs(root, info.name) if root else []
    with dialog_host(), ui.dialog() as dialog, ui.card().classes("p-0").style("width: 720px; max-width: 95vw;"):
        with ui.column().classes("w-full gap-0").style("padding: 14px 16px 10px;"):
            ui.label(f"Runs of {info.name}").style(_TITLE)
            if not rows:
                ui.label("No runs yet.").style(_TEXT)
            for r in rows:
                with ui.row().classes("items-center gap-3 no-wrap").style("padding: 3px 0;"):
                    ui.label(str(r.get("started_at", "?"))).style(_VAL)
                    ui.label(str(r.get("mode", "?"))).style(_TEXT)
                    _chip(
                        str(r.get("verdict", "?")), ok=r.get("verdict") in ("PASS", "DRIFT"), tip=str(r.get("run_dir"))
                    )
                    ui.label(f"{r.get('n_pass', '?')}/{r.get('n_stages', '?')} stages").style(_TEXT)
                    ui.label(str(r.get("report", ""))).style(_VAL + " color: #475569;")
        with ui.row().classes("w-full justify-end items-center gap-2").style(_FOOTER):
            house_button("Close", dialog.close)
    dialog.open()

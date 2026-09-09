"""Protocols dialog — the landing-page entry to protocol bundles (roadmap 16).

Opened from the status strip's "Protocols" action beside the settings gear. One row per discovered
bundle: name (click → every stage with its parameters) · version, stages, species, description ·
[Create project]. Create = the regular project creation with the protocol's species and stages
applied (`apply_protocol`), then the workspace with the roster populated — the user presses the
normal Run. The workspace Protocols view (`ui/protocols_view.py`) is where the protocol's parameters
are read beside the project's.
"""

from __future__ import annotations

import asyncio
import glob
import logging
from datetime import datetime
from pathlib import Path

from nicegui import app, ui

from services.configs.user_prefs_service import get_prefs_service
from services.protocols.apply import apply_protocol
from services.protocols.discovery import ProtocolInfo, list_protocols
from services.protocols.schema import Protocol
from ui.components.buttons import house_button
from ui.components.copyable import copyable_path
from ui.components.dialogs import dialog_host
from ui.components.fields import house_text
from ui.components.reactive import SingleFlight
from ui.dashboard.css import ensure_assets_loaded
from ui.open_project import open_project_in_workspace
from ui.protocols_blocks import FOOTER, TEXT, TITLE, VAL, species_block, stage_block
from ui.ui_state import get_ui_state_manager

logger = logging.getLogger(__name__)

_flight = SingleFlight()

# A grid row is a plain div inside a ui.column (align-items: flex-start): without an
# explicit width it shrink-wraps, so the row carries width: 100% itself.
_ROW = (
    "display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 10px; align-items: center; "
    "padding: 6px 0; width: 100%; box-sizing: border-box;"
)


async def open_protocols_dialog(backend) -> None:
    async with _flight("protocols") as acquired:
        if not acquired:
            return
        ensure_assets_loaded()
        infos = await asyncio.to_thread(list_protocols)
        with dialog_host(), ui.dialog() as dialog, ui.card().classes("p-0").style("width: 720px; max-width: 95vw;"):
            with ui.column().classes("w-full gap-0").style("padding: 14px 16px 10px;"):
                ui.label("Protocols").style(TITLE)
                ui.label(
                    "The shape of a pipeline with its parameters, as a bundle under config/protocols/ or "
                    "~/.crboost/protocols/. Click a protocol to see every stage and its parameters. Create project "
                    "makes a regular project with those species and stages, ready to Run."
                ).style(TEXT)
            ui.element("div").style("height: 1px; background: #e2e8f0; margin: 0 16px;")
            with ui.column().classes("w-full gap-0").style("padding: 4px 16px 8px;"):
                if not infos:
                    ui.label("No protocol bundles found.").style(TEXT + " padding: 8px 0;")
                for info in infos:
                    _protocol_row(backend, dialog, info)
            with ui.row().classes("w-full justify-end items-center gap-2").style(FOOTER):
                house_button("Close", dialog.close)
        dialog.open()
        await dialog


def _protocol_row(backend, dialog, info: ProtocolInfo) -> None:
    with ui.element("div").style(_ROW + " border-bottom: 1px solid #f1f5f9;"):
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
        if info.protocol is not None:
            house_button(
                "Create project",
                lambda i=info: _open_create_dialog(backend, dialog, i),
                kind="accent",
                tooltip="A regular project with this protocol's species and stages, ready to Run",
            )
        else:
            ui.label("").style(TEXT)


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
                for sp in p.species:
                    species_block(sp)
                for i, st in enumerate(p.stages, start=1):
                    stage_block(i, st)
        with ui.row().classes("w-full justify-end items-center gap-2").style(FOOTER):
            house_button("Close", dialog.close)
    dialog.open()


# ── Create project ──


async def _open_create_dialog(backend, parent_dialog, info: ProtocolInfo) -> None:
    """The regular project creation, with the protocol applied: `apply_protocol` creates the
    project through the same facade as the landing form, registers the species with their assets
    and instantiates every stage with its parameters; then the workspace opens with the roster
    populated and the normal Run button submits exactly those stages."""
    protocol = info.protocol
    if protocol is None:
        return
    async with _flight("create") as acquired:
        if not acquired:
            return
        ui_mgr = get_ui_state_manager()
        defaults = await backend.get_default_data_globs()
        base_default = ui_mgr.data_import.project_base_path or await backend.get_default_project_base()
        with dialog_host(), ui.dialog() as dialog, ui.card().classes("p-0").style("width: 640px; max-width: 95vw;"):
            with ui.column().classes("w-full gap-2").style("padding: 14px 16px 10px;"):
                ui.label(f"Create project from {protocol.name} v{protocol.version}").style(TITLE)
                ui.label(
                    f"{len(protocol.stages)} stages and {len(protocol.species)} species are applied to a new project "
                    "on the data below; dataset facts (pixel size, dose, tilt axis) come from the mdocs."
                ).style(TEXT)
                name_in = house_text("name", width="w-full", value=f"{protocol.name}-{datetime.now():%Y%m%d-%H%M}")
                base_in = house_text("base", width="w-full", value=str(base_default))
                movies_in = house_text("movies", width="w-full", value=defaults.get("movies", ""))
                mdocs_in = house_text("mdocs", width="w-full", value=defaults.get("mdocs", ""))
                gain_in = house_text("gain ref", width="w-full", placeholder="optional")

            async def _create() -> None:
                async with _flight("create-submit") as got:
                    if not got:
                        return
                    name = (name_in.value or "").strip()
                    base = (base_in.value or "").strip()
                    movies = (movies_in.value or "").strip()
                    mdocs = (mdocs_in.value or "").strip()
                    if not (name and base and movies and mdocs):
                        ui.notify("Name, base, movies and mdocs are required.", type="warning")
                        return
                    if not await asyncio.to_thread(glob.glob, mdocs):
                        ui.notify(f"No mdoc matches {mdocs}", type="warning")
                        return
                    ui.notify(f"Creating {name}…", timeout=3000)
                    res = await apply_protocol(
                        backend,
                        protocol,
                        project_name=name,
                        project_base_path=base,
                        movies_glob=movies,
                        mdocs_glob=mdocs,
                        gain_reference_path=(gain_in.value or "").strip() or None,
                    )
                    if not res["success"]:
                        ui.notify(f"Create failed: {res['error']}", type="negative", timeout=10000)
                        return
                    warnings = res.get("warnings", [])
                    for w in warnings[:3]:
                        ui.notify(w, type="warning", timeout=8000)
                    if len(warnings) > 3:
                        ui.notify(f"+{len(warnings) - 3} more apply warnings — see the server log", type="warning")
                    prefs = get_prefs_service()
                    prefs.prefs.add_recent_root(base, label=name)
                    prefs.save_to_app_storage(app.storage.user)
                    dialog.close()
                    parent_dialog.close()
                    await open_project_in_workspace(backend, ui_mgr, Path(res["project_path"]))

            with ui.row().classes("w-full justify-end items-center gap-2").style(FOOTER):
                house_button("Cancel", dialog.close)
                house_button("Create", _create, kind="accent")
        dialog.open()

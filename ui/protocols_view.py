"""Protocols view — the workspace surface of roadmap 16.

Opened from the foot-of-rail protocol light (`pipeline_roster._build_protocol_btn`) and swapped
into the main area like the Journey / Tomograms views. One header, two levels:

- the header is the protocol SWITCHER (the Journey's caret + dropdown idiom): the selected
  protocol's name, version and description, every discovered bundle behind the caret;
- THIS PROJECT — when this project was created from the selected protocol
  (`ProjectState.protocol_origin`): per stage, the parameters the protocol pinned beside what the
  job holds now, with the ones a person changed since highlighted ("edited");
- PROTOCOL — the bundle as declared (species, stages with their parameters) and "save this
  project as a protocol".

A protocol is the shape of a pipeline with its parameters, nothing more: results, logs and
statuses live where they always did (the roster, the job tabs, the Journey). Disk reads happen
off the event loop; the two FingerprintedViews rebuild only when their signature moves.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from nicegui import ui

from services.models_base import JobStatus
from services.project_state import ProjectState, get_project_state_for
from services.protocols.apply import stage_edits
from services.protocols.discovery import USER_PROTOCOLS_DIR, ProtocolInfo, list_protocols
from services.protocols.export import export_protocol
from services.protocols.schema import PROJECT_PROTOCOL_DIRNAME, Protocol, dump_protocol, load_protocol
from ui.components.buttons import house_button
from ui.components.chip import render_chip
from ui.components.fields import house_text
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.dashboard.css import ensure_assets_loaded
from ui.job_plugins._field_styles import PAGE_SECTION_STYLE
from ui.protocols_blocks import KEY, LABEL, TEXT, VAL, fmt_value, kv_grid, species_block, stage_block, stage_title
from ui.styles import MONO, SANS as FONT

logger = logging.getLogger(__name__)

_TICK_S = 15.0  # the view's poll re-reads the frozen copy (disk) → 15 s per CLAUDE.md
_LIVE = (JobStatus.RUNNING, JobStatus.QUEUED)
_GRID3 = "display: grid; grid-template-columns: max-content minmax(0, 1fr) minmax(0, 1fr); gap: 2px 14px; width: 100%;"
_EDITED_CELL = "background: #fffbeb; border-radius: 3px; padding: 0 4px; color: #b45309;"


# ── shared with the rail light ────────────────────────────────────────────────


def protocol_light(project_path: Path | None) -> tuple[str, str]:
    """(kind, tooltip) for the rail light — off (not from a protocol) · lit (created from one) ·
    live (its pipeline is running). In-memory state only; safe on a 6 s tick."""
    if project_path is None:
        return "off", "No project."
    state = get_project_state_for(project_path)
    origin = state.protocol_origin
    if origin is None:
        return "off", "Not created from a protocol. Click for the protocol library."
    head = f"{origin.name} v{origin.version}"
    if state.pipeline_active:
        ids = [i for i in state.pipeline_order if i in state.jobs]
        done = sum(1 for i in ids if state.jobs[i].execution_status == JobStatus.SUCCEEDED)
        live = [i for i in ids if state.jobs[i].execution_status in _LIVE]
        what = ", ".join(live) if live else "settling"
        return "live", f"{head} — running: {what} · {done}/{len(ids)} done. Click for the Protocols view."
    return "lit", f"Created from {head}. Click for the Protocols view."


def _section(title: str, tooltip: str | None = None) -> None:
    lab = ui.label(title).style(PAGE_SECTION_STYLE)
    if tooltip:
        lab.tooltip(tooltip)


# ── the page ──────────────────────────────────────────────────────────────────


class ProtocolsPage:
    """Built once (lazily, on the first click of the protocol light), like the Tomograms wall."""

    def __init__(self, container, backend, ui_mgr, callbacks: dict | None = None) -> None:
        self.container = container
        self.backend = backend
        self.ui_mgr = ui_mgr
        self.callbacks = callbacks or {}
        self.project_path = Path(ui_mgr.project_path)
        self.infos: list[ProtocolInfo] = []
        self.selected: str | None = None
        self.frozen: Protocol | None = None  # <project>/protocol/protocol.yaml, when this project has an origin
        self.frozen_error: str | None = None
        self._refs: dict[str, Any] = {}
        self._flight = SingleFlight()
        self._built = False
        self._active = False
        self.params_view: _ParamsView | None = None
        self.protocol_view: _ProtocolView | None = None
        self._timer = ui.timer(_TICK_S, self._tick)
        self._timer.deactivate()

    # ── state accessors ───────────────────────────────────────────────────────

    @property
    def state(self) -> ProjectState:
        # Explicit-path resolution, not the tab accessor: the tick runs from a timer.
        return get_project_state_for(self.project_path)

    def info_for(self, name: str | None) -> ProtocolInfo | None:
        return next((i for i in self.infos if i.name == name), None)

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def show(self) -> None:
        async with self._flight("show") as acquired:
            if not acquired:
                return
            if not self._built:
                await self._build()
            else:
                await self.refresh()

    def set_active(self, on: bool) -> None:
        self._active = on
        if on:
            self._timer.activate()
        else:
            self._timer.deactivate()

    async def _tick(self) -> None:
        if not self._active or not self._built:
            return
        await self._collect()
        await self.refresh()

    async def refresh(self) -> None:
        if self.params_view is not None:
            self.params_view.refresh()
        if self.protocol_view is not None:
            self.protocol_view.refresh()

    async def _build(self) -> None:
        ensure_assets_loaded()
        self.container.clear()
        with self.container, ui.element("div").classes("cb-empty"):
            ui.spinner(size="lg", color="indigo")
            ui.label("Reading protocols…").classes("text-xs")
        await asyncio.sleep(0.03)  # flush the spinner before the disk pass
        await self._collect(all_protocols=True)
        origin = self.state.protocol_origin
        names = [i.name for i in self.infos]
        self.selected = origin.name if origin is not None and origin.name in names else (names[0] if names else None)
        self.container.clear()
        with self.container:
            with (
                ui.column()
                .classes("w-full")
                .style("height: 100%; overflow-y: auto; padding: 10px 18px 24px; gap: 0; box-sizing: border-box;")
            ):
                self._refs["header"] = ui.element("div").classes("w-full")
                self._refs["params_host"] = ui.element("div").classes("w-full")
                self._refs["proto_host"] = ui.element("div").classes("w-full")
        self._build_header()
        self.params_view = _ParamsView(self._refs["params_host"], self)
        self.protocol_view = _ProtocolView(self._refs["proto_host"], self)
        self._built = True
        await self.refresh()

    async def _collect(self, *, all_protocols: bool = False) -> None:
        """Disk, off the loop: the bundles (on build / on demand) and the project's frozen copy."""
        has_origin = self.state.protocol_origin is not None
        reread = all_protocols or not self.infos

        def _read():
            infos = list_protocols() if reread else self.infos
            frozen, frozen_error = None, None
            if has_origin:
                try:
                    frozen = load_protocol(self.project_path / PROJECT_PROTOCOL_DIRNAME)
                except (FileNotFoundError, ValueError) as e:
                    frozen_error = str(e)
            return infos, frozen, frozen_error

        self.infos, self.frozen, self.frozen_error = await asyncio.to_thread(_read)

    async def select(self, name: str) -> None:
        if name == self.selected:
            return
        self.selected = name
        self._build_header()
        await self.refresh()

    # ── header: the switcher ──────────────────────────────────────────────────

    def _build_header(self) -> None:
        host = self._refs.get("header")
        if host is None:
            return
        host.clear()
        info = self.info_for(self.selected)
        origin = self.state.protocol_origin
        with host, ui.element("div").classes("cb-jhead").style("margin-bottom: 14px;"):
            with ui.element("div").classes("cb-jhead-left"):
                if info is None:
                    ui.label("No protocol bundles under config/protocols/ or ~/.crboost/protocols/.").classes(
                        "cb-jhead-empty"
                    )
                else:
                    p = info.protocol
                    with ui.element("div").classes("cb-jswitch"):
                        ui.label("▾").classes("cb-jswitch-caret")
                        ui.label(info.name).classes("cb-jswitch-pos").tooltip(str(info.bundle_dir))
                        ui.label(f"v{p.version}" if p else "broken").classes("cb-jswitch-idx").tooltip(
                            "Switch protocol"
                        )
                        menu = ui.menu().props("anchor='bottom left' self='top left' max-height=85vh max-width=96vw")
                        with menu:
                            self._switcher_table(menu)
                    ui.element("div").classes("cb-jsep")
                    if p is None:
                        ui.label(f"broken: {info.error}").style(TEXT + " color: #b91c1c;")
                    elif p.description:
                        ui.label(p.description).style(TEXT)
                    if origin is not None and origin.name == info.name:
                        render_chip(
                            "this project",
                            f"from v{origin.version}",
                            status="info",
                            tooltip=f"Created from {origin.name} v{origin.version} on {origin.applied_at or '?'}",
                        )

    def _switcher_table(self, menu) -> None:
        cols = "28px minmax(170px, 1fr) repeat(3, max-content)"
        with ui.element("div").classes("cb-jmenu").style(f"--cb-jcols: {cols};"):
            with ui.element("div").classes("cb-jrow cb-jrow-head"):
                for head in ("#", "Protocol", "version", "stages", "species"):
                    ui.label(head).classes("cb-jh")
            for i, info in enumerate(self.infos, start=1):
                row = ui.element("div").classes("cb-jrow" + (" selected" if info.name == self.selected else ""))

                def _pick(_e=None, n=info.name):
                    menu.close()
                    return self.select(n)

                row.on("click", _pick)
                with row:
                    ui.label(str(i)).classes("cb-jcell cb-jcell-idx")
                    with ui.element("div").classes("cb-jcell cb-jcell-name").tooltip(str(info.bundle_dir)):
                        ui.label(info.name).classes("cb-jname")
                        ui.label(info.protocol.description if info.protocol else f"broken: {info.error}").classes(
                            "cb-jname-full"
                        )
                    p = info.protocol
                    ui.label(f"v{p.version}" if p else "—").classes("cb-jcell")
                    ui.label(str(len(p.stages)) if p else "—").classes("cb-jcell")
                    ui.label(str(len(p.species)) if p else "—").classes("cb-jcell")

    # ── actions ───────────────────────────────────────────────────────────────

    async def save_as_protocol(self, name: str) -> None:
        name = (name or "").strip()
        if not name or "/" in name or name.startswith("."):
            ui.notify("Give the protocol a plain directory name (no slash).", type="warning")
            return
        target = USER_PROTOCOLS_DIR / name
        if target.exists():
            ui.notify(f"{target} already exists — pick another name (bump the version in its yaml).", type="negative")
            return
        state = self.state

        def _do() -> list[str]:
            protocol, warnings = export_protocol(
                state, name=name, bundle_dir=target, description=f"exported from {state.project_name}"
            )
            dump_protocol(protocol, target)
            return warnings

        try:
            warnings = await asyncio.to_thread(_do)
        except Exception as e:
            logger.exception("export to %s failed", target)
            ui.notify(f"Export failed: {e}", type="negative", timeout=8000)
            return
        ui.notify(f"Saved protocol '{name}' to {target}", type="positive", timeout=6000)
        for w in warnings[:3]:
            ui.notify(w, type="warning", timeout=8000)
        if len(warnings) > 3:
            ui.notify(f"+{len(warnings) - 3} more export warnings — see the server log", type="warning")
        await self._collect(all_protocols=True)
        self._build_header()
        await self.refresh()


# ── THIS PROJECT: its parameters beside the protocol's ────────────────────────


class _ParamsView(FingerprintedView):
    def __init__(self, container, page: ProtocolsPage) -> None:
        super().__init__(container)
        self.page = page

    def _visible(self) -> bool:
        origin = self.page.state.protocol_origin
        return origin is not None and origin.name == self.page.selected

    def _stage_ids(self) -> list[str]:
        if self.page.frozen is not None:
            return self.page.frozen.stage_ids()
        state = self.page.state
        return [i for i in state.pipeline_order if i in state.jobs]

    def _edits(self) -> dict[str, list[tuple[str, Any, Any]]]:
        frozen = self.page.frozen
        return stage_edits(self.page.state, frozen) if frozen is not None else {}

    def signature(self) -> Any:
        if not self._visible():
            return ("hidden", self.page.selected)
        state = self.page.state
        ids = self._stage_ids()
        edits = tuple(
            (iid, tuple((n, fmt_value(a), fmt_value(b)) for n, a, b in rows))
            for iid, rows in sorted(self._edits().items())
        )
        return (tuple(ids), tuple(i in state.jobs for i in ids), edits, self.page.frozen_error)

    def render(self) -> None:
        if not self._visible():
            return
        page = self.page
        state = page.state
        origin = state.protocol_origin
        ids = self._stage_ids()
        edits = self._edits()
        n_edited = sum(len(v) for v in edits.values())
        with ui.column().classes("w-full gap-2").style("margin-bottom: 22px;"):
            with ui.row().classes("w-full items-center gap-2 no-wrap"):
                _section(
                    "This project",
                    f"{state.project_name} was created from {origin.name} v{origin.version}: per stage, the "
                    "parameters the protocol pinned beside what the job holds now.",
                )
                ui.label(f"applied {origin.applied_at or '?'}").style(TEXT)
                if n_edited:
                    render_chip(
                        "edited",
                        str(n_edited),
                        status="warn",
                        tooltip="Parameters changed in the job tabs since the protocol was applied",
                    )
                if page.frozen_error:
                    render_chip("frozen copy", "unreadable", status="error", tooltip=page.frozen_error)
            for index, iid in enumerate(ids, start=1):
                self._stage_row(index, iid, state.jobs.get(iid), edits.get(iid) or [])

    def _stage_row(self, index: int, iid: str, jm, edits: list) -> None:
        page = self.page
        frozen_stage = page.frozen.get_stage(iid) if page.frozen is not None else None
        title = stage_title(frozen_stage)[0] if frozen_stage is not None else (getattr(jm, "job_type", None) or iid)
        edited = {n: (a, b) for n, a, b in edits}
        with (
            ui.expansion()
            .classes("w-full cb-scroll-tight")
            .style("border-bottom: 1px solid #f1f5f9;")
            .props("dense") as exp
        ):
            exp.props('header-class="p-0"')
            with exp.add_slot("header"):
                with (
                    ui.row()
                    .classes("w-full items-center no-wrap")
                    .style("gap: 8px; padding: 4px 6px; min-height: 26px;")
                ):
                    ui.label(f"{index}.").style(f"{MONO} font-size: 9px; color: #94a3b8; width: 18px;")
                    ui.label(str(title)).style(f"{FONT} font-size: 11px; font-weight: 600; color: #1e293b;")
                    ui.label(iid).style(f"{MONO} font-size: 10px; color: #64748b;")
                    if jm is None:
                        render_chip(
                            "stage", "removed", status="error", tooltip="This stage is no longer in the project"
                        )
                    if edits:
                        names = ", ".join(n for n, _a, _b in edits)
                        render_chip(
                            "edited",
                            str(len(edits)),
                            status="warn",
                            tooltip=f"Job ≠ protocol: {names} — the job keeps what it was given",
                        )
            with ui.column().classes("w-full gap-1").style("padding: 6px 8px 12px 30px;"):
                ui.label("parameters — protocol | current").style(LABEL)
                if frozen_stage is None:
                    ui.label("No frozen protocol copy for this stage.").style(TEXT)
                elif not frozen_stage.params:
                    ui.label("The protocol pins no parameters here — code defaults.").style(TEXT)
                else:
                    with ui.element("div").style(_GRID3):
                        for name, want in frozen_stage.params.items():
                            have = getattr(jm, name, None) if jm is not None else None
                            ui.label(name).style(KEY)
                            ui.label(fmt_value(want)).style(VAL + " overflow-wrap: anywhere;")
                            cell = ui.label(fmt_value(have) if jm is not None else "—").style(
                                VAL + " overflow-wrap: anywhere;"
                            )
                            if name in edited:
                                was, now = fmt_value(edited[name][0]), fmt_value(edited[name][1])
                                cell.style(_EDITED_CELL).tooltip(f"edited: protocol {was} ≠ current {now}")
                    if jm is not None:
                        uncovered = sorted(set(getattr(jm, "USER_PARAMS", set())) - set(frozen_stage.params))
                        if uncovered:
                            ui.label(
                                "not pinned by the protocol (code or species defaults): " + ", ".join(uncovered)
                            ).style(TEXT)


# ── PROTOCOL: the bundle as declared, save-as ─────────────────────────────────


class _ProtocolView(FingerprintedView):
    def __init__(self, container, page: ProtocolsPage) -> None:
        super().__init__(container)
        self.page = page

    def signature(self) -> Any:
        page = self.page
        return (page.selected, tuple(i.name for i in page.infos), page.state.project_name)

    def render(self) -> None:
        page = self.page
        info = page.info_for(page.selected)
        if info is None:
            return
        p = info.protocol
        with ui.column().classes("w-full gap-5"):
            with ui.column().classes("w-full gap-1"):
                _section("Protocol as declared", str(info.bundle_dir))
                if p is None:
                    ui.label(f"broken: {info.error}").style(TEXT + " color: #b91c1c;")
                else:
                    kv_grid(
                        [("bundle", str(info.bundle_dir)), ("version", p.version), ("schema", p.schema_version)]
                        + [(f"provenance {k}", v) for k, v in p.provenance.items()]
                    )
                    with (
                        ui.expansion(f"{len(p.species)} species · {len(p.stages)} stages with their parameters")
                        .props("dense")
                        .classes("w-full cb-scroll-tight")
                        .style(f"{FONT} font-size: 10px; color: #475569;")
                    ):
                        with ui.column().classes("w-full gap-0").style("padding: 0 4px 8px;"):
                            for sp in p.species:
                                species_block(sp)
                            for i, st in enumerate(p.stages, start=1):
                                stage_block(i, st)
            with ui.column().classes("w-full gap-1"):
                _section(
                    "Save this project as a protocol",
                    "Export this project's pipeline (species, assets, every stage's parameters) as a new bundle under "
                    "~/.crboost/protocols/<name>/ — the way a workflow that worked becomes something you can hand over",
                )
                with ui.row().classes("items-center gap-2 no-wrap"):
                    name_input = house_text("name", width="w-56", placeholder=f"{page.state.project_name}-v1")
                    house_button(
                        "Save as protocol",
                        lambda: page.save_as_protocol(name_input.value or ""),
                        tooltip="Writes protocol.yaml + assets/; refuses to overwrite an existing bundle",
                    )

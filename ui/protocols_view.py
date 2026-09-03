"""Protocols view — the workspace surface of roadmap 16 (D6).

Opened from the foot-of-rail protocol light (`pipeline_roster._build_protocol_btn`) and swapped
into the main area like the Journey / Tomograms views. One header, two levels:

- the header is the protocol SWITCHER (the Journey's caret + dropdown idiom): the selected
  protocol's name, version and description, every discovered bundle behind the caret — input /
  baseline state, run count, last verdict — and the launch actions;
- RUN — when THIS project was created from the selected protocol (`ProjectState.protocol_origin`):
  its stages as they stand, one row each — status dot · verdict chip · metrics · checks · failure
  signature · "edited" chips (a job param ≠ what the protocol pinned; never "drift", D7) — each
  expandable to the parameter table (protocol | current), the dispatched `run_submit.script`, the
  run.out / run.err tails and the job dir's files. Actions: Evaluate now · Stop chain · Promote to
  baseline record · Report;
- PROTOCOL — the bundle as declared (species, stages with parameters), the test-bundle state,
  every run of it under both run roots (open any), and "save this project as a protocol".

Reads only. `evaluate_run` is the one writer (into `<project>/protocol/`) and is idempotent; the
rail light's tick calls it once when the chain settles (`evaluate_if_settled`), so a verdict
appears without the view ever being opened. Disk reads happen off the event loop and are cached
on mtimes; the two FingerprintedViews rebuild only when their signature moves.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from nicegui import app as ng_app, ui

from services.array_tasks import read_tail
from services.configs.config_service import get_config_service
from services.models_base import JobStatus
from services.project_state import ProjectState, get_project_state_for
from services.protocols.apply import stage_edits
from services.protocols.discovery import USER_PROTOCOLS_DIR, ProtocolInfo, list_protocols
from services.protocols.export import export_protocol
from services.protocols.schema import PROJECT_PROTOCOL_DIRNAME, Protocol, dump_protocol, load_protocol
from services.regress.bands import BANDS_FILE
from services.regress.checks import job_dir_of
from services.regress.inputs import check_input, input_dir_for
from services.regress.report import REPORT_MD, list_runs, read_run_json, run_dir_of, run_json_mtime, stage_files
from services.regress.runner import cli_runs_root, evaluate_run, launch_run
from ui.background_task import BackgroundTask
from ui.components.buttons import house_button
from ui.components.chip import render_chip
from ui.components.copyable import copyable_path
from ui.components.dialogs import dialog_host
from ui.components.fields import house_text
from ui.components.log_pane import STDERR_STYLE, STDOUT_STYLE, log_pane
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.dashboard.css import ensure_assets_loaded
from ui.job_plugins._field_styles import PAGE_SECTION_STYLE
from ui.open_project import open_project_in_workspace
from ui.protocols_blocks import (
    KEY,
    LABEL,
    TEXT,
    VAL,
    fmt_value,
    kv_grid,
    launched_dialog,
    species_block,
    stage_block,
    stage_title,
    state_chip,
    submit_download,
)
from ui.status_indicator import _dot_html, _running_spinner_html
from ui.styles import MONO, SANS as FONT

logger = logging.getLogger(__name__)

_TICK_S = 15.0  # the view's poll touches disk (run.json, log tails) → 15 s per CLAUDE.md
_EVAL_FLIGHT = SingleFlight()  # keyed by project path; the rail tick and the view share it
_EVAL_ERRORS: dict[str, str] = {}  # project path → last evaluate failure — shown, never swallowed
_RUN_JSON_CACHE: dict[str, tuple[float | None, dict | None]] = {}

VERDICT_STATUS = {
    "PASS": "ok",
    "UNBANDED": "info",
    "DRIFT": "warn",
    "FAIL": "error",
    "INFRA": "error",
    "SKIPPED": "neutral",
    "PENDING": "neutral",
    "LIVE": "info",
}
_OK_VERDICTS = ("PASS", "UNBANDED")
_LIVE = (JobStatus.RUNNING, JobStatus.QUEUED)
_SETTLED = (JobStatus.SUCCEEDED, JobStatus.FAILED)
_GRID3 = "display: grid; grid-template-columns: max-content minmax(0, 1fr) minmax(0, 1fr); gap: 2px 14px; width: 100%;"
_EDITED_CELL = "background: #fffbeb; border-radius: 3px; padding: 0 4px; color: #b45309;"
_LINK = f"{FONT} font-size: 10px; color: #6366f1; cursor: pointer; text-decoration: underline dotted;"


# ── shared with the rail light ────────────────────────────────────────────────


def cached_run_json(project_path: Path) -> dict[str, Any] | None:
    """The project's run.json, re-read only when its mtime moved (one stat per call)."""
    key = str(project_path)
    mt = run_json_mtime(project_path)
    hit = _RUN_JSON_CACHE.get(key)
    if hit is not None and hit[0] == mt:
        return hit[1]
    data = read_run_json(project_path) if mt is not None else None
    _RUN_JSON_CACHE[key] = (mt, data)
    return data


def protocol_light(project_path: Path | None) -> tuple[str, str]:
    """(kind, tooltip) for the rail light — off · applied · live · pending · ok · bad. In-memory
    state plus one stat; safe on a 6 s tick."""
    if project_path is None:
        return "off", "No project."
    state = get_project_state_for(project_path)
    origin = state.protocol_origin
    if origin is None:
        return "off", "Not created from a protocol. Click for the protocol library and past runs."
    head = f"{origin.name} v{origin.version}"
    if state.pipeline_active:
        ids = [i for i in state.pipeline_order if i in state.jobs]
        done = sum(1 for i in ids if state.jobs[i].execution_status == JobStatus.SUCCEEDED)
        live = [i for i in ids if state.jobs[i].execution_status in _LIVE]
        what = ", ".join(live) if live else "settling"
        return "live", f"{head} — running: {what} · {done}/{len(ids)} done. Click for the Protocols view."
    failure = _EVAL_ERRORS.get(str(project_path))
    if failure:
        return "pending", f"{head} — evaluation failed: {failure}. Click for the Protocols view."
    data = cached_run_json(project_path)
    if data is None:
        return "applied", f"{head} — applied, not launched by the harness. Click to evaluate or run."
    if data.get("error"):
        return "bad", f"{head} — {data['error']}. Click for the Protocols view."
    verdict = str(data.get("verdict") or "PENDING")
    if verdict in ("PENDING", "LIVE"):
        return "pending", f"{head} — settled, evaluating…"
    stages = data.get("stages") or []
    n_ok = sum(1 for s in stages if s.get("verdict") in _OK_VERDICTS)
    kind = "ok" if verdict == "PASS" else "bad"
    return kind, f"{head} — {verdict} · {n_ok}/{len(stages)} stages. Click for the Protocols view."


async def run_evaluation(backend, project_path: Path, *, force_record: bool = False) -> bool:
    """`evaluate_run` under one flight per project. A failure is remembered for the light and
    the view (and logged with its traceback); the next Evaluate retries."""
    key = str(project_path)
    async with _EVAL_FLIGHT(key) as acquired:
        if not acquired:
            return False
        try:
            await evaluate_run(backend, project_path, force_record=force_record)
        except Exception as e:
            logger.exception("evaluate_run(%s) failed", project_path)
            _EVAL_ERRORS[key] = str(e)
            return False
        _EVAL_ERRORS.pop(key, None)
        return True


async def evaluate_if_settled(backend, project_path: Path) -> bool:
    """The rail tick's hook: a harness-launched run whose chain has settled and whose run.json
    has no verdict yet is evaluated once, so the light turns green/red on its own."""
    state = get_project_state_for(project_path)
    if state.protocol_origin is None or state.pipeline_active:
        return False
    data = cached_run_json(project_path)
    if data is None or data.get("error") or data.get("finished_at"):
        return False
    if str(project_path) in _EVAL_ERRORS:
        return False  # failed once already; a manual Evaluate retries
    return await run_evaluation(backend, project_path)


async def launch_project_base(backend) -> Path:
    """Where a UI launch creates its run project: the tab's preferred base, else the config's."""
    from services.configs.user_prefs_service import get_prefs_service

    base = None
    try:
        base = get_prefs_service().load_from_app_storage(ng_app.storage.user).project_base_path
    except RuntimeError:
        pass  # no page context (a bare timer) — the config default below is the answer then
    return Path(base or await backend.get_default_project_base()).expanduser()


def _facts_for(info: ProtocolInfo, roots: list[Path], local_root: Path | None) -> dict[str, Any]:
    facts: dict[str, Any] = {"input": None, "input_dir": "", "baseline": None, "runs": list_runs(roots, info.name)}
    p = info.protocol
    if p is not None and info.has_test_bundle:
        chk = check_input(p, local_root, verify_checksums=False) if local_root is not None else None
        facts["input"] = bool(chk and chk.ok)
        facts["input_dir"] = str(input_dir_for(p, local_root)) if local_root is not None else ""
        facts["baseline"] = (info.bundle_dir / "test" / BANDS_FILE).exists()
    return facts


def _verdict_chip(verdict: str | None, tooltip: str = "") -> None:
    v = str(verdict or "—")
    render_chip("verdict", v, status=VERDICT_STATUS.get(v, "warn"), tooltip=tooltip or None)


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
        self.facts: dict[str, dict[str, Any]] = {}
        self.roots: list[Path] = []
        self.selected: str | None = None
        self.frozen: Protocol | None = None  # <project>/protocol/protocol.yaml, when this project has an origin
        self.frozen_error: str | None = None
        self._refs: dict[str, Any] = {}
        self._flight = SingleFlight()
        self._built = False
        self._active = False
        self.run_view: _RunView | None = None
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
        await evaluate_if_settled(self.backend, self.project_path)
        await self._collect()
        await self.refresh()

    async def refresh(self) -> None:
        if self.run_view is not None:
            await self.run_view.prepare()
            self.run_view.refresh()
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
                self._refs["run_host"] = ui.element("div").classes("w-full")
                self._refs["proto_host"] = ui.element("div").classes("w-full")
        self._build_header()
        self.run_view = _RunView(self._refs["run_host"], self)
        self.protocol_view = _ProtocolView(self._refs["proto_host"], self)
        self._built = True
        await self.refresh()

    async def _collect(self, *, all_protocols: bool = False) -> None:
        """Disk, off the loop: the bundles (on build / on demand), the project's frozen copy,
        and the facts of the selected protocol (input · baseline · its runs)."""
        base = await launch_project_base(self.backend)
        local_root = get_config_service().local_data_root
        self.roots = [base] + ([cli_runs_root(local_root)] if local_root is not None else [])
        has_origin = self.state.protocol_origin is not None
        wanted = None if all_protocols or not self.infos else self.selected

        def _read():
            infos = list_protocols() if wanted is None else self.infos
            frozen, frozen_error = None, None
            if has_origin:
                try:
                    frozen = load_protocol(self.project_path / PROJECT_PROTOCOL_DIRNAME)
                except (FileNotFoundError, ValueError) as e:
                    frozen_error = str(e)
            facts = {i.name: _facts_for(i, self.roots, local_root) for i in infos if wanted in (None, i.name)}
            return infos, frozen, frozen_error, facts

        infos, frozen, frozen_error, facts = await asyncio.to_thread(_read)
        self.infos = infos
        self.frozen, self.frozen_error = frozen, frozen_error
        self.facts.update(facts)

    async def select(self, name: str) -> None:
        if name == self.selected:
            return
        self.selected = name
        self._build_header()
        await self._collect()
        await self.refresh()

    # ── header: the switcher + launch actions ─────────────────────────────────

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
                            tooltip=f"Created from {origin.name} v{origin.version} on {origin.applied_at or '?'}"
                            + (f" as a harness {origin.mode}" if origin.mode else ""),
                        )
            with ui.element("div").classes("cb-jhead-right").style("gap: 6px;"):
                if info is not None and info.protocol is not None and info.has_test_bundle:
                    house_button(
                        "Record",
                        lambda i=info: self._launch(i, "record"),
                        tooltip="Create a run project from the frozen input, submit the chain, and keep the "
                        "result as a baseline record once it settles",
                    )
                    house_button(
                        "Run",
                        lambda i=info: self._launch(i, "run"),
                        kind="accent",
                        tooltip="Create a run project from the frozen input and submit the chain — "
                        "the server watches it; verdicts appear here when it settles",
                    )

    def _switcher_table(self, menu) -> None:
        cols = "28px minmax(170px, 1fr) repeat(6, max-content)"
        with ui.element("div").classes("cb-jmenu").style(f"--cb-jcols: {cols};"):
            with ui.element("div").classes("cb-jrow cb-jrow-head"):
                for head in ("#", "Protocol", "version", "stages", "input", "baseline", "runs", "last"):
                    ui.label(head).classes("cb-jh")
            for i, info in enumerate(self.infos, start=1):
                f = self.facts.get(info.name) or {}
                runs = f.get("runs") or []
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
                    ui.label(_tri(f.get("input"), "present", "missing", "no test")).classes("cb-jcell")
                    ui.label(_tri(f.get("baseline"), "recorded", "none", "no test")).classes("cb-jcell")
                    ui.label(str(len(runs))).classes("cb-jcell")
                    ui.label(str(runs[0].get("verdict") or "—") if runs else "—").classes("cb-jcell")

    # ── actions ───────────────────────────────────────────────────────────────

    async def _launch(self, info: ProtocolInfo, mode: str) -> None:
        protocol = info.protocol
        if protocol is None:
            return
        base = await launch_project_base(self.backend)  # read HERE: a background task has no client context
        backend = self.backend

        async def work(progress_cb):
            res = await launch_run(backend, protocol, mode=mode, project_base=base, progress_cb=progress_cb)
            if not res["success"]:
                raise RuntimeError(res["error"])
            return res["project_path"]

        def done(task) -> None:
            if task.status == "succeeded" and task.result_message:
                launched_dialog(self.backend, self.ui_mgr, task.result_message, mode)
            elif task.status != "succeeded":
                ui.notify(f"Protocol {mode} failed: {task.error}", type="negative", timeout=10000)

        BackgroundTask(
            title=f"Protocol {mode} · {info.name}",
            subtitle=f"apply + submit under {base}",
            dedup_key=f"regress:{info.name}",
        ).submit(work, on_complete=done, poll_interval=2.0)

    async def evaluate_now(self, *, force_record: bool = False) -> None:
        ok_ = await run_evaluation(self.backend, self.project_path, force_record=force_record)
        if not ok_:
            failure = _EVAL_ERRORS.get(str(self.project_path))
            if failure:
                ui.notify(f"Evaluate failed: {failure}", type="negative", timeout=8000)
        elif force_record:
            data = cached_run_json(self.project_path) or {}
            recorded = [a for a in data.get("annotations") or [] if a.startswith("recorded as baseline record")]
            ui.notify(
                recorded[-1] if recorded else "Evaluated — not recorded (the chain has not settled)", timeout=6000
            )
        await self._collect()
        await self.refresh()

    async def stop_chain(self) -> None:
        async with self._flight("stop") as acquired:
            if not acquired:
                return
            state = self.state
            live = [
                iid
                for iid, jm in state.jobs.items()
                if getattr(jm, "slurm_job_id", None) and jm.execution_status in (*_LIVE, JobStatus.SCHEDULED)
            ]
            with dialog_host(), ui.dialog() as dialog, ui.card().style("min-width: 360px; padding: 16px;"):
                ui.label("Stop the protocol chain?").classes("text-base font-bold text-gray-800")
                ui.label(str(self.project_path)).classes("text-xs font-mono text-gray-400 mt-1")
                ui.label(f"{len(live)} tracked SLURM job(s) will be cancelled and marked Failed.").classes(
                    "text-sm text-gray-600 mt-2"
                )
                for iid in live:
                    ui.label(f"{iid}  [{state.jobs[iid].slurm_job_id}]").classes("text-xs font-mono text-gray-500 ml-2")
                with ui.row().classes("mt-4 gap-2 justify-end w-full"):
                    house_button("Cancel", lambda: dialog.submit(False))
                    house_button("Stop chain", lambda: dialog.submit(True), kind="danger")
            dialog.open()
            if not await dialog:
                return
            res = await self.backend.pipeline_runner.stop_and_cleanup(self.project_path, [])
            self.ui_mgr.set_pipeline_running(False)
            rebuild_run = self.callbacks.get("enable_run_button")
            if rebuild_run:
                rebuild_run()
            if res.get("success"):
                ui.notify(f"Chain stopped ({res.get('cancelled_slurm_jobs', 0)} job(s) cancelled).", type="warning")
            else:
                ui.notify(f"Stopped with errors: {res.get('error')}", type="negative", timeout=8000)
            await self.evaluate_now()

    async def open_report(self) -> None:
        path = run_dir_of(self.project_path) / REPORT_MD

        def _read() -> str | None:
            return path.read_text() if path.exists() else None

        text = await asyncio.to_thread(_read)
        if text is None:
            ui.notify("No report.md yet — Evaluate first.", type="warning")
            return
        with dialog_host(), ui.dialog() as dialog, ui.card().classes("p-0").style("width: 900px; max-width: 95vw;"):
            with ui.row().classes("w-full items-center gap-2").style("padding: 12px 16px 6px;"):
                ui.label("report.md").style(f"{FONT} font-size: 12px; font-weight: 700; color: #0f172a;")
                copyable_path(str(path), font_size="9px", color="#94a3b8")
            with ui.scroll_area().classes("w-full cb-scroll-tight").style("height: 70vh;"):
                ui.markdown(text).style(f"{FONT} font-size: 11px; padding: 0 16px 16px;")
            with (
                ui.row()
                .classes("w-full justify-end items-center gap-2")
                .style("padding: 10px 16px; background: #f8fafc; border-top: 1px solid #e2e8f0;")
            ):
                house_button("Close", dialog.close)
        dialog.open()

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


def _tri(value: bool | None, yes: str, no: str, none: str) -> str:
    if value is None:
        return none
    return yes if value else no


# ── RUN: this project's stages ────────────────────────────────────────────────


class _RunView(FingerprintedView):
    _TAILS = (("run_submit.script", 200), ("run.out", 200), ("run.err", 100))

    def __init__(self, container, page: ProtocolsPage) -> None:
        super().__init__(container)
        self.page = page
        self._expanded: set[str] = set()
        self._log_cache: dict[tuple[str, str], tuple[float | None, str]] = {}
        self._files: dict[str, list[Path]] = {}
        self._log_sig: tuple = ()

    # ── inputs ────────────────────────────────────────────────────────────────

    def _visible(self) -> bool:
        origin = self.page.state.protocol_origin
        return origin is not None and origin.name == self.page.selected

    def _stage_ids(self) -> list[str]:
        if self.page.frozen is not None:
            return self.page.frozen.stage_ids()
        data = cached_run_json(self.page.project_path) or {}
        ids = [s.get("instance_id") for s in data.get("stages") or [] if s.get("instance_id")]
        return ids or [i for i in self.page.state.pipeline_order if i in self.page.state.jobs]

    def _edits(self) -> dict[str, list[tuple[str, Any, Any]]]:
        frozen = self.page.frozen
        return stage_edits(self.page.state, frozen) if frozen is not None else {}

    def signature(self) -> Any:
        if not self._visible():
            return ("hidden", self.page.selected)
        state = self.page.state
        statuses = []
        for iid in self._stage_ids():
            jm = state.jobs.get(iid)
            statuses.append(
                (
                    iid,
                    str(getattr(jm, "execution_status", "")),
                    getattr(jm, "relion_job_name", None),
                    str(getattr(jm, "slurm_job_id", None)),
                )
            )
        edits = tuple(
            (iid, tuple((n, fmt_value(a), fmt_value(b)) for n, a, b in rows))
            for iid, rows in sorted(self._edits().items())
        )
        return (
            tuple(statuses),
            bool(state.pipeline_active),
            run_json_mtime(self.page.project_path),
            edits,
            tuple(sorted(self._expanded)),
            self._log_sig,
            _EVAL_ERRORS.get(str(self.page.project_path)),
            self.page.frozen_error,
        )

    async def prepare(self) -> None:
        """Off the loop: the tails (and file lists) of the EXPANDED stages' job dirs, cached on
        mtime; their mtimes ride in the signature so a growing log rebuilds the row."""
        if not self._visible() or not self._expanded:
            self._log_sig = ()
            return
        state = self.page.state
        project_path = self.page.project_path
        targets: list[tuple[str, Path]] = []
        for iid in sorted(self._expanded):
            jm = state.jobs.get(iid)
            job_dir = job_dir_of(project_path, jm) if jm is not None else None
            if job_dir is not None:
                targets.append((iid, job_dir))
        cache = self._log_cache

        def _read():
            logs: dict[tuple[str, str], tuple[float | None, str]] = {}
            files: dict[str, list[Path]] = {}
            sig: list[tuple] = []
            for iid, job_dir in targets:
                files[iid] = stage_files(job_dir) if job_dir.is_dir() else []
                for name, n_lines in self._TAILS:
                    path = job_dir / name
                    try:
                        mt: float | None = path.stat().st_mtime
                    except OSError:
                        mt = None
                    key = (iid, name)
                    hit = cache.get(key)
                    if mt is None:
                        text = ""
                    elif hit is not None and hit[0] == mt:
                        text = hit[1]
                    else:
                        text = read_tail(path, max_lines=n_lines)
                    logs[key] = (mt, text)
                    sig.append((iid, name, mt))
            return logs, files, tuple(sig)

        self._log_cache, self._files, self._log_sig = await asyncio.to_thread(_read)

    def _toggle(self, iid: str, opened: bool) -> None:
        if opened:
            self._expanded.add(iid)
        else:
            self._expanded.discard(iid)
        if opened:
            # Read this stage's tails now rather than at the next 15 s tick.
            asyncio.create_task(self.page.refresh())

    # ── render ────────────────────────────────────────────────────────────────

    def render(self) -> None:
        if not self._visible():
            return
        page = self.page
        state = page.state
        origin = state.protocol_origin
        data = cached_run_json(page.project_path)
        by_iid = {s.get("instance_id"): s for s in (data or {}).get("stages") or []}
        ids = self._stage_ids()
        edits = self._edits()
        active = bool(state.pipeline_active)
        verdict = str(data.get("verdict")) if data and data.get("verdict") else None
        driver = ((data or {}).get("driver") or {}).get("mode")
        cli_live = driver == "cli" and (active or verdict in ("PENDING", "LIVE"))
        n_settled = sum(1 for i in ids if getattr(state.jobs.get(i), "execution_status", None) in _SETTLED)
        failure = _EVAL_ERRORS.get(str(page.project_path))

        with ui.column().classes("w-full gap-2").style("margin-bottom: 22px;"):
            with ui.row().classes("w-full items-center gap-2 no-wrap"):
                _section(
                    "This project",
                    f"{state.project_name} was created from {origin.name} v{origin.version}; every row is one of its "
                    "stages as it stands now — the roster's status, the harness's verdict.",
                )
                render_chip("mode", origin.mode or "applied", status="neutral", tooltip="How this project came to be")
                if data is not None and not data.get("error"):
                    n_ok = sum(1 for s in by_iid.values() if s.get("verdict") in _OK_VERDICTS)
                    _verdict_chip(verdict, f"{n_ok}/{len(by_iid)} stages PASS or unbanded")
                elif data is not None:
                    render_chip("run.json", "unreadable", status="error", tooltip=str(data.get("error")))
                else:
                    render_chip(
                        "verdict",
                        "not evaluated",
                        status="neutral",
                        tooltip="Not launched by the harness. Evaluate now checks the stages as they stand.",
                    )
                ui.label(f"{n_settled}/{len(ids)} settled").style(TEXT)
                if active:
                    ui.html(_running_spinner_html(14, "#3b82f6"), sanitize=False, tag="span").style(
                        "display: inline-flex; align-items: center;"
                    )
                    live = [i for i in ids if getattr(state.jobs.get(i), "execution_status", None) in _LIVE]
                    ui.label("running: " + (", ".join(live) or "settling")).style(TEXT)
                if cli_live:
                    d = data.get("driver") or {}
                    render_chip(
                        "driver",
                        "CLI",
                        status="warn",
                        tooltip=f"Driven by crboost_regress.py on {d.get('host', '?')} (pid {d.get('pid', '?')}) — "
                        "read-only here; stop it from that terminal (roadmap 15 owns the cross-process lock)",
                    )
                if failure:
                    render_chip("evaluate", "failed", status="error", tooltip=failure)
                if page.frozen_error:
                    render_chip("frozen copy", "unreadable", status="error", tooltip=page.frozen_error)
                ui.space()
                house_button(
                    "Evaluate now",
                    page.evaluate_now,
                    tooltip="Re-read every stage and rewrite protocol/run.json + report.md (idempotent)",
                )
                if active and not cli_live:
                    house_button(
                        "Stop chain",
                        page.stop_chain,
                        kind="danger",
                        tooltip="scancel every tracked job of this project and mark them Failed",
                    )
                if not active and data is not None and not cli_live:
                    house_button(
                        "Promote to baseline record",
                        lambda: page.evaluate_now(force_record=True),
                        tooltip="Keep this run's metrics + artifacts as a baseline record "
                        "(what `crboost_regress.py record` does); once per run",
                    )
                if data is not None:
                    house_button("Report", page.open_report, tooltip="protocol/report.md, rendered")
            with ui.row().classes("w-full items-center gap-3 no-wrap"):
                rows: list[tuple[str, Any]] = [("applied", origin.applied_at or "?")]
                if data:
                    rows += [
                        ("finished", data.get("finished_at") or "—"),
                        ("evaluated", data.get("evaluated_at") or "—"),
                    ]
                for k, v in rows:
                    ui.label(k).style(KEY)
                    ui.label(fmt_value(v)).style(VAL)
                copyable_path(str(run_dir_of(page.project_path)), font_size="9px", color="#94a3b8")
            notes = list((data or {}).get("annotations") or [])
            if notes:
                with (
                    ui.expansion(f"{len(notes)} annotation(s)")
                    .props("dense")
                    .classes("w-full cb-scroll-tight")
                    .style(f"{FONT} font-size: 10px; color: #475569;")
                ):
                    for a in notes:
                        ui.label(f"· {a}").style(TEXT + " overflow-wrap: anywhere;")
            for index, iid in enumerate(ids, start=1):
                self._stage_row(index, iid, state.jobs.get(iid), by_iid.get(iid), edits.get(iid) or [], active)

    def _stage_row(self, index: int, iid: str, jm, sr: dict | None, edits: list, active: bool) -> None:
        page = self.page
        frozen_stage = page.frozen.get_stage(iid) if page.frozen is not None else None
        title = stage_title(frozen_stage)[0] if frozen_stage is not None else (getattr(jm, "job_type", None) or iid)
        status = getattr(jm, "execution_status", JobStatus.UNKNOWN) if jm is not None else JobStatus.UNKNOWN
        verdict = (sr or {}).get("verdict")
        if verdict is None and active and status not in _SETTLED:
            verdict = "PENDING"
        dot = (
            _running_spinner_html(14, "#3b82f6")
            if status == JobStatus.RUNNING
            else _dot_html(status, is_orphaned=bool(getattr(jm, "is_orphaned", False)))
        )
        job_dir = job_dir_of(page.project_path, jm) if jm is not None else None
        with (
            ui.expansion(value=iid in self._expanded, on_value_change=lambda e, i=iid: self._toggle(i, bool(e.value)))
            .classes("w-full cb-scroll-tight")
            .style("border-bottom: 1px solid #f1f5f9; min-height: 0;")
            .props("dense") as exp
        ):
            exp.props('header-class="p-0"')
            with exp.add_slot("header"):
                with (
                    ui.row()
                    .classes("w-full items-center no-wrap")
                    .style("gap: 8px; padding: 4px 6px; min-height: 26px;")
                ):
                    ui.html(dot, sanitize=False, tag="span").style("display: inline-flex; align-items: center;")
                    ui.label(f"{index}.").style(f"{MONO} font-size: 9px; color: #94a3b8; width: 18px;")
                    ui.label(str(title)).style(f"{FONT} font-size: 11px; font-weight: 600; color: #1e293b;")
                    ui.label(iid).style(f"{MONO} font-size: 10px; color: #64748b;")
                    ui.label(str(getattr(status, "value", status))).style(f"{FONT} font-size: 9px; color: #94a3b8;")
                    if jm is None:
                        render_chip(
                            "stage", "removed", status="error", tooltip="This stage is no longer in the project"
                        )
                    if verdict:
                        _verdict_chip(verdict, "; ".join((sr or {}).get("notes") or []) or "")
                    if edits:
                        names = ", ".join(n for n, _a, _b in edits)
                        render_chip(
                            "edited",
                            str(len(edits)),
                            status="warn",
                            tooltip=f"Job ≠ protocol: {names} — the job keeps what it was given",
                        )
                    infra = (sr or {}).get("infra")
                    if infra:
                        render_chip(
                            "infra",
                            str(infra.get("label") or "signature"),
                            status="error",
                            tooltip=f"{infra.get('file', '')}: {infra.get('line', '')}"
                            + (f" — node {infra['node']}" if infra.get("node") else ""),
                        )
                    ui.space()
                    metrics = (sr or {}).get("metrics") or {}
                    shown = [f"{k}={fmt_value(v)}" for k, v in metrics.items() if not isinstance(v, list)][:4]
                    if shown:
                        ui.label(" · ".join(shown)).style(f"{MONO} font-size: 9px; color: #64748b;").tooltip(
                            ", ".join(f"{k}={fmt_value(v)}" for k, v in metrics.items())
                        )
            if iid in self._expanded:
                self._stage_body(iid, jm, sr, frozen_stage, edits, job_dir)

    def _stage_body(self, iid: str, jm, sr: dict | None, frozen_stage, edits: list, job_dir: Path | None) -> None:
        edited = {n: (a, b) for n, a, b in edits}
        with ui.column().classes("w-full gap-3").style("padding: 6px 8px 12px 30px;"):
            # Parameters: what the protocol pinned, what the job holds now.
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
                            cell.style(_EDITED_CELL).tooltip(
                                f"edited: protocol {fmt_value(edited[name][0])} ≠ current {fmt_value(edited[name][1])}"
                            )
                if jm is not None:
                    uncovered = sorted(set(getattr(jm, "USER_PARAMS", set())) - set(frozen_stage.params))
                    if uncovered:
                        ui.label(
                            "not pinned by the protocol (code or species defaults): " + ", ".join(uncovered)
                        ).style(TEXT)
            checks = (sr or {}).get("checks") or []
            if checks:
                ui.label("checks").style(LABEL)
                with ui.element("div").style(
                    "display: grid; grid-template-columns: max-content max-content max-content minmax(0, 1fr); "
                    "gap: 2px 14px; width: 100%;"
                ):
                    for c in checks:
                        ui.label(str(c.get("metric"))).style(KEY)
                        ui.label(fmt_value(c.get("value"))).style(VAL)
                        v = str(c.get("verdict"))
                        v_color = "#047857" if v == "PASS" else "#b91c1c"
                        ui.label(v).style(f"{MONO} font-size: 10px; color: {v_color}; font-weight: 600;")
                        ui.label(f"{fmt_value(c.get('band'))}  {c.get('detail') or ''}".strip()).style(TEXT)
            notes = list((sr or {}).get("notes") or [])
            infra = (sr or {}).get("infra")
            if infra:
                notes.append(
                    f"INFRA {infra.get('label')}: {infra.get('file', '')} — {infra.get('line', '')}"
                    + (f" (node {infra['node']})" if infra.get("node") else "")
                )
            if notes:
                ui.label("notes").style(LABEL)
                for n in notes:
                    ui.label(f"· {n}").style(TEXT + " overflow-wrap: anywhere;")
            # Dispatch + logs (tails read in prepare()).
            script = self._log_cache.get((iid, "run_submit.script"), (None, ""))[1]
            out = self._log_cache.get((iid, "run.out"), (None, ""))[1]
            errt = self._log_cache.get((iid, "run.err"), (None, ""))[1]
            if job_dir is None:
                ui.label("Not deployed yet — no job directory.").style(TEXT)
            else:
                if script:
                    log_pane("run_submit.script", script, job_dir / "run_submit.script", max_h="30vh", **STDOUT_STYLE)
                if out:
                    log_pane("stdout", out, job_dir / "run.out", max_h="50vh", **STDOUT_STYLE)
                if errt:
                    log_pane("stderr", errt, job_dir / "run.err", max_h="40vh", **STDERR_STYLE)
                if not (script or out or errt):
                    ui.label("No logs yet.").style(TEXT)
                files = self._files.get(iid) or []
                if files:
                    ui.label("files").style(LABEL)
                    for p in files:
                        copyable_path(str(p), font_size="9px", color="#64748b")


# ── PROTOCOL: the bundle, its runs, save-as ───────────────────────────────────


class _ProtocolView(FingerprintedView):
    def __init__(self, container, page: ProtocolsPage) -> None:
        super().__init__(container)
        self.page = page

    def signature(self) -> Any:
        page = self.page
        f = page.facts.get(page.selected) or {}
        runs = tuple(
            (r.get("project_dir"), r.get("verdict"), r.get("finished_at"), r.get("evaluated_at"))
            for r in f.get("runs") or []
        )
        return (page.selected, f.get("input"), f.get("baseline"), runs, tuple(i.name for i in page.infos))

    def render(self) -> None:
        page = self.page
        info = page.info_for(page.selected)
        if info is None:
            return
        p = info.protocol
        f = page.facts.get(info.name) or {}
        local_root = get_config_service().local_data_root
        with ui.column().classes("w-full gap-5"):
            # Runs
            with ui.column().classes("w-full gap-1"):
                _section(
                    "Runs",
                    "Every project created from this protocol by the harness, under the project base "
                    "(UI launches) and local_data/runs (CLI launches). Open any of them.",
                )
                runs = f.get("runs") or []
                if not runs:
                    ui.label("No runs yet.").style(TEXT)
                for r in runs:
                    self._run_row(r)
            # Test bundle
            if p is not None and info.has_test_bundle:
                with ui.column().classes("w-full gap-1"):
                    _section("Test bundle", "The frozen input under local_data/input and the recorded baseline bands")
                    with ui.row().classes("items-center gap-2 no-wrap"):
                        ui.label("input").style(KEY)
                        state_chip(
                            "present" if f.get("input") else "missing",
                            ok=bool(f.get("input")),
                            tip=f.get("input_dir") or "no input root configured (local_data.root / DefaultProjectBase)",
                        )
                        if local_root is not None and not f.get("input"):
                            house_button(
                                "Download",
                                lambda i=info, r=local_root: submit_download(i, r),
                                tooltip="Fetch the frozen input from EMPIAR into local_data/input (resumable; "
                                "progress in the tray)",
                            )
                        ui.label("baseline").style(KEY)
                        state_chip(
                            "recorded" if f.get("baseline") else "none",
                            ok=bool(f.get("baseline")),
                            tip="test/bands.yaml"
                            if f.get("baseline")
                            else "No bands yet — record three runs, then derive the bands from them "
                            "(`crboost_regress.py bless`)",
                        )
                        if local_root is not None:
                            copyable_path(
                                str(Path(local_root) / "baseline" / info.name), font_size="9px", color="#94a3b8"
                            )
            # The bundle as declared
            with ui.column().classes("w-full gap-1"):
                _section("Protocol as declared", str(info.bundle_dir))
                if p is None:
                    ui.label(f"broken: {info.error}").style(TEXT + " color: #b91c1c;")
                else:
                    kv_grid(
                        [("bundle", str(info.bundle_dir)), ("version", p.version), ("schema", p.schema_version)]
                        + [(f"provenance {k}", v) for k, v in p.provenance.items()]
                        + [
                            (f"expects {k}", f"about {e.about:g}" + (f" ± {e.tol:g}" if e.tol else ""))
                            for k, e in p.expects.items()
                        ]
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
            # Save as
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

    def _run_row(self, r: dict[str, Any]) -> None:
        page = self.page
        project_dir = str(r.get("project_dir") or "")
        is_this = project_dir and Path(project_dir).resolve() == page.project_path.resolve()
        with (
            ui.row()
            .classes("w-full items-center gap-3 no-wrap")
            .style("min-height: 24px; border-bottom: 1px solid #f1f5f9;")
        ):
            ui.label(str(r.get("started_at") or "?")[:19]).style(VAL)
            ui.label(str(r.get("mode") or "?")).style(TEXT)
            ui.label(str(r.get("driver") or "?")).style(f"{MONO} font-size: 9px; color: #94a3b8;").tooltip(
                "server = launched from the UI, watched by this server · cli = crboost_regress.py"
            )
            if r.get("error"):
                render_chip("run.json", "unreadable", status="error", tooltip=str(r["error"]))
            else:
                _verdict_chip(
                    r.get("verdict"), f"{r.get('n_pass', '?')}/{r.get('n_stages', '?')} stages PASS or unbanded"
                )
                ui.label(f"{r.get('n_pass', '?')}/{r.get('n_stages', '?')}").style(TEXT)
            ui.label(Path(project_dir).name if project_dir else "?").style(VAL + " font-weight: 600;").tooltip(
                project_dir
            )
            ui.space()
            if is_this:
                render_chip("this", "project", status="info")
            elif project_dir:

                async def _open(_e=None, target=project_dir) -> None:
                    await open_project_in_workspace(page.backend, page.ui_mgr, Path(target))

                ui.label("open ↗").style(_LINK).on("click", _open).tooltip("Open this run project in the workspace")

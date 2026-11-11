# ui/components/array_task_tracker.py
"""
The Tasks panel of an array job's page: the supervisor and every array task as a short row on the
left, the selected one's log on the right, the two scrolling apart. The supervisor's row stays at
the top of the list; it is the job's own log, and the panel opens on it.

It reads the file-based contract the supervisor writes:

    {job_dir}/
    ├── .task_manifest.json   # {"items": [...], "item_label": "Tilt Series", "array_job_id": ...}
    ├── .task_status/
    │   ├── {name}.ok
    │   └── {name}.fail
    ├── run.out, run.err      # the supervisor's log
    ├── task_0.out            # first line: "[SLURM] job <array>_<index> = <job id> ..."
    └── task_0.err

Register as an extra tab via the plugin system:

    @register_extra_tab(JobType.YOUR_JOB, key="tasks", label="Tasks", icon="view_list")
    def render(job_type, instance_id, job_model, backend, ui_mgr):
        render_array_task_tracker(instance_id, job_model, ui_mgr)
"""

import asyncio
from dataclasses import dataclass
from pathlib import Path

from nicegui import ui

from services.array_tasks import (
    TaskProgress,
    read_manifest,
    scan_statuses,
    shorten_ts_names,
    sort_ts_by_position,
    task_slurm_ids,
)
from services.models_base import JobStatus
from services.project_state import get_project_state_for
from ui.components.copyable import _write_clipboard
from ui.components.log_pane import LogViewer, job_log_placeholder
from ui.components.reactive import owned_timer
from ui.status_indicator import _dot_html
from ui.styles import MONO, SANS

# ── Status constants ──

_OK = "ok"
_FAIL = "fail"
_RUNNING = "running"
_PENDING = "pending"
_SKIP = "skip"

_CHIP = {
    _OK: ("check_circle", "#16a34a"),
    _FAIL: ("error", "#dc2626"),
    _RUNNING: ("sync", "#2563eb"),
    _PENDING: ("schedule", "#9ca3af"),
    # Intentional non-run: upstream produced nothing for this item, supervisor
    # pre-marked it. Distinguished from "fail" (red) and "pending" (waiting)
    # so the user can tell "skipped by design" from "didn't make it yet".
    _SKIP: ("remove_circle_outline", "#94a3b8"),
}

# The supervisor's row: no manifest item has an empty name.
SUPERVISOR = ""

_ROW_STYLE = "display: flex; align-items: center; gap: 6px; height: 20px; padding: 0 8px 0 10px; min-width: 0;"
_NAME_STYLE = (
    f"{MONO} font-size: 10px; color: #334155; flex: 1 1 auto; min-width: 0; "
    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
)
_ID_STYLE = f"{MONO} font-size: 9px; color: #94a3b8; flex-shrink: 0; white-space: nowrap;"


# ── Public API ──


def render_array_task_tracker(instance_id: str, job_model, ui_mgr) -> None:
    """Render the Tasks panel into the current slot. A deep link from the roster's tilt-series rows
    (`ui_mgr.focus_ts_by_instance[instance_id]`, popped here) selects that task. The job page's
    openers reach the built panel through `monitor_logs` of the job's widget refs: "show_supervisor"
    and "show_focus" (the deep link set after the panel was built)."""
    if not job_model.relion_job_name or ui_mgr.project_path is None:
        job_log_placeholder(instance_id, ui_mgr)
        return
    _TaskPanel(instance_id, job_model, ui_mgr).build()


# ── Internals ──


def _summary_html(statuses: dict[str, str], item_label: str) -> str:
    p = TaskProgress.from_statuses(statuses)

    parts = []
    if p.n_ok:
        parts.append(f'<span style="color: #16a34a; font-weight: 600;">{p.n_ok} done</span>')
    if p.n_running:
        parts.append(f'<span style="color: #2563eb; font-weight: 600;">{p.n_running} running</span>')
    if p.n_fail:
        parts.append(f'<span style="color: #dc2626; font-weight: 600;">{p.n_fail} failed</span>')
    if p.n_skip:
        parts.append(f'<span style="color: #94a3b8;">{p.n_skip} skipped</span>')
    if p.n_pending:
        parts.append(f'<span style="color: #9ca3af;">{p.n_pending} pending</span>')

    return f'<span style="{MONO} font-size: 10px;">{" · ".join(parts)} / {p.total} {item_label.lower()}</span>'


def _read_tasks(job_dir: Path, shown: dict[str, str]) -> tuple[dict | None, dict[str, str], dict[str, str]]:
    """The manifest, every item's status, and the SLURM ids worth reading: of the items whose status moved
    since `shown` (a rerun gives a task a new id) and of the running ones (a log that just started, or a
    task that started again). Blocking: run it in a thread."""
    manifest = read_manifest(job_dir)
    items = (manifest or {}).get("items") or []
    if not items:
        return manifest, {}, {}
    statuses = scan_statuses(job_dir, items)
    want = {n for n, s in statuses.items() if s != shown.get(n) or s == _RUNNING}
    ids = task_slurm_ids(job_dir, manifest, only=want) if want else {}
    return manifest, statuses, ids


@dataclass
class _Row:
    element: ui.element
    sid: ui.label
    icon: ui.icon | None = None  # None on the supervisor's row: it has the job's status light


class _TaskPanel:
    """One job's Tasks panel. Rows are built once, when the manifest is first read, and then only
    repainted: a status poll moves an icon and an id, a selection two row classes."""

    def __init__(self, instance_id: str, job_model, ui_mgr) -> None:
        self.instance_id = instance_id
        self.ui_mgr = ui_mgr
        self.project_path: Path = ui_mgr.project_path
        self.job_name: str = job_model.relion_job_name
        self.job_dir = self.project_path / self.job_name.rstrip("/")
        self.items: list[str] = []
        self.index: dict[str, int] = {}
        self.item_label = "Item"
        self.rows: dict[str, _Row] = {}
        self.statuses: dict[str, str] = {}
        self.ids: dict[str, str] = {}
        self.selected = SUPERVISOR
        # A deep link's task, selected once its row exists.
        self.focus: str | None = ui_mgr.focus_ts_by_instance.pop(instance_id, None)
        # The job has ended and its tasks have settled since the last read: no read until it runs again.
        self.quiet = False
        # Whether the log poll read on its last tick: it reads once more after the log stops changing.
        self.log_was_active = False
        self.supervisor_shown: tuple | None = None

    # ── Layout ──

    def build(self) -> None:
        with ui.element("div").style("display: flex; flex: 1 1 0; min-height: 0; width: 100%;") as root:
            # The list: its own scroll; the summary and the supervisor's row stay at its top.
            self.list = ui.element("div").style(
                "width: 250px; flex-shrink: 0; overflow-y: auto; position: relative; "
                "border-right: 1px solid var(--cb-box-line, #dbe3ec);"
            )
            with self.list:
                with ui.element("div").style("position: sticky; top: 0; z-index: 1; background: #ffffff;"):
                    self.summary = ui.html("", sanitize=False).style("display: block; padding: 6px 10px 4px;")
                    self.summary.set_visibility(False)
                    self.bar = (
                        ui.linear_progress(value=0, show_value=False)
                        .classes("w-full")
                        .style("height: 2px;")
                        .props("color=positive")
                    )
                    self.bar.set_visibility(False)
                    self.rows[SUPERVISOR] = self._supervisor_row()
                self.note = ui.label("Tasks show once the supervisor submits them").style(
                    f"{SANS} font-size: 10px; color: #94a3b8; font-style: italic; padding: 8px 10px;"
                )
                self.body = ui.element("div").style("display: flex; flex-direction: column;")
            # The log of the selected row: the rest of the width, its own scroll.
            with ui.element("div").style(
                "flex: 1 1 0; min-width: 0; min-height: 0; display: flex; flex-direction: column;"
            ):
                self.viewer = LogViewer(self.project_path)
        self.root = root
        self.rows[SUPERVISOR].element.classes(add="active")
        job = self._job()
        if job is not None:
            self._paint_supervisor(job)
        self.viewer.show(*self._files(SUPERVISOR))

        hooks = self.ui_mgr.get_job_widget_refs(self.instance_id).monitor_logs
        hooks["show_supervisor"] = lambda: self.select(SUPERVISOR)
        hooks["show_focus"] = self.take_focus

        # Every read is off the loop and runs while this job's page is the one shown.
        owned_timer(0.01, self._first_read, root, once=True)
        scope = self.ui_mgr.job_scope(self.instance_id)
        scope.timer(5.0, self._poll_tasks, root)
        self.ui_mgr.get_job_widget_refs(self.instance_id).logs_timer = scope.timer(3.0, self._poll_log, root)

    def _supervisor_row(self) -> _Row:
        row = (
            ui.element("div")
            .classes("cb-tlog-row")
            .style(f"{_ROW_STYLE} height: 24px; border-bottom: 1px solid #e3eaf2;")
        )
        row.props["title"] = "The job's own log: run.out and run.err"
        row.on("click", lambda: self.select(SUPERVISOR))
        with row:
            self.light = ui.html("", sanitize=False, tag="span").style(
                "display: inline-flex; width: 12px; justify-content: center; flex-shrink: 0;"
            )
            ui.label("Supervisor").style(
                f"{SANS} font-size: 10px; font-weight: 600; color: #12324f; flex: 1 1 auto; min-width: 0;"
            )
            sid = ui.label("—").classes("cb-tlog-copy").style(_ID_STYLE)
            sid.on("click.stop", lambda: self._click_copy(SUPERVISOR, self._supervisor_sid()))
        return _Row(row, sid)

    def _supervisor_sid(self) -> str | None:
        return self.supervisor_shown[2] if self.supervisor_shown else None

    def _build_rows(self) -> None:
        """One row per manifest item, in (stage, beam) order: its status, its short name (the full one in the
        hover) and the SLURM id it ran under."""
        names = shorten_ts_names(self.items)
        icon_name, colour = _CHIP[_PENDING]
        with self.body:
            for name in sort_ts_by_position(self.items):
                row = ui.element("div").classes("cb-tlog-row").style(_ROW_STYLE)
                row.props["title"] = name
                row.on("click", lambda n=name: self.select(n))
                with row:
                    icon = ui.icon(icon_name, size="12px").style(f"color: {colour}; flex-shrink: 0;")
                    label = ui.label(names.get(name, name)).classes("cb-tlog-copy").style(_NAME_STYLE)
                    label.on("click.stop", lambda n=name: self._click_copy(n, n))
                    sid = ui.label("—").classes("cb-tlog-copy").style(_ID_STYLE)
                    sid.on("click.stop", lambda n=name: self._click_copy(n, self.ids.get(n)))
                self.rows[name] = _Row(row, sid, icon)
        self.note.set_visibility(False)

    # ── Selection ──

    def select(self, key: str) -> None:
        """Show `key`'s log (SUPERVISOR: the job's own) and mark its row; the log is read at once."""
        if key not in self.rows or self.root.is_deleted:
            return
        if key != self.selected:
            self.rows[self.selected].element.classes(remove="active")
            self.rows[key].element.classes(add="active")
            self.selected = key
            self.viewer.show(*self._files(key))
        owned_timer(0.01, self._read_log, self.root, once=True)

    def take_focus(self) -> None:
        """The roster's link to one task (`ui_mgr.focus_ts_by_instance`): select it and bring its row into view
        in the list. Kept until the rows exist."""
        if self.root.is_deleted:
            return
        name = self.ui_mgr.focus_ts_by_instance.pop(self.instance_id, None) or self.focus
        if name is None:
            return
        if name not in self.rows:
            self.focus = name
            return
        self.focus = None
        self.select(name)
        row = self.rows[name].element
        row.client.run_javascript(
            f"(() => {{ const r = getHtmlElement({row.id}), p = getHtmlElement({self.list.id}); "
            "if (r && p) p.scrollTop = Math.max(0, r.offsetTop - p.clientHeight / 2); })()"
        )

    async def _click_copy(self, key: str, text: str | None) -> None:
        """A click on a row's name or SLURM id: on the selected row it copies, on any other it selects."""
        if key != self.selected:
            self.select(key)
        elif text:
            await _write_clipboard(text)

    def _files(self, key: str) -> tuple[Path, Path]:
        if key == SUPERVISOR:
            return self.job_dir / "run.out", self.job_dir / "run.err"
        idx = self.index[key]
        return self.job_dir / f"task_{idx}.out", self.job_dir / f"task_{idx}.err"

    # ── Polls ──

    def _job(self):
        return get_project_state_for(self.project_path).jobs.get(self.instance_id)

    async def _first_read(self) -> None:
        await self._poll_tasks(first=True)
        await self._read_log()

    async def _poll_tasks(self, first: bool = False) -> None:
        job = self._job()
        if job is None or self.root.is_deleted:
            return
        if job.relion_job_name != self.job_name:
            # Run again into another job directory: the page is rebuilt on it.
            self.ui_mgr.request_rebuild()
            return
        self._paint_supervisor(job)
        live = job.execution_status.is_live
        if self.quiet and not live and not first:
            return
        manifest, statuses, ids = await asyncio.to_thread(_read_tasks, self.job_dir, dict(self.statuses))
        if self.root.is_deleted:
            return
        items = (manifest or {}).get("items") or []
        if manifest is not None and not items:
            self.note.set_text("The task manifest lists no items")
        if items and not self.items:
            self.items = items
            self.index = {name: i for i, name in enumerate(items)}
            self.item_label = manifest.get("item_label") or "Item"
            self._build_rows()
            if self.focus is not None:
                self.take_focus()
        self.ids.update(ids)
        self._paint_rows(statuses)
        p = TaskProgress.from_statuses(statuses)
        # Arrays outlive a supervisor that timed out: a task still unsettled may yet move.
        self.quiet = not live and p.n_settled == p.total

    def _paint_supervisor(self, job) -> None:
        shown = (job.execution_status, job.is_orphaned, job.slurm_job_id)
        if shown == self.supervisor_shown:
            return
        self.supervisor_shown = shown
        self.light.set_content(_dot_html(job.execution_status, is_orphaned=job.is_orphaned, size_px=7))
        self.rows[SUPERVISOR].sid.set_text(job.slurm_job_id or "—")

    def _paint_rows(self, statuses: dict[str, str]) -> None:
        """Repaint what moved: the icons of the rows whose status changed, the ids, the summary and the bar."""
        for name in self.items:
            row = self.rows.get(name)
            if row is None:
                continue
            status = statuses.get(name, _PENDING)
            if self.statuses.get(name) != status:
                icon_name, colour = _CHIP.get(status, _CHIP[_PENDING])
                row.icon.set_name(icon_name)
                row.icon.style(f"color: {colour};")
            sid = self.ids.get(name) or "—"
            if row.sid.text != sid:
                row.sid.set_text(sid)
        if statuses and statuses != self.statuses:
            p = TaskProgress.from_statuses(statuses)
            self.summary.set_content(_summary_html(statuses, self.item_label))
            self.summary.set_visibility(True)
            # Settled = ok + fail + skip: skipped items never run, so the bar fills when only oks and skips remain.
            self.bar.set_value(p.n_settled / p.total)
            self.bar.props(f"color={'negative' if p.n_fail else 'positive'}")
            self.bar.set_visibility(True)
        self.statuses = statuses

    async def _poll_log(self) -> None:
        job = self._job()
        if job is None or job.relion_job_name != self.job_name:
            return
        active = job.execution_status.is_live or self.statuses.get(self.selected) == _RUNNING
        # One more read after the log stops changing, for its last lines.
        if active or self.log_was_active:
            await self._read_log()
        self.log_was_active = active

    async def _read_log(self) -> None:
        if self.selected == SUPERVISOR:
            job = self._job()
            live = job is not None and job.execution_status == JobStatus.RUNNING
        else:
            live = self.statuses.get(self.selected) == _RUNNING
        await self.viewer.refresh(live=live)

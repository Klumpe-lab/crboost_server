"""Log views: one look for every log in the app.

A log is a dark slate well (`.cb-log-well`, ui/dashboard/css.py) of wrapped 10 px mono, set
apart from the light UI around it; lines that read as errors are red, warnings amber, and the
app's own notes (a truncated head, a file not written yet) dim italic. `log_pane` is a static
one with a header (stream, file name, copy-the-whole-file); `LiveLog` is a polled one; `LogViewer`
is a job page's, a pair of log files polled under a stdout | stderr switch. All take text, so
anything that has a file can show it — an array task's task_N.out, a protocol stage's run.out /
run.err / run_submit.script.
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import NamedTuple

from nicegui import ui

from services.array_tasks import escape_html
from services.project_state import get_project_state_for
from ui.components.buttons import icon_button
from ui.components.copyable import _write_clipboard
from ui.components.segmented import render_segmented
from ui.styles import MONO, SANS

logger = logging.getLogger(__name__)

# Clipboard payload cap: a runaway log can be hundreds of MB, and the text travels
# over the socket. Keep the tail, say so at the top.
_COPY_CAP_BYTES = 8 * 2**20

# A polled well shows a log's last lines, read from its last bytes only: a supervisor's run.err
# reaches megabytes, and the poll runs every few seconds.
_TAIL_BYTES = 256 * 1024
TAIL_LINES = 500
_CUT_NOTE = "[... truncated — earlier output not shown; the copy button copies the whole log ...]"

_ERR_LINE = re.compile(
    r"\b(error|exception|traceback|fatal|failed|failure|killed|oom|segmentation fault|cancelled)\b", re.I
)
_WARN_LINE = re.compile(r"\bwarn(ing)?\b", re.I)
# What the log readers put in place of a log: not the tool's output, so it reads as a note.
_NOTE_LINE = re.compile(r"^(\[\.\.\. truncated .*\]|[\w.-]+\.(out|err) not found\.|no (output|errors) yet)$", re.I)


def log_html(text: str) -> str:
    """The log as the well's HTML: escaped, one span per line that needs a color."""
    out = []
    for line in text.split("\n"):
        safe = escape_html(line)
        if _NOTE_LINE.match(line.strip()):
            out.append(f'<span class="cb-log-note">{safe}</span>')
        elif _ERR_LINE.search(line):
            out.append(f'<span class="cb-log-err">{safe}</span>')
        elif _WARN_LINE.search(line):
            out.append(f'<span class="cb-log-warn">{safe}</span>')
        else:
            out.append(safe)
    return "\n".join(out)


def log_pane(title: str, text: str, path: Path, *, max_h: str = "60vh") -> None:
    with ui.row().classes("w-full items-center no-wrap").style("gap: 6px;"):
        ui.label(title).style(
            f"{MONO} font-size: 9px; color: {'#dc2626' if title == 'stderr' else '#475569'}; font-weight: 600;"
        )
        ui.label(path.name).style(f"{MONO} font-size: 9px; color: #94a3b8;")
        ui.space()
        icon_button("content_copy", lambda p=path: copy_full_log(p), tooltip=f"Copy the full {title} to the clipboard")
    ui.html(log_html(text), sanitize=False).classes("cb-log-well w-full").style(f"max-height: {max_h};")


def _read_full(path: Path) -> str | None:
    if not path.exists():
        return None
    text = path.read_text(errors="replace")
    if len(text) > _COPY_CAP_BYTES:
        text = f"[... truncated to the last {_COPY_CAP_BYTES // 2**20} MB ...]\n" + text[-_COPY_CAP_BYTES:]
    return text


async def copy_full_log(path: Path) -> None:
    text = await asyncio.to_thread(_read_full, path)
    if text is None:
        ui.notify(f"{path.name} not found", type="warning")
        return
    ui.clipboard.write(text)
    ui.notify(f"Copied {path.name} ({len(text.splitlines())} lines)", type="positive", timeout=1500)


class LogTail(NamedTuple):
    text: str  # the lines a well shows, a cut head noted above them
    lines: int  # non-empty lines among them
    cut: bool  # earlier output is left out


def read_log_tail(path: Path) -> LogTail | None:
    """A log's last TAIL_LINES lines, from at most its last _TAIL_BYTES; None when it is missing or holds only
    blank lines. Blocking: run it in a thread."""
    try:
        with open(path, "rb") as f:
            size = f.seek(0, 2)
            f.seek(max(0, size - _TAIL_BYTES))
            raw = f.read()
    except FileNotFoundError:  # not written yet, or gone with its job
        return None
    except OSError as e:
        # Read on a timer: the well says what went wrong, the log file only at debug.
        logger.debug("Could not read %s", path, exc_info=True)
        return LogTail(f"Error reading {path.name}: {e}", 1, False)
    cut = size > _TAIL_BYTES
    if cut:
        # The seek landed mid-line.
        raw = raw[raw.find(b"\n") + 1 :]
    lines = raw.decode("utf-8", errors="replace").splitlines()
    if len(lines) > TAIL_LINES:
        lines, cut = lines[-TAIL_LINES:], True
    shown = sum(1 for line in lines if line.strip())
    if not shown:
        return None
    return LogTail("\n".join([_CUT_NOTE, *lines] if cut else lines), shown, cut)


def _read_pair(out: Path, err: Path) -> tuple[LogTail | None, LogTail | None]:
    return read_log_tail(out), read_log_tail(err)


# Keeps a live log stuck to its end unless the user has scrolled up to read.
_STICK_JS = """(() => {
    const e = getHtmlElement(%d);
    if (!e) return;
    if (!e.dataset.stickWatch) {
        e.dataset.stickWatch = '1';
        e.addEventListener('scroll', () => {
            e.dataset.stick = (e.scrollHeight - e.scrollTop - e.clientHeight < 24) ? '1' : '0';
        });
    }
    if (e.dataset.stick !== '0') e.scrollTop = e.scrollHeight;
})()"""
_RESTICK_JS = "(() => { const e = getHtmlElement(%d); if (e) e.dataset.stick = '1'; })()"


class LiveLog:
    """A polled log tail as one log well, written only when the text changed and kept scrolled
    to its end unless the user scrolled up. (`ui.log` makes an element per line: pushing a
    500-line tail on every poll created and deleted a thousand elements per tick.)"""

    def __init__(self) -> None:
        self._text: str | None = None
        self.element = ui.html("", sanitize=False).classes("cb-log-well cb-livelog")

    @property
    def is_deleted(self) -> bool:
        return self.element.is_deleted

    def set_visibility(self, visible: bool) -> None:
        self.element.set_visibility(visible)

    def set_text(self, text: str) -> None:
        if text == self._text or self.element.is_deleted:
            return
        self._text = text
        self.element.set_content(log_html(text))
        self.element.client.run_javascript(_STICK_JS % self.element.id)

    def reset(self) -> None:
        """Another log takes the well: the next `set_text` writes whatever it is given and lands at its end,
        wherever the reader had scrolled the last one."""
        self._text = None
        if not self.element.is_deleted:
            self.element.client.run_javascript(_RESTICK_JS % self.element.id)

    def stick(self) -> None:
        """To the end, unless the reader scrolled up: a well that was hidden while it was written."""
        if not self.element.is_deleted:
            self.element.client.run_javascript(_STICK_JS % self.element.id)


_STREAMS = (("stdout", "stdout"), ("stderr", "stderr"))
_PLACEHOLDER = {"stdout": "No output yet", "stderr": "No errors yet"}


class LogViewer:
    """A job page's log: the stdout | stderr switch with stderr's line count on it, the file shown (a click
    copies its full path), a live mark while it is written, the job's SLURM id with `slurm_id` (a click
    copies it), and the copy-the-whole-file button, over a LiveLog well per stream that fills the rest of
    its column. `show` points it at another pair of files; `refresh` reads their tails off the loop."""

    def __init__(self, project_path: Path | None, *, slurm_id: bool = False) -> None:
        self._project_path = project_path
        self._files: dict[str, Path] = {}
        self._active = "stdout"
        self._sid = ""
        with ui.row().classes("w-full items-center no-wrap").style("gap: 8px; flex-shrink: 0; padding: 6px 10px 0;"):
            self._seg = render_segmented(list(_STREAMS), "stdout", self._switch, classes="cb-seg-sm")
            self._path = (
                ui.label("")
                .classes("cb-copy-path")
                .style(
                    f"{MONO} font-size: 9px; color: #94a3b8; overflow: hidden; text-overflow: ellipsis; "
                    "white-space: nowrap; min-width: 0;"
                )
                .on("click.stop", self._copy_path)
            )
            self._live = ui.row().classes("items-center no-wrap").style("gap: 4px; flex-shrink: 0;")
            with self._live:
                ui.element("div").classes("cb-live-pulse").style(
                    "width: 6px; height: 6px; border-radius: 50%; background: #3b82f6;"
                )
                ui.label("live").style(f"{SANS} font-size: 9px; color: #3b82f6;")
            self._live.set_visibility(False)
            ui.space()
            self._sid_label: ui.label | None = None
            if slurm_id:
                self._sid_label = (
                    ui.label("")
                    .classes("cb-copy-path")
                    .style(f"{MONO} font-size: 9px; color: #64748b; flex-shrink: 0; white-space: nowrap;")
                    .on("click.stop", self._copy_sid)
                )
                self._sid_label.props["title"] = "The job's SLURM id · click to copy"
                self._sid_label.set_visibility(False)
            icon_button("content_copy", self._copy_full, tooltip="Copy the whole log to the clipboard")
        self._logs = {"stdout": LiveLog(), "stderr": LiveLog()}
        self._logs["stderr"].set_visibility(False)

    @property
    def is_deleted(self) -> bool:
        return self._logs["stdout"].is_deleted

    def show(self, out: Path, err: Path) -> None:
        """Point the viewer at another log's two files. The wells wait for the next `refresh`."""
        files = {"stdout": out, "stderr": err}
        if files == self._files:
            return
        self._files = files
        for log in self._logs.values():
            log.reset()
        self._seg.set_badge("stderr", "")
        self._paint_path()

    async def refresh(self, *, live: bool) -> None:
        """Read the shown files' tails in a thread and write what changed; `live` lights the live mark."""
        files = self._files
        if not files or self.is_deleted:
            return
        out, err = await asyncio.to_thread(_read_pair, files["stdout"], files["stderr"])
        # Pointed elsewhere, or gone, while it read.
        if files is not self._files or self.is_deleted:
            return
        for stream, tail in (("stdout", out), ("stderr", err)):
            self._logs[stream].set_text(tail.text if tail else _PLACEHOLDER[stream])
        self._seg.set_badge("stderr", f"{err.lines}{'+' if err.cut else ''}" if err else "")
        self._live.set_visibility(live)

    def set_slurm_id(self, sid: str | None) -> None:
        if self._sid_label is None or (sid or "") == self._sid:
            return
        self._sid = sid or ""
        self._sid_label.set_text(f"SLURM {self._sid}")
        self._sid_label.set_visibility(bool(self._sid))

    def _switch(self, stream: str) -> None:
        self._active = stream
        for key, log in self._logs.items():
            log.set_visibility(key == stream)
        self._seg.set_active(stream)
        self._paint_path()
        self._logs[stream].stick()

    def _paint_path(self) -> None:
        path = self._files.get(self._active)
        if path is None:
            self._path.set_text("")
            return
        root = self._project_path
        self._path.set_text(str(path.relative_to(root) if root and path.is_relative_to(root) else path))
        self._path.props["title"] = f"{path}\nClick to copy"
        self._path.update()

    async def _copy_path(self) -> None:
        path = self._files.get(self._active)
        if path is not None:
            await _write_clipboard(str(path))

    async def _copy_sid(self) -> None:
        if self._sid:
            await _write_clipboard(self._sid)

    async def _copy_full(self) -> None:
        path = self._files.get(self._active)
        if path is None:
            ui.notify("No log to copy yet", type="warning")
            return
        await copy_full_log(path)


def job_log_placeholder(instance_id: str, ui_mgr) -> None:
    """The log view of a job that has not run: nothing to show yet. While the pipeline runs, the page is
    rebuilt once the job has a directory, so its logs show."""
    with ui.column().classes("w-full h-full items-center justify-center gap-2") as placeholder:
        ui.icon("schedule", size="24px").style("color: #cbd5e1;")
        ui.label("Scheduled · its logs show here once it runs").style(f"{SANS} font-size: 11px; color: #94a3b8;")
    if not ui_mgr.is_running:
        return

    def swap() -> None:
        project_path = ui_mgr.project_path
        job = get_project_state_for(project_path).jobs.get(instance_id) if project_path else None
        if job is not None and job.relion_job_name:
            ui_mgr.cleanup_job_logs_timer(instance_id)
            ui_mgr.request_rebuild()
        elif not ui_mgr.is_running:
            ui_mgr.cleanup_job_logs_timer(instance_id)

    ui_mgr.get_job_widget_refs(instance_id).logs_timer = ui_mgr.job_scope(instance_id).timer(3.0, swap, placeholder)

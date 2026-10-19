"""Logging of the server process: a quiet, readable console and a rotating log file.

Console (stderr): the operator events (`services.event_log.events`) and the warnings and errors of the
server's own code, one line each the way a compose log reads — emitter, time, message — with a blank
line between bursts. A traceback shows there as its last line only; the file has it whole. `--debug`
sends everything to the console.

File (`~/.crboost/logs/crboost-<port>.log`, five files of 20 MB at most): everything the server's own
code logs at INFO and above, every library's warnings and errors, full tracebacks — the record of which
projects were created, opened and deleted, what went to SLURM and how it ended. One file per port, so a
development checkout and the shared install running side by side never rotate each other's log.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TextIO

from services.event_log import EVENTS_LOGGER_NAME
from services.loop_watchdog import TRACE_STALLS
from services.server_auth import CRBOOST_HOME

LOG_DIR = CRBOOST_HOME / "logs"
_MAX_BYTES = 20 * 1024**2
_BACKUPS = 4  # with the live file, 100 MB at most
_FILE_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s  %(module)s:%(lineno)d  %(message)s"
_FILE_DATEFMT = "%Y-%m-%d %H:%M:%S"

# First component of the server's own logger names; any other logger is a library's.
_OWN_ROOTS = frozenset({"crboost", "services", "ui", "backend", "startup_summary", "__main__"})
# Libraries that log below WARNING on their own: held at WARNING, so they never even build the records.
_LIBRARIES = (
    "nicegui",
    "uvicorn",
    "uvicorn.access",
    "uvicorn.error",
    "starlette",
    "fastapi",
    "httpcore",
    "httpx",
    "asyncio",
    "PIL",
    "matplotlib",
    "urllib3",
)
_WATCHDOG_LOGGER = "services.loop_watchdog"
# NiceGUI warns on every 0.1 s binding refresh that takes over 10 ms, ten lines a second under load.
_BINDING_WARNING = "binding propagation for"
_BINDING_EVERY_S = 60.0

# Console layout: `emitter │ HH:MM:SS  ▲ message`.
_LABEL_W = 10
_TIME_W = 8
# A line this long after the one before it starts a new group, set off by a blank line.
_GAP_S = 3.0
# Events are labelled by the module that emitted them, the server's other loggers by their module name.
_EVENT_LABELS = {
    "pipeline_orchestrator_service": "pipeline",
    "pipeline_runner": "jobs",
    "backend": "jobs",
    "project_service": "project",
    "project_delete": "project",
}
_LOGGER_LABELS = {
    "pipeline_orchestrator_service": "pipeline",
    "pipeline_runner": "jobs",
    "pipeline_monitor": "jobs",
    "project_service": "project",
    "project_delete": "project",
    "loop_watchdog": "watchdog",
    "__main__": "server",
}
# ANSI SGR codes: modest, and only where they say something.
_LABEL_COLORS = {"pipeline": "36", "jobs": "34", "project": "32", "server": "35", "worker": "35"}
_LEVEL_GLYPHS = {logging.WARNING: ("▲", "33"), logging.ERROR: ("✗", "31"), logging.CRITICAL: ("✗", "31")}
_STATUS_COLORS = {"Succeeded": "32", "Failed": "31", "Running": "36", "Queued": "2", "Scheduled": "2"}
_STATUS_RE = re.compile(r"\b(" + "|".join(_STATUS_COLORS) + r")\b")
# The project an event names comes first: `<project> · <what happened>`.
_PROJECT_SEP = " · "


def _is_event(record: logging.LogRecord) -> bool:
    return record.name == EVENTS_LOGGER_NAME or record.name.startswith(EVENTS_LOGGER_NAME + ".")


def _is_own(record: logging.LogRecord) -> bool:
    return record.name.split(".", 1)[0] in _OWN_ROOTS


def _label_of(record: logging.LogRecord) -> str:
    if _is_event(record):
        return _EVENT_LABELS.get(record.module, record.module)
    if not _is_own(record):
        return record.name.split(".", 1)[0]
    if record.name.startswith("ui."):
        return "ui"
    last = record.name.rsplit(".", 1)[-1]
    return _LOGGER_LABELS.get(last, last.removesuffix("_service"))


def _wants_color(stream: TextIO) -> bool:
    return stream.isatty() and not os.environ.get("NO_COLOR") and not os.environ.get("CRBOOST_PLAIN_LOG")


class _NiceGuiFilter(logging.Filter):
    """NiceGUI's own noise, for every handler: the storage pruner's "Request is not set" (harmless, on
    every page teardown) is dropped, and the slow-binding warning is let through once a minute."""

    def __init__(self) -> None:
        super().__init__()
        self._last_binding = float("-inf")

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if "Request is not set" in message:
            return False
        if message.startswith(_BINDING_WARNING):
            now = time.monotonic()
            if now - self._last_binding < _BINDING_EVERY_S:
                return False
            self._last_binding = now
        return True


class _ConsoleFilter(logging.Filter):
    """Events, and the warnings and errors that are news: not the stall watchdog's lines (unless
    CRBOOST_TRACE_STALLS asks for them) nor NiceGUI's slow-binding warning, which the file keeps."""

    def __init__(self, debug: bool) -> None:
        super().__init__()
        self._debug = debug

    def filter(self, record: logging.LogRecord) -> bool:
        if self._debug or _is_event(record):
            return True
        if record.levelno < logging.WARNING:
            return False
        if record.name == _WATCHDOG_LOGGER:
            return TRACE_STALLS
        return not record.getMessage().startswith(_BINDING_WARNING)


class _FileFilter(logging.Filter):
    """The server's own records at INFO and above; a library's only from WARNING."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.WARNING or _is_own(record)


class ConsoleFormatter(logging.Formatter):
    """`emitter │ HH:MM:SS  ▲ message`, continuation lines under the message. An exception shows as its
    last line and where its traceback is; without a log file, as the whole traceback."""

    def __init__(self, *, color: bool, log_file: Path | None, label: str | None = None) -> None:
        super().__init__()
        self._color = color
        self._log_file = log_file
        self._label = label

    def _paint(self, code: str, text: str) -> str:
        return f"\x1b[{code}m{text}\x1b[0m" if self._color and code else text

    def format(self, record: logging.LogRecord) -> str:
        label = self._label or _label_of(record)
        if len(label) > _LABEL_W:
            label = label[: _LABEL_W - 1] + "…"
        glyph, glyph_color = _LEVEL_GLYPHS.get(record.levelno, (" ", ""))
        stamp = time.strftime("%H:%M:%S", time.localtime(record.created))
        bar = self._paint("2", " │ ")
        head = (
            f"{self._paint(_LABEL_COLORS.get(label, ''), f'{label:<{_LABEL_W}}')}{bar}"
            f"{self._paint('2', stamp)}  {self._paint(glyph_color, glyph)} "
        )
        message = record.getMessage()
        if _is_event(record):
            message = self._paint_event(message)
        lines = message.splitlines() or [""]
        lines += self._exception_lines(record)
        indent = " " * _LABEL_W + bar + " " * (_TIME_W + 4)
        return head + lines[0] + "".join(f"\n{indent}{line}" for line in lines[1:])

    def _paint_event(self, message: str) -> str:
        project, sep, rest = message.partition(_PROJECT_SEP)
        if not sep:
            project, rest = "", message
        rest = _STATUS_RE.sub(lambda m: self._paint(_STATUS_COLORS[m.group(1)], m.group(1)), rest)
        return self._paint("1", project) + self._paint("2", sep) + rest if sep else rest

    def _exception_lines(self, record: logging.LogRecord) -> list[str]:
        if record.exc_info and record.exc_info[1] is not None:
            if self._log_file is None:
                return self.formatException(record.exc_info).splitlines()
            exc = record.exc_info[1]
            where = str(self._log_file).replace(str(Path.home()), "~", 1)
            return f"↳ {type(exc).__name__}: {exc}  (traceback in {where})".splitlines()
        if record.stack_info:
            return self.formatStack(record.stack_info).splitlines()
        return []


class _ConsoleHandler(logging.StreamHandler):
    """Sets a burst apart: a line more than _GAP_S after the one before it gets a blank line above."""

    def __init__(self, stream: TextIO) -> None:
        super().__init__(stream)
        self._last: float | None = None

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        now = time.monotonic()
        gap = self._last is not None and now - self._last > _GAP_S
        self._last = now
        return f"\n{text}" if gap else text


def setup_logging(*, debug: bool, port: int) -> Path | None:
    """Install the console and the file handler on the root logger. Returns the log file, or None when
    it could not be opened (the console then says why, and the server runs without one)."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if debug else logging.INFO)
    logging.getLogger(EVENTS_LOGGER_NAME).setLevel(logging.INFO)
    for name in _LIBRARIES:
        logging.getLogger(name).setLevel(logging.WARNING)
    logging.getLogger("nicegui").addFilter(_NiceGuiFilter())

    log_file: Path | None = LOG_DIR / f"crboost-{port}.log"
    file_handler = None
    problem = None
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(log_file, maxBytes=_MAX_BYTES, backupCount=_BACKUPS, encoding="utf-8")
    except OSError as e:
        # No room or permission under ~/.crboost must not keep the server from starting: it logs to the console.
        problem, log_file = e, None

    console = _ConsoleHandler(sys.stderr)
    console.setFormatter(ConsoleFormatter(color=_wants_color(sys.stderr), log_file=log_file))
    console.addFilter(_ConsoleFilter(debug))
    root.addHandler(console)

    if file_handler is None:
        logging.getLogger(__name__).warning("No log file, the console is the only log: %s", problem)
        return None
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(_FILE_FORMAT, _FILE_DATEFMT))
    file_handler.addFilter(_FileFilter())
    root.addHandler(file_handler)
    return log_file


def setup_worker_logging() -> None:
    """A worker process (services/workers.py): its warnings and errors on the console, labelled `worker`."""
    handler = _ConsoleHandler(sys.stderr)
    handler.setFormatter(ConsoleFormatter(color=_wants_color(sys.stderr), log_file=None, label="worker"))
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    root.addHandler(handler)

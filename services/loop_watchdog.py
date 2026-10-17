"""Event-loop stall watchdog.

The server answers every click of every browser tab on one asyncio loop. Anything that holds that
loop (a synchronous disk read, a pydantic dump, a thread holding the GIL) delays every client's
next event by as long. This makes such stalls visible instead of guessed at: a heartbeat on the
loop stamps the time every 50 ms, and a daemon thread checks the stamp. While the loop is late,
each check notes which function of the server's own code the loop is in, so when the loop is back
from a stall over the threshold, one line gives the stall's length, where it was spent, and the UI
load at that moment: clients, mounted elements, polled bindings.

The threshold is 1 s, a stall a user feels, and that line is all that is logged. With
CRBOOST_TRACE_STALLS=1 it is 250 ms, and as a stall passes it the thread also logs where the loop
thread is, plus any other thread that is busy rather than waiting (a thread holding the GIL stalls
the loop from outside it): the stacks for fixing one.

One per process, started at app startup (main.py). Warnings, so they pass the quiet log filter.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import time
import traceback
from collections import Counter
from pathlib import Path

logger = logging.getLogger(__name__)

_HEARTBEAT_S = 0.05
# A loop this late has missed a heartbeat: busy in one callback.
_BUSY_S = 2 * _HEARTBEAT_S
_TRACE = bool(os.environ.get("CRBOOST_TRACE_STALLS"))
_THRESHOLD_S = 0.25 if _TRACE else 1.0
# A stall this many times the threshold gets a second sample: a long stall can move between callers.
_RESAMPLE_FACTOR = 4
_STACK_DEPTH = 18
# How many of a stall's places its end line names.
_TOP_PLACES = 4
# Innermost frames of a thread that is waiting, not working: such threads are left out of a report.
_IDLE_FILES = ("threading.py", "queue.py", "selectors.py", "concurrent/futures/thread.py", "multiprocessing/")
_SERVER_ROOT = str(Path(__file__).absolute().parents[1]) + os.sep


class LoopWatchdog:
    def __init__(self, loop: asyncio.AbstractEventLoop, threshold_s: float = _THRESHOLD_S) -> None:
        self._loop = loop
        self._threshold = threshold_s
        self._beat = time.monotonic()
        self._loop_thread: int | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        # Called on the loop thread: known before the first heartbeat, which a startup that blocks
        # the loop delays (that stall would otherwise be reported as some other busy thread).
        self._loop_thread = threading.get_ident()
        self._loop.call_soon(self._tick)
        threading.Thread(target=self._watch, name="loop-watchdog", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def _tick(self) -> None:
        self._beat = time.monotonic()
        self._loop_thread = threading.get_ident()
        if not self._stop.is_set():
            self._loop.call_later(_HEARTBEAT_S, self._tick)

    def _watch(self) -> None:
        # One stall is the time between two heartbeats: it ends when the stamp moves.
        stall_beat: float | None = None
        samples = 0
        peak = 0.0
        places: Counter[str] = Counter()
        while not self._stop.wait(_HEARTBEAT_S):
            beat = self._beat
            lag = time.monotonic() - beat
            if stall_beat is not None and beat != stall_beat:
                if peak:
                    # Logged from the loop: the UI counts read structures only the loop mutates.
                    self._loop.call_soon_threadsafe(_log_stall_end, peak * 1000, _spent_in(places))
                stall_beat, samples, peak = None, 0, 0.0
                places.clear()
            if lag <= _BUSY_S:
                continue
            stall_beat = beat
            places[self._place()] += 1
            if lag > self._threshold:
                peak = max(peak, lag)
                if _TRACE and (samples == 0 or (samples == 1 and lag > self._threshold * _RESAMPLE_FACTOR)):
                    samples += 1
                    logger.warning("Event loop blocked for %.0f ms so far.\n%s", lag * 1000, self._stacks())

    def _place(self) -> str:
        """The function of the server's own code the loop thread is in (time in a library counts
        against the server function that called it); outside the server's code, the innermost one."""
        frame = sys._current_frames().get(self._loop_thread) if self._loop_thread is not None else None
        innermost = frame
        while frame is not None:
            path = frame.f_code.co_filename
            if path.startswith(_SERVER_ROOT) and "site-packages" not in path:
                return f"{path[len(_SERVER_ROOT) :]}:{frame.f_code.co_name}"
            frame = frame.f_back
        if innermost is None:
            return "(unknown)"
        return f"{'/'.join(Path(innermost.f_code.co_filename).parts[-2:])}:{innermost.f_code.co_name}"

    def _stacks(self) -> str:
        frames = sys._current_frames()
        parts: list[str] = []
        loop_frame = frames.get(self._loop_thread) if self._loop_thread is not None else None
        if loop_frame is not None:
            # The import machinery's own frames are left out, so the stack reaches the import that runs.
            entries = [f for f in traceback.extract_stack(loop_frame) if not f.filename.startswith("<frozen importlib")]
            parts.append("Loop thread:\n" + "".join(traceback.format_list(entries[-_STACK_DEPTH:])))
        names = {t.ident: t.name for t in threading.enumerate()}
        for ident, frame in frames.items():
            if ident in (self._loop_thread, threading.get_ident()):
                continue
            if any(part in frame.f_code.co_filename for part in _IDLE_FILES):
                continue
            stack = "".join(traceback.format_stack(frame)[-6:])
            parts.append(f"Busy thread {names.get(ident, ident)}:\n{stack}")
        return "\n".join(parts) or "(no stack available)"


def _spent_in(places: Counter[str]) -> str:
    """`place ×n` for a stall's most frequent places, n of its checks 50 ms apart."""
    total = sum(places.values())
    top = ", ".join(f"{place} ×{n}" for place, n in places.most_common(_TOP_PLACES))
    return f"{top} (of {total} checks)"


def _log_stall_end(stalled_ms: float, spent_in: str) -> None:
    logger.warning("Event loop was blocked for %.0f ms, in %s. UI load: %s", stalled_ms, spent_in, ui_load())


def ui_load() -> str:
    """Clients, mounted elements and polled bindings in this process, as one line."""
    try:
        from nicegui import Client, binding

        clients = list(Client.instances.values())
        elements = sum(len(c.elements) for c in clients)
        return f"{len(clients)} clients, {elements} elements, {len(binding.active_links)} polled bindings"
    except Exception as e:  # diagnostics must never take the server down
        return f"unavailable ({e})"

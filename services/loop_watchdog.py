"""Event-loop stall watchdog.

The server answers every click of every browser tab on one asyncio loop. Anything that holds that
loop (a synchronous disk read, a pydantic dump, a thread holding the GIL) delays every client's
next event by as long. This makes such stalls visible instead of guessed at: a heartbeat on the
loop stamps the time every 50 ms, and a daemon thread checks the stamp. When it is older than the
threshold, the thread logs where the loop thread is, plus any other thread that is busy rather
than waiting (a thread holding the GIL stalls the loop from outside it). When the loop is back, a
second line gives the stall's length and the UI load at that moment: clients, mounted elements,
polled bindings.

One per process, started at app startup (main.py). Warnings, so they pass the quiet log filter.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
import time
import traceback

logger = logging.getLogger(__name__)

_HEARTBEAT_S = 0.05
_THRESHOLD_S = 0.25
# A stall this many times the threshold gets a second sample: a long stall can move between callers.
_RESAMPLE_FACTOR = 4
_STACK_DEPTH = 18
# Innermost frames of a thread that is waiting, not working: such threads are left out of a report.
_IDLE_FILES = ("threading.py", "queue.py", "selectors.py", "concurrent/futures/thread.py", "multiprocessing/")


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
        samples = 0
        peak = 0.0
        while not self._stop.wait(_HEARTBEAT_S):
            lag = time.monotonic() - self._beat
            if lag > self._threshold:
                peak = max(peak, lag)
                if samples == 0 or (samples == 1 and lag > self._threshold * _RESAMPLE_FACTOR):
                    samples += 1
                    logger.warning("Event loop blocked for %.0f ms so far.\n%s", lag * 1000, self._stacks())
            elif samples:
                stalled_ms = peak * 1000
                samples, peak = 0, 0.0
                # Logged from the loop: the UI counts read structures only the loop mutates.
                self._loop.call_soon_threadsafe(_log_stall_end, stalled_ms)

    def _stacks(self) -> str:
        frames = sys._current_frames()
        parts: list[str] = []
        loop_frame = frames.get(self._loop_thread) if self._loop_thread is not None else None
        if loop_frame is not None:
            parts.append("Loop thread:\n" + "".join(traceback.format_stack(loop_frame)[-_STACK_DEPTH:]))
        names = {t.ident: t.name for t in threading.enumerate()}
        for ident, frame in frames.items():
            if ident in (self._loop_thread, threading.get_ident()):
                continue
            if any(part in frame.f_code.co_filename for part in _IDLE_FILES):
                continue
            stack = "".join(traceback.format_stack(frame)[-6:])
            parts.append(f"Busy thread {names.get(ident, ident)}:\n{stack}")
        return "\n".join(parts) or "(no stack available)"


def _log_stall_end(stalled_ms: float) -> None:
    logger.warning("Event loop was blocked for %.0f ms. UI load: %s", stalled_ms, ui_load())


def ui_load() -> str:
    """Clients, mounted elements and polled bindings in this process, as one line."""
    try:
        from nicegui import Client, binding

        clients = list(Client.instances.values())
        elements = sum(len(c.elements) for c in clients)
        return f"{len(clients)} clients, {elements} elements, {len(binding.active_links)} polled bindings"
    except Exception as e:  # diagnostics must never take the server down
        return f"unavailable ({e})"

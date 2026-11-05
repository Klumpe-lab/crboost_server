"""The server's startup on the terminal: one line, rewritten in place while the stages run and cleared once the
server is up:

    Starting CryoBoost...  loading the app  3 s

ASCII only. Without a terminal (output to a file) each stage is one plain line when it finishes. Stdlib only."""

from __future__ import annotations

import atexit
import sys
import threading
import time

_DIM, _RESET = "\x1b[2m", "\x1b[0m"
_HIDE_CURSOR, _SHOW_CURSOR, _CLEAR_LINE = "\x1b[?25l", "\x1b[?25h", "\r\x1b[K"
# One more dot each tick, back to one after three.
_TICK_S = 0.4


class Spinner:
    def __init__(self):
        self._out = sys.stderr
        self._tty = self._out.isatty()
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._thread: threading.Thread | None = None
        self._label = ""
        self._doing = ""
        self._started = 0.0
        self._t0 = 0.0
        # (stage, seconds) of every finished stage, in order.
        self.timings: list[tuple[str, float]] = []

    def start(self) -> Spinner:
        self._started = time.monotonic()
        if self._tty:
            self._out.write(_HIDE_CURSOR)
            # A crash between start and stop must not leave the terminal without a cursor.
            atexit.register(self.stop)
            self._thread = threading.Thread(target=self._run, name="startup-spinner", daemon=True)
            self._thread.start()
        return self

    def stage(self, label: str, doing: str) -> None:
        """Finish the stage in progress (if any) and start `label`, described by `doing`."""
        with self._lock:
            self._finish()
            self._label, self._doing, self._t0 = label, doing, time.monotonic()

    def stop(self) -> None:
        """Finish the stage in progress and give the cursor back. Safe to call more than once."""
        with self._lock:
            self._finish()
        if self._thread is not None:
            self._done.set()
            self._thread.join()
            self._thread = None
            self._out.write(_CLEAR_LINE + _SHOW_CURSOR)
            self._out.flush()

    def _finish(self) -> None:
        if not self._label:
            return
        secs = time.monotonic() - self._t0
        self.timings.append((self._label, secs))
        if not self._tty:
            self._out.write(f"CryoBoost startup · {self._label} {secs:.1f} s\n")
            self._out.flush()
        self._label = ""

    def _run(self) -> None:
        frame = 0
        while not self._done.wait(_TICK_S):
            with self._lock:
                if self._label:
                    dots = "." * (frame % 3 + 1)
                    secs = time.monotonic() - self._started
                    # The cursor goes back to the line's start, so a log line printed meanwhile overwrites this
                    # line (services/log_setup.py clears the rest of it) instead of being appended to it.
                    self._out.write(
                        f"{_CLEAR_LINE}Starting CryoBoost{dots:<3}  {_DIM}{self._doing}  {secs:.0f} s{_RESET}\r"
                    )
                    self._out.flush()
            frame += 1

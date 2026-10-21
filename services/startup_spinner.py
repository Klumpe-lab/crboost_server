"""The server's startup on the terminal, one stage at a time. A finished stage stays as a line with its time;
the stage in progress animates as a tilt series being collected, dose-symmetric from 0° out to ±60°:

      ok  ports      0.4 s  24326
      ..  modules    3.1 s  loading the app    -60 [......::::|:::.......] +60

ASCII only. Without a terminal (output to a file) each stage is one plain line when it finishes. Stdlib only."""

from __future__ import annotations

import atexit
import sys
import threading
import time

_GREEN, _CYAN, _DIM, _RESET = "\x1b[32m", "\x1b[36m", "\x1b[2m", "\x1b[0m"
_HIDE_CURSOR, _SHOW_CURSOR, _CLEAR_LINE = "\x1b[?25l", "\x1b[?25h", "\r\x1b[K"

# -60..+60 in 6° steps, and the order a dose-symmetric scheme collects them in: 0, +6, -6, +12, -12, …
_SLOTS = 21
_CENTRE = _SLOTS // 2
_ORDER = [_CENTRE] + [_CENTRE + sign * k for k in range(1, _CENTRE + 1) for sign in (1, -1)]
# Frames the finished series stays on screen before the next one starts.
_HOLD = 4
_TICK_S = 0.1


def _track(frame: int) -> str:
    """The tilt track at `frame`: collected tilts ':', the one being collected '|', the rest '.'."""
    k = frame % (len(_ORDER) + _HOLD)
    cells = ["."] * _SLOTS
    for slot in _ORDER[: min(k, len(_ORDER))]:
        cells[slot] = ":"
    if k < len(_ORDER):
        cells[_ORDER[k]] = "|"
    return "".join(cells)


class Spinner:
    def __init__(self):
        self._out = sys.stderr
        self._tty = self._out.isatty()
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._thread: threading.Thread | None = None
        self._label = ""
        self._doing = ""
        self._note = ""
        self._t0 = 0.0
        # (stage, seconds) of every finished stage, in order.
        self.timings: list[tuple[str, float]] = []

    def start(self) -> Spinner:
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
            self._label, self._doing, self._note, self._t0 = label, doing, "", time.monotonic()

    def note(self, text: str) -> None:
        """What the finished line of the stage in progress says after its time."""
        self._note = text

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
        if self._tty:
            line = f"{_CLEAR_LINE}  {_GREEN}ok{_RESET}  {self._label:<9}{secs:5.1f} s  {_DIM}{self._note}{_RESET}\n"
        else:
            line = f"CryoBoost startup · {self._label} {secs:.1f} s{'  ' + self._note if self._note else ''}\n"
        self._out.write(line)
        self._out.flush()
        self._label = ""

    def _run(self) -> None:
        frame = 0
        while not self._done.wait(_TICK_S):
            with self._lock:
                if self._label:
                    secs = time.monotonic() - self._t0
                    # The cursor goes back to the line's start, so a log line printed meanwhile overwrites the
                    # stage line (services/log_setup.py clears the rest of it) instead of being appended to it.
                    self._out.write(
                        f"{_CLEAR_LINE}  {_CYAN}..{_RESET}  {self._label:<9}{secs:5.1f} s  {_DIM}{self._doing}"
                        f"  -60 [{_RESET}{_CYAN}{_track(frame)}{_RESET}{_DIM}] +60{_RESET}\r"
                    )
                    self._out.flush()
            frame += 1

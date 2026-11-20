"""crboost's side of its link to EisGreis, the viewer behind the napari chip: which protocol version to write, whether
a viewer is running, and the line that starts one with the user's remote desktop.

crboost and EisGreis share no code. crboost runs the install's `eisgreis-crboost` (conf.yaml `napari.eisgreis`) and
talks to it only through files in `~/.crboost/napari/<id>/`, as EisGreis's `docs/crboost-protocol.md` (the reference)
describes them: crboost writes `open.json` (and `bundle.json`, `services/visualization/napari_bundle.py`); the viewer
writes `opened.json`, `desktop.json` (the versions it reads, where it runs) and touches `alive` every 10 s while it
runs. Both sides write the newest version they share; a side that meets none says so and falls back to nothing.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from services.result import err, ok

logger = logging.getLogger(__name__)

PROTOCOLS = (1,)  # the open.json versions this crboost writes
DESKTOP = "desktop.json"
ALIVE = "alive"
ALIVE_S = 30.0  # a viewer whose heartbeat is older than this has stopped
COMMAND = "eisgreis-crboost"
AUTOSTART_FILE = Path.home() / ".config" / "autostart" / "crboost-napari.desktop"

_versions_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_VERSIONS_TTL_S = 300.0


def eisgreis_command(install: str | Path) -> Path:
    return Path(install).expanduser() / COMMAND


def install_versions(install: str | Path) -> dict[str, Any]:
    """`ok(eisgreis=…, protocol=[…], bundle_versions=[…])` from the install's `eisgreis-crboost --version`, or
    `err` saying why it did not answer. Blocking (it starts the install's Python): call it from a thread. Kept five
    minutes per install."""
    key = str(Path(install).expanduser())
    cached = _versions_cache.get(key)
    if cached is not None and time.monotonic() - cached[0] < _VERSIONS_TTL_S:
        return cached[1]
    command = eisgreis_command(install)
    if not os.access(command, os.X_OK):
        return err(f"napari.eisgreis is {key}, which has no {COMMAND}: point it at an EisGreis install")
    try:
        proc = subprocess.run([str(command), "--version"], capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired) as e:
        logger.exception("%s --version did not answer", command)
        return err(f"{command} --version did not answer: {e}")
    try:
        record = json.loads(proc.stdout.strip().splitlines()[-1])
        result = ok(
            eisgreis=str(record["eisgreis"]),
            protocol=[int(v) for v in record["protocol"]],
            bundle_versions=[int(v) for v in record["bundle_versions"]],
        )
    except (IndexError, KeyError, TypeError, ValueError):
        logger.error("%s --version answered %r (stderr %r)", command, proc.stdout[-500:], proc.stderr[-500:])
        detail = (proc.stderr or proc.stdout).strip().splitlines()[-1:] or ["no output"]
        return err(f"{command} --version did not say which protocol it reads: {detail[0]}")
    _versions_cache[key] = (time.monotonic(), result)
    return result


def choose_protocol(viewer_protocols: list[int], *, eisgreis: str | None, install: str | None) -> dict[str, Any]:
    """`ok(protocol=…)`, the newest version this crboost writes and the viewer reads, or `err` saying which side to
    update."""
    shared = set(PROTOCOLS) & {int(v) for v in viewer_protocols}
    if shared:
        return ok(protocol=max(shared))
    side = "EisGreis" if max(viewer_protocols, default=0) < min(PROTOCOLS) else "crboost"
    reads = ", ".join(str(v) for v in viewer_protocols) or "none"
    writes = ", ".join(str(v) for v in PROTOCOLS)
    viewer = f"EisGreis {eisgreis or '(version unknown)'}" + (f" at {install}" if install else "")
    return err(f"{viewer} reads protocol {reads}, this crboost writes {writes}: update {side}")


def read_desktop(session_dir: Path) -> dict[str, Any] | None:
    """The viewer's `desktop.json`, or None when it has none (not started yet, or stopped). Blocking."""
    try:
        data = json.loads((Path(session_dir) / DESKTOP).read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as e:  # the viewer renames into place, so this is a fault on its side
        logger.error("%s unreadable: %s", Path(session_dir) / DESKTOP, e)
        return None
    return data if isinstance(data, dict) else None


def heartbeat_age(session_dir: Path) -> float | None:
    """Seconds since the viewer last touched `alive`, or None when it has none. Blocking (a stat)."""
    try:
        return max(0.0, time.time() - (Path(session_dir) / ALIVE).stat().st_mtime)
    except FileNotFoundError:
        return None


def is_alive(session_dir: Path) -> bool:
    age = heartbeat_age(session_dir)
    return age is not None and age < ALIVE_S


# ── starting EisGreis with the user's remote desktop ──────────────────────────────────────────────────────────────


def autostart_entry(install: str | Path) -> str:
    """The desktop entry Xfce runs at the start of every desktop session: EisGreis waits for crboost's first request,
    with no window until then."""
    command = str(eisgreis_command(install))
    quoted = f'"{command}"' if any(c in command for c in ' "\\$`') else command
    return (
        "[Desktop Entry]\n"
        "Type=Application\n"
        "Name=EisGreis for crboost\n"
        "Comment=Lets crboost show tomograms in this desktop (napari); written by crboost's User settings\n"
        f"Exec={quoted} --wait\n"
        "Terminal=false\n"
        "X-GNOME-Autostart-enabled=true\n"
    )


def autostart_state() -> dict[str, Any]:
    """`{"on": bool, "command": the Exec line or None}`. Blocking (reads one small file)."""
    try:
        text = AUTOSTART_FILE.read_text()
    except FileNotFoundError:
        return {"on": False, "command": None}
    command = next((line[5:] for line in text.splitlines() if line.startswith("Exec=")), None)
    return {"on": True, "command": command}


def set_autostart(on: bool, install: str | Path | None) -> dict[str, Any]:
    """Write the autostart entry for `install` (on) or remove it (off). Blocking."""
    if not on:
        try:
            AUTOSTART_FILE.unlink()
        except FileNotFoundError:  # already off
            pass
        return ok(on=False)
    if not install:
        return err("napari.eisgreis is not set in conf.yaml, so there is no EisGreis to start with the desktop")
    if not os.access(eisgreis_command(install), os.X_OK):
        return err(
            f"{eisgreis_command(install)} is not there or not executable: is napari.eisgreis an EisGreis install?"
        )
    tmp = None
    try:
        AUTOSTART_FILE.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(AUTOSTART_FILE.parent), prefix=".crboost-napari.", suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            f.write(autostart_entry(install))
        os.chmod(tmp, 0o644)
        os.replace(tmp, AUTOSTART_FILE)
    except OSError as e:
        logger.exception("could not write %s", AUTOSTART_FILE)
        if tmp is not None and os.path.exists(tmp):
            os.unlink(tmp)
        return err(f"{AUTOSTART_FILE} could not be written: {e}")
    return ok(on=True, path=str(AUTOSTART_FILE))

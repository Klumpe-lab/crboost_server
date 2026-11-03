"""Reading a project the kitchen does not own.

A project installed in this server (the home project, or one open in another browser tab) is read
through its live state: the in-memory state is authoritative. Any other project is read through a
snapshot: its ``project_params.json`` parsed read-only and its tilt-series registry loaded, neither
entered in this server's registries. An installed state is reconciled and saved by this server's
pipeline monitor and curation watcher; a snapshot never is, and nothing here writes into the project
it describes.

Blocking (file reads and parses): call `project_view` in a thread.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path

from services.project_state import ProjectState, _project_states
from services.tilt_series.registry import TiltSeriesRegistry, _registries, get_registry_for

logger = logging.getLogger(__name__)

PARAMS_FILE = "project_params.json"


@dataclass(eq=False)
class ProjectView:
    path: Path  # resolved
    state: ProjectState
    installed: bool  # the live state of a project this server has open; else a read-only snapshot
    stamp: tuple  # what the snapshot was read from: (params mtime, registry index mtime)
    _registry: TiltSeriesRegistry | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def label(self) -> str:
        return self.state.project_name or self.path.name

    @property
    def registry(self) -> TiltSeriesRegistry:
        """The tilt-series registry, read on first use (listing what a project offers needs none)."""
        if self.installed:
            return _registries.get(self.path) or get_registry_for(self.path)
        with self._lock:
            if self._registry is None:
                registry = TiltSeriesRegistry(self.path)
                registry.load()
                self._registry = registry
                logger.info(
                    "Kitchen snapshot of %s: registry read (%d tilt series)",
                    self.path.name,
                    len(registry.tilt_series_ids()),
                )
            return self._registry


_SNAPSHOTS: dict[Path, ProjectView] = {}
_LOCK = threading.Lock()


def _stamp(path: Path) -> tuple:
    out = []
    for p in (path / PARAMS_FILE, path / "registry" / "index.json"):
        try:
            out.append(os.stat(p).st_mtime_ns)
        except FileNotFoundError:
            out.append(None)
    return tuple(out)


def is_installed(path: Path) -> bool:
    return Path(path).resolve() in _project_states


def project_view(path: Path) -> ProjectView:
    """The project at `path`: its live state when this server has it open, else a snapshot, re-read
    when its params file or registry index moved since the last read."""
    resolved = Path(path).resolve()
    state = _project_states.get(resolved)
    if state is not None:
        return ProjectView(resolved, state, True, ())
    stamp = _stamp(resolved)
    with _LOCK:
        hit = _SNAPSHOTS.get(resolved)
        if hit is not None and hit.stamp == stamp:
            return hit
    if stamp[0] is None:
        raise FileNotFoundError(f"{resolved} has no {PARAMS_FILE}")
    snapshot = ProjectState.load(resolved / PARAMS_FILE, read_only=True)
    view = ProjectView(resolved, snapshot, False, stamp)
    with _LOCK:
        _SNAPSHOTS[resolved] = view
    logger.info("Kitchen snapshot of %s read", resolved.name)
    return view


def is_current(view: ProjectView) -> bool:
    """False when an installed project was closed or opened since, or a snapshot's files moved."""
    if view.installed:
        return view.path in _project_states
    return view.path not in _project_states and _stamp(view.path) == view.stamp

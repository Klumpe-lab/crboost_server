"""What the views show of a project's segmentation layers: which layers are hidden, and how opaque
the fills are. One setting for the Tomograms view, the Journey, the pick viewer and what "Open in
ChimeraX" loads, held by the server for as long as it runs and not saved with the project: it is
what someone is looking at, not a property of the data. A layer is `<producer>:<feature>`."""

from __future__ import annotations

import os

DEFAULT_OPACITY = "0.4"

_hidden: dict[str, set[str]] = {}
_opacity: dict[str, str] = {}


def _key(project_path) -> str:
    return os.path.normpath(str(project_path))


def layer_id(producer: str, feature: str) -> str:
    return f"{producer}:{feature}"


def is_shown(project_path, layer: str) -> bool:
    return layer not in _hidden.get(_key(project_path), ())


def set_shown(project_path, layer: str, shown: bool) -> None:
    hidden = _hidden.setdefault(_key(project_path), set())
    if shown:
        hidden.discard(layer)
    else:
        hidden.add(layer)


def opacity(project_path) -> str:
    return _opacity.get(_key(project_path), DEFAULT_OPACITY)


def set_opacity(project_path, value: str) -> None:
    _opacity[_key(project_path)] = value

"""What the views show of a project's annotation layers (the pick layers and the segmentations), and
how opaque the segmentation fills are. One setting for the Tomograms view, the Journey, the pick
viewer and what "Open in ChimeraX" loads, held by the server for as long as it runs and not saved
with the project: it is what someone is looking at, not a property of the data. A segmentation layer
is `<producer>:<feature>`; a pick layer names its picks (a candidate-extract job, a list).

A layer follows the version: it shows over a picture of the reconstruction it was made on (or when
either side's reconstruction is not known), and is hidden over another. Switching a layer by hand
pins it, shown or hidden, on every version; a pin back to what following gives is dropped."""

from __future__ import annotations

import os
from collections.abc import Iterable

DEFAULT_OPACITY = "0.4"

_pinned: dict[str, dict[str, bool]] = {}
_opacity: dict[str, str] = {}


def _key(project_path) -> str:
    return os.path.normpath(str(project_path))


def layer_id(producer: str, feature: str) -> str:
    return f"{producer}:{feature}"


def on_source(layer_recon: str | None, shows: str | None) -> bool:
    """Whether a layer made on reconstruction `layer_recon` belongs over a picture of `shows`."""
    return layer_recon is None or shows is None or layer_recon == shows


def is_shown(project_path, layer: str, following: bool = True) -> bool:
    """The layer's pin, else `following` (what the version gives it, `on_source`)."""
    return _pinned.get(_key(project_path), {}).get(layer, following)


def set_shown(project_path, layer: str, shown: bool, following: bool) -> None:
    pins = _pinned.setdefault(_key(project_path), {})
    if shown == following:
        pins.pop(layer, None)
    else:
        pins[layer] = shown


def is_pinned(project_path, layer: str) -> bool:
    return layer in _pinned.get(_key(project_path), {})


def unpin(project_path, layers: Iterable[str]) -> None:
    pins = _pinned.get(_key(project_path), {})
    for layer in layers:
        pins.pop(layer, None)


def opacity(project_path) -> str:
    return _opacity.get(_key(project_path), DEFAULT_OPACITY)


def set_opacity(project_path, value: str) -> None:
    _opacity[_key(project_path)] = value

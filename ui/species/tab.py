"""Tab contract of the Species page (roadmap 10 §6).

The page treats every tab the same way: `build(container)` once, on the first
selection of (species, tab), into a container the page owns and keeps (visibility
flips on switch — never clear()+rebuild); `refresh()` on the page's 3-s observe tick
and whenever the tab is shown, expected to be signature-gated (`FingerprintedView`)
or a no-op.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from nicegui import ui


@dataclass(frozen=True, slots=True)
class TabContext:
    """What every tab of one species gets from the page."""

    backend: object
    project_path: Path
    species_id: str
    callbacks: dict
    # Page hook: the species was deleted (registry + files already gone) — drop
    # its cached containers and select what remains.
    on_species_deleted: Callable[[str], None]


class SpeciesTab(Protocol):
    def build(self, container: ui.element) -> None: ...

    def refresh(self) -> None: ...


class PlaceholderTab:
    """One line of text — the tab's home is a later stage (no dead affordances)."""

    def __init__(self, text: str) -> None:
        self._text = text

    def build(self, container: ui.element) -> None:
        with container:
            ui.label(self._text).classes("text-[11px] italic text-gray-400 px-3 py-2")

    def refresh(self) -> None:
        return None

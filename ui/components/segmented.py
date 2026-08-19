"""Compact segmented control (roadmap 10 S1).

The flat, dense tab switcher of the job tab (`job_tab_component._render_tab_switcher`)
hoisted into a reusable component: 9 px buttons on one bordered strip, the active
segment tinted `#f1f5f9`. NOT Quasar `ui.tabs` (see `feedback_ui_chrome_conventions`).
Switching only flips the `active` class on the segments — no clear()+rebuild, so a
click that lands mid-switch still hits a live element.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from nicegui import ui


class Segmented:
    """One `.cb-seg` strip. `tabs` = ``[(key, label), ...]``; `on_switch(key)` fires on a
    click of a non-active segment (the caller flips its content and calls `set_active`)."""

    def __init__(self, tabs: Sequence[tuple[str, str]], active: str, on_switch: Callable[[str], None]) -> None:
        self._on_switch = on_switch
        self._active = active
        self._segments: dict[str, ui.element] = {}
        self._badges: dict[str, ui.label] = {}
        with ui.element("div").classes("cb-seg"):
            for key, label in tabs:
                seg = ui.element("div").classes("cb-seg-btn" + (" active" if key == active else ""))
                with seg:
                    ui.label(label)
                    # Built once, text set later: a count change must never rebuild the
                    # strip (a click mid-rebuild would land on a dead element).
                    badge = ui.label("").classes("cb-seg-badge")
                    badge.set_visibility(False)
                    self._badges[key] = badge
                seg.on("click", lambda _e, k=key: self._click(k))
                self._segments[key] = seg

    def _click(self, key: str) -> None:
        if key == self._active:
            return
        self._on_switch(key)

    @property
    def active(self) -> str:
        return self._active

    def set_badge(self, key: str, text: str) -> None:
        """Count beside a segment's label. Empty string hides it. Idempotent — call it
        from a poll without gating; only a changed value touches the DOM."""
        badge = self._badges.get(key)
        if badge is None or badge.text == text:
            return
        badge.set_text(text)
        badge.set_visibility(bool(text))

    def set_active(self, key: str) -> None:
        if key == self._active:
            return
        self._active = key
        for k, seg in self._segments.items():
            if k == key:
                seg.classes(add="active")
            else:
                seg.classes(remove="active")


def render_segmented(tabs: Sequence[tuple[str, str]], active: str, on_switch: Callable[[str], None]) -> Segmented:
    """Render a segmented control into the current slot and return its handle."""
    return Segmented(tabs, active, on_switch)

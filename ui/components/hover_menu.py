"""Title-line tools: a small trigger whose dropdown opens under the pointer.

`hover_menu(trigger)` hangs a Quasar menu under `trigger`. It shows after a short hover and stays
while the pointer is on the trigger or on the menu; it closes a moment after the pointer has left
both, on Esc, or on a click elsewhere. A click on the trigger opens it at once. Opening and closing
run in the browser, with no server round-trip; one shared timer, so opening one closes any other.
The rows inside are the caller's. `ChoiceRows` is the single-choice list: a check on the chosen row,
a header over each group when there are several, a note at a row's end; a pick moves the check in
place. `tool_trigger` is the trigger's look: a glyph, then a label when it has one, then a caret; boxed
like a field when it sits among a panel's content rather than on a title line.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, NamedTuple

from nicegui import ui

from ui.components.svg_icon import render_glyph

_STATE = "(window.__cbHoverMenu ??= {timer: 0, open: null})"
_STAY = f"() => {{ clearTimeout({_STATE}.timer); }}"
_LEAVE = (
    f"() => {{ const s = {_STATE}; clearTimeout(s.timer); s.timer = setTimeout(() => {{ "
    "if (s.open !== null) runMethod(s.open, 'hide', []); s.open = null; }, 240); }"
)
# How long the pointer rests on a trigger before its menu opens: crossing the title line on the way
# to its close cross opens nothing.
_OPEN_DELAY_MS = 110


def _enter_js(menu_id: int, delay_ms: int) -> str:
    return (
        f"() => {{ const s = {_STATE}; clearTimeout(s.timer); s.timer = setTimeout(() => {{ "
        f"if (s.open !== null && s.open !== {menu_id}) runMethod(s.open, 'hide', []); "
        f"s.open = {menu_id}; runMethod({menu_id}, 'show', []); }}, {delay_ms}); }}"
    )


def hover_menu(trigger: ui.element, *, align: str = "left", auto_close: bool = False) -> tuple[ui.menu, ui.element]:
    """Attach a hover-opened menu to `trigger`; returns ``(menu, body)``, the rows going into `body`.
    `align` "right" lines the menu's right edge up with the trigger's (for tools at a line's right end).
    `auto_close`: any click in the menu closes it (single-choice menus)."""
    side = "right" if align == "right" else "left"
    props = (
        f'no-parent-event no-focus no-refocus anchor="bottom {side}" self="top {side}" :offset="[0, 3]" '
        'transition-show="fade" transition-hide="fade" :transition-duration="80"'
    )
    if auto_close:
        props += " auto-close"
    with trigger:
        menu = ui.menu().props(props).classes("cb-hmenu")
        with menu:
            body = ui.element("div").classes("cb-hmenu-body")
    trigger.on("mouseenter", js_handler=_enter_js(menu.id, _OPEN_DELAY_MS))
    trigger.on("click", js_handler=_enter_js(menu.id, 0))
    trigger.on("mouseleave", js_handler=_LEAVE)
    body.on("mouseenter", js_handler=_STAY)
    body.on("mouseleave", js_handler=_LEAVE)
    return menu, body


def tool_trigger(glyph: str, text: str | None = None, *, boxed: bool = False) -> tuple[ui.element, ui.label | None]:
    """A title-line tool: `glyph` (a `static/icons` asset or a Material name), then `text` (a label the
    caller can re-set) when given, then a caret; `boxed` draws it in a field's box. Returns ``(trigger,
    label or None)``."""
    trigger = ui.element("div").classes("cb-htool" + (" cb-htool--box" if boxed else ""))
    with trigger:
        render_glyph(glyph, size=13).classes("cb-htool-glyph")
        label = ui.label(text).classes("cb-htool-text") if text is not None else None
        ui.icon("expand_more", size="12px").classes("cb-htool-caret")
    return trigger, label


class Choice(NamedTuple):
    key: str
    label: str
    group: str = ""
    note: str = ""
    enabled: bool = True
    tip: str = ""


class ChoiceRows:
    """Single-choice rows in the current slot. A click on an enabled row calls `on_pick(key)`, which
    calls `set_active` once it has switched; a disabled row is grey and picks nothing."""

    def __init__(self, choices: Sequence[Choice], active: str, on_pick: Callable[[str], Any]) -> None:
        self._active = active
        self._rows: dict[str, ui.element] = {}
        grouped = len({c.group for c in choices}) > 1
        group: str | None = None
        for c in choices:
            if grouped and c.group != group:
                group = c.group
                ui.label(c.group).classes("cb-hmenu-group")
            classes = "cb-hmenu-item" + (" nested" if grouped else "")
            classes += (" on" if c.key == active else "") + ("" if c.enabled else " off")
            row = ui.element("div").classes(classes)
            with row:
                ui.icon("check", size="12px").classes("cb-hmenu-check")
                ui.label(c.label).classes("cb-hmenu-text")
                if c.note:
                    ui.label(c.note).classes("cb-hmenu-note")
            if c.tip:
                row.props["title"] = c.tip
            if c.enabled:
                row.on("click", lambda _e, k=c.key: on_pick(k))
            self._rows[c.key] = row

    def set_active(self, key: str) -> None:
        if key == self._active:
            return
        old, new = self._rows.get(self._active), self._rows.get(key)
        self._active = key
        if old is not None and not old.is_deleted:
            old.classes(remove="on")
        if new is not None and not new.is_deleted:
            new.classes(add="on")

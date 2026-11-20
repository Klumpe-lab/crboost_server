"""The app's own on/off controls: `house_check`, a checkbox, and `yes_no`, the switch a job parameter's row holds.

Quasar's dense checkbox centres its label on a 14 px box, so the label's baseline sits about 2 px above the box's
bottom edge. Ours is a 10 px square that holds no text (the tick is an out-of-flow pseudo-element), in an
`inline-flex` row aligned on baselines: a box with no text takes its bottom edge as its baseline, so the label sits on
the box's line.

Both bind like `bind_value`: the model's attribute drives the control, a click writes it. `on_change` gets the
event NiceGUI's checkbox sends (`e.value`, `e.sender`), so either replaces `ui.checkbox` as it stands. The look of
the `on` state is in `CHECK_CSS`, delivered per client by `ui/dashboard/css.py`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from nicegui import binding, ui
from nicegui.events import ValueChangeEventArguments, handle_event

CHECK_CSS = """
.cb-check { display: inline-flex; align-items: baseline; gap: 6px; cursor: pointer; user-select: none; }
.cb-check .cb-check-b { position: relative; display: inline-block; width: 10px; height: 10px; box-sizing: border-box;
  border: 1px solid var(--cb-ink-3, #5b7089); border-radius: 1px; flex-shrink: 0;
  background: var(--cb-surface, #ffffff); }
.cb-check:hover .cb-check-b { border-color: var(--cb-field-focus, #4a8fd4); }
.cb-check.on .cb-check-b { background: var(--cb-check-on, #334155); border-color: var(--cb-check-on, #334155); }
.cb-check.on .cb-check-b::before { content: '✓'; position: absolute; inset: 0; color: var(--cb-check-on-ink, #fff);
  font-size: 8px;
  line-height: 8px; text-align: center; }
.cb-check .cb-check-t { font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif); font-size: 10px; line-height: 14px;
  color: var(--cb-check-ink, #334155); white-space: nowrap; }
.cb-check.cb-dis { cursor: default; opacity: .55; }
.cb-check.cb-dis:hover .cb-check-b { border-color: var(--cb-ink-3, #5b7089); }
.cb-yn { display: inline-flex; flex-shrink: 0; height: 20px; box-sizing: border-box; cursor: pointer;
  user-select: none; border: 1px solid var(--cb-field-line, #d3dde8); border-radius: var(--cb-field-radius, 2px);
  overflow: hidden; background: var(--cb-field-bg, #ffffff); }
.cb-yn > span { font-family: var(--cb-mono, 'IBM Plex Mono', monospace); font-size: 9px; padding: 0 7px;
  line-height: 18px; color: var(--cb-ink-4, #94a3b8); }
.cb-yn:hover { border-color: var(--cb-field-line-hover, #a9c6e4); }
.cb-yn.on > .y, .cb-yn:not(.on) > .n { background: var(--cb-check-on, #334155);
  color: var(--cb-check-on-ink, #ffffff); }
.cb-yn.cb-dis { cursor: default; }
.cb-yn.cb-dis:hover { border-color: var(--cb-field-line, #d3dde8); }
.cb-yn.cb-dis.on > .y, .cb-yn.cb-dis:not(.on) > .n { background: var(--cb-ink-4, #94a3b8); }
"""


class _Flip(ui.element):
    """An element holding a boolean, drawn as its `on` class; a click flips it."""

    def __init__(
        self, tag: str, cls: str, *, value: bool, on_change: Callable[..., Any] | None, disabled: bool
    ) -> None:
        super().__init__(tag)
        self._value = bool(value)
        self._on_change = on_change
        self._target: tuple[Any, str] | None = None
        self._enabled = True
        self.classes(cls + (" on" if self._value else ""))
        self.on("click", self._click)
        self.set_enabled(not disabled)

    @property
    def value(self) -> bool:
        return self._value

    @value.setter
    def value(self, v: Any) -> None:
        """A change from anywhere (a click, code, the bound model) redraws the control, writes the bound attribute
        when it differs, and sends `on_change`, as `ui.checkbox` does."""
        v = bool(v)
        if v == self._value:
            return
        previous, self._value = self._value, v
        self.classes(add="on") if v else self.classes(remove="on")
        if self._target is not None and bool(getattr(*self._target)) != v:
            setattr(self._target[0], self._target[1], v)
        if self._on_change is not None:
            event = ValueChangeEventArguments(sender=self, client=self.client, value=v, previous_value=previous)
            handle_event(self._on_change, event)

    def set_value(self, v: Any) -> None:
        self.value = v

    def set_enabled(self, on: bool) -> None:
        self._enabled = on
        self.classes(remove="cb-dis") if on else self.classes(add="cb-dis")

    def enable(self) -> None:
        self.set_enabled(True)

    def disable(self) -> None:
        self.set_enabled(False)

    def on_value_change(self, handler: Callable[..., Any]) -> _Flip:
        """Another handler for a click's change, as `ui.checkbox.on_value_change`."""
        previous = self._on_change

        def both(e):
            if previous is not None:
                handle_event(previous, e)
            handle_event(handler, e)

        self._on_change = both
        return self

    def bind_value(self, model: Any, attr: str) -> _Flip:
        """The model's attribute drives the control; a click writes it."""
        self._target = (model, attr)
        binding.bind_from(self, "value", model, attr, backward=bool)
        return self

    def _click(self) -> None:
        if self._enabled:
            self.value = not self._value


def house_check(
    label: str = "",
    *,
    model: Any = None,
    attr: str | None = None,
    value: bool = False,
    on_change: Callable[..., Any] | None = None,
    disabled: bool = False,
    tooltip: str | None = None,
) -> _Flip:
    """A checkbox whose box's bottom edge is its label's baseline. With `model`/`attr` it binds to that attribute
    (`value` is then read from it); `on_change` gets the change event (`e.value`) after the write. An empty `label`
    draws the box alone (a table's row check)."""
    if model is not None and attr is not None:
        value = bool(getattr(model, attr))
    el = _Flip("label", "cb-check", value=value, on_change=on_change, disabled=disabled)
    with el:
        ui.element("span").classes("cb-check-b")
        if label:
            ui.label(label).classes("cb-check-t")
    if model is not None and attr is not None:
        el.bind_value(model, attr)
    if tooltip:
        el.props["title"] = tooltip
    return el


def yes_no(
    *,
    model: Any = None,
    attr: str | None = None,
    value: bool = False,
    on_change: Callable[..., Any] | None = None,
    disabled: bool = False,
) -> _Flip:
    """A yes | no switch, the dark half the value; it stands where a field's box would."""
    if model is not None and attr is not None:
        value = bool(getattr(model, attr))
    el = _Flip("div", "cb-yn", value=value, on_change=on_change, disabled=disabled)
    with el:
        ui.html("yes", sanitize=False, tag="span").classes("y")
        ui.html("no", sanitize=False, tag="span").classes("n")
    if model is not None and attr is not None:
        el.bind_value(model, attr)
    return el

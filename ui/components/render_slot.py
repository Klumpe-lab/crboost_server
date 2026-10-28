"""A picture's place in a view while it renders (services/renders.py).

`RenderSlot(render, fill)` is a box in the shape of the picture. While the picture is not current the
box is its skeleton: a dim dashed outline that breathes while the render runs, with a restart button
in its corner, so a render that seems stuck can be started again by hand (it then shows in the task
tray). A render that failed or was cancelled keeps the skeleton, outlined in red, and says why on the
button's hover. Once the picture is current the box is cleared and `fill()` builds the finished
content into it, in place: nothing else on the page is rebuilt. A picture that is already current
never shows its skeleton (the outline fades in only after a moment).

The render starts by itself (`renders.ensure`). The slot waits on it from a task of its own, so it
fills whatever is rebuilt around it meanwhile, and a slot deleted before its render lands just stops
waiting. `fill` may be async (a fill that reads what the render wrote reads it off the loop).

`keep_current(render, owner, on_landed)` is the same without a box, for a render whose result a view
shows in other ways (a job's preview pass behind several sections, the overlays a command names): it
starts the render if the result is not current, and calls `on_landed` once it lands, unless `owner` is
gone by then.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from typing import Any

from nicegui import background_tasks, ui

from services import renders
from services.renders import FAILED, FRESH, RUNNING, Render
from ui.components.buttons import icon_button

logger = logging.getLogger(__name__)

_LOOKS = ("cb-rslot--wait", "cb-rslot--run", "cb-rslot--failed")


class RenderSlot:
    """The box (`box`) for `render`'s picture, in the current slot, styled `classes` / `style` (which
    must give it its size: the picture's aspect ratio, or a height). `fill()` runs in the box once the
    picture is current.

    `on_landed`, when given, runs instead of `fill` when the picture lands while the slot waits, for a
    view that must read its data again rather than draw the picture into the box (it typically
    rebuilds the section the slot is in). `failure` starts the slot on a failure the view itself
    knows of (an entry a pass wrote as failed); its restart button restarts `render`."""

    def __init__(
        self,
        render: Render,
        fill: Callable[[], Any],
        *,
        classes: str = "",
        style: str = "",
        on_landed: Callable[[], Any] | None = None,
        failure: str | None = None,
    ) -> None:
        self.render = render
        self._fill = fill
        self._on_landed = on_landed
        self._watching = False
        self._waited = False
        self.box = ui.element("div").classes(" ".join(filter(None, ("cb-rslot", classes))))
        if style:
            self.box.style(style)
        if failure:
            self._skeleton(failure)
            return
        self._look("cb-rslot--wait")
        self._watch_soon()

    def _watch_soon(self) -> None:
        if not self._watching:
            self._watching = True
            background_tasks.create(self._watch(), name=f"render-slot {self.render.key}")

    async def _watch(self) -> None:
        try:
            while not self.box.is_deleted:
                state = await renders.ensure(self.render)
                if self.box.is_deleted:
                    return
                if state == FRESH:
                    await self._landed()
                    return
                if state == FAILED:
                    self._skeleton(renders.error(self.render.key) or "unknown")
                    return
                self._waited = True
                self._skeleton(None)
                await renders.wait(self.render.key)
        finally:
            self._watching = False

    async def _landed(self) -> None:
        if self._waited and self._on_landed is not None:
            self._on_landed()
            return
        self.box.clear()
        self.box.classes(remove=" ".join(_LOOKS))
        try:
            with self.box:
                filled = self._fill()
                if inspect.isawaitable(filled):
                    await filled
        except Exception as e:
            logger.exception("Showing %s failed", self.render.key)
            self._skeleton(f"showing it failed: {e}")

    def _look(self, look: str) -> None:
        self.box.classes(remove=" ".join(_LOOKS))
        self.box.classes(add=look)

    def _skeleton(self, failure: str | None) -> None:
        """The outline and its restart button: breathing while the render runs (`failure` None), red when
        it did not finish."""
        self.box.clear()
        self._look("cb-rslot--failed" if failure else "cb-rslot--run")
        title = self.render.title
        tip = (
            f"{title} did not finish: {failure}. Click to render it again."
            if failure
            else f"{title}: rendering. Click to start it again if it seems stuck (progress in the task tray)."
        )
        with self.box, ui.element("div").classes("cb-rslot-tools"):
            if failure:
                ui.icon("error_outline", size="14px").classes("cb-rslot-err")
            # .stop: the box may sit in a clickable frame (a wall tile zooms on a click).
            icon_button("refresh", tooltip=tip).classes("cb-rslot-btn").on("click.stop", lambda _e: self._restart())

    def fail(self, reason: str) -> None:
        """Show the failed skeleton for a failure the view finds in what the render wrote (a pass that
        has nothing for this item); its button restarts the render."""
        self._skeleton(reason)

    def _restart(self) -> None:
        renders.restart(self.render)
        self._waited = True
        self._skeleton(None)
        self._watch_soon()


def keep_current(
    render: Render,
    owner: Any,
    on_landed: Callable[[], Any] | None = None,
    *,
    on_failed: Callable[[], Any] | None = None,
) -> None:
    """Start `render` if its result is not current, and call `on_landed` when a render of it lands while
    `owner` (an element) is still there, `on_failed` when it ends without landing; nothing when it was
    current already."""

    async def _follow() -> None:
        state = await renders.ensure(render)
        if state != RUNNING:
            return
        while state == RUNNING:
            await renders.wait(render.key)
            if owner.is_deleted:
                return
            state = await renders.ensure(render)
        if state == FRESH and on_landed is not None:
            on_landed()
        elif state == FAILED and on_failed is not None:
            on_failed()

    background_tasks.create(_follow(), name=f"keep-current {render.key}")

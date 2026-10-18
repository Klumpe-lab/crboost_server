"""
Reactive UI primitives — timers, refreshes, and async handler guards.

# Why this module exists

NiceGUI's default refresh idiom is `container.clear()` followed by a full
rebuild. It's expressive and easy to write, but it has two UX costs:

  1. Every child element (and its event handlers) is destroyed and
     re-created. If a user click is mid-flight against the old DOM, it
     gets dropped silently — the element it targeted no longer exists
     by the time the websocket message arrives. Symptom: "I clicked
     the button and nothing happened."
  2. Hover state resets. If the cursor is over a row that gets torn
     down, :hover doesn't fire on the new row until the cursor moves.
     Symptom: "the row doesn't react to my cursor for a few seconds."

These costs compound when the rebuild is timer-driven (autonomous,
fires whether or not the user is interacting) and the timer cadence is
short relative to typical click latency. A 3-second poll with a 200ms
rebuild window has a ~7% chance of swallowing any given click.

`FingerprintedView` and `SingleFlight` are the standard answers.

# The pattern

  - Pollers observe state. They tick on a timer, but they don't touch
    the DOM directly. They compute a cheap fingerprint of the data
    they'd render and ask the view to refresh. The view rebuilds only
    if the fingerprint changed.

  - Async handlers that own a dialog or do long work are wrapped in
    `SingleFlight`. Re-entering while in flight is a silent no-op,
    so dropped/rapid clicks can't stack invocations.

  - Animations (spinners, pulses) are CSS @keyframes, not server-side
    timer ticks. The server has no business running 6-Hz timers to
    advance a glyph.

See CLAUDE.md "UI reactivity patterns" for the full rationale and
when to reach for each helper.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any
from collections.abc import Callable, Hashable

from nicegui import ui

from ui.components.dialogs import dialog_host

logger = logging.getLogger(__name__)

# How long typing must pause before a field's derived views (summaries, warnings) follow it.
EDIT_SETTLE_S = 0.3


def slot_is_gone(slot) -> bool:
    """Whether the element a captured slot belongs to is gone: deleted, or already garbage-collected
    (NiceGUI's `slot.parent` raises then rather than returning a deleted element)."""
    try:
        return slot.parent.is_deleted
    except RuntimeError:  # the parent was collected: as gone as a deleted one
        return True


class Debounced:
    """`callback` run once, `delay_s` after the last call: for work that follows an edit (a summary,
    a warning, a section rebuild) and must not run per keystroke. The field itself updates the
    model at once; only what is derived from it waits for the typing to pause. `cancel()` drops a
    pending run (a SLURM field taking focus holds its section's rebuild)."""

    def __init__(self, callback: Callable[[], Any], delay_s: float = EDIT_SETTLE_S) -> None:
        self._callback = callback
        self._delay_s = delay_s
        self._task: asyncio.Task | None = None

    def __call__(self) -> None:
        self.cancel()
        try:
            slot = ui.context.slot
        except RuntimeError:
            slot = None
        self._task = asyncio.create_task(self._later(slot))

    def cancel(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()

    async def _later(self, slot) -> None:
        await asyncio.sleep(self._delay_s)
        # NiceGUI keeps a slot stack per asyncio task, and this one starts empty: the callback runs
        # in the slot of the call that scheduled it, so whatever it builds lands where it would have.
        if slot is None:
            self._callback()
            return
        if slot_is_gone(slot):
            return
        with slot:
            self._callback()


def owned_timer(interval: float, callback: Callable[[], Any], owner: Any, *, once: bool = False) -> ui.timer:
    """A ui.timer for a view whose container a rebuild can delete (a roster row, a job page).

    It is parented at the page layout, which no rebuild clears, and cancels itself on its first
    tick after `owner` (the view's container) is deleted. A timer created inside the container
    dies with it instead, and when that happens before its first tick (a page build renders the
    view twice before the browser connects), NiceGUI logs "The parent slot of the element has
    been deleted": it reads the timer's parent slot before it checks whether the timer was
    deleted."""
    timer: ui.timer | None = None

    def tick() -> Any:
        if owner.is_deleted:
            if timer is not None:
                timer.cancel()
            return None
        return callback()

    with dialog_host():
        timer = ui.timer(interval, tick, once=once)
    return timer


class ViewScope:
    """Whether a view is on screen, for the timers that serve it.

    A view that is not on screen does no work: its pollers are made with `scope.timer(...)`, which
    pauses them while the scope is inactive and resumes them when it is shown again. Whoever shows
    and hides the view calls `set_active`: the workspace for the pipeline view (`ui/workspace_page.py`
    `_apply_views`), the job panel for one job's page (`PipelineBuilderPanel._sync_job_scopes`).
    `on_show` callbacks run when the scope comes back, for a view that must catch up at once rather
    than on its next tick. Scopes live per browser tab (`UIStateManager.view_scope`)."""

    def __init__(self, active: bool = True) -> None:
        self.active = active
        self._timers: list[tuple[ui.timer, Any]] = []
        self._on_show: list[Callable[[], Any]] = []

    def timer(self, interval: float, callback: Callable[[], Any], owner: Any, *, once: bool = False) -> ui.timer:
        """An `owned_timer` that runs only while the scope is active."""
        t = owned_timer(interval, callback, owner, once=once)
        if not self.active:
            t.deactivate()
        self._timers.append((t, owner))
        return t

    def on_show(self, callback: Callable[[], Any]) -> None:
        self._on_show.append(callback)

    def set_active(self, on: bool) -> None:
        if on == self.active:
            return
        self.active = on
        live: list[tuple[ui.timer, Any]] = []
        for t, owner in self._timers:
            if getattr(t, "_is_canceled", False) or t.is_deleted:
                continue
            if owner.is_deleted:
                # A paused timer never ticks, so it would never notice its owner is gone.
                t.cancel()
                continue
            live.append((t, owner))
            if on:
                t.activate()
            else:
                t.deactivate()
        self._timers = live
        if on:
            for callback in list(self._on_show):
                callback()


class FingerprintedView:
    """A container view that re-renders only when its signature changes.

    Subclass and override `signature()` and `render()`. Call `refresh()`
    from timers or event handlers — it's a no-op when the signature
    hasn't changed since the last render.

    The signature must reflect every piece of state the render reads.
    A missed input means a stale view; an extra input means harmless
    extra rebuilds. Err toward extra inputs.

    `signature()` is allowed to do cheap disk I/O (file mtimes, small
    JSON reads) but should not block on the network or do heavy
    computation — it runs on every tick, even when the view doesn't
    rebuild.
    """

    def __init__(self, container: Any = None) -> None:
        """`container` is the NiceGUI element that holds the view's children.

        It must support `clear()` and the `with container:` context-manager
        protocol — i.e. anything that subclasses `ui.element`.

        Pass `None` if the container is assigned later by the owner (e.g.,
        page rebuilds replace the element); override `_get_container()` to
        look it up dynamically.
        """
        self._container = container
        self._last_signature: Hashable | None = None
        self._dirty: bool = True

    def _get_container(self) -> Any:
        """Return the current container, or None if not yet mounted. Override
        when the container can change across page rebuilds."""
        return self._container

    def signature(self) -> Hashable:
        raise NotImplementedError

    def render(self) -> None:
        """Write children into the container. The harness already cleared
        the container and entered its context manager before calling this."""
        raise NotImplementedError

    def mark_dirty(self) -> None:
        """Force the next `refresh()` to rebuild regardless of signature.

        Use for structural changes the signature can't naturally capture
        (e.g., a CSS-only style change that the renderer reads from a
        non-pydantic source). Prefer extending the signature when possible.
        """
        self._dirty = True

    def refresh(self) -> None:
        container = self._get_container()
        # Deleted along with its client (tab closed) after this tick was scheduled: clearing it
        # only makes NiceGUI warn "Client has been deleted but is still being used".
        if container is None or container.is_deleted:
            return
        try:
            sig = self.signature()
        except Exception as e:
            # Better to over-render than to crash the UI tick. The next
            # successful signature() will re-establish the baseline.
            logger.info("FingerprintedView signature() raised, forcing rebuild: %s", e)
            sig = None
            self._dirty = True
        if not self._dirty and sig == self._last_signature:
            return
        try:
            container.clear()
            with container:
                self.render()
        except RuntimeError as e:
            # Client gone (tab close races with a tick). Leave dirty so
            # the next mount rebuilds; don't update last_signature.
            logger.info("FingerprintedView render skipped (client gone): %s", e)
            return
        self._last_signature = sig
        self._dirty = False


class SingleFlight:
    """Tracks in-flight async operations by key. Re-entering with the same
    key while one is in flight is a silent no-op.

    Use for any handler that opens a dialog or does long async work that
    the user can re-trigger by clicking faster than the work completes.

    Usage:

        flight = SingleFlight()

        async def open_thing():
            async with flight("open_thing") as acquired:
                if not acquired:
                    return
                # ... open dialog, await result, etc.

    Why a set-based guard rather than disabling the trigger button:
    the button may be destroyed and recreated by an unrelated refresh
    (e.g., the rosters rebuild). Disabling the old button doesn't
    disable the new one. The guard operates on the handler, not the
    element, so it's robust to DOM churn.
    """

    def __init__(self) -> None:
        self._in_flight: set[str] = set()

    @asynccontextmanager
    async def __call__(self, key: str):
        if key in self._in_flight:
            yield False
            return
        self._in_flight.add(key)
        try:
            yield True
        finally:
            self._in_flight.discard(key)

    def in_flight(self, key: str) -> bool:
        return key in self._in_flight

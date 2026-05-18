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

  - Pollers observe state. They tick on a timer, but they DON'T touch
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

import logging
from contextlib import asynccontextmanager
from typing import Any, Hashable, Optional, Set

logger = logging.getLogger(__name__)


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
        self._last_signature: Optional[Hashable] = None
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
        if container is None:
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
        self._in_flight: Set[str] = set()

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

"""Pictures made for display: one render per picture, however many views wait for it.

A view that shows something rendered from project data (a tomogram's slabs, a list's cutout atlas, a
display filter of an atlas, a candidate-extract job's preview pass, a job's IMOD overlays) describes
the render as a `Render` and asks `ensure` for it. The render starts when its outputs are missing or
older than its sources and nothing renders them yet; every view asking for the same picture waits on
that one render (`wait`), which tells all of them when it settles, whatever was rebuilt or closed in
the meantime. The work runs in a worker process (services/workers.py), or in a thread for work that
only waits on a subprocess, and shows in the task tray (services/background_tasks.py) like any other
background task, where it can be cancelled; a `quiet` render (one tile of a wall of them) stays out
of the tray unless it is restarted by hand.

What is remembered: the renders in flight; why each failed or cancelled render did not finish, until
`restart` or until its outputs turn current by other means; and the sources each finished render was
made from, so a picture rendered from a volume whose mtime runs ahead of the server's clock (a compute
node's clock) still counts as current. Nothing else: a finished render is its files on disk, so no
record can claim "rendering" for a render that is not running. A failed render is not retried by
itself; `restart` (the button on a view's skeleton, ui/components/render_slot.py) drops whatever is
running or failed and starts it again.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from services.background_tasks import get_background_task_registry
from services.workers import WorkKind, run_in_worker

logger = logging.getLogger(__name__)

FRESH = "fresh"
RUNNING = "running"
FAILED = "failed"


@dataclass(eq=False)
class Render:
    """One picture's render. `key` names the picture (its output path, usually): views asking for the
    same key share one render. `fn(*args, **kwargs)` makes it, in a worker of `kind` (a module-level,
    worker-safe function: services/workers.py), or in a thread with `in_thread`; it must write every
    path in `outputs`. With `progress`, `fn` takes a `progress_cb(current, total, message)` keyword,
    which the tray shows; `after()`, when given, runs in the server once `fn` has returned (to record
    where the render wrote, in project state). The picture is current when every output exists, none
    is older than any of `sources`, and `current()`, when given, agrees: for what mtimes cannot tell (a
    schema version in an index, a manifest's completeness, files the render names only once it runs).
    `current` runs in a thread and may read files."""

    key: str
    title: str
    fn: Callable[..., Any]
    args: tuple = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    outputs: tuple[Path, ...] = ()
    sources: tuple[Path, ...] = ()
    current: Callable[[], bool] | None = None
    after: Callable[[], Awaitable[None]] | None = None
    kind: WorkKind = WorkKind.RENDER
    in_thread: bool = False
    progress: bool = False
    quiet: bool = False
    project_path: str | None = None
    subtitle: str | None = None


@dataclass(eq=False)
class _Flight:
    render: Render
    done: asyncio.Future
    task_id: str = ""
    task: asyncio.Task | None = None


_flights: dict[str, _Flight] = {}
_failed: dict[str, str] = {}
_ensuring: set[asyncio.Task] = set()
# key -> the sources' signature when its last finished render started.
_made_from: dict[str, tuple] = {}


def _signature(paths) -> tuple:
    sig = []
    for p in paths:
        try:
            st = os.stat(p)
            sig.append((str(p), st.st_mtime_ns, st.st_size))
        except OSError:
            sig.append((str(p), None, None))
    return tuple(sig)


def staleness(r: Render) -> str | None:
    """Why `r`'s picture is not current, or None when it is. Blocking (stats, and `r.current`): call it
    in a thread."""
    oldest = None
    for out in r.outputs:
        try:
            m = os.stat(out).st_mtime
        except FileNotFoundError:
            return f"{out} is missing"
        oldest = m if oldest is None else min(oldest, m)
    if oldest is not None and r.sources and _made_from.get(r.key) != _signature(r.sources):
        for src in r.sources:
            try:
                m = os.stat(src).st_mtime
            except FileNotFoundError:
                continue  # a source that is gone cannot be newer; the render itself says if it is needed
            if m > oldest:
                return f"{src} is {m - oldest:.0f} s newer than the picture"
    if r.current is not None and not r.current():
        return "its outputs are incomplete or out of date"
    return None


async def ensure(r: Render) -> str:
    """The state of `r`'s picture: FRESH, RUNNING or FAILED (`error(r.key)` says why). A picture that is
    not current, with nothing rendering it and no failure on record, starts rendering here."""
    if r.key in _flights:
        return RUNNING
    why = await asyncio.to_thread(staleness, r)
    if why is None:
        _failed.pop(r.key, None)
        return FRESH
    # Another view may have started it while the check ran.
    if r.key in _flights:
        return RUNNING
    if r.key in _failed:
        return FAILED
    logger.info("render %s: starting (%s)", r.key, why)
    _start(r)
    return RUNNING


def ensure_soon(r: Render) -> None:
    """`ensure` from code that cannot wait for it (a status reconciler, a sync builder)."""
    task = asyncio.get_running_loop().create_task(ensure(r), name=f"ensure:{r.key}")
    _ensuring.add(task)
    task.add_done_callback(_ensuring.discard)


async def wait(key: str) -> None:
    """Until the render of `key` in flight, if any, settles. Shielded: a waiting view that goes away does
    not cancel the render for the others."""
    flight = _flights.get(key)
    if flight is not None:
        await asyncio.shield(flight.done)


def running(key: str) -> bool:
    return key in _flights


def error(key: str) -> str | None:
    """Why the last render of `key` did not finish, until it is restarted."""
    return _failed.get(key)


def restart(r: Render) -> None:
    """Start `r` again, in the tray however quiet it is: a render of it in flight is cancelled, a failure
    on record is dropped."""
    old = _flights.pop(r.key, None)
    _failed.pop(r.key, None)
    logger.info("render %s: restarted by hand", r.key)
    _start(r, quiet=False)
    if old is not None:
        # Its worker, if it has one, runs to the end; only the wait on it ends here, at once (a task
        # cancelled before it ran never reaches its own settling).
        if old.task is not None:
            old.task.cancel()
        else:
            get_background_task_registry().cancel(old.task_id)
        if not old.done.done():
            old.done.set_result(None)


def _start(r: Render, *, quiet: bool | None = None) -> None:
    flight = _Flight(r, asyncio.get_running_loop().create_future())
    _flights[r.key] = flight

    async def work(progress_cb) -> str | None:
        try:
            made_from = await asyncio.to_thread(_signature, r.sources)
            if r.in_thread:
                result = await asyncio.to_thread(r.fn, *r.args, **r.kwargs)
            else:
                result = await run_in_worker(
                    r.kind, r.fn, *r.args, progress_cb=progress_cb if r.progress else None, **r.kwargs
                )
            if r.after is not None:
                await r.after()
            if await asyncio.to_thread(_signature, r.sources) == made_from:
                _made_from[r.key] = made_from
                why = await asyncio.to_thread(staleness, r)
                if why is not None:
                    raise RuntimeError(f"the render finished, but {why}")
            # Sources that changed while it ran: the next look at the picture renders it again.
            return result if isinstance(result, str) else None
        except asyncio.CancelledError:
            if _flights.get(r.key) is flight:
                _failed[r.key] = "cancelled"
            raise
        except Exception as e:
            if _flights.get(r.key) is flight:
                _failed[r.key] = str(e) or type(e).__name__
            raise
        finally:
            if _flights.get(r.key) is flight:
                del _flights[r.key]
            if not flight.done.done():
                flight.done.set_result(None)

    if r.quiet if quiet is None else quiet:
        flight.task = asyncio.create_task(_quietly(work, r.key), name=f"render:{r.key}")
        flight.task.add_done_callback(lambda _t: _settle(flight))
    else:
        registry = get_background_task_registry()
        flight.task_id = registry.submit(work, title=r.title, subtitle=r.subtitle, project_path=r.project_path)
        registry.add_done_callback(flight.task_id, lambda _record: _settle(flight))


def _settle(flight: _Flight) -> None:
    """A flight's end, however its task ended: also a task cancelled before it ran, whose work never
    reached its own `finally`."""
    key = flight.render.key
    if _flights.get(key) is flight:
        del _flights[key]
        _failed.setdefault(key, "cancelled")
    if not flight.done.done():
        flight.done.set_result(None)


async def _quietly(work, key: str) -> None:
    """A quiet render's task: its failure is on record (`error`) and in the log, not in the tray."""
    try:
        await work(lambda *_a: None)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Render %s failed", key)

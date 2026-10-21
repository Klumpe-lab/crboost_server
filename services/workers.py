"""Worker processes for CPU-bound work.

The server is one process, and its event loop answers every click of every browser tab. Work that
is CPU-bound in Python (preview passes, cutout filters, slab renders, volume registration, tilt
thumbnails) holds the GIL while it runs, so even in a thread it stalls that loop. Such work runs
here instead: a pool of worker processes owned by the server, started on first use and stopped
with it.

What may be sent (`run_in_worker`):
  - a module-level function that imports without the UI (no nicegui),
  - arguments and a result that pickle: paths, numbers, strings, small dicts and lists. Never a
    ProjectState, a job model, an element or a callback: a worker reads what it needs from disk;
  - side effects only on files the work owns. Every state change and UI update happens back in
    the server, from the returned result (a BackgroundTask's on_complete).

Each kind of work has its own cap, so a burst of one kind (a wall of slab renders) cannot take
every worker from another. Workers run at a lower CPU priority than the server, and are spawned
(not forked: the server is multi-threaded, and a fork copies its locks mid-use).
"""

from __future__ import annotations

import asyncio
import ctypes
import functools
import json
import logging
import multiprocessing
import os
import signal
import tempfile
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from enum import StrEnum
from typing import Any, TypeVar

from services.log_setup import setup_worker_logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class WorkKind(StrEnum):
    """What a piece of worker work is, for its cap."""

    RENDER = "render"  # one picture from one volume: slabs, matched planes, cutout atlases
    PREVIEW = "preview"  # a candidate-extract job's whole preview pass
    THUMBNAILS = "thumbnails"  # a project's tilt preview pass
    SEGMENTATIONS = "segmentations"  # reading a batch of segmentation volumes an import registers


# Concurrent tasks per kind. A preview or thumbnail pass is long and walks a whole job, so one at a
# time; renders are short and asked for in bursts.
_CAPS: dict[WorkKind, int] = {
    WorkKind.RENDER: 3,
    WorkKind.PREVIEW: 1,
    WorkKind.THUMBNAILS: 1,
    WorkKind.SEGMENTATIONS: 1,
}
_DEFAULT_WORKERS = 4
# Added to the workers' nice value: under load the server's loop gets the CPU first.
_WORKER_NICENESS = 10
# prctl(2): the signal a process gets when its parent dies.
_PR_SET_PDEATHSIG = 1
# How often the server reads a running task's progress file.
_PROGRESS_POLL_S = 1.0

_pool: ProcessPoolExecutor | None = None
_limits: dict[WorkKind, asyncio.Semaphore] = {}


def _worker_count() -> int:
    try:
        return max(1, int(os.environ.get("CRBOOST_WORKERS", _DEFAULT_WORKERS)))
    except ValueError:
        return _DEFAULT_WORKERS


def _init_worker() -> None:
    # Ctrl-C reaches the whole process group; the server shuts the pool down itself.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    # A server killed with -9, or lost with its terminal, would otherwise leave its workers running for good.
    try:
        ctypes.CDLL(None, use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)
    except (OSError, AttributeError):
        # Not Linux: no prctl, and the workers outlive a killed server as before.
        pass
    try:
        os.nice(_WORKER_NICENESS)
    except OSError:
        pass
    setup_worker_logging()


def _get_pool() -> ProcessPoolExecutor:
    global _pool
    if _pool is None:
        _pool = ProcessPoolExecutor(
            max_workers=_worker_count(), mp_context=multiprocessing.get_context("spawn"), initializer=_init_worker
        )
    return _pool


def _limit(kind: WorkKind) -> asyncio.Semaphore:
    if kind not in _limits:
        _limits[kind] = asyncio.Semaphore(_CAPS[kind])
    return _limits[kind]


def _write_progress(path: str, current: int, total: int, message: str = "") -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump([int(current), int(total), str(message or "")], f)
    os.replace(tmp, path)


def _call_reporting_progress(fn: Callable[..., T], progress_path: str, args: tuple, kwargs: dict) -> T:
    """Worker side of `progress_cb`: `fn` gets a callback that writes its progress to a small file,
    which the server reads while it waits."""
    return fn(*args, progress_cb=functools.partial(_write_progress, progress_path), **kwargs)


def _read_progress(path: str) -> tuple[int, int, str] | None:
    try:
        with open(path) as f:
            current, total, message = json.load(f)
        return int(current), int(total), str(message)
    except (OSError, ValueError, TypeError):
        return None


async def run_in_worker(
    kind: WorkKind,
    fn: Callable[..., T],
    *args: Any,
    progress_cb: Callable[[int, int, str], None] | None = None,
    **kwargs: Any,
) -> T:
    """Run `fn(*args, **kwargs)` in a worker process and return its result; see the module docstring
    for what `fn` and its arguments may be. Waits for a free slot of `kind` first.

    With `progress_cb` (a BackgroundTask's), `fn` must take a `progress_cb(current, total, message)`
    keyword: what it reports there reaches `progress_cb` here about once a second. A worker that
    died (out of memory, killed) fails this call, and the next call starts a fresh pool."""
    global _pool
    async with _limit(kind):
        loop = asyncio.get_running_loop()
        progress_path = None
        if progress_cb is not None:
            fd, progress_path = tempfile.mkstemp(prefix="crboost-progress-", suffix=".json")
            os.close(fd)
            call = functools.partial(_call_reporting_progress, fn, progress_path, args, kwargs)
        else:
            call = functools.partial(fn, *args, **kwargs)
        try:
            future = loop.run_in_executor(_get_pool(), call)
            if progress_path is None:
                return await future
            while True:
                done, _pending = await asyncio.wait({future}, timeout=_PROGRESS_POLL_S)
                seen = await asyncio.to_thread(_read_progress, progress_path)
                if seen is not None:
                    progress_cb(*seen)
                if done:
                    return future.result()
        except BrokenProcessPool:
            logger.warning("A worker process died running %s; the pool is restarted for the next task", fn.__name__)
            broken, _pool = _pool, None
            if broken is not None:
                broken.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            if progress_path is not None:
                for p in (progress_path, f"{progress_path}.tmp"):
                    try:
                        os.unlink(p)
                    except OSError:
                        pass


def warm_up() -> None:
    """Start one worker at server startup, in the background: a spawned worker starts a fresh interpreter
    (Python's spawn re-imports main.py, as `__mp_main__`) before its first task, which would otherwise
    delay the first render."""
    _get_pool().submit(os.getpid)


def shutdown() -> None:
    """Stop the workers (server shutdown); running tasks are cancelled."""
    global _pool
    if _pool is not None:
        _pool.shutdown(wait=False, cancel_futures=True)
        _pool = None

"""The user's napari session, as the rail's napari light shows it: one process-wide cache.

`backend.find_active_napari_session` reads the session registry and asks `squeue`, so it is not polled per tab:
`poll()` refreshes the cached answer at most every `POLL_S` (or at once with `force`, after the dialog starts or
stops a session), and every light reads the cache. A poll that raises leaves `unknown`, never `off`: the light must
not tell the user nothing runs while a session is up.
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

POLL_S = 15.0

OFF = "off"
PENDING = "pending"  # queued in SLURM
STARTING = "starting"  # running, the desktop not up yet
READY = "ready"
UNKNOWN = "unknown"  # never polled yet, or the last poll raised

_state: dict = {"status": UNKNOWN, "at": 0.0, "info": {}, "error": ""}
_polling = False


def snapshot() -> tuple[str, dict]:
    """(status, the session's info: node, requested, opened, …), no I/O."""
    return _state["status"], _state["info"]


def last_error() -> str:
    return _state["error"]


def shown_volume(info: dict) -> str | None:
    """The volume the session was last asked to show, or None."""
    layers = (info.get("requested") or {}).get("layers") or []
    return str(layers[0].get("path") or "") or None if layers else None


async def poll(backend, *, force: bool = False) -> str:
    """Refresh the cache when it is older than `POLL_S` (or `force`); a concurrent call returns the cache."""
    global _polling
    if backend is None or _polling or (not force and time.monotonic() - _state["at"] < POLL_S):
        return _state["status"]
    _polling = True
    try:
        info = await backend.find_active_napari_session()
        if not info:
            _state.update(status=OFF, info={}, error="")
        else:
            if info.get("status") == "ready":
                status = READY
            elif str(info.get("slurm_state") or "").upper().startswith("PEND"):
                status = PENDING
            else:
                status = STARTING
            _state.update(status=status, info=dict(info), error="")
    except Exception as e:  # reported, and shown as unknown rather than as no session
        logger.exception("napari session poll failed")
        _state.update(status=UNKNOWN, info={}, error=f"{type(e).__name__}: {e}")
    finally:
        _state["at"] = time.monotonic()
        _polling = False
    return _state["status"]

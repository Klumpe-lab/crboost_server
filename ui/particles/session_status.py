"""Curation-session liveness, shared by the Journey and the Species page (roadmap 11-S4).

`backend.find_active_curation_session_any` shells out to `squeue`, so this must not be
polled per surface: one process-wide cached flag is refreshed at most every `POLL_S`, and
every caller reads the cache. Callers `poll()` on whatever cadence suits them — the throttle
here decides when a `squeue` actually happens, so adding a second observer costs nothing.
Since picking-UI 09-S2 there is one observer, the Particles registry's Picks & curation tab:
the Journey no longer starts or swaps a session, so it neither shows liveness nor pays for it.

Three states, not two. A `squeue` that RAISES must not read as "no session running" — that
would tell the user to start a second ChimeraX while one is up. It reports `unknown` and
the surfaces say so (never-fail-silently).
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

POLL_S = 16.0  # the Journey's cadence (every 4th 4-s tick), now the shared throttle

LIVE = "live"
OFF = "off"
UNKNOWN = "unknown"  # never polled yet, or the last poll raised

_state: dict = {"status": UNKNOWN, "at": 0.0, "error": ""}
_polling = False  # module-level re-entry guard (SingleFlight needs an owning object)


def status() -> str:
    """`live` | `off` | `unknown` — the cached answer, no I/O. Safe in a signature()."""
    return _state["status"]


def is_live() -> bool:
    """True only when a session is KNOWN to be running (`unknown` is not `live`)."""
    return _state["status"] == LIVE


def last_error() -> str:
    """Why the last poll could not answer, for the tooltip of an `unknown` chip."""
    return _state["error"]


def is_stale() -> bool:
    """True when the cached answer is older than `POLL_S` — the next `poll()` will hit
    `squeue`."""
    return time.monotonic() - _state["at"] >= POLL_S


async def poll(backend, *, force: bool = False) -> str:
    """Refresh the cached status if it is older than `POLL_S` (or `force`), and return it.
    Cheap and safe to call on any tick: within the window it returns the cache without
    touching the cluster, and a concurrent call is a no-op rather than a second `squeue`."""
    global _polling
    if backend is None:
        return _state["status"]
    if _polling or (not force and not is_stale()):
        return _state["status"]
    _polling = True
    try:
        live = bool(await backend.find_active_curation_session_any())
        _state["status"] = LIVE if live else OFF
        _state["error"] = ""
    except Exception as e:
        # Reported, not swallowed, and NOT downgraded to "off": the surfaces show
        # `unknown` with this text rather than inviting a duplicate ChimeraX launch.
        logger.exception("Curation-session poll failed")
        _state["status"] = UNKNOWN
        _state["error"] = f"{type(e).__name__}: {e}"
    finally:
        _state["at"] = time.monotonic()
        _polling = False
    return _state["status"]

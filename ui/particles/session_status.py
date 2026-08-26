"""Curation-session liveness, shared by the Journey and the Species page (roadmap 11-S4).

`backend.find_active_curation_session_any` shells out to `squeue`, so this must not be
polled per surface: one process-wide cached flag is refreshed at most every `POLL_S`, and
every caller reads the cache. Callers `poll()` on whatever cadence suits them — the throttle
here decides when a `squeue` actually happens, so adding a second observer costs nothing.
Two observers: the Particles registry's Picks & curation tab, and the always-present
control-session icon at the bottom of the primary sidebar. Both read this one cache, so the
second costs no extra `squeue`.

Three states, not two. A `squeue` that RAISES must not read as "no session running" — that
would tell the user to start a second ChimeraX while one is up. It reports `unknown` and
the surfaces say so (never-fail-silently).

The poll also caches the live session's declared SCOPE (its `scope.json`: species + tomogram
it was launched on), because the sidebar icon's whole job in the active state is to answer
"what is being picked right now" on hover. Empty for `off`/`unknown`, and empty as well for a
session launched before scopes were recorded — stated as unknown, never guessed at.
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

POLL_S = 16.0  # the Journey's cadence (every 4th 4-s tick), now the shared throttle

LIVE = "live"
OFF = "off"
UNKNOWN = "unknown"  # never polled yet, or the last poll raised

_state: dict = {"status": UNKNOWN, "at": 0.0, "error": "", "scope": {}}
_polling = False  # module-level re-entry guard (SingleFlight needs an owning object)


def status() -> str:
    """`live` | `off` | `unknown` — the cached answer, no I/O. Safe in a signature()."""
    return _state["status"]


def scope() -> dict:
    """The live session's declared scope (`species_id`, `species_label`, `tomo_name`,
    `project_path`, `curation_dir`), or `{}` when nothing is running / the session recorded
    none. No I/O — safe in a signature()."""
    return _state["scope"]


def scope_text() -> str:
    """'<species> · <tomogram>' for the live session, "" when there is no scope to name."""
    sc = _state["scope"]
    species = str(sc.get("species_label") or sc.get("species_id") or "")
    tomo = str(sc.get("tomo_name") or "")
    return " · ".join(p for p in (species, tomo) if p)


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
        info = await backend.find_active_curation_session_any()
        _state["status"] = LIVE if info else OFF
        _state["scope"] = dict((info or {}).get("scope") or {})
        _state["error"] = ""
    except Exception as e:
        # Reported, not swallowed, and NOT downgraded to "off": the surfaces show
        # `unknown` with this text rather than inviting a duplicate ChimeraX launch.
        logger.exception("Curation-session poll failed")
        _state["status"] = UNKNOWN
        _state["scope"] = {}
        _state["error"] = f"{type(e).__name__}: {e}"
    finally:
        _state["at"] = time.monotonic()
        _polling = False
    return _state["status"]

# services/result.py
"""One idiom for service-level outcomes (roadmap 03).

Every service/backend method that reports an outcome builds it here — never a
hand-built ``{"success": ...}`` literal. The result stays a plain dict (JSON-safe,
NiceGUI-safe, zero migration cliff); payload keys stay ad-hoc. The only hard
contract: ``success`` is always present, failures always carry ``error`` (human
text), and ``code`` appears only for the few flows where a caller branches on the
cause. UI consumption rule: render ``result["error"]``; branch on
``result.get("code")`` against :class:`ErrorCode`, never on message text.

If typed payloads ever earn their keep, these two functions are the single seam
where a ``ServiceResult`` model (``extra="allow"``) can be introduced without
touching call sites.
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any


class ErrorCode(StrEnum):
    """Stable machine-readable failure causes — the values are the wire format.

    Add a member ONLY when a caller genuinely branches on the cause; everything
    else is text-only via ``err(message)``.
    """

    # Stage-0 census (03-stage0-census.md): the repo has exactly ONE cause-branching
    # consumer today — tomo_dashboard_dialog's import-picks fallback dialog. Candidates
    # that did NOT earn a code: "No jobs selected." (nobody branches; prose is enough)
    # and curation "nothing open" (that's success with count=0, not an error).
    NO_COORDS_FOUND = auto()


def ok(**data: Any) -> dict[str, Any]:
    """Successful outcome; payload keys stay ad-hoc by design."""
    return {"success": True, **data}


def err(message: str, *, code: ErrorCode | None = None, **data: Any) -> dict[str, Any]:
    """Failed outcome. ``error`` is always the human-facing text — never None."""
    return {"success": False, "error": message, **({"code": code} if code else {}), **data}

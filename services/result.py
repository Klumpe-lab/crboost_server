# services/result.py
"""One idiom for service-level outcomes.

Every service/backend method that reports an outcome builds it here, not as a
hand-built ``{"success": ...}`` literal. The result is a plain dict (JSON-safe,
NiceGUI-safe); payload keys are ad-hoc. The only hard
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
    """Stable machine-readable failure causes; the values are the wire format.

    Add a member only when a caller branches on the cause; everything else is
    text-only via ``err(message)``.
    """

    # Branched on by tomo_dashboard_dialog's import-picks fallback dialog.
    NO_COORDS_FOUND = auto()

    # Resolving `/p/<name>` branches three ways: open it, render a chooser over the
    # candidate paths, or send the user back to the landing page.
    PROJECT_NOT_FOUND = auto()
    PROJECT_AMBIGUOUS = auto()


def ok(**data: Any) -> dict[str, Any]:
    """Successful outcome; payload keys stay ad-hoc by design."""
    return {"success": True, **data}


def err(message: str, *, code: ErrorCode | None = None, **data: Any) -> dict[str, Any]:
    """Failed outcome. ``error`` is always the human-facing text — never None."""
    return {"success": False, "error": message, **({"code": code} if code else {}), **data}

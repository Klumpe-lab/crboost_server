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

    # A job's readout was asked for before the job wrote its outputs: shown as a hint, not an error.
    NOT_WRITTEN_YET = auto()

    # Run refused by the checks of a job's particle-set input: the pipeline panel lists them and redraws the job.
    SET_CHECKS = auto()

    # A call needs a decision only its caller can make (a value nobody gave, one of several options): the
    # result's `needs` names each one (`need`), and the call refuses until they are given.
    NEEDS_DECISION = auto()

    # A write to a job that is queued, running or finished: its settings stay as they ran or will run.
    JOB_FROZEN = auto()

    # The agent API changes only the projects it made; one made by hand is changed in the UI.
    NOT_WRITABLE = auto()


def ok(**data: Any) -> dict[str, Any]:
    """Successful outcome; payload keys stay ad-hoc by design."""
    return {"success": True, **data}


def err(message: str, *, code: ErrorCode | None = None, **data: Any) -> dict[str, Any]:
    """Failed outcome. ``error`` is always the human-facing text — never None."""
    return {"success": False, "error": message, **({"code": code} if code else {}), **data}


def need(kind: str, what: str, why: str, *, options: list | None = None, **data: Any) -> dict[str, Any]:
    """One open decision, for an ``err(..., code=ErrorCode.NEEDS_DECISION, needs=[...])``. ``kind``: "value" (a
    value nobody gave), "choice" (one of ``options``) or "pin" (which source an input reads). ``what`` names the
    field or input, ``why`` says what it is for; extra keys describe the options further."""
    return {"kind": kind, "what": what, "why": why, **({"options": options} if options is not None else {}), **data}

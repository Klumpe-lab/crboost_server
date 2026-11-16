"""Change plans: what an agent proposes to do, shown to the user on one page and approved there once.

An agent (the agent API) sends every write of a change at once (`plan_changes`); `api/plans.py` checks each
step and rehearses those it can on a copy of the project, and stores the plan here. The page
(`ui/agent_plan_page.py`) shows it per project; Approve on that page is the only way a plan becomes approved,
and only an approved plan is carried out (`apply_changes`). Plans live in this server's memory: a restart
forgets them, approvals included.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any

PLAN_TTL = timedelta(hours=1)
# Plans kept for their page and get_plan after they ended, newest first.
KEEP_ENDED = 50


class PlanStatus(StrEnum):
    PROPOSED = "proposed"
    APPROVED = "approved"
    REJECTED = "rejected"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    EXPIRED = "expired"


class StepState(StrEnum):
    PENDING = "pending"
    DONE = "done"
    FAILED = "failed"
    NOT_RUN = "not run"


@dataclass
class PlanStep:
    index: int
    verb: str
    args: dict[str, Any]
    # The project folder the step changes (resolved); None for one that names none.
    project: str | None
    # One line for the user: what the step does.
    what: str
    # Rehearsed on a copy of the project (its effect is in the project's diff), or only described.
    simulated: bool
    # Permanent: deletes for good (the page repeats these beside Approve).
    permanent: bool = False
    state: StepState = StepState.PENDING
    result: dict[str, Any] | None = None


@dataclass
class RosterChip:
    instance_id: str
    name: str
    status: str
    # kept | added | removed (off the roster) | deleted (to Trash) | cloned
    mark: str = "kept"
    clone_of: str | None = None


@dataclass
class Change:
    job: str
    # param | slurm | input
    kind: str
    name: str
    before: Any
    after: Any


@dataclass
class Deletion:
    what: str
    size_bytes: int | None
    permanent: bool
    orphans: list[str] = field(default_factory=list)


@dataclass
class ProjectPlan:
    path: str
    name: str
    # Made by this plan's create_project step: nothing to compare with yet.
    new: bool = False
    # A project made by hand (not by the API): the agent edits the user's own work.
    hand_made: bool = False
    roster: list[RosterChip] = field(default_factory=list)
    changes: list[Change] = field(default_factory=list)
    deletions: list[Deletion] = field(default_factory=list)
    # Steps described but not rehearsed (species, templates, imports, the tilt filter's mode, ...).
    other: list[str] = field(default_factory=list)
    # plan_run on the rehearsed copy, when the plan runs this project.
    run: dict[str, Any] | None = None
    # create_project's values, for a project the plan makes.
    creates: dict[str, Any] | None = None
    # What the plan was proposed against; apply refuses when the project moved since.
    fingerprint: str = ""

    def summary(self) -> str:
        """One line: what the plan does to this project."""
        if self.new:
            parts = ["new project"]
        else:
            parts = []
        added = sum(1 for c in self.roster if c.mark in ("added", "cloned"))
        removed = sum(1 for c in self.roster if c.mark == "removed")
        if added:
            parts.append(f"+{added} job{'s' * (added != 1)}")
        if removed:
            parts.append(f"−{removed} job{'s' * (removed != 1)}")
        for kind, label in (("param", "parameter"), ("slurm", "SLURM value"), ("input", "input")):
            n = sum(1 for c in self.changes if c.kind == kind)
            if n:
                parts.append(f"{n} {label}{'s' * (n != 1)}")
        trash = [d for d in self.deletions if not d.permanent]
        gone = [d for d in self.deletions if d.permanent]
        if trash:
            parts.append(f"delete {len(trash)} to Trash ({total_size(trash)})")
        if gone:
            parts.append(f"DELETE FOR GOOD: {len(gone)} ({total_size(gone)})")
        if self.other:
            parts.append(f"{len(self.other)} other change{'s' * (len(self.other) != 1)}")
        if self.run is not None:
            n = len(self.run.get("jobs") or [])
            parts.append(f"run {n} job{'s' * (n != 1)}")
            if self.run.get("refusals"):
                parts.append("RUN REFUSED")
        return f"{self.name} · " + (", ".join(parts) or "no change")


@dataclass
class Plan:
    id: str
    title: str
    steps: list[PlanStep]
    projects: dict[str, ProjectPlan]
    created_at: datetime = field(default_factory=datetime.now)
    status: PlanStatus = PlanStatus.PROPOSED
    decided_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    task_id: str | None = None

    @property
    def expires_at(self) -> datetime:
        return self.created_at + PLAN_TTL

    @property
    def permanent_steps(self) -> list[PlanStep]:
        return [s for s in self.steps if s.permanent]

    def refresh_expiry(self, now: datetime | None = None) -> None:
        if self.status in (PlanStatus.PROPOSED, PlanStatus.APPROVED) and (now or datetime.now()) > self.expires_at:
            self.status = PlanStatus.EXPIRED

    def lines(self) -> list[str]:
        return [p.summary() for p in self.projects.values()]


def total_size(deletions: list[Deletion]) -> str:
    """Their sizes added up; "size not measured" when one is unknown, rather than a total that looks whole."""
    if any(d.size_bytes is None for d in deletions):
        known = sum(d.size_bytes or 0 for d in deletions)
        return f"at least {format_bytes(known)}, not all measured" if known else "size not measured"
    return format_bytes(sum(d.size_bytes or 0 for d in deletions))


def format_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1000 or unit == "TB":
            return f"{size:.0f} {unit}" if unit in ("B", "KB") else f"{size:.1f} {unit}"
        size /= 1000
    return f"{n} B"


class PlanStore:
    def __init__(self) -> None:
        self._plans: dict[str, Plan] = {}

    def add(self, title: str, steps: list[PlanStep], projects: dict[str, ProjectPlan]) -> Plan:
        plan = Plan(id="p-" + secrets.token_hex(4), title=title, steps=steps, projects=projects)
        self._plans[plan.id] = plan
        self._prune()
        return plan

    def get(self, plan_id: str) -> Plan | None:
        plan = self._plans.get(plan_id)
        if plan is not None:
            plan.refresh_expiry()
        return plan

    def decide(self, plan_id: str, approve: bool) -> Plan | None:
        """The user's answer on the plan's page. Only a proposed plan can be decided."""
        plan = self.get(plan_id)
        if plan is None or plan.status != PlanStatus.PROPOSED:
            return plan
        plan.status = PlanStatus.APPROVED if approve else PlanStatus.REJECTED
        plan.decided_at = datetime.now()
        return plan

    def _prune(self) -> None:
        for plan in self._plans.values():
            plan.refresh_expiry()
        ended = sorted(
            (p for p in self._plans.values() if p.status not in (PlanStatus.PROPOSED, PlanStatus.APPROVED)),
            key=lambda p: p.created_at,
            reverse=True,
        )
        for plan in ended[KEEP_ENDED:]:
            del self._plans[plan.id]


_store: PlanStore | None = None


def get_plan_store() -> PlanStore:
    global _store
    if _store is None:
        _store = PlanStore()
    return _store

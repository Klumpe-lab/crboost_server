"""Change plans: what an agent proposes to do, shown to the user in words and confirmed once.

An agent (the agent API) sends every write of a change at once (`plan_changes`); `api/plans.py` checks each
step, rehearses those it can on a copy of each project (a blank one for a project the plan makes), and stores
the plan here, as the user reads it: per project its jobs with their fields by title (now → after), deletions,
values, the run, and the steps in sentences. The terminal shows it as markdown (`services/agent_plan_view.py`)
and the user confirms through Claude Code's prompt (`apply_changes_now`); the page (`ui/agent_plan_page.py`)
shows it as an editor, and its Approve is the other way a plan becomes approved. An edit on the page rewrites
the plan's steps and rehearses it again (`PlanStore.revise`). Plans live in this server's memory: a restart
forgets them, approvals included.
"""

from __future__ import annotations

import secrets
from collections.abc import Awaitable, Callable
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
    # The arguments as the agent sent them (a revision rewrites them).
    args: dict[str, Any]
    # The project folder the step changes (resolved); None for one that names none.
    project: str | None
    # The step in a sentence, for the user.
    what: str
    # Rehearsed on a copy of the project (its effect is in the project's jobs), or only described.
    simulated: bool = False
    # Deletes for good: Approve turns red and the terminal path refuses it.
    permanent: bool = False
    # Why a step of a project the plan makes could not be previewed (it runs all the same).
    note: str | None = None
    state: StepState = StepState.PENDING
    result: dict[str, Any] | None = None


@dataclass
class FieldView:
    """One field of a job as its tab shows it, before and after the plan."""

    name: str
    label: str
    # param | slurm | input
    kind: str
    before: Any
    after: Any
    # int | float | bool | choice | text | list (a list is shown, not edited)
    type: str = "text"
    choices: list[Any] | None = None
    bounds: dict[str, float] | None = None
    help: str | None = None
    editable: bool = False

    @property
    def changed(self) -> bool:
        return self.before != self.after


@dataclass
class JobView:
    instance_id: str
    name: str
    status: str
    # kept | added | cloned | removed (off the roster) | deleted (to Trash)
    mark: str = "kept"
    clone_of: str | None = None
    species: str | None = None
    # Has run or runs now: its settings are not changed by the plan.
    frozen: bool = False
    fields: list[FieldView] = field(default_factory=list)

    @property
    def changes(self) -> list[FieldView]:
        return [f for f in self.fields if f.changed]

    @property
    def touched(self) -> bool:
        return self.mark != "kept" or bool(self.changes)


@dataclass
class Deletion:
    what: str
    size_bytes: int | None
    permanent: bool
    # The plan step that deletes it: the page's keep switch drops that step.
    step: int = 0
    orphans: list[str] = field(default_factory=list)


@dataclass
class ProjectPlan:
    path: str
    name: str
    # Made by this plan's create_project step.
    new: bool = False
    # A project made by hand (not by the API): the agent edits the user's own work.
    hand_made: bool = False
    # Jobs in roster order after the plan, with the ones it takes off where they stood.
    jobs: list[JobView] = field(default_factory=list)
    deletions: list[Deletion] = field(default_factory=list)
    # Steps described, not rehearsed (imports, templates, the tilt filter's mode, ...), as sentences.
    also: list[str] = field(default_factory=list)
    # plan_run on the rehearsed copy, when the plan runs this project.
    run: dict[str, Any] | None = None
    # A new project's values, as (label, value) in the order the landing page shows them.
    values: list[tuple[str, str]] = field(default_factory=list)
    # What the plan was proposed against; apply refuses when the project moved since.
    fingerprint: str = ""

    @property
    def changes(self) -> list[tuple[JobView, FieldView]]:
        return [(j, f) for j in self.jobs for f in j.changes]

    def summary(self) -> str:
        """One line: what the plan does to this project."""
        parts = ["new project"] if self.new else []
        added = sum(1 for j in self.jobs if j.mark in ("added", "cloned"))
        off = sum(1 for j in self.jobs if j.mark == "removed")
        if added:
            parts.append(f"+{added} job{'s' * (added != 1)}")
        if off:
            parts.append(f"−{off} job{'s' * (off != 1)}")
        n = len(self.changes)
        if n:
            parts.append(f"{n} setting{'s' * (n != 1)}")
        trash = [d for d in self.deletions if not d.permanent]
        gone = [d for d in self.deletions if d.permanent]
        if trash:
            parts.append(f"{len(trash)} to Trash ({total_size(trash)})")
        if gone:
            parts.append(f"DELETED FOR GOOD: {len(gone)} ({total_size(gone)})")
        if self.also:
            parts.append(f"{len(self.also)} more step{'s' * (len(self.also) != 1)}")
        if self.run is not None:
            n_run = len(self.run.get("jobs") or [])
            parts.append(f"run {n_run} job{'s' * (n_run != 1)}" + (" (REFUSED)" if self.run.get("refusals") else ""))
        return f"{self.name}: " + (", ".join(parts) or "no change")


@dataclass
class Plan:
    id: str
    title: str
    steps: list[PlanStep]
    projects: dict[str, ProjectPlan]
    created_at: datetime = field(default_factory=datetime.now)
    # Moves with every edit on the page; an edit sends an approved plan back to proposed.
    revision: int = 1
    status: PlanStatus = PlanStatus.PROPOSED
    decided_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    task_id: str | None = None
    # The terminal dialog apply_changes_now opened through the MCP forwarder: its one-time token and the revision
    # it shows. The forwarder sends the token back on the user's Accept.
    confirmation: tuple[str, int] | None = None

    @property
    def expires_at(self) -> datetime:
        return self.created_at + PLAN_TTL

    @property
    def permanent_steps(self) -> list[PlanStep]:
        return [s for s in self.steps if s.permanent]

    @property
    def editable(self) -> bool:
        return self.status in (PlanStatus.PROPOSED, PlanStatus.APPROVED)

    def refresh_expiry(self, now: datetime | None = None) -> None:
        if self.editable and (now or datetime.now()) > self.expires_at:
            self.status = PlanStatus.EXPIRED

    def lines(self) -> list[str]:
        return [p.summary() for p in self.projects.values()]

    def raw_steps(self) -> list[dict[str, Any]]:
        return [{"verb": s.verb, "args": dict(s.args)} for s in self.steps]


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


# ── Edits from the page ───────────────────────────────────────────────────────

_SETTING_VERB = {"param": "set_params", "slurm": "set_slurm"}


def coerce(f: FieldView, raw: Any) -> Any:
    """A value typed in for a field (the page's input, the terminal dialog's answer) as the field's type.
    Raises ValueError or TypeError for one that is not."""
    if raw is None or raw == "":
        return None
    if f.type == "int":
        return round(float(raw))
    if f.type == "float":
        return float(raw)
    if f.type == "bool":
        if isinstance(raw, str):
            return raw.strip().lower() in ("true", "on", "yes", "1")
        return bool(raw)
    return raw


Edit = tuple[str, str, FieldView, Any]  # (project, job, field, value)


def _with_value(pairs: list[tuple[dict[str, Any], str | None]], edit: Edit) -> list[tuple[dict[str, Any], str | None]]:
    project, job, f, value = edit
    verb = _SETTING_VERB[f.kind]
    pairs = [({"verb": s["verb"], "args": dict(s["args"])}, p) for s, p in pairs]
    owner = None
    for i, (s, p) in enumerate(pairs):
        if s["verb"] == verb and p == project and s["args"].get("job") == job:
            owner = i
    back_to_now = value == f.before
    if owner is not None:
        values = dict(pairs[owner][0]["args"].get("values") or {})
        if back_to_now:
            values.pop(f.name, None)
        else:
            values[f.name] = value
        if values:
            pairs[owner][0]["args"]["values"] = values
        else:
            del pairs[owner]
        return pairs
    if back_to_now:
        return pairs
    at = len(pairs)
    for i, (s, p) in enumerate(pairs):
        if p == project and s["verb"] != "run":
            at = i + 1
    pairs.insert(at, ({"verb": verb, "args": {"project": project, "job": job, "values": {f.name: value}}}, project))
    return pairs


def steps_with_edits(plan: Plan, edits: list[Edit], drop: set[int] = frozenset()) -> list[dict[str, Any]]:
    """The plan's steps with fields set and deletion steps dropped (by index). A field is merged into the last
    step that sets that kind of field on that job, else a new step after the project's last step that is not its
    run; set back to what the job has now, it leaves the plan, and a step left with nothing to set goes too."""
    pairs = [
        (raw, step.project) for raw, step in zip(plan.raw_steps(), plan.steps, strict=True) if step.index not in drop
    ]
    for edit in edits:
        pairs = _with_value(pairs, edit)
    return [s for s, _ in pairs]


def steps_with_value(plan: Plan, project: str, job: str, f: FieldView, value: Any) -> list[dict[str, Any]]:
    """The plan's steps with one field set to `value` (an edit on the page)."""
    return steps_with_edits(plan, [(project, job, f, value)])


def steps_without(plan: Plan, index: int) -> list[dict[str, Any]]:
    """The plan's steps without the one numbered `index` (a deletion the user keeps)."""
    return [s for s, step in zip(plan.raw_steps(), plan.steps, strict=True) if step.index != index]


# ── The store ─────────────────────────────────────────────────────────────────

# Rehearses a plan's steps again: (title, steps) -> ok(steps, projects) | err. Set by the agent API at startup
# (`api/plans.py`), so an edit on the page goes through the same checks as a proposal.
Rebuilder = Callable[[str, list[dict[str, Any]]], Awaitable[dict[str, Any]]]


class PlanStore:
    def __init__(self) -> None:
        self._plans: dict[str, Plan] = {}
        self.rebuilder: Rebuilder | None = None

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

    async def revise(self, plan_id: str, steps: list[dict[str, Any]]) -> dict[str, Any]:
        """The plan with new steps (an edit on its page), rehearsed again. On success the plan keeps its id, its
        revision moves, it is proposed again (an approval was for the plan as it stood) and has a fresh hour.
        The rehearsal's refusal comes back as it is, the plan unchanged."""
        plan = self.get(plan_id)
        if plan is None or not plan.editable:
            return {"success": False, "error": "This plan can no longer be changed."}
        if self.rebuilder is None:
            return {"success": False, "error": "The agent API is not running in this server."}
        if not steps:
            return {"success": False, "error": "A plan needs at least one step; reject it instead."}
        res = await self.rebuilder(plan.title, steps)
        if not res.get("success"):
            return res
        plan.steps = res["steps"]
        plan.projects = res["projects"]
        plan.revision += 1
        plan.status = PlanStatus.PROPOSED
        plan.decided_at = None
        plan.created_at = datetime.now()
        return res

    def _prune(self) -> None:
        for plan in self._plans.values():
            plan.refresh_expiry()
        ended = sorted((p for p in self._plans.values() if not p.editable), key=lambda p: p.created_at, reverse=True)
        for plan in ended[KEEP_ENDED:]:
            del self._plans[plan.id]


_store: PlanStore | None = None


def get_plan_store() -> PlanStore:
    global _store
    if _store is None:
        _store = PlanStore()
    return _store

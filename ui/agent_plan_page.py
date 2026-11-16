"""The page of an agent's change plan (`/agent/plan/<id>`): everything the plan will do, per project, on one
scrollable page, and the one Approve the plan waits for (`services/agent_plans.py`).

Per project: the roster after the plan (added, cloned, taken off, deleted), the parameter, SLURM and input
changes as now → after, the deletions with their sizes and whether they are for good, the steps only described,
and what the run will submit. Approve here is the only way a plan becomes approved; the agent then applies it,
and the page follows the steps.
"""

from __future__ import annotations

import json
from typing import Any

from nicegui import ui

from services.agent_plans import Plan, PlanStatus, ProjectPlan, format_bytes, get_plan_store
from ui.components.buttons import house_button
from ui.components.reactive import FingerprintedView, owned_timer
from ui.job_plugins._field_styles import (
    CLR_LABEL,
    CLR_SUBLABEL,
    CLR_VALUE,
    HELPER_STYLE,
    LABEL_VOICE,
    PANEL_HEAD_STYLE,
    PANEL_STYLE,
    PANEL_TITLE_STYLE,
)
from ui.styles import MONO, SANS

_RED = "#b91c1c"
_GREEN = "#15803d"
_AMBER = "#b45309"

# Roster chip looks by mark: (ink, fill, ring, struck through).
_CHIP = {
    "kept": ("#334155", "#f8fafc", "#dbe3ec", False),
    "added": (_GREEN, "#f0fdf4", "#bbf7d0", False),
    "cloned": (_GREEN, "#f0fdf4", "#bbf7d0", False),
    "removed": ("#94a3b8", "#f8fafc", "#e2e8f0", True),
    "deleted": (_RED, "#fef2f2", "#fecaca", True),
}
_STATUS_INK = {
    PlanStatus.PROPOSED: "#1f4e79",
    PlanStatus.APPROVED: _GREEN,
    PlanStatus.RUNNING: "#1f4e79",
    PlanStatus.DONE: _GREEN,
    PlanStatus.REJECTED: CLR_LABEL,
    PlanStatus.FAILED: _RED,
    PlanStatus.EXPIRED: CLR_LABEL,
}
_STEP_INK = {"pending": CLR_SUBLABEL, "done": _GREEN, "failed": _RED, "not run": CLR_SUBLABEL}

_CELL = f"{MONO} font-size: 10px; color: {CLR_VALUE}; padding: 2px 8px 2px 0; vertical-align: top;"
_HEAD_CELL = f"{SANS} font-size: 9px; color: {CLR_LABEL}; padding: 0 8px 3px 0; text-align: left; font-weight: 500;"
_SUB = f"{SANS} font-size: 10px; font-weight: 600; color: {CLR_LABEL}; margin-top: 8px;"


def _value(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=str)
    return str(v)


def _badge(text: str, ink: str, fill: str = "transparent", ring: str | None = None) -> None:
    ui.label(text).style(
        f"{SANS} font-size: 9px; font-weight: 600; color: {ink}; background: {fill}; "
        f"border: 1px solid {ring or ink}; border-radius: 3px; padding: 0 5px; line-height: 15px; flex-shrink: 0;"
    )


def _roster(pp: ProjectPlan) -> None:
    with ui.element("div").style("display: flex; flex-wrap: wrap; gap: 4px; margin-top: 6px;"):
        for c in pp.roster:
            ink, fill, ring, struck = _CHIP.get(c.mark, _CHIP["kept"])
            text = c.name + (f" ← {c.clone_of}" if c.clone_of else "")
            tip = f"{c.instance_id} · {c.status or 'new'} · {c.mark}"
            ui.label(text).style(
                f"{SANS} font-size: 10px; color: {ink}; background: {fill}; border: 1px solid {ring}; "
                f"border-radius: 3px; padding: 1px 6px; text-decoration: {'line-through' if struck else 'none'};"
            ).tooltip(tip)


def _table(head: list[str], rows: list[list[str]], inks: list[str | None] | None = None) -> None:
    with ui.element("table").style("border-collapse: collapse; margin-top: 3px;"):
        with ui.element("tr"):
            for h in head:
                with ui.element("th").style(_HEAD_CELL):
                    ui.label(h)
        for i, row in enumerate(rows):
            ink = (inks or [None] * len(rows))[i]
            with ui.element("tr"):
                for cell in row:
                    with ui.element("td").style(_CELL + (f" color: {ink};" if ink else "")):
                        ui.label(cell).style("white-space: pre-wrap; word-break: break-word;")


def _project_section(pp: ProjectPlan) -> None:
    with ui.column().classes("w-full").style(f"{PANEL_STYLE} gap: 2px;"):
        with ui.row().classes("items-center no-wrap").style(f"{PANEL_HEAD_STYLE} gap: 6px;"):
            ui.label(pp.name).style(PANEL_TITLE_STYLE)
            if pp.new:
                _badge("new project", _GREEN)
            elif pp.hand_made:
                _badge("your project, made by hand", _AMBER)
            ui.label(pp.path).style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; margin-left: auto;")
        ui.label(pp.summary()).style(f"{HELPER_STYLE} margin-top: 4px;")

        if pp.creates:
            ui.label("Made with").style(_SUB)
            _table(["value", "given"], [[k, _value(v)] for k, v in sorted(pp.creates.items())])

        if pp.roster:
            ui.label("Roster after the plan").style(_SUB)
            _roster(pp)

        if pp.changes:
            ui.label("Settings").style(_SUB)
            _table(
                ["job", "kind", "name", "now", "after"],
                [[c.job, c.kind, c.name, _value(c.before), _value(c.after)] for c in pp.changes],
            )

        if pp.deletions:
            ui.label("Deletions").style(_SUB)
            _table(
                ["what", "size", "kind", "leaves without input"],
                [
                    [
                        d.what,
                        format_bytes(d.size_bytes) if d.size_bytes is not None else "not measured",
                        "FOR GOOD" if d.permanent else "to Trash",
                        ", ".join(d.orphans) or "—",
                    ]
                    for d in pp.deletions
                ],
                [(_RED if d.permanent else _AMBER) for d in pp.deletions],
            )

        if pp.other:
            ui.label("Also (not rehearsed: shown as asked)").style(_SUB)
            for line in pp.other:
                ui.label(line).style(f"{MONO} font-size: 10px; color: {CLR_VALUE};")

        if pp.run is not None:
            jobs = pp.run.get("jobs") or []
            held = pp.run.get("held_by_review")
            title = f"Run: {len(jobs)} job{'s' * (len(jobs) != 1)} submitted"
            ui.label(title + (f"; waits for the {held} review" if held else "")).style(_SUB)
            rows = []
            for j in jobs:
                r = j.get("resources") or {}
                rows.append(
                    [
                        j["instance_id"] + ("  (waits for review)" if j.get("waits_for_review") else ""),
                        _value(r.get("partition")),
                        _value(r.get("gres")),
                        _value(r.get("cpus_per_task")),
                        _value(r.get("mem")),
                        _value(r.get("time")),
                    ]
                )
            if rows:
                _table(["job", "partition", "gres", "cpus", "mem", "time"], rows)
            leaves = pp.run.get("leaves") or {}
            if leaves.get("live"):
                ui.label(f"Left running: {', '.join(leaves['live'])}").style(HELPER_STYLE)
            for refusal in pp.run.get("refusals") or []:
                ui.label(f"Run refused: {refusal}").style(f"{SANS} font-size: 10px; color: {_RED};")


class _PlanView(FingerprintedView):
    def __init__(self, container, plan_id: str) -> None:
        super().__init__(container)
        self.plan_id = plan_id

    def signature(self):
        plan = get_plan_store().get(self.plan_id)
        if plan is None:
            return None
        return (plan.status, plan.error, tuple(s.state for s in plan.steps))

    def render(self) -> None:
        plan = get_plan_store().get(self.plan_id)
        if plan is None:
            ui.label(f"No plan {self.plan_id}. Plans live in the server's memory: a restart forgets them.").style(
                LABEL_VOICE
            )
            return
        self._header(plan)
        for pp in plan.projects.values():
            _project_section(pp)
        self._steps(plan)
        self._footer(plan)

    def _header(self, plan: Plan) -> None:
        with ui.row().classes("w-full items-center no-wrap").style("gap: 8px;"):
            ui.label(plan.title).style(f"{SANS} font-size: 15px; font-weight: 600; color: #12324f;")
            _badge(plan.status.value, _STATUS_INK[plan.status])
        ui.label(
            f"Proposed by an agent through the CryoBoost API at {plan.created_at:%H:%M}; "
            f"{len(plan.steps)} steps over {len(plan.projects)} project{'s' * (len(plan.projects) != 1)}. "
            + (
                f"Waiting for your approval until {plan.expires_at:%H:%M}. Nothing has changed yet."
                if plan.status == PlanStatus.PROPOSED
                else ""
            )
        ).style(HELPER_STYLE)
        if plan.error:
            ui.label(plan.error).style(f"{SANS} font-size: 11px; color: {_RED};")

    def _steps(self, plan: Plan) -> None:
        with ui.column().classes("w-full").style(f"{PANEL_STYLE} gap: 2px;"):
            ui.label("Steps, in the order they run").style(f"{PANEL_TITLE_STYLE} {PANEL_HEAD_STYLE}")
            for s in plan.steps:
                with ui.row().classes("items-baseline no-wrap").style("gap: 8px; margin-top: 3px;"):
                    ui.label(f"{s.index:>2}").style(f"{MONO} font-size: 10px; color: {CLR_SUBLABEL};")
                    ui.label(s.state.value).style(
                        f"{SANS} font-size: 9px; color: {_STEP_INK[s.state.value]}; width: 44px; flex-shrink: 0;"
                    )
                    ui.label(s.what).style(
                        f"{MONO} font-size: 10px; color: {_RED if s.permanent else CLR_VALUE}; word-break: break-all;"
                    )
                    if s.result is not None and not s.result.get("success"):
                        ui.label(str(s.result.get("error"))).style(f"{SANS} font-size: 10px; color: {_RED};")

    def _footer(self, plan: Plan) -> None:
        if plan.status != PlanStatus.PROPOSED:
            return
        permanent = plan.permanent_steps
        with ui.row().classes("w-full items-center justify-end no-wrap").style("gap: 8px; padding: 4px 0 24px;"):
            if permanent:
                ui.label("Deletes for good: " + "; ".join(s.what for s in permanent)).style(
                    f"{SANS} font-size: 10px; color: {_RED}; margin-right: auto;"
                )
            house_button("Reject", lambda: self._decide(False))
            house_button(
                "Approve",
                lambda: self._decide(True),
                kind="danger" if permanent else "accent",
                tooltip="The agent then carries the plan out, step by step",
            )

    def _decide(self, approve: bool) -> None:
        plan = get_plan_store().decide(self.plan_id, approve)
        if plan is not None and plan.status == PlanStatus.APPROVED:
            ui.notify("Approved: the agent can apply the plan now.", type="positive")
        self.refresh()


def render_agent_plan_page(plan_id: str) -> None:
    with ui.column().classes("w-full items-center bg-gray-50").style("min-height: 100vh; padding: 18px 16px;"):
        container = ui.column().classes("w-full").style("max-width: 1100px; gap: 12px;")
        view = _PlanView(container, plan_id)
        view.refresh()
        owned_timer(3.0, view.refresh, container)

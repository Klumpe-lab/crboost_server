"""The page of an agent's change plan (`/agent/plan/<id>`): the plan as an editor.

Laid out as the app's job views: on the left each project of the plan with its roster after the plan (added,
cloned, taken off, to Trash), on the right what is selected. A project's overview shows its values (a new
project), deletions with a keep switch, the run, and its steps in words; a job shows its parameters and SLURM
settings as its tab does, a changed one with the value it has now struck beside it, and editable while the job
has not run. An edit rewrites the plan's steps and rehearses it again (`PlanStore.revise`); a plan edited after
Approve is proposed again. Approve here is the page's way to approve; the agent then applies the plan, and the
page follows its steps.

An edit lands when the field is left (or on Enter, or at once for a switch or a choice), and the page does not
rebuild under it: the view adopts the edited state, and the struck "now" values catch up at the next selection.
"""

from __future__ import annotations

from typing import Any

from nicegui import ui

from services.agent_plans import (
    FieldView,
    JobView,
    Plan,
    PlanStatus,
    ProjectPlan,
    coerce,
    format_bytes,
    get_plan_store,
    steps_with_value,
    steps_without,
)
from services.agent_plan_view import show
from ui.components.buttons import house_button
from ui.components.fields import house_number, house_select, house_text
from ui.components.reactive import FingerprintedView, SingleFlight, owned_timer
from ui.job_plugins._field_styles import (
    CLR_LABEL,
    CLR_SUBLABEL,
    CLR_VALUE,
    HELPER_STYLE,
    LABEL_STYLE,
    LABEL_VOICE,
    PANEL_STYLE,
    ROW_STYLE,
    section_head,
)
from ui.styles import MONO, SANS

_RED = "#b91c1c"
_GREEN = "#15803d"
_AMBER = "#b45309"
_ACCENT = "#1f4e79"

# Rail marks: (badge text, ink, row tint).
_MARK = {
    "kept": ("", CLR_VALUE, "transparent"),
    "added": ("new", _GREEN, "#f0fdf4"),
    "cloned": ("clone", _GREEN, "#f0fdf4"),
    "removed": ("off", CLR_SUBLABEL, "#f8fafc"),
    "deleted": ("Trash", _RED, "#fef2f2"),
}
_DOT = {"Succeeded": "#22c55e", "Failed": "#ef4444", "Running": "#0ea5e9", "Queued": "#94a3b8"}
_STATUS_INK = {
    PlanStatus.PROPOSED: _ACCENT,
    PlanStatus.APPROVED: _GREEN,
    PlanStatus.RUNNING: _ACCENT,
    PlanStatus.DONE: _GREEN,
    PlanStatus.REJECTED: CLR_LABEL,
    PlanStatus.FAILED: _RED,
    PlanStatus.EXPIRED: CLR_LABEL,
}
_STEP_INK = {"pending": CLR_SUBLABEL, "done": _GREEN, "failed": _RED, "not run": CLR_SUBLABEL}
_SECTION = f"{SANS} font-size: 10px; font-weight: 600; color: {CLR_LABEL}; margin: 10px 0 3px;"
_CELL = f"{MONO} font-size: 10px; color: {CLR_VALUE}; padding: 2px 10px 2px 0; vertical-align: top;"
_HEAD_CELL = f"{SANS} font-size: 9px; color: {CLR_LABEL}; padding: 0 10px 3px 0; text-align: left; font-weight: 500;"


def _badge(text: str, ink: str) -> None:
    ui.label(text).style(
        f"{SANS} font-size: 9px; font-weight: 600; color: {ink}; border: 1px solid {ink}; border-radius: 3px; "
        "padding: 0 5px; line-height: 14px; flex-shrink: 0;"
    )


def _table(head: list[str], rows: list[list[str]], inks: list[str | None] | None = None) -> None:
    with ui.element("table").style("border-collapse: collapse;"):
        with ui.element("tr"):
            for h in head:
                with ui.element("th").style(_HEAD_CELL):
                    ui.label(h)
        for i, row in enumerate(rows):
            ink = inks[i] if inks else None
            with ui.element("tr"):
                for cell in row:
                    with ui.element("td").style(_CELL + (f" color: {ink};" if ink else "")):
                        ui.label(cell).style("white-space: pre-wrap; word-break: break-word;")


class _PlanView(FingerprintedView):
    def __init__(self, container, plan_id: str) -> None:
        super().__init__(container)
        self.plan_id = plan_id
        # (project path, job instance id); a None job is the project's overview.
        self.selected: tuple[str, str | None] | None = None
        self.flight = SingleFlight()
        self._edited_note = None

    def signature(self):
        plan = get_plan_store().get(self.plan_id)
        if plan is None:
            return None
        return (plan.status, plan.revision, plan.error, tuple(s.state for s in plan.steps))

    # ── Layout ────────────────────────────────────────────────────────────

    def render(self) -> None:
        plan = get_plan_store().get(self.plan_id)
        if plan is None:
            ui.label(f"No plan {self.plan_id}. Plans live in the server's memory: a restart forgets them.").style(
                LABEL_VOICE
            )
            return
        if self.selected is None or self.selected[0] not in plan.projects:
            self.selected = self._first_selection(plan)
        self._header(plan)
        with ui.row().classes("w-full no-wrap items-start").style("gap: 14px;"):
            with ui.column().style(
                "width: 290px; flex-shrink: 0; position: sticky; top: 12px; max-height: calc(100vh - 24px); "
                "overflow-y: auto; gap: 10px;"
            ):
                for pp in plan.projects.values():
                    self._rail_project(plan, pp)
            with ui.column().style("flex: 1; min-width: 0; gap: 12px;"):
                path, iid = self.selected
                pp = plan.projects[path]
                job = next((j for j in pp.jobs if j.instance_id == iid), None) if iid else None
                if job is None:
                    self._project_pane(plan, pp)
                else:
                    self._job_pane(plan, pp, job)
        self._footer(plan)

    @staticmethod
    def _first_selection(plan: Plan) -> tuple[str, str | None]:
        for path, pp in plan.projects.items():
            if pp.deletions or pp.run or pp.values:
                return path, None
            touched = next((j for j in pp.jobs if j.touched), None)
            if touched is not None:
                return path, touched.instance_id
        return next(iter(plan.projects)), None

    def _select(self, path: str, iid: str | None) -> None:
        self.selected = (path, iid)
        self.mark_dirty()
        self.refresh()

    def _header(self, plan: Plan) -> None:
        with ui.row().classes("w-full items-center no-wrap").style("gap: 8px;"):
            ui.label(plan.title).style(f"{SANS} font-size: 15px; font-weight: 600; color: #12324f;")
            _badge(plan.status.value, _STATUS_INK[plan.status])
            if plan.revision > 1:
                ui.label(f"revision {plan.revision}").style(HELPER_STYLE)
            self._edited_note = ui.label("").style(f"{HELPER_STYLE} color: {_ACCENT};")
        if plan.editable:
            text = (
                f"Proposed by an agent through the CryoBoost API; nothing has changed yet. Edit what you want "
                f"changed, then approve, before {plan.expires_at:%H:%M}."
                if plan.status == PlanStatus.PROPOSED
                else "Approved: the agent carries it out next. An edit sends it back for approval."
            )
        else:
            text = f"{len(plan.steps)} steps, {plan.status.value}."
        ui.label(text).style(HELPER_STYLE)
        if plan.error:
            ui.label(plan.error).style(f"{SANS} font-size: 11px; color: {_RED};")

    # ── Rail ──────────────────────────────────────────────────────────────

    def _rail_project(self, plan: Plan, pp: ProjectPlan) -> None:
        with ui.column().classes("w-full").style(f"{PANEL_STYLE} gap: 1px;"):
            with section_head(pp.name, tooltip=pp.path):
                if pp.new:
                    _badge("new", _GREEN)
                elif pp.hand_made:
                    _badge("yours", _AMBER).tooltip("Made by hand: the agent is changing your own project")
            self._rail_row(plan, pp, None, "Overview", "", CLR_VALUE, "transparent", None)
            for j in pp.jobs:
                badge, ink, tint = _MARK.get(j.mark, _MARK["kept"])
                if j.mark == "kept" and j.changes:
                    badge, ink = f"{len(j.changes)} set", _ACCENT
                self._rail_row(plan, pp, j, j.name, badge, ink, tint, _DOT.get(j.status, "#cbd5e1"))

    def _rail_row(self, plan, pp, job: JobView | None, text, badge, ink, tint, dot) -> None:
        iid = job.instance_id if job else None
        on = self.selected == (pp.path, iid)
        struck = job is not None and job.mark in ("removed", "deleted")
        row = (
            ui.row()
            .classes("w-full items-center no-wrap cursor-pointer")
            .style(f"gap: 6px; padding: 2px 6px; border-radius: 3px; background: {'#e6eef7' if on else tint};")
        )
        with row:
            if dot is not None:
                ui.element("div").style(
                    f"width: 6px; height: 6px; border-radius: 50%; background: {dot}; flex-shrink: 0;"
                )
            ui.label(text).style(
                f"{SANS} font-size: 10px; color: {ink}; flex: 1; min-width: 0; overflow: hidden; "
                f"text-overflow: ellipsis; white-space: nowrap; font-weight: {600 if on or iid is None else 400}; "
                f"text-decoration: {'line-through' if struck else 'none'};"
            )
            if badge:
                ui.label(badge).style(f"{SANS} font-size: 9px; color: {ink}; flex-shrink: 0;")
        row.mark(f"rail:{iid or 'overview'}")
        row.on("click", lambda p=pp.path, i=iid: self._select(p, i))

    # ── Project overview ──────────────────────────────────────────────────

    def _project_pane(self, plan: Plan, pp: ProjectPlan) -> None:
        with ui.column().classes("w-full").style(f"{PANEL_STYLE} gap: 2px;"):
            section_head(pp.name)
            ui.label(pp.summary().split(": ", 1)[-1]).style(f"{HELPER_STYLE} margin-top: 6px;")

            if pp.values:
                ui.label("Values").style(_SECTION)
                for label, value in pp.values:
                    with ui.element("div").style(ROW_STYLE):
                        ui.label(label).style(LABEL_STYLE)
                        ui.label(value).style(f"{MONO} font-size: 11px; color: {CLR_VALUE};")

            changes = pp.changes
            if changes:
                ui.label("Settings").style(_SECTION)
                _table(
                    ["Job", "Setting", "Now", "After"],
                    [
                        [j.name, f.label + (" (SLURM)" if f.kind == "slurm" else ""), show(f.before), show(f.after)]
                        for j, f in changes
                    ],
                )

            if pp.deletions:
                ui.label("Deletions").style(_SECTION)
                for d in pp.deletions:
                    self._deletion_row(plan, d)

            if pp.run is not None:
                self._run(pp)

            if pp.also:
                ui.label("Also (shown as asked, not previewed)").style(_SECTION)
                for line in pp.also:
                    ui.label(line).style(f"{SANS} font-size: 10px; color: {CLR_VALUE};")

            steps = [s for s in plan.steps if s.project == pp.path]
            if steps:
                ui.label("Steps, in the order they run").style(_SECTION)
                for s in steps:
                    with ui.row().classes("items-baseline no-wrap").style("gap: 8px;"):
                        ui.label(f"{s.index}").style(f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; width: 14px;")
                        ui.label(s.state.value).style(
                            f"{SANS} font-size: 9px; color: {_STEP_INK[s.state.value]}; width: 40px; flex-shrink: 0;"
                        )
                        ui.label(s.what).style(f"{SANS} font-size: 10px; color: {_RED if s.permanent else CLR_VALUE};")
                        if s.note:
                            ui.label(f"(not previewed: {s.note})").style(HELPER_STYLE)
                        if s.result is not None and not s.result.get("success"):
                            ui.label(str(s.result.get("error"))).style(f"{SANS} font-size: 10px; color: {_RED};")

    def _deletion_row(self, plan: Plan, d) -> None:
        size = format_bytes(d.size_bytes) if d.size_bytes is not None else "size not measured"
        ink = _RED if d.permanent else _AMBER
        with ui.row().classes("w-full items-center no-wrap").style("gap: 8px;"):
            if plan.editable:
                ui.switch(value=True, on_change=lambda e, i=d.step: self._keep(e.value, i)).props("dense").style(
                    "transform: scale(0.7);"
                ).tooltip("Off: keep it (the step leaves the plan)")
            ui.label(d.what).style(f"{SANS} font-size: 10px; color: {CLR_VALUE}; flex: 1;")
            ui.label(size).style(f"{MONO} font-size: 10px; color: {CLR_LABEL};")
            _badge("for good" if d.permanent else "to Trash", ink)
        if d.orphans:
            ui.label("Leaves without input: " + ", ".join(d.orphans)).style(f"{HELPER_STYLE} margin-left: 34px;")

    def _run(self, pp: ProjectPlan) -> None:
        jobs = pp.run.get("jobs") or []
        held = pp.run.get("held_by_review")
        title = f"Run: {len(jobs)} job{'s' * (len(jobs) != 1)} submitted" + (
            f", held for the {held} review" if held else ""
        )
        ui.label(title).style(_SECTION)
        if jobs:
            rows = []
            for j in jobs:
                r = j.get("resources") or {}
                rows.append(
                    [
                        j["instance_id"] + ("  (waits for review)" if j.get("waits_for_review") else ""),
                        *(show(r.get(k)) for k in ("partition", "gres", "cpus_per_task", "mem", "time")),
                    ]
                )
            _table(["Job", "Partition", "GPUs", "CPUs", "Memory", "Time"], rows)
        for refusal in pp.run.get("refusals") or []:
            ui.label(f"Run refused: {refusal}").style(f"{SANS} font-size: 10px; color: {_RED};")

    # ── A job, as its tab ─────────────────────────────────────────────────

    def _job_pane(self, plan: Plan, pp: ProjectPlan, job: JobView) -> None:
        badge, ink, _ = _MARK.get(job.mark, _MARK["kept"])
        with ui.column().classes("w-full").style(f"{PANEL_STYLE} gap: 2px;"):
            with section_head(job.name, right=job.status):
                if badge:
                    _badge({"new": "added by the plan", "clone": "added by the plan"}.get(badge, badge), ink)
            if job.clone_of:
                ui.label(f"A clone of {job.clone_of}: it starts from that job's settings.").style(
                    f"{HELPER_STYLE} margin-top: 6px;"
                )
            if job.mark in ("removed", "deleted"):
                ui.label(
                    "The plan takes this job off the roster"
                    + (" and moves its folder to Trash." if job.mark == "deleted" else ".")
                ).style(f"{HELPER_STYLE} margin-top: 6px;")
                return
            if job.frozen:
                ui.label("This job has run: its settings stay as they are (clone it to run it again).").style(
                    f"{HELPER_STYLE} margin-top: 6px;"
                )
            editable = plan.editable and not job.frozen
            for kind, title in (("param", "Parameters"), ("slurm", "SLURM"), ("input", "Inputs")):
                fields = [f for f in job.fields if f.kind == kind]
                if not fields:
                    continue
                ui.label(title).style(_SECTION)
                for f in fields:
                    self._field_row(plan, pp, job, f, editable and f.editable)

    def _field_row(self, plan: Plan, pp: ProjectPlan, job: JobView, f: FieldView, editable: bool) -> None:
        hint = f.help
        if f.bounds:
            limits = ", ".join(f"{k} {v:g}" for k, v in f.bounds.items())
            hint = f"{hint} ({limits})" if hint else limits
        with ui.element("div").style(f"{ROW_STYLE} align-items: center; min-height: 21px;"):
            lbl = ui.label(f.label).style(LABEL_STYLE + (f" color: {_ACCENT}; font-weight: 600;" if f.changed else ""))
            if hint:
                lbl.tooltip(hint)
            if editable:
                self._editor(plan, pp, job, f)
            else:
                ui.label(show(f.after)).style(f"{MONO} font-size: 11px; color: {CLR_VALUE};")
            if f.changed:
                ui.label(show(f.before)).style(
                    f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; text-decoration: line-through;"
                ).tooltip("What the job has now" if job.mark == "kept" else "As the job is added")

    def _editor(self, plan: Plan, pp: ProjectPlan, job: JobView, f: FieldView) -> None:
        commit = lambda value: self._commit(pp.path, job.instance_id, f, value)  # noqa: E731
        if f.type == "bool":
            ui.switch(value=bool(f.after), on_change=lambda e: commit(e.value)).props("dense").style(
                "transform: scale(0.7);"
            )
        elif f.type == "choice" and f.choices:
            options = list(f.choices)
            if f.after not in options:
                options.append(f.after)
            house_select("", options, value=f.after, width="w-48", on_change=lambda e: commit(e.value))
        elif f.type in ("int", "float"):
            kw: dict[str, Any] = {"value": f.after, "precision": 0 if f.type == "int" else None}
            if f.bounds:
                if "ge" in f.bounds:
                    kw["min"] = f.bounds["ge"]
                if "le" in f.bounds:
                    kw["max"] = f.bounds["le"]
            el = house_number("", width="w-28", **kw)
            el.on("blur", lambda: commit(el.value))
            el.on("keydown.enter", lambda: commit(el.value))
        else:
            el = house_text("", value="" if f.after is None else str(f.after), width="w-48")
            el.on("blur", lambda: commit(el.value))
            el.on("keydown.enter", lambda: commit(el.value))

    # ── Edits ─────────────────────────────────────────────────────────────

    async def _commit(self, path: str, iid: str, f: FieldView, raw: Any) -> None:
        try:
            value = coerce(f, raw)
        except (TypeError, ValueError):
            ui.notify(f"{f.label}: not a {f.type}", type="warning")
            return
        if value == f.after:
            return
        async with self.flight("edit") as acquired:
            if not acquired:
                return
            plan = get_plan_store().get(self.plan_id)
            if plan is None or not plan.editable:
                return
            res = await get_plan_store().revise(self.plan_id, steps_with_value(plan, path, iid, f, value))
            if not res.get("success"):
                ui.notify(res.get("error") or "Not changed", type="warning", timeout=8000)
                self.mark_dirty()
                self.refresh()
                return
            # The field already shows the new value: take the plan as painted, so the page does not rebuild under
            # the user; the rest catches up at the next selection.
            f.after = value
            self.adopt()
            if self._edited_note is not None and not self._edited_note.is_deleted:
                self._edited_note.set_text(f"edited: revision {plan.revision}, to be approved again")

    async def _keep(self, delete: bool, step: int) -> None:
        if delete:
            return
        async with self.flight("edit") as acquired:
            if not acquired:
                return
            plan = get_plan_store().get(self.plan_id)
            if plan is None or not plan.editable:
                return
            res = await get_plan_store().revise(self.plan_id, steps_without(plan, step))
            if not res.get("success"):
                ui.notify(res.get("error") or "Not changed", type="warning", timeout=8000)
            self.mark_dirty()
            self.refresh()

    # ── Footer ────────────────────────────────────────────────────────────

    def _footer(self, plan: Plan) -> None:
        if plan.status != PlanStatus.PROPOSED:
            return
        permanent = plan.permanent_steps
        with ui.row().classes("w-full items-center justify-end no-wrap").style("gap: 8px; padding: 6px 0 24px;"):
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
        self.mark_dirty()
        self.refresh()


def render_agent_plan_page(plan_id: str) -> None:
    with ui.column().classes("w-full items-center bg-gray-50").style("min-height: 100vh; padding: 18px 16px;"):
        container = ui.column().classes("w-full").style("max-width: 1180px; gap: 12px;")
        view = _PlanView(container, plan_id)
        view.refresh()
        owned_timer(3.0, view.refresh, container)

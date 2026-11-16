"""Change plans over the API: `plan_changes`, `get_plan`, `apply_changes`.

A plan is every write of a change, sent at once. Proposing changes nothing: each step's input is checked and
its project passed through the gate (`api/reach.py`), the steps that only edit a project's state (the roster,
parameters, SLURM, inputs, a job's deletion, the run's plan) are rehearsed in order on a copy of the project,
the others are described with what can be read now (sizes, orphans), and the plan is stored
(`services/agent_plans.py`) for its page. The user approves it there; `apply_changes` then runs the steps in
order through the same dispatch as single calls, the gate checked again per step, and stops at the first that
fails. Nothing is rolled back: each step stands on its own, as a click in the UI does.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from api.context import ApiContext, open_project
from api.handlers import Call, _plain
from api.reach import write_refusal
from services import job_params, pipeline_edit
from services.agent_plans import (
    Change,
    Deletion,
    PlanStatus,
    PlanStep,
    ProjectPlan,
    RosterChip,
    StepState,
    get_plan_store,
)
from services.background_tasks import get_background_task_registry
from services.jobs.spec import instance_display_name
from services.models_base import JobType
from services.path_resolution_service import consumers_of
from services.project_disk_usage import read_cache
from services.project_state import ProjectState, get_project_state_for
from services.project_trash import list_trash
from services.result import ErrorCode, err, ok

logger = logging.getLogger(__name__)

# Deletes for good: Approve turns red and names them.
PERMANENT = {"clear_trash", "delete_project", "delete_species", "delete_template", "delete_mask"}
# Never a plan step: plans do not nest.
NOT_A_STEP = {"apply_changes"}


# ── Rehearsal ─────────────────────────────────────────────────────────────────


def _rehearse_delete_job(state: ProjectState, iid: str) -> dict[str, Any]:
    """What `pipeline_edit.delete_job` does to the state, without the move to Trash."""
    notice = pipeline_edit.in_use_notice(state, iid)
    if notice:
        return err(notice)
    jm = state.jobs.get(iid)
    if jm is not None and jm.relion_job_name:
        state.jobs.pop(iid, None)
        state.job_path_mapping.pop(iid, None)
        if iid in state.pipeline_order:
            return pipeline_edit.remove_job(state, iid)
        return ok(instance_id=iid)
    return pipeline_edit.remove_job(state, iid)


def _rehearse(state: ProjectState, verb: str, a) -> dict[str, Any] | None:
    """The step done on the copy, as its door does it; None for a verb not rehearsed."""
    if verb == "add_job":
        try:
            job_type = JobType(a.job_type)
        except ValueError:
            return err(f"No job type {a.job_type!r} (list_job_types lists them).")
        return pipeline_edit.add_job(
            state, job_type, species_id=a.species, after=a.after, reading=a.reading, clone_of=a.clone_of
        )
    if verb == "remove_job":
        return pipeline_edit.remove_job(state, a.job)
    if verb == "delete_job":
        return _rehearse_delete_job(state, a.job)
    if verb == "set_params":
        return job_params.set_params(state, a.job, a.values)
    if verb == "set_slurm":
        return job_params.set_slurm(state, a.job, a.values, reset=a.reset)
    if verb == "set_input":
        return job_params.set_input(state, a.job, a.slot, source=a.source, path=a.path, auto=a.auto, facet=a.facet)
    if verb == "reset_inputs":
        return job_params.reset_inputs(state, a.job)
    return None


def _job_settings(jm) -> dict[str, dict[str, Any]]:
    return {
        "param": {n: _plain(getattr(jm, n, None)) for n in sorted(jm.USER_PARAMS)},
        "slurm": _plain(dict(jm.slurm_overrides)),
        "input": _plain(dict(jm.source_overrides)),
    }


def fingerprint(state: ProjectState) -> str:
    """What a plan was proposed against: the roster and every job's settings, the species and their files.
    Statuses are left out, so a run going on does not make a plan stale; a step refused by its own door over a
    status (a job that started meanwhile) is refused when it runs."""
    data = {
        "roster": state.roster(),
        "jobs": {iid: _job_settings(jm) for iid, jm in sorted(state.jobs.items())},
        "species": [
            (s.id, [t.id for t in s.templates], [m.id for m in s.masks], s.selected_template_id, s.selected_mask_id)
            for s in state.species_registry
        ],
    }
    return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()


def _diff(before: dict[str, dict], after: dict[str, dict], iid: str) -> list[Change]:
    out: list[Change] = []
    for kind in ("param", "slurm", "input"):
        b, a = before.get(kind, {}), after.get(kind, {})
        for name in sorted(set(b) | set(a)):
            if b.get(name) != a.get(name):
                out.append(Change(job=iid, kind=kind, name=name, before=b.get(name), after=a.get(name)))
    return out


def _roster(before: ProjectState, after: ProjectState, deleted: set[str], clones: dict[str, str]) -> list[RosterChip]:
    """The roster after the plan, with the jobs it takes off left where they stood."""

    def chip(state: ProjectState, iid: str, mark: str) -> RosterChip:
        jm = state.jobs.get(iid)
        name = instance_display_name(iid, jm) if jm is not None else iid
        status = jm.execution_status.value if jm is not None else ""
        return RosterChip(iid, name, status, mark, clones.get(iid))

    old = before.roster()
    new = after.roster()
    chips = [chip(after, iid, "kept" if iid in old else ("cloned" if iid in clones else "added")) for iid in new]
    for i, iid in enumerate(old):
        if iid in new:
            continue
        mark = "deleted" if iid in deleted else "removed"
        prev = next((p for p in reversed(old[:i]) if any(c.instance_id == p for c in chips)), None)
        at = 0 if prev is None else next(k for k, c in enumerate(chips) if c.instance_id == prev) + 1
        chips.insert(at, chip(before, iid, mark))
    return chips


def _sizes(project: Path) -> tuple[dict[str, int], int | None]:
    usage = read_cache(project)
    if usage is None:
        return {}, None
    return dict(usage.jobs or {}), usage.total_bytes


def _trash_sizes(project: Path) -> dict[str, int]:
    return {e.name: e.size_bytes for e in list_trash(project)}


def _what(verb: str, args: dict[str, Any]) -> str:
    shown = {k: v for k, v in args.items() if k != "project" and v not in (None, [], {}, False)}
    return verb + (" " + json.dumps(shown, default=str, sort_keys=True) if shown else "")


# ── Proposing ─────────────────────────────────────────────────────────────────


def plan_url(ctx: ApiContext, plan_id: str) -> str:
    return f"http://{ctx.host}:{ctx.port}/agent/plan/{plan_id}"


async def plan_changes(call: Call) -> dict[str, Any]:
    ctx = call.ctx
    raw_steps = call.args.steps
    steps: list[PlanStep] = []
    typed: list[Any] = []
    # Projects a create_project step makes: later steps may name them by name or path.
    made: dict[str, Path] = {}
    projects: dict[str, ProjectPlan] = {}

    for i, raw in enumerate(raw_steps, start=1):
        verb = ctx.verbs.get(raw.verb)
        if verb is None or verb.kind != "write" or raw.verb in NOT_A_STEP:
            return err(f"Step {i}: {raw.verb!r} is not a write verb a plan can hold.", step=i)
        try:
            a = verb.model.model_validate(raw.args)
        except ValueError as e:
            return err(f"Step {i} ({raw.verb}): bad input: {e}", step=i)
        typed.append(a)
        target: Path | None = None
        if raw.verb == "create_project" and getattr(a, "name", None):
            base = getattr(a, "base", None) or await ctx.backend.get_default_project_base()
            target = Path(base).expanduser() / a.name.strip()
            made[a.name.strip()] = target
            made[str(target)] = target
            projects[str(target)] = ProjectPlan(
                path=str(target),
                name=target.name,
                new=True,
                creates={k: _plain(v) for k, v in a.model_dump().items() if v not in (None, [], {})},
            )
        elif verb.names_project:
            if a.project in made:
                target = made[a.project]
            else:
                target, why = await open_project(a.project)
                if why is not None:
                    return err(f"Step {i} ({raw.verb}): {why['error']}", step=i)
                key = str(target)
                if key not in projects:
                    refusal = await write_refusal(get_project_state_for(target), target)
                    if refusal:
                        return err(
                            f"Step {i} ({raw.verb}): {refusal}",
                            step=i,
                            code=ErrorCode.NOT_WRITABLE,
                            ui_url=ctx.ui_url(target),
                        )
                    state = get_project_state_for(target)
                    projects[key] = ProjectPlan(
                        path=key, name=target.name, hand_made=not state.made_by_api, fingerprint=fingerprint(state)
                    )
        steps.append(
            PlanStep(
                index=i,
                verb=raw.verb,
                args=raw.args,
                project=str(target) if target is not None else None,
                what=_what(raw.verb, raw.args),
                simulated=False,
                permanent=raw.verb in PERMANENT,
            )
        )

    needs: list[dict[str, Any]] = []
    for key, pp in projects.items():
        if pp.new:
            pp.other = [s.what for s in steps if s.project == key and s.verb != "create_project"]
            continue
        problem = await _rehearse_project(call, Path(key), pp, steps, typed, needs)
        if problem is not None:
            return problem

    if needs:
        return err(
            "The plan has open decisions: put them to the user, then propose again with the answers.",
            code=ErrorCode.NEEDS_DECISION,
            needs=needs,
        )

    plan = get_plan_store().add(call.args.title, steps, projects)
    url = plan_url(ctx, plan.id)
    return ok(
        plan_id=plan.id,
        ui_url=url,
        projects=plan.lines(),
        steps=len(steps),
        permanent=[s.what for s in plan.permanent_steps],
        expires_at=plan.expires_at.isoformat(timespec="minutes"),
        message=(
            "Nothing has changed yet. Show the user the project lines and the link; they review and approve the "
            "plan on that page. Then apply_changes(plan_id)."
        ),
    )


async def _rehearse_project(
    call: Call, path: Path, pp: ProjectPlan, steps: list[PlanStep], typed: list[Any], needs: list[dict]
) -> dict[str, Any] | None:
    """Rehearse this project's steps on a copy; fill `pp`. An err to return when a step is refused."""
    live = get_project_state_for(path)
    before = await asyncio.to_thread(live.detached_copy)
    copy = await asyncio.to_thread(live.detached_copy)
    baseline: dict[str, dict] = {iid: _job_settings(jm) for iid, jm in before.jobs.items()}
    deleted: set[str] = set()
    clones: dict[str, str] = {}
    sizes, total = await asyncio.to_thread(_sizes, path)

    for step, a in zip(steps, typed, strict=True):
        if step.project != str(path):
            continue
        if step.verb == "run":
            step.simulated = True
            pp.run = await call.backend.pipeline_orchestrator.plan_run(path, state=copy)
            needs.extend(pp.run.get("needs") or [])
            continue
        if step.verb == "delete_job":
            jm = copy.jobs.get(a.job)
            job_dir = jm.relion_job_name.rstrip("/") if jm is not None and jm.relion_job_name else None
            if job_dir is not None:
                pp.deletions.append(
                    Deletion(
                        what=f"{instance_display_name(a.job, jm)} ({job_dir}) to Trash",
                        size_bytes=sizes.get(job_dir),
                        permanent=False,
                        orphans=consumers_of(copy, a.job),
                    )
                )
                deleted.add(a.job)
        res = _rehearse(copy, step.verb, a)
        if res is None:
            pp.other.append(step.what)
            await _describe_deletion(path, step, a, copy, pp, total)
            continue
        step.simulated = True
        if not res["success"]:
            return err(f"Step {step.index} ({step.verb}) on {path.name}: {res['error']}", step=step.index)
        needs.extend(res.get("needs") or [])
        if step.verb == "add_job":
            for iid in res.get("added") or [res.get("instance_id")]:
                if iid and iid in copy.jobs and iid not in baseline:
                    baseline[iid] = _job_settings(copy.jobs[iid])
            if a.clone_of and res.get("instance_id"):
                clones[res["instance_id"]] = a.clone_of

    pp.roster = _roster(before, copy, deleted, clones)
    for iid in copy.roster():
        if iid in copy.jobs and iid in baseline:
            pp.changes.extend(_diff(baseline[iid], _job_settings(copy.jobs[iid]), iid))
    return None


async def _describe_deletion(path: Path, step: PlanStep, a, state: ProjectState, pp: ProjectPlan, total) -> None:
    """The deletions that are not rehearsed, with their sizes where they are known."""
    if step.verb == "delete_project":
        pp.deletions.append(Deletion(what=f"the whole project {path.name}", size_bytes=total, permanent=True))
    elif step.verb == "clear_trash":
        trash = await asyncio.to_thread(_trash_sizes, path)
        names = a.names if a.names is not None else sorted(trash)
        size = sum(trash.get(n, 0) for n in names)
        pp.deletions.append(Deletion(what=f"Trash: {', '.join(names) or 'nothing'}", size_bytes=size, permanent=True))
    elif step.verb == "delete_species":
        refs = state.species_references(a.species)
        sp = state.get_species(a.species)
        n_files = len(sp.templates) + len(sp.masks) if sp is not None else 0
        pp.deletions.append(
            Deletion(
                what=f"species {a.species}: its {n_files} template and mask files, and its jobs to Trash",
                size_bytes=None,
                permanent=True,
                orphans=list(refs.get("jobs", [])),
            )
        )
    elif step.verb in ("delete_template", "delete_mask"):
        kind = "template" if step.verb == "delete_template" else "mask"
        file_id = a.template_id if kind == "template" else a.mask_id
        pp.deletions.append(
            Deletion(what=f"{kind} {file_id} of {a.species} (file and sidecar)", size_bytes=None, permanent=True)
        )


# ── Reading and applying ──────────────────────────────────────────────────────


def _step_row(s: PlanStep) -> dict[str, Any]:
    row: dict[str, Any] = {"step": s.index, "what": s.what, "state": s.state.value}
    if s.result is not None and not s.result.get("success"):
        row["error"] = s.result.get("error")
    return row


async def get_plan(call: Call) -> dict[str, Any]:
    plan = get_plan_store().get(call.args.plan_id)
    if plan is None:
        return err(f"No plan {call.args.plan_id} (plans live in the server's memory: a restart forgets them).")
    return ok(
        plan_id=plan.id,
        title=plan.title,
        status=plan.status.value,
        projects=plan.lines(),
        steps=[_step_row(s) for s in plan.steps],
        error=plan.error,
        task_id=plan.task_id,
        ui_url=plan_url(call.ctx, plan.id),
    )


async def _wait_for_task(ctx: ApiContext, task_id: str) -> dict[str, Any]:
    registry = get_background_task_registry()
    while True:
        record = registry.get(task_id)
        if record is None or not record.is_running:
            break
        await asyncio.sleep(1.0)
    result = ctx.results.get(task_id)
    if result is not None:
        return result
    if record is not None and record.status == "succeeded":
        return ok(message=record.result_message or "")
    return err(record.error if record is not None and record.error else f"Background task {task_id} did not finish.")


async def apply_changes(call: Call) -> dict[str, Any]:
    ctx = call.ctx
    plan = get_plan_store().get(call.args.plan_id)
    url = None if plan is None else plan_url(ctx, plan.id)
    if plan is None:
        return err(f"No plan {call.args.plan_id} (plans live in the server's memory: a restart forgets them).")
    if plan.status == PlanStatus.PROPOSED:
        return err(f"The user has not approved {plan.id} yet: it is approved on its page.", ui_url=url)
    if plan.status != PlanStatus.APPROVED:
        return err(f"{plan.id} is {plan.status.value}: nothing to apply. Propose the change again.", ui_url=url)
    for key, pp in plan.projects.items():
        if pp.new:
            continue
        if fingerprint(get_project_state_for(Path(key))) != pp.fingerprint:
            plan.status = PlanStatus.FAILED
            plan.error = f"{pp.name} changed after the plan was proposed."
            return err(f"{pp.name} changed after the plan was proposed: propose the change again.", ui_url=url)

    plan.status = PlanStatus.RUNNING

    async def work(progress_cb) -> str:
        total = len(plan.steps)
        for n, step in enumerate(plan.steps):
            progress_cb(n, total, step.what)
            verb = ctx.verbs[step.verb]
            res = await ctx.run_verb(verb, verb.model.model_validate(step.args), plan_id=plan.id)
            if res.get("success") and res.get("task_id"):
                res = await _wait_for_task(ctx, res["task_id"])
            step.result = res
            if not res.get("success"):
                step.state = StepState.FAILED
                for rest in plan.steps[n + 1 :]:
                    rest.state = StepState.NOT_RUN
                plan.status = PlanStatus.FAILED
                plan.error = f"Step {step.index} ({step.verb}): {res.get('error')}"
                plan.finished_at = datetime.now()
                raise RuntimeError(plan.error)
            step.state = StepState.DONE
        plan.status = PlanStatus.DONE
        plan.finished_at = datetime.now()
        progress_cb(total, total, "done")
        return f"{plan.title}: {total} steps done"

    plan.task_id = get_background_task_registry().submit(
        work, title=f"Agent plan: {plan.title}", dedup_key=f"plan:{plan.id}"
    )
    return ok(
        task_id=plan.task_id,
        plan_id=plan.id,
        ui_url=url,
        message="Applying the plan step by step in the background: follow it with get_plan or get_task.",
    )

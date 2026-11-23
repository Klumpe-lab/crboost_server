"""A change plan as the crboost pane in Claude Code draws it (`agent/claude_plugin`): per project one table, a row per
job the plan touches and a column per setting it changes, each cell the value after the plan beside what the job has
now, and whether and how it is edited (type, choices, bounds, help). Built from the same `Plan` the page and the
terminal overview read, so the three never disagree; the pane only draws it and sends edits back (`edit_plan`).

Edits come back as text (what the person typed or picked) and are read here as the field's type: a choice by the
label the table gave it, so a number or an enum value round-trips through the pane unchanged.
"""

from __future__ import annotations

from typing import Any

from services.agent_plan_view import show
from services.agent_plans import Deletion, Edit, FieldView, JobView, Plan, ProjectPlan, coerce, format_bytes

_BOUND = {"ge": "≥", "gt": ">", "le": "≤", "lt": "<"}
# The settings an edit may change: a job's input is shown, never edited (the page edits none either).
EDITABLE_KINDS = ("param", "slurm")
_BOOL_WORDS = ("true", "on", "yes", "1", "false", "off", "no", "0")


def _bounds(f: FieldView) -> str:
    if not f.bounds:
        return ""
    return ", ".join(f"{_BOUND.get(k, k)} {v:g}" for k, v in f.bounds.items())


def column_id(f: FieldView) -> str:
    return f"{f.kind}:{f.name}"


def _cell(f: FieldView, job: JobView, editable: bool) -> dict[str, Any]:
    return {
        "now": show(f.before),
        "after": show(f.after),
        "changed": f.changed,
        "editable": editable and f.editable and f.kind in EDITABLE_KINDS,
        "type": f.type,
        "choices": [show(c) for c in f.choices] if f.choices else None,
        "bounds": _bounds(f),
        "help": f.help or "",
        "label": f.label,
        # What an edit starts from: the value after the plan as the person types it.
        "value": "" if f.after is None else show(f.after),
    }


def _what(j: JobView) -> str:
    """What the plan does to the job, in a few words."""
    if j.mark == "added":
        return "added"
    if j.mark == "cloned":
        return f"clone of {j.clone_of}"
    if j.mark == "removed":
        return "off the roster"
    if j.mark == "deleted":
        return "to Trash"
    n = len(j.changes)
    return f"{n} setting{'s' * (n != 1)}" if n else "unchanged"


def _deletion(d: Deletion) -> dict[str, Any]:
    return {
        "what": d.what,
        "size": format_bytes(d.size_bytes) if d.size_bytes is not None else "size not measured",
        "permanent": d.permanent,
        "step": d.step,
        "orphans": list(d.orphans),
    }


def _run(run: dict[str, Any] | None) -> dict[str, Any] | None:
    if run is None:
        return None
    jobs = []
    for job in run.get("jobs") or []:
        r = job.get("resources") or {}
        jobs.append(
            {
                "job": job["instance_id"],
                "partition": show(r.get("partition")),
                "gpus": show(r.get("gres")),
                "cpus": show(r.get("cpus_per_task")),
                "mem": show(r.get("mem")),
                "time": show(r.get("time")),
                "waits_for_review": bool(job.get("waits_for_review")),
            }
        )
    return {
        "jobs": jobs,
        "held_by_review": run.get("held_by_review"),
        "refusals": list(run.get("refusals") or []),
        "left_running": list((run.get("leaves") or {}).get("live") or []),
    }


def project_table(pp: ProjectPlan, editable: bool) -> dict[str, Any]:
    """One project: its rows (the jobs the plan touches, in roster order) and columns (every setting the plan
    changes in it, parameters first, then SLURM, then inputs, each where it first appears)."""
    touched = [j for j in pp.jobs if j.touched]
    order = {"param": 0, "slurm": 1, "input": 2}
    seen: dict[str, dict[str, Any]] = {}
    for j in touched:
        for f in j.changes:
            seen.setdefault(column_id(f), {"id": column_id(f), "kind": f.kind, "name": f.name, "label": f.label})
    columns = sorted(seen.values(), key=lambda c: order.get(c["kind"], 3))
    ids = set(seen)
    rows = []
    for j in touched:
        open_ = editable and not j.frozen and j.mark not in ("removed", "deleted")
        rows.append(
            {
                "job": j.instance_id,
                "name": j.name,
                "mark": j.mark,
                "clone_of": j.clone_of,
                "status": j.status,
                "frozen": j.frozen,
                "what": _what(j),
                "cells": {column_id(f): _cell(f, j, open_) for f in j.fields if column_id(f) in ids},
            }
        )
    tag = "new project" if pp.new else ("your project, made by hand" if pp.hand_made else "")
    return {
        "key": pp.path,
        "name": pp.name,
        "tag": tag,
        "summary": pp.summary().split(": ", 1)[-1],
        "values": [[label, value] for label, value in pp.values],
        "columns": columns,
        "rows": rows,
        "deletions": [_deletion(d) for d in pp.deletions],
        "run": _run(pp.run),
        "also": list(pp.also),
    }


def plan_table(plan: Plan, url: str) -> dict[str, Any]:
    """The whole plan for the pane: its head, a table per project, and every step in words."""
    return {
        "plan_id": plan.id,
        "revision": plan.revision,
        "status": plan.status.value,
        "title": plan.title,
        "expires": f"{plan.expires_at:%H:%M}",
        "ui_url": url,
        "editable": plan.editable,
        "permanent": [s.what for s in plan.permanent_steps],
        "projects": [project_table(pp, plan.editable) for pp in plan.projects.values()],
        "steps": [
            {"index": s.index, "what": s.what, "note": s.note, "permanent": s.permanent, "state": s.state.value}
            for s in plan.steps
        ],
    }


# ── Edits from the pane ───────────────────────────────────────────────────────


def _find(plan: Plan, project: str, job: str, kind: str, name: str) -> tuple[ProjectPlan, JobView, FieldView]:
    pp = plan.projects.get(project)
    if pp is None:
        raise LookupError(f"no project {project} in the plan")
    j = next((x for x in pp.jobs if x.instance_id == job), None)
    if j is None:
        raise LookupError(f"no job {job} in {pp.name}'s part of the plan")
    f = next((x for x in j.fields if x.kind == kind and x.name == name), None)
    if f is None:
        raise LookupError(f"{j.name} has no {kind} setting {name}")
    return pp, j, f


def pane_value(f: FieldView, raw: Any) -> Any:
    """What the person typed or picked, as the field's type. Raises ValueError for one that is not."""
    if f.type == "choice" and f.choices:
        for choice in f.choices:
            if raw == choice or (isinstance(raw, str) and raw == show(choice)):
                return choice
        raise ValueError(f"not one of {', '.join(show(c) for c in f.choices)}")
    if f.type == "bool" and isinstance(raw, str) and raw.strip().lower() not in _BOOL_WORDS:
        raise ValueError("on or off")
    try:
        return coerce(f, raw)
    except (TypeError, ValueError) as e:
        raise ValueError(f"not a {f.type}") from e


def pane_edits(plan: Plan, edits: list[dict[str, Any]]) -> list[Edit]:
    """The pane's edits as the page's (project, job, field, value). Raises LookupError or ValueError naming the
    setting that cannot take its value."""
    out: list[Edit] = []
    for e in edits:
        pp, j, f = _find(plan, e["project"], e["job"], e["kind"], e["field"])
        if f.kind not in EDITABLE_KINDS or not f.editable or j.frozen or j.mark in ("removed", "deleted"):
            raise ValueError(f"{j.name} · {f.label} is not edited here")
        try:
            value = pane_value(f, e["value"])
        except ValueError as why:
            raise ValueError(f"{j.name} · {f.label}: {why}") from why
        out.append((pp.path, j.instance_id, f, value))
    return out


def deletion_steps(plan: Plan) -> set[int]:
    """The steps a pane may drop: the deletions it lists (kept instead)."""
    return {d.step for pp in plan.projects.values() for d in pp.deletions}

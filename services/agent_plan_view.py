"""A change plan as the terminal shows it: markdown, per project its values, roster, settings (now → after),
deletions, the steps not previewed, and the run. Claude Code renders it; the agent prints it as it is. Built
from the same `Plan` the page shows, so the two never disagree."""

from __future__ import annotations

from typing import Any

from services.agent_plans import FieldView, JobView, Plan, ProjectPlan, format_bytes

_MARK = {"added": "+", "cloned": "+", "removed": "−", "deleted": "−"}


def show(value: Any) -> str:
    """A value as the user reads it: on/off, a dash for nothing, lists comma-joined, never JSON."""
    if value is None or value == "":
        return "—"
    if isinstance(value, bool):
        return "on" if value else "off"
    if isinstance(value, float):
        return f"{value:g}"
    if isinstance(value, (list, tuple)):
        return ", ".join(show(v) for v in value) or "—"
    if isinstance(value, dict):
        return "; ".join(f"{k}: {show(v)}" for k, v in value.items()) or "—"
    return str(value)


def _cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def _roster_line(pp: ProjectPlan) -> str:
    parts = []
    for j in pp.jobs:
        mark = _MARK.get(j.mark, "")
        name = j.name + (f" (clone of {j.clone_of})" if j.clone_of else "")
        if j.mark in ("added", "cloned"):
            parts.append(f"**{mark} {name}**")
        elif j.mark in ("removed", "deleted"):
            parts.append(f"~~{name}~~" + (" (to Trash)" if j.mark == "deleted" else " (off the roster)"))
        else:
            parts.append(name + (" *(changed)*" if j.changes else ""))
    return " · ".join(parts)


def _project(pp: ProjectPlan) -> list[str]:
    tag = " — new project" if pp.new else (" — your project, made by hand" if pp.hand_made else "")
    out = [f"### {pp.name}{tag}", ""]
    if pp.values:
        out += ["| Value | |", "|---|---|"]
        out += [f"| {_cell(k)} | {_cell(v)} |" for k, v in pp.values]
        out.append("")
    if pp.jobs:
        out += [f"**Roster:** {_roster_line(pp)}", ""]
    changes = pp.changes
    if changes:
        out += ["| Job | Setting | Now | After |", "|---|---|---|---|"]
        for j, f in changes:
            label = f.label + (" (SLURM)" if f.kind == "slurm" else "")
            now = show(f.before) + (" *(as added)*" if j.mark in ("added", "cloned") else "")
            out.append(f"| {_cell(j.name)} | {_cell(label)} | {_cell(now)} | **{_cell(show(f.after))}** |")
        out.append("")
    if pp.deletions:
        out += ["| Deleted | Size | | Leaves without input |", "|---|---|---|---|"]
        for d in pp.deletions:
            size = format_bytes(d.size_bytes) if d.size_bytes is not None else "not measured"
            kind = "**for good**" if d.permanent else "to Trash"
            out.append(f"| {_cell(d.what)} | {size} | {kind} | {_cell(', '.join(d.orphans) or '—')} |")
        out.append("")
    if pp.also:
        out += ["Also, not previewed:"] + [f"- {_cell(s)}" for s in pp.also] + [""]
    if pp.run is not None:
        jobs = pp.run.get("jobs") or []
        held = pp.run.get("held_by_review")
        head = f"**Run:** {len(jobs)} job{'s' * (len(jobs) != 1)} submitted"
        out += [head + (f", held for the {held} review" if held else ""), ""]
        if jobs:
            out += ["| Job | Partition | GPUs | CPUs | Memory | Time |", "|---|---|---|---|---|---|"]
            for job in jobs:
                r = job.get("resources") or {}
                name = job["instance_id"] + (" (waits for review)" if job.get("waits_for_review") else "")
                cells = [name, r.get("partition"), r.get("gres"), r.get("cpus_per_task"), r.get("mem"), r.get("time")]
                out.append("| " + " | ".join(_cell(show(c)) for c in cells) + " |")
            out.append("")
        for refusal in pp.run.get("refusals") or []:
            out.append(f"**Run refused:** {_cell(refusal)}")
        live = (pp.run.get("leaves") or {}).get("live")
        if live:
            out.append(f"Left running: {', '.join(live)}")
        out.append("")
    if not (pp.values or changes or pp.deletions or pp.also or pp.run or any(j.mark != "kept" for j in pp.jobs)):
        out += ["No change.", ""]
    return out


def overview(plan: Plan, url: str) -> str:
    """The whole plan as markdown, for the terminal."""
    n = len(plan.steps)
    out = [
        f"**{plan.title}** — plan `{plan.id}`"
        + (f", revision {plan.revision}" if plan.revision > 1 else "")
        + f", {n} step{'s' * (n != 1)}, {plan.status.value}, expires {plan.expires_at:%H:%M}",
        "",
    ]
    for pp in plan.projects.values():
        out += _project(pp)
    notes = [s for s in plan.steps if s.note]
    if notes:
        out += ["Not previewed (they run all the same):"] + [f"- {s.what}: {_cell(s.note)}" for s in notes] + [""]
    permanent = plan.permanent_steps
    if permanent:
        out += ["**Deleted for good** (approve these on the page): " + "; ".join(s.what for s in permanent), ""]
    out.append(f"Edit or approve it on the page: {url}")
    return "\n".join(out)


# ── The terminal dialog ───────────────────────────────────────────────────────

_INDENT = "  "


def _columns(rows: list[list[str]], indent: str) -> list[str]:
    """Rows as aligned columns of plain text; the last column runs free."""
    if not rows:
        return []
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]) - 1)]
    return [
        (indent + "  ".join(c.ljust(w) for c, w in zip(r, widths, strict=False)) + "  " + r[-1]).rstrip() for r in rows
    ]


def _dialog_project(pp: ProjectPlan) -> list[str]:
    tag = "  (new project)" if pp.new else ("  (your project, made by hand)" if pp.hand_made else "")
    out = [f"{pp.name}{tag}"]
    if pp.values:
        out += _columns([[k, v] for k, v in pp.values], _INDENT)
    moved = [j for j in pp.jobs if j.mark != "kept"]
    if moved:
        out += ["", _INDENT + "Jobs"]
        for j in moved:
            sign = {"added": "+", "cloned": "+", "removed": "-", "deleted": "-"}[j.mark]
            what = {"cloned": f"clone of {j.clone_of}", "removed": "off the roster", "deleted": "to Trash"}
            out.append(f"{_INDENT * 2}{sign} {j.name}" + (f"  ({what[j.mark]})" if j.mark in what else ""))
    changed = [j for j in pp.jobs if j.changes]
    if changed:
        out += ["", _INDENT + "Settings"]
        rows: list[list[str]] = []
        for j in changed:
            rows.append([j.name + ("  (as added)" if j.mark in ("added", "cloned") else ""), "now", "after"])
            for f in j.changes:
                label = _INDENT + f.label + (" (SLURM)" if f.kind == "slurm" else "")
                rows.append([label, show(f.before), show(f.after)])
        out += _columns(rows, _INDENT * 2)
    if pp.deletions:
        out += ["", _INDENT + "Deleted"]
        rows = []
        for d in pp.deletions:
            size = format_bytes(d.size_bytes) if d.size_bytes is not None else "size not measured"
            left = f"leaves without input: {', '.join(d.orphans)}" if d.orphans else ""
            rows.append([d.what, size, "FOR GOOD" if d.permanent else "to Trash", left])
        out += _columns(rows, _INDENT * 2)
    if pp.also:
        out += ["", _INDENT + "Also (not previewed)"] + [f"{_INDENT * 2}{s}" for s in pp.also]
    if pp.run is not None:
        jobs = pp.run.get("jobs") or []
        held = pp.run.get("held_by_review")
        out += [
            "",
            _INDENT
            + f"Run: {len(jobs)} job{'s' * (len(jobs) != 1)} submitted"
            + (f", held for the {held} review" if held else ""),
        ]
        rows = []
        for job in jobs:
            r = job.get("resources") or {}
            cells = [
                r.get("partition"),
                r.get("gres"),
                f"{show(r.get('cpus_per_task'))} CPUs",
                r.get("mem"),
                r.get("time"),
            ]
            rows.append([job["instance_id"]] + [show(c) for c in cells])
        out += _columns(rows, _INDENT * 2)
        out += [f"{_INDENT * 2}REFUSED: {x}" for x in pp.run.get("refusals") or []]
        live = (pp.run.get("leaves") or {}).get("live")
        if live:
            out.append(f"{_INDENT * 2}left running: {', '.join(live)}")
    if len(out) == 1:
        out.append(_INDENT + "no change")
    return out


def dialog_text(plan: Plan, url: str) -> str:
    """The whole plan as plain text with aligned columns, for the terminal dialog the user answers: what Claude
    Code shows above Apply / Do nothing. No markdown, so it reads the same however the dialog renders it."""
    out: list[str] = []
    for pp in plan.projects.values():
        out += [*_dialog_project(pp), ""]
    notes = [s for s in plan.steps if s.note]
    if notes:
        out += ["Not previewed (they run all the same):"] + [f"{_INDENT}{s.what}: {s.note}" for s in notes] + [""]
    out.append(f"Expires {plan.expires_at:%H:%M}. Edit it on the page instead: {url}")
    return "\n".join(out)


# ── The terminal dialog as a form ─────────────────────────────────────────────
#
# Claude Code shows a server dialog's message clipped to its first lines ("… (+22 more lines)") and every form
# field whole, one "title: value" row each. So the gist goes in the message's first two lines and the plan in
# fields: facts as one-choice rows, the settings it changes as fields to edit (filled in with the plan's value),
# each deletion as a yes/no. Accept applies the plan as the form stands; Decline changes nothing.

# Claude Code cuts a field's title at about 50 characters.
TITLE_MAX = 48

# What a field of the form edits: a setting, ("set", project, job, field), or a deletion kept, ("keep", step).
Target = tuple[str, str, str, FieldView] | tuple[str, int]


def _title(text: str) -> str:
    return text if len(text) <= TITLE_MAX else text[: TITLE_MAX - 1] + "…"


def _fact(title: str, value: str) -> dict[str, Any]:
    """A row that only shows: one choice, already chosen."""
    value = value or "—"
    return {"type": "string", "title": _title(title), "oneOf": [{"const": value, "title": value}], "default": value}


def _setting(job: JobView, f: FieldView) -> dict[str, Any]:
    label = f.label + (" (SLURM)" if f.kind == "slurm" else "")
    out: dict[str, Any] = {
        "title": _title(f"{label} · {job.name}"),
        "description": f"{job.name}: {label}, now {show(f.before)}" + (f". {f.help}" if f.help else ""),
    }
    if f.type == "bool":
        out.update(type="boolean", default=bool(f.after))
    elif f.type in ("int", "float"):
        out["type"] = "integer" if f.type == "int" else "number"
        if f.after is not None:
            out["default"] = f.after
        bounds = f.bounds or {}
        for key, schema_key in (("ge", "minimum"), ("le", "maximum")):
            if key in bounds:
                out[schema_key] = bounds[key]
    elif f.type == "choice" and f.choices:
        out.update(
            type="string",
            oneOf=[{"const": str(c), "title": show(c)} for c in f.choices],
            default=str(f.after) if f.after is not None else str(f.choices[0]),
        )
    else:
        out.update(type="string", default="" if f.after is None else str(f.after))
    return out


def dialog_form(plan: Plan, url: str) -> tuple[str, dict[str, Any], dict[str, Target]]:
    """The plan as the terminal dialog shows it: (message, form schema, what each editable field edits)."""
    props: dict[str, dict[str, Any]] = {}
    targets: dict[str, Target] = {}

    def add(prop: dict[str, Any], target: Target | None = None) -> None:
        key = f"f{len(props)}"
        props[key] = prop
        if target is not None:
            targets[key] = target

    for path, pp in plan.projects.items():
        tag = "new project" if pp.new else ("your project, made by hand" if pp.hand_made else "")
        add(_fact("Project", pp.name + (f" ({tag})" if tag else "")))
        for label, value in pp.values:
            add(_fact(label, value))
        for j in pp.jobs:
            if j.mark in ("added", "cloned"):
                add(_fact("Adds", j.name + (f" (clone of {j.clone_of})" if j.clone_of else "")))
            elif j.mark == "removed":
                add(_fact("Takes off the roster", j.name))
        for j in pp.jobs:
            for f in j.changes:
                if f.editable and j.mark not in ("removed", "deleted"):
                    add(_setting(j, f), ("set", path, j.instance_id, f))
                else:
                    add(_fact(f"{f.label} · {j.name}", f"{show(f.before)} → {show(f.after)}"))
        for d in pp.deletions:
            size = format_bytes(d.size_bytes) if d.size_bytes is not None else "size not measured"
            add(
                {
                    "type": "boolean",
                    "title": _title(f"Delete {d.what}"),
                    "description": f"{d.what}: {size}, to Trash"
                    + (f"; leaves without input: {', '.join(d.orphans)}" if d.orphans else "")
                    + ". Off keeps it.",
                    "default": True,
                },
                ("keep", d.step),
            )
        for s in pp.also:
            add(_fact("Also", s))
        if pp.run is not None:
            jobs = pp.run.get("jobs") or []
            add(
                _fact(
                    "Runs",
                    f"{len(jobs)} job{'s' * (len(jobs) != 1)}" + (" (REFUSED)" if pp.run.get("refusals") else ""),
                )
            )
            for job in jobs:
                r = job.get("resources") or {}
                add(_fact(job["instance_id"], " · ".join(show(r.get(k)) for k in ("partition", "gres", "time"))))
    for s in plan.steps:
        if s.note:
            add(_fact("Not previewed", f"{s.what}: {s.note}"))

    summary = "; ".join(p.summary() for p in plan.projects.values())
    message = "\n".join(
        [
            f"{plan.title}: {summary}",
            f"Change a value below to edit the plan · Accept applies it as shown · Decline changes nothing · "
            f"expires {plan.expires_at:%H:%M}",
            "",
            dialog_text(plan, url),
        ]
    )
    return message, {"type": "object", "properties": props}, targets

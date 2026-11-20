"""The one renderer of a job's Parameters: `services/job_layout.layout_of(job_model)` drawn at the field geometry.

Each group is its title (`group_title`) and its rows: the values first, then the on/off parameters as yes | no rows,
so a toggle stays in its group. A row is the label column, the box at its width class (S, M or L), the unit column.
The groups flow into CSS columns of 330 px, as many as the section's width holds.

A plugin keeps only what is its own, through hooks:
- `before`: drawn above the groups (a job's notes, banners, a live summary);
- `custom={field: build}`: draws that field's row instead (a choice fed from project state);
- `replace={group: build}`: draws the group's rows instead (template matching's species rows);
- `after={group: build}`: rows under the group (a note on what the group's fields resolve to).
A hook may return a refresh; the renderer's own refresh runs them, after an edit in any section and on the page's
poll. Every row is built once: a parameter that means nothing under another's value (`shows_param`) has its row
hidden, never rebuilt, so no control is replaced under the user.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from nicegui import ui

from services.job_layout import CHOICE, LIST, NUMBER, TOGGLE, FieldLayout, GroupLayout, L, layout_of
from ui.job_plugins._field_styles import (
    FIELD_ROW_STYLE,
    FROZEN_BOX_STYLE,
    MONO,
    SANS,
    CLR_SUBLABEL,
    _label,
    box_style,
    enum_field,
    group_title,
    numeric_field,
    path_row,
    text_field,
    toggle_field,
)

# Groups flow into columns this wide; a row's L box runs to its column's edge.
GROUP_COLUMN_PX = 330
FLOW_STYLE = f"column-width: {GROUP_COLUMN_PX}px; column-gap: 28px; width: 100%;"
GROUP_STYLE = (
    "display: flex; flex-direction: column; gap: 3px; margin-bottom: 10px; break-inside: avoid; width: 100%; "
    "min-width: 0;"
)
_PATHLIKE = ("path", "dir", "glob", "pattern", "file", "folder")
# A value's ink.
_VALUE_INK = "var(--cb-ink-2, #1e293b)"

Build = Callable[..., Any]


def _is_pathlike(name: str) -> bool:
    return any(k in name.lower() for k in _PATHLIKE)


def _species_hint(job_model, project_path: Path | None, instance_id: str | None) -> dict[str, str]:
    """For each field the job took from its species: the species' value, named in the row's hover."""
    if project_path is None or job_model.job_type is None:
        return {}
    from services.models_base import resolve_species
    from services.pipeline_edit import species_defaults
    from services.project_state import get_project_state_for

    species, _ = resolve_species(get_project_state_for(project_path), job_model, instance_id)
    if species is None:
        return {}
    note = "Changing it here affects only this job; the species is not modified."
    out = {}
    for name, value in species_defaults(job_model.job_type, species).items():
        shown = Path(value).name if isinstance(value, str) and "/" in value else value
        out[name] = f"Species default ({species.name}): {shown}. {note}"
    return out


def render_field(
    f: FieldLayout,
    job_model,
    *,
    is_frozen: bool,
    save_handler: Callable,
    project_path: Path | None = None,
    hint: str | None = None,
):
    """One parameter's row at the field geometry, by its kind. Returns the control."""
    hint = hint if hint is not None else f.hint
    if f.kind == TOGGLE:
        return toggle_field(f.title, job_model, f.name, is_frozen=is_frozen, save_handler=save_handler, hint=hint)
    if f.kind == CHOICE and f.enum is not None:
        return enum_field(
            f.title,
            job_model,
            f.name,
            f.enum,
            is_frozen=is_frozen,
            save_handler=save_handler,
            hint=hint,
            width=f.width,
            unit=f.unit,
        )
    if f.kind == NUMBER:
        return numeric_field(
            f.title,
            job_model,
            f.name,
            is_frozen=is_frozen,
            save_handler=save_handler,
            hint=hint,
            suffix=f.unit,
            width=f.width,
        )
    if f.kind == LIST:
        # A list with no plugin row of its own: what it holds, read-only.
        with ui.element("div").style(FIELD_ROW_STYLE):
            _label(f.title, hint)
            values = getattr(job_model, f.name) or []
            ui.label(", ".join(map(str, values)) or "none").style(
                f"{FROZEN_BOX_STYLE} {box_style(L)} {MONO} font-size: 10.5px; overflow: hidden; "
                f"white-space: nowrap; text-overflow: ellipsis; color: {_VALUE_INK if values else CLR_SUBLABEL};"
            )
        return None
    if f.width == L and _is_pathlike(f.name):
        return path_row(
            f.title, job_model, f.name, is_frozen=is_frozen, save_handler=save_handler, hint=hint, project=project_path
        )
    return text_field(
        f.title,
        job_model,
        f.name,
        is_frozen=is_frozen,
        save_handler=save_handler,
        hint=hint,
        suffix=f.unit,
        width=f.width,
    )


def render_params(
    job_model,
    *,
    is_frozen: bool,
    save_handler: Callable,
    project_path: Path | None = None,
    instance_id: str | None = None,
    exclude: set[str] | frozenset[str] = frozenset(),
    before: Build | None = None,
    custom: dict[str, Build] | None = None,
    replace: dict[str, Build] | None = None,
    after: dict[str, Build] | None = None,
) -> Callable[[], None]:
    """Draw the job's Parameters; returns their refresh (rows shown or hidden as `shows_param` says now, then each
    hook's refresh). `custom` builders take the field's `FieldLayout`; `replace` builders the `GroupLayout`;
    `before` and `after` builders nothing."""
    custom, replace, after = custom or {}, replace or {}, after or {}
    refreshes: list[Callable[[], Any]] = []
    rows: list[tuple[ui.element, str]] = []
    groups: list[tuple[ui.element, list[str]]] = []

    def keep(result) -> None:
        if callable(result):
            refreshes.append(result)

    if before is not None:
        keep(before())
    layout = [
        GroupLayout(g.title, tuple(f for f in g.fields if f.name not in exclude))
        for g in layout_of(job_model)
        if g.title in replace or any(f.name not in exclude for f in g.fields)
    ]
    if not layout:
        ui.label("No configurable parameters.").style(f"{SANS} font-size: 10px; color: {CLR_SUBLABEL};")
        return lambda: None
    species_hint = _species_hint(job_model, project_path, instance_id)
    with ui.element("div").classes("cb-pflow").style(FLOW_STYLE):
        for g in layout:
            box = ui.element("div").classes("cb-pgroup").style(GROUP_STYLE)
            with box:
                group_title(g.title)
                if g.title in replace:
                    keep(replace[g.title](g))
                    names: list[str] = []
                else:
                    names = []
                    for f in (*g.values, *g.toggles):
                        row = ui.element("div").style("width: 100%; min-width: 0;")
                        with row:
                            if f.name in custom:
                                keep(custom[f.name](f))
                            else:
                                render_field(
                                    f,
                                    job_model,
                                    is_frozen=is_frozen,
                                    save_handler=save_handler,
                                    project_path=project_path,
                                    hint="\n".join(x for x in (f.hint, species_hint[f.name]) if x)
                                    if f.name in species_hint
                                    else None,
                                )
                        rows.append((row, f.name))
                        names.append(f.name)
                if g.title in after:
                    keep(after[g.title]())
            groups.append((box, names))

    def show() -> None:
        for row, name in rows:
            if not row.is_deleted:
                row.set_visibility(job_model.shows_param(name))
        for box, names in groups:
            if names and not box.is_deleted:
                box.set_visibility(any(job_model.shows_param(n) for n in names))

    def refresh() -> None:
        show()
        for r in refreshes:
            r()

    show()
    return refresh

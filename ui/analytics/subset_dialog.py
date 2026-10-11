"""The rule-filter dialog: a new particle set from one parent and an ordered list of rules, with
the funnel previewed live before anything is written.

Rules apply top to bottom, each to what the rules above kept. The parent is never changed; the
new set records the parent, the rules and the funnel (services/particles/set_ops.py).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nicegui import ui

from services.array_tasks import ts_display_name
from services.particles.particle_sets import ParentRef, ParticleSet, Rule, RuleKind
from ui.analytics.common import ERROR_CLS, HINT_CLS, LABEL_CLS, esc
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_number, house_select, house_text
from ui.components.reactive import SingleFlight
from ui.styles import MONO

_PREVIEW_DEBOUNCE_S = 0.4


@dataclass
class _RuleRow:
    kind: RuleKind
    on: bool = False
    value: float | None = None
    upper: float | None = None
    tomos: list[str] = field(default_factory=list)
    other_key: str | None = None
    other_min_sigma: float | None = None


@dataclass
class SubsetDefaults:
    """What the dialog opens with: the parent to preselect, the cut on screen, the species Ø
    (the nearest-neighbour rule's starting value, said so in its hint) and the tomograms."""

    parent_key: str | None
    cut: float | None
    diameter_ang: float | None
    tomos: list[str]


# In the order they apply: where picks are (tomograms, faces, depth, other species), then how they
# score, then duplicates (the higher score of a pair wins), then a per-tomogram top N.
_RULE_TEXT: dict[RuleKind, tuple[str, str]] = {
    RuleKind.EXCLUDE_TOMOS: ("exclude tomograms", "Leave these tomograms out."),
    RuleKind.FACE_DISTANCE: ("≥ Å from the volume faces", "Drop picks closer than this to any face of the volume."),
    RuleKind.DEPTH_BAND: (
        "depth from pick plane (Å)",
        "Keep picks between these distances from the plane fitted through the tomogram's picks (where the lamella "
        "holds them); negative is below it. Tomograms with fewer than 20 picks have no plane and are kept whole.",
    ),
    RuleKind.AWAY_FROM: (
        "away from another set (Å)",
        "Drop picks within this distance of the other set's picks (one position claimed by two species).",
    ),
    RuleKind.MIN_SIGMA: ("σ ≥", "Keep picks scoring at or above this, in σ (LCC ÷ the tomogram's noise level)."),
    RuleKind.MIN_LCC: ("LCC ≥", "Keep picks whose raw LCC is at or above this. Not comparable between tomograms."),
    RuleKind.MIN_NN: (
        "nearest neighbour ≥ Å",
        "Thin pairs closer than this, keeping the higher-scoring pick (duplicates of one particle).",
    ),
    RuleKind.TOP_N: ("top N per tomogram", "Keep the N highest-scoring picks of each tomogram."),
}


async def open_subset_dialog(
    backend, project_path: Path, species_id: str, parents: list[ParentRef], defaults: SubsetDefaults
) -> ParticleSet | None:
    """Show the dialog; the created set, or None when cancelled."""
    if not parents:
        ui.notify("Nothing to make a set from yet: no candidates, subtomo particles or sets.", type="warning")
        return None
    by_key = {p.key: p for p in parents}
    rows = {k: _RuleRow(k) for k in _RULE_TEXT}
    if defaults.cut is not None:
        rows[RuleKind.MIN_SIGMA].on = True
        rows[RuleKind.MIN_SIGMA].value = round(defaults.cut, 2)
    if defaults.diameter_ang:
        rows[RuleKind.MIN_NN].value = float(defaults.diameter_ang)
    st: dict[str, Any] = {
        "parent": defaults.parent_key if defaults.parent_key in by_key else parents[0].key,
        "name": f"≥ {defaults.cut:.1f}σ" if defaults.cut is not None else "subset",
        "busy": False,
    }
    flight = SingleFlight()
    pending: dict[str, asyncio.Task | None] = {"task": None}

    def rules() -> list[Rule]:
        out: list[Rule] = []
        for kind, r in rows.items():
            if not r.on:
                continue
            if kind == RuleKind.EXCLUDE_TOMOS:
                if r.tomos:
                    out.append(Rule(kind=kind, tomos=list(r.tomos)))
                continue
            if r.value is None:
                continue
            if kind == RuleKind.AWAY_FROM:
                other = by_key.get(r.other_key or "")
                if other is None:
                    continue
                out.append(Rule(kind=kind, value=r.value, other=other, other_min_sigma=r.other_min_sigma))
                continue
            out.append(Rule(kind=kind, value=r.value, upper=r.upper))
        return out

    with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 760px; max-width: 95vw; gap: 8px;"):
        ui.label("New particle set").style("font-size: 13px; font-weight: 600; color: #0f172a;")
        ui.label(
            "Rules apply top to bottom, each to what the rules above kept. The parent stays as it is; the new set "
            "records it, the rules and the funnel."
        ).classes(HINT_CLS)
        with ui.row().classes("w-full items-center gap-3 no-wrap"):
            house_text("name", value=st["name"], width="w-48", on_change=lambda e: st.update(name=e.value or ""))
            house_select(
                "from",
                {p.key: f"{p.label} · {p.level.value}" for p in parents},
                value=st["parent"],
                width="w-96",
                hint="The table the rules read: candidates (coordinates, need subtomo extraction), subtomo particles "
                "(extracted: the subset needs no new extraction) or an existing set.",
                on_change=lambda e: (st.update(parent=e.value), schedule()),
            )
        ui.label("rules").classes(LABEL_CLS)
        with ui.column().classes("w-full gap-1"):
            for kind, (label, tip) in _RULE_TEXT.items():
                _rule_row(rows[kind], label, tip, defaults, parents, schedule=lambda: schedule())
        ui.label("funnel").classes(LABEL_CLS)
        funnel_box = ui.html("", sanitize=False)
        error = ui.label("").classes(ERROR_CLS)
        with ui.row().classes("w-full justify-end gap-2"):
            house_button("Cancel", lambda: dlg.submit(None))
            create_btn = house_button("Create set", None, kind="accent")

    async def preview() -> None:
        async with flight("preview") as acquired:
            if not acquired:
                return
            res = await backend.preview_particle_set(by_key[st["parent"]], rules())
            if funnel_box.is_deleted:
                return
            if not res["success"]:
                funnel_box.set_content("")
                error.set_text(res["error"])
                return
            error.set_text("")
            funnel_box.set_content(_funnel_html(res["funnel"]))

    def schedule() -> None:
        task = pending["task"]
        if task is not None and not task.done():
            task.cancel()

        async def later() -> None:
            await asyncio.sleep(_PREVIEW_DEBOUNCE_S)
            await preview()

        pending["task"] = asyncio.create_task(later())

    async def create() -> None:
        async with flight("create") as acquired:
            if not acquired:
                return
            name = (st["name"] or "").strip()
            if not name:
                error.set_text("Give the set a name.")
                return
            res = await backend.create_particle_set(
                project_path, parent=by_key[st["parent"]], rules=rules(), name=name, species_id=species_id
            )
            if not res["success"]:
                error.set_text(res["error"])
                return
            dlg.submit(res["set"])

    create_btn.on_click(create)
    schedule()
    result = await dlg
    dlg.delete()
    if isinstance(result, ParticleSet):
        ui.notify(
            f"Set {result.name}: {result.n_particles:,} particles in {result.n_tomograms} tomograms", type="positive"
        )
        return result
    return None


def _rule_row(r: _RuleRow, label: str, tip: str, defaults: SubsetDefaults, parents, *, schedule) -> None:
    def toggle(e) -> None:
        r.on = bool(e.value)
        schedule()

    def setter(attr: str):
        def _set(e) -> None:
            setattr(r, attr, None if e.value in (None, "") else (e.value if attr == "tomos" else float(e.value)))
            schedule()

        return _set

    with ui.row().classes("w-full items-center gap-2 no-wrap").style("min-height: 22px;"):
        ui.checkbox(value=r.on, on_change=toggle).props("dense size=xs")
        ui.label(label).style("font-size: 10px; color: #334155; width: 170px;").tooltip(tip)
        match r.kind:
            case RuleKind.DEPTH_BAND:
                house_number("", value=r.value, width="w-20", step=10, on_change=setter("value"))
                ui.label("to").classes(HINT_CLS)
                house_number("", value=r.upper, width="w-20", step=10, on_change=setter("upper"))
            case RuleKind.EXCLUDE_TOMOS:
                house_select(
                    "",
                    {t: ts_display_name(t) for t in defaults.tomos},
                    value=r.tomos,
                    multiple=True,
                    width="w-96",
                    on_change=setter("tomos"),
                ).props("use-chips")
            case RuleKind.AWAY_FROM:
                house_number("", value=r.value, width="w-20", step=10, on_change=setter("value"))
                house_select(
                    "set",
                    {p.key: p.label for p in parents},
                    value=r.other_key,
                    width="w-64",
                    on_change=lambda e: (setattr(r, "other_key", e.value), schedule()),
                )
                house_number(
                    "its σ ≥", value=r.other_min_sigma, width="w-16", step=0.5, on_change=setter("other_min_sigma")
                )
            case _:
                step = 0.1 if r.kind == RuleKind.MIN_SIGMA else (0.005 if r.kind == RuleKind.MIN_LCC else 1)
                house_number("", value=r.value, width="w-20", step=step, on_change=setter("value"))
                if r.kind == RuleKind.MIN_NN and defaults.diameter_ang:
                    ui.label(f"starts at the species Ø, {defaults.diameter_ang:g} Å").classes(HINT_CLS)


def _funnel_html(steps) -> str:
    rows = []
    for i, s in enumerate(steps):
        step = s if isinstance(s, dict) else s.model_dump()
        removed = "" if i == 0 else f"−{step['removed']:,}"
        note = f' title="{esc(step.get("note", ""))}"' if step.get("note") else ""
        rows.append(
            f"<tr><td>{esc(step['label'])}</td><td class='n'>{step['kept']:,}</td>"
            f"<td class='n dim'>{removed}</td><td class='mk'{note}>{'note' if step.get('note') else ''}</td></tr>"
        )
    return (
        f'<table class="cb-atable" style="{MONO}"><thead><tr><th>step</th><th class="n">kept</th>'
        f'<th class="n">removed</th><th></th></tr></thead><tbody>{"".join(rows)}</tbody></table>'
    )

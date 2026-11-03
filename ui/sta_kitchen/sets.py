"""The kitchen's Make set dialogs: the plan (`services/sta_kitchen/export.py`) shown before anything is written,
in plain words, then the set made in a worker.

- `open_make_set`: one set, from the draft as it stands, from one subset (its right-click), or from the
  selection (the selection bar: the picks become a subset when the set is made). The choice of one frame for an
  acquisition seen in frames that cannot share picks sits under the plan.
- `open_make_sets`: a set of each subset of a group, made in turn.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from nicegui import ui

from services.sta_kitchen import service
from services.sta_kitchen.models import Scope
from ui.analytics.common import ERROR_CLS, HINT_CLS
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_text

if TYPE_CHECKING:
    from ui.sta_kitchen.tab import KitchenTab

logger = logging.getLogger(__name__)

_TITLE = "font-size: 13px; font-weight: 600; color: #0f172a;"
_CELL = "font-size: 10px; color: #334155;"
_MONO = "font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: #334155;"
_WRAP = "white-space: normal; line-height: 1.45;"
# Conflicted acquisitions listed one by one; the "prefer" buttons cover the rest.
CONFLICTS_LISTED = 30
SELECTION = "__selection__"  # the scope id of picks selected in the browser, before they become a subset


def _scope_text(lp, scope: Scope | None, n_selected: int | None) -> str:
    """Where a set's picks come from, in words: the draft, a subset (and its group), or a selection."""
    if n_selected is not None:
        return f"the {n_selected:,} selected picks"
    if scope is None:
        scope = lp.pool.draft.scope
    if not scope.include and not scope.exclude:
        return "every pick of the pool"
    name = {x.id: x.name for x in lp.pool.subsets}.get
    parts = []
    if scope.include:
        parts.append(" ∪ ".join(f"“{name(i, i)}”" for i in scope.include))
    parts += [f"not “{name(i, i)}”" for i in scope.exclude]
    groups = {lp.pool.group(x.group).name for x in lp.pool.subsets if x.id in scope.include and lp.pool.group(x.group)}
    return "subset " + " ".join(parts) + (f" (group {', '.join(sorted(groups))})" if groups else "")


def _plan_lines(plan: dict) -> None:
    """The plan in the dialog: counts, per project, hand verdicts, renames, what stops it, what is unverified."""
    dedup = plan["n_before_dedup"] - plan["n"]
    ui.label(
        f"{plan['n']:,} picks in {plan['n_frames']} tomograms of {plan['n_projects']} project(s)"
        + (f"; {dedup:,} picks within Ø/2 of a better one removed" if dedup else "")
    ).style(_CELL)
    tomos = plan.get("per_project_tomograms") or {}
    with ui.element("div").style(
        "display: grid; grid-template-columns: minmax(0, 1fr) auto auto; gap: 0 12px; width: 100%;"
    ):
        for project, n in plan["per_project"].items():
            ui.label(project).style(_MONO).classes("truncate")
            ui.label(f"{n:,} picks").style(_MONO)
            ui.label(f"{tomos.get(project, 0)} tomograms").style(_MONO)
    if plan["kept_by_hand"] or plan["dropped_by_hand"]:
        ui.label(f"by hand: {plan['kept_by_hand']:,} kept, {plan['dropped_by_hand']:,} dropped").classes(HINT_CLS)
    renamed = plan.get("renamed") or []
    if renamed:
        shown = ", ".join(f"{r['tomo']} ({r['project']}) → {r['as']}" for r in renamed[:6])
        ui.label(
            f"{len(renamed)} tomogram(s) renamed, their names repeating across projects: {shown}"
            + (", …" if len(renamed) > 6 else "")
            + ". Each gets its own per-tilt star under the new name in the set."
        ).style(_CELL + _WRAP)
    for b in plan["blockers"][:8]:
        ui.label(b).classes(ERROR_CLS).style(_WRAP)
    if len(plan["blockers"]) > 8:
        ui.label(f"and {len(plan['blockers']) - 8} more").classes(ERROR_CLS)
    for u in plan["unverified"] + plan["notes"]:
        ui.label(u).style(_CELL + _WRAP)


async def open_make_set(
    tab: KitchenTab, *, scope: Scope | None = None, name: str | None = None, selection: list[str] | None = None
) -> None:
    """Make set from the draft (`scope` None), from it narrowed to `scope` (a subset), or from the picks
    `selection` names, which become a subset of the set's name when it is made."""
    lp = tab.lp
    names = {x.id: x.name for x in lp.pool.subsets}
    if selection is not None:
        scope = Scope(include=[SELECTION])
    if name is None:
        name = f"{tab.ctx.species_id} {service.describe_draft(lp.pool.draft.model_dump(), names)}"[:60]
    st = {"name": name, "plan": None}
    extra = {SELECTION: set(selection)} if selection is not None else None
    with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 760px; max-width: 95vw; gap: 8px;"):
        ui.label("Make set").style(_TITLE)
        ui.label(
            "The kept picks become a particle set of this project, fixed as they are now. A job reading it takes "
            "what it needs of each pick from the jobs that made it: Reconstruct particle and the extraction jobs "
            "the positions and angles, Class 3D and Refine 3D the particle images an extraction cut (chosen on the "
            "job's input). Nothing is written into the projects it reads from."
        ).classes(HINT_CLS).style(_WRAP)
        ui.label(
            f"From {_scope_text(lp, scope, len(selection) if selection is not None else None)}, with the filters, "
            "the hand verdicts and the source choices as they are now."
        ).style(_CELL + _WRAP)
        house_text("name", value=st["name"], width="w-96", on_change=lambda e: st.update(name=e.value or ""))
        body = ui.column().classes("w-full gap-1")
        frames = ui.column().classes("w-full gap-1").style("max-height: 34vh; overflow-y: auto;")

        async def compute() -> None:
            plan = await service.plan_set(lp, st["name"] or "pool set", scope=scope, extra=extra)
            st["plan"] = plan
            body.clear()
            with body:
                _plan_lines(plan)
            await render_frames()
            make_btn.set_enabled(not plan["blockers"] and plan["n"] > 0)

        async def render_frames() -> None:
            conflicts = await asyncio.to_thread(service.frame_conflicts, lp)
            frames.clear()
            if not conflicts:
                return
            with frames:
                open_ = sum(1 for c in conflicts if not c["chosen"])
                ui.label(
                    f"{len(conflicts)} tilt series were reconstructed twice in ways whose picks cannot be shared "
                    f"(another alignment, geometry or hand); {open_} without a choice. A set takes one "
                    "reconstruction per tilt series."
                ).style(_CELL + _WRAP)
                sources = sorted({it["source"] for c in conflicts for it in c["items"]})
                with ui.row().classes("items-center gap-2"):
                    ui.label("for all of them, prefer").classes(HINT_CLS)
                    for src in sources:
                        house_button(src, lambda _e, src=src: prefer(src))
                for c in conflicts[:CONFLICTS_LISTED]:
                    with ui.row().classes("w-full items-center no-wrap gap-2"):
                        ui.label(c["tomo"]).style(_CELL).classes("truncate").style("width: 280px;")
                        for it in c["items"]:
                            btn = house_button(
                                f"{it['source']} · {it['rows']:,}",
                                lambda _e, it=it: choose(it),
                                kind="accent" if it["chosen"] else "default",
                            )
                            btn.tooltip(it["frame"])
                if len(conflicts) > CONFLICTS_LISTED:
                    ui.label(f"and {len(conflicts) - CONFLICTS_LISTED} more: use prefer").classes(HINT_CLS)

        async def prefer(src: str) -> None:
            n = await service.prefer_source(lp, src)
            ui.notify(f"{n} tilt series now take their reconstruction from {src}")
            tab._push()
            await compute()

        async def choose(it: dict) -> None:
            await service.choose_frame(lp, it["acq"], it["frame"])
            tab._push()
            await compute()

        async def do_make() -> None:
            plan = st["plan"]
            if plan is None or plan["blockers"]:
                return
            set_name = st["name"] or plan["name"]
            if selection is not None:
                # The selection becomes a subset first, so the set names where its picks came from.
                sub = await service.create_subset(lp, set_name, list(selection))
                tab.push_subsets()
                plan = await service.plan_set(lp, set_name, scope=Scope(include=[sub.id]))
                if plan["blockers"] or plan["n"] != st["plan"]["n"]:
                    ui.notify(
                        f"The pool moved while the dialog was open: the selection, kept as subset “{sub.name}”, "
                        f"now gives {plan['n']:,} picks. Make set from its menu.",
                        type="warning",
                        multi_line=True,
                    )
                    dlg.close()
                    return
            plan["name"] = set_name
            dlg.close()
            await _make(lp, plan)

        with ui.row().classes("w-full items-center gap-2"):
            ui.element("div").style("flex: 1;")
            house_button("Cancel", dlg.close)
            make_btn = house_button("Make set", do_make, kind="accent")
    dlg.open()
    await compute()


async def _make(lp, plan: dict) -> dict | None:
    ui.notify(f"Making set {plan['name']}: {plan['n']:,} picks…")
    try:
        ps = await service.make_set(lp, plan)
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Kitchen: the set %s could not be made", plan["name"])
        ui.notify(f"The set {plan['name']} could not be made: {e}", type="negative", multi_line=True)
        return None
    ui.notify(f"Set {ps['id']} made: {ps['n_particles']:,} picks", type="positive")
    return ps


async def open_make_sets(tab: KitchenTab, group_id: str) -> None:
    """A set of each subset of a group, each from the draft narrowed to that subset, made in turn."""
    lp = tab.lp
    group = lp.pool.group(group_id)
    subs = [x for x in lp.pool.subsets if x.group == group_id]
    if group is None or not subs:
        ui.notify("The group holds no subset", type="warning")
        return
    st = {"plans": {}, "names": {x.id: x.name for x in subs}}
    with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 760px; max-width: 95vw; gap: 8px;"):
        ui.label(f"A set of each subset of {group.name}").style(_TITLE)
        ui.label(
            "Each subset becomes its own particle set, with the filters, the hand verdicts and the source choices as "
            "they are now; the sets are made one after the other. A subset whose plan names a problem is left out."
        ).classes(HINT_CLS).style(_WRAP)
        rows = ui.column().classes("w-full gap-2").style("max-height: 52vh; overflow-y: auto;")

        async def compute() -> None:
            plans = {}
            for x in subs:
                plans[x.id] = await service.plan_set(lp, st["names"][x.id] or x.name, scope=Scope(include=[x.id]))
            st["plans"] = plans
            rows.clear()
            with rows:
                for x in subs:
                    p = plans[x.id]
                    with ui.column().classes("w-full gap-0"):
                        with ui.row().classes("w-full items-center gap-2 no-wrap"):
                            ui.element("span").style(
                                f"width: 8px; height: 8px; border-radius: 50%; background: {x.color}; flex: 0 0 auto;"
                            )
                            house_text(
                                "set",
                                value=st["names"][x.id],
                                width="w-64",
                                on_change=lambda e, sid=x.id: st["names"].update({sid: e.value or ""}),
                            )
                            ui.label(
                                f"{p['n']:,} picks · {p['n_frames']} tomograms · {p['n_projects']} project(s)"
                            ).style(_MONO)
                        for b in p["blockers"][:3]:
                            ui.label(b).classes(ERROR_CLS).style(_WRAP)
                        if not p["n_selected"]:
                            ui.label("no pick of it passes the filters: left out").classes(HINT_CLS)
            ready = [x for x in subs if not plans[x.id]["blockers"] and plans[x.id]["n"]]
            make_btn.set_text(f"Make {len(ready)} set(s)")
            make_btn.set_enabled(bool(ready))

        async def do_make() -> None:
            ready = [x for x in subs if not st["plans"][x.id]["blockers"] and st["plans"][x.id]["n"]]
            dlg.close()
            made = []
            for x in ready:
                plan = st["plans"][x.id]
                plan["name"] = st["names"][x.id] or x.name
                if await _make(lp, plan) is not None:
                    made.append(plan["name"])
            left = len(subs) - len(made)
            ui.notify(
                f"{len(made)} set(s) made from {group.name}" + (f"; {left} subset(s) left out" if left else ""),
                type="positive" if made else "warning",
            )

        with ui.row().classes("w-full items-center gap-2"):
            ui.element("div").style("flex: 1;")
            house_button("Cancel", dlg.close)
            make_btn = house_button("Make sets", do_make, kind="accent")
    dlg.open()
    await compute()

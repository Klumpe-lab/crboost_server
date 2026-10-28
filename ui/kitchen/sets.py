"""The kitchen's Doctor, Make set and Ledger dialogs.

Doctor: the markers (`services/kitchen/doctor.py`), and for an acquisition seen in frames that cannot
share picks, the choice of one (per acquisition, or "prefer" a project's reconstruction for all).
Make set: the plan (`services/kitchen/export.py`) shown before anything is written, then the set made
in a worker. Ledger: the sets made from this pool, their readers, and a comparison of two of them in
the gallery.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from nicegui import ui

from services.kitchen import service
from ui.analytics.common import ERROR_CLS, HINT_CLS
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_select, house_text

if TYPE_CHECKING:
    from ui.kitchen.tab import KitchenTab

logger = logging.getLogger(__name__)

_TITLE = "font-size: 13px; font-weight: 600; color: #0f172a;"
_CELL = "font-size: 10px; color: #334155;"
_MONO = "font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: #334155;"
_SEVERITY = {"block": "#dc2626", "warn": "#b45309", "info": "#475569"}
LEVELS = {
    "auto": "as the rows are",
    "coordinates": "coordinates (extract later)",
    "extracted_only": "only the extracted rows",
}
# Conflicted acquisitions listed one by one; the "prefer" buttons cover the rest.
CONFLICTS_LISTED = 30


async def open_doctor(tab: KitchenTab) -> None:
    lp = tab.lp
    with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 860px; max-width: 95vw; gap: 8px;"):
        ui.label("Doctor").style(_TITLE)
        ui.label("Markers point at what needs a look. None of them drops anything.").classes(HINT_CLS)
        box = ui.column().classes("w-full gap-2").style("max-height: 64vh; overflow-y: auto;")

        async def render() -> None:
            markers = await service.diagnose(lp)
            box.clear()
            with box:
                if not markers:
                    ui.label("Nothing to look at.").classes(HINT_CLS)
                conflicts = [m for m in markers if m.key == "frames_conflict"]
                for m in markers:
                    if m.key == "frames_conflict":
                        continue
                    with ui.row().classes("w-full items-baseline no-wrap gap-2"):
                        ui.label("⚑").style(f"color: {_SEVERITY[m.severity]}; font-size: 10px;")
                        ui.label(m.text).style(_CELL)
                if conflicts:
                    _render_conflicts(conflicts, render)
            tab._push()

        def _render_conflicts(conflicts, rerender) -> None:
            open_ = [c for c in conflicts if c.severity == "block"]
            ui.label(
                f"{len(conflicts)} acquisitions are in frames that cannot share picks; {len(open_)} without a choice. "
                + conflicts[0].text.split(". ", 1)[1]
            ).style(_CELL)
            sources = sorted({service.frame_source(_frame(lp, it["frame"])) for c in conflicts for it in c.items})
            with ui.row().classes("items-center gap-2"):
                ui.label("for all of them, prefer").classes(HINT_CLS)
                for src in sources:
                    house_button(src, lambda _e, src=src: prefer(src))
            for c in conflicts[:CONFLICTS_LISTED]:
                with ui.row().classes("w-full items-center no-wrap gap-2"):
                    ui.label(c.items[0]["tomo"]).style(_CELL).classes("truncate").style("width: 280px;")
                    for it in c.items:
                        label = f"{service.frame_source(_frame(lp, it['frame']))} · {it['rows']:,}"
                        btn = house_button(
                            label, lambda _e, it=it: choose(it), kind="accent" if it["chosen"] else "default"
                        )
                        btn.tooltip(it["frame"])
            if len(conflicts) > CONFLICTS_LISTED:
                ui.label(f"and {len(conflicts) - CONFLICTS_LISTED} more: use prefer").classes(HINT_CLS)

        async def prefer(src: str) -> None:
            n = await service.prefer_source(lp, src)
            ui.notify(f"{n} acquisition(s) now take their frame from {src}")
            await render()

        async def choose(it: dict) -> None:
            await service.choose_frame(lp, it["acq"], it["frame"])
            await render()

        await render()
        with ui.row().classes("w-full items-center"):
            ui.element("div").style("flex: 1;")
            house_button("Close", dlg.close)
    dlg.open()


def _frame(lp, key: str) -> dict:
    return next((f for f in lp.table.frames if f["key"] == key), {"key": key})


async def open_make_set(tab: KitchenTab) -> None:
    lp = tab.lp
    sp_name = tab.ctx.species_id
    st = {"name": f"{sp_name} {service.describe_draft(lp.pool.draft.model_dump())}"[:60], "level": None, "plan": None}
    with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 760px; max-width: 95vw; gap: 8px;"):
        ui.label("Make set").style(_TITLE)
        ui.label(
            "The kept rows become a particle set of this project, which Extract, Class 3D and Refine 3D can read. "
            "Nothing is written into the projects it reads from."
        ).classes(HINT_CLS)
        with ui.row().classes("w-full items-center gap-3 no-wrap"):
            house_text("name", value=st["name"], width="w-72", on_change=lambda e: st.update(name=e.value or ""))
            house_select(
                "level",
                LEVELS,
                value="auto",
                width="w-56",
                on_change=lambda e: (st.update(level=None if e.value == "auto" else e.value), refresh()),
            )
        body = ui.column().classes("w-full gap-1")

        async def compute() -> None:
            plan = await service.plan_set(lp, st["name"] or "pool set", st["level"])
            st["plan"] = plan
            body.clear()
            with body:
                ui.label(
                    f"{plan['n']:,} {plan['level']} rows in {plan['n_frames']} tomograms of {plan['n_projects']} "
                    f"project(s)"
                    + (
                        f"; {plan['n_before_dedup'] - plan['n']:,} clashes deduplicated"
                        if plan["n_before_dedup"] > plan["n"]
                        else ""
                    )
                ).style(_CELL)
                ui.label(" · ".join(f"{k}: {v:,}" for k, v in plan["per_project"].items())).style(_MONO)
                if plan["kept_by_hand"] or plan["dropped_by_hand"]:
                    ui.label(f"hand: +{plan['kept_by_hand']} kept, −{plan['dropped_by_hand']} dropped").classes(
                        HINT_CLS
                    )
                for b in plan["blockers"][:8]:
                    ui.label(b).classes(ERROR_CLS)
                if len(plan["blockers"]) > 8:
                    ui.label(f"and {len(plan['blockers']) - 8} more").classes(ERROR_CLS)
                for u in plan["unverified"] + plan["notes"]:
                    ui.label(u).style(_CELL)
            make_btn.set_enabled(not plan["blockers"] and plan["n"] > 0)

        def refresh() -> None:
            asyncio.get_running_loop().create_task(compute())

        async def do_make() -> None:
            plan = st["plan"]
            if plan is None or plan["blockers"]:
                return
            plan["name"] = st["name"] or plan["name"]
            dlg.close()
            ui.notify(f"Making set {plan['name']}: {plan['n']:,} rows…")
            try:
                ps = await service.make_set(lp, plan)
            except (OSError, ValueError, RuntimeError) as e:
                logger.exception("Kitchen: the set %s could not be made", plan["name"])
                ui.notify(f"The set could not be made: {e}", type="negative", multi_line=True)
                return
            ui.notify(f"Set {ps['id']} made: {ps['n_particles']:,} {ps['level']} particles", type="positive")

        with ui.row().classes("w-full items-center gap-2"):
            ui.element("div").style("flex: 1;")
            house_button("Cancel", dlg.close)
            make_btn = house_button("Make set", do_make, kind="accent")
    dlg.open()
    await compute()


async def open_ledger(tab: KitchenTab) -> None:
    lp = tab.lp
    rows = await asyncio.to_thread(service.ledger, lp)
    picked: list[dict] = []
    with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 920px; max-width: 95vw; gap: 8px;"):
        ui.label("Ledger").style(_TITLE)
        ui.label("The sets made from this pool, newest first. Tick two to compare them in the gallery.").classes(
            HINT_CLS
        )
        with ui.column().classes("w-full gap-2").style("max-height: 64vh; overflow-y: auto;"):
            if not rows:
                ui.label("No set made from this pool yet.").classes(HINT_CLS)
            for r in rows:
                with ui.column().classes("w-full gap-0"):
                    with ui.row().classes("w-full items-center no-wrap gap-2"):
                        ui.checkbox(on_change=lambda e, r=r: picked.append(r) if e.value else picked.remove(r)).props(
                            "dense size=xs"
                        )
                        ui.label(r["name"]).style("font-size: 11px; font-weight: 600; color: #1e293b;")
                        ui.label(f"{r['id']} · {r['created_at'][:16]} · {r['level']}").classes(HINT_CLS)
                        ui.label(f"{r['n']:,} in {r['n_tomograms']} tomograms").style(_MONO)
                        if r["stale"]:
                            ui.label("⚑ a source changed since").style("color: #b45309; font-size: 9px;").tooltip(
                                ", ".join(r["stale"])
                            )
                    with ui.column().classes("gap-0").style("padding-left: 22px;"):
                        ui.label(r["recipe"]).style(_CELL)
                        ui.label(" · ".join(f"{k}: {v:,}" for k, v in r["per_project"].items())).style(_MONO)
                        readers = " · ".join(f"{iid}: {h}" for iid, h in r["readers"]) or "no job reads it yet"
                        ui.label(readers).classes(HINT_CLS)

        async def compare() -> None:
            if len(picked) != 2:
                ui.notify("Tick exactly two sets to compare.", type="warning")
                return
            a, b = picked
            ua, ub = (set(await asyncio.to_thread(service.set_uids, x["uids"])) for x in (a, b))
            dlg.close()
            tab.view.show_uids(
                [
                    {"label": f"only in {a['name']}", "uids": sorted(ua - ub)},
                    {"label": f"only in {b['name']}", "uids": sorted(ub - ua)},
                ]
            )

        with ui.row().classes("w-full items-center gap-2"):
            house_button("Compare", compare)
            ui.element("div").style("flex: 1;")
            house_button("Close", dlg.close)
    dlg.open()

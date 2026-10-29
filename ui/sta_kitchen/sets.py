"""The kitchen's Make set dialog: the plan (`services/sta_kitchen/export.py`) shown before anything is written,
the choice of one frame for an acquisition seen in frames that cannot share picks (per acquisition, or
"prefer" a project's reconstruction for all of them), then the set made in a worker.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from nicegui import ui

from services.sta_kitchen import service
from ui.analytics.common import ERROR_CLS, HINT_CLS
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_select, house_text

if TYPE_CHECKING:
    from ui.sta_kitchen.tab import KitchenTab

logger = logging.getLogger(__name__)

_TITLE = "font-size: 13px; font-weight: 600; color: #0f172a;"
_CELL = "font-size: 10px; color: #334155;"
_MONO = "font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: #334155;"
LEVELS = {
    "auto": "as the rows are",
    "coordinates": "coordinates (extract later)",
    "extracted_only": "only the extracted rows",
}
# Conflicted acquisitions listed one by one; the "prefer" buttons cover the rest.
CONFLICTS_LISTED = 30


async def open_make_set(tab: KitchenTab) -> None:
    lp = tab.lp
    sp_name = tab.ctx.species_id
    recipe = service.describe_draft(lp.pool.draft.model_dump(), {x.id: x.name for x in lp.pool.subsets})
    st = {"name": f"{sp_name} {recipe}"[:60], "level": None, "plan": None}
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
        frames = ui.column().classes("w-full gap-1").style("max-height: 40vh; overflow-y: auto;")

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
                    f"{len(conflicts)} acquisitions were reconstructed in frames that cannot share picks (another "
                    f"alignment, geometry or hand); {open_} without a choice. A set takes one frame per acquisition."
                ).style(_CELL)
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
            ui.notify(f"{n} acquisition(s) now take their frame from {src}")
            tab._push()
            await compute()

        async def choose(it: dict) -> None:
            await service.choose_frame(lp, it["acq"], it["frame"])
            tab._push()
            await compute()

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

"""Sources: what feeds a pool, and adding more from this project or any other one found under the
recent project roots. Another project's species is matched by its lab-catalog id or chosen by hand,
never by name; its files are read, never written.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from nicegui import ui

from services.kitchen import service
from services.kitchen.members import MemberOffer, matching_species, member_key, offered_members
from services.kitchen.models import PoolMember
from services.kitchen.snapshots import ProjectView, project_view
from ui.analytics.common import ERROR_CLS, HINT_CLS
from ui.components.buttons import house_button, icon_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_select

if TYPE_CHECKING:
    from ui.kitchen.tab import KitchenTab

logger = logging.getLogger(__name__)

NOT_HERE = ""
_TITLE = "font-size: 13px; font-weight: 600; color: #0f172a;"
_CELL = "font-size: 10px; color: #334155;"
_MONO = "font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: #334155;"


async def open_sources(tab: KitchenTab) -> None:
    lp = tab.lp
    with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 860px; max-width: 95vw; gap: 8px;"):
        ui.label("Sources").style(_TITLE)
        ui.label(
            "Every source's picks are rows of the pool. Other projects are read, never written; their tables are "
            "cached in this project's Analytics/kitchen/."
        ).classes(HINT_CLS)
        box = ui.column().classes("w-full gap-1").style("max-height: 60vh; overflow-y: auto;")

        def render() -> None:
            box.clear()
            with box:
                if not lp.pool.members:
                    ui.label("No sources yet.").classes(HINT_CLS)
                with ui.element("div").style(
                    "display: grid; grid-template-columns: minmax(0, 1.2fr) minmax(0, 2fr) 70px minmax(0, 1fr) 24px; "
                    "gap: 2px 10px; align-items: center; width: 100%;"
                ):
                    for m in lp.pool.members:
                        st = lp.members.get(m.key)
                        ui.label(m.project_label).style(_CELL).classes("truncate")
                        ui.label(m.label or m.ref).style(_CELL).classes("truncate").tooltip(f"{m.species_id} · {m.key}")
                        ui.label(f"{st.n:,}" if st and st.state == "ok" else "–").style(_MONO)
                        state = (st.state if st else "pending") + (f": {st.error}" if st and st.error else "")
                        ui.label(state).classes(ERROR_CLS if st and st.state == "error" else HINT_CLS).classes(
                            "truncate"
                        ).tooltip(state)
                        icon_button(
                            "close", lambda _e, k=m.key: remove(k), tooltip="Remove from the pool", kind="danger"
                        )

        async def remove(key: str) -> None:
            await service.remove_member(lp, key)
            render()
            tab._push()

        async def add() -> None:
            await open_add_sources(tab, on_added=render)

        render()
        with ui.row().classes("w-full items-center gap-2"):
            house_button("Add sources…", add, tooltip="Candidate extractions and pick lists of this or another project")
            ui.element("div").style("flex: 1;")
            house_button("Close", dlg.close)
    dlg.open()


def _discover(home: Path, members: list[str]) -> list[Path]:
    """This project, the projects under the recent project roots, and those the pool already reads."""
    from services.aggregation.discovery import discover_pick_list_projects
    from services.configs.user_prefs_service import get_prefs_service

    roots = [r.path for r in get_prefs_service().prefs.recent_project_roots if r.path]
    out = [Path(home).resolve()]
    for p in [*discover_pick_list_projects(roots), *(Path(m).resolve() for m in members)]:
        if p not in out:
            out.append(p)
    return out


def _view_or_error(path: Path) -> ProjectView | str:
    try:
        return project_view(path)
    except (OSError, ValueError) as e:
        logger.warning("Kitchen: %s cannot be read: %s", path, e)
        return f"{type(e).__name__}: {e}"


async def open_add_sources(tab: KitchenTab, *, on_added) -> None:
    lp = tab.lp
    home_view = await asyncio.to_thread(project_view, tab.ctx.project_path)
    home_species = home_view.state.get_species(lp.species_id)
    catalog_id = getattr(home_species, "catalog_id", None)
    paths = await asyncio.to_thread(_discover, tab.ctx.project_path, [m.project_path for m in lp.pool.members])
    views = await asyncio.gather(*(asyncio.to_thread(_view_or_error, p) for p in paths))
    rows: list[dict] = []
    for path, view in zip(paths, views, strict=True):
        if isinstance(view, str):
            rows.append({"path": path, "view": None, "error": view, "species": NOT_HERE, "offers": [], "ticked": set()})
            continue
        sid = lp.species_id if path == home_view.path else (matching_species(view, catalog_id) or NOT_HERE)
        rows.append({"path": path, "view": view, "error": "", "species": sid, "offers": [], "ticked": set()})
    await asyncio.gather(*(_load_offers(r) for r in rows))

    with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 900px; max-width: 95vw; gap: 8px;"):
        ui.label("Add sources").style(_TITLE)
        ui.label(
            "Projects under the recent project roots. A species is matched by its lab-catalog id"
            + (f" ({catalog_id})" if catalog_id else ", and this one has none: choose it per project")
            + "; never by name."
        ).classes(HINT_CLS)
        body = ui.column().classes("w-full gap-2").style("max-height: 62vh; overflow-y: auto;")
        footer_count = {"label": None}

        def n_ticked() -> int:
            return sum(len(r["ticked"]) for r in rows)

        def render() -> None:
            body.clear()
            in_pool = {m.key for m in lp.pool.members}
            with body:
                for r in rows:
                    with ui.column().classes("w-full gap-0"):
                        with ui.row().classes("w-full items-center no-wrap gap-2"):
                            label = r["path"].name + (" (this project)" if r["path"] == home_view.path else "")
                            ui.label(label).style("font-size: 11px; font-weight: 600; color: #1e293b;")
                            if r["error"]:
                                ui.label(r["error"]).classes(ERROR_CLS).classes("truncate")
                                continue
                            options = {NOT_HERE: "not in this project"} | {
                                sp.id: sp.name or sp.id for sp in r["view"].state.species_registry
                            }
                            house_select(
                                "species",
                                options,
                                value=r["species"],
                                width="w-44",
                                on_change=lambda e, r=r: pick(r, e.value),
                            )
                        for o in r["offers"]:
                            key = member_key(r["path"], o.kind, o.ref)
                            with ui.row().classes("w-full items-center no-wrap gap-2").style("padding-left: 14px;"):
                                if key in in_pool:
                                    ui.label("in the pool").classes(HINT_CLS).style("width: 70px;")
                                else:
                                    ui.checkbox(
                                        value=key in r["ticked"], on_change=lambda e, r=r, k=key: tick(r, k, e.value)
                                    ).props("dense size=xs")
                                ui.label(o.label).style(_CELL)
                                ui.label(_counts(o)).style(_MONO)
            if footer_count["label"] is not None:
                footer_count["label"].set_text(f"{n_ticked()} ticked")

        def tick(r: dict, key: str, on: bool) -> None:
            (r["ticked"].add if on else r["ticked"].discard)(key)
            footer_count["label"].set_text(f"{n_ticked()} ticked")

        async def pick(r: dict, sid: str) -> None:
            r["species"], r["ticked"] = sid, set()
            await _load_offers(r)
            render()

        def tick_all() -> None:
            in_pool = {m.key for m in lp.pool.members}
            for r in rows:
                r["ticked"] = {member_key(r["path"], o.kind, o.ref) for o in r["offers"]} - in_pool
            render()

        async def add() -> None:
            members = []
            for r in rows:
                for o in r["offers"]:
                    key = member_key(r["path"], o.kind, o.ref)
                    if key in r["ticked"]:
                        members.append(
                            PoolMember(
                                key=key,
                                project_path=str(r["path"]),
                                project_label=r["path"].name,
                                species_id=r["species"],
                                kind=o.kind,
                                ref=o.ref,
                                label=o.label,
                            )
                        )
            dlg.close()
            if members:
                await service.add_members(lp, members)
                on_added()
                tab._push()

        render()
        with ui.row().classes("w-full items-center gap-2"):
            house_button("Tick every source shown", tick_all)
            footer_count["label"] = ui.label(f"{n_ticked()} ticked").classes(HINT_CLS)
            ui.element("div").style("flex: 1;")
            house_button("Cancel", dlg.close)
            house_button("Add", add, kind="accent")
    dlg.open()


async def _load_offers(r: dict) -> None:
    if r["view"] is None or not r["species"]:
        r["offers"] = []
        return
    try:
        r["offers"] = await asyncio.to_thread(offered_members, r["view"], r["species"])
    except (OSError, ValueError) as e:
        logger.exception("Kitchen: offers of %s could not be listed", r["path"])
        r["offers"], r["error"] = [], f"{type(e).__name__}: {e}"


def _counts(o: MemberOffer) -> str:
    parts = []
    if o.n_rows is not None:
        parts.append(f"{o.n_rows:,} picks")
    if o.n_tomos is not None:
        parts.append(f"{o.n_tomos} tomograms")
    if o.extracted:
        parts.append("extracted")
    return " · ".join(parts)

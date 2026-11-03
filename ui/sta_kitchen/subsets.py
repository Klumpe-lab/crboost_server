"""The kitchen's subset commands that need a word from the user: a name, a group, a confirmation. The
element asks (``subset_cmd`` events); the subsets themselves are `services/sta_kitchen/service.py`'s.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from nicegui import ui

from services.sta_kitchen import service
from ui.analytics.common import HINT_CLS
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_select, house_text

if TYPE_CHECKING:
    from ui.sta_kitchen.tab import KitchenTab

logger = logging.getLogger(__name__)

_TITLE = "font-size: 13px; font-weight: 600; color: #0f172a;"
NO_GROUP = ""


def _ask(title: str, hint: str, value: str, on_ok, *, groups: dict[str, str] | None = None, group: str = "") -> None:
    """A small dialog: a name (and, with `groups`, a group), then `on_ok(name, group)`."""
    st = {"name": value, "group": group}
    with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 420px; gap: 8px;"):
        ui.label(title).style(_TITLE)
        if hint:
            ui.label(hint).classes(HINT_CLS)
        field = house_text("name", value=value, width="w-64", on_change=lambda e: st.update(name=e.value or ""))
        if groups is not None:
            house_select(
                "group",
                {NO_GROUP: "none (may overlap)"} | groups,
                value=group,
                width="w-64",
                hint="Inside a group its subsets never share a pick: adding moves",
                on_change=lambda e: st.update(group=e.value or ""),
            )

        async def ok() -> None:
            dlg.close()
            await on_ok(st["name"], st["group"] or None)

        field.on("keydown.enter", ok)
        with ui.row().classes("w-full items-center gap-2"):
            ui.element("div").style("flex: 1;")
            house_button("Cancel", dlg.close)
            house_button("OK", ok, kind="accent")
    dlg.open()


async def _make_set_command(tab: KitchenTab, cmd: str, sid, gid, uids: list[str]) -> None:
    from services.sta_kitchen.models import Scope
    from ui.sta_kitchen.sets import open_make_set, open_make_sets

    lp = tab.lp
    if lp.table is None:
        ui.notify("The pool has no table yet", type="warning")
        return
    if cmd == "make_set" and lp.pool.subset(sid):
        await open_make_set(tab, scope=Scope(include=[sid]), name=lp.pool.subset(sid).name)
    elif cmd == "make_sets" and lp.pool.group(gid):
        await open_make_sets(tab, gid)
    elif cmd == "make_set_selection" and uids:
        await open_make_set(tab, selection=uids, name=f"selection of {len(uids):,}")


def _confirm(title: str, text: str, on_ok) -> None:
    with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 420px; gap: 8px;"):
        ui.label(title).style(_TITLE)
        ui.label(text).classes(HINT_CLS)

        async def ok() -> None:
            dlg.close()
            await on_ok()

        with ui.row().classes("w-full items-center gap-2"):
            ui.element("div").style("flex: 1;")
            house_button("Cancel", dlg.close)
            house_button("Delete", ok, kind="danger")
    dlg.open()


async def run_command(tab: KitchenTab, args: dict) -> None:
    """One ``subset_cmd`` from the element: new, new_passing, rename, color, group, delete, new_group,
    rename_group, delete_group, and Make set from a subset (make_set), from each subset of a group (make_sets)
    or from the selection (make_set_selection)."""
    lp = tab.lp
    if lp is None:
        return
    cmd, sid, gid = args.get("cmd"), args.get("id"), args.get("group")
    groups = {g.id: g.name for g in lp.pool.groups}
    if cmd in ("make_set", "make_sets", "make_set_selection"):
        await _make_set_command(tab, cmd, sid, gid, [str(u) for u in args.get("uids") or []])
        return

    async def notify(error: str) -> None:
        if error:
            ui.notify(error, type="warning", multi_line=True)
        tab.push_subsets()

    if cmd in ("new", "new_passing"):
        uids = [str(u) for u in args.get("uids") or []]
        if cmd == "new_passing":
            sel = await service.selection(lp)
            uids = [lp.table.uid_hex(int(r)) for r in sel.final.nonzero()[0]] if sel is not None else []
        n = len(uids)

        async def make(name: str, group: str | None) -> None:
            await service.create_subset(lp, name, uids, group)
            tab.push_subsets()

        what = f"{n:,} picks" if n else "no picks yet (add them later)"
        _ask("New subset", f"Holds {what}.", f"subset {len(lp.pool.subsets) + 1}", make, groups=groups, group=gid or "")
    elif cmd == "rename" and lp.pool.subset(sid):

        async def rename(name: str, _g) -> None:
            await notify(await service.update_subset(lp, sid, name=name))

        _ask("Rename subset", "", lp.pool.subset(sid).name, rename)
    elif cmd == "color" and lp.pool.subset(sid):
        await notify(await service.update_subset(lp, sid, color=str(args.get("color") or "")))
    elif cmd == "group" and lp.pool.subset(sid):
        if gid == "new":

            async def new_group(name: str, _g) -> None:
                g = await service.create_group(lp, name)
                await notify(await service.update_subset(lp, sid, group=g.id))

            _ask(
                "New group",
                "Its subsets never share a pick: adding a pick to one takes it out of the others.",
                f"group {len(lp.pool.groups) + 1}",
                new_group,
            )
        else:
            await notify(await service.update_subset(lp, sid, group=gid or None))
    elif cmd == "delete" and lp.pool.subset(sid):
        sub = lp.pool.subset(sid)

        async def delete() -> None:
            await service.delete_subset(lp, sid)
            tab.push_subsets()
            tab.push_draft()

        _confirm(
            "Delete subset",
            f"{sub.name} goes, with the membership of its {len(lp.subset_uids.get(sid, ())):,} picks; "
            "the picks stay in the pool.",
            delete,
        )
    elif cmd == "new_group":

        async def make_group(name: str, _g) -> None:
            await service.create_group(lp, name)
            tab.push_subsets()

        _ask(
            "New group",
            "Its subsets never share a pick: adding a pick to one takes it out of the others.",
            f"group {len(lp.pool.groups) + 1}",
            make_group,
        )
    elif cmd == "rename_group" and lp.pool.group(gid):

        async def rename_group(name: str, _g) -> None:
            await service.rename_group(lp, gid, name)
            tab.push_subsets()

        _ask("Rename group", "", lp.pool.group(gid).name, rename_group)
    elif cmd == "delete_group" and lp.pool.group(gid):

        async def delete_group() -> None:
            await service.delete_group(lp, gid)
            tab.push_subsets()

        _confirm(
            "Delete group", f"{lp.pool.group(gid).name} goes; its subsets stay, and may overlap again.", delete_group
        )

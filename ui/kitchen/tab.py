"""The STA kitchen tab of the Particles registry: one species' picks from every source, in one pool.

The tab owns two header rows of house controls and one `KitchenView`; everything per pick happens in
the browser (``kitchen.js``). The pool itself is `services/kitchen/service.py`'s, shared with any other
browser tab on the same species. While the tab is off screen its timers do nothing (`ViewScope`).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from urllib.parse import quote

from nicegui import ui

from services.kitchen import service
from services.kitchen.models import MAX_TAGS, Draft, Tag, Verdict
from services.project_state import get_project_state_for
from ui.components.buttons import house_button, icon_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_select, house_text
from ui.components.reactive import SingleFlight, ViewScope
from ui.components.segmented import render_segmented
from ui.dashboard.css import ensure_assets_loaded
from ui.kitchen import routes  # noqa: F401  (registers the kitchen's routes)
from ui.kitchen.view import KitchenView
from ui.species.tab import TabContext

logger = logging.getLogger(__name__)

SORTS = {
    "sigma": "σ",
    "depth": "depth",
    "nn": "nearest neighbour",
    "tomogram": "tomogram",
    "random": "random",
    "cut": "distance to the σ cut",
}
GROUPS = {"none": "none", "tomogram": "tomogram", "project": "project", "sigma": "σ bin", "tag": "tag"}
ROW_ORDERS = {"passing": "most passing", "ctf": "worst CTF fit", "project": "project", "name": "name"}
OBSERVE_S = 3.0
DISK_S = 15.0


class KitchenTab:
    def __init__(self, ctx: TabContext) -> None:
        self.ctx = ctx
        self.lp: service.LoadedPool | None = None
        self.view: KitchenView | None = None
        self.scope = ViewScope(active=False)
        self.prefs = {
            "mode": "pool",
            "size": "M",
            "sort": "sigma",
            "group": "none",
            "curating": False,
            "row_order": "passing",
        }
        self._rev = -1
        self._version = ""
        self._opening: asyncio.Task | None = None
        self._flight = SingleFlight()
        self._slot = None
        self._refs: dict[str, ui.element] = {}
        self._segs: dict = {}
        self._doctor_task: asyncio.Task | None = None
        self._avg_task: asyncio.Task | None = None
        self._avg_again = False

    # ── Tab contract ───────────────────────────────────────────────────────────────────────────

    def build(self, container: ui.element) -> None:
        ensure_assets_loaded()
        species = get_project_state_for(self.ctx.project_path).get_species(self.ctx.species_id)
        color = getattr(species, "color", "") or "#3b82f6"
        name = getattr(species, "name", "") or self.ctx.species_id
        with container:
            with (
                ui.element("div")
                .classes("cbk-tab")
                .style(
                    "display: flex; flex-direction: column; width: 100%; height: 100%; min-height: 0; gap: 6px; "
                    "padding: 0 10px 10px 10px;"
                )
            ):
                with ui.row().classes("items-center no-wrap w-full gap-2").style("min-height: 22px;"):
                    icon_button("view_sidebar", self._toggle_rail, tooltip="Show or hide the species list")
                    ui.label(f"STA kitchen · {name}").style("font-size: 12px; font-weight: 600; color: #0f172a;")
                    self._refs["state"] = ui.label("").style("font-size: 9px; color: #64748b;")
                    ui.element("div").style("flex: 1;")
                    house_button("Tags", self._open_tags, tooltip="Tags particles can carry (keys 1–9 while curating)")
                    house_button("Sources", self._open_sources, tooltip="What feeds this pool, and adding more")
                    self._refs["doctor"] = house_button(
                        "Doctor", self._open_doctor, tooltip="What in this pool needs a look; drops nothing"
                    )
                    house_button("Ledger", self._open_ledger, tooltip="The sets made from this pool, and their readers")
                    house_button("Make set…", self._open_make_set, kind="accent")
                with ui.row().classes("items-center no-wrap w-full gap-3").style("min-height: 20px;"):
                    self._segs["mode"] = render_segmented(
                        (("pool", "Pool"), ("tomo", "By tomogram")), self.prefs["mode"], self._set_mode
                    )
                    self._segs["size"] = render_segmented(
                        (("S", "S"), ("M", "M"), ("L", "L")), self.prefs["size"], self._set_size
                    )
                    self._segs["curate"] = render_segmented(
                        (("browse", "Browse"), ("curate", "Curate")), "browse", self._set_curating
                    )
                    house_select(
                        "sort",
                        SORTS,
                        value=self.prefs["sort"],
                        width="w-36",
                        on_change=lambda e: self._set("sort", e.value),
                    )
                    house_select(
                        "group",
                        GROUPS,
                        value=self.prefs["group"],
                        width="w-28",
                        on_change=lambda e: self._set("group", e.value),
                    )
                    house_select(
                        "rows",
                        ROW_ORDERS,
                        value=self.prefs["row_order"],
                        width="w-32",
                        hint="By tomogram: the order of the rows",
                        on_change=lambda e: self._set("row_order", e.value),
                    )
                self.view = KitchenView(species_color=color, tile_url="", slab_url="", view=self._view_props()).style(
                    "flex: 1 1 0%; min-height: 0; width: 100%;"
                )
        self.view.on("loaded", self._on_loaded)
        self.view.on("need_tiles", self._on_need_tiles)
        self.view.on("draft", self._on_draft)
        self.view.on("verdict", self._on_verdict)
        self.view.on("tag", self._on_tag)
        self.view.on("mode", lambda e: self._set_mode((e.args or {}).get("mode", "pool")))
        self._slot = ui.context.slot
        self.scope.timer(OBSERVE_S, self._observe, self.view)
        self.scope.timer(DISK_S, self._disk_tick, self.view)

    def refresh(self) -> None:
        if self.scope.active:
            self._observe()

    def set_visible(self, on: bool) -> None:
        self.scope.set_active(on)
        if on and self.lp is None and self._opening is None:
            self._opening = asyncio.get_running_loop().create_task(self._open())

    # ── Pool ───────────────────────────────────────────────────────────────────────────────────

    def _q(self) -> str:
        return f"home={quote(str(self.ctx.project_path))}&species={quote(self.ctx.species_id)}"

    def _view_props(self) -> dict:
        return {k: self.prefs[k] for k in ("mode", "size", "sort", "group", "curating", "row_order")}

    async def _open(self) -> None:
        self._set_state("opening the pool…")
        try:
            self.lp = await service.open_pool(self.ctx.project_path, self.ctx.species_id)
            self._set_state("reading the members' tables…")
            await service.rebuild(self.lp)
        except (OSError, ValueError) as e:
            logger.exception("Kitchen pool %s/%s could not be opened", self.ctx.project_path.name, self.ctx.species_id)
            self._set_state(f"the pool could not be opened: {e}")
            return
        finally:
            self._opening = None
        self._push()

    def _set_state(self, text: str) -> None:
        label = self._refs.get("state")
        if label is not None and label.text != text:
            label.set_text(text)

    def _push(self) -> None:
        """Send what moved since the last push: the table's URLs, the draft, the frame choice."""
        lp, view = self.lp, self.view
        if lp is None or view is None or view.is_deleted:
            return
        self._rev = lp.rev
        building = [k for k, s in lp.members.items() if s.state == "building"]
        errors = [k for k, s in lp.members.items() if s.state == "error"]
        state = []
        if building:
            state.append(f"building {len(building)} member table(s)…")
        if errors:
            state.append(f"{len(errors)} member(s) unreadable: see Sources")
        if not lp.pool.members:
            state.append("no sources yet")
        self._set_state(" · ".join(state))
        if lp.table is None:
            return
        settings = hashlib.sha1(f"{lp.pool.box_a}|{lp.pool.slab_a}".encode()).hexdigest()[:8]
        version = lp.table.version
        props = {
            "tile_url": f"/api/kitchen/tile?{self._q()}&v={settings}&uid=",
            "slab_url": f"/api/kitchen/slab?{self._q()}&v={version}&frame={{frame}}&which={{which}}",
            "frame_choice": dict(lp.pool.frame_choice),
            "tag_defs": [t.model_dump() for t in lp.pool.tags],
        }
        if self._doctor_task is None or self._doctor_task.done():
            self._doctor_task = asyncio.get_running_loop().create_task(self._count_markers())
        if version != self._version:
            self._version = version
            self._schedule_averages()
            props["draft"] = lp.pool.draft.model_dump()
            props["table_url"] = f"/api/kitchen/table?{self._q()}&v={version}"
        view.set_props(**props)

    def _observe(self) -> None:
        if self.lp is not None and self.lp.rev != self._rev:
            self._push()

    async def _disk_tick(self) -> None:
        if self.lp is None or self.lp.lock.locked():
            return
        if await service.rebuild(self.lp):
            self._push()

    # ── Events from the element ────────────────────────────────────────────────────────────────

    def _on_loaded(self, _e) -> None:
        if self.lp is not None and self.view is not None:
            self.view.apply_verdicts(service.verdict_map(self.lp))
            self.view.apply_tags(dict(self.lp.tags))

    async def _on_need_tiles(self, e) -> None:
        if self.lp is not None:
            await service.prefetch_tiles(self.lp, list((e.args or {}).get("uids") or [])[:600])

    async def _on_draft(self, e) -> None:
        """A released filter: kept as the pool's draft, then counted here as the authority."""
        if self.lp is None:
            return
        args = e.args or {}
        try:
            draft = Draft.model_validate(args.get("draft") or {})
        except ValueError:
            logger.exception("Kitchen: the browser sent a draft that does not parse")
            return
        await service.set_draft(self.lp, draft)
        self._rev = self.lp.rev  # the browser already shows this draft
        self._schedule_averages()
        sel = await service.selection(self.lp)
        if sel is not None and args.get("n") is not None and int(args["n"]) != sel.n:
            logger.warning(
                "Kitchen %s/%s: the browser counts %s kept, the server %d, for one draft",
                self.ctx.project_path.name,
                self.ctx.species_id,
                args["n"],
                sel.n,
            )

    async def _on_verdict(self, e) -> None:
        args = e.args or {}
        if self.lp is not None and args.get("uids"):
            uids = [str(u) for u in args["uids"]]
            await service.record_verdicts(self.lp, uids, Verdict(args.get("value", "clear")))
            self._rev = self.lp.rev

    async def _on_tag(self, e) -> None:
        args = e.args or {}
        if self.lp is not None and args.get("uids"):
            uids = [str(u) for u in args["uids"]]
            await service.record_tags(self.lp, uids, int(args["tag"]), bool(args.get("on")))
            self._rev = self.lp.rev

    # ── Controls ───────────────────────────────────────────────────────────────────────────────

    def _set(self, key: str, value) -> None:
        self.prefs[key] = value
        if self.view is not None:
            self.view.set_props(**self._view_props())

    def _set_mode(self, key: str) -> None:
        self._segs["mode"].set_active(key)
        self._set("mode", key)

    def _set_size(self, key: str) -> None:
        self._segs["size"].set_active(key)
        self._set("size", key)

    def _set_curating(self, key: str) -> None:
        self._segs["curate"].set_active(key)
        self._set("curating", key == "curate")

    async def _open_tags(self) -> None:
        async with self._flight("tags") as acquired:
            if not acquired or self.lp is None:
                return
            await self._tags_dialog()

    async def _tags_dialog(self) -> None:
        lp = self.lp
        before = [t.model_copy() for t in lp.pool.tags]

        def cancel() -> None:
            lp.pool.tags = before
            dlg.close()

        with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 420px; max-width: 95vw; gap: 8px;"):
            ui.label("Tags").style("font-size: 13px; font-weight: 600; color: #0f172a;")
            ui.label("A tag belongs to the particle. While curating, keys 1–9 toggle the tag with that key.").classes(
                "text-[10px] text-gray-400"
            )
            box = ui.column().classes("w-full gap-1")

            def render() -> None:
                box.clear()
                with box:
                    for t in lp.pool.tags:
                        with ui.row().classes("items-center gap-2 no-wrap"):
                            key = f"key {t.key}" if t.key else "no key"
                            ui.label(key).style("font-size: 9px; color: #64748b; width: 40px;")
                            house_text("", value=t.name, width="w-48", on_change=lambda e, t=t: rename(t, e.value))
                            icon_button("close", lambda _e, t=t: remove(t), tooltip="Remove this tag", kind="danger")

            def rename(t: Tag, name: str) -> None:
                if name:
                    t.name = name

            def remove(t: Tag) -> None:
                lp.pool.tags = [x for x in lp.pool.tags if x.id != t.id]
                render()

            def add() -> None:
                used = {t.id for t in lp.pool.tags}
                free = next((i for i in range(MAX_TAGS) if i not in used), None)
                if free is None:
                    ui.notify(f"A pool has at most {MAX_TAGS} tags.", type="warning")
                    return
                keys = {t.key for t in lp.pool.tags}
                key = next((k for k in range(1, 10) if k not in keys), None)
                lp.pool.tags.append(Tag(id=free, name=f"tag {free + 1}", key=key))
                render()

            async def save() -> None:
                dlg.close()
                await service.save_pool(lp)
                self._push()

            render()
            with ui.row().classes("w-full items-center gap-2"):
                house_button("Add tag", add)
                ui.element("div").style("flex: 1;")
                house_button("Cancel", cancel)
                house_button("Save", save, kind="accent")
        dlg.open()

    def _toggle_rail(self) -> None:
        toggle = self.ctx.callbacks.get("species_toggle_rail")
        if toggle is not None:
            toggle()

    async def _open_sources(self) -> None:
        async with self._flight("sources") as acquired:
            if not acquired or self.lp is None:
                return
            from ui.kitchen.sources import open_sources

            await open_sources(self)

    async def _open_doctor(self) -> None:
        async with self._flight("doctor") as acquired:
            if acquired and self.lp is not None and self.lp.table is not None:
                from ui.kitchen.sets import open_doctor

                await open_doctor(self)

    async def _open_ledger(self) -> None:
        async with self._flight("ledger") as acquired:
            if acquired and self.lp is not None:
                from ui.kitchen.sets import open_ledger

                await open_ledger(self)

    async def _open_make_set(self) -> None:
        async with self._flight("make_set") as acquired:
            if acquired and self.lp is not None and self.lp.table is not None:
                from ui.kitchen.sets import open_make_set

                await open_make_set(self)

    def _schedule_averages(self) -> None:
        if self._avg_task is not None and not self._avg_task.done():
            self._avg_again = True
            return
        self._avg_task = asyncio.get_running_loop().create_task(self._refresh_averages())

    async def _refresh_averages(self) -> None:
        """The kept and removed means after a release; one at a time, the last request wins."""
        while True:
            self._avg_again = False
            try:
                res = await service.averages(self.lp)
            except (OSError, ValueError, RuntimeError) as e:
                logger.warning("Kitchen: the averages could not be made: %s", e)
                return
            self.lp.averages = res
            if not self._avg_again:
                break
        if res is None or self.view is None or self.view.is_deleted:
            return
        props = {}
        for which, entry in res.items():
            url = (
                f"/api/kitchen/average?{self._q()}&which={which}&v={entry.get('digest', '')}"
                if entry.get("file")
                else ""
            )
            props[which] = {"url": url, "n": entry.get("n", 0), "of": entry.get("of", 0)}
        self.view.set_props(averages=props)

    async def _count_markers(self) -> None:
        """The Doctor button's count: markers that block or warn."""
        try:
            markers = await service.diagnose(self.lp)
        except (OSError, ValueError) as e:
            logger.warning("Kitchen: the Doctor could not run: %s", e)
            return
        n = sum(1 for m in markers if m.severity in ("block", "warn"))
        btn = self._refs.get("doctor")
        if btn is not None and not btn.is_deleted:
            btn.set_text(f"Doctor ⚑{n}" if n else "Doctor")

    async def set_draft(self, draft: Draft) -> None:
        if self.lp is not None:
            await service.set_draft(self.lp, draft)

"""The STA kitchen tab of the Particles registry: one species' picks from every source, in one pool.

The tab owns one header row of house controls and one `KitchenView`; everything per pick happens in
the browser (``sta_kitchen.js``). The pool itself is `services/sta_kitchen/service.py`'s, shared with any other
browser tab on the same species. While the tab is off screen its timers do nothing (`ViewScope`).
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import quote

from nicegui import ui

from services.sta_kitchen import service
from services.sta_kitchen.models import Draft, Verdict
from services.project_state import get_project_state_for
from ui.components.buttons import house_button, icon_button
from ui.components.fields import house_select
from ui.components.reactive import SingleFlight, ViewScope
from ui.components.segmented import render_segmented
from ui.dashboard.css import ensure_assets_loaded
from ui.sta_kitchen import routes  # noqa: F401  (registers the kitchen's routes)
from ui.sta_kitchen.view import KitchenView
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
GROUPS = {"none": "none", "tomogram": "tomogram", "project": "project", "sigma": "σ bin"}
ROW_ORDERS = {"passing": "most passing", "ctf": "worst CTF fit", "project": "project", "name": "name"}
# What a cutout's corner shows: a column of the pool table, in its own unit.
METRICS = {
    "none": "nothing",
    "sigma": "σ",
    "margin": "σ above FP=1",
    "lcc": "LCC",
    "rank": "rank",
    "depth_a": "depth",
    "nn_a": "nearest neighbour",
    "face_a": "to a face",
    "z_pct": "height in volume",
    "ctf_res": "CTF fit",
    "n_tilts": "tilts",
}
# What a tile shows: a cutout of the pick's reconstruction, or its extracted subtomogram where one exists.
PICTURES = {"recon": "tomogram cutouts", "ext": "extracted subtomograms"}
PICTURES_TIP = (
    "Extracted subtomograms: each pick's subtomogram from the extraction (or Warp export) jobs of its "
    "candidates, the mean of its visible tilts, as the Journey shows them, at the extraction's own box; "
    "a pick nobody extracted says so"
)
MEANS_TIP = (
    "Unaligned means of up to 200 kept and 200 removed cutouts (a fixed random sample): does the cut keep "
    "particles and remove noise? Made only when you ask, because a first pair cuts up to 400 cutouts from "
    "their reconstructions (about half a minute); a new cut leaves them up, marked as made for an earlier cut."
)
# How the picks show on a card's slabs (By tomogram): the mark, its size, and what becomes of the picks the
# recipe does not keep and of those dropped by hand.
DOT_CHOICES = {
    "shape": ("mark", (("dot", "●"), ("ring", "○"), ("cross", "+"), ("square", "■"))),
    "size": ("size", (("S", "S"), ("M", "M"), ("L", "L"))),
    "others": ("not kept", (("hollow", "hollow"), ("faint", "faint"), ("hidden", "hidden"))),
    "dropped": ("dropped by hand", (("cross", "red ×"), ("hollow", "as not kept"), ("hidden", "hidden"))),
    "color": ("colour", (("species", "species"), ("subset", "first subset"))),
}
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
            "size": "S",
            "sort": "sigma",
            "group": "none",
            "metric": "sigma",
            "curating": False,
            "row_order": "passing",
            "pictures": "recon",
            "dots": {"shape": "dot", "size": "M", "others": "hollow", "dropped": "cross", "color": "species"},
        }
        self._rev = -1
        self._subset_rev = -1  # the subsets' memberships the browser holds
        self._version = ""
        self._opening: asyncio.Task | None = None
        self._flight = SingleFlight()
        self._refs: dict[str, ui.element] = {}
        self._segs: dict = {}
        self._means_draft: str | None = None  # the draft the shown means were made for
        self._sets_task: asyncio.Task | None = None

    # ── Tab contract ───────────────────────────────────────────────────────────────────────────

    def build(self, container: ui.element) -> None:
        ensure_assets_loaded()
        species = get_project_state_for(self.ctx.project_path).get_species(self.ctx.species_id)
        color = getattr(species, "color", "") or "#3b82f6"
        diameter = getattr(species, "diameter_ang", None)
        with container:
            with (
                ui.element("div")
                .classes("cbk-tab")
                .style(
                    "display: flex; flex-direction: column; width: 100%; height: 100%; min-height: 0; gap: 6px; "
                    "padding: 0 10px 10px 10px;"
                )
            ):
                with ui.row().classes("items-center w-full gap-2").style("min-height: 22px; row-gap: 4px;"):
                    icon_button("view_sidebar", self._toggle_rail, tooltip="Show or hide the species list")
                    self._segs["mode"] = render_segmented(
                        (("pool", "Pool"), ("tomo", "By tomogram"), ("review", "Review")),
                        self.prefs["mode"],
                        self._set_mode,
                    )
                    self._segs["size"] = render_segmented(
                        (("S", "S"), ("M", "M"), ("L", "L"), ("XL", "XL")), self.prefs["size"], self._set_size
                    )
                    self._segs["curate"] = render_segmented(
                        (("browse", "Select"), ("curate", "Paint")), "browse", self._set_curating
                    )
                    house_select(
                        "sort",
                        SORTS,
                        value=self.prefs["sort"],
                        width="w-36",
                        on_change=lambda e: self._set("sort", e.value),
                    )
                    with ui.element("div") as group_box:
                        house_select(
                            "group",
                            GROUPS,
                            value=self.prefs["group"],
                            width="w-28",
                            on_change=lambda e: self._set("group", e.value),
                        )
                    with ui.element("div") as rows_box:
                        house_select(
                            "rows",
                            ROW_ORDERS,
                            value=self.prefs["row_order"],
                            width="w-32",
                            hint="The order of the tomograms",
                            on_change=lambda e: self._set("row_order", e.value),
                        )
                    house_select(
                        "metric",
                        METRICS,
                        value=self.prefs["metric"],
                        width="w-32",
                        hint="The number each cutout shows in its corner",
                        on_change=lambda e: self._set("metric", e.value),
                    )
                    house_select(
                        "pictures",
                        PICTURES,
                        value=self.prefs["pictures"],
                        width="w-36",
                        hint=PICTURES_TIP,
                        on_change=lambda e: self._set_pictures(e.value),
                    )
                    with ui.element("div") as dots_box:
                        self._build_dots()
                    self._refs["group_box"], self._refs["rows_box"] = group_box, rows_box
                    self._refs["dots_box"] = dots_box
                    self._refs["state"] = ui.label("").style("font-size: 9px; color: #64748b;")
                    ui.element("div").style("flex: 1;")
                    self._build_means()
                    house_button("Sources", self._open_sources, tooltip="What feeds this pool, and adding more")
                    house_button("Make set…", self._open_make_set, kind="accent")
                self.view = KitchenView(species_color=color, diameter_a=diameter, view=self._view_props()).style(
                    "flex: 1 1 0%; min-height: 0; width: 100%;"
                )
        self._show_mode_controls()
        self.view.on("loaded", self._on_loaded)
        self.view.on("draft", self._on_draft)
        self.view.on("verdict", self._on_verdict)
        self.view.on("review", self._on_review)
        self.view.on("subset", self._on_subset)
        self.view.on("subset_cmd", self._on_subset_cmd)
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
        keys = ("mode", "size", "sort", "group", "metric", "curating", "row_order", "dots")
        return {k: self.prefs[k] for k in keys}

    async def _open(self) -> None:
        self._set_state("opening the pool…")
        try:
            self.lp = await service.open_pool(self.ctx.project_path, self.ctx.species_id)
            self._set_state("reading the members' tables…")
            await service.rebuild(self.lp)
            await service.ensure_set_masks(self.lp)
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
        version = lp.table.version
        props = {
            "tiles_url": f"/api/kitchen/tiles?{self._q()}&v={service.tile_settings(lp)}&src={self.prefs['pictures']}",
            "slab_url": f"/api/kitchen/slab?{self._q()}&v={version}&frame={{frame}}&which={{which}}",
            "frame_choice": dict(lp.pool.frame_choice),
            "set_masks": service.set_mask_payload(lp),
            "review": lp.review,
            "loupe_url": f"/api/kitchen/loupe?{self._q()}&uid={{uid}}",
            "loupe_png_url": f"/api/kitchen/loupe.png?{self._q()}&name={{name}}",
            "source_choice": dict(lp.pool.source_choice),
            **service.subset_meta(lp),
        }
        if self._sets_task is None:
            self._sets_task = asyncio.get_running_loop().create_task(self._refresh_sets())
        if version != self._version:
            self._version = version
            props["draft"] = lp.pool.draft.model_dump()
            props["table_url"] = f"/api/kitchen/table?{self._q()}&v={version}"
        view.set_props(**props)
        if self._subset_rev != lp.subset_rev:
            self._subset_rev = lp.subset_rev
            view.apply_subsets(service.subset_lists(lp), True)

    def push_subsets(self) -> None:
        """After a subset command: names, colours and groups, and the memberships when they moved."""
        self._push()

    def push_draft(self) -> None:
        """The pool's draft, after a change made here (Sources, a deleted subset): the browser takes it whole."""
        if self.lp is not None and self.view is not None and not self.view.is_deleted:
            self.view.send_draft(self.lp.pool.draft.model_dump())
            self._push()

    def _observe(self) -> None:
        if self.lp is not None and self.lp.rev != self._rev:
            self._push()

    async def _disk_tick(self) -> None:
        if self.lp is None or self.lp.lock.locked():
            return
        rebuilt = await service.rebuild(self.lp)
        if await service.ensure_set_masks(self.lp) or rebuilt:
            self._push()

    async def _refresh_sets(self) -> None:
        """The home project's particle sets of the species, for "+ filter" (a set made since shows up), and per
        subset the sets made in it and how much it changed since the newest (the rail's marks)."""
        try:
            lp = self.lp
            if lp is not None and self.view is not None and not self.view.is_deleted:
                from services.sta_kitchen.setmatch import pool_sets

                sets = await asyncio.to_thread(pool_sets, lp.home, lp.species_id)
                held = {k: set(v) for k, v in lp.subset_uids.items()}
                marks = await asyncio.to_thread(service.subset_set_marks, lp, held)
                if not self.view.is_deleted:
                    self.view.set_props(sets=sets, subset_sets=marks)
        finally:
            self._sets_task = None

    # ── Events from the element ────────────────────────────────────────────────────────────────

    def _on_loaded(self, _e) -> None:
        if self.lp is not None and self.view is not None:
            self.view.apply_verdicts(service.verdict_map(self.lp))
            self._subset_rev = self.lp.subset_rev
            self.view.apply_subsets(service.subset_lists(self.lp), True)

    async def _on_subset(self, e) -> None:
        """Picks added to or taken out of a subset in the browser, which already shows it."""
        args = e.args or {}
        if self.lp is None or not args.get("uids"):
            return
        uids = [str(u) for u in args["uids"]]
        error = await service.edit_subset(self.lp, str(args.get("op")), str(args.get("id")), uids)
        if error:
            ui.notify(error, type="warning")
            self._subset_rev = -1  # the browser's memberships are wrong: send them again
        else:
            self._subset_rev = self.lp.subset_rev
        self._rev = -1
        self._observe()

    async def _on_subset_cmd(self, e) -> None:
        from ui.sta_kitchen.subsets import run_command

        if isinstance(e.args, dict):
            await run_command(self, e.args)

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
        self._mark_means()
        sel = await service.selection(self.lp)  # matches the sets a new set filter names
        if self.view is not None and not self.view.is_deleted:
            self.view.set_props(set_masks=service.set_mask_payload(self.lp))
        # A count made while a set's match was still on its way is not compared.
        if sel is not None and args.get("n") is not None and not args.get("pending") and int(args["n"]) != sel.n:
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
            self._mark_means()

    async def _on_review(self, e) -> None:
        """The review sample and its labels, as the browser holds them (only ever a few hundred uids)."""
        review = e.args
        if self.lp is None or not isinstance(review, dict) or not isinstance(review.get("sample"), dict):
            return
        await service.save_review(self.lp, review)
        self._rev = self.lp.rev  # the browser already shows it

    # ── Controls ───────────────────────────────────────────────────────────────────────────────

    def _set(self, key: str, value) -> None:
        self.prefs[key] = value
        if self.view is not None:
            self.view.set_props(**self._view_props())

    def _set_pictures(self, key: str) -> None:
        self.prefs["pictures"] = key
        self._push()  # a new tiles URL: the browser asks for the other pictures

    def _set_mode(self, key: str) -> None:
        self._segs["mode"].set_active(key)
        self._set("mode", key)
        self._show_mode_controls()

    def _show_mode_controls(self) -> None:
        """Group orders the gallery, rows the by-tomogram stream: each shows with its own mode only."""
        mode = self.prefs["mode"]
        self._refs["group_box"].set_visibility(mode == "pool")
        self._refs["rows_box"].set_visibility(mode == "tomo")
        self._refs["dots_box"].set_visibility(mode == "tomo")

    def _set_size(self, key: str) -> None:
        self._segs["size"].set_active(key)
        self._set("size", key)

    def _set_curating(self, key: str) -> None:
        self._segs["curate"].set_active(key)
        self._set("curating", key == "curate")

    def _build_dots(self) -> None:
        """The Dots button: how the picks show on the cards' slabs."""
        btn = house_button("Dots", tooltip="How the picks show on the tomogram slabs")
        with btn, ui.menu().props("anchor='bottom left' self='top left'"):
            with ui.element("div").style(
                "display: grid; grid-template-columns: auto auto; gap: 6px 10px; align-items: center; padding: 10px;"
            ):
                for key, (label, choices) in DOT_CHOICES.items():
                    ui.label(label).style("font-size: 9px; color: #475569; white-space: nowrap;")
                    self._segs[f"dots_{key}"] = render_segmented(
                        choices, self.prefs["dots"][key], lambda v, k=key: self._set_dot(k, v)
                    )

    def _set_dot(self, key: str, value: str) -> None:
        self._segs[f"dots_{key}"].set_active(value)
        self._set("dots", {**self.prefs["dots"], key: value})

    def _toggle_rail(self) -> None:
        toggle = self.ctx.callbacks.get("species_toggle_rail")
        if toggle is not None:
            toggle()

    async def _open_sources(self) -> None:
        async with self._flight("sources") as acquired:
            if not acquired or self.lp is None:
                return
            from ui.sta_kitchen.sources import open_sources

            await open_sources(self)

    async def _open_make_set(self) -> None:
        async with self._flight("make_set") as acquired:
            if acquired and self.lp is not None and self.lp.table is not None:
                from ui.sta_kitchen.sets import open_make_set

                await open_make_set(self)

    # ── Kept vs removed means ──────────────────────────────────────────────────────────────────

    def _build_means(self) -> None:
        """The Means button and its panel: the two pictures, made only on "Compute"."""
        btn = house_button("Means", tooltip=MEANS_TIP)
        with btn, ui.menu().props("anchor='bottom right' self='top right'"):
            with ui.element("div").style(
                "display: flex; flex-direction: column; gap: 8px; padding: 10px 12px; width: 290px;"
            ):
                ui.label(MEANS_TIP).style("font-size: 9px; line-height: 1.45; color: #475569;")
                with ui.row().classes("no-wrap gap-4 justify-center w-full"):
                    for which in ("kept", "removed"):
                        with ui.column().classes("items-center gap-1"):
                            img = ui.element("img").style(
                                "width: 112px; height: 112px; border-radius: var(--cb-r-lg, 6px); background: #0f172a; "
                                "display: block; object-fit: cover;"
                            )
                            img.set_visibility(False)
                            cap = ui.label(f"{which}: not made yet").style("font-size: 9px; color: #64748b;")
                            self._refs[f"means_{which}"], self._refs[f"means_{which}_cap"] = img, cap
                with ui.row().classes("items-center gap-2 no-wrap"):
                    house_button("Compute for this cut", self._compute_means, kind="accent")
                    self._refs["means_state"] = ui.label("").style("font-size: 9px; color: #64748b;")

    async def _compute_means(self) -> None:
        async with self._flight("means") as acquired:
            if not acquired or self.lp is None or self.lp.table is None:
                return
            state = self._refs["means_state"]
            state.set_text("cutting up to 400 cutouts…")
            try:
                res = await service.averages(self.lp)
            except (OSError, ValueError, RuntimeError) as e:
                logger.exception(
                    "Kitchen %s/%s: the means could not be made", self.ctx.project_path.name, self.ctx.species_id
                )
                state.set_text(f"the means could not be made: {e}")
                return
            if res is None:
                state.set_text("the pool has no table yet")
                return
            self.lp.averages = res
            self._means_draft = self.lp.pool.draft.model_dump_json() + str(len(self.lp.verdicts))
            for which, entry in res.items():
                img, cap = self._refs[f"means_{which}"], self._refs[f"means_{which}_cap"]
                if entry.get("file"):
                    img.props["src"] = f"/api/kitchen/average?{self._q()}&which={which}&v={entry['digest']}"
                    img.update()
                    cap.set_text(f"{which}: mean of {entry['n']:,} of {entry['of']:,}")
                else:
                    cap.set_text(f"{which}: none to average")
                img.set_visibility(bool(entry.get("file")))
            state.set_text("for this cut")

    def _mark_means(self) -> None:
        """After a new cut or verdict, means made for an earlier one say so; nothing is recomputed."""
        if self._means_draft is None or self.lp is None:
            return
        current = self.lp.pool.draft.model_dump_json() + str(len(self.lp.verdicts))
        self._refs["means_state"].set_text(
            "for this cut" if current == self._means_draft else "made for an earlier cut"
        )

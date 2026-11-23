"""The Sub-units block of a Refine 3D's or an M refine's Results: the sub-particle sets made of the job's refined
particles, and Make sub-particle set, whose dialog asks where one unit sits in a particle (an axis of its point
group and a radius) or along a filament (a helix) and writes the copies as a set (services/particles/
subparticles.py, roadmap 71). The kind of site follows from the job; nothing here chooses it. Beside it, Make
expansion set: the job's particles in every pose their symmetry, or their lattice, makes equal, the same images
(`subparticles.expand`)."""

from __future__ import annotations

import asyncio
from pathlib import Path

from nicegui import ui

from services.particles import sites
from ui.analytics.common import ERROR_CLS, TABLE_ON_PANEL_STYLE, esc
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_number, house_select, house_text
from ui.components.reactive import Debounced, SingleFlight, owned_timer
from ui.dashboard.css import ensure_assets_loaded
from ui.job_plugins._field_styles import HELPER_STYLE, PANEL_TITLE_STYLE
from ui.protocols_blocks import FOOTER, TEXT, TITLE

RULE = (
    "A sub-particle set holds the copies of one unit in every refined particle of this job: each copy's centre "
    "and pose are its particle's followed by where the unit sits in it, and every copy of one capsid or filament "
    "is in one half-set. The images are cut anew, in a small box, by a Warp Export or a Reconstruct Particle that "
    "reads the set. The unit is a species of its own: its symmetry and Ø are what those jobs start from."
)
EXPANSION_RULE = (
    "An expansion set holds every refined particle of this job in each pose its symmetry makes equal: the same "
    "images, each copy's pose and origin changed. For segments of a filament, the registrations of their lattice "
    "that the true repeat tells apart (a microtubule's 26 for its alpha/beta dimers: 13 seams by 2 registers), each "
    "by its smallest move along the axis; for particles, the operators of the point group they were refined with. "
    "A Class 3D with Skip alignment on the set scores every copy against one reference."
)
PHASE_RULE = (
    "Where one unit sits in the map: its angle about the axis from the map's X, and its height from the box "
    "centre. Left empty, the copies sit on the lattice through the map's X axis at mid-height, which centres "
    "them on the lattice and not necessarily on a unit."
)
_ROW = "display: flex; align-items: center; gap: 10px; flex-wrap: wrap;"
_FLOAT = "%.6g"


def _number(value) -> float | None:
    """A field's value as a number, None when it is empty."""
    try:
        return None if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return None


class SubUnitsPanel:
    def __init__(self, backend, project_path: Path, instance_id: str) -> None:
        self.backend = backend
        self.project_path = Path(project_path)
        self.instance_id = instance_id
        self._flight = SingleFlight()
        self._box = None

    def build(self) -> None:
        self._box = ui.column().classes("w-full").style("gap: 6px;")
        owned_timer(0.05, self._load, self._box, once=True)

    async def _load(self) -> None:
        res = await self.backend.subparticle_readout(self.project_path, self.instance_id)
        if self._box.is_deleted:
            return
        self._box.clear()
        with self._box:
            if not res["success"]:
                ui.label(res["error"]).classes(ERROR_CLS)
                return
            self._body(res["readout"])

    def _body(self, r: dict) -> None:
        # A job whose particles hold no copies of a unit (refined in C1, no filament) shows nothing.
        if r["kind"] is None:
            return
        with ui.element("div").style("display: flex; flex-direction: column; gap: 4px; width: 100%; margin-top: 6px;"):
            with ui.element("div").style("display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap;"):
                ui.label("Sub-units").style(PANEL_TITLE_STYLE).tooltip(RULE)
                made = len(r["sets"])
                ui.label(f"{made} set{'' if made == 1 else 's'} made of this job" if made else "none made yet").style(
                    HELPER_STYLE
                )
            if r["sets"]:
                names = {s["id"]: s["name"] for s in r["species"]}
                body = "".join(
                    f"<tr><td>{esc(s['name'])}</td><td>{esc(names.get(s['species'], s['species']))}</td>"
                    f"<td>{esc(s['site'])}</td><td class='n'>{s['picks']:,}</td><td>{esc(s['set'])}</td></tr>"
                    for s in r["sets"]
                )
                ui.html(
                    '<table class="cb-atable"><thead><tr><th>set</th><th>sub-unit</th><th>where it sits</th>'
                    f"<th class='n'>copies</th><th>id</th></tr></thead><tbody>{body}</tbody></table>"
                ).classes("cb-atable-wrap").style(f"max-height: 200px; {TABLE_ON_PANEL_STYLE}")
            expansions = (r.get("expansion") or {}).get("sets") or []
            if expansions:
                body = "".join(
                    f"<tr><td>{esc(s['name'])}</td><td>{esc(s['expansion'])}</td><td class='n'>{s['picks']:,}</td>"
                    f"<td>{esc(s['set'])}</td></tr>"
                    for s in expansions
                )
                ui.html(
                    '<table class="cb-atable"><thead><tr><th>expansion set</th><th>poses</th>'
                    f"<th class='n'>copies</th><th>id</th></tr></thead><tbody>{body}</tbody></table>"
                ).classes("cb-atable-wrap").style(f"max-height: 160px; {TABLE_ON_PANEL_STYLE}")
            with ui.element("div").style(_ROW + " margin-top: 2px;"):
                house_button("Make sub-particle set", lambda: self._open(r), tooltip=RULE)
                if (r.get("expansion") or {}).get("kind"):
                    house_button("Make expansion set", lambda: self._open_expansion(r), tooltip=EXPANSION_RULE)

    async def _open(self, r: dict) -> None:
        async with self._flight("make") as acquired:
            if not acquired:
                return
            made = await _MakeDialog(self.backend, self.project_path, self.instance_id, r).run()
            if made and not self._box.is_deleted:
                await self._load()

    async def _open_expansion(self, r: dict) -> None:
        async with self._flight("expand") as acquired:
            if not acquired:
                return
            made = await _ExpandDialog(self.backend, self.project_path, self.instance_id, r).run()
            if made and not self._box.is_deleted:
                await self._load()


class _ExpandDialog:
    """An expansion, typed: for segments the lattice's helix, its repeat along the axis and the units to a true
    repeat (filled from the last expansion, or the last sub-particle site, of this job's species); for particles
    nothing to type. The count follows the values; Make set waits until they are whole."""

    KEYS = ("twist_deg", "rise_ang", "axial_repeat_ang", "units_per_repeat")

    def __init__(self, backend, project_path: Path, instance_id: str, readout: dict) -> None:
        self.backend = backend
        self.project_path = project_path
        self.instance_id = instance_id
        self.r = readout
        self.e = readout["expansion"]
        self.lattice = self.e["kind"] == sites.LATTICE
        self.values: dict = {}
        self.name = ""
        self._fields: dict[str, ui.element] = {}
        self._made = False
        self._plan_ok = False
        self._make_btn = None
        self._replan = Debounced(self._schedule_plan)
        self._flight = SingleFlight()

    async def run(self) -> bool:
        ensure_assets_loaded()
        r = self.r
        what = "segments of filaments" if self.lattice else f"particles refined with {r['group']}"
        with (
            dialog_host(),
            ui.dialog() as self.dialog,
            ui.card().classes("p-0").style("width: 520px; max-width: 95vw;"),
        ):
            with ui.column().classes("w-full gap-0").style("padding: 14px 16px 6px;"):
                ui.label("Make expansion set").style(TITLE).tooltip(EXPANSION_RULE)
                ui.label(f"from {r['job']}: {what}, the same images").style(TEXT)
            with ui.column().classes("w-full").style("padding: 4px 16px 8px; gap: 8px;"):
                if self.lattice:
                    self._lattice_rows()
                else:
                    ui.label(
                        f"Each particle in the {self.e.get('per_particle', '?')} poses of {r['group']}'s operators."
                    ).style(TEXT)
                with ui.element("div").style(_ROW):
                    house_text(
                        "name",
                        value="",
                        placeholder="the set's name",
                        width="w-56",
                        on_change=lambda e: setattr(self, "name", e.value or ""),
                    )
                self._count = ui.label("").style(TEXT + " min-height: 14px;")
            with ui.row().classes("w-full justify-end items-center gap-2").style(FOOTER):
                house_button("Cancel", self.dialog.close)
                self._make_btn = house_button("Make set", self._make, kind="accent")
        self._make_btn.disable()
        last = self.e.get("last")
        if self.lattice and last:
            self._fill(last["values"] or {})
        self._schedule_plan()
        self.dialog.open()
        await self.dialog
        return self._made

    def _lattice_rows(self) -> None:
        with ui.element("div").style(_ROW):
            self._fields["twist_deg"] = self._num("twist_deg", "twist °", "The lattice's twist per helical step.")
            self._fields["rise_ang"] = self._num("rise_ang", "rise Å", "The lattice's rise per helical step.")
            self._fields["axial_repeat_ang"] = self._num(
                "axial_repeat_ang", "repeat Å", "The unit's repeat straight along the axis (a microtubule's monomer)."
            )
            self._fields["units_per_repeat"] = self._num(
                "units_per_repeat",
                "units",
                "How many units make the true repeat along the axis, which the map could not tell apart: 2 for "
                "a microtubule's alpha/beta dimers.",
                width="w-12",
            )
        last = self.e.get("last")
        if last:
            where = f"the expansion {last['name']}" if last.get("set") else "the last sub-particle site of this species"
            ui.label(f"values as in {where}").style(HELPER_STYLE).tooltip("Change any of them.")

    def _num(self, key: str, label: str, hint: str, *, width: str = "w-20"):
        return house_number(
            label, value=None, format=_FLOAT, width=width, hint=hint, on_change=lambda e, k=key: self._set(k, e.value)
        )

    def _fill(self, values: dict) -> None:
        for key in self.KEYS:
            el, value = self._fields.get(key), values.get(key)
            if el is not None and value is not None:
                el.set_value(value)

    def _set(self, key: str, value) -> None:
        n = _number(value)
        self.values[key] = (int(n) if n is not None and n >= 1 else None) if key == "units_per_repeat" else n
        self._plan_ok = False
        if self._make_btn is not None:
            self._make_btn.disable()
        self._replan()

    def _given(self) -> dict:
        return {k: v for k, v in self.values.items() if v is not None}

    def _missing(self) -> list[str]:
        if not self.lattice:
            return []
        words = {"twist_deg": "the twist", "rise_ang": "the rise", "axial_repeat_ang": "the repeat"}
        words["units_per_repeat"] = "the units to a repeat"
        return [w for k, w in words.items() if k not in self._given()]

    def _schedule_plan(self) -> None:
        missing = self._missing()
        if missing:
            self._plan_ok = False
            self._make_btn.disable()
            self._count.set_text("Not given yet: " + ", ".join(missing) + ".")
            return
        asyncio.create_task(self._plan())

    async def _plan(self) -> None:
        values = self._given()
        res = await self.backend.expansion_plan(self.project_path, instance_id=self.instance_id, values=values)
        if self._count.is_deleted or values != self._given():
            return
        if not res["success"]:
            self._plan_ok = False
            self._make_btn.disable()
            self._count.set_text(res["error"])
            return
        p = res["plan"]
        per = "segment" if self.lattice else "particle"
        moved = f" · moved at most {p['max_shift_ang']:g} Å along the axis" if p["max_shift_ang"] else ""
        self._count.set_text(f"{p['per_parent']} per {per} · {p['parents']:,} → {p['picks']:,}{moved}")
        self._plan_ok = True
        self._make_btn.enable()

    async def _make(self) -> None:
        async with self._flight("make") as acquired:
            if not acquired or not self._plan_ok:
                return
            name = self.name.strip() or f"{self.instance_id} expanded"
            self._count.set_text("making the set…")
            res = await self.backend.create_expansion_set(
                self.project_path, instance_id=self.instance_id, name=name, values=self._given()
            )
            if self._count.is_deleted:
                return
            if not res["success"]:
                self._count.set_text(res["error"])
                return
            ps = res["set"]
            self._made = True
            ui.notify(f"Set {ps.name}: {ps.n_particles:,} copies in {ps.n_tomograms} tomograms", type="positive")
            self.dialog.close()


class _MakeDialog:
    """One site, typed: the sub-unit's species, where it sits, the job its values were measured on. The count
    follows the values; Make set waits until the site is whole."""

    def __init__(self, backend, project_path: Path, instance_id: str, readout: dict) -> None:
        self.backend = backend
        self.project_path = project_path
        self.instance_id = instance_id
        self.r = readout
        self.helix = readout["kind"] == sites.HELIX
        self.species: str | None = None
        self.values: dict = {}
        self.measured_on: str | None = None
        self.name = ""
        self._fields: dict[str, ui.element] = {}
        self._made = False
        self._plan_ok = False
        self._make_btn = None
        self._plan_task: asyncio.Task | None = None
        self._replan = Debounced(self._schedule_plan)
        self._flight = SingleFlight()

    # ── the dialog ──

    async def run(self) -> bool:
        ensure_assets_loaded()
        r = self.r
        what = "segments of filaments" if self.helix else f"particles refined with {r['group']}"
        with (
            dialog_host(),
            ui.dialog() as self.dialog,
            ui.card().classes("p-0").style("width: 520px; max-width: 95vw;"),
        ):
            with ui.column().classes("w-full gap-0").style("padding: 14px 16px 6px;"):
                ui.label("Make sub-particle set").style(TITLE).tooltip(RULE)
                ui.label(f"from {r['job']}: {what}").style(TEXT)
            with ui.column().classes("w-full").style("padding: 4px 16px 8px; gap: 8px;"):
                self._species_row()
                if self.helix:
                    self._helix_rows()
                else:
                    self._particle_rows()
                with ui.element("div").style(_ROW):
                    self._fields["measured_on"] = house_select(
                        "measured on",
                        {m["job"]: m["label"] for m in r["measured_on"]},
                        value=None,
                        width="w-56",
                        hint="The job whose map these values were read from. It is kept with the set.",
                        on_change=lambda e: self._set("measured_on", e.value, own=True),
                    )
                with ui.element("div").style(_ROW):
                    house_text(
                        "name",
                        value="",
                        placeholder="the set's name",
                        width="w-56",
                        on_change=lambda e: setattr(self, "name", e.value or ""),
                    )
                self._count = ui.label("").style(TEXT + " min-height: 14px;")
            with ui.row().classes("w-full justify-end items-center gap-2").style(FOOTER):
                house_button("Cancel", self.dialog.close)
                self._make_btn = house_button("Make set", self._make, kind="accent")
        self._make_btn.disable()
        self._say_waiting()
        self.dialog.open()
        await self.dialog
        return self._made

    def _species_row(self) -> None:
        options = {s["id"]: s["name"] for s in self.r["species"]}
        with ui.element("div").style(_ROW):
            if not options:
                ui.label(
                    "This project has no other species. A sub-unit is a species of its own (its symmetry and Ø are "
                    "what its jobs start from): make it on the Species page first."
                ).style(TEXT)
                return
            house_select(
                "sub-unit",
                options,
                value=None,
                width="w-48",
                hint="The species the copies are of. Jobs reading the set take their symmetry and Ø from it.",
                on_change=lambda e: self._choose_species(e.value),
            )
            self._own = ui.label("").style(HELPER_STYLE)
            self._filled = ui.label("").style(HELPER_STYLE)
            self._filled.tooltip("Filled from the last sub-particle set made of this sub-unit. Change any of them.")

    def _particle_rows(self) -> None:
        axes = {a["axis"]: f"{a['axis']} · {a['per_particle']} per particle" for a in self.r["axes"]}
        axes[sites.DIRECTION] = "a direction · the group's order per particle, fewer on an axis"
        with ui.element("div").style(_ROW):
            self._fields["axis"] = house_select(
                "axis",
                axes,
                value=None,
                width="w-56",
                hint=f"Where the unit sits in the map: on one of {self.r['group']}'s axes, or along any direction.",
                on_change=lambda e: self._set_axis(e.value),
            )
            self._fields["radius_ang"] = self._num(
                "radius_ang", "radius Å", "The unit's centre from the particle's centre, read off the map."
            )
        self._direction_row = ui.element("div").style(_ROW)
        with self._direction_row:
            hint = "The direction from the particle's centre to the unit, in the map's frame. Any length."
            for i, axis in enumerate("xyz"):
                self._fields[f"direction{i}"] = self._num(f"direction{i}", axis, hint, width="w-16")
        self._direction_row.set_visibility(False)

    def _helix_rows(self) -> None:
        own = self.r.get("helical")
        from_job = "This refinement imposed it." if own else "Read off the map: this refinement imposed no lattice."
        with ui.element("div").style(_ROW):
            self._fields["twist_deg"] = self._num("twist_deg", "twist °", f"The helix's twist per step. {from_job}")
            self._fields["rise_ang"] = self._num("rise_ang", "rise Å", f"The helix's rise per step. {from_job}")
            self._fields["radius_ang"] = self._num(
                "radius_ang", "radius Å", "The unit's centre from the filament's axis, read off the map."
            )
        with ui.element("div").style(_ROW):
            self._fields["axial_repeat_ang"] = self._num(
                "axial_repeat_ang",
                "repeat Å",
                "For a lattice with several starts: the unit's repeat straight along the axis (a microtubule's "
                "monomer repeat). The helix's steps alone visit only some of such a lattice's units. Empty: the "
                "helix alone.",
            )
            self._fields["every"] = self._num(
                "every", "every", "Every n-th step of the helix. 1: every step.", width="w-12"
            )
            self._fields["azimuth_deg"] = self._num("azimuth_deg", "azimuth °", PHASE_RULE, width="w-16")
            self._fields["axial_ang"] = self._num("axial_ang", "along Å", PHASE_RULE, width="w-16")
        if own:
            self._fill({"twist_deg": own["twist_deg"], "rise_ang": own["rise_ang"]})

    def _num(self, key: str, label: str, hint: str, *, width: str = "w-20"):
        return house_number(
            label, value=None, format=_FLOAT, width=width, hint=hint, on_change=lambda e, k=key: self._set(k, e.value)
        )

    # ── values ──

    def _set(self, key: str, value, *, own: bool = False) -> None:
        if own:
            self.measured_on = value or None
        elif key == "every":
            n = _number(value)
            self.values[key] = int(n) if n is not None and n >= 1 else None
        else:
            self.values[key] = _number(value)
        self._changed()

    def _changed(self) -> None:
        """A value moved: what was counted is no longer what Make set would write, so it waits for the new count."""
        self._plan_ok = False
        if self._make_btn is not None:
            self._make_btn.disable()
        self._replan()

    def _set_axis(self, axis: str | None) -> None:
        self.values["axis"] = axis
        self._direction_row.set_visibility(axis == sites.DIRECTION)
        self._say_own()
        self._changed()

    def _fill(self, values: dict) -> None:
        """Values into their fields (each field's change handler takes it into `self.values`)."""
        for key, value in values.items():
            el = self._fields.get(key)
            if el is None or value is None:
                continue
            # A choice that is no longer offered (a job since deleted) stays empty.
            if isinstance(el, ui.select) and value not in el.options:
                continue
            el.set_value(value)

    def _choose_species(self, species_id: str | None) -> None:
        self.species = species_id
        last = (self.r.get("last") or {}).get(species_id or "")
        if last:
            site = last["site"]
            if self.helix:
                keys = ("twist_deg", "rise_ang", "radius_ang", "axial_repeat_ang", "every", "azimuth_deg", "axial_ang")
                self._fill({k: site.get(k) for k in keys})
            else:
                named = site.get("axis") not in ("", sites.DIRECTION, None)
                self._fill({"axis": site["axis"] if named else sites.DIRECTION, "radius_ang": site.get("radius_ang")})
                if not named:
                    self._fill({f"direction{i}": x for i, x in enumerate(site.get("direction") or [])})
            self._fill({"measured_on": last.get("measured_on") or None})
        self._filled.set_text(f"values as in {last['name']}" if last else "")
        self._say_own()
        self._changed()

    def _site_values(self) -> dict:
        v = {k: x for k, x in self.values.items() if x is not None and not k.startswith("direction")}
        if not self.helix and v.get("axis") == sites.DIRECTION:
            d = [self.values.get(f"direction{i}") for i in range(3)]
            if all(x is not None for x in d):
                v["direction"] = d
        return v

    def _missing(self) -> list[str]:
        v, out = self._site_values(), []
        if not self.species:
            out.append("the sub-unit")
        if self.helix:
            out += [w for k, w in (("twist_deg", "the twist"), ("rise_ang", "the rise")) if k not in v]
        elif not v.get("axis"):
            out.append("the axis")
        elif v["axis"] == sites.DIRECTION and "direction" not in v:
            out.append("the direction")
        if "radius_ang" not in v:
            out.append("the radius")
        if self.r["measured_on"] and not self.measured_on:
            out.append("the job the values were measured on")
        return out

    # ── what the values give ──

    def _say_own(self) -> None:
        """Beside the sub-unit: the unit's own symmetry on the chosen axis, marked when its species has another."""
        if self.helix or not hasattr(self, "_own"):
            return
        own = next((a["own_symmetry"] for a in self.r["axes"] if a["axis"] == self.values.get("axis")), "")
        sp = next((s for s in self.r["species"] if s["id"] == self.species), None)
        if not own or sp is None:
            self._own.set_text(f"its own symmetry here: {own}" if own else "")
            return
        differs = sites.group_name(sp["symmetry"]) != own
        self._own.set_text(
            f"its own symmetry here: {own}" + (f" · {sp['name']} has {sp['symmetry']}" if differs else "")
        )
        self._own.style("color: var(--cb-warn-ink, #8a4b06);" if differs else "color: inherit;")

    def _say_waiting(self) -> None:
        missing = self._missing()
        self._plan_ok = False
        self._make_btn.disable()
        self._count.set_text("Not given yet: " + ", ".join(missing) + "." if missing else "")

    def _schedule_plan(self) -> None:
        if self._missing():
            self._say_waiting()
            return
        # It builds nothing (it writes the count line), so it needs no slot of its own.
        self._plan_task = asyncio.create_task(self._plan())

    async def _plan(self) -> None:
        species, values = self.species, self._site_values()
        res = await self.backend.subparticle_plan(
            self.project_path, instance_id=self.instance_id, species_id=species, values=values
        )
        if self._count.is_deleted or (species, values) != (self.species, self._site_values()):
            return
        if not res["success"]:
            self._plan_ok = False
            self._make_btn.disable()
            self._count.set_text(res["error"])
            return
        p = res["plan"]
        per = "segment" if self.helix else "particle"
        dropped = [f"{f['removed']:,} {f['label']}" for f in p["funnel"][2:] if f["removed"]]
        text = f"{p['per_parent']} per {per} · {p['parents']:,} → {p['picks']:,}"
        self._count.set_text(text + (" · left out: " + "; ".join(dropped) if dropped else ""))
        self._plan_ok = True
        self._make_btn.enable()

    async def _make(self) -> None:
        async with self._flight("make") as acquired:
            if not acquired or not self._plan_ok:
                return
            sp = next(s for s in self.r["species"] if s["id"] == self.species)
            name = self.name.strip() or f"{sp['name']} of {self.instance_id}"
            self._count.set_text("making the set…")
            res = await self.backend.create_subparticle_set(
                self.project_path,
                instance_id=self.instance_id,
                species_id=self.species,
                name=name,
                values=self._site_values(),
                measured_on=self.measured_on or "",
            )
            if self._count.is_deleted:
                return
            if not res["success"]:
                self._count.set_text(res["error"])
                return
            ps = res["set"]
            self._made = True
            ui.notify(f"Set {ps.name}: {ps.n_particles:,} copies in {ps.n_tomograms} tomograms", type="positive")
            self.dialog.close()

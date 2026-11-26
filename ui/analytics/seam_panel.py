"""The seam block of a Class 3D that scored an expansion set of filament segments: which registration of its
lattice each piece of filament is on by the vote of its segments' scores (services/particles/filament_seam.py),
whether the vote means anything, and Make registered set, which writes each segment on its registration so a unit
of the true repeat (a tubulin dimer) sits at one site in every one."""

from __future__ import annotations

import re
from pathlib import Path

from nicegui import ui

from services.particles.filament_seam import DECIDED
from services.result import ErrorCode
from ui.analytics.common import ERROR_CLS, HINT_CLS, TABLE_ON_PANEL_STYLE, esc
from ui.components.buttons import house_button
from ui.components.check import house_check
from ui.components.fields import house_number, house_text
from ui.components.reactive import SingleFlight, owned_timer
from ui.job_plugins._field_styles import HELPER_STYLE, PANEL_TITLE_STYLE
from ui.styles import SANS

_BODY = f"{SANS} font-size: 11px; color: var(--cb-ink-2, #1e293b);"
_MUTED = "var(--cb-ink-3, #5b7089)"
_POSITION_RE = re.compile(r"^(?P<source>.+?)_(?P<pos>Position_\d+(?:_\d+)?)$")

VOTE_RULE = (
    "Every segment was scored on each registration of its lattice. Neighbouring segments are first brought into "
    "one frame per filament; a piece of filament then takes the registration its segments' summed scores fit "
    "best, read only in the part of the scores that swaps with the units (alpha and beta). A piece is registered "
    "when it is long enough, by its length alone, so no score chooses which segments an average holds."
)
HALVES_RULE = (
    "The two halves of a piece, a block apart, share no data and vote alone. Registrations a step apart label "
    "one protofilament's units differently, thirteen steps apart all of them: the share of sites the halves "
    "label alike is a half when the scores tell nothing and 1 when the halves agree."
)
HELD_OUT_RULE = (
    "Each segment takes the registration its piece voted without the segment's own block and that block's "
    "neighbours, which share its data: an average of the set then holds nothing the segments' own noise chose, "
    "and shows whether the units are told apart. Off: the whole piece's registration, the most accurate one."
)


def _split_name(tomo: str) -> tuple[str, str]:
    m = _POSITION_RE.match(tomo)
    return (m["source"], m["pos"]) if m else ("", tomo)


class SeamPanel:
    def __init__(self, backend, project_path: Path, instance_id: str) -> None:
        self.backend = backend
        self.project_path = Path(project_path)
        self.instance_id = instance_id
        self._radius: float | None = None
        self._min_segments: int | None = None
        self._block_segments: int | None = None
        self._name = ""
        self._held_out = True
        self._status = None
        self._flight = SingleFlight()

    def build(self) -> None:
        self._box = ui.column().classes("w-full").style("gap: 6px;")
        owned_timer(0.05, self._start, self._box, once=True)

    async def _start(self) -> None:
        res = await self.backend.seam_start(self.project_path, self.instance_id)
        if self._box.is_deleted or not res["success"] or not res["start"]["applies"]:
            return
        start = res["start"]
        self._radius, self._min_segments = start["radius_ang"], start["min_segments"]
        self._block_segments = start["block_segments"]
        with self._box:
            with ui.element("div").style(
                "display: flex; flex-direction: column; gap: 4px; width: 100%; margin-top: 6px;"
            ):
                with ui.element("div").style("display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap;"):
                    ui.label("Seam and register").style(PANEL_TITLE_STYLE).tooltip(VOTE_RULE)
                    ui.label(f"the scores of {start['scored_set_name']}").style(HELPER_STYLE)
                with ui.element("div").style("display: flex; align-items: center; gap: 10px; flex-wrap: wrap;"):
                    house_number(
                        "radius (Å)",
                        value=self._radius,
                        width="w-20",
                        on_change=lambda e: setattr(self, "_radius", e.value),
                    ).tooltip(
                        "How far the lattice's units sit from the filament's axis, as measured on your map. It "
                        "turns a difference in angle between neighbouring segments into a distance."
                    )
                    house_number(
                        "least segments",
                        value=self._min_segments,
                        width="w-20",
                        on_change=lambda e: setattr(self, "_min_segments", e.value),
                    ).tooltip(
                        "A piece of filament is registered when it has this many segments on one lattice. Empty: "
                        "the least a held-out vote needs, four blocks. The table below the vote says what a "
                        "longer cut buys."
                    )
                    house_button("Read the vote", self._read)
            self._result = ui.column().classes("w-full").style("gap: 4px;")
        if self._radius:
            await self._read()

    def _values(self) -> dict:
        v: dict = {"radius_ang": self._radius}
        if self._min_segments:
            v["min_segments"] = int(self._min_segments)
        if self._block_segments:
            v["block_segments"] = int(self._block_segments)
        return v

    async def _read(self) -> None:
        async with self._flight("read") as acquired:
            if not acquired:
                return
            self._result.clear()
            with self._result:
                if not self._radius:
                    ui.label(
                        "The radius is not given: the vote cannot bring neighbouring segments into one frame."
                    ).classes(HINT_CLS)
                    return
                ui.label("reading the segments' scores…").classes(HINT_CLS)
            res = await self.backend.seam_readout(self.project_path, self.instance_id, values=self._values())
            if self._result.is_deleted:
                return
            self._result.clear()
            with self._result:
                if not res["success"]:
                    expected = res.get("code") == ErrorCode.NOT_WRITTEN_YET
                    ui.label(res["error"]).classes(HINT_CLS if expected else ERROR_CLS)
                    return
                self._body(res["vote"])

    def _body(self, v: dict) -> None:
        ui.label(
            f"{v['decided']} of {v['pieces']} pieces of filament registered ({v['segments_decided']:,} of "
            f"{v['segments']:,} segments): those of {v['min_segments']} segments or more on one lattice. "
            f"{v['segments_left_out']:,} segments sit on no registration of their neighbour's and are left out."
        ).style(_BODY)
        blocks = (v.get("checks") or {}).get("blocks")
        if blocks:
            ui.label(
                f"Blocks of one piece that share no data vote within one step of each other in "
                f"{100 * blocks['within_one_step']:.0f} % of {blocks['pairs']:,} pairs; blocks of different "
                f"filaments in {100 * (blocks['other_filaments_within_one_step'] or 0):.0f} %."
            ).style(_BODY)
        for note in v.get("notes") or []:
            ui.label(note).style(f"{SANS} font-size: 11px; color: var(--cb-warn-ink, #8a4b06);")
        by_length = (v.get("checks") or {}).get("by_length") or []
        if by_length:
            ui.html(self._lengths_html(by_length, v["min_segments"])).classes("cb-atable-wrap").style(
                f"max-width: 520px; {TABLE_ON_PANEL_STYLE}"
            ).tooltip(HALVES_RULE)
        ui.html(self._table_html(v["per_piece"])).classes("cb-atable-wrap").style(
            f"max-height: 260px; {TABLE_ON_PANEL_STYLE}"
        )
        if v.get("sets"):
            made = "; ".join(
                f"{s['name']} ({s['segments']:,} segments, {'held out' if s['held_out'] else 'whole pieces'})"
                for s in v["sets"]
            )
            ui.label(f"Sets made of this vote: {made}").style(HELPER_STYLE)
        self._make_controls()

    @staticmethod
    def _lengths_html(rows: list[dict], chosen: int) -> str:
        head = (
            "<th class='n'>segments or more</th><th class='n'>pieces</th><th class='n'>segments</th>"
            "<th class='n' title='Steps between the registrations the two halves of a piece voted, on average'>"
            "halves apart</th><th class='n' title='The share of the lattice sites the two halves label alike; "
            "50 % when nothing is known'>sites alike</th>"
        )
        body = []
        for r in rows:
            mark = " style='font-weight:600'" if r["min_segments"] == chosen else ""
            body.append(
                f"<tr{mark}><td class='n'>{r['min_segments']}</td><td class='n'>{r['pieces']}</td>"
                f"<td class='n'>{r['segments']:,}</td><td class='n'>{r['halves_mean_steps']:.1f}</td>"
                f"<td class='n'>{100 * r['sites_alike']:.0f} %</td></tr>"
            )
        return f'<table class="cb-atable"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table>'

    @staticmethod
    def _table_html(rows: list[dict]) -> str:
        names = [_split_name(r["tomo"]) for r in rows]
        with_source = len({s for s, _ in names}) > 1
        head = (
            ("<th>from</th>" if with_source else "")
            + "<th>tomogram</th><th class='n'>filament</th><th class='n'>piece</th><th class='n'>segments</th>"
            + "<th class='n' title='The registration of the lattice the piece is on, in its first segment&#39;s "
            "frame'>registration</th><th class='n' title='Steps between the registrations its two halves voted'>"
            "halves apart</th><th>verdict</th>"
        )
        order = sorted(range(len(rows)), key=lambda i: (rows[i]["verdict"] != DECIDED, -rows[i]["segments"]))
        body = []
        for i in order:
            r, (source, pos) = rows[i], names[i]
            decided = r["verdict"] == DECIDED
            muted = "" if decided else f" style='color:{_MUTED}'"
            halves = "" if r["halves_steps_apart"] is None else str(r["halves_steps_apart"])
            body.append(
                f"<tr{muted}>"
                + (f"<td>{esc(source)}</td>" if with_source else "")
                + f"<td title='{esc(r['tomo'])}'>{esc(pos)}</td><td class='n'>{esc(r['tube'])}</td>"
                f"<td class='n'>{esc(r['piece'])}</td><td class='n'>{r['segments']:,}</td>"
                f"<td class='n'>{r['registration']}</td><td class='n'>{halves}</td>"
                f"<td>{esc(r['verdict'] if decided else r['why'])}</td></tr>"
            )
        return f'<table class="cb-atable"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table>'

    def _make_controls(self) -> None:
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-top: 4px;"
        ):
            house_text(
                "name",
                value=self._name,
                placeholder="e.g. microtubules registered",
                width="w-48",
                on_change=lambda e: setattr(self, "_name", e.value or ""),
            )
            house_check(
                "held out",
                value=self._held_out,
                on_change=lambda e: setattr(self, "_held_out", bool(e.value)),
                tooltip=HELD_OUT_RULE,
            )
            house_button(
                "Make registered set",
                self._make,
                kind="accent",
                tooltip="A new particle set of the registered pieces' segments, each in the pose and origin of the "
                "registration it was given, the same images. A sub-particle set made of it places the units of "
                "the true repeat. The scoring job is not changed.",
            )
            self._status = ui.label("").classes(HINT_CLS)

    async def _make(self) -> None:
        async with self._flight("make") as acquired:
            if not acquired:
                return
            name = self._name.strip() or "registered"
            self._status.set_text("making the set…")
            res = await self.backend.create_seam_set(
                self.project_path,
                class3d_instance=self.instance_id,
                name=name,
                values=self._values(),
                held_out=self._held_out,
            )
            if self._status.is_deleted:
                return
            if not res["success"]:
                self._status.set_text(res["error"])
                return
            ps = res["set"]
            self._status.set_text(f"Set {ps.name}: {ps.n_particles:,} segments in {ps.n_tomograms} tomograms")

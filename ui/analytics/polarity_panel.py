"""The polarity block of a helical Refine 3D's Results tab: which way each filament points by the vote of its
segments (services/particles/filament_polarity.py), whether the vote means anything, and Make set, which writes
the decided directions into a set the next Refine 3D reads with them held."""

from __future__ import annotations

import re
from pathlib import Path

from nicegui import ui

from services.particles.filament_polarity import KEPT, REVERSED, UNDECIDED
from services.result import ErrorCode
from ui.analytics.common import ERROR_CLS, HINT_CLS, TABLE_ON_PANEL_STYLE, esc
from ui.components.buttons import house_button
from ui.components.check import house_check
from ui.components.fields import house_text
from ui.components.reactive import SingleFlight, owned_timer
from ui.job_plugins._field_styles import HELPER_STYLE, PANEL_TITLE_STYLE
from ui.styles import SANS

_BODY = f"{SANS} font-size: 11px; color: var(--cb-ink-2, #1e293b);"
_MUTED = "var(--cb-ink-3, #5b7089)"
_POSITION_RE = re.compile(r"^(?P<source>.+?)_(?P<pos>Position_\d+(?:_\d+)?)$")

VOTE_RULE = (
    "Each filament points the way at least two thirds of its segments point after the refinement (compared with "
    "the direction each went in with). The set turns the others round and holds the decided direction in the "
    "next refinement. A filament without that majority is undecided: left out of the set unless kept."
)
PIECES_RULE = (
    "Two pieces of one filament more than a box apart share no density. They choose the same direction only if "
    "the map tells a filament's ends apart: about half of them agree when it cannot, and the vote is then noise."
)


def _split_name(tomo: str) -> tuple[str, str]:
    m = _POSITION_RE.match(tomo)
    return (m["source"], m["pos"]) if m else ("", tomo)


class PolarityPanel:
    def __init__(self, backend, project_path: Path, instance_id: str, species_id: str) -> None:
        self.backend = backend
        self.project_path = Path(project_path)
        self.instance_id = instance_id
        self.species_id = species_id
        self._name = ""
        self._keep_undecided = False
        self._status = None
        self._flight = SingleFlight()

    def build(self) -> None:
        self._box = ui.column().classes("w-full").style("gap: 6px;")
        with self._box:
            ui.label("reading the filaments' vote…").classes(HINT_CLS)
        owned_timer(0.05, self._load, self._box, once=True)

    async def _load(self) -> None:
        res = await self.backend.polarity_readout(self.project_path, self.instance_id)
        if self._box.is_deleted:
            return
        self._box.clear()
        with self._box:
            if not res["success"]:
                expected = res.get("code") == ErrorCode.NOT_WRITTEN_YET
                ui.label(res["error"]).classes(HINT_CLS if expected else ERROR_CLS)
                return
            self._body(res["vote"])

    def _body(self, v: dict) -> None:
        decided = v[KEPT] + v[REVERSED]
        when = "converged" if v["converged"] else f"iteration {v['iteration']}, so far"
        with ui.element("div").style("display: flex; flex-direction: column; gap: 4px; width: 100%; margin-top: 6px;"):
            with ui.element("div").style("display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap;"):
                ui.label("Filament polarity").style(PANEL_TITLE_STYLE).tooltip(VOTE_RULE)
                ui.label(f"{decided} of {v['filaments']} filaments decided · {when}").style(HELPER_STYLE)
            ui.label(
                f"{v[KEPT]} kept the trace's direction, {v[REVERSED]} reversed it, {v[UNDECIDED]} undecided "
                f"({v['segments_undecided']:,} segments); {v['segments_turned']:,} segments of decided filaments "
                "point against their filament's majority and would be turned round."
            ).style(_BODY)
            p = v.get("pieces") or {}
            if p.get("decided"):
                with ui.label(
                    f"Two ends of one filament chose the same direction in {p['agree']} of {p['decided']} filaments "
                    "long enough to test (about half when the map cannot tell the ends apart)."
                ).style(_BODY):
                    ui.tooltip(PIECES_RULE)
            else:
                ui.label("No filament is long enough to test the vote against itself (three boxes).").style(
                    f"{SANS} font-size: 11px; color: {_MUTED};"
                )
            ui.html(self._table_html(v["per_filament"]), sanitize=False).classes("cb-atable-wrap").style(
                f"max-height: 260px; {TABLE_ON_PANEL_STYLE}"
            )
            self._make_controls()

    @staticmethod
    def _table_html(rows: list[dict]) -> str:
        names = [_split_name(r["tomo"]) for r in rows]
        sources = {s for s, _ in names}
        with_source = len(sources) > 1
        head = (
            ("<th>from</th>" if with_source else "")
            + "<th>tomogram</th><th class='n'>filament</th><th class='n'>segments</th>"
            + "<th class='n' title='Segments the refinement turned round from the direction they went in with'>"
            "turned</th><th class='n'>majority</th><th>verdict</th>"
        )
        body = []
        order = sorted(range(len(rows)), key=lambda i: (rows[i]["verdict"] != UNDECIDED, names[i], rows[i]["tube"]))
        for i in order:
            r, (source, pos) = rows[i], names[i]
            muted = f" style='color:{_MUTED}'" if r["verdict"] == UNDECIDED else ""
            body.append(
                f"<tr{muted}>"
                + (f"<td>{esc(source)}</td>" if with_source else "")
                + f"<td title='{esc(r['tomo'])}'>{esc(pos)}</td><td class='n'>{r['tube']}</td>"
                f"<td class='n'>{r['segments']:,}</td><td class='n'>{r['reversed']:,}</td>"
                f"<td class='n'>{100 * r['majority']:.0f} %</td><td>{esc(r['verdict'])}</td></tr>"
            )
        return f'<table class="cb-atable"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table>'

    def _make_controls(self) -> None:
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-top: 4px;"
        ):
            house_text(
                "name",
                value="",
                placeholder="e.g. microtubule polarity",
                width="w-48",
                on_change=lambda e: setattr(self, "_name", e.value or ""),
            )
            house_check(
                "keep undecided",
                on_change=lambda e: setattr(self, "_keep_undecided", bool(e.value)),
                tooltip="Keep the undecided filaments as they went in, both directions open to the next refinement, "
                "instead of leaving them out.",
            )
            house_button(
                "Make polarity set",
                self._make,
                kind="accent",
                tooltip="A new particle set of these segments, each filament turned to its majority's direction and "
                "held there (flip ratio 0) for the next Refine 3D. The refinement is not changed.",
            )
            self._status = ui.label("").classes(HINT_CLS)

    async def _make(self) -> None:
        async with self._flight("make") as acquired:
            if not acquired:
                return
            name = self._name.strip() or "polarity"
            self._status.set_text("making the set…")
            res = await self.backend.create_polarity_set(
                self.project_path,
                refine3d_instance=self.instance_id,
                name=name,
                species_id=self.species_id,
                keep_undecided=self._keep_undecided,
            )
            if self._status.is_deleted:
                return
            if not res["success"]:
                self._status.set_text(res["error"])
                return
            ps = res["set"]
            self._status.set_text(f"Set {ps.name}: {ps.n_particles:,} segments in {ps.n_tomograms} tomograms")

"""Controls: what the Gaussian formula cannot say, measured on the user's own data.

- The Gaussian null is the dashed curve of the survival chart above.
- Control templates: make a decoy (phase-randomised: same spectrum, no shape) or the
  mirrored template (the other hand) from one of the species' templates; a template-match job run
  with it on a subset of tomograms (every Nth) is then compared here with the real run on the same
  tomograms — the decoy's picks at or above the cut over the real ones estimate the false discovery
  rate at the cut, and the mirror's peaks against the real ones tell the hand.
- Random-position decoys: random positions inside each tomogram's pick slab, added to a set and
  classified with it; each Class 3D result then counts the decoys per class.
- Recall against a reference list (hand-placed picks, or another set), per σ.
- Half-set independence of a set: particles closer than a diameter in opposite halves.

The cut-dependent numbers follow the cut in place.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from nicegui import ui

from services.particles import analytics
from services.particles.analytics_store import AnalyticsSource
from services.particles.particle_sets import ParentRef
from services.particles.set_ops import half_set_check, read_table, recall_curve, reference_table
from ui.analytics.common import HINT_CLS, LABEL_CLS, TOOLTIP_STYLE
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_number, house_select, house_text
from ui.components.reactive import SingleFlight
from ui.dashboard.figures import build_curves_chart, build_survival_chart
from ui.styles import MONO

CHART_PX = 150


class ControlsPanel:
    def __init__(
        self,
        backend,
        project_path: Path,
        species_id: str,
        *,
        rows: list[dict[str, Any]],
        controls: dict[str, tuple[AnalyticsSource, list[dict[str, Any]]]],
        templates: list[tuple[str, str]],
        selected_template: str | None,
        parents: list[ParentRef],
        own_key: str | None,
        diameter: float | None,
        cut: float,
        state_getter: Callable[[], Any],
        on_changed: Callable[[], Any],
    ) -> None:
        self.backend = backend
        self.project_path = project_path
        self.species_id = species_id
        self.cand_rows = [r["cand"] for r in rows if r.get("cand")]
        self.controls = controls
        self.templates = templates
        self.template = selected_template or (templates[0][0] if templates else None)
        self.parents = parents
        self.own_key = own_key
        self.diameter = diameter
        self.cut = cut
        self._state = state_getter
        self._on_changed = on_changed
        self._flight = SingleFlight()
        self._decoy_key = next((k for k, (s, _r) in controls.items() if s.control == "decoy"), None)
        self._mirror_key = next((k for k, (s, _r) in controls.items() if s.control == "mirror"), None)
        self._fdr_text = None
        self._fdr_chart = None
        self._hand_text = None
        self._recall = {"ref": "picklists", "radius": diameter / 2 if diameter else None, "result": None}
        self._recall_text = None
        self._recall_chart = None
        self._halves_text = None
        self._halves = {"set": next((p.key for p in parents if p.level.value == "extracted"), None)}

    def build(self) -> None:
        ui.label(
            "The Gaussian null is the dashed curve of the survival chart above; these are measured on your data."
        ).classes(HINT_CLS)
        self._control_templates()
        self._decoy_positions()
        self._recall_block()
        self._halves_block()

    # ── Control templates ──

    def _control_templates(self) -> None:
        with ui.label("control templates").classes(LABEL_CLS):
            ui.tooltip(
                "Decoy: the template with its Fourier phases randomised — the same amplitude spectrum and mask, no "
                "shape. Mirror: the template flipped along Z, the other hand. Each is registered on the species, not "
                "selected. Then add a Template match job that uses it (same mask and settings, every Nth tomogram, "
                "e.g. 10) and a Pick candidates job with the same cutoff; the comparison appears here."
            ).style(TOOLTIP_STYLE)
        with ui.element("div").style("display: flex; align-items: center; gap: 10px; flex-wrap: wrap;"):
            if not self.templates:
                ui.label("The species has no template to make a control of.").classes(HINT_CLS)
            else:
                house_select(
                    "from", dict(self.templates), value=self.template, width="w-72", on_change=self._set_template
                )
                house_button("Make decoy template", lambda: self._make("decoy"))
                house_button("Make mirrored template", lambda: self._make("mirror"))
        if self._decoy_key:
            self._fdr_block()
        if self._mirror_key:
            self._hand_block()

    def _set_template(self, e) -> None:
        self.template = e.value

    async def _make(self, kind: str) -> None:
        async with self._flight("make") as acquired:
            if not acquired or not self.template:
                return
            res = await self.backend.make_control_template(self.project_path, self.species_id, self.template, kind)
            if not res["success"]:
                ui.notify(res["error"], type="negative")
                return
            ui.notify(
                f"{Path(res['path']).name} registered on the species. Add a Template match job that uses it.",
                type="positive",
                multi_line=True,
            )
            self._on_changed()

    def _fdr_block(self) -> None:
        decoys = {k: s.label for k, (s, _r) in self.controls.items() if s.control == "decoy"}
        with ui.element("div").style("display: flex; align-items: center; gap: 10px; flex-wrap: wrap;"):
            house_select("decoy run", decoys, value=self._decoy_key, width="w-96", on_change=self._set_decoy)
            self._fdr_text = ui.label("").style(f"{MONO} font-size: 10px; color: #1e293b;")
        with ui.element("div").style("width: 100%; max-width: 560px;"):
            self._fdr_chart = ui.echart(self._fdr_options()).style(f"height: {CHART_PX}px; width: 100%;")
        self._update_fdr()

    def _set_decoy(self, e) -> None:
        self._decoy_key = e.value
        self._update_fdr()

    def _fdr_options(self) -> dict:
        decoy_rows = self.controls[self._decoy_key][1] if self._decoy_key in self.controls else []
        f = analytics.fdr_curve(self.cand_rows, decoy_rows)
        return build_survival_chart(
            f["z"], f["target"], [], cut=self.cut, extra=[{"name": "decoy template", "y": f["decoy"]}]
        )

    def _update_fdr(self) -> None:
        if self._fdr_text is None or self._fdr_text.is_deleted or self._decoy_key not in self.controls:
            return
        nd, nt, ratio, n = analytics.fdr_at(self.cand_rows, self.controls[self._decoy_key][1], self.cut)
        self._fdr_text.set_text(
            f"at {self.cut:.1f}σ on the {n} tomograms both ran: {nd:,} decoy / {nt:,} real picks"
            + (f" — false discovery rate ≈ {100 * ratio:.1f} %" if ratio is not None else "")
        )
        if self._fdr_chart is not None and not self._fdr_chart.is_deleted:
            self._fdr_chart.options.clear()
            self._fdr_chart.options.update(self._fdr_options())
            self._fdr_chart.update()

    def _hand_block(self) -> None:
        mirrors = {k: s.label for k, (s, _r) in self.controls.items() if s.control == "mirror"}
        with ui.element("div").style("display: flex; align-items: center; gap: 10px; flex-wrap: wrap;"):
            house_select("mirror run", mirrors, value=self._mirror_key, width="w-96", on_change=self._set_mirror)
            self._hand_text = ui.label("").style(f"{MONO} font-size: 10px; color: #1e293b;")
        self._update_hand()

    def _set_mirror(self, e) -> None:
        self._mirror_key = e.value
        self._update_hand()

    def _update_hand(self) -> None:
        if self._hand_text is None or self._mirror_key not in self.controls:
            return
        h = analytics.hand_comparison(self.cand_rows, self.controls[self._mirror_key][1])
        if not h["n_common"]:
            self._hand_text.set_text("no tomogram with picks in both runs")
            return
        self._hand_text.set_text(
            f"the template's highest peak beats its mirror's in {h['n_template_higher']} of {h['n_common']} tomograms "
            f"(median {h['median_difference']:+.2f}σ)"
        )

    # ── Random-position decoys ──

    def _decoy_positions(self) -> None:
        with ui.label("random-position decoys").classes(LABEL_CLS):
            ui.tooltip(
                "Random positions inside each tomogram's pick slab, at least a diameter from every pick, added to a "
                "set as particles that score 0σ. Extract the set and classify it: each Class 3D result below counts "
                "the decoys per class — the share in the good classes is the classifier's false-acceptance rate."
            ).style(TOOLTIP_STYLE)
        with ui.element("div").style("display: flex; align-items: center; gap: 10px;"):
            house_button("Make decoy positions…", self._decoy_dialog)

    async def _decoy_dialog(self) -> None:
        async with self._flight("decoys") as acquired:
            if not acquired:
                return
            coords = [p for p in self.parents if p.kind != "merge"]
            if not coords:
                ui.notify("No candidates or set to add decoys to.", type="warning")
                return
            st = {
                "parent": self.own_key if any(p.key == self.own_key for p in coords) else coords[0].key,
                "fraction": 0.2,
                "dist": self.diameter,
                "seed": 1,
                "name": "with decoys",
            }
            with dialog_host(), ui.dialog() as dlg, ui.card().style("width: 600px; max-width: 95vw; gap: 8px;"):
                ui.label("Random-position decoys").style("font-size: 13px; font-weight: 600; color: #0f172a;")
                ui.label(
                    "The new set holds the parent's picks plus the decoys, as coordinates: extract it, then classify."
                ).classes(HINT_CLS)
                by_key = {p.key: p for p in coords}
                house_select(
                    "from",
                    {k: p.label for k, p in by_key.items()},
                    value=st["parent"],
                    width="w-96",
                    on_change=lambda e: st.update(parent=e.value),
                )
                house_number(
                    "decoys per pick",
                    value=st["fraction"],
                    step=0.05,
                    min=0.01,
                    width="w-20",
                    on_change=lambda e: st.update(fraction=e.value),
                )
                house_number(
                    "min distance Å",
                    value=st["dist"],
                    step=10,
                    min=1,
                    width="w-20",
                    hint="From every pick and every other decoy; starts at the species diameter.",
                    on_change=lambda e: st.update(dist=e.value),
                )
                house_number(
                    "seed", value=st["seed"], step=1, min=0, width="w-16", on_change=lambda e: st.update(seed=e.value)
                )
                house_text("name", value=st["name"], width="w-64", on_change=lambda e: st.update(name=e.value))
                with ui.row().classes("w-full justify-end gap-2"):
                    house_button("Cancel", lambda: dlg.submit(False))
                    house_button("Make set", lambda: dlg.submit(True), kind="accent")
            go = await dlg
            dlg.delete()
            if not go:
                return
            if not st["fraction"] or not st["dist"]:
                ui.notify("Decoys per pick and a minimum distance are needed.", type="warning")
                return
            res = await self.backend.create_decoy_set(
                self.project_path,
                parent=by_key[st["parent"]],
                fraction=float(st["fraction"]),
                min_distance_ang=float(st["dist"]),
                seed=int(st["seed"] or 0),
                name=(st["name"] or "with decoys").strip(),
                species_id=self.species_id,
            )
            if not res["success"]:
                ui.notify(res["error"], type="negative", multi_line=True)
                return
            ps = res["set"]
            ui.notify(f"Set {ps.name}: {ps.details.get('n_decoys', 0):,} decoys added", type="positive")
            self._on_changed()

    # ── Recall ──

    def _recall_block(self) -> None:
        own = next((p for p in self.parents if p.key == self.own_key), None)
        if own is None:
            return
        refs = {"picklists": "the species' hand-placed and imported pick lists"}
        refs.update({p.key: p.label for p in self.parents if p.key != self.own_key})
        with ui.label("recall against a reference").classes(LABEL_CLS):
            ui.tooltip(
                "Over the tomograms the reference covers: the share of reference picks with a candidate at or above "
                "σ within the radius (recall), and the share of candidates at or above σ within the radius of a "
                "reference pick (precision). Coordinates must share the tomograms' frame."
            ).style(TOOLTIP_STYLE)
        with ui.element("div").style("display: flex; align-items: center; gap: 10px; flex-wrap: wrap;"):
            house_select(
                "reference",
                refs,
                value=self._recall["ref"],
                width="w-72",
                on_change=lambda e: self._recall.update(ref=e.value),
            )
            house_number(
                "within Å",
                value=self._recall["radius"],
                step=10,
                min=1,
                width="w-20",
                hint="Starts at the species radius.",
                on_change=lambda e: self._recall.update(radius=e.value),
            )
            house_button("Compute", lambda: self._compute_recall(own))
            self._recall_text = ui.label("").style(f"{MONO} font-size: 10px; color: #1e293b;")
        with ui.element("div").style("width: 100%; max-width: 560px;"):
            self._recall_chart = ui.echart(build_curves_chart([], [], x_name="σ", y_name="share")).style(
                f"height: {CHART_PX}px; width: 100%;"
            )

    async def _compute_recall(self, own: ParentRef) -> None:
        async with self._flight("recall") as acquired:
            if not acquired:
                return
            radius = self._recall["radius"]
            if not radius:
                self._recall_text.set_text("Give a radius.")
                return
            state = self._state()

            def run():
                ref = reference_table(state, self.project_path, self.species_id, self._recall["ref"])
                return recall_curve(read_table(own.particles_star).df, ref, float(radius))

            self._recall_text.set_text("computing…")
            res = await self.backend.particle_control(run)
            if self._recall_text.is_deleted:
                return
            if not res["success"]:
                self._recall_text.set_text(res["error"])
                return
            self._recall["result"] = res["result"]
            self._update_recall()

    def _update_recall(self) -> None:
        r = self._recall.get("result")
        if not r or self._recall_text is None or self._recall_text.is_deleted:
            return
        z = r["z"]
        at = next((i for i, zz in enumerate(z) if zz >= self.cut), None)
        if at is not None:
            prec = r["precision"][at]
            self._recall_text.set_text(
                f"{r['n_reference']:,} reference picks in {r['n_tomograms']} tomograms; at {self.cut:.1f}σ recall "
                f"{100 * r['recall'][at]:.0f} %" + ("" if prec is None else f", precision {100 * prec:.0f} %")
            )
        self._recall_chart.options.clear()
        self._recall_chart.options.update(
            build_curves_chart(
                z,
                [{"name": "recall", "y": r["recall"]}, {"name": "precision", "y": r["precision"]}],
                x_name="σ cut",
                y_name="share",
                y_max=1.0,
                cut=self.cut,
            )
        )
        self._recall_chart.update()

    # ── Half-set independence ──

    def _halves_block(self) -> None:
        sets = {p.key: p.label for p in self.parents if p.level.value == "extracted"}
        if not sets:
            return
        with ui.label("half-set independence").classes(LABEL_CLS):
            ui.tooltip(
                "Pairs of particles closer than the species diameter that sit in opposite halves (rlnRandomSubset 1 "
                "and 2): the same particle, or overlapping density, in both halves inflates the FSC. Only sets whose "
                "particles carry halves (after a refinement) can be checked; a split by tomogram avoids the problem."
            ).style(TOOLTIP_STYLE)
        with ui.element("div").style("display: flex; align-items: center; gap: 10px; flex-wrap: wrap;"):
            house_select(
                "set",
                sets,
                value=self._halves["set"],
                width="w-72",
                on_change=lambda e: self._halves.update(set=e.value),
            )
            house_button("Check", self._check_halves)
            self._halves_text = ui.label("").style(f"{MONO} font-size: 10px; color: #1e293b;")

    async def _check_halves(self) -> None:
        async with self._flight("halves") as acquired:
            if not acquired:
                return
            ref = next((p for p in self.parents if p.key == self._halves["set"]), None)
            if ref is None or not self.diameter:
                self._halves_text.set_text("Pick a set; the species needs a diameter.")
                return
            res = await self.backend.particle_control(
                lambda: half_set_check(read_table(ref.particles_star).df, float(self.diameter))
            )
            if not res["success"]:
                self._halves_text.set_text(res["error"])
                return
            h = res["result"]
            if not h["has_halves"]:
                self._halves_text.set_text("no rlnRandomSubset in this set: the halves are made at reconstruction")
            else:
                self._halves_text.set_text(
                    f"{h['cross_pairs']:,} pairs closer than {self.diameter:g} Å across halves, involving "
                    f"{h['n_involved']:,} of {h['n']:,} particles"
                )

    # ── The cut ──

    def set_cut(self, cut: float) -> None:
        self.cut = cut
        self._update_fdr()
        self._update_recall()

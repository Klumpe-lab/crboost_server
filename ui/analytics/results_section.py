"""Results of the jobs after extraction — Class 3D and Reconstruct particle — for one species, one
job at a time. Used by the Species page's Analytics tab (a job selector) and by those jobs' own
Results tab (one fixed job). The readout is fetched off the event loop; the first read of a job that
finished before its driver wrote a sidecar renders its images and takes a while.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from nicegui import ui

from services.particles.records import Record
from services.result import ErrorCode
from services.particles.set_ops import class_agreement
from ui.analytics.common import ERROR_CLS, HINT_CLS, LABEL_CLS, TOOLTIP_STYLE
from ui.analytics.readout_panel import ReadoutPanel
from ui.components.buttons import house_button
from ui.components.fields import house_select
from ui.components.reactive import SingleFlight, owned_timer
from ui.styles import MONO


class ResultsSection:
    def __init__(
        self,
        backend,
        project_path: Path,
        species_id: str,
        jobs: list[tuple[str, str]],
        *,
        on_set_created: Callable[[], Any] | None = None,
    ) -> None:
        self.backend = backend
        self.project_path = project_path
        self.species_id = species_id
        self.jobs = jobs
        self._on_set_created = on_set_created
        self._chosen = jobs[0][0] if jobs else None
        self._box = None
        self._flight = SingleFlight()

    def build(self) -> None:
        if not self.jobs:
            ui.label("No Class 3D or Reconstruct particle job for this species yet.").classes(HINT_CLS)
            return
        if len(self.jobs) > 1:
            house_select("job", dict(self.jobs), value=self._chosen, width="w-72", on_change=self._on_choose)
        self._box = ui.column().classes("w-full gap-2")
        owned_timer(0.05, self._load, self._box, once=True)

    async def _on_choose(self, e) -> None:
        if e.value and e.value != self._chosen:
            self._chosen = e.value
            await self._load()

    async def _load(self) -> None:
        """Fetch and draw the chosen job's readout; a choice made while one is fetched is fetched next."""
        async with self._flight("load") as acquired:
            if not acquired or self._box is None:
                return
            while True:
                iid = self._chosen
                self._box.clear()
                with self._box:
                    ui.label("reading the job's results…").classes(HINT_CLS)
                res = await self.backend.particle_readout(self.project_path, iid)
                if self._box.is_deleted:
                    return
                if iid == self._chosen:
                    break
            self._box.clear()
            with self._box:
                if not res["success"]:
                    expected = res.get("code") == ErrorCode.NOT_WRITTEN_YET
                    ui.label(res["error"]).classes(HINT_CLS if expected else ERROR_CLS)
                    return
                record: Record = res["record"]
                is_class3d = record.provenance.kind == "class3d"
                make = self._make_subset(iid) if is_class3d else None
                ReadoutPanel(record, decoys=res.get("decoys"), on_make_subset=make).build()
                others = [(j, label) for j, label in self.jobs if j != iid] if is_class3d else []
                if others:
                    self._compare_controls(record, others)

    def _compare_controls(self, record: Record, others: list[tuple[str, str]]) -> None:
        """How this classification's membership agrees with another's (a repeat with another seed, or
        another number of classes): for each class here, the class there most of its particles went to."""
        st = {"other": others[0][0]}
        with ui.element("div").style("display: flex; align-items: center; gap: 10px; flex-wrap: wrap;"):
            with ui.label("compare membership").classes(LABEL_CLS):
                ui.tooltip(
                    "Particles matched by name (or position) between this classification's last iteration and "
                    "another's: a stable class keeps its particles across a repeat with another seed."
                ).style(TOOLTIP_STYLE)
            house_select(
                "with", dict(others), value=st["other"], width="w-64", on_change=lambda e: st.update(other=e.value)
            )
            out = ui.label("").style(f"{MONO} font-size: 10px; color: #1e293b; white-space: pre-line;")

            async def compare() -> None:
                async with self._flight("compare") as acquired:
                    if not acquired:
                        return
                    out.set_text("comparing…")
                    other = await self.backend.particle_readout(self.project_path, st["other"])
                    if not other["success"]:
                        out.set_text(other["error"])
                        return
                    res = await self.backend.particle_control(
                        class_agreement, Path(record.data["data_star"]), Path(other["record"].data["data_star"])
                    )
                    if not res["success"]:
                        out.set_text(res["error"])
                        return
                    a = res["result"]
                    lines = [f"{a['n_common']:,} shared particles; {100 * a['agreement']:.0f} % stay together"]
                    lines += [
                        f"class {c['class']} ({c['n']:,}) → class {c['best']} ({100 * c['share']:.0f} %)"
                        for c in a["classes"]
                    ]
                    out.set_text("\n".join(lines))

            house_button("Compare", compare)

    def _make_subset(self, iid: str):
        async def make(classes: list[int], name: str) -> dict:
            res = await self.backend.create_class_subset(
                self.project_path, class3d_instance=iid, classes=classes, name=name, species_id=self.species_id
            )
            if res["success"] and self._on_set_created is not None:
                self._on_set_created()
            return res

        return make

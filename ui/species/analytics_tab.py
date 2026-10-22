"""Analytics tab — the particle stage of one species at a glance, in σ, and its particle sets.

Top to bottom: which template match / extraction the numbers describe, where the numbers come from
(driver sidecar or backfill, hover for sources), the score panel (stat tiles, σ ladder, the cut with
live counts, the σ histogram and the survival curve against Gaussian noise), the funnel, the
markers, the per-tomogram table, and the species' particle sets with their lineage.

Everything shown is a record (`services/particles/analytics_store.py`) or a set
(`services/particles/particle_sets.py`); this tab reads no job file itself. Both are fetched off the
event loop when the in-memory key (registry rev, job statuses, the chosen source) moves, on show,
and at most every `_DISK_REFRESH_S` while shown; a record whose sources did not change is served
from memory. The view is a `FingerprintedView` over their identities; moving the cut updates
numbers and charts in place and rebuilds nothing.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nicegui import ui

from services.jobs.spec import display_name
from services.models_base import JobType
from services.particles import analytics
from services.particles.analytics_store import (
    AnalyticsSource,
    candidates_record,
    job_dir_of,
    sources_for_species,
    species_diameter,
    subtomo_instances_for_ce,
    subtomo_record,
    table_record,
    tm_health_record,
    tomogram_quality,
)
from services.particles.particle_sets import ParentRef, ParticleSet, SetOp, jobs_reading_set, list_sets
from services.particles.records import Record
from services.particles.set_ops import parent_options, parent_options_all_species, read_table
from services.particles.species_jobs import jobs_for_species
from services.project_state import get_project_state_for
from ui.analytics.controls_panel import ControlsPanel
from ui.analytics.common import ERROR_CLS, HINT_CLS, LABEL_CLS, TOOLTIP_STYLE, panel, render_provenance
from ui.analytics.funnel import FunnelLine, FunnelStep
from ui.analytics.project_breakdown import ProjectBreakdown
from ui.analytics.results_section import ResultsSection
from ui.analytics.score_panel import ScorePanel
from ui.analytics.sets_panel import SetsPanel
from ui.analytics.spatial_panel import SpatialPanel
from ui.analytics.subset_dialog import SubsetDefaults, open_subset_dialog
from ui.analytics.tomogram_table import MARKER_LABELS, TomogramTable
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_select
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.job_plugins._field_styles import panel_head
from ui.species.tab import TabContext
from ui.styles import SANS

logger = logging.getLogger(__name__)

_DISK_REFRESH_S = 15.0


@dataclass
class _Subtomo:
    instance_id: str
    record: Record | None
    n_curated: int | None  # None: no curated subset exists

    @property
    def n_all(self) -> int | None:
        return None if self.record is None else self.record.data.get("n_out")


@dataclass
class _Computed:
    sources: tuple[AnalyticsSource, ...] = ()
    source: AnalyticsSource | None = None
    tm: Record | None = None
    cand: Record | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)
    markers: list[analytics.Marker] = field(default_factory=list)
    subtomo: _Subtomo | None = None
    sets: list[ParticleSet] = field(default_factory=list)
    readers: dict[str, list[str]] = field(default_factory=dict)
    stale: dict[str, list[str]] = field(default_factory=dict)
    parents: list[ParentRef] = field(default_factory=list)
    quality: dict[str, dict[str, Any]] = field(default_factory=dict)  # registry facts per tomogram
    others: list[ParentRef] = field(default_factory=list)  # other species' tables, for the overlap
    result_jobs: list[tuple[str, str]] = field(default_factory=list)  # Class 3D / Reconstruct particle
    # Control runs (decoy / mirror template) of the species: key → (source, candidate rows).
    controls: dict[str, tuple[AnalyticsSource, list[dict[str, Any]]]] = field(default_factory=dict)
    templates: list[tuple[str, str]] = field(default_factory=list)
    selected_template: str | None = None
    diameter: float | None = None
    notes: tuple[str, ...] = ()  # expected absences ("no candidates yet")
    errors: tuple[str, ...] = ()  # failures, traceback in the server log

    def signature(self) -> tuple:
        def rec_id(r: Record | None):
            return None if r is None else (r.provenance.fingerprint, r.provenance.computed_at)

        return (
            tuple((s.key, s.label, s.succeeded) for s in self.sources),
            self.source.key if self.source else None,
            rec_id(self.tm),
            rec_id(self.cand),
            None
            if self.subtomo is None
            else (self.subtomo.instance_id, rec_id(self.subtomo.record), self.subtomo.n_curated),
            tuple(sorted((k, tuple(sorted(v.items()))) for k, v in self.quality.items())),
            tuple((s.id, s.n_particles) for s in self.sets),
            tuple(sorted((k, tuple(v)) for k, v in self.readers.items())),
            tuple(sorted((k, tuple(v)) for k, v in self.stale.items())),
            tuple(p.key for p in self.parents),
            tuple(p.key for p in self.others),
            tuple(self.result_jobs),
            tuple((k, s.label, len(r)) for k, (s, r) in self.controls.items()),
            tuple(self.templates),
            self.notes,
            self.errors,
        )


def _count_rows(path: Path) -> int | None:
    try:
        return len(read_table(path).df)
    except (OSError, ValueError) as e:
        logger.warning("Could not count rows of %s: %s", path, e)
        return None


def _compute(state, project_path: Path, species_id: str, wanted: str | None) -> _Computed:
    sources = tuple(sources_for_species(state, species_id, project_path))
    src = (
        next((s for s in sources if s.key == wanted), None)
        or next((s for s in sources if s.succeeded and not s.control), None)
        or next((s for s in sources if s.succeeded), None)
        or (sources[0] if sources else None)
    )
    out = _Computed(sources=sources, source=src)
    notes: list[str] = []
    errors: list[str] = []
    if src is not None and src.kind != "jobs":
        try:
            out.cand = table_record(project_path, src)
        except FileNotFoundError as e:
            notes.append(str(e))
        except Exception as e:
            logger.exception("Analytics record for %s failed", src.key)
            errors.append(f"{src.label}: {type(e).__name__}: {e}")
        out.rows = analytics.tomogram_rows(None, out.cand.data if out.cand else None)
    elif src is not None:
        for attr, instance, fetch in (
            ("tm", src.tm_instance, tm_health_record),
            ("cand", src.ce_instance, candidates_record),
        ):
            if not instance:
                continue
            try:
                setattr(out, attr, fetch(project_path, state, instance))
            except FileNotFoundError as e:
                notes.append(str(e))  # not run yet: said, not an error
            except Exception as e:
                logger.exception("Analytics record for %s failed", instance)
                errors.append(f"{instance}: {type(e).__name__}: {e}")
        if src.ce_instance:
            subtomos = subtomo_instances_for_ce(state, src.ce_instance, project_path)
            sub_dir = job_dir_of(state, project_path, subtomos[0]) if subtomos else None
            if sub_dir is not None and (sub_dir / "particles.star").exists():
                curated = sub_dir / "particles_filtered.star"
                try:
                    sub_rec = subtomo_record(project_path, state, subtomos[0])
                except (OSError, ValueError) as e:
                    logger.exception("Subtomo record for %s failed", subtomos[0])
                    errors.append(f"{subtomos[0]}: {type(e).__name__}: {e}")
                    sub_rec = None
                out.subtomo = _Subtomo(subtomos[0], sub_rec, _count_rows(curated) if curated.exists() else None)
        out.rows = analytics.tomogram_rows(
            out.tm.data if out.tm else None,
            out.cand.data if out.cand else None,
            out.subtomo.record.data if out.subtomo and out.subtomo.record else None,
        )
        out.markers = analytics.tomogram_markers(out.rows)
        try:
            out.quality = tomogram_quality(project_path)
        except (OSError, ValueError) as e:
            logger.exception("Registry facts unavailable for %s", project_path)
            notes.append(f"Data-quality facts unavailable: {e}")
    out.sets = [s for s in list_sets(project_path) if s.species_id == species_id]
    out.readers = {s.id: r for s in out.sets if (r := jobs_reading_set(state, s.id))}
    out.stale = {s.id: [p.label for p in st] for s in out.sets if (st := s.stale_parents())}
    out.parents = parent_options(state, project_path, species_id)
    out.others = [p for p in parent_options_all_species(state, project_path) if p.species_id != species_id]
    for ctl in sources:
        if not ctl.control or not ctl.ce_instance or ctl is src:
            continue
        try:
            rec = candidates_record(project_path, state, ctl.ce_instance)
        except FileNotFoundError:
            continue  # the control run has not extracted yet
        except Exception as e:
            logger.exception("Control record for %s failed", ctl.ce_instance)
            errors.append(f"{ctl.label}: {type(e).__name__}: {e}")
            continue
        out.controls[ctl.key] = (ctl, list(rec.data.get("tomograms") or []))
    sp = state.get_species(species_id)
    if sp is not None:
        out.templates = [(t.id, Path(t.template_path).name) for t in sp.templates]
        out.selected_template = sp.selected_template_id or None
    out.result_jobs = [
        (iid, f"{display_name(jm.job_type)} {Path(jm.relion_job_name.rstrip('/')).name if jm.relion_job_name else iid}")
        for iid, jm in jobs_for_species(state, species_id)
        if jm.job_type in (JobType.CLASS3D, JobType.RECONSTRUCT_PARTICLE)
    ][::-1]
    out.diameter = species_diameter(state, src.ce_instance or src.tm_instance) if src else None
    out.notes, out.errors = tuple(notes), tuple(errors)
    return out


class _AnalyticsView(FingerprintedView):
    def __init__(self, container: ui.element, tab: AnalyticsTab) -> None:
        super().__init__(container)
        self._tab = tab
        self._funnel: FunnelLine | None = None
        self._breakdown: ProjectBreakdown | None = None
        self._spatial: SpatialPanel | None = None
        self._controls: ControlsPanel | None = None

    def signature(self) -> Any:
        c = self._tab.computed
        return (self._tab.ctx.species_id, None if c is None else c.signature())

    def render(self) -> None:
        self._funnel = None
        self._breakdown = None
        self._spatial = None
        self._controls = None
        c = self._tab.computed
        # Each block is a panel; the slot's gap separates them.
        with panel():
            with panel_head(
                "Scores in σ",
                tooltip="Template-matching scores divided by each tomogram's noise level σ, so one cut means the "
                "same everywhere. Rules on this page mark tomograms; nothing is dropped by them.",
            ):
                if c is not None and c.sources:
                    house_select(
                        "source",
                        {s.key: s.label for s in c.sources},
                        value=c.source.key if c.source else None,
                        width="w-96",
                        hint="Which template match and extraction the numbers describe.",
                        on_change=lambda e: self._tab.choose_source(e.value),
                    )
                house_button("Refresh", self._tab.refresh_now, tooltip="Re-read the records now")
            if c is None:
                ui.label("computing…").classes(HINT_CLS)
                return
            for e in c.errors:
                ui.label(f"Unavailable — {e}").classes(ERROR_CLS).tooltip("Full traceback in the server log.")
            if not c.sources:
                ui.label(
                    "No template match or candidate extraction for this species yet — add them from the Jobs tab."
                ).classes(HINT_CLS)
            for n in c.notes:
                ui.label(n).classes(HINT_CLS)
            if c.rows:
                render_provenance([("template match", c.tm), ("candidates", c.cand)])
                ScorePanel(
                    c.rows,
                    c.cand.data if c.cand else None,
                    cut=self._tab.cut_for(c),
                    on_cut=lambda v: self._on_cut(c, v),
                    on_make_subset=(lambda: self._tab.make_subset(c)) if c.cand else None,
                ).build()
        if c.rows:
            self._render_scores(c)
        with panel(
            "Particle sets",
            tooltip="Every cut, split or class subset of particles is a new set: its parent is never changed, "
            "and its recipe (parent, operation, parameters) and funnel are recorded. A set is offered as an "
            "input in the I/O section of Subtomo extraction, Reconstruct particle and Class 3D.",
        ):
            SetsPanel(c.sets, readers=c.readers, stale=c.stale, on_delete=self._tab.delete_set).build()
        with panel(
            "Classes and maps",
            tooltip="Results of this species' Class 3D and Reconstruct particle jobs: convergence, classes "
            "against score, tomogram and project, class maps and members, half-map FSC. Each job's Results "
            "tab shows the same.",
        ):
            ResultsSection(
                self._tab.backend,
                self._tab.ctx.project_path,
                self._tab.ctx.species_id,
                c.result_jobs,
                on_set_created=lambda: asyncio.create_task(self._tab.refresh_now()),
            ).build()

    def _render_scores(self, c: _Computed) -> None:
        """The panels after the scores: per project, where the picks sit, the tomograms, the controls."""
        cut = self._tab.cut_for(c)
        projects = (c.cand.data.get("projects") or {}) if c.cand else {}
        if projects:
            with panel(
                "Per project",
                tooltip="This merge's particles by the project they came from: one row per project, each row's "
                "tomograms and particles, the σ ladder and the count at or above the cut.",
            ):
                self._breakdown = ProjectBreakdown(c.rows, projects, cut=cut)
                self._breakdown.build()
        with panel(
            "Where the picks sit",
            tooltip="The funnel from template match to refinement, the pick slab, faces, neighbours, the extraction's "
            "losses, and picks at or above the cut against data quality per tomogram.",
        ):
            steps, cut_index = self._funnel_steps(c)
            if steps:
                self._funnel = FunnelLine(
                    steps,
                    [r["cand"] for r in c.rows if r.get("cand")],
                    cut=cut,
                    cut_index=cut_index,
                    open_job=self._tab.ctx.callbacks.get("open_job"),
                )
                self._funnel.build()
            self._spatial = SpatialPanel(
                c.rows,
                c.quality,
                c.subtomo.record.data if c.subtomo and c.subtomo.record else None,
                cut=cut,
                nn_close_ang=(c.cand.data.get("nn_close_ang") if c.cand else None),
                others=c.others,
                on_overlap=self._overlap_handler(c),
            )
            self._spatial.build()
        with panel(
            "Tomograms",
            tooltip="One row per tomogram of the template match. Hover a cell for its detail, a marker for its rule.",
        ):
            self._render_marker_summary(c)
            TomogramTable(c.rows, c.markers).build()
        if c.source is not None and c.source.kind == "jobs":
            with panel(
                "Controls",
                tooltip="Checks of the scores measured on this data rather than taken from the Gaussian formula: "
                "control templates (decoy, mirror), random-position decoys, recall against a reference, half-set "
                "independence.",
            ):
                own_key = f"job:{c.source.ce_instance}:candidates" if c.source.ce_instance else None
                self._controls = ControlsPanel(
                    self._tab.backend,
                    self._tab.ctx.project_path,
                    self._tab.ctx.species_id,
                    rows=c.rows,
                    controls=c.controls,
                    templates=c.templates,
                    selected_template=c.selected_template,
                    parents=c.parents,
                    own_key=own_key,
                    diameter=c.diameter,
                    cut=cut,
                    state_getter=lambda: get_project_state_for(self._tab.ctx.project_path),
                    on_changed=lambda: asyncio.create_task(self._tab.refresh_now()),
                )
                self._controls.build()

    def _overlap_handler(self, c: _Computed):
        """The overlap count for this source's own table at the current cut, or None when the source has
        no table a set could be read from."""
        own_key = c.source.key if c.source and c.source.kind != "jobs" else None
        if own_key is None and c.source and c.source.ce_instance:
            own_key = f"job:{c.source.ce_instance}:candidates"
        own = next((p for p in c.parents if p.key == own_key), None)
        if own is None:
            return None

        async def count(other: ParentRef, radius: float, min_sigma: float | None) -> dict:
            return await self._tab.backend.particle_overlap(own, self._tab.cut_for(c), other, min_sigma, radius)

        return count

    def _on_cut(self, c: _Computed, value: float) -> None:
        self._tab.set_cut(c, value)
        if self._funnel is not None:
            self._funnel.set_cut(value)
        if self._breakdown is not None:
            self._breakdown.set_cut(value)
        if self._spatial is not None:
            self._spatial.set_cut(value)
        if self._controls is not None:
            self._controls.set_cut(value)

    def _funnel_steps(self, c: _Computed) -> tuple[list[FunnelStep], int]:
        steps: list[FunnelStep] = []
        src = c.source
        if c.tm is not None and src and src.tm_instance:
            d = c.tm.data
            steps.append(
                FunnelStep(
                    "TM tomograms", f"{d['n_done']}/{d['n_tomograms']}", "Template-matched tomograms.", src.tm_instance
                )
            )
        cut_index = len(steps) + 1
        if c.cand is not None and src and src.ce_instance:
            d = c.cand.data
            steps.append(
                FunnelStep(
                    "candidates",
                    f"{d['n_total']:,}",
                    f"Picks the extraction kept, in {d['n_with_picks']} tomograms.",
                    src.ce_instance,
                )
            )
        else:
            return steps, len(steps) + 1
        sub = c.subtomo
        steps.append(
            FunnelStep(
                "extracted",
                "—" if sub is None or sub.n_all is None else f"{sub.n_all:,}",
                "Particles of the newest subtomo extraction made from these candidates (all of its candidates; out of "
                "bounds ones are lost)."
                if sub
                else "No subtomo extraction reads these candidates yet.",
                sub.instance_id if sub else None,
            )
        )
        steps.append(
            FunnelStep(
                "curated",
                "—" if sub is None or sub.n_curated is None else f"{sub.n_curated:,}",
                "Particles kept by the gallery's keep/drop curation (particles_filtered.star)."
                if sub and sub.n_curated is not None
                else "No curated subset of the extraction yet.",
                sub.instance_id if sub and sub.n_curated is not None else None,
            )
        )
        class_sets = [s for s in c.sets if s.recipe.op == SetOp.CLASS_SUBSET]
        steps.append(
            FunnelStep(
                "class-kept",
                f"{class_sets[-1].n_particles:,}" if class_sets else "—",
                f"The newest class subset: {class_sets[-1].name}." if class_sets else "No class subset made yet.",
            )
        )
        steps.append(FunnelStep("refined", "—", "Refinement runs outside crboost; nothing to count here yet."))
        return steps, cut_index

    def _render_marker_summary(self, c: _Computed) -> None:
        if not c.markers:
            return
        counts = Counter(m.key for m in c.markers)
        with ui.element("div").style("display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap;"):
            ui.label("markers").classes(LABEL_CLS)
            for key, n in counts.items():
                with ui.label(f"{MARKER_LABELS.get(key, key)} {n}").style(
                    f"{SANS} font-size: 10px; color: #b45309; cursor: help;"
                ):
                    ui.tooltip(f"{analytics.MARKER_RULES[key]}\n{analytics.MARKERS_DROP_NOTHING}").style(TOOLTIP_STYLE)


class AnalyticsTab:
    def __init__(self, ctx: TabContext) -> None:
        self.ctx = ctx
        self.computed: _Computed | None = None
        self._wanted: str | None = None
        self._cuts: dict[str, float] = {}
        self._last_key: Any = None
        self._last_at = 0.0
        self._flight = SingleFlight()
        self._view: _AnalyticsView | None = None

    @property
    def backend(self):
        return self.ctx.backend

    # ── Tab protocol ──

    def build(self, container: ui.element) -> None:
        with container:
            slot = ui.column().classes("w-full p-2").style("gap: 10px;")
        self._view = _AnalyticsView(slot, self)
        self._view.refresh()

    def refresh(self) -> None:
        key = self._memory_key()
        stale = time.monotonic() - self._last_at >= _DISK_REFRESH_S
        if self.computed is None or key != self._last_key or stale:
            asyncio.create_task(self._recompute(key))
        elif self._view is not None:
            self._view.refresh()

    async def refresh_now(self) -> None:
        self._last_at = 0.0
        self.refresh()

    def _memory_key(self) -> tuple:
        state = get_project_state_for(self.ctx.project_path)
        return (
            state.registry_rev,
            tuple(sorted((iid, str(getattr(jm, "execution_status", ""))) for iid, jm in state.jobs.items())),
            self._wanted,
        )

    async def _recompute(self, key: tuple) -> None:
        async with self._flight("compute") as acquired:
            if not acquired:
                return
            state = get_project_state_for(self.ctx.project_path)
            try:
                computed = await asyncio.to_thread(
                    _compute, state, self.ctx.project_path, self.ctx.species_id, self._wanted
                )
            except Exception as e:
                logger.exception("Analytics compute failed for %s", self.ctx.species_id)
                computed = _Computed(errors=(f"{type(e).__name__}: {e}",))
            self.computed = computed
            self._last_key = key
            self._last_at = time.monotonic()
            if self._view is not None:
                self._view.refresh()

    # ── Choices (per page session; nothing here is stored as truth) ──

    def choose_source(self, key: str | None) -> None:
        if key and key != self._wanted:
            self._wanted = key
            self.refresh()

    def cut_for(self, c: _Computed) -> float:
        """The cut shown for a source: the one set this session, else the lowest extraction cutoff —
        a cut that keeps every candidate."""
        key = c.source.key if c.source else ""
        if key in self._cuts:
            return self._cuts[key]
        cands = [r["cand"] for r in c.rows if r.get("cand")]
        cutoffs = [float(x["cutoff_sigma"]) for x in cands if x.get("cutoff_sigma") is not None]
        return math.floor(min(cutoffs) * 100) / 100 if cutoffs else 0.0

    def set_cut(self, c: _Computed, value: float) -> None:
        if c.source is not None:
            self._cuts[c.source.key] = value

    # ── Sets ──

    async def make_subset(self, c: _Computed) -> None:
        async with self._flight("subset") as acquired:
            if not acquired:
                return
            preferred = None
            if c.source is not None and c.source.kind != "jobs":
                preferred = c.source.key
            elif c.subtomo is not None:
                keys = [f"job:{c.subtomo.instance_id}:particles_filtered", f"job:{c.subtomo.instance_id}:particles"]
                preferred = next((k for k in keys if any(p.key == k for p in c.parents)), None)
            if preferred is None and c.source is not None and c.source.ce_instance:
                preferred = f"job:{c.source.ce_instance}:candidates"
            defaults = SubsetDefaults(
                parent_key=preferred, cut=self.cut_for(c), diameter_ang=c.diameter, tomos=[r["tomo"] for r in c.rows]
            )
            created = await open_subset_dialog(
                self.backend, self.ctx.project_path, self.ctx.species_id, c.parents, defaults
            )
            if created is not None:
                await self.refresh_now()

    async def delete_set(self, s: ParticleSet) -> None:
        async with self._flight("delete") as acquired:
            if not acquired:
                return
            readers = jobs_reading_set(get_project_state_for(self.ctx.project_path), s.id)
            children = [
                x.name
                for x in (self.computed.sets if self.computed else [])
                if any(p.kind == "set" and p.set_id == s.id for p in x.recipe.parents)
            ]
            if not await _confirm_delete(s, readers, children):
                return
            res = await self.backend.delete_particle_set(
                self.ctx.project_path, s.id, reason="deleted in the Analytics tab"
            )
            if not res["success"]:
                ui.notify(res["error"], type="negative")
                return
            ui.notify(f"Set {s.name} deleted; its recipe stays in the operation log.", type="info")
            await self.refresh_now()


async def _confirm_delete(s: ParticleSet, readers: list[str], children: list[str]) -> bool:
    lines = [
        f"{s.n_particles:,} particles in {s.n_tomograms} tomograms. Its files are removed; the operation log keeps "
        "its recipe."
    ]
    if readers:
        lines.append(f"Jobs whose input names it: {', '.join(readers)} — their input will no longer resolve.")
    if children:
        lines.append(f"Sets made from it keep their own files: {', '.join(children)}.")
    with dialog_host(), ui.dialog() as dlg, ui.card().classes("w-96"):
        ui.label(f"Delete set {s.name}?").style(f"{SANS} font-size: 13px; font-weight: 600; color: #0f172a;")
        for line in lines:
            ui.label(line).style(f"{SANS} font-size: 12px; color: #475569;")
        with ui.row().classes("w-full justify-end mt-3 gap-2"):
            house_button("Cancel", lambda: dlg.submit(False))
            house_button("Delete set", lambda: dlg.submit(True), kind="danger")
    go = await dlg
    dlg.delete()
    return bool(go)

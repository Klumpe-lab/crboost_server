"""Picks tab (roadmap 11-S2) — every pick list of this species, grouped by tomogram.

The "manage & act" half of the split decided 2026-08-16: the Journey shows ONE tomogram
and is where picks are *looked at* and curated (canvas, galleries, keep/drop); this table
shows every tomogram at once and is where lists are *acted on* — extract / re-extract,
dedup, merge, delete, choose the authoritative list, import from a path, extract
everything pending.

Rows come from `species_overview` (09-S3). It and the ref resolution it feeds
(`species_anchors` + `tomograms_star_for` per tomogram + `known_tomograms` for the import
picker's universe) all read star files, so they run in one off-loop `_compute` on the
Overview tab's cadence: when the in-memory key (registry rev + job statuses) moves, on
show, and at most every `_DISK_REFRESH_S` while shown. The view itself is a
`FingerprintedView` over that computed value.

Every action is `ui/particles/list_actions` — carved out of the Journey in 11-S1 and, since
11-S3, driven from HERE alone (the Journey keeps only ⚡, which shares
`load_tomo_into_session` / `curate_in_artiax`). `on_done` is a no-op here: the actions bump
the registry rev, and the rev is what repaints this view.

Two columns tell the extraction story, and the split is deliberate: `ext` is DERIVED
(`PickList.extraction_state()` — is there a RECORDED extraction output that still exists and
is current with the picks?), while `job` reports the per-list extraction JOB behind it
(roadmap 07-S4) — queued / running / succeeded / failed, with the failure text and the
geometry it cut with on hover and its logs on click. Neither is a disk sweep: an extraction
whose awaiter died is recorded by nobody, so both columns go stale together (see
`backend._await_extraction_outdirs`) and re-extracting is the recovery — which is why nothing
here hard-blocks that action. This table is the only place either column is visible.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nicegui import ui

from services.aggregation.authoritative import extraction_params_for_species
from services.dashboard_data import glyph_for
from services.models_base import JobStatus, ListExtractionState, PickListType
from services.particles.list_admin import delete_pick_list, pick_list_files
from services.particles.list_ref import AUTO_SLUG, ListRef, auto_ref, list_ref_for, species_tomo_map
from services.particles.species_overview import (
    NOT_APPLICABLE,
    ExtractJob,
    ListRow,
    SpeciesOverview,
    extract_job_for,
    species_overview,
)
from services.project_state import get_project_state_for
from ui.background_task import BackgroundTask
from ui.components.chip import render_chip
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.particles import list_actions
from ui.species.tab import TabContext

logger = logging.getLogger(__name__)

_DISK_REFRESH_S = 15.0  # rows/refs recompute cadence while shown (star reads)
_LABEL_CLS = "text-[10px] font-bold text-gray-500 uppercase tracking-wider"
_HINT_CLS = "text-[10px] text-gray-400"
_LINK_CLS = "text-[10px] text-indigo-500 cursor-pointer underline decoration-dotted"

_GATE_STATUS = {"READY": "ok", "PENDING": "warn", "BLOCKED": "error"}

# The `job` column's mark per extraction-job status: (glyph, css class, words for the hover).
# RUNNING carries no glyph — a spinner replaces it, the one honest way to say "right now".
_JOB_CHIP = {
    JobStatus.SCHEDULED: ("◌", "cb-badge-stale", "submitting"),
    JobStatus.QUEUED: ("◌", "cb-badge-queued", "queued in SLURM"),
    JobStatus.RUNNING: ("", "", "running"),
    JobStatus.SUCCEEDED: ("✓", "cb-badge-todo", "last run succeeded"),
    JobStatus.FAILED: ("✕", "cb-badge-err", "last run FAILED"),
    JobStatus.UNKNOWN: ("?", "cb-badge-todo", "status unknown"),
}


def _job_tooltip(job: ExtractJob, status: JobStatus) -> str:
    """One actionable line for the `job` mark: what the extraction is doing, the SLURM id to
    check, the geometry it cut with (nothing else in the project records that — before roadmap
    07 it lived only in the launch command line) and the failure text when there is one.
    Quasar tooltips collapse newlines, so it stays a single line."""
    bits = [f"extraction {_JOB_CHIP.get(status, ('', '', status.value))[2]}"]
    if job.slurm_job_id:
        bits.append(f"SLURM {job.slurm_job_id}")
    bits.append(list_actions.extraction_geometry_text(job))
    if job.error:
        bits.append(job.error if len(job.error) <= 240 else job.error[:240] + " …")
    bits.append("click for logs" if job.job_dir else "no log dir recorded for this submit")
    return " — ".join(bits)


def _count_text(count: int, kept: int | None) -> str:
    """'kept/total' when a keep/drop filter is committed for this list, else the total
    (the Journey's `_list_count_text` rule — equal-to-total is no effective filter)."""
    return f"{kept}/{count}" if kept is not None and kept != count else str(count)


def _dedup_default_radius(state, species_id: str) -> float:
    """Default overlap radius (Å) ≈ Ø/2 from the species (the Journey takes it from the
    candidate-extract job, which a de-novo species does not have); 100 Å when Ø is unset.
    Not a silent default — the dedup dialog shows the live clash count at the radius and
    removes nothing until the user commits."""
    sp = state.get_species(species_id)
    d = float(getattr(sp, "diameter_ang", 0.0) or 0.0)
    return round(d / 2.0, 1) if d > 0 else 100.0


# ── Off-loop compute ──────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class _Computed:
    """Everything the render reads, in comparable (signature-able) form: tuples, not
    dicts, so `==` decides whether the view repaints."""

    overview: SpeciesOverview | None
    refs: tuple[tuple[str, str, ListRef], ...]  # (tomo, slug, ref) — one per overview row
    tomo_refs: tuple[tuple[str, ListRef], ...]  # per-tomo reference slot: ⚡ / import target
    species_color: str
    error: str | None


def _compute(state, project_path: Path, species_id: str) -> _Computed:
    """Thread body: `species_overview`, the per-tomogram `tomograms.star` resolution and
    the import picker's tomogram universe all read star files."""
    ov = species_overview(state, project_path, species_id)
    sp = state.get_species(species_id)
    color = str(getattr(sp, "color", "") or "#3b82f6")
    # Universe = tomograms with rows ∪ every tomogram the project describes: a .coords can
    # be imported into a tomogram that has no picks yet.
    anchors, stars = species_tomo_map(state, project_path, species_id, tuple(r.tomo_name for r in ov.rows))

    refs = tuple(
        (
            r.tomo_name,
            r.slug,
            list_ref_for(
                project_path,
                anchors,
                species_id,
                r.tomo_name,
                r.slug,
                label=r.label,
                list_type=r.list_type,
                star_path=r.star_path,
                tomograms_star=stars.get(r.tomo_name),
            ),
        )
        for r in ov.rows
    )
    tomo_refs = tuple((t, auto_ref(project_path, anchors, species_id, t, star)) for t, star in stars.items())
    return _Computed(ov, refs, tomo_refs, color, None)


# ── View ──────────────────────────────────────────────────────────────────────


class _PicksView(FingerprintedView):
    def __init__(self, container: ui.element, tab: PicksTab) -> None:
        super().__init__(container)
        self._tab = tab

    def signature(self) -> Any:
        c = self._tab.computed
        return (
            self._tab.ctx.species_id,
            get_project_state_for(self._tab.ctx.project_path).registry_rev,
            None if c is None else (c.overview, c.refs, c.tomo_refs, c.species_color, c.error),
            tuple(sorted((t, tuple(sorted(s))) for t, s in self._tab.ticks.items() if s)),
        )

    # ── Chrome ────────────────────────────────────────────────────────────────

    def render(self) -> None:
        c = self._tab.computed
        with ui.row().classes("w-full items-baseline gap-2 px-1"):
            ui.label("PICKS").classes(_LABEL_CLS)
            ui.label("every list on every tomogram — act here, look in the Journey").classes(_HINT_CLS)
        if c is None:
            ui.label("computing…").classes(_HINT_CLS + " px-1")
            return
        if c.error:
            ui.label(f"Picks unavailable — {c.error}").classes("text-[11px] text-red-600 px-1").tooltip(
                "species_overview / ref resolution raised; full traceback in the server log"
            )
            return
        self._render_header(c)
        if not c.overview.rows:
            self._render_empty()
            return
        by_tomo: dict[str, list[ListRow]] = {}
        for row in c.overview.rows:
            by_tomo.setdefault(row.tomo_name, []).append(row)
        for tomo, rows in by_tomo.items():
            self._render_group(c, tomo, rows)

    def _render_header(self, c: _Computed) -> None:
        ov = c.overview
        with ui.row().classes("w-full items-center gap-2 px-1 flex-wrap"):
            with ui.element("div").classes("cb-chip-strip"):
                render_chip("lists", str(len(ov.rows)), tooltip="pick lists over every tomogram of this species")
                render_chip("picks", str(ov.n_picks), tooltip="sum of every list's count")
                render_chip("kept", str(ov.n_kept), tooltip="after committed keep/drop curation")
                render_chip(
                    "gate",
                    ov.gate,
                    status=_GATE_STATUS.get(ov.gate, "neutral"),
                    tooltip="Authoritative-list roll-up: READY = nothing left to extract, PENDING = authoritative "
                    "lists still to extract, BLOCKED = a choice cannot be extracted by extraction alone.",
                )
            ui.space()
            ui.button("Extract all pending", icon="science", on_click=self._tab.extract_all_pending).props(
                "flat dense no-caps size=sm color=indigo"
            ).tooltip(
                "Subtomo-extract every authoritative list that is not extracted / stale — shows what it would "
                "submit before anything is sent"
            )
            ui.button("Import picks from path…", icon="download", on_click=self._tab.import_from_path).props(
                "flat dense no-caps size=sm color=indigo"
            ).tooltip("Register a .coords saved from ArtiaX (or an external one) as this species' manual list")
            ui.button("Open in Journey", icon="open_in_new", on_click=lambda: self._tab.open_in_journey(None)).props(
                "flat dense no-caps size=sm"
            ).tooltip("Show this species on the tomogram journey (look & curate)")

    def _render_empty(self) -> None:
        with ui.column().classes("w-full items-center gap-1 py-6"):
            ui.icon("scatter_plot", size="32px").classes("text-gray-300")
            ui.label("No picks yet for this species.").classes("text-xs text-gray-400")
            with ui.row().classes("items-center gap-1"):
                ui.label("Load a tomogram into ArtiaX from the").classes(_HINT_CLS)
                ui.label("Curation tab").classes(_LINK_CLS).on("click", lambda: self._tab.select_tab("curation"))
                ui.label("or run Pick candidates from the").classes(_HINT_CLS)
                ui.label("Jobs tab").classes(_LINK_CLS).on("click", lambda: self._tab.select_tab("jobs"))

    # ── One tomogram ──────────────────────────────────────────────────────────

    def _render_group(self, c: _Computed, tomo: str, rows: list[ListRow]) -> None:
        tomo_ref = self._tab.tomo_ref(tomo)
        with ui.element("div").classes("cb-ptable-group"):
            ui.label(tomo).classes("cb-ptable-group-name")
            ui.label(f"{len(rows)} list{'s' if len(rows) != 1 else ''}").classes(_HINT_CLS)
            if not any(r.is_authoritative for r in rows):
                # PER TOMOGRAM, not per species: `get_authoritative_slug` falls back to 'auto',
                # and whether that resolves is a property of THIS group — a species with a
                # candidate-extract job still has no auto row on a tomogram where the CE
                # returned no picks, which is the same dead end as a de-novo species. A choice
                # left dangling by a deleted list lands here too, which is right: in every one
                # of those cases every row reads unchecked, the gate reads BLOCKED, and without
                # this nothing says a decision is outstanding. Deliberately NOT pre-checked on
                # the user's behalf, even with a single candidate list: the gate reads the
                # STORED choice, so a checked-looking radio that persisted nothing would show
                # "chosen" next to a BLOCKED roll-up.
                ui.label("no authoritative list").classes("text-[10px] text-orange-700 font-medium").tooltip(
                    "Nothing downstream consumes this tomogram until one list is marked authoritative — click a "
                    "row's radio in the 'auth' column. The default is the auto candidate set, which does not "
                    "exist here (no candidate-extract job, or it found nothing on this tomogram); a choice whose "
                    "list was deleted reads the same way."
                )
            ui.space()
            if tomo_ref is not None:
                ui.button(
                    icon="bolt",
                    on_click=lambda _e, r=tomo_ref: list_actions.load_tomo_into_session(self._tab.backend, r),
                ).props("flat dense round size=sm color=amber-8").tooltip(
                    "Load this tomogram + its picks into the running ArtiaX session"
                )
            ui.label("journey ↗").classes(_LINK_CLS).on("click", lambda _e, t=tomo: self._tab.open_in_journey(t))

        auth_icons: dict[str, Any] = {}
        with ui.element("div").classes("cb-ltable"):
            with ui.element("div").classes("cb-ltable-row cb-ptable-row cb-ltable-head"):
                ui.element("div")  # tick
                ui.label("auth").classes("cb-ltable-h-cell").tooltip("Authoritative downstream list (one per tomogram)")
                ui.element("div")  # swatch
                ui.label("list").classes("cb-ltable-h-name")
                ui.label("picks").classes("cb-ltable-h-num")
                ui.label("ext").classes("cb-ltable-h-cell").tooltip("Subtomo-extracted state")
                ui.label("job").classes("cb-ltable-h-cell").tooltip(
                    "The per-list extraction JOB behind that state — queued / running / succeeded / failed. "
                    "Hover a mark for the geometry it cut with and any failure text; click it for the logs."
                )
                ui.label("source").classes("cb-ltable-h-name")
                ui.element("div")  # actions
            for row in rows:
                self._render_row(c, tomo, row, auth_icons)
        self._render_merge_bar(tomo, rows)

    def _render_row(self, c: _Computed, tomo: str, row: ListRow, auth_icons: dict) -> None:
        ref = self._tab.ref(tomo, row.slug)
        with ui.element("div").classes("cb-ltable-row cb-ptable-row"):
            # Tick — merge selection only (rows have no detail pane to select into).
            tick = ui.element("div").classes("cb-ptable-tick")
            if row.slug in self._tab.ticks.get(tomo, set()):
                tick.classes(add="on")
            tick.tooltip("Tick to include this list in a merge")
            tick.on("click", lambda _e, s=row.slug: self._tab.toggle_tick(tomo, s))

            with ui.element("div").classes("cb-ltable-cell"):
                on = row.is_authoritative
                icon = ui.icon("radio_button_checked" if on else "radio_button_unchecked", size="15px").classes(
                    "cb-auth " + ("cb-auth-on" if on else "cb-auth-off")
                )
                icon.tooltip("Authoritative list — downstream extraction/aggregation consumes this one. Click to set.")
                icon.on("click", lambda _e, s=row.slug: self._tab.set_authoritative(tomo, s, auth_icons))
                auth_icons[row.slug] = icon

            ui.element("div").classes(f"cb-ltable-swatch cb-swatch-{glyph_for(PickListType(row.list_type))}").style(
                f"background: {c.species_color};"
            ).tooltip(f"{row.list_type} list")
            ui.label(row.label).classes("cb-ltable-name").tooltip(row.star_path or "no backing star on disk")
            ui.label(_count_text(row.count, row.kept)).classes("cb-ltable-count").tooltip(
                "kept / total picks after keep-drop curation"
            )
            with ui.element("div").classes("cb-ltable-cell"):
                self._render_ext_badge(row)
            self._render_job_chip(row)
            if row.source_kind:
                ui.label(row.source_kind).classes("cb-ptable-source").tooltip(
                    f"{row.source_kind} · {row.source_ref or '—'}"
                )
            else:
                ui.label("—").classes("cb-ptable-source").tooltip(
                    "no provenance recorded — this list pre-dates source tracking (09-S2)"
                )
            with ui.row().classes("items-center gap-0 flex-nowrap"):
                self._render_actions(tomo, row, ref)

    def _render_ext_badge(self, row: ListRow) -> None:
        if row.extraction_state == NOT_APPLICABLE:
            ui.label("·").classes("cb-ltable-badge text-gray-300").tooltip(
                "auto list of a species with no subtomo-extraction job — nothing to extract here"
            )
            return
        text, cls = list_actions.extraction_badge(ListExtractionState(row.extraction_state))
        if text:
            ui.label(text.split(" ", 1)[0]).classes(f"cb-ltable-badge {cls}").tooltip(text)

    def _render_job_chip(self, row: ListRow) -> None:
        """The `job` column: what the per-list extraction INSTANCE is doing (roadmap 07-S4).

        ALWAYS emits exactly one grid child — the cell stays empty for a list with no
        instance (the `auto` list never gets one, and a list that was never submitted has
        nothing to add to `ext`), because a skipped child would slide every later column of
        that row one place left. Clicking opens the job's logs: this job deliberately has no
        roster row, so the mark is the only door to them."""
        cell = ui.element("div").classes("cb-ltable-cell")
        job = row.extract_job
        if job is None:
            return
        status = JobStatus(job.status)
        glyph, cls, _ = _JOB_CHIP.get(status, ("?", "cb-badge-todo", status.value))
        with cell:
            if status == JobStatus.RUNNING:
                ui.spinner("dots", size="xs").classes("text-blue-500")
            else:
                ui.label(glyph).classes(f"cb-ltable-badge {cls}")
        cell.classes(add="cb-ptable-job").tooltip(_job_tooltip(job, status))
        cell.on("click", lambda _e, r=row: self._tab.open_logs(r))

    def _render_actions(self, tomo: str, row: ListRow, ref: ListRef | None) -> None:
        if ref is None or row.slug == AUTO_SLUG:
            return  # the auto list follows its subtomo job; it is not extracted per list
        extracted = row.extraction_state == ListExtractionState.EXTRACTED.value
        ui.button(
            icon="science", on_click=lambda _e, r=ref: list_actions.extract_list(self._tab.backend, r, on_done=_noop)
        ).props("flat dense round size=sm color=indigo").tooltip(
            "Re-extract this list's subtomograms" if extracted else "Extract this list's subtomograms"
        )
        if row.list_type == PickListType.MERGED.value:
            ui.button(icon="join_inner", on_click=lambda _e, r=ref: self._tab.open_dedup(r)).props(
                "flat dense round size=sm color=orange-7"
            ).tooltip("Overlapping picks — inspect and deduplicate")
        ui.button(icon="delete_outline", on_click=lambda _e, r=ref: self._tab.delete_list(r)).props(
            "flat dense round size=sm color=negative"
        ).tooltip("Delete this list (its star, curated subset, extraction output and saves)")

    def _render_merge_bar(self, tomo: str, rows: list[ListRow]) -> None:
        """Shown only when ≥ 2 lists of THIS tomogram are ticked (the Journey's rule): a
        merge is a union within one tomogram's coordinate frame."""
        ticked = self._tab.ticks.get(tomo, set())
        if len(ticked) < 2:
            return
        names = [r.label for r in rows if r.slug in ticked]
        with ui.element("div").classes("cb-merge-bar"):
            with ui.row().classes("w-full items-center gap-2"):
                ui.label(f"Merge {len(ticked)}:").classes("text-[11px] font-semibold text-indigo-900")
                ui.label(" + ".join(names)).classes("text-[10px] text-indigo-700 truncate")
                # The name survives a re-render: ticking one more list rebuilds this bar
                # (the tick is part of the view signature), and a half-typed name must
                # not vanish under the user.
                name_in = (
                    ui.input(placeholder="merged list name", value=self._tab.merge_names.get(tomo, ""))
                    .props("dense outlined")
                    .classes("w-40 text-xs")
                    .tooltip("Re-using a name replaces that merge; a new name makes a distinct list")
                )
                name_in.on_value_change(lambda e, t=tomo: self._tab.set_merge_name(t, e.value or ""))
                ui.button(
                    "Merge", icon="merge", on_click=lambda _e, t=tomo: self._tab.merge(t, name_in.value or "")
                ).props("dense no-caps size=sm color=indigo")
                ui.button("Clear", on_click=lambda _e, t=tomo: self._tab.clear_ticks(t)).props(
                    "flat dense no-caps size=sm"
                )


def _noop() -> None:
    """`on_done` for the shared actions: this page repaints off the registry rev they
    bump, so there is nothing to call back into."""
    return None


# ── Tab ───────────────────────────────────────────────────────────────────────


class PicksTab:
    def __init__(self, ctx: TabContext) -> None:
        self.ctx = ctx
        self.computed: _Computed | None = None
        self.ticks: dict[str, set[str]] = {}  # tomo → ticked slugs (merge selection)
        self.merge_names: dict[str, str] = {}  # tomo → the merge name being typed
        self._last_key: Any = None
        self._last_at: float = 0.0
        self._flight = SingleFlight()
        self._view: _PicksView | None = None

    @property
    def backend(self):
        return self.ctx.backend

    # ── Tab protocol ──────────────────────────────────────────────────────────

    def build(self, container: ui.element) -> None:
        with container:
            slot = ui.column().classes("w-full gap-1 p-2")
        self._view = _PicksView(slot, self)
        # Paint "computing…" only; the first compute is kicked by the page's
        # `_refresh_visible_tab` right after this (build runs during page construction,
        # where scheduling a task is not this object's business).
        self._view.refresh()

    def refresh(self) -> None:
        key = self._memory_key()
        stale = time.monotonic() - self._last_at >= _DISK_REFRESH_S
        if self.computed is None or key != self._last_key or stale:
            asyncio.create_task(self._recompute(key))
        elif self._view is not None:
            self._view.refresh()

    def _memory_key(self) -> tuple:
        state = get_project_state_for(self.ctx.project_path)
        return (
            state.registry_rev,
            tuple(sorted((iid, str(getattr(jm, "execution_status", ""))) for iid, jm in state.jobs.items())),
        )

    async def _recompute(self, key: tuple) -> None:
        async with self._flight("compute") as acquired:
            if not acquired:
                return
            ctx = self.ctx
            if self.backend is not None:
                # Settle any extraction instance whose awaiter is gone BEFORE reading the rows
                # off it (roadmap 07 review, finding A): otherwise the `job` chip asserts
                # Queued/Running for a job that left the SLURM queue long ago, and a finished
                # one stays unrecorded. No-op — and no squeue call — when nothing is
                # non-terminal, which is the normal case; runs at most per `_DISK_REFRESH_S`.
                await self.backend.reconcile_pick_list_extractions(ctx.project_path, ctx.species_id)
            state = get_project_state_for(ctx.project_path)
            try:
                computed = await asyncio.to_thread(_compute, state, ctx.project_path, ctx.species_id)
            except Exception as e:
                # Reported, not swallowed: traceback to the log, the cause into the tab.
                logger.exception("Picks compute failed for %s", ctx.species_id)
                computed = _Computed(None, (), (), "#3b82f6", f"{type(e).__name__}: {e}")
            self.computed = computed
            self._last_key = key
            self._last_at = time.monotonic()
            if self._view is not None:
                self._view.refresh()

    # ── Ref lookup ────────────────────────────────────────────────────────────

    def ref(self, tomo: str, slug: str) -> ListRef | None:
        c = self.computed
        if c is None:
            return None
        return next((r for t, s, r in c.refs if t == tomo and s == slug), None)

    def tomo_ref(self, tomo: str) -> ListRef | None:
        c = self.computed
        if c is None:
            return None
        return next((r for t, r in c.tomo_refs if t == tomo), None)

    # ── Selection ─────────────────────────────────────────────────────────────

    def toggle_tick(self, tomo: str, slug: str) -> None:
        picked = self.ticks.setdefault(tomo, set())
        if slug in picked:
            picked.discard(slug)
        else:
            picked.add(slug)
        if self._view is not None:
            self._view.refresh()  # the merge bar appears/disappears at 2 ticks

    def clear_ticks(self, tomo: str) -> None:
        self.ticks.pop(tomo, None)
        self.merge_names.pop(tomo, None)
        if self._view is not None:
            self._view.refresh()

    def set_merge_name(self, tomo: str, name: str) -> None:
        self.merge_names[tomo] = name

    def select_tab(self, key: str) -> None:
        select = self.ctx.callbacks.get("species_select_tab")
        if select is not None:
            select(key)

    async def open_in_journey(self, tomo: str | None) -> None:
        """Show the journey (building it on first use) and, when a tomogram is named,
        select its column + scroll the particles section into view."""
        async with self._flight("journey") as acquired:
            if not acquired:
                return
            toggle = self.ctx.callbacks.get("toggle_journey")
            if toggle is None:
                ui.notify("Journey not available in this view", type="warning")
                return
            await toggle()
            show = self.ctx.callbacks.get("journey_show_ts")
            if tomo and show is not None:
                await show(tomo, section="particles")

    async def set_authoritative(self, tomo: str, slug: str, icons: dict) -> None:
        state = get_project_state_for(self.ctx.project_path)
        if state.get_authoritative_slug(self.ctx.species_id, tomo) == slug:
            return
        state.set_authoritative_slug(self.ctx.species_id, tomo, slug)
        await self.backend.save_project(self.ctx.project_path, force=True)
        for s, icon in icons.items():
            on = s == slug
            icon.name = "radio_button_checked" if on else "radio_button_unchecked"
            icon.classes(add="cb-auth-on" if on else "cb-auth-off", remove="cb-auth-off" if on else "cb-auth-on")
        ui.notify(f"Authoritative → {slug} on {tomo} — downstream extraction/aggregation will use it", type="positive")

    # ── Actions ───────────────────────────────────────────────────────────────

    async def merge(self, tomo: str, name: str) -> None:
        async with self._flight(f"merge:{tomo}") as acquired:
            if not acquired:
                return
            refs = [r for r in (self.ref(tomo, s) for s in sorted(self.ticks.get(tomo, set()))) if r is not None]
            if len(refs) < 2:
                ui.notify("Tick at least 2 lists of one tomogram to merge.", type="warning")
                return
            if await list_actions.merge_lists(self.backend, refs, name):
                self.clear_ticks(tomo)
                self.refresh()

    def open_dedup(self, ref: ListRef) -> None:
        state = get_project_state_for(self.ctx.project_path)
        list_actions.open_dedup_dialog(
            self.backend,
            ref,
            default_radius_ang=_dedup_default_radius(state, self.ctx.species_id),
            on_done=self.refresh,
        )

    async def open_logs(self, row: ListRow) -> None:
        """This list's extraction job: its logs, the geometry it cut with, its failure text
        (roadmap 07-S4). SingleFlight-guarded like every dialog opener here — the mark lives
        in a rev-gated container a refresh can replace mid-click, so a user can legitimately
        land several clicks before one opens."""
        job = row.extract_job
        if job is None:
            return
        async with self._flight(f"logs:{job.instance_id}") as acquired:
            if not acquired:
                return
            await list_actions.open_extraction_logs(
                self.backend, self.ctx.project_path, job, title=f"{row.label} · {row.tomo_name}"
            )

    def import_from_path(self) -> None:
        """Species-level import: the .coords maps into ONE tomogram's frame, and that
        tomogram may have no picks yet — so the picker gets every tomogram the project
        describes, not just the ones with rows."""
        c = self.computed
        if c is None or not c.tomo_refs:
            ui.notify("No tomogram with a resolvable tomograms.star yet — nothing to import into.", type="warning")
            return
        options = dict(c.tomo_refs)
        first = next(iter(options.values()))
        list_actions.import_picks_from_path(
            self.backend,
            first,
            on_done=self.refresh,
            tomo_options=options,
            intro="Paste the full path to a .coords saved from ArtiaX (any filename) and pick the tomogram it "
            "belongs to. It is registered as this species' manual list for that tomogram.",
        )

    async def delete_list(self, ref: ListRef) -> None:
        """Confirm (listing exactly what goes) → `list_admin.delete_pick_list`. The
        authoritative choice is left dangling on purpose — the gate surfaces it rather
        than silently falling back to `auto`."""
        async with self._flight(f"delete:{ref.tomo_name}:{ref.slug}") as acquired:
            if not acquired:
                return
            state = get_project_state_for(ref.project_path)
            pl = state.get_pick_list(ref.slug, ref.species_id, ref.tomo_name)
            if pl is None:
                ui.notify(f"'{ref.label}' is not a registered list — nothing to delete.", type="warning")
                return
            files = await asyncio.to_thread(pick_list_files, pl)
            job = extract_job_for(state, ref.species_id, ref.tomo_name, ref.slug)
            live_job = job is not None and job.is_live
            with list_actions.dialog_host(), ui.dialog() as confirm, ui.card().classes("w-[30rem] max-w-full gap-2"):
                ui.label(f"Delete '{ref.label}' on {ref.tomo_name}?").classes("text-sm font-bold")
                for kind, label in (("stars", "star file"), ("dirs", "extraction output"), ("coords", "ArtiaX save")):
                    for path in files[kind]:
                        ui.label(f"• {label}: {path}").classes("text-[10px] font-mono text-gray-600")
                if not any(files.values()):
                    ui.label("• nothing on disk — only the registry entry").classes("text-[11px] text-gray-500")
                if files["coords"]:
                    ui.label(
                        "The .coords saves go too — otherwise the curation watcher re-registers this list on the "
                        "next save scan. Archived copies under imports/ are kept."
                    ).classes(_HINT_CLS)
                if pl.slug == state.get_authoritative_slug(ref.species_id, ref.tomo_name):
                    ui.label(
                        "This is the authoritative list for the tomogram — the choice is left dangling (the gate "
                        "will flag it) rather than silently falling back."
                    ).classes("text-[10px] text-orange-700")
                if live_job:
                    ui.label(
                        f"An extraction for this list is {job.status.lower()} (SLURM {job.slurm_job_id or '—'}) — "
                        "it is cancelled first. Left running it would re-create the output directory this delete "
                        "removes, and its instance goes with the list, so nothing would be left to stop it with."
                    ).classes("text-[10px] text-orange-700")
                ui.label("This cannot be undone.").classes(_HINT_CLS + " text-red-600")
                with ui.row().classes("w-full justify-end gap-2"):
                    ui.button("Cancel", on_click=lambda: confirm.submit(None)).props("flat dense no-caps")
                    ui.button("Delete list", on_click=lambda: confirm.submit(True)).props(
                        "unelevated dense no-caps color=negative"
                    )
            go = await confirm
            confirm.delete()
            if not go:
                return
            if live_job and self.backend is None:
                ui.notify(
                    "Backend unavailable — the in-flight extraction was NOT cancelled and may re-create the "
                    "directory this delete removes.",
                    type="warning",
                    timeout=6000,
                )
            elif live_job:
                cancelled = await self.backend.cancel_pick_list_extraction(
                    ref.project_path, ref.species_id, ref.tomo_name, ref.slug
                )
                if not cancelled.get("success"):
                    ui.notify(cancelled["error"], type="warning", timeout=6000)
            result = await delete_pick_list(ref.project_path, ref.species_id, ref.tomo_name, ref.slug)
            if not result.get("success"):
                ui.notify(result["error"], type="negative")
                return
            for problem in result.get("errors") or []:
                ui.notify(problem, type="warning", timeout=5000)
            ui.notify(f"Deleted '{ref.label}' ({result.get('deleted_files', 0)} file(s))", type="positive")
            self.ticks.get(ref.tomo_name, set()).discard(ref.slug)
            self.refresh()

    # ── Extract all pending ───────────────────────────────────────────────────

    async def extract_all_pending(self) -> None:
        """Pre-flight FIRST (the §8.3 gate report is the preview), then submit. Never a
        blind fan-out: `extract_authoritative_pending` starts one SLURM job per pending
        list, so the user sees the count, the tomograms and the blocked reasons before
        confirming — and commits the extraction geometry here if the species has none."""
        async with self._flight("extract_all") as acquired:
            if not acquired:
                return
            ctx = self.ctx
            report = await self.backend.get_authoritative_gate_report(ctx.project_path, ctx.species_id)
            pending, blocked = report.get("pending") or [], report.get("blocked") or []
            if not pending:
                msg = "Nothing pending — every authoritative list is extracted and current."
                if blocked:
                    msg = f"Nothing extractable: {len(blocked)} authoritative list(s) blocked (see the table)."
                ui.notify(msg, type="warning" if blocked else "info", timeout=5000)
                return

            state = get_project_state_for(ctx.project_path)
            need_geometry = extraction_params_for_species(state, ctx.species_id) is None
            with list_actions.dialog_host(), ui.dialog() as confirm, ui.card().classes("w-[32rem] max-w-full gap-2"):
                ui.label(f"Extract {len(pending)} pending list(s)?").classes("text-sm font-bold")
                ui.label("One subtomo-extraction job per list, submitted now and tracked in the task tray.").classes(
                    _HINT_CLS
                )
                for h in pending:
                    ui.label(f"• {h['tomo_name']} · {h['slug']} ({h['extraction_state']})").classes(
                        "text-[10px] font-mono text-gray-600"
                    )
                for h in blocked:
                    reason = "; ".join(h.get("notes") or []) or h.get("kind") or "not extractable here"
                    ui.label(f"⚠ blocked — {h['tomo_name']} · {h['slug']}: {reason}").classes(
                        "text-[10px] text-orange-700"
                    )
                box_in = bin_in = crop_in = None
                if need_geometry:
                    ui.label(
                        "This species has no committed extraction geometry (no subtomo job to inherit it from). "
                        "Set it once — it is saved on the species and reused for every later extraction."
                    ).classes("text-xs text-gray-600")
                    box_in, bin_in, crop_in = list_actions.geometry_inputs()
                with ui.row().classes("w-full justify-end gap-2"):
                    ui.button("Cancel", on_click=lambda: confirm.submit(None)).props("flat dense no-caps")
                    ui.button("Extract", icon="science", on_click=lambda: confirm.submit(True)).props(
                        "dense no-caps color=indigo"
                    )
            go = await confirm
            geometry = (box_in.value, bin_in.value, crop_in.value) if need_geometry and go else None
            confirm.delete()
            if not go:
                return
            if need_geometry and not await list_actions.commit_extraction_geometry(
                self.backend, ctx.project_path, ctx.species_id, *geometry
            ):
                return
            self._submit_extract_all(len(pending))

    def _submit_extract_all(self, n_pending: int) -> None:
        ctx = self.ctx
        backend = self.backend

        async def _run(progress_cb):
            progress_cb(0, n_pending, "extracting pending lists…")
            res = await backend.extract_authoritative_pending(ctx.project_path, ctx.species_id)
            return (
                f"{len(res.get('succeeded') or [])} extracted · {len(res.get('failed') or [])} failed · "
                f"{len(res.get('blocked') or [])} blocked · {len(res.get('still_running') or [])} still running"
            )

        BackgroundTask(
            title=f"Extract pending · {ctx.species_id}",
            subtitle=f"{n_pending} list(s)",
            project_path=str(ctx.project_path),
            dedup_key=f"extract_pending:{ctx.species_id}",
        ).submit(_run, on_complete=lambda _t: self.refresh(), show_start_toast=True)

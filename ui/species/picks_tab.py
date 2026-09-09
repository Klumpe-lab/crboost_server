"""Picks & curation (picking-UI roadmap 09) — the ONE surface for this species' picks.

Merges what were two tabs (Picks, 11-S2 · Curation, 11-S4) into a single one, because they
answered the same two questions with two vocabularies: *which tomogram* and *which species*.
Everything else a pick set has — source, count, filter status, extraction status, the
template+mask it came from — is an auxiliary fact of a row here.

Top to bottom:

- **Title row** — `PICKS & CURATION` (its tooltip carries the WHERE-TO-SAVE contract, derived
  from the watcher's own cadence) and `Extract all pending`. The session chip that sat here
  went with 13-S3: the control center is reachable only through `Curate picks`, and the live
  marker sits on the in-session tomogram's button. The lists / picks / kept / extracted chips
  are gone too: each was a sum of the table right below, so the strip restated the page.
- **One group per tomogram**, over the species' whole tomogram universe rather than only the
  ones that already hold picks: a tomogram with nothing on it is exactly where de-novo
  picking starts, and it is a legitimate `.coords` import target. The name carries the
  copy-save-dir button (it copies that tomogram's folder), and the group actions are the ONE
  place each remaining verb exists — `Curate picks` (the single door to ArtiaX in the app,
  09-S2 / 13-S3, with a green dot when the live session has this tomogram open), the viewer
  icon and the journey icon.
- **The list table** per group: swatch · list · source · origin · picks · Extracted · job ·
  actions, closed by the always-present import row. `origin` (09-S4) names the template +
  mask that produced the picks, resolved from the candidate-extract instance's recorded
  template-match input — never guessed.
- **Watcher footer** — what the server did with this species' saves, plus every dir holding
  a save it could NOT attribute, with the reason. That second list is the never-silent half.

Rows come from `species_overview` (09-S3); refs, the tomogram universe and the per-tomogram
curation-dir globs all read disk, so they run in one off-loop `_compute`: when the in-memory
key (registry rev + job statuses + watcher log) moves, on show, and at most every
`_DISK_REFRESH_S` while shown. The view is a `FingerprintedView` over that value; the session
poll is the shared, throttled `session_status` cache, so this tab adds no `squeue` traffic.

Every action is `ui/particles/list_actions`, so the Journey and this tab drive one
implementation. `on_done` is a no-op for most of them: the actions bump the registry rev, and
the rev is what repaints this view.

Two columns tell the extraction story, and the split is deliberate: `Extracted` is DERIVED
(`PickList.extraction_state()` — is there a RECORDED extraction output that still exists and
is current with the picks?), while `job` reports the per-list extraction JOB behind it
(roadmap 07-S4) — queued / running / succeeded / failed, with the failure text and the
geometry it cut with on hover and its logs on click. Neither is a disk sweep: an extraction
whose awaiter died is recorded by nobody, so both columns go stale together (see
`backend._await_extraction_outdirs`) and re-extracting is the recovery — which is why nothing
here hard-blocks that action. This table is the only place either column is visible.

Aggregation is deliberately NOT here (09-S3): merged lists still render, and their dedup
inspect still opens, but CREATING a merge moves to the Aggregate-candidates flow. The service
and dialog layers (`list_actions.merge_lists` / `open_dedup_dialog`) are untouched.

There is no `auth` column and no roll-up `gate` chip any more. Both served the
authoritative-list model — one list per (species, tomogram) nominated as THE one downstream
consumes — which is gone: what reaches a refinement is now whatever the user selects as a
source in the Aggregate-candidates flow, chosen there, in front of the merge it feeds. So a
list here is simply extracted or not, and `Extract all pending` cuts every list that isn't.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nicegui import ui

from services.aggregation.extraction import extraction_params_for_species
from services.curation.watcher import FULL_SWEEP_EVERY, SETTLE_SEC, TICK_SEC
from services.dashboard_data import glyph_for
from services.models_base import JobStatus, ListExtractionState, PickListType, PickSourceKind
from services.particles.ingest import is_default_slug
from services.particles.list_ref import AUTO_SLUG, ListRef, auto_ref, list_ref_for, species_tomo_map
from services.particles.species_overview import NOT_APPLICABLE, ExtractJob, ListRow, SpeciesOverview, species_overview
from services.project_state import get_project_state_for
from services.visualization import artiax_bridge
from ui.background_task import BackgroundTask
from ui.components.buttons import house_button
from ui.components.chip import render_chip
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.components.svg_icon import load_icon_svg
from ui.local_file_picker import local_file_picker
from ui.particles import list_actions, session_status
from ui.species.tab import TabContext
from ui.styles import MONO

logger = logging.getLogger(__name__)

_DISK_REFRESH_S = 15.0  # rows/refs/curation-dir recompute cadence while shown (star reads + globs)
_LABEL_CLS = "text-[10px] font-bold text-gray-500 uppercase tracking-wider"
_HINT_CLS = "text-[10px] text-gray-400"
_LINK_CLS = "text-[10px] text-indigo-500 cursor-pointer underline decoration-dotted"


# What the save contract promises, derived from the watcher's own cadence: a full sweep of
# Curation/*/*/ every FULL_SWEEP_EVERY ticks, plus the settle window that keeps a half-written
# file until the next tick. A tomogram LOADED in the live session is in the watcher's hot set
# and is rescanned every tick instead.
_DETECT_S = FULL_SWEEP_EVERY * TICK_SEC + SETTLE_SEC
_HOT_DETECT_S = TICK_SEC + SETTLE_SEC

_EVENT_CLS = {
    "ingested": "cb-badge-ok",
    "error": "text-red-600",
    "no-geometry": "cb-badge-stale",
    "unattributed": "cb-badge-stale",
    "off-scope": "text-red-600",  # a save landed in a folder the live session was NOT scoped to
}

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


def _coords_path(row: ListRow) -> str:
    """The ``.coords`` file behind a hand-picked list — a session save sits beside its star
    under the list's recorded stem, an external import IS its recorded path. "" for auto /
    merged lists and for rows that pre-date provenance (nothing is guessed)."""
    if row.list_type != PickListType.MANUAL.value or not row.source_ref:
        return ""
    if row.source_kind == PickSourceKind.ARTIAX.value and row.star_path:
        return str(Path(row.star_path).parent / f"{row.source_ref}.coords")
    if row.source_kind == PickSourceKind.IMPORT.value and "/" in row.source_ref:
        return row.source_ref
    return ""


def _save_contract(species_slug: str) -> str:
    """The WHERE-TO-SAVE contract, as the title's tooltip (09-S1, reworded by 13-S1). It
    used to be a prose block of its own; it is the one thing a user must get right by
    hand, but it is also read once, so it hangs off the title rather than the page."""
    return (
        f"'Curate picks' pre-seeds ONE empty list per tomogram — Curation/{species_slug}/<tomogram>/"
        f"{species_slug}__<tomogram>__picks.coords — and opens it last in ArtiaX. Pick into it and save it back to "
        f"that same file (positions-only .coords, not RELION star) · picked up within ~{_HOT_DETECT_S:.0f} s in the "
        f"session's tomogram, ~{_DETECT_S:.0f} s elsewhere — nothing to press here. ArtiaX's save dialog opens in "
        f"the folder you LAST saved to, so after a switch paste the list's path (its ⧉ button) into the dialog. "
        f"A save under another name in that folder still becomes its own list, named after the file; a save "
        f"outside these folders shows in UNATTRIBUTED SAVES below, where you assign it."
    )


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
class _TomoInfo:
    """One tomogram group's own facts — the half that came from the Curation tab."""

    tomo_name: str
    has_geometry: bool  # a tomograms.star carries it — without one nothing loads or imports
    n_saves: int  # user .coords in its curation dir (what the watcher would pick up)
    curation_dir: str
    ref: ListRef  # the tomogram's reference slot: what curate / import act on


@dataclass(frozen=True, slots=True)
class _Computed:
    """Everything the render reads, in comparable (signature-able) form: tuples, not
    dicts, so `==` decides whether the view repaints."""

    overview: SpeciesOverview | None
    refs: tuple[tuple[str, str, ListRef], ...]  # (tomo, slug, ref) — one per overview row
    tomos: tuple[_TomoInfo, ...]  # one per tomogram of the species' universe, name-sorted
    species_slug: str  # the Curation/<slug>/ directory the watcher scans
    species_color: str
    error: str | None


def _compute(state, project_path: Path, species_id: str) -> _Computed:
    """Thread body: `species_overview`, the per-tomogram `tomograms.star` resolution and one
    glob per curation dir."""
    ov = species_overview(state, project_path, species_id)
    sp = state.get_species(species_id)
    color = str(getattr(sp, "color", "") or "#3b82f6")
    label = str(getattr(sp, "name", "") or species_id)
    # Universe = tomograms with rows ∪ every tomogram the project describes: a .coords can
    # be imported into — and a session started on — a tomogram that has no picks yet.
    anchors, stars = species_tomo_map(state, project_path, species_id, tuple(r.tomo_name for r in ov.rows))
    # The species half of the save path, taken from `curation_dir` itself rather than
    # re-slugged here: the contract must name the directory the watcher scans, and only
    # that helper owns the slug rule.
    species_slug = artiax_bridge.curation_dir(project_path, "x", species_id=species_id, species_label=label).parent.name

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
    tomos = []
    for tomo, star in stars.items():
        cdir = artiax_bridge.curation_dir(project_path, tomo, species_id=species_id, species_label=label)
        tomos.append(
            _TomoInfo(
                tomo_name=tomo,
                has_geometry=star is not None,
                n_saves=_count_user_saves(cdir, species_id, tomo),
                curation_dir=str(cdir),
                ref=auto_ref(project_path, anchors, species_id, tomo, star),
            )
        )
    return _Computed(ov, refs, tuple(tomos), species_slug, color, None)


def _count_user_saves(cdir: Path, species_id: str, tomo: str) -> int:
    """User `.coords` in the dir, EXCLUDING an untouched seed (13-S1): the 0-byte file
    crboost created is not something the user saved, and the `saves` chip claims it is."""
    seed = artiax_bridge.seed_coords_path(cdir, species_id, tomo)
    n = 0
    for c in artiax_bridge.user_coords_saves(cdir):
        try:
            if c == seed and c.stat().st_size == 0:
                continue
        except OSError:
            continue  # vanished between glob and stat — the next recompute sees what is there
        n += 1
    return n


# ── View ──────────────────────────────────────────────────────────────────────


class _PicksView(FingerprintedView):
    def __init__(self, container: ui.element, tab: PicksTab) -> None:
        super().__init__(container)
        self._tab = tab

    def signature(self) -> Any:
        c = self._tab.computed
        sc = session_status.scope()
        return (
            self._tab.ctx.species_id,
            get_project_state_for(self._tab.ctx.project_path).registry_rev,
            session_status.status(),
            (sc.get("species_id"), sc.get("tomo_name")),  # the live dot moves with the session's scope
            None if c is None else (c.overview, c.refs, c.tomos, c.species_slug, c.species_color, c.error),
            self._tab.watcher_signature(),
        )

    # ── Chrome ────────────────────────────────────────────────────────────────

    def render(self) -> None:
        c = self._tab.computed
        with ui.row().classes("w-full items-center gap-2 px-1"):
            # The title carries the save contract on hover (13-S3); the hint tail that sat
            # beside it said what the table below makes obvious.
            ui.label("PICKS & CURATION").classes(_LABEL_CLS).tooltip(
                _save_contract(c.species_slug) if c is not None and not c.error else "computing…"
            )
            ui.space()
            if c is not None and not c.error:
                # The watcher scans the session's folder every ~5 s and everything else every
                # ~30 s; this sweeps the whole project now and recomputes the table.
                house_button("Refresh", self._tab.refresh_now).tooltip(
                    "Scan every curation folder for saves now and recompute the counts — the watcher otherwise "
                    "sees the session's own folder within ~7 s and the others within ~30 s"
                )
                house_button("Extract all pending", self._tab.extract_all_pending).tooltip(
                    "Subtomo-extract every list of this species that is not extracted / stale — shows what it would "
                    "submit before anything is sent"
                )
        if c is None:
            ui.label("computing…").classes(_HINT_CLS + " px-1")
            return
        if c.error:
            ui.label(f"Picks unavailable — {c.error}").classes("text-[11px] text-red-600 px-1").tooltip(
                "species_overview / ref resolution / the curation-dir scan raised; full traceback in the server log"
            )
            return
        if not c.tomos:
            ui.label(
                "No tomogram is described by any tomograms.star yet — reconstruct or import tomograms first."
            ).classes(_HINT_CLS + " px-1 pt-2")
            return
        if not c.overview.rows:
            self._render_no_picks_hint()
        by_tomo: dict[str, list[ListRow]] = {}
        for row in c.overview.rows:
            by_tomo.setdefault(row.tomo_name, []).append(row)
        for info in c.tomos:
            self._render_group(c, info, by_tomo.get(info.tomo_name, []))
        self._render_log()

    def _render_no_picks_hint(self) -> None:
        with ui.row().classes("w-full items-center gap-1 px-1 pt-1"):
            ui.label("No picks yet — 'Curate picks' on a tomogram below to pick into it in ArtiaX, or run").classes(
                _HINT_CLS
            )
            ui.label("Pick candidates").classes(_LINK_CLS).on("click", lambda: self._tab.select_tab("jobs"))
            ui.label("from the Jobs tab.").classes(_HINT_CLS)

    # ── One tomogram ──────────────────────────────────────────────────────────

    def _render_group(self, c: _Computed, info: _TomoInfo, rows: list[ListRow]) -> None:
        with ui.element("div").classes("cb-ptable-group"):
            ui.label(info.tomo_name).classes("cb-ptable-group-name")
            # Copying the save dir is an act ON THE NAME beside it — the folder is the
            # tomogram's — so it sits with the name rather than at the far right with the
            # navigation verbs, a whole row's width away from what it copies.
            ui.button(icon="content_copy", on_click=lambda _e, d=info.curation_dir: self._tab.copy(d)).props(
                "flat dense round size=sm"
            ).tooltip(info.curation_dir)
            if rows:
                ui.label(f"{len(rows)} list{'s' if len(rows) != 1 else ''}").classes(_HINT_CLS)
            else:
                ui.label("no lists").classes(_HINT_CLS)
            if not info.has_geometry:
                render_chip(
                    "geometry",
                    "missing",
                    status="error",
                    tooltip="No tomograms.star carries this tomogram, so a .coords cannot be mapped into it and "
                    "ArtiaX has no volume to open. Reconstruct or import it first.",
                )
            if info.n_saves:
                render_chip(
                    "saves",
                    str(info.n_saves),
                    status="info",
                    tooltip="user .coords files in this tomogram's curation dir (an untouched pre-seeded list is not "
                    "a save) — the watcher registers EACH as its own pick list, named after the file",
                )
            ui.space()
            self._render_group_actions(info)

        # The table box is drawn for EVERY tomogram, empty of lists or not, because the
        # import row lives in it and that row is the point of an empty tomogram.
        with ui.element("div").classes("cb-ltable cb-ptable"):
            if rows:
                with ui.element("div").classes("cb-ltable-row cb-ptable-row cb-ltable-head"):
                    ui.element("div")  # swatch
                    ui.label("list").classes("cb-ltable-h-name")
                    ui.label("source").classes("cb-ltable-h-name")
                    ui.label("origin").classes("cb-ltable-h-name").tooltip(
                        "The template and mask these picks came out of. Hover a cell for the full paths."
                    )
                    ui.label("picks").classes("cb-ltable-h-num")
                    ui.label("Extracted").classes("cb-ltable-h-cell").tooltip(
                        "Subtomo-extracted state: ✓ when this list's subtomograms are cut and current with its "
                        "picks, ⚠ when they are stale, blank when it has not been extracted."
                    )
                    ui.label("job").classes("cb-ltable-h-cell").tooltip(
                        "The per-list extraction JOB behind that state — queued / running / succeeded / failed. "
                        "Hover a mark for the geometry it cut with and any failure text; click it for the logs."
                    )
                    ui.element("div")  # actions
                for row in rows:
                    self._render_row(c, info.tomo_name, row)
            self._render_import_row(info)

    def _render_import_row(self, info: _TomoInfo) -> None:
        """The import affordance, one per tomogram and ALWAYS there — under a group's lists
        and, for a tomogram with none, as the only thing in its table box. A `.coords` that
        landed outside the curation dir has no other way into this species, and a tomogram
        with no picks yet is exactly where a de-novo import starts; an icon that only
        appeared beside groups that already had rows hid the case it exists for."""
        row = ui.element("div").classes("cb-ptable-import")
        with row:
            ui.label("+")
            ui.label("import a .coords list by path…")
        row.tooltip(
            "Opens a file browser — paste an absolute path into its path bar to go straight to a file. The "
            f"file is read as full-resolution coordinates and registered as a manual list on {info.tomo_name}."
        )
        row.on("click", lambda _e, i=info: self._tab.import_picks(i.ref, start_dir=i.curation_dir))

    def _render_group_actions(self, info: _TomoInfo) -> None:
        """The ONE place each per-tomogram verb exists (09-S2): the door to ArtiaX, and the
        jumps to the viewer and the Journey. The sidebar launcher, the Picks-tab ⚡, the
        species-level import header button, the session chip, the rail light and the
        viewer-rail `curate ↗` are all gone (09-S2 … 13-S3); the Journey's ⚡ navigates here.
        Import moved to the table's own import row and copy to the tomogram's name; what is
        left here is what LEAVES this table.

        `Curate picks` is the one icon+text button in the house vocabulary: the glyph IS the
        door. The green dot beside it is the live marker — the running session has THIS
        tomogram open — read from the shared `session_status` cache the tab already polls."""
        # The live dot precedes the button, so a row that gains it does not shift the
        # button under a cursor already on it.
        if self._tab.session_live_here(info.tomo_name):
            ui.element("span").classes("cb-artiax-live").style(
                "display: inline-block; width: 7px; height: 7px; border-radius: 50%; background: #16a34a; "
                "flex: 0 0 auto;"
            ).tooltip("the live ChimeraX + ArtiaX session has this tomogram open")
        house_button(
            "Curate picks",
            lambda _e, r=info.ref: self._tab.curate(r),
            icon="view_in_ar",
            tooltip="Prepares this tomogram for ArtiaX — the reference picks, the startup script, the manifest "
            "that makes every save here attributable — pre-seeds "
            f"{artiax_bridge.default_seed_name(self._tab.ctx.species_id, info.tomo_name)} as "
            "a 0-pick list (never overwrites one that exists), and opens the control center to start a session "
            "on it. With a session already running on another tomogram it offers Switch or Restart.",
        )
        # 09-S5, landed with picking_ui/11-S5: the viewer icon opens the full-page pick
        # viewer on this (species, tomogram) — slabs, the lists strip and the cutout gallery
        # at workspace size, with curation mode. The Journey icon stays beside it because
        # the two answer different questions: the viewer is about THESE picks, the Journey
        # about everything that happened to this tilt-series before them.
        ui.button(icon="grid_view", on_click=lambda _e, t=info.tomo_name: self._tab.open_viewer(t)).props(
            "flat dense round size=sm color=indigo"
        ).tooltip("Open this tomogram's picks in the full-page pick viewer")
        with (
            ui.button(on_click=lambda _e, t=info.tomo_name: self._tab.open_in_journey(t))
            .props("flat dense round size=sm color=indigo")
            .tooltip("This tomogram's whole pipeline — motion, CTF, alignment, reconstruction")
        ):
            ui.html(load_icon_svg("journey.svg", "#4f46e5", size=16), sanitize=False)

    def _render_row(self, c: _Computed, tomo: str, row: ListRow) -> None:
        ref = self._tab.ref(tomo, row.slug)
        with ui.element("div").classes("cb-ltable-row cb-ptable-row"):
            ui.element("div").classes(f"cb-ltable-swatch cb-swatch-{glyph_for(PickListType(row.list_type))}").style(
                f"background: {c.species_color};"
            ).tooltip(f"{row.list_type} list")
            # ONE grid child: the name and, for a hand-picked list, the copy-path button
            # beside it (the maintainer, 2026-09-06: no alias, the file name itself, and its
            # path one click away — it is what has to be found in ArtiaX's save dialog).
            coords = _coords_path(row)
            with ui.row().classes("items-center gap-0 flex-nowrap").style("min-width: 0; overflow: hidden;"):
                name = ui.label(row.label).classes("cb-ltable-name").style("min-width: 0;")
                if is_default_slug(row.slug, self._tab.ctx.species_id, tomo):
                    name.tooltip(
                        f"the pre-seeded ArtiaX list — pick into it and save it back to {coords or 'this file'}; "
                        f"crboost updates this row · {row.star_path or 'no backing star'}"
                    )
                else:
                    name.tooltip(row.star_path or "no backing star on disk")
                if coords:
                    ui.button(icon="content_copy", on_click=lambda _e, p=coords: self._tab.copy_file(p)).props(
                        "flat dense round size=xs"
                    ).tooltip(f"Copy this list's .coords path — {coords}")
            if row.source_kind:
                ui.label(row.source_kind).classes("cb-ptable-source").tooltip(
                    f"{row.source_kind} · {row.source_ref or '—'}"
                )
            else:
                ui.label("—").classes("cb-ptable-source").tooltip(
                    "no provenance recorded — this list pre-dates source tracking (09-S2)"
                )
            self._render_origin(row)
            ui.label(_count_text(row.count, row.kept)).classes("cb-ltable-count").tooltip(
                "kept / total picks after keep-drop curation"
            )
            self._render_ext_cell(row)
            self._render_job_chip(row)
            with ui.row().classes("cb-ptable-actions items-center gap-0 flex-nowrap"):
                self._render_actions(tomo, row, ref)

    def _render_origin(self, row: ListRow) -> None:
        """The `origin` cell (09-S4): which template and mask produced these coordinates.

        ALWAYS exactly one grid child. Three honest answers and no fourth: the template's
        file name (full paths on hover), "—" for coordinates no template produced, or the
        reason the chain could not be resolved — never a guess at which template it "probably"
        was (CLAUDE.md "Surfacing uncertainty")."""
        if row.origin_note:
            ui.label(row.origin_note).classes("cb-ptable-source text-orange-700").tooltip(
                "Cannot say which template and mask produced these picks: "
                f"{row.origin_note}. The chain is candidate-extract job → the template-match job it consumed → "
                "that job's template + mask; one link of it is missing, and crboost will not guess the rest."
            )
            return
        if not row.template_path:
            ui.label("—").classes("cb-ptable-source").tooltip(
                "Hand-placed, imported or merged coordinates — no template produced them."
            )
            return
        ui.label(Path(row.template_path).name).classes("cb-ptable-source").tooltip(
            f"template {row.template_path} · mask {row.mask_path or '—'}"
        )

    def _render_ext_cell(self, row: ListRow) -> None:
        """The `Extracted` column — ALWAYS exactly one grid child, a MARK only when there is
        something to mark: ✓ when this list's subtomograms are cut and current, ⚠ when they
        are stale and need re-cutting. Not-extracted and not-applicable are BLANK. They used
        to draw `○` and `·`, but not-extracted is the majority state of a picking project, so
        a glyph on nearly every row said nothing while making the two that matter hard to
        find. The state stays on the cell's hover either way — blank here means "nothing has
        happened", never "we don't know"."""
        cell = ui.element("div").classes("cb-ltable-cell")
        if row.extraction_state == NOT_APPLICABLE:
            cell.tooltip("auto list of a species with no subtomo-extraction job — nothing to extract here")
            return
        text, cls = list_actions.extraction_badge(ListExtractionState(row.extraction_state))
        if not text:
            return
        glyph, _, words = text.partition(" ")
        if row.extraction_state == ListExtractionState.NOT_EXTRACTED.value:
            cell.tooltip(words)  # no mark, so the hover carries the words without its glyph
            return
        with cell:
            ui.label(glyph).classes(f"cb-ltable-badge {cls}")
        cell.tooltip(text)

    def _render_job_chip(self, row: ListRow) -> None:
        """The `job` column: what the per-list extraction INSTANCE is doing (roadmap 07-S4).

        ALWAYS emits exactly one grid child — the cell stays empty for a list with no
        instance (the `auto` list never gets one, and a list that was never submitted has
        nothing to add to `Extracted`), because a skipped child would slide every later column of
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
        """extract · dedup · delete, always in those three slots.

        `dedup` only applies to a merged list, but the slot is HELD (an empty box of the same
        width) on every other row rather than skipped: three verbs that slide one place left
        whenever a row happens not to be merged is the same staggering the grid template above
        exists to kill, one level down."""
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
        else:
            ui.element("div").style("width: 28px; height: 28px; flex: 0 0 auto;")
        ui.button(icon="delete_outline", on_click=lambda _e, r=ref: self._tab.delete_list(r)).props(
            "flat dense round size=sm color=negative"
        ).tooltip("Delete this list (its star, curated subset, extraction output and saves)")

    # ── Watcher log ───────────────────────────────────────────────────────────

    def _render_log(self) -> None:
        events, unattributed = self._tab.watcher_view()
        with ui.row().classes("w-full items-baseline gap-2 px-1 pt-3"):
            ui.label("WATCHER").classes(_LABEL_CLS)
            ui.label("what the server did with saves for this species").classes(_HINT_CLS)
        if not events:
            ui.label("Nothing ingested yet for this species.").classes(_HINT_CLS + " px-1")
        for e in events:
            with ui.row().classes("w-full items-center gap-2 px-1").style("min-height: 20px; flex-wrap: nowrap;"):
                ui.label(e["ts"].replace("T", " ")).classes("text-[10px] text-gray-400").style(MONO)
                ui.label(e["kind"]).classes(f"text-[10px] font-bold {_EVENT_CLS.get(e['kind'], 'text-gray-500')}")
                ui.label(e["tomo_name"]).classes("text-[10px] text-slate-600 truncate").style(MONO)
                ui.label(Path(e["coords"]).name).classes("text-[10px] text-gray-500 truncate").tooltip(e["coords"])
                if e.get("count") is not None:
                    ui.label(f"{e['count']} picks").classes("text-[10px] text-emerald-700")
                if e.get("message"):
                    ui.label(e["message"]).classes("text-[10px] text-orange-700 truncate").tooltip(e["message"])
        if unattributed:
            with ui.row().classes("w-full items-baseline gap-2 px-1 pt-2"):
                ui.label("UNATTRIBUTED SAVES").classes(_LABEL_CLS + " text-orange-600")
                ui.label("project-wide — a save here reached no species, and was never guessed at").classes(_HINT_CLS)
            for u in unattributed:
                with ui.column().classes("w-full gap-0 px-1"):
                    with ui.row().classes("w-full items-center gap-2").style("flex-wrap: nowrap;"):
                        ui.label(u["dir"]).classes("text-[10px] text-slate-700 truncate").style(MONO).tooltip(u["dir"])
                        n = len(u.get("files") or [])
                        if n:
                            ui.label(f"{n} file{'s' if n != 1 else ''}").classes(_HINT_CLS).tooltip(
                                "\n".join(Path(f).name for f in u["files"])
                            )
                        ui.space()
                        # The staging step (10-S2): the ONLY way these picks acquire a
                        # species, and it is the user's call — never an inference.
                        ui.label("assign ↗").classes(_LINK_CLS).on(
                            "click", lambda _e, entry=u: self._tab.assign_unattributed(entry)
                        ).tooltip("Move these .coords into a species + tomogram's curation folder and import them")
                    ui.label(u["reason"]).classes("text-[10px] text-orange-700")


def _noop() -> None:
    """`on_done` for the shared actions: this page repaints off the registry rev they
    bump, so there is nothing to call back into."""
    return None


# ── Tab ───────────────────────────────────────────────────────────────────────


class PicksTab:
    def __init__(self, ctx: TabContext) -> None:
        self.ctx = ctx
        self.computed: _Computed | None = None
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

    async def refresh_now(self) -> None:
        """The Refresh button: sweep every curation folder of the project NOW (the watcher
        ingests what it finds and bumps the registry rev), then recompute this tab without
        waiting for its 15 s disk cadence. A save younger than the watcher's settle window
        is picked up on the next tick, not here — ArtiaX writes are not atomic."""
        async with self._flight("refresh_now") as acquired:
            if not acquired:
                return
            watcher = self._watcher()
            if watcher is not None:
                await watcher.sweep_now(self.ctx.project_path)
            self._last_at = 0.0
            self.refresh()

    def refresh(self) -> None:
        """Page tick / on show. Two independent cadences: the session poll (shared, ~16 s,
        squeue) and the disk recompute (15 s) — neither runs while the page is hidden,
        because the page only ticks the VISIBLE tab."""
        if session_status.is_stale():
            asyncio.create_task(self._poll_session())
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
            self.watcher_signature(),
        )

    async def _poll_session(self) -> None:
        async with self._flight("session") as acquired:
            if not acquired:
                return
            await session_status.poll(self.backend)
            if self._view is not None:
                self._view.refresh()

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
                computed = _Computed(None, (), (), "", "#3b82f6", f"{type(e).__name__}: {e}")
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

    # ── Watcher (in-memory; safe on the event loop and in a signature) ────────

    def _watcher(self):
        return getattr(self.backend, "curation_watcher", None)

    def watcher_view(self) -> tuple[list[dict], list[dict]]:
        """(this species' events newest-first, capped; project-wide unattributed dirs)."""
        watcher = self._watcher()
        if watcher is None:
            return [], []
        events = [e for e in watcher.events(self.ctx.project_path) if e["species_id"] == self.ctx.species_id]
        return list(reversed(events))[:12], watcher.unattributed(self.ctx.project_path)

    def watcher_signature(self) -> tuple:
        """Cheap fingerprint of what `_render_log` draws — the watcher appends in place, so
        counts + the newest timestamp are enough to notice."""
        events, unattributed = self.watcher_view()
        return (len(events), events[0]["ts"] if events else "", tuple(u["dir"] for u in unattributed))

    # ── Navigation ────────────────────────────────────────────────────────────

    def select_tab(self, key: str) -> None:
        select = self.ctx.callbacks.get("species_select_tab")
        if select is not None:
            select(key)

    async def open_viewer(self, tomo: str | None) -> None:
        """Open the full-page pick viewer on this species + tomogram (picking_ui 11-S5).
        The workspace builds the page on first use and swaps it into the main area; this
        tab's own SingleFlight guards the link, which sits in a poll-refreshed container."""
        async with self._flight("viewer") as acquired:
            if not acquired:
                return
            open_viewer = self.ctx.callbacks.get("open_pick_viewer")
            if open_viewer is None:
                ui.notify("Pick viewer not available in this view", type="warning")
                return
            await open_viewer(self.ctx.species_id, tomo)

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

    # ── ArtiaX round trip ─────────────────────────────────────────────────────

    def session_live_here(self, tomo: str) -> bool:
        """True when the running session's recorded scope is THIS species + tomogram — the
        live dot beside `Curate picks` (13-S3). No I/O: reads the shared `session_status`
        cache this tab already polls, and the view's signature carries the scope so the dot
        moves on the tick after a switch."""
        if not session_status.is_live():
            return False
        sc = session_status.scope()
        return sc.get("species_id") == self.ctx.species_id and sc.get("tomo_name") == tomo

    async def curate(self, ref: ListRef) -> None:
        """THE launch/scope affordance (09-S2), and since 10-S1 the ONLY one: it declares
        this (species, tomogram) as a session's scope — writing the reference export, the
        `.cxc`, the `manifest.json` and the pre-seeded default list (13-S1) — and opens the
        control center to start one.

        It used to branch on liveness HERE and swap a running session over the REST channel.
        That branch stays gone: this is one path, always. When a session is live on another
        tomogram, the control center says so and offers the confirmed switch (13-S2) or a
        restart — the decision is made there, in front of the warning, never on a stale
        liveness flag in this tab."""
        await list_actions.curate_in_artiax(self.backend, ref)

    async def import_picks(self, ref: ListRef, *, start_dir: str | None = None) -> None:
        """The import row's click: BROWSE first — the shared file picker, opened on this
        tomogram's curation dir, whose path bar also takes an absolute path straight to a
        file — then the import confirm, pre-filled, because the frame a by-path file is
        read in is stated there and nowhere else. SingleFlight-guarded like every dialog
        opener here: the row sits in a poll-refreshed container that can be replaced
        mid-click, so several clicks can legitimately land before one opens."""
        async with self._flight(f"import:{ref.tomo_name}") as acquired:
            if not acquired:
                return
            if ref.tomograms_star is None:
                ui.notify(
                    f"No tomograms.star resolved for {ref.tomo_name} — cannot map .coords into it.", type="warning"
                )
                return
            start = str(self.ctx.project_path)
            if start_dir and await asyncio.to_thread(Path(start_dir).is_dir):
                start = start_dir
            picked = await local_file_picker(start, upper_limit=None, mode="file", glob="*.coords")
            if not picked:
                return
            list_actions.import_picks_from_path(
                self.backend,
                ref,
                on_done=self.refresh,
                intro=f"Import this .coords as a manual pick list on {ref.tomo_name}.",
                initial_path=picked[0],
            )

    async def assign_unattributed(self, entry: dict) -> None:
        """Give an orphaned save an explicit (species, tomogram) — the staging step of
        10-S2. Its files are MOVED into the right curation dir and the watcher ingests
        them; nothing here infers anything."""
        await list_actions.assign_unattributed(self.backend, self.ctx.project_path, entry, on_done=self.refresh)

    def copy(self, path: str) -> None:
        ui.clipboard.write(path)
        ui.notify("Copied the save folder — paste it into ArtiaX's save dialog", type="positive", timeout=1500)

    def copy_file(self, path: str) -> None:
        ui.clipboard.write(path)
        ui.notify(
            "Copied the .coords path — paste it into the file-name field of ArtiaX's save dialog",
            type="positive",
            timeout=2500,
        )

    # ── List actions ──────────────────────────────────────────────────────────

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

    async def delete_list(self, ref: ListRef) -> None:
        """`list_actions.delete_list` — the same confirm + delete the Journey / viewer rail
        offers, which is why it lives there and not here."""
        await list_actions.delete_list(
            self.backend, ref.project_path, ref.species_id, ref.tomo_name, ref.slug, ref.label, on_done=self.refresh
        )

    # ── Extract all pending ───────────────────────────────────────────────────

    async def extract_all_pending(self) -> None:
        """Pre-flight FIRST (the pending report is the preview), then submit. Never a blind
        fan-out: `extract_pending_lists` starts one SLURM job per pending list, so the user
        sees the count, the tomograms and the blocked reasons before confirming — and commits
        the extraction geometry here if the species has none."""
        async with self._flight("extract_all") as acquired:
            if not acquired:
                return
            ctx = self.ctx
            rows = await self.backend.get_pending_extractions(ctx.project_path, ctx.species_id)
            pending = [r for r in rows if not r["blocked_reason"]]
            blocked = [r for r in rows if r["blocked_reason"]]
            if not pending:
                msg = "Nothing pending — every list is extracted and current with its picks."
                if blocked:
                    msg = f"Nothing extractable: {len(blocked)} list(s) blocked (see the table)."
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
                    ui.label(f"⚠ blocked — {h['tomo_name']} · {h['slug']}: {h['blocked_reason']}").classes(
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
                    house_button("Cancel", lambda: confirm.submit(None))
                    house_button("Extract", lambda: confirm.submit(True), kind="accent")
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
            res = await backend.extract_pending_lists(ctx.project_path, ctx.species_id)
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

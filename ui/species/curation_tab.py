"""Curation tab (roadmap 11-S4) — the ArtiaX round trip for this species.

Four blocks, in the order the round trip happens:

1. **Session** — live / off / unknown (`ui/particles/session_status`, one shared squeue
   poll for the whole server) + "Open control center" (the existing
   `curation_session_dialog`; an inline split is v2).
2. **Save contract** — spelled out ONCE per species instead of per tomogram, because it is
   the one thing a user must get right by hand: where to save, in what format, under what
   name, and how long before the watcher picks it up. The timings are derived from the
   watcher's own constants, not retyped.
3. **Tomograms** — one row each: geometry · lists · saves on disk · curate (start/attach a
   session on this tomogram) · ⚡ (swap the running session to it) · import a `.coords` by
   path · copy the save dir.
4. **Watcher log + unattributed dirs** — what the `CurationWatcher` did with this species'
   saves, and every dir holding a save it could NOT attribute, with the reason. That second
   list is the never-silent half: a save under a mistyped dir would otherwise vanish.

Same shape as the other tabs: an off-loop `_compute` (curation dirs are globbed, tomogram
stars resolved) behind a rev-gated `FingerprintedView`, recomputed when the in-memory key
moves, on show, and at most every 15 s while shown. Per-tomo actions are
`ui/particles/list_actions`, so the Journey and this tab drive one implementation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nicegui import ui

from services.curation.watcher import FULL_SWEEP_EVERY, SETTLE_SEC, TICK_SEC
from services.particles.list_ref import ListRef, auto_ref, species_tomo_map
from services.project_state import get_project_state_for
from services.visualization import artiax_bridge
from ui.components.buttons import house_button
from ui.components.chip import render_chip
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.curation_session_dialog import open_curation_control_center
from ui.particles import list_actions, session_status
from ui.species.tab import TabContext
from ui.styles import MONO

logger = logging.getLogger(__name__)

_DISK_REFRESH_S = 15.0
# What the save-contract line promises, derived from the watcher's own cadence: a full
# sweep of Curation/*/*/ every FULL_SWEEP_EVERY ticks, plus the settle window that keeps a
# half-written file until the next tick. A tomogram LOADED in the live session is in the
# watcher's hot set and is rescanned every tick instead.
_DETECT_S = FULL_SWEEP_EVERY * TICK_SEC + SETTLE_SEC
_HOT_DETECT_S = TICK_SEC + SETTLE_SEC

_LABEL_CLS = "text-[10px] font-bold text-gray-500 uppercase tracking-wider"
_HINT_CLS = "text-[10px] text-gray-400"
_BODY_CLS = "text-[11px] text-gray-600"

_SESSION_CHIP = {
    session_status.LIVE: ("running", "ok", "A ChimeraX + ArtiaX session of yours is up — ⚡ swaps it to a tomogram"),
    session_status.OFF: ("none", "neutral", "No session running — 'curate' on a tomogram row starts one"),
    session_status.UNKNOWN: ("unknown", "warn", "Could not ask SLURM whether a session is running"),
}
_EVENT_CLS = {
    "ingested": "cb-badge-ok",
    "error": "text-red-600",
    "no-geometry": "cb-badge-stale",
    "unattributed": "cb-badge-stale",
}


@dataclass(frozen=True, slots=True)
class _TomoRow:
    tomo_name: str
    has_geometry: bool  # a tomograms.star carries it — without one nothing can be loaded or imported
    n_lists: int
    n_saves: int  # user .coords in its curation dir (what the watcher would pick up)
    curation_dir: str
    ref: ListRef


@dataclass(frozen=True, slots=True)
class _Computed:
    rows: tuple[_TomoRow, ...]
    species_slug: str
    error: str | None


def _compute(state, project_path: Path, species_id: str) -> _Computed:
    """Thread body: tomogram-star resolution + one glob per curation dir."""
    sp = state.get_species(species_id)
    label = str(getattr(sp, "name", "") or species_id)
    anchors, stars = species_tomo_map(state, project_path, species_id)
    # The species half of the save path, taken from `curation_dir` itself rather than
    # re-slugged here: the contract line must name the directory the watcher scans, and
    # only that helper owns the slug rule.
    species_slug = artiax_bridge.curation_dir(project_path, "x", species_id=species_id, species_label=label).parent.name
    rows: list[_TomoRow] = []
    for tomo, star in stars.items():
        cdir = artiax_bridge.curation_dir(project_path, tomo, species_id=species_id, species_label=label)
        rows.append(
            _TomoRow(
                tomo_name=tomo,
                has_geometry=star is not None,
                n_lists=len(state.get_pick_lists(species_id, tomo)),
                n_saves=len(artiax_bridge.user_coords_saves(cdir)),
                curation_dir=str(cdir),
                ref=auto_ref(project_path, anchors, species_id, tomo, star),
            )
        )
    return _Computed(tuple(rows), species_slug, None)


class _CurationView(FingerprintedView):
    def __init__(self, container: ui.element, tab: CurationTab) -> None:
        super().__init__(container)
        self._tab = tab

    def signature(self) -> Any:
        c = self._tab.computed
        return (
            self._tab.ctx.species_id,
            get_project_state_for(self._tab.ctx.project_path).registry_rev,
            session_status.status(),
            None if c is None else (c.rows, c.species_slug, c.error),
            self._tab.watcher_signature(),
        )

    def render(self) -> None:
        self._render_session()
        c = self._tab.computed
        if c is None:
            ui.label("computing…").classes(_HINT_CLS + " px-1")
            return
        if c.error:
            ui.label(f"Curation view unavailable — {c.error}").classes("text-[11px] text-red-600 px-1").tooltip(
                "the per-tomogram scan raised; full traceback in the server log"
            )
            return
        self._render_contract(c)
        self._render_tomos(c)
        self._render_log()

    # ── Session ───────────────────────────────────────────────────────────────

    def _render_session(self) -> None:
        st = session_status.status()
        text, status, tip = _SESSION_CHIP[st]
        with ui.row().classes("w-full items-center gap-2 px-1"):
            ui.label("CURATION").classes(_LABEL_CLS)
            with ui.element("div").classes("cb-chip-strip"):
                render_chip(
                    "session",
                    text,
                    status=status,
                    tooltip=(f"{tip}\n{session_status.last_error()}" if session_status.last_error() else tip),
                )
            house_button("Open control center", self._tab.open_control_center).tooltip(
                "Start / connect to a ChimeraX + ArtiaX session and see its load commands"
            )

    # ── Save contract ─────────────────────────────────────────────────────────

    def _render_contract(self, c: _Computed) -> None:
        with ui.column().classes("w-full gap-0 px-1 pt-2"):
            ui.label("WHERE TO SAVE").classes(_LABEL_CLS)
            ui.label(f"Curation/{c.species_slug}/<tomogram>/").classes("text-[11px] text-gray-700").style(MONO)
            ui.label(
                f"Format .coords (positions only, corner-Å). Any filename EXCEPT auto.coords and *_ref.coords "
                f"— those are crboost's own exports. Newest save in the folder wins. Picked up automatically "
                f"within ~{_HOT_DETECT_S:.0f} s while the tomogram is loaded in the session, ~{_DETECT_S:.0f} s "
                f"otherwise; no need to press anything here."
            ).classes(_HINT_CLS)

    # ── Tomograms ─────────────────────────────────────────────────────────────

    def _render_tomos(self, c: _Computed) -> None:
        with ui.row().classes("w-full items-baseline gap-2 px-1 pt-2"):
            ui.label("TOMOGRAMS").classes(_LABEL_CLS)
            ui.label("start a session on one, or swap the running one to it").classes(_HINT_CLS)
        if not c.rows:
            ui.label(
                "No tomogram is described by any tomograms.star yet — reconstruct or import tomograms first."
            ).classes(_HINT_CLS + " px-1")
            return
        for row in c.rows:
            self._render_tomo_row(row)

    def _render_tomo_row(self, row: _TomoRow) -> None:
        with (
            ui.row()
            .classes("w-full items-center gap-2 px-1")
            .style("min-height: 26px; border-bottom: 1px solid #f1f5f9; flex-wrap: nowrap;")
        ):
            ui.label(row.tomo_name).classes("text-[11px] font-semibold text-slate-700 truncate").style(MONO)
            if not row.has_geometry:
                render_chip(
                    "geometry",
                    "missing",
                    status="error",
                    tooltip="No tomograms.star carries this tomogram, so a .coords cannot be mapped into it and "
                    "ArtiaX has no volume to open. Reconstruct or import it first.",
                )
            if row.n_lists:
                render_chip("lists", str(row.n_lists), tooltip="pick lists registered for this species here")
            if row.n_saves:
                render_chip(
                    "saves",
                    str(row.n_saves),
                    status="info",
                    tooltip="user .coords files in this tomogram's curation dir — the newest is what the watcher "
                    "registers as the manual list",
                )
            ui.space()
            ui.button(icon="view_in_ar", on_click=lambda _e, r=row.ref: self._tab.curate(r)).props(
                "flat dense round size=sm color=indigo"
            ).tooltip("Curate in ArtiaX — prepare this tomogram's bundle and open the control center on it")
            ui.button(icon="bolt", on_click=lambda _e, r=row.ref: self._tab.load_into_session(r)).props(
                "flat dense round size=sm color=amber-8"
            ).tooltip("Load this tomogram + its picks into the ALREADY-running session")
            ui.button(icon="download", on_click=lambda _e, r=row.ref: self._tab.import_picks(r)).props(
                "flat dense round size=sm"
            ).tooltip("Import a .coords by explicit path (a save that landed outside the curation dir)")
            ui.button(icon="content_copy", on_click=lambda _e, d=row.curation_dir: self._tab.copy(d)).props(
                "flat dense round size=sm"
            ).tooltip(row.curation_dir)

    # ── Watcher log ───────────────────────────────────────────────────────────

    def _render_log(self) -> None:
        events, unattributed = self._tab.watcher_view()
        with ui.row().classes("w-full items-baseline gap-2 px-1 pt-2"):
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
                    ui.label(u["dir"]).classes("text-[10px] text-slate-700 truncate").style(MONO).tooltip(u["dir"])
                    ui.label(u["reason"]).classes("text-[10px] text-orange-700")


class CurationTab:
    def __init__(self, ctx: TabContext) -> None:
        self.ctx = ctx
        self.computed: _Computed | None = None
        self._last_key: Any = None
        self._last_at: float = 0.0
        self._flight = SingleFlight()
        self._view: _CurationView | None = None

    # ── Tab protocol ──────────────────────────────────────────────────────────

    def build(self, container: ui.element) -> None:
        with container:
            slot = ui.column().classes("w-full gap-1 p-2")
        self._view = _CurationView(slot, self)
        self._view.refresh()

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
        return (get_project_state_for(self.ctx.project_path).registry_rev, self.watcher_signature())

    async def _poll_session(self) -> None:
        async with self._flight("session") as acquired:
            if not acquired:
                return
            await session_status.poll(self.ctx.backend)
            if self._view is not None:
                self._view.refresh()

    async def _recompute(self, key: tuple) -> None:
        async with self._flight("compute") as acquired:
            if not acquired:
                return
            ctx = self.ctx
            state = get_project_state_for(ctx.project_path)
            try:
                computed = await asyncio.to_thread(_compute, state, ctx.project_path, ctx.species_id)
            except Exception as e:
                # Reported, not swallowed: traceback to the log, the cause into the tab.
                logger.exception("Curation compute failed for %s", ctx.species_id)
                computed = _Computed((), "", f"{type(e).__name__}: {e}")
            self.computed = computed
            self._last_key = key
            self._last_at = time.monotonic()
            if self._view is not None:
                self._view.refresh()

    # ── Watcher (in-memory; safe on the event loop and in a signature) ────────

    def _watcher(self):
        return getattr(self.ctx.backend, "curation_watcher", None)

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

    # ── Actions ───────────────────────────────────────────────────────────────

    async def open_control_center(self) -> None:
        async with self._flight("control_center") as acquired:
            if not acquired:
                return
            await open_curation_control_center(self.ctx.backend, self.ctx.project_path)

    async def curate(self, ref: ListRef) -> None:
        await list_actions.curate_in_artiax(self.ctx.backend, ref)

    async def load_into_session(self, ref: ListRef) -> None:
        await list_actions.load_tomo_into_session(self.ctx.backend, ref)

    def import_picks(self, ref: ListRef) -> None:
        list_actions.import_picks_from_path(self.ctx.backend, ref, on_done=self.refresh)

    def copy(self, path: str) -> None:
        ui.clipboard.write(path)
        ui.notify("Copied the save folder — paste it into ArtiaX's save dialog", type="positive", timeout=1500)

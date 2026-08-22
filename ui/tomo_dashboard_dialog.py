"""
Journey — per-tilt-series dashboard.

The dialog is anchored on a tilt-series. The sidebar lists every TS in the
project (union across all array-job manifests) with a 6-pill journey strip
showing pipeline-stage status. Selecting a TS loads a stack of section cards
in the main pane — one per pipeline stage that has data for the selected TS.

Pill states per stage:
  ok        — stage produced expected output for this TS
  fail      — stage errored on this TS
  running   — stage is in flight (job RUNNING/QUEUED)
  zero      — stage processed this TS but produced no output (e.g. PyTOM
              returned zero picks above cutoff). Distinguishable from
              "pending" so users can tell "ran but yielded nothing" from
              "never ran".
  pending   — stage hasn't reached this TS yet

The surface used to be called "Tomogram Dashboard"; it's been renamed
"Journey" because it carries per-TS analytics across the whole pipeline,
not just the candidate-extract preview pair.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pandas as pd
from nicegui import app, ui

from services.configs.user_prefs_service import get_prefs_service
from services.dashboard_data import (
    alignment_registry_df,
    collect_dashboard_journey,
    collect_species_journey,
    denoised_mrc_from_registry,
    filter_kept_dropped_from_registry,
    filter_verdicts_from_registry,
    find_job_by_type,
    fsm_registry_df,
    job_dir_for,
    journey_signature,
    read_tomograms_table,
    recon_mrc_map,
    resolve_volume_for_3dmod,
    tsctf_registry_df,
    vis_asset_url,
    warp_hand_from_registry,
)
from services.models_base import InstanceId, JobType
from services.pixel_chain import apply_sanity_rules, compute_pixel_chain
from services.visualization.preview_orchestrator import _find_warp_tomo_preview
from services.visualization.preview_render import is_output_stale, render_xy_slab_preview
from ui.components.chip import render_chip
from ui.current_project import current_project_state
from ui.dashboard.css import ensure_assets_loaded
from ui.dashboard.figures import _build_per_tilt_chart, _is_meaningful_series, _safe_floats, _stats
from ui.dashboard.pixel_sanity import render_pixel_sanity_table
from ui.dashboard.strip import build_strip
from ui.particles.pick_viewer import render_imported_particles_section, render_particles_section, reset_auto_kick_state

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dashboard panel prefs (R2/R3) — user-level, persisted across projects + TS
# via the shared user_prefs_service (app.storage.user + ~/.crboost/prefs.json).
# ---------------------------------------------------------------------------

# (key, label) for every toggleable detail panel, in render order. The label
# shows in the Panels toggle row; the key is the stable pref id AND the gate key
# used in _render_main_pane_for_ts.
_DASHBOARD_PANEL_KEYS: list[tuple[str, str]] = [
    ("dataset", "Dataset"),
    ("fs_ctf", "FS·CTF"),
    ("tilt_filter", "Tilt-filter"),
    ("ts_align", "TS-align"),
    ("ts_ctf", "TS-CTF"),
    ("tilt_qc", "Tilt-QC"),
    ("reconstruct", "Reconstruct"),
    ("particles", "Particles"),
]


def _hidden_dashboard_panels() -> set[str]:
    """Panel keys the user toggled OFF. Stored as a list (absence ⇒ visible)."""
    try:
        return set(get_prefs_service().prefs.dashboard_hidden_panels)
    except Exception:
        return set()


def _dataset_collapsed() -> bool:
    try:
        return bool(get_prefs_service().prefs.dashboard_dataset_collapsed)
    except Exception:
        return True


def _save_dashboard_prefs() -> None:
    """Persist the prefs singleton to app.storage.user (+ the ~/.crboost mirror)."""
    try:
        get_prefs_service().save_to_app_storage(app.storage.user)
    except Exception:
        pass


def _toggle_panel(key: str, visible: bool, on_change) -> None:
    svc = get_prefs_service()
    hidden = set(svc.prefs.dashboard_hidden_panels)
    if visible:
        hidden.discard(key)
    else:
        hidden.add(key)
    svc.prefs.dashboard_hidden_panels = sorted(hidden)
    _save_dashboard_prefs()
    on_change()


def _build_panel_toggle_row(host, on_change) -> None:
    """Dense-checkbox row (one per detail panel) gating which sections render.
    Built once as journey chrome — toggling updates the user pref, persists it,
    and re-renders the detail pane via on_change."""
    host.clear()
    hidden = _hidden_dashboard_panels()
    with host:
        ui.label("Panels").classes("cb-panel-toggle-label")
        for key, label in _DASHBOARD_PANEL_KEYS:
            ui.checkbox(label, value=(key not in hidden)).props("dense").classes(
                "text-[11px] cb-panel-cb"
            ).on_value_change(lambda e, k=key: _toggle_panel(k, e.value, on_change))


# ---------------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------------


def build_journey_panel(container, callbacks: dict | None = None) -> None:
    """Build the per-TS Journey dashboard embedded into ``container``.

    Formerly ``open_tomo_dashboard`` (a maximized dialog). De-dialoged in P1 so
    the journey swaps into the workspace ``main_area`` like the pipeline and
    workbench views, instead of an overlay that covered the 60px icon strip.
    ``container`` is the workspace's ``journey_container`` (a flex column).
    ``callbacks``, when given, receives ``on_journey_active(bool)`` so the
    workspace can pause the live-refresh timer while the journey is hidden, and
    supplies ``open_species`` + ``species_select_tab`` — composed here into the
    Particles section's route to the Species page, which owns the pick-list actions
    since 11-S3. Read here and passed DOWN rather than stashed module-level: those
    callbacks close over one client's page.
    """
    state = current_project_state()
    if state.project_path is None:
        container.clear()
        with container, ui.element("div").classes("cb-empty"):
            ui.icon("folder_off", size="40px").classes("text-gray-400")
            ui.label("No project loaded.").classes("text-sm text-gray-500")
        return

    project_path = Path(state.project_path)
    _open_species = (callbacks or {}).get("open_species")

    def _manage_species(species_id: str) -> None:
        """The Particles section's route into the registry — used by the section header's
        'manage in Particles registry ↗' and the list toolbox's 'curate ↗' (09-S2).
        Composed here, where the workspace's callbacks are in scope: select the species AND
        land on Picks & curation — the page otherwise reuses its last tab (Overview on a
        fresh workspace), and none of the actions that moved off the Journey are on
        Overview, so the link would strand the user one step short of what it promises."""
        _open_species(species_id)
        select_tab = (callbacks or {}).get("species_select_tab")
        if select_tab is not None:
            select_tab("picks")

    manage_species = _manage_species if _open_species is not None else None
    # `full viewer ↗` (11-S6): the slim mount's route into the full-page pick viewer on
    # the active species + this tomogram. None on a standalone journey mount.
    _open_viewer = (callbacks or {}).get("open_pick_viewer")
    _, ts_names0 = collect_dashboard_journey(state, project_path)
    selected = {"ts": ts_names0[0] if ts_names0 else None}

    # Per-mount auto-kick dedup. Cleared on build so a reload after the user
    # fixed a stuck job re-triggers generation.
    reset_auto_kick_state()
    ensure_assets_loaded()

    container.clear()
    with container:
        # Column layout: heatmap status strip (the per-TS nav) on top, the
        # selected TS's detail pane below. Replaces the old 300px left sidebar.
        strip_container = ui.element("div").classes("cb-strip")
        # R2: per-panel visibility toggle row (which detail sections render).
        # Populated below once render_main exists; persists across TS + projects.
        panel_toggle_container = ui.element("div").classes("cb-panel-toggle-row")
        main_area = (
            ui.element("div")
            .classes("cb-main")
            .style("flex: 1 1 0; min-height: 0; overflow-y: auto; overflow-x: hidden;")
        )

    _strip_sig: dict[str, object] = {"sig": None}
    _main_sig: dict[str, object] = {"sig": None}
    col_els: dict[str, object] = {}
    _sel_gen = {"n": 0}

    def _registry_sig() -> tuple:
        # Every migrated section (fs-motion, alignment, ts-ctf, tilt-filter,
        # denoise path) reads the TiltSeriesRegistry; its index.json mtime moves
        # whenever a driver ingests outputs or the filter re-stamps verdicts —
        # exactly the events that must rebuild the pane. One stat per tick.
        from services.tilt_series import get_registry_for

        try:
            return ("registry", int(get_registry_for(project_path).index_path.stat().st_mtime))
        except Exception:
            return ("registry", 0)

    def _main_signature(journey_data=None, species_data=None) -> tuple:
        # FingerprintedView discipline for the main pane (mirrors render_strip).
        # The 4 s live timer fires refresh_all on every background-task tick, but
        # rebuilding the pane tears down its Plotly charts (and WebGL contexts) —
        # so gate on the SELECTED ts's fingerprint: an unrelated-ts task moving
        # must not twitch the pane. Landed artifacts (previews / slabs / manifests)
        # are NOT fingerprinted here — each auto-kick's on_complete calls
        # request_refresh(force_main=True), which bypasses this gate.
        ts = selected["ts"]
        if ts is None:
            return ("__no_ts__",)
        if journey_data is None:
            journey_data = collect_dashboard_journey(state, project_path)
        journey, _ts_names = journey_data
        # Only the SELECTED ts is fingerprinted below, so collect only its rows: a
        # de-novo species' subtomo_status is derived per pick list and costs a few
        # stats each (11-S3), and this runs on every refresh — no reason to pay it for
        # the other 39 tomograms. When `refresh_all` already collected UNFILTERED for the
        # strip, slice this TS's rows out of THAT rather than paying a second pass:
        # `only_ts` is a pure row filter (`services/dashboard_data.py`), so the results are
        # identical. The strip's own collect (unfiltered) draws them all.
        if species_data is not None:
            species_journey = {ts: species_data.get(ts, [])}
        else:
            species_journey = collect_species_journey(state, project_path, only_ts=ts)
        # The journey sig only covers the 4 prep pill stages + per-species picks;
        # sections like tilt_filter / dataset read job state it never sees. Fold in
        # every job's execution_status (section-agnostic, cheap in-memory scan —
        # catches a re-run's running→succeeded transition) plus the tilt-filter
        # output mtimes (the one section with a job-less standalone path).
        job_states = tuple(
            (iid, str(getattr(jm, "execution_status", ""))) for iid, jm in sorted((state.jobs or {}).items())
        )
        # Per-TS pick-list facts the pane renders (rail table rows). Deliberately
        # EXCLUDES filtered_count (in-pane keep/drop commits) — and NOT the coarse
        # registry_rev — so a keep/drop burst never tears the pane down (roadmap 08 §1).
        pick_lists_sig = tuple(
            sorted(
                (pl.species_id, pl.slug, pl.count, str(pl.path), str(pl.extracted_at))
                for pl in (state.pick_lists or [])
                if pl.tomo_name == ts
            )
        )
        # The rail's authoritative ◉ became display-only in 11-S3, so nothing repaints it
        # in place any more — it HAS to move this fingerprint, or setting it on the
        # Species page leaves the Journey lighting the old row, which is the exact drift
        # the manage/look split exists to prevent. (It was excluded while the Journey
        # owned the click, to keep a radio click from tearing the pane down.) In-memory
        # dict lookups per species — no disk.
        auth_sig = tuple(sorted((s.id, state.get_authoritative_slug(s.id, ts)) for s in state.species_registry))
        return (
            ts,
            journey_signature(journey, species_journey, [ts]),
            job_states,
            _registry_sig(),
            tuple(sorted(_hidden_dashboard_panels())),
            # A fresh species with no rows yet must still surface as a species tab.
            state.species_identity(),
            pick_lists_sig,
            auth_sig,
        )

    def render_main(force: bool = False, *, journey_data=None, species_data=None) -> None:
        # Signature-gated: skip the teardown+rebuild when nothing the selected
        # pane shows changed. `force=True` (artifact completions, selection
        # change, user toggles) always rebuilds. See _main_signature.
        sig = _main_signature(journey_data, species_data)
        if not force and _main_sig["sig"] is not None and sig == _main_sig["sig"]:
            return
        _main_sig["sig"] = sig
        main_area.clear()
        with main_area:
            if selected["ts"] is None:
                _render_no_data_empty_state()
            else:
                _render_main_pane_for_ts(
                    selected["ts"],
                    state,
                    project_path,
                    request_refresh,
                    render_strip,
                    manage_species,
                    open_viewer=_open_viewer,
                )

    def render_strip(journey_data=None, species_data=None) -> None:
        # Signature-gated (FingerprintedView discipline): the 4 s live timer
        # calls refresh_all on every background-task tick, but we only rebuild
        # when the journey data OR the selection actually changed — otherwise a
        # tick mid-click would tear down the column under the click.
        # `refresh_all` hoists both collects so one pass pays for them once; a bare call
        # (selection change, exclude toggle, the pane's own callback) collects here.
        if journey_data is None:
            journey_data = collect_dashboard_journey(state, project_path)
        if species_data is None:
            species_data = collect_species_journey(state, project_path)
        journey, ts_names = journey_data
        species_journey = species_data
        try:
            from services.tilt_series import get_registry_for

            excluded_ids = set(get_registry_for(project_path).excluded_ids())
        except Exception:
            excluded_ids = set()
        # Fold the exclusion set into the signature so toggling a TS forces a
        # rebuild (the journey data itself doesn't change until the next run).
        sig = (journey_signature(journey, species_journey, ts_names), selected["ts"], frozenset(excluded_ids))
        if col_els and sig == _strip_sig["sig"]:
            return
        _strip_sig["sig"] = sig
        recon_mrc = recon_mrc_map(state, project_path)
        col_els.clear()
        col_els.update(
            build_strip(
                strip_container,
                journey=journey,
                species_journey=species_journey,
                ts_names=ts_names,
                selected_ts=selected["ts"],
                recon_mrc_map=recon_mrc,
                on_select=select_ts,
                info_popover=_render_ts_info_popover,
                excluded_ids=excluded_ids,
                on_toggle_exclude=toggle_exclude,
            )
        )

    async def toggle_exclude(ts: str) -> None:
        # Forward-only mute: flip the registry flag + persist. The effect (drivers
        # pre-skip this TS) lands on the next run; the strip mutes it immediately.
        # Guard against emptying the set — excluding the last active TS would
        # hand every downstream supervisor an empty input and deadlock the run.
        from services.tilt_series import get_registry_for

        reg = get_registry_for(project_path)
        if not reg.has_tilt_series(ts):
            ui.notify(f"{ts} isn't in the tilt-series registry — reload the project first.", type="warning")
            return
        currently = reg.get_tilt_series(ts).is_excluded
        if not currently:
            active = [t for t in reg.tilt_series_ids() if not reg.get_tilt_series(t).is_excluded]
            if len(active) <= 1:
                ui.notify("Can't exclude the last remaining tilt-series.", type="warning")
                return
        reg.set_excluded(ts, not currently, reason="excluded via dashboard" if not currently else None)
        await reg.save_async()
        ui.notify(
            f"{ts}: {'excluded — will be skipped on the next run' if not currently else 'restored to processing'}",
            type="info",
        )
        render_strip()

    async def select_ts(ts: str) -> None:
        # Instant feedback: move the column highlight + paint a spinner now and
        # flush to the client, THEN run the heavy per-TS render. The render is
        # still synchronous (it builds NiceGUI elements), but the CSS spinner
        # animates client-side while it runs, so the click feels instant. A
        # generation guard drops a stale render if the user clicks another column
        # during the flush. The strip rebuild runs LAST so any run_javascript in
        # the detail render fires while the clicked column is still alive (the
        # select-ordering pitfall), and it picks up the new selection + filtered.
        if selected["ts"] == ts:
            return
        prev = selected["ts"]
        selected["ts"] = ts
        _sel_gen["n"] += 1
        mine = _sel_gen["n"]
        if prev in col_els:
            col_els[prev].classes(remove="selected")
        if ts in col_els:
            col_els[ts].classes(add="selected")
        main_area.clear()
        with main_area, ui.element("div").classes("cb-empty"):
            ui.spinner(size="lg", color="indigo")
            ui.label("Loading…").classes("text-xs")
        await asyncio.sleep(0.02)
        if mine != _sel_gen["n"]:
            return  # superseded by a newer click during the flush
        render_main(force=True)  # selection changed — always rebuild
        render_strip()

    def refresh_all(force_main: bool = False) -> None:
        # ONE collect per pass. `render_strip` needs both unfiltered and `_main_signature`
        # needs the same journey plus one TS's species rows, and NEITHER collector memoizes
        # internally — so this ran the whole thing twice on every 4 s tick and on every
        # keep/drop Save (`refresh_roster`), which on a fully-extracted de-novo project is
        # the per-list extraction stats of every tomogram, doubled.
        journey_data = collect_dashboard_journey(state, project_path)
        species_data = collect_species_journey(state, project_path)
        render_strip(journey_data, species_data)
        render_main(force=force_main, journey_data=journey_data, species_data=species_data)

    # P1: coalesce the initial refresh storm. On load several background auto-kicks
    # (preview / IMOD / recon-slabs / coords-ingest / list-cutouts) each fire
    # on_complete → refresh, and the 4 s live tick adds more — N immediate full
    # rebuilds make the page "jitter a few times on load". Those paths call
    # request_refresh() to raise a flag instead; the coalesce timer (set up with the
    # live timer below) flushes ONE trailing-edge rebuild once requests go quiet. The
    # first paint just below still rebuilds immediately.
    _refresh_req = {"pending": False, "quiet": 0, "force_main": False}

    def request_refresh(force_main: bool = True) -> None:
        # force_main=True (the default — auto-kick on_complete handlers and user
        # actions) rebuilds the main pane unconditionally: an artifact landed or
        # the user acted. The 4 s live timer passes force_main=False so an
        # unrelated-ts task tick lets render_main self-gate (no Plotly teardown).
        _refresh_req["pending"] = True
        if force_main:
            _refresh_req["force_main"] = True
        _refresh_req["quiet"] = 0

    _build_panel_toggle_row(panel_toggle_container, render_main)
    refresh_all()

    # Live refresh while the journey is the active view: re-render every 4 s if
    # any background task is in flight for this project. Keeps the strip /
    # section cards in sync with manifests being written by an async preview-
    # render or IMOD-gen task. Skip the rebuild when nothing's running to avoid
    # burning the event loop on idle dashboards.
    from services.background_tasks import get_background_task_registry

    _last_signature = {"sig": None}
    _active = {"on": True}

    async def _maybe_refresh() -> None:
        if not _active["on"]:
            return
        try:
            registry = get_background_task_registry()
            proj_tasks = registry.for_project(str(project_path))
            active = [t for t in proj_tasks if t.is_running]
            # Running-task MEMBERSHIP only — deliberately NOT progress_current/total.
            # The main pane shows "generating…" placeholders + final results, never a
            # progress bar (that lives in the tray), so a task START (id appears here)
            # and FINISH (id leaves here → enters `finished` below) each warrant one
            # rebuild — but a per-tick PROGRESS update must NOT, or every tick of a
            # long preview/cutout job tears down and rebuilds the whole pane (the
            # "jittery every few seconds" bug). CLAUDE.md: polls observe state, they
            # don't rebuild the DOM on every tick.
            running = tuple(sorted(t.id for t in active))
            # Recently-finished tasks so the dashboard picks up the final manifest
            # write within one refresh window.
            finished = tuple(
                (t.id, t.status)
                for t in proj_tasks
                if not t.is_running and t.finished_at and (t.finished_at - t.started_at).total_seconds() < 86400
            )
            # No curation-session liveness here since 09-S2: the Journey no longer starts
            # or swaps an ArtiaX session, so it neither shows session state nor pays for a
            # `squeue`. The one surface that does is the Particles registry's Picks &
            # curation tab, which polls the shared `ui/particles/session_status` cache.
            #
            # registry_rev: coarse in-memory counter of species / pick-list / template
            # mutations (roadmap 08 §1). It only WAKES this gate; the strip / main
            # sigs below are precise (species_identity, per-TS pick-list tuple), so a
            # bump that changes nothing drawn is a no-op rebuild-wise. An ArtiaX save
            # reaches the pane THROUGH it: the server-side CurationWatcher registers the
            # `manual` list (add_pick_list → rev++), so no .coords mtime is folded here.
            sig = (running, finished, state.registry_rev)
            if sig != _last_signature["sig"]:
                prev = _last_signature["sig"]
                _last_signature["sig"] = sig
                # Name what moved, so a lingering rebuild is diagnosable from the log
                # rather than guessed at (the dashboard has no auto-reload).
                if prev is not None:
                    moved = []
                    if running != prev[0]:
                        moved.append("tasks")
                    if finished != prev[1]:
                        moved.append("tasks-done")
                    if sig[2] != prev[2]:
                        moved.append("curation-session")
                    if sig[3] != prev[3]:
                        moved.append("registry")
                    logger.info("journey live-refresh rebuild (changed: %s)", ", ".join(moved) or "unknown")
                request_refresh(force_main=False)  # timer tick — let render_main self-gate
        except RuntimeError:
            # Client gone — timer will clean up shortly.
            pass

    live_timer = ui.timer(4.0, _maybe_refresh)

    def _flush_refresh() -> None:
        # Trailing-edge flush of the coalesced refresh (P1): rebuild once the request
        # flag has been quiet for ~one tick, so a burst of auto-kick completions
        # collapses into a single rebuild instead of N. Idle cost is one bool check.
        if not _refresh_req["pending"]:
            return
        _refresh_req["quiet"] += 1
        if _refresh_req["quiet"] >= 2:
            fm = bool(_refresh_req["force_main"])
            _refresh_req["pending"] = False
            _refresh_req["quiet"] = 0
            _refresh_req["force_main"] = False
            refresh_all(force_main=fm)

    refresh_coalesce_timer = ui.timer(0.2, _flush_refresh)

    def _set_journey_active(on: bool) -> None:
        # Pause the 4 s live-refresh (its signature does per-tick disk I/O in
        # render_strip) whenever the journey isn't the visible view; resume when
        # it is. Driven by the workspace's _switch_to.
        _active["on"] = on
        try:
            if on:
                live_timer.activate()
                refresh_coalesce_timer.activate()
            else:
                live_timer.deactivate()
                refresh_coalesce_timer.deactivate()
        except Exception:
            pass

    async def _show_ts(ts: str, section: str | None = None) -> None:
        """The Species page's "open in Journey" (roadmap 11-S2): select this
        tilt-series' column and optionally scroll one section card into view. The
        workspace has already switched to (and built) the journey by the time this runs.
        A tomogram with no column is reported, not silently ignored — the strip only
        holds tilt-series the project's array-job manifests describe."""
        if ts and ts not in col_els:
            ui.notify(f"{ts} has no column in the journey strip yet (no processed data for it).", type="warning")
            return
        if ts:
            await select_ts(ts)
        if section:
            _scroll_section_into_view(section)

    if callbacks is not None:
        callbacks["on_journey_active"] = _set_journey_active
        callbacks["journey_show_ts"] = _show_ts


def _render_no_data_empty_state() -> None:
    """Shown in the main pane when the project has no array-job manifests
    (so the sidebar is empty too). The dashboard still opens — this gives
    the user a stable place to land when the project hasn't been run yet."""
    with ui.element("div").classes("cb-empty"):
        ui.icon("hourglass_empty", size="48px").classes("text-gray-400")
        ui.label("No tilt-series data yet.").classes("text-sm text-gray-500")
        ui.label(
            "Run any of the array jobs (FS Motion/CTF, Alignment, CTF, Reconstruct)"
            " to populate the sidebar with per-TS rows."
        ).classes("text-[11px] italic text-gray-400 text-center").style("max-width: 460px;")


def _scroll_section_into_view(section_key: str) -> None:
    """Scroll the first section card whose data-section matches `section_key`
    into view inside the main pane. Runs after the DOM settles."""
    ui.run_javascript(
        "setTimeout(function(){"
        f"  const el = document.querySelector('.cb-section-card[data-section={section_key!r}]');"
        "  if (el) el.scrollIntoView({behavior: 'smooth', block: 'start'});"
        "}, 120);"
    )


def _info_copy_row(key: str, value: str, copy_value: str | None = None) -> None:
    """One key/value line in the info popover with a copy-full-value button."""
    cv = copy_value if copy_value is not None else value
    with ui.element("div").classes("cb-info-row"):
        ui.label(key).classes("cb-info-key")
        ui.label(value).classes("cb-info-val").tooltip(cv)
        (
            ui.button(
                icon="content_copy",
                on_click=lambda v=cv: (ui.clipboard.write(v), ui.notify("Copied path", type="positive", timeout=800)),
            )
            .props("flat dense round size=sm")
            .classes("cb-info-copy")
            .tooltip("Copy full path to clipboard")
        )


def _ts_meta_line(species_list: list[dict]) -> str | None:
    """Pixel size · dims line for the info popover, from the first species
    whose manifest entry carries them."""
    for sp in species_list:
        px = sp.get("pixel_size_ang")
        dims = sp.get("tomo_dims")
        if px or dims:
            bits = []
            if px:
                bits.append(f"{px:.2f} Å/px")
            if dims:
                bits.append("×".join(str(int(d)) for d in dims))
            return " · ".join(bits)
    return None


def _render_ts_info_popover(ts_name: str, species_list: list[dict], recon_mrc: str | None) -> None:
    """The ⓘ button → click popover: full tomo name, metadata, and the relevant
    file paths (each with a copy-full-path button). Click-opened so the copy
    buttons are actually usable (a hover tooltip dismisses as you reach them)."""
    btn = ui.button(icon="info_outline").props("flat dense round size=sm").classes("cb-ts-info-btn")
    # Stop the click bubbling to the row so opening info doesn't also switch TS.
    btn.on("click.stop", lambda: None)
    with btn, ui.menu().props("anchor='bottom right' self='top right'"):
        with ui.element("div").classes("cb-info-card"):
            _info_copy_row("tomogram", ts_name, ts_name)
            meta = _ts_meta_line(species_list)
            if meta:
                ui.label(meta).classes("cb-info-meta")
            if recon_mrc:
                _info_copy_row("recon", recon_mrc, recon_mrc)
            for sp in species_list:
                if sp.get("ce_star"):
                    _info_copy_row(f"{sp['label']} picks", sp["ce_star"], sp["ce_star"])
                if sp.get("subtomo_star"):
                    _info_copy_row(f"{sp['label']} subtomo", sp["subtomo_star"], sp["subtomo_star"])


# ---------------------------------------------------------------------------
# Main pane orchestrator: per-TS section stack
# ---------------------------------------------------------------------------


def _render_main_pane_for_ts(
    ts_name: str,
    project_state,
    project_path: Path,
    refresh,
    refresh_roster=None,
    manage_species=None,
    *,
    open_viewer=None,
) -> None:
    """Render the main-pane section stack for the selected TS. Sections emit
    in pipeline order; each is a no-op if the corresponding job isn't in the
    pipeline (per ROADMAP §2.1 contract). `refresh_roster` is a sidebar-only
    refresh the gallery calls after save/discard so the roster review column
    updates without rebuilding the main pane (which would reset the active tab).
    `manage_species(species_id)` is the workspace's route to the Species page's Picks
    tab — threaded (not stashed module-level) because it closes over ONE client's page."""
    rendered_any = False
    hidden = _hidden_dashboard_panels()

    # Project-wide / per-TS analytics — primitive datadumps for now (Slice C).
    # Each panel is gated on the user's visibility pref (R2); keys match
    # _DASHBOARD_PANEL_KEYS so the Panels toggle row drives what renders here.
    section_emitters = (
        ("dataset", _render_dataset_section),
        ("fs_ctf", _render_fs_motion_ctf_section),
        ("tilt_filter", _render_tilt_filter_section),
        ("ts_align", _render_ts_alignment_section),
        ("ts_ctf", _render_ts_ctf_section),
        ("tilt_qc", _render_tilt_qc_section),
        ("reconstruct", _render_reconstruct_section),
    )
    for key, emit in section_emitters:
        if key in hidden:
            continue
        if emit(ts_name, project_state, project_path, refresh):
            rendered_any = True

    # Particles — one unified section: a shared tomogram canvas with every
    # species' picks overlaid (toggleable), plus a per-species tab carrying
    # that species' TM sanity strip + gallery / scatter. Replaces both the
    # old per-species Template Match cards and the candidate-extract cards.
    if "particles" not in hidden:
        # Candidate-extract path first; if it renders nothing (no TEMPLATE_EXTRACT
        # jobs — e.g. a particle-only project), fall back to the imported-tomogram
        # manual-picking section so the imported tomos still surface.
        if render_particles_section(
            ts_name,
            project_state,
            project_path,
            refresh,
            refresh_roster,
            manage_species,
            open_viewer=open_viewer,
        ):
            rendered_any = True
        elif render_imported_particles_section(ts_name, project_state, project_path, refresh, refresh_roster):
            rendered_any = True

    if not rendered_any:
        if len(hidden) >= len(_DASHBOARD_PANEL_KEYS):
            with ui.element("div").classes("cb-empty"):
                ui.icon("visibility_off", size="36px").classes("text-gray-400")
                ui.label("All panels hidden.").classes("text-xs")
                ui.label("Re-enable sections in the Panels row above.").classes("text-[11px] italic text-center").style(
                    "max-width: 420px;"
                )
        else:
            with ui.element("div").classes("cb-empty"):
                ui.icon("hourglass_empty", size="36px")
                ui.label(f"No section data yet for {ts_name}.").classes("text-xs")
                ui.label("Section cards appear once the matching pipeline jobs have run.").classes(
                    "text-[11px] italic text-center"
                ).style("max-width: 420px;")


# ---------------------------------------------------------------------------
# Slice C analytics sections — primitive key/value datadumps (no new manifests
# needed; read directly from job_model.* + the stage's output star)
# ---------------------------------------------------------------------------


def _render_datadump_card(
    section_key: str,
    icon: str,
    title: str,
    metric_strip: str,
    instance_id: str | None,
    job_status_label: str | None,
    rows: list[tuple[str, str]],
    note: str | None = None,
) -> None:
    """Slice-C primitive section card: header + 1-line metric strip + key/value
    grid. Reused by every analytics emitter."""
    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = section_key
        if instance_id:
            card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon(icon, size="14px").classes("text-indigo-600")
            ui.label(title).classes("cb-section-title")
            if instance_id:
                ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            if metric_strip:
                ui.label(metric_strip).classes("cb-metric-strip")
            if job_status_label and job_status_label.lower() != "succeeded":
                ui.label(job_status_label).classes("text-[10px] text-amber-600 font-mono")
        if rows:
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label("—" if v is None or v == "" else str(v)).classes("cb-datadump-val")
        if note:
            ui.label(note).classes("cb-section-placeholder")


# ---------------------------------------------------------------------------
# tmResults *_job.json reader — surfaces what PyTOM actually applied per TS
# (vs. what the user declared in project_params.json)
# ---------------------------------------------------------------------------


def _read_tomohand_from_import_star(star_path: Path) -> int | None:
    """Return `_rlnTomoHand` from an Import-job tilt_series.star, sampling the
    first data table that carries it. Returns ±1 or None on absence."""
    if not star_path.exists():
        return None
    try:
        import starfile

        data = starfile.read(star_path, always_dict=True)
    except Exception as e:
        logger.warning("Could not read TomoHand from %s: %s", star_path, e)
        return None
    for v in data.values():
        if isinstance(v, pd.DataFrame) and "rlnTomoHand" in v.columns:
            vals = pd.to_numeric(v["rlnTomoHand"], errors="coerce").dropna().unique().tolist()
            if not vals:
                continue
            # Mixed values across TS are unusual but possible — surface +1/-1
            # as a magnitude (sign of the first) when uniform, else 0 sentinel.
            uniq = sorted({round(x) for x in vals})
            if len(uniq) == 1:
                return int(uniq[0])
            return 0  # mixed
    return None


def _find_import_job(project_state) -> tuple[str, object] | None:
    """Locate the Import (relion.importtomo) job. The dataset chip needs it
    to cross-check the in-memory `invert_defocus_hand` against the actual
    `_rlnTomoHand` Import wrote into `tilt_series.star`."""
    return find_job_by_type(project_state, JobType.IMPORT_MOVIES)


def _render_stage0_chips(project_state, project_path: Path) -> None:
    """Per-project chips that summarize import-time/microscope choices that
    silently change downstream science. Currently: TomoHand (chirality /
    depth-dependent defocus sign). Empty container if nothing to show."""
    from services.models_base import AcquisitionParams

    acq = project_state.acquisition
    # In-memory intention. invert_defocus_hand=True → flip_tiltseries_hand=Yes
    # → TomoHand=-1; False → TomoHand=+1. Source: ImportMoviesParams._get_job_specific_options.
    config_hand = -1 if bool(acq.invert_defocus_hand) else 1

    # On-disk: read tilt_series.star from the Import job, if it exists.
    disk_hand: int | None = None
    imp = _find_import_job(project_state)
    if imp:
        imp_dir = job_dir_for(project_state, imp[0], imp[1], project_path)
        if imp_dir:
            disk_hand = _read_tomohand_from_import_star(imp_dir / "tilt_series.star")

    # Stale-default: compare to the AcquisitionParams field default. Catches
    # projects created before the 2026-05-16 invert_defocus_hand=True flip.
    default_hand = -1 if bool(AcquisitionParams.model_fields["invert_defocus_hand"].default) else 1

    # Status logic:
    #   error  — disk_hand exists and disagrees with config_hand (Import was run
    #            with one setting, the project then edited it; downstream jobs
    #            will use the disk value but the user thinks otherwise)
    #   warn   — config differs from the current code default (drift candidate)
    #   warn   — disk_hand == 0 (mixed across tilt-series; rare, usually
    #            indicates a double-imported project)
    #   ok     — everything aligned
    if disk_hand == 0:
        status = "warn"
        value = "mixed"
        tooltip = (
            "Different `_rlnTomoHand` values across tilt-series in this project's "
            "Import output. Usually means the project was double-imported with "
            "different invert_defocus_hand settings. Verify by re-importing."
        )
    elif disk_hand is not None and disk_hand != config_hand:
        status = "error"
        value = f"{disk_hand:+d}"
        tooltip = (
            f"Import wrote _rlnTomoHand={disk_hand:+d} into tilt_series.star "
            f"(this is what downstream jobs will use), but project_params.json now "
            f"declares invert_defocus_hand={acq.invert_defocus_hand} → expected "
            f"{config_hand:+d}. Re-run Import to align the two, or revert the config."
        )
    elif config_hand != default_hand:
        status = "warn"
        value = f"{config_hand:+d}"
        tooltip = (
            f"_rlnTomoHand={config_hand:+d} (from invert_defocus_hand="
            f"{acq.invert_defocus_hand}). Current code default would give "
            f"{default_hand:+d} — verify this project's value is intentional. "
            f"_rlnTomoHand is the sign convention for depth-dependent defocus in "
            f"RELION's CTF correction (see HANDOFF_412_DEBUG.md)."
        )
    else:
        status = "ok"
        value = f"{config_hand:+d}"
        src = "from Import output" if disk_hand is not None else "from acquisition config"
        tooltip = (
            f"_rlnTomoHand={config_hand:+d} ({src}). Sign convention for "
            f"depth-dependent defocus in CTF correction. The Klumpe-lab Titan "
            f"convention is -1 (invert_defocus_hand=True)."
        )

    # Third authority (registry): the hand Warp ACTUALLY applied (ts_defocus_hand,
    # recorded by the tsCtf ingest). The Import star is the declared intention;
    # this is what the data got — disagreement is a real chirality finding.
    warp_hand = warp_hand_from_registry(project_state, project_path)
    if warp_hand == 0:
        if status == "ok":
            status = "warn"
        tooltip += " Registry: Warp applied DIFFERENT hands across tilt-series (mixed runs?) — verify per-TS."
    elif warp_hand is not None:
        if warp_hand != config_hand and status != "error":
            status = "error"
            value = f"{warp_hand:+d}"
            tooltip += (
                f" Registry: Warp applied ts_defocus_hand {warp_hand:+d} — this DISAGREES with the "
                f"declared {config_hand:+d}; downstream defocus signs came from Warp's value."
            )
        else:
            tooltip += f" Registry: Warp applied ts_defocus_hand {warp_hand:+d}."

    with ui.element("div").classes("cb-chip-strip"):
        render_chip("TomoHand", value, status=status, tooltip=tooltip, icon="compare_arrows")


def _render_dataset_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    """Project-wide acquisition + microscope settings + pixel/binning sanity table.
    Two-column key/val grid above; below it, a dense per-stage table showing
    pixel size, tomo dims, and template/extract/subtomo box + padding with
    inline sanity-rule warnings (ROADMAP §11). A chip strip at the top
    surfaces import-time choices that silently change downstream science
    (TomoHand, etc.)."""
    ms = project_state.microscope
    acq = project_state.acquisition

    metric_parts = [f"{ms.pixel_size_angstrom:g} Å/px", f"{int(ms.acceleration_voltage_kv)} kV"]
    if acq.dose_per_tilt:
        metric_parts.append(f"{acq.dose_per_tilt:g} e⁻/Å²/tilt")
    if acq.tilt_axis_degrees is not None:
        metric_parts.append(f"axis {acq.tilt_axis_degrees:g}°")
    if project_state.import_total_tilt_series:
        metric_parts.append(f"{project_state.import_selected_tilt_series}/{project_state.import_total_tilt_series} TS")

    rows: list[tuple[str, str]] = [
        ("microscope", getattr(ms.microscope_type, "value", str(ms.microscope_type))),
        ("voltage", f"{ms.acceleration_voltage_kv:g} kV"),
        ("Cs", f"{ms.spherical_aberration_mm:g} mm"),
        ("ampl. contrast", f"{ms.amplitude_contrast:g}"),
        ("pixel size", f"{ms.pixel_size_angstrom:g} Å"),
        ("dose / tilt", f"{acq.dose_per_tilt:g} e⁻/Å²" if acq.dose_per_tilt else "—"),
        ("tilt axis", f"{acq.tilt_axis_degrees:g}°"),
        ("detector", f"{acq.detector_dimensions[0]}×{acq.detector_dimensions[1]} px"),
        ("acq. software", acq.acquisition_software or "—"),
        ("sample thickness", f"{acq.sample_thickness_nm:g} nm" if acq.sample_thickness_nm else "—"),
    ]
    if acq.eer_fractions_per_frame:
        rows.append(("EER fractions/frame", str(acq.eer_fractions_per_frame)))
    if acq.invert_tilt_angles:
        rows.append(("invert tilts", "yes"))
    if acq.invert_defocus_hand:
        rows.append(("invert defocus hand", "yes"))

    pixel_rows = compute_pixel_chain(project_state)
    apply_sanity_rules(pixel_rows)

    collapsed = _dataset_collapsed()
    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "dataset"
        # R3: clickable header toggles the body. Collapsed (default) = just the
        # header + metric strip; expanded reveals the chips, key/val grid, and
        # the pixel/binning sanity table. State persists across projects + TS.
        header = ui.element("div").classes("cb-section-card-header cb-collapsible-header")
        with header:
            ui.icon("memory", size="14px").classes("text-indigo-600")
            ui.label("Dataset").classes("cb-section-title")
            ui.space()
            ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            caret = ui.icon("expand_less", size="18px").classes("cb-collapse-caret")
        if collapsed:
            caret.classes(add="rot")
        body = ui.element("div").classes("cb-collapsible-body")
        if collapsed:
            body.classes(add="cb-collapsed")
        with body:
            _render_stage0_chips(project_state, project_path)
            # 2-col grid: pairs of (key, val, key, val) per visual row.
            with ui.element("div").classes("cb-datadump-grid-2col"):
                for k, v in rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label("—" if v is None or v == "" else str(v)).classes("cb-datadump-val")
            render_pixel_sanity_table(pixel_rows)

        def _toggle_dataset(_=None, _body=body, _caret=caret) -> None:
            svc = get_prefs_service()
            now_collapsed = not svc.prefs.dashboard_dataset_collapsed
            svc.prefs.dashboard_dataset_collapsed = now_collapsed
            _save_dashboard_prefs()
            if now_collapsed:
                _body.classes(add="cb-collapsed")
                _caret.classes(add="rot")
            else:
                _body.classes(remove="cb-collapsed")
                _caret.classes(remove="rot")

        header.on("click", _toggle_dataset)

    return True


# --- Plot helpers that run inside an existing card body ----------------------


_DEFOCUS_U_COLOR = "#4338ca"  # indigo-700
_DEFOCUS_V_COLOR = "#a855f7"  # purple-500
_CTF_RES_COLOR = "#059669"  # emerald-600
_CTF_FOM_COLOR = "#0ea5e9"  # sky-500
_MOTION_TOTAL_COLOR = "#d97706"  # amber-600
_MOTION_EARLY_COLOR = "#fb923c"  # orange-400
_MOTION_LATE_COLOR = "#ea580c"  # orange-600
_X_SHIFT_COLOR = "#0891b2"  # cyan-600
_Y_SHIFT_COLOR = "#7c3aed"  # violet-600


def _stat_strip(rows: list[tuple[str, str]]) -> None:
    """Tiny inline stats line: `key val · key val ...`"""
    if not rows:
        return
    with ui.element("div").classes("cb-stat-strip"):
        for k, v in rows:
            with ui.element("span"):
                ui.html(f"<span class='cb-stat-key'>{k}</span><span class='cb-stat-val'>{v}</span>", sanitize=False)


def _plot_cell(label: str, fig: dict, *, height_px: int = 220, wide: bool = False, hint: str | None = None) -> None:
    """One plot tile inside a `.cb-plot-row` parent. `hint` is a short tooltip
    explainer attached to the title (helps newcomers parse the metric).

    Default height bumped to 220px (from 150) to give the markers vertical
    breathing room — at 150 the dots crowd together and small spreads vanish.
    """
    cls = "cb-plot-cell cb-plot-cell-wide" if wide else "cb-plot-cell"
    with ui.element("div").classes(cls):
        with ui.row().classes("items-center gap-1").style("padding: 1px 4px 0;"):
            lbl = ui.label(label).classes("cb-plot-label").style("padding: 0;")
            if hint:
                lbl.tooltip(hint)
                ui.icon("info_outline", size="11px").classes("text-gray-400 cursor-help").tooltip(hint)
        ui.plotly(fig).style(f"width: 100%; height: {height_px}px;")


def _render_registry_gap(ts_name: str, status_label: str, param_rows: list[tuple[str, str]] | None = None) -> None:
    """Loud, honest placeholder when the registry has no data for a job+TS.
    Stage-0 decision: consumers are registry-only — a run that predates registry
    ingest shows this marker and re-earns its dashboard data by re-running;
    there is no silent star fallback."""
    running = status_label.lower() in ("running", "queued", "scheduled")
    if running:
        ui.label("Job is running — per-tilt results land in the registry when it completes.").classes(
            "cb-section-placeholder"
        )
    else:
        ui.label(
            f"No registry data for {ts_name} — this run predates registry ingest. "
            "Re-run the job to populate it (per-tilt stars are no longer read)."
        ).classes("cb-section-placeholder text-amber-700")
    if param_rows:
        with ui.element("div").classes("cb-datadump-grid"):
            for k, v in param_rows:
                ui.label(k).classes("cb-datadump-key")
                ui.label(str(v)).classes("cb-datadump-val")


def _per_tilt_customdata(df: pd.DataFrame) -> list[list]:
    """Build [[tilt_index, frame_basename], ...] customdata so plot hovers
    can name the specific tilt instead of just its angle."""
    n = len(df)
    if "rlnMicrographMovieName" in df.columns:
        bases = [Path(str(v)).name for v in df["rlnMicrographMovieName"].tolist()]
    else:
        bases = [""] * n
    return [[i + 1, bases[i]] for i in range(n)]


# One-line explainers attached to plot titles. Defocus / astig / shifts are
# domain-jargon; quick tooltips let newcomers parse the dashboard without
# leaving the page.
_HINT_DEFOCUS = (
    "Per-tilt astigmatic CTF defocus. Defocus U = long-axis (more underfocus), "
    "V = short-axis. Mean ≈ (U+V)/2 is the conventionally reported defocus; "
    "spread between U and V is astigmatism."
)
_HINT_ASTIG = (
    "Magnitude of CTF astigmatism (|U − V|, Å). Large astig widens CTF zeros and reduces achievable resolution."
)
_HINT_CTF_RES = "Best resolution (Å) at which CTF zeros could be fit. Lower = better fit / more usable signal."
_HINT_CTF_FOM = "CTF fit figure-of-merit, dimensionless 0..1. Higher = more confident fit."
_HINT_MOTION = (
    "Per-tilt beam-induced motion (WarpTools MeanFrameMovement; units per Warp convention, ≈ Å). "
    "Higher = more drift/charging, and it typically rises toward high tilt. Read from the frameseries XML, "
    "not the star (whose motion columns are placeholders)."
)
_HINT_SHIFT = (
    "Per-tilt translation in Å applied during alignment to register each tilt to a common reference. "
    "A spike means a tilt is hard to align (often: contamination, charging, or ice motion)."
)
_HINT_ALIGN_ANGLES = (
    "Refined per-tilt rotational corrections. X tilt − nominal = how far the refit moved the stage tilt; "
    "Y tilt and Z rot are the secondary tilt-axis and in-plane rotation."
)
_HINT_THROUGHFOCUS = (
    "Mean per-tilt CTF defocus ((U+V)/2) vs stage tilt, sorted by angle — the through-focus curve. "
    "A clean tilt-series traces a smooth trend; scatter or a kink at high tilt flags bad CTF fits. "
    "The dotted line is a linear fit; its slope sign is a handedness cue (ties to the TomoHand / 412 "
    "defocus-sign issue)."
)


def _render_ctf_motion_plots(
    df: pd.DataFrame,
    *,
    show_motion: bool = True,
    ctf_res: list | None = None,
    motion: list | None = None,
    dl_by_frame: dict | None = None,
) -> None:
    """Defocus + astigmatism (always plotted as scatter, since each tilt is an
    independent estimate). CTF max-resolution / FOM / motion are gated on
    `_is_meaningful_series` because WarpTools-exported RELION stars often
    write `1e-6` placeholders for those columns — see
    `project_warp_relion_star_placeholders.md`.

    `ctf_res` / `motion` are the REAL per-tilt series (registry QC fields,
    XML-sourced at ingest) — the star fallback columns only render for
    non-WarpTools exports that populate them for real."""
    tilts = _safe_floats(df["rlnTomoNominalStageTiltAngle"])
    cd = _per_tilt_customdata(df)
    if dl_by_frame:
        # Append the tilt-filter verdict (keep/drop + prob) as customdata[2] so each
        # per-tilt point's hover shows what the tilt-filter thought of that tilt.
        cd = [[*row, dl_by_frame.get(row[1], "—")] for row in cd]

    xml_res: list | None = ctf_res
    xml_motion: list | None = motion

    has_def = "rlnDefocusU" in df.columns and "rlnDefocusV" in df.columns
    has_astig = "rlnCtfAstigmatism" in df.columns
    has_res = "rlnCtfMaxResolution" in df.columns
    has_fom = "rlnCtfFigureOfMerit" in df.columns
    has_motion_total = "rlnAccumMotionTotal" in df.columns

    skipped: list[str] = []

    with ui.element("div").classes("cb-plot-row"):
        if has_def:
            du = [None if v is None else v / 1.0e4 for v in _safe_floats(df["rlnDefocusU"])]
            dv = [None if v is None else v / 1.0e4 for v in _safe_floats(df["rlnDefocusV"])]
            fig = _build_per_tilt_chart(
                tilts,
                [
                    {"name": "Defocus U", "y": du, "color": _DEFOCUS_U_COLOR, "marker_size": 7},
                    {"name": "Defocus V", "y": dv, "color": _DEFOCUS_V_COLOR, "marker_size": 7},
                ],
                y_label="defocus (µm)",
                customdata=cd,
                y_unit=" µm",
                y_range=(0.0, 10.0),
            )
            _plot_cell("Defocus U / V per tilt", fig, hint=_HINT_DEFOCUS)
        if has_astig:
            astig = _safe_floats(df["rlnCtfAstigmatism"])
            if _is_meaningful_series(astig):
                fig = _build_per_tilt_chart(
                    tilts,
                    [{"name": "astig", "y": astig, "color": "#ec4899", "marker_size": 7}],
                    y_label="astigmatism (Å)",
                    customdata=cd,
                    y_unit=" Å",
                    y_range=(0.0, 1500.0),
                )
                _plot_cell("Astigmatism per tilt", fig, hint=_HINT_ASTIG)

    # CTF fit resolution: prefer the real per-tilt CTFResolutionEstimate from the
    # XML; fall back to the star column only for non-WarpTools exports that
    # populate it for real (WarpTools writes a 1e-6 placeholder there).
    res = xml_res if (xml_res is not None and _is_meaningful_series(xml_res)) else None
    if res is None and has_res:
        star_res = _safe_floats(df["rlnCtfMaxResolution"])
        res = star_res if _is_meaningful_series(star_res) else None
    if res is not None:
        with ui.element("div").classes("cb-plot-row"):
            fig = _build_per_tilt_chart(
                tilts,
                [{"name": "CTF fit res", "y": res, "color": _CTF_RES_COLOR, "marker_size": 6}],
                y_label="resolution (Å)",
                customdata=cd,
                y_unit=" Å",
                y_range=(0.0, 30.0),
            )
            _plot_cell("CTF fit resolution per tilt", fig, hint=_HINT_CTF_RES)
    elif has_res:
        skipped.append("CTF max-res")
    if has_fom:
        fom = _safe_floats(df["rlnCtfFigureOfMerit"])
        if _is_meaningful_series(fom, threshold=1e-4):
            with ui.element("div").classes("cb-plot-row"):
                fig = _build_per_tilt_chart(
                    tilts,
                    [{"name": "FOM", "y": fom, "color": _CTF_FOM_COLOR, "marker_size": 6}],
                    y_label="FOM",
                    customdata=cd,
                    y_range=(0.0, 1.0),
                )
                _plot_cell("CTF figure of merit per tilt", fig, hint=_HINT_CTF_FOM)
        else:
            skipped.append("CTF FOM")

    # Motion: prefer the real per-tilt MeanFrameMovement from the XML (single
    # series). Fall back to the star's AccumMotion total/early/late only when
    # those are real (non-WarpTools exports) — WarpTools writes 1e-6 there.
    if show_motion:
        if xml_motion is not None and _is_meaningful_series(xml_motion, threshold=1e-4):
            with ui.element("div").classes("cb-plot-row"):
                series = [{"name": "motion", "y": xml_motion, "color": _MOTION_TOTAL_COLOR, "mode": "lines+markers"}]
                fig = _build_per_tilt_chart(tilts, series, y_label="mean frame motion", customdata=cd)
                _plot_cell("Beam-induced motion per tilt", fig, wide=True, hint=_HINT_MOTION)
        elif has_motion_total and _is_meaningful_series(_safe_floats(df["rlnAccumMotionTotal"]), threshold=0.05):
            mt = _safe_floats(df["rlnAccumMotionTotal"])
            with ui.element("div").classes("cb-plot-row"):
                series = [{"name": "total", "y": mt, "color": _MOTION_TOTAL_COLOR, "mode": "lines+markers"}]
                if "rlnAccumMotionEarly" in df.columns:
                    series.append(
                        {
                            "name": "early",
                            "y": _safe_floats(df["rlnAccumMotionEarly"]),
                            "color": _MOTION_EARLY_COLOR,
                            "dash": "dot",
                            "mode": "lines+markers",
                        }
                    )
                if "rlnAccumMotionLate" in df.columns:
                    series.append(
                        {
                            "name": "late",
                            "y": _safe_floats(df["rlnAccumMotionLate"]),
                            "color": _MOTION_LATE_COLOR,
                            "dash": "dash",
                            "mode": "lines+markers",
                        }
                    )
                fig = _build_per_tilt_chart(tilts, series, y_label="accum. motion (Å)", customdata=cd, y_unit=" Å")
                _plot_cell("Beam-induced motion per tilt", fig, wide=True, hint=_HINT_MOTION)
        else:
            skipped.append("motion")

    if skipped:
        ui.label(
            f"Not shown: {', '.join(skipped)} — no real value in this export "
            "(WarpTools leaves these star columns as placeholders and exports no CTF figure-of-merit)."
        ).classes("cb-section-placeholder")


def _render_alignment_plots(df: pd.DataFrame) -> None:
    """Per-tilt shift magnitude, X/Y/Z angle deltas relative to nominal.

    Markers only — even for smoothly-varying metrics, connecting per-tilt
    estimates with lines turns outliers into zigzag and obscures the actual
    distribution (see `feedback_dashboard_plot_principles`).
    """
    tilts = _safe_floats(df["rlnTomoNominalStageTiltAngle"])
    cd = _per_tilt_customdata(df)
    has_shift = "rlnTomoXShiftAngst" in df.columns and "rlnTomoYShiftAngst" in df.columns
    has_xtilt = "rlnTomoXTilt" in df.columns
    has_ytilt = "rlnTomoYTilt" in df.columns
    has_zrot = "rlnTomoZRot" in df.columns

    with ui.element("div").classes("cb-plot-row"):
        if has_shift:
            xs = _safe_floats(df["rlnTomoXShiftAngst"])
            ys = _safe_floats(df["rlnTomoYShiftAngst"])
            mag = [
                (x * x + y * y) ** 0.5 if x is not None and y is not None else None
                for x, y in zip(xs, ys, strict=False)
            ]
            if _is_meaningful_series(mag):
                fig = _build_per_tilt_chart(
                    tilts,
                    [
                        {"name": "|shift|", "y": mag, "color": "#1d4ed8"},
                        {"name": "X shift", "y": xs, "color": _X_SHIFT_COLOR},
                        {"name": "Y shift", "y": ys, "color": _Y_SHIFT_COLOR},
                    ],
                    y_label="shift (Å)",
                    customdata=cd,
                    y_unit=" Å",
                )
                _plot_cell("Refined shift per tilt", fig, hint=_HINT_SHIFT)
        if has_xtilt or has_ytilt or has_zrot:
            series = []
            if has_xtilt:
                xt = _safe_floats(df["rlnTomoXTilt"])
                resid = [
                    val - nt if nt is not None and val is not None else None
                    for nt, val in zip(tilts, xt, strict=False)
                ]
                if _is_meaningful_series(resid):
                    series.append({"name": "X tilt − nom", "y": resid, "color": "#dc2626"})
            if has_ytilt:
                yt = _safe_floats(df["rlnTomoYTilt"])
                if _is_meaningful_series(yt):
                    series.append({"name": "Y tilt", "y": yt, "color": "#f97316"})
            if has_zrot:
                zr = _safe_floats(df["rlnTomoZRot"])
                if _is_meaningful_series(zr):
                    series.append({"name": "Z rot", "y": zr, "color": "#0ea5e9"})
            if series:
                fig = _build_per_tilt_chart(tilts, series, y_label="angle (°)", customdata=cd, y_unit="°")
                _plot_cell("Refined alignment angles", fig, hint=_HINT_ALIGN_ANGLES)


def _render_fs_motion_ctf_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    found = find_job_by_type(project_state, JobType.FS_MOTION_CTF)
    if not found:
        return False
    instance_id, jm = found
    job_dir = job_dir_for(project_state, instance_id, jm, project_path)
    status_label = getattr(jm.execution_status, "value", str(jm.execution_status))
    metric_parts = [f"motion {jm.m_grid}", f"bfac {jm.m_bfac}", f"ctf {jm.c_range_min_max} Å", f"win {jm.c_window}"]
    param_rows = [
        ("motion range", jm.m_range_min_max),
        ("motion grid", jm.m_grid),
        ("motion bfac", str(jm.m_bfac)),
        ("ctf range", jm.c_range_min_max),
        ("ctf grid", jm.c_grid),
        ("ctf window", str(jm.c_window)),
        ("defocus search", f"{jm.c_defocus_min_max} µm"),
        ("phase shift", "yes" if jm.do_phase else "no"),
        ("avg halves", "yes" if jm.out_average_halves else "no"),
        ("skip first / last", f"{jm.out_skip_first} / {jm.out_skip_last}"),
        ("perdevice", str(jm.perdevice)),
    ]

    if job_dir is None:
        _render_datadump_card(
            "fs_motion_ctf",
            "speed",
            "FS Motion / CTF",
            " · ".join(metric_parts),
            instance_id,
            status_label,
            param_rows,
            note="Job hasn't started — outputs not on disk yet.",
        )
        return True

    df = fsm_registry_df(project_path, instance_id, ts_name)
    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "fs_motion_ctf"
        card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("speed", size="14px").classes("text-indigo-600")
            ui.label("FS Motion / CTF").classes("cb-section-title")
            ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            if status_label.lower() != "succeeded":
                ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        if df is None:
            _render_registry_gap(ts_name, status_label, param_rows)
            return True

        # Real CTF-fit resolution + motion: registry QC fields (XML-sourced at
        # ingest); the star's rlnCtfMaxResolution / rlnAccumMotion* were 1e-6
        # placeholders, which is why these never came from star columns.
        ctf_res_series = _safe_floats(df["cbCtfResolution"])
        motion_series = _safe_floats(df["cbMeanFrameMovement"])
        defocus_um = [v / 1.0e4 for v in _safe_floats(df.get("rlnDefocusU", [])) if v is not None]
        d_stats = _stats(defocus_um)
        r_stats = _stats([v for v in ctf_res_series if v is not None])
        m_stats = _stats([v for v in motion_series if v is not None])
        strip_rows: list[tuple[str, str]] = [("tilts", str(len(df)))]
        if d_stats["n"]:
            strip_rows.append(
                ("defocus", f"{d_stats['median']:.2f} µm (Q1 {d_stats['q1']:.2f} · Q3 {d_stats['q3']:.2f})")
            )
        if r_stats["n"]:
            strip_rows.append(("CTF res", f"{r_stats['median']:.1f} Å (worst {r_stats['max']:.1f})"))
        if m_stats["n"]:
            strip_rows.append(("motion (max)", f"{m_stats['max']:.2f}"))
        _stat_strip(strip_rows)

        _render_ctf_motion_plots(df, show_motion=True, ctf_res=ctf_res_series, motion=motion_series)

        with ui.expansion("Job parameters").classes("w-full text-[10px]").props("dense"):
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
    return True


def _render_ts_alignment_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    found = find_job_by_type(project_state, JobType.TS_ALIGNMENT)
    if not found:
        return False
    instance_id, jm = found
    job_dir = job_dir_for(project_state, instance_id, jm, project_path)
    status_label = getattr(jm.execution_status, "value", str(jm.execution_status))
    method = getattr(jm.alignment_method, "value", str(jm.alignment_method))
    metric_parts = [
        f"{method}",
        f"{jm.rescale_angpixs:g} Å/px",
        jm.tomo_dimensions,
        f"thick {jm.sample_thickness_nm:g} nm",
    ]
    param_rows = [
        ("method", method),
        ("rescale", f"{jm.rescale_angpixs:g} Å/px"),
        ("tomo dims", jm.tomo_dimensions),
        ("sample thickness", f"{jm.sample_thickness_nm:g} nm"),
        ("patch X / Y", f"{jm.patch_x} / {jm.patch_y}"),
        ("axis iter / batch", f"{jm.axis_iter} / {jm.axis_batch}"),
        ("imod patch / overlap", f"{jm.imod_patch_size} / {jm.imod_overlap}"),
        ("perdevice", str(jm.perdevice)),
    ]

    if job_dir is None:
        _render_datadump_card(
            "ts_alignment",
            "straighten",
            "TS Alignment",
            " · ".join(metric_parts),
            instance_id,
            status_label,
            param_rows,
            note="Job hasn't started — outputs not on disk yet.",
        )
        return True

    df = alignment_registry_df(project_path, instance_id, ts_name)
    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "ts_alignment"
        card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("straighten", size="14px").classes("text-indigo-600")
            ui.label("TS Alignment").classes("cb-section-title")
            ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            if status_label.lower() != "succeeded":
                ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        if df is None:
            _render_registry_gap(ts_name, status_label, param_rows)
            return True

        # Stat strip: max shift magnitude + tilt-axis residual range
        x_shift = _safe_floats(df.get("rlnTomoXShiftAngst", [])) if "rlnTomoXShiftAngst" in df.columns else []
        y_shift = _safe_floats(df.get("rlnTomoYShiftAngst", [])) if "rlnTomoYShiftAngst" in df.columns else []
        mag = [
            (x * x + y * y) ** 0.5 for x, y in zip(x_shift, y_shift, strict=False) if x is not None and y is not None
        ]
        m_stats = _stats(mag)
        strip_rows: list[tuple[str, str]] = [("tilts", str(len(df)))]
        if m_stats["n"]:
            strip_rows.append(("|shift| max / median", f"{m_stats['max']:.1f} / {m_stats['median']:.1f} Å"))
        if "rlnTomoYTilt" in df.columns:
            yt = [v for v in _safe_floats(df["rlnTomoYTilt"]) if v is not None]
            if yt:
                yt_stats = _stats(yt)
                strip_rows.append(("Y tilt range", f"{yt_stats['min']:.2f}° → {yt_stats['max']:.2f}°"))
        _stat_strip(strip_rows)

        _render_alignment_plots(df)

        with ui.expansion("Job parameters").classes("w-full text-[10px]").props("dense"):
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
    return True


def _render_ts_ctf_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    found = find_job_by_type(project_state, JobType.TS_CTF)
    if not found:
        return False
    instance_id, jm = found
    job_dir = job_dir_for(project_state, instance_id, jm, project_path)
    status_label = getattr(jm.execution_status, "value", str(jm.execution_status))
    metric_parts = [
        f"defocus {jm.defocus_min_max} µm",
        f"range {jm.range_min_max} Å",
        f"win {jm.window}",
        f"hand {jm.defocus_hand}",
    ]
    param_rows = [
        ("range", f"{jm.range_min_max} Å"),
        ("defocus search", f"{jm.defocus_min_max} µm"),
        ("defocus hand", jm.defocus_hand),
        ("window", str(jm.window)),
        ("phase shift", "yes" if jm.do_phase else "no"),
        ("perdevice", str(jm.perdevice)),
    ]

    if job_dir is None:
        _render_datadump_card(
            "ts_ctf",
            "blur_on",
            "TS CTF (post-alignment)",
            " · ".join(metric_parts),
            instance_id,
            status_label,
            param_rows,
            note="Job hasn't started — outputs not on disk yet.",
        )
        return True

    df = tsctf_registry_df(project_path, instance_id, ts_name)
    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "ts_ctf"
        card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("blur_on", size="14px").classes("text-indigo-600")
            ui.label("TS CTF (post-alignment)").classes("cb-section-title")
            ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            if status_label.lower() != "succeeded":
                ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        if df is None:
            _render_registry_gap(ts_name, status_label, param_rows)
            return True

        defocus_um = [v / 1.0e4 for v in _safe_floats(df.get("rlnDefocusU", [])) if v is not None]
        d_stats = _stats(defocus_um)
        strip_rows: list[tuple[str, str]] = [("tilts", str(len(df)))]
        if d_stats["n"]:
            strip_rows.append(
                ("defocus", f"{d_stats['median']:.2f} µm (range {d_stats['min']:.2f}–{d_stats['max']:.2f})")
            )

        # Tilt-filter per-tilt verdict (keep/drop + DL probability): summarised in the
        # strip and surfaced on each plot point's hover below. Silent no-op if the
        # tilt-filter job hasn't stamped this TS.
        dl_by_frame = filter_verdicts_from_registry(project_path, ts_name)
        if dl_by_frame:
            n_keep = sum(1 for v in dl_by_frame.values() if v.startswith("keep"))
            strip_rows.append(("DL keep", f"{n_keep}/{len(dl_by_frame)}"))
        _stat_strip(strip_rows)

        # Skip the motion plot here — TS CTF doesn't change per-tilt motion;
        # that's already shown in the FS Motion/CTF section above.
        _render_ctf_motion_plots(df, show_motion=False, dl_by_frame=dl_by_frame or None)

        with ui.expansion("Job parameters").classes("w-full text-[10px]").props("dense"):
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
    return True


# --- Tilt QC (through-focus + alignment difficulty) ---------------------------


def _linear_slope_intercept(xs: list, ys: list) -> tuple:
    """OLS slope + intercept over paired (x, y), skipping None entries. Returns
    (slope, intercept) or (None, None) for < 2 points or zero x-variance.
    numpy-free (the dashboard venv has no numpy)."""
    pts = [(x, y) for x, y in zip(xs, ys, strict=False) if x is not None and y is not None]
    n = len(pts)
    if n < 2:
        return None, None
    mx = sum(x for x, _ in pts) / n
    my = sum(y for _, y in pts) / n
    den = sum((x - mx) ** 2 for x, _ in pts)
    if den == 0:
        return None, None
    slope = sum((x - mx) * (y - my) for x, y in pts) / den
    return slope, my - slope * mx


def _defocus_source_df(project_state, project_path: Path, ts_name: str):
    """The per-tilt defocus source — prefer TS CTF (post-alignment, most
    refined), fall back to FS Motion/CTF. Registry reads. Returns
    (df, source_label) or (None, None)."""
    for jt, label, reader in (
        (JobType.TS_CTF, "tsCtf", tsctf_registry_df),
        (JobType.FS_MOTION_CTF, "fsMotion", fsm_registry_df),
    ):
        found = find_job_by_type(project_state, jt)
        if not found:
            continue
        df = reader(project_path, found[0], ts_name)
        if df is not None and "rlnDefocusU" in df.columns:
            return df, label
    return None, None


def _render_tilt_qc_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    """Tomo-native per-tilt QC (roadmap ③): the defocus through-focus curve
    (with a linear-fit handedness slope cue) beside shift-magnitude-vs-tilt
    (per-TS max = the headline alignment-difficulty number). Both read existing
    per-tilt stars — no new plumbing. No-op if neither CTF nor alignment has run
    for this TS."""
    def_df, def_src = _defocus_source_df(project_state, project_path, ts_name)

    align_df = None
    align = find_job_by_type(project_state, JobType.TS_ALIGNMENT)
    if align:
        align_df = alignment_registry_df(project_path, align[0], ts_name)

    # Defocus (µm) mean per tilt, sorted by tilt angle so the through-focus trend
    # reads as a curve (the star is acquisition-ordered).
    def_tilts = def_mean = def_fit = None
    slope = None
    if def_df is not None and "rlnTomoNominalStageTiltAngle" in def_df.columns:
        raw_t = _safe_floats(def_df["rlnTomoNominalStageTiltAngle"])
        du = _safe_floats(def_df["rlnDefocusU"])
        dv = _safe_floats(def_df["rlnDefocusV"]) if "rlnDefocusV" in def_df.columns else du
        mean_um = [
            ((u + v) / 2.0) / 1.0e4 if u is not None and v is not None else None
            for u, v in zip(du, dv, strict=False)
        ]
        pairs = sorted(
            [(t, m) for t, m in zip(raw_t, mean_um, strict=False) if t is not None and m is not None],
            key=lambda p: p[0],
        )
        if pairs:
            def_tilts = [p[0] for p in pairs]
            def_mean = [p[1] for p in pairs]
            slope, intercept = _linear_slope_intercept(def_tilts, def_mean)
            if slope is not None:
                def_fit = [intercept + slope * t for t in def_tilts]

    # Shift magnitude (Å) per tilt (same math as the alignment section; surfaced
    # here as the headline alignment-difficulty number alongside defocus).
    sh_tilts = sh_mag = None
    if align_df is not None and {"rlnTomoXShiftAngst", "rlnTomoYShiftAngst"}.issubset(align_df.columns):
        sh_tilts = _safe_floats(align_df["rlnTomoNominalStageTiltAngle"])
        xs = _safe_floats(align_df["rlnTomoXShiftAngst"])
        ys = _safe_floats(align_df["rlnTomoYShiftAngst"])
        mag = [
            (x * x + y * y) ** 0.5 if x is not None and y is not None else None for x, y in zip(xs, ys, strict=False)
        ]
        sh_mag = mag if _is_meaningful_series(mag) else None

    if def_mean is None and sh_mag is None:
        return False

    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "tilt_qc"
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("insights", size="14px").classes("text-indigo-600")
            ui.label("Tilt QC").classes("cb-section-title")
            ui.space()
            ui.label("through-focus + alignment difficulty").classes("cb-metric-strip")

        strip_rows: list[tuple[str, str]] = []
        if def_mean is not None:
            d_stats = _stats(def_mean)
            strip_rows.append(("defocus median", f"{d_stats['median']:.2f} µm ({def_src})"))
            if slope is not None:
                trend = "rises" if slope > 0 else "falls" if slope < 0 else "flat"
                strip_rows.append(("defocus trend", f"{slope:+.3f} µm/° ({trend} with +tilt)"))
        if sh_mag is not None:
            m_stats = _stats([v for v in sh_mag if v is not None])
            strip_rows.append(("|shift| max / median", f"{m_stats['max']:.1f} / {m_stats['median']:.1f} Å"))
        if strip_rows:
            _stat_strip(strip_rows)

        with ui.element("div").classes("cb-plot-row"):
            if def_mean is not None:
                series = [{"name": "mean defocus", "y": def_mean, "color": _DEFOCUS_U_COLOR, "marker_size": 7}]
                if def_fit is not None:
                    series.append({"name": "trend", "y": def_fit, "color": "#94a3b8", "mode": "lines", "dash": "dot"})
                fig = _build_per_tilt_chart(def_tilts, series, y_label="defocus (µm)", y_unit=" µm")
                _plot_cell("Defocus vs tilt (through-focus)", fig, hint=_HINT_THROUGHFOCUS)
            if sh_mag is not None:
                fig = _build_per_tilt_chart(
                    sh_tilts,
                    [{"name": "|shift|", "y": sh_mag, "color": "#1d4ed8", "marker_size": 7}],
                    y_label="shift (Å)",
                    y_unit=" Å",
                )
                _plot_cell("Shift magnitude vs tilt", fig, hint=_HINT_SHIFT)
    return True


# --- Tilt Filter --------------------------------------------------------------


def _render_tilt_filter_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    """Per-TS tilt-filter diagnostics, from the registry's per-frame verdicts
    (stamped by both the DL and manual filter paths). Renders when a
    TILT_FILTER job exists or when this TS carries stamped verdicts; a run
    that predates verdict stamping shows the registry-gap marker."""
    info = filter_kept_dropped_from_registry(project_path, ts_name)

    job_found = find_job_by_type(project_state, JobType.TILT_FILTER)
    if job_found is None and info is None:
        return False
    instance_id = job_found[0] if job_found else "TiltFilter (standalone)"
    jm = job_found[1] if job_found else None

    metric_parts = []
    if jm is not None:
        metric_parts = [f"model {jm.model_name}", f"thresh {jm.prob_threshold:g}", f"action {jm.prob_action}"]

    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "tilt_filter"
        card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("tune", size="14px").classes("text-indigo-600")
            ui.label("Tilt Filter").classes("cb-section-title")
            ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            if metric_parts:
                ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            if jm is not None:
                status_label = getattr(jm.execution_status, "value", str(jm.execution_status))
                if status_label.lower() != "succeeded":
                    ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        if info is None:
            status_label = getattr(jm.execution_status, "value", str(jm.execution_status)) if jm is not None else ""
            _render_registry_gap(ts_name, status_label)
            return True

        n_labeled = info["n_labeled"]
        n_kept = info["n_kept"]
        n_dropped = len(info["dropped"])
        kept_pct = (100.0 * n_kept / n_labeled) if n_labeled else 0.0
        strip_rows = [("kept", f"{n_kept}/{n_labeled}  ({kept_pct:.0f}%)"), ("dropped", str(n_dropped))]
        if jm is not None:
            strip_rows.append(("manual labels", str(len(jm.tilt_labels))))
        _stat_strip(strip_rows)

        if info["dropped"]:
            with ui.expansion(f"{n_dropped} dropped tilt(s)", value=True).classes("w-full text-[10px]").props("dense"):
                with ui.element("div").classes("cb-drop-list"):
                    for d in info["dropped"]:
                        with ui.element("div").classes("cb-drop-row"):
                            tilt_str = f"{d['tilt_angle']:+.2f}°" if d["tilt_angle"] is not None else "?°"
                            ui.label(tilt_str).classes("cb-drop-tilt")
                            ui.label(d["frame"])

        if jm is not None:
            param_rows = [
                ("model", jm.model_name),
                ("image size", str(jm.image_size)),
                ("dl batch size", str(jm.dl_batch_size)),
                ("prob threshold", f"{jm.prob_threshold:g}"),
                ("prob action", jm.prob_action),
            ]
            with ui.expansion("Filter parameters").classes("w-full text-[10px]").props("dense"):
                with ui.element("div").classes("cb-datadump-grid"):
                    for k, v in param_rows:
                        ui.label(k).classes("cb-datadump-key")
                        ui.label(str(v)).classes("cb-datadump-val")

    return True


# ---------------------------------------------------------------------------
# Reconstruct section: surfaces per-TS reconstructed-tomogram polarity.
#
# The template/mask polarity (BLACK vs WHITE) must match the tomogram
# polarity. If the WarpTools `TomoFullReconstructInvert` setting changes
# between runs or projects, the polarity chip flags the mismatch before
# TM produces meaningless CC scores. Reads a center 1024×1024 Z slice
# only — full-volume reads are forbidden per ROADMAP §4.1.
# ---------------------------------------------------------------------------


_TOMO_POLARITY_CACHE: dict[tuple[str, int], dict] = {}


def _compute_tomogram_polarity(mrc_path: Path) -> dict | None:
    """Sample a center 1024×1024 Z slice from a reconstructed tomogram,
    compute %bright / %dark voxel fractions, classify polarity. Cached by
    (path, mtime). Returns None on read failure or non-3D volumes."""
    try:
        st = mrc_path.stat()
    except OSError:
        return None
    key = (str(mrc_path), int(st.st_mtime))
    cached = _TOMO_POLARITY_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        import mrcfile
        import numpy as np

        with mrcfile.mmap(str(mrc_path), mode="r") as m:
            data = m.data
            if data.ndim != 3:
                return None
            nz, ny, nx = data.shape
            cz, cy, cx = nz // 2, ny // 2, nx // 2
            half = 512
            y0, y1 = max(0, cy - half), min(ny, cy + half)
            x0, x1 = max(0, cx - half), min(nx, cx + half)
            # Materialize a copy so the array survives the mmap close
            # (ROADMAP §4.5 mmap view trap).
            slab = np.array(data[cz, y0:y1, x0:x1], dtype=np.float32, copy=True)
    except Exception as e:
        logger.warning("Could not read tomogram %s for polarity: %s", mrc_path, e)
        return None

    if slab.size == 0:
        return None
    mean = float(slab.mean())
    std = float(slab.std()) or 1.0
    upper = mean + 1.5 * std
    lower = mean - 1.5 * std
    n = float(slab.size)
    pct_bright = 100.0 * float((slab > upper).sum()) / n
    pct_dark = 100.0 * float((slab < lower).sum()) / n

    # Classify. Margins picked from the doc example: GT had 6.6%/6.8% =
    # essentially symmetric. >1.5x ratio + >8% absolute = clear skew.
    if pct_bright > 1.5 * pct_dark and pct_bright > 8.0:
        polarity = "bright"
    elif pct_dark > 1.5 * pct_bright and pct_dark > 8.0:
        polarity = "dark"
    else:
        polarity = "symmetric"

    result = {"pct_bright": pct_bright, "pct_dark": pct_dark, "polarity": polarity, "slab_shape": list(slab.shape)}
    _TOMO_POLARITY_CACHE[key] = result
    return result


def _expected_polarity_from_templates(project_state) -> str | None:
    """Return the consensus selected-template polarity across species
    ("white" or "black") if every species agrees, else None."""
    seen: set[str] = set()
    for sp in project_state.species_registry or []:
        tpl = sp.get_selected_template() if hasattr(sp, "get_selected_template") else None
        pol = getattr(tpl, "polarity", None) if tpl else None
        if pol:
            seen.add(str(pol).lower())
    if len(seen) == 1:
        return next(iter(seen))
    return None


def _tomo_polarity_chip_status(polarity: str, expected: str | None) -> tuple[str, str]:
    """Return (status, hint) for the polarity chip."""
    if expected is None:
        if polarity == "symmetric":
            return "neutral", "Roughly symmetric distribution — typical reconstruction."
        return (
            "info",
            f"{polarity.capitalize()}-skewed reconstruction. Make sure your template polarity "
            f"({polarity}) matches this tomogram.",
        )
    if expected == "white" and polarity == "dark":
        return "error", (
            "Template polarity is WHITE (expects bright particles) but the reconstructed "
            "tomogram is DARK-skewed. PyTOM is matching inverted contrast — invert the "
            "template, the tomogram, or both, and re-run TM."
        )
    if expected == "black" and polarity == "bright":
        return "error", (
            "Template polarity is BLACK (expects dark particles) but the reconstructed "
            "tomogram is BRIGHT-skewed. PyTOM is matching inverted contrast — invert the "
            "template, the tomogram, or both, and re-run TM."
        )
    return "ok", f"Tomogram polarity ({polarity}) matches template polarity ({expected})."


def _render_reconstruct_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    """Per-TS Reconstruct card. Currently surfaces tomogram polarity; the
    WarpTools PNG + X/Z slab + 3dmod copy command are still hosted in the
    Candidate Extract section (Slice B refactor pending)."""
    rec = find_job_by_type(project_state, JobType.TS_RECONSTRUCT)
    if not rec:
        return False
    rec_iid, rec_jm = rec
    job_dir = job_dir_for(project_state, rec_iid, rec_jm, project_path)
    if not job_dir:
        return False
    tomo_df = read_tomograms_table(job_dir / "tomograms.star")
    if tomo_df is None or "rlnTomoName" not in tomo_df.columns:
        return False
    row = tomo_df[tomo_df["rlnTomoName"].astype(str) == ts_name]
    if row.empty:
        return False
    tomo_row = row.iloc[0]
    mrc_path = resolve_volume_for_3dmod(tomo_row, project_path)

    metric_parts: list[str] = []
    rescale = float(getattr(rec_jm, "rescale_angpixs", 0.0) or 0.0)
    if rescale:
        metric_parts.append(f"{rescale:g} Å/px")
    if "rlnTomoTomogramBinning" in tomo_row.index:
        try:
            metric_parts.append(f"bin {float(tomo_row['rlnTomoTomogramBinning']):g}")
        except (TypeError, ValueError):
            pass

    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "reconstruct"
        card._props["data-instance"] = rec_iid
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("view_in_ar", size="14px").classes("text-indigo-600")
            ui.label("Reconstruct").classes("cb-section-title")
            ui.label(rec_iid).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            if metric_parts:
                ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            status_label = getattr(rec_jm.execution_status, "value", str(rec_jm.execution_status))
            if status_label.lower() != "succeeded":
                ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        if mrc_path is None or not Path(mrc_path).exists():
            with ui.element("div").classes("cb-chip-strip"):
                render_chip(
                    "polarity",
                    "no MRC",
                    status="neutral",
                    tooltip="Reconstructed tomogram MRC not on disk for this TS — can't sample for polarity.",
                    icon="brightness_medium",
                )
            _render_recon_big_preview(ts_name, project_state, project_path, None, refresh)
            return True

        polarity = _compute_tomogram_polarity(Path(mrc_path))
        with ui.element("div").classes("cb-chip-strip"):
            if polarity is None:
                render_chip(
                    "polarity",
                    "read error",
                    status="warn",
                    tooltip=f"Could not sample center Z slice of {mrc_path} (see server logs).",
                    icon="brightness_medium",
                )
            else:
                expected = _expected_polarity_from_templates(project_state)
                status, hint = _tomo_polarity_chip_status(polarity["polarity"], expected)
                render_chip(
                    "polarity",
                    polarity["polarity"],
                    status=status,
                    tooltip=(
                        f"Reconstructed tomogram polarity: {polarity['polarity'].upper()} "
                        f"({polarity['pct_bright']:.1f}% bright vs {polarity['pct_dark']:.1f}% "
                        f"dark voxels in a center {polarity['slab_shape'][0]}×{polarity['slab_shape'][1]} "
                        f"Z slice). {hint}"
                    ),
                    icon="brightness_medium",
                )
                render_chip(
                    "bright %",
                    f"{polarity['pct_bright']:.1f}",
                    status="neutral",
                    tooltip="Fraction of voxels above mean + 1.5σ in the sampled Z slice.",
                )
                render_chip(
                    "dark %",
                    f"{polarity['pct_dark']:.1f}",
                    status="neutral",
                    tooltip="Fraction of voxels below mean − 1.5σ in the sampled Z slice.",
                )
        _render_recon_big_preview(ts_name, project_state, project_path, Path(mrc_path), refresh)
    return True


def _render_recon_big_preview(
    ts_name: str, project_state, project_path: Path, mrc_path: Path | None, refresh
) -> None:
    """Side-by-side tomogram preview: the WarpTools recon PNG (left) and the
    cryoCARE/IsoNet denoised X/Y slab (right).

    The recon PNG sits next to the .mrc as `<tomo>_<res>Apx.png`, written by
    ts_reconstruct. The denoised volume has no such PNG, so its pane is an X/Y
    slab rendered from the denoised MRC on first view (background, cached). The
    denoised pane only appears once denoisepredict has produced this TS; until
    then the recon PNG shows alone. Images use `object-fit: contain` so the
    natural aspect ratio (wide XY top-down) survives a tall viewport."""
    png_path = _find_warp_tomo_preview(project_path, ts_name, mrc_path)

    # One denoised pane, switchable across every denoise method that has a result for
    # this TS (cryoCARE, IsoNet, …). The selection persists per-project across rebuilds.
    methods = _available_denoise_methods_for_ts(project_state, project_path, ts_name)
    has_denoise = bool(methods)
    sel_key = str(project_path)
    labels = [m[0] for m in methods]
    selected = _SELECTED_DENOISE_METHOD.get(sel_key)
    if selected not in labels:
        selected = labels[0] if labels else None
    sel = next((m for m in methods if m[0] == selected), None)

    dn_png: Path | None = None
    dn_mrc: Path | None = None
    if sel is not None:
        _, dn_job_dir, dn_mrc = sel
        _auto_kick_denoise_slab(dn_job_dir, ts_name, dn_mrc, project_path, refresh)
        candidate = _denoise_slab_path(dn_job_dir, ts_name)
        if candidate.exists():
            dn_png = candidate

    if (not png_path or not png_path.exists()) and not has_denoise:
        ui.label("No tomogram preview on disk for this TS yet.").classes("cb-section-placeholder")
        return

    with ui.element("div").classes("cb-recon-compare"):
        # ── recon (left): clean WarpTools PNG ──
        with ui.element("div").classes("cb-recon-compare-pane"):
            ui.label("recon").classes("cb-recon-pane-tag")
            if png_path and png_path.exists():
                with ui.element("div").classes("cb-recon-preview"):
                    ui.html(
                        f"<img src='{vis_asset_url(str(png_path))}' alt='{ts_name} WarpTools recon' />", sanitize=False
                    )
                ui.label(f"WarpTools · {png_path.name}").classes("cb-recon-preview-caption")
            else:
                ui.label("No WarpTools preview PNG on disk.").classes("cb-section-placeholder")
        # ── denoised (right): X/Y slab for the selected method ──
        if has_denoise:
            with ui.element("div").classes("cb-recon-compare-pane"):
                with ui.row().classes("items-center gap-2 no-wrap"):
                    ui.label("denoised").classes("cb-recon-pane-tag")
                    if len(methods) >= 2:

                        def _on_method(e, _key=sel_key) -> None:
                            _SELECTED_DENOISE_METHOD[_key] = e.value
                            refresh()

                        (
                            ui.toggle({lbl: lbl for lbl in labels}, value=selected, on_change=_on_method)
                            .props("dense no-caps unelevated size=sm toggle-color=indigo color=grey-2")
                            .classes("cb-denoise-method-toggle")
                            .tooltip("Switch the denoised preview between methods with a result for this TS.")
                        )
                    else:
                        ui.label(selected).classes("cb-recon-pane-method")
                if dn_png is not None:
                    with ui.element("div").classes("cb-recon-preview"):
                        ui.html(
                            f"<img src='{vis_asset_url(str(dn_png))}' alt='{ts_name} {selected} denoised' />",
                            sanitize=False,
                        )
                    ui.label(f"{selected} · denoised X/Y slab · {dn_mrc.name}").classes("cb-recon-preview-caption")
                else:
                    ui.label("rendering denoised slab…").classes("cb-section-placeholder")


# ── Denoised tomogram preview (shown side-by-side with the recon in the Reconstruct
# section) ─────────────────────────────────────────────────────────────────────────
# cryoCARE/IsoNet write a denoised volume per tomogram but NO WarpTools preview PNG,
# so the denoised pane is an X/Y slab we render from the denoised MRC ourselves —
# same renderer + percentile pipeline as the recon slab — background-built and cached
# under the denoise job's vis/slabs. The denoised tomograms.star repoints
# rlnTomoReconstructedTomogram at the denoised volume, so the recon volume resolver
# (resolve_volume_for_3dmod) works unchanged.
_AUTO_KICKED_DENOISE_SLAB: set[str] = set()

# Which denoise method's slab the user is currently viewing, keyed by project path.
# A project can host several denoisepredict jobs (cryoCARE, IsoNet, …); the
# Reconstruct section offers a per-method toggle and remembers the pick here so it
# survives the dashboard's coalesced rebuilds.
_SELECTED_DENOISE_METHOD: dict[str, str] = {}


def _denoise_method_label(job_model, instance_id: str, project_state=None) -> str:
    """Human label for a denoisepredict job's method ('cryoCARE', 'IsoNet', …),
    falling back to the instance_id when the field is absent.

    Predict INHERITS its method from denoise-train at run time and never persists it back,
    so the stored `denoise_method` is the cryoCARE default even when IsoNet actually ran.
    Prefer the same inherited value the driver/job-tab use, then the stored field."""
    m = None
    if project_state is not None:
        inherit = getattr(job_model, "inherited_from_train", None)
        if callable(inherit):
            try:
                m = inherit(project_state)[0]
            except Exception:
                m = None
    if m is None:
        m = getattr(job_model, "denoise_method", None)
    return getattr(m, "value", None) or (str(m) if m else instance_id)


def _available_denoise_methods_for_ts(project_state, project_path: Path, ts_name: str) -> list[tuple[str, Path, Path]]:
    """(method_label, job_dir, denoised_mrc) for every denoisepredict job that has a
    denoised volume for this TS, ordered by label. When two jobs share a method the
    label is suffixed with the instance_id to keep selector keys unique/stable."""
    found: list[tuple[str, str, Path, Path]] = []  # (label, iid, job_dir, mrc)
    for iid, jm in (project_state.jobs or {}).items():
        is_dn = getattr(jm, "job_type", None) == JobType.DENOISE_PREDICT or InstanceId.matches(
            iid, JobType.DENOISE_PREDICT
        )
        if not is_dn:
            continue
        job_dir = job_dir_for(project_state, iid, jm, project_path)
        if not job_dir:
            continue
        mrc = denoised_mrc_from_registry(project_path, iid, ts_name)
        if mrc is not None:
            found.append((_denoise_method_label(jm, iid, project_state), iid, job_dir, mrc))
    label_counts: dict[str, int] = {}
    for lbl, *_ in found:
        label_counts[lbl] = label_counts.get(lbl, 0) + 1
    out: list[tuple[str, Path, Path]] = []
    for lbl, iid, job_dir, mrc in found:
        disp = lbl if label_counts[lbl] == 1 else f"{lbl} ({iid})"
        out.append((disp, job_dir, mrc))
    out.sort(key=lambda t: t[0].lower())
    return out


def _denoise_slab_path(denoise_job_dir: Path, ts_name: str) -> Path:
    return denoise_job_dir / "vis" / "slabs" / f"{ts_name}_xy.png"


def _render_denoise_slab_sync(mrc_path: Path, xy_png: Path) -> str:
    render_xy_slab_preview(Path(mrc_path), xy_png)
    return "denoised slab rendered"


def _auto_kick_denoise_slab(denoise_job_dir: Path, ts_name: str, mrc_path: Path, project_path: Path, refresh) -> None:
    """Render the denoised X/Y slab in the background if missing/stale. Mirrors
    _auto_kick_recon_slabs: module-level dedup set + BackgroundTask dedup_key,
    refresh-on-complete so the denoised pane fills when the PNG lands."""
    key = f"{denoise_job_dir}:{ts_name}"
    if key in _AUTO_KICKED_DENOISE_SLAB:
        return
    xy_png = _denoise_slab_path(denoise_job_dir, ts_name)
    if xy_png.exists() and not is_output_stale(xy_png, [mrc_path]):
        return
    _AUTO_KICKED_DENOISE_SLAB.add(key)

    async def _run(progress_cb):
        import asyncio as _asyncio

        progress_cb(0, 0, "rendering denoised slab…")
        return await _asyncio.to_thread(_render_denoise_slab_sync, mrc_path, xy_png)

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title=f"Render denoised slab · {ts_name}",
        subtitle="cryoCARE/IsoNet denoised tomogram preview",
        project_path=str(project_path),
        dedup_key=f"denoise-slab:{denoise_job_dir}:{ts_name}",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)

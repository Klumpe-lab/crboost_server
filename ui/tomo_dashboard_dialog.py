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
import json
import logging
import uuid
from contextlib import nullcontext
from pathlib import Path
from typing import Optional

import pandas as pd
from nicegui import app, context, ui

from services.configs.user_prefs_service import get_prefs_service
from services.models_base import JobStatus, JobType, ListExtractionState, PickListType
from services.project_state import PickList, get_project_state, get_state_service
from services.visualization.imod_vis import generate_candidate_vis
from services.visualization.preview_orchestrator import (
    _find_warp_tomo_preview,
    generate_candidate_previews,
    read_preview_manifest,
)
from services.visualization.preview_render import is_output_stale, render_xy_slab_preview, render_xz_slab_preview
from ui.components.reactive import SingleFlight
from ui.dashboard.css import ensure_assets_loaded
from ui.dashboard.figures import (
    _build_per_tilt_chart,
    _build_score_hist_fig,
    _build_xy_scatter_fig,
    _build_xz_scatter_fig,
    _is_meaningful_series,
    _safe_floats,
    _stats,
)
from ui.dashboard.data import (
    _SPECIES_OVERLAY_COLORS,
    _candidate_extract_instances,
    _collect_dashboard_journey,
    _collect_species_journey,
    _find_job_by_type,
    _glyph_for,
    _job_dir_for,
    _journey_signature,
    _matching_subtomo_instance,
    _position_label,
    _read_tomograms_table,
    _recon_mrc_map,
    _resolve_species,
    _resolve_volume_for_3dmod,
    _split_species_id,
    _template_match_instances,
    _vis_asset_url,
)
from ui.dashboard.pixel_sanity import _apply_sanity_rules, _compute_pixel_chain, _render_pixel_sanity_table
from ui.dashboard.strip import build_strip

logger = logging.getLogger(__name__)

# Guards the per-tomo "Curate in ArtiaX" handler against re-entry: the button can
# be destroyed + rebuilt mid-click by a dashboard refresh, so several clicks may
# land before one does — without this each would prep a bundle + open a dialog.
# See ui/components/reactive.py and CLAUDE.md "UI reactivity patterns".
_curation_flight = SingleFlight()


# Default overlay color per workbench-authored list type, chosen to sit apart from
# the per-species auto palette (_SPECIES_OVERLAY_COLORS) so a manual/imported/merged
# layer reads as a distinct lane over the same tomogram. Persisted onto the PickList
# at creation (PickList.color), so this is only the seed — retuning it here doesn't
# restyle already-registered lists.
_PICK_LIST_DEFAULT_COLOR = {
    PickListType.MANUAL: "#00e676",  # emerald — human picks
    PickListType.IMPORTED: "#ffea00",  # yellow — external
    PickListType.MERGED: "#ff6d00",  # deep orange — committed merge
}

# Extraction-state badge shown on each workbench list chip (Slice A surfaces it;
# the per-list Extract action that flips it is Slice C). Auto lists show none.
_EXTRACTION_BADGE = {
    ListExtractionState.EXTRACTED: ("✓ extracted", "cb-badge-ok"),
    ListExtractionState.NOT_EXTRACTED: ("○ not extracted", "cb-badge-todo"),
    ListExtractionState.STALE: ("⚠ stale · re-extract", "cb-badge-stale"),
}

# Per-chip type tag (Slice B item 3): distinguishes the pytom auto list from the
# manual/imported/merged workbench lists at a glance.
_LIST_TYPE_TAG = {
    PickListType.AUTO: "pytom",
    PickListType.MANUAL: "manual (ArtiaX)",
    PickListType.IMPORTED: "imported",
    PickListType.MERGED: "merged",
    PickListType.FILTERED: "filtered",
}

# Sticky per-(species_id, tomo) selected list slug so a background-render refresh
# (which rebuilds the whole tab body) doesn't bounce the user back to the auto list.
_SELECTED_LIST_SLUG: dict[tuple[str, str], str] = {}

# Sticky per-(species_id, tomo) set of slugs ticked for an inline merge. Module-level
# so the rail rebuild on refresh doesn't drop a half-made selection (cf. the selected-
# slug above). Cleared after a merge lands. Drives the co-located merge bar.
_MERGE_SELECT: dict[tuple[str, str], set[str]] = {}


def _read_picks_json(path: Path) -> dict:
    if not path or not Path(path).exists():
        return {"picks": [], "tomo_dims_xyz_px": [0, 0, 0], "score_field": None, "n": 0}
    try:
        return json.loads(Path(path).read_text())
    except Exception as e:
        logger.warning("Failed to load picks.json %s: %s", path, e)
        return {"picks": [], "tomo_dims_xyz_px": [0, 0, 0], "score_field": None, "n": 0}


# ---------------------------------------------------------------------------
# Per-tilt star helpers (for FS Motion/CTF, TS Align, TS CTF, Tilt Filter)
# ---------------------------------------------------------------------------


def _read_per_tilt_df(per_tilt_star_path: Path) -> Optional[pd.DataFrame]:
    """Load the per-TS tilt block from a per-tilt star file. Each per-tilt
    star has one data block named after the TS, with one row per tilt."""
    if not per_tilt_star_path.exists():
        return None
    try:
        import starfile

        data = starfile.read(per_tilt_star_path, always_dict=True)
        for v in data.values():
            if isinstance(v, pd.DataFrame) and "rlnTomoNominalStageTiltAngle" in v.columns:
                return v
    except Exception as e:
        logger.warning("Could not read per-tilt star %s: %s", per_tilt_star_path, e)
    return None


def _per_tilt_star_path(job_dir: Path, ts_name: str) -> Path:
    """Convention used by FS Motion/CTF, TS Align, TS CTF — per-tilt star
    sits at ``<job_dir>/tilt_series/<ts_name>.star``."""
    return job_dir / "tilt_series" / f"{ts_name}.star"


# Atlas-index parse cache (P3): path -> (mtime, meta). The cutout sheet re-reads
# the same index JSON on every visit; memoizing by mtime skips the parse on a warm
# revisit AND lets the sheet skip its loading spinner when the index is already in
# memory. Invalidated automatically when the index file is rewritten (new mtime).
_ATLAS_INDEX_MEMO: dict[str, tuple[float, dict]] = {}


def _read_atlas_index(index_path: Path) -> Optional[dict]:
    if not index_path or not Path(index_path).exists():
        return None
    key = str(index_path)
    try:
        mtime = Path(index_path).stat().st_mtime
    except OSError:
        mtime = 0.0
    hit = _ATLAS_INDEX_MEMO.get(key)
    if hit is not None and hit[0] == mtime:
        return hit[1]
    try:
        meta = json.loads(Path(index_path).read_text())
        if not meta.get("index"):
            return None
        _ATLAS_INDEX_MEMO[key] = (mtime, meta)
        return meta
    except Exception as e:
        logger.warning("Could not parse cutout index %s: %s", index_path, e)
        return None


# Keep/drop derive cache (P2 + P3): derive_keep_state_for_list reads TWO stars
# (source + <slug>_filtered.star) to recover which rows survived curation. Both the
# rail count (P2) and the cutout sheet's keep overlay (P3) need it, and collect runs
# on every 4s refresh — so memoize by (source mtime, filtered mtime) to avoid
# re-reading two stars per list per tick. None = no filter committed (all kept).
_KEEP_STATE_MEMO: dict[str, tuple[tuple, Optional[set[int]]]] = {}


def _memoized_keep_state(source_star: Path) -> Optional[set[int]]:
    """``picks_filter.derive_keep_state_for_list`` memoized by the two stars' mtimes
    so the table count and the cutout keep overlay share one read. Returns the kept
    ROW indices of ``source_star`` (None when no ``_filtered`` star exists)."""
    from services.visualization import picks_filter

    src = Path(source_star)
    filt = picks_filter.filtered_list_path(src)
    try:
        fmt = filt.stat().st_mtime if filt.exists() else None
    except OSError:
        fmt = None
    if fmt is None:
        return None  # no filtered star → all rows kept; nothing to read or cache
    try:
        smt = src.stat().st_mtime if src.exists() else 0.0
    except OSError:
        smt = 0.0
    key = str(src)
    sig = (smt, fmt)
    hit = _KEEP_STATE_MEMO.get(key)
    if hit is not None and hit[0] == sig:
        return hit[1]
    try:
        keep = picks_filter.derive_keep_state_for_list(src)
    except Exception:
        keep = None
    _KEEP_STATE_MEMO[key] = (sig, keep)
    return keep


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


def build_journey_panel(container, callbacks: Optional[dict] = None) -> None:
    """Build the per-TS Journey dashboard embedded into ``container``.

    Formerly ``open_tomo_dashboard`` (a maximized dialog). De-dialoged in P1 so
    the journey swaps into the workspace ``main_area`` like the pipeline and
    workbench views, instead of an overlay that covered the 60px icon strip.
    ``container`` is the workspace's ``journey_container`` (a flex column).
    ``callbacks``, when given, receives ``on_journey_active(bool)`` so the
    workspace can pause the live-refresh timer while the journey is hidden.
    """
    state = get_project_state()
    if state.project_path is None:
        container.clear()
        with container, ui.element("div").classes("cb-empty"):
            ui.icon("folder_off", size="40px").classes("text-gray-400")
            ui.label("No project loaded.").classes("text-sm text-gray-500")
        return

    project_path = Path(state.project_path)
    _, ts_names0 = _collect_dashboard_journey(state, project_path)
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
    col_els: dict[str, object] = {}
    _sel_gen = {"n": 0}

    def render_main() -> None:
        main_area.clear()
        with main_area:
            if selected["ts"] is None:
                _render_no_data_empty_state()
            else:
                _render_main_pane_for_ts(selected["ts"], state, project_path, request_refresh, render_strip)

    def render_strip() -> None:
        # Signature-gated (FingerprintedView discipline): the 4 s live timer
        # calls refresh_all on every background-task tick, but we only rebuild
        # when the journey data OR the selection actually changed — otherwise a
        # tick mid-click would tear down the column under the click.
        journey, ts_names = _collect_dashboard_journey(state, project_path)
        species_journey = _collect_species_journey(state, project_path)
        sig = (_journey_signature(journey, species_journey, ts_names), selected["ts"])
        if col_els and sig == _strip_sig["sig"]:
            return
        _strip_sig["sig"] = sig
        recon_mrc = _recon_mrc_map(state, project_path)
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
            )
        )

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
        render_main()
        render_strip()

    def refresh_all() -> None:
        render_strip()
        render_main()

    # P1: coalesce the initial refresh storm. On load several background auto-kicks
    # (preview / IMOD / recon-slabs / coords-ingest / list-cutouts) each fire
    # on_complete → refresh, and the 4 s live tick adds more — N immediate full
    # rebuilds make the page "jitter a few times on load". Those paths call
    # request_refresh() to raise a flag instead; the coalesce timer (set up with the
    # live timer below) flushes ONE trailing-edge rebuild once requests go quiet. The
    # first paint just below still rebuilds immediately.
    _refresh_req = {"pending": False, "quiet": 0}

    def request_refresh() -> None:
        _refresh_req["pending"] = True
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
    _curation_tick = {"n": 0}

    def _curation_bundles_sig() -> tuple:
        # Newest .coords mtime per per-(species,tomo) curation dir. An ArtiaX save
        # is an EXTERNAL process — it never appears as a background task, so the
        # registry signature below can't see it. Folding these mtimes into the
        # signature is what lets a fresh save move it → triggers a rebuild →
        # `_auto_kick_coords_ingest` finally runs and ingests WITHOUT a click.
        # Runs OFF-loop (see _maybe_refresh) — globs a handful of small dirs.
        base = Path(project_path) / "Curation"
        out: list[tuple[str, int]] = []
        try:
            for sp_dir in base.iterdir():
                if not sp_dir.is_dir():
                    continue
                for tomo_dir in sp_dir.iterdir():
                    if not tomo_dir.is_dir():
                        continue
                    newest = 0.0
                    for c in tomo_dir.glob("*.coords"):
                        try:
                            newest = max(newest, c.stat().st_mtime)
                        except OSError:
                            pass
                    out.append((f"{sp_dir.name}/{tomo_dir.name}", int(newest)))
        except OSError:
            return ()
        return tuple(sorted(out))

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
            # External ArtiaX .coords saves aren't registry tasks — fold the
            # bundle-dir mtimes in (off-loop) so a fresh save still triggers the
            # rebuild that runs the auto-ingest prescan.
            curation = await asyncio.to_thread(_curation_bundles_sig)
            # Curation-session liveness drives the toolbox 'Curate' button color (gray
            # = none / green = live). It shells out to squeue, so poll at a slow cadence
            # (~every 4th 4 s tick ≈ 16 s) and fold the bool into the signature so a
            # session started/stopped anywhere repaints the rail.
            _curation_tick["n"] += 1
            if _curation_tick["n"] % 4 == 1:
                try:
                    from backend import get_backend

                    bk = get_backend()
                    _CURATION_SESSION_LIVE["on"] = bool(await bk.find_active_curation_session_any()) if bk else False
                except Exception:
                    pass
            sig = (running, finished, curation, _CURATION_SESSION_LIVE["on"])
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
                    if curation != prev[2]:
                        moved.append("curation-save")
                    if len(prev) > 3 and sig[3] != prev[3]:
                        moved.append("curation-session")
                    logger.info("journey live-refresh rebuild (changed: %s)", ", ".join(moved) or "unknown")
                request_refresh()
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
            _refresh_req["pending"] = False
            _refresh_req["quiet"] = 0
            refresh_all()

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

    if callbacks is not None:
        callbacks["on_journey_active"] = _set_journey_active


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


def _info_copy_row(key: str, value: str, copy_value: Optional[str] = None) -> None:
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


def _ts_meta_line(species_list: list[dict]) -> Optional[str]:
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


def _render_ts_info_popover(ts_name: str, species_list: list[dict], recon_mrc: Optional[str]) -> None:
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


def _render_main_pane_for_ts(ts_name: str, project_state, project_path: Path, refresh, refresh_roster=None) -> None:
    """Render the main-pane section stack for the selected TS. Sections emit
    in pipeline order; each is a no-op if the corresponding job isn't in the
    pipeline (per ROADMAP §2.1 contract). `refresh_roster` is a sidebar-only
    refresh the gallery calls after save/discard so the roster review column
    updates without rebuilding the main pane (which would reset the active tab)."""
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
        if _render_particles_section(ts_name, project_state, project_path, refresh, refresh_roster):
            rendered_any = True
        elif _render_imported_particles_section(ts_name, project_state, project_path, refresh, refresh_roster):
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
    instance_id: Optional[str],
    job_status_label: Optional[str],
    rows: list[tuple[str, str]],
    note: Optional[str] = None,
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


def _render_chip(
    label: str, value: str, *, status: str = "neutral", tooltip: Optional[str] = None, icon: Optional[str] = None
) -> None:
    """One status chip. `status` ∈ {ok, warn, error, info, neutral}."""
    cls = f"cb-chip cb-chip-{status}"
    with ui.element("span").classes(cls) as chip:
        if icon:
            ui.icon(icon, size="11px").classes("cb-chip-icon")
        ui.label(label).classes("cb-chip-label")
        ui.label(value).classes("cb-chip-value")
        if tooltip:
            chip.tooltip(tooltip)


def _read_tomohand_from_import_star(star_path: Path) -> Optional[int]:
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
            uniq = sorted({int(round(x)) for x in vals})
            if len(uniq) == 1:
                return int(uniq[0])
            return 0  # mixed
    return None


def _find_import_job(project_state) -> Optional[tuple[str, object]]:
    """Locate the Import (relion.importtomo) job. The dataset chip needs it
    to cross-check the in-memory `invert_defocus_hand` against the actual
    `_rlnTomoHand` Import wrote into `tilt_series.star`."""
    return _find_job_by_type(project_state, JobType.IMPORT_MOVIES)


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
    disk_hand: Optional[int] = None
    imp = _find_import_job(project_state)
    if imp:
        imp_dir = _job_dir_for(imp[0], imp[1], project_path)
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

    with ui.element("div").classes("cb-chip-strip"):
        _render_chip("TomoHand", value, status=status, tooltip=tooltip, icon="compare_arrows")


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

    pixel_rows = _compute_pixel_chain(project_state)
    _apply_sanity_rules(pixel_rows)

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
            _render_pixel_sanity_table(pixel_rows)

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


def _plot_cell(label: str, fig: dict, *, height_px: int = 220, wide: bool = False, hint: Optional[str] = None) -> None:
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


def _per_tilt_customdata(df: pd.DataFrame) -> list[list]:
    """Build [[tilt_index, frame_basename], ...] customdata so plot hovers
    can name the specific tilt instead of just its angle."""
    n = int(len(df))
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


def _render_ctf_motion_plots(
    df: pd.DataFrame, *, show_motion: bool = True, frameseries_dir: Optional[Path] = None
) -> None:
    """Defocus + astigmatism (always plotted as scatter, since each tilt is an
    independent estimate). CTF max-resolution / FOM / motion are gated on
    `_is_meaningful_series` because WarpTools-exported RELION stars often
    write `1e-6` placeholders for those columns — see
    `project_warp_relion_star_placeholders.md`.

    When `frameseries_dir` (the FS-motion job's `warp_frameseries` folder) is
    given, the CTF-resolution and motion panels read the REAL per-tilt values
    from the WarpTools XML instead of the placeholder star columns — see
    `docs/preprocessing-metrics-inventory.md` §4."""
    tilts = _safe_floats(df["rlnTomoNominalStageTiltAngle"])
    cd = _per_tilt_customdata(df)

    # Real per-tilt CTF-fit resolution + motion live in the WarpTools frameseries
    # XML, not the star (the star columns are 1e-6 / 'None' placeholders). Read
    # them once here so the CTF-res + motion panels show real data.
    xml_res: Optional[list] = None
    xml_motion: Optional[list] = None
    if frameseries_dir is not None and "rlnMicrographMovieName" in df.columns:
        from services.tilt_series.frameseries_quality import quality_series

        xml_res, xml_motion = quality_series(frameseries_dir, df["rlnMicrographMovieName"].tolist())

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
            mag = [(x * x + y * y) ** 0.5 if x is not None and y is not None else None for x, y in zip(xs, ys)]
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
                resid = [val - nt if nt is not None and val is not None else None for nt, val in zip(tilts, xt)]
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
    found = _find_job_by_type(project_state, JobType.FS_MOTION_CTF)
    if not found:
        return False
    instance_id, jm = found
    job_dir = _job_dir_for(instance_id, jm, project_path)
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

    df = _read_per_tilt_df(_per_tilt_star_path(job_dir, ts_name))
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
            ui.label(f"No per-tilt star at tilt_series/{ts_name}.star yet.").classes("cb-section-placeholder")
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
            return True

        # Real CTF-fit resolution + motion come from the frameseries XML, not the
        # placeholder star columns (rlnCtfMaxResolution / rlnAccumMotion* = 1e-6).
        fs_warp_dir = job_dir / "warp_frameseries"
        ctf_res: list = []
        motion_total: list = []
        if "rlnMicrographMovieName" in df.columns:
            from services.tilt_series.frameseries_quality import quality_series

            r_xml, m_xml = quality_series(fs_warp_dir, df["rlnMicrographMovieName"].tolist())
            ctf_res = [v for v in r_xml if v is not None]
            motion_total = [v for v in m_xml if v is not None]
        defocus_um = [v / 1.0e4 for v in _safe_floats(df.get("rlnDefocusU", [])) if v is not None]
        d_stats = _stats(defocus_um)
        r_stats = _stats(ctf_res)
        m_stats = _stats(motion_total)
        strip_rows: list[tuple[str, str]] = [("tilts", str(int(len(df))))]
        if d_stats["n"]:
            strip_rows.append(
                ("defocus", f"{d_stats['median']:.2f} µm (Q1 {d_stats['q1']:.2f} · Q3 {d_stats['q3']:.2f})")
            )
        if r_stats["n"]:
            strip_rows.append(("CTF res", f"{r_stats['median']:.1f} Å (worst {r_stats['max']:.1f})"))
        if m_stats["n"]:
            strip_rows.append(("motion (max)", f"{m_stats['max']:.2f}"))
        _stat_strip(strip_rows)

        _render_ctf_motion_plots(df, show_motion=True, frameseries_dir=fs_warp_dir)

        with ui.expansion("Job parameters").classes("w-full text-[10px]").props("dense"):
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
    return True


def _render_ts_alignment_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    found = _find_job_by_type(project_state, JobType.TS_ALIGNMENT)
    if not found:
        return False
    instance_id, jm = found
    job_dir = _job_dir_for(instance_id, jm, project_path)
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

    df = _read_per_tilt_df(_per_tilt_star_path(job_dir, ts_name))
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
            ui.label(f"No per-tilt star at tilt_series/{ts_name}.star yet.").classes("cb-section-placeholder")
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
            return True

        # Stat strip: max shift magnitude + tilt-axis residual range
        x_shift = _safe_floats(df.get("rlnTomoXShiftAngst", [])) if "rlnTomoXShiftAngst" in df.columns else []
        y_shift = _safe_floats(df.get("rlnTomoYShiftAngst", [])) if "rlnTomoYShiftAngst" in df.columns else []
        mag = [(x * x + y * y) ** 0.5 for x, y in zip(x_shift, y_shift) if x is not None and y is not None]
        m_stats = _stats(mag)
        strip_rows: list[tuple[str, str]] = [("tilts", str(int(len(df))))]
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
    found = _find_job_by_type(project_state, JobType.TS_CTF)
    if not found:
        return False
    instance_id, jm = found
    job_dir = _job_dir_for(instance_id, jm, project_path)
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

    df = _read_per_tilt_df(_per_tilt_star_path(job_dir, ts_name))
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
            ui.label(f"No per-tilt star at tilt_series/{ts_name}.star yet.").classes("cb-section-placeholder")
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
            return True

        defocus_um = [v / 1.0e4 for v in _safe_floats(df.get("rlnDefocusU", [])) if v is not None]
        ctf_res = [v for v in _safe_floats(df.get("rlnCtfMaxResolution", [])) if v is not None]
        d_stats = _stats(defocus_um)
        r_stats = _stats(ctf_res)
        strip_rows: list[tuple[str, str]] = [("tilts", str(int(len(df))))]
        if d_stats["n"]:
            strip_rows.append(
                ("defocus", f"{d_stats['median']:.2f} µm (range {d_stats['min']:.2f}–{d_stats['max']:.2f})")
            )
        if r_stats["n"]:
            strip_rows.append(("CTF res", f"{r_stats['median']:.1f} Å (worst {r_stats['max']:.1f})"))
        _stat_strip(strip_rows)

        # Skip the motion plot here — TS CTF doesn't change per-tilt motion;
        # that's already shown in the FS Motion/CTF section above.
        _render_ctf_motion_plots(df, show_motion=False)

        with ui.expansion("Job parameters").classes("w-full text-[10px]").props("dense"):
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
    return True


# --- Tilt Filter --------------------------------------------------------------


def _resolve_tilt_filter_dir(project_state, project_path: Path) -> Optional[Path]:
    """Return the directory containing tiltseries_filtered.star and
    tiltseries_labeled.star — supports both pipeline-job tilt filtering
    (TILT_FILTER) and the standalone TiltFilter tool that writes to
    `<project_root>/TiltFilter/`."""
    found = _find_job_by_type(project_state, JobType.TILT_FILTER)
    if found:
        instance_id, jm = found
        jd = _job_dir_for(instance_id, jm, project_path)
        if jd is not None:
            cand = jd / "filtered"
            if (cand / "tiltseries_labeled.star").exists() or (cand / "tiltseries_filtered.star").exists():
                return cand
    standalone = project_path / "TiltFilter"
    if (standalone / "tiltseries_labeled.star").exists() or (standalone / "tiltseries_filtered.star").exists():
        return standalone
    return None


def _read_per_tilt_frame_names(per_tilt_star: Path) -> list[str]:
    df = _read_per_tilt_df(per_tilt_star)
    if df is None or "rlnMicrographMovieName" not in df.columns:
        return []
    return [str(v) for v in df["rlnMicrographMovieName"].tolist()]


def _read_per_tilt_kept_dropped(filter_dir: Path, ts_name: str) -> Optional[dict]:
    """Diff labeled vs filtered per-tilt star to compute kept and dropped rows.

    Returns a dict with keys: n_labeled, n_kept, dropped (list of dicts with
    `index`, `tilt_angle`, `frame`). None if the labeled file is missing."""
    labeled_p = filter_dir / "tilt_series_labeled" / f"{ts_name}.star"
    filtered_p = filter_dir / "tilt_series_filtered" / f"{ts_name}.star"
    labeled_df = _read_per_tilt_df(labeled_p)
    if labeled_df is None:
        return None
    kept_frames: set[str] = set()
    if filtered_p.exists():
        kept_frames = set(_read_per_tilt_frame_names(filtered_p))
    n_labeled = int(len(labeled_df))
    dropped: list[dict] = []
    if "rlnMicrographMovieName" in labeled_df.columns and kept_frames:
        for i, row in labeled_df.iterrows():
            frame = str(row["rlnMicrographMovieName"])
            if frame in kept_frames:
                continue
            tilt_angle = None
            if "rlnTomoNominalStageTiltAngle" in labeled_df.columns:
                try:
                    tilt_angle = float(row["rlnTomoNominalStageTiltAngle"])
                except (TypeError, ValueError):
                    tilt_angle = None
            dropped.append({"index": int(i), "tilt_angle": tilt_angle, "frame": Path(frame).name})
    n_kept = n_labeled - len(dropped) if kept_frames else n_labeled
    return {"n_labeled": n_labeled, "n_kept": n_kept, "dropped": dropped, "labeled_df": labeled_df}


def _render_tilt_filter_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    """Per-TS tilt-filter diagnostics. Renders for either:
      - a TILT_FILTER pipeline job (uses its `filtered/` subdir), or
      - the standalone TiltFilter tool output at `<project_root>/TiltFilter/`.
    Skips silently when neither is present."""
    filter_dir = _resolve_tilt_filter_dir(project_state, project_path)
    if filter_dir is None:
        return False

    job_found = _find_job_by_type(project_state, JobType.TILT_FILTER)
    instance_id = job_found[0] if job_found else "TiltFilter (standalone)"
    jm = job_found[1] if job_found else None

    metric_parts = []
    if jm is not None:
        metric_parts = [f"model {jm.model_name}", f"thresh {jm.prob_threshold:g}", f"action {jm.prob_action}"]

    info = _read_per_tilt_kept_dropped(filter_dir, ts_name)
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
            ui.label(f"No labeled tilt list for {ts_name} in {filter_dir}").classes("cb-section-placeholder")
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


def _compute_tomogram_polarity(mrc_path: Path) -> Optional[dict]:
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


def _expected_polarity_from_templates(project_state) -> Optional[str]:
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


def _tomo_polarity_chip_status(polarity: str, expected: Optional[str]) -> tuple[str, str]:
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
    rec = _find_job_by_type(project_state, JobType.TS_RECONSTRUCT)
    if not rec:
        return False
    rec_iid, rec_jm = rec
    job_dir = _job_dir_for(rec_iid, rec_jm, project_path)
    if not job_dir:
        return False
    tomo_df = _read_tomograms_table(job_dir / "tomograms.star")
    if tomo_df is None or "rlnTomoName" not in tomo_df.columns:
        return False
    row = tomo_df[tomo_df["rlnTomoName"].astype(str) == ts_name]
    if row.empty:
        return False
    tomo_row = row.iloc[0]
    mrc_path = _resolve_volume_for_3dmod(tomo_row, project_path)

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
                _render_chip(
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
                _render_chip(
                    "polarity",
                    "read error",
                    status="warn",
                    tooltip=f"Could not sample center Z slice of {mrc_path} (see server logs).",
                    icon="brightness_medium",
                )
            else:
                expected = _expected_polarity_from_templates(project_state)
                status, hint = _tomo_polarity_chip_status(polarity["polarity"], expected)
                _render_chip(
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
                _render_chip(
                    "bright %",
                    f"{polarity['pct_bright']:.1f}",
                    status="neutral",
                    tooltip="Fraction of voxels above mean + 1.5σ in the sampled Z slice.",
                )
                _render_chip(
                    "dark %",
                    f"{polarity['pct_dark']:.1f}",
                    status="neutral",
                    tooltip="Fraction of voxels below mean − 1.5σ in the sampled Z slice.",
                )
        _render_recon_big_preview(ts_name, project_state, project_path, Path(mrc_path), refresh)
    return True


def _render_recon_big_preview(
    ts_name: str, project_state, project_path: Path, mrc_path: Optional[Path], refresh
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

    dn_png: Optional[Path] = None
    dn_mrc: Optional[Path] = None
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
                        f"<img src='{_vis_asset_url(str(png_path))}' alt='{ts_name} WarpTools recon' />", sanitize=False
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
                            f"<img src='{_vis_asset_url(str(dn_png))}' alt='{ts_name} {selected} denoised' />",
                            sanitize=False,
                        )
                    ui.label(f"{selected} · denoised X/Y slab · {dn_mrc.name}").classes("cb-recon-preview-caption")
                else:
                    ui.label("rendering denoised slab…").classes("cb-section-placeholder")


def _render_invert_switch(root) -> None:
    """Polarity-invert toggle wired to add/remove `cb-invert-polarity` on
    `root`. The CSS flips every `.cb-tomo-preview > img` (and cutout tiles)
    under that root together, so density convention stays consistent."""

    def _on_invert(e, _root=root):
        if e.value:
            _root.classes(add="cb-invert-polarity")
        else:
            _root.classes(remove="cb-invert-polarity")

    (
        ui.switch("Invert", value=False, on_change=_on_invert)
        .props("dense color=indigo-6 left-label")
        .classes("text-[10px]")
        .tooltip(
            "Flip the apparent intensity of the tomogram slabs (and cutout tiles). "
            "The picker uses raw densities — this is viewer-only."
        )
    )


# ---------------------------------------------------------------------------
# Shared recon-stage tomogram canvas + multi-species pick overlay
#
# One X/Y + X/Z slab rendered from the reconstructed MRC (same percentile +
# flip pipeline as the cutout tiles, so Invert flips everything together),
# reused as the backdrop for every species' picks. Each candidate-extract
# instance's picks are drawn as a color-coded ghost-dot layer the user can
# toggle by species. Progressive: recon-only shows just the slabs; picks
# appear once candidate-extract has run.
# ---------------------------------------------------------------------------


_AUTO_KICKED_RECON_SLABS: set[str] = set()


def _recon_slab_paths(recon_job_dir: Path, ts_name: str) -> tuple[Path, Path]:
    base = recon_job_dir / "vis" / "slabs"
    return base / f"{ts_name}_xy.png", base / f"{ts_name}_xz.png"


def _render_recon_slabs_sync(mrc_path: Path, xy_png: Path, xz_png: Path) -> str:
    render_xy_slab_preview(Path(mrc_path), xy_png)
    render_xz_slab_preview(Path(mrc_path), xz_png)
    return "recon slabs rendered"


def _auto_kick_recon_slabs(recon_job_dir: Path, ts_name: str, mrc_path: Path, project_path: Path, refresh) -> None:
    """Render the shared X/Y + X/Z slabs in the background if missing/stale.
    Mirrors the candidate-extract preview auto-kick: module-level dedup set
    plus BackgroundTask dedup_key, refresh-on-complete so the canvas fills
    when the PNGs land."""
    key = f"{recon_job_dir}:{ts_name}"
    if key in _AUTO_KICKED_RECON_SLABS:
        return
    xy_png, xz_png = _recon_slab_paths(recon_job_dir, ts_name)
    fresh = (
        xy_png.exists()
        and xz_png.exists()
        and not is_output_stale(xy_png, [mrc_path])
        and not is_output_stale(xz_png, [mrc_path])
    )
    if fresh:
        return
    _AUTO_KICKED_RECON_SLABS.add(key)

    async def _run(progress_cb):
        import asyncio as _asyncio

        progress_cb(0, 0, "rendering tomogram slabs…")
        return await _asyncio.to_thread(_render_recon_slabs_sync, mrc_path, xy_png, xz_png)

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title=f"Render tomogram slabs · {ts_name}",
        subtitle="Shared canvas for the pick overlay",
        project_path=str(project_path),
        dedup_key=f"recon-slabs:{recon_job_dir}:{ts_name}",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)


# ── Denoised tomogram preview (shown side-by-side with the recon in the Reconstruct
# section) ─────────────────────────────────────────────────────────────────────────
# cryoCARE/IsoNet write a denoised volume per tomogram but NO WarpTools preview PNG,
# so the denoised pane is an X/Y slab we render from the denoised MRC ourselves —
# same renderer + percentile pipeline as the recon slab — background-built and cached
# under the denoise job's vis/slabs. The denoised tomograms.star repoints
# rlnTomoReconstructedTomogram at the denoised volume, so the recon volume resolver
# (_resolve_volume_for_3dmod) works unchanged.
_AUTO_KICKED_DENOISE_SLAB: set[str] = set()

# Which denoise method's slab the user is currently viewing, keyed by project path.
# A project can host several denoisepredict jobs (cryoCARE, IsoNet, …); the
# Reconstruct section offers a per-method toggle and remembers the pick here so it
# survives the dashboard's coalesced rebuilds.
_SELECTED_DENOISE_METHOD: dict[str, str] = {}


def _denoise_method_label(job_model, instance_id: str) -> str:
    """Human label for a denoisepredict job's method ('cryoCARE', 'IsoNet', …),
    falling back to the instance_id when the field is absent."""
    m = getattr(job_model, "denoise_method", None)
    return getattr(m, "value", None) or (str(m) if m else instance_id)


def _resolve_denoised_mrc_for_job(job_dir: Path, project_path: Path, ts_name: str) -> Optional[Path]:
    """Denoised-tomogram MRC for one denoisepredict job + TS, or None. Prefers the
    job's aggregated tomograms.star (rlnTomoReconstructedTomogram); falls back to the
    per-tomogram file under denoised/ so results surface as the SLURM array lands
    them, before the final star is written at job end."""
    tomo_df = _read_tomograms_table(job_dir / "tomograms.star")
    if tomo_df is not None and "rlnTomoName" in tomo_df.columns:
        match = tomo_df[tomo_df["rlnTomoName"].astype(str) == ts_name]
        if not match.empty:
            mrc = _resolve_volume_for_3dmod(match.iloc[0], project_path)
            if mrc:
                return Path(mrc)
    dn_dir = job_dir / "denoised"
    if dn_dir.is_dir():
        # Per-tomo file is "<ts_name>_<apix>Apx.mrc"; the tail after "<ts_name>_"
        # must be just "<apix>Apx" (no extra underscore) so Position_1 doesn't match
        # Position_1_2's file.
        for f in sorted(dn_dir.glob(f"{ts_name}_*Apx.mrc")):
            tail = f.stem[len(ts_name) + 1 :]
            if tail.endswith("Apx") and tail[:-3].replace(".", "", 1).isdigit():
                return f
    return None


def _available_denoise_methods_for_ts(
    project_state, project_path: Path, ts_name: str
) -> list[tuple[str, Path, Path]]:
    """(method_label, job_dir, denoised_mrc) for every denoisepredict job that has a
    denoised volume for this TS, ordered by label. When two jobs share a method the
    label is suffixed with the instance_id to keep selector keys unique/stable."""
    found: list[tuple[str, str, Path, Path]] = []  # (label, iid, job_dir, mrc)
    for iid, jm in (project_state.jobs or {}).items():
        is_dn = (
            getattr(jm, "job_type", None) == JobType.DENOISE_PREDICT
            or iid.split("__")[0] == JobType.DENOISE_PREDICT.value
        )
        if not is_dn:
            continue
        job_dir = _job_dir_for(iid, jm, project_path)
        if not job_dir:
            continue
        mrc = _resolve_denoised_mrc_for_job(job_dir, project_path, ts_name)
        if mrc is not None:
            found.append((_denoise_method_label(jm, iid), iid, job_dir, mrc))
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


# ── Per-list recon cutouts (the workbench contact sheet) ───────────────────────
# Workbench lists (manual/imported/merged) were never subtomo-extracted, so they
# have no subtomo atlas. We cut tiles straight from the binned recon at each pick
# voxel via services.visualization.recon_cutouts (same atlas PNG + index schema as
# the subtomo gallery), background-built like the recon slabs and shown read-only.
_AUTO_KICKED_LIST_CUTOUTS: set[str] = set()


def _fs_slug(name: str) -> str:
    """Filesystem-safe slug (no regex dep) for cutout cache filenames."""
    return "".join(c if (c.isalnum() or c in "._-") else "_" for c in str(name)).strip("_") or "x"


def _list_cutout_paths(project_path: Path, species_id: str, tomo_name: str, slug: str) -> tuple[Path, Path]:
    """(atlas PNG, index JSON) cache paths for one workbench list's recon cutouts."""
    base = Path(project_path) / ".curation_sessions" / "cutouts" / _fs_slug(species_id)
    stem = f"{_fs_slug(tomo_name)}__{_fs_slug(slug)}"
    return base / f"{stem}.png", base / f"{stem}.json"


def _list_cutout_box_px(sp: dict) -> int:
    """Cutout box edge in binned-recon px ≈ 2× particle diameter (context around
    the pick), clamped. Falls back to 48 when diameter/pixel size is unknown."""
    diameter = float(getattr(sp.get("jm"), "particle_diameter_ang", 0.0) or 0.0)
    px = (sp.get("entry") or {}).get("pixel_size_ang")
    if diameter and px and px > 0:
        half = max(16, min(96, int(round(diameter / px))))
        return half * 2
    return 48


# ── Cutout display-filter UI (shared by the auto gallery + list sheet) ──
# Presets live in services/visualization/cutout_filters.py. The control is two
# compact selects: TYPE (None/Denoise/Local contrast/Bandpass/Lowpass) and, when
# the type has more than one option, PARAM (its intensity/cutoff). `on_select(key)`
# fires with the chosen preset key whenever either changes.
def _cutout_filter_tooltip(apix, apix_source: str) -> str:
    base = (
        "Display filter only — never changes the data, the picks, or any star file. "
        "Denoise / Local contrast usually read best on noisy tomograms; "
        "Lowpass / Bandpass are frequency cutoffs. "
    )
    if apix and apix > 0:
        return base + f"Frequency cutoffs use {apix:.2f} Å/px (from {apix_source}; e.g. 30 Å ≈ {30.0 / apix:.0f}px)."
    return base + "Pixel size is unknown (absent from the cutout header), so the frequency filters are hidden."


def _render_cutout_filter_controls(presets: list, apix, apix_source: str, on_select) -> None:
    """Render the compact TYPE (+ conditional PARAM) selects into the current row.
    `presets` are the preset dicts that have a variant atlas on disk (ordered).
    Calls `on_select(preset_key)` whenever the selection changes."""
    if len(presets) < 2:
        return
    types: list[tuple[str, str]] = []
    seen: set[str] = set()
    by_type: dict[str, list[dict]] = {}
    for p in presets:
        t = p.get("type", "raw")
        by_type.setdefault(t, []).append(p)
        if t not in seen:
            seen.add(t)
            types.append((t, p.get("type_label", t)))

    def _params(t: str) -> dict:
        return {p["key"]: (p.get("param_label") or "—") for p in by_type.get(t, [])}

    st = {"type": types[0][0], "key": by_type[types[0][0]][0]["key"]}

    type_sel = (
        ui.select({t: lbl for t, lbl in types}, value=st["type"])
        .props("dense outlined options-dense")
        .classes("text-xs")
        .style("min-width: 116px;")
    )
    param_sel = (
        ui.select(_params(st["type"]), value=st["key"])
        .props("dense outlined options-dense")
        .classes("text-xs")
        .style("min-width: 84px;")
    )
    param_sel.set_visibility(len(by_type[st["type"]]) > 1)

    def _on_type(e) -> None:
        t = e.value or types[0][0]
        st["type"] = t
        opts = _params(t)
        keys = list(opts)
        st["key"] = keys[0] if keys else t
        multi = len(keys) > 1
        param_sel.set_visibility(multi)
        if multi:
            param_sel.set_options(opts, value=st["key"])
        on_select(st["key"])

    def _on_param(e) -> None:
        st["key"] = e.value or st["key"]
        on_select(st["key"])

    type_sel.on_value_change(_on_type)
    param_sel.on_value_change(_on_param)
    with ui.icon("help_outline", size="13px").style("color: #94a3b8; cursor: help;"):
        ui.tooltip(_cutout_filter_tooltip(apix, apix_source)).style("font-size: 10px; max-width: 320px;")


def _atlas_index_is_current(index_path: Path) -> bool:
    """A cutout index from an older filter-preset set has a stale (or missing)
    `filter_schema`; treat those as stale so the current variant atlases get
    (re)generated."""
    from services.visualization.cutout_filters import FILTER_SCHEMA_VERSION

    meta = _read_atlas_index(index_path)
    return bool(meta) and meta.get("filter_schema") == FILTER_SCHEMA_VERSION


def _auto_kick_list_cutouts(
    recon_mrc: Path,
    picks: list,
    star_path: Optional[str],
    atlas_path: Path,
    index_path: Path,
    box_px: int,
    project_path: Path,
    refresh,
    dedup_key: str,
    apix_hint: Optional[float] = None,
) -> bool:
    """True if the list's recon-cutout atlas is on disk + fresh (render now); else
    kick ONE background build (mirrors `_auto_kick_recon_slabs`) and return False
    so the caller shows a placeholder. `dedup_key` carries the star mtime, so a
    re-import (new mtime) rebuilds without needing a dashboard reopen."""
    sources = [Path(recon_mrc)] + ([Path(star_path)] if star_path else [])
    if (
        atlas_path.exists()
        and index_path.exists()
        and not is_output_stale(atlas_path, sources)
        and _atlas_index_is_current(index_path)
    ):
        return True
    if dedup_key in _AUTO_KICKED_LIST_CUTOUTS:
        return False
    _AUTO_KICKED_LIST_CUTOUTS.add(dedup_key)

    async def _run(progress_cb):
        import asyncio as _asyncio

        from services.visualization.recon_cutouts import render_recon_cutouts_atlas

        progress_cb(0, 0, "cutting tiles from the recon…")
        return await _asyncio.to_thread(
            render_recon_cutouts_atlas,
            Path(recon_mrc),
            picks,
            Path(atlas_path),
            Path(index_path),
            box_px=box_px,
            apix_hint=apix_hint,
        )

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title="Render list cutouts",
        subtitle="Recon-sourced tiles for a curation list",
        project_path=str(project_path),
        dedup_key=f"list-cutouts:{dedup_key}",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)
    return False


def _render_list_header(lst: dict, sp: dict, project_path: Path) -> None:
    """Swatch + label (+ merge provenance) for one workbench list, rendered inside a
    caller-provided row so the contact sheet and the building/empty states share one
    header. The 'Open in ArtiaX' action lived here too but was REDUNDANT with the
    rail toolbox's Curate/Load (both open this tomo in ArtiaX) — removed per the user;
    `_handle_open_list_in_artiax` stays as the W1 round-trip-edit foundation."""
    ui.element("div").classes(f"cb-species-swatch cb-swatch-{lst['shape']}").style(f"background: {lst['color']};")
    ui.label(lst["label"]).classes("cb-section-title")
    parents = lst.get("parent_slugs") or []
    if parents:
        ui.label("⋃ " + ", ".join(parents)).classes("cb-detail-meta").tooltip(
            "Merged from these lists (row order = type priority: manual/imported before auto)"
        )


def _render_list_cutouts_status(lst: dict, sp: dict, project_path: Path, *, building: bool) -> None:
    """Slim header row shown while a list's cutouts build (spinner) or when the
    build produced nothing (picks outside the recon / recon unreadable)."""
    with ui.element("div").classes("w-full").style("margin-top: 10px;"), ui.row().classes("items-center gap-2"):
        _render_list_header(lst, sp, project_path)
        ui.label(f"· {len(lst.get('picks', []))} picks").classes("cb-detail-meta")
        if building:
            ui.spinner(size="16px", color="indigo-500")
            ui.label("rendering cutouts from the recon…").style("font-size: 10px; color: #94a3b8;")
        else:
            ui.label("no cutouts (picks outside the recon, or recon unreadable)").style(
                "font-size: 10px; color: #94a3b8;"
            )


def _render_list_cutout_sheet(
    lst: dict, sp: dict, project_path: Path, atlas_meta: dict, atlas_path: str, initial_keep, refresh
) -> None:
    """Interactive contact sheet for one workbench list (manual/imported/merged):
    CSS-sprite tiles cut from the recon, each click-toggleable keep/drop. A dropped
    tile greys out AND greys its matching slab ghost-dot (the dot↔tile sync, via the
    list's own `_lid_xy/_lid_xz` canvas layers). Each toggle AUTO-COMMITS the kept
    subset to `<slug>_filtered.star` (no Save click) — so the selection persists across
    navigation and the merge + per-list extraction consume exactly the kept picks; the
    rail table's count cell live-updates to kept/total. "Reset" clears the filter (all
    kept). These lists are scoreless, so keep/discard IS the filter — there is no score
    threshold ([[feedback_per_list_extraction]])."""
    from services.visualization import picks_filter

    index = atlas_meta.get("index", {})
    if not index:
        return
    cols = int(atlas_meta.get("cols", 8))
    rows = int(atlas_meta.get("rows", 1))
    atlas_url = _vis_asset_url(atlas_path)
    tile = 96
    bg_w, bg_h = cols * tile, rows * tile
    all_idx = sorted(int(k) for k in index.keys())
    star_path = lst.get("path")
    # The list's own slab dot-layers (stashed by the canvas renderer) — used to
    # mirror keep/drop onto the ghost dots. Empty when the list isn't on the canvas.
    layer_ids = [lid for lid in (lst.get("_lid_xy"), lst.get("_lid_xz")) if lid]
    grid_id = f"cb-list-grid-{uuid.uuid4().hex[:8]}"
    client = ui.context.client
    tiles: dict[int, object] = {}
    # keep_set == None means "no curation yet — all kept implicitly"; it materializes
    # to a concrete set on the first toggle. Every toggle AUTO-COMMITS to
    # <slug>_filtered.star (serialized via `commit`) — so the selection persists across
    # navigation and the merge/extraction consume exactly the kept picks, no Save click.
    state = {"keep_set": set(initial_keep) if initial_keep is not None else None}
    commit = {"running": False, "dirty": False}
    species_id = sp.get("species_id") or ""
    tomo_name = sp["row"]["tomo_name"]

    def _is_kept(i: int) -> bool:
        ks = state["keep_set"]
        return True if ks is None else i in ks

    def _dropped_now() -> list[int]:
        ks = state["keep_set"]
        return [] if ks is None else [i for i in all_idx if i not in ks]

    def _run_js(js: str) -> None:
        try:
            client.run_javascript(js)
        except Exception:
            pass  # client gone / slot torn down — the dot sync is best-effort cosmetic

    def _sync_all_dots() -> None:
        if not layer_ids:
            return
        _run_js(
            "(function(){var ls=%(ls)s;var d=new Set(%(d)s);ls.forEach(function(lid){"
            "var h=document.getElementById(lid);if(!h)return;"
            "h.querySelectorAll('.cb-pick-ghost[data-pick-idx]').forEach(function(g){"
            "if(d.has(g.getAttribute('data-pick-idx')))g.classList.add('cb-pick-ghost-dropped');"
            "else g.classList.remove('cb-pick-ghost-dropped');});});})();"
            % {"ls": json.dumps(layer_ids), "d": json.dumps([str(i) for i in _dropped_now()])}
        )

    def _counter_txt() -> str:
        # Count kept among the VISIBLE tiles only — keep_set may carry tile-less
        # row indices (out-of-bounds picks kept in a prior filter), which must not
        # inflate the counter past the tile count.
        ks = state["keep_set"]
        kept = len(all_idx) if ks is None else sum(1 for i in all_idx if i in ks)
        return f"kept {kept}/{len(all_idx)}"

    def _update_count_cell(total: int, fc) -> None:
        # Live-update this list's count cell in the rail table (shared `lst` dict).
        el = lst.get("_count_el")
        if el is None:
            return
        try:
            el.set_text(_list_count_text(total, fc))
        except Exception:
            pass  # rail torn down — best-effort cosmetic

    async def _commit_loop() -> None:
        """Auto-commit the current keep/drop to <slug>_filtered.star, serialized so
        rapid toggles can't race or hammer Lustre: a toggle arriving mid-write just
        flags `dirty` and the running loop re-writes the final state. Drops empty →
        the filtered star is discarded (revert to all-kept). Persists the kept count
        on the PickList so the table survives navigation."""
        if not star_path:
            return
        if commit["running"]:
            commit["dirty"] = True
            return
        commit["running"] = True
        try:
            while True:
                commit["dirty"] = False
                ks = state["keep_set"]
                dropped = (set(all_idx) - ks) if ks is not None else set()
                try:
                    if dropped:
                        res = await asyncio.to_thread(picks_filter.save_filtered_list, Path(star_path), set(dropped))
                        kept, total = int(res["kept"]), int(res["total"])
                    else:
                        await asyncio.to_thread(picks_filter.discard_filtered_list, Path(star_path))
                        kept = total = len(lst.get("picks") or [])
                except Exception:
                    logger.exception("keep/drop auto-commit failed for %s", star_path)
                    break
                fc = None if kept == total else kept
                pl = get_project_state().get_pick_list(lst["slug"], species_id, tomo_name)
                if pl is not None and pl.filtered_count != fc:
                    pl.filtered_count = fc
                    await get_state_service().save_project(force=True)
                lst["filtered_count"] = fc
                _update_count_cell(total, fc)
                if not commit["dirty"]:
                    break
        finally:
            commit["running"] = False

    async def _toggle(i: int) -> None:
        ks = state["keep_set"]
        if ks is None:
            ks = set(all_idx)  # materialize keep-all, then flip this one
            state["keep_set"] = ks
        kept = i not in ks  # state AFTER the toggle
        if kept:
            ks.add(i)
        else:
            ks.discard(i)
        t = tiles.get(i)
        if t is not None:
            if kept:
                t.classes(remove="cb-tile-dropped")
            else:
                t.classes(add="cb-tile-dropped")
        if layer_ids:
            _run_js(
                "(function(){var ls=%(ls)s;var idx='%(i)s';var drop=%(drop)s;ls.forEach(function(lid){"
                "var h=document.getElementById(lid);if(!h)return;"
                "h.querySelectorAll('.cb-pick-ghost[data-pick-idx=\"'+idx+'\"]').forEach(function(g){"
                "if(drop)g.classList.add('cb-pick-ghost-dropped');else g.classList.remove('cb-pick-ghost-dropped');"
                "});});})();" % {"ls": json.dumps(layer_ids), "i": i, "drop": "false" if kept else "true"}
            )
        counter.set_text(_counter_txt())
        await _commit_loop()  # auto-save the keep/drop → persists + feeds merge/extraction

    async def _on_reset() -> None:
        state["keep_set"] = None
        for t in tiles.values():
            t.classes(remove="cb-tile-dropped")
        _sync_all_dots()
        counter.set_text(_counter_txt())
        await _commit_loop()  # discards the filtered star + clears the kept count
        ui.notify("Reset — all picks kept", type="positive", timeout=1500)

    def _install_hover_bridge() -> None:
        """Bidirectional hover brushing for THIS list: hover a tile → glow its slab
        ghost-dot(s); hover a ghost-dot → glow it + highlight/scroll-to its tile.
        Delegated on the Particles section card (the stable ancestor of both the
        canvas and this sheet), grid re-resolved lazily — mirrors the auto gallery
        bridge, but the handler pair is stashed on the card and the prior one removed
        first, so re-renders don't stack duplicate listeners. No-op without the
        list's canvas dot-layers."""
        if not layer_ids:
            return
        _run_js(
            """
            setTimeout(function() {
                var gid = %(grid)s;
                var lids = %(layers)s;
                if (!document.getElementById(gid)) return;
                var root = document.getElementById(gid).closest('.cb-section-card') || document.body;
                function getGrid() { return document.getElementById(gid); }
                function layers() {
                    return lids.map(function(id) { return document.getElementById(id); }).filter(Boolean);
                }
                function ourGhost(el) {
                    var l = el.closest && el.closest('.cb-pick-layer');
                    return !!l && lids.indexOf(l.id) !== -1;
                }
                function ghostsFor(idx) {
                    var out = [];
                    layers().forEach(function(h) {
                        h.querySelectorAll('.cb-pick-ghost[data-pick-idx="' + idx + '"]')
                         .forEach(function(g) { out.push(g); });
                    });
                    return out;
                }
                function clearActive() {
                    layers().forEach(function(h) {
                        h.querySelectorAll('.cb-pick-ghost.cb-ghost-active')
                         .forEach(function(g) { g.classList.remove('cb-ghost-active'); });
                    });
                    var gr = getGrid();
                    if (gr) gr.querySelectorAll('.cb-tile-highlight')
                            .forEach(function(t) { t.classList.remove('cb-tile-highlight'); });
                }
                function isOurs(el) {
                    if (!el || !el.closest) return false;
                    var gr = getGrid();
                    var t = el.closest('.cb-gallery-tile[data-pick-idx]');
                    if (t && gr && gr.contains(t)) return true;
                    var gh = el.closest('.cb-pick-ghost[data-pick-idx]');
                    return !!(gh && ourGhost(gh));
                }
                if (root._cbListBridge) {
                    root.removeEventListener('mouseover', root._cbListBridge.over);
                    root.removeEventListener('mouseout', root._cbListBridge.out);
                }
                var over = function(e) {
                    if (!e.target.closest) return;
                    var gr = getGrid();
                    var t = e.target.closest('.cb-gallery-tile[data-pick-idx]');
                    if (t && gr && gr.contains(t)) {
                        var idx = t.getAttribute('data-pick-idx');
                        clearActive();
                        ghostsFor(idx).forEach(function(g) { g.classList.add('cb-ghost-active'); });
                        return;
                    }
                    var gh = e.target.closest('.cb-pick-ghost[data-pick-idx]');
                    if (gh && ourGhost(gh)) {
                        var gi = gh.getAttribute('data-pick-idx');
                        clearActive();
                        ghostsFor(gi).forEach(function(g) { g.classList.add('cb-ghost-active'); });
                        var g2 = getGrid();
                        if (g2) {
                            var tl = g2.querySelector('.cb-gallery-tile[data-pick-idx="' + gi + '"]');
                            if (tl) {
                                tl.classList.add('cb-tile-highlight');
                                var tr = tl.getBoundingClientRect(), gb = g2.getBoundingClientRect();
                                if (tr.top < gb.top || tr.bottom > gb.bottom)
                                    tl.scrollIntoView({block: 'nearest', behavior: 'smooth'});
                            }
                        }
                    }
                };
                var out = function(e) {
                    if (!e.target.closest) return;
                    var lv = e.target.closest('.cb-gallery-tile[data-pick-idx]') ||
                             e.target.closest('.cb-pick-ghost[data-pick-idx]');
                    if (!lv || !isOurs(lv)) return;
                    if (!isOurs(e.relatedTarget)) clearActive();
                };
                root._cbListBridge = {over: over, out: out};
                root.addEventListener('mouseover', over);
                root.addEventListener('mouseout', out);
            }, 60);
            """
            % {"grid": json.dumps(grid_id), "layers": json.dumps(layer_ids)}
        )

    from services.visualization.cutout_filters import get_filter_presets, keyed_path

    _apix = atlas_meta.get("apix")
    _apix_source = atlas_meta.get("apix_source") or "unknown"
    _list_filter_presets = [
        p for p in get_filter_presets() if p["key"] == "raw" or keyed_path(Path(atlas_path), p["key"]).exists()
    ]

    def _on_list_select(key: str) -> None:
        # Swap each tile's background-image to the chosen variant atlas, in place —
        # no rebuild, so the keep/drop handlers and dot sync survive the switch.
        url = _vis_asset_url(str(keyed_path(Path(atlas_path), key)))
        for t in tiles.values():
            try:
                t.style(add=f"background-image: url({url});")
            except Exception:
                pass  # tile torn down — best-effort

    with ui.element("div").classes("w-full").style("margin-top: 10px;"):
        with ui.row().classes("items-center gap-2").style("margin-bottom: 4px;"):
            _render_list_header(lst, sp, project_path)
            counter = ui.label(_counter_txt()).classes("cb-filter-counter")
            ui.space()
            _render_cutout_filter_controls(_list_filter_presets, _apix, _apix_source, _on_list_select)
            ui.button("Reset", icon="restart_alt", on_click=_on_reset).props("flat dense no-caps").tooltip(
                "Clear this list's keep/drop — revert to all picks kept"
            )
        ui.label("click a tile to keep/drop — saved automatically · hover to find it on the slab").style(
            "font-size: 9px; color: #94a3b8; margin-bottom: 4px;"
        )
        grid_el = ui.element("div").style("display: flex; flex-wrap: wrap; gap: 4px;")
        grid_el._props["id"] = grid_id
        with grid_el:
            for i in all_idx:
                pos = index.get(str(i))
                if not pos:
                    continue
                r, c = pos
                cls = "cb-gallery-tile" if _is_kept(i) else "cb-gallery-tile cb-tile-dropped"
                t = (
                    ui.element("div")
                    .classes(cls)
                    .style(
                        f"background-image: url({atlas_url}); background-size: {bg_w}px {bg_h}px; "
                        f"background-position: {-c * tile}px {-r * tile}px;"
                    )
                )
                t._props["title"] = f"#{i} · click: keep/drop"
                t._props["data-pick-idx"] = str(i)
                t.on("click", lambda _e, i=i: _toggle(i))
                tiles[i] = t
    _sync_all_dots()
    _install_hover_bridge()


def _render_list_extraction_bar(sp: dict, lst: dict, project_path: Path, refresh) -> None:
    """Slice C: per-list extraction status + action. Shows the DERIVED extraction badge
    (○ not extracted / ✓ extracted / ⚠ stale, from ``PickList.extraction_state()``) and
    an Extract / Re-extract button that subtomo-extracts THIS list (its ``_filtered``
    subset when present) into ``Curation/<species>/<tomo>/<slug>/``."""
    species_id = sp.get("species_id") or ""
    tomo_name = sp["row"]["tomo_name"]
    pl = get_project_state().get_pick_list(lst["slug"], species_id, tomo_name)
    est = pl.extraction_state() if pl is not None else ListExtractionState.NOT_EXTRACTED
    text, cls = _EXTRACTION_BADGE.get(est, ("", ""))
    with ui.row().classes("items-center gap-2 w-full").style("margin: 0 0 8px; padding-bottom: 6px;"):
        ui.icon("dataset", size="15px").classes("text-slate-400")
        ui.label("Extraction").classes("text-[11px] font-semibold text-slate-600")
        if text:
            ui.label(text).classes(cls).style("font-size: 11px; font-weight: 700;")
        ui.space()
        label = "Extract" if est == ListExtractionState.NOT_EXTRACTED else "Re-extract"
        btn = ui.button(label, icon="science", on_click=lambda: _handle_extract_list(sp, lst, project_path, refresh))
        btn.props("dense no-caps size=sm unelevated color=indigo")
        btn.tooltip("Subtomo-extract this list's kept picks for downstream refinement (one SLURM job)")


async def _handle_extract_list(sp: dict, lst: dict, project_path: Path, refresh) -> None:
    """Submit + track a per-list subtomo extraction (Slice C). Resolves the species'
    candidate optset + this list's curated star + the species subtomo params, fires
    ``backend.extract_pick_list``, watches the out dir for completion (the qsub wrapper's
    ``RELION_JOB_EXIT_*`` markers + the driver's ``result.json``), and records
    ``PickList.mark_extracted`` on success. SingleFlight-guarded; the watch runs in a
    BackgroundTask (no client context → persist by explicit ``project_path``, the W2 lesson)."""
    from backend import get_backend
    from services.visualization import picks_filter

    slug = lst["slug"]
    species_id = sp.get("species_id") or ""
    tomo_name = sp["row"]["tomo_name"]
    async with _curation_flight(f"extract:{species_id}:{tomo_name}:{slug}") as acquired:
        if not acquired:
            return
        backend = get_backend()
        if backend is None:
            ui.notify("Backend unavailable.", type="negative")
            return
        star = lst.get("path")
        if not star:
            ui.notify(f"'{lst.get('label')}' has no backing star to extract.", type="warning")
            return
        # Prefer the curated subset so extraction consumes the KEPT picks, not all of them.
        filtered = picks_filter.filtered_list_path(Path(star))
        list_star = str(filtered) if filtered.exists() else str(star)
        candidate_optset = Path(sp["job_dir"]) / "optimisation_set.star"
        if not candidate_optset.exists():
            ui.notify(
                "Species candidate optimisation_set.star not found — cannot extract.", type="negative", timeout=5000
            )
            return
        jm = sp.get("subtomo_jm")
        params = dict(
            box_size=int(getattr(jm, "box_size", 384) or 384),
            binning=float(getattr(jm, "binning", 1.0) or 1.0),
            crop_size=int(getattr(jm, "crop_size", 224) or 224),
            max_dose=float(getattr(jm, "max_dose", -1.0)),
            min_frames=int(getattr(jm, "min_frames", 1) or 1),
            do_stack2d=bool(getattr(jm, "do_stack2d", True)),
            do_float16=bool(getattr(jm, "do_float16", True)),
        )

        async def _run(progress_cb):
            progress_cb(0, 0, "submitting extraction…")
            res = await backend.extract_pick_list(
                project_path, candidate_optset, Path(list_star), tomo_name, species_id, slug, **params
            )
            if not res.get("success"):
                return res
            out_dir = Path(res["out_dir"])

            def _poll() -> tuple:
                import json as _json

                rj = out_dir / "result.json"
                if (out_dir / "RELION_JOB_EXIT_FAILURE").exists():
                    try:
                        return ("failed", _json.loads(rj.read_text()) if rj.exists() else {})
                    except Exception:
                        return ("failed", {})
                if (out_dir / "RELION_JOB_EXIT_SUCCESS").exists() or rj.exists():
                    try:
                        return ("done", _json.loads(rj.read_text()) if rj.exists() else {})
                    except Exception:
                        return ("done", {})
                return ("running", {})

            data: dict = {}
            for _ in range(360):  # ~60 min at a 10 s cadence
                status, d = await asyncio.to_thread(_poll)
                if status == "failed":
                    err = d.get("error") or "extraction job failed — see run.err in the list's out dir"
                    return {"success": False, "error": err}
                if status == "done":
                    data = d
                    break
                progress_cb(0, 0, "extracting subtomograms…")
                await asyncio.sleep(10)
            else:
                return {"success": False, "error": "extraction still running — check the SLURM job / tray"}
            if not data.get("ok"):
                return {"success": False, "error": data.get("error") or "extraction produced no usable result"}
            st = get_state_service().state_for(project_path)
            pl = st.get_pick_list(slug, species_id, tomo_name)
            if pl is not None:
                pl.mark_extracted(data["optimisation_set"], int(data.get("count", 0)))
                await get_state_service().save_project(project_path=project_path, force=True)
            return {"success": True, "count": int(data.get("count", 0))}

        from ui.background_task import BackgroundTask

        BackgroundTask(
            title=f"Extract · {lst.get('label') or slug}",
            subtitle=tomo_name,
            project_path=str(project_path),
            dedup_key=f"extract:{species_id}:{tomo_name}:{slug}",
        ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=True)
        ui.notify(f"Extraction submitted for '{lst.get('label') or slug}' — tracking in the task tray.", type="info")


async def _render_single_list_cutouts(sp: dict, lst: dict, project_path: Path, refresh) -> None:
    """Detail pane for ONE workbench list (manual/imported/merged): a read-only
    recon-sourced cutout sheet (these lists were never subtomo-extracted, so tiles
    are cut from the binned recon at each pick voxel) + the overlap/dedup panel for
    a merged list. Carved from the old per-list loop so the rail's detail pane can
    show a single selected list.

    The read-only disk probes (recon/star stat, atlas staleness, atlas-index read)
    run OFF the event loop via ``asyncio.to_thread`` so selecting a list doesn't
    freeze the whole UI on Lustre latency — a spinner shows until they return."""
    # Slice C: per-list extraction status + Extract/Re-extract action (shown for every
    # workbench list, even with no recon preview below).
    _render_list_extraction_bar(sp, lst, project_path, refresh)
    recon = (sp.get("row") or {}).get("vol_path")

    def _no_recon() -> None:
        with ui.element("div").classes("cb-empty"):
            ui.icon("image_not_supported", size="24px").classes("text-gray-400")
            ui.label("No reconstructed tomogram on disk — cutouts unavailable.").classes("text-xs")
            ui.label("This list's dots still overlay the slab canvas on the left.").classes(
                "text-[11px] italic text-gray-500"
            )
        if lst.get("list_type") == PickListType.MERGED:
            _render_clash_panel(lst, sp, project_path, refresh)

    if not recon:
        _no_recon()
        return
    species_id = sp.get("species_id") or ""
    tomo_name = sp["row"]["tomo_name"]
    box_px = _list_cutout_box_px(sp)
    atlas_path, index_path = _list_cutout_paths(project_path, species_id, tomo_name, lst["slug"])
    star_path = lst.get("path")

    # Spinner while the disk probes run off-loop (Lustre stat/read latency was the
    # "laggy on switch" freeze — it blocked the event loop mid-click). P3: on a warm
    # REVISIT the atlas index is already in memory and the keep-state derive is
    # memoized, so the probe returns near-instantly — skip the "loading cutouts…"
    # spinner that otherwise flashes on every open and reads as a full re-render.
    warm = str(index_path) in _ATLAS_INDEX_MEMO
    pending_box = None
    if warm:
        logger.info("list-cutouts[%s/%s]: atlas cache HIT — rendering without spinner", species_id, lst["slug"])
    else:
        with ui.element("div").classes("w-full").style("margin-top: 10px;") as pending_box:
            with ui.row().classes("items-center gap-2"):
                ui.spinner(size="16px", color="indigo-500")
                ui.label("loading cutouts…").style("font-size: 10px; color: #94a3b8;")

    def _probe() -> dict:
        recon_exists = Path(recon).exists()
        try:
            star_sig = int(Path(star_path).stat().st_mtime) if star_path and Path(star_path).exists() else 0
        except OSError:
            star_sig = 0
        sources = [Path(recon)] + ([Path(star_path)] if star_path else [])
        fresh = (
            atlas_path.exists()
            and index_path.exists()
            and not is_output_stale(atlas_path, sources)
            and _atlas_index_is_current(index_path)
        )
        # Existing keep/drop curation for this list (centered-Å match against
        # <slug>_filtered.star), derived off-loop here so the interactive sheet opens
        # already reflecting saved drops. Memoized by mtime (shared with the rail
        # count). None = no filter yet (all kept).
        keep = _memoized_keep_state(Path(star_path)) if star_path else None
        return {
            "recon_exists": recon_exists,
            "star_sig": star_sig,
            "atlas_meta": _read_atlas_index(index_path) if fresh else None,
            "index_exists": index_path.exists(),
            "keep": keep,
        }

    io = await asyncio.to_thread(_probe)
    if pending_box is not None:
        pending_box.delete()

    if not io["recon_exists"]:
        _no_recon()
        return
    atlas_meta = io["atlas_meta"]
    if atlas_meta is None:
        # Cold path: kick ONE background build (stays on-loop — it installs a
        # ui.timer) and fall through to the building/empty status.
        dedup_key = f"{species_id}:{tomo_name}:{lst['slug']}:{io['star_sig']}"
        if _auto_kick_list_cutouts(
            Path(recon),
            lst["picks"],
            star_path,
            atlas_path,
            index_path,
            box_px,
            project_path,
            refresh,
            dedup_key,
            apix_hint=(sp.get("entry") or {}).get("pixel_size_ang"),
        ):
            atlas_meta = _read_atlas_index(index_path)  # rare: built between probe and now
    if atlas_meta:
        _render_list_cutout_sheet(lst, sp, project_path, atlas_meta, str(atlas_path), io.get("keep"), refresh)
    else:
        # No tiles yet: a written index means the build ran (empty result);
        # no index means it's still in flight.
        _render_list_cutouts_status(lst, sp, project_path, building=not io["index_exists"])
    # Merged lists carry the overlap/dedup panel (the user-driven radius dedup).
    if lst.get("list_type") == PickListType.MERGED:
        _render_clash_panel(lst, sp, project_path, refresh)


def _dedup_default_radius(sp: dict) -> float:
    """Default overlap radius (Å) ≈ particle_diameter / 2 from the candidate-extract
    job; 100 Å when the diameter is unknown."""
    d = float(getattr(sp.get("jm"), "particle_diameter_ang", 0.0) or 0.0)
    return round(d / 2.0, 1) if d > 0 else 100.0


def _render_clash_panel(lst: dict, sp: dict, project_path: Path, refresh) -> None:
    """Overlap overview + 'Deduplicate' for a merged list. At the CHOSEN radius it
    shows how many picks clash; the user varies the radius and clicks Deduplicate to
    remove them (manual kept over auto). Nothing dedups automatically. Calm styling —
    informational, not an alarm."""
    from backend import get_backend

    star = lst.get("path")
    if not star:
        return
    species_id = sp.get("species_id") or ""
    tomo_name = sp["row"]["tomo_name"]

    with (
        ui.element("div")
        .classes("w-full")
        .style(
            "margin: 4px 0 2px; padding: 6px 8px; background: #fff7ed; border: 1px solid #fed7aa; border-radius: 6px;"
        )
    ):
        with ui.row().classes("items-center gap-2 w-full"):
            ui.icon("join_inner", size="14px").classes("text-orange-700")
            ui.label("Overlap").classes("text-[11px] font-semibold text-orange-800")
            radius_in = (
                ui.number(value=_dedup_default_radius(sp), step=1, min=0)
                .props("dense outlined suffix=Å debounce=600")
                .classes("text-xs")
                .style("width: 92px;")
                .tooltip("Two picks closer than this (Å) are treated as the same particle")
            )
            note = ui.label("checking overlaps…").classes("text-[11px] text-gray-600")
            ui.space()
            dedup_btn = (
                ui.button("Deduplicate", icon="cleaning_services", on_click=lambda: _do_dedup())
                .props("dense no-caps size=sm color=orange-7")
                .tooltip(
                    "Remove every pick within the radius of a higher-priority pick (manual kept over auto). "
                    "Rewrites this merged list — re-extract after."
                )
            )

        async def _recompute(_e=None):
            backend = get_backend()
            if backend is None:
                return
            r = float(radius_in.value or 0)
            stats = await backend.list_clash_stats(star, tomo_name, r)
            if not stats.get("success"):
                note.set_text("overlap check unavailable")
                return
            nt, nc, nr, na = stats["n_total"], stats["n_clashing"], stats["n_removed"], stats["n_after"]
            if nr <= 0:
                note.set_text(f"no overlaps at {r:g} Å · {nt} picks")
                note.classes(replace="text-[11px] text-emerald-700")
                dedup_btn.props("disable")
            else:
                note.set_text(f"{nc} of {nt} clash at {r:g} Å → dedup keeps {na}")
                note.classes(replace="text-[11px] text-orange-800 font-medium")
                dedup_btn.props(remove="disable")

        async def _do_dedup(_e=None):
            backend = get_backend()
            if backend is None:
                ui.notify("Backend unavailable.", type="negative")
                return
            r = float(radius_in.value or 0)
            res = await backend.deduplicate_pick_list(star, tomo_name, r)
            if not res.get("success"):
                ui.notify(f"Deduplicate failed: {res.get('error')}", type="negative")
                return
            state_obj = get_project_state()
            pl = state_obj.get_pick_list(lst["slug"], species_id, tomo_name)
            if pl is not None:
                pl.count = int(res.get("n_after", pl.count))
                state_obj.mark_dirty()
                # AWAIT (force) so the dedup'd count lands on disk — a fire-and-forget
                # create_task gets GC'd before it runs (same bug as the manual-list save).
                await get_state_service().save_project(force=True)
            ui.notify(
                f"Removed {res.get('n_removed', 0)} overlapping picks · {res.get('n_after', 0)} kept", type="positive"
            )
            refresh()

        radius_in.on_value_change(_recompute)
        import asyncio as _asyncio

        _asyncio.create_task(_recompute())


# _open_merge_dialog was removed 2026-06-11 — merging is now the co-located inline
# merge bar built in _render_list_rail (tick 2+ pills → name → Merge), no popup.
# See W3 in services/visualization/ARTIAX_BRIDGE_PLAN.md.


def _render_pick_layer(picks: list, color: str, dims: list, axis: str, layer_id: str, shape: str = "circle"):
    """Render one pick list's ghost-dot layer over a shared slab canvas.

    Carries a stable DOM `layer_id` and per-dot `data-pick-idx` plus a
    `.cb-pick-marker` so the gallery↔canvas hover bridge in
    `_render_gallery_body` can target this exact list's dots (the bridge
    does `getElementById(layer_id)` then scopes its querySelector to it).
    `shape` (circle/diamond/square/triangle) is the per-list glyph, applied on
    the layer so it cascades to every child dot. Returns the layer element so a
    checkbox can toggle its visibility."""
    x_dim = max(int(dims[0]), 1)
    y_dim = max(int(dims[1]), 1)
    z_dim = max(int(dims[2]), 1)
    # `--sp-color` cascades to every child .cb-pick-ghost (the restyle rule
    # reads it via var()), so the species color survives the !important dot
    # styling without per-dot inline overrides.
    layer = ui.element("div").classes(f"cb-pick-layer cb-shape-{shape}").style(f"--sp-color: {color};")
    layer._props["id"] = layer_id
    with layer:
        for p in picks:
            try:
                idx = int(p.get("i"))
                fx = max(0.0, min(1.0, float(p.get("x", 0)) / x_dim))
                if axis == "xy":
                    fv = 1.0 - max(0.0, min(1.0, float(p.get("y", 0)) / y_dim))
                else:
                    fv = 1.0 - max(0.0, min(1.0, float(p.get("z", 0)) / z_dim))
            except (TypeError, ValueError):
                continue
            dot = ui.element("div").classes("cb-pick-ghost")
            dot._props["data-pick-idx"] = str(idx)
            dot.style(f"left: {fx * 100:.3f}%; top: {fv * 100:.3f}%;")
        # Hover marker the bridge moves to the hovered pick (one per layer).
        ui.element("div").classes("cb-pick-marker")
    return layer


# ---------------------------------------------------------------------------
# Template Match size chips — reused by the per-species tab sanity strip.
# ---------------------------------------------------------------------------


def _resolve_recon_mrc_for_ts(project_state, project_path: Path, ts_name: str) -> tuple[Optional[Path], Optional[Path]]:
    """(recon_job_dir, reconstructed-tomogram MRC) for this TS, or Nones.

    Falls back to the project-level imported tomograms.star (PARTICLES-header import
    utility) for data-less / particle-only projects, which have no TS_RECONSTRUCT job."""
    rec = _find_job_by_type(project_state, JobType.TS_RECONSTRUCT)
    if rec:
        recon_job_dir = _job_dir_for(rec[0], rec[1], project_path)
        if not recon_job_dir:
            return None, None
        star_path = recon_job_dir / "tomograms.star"
    else:
        imported = project_state.imported_tomograms_star_path()
        if not imported:
            return None, None
        star_path = Path(imported)
        recon_job_dir = star_path.parent
    tomo_df = _read_tomograms_table(star_path)
    if tomo_df is None or "rlnTomoName" not in tomo_df.columns:
        return recon_job_dir, None
    match = tomo_df[tomo_df["rlnTomoName"].astype(str) == ts_name]
    if match.empty:
        return recon_job_dir, None
    mrc = _resolve_volume_for_3dmod(match.iloc[0], project_path)
    return recon_job_dir, (Path(mrc) if mrc else None)


def _read_pick_list_voxels(star_path: Path, dims: list, pixel_size: Optional[float]) -> list[dict]:
    """Read a centered-Å pick star → voxel-space picks ``[{i, x, y, z}]`` for the
    canvas overlay, using the binned ``dims`` + ``pixel_size`` already resolved in
    the render context (no MRC re-read per render). Returns ``[]`` if the file,
    its deps, or the centered-coord columns are unavailable — so a missing/changed
    list degrades to 'nothing drawn' rather than breaking the dashboard render."""
    if not star_path or not Path(star_path).exists() or not pixel_size or pixel_size <= 0:
        return []
    try:
        import starfile

        from services.visualization.coords import CENTERED_COLS, centered_angst_to_voxel

        data = starfile.read(star_path, always_dict=True)
        df = None
        for v in data.values():
            if hasattr(v, "columns") and all(c in v.columns for c in CENTERED_COLS):
                df = v
                break
        if df is None or len(df) == 0:
            return []
        coords = df[CENTERED_COLS].to_numpy(dtype=float)
        vox = centered_angst_to_voxel(coords, [int(dims[0]), int(dims[1]), int(dims[2])], float(pixel_size))
        return [{"i": i, "x": float(vox[i][0]), "y": float(vox[i][1]), "z": float(vox[i][2])} for i in range(len(vox))]
    except Exception as e:
        logger.warning("Could not read pick list %s: %s", star_path, e)
        return []


def _collect_pick_lists_for_species(sp: dict, project_state, ts_name: str) -> list[dict]:
    """The pick lists overlaid for one species on this TS, each render-ready:
    {slug, label, list_type, color, shape, picks, dims, visible}.

    The `auto` PyTOM list comes from picks.json (always present when picked).
    Workbench-authored lists (manual/imported/merged) come from the ProjectState
    registry — each backed by a centered-Å star whose coords are mapped into the
    same voxel space as the auto picks so they overlay on the shared canvas. The
    canvas/toggle/cutout machinery already iterates this list, so a new type is
    purely another entry here."""
    lists: list[dict] = []
    auto_picks = sp.get("picks") or []
    if auto_picks:
        lists.append(
            {
                "slug": "auto",
                "label": sp["label"],
                "list_type": PickListType.AUTO,
                "color": sp["color"],
                "shape": _glyph_for(PickListType.AUTO),
                "picks": auto_picks,
                "dims": sp["dims"],
                "visible": True,
                "filtered_count": sp.get("auto_kept_count"),
            }
        )
    species_id = sp.get("species_id") or ""
    if species_id:
        dims = sp.get("dims") or [1, 1, 1]
        pixel_size = (sp.get("entry") or {}).get("pixel_size_ang")
        for pl in project_state.get_pick_lists(species_id, ts_name):
            picks = _read_pick_list_voxels(Path(pl.path), dims, pixel_size)
            if not picks:
                # P4: a PERSISTED list that reads back as 0 picks must NOT be silently
                # dropped — that is exactly how a merged/manual list could vanish from
                # the rail (a coord/dims/apix regression making its star unreadable
                # looked identical to "no list"). Keep it in the rail (visible,
                # selectable, debuggable) and log the cause instead of skipping it.
                logger.warning(
                    "pick list %r (%s) for %s/%s read back 0 picks from %s — rendering empty",
                    pl.slug,
                    pl.list_type,
                    species_id,
                    ts_name,
                    pl.path,
                )
            # P2: the table count must match the cutout sheet, which derives kept/total
            # live from <slug>_filtered.star. pl.filtered_count is a cache that goes
            # stale (None) when the filter was committed in a prior session, so source
            # the count from the same star the sheet reads whenever it exists.
            filtered_count = pl.filtered_count
            if picks:
                keep = _memoized_keep_state(Path(pl.path))
                if keep is not None:
                    filtered_count = len(keep)
            else:
                filtered_count = None
            # Sync the persisted cache so PickList.extraction_state() (which reads
            # pl.filtered_count, not this local) compares the extracted count against the
            # SAME kept count the sheet + table show. Without this, a list filtered in a
            # prior session (filtered_count=None on disk) reads falsely STALE right after a
            # correct extraction of its kept subset. mark_dirty so the corrected count
            # persists for cross-session / aggregation reads that never re-render this panel.
            if pl.filtered_count != filtered_count:
                pl.filtered_count = filtered_count
                project_state.mark_dirty()
            lists.append(
                {
                    "slug": pl.slug,
                    "label": pl.label or pl.slug,
                    "list_type": pl.list_type,
                    "color": pl.color,
                    "shape": _glyph_for(pl.list_type),
                    "picks": picks,
                    "dims": dims,
                    "visible": pl.visible,
                    "path": pl.path,
                    "parent_slugs": pl.parent_slugs,
                    "filtered_count": filtered_count,
                }
            )
    return lists


def _collect_species_data_for_ts(project_state, project_path: Path, ts_name: str, refresh) -> list[dict]:
    """One entry per candidate-extract instance that has a row for this TS,
    with everything the Particles section needs (row, manifest, entry, picks,
    color, lists). Drives both the shared canvas overlay and the per-species tabs."""
    out: list[dict] = []
    for idx, (iid, jm) in enumerate(_candidate_extract_instances(project_state)):
        job_dir = _job_dir_for(iid, jm, project_path)
        if not job_dir:
            continue
        ce_rows = _collect_tomo_rows_for_instance(job_dir, project_path)
        row = next((r for r in ce_rows if r["tomo_name"] == ts_name), None)
        if row is None:
            continue
        # Lazy-generate previews + IMOD overlays for this species (idempotent;
        # refresh re-renders the dashboard when the background job lands).
        _auto_kick_preview_generation(iid, jm, job_dir, project_path, refresh)
        _auto_kick_imod_generation(iid, jm, job_dir, project_path, refresh)
        manifest = read_preview_manifest(job_dir) or {}
        entry = (manifest.get("tomograms") or {}).get(ts_name) or {}
        picks_data = _read_picks_json(Path(entry["picks_json"])) if entry.get("picks_json") else {}
        label = (manifest.get("template") or {}).get("species_name") or _split_species_id(iid) or iid
        # Resolve the subtomo job for THIS species (by species_id) so the
        # gallery's save-filter writes into the right job — the old lex-greatest
        # heuristic mis-targeted every species at one subtomo job.
        _, species_id = _resolve_species(project_state, jm, iid)
        sub_match = _matching_subtomo_instance(project_state, species_id)
        subtomo_job_dir = _job_dir_for(sub_match[0], sub_match[1], project_path) if sub_match else None
        # Auto list's curated kept count (the subtomo-gallery keep/drop), read off the
        # cheap reviewed sidecar so the table shows kept/total for auto like the rest.
        auto_kept_count = None
        if subtomo_job_dir:
            try:
                from services.visualization import picks_filter

                auto_kept_count = picks_filter.read_reviewed_counts(subtomo_job_dir).get(ts_name)
            except Exception:
                auto_kept_count = None
        # Prescan: auto-ingest a fresh ArtiaX save at the bundle's tomo-named path
        # so a curated list surfaces without an explicit Import click.
        _auto_kick_coords_ingest(job_dir, project_path, species_id, str(label), ts_name, refresh)
        sp_entry = {
            "idx": idx,
            "iid": iid,
            "jm": jm,
            "job_dir": job_dir,
            "species_id": species_id,
            "subtomo_job_dir": subtomo_job_dir,
            "subtomo_jm": sub_match[1] if sub_match else None,
            "auto_kept_count": auto_kept_count,
            "row": row,
            "manifest": manifest,
            "entry": entry,
            "label": str(label),
            "color": _SPECIES_OVERLAY_COLORS[idx % len(_SPECIES_OVERLAY_COLORS)],
            "picks": picks_data.get("picks") or [],
            "dims": picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1],
        }
        sp_entry["lists"] = _collect_pick_lists_for_species(sp_entry, project_state, ts_name)
        out.append(sp_entry)
    return out


def _eye_name(visible: bool) -> str:
    return "visibility" if visible else "visibility_off"


def _apply_pick_list_visibility(lst: dict) -> None:
    """Show/hide one pick list's canvas dot layers per its ``visible`` flag and
    keep that list's rail-chip eye glyph in sync. The dot layers (``_layer_els``)
    and the chip eye (``_eye_el``) are stashed on the list dict by the canvas /
    rail renderers, so this reaches both from either eye."""
    vis = lst.get("visible", True)
    for layer in lst.get("_layer_els") or []:
        if vis:
            layer.classes(remove="cb-pick-layer-hidden")
        else:
            layer.classes(add="cb-pick-layer-hidden")
    eye = lst.get("_eye_el")
    if eye is not None:
        eye.name = _eye_name(vis)


def _sync_species_master_eye(sp: dict) -> None:
    """Re-point a species' tab master-eye at whether ANY of its lists is visible,
    so toggling an individual chip eye keeps the master glyph honest."""
    eye = sp.get("_master_eye_el")
    if eye is None:
        return
    vis = any(lst.get("visible", True) for lst in (sp.get("lists") or []))
    sp["_master_visible"] = vis
    eye.name = _eye_name(vis)


def _render_species_master_eye(sp: dict, tab) -> None:
    """A master visibility eye inside a species tab: one click toggles ALL that
    species' canvas overlay layers. It lives on the tab strip (not the rail) so a
    species' overlay can be toggled even while another species' tab is open —
    what the retired cross-species 'Show picks' row gave. ``click.stop`` keeps the
    click from also switching tabs."""
    vis0 = any(lst.get("visible", True) for lst in (sp.get("lists") or []))
    sp["_master_visible"] = vis0
    with tab:
        eye = ui.icon(_eye_name(vis0), size="14px").classes("cb-eye cb-tab-eye")
        eye.tooltip("Show / hide all of this species' picks on the canvas")
    sp["_master_eye_el"] = eye

    def _toggle(_e, _sp=sp, _eye=eye):
        vis = not _sp.get("_master_visible", True)
        _sp["_master_visible"] = vis
        for lst in _sp.get("lists") or []:
            lst["visible"] = vis
            _apply_pick_list_visibility(lst)
        _eye.name = _eye_name(vis)

    eye.on("click.stop", _toggle)


# Slab height cap (vh) for the Particles canvas: the X/Y slab is capped at this
# fraction of viewport height, so the slab column's width = _SLAB_MAX_VH·aspect.
# Lower → narrower slabs → more width for the gallery/tabs column beside them.
_SLAB_MAX_VH = 60

# Hard ceiling on the slab column's share of the Particles row WIDTH. The vh cap
# above sets the slab width to _SLAB_MAX_VH·aspect, which on a typical monitor
# lands at ~half the row — so the gallery/rail column beside it only ever got the
# OTHER half, regardless of flex (this was the "gallery is half-width" bug: the two
# prior fixes tweaked vh + flex but never bounded the horizontal split). Capping the
# slab's width as a % of the row bounds that split directly, so the gallery/rail
# column always claims the rest. Tune to taste: lower → wider gallery.
_SLAB_MAX_PCT = 34


def _imported_wip_marker(text: str, tip: str) -> None:
    """A subtle red 'unverified / missing' marker with an explanatory tooltip.
    Per CLAUDE.md ('Surfacing uncertainty'): show a missing/ambiguous param, never
    silently default it."""
    with ui.row().classes("items-center gap-1").style("display:inline-flex;"):
        ui.icon("error_outline", size="13px").classes("text-red-500")
        ui.label(text).classes("font-mono text-red-600").tooltip(tip)


def _render_imported_species_row(sp_obj, project_state, ts_name: str) -> None:
    """One registered species' manual pick lists on this imported tomogram.

    Counts come straight from the PickList registry (pl.count) — independent of the
    tomogram's apix/dims, which are surfaced separately with their own markers (so a
    suspect/missing geometry never silently mis-states a pick count). P2.1 lists picks;
    P2.2 adds the canvas overlay, which is what actually needs apix + dims."""
    color = (
        getattr(sp_obj, "color", "")
        or _SPECIES_OVERLAY_COLORS[sum(map(ord, str(sp_obj.id))) % len(_SPECIES_OVERLAY_COLORS)]
    )
    lists = project_state.get_pick_lists(sp_obj.id, ts_name)
    with ui.element("div").style("display:flex; gap:10px; align-items:center; padding:3px 2px; font-size:11px;"):
        ui.icon("circle", size="10px").style(f"color:{color};")
        ui.label(str(sp_obj.name)).style("font-weight:600;")
        if lists:
            total = sum(int(getattr(pl, "count", 0) or 0) for pl in lists)
            ui.label(f"{len(lists)} list(s) · {total} picks").classes("font-mono text-gray-600")
        else:
            ui.label("no picks yet").classes("italic text-gray-400")


def _render_imported_particles_section(
    ts_name: str, project_state, project_path: Path, refresh, refresh_roster=None
) -> bool:
    """Particles section for imported tomograms (data-less / particle-only projects).

    A separate, deliberately-simple renderer: the candidate-extract canvas + gallery
    machinery assumes auto-pick data this source has none of, so we don't drive it
    blind. Shows the imported tomogram's geometry — with provenance markers, never a
    silent apix default (CLAUDE.md) — plus each registered species' manual pick lists.
    The 'register species & pick' launcher (P2.2) will mount here."""
    imported_star = project_state.imported_tomograms_star_path()
    if not imported_star:
        return False
    tomo_df = _read_tomograms_table(Path(imported_star))
    if tomo_df is None or "rlnTomoName" not in tomo_df.columns:
        return False
    match = tomo_df[tomo_df["rlnTomoName"].astype(str) == ts_name]
    if match.empty:
        return False
    row = match.iloc[0]

    # Geometry from the star (cheap — no MRC re-read on the event loop). apix is
    # surfaced WITH provenance: if it looks like the synthesize fallback (the recon
    # MRC header had no voxel_size and no override was set), flag it red rather than
    # presenting 1.0 A/px as if it were measured.
    # binning follows RELION's convention (absent ⇒ 1.0); a corrupt non-positive /
    # NaN value also falls back to 1 rather than poisoning the apix + dims math.
    try:
        binning = float(row.get("rlnTomoTomogramBinning", 1.0))
    except (TypeError, ValueError):
        binning = 1.0
    if not (binning > 0):
        binning = 1.0
    try:
        ts_px = float(row.get("rlnTomoTiltSeriesPixelSize", 0.0))
    except (TypeError, ValueError):
        ts_px = 0.0
    binned_apix = ts_px * binning if ts_px > 0 else 0.0

    def _binned_dim(col: str):
        try:
            return int(round(float(row[col]) / binning)) if col in row.index else None
        except (TypeError, ValueError):
            return None

    bx, by, bz = _binned_dim("rlnTomoSizeX"), _binned_dim("rlnTomoSizeY"), _binned_dim("rlnTomoSizeZ")
    dims_known = None not in (bx, by, bz)
    species = list(getattr(project_state, "species_registry", []) or [])

    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "particles"
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("scatter_plot", size="14px").classes("text-indigo-600")
            ui.label("Particles").classes("cb-section-title")
            ui.label("imported tomogram").classes("text-[10px] font-mono text-gray-500")
            ui.space()
            ui.label("manual picking · WIP").classes("text-[10px] font-mono text-amber-600").tooltip(
                "Particle-only project: pick manually in ArtiaX. Automated template matching on imported "
                "tomograms is not wired yet (it needs a per-tilt-series star)."
            )

        with ui.element("div").style(
            "display:flex; gap:16px; align-items:center; flex-wrap:wrap; padding:6px 2px; font-size:11px;"
        ):
            ui.label(ts_name).classes("font-mono").style("font-weight:600;")
            if dims_known:
                ui.label(f"{bx} x {by} x {bz} px").classes("font-mono text-gray-600")
            else:
                _imported_wip_marker(
                    "dimensions unavailable", "tomograms.star has no rlnTomoSizeX/Y/Z for this tomogram."
                )
            if abs(binning - 1.0) > 1e-6:
                ui.label(f"bin x{binning:g}").classes("font-mono text-gray-500")
            # `not (ts_px > 0)` catches 0 / negative / NaN — definitely missing.
            # A bare 1.0 A/px (binning 1) is AMBIGUOUS: it's the writer's fallback
            # when an MRC header lacks voxel_size, but could also be genuine — so we
            # surface it for confirmation rather than asserting either way.
            if not (ts_px > 0):
                _imported_wip_marker(
                    "pixel size unset",
                    "tomograms.star has no usable rlnTomoTiltSeriesPixelSize for this tomogram — set "
                    "'Pixel size' on the Import Tomograms job.",
                )
            elif abs(ts_px - 1.0) < 1e-6 and abs(binning - 1.0) < 1e-6:
                _imported_wip_marker(
                    f"{binned_apix:g} A/px · verify",
                    "Pixel size is exactly 1.0 A/px. If the recon MRC header lacked voxel_size this is the "
                    "import fallback and picks would be mis-scaled — set 'Pixel size' on the Import Tomograms "
                    "job. If 1.0 A/px is genuinely correct, ignore this.",
                )
            else:
                ui.label(f"{binned_apix:g} A/px").classes("font-mono text-gray-600")

        if not species:
            ui.label("No species registered yet — register one to start picking.").classes(
                "italic text-gray-500"
            ).style("padding:6px 2px; font-size:11px;")
        else:
            for sp_obj in species:
                _render_imported_species_row(sp_obj, project_state, ts_name)
    return True


def _render_particles_section(ts_name: str, project_state, project_path: Path, refresh, refresh_roster=None) -> bool:
    """Unified Particles section: a shared tomogram canvas with every species'
    picks overlaid (toggleable), plus a per-species tab carrying that species'
    gallery / scatter. Replaces the old per-species candidate-extract cards."""
    species_data = _collect_species_data_for_ts(project_state, project_path, ts_name, refresh)
    if not species_data:
        return False

    recon_job_dir, mrc_path = _resolve_recon_mrc_for_ts(project_state, project_path, ts_name)

    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "particles"
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("scatter_plot", size="14px").classes("text-indigo-600")
            ui.label("Particles").classes("cb-section-title")
            ui.label(f"{len(species_data)} species").classes("text-[10px] font-mono text-gray-500")
            ui.space()
            # Per-species generate controls (Render previews · Re-render · IMOD)
            # live in the panel toolbar, following the active tab; the canvas-wide
            # Invert switch is section-level (shared across species).
            admin_host = ui.row().classes("items-center gap-0 cb-section-admin")
            if mrc_path is not None:
                _render_invert_switch(card)

        def _show_admin_for(sp: dict) -> None:
            admin_host.clear()
            with admin_host:
                _render_species_admin_buttons(sp, project_path, refresh)

        _show_admin_for(species_data[0])

        # Two columns: slabs LEFT (always visible for inspection), galleries
        # RIGHT — so the user can hover a tile and watch its dot on the canvas
        # at the same time. Wraps on narrow viewports.
        with ui.element("div").classes("cb-particles-split"):
            canvas_col = ui.element("div").classes("cb-particles-canvas-col")
            # Width = the slab's natural height-capped width (X/Y height-capped at
            # _SLAB_MAX_VH), but never more than _SLAB_MAX_PCT% of the row — so the
            # gallery/rail column beside it always gets the majority instead of the
            # slab eating ~half the row by its vh·aspect width. All species share the
            # TS's reconstructed tomogram → take the first real dims for the aspect.
            cdims = next((sp["dims"] for sp in species_data if sp.get("dims") and tuple(sp["dims"]) != (1, 1, 1)), None)
            cx, cy = (max(int(cdims[0]), 1), max(int(cdims[1]), 1)) if cdims else (1, 1)
            canvas_col.style(f"width: min(1080px, calc({_SLAB_MAX_VH}vh * {cx} / {cy})); max-width: {_SLAB_MAX_PCT}%")
            with canvas_col:
                # Shared canvas: lazily renders the recon slab and overlays each
                # species' picks. Returns {iid: {"xy": layer_id, "xz": layer_id}}
                # so each tab's gallery cross-links to its own dots.
                canvas_layers = _render_particles_canvas(
                    species_data, recon_job_dir, mrc_path, ts_name, project_path, refresh
                )

            with ui.element("div").classes("cb-particles-tabs-col"):
                tab_objs: list[tuple[dict, object]] = []
                sp_by_iid = {s["iid"]: s for s in species_data}

                def _on_species_tab(e) -> None:
                    sp = sp_by_iid.get(e.value)
                    if sp is not None:
                        _show_admin_for(sp)

                tabs = (
                    ui.tabs()
                    .props("dense align=left indicator-color=indigo")
                    .classes("cb-species-tabs")
                    .on_value_change(_on_species_tab)
                )
                with tabs:
                    for sp in species_data:
                        # Name + a master-eye toggling all of this species' canvas
                        # overlays at once (replacing the retired cross-species
                        # "Show picks" row). A CSS ::before dot (driven by the inline
                        # --sp-color) ties each tab to its canvas overlay color.
                        tab = ui.tab(sp["iid"], label=sp["label"]).classes("cb-species-tab")
                        tab.style(f"--sp-color: {sp['color']};")
                        if any(lst.get("_layer_els") for lst in sp.get("lists") or []):
                            _render_species_master_eye(sp, tab)
                        tab_objs.append((sp, tab))
                with ui.tab_panels(tabs, value=tab_objs[0][1]).classes("w-full cb-species-panels"):
                    for sp, tab in tab_objs:
                        with ui.tab_panel(tab):
                            _render_species_tab_body(
                                sp, canvas_layers.get(sp["iid"]), project_path, refresh, refresh_roster
                            )
    return True


def _render_particles_canvas(
    species_data: list[dict],
    recon_job_dir: Optional[Path],
    mrc_path: Optional[Path],
    ts_name: str,
    project_path: Path,
    refresh,
) -> dict:
    """Maximized shared slab canvas with each species' picks overlaid as a
    color-coded, toggleable layer. Returns {iid: {"xy": layer_id, "xz":
    layer_id|None}} so each tab's gallery can cross-link to its dots."""
    layer_ids: dict[str, dict] = {}

    if recon_job_dir is None or mrc_path is None:
        ui.label("No reconstructed tomogram on disk — canvas unavailable; per-species galleries below.").classes(
            "cb-section-placeholder"
        )
        return layer_ids

    _auto_kick_recon_slabs(recon_job_dir, ts_name, mrc_path, project_path, refresh)
    xy_png, xz_png = _recon_slab_paths(recon_job_dir, ts_name)
    if not xy_png.exists():
        with ui.element("div").classes("cb-empty"):
            ui.spinner(size="26px", color="indigo-500")
            ui.label("Rendering tomogram slices…").classes("text-xs")
            ui.label("Auto-kicked in the background — the canvas fills when the slab PNG lands.").classes(
                "text-[11px] italic text-gray-500"
            )
        return layer_ids

    with_picks = [sp for sp in species_data if sp.get("lists")]
    if not with_picks:
        # Recon slab exists but nothing picked yet — clean slab, no overlay.
        with ui.element("div").classes("cb-recon-preview cb-recon-canvas").style(f"max-height: {_SLAB_MAX_VH}vh;"):
            ui.image(_vis_asset_url(str(xy_png)))
        return layer_ids

    dims = with_picks[0]["dims"]
    x_dim = max(int(dims[0]), 1)
    y_dim = max(int(dims[1]), 1)
    z_dim = max(int(dims[2]), 1)
    nonce = uuid.uuid4().hex[:8]

    # Visibility is driven by the per-tab master-eye + per-chip eyes (Slice B
    # item 1) — the old per-list "Show picks" checkbox row was retired. Both eyes
    # toggle each list's `_layer_els` (populated just below) through
    # `_apply_pick_list_visibility`.

    # X/Y and X/Z share ONE stack that fills the (per-tomo width-capped) canvas
    # column — each child is width:100% of the stack and derives its height from
    # its own aspect-ratio. The COLUMN's max-width (set in _render_particles_section
    # to min(1080px, _SLAB_MAX_VH·x/y), R1) caps the X/Y at ~_SLAB_MAX_VH tall while preserving the
    # tomogram aspect AND hugging the previews (no whitespace before the gallery);
    # the X/Z then reads as a proportional strip below it (height = width · z/x).
    stack = ui.element("div").classes("cb-canvas-stack")
    with stack:
        xy_host = ui.element("div").classes("cb-tomo-preview cb-recon-canvas")
        xy_host.style(f"aspect-ratio: {x_dim}/{y_dim};")
        with xy_host:
            ui.image(_vis_asset_url(str(xy_png)))
            for sp in with_picks:
                for lst in sp["lists"]:
                    lid = f"cb-pl-{nonce}-{sp['idx']}-{lst['slug']}-xy"
                    lst.setdefault("_layer_els", []).append(
                        _render_pick_layer(lst["picks"], lst["color"], lst["dims"], "xy", lid, lst["shape"])
                    )
                    lst["_lid_xy"] = lid
                    # Gallery↔canvas bridge targets the species' primary list (auto
                    # if present, else the first) — what the cutout gallery mirrors.
                    if lst["slug"] == "auto" or "xy" not in layer_ids.get(sp["iid"], {}):
                        layer_ids.setdefault(sp["iid"], {})["xy"] = lid

        if xz_png.exists():
            xz_host = ui.element("div").classes("cb-tomo-preview cb-recon-canvas")
            xz_host.style(f"aspect-ratio: {x_dim}/{z_dim}; margin-top: 6px;")
            with xz_host:
                ui.image(_vis_asset_url(str(xz_png)))
                for sp in with_picks:
                    for lst in sp["lists"]:
                        lid = f"cb-pl-{nonce}-{sp['idx']}-{lst['slug']}-xz"
                        lst.setdefault("_layer_els", []).append(
                            _render_pick_layer(lst["picks"], lst["color"], lst["dims"], "xz", lid, lst["shape"])
                        )
                        lst["_lid_xz"] = lid
                        if lst["slug"] == "auto" or "xz" not in layer_ids.get(sp["iid"], {}):
                            layer_ids.setdefault(sp["iid"], {})["xz"] = lid

    return layer_ids


def _tm_essentials_for_species(sp: dict) -> dict:
    """Resolve the Template-Match instance matched to this candidate-extract
    species (by species_id) and pull the interpretive essentials shown in the
    tab header: matched TM instance id, angular search θ, and symmetry."""
    state = get_project_state()
    species, species_id = _resolve_species(state, sp["jm"], sp["iid"])
    info: dict = {
        "species": species,
        "species_id": species_id,
        "tm_iid": None,
        "tm_jm": None,
        "theta": None,
        "sym": None,
    }
    for tm_iid, tm_jm in _template_match_instances(state):
        _, tm_sid = _resolve_species(state, tm_jm, tm_iid)
        if tm_sid == species_id:
            info["tm_iid"], info["tm_jm"] = tm_iid, tm_jm
            info["theta"] = getattr(tm_jm, "angular_search", None)
            info["sym"] = (getattr(species, "symmetry", None) if species else None) or getattr(tm_jm, "symmetry", None)
            break
    return info


async def _handle_curate_in_artiax(sp: dict, project_path: Path) -> None:
    """Per-tomo 'Curate in ArtiaX': export this (species, tomo)'s picks to a
    `.coords` + `.cxc`, then open the curation control center bound to this
    tomogram. The control center is status-first — it shows the live session's
    connection info + the load commands, or a Start button that preloads this
    tomogram. SingleFlight-guarded so repeated clicks prep only one bundle."""
    from backend import get_backend
    from ui.curation_session_dialog import open_curation_control_center

    tomo_name = sp["row"]["tomo_name"]
    async with _curation_flight(f"{sp.get('species_id')}:{tomo_name}") as acquired:
        if not acquired:
            return
        backend = get_backend()
        if backend is None:
            ui.notify("Backend unavailable.", type="negative")
            return
        job_dir = Path(sp["job_dir"])
        ui.notify(f"Preparing ArtiaX bundle for {tomo_name}…", type="info")
        bundle = await backend.prepare_curation_bundle(
            project_path,
            job_dir / "candidates.star",
            job_dir / "tomograms.star",
            tomo_name,
            sp.get("label") or sp.get("species_id") or "",
            species_id=sp.get("species_id") or "",
        )
        if not bundle.get("success"):
            ui.notify(f"Could not prepare picks for {tomo_name}: {bundle.get('error')}", type="negative")
            return
        bundle["tomo_name"] = tomo_name
        bundle["candidates_star"] = str(job_dir / "candidates.star")
        bundle["tomograms_star"] = str(job_dir / "tomograms.star")
        bundle["species_id"] = sp.get("species_id") or ""
        bundle["species_label"] = sp.get("label") or sp.get("species_id") or ""
        await open_curation_control_center(backend, project_path, bundle=bundle)


async def _handle_load_into_session(sp: dict, project_path: Path) -> None:
    """Per-tomo 'Load into running session': swap the user's ALREADY-running
    ChimeraX/ArtiaX to THIS (species, tomo) over the REST channel — the reuse path
    that avoids relaunching a viewer per tomogram (the session is per-user, found
    across all projects). No live session → point the user at 'Curate in ArtiaX'."""
    from backend import get_backend

    tomo_name = sp["row"]["tomo_name"]
    species_id = sp.get("species_id") or ""
    async with _curation_flight(f"loadinto:{species_id}:{tomo_name}") as acquired:
        if not acquired:
            return
        # This handler awaits a ~20 s load; during it the dashboard's periodic
        # main_area.clear() deletes the slot this coroutine was entered under, so a later
        # bare ui.notify dies with "parent element ... has been deleted". Capture the page
        # LAYOUT slot (never cleared) up front and route every notify through it; swallow
        # the residual race so a stale toast never surfaces a traceback.
        try:
            host = context.client.layout.default_slot
        except Exception:
            host = nullcontext()

        def _notify(msg: str, **kw) -> None:
            try:
                with host:
                    ui.notify(msg, **kw)
            except Exception:
                logger.info("load-into-session: dropped notify (slot gone): %s", msg)

        backend = get_backend()
        if backend is None:
            _notify("Backend unavailable.", type="negative")
            return
        active = await backend.find_active_curation_session_any()
        if not active:
            active = await backend.find_active_curation_session(project_path)
        if not active or not active.get("rest_port"):
            _notify(
                "No running ChimeraX session yet — click ‘Curate in ArtiaX’ to start one, then load tomograms into it.",
                type="warning",
                timeout=6000,
            )
            return

        # Confirm — `close session` wipes unsaved manual picks. Layout-parented so
        # the 4 s dashboard refresh can't clear the dialog mid-interaction.
        with host:
            with ui.dialog().props("persistent") as confirm, ui.card().classes("w-[26rem] max-w-full gap-2"):
                ui.label("Load into running session?").classes("text-sm font-bold")
                ui.label(
                    f"Swap the running ArtiaX (on {active.get('node') or '?'}) to {tomo_name} + its picks, clearing "
                    "what's open now. Any manual picks you haven't saved for the current tomogram would be lost."
                ).classes("text-[12px] text-gray-600")
                save_cb = ui.checkbox("Save my current picks first", value=True).props("dense").classes("text-[12px]")
                ui.label("crboost saves your open lists to the current tomogram's folder before switching.").classes(
                    "text-[10px] text-gray-400"
                )
                with ui.row().classes("w-full justify-end gap-2"):
                    ui.button("Cancel", on_click=lambda: confirm.submit(None)).props("flat dense no-caps")
                    ui.button("Load", color="indigo", on_click=lambda: confirm.submit(True)).props("dense no-caps")
        go = await confirm
        do_save = bool(save_cb.value) if go else False
        try:
            confirm.delete()
        except Exception:
            pass
        if not go:
            return

        job_dir = Path(sp["job_dir"])
        _notify(f"Loading {tomo_name} into the running session…", type="info")
        res = await backend.load_into_session(
            active,
            project_path,
            job_dir / "candidates.star",
            job_dir / "tomograms.star",
            tomo_name,
            sp.get("label") or species_id or "",
            species_id=species_id,
            save_first=do_save,
        )
        if res.get("success"):
            n = res.get("auto_count")
            _notify(
                f"Loaded {tomo_name}{f' ({n} picks)' if n is not None else ''} into the running session.",
                type="positive",
            )
        else:
            _notify(f"Load failed: {res.get('error') or 'unknown error'}", type="negative", timeout=7000)


async def _handle_open_list_in_artiax(sp: dict, lst: dict, project_path: Path) -> None:
    """Per-list 'Open in ArtiaX': preload a CHOSEN workbench list (its centered-Å
    star) into a curation session for another pass. Mirrors `_handle_curate_in_artiax`
    but exports the list's own picks (labelled by slug so its reference `.coords`
    is named apart from the user's save). Saving in ArtiaX yields a NEW `.coords`
    → re-import via the tab's 'Import picks'; this list's star is untouched."""
    from backend import get_backend
    from ui.curation_session_dialog import open_curation_control_center

    tomo_name = sp["row"]["tomo_name"]
    species_id = sp.get("species_id") or ""
    async with _curation_flight(f"openlist:{species_id}:{tomo_name}:{lst['slug']}") as acquired:
        if not acquired:
            return
        backend = get_backend()
        if backend is None:
            ui.notify("Backend unavailable.", type="negative")
            return
        star = lst.get("path")
        if not star:
            ui.notify(f"'{lst.get('label')}' has no backing file to open.", type="warning")
            return
        job_dir = Path(sp["job_dir"])
        ui.notify(f"Preparing {lst.get('label')} for ArtiaX…", type="info")
        bundle = await backend.prepare_curation_bundle(
            project_path,
            job_dir / "candidates.star",
            job_dir / "tomograms.star",
            tomo_name,
            sp.get("label") or species_id or "",
            species_id=species_id,
            source_star=Path(star),
            coords_label=lst["slug"],
        )
        if not bundle.get("success"):
            ui.notify(f"Could not prepare {lst.get('label')}: {bundle.get('error')}", type="negative")
            return
        bundle["tomo_name"] = tomo_name
        bundle["candidates_star"] = str(job_dir / "candidates.star")
        bundle["tomograms_star"] = str(job_dir / "tomograms.star")
        bundle["species_id"] = species_id
        bundle["species_label"] = sp.get("label") or species_id or ""
        bundle["source_star"] = str(star)
        bundle["coords_label"] = lst["slug"]
        await open_curation_control_center(backend, project_path, bundle=bundle)


async def _persist_manual_pick_list(result: dict, species_id: str, tomo_name: str, project_path: Path) -> int:
    """Upsert the `manual` PickList for this (species, tomo) from a backend import
    result and persist ProjectState — AWAITED with force=True so the registry
    actually lands on disk. A fire-and-forget `create_task(save_project())` was
    getting GC'd before it ran, leaving `pick_lists: []` in project_params.json and
    forcing a re-import on every reopen. Returns the imported pick count. UI-free so
    both the explicit-import click path and the prescan auto-ingest share it. One
    `manual` list per (species, tomo) — a re-import replaces it (the raw .coords are
    still archived per-import for provenance).

    `project_path` is REQUIRED — it resolves the real registry by path. The prescan
    auto-ingest runs in a BackgroundTask with NO client/tab context, where bare
    `get_project_state()` returns a blank throwaway; the add + save then silently
    no-opped and the manual list never surfaced (W2)."""
    # P5: label the list after the .coords file the user named in ArtiaX (its stem),
    # not a fixed "Manual (ArtiaX)". The `manual` slug stays stable for re-ingest;
    # only the display label tracks the source filename. Falls back when unknown.
    src_stem = Path(result.get("coords_source") or "").stem
    get_state_service().state_for(project_path).add_pick_list(
        PickList(
            slug="manual",
            label=src_stem or "Manual (ArtiaX)",
            list_type=PickListType.MANUAL,
            species_id=species_id,
            tomo_name=tomo_name,
            path=result["out_star"],
            count=int(result.get("count", 0)),
            color=_PICK_LIST_DEFAULT_COLOR.get(PickListType.MANUAL, "#00e676"),
            created_by=result.get("created_by", ""),
        )
    )
    await get_state_service().save_project(project_path=project_path, force=True)
    return int(result.get("count", 0))


async def _register_manual_pick_list(sp: dict, result: dict, refresh, project_path: Path) -> None:
    """Explicit-import click path: persist the `manual` list, toast, and refresh so
    the new diamond layer appears on the canvas."""
    tomo_name = sp["row"]["tomo_name"]
    count = await _persist_manual_pick_list(result, sp.get("species_id") or "", tomo_name, project_path)
    src = Path(result.get("coords_source", "")).name
    ui.notify(
        f"Imported {count} manual picks for {tomo_name}" + (f" (from {src})" if src else ""),
        type="positive",
        timeout=3000,
    )
    refresh()


_AUTO_INGESTED_COORDS: set[str] = set()

# Whether the user has a live ChimeraX+ArtiaX curation session right now. Polled at a
# slow cadence by the journey's _maybe_refresh (squeue is the source of truth) and read
# by the rail toolbox to color the 'Curate' button (gray = none / green = live).
_CURATION_SESSION_LIVE: dict = {"on": False}


def _pending_save_for_tomo(
    project_path: Path, species_label: str, species_id: str, tomo_name: str
) -> Optional[tuple[Path, float]]:
    """The ArtiaX `.coords` save to auto-ingest for THIS (species, tomo), or None.

    Saves land in the per-(species,tomo) `curation_dir`, so every `.coords` under it
    is THIS tomogram's by construction — the newest one that isn't a crboost export
    (`auto.coords` / `*_ref.coords`) is the user's save, no name/single-tomo heuristic
    needed (that ambiguity was the old per-species cross-tomo bleed). Returns
    (path, mtime) of the newest qualifying save, for the caller's guards."""
    from services.visualization import artiax_bridge

    d = artiax_bridge.curation_dir(project_path, tomo_name, species_id=species_id, species_label=species_label)
    if not d.is_dir():
        return None
    saves: list[tuple[Path, float]] = []
    for c in d.glob("*.coords"):
        if c.name == "auto.coords" or c.name.endswith("_ref.coords"):
            continue  # crboost's reference exports, not the user's save
        try:
            saves.append((c, c.stat().st_mtime))
        except OSError:
            continue
    if not saves:
        return None
    saves.sort(key=lambda t: t[1], reverse=True)
    return saves[0]


def _auto_kick_coords_ingest(
    job_dir: Path, project_path: Path, species_id: str, species_label: str, tomo_name: str, refresh
) -> None:
    """Prescan: if the user saved an ArtiaX `.coords` for this (species, tomo) — see
    `_pending_save_for_tomo` for how an arbitrarily-named save is safely attributed —
    and it's newer than the registered `manual` list (or there's none yet), ingest it
    in the background so the list appears without a click. An mtime-keyed dedup set +
    a created_at guard make it idempotent; the Import button stays for out-of-tree /
    unattributable saves. Safe to call every render."""
    if not species_id:
        return
    pending = _pending_save_for_tomo(project_path, species_label, species_id, tomo_name)
    if pending is None:
        logger.info("coords-prescan[%s/%s]: no attributable ArtiaX .coords save in bundle", species_label, tomo_name)
        return
    coords, mtime = pending
    key = f"{species_id}:{tomo_name}:{mtime}"
    if key in _AUTO_INGESTED_COORDS:
        logger.info("coords-prescan[%s]: save already ingested this session (mtime %.0f)", tomo_name, mtime)
        return
    pl = get_project_state().get_pick_list("manual", species_id, tomo_name)
    if pl is not None and pl.created_at is not None and pl.created_at.timestamp() >= mtime:
        logger.info(
            "coords-prescan[%s]: manual list already current (created %.0f ≥ save %.0f)",
            tomo_name,
            pl.created_at.timestamp(),
            mtime,
        )
        return  # the registered manual list already reflects this (or a newer) save
    _AUTO_INGESTED_COORDS.add(key)
    logger.info("coords-prescan[%s]: ingesting %s (mtime %.0f) in background", tomo_name, coords.name, mtime)

    async def _run(progress_cb):
        from backend import get_backend

        backend = get_backend()
        if backend is None:
            return {"success": False, "error": "no backend"}
        progress_cb(0, 0, "ingesting ArtiaX save…")
        result = await backend.import_curation_picks(
            project_path, job_dir / "tomograms.star", tomo_name, species_label, species_id, coords_path=coords
        )
        if result.get("success"):
            await _persist_manual_pick_list(result, species_id, tomo_name, project_path)
        return result

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title=f"Ingest ArtiaX save · {tomo_name}",
        subtitle=species_label or species_id,
        project_path=str(project_path),
        dedup_key=f"coords-ingest:{species_id}:{tomo_name}:{mtime}",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)


async def _handle_import_curation_picks(sp: dict, project_path: Path, refresh) -> None:
    """Per-tomo 'Import picks': find the .coords the user saved in ArtiaX (any
    filename, newest first), convert → the tomo's manual.star, register a `manual`
    PickList. If nothing is found in the curation dir, prompt for an explicit
    path (ArtiaX's save dialog may default anywhere). SingleFlight-guarded."""
    from backend import get_backend

    tomo_name = sp["row"]["tomo_name"]
    species_id = sp.get("species_id") or ""
    species_label = sp.get("label") or species_id or ""
    async with _curation_flight(f"import:{species_id}:{tomo_name}") as acquired:
        if not acquired:
            return
        backend = get_backend()
        if backend is None:
            ui.notify("Backend unavailable.", type="negative")
            return
        job_dir = Path(sp["job_dir"])
        result = await backend.import_curation_picks(
            project_path, job_dir / "tomograms.star", tomo_name, species_label, species_id
        )
        if not result.get("success"):
            if result.get("error") == "no_coords_found":
                _open_manual_coords_path_dialog(sp, project_path, refresh)
                return
            ui.notify(f"Import failed: {result.get('error')}", type="negative", timeout=4000)
            return
        await _register_manual_pick_list(sp, result, refresh, project_path)


def _open_manual_coords_path_dialog(sp: dict, project_path: Path, refresh) -> None:
    """Fallback when no saved .coords was auto-found: let the user paste the full
    path to the file they saved in ArtiaX (we don't control where its save dialog
    defaults). Imports via the same backend path + registers the manual list."""
    from backend import get_backend

    tomo_name = sp["row"]["tomo_name"]
    species_id = sp.get("species_id") or ""
    species_label = sp.get("label") or species_id or ""
    job_dir = Path(sp["job_dir"])
    with ui.dialog() as dialog, ui.card().classes("w-[34rem] max-w-full gap-2"):
        ui.label(f"Import ArtiaX picks — {tomo_name}").classes("text-base font-bold")
        ui.label(
            "No saved .coords was found in this project's curation dirs. Paste the full path to the "
            ".coords you saved from ArtiaX (any filename)."
        ).classes("text-xs text-gray-600")
        path_in = ui.input("path to .coords").props("dense outlined").classes("w-full font-mono text-xs")

        async def _do_import():
            p = (path_in.value or "").strip()
            if not p:
                ui.notify("Enter a path", type="warning")
                return
            backend = get_backend()
            if backend is None:
                ui.notify("Backend unavailable.", type="negative")
                return
            result = await backend.import_curation_picks(
                project_path, job_dir / "tomograms.star", tomo_name, species_label, species_id, coords_path=Path(p)
            )
            if not result.get("success"):
                ui.notify(f"Import failed: {result.get('error')}", type="negative", timeout=4000)
                return
            dialog.close()
            await _register_manual_pick_list(sp, result, refresh, project_path)

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat")
            ui.button("Import", icon="download", color="indigo", on_click=_do_import).props("no-caps")
    dialog.open()


def _render_species_admin_buttons(sp: dict, project_path: Path, refresh) -> None:
    """The three per-species preview/overlay generate controls as icon buttons —
    Render previews (gen-missing) · Re-render all (cache-bypass) · (Re)generate
    IMOD overlays. Rendered into the Particles section title bar (next to the
    canvas-wide Invert switch), following the active species tab — so they sit in
    the panel toolbar, not crammed into the tab body."""
    iid, jm, job_dir = sp["iid"], sp["jm"], sp["job_dir"]
    imod_dir = job_dir / "vis" / "imodPartRad"
    has_imod_models = imod_dir.exists() and any(imod_dir.glob("*.mod"))
    gen_missing_btn = (
        ui.button(
            icon="auto_fix_high",
            on_click=lambda: _handle_generate_for_instance(
                iid, jm, job_dir, project_path, False, gen_missing_btn, refresh
            ),
        )
        .props("flat dense round size=sm")
        .classes("text-purple-600")
        .tooltip("Render previews for tomograms whose manifest entry is missing or stale.")
    )
    regen_btn = (
        ui.button(
            icon="refresh",
            on_click=lambda: _handle_generate_for_instance(iid, jm, job_dir, project_path, True, regen_btn, refresh),
        )
        .props("flat dense round size=sm")
        .classes("text-gray-500")
        .tooltip("Re-render all: bypass cache and regenerate every tomogram's preview from scratch.")
    )
    imod_btn = (
        ui.button(
            icon="scatter_plot",
            on_click=lambda: _handle_generate_imod_for_instance(iid, jm, job_dir, project_path, imod_btn, refresh),
        )
        .props("flat dense round size=sm")
        .classes("text-blue-600")
        .tooltip("Regenerate IMOD .mod overlays" if has_imod_models else "Generate IMOD .mod overlays")
    )


def _render_species_tab_body(
    sp: dict, layer_ids: Optional[dict], project_path: Path, refresh, refresh_roster=None
) -> None:
    """One species' tab: a horizontal pick-list rail ABOVE the gallery/detail (so
    the short rail doesn't leave dead space beside the tall gallery), with the
    per-species admin controls hoisted to the panel toolbar. The rail lists every
    pick list (auto + manual/imported/merged) as a chip — swatch · count · type
    tag · extraction badge; clicking a chip drives the detail below (auto →
    subtomo gallery/scatter cross-linked to this species' canvas dots via
    `layer_ids`; a workbench list → its recon cutout sheet). 3dmod at the bottom."""
    tm_info = _tm_essentials_for_species(sp)
    if sp["row"]["status"] == "missing-volume":
        ui.label("No reconstructed tomogram on disk for 3dmod — picks plot still works.").classes(
            "text-[11px] text-amber-700 italic"
        )

    # Always present an `auto` entry so the PyTOM section (incl. its zero-picks /
    # errored / generating states) stays reachable even when there are no auto
    # picks (the collector omits a 0-pick auto list to keep the canvas overlay clean).
    lists = list(sp.get("lists") or [])
    if not any(lst["slug"] == "auto" for lst in lists):
        lists.insert(
            0,
            {
                "slug": "auto",
                "label": sp.get("label") or "auto",
                "list_type": PickListType.AUTO,
                "color": sp.get("color") or "#6366f1",
                "shape": _glyph_for(PickListType.AUTO),
                "picks": sp.get("picks") or [],
                "dims": sp.get("dims") or [1, 1, 1],
                "visible": True,
                "filtered_count": sp.get("auto_kept_count"),
            },
        )

    key = (sp.get("species_id") or "", sp["row"]["tomo_name"])
    slugs = {lst["slug"] for lst in lists}
    default_slug = "auto" if "auto" in slugs else (lists[0]["slug"] if lists else None)
    cur = _SELECTED_LIST_SLUG.get(key, default_slug)
    if cur not in slugs:
        cur = default_slug
    sel = {"slug": cur}
    chip_els: dict[str, object] = {}

    with ui.element("div").classes("cb-workbench-split"):
        rail_host = ui.element("div").classes("cb-list-rail-host")
        detail_host = ui.element("div").classes("cb-list-detail-host")

    async def _render_detail() -> None:
        detail_host.clear()
        lst = next((x for x in lists if x["slug"] == sel["slug"]), None)
        with detail_host:
            await _render_list_detail(sp, lst, layer_ids, project_path, refresh, refresh_roster)

    async def _select(slug: str) -> None:
        if slug == sel["slug"] or slug not in slugs:
            return
        if sel["slug"] in chip_els:
            chip_els[sel["slug"]].classes(remove="selected")
        sel["slug"] = slug
        _SELECTED_LIST_SLUG[key] = slug
        if slug in chip_els:
            chip_els[slug].classes(add="selected")
        await _render_detail()

    with rail_host:
        _render_list_rail(
            sp,
            lists,
            project_path,
            refresh,
            tm_info=tm_info,
            selected_slug=sel["slug"],
            on_select=_select,
            chip_els=chip_els,
        )
    # The detail pane renders one tick later via a once-timer: _render_detail is
    # async now (its workbench-list branch probes disk off-loop), so it can't be
    # called inline from this sync builder — schedule it onto the event loop.
    ui.timer(0.05, _render_detail, once=True)

    # 3dmod copy-command at the BOTTOM of the tab (Slice B item 5) — below the
    # rail + detail, out of the old header expansion.
    _render_3dmod_section(sp["row"])


def _render_list_eye(lst: dict, sp: dict) -> None:
    """Per-list visibility eye on a rail chip: toggles just this list's canvas dot
    layers. ``click.stop`` so toggling visibility doesn't also select the chip;
    the species tab's master-eye is re-synced after each toggle."""
    vis0 = lst.get("visible", True)
    eye = ui.icon(_eye_name(vis0), size="14px").classes("cb-eye")
    eye.tooltip("Show / hide this list on the canvas")
    lst["_eye_el"] = eye

    def _toggle(_e, _lst=lst, _sp=sp):
        _lst["visible"] = not _lst.get("visible", True)
        _apply_pick_list_visibility(_lst)
        _sync_species_master_eye(_sp)

    eye.on("click.stop", _toggle)


def _attach_auto_chip_tooltip(el, sp: dict, tm_info: dict) -> None:
    """Rich hover tooltip for the pytom (auto) chip's type tag: the per-tomo
    auto-pick stats + the template-match run params that used to clutter the tab
    header inline (Slice B item 3). Two labeled sections so it's clear the stats
    describe the auto pick SET and the TM line the template-match RUN."""
    row = sp["row"]
    entry = sp.get("entry") or {}
    diameter = float(getattr(sp["jm"], "particle_diameter_ang", 0.0) or 0.0)
    score_field = (sp.get("manifest") or {}).get("score_field")
    tm_bits: list[str] = []
    if tm_info.get("tm_iid"):
        tm_bits.append(str(tm_info["tm_iid"]))
    if tm_info.get("theta"):
        tm_bits.append(f"θ {tm_info['theta']}°")
    if tm_info.get("sym"):
        tm_bits.append(f"sym {tm_info['sym']}")
    if diameter:
        tm_bits.append(f"Ø {diameter:.0f} Å")
    with el:
        with ui.tooltip().classes("cb-chip-tooltip"):
            ui.label("Auto pick set (PyTOM)").classes("cb-tt-head")
            ui.label(f"{row['position_label']} · {row['tomo_name']}").classes("cb-tt-line")
            stat = f"N = {row['n_picks']}"
            if row.get("score_range"):
                stat += f" · CC {row['score_range'][0]:.3f}–{row['score_range'][1]:.3f}"
            if entry.get("score_mean") is not None:
                stat += f" · mean {entry['score_mean']:.3f}"
            ui.label(stat).classes("cb-tt-line")
            if score_field:
                ui.label(f"score field: {score_field}").classes("cb-tt-sub")
            if tm_bits:
                ui.separator().classes("cb-tt-sep")
                ui.label("Template-match run").classes("cb-tt-head")
                ui.label("  ·  ".join(tm_bits)).classes("cb-tt-line")


def _list_count_text(total: int, filtered_count: Optional[int]) -> str:
    """Table count cell text: 'kept/total' when a keep/drop filter is committed for
    this list, else just the total. `filtered_count` is None (no filter) or the kept
    count; equal-to-total is treated as no effective filter."""
    if filtered_count is not None and filtered_count != total:
        return f"{filtered_count}/{total}"
    return str(total)


def _render_list_rail(
    sp: dict, lists: list[dict], project_path: Path, refresh, *, tm_info: dict, selected_slug, on_select, chip_els: dict
) -> None:
    """The pick-list subpanel header: a compact aligned TABLE (header + one row per
    list: merge-check · swatch · name · count(kept/total) · authoritative-radio ·
    extracted-mark · visibility eye) on the left + a vertical action toolbox (Curate /
    Load / Import) on the right. Every row shares one grid template so the columns line
    up under the header. The auto (pytom) row's name carries a hover tooltip with its
    pick stats + template-match essentials. The authoritative radio is one-per-(species,
    tomo) — clicking it sets which list downstream extraction/aggregation consume.
    Clicking a row selects it → drives the detail; the eye, the auth radio and the
    merge-check use click.stop so they don't also select. Ticking 2+ rows reveals an
    INLINE merge bar (name → Merge). `chip_els` is filled {slug: row-element} so
    selection can re-highlight without rebuilding the table."""
    from backend import get_backend

    state_obj = get_project_state()
    species_id = sp.get("species_id") or ""
    species_label = sp.get("label") or species_id or ""
    tomo_name = sp["row"]["tomo_name"]
    key = (species_id, tomo_name)
    _MERGE_SELECT.setdefault(key, set())
    merge_boxes: dict[str, object] = {}

    def _box_style(checked: bool) -> str:
        base = "width:13px; height:13px; border-radius:3px; cursor:pointer; flex-shrink:0;"
        return base + (
            " background:#6366f1; border:1.5px solid #6366f1;"
            if checked
            else " background:transparent; border:1.5px solid #cbd5e1;"
        )

    def _source_for(slug: str):
        # Each ticked list contributes its KEPT subset to the merge — the user's
        # keep/drop must NOT bleed unselected picks into a merge:
        #   • auto  → particles_filtered.star (the curated subtomo set; it carries
        #     centered-Å coords + rlnTomoName, so the merge reads exactly the kept
        #     auto picks for this tomo) when filtered, else the full candidates.star.
        #   • workbench → <slug>_filtered.star when the cutout sheet committed drops,
        #     else the full list star.
        from services.visualization import picks_filter

        if slug == "auto":
            sub = sp.get("subtomo_job_dir")
            if sub and picks_filter.has_filtered_set(Path(sub)):
                return {"path": str(Path(sub) / picks_filter.PARTICLES_FILTERED_NAME), "type": "auto", "slug": "auto"}
            return {"path": str(Path(sp["job_dir"]) / "candidates.star"), "type": "auto", "slug": "auto"}
        lst = next((x for x in lists if x["slug"] == slug), None)
        if not lst or not lst.get("path"):
            return None
        lt = lst.get("list_type")
        filtered = picks_filter.filtered_list_path(Path(lst["path"]))
        path = str(filtered) if filtered.exists() else lst["path"]
        return {"path": path, "type": (lt.value if hasattr(lt, "value") else str(lt)), "slug": slug}

    def _update_merge_bar() -> None:
        n = len(_MERGE_SELECT.get(key, set()))
        merge_bar.style(f"display: {'flex' if n >= 2 else 'none'}; align-items:center; gap:6px; margin-top:6px;")
        merge_btn.set_text(f"Merge {n} lists" if n >= 2 else "Merge")

    def _clear_merge() -> None:
        _MERGE_SELECT[key] = set()
        for b in merge_boxes.values():
            b.style(_box_style(False))
        _update_merge_bar()

    def _toggle_merge(slug: str) -> None:
        selset = _MERGE_SELECT.setdefault(key, set())
        selset.discard(slug) if slug in selset else selset.add(slug)
        box = merge_boxes.get(slug)
        if box is not None:
            box.style(_box_style(slug in selset))
        _update_merge_bar()

    async def _do_inline_merge() -> None:
        chosen = [s for s in (_source_for(sl) for sl in _MERGE_SELECT.get(key, set())) if s]
        if len(chosen) < 2:
            ui.notify("Tick at least 2 lists to merge.", type="warning")
            return
        backend = get_backend()
        if backend is None:
            ui.notify("Backend unavailable.", type="negative")
            return
        # NAME → slug: re-using a name replaces that merge (upsert); a new name makes a
        # distinct merged list (own slug → own star, chip, curation), so no clobber.
        raw_name = (name_in.value or "").strip() or "Merged"
        slug = f"merged__{_fs_slug(raw_name)}"
        res = await backend.merge_pick_lists(
            project_path,
            species_id,
            species_label,
            tomo_name,
            [{"path": c["path"], "type": c["type"]} for c in chosen],
            out_slug=slug,
        )
        if not res.get("success"):
            ui.notify(f"Merge failed: {res.get('error')}", type="negative")
            return
        get_project_state().add_pick_list(
            PickList(
                slug=slug,
                label=raw_name,
                list_type=PickListType.MERGED,
                species_id=species_id,
                tomo_name=tomo_name,
                path=res["out_star"],
                count=int(res.get("count", 0)),
                color=_PICK_LIST_DEFAULT_COLOR.get(PickListType.MERGED, "#ff6d00"),
                parent_slugs=[c["slug"] for c in chosen],
                created_by=getattr(backend, "username", ""),
            )
        )
        # Persist by explicit project_path (not the client-context default) so the
        # merged list survives a restart even if this runs without a resolvable
        # client state — the same contract the manual-list persist proved out (P4).
        await get_state_service().save_project(project_path=project_path, force=True)
        _MERGE_SELECT[key] = set()  # consumed
        _SELECTED_LIST_SLUG[key] = slug  # land on the new merge
        ui.notify(f"Created '{raw_name}' — {res.get('count', 0)} picks from {len(chosen)} lists", type="positive")
        refresh()

    # Authoritative-list selector (one per species,tomo): which list downstream
    # extraction/aggregation consume. Default 'auto'. Radio-style — exactly one on.
    auth_state = {"slug": state_obj.get_authoritative_slug(species_id, tomo_name)}
    auth_icons: dict[str, object] = {}

    async def _set_authoritative(slug: str) -> None:
        if slug == auth_state["slug"]:
            return
        st = get_project_state()
        st.set_authoritative_slug(species_id, tomo_name, slug)
        await get_state_service().save_project(force=True)
        auth_state["slug"] = slug
        for s, ic in auth_icons.items():
            if ic is None:
                continue
            on = s == slug
            ic.name = "radio_button_checked" if on else "radio_button_unchecked"
            ic.classes(add="cb-auth-on" if on else "cb-auth-off", remove="cb-auth-off" if on else "cb-auth-on")
        label = next((x["label"] for x in lists if x["slug"] == slug), slug)
        ui.notify(f"Authoritative → {label} — downstream extraction/aggregation will use it", type="positive")

    with ui.element("div").classes("cb-list-top"):
        # The list TABLE: one aligned row per pick list — [merge-check · swatch · name ·
        # count(kept/total) · authoritative-radio · extracted-mark · eye]. A row click
        # selects it → drives the detail gallery; the merge-check, the auth radio and the
        # eye use click.stop so they don't also select. Every row shares the .cb-ltable-row
        # grid template, so the columns line up under the header.
        with ui.element("div").classes("cb-ltable"):
            with ui.element("div").classes("cb-ltable-row cb-ltable-head"):
                ui.element("div")  # merge-check col
                ui.element("div")  # swatch col
                ui.label("list").classes("cb-ltable-h-name")
                ui.label("picks").classes("cb-ltable-h-num")
                ui.label("auth").classes("cb-ltable-h-cell").tooltip("Authoritative downstream list (one per tomogram)")
                ui.label("ext").classes("cb-ltable-h-cell").tooltip("Subtomo-extracted state")
                ui.label("path").classes("cb-ltable-h-cell").tooltip("Copy the full path to this list's backing file")
                ui.element("div")  # eye col
            for lst in lists:
                slug = lst["slug"]
                row = ui.element("div").classes("cb-ltable-row")
                chip_els[slug] = row
                if slug == selected_slug:
                    row.classes(add="selected")
                row.on("click", lambda e, s=slug: on_select(s))
                with row:
                    with ui.element("div").classes("cb-ltable-cell"):
                        mbox = ui.element("div").style(_box_style(slug in _MERGE_SELECT[key]))
                        mbox.tooltip("Tick to include this list in a merge")
                        mbox.on("click.stop", lambda e, s=slug: _toggle_merge(s))
                        merge_boxes[slug] = mbox
                    ui.element("div").classes(f"cb-ltable-swatch cb-swatch-{lst['shape']}").style(
                        f"background: {lst['color']};"
                    )
                    name = ui.label(lst["label"]).classes("cb-ltable-name")
                    if slug == "auto":
                        name.style("cursor: help;")
                        _attach_auto_chip_tooltip(name, sp, tm_info)
                    total = len(lst.get("picks") or [])
                    cnt = ui.label(_list_count_text(total, lst.get("filtered_count"))).classes("cb-ltable-count")
                    cnt.tooltip("kept / total picks after keep-drop curation")
                    lst["_count_el"] = cnt  # so the cutout sheet can live-update it on keep/drop
                    with ui.element("div").classes("cb-ltable-cell"):
                        on = slug == auth_state["slug"]
                        ic = ui.icon("radio_button_checked" if on else "radio_button_unchecked", size="15px").classes(
                            "cb-auth " + ("cb-auth-on" if on else "cb-auth-off")
                        )
                        ic.tooltip("Authoritative list — downstream tools consume this one. Click to set.")
                        ic.on("click.stop", lambda e, s=slug: _set_authoritative(s))
                        auth_icons[slug] = ic
                    with ui.element("div").classes("cb-ltable-cell"):
                        if slug != "auto":
                            pl = state_obj.get_pick_list(slug, species_id, tomo_name)
                            est = pl.extraction_state() if pl is not None else ListExtractionState.NOT_EXTRACTED
                            text, cls = _EXTRACTION_BADGE.get(est, ("", ""))
                            if text:
                                # Symbol only in the table (○/✓/⚠); full label on hover.
                                ui.label(text.split(" ", 1)[0]).classes(f"cb-ltable-badge {cls}").tooltip(text)
                    with ui.element("div").classes("cb-ltable-cell"):
                        # P6: copy the full path to this list's backing file (auto →
                        # candidates.star; workbench → its star). Tooltip shows it; the
                        # click copies. click.stop so copying doesn't also select the row.
                        copy_path = (
                            str(Path(sp["job_dir"]) / "candidates.star") if slug == "auto" else (lst.get("path") or "")
                        )
                        if copy_path:
                            cbtn = (
                                ui.button(icon="content_copy")
                                .props("flat dense round size=sm")
                                .classes("cb-info-copy")
                                .tooltip(copy_path)
                            )
                            cbtn.on(
                                "click.stop",
                                lambda e, v=copy_path: (
                                    ui.clipboard.write(v),
                                    ui.notify("Copied path", type="positive", timeout=800),
                                ),
                            )
                    with ui.element("div").classes("cb-ltable-cell"):
                        if lst.get("_layer_els"):
                            _render_list_eye(lst, sp)
        # The per-(species,tomo) curation actions, pulled OUT of the table into a
        # vertical side toolbox (Curate in ArtiaX / Load into session / Import).
        with ui.element("div").classes("cb-list-toolbox"):
            # Curate button reflects live-session state: muted gray when no ChimeraX
            # session is up, green when one is running (polled into _CURATION_SESSION_LIVE).
            _sess_live = _CURATION_SESSION_LIVE.get("on", False)
            (
                ui.button(icon="view_in_ar", on_click=lambda: _handle_curate_in_artiax(sp, project_path))
                .props("flat dense round size=sm")
                .classes("cb-curate-live" if _sess_live else "cb-curate-off")
                .tooltip(
                    "Curate in ArtiaX — a session is running; open this tomogram + its picks in it"
                    if _sess_live
                    else "Curate in ArtiaX — start a ChimeraX + ArtiaX session for this tomogram + its picks"
                )
            )
            (
                ui.button(icon="swap_horiz", on_click=lambda: _handle_load_into_session(sp, project_path))
                .props("flat dense round size=sm color=indigo")
                .tooltip("Load into running session — swap the live ArtiaX to this tomogram (reuse one session)")
            )
            (
                ui.button(icon="download", on_click=lambda: _handle_import_curation_picks(sp, project_path, refresh))
                .props("flat dense round size=sm")
                .classes("text-slate-500")
                .tooltip("Import picks — ingest a .coords you saved in ArtiaX as a manual pick list")
            )
    # Inline merge bar — co-located replacement for the merge popup. A SIBLING of the
    # table+toolbox row (both children of the full-width rail_host block → it stacks
    # BELOW them); hidden until 2+ rows are ticked, shown/labeled by _update_merge_bar.
    merge_bar = (
        ui.element("div").classes("cb-merge-bar").style("display:none; align-items:center; gap:6px; width:100%;")
    )
    with merge_bar:
        ui.icon("join_inner", size="16px").classes("text-indigo-600").tooltip(
            "Union the ticked lists into a named merged list (dedup overlaps after, in its own panel)"
        )
        name_in = (
            ui.input(placeholder="merge name").props("dense outlined").classes("text-xs").style("max-width:150px;")
        )
        merge_btn = ui.button("Merge", on_click=_do_inline_merge).props("dense no-caps unelevated color=indigo size=sm")
        ui.button(icon="close", on_click=lambda: _clear_merge()).props("flat dense round size=sm").classes(
            "text-slate-400"
        ).tooltip("Clear merge selection")
    _update_merge_bar()


async def _render_list_detail(
    sp: dict, lst: Optional[dict], layer_ids: Optional[dict], project_path: Path, refresh, refresh_roster
) -> None:
    """Detail pane for the selected rail chip: the auto list shows the status-aware
    subtomo gallery / scatter; a workbench list shows its recon cutout sheet (whose
    disk probes run off-loop, hence async)."""
    if lst is None:
        with ui.element("div").classes("cb-empty"):
            ui.icon("inbox", size="28px").classes("text-gray-400")
            ui.label("No pick lists yet for this species.").classes("text-xs")
        return
    if lst["slug"] == "auto":
        _render_species_auto_section(sp, layer_ids, project_path, refresh, refresh_roster)
    else:
        await _render_single_list_cutouts(sp, lst, project_path, refresh)


def _render_species_auto_section(
    sp: dict, layer_ids: Optional[dict], project_path: Path, refresh, refresh_roster=None
) -> None:
    """The status-aware AUTO (PyTOM) gallery for a species tab: the subtomo
    cutout gallery when extracted, else the scatter fallback, or an
    error/empty/spinner state. Factored out of `_render_species_tab_body` so the
    workbench lists' cutouts render after it without being skipped by its early
    returns."""
    row, manifest, entry = sp["row"], sp["manifest"], sp["entry"]
    status = row["status"]
    if status == "errored":
        with ui.element("div").classes("cb-empty"):
            ui.icon("error_outline", size="28px").classes("text-red-500")
            ui.label("Render error: " + (row.get("error") or "unknown")).classes("text-xs text-red-600")
        return
    if status == "zero-picks":
        _render_zero_picks_empty_state(manifest, sp["label"])
        return
    if status not in ("ok", "missing-volume"):
        with ui.element("div").classes("cb-empty"):
            ui.spinner(size="28px", color="indigo-500")
            ui.label("Generating preview for this tilt-series…").classes("text-xs")
            ui.label("Auto-kicked in the background — the page refreshes when the manifest lands.").classes(
                "text-[11px] italic text-gray-500"
            )
        return

    has_atlas = bool(entry.get("cutout_atlas") and entry.get("cutout_index"))
    if has_atlas:
        atlas_meta = _read_atlas_index(Path(entry["cutout_index"]))
        if atlas_meta is not None:
            xy_id = (layer_ids or {}).get("xy") or ""
            xz_id = (layer_ids or {}).get("xz") or ""
            gallery_id = f"cb-gallery-{uuid.uuid4().hex[:8]}"
            _render_gallery_body(
                row,
                entry,
                manifest,
                atlas_meta,
                xy_id,
                xz_id,
                gallery_id,
                bool(xy_id),
                bool(xz_id),
                sp["job_dir"],
                sp.get("subtomo_job_dir"),
                refresh_roster,
            )
            return
    # No subtomo cutouts yet — scatter fallback (its own mini X/Y + X/Z plots).
    _render_picks_scatter_section(row, entry, manifest)


def _render_zero_picks_empty_state(manifest: dict, species_name: Optional[str]) -> None:
    """Empty state for tomograms PyTOM processed but where no candidate
    exceeded the cutoff. Shows the species template + score field so users
    can tell at a glance "this species got nothing here" — distinct from the
    generic "still generating" spinner the no-preview branch falls through to.
    """
    template_block = manifest.get("template") or {}
    thumb_path = template_block.get("thumb_path")
    species_label = species_name or template_block.get("species_name") or "this species"
    score_field = manifest.get("score_field") or "rlnLCCmax"
    summary = manifest.get("summary") or {}
    zero_n = len(summary.get("zero_picks") or [])
    ok_n = len(summary.get("ok") or [])
    total = zero_n + ok_n
    with ui.element("div").classes("cb-empty"):
        with ui.row().classes("items-center gap-4 justify-center w-full flex-wrap"):
            if thumb_path:
                ui.image(_vis_asset_url(thumb_path)).style("width: 96px; height: 96px;").classes(
                    "rounded border border-gray-200 bg-gray-50 object-contain"
                )
            with ui.column().classes("gap-1 items-start"):
                ui.label(f"0 picks above cutoff for {species_label}").classes("text-sm font-semibold text-amber-700")
                ui.label(f"PyTOM processed this tilt-series but no {score_field} value cleared the cutoff.").classes(
                    "text-[11px] text-gray-700"
                )
                if total:
                    ui.label(f"{zero_n} of {total} TS in this job came back empty.").classes(
                        "text-[11px] italic text-gray-500"
                    )
                ui.label(
                    "Check the picking cutoff, template chirality (TomoHand), "
                    "or template appropriateness for this species."
                ).classes("text-[10px] italic text-gray-500").style("max-width: 480px;")


def _render_3dmod_section(row: dict) -> None:
    """Render the 3dmod copy-command. Always shows the full `3dmod <vol>
    <mod>` form so the user can copy a ready-to-paste invocation; status
    indicator next to the field reports whether the .mod overlay is
    on disk yet (green) or still being generated (amber)."""
    if not row.get("vol_path"):
        return
    mod = row.get("mod_path")
    mod_exists = bool(row.get("mod_exists"))
    if mod:
        cmd = f"3dmod {row['vol_path']} {mod}"
        if mod_exists:
            status_icon, status_color, status_tip = "check_circle", "text-emerald-600", "IMOD overlay ready."
        else:
            status_icon, status_color = "hourglass_top", "text-amber-600"
            status_tip = (
                "IMOD overlay being generated in the background. The command above is "
                "copyable already — by the time you paste, the .mod file should be in place. "
                "Picks will appear as circles over the tomogram."
            )
    else:
        cmd = f"3dmod {row['vol_path']}"
        status_icon, status_color, status_tip = "info", "text-gray-400", "Open volume only."
    with ui.row().classes("w-full items-center gap-1"):
        ui.label("3dmod").classes("text-[9px] uppercase font-bold text-gray-400 w-12")
        ui.input(value=cmd).props("dense outlined readonly hide-bottom-space").classes(
            "text-xs font-mono flex-1"
        ).style("min-width: 0;")
        ui.icon(status_icon, size="14px").classes(status_color).tooltip(status_tip)
        ui.button(
            icon="content_copy",
            on_click=lambda c=cmd: (ui.clipboard.write(c), ui.notify("Copied", type="positive", timeout=800)),
        ).props("flat dense round size=sm").classes("text-gray-500 hover:text-gray-800").tooltip("Copy 3dmod command")


def _render_reference_strip(
    picks: list, cutout_index: dict, atlas_url: str, cols: int, rows: int, manifest: dict
) -> None:
    """Calibration row above the main gallery: template tile + lowest-score picks.

    Two pedagogical anchors: on the left, what the picker thinks "particle"
    looks like (the template's central X/Y slice through the same percentile
    pipeline); on the right, the worst-scoring picks in this tomogram — the
    practical noise floor that survived the cutoff. The user evaluates
    medium-score tiles in between against both extremes.
    """
    template_block = (manifest or {}).get("template") if isinstance(manifest, dict) else None
    template_thumb_path = template_block.get("thumb_path") if template_block else None
    template_url = _vis_asset_url(template_thumb_path) if template_thumb_path else None

    available = sorted((int(k) for k in cutout_index.keys()), reverse=True)
    worst_n = min(4, len(available))
    worst_indices = available[:worst_n]

    if not template_url and not worst_indices:
        return

    DISPLAY_TILE_PX = 96
    bg_w = cols * DISPLAY_TILE_PX
    bg_h = rows * DISPLAY_TILE_PX

    with ui.element("div").classes("cb-reference-strip"):
        if template_url:
            with ui.element("div").classes("cb-ref-group"):
                ui.label("Template").classes("cb-ref-label")
                with ui.element("div").classes("cb-ref-tiles"):
                    tile = ui.element("div").classes("cb-template-tile")
                    tile.style(f"background-image: url({template_url});")
                    apix = template_block.get("pixel_size_ang") if template_block else None
                    name = template_block.get("species_name") if template_block else None
                    tip_parts = [str(name or "template")]
                    if apix:
                        tip_parts.append(f"{apix:.2f} Å/px")
                    box = template_block.get("box_px") if template_block else None
                    if box:
                        tip_parts.append(f"box {box}")
                    tile._props["title"] = " · ".join(tip_parts)
                    with tile:
                        ui.html("REF", sanitize=False).classes("cb-tile-rank")
        if template_url and worst_indices:
            ui.element("div").classes("cb-ref-divider")
        if worst_indices:
            with ui.element("div").classes("cb-ref-group"):
                ui.label(f"Lowest scores · noise floor ({worst_n})").classes("cb-ref-label")
                with ui.element("div").classes("cb-ref-tiles"):
                    for n_from_bottom, pick_idx in enumerate(worst_indices):
                        pos = cutout_index.get(str(pick_idx))
                        if not pos:
                            continue
                        r, c = pos
                        bg_x = -c * DISPLAY_TILE_PX
                        bg_y = -r * DISPLAY_TILE_PX
                        style = (
                            f"background-image: url({atlas_url}); "
                            f"background-size: {bg_w}px {bg_h}px; "
                            f"background-position: {bg_x}px {bg_y}px;"
                        )
                        pick = picks[pick_idx] if 0 <= pick_idx < len(picks) else None
                        tile = ui.element("div").classes("cb-noise-tile")
                        tile.style(style)
                        tip = [f"#{pick_idx} (low score reference)"]
                        if pick and pick.get("score") is not None:
                            tip.append(f"score={pick['score']:.4f}")
                        tile._props["title"] = " · ".join(tip)
                        with tile:
                            ui.html(f"L{n_from_bottom + 1}", sanitize=False).classes("cb-tile-rank")
                            if pick and pick.get("score") is not None:
                                ui.html(f"{pick['score']:.3f}", sanitize=False).classes("cb-tile-score")


def _render_peek_skeleton() -> dict:
    """3D peek section: copyable `3dmod <peek>` command for the selected pick.

    The peek is a small subvolume extracted from the reconstructed tomogram
    on click (lazily — most users don't 3dmod every pick). Lives below the
    hover card; mirrors the per-tomogram 3dmod section's styling so the
    user sees it as the per-pick analogue.
    """
    refs: dict = {"current_pick": None}
    ui.label("3D peek").classes("cb-section-title mt-2")
    with ui.row().classes("w-full items-center gap-1"):
        ui.label("3dmod").classes("text-[9px] uppercase font-bold text-gray-400 w-12")
        cmd = (
            ui.input(value="(shift-click a tile to extract a subvolume)")
            .props("dense outlined readonly hide-bottom-space")
            .classes("text-xs font-mono flex-1")
            .style("min-width: 0;")
        )
        status = (
            ui.icon("touch_app", size="14px")
            .classes("text-gray-400")
            .tooltip("Click a tile to extract its 3D subvolume; then copy this command and run in a terminal with X11.")
        )
        copy_btn = (
            ui.button(
                icon="content_copy",
                on_click=lambda: (
                    ui.clipboard.write(refs["cmd_input"].value),
                    ui.notify("Copied", type="positive", timeout=800),
                ),
            )
            .props("flat dense round size=sm")
            .classes("text-gray-500 hover:text-gray-800")
            .tooltip("Copy 3dmod command")
        )
    ui.label("In 3dmod: open Image → Slicer for the X/Y, X/Z, Y/Z triptych centered on the pick.").classes(
        "text-[10px] text-gray-500 italic"
    )
    refs["cmd_input"] = cmd
    refs["status_icon"] = status
    refs["copy_btn"] = copy_btn
    return refs


async def _trigger_peek_for_pick(
    pick_idx: int,
    picks: list,
    tomo_mrc: Optional[str],
    peek_dir: Optional[Path],
    pixel_size_ang: Optional[float],
    particle_diameter_ang: Optional[float],
    peek_refs: dict,
    state: dict,
) -> None:
    """Extract the subvolume around the clicked pick, populate the 3dmod cmd.

    Stale-result guard: if the user clicks several tiles in quick succession,
    only the latest selection's extraction populates the UI — we track
    `state["selected_idx"]` and skip the update if it changed by the time
    extraction finishes."""
    cmd = peek_refs.get("cmd_input")
    status = peek_refs.get("status_icon")
    if cmd is None or status is None:
        return
    if not tomo_mrc or peek_dir is None or not (0 <= pick_idx < len(picks)):
        cmd.set_value("(no tomogram volume on disk)")
        status.props("name=error_outline").classes(replace="text-red-500")
        return
    pick = picks[pick_idx]
    try:
        x = int(pick.get("x"))
        y = int(pick.get("y"))
        z = int(pick.get("z"))
    except (TypeError, ValueError):
        cmd.set_value("(pick coords missing)")
        return

    # Box ≈ 3× particle diameter in tomogram px; clamp so user can navigate
    # context without dragging gigabytes around. Fallback to 48 px half (≈96
    # cube) when we can't compute — works for most particles at bin 4 / bin 8.
    if pixel_size_ang and particle_diameter_ang and pixel_size_ang > 0:
        diameter_px = particle_diameter_ang / pixel_size_ang
        half_box = int(round(diameter_px * 1.5))
    else:
        half_box = 48
    half_box = max(24, min(128, half_box))

    out_path = peek_dir / f"pick_{pick_idx:05d}.mrc"
    cmd.set_value(f"extracting subvolume for pick #{pick_idx}…")
    status.props("name=hourglass_top").classes(replace="text-amber-600")

    import asyncio as _asyncio

    from services.visualization.preview_render import extract_pick_subvolume

    if not out_path.exists():
        result = await _asyncio.to_thread(extract_pick_subvolume, Path(tomo_mrc), x, y, z, half_box, out_path)
    else:
        result = out_path  # cache hit

    if state.get("selected_idx") != pick_idx:
        return  # user clicked elsewhere — discard stale result

    if result is None:
        cmd.set_value(f"(extraction failed for pick #{pick_idx})")
        status.props("name=error_outline").classes(replace="text-red-500")
        status.tooltip("Subvolume extraction failed; check server logs.")
        return
    cmd.set_value(f"3dmod {out_path}")
    status.props("name=check_circle").classes(replace="text-emerald-600")
    status.tooltip(
        f"Subvolume ready: half_box={half_box} px around (x={x}, y={y}, z={z}). "
        "Copy the command and run in a terminal with X11 forwarding."
    )


def _render_gallery_body(
    row: dict,
    entry: dict,
    manifest: dict,
    atlas_meta: dict,
    xy_host_id: str,
    xz_host_id: str,
    gallery_id: str,
    has_xy: bool,
    has_xz: bool,
    ce_job_dir: Optional[Path],
    subtomo_job_dir: Optional[Path],
    refresh_roster=None,
) -> None:
    picks_json_path = entry.get("picks_json")
    picks_data = (
        _read_picks_json(Path(picks_json_path)) if picks_json_path else {"picks": [], "tomo_dims_xyz_px": [0, 0, 0]}
    )
    picks = picks_data.get("picks", [])
    tomo_dims = picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1]
    pixel_size_ang = entry.get("pixel_size_ang")

    atlas_url = _vis_asset_url(entry["cutout_atlas"])
    cols = int(atlas_meta.get("cols", 8))
    rows = int(atlas_meta.get("rows", 1))
    cutout_index = atlas_meta.get("index", {})
    failures = entry.get("cutout_failures") or atlas_meta.get("failures") or []

    # Display-filter variants (raw + lowpass/bandpass). Each variant is a sibling
    # atlas PNG that shares this exact tile index, so switching filters just swaps
    # the atlas URL the tiles + noise-reference tiles point at — no re-layout. apix
    # is read from the cutout file's OWN header (subtomo and recon use different
    # bins), so the Å→px conversion in the unit toggle/tooltip is always correct.
    from services.visualization.cutout_filters import get_filter_presets

    cutout_variants = entry.get("cutout_variants") or {}
    cutout_apix = entry.get("cutout_apix")
    cutout_apix_source = entry.get("cutout_apix_source") or "unknown"
    _filter_presets = [p for p in get_filter_presets() if p["key"] in cutout_variants]

    def _variant_atlas_url(key: str) -> str:
        v = cutout_variants.get(key)
        if v and v.get("atlas"):
            return _vis_asset_url(v["atlas"])
        return _vis_asset_url(entry["cutout_atlas"])

    def _on_filter_select(key: str) -> None:
        # Swap the atlas the tiles + noise-reference tiles point at, then re-render
        # (positions are identical across filters — only the pixels differ).
        nonlocal atlas_url
        atlas_url = _variant_atlas_url(key)
        _refresh_reference_strip()
        _refresh_grid()

    x_dim = max(int(tomo_dims[0]), 1)
    y_dim = max(int(tomo_dims[1]), 1)
    z_dim = max(int(tomo_dims[2]), 1)
    pick_xy_frac: dict[int, list[float]] = {}
    pick_xz_frac: dict[int, list[float]] = {}
    for k in cutout_index:
        try:
            i = int(k)
        except (TypeError, ValueError):
            continue
        if 0 <= i < len(picks):
            p = picks[i]
            fx = max(0.0, min(1.0, float(p.get("x", 0)) / x_dim))
            fy_top = max(0.0, min(1.0, 1.0 - float(p.get("y", 0)) / y_dim))
            fz_top = max(0.0, min(1.0, 1.0 - float(p.get("z", 0)) / z_dim))
            pick_xy_frac[i] = [fx, fy_top]
            pick_xz_frac[i] = [fx, fz_top]

    # ce_job_dir + subtomo_job_dir are passed in from the species data, resolved
    # per-species. They used to be guessed here — ce via path arithmetic that
    # landed on <ce>/vis (the "Could not read candidates … from …/vis" bug), and
    # subtomo via a lex-greatest heuristic that mis-targeted every species at one
    # job. Both are now correct for any number of registered species.
    ts_name = row.get("tomo_name") or ""
    from services.visualization import picks_filter

    # Degenerate picks: candidates that produced no cutout (no subtomo match /
    # render failed). Surfaced as greyed tiles at the end of the grid so the
    # gallery's tile count vs pick index is transparent to the user.
    degenerate_indices: list[int] = []
    for f in failures:
        try:
            degenerate_indices.append(int(f.get("i")))
        except (TypeError, ValueError):
            continue
    n_picks_total = len(picks)
    all_pick_indices: set[int] = set()
    for k in cutout_index.keys():
        try:
            all_pick_indices.add(int(k))
        except (TypeError, ValueError):
            continue

    # Load any existing curation. derive_keep_state_for_ts returns None when
    # there's no filtered file at all (= "all kept implicitly"); we materialize
    # that to a None sentinel in state so the user sees an unmarked starting
    # point, and only mutate to a concrete set once they actually click a tile.
    initial_keep_set: Optional[set[int]] = None
    if subtomo_job_dir is not None and ce_job_dir is not None and ts_name:
        try:
            initial_keep_set = picks_filter.derive_keep_state_for_ts(
                subtomo_job_dir, ce_job_dir, ts_name, n_picks_total
            )
        except Exception as e:
            logger.warning("Could not load existing filter for %s: %s", ts_name, e)

    state = {
        "selected_idx": None,  # used by shift-click peek extraction
        "sort_mode": "best",
        # Filter state. keep_set == None means "no curation yet — every pick
        # implicitly kept". Once the user toggles anything we materialize it
        # to a concrete set so subsequent toggles can flip membership cleanly.
        "keep_set": initial_keep_set,
        "saved_baseline": (set(initial_keep_set) if initial_keep_set is not None else None),
        # Tracks the saved state of the "exclude degenerate" checkbox so toggling
        # it alone counts as a dirty change worth saving.
        "saved_exclude_degen": True,
    }

    # Layout: cutouts on top (sitting next to the tomogram preview), then every
    # control + metadata strip compacted underneath. Two sibling boxes; cutouts
    # created first so it renders above.
    cutouts_box = ui.element("div").classes("cb-cutouts-box w-full")
    controls_box = ui.element("div").classes("cb-gallery-controls-box w-full")

    with cutouts_box, ui.row().classes("cb-cutouts-head w-full items-center gap-2"):
        ui.label("Subtomo gallery").classes("cb-section-title")
        ui.label("click: keep/drop · drag: box-select (⇧ keeps)").style("font-size: 9px; color: #94a3b8;").tooltip(
            "Drag a rectangle over the cutouts to flip the enclosed picks: a mostly-kept box drops "
            "them, a mostly-dropped box restores them. Shift-drag always keeps."
        )
        ui.space()
        # Save/Reset live in the header so they're always visible; the rest of
        # the filter chrome (counter, exclude, sort, path) is compacted below.
        actions_slot = ui.row().classes("cb-cutouts-actions items-center gap-1")

    with controls_box:
        # Hovered-pick stats strip (filled by the hover bridge) + sort/filter, compact.
        hover_labels = _render_hover_card_skeleton(horizontal=True)
        with ui.row().classes("w-full items-center gap-1"):
            ui.icon("sort", size="14px").style("color: #94a3b8;").tooltip("Sort order")
            sort_select = (
                ui.select(options={"best": "Best", "worst": "Worst", "z": "Z depth"}, value="best")
                .props("dense outlined options-dense")
                .classes("text-xs")
                .style("min-width: 92px;")
            )
            ui.icon("tune", size="14px").style("color: #94a3b8; margin-left: 6px;").tooltip("Display filter")
            _render_cutout_filter_controls(_filter_presets, cutout_apix, cutout_apix_source, _on_filter_select)
            ui.space()

    # Filter toolbar: counter on the left, action buttons on the right. Shown
    # only when we can actually save (subtomo + ce dirs both resolvable). When
    # the dashboard is viewing a candidate-extract that has no downstream
    # subtomo job yet, the gallery is also missing — atlas_meta would be None
    # — so this branch effectively only renders for fully-wired pipelines.
    counter_label = None
    save_btn = None
    discard_btn = None
    exclude_degen_cb = None  # set in the toolbar when there are degenerate picks
    can_filter = subtomo_job_dir is not None and ce_job_dir is not None and ts_name

    def _exclude_degen_on() -> bool:
        return bool(exclude_degen_cb.value) if exclude_degen_cb is not None else True

    def _effective_keep_set() -> set[int]:
        # The set actually written on save: kept OK picks, plus degenerate picks
        # only when the user unchecked "exclude". Degenerate "no subtomo match"
        # picks have no subtomo row so save_filtered_picks_for_ts drops them
        # regardless; the checkbox only changes the fate of render-failed picks
        # that DO have a subtomo.
        ks = state["keep_set"]
        base = set(all_pick_indices) if ks is None else set(ks)
        if not _exclude_degen_on():
            base |= set(degenerate_indices)
        return base

    def _is_dirty() -> bool:
        if state["keep_set"] != state["saved_baseline"]:
            return True
        return _exclude_degen_on() != state["saved_exclude_degen"]

    def _refresh_counter():
        if counter_label is None:
            return
        ks = state["keep_set"]
        kept = len(ks) if ks is not None else len(all_pick_indices)
        total = len(all_pick_indices)
        txt = f"kept {kept}/{total} (TS)"
        if degenerate_indices:
            txt += f" · {len(degenerate_indices)} not extracted"
        counter_label.set_text(txt)
        if _is_dirty():
            counter_label.classes(add="cb-filter-dirty")
        else:
            counter_label.classes(remove="cb-filter-dirty")
        if save_btn is not None:
            if _is_dirty():
                save_btn.props(remove="disable")
            else:
                save_btn.props("disable")
        if discard_btn is not None:
            has_saved = (
                subtomo_job_dir is not None and (subtomo_job_dir / picks_filter.PARTICLES_FILTERED_NAME).exists()
            )
            if has_saved:
                discard_btn.props(remove="disable")
            else:
                discard_btn.props("disable")

    if can_filter:
        # Metadata strip (below the cutouts): kept counter + exclude toggle.
        with controls_box, ui.row().classes("cb-filter-toolbar w-full"):
            counter_label = ui.label("kept …/…").classes("cb-filter-counter")
            if degenerate_indices:
                exclude_degen_cb = (
                    ui.checkbox(f"exclude {len(degenerate_indices)} not-extracted", value=True)
                    .props("dense")
                    .classes("cb-degen-toggle")
                    .tooltip(
                        "Picks that produced no subtomo cutout (no match / render failed). On (default) keeps "
                        "them out of the saved set; uncheck to include any that do have a subtomo."
                    )
                    .on_value_change(lambda: (_refresh_grid(), _refresh_counter()))
                )
            ui.space()

        def _on_save():
            try:
                if not _is_dirty():
                    ui.notify("No changes to save", type="info", timeout=1500)
                    return
                effective = _effective_keep_set()
                result = picks_filter.save_filtered_picks_for_ts(subtomo_job_dir, ce_job_dir, ts_name, effective)
                ks = state["keep_set"]
                state["saved_baseline"] = set(ks) if ks is not None else None
                state["saved_exclude_degen"] = _exclude_degen_on()
                ui.notify(f"Saved {result['kept_for_ts']} picks for {ts_name}", type="positive", timeout=2000)
                _refresh_counter()
                _refresh_path_row()
                if refresh_roster:
                    refresh_roster()  # roster review column updates now, not on next switch
            except Exception as e:
                logger.exception("Save filter failed for %s", ts_name)
                ui.notify(f"Save failed: {e}", type="negative", timeout=4000)

        def _on_discard():
            try:
                outcome = picks_filter.discard_ts_filter(subtomo_job_dir, ts_name)
                if outcome == "noop":
                    ui.notify("No filter to reset for this tomogram", type="info", timeout=1500)
                    return
                state["keep_set"] = None
                state["saved_baseline"] = None
                state["saved_exclude_degen"] = True
                if outcome == "removed_all":
                    ui.notify(
                        f"Reset {ts_name} — no curation left for this species, downstream uses original picks",
                        type="info",
                        timeout=2800,
                    )
                else:
                    ui.notify(
                        f"Reset {ts_name} to original picks — other tomograms keep their curation",
                        type="info",
                        timeout=2800,
                    )
                _refresh_grid()
                _refresh_counter()
                _refresh_path_row()
                if refresh_roster:
                    refresh_roster()
            except Exception as e:
                logger.exception("Discard filter failed")
                ui.notify(f"Discard failed: {e}", type="negative", timeout=4000)

        # Action buttons in the always-visible header slot.
        with actions_slot:
            discard_btn = (
                ui.button("Reset this tomo", icon="delete_outline", on_click=_on_discard)
                .props("flat dense size=sm color=red-7")
                .classes("text-xs")
                .tooltip(
                    "Revert THIS tomogram's picks to the original (all kept). Other tomograms in this "
                    "species keep their curation. Removing the last curated tomogram deletes the filter."
                )
            )
            save_btn = (
                ui.button("Save picks", icon="save", on_click=_on_save)
                .props("dense size=sm color=indigo-6 unelevated")
                .classes("text-xs")
                .tooltip(
                    "Write optimisation_set_filtered.star + particles_filtered.star next to the subtomo "
                    "job's outputs. Downstream consumers (reconstruct_particle, class3d) auto-prefer the "
                    "filtered file via the IO-slot resolver when it exists."
                )
            )

        # Saved filtered-set path + copy button — shown only once a filter exists
        # (after Save), gone after Discard. So the user knows the file is there.
        with controls_box:
            path_row = ui.row().classes("cb-filter-path w-full items-center")

        def _filtered_set_path() -> Optional[str]:
            if subtomo_job_dir is None:
                return None
            p = subtomo_job_dir / picks_filter.OPTIMISATION_SET_FILTERED_NAME
            return str(p) if p.exists() else None

        def _refresh_path_row() -> None:
            path_row.clear()
            fp = _filtered_set_path()
            if not fp:
                path_row.set_visibility(False)
                return
            path_row.set_visibility(True)
            with path_row:
                ui.label("filtered set").classes("cb-info-key")
                ui.label(fp).classes("cb-info-val").tooltip(fp)
                (
                    ui.button(
                        icon="content_copy",
                        on_click=lambda: (
                            ui.clipboard.write(fp),
                            ui.notify("Copied path", type="positive", timeout=800),
                        ),
                    )
                    .props("flat dense round size=sm")
                    .classes("cb-info-copy")
                    .tooltip("Copy filtered-set path to clipboard")
                )

        _refresh_path_row()

    with cutouts_box:
        # Wrapped so a filter switch re-renders the noise-floor reference tiles
        # under the same filter as the grid (the template thumb is its own clean
        # reference and stays raw).
        ref_strip_container = ui.element("div").classes("w-full")

        def _refresh_reference_strip():
            ref_strip_container.clear()
            with ref_strip_container:
                _render_reference_strip(picks, cutout_index, atlas_url, cols, rows, manifest)

        _refresh_reference_strip()

        grid_container = ui.element("div").classes("cb-gallery-scroll w-full")
        grid_container._props["id"] = gallery_id

        peek_refs = _render_peek_skeleton()

    def _sorted_pick_indices() -> list:
        mode = state["sort_mode"]
        available = [int(k) for k in cutout_index.keys()]
        if mode == "best":
            return sorted(available)
        if mode == "worst":
            return sorted(available, reverse=True)
        if mode == "z":

            def _z(i):
                return picks[i].get("z", 0) if 0 <= i < len(picks) else 0

            return sorted(available, key=_z, reverse=True)
        return sorted(available)

    tomo_mrc = entry.get("tomo_mrc")
    particle_diameter_ang = (manifest or {}).get("particle_diameter_ang") if isinstance(manifest, dict) else None
    picks_json_path = entry.get("picks_json")
    peek_dir = (Path(picks_json_path).parent / "peeks") if picks_json_path else None

    def _is_kept(pick_idx: int) -> bool:
        ks = state["keep_set"]
        if ks is None:
            return True  # implicit keep-all
        return pick_idx in ks

    def _toggle_keep(pick_idx: int) -> None:
        if pick_idx in degenerate_indices:
            return  # degenerate picks aren't individually keepable (no cutout)
        ks = state["keep_set"]
        if ks is None:
            # First click materializes the keep_set with everything as a
            # baseline so subsequent toggles can remove individual entries.
            ks = set(all_pick_indices)
            state["keep_set"] = ks
        if pick_idx in ks:
            ks.discard(pick_idx)
        else:
            ks.add(pick_idx)
        _refresh_grid()
        _refresh_counter()

    def _on_dot_toggle(e) -> None:
        # Clicking a ghost dot on the slab toggles keep/drop, same as a tile.
        # Dispatched from the hover bridge via a CustomEvent on grid_container.
        try:
            idx = int((e.args or {}).get("idx"))
        except (TypeError, ValueError):
            return
        _toggle_keep(idx)

    # Captured at render time (valid context). _sync_dropped_dots uses this
    # instead of resolving the client via context.slot — which, inside a
    # tile-click handler, is the tile that _refresh_grid's grid_container.clear()
    # just deleted (the "parent element this slot belongs to has been deleted"
    # crash that also left the Save button stuck disabled because the exception
    # aborted _toggle_keep before _refresh_counter ran).
    _gallery_client = ui.context.client

    def _sync_dropped_dots() -> None:
        # Reflect keep/drop onto the canvas ghost dots: dropped picks (and, when
        # the exclude box is on, degenerate picks) get greyed via a class toggle
        # on the matching dots in THIS species' layers. Keeps the slab and the
        # gallery in agreement whichever side the user clicked.
        ks = state["keep_set"]
        dropped = [] if ks is None else [i for i in all_pick_indices if i not in ks]
        if _exclude_degen_on():
            dropped = list(dropped) + list(degenerate_indices)
        layer_ids = [lid for lid in (xy_host_id, xz_host_id) if lid]
        if not layer_ids:
            return
        js = (
            "(function(){const ls=%(layers)s;const d=new Set(%(dropped)s);"
            "ls.forEach(function(lid){const h=document.getElementById(lid);if(!h)return;"
            "h.querySelectorAll('.cb-pick-ghost[data-pick-idx]').forEach(function(g){"
            "if(d.has(g.getAttribute('data-pick-idx')))g.classList.add('cb-pick-ghost-dropped');"
            "else g.classList.remove('cb-pick-ghost-dropped');});});})();"
        ) % {"layers": json.dumps(layer_ids), "dropped": json.dumps([str(i) for i in dropped])}
        try:
            _gallery_client.run_javascript(js)
        except Exception:
            pass  # client / elements gone (e.g. TS switched) — dots resync on next render

    def _trigger_peek(pick_idx: int) -> None:
        state["selected_idx"] = pick_idx
        import asyncio as _asyncio

        _asyncio.create_task(
            _trigger_peek_for_pick(
                pick_idx, picks, tomo_mrc, peek_dir, pixel_size_ang, particle_diameter_ang, peek_refs, state
            )
        )

    def _on_tile_click(pick_idx: int, shift: bool) -> None:
        if shift:
            # Power-user path: peek-extract the subvolume so the 3dmod cmd
            # input populates. Doesn't affect keep/drop state.
            _trigger_peek(pick_idx)
            return
        _toggle_keep(pick_idx)

    DISPLAY_TILE_PX = 96
    bg_w = cols * DISPLAY_TILE_PX
    bg_h = rows * DISPLAY_TILE_PX
    bg_size_css = f"{bg_w}px {bg_h}px"

    def _refresh_grid():
        grid_container.clear()
        with grid_container, ui.element("div").classes("cb-gallery-grid"):
            for rank, pick_idx in enumerate(_sorted_pick_indices()):
                pos = cutout_index.get(str(pick_idx))
                if not pos:
                    continue
                r, c = pos
                bg_x = -c * DISPLAY_TILE_PX
                bg_y = -r * DISPLAY_TILE_PX
                pick = picks[pick_idx] if 0 <= pick_idx < len(picks) else None
                cls = "cb-gallery-tile"
                if not _is_kept(pick_idx):
                    cls += " cb-tile-dropped"
                style = (
                    f"background-image: url({atlas_url}); "
                    f"background-size: {bg_size_css}; "
                    f"background-position: {bg_x}px {bg_y}px;"
                )
                tile = ui.element("div").classes(cls).style(style)
                tile._props["data-pick-idx"] = str(pick_idx)
                if pick is not None:
                    parts = [f"#{pick_idx}"]
                    if pick.get("score") is not None:
                        parts.append(f"score={pick['score']:.4f}")
                    parts.append(f"x={int(pick.get('x', 0))}")
                    parts.append(f"y={int(pick.get('y', 0))}")
                    parts.append(f"z={int(pick.get('z', 0))}")
                    parts.append("(click: keep/drop · shift-click: extract for 3dmod)")
                    tile._props["title"] = "  ".join(parts)
                # JS handler captures the shift modifier so we can route to
                # toggle vs. peek-extract; emit returns the dict that becomes
                # e.args on the Python side.
                tile.on(
                    "click",
                    lambda e, i=pick_idx: _on_tile_click(i, bool((e.args or {}).get("shift", False))),
                    js_handler="(event) => { emit({shift: event.shiftKey}); }",
                )
                with tile:
                    ui.html(f"#{rank + 1}", sanitize=False).classes("cb-tile-rank")
                    ui.html(f"{pick_idx}", sanitize=False).classes("cb-tile-idx")
                    if pick is not None and pick.get("z") is not None:
                        ui.html(f"z{int(pick['z'])}", sanitize=False).classes("cb-tile-z")
                    if pick and pick.get("score") is not None:
                        ui.html(f"{pick['score']:.3f}", sanitize=False).classes("cb-tile-score")

            # Degenerate tiles at the end: candidates that produced no cutout.
            # Greyed + non-interactive, index + reason in tooltip — so it's clear
            # why the rendered-tile count is below the pick-index range.
            for f in failures:
                try:
                    fi = int(f.get("i"))
                except (TypeError, ValueError):
                    continue
                reason = str(f.get("reason", "unknown"))
                dcls = "cb-gallery-tile cb-tile-degenerate"
                if _exclude_degen_on():
                    dcls += " cb-tile-dropped"  # same red-orange excluded look as a dropped pick
                dtile = ui.element("div").classes(dcls)
                dtile._props["title"] = f"#{fi} · {reason} — no cutout rendered" + (
                    " · excluded from saved picks" if _exclude_degen_on() else " · kept if it has a subtomo"
                )
                with dtile:
                    ui.html(f"{fi}", sanitize=False).classes("cb-tile-idx")
                    ui.html("✕", sanitize=False).classes("cb-tile-degen-mark")
        _sync_dropped_dots()

    sort_select.on_value_change(lambda e: (state.update(sort_mode=e.value or "best"), _refresh_grid()))

    # The filter TYPE/PARAM selects (rendered above) wire their own handlers via
    # `_on_filter_select`, which swaps `atlas_url` and re-renders.

    # Ghost-dot clicks on the slab toggle keep/drop too — the bridge dispatches a
    # CustomEvent on grid_container, scoped to it so it's cleaned up with the gallery.
    grid_container.on("cbpicktoggle", _on_dot_toggle, js_handler="(e) => emit(e.detail)")

    # Box-select (lasso): the marquee JS collects the enclosed tiles' pick idxs
    # and dispatches `cbpicklasso` with {idxs, keep}. Default = drop them all;
    # Shift-drag keeps/restores them. One uniform action over the box (not a
    # per-tile toggle, which would be ambiguous over a mixed selection).
    def _on_lasso(e) -> None:
        args = e.args or {}
        keep = bool(args.get("keep"))
        ks = state["keep_set"]
        if ks is None:
            ks = set(all_pick_indices)
            state["keep_set"] = ks
        changed = False
        for raw in args.get("idxs") or []:
            try:
                i = int(raw)
            except (TypeError, ValueError):
                continue
            if i in degenerate_indices:
                continue
            if keep and i not in ks:
                ks.add(i)
                changed = True
            elif not keep and i in ks:
                ks.discard(i)
                changed = True
        if changed:
            _refresh_grid()
            _refresh_counter()

    grid_container.on("cbpicklasso", _on_lasso, js_handler="(e) => emit(e.detail)")

    # Box-select is ALWAYS on: a plain click on a tile still toggles keep/drop;
    # a click-drag across the cutouts panel draws a marquee and drops (Shift =
    # keep) everything inside. A movement threshold separates click from drag,
    # and the synthetic click the browser fires at the end of a drag is
    # swallowed so it doesn't also toggle the tile under the cursor.
    # One global delegated handler per client (guarded) + lazy grid resolution so
    # it survives q-tab-panel mounts (inactive panels aren't in the DOM at render).
    _marquee_js = """
    (function() {
        if (window.__cbLassoInit) return;
        window.__cbLassoInit = true;
        var THRESH = 5;
        var rect = null, grid = null, sx = 0, sy = 0, shiftKey = false, pending = false, dragging = false;
        function place(cx, cy) {
            var gb = grid.getBoundingClientRect();
            rect.style.left = (Math.min(sx, cx) - gb.left + grid.scrollLeft) + 'px';
            rect.style.top = (Math.min(sy, cy) - gb.top + grid.scrollTop) + 'px';
            rect.style.width = Math.abs(cx - sx) + 'px';
            rect.style.height = Math.abs(cy - sy) + 'px';
        }
        document.addEventListener('mousedown', function(e) {
            if (e.button !== 0 || !e.target.closest) return;
            var g = e.target.closest('.cb-gallery-scroll');
            if (!g) return;
            grid = g; sx = e.clientX; sy = e.clientY; shiftKey = e.shiftKey;
            pending = true; dragging = false;  // don't preventDefault — keep clicks alive
        });
        document.addEventListener('mousemove', function(e) {
            if (!pending && !dragging) return;
            if (pending && (Math.abs(e.clientX - sx) > THRESH || Math.abs(e.clientY - sy) > THRESH)) {
                pending = false; dragging = true;
                rect = document.createElement('div');
                rect.className = 'cb-lasso-rect';
                grid.appendChild(rect);
                grid.classList.add('cb-lasso-dragging');
            }
            if (dragging) { e.preventDefault(); place(e.clientX, e.clientY); }
        });
        document.addEventListener('mouseup', function(e) {
            if (!dragging) { pending = false; grid = null; return; }  // plain click — let it through
            var rb = rect.getBoundingClientRect();
            var idxs = [], dropped = 0;
            grid.querySelectorAll('.cb-gallery-tile[data-pick-idx]').forEach(function(t) {
                if (t.classList.contains('cb-tile-degenerate')) return;
                var tb = t.getBoundingClientRect();
                var cx = tb.left + tb.width / 2, cy = tb.top + tb.height / 2;
                if (cx >= rb.left && cx <= rb.right && cy >= rb.top && cy <= rb.bottom) {
                    idxs.push(parseInt(t.getAttribute('data-pick-idx'), 10));
                    if (t.classList.contains('cb-tile-dropped')) dropped++;
                }
            });
            rect.remove(); rect = null;
            grid.classList.remove('cb-lasso-dragging');
            if (idxs.length) {
                // Symmetric: unify the box toward the opposite of its current
                // majority — mostly-kept → drop, mostly-dropped → restore (keep).
                // Shift forces keep (explicit restore). Ties drop.
                var keep = shiftKey ? true : (dropped > (idxs.length - dropped));
                grid.dispatchEvent(new CustomEvent('cbpicklasso', {detail: {idxs: idxs, keep: keep}}));
            }
            // Swallow the click the browser synthesizes from this drag so it
            // doesn't also toggle the tile under the pointer. Capture-phase, and
            // self-removing on the next tick whether or not a click fires.
            function sw(ev) { ev.stopPropagation(); ev.preventDefault(); }
            window.addEventListener('click', sw, true);
            setTimeout(function() { window.removeEventListener('click', sw, true); }, 0);
            dragging = false; pending = false; grid = null;
        });
    })();
    """
    ui.run_javascript(_marquee_js)

    _refresh_grid()
    _refresh_counter()

    if (has_xy and pick_xy_frac) or (has_xz and pick_xz_frac):
        slices = []
        if has_xy and pick_xy_frac:
            slices.append({"id": xy_host_id, "picks": pick_xy_frac})
        if has_xz and pick_xz_frac:
            slices.append({"id": xz_host_id, "picks": pick_xz_frac})
        pick_meta = _build_pick_meta_for_js(picks, pixel_size_ang)
        hover_card_id = hover_labels.get("__card_id") or ""
        # Bidirectional hover bridge:
        #   gallery tile mouseover → place marker on each preview, mark
        #     matching ghost dots active, fill hover card.
        #   ghost-dot mouseover     → same, plus add .cb-tile-highlight on
        #     the matching gallery tile and scroll it into view if hidden.
        # Both directions write the hover card via JS (no Python round-trip)
        # using pick_meta pre-formatted server-side. The lone Python path
        # (tile click → persistent .selected) is unchanged.
        bridge_js = """
        setTimeout(function() {
            const galleryId = %(gallery_id)s;
            const slices = %(slices)s;
            const meta = %(meta)s;
            const hoverCard = document.getElementById(%(card_id)s);
            // Layer hosts live in the always-present left canvas column, so they
            // resolve now and stay valid across tab switches. `s.id` is the
            // .cb-pick-layer id; the marker + ghost dots are its children.
            const wired = slices.map(function(s) {
                const host = document.getElementById(s.id);
                if (!host) return null;
                return {
                    host: host,
                    marker: host.querySelector('.cb-pick-marker'),
                    picks: s.picks,
                    layerId: s.id
                };
            }).filter(function(x) { return x && x.marker; });
            if (!wired.length) return;

            // Delegate on the Particles card — the common ancestor of BOTH the
            // canvas (left) and every species' gallery (right, in lazily-mounted
            // tab panels). The gallery grid is resolved LAZILY at event time, so
            // the cross-link works no matter which tab was active when this ran.
            // (Fix for the dead-hover bug on non-default tabs: q-tab-panels only
            // mount the active panel, so the old one-shot getElementById(galleryId)
            // bailed for inactive tabs and never wired their listeners.)
            const root = wired[0].host.closest('.cb-section-card') || document.body;
            const ourLayerIds = wired.map(function(w) { return w.layerId; });
            function getGrid() { return document.getElementById(galleryId); }
            function ourGhost(el) {
                const layer = el.closest && el.closest('.cb-pick-layer');
                return !!layer && ourLayerIds.indexOf(layer.id) !== -1;
            }

            function placeMarkers(idx) {
                wired.forEach(function(w) {
                    const xy = w.picks[idx];
                    if (!xy) { w.marker.style.opacity = '0'; return; }
                    w.marker.style.left = (xy[0] * 100).toFixed(3) + '%%';
                    w.marker.style.top = (xy[1] * 100).toFixed(3) + '%%';
                    w.marker.style.opacity = '1';
                });
            }
            function hideMarkers() {
                wired.forEach(function(w) { w.marker.style.opacity = '0'; });
            }
            function setGhostActive(idx, on) {
                wired.forEach(function(w) {
                    w.host.querySelectorAll('.cb-pick-ghost[data-pick-idx="' + idx + '"]').forEach(function(g) {
                        if (on) g.classList.add('cb-ghost-active');
                        else g.classList.remove('cb-ghost-active');
                    });
                });
            }
            function setTileHighlight(idx, on, scrollIntoView) {
                const grid = getGrid();
                if (!grid) return;
                const tile = grid.querySelector('.cb-gallery-tile[data-pick-idx="' + idx + '"]');
                if (!tile) return;
                if (on) tile.classList.add('cb-tile-highlight');
                else tile.classList.remove('cb-tile-highlight');
                if (on && scrollIntoView) {
                    const tr = tile.getBoundingClientRect();
                    const gr = grid.getBoundingClientRect();
                    if (tr.top < gr.top || tr.bottom > gr.bottom) {
                        tile.scrollIntoView({block: 'nearest', behavior: 'smooth'});
                    }
                }
            }
            function fillHover(idx) {
                if (!hoverCard) return;
                if (idx == null) {
                    hoverCard.querySelectorAll('.cb-hover-val').forEach(function(el) {
                        el.textContent = '—';
                    });
                    return;
                }
                const m = meta[idx];
                if (!m) return;
                ['idx','px','ang','score','z%%-tile','nn'].forEach(function(k) {
                    const sel = '.cb-hover-val[data-hover-key="' + k.replace('"','\\\\"') + '"]';
                    const el = hoverCard.querySelector(sel);
                    if (el) el.textContent = m[k] != null ? m[k] : '—';
                });
            }
            // Clear ALL active state in our scope (every active ghost + every
            // highlighted tile). Called at the start of each hover so only one
            // pick is ever active — moving tile→tile no longer accumulates
            // stuck dots (the bug: mouseout only fired on full grid-exit, so a
            // tile→tile move never deactivated the one you left).
            function clearActive() {
                wired.forEach(function(w) {
                    w.host.querySelectorAll('.cb-pick-ghost.cb-ghost-active').forEach(function(g) {
                        g.classList.remove('cb-ghost-active');
                    });
                });
                const grid = getGrid();
                if (grid) grid.querySelectorAll('.cb-tile-highlight').forEach(function(t) {
                    t.classList.remove('cb-tile-highlight');
                });
            }
            function isOurs(el) {
                if (!el || !el.closest) return false;
                const grid = getGrid();
                const tile = el.closest('.cb-gallery-tile[data-pick-idx]');
                if (tile && grid && grid.contains(tile)) return true;
                const gh = el.closest('.cb-pick-ghost[data-pick-idx]');
                return !!(gh && ourGhost(gh));
            }

            // Single delegated mouseover/mouseout on the card handles BOTH
            // directions: a gallery tile (filtered to OUR grid) and a ghost dot
            // (filtered to OUR species' layers). Filtering keeps the N per-species
            // bridges on the shared card from cross-firing. Every mouseover
            // resets first so exactly one pick is active at a time.
            root.addEventListener('mouseover', function(e) {
                if (!e.target.closest) return;
                const grid = getGrid();
                const tile = e.target.closest('.cb-gallery-tile[data-pick-idx]');
                if (tile && grid && grid.contains(tile)) {
                    const idx = tile.getAttribute('data-pick-idx');
                    clearActive();
                    placeMarkers(idx);
                    setGhostActive(idx, true);
                    fillHover(idx);
                    return;
                }
                const ghost = e.target.closest('.cb-pick-ghost[data-pick-idx]');
                if (ghost && ourGhost(ghost)) {
                    const idx = ghost.getAttribute('data-pick-idx');
                    clearActive();
                    placeMarkers(idx);
                    setGhostActive(idx, true);
                    setTileHighlight(idx, true, true);
                    fillHover(idx);
                }
            });
            root.addEventListener('mouseout', function(e) {
                if (!e.target.closest) return;
                const leaving = e.target.closest('.cb-gallery-tile[data-pick-idx]') ||
                    e.target.closest('.cb-pick-ghost[data-pick-idx]');
                if (!leaving || !isOurs(leaving)) return;
                // Only tear down when the pointer leaves our interactive area
                // entirely; tile→tile / tile→ghost moves are handled by the next
                // mouseover's clearActive().
                if (!isOurs(e.relatedTarget)) {
                    clearActive();
                    hideMarkers();
                    fillHover(null);
                }
            });
            // Ghost-dot click → toggle keep/drop for that pick (same as a tile).
            // Dispatched as a CustomEvent on our grid so the NiceGUI handler bound
            // to grid_container picks it up (scoped to the gallery, auto-cleaned).
            root.addEventListener('click', function(e) {
                if (!e.target.closest) return;
                const ghost = e.target.closest('.cb-pick-ghost[data-pick-idx]');
                if (!ghost || !ourGhost(ghost)) return;
                const grid = getGrid();
                if (grid) grid.dispatchEvent(new CustomEvent('cbpicktoggle',
                    {detail: {idx: ghost.getAttribute('data-pick-idx')}}));
            });
        }, 80);
        """ % {
            "gallery_id": json.dumps(gallery_id),
            "slices": json.dumps(slices),
            "meta": json.dumps(pick_meta),
            "card_id": json.dumps(hover_card_id),
        }
        ui.run_javascript(bridge_js)


def _render_picks_scatter_section(row: dict, entry: dict, manifest: dict) -> None:
    """Scatter-only fallback: X/Y + X/Z + score histogram. Renders only when
    the gallery isn't available."""
    picks_json_path = entry.get("picks_json")
    picks_data = (
        _read_picks_json(Path(picks_json_path)) if picks_json_path else {"picks": [], "tomo_dims_xyz_px": [0, 0, 0]}
    )
    picks = picks_data.get("picks", [])
    tomo_dims = tuple(picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1])
    score_field = manifest.get("score_field")
    pixel_size_ang = entry.get("pixel_size_ang")
    xz_url = _vis_asset_url(entry["xz_preview"]) if entry.get("xz_preview") else None

    x_dim, y_dim, z_dim = (max(int(d), 1) for d in tomo_dims)
    xy_aspect = min(2.5, max(0.5, x_dim / y_dim))
    xz_aspect = min(5.0, max(1.5, x_dim / z_dim))
    xy_target_h = 380
    xy_max_w = min(640, max(280, int(xy_target_h * xy_aspect)))
    xz_target_h = 220
    xz_max_w = min(720, max(280, int(xz_target_h * xz_aspect)))

    with ui.element("div").classes("cb-section-card w-full"):
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("scatter_plot", size="14px").classes("text-indigo-600")
            ui.label("Pick distribution").classes("cb-section-title")
            ui.label(f"  ({row['n_picks']} picks)").classes("text-[10px] text-gray-400")
            ui.space()
            ui.label("· no subtomo extraction yet — gallery view unavailable").classes(
                "text-[10px] text-amber-700 italic"
            )

        with ui.row().classes("w-full gap-3 items-stretch flex-wrap"):
            with ui.column().classes("gap-1").style(f"flex: 1 1 320px; min-width: 280px; max-width: {xy_max_w}px;"):
                ui.label("X / Y top-down").classes("cb-section-title")
                with ui.element("div").classes("cb-aspect").style(f"aspect-ratio: {xy_aspect};"):
                    xy_plot = ui.plotly(_build_xy_scatter_fig(picks, tomo_dims, score_field)).style(
                        "width: 100%; height: 100%;"
                    )
            with (
                ui.column()
                .classes("gap-2 cb-picks-right")
                .style(f"flex: 1 1 320px; min-width: 280px; max-width: {xz_max_w}px;")
            ):
                ui.label("X / Z side").classes("cb-section-title")
                with ui.element("div").classes("cb-aspect").style(f"aspect-ratio: {xz_aspect};"):
                    xz_plot = ui.plotly(_build_xz_scatter_fig(picks, tomo_dims, score_field, xz_url)).style(
                        "width: 100%; height: 100%;"
                    )
                ui.label("Score distribution").classes("cb-section-title")
                ui.plotly(_build_score_hist_fig(picks, score_field)).style("width: 100%; height: 160px;")
                ui.label("Hovered pick").classes("cb-section-title")
                hover_labels = _render_hover_card_skeleton()

                def on_hover(e, _picks=picks, _ps=pixel_size_ang, _labels=hover_labels):
                    _update_hover_card(e, _picks, _ps, _labels)

                xy_plot.on("plotly_hover", on_hover, throttle=0.08)
                xz_plot.on("plotly_hover", on_hover, throttle=0.08)


def _build_pick_meta_for_js(picks: list, pixel_size_ang: Optional[float]) -> dict:
    """Pre-compute per-pick formatted strings for the JS-driven hover card.

    Mirrors `_update_hover_card`'s formatting verbatim so the visual output
    is identical whether the update came from a Plotly hover (Python) or a
    ghost-dot hover (JS). Keyed by str(pick_index) — JS reads it directly.
    """
    out: dict = {}
    for pick in picks:
        i = pick.get("i")
        if i is None:
            continue
        entry: dict = {"idx": f"#{pick.get('i')}", "px": f"{pick.get('x')}, {pick.get('y')}, {pick.get('z')}"}
        if pixel_size_ang:
            ax = pick.get("x", 0) * pixel_size_ang
            ay = pick.get("y", 0) * pixel_size_ang
            az = pick.get("z", 0) * pixel_size_ang
            entry["ang"] = f"{ax:.0f}, {ay:.0f}, {az:.0f}"
        else:
            entry["ang"] = "(no pixel size)"
        sc = pick.get("score")
        entry["score"] = f"{sc:.4f}" if sc is not None else "—"
        zp = pick.get("z_pct")
        entry["z%-tile"] = f"{zp:.0f}" if zp is not None else "—"
        nn_px = pick.get("nn_px")
        if nn_px is not None:
            entry["nn"] = f"{nn_px:.1f} px ({nn_px * pixel_size_ang:.0f} Å)" if pixel_size_ang else f"{nn_px:.1f} px"
        else:
            entry["nn"] = "—"
        out[str(i)] = entry
    return out


def _render_hover_card_skeleton(horizontal: bool = False) -> dict:
    labels: dict = {}
    # Pre-populate the full field grid up-front (idle dashes) instead of the
    # old lazy-populate-on-first-hover pattern. The JS bridge needs stable
    # DOM nodes with data-hover-key attributes to write into when a ghost
    # dot is hovered (no Python round-trip), and _update_hover_card just
    # writes set_text on the same labels for the Python-driven paths.
    # `horizontal` lays the key/value pairs out inline (gallery top strip);
    # the default 2-column grid is used by the scatter fallback.
    card_id = f"cb-hover-card-{uuid.uuid4().hex[:8]}"
    cls = "cb-hover-card cb-hover-horizontal" if horizontal else "cb-hover-card"
    with ui.element("div").classes(cls) as card:
        card._props["id"] = card_id
        for key in ("idx", "px", "ang", "score", "z%-tile", "nn"):
            if horizontal:
                with ui.element("div").classes("cb-hover-pair"):
                    ui.label(key).classes("cb-hover-key")
                    v_el = ui.label("—").classes("cb-hover-val")
                    v_el._props["data-hover-key"] = key
                    labels[key] = v_el
            else:
                ui.label(key).classes("cb-hover-key")
                v_el = ui.label("—").classes("cb-hover-val")
                v_el._props["data-hover-key"] = key
                labels[key] = v_el
    labels["__card"] = card
    labels["__card_id"] = card_id
    labels["__populated"] = True
    return labels


def _update_hover_card(e, picks: list, pixel_size_ang, labels: dict) -> None:
    args = getattr(e, "args", None) or {}
    points = args.get("points") or []
    if not points:
        return
    p = points[0] or {}
    cd = p.get("customdata") or []
    if not cd:
        return
    try:
        idx = int(cd[0])
    except (TypeError, ValueError):
        return
    if idx < 0 or idx >= len(picks):
        return
    pick = picks[idx]

    if labels.get("__card") is None:
        return

    labels["idx"].set_text(f"#{pick['i']}")
    labels["px"].set_text(f"{pick['x']}, {pick['y']}, {pick['z']}")
    if pixel_size_ang:
        ax = pick["x"] * pixel_size_ang
        ay = pick["y"] * pixel_size_ang
        az = pick["z"] * pixel_size_ang
        labels["ang"].set_text(f"{ax:.0f}, {ay:.0f}, {az:.0f}")
    else:
        labels["ang"].set_text("(no pixel size)")
    if pick.get("score") is not None:
        labels["score"].set_text(f"{pick['score']:.4f}")
    else:
        labels["score"].set_text("—")
    if pick.get("z_pct") is not None:
        labels["z%-tile"].set_text(f"{pick['z_pct']:.0f}")
    else:
        labels["z%-tile"].set_text("—")
    if pick.get("nn_px") is not None:
        nn_px = pick["nn_px"]
        if pixel_size_ang:
            labels["nn"].set_text(f"{nn_px:.1f} px ({nn_px * pixel_size_ang:.0f} Å)")
        else:
            labels["nn"].set_text(f"{nn_px:.1f} px")
    else:
        labels["nn"].set_text("—")


# ---------------------------------------------------------------------------
# Per-instance row collection: builds row dicts for every tomogram in a
# candidate-extract job, merging tomograms.star and the preview manifest.
# ---------------------------------------------------------------------------


def _collect_tomo_rows_for_instance(job_dir: Path, project_path: Path) -> list[dict]:
    tomograms_star = job_dir / "tomograms.star"
    tomo_df = _read_tomograms_table(tomograms_star)
    manifest = read_preview_manifest(job_dir) or {}
    tomo_entries = manifest.get("tomograms") or {}
    summary = manifest.get("summary") or {}
    missing_volume = set(summary.get("missing_volume") or [])
    errored_map = {e["tomo"]: e.get("error", "") for e in (summary.get("errored") or [])}
    # "zero-picks": orchestrator processed this TS but PyTOM produced no
    # candidates above cutoff. Without this bucket the per-TS card falls into
    # the generic "no-preview" spinner branch and lies to the user about being
    # "still generating" indefinitely.
    zero_picks = set(summary.get("zero_picks") or [])

    rows: list[dict] = []
    if tomo_df is None:
        for tomo_name, entry in tomo_entries.items():
            label, (stage, beam) = _position_label(tomo_name)
            mod_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
            rows.append(
                {
                    "tomo_name": tomo_name,
                    "position_label": label,
                    "stage": stage,
                    "beam": beam,
                    "vol_path": entry.get("tomo_mrc"),
                    "mod_path": str(mod_path),
                    "mod_exists": mod_path.exists(),
                    "n_picks": entry.get("n_picks"),
                    "score_range": entry.get("score_range"),
                    "status": "ok" if entry.get("picks_json") else "no-preview",
                    "error": None,
                }
            )
        for tomo_name in zero_picks - set(tomo_entries.keys()):
            label, (stage, beam) = _position_label(tomo_name)
            mod_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
            rows.append(
                {
                    "tomo_name": tomo_name,
                    "position_label": label,
                    "stage": stage,
                    "beam": beam,
                    "vol_path": None,
                    "mod_path": str(mod_path),
                    "mod_exists": False,
                    "n_picks": 0,
                    "score_range": None,
                    "status": "zero-picks",
                    "error": None,
                }
            )
    else:
        for _, tomo_row in tomo_df.iterrows():
            tomo_name = str(tomo_row["rlnTomoName"])
            label, (stage, beam) = _position_label(tomo_name)
            entry = tomo_entries.get(tomo_name) or {}
            vol_path = _resolve_volume_for_3dmod(tomo_row, project_path)
            mod_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
            if tomo_name in missing_volume and not entry.get("picks_json"):
                status = "missing-volume"
            elif tomo_name in errored_map:
                status = "errored"
            elif entry.get("picks_json"):
                status = "ok"
            elif tomo_name in zero_picks:
                status = "zero-picks"
            else:
                status = "no-preview"
            rows.append(
                {
                    "tomo_name": tomo_name,
                    "position_label": label,
                    "stage": stage,
                    "beam": beam,
                    "vol_path": str(vol_path) if vol_path else None,
                    "mod_path": str(mod_path),
                    "mod_exists": mod_path.exists(),
                    "n_picks": entry.get("n_picks"),
                    "score_range": entry.get("score_range"),
                    "status": status,
                    "error": errored_map.get(tomo_name),
                }
            )
    rows.sort(key=lambda r: (r["stage"], r["beam"], r["tomo_name"]))
    return rows


# ---------------------------------------------------------------------------
# Generation handlers (regen previews / IMOD models from per-card buttons)
# ---------------------------------------------------------------------------


def _make_imod_command_runner():
    from services.computing.container_service import get_container_service

    container_service = get_container_service()

    def runner(cmd: str, cwd: Path) -> None:
        import subprocess

        wrapped = container_service.wrap_command_for_tool(cmd, cwd=cwd, tool_name="imod", additional_binds=[str(cwd)])
        result = subprocess.run(wrapped, shell=True, capture_output=True, text=True, cwd=cwd)
        if result.returncode != 0:
            raise RuntimeError(
                f"Container command failed (rc={result.returncode}): {result.stderr.strip() or result.stdout.strip()}"
            )

    return runner


def _generate_imod_sync(
    candidates_star: Path, tomograms_star: Path, diameter: float, job_dir: Path, project_path: Path
) -> None:
    generate_candidate_vis(
        candidates_star=candidates_star,
        tomograms_star=tomograms_star,
        particle_diameter_ang=diameter,
        output_dir=job_dir,
        command_runner=_make_imod_command_runner(),
        project_root=project_path,
    )


# Per-mount dedup so the auto-kick helpers don't pile up completion timers
# when the user clicks between tilt-series in the sidebar. BackgroundTask's
# own dedup_key prevents redundant *work*, but we still want to avoid
# installing multiple ui.timer pollers per (job_dir).
_AUTO_KICKED_PREVIEWS: set[str] = set()
_AUTO_KICKED_IMOD: set[str] = set()


def reset_auto_kick_state() -> None:
    """Clear the auto-kick dedup sets — used by `open_tomo_dashboard` so each
    fresh dashboard mount can re-trigger generation if the page is reloaded."""
    _AUTO_KICKED_PREVIEWS.clear()
    _AUTO_KICKED_IMOD.clear()
    _AUTO_KICKED_RECON_SLABS.clear()
    _AUTO_KICKED_LIST_CUTOUTS.clear()
    _AUTO_INGESTED_COORDS.clear()


def _auto_kick_preview_generation(instance_id: str, job_model, job_dir: Path, project_path: Path, refresh) -> bool:
    """If the candidate-extract job has succeeded but some tomograms are
    missing from the preview manifest, kick off a background 'Render
    missing' with completion handler that refreshes the page when done.
    Returns True iff a kickoff was submitted (or one was already in
    flight). Safe to call on every render — both module-level set and
    BackgroundTask dedup_key prevent re-submission."""
    key = str(job_dir)
    if key in _AUTO_KICKED_PREVIEWS:
        return False
    if getattr(job_model, "execution_status", None) != JobStatus.SUCCEEDED:
        return False
    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        return False
    _AUTO_KICKED_PREVIEWS.add(key)

    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
    state = get_project_state()

    async def _run(progress_cb):
        import asyncio as _asyncio

        return await _asyncio.to_thread(
            generate_candidate_previews,
            candidates_star,
            tomograms_star,
            diameter,
            job_dir,
            project_path,
            progress_cb,
            False,
            state,
            instance_id,
            job_model,
        )

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title=f"Auto-render previews · {instance_id}",
        subtitle="Filling missing tomogram entries",
        project_path=str(project_path),
        dedup_key=f"render-previews:{job_dir}:no-force",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)
    return True


def _auto_kick_imod_generation(instance_id: str, job_model, job_dir: Path, project_path: Path, refresh) -> bool:
    """If candidates.star exists for a succeeded extract but the IMOD .mod
    overlays aren't on disk, kick off background generation that auto-
    refreshes the 3dmod command lines on completion. Same dedup semantics
    as the preview kickoff above."""
    key = str(job_dir)
    if key in _AUTO_KICKED_IMOD:
        return False
    if getattr(job_model, "execution_status", None) != JobStatus.SUCCEEDED:
        return False
    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        return False
    imod_dir = job_dir / "vis" / "imodPartRad"
    if imod_dir.exists() and any(imod_dir.glob("*.mod")):
        return False  # already have overlays
    _AUTO_KICKED_IMOD.add(key)

    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))

    async def _run(progress_cb):
        import asyncio as _asyncio

        progress_cb(0, 0, "generating IMOD .mod overlays…")
        await _asyncio.to_thread(_generate_imod_sync, candidates_star, tomograms_star, diameter, job_dir, project_path)
        return "IMOD overlays ready; 3dmod commands now include them"

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title=f"Auto-generate IMOD overlays · {instance_id}",
        subtitle="Per-tomogram .mod files for 3dmod",
        project_path=str(project_path),
        dedup_key=f"imod-models:{job_dir}",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)
    return True


async def _handle_generate_imod_for_instance(
    instance_id: str, job_model, job_dir: Path, project_path: Path, btn, refresh
) -> None:
    """Submit IMOD model generation to the background task registry. Same
    pattern as the preview-render handler: fire-and-forget, user tracks
    via the workspace tray."""
    import asyncio as _asyncio

    from ui.background_task import BackgroundTask

    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        ui.notify(
            "candidates.star or tomograms.star missing — cannot generate IMOD models", type="negative", timeout=4000
        )
        return
    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))

    async def _run(progress_cb) -> str:
        # IMOD generation has no per-tomogram progress hook today; treat as
        # indeterminate (total=0).
        progress_cb(0, 0, "generating IMOD .mod overlays…")
        await _asyncio.to_thread(_generate_imod_sync, candidates_star, tomograms_star, diameter, job_dir, project_path)
        return "IMOD overlays ready; 3dmod commands now include them"

    BackgroundTask(
        title=f"IMOD models · {instance_id}",
        subtitle="Generate .mod overlays for 3dmod",
        project_path=str(project_path),
        dedup_key=f"imod-models:{job_dir}",
    ).submit(_run)
    refresh()


async def _handle_generate_for_instance(
    instance_id: str, job_model, job_dir: Path, project_path: Path, force: bool, btn, refresh
) -> None:
    """Submit a preview-render to the BackgroundTaskRegistry. Returns
    immediately; user tracks progress via the workspace tray. Multiple
    clicks on the same (job_dir, force) combo dedupe to a single task to
    keep concurrent writes to manifest.json from racing each other."""
    import asyncio as _asyncio

    from ui.background_task import BackgroundTask

    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        ui.notify("candidates.star or tomograms.star missing — cannot render previews", type="negative", timeout=4000)
        return
    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
    state = get_project_state()

    action = "Re-render all" if force else "Render missing"
    subtitle = "Bypass cache; regenerate every tomogram" if force else "Skip tomograms with a fresh manifest entry"

    async def _run(progress_cb) -> str:
        # The orchestrator is sync; off-thread it so the event loop stays
        # responsive. progress_cb signature matches:
        #   preview_orchestrator.py: progress_cb(i, total, tomo_name)
        # which lines up with our registry's (current, total, message).
        summary = await _asyncio.to_thread(
            generate_candidate_previews,
            candidates_star,
            tomograms_star,
            diameter,
            job_dir,
            project_path,
            progress_cb,
            force,
            state,
            instance_id,
            job_model,
        )
        n_new = len(summary["ok"])
        n_cached = len(summary["skipped_cached"])
        n_missing = len(summary["missing_volume"])
        n_err = len(summary["errored"])
        # Pull zero-pick count from the manifest summary if the orchestrator
        # recorded it (manifest v10+).
        try:
            from services.visualization.preview_orchestrator import read_preview_manifest

            manifest = read_preview_manifest(job_dir) or {}
            n_zero = len((manifest.get("summary") or {}).get("zero_picks") or [])
        except Exception:
            n_zero = 0

        parts = [f"{n_new} rendered", f"{n_cached} cached"]
        if n_zero:
            parts.append(f"{n_zero} zero-picks")
        if n_missing:
            parts.append(f"{n_missing} missing volume")
        if n_err:
            parts.append(f"{n_err} errored")
        return ", ".join(parts)

    BackgroundTask(
        title=f"Journey previews · {instance_id}",
        subtitle=f"{action} — {subtitle}",
        project_path=str(project_path),
        # Dedup keyed by job_dir + force flag so concurrent clicks coalesce
        # rather than racing on writes to the same manifest.json.
        dedup_key=f"preview-render:{job_dir}:force={force}",
    ).submit(_run)
    refresh()

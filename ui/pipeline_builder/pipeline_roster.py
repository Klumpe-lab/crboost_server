import asyncio
import logging
from pathlib import Path
from typing import Any, TYPE_CHECKING
from nicegui import ui
from services.array_tasks import TaskProgress
from services.models_base import JobStatus
from services.project_state import JobType
from ui.current_project import current_project_state

from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.reactive import FingerprintedView
from ui.components.species_pill import render_species_pill
from ui.components.svg_icon import load_icon_svg
from ui.curation_session_dialog import open_curation_control_center
from ui.particles import session_status
from ui.styles import MONO, SANS as FONT
from ui.status_indicator import BoundStatusDot, _running_spinner_html
from services.models_base import InstanceId, instance_id_to_job_type
from ui.ui_state import get_job_display_name, get_instance_display_name
from ui.pipeline_builder.pipeline_constants import (
    PHASE_JOBS,
    PHASE_META,
    PHASE_PARTICLES,
    ROSTER_ANCHOR,
    SB_MUTE,
    SB_ACT,
    SB_ABG,
    SB_SEP,
    missing_deps,
    fmt,
)

if TYPE_CHECKING:
    from ui.pipeline_builder.pipeline_builder_panel import PipelineBuilderPanel

logger = logging.getLogger(__name__)

# Rail curation-session indicator: how often it ASKS `session_status`. That module keeps its
# own ~16 s throttle on the actual `squeue`, so a tick inside the window is a dict read —
# this cadence only decides how fast the icon reacts once the answer changes.
_CURATION_TICK_S = 6.0


def _ts_cell(text: str, color: str, extra: str = ""):
    """Tiny monospace cell for the tilt-series table in the metadata popup."""
    ui.label(text).style(f"font-size: 9px; font-family: 'IBM Plex Mono', monospace; color: {color}; {extra}")


# Journey nav glyph: three ascending bars — the surface carries per-TS progress and
# statistics across the whole pipeline, which the previous ringed-lines circle said
# nothing about. No axis: at 18 px the bars alone are the legible read.
_TOMO_DASHBOARD_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
    '<line x1="5.5" y1="19" x2="5.5" y2="14"/>'
    '<line x1="12" y1="19" x2="12" y2="9.5"/>'
    '<line x1="18.5" y1="19" x2="18.5" y2="5"/>'
    "</svg>"
)
_SB_INFO = "#c0cad4"
_AVATAR_PALETTE = ["#3b82f6", "#8b5cf6", "#06b6d4", "#10b981", "#f59e0b", "#ec4899"]

# Rail count badges: how much data is behind an icon, without opening it. Recomputed off
# the event loop on this cadence (disk — star rows + a stat per volume, memoized on the
# stars' mtimes, so an unchanged project costs two stats a tick), and repainted only when
# a number actually moves. Load-bearing chrome, so the look is inline on the element —
# ui/main_ui.py's shell stylesheet is served stale on this deployment.
_COUNTS_TICK_S = 15.0
_SB_BADGE_STYLE = (
    "position: absolute; right: -2px; bottom: -2px; min-width: 14px; height: 13px; "
    "padding: 0 3px; border-radius: 7px; background: #e2e8f0; color: #475569; "
    f"{FONT} font-size: 8px; font-weight: 700; line-height: 13px; text-align: center; "
    "pointer-events: none; box-shadow: 0 0 0 1.5px #f8fafc; display: none;"
)

_JOBS_TIP = (
    "Jobs — the pipeline roster. Click it while you are already here to collapse the "
    "roster and give an open job page the full width."
)
_GALLERY_TIP = "Tomograms — the wall of reconstructions, all of them at once"


def _tomo_badge_tip(counts) -> str:
    """The Tomograms icon's hover: the badge number, spelled out by where it came from.
    A row whose volume is not on disk is called out rather than folded into the total —
    the count is of what the project's stars DESCRIBE, and that is not always what exists."""
    if counts.total == 0:
        return f"{_GALLERY_TIP}\nNothing reconstructed or imported yet."
    parts = []
    if counts.reconstructed:
        parts.append(f"{counts.reconstructed} reconstructed")
    if counts.imported:
        parts.append(f"{counts.imported} imported")
    text = f"{_GALLERY_TIP}\n{counts.total} tomogram(s) — {' · '.join(parts)}"
    if counts.missing:
        text += f"\n⚠ {counts.missing} volume(s) are not on disk at the path their star records"
    if counts.unreadable:
        text += f"\n⚠ {len(counts.unreadable)} tomograms.star could not be read"
    return text


def _ts_badge_tip(counts) -> str:
    """The Jobs icon's hover: how many tilt-series the pipeline has to work with."""
    if counts.total == 0:
        return f"{_JOBS_TIP}\nNo tilt-series imported yet."
    text = f"{_JOBS_TIP}\n{counts.total} tilt-series"
    if counts.excluded:
        text += f" — {counts.selected} selected for processing, {counts.excluded} excluded"
    return text


def _avatar_color(name: str) -> str:
    return _AVATAR_PALETTE[hash(name) % len(_AVATAR_PALETTE)]


def _inject_svg_color(svg: str, color: str) -> str:
    """Replace currentColor AND inject explicit fill/stroke so Quasar button doesn't swallow it."""
    svg = svg.replace("currentColor", color)
    # If the SVG has no explicit fill or stroke referencing the color yet,
    # stamp a style onto the root element as a fallback.
    if 'style="' in svg:
        svg = svg.replace('style="', f'style="fill:{color};stroke:{color};', 1)
    else:
        svg = svg.replace("<svg", f'<svg style="fill:{color};stroke:{color};"', 1)
    return svg


def _resolve_array_job_dir(job_model, project_path: Path | None = None) -> Path | None:
    """Resolve job directory for an array job model."""
    if not job_model:
        return None
    stored = (job_model.paths or {}).get("job_dir")
    if stored:
        p = Path(stored)
        if p.is_dir():
            return p
    if project_path:
        rjn = getattr(job_model, "relion_job_name", None)
        if rjn:
            p = project_path / rjn.rstrip("/")
            if p.is_dir():
                return p
    return None


def _get_array_progress(job_model, project_path: Path | None = None) -> TaskProgress | None:
    """Return a TaskProgress for array jobs, or None (no job dir / no manifest).

    Statuses are resolved PER MANIFEST ITEM via scan_statuses (an item can carry
    both `.ok` and `.fail` from a superseded submission; `.ok` wins, matching
    the per-TS sub-rows), and the tally is TaskProgress — the same settledness
    arithmetic the task tracker uses. Skipped (muted) items count as settled, so
    a job whose remaining items are all ok/skip reads as complete instead of
    sitting at 5/6 forever.
    """
    from services.array_tasks import manifest_items, progress

    job_dir = _resolve_array_job_dir(job_model, project_path)
    if job_dir is None:
        return None

    items = manifest_items(job_dir)
    if not items:
        return None
    return progress(job_dir, items)


def _get_array_ts_statuses(
    job_model, project_path: Path | None = None
) -> tuple[list[str], dict[str, str], dict[str, str]] | None:
    """Return (items, statuses, display_names) for per-TS sub-rows, or None."""
    from services.array_tasks import manifest_items, shorten_ts_names, scan_statuses

    job_dir = _resolve_array_job_dir(job_model, project_path)
    if job_dir is None:
        return None

    items = manifest_items(job_dir)
    if not items:
        return None

    statuses = scan_statuses(job_dir, items)
    display_names = shorten_ts_names(items)
    return items, statuses, display_names


class RosterWidget(FingerprintedView):
    """The pipeline sidebar (job roster).

    Inherits FingerprintedView: `refresh()` is a no-op when nothing the
    sidebar would render has changed since last paint. This matters because
    the status poller calls refresh() every 3 s — without the gate, every
    click target in the sidebar would be torn down and rebuilt on each
    tick, dropping in-flight clicks and resetting hover state.

    Row "working" indicators for RUNNING jobs are a self-contained inline
    SVG/SMIL pulsating dot (ui/status_indicator._running_spinner_html) — no
    server-side spinner tick, and no stylesheet dependency.
    """

    def __init__(self, panel: "PipelineBuilderPanel"):
        super().__init__()
        self.panel = panel
        self._flash_phase: str | None = None
        self._roster_visible: bool = True
        self._roster_phase: str | None = None
        # Which workspace view is showing (pipeline / workbench / journey).
        # Drives the nav-icon highlight; set by workspace _switch_to via
        # set_active_mode. Starts "pipeline" (the default view at load).
        self._active_mode: str = "pipeline"
        # Last (status, scope, error) painted onto the rail's curation-session indicator, so
        # its timer only touches the DOM when the session state actually moved.
        self._curation_paint: tuple[str, str, str] | None = None
        # Last (tomogram, tilt-series) counts painted onto the rail badges, so their timer
        # only touches the DOM when a number actually moved.
        self._counts_paint: tuple | None = None
        self._refs: dict = {}
        # Per-instance expansion state for per-TS sub-rows, persisted across
        # roster refreshes (status_poller refreshes the roster every few seconds
        # and would otherwise collapse rows the user had opened).
        self._expanded_instances: dict[str, bool] = {}
        # Per-tick cache of array job state. Populated by signature(), read by
        # render(). Keyed by instance_id. Avoids redundant disk reads per tick.
        self._array_progress_cache: dict[str, TaskProgress | None] = {}
        self._array_ts_cache: dict[str, tuple[list[str], dict[str, str], dict[str, str]] | None] = {}

    def _get_container(self) -> Any:
        return self.panel.roster_panel

    def _status_widget(self, instance_id: str):
        from ui.status_indicator import _dot_html, _running_spinner_html

        job_model = current_project_state().jobs.get(instance_id)
        if not job_model:
            BoundStatusDot(instance_id)
            return

        def _content(status, jm=job_model):
            if status == JobStatus.RUNNING:
                # Pulsating dot for RUNNING jobs — self-contained inline SVG/SMIL
                # (no stylesheet dependency; see _running_spinner_html).
                return _running_spinner_html(14, "#3b82f6")
            return _dot_html(status, is_orphaned=jm.is_orphaned)

        ui.html("", sanitize=False, tag="span").bind_content_from(job_model, "execution_status", backward=_content)

    # ── Roster ────────────────────────────────────────────────────────────────

    def signature(self) -> Any:
        """Fingerprint of every input the roster's render() reads.

        Cheap in-memory reads dominate. Per-array-job disk reads (~25 file
        existence checks per array job) match what render() does today, so
        the signature path is not adding new I/O — it lets us skip the
        more expensive DOM rebuild downstream.
        """
        panel = self.panel
        ui_mgr = panel.ui_mgr
        jobs = current_project_state().jobs

        # Populate caches that render() will re-read so the two stay in lockstep.
        # (No memoization across signature+render; they touch the same files,
        # but file-system caching makes the second read cheap.)
        self._array_progress_cache = {}
        self._array_ts_cache = {}
        for iid, jm in jobs.items():
            if "array_throttle" not in getattr(jm, "USER_PARAMS", set()):
                continue
            self._array_progress_cache[iid] = _get_array_progress(jm, ui_mgr.project_path)
            if self._expanded_instances.get(iid):
                self._array_ts_cache[iid] = _get_array_ts_statuses(jm, ui_mgr.project_path)

        per_job = tuple(
            (
                iid,
                getattr(jm, "execution_status", None),
                getattr(jm, "species_id", None),
                getattr(jm, "relion_job_name", None),
                bool(getattr(jm, "is_orphaned", False)),
                bool(getattr(jm, "IS_INTERACTIVE", False)),
            )
            for iid, jm in sorted(jobs.items())
        )

        # For expanded array jobs, fold in the per-TS status detail so toggling
        # individual TS task outcomes (ok ↔ fail) re-renders even when summary
        # counts are unchanged. For collapsed jobs, summary is enough.
        array_state = tuple(
            (
                iid,
                self._expanded_instances.get(iid, False),
                self._array_progress_cache.get(iid),
                tuple(sorted((self._array_ts_cache.get(iid) or (None, {}, None))[1].items()))
                if self._expanded_instances.get(iid)
                else None,
            )
            for iid in sorted(self._array_progress_cache.keys())
        )

        return (
            self._flash_phase,
            self._roster_visible,
            tuple(ui_mgr.selected_jobs),
            ui_mgr.active_instance_id,
            ui_mgr.is_running,
            per_job,
            array_state,
            # Species pill draws name + color: a rename / recolor in the workbench
            # must repaint the row (roadmap 08 S0.3).
            current_project_state().species_identity(),
            # The PARTICLES header's import button carries a green dot once tomograms have
            # been imported. Without this input the dot only appeared on the NEXT unrelated
            # change, so a successful import looked like it had done nothing.
            self._imported_tomograms_fingerprint(),
        )

    @staticmethod
    def _imported_tomograms_fingerprint() -> tuple:
        rec = current_project_state().imported_tomograms
        if rec is None:
            return ()
        return (bool(rec.star_path), int(rec.count or 0), len(rec.batches or []))

    def refresh(self):
        """Render the roster if its signature changed since last paint.

        Same call site as before — but inherits FingerprintedView's gate so
        the 3 s status poll no longer rebuilds the sidebar on every tick
        when nothing has actually changed.
        """
        super().refresh()

    def render(self):
        panel = self.panel
        for phase_id, jobs in PHASE_JOBS.items():
            icon_or_svg, phase_label, _ = PHASE_META[phase_id]
            is_flashing = self._flash_phase == phase_id

            with (
                ui.element("div")
                .props(f'id="{ROSTER_ANCHOR[phase_id]}"')
                .style(
                    "display: flex; align-items: center; gap: 5px; "
                    "padding: 4px 8px 3px 10px; "
                    "background: #f1f5f9; border-bottom: 1px solid #e5e7eb; "
                    "position: sticky; top: 0; z-index: 2;"
                )
            ):
                if icon_or_svg.startswith("<svg"):
                    ui.html(self._load_svg(icon_or_svg).replace("currentColor", "#94a3b8"), sanitize=False).style(
                        "width: 12px; height: 12px; flex-shrink: 0; display: flex;"
                    )
                else:
                    ui.icon(icon_or_svg, size="12px").style("color: #94a3b8; flex-shrink: 0;")

                ui.label(phase_label.upper()).style(
                    "font-size: 9px; font-weight: 700; color: #94a3b8; letter-spacing: 0.07em; line-height: 1;"
                )
                if phase_id == PHASE_PARTICLES:
                    ui.space()
                    self._build_new_species_btn()
                    self._build_import_tomograms_btn()
                    self._build_aggregation_merge_btn()

            for job_type in jobs:
                instances = panel.ui_mgr.get_instances_for_type(job_type)

                if not instances:
                    # Unselected job type — single clickable row
                    if is_flashing:
                        row_bg, l_border, name_color = "#fefce8", "#fde68a", "#78716c"
                    else:
                        row_bg, l_border, name_color = "transparent", "transparent", "#9ca3af"

                    with (
                        ui.element("div")
                        .style(
                            f"display: flex; align-items: center; gap: 6px; "
                            f"padding: 4px 8px 4px 10px; cursor: pointer; "
                            f"background: {row_bg}; border-left: 2px solid {l_border};"
                        )
                        .on("click", lambda j=job_type: self._on_unselected_click(j))
                    ):
                        ui.icon("check_box_outline_blank", size="13px").style("color: #d1d5db; flex-shrink: 0;")
                        ui.label(get_job_display_name(job_type)).style(
                            f"{MONO} font-size: 11px; font-weight: 400; color: {name_color}; "
                            "flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                        )

                elif len(instances) == 1:
                    # Single instance — flat row, no header/instance split
                    instance_id = instances[0]
                    self._render_instance_row(panel, job_type, instance_id, indent=10, show_add=True)

                else:
                    # Multiple instances — header + instance rows
                    missing = missing_deps(job_type, set(panel.ui_mgr.selected_jobs))
                    any_active = any(panel.ui_mgr.active_instance_id == iid for iid in instances)
                    header_border = "#3b82f6" if any_active else "#e5e7eb"

                    with ui.element("div").style(
                        f"display: flex; align-items: center; gap: 6px; "
                        f"padding: 4px 8px 4px 10px; "
                        f"background: #f8fafc; border-left: 2px solid {header_border};"
                    ):
                        ui.label(get_job_display_name(job_type)).style(
                            f"{MONO} font-size: 11px; font-weight: 600; color: #374151; "
                            "flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                        )
                        ui.label(str(len(instances))).style(
                            "font-size: 9px; font-weight: 700; color: #6b7280; "
                            "background: #e5e7eb; border-radius: 999px; "
                            "padding: 1px 5px; flex-shrink: 0;"
                        )
                        if missing:
                            ui.icon("warning", size="11px").style("color: #f59e0b; flex-shrink: 0;").tooltip(
                                "Missing: " + ", ".join(get_job_display_name(d) for d in missing)
                            )
                        (
                            ui.button(icon="add", on_click=lambda j=job_type: panel.prompt_species_and_add(j))
                            .props("flat dense round size=xs")
                            .style("color: #6b7280; flex-shrink: 0;")
                            .tooltip(f"Add another {get_job_display_name(job_type)}")
                        )

                    for instance_id in instances:
                        self._render_instance_row(panel, job_type, instance_id, indent=18)

    def _render_instance_row(self, panel, job_type, instance_id, indent=18, show_add=False):
        """Render a single job instance row — single line with icons at end."""
        job_model = current_project_state().jobs.get(instance_id)

        base_name = get_job_display_name(job_type)
        relion_job_name = getattr(job_model, "relion_job_name", None) if job_model else None
        if relion_job_name:
            job_folder = relion_job_name.rstrip("/").split("/")[-1]
            display_text = f"{base_name} ({job_folder})"
        else:
            suffix = InstanceId.split(instance_id)[1]
            if suffix is not None:
                display_text = f"{base_name} #{suffix}" if suffix.isdigit() else f"{base_name} ({suffix})"
            else:
                display_text = base_name

        species_id = getattr(job_model, "species_id", None) if job_model else None
        species = None
        if species_id and panel.ui_mgr.project_path:
            from services.project_state import get_project_state_for

            s_state = get_project_state_for(panel.ui_mgr.project_path)
            species = s_state.get_species(species_id)

        is_active = panel.ui_mgr.active_instance_id == instance_id
        if is_active:
            row_bg, l_border = "#f0f4f8", "#475569"
            name_color, name_wt = "#1e293b", "600"
        else:
            row_bg, l_border = "white", "#e5e7eb"
            name_color, name_wt = "#1e293b", "400"

        with ui.element("div").style(
            f"display: flex; align-items: center; gap: 6px; "
            f"padding: 4px 4px 4px {indent}px; "
            f"background: {row_bg}; border-left: 2px solid {l_border}; "
            f"min-width: 0;"
        ):
            # Status dot
            with ui.element("span").style("overflow: visible; line-height: 0; flex-shrink: 0;"):
                self._status_widget(instance_id)
            # Clickable name
            with (
                ui.element("div")
                .style("flex: 1; min-width: 0; cursor: pointer; overflow: hidden;")
                .on("click", lambda iid=instance_id: panel.switch_tab(iid))
            ):
                ui.label(display_text).style(
                    f"{MONO} font-size: 11px; font-weight: {name_wt}; color: {name_color}; "
                    "white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
                )
            # Species badge
            if species:
                render_species_pill(species, compact=True)
            # Inline array progress (e.g., "17/18" green, or "17/18 1!" red).
            # Read from the per-tick cache populated by signature() so render and
            # signature can't disagree on what's being painted.
            progress = self._array_progress_cache.get(instance_id)
            if progress is not None:
                n_ok, n_fail, n_skip, n_total = progress.n_ok, progress.n_fail, progress.n_skip, progress.total
                # Live "running now" chip — shows that a parallel array is actively
                # working even while the settled count (below) sits low between
                # throttle-waves, so the row no longer looks frozen at 0/N.
                if progress.n_running > 0:
                    ui.label(f"▸{progress.n_running}").style(
                        f"{MONO} font-size: 9px; font-weight: 700; color: #2563eb; flex-shrink: 0;"
                    ).tooltip(f"{progress.n_running} tilt-series running now")
                # Skipped (muted) chip — settled by design, shown apart from the
                # ok count so "5/6 ⊘1" isn't mistaken for an incomplete run.
                if n_skip > 0:
                    ui.label(f"⊘{n_skip}").style(
                        f"{MONO} font-size: 9px; font-weight: 600; color: #94a3b8; flex-shrink: 0;"
                    ).tooltip(f"{n_skip} tilt-series skipped (muted / nothing to do)")
                if n_fail > 0:
                    # Show "ok/total fail!" — e.g. "17/18 1!"
                    ui.label(f"{n_ok}/{n_total}").style(
                        f"{MONO} font-size: 9px; font-weight: 600; color: #16a34a; flex-shrink: 0;"
                    )
                    ui.label(f"{n_fail}!").style(
                        f"{MONO} font-size: 9px; font-weight: 700; color: #dc2626; flex-shrink: 0;"
                    )
                elif progress.n_settled == n_total:
                    ui.label(f"{n_ok}/{n_total}").style(
                        f"{MONO} font-size: 9px; font-weight: 600; color: #16a34a; flex-shrink: 0;"
                    )
                elif progress.n_settled > 0:
                    ui.label(f"{n_ok}/{n_total}").style(
                        f"{MONO} font-size: 9px; font-weight: 600; color: #2563eb; flex-shrink: 0;"
                    )
                else:
                    ui.label(f"0/{n_total}").style(
                        f"{MONO} font-size: 9px; font-weight: 600; color: #9ca3af; flex-shrink: 0;"
                    )
            # Row actions (right-aligned, flex-shrink: 0). Config/Logs/Files are
            # deliberately NOT repeated per row — they are reachable from the
            # job's top-bar tab switcher once the job is opened by clicking its
            # name, so the per-row icons were pure redundancy.
            is_array = "array_throttle" in getattr(job_model, "USER_PARAMS", set()) if job_model else False
            with ui.element("div").style("display: flex; align-items: center; gap: 0; flex-shrink: 0;"):
                # Array jobs get a single on-row chevron that toggles the inline
                # per-TS sub-row list in place. It renders for EVERY array job
                # (keyed on array_throttle — the same condition as the sub-rows
                # below), not only those that register a Tasks tab, so a job like
                # Subtomo Extraction can't end up with a list it has no control to
                # collapse. The glyph reflects the current expand state so it reads
                # as a real toggle.
                from ui.job_plugins import get_extra_tabs

                if is_array:
                    # Seed the same default the sub-row renderer uses (expanded
                    # while running) so the glyph and the list can't disagree on
                    # the very first paint.
                    is_running_job = job_model is not None and job_model.execution_status == JobStatus.RUNNING
                    expanded_now = self._expanded_instances.setdefault(instance_id, is_running_job)
                    chevron = "expand_more" if expanded_now else "chevron_right"
                    (
                        ui.button(icon=chevron, on_click=lambda iid=instance_id: self._toggle_ts_expansion(iid))
                        .props("flat dense round size=xs color=grey-7")
                        .style("flex-shrink: 0;")
                        .tooltip("Toggle tilt-series list")
                    )
                # Extra tabs other than "tasks" still render as on-row nav buttons.
                # ("tasks" is the toggle above and stays reachable from the top bar.)
                for et in get_extra_tabs(job_type):
                    if et.key == "tasks":
                        continue
                    (
                        ui.button(
                            icon=et.icon,
                            on_click=lambda iid=instance_id, tk=et.key: panel.switch_to_job_subsection(iid, tk),
                        )
                        .props("flat dense round size=xs color=grey-7")
                        .style("flex-shrink: 0;")
                        .tooltip(et.label)
                    )
                if show_add and not panel.ui_mgr.is_running:
                    (
                        ui.button(icon="add", on_click=lambda j=job_type: panel.prompt_species_and_add(j))
                        .props("flat dense round size=xs color=grey-5")
                        .style("flex-shrink: 0;")
                        .tooltip(f"Add {get_job_display_name(job_type)}")
                    )
                if not panel.ui_mgr.is_running:
                    (
                        ui.button(icon="close", on_click=lambda _, iid=instance_id: self._on_remove_click(iid))
                        .props("flat dense round size=xs color=grey-4")
                        .tooltip("Remove")
                    )

        # ── Per-TS sub-rows (collapsible, for array jobs) ──
        # `is_array` computed above with the row actions; reuse it so the chevron
        # and the sub-rows are gated on exactly the same condition.
        if is_array:
            is_running = job_model.execution_status == JobStatus.RUNNING
            # Default to expanded while running; persist any user toggle across refreshes.
            expanded = self._expanded_instances.setdefault(instance_id, is_running)
            # signature() populated this cache for expanded array jobs only.
            # When collapsed the on-row chevron is the only control; the per-TS
            # detail loads on the next tick after the user expands.
            ts_data = self._array_ts_cache.get(instance_id) if expanded else None
            if ts_data is not None:
                items, statuses, display_names = ts_data
            else:
                items, statuses, display_names = [], {}, {}
            job_dir = _resolve_array_job_dir(job_model, panel.ui_mgr.project_path)
            self._render_ts_sub_rows(
                instance_id, items, statuses, display_names, indent + 8, expanded=expanded, job_dir=job_dir
            )
        elif (
            job_model is not None
            and not getattr(job_model, "IS_INTERACTIVE", False)
            and getattr(job_model, "relion_job_name", None)
        ):
            # Non-array jobs that have already been deployed: surface a
            # small "single-shot" hint so users don't think a per-TS
            # collapsible row is missing/broken. e.g. SUBTOMO_EXTRACTION
            # is one `relion_tomo_subtomo` invocation across all TS at
            # once — there is no per-TS subtask to expand into.
            with ui.element("div").style(
                f"display: flex; align-items: center; gap: 5px; "
                f"padding: 0 4px 0 {indent + 12}px; height: 12px; background: transparent;"
            ):
                ui.icon("horizontal_rule", size="9px").style("color: #cbd5e1; flex-shrink: 0;")
                ui.label("single-shot job (no per-TS tasks)").style(
                    f"{MONO} font-size: 8px; color: #94a3b8; font-style: italic;"
                )

    def _render_ts_sub_rows(
        self,
        instance_id: str,
        items: list[str],
        statuses: dict[str, str],
        display_names: dict[str, str],
        indent: int,
        expanded: bool = False,
        job_dir: Path | None = None,
    ):
        """Render the collapsible per-tilt-series status list under an array job.

        Visibility is driven solely by the on-row chevron toggle in
        _render_instance_row (`expanded`); there is no separate arrow row
        underneath the parent anymore — it used to render even before a job had
        produced any tasks, which broke the visual flow of the roster. When the
        list is expanded but the job hasn't run yet, a muted hint is shown
        instead of an empty box. Rows are ordered by (stage, beam) ascending.
        Clicking a row navigates the main pane to this job's Tasks tab and scrolls
        to the matching entry there — no pop-up dialog, nothing to get auto-closed
        by a background refresh.
        """
        from services.array_tasks import sort_ts_by_position

        _TS_COLORS = {"ok": "#16a34a", "fail": "#dc2626", "running": "#2563eb", "pending": "#d1d5db"}
        _TS_ICONS = {"ok": "check_circle", "fail": "error", "running": "sync", "pending": "radio_button_unchecked"}

        container = ui.element("div").style(
            f"display: {'block' if expanded else 'none'}; border-left: 2px solid #e2e8f0; margin-left: {indent - 4}px;"
        )

        display_order = sort_ts_by_position(items)
        panel = self.panel
        with container:
            if not display_order:
                ui.label("Per-tilt-series tasks appear here once the job starts running.").style(
                    f"{MONO} font-size: 8px; color: #94a3b8; font-style: italic; padding: 2px 6px 2px 8px;"
                )
            for ts_name in display_order:
                status = statuses.get(ts_name, "pending")
                color = _TS_COLORS.get(status, _TS_COLORS["pending"])
                icon_name = _TS_ICONS.get(status, "radio_button_unchecked")
                short_name = display_names.get(ts_name, ts_name)

                row_style = (
                    "display: flex; align-items: center; gap: 5px; "
                    "padding: 2px 6px 2px 8px; border-bottom: 1px solid #f8fafc;"
                )
                # Active when there is something to jump to (at least queued → started).
                if status != "pending":
                    row_style += " cursor: pointer;"

                row = ui.element("div").style(row_style)
                if status != "pending":

                    def _jump(_e, iid=instance_id, tn=ts_name):
                        # One-shot deep link; the Tasks tab pops this to auto-expand
                        # and scroll the matching row into view.
                        panel.ui_mgr.focus_ts_by_instance[iid] = tn
                        panel.switch_to_job_subsection(iid, "tasks")

                    row.on("click", _jump)

                with row:
                    ui.icon(icon_name, size="11px").style(f"color: {color}; flex-shrink: 0;")
                    ui.label(short_name).style(
                        f"{MONO} font-size: 10px; color: #64748b; "
                        "flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                    )
                    ui.label(status).style(
                        f"{MONO} font-size: 8px; color: {color}; text-transform: uppercase; "
                        "font-weight: 600; flex-shrink: 0;"
                    )

    def _toggle_ts_expansion(self, instance_id: str) -> None:
        """Flip the persisted expansion state for an array job's per-TS sub-rows."""
        self._expanded_instances[instance_id] = not self._expanded_instances.get(instance_id, False)
        self.refresh()

    async def _on_unselected_click(self, job_type: JobType):
        panel = self.panel
        if panel.ensure_pipeline_mode:
            panel.ensure_pipeline_mode()
        if panel.ui_mgr.is_running:
            return
        missing = missing_deps(job_type, set(panel.ui_mgr.selected_jobs))
        if missing:
            ui.notify(
                f"{get_job_display_name(job_type)} typically requires: "
                + ", ".join(get_job_display_name(d) for d in missing),
                type="warning",
                timeout=3000,
            )
        await panel.prompt_species_and_add(job_type)

    async def _on_remove_click(self, instance_id: str):
        panel = self.panel
        if panel.ui_mgr.is_running:
            return

        project_path = panel.ui_mgr.project_path
        if not project_path:
            panel.remove_instance_from_pipeline(instance_id)
            return

        from services.project_state import get_project_state_for

        state = get_project_state_for(project_path)
        job_model = state.jobs.get(instance_id)
        status = job_model.execution_status if job_model else None

        # Interactive jobs get a lightweight removal (no file trashing).
        if job_model and getattr(job_model, "IS_INTERACTIVE", False):
            await self._remove_interactive_job(instance_id, job_model, state)
            return

        if status not in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            panel.remove_instance_from_pipeline(instance_id)
            return

        from services.scheduling_and_orchestration.pipeline_deletion_service import get_deletion_service

        deletion_service = get_deletion_service()
        preview = None
        if project_path and job_model.relion_job_name:
            preview = deletion_service.preview_deletion(
                project_path, job_model.relion_job_name, job_resolver=panel.backend.pipeline_orchestrator.job_resolver
            )

        # Page-slot parented: the roster is a FingerprintedView whose poll rebuilds these
        # rows, and a confirm dialog parented in a row that gets torn down takes the delete
        # with it. See ui/components/dialogs.py.
        with dialog_host(), ui.dialog() as dialog, ui.card().classes("w-[28rem]"):
            ui.label(f"Delete {get_instance_display_name(instance_id, job_model)}?").classes("text-lg font-bold")
            ui.label("This will move the job files to Trash/ and remove it from the pipeline.").classes(
                "text-sm text-gray-600 mb-2"
            )

            if preview and preview.get("success") and preview.get("downstream_count", 0) > 0:
                downstream = preview.get("downstream_jobs", [])
                with ui.card().classes("w-full bg-orange-50 border border-orange-200 p-3 mb-2"):
                    with ui.row().classes("items-center gap-2 mb-2"):
                        ui.icon("warning", size="20px").classes("text-orange-600")
                        ui.label(f"{len(downstream)} job(s) will become orphaned:").classes(
                            "text-sm font-bold text-orange-800"
                        )
                    with ui.column().classes("gap-1 ml-6"):
                        for detail in downstream:
                            with ui.row().classes("items-center gap-2"):
                                ui.label(detail.get("path", "Unknown")).classes("text-xs font-mono text-gray-700")
                                if detail.get("type"):
                                    ui.label(f"({detail['type']})").classes("text-xs text-gray-500")
                                ui.label(f"- {detail.get('status', 'Unknown')}").classes("text-xs text-gray-500")
                    ui.label("These jobs will have broken input references and may fail if re-run.").classes(
                        "text-xs text-orange-700 mt-2"
                    )
            else:
                ui.label("No downstream jobs will be affected.").classes(
                    "text-sm text-green-600 bg-green-50 p-2 rounded"
                )

            with ui.row().classes("w-full justify-end mt-4 gap-2"):
                house_button("Cancel", dialog.close)

                async def confirm():
                    dialog.close()
                    try:
                        result = await panel.backend.delete_job(
                            instance_id_to_job_type(instance_id).value,
                            project_path=panel.ui_mgr.project_path,
                            instance_id=instance_id,
                        )
                        if result.get("success"):
                            orphans = result.get("orphaned_jobs", [])
                            if orphans:
                                ui.notify(
                                    f"Deleted. {len(orphans)} downstream job(s) orphaned.", type="warning", timeout=5000
                                )
                            else:
                                ui.notify("Job deleted.", type="positive")
                            panel.remove_instance_from_pipeline(instance_id)
                        else:
                            ui.notify(f"Delete failed: {result.get('error')}", type="negative", timeout=8000)
                    except Exception as e:
                        ui.notify(f"Error: {e}", type="negative")

                house_button("Delete", confirm, kind="danger")

        dialog.open()

    async def _remove_interactive_job(self, instance_id: str, job_model, state):
        """Custom removal for interactive jobs — preserves data, warns about downstream."""
        panel = self.panel
        downstream = []
        if job_model.execution_status == JobStatus.SUCCEEDED:
            for iid, jm in state.jobs.items():
                if iid == instance_id:
                    continue
                if jm.execution_status == JobStatus.SUCCEEDED:
                    # Check if this job consumed the tilt filter's output
                    for path_val in (jm.paths or {}).values():
                        if path_val and "tiltseries_filtered" in str(path_val):
                            downstream.append(iid)
                            break

        with dialog_host(), ui.dialog() as dialog, ui.card().classes("w-[28rem]"):
            ui.label(f"Remove {get_instance_display_name(instance_id, job_model)}?").classes("text-lg font-bold")
            ui.label("Your labels and thumbnails will be preserved and restored if you re-add this job.").classes(
                "text-sm text-gray-600 mb-2"
            )

            if downstream:
                with ui.card().classes("w-full bg-orange-50 border border-orange-200 p-3 mb-2"):
                    with ui.row().classes("items-center gap-2 mb-1"):
                        ui.icon("warning", size="20px").classes("text-orange-600")
                        ui.label(f"{len(downstream)} downstream job(s) used the filtered tilts:").classes(
                            "text-sm font-bold text-orange-800"
                        )
                    with ui.column().classes("gap-1 ml-6"):
                        for iid in downstream:
                            dm = state.jobs.get(iid)
                            name = get_instance_display_name(iid, dm)
                            ui.label(name).classes("text-xs font-mono text-gray-700")
                    ui.label(
                        "These jobs were processed with the filtered tilt set. "
                        "If you re-add the filter and select different tilts, these results "
                        "will be stale and should be re-run to stay consistent."
                    ).classes("text-xs text-orange-700 mt-2")

            with ui.row().classes("w-full justify-end mt-4 gap-2"):
                house_button("Cancel", dialog.close)

                def confirm():
                    dialog.close()
                    # Remove from pipeline but keep labels in project state.
                    del state.jobs[instance_id]
                    state.job_path_mapping.pop(instance_id, None)
                    panel.remove_instance_from_pipeline(instance_id)
                    ui.notify("Tilt filter removed. Labels preserved.", type="info")

                house_button("Remove", confirm, kind="danger")

        dialog.open()

    # ── Roster toggle ─────────────────────────────────────────────────────────

    def toggle(self):
        self._roster_visible = not self._roster_visible
        if self.panel.roster_panel is not None:
            self.panel.roster_panel.style(f"display: {'flex' if self._roster_visible else 'none'};")
        self._update_pipeline_btn_style()
        self.refresh()

    def _clear_flash(self):
        self._flash_phase = None
        self.refresh()

    def _update_pipeline_btn_style(self):
        """Repaint the layers icon for the current roster visibility.

        Updates the icon element IN PLACE. It used to `container.clear()` + rebuild, which
        also destroyed the Quasar tooltip parented to the container — so the Pipeline button
        silently lost its hover text after the first mode switch.
        """
        container = self._refs.get("pipeline_btn")
        if container is None:
            return
        bg = SB_ABG if self._roster_visible else "transparent"
        color = SB_ACT if self._roster_visible else SB_MUTE
        container.style(
            f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
            f"background: {bg}; position: relative; "  # relative: the count badge anchors to it
            f"display: flex; align-items: center; justify-content: center; "
            f"cursor: pointer; flex-shrink: 0;"
        )
        icon = self._refs.get("pipeline_btn_icon")
        if icon is not None:
            icon.content = self._load_svg("layers.svg").replace("currentColor", color)

    def set_active_mode(self, mode: str):
        """Highlight the nav icon for the active view (exactly one lit) and hide the job
        roster everywhere except the pipeline view. Driven by the workspace's _switch_to.

        The roster is the pipeline view's OWN navigation, so it goes away with that view
        rather than half-following it: Particles / Journey / Tomograms / the pick viewer
        each get the full width, and collapse-expand is what it always was — a choice about
        how much room the open job page gets, only meaningful while one is open. Anything
        else is a second, invisible state variable ("am I on Particles WITH the roster?")
        that nothing on screen explains.
        """
        self._active_mode = mode
        if self.panel.roster_panel is not None:
            if mode != "pipeline":
                self.panel.roster_panel.style("display: none;")
            else:
                self.panel.roster_panel.style(f"display: {'flex' if self._roster_visible else 'none'};")
        # Pipeline icon keeps its roster-aware styling when it's the active view;
        # otherwise it dims. The other views are a plain background highlight.
        if mode == "pipeline":
            self._update_pipeline_btn_style()
        else:
            pc = self._refs.get("pipeline_btn")
            if pc is not None:
                pc.style("background: transparent;")
        for ref_key, m in (("wb_btn", "workbench"), ("dashboard_btn", "journey"), ("gallery_btn", "gallery")):
            c = self._refs.get(ref_key)
            if c is not None:
                c.style(f"background: {SB_ABG if mode == m else 'transparent'};")

    def _on_pipeline_icon(self):
        """Layers icon: return to the pipeline view if we're elsewhere; if
        already in pipeline, toggle the job roster (its secondary function)."""
        if self._active_mode != "pipeline":
            ensure = self.panel.ensure_pipeline_mode
            if ensure is not None:
                ensure()
        else:
            self.toggle()

    async def _open_journey(self):
        """Switch to (or toggle off) the embedded journey view. Async because
        the first open lazily builds the panel behind a spinner."""
        tj = self.panel.toggle_journey
        if tj is not None:
            await tj()

    async def _open_gallery(self):
        """Switch to (or toggle off) the tomogram gallery. Lazily built like the journey."""
        tg = self.panel.toggle_gallery
        if tg is not None:
            await tg()

    # ── Sidebar ───────────────────────────────────────────────────────────────

    def build_sidebar(self):
        panel = self.panel
        if panel.primary_sidebar is None:
            return

        state = current_project_state()

        with panel.primary_sidebar:
            ui.element("div").style("height: 8px;")

            self._build_project_avatar(state)

            ui.element("div").style("height: 4px;")
            self._sb_sep()
            ui.element("div").style("height: 4px;")

            self._sb_svg_btn(
                "layers.svg", _JOBS_TIP, self._on_pipeline_icon, ref_key="pipeline_btn", active=True, badge=True
            )

            if panel.toggle_workbench is not None:
                ui.element("div").style("height: 1px;")
                wb_btn = self._sb_svg_btn(
                    "particle.svg",
                    "Particles registry — species, templates, picks & curation",
                    panel.toggle_workbench,
                    ref_key="wb_btn",
                )
                panel.callbacks["wb_btn"] = wb_btn

            # Tomograms — the birds-eye wall of reconstructions (ui/tomo_gallery.py).
            # The "Tomogram Previews" grid the dashboard consolidation folded away,
            # back as its own view: the Journey answers "how did THIS tilt-series go",
            # the wall answers "how do they all look".
            if panel.toggle_gallery is not None:
                ui.element("div").style("height: 1px;")
                self._sb_svg_btn(
                    "tomo_preview.svg", _GALLERY_TIP, self._open_gallery, ref_key="gallery_btn", badge=True
                )

            # Journey — unified per-TS inspection surface that replaces the old
            # "Tilt Series Journey" matrix and standalone "Candidate Previews"
            # dialog. See services/visualization/ROADMAP.md for the consolidation plan.
            ui.element("div").style("height: 1px;")
            self._build_dashboard_btn()

            # SLURM defaults / resource profiles live inside the project overview popup now.

            ui.element("div").style("height: 10px;")
            self._sb_sep()
            ui.element("div").style("height: 6px;")

            run_slot = ui.element("div").style(
                "width: 100%; display: flex; flex-direction: column; align-items: center; padding: 2px 6px; gap: 3px;"
            )
            self._refs["run_slot"] = run_slot

            self._sb_svg_btn("cross.svg", "Close project", lambda: ui.navigate.to("/"))

            ui.element("div").style("flex: 1;")

            # Pinned to the FOOT of the rail: the curation-session indicator is not a place
            # to navigate to, it is the answer to "is ArtiaX up, and on what". Keeping it out
            # of the view stack above says so without a separator.
            self._build_curation_session_btn()
            ui.element("div").style("height: 6px;")

        self.rebuild_run_slot()
        # How much data is behind the Jobs and Tomograms icons. Painted once now (so a
        # reload lands on real numbers rather than blank badges that fill in 15 s later)
        # and then on the poll; an import repaints immediately via refresh_counts().
        ui.timer(0.2, self._tick_counts, once=True)
        ui.timer(_COUNTS_TICK_S, self._tick_counts)

    # ── Rail count badges ─────────────────────────────────────────────────────

    async def _tick_counts(self):
        """Recount the project's tilt-series and tomograms off the event loop and paint
        the badges. Explicit-path state resolution, not the tab accessor: this runs from a
        timer, where a bare lookup can hand back a blank throwaway state."""
        from services.project_counts import tilt_series_counts, tomogram_counts
        from services.project_state import get_project_state_for

        project_path = self.panel.ui_mgr.project_path
        if project_path is None:
            return

        def _read():
            state = get_project_state_for(project_path)
            return tomogram_counts(state, project_path), tilt_series_counts(state, project_path)

        try:
            tomo, ts = await asyncio.to_thread(_read)
        except Exception:
            # Reported, not swallowed — a badge that silently stops moving is worse than
            # one that never appeared, and the traceback names which reader broke.
            logger.exception("Could not recount project data for the rail badges")
            return
        self._paint_counts(tomo, ts)

    def refresh_counts(self):
        """Repaint the rail badges NOW rather than at the next tick — for the moments the
        user is watching for the number to move (an import just committed)."""
        asyncio.create_task(self._tick_counts())

    def _paint_counts(self, tomo, ts):
        """Gated on the counts it last painted: this runs on a timer, and re-sending
        identical text every 15 s is churn the client processes for nothing."""
        if (tomo, ts) == self._counts_paint:
            return
        self._counts_paint = (tomo, ts)
        self._set_badge("pipeline_btn", ts.total, _ts_badge_tip(ts))
        self._set_badge("gallery_btn", tomo.total, _tomo_badge_tip(tomo))

    def _set_badge(self, ref_key: str, n: int, tooltip: str):
        badge = self._refs.get(f"{ref_key}_badge")
        if badge is not None:
            badge.set_text("999+" if n > 999 else str(n))
            badge.style("display: block;" if n > 0 else "display: none;")
        tip = self._refs.get(f"{ref_key}_tip")
        if tip is not None:
            tip.set_text(tooltip)

    def _build_project_avatar(self, state):
        name = state.project_name or "---"
        initials = name[:3].upper()
        color = _avatar_color(name)

        avatar = (
            ui.element("div")
            .style(
                f"width: 34px; height: 34px; border-radius: 50%; "
                f"background: {color}1a; border: 1.5px solid {color}66; "
                f"display: flex; align-items: center; justify-content: center; "
                f"cursor: pointer; flex-shrink: 0;"
            )
            .on("click", self._open_project_hub)
            .tooltip(name)
        )
        with avatar:
            ui.label(initials).style(
                f"font-size: 9px; font-weight: 700; color: {color}; "
                "letter-spacing: 0.04em; line-height: 1; pointer-events: none;"
            )
        return avatar

    def _render_project_params(self, state, tomo_counts=None) -> None:
        """Render a project's parameter sections (Project / Acquisition /
        Dataset) into the current container. Used by the left pane of the
        project hub; works for any loaded-or-detached ProjectState. SLURM
        defaults intentionally NOT shown here — they live in the landing-page
        settings editor now.

        ``tomo_counts`` is a ``services.project_counts.TomogramCounts`` read off the event
        loop by the caller — the same number the rail badge shows, so the pane and the
        badge cannot disagree. Omitted (None) means "not counted", which renders as no
        Tomograms row rather than as zero."""
        if state is None:
            with ui.element("div").style("padding: 40px 16px; text-align: center;"):
                ui.icon("touch_app", size="22px").style("color: #cbd5e1;")
                ui.label("Select a project to preview its parameters").style(
                    f"{FONT} font-size: 11px; color: #94a3b8; margin-top: 6px;"
                )
            return

        ts_sel = state.import_selected_tilt_series or 0
        ts_tot = state.import_total_tilt_series or 0
        if ts_tot:
            ts_display = f"{ts_sel} of {ts_tot} selected"
        elif ts_sel:
            ts_display = str(ts_sel)
        else:
            ts_display = "---"
        self._render_overview_section(
            "Project",
            [
                ("Name", state.project_name),
                ("Root", str(state.project_path) if state.project_path else "---"),
                ("Movies", state.movies_glob or "---"),
                ("MDOC", state.mdocs_glob or "---"),
                ("Tilt-series", ts_display),
            ],
        )
        self._render_overview_section(
            "Acquisition",
            [
                ("Pixel", f"{fmt(state.microscope.pixel_size_angstrom)} Å"),
                ("Voltage", f"{fmt(state.microscope.acceleration_voltage_kv)} kV"),
                ("Cs", f"{fmt(state.microscope.spherical_aberration_mm)} mm"),
                ("Amp. C.", fmt(state.microscope.amplitude_contrast)),
                ("Dose", f"{fmt(state.acquisition.dose_per_tilt)} e⁻/Å²"),
                ("Tilt ax.", f"{fmt(state.acquisition.tilt_axis_degrees)} °"),
            ],
        )

        # Dataset. Rendered whenever there is ANYTHING to say — a particle-only project
        # has no frames and no tilt-series but can have imported tomograms, and gating the
        # whole section on the frame import is what made those projects read as empty.
        ds_rows: list[tuple] = []
        if tomo_counts is not None and tomo_counts.total:
            split = " · ".join(
                p
                for p in (
                    f"{tomo_counts.reconstructed} reconstructed" if tomo_counts.reconstructed else "",
                    f"{tomo_counts.imported} imported" if tomo_counts.imported else "",
                )
                if p
            )
            ds_rows.append(("Tomograms", f"{tomo_counts.total}  ({split})" if split else str(tomo_counts.total)))
            if tomo_counts.missing:
                # Stated, never folded into the count — the star describes a volume that
                # is not where it says it is, and that is the user's decision to make.
                ds_rows.append(("Not on disk", str(tomo_counts.missing), "#b45309"))
        if state.import_source_directory:
            ds_rows.append(("Source", state.import_source_directory))
        if state.import_frame_extension:
            ds_rows.append(("Format", state.import_frame_extension))
        if ds_rows or state.import_total_positions or state.import_total_tilt_series:
            self._render_overview_section("Dataset", ds_rows)
            self._render_dataset_ts_expansion(state)

        ui.element("div").style("height: 10px;")

    # ── Overview helpers (denser layout, no nested scroll) ────────────────────

    def _overview_section_header(self, title: str) -> None:
        """Category head. Whitespace above it is the separation — no banded background and
        no rules: with a hairline under every row as well, the pane read as a stack of
        boxes with the values pushed to the far margin."""
        ui.element("div").style("height: 14px;")
        ui.label(title).style(
            f"{FONT} padding: 0 12px 4px; font-size: 9px; font-weight: 700; "
            "color: #94a3b8; letter-spacing: 0.09em; text-transform: uppercase;"
        )

    def _render_overview_section(self, title: str, rows: list) -> None:
        """Label · value pairs on one grid. The label column is fixed, so values line up
        and sit NEXT to what names them instead of across a 380 px gulf from it.
        A row may carry a third element: an explicit value colour (warnings)."""
        self._overview_section_header(title)
        for row in rows:
            row_lbl, row_val = row[0], row[1]
            color = row[2] if len(row) > 2 else "#1e40af"
            with ui.element("div").style(
                "display: grid; grid-template-columns: 68px minmax(0, 1fr); "
                "align-items: baseline; padding: 1px 12px; gap: 8px;"
            ):
                ui.label(row_lbl).style(f"{FONT} font-size: 10px; color: #94a3b8;")
                ui.label(str(row_val)).style(f"{MONO} font-size: 10px; color: {color}; word-break: break-all;")

    def _render_dataset_ts_expansion(self, state) -> None:
        """Collapsible per-tilt-series table living on the Dataset row.

        Rows come from the TiltSeriesRegistry (roadmap 02 stage 4) — the
        ProjectState mirror it used to read is gone. Pre-registry projects
        simply have no expansion (counts in the header still render).
        """
        from services.tilt_series import get_registry_for

        try:
            all_ts = list(get_registry_for(state.project_path).all_tilt_series())
        except Exception:
            all_ts = []
        if not all_ts:
            return
        selected_ts = [t for t in all_ts if t.is_selected]
        excluded_ts = [t for t in all_ts if not t.is_selected]
        sel = state.import_selected_tilt_series
        tot = state.import_total_tilt_series
        header_text = f"{sel} of {tot} tilt-series"

        exp = ui.expansion().props("dense header-class=q-px-none").style("width: 100%; background: transparent;")

        with exp.add_slot("header"):
            # Same label column as _render_overview_section — this IS one of its rows, it
            # just happens to open.
            with ui.element("div").style(
                "display: grid; grid-template-columns: 68px minmax(0, 1fr); "
                "align-items: baseline; padding: 1px 12px; gap: 8px; width: 100%;"
            ):
                ui.label("Selected").style(f"{FONT} font-size: 10px; color: #94a3b8;")
                ui.label(header_text).style(f"{MONO} font-size: 10px; color: #1e40af;")

        with exp:
            with ui.element("div").style(
                "display: grid; grid-template-columns: 44px 44px 44px 1fr; gap: 0; "
                "padding: 3px 11px; background: #f8fafc;"
            ):
                for hdr in ("POS", "BEAM", "TILTS", "MDOC"):
                    ui.label(hdr).style(
                        "font-size: 8px; font-weight: 600; color: #94a3b8; "
                        "letter-spacing: 0.04em; text-transform: uppercase;"
                    )
            # Show all rows inline — the outer menu has a single scrollbar.
            for td in sorted(selected_ts, key=lambda x: (x.stage_position, x.beam_position)):
                with ui.element("div").style(
                    "display: grid; grid-template-columns: 44px 44px 44px 1fr; gap: 0; "
                    "padding: 2px 11px; border-top: 1px solid #fafbfc;"
                ):
                    _ts_cell(str(td.stage_position), "#64748b")
                    _ts_cell(str(td.beam_position), "#64748b")
                    _ts_cell(str(td.frame_count), "#64748b")
                    _ts_cell(
                        td.mdoc_filename, "#94a3b8", extra="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"
                    )
            if excluded_ts:
                with ui.element("div").style("padding: 4px 11px; border-top: 1px solid #e2e8f0;"):
                    ui.label(f"+{len(excluded_ts)} excluded").style(
                        "font-size: 9px; font-style: italic; color: #cbd5e1;"
                    )

    async def _open_project_hub(self):
        from nicegui import app as ng_app
        from services.project_state import get_project_state_for
        from services.configs.user_prefs_service import get_prefs_service
        from ui.projects_overview import ProjectsOverview

        panel = self.panel

        prefs_service = get_prefs_service()
        prefs = prefs_service.load_from_app_storage(ng_app.storage.user)
        base_path = prefs.project_base_path or await panel.backend.get_default_project_base()

        current_path_str = str(panel.ui_mgr.project_path.resolve()) if panel.ui_mgr.project_path else None

        # Mutable base path read by the overview each refresh; updated by the
        # BASE input + Recent Locations dropdown. Holding a reference here
        # rather than capturing the value avoids stale closures when the
        # auto-refresh fires.
        current_base = {"path": base_path}
        history_refs: dict = {"container": None, "visible": False, "dropdown": None, "path_input": None}
        overview_ref: dict = {"comp": None}

        # ── switch handler ────────────────────────────────────────────────────

        async def _switch_project(target: Path):
            dialog.close()
            comp = overview_ref.get("comp")
            if comp is not None:
                comp.stop()
            await panel.backend.load_existing_project(str(target))
            loaded_state = get_project_state_for(target)
            panel.ui_mgr.load_from_project(
                project_path=target, scheme_name="loaded", jobs=list(loaded_state.jobs.keys())
            )
            ui.navigate.to("/workspace")

        # ── history helpers ───────────────────────────────────────────────────

        def _render_history():
            c = history_refs.get("container")
            if c is None:
                return
            c.clear()
            roots = prefs_service.prefs.recent_project_roots
            with c:
                if not roots:
                    ui.label("No saved locations").style(
                        f"{FONT} font-size: 10px; color: #cbd5e1; font-style: italic; padding: 8px 12px;"
                    )
                else:
                    for root in roots[:12]:
                        with (
                            ui.element("div")
                            .style("display: flex; align-items: center; gap: 4px; padding: 5px 10px; cursor: pointer;")
                            .classes("hover:bg-slate-50")
                        ):
                            ui.label(root.path).style(
                                f"{MONO} font-size: 10px; color: #475569; flex: 1; "
                                "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                            ).on("click", lambda p=root.path: _use_history(p))
                            (
                                ui.button(icon="close", on_click=lambda p=root.path: _remove_history(p))
                                .props("flat dense round size=xs")
                                .style("color: #cbd5e1; flex-shrink: 0;")
                            )
                with ui.element("div").style(
                    "display: flex; justify-content: flex-end; padding: 4px 10px; border-top: 1px solid #f1f5f9;"
                ):
                    house_button("Clear all", _clear_history)

        def _toggle_history():
            dd = history_refs.get("dropdown")
            if dd is None:
                return
            if history_refs["visible"]:
                dd.style("display: none;")
                history_refs["visible"] = False
            else:
                _render_history()
                dd.style("display: block;")
                history_refs["visible"] = True

        def _close_history():
            dd = history_refs.get("dropdown")
            if dd:
                dd.style("display: none;")
            history_refs["visible"] = False

        async def _use_history(path: str):
            _close_history()
            path_input_ref = history_refs.get("path_input")
            if path_input_ref:
                path_input_ref.value = path
            current_base["path"] = path
            comp = overview_ref.get("comp")
            if comp is not None:
                await comp.refresh()

        def _remove_history(path: str):
            prefs_service.prefs.remove_recent_root(path)
            prefs_service.save_to_app_storage(ng_app.storage.user)
            _render_history()

        def _clear_history():
            prefs_service.prefs.clear_recent_roots()
            prefs_service.save_to_app_storage(ng_app.storage.user)
            _render_history()

        async def _apply_base_change():
            new_base = (history_refs["path_input"].value or "").strip()
            if not new_base:
                return
            current_base["path"] = new_base
            comp = overview_ref.get("comp")
            if comp is not None:
                await comp.refresh()
                if comp._projects:
                    prefs_service.prefs.add_recent_root(new_base)
                    prefs_service.save_to_app_storage(ng_app.storage.user)
                    _render_history()

        # ── selected-project preview (left pane) ─────────────────────────────
        selected_ref = {"path": current_path_str}
        left_refs: dict = {"container": None}

        async def _load_left(path_str):
            from services.project_counts import tomogram_counts

            left = left_refs.get("container")
            if left is None:
                return
            state = None
            if path_str:
                try:
                    state = await panel.backend.read_project_state_detached(path_str)
                except Exception as e:
                    logger.info("Preview load failed for %s: %s", path_str, e)
                    state = None
            counts = None
            if state is not None:
                try:
                    counts = await asyncio.to_thread(tomogram_counts, state, Path(path_str))
                except Exception:
                    # Reported, not swallowed; the pane still renders, minus the row.
                    logger.exception("Could not count tomograms for %s", path_str)
            left.clear()
            with left:
                self._render_project_params(state, counts)

        async def _on_select(target: Path):
            selected_ref["path"] = str(target)
            await _load_left(str(target))

        # ── dialog ────────────────────────────────────────────────────────────

        with (
            ui.dialog() as dialog,
            ui.card().style(
                "width: 96vw; max-width: 1440px; height: 88vh; padding: 0; overflow: hidden; "
                "border-radius: 8px; box-shadow: 0 12px 32px rgba(0,0,0,0.18); "
                "display: flex; flex-direction: column;"
            ),
        ):
            # Base path bar. Width fix: the inner flex row + input carry an
            # explicit width:100% so the input fills the strip instead of
            # shrink-wrapping to ~80 px of intrinsic content width.
            with ui.element("div").style(
                "display: flex; align-items: center; gap: 6px; width: 100%; "
                "padding: 7px 12px; border-bottom: 1px solid #e5e7eb; "
                "background: #f8fafc; position: relative; flex-shrink: 0;"
            ):
                ui.label("BASE").style(
                    f"{FONT} font-size: 9px; font-weight: 700; color: #94a3b8; letter-spacing: 0.09em; flex-shrink: 0;"
                )

                with ui.element("div").style("flex: 1 1 auto; position: relative; min-width: 0; width: 100%;"):
                    with ui.element("div").style("display: flex; align-items: center; gap: 4px; width: 100%;"):
                        path_input = (
                            ui.input(value=current_base["path"])
                            .props("dense borderless")
                            .classes("flex-1")
                            .style(
                                f"width: 100%; min-width: 0; font-size: 10px; {MONO} "
                                "color: #1e293b; background: transparent;"
                            )
                            .on("blur", _apply_base_change)
                        )
                        history_refs["path_input"] = path_input
                        (
                            ui.button(icon="expand_more", on_click=_toggle_history)
                            .props("flat dense round size=xs")
                            .style("color: #94a3b8; flex-shrink: 0;")
                            .tooltip("Recent locations")
                        )

                    history_dropdown = ui.element("div").style(
                        "display: none; position: absolute; top: calc(100% + 4px); left: 0; right: 0; "
                        "z-index: 9999; background: white; "
                        "border: 1px solid #e2e8f0; border-radius: 5px; "
                        "box-shadow: 0 4px 16px rgba(15,23,42,0.10); "
                        "max-height: 200px; overflow-y: auto;"
                    )
                    history_refs["dropdown"] = history_dropdown
                    with history_dropdown:
                        history_refs["container"] = ui.element("div").style("width: 100%;")

            # Two-column body: left = selected project's params, right = the
            # all-projects roster (single-click previews here; arrow opens).
            with ui.element("div").style(
                "display: flex; flex-direction: row; align-items: stretch; width: 100%; flex: 1 1 auto; min-height: 0;"
            ):
                # LEFT — parameter panel for the previewed project.
                with ui.element("div").style(
                    "flex: 0 0 380px; max-width: 40%; border-right: 1px solid #e5e7eb; "
                    "display: flex; flex-direction: column; min-height: 0; background: #ffffff;"
                ):
                    with ui.element("div").style(
                        "padding: 8px 14px; border-bottom: 1px solid #f1f5f9; flex-shrink: 0;"
                    ):
                        ui.label("Project parameters").style(
                            f"{FONT} font-size: 12px; font-weight: 600; color: #0f172a;"
                        )
                        ui.label("Click a project to preview · use the arrow to open it").style(
                            f"{FONT} font-size: 9px; color: #94a3b8; margin-top: 1px;"
                        )
                    with ui.scroll_area().classes("w-full").style("flex: 1 1 auto; min-height: 0; padding: 0;"):
                        left_refs["container"] = ui.element("div").classes("w-full")

                # RIGHT — roster; preview on click, travel via the row arrow.
                with ui.element("div").style(
                    "flex: 1 1 auto; min-width: 0; display: flex; flex-direction: column; min-height: 0;"
                ):
                    overview = ProjectsOverview(
                        panel.backend,
                        on_open=_switch_project,
                        on_select=_on_select,
                        on_delete=None,
                        base_path_provider=lambda: current_base["path"],
                        auto_refresh_sec=15.0,
                        current_path=current_path_str,
                        selected_path=current_path_str,
                        show_filter=True,
                        height_css="calc(88vh - 116px)",
                        title="Projects Overview",
                    )
                    overview_ref["comp"] = overview
                    overview.build()

        # Paint the left pane for the currently-loaded project up front.
        await _load_left(selected_ref["path"])

        # Cancel auto-refresh on any dismissal path. `hide` covers backdrop
        # click / escape / programmatic close; `before-hide` is a Quasar
        # safety net. _switch_project also calls stop() before navigate.to
        # so closures don't outlive the dialog.
        dialog.on("hide", lambda: overview.stop())
        dialog.on("before-hide", lambda: overview.stop())
        dialog.open()

    # ── Run slot ──────────────────────────────────────────────────────────────

    def rebuild_run_slot(self):
        run_slot = self._refs.get("run_slot")
        if run_slot is None:
            return
        run_slot.clear()
        panel = self.panel
        with run_slot:
            if panel.ui_mgr.is_running:
                stop_div = (
                    ui.element("div")
                    .style(
                        "width: 30px; height: 30px; border-radius: 50%; cursor: pointer; "
                        "background: #fef2f2; border: 1px solid #fecaca; color: #b91c1c; "
                        "display: flex; align-items: center; justify-content: center; flex-shrink: 0;"
                    )
                    .on("click", panel.handle_stop_pipeline)
                    .tooltip("Stop pipeline")
                )
                with stop_div:
                    ui.html(self._load_svg("stop.svg"), sanitize=False).style(
                        "width: 16px; height: 16px; display: flex; pointer-events: none;"
                    )

                # Pulsating dot (self-contained inline SVG/SMIL; see _running_spinner_html).
                ui.html(
                    '<div style="width:100%;margin-top:4px;display:flex;justify-content:center;">'
                    + _running_spinner_html(20, "#3b82f6")
                    + "</div>",
                    sanitize=False,
                )

                status_lbl = ui.label("").style(
                    f"font-size: 8px; color: {SB_MUTE}; "
                    "font-family: 'IBM Plex Mono', monospace; "
                    "text-align: center; line-height: 1.4; word-break: break-all; "
                    "white-space: pre-line; display: block; width: 100%; padding: 0 3px;"
                )
                self._refs["status_label"] = status_lbl
            else:
                play_div = (
                    ui.element("div")
                    .style(
                        "width: 30px; height: 30px; border-radius: 50%; cursor: pointer; "
                        "background: #f0fdf4; border: 1px solid #bbf7d0; color: #15803d; "
                        "display: flex; align-items: center; justify-content: center; flex-shrink: 0;"
                    )
                    .on("click", panel.handle_run_pipeline)
                    .tooltip("Run pipeline")
                )
                with play_div:
                    ui.html(self._load_svg("play.svg"), sanitize=False).style(
                        "width: 16px; height: 16px; display: flex; pointer-events: none;"
                    )

    # ── Status label ──────────────────────────────────────────────────────────

    # Row + sidebar "working" indicators are self-contained inline SVG/SMIL
    # (_running_spinner_html); there is no server-driven advance() loop. The
    # previous 0.17 s ui.timer + ui.run_javascript broadcast lived here.

    def update_status_label(self, overview: dict):
        el = self._refs.get("status_label")
        if el is None:
            return
        done = overview.get("completed", 0) + overview.get("failed", 0)
        # Backend total already excludes IMPORT_MOVIES and TS_IMPORT (both hidden
        # from PHASE_JOBS / roster). Mirror that filter on the fallback so the
        # first paint -- before the first overview poll returns -- doesn't inflate.
        _hidden = {JobType.IMPORT_MOVIES.value, JobType.TS_IMPORT.value}
        visible_selected = sum(1 for iid in self.panel.ui_mgr.selected_jobs if InstanceId.split(iid)[0] not in _hidden)
        total = overview.get("total", visible_selected) if overview else visible_selected
        text = f"{done}/{total}"
        # Surface SLURM queue waits so a long pending time reads as a cluster
        # wait, not a hung orchestrator. This runs on every poll tick (it is NOT
        # signature-gated), so the elapsed minutes stay live.
        queued_jobs = (overview or {}).get("queued_jobs") or []
        if queued_jobs:
            max_pending = max((q.get("pending_secs", 0) for q in queued_jobs), default=0)
            text += f"\n{len(queued_jobs)} in SLURM queue"
            if max_pending >= 60:
                text += f"\n{max_pending // 60}m for nodes"
        el.set_text(text)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _sb_sep(self):
        ui.element("div").style(f"height: 1px; background: {SB_SEP}; width: 24px; margin: 3px auto;")

    def _load_svg(self, name: str) -> str:
        if name.startswith("<svg"):
            return name
        return load_icon_svg(name)

    def _build_new_species_btn(self):
        """PARTICLES-header utility: create a label-only species de novo.

        The de-novo path: no template, no template-matching job, possibly zero
        parameters — the species exists so the user can start hand-picking in
        ArtiaX immediately. The Species page's "+" remains the template-driven
        entry point (`origin="workbench"`); this one is for species that never have one.
        """
        from ui.species.prompt import create_species

        project_path = self.panel.ui_mgr.project_path

        async def _create():
            # SingleFlight: this button lives in a poll-refreshed container, so it can
            # be destroyed and rebuilt mid-click — without the guard each stray click
            # queues another dialog.
            async with self.panel.flight("new_species") as acquired:
                if not acquired:
                    return
                # origin="manual": a de-novo species may never have a template.
                species = await create_species(self.panel.backend, project_path, origin="manual")
                if species is None:
                    return
                ui.notify(f"Created species '{species.name}'", type="positive")
                self.panel.rebuild_pipeline_ui()

        container = (
            ui.element("div")
            .style(
                "width: 22px; height: 22px; border-radius: 4px; "
                "display: flex; align-items: center; justify-content: center; "
                "cursor: pointer; flex-shrink: 0;"
            )
            .on("click", _create)
            .tooltip("New species (pick by hand — no template needed)")
        )
        with container:
            ui.icon("add_circle_outline", size="15px").style("color: #6366f1; pointer-events: none;")
        return container

    def _build_import_tomograms_btn(self):
        """PARTICLES-header utility: import tomograms into a data-less project. A
        project-level artifact, NOT a job (no SLURM/IO). Disabled when upstream tomograms
        already exist (a regular project at the particle stage); a green dot marks an
        import already committed. See ui/tomogram_import_dialog.py."""
        from services.models_base import JobType
        from services.project_state import get_project_state_for
        from ui.tomogram_import_dialog import open_tomogram_import_dialog

        project_path = self.panel.ui_mgr.project_path
        has_upstream = False
        already = False
        try:
            state = get_project_state_for(project_path)
            has_upstream = any(getattr(jm, "job_type", None) == JobType.TS_RECONSTRUCT for jm in state.jobs.values())
            already = state.imported_tomograms is not None and bool(state.imported_tomograms.star_path)
        except Exception as e:
            logger.debug("import-tomograms header button: could not read project state: %s", e)

        disabled = has_upstream
        tip = (
            "Tomograms already come from the pipeline above"
            if has_upstream
            else ("Import more tomograms" if already else "Import tomograms")
        )

        def _committed():
            # Three surfaces answer "did that land": this header's green dot (roster
            # render), the Tomograms rail badge, and the wall itself. The first two are
            # repainted here so the confirmation is on screen the moment the dialog
            # closes; the wall is only invalidated, since it re-collects when visited.
            self.panel.rebuild_pipeline_ui()
            self.refresh_counts()
            invalidate = self.panel.callbacks.get("invalidate_gallery")
            if invalidate:
                invalidate()

        def _open():
            if disabled:
                return
            open_tomogram_import_dialog(self.panel.backend, project_path, on_done=_committed)

        container = (
            ui.element("div")
            .style(
                "width: 22px; height: 22px; border-radius: 4px; "
                "display: flex; align-items: center; justify-content: center; "
                f"cursor: {'not-allowed' if disabled else 'pointer'}; flex-shrink: 0; position: relative; "
                f"opacity: {'0.4' if disabled else '1'};"
            )
            .on("click", lambda: _open())
            .tooltip(tip)
        )
        with container:
            ui.icon("library_add", size="15px").style(
                f"color: {'#9ca3af' if disabled else '#6366f1'}; pointer-events: none;"
            )
            if already and not disabled:
                ui.element("div").style(
                    "position: absolute; top: 1px; right: 1px; width: 6px; height: 6px; "
                    "border-radius: 50%; background: #16a34a; pointer-events: none;"
                )
        return container

    def _build_aggregation_merge_btn(self):
        """PARTICLES-header utility: open the Aggregate dialog on the coordinate grade (its
        header switch reaches the extracted grade). Shows a small green dot when a merged
        optimisation_set already exists, so you can see at a glance whether
        the merge has been done.

        Was a sidebar button gated on `state.is_aggregation` (de-novo S6 deleted that flag):
        it belongs beside the other two PARTICLES-header utilities — new species, import
        tomograms — because all three answer "where do this project's particles come from",
        and it is now available in every project rather than only in ones whose creator
        happened to tick a box."""
        from services.project_state import get_project_state_for
        from ui.aggregation.aggregate_dialog import open_aggregate_dialog
        from ui.aggregation.merge_card import has_merged_outputs

        project_path = self.panel.ui_mgr.project_path
        merged = False
        try:
            merged = has_merged_outputs(get_project_state_for(project_path))
        except Exception as e:
            logger.debug("merge-sources header button: could not read project state: %s", e)
        container = (
            ui.element("div")
            .style(
                "width: 22px; height: 22px; border-radius: 4px; "
                "display: flex; align-items: center; justify-content: center; "
                "cursor: pointer; flex-shrink: 0; position: relative;"
            )
            .on("click", lambda: open_aggregate_dialog(project_path))
            .tooltip(
                "Aggregate — unite one species' picks across lists, tomograms and projects; "
                "coordinates or extracted particles" + (" (aggregated)" if merged else "")
            )
        )
        with container:
            ui.icon("merge_type", size="14px").style(
                "color: #9333ea; pointer-events: none;"  # purple-600
            )
            if merged:
                ui.element("div").style(
                    "position: absolute; top: 4px; right: 4px; width: 6px; height: 6px; "
                    "border-radius: 50%; background: #16a34a; pointer-events: none;"  # green-600
                )
        self._refs["aggregation_merge_btn"] = container
        return container

    def _build_dashboard_btn(self):
        """Sidebar button that opens the per-TS Journey dashboard.

        Always visible — the dashboard itself shows an empty-state notification
        when the project has no array-job data yet, so this is a stable
        anchor in the sidebar instead of a button that pops in and out as
        jobs run.

        It used to carry a green dot whenever ANY preview had ever been rendered. That is
        true for the whole life of a project after the first reconstruction, so it read as a
        permanent "something is new" badge pointing at nothing in particular — removed.
        """
        return self._sb_svg_btn(
            _TOMO_DASHBOARD_SVG,
            "Journey — one tilt-series end to end: motion, CTF, alignment, reconstruction, picks",
            self._open_journey,
            ref_key="dashboard_btn",
        )

    # A ChimeraX + ArtiaX launcher used to sit here. It went with picking-UI roadmap 09-S2:
    # a session is always started ON a tomogram, from the Particles registry's
    # "Picks & curation" tab ('curate' on a tomogram group), so the app has exactly one
    # launch affordance. What sits at the bottom of the rail now is an INDICATOR of that
    # session (_build_curation_session_btn) whose click opens the control center — it never
    # launches, so there is still exactly one launch affordance.

    def _build_curation_session_btn(self):
        """Bottom-of-rail ChimeraX + ArtiaX control-session indicator.

        Ever-present like the other rail entries, and dim while nothing is running. When a
        session of this user IS up it turns green and breathes (CSS keyframes — see
        `.cb-artiax-live`), and its hover says WHICH species and tomogram that session was
        launched on, read from the session's own recorded scope. Clicking always opens the
        control center: connect details while one is up, and the start panel otherwise.

        `unknown` (a `squeue` that raised) is its own amber state — never painted as "no
        session", which is the reading that gets a user to start a second ChimeraX.
        """
        container = (
            ui.element("div")
            .style(
                "width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                "background: transparent; "
                "display: flex; align-items: center; justify-content: center; "
                "cursor: pointer; flex-shrink: 0;"
            )
            .on("click", self._open_control_center)
        )
        with container:
            icon = ui.icon("view_in_ar", size="18px").style(f"color: {SB_MUTE}; pointer-events: none;")
            tip = ui.tooltip("")
        self._refs["curation_btn"] = container
        self._refs["curation_icon"] = icon
        self._refs["curation_tip"] = tip
        self._paint_curation_session()
        # The status itself is the shared, throttled session_status cache (one `squeue` per
        # POLL_S across every observer), so this tick is nearly free and only repaints when
        # the rendered state actually moved.
        ui.timer(_CURATION_TICK_S, self._tick_curation_session)
        return container

    async def _tick_curation_session(self):
        await session_status.poll(self.panel.backend)
        self._paint_curation_session()

    def _paint_curation_session(self):
        """Reflect the cached session state onto the rail icon. Gated on the (status, scope)
        it last painted: this runs on a timer, and re-sending identical style/class strings
        every tick is churn the client has to process for nothing.

        Four states, not three. `unknown` splits by WHY: a `squeue` that raised is amber and
        says so, while "not asked yet" (the first seconds after a page load) is just dim —
        painting that one amber would cry wolf on every single load.
        """
        st = session_status.status()
        scope = session_status.scope_text()
        error = session_status.last_error()
        if (st, scope, error) == self._curation_paint:
            return
        self._curation_paint = (st, scope, error)

        icon = self._refs.get("curation_icon")
        tip = self._refs.get("curation_tip")
        if icon is None or tip is None:
            return
        if st == session_status.LIVE:
            color, live = "#16a34a", True
            text = (
                f"Picking {scope or 'a scope this session did not record'} — ChimeraX + ArtiaX is up. "
                "Click for the control center."
            )
        elif st == session_status.UNKNOWN and error:
            color, live = "#d97706", False
            text = f"Could not ask SLURM whether a curation session is running — {error}. Click for the control center."
        elif st == session_status.UNKNOWN:
            color, live = SB_MUTE, False
            text = "Checking for a running curation session… Click for the control center."
        else:
            color, live = SB_MUTE, False
            text = (
                "No curation session. Start one with 'curate' on a tomogram in Picks & curation, "
                "or click here for the control center."
            )
        icon.style(f"color: {color}; pointer-events: none;")
        icon.classes(add="cb-artiax-live" if live else "", remove="" if live else "cb-artiax-live")
        tip.set_text(text)

    async def _open_control_center(self):
        """The rail indicator's click. Opens the SAME control center the Picks & curation
        session chip does — SingleFlight-guarded, since it owns a dialog."""
        async with self.panel.flight("rail_control_center") as acquired:
            if not acquired:
                return
            await open_curation_control_center(self.panel.backend, self.panel.ui_mgr.project_path)

    def _sb_svg_btn(self, svg_name, tooltip, on_click, active=False, ref_key=None, color_override=None, badge=False):
        bg = SB_ABG if active else "transparent"
        color = color_override or (SB_ACT if active else SB_MUTE)

        svg = self._load_svg(svg_name).replace("currentColor", color)

        container = (
            ui.element("div")
            .style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"background: {bg}; position: relative; "
                f"display: flex; align-items: center; justify-content: center; "
                f"cursor: pointer; flex-shrink: 0;"
            )
            .on("click", on_click)
        )
        with container:
            icon = ui.html(svg, sanitize=False).style("width: 18px; height: 18px; display: flex; pointer-events: none;")
            # Built as an explicit child rather than `.tooltip(...)` on the chain so the
            # count line can be re-set in place (same reason as the curation indicator).
            # pre-line: these carry a count line under the description, and QTooltip's
            # default `white-space: normal` collapses the newline into a run-on sentence.
            tip = ui.tooltip(tooltip).style("white-space: pre-line;")
            badge_el = ui.label("").style(_SB_BADGE_STYLE) if badge else None
        if ref_key:
            self._refs[ref_key] = container
            # The icon separately, so a re-colour can set its markup in place instead of
            # clearing the container — which would take the tooltip with it.
            self._refs[f"{ref_key}_icon"] = icon
            self._refs[f"{ref_key}_tip"] = tip
            if badge_el is not None:
                self._refs[f"{ref_key}_badge"] = badge_el
        return container

    def _info_popup_btn(self, icon_name: str, title: str, rows: list, icon_color: str | None = None):
        color = icon_color or SB_MUTE
        btn = (
            ui.button(icon=icon_name)
            .props("flat dense")
            .style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"color: {color}; background: transparent; min-width: 0;"
            )
        )
        with btn:
            with (
                ui.menu()
                .props('anchor="center right" self="center left" :offset="[8,0]"')
                .style(
                    "background: #ffffff; border: 1px solid #e2e8f0; "
                    "border-radius: 5px; overflow: hidden; min-width: 210px; "
                    "padding: 0; box-shadow: 0 4px 12px rgba(0,0,0,0.08);"
                )
            ):
                with ui.element("div").style(
                    "padding: 7px 11px 5px; font-size: 9px; font-weight: 700; "
                    "color: #94a3b8; letter-spacing: 0.09em; text-transform: uppercase; "
                    "border-bottom: 1px solid #f1f5f9;"
                ):
                    ui.label(title)
                for row_lbl, row_val in rows:
                    with ui.element("div").style(
                        "display: flex; justify-content: space-between; align-items: baseline; "
                        "padding: 5px 11px; border-bottom: 1px solid #f8fafc; gap: 10px;"
                    ):
                        ui.label(row_lbl).style("font-size: 10px; color: #94a3b8; flex-shrink: 0;")
                        ui.label(str(row_val)).style(
                            "font-size: 10px; font-family: 'IBM Plex Mono', monospace; "
                            "color: #1e40af; text-align: right; word-break: break-all;"
                        )
                ui.element("div").style("height: 4px;")
        return btn

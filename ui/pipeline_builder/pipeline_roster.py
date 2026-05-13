import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
from nicegui import ui
from services.models_base import JobStatus
from services.project_state import JobType, get_project_state

from ui.styles import MONO, SANS as FONT
from ui.status_indicator import BoundStatusDot
from ui.ui_state import get_job_display_name, get_instance_display_name, instance_id_to_job_type
from ui.pipeline_builder.pipeline_constants import (
    PHASE_JOBS,
    PHASE_META,
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


def _ts_cell(text: str, color: str, extra: str = ""):
    """Tiny monospace cell for the tilt-series table in the metadata popup."""
    ui.label(text).style(f"font-size: 9px; font-family: 'IBM Plex Mono', monospace; color: {color}; {extra}")


def _profile_row_fields(gres: str, mem: str, cpus: str, time_val: str):
    """Compact inline summary of SLURM resource fields for the profiles popup."""
    with ui.row().classes("items-center gap-2"):
        for lbl, val in [("gres", gres), ("mem", mem), ("cpu", cpus), ("time", time_val)]:
            ui.label(f"{lbl}: {val}").style("font-size: 9px; font-family: 'IBM Plex Mono', monospace; color: #64748b;")


_TOMO_DASHBOARD_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
    '<circle cx="12" cy="12" r="8"/>'
    '<line x1="4.5" y1="9" x2="19.5" y2="9"/>'
    '<line x1="4" y1="12" x2="20" y2="12"/>'
    '<line x1="4.5" y1="15" x2="19.5" y2="15"/>'
    "</svg>"
)
_SB_INFO = "#c0cad4"
_AVATAR_PALETTE = ["#3b82f6", "#8b5cf6", "#06b6d4", "#10b981", "#f59e0b", "#ec4899"]


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


def _resolve_array_job_dir(job_model, project_path: Optional[Path] = None) -> Optional[Path]:
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


def _get_array_progress(job_model, project_path: Optional[Path] = None) -> Optional[Tuple[int, int, int]]:
    """Return (n_done, n_failed, n_total) for array jobs, or None if not applicable."""
    job_dir = _resolve_array_job_dir(job_model, project_path)
    if job_dir is None:
        return None

    manifest_path = job_dir / ".task_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception:
        return None
    items = manifest.get("items", [])
    if not items:
        return None

    status_dir = job_dir / ".task_status"
    n_ok = 0
    n_fail = 0
    if status_dir.is_dir():
        for p in status_dir.iterdir():
            if p.suffix == ".ok":
                n_ok += 1
            elif p.suffix == ".fail":
                n_fail += 1
    return (n_ok + n_fail, n_fail, len(items))


def _get_array_ts_statuses(
    job_model, project_path: Optional[Path] = None
) -> Optional[Tuple[List[str], Dict[str, str], Dict[str, str]]]:
    """Return (items, statuses, display_names) for per-TS sub-rows, or None."""
    from ui.components.task_utils import shorten_ts_names, scan_statuses

    job_dir = _resolve_array_job_dir(job_model, project_path)
    if job_dir is None:
        return None

    manifest_path = job_dir / ".task_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception:
        return None
    items = manifest.get("items", [])
    if not items:
        return None

    statuses = scan_statuses(job_dir, items)
    display_names = shorten_ts_names(items)
    return items, statuses, display_names


class RosterWidget:
    def __init__(self, panel: "PipelineBuilderPanel"):
        self.panel = panel
        self._flash_phase: Optional[str] = None
        self._roster_visible: bool = True
        self._roster_phase: Optional[str] = None
        self._refs: Dict = {}
        self._spinner_frames = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        self._spinner_idx: int = 0
        self._live_spinners: List = []
        # Per-instance expansion state for per-TS sub-rows, persisted across
        # roster refreshes (status_poller refreshes the roster every few seconds
        # and would otherwise collapse rows the user had opened).
        self._expanded_instances: Dict[str, bool] = {}

    def _status_widget(self, instance_id: str):
        from services.project_state import get_project_state
        from ui.status_indicator import _dot_html

        job_model = get_project_state().jobs.get(instance_id)
        if not job_model:
            BoundStatusDot(instance_id)
            return

        def _content(status, jm=job_model):
            if status == JobStatus.RUNNING:
                frame = self._spinner_frames[self._spinner_idx]
                return (
                    f'<span class="cb-row-spinner" '
                    f"style=\"font-family:'IBM Plex Mono',monospace;font-size:13px;"
                    f'color:#3b82f6;line-height:1;flex-shrink:0;">{frame}</span>'
                )
            return _dot_html(status, is_orphaned=jm.is_orphaned)

        ui.html("", sanitize=False, tag="span").bind_content_from(job_model, "execution_status", backward=_content)

    # ── Roster ────────────────────────────────────────────────────────────────

    def refresh(self):
        panel = self.panel
        if panel.roster_panel is None:
            return
        panel.roster_panel.clear()

        with panel.roster_panel:
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
        job_model = panel.state_service.state.jobs.get(instance_id)

        base_name = get_job_display_name(job_type)
        relion_job_name = getattr(job_model, "relion_job_name", None) if job_model else None
        if relion_job_name:
            job_folder = relion_job_name.rstrip("/").split("/")[-1]
            display_text = f"{base_name} ({job_folder})"
        else:
            parts = instance_id.split("__", 1)
            if len(parts) > 1:
                suffix = parts[1]
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
                with ui.element("div").style(
                    f"display: inline-flex; align-items: center; flex-shrink: 0; "
                    f"background: {species.color}18; border: 1px solid {species.color}55; "
                    f"border-radius: 999px; padding: 1px 6px;"
                ):
                    ui.label(species.name).style(
                        f"font-size: 8px; color: {species.color}; font-weight: 600; white-space: nowrap;"
                    )
            # Inline array progress (e.g., "17/18" green, or "17/18 1!" red)
            progress = _get_array_progress(job_model, panel.ui_mgr.project_path)
            if progress is not None:
                n_done, n_fail, n_total = progress
                n_ok = n_done - n_fail
                if n_fail > 0:
                    # Show "ok/total fail!" — e.g. "17/18 1!"
                    ui.label(f"{n_ok}/{n_total}").style(
                        f"{MONO} font-size: 9px; font-weight: 600; color: #16a34a; flex-shrink: 0;"
                    )
                    ui.label(f"{n_fail}!").style(
                        f"{MONO} font-size: 9px; font-weight: 700; color: #dc2626; flex-shrink: 0;"
                    )
                elif n_done == n_total:
                    ui.label(f"{n_ok}/{n_total}").style(
                        f"{MONO} font-size: 9px; font-weight: 600; color: #16a34a; flex-shrink: 0;"
                    )
                elif n_done > 0:
                    ui.label(f"{n_ok}/{n_total}").style(
                        f"{MONO} font-size: 9px; font-weight: 600; color: #2563eb; flex-shrink: 0;"
                    )
                else:
                    ui.label(f"0/{n_total}").style(
                        f"{MONO} font-size: 9px; font-weight: 600; color: #9ca3af; flex-shrink: 0;"
                    )
            # Subsection icons + actions (right-aligned, flex-shrink: 0)
            with ui.element("div").style("display: flex; align-items: center; gap: 0; flex-shrink: 0;"):
                for icon_name, tab_key, tip in [
                    ("tune", "config", "Config"),
                    ("article", "logs", "Logs"),
                    ("folder_open", "files", "Files"),
                ]:
                    (
                        ui.button(
                            icon=icon_name,
                            on_click=lambda iid=instance_id, tk=tab_key: panel.switch_to_job_subsection(iid, tk),
                        )
                        .props("flat dense round size=xs color=grey-7")
                        .style("flex-shrink: 0;")
                        .tooltip(tip)
                    )
                # Extra tab icons. For array jobs the "tasks" tab is repurposed
                # to toggle the inline per-TS sub-row view instead of navigating
                # to a separate tab.
                from ui.job_plugins import get_extra_tabs

                for et in get_extra_tabs(job_type):
                    if et.key == "tasks":
                        (
                            ui.button(
                                icon=et.icon,
                                on_click=lambda iid=instance_id: self._toggle_ts_expansion(iid),
                            )
                            .props("flat dense round size=xs color=grey-7")
                            .style("flex-shrink: 0;")
                            .tooltip("Toggle tilt-series list")
                        )
                    else:
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
        is_array = "array_throttle" in getattr(job_model, "USER_PARAMS", set()) if job_model else False
        if is_array:
            ts_data = _get_array_ts_statuses(job_model, panel.ui_mgr.project_path)
            if ts_data is not None:
                items, statuses, display_names = ts_data
                is_running = job_model.execution_status == JobStatus.RUNNING
                # Default to expanded while running; persist any user toggle across refreshes.
                expanded = self._expanded_instances.setdefault(instance_id, is_running)
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
        items: List[str],
        statuses: Dict[str, str],
        display_names: Dict[str, str],
        indent: int,
        expanded: bool = False,
        job_dir: Optional[Path] = None,
    ):
        """Render collapsible per-tilt-series status rows under an array job.

        Layout order (top → bottom):
          1. Slim arrow indicator row, always directly under the parent job row.
          2. Expanded list of per-TS rows (only when expanded).

        The arrow stays anchored under the parent row; it never travels to the
        bottom of the expanded list. Rows are ordered by (stage, beam) ascending.
        Clicking a row navigates the main pane to this job's Tasks tab and scrolls
        to the matching entry there — no pop-up dialog, nothing to get auto-closed
        by a background refresh.
        """
        from ui.components.task_utils import sort_ts_by_position

        _TS_COLORS = {"ok": "#16a34a", "fail": "#dc2626", "running": "#2563eb", "pending": "#d1d5db"}
        _TS_ICONS = {"ok": "check_circle", "fail": "error", "running": "sync", "pending": "radio_button_unchecked"}

        # 1. Arrow indicator row — always rendered, directly under the main job row.
        with (
            ui.element("div")
            .style(
                f"display: flex; align-items: center; gap: 0; "
                f"padding: 0 4px 0 {indent}px; cursor: pointer; "
                f"height: 10px; background: transparent;"
            )
            .on("click", lambda _e, iid=instance_id: self._toggle_ts_expansion(iid))
        ):
            ui.icon("expand_more" if expanded else "chevron_right", size="11px").style(
                "color: #cbd5e1; flex-shrink: 0;"
            )

        # 2. Expanded list — rendered AFTER the arrow so it sits below it.
        container = ui.element("div").style(
            f"display: {'block' if expanded else 'none'}; border-left: 2px solid #e2e8f0; margin-left: {indent - 4}px;"
        )

        display_order = sort_ts_by_position(items)
        panel = self.panel
        with container:
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

        with ui.dialog() as dialog, ui.card().classes("w-[28rem]"):
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
                ui.button("Cancel", on_click=dialog.close).props("flat")

                async def confirm():
                    dialog.close()
                    try:
                        result = await panel.backend.delete_job(
                            instance_id_to_job_type(instance_id).value, instance_id=instance_id
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

                delete_btn = ui.button("Delete", color="red", on_click=confirm)
                if preview and preview.get("downstream_count", 0) > 0:
                    delete_btn.props('icon="delete_forever"')

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

        with ui.dialog() as dialog, ui.card().classes("w-[28rem]"):
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
                ui.button("Cancel", on_click=dialog.close).props("flat")

                def confirm():
                    dialog.close()
                    # Remove from pipeline but keep labels in project state.
                    del state.jobs[instance_id]
                    state.job_path_mapping.pop(instance_id, None)
                    panel.remove_instance_from_pipeline(instance_id)
                    ui.notify("Tilt filter removed. Labels preserved.", type="info")

                ui.button("Remove", color="red", on_click=confirm)

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
        container = self._refs.get("pipeline_btn")
        if container is None:
            return
        bg = SB_ABG if self._roster_visible else "transparent"
        color = SB_ACT if self._roster_visible else SB_MUTE
        container.style(
            f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
            f"background: {bg}; "
            f"display: flex; align-items: center; justify-content: center; "
            f"cursor: pointer; flex-shrink: 0;"
        )
        svg = self._load_svg("layers.svg").replace("currentColor", color)
        container.clear()
        with container:
            ui.html(svg, sanitize=False).style("width: 18px; height: 18px; display: flex; pointer-events: none;")

    # ── Sidebar ───────────────────────────────────────────────────────────────

    def build_sidebar(self):
        panel = self.panel
        if panel.primary_sidebar is None:
            return

        state = get_project_state()

        with panel.primary_sidebar:
            ui.element("div").style("height: 8px;")

            self._build_project_avatar(state)
            ui.element("div").style("height: 2px;")
            self._build_metadata_btn(state)

            ui.element("div").style("height: 4px;")
            self._sb_sep()
            ui.element("div").style("height: 4px;")

            self._sb_svg_btn("layers.svg", "Pipeline", lambda: self.toggle(), ref_key="pipeline_btn", active=True)

            if panel.toggle_workbench is not None:
                ui.element("div").style("height: 1px;")
                wb_btn = self._sb_svg_btn("vial.svg", "Template Workbench", panel.toggle_workbench, ref_key="wb_btn")
                panel.callbacks["wb_btn"] = wb_btn

            # Tomogram Dashboard — unified per-TS inspection surface that replaces
            # the old "Tomogram Previews" grid, "Tilt Series Journey" matrix, and
            # standalone "Candidate Previews" dialog. See services/visualization/
            # ROADMAP.md for the consolidation plan.
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

            # Aggregation projects: open the merge-sources dialog from the
            # sidebar instead of taking up half the workspace inline.
            if getattr(state, "is_aggregation", False):
                self._build_aggregation_merge_btn()

            self._sb_svg_btn("cross.svg", "Close project", lambda: ui.navigate.to("/"))

            ui.element("div").style("flex: 1;")
            ui.element("div").style("height: 6px;")

        self.rebuild_run_slot()

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

    def _build_metadata_btn(self, state):
        """
        Compact stats widget showing key dataset numbers inline.
        Click opens the full project overview popup.
        """
        px = state.microscope.pixel_size_angstrom
        n_ts = state.import_selected_tilt_series or 0
        n_pos = state.import_selected_positions or 0

        outer = (
            ui.element("div")
            .style(
                "width: 34px; border-radius: 5px; margin: 1px 0; padding: 4px 0; "
                "background: #f1f5f9; display: flex; flex-direction: column; "
                "align-items: center; gap: 3px; cursor: pointer; flex-shrink: 0; "
                "position: relative; border: 1px solid #e2e8f0;"
            )
            .tooltip("Click for full project parameters")
        )

        _stat_val = f"{MONO} font-size: 9px; font-weight: 700; color: #1e40af; line-height: 1; pointer-events: none;"
        _stat_lbl = "font-size: 7px; font-weight: 500; color: #94a3b8; line-height: 1; pointer-events: none;"

        with outer:
            # Pixel size
            ui.label(f"{px:.2f}" if px else "---").style(_stat_val)
            ui.label("\u212b").style(_stat_lbl)
            # Separator
            ui.element("div").style("width: 14px; height: 1px; background: #e2e8f0;")
            # Tilt series count
            ui.label(str(n_ts) if n_ts else "---").style(_stat_val)
            ui.label("ts").style(_stat_lbl)
            # Positions
            if n_pos:
                ui.element("div").style("width: 14px; height: 1px; background: #e2e8f0;")
                ui.label(str(n_pos)).style(_stat_val)
                ui.label("pos").style(_stat_lbl)

            # Zero-size invisible button that owns the menu anchor --
            # positioned absolutely so it doesn't affect layout
            anchor_btn = (
                ui.button()
                .props("flat dense")
                .style(
                    "position: absolute; width: 0; height: 0; min-width: 0; "
                    "padding: 0; opacity: 0; pointer-events: none;"
                )
            )
            with anchor_btn:
                with (
                    ui.menu()
                    .props('anchor="center right" self="center left" :offset="[8,0]"')
                    .style(
                        # Wide panel, single outer scroll. Inner sections intentionally
                        # have NO overflow — the overview is a read-only dump and having
                        # multiple nested scrollbars just wastes dexterity.
                        "background: #ffffff; border: 1px solid #e2e8f0; "
                        "border-radius: 5px; overflow-y: auto; "
                        "min-width: 840px; max-width: 92vw; max-height: 85vh; "
                        "padding: 0; box-shadow: 0 4px 12px rgba(0,0,0,0.08);"
                    )
                ) as menu:
                    self._render_overview_section(
                        "Project",
                        [
                            ("Name", state.project_name),
                            ("Root", str(state.project_path) if state.project_path else "---"),
                            ("Movies", state.movies_glob or "---"),
                            ("MDOC", state.mdocs_glob or "---"),
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

                    if state.import_total_positions or state.import_total_tilt_series:
                        ds_rows = []
                        if state.import_source_directory:
                            ds_rows.append(("Source", state.import_source_directory))
                        if state.import_frame_extension:
                            ds_rows.append(("Format", state.import_frame_extension))
                        self._render_overview_section("Dataset", ds_rows, bottom_border=False)
                        self._render_dataset_ts_expansion(state)

                    self._render_slurm_defaults_section()

                    ui.element("div").style("height: 6px;")

            # clicking the outer div opens the menu via JS
            outer.on("click", lambda: menu.open())

        return outer

    # ── Overview helpers (denser layout, no nested scroll) ────────────────────

    def _overview_section_header(self, title: str) -> None:
        ui.element("div").style("height: 6px;")  # small gap between categories
        with ui.element("div").style(
            "padding: 4px 11px 3px; font-size: 9px; font-weight: 700; "
            "color: #94a3b8; letter-spacing: 0.09em; text-transform: uppercase; "
            "background: #f8fafc; border-top: 1px solid #e2e8f0; border-bottom: 1px solid #e2e8f0;"
        ):
            ui.label(title)

    def _render_overview_section(self, title: str, rows: list, bottom_border: bool = True) -> None:
        self._overview_section_header(title)
        for row_lbl, row_val in rows:
            divider = "1px solid #f8fafc" if bottom_border else "none"
            with ui.element("div").style(
                f"display: flex; justify-content: space-between; align-items: baseline; "
                f"padding: 3px 11px; border-bottom: {divider}; gap: 10px;"
            ):
                ui.label(row_lbl).style("font-size: 10px; color: #94a3b8; flex-shrink: 0;")
                ui.label(str(row_val)).style(
                    f"{MONO} font-size: 10px; color: #1e40af; text-align: right; word-break: break-all;"
                )

    def _render_dataset_ts_expansion(self, state) -> None:
        """Collapsible per-tilt-series table living on the Dataset row.

        Replaces the old fixed-height, nested-scrollbar TS table.
        """
        ts_details = state.import_tilt_series_details
        if not ts_details:
            return
        selected_ts = [td for td in ts_details if td.selected]
        excluded_ts = [td for td in ts_details if not td.selected]
        sel = state.import_selected_tilt_series
        tot = state.import_total_tilt_series
        header_text = f"{sel} of {tot} tilt-series"

        exp = ui.expansion().props("dense header-class=q-px-none").style(
            "width: 100%; border-bottom: 1px solid #f8fafc; background: transparent;"
        )

        with exp.add_slot("header"):
            with ui.row().classes("w-full items-center").style("gap: 10px; padding: 0 11px;"):
                ui.label("Selected").style("font-size: 10px; color: #94a3b8; flex-shrink: 0;")
                ui.space()
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
                    _ts_cell(str(td.tilt_count), "#64748b")
                    _ts_cell(
                        td.mdoc_filename,
                        "#94a3b8",
                        extra="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;",
                    )
            if excluded_ts:
                with ui.element("div").style("padding: 4px 11px; border-top: 1px solid #e2e8f0;"):
                    ui.label(f"+{len(excluded_ts)} excluded").style(
                        "font-size: 9px; font-style: italic; color: #cbd5e1;"
                    )

    def _render_slurm_defaults_section(self) -> None:
        """SLURM defaults + per-job profiles inline in the project overview.

        Folds in what used to be a separate gear-icon popup so there's one
        canonical place in the UI for project-wide config.
        """
        try:
            from services.configs.config_service import get_config_service
            from services.models_base import JobType

            cs = get_config_service()
            profiles = cs.config.job_resource_profiles
            sup = cs.supervisor_slurm_defaults
            defaults = cs.slurm_defaults
        except Exception:
            return

        self._overview_section_header("SLURM defaults")

        # Global defaults row
        with ui.element("div").style("padding: 3px 11px;"):
            ui.label("Global").style(
                "font-size: 9px; font-weight: 600; color: #94a3b8; margin-bottom: 2px;"
            )
            _profile_row_fields(defaults.gres, defaults.mem, str(defaults.cpus_per_task), defaults.time)

        if profiles:
            # Column header
            with ui.element("div").style(
                "display: grid; grid-template-columns: 1fr 72px 56px 44px 64px; gap: 4px; "
                "padding: 3px 11px 2px; border-top: 1px solid #f1f5f9; background: #f8fafc;"
            ):
                for hdr in ("JOB TYPE", "GRES", "MEM", "CPU", "TIME"):
                    ui.label(hdr).style(
                        "font-size: 8px; font-weight: 600; color: #94a3b8; letter-spacing: 0.04em;"
                    )

            job_type_labels = {jt.value: jt.name.replace("_", " ").title() for jt in JobType}
            for key, profile in profiles.items():
                p = profile.model_dump(exclude_none=True)
                gres = p.get("gres", "")
                mem = p.get("mem", "")
                cpus = str(p.get("cpus_per_task", ""))
                time_val = p.get("time", "")
                display_name = job_type_labels.get(key, key)
                is_gpu = bool(gres)
                row_bg = "#ffffff" if is_gpu else "#fafbfc"

                with ui.element("div").style(
                    f"display: grid; grid-template-columns: 1fr 72px 56px 44px 64px; gap: 4px; "
                    f"padding: 2px 11px; border-bottom: 1px solid #fafbfc; background: {row_bg};"
                ):
                    ui.label(display_name).style(
                        f"{MONO} font-size: 10px; color: #374151; "
                        "white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
                    )
                    gres_color = "#2563eb" if is_gpu else "#94a3b8"
                    ui.label(gres or "(none)").style(f"{MONO} font-size: 10px; color: {gres_color};")
                    ui.label(mem).style(f"{MONO} font-size: 10px; color: #374151;")
                    ui.label(cpus).style(f"{MONO} font-size: 10px; color: #374151;")
                    ui.label(time_val).style(f"{MONO} font-size: 10px; color: #374151;")

        with ui.element("div").style("padding: 3px 11px; background: #f8fafc; border-top: 1px solid #e2e8f0;"):
            ui.label("Supervisor (array jobs)").style(
                "font-size: 9px; font-weight: 600; color: #94a3b8; margin-bottom: 2px;"
            )
            _profile_row_fields(sup.gres or "(none)", sup.mem, str(sup.cpus_per_task), sup.time)

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
        history_refs: Dict = {"container": None, "visible": False, "dropdown": None, "path_input": None}
        overview_ref: Dict = {"comp": None}

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
                    ui.button("Clear all", on_click=_clear_history).props("flat dense no-caps").style(
                        f"{FONT} font-size: 10px; color: #94a3b8;"
                    )

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

        # ── dialog ────────────────────────────────────────────────────────────

        with (
            ui.dialog() as dialog,
            ui.card().style(
                "width: 920px; max-width: 92vw; padding: 0; overflow: hidden; "
                "border-radius: 6px; box-shadow: 0 8px 24px rgba(0,0,0,0.12);"
            ),
        ):
            # Base path bar with history dropdown
            with ui.element("div").style(
                "display: flex; align-items: center; gap: 6px; "
                "padding: 7px 10px; border-bottom: 1px solid #e5e7eb; "
                "background: #f8fafc; position: relative;"
            ):
                ui.label("BASE").style(
                    f"{FONT} font-size: 9px; font-weight: 700; color: #94a3b8; letter-spacing: 0.09em; flex-shrink: 0;"
                )

                with ui.element("div").style("flex: 1; position: relative; min-width: 0;"):
                    with ui.element("div").style("display: flex; align-items: center; gap: 4px;"):
                        path_input = (
                            ui.input(value=current_base["path"])
                            .props("dense borderless")
                            .style(f"flex: 1; font-size: 10px; {MONO} color: #1e293b; background: transparent;")
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

            # Mount the same Projects Overview widget as the landing page.
            # Delete is intentionally disabled in the in-workspace switcher --
            # too easy to nuke a project mid-session by mistake. The
            # currently-loaded project is shown but rendered inert with a
            # "current" highlight so the roster keeps continuity.
            overview = ProjectsOverview(
                panel.backend,
                on_open=_switch_project,
                on_delete=None,
                base_path_provider=lambda: current_base["path"],
                auto_refresh_sec=15.0,
                current_path=current_path_str,
                show_filter=True,
                height_px=760,
                title="Projects Overview",
            )
            overview_ref["comp"] = overview
            overview.build()

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

                spinner = ui.label("⠋").style(
                    "font-family: 'IBM Plex Mono', monospace; font-size: 18px; "
                    "color: #3b82f6; text-align: center; line-height: 1; "
                    "display: block; width: 100%; margin-top: 2px;"
                )
                self._refs["spinner"] = spinner

                status_lbl = ui.label("").style(
                    f"font-size: 8px; color: {SB_MUTE}; "
                    "font-family: 'IBM Plex Mono', monospace; "
                    "text-align: center; line-height: 1.4; word-break: break-all; "
                    "display: block; width: 100%; padding: 0 3px;"
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

    # ── Spinner / status label ────────────────────────────────────────────────

    def advance_spinner(self):
        self._spinner_idx = (self._spinner_idx + 1) % len(self._spinner_frames)
        frame = self._spinner_frames[self._spinner_idx]

        el = self._refs.get("spinner")
        if el:
            el.set_text(frame)

        ui.run_javascript(f"document.querySelectorAll('.cb-row-spinner').forEach(e => e.textContent = {repr(frame)});")

    def start_spinner_timer(self):
        if self._refs.get("spinner_timer"):
            return
        self._refs["spinner_timer"] = ui.timer(0.17, self.advance_spinner)

    def stop_spinner_timer(self):
        t = self._refs.pop("spinner_timer", None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass

    def update_status_label(self, overview: Dict):
        el = self._refs.get("status_label")
        if el is None:
            return
        done = overview.get("completed", 0) + overview.get("failed", 0)
        # Backend total already excludes IMPORT_MOVIES and TS_IMPORT (both hidden
        # from PHASE_JOBS / roster). Mirror that filter on the fallback so the
        # first paint -- before the first overview poll returns -- doesn't inflate.
        _hidden = {JobType.IMPORT_MOVIES.value, JobType.TS_IMPORT.value}
        visible_selected = sum(1 for iid in self.panel.ui_mgr.selected_jobs if iid.split("__")[0] not in _hidden)
        total = overview.get("total", visible_selected) if overview else visible_selected
        el.set_text(f"{done}/{total}")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _sb_sep(self):
        ui.element("div").style(f"height: 1px; background: {SB_SEP}; width: 24px; margin: 3px auto;")

    def _load_svg(self, name: str) -> str:
        if name.startswith("<svg"):
            return name
        p = Path("static/icons") / name
        try:
            return p.read_text()
        except FileNotFoundError:
            return '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"/>'

    def _build_aggregation_merge_btn(self):
        """Sidebar button that opens the merge-sources dialog. Shows a small
        green dot when MergedSources/optimisation_set.star already exists so
        you can see at a glance whether the merge has been done."""
        from ui.aggregation_merge_card import has_merged_outputs, open_aggregation_merge_dialog

        merged = has_merged_outputs()
        container = (
            ui.element("div")
            .style(
                "width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                "background: transparent; "
                "display: flex; align-items: center; justify-content: center; "
                "cursor: pointer; flex-shrink: 0; position: relative;"
            )
            .on("click", lambda: open_aggregation_merge_dialog())
            .tooltip("Merge sources" + (" (merged)" if merged else ""))
        )
        with container:
            ui.icon("merge_type", size="18px").style(
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
        """
        from ui.tomo_dashboard_dialog import has_any_previews_rendered, open_tomo_dashboard

        rendered = has_any_previews_rendered()
        svg = self._load_svg(_TOMO_DASHBOARD_SVG).replace("currentColor", SB_MUTE)

        container = (
            ui.element("div")
            .style(
                "width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                "background: transparent; "
                "display: flex; align-items: center; justify-content: center; "
                "cursor: pointer; flex-shrink: 0; position: relative;"
            )
            .on("click", lambda: open_tomo_dashboard())
            .tooltip("Journey" + (" · previews rendered" if rendered else ""))
        )
        with container:
            ui.html(svg, sanitize=False).style("width: 18px; height: 18px; display: flex; pointer-events: none;")
            if rendered:
                ui.element("div").style(
                    "position: absolute; top: 4px; right: 4px; width: 6px; height: 6px; "
                    "border-radius: 50%; background: #16a34a; pointer-events: none;"
                )
        self._refs["dashboard_btn"] = container
        return container

    def _sb_svg_btn(self, svg_name, tooltip, on_click, active=False, ref_key=None, color_override=None):
        bg = SB_ABG if active else "transparent"
        color = color_override or (SB_ACT if active else SB_MUTE)

        svg = self._load_svg(svg_name).replace("currentColor", color)

        container = (
            ui.element("div")
            .style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"background: {bg}; "
                f"display: flex; align-items: center; justify-content: center; "
                f"cursor: pointer; flex-shrink: 0;"
            )
            .on("click", on_click)
            .tooltip(tooltip)
        )
        with container:
            ui.html(svg, sanitize=False).style("width: 18px; height: 18px; display: flex; pointer-events: none;")
        if ref_key:
            self._refs[ref_key] = container
        return container

    def _info_popup_btn(self, icon_name: str, title: str, rows: list, icon_color: str = None):
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


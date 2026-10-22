# ui/pipeline_builder/job_tab_component.py
import asyncio
import logging
from collections.abc import Callable

from nicegui import ui

from backend import get_backend
from services.path_resolution_service import cut_output
from services.project_state import JobStatus, JobType
from ui.components.buttons import house_button, icon_button
from ui.components.copyable import copy_on_click
from ui.components.reactive import Debounced
from ui.current_project import current_project_state
from services.scheduling_and_orchestration.pipeline_deletion_service import get_deletion_service
from ui.job_plugins import get_extra_tabs, get_full_panel_renderer
from ui.job_plugins._field_styles import PANEL_HEAD_STYLE, PANEL_STYLE, PANEL_TITLE_STYLE, config_section, fold_panel
from ui.routing import View
from ui.components.species_pill import render_species_mark
from ui.status_indicator import BoundStatusDot
from services.models_base import instance_id_to_job_type
from ui.ui_state import UIStateManager, MonitorTab, get_instance_display_name
from ui.pipeline_builder.config_tab import render_config_tab, in_use_notice, is_job_frozen
from ui.pipeline_builder.io_tab import render_io_tab
from ui.pipeline_builder.slurm_tab import render_slurm_tab
from ui.pipeline_builder.logs_tab import render_logs_tab
from ui.pipeline_builder.files_tab import render_files_tab
from ui.pipeline_builder.log_progress_chip import render_log_progress_chip

logger = logging.getLogger(__name__)

# Job types still under active development. Flagged with a purple "Experimental" badge
# in the job header so users know the tool is not yet production-hardened (its interface,
# routing and outputs may still change).
_EXPERIMENTAL_JOB_TYPES = {JobType.MISS_ALIGN}

# The line under the view's title on every job page: no rule under it, the sections below are panels.
_JOB_LINE_STYLE = "flex-shrink: 0; gap: 8px; min-height: 22px; padding: 0 14px 6px;"
_PATH_STYLE = (
    "font-size: 10px; font-family: 'IBM Plex Mono', monospace; color: #94a3b8; "
    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0;"
)
# A mark on that line (Interactive / Automatic, Experimental): the house chip, light blue, or warm for
# what is still being built.
_MARK_STYLE = (
    "display: inline-flex; align-items: center; flex-shrink: 0; border-radius: 3px; padding: 0 6px; "
    "font: 600 9px 'IBM Plex Sans', sans-serif; line-height: 16px; white-space: nowrap; "
)
_MARK_INFO = _MARK_STYLE + "background: #edf4fb; border: 1px solid #c9dbee; color: #1f4e79;"
_MARK_WARM = _MARK_STYLE + "background: #fff7e6; border: 1px solid #f3d9a4; color: #8a5a00;"


def create_save_handler(ui_mgr: UIStateManager) -> Callable:
    """Debounced save for config-field handlers: each call re-arms a 1 s
    trailing-edge save of the tab's project via the facade (which coalesces
    per project path).

    The project comes from the tab's `ui_mgr`, not a tab-context lookup: the handler
    also fires from tasks with no client context (the I/O section's save-and-refresh),
    where that lookup yields a blank state and the save is dropped."""

    def _trigger() -> None:
        bk = get_backend()
        if bk is None:
            return
        try:
            asyncio.create_task(bk.save_project(ui_mgr.project_path, debounce_s=1.0))
        except RuntimeError:
            logger.info("Debounced save skipped — no running event loop")

    return _trigger


# The sections after Parameters / I/O / SLURM that read the job directory when built: shown folded
# until opened (or named by the URL), then built once. Each sits in a fixed-height box the user can
# drag taller; its view scrolls inside it.
_LAZY_BOX_STYLE = (
    "width: 100%; height: 360px; min-height: 140px; resize: vertical; overflow: hidden; "
    "display: flex; flex-direction: column; background: #fff; border-radius: 4px; "
    "outline: 1px solid var(--cb-box-line, #dbe3ec); outline-offset: -1px;"
)


def _section_dom_id(instance_id: str, key: str) -> str:
    return f"cb-jsec-{instance_id}-{key}"


def _lazy_sections(job_type: JobType) -> list[tuple[str, str, bool]]:
    """(key, title, boxed) of the page's folded sections, in page order: the extra tabs (Tasks,
    Results), the log, the files. Boxed ones fill a fixed-height box; the rest take their height."""
    out = [(et.key, et.label, et.key == "tasks") for et in get_extra_tabs(job_type)]
    out.append((MonitorTab.LOGS.value, "Log", True))
    out.append((MonitorTab.FILES.value, "Files", True))
    return out


def _render_lazy_section(title: str, boxed: bool, *, open_: bool, dom_id: str, build: Callable) -> Callable:
    """A folded section: its title line opens and closes it; the content is built on the first
    opening. Returns its opener."""
    built = {"done": False}
    panel = ui.column().classes("w-full cb-jsec-fold").style(f"gap: 8px; {PANEL_STYLE}")
    with panel.props(f"id={dom_id}"):
        head_style = f"gap: 5px; cursor: pointer; user-select: none; {PANEL_HEAD_STYLE}"
        with ui.row().classes("items-center no-wrap").style(head_style) as head:
            caret = ui.label("▸").style("font-size: 9px; color: #94a3b8; width: 8px;")
            ui.label(title).style(PANEL_TITLE_STYLE)
        body = ui.element("div").style(_LAZY_BOX_STYLE if boxed else "width: 100%;")
        body.set_visibility(False)

    def set_open(on: bool) -> None:
        if on and not built["done"]:
            built["done"] = True
            with body:
                build()
        body.set_visibility(on)
        caret.set_text("▾" if on else "▸")
        fold_panel(panel, head, on)

    head.on("click", lambda _e: set_open(not body.visible))
    set_open(open_)
    return lambda: set_open(True)


def _render_job_page(
    job_type: JobType,
    instance_id: str,
    job_model,
    is_frozen: bool,
    save_handler: Callable,
    backend,
    ui_mgr: UIStateManager,
    callbacks: dict[str, Callable] | None = None,
) -> dict[str, Callable]:
    """The job's one page: Parameters, Inputs / Outputs and SLURM resources open, then the folded
    sections (_lazy_sections). Returns each folded section's opener by key."""
    # An edit in one section can change what another shows: missAlign's parameters set the time limit
    # and GRES its SLURM section displays, and its warnings read the SLURM settings. Parameters and
    # SLURM save through `on_edit`, which then reruns the refresh each of them hands back.
    edit_refreshes: list[Callable[[], None]] = []

    def _run_refreshes() -> None:
        for refresh in edit_refreshes:
            refresh()

    # The fields write the model on every change; the sections that follow them repaint once
    # typing pauses, not per keystroke.
    refresh_soon = Debounced(_run_refreshes)

    def on_edit() -> None:
        save_handler()
        refresh_soon()

    openers: dict[str, Callable] = {}
    active = ui_mgr.get_job_ui_state(instance_id).active_monitor_tab
    # cb-scroll-tight drops the scroll area's own 1 rem padding, so the panels' left edges line up with
    # the view's title and the job's line above them (both 14 px in). The cb-job* classes place each
    # section in the two-column grid the styles lay a wide page out in (ui/dashboard/css.py `_THEME_CSS`).
    with ui.scroll_area().classes("w-full h-full cb-scroll-tight cb-jobscroll"):
        with ui.column().classes("w-full cb-jobpage").style("padding: 4px 14px 16px; gap: 10px;"):
            with config_section("Parameters") as params_sec:
                params_sec.classes("cb-jsec-params")
                params_refresh = render_config_tab(
                    job_type,
                    job_model,
                    is_frozen,
                    ui_mgr,
                    backend,
                    on_edit,
                    instance_id=instance_id,
                    callbacks=callbacks,
                )
                if params_refresh is not None:
                    edit_refreshes.append(params_refresh)
            # An input chosen there changes what the other sections say of it (template matching's
            # search-region note), so it is an edit like theirs.
            io_sec = ui.element("div").classes("cb-jsec-io").style("width: 100%;")
            with io_sec:
                edit_refreshes.append(render_io_tab(job_type, instance_id, job_model, is_frozen, ui_mgr, on_edit))
            with config_section("SLURM resources") as slurm_sec:
                slurm_sec.classes("cb-jsec-slurm")
                edit_refreshes.append(render_slurm_tab(job_model, is_frozen, on_edit))

            builders: dict[str, Callable[[], None]] = {
                et.key: lambda et=et: et.render(job_type, instance_id, job_model, backend, ui_mgr)
                for et in get_extra_tabs(job_type)
            }
            builders[MonitorTab.LOGS.value] = lambda: render_logs_tab(job_type, instance_id, job_model, backend, ui_mgr)
            builders[MonitorTab.FILES.value] = lambda: render_files_tab(job_type, job_model, ui_mgr)
            for key, title, boxed in _lazy_sections(job_type):
                # The log is open from the start once the job has run; the others when the URL names them.
                open_ = key == active or (key == MonitorTab.LOGS.value and bool(job_model.relion_job_name))
                openers[key] = _render_lazy_section(
                    title, boxed, open_=open_, dom_id=_section_dom_id(instance_id, key), build=builders[key]
                )
    return openers


def render_job_tab(
    job_type: JobType, instance_id: str, backend, ui_mgr: UIStateManager, callbacks: dict[str, Callable]
) -> None:
    state = current_project_state()
    job_model = state.jobs.get(instance_id)

    if not job_model:
        ui.label(f"Error: Job model for '{instance_id}' not found.").classes("text-xs text-red-600")
        return

    # --- Full-panel interactive jobs (tilt filter, etc.) bypass tab chrome ---
    full_renderer = get_full_panel_renderer(job_type)
    if full_renderer:
        _render_interactive_job(job_type, instance_id, job_model, backend, ui_mgr, callbacks, full_renderer)
        return

    widget_refs = ui_mgr.get_job_widget_refs(instance_id)
    frozen = is_job_frozen(instance_id)
    is_running = job_model.execution_status == JobStatus.RUNNING

    # --- The job's line under the view's title (which names the job): its marks, folder and actions ---
    with ui.row().classes("w-full items-center no-wrap").style(_JOB_LINE_STYLE):
        # Species glyph for particle jobs (templatematching / subtomo / reconstruct
        # / class3d / candidate-extract), named on hover — the roster's own mark.
        _sp_id = getattr(job_model, "species_id", None)
        _species = state.get_species(_sp_id) if _sp_id else None
        if _species is not None:
            render_species_mark(_species)

        if job_type in _EXPERIMENTAL_JOB_TYPES:
            ui.label("Experimental").style(_MARK_WARM).tooltip(
                "Experimental — under active development; interface, routing and outputs may change"
            )

        if job_model.relion_job_name and ui_mgr.project_path:
            full_path = str(ui_mgr.project_path / job_model.relion_job_name.rstrip("/"))
            copy_on_click(ui.label(full_path).style(_PATH_STYLE), full_path)

        render_log_progress_chip(job_type, instance_id, ui_mgr)

        ui.space()

        if cut_output(job_type) is not None:
            house_button(
                "Subset",
                lambda: _handle_subset(instance_id, callbacks),
                tooltip="A new Subset job: this job's output cut down to the tilt series you choose, for jobs "
                "added after it. Needs this job finished.",
            )

        if is_running:
            house_button(
                "Cancel", lambda: _handle_stop_job(job_type, instance_id, job_model, backend, ui_mgr, callbacks)
            )

        icon_button("refresh", lambda: _force_status_refresh(callbacks), tooltip="Refresh the job's status")

        if ui_mgr.is_project_created:
            icon_button(
                "delete",
                lambda: _handle_delete(job_type, instance_id, job_model, backend, ui_mgr, callbacks),
                tooltip="Delete this job",
                kind="danger",
            )

    # --- Content ---
    save_handler = create_save_handler(ui_mgr)
    with ui.column().classes("w-full overflow-hidden").style("flex: 1 1 0%; min-height: 0;"):
        widget_refs.section_openers = _render_job_page(
            job_type, instance_id, job_model, frozen, save_handler, backend, ui_mgr, callbacks
        )


def _render_interactive_job(
    job_type: JobType,
    instance_id: str,
    job_model,
    backend,
    ui_mgr: UIStateManager,
    callbacks: dict[str, Callable],
    full_renderer: Callable,
):
    """Lightweight chrome for interactive tool-type jobs (no tab strip)."""
    # The tilt filter in DL auto is a chain job, which its IS_INTERACTIVE says.
    interactive = getattr(job_model, "IS_INTERACTIVE", True)
    pill_tip = (
        "Interactive — this step is driven by you: label the tilts (by hand, or by reviewing DL "
        "predictions) and Approve. A Run holds alignment and everything after it until you do. If you "
        "don't add this job to the pipeline, processing continues without tilt filtering."
        if interactive
        else "Automatic — a Run submits this step as a chain job after fsMotion: it predicts every tilt and "
        "drops those at or above the threshold (labels you set still win), and alignment follows without a review."
    )
    # --- The job's line under the view's title, as on every job page ---
    with ui.row().classes("w-full items-center no-wrap").style(_JOB_LINE_STYLE):
        BoundStatusDot(instance_id)
        ui.label("Interactive" if interactive else "Automatic").style(_MARK_INFO).tooltip(pill_tip)
        if job_model.relion_job_name and ui_mgr.project_path:
            full_path = str(ui_mgr.project_path / job_model.relion_job_name.rstrip("/"))
            ui.label(full_path).style(_PATH_STYLE)
        ui.space()
        if ui_mgr.is_project_created:
            icon_button(
                "delete",
                lambda: _handle_delete(job_type, instance_id, job_model, backend, ui_mgr, callbacks),
                tooltip="Delete this job",
                kind="danger",
            )

    # --- Full panel content ---
    save_handler = create_save_handler(ui_mgr)
    with ui.column().classes("w-full overflow-hidden").style("flex: 1 1 0%; min-height: 0;"):
        full_renderer(job_type, instance_id, job_model, backend, ui_mgr, save_handler)


def _handle_tab_switch(
    job_type: JobType, instance_id: str, tab_key: str, backend, ui_mgr: UIStateManager, callbacks: dict[str, Callable]
):
    """A deep link to one part of the job's page (the URL's third segment, the roster's Tasks
    link): open that section and scroll it into view; "config" is the top of the page."""
    ui_mgr.set_job_monitor_tab(instance_id, tab_key, user_initiated=True)
    set_url = (callbacks or {}).get("set_url")
    if set_url:
        set_url(View.JOB, instance_id, tab_key)
    opener = ui_mgr.get_job_widget_refs(instance_id).section_openers.get(tab_key)
    if opener is None:
        return
    opener()
    # A section built just now reaches the browser with this update; scroll once it is there.
    ui.run_javascript(
        f"setTimeout(() => document.getElementById({_section_dom_id(instance_id, tab_key)!r})"
        "?.scrollIntoView({block: 'start', behavior: 'smooth'}), 60);"
    )


async def _handle_stop_job(
    job_type: JobType, instance_id: str, job_model, backend, ui_mgr: UIStateManager, callbacks: dict[str, Callable]
):
    project_path = ui_mgr.project_path
    job_dir = (project_path / job_model.relion_job_name.rstrip("/")) if job_model.relion_job_name else None

    with ui.dialog() as dialog, ui.card().style("min-width: 360px; padding: 16px;"):
        ui.label(f"Cancel {get_instance_display_name(instance_id, job_model)}?").classes(
            "text-base font-bold text-gray-800"
        )
        if job_dir:
            ui.label(str(job_dir)).classes("text-xs font-mono text-gray-500 mt-1")
        ui.label(
            "The SLURM job will be cancelled and this job marked Failed; jobs that wait for it fail too, "
            "the rest keep running."
        ).classes("text-sm text-gray-600 mt-2")
        with ui.row().classes("mt-4 gap-2 justify-end w-full"):
            house_button("Cancel", lambda: dialog.submit(False))
            house_button("Stop job", lambda: dialog.submit(True), kind="accent")

    confirmed = await dialog
    if not confirmed:
        return

    result = await backend.pipeline_runner.cancel_job(project_path, instance_id)

    # The other jobs keep running, and so does the pipeline (pipeline_active stays until every
    # job has ended): the tab keeps polling while it is active.
    active = bool(current_project_state().pipeline_active)
    if not active and "stop_all_timers" in callbacks:
        callbacks["stop_all_timers"]()
    ui_mgr.set_pipeline_running(active)
    if "rebuild_pipeline_ui" in callbacks:
        callbacks["rebuild_pipeline_ui"]()

    if result.get("success"):
        ui.notify(result.get("message", "Job cancelled."), type="warning", timeout=5000)
    else:
        ui.notify(f"Cancel failed: {result.get('error')}", type="negative", timeout=8000)


def _handle_subset(instance_id: str, callbacks: dict[str, Callable]) -> None:
    """The Subset button: a Subset of this job's output, refused until the job has finished (the
    header is not re-rendered when its status moves, so the status is read here)."""
    job_model = current_project_state().jobs.get(instance_id)
    if job_model is None or job_model.execution_status != JobStatus.SUCCEEDED:
        ui.notify("A Subset cuts a finished output: wait for this job to succeed.", type="warning")
        return
    add = callbacks.get("add_instance_to_pipeline")
    if add is not None:
        add(JobType.SUBSET, after=instance_id)


def _handle_delete(
    job_type: JobType, instance_id: str, job_model, backend, ui_mgr: UIStateManager, callbacks: dict[str, Callable]
):
    notice = in_use_notice(current_project_state(), instance_id)
    if notice:
        ui.notify(notice, type="warning")
        return
    # Interactive jobs use the roster's custom removal flow.
    if getattr(job_model, "IS_INTERACTIVE", False):
        remove_cb = callbacks.get("remove_instance_from_pipeline")
        if remove_cb:
            state = current_project_state()
            if state and instance_id in state.jobs:
                del state.jobs[instance_id]
                state.job_path_mapping.pop(instance_id, None)
                state.mark_dirty()
                asyncio.create_task(get_backend().save_project(state.project_path))
            ui.notify("Tilt filter removed. Labels preserved.", type="info")
            remove_cb(instance_id)
        return

    preview = get_deletion_service().preview_deletion(current_project_state(), instance_id)

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
            ui.label("No downstream jobs will be affected.").classes("text-sm text-green-600 bg-green-50 p-2 rounded")

        with ui.row().classes("w-full justify-end mt-4 gap-2"):
            house_button("Cancel", dialog.close)

            async def confirm():
                dialog.close()
                ui.notify("Deleting job...", type="info", timeout=1500)
                try:
                    result = await backend.delete_job(
                        instance_id_to_job_type(instance_id).value,
                        project_path=ui_mgr.project_path,
                        instance_id=instance_id,
                    )
                    if result.get("success"):
                        orphans = result.get("orphaned_jobs", [])
                        if orphans:
                            ui.notify(
                                f"Job deleted. {len(orphans)} downstream job(s) orphaned.", type="warning", timeout=5000
                            )
                        else:
                            ui.notify("Job deleted successfully.", type="positive")
                        remove_cb = callbacks.get("remove_instance_from_pipeline")
                        if remove_cb:
                            remove_cb(instance_id)
                    else:
                        ui.notify(f"Delete failed: {result.get('error')}", type="negative", timeout=8000)
                except Exception as e:
                    ui.notify(f"Error: {e}", type="negative")
                    import traceback

                    traceback.print_exc()

            house_button("Delete", confirm, kind="danger")

    dialog.open()


def _force_status_refresh(callbacks: dict[str, Callable]):
    ui.notify("Refreshing statuses...", timeout=1)
    if "check_and_update_statuses" in callbacks:
        asyncio.create_task(callbacks["check_and_update_statuses"]())

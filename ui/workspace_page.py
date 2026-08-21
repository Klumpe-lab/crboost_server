import asyncio

from nicegui import ui

from backend import CryoBoostBackend
from ui.background_task_tray import mount_background_task_tray
from ui.components.buttons import house_button
from ui.components.reactive import SingleFlight
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
from ui.species.page import build_species_page
from ui.ui_state import get_ui_state_manager


# Client-side resize for the job-roster panel: drag the divider, clamp to a
# sane range, and persist the width in localStorage so it survives reloads. A
# MutationObserver mirrors the roster's display onto the handle so the divider
# hides with the roster in journey mode. Idempotent (guarded by data-wired).
_ROSTER_RESIZER_JS = """
(function(){
  const roster = document.getElementById('cb-roster-panel');
  const handle = document.getElementById('cb-roster-resizer');
  if(!roster || !handle || handle.dataset.wired) return;
  handle.dataset.wired = '1';
  const MINW = 240, MAXW = 720;
  const clamp = (w) => Math.min(MAXW, Math.max(MINW, w));
  const apply = (w) => { roster.style.width = w+'px'; roster.style.minWidth = w+'px'; };
  try { const s = localStorage.getItem('cbRosterW'); if(s) apply(clamp(parseInt(s,10))); } catch(e){}
  let dragging=false, startX=0, startW=0;
  handle.addEventListener('mousedown', (e)=>{
    dragging=true; startX=e.clientX; startW=roster.getBoundingClientRect().width;
    handle.classList.add('dragging');
    document.body.style.cursor='col-resize'; document.body.style.userSelect='none';
    e.preventDefault();
  });
  window.addEventListener('mousemove', (e)=>{
    if(!dragging) return;
    apply(clamp(Math.round(startW + (e.clientX - startX))));
  });
  window.addEventListener('mouseup', ()=>{
    if(!dragging) return;
    dragging=false; handle.classList.remove('dragging');
    document.body.style.cursor=''; document.body.style.userSelect='';
    try { localStorage.setItem('cbRosterW', String(Math.round(roster.getBoundingClientRect().width))); } catch(e){}
  });
  try {
    const obs = new MutationObserver(()=>{
      handle.style.display = (getComputedStyle(roster).display === 'none') ? 'none' : 'block';
    });
    obs.observe(roster, {attributes:true, attributeFilter:['style']});
  } catch(e){}
})();
"""


def build_workspace_page(backend: CryoBoostBackend):
    ui_mgr = get_ui_state_manager()
    ui_mgr.prepare_for_page_rebuild()

    if not ui_mgr.is_project_created:
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("error_outline", size="64px").classes("text-red-400")
            ui.label("No project loaded").classes("text-xl text-gray-600")
            house_button("Return to start", lambda: ui.navigate.to("/"))
        return

    if not ui_mgr.project_path or not ui_mgr.project_path.exists():
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("folder_off", size="64px").classes("text-orange-400")
            ui.label("Project path is invalid").classes("text-xl text-gray-600")
            ui.label(str(ui_mgr.project_path)).classes("text-sm text-gray-400 font-mono")
            house_button("Return to start", lambda: ui.navigate.to("/"))
        return

    callbacks = {}
    _mode = {"current": "pipeline"}
    _refs = {}

    def _switch_to(mode_name: str):
        """Swap the visible view in main_area: pipeline / workbench / journey.
        Each is a sibling container toggled via CSS display. Journey also hides
        the 300px job roster for full width and pauses its live-refresh timer
        when not visible (handled by the roster's set_active_mode and the
        journey panel's on_journey_active)."""
        containers = {
            "pipeline": _refs.get("pipeline_container"),
            "workbench": _refs.get("workbench_container"),
            "journey": _refs.get("journey_container"),
        }
        # If already on this mode, toggle back to pipeline.
        if _mode["current"] == mode_name and mode_name != "pipeline":
            mode_name = "pipeline"

        _mode["current"] = mode_name
        for name, c in containers.items():
            if c is None:
                continue
            if name == mode_name:
                c.style("display: flex; flex-direction: column;")
            else:
                c.style("display: none;")

        if mode_name == "pipeline":
            invalidate = callbacks.get("invalidate_tm_tabs")
            if invalidate:
                invalidate()

        # Nav-icon highlight + roster hide/restore (roster owns _roster_visible).
        set_mode = callbacks.get("set_active_mode")
        if set_mode:
            set_mode(_mode["current"])

        # Pause/resume the journey's 4 s live-refresh with its visibility.
        on_journey = callbacks.get("on_journey_active")
        if on_journey:
            on_journey(_mode["current"] == "journey")

        # Instant registry observe when the Species page is shown (species created
        # from the roster / another tab appear without waiting for its 3 s poll).
        on_wb = callbacks.get("on_workbench_active")
        if on_wb:
            on_wb(_mode["current"] == "workbench")

    def _toggle_workbench():
        _switch_to("workbench")

    def _open_species(species_id: str):
        """The 'open in Species' link of a job's Config tab: show the Species page ON
        that species (the view keeps its internal mode name "workbench")."""
        if _mode["current"] != "workbench":
            _switch_to("workbench")
        select = callbacks.get("workbench_select_species")
        if select:
            select(species_id)

    def ensure_pipeline_mode():
        if _mode["current"] != "pipeline":
            _switch_to("pipeline")

    _journey_flight = SingleFlight()
    _journey_built = {"done": False}

    async def _show_journey():
        # SingleFlight: the icon button can be destroyed+recreated mid-click by
        # a roster refresh, so several clicks may land — collapse them to one.
        async with _journey_flight("toggle") as acquired:
            if not acquired:
                return
            _switch_to("journey")  # toggles back to pipeline if already on journey
            if _mode["current"] != "journey":
                return
            if _journey_built["done"]:
                return
            _journey_built["done"] = True
            jc = _refs.get("journey_container")
            if jc is None:
                return
            # Paint a spinner immediately, yield so it flushes to the client,
            # then build. The first per-TS render blocks ~1-2 s; the CSS spinner
            # keeps animating client-side meanwhile. Later switches are an
            # instant display swap (the panel persists).
            with (
                jc,
                ui.element("div").style(
                    "width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; gap: 10px;"
                ),
            ):
                ui.spinner(size="lg", color="indigo")
                ui.label("Loading journey…").classes("text-sm text-gray-500")
            await asyncio.sleep(0.03)
            from ui.tomo_dashboard_dialog import build_journey_panel

            build_journey_panel(jc, callbacks)

    callbacks["toggle_workbench"] = _toggle_workbench
    callbacks["open_species"] = _open_species
    callbacks["ensure_pipeline_mode"] = ensure_pipeline_mode
    callbacks["toggle_journey"] = _show_journey

    with ui.element("div").style(
        "position: fixed; inset: 0; display: flex; flex-direction: row; "
        "overflow: hidden; gap: 0; margin: 0; padding: 0;"
    ):
        primary_sidebar = ui.element("div").style(
            "width: 60px; min-width: 60px; height: 100%; flex-shrink: 0; "
            "background: #f8fafc; display: flex; flex-direction: column; "
            "align-items: center; gap: 0; overflow: visible; z-index: 20; "
            "border-right: 1px solid #e2e8f0;"
        )

        roster_panel = (
            ui.element("div")
            .props("id=cb-roster-panel")
            .style(
                "width: 340px; min-width: 340px; height: 100%; flex-shrink: 0; "
                "background: #ffffff; border-right: 1px solid #e5e7eb; "
                "overflow-y: auto; overflow-x: hidden; "
                "flex-direction: column; gap: 0; display: flex;"
            )
        )

        # Draggable divider between the job roster and the params/main area.
        # Resize + persistence is client-side JS (see _ROSTER_RESIZER_JS) so the
        # drag is smooth and survives reloads via localStorage.
        ui.element("div").props("id=cb-roster-resizer").classes("cb-roster-resizer")

        main_area = ui.element("div").style(
            "flex: 1; min-width: 0; height: 100%; overflow: hidden; display: flex; flex-direction: row; gap: 0;"
        )

        with main_area:
            pipeline_container = ui.element("div").style(
                "width: 100%; height: 100%; display: flex; flex-direction: column;"
            )
            _refs["pipeline_container"] = pipeline_container
            with pipeline_container:
                build_pipeline_builder_panel(
                    backend,
                    callbacks,
                    primary_sidebar=primary_sidebar,
                    roster_panel=roster_panel,
                    toggle_workbench=_toggle_workbench,
                    ensure_pipeline_mode=ensure_pipeline_mode,
                    toggle_journey=_show_journey,
                )

            workbench_container = ui.element("div").style(
                "width: 100%; height: 100%; display: none; flex-direction: column;"
            )
            _refs["workbench_container"] = workbench_container
            with workbench_container:
                build_species_page(backend, callbacks)

            # Journey: built lazily on first switch (see _show_journey) so the
            # heavy per-TS render doesn't tax every workspace load. Starts hidden.
            journey_container = ui.element("div").style(
                "width: 100%; height: 100%; display: none; flex-direction: column;"
            )
            _refs["journey_container"] = journey_container

    # Floating background-tasks tray at workspace scope so spun-off
    # renders/builds remain visible across dialog open/close and view
    # toggles. Survives the entire workspace session; teardown happens
    # when the page rebuilds.
    mount_background_task_tray(
        project_path_provider=lambda: str(ui_mgr.project_path) if ui_mgr.project_path else None
    )

    # Wire the roster resizer once the DOM for this page exists on the client.
    ui.timer(0.1, lambda: ui.run_javascript(_ROSTER_RESIZER_JS), once=True)

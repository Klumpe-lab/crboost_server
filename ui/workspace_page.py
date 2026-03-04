# ui/workspace_page.py

from nicegui import ui
from backend import CryoBoostBackend
from services.project_state import get_project_state
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
from ui.ui_state import get_ui_state_manager


def _fmt(v) -> str:
    if v is None:
        return "---"
    if isinstance(v, float):
        return f"{v:.4g}"
    return str(v)


def build_workspace_page(backend: CryoBoostBackend):
    ui_mgr = get_ui_state_manager()
    state = get_project_state()

    ui_mgr.prepare_for_page_rebuild()

    if not ui_mgr.is_project_created:
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("error_outline", size="64px").classes("text-red-400")
            ui.label("No project loaded").classes("text-xl text-gray-600")
            ui.button("Return to Start", icon="home", on_click=lambda: ui.navigate.to("/"))
        return

    if not ui_mgr.project_path or not ui_mgr.project_path.exists():
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("folder_off", size="64px").classes("text-orange-400")
            ui.label("Project path is invalid").classes("text-xl text-gray-600")
            ui.label(f"Path: {ui_mgr.project_path}").classes("text-sm text-gray-400 font-mono")
            ui.button("Return to Start", icon="home", on_click=lambda: ui.navigate.to("/"))
        return

    # ===================================================================
    # HEADER
    # ===================================================================
    with ui.header().classes("bg-white border-b border-gray-200 text-gray-800 h-auto px-4 py-1"):
        with ui.row().classes("w-full items-center justify-between"):
            with ui.row().classes("items-center gap-5"):
                with ui.row().classes("items-center gap-1"):
                    ui.icon("layers", size="16px").classes("text-blue-600")
                    ui.label(state.project_name).classes("font-semibold text-xs")

                    with (
                        ui.button(icon="science", on_click=None)
                        .props("flat dense round size=sm")
                        .classes("text-gray-400 hover:text-blue-600")
                    ):
                        with ui.menu().classes("p-0"):
                            with ui.card().classes("p-3 border border-gray-200 shadow-md").style("min-width: 220px;"):
                                ui.label("Experimental Parameters").classes(
                                    "text-[10px] font-black text-gray-400 uppercase tracking-wide mb-2"
                                )
                                _params = [
                                    ("Pixel Size", f"{_fmt(state.microscope.pixel_size_angstrom)} A"),
                                    ("Voltage", f"{_fmt(state.microscope.acceleration_voltage_kv)} kV"),
                                    ("Cs", f"{_fmt(state.microscope.spherical_aberration_mm)} mm"),
                                    ("Amp Contrast", _fmt(state.microscope.amplitude_contrast)),
                                    ("Dose / Tilt", f"{_fmt(state.acquisition.dose_per_tilt)} e-/A^2"),
                                    ("Tilt Axis", f"{_fmt(state.acquisition.tilt_axis_degrees)} deg"),
                                ]
                                for plabel, pval in _params:
                                    with ui.row().classes(
                                        "w-full justify-between py-1 border-b border-gray-100 last:border-0"
                                    ):
                                        ui.label(plabel).classes("text-xs text-gray-500")
                                        ui.label(pval).classes("text-xs font-medium text-gray-700 font-mono")

                if state.project_path:
                    with ui.column().classes("gap-0"):
                        ui.label("ROOT").classes("text-[8px] font-bold text-gray-400 uppercase leading-none")
                        ui.label(str(state.project_path)).classes("text-[10px] font-mono text-gray-500 leading-tight")

                if state.movies_glob:
                    with ui.column().classes("gap-0"):
                        ui.label("MOVIES").classes("text-[8px] font-bold text-gray-400 uppercase leading-none")
                        ui.label(state.movies_glob).classes("text-[10px] font-mono text-gray-500 leading-tight")

                if state.mdocs_glob:
                    with ui.column().classes("gap-0"):
                        ui.label("MDOC").classes("text-[8px] font-bold text-gray-400 uppercase leading-none")
                        ui.label(state.mdocs_glob).classes("text-[10px] font-mono text-gray-500 leading-tight")

            with ui.row().classes("items-center gap-1"):
                ui.button(icon="close", on_click=lambda: ui.navigate.to("/")).props("flat dense round size=sm").classes(
                    "text-red-400"
                ).tooltip("Close project")

    # ===================================================================
    # MAIN CONTENT
    # ===================================================================
    callbacks = {}

    with ui.column().classes("w-full p-0").style("height: calc(100vh - 56px); overflow: hidden;"):
        build_pipeline_builder_panel(backend, callbacks)

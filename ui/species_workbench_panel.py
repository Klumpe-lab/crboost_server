from __future__ import annotations
import asyncio
from pathlib import Path
from typing import Dict, Optional

from nicegui import ui

from services.project_state import get_project_state_for, get_state_service
from ui.ui_state import get_ui_state_manager


async def _prompt_species_name() -> Optional[str]:
    with ui.dialog() as dialog, ui.card().classes("w-80 p-4 gap-3"):
        ui.label("New Species").classes("text-base font-bold text-gray-800")
        name_input = (
            ui.input(label="Species name", placeholder="e.g. Ribosome, 26S Proteasome")
            .props("outlined dense autofocus")
            .classes("w-full")
        )

        def _confirm():
            v = name_input.value.strip()
            if v:
                dialog.submit(v)

        def _cancel():
            dialog.submit(None)

        name_input.on("keydown.enter", _confirm)

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=_cancel).props("flat dense no-caps")
            ui.button("Create", on_click=_confirm).props("dense no-caps unelevated color=primary")

    return await dialog


def build_species_workbench_panel(backend) -> None:
    ui_mgr = get_ui_state_manager()
    project_path = ui_mgr.project_path
    if not project_path:
        with ui.column().classes("w-full h-full items-center justify-center"):
            ui.label("No project loaded").classes("text-sm text-gray-400")
        return

    _active: Dict[str, Optional[str]] = {"species_id": None}
    _workbench_containers: Dict[str, object] = {}
    _refs: Dict[str, object] = {}

    # ── Tab strip ────────────────────────────────────────────────────────────

    def _refresh_tab_strip():
        strip = _refs.get("strip")
        if not strip:
            return
        strip.clear()
        state = get_project_state_for(project_path)
        with strip:
            for species in state.species_registry:
                is_active = _active["species_id"] == species.id
                with (
                    ui.button(on_click=lambda sid=species.id: _switch_species(sid))
                    .props("flat no-caps dense")
                    .style(
                        f"padding: 6px 18px; border-radius: 0; "
                        f"background: {'white' if is_active else '#fafafa'}; "
                        f"color: {'#1f2937' if is_active else '#9ca3af'}; "
                        f"border-top: 3px solid {species.color if is_active else 'transparent'}; "
                        f"border-right: 1px solid #e5e7eb; "
                        f"font-size: 12px; font-weight: {'500' if is_active else '400'};"
                    )
                ):
                    with ui.row().classes("items-center gap-2"):
                        ui.element("div").style(
                            f"width: 8px; height: 8px; border-radius: 50%; "
                            f"background: {species.color}; flex-shrink: 0;"
                        )
                        ui.label(species.name)

            ui.button(icon="add", on_click=_add_species).props("flat dense round size=sm").style(
                "color: #6b7280; margin: 0 6px;"
            ).tooltip("Add species")

    # ── Species rendering ─────────────────────────────────────────────────────

    def _ensure_species_rendered(species_id: str):
        if species_id in _workbench_containers:
            return
        content = _refs.get("content")
        if not content:
            return
        state = get_project_state_for(project_path)
        species = state.get_species(species_id)
        if not species:
            return
        species_folder = project_path / "templates" / species_id
        species_folder.mkdir(parents=True, exist_ok=True)

        with content:
            container = (
                ui.column()
                .classes("w-full overflow-auto")
                .style("flex: 1 1 0%; min-height: 0;")
            )
            container.set_visibility(False)
            _workbench_containers[species_id] = container
            with container:
                from ui.template_workbench import TemplateWorkbench
                TemplateWorkbench(backend, str(project_path), species_id=species_id)

    def _switch_species(species_id: str):
        _active["species_id"] = species_id
        # Hide empty state if still visible
        empty = _refs.get("empty")
        if empty:
            empty.set_visibility(False)
        _ensure_species_rendered(species_id)
        for sid, c in _workbench_containers.items():
            c.set_visibility(sid == species_id)
        _refresh_tab_strip()

    async def _add_species():
        name = await _prompt_species_name()
        if not name:
            return
        state = get_project_state_for(project_path)
        species = state.add_species(name)
        (project_path / "templates" / species.id).mkdir(parents=True, exist_ok=True)
        await get_state_service().save_project(project_path=project_path)
        _switch_species(species.id)

    # ── Layout ────────────────────────────────────────────────────────────────

    with ui.column().classes("w-full h-full gap-0").style("min-height: 0;"):
        # Header bar
        with ui.row().classes("w-full items-center px-4 py-2 border-b bg-gray-50 gap-3 flex-shrink-0"):
            ui.icon("biotech", size="16px").style("color: #6b7280;")
            ui.label("Template Workbench").classes("text-sm font-semibold text-gray-700")

        # Tab strip
        strip = ui.element("div").style(
            "display: flex; flex-direction: row; width: 100%; flex-shrink: 0; "
            "border-bottom: 1px solid #e5e7eb; overflow-x: auto; background: #fafafa; "
            "align-items: stretch;"
        )
        _refs["strip"] = strip

        # Content area
        content = ui.element("div").style(
            "display: flex; flex-direction: column; width: 100%; flex: 1 1 0%; "
            "min-height: 0; overflow: hidden;"
        )
        _refs["content"] = content

    # ── Initial state ─────────────────────────────────────────────────────────

    state = get_project_state_for(project_path)

    with content:
        empty = ui.column().classes("w-full h-full items-center justify-center gap-4")
        _refs["empty"] = empty
        with empty:
            ui.icon("biotech", size="48px").classes("text-gray-300")
            ui.label("No species registered yet").classes("text-sm text-gray-400")
            ui.button("Add first species", icon="add", on_click=_add_species).props(
                "unelevated no-caps"
            ).style("background: #3b82f6; color: white; border-radius: 6px; padding: 6px 16px;")

    _refresh_tab_strip()

    if state.species_registry:
        _switch_species(state.species_registry[0].id)
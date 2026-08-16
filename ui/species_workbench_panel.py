from __future__ import annotations

from nicegui import ui

from services.project_state import get_project_state_for
from ui.components.reactive import FingerprintedView
from ui.ui_state import get_ui_state_manager


async def _prompt_species_name() -> str | None:
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


def build_species_workbench_panel(backend, callbacks: dict | None = None) -> None:
    """Build the Template Workbench view. ``callbacks``, when given, receives
    ``on_workbench_active(bool)`` (instant registry observe when the view is shown;
    the 3 s poll covers the rest) and ``workbench_select_species(species_id)`` (the
    "open in Species" link of a job's Config tab)."""
    ui_mgr = get_ui_state_manager()
    project_path = ui_mgr.project_path
    if not project_path:
        with ui.column().classes("w-full h-full items-center justify-center"):
            ui.label("No project loaded").classes("text-sm text-gray-400")
        return

    _active: dict[str, str | None] = {"species_id": None}
    _workbench_containers: dict[str, object] = {}
    _refs: dict[str, object] = {}

    # ── Tab strip ────────────────────────────────────────────────────────────
    # A FingerprintedView so every strip repaint goes through ONE gate: species
    # created / renamed / recolored / deleted anywhere (roster "+", another tab,
    # the workbench swatch) show up here without a browser reload (roadmap 08 S0.4).

    class _StripView(FingerprintedView):
        def _get_container(self):
            return _refs.get("strip")

        def signature(self):
            return (_active["species_id"], get_project_state_for(project_path).species_identity())

        def render(self):
            state = get_project_state_for(project_path)
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

    strip_view = _StripView()

    # ── Species rendering ─────────────────────────────────────────────────────

    def _drop_container(species_id: str):
        container = _workbench_containers.pop(species_id, None)
        if container is not None:
            content = _refs.get("content")
            if content:
                try:
                    content.remove(container)
                except Exception:
                    container.set_visibility(False)

    def _handle_species_deleted(species_id: str):
        """Workbench just confirmed deletion. Drop its container and
        switch to whatever remains (or fall back to the empty state)."""
        _drop_container(species_id)
        # State already mutated by the workbench — re-read to find remainder.
        state = get_project_state_for(project_path)
        if state.species_registry:
            _switch_species(state.species_registry[0].id)
        else:
            _active["species_id"] = None
            empty = _refs.get("empty")
            if empty:
                empty.set_visibility(True)
            strip_view.refresh()

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
                TemplateWorkbench(
                    backend,
                    str(project_path),
                    species_id=species_id,
                    on_species_deleted=_handle_species_deleted,
                )

    def _switch_species(species_id: str):
        _active["species_id"] = species_id
        # Hide empty state if still visible
        empty = _refs.get("empty")
        if empty:
            empty.set_visibility(False)
        _ensure_species_rendered(species_id)
        for sid, c in _workbench_containers.items():
            c.set_visibility(sid == species_id)
        strip_view.refresh()

    def _observe():
        """3 s poll + on-show: reconcile the panel with the live registry. Species
        created elsewhere (roster "+", another tab) get selected; ones deleted
        elsewhere lose their container (a same-id re-create must not reuse it) and the
        panel falls back to the remainder / the empty state; the strip repaints only
        when its signature moved. The steady-state tick is an in-memory id list + a
        tuple compare, so it is fine while hidden."""
        ids = [s.id for s in get_project_state_for(project_path).species_registry]
        for sid in [s for s in _workbench_containers if s not in ids]:
            _drop_container(sid)
        if not ids:
            if _active["species_id"] is not None:  # deleted elsewhere → empty state
                _active["species_id"] = None
                _refs["empty"].set_visibility(True)
        elif _active["species_id"] not in ids:  # created elsewhere / active deleted → pick first
            _switch_species(ids[0])
        strip_view.refresh()

    async def _add_species():
        name = await _prompt_species_name()
        if not name:
            return
        state = get_project_state_for(project_path)
        species = state.add_species(name)
        (project_path / "templates" / species.id).mkdir(parents=True, exist_ok=True)
        await backend.save_project(project_path)
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

    strip_view.refresh()

    if state.species_registry:
        _switch_species(state.species_registry[0].id)

    # Observe the registry: 3 s poll (in-memory tuple only) + instant on show.
    ui.timer(3.0, _observe)

    def _set_active(on: bool) -> None:
        if on:
            _observe()

    if callbacks is not None:
        callbacks["on_workbench_active"] = _set_active
        # "open in Species" from a job's Config tab (workspace_page._open_species).
        callbacks["workbench_select_species"] = _switch_species

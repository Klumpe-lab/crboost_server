"""User settings: the switches that belong to the person running this server (`services/configs/user_settings.py`),
opened from the person icon on the start page's toolbar. The gear beside it is the server configuration.

*Start EisGreis with my remote desktop* is not stored with the others: it is the desktop's own autostart entry
(`services/curation/napari_link.py`), which the switch writes or removes, so it holds wherever crboost runs."""

from __future__ import annotations

import asyncio
import logging

from nicegui import ui

from services.configs.config_service import get_config_service
from services.configs.user_settings import UserSettings, load_user_settings, save_user_settings
from services.curation import napari_link
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.job_plugins._field_styles import HELPER_STYLE, LABEL_VOICE, PANEL_TITLE_STYLE

logger = logging.getLogger(__name__)

AGENTS_LABEL = "Allow agents to access and edit my projects"
AGENTS_HELP = (
    "Agents (the CryoBoost agent API, e.g. Claude Code) may then change and delete in the projects you own, not "
    "only in those they made: add, set, run, stop and delete jobs, species and templates. Reading is open "
    "either way: agents can read every project, yours and others'. Lab-shared projects and other people's stay "
    "out of their reach. Deleting a project or emptying a Trash always waits for your approval on the agent's "
    "plan page."
)
DESKTOP_LABEL = "Start EisGreis with my remote desktop"
DESKTOP_HELP = (
    "Every remote desktop you open on the cluster then starts EisGreis, the napari viewer, waiting with no window "
    "until you show a tomogram from its napari chip. It writes {file}; off removes it."
)


def _switch_row(label: str, help_text: str, value: bool, on_change, *, enabled: bool = True) -> None:
    with ui.row().classes("w-full items-start no-wrap").style("gap: 10px;"):
        with ui.column().classes("gap-1").style("flex: 1; min-width: 0;"):
            ui.label(label).style(LABEL_VOICE)
            ui.label(help_text).style(f"{HELPER_STYLE} line-height: 1.45;")
        switch = (
            ui.switch(value=value, on_change=on_change).props("dense").style("transform: scale(0.75); flex-shrink: 0;")
        )
        if not enabled:
            switch.disable()


async def open_user_settings() -> None:
    settings = await asyncio.to_thread(load_user_settings)
    working = settings.model_copy()
    autostart = await asyncio.to_thread(napari_link.autostart_state)
    install = get_config_service().napari.eisgreis
    desktop = {"on": autostart["on"]}

    with (
        dialog_host(),
        ui.dialog() as dialog,
        ui.card().style("width: 460px; max-width: 92vw; padding: 14px 16px; gap: 10px;"),
    ):
        ui.label("User settings").style(PANEL_TITLE_STYLE)

        def on_agents(e) -> None:
            working.agents_may_edit_my_projects = bool(e.value)

        _switch_row(AGENTS_LABEL, AGENTS_HELP, working.agents_may_edit_my_projects, on_agents)

        def on_desktop(e) -> None:
            desktop["on"] = bool(e.value)

        help_text = DESKTOP_HELP.format(file=napari_link.AUTOSTART_FILE)
        if autostart["on"] and autostart["command"]:
            help_text += f" Now it runs {autostart['command']}."
        if not install:
            help_text = "napari.eisgreis is not set in this server's conf.yaml, so there is no EisGreis to start."
        _switch_row(DESKTOP_LABEL, help_text, autostart["on"], on_desktop, enabled=bool(install) or autostart["on"])

        async def save() -> None:
            try:
                await asyncio.to_thread(save_user_settings, UserSettings.model_validate(working.model_dump()))
            except OSError as e:
                logger.exception("Could not save the user settings")
                ui.notify(f"Not saved: {e}", type="negative")
                return
            if desktop["on"] != autostart["on"]:
                res = await asyncio.to_thread(napari_link.set_autostart, desktop["on"], install)
                if not res.get("success"):
                    ui.notify(f"EisGreis with the desktop: {res.get('error')}", type="negative", multi_line=True)
                    return
            dialog.close()
            state = "may now edit your projects" if working.agents_may_edit_my_projects else "edit only their own"
            with_desktop = "; EisGreis starts with your desktop" if desktop["on"] else ""
            ui.notify(f"Saved: agents {state}{with_desktop}.", type="positive")

        with ui.row().classes("w-full justify-end").style("gap: 6px; margin-top: 4px;"):
            house_button("Cancel", dialog.close)
            house_button("Save", save, kind="accent")

    dialog.open()

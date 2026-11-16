"""User settings: the switches that belong to the person running this server (`services/configs/user_settings.py`),
opened from the person icon on the start page's toolbar. The gear beside it is the server configuration."""

from __future__ import annotations

import asyncio
import logging

from nicegui import ui

from services.configs.user_settings import UserSettings, load_user_settings, save_user_settings
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


async def open_user_settings() -> None:
    settings = await asyncio.to_thread(load_user_settings)
    working = settings.model_copy()

    with (
        dialog_host(),
        ui.dialog() as dialog,
        ui.card().style("width: 460px; max-width: 92vw; padding: 14px 16px; gap: 10px;"),
    ):
        ui.label("User settings").style(PANEL_TITLE_STYLE)
        with ui.row().classes("w-full items-start no-wrap").style("gap: 10px;"):
            with ui.column().classes("gap-1").style("flex: 1; min-width: 0;"):
                ui.label(AGENTS_LABEL).style(LABEL_VOICE)
                ui.label(AGENTS_HELP).style(f"{HELPER_STYLE} line-height: 1.45;")

            def on_toggle(e) -> None:
                working.agents_may_edit_my_projects = bool(e.value)

            ui.switch(value=working.agents_may_edit_my_projects, on_change=on_toggle).props("dense").style(
                "transform: scale(0.75); flex-shrink: 0;"
            )

        async def save() -> None:
            try:
                await asyncio.to_thread(save_user_settings, UserSettings.model_validate(working.model_dump()))
            except OSError as e:
                logger.exception("Could not save the user settings")
                ui.notify(f"Not saved: {e}", type="negative")
                return
            dialog.close()
            state = "may now edit your projects" if working.agents_may_edit_my_projects else "edit only their own"
            ui.notify(f"Saved: agents {state}.", type="positive")

        with ui.row().classes("w-full justify-end").style("gap: 6px; margin-top: 4px;"):
            house_button("Cancel", dialog.close)
            house_button("Save", save, kind="accent")

    dialog.open()

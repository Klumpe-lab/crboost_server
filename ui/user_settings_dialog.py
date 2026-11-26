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
from ui.components.buttons import house_button, icon_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_select
from ui.dashboard.theme import PAPERS, THEMES, switch_theme, theme_of
from ui.job_plugins._field_styles import HELPER_STYLE, LABEL_VOICE, PANEL_TITLE_STYLE, SANS

logger = logging.getLogger(__name__)

AGENTS_LABEL = "Allow agents to access and edit my projects"
AGENTS_HELP = (
    "Agents (the CryoBoost agent API, e.g. Claude Code) may then change and delete in the projects you own, not "
    "only in those they made: add, set, run, stop and delete jobs, species and templates. Reading is open "
    "either way: agents can read every project, yours and others'. Lab-shared projects and other people's stay "
    "out of their reach. Deleting a project or emptying a Trash always waits for your approval on the agent's "
    "plan page."
)
THEME_HELP = (
    "How every page looks: Light, Dark, or a light look on a softer ground than white (Mist, Sage, Linen; side by "
    "side on /dev/ideas). Shown at once; Save keeps it. A page's address can try one for itself: ?theme=mist."
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

        # The theme is shown as it is picked; closing without Save puts the page back in the one it had.
        shown = {"theme": theme_of(getattr(ui.context.client, "_cb_theme", working.theme)).name, "saved": False}

        def on_hide() -> None:
            if not shown["saved"] and working.theme != shown["theme"]:
                switch_theme(shown["theme"])

        dialog.on("hide", on_hide)

        def on_theme(e) -> None:
            working.theme = theme_of(e.value).name
            switch_theme(working.theme)

        with ui.column().classes("w-full gap-1"):
            house_select(
                "Theme",
                {n: t.label for n, t in THEMES.items()},
                value=theme_of(working.theme).name,
                on_change=on_theme,
                width="w-32",
            )
            ui.label(THEME_HELP).style(f"{HELPER_STYLE} line-height: 1.45;")

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
            shown["saved"] = True
            dialog.close()
            state = "may now edit your projects" if working.agents_may_edit_my_projects else "edit only their own"
            with_desktop = "; EisGreis starts with your desktop" if desktop["on"] else ""
            ui.notify(f"Saved: agents {state}{with_desktop}.", type="positive")

        with ui.row().classes("w-full justify-end").style("gap: 6px; margin-top: 4px;"):
            house_button("Cancel", dialog.close)
            house_button("Save", save, kind="accent")

    dialog.open()


async def choose_theme(name: str) -> None:
    """Put the page in a theme now and keep it as the user's."""
    settings = await asyncio.to_thread(load_user_settings)
    settings.theme = theme_of(name).name
    try:
        await asyncio.to_thread(save_user_settings, settings)
    except OSError as e:
        logger.exception("Could not save the theme")
        ui.notify(f"Not saved: {e}", type="negative")
        return
    switch_theme(settings.theme)


def theme_menu_button() -> None:
    """The half-moon: Light and Dark, the papers under them; the page's theme marked."""
    items: dict[str, ui.menu_item] = {}

    def mark(now: str) -> None:
        for n, item in items.items():
            ink = "font-weight: 600; color: var(--cb-ink-1, #12324f)" if n == now else "color: var(--cb-ink-2, #1e293b)"
            item.style(replace=f"{SANS} font-size: 11px; min-height: 26px; padding: 0 14px; {ink};")

    async def pick(name: str) -> None:
        mark(name)
        await choose_theme(name)

    with icon_button("contrast"):
        # Its tip to its left: below, it would lie over the open menu.
        ui.tooltip("Theme").props("anchor='center left' self='center right'")
        menu = ui.menu().props("auto-close anchor='bottom right' self='top right'")
    with menu:
        for group in (("light", "dark"), PAPERS):
            if group is PAPERS:
                ui.separator()
            for name in group:
                items[name] = ui.menu_item(THEMES[name].label, lambda n=name: pick(n))
    mark(theme_of(getattr(ui.context.client, "_cb_theme", None)).name)

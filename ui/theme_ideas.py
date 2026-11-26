"""`/dev/ideas`: the themes side by side, and moving grounds to keep around.

The themes: each board is a small workspace built from the app's own parts (the rail, the job roster, a job page
with its Parameters, Results, a picture, stat tiles and the log well) under one theme. The board carries
`data-cb-theme`, so the theme's values apply to what is inside it and nowhere else. A board's links open the real
app in its theme for that page only (`?theme=<name>`); Use makes it the user's theme.

The moving grounds (`ui/dashboard/backdrops.py`) are ideas: each on a desk with a card over it, under any theme, at
its own pace or eight times faster, and full screen.
"""

from __future__ import annotations

import asyncio

from nicegui import ui

from services.configs.user_prefs_service import get_prefs_service
from services.configs.user_settings import load_user_settings, save_user_settings
from services.models_base import JobStatus
from ui.components.buttons import house_button, icon_button
from ui.components.check import house_check
from ui.components.segmented import render_segmented
from ui.dashboard.backdrops import Backdrop, backdrop, backdrop_css, backdrops
from ui.dashboard.theme import THEMES, Theme, theme_block
from ui.job_plugins._field_styles import (
    FIELD_LABEL_STYLE,
    FIELD_ROW_STYLE,
    MONO,
    PANEL_TITLE_STYLE,
    SANS,
    UNIT_STYLE,
    VIEW_HEAD_STYLE,
    VIEW_TITLE_STYLE,
    box_input,
    group_title,
    num_html,
    section,
    view_head_icon,
)
from ui.status_indicator import _badge_html, _dot_html

# A micrograph's noise for the picture tiles: grey fractal noise on the image pane's dark ground.
_NOISE = (
    "url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='200' height='120'>"
    "<filter id='n'><feTurbulence type='fractalNoise' baseFrequency='0.75' numOctaves='3' seed='7'/>"
    "<feColorMatrix type='saturate' values='0'/><feComponentTransfer><feFuncA type='linear' slope='0.9'/>"
    "</feComponentTransfer></filter><rect width='100%' height='100%' filter='url(%23n)'/></svg>\")"
)

_CARD = (
    "background: var(--cb-surface, #ffffff); "
    "border: 1px solid var(--cb-panel-line, #d6e0ea); border-radius: var(--cb-r-lg, 3px); "
    "display: flex; flex-direction: column; min-width: 0; overflow: hidden;"
)
_BAND = (
    "font-family: var(--cb-label-font); font-size: var(--cb-label-size); text-transform: var(--cb-label-case); "
    "letter-spacing: var(--cb-label-track); color: var(--cb-label-ink); padding: 6px 8px 4px; "
    "background: var(--cb-head); border-bottom: 1px solid var(--cb-line);"
)
_TAG = (
    "font-family: var(--cb-tag-font); font-size: var(--cb-tag-size); font-weight: var(--cb-tag-weight); "
    "text-transform: var(--cb-tag-case); letter-spacing: var(--cb-tag-track); color: var(--cb-tag-ink); "
    "background: var(--cb-tag); border: 1px solid var(--cb-tag-line); border-radius: var(--cb-tag-radius); "
    "padding: 0 4px; line-height: 13px; white-space: nowrap;"
)
_ROWS = (
    ("Import", JobStatus.SUCCEEDED, "job001", "12/12", "copia"),
    ("Motion & CTF", JobStatus.SUCCEEDED, "job002", "12/12", "copia"),
    ("Alignment", JobStatus.SUCCEEDED, "job004", "12/12", "copia"),
    ("Reconstruct", JobStatus.RUNNING, "job024", "7/12", "copia"),
    ("Denoise Train", JobStatus.QUEUED, "job025", "", "copia"),
    ("Template Match", JobStatus.FAILED, "job026", "3/12", "capsid"),
    ("Pick candidates", JobStatus.UNKNOWN, "", "", ""),
)


def _roster() -> None:
    with ui.element("div").style(_CARD + " width: 210px; flex-shrink: 0;"):
        with ui.element("div").style(VIEW_HEAD_STYLE):
            view_head_icon("layers")
            ui.label("Jobs").style(VIEW_TITLE_STYLE)
        ui.label("Preprocessing").style(_BAND)
        for i, (name, status, job, count, tag) in enumerate(_ROWS):
            on = name == "Reconstruct"
            row_bg = "background: var(--cb-rail-on, #dbe7f3);" if on else ""
            tick = "box-shadow: inset 2px 0 0 var(--cb-accent, #4a8fd4);" if on else ""
            with ui.element("div").style(
                "display: flex; align-items: center; gap: 6px; padding: 0 8px; height: 22px; "
                f"border-bottom: 1px solid var(--cb-line, #e4eaf1); {row_bg}{tick}"
            ):
                ui.html(_dot_html(status, size_px=7), sanitize=True)
                ink = "var(--cb-ink-4, #94a3b8)" if not job else "var(--cb-ink-2, #1e293b)"
                ui.label(name).style(f"{SANS} font-size: 10.5px; color: {ink}; white-space: nowrap;")
                ui.label(job).style(f"{MONO} font-size: 8.5px; color: var(--cb-ink-4, #94a3b8);")
                ui.element("div").style("flex: 1;")
                if count:
                    ok = "var(--cb-ok-ink, #0f6b4c)" if status == JobStatus.SUCCEEDED else "var(--cb-ink-3, #5b7089)"
                    ui.label(count).style(f"{MONO} font-size: 8.5px; color: {ok};")
                if tag:
                    ui.label(tag).style(_TAG)
            if i == 2:
                ui.label("Particles").style(_BAND)


def _field(label: str, value: str, unit: str = "", width: str = "S") -> None:
    with ui.element("div").style(FIELD_ROW_STYLE):
        ui.label(label).style(FIELD_LABEL_STYLE)
        box_input(ui.input(value=value), width)
        ui.label(unit).style(UNIT_STYLE)


def _picture(caption: str) -> None:
    with ui.element("div").style("display: flex; flex-direction: column; gap: 2px;"):
        ui.element("div").style(
            f"width: 96px; height: 58px; background: var(--cb-pane) {_NOISE}; background-size: cover; "
            "border-radius: var(--cb-img-radius, 1px); box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.18);"
        )
        ui.label(caption).style(f"{MONO} font-size: 8.5px; color: var(--cb-ink-3, #5b7089);")


def _stat(label: str, value: str) -> None:
    with ui.element("div").classes("cb-stat"):
        ui.label(label).classes("cb-stat-label")
        ui.html(f'<span class="cb-stat-value">{num_html(value)}</span>', sanitize=True)


def _job_page() -> None:
    with ui.element("div").style(_CARD + " flex: 1 1 0;"):
        with ui.element("div").style(VIEW_HEAD_STYLE + " box-shadow: inset 2px 0 0 var(--cb-accent, #4a8fd4);"):
            view_head_icon("view_in_ar")
            ui.label("Reconstruct").style(VIEW_TITLE_STYLE)
            ui.label("job024").style(f"{MONO} font-size: 9px; color: var(--cb-ink-4, #94a3b8);")
            ui.element("div").style("flex: 1;")
            ui.html(_badge_html(JobStatus.RUNNING), sanitize=True)
        with ui.element("div").style("display: flex; gap: 16px; padding: 10px 12px; min-height: 0;"):
            with ui.element("div").style(
                "flex: 1 1 0; min-width: 0; display: flex; flex-direction: column; gap: 10px;"
            ):
                with section("Results", right="7 of 12", accent=True):
                    with ui.element("div").style("display: flex; gap: 6px; flex-wrap: wrap;"):
                        _picture("TS_01 · 1.55 Å")
                        _picture("TS_02 · 1.55 Å")
                    with ui.element("div").classes("cb-stats").style("margin: 2px 0 0;"):
                        _stat("Thickness", "182.4 nm")
                        _stat("Pixel", "6.2 Å")
                        _stat("Done", "7/12")
                with section("Log"):
                    ui.html(
                        "reconstruct TS_07 (bin 4)\nwrote TS_07_6.20Apx.mrc\n"
                        '<span class="cb-log-warn">TS_09: 3 tilts excluded</span>\n'
                        '<span class="cb-log-note">7 of 12 written</span>',
                        sanitize=False,
                    ).classes("cb-log-well").style("max-height: 84px;")
            with ui.element("div").style("width: 222px; flex-shrink: 0; display: flex; flex-direction: column;"):
                with section("Parameters"):
                    group_title("Geometry")
                    _field("Binning", "4", "×")
                    _field("Thickness", "300", "nm")
                    _field("Pixel size", "6.2", "Å")
                    group_title("Output")
                    with ui.element("div").style("display: flex; gap: 12px; padding: 2px 0 6px;"):
                        house_check("Halves", value=True)
                        house_check("Deconvolve", value=False)
                    with ui.element("div").style("display: flex; gap: 6px;"):
                        house_button("Run", kind="accent")
                        house_button("Clone")
                        house_button("Delete", kind="danger")


def _rail() -> None:
    with ui.element("div").style(
        "width: 34px; flex-shrink: 0; display: flex; flex-direction: column; align-items: center; gap: 4px; "
        "padding-top: 6px; background: var(--cb-rail, #eef3f8); border-right: 1px solid var(--cb-rail-line, #d3dde8);"
    ):
        for icon in ("layers", "view_in_ar", "grid_view", "route"):
            icon_button(icon)


def _swatches(t: Theme) -> None:
    for key, name in (("--cb-desk", "desk"), ("--cb-surface", "card"), ("--cb-line", "rule"), ("--cb-ink-2", "ink")):
        with ui.element("div").style("display: flex; align-items: center; gap: 3px;"):
            ui.element("div").style(
                f"width: 10px; height: 10px; background: var({key}); box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.15);"
            )
            ui.label(name).style(f"{MONO} font-size: 8.5px; color: var(--cb-ink-3, #5b7089);")


def _board(t: Theme, project: str | None, on_use) -> None:
    with (
        ui.element("div")
        .props(f'data-cb-theme="{t.name}"')
        .style(
            "display: flex; flex-direction: column; gap: 0; border-radius: var(--cb-r-lg, 3px); overflow: hidden; "
            "background: var(--cb-desk, #e9eef4); background-image: var(--cb-desk-art, none); "
            "background-size: var(--cb-desk-art-size, auto); box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.12);"
        )
    ):
        with ui.element("div").style("display: flex; align-items: center; gap: 10px; padding: 8px 10px 0;"):
            ui.label(t.label).style(PANEL_TITLE_STYLE + " color: var(--cb-ink-1, #12324f);")
            ui.label(t.note).style(f"{SANS} font-size: 10px; color: var(--cb-ink-3, #5b7089); flex: 1; min-width: 0;")
            _swatches(t)
        with ui.element("div").style("display: flex; align-items: center; gap: 6px; padding: 6px 10px 0;"):
            links = [("Projects", f"/?theme={t.name}")]
            if project:
                links.append((project, f"/p/{project}?theme={t.name}"))
            for label, href in links:
                ui.link(f"open {label} ↗", href, new_tab=True).style(
                    f"{MONO} font-size: 9px; color: var(--cb-head-accent, #2f6fb3);"
                )
            ui.element("div").style("flex: 1;")
            house_button("Use", lambda n=t.name: on_use(n))
        with ui.element("div").style("display: flex; gap: 8px; padding: 8px 10px 10px; height: 330px;"):
            _rail()
            _roster()
            _job_page()


# A moving ground's stage: the desk with its dot grid; full screen on a click of its corner button.
_STAGE_CSS = """
.cb-bd-stage { position: relative; height: 270px; overflow: hidden; border-radius: var(--cb-r-lg, 3px);
    background: var(--cb-desk); background-image: var(--cb-desk-art); background-size: var(--cb-desk-art-size);
    box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.12); }
.cb-bd-stage.cb-bd-full { position: fixed; inset: 0; height: auto; z-index: 6000; border-radius: 0; }
/* The card's place is inline (it wins over a class), so full screen overrides it. */
.cb-bd-stage.cb-bd-full .cb-bd-card { width: 360px !important; bottom: auto !important; top: 24px !important;
    right: 24px !important; }
"""


def _mini_card() -> None:
    with (
        ui.element("div")
        .classes("cb-bd-card")
        .style(_CARD + " position: absolute; z-index: 1; top: 14px; right: 14px; bottom: 14px; width: 38%;")
    ):
        with ui.element("div").style(VIEW_HEAD_STYLE):
            view_head_icon("layers")
            ui.label("Jobs").style(VIEW_TITLE_STYLE)
        for name, status, job, _count, _tag in _ROWS[:6]:
            with ui.element("div").style(
                "display: flex; align-items: center; gap: 6px; padding: 0 8px; height: 22px; "
                "border-bottom: 1px solid var(--cb-line, #e4eaf1);"
            ):
                ui.html(_dot_html(status, size_px=7), sanitize=True)
                ui.label(name).style(f"{SANS} font-size: 10.5px; color: var(--cb-ink-2, #1e293b); white-space: nowrap;")
                ui.element("div").style("flex: 1;")
                ui.label(job).style(f"{MONO} font-size: 8.5px; color: var(--cb-ink-4, #94a3b8);")


def _ground(b: Backdrop) -> None:
    with ui.element("div").style("display: flex; flex-direction: column; gap: 5px; min-width: 0;"):
        with ui.element("div").style("display: flex; align-items: baseline; gap: 8px;"):
            ui.label(b.label).style(PANEL_TITLE_STYLE + " color: var(--cb-ink-1, #12324f); white-space: nowrap;")
            ui.label(b.note).style(f"{SANS} font-size: 10px; color: var(--cb-ink-3, #5b7089); min-width: 0;")
        with ui.element("div").classes("cb-bd-stage") as stage:
            backdrop(b.name)
            _mini_card()
            with ui.element("div").style("position: absolute; z-index: 2; left: 10px; bottom: 10px;"):
                icon_button(
                    "open_in_full", lambda: stage.classes(toggle="cb-bd-full"), tooltip="Full screen (again: back)"
                )


def _grounds() -> None:
    segs = {}

    def on_theme(name: str) -> None:
        stages.props(f'data-cb-theme="{name}"')
        segs["theme"].set_active(name)

    def on_speed(k: str) -> None:
        stages.style(f"--cb-bd-speed: {k};")
        segs["speed"].set_active(k)

    with ui.element("div").style("display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-top: 10px;"):
        ui.label("Moving grounds").style(PANEL_TITLE_STYLE + " font-size: 13px;")
        ui.label("under").style(f"{SANS} font-size: 10.5px; color: var(--cb-ink-3);")
        segs["theme"] = render_segmented([(n, t.label) for n, t in THEMES.items()], "light", on_theme)
        ui.label("pace").style(f"{SANS} font-size: 10.5px; color: var(--cb-ink-3);")
        segs["speed"] = render_segmented([("1", "its own"), ("8", "8× faster")], "1", on_speed)
    ui.label(
        "Slow backgrounds the browser draws, to keep around for the desk, an empty view or a page that waits. Only "
        "transforms and opacity move (no repaint, nothing from the server); the ink is the theme's, so each follows "
        "it. A system set to reduce motion stops them."
    ).style(f"{SANS} font-size: 10.5px; color: var(--cb-ink-3); max-width: 900px;")
    with (
        ui.element("div")
        .props('data-cb-theme="light"')
        .style(
            "display: grid; grid-template-columns: repeat(auto-fill, minmax(460px, 1fr)); gap: 16px 14px; "
            "padding: 10px; border-radius: var(--cb-r-lg, 3px); background: var(--cb-desk);"
        ) as stages
    ):
        for b in backdrops().values():
            _ground(b)


def render_theme_ideas() -> None:
    recent = get_prefs_service().prefs.recent_projects
    project = recent[0].path.rstrip("/").rsplit("/", 1)[-1] if recent else None
    ui.add_head_html(f"<style>{''.join(theme_block(n) for n in THEMES)}{backdrop_css()}{_STAGE_CSS}</style>")

    async def use(name: str) -> None:
        settings = await asyncio.to_thread(load_user_settings)
        settings.theme = name
        try:
            await asyncio.to_thread(save_user_settings, settings)
        except OSError as e:
            ui.notify(f"Not saved: {e}", type="negative")
            return
        ui.notify(f"Pages now open in {THEMES[name].label}.", type="positive")

    with ui.element("div").style(
        "position: fixed; inset: 0; overflow-y: auto; background: var(--cb-desk); "
        "background-image: var(--cb-desk-art); background-size: var(--cb-desk-art-size); padding: 18px 22px 40px;"
    ):
        with ui.element("div").style(
            "max-width: 1560px; margin: 0 auto; display: flex; flex-direction: column; gap: 14px;"
        ):
            with ui.element("div").style("display: flex; flex-direction: column; gap: 3px;"):
                ui.label("Less white: themes and moving grounds").style(PANEL_TITLE_STYLE + " font-size: 14px;")
                ui.label(
                    "The app's own parts under each theme, every desk with the dot grid. 'open … ↗' shows the real "
                    "app in a theme for that page only; Use makes it the theme every page opens in (also the "
                    "half-moon on the start page, and User settings)."
                ).style(f"{SANS} font-size: 10.5px; color: var(--cb-ink-3); max-width: 900px;")
            with ui.element("div").style(
                "display: grid; grid-template-columns: repeat(auto-fill, minmax(700px, 1fr)); gap: 14px;"
            ):
                for t in THEMES.values():
                    _board(t, project, use)
            _grounds()

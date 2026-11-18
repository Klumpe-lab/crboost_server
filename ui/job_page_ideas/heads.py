"""Header ladders, long paths and checkboxes for the job-page prototypes.

A ladder is the set of headers one look draws, from the most to the least important:

    page     the job's own title line
    section  Parameters, Inputs & Outputs, SLURM resources, Tasks, Files
    accent   a section that asks to be looked at (a failed job's Log, Results)
    group    a group inside a section: Angles & offsets, Reference & mask

today  Harbor Mono now: the pale blue-grey band, the 2 px accent on its left, the hairline under it.
rule   Oxide's console: mono caps title, a hairline running on from it to the right edge; no fill, no corners.
tab    the title in a flat inverted tab sitting on a full-width ink rule.
band   a flat band, square, ruled in ink along its top instead of on its left.

The look is picked per panel (`use`), so one page can show every ladder side by side.
"""

from __future__ import annotations

import html
from contextlib import contextmanager
from pathlib import Path

from nicegui import ui

from ui.job_plugins._field_styles import PANEL_HEAD_STYLE, PANEL_STYLE, PANEL_TITLE_STYLE

MONO = "font-family: var(--cb-mono, 'IBM Plex Mono', monospace);"
SANS = "font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif);"
INK_1 = "#12324f"  # --cb-ink-1: titles
INK_2 = "#1e293b"
INK_3 = "#5b7089"
INK_4 = "#94a3b8"
RULE = "#d6e0ea"  # --cb-panel-line
ACCENT = "#2f6fb3"  # one step darker than --cb-pick, so white text on it reads
ACCENT_SOFT = "#a9c6e4"
CAPS = "text-transform: uppercase; letter-spacing: .09em;"

LOOKS = ("today", "rule", "tab", "band")
_look = {"name": "today"}


def use(name: str) -> None:
    _look["name"] = name if name in LOOKS else "today"


def look() -> str:
    return _look["name"]


def _meta(text: str) -> None:
    if text:
        ui.label(text).style(f"{MONO} font-size: 8.5px; {CAPS} color: {INK_4}; margin-left: auto; white-space: nowrap;")


def _head(title: str, right: str, accent: bool) -> None:
    name = look()
    if name == "rule":
        with ui.element("div").style("display: flex; align-items: center; gap: 8px; min-height: 18px;"):
            if accent:
                ui.element("div").style(f"width: 6px; height: 6px; background: {ACCENT}; flex-shrink: 0;")
            ui.label(title).style(
                f"{MONO} font-size: 9.5px; font-weight: 600; {CAPS} color: {ACCENT if accent else INK_1}; "
                "white-space: nowrap;"
            )
            ui.element("div").style(f"flex: 1; height: 1px; background: {ACCENT_SOFT if accent else RULE};")
            if right:
                ui.label(right).style(f"{MONO} font-size: 8.5px; {CAPS} color: {INK_4}; white-space: nowrap;")
        return
    if name == "tab":
        ink = ACCENT if accent else INK_1
        with ui.element("div").style(
            f"display: flex; align-items: flex-end; gap: 8px; border-bottom: 1px solid {ink}; min-height: 18px;"
        ):
            ui.label(title).style(
                f"{MONO} font-size: 8.5px; font-weight: 500; {CAPS} color: #fff; background: {ink}; "
                "padding: 3px 8px 2px; line-height: 11px; white-space: nowrap;"
            )
            if right:
                ui.label(right).style(
                    f"{MONO} font-size: 8.5px; {CAPS} color: {INK_4}; margin-left: auto; padding-bottom: 2px; "
                    "white-space: nowrap;"
                )
        return
    if name == "band":
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 8px; padding: 4px 8px; min-height: 22px; "
            "box-sizing: border-box; "
            f"background: {'#eef4fb' if accent else '#f5f8fb'}; "
            f"border-top: {'2px solid ' + ACCENT if accent else '1px solid ' + INK_1};"
        ):
            ui.label(title).style(
                f"{MONO} font-size: 9px; font-weight: 600; {CAPS} color: {ACCENT if accent else INK_1}; "
                "white-space: nowrap;"
            )
            _meta(right)
        return
    # today
    accent_css = f"box-shadow: 0 1px 0 #dbe3ec, inset 2px 0 0 {'#d97706' if accent else '#4a8fd4'};"
    head_css = f"{PANEL_HEAD_STYLE} {accent_css if accent else ''} display: flex; align-items: center;"
    with ui.element("div").style(head_css):
        ui.label(title).style(PANEL_TITLE_STYLE)
        if right:
            ui.label(right).style(f"{SANS} font-size: 9.5px; color: {INK_4}; margin-left: auto;")


@contextmanager
def section(title: str, *, right: str = "", accent: bool = False, extra: str = ""):
    """A section: its header, then its content, one box deep."""
    if look() == "today":
        box = ui.column().classes("w-full cbjp-sec").style(f"gap: 6px; {PANEL_STYLE} {extra}")
    else:
        box = ui.column().classes("w-full cbjp-sec").style(f"gap: 7px; {extra}")
    with box:
        _head(title, right, accent)
        yield box


def group_title(text: str) -> None:
    """The header of a group inside a section: one step under the section's, never a box."""
    name = look()
    if name == "rule":
        ui.label(text).style(f"{SANS} font-size: 10.5px; font-weight: 600; color: {INK_2}; margin: 3px 0 2px;")
    elif name == "tab":
        with ui.element("div").style("display: flex; align-items: center; gap: 6px; margin: 3px 0 2px;"):
            ui.element("div").style(f"width: 5px; height: 5px; border: 1px solid {INK_1}; flex-shrink: 0;")
            ui.label(text).style(f"{MONO} font-size: 8.5px; font-weight: 600; {CAPS} color: {INK_1};")
    elif name == "band":
        ui.label(text).style(
            f"{SANS} font-size: 10.5px; font-weight: 600; color: {INK_1}; margin: 3px 0 2px; padding-bottom: 2px; "
            f"border-bottom: 1px dotted {RULE};"
        )
    else:
        ui.label(text).style(f"{SANS} font-size: 10.5px; font-weight: 600; color: #0f172a; margin: 2px 0 3px;")


def page_title(text: str, meta: str = "") -> None:
    """The specimen's page level: the job's own title line, as large in every look."""
    with ui.element("div").style("display: flex; align-items: baseline; gap: 8px;"):
        ui.label(text).style(f"{SANS} font-size: 13px; font-weight: 600; color: {INK_1};")
        if meta:
            ui.label(meta).style(f"{MONO} font-size: 9.5px; color: {INK_4};")


# ── long paths ───────────────────────────────────────────────────────────────

PATH_CSS = """
.cbjp-path { cursor: pointer; }
.cbjp-path.open { display: block !important; }
.cbjp-path.open > span { white-space: normal !important; overflow: visible !important; }
"""
_PATH_BOX = (
    "display: flex; min-width: 0; font-family: var(--cb-mono, 'IBM Plex Mono', monospace); font-size: 9.5px; "
    "line-height: 14px;"
)
# The folder gives way a thousand times faster than the name, so the name is cut only once the folder is gone.
_DIR_CUT = (
    "color: #5b7089; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0; flex: 0 1000 auto;"
)
_NAME_CUT = (
    "color: #1e293b; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0; flex: 0 1 auto;"
)
_DIR_WRAP = "color: #5b7089;"
_NAME_WRAP = "color: #1e293b;"


def split(path: str, project: Path | None) -> tuple[str, str]:
    """(folder, name) of `path`, the folder relative to the project when the path is inside it."""
    p = Path(path)
    if project is not None:
        try:
            p = p.relative_to(project)
        except ValueError:
            pass
    folder = str(p.parent)
    return ("" if folder == "." else folder + "/"), p.name


def path(path_: str, project: Path | None, mode: str) -> None:
    """A path on one line or more, its file name cut last.

    cut   one line; the folder gives way (ellipsis at its end) before the file name does.
    wrap  every part shown, wrapping only after a slash.
    open  `cut`, and a click opens it to `wrap` in place (and closes it again).
    """
    folder, name = split(path_, project)
    # A zero-width space after each slash: a break there when the path wraps, nothing while it is one line.
    folder_html = html.escape(folder).replace("/", "/&#8203;")
    name_html = html.escape(name)
    if mode == "wrap":
        el = ui.html(
            f"<span style='{_DIR_WRAP}'>{folder_html}</span><span style='{_NAME_WRAP}'>{name_html}</span>",
            sanitize=False,
        ).style(_PATH_BOX.replace("display: flex;", "display: block;") + " overflow-wrap: anywhere;")
    else:
        el = ui.html(
            f"<span style='{_DIR_CUT}'>{folder_html}</span><span style='{_NAME_CUT}'>{name_html}</span>", sanitize=False
        ).style(_PATH_BOX)
    el.classes("cbjp-path").props(f'title="{html.escape(path_)}"')
    if mode == "open":
        el.on("click", js_handler="(e) => e.currentTarget.classList.toggle('open')")


# ── checkboxes ───────────────────────────────────────────────────────────────

CHECK_CSS = """
.cbjp-hcheck { display: inline-flex; align-items: baseline; gap: 6px; cursor: pointer; user-select: none; }
.cbjp-hcheck .b { position: relative; width: 10px; height: 10px; box-sizing: border-box; border: 1px solid #5b7089;
  border-radius: 1px; flex-shrink: 0; display: inline-block; }
/* The tick is out of flow, so the box holds no text and its baseline is its bottom edge. */
.cbjp-hcheck.on .b { background: #334155; border-color: #334155; }
.cbjp-hcheck.on .b::before { content: '✓'; position: absolute; inset: 0; color: #fff; font-size: 8px; line-height: 8px;
  text-align: center; }
.cbjp-hcheck .t { font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif); font-size: 10px; line-height: 14px;
  color: #334155; }
/* The F panel's guide: a red line along the bottom of each box, out under its label. */
.cbjp-guide .q-checkbox__bg::after, .cbjp-guide .b::after { content: ''; position: absolute; left: -2px;
  bottom: -1px; width: 150px; border-top: 1px solid rgba(220, 38, 38, 0.6); }
"""


def house_check(label: str, on: bool) -> None:
    """A checkbox drawn by us: a 10 px box whose bottom edge is its label's baseline (a box with no text has its
    bottom as its baseline, and the row aligns baselines), so the text sits on the box's line, not above it."""
    el = ui.html(f"<span class='b'></span><span class='t'>{label}</span>", sanitize=False).classes(
        f"cbjp-hcheck {'on' if on else ''}"
    )
    el.on("click", js_handler="(e) => e.currentTarget.classList.toggle('on')")

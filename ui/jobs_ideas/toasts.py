"""Toasts in the app's own chrome: the popup card's edge, the title stripe, the square status pip, the label voice.
Each is a class over ui.notify (look.py), so one rule restyles every toast the app raises; the kind (its word and
its pip) comes from the type a call already passes. Every variant is shown at rest and can be fired for real."""

from __future__ import annotations

import html

from nicegui import ui

from ui.components.buttons import house_button
from ui.jobs_ideas.look import MONO, SANS, SOFT

_PATH = "/groups/<lab>/_crboost_data_api/<project>/External/job008"

VARIANTS = [
    (
        "t1",
        "T1 · Card",
        "The popup card's chrome: a title stripe with the pip and the kind, the message under it. Bottom right.",
    ),
    ("t2", "T2 · Line", "One row, as tall as a roster row: pip, kind, message. Bottom right."),
    ("t3", "T3 · Status bar", "The line across the foot of the window, in the title-stripe tint; nothing floats."),
]
# (Quasar type, extra class, message): what a call site raises today.
KINDS = [
    ("positive", "cbji-k-copied", _PATH),
    ("info", "", "3 jobs queued: Reconstruct 008, Template Match 014, Template Match 015."),
    ("warning", "", "This project has no job 'tsCtf__4'."),
    ("negative", "", "Submit failed: sbatch: error: invalid partition specified: gpu_l40s"),
]
_TYPE_CLASS = {"positive": "bg-positive", "info": "bg-info", "warning": "bg-warning", "negative": "bg-negative"}

# The copy that says so on the path itself (T0): the path flashes, a COPIED tag sits at its right end.
_COPIED_JS = (
    "(e) => { const t = e.currentTarget; const r = t.getBoundingClientRect(); "
    "const s = document.createElement('span'); s.className = 'cbji-copied'; s.textContent = 'copied'; "
    "s.style.left = Math.min(r.right + 6, window.innerWidth - 60) + 'px'; s.style.top = (r.top + r.height / 2 - 7) + "
    "'px'; "
    "document.body.appendChild(s); setTimeout(() => s.remove(), 1150); "
    "t.classList.remove('cbji-copied-flash'); void t.offsetWidth; t.classList.add('cbji-copied-flash'); emit(); }"
)


def _fire(variant: str, qtype: str, extra: str, message: str) -> None:
    ui.notify(
        message,
        type=qtype,
        position="bottom" if variant == "t3" else "bottom-right",
        classes=f"cbji-t cbji-{variant} {extra}".strip(),
        timeout=3000,
    )


def _static(variant: str, qtype: str, extra: str, message: str) -> str:
    """The toast at rest: Quasar's own markup and classes (with the white text class it adds), so the same rules
    style it."""
    return (
        f'<div class="q-notification row items-stretch q-notification--standard {_TYPE_CLASS[qtype]} text-white '
        f'cbji-t cbji-{variant} {extra}" style="margin:0;position:static;">'
        '<div class="q-notification__wrapper col relative-position border-radius-inherit column no-wrap">'
        '<div class="q-notification__content row items-center col">'
        '<i class="q-icon notranslate material-icons q-notification__icon" aria-hidden="true">info</i>'
        f'<div class="q-notification__message col">{html.escape(message)}</div></div></div></div>'
    )


def render_toasts() -> None:
    with ui.element("div").style("display: flex; flex-direction: column; gap: 22px;"):
        with ui.element("div").style("display: flex; flex-direction: column; gap: 4px;"):
            ui.label("T0 · Copied, in place").style(
                f"{MONO} font-size: 9px; text-transform: uppercase; letter-spacing: .07em; color: #12324f;"
            )
            ui.label(
                "For copies only, with any of T1–T3 for the rest: the path flashes and a COPIED tag sits at its end "
                "for a second. Click the path."
            ).style(f"{SANS} font-size: 10px; color: {SOFT};")
            p = ui.label(_PATH).style(
                f"{MONO} font-size: 9.5px; color: #5b7089; cursor: pointer; align-self: flex-start; padding: 1px 3px;"
            )
            p.props["title"] = "Click to copy"
            p.on("click", lambda: ui.clipboard.write(_PATH), js_handler=_COPIED_JS)
        for key, name, words in VARIANTS:
            with ui.element("div").style("display: flex; flex-direction: column; gap: 6px;"):
                ui.label(name).style(
                    f"{MONO} font-size: 9px; text-transform: uppercase; letter-spacing: .07em; color: #12324f;"
                )
                ui.label(words).style(f"{SANS} font-size: 10px; color: {SOFT};")
                width = "100%" if key == "t3" else "auto"
                with ui.element("div").style(
                    "display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-start; "
                    f"{'flex-direction: column;' if key == 't3' else ''}"
                ):
                    for qtype, extra, message in KINDS:
                        ui.html(_static(key, qtype, extra, message), sanitize=False).style(f"width: {width};")
                with ui.element("div").style("display: flex; gap: 6px;"):
                    for qtype, extra, message in KINDS:
                        word = {"positive": "a copy", "info": "a note", "warning": "a warning", "negative": "an error"}
                        house_button(
                            f"Fire {word[qtype]}", lambda k=key, q=qtype, x=extra, m=message: _fire(k, q, x, m)
                        )

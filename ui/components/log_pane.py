"""Log views: one look for every log in the app.

A log is a dark slate well (`.cb-log-well`, ui/dashboard/css.py) of wrapped 10 px mono, set
apart from the light UI around it; lines that read as errors are red, warnings amber, and the
app's own notes (a truncated head, a file not written yet) dim italic. `log_pane` is a static
one with a header (stream, file name, copy-the-whole-file); `LiveLog` is a polled one. Both take
text, so anything that has a file can show it — an array task's task_N.out, a protocol stage's
run.out / run.err / run_submit.script.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

from nicegui import ui

from services.array_tasks import escape_html
from ui.styles import MONO

# Clipboard payload cap: a runaway log can be hundreds of MB, and the text travels
# over the socket. Keep the tail, say so at the top.
_COPY_CAP_BYTES = 8 * 2**20

_ERR_LINE = re.compile(
    r"\b(error|exception|traceback|fatal|failed|failure|killed|oom|segmentation fault|cancelled)\b", re.I
)
_WARN_LINE = re.compile(r"\bwarn(ing)?\b", re.I)
# What the log readers put in place of a log: not the tool's output, so it reads as a note.
_NOTE_LINE = re.compile(r"^(\[\.\.\. truncated .*\]|run\.(out|err) not found\.|no (output|errors) yet)$", re.I)


def log_html(text: str) -> str:
    """The log as the well's HTML: escaped, one span per line that needs a color."""
    out = []
    for line in text.split("\n"):
        safe = escape_html(line)
        if _NOTE_LINE.match(line.strip()):
            out.append(f'<span class="cb-log-note">{safe}</span>')
        elif _ERR_LINE.search(line):
            out.append(f'<span class="cb-log-err">{safe}</span>')
        elif _WARN_LINE.search(line):
            out.append(f'<span class="cb-log-warn">{safe}</span>')
        else:
            out.append(safe)
    return "\n".join(out)


def log_pane(title: str, text: str, path: Path, *, max_h: str = "60vh") -> None:
    with ui.row().classes("w-full items-center no-wrap").style("gap: 6px;"):
        ui.label(title).style(
            f"{MONO} font-size: 9px; color: {'#dc2626' if title == 'stderr' else '#475569'}; font-weight: 600;"
        )
        ui.label(path.name).style(f"{MONO} font-size: 9px; color: #94a3b8;")
        ui.space()
        (
            ui.button(icon="content_copy", on_click=lambda p=path: copy_full_log(p))
            .props("flat dense round size=xs")
            .style("color: #94a3b8;")
            .tooltip(f"Copy the full {title} to the clipboard")
        )
    ui.html(log_html(text), sanitize=False).classes("cb-log-well w-full").style(f"max-height: {max_h};")


def _read_full(path: Path) -> str | None:
    if not path.exists():
        return None
    text = path.read_text(errors="replace")
    if len(text) > _COPY_CAP_BYTES:
        text = f"[... truncated to the last {_COPY_CAP_BYTES // 2**20} MB ...]\n" + text[-_COPY_CAP_BYTES:]
    return text


async def copy_full_log(path: Path) -> None:
    text = await asyncio.to_thread(_read_full, path)
    if text is None:
        ui.notify(f"{path.name} not found", type="warning")
        return
    ui.clipboard.write(text)
    ui.notify(f"Copied {path.name} ({len(text.splitlines())} lines)", type="positive", timeout=1500)


# Keeps a live log stuck to its end unless the user has scrolled up to read.
_STICK_JS = """(() => {
    const e = getHtmlElement(%d);
    if (!e) return;
    if (!e.dataset.stickWatch) {
        e.dataset.stickWatch = '1';
        e.addEventListener('scroll', () => {
            e.dataset.stick = (e.scrollHeight - e.scrollTop - e.clientHeight < 24) ? '1' : '0';
        });
    }
    if (e.dataset.stick !== '0') e.scrollTop = e.scrollHeight;
})()"""


class LiveLog:
    """A polled log tail as one log well, written only when the text changed and kept scrolled
    to its end unless the user scrolled up. (`ui.log` makes an element per line: pushing a
    500-line tail on every poll created and deleted a thousand elements per tick.)"""

    def __init__(self) -> None:
        self._text: str | None = None
        self.element = ui.html("", sanitize=False).classes("cb-log-well cb-livelog")

    @property
    def is_deleted(self) -> bool:
        return self.element.is_deleted

    def set_visibility(self, visible: bool) -> None:
        self.element.set_visibility(visible)

    def set_text(self, text: str) -> None:
        if text == self._text or self.element.is_deleted:
            return
        self._text = text
        self.element.set_content(log_html(text))
        self.element.client.run_javascript(_STICK_JS % self.element.id)

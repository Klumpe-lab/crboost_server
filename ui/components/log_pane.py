"""One log pane.

Title + file name + a copy button that copies the whole file (the pane shows the tail), over a
wrapped <pre> so long lines fold instead of scrolling sideways. Takes a bare Path so that
anything that has a file can show it — an array task's task_N.out, a protocol stage's
run.out / run.err / run_submit.script — without a relion_job_name or per-instance widget refs.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from nicegui import ui

from services.array_tasks import escape_html
from ui.styles import MONO

STDOUT_STYLE = {"color": "#334155", "bg": "#f8fafc", "border": "#e2e8f0"}
STDERR_STYLE = {"color": "#b91c1c", "bg": "#fef2f2", "border": "#fecaca"}

# Clipboard payload cap: a runaway log can be hundreds of MB, and the text travels
# over the socket. Keep the tail, say so at the top.
_COPY_CAP_BYTES = 8 * 2**20


def log_pane(
    title: str,
    text: str,
    path: Path,
    *,
    color: str = "#334155",
    bg: str = "#f8fafc",
    border: str = "#e2e8f0",
    max_h: str = "60vh",
) -> None:
    with ui.row().classes("w-full items-center no-wrap").style("gap: 6px;"):
        ui.label(title).style(
            f"{MONO} font-size: 9px; color: {'#dc2626' if title == 'stderr' else '#64748b'}; font-weight: 600;"
        )
        ui.label(path.name).style(f"{MONO} font-size: 9px; color: #94a3b8;")
        ui.space()
        (
            ui.button(icon="content_copy", on_click=lambda p=path: copy_full_log(p))
            .props("flat dense round size=xs")
            .style("color: #94a3b8;")
            .tooltip(f"Copy the full {title} to the clipboard")
        )
    ui.html(
        f'<pre style="{MONO} font-size: 10px; line-height: 1.45; color: {color}; '
        f"white-space: pre-wrap; overflow-wrap: anywhere; margin: 0; width: 100%; box-sizing: border-box; "
        f"max-height: {max_h}; overflow-y: auto; background: {bg}; "
        f'padding: 6px 8px; border-radius: 4px; border: 1px solid {border};">'
        f"{escape_html(text)}</pre>",
        sanitize=False,
    ).classes("w-full")


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


# Keeps a live log pinned to its end unless the user has scrolled up to read.
_PIN_JS = """(() => {
    const e = getHtmlElement(%d);
    if (!e) return;
    if (!e.dataset.pinWatch) {
        e.dataset.pinWatch = '1';
        e.addEventListener('scroll', () => {
            e.dataset.pinned = (e.scrollHeight - e.scrollTop - e.clientHeight < 24) ? '1' : '0';
        });
    }
    if (e.dataset.pinned !== '0') e.scrollTop = e.scrollHeight;
})()"""


class LiveLog:
    """A polled log tail as one text element, written only when the text changed and kept
    scrolled to its end unless the user scrolled up. (`ui.log` makes an element per line: pushing a
    500-line tail on every poll created and deleted a thousand elements per tick.)"""

    def __init__(self, *, style: str) -> None:
        self._text: str | None = None
        self.element = ui.label("").classes("cb-livelog").style(style)

    @property
    def is_deleted(self) -> bool:
        return self.element.is_deleted

    def set_visibility(self, visible: bool) -> None:
        self.element.set_visibility(visible)

    def set_text(self, text: str) -> None:
        if text == self._text or self.element.is_deleted:
            return
        self._text = text
        self.element.set_text(text)
        self.element.client.run_javascript(_PIN_JS % self.element.id)

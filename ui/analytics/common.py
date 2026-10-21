"""Shared bits of the analytics panels: the type ranks, number formats, the provenance line."""

from __future__ import annotations

import html
import math
from collections.abc import Iterator, Sequence
from contextlib import contextmanager

from nicegui import ui

from services.particles.records import Record
from ui.job_plugins._field_styles import PANEL_HEAD_STYLE, PANEL_STYLE, PANEL_TITLE_STYLE

# The Species page's three type ranks: section (12 px semibold, mixed case, the title of its
# panel), field (9 px uppercase, muted), hint (10 px, lightest).
LABEL_CLS = "text-[9px] font-bold text-gray-400 uppercase tracking-wide"
HINT_CLS = "text-[10px] text-gray-400"
ERROR_CLS = "text-[11px] text-red-600"
WARN_COLOR = "#b45309"
# Multi-line hover text: keep the line breaks, cap the width.
TOOLTIP_STYLE = "white-space: pre-line; max-width: 460px;"


# A chart on a panel sits on white, as the Journey's chart tiles do.
CHART_CELL_STYLE = "background: #ffffff; border-radius: 4px; padding: 6px; box-sizing: border-box;"
# A table on a panel: a white body under its white sticky head, no rule above it.
TABLE_ON_PANEL_STYLE = "background: #ffffff; border-radius: 4px; border-top: none;"


def section(title: str, *, tooltip: str | None = None) -> None:
    with ui.label(title).style(PANEL_TITLE_STYLE):
        if tooltip:
            ui.tooltip(tooltip).style(TOOLTIP_STYLE)


@contextmanager
def panel(title: str | None = None, *, tooltip: str | None = None) -> Iterator[ui.element]:
    """One large block of an analytics page: a pale gray-blue panel, its title inside. Panels
    are separated by the page's gap, never by a rule; a block inside one is never a panel."""
    with ui.column().classes("w-full").style(f"gap: 8px; {PANEL_STYLE}") as el:
        if title:
            with ui.element("div").style(PANEL_HEAD_STYLE):
                section(title, tooltip=tooltip)
        yield el


def fmt_sigma(v: float | None, digits: int = 1) -> str:
    return "—" if v is None else f"{v:.{digits}f}σ"


def fmt_num(v: float | int | None) -> str:
    if v is None:
        return "—"
    if isinstance(v, float) and not math.isfinite(v):
        return "—"
    return f"{v:,}" if isinstance(v, int) else f"{v:,.4g}"


def fmt_sci(v: float | None) -> str:
    return "—" if v is None else f"{v:.2e}"


def fmt_minutes(v: float | None) -> str:
    return "—" if v is None else f"{v:.0f} min"


def esc(s: object) -> str:
    return html.escape(str(s), quote=True)


def provenance_text(records: Sequence[tuple[str, Record | None]]) -> str:
    """One line per record: what it is, who computed it, when, from how many files."""
    lines = []
    for label, rec in records:
        if rec is None:
            continue
        p = rec.provenance
        how = {"driver": "the job's driver", "server": "the server, from the job's files"}.get(
            p.computed_by, p.computed_by
        )
        line = f"{label}: {p.kind} v{p.schema} by {how}, {p.computed_at} — {len(p.sources)} source file(s)"
        if p.note:
            line += f"; {p.note}"
        lines.append(line + f" [{p.module}]")
    return "\n".join(lines)


def render_provenance(records: Sequence[tuple[str, Record | None]]) -> None:
    """The muted "where these numbers come from" line under a panel title; the hover lists each
    record's producer, code and sources."""
    present = [(label, r) for label, r in records if r is not None]
    if not present:
        return
    short = " · ".join(
        f"{label}: {'driver sidecar' if r.provenance.computed_by == 'driver' else 'backfill'} "
        f"{r.provenance.computed_at.replace('T', ' ')[:16]}"
        for label, r in present
    )
    with ui.label(short).classes(HINT_CLS):
        ui.tooltip(provenance_text(present)).style(TOOLTIP_STYLE)

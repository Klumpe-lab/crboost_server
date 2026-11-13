"""The branch map's look as variables, and its small text helpers.

Every colour and line the look decides is a CSS variable `--cb-<name>` read with a fallback, so the map takes its
values from the app's theme block (ui/dashboard/css.py `_THEME_CSS`) and looks the same as the rest of the app
when that block moves. The ink scale, the states, what is chosen and the map's own surfaces are named here once;
the components below never spell a colour.
"""

from __future__ import annotations

from nicegui import ui

from ui.dashboard.figures import JOURNEY_THEME as TH

FONT = "font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif);"
MONO_FONT = "font-family: var(--cb-mono, 'IBM Plex Mono', monospace);"


def tok(name: str, today: str) -> str:
    """A value the look decides: the app's variable `--cb-<name>`, `today` wherever no look sets it."""
    return f"var(--cb-{name}, {today})"


INK = tok("ink-1", "#0f172a")
BODY = tok("ink-2", "#334155")
SOFT = tok("ink-3", "#64748b")
MUTED = tok("ink-4", "#94a3b8")
LINE = tok("line", "#e2e8f0")
BLUE = tok("pick", "#2563eb")  # what is chosen: an input set by hand, the chain a new job reads, the candidates
GREEN = tok("ok-dot", "#10b981")
AMBER = tok("warn-dot", "#d97706")
RED = tok("bad-dot", "#ef4444")
SLATE = tok("bm-port", "#64748b")
CARD_BORDER = tok("bm-card-line", "#d6dee8")
PAPER = tok("bm-paper", "#f1f4f8")
PAPER_DOT = tok("bm-paper-dot", "#c3cbd6")
HAIR = tok("bm-hair", "#e9eef3")  # the hairline between a card's zones
BAND = tok("bm-band", "#f8fafc")  # the header and footer bands
DARK = "#0f172a"  # the Journey's canvas: pictures sit on it in every look
RUN = tok("run-dot", "#3b82f6")  # a tilt series on the GPU now
OFF = tok("off-dot", "#cbd5e1")  # a job not run yet
QUEUED = tok("que-dot", "#60a5fa")
STATUS = {"done": GREEN, "running": RUN, "failed": RED, "queued": QUEUED, "scheduled": OFF}
# The selected tilt series' chip, in the picker and on its handle.
CHIP_ON = tok("bm-chip-on", "#334155")
CHIP_ON_LINE = tok("bm-chip-on-line", "#334155")
CHIP_ON_INK = tok("bm-chip-on-ink", "#fff")
CHIP_ON_SOFT = tok("bm-chip-on-soft", "#cbd5e1")
CARD_SHADOW = tok("bm-card-shadow", "0 1px 2px rgba(15,23,42,0.05), 0 6px 16px rgba(15,23,42,0.05)")
INNER_R = "calc(var(--cb-bm-card-radius, 8px) - 1px)"  # the header's and footer's corners inside the card's
MENU_STYLE = (
    f"border: 1px solid {LINE}; border-radius: var(--cb-bm-menu-radius, 8px); "
    "box-shadow: var(--cb-bm-menu-shadow, 0 10px 28px rgba(15,23,42,0.16));"
)

# One mark per tilt series in a magazine or a trail: its fill and the word its hover says.
BOX_COLORS = {
    "ok": (GREEN, "done"),
    "fail": (tok("bad-dot", "#dc2626"), "failed"),
    "running": (RUN, "running"),
    "pending": (tok("wait-dot", "#d1d5db"), "waiting"),
    "skip": (tok("bm-skip", "#94a3b8"), "skipped"),
    "none": (tok("bm-none", "#e9edf2"), "not in this job"),
}


def voice(
    kind: str, size: float, ink: str, *, weight: int = 600, case: str = "none", track: str = "0", mono: bool = False
) -> str:
    """A label's type as variables (`--cb-label-*` for labels, `--cb-tag-*` for the tool badges), today's look in
    the fallbacks: one look turns every label on the map into the same voice."""
    face = "'IBM Plex Mono', monospace" if mono else "'IBM Plex Sans', sans-serif"
    return (
        f"font-family: var(--cb-{kind}-font, {face}); font-size: var(--cb-{kind}-size, {size}px); "
        f"font-weight: var(--cb-{kind}-weight, {weight}); text-transform: var(--cb-{kind}-case, {case}); "
        f"letter-spacing: var(--cb-{kind}-track, {track}); color: var(--cb-{kind}-ink, {ink});"
    )


def label(
    text: str,
    size: float,
    ink: str,
    *,
    weight: int = 600,
    case: str = "uppercase",
    track: str = "0.05em",
    extra: str = "",
) -> ui.element:
    """A label (an input's name, a block's title, a field's name) in the label voice."""
    return ui.label(text).style(
        f"{voice('label', size, ink, weight=weight, case=case, track=track)} line-height: 1.3; {extra}"
    )


def text(
    content: str, size: float, color: str, *, bold: bool = False, mono: bool = False, extra: str = ""
) -> ui.element:
    """A run of text in the house face: a name, a value, a sentence."""
    return ui.label(content).style(
        f"{MONO_FONT if mono else FONT} font-size: {size}px; color: {color}; "
        f"font-weight: {600 if bold else 400}; line-height: 1.3; {extra}"
    )


def hrow(gap: int = 6, extra: str = "") -> ui.element:
    return ui.element("div").style(f"display: flex; align-items: center; gap: {gap}px; min-width: 0; {extra}")


__all__ = ["TH"]

"""The Journey's style passes while they are compared: which one this browser shows.

The choice lives in ``app.storage.user["journey_style"]``, outside the prefs model. The
Journey's root carries ``cb-jv-<pass>`` for the CSS (``ui/dashboard/css.py``), and its charts
take the pass's ``ChartTheme`` (``ui/dashboard/figures.CHART_THEMES``) whenever they are built.
"""

from __future__ import annotations

from enum import StrEnum

from nicegui import app

from ui.dashboard.figures import CHART_THEMES, ChartTheme

_KEY = "journey_style"


class JourneyStyle(StrEnum):
    """A pass of the Journey's style."""

    A = "a"  # Ledger: the Tilts tab
    B = "b"  # Dense: the most on one screen
    C = "c"  # Soft: one muted accent


# The style select's options.
STYLE_LABELS = {JourneyStyle.A: "A · ledger", JourneyStyle.B: "B · dense", JourneyStyle.C: "C · soft"}


def journey_style() -> JourneyStyle:
    """The pass this browser shows; A until one is chosen."""
    try:
        return JourneyStyle(app.storage.user.get(_KEY, JourneyStyle.A))
    except ValueError:  # a stored value no pass carries any more: show the default
        return JourneyStyle.A


def set_journey_style(style: JourneyStyle) -> None:
    app.storage.user[_KEY] = style.value


def journey_theme() -> ChartTheme:
    """The chart theme of the pass this browser shows."""
    return CHART_THEMES[journey_style()]


def style_classes(style: JourneyStyle) -> tuple[str, str]:
    """(the root classes to remove, the one to add) for showing `style`."""
    return " ".join(f"cb-jv-{s}" for s in JourneyStyle), f"cb-jv-{style}"

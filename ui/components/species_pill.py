"""The one species pill (roadmap 08 S1).

Every surface that names a species — roster row, job-tab header, the Config tab
of a particle-phase job (and the Species rail in roadmap 10) — draws the same
capsule: species name in the species color on a tinted, bordered pill. Three
copies of the style string used to live in the roster, the default renderer and
the job-tab header; a recolor / rename now looks identical everywhere.
"""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

from ui.styles import SANS


def render_species_pill(species, *, compact: bool = False, tooltip: str | None = None):
    """Read-only species capsule. `compact` is the roster-row size (8 px text);
    the default is the header / Config-tab size (9 px)."""
    font_px = 8 if compact else 9
    pad = "1px 6px" if compact else "1px 8px"
    pill = ui.element("div").style(
        f"display: inline-flex; align-items: center; flex-shrink: 0; "
        f"background: {species.color}18; border: 1px solid {species.color}55; "
        f"border-radius: 999px; padding: {pad};"
    )
    if tooltip:
        pill.tooltip(tooltip)
    with pill:
        ui.label(species.name).style(
            f"font-size: {font_px}px; color: {species.color}; font-weight: 600; white-space: nowrap;"
        )
    return pill


def species_opener(callbacks: dict | None, species_id: str) -> Callable[[], None] | None:
    """The "open in Species" action for `species_id` — the workspace registers
    `callbacks["open_species"]` (switch to the workbench view + select the species).
    None when the surface has no view switcher (no callbacks / hook not registered),
    in which case the line renders without the link."""
    fn = (callbacks or {}).get("open_species")
    if fn is None:
        return None
    return lambda: fn(species_id)


def render_species_line(species, *, on_open: Callable[[], None] | None = None) -> None:
    """The one-line species header of a particle-phase job's Config tab: the pill
    plus an "open in Species" link (the workbench view today, the Species page
    from roadmap 10). With no resolvable species the line is an amber notice —
    the job's dropdowns / sanity checks depend on the link, so it must not be
    silent (CLAUDE.md "Surfacing uncertainty")."""
    with ui.row().classes("items-center gap-2").style("margin-bottom: 6px; flex-wrap: nowrap;"):
        if species is None:
            ui.icon("warning", size="13px").classes("text-amber-600")
            ui.label("No species linked to this job — assign one or use a `__<species_id>` instance suffix.").style(
                f"{SANS} font-size: 10px; color: #b45309;"
            )
            return
        ui.label("Species").style(
            f"{SANS} font-size: 9px; font-weight: 700; color: #94a3b8; "
            "letter-spacing: 0.06em; text-transform: uppercase;"
        )
        render_species_pill(species)
        if on_open is not None:
            ui.label("open in Species ↗").style(
                f"{SANS} font-size: 10px; color: #6366f1; cursor: pointer; text-decoration: underline dotted;"
            ).on("click", lambda _e: on_open()).tooltip("Templates, masks, picks and settings of this species")

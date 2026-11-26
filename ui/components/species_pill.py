"""The one species mark.

Every surface that names a species — job-tab header, pipeline table, the Config tab
of a particle-phase job, the registry's rail and title line, the roster's species tag, the
wall's Layers menu — draws the species' glyph, named on hover. The glyph says what kind of
particle it is: the icosahedral capsid by default, or one the species picked from
`static/icons/species/`, in the theme's grey or a muted hue the species chose
(`SPECIES_GLYPHS`, `SPECIES_GLYPH_HUES`). Where the name is the content (rail, title line,
Config tab line) it follows the glyph in plain slate text. The overlay colour stays on the
canvases it keys, never on names.
"""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

from services.models_base import species_glyph
from ui.components.svg_icon import load_icon_svg
from ui.styles import SANS


def species_glyph_svg(species, size: int = 11, *, key: str | None = None, ink: str | None = None) -> str:
    """The markup of a species' glyph at `size` px in its hue; `key` / `ink` draw another of the set instead."""
    own_key, own_ink = species_glyph(species)
    return load_icon_svg(f"species/{key or own_key}.svg", ink or own_ink, size=size)


def render_species_mark(species, *, named: bool = False, tooltip: str | None = None, size: int = 11):
    """Read-only species mark: the species' glyph, plus the name when `named`. The hover
    says `tooltip`, by default "Species: <name>"; "" gives it none (a caller whose own
    hover names the species)."""
    mark = ui.element("div").style("display: inline-flex; align-items: center; gap: 4px; flex-shrink: 0; min-width: 0;")
    with mark:
        ui.html(species_glyph_svg(species, size), sanitize=False).style("display: flex;")
        if named:
            ui.label(species.name).style(
                f"{SANS} font-size: 10px; color: var(--cb-slate-700, #334155); font-weight: 600; white-space: nowrap; "
                "overflow: hidden; text-overflow: ellipsis;"
            )
        if tooltip != "":
            ui.tooltip(tooltip or f"Species: {species.name}")
    return mark


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
    """The one-line species header of a particle-phase job's Config tab: the named mark
    plus an "open in Species" link. With no resolvable species the line is an amber notice —
    the job's dropdowns / sanity checks depend on the link, so it must not be
    silent (CLAUDE.md "Surfacing uncertainty")."""
    with ui.row().classes("items-center gap-2").style("margin-bottom: 6px; flex-wrap: nowrap;"):
        if species is None:
            ui.icon("warning", size="13px").classes("text-amber-600")
            ui.label("No species linked to this job — assign one or use a `__<species_id>` instance suffix.").style(
                f"{SANS} font-size: 10px; color: var(--cb-amber-700, #b45309);"
            )
            return
        ui.label("Species").style(
            f"{SANS} font-size: 9px; font-weight: 700; color: var(--cb-slate-400, #94a3b8); "
            "letter-spacing: 0.06em; text-transform: uppercase;"
        )
        render_species_mark(species, named=True)
        if on_open is not None:
            ui.label("open in Particles registry ↗").style(
                f"{SANS} font-size: 10px; color: var(--cb-head-ink, #12324f); cursor: pointer; "
                "text-decoration: underline dotted;"
            ).on("click", lambda _e: on_open()).tooltip("Templates, masks, picks and settings of this species")

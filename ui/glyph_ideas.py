"""`/dev/glyphs`: the species glyph catalogue, drawn from real structures in the toon style (roadmap 77).

Public structures (PDB, EMDB) and synthetic membranes, blurred to keep their architecture and drop their atoms,
rendered in ChimeraX (`scripts/glyph_renders/render_in_chimerax.py`) and drawn in the theme's ink as a toon: the
shading in three tones and the outline (`scripts/glyph_renders/make_glyphs.py --only toon`), scaled down to every size
a species mark is drawn at, into `~/.crboost/glyph_ideas/renders.json`. The page shows them when that file is there.

Each glyph is a mask over the theme's ink; a second mask for dark themes makes the lit side the bright one.
"""

from __future__ import annotations

import json
from pathlib import Path

from nicegui import ui

from services.models_base import SPECIES_GLYPH_HUES
from ui.components.segmented import render_segmented
from ui.dashboard.theme import THEMES, theme_block
from ui.job_plugins._field_styles import MONO, PANEL_TITLE_STYLE, SANS

_RENDERS = Path.home() / ".crboost" / "glyph_ideas" / "renders.json"

# render name -> (kind, where it comes from)
KINDS = {
    "ribosome_front": ("Ribosome", "E. coli 70S, PDB 7K00; the 30S a shade apart"),
    "proteasome26_front": ("26S proteasome", "human, PDB 5GJR; the 19S caps a shade apart"),
    "proteasome_side": ("20S proteasome", "yeast, PDB 1RYP; alpha and beta rings"),
    "groel_front": ("Chaperonin", "GroEL-GroES, PDB 1AON; GroES a shade apart"),
    "nucleosome_front": ("Nucleosome", "PDB 1KX5; the histones a shade apart from the DNA"),
    "rubisco_front": ("RuBisCO", "spinach L8S8, PDB 8RUC; the small subunits a shade apart"),
    "ferritin_front": ("Apoferritin", "human, PDB 6Z6U"),
    "igg_front": ("Antibody", "IgG, PDB 1IGT"),
    "tubulin_front": ("Protein", "unidentified: an alpha-beta tubulin dimer, PDB 1JFF"),
    "microtubule_tilted": ("Microtubule", "13 protofilaments, EMD-14150; tilted to its open end"),
    "microtubule_open": ("Microtubule, cut open", "the same, its near half cut away"),
    "actin_diag": ("Actin filament", "PDB 5ONV, extended along its own helix"),
    "aav_front": ("Icosahedral capsid", "AAV2, PDB 1LP3"),
    "virus_front": ("Enveloped virus", "a bilayer sphere with spikes (PDB 6VXX)"),
    "spike_side": ("Spike", "SARS-CoV-2 S trimer, PDB 6VXX"),
    "lattice_tilt": ("Lattice", "HIV-1 CA hexamers, PDB 3H47, tiled"),
    "clathrin_front": ("Coat", "clathrin cage, PDB 1XI4"),
    "npc_tilted": ("Nuclear pore", "human NPC, PDB 7R5J"),
    "membrane_edge": ("Membrane", "a lipid bilayer: two leaflets"),
    "vesicle_cut": ("Vesicle", "a bilayer sphere, cut through its middle"),
    "nachr_edge": ("Membrane protein", "acetylcholine receptor, PDB 2BG9, in its bilayer"),
    "atpase_edge": ("ATP synthase", "Polytomella dimer, PDB 6RD4, in its bilayer"),
}
SIZES = (9, 11, 14, 22, 40, 64)


def load_renders() -> dict:
    """The glyphs made from the renders, or none: a server where none were made shows how to make them."""
    try:
        return json.loads(_RENDERS.read_text())
    except FileNotFoundError:  # nothing rendered here yet; the page says how
        return {}


def _mask(image: str) -> str:
    v = f"{image} center / contain no-repeat"
    return f"-webkit-mask: {v}; mask: {v};"


def _css(renders: dict) -> str:
    rules = [".gr { flex-shrink: 0; background: var(--gr-ink, var(--cb-ink-3, #5b7089)); }"]
    for name, techs in renders.items():
        toon = techs.get("toon")
        if not toon:
            continue
        for px in SIZES:
            cls = f"gr-{name}-{px}"
            light, dark = toon["light"].get(str(px)), toon.get("dark", {}).get(str(px))
            if light:
                rules.append(f".{cls} {{ {_mask(f'url({light})')} }}")
            if dark:
                rules.append(f'[data-cb-theme="dark"] .{cls} {{ {_mask(f"url({dark})")} }}')
    return "\n".join(rules)


def _glyph(name: str, px: int, ink: str | None = None) -> None:
    style = f"width: {px}px; height: {px}px;" + (f" --gr-ink: {ink};" if ink else "")
    ui.element("div").classes(f"gr gr-{name}-{px}").style(style)


_NOTE = f"{SANS} font-size: 10.5px; color: var(--cb-ink-3, #5b7089); max-width: 1000px; line-height: 1.5;"
_CARD = (
    "background: var(--cb-surface, #ffffff); border: 1px solid var(--cb-panel-line, #d6e0ea); "
    "border-radius: var(--cb-r-lg, 3px);"
)
_LINE = "border-bottom: 1px solid var(--cb-line, #e4eaf1);"


def _catalogue(renders: dict) -> None:
    with ui.element("div").style(
        "display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap: 10px;"
    ):
        for name, (kind, source) in KINDS.items():
            if name not in renders:
                continue
            with ui.element("div").style(_CARD + " display: flex; gap: 12px; padding: 10px 12px; align-items: center;"):
                _glyph(name, 64)
                with ui.element("div").style("display: flex; flex-direction: column; gap: 6px; min-width: 0;"):
                    ui.label(kind).style(f"{SANS} font-size: 12px; font-weight: 600; color: var(--cb-ink-1, #12324f);")
                    ui.label(source).style(
                        f"{SANS} font-size: 9.5px; color: var(--cb-ink-4, #94a3b8); line-height: 1.35;"
                    )
                    with ui.element("div").style("display: flex; align-items: flex-end; gap: 8px;"):
                        for px in SIZES[:-1]:
                            _glyph(name, px)


def _in_place(renders: dict) -> None:
    """Where the app draws them: the roster's species tags (9 px), a rail of every kind (11 px), titles (14 px)."""
    hues = [SPECIES_GLYPH_HUES[h][1] for h in ("grey", "steel", "verdigris", "ochre", "rust", "slate")]
    names = [n for n in KINDS if n in renders]
    with ui.element("div").style(_CARD + " display: flex; flex-direction: column; gap: 10px; padding: 10px 12px;"):
        with ui.element("div").style("display: flex; gap: 5px; flex-wrap: wrap;"):
            for i, name in enumerate(names):
                with ui.element("div").style(
                    "display: inline-flex; align-items: center; gap: 3px; padding: 0 4px; height: 15px; "
                    "border: 1px solid var(--cb-tag-line, #dbe3ec); background: var(--cb-tag, #f1f5f9); "
                    "border-radius: var(--cb-tag-radius, 2px);"
                ):
                    _glyph(name, 9, hues[i % len(hues)])
                    ui.label(KINDS[name][0].upper()[:14]).style(f"{MONO} font-size: 8px; color: var(--cb-tag-ink);")
        with ui.element("div").style(
            "display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); column-gap: 24px;"
        ):
            for i, name in enumerate(names):
                with ui.element("div").style(f"display: flex; align-items: center; gap: 7px; height: 22px; {_LINE}"):
                    _glyph(name, 11, hues[i % len(hues)])
                    ui.label(KINDS[name][0]).style(
                        f"{SANS} font-size: 10px; font-weight: 600; color: var(--cb-slate-700, #334155);"
                    )
                    ui.element("div").style("flex: 1;")
                    ui.label(f"{(i + 3) * 911:,}").style(f"{MONO} font-size: 9px; color: var(--cb-ink-4, #94a3b8);")
        with ui.element("div").style("display: flex; gap: 22px; flex-wrap: wrap;"):
            for name in names[:4]:
                with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
                    _glyph(name, 14)
                    ui.label(KINDS[name][0]).style(PANEL_TITLE_STYLE + " color: var(--cb-ink-1, #12324f);")


def render_glyph_ideas(renders: dict) -> None:
    ui.add_head_html(f"<style>{''.join(theme_block(n) for n in THEMES)}{_css(renders)}</style>")
    segs = {}

    def on_theme(name: str) -> None:
        root.props(f'data-cb-theme="{name}"')
        segs["theme"].set_active(name)

    with (
        ui.element("div")
        .props('data-cb-theme="light"')
        .style(
            "position: fixed; inset: 0; overflow-y: auto; background: var(--cb-desk); "
            "background-image: var(--cb-desk-art); background-size: var(--cb-desk-art-size); padding: 18px 22px 40px;"
        ) as root
    ):
        with ui.element("div").style(
            "max-width: 1500px; margin: 0 auto; display: flex; flex-direction: column; gap: 12px;"
        ):
            with ui.element("div").style("display: flex; align-items: center; gap: 12px;"):
                ui.label("Particle glyphs").style(PANEL_TITLE_STYLE + " font-size: 14px; color: var(--cb-ink-1);")
                segs["theme"] = render_segmented([(n, t.label) for n, t in THEMES.items()], "light", on_theme)
            ui.label(
                "Public structures and synthetic membranes, blurred to keep their architecture and drop their atoms, "
                "rendered in ChimeraX and drawn as a toon in the theme's ink: the shading in three tones and the "
                "outline. Where a particle has two kinds of part (subunits, a cap, the DNA, its membrane) the second "
                "is a shade apart. Each at 64 px and at 9, 11, 14, 22 and 40, the sizes the app draws a species mark."
            ).style(_NOTE)
            if not renders:
                ui.label(
                    "Nothing rendered on this server yet: scripts/glyph_renders/render_in_chimerax.py renders, "
                    "scripts/glyph_renders/make_glyphs.py writes ~/.crboost/glyph_ideas/renders.json."
                ).style(f"{MONO} font-size: 10px; color: var(--cb-ink-3, #5b7089);")
                return
            _catalogue(renders)
            ui.label("In place: the roster's tags, a rail, titles").style(
                PANEL_TITLE_STYLE + " margin-top: 8px; color: var(--cb-ink-1);"
            )
            _in_place(renders)

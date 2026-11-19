"""Panels F and G of `/dev/registry-ideas`: a species' glyph and its hue, and how picks are drawn on a tomogram
picture.

F: the glyph is chrome. Every species shows the default glyph in the theme's grey until someone picks another of
the set (`glyphs.KINDS`) or a muted hue; the overlay colour stays on the canvases. The picker sits on the Species
tab, beside the overlay colour.

G: the picks on the tomogram wall and its zoom, as the gallery draws them today and with controls: the mark (a
dot, a ring, a ring as wide as the particle, a cross, a point), its size, the white edge, opacity, the palette and
which species. The look would live in the wall's Layers menu, one per user.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

from nicegui import ui

from ui.components.check import house_check
from ui.components.segmented import render_segmented
from ui.job_plugins._field_styles import FIELD_LABEL_STYLE, FIELD_ROW_STYLE, MONO, SANS, VIEW_TITLE_STYLE
from ui.registry_ideas import glyphs, parts
from ui.registry_ideas.data import PALETTES, Overlay, Species, in_palette
from ui.registry_ideas.parts import BODY, CAPTION, FAINT, INK, SOFT, DotLook

TILE_W = 240  # the wall's default tile (ui/tomo_gallery._SIZES "M")
ZOOM_W = 600  # about what the zoom gives the slice beside its side column


class Looks:
    """Each species' glyph and hue on this page: the default glyph in grey until one is picked. The views that draw a
    species' mark watch it and draw again when it changes."""

    def __init__(self, default: str, style: str = "density") -> None:
        self.default = default
        self.style = style  # which set the page draws (glyphs.STYLES)
        self.palette = "today"  # which overlay palette the page draws (data.PALETTES)
        self._by: dict[str, tuple[str, str]] = {}
        self._watchers: list[tuple[ui.element, Callable[[], None]]] = []

    def of(self, sid: str) -> tuple[str, str]:
        return self._by.get(sid, (self.default, "grey"))

    def color(self, sid: str) -> str:
        return glyphs.HUES[self.of(sid)[1]][1]

    def set(self, sid: str, *, glyph: str | None = None, hue: str | None = None) -> None:
        g, h = self.of(sid)
        self._by[sid] = (glyph or g, hue or h)
        alive = []
        for el, fn in self._watchers:
            if not el.is_deleted:
                alive.append((el, fn))
                fn()
        self._watchers = alive

    def watch(self, el: ui.element, fn: Callable[[], None]) -> None:
        self._watchers.append((el, fn))

    def name(self, sid: str) -> str:
        g, _h = self.of(sid)
        t = glyphs.title(g, self.style)
        return f"{t.lower()} (default)" if g == self.default else t

    def svg(self, key: str, size: int, color: str) -> str:
        return glyphs.draw(key, size, color, self.style)


def mark(looks: Looks, sid: str, size: int = 11) -> ui.html:
    g, _h = looks.of(sid)
    return ui.html(looks.svg(g, size, looks.color(sid)), sanitize=False).style("display: flex;")


def live(looks: Looks, build: Callable[[], None], style: str = "display: contents;") -> ui.element:
    """A slot that builds `build` and builds it again whenever a look changes."""
    box = ui.element("div").style(style)

    def draw() -> None:
        box.clear()
        with box:
            build()

    draw()
    looks.watch(box, draw)
    return box


# ── the picker ───────────────────────────────────────────────────────────────


def picker(looks: Looks, sid: str) -> None:
    """The glyphs and the hues, the current ones ringed. A click sets it at once."""

    def build() -> None:
        g, h = looks.of(sid)
        color = looks.color(sid)
        with ui.element("div").style("display: flex; flex-direction: column; gap: 8px; padding: 8px; width: 252px;"):
            ui.label("glyph").style(parts.TH)
            with ui.element("div").style("display: grid; grid-template-columns: repeat(6, 36px); gap: 4px;"):
                kinds = glyphs.catalogue(looks.style)[1]
                for key in (looks.default, *(k for k, _t in kinds)):
                    on = key == g
                    ring = "var(--cb-accent, #4a8fd4)" if on else "#e4eaf1"
                    fill = "#f4f8fc" if on else "#fff"
                    tile = ui.element("div").style(
                        "width: 36px; height: 36px; display: flex; align-items: center; justify-content: center; "
                        f"cursor: pointer; box-shadow: inset 0 0 0 1px {ring}; background: {fill};"
                    )
                    tile.on("click", lambda _e, k=key: looks.set(sid, glyph=k))
                    with tile:
                        ui.html(looks.svg(key, 22, color), sanitize=False)
                    t = glyphs.title(key, looks.style)
                    tile.tooltip(f"{t}: the default" if key == looks.default else t)
            ui.label("hue").style(parts.TH)
            with ui.element("div").style("display: flex; gap: 5px; flex-wrap: wrap;"):
                for key, (word, c) in glyphs.HUES.items():
                    on = key == h
                    sw = ui.element("div").style(
                        f"width: 14px; height: 14px; background: {c}; cursor: pointer; "
                        f"box-shadow: 0 0 0 {'2px #fff, 0 0 0 3px #12324f' if on else '1px rgba(15, 29, 46, .15)'};"
                    )
                    sw.on("click", lambda _e, k=key: looks.set(sid, hue=k))
                    sw.tooltip(word + (" (the default)" if key == "grey" else ""))

    live(looks, build, "display: block;")


def glyph_row(looks: Looks, sid: str) -> None:
    """The Species tab's row: the glyph and what it is; a click opens the picker."""
    with ui.element("div").style(FIELD_ROW_STYLE):
        ui.label("Glyph").style(FIELD_LABEL_STYLE).tooltip(
            "How this species is marked in the app's chrome: the rail, the roster, job pages, the layers. Grey unless "
            "a hue is picked; the canvases draw the overlay colour."
        )

        def build() -> None:
            _g, h = looks.of(sid)
            with ui.element("div").style(
                "display: inline-flex; align-items: center; gap: 7px; cursor: pointer; padding: 1px 6px 1px 3px; "
                "border: 1px solid #dbe3ec; border-radius: 2px;"
            ):
                mark(looks, sid, 14)
                ui.label(looks.name(sid)).style(f"{SANS} font-size: 10px; color: {BODY};")
                ui.label(glyphs.HUES[h][0]).style(f"{MONO} font-size: 9px; color: {SOFT};")
                ui.label("▾").style(f"font-size: 8px; color: {FAINT};")
                with ui.menu():
                    picker(looks, sid)

        live(looks, build, "display: inline-flex;")


# ── F: the panels ────────────────────────────────────────────────────────────


def defaults_panel(current: str, href: Callable[[str], str], sp_name: str, style: str) -> None:
    """The candidates for the default glyph, each at four sizes and in the two places it is seen most."""
    candidates = glyphs.catalogue(style)[0]
    with ui.element("div").style(
        f"display: grid; grid-template-columns: repeat({len(candidates)}, minmax(0, 1fr)); gap: 14px;"
    ):
        for key, title, words in candidates:
            on = key == current
            with (
                ui.link(target=href(key))
                .style(
                    "text-decoration: none; display: flex; flex-direction: column; gap: 8px; padding: 10px; "
                    f"box-shadow: inset 0 0 0 1px {'var(--cb-accent, #4a8fd4)' if on else '#e4eaf1'}; "
                    f"background: {'#f4f8fc' if on else '#fff'};"
                )
                .tooltip("Use it as the default across this page")
            ):
                with ui.element("div").style("display: flex; align-items: baseline; gap: 8px;"):
                    ui.label(title).style(f"{SANS} font-size: 11px; font-weight: 600; color: {INK};")
                    if on:
                        ui.label("on this page").style(parts.TH)
                ui.label(words).style(f"{CAPTION} min-height: 28px;")
                with ui.element("div").style("display: flex; align-items: flex-end; gap: 12px;"):
                    for size in (11, 14, 24, 40):
                        ui.html(glyphs.draw(key, size, "#94a3b8" if size < 20 else "#5b7089", style), sanitize=False)
                with ui.element("div").style(
                    "display: flex; align-items: center; gap: 5px; padding: 3px 6px; background: #fff; "
                    "border: 1px solid #eef2f6;"
                ):
                    ui.html(glyphs.draw(key, 11, "#94a3b8", style), sanitize=False)
                    ui.label(sp_name).style(
                        f"{SANS} font-size: 10px; color: #334155; font-weight: 600; overflow: hidden; "
                        "text-overflow: ellipsis; white-space: nowrap; min-width: 0;"
                    )
                with ui.element("div").style("display: flex;"):
                    _tag(glyphs.draw(key, 9, "#94a3b8", style), sp_name)


def _tag(glyph_svg: str, name: str) -> None:
    """The roster's species tag, with the glyph in front of the name."""
    with ui.element("span").classes("cbri-tag").style("gap: 3px; padding: 0 3px 0 2px; max-width: 18ch;"):
        ui.html(glyph_svg, sanitize=False)
        ui.label(name).style("overflow: hidden; text-overflow: ellipsis;")


def in_place(looks: Looks, sp: Species, n_picks: int) -> None:
    """The species' mark where the app draws it, with whatever the picker beside it says."""

    def build() -> None:
        g, _h = looks.of(sp.id)
        c = looks.color(sp.id)
        rows: list[tuple[str, Callable[[], None]]] = []

        def title() -> None:
            ui.label("Particles registry").style(VIEW_TITLE_STYLE + " margin-right: 8px;")
            mark(looks, sp.id)
            ui.label(sp.name).style(f"{SANS} font-size: 10px; color: #334155; font-weight: 600;")

        def rail() -> None:
            mark(looks, sp.id)
            ui.label(sp.name).style(f"{SANS} font-size: 10px; color: #334155; font-weight: 600;")
            ui.label(f"{n_picks:,} picks").style(f"{MONO} font-size: 8.5px; color: {SOFT}; margin-left: 18px;")

        def roster() -> None:
            ui.element("div").style("width: 7px; height: 7px; background: #22a06b;")
            ui.label("Template Match").style(f"{MONO} font-size: 10px; color: {INK}; width: 17ch;")
            ui.label("job010").style(f"{MONO} font-size: 8px; color: {FAINT}; width: 7ch;")
            _tag(looks.svg(g, 9, c), sp.name)

        def job_line() -> None:
            ui.label("Species").style(
                f"{SANS} font-size: 9px; font-weight: 700; color: {FAINT}; text-transform: uppercase;"
            )
            mark(looks, sp.id)
            ui.label(sp.name).style(f"{SANS} font-size: 10px; color: #334155; font-weight: 600;")
            ui.label("open in Particles registry ↗").style(
                f"{SANS} font-size: 10px; color: {INK}; text-decoration: underline dotted;"
            )

        def layers() -> None:
            oc = in_palette(sp.color, looks.palette)
            ui.element("div").style(f"width: 8px; height: 8px; border-radius: 50%; background: {oc};").tooltip(
                "the overlay colour: what the picks are drawn in on the canvas"
            )
            mark(looks, sp.id)
            ui.label(f"TM picks · {sp.name}").style(f"{SANS} font-size: 10px; color: {BODY};")
            ui.label(f"{n_picks:,}").style(f"{MONO} font-size: 9px; color: {SOFT}; margin-left: 10px;")

        rows = [
            ("the registry's title line", title),
            ("the rail", rail),
            ("a roster row's end", roster),
            ("a job page's species line", job_line),
            ("the wall's Layers menu", layers),
        ]
        with ui.element("div").style(
            "display: grid; grid-template-columns: 150px minmax(0, 1fr); align-items: center; gap: 10px 14px;"
        ):
            for word, fn in rows:
                ui.label(word).style(f"{SANS} font-size: 9.5px; color: {SOFT};")
                with ui.element("div").style("display: flex; align-items: center; gap: 6px; min-height: 22px;"):
                    fn()

    live(looks, build, "display: block;")


# ── G: picks on the tomogram picture ─────────────────────────────────────────

MARKS = (("dot", "dot"), ("ring", "ring"), ("scale", "to scale"), ("cross", "cross"), ("point", "point"))
PROPOSED = DotLook(mark="ring", r=3.0, rim=False, alpha=0.9)


class DotsPanel:
    """Today's dots beside dots drawn with the controls, on the wall's tile and on the zoom."""

    def __init__(self, overlays: dict[str, Overlay], palette: str = "today") -> None:
        self.overlays = overlays
        self.which = "typical" if "typical" in overlays else next(iter(overlays))
        self.look = replace(PROPOSED, palette=palette)
        self.today: list[ui.html] = []
        self.after: list[ui.html] = []
        self.caption: ui.label | None = None
        self.size_read: ui.label | None = None

    @property
    def ov(self) -> Overlay:
        return self.overlays[self.which]

    def build(self) -> None:
        def label(text: str) -> None:
            ui.label(text).style(parts.TH + " width: 64px; flex-shrink: 0;")

        with ui.element("div").style("display: flex; flex-direction: column; gap: 7px;"):
            with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
                label("tomogram")
                render_segmented(
                    [
                        (k, f"{o.short} · {sum(len(x.dots) for x in o.layers):,} picks")
                        for k, o in self.overlays.items()
                    ],
                    self.which,
                    self._tomo,
                    classes="cb-seg-sm",
                )
            with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
                label("mark")
                render_segmented(list(MARKS), self.look.mark, lambda k: self._set(mark=k), classes="cb-seg-sm")
                ui.label("to scale: a ring as wide as the species' diameter").style(f"{CAPTION} margin-left: 6px;")
            with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
                label("size")
                ui.slider(
                    min=0.5, max=4.0, step=0.1, value=self.look.r, on_change=lambda e: self._set(r=float(e.value))
                ).props("dense track-size=2px thumb-size=10px").classes("cb-fit-slider").style(
                    "width: 160px; margin: 0;"
                )
                self.size_read = ui.label("").style(f"{MONO} font-size: 9px; color: {BODY}; width: 150px;")
                label("opacity")
                ui.slider(
                    min=0.15,
                    max=1.0,
                    step=0.05,
                    value=self.look.alpha,
                    on_change=lambda e: self._set(alpha=float(e.value)),
                ).props("dense track-size=2px thumb-size=10px").classes("cb-fit-slider").style(
                    "width: 120px; margin: 0;"
                )
            with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
                label("palette")
                render_segmented(
                    [(k, v[0]) for k, v in PALETTES.items()],
                    self.look.palette,
                    lambda k: self._set(palette=k),
                    classes="cb-seg-sm",
                )
            with ui.element("div").style("display: flex; align-items: center; gap: 14px;"):
                label("draw")
                house_check("white edge", value=self.look.rim, on_change=lambda e: self._set(rim=bool(e.value)))
                for layer in {x.species: x for o in self.overlays.values() for x in o.layers}.values():
                    house_check(
                        layer.species, value=True, on_change=lambda e, s=layer.species: self._toggle(s, bool(e.value))
                    )
            for title, store in (
                ("today · the gallery's dots: 2 px radius on the zoom, 1.1 on the wall, a white edge", self.today),
                ("with these settings", self.after),
            ):
                ui.label(title).style(parts.TH + " margin-top: 8px;")
                with ui.element("div").style("display: flex; gap: 14px; align-items: flex-start;"):
                    for wall in (True, False):
                        with ui.element("div").style("display: flex; flex-direction: column; gap: 3px;"):
                            store.append(ui.html("", sanitize=False))
                            ui.label("wall tile, M" if wall else "zoom").style(
                                f"{MONO} font-size: 8px; color: {FAINT};"
                            )
            self.caption = ui.label("").style(CAPTION)
        self._draw(today=True)

    def _tomo(self, key: str) -> None:
        self.which = key
        self._draw(today=True)

    def _set(self, **kw) -> None:
        self.look = replace(self.look, **kw)
        self._draw()

    def _toggle(self, species: str, on: bool) -> None:
        hidden = set(self.look.hidden)
        (hidden.discard if on else hidden.add)(species)
        self.look = replace(self.look, hidden=frozenset(hidden))
        self._draw()

    def _draw(self, *, today: bool = False) -> None:
        ov = self.ov
        if today:
            self.today[0].set_content(parts.dots_html(ov, parts.TODAY_WALL, size=TILE_W, wall=True))
            self.today[1].set_content(parts.dots_html(ov, parts.TODAY_ZOOM, size=ZOOM_W))
        self.after[0].set_content(parts.dots_html(ov, self.look, size=TILE_W, wall=True))
        self.after[1].set_content(parts.dots_html(ov, self.look, size=ZOOM_W))
        lk = self.look
        if lk.mark == "scale":
            self.size_read.set_text("size: the particle's")
        else:
            self.size_read.set_text(f"r {lk.r:.1f} px zoom · {lk.r * parts.WALL_SCALE:.1f} px wall")
        counts = " · ".join(f"{x.species} {len(x.dots):,}" for x in ov.layers)
        self.caption.set_text(
            f"{ov.short}: {counts}. The wall tile draws at most {parts.WALL_CAP} picks per species, as the gallery "
            f"does; the zoom draws them all. Proposed default: a {PROPOSED.r:g} px {PROPOSED.mark}, no white edge, "
            f"{PROPOSED.alpha:.0%} opacity."
        )

"""The tilt-series picker above the map: a handle naming the tilt series picked (or how many there are), the pin,
and, while open, one chip per tilt series with its position and a trail of per-job marks. Picking one rings its
square in every magazine and turns the large cards to it; it is the workspace's selected tilt series, so the
Journey and the Tomograms view follow."""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

from ui.branch_map.card import light
from ui.branch_map.model import Project
from ui.branch_map.plots import svg
from ui.branch_map.state import MapState
from ui.branch_map.style import (
    BOX_COLORS,
    CARD_BORDER,
    CHIP_ON,
    CHIP_ON_INK,
    CHIP_ON_LINE,
    CHIP_ON_SOFT,
    HAIR,
    INK,
    MUTED,
    SOFT,
    hrow,
    label,
    text,
    tok,
)


def trail_svg(pr: Project, trail: list[tuple[str, str]], *, side: float, gap: float = 1) -> str:
    """How one tilt series fared at every job, one mark per job in map order, named on hover."""
    parts = []
    for i, (jid, state) in enumerate(trail):
        fill, word = BOX_COLORS.get(state, BOX_COLORS["pending"])
        j = pr.by_id.get(jid)
        name = pr.ident(j) if j else jid
        x = i * (side + gap)
        parts.append(
            f'<rect x="{x:.1f}" y="0" width="{side}" height="{side}" rx="{0.5 if side < 4 else 1}" fill="{fill}">'
            f"<title>{name} · {word}</title></rect>"
        )
    w = len(trail) * (side + gap) - gap
    return svg(max(w, 1), side, parts)


def _worst(trail: list[tuple[str, str]]) -> str:
    states = {s for _, s in trail}
    for s in ("fail", "running", "pending", "ok"):
        if s in states:
            return s
    return "ok"


def picker_body(pr: Project, st: MapState, pick: Callable[[str], None]) -> None:
    """One chip per tilt series: its light, its position, its trail. `pick(ts)` is a click on one."""
    if not pr.ts_ids:
        text("no tilt series known", 9, MUTED)
        return
    with ui.element("div").style("display: flex; flex-wrap: wrap; gap: 4px 5px; padding: 4px 0;"):
        for ts in pr.ts_ids:
            on = ts == st.active_ts
            trail = pr.trail(ts)
            chip = (
                ui.element("div")
                .classes("cb-bm-chip")
                .style(
                    "display: flex; align-items: center; gap: 6px; height: 19px; padding: 0 7px 0 6px; "
                    "border-radius: var(--cb-bm-chip-radius, 10px); "
                    f"border: 1px solid {CHIP_ON_LINE if on else CARD_BORDER}; "
                    f"background: {CHIP_ON if on else '#fff'}; cursor: pointer;"
                )
            )
            with chip:
                light({"fail": "failed", "running": "running", "pending": "scheduled", "ok": "done"}[_worst(trail)], 6)
                text(pr.ts_title(ts), 9, CHIP_ON_INK if on else INK, bold=True, extra="white-space: nowrap;")
                ui.html(trail_svg(pr, trail, side=4), sanitize=False).style("line-height: 0;")
            facts = pr.ts_static.get(ts)
            tip = ts
            if facts is not None:
                tip += f" · {facts.tilts} tilts" + (f", {facts.dropped} dropped" if facts.dropped else "")
                if facts.defocus_um is not None:
                    tip += f" · {facts.defocus_um:.1f} µm"
            chip.tooltip(tip)
            chip.on("click", lambda _e, t=ts: pick(t))


def render_picker(pr: Project, st: MapState, redraw, pick: Callable[[str | None], None]) -> None:
    """The bar above the map: a handle that names the tilt series picked (or how many there are), the pin, and the
    chips while it is open."""
    with ui.element("div").style(
        f"border: 1px solid var(--cb-bm-box-line, {CARD_BORDER}); border-radius: var(--cb-bm-box-radius, 8px); "
        "background: var(--cb-bm-box-bg, #fff); margin-bottom: var(--cb-bm-box-gap, 8px); "
        "padding: 0 var(--cb-bm-box-pad, 10px); box-shadow: var(--cb-bm-box-rule, none); flex-shrink: 0;"
    ):
        with hrow(8, "height: 26px;"):
            handle = hrow(5, "cursor: pointer; height: 26px;").classes("cb-bm-handle")
            with handle:
                ui.icon("expand_less" if st.picker_open else "expand_more", size="14px").style(f"color: {SOFT};")
                label("Tilt series", 10, INK, case="none", track="0")
                text(str(len(pr.ts_ids)), 10, MUTED, mono=True)
            handle.on("click", lambda _e: (setattr(st, "picker_open", not st.picker_open), redraw()))
            if st.active_ts:
                chip = hrow(
                    5,
                    "height: 18px; padding: 0 4px 0 8px; border-radius: var(--cb-bm-chip-radius, 9px); "
                    f"background: {CHIP_ON}; box-shadow: inset 0 0 0 1px {CHIP_ON_LINE};",
                )
                with chip:
                    text(pr.ts_title(st.active_ts), 9, CHIP_ON_INK, bold=True)
                    text(
                        st.active_ts,
                        9,
                        CHIP_ON_SOFT,
                        mono=True,
                        extra="max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;",
                    )
                    x = ui.element("div").style(
                        "width: 14px; height: 14px; display: flex; align-items: center; justify-content: center; "
                        f"cursor: pointer; color: {CHIP_ON_SOFT};"
                    )
                    with x:
                        ui.icon("close", size="11px")
                    x.tooltip("No tilt series picked: the large cards show every tilt series again")
                    x.on("click.stop", lambda _e: pick(None))
            else:
                text("pick one to ring it in every magazine and turn the large cards to it", 9, MUTED)
            ui.element("div").style("flex: 1 1 0;")
            if st.picker_open or st.picker_pinned:
                pin = ui.element("div").style(
                    "width: 16px; height: 16px; display: flex; align-items: center; justify-content: center; "
                    f"cursor: pointer; color: {INK if st.picker_pinned else tok('off-dot', '#cbd5e1')}; "
                    f"transform: rotate({0 if st.picker_pinned else 45}deg);"
                )
                with pin:
                    ui.icon("push_pin", size="13px")
                pin.tooltip("Pinned: stays open after a pick" if st.picker_pinned else "Pin the picker open")
                pin.on(
                    "click",
                    lambda _e: (
                        setattr(st, "picker_pinned", not st.picker_pinned),
                        setattr(st, "picker_open", True),
                        redraw(),
                    ),
                )
        if st.picker_open:
            with ui.element("div").style(
                f"border-top: 1px solid {HAIR}; padding: 2px 0 4px; max-height: 220px; overflow-y: auto; "
                "scrollbar-width: thin;"
            ):
                picker_body(pr, st, pick)

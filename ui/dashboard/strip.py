"""Journey — the tilt series it shows, on the view's title line, and the switcher behind it.

The title line names the selected tilt series by its position, spelled out (``Stage position 3 ·
Beam position 2``), and where it sits among them (``3 / 14``). A click on the name drops down every
tilt series in the project: a search field, then one line per tilt series, grouped by stage
position, with every species' pick count in it, and the totals at the foot. The ⓘ (its files and
pick counts) and the exclude switch go with the title line's tools.

Pure presentation over ``services.dashboard_data``'s collectors — no new disk reads, and no
back-import into ``tomo_dashboard_dialog`` (the ⓘ info popover arrives as a callback, keeping the
package DAG one-directional).
"""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

from services.dashboard_data import spelled_position

# status → human word, for the pick-count tooltips.
_STATUS_WORD = {
    "ok": "done",
    "fail": "failed",
    "running": "running",
    "zero": "ran, nothing above cutoff",
    "pending": "not started",
    # Emitted by the per-TS task scanner for a tilt series excluded from processing.
    "skip": "skipped",
}

# The search field filters the rows in the browser: every word typed must be in a row's
# `data-q` (its name and its position, both ways of writing it); a group with no row left hides.
_FILTER_JS = """(e) => {
    const box = e.target.closest('.cb-ts-menu');
    if (!box) return;
    const words = e.target.value.toLowerCase().split(/\\s+/).filter(Boolean);
    for (const group of box.querySelectorAll('.cb-ts-group')) {
        let any = false;
        for (const row of group.querySelectorAll('.cb-ts-row[data-q]')) {
            const hit = words.every((w) => row.dataset.q.includes(w));
            row.style.display = hit ? '' : 'none';
            any = any || hit;
        }
        group.style.display = any ? '' : 'none';
    }
}"""
# Enter takes the first tilt series left.
_ENTER_JS = """(e) => {
    if (e.key !== 'Enter') return;
    const box = e.target.closest('.cb-ts-menu');
    const row = box && [...box.querySelectorAll('.cb-ts-row[data-q]')].find((r) => r.offsetParent !== null);
    if (row) row.click();
}"""


def _on_show_js(search_id: int) -> str:
    """The dropdown opens with an empty search field, the cursor in it and the selected row in view."""
    return (
        f"() => {{ const i = getHtmlElement({search_id}); if (!i) return; "
        "i.value = ''; i.dispatchEvent(new Event('input')); i.focus(); "
        "const sel = i.closest('.cb-ts-menu').querySelector('.cb-ts-row.selected'); "
        "if (sel) sel.scrollIntoView({block: 'nearest'}); }"
    )


def _aggregate_species(species_journey: dict, ts_names: list) -> list[dict]:
    """Collapse the per-TS species lists into one ordered row-spec per species,
    with project-wide Σ pick / Σ kept totals. Order + color follow the species
    ``idx`` (stable across TS), so the columns line up with the canvas overlay
    and the particle tabs."""
    meta: dict[int, dict] = {}
    for ts in ts_names:
        for s in species_journey.get(ts, []):
            m = meta.setdefault(
                s["idx"],
                {"idx": s["idx"], "label": s["label"], "color": s["color"], "picks": 0, "filt": 0, "has_filt": False},
            )
            if s.get("n_picks") is not None:
                m["picks"] += s["n_picks"]
            if s.get("filtered_count") is not None:
                m["filt"] += s["filtered_count"]
                m["has_filt"] = True
    return [meta[k] for k in sorted(meta)]


def _pick_state(s: dict) -> str:
    """Only genuinely-stateful conditions get a tint: running (blue), failed (red),
    ran-but-zero (grey). Picks merely existing is neutral."""
    pk = s.get("pick_status")
    sub = s.get("subtomo_status")
    if pk == "running" or sub == "running":
        return "running"
    if pk == "fail" or sub == "fail":
        return "fail"
    if pk == "ok" or sub == "ok":
        return "has"
    if pk == "zero" or sub == "zero":
        return "zero"
    return "pending"


def _pick_tooltip(s: dict) -> str:
    pk = _STATUS_WORD.get(s.get("pick_status"), s.get("pick_status") or "—")
    sub = _STATUS_WORD.get(s.get("subtomo_status"), s.get("subtomo_status") or "—")
    n = s.get("n_picks")
    npart = f"{n} picks" if n is not None else "no pick count yet"
    return f"{s['label']} — picking: {pk} ({npart}); extraction: {sub}"


def _species_count(s: dict | None, sp: dict) -> None:
    """One species cell: count (· kept after curation)."""
    if s is None:
        with ui.element("div").classes("cb-jsp pending").tooltip(f"{sp['label']}: not in this tilt series"):
            ui.label("—").classes("cb-jsp-n")
        return
    n = s.get("n_picks")
    with ui.element("div").classes(f"cb-jsp {_pick_state(s)}").tooltip(_pick_tooltip(s)):
        ui.label(str(n) if n is not None else "·").classes("cb-jsp-n")
        fc = s.get("filtered_count")
        if fc is not None:
            ui.label(f"{fc} kept").classes("cb-jsp-kept").tooltip(f"{fc} kept after curation")


def _ordered(ts_names: list) -> list[str]:
    """By stage position, then beam; names without a position after them, by name."""

    def key(ts: str) -> tuple:
        spelled = spelled_position(ts)
        return (0, spelled[2], ts) if spelled else (1, (0, 0), ts)

    return sorted(ts_names, key=key)


def _ts_title(ts: str) -> str:
    """The tilt series as the title line names it: its position spelled out, else its name."""
    spelled = spelled_position(ts)
    return f"{spelled[0]} · {spelled[1]}" if spelled else ts


def build_strip(
    lead,
    tools,
    *,
    species_journey: dict,
    ts_names: list,
    selected_ts: str | None,
    recon_mrc_map: dict,
    on_select: Callable[[str], object],
    info_popover: Callable | None = None,
    excluded_ids: set | None = None,
    on_toggle_exclude: Callable[[str], object] | None = None,
) -> dict:
    """Build the selected tilt series' name (with the switcher behind it) into ``lead`` and its ⓘ and
    exclude switch into ``tools``; return ``{ts: row element}`` of the switcher, so the caller can
    tell which tilt series have a row.

    ``on_select(ts)`` is the row-click handler; ``info_popover(ts, species, recon_mrc)`` (optional)
    renders the ⓘ popover — passed in so this module never imports the shell.
    """
    species = _aggregate_species(species_journey, ts_names)
    excluded = excluded_ids or set()
    order = _ordered(ts_names)
    row_els: dict[str, object] = {}

    lead.clear()
    tools.clear()
    if selected_ts is None or not ts_names:
        with lead:
            ui.label("No tilt series yet").classes("cb-ts-empty")
        return row_els

    with lead:
        ui.icon("chevron_right", size="14px").classes("cb-ts-sep")
        switch = ui.element("div").classes("cb-ts-switch")
        switch.props["title"] = selected_ts
        with switch:
            ui.label(_ts_title(selected_ts)).classes("cb-ts-name" + (" excluded" if selected_ts in excluded else ""))
            ui.icon("expand_more", size="13px").classes("cb-ts-caret")
            # QMenu opens on its parent's click — the whole name is the trigger.
            menu = (
                ui.menu()
                .props("anchor='bottom left' self='top left' :offset='[0, 3]' max-height=80vh max-width=96vw")
                .classes("cb-hmenu")
            )
            with menu:
                row_els.update(_build_switcher(species_journey, species, order, selected_ts, excluded, on_select, menu))
        if selected_ts in order:
            ui.label(f"{order.index(selected_ts) + 1} / {len(order)}").classes("cb-ts-idx")

    with tools:
        if info_popover is not None:
            info_popover(selected_ts, species_journey.get(selected_ts, []), recon_mrc_map.get(selected_ts))
        if on_toggle_exclude is not None:
            is_excl = selected_ts in excluded
            tog = ui.icon("undo" if is_excl else "block").classes("cb-jexcl" + (" on" if is_excl else ""))
            tog.tooltip(
                "Restore this tilt series to processing"
                if is_excl
                else "Exclude this tilt series from processing (applies on the next run)"
            )
            tog.on("click", lambda t=selected_ts: on_toggle_exclude(t))
    return row_els


def _build_switcher(
    species_journey: dict,
    species: list[dict],
    order: list[str],
    selected_ts: str | None,
    excluded: set,
    on_select: Callable[[str], object],
    menu,
) -> dict:
    """The dropdown: a search field, then the tilt series grouped by stage position, one line each
    (its beam position, its full name, every species' pick count), and a totals line. Clicking a row
    selects it and closes the menu."""
    row_els: dict[str, object] = {}
    # Fixed tracks: every line is a grid of its own, so only fixed widths keep the columns lined up.
    cols = "104px minmax(90px, 1fr)" + (f" repeat({len(species)}, 84px)" if species else "")
    with ui.element("div").classes("cb-ts-menu").style(f"--cb-ts-cols: {cols};"):
        with ui.element("div").classes("cb-ts-search"):
            ui.icon("search", size="13px")
            search = ui.element("input").classes("cb-ts-search-input")
            search.props.update(type="search", placeholder="Search tilt series", autocomplete="off", spellcheck="false")
            search.on("input", js_handler=_FILTER_JS)
            search.on("keydown", js_handler=_ENTER_JS)
        if species:
            with ui.element("div").classes("cb-ts-row cb-ts-head"):
                ui.label("")
                ui.label("")
                for sp in species:
                    with ui.element("div").classes("cb-ts-sp").tooltip(sp["label"]):
                        ui.element("div").classes("cb-jsp-dot").style(f"background: {sp['color']};")
                        ui.label(sp["label"]).classes("cb-ts-sp-name")
        group_of: str | None = None
        holder = None
        for ts in order:
            spelled = spelled_position(ts)
            group = spelled[0] if spelled else None
            if holder is None or group != group_of:
                group_of = group
                holder = ui.element("div").classes("cb-ts-group")
                if group is not None:
                    with holder:
                        ui.label(group).classes("cb-ts-group-head")
            cls = "cb-ts-row" + (" selected" if ts == selected_ts else "") + (" excluded" if ts in excluded else "")
            with holder:
                row = ui.element("div").classes(cls)
            if spelled:
                stage, beam = spelled[2]
                row.props["data-q"] = f"{ts} {_ts_title(ts)} pos {stage} beam {beam}".lower()
            else:
                row.props["data-q"] = ts.lower()
            row_els[ts] = row

            def _pick(_e=None, t=ts):
                menu.close()
                # `on_select` is the journey's async select_ts: hand its coroutine back
                # to NiceGUI, which awaits an awaitable handler result.
                return on_select(t)

            row.on("click", _pick)
            with row:
                if spelled:
                    ui.label(spelled[1]).classes("cb-ts-beam")
                    ui.label(ts).classes("cb-ts-full")
                else:
                    ui.label(ts).classes("cb-ts-beam cb-ts-wide")
                by_idx = {s["idx"]: s for s in species_journey.get(ts, [])}
                for sp in species:
                    _species_count(by_idx.get(sp["idx"]), sp)
        with ui.element("div").classes("cb-ts-row cb-ts-total"):
            ui.label(f"{len(order)} tilt series").classes("cb-ts-total-name")
            ui.label("")
            for sp in species:
                with (
                    ui.element("div")
                    .classes("cb-jsp")
                    .tooltip(
                        "Picks in all tilt series / kept after curation"
                        if sp["has_filt"]
                        else "Picks in all tilt series"
                    )
                ):
                    ui.label(str(sp["picks"])).classes("cb-jsp-n")
                    if sp["has_filt"]:
                        ui.label(f"{sp['filt']} kept").classes("cb-jsp-kept")
    menu.on("show", js_handler=_on_show_js(search.id))
    return row_els

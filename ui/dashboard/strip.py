"""Journey dashboard — the tomogram header line + the tomogram switcher.

The Journey's header is ONE thin line about the selected tilt series: its position
label and index (``Pos 3 · Beam 2``, ``3 / 14``), the four preparation stages as
labelled status dots, and every species' pick count in it. The line is clickable: it
drops down a table of every tilt series in the project (the same facts per row, plus
project-wide pick totals) to switch the journey to another one. The overview lives in
that dropdown — it is not on screen while a single tomogram is being inspected.

Pure presentation over ``services.dashboard_data``'s collectors — no new disk reads, and no
back-import into ``tomo_dashboard_dialog`` (the ⓘ info popover arrives as a callback,
keeping the package DAG one-directional).
"""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

from services.dashboard_data import PREP_STAGES, position_label

# status → human word, for dot tooltips.
_STATUS_WORD = {
    "ok": "done",
    "fail": "failed",
    "running": "running",
    "zero": "ran, nothing above cutoff",
    "pending": "not started",
    # Emitted by the per-TS task scanner for a tilt series excluded from processing.
    "skip": "skipped",
}

# Full words for the four preparation stages — the header has room for them.
_STAGE_WORD = {"fs_ctf": "Motion & CTF", "align": "Alignment", "ctf": "CTF", "recon": "Reconstruction"}


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
    """Only genuinely-stateful conditions get a tint: running (amber), failed (red),
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


def _prep_dots(jr: dict, *, labelled: bool) -> None:
    for key, _slabel, _jt in PREP_STAGES:
        st = jr.get(key, "pending")
        word = _STAGE_WORD.get(key, _slabel)
        with ui.element("div").classes("cb-jstage").tooltip(f"{word}: {_STATUS_WORD.get(st, st)}"):
            ui.element("div").classes(f"cb-jdot {st}")
            if labelled:
                ui.label(word).classes("cb-jstage-name")


def _species_count(s: dict | None, sp: dict) -> None:
    """One species cell: swatch · count (· kept after curation)."""
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


def build_strip(
    container,
    *,
    journey: dict,
    species_journey: dict,
    ts_names: list,
    selected_ts: str | None,
    recon_mrc_map: dict,
    on_select: Callable[[str], object],
    info_popover: Callable | None = None,
    excluded_ids: set | None = None,
    on_toggle_exclude: Callable[[str], object] | None = None,
) -> dict:
    """Build the header line for ``selected_ts`` into ``container`` (with the
    tomogram-switcher dropdown behind it); return ``{ts: row element}`` of the
    dropdown so the caller can tell which tilt series have a row.

    ``on_select(ts)`` is the row-click handler; ``info_popover(ts, species,
    recon_mrc)`` (optional) renders the ⓘ file-paths popover — passed in so this
    module never imports the shell.
    """
    species = _aggregate_species(species_journey, ts_names)
    excluded = excluded_ids or set()
    row_els: dict[str, object] = {}

    container.clear()
    with container:
        if selected_ts is None or not ts_names:
            ui.label("No tilt series yet").classes("cb-jhead-empty")
            return row_els

        sel_idx = ts_names.index(selected_ts) + 1 if selected_ts in ts_names else 0
        pos, _ = position_label(selected_ts)
        is_excl = selected_ts in excluded

        # ── The switcher: the current tomogram's name, clickable ───────────
        switch = ui.element("div").classes("cb-jswitch")
        with switch:
            ui.label("▾").classes("cb-jswitch-caret")
            ui.label(pos).classes("cb-jswitch-pos" + (" excluded" if is_excl else "")).tooltip(selected_ts)
            ui.label(f"{sel_idx} / {len(ts_names)}").classes("cb-jswitch-idx").tooltip("Switch tomogram")
            # QMenu opens on its parent's click — the whole switch block is the trigger.
            menu = ui.menu().props("anchor='bottom left' self='top left' max-height=85vh max-width=96vw")
            with menu:
                row_els.update(
                    _build_switcher_table(
                        journey, species_journey, species, ts_names, selected_ts, excluded, on_select, menu
                    )
                )

        ui.element("div").classes("cb-jsep")
        _prep_dots(journey.get(selected_ts, {}), labelled=True)

        sp_rows = species_journey.get(selected_ts, [])
        if sp_rows:
            ui.element("div").classes("cb-jsep")
            by_idx = {s["idx"]: s for s in sp_rows}
            for sp in species:
                s = by_idx.get(sp["idx"])
                if s is None:
                    continue
                with ui.element("div").classes("cb-jhead-species"):
                    ui.element("div").classes("cb-jsp-dot").style(f"background: {sp['color']};")
                    ui.label(sp["label"]).classes("cb-jsp-name")
                    _species_count(s, sp)

        if info_popover is not None:
            ui.element("div").classes("cb-jsep")
            info_popover(selected_ts, sp_rows, recon_mrc_map.get(selected_ts))
        if on_toggle_exclude is not None:
            tog = ui.icon("undo" if is_excl else "block").classes("cb-jexcl" + (" on" if is_excl else ""))
            tog.tooltip(
                "Restore this tilt series to processing"
                if is_excl
                else "Exclude this tilt series from processing (applies on the next run)"
            )
            tog.on("click", lambda t=selected_ts: on_toggle_exclude(t))
    return row_els


def _build_switcher_table(
    journey: dict,
    species_journey: dict,
    species: list[dict],
    ts_names: list,
    selected_ts: str | None,
    excluded: set,
    on_select: Callable[[str], object],
    menu,
) -> dict:
    """The dropdown: one row per tilt series with the same facts as the header line,
    plus a totals row. Clicking a row selects it and closes the menu."""
    row_els: dict[str, object] = {}
    # `repeat(0, …)` is invalid CSS and would void the whole template — only add
    # the species tracks when there are species.
    cols = "36px minmax(150px, 1fr) repeat(4, max-content)"
    if species:
        cols += f" repeat({len(species)}, minmax(84px, max-content))"
    with ui.element("div").classes("cb-jmenu").style(f"--cb-jcols: {cols};"):
        with ui.element("div").classes("cb-jrow cb-jrow-head"):
            ui.label("#").classes("cb-jh")
            ui.label("Tomogram").classes("cb-jh")
            for key, _slabel, _jt in PREP_STAGES:
                ui.label(_STAGE_WORD.get(key, _slabel)).classes("cb-jh cb-jh-c")
            for sp in species:
                with ui.element("div").classes("cb-jh cb-jh-sp").tooltip(sp["label"]):
                    ui.element("div").classes("cb-jsp-dot").style(f"background: {sp['color']};")
                    ui.label(sp["label"]).classes("cb-jh-spname")
        for i, ts in enumerate(ts_names, start=1):
            is_sel = ts == selected_ts
            is_excl = ts in excluded
            cls = "cb-jrow" + (" selected" if is_sel else "") + (" excluded" if is_excl else "")
            row = ui.element("div").classes(cls)
            row_els[ts] = row

            def _pick(_e=None, t=ts):
                menu.close()
                # `on_select` is the journey's async select_ts: hand its coroutine back
                # to NiceGUI, which awaits an awaitable handler result.
                return on_select(t)

            row.on("click", _pick)
            with row:
                ui.label(str(i)).classes("cb-jcell cb-jcell-idx")
                pos, _ = position_label(ts)
                with ui.element("div").classes("cb-jcell cb-jcell-name").tooltip(ts):
                    ui.label(pos).classes("cb-jname")
                    ui.label(ts).classes("cb-jname-full")
                _prep_dots(journey.get(ts, {}), labelled=False)
                by_idx = {s["idx"]: s for s in species_journey.get(ts, [])}
                for sp in species:
                    _species_count(by_idx.get(sp["idx"]), sp)
        with ui.element("div").classes("cb-jrow cb-jrow-total"):
            ui.label("").classes("cb-jcell")
            ui.label(f"{len(ts_names)} tilt series").classes("cb-jcell cb-jtotal-name")
            for _ in PREP_STAGES:
                ui.label("").classes("cb-jcell")
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
    return row_els

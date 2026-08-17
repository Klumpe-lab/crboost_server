"""Journey dashboard — heatmap status strip.

The strip is the Journey panel's header: a transpose of the per-TS journey data
into a compact matrix. Frozen-left rows (a ``prep`` track + one row per species,
each carrying project-wide Σ pick / Σ filtered totals) label horizontally-
scrolling tilt-series columns. Each ``prep`` cell is four status dots
(FS/Align/CTF/Recon); each species cell is that TS's pick count, shaded by how
far it progressed (picked → extracted). Clicking a column selects that TS and
drives the detail pane below; the selected column is highlighted and surfaces
its filtered count.

Pure presentation over ``services.dashboard_data``'s collectors — no new disk reads, and no
back-import into ``tomo_dashboard_dialog`` (the ⓘ info popover arrives as a
callback, keeping the package DAG one-directional). Verified by py_compile +
ruff only (no runtime test in this venv — see reference_hpc_env).
"""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

from services.dashboard_data import PREP_STAGES, position_label

# status → human word, for cell/dot tooltips. Mirrors tomo_dashboard_dialog's
# _PILL_TOOLTIP_LABEL; kept local so the strip doesn't back-import the shell.
_STATUS_WORD = {
    "ok": "done",
    "fail": "failed",
    "running": "running",
    "zero": "ran, 0 above cutoff",
    "pending": "not started",
    # Emitted by the per-TS task scanner for a tilt series excluded from processing.
    # It has always been able to reach here; without an entry the raw token showed.
    "skip": "skipped",
}


def _aggregate_species(species_journey: dict, ts_names: list) -> list[dict]:
    """Collapse the per-TS species lists into one ordered row-spec per species,
    with project-wide Σ pick / Σ filtered totals. Order + color follow the
    species ``idx`` (stable across TS), so the strip rows line up with the canvas
    overlay and the particle tabs."""
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


def _compact_col_label(ts: str) -> str:
    """Tight column header for a ~52px column. Position → ``stage·beam``;
    otherwise the trailing name segment, capped. Full name lives in the tooltip."""
    _, (stage, beam) = position_label(ts)
    if stage:
        return f"{stage}·{beam}"
    return ts.rsplit("_", 1)[-1][:6]


def _cell_class(s: dict) -> str:
    """State class for a species cell. Deliberately NOT a success-green gradient
    (picks merely existing isn't a "success") — only genuinely-stateful conditions
    get a fill: running (amber), failed (red), ran-but-zero (gray). "has" (picks
    present) is neutral. The auto-vs-curated distinction is carried by the two
    numbers — auto count slate, kept count green — not by the background."""
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


def _cell_tooltip(s: dict) -> str:
    pk = _STATUS_WORD.get(s.get("pick_status"), s.get("pick_status") or "—")
    sub = _STATUS_WORD.get(s.get("subtomo_status"), s.get("subtomo_status") or "—")
    n = s.get("n_picks")
    npart = f"{n} picks" if n is not None else "no pick count yet"
    return f"{s['label']} — pick: {pk} ({npart}); subtomo: {sub}"


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
    """Build the heatmap strip into ``container``; return ``{ts: column element}``
    so the caller can move the selection highlight without a full rebuild.

    ``on_select(ts)`` is the column-click handler; ``info_popover(ts, species,
    recon_mrc)`` (optional) renders the ⓘ file-paths popover for the selected TS
    into the frozen-left corner — passed in so this module never imports the
    shell.
    """
    species = _aggregate_species(species_journey, ts_names)
    excluded = excluded_ids or set()
    col_els: dict[str, object] = {}

    container.clear()
    with container, ui.element("div").classes("cb-strip-wrap"):
        # ── Frozen-left row labels ──────────────────────────────────────
        with ui.element("div").classes("cb-strip-left"):
            with ui.element("div").classes("cb-strip-corner"):
                ui.label(f"{len(ts_names)} TS").classes("cb-strip-corner-count")
                ui.space()
                if info_popover is not None and selected_ts is not None:
                    info_popover(selected_ts, species_journey.get(selected_ts, []), recon_mrc_map.get(selected_ts))
            with ui.element("div").classes("cb-strip-rowlabel cb-strip-prep"):
                ui.label("prep").classes("cb-strip-rl-name")
                ui.label("FS·Al·CT·Re").classes("cb-strip-rl-sub")
            for sp in species:
                with ui.element("div").classes("cb-strip-rowlabel"):
                    ui.element("div").classes("cb-strip-sp-dot").style(f"background: {sp['color']};")
                    ui.label(sp["label"]).classes("cb-strip-rl-name").tooltip(sp["label"])
                    ui.space()
                    tot = f"{sp['picks']}/{sp['filt']}" if sp["has_filt"] else str(sp["picks"])
                    ui.label(tot).classes("cb-strip-rl-sum").tooltip(
                        "Σ picks / Σ kept after curation" if sp["has_filt"] else "Σ picks across all tilt series"
                    )

        # ── Horizontally-scrolling TS columns ───────────────────────────
        with ui.element("div").classes("cb-strip-scroll"), ui.element("div").classes("cb-strip-cols"):
            for ts in ts_names:
                is_sel = ts == selected_ts
                is_excl = ts in excluded
                cls = "cb-strip-col" + (" selected" if is_sel else "") + (" excluded" if is_excl else "")
                col = ui.element("div").classes(cls)
                col.on("click", lambda t=ts: on_select(t))
                col_els[ts] = col
                with col:
                    ui.label(_compact_col_label(ts)).classes("cb-strip-colhead").tooltip(ts)
                    # Per-TS exclude/restore toggle — the mute control that is
                    # present at every stage (the strip is always on screen).
                    # click.stop so toggling doesn't also select the column.
                    if on_toggle_exclude is not None:
                        tog = ui.icon("undo" if is_excl else "block").classes(
                            "cb-strip-excl" + (" on" if is_excl else "")
                        )
                        tog.tooltip(
                            "Restore this tilt-series to processing"
                            if is_excl
                            else "Exclude this tilt-series from processing (applies on next run)"
                        )
                        tog.on("click.stop", lambda t=ts: on_toggle_exclude(t))
                    jr = journey.get(ts, {})
                    with ui.element("div").classes("cb-strip-cell cb-strip-prepcell"):
                        for key, slabel, _jt in PREP_STAGES:
                            st = jr.get(key, "pending")
                            ui.element("div").classes(f"cb-strip-dot {st}").tooltip(
                                f"{slabel}: {_STATUS_WORD.get(st, st)}"
                            )
                    by_idx = {s["idx"]: s for s in species_journey.get(ts, [])}
                    for sp in species:
                        s = by_idx.get(sp["idx"])
                        if s is None:
                            with (
                                ui.element("div")
                                .classes("cb-strip-cell cb-strip-pickcell pending")
                                .tooltip(f"{sp['label']}: not in this tilt-series")
                            ):
                                ui.label("—").classes("cb-strip-n")
                            continue
                        n = s.get("n_picks")
                        with (
                            ui.element("div")
                            .classes(f"cb-strip-cell cb-strip-pickcell {_cell_class(s)}")
                            .tooltip(_cell_tooltip(s))
                        ):
                            ui.label(str(n) if n is not None else "·").classes("cb-strip-n")
                            fc = s.get("filtered_count")
                            if fc is not None:
                                ui.label(f"▸{fc}").classes("cb-strip-filt").tooltip(f"{fc} kept after curation")
    return col_els

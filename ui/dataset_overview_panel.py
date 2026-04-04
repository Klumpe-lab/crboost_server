# ui/dataset_overview_panel.py
"""
NiceGUI component that displays a parsed cryo-ET dataset as a collapsible,
selectable table of stage positions and their tilt-series (beam positions).
"""

from typing import Callable, Dict, List, Optional

from nicegui import ui

from services.dataset_models import DatasetOverview, AcquisitionSummary
from ui.styles import MONO, SANS as FONT

CLR_HEADING = "#0f172a"
CLR_LABEL = "#475569"
CLR_SUBLABEL = "#94a3b8"
CLR_GHOST = "#cbd5e1"
CLR_BORDER = "#e2e8f0"
CLR_SUCCESS = "#0d9488"
CLR_ERROR = "#be4343"
CLR_WARN = "#d97706"
CLR_WARN_BG = "#fffbeb"
CLR_POS_BG = "#f1f5f9"

# Per-category colors for warnings and flash highlights
CATEGORY_COLORS = {
    "pixel_size": ("#7c3aed", "#ede9fe", "#f5f3ff"),  # violet
    "voltage": ("#dc2626", "#fee2e2", "#fef2f2"),  # red
    "dose_per_tilt": ("#d97706", "#fef3c7", "#fffbeb"),  # amber
    "tilt_axis": ("#0891b2", "#cffafe", "#ecfeff"),  # cyan
    "angle_range": ("#059669", "#d1fae5", "#ecfdf5"),  # emerald
}
_DEFAULT_CAT = ("#d97706", "#fef3c7", "#fffbeb")  # fallback amber

CB_STYLE = "padding: 0; margin: 0 2px;"
CB_PROPS = "dense size=xs"
CELL = "padding: 3px 5px;"
COL_WIDTHS = "24px 44px 40px 40px 110px 58px 50px 50px 50px 1fr"


def build_dataset_overview_panel(overview: DatasetOverview, on_change: Optional[Callable[[], None]] = None) -> None:
    """Render the dataset overview with collapsible positions and selection."""

    if not overview.positions:
        if overview.parse_warnings:
            for w in overview.parse_warnings:
                ui.label(w).style(f"{FONT} font-size: 10px; color: {CLR_ERROR};")
        else:
            ui.label("No positions found").style(f"{FONT} font-size: 10px; color: {CLR_GHOST}; font-style: italic;")
        return

    # Registry: ts_label -> row element (for scroll-to-highlight)
    row_registry: Dict[str, ui.element] = {}

    # --- State tracking ---
    all_pos_cbs: List[tuple] = []
    summary_label = ui.label("")
    summary_label.style(f"{MONO} font-size: 10px; color: {CLR_SUBLABEL};")

    # --- Dynamic acquisition summary bar ---
    summary_bar_container = ui.column().classes("w-full gap-0")

    def refresh_all():
        _refresh_counts()
        _refresh_summary_bar()
        if on_change:
            on_change()

    def _refresh_counts():
        sel = overview.selected_tilt_series
        tot = overview.total_tilt_series
        sf = overview.selected_frames
        tf = overview.total_frames
        summary_label.text = f"{len(overview.positions)} pos  \u00b7  {sel}/{tot} tilt-series  \u00b7  {sf}/{tf} frames"

    def _refresh_summary_bar():
        summary_bar_container.clear()
        s = overview.selected_acquisition_summary()
        with summary_bar_container:
            _build_acquisition_summary_bar(s, overview, row_registry)

    _refresh_counts()
    _refresh_summary_bar()

    # --- Header ---
    with ui.row().classes("w-full items-baseline justify-between mt-2 mb-1"):
        ui.label("Dataset Overview").style(
            f"{FONT} font-size: 12px; font-weight: 600; color: {CLR_HEADING}; letter-spacing: -0.01em;"
        )
        summary_label.move(target_index=1)

    hdr_style = (
        f"{FONT} font-size: 8px; font-weight: 500; color: {CLR_SUBLABEL}; "
        f"text-transform: uppercase; letter-spacing: 0.04em; {CELL}"
    )

    # --- Table container ---
    with (
        ui.column()
        .classes("w-full gap-0")
        .style(
            f"border: 1px solid {CLR_BORDER}; border-radius: 6px; "
            "overflow: hidden; max-height: 420px; overflow-y: auto;"
        )
    ):
        # Column headers with select-all checkbox
        with (
            ui.element("div")
            .classes("w-full")
            .style(
                f"display: grid; grid-template-columns: {COL_WIDTHS}; "
                f"border-bottom: 1px solid {CLR_BORDER}; background: #f8fafc; "
                "position: sticky; top: 0; z-index: 1;"
            )
        ):

            def on_select_all(e):
                for p, cb in all_pos_cbs:
                    p.selected = e.value
                    for ts in p.tilt_series:
                        ts.selected = e.value
                    cb.value = e.value
                refresh_all()

            (ui.checkbox(value=True, on_change=on_select_all).props(CB_PROPS).style(CB_STYLE))
            ui.label("POS").style(hdr_style)
            ui.label("BEAM").style(hdr_style)
            ui.label("TILTS").style(hdr_style)
            ui.label("ANGLE RANGE").style(hdr_style)
            ui.label("PIX (\u212b)").style(hdr_style)
            ui.label("kV").style(hdr_style)
            ui.label("DOSE").style(hdr_style)
            ui.label("AXIS").style(hdr_style)
            ui.label("MDOC").style(hdr_style)

        # Position groups
        for pos in overview.positions:
            pos_cb = _build_position_group(pos, overview, refresh_all, row_registry)
            all_pos_cbs.append((pos, pos_cb))

    # --- Parse warnings ---
    if overview.parse_warnings:
        with ui.column().classes("w-full gap-0 mt-2"):
            for w in overview.parse_warnings[:3]:
                ui.label(w).style(f"{FONT} font-size: 9px; color: {CLR_ERROR}; padding-left: 2px;")
            rest = len(overview.parse_warnings) - 3
            if rest > 0:
                ui.label(f"... +{rest} more").style(
                    f"{FONT} font-size: 9px; color: {CLR_SUBLABEL}; font-style: italic; padding-left: 2px;"
                )


def _flash_row(row_el: ui.element, highlight_color: str):
    """Scroll to a row element and flash-highlight it."""
    row_el.run_method("scrollIntoView", {"behavior": "smooth", "block": "center"})
    row_el.style(add=f"background: {highlight_color} !important; transition: background 0.3s;")
    ui.timer(1.5, lambda: row_el.style(add="background: white !important; transition: background 0.8s;"), once=True)


def _bucket_by_param(overview, param_key):
    """Group selected tilt-series by their value for the given param.

    Returns dict of rounded_value -> list of ts_label strings,
    and the majority value.
    """
    buckets: Dict[object, List[str]] = {}
    for p in overview.positions:
        for ts in p.tilt_series:
            if not ts.selected:
                continue
            if param_key == "angle_range":
                lo, hi = ts.angle_range
                val = (round(lo, 0), round(hi, 0))
            else:
                val = getattr(ts, param_key, None)
                if val is not None:
                    if param_key == "pixel_size":
                        val = round(val, 3)
                    elif param_key == "dose_per_tilt":
                        val = round(val, 1)
                    elif param_key == "tilt_axis":
                        val = round(val, 1)
                    elif param_key == "voltage":
                        val = round(val, 0)
            if val is None:
                continue
            buckets.setdefault(val, []).append(ts.ts_label)

    if len(buckets) <= 1:
        return {}, None
    majority_val = max(buckets, key=lambda k: len(buckets[k]))
    return buckets, majority_val


def _build_acquisition_summary_bar(s, overview, row_registry):
    """Top-level bar showing unique acquisition values and consistency."""
    if not s.pixel_sizes and not s.voltages:
        return

    with (
        ui.element("div")
        .classes("w-full")
        .style(f"border: 1px solid {CLR_BORDER}; border-radius: 6px; padding: 6px 10px; background: #f8fafc;")
    ):
        with ui.row().classes("w-full items-center gap-4 flex-wrap"):
            _param_chip("Pixel", s.pixel_sizes, "\u212b", ".3f", "pixel_size")
            _param_chip("Voltage", s.voltages, "kV", ".0f", "voltage")
            _param_chip("Dose/tilt", s.doses, "e\u207b/\u212b\u00b2", ".1f", "dose_per_tilt")
            _param_chip("Tilt axis", s.tilt_axes, "\u00b0", ".1f", "tilt_axis")

        warnings = s.param_warnings()
        if warnings:
            with ui.column().classes("w-full gap-1 mt-1"):
                for param_key, label, detail in warnings:
                    _build_warning_row(param_key, label, detail, overview, row_registry)
        elif s.pixel_sizes or s.voltages:
            ui.label("All selected tilt-series share consistent parameters").style(
                f"{FONT} font-size: 9px; color: {CLR_SUCCESS}; margin-top: 2px;"
            )


def _format_val(param_key, val):
    """Format a single bucket value for display."""
    if param_key == "angle_range":
        lo, hi = val
        return f"[{lo:+.0f}\u00b0..{hi:+.0f}\u00b0]"
    if param_key == "pixel_size":
        return f"{val:.3f} \u212b"
    if param_key == "voltage":
        return f"{val:.0f} kV"
    if param_key == "dose_per_tilt":
        return f"{val:.1f} e\u207b/\u212b\u00b2"
    if param_key == "tilt_axis":
        return f"{val:.1f}\u00b0"
    return str(val)


def _build_warning_row(param_key, label, _detail, overview, row_registry):
    """Warning with per-value breakdown; each tilt-series is a clickable link."""
    cat_clr, cat_highlight, cat_bg = CATEGORY_COLORS.get(param_key, _DEFAULT_CAT)
    buckets, majority_val = _bucket_by_param(overview, param_key)
    if not buckets:
        return

    with (
        ui.column()
        .classes("w-full gap-1 py-1.5 px-2 rounded")
        .style(f"background: {cat_bg}; border: 1px solid {cat_highlight};")
    ):
        # Header line
        with ui.row().classes("items-center gap-1.5"):
            ui.icon("warning_amber", size="14px").style(f"color: {cat_clr};")
            ui.label(label).style(f"{FONT} font-size: 10px; font-weight: 600; color: {cat_clr};")
            majority_str = _format_val(param_key, majority_val)
            n_majority = len(buckets.get(majority_val, []))
            ui.label(f"majority: {majority_str} ({n_majority})").style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL};")

        # Each non-majority value with its tilt-series links
        for val, ts_labels in sorted(buckets.items(), key=lambda kv: len(kv[1])):
            if val == majority_val:
                continue
            val_str = _format_val(param_key, val)
            with ui.row().classes("items-center gap-1 pl-5 flex-wrap"):
                ui.label(val_str).style(f"{MONO} font-size: 10px; font-weight: 600; color: {cat_clr};")
                ui.label("\u2192").style(f"font-size: 9px; color: {CLR_SUBLABEL};")
                for ts_label in ts_labels:

                    def _make_click(lbl=ts_label):
                        def handler():
                            el = row_registry.get(lbl)
                            if el:
                                _flash_row(el, cat_highlight)

                        return handler

                    (
                        ui.button(ts_label, on_click=_make_click())
                        .props("flat dense no-caps")
                        .style(
                            f"{MONO} font-size: 9px; color: {cat_clr}; "
                            "padding: 0 3px; min-height: 0; "
                            "text-decoration: underline; "
                            "text-underline-offset: 2px;"
                        )
                    )


def _param_chip(label, values, unit, fmt=".2f", category=None):
    """Render a small label+value chip for a parameter."""
    if not values:
        return
    mixed = len(values) > 1
    cat_clr = CATEGORY_COLORS.get(category, _DEFAULT_CAT)[0] if mixed else CLR_HEADING
    with ui.row().classes("items-baseline gap-0.5"):
        ui.label(label).style(
            f"{FONT} font-size: 8px; color: {CLR_SUBLABEL}; text-transform: uppercase; letter-spacing: 0.03em;"
        )
        vals = ", ".join(f"{v:{fmt}}" for v in values)
        ui.label(vals).style(f"{MONO} font-size: 11px; font-weight: 600; color: {cat_clr};")
        if unit:
            ui.label(unit).style(f"{FONT} font-size: 8px; color: {CLR_SUBLABEL};")


def _build_position_group(pos, overview, refresh_all, row_registry):
    """Build a collapsible position header + tilt-series rows. Returns pos checkbox."""
    ts_container_ref = [None]
    expanded_ref = [True]
    chevron_ref = [None]
    ts_checkboxes = []
    pos_cb_ref = [None]

    def on_pos_checkbox(e):
        pos.selected = e.value
        for ts in pos.tilt_series:
            ts.selected = e.value
        for cb in ts_checkboxes:
            cb.value = e.value
        refresh_all()

    def on_ts_checkbox(ts, e):
        ts.selected = e.value
        all_sel = all(t.selected for t in pos.tilt_series)
        none_sel = not any(t.selected for t in pos.tilt_series)
        if pos_cb_ref[0]:
            pos_cb_ref[0].value = all_sel
        pos.selected = not none_sel
        refresh_all()

    def toggle_expand():
        expanded_ref[0] = not expanded_ref[0]
        if ts_container_ref[0]:
            ts_container_ref[0].set_visibility(expanded_ref[0])
        if chevron_ref[0]:
            chevron_ref[0].text = "expand_more" if expanded_ref[0] else "chevron_right"

    n_beams = pos.beam_count
    n_tilts = pos.total_tilts
    a_min = min(ts.angle_range[0] for ts in pos.tilt_series)
    a_max = max(ts.angle_range[1] for ts in pos.tilt_series)
    angle_str = f"{a_min:+.1f}\u00b0..{a_max:+.1f}\u00b0"

    # --- Position header row ---
    with (
        ui.element("div")
        .classes("w-full")
        .style(
            f"display: grid; grid-template-columns: {COL_WIDTHS}; "
            f"background: {CLR_POS_BG}; "
            f"border-bottom: 1px solid {CLR_BORDER}; cursor: pointer;"
        )
        .on("click", toggle_expand)
    ):
        pos_cb = (
            ui.checkbox(value=pos.selected, on_change=on_pos_checkbox)
            .props(CB_PROPS)
            .style(CB_STYLE)
            .on("click.stop", lambda: None)
        )
        pos_cb_ref[0] = pos_cb

        with ui.row().classes("items-center gap-0.5").style(f"{CELL}"):
            chevron = ui.icon("expand_more", size="12px").style(f"color: {CLR_SUBLABEL};")
            chevron_ref[0] = chevron
            ui.label(str(pos.stage_position)).style(f"{MONO} font-size: 11px; font-weight: 600; color: {CLR_HEADING};")

        ui.label(f"{n_beams}").style(f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; {CELL}")
        ui.label(f"{n_tilts}").style(f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; {CELL}")
        ui.label(angle_str).style(f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; {CELL}")
        for _ in range(5):
            ui.label("").style(CELL)

    # --- Tilt-series child rows ---
    ts_container = ui.column().classes("w-full gap-0")
    ts_container_ref[0] = ts_container

    with ts_container:
        for ts_idx, ts in enumerate(pos.tilt_series):
            is_last = ts_idx == len(pos.tilt_series) - 1
            border = "" if is_last else f"border-bottom: 1px solid {CLR_BORDER};"
            a_lo, a_hi = ts.angle_range
            ts_angle = f"{a_lo:+.1f}\u00b0..{a_hi:+.1f}\u00b0"

            row_el = (
                ui.element("div")
                .classes("w-full")
                .style(f"display: grid; grid-template-columns: {COL_WIDTHS}; background: white; {border}")
            )
            row_registry[ts.ts_label] = row_el

            with row_el:
                cb = (
                    ui.checkbox(value=ts.selected, on_change=lambda e, t=ts: on_ts_checkbox(t, e))
                    .props(CB_PROPS)
                    .style(CB_STYLE)
                )
                ts_checkboxes.append(cb)

                ui.label("").style(CELL)

                ui.label(str(ts.beam_position)).style(f"{MONO} font-size: 10px; color: {CLR_LABEL}; {CELL}")

                tc_style = f"{MONO} font-size: 10px; {CELL}"
                if ts.missing_frames > 0:
                    tc_style += f" color: {CLR_ERROR};"
                    tc_text = f"{ts.tilt_count}({ts.missing_frames}?)"
                else:
                    tc_style += f" color: {CLR_LABEL};"
                    tc_text = str(ts.tilt_count)
                ui.label(tc_text).style(tc_style)

                ui.label(ts_angle).style(f"{MONO} font-size: 10px; color: {CLR_LABEL}; {CELL}")

                _acq_cell(ts.pixel_size, ".3f")
                _acq_cell(ts.voltage, ".0f")
                _acq_cell(ts.dose_per_tilt, ".1f")
                _acq_cell(ts.tilt_axis, ".1f")

                ui.label(ts.mdoc_filename).style(
                    f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; {CELL} "
                    "overflow: hidden; text-overflow: ellipsis; "
                    "white-space: nowrap;"
                )

    if pos.beam_count > 6:
        expanded_ref[0] = False
        ts_container.set_visibility(False)
        if chevron_ref[0]:
            chevron_ref[0].text = "chevron_right"

    return pos_cb


def _acq_cell(value, fmt):
    if value is not None:
        ui.label(f"{value:{fmt}}").style(f"{MONO} font-size: 10px; color: {CLR_LABEL}; {CELL}")
    else:
        ui.label("\u2014").style(f"{MONO} font-size: 10px; color: {CLR_GHOST}; {CELL}")


def build_dry_run_summary(overview: DatasetOverview) -> None:
    """Render a summary of what the import will do, with data safety info."""
    selected = overview.get_selected_tilt_series()
    total_ts = overview.total_tilt_series
    total_frames = overview.total_frames
    sel_frames = overview.selected_frames
    excluded_ts = total_ts - len(selected)
    excluded_frames = total_frames - sel_frames

    with ui.column().classes("w-full gap-2"):
        ui.label("Import Preview").style(f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING};")

        with ui.column().classes("w-full gap-0"):
            ui.label(f"{len(selected)} tilt-series  \u00b7  {sel_frames} frames will be included").style(
                f"{MONO} font-size: 11px; color: {CLR_SUCCESS};"
            )
            if excluded_ts > 0:
                ui.label(f"{excluded_ts} tilt-series  \u00b7  {excluded_frames} frames excluded").style(
                    f"{MONO} font-size: 11px; color: {CLR_SUBLABEL};"
                )

        sel_summary = overview.selected_acquisition_summary()
        for _, label, detail in sel_summary.param_warnings():
            ui.label(f"\u26a0 {label}: {detail}").style(f"{FONT} font-size: 10px; font-weight: 500; color: {CLR_WARN};")
        if not sel_summary.param_warnings() and sel_summary.pixel_sizes:
            ui.label("Selected tilt-series have consistent parameters").style(
                f"{FONT} font-size: 10px; color: {CLR_SUCCESS};"
            )

        with (
            ui.column()
            .classes("w-full gap-0")
            .style(
                f"border: 1px solid {CLR_BORDER}; border-radius: 6px; "
                "max-height: 200px; overflow-y: auto; background: #f8fafc;"
            )
        ):
            for ts in selected:
                ui.label(f"  {ts.ts_label}  ({ts.tilt_count} tilts)").style(
                    f"{MONO} font-size: 10px; color: {CLR_LABEL}; padding: 2px 8px;"
                )

        # Data safety guarantee
        with (
            ui.column()
            .classes("w-full gap-1")
            .style(f"border: 1px solid {CLR_BORDER}; border-radius: 6px; padding: 10px 12px; background: #f0fdf4;")
        ):
            with ui.row().classes("items-center gap-2"):
                ui.icon("verified_user", size="16px").style("color: #16a34a;")
                ui.label("Your original data will not be modified").style(
                    f"{FONT} font-size: 11px; font-weight: 600; color: #15803d;"
                )
            ui.label(
                "CryoBoost creates symbolic links (symlinks) pointing to your "
                "original frame files. The raw .eer/.mrc/.tiff files are never "
                "copied, moved, or modified. Mdoc files are copied into the "
                "project directory with updated paths, but the originals remain "
                "untouched. All downstream processing operates on symlinked copies."
            ).style(f"{FONT} font-size: 10px; color: #166534; line-height: 1.5;")

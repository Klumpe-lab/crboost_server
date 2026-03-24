"""
FsMotionCtf plugin -- custom parameter layout.

Groups parameters by their physical meaning:
  - Motion estimation
  - CTF estimation
  - Output control
  - Processing
"""

from typing import Callable

from nicegui import ui

from services.models_base import JobType
from ui.job_plugins import register_params_renderer


def _field(
    label: str,
    job_model,
    attr: str,
    *,
    width: str = "10ch",
    suffix: str = "",
    hint: str = "",
    is_frozen: bool = False,
    save_handler: Callable,
    is_number: bool = False,
):

    with ui.column().classes("gap-0"):
        lbl = (
            ui.label(label)
            .classes("text-xs text-gray-500")
            .style("font-family: 'IBM Plex Sans', sans-serif; font-weight: 500; letter-spacing: 0.01em;")
        )
        if hint:
            lbl.tooltip(hint)

        with ui.row().classes("items-baseline gap-1"):
            if is_number:
                val = getattr(job_model, attr)
                inp = ui.number(value=val, format="%.4g").bind_value(job_model, attr)
            else:
                inp = ui.input().bind_value(job_model, attr)

            inp.props("dense borderless hide-bottom-space")
            inp.style(
                f"width: {width}; "
                "font-family: 'IBM Plex Mono', monospace; "
                "font-size: 13px; "
                "border-bottom: 1.5px solid #d1d5db; "
                "border-radius: 0; "
                "padding: 1px 2px 2px 2px; "
                "line-height: 1.4;"
            )

            if is_frozen:
                inp.props("readonly").style("color: #9ca3af;")
            else:
                inp.on_value_change(save_handler)

            if suffix:
                ui.label(suffix).style(
                    "font-family: 'IBM Plex Sans', sans-serif; font-size: 11px; color: #9ca3af; margin-left: 2px;"
                )


def _toggle(label: str, job_model, attr: str, *, hint: str = "", is_frozen: bool = False, save_handler: Callable):
    cb = ui.checkbox(label).bind_value(job_model, attr).props("dense")
    cb.style("font-family: 'IBM Plex Sans', sans-serif; font-size: 12px; color: #374151;")
    if hint:
        cb.tooltip(hint)
    if is_frozen:
        cb.disable()
    else:
        cb.on_value_change(save_handler)


def _section(text: str):
    ui.label(text).style(
        "font-family: 'IBM Plex Sans', sans-serif; "
        "font-size: 13px; "
        "font-weight: 600; "
        "color: #1f2937; "
        "letter-spacing: 0.02em;"
    )


def _rule():
    ui.element("div").style("width: 100%; height: 1px; background: #e5e7eb; margin: 6px 0 2px 0;")


@register_params_renderer(JobType.FS_MOTION_CTF)
def render_fs_motion_ctf_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None):
    ctx = dict(job_model=job_model, is_frozen=is_frozen, save_handler=save_handler)

    with ui.card().classes("w-full border border-gray-200 shadow-sm bg-white").style("padding: 16px 20px;"):
        # ── Motion Estimation ──────────────────────────────
        _section("Motion Estimation")
        with ui.row().classes("w-full items-end gap-x-8 gap-y-2 mt-1 mb-1"):
            _field(
                "Range",
                **ctx,
                attr="m_range_min_max",
                width="9ch",
                suffix="A",
                hint="Motion estimation range min:max in Angstroms",
            )
            _field(
                "B-factor",
                **ctx,
                attr="m_bfac",
                width="7ch",
                is_number=True,
                hint="Smoothing B-factor (negative = more smoothing)",
            )
            _field("Grid", **ctx, attr="m_grid", width="7ch", hint="Motion estimation grid XxYxZ")

        _rule()

        # ── CTF Estimation ─────────────────────────────────
        # ── CTF Estimation ─────────────────────────────────
        _section("CTF Estimation")
        with ui.row().classes("w-full items-end gap-x-8 gap-y-2 mt-1"):
            _field(
                "Resolution range",
                **ctx,
                attr="c_range_min_max",
                width="9ch",
                suffix="A",
                hint="CTF fitting resolution range min:max in Angstroms",
            )
            _field(
                "Defocus range",
                **ctx,
                attr="c_defocus_min_max",
                width="9ch",
                suffix="um",
                hint="Defocus search range min:max in microns",
            )
            _field("Grid", **ctx, attr="c_grid", width="7ch", hint="CTF estimation grid XxYxZ")
            _field(
                "Window",
                **ctx,
                attr="c_window",
                width="6ch",
                suffix="px",
                is_number=True,
                hint="FFT window size for CTF estimation",
            )
            _toggle("Use frame sum", **ctx, attr="c_use_sum", hint="Use summed frames for CTF estimation")
            _toggle("Do phase", **ctx, attr="do_phase", hint="Estimate phase shifts (CTF phase plate or spurious phase)")

        _rule()

        # ── Output ─────────────────────────────────────────
        _section("Output")
        with ui.row().classes("w-full items-end gap-x-8 gap-y-2 mt-1"):
            _field(
                "Skip first",
                **ctx,
                attr="out_skip_first",
                width="5ch",
                suffix="tilts",
                is_number=True,
                hint="Skip this many initial tilts",
            )
            _field(
                "Skip last",
                **ctx,
                attr="out_skip_last",
                width="5ch",
                suffix="tilts",
                is_number=True,
                hint="Skip this many final tilts",
            )
            _toggle(
                "Average halves",
                **ctx,
                attr="out_average_halves",
                hint="Output half-set averages for independent validation",
            )

        _rule()

        # ── Processing ─────────────────────────────────────
        _section("Processing")
        with ui.row().classes("w-full items-end gap-x-8 gap-y-2 mt-1"):
            _field(
                "Per GPU",
                **ctx,
                attr="perdevice",
                width="5ch",
                is_number=True,
                hint="Parallel tilt series per GPU device",
            )
            _field("Max series", **ctx, attr="do_at_most", width="6ch", is_number=True, hint="-1 = process all")
            _field(
                "Gain ops",
                **ctx,
                attr="gain_operations",
                width="14ch",
                hint="Gain reference operations (e.g. flip, rotate)",
            )

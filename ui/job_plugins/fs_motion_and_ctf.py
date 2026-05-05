"""
FsMotionCtf plugin -- custom parameter layout.

Groups parameters by their physical meaning (Motion, CTF, Output,
Processing). Each group renders into the shared cb-field grid so the look
matches the rest of the job pages.
"""

from ui.job_plugins import register_params_renderer
from ui.job_plugins._field_styles import (
    field_grid,
    section_header,
    toggle_row,
    text_field,
    numeric_field,
    toggle_field,
)
from services.models_base import JobType


@register_params_renderer(JobType.FS_MOTION_CTF)
def render_fs_motion_ctf_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None, **_ctx):
    common = dict(job_model=job_model, is_frozen=is_frozen, save_handler=save_handler)

    # ── Motion Estimation ───────────────────────────────────────────────
    section_header("Motion Estimation")
    with field_grid():
        text_field("Range", attr="m_range_min_max",
                   suffix="A", hint="Motion estimation range min:max in Angstroms", **common)
        numeric_field("B-factor", attr="m_bfac",
                      hint="Smoothing B-factor (negative = more smoothing)", **common)
        text_field("Grid", attr="m_grid", hint="Motion estimation grid XxYxZ", **common)

    # ── CTF Estimation ──────────────────────────────────────────────────
    section_header("CTF Estimation")
    with field_grid():
        text_field("Resolution range", attr="c_range_min_max",
                   suffix="A", hint="CTF fitting resolution range min:max in Angstroms", **common)
        text_field("Defocus range", attr="c_defocus_min_max",
                   suffix="um", hint="Defocus search range min:max in microns", **common)
        text_field("Grid", attr="c_grid", hint="CTF estimation grid XxYxZ", **common)
        numeric_field("Window", attr="c_window",
                      suffix="px", hint="FFT window size for CTF estimation", **common)
    with toggle_row():
        toggle_field("Use frame sum", attr="c_use_sum",
                     hint="Use summed frames for CTF estimation", **common)
        toggle_field("Estimate phase shifts", attr="do_phase",
                     hint="Estimate phase shifts (CTF phase plate or spurious phase)", **common)

    # ── Output ──────────────────────────────────────────────────────────
    section_header("Output")
    with field_grid():
        numeric_field("Skip first", attr="out_skip_first",
                      suffix="tilts", hint="Skip this many initial tilts", **common)
        numeric_field("Skip last", attr="out_skip_last",
                      suffix="tilts", hint="Skip this many final tilts", **common)
    with toggle_row():
        toggle_field("Average halves", attr="out_average_halves",
                     hint="Output half-set averages for independent validation", **common)

    # ── Processing ──────────────────────────────────────────────────────
    section_header("Processing")
    with field_grid():
        numeric_field("Per GPU", attr="perdevice",
                      hint="Parallel tilt series per GPU device", **common)
        numeric_field("Max series", attr="do_at_most",
                      hint="-1 = process all", **common)
        text_field("Gain ops", attr="gain_operations",
                   hint="Gain reference operations (e.g. flip, rotate)", **common)

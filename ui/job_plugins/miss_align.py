"""
missAlign plugin: the default parameter form with a live summary above it of what the settings
resolve to — the pixel size and patch field of view of every macro-iteration on the stacks the job
will read — and of the settings that defeat themselves (an epoch cap at or below the last LR
milestone, an unreadable schedule, milestone list or z_box, more training GPUs than allocated, a
macro-iteration longer than one run's wall-time). The driver checks the same things again before
it spends GPU time.
"""

from nicegui import ui

from services.jobs.miss_align import ScheduleEntry, parse_z_box
from services.models_base import JobType
from ui.job_plugins import register_params_renderer
from ui.job_plugins._field_styles import CLR_HEADER, HELPER_STYLE, MONO
from ui.job_plugins.default_renderer import render_config_preamble, render_default_params

_WARN_STYLE = HELPER_STYLE + " color: #b45309;"
_ROW_STYLE = f"{MONO} font-size: 10px; color: {CLR_HEADER}; white-space: pre;"


@register_params_renderer(JobType.MISS_ALIGN)
def render_miss_align_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None, **ctx):
    render_config_preamble(job_model)

    @ui.refreshable
    def summary():
        _render_summary(job_model)

    summary()

    def save_and_refresh():
        save_handler()
        summary.refresh()

    render_default_params(job_type, job_model, is_frozen, save_and_refresh, exclude=ctx.get("exclude"))


def _groups(entries: tuple[ScheduleEntry, ...]) -> list[tuple[int, int, ScheduleEntry]]:
    """Consecutive identical entries as (first iteration, last iteration, entry), 1-based."""
    groups: list[tuple[int, int, ScheduleEntry]] = []
    for i, entry in enumerate(entries, 1):
        if groups and groups[-1][2] == entry:
            groups[-1] = (groups[-1][0], i, entry)
        else:
            groups.append((i, i, entry))
    return groups


def _render_summary(job_model) -> None:
    warnings: list[str] = []
    try:
        entries = job_model.schedule()
    except ValueError as e:
        entries = ()
        warnings.append(f"Schedule: {e}")
    stack_apix = job_model.planned_stack_apix()

    with ui.column().classes("w-full").style("gap: 1px; margin-bottom: 8px;"):
        if entries:
            if stack_apix:
                source = "rebuilt from the averages" if job_model.prepare_stacks_apix > 0 else "the alignment job's"
                ui.label(f"{len(entries)} macro-iterations on {stack_apix:.4g} Å/px stacks ({source})").style(
                    HELPER_STYLE
                )
            else:
                ui.label(f"{len(entries)} macro-iterations; pixel sizes resolve when the job reads the stacks").style(
                    HELPER_STYLE
                )
            for first, last, entry in _groups(entries):
                span = str(first) if first == last else f"{first}–{last}"
                text = f"{span:>5}  {entry!s:<18}"
                note = None
                if stack_apix:
                    pixel = entry.pixel_angstrom(stack_apix)
                    text += (
                        f"ds{entry.effective_downsample(stack_apix)} · {pixel:.4g} Å · "
                        f"patch {job_model.patch_size * pixel:.0f} Å"
                    )
                    note = entry.rounding_note(stack_apix)
                with ui.row().classes("items-baseline gap-2").style("flex-wrap: nowrap;"):
                    ui.label(text).style(_ROW_STYLE)
                    if note:
                        ui.label(note).style(_WARN_STYLE)

        try:
            job_model.milestones()
        except ValueError as e:
            warnings.append(str(e))
        cap = job_model.epoch_cap_warning()
        if cap:
            warnings.append(cap)
        try:
            z_box = parse_z_box(job_model.z_box)
            if z_box != "full" and any(e.is_volume_warp for e in entries):
                warnings.append("A [X,Y,Z,T] volume-warp entry needs z_box=full: that grid is laid over the box.")
        except ValueError as e:
            warnings.append(str(e))
        if job_model.training_gpus > job_model.num_gpus:
            warnings.append(f"training_gpus ({job_model.training_gpus}) exceeds num_gpus ({job_model.num_gpus}).")
        walltime = job_model.walltime_warning()
        if walltime:
            warnings.append(walltime)
        for text in warnings:
            ui.label(text).style(_WARN_STYLE)

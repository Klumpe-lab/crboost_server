"""Candidate Extraction ("Pick candidates") plugin — peak picking on the TM score
volumes; no volume is cut here (that is subtomo extraction).

IMOD model generation, MIP previews, and 3dmod copy commands live inside
the unified Tomogram Dashboard's Candidate Extract section card
(ui/tomo_dashboard_dialog.py), since they operate across all extract
instances and want to be inspectable without first navigating to a specific
job tab.

Species facts (Ø, symmetry, templates) live on the species itself, not here.
particle_diameter_ang on this job is a per-job override, in the species group
of its parameters; new projects should leave it at the species' value and edit
species.diameter_ang via the workbench.
"""

from nicegui import ui

from services.models_base import JobType
from ui.job_plugins import register_params_renderer
from ui.job_plugins._field_styles import FIELD_ROW_STYLE, _label, _select_chrome, _unit
from ui.job_plugins.default_renderer import render_default_params_card

# score_filter_method is a string field in the param class — keep it that
# way (legacy persisted values stay valid), but expose only the two values
# the driver actually understands.
SCORE_FILTER_CHOICES = ["None", "tophat"]
_SCORE_FILTER_HINT = "Optional post-extract filter. 'tophat' applies a tophat morphological filter on the score map."


@register_params_renderer(JobType.TEMPLATE_EXTRACT_PYTOM)
def render_candidate_extract_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    """The default parameters; the strategy shows only its own threshold (`shows_param`), and the score filter is a
    choice of the two filters the driver knows."""
    custom = {
        "score_filter_method": lambda f: _string_select(
            f.title,
            job_model=job_model,
            attr=f.name,
            choices=SCORE_FILTER_CHOICES,
            is_frozen=is_frozen,
            save_handler=save_handler,
            hint=_SCORE_FILTER_HINT,
            width=f.width,
        )
    }
    return render_default_params_card(job_type, job_model, is_frozen, save_handler, ui_mgr=ui_mgr, custom=custom, **ctx)


def _string_select(label, *, job_model, attr, choices, is_frozen, save_handler, hint=None, width="M"):
    """A select for a plain-string field with a fixed choice set, in the field row's geometry."""
    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(label, hint)
        current = getattr(job_model, attr, choices[0]) or choices[0]
        if current not in choices:
            choices = [current, *choices]
        sel = ui.select(options=choices, value=current).bind_value(job_model, attr)
        _select_chrome(sel, width)
        if is_frozen:
            sel.disable()
        else:
            sel.on_value_change(lambda _e: save_handler())
        _unit(width, "")
        return sel

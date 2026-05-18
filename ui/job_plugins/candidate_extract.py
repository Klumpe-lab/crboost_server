"""Candidate Extraction plugin.

IMOD model generation, MIP previews, and 3dmod copy commands live inside
the unified Tomogram Dashboard's Candidate Extract section card
(ui/tomo_dashboard_dialog.py), since they operate across all extract
instances and want to be inspectable without first navigating to a specific
job tab.

The v2 template summary card at the top surfaces the species's particle
metadata (diameter, symmetry) the user is picking for. particle_diameter_ang
on this job remains as a per-job override; new v2 projects should leave
it at the default and edit species.diameter_ang via the workbench.
"""

from nicegui import ui

from services.jobs._base import ExtractionCutoffMethod
from services.models_base import JobType
from services.project_state import get_project_state_for
from services.templating.template_metadata import resolve_species_from_job
from ui.components.template_summary_card import render_template_summary_card
from ui.job_plugins import register_params_renderer
from ui.job_plugins._field_styles import (
    LABEL_STYLE,
    ROW_STYLE,
    enum_field,
    field_grid,
    numeric_field,
    section_header,
    text_field,
)

# score_filter_method is a string field in the param class — keep it that
# way (legacy persisted values stay valid), but expose only the two values
# the driver actually understands.
SCORE_FILTER_CHOICES = ["None", "tophat"]


@register_params_renderer(JobType.TEMPLATE_EXTRACT_PYTOM)
def render_candidate_extract_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    instance_id = ctx.get("instance_id")
    species = None
    if ui_mgr and ui_mgr.project_path:
        state = get_project_state_for(ui_mgr.project_path)
        species, _ = resolve_species_from_job(state, job_model, instance_id)

    if species is not None:
        render_template_summary_card(species)
    else:
        with ui.card().classes("w-full border border-dashed border-amber-300 bg-amber-50 mt-1"):
            with ui.row().classes("w-full items-center px-3 py-2 gap-2"):
                ui.icon("warning", size="14px").classes("text-amber-600")
                ui.label("No species linked to this job").classes("text-xs text-amber-800 font-semibold")
            with ui.column().classes("w-full px-3 pb-2 gap-1"):
                ui.label(
                    "Without a species link the candidate-extract summary can't be shown. "
                    "Assign a species or use a `__<species_id>` instance suffix."
                ).classes("text-[11px] text-amber-700")

    common = dict(job_model=job_model, is_frozen=is_frozen, save_handler=save_handler)

    # ── Pick threshold ──────────────────────────────────────────────────
    # The strategy dropdown swaps which cutoff input we show. The two
    # strategies have INCOMPARABLE scales (CC ∈ [0,1] vs FP count typically
    # 1–100), so they live in separate backing fields (cc_threshold and
    # expected_false_positives) and we only render the one that matches the
    # current strategy. Flipping the strategy preserves your value for the
    # OTHER strategy — no value-stomping.
    section_header("Pick threshold", first=True)
    with field_grid():
        # Strategy change must re-render the per-strategy cutoff field below.
        def _on_strategy_change():
            save_handler()
            _render_cutoff_for_strategy()

        enum_field(
            "Strategy",
            attr="cutoff_method",
            enum_type=ExtractionCutoffMethod,
            hint=(
                "Manual = raw LCC threshold (e.g. 0.10). "
                "False-positives = statistical: PyTOM fits noise distribution and "
                "picks the threshold giving N expected FPs per tomogram."
            ),
            job_model=job_model,
            is_frozen=is_frozen,
            save_handler=_on_strategy_change,
        )

        cutoff_container = ui.element("div").style("width: 100%;")

        def _render_cutoff_for_strategy():
            cutoff_container.clear()
            method_raw = getattr(job_model, "cutoff_method", ExtractionCutoffMethod.FALSE_POSITIVES)
            method_str = method_raw.value if hasattr(method_raw, "value") else str(method_raw)
            with cutoff_container:
                if method_str == ExtractionCutoffMethod.MANUAL.value:
                    numeric_field(
                        "LCC threshold",
                        attr="cc_threshold",
                        hint=(
                            "Local cross-correlation threshold. Keep peaks with score ≥ this. "
                            "LCC is bounded [0, 1]; typical real-data peaks land 0.05–0.20."
                        ),
                        **common,
                    )
                else:
                    numeric_field(
                        "Expected FPs / TS",
                        attr="expected_false_positives",
                        hint=(
                            "Expected false positives per tomogram. PyTOM fits the score-map "
                            "noise distribution and picks the threshold giving this many FPs. "
                            "Strict: 1. Moderate: 10. Loose: 100."
                        ),
                        **common,
                    )

        _render_cutoff_for_strategy()

        numeric_field(
            "Max picks / TS",
            attr="max_num_particles",
            hint="Hard cap on candidates per tilt-series after the cutoff is applied.",
            **common,
        )

    # ── Particle geometry ───────────────────────────────────────────────
    # particle_diameter_ang also drives non-maximum suppression spacing
    # between picks, not just bookkeeping.
    section_header("Particle geometry")
    with field_grid():
        numeric_field(
            "Diameter",
            attr="particle_diameter_ang",
            suffix="Å",
            hint="Particle diameter — also sets NMS minimum distance between picks.",
            **common,
        )

    # ── Advanced ────────────────────────────────────────────────────────
    section_header("Advanced")
    with field_grid():
        text_field(
            "Score-map apix",
            attr="apix_score_map",
            suffix="Å/px",
            hint="Pixel size of the score map. 'auto' reads from tomograms.star (recommended).",
            **common,
        )
        _string_select(
            "Score filter",
            attr="score_filter_method",
            choices=SCORE_FILTER_CHOICES,
            hint="Optional post-extract filter. 'tophat' applies a tophat morphological filter on the score map.",
            **common,
        )
        text_field(
            "Score filter val",
            attr="score_filter_value",
            hint="For tophat: '<connectivity>:<bins>' (e.g. '1:5'). Leave 'None' otherwise.",
            **common,
        )
        numeric_field(
            "Array throttle",
            attr="array_throttle",
            hint="Max concurrent SLURM array tasks (per-tomogram extracts).",
            **common,
        )


def _string_select(label, *, job_model, attr, choices, is_frozen, save_handler, hint=None):
    """Inline select for plain-string fields with a fixed choice set
    (e.g. score_filter_method). Mirrors enum_field's row layout."""
    with ui.element("div").style(ROW_STYLE):
        lbl = ui.label(label).style(LABEL_STYLE)
        if hint:
            lbl.tooltip(hint)
        current = getattr(job_model, attr, choices[0]) or choices[0]
        if current not in choices:
            choices = [current, *choices]
        sel = ui.select(options=choices, value=current).bind_value(job_model, attr)
        sel.props("dense borderless hide-bottom-space")
        sel.style("font-family: 'IBM Plex Mono', monospace; font-size: 11px; flex: 1 1 0; min-width: 0;")
        if is_frozen:
            sel.disable()
        else:
            sel.on_value_change(lambda _e: save_handler())
        return sel

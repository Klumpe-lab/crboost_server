"""
Generic (default) parameter renderer.

Auto-groups fields by their underlying type, then renders each group with
the shared `cb-field*` visual language: inline label/value pairs in an
auto-packing CSS grid, no underline-only inputs, font sizes congruent with
the toolbar / pipeline roster.
"""

from collections.abc import Callable
from pathlib import Path

from nicegui import ui

from services.models_base import JobType
from ui.components.species_pill import render_species_line, species_opener
from ui.job_plugins._field_styles import (
    field_grid,
    toggle_row,
    text_field,
    numeric_field,
    enum_field,
    toggle_field,
    path_row,
    is_enum_type,
    SANS,
    CLR_SUBLABEL,
)

BASE_FIELDS: set[str] = {
    "execution_status",
    "relion_job_name",
    "relion_job_number",
    "paths",
    "additional_binds",
    "slurm_overrides",
    "source_overrides",
    "is_orphaned",
    "missing_inputs",
    "JOB_CATEGORY",
    "workbench",
    "additional_sources",
    "merge_only",
}


def _get_description(job_model, param_name: str) -> str | None:
    field_info = job_model.model_fields.get(param_name)
    if field_info and field_info.description:
        return field_info.description
    return None


def _is_pathlike(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in ("path", "dir", "glob", "pattern", "file", "folder"))


def _classify_fields(job_model, field_names: set[str]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {"paths": [], "numeric": [], "text": [], "enum": [], "toggle": []}

    for name in sorted(field_names):
        value = getattr(job_model, name)
        field_info = job_model.model_fields.get(name)
        field_type = field_info.annotation if field_info else None

        if isinstance(value, bool):
            groups["toggle"].append(name)
        elif _is_pathlike(name):
            groups["paths"].append(name)
        elif isinstance(value, (int, float)) or value is None:
            groups["numeric"].append(name)
        elif isinstance(value, str):
            if is_enum_type(field_type):
                groups["enum"].append(name)
            else:
                groups["text"].append(name)

    return groups


# ──────────────────────────────────────────────────────────────────────────
# Species badge -- shared by default renderer and custom plugins
# ──────────────────────────────────────────────────────────────────────────


def render_species_badge(job_model, project_path: Path | None, *, callbacks: dict | None = None):
    """Species line at the top of a particle-phase job config on the DEFAULT render
    path (reconstruct / class3d / …): the shared pill + "open in Species" link.
    No-op for jobs without a `species_id`. The TM / pick-candidates / subtomo
    plugins render the same line themselves (via `resolve_species`)."""
    species_id = getattr(job_model, "species_id", None)
    if not species_id or not project_path:
        return
    from services.project_state import get_project_state_for

    species = get_project_state_for(project_path).get_species(species_id)
    if species is None:
        return
    render_species_line(species, on_open=species_opener(callbacks, species.id))


def render_denoise_inheritance(job_model, project_path: Path | None):
    """Read-only row for denoise-predict: the denoiser (cryoCARE / IsoNet) and its deconv
    setting are inherited from the denoise-train job — predict has no independent setting,
    so a train/predict mismatch is impossible. Shows the resolved method when available."""
    method = None
    if project_path:
        from services.project_state import get_project_state_for

        # inherited_from_train returns (None, None) when it can't resolve — no except needed.
        method, _ = job_model.inherited_from_train(get_project_state_for(project_path))

    label = method.value if method is not None else "set in denoise-train"
    with ui.row().classes("items-center gap-2").style("margin-bottom: 8px;"):
        ui.label("Denoiser").style(
            f"{SANS} font-size: 9px; font-weight: 700; color: {CLR_SUBLABEL}; "
            "letter-spacing: 0.06em; text-transform: uppercase;"
        )
        with (
            ui.element("div")
            .style(
                "display: inline-flex; align-items: center; background: #f3e8ff; "
                "border: 1px solid #e9d5ff; border-radius: 999px; padding: 1px 8px;"
            )
            .tooltip("Inherited from the denoise-train job — set the method there; predict follows it automatically.")
        ):
            ui.label(label).style("font-size: 10px; color: #6b21a8; font-weight: 600;")
        ui.label("inherited from denoise-train").style(f"{SANS} font-size: 9px; color: {CLR_SUBLABEL};")


# ──────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────


def render_config_preamble(job_model):
    """Collapsible 'How this job works' panel from the param class's CONFIG_PREAMBLE
    (markdown). No-op when the class defines no preamble."""
    text = (getattr(job_model, "CONFIG_PREAMBLE", "") or "").strip()
    if not text:
        return
    with (
        ui.expansion("How this job works", icon="info")
        .props("dense")
        .classes("w-full")
        .style("border: 1px solid #e9d5ff; border-radius: 5px; background: #faf5ff; margin-bottom: 8px;")
    ) as exp:
        exp.props('header-class="text-[11px] font-semibold text-purple-800"')
        with ui.column().classes("w-full").style("padding: 2px 12px 8px;"):
            ui.markdown(text).classes("cb-preamble").style("font-size: 11px; line-height: 1.5; color: #334155;")


def render_default_params(
    job_type, job_model, is_frozen: bool, save_handler: Callable, exclude: set[str] | None = None, **_ctx
):
    """Render all job-specific fields, grouped by type."""
    user_params = getattr(job_model, "USER_PARAMS", set())
    job_specific = user_params if user_params else (set(job_model.model_fields.keys()) - BASE_FIELDS)

    if exclude:
        job_specific = job_specific - exclude

    if not job_specific:
        ui.label("No configurable parameters.").style(f"{SANS} font-size: 10px; color: {CLR_SUBLABEL};")
        return

    groups = _classify_fields(job_model, job_specific)

    # Path-like fields each get their own full-width row (label left, input right).
    if groups["paths"]:
        with ui.column().classes("w-full gap-1").style("margin-bottom: 8px;"):
            for name in groups["paths"]:
                path_row(
                    _label_for(name),
                    job_model,
                    name,
                    is_frozen=is_frozen,
                    save_handler=save_handler,
                    hint=_get_description(job_model, name),
                )

    # Numeric / enum / text fields share an auto-packing grid.
    param_fields = groups["numeric"] + groups["enum"] + groups["text"]
    if param_fields:
        with field_grid():
            for name in param_fields:
                value = getattr(job_model, name)
                field_info = job_model.model_fields.get(name)
                field_type = field_info.annotation if field_info else None
                hint = _get_description(job_model, name)

                if is_enum_type(field_type):
                    enum_field(
                        _label_for(name),
                        job_model,
                        name,
                        field_type,
                        is_frozen=is_frozen,
                        save_handler=save_handler,
                        hint=hint,
                    )
                elif isinstance(value, (int, float)) or value is None:
                    numeric_field(
                        _label_for(name), job_model, name, is_frozen=is_frozen, save_handler=save_handler, hint=hint
                    )
                else:
                    text_field(
                        _label_for(name), job_model, name, is_frozen=is_frozen, save_handler=save_handler, hint=hint
                    )

    # Toggles wrap into their own row beneath.
    if groups["toggle"]:
        with ui.element("div").style("margin-top: 6px; width: 100%;"):
            with toggle_row():
                for name in groups["toggle"]:
                    toggle_field(
                        _label_for(name),
                        job_model,
                        name,
                        is_frozen=is_frozen,
                        save_handler=save_handler,
                        hint=_get_description(job_model, name),
                    )


def render_default_params_card(
    job_type, job_model, is_frozen: bool, save_handler: Callable, exclude: set[str] | None = None, **_ctx
):
    """render_default_params with an optional species badge prefix."""
    ui_mgr = _ctx.get("ui_mgr")
    # A Path, not str: get_project_state_for() calls .resolve() on it. The old str
    # here made both helpers below raise AttributeError inside their (since removed)
    # blanket excepts — the species badge never rendered on this path and the
    # denoiser row always read "set in denoise-train".
    project_path = ui_mgr.project_path if ui_mgr and ui_mgr.project_path else None

    render_species_badge(job_model, project_path, callbacks=_ctx.get("callbacks"))
    render_config_preamble(job_model)
    if job_type == JobType.DENOISE_PREDICT:
        render_denoise_inheritance(job_model, project_path)
    render_default_params(job_type, job_model, is_frozen, save_handler, exclude=exclude)


def _label_for(name: str) -> str:
    from ui.utils import snake_to_title

    return snake_to_title(name)

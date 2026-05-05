"""
Generic (default) parameter renderer.

Auto-groups fields by their underlying type, then renders each group with
the shared `cb-field*` visual language: inline label/value pairs in an
auto-packing CSS grid, no underline-only inputs, font sizes congruent with
the toolbar / pipeline roster.
"""

from typing import Callable, Dict, List, Optional, Set

from nicegui import ui

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

BASE_FIELDS: Set[str] = {
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


def _get_description(job_model, param_name: str) -> Optional[str]:
    field_info = job_model.model_fields.get(param_name)
    if field_info and field_info.description:
        return field_info.description
    return None


def _is_pathlike(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in ("path", "dir", "glob", "pattern", "file", "folder"))


def _classify_fields(job_model, field_names: Set[str]) -> Dict[str, List[str]]:
    groups: Dict[str, List[str]] = {"paths": [], "numeric": [], "text": [], "enum": [], "toggle": []}

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


def render_species_badge(job_model, project_path: Optional[str]):
    """Read-only species pill shown at the top of any particle-phase job config."""
    species_id = getattr(job_model, "species_id", None)
    if not species_id or not project_path:
        return

    try:
        from services.project_state import get_project_state_for

        state = get_project_state_for(project_path)
        species = state.get_species(species_id)
    except Exception:
        return

    if species is None:
        return

    with ui.row().classes("items-center gap-2").style("margin-bottom: 6px;"):
        ui.label("Particle").style(f"{SANS} font-size: 9px; font-weight: 700; color: {CLR_SUBLABEL}; "
                                   "letter-spacing: 0.06em; text-transform: uppercase;")
        with ui.element("div").style(
            f"display: inline-flex; align-items: center; "
            f"background: {species.color}18; border: 1px solid {species.color}55; "
            f"border-radius: 999px; padding: 1px 8px;"
        ):
            ui.label(species.name).style(f"font-size: 10px; color: {species.color}; font-weight: 600;")


# ──────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────


def render_default_params(
    job_type, job_model, is_frozen: bool, save_handler: Callable, exclude: Optional[Set[str]] = None, **_ctx
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
                    _label_for(name), job_model, name,
                    is_frozen=is_frozen, save_handler=save_handler,
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
                    enum_field(_label_for(name), job_model, name, field_type,
                               is_frozen=is_frozen, save_handler=save_handler, hint=hint)
                elif isinstance(value, (int, float)) or value is None:
                    numeric_field(_label_for(name), job_model, name,
                                  is_frozen=is_frozen, save_handler=save_handler, hint=hint)
                else:
                    text_field(_label_for(name), job_model, name,
                               is_frozen=is_frozen, save_handler=save_handler, hint=hint)

    # Toggles wrap into their own row beneath.
    if groups["toggle"]:
        with ui.element("div").style("margin-top: 6px; width: 100%;"):
            with toggle_row():
                for name in groups["toggle"]:
                    toggle_field(_label_for(name), job_model, name,
                                 is_frozen=is_frozen, save_handler=save_handler,
                                 hint=_get_description(job_model, name))


def render_default_params_card(
    job_type, job_model, is_frozen: bool, save_handler: Callable, exclude: Optional[Set[str]] = None, **_ctx
):
    """render_default_params with an optional species badge prefix."""
    ui_mgr = _ctx.get("ui_mgr")
    project_path = str(ui_mgr.project_path) if ui_mgr and ui_mgr.project_path else None

    render_species_badge(job_model, project_path)
    render_default_params(job_type, job_model, is_frozen, save_handler, exclude=exclude)


def _label_for(name: str) -> str:
    from ui.utils import snake_to_title

    return snake_to_title(name)

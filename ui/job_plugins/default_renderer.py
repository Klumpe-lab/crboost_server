"""
Generic (default) parameter renderer.

Auto-groups fields by type, renders with compact data-import-panel style:
borderless inputs, bottom-border only, small labels in normal case.
"""

from enum import Enum
from typing import Callable, Dict, List, Optional, Set

from nicegui import ui

from ui.utils import snake_to_title

MONO = "font-family: 'IBM Plex Mono', monospace;"
FONT = "font-family: 'IBM Plex Sans', sans-serif;"

_CLR_LABEL = "#475569"  # slate-600
_CLR_SUBLABEL = "#94a3b8"  # slate-400
_CLR_BORDER = "#cbd5e1"  # slate-300
_CLR_VALUE = "#1e293b"  # slate-800

_INPUT_STYLE = (
    f"{MONO} font-size: 11px; border-bottom: 1px solid {_CLR_BORDER}; "
    "border-radius: 0; padding: 1px 2px; line-height: 1.4;"
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
            is_enum = field_type is not None and isinstance(field_type, type) and issubclass(field_type, Enum)
            if is_enum:
                groups["enum"].append(name)
            else:
                groups["text"].append(name)

    return groups


# ------------------------------------------------------------------
# Species badge -- shared by default renderer and custom plugins
# ------------------------------------------------------------------


def render_species_badge(job_model, project_path: Optional[str]):
    """
    Read-only species pill shown at the top of any particle-phase job config.
    No-ops silently if job has no species_id or the registry lookup fails.
    """
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

    with ui.row().classes("items-center gap-2 mb-2"):
        ui.label("Particle").style(f"{FONT} font-size: 10px; font-weight: 600; color: {_CLR_SUBLABEL};")
        with ui.element("div").style(
            f"display: inline-flex; align-items: center; "
            f"background: {species.color}18; border: 1px solid {species.color}55; "
            f"border-radius: 999px; padding: 1px 8px;"
        ):
            ui.label(species.name).style(f"font-size: 10px; color: {species.color}; font-weight: 600;")


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def render_default_params(
    job_type, job_model, is_frozen: bool, save_handler: Callable, exclude: Optional[Set[str]] = None, **_ctx
):
    """Render all job-specific fields, grouped by type."""
    user_params = getattr(job_model, "USER_PARAMS", set())
    job_specific = user_params if user_params else (set(job_model.model_fields.keys()) - BASE_FIELDS)

    if exclude:
        job_specific = job_specific - exclude

    if not job_specific:
        ui.label("No configurable parameters.").style(f"{FONT} font-size: 10px; color: {_CLR_SUBLABEL};")
        return

    groups = _classify_fields(job_model, job_specific)

    if groups["paths"]:
        with ui.column().classes("w-full gap-1 mb-2"):
            for name in groups["paths"]:
                _render_path_field(name, job_model, is_frozen, save_handler)

    param_fields = groups["numeric"] + groups["enum"] + groups["text"]
    if param_fields:
        with ui.row().classes("w-full flex-wrap gap-x-3 gap-y-1 items-end mb-2"):
            for name in param_fields:
                value = getattr(job_model, name)
                field_info = job_model.model_fields.get(name)
                field_type = field_info.annotation if field_info else None
                is_enum = field_type is not None and isinstance(field_type, type) and issubclass(field_type, Enum)

                if is_enum:
                    _render_enum_field(name, job_model, field_type, is_frozen, save_handler)
                elif isinstance(value, (int, float)) or value is None:
                    _render_numeric_field(name, job_model, is_frozen, save_handler)
                else:
                    _render_text_field(name, job_model, is_frozen, save_handler)

    if groups["toggle"]:
        with ui.row().classes("w-full flex-wrap gap-x-4 gap-y-1 items-center"):
            for name in groups["toggle"]:
                _render_toggle_field(name, job_model, is_frozen, save_handler)


def render_default_params_card(
    job_type, job_model, is_frozen: bool, save_handler: Callable, exclude: Optional[Set[str]] = None, **_ctx
):
    """render_default_params with optional species badge. No card wrapper."""
    ui_mgr = _ctx.get("ui_mgr")
    project_path = str(ui_mgr.project_path) if ui_mgr and ui_mgr.project_path else None

    render_species_badge(job_model, project_path)
    render_default_params(job_type, job_model, is_frozen, save_handler, exclude=exclude)


# ------------------------------------------------------------------
# Shared tooltip helper
# ------------------------------------------------------------------


def _with_tooltip(element, job_model, param_name: str):
    desc = _get_description(job_model, param_name)
    if desc:
        element.tooltip(desc)
    return element


# ------------------------------------------------------------------
# Field renderers
# ------------------------------------------------------------------


def _render_path_field(param_name, job_model, is_frozen, save_handler):
    label = snake_to_title(param_name)
    desc = _get_description(job_model, param_name)

    with ui.row().classes("w-full items-baseline gap-2"):
        lbl = ui.label(label).style(
            f"{FONT} font-size: 9px; font-weight: 400; color: {_CLR_LABEL}; flex-shrink: 0; width: 80px;"
        )
        if desc:
            lbl.tooltip(desc)

        inp = ui.input().bind_value(job_model, param_name)
        inp.props("dense borderless hide-bottom-space")
        inp.style(f"{_INPUT_STYLE} flex: 1;")

        if is_frozen:
            inp.props("readonly").style(f"color: {_CLR_SUBLABEL};")
        else:
            inp.on_value_change(save_handler)


def _render_numeric_field(param_name, job_model, is_frozen, save_handler):
    label = snake_to_title(param_name)
    value = getattr(job_model, param_name)

    with ui.column().classes("gap-0"):
        lbl = ui.label(label).style(f"{FONT} font-size: 9px; font-weight: 400; color: {_CLR_LABEL}; line-height: 1;")
        _with_tooltip(lbl, job_model, param_name)

        inp = ui.number(value=value, format="%.4g").bind_value(job_model, param_name)
        inp.props("dense borderless hide-bottom-space")
        inp.style(f"{_INPUT_STYLE} width: 9ch;")

        if is_frozen:
            inp.props("readonly").style(f"color: {_CLR_SUBLABEL};")
        else:
            inp.on_value_change(save_handler)


def _render_text_field(param_name, job_model, is_frozen, save_handler):
    label = snake_to_title(param_name)
    value = getattr(job_model, param_name)
    width = "18ch" if len(str(value or "")) > 12 else "12ch"

    with ui.column().classes("gap-0"):
        lbl = ui.label(label).style(f"{FONT} font-size: 9px; font-weight: 400; color: {_CLR_LABEL}; line-height: 1;")
        _with_tooltip(lbl, job_model, param_name)

        inp = ui.input().bind_value(job_model, param_name)
        inp.props("dense borderless hide-bottom-space")
        inp.style(f"{_INPUT_STYLE} width: {width};")

        if is_frozen:
            inp.props("readonly").style(f"color: {_CLR_SUBLABEL};")
        else:
            inp.on_value_change(save_handler)


def _render_enum_field(param_name, job_model, field_type, is_frozen, save_handler):
    label = snake_to_title(param_name)
    value = getattr(job_model, param_name)

    with ui.column().classes("gap-0"):
        lbl = ui.label(label).style(f"{FONT} font-size: 9px; font-weight: 400; color: {_CLR_LABEL}; line-height: 1;")
        _with_tooltip(lbl, job_model, param_name)

        options = [e.value for e in field_type]
        sel = ui.select(options=options, value=value).bind_value(job_model, param_name)
        sel.props("dense borderless hide-bottom-space")
        sel.style(f"{MONO} font-size: 11px; width: 16ch; color: {_CLR_VALUE};")

        if is_frozen:
            sel.style(f"color: {_CLR_SUBLABEL};").disable()
        else:
            sel.on_value_change(save_handler)


def _render_toggle_field(param_name, job_model, is_frozen, save_handler):
    label = snake_to_title(param_name)
    desc = _get_description(job_model, param_name)

    cb = ui.checkbox(label).bind_value(job_model, param_name).props("dense size=xs")
    cb.style(f"{FONT} font-size: 9px; color: {_CLR_LABEL};")

    if desc:
        cb.tooltip(desc)

    if is_frozen:
        cb.disable()
    else:
        cb.on_value_change(save_handler)

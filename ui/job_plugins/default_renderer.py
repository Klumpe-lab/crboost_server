"""
Generic (default) parameter renderer.

Auto-groups fields by type, extracts descriptions from Pydantic Field metadata,
renders with consistent sizing and semantic layout.
"""

from enum import Enum
from typing import Callable, Dict, List, Optional, Set, Tuple

from nicegui import ui


# Infrastructure fields that never appear in the parameter grid
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


def snake_to_title(s: str) -> str:
    return " ".join(word.capitalize() for word in s.split("_"))


def _get_description(job_model, param_name: str) -> Optional[str]:
    """Extract description from Pydantic Field metadata."""
    field_info = job_model.model_fields.get(param_name)
    if field_info and field_info.description:
        return field_info.description
    return None


def _is_pathlike(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in ("path", "dir", "glob", "pattern", "file", "folder"))


def _classify_fields(job_model, field_names: Set[str]) -> Dict[str, List[str]]:
    """Sort fields into semantic groups based on their type and name."""
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
# Public API
# ------------------------------------------------------------------


def render_default_params(job_type, job_model, is_frozen: bool, save_handler: Callable, **_ctx):
    """Render all job-specific fields, grouped by type (no card wrapper)."""
    job_specific = set(job_model.model_fields.keys()) - BASE_FIELDS

    if not job_specific:
        ui.label("This job has no configurable parameters.").classes("text-xs text-gray-500 italic")
        return

    groups = _classify_fields(job_model, job_specific)

    # Paths section
    if groups["paths"]:
        _render_group_label("Paths")
        with ui.column().classes("w-full gap-2 mb-4"):
            for name in groups["paths"]:
                _render_path_field(name, job_model, is_frozen, save_handler)

    # Numeric + enum + text in a single grid
    param_fields = groups["numeric"] + groups["enum"] + groups["text"]
    if param_fields:
        _render_group_label("Parameters")
        with ui.row().classes("w-full flex-wrap gap-x-4 gap-y-4 items-start mb-4"):
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

    # Toggles as a compact row
    if groups["toggle"]:
        _render_group_label("Options")
        with ui.row().classes("w-full flex-wrap gap-x-5 gap-y-2 items-center"):
            for name in groups["toggle"]:
                _render_toggle_field(name, job_model, is_frozen, save_handler)


def render_default_params_card(job_type, job_model, is_frozen: bool, save_handler: Callable, **_ctx):
    """render_default_params wrapped in the standard card frame."""
    with ui.card().classes("w-full border border-gray-200 shadow-sm overflow-hidden bg-white"):
        with ui.row().classes("w-full items-center px-3 py-2 bg-gray-50 border-b border-gray-100"):
            ui.icon("tune", size="18px").classes("text-gray-500")
            ui.label("Job Parameters").classes("text-sm font-bold text-gray-800")

        with ui.column().classes("w-full p-4"):
            render_default_params(job_type, job_model, is_frozen, save_handler)


# ------------------------------------------------------------------
# Group label
# ------------------------------------------------------------------


def _render_group_label(text: str):
    ui.label(text).classes("text-[10px] font-black text-gray-400 uppercase tracking-widest mb-1")


# ------------------------------------------------------------------
# Shared tooltip helper
# ------------------------------------------------------------------


def _with_tooltip(element, job_model, param_name: str):
    """Attach a tooltip from the field's description, if available."""
    desc = _get_description(job_model, param_name)
    if desc:
        element.tooltip(desc)
    return element


# ------------------------------------------------------------------
# Field renderers
# ------------------------------------------------------------------


def _render_path_field(param_name, job_model, is_frozen, save_handler):
    """Full-width path input with description as helper text."""
    label = snake_to_title(param_name)
    desc = _get_description(job_model, param_name)

    with ui.row().classes("w-full items-center gap-3"):
        lbl = ui.label(label).classes("text-[10px] font-bold text-gray-400 uppercase w-28 shrink-0 text-right")
        if desc:
            lbl.tooltip(desc)

        inp = ui.input().bind_value(job_model, param_name)
        inp.props("dense outlined hide-bottom-space")
        inp.classes("flex-1 text-xs font-mono")

        if is_frozen:
            inp.classes("bg-gray-50 text-gray-500").props("readonly")
        else:
            inp.on_value_change(save_handler)


def _render_numeric_field(param_name, job_model, is_frozen, save_handler):
    """Compact numeric input with consistent width."""
    label = snake_to_title(param_name)
    value = getattr(job_model, param_name)

    with ui.column().classes("gap-0.5"):
        lbl = ui.label(label).classes("text-[10px] font-bold text-gray-400 uppercase leading-none ml-0.5")
        _with_tooltip(lbl, job_model, param_name)

        inp = ui.number(value=value, format="%.4g").bind_value(job_model, param_name)
        inp.props("dense outlined hide-bottom-space")
        inp.classes("text-xs font-mono")
        inp.style("width: 11ch;")

        if is_frozen:
            inp.classes("bg-gray-50 text-gray-500").props("readonly")
        else:
            inp.on_value_change(save_handler)


def _render_text_field(param_name, job_model, is_frozen, save_handler):
    """Compact text input, width adapts to content type."""
    label = snake_to_title(param_name)
    value = getattr(job_model, param_name)

    # Heuristic widths based on content shape
    if any(k in param_name.lower() for k in ("range", "grid", "dimensions")):
        width = "14ch"
    elif len(str(value or "")) > 16:
        width = "24ch"
    else:
        width = "14ch"

    with ui.column().classes("gap-0.5"):
        lbl = ui.label(label).classes("text-[10px] font-bold text-gray-400 uppercase leading-none ml-0.5")
        _with_tooltip(lbl, job_model, param_name)

        inp = ui.input().bind_value(job_model, param_name)
        inp.props("dense outlined hide-bottom-space")
        inp.classes("text-xs font-mono")
        inp.style(f"width: {width};")

        if is_frozen:
            inp.classes("bg-gray-50 text-gray-500").props("readonly")
        else:
            inp.on_value_change(save_handler)


def _render_enum_field(param_name, job_model, field_type, is_frozen, save_handler):
    """Dropdown for enum fields."""
    label = snake_to_title(param_name)
    value = getattr(job_model, param_name)

    with ui.column().classes("gap-0.5"):
        lbl = ui.label(label).classes("text-[10px] font-bold text-gray-400 uppercase leading-none ml-0.5")
        _with_tooltip(lbl, job_model, param_name)

        options = [e.value for e in field_type]
        sel = ui.select(options=options, value=value).bind_value(job_model, param_name)
        sel.props("dense outlined hide-bottom-space")
        sel.classes("text-xs font-mono")
        sel.style("width: 18ch;")

        if is_frozen:
            sel.classes("bg-gray-50 text-gray-500").disable()
        else:
            sel.on_value_change(save_handler)


def _render_toggle_field(param_name, job_model, is_frozen, save_handler):
    """Compact checkbox toggle."""
    label = snake_to_title(param_name)
    desc = _get_description(job_model, param_name)

    cb = ui.checkbox(label).bind_value(job_model, param_name).props("dense")
    cb.classes("text-xs")

    if desc:
        cb.tooltip(desc)

    if is_frozen:
        cb.disable()
    else:
        cb.on_value_change(save_handler)

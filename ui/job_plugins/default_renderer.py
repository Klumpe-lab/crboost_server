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
    field_group,
    toggle_row,
    text_field,
    numeric_field,
    enum_field,
    choice_row,
    toggle_field,
    path_row,
    is_enum_type,
    SANS,
    CLR_SUBLABEL,
    HELPER_STYLE,
    LABEL_W,
)

BASE_FIELDS: set[str] = {
    "execution_status",
    "relion_job_name",
    "relion_job_number",
    "paths",
    "slurm_overrides",
    "source_overrides",
    "is_orphaned",
    "missing_inputs",
    "JOB_CATEGORY",
    "workbench",
    "additional_sources",
    "merge_only",
}

# An open decision, under a field row's value column (label column + gap), in the Segment tab's amber.
_NOTE_WARN_STYLE = f"{HELPER_STYLE} color: #b45309; margin: 0 0 4px calc(var(--cb-label-w, {LABEL_W}px) + 8px);"


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
    path (reconstruct / class3d / …): the shared species mark + "open in Species" link.
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


def render_denoise_params(
    job_model,
    project_path: Path | None,
    instance_id: str | None,
    *,
    is_frozen: bool,
    save_handler: Callable,
    observe: Callable | None = None,
) -> Callable[[], None]:
    """Denoise-predict's parameters: the Denoiser, under it only the fields of the method it names, then
    which tomograms run.

    The Denoiser (`job_params.denoiser_choices`) is which model the job runs, as one choice, set through the
    setting door with the method, IsoNet deconv and model input it brings. The row shows what Inputs has
    wired and never picks for the user: with nothing wired it is empty and says so. Returns the refresh of
    the Denoiser and its fields, a no-op unless what they show changed (an input chosen in Inputs, say); the
    page's poll runs it too, except while one of its fields has focus.
    """
    from services.job_params import denoiser_choices, set_params
    from services.models_base import DenoiseMethod, EasymodeDenoiser
    from services.project_state import get_project_state_for
    from ui.components.reactive import FocusHold

    fields = type(job_model).model_fields
    box = ui.column().classes("w-full gap-0")
    shown_key: list = [None]
    hold = FocusHold()

    def state():
        return get_project_state_for(project_path) if project_path else None

    def choices() -> tuple[dict[str, str], str]:
        st = state()
        if st is None or instance_id is None:
            return {}, ""
        return denoiser_choices(st, instance_id)

    def pick(value: str) -> None:
        st = state()
        if st is None or instance_id is None:
            return
        res = set_params(st, instance_id, {"denoiser": value})
        if not res["success"]:
            ui.notify(res["error"], type="warning")
        save_handler()
        refresh()

    def method_of(shown: str) -> DenoiseMethod | None:
        """The method the shown entry runs: easymode, the one said for a model file, or a train job's own."""
        kind, _, name = shown.partition(":")
        if shown == DenoiseMethod.EASYMODE.value:
            return DenoiseMethod.EASYMODE
        if kind == "manual":
            return DenoiseMethod(name)
        if kind == "train":
            train = state().jobs.get(name)
            return DenoiseMethod(train.denoise_method) if train else None
        return None

    def number(attr: str) -> None:
        hold.watch(
            numeric_field(
                fields[attr].title,
                job_model,
                attr,
                is_frozen=is_frozen,
                save_handler=save_handler,
                hint=fields[attr].description,
            )
        )

    def build(options: dict[str, str], shown: str) -> None:
        hold.watch(
            choice_row(
                "Denoiser",
                job_model,
                "denoise_method",
                options,
                is_frozen=is_frozen,
                save_handler=save_handler,
                value=shown,
                hint="A trained model runs as it was trained: choose the denoise-train job that made it, and "
                "Inputs reads its model. A model file set by hand in Inputs: say whether cryoCARE or IsoNet "
                "trained it. easymode: its pretrained denoisers on the full reconstruction, no model input.",
                on_change=pick,
            )
        )
        if not shown:
            ui.label("Not chosen yet: easymode, a denoise-train job's model, or a model file set in Inputs.").style(
                _NOTE_WARN_STYLE
            )
        method = method_of(shown)
        with field_grid():
            if method == DenoiseMethod.EASYMODE:
                hold.watch(
                    enum_field(
                        "Model",
                        job_model,
                        "easymode_denoiser",
                        EasymodeDenoiser,
                        is_frozen=is_frozen,
                        save_handler=save_handler,
                        hint=fields["easymode_denoiser"].description,
                    )
                )
                number("easymode_tta")
                number("easymode_iter")
            elif method == DenoiseMethod.CRYOCARE:
                for attr in ("ntiles_x", "ntiles_y", "ntiles_z"):
                    number(attr)
            elif shown == f"manual:{DenoiseMethod.ISONET.value}":
                toggle_field(
                    "IsoNet deconv",
                    job_model,
                    "isonet_deconv",
                    is_frozen=is_frozen,
                    save_handler=save_handler,
                    hint=fields["isonet_deconv"].description,
                )

    def refresh() -> None:
        options, shown = choices()
        key = (tuple(options.items()), shown)
        if key == shown_key[0] or box.is_deleted:
            return
        shown_key[0] = key
        box.clear()
        with box:
            build(options, shown)

    refresh()
    if observe is not None:
        observe(lambda: None if hold.held else refresh())
    # Outside the box: no method change rebuilds it under the user's typing.
    with field_group(), field_grid():
        text_field(
            fields["denoising_tomo_name"].title,
            job_model,
            "denoising_tomo_name",
            is_frozen=is_frozen,
            save_handler=save_handler,
            hint=fields["denoising_tomo_name"].description,
        ).props('placeholder="all"')
    return refresh


# ──────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────


def render_config_preamble(job_model):
    """Collapsible 'How this job works' fold from the param class's CONFIG_PREAMBLE
    (markdown). No-op when the class defines no preamble."""
    text = (getattr(job_model, "CONFIG_PREAMBLE", "") or "").strip()
    if not text:
        return
    with (
        ui.expansion("How this job works", icon="info_outline")
        .props("dense")
        .classes("w-full")
        .style("margin-bottom: 8px;")
    ) as exp:
        # Folded text inside the Parameters panel: no box of its own.
        exp.props('header-class="text-[11px] font-semibold text-slate-600 px-0"')
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
                    _label_for(job_model, name),
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
                        _label_for(job_model, name),
                        job_model,
                        name,
                        field_type,
                        is_frozen=is_frozen,
                        save_handler=save_handler,
                        hint=hint,
                    )
                elif isinstance(value, (int, float)) or value is None:
                    numeric_field(
                        _label_for(job_model, name),
                        job_model,
                        name,
                        is_frozen=is_frozen,
                        save_handler=save_handler,
                        hint=hint,
                    )
                else:
                    text_field(
                        _label_for(job_model, name),
                        job_model,
                        name,
                        is_frozen=is_frozen,
                        save_handler=save_handler,
                        hint=hint,
                    )

    # Toggles wrap into their own row beneath.
    if groups["toggle"]:
        with ui.element("div").style("margin-top: 6px; width: 100%;"):
            with toggle_row():
                for name in groups["toggle"]:
                    toggle_field(
                        _label_for(job_model, name),
                        job_model,
                        name,
                        is_frozen=is_frozen,
                        save_handler=save_handler,
                        hint=_get_description(job_model, name),
                    )


def render_default_params_card(
    job_type, job_model, is_frozen: bool, save_handler: Callable, exclude: set[str] | None = None, **_ctx
):
    """render_default_params with an optional species badge prefix. Returns the denoiser block's refresh on
    denoise-predict, None elsewhere."""
    ui_mgr = _ctx.get("ui_mgr")
    # A Path, not str: get_project_state_for() calls .resolve() on it. The old str
    # here made both helpers below raise AttributeError inside their (since removed)
    # blanket excepts — the species badge never rendered on this path and the
    # denoiser row always read "set in denoise-train".
    project_path = ui_mgr.project_path if ui_mgr and ui_mgr.project_path else None

    render_species_badge(job_model, project_path, callbacks=_ctx.get("callbacks"))
    render_config_preamble(job_model)
    refresh = None
    if job_type == JobType.DENOISE_PREDICT:
        refresh = render_denoise_params(
            job_model,
            project_path,
            _ctx.get("instance_id"),
            is_frozen=is_frozen,
            save_handler=save_handler,
            observe=_ctx.get("observe"),
        )
        # The block above owns these; the generic grid renders only what a later field adds.
        exclude = (exclude or set()) | job_model.DENOISER_FIELDS
        if not job_model.USER_PARAMS - exclude:
            return refresh
    render_default_params(job_type, job_model, is_frozen, save_handler, exclude=exclude)
    return refresh


def _label_for(job_model, name: str) -> str:
    """The field's declared `title` (a readable name with its unit), else the title-cased field name."""
    from ui.utils import snake_to_title

    field_info = job_model.model_fields.get(name)
    return (field_info.title if field_info and field_info.title else None) or snake_to_title(name)

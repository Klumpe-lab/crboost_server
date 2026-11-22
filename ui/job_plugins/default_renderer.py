"""
The default Parameters of a job: `params_renderer.render_params` over the job's layout, with the config preamble
above it; denoise-predict's Denoiser block in place of its Denoiser group.
"""

from collections.abc import Callable
from pathlib import Path

from nicegui import ui

from services.models_base import JobType
from ui.job_plugins._field_styles import (
    field_grid,
    numeric_field,
    enum_field,
    choice_row,
    toggle_field,
    HELPER_STYLE,
    NOTE_INDENT,
)
from ui.job_plugins.params_renderer import _is_pathlike, render_params

__all__ = ["_is_pathlike", "render_config_preamble", "render_default_params", "render_default_params_card"]

# An open decision, under a field row's value column (label column + gap), in the Segment tab's amber.
_NOTE_WARN_STYLE = f"{HELPER_STYLE} color: #b45309; margin: 0 0 4px {NOTE_INDENT}px;"
# A warning about the whole job, above its parameters.
_BANNER_WARN_STYLE = f"{HELPER_STYLE} color: var(--cb-warn-ink, #8a4b06); margin: 0 0 8px;"


def render_denoise_params(
    job_model,
    project_path: Path | None,
    instance_id: str | None,
    *,
    is_frozen: bool,
    save_handler: Callable,
    observe: Callable | None = None,
) -> Callable[[], None]:
    """Denoise-predict's Denoiser group: the Denoiser, under it only the fields of the method it names.

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
    return refresh


def _render_halves_note(project_path: Path | None, instance_id: str | None, observe: Callable | None) -> Callable:
    """IsoNet's warning about deconvolved half-maps (`job_params.isonet_halves_warning`) above the parameters: one
    line, written only when its text changes. It follows the page's poll as well as edits here, because the
    setting it reads is on the reconstruction's page."""
    from services.job_params import isonet_halves_warning
    from services.project_state import get_project_state_for

    label = ui.label("").style(_BANNER_WARN_STYLE)
    label.set_visibility(False)
    shown: list[str | None] = [None]

    def refresh() -> None:
        if label.is_deleted or project_path is None or instance_id is None:
            return
        state = get_project_state_for(project_path)
        text = isonet_halves_warning(state, instance_id) if instance_id in state.jobs else None
        if text == shown[0]:
            return
        shown[0] = text
        label.set_text(text or "")
        label.set_visibility(bool(text))

    refresh()
    if observe is not None:
        observe(refresh)
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
    job_type, job_model, is_frozen: bool, save_handler: Callable, exclude: set[str] | None = None, **ctx
) -> Callable[[], None]:
    """Every user parameter of the job at the field geometry (`params_renderer.render_params`); `ctx` carries the
    renderer's hooks (`before`, `custom`, `replace`, `after`) and `ui_mgr` / `instance_id`. Returns the refresh."""
    ui_mgr = ctx.get("ui_mgr")
    return render_params(
        job_model,
        is_frozen=is_frozen,
        save_handler=save_handler,
        project_path=ui_mgr.project_path if ui_mgr and ui_mgr.project_path else None,
        instance_id=ctx.get("instance_id"),
        exclude=set(exclude or ()),
        before=ctx.get("before"),
        custom=ctx.get("custom"),
        replace=ctx.get("replace"),
        after=ctx.get("after"),
    )


def render_default_params_card(
    job_type, job_model, is_frozen: bool, save_handler: Callable, exclude: set[str] | None = None, **_ctx
):
    """The config preamble, then the parameters; on denoise-predict the Denoiser block draws its group. Returns the
    parameters' refresh."""
    ui_mgr = _ctx.get("ui_mgr")
    # A Path, not str: get_project_state_for() calls .resolve() on it.
    project_path = ui_mgr.project_path if ui_mgr and ui_mgr.project_path else None
    replace = dict(_ctx.pop("replace", None) or {})
    observe = _ctx.get("observe")
    if job_type == JobType.DENOISE_PREDICT:
        replace["Denoiser"] = lambda _g: render_denoise_params(
            job_model,
            project_path,
            _ctx.get("instance_id"),
            is_frozen=is_frozen,
            save_handler=save_handler,
            observe=observe,
        )
    before = _ctx.pop("before", None)

    def head():
        render_config_preamble(job_model)
        refreshes = []
        if job_type in (JobType.DENOISE_TRAIN, JobType.DENOISE_PREDICT):
            refreshes.append(_render_halves_note(project_path, _ctx.get("instance_id"), observe))
        if before is not None and callable(r := before()):
            refreshes.append(r)

        def refresh() -> None:
            for r in refreshes:
                r()

        return refresh if refreshes else None

    return render_default_params(
        job_type, job_model, is_frozen, save_handler, exclude=exclude, before=head, replace=replace, **_ctx
    )

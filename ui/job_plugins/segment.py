"""Segment plugin: the method, then what it runs — easymode features picked from the weights
cache's registry, or one Ais model and the feature its title names — with TTA, easymode's output
format, segmenting at easymode's 10 Å and smoothing plane-by-plane networks along Z. The input is the
I/O section's; an IsoNet-denoised input is marked here and never blocked: easymode was trained on
raw, even/odd and noise2noise reconstructions.

The note on the input is what the job tab reruns after an edit in another section (the I/O choice
changes it); the fields themselves are never rebuilt under the user."""

from functools import lru_cache
from pathlib import Path

from nicegui import ui

from services.models_base import DenoiseMethod, JobType, SegmentFormat, SegmentMethod
from services.path_resolution_service import PathResolutionService
from services.project_state import get_project_state_for
from services.segmentation.easymode import ais_model_title, easymode_cache_dir, read_catalog
from services.segmentation.manifest import ENGINE_AIS, feature_slug
from ui.job_plugins import register_params_renderer
from ui.job_plugins._field_styles import (
    HELPER_STYLE,
    enum_field,
    field_grid,
    multi_choice_row,
    numeric_field,
    path_row,
    toggle_field,
    toggle_row,
)
from ui.job_plugins.default_renderer import render_config_preamble

_WARN_STYLE = HELPER_STYLE + " color: #b45309;"
# Under the value column of a field row (label column 110 px + gap).
_NOTE_STYLE = " margin: 0 0 4px 118px;"


@lru_cache(maxsize=4)
def _catalog_at(cache_dir: str) -> dict:
    """The weights cache's features, read once per cache: it is frozen."""
    return read_catalog(Path(cache_dir))


@lru_cache(maxsize=16)
def _title_at(path: str, mtime_ns: int) -> str:
    return ais_model_title(Path(path))


def _description(job_model, name: str) -> str | None:
    field = type(job_model).model_fields.get(name)
    return field.description if field else None


def _note(text: str, *, warn: bool):
    return ui.label(text).style((_WARN_STYLE if warn else HELPER_STYLE) + _NOTE_STYLE)


def _set_note(label, text: str, warn: bool) -> None:
    label.set_text(text)
    label.style(replace=(_WARN_STYLE if warn else HELPER_STYLE) + _NOTE_STYLE)


@register_params_renderer(JobType.SEGMENT)
def render_segment_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    render_config_preamble(job_model)

    @ui.refreshable
    def method_block():
        if job_model.method == SegmentMethod.AIS_MODEL:
            _render_ais_model(job_model, is_frozen, save_handler)
        else:
            _render_easymode(job_model, is_frozen, save_handler)

    def on_method():
        save_handler()
        method_block.refresh()

    with field_grid():
        enum_field(
            "Method",
            job_model,
            "method",
            SegmentMethod,
            is_frozen=is_frozen,
            save_handler=on_method,
            hint=_description(job_model, "method"),
        )
        numeric_field(
            "TTA", job_model, "tta", is_frozen=is_frozen, save_handler=save_handler, hint=_description(job_model, "tta")
        )
    method_block()

    @ui.refreshable
    def input_note():
        _render_input_note(ui_mgr, ctx.get("instance_id"))

    input_note()
    return input_note.refresh


def _render_easymode(job_model, is_frozen: bool, save_handler) -> None:
    with field_grid():
        enum_field(
            "Output format",
            job_model,
            "output_format",
            SegmentFormat,
            is_frozen=is_frozen,
            save_handler=save_handler,
            hint=_description(job_model, "output_format"),
        )
    with toggle_row():
        toggle_field(
            "Segment at 10 Å",
            job_model,
            "match_training_voxel",
            is_frozen=is_frozen,
            save_handler=save_handler,
            hint=_description(job_model, "match_training_voxel"),
        )
        _smooth_toggle(job_model, is_frozen, save_handler, "Smooth 2.5D features along Z")
    try:
        catalog = _catalog_at(str(easymode_cache_dir()))
    except (LookupError, OSError, ValueError) as e:
        # Absent is stated: no feature list is guessed without the cache.
        multi_choice_row(
            "Features",
            job_model,
            "features",
            {},
            is_frozen=True,
            save_handler=save_handler,
            placeholder="weights cache not readable",
        )
        _note(str(e), warn=True)
        return

    # Features the job holds that the cache does not stay listed, so they can be seen and removed.
    unknown = [f for f in job_model.features if f not in catalog]
    options = {name: name for name in catalog} | {f: f"{f} · not in the weights cache" for f in unknown}
    summary = None

    def on_change():
        save_handler()
        _set_note(summary, *_features_summary(job_model.features, catalog))

    multi_choice_row(
        "Features",
        job_model,
        "features",
        options,
        is_frozen=is_frozen,
        save_handler=on_change,
        hint="The features of easymode's weights cache (its registry.json); each runs that feature's default network.",
    )
    text, warn = _features_summary(job_model.features, catalog)
    summary = _note(text, warn=warn)


def _features_summary(features: list[str], catalog: dict) -> tuple[str, bool]:
    """(text, is_warning) under the feature picker: which network family each chosen feature runs
    on, or what keeps the job from starting."""
    if not features:
        return "No features chosen: the job does not start without one.", True
    unknown = [f for f in features if f not in catalog]
    if unknown:
        return f"Not in the weights cache, so the job fails until they are removed: {', '.join(unknown)}", True
    ais = [f for f in features if catalog[f].engine == ENGINE_AIS]
    native = [f for f in features if catalog[f].engine != ENGINE_AIS]
    parts = ([f"Ais networks: {', '.join(ais)}"] if ais else []) + (
        [f"easymode's 3D networks: {', '.join(native)}"] if native else []
    )
    return " · ".join(parts), False


def _render_ais_model(job_model, is_frozen: bool, save_handler) -> None:
    note = None

    def on_change():
        save_handler()
        _set_note(note, *_model_note(job_model.ais_model_path))

    # Read when the field is left, not per keystroke: the title is in the model file.
    path_row(
        "Ais model",
        job_model,
        "ais_model_path",
        is_frozen=is_frozen,
        save_handler=on_change,
        hint=_description(job_model, "ais_model_path"),
        on_change=False,
    )
    text, warn = _model_note(job_model.ais_model_path)
    note = _note(text, warn=warn)
    with toggle_row():
        _smooth_toggle(job_model, is_frozen, save_handler, "Smooth along Z (2D and 2.5D models)")


def _smooth_toggle(job_model, is_frozen: bool, save_handler, label: str) -> None:
    toggle_field(
        label,
        job_model,
        "smooth_z",
        is_frozen=is_frozen,
        save_handler=save_handler,
        hint=_description(job_model, "smooth_z"),
    )


def _model_note(path: str) -> tuple[str, bool]:
    path = (path or "").strip()
    if not path:
        return "Point at the .scnm model to apply.", True
    try:
        title = _title_at(path, Path(path).stat().st_mtime_ns)
    except FileNotFoundError:
        return f"No such file: {path}", True
    except (OSError, ValueError) as e:
        return str(e), True
    return f"Feature: {feature_slug(title)} (the model's title, “{title}”)", False


def _render_input_note(ui_mgr, instance_id: str | None) -> None:
    """Marks an input denoised with IsoNet, which corrects the missing wedge: easymode's training
    data had none of that."""
    if ui_mgr is None or not ui_mgr.project_path or not instance_id:
        return
    state = get_project_state_for(ui_mgr.project_path)
    source = PathResolutionService(state).source_of(instance_id, "input_tomograms")
    jm = state.jobs.get(source) if source else None
    if jm is None or jm.job_type != JobType.DENOISE_PREDICT:
        return
    method = jm.inherited_from_train(state)[0] or jm.denoise_method
    if method != DenoiseMethod.ISONET:
        return
    _note(
        f"The input, {source}, is denoised with IsoNet (missing wedge corrected). easymode was trained on raw, "
        "even/odd and noise2noise reconstructions: check its result on these before relying on it.",
        warn=True,
    )

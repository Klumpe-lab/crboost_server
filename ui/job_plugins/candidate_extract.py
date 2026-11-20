"""Candidate Extraction ("Pick candidates") plugin — picking from template-matching score maps or from a
segmented feature; no volume is cut here (that is subtomo extraction).

IMOD model generation, MIP previews, and 3dmod copy commands live inside
the unified Tomogram Dashboard's Candidate Extract section card
(ui/tomo_dashboard_dialog.py), since they operate across all extract
instances and want to be inspectable without first navigating to a specific
job tab.

Species facts (Ø, symmetry, templates, the segmentation feature) live on the species itself, not here.
particle_diameter_ang and feature on this job are per-job overrides, in the species group and the
Segmentation group of its parameters; new jobs take the species' values when they are made.

The feature row lists the features the job's segmentations input holds, each with its coverage; the
manifest is read in a thread, and the choices are set in place when they move, never rebuilt under the
user.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

from nicegui import ui

from services.job_params import set_params
from services.models_base import JobType, SegmentMethod, resolve_species
from services.path_resolution_service import PathResolutionService, override_instance_id
from services.project_state import get_project_state_for
from services.segmentation.manifest import read_manifest, segment_feature_id
from services.visualization.tomo_geometry import read_tomo_table
from ui.components.reactive import SingleFlight
from ui.job_plugins import register_params_renderer
from ui.job_plugins._field_styles import FIELD_ROW_STYLE, HELPER_STYLE, NOTE_INDENT, L, _label, _select_chrome, _unit
from ui.job_plugins.default_renderer import render_default_params_card

# score_filter_method is a string field in the param class — keep it that
# way (legacy persisted values stay valid), but expose only the two values
# the driver actually understands.
SCORE_FILTER_CHOICES = ["None", "tophat"]
_SCORE_FILTER_HINT = "Optional post-extract filter. 'tophat' applies a tophat morphological filter on the score map."

_WARN_STYLE = HELPER_STYLE + " color: #b45309;"
_NOTE_STYLE = f" margin: 0 0 4px {NOTE_INDENT}px;"
# The feature choices touch disk (the manifest, the tomograms star): read at most this often while the page polls.
_DISK_REFRESH_S = 15.0
_NOT_CHOSEN = "not chosen"


@register_params_renderer(JobType.TEMPLATE_EXTRACT_PYTOM)
def render_candidate_extract_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    """The default parameters; each source shows only its own (`shows_param`), the strategy only its own threshold.
    The score filter is a choice of the two filters the driver knows; the feature, of those the segmentations
    input holds."""
    project_path = ui_mgr.project_path if ui_mgr and ui_mgr.project_path else None
    instance_id = ctx.get("instance_id")
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
        ),
        "feature": lambda f: (
            _FeatureRow(
                job_model, project_path, instance_id, is_frozen=is_frozen, save_handler=save_handler, hint=f.hint
            ).refresh
        ),
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


def feature_choices(path: str | None, source: str, planned: list[str] | None) -> tuple[dict[str, str], str, bool]:
    """(options, note, is_warning) of the feature row for the segmentations at `path` (None: nothing to read), made
    by `source`. Each feature reads "ribosome_ais · in 46 of 48 tomograms · mean p 1.2 %": the tomograms it has a
    volume of, of those the producer segmented (its input star's rows, else the manifest's tomograms). A producer
    that has not run lists what it is set to make (`planned`). Reads disk: runs in a thread."""
    if path is None:
        return {}, "No segmentations to read: add a Segment job, or import segmentations in the Tomograms view.", True
    if not Path(path).is_file():
        if planned:
            return {f: f"{f} · once {source} has run" for f in planned}, f"{source} has not run yet.", False
        return {}, f"{source} has not written its segmentations yet: its features show once it has.", True
    try:
        manifest = read_manifest(Path(path).parent)
    except (OSError, ValueError) as e:
        return {}, f"{source}'s segmentations.json could not be read: {e}", True
    star = manifest.parameters.get("input_tomograms")
    table = read_tomo_table(Path(star)) if star else None
    total = len(table) if table is not None else len({e.tomogram for e in manifest.entries})
    options: dict[str, str] = {}
    for feature in manifest.features():
        entries = [e for e in manifest.entries if e.feature == feature]
        fractions = [e.fraction_pct for e in entries if e.fraction_pct is not None]
        mean = f" · mean p {sum(fractions) / len(fractions):.2g} %" if fractions else ""
        options[feature] = f"{feature} · in {len(entries)} of {total} tomograms{mean}"
    if not options:
        return {}, f"{source}'s segmentations hold no volumes.", True
    return options, "", False


class _FeatureRow:
    """The Feature row of the segmentation source: a select over what the segmentations input holds, and a note
    under it. Built once; `refresh` (the page's poll, an edit) reads the input again when it moved, or every
    `_DISK_REFRESH_S`, off the event loop, and sets the choices in place."""

    def __init__(self, job_model, project_path: Path | None, instance_id: str | None, *, is_frozen, save_handler, hint):
        self._jm = job_model
        self._project_path = project_path
        self._iid = instance_id
        self._frozen = is_frozen
        self._save = save_handler
        self._flight = SingleFlight()
        self._key: tuple | None = None
        self._at = 0.0
        hint = "\n".join(x for x in (hint, self._species_note()) if x)
        with ui.element("div").style(FIELD_ROW_STYLE):
            _label("Feature", hint)
            held = job_model.feature or ""
            self._select = ui.select(options=self._with_held({}), value=held)
            _select_chrome(self._select, L)
            self._select.disable()  # until the input's features are read
            self._select.on_value_change(self._on_change)
        self._note = ui.label("").style(HELPER_STYLE + _NOTE_STYLE)
        self._note.set_visibility(False)
        self.refresh()

    def _species_note(self) -> str:
        """The species' feature, the default this job took when it was made, or that it names none."""
        if self._project_path is None or self._jm.job_type is None:
            return ""
        species, _ = resolve_species(get_project_state_for(self._project_path), self._jm, self._iid)
        if species is None:
            return ""
        if species.segmentation_feature:
            return (
                f"Species default ({species.name}): {species.segmentation_feature}. Changing it here affects only "
                "this job; the species is not modified."
            )
        return f"{species.name} names no segmentation feature: choose one here, or name it on its Species page."

    def _with_held(self, options: dict[str, str]) -> dict[str, str]:
        """The choices with "not chosen" first, and the feature the job holds, said so when the input lacks it."""
        out = {"": _NOT_CHOSEN} | options
        held = self._jm.feature or ""
        if held and held not in out:
            out[held] = f"{held} · not in these segmentations"
        return out

    def _source(self) -> tuple[str | None, str, list[str] | None]:
        """In memory: the segmentations the input resolves to (path, None when nothing), the producer's name, and
        what a Segment job that has not run is set to make."""
        if self._project_path is None or not self._iid:
            return None, "", None
        state = get_project_state_for(self._project_path)
        v = PathResolutionService(state).validate_input_slot(
            self._jm.job_type, self._jm, "input_segmentations", check_filesystem=False, consumer_instance_id=self._iid
        )
        producer = override_instance_id(v.source_key or "")
        label = producer or (v.source_key or "the segmentations")
        jm = state.jobs.get(producer) if producer else None
        planned = None
        if jm is not None and jm.job_type == JobType.SEGMENT and jm.method == SegmentMethod.EASYMODE:
            # easymode's features by the id the job writes; an Ais model's title is read when the job runs.
            planned = sorted(segment_feature_id(f) for f in jm.features)
        return v.resolved_path or None, label, planned

    def refresh(self) -> None:
        if self._select.is_deleted or not self._jm.from_segmentation:
            return
        key = (self._jm.feature, tuple(sorted((self._jm.source_overrides or {}).items())))
        if key == self._key and time.monotonic() - self._at < _DISK_REFRESH_S:
            return
        self._key, self._at = key, time.monotonic()
        asyncio.create_task(self._load())

    async def _load(self) -> None:
        async with self._flight("load") as acquired:
            if not acquired:
                return
            path, label, planned = self._source()
            options, note, warn = await asyncio.to_thread(feature_choices, path, label, planned)
            self._apply(options, note, warn)

    def _apply(self, options: dict[str, str], note: str, warn: bool) -> None:
        if self._select.is_deleted:
            return
        choices = self._with_held(options)
        if choices != self._select.options:
            self._select.set_options(choices, value=self._jm.feature or "")
        if self._frozen or not options:
            self._select.disable()
        else:
            self._select.enable()
        held = self._jm.feature or ""
        if not held:
            note, warn = note or "No feature chosen: the job does not start without one.", True
        elif options and held not in options:
            note, warn = f"{held} is not among these segmentations' features: choose one of theirs.", True
        self._note.set_text(note)
        self._note.style(replace=(_WARN_STYLE if warn else HELPER_STYLE) + _NOTE_STYLE)
        self._note.set_visibility(bool(note))

    def _on_change(self, e) -> None:
        value = e.value or ""
        if value == (self._jm.feature or ""):
            return
        if self._project_path is None or not self._iid:
            self._jm.feature = value  # no project to hold the job: nothing for the door to check against
        else:
            res = set_params(get_project_state_for(self._project_path), self._iid, {"feature": value})
            if not res["success"]:
                ui.notify(res["error"], type="warning")
                self._select.set_value(self._jm.feature or "")
                return
        self._save()
        self._key = None  # the note follows the choice at the next refresh
        self.refresh()

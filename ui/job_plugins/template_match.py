"""
Template Matching plugin.

Species is locked at job creation time. The species's templates and masks
are surfaced as two independent dropdowns so the user picks both
explicitly — defaulting to whichever the species has selected. Search
symmetry is also rendered here. The full SymmetryGroup enum is offered:
Cn (n=1..6) is honored by PyTOM's --z-axis-rotational-symmetry flag;
D/T/O/I are honored via a custom angle list (auto-generated at submission
by services.templating.angle_lists and passed to PyTOM as
--angular-search <file>). Default is inherited from species.symmetry at
job-creation time but the user can override here. The default-renderer
skips template_path / mask_path / symmetry because we render all three
in the "From species" section below.

Those three rows are job parameters with a species-shaped default, so they sit in the
same Parameters card as everything else — one `section_header` + `field_grid` in the
house vocabulary, never a nested card with its own font scale. Each row's tooltip
names the species value it defaulted from, because the driver reads the job's value:
the species card can say I1 while the run is C1, and only this tooltip and the Species
page's Jobs-tab drift chip will say so.

The Config tab opens with the one species line (pill + "open in Species");
species facts (templates, masks, Ø, symmetry) live on the species itself, not
here. If species.templates is empty the row states the absence and
points at the Species page rather than preselecting anything.

The Search region section restricts matching to segmented features of the tomograms
(services/segmentation/search_region.py). The features offered are the project's: registered,
or set on a Segment job. Its note names the segmentations the job will read (the I/O section's,
which shows that input only while a region is set) and what they lack.
"""

from pathlib import Path

from nicegui import ui

from services.jobs.template_match import TM_SYMMETRY_CHOICES
from services.models_base import JobType, SegmentMethod
from services.path_resolution_service import PathResolutionService, override_instance_id
from services.project_state import get_project_state_for
from services.models_base import resolve_species
from services.segmentation.search_region import describe, read_segmentations
from services.templating.template_metadata import read_template_header
from ui.components.species_pill import render_species_line, species_opener
from ui.job_plugins import register_params_renderer
from ui.job_plugins._field_styles import (
    HELPER_STYLE,
    SPECIES_SECTION_TITLE,
    choice_row,
    field_grid,
    multi_choice_row,
    numeric_field,
    section_header,
)
from ui.job_plugins.default_renderer import render_config_preamble, render_default_params

_OVERRIDE_NOTE = "Changing it here affects only this job instance — the species is not modified."
_SEARCH_FIELDS = {"search_include", "search_exclude", "search_dilation_ang"}
_WARN_STYLE = HELPER_STYLE + " color: #b45309;"
# Under the value column of a field row (label column 110 px + gap).
_NOTE_STYLE = " margin: 0 0 4px 118px;"


@register_params_renderer(JobType.TEMPLATE_MATCH_PYTOM)
def render_template_match_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None, **_ctx):
    instance_id = _ctx.get("instance_id")

    species = None
    if ui_mgr and ui_mgr.project_path:
        state = get_project_state_for(ui_mgr.project_path)
        species, _resolved_sid = resolve_species(state, job_model, instance_id)

    render_species_line(species, on_open=species_opener(_ctx.get("callbacks"), species.id) if species else None)

    # Render the regular params with template_path/mask_path/symmetry excluded —
    # we handle these specially below (or via the species header). The ctx
    # exclude (array_throttle → SLURM section) is honored too.
    exclude = {"template_path", "mask_path", "symmetry"} | _SEARCH_FIELDS | set(_ctx.get("exclude") or ())
    render_config_preamble(job_model)
    render_default_params(job_type, job_model, is_frozen, save_handler, exclude=exclude)

    if species is not None:
        # ── Template + mask + search symmetry ─────────────────────────────────
        section_header(SPECIES_SECTION_TITLE)
        with field_grid():
            _render_template_dropdown(species, job_model, is_frozen, save_handler)
            _render_mask_dropdown(species, job_model, is_frozen, save_handler)
            _render_symmetry_dropdown(species, job_model, is_frozen, save_handler)

    if ui_mgr is None or not ui_mgr.project_path:
        return None
    state = get_project_state_for(ui_mgr.project_path)
    return _render_search_region(state, job_model, instance_id, is_frozen, save_handler)


def _render_search_region(state, job_model, instance_id: str | None, is_frozen: bool, save_handler):
    """The features to search in and to leave out, and how far to grow the region; under them, what
    the job will read. Returns the note's refresh, which the job tab reruns after an edit."""
    section_header("Search region")
    features = _project_features(state)
    held = [*job_model.search_include, *job_model.search_exclude]
    options = {f: f for f in features} | {f: f"{f} · not segmented in this project" for f in held if f not in features}
    with field_grid():
        multi_choice_row(
            "Search in",
            job_model,
            "search_include",
            options,
            is_frozen=is_frozen,
            save_handler=save_handler,
            placeholder="no segmentations in this project",
            hint="Match only where one of these features has p ≥ 0.5. None: the whole tomogram, less what is left out.",
        )
        multi_choice_row(
            "Leave out",
            job_model,
            "search_exclude",
            options,
            is_frozen=is_frozen,
            save_handler=save_handler,
            placeholder="no segmentations in this project",
            hint="Never match where one of these features has p ≥ 0.5 (void, ice, …).",
        )
        numeric_field(
            "Grow by",
            job_model,
            "search_dilation_ang",
            is_frozen=is_frozen,
            save_handler=save_handler,
            suffix="Å",
            hint="Grow the searched features by this distance before what is left out is cut away, so a particle "
            "on the region's edge is matched whole.",
        )

    @ui.refreshable
    def note() -> None:
        text, warn = _region_note(state, job_model, instance_id)
        ui.label(text).style((_WARN_STYLE if warn else HELPER_STYLE) + _NOTE_STYLE)

    note()
    return note.refresh


def _project_features(state) -> list[str]:
    """The features this project has segmentations of, or a Segment job is set to make."""
    ids = {f.id for f in state.feature_registry}
    for jm in state.jobs.values():
        if jm.job_type == JobType.SEGMENT and jm.method == SegmentMethod.EASYMODE:
            ids |= set(jm.features)
    return sorted(ids)


def _region_note(state, job_model, instance_id: str | None) -> tuple[str, bool]:
    """(text, is_warning): what the region is and which segmentations it reads, or what keeps it
    from running."""
    if not job_model.restricts_search:
        return "The whole tomogram is searched.", False
    both = sorted(set(job_model.search_include) & set(job_model.search_exclude))
    if both:
        return f"{', '.join(both)}: both searched in and left out, so nothing would be searched.", True
    region = describe(job_model.search_include, job_model.search_exclude, job_model.search_dilation_ang)
    v = PathResolutionService(state).validate_input_slot(
        job_model.job_type, job_model, "input_segmentations", check_filesystem=False, consumer_instance_id=instance_id
    )
    if v.error_message:
        return f"Searched: {region}. Segmentations: {v.error_message}", True
    if not v.resolved_path:
        return (
            f"Searched: {region}. No segmentations to read: add a Segment job, or import segmentations in the "
            "Tomograms view.",
            True,
        )
    source = _source_label(state, v.source_key or "")
    try:
        have = set(read_segmentations(v.resolved_path).features())
    except FileNotFoundError:
        return f"Searched: {region}, read from {source} once it has run.", False
    except (OSError, ValueError) as e:
        return f"Searched: {region}. {source}'s segmentations.json could not be read: {e}", True
    lack = sorted({*job_model.search_include, *job_model.search_exclude} - have)
    if lack:
        return f"Searched: {region}. {source} has no {', '.join(lack)}: pick other segmentations in I/O.", True
    return f"Searched: {region}, from {source}.", False


def _source_label(state, source_key: str) -> str:
    """The segmentations an input reads, by name: a job's instance, or an import batch's label."""
    instance = override_instance_id(source_key)
    if instance is not None:
        return instance
    batch = Path(source_key.partition(":")[2]).name
    rec = state.get_segmentation_import(batch)
    return f"the segmentation {rec.label}" if rec is not None else source_key


def _default_note(kind: str, default_name: str | None) -> str:
    """ "why is this not what the species says", answerable without leaving the tab."""
    if default_name:
        return f"Species default: `{default_name}`. {_OVERRIDE_NOTE}"
    return f"The species has no selected {kind}. {_OVERRIDE_NOTE}"


def _render_template_dropdown(species, job_model, is_frozen: bool, save_handler) -> None:
    templates = list(getattr(species, "templates", []) or [])
    current = getattr(job_model, "template_path", "") or ""
    selected = species.get_selected_template() if hasattr(species, "get_selected_template") else None
    default_path = (selected.template_path if selected else "") or ""
    hint = _default_note("template", Path(default_path).name if default_path else None)

    if not templates:
        choice_row(
            "Template",
            job_model,
            "template_path",
            {},
            is_frozen=is_frozen,
            save_handler=save_handler,
            hint=hint,
            placeholder="no templates registered",
        )
        _absent_pointer("Register one in the Particles registry → Templates & masks.")
        return

    options: dict[str, str] = {}
    for t in templates:
        h = read_template_header(t.template_path)
        bits = []
        if h.apix_ang:
            bits.append(f"{h.apix_ang:.3g} Å/px")
        if h.box_px:
            bits.append(f"box {h.box_px}")
        bits.append(t.polarity)
        if t.lowpass_resolution_ang:
            bits.append(f"lp {t.lowpass_resolution_ang:g}Å")
        suffix_bits = " · ".join(bits)
        label = f"{Path(t.template_path).name}  ·  {suffix_bits}"
        options[t.template_path] = label

    # Resolve the value: explicit per-job override > species selected > first entry
    value = current or default_path
    if value not in options and options:
        value = next(iter(options.keys()))

    choice_row(
        "Template",
        job_model,
        "template_path",
        options,
        is_frozen=is_frozen,
        save_handler=save_handler,
        value=value,
        hint=hint,
    )


def _render_symmetry_dropdown(species, job_model, is_frozen: bool, save_handler) -> None:
    # Annotate each option with how PyTOM is going to honor it so the user
    # knows what they're picking. Cn (n=1..6) goes through the dedicated
    # --z-axis-rotational-symmetry flag; D/T/O/I are translated by the
    # driver into a custom asymmetric-unit angle list, which gives the same
    # |group|× speedup for a template already symmetric under that group.
    def _suffix(s: str) -> str:
        if s == "C1":
            return "  ·  no symmetry (full SO(3) search)"
        if s.startswith("C"):
            return f"  ·  {s[1:]}-fold rotational symmetry (PyTOM --z-axis flag)"
        return f"  ·  point group {s} (angle list, ~{_group_order(s)}× speedup)"

    options = {s: f"{s}{_suffix(s)}" for s in TM_SYMMETRY_CHOICES}
    value = getattr(job_model, "symmetry", "C1") or "C1"
    if value not in options:
        value = "C1"
    species_sym = getattr(species, "symmetry", "") or "C1"
    choice_row(
        "Symmetry",
        job_model,
        "symmetry",
        options,
        is_frozen=is_frozen,
        save_handler=save_handler,
        value=value,
        empty_value="C1",
        hint=(
            f"Species default: {'None (C1)' if species_sym == 'C1' else species_sym}. {_OVERRIDE_NOTE}\n"
            "Cn uses PyTOM's --z-axis-rotational-symmetry flag (n-fold around z). "
            "D/T/O/I generate a custom asymmetric-unit angle list at submission and "
            "pass --angular-search <file>. The template must be symmetric under the "
            "chosen group — RELION reconstructions made with --sym <group> are."
        ),
    )


def _group_order(point_group: str) -> int:
    """Order of the point group (|G|) for the suffix label. Mirrors the table
    in services.templating.angle_lists.POINT_GROUP_ORDER but kept inline here
    to avoid importing scipy-dependent modules at UI render time."""
    return {"D2": 4, "D3": 6, "D4": 8, "D5": 10, "D6": 12, "T": 12, "O": 24, "I1": 60, "I2": 60}.get(point_group, 1)


def _render_mask_dropdown(species, job_model, is_frozen: bool, save_handler) -> None:
    masks = list(getattr(species, "masks", []) or [])
    current = getattr(job_model, "mask_path", "") or ""
    selected = species.get_selected_mask() if hasattr(species, "get_selected_mask") else None
    default_path = (selected.mask_path if selected else "") or ""
    hint = _default_note("mask", Path(default_path).name if default_path else None)

    if not masks:
        choice_row(
            "Mask",
            job_model,
            "mask_path",
            {},
            is_frozen=is_frozen,
            save_handler=save_handler,
            hint=hint,
            placeholder="no masks registered",
        )
        _absent_pointer("Create or import one in the Particles registry → Templates & masks.")
        return

    # Include a "(none)" option so the user can explicitly skip a mask.
    options: dict[str, str] = {"": "(none)"}
    for m in masks:
        method = m.method or "manual"
        label = f"{Path(m.mask_path).name}  ·  {method}"
        options[m.mask_path] = label

    value = current or default_path or ""
    if value not in options:
        value = ""

    choice_row(
        "Mask", job_model, "mask_path", options, is_frozen=is_frozen, save_handler=save_handler, value=value, hint=hint
    )


def _absent_pointer(text: str) -> None:
    """The amber "nothing registered" pointer that rides under an empty row. Absent must
    read as absent, not as defaulted."""
    with ui.row().classes("items-center gap-1 no-wrap").style("margin: -2px 0 4px 118px;"):
        ui.icon("warning", size="11px").classes("text-amber-600")
        ui.label(text).style("font-family: 'IBM Plex Sans', sans-serif; font-size: 9px; color: #b45309;")

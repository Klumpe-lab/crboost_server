"""Journey dashboard — pixel / binning sanity panel (ROADMAP §11).

One dense monospace table showing how pixel size + tomogram dimensions +
per-instance box / padding / particle-diameter propagate through the pipeline,
with inline sanity-rule warnings (box vs particle Ø, crop > box, template px ≠
recon px, …). The computation lives in ``services.pixel_chain``
(``compute_pixel_chain`` builds the rows, ``apply_sanity_rules`` annotates
them); ``render_pixel_sanity_table`` here draws them.
"""

from __future__ import annotations

from nicegui import ui

# Compatibility re-exports (roadmap-01 stage 3c) — the computation moved to
# services/pixel_chain.py. Delete after one release; new code imports from
# services.pixel_chain directly.
from services.pixel_chain import apply_sanity_rules as _apply_sanity_rules, compute_pixel_chain as _compute_pixel_chain

__all__ = ["_apply_sanity_rules", "_compute_pixel_chain", "_render_pixel_sanity_table", "render_pixel_sanity_table"]


# --- Sanity-table renderers --------------------------------------------------


def _fmt_px(v: float | None) -> str:
    return "—" if not v else f"{v:g}"


def _fmt_dims_px(d: tuple | None) -> str:
    if d is None:
        return "—"
    parts = [str(x) for x in d if x is not None]
    return " × ".join(parts) if parts else "—"


def _fmt_dims_ang(d: tuple | None, px: float | None) -> str:
    if d is None or not px:
        return "—"
    vals = [round(x * px) for x in d if x is not None]
    return " × ".join(f"{v:,}" for v in vals) if vals else "—"


def _fmt_box_combined(r: dict) -> str:
    """`128 px (794 Å)` or `—`. Single column, both units inline."""
    bp = r.get("box_px")
    ba = r.get("box_ang")
    if bp is None and ba is None:
        return "—"
    if bp is None:
        return f"{ba:g} Å"
    if ba is None:
        return f"{bp} px"
    return f"{bp} px ({ba:g} Å)"


def _fmt_crop_combined(r: dict) -> str:
    cp = r.get("_crop_px")
    eff_px = r.get("px_size_ang")
    if not cp:
        return "—"
    if eff_px:
        return f"{cp} px ({cp * eff_px:g} Å)"
    return f"{cp} px"


def _fmt_particle(r: dict) -> str:
    d = r.get("particle_diameter_ang")
    if not d:
        return "—"
    return f"{d:g} Å"


def _pixel_cell(text: str, warning: tuple[str, str] | None = None, *, notes: bool = False) -> None:
    cls = "cb-pixel-cell"
    if notes:
        cls += " cb-pixel-notes"
    if warning:
        cls += f" cb-pixel-warn-{warning[0]}"
    with ui.element("div").classes(cls):
        ui.label(text)
        if warning:
            icon = "error" if warning[0] == "error" else "warning_amber"
            ui.icon(icon, size="12px").classes("cb-pixel-warn-icon").tooltip(warning[1])


_PIXEL_COLUMNS: list[tuple[str, str, str]] = [
    ("stage", "stage", ""),
    ("px", "Å/px", "Pixel size at this stage."),
    ("dim_px", "tomo (px)", "Tomogram (or detector) dimensions in voxels at this stage's pixel size."),
    (
        "dim_ang",
        "tomo (Å)",
        "Physical size of the imaged volume in Å. Approximately invariant across stages — "
        "only the px sampling changes with binning.",
    ),
    (
        "box",
        "box",
        "Template-volume box (TM) or particle subtomo box (Subtomo). Shown as 'N px (M Å)'. "
        "Flagged when the Å dimension is outside 1.5–3× of particle diameter.",
    ),
    (
        "crop",
        "crop",
        "Cropped subtomogram size — Subtomo Extract only. crop_size = -1 in config means 'no cropping'. "
        "Shown as 'N px (M Å)'.",
    ),
    (
        "particle",
        "particle",
        "Particle diameter (Å) — set on Candidate Extract. Cross-applies to TM and Subtomo rows for "
        "the box-vs-particle sanity check (since the box must contain the particle plus margin).",
    ),
    ("notes", "notes", ""),
]

_UNIVERSAL_STAGE_KEYS = {"camera", "fs_ctf", "tilt_filter", "align", "ts_ctf", "recon"}


def _render_pixel_row_cells(r: dict) -> None:
    """Emit the row cells for one row inside the surrounding `cb-pixel-table`.
    Column count must match `_PIXEL_COLUMNS`."""
    stripe = r.get("species_color")
    for col_key, _label, _hint in _PIXEL_COLUMNS:
        warning = r["warnings"].get(col_key)
        if col_key == "stage":
            cls = "cb-pixel-cell"
            if warning:
                cls += f" cb-pixel-warn-{warning[0]}"
            with ui.element("div").classes(cls):
                ui.element("span").classes("cb-pixel-stripe").style(f"background:{stripe};" if stripe else "")
                ui.label(r["stage_label"]).classes("cb-pixel-stage-label")
                if r.get("instance_id"):
                    ui.label(r["instance_id"]).classes("cb-pixel-instance-label")
        elif col_key == "px":
            _pixel_cell(_fmt_px(r["px_size_ang"]), warning)
        elif col_key == "dim_px":
            _pixel_cell(_fmt_dims_px(r["tomo_px"]), warning)
        elif col_key == "dim_ang":
            _pixel_cell(_fmt_dims_ang(r["tomo_px"], r["px_size_ang"]), warning)
        elif col_key == "box":
            _pixel_cell(_fmt_box_combined(r), warning)
        elif col_key == "crop":
            _pixel_cell(_fmt_crop_combined(r), warning)
        elif col_key == "particle":
            _pixel_cell(_fmt_particle(r), warning)
        elif col_key == "notes":
            _pixel_cell(" · ".join(r["notes"]) if r["notes"] else "—", warning, notes=True)


def _render_box_crop_header_tooltip(kind: str) -> None:
    """Rich, multi-line info tooltip for the box / crop column headers, using
    the dashboard's light-card tooltip idiom (`cb-chip-tooltip`). Explains the
    reconstruction-box-vs-output-box distinction plus a few worked scenarios —
    box/crop sizing is the user's #1 binning-arithmetic foot-gun. Kept in sync
    with the job-panel explainer (ui/job_plugins/subtomo_extraction.py)."""
    with ui.tooltip().classes("cb-chip-tooltip"):
        if kind == "box":
            ui.label("BOX — reconstruction box").classes("cb-tt-head")
            ui.label(
                "The cube RELION builds each pseudo-subtomogram in (on a TM row: the template "
                "volume). Must hold the particle PLUS the CTF-delocalized signal that high defocus "
                "smears outward — too small truncates high-resolution information."
            )
            ui.label("aim 2–3× particle Ø · 1.5× floor · even numbers").classes("cb-tt-sub")
            ui.separator().classes("cb-tt-sep")
            ui.label("examples · Ø ≈ 300 Å").classes("cb-tt-head")
            for line in (
                "bin 4 (4 Å vox): 192 px = 768 Å · 2.6× ✓",
                "bin 4: 64 px = 256 Å · 0.85× ✗ clipped",
                "bin 2 (2 Å vox): 320 px = 640 Å · 2.1× ✓",
            ):
                ui.label(line).classes("cb-tt-line")
        else:  # crop
            ui.label("CROP — output box").classes("cb-tt-head")
            ui.label(
                "The central cube kept after reconstruction — what Refine3D / Class3D load, so it "
                "sets on-disk size + downstream memory. The rebuild re-localizes signal to the "
                "center, so the outer rim is redundant and safe to trim. −1 = no cropping."
            )
            ui.label("must be ≤ box · aim ≥ 1.5× particle Ø").classes("cb-tt-sub")
            ui.separator().classes("cb-tt-sep")
            ui.label("examples · Ø ≈ 300 Å").classes("cb-tt-head")
            for line in (
                "bin 4: 112 px = 448 Å · 1.5× ✓",
                "bin 2: 160 px = 320 Å · 1.07× ⚠ tight",
                "crop > box · ✗ invalid",
            ):
                ui.label(line).classes("cb-tt-line")


def _render_pixel_header_cells() -> None:
    for col_key, label, hint in _PIXEL_COLUMNS:
        with ui.element("div").classes("cb-pixel-cell cb-pixel-header"):
            ui.label(label)
            # box / crop get a rich multi-line tooltip (explanation + worked
            # scenarios); every other column keeps its plain one-line hint.
            if col_key in ("box", "crop"):
                with ui.icon("info_outline", size="11px").classes("cb-pixel-warn-icon"):
                    _render_box_crop_header_tooltip(col_key)
            elif hint:
                ui.icon("info_outline", size="11px").classes("cb-pixel-warn-icon").tooltip(hint)


def _group_rows_by_species(rows: list[dict]) -> tuple[list[dict], list[tuple[str | None, list[dict]]]]:
    """Split into (universal_rows, [(species_id, species_rows), ...]).
    Universal stages share one table; per-species stages each get their own
    sub-table so multi-species projects stay readable."""
    universal: list[dict] = []
    by_species: dict[str | None, list[dict]] = {}
    species_order: list[str | None] = []
    for r in rows:
        if r["stage_key"] in _UNIVERSAL_STAGE_KEYS:
            universal.append(r)
            continue
        sid = r.get("species_id")
        if sid not in by_species:
            species_order.append(sid)
            by_species[sid] = []
        by_species[sid].append(r)
    return universal, [(sid, by_species[sid]) for sid in species_order]


def render_pixel_sanity_table(rows: list[dict]) -> None:
    """Dense monospace table. Universal stages (Camera → Recon) at the top,
    per-species stages (TM, Pick, Subtomo) underneath, separated by a
    full-width species marker row. All rows share one CSS Grid so columns
    line up across species. Wrapper has `overflow-x: auto` for narrow
    viewports. Sanity-rule violations surface as per-cell icons with
    tooltips."""
    if not rows:
        return

    with ui.element("div").classes("cb-pixel-section-title"):
        ui.icon("rule", size="13px").classes("text-indigo-600")
        ui.label("Pixel / binning sanity")
        ui.icon("info_outline", size="11px").classes("cb-pixel-warn-icon").tooltip(
            "Per-stage pixel size, tomogram dimensions, and template/extract/subtomo "
            "box + padding. Inline warnings flag binning-arithmetic mistakes "
            "(particle won't fit in box, template px ≠ recon px, etc.). "
            "Per-particle stages (TM, Pick, Subtomo) appear under a species marker; "
            "the table scrolls horizontally on narrow viewports."
        )

    universal, species_groups = _group_rows_by_species(rows)

    with ui.element("div").classes("cb-pixel-table-wrapper"):
        with ui.element("div").classes("cb-pixel-table"):
            _render_pixel_header_cells()
            for r in universal:
                _render_pixel_row_cells(r)
            for sid, group_rows in species_groups:
                first = group_rows[0]
                species_name = first.get("species_name") or sid or "unspecified"
                species_color = first.get("species_color") or "#94a3b8"
                with ui.element("div").classes("cb-pixel-species-row"):
                    ui.element("span").classes("cb-pixel-stripe").style(f"background:{species_color};")
                    ui.label(str(species_name)).classes("cb-pixel-species-name")
                    if sid and species_name != sid:
                        ui.label(f"({sid})").classes("cb-pixel-species-id")
                for r in group_rows:
                    _render_pixel_row_cells(r)


# Old name kept importable for one release.
_render_pixel_sanity_table = render_pixel_sanity_table

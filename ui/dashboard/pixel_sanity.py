"""Journey dashboard — pixel / binning sanity panel (ROADMAP §11).

One dense monospace table showing how pixel size + tomogram dimensions +
per-instance box / padding / particle-diameter propagate through the pipeline,
with inline sanity-rule warnings (box vs particle Ø, crop > box, template px ≠
recon px, …). ``_compute_pixel_chain`` builds the rows, ``_apply_sanity_rules``
annotates them, ``_render_pixel_sanity_table`` draws them. Extracted from
``ui/tomo_dashboard_dialog.py`` (R0 refactor); py_compile + ruff only.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from nicegui import ui

from services.models_base import JobType
from services.templating.template_metadata import get_effective_template_path, read_template_header
from ui.dashboard.data import (
    _candidate_extract_instances,
    _find_job_by_type,
    _resolve_species,
    _subtomo_extract_instances,
    _template_match_instances,
)


# Template-header reads are cached centrally in
# services.templating.template_metadata (mtime-keyed); use the shared
# helper here as a thin tuple shim so existing callsites don't change.


def _read_template_apix_box(template_path: str) -> tuple[Optional[float], Optional[int]]:
    info = read_template_header(template_path)
    return info.apix_ang, info.box_px


def _parse_tomo_dimensions(s: str) -> Optional[tuple[int, int, int]]:
    """`'4096x4096x2048'` → `(4096, 4096, 2048)`. Returns None on parse failure.
    Native-pixel-size dimensions as written into TsAlignmentParams.tomo_dimensions."""
    if not s:
        return None
    try:
        parts = s.lower().split("x")
        if len(parts) != 3:
            return None
        return (int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, AttributeError):
        return None


def _scale_tomo_dims(native_dims: tuple[int, int, int], native_px: float, target_px: float) -> tuple[int, int, int]:
    if target_px <= 0 or native_px <= 0:
        return native_dims
    f = native_px / target_px
    return (int(round(native_dims[0] * f)), int(round(native_dims[1] * f)), int(round(native_dims[2] * f)))


def _compute_pixel_chain(project_state) -> list[dict]:
    """Walk pipeline stages and return rows for the pixel-sanity table.
    One row per pipeline stage; multi-instance fan-out for TM / Pick / Subtomo
    (one row per species). Each row has the columns the table renders, plus
    an empty `warnings` dict that `_apply_sanity_rules` later populates."""
    ms = project_state.microscope
    acq = project_state.acquisition
    native_px = float(ms.pixel_size_angstrom or 0.0)

    rows: list[dict] = []

    def make_row(stage_key: str, stage_label: str, **kw) -> dict:
        return {
            "stage_key": stage_key,
            "stage_label": stage_label,
            "px_size_ang": kw.get("px_size_ang"),
            "tomo_px": kw.get("tomo_px"),
            "box_px": kw.get("box_px"),
            "box_ang": kw.get("box_ang"),
            "particle_diameter_ang": kw.get("particle_diameter_ang"),
            "notes": kw.get("notes") or [],
            "warnings": {},
            "instance_id": kw.get("instance_id"),
            "species_color": kw.get("species_color"),
            "species_id": kw.get("species_id"),
            "species_name": kw.get("species_name"),
            "_template_workbench_px": kw.get("_template_workbench_px"),
            "_crop_px": kw.get("_crop_px"),
        }

    # ---- Camera (always) ----
    rows.append(
        make_row(
            "camera",
            "Camera",
            px_size_ang=native_px or None,
            tomo_px=(acq.detector_dimensions[0], acq.detector_dimensions[1], None),
            notes=[f"detector frame · {int(ms.acceleration_voltage_kv)} kV"],
        )
    )

    # ---- FS Motion / CTF ----
    fs = _find_job_by_type(project_state, JobType.FS_MOTION_CTF)
    if fs:
        rows.append(
            make_row(
                "fs_ctf",
                "FS Motion / CTF",
                px_size_ang=native_px or None,
                instance_id=fs[0],
                notes=["per-frame; no rescale"],
            )
        )

    # ---- Tilt Filter (pipeline job OR standalone) ----
    tf_in_pipeline = _find_job_by_type(project_state, JobType.TILT_FILTER)
    tf_standalone = (
        project_state.project_path is not None and (Path(project_state.project_path) / "TiltFilter").exists()
    )
    if tf_in_pipeline or tf_standalone:
        rows.append(
            make_row(
                "tilt_filter",
                "Tilt Filter",
                px_size_ang=native_px or None,
                instance_id=tf_in_pipeline[0] if tf_in_pipeline else None,
                notes=["row drop only; no rescale"],
            )
        )

    # ---- Alignment (rescale + native-px tomo dims) ----
    ali = _find_job_by_type(project_state, JobType.TS_ALIGNMENT)
    aligned_px: Optional[float] = None
    aligned_dims_native: Optional[tuple[int, int, int]] = None
    aligned_dims_at_align_px: Optional[tuple[int, int, int]] = None
    if ali:
        ali_iid, ali_jm = ali
        v = float(getattr(ali_jm, "rescale_angpixs", 0.0) or 0.0)
        aligned_px = v if v > 0 else None
        aligned_dims_native = _parse_tomo_dimensions(getattr(ali_jm, "tomo_dimensions", "") or "")
        if aligned_dims_native and aligned_px and native_px > 0:
            aligned_dims_at_align_px = _scale_tomo_dims(aligned_dims_native, native_px, aligned_px)
        notes = []
        if aligned_px and native_px > 0:
            notes.append(f"rescale ÷{aligned_px / native_px:.1f}")
        am = getattr(ali_jm, "alignment_method", None)
        if am is not None:
            notes.append(f"method={getattr(am, 'value', am)}")
        rows.append(
            make_row(
                "align",
                "Align",
                px_size_ang=aligned_px,
                tomo_px=aligned_dims_at_align_px,
                instance_id=ali_iid,
                notes=notes,
            )
        )

    # ---- TS CTF (post-alignment refit; inherits aligned px / dims) ----
    ctf = _find_job_by_type(project_state, JobType.TS_CTF)
    if ctf:
        rows.append(
            make_row(
                "ts_ctf",
                "TS CTF",
                px_size_ang=aligned_px,
                tomo_px=aligned_dims_at_align_px,
                instance_id=ctf[0],
                notes=["inherits align scale"],
            )
        )

    # ---- Reconstruct (rescale to recon_px) ----
    rec = _find_job_by_type(project_state, JobType.TS_RECONSTRUCT)
    recon_px: Optional[float] = None
    recon_dims: Optional[tuple[int, int, int]] = None
    if rec:
        rec_iid, rec_jm = rec
        v = float(getattr(rec_jm, "rescale_angpixs", 0.0) or 0.0)
        recon_px = v if v > 0 else None
        if aligned_dims_native and recon_px and native_px > 0:
            recon_dims = _scale_tomo_dims(aligned_dims_native, native_px, recon_px)
        notes = []
        if recon_px and native_px > 0:
            notes.append(f"rescale ÷{recon_px / native_px:.1f}")
        if getattr(rec_jm, "deconv", 0):
            notes.append("deconv")
        rows.append(
            make_row("recon", "Recon", px_size_ang=recon_px, tomo_px=recon_dims, instance_id=rec_iid, notes=notes)
        )

    # ---- Template Match (one row per species) ----
    for tm_iid, tm_jm in _template_match_instances(project_state):
        species, species_id = _resolve_species(project_state, tm_jm, tm_iid)
        # Template path: per-job override (v1) wins when set, otherwise the
        # species's v2 template (or v1 fallback). MRC header is the
        # authoritative source for apix and box.
        tmpl_path = getattr(tm_jm, "template_path", "") or (get_effective_template_path(species) if species else "")
        tmpl_px = 0.0
        tmpl_box = 0
        if tmpl_path:
            mrc_apix, mrc_box = _read_template_apix_box(tmpl_path)
            tmpl_px = float(mrc_apix or 0.0)
            tmpl_box = int(mrc_box or 0)
        op_px = recon_px or aligned_px
        tmpl_box_ang = (tmpl_box * tmpl_px) if (tmpl_box and tmpl_px) else None
        notes = []
        ang_search = getattr(tm_jm, "angular_search", None)
        if ang_search:
            notes.append(f"θ={ang_search}°")
        # Symmetry: prefer species (v2 source of truth); fall back to job (v1).
        sym = (getattr(species, "symmetry", None) if species else None) or getattr(tm_jm, "symmetry", None)
        if sym:
            notes.append(f"sym={sym}")
        if tmpl_px:
            notes.append(f"tmpl px={tmpl_px:g}")
        if tmpl_path and not (tmpl_px and tmpl_box):
            notes.append("tmpl header unreadable")
        rows.append(
            make_row(
                "tm",
                "TM",
                px_size_ang=op_px,
                tomo_px=recon_dims,
                box_px=tmpl_box if tmpl_box else None,
                box_ang=tmpl_box_ang,
                instance_id=tm_iid,
                species_color=getattr(species, "color", None),
                species_id=species_id,
                species_name=getattr(species, "name", None) or species_id,
                _template_workbench_px=tmpl_px or None,
                notes=notes,
            )
        )

    # ---- Candidate Extract (one row per species) ----
    candidate_diameter_by_species: dict[Optional[str], list[tuple[str, float]]] = {}
    for ce_iid, ce_jm in _candidate_extract_instances(project_state):
        species, species_id = _resolve_species(project_state, ce_jm, ce_iid)
        # Particle diameter: prefer species.diameter_ang (v2 source of truth);
        # fall back to the per-Pick-job value (v1) so projects pre-migration
        # still surface a number.
        species_diameter = float(getattr(species, "diameter_ang", 0.0) or 0.0) if species else 0.0
        diameter = species_diameter or float(getattr(ce_jm, "particle_diameter_ang", 0.0) or 0.0)
        if diameter:
            candidate_diameter_by_species.setdefault(species_id, []).append((ce_iid, diameter))
        notes = []
        method = getattr(ce_jm, "cutoff_method", None)
        cv = getattr(ce_jm, "cutoff_value", None)
        if method is not None and cv is not None:
            mv = getattr(method, "value", str(method))
            notes.append(f"{mv}={cv:g}")
        max_n = getattr(ce_jm, "max_num_particles", None)
        if max_n:
            notes.append(f"max N={max_n}")
        score_apix = getattr(ce_jm, "apix_score_map", "auto") or "auto"
        if score_apix and score_apix != "auto":
            notes.append(f"score apix={score_apix}")
        # Particle diameter in voxels at recon px (handy mental check)
        if diameter and recon_px:
            notes.append(f"Ø ≈ {diameter / recon_px:.0f} px @ {recon_px:g} Å/px")
        rows.append(
            make_row(
                "pick",
                "Pick",
                px_size_ang=recon_px,
                tomo_px=recon_dims,
                particle_diameter_ang=diameter or None,
                instance_id=ce_iid,
                species_color=getattr(species, "color", None),
                species_id=species_id,
                species_name=getattr(species, "name", None) or species_id,
                notes=notes,
            )
        )

    # ---- Subtomo Extract (one row per species) ----
    for se_iid, se_jm in _subtomo_extract_instances(project_state):
        species, species_id = _resolve_species(project_state, se_jm, se_iid)
        binning = float(getattr(se_jm, "binning", 1.0) or 1.0)
        eff_px = (native_px * binning) if native_px > 0 else None
        bx = int(getattr(se_jm, "box_size", 0) or 0)
        cx = int(getattr(se_jm, "crop_size", -1) or -1)
        box_px = bx if bx > 0 else None
        box_ang = (box_px * eff_px) if (box_px and eff_px) else None
        notes = []
        if binning != 1.0:
            notes.append(f"bin={binning:g}")
        # Surface candidate diameter cross-link for sanity rule
        diameter_for_species: Optional[float] = None
        items = candidate_diameter_by_species.get(species_id) or []
        if items:
            diameter_for_species = items[0][1]
        rows.append(
            make_row(
                "subtomo",
                "Subtomo",
                px_size_ang=eff_px,
                box_px=box_px,
                box_ang=box_ang,
                particle_diameter_ang=diameter_for_species,
                instance_id=se_iid,
                species_color=getattr(species, "color", None),
                species_id=species_id,
                species_name=getattr(species, "name", None) or species_id,
                _crop_px=cx if cx > 0 else None,
                notes=notes,
            )
        )

    return rows


def _apply_sanity_rules(rows: list[dict]) -> None:
    """Mutate `rows`: populate per-cell `warnings` for sanity-rule violations.

    Each warning is `(level, message)` where level ∈ {"error", "warn", "info"}
    and the dict key matches a column id from `_PIXEL_COLUMNS` (so the icon
    attaches to the offending cell).
    """
    recon_px: Optional[float] = None
    for r in rows:
        if r["stage_key"] == "recon":
            recon_px = r["px_size_ang"]
            break

    # Particle diameter consistency across candidate-extract instances of
    # the same species
    by_species: dict[Optional[str], list[dict]] = {}
    for r in rows:
        if r["stage_key"] == "pick" and r.get("particle_diameter_ang"):
            by_species.setdefault(r.get("species_id"), []).append(r)
    for sid, items in by_species.items():
        if len(items) <= 1:
            continue
        diam_values = [r["particle_diameter_ang"] for r in items]
        if max(diam_values) - min(diam_values) > 1e-3 * max(diam_values):
            msg = (
                f"Particle diameter differs across candidate-extract instances for "
                f"species '{sid or '—'}' ({min(diam_values):g}–{max(diam_values):g} Å) — "
                f"likely a binning-arithmetic mistake."
            )
            for r in items:
                r["warnings"]["particle"] = ("warn", msg)

    # Box vs particle diameter  (TM and Subtomo)
    # Box vs particle Ø — tighter zones than the older 1.5–3.0× window
    # (per JOURNEY_CANDIDATE_METRICS.md §"Box and crop sizing rationality"):
    #   red  < 1.5×  (particle won't fit; tight Refine3D shifts will clip)
    #   amber 1.5–2.0× (acceptable but no margin for refinement)
    #   green 2.0–3.0×
    #   amber > 3.0× (wasted compute)
    for r in rows:
        if r["stage_key"] not in ("tm", "subtomo"):
            continue
        b = r.get("box_ang")
        d = r.get("particle_diameter_ang")
        if not b or not d:
            continue
        ratio = b / d
        if ratio < 1.5:
            r["warnings"]["box"] = (
                "error",
                f"Box {b:g} Å is {ratio:.2f}× particle diameter {d:g} Å — particle won't fit. "
                f"Aim for ≥ 2.0× (≥ 1.5× absolute floor).",
            )
        elif ratio < 2.0:
            r["warnings"]["box"] = (
                "warn",
                f"Box {b:g} Å is {ratio:.2f}× particle diameter {d:g} Å — tight; no margin for "
                f"Refine3D shifts. Aim for ≥ 2.0×.",
            )
        elif ratio > 3.0:
            r["warnings"]["box"] = (
                "warn",
                f"Box {b:g} Å is {ratio:.2f}× particle diameter {d:g} Å — wasted compute. Aim for 2.0–3.0×.",
            )

    # Template volume px vs recon px (silent mismatch ⇒ garbage picks)
    if recon_px:
        for r in rows:
            if r["stage_key"] != "tm":
                continue
            wb_px = r.get("_template_workbench_px")
            if wb_px and abs(wb_px - recon_px) / recon_px > 0.05:
                r["warnings"]["px"] = (
                    "error",
                    f"Template prepared at {wb_px:g} Å/px but reconstruction is at "
                    f"{recon_px:g} Å/px. Picks will be unreliable from this mismatch. "
                    f"Re-render the template at {recon_px:g} Å/px (simpler than re-running "
                    f"the reconstruction; templates are cheap to regenerate).",
                )

    # Subtomo crop sanity
    #
    # Crop ratio thresholds (per JOURNEY_CANDIDATE_METRICS.md):
    #   red    crop < diameter  (particle clipped — absolute floor)
    #   red    crop > box       (invalid; crop must fit inside box)
    #   amber  crop / diameter  < 1.2× (tight; no margin for shifts)
    #   green  crop / diameter ≥ 1.5×
    for r in rows:
        if r["stage_key"] != "subtomo":
            continue
        crop = r.get("_crop_px")
        box = r.get("box_px")
        eff_px = r.get("px_size_ang")
        diameter = r.get("particle_diameter_ang")
        if box and crop is not None and crop > box:
            r["warnings"]["crop"] = (
                "error",
                f"crop ({crop} px) > box ({box} px) — invalid; crop must fit within the box.",
            )
            continue
        # Cropped volume must contain the particle (≥ 1× diameter is the
        # absolute floor; below that, the particle doesn't fit in the cropped
        # output cube and gets clipped). Between 1.0–1.2× is the "tight,
        # no margin" warn zone; ≥ 1.5× is the comfortable target.
        if crop and eff_px and diameter:
            crop_ang = crop * eff_px
            ratio = crop_ang / diameter
            if ratio < 1.0:
                r["warnings"]["crop"] = (
                    "error",
                    f"crop {crop_ang:g} Å ({crop} px) < particle diameter {diameter:g} Å — "
                    f"particle won't fit in the cropped subtomogram. Increase crop_size.",
                )
            elif ratio < 1.2:
                r["warnings"]["crop"] = (
                    "warn",
                    f"crop {crop_ang:g} Å is {ratio:.2f}× particle diameter {diameter:g} Å — "
                    f"tight; only {(ratio - 1) * 50:.0f}% margin per side around the particle. "
                    f"Refine3D shifts may clip. Aim for ≥ 1.5×.",
                )


# --- Sanity-table renderers --------------------------------------------------


def _fmt_px(v: Optional[float]) -> str:
    return "—" if not v else f"{v:g}"


def _fmt_dims_px(d: Optional[tuple]) -> str:
    if d is None:
        return "—"
    parts = [str(x) for x in d if x is not None]
    return " × ".join(parts) if parts else "—"


def _fmt_dims_ang(d: Optional[tuple], px: Optional[float]) -> str:
    if d is None or not px:
        return "—"
    vals = [int(round(x * px)) for x in d if x is not None]
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


def _pixel_cell(text: str, warning: Optional[tuple[str, str]] = None, *, notes: bool = False) -> None:
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


def _group_rows_by_species(rows: list[dict]) -> tuple[list[dict], list[tuple[Optional[str], list[dict]]]]:
    """Split into (universal_rows, [(species_id, species_rows), ...]).
    Universal stages share one table; per-species stages each get their own
    sub-table so multi-species projects stay readable."""
    universal: list[dict] = []
    by_species: dict[Optional[str], list[dict]] = {}
    species_order: list[Optional[str]] = []
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


def _render_pixel_sanity_table(rows: list[dict]) -> None:
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

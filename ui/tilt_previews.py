"""Tilt previews: every tilt of the project as the PNG the tilt filter makes of its
motion-corrected average, grouped by tilt series, each showing what it is.

The Tomograms view shows them two ways (``ui/tomo_gallery.py``): the Tilts tab lists every
series as a grid of cards under a header with a strip of its tilts, and a tile on the
tomogram wall can swap its slice for a mosaic of its own series' tilts, in the box the slice
occupied. Both come from here: the collection (the registry plus one listing of the PNG
directory), the card, mosaic, header and caption HTML, the mosaic's fit, and the click
scripts.

Every tilt carries its state (``services/tilt_series/tilt_state.py``): whether it is in the
tomogram and, if not, why; what the uncommitted review says; the model's P(bad) in DL review;
whether its exposure was dark. A card shows it as its border, stripes and caption, a mosaic
cell as stripes and an amber border, a header strip as one tick per tilt, and a tomogram's
caption sums its series up. A dark exposure is a marker only: nothing here drops a tilt.

A project holds thousands of tilts, so a series is one HTML string with one delegated click
handler rather than an element per tilt. A tilt opens the tilt filter's full-size viewer,
which re-renders the averaged MRC.
"""

from __future__ import annotations

import html
import logging
import math
import statistics
import urllib.parse
from collections.abc import Iterable, Sequence
from pathlib import Path

from services.dashboard_data import find_job_by_type, position_label
from services.models_base import JobStatus, JobType
from services.tilt_series import get_registry_for
from services.tilt_series.tilt_state import (
    BLANK_EXPOSURE_FRACTION,
    DIM_EXPOSURE_FRACTION,
    SeriesSummary,
    TiltState,
    series_summary,
    tilt_states,
)
from ui.components.dialogs import dialog_host

logger = logging.getLogger(__name__)

# One delegated handler per series: the tilt under the click, by its frame id.
TILT_CLICK_JS = """(event) => {
    const tilt = event.target.closest('[data-key]');
    if (tilt) emit({key: tilt.dataset.key});
}"""

# The strip sits in a group header, whose click opens and closes the group: a tick keeps its
# click to itself, anywhere else on the header still toggles.
STRIP_CLICK_JS = """(event) => {
    const tick = event.target.closest('[data-key]');
    if (!tick) return;
    event.stopPropagation();
    emit({key: tick.dataset.key});
}"""

STRIP_PX = 200
_AMBER_TEXT = "#b45309"
_RED_TEXT = "#be4343"
_MUTED = "#94a3b8"

DARK_RULE = (
    f"Dark exposure: the tilt's mdoc mean counts are under {DIM_EXPOSURE_FRACTION:.0%} of its tilt series' "
    f"median; under {BLANK_EXPOSURE_FRACTION:.0%} it is blank (the beam was blocked, by a grid bar or the "
    "lamella edge). A blank exposure in a tomogram back-projects as straight streaks. This is a marker "
    "only: nothing is dropped because of it. The tilt filter's review decides."
)

# Card look → (legend text, legend tooltip, swatch background, swatch border), strongest first.
_LEGEND = (
    (
        "cb-ts-drop",
        "dropped by the tilt filter",
        "The committed tilt-filter verdict drops this tilt, so alignment leaves it out of the tomogram.",
        "repeating-linear-gradient(135deg, transparent 0 3px, rgba(239,68,68,0.8) 3px 4px)",
        "1.5px solid #ef4444",
    ),
    (
        "cb-ts-out",
        "not in alignment's output",
        "Not in alignment's output although no verdict drops it: Warp's import drops dark tilts at the end of "
        "a series, or the verdict predates the registry.",
        "repeating-linear-gradient(135deg, transparent 0 3px, rgba(148,163,184,0.9) 3px 4px)",
        "1px solid #cbd5e1",
    ),
    (
        "cb-ts-hbad",
        "your label: bad",
        "You labelled it bad and the review is not approved yet; Approve drops it.",
        "transparent",
        "1.5px solid #ef4444",
    ),
    (
        "cb-ts-mbad",
        "the model's call: bad",
        "The model's P(bad) is at or above the threshold and the review is not approved yet; Approve drops it "
        "unless you label it good.",
        "transparent",
        "1.5px dashed #ef4444",
    ),
    (
        "cb-ts-dark",
        f"dark exposure (under {DIM_EXPOSURE_FRACTION:.0%} of its series' median counts): a marker, never a drop",
        DARK_RULE,
        "transparent",
        "1.5px solid #f59e0b",
    ),
)

# A tick on the header strip takes its card's look.
_TICK_CLASS = {
    "cb-ts-drop": "cb-tk-drop",
    "cb-ts-out": "cb-tk-out",
    "cb-ts-hbad": "cb-tk-flag",
    "cb-ts-mbad": "cb-tk-flag",
    "cb-ts-dark": "cb-tk-dark",
    "": "",
}


# ── Collection ────────────────────────────────────────────────────────────────


def registry_tilt_series(project_path: Path) -> tuple[list, str | None]:
    """The registry's tilt series, and the error to state when it cannot be read.

    Call on the event loop: the cached registry is shared with every reader, and a re-sync
    started from a worker thread could change it under one of them."""
    try:
        return list(get_registry_for(project_path).all_tilt_series()), None
    except Exception:
        logger.exception("Tilt previews: the tilt-series registry of %s could not be read", project_path)
        return [], "The tilt-series registry could not be read; see the server log."


def tilt_context(state) -> dict:
    """What a tilt's state depends on besides the registry: the jobs whose outputs say which
    tilts are in the tomogram and carry the metrics, and the tilt filter's review as it stands.
    Call on the event loop (the job models are live state); the result is a plain snapshot."""

    def instance(jt: JobType) -> str | None:
        found = find_job_by_type(state, jt)
        return found[0] if found else None

    found = find_job_by_type(state, JobType.TILT_FILTER)
    jm = found[1] if found else None
    return {
        "alignment_instance": instance(JobType.TS_ALIGNMENT),
        "ts_ctf_instance": instance(JobType.TS_CTF),
        "fs_motion_instance": instance(JobType.FS_MOTION_CTF),
        "committed": jm is not None and jm.execution_status == JobStatus.SUCCEEDED,
        "mode": jm.mode if jm is not None else None,
        "labels": dict(jm.tilt_labels) if jm is not None else {},
        "threshold": jm.threshold if jm is not None else None,
    }


def collect_tilt_groups(tilt_series: Iterable, png_dir: Path, ctx: dict) -> tuple[list[dict], dict]:
    """One group per tilt series with something to show, ordered like the tomogram wall, and
    the facts about the whole project the Tilts tab states once.

    A group is ``{ts, label, order, tilts, n_png, summary, ticks, ctf_fit, counts_known}``:
    ``tilts`` are the tilts that show, each ``{key, angle, number, png, mrc, state, tip, look,
    cell_look, cap}``; ``ticks`` is every tilt of the series as ``(state, title)`` for the
    header strip; ``summary`` the series' ``SeriesSummary``; ``ctf_fit`` its tsCtf fit
    resolution (Å) or None. The facts are ``{span, looks, lacking, n_series}``: the range of
    every stage tilt, the card looks present, and per field family the number of series
    without it.

    A tilt shows when it has a preview PNG or a motion-corrected average, the full-size
    viewer's source. Its PNG is named after that average (the thumbnail pass converts the
    averages), else after the frame id. Tilts run by angle, most negative first. Disk: one
    listing of the PNG directory. Safe in a thread."""
    pngs = {p.stem: p for p in png_dir.glob("*.png")}
    review = {k: ctx[k] for k in ("alignment_instance", "committed", "labels", "threshold", "mode")}
    groups: list[dict] = []
    angles: list[float] = []
    looks: set[str] = set()
    lacking = {"counts": 0, "qc": 0, "alignment": 0}
    for ts in tilt_series:
        states = {s.frame_id: s for s in tilt_states(ts, **review)}
        metrics = _tilt_metrics(ts, ctx)
        tilts = []
        for frame in ts.frames:
            avg = next((o.averaged_mrc for o in frame.outputs.values() if o.output_type == "fs_motion_ctf"), None)
            png = (pngs.get(Path(avg).stem) if avg is not None else None) or pngs.get(frame.id)
            if avg is None and png is None:
                continue
            s = states[frame.id]
            tilts.append(
                {
                    "key": frame.id,
                    "angle": frame.nominal_tilt_angle_deg,
                    "number": s.number,
                    "png": str(png) if png is not None else None,
                    "mrc": str(avg) if avg is not None else "",
                    "state": s,
                    "tip": tilt_tip(s, metrics[frame.id], ctx["threshold"]),
                    "look": _look(s),
                    "cell_look": _look(s, review=False),
                    "cap": _card_caption(s, ctx["threshold"]),
                }
            )
        if not tilts:
            continue
        tilts.sort(key=lambda t: (t["angle"], t["number"]))
        ordered = sorted(states.values(), key=lambda s: (s.angle, s.number))
        angles.extend(s.angle for s in ordered)
        looks.update(_look(s) for s in ordered)
        ctf_out = ts.outputs.get(ctx["ts_ctf_instance"]) if ctx["ts_ctf_instance"] else None
        counts_known = any(f.mean_intensity is not None for f in ts.frames)
        lacking["counts"] += not counts_known
        lacking["qc"] += _lacks_qc(ts, ctx["fs_motion_instance"])
        aln_out = ts.outputs.get(ctx["alignment_instance"]) if ctx["alignment_instance"] else None
        lacking["alignment"] += aln_out is not None and aln_out.output_type == "ts_alignment" and not aln_out.per_frame
        label, order = position_label(ts.id)
        groups.append(
            {
                "ts": ts.id,
                "label": label,
                "order": order,
                "tilts": tilts,
                "n_png": sum(1 for t in tilts if t["png"]),
                "summary": series_summary(ordered),
                "ticks": [(s, _tick_title(s)) for s in ordered],
                "ctf_fit": ctf_out.ctf_resolution if ctf_out is not None and ctf_out.output_type == "ts_ctf" else None,
                "counts_known": counts_known,
            }
        )
    groups.sort(key=lambda g: (g["order"], g["ts"]))
    looks.discard("")
    facts = {
        "span": (min(angles), max(angles)) if angles else None,
        "looks": looks,
        "lacking": lacking,
        "n_series": len(groups),
    }
    return groups, facts


def _lacks_qc(ts, fs_motion_instance: str | None) -> bool:
    """fsMotion has outputs for the series but none of them carries the per-tilt CTF fit or
    motion (an ingest older than those fields)."""
    outs = [f.outputs[fs_motion_instance] for f in ts.frames if fs_motion_instance in f.outputs]
    return bool(outs) and all(o.ctf_resolution is None and o.mean_frame_movement is None for o in outs)


def _tilt_metrics(ts, ctx: dict) -> dict[str, dict]:
    """Each frame's recorded numbers for its tooltip, by frame id. Δ defocus is against the
    series' median, from CTF after alignment for the tilts it fit, else from Motion & CTF;
    the dose before a tilt counts as unknown when the mdoc recorded none (every tilt reads 0)."""
    fsm, ctf, aln = ctx["fs_motion_instance"], ctx["ts_ctf_instance"], ctx["alignment_instance"]
    fs_outs = {f.id: f.outputs[fsm] for f in ts.frames if fsm and fsm in f.outputs}
    fs_defocus = {fid: (o.defocus_u_angstrom + o.defocus_v_angstrom) / 2e4 for fid, o in fs_outs.items()}
    ctf_out = ts.outputs.get(ctf) if ctf else None
    ts_defocus = (
        {e.frame_id: (e.defocus_u_angstrom + e.defocus_v_angstrom) / 2e4 for e in ctf_out.per_frame}
        if ctf_out is not None and ctf_out.output_type == "ts_ctf"
        else {}
    )
    fs_median = statistics.median(fs_defocus.values()) if fs_defocus else None
    ts_median = statistics.median(ts_defocus.values()) if ts_defocus else None
    aln_out = ts.outputs.get(aln) if aln else None
    shifts = (
        {e.frame_id: math.hypot(e.x_shift_angstrom, e.y_shift_angstrom) for e in aln_out.per_frame}
        if aln_out is not None and aln_out.output_type == "ts_alignment"
        else {}
    )
    dose_known = any(f.pre_exposure_e_per_a2 for f in ts.frames)
    means = [f.mean_intensity for f in ts.frames if f.mean_intensity is not None]
    median_counts = statistics.median(means) if means else None
    out: dict[str, dict] = {}
    for f in ts.frames:
        o = fs_outs.get(f.id)
        if f.id in ts_defocus:
            ddefocus, source = ts_defocus[f.id] - ts_median, "CTF after alignment"
        elif f.id in fs_defocus:
            ddefocus, source = fs_defocus[f.id] - fs_median, "Motion & CTF"
        else:
            ddefocus, source = None, None
        out[f.id] = {
            "ctf_res": o.ctf_resolution if o is not None else None,
            "motion": o.mean_frame_movement if o is not None else None,
            "ddefocus": ddefocus,
            "defocus_source": source,
            "dose": f.pre_exposure_e_per_a2 if dose_known else None,
            "shift": shifts.get(f.id),
            "counts": f.mean_intensity,
            "median_counts": median_counts,
        }
    return out


def thumbnail_task_key(project_path: Path, png_dir: Path) -> str:
    """The dedup key the thumbnail pass runs under (``ensure_tilt_thumbnails`` and the tilt
    filter's Generate button), so a page can tell while it runs."""
    return f"tilt-filter-thumbnails:{project_path}:{png_dir}"


# ── Words ─────────────────────────────────────────────────────────────────────


def _signed(value: float, digits: int) -> str:
    """A signed number with a true minus sign: +42.0, −12.0."""
    return f"{value:+.{digits}f}".replace("-", "−")


def _pct_short(ratio: float) -> str:
    pct = ratio * 100
    return "<0.1%" if pct < 0.1 else f"{pct:.1f}%"


def _pct_fine(ratio: float) -> str:
    pct = ratio * 100
    if pct < 0.001:
        return "<0.001 %"
    if pct < 1:
        return f"{pct:.2g} %"
    return f"{pct:.1f} %" if pct < 10 else f"{pct:.0f} %"


def _angles(states: Sequence[TiltState], limit: int = 8) -> str:
    shown = ", ".join(f"{_signed(s.angle, 0)}°" for s in sorted(states, key=lambda s: s.angle)[:limit])
    return shown + (f" and {len(states) - limit} more" if len(states) > limit else "")


def where_sentence(s: TiltState) -> str:
    """Whether the tilt is in the tomogram and, if not, why, as one sentence."""
    if s.drop is not None:
        why = "" if s.drop == "tilt-filter" else f" ({s.drop})"
        if s.in_tomogram:
            return f"Dropped by the tilt filter{why}, yet in alignment's output: alignment ran before this verdict."
        if s.in_tomogram is None:
            return f"Dropped by the tilt filter{why}; alignment has not run for this series."
        return f"Dropped by the tilt filter{why}: not in the tomogram."
    if s.in_tomogram is False:
        return (
            "Not in alignment's output, and no verdict drops it: Warp's import drops dark tilts at the end of a "
            "series, or the verdict predates the registry."
        )
    if s.in_tomogram:
        return "In the tomogram."
    return "Alignment has not recorded this series yet."


def _review_sentence(s: TiltState) -> str | None:
    return {
        "human_bad": "Your label: bad, not approved yet. Approve drops it.",
        "human_good": "Your label: good.",
        "model_bad": "The model's call: bad, not approved yet. Approve drops it unless you label it good.",
    }.get(s.review or "")


def tilt_tip(s: TiltState, m: dict, threshold: float | None) -> str:
    """A tilt's native tooltip, one fact per line: what it is, then its recorded numbers, then
    what is not recorded."""
    lines = [f"{_signed(s.angle, 1)}° · tilt {s.number} · {s.frame_id}", where_sentence(s)]
    review = _review_sentence(s)
    if review:
        lines.append(review)
    if s.p_bad is not None:
        cut = f" against the threshold {threshold:.2f}" if threshold is not None else ""
        lines.append(f"P(bad) {s.p_bad:.2f}{cut}: the model's ranking, not a calibrated probability.")
    missing: list[str] = []
    if s.exposure_ratio is not None:
        ratio = (
            f"{_pct_fine(s.exposure_ratio)} of the series' median mdoc counts "
            f"({m['counts']:.3g} against {m['median_counts']:.3g})"
        )
        if s.is_dark:
            lines.append(f"Dark exposure ({s.exposure}): {ratio}.")
            lines.append(DARK_RULE)
        else:
            lines.append(f"Exposure: {ratio}.")
    else:
        missing.append("mdoc counts" if m["counts"] is None else "an exposure ratio (the series' median counts are 0)")
    numbers: list[str] = []
    for value, shown, name in (
        (m["ctf_res"], lambda v: f"CTF fit {v:.1f} Å", "CTF fit"),
        (m["motion"], lambda v: f"motion {v:.2f}", "motion"),
        (m["ddefocus"], lambda v: f"Δ defocus {v:+.2f} µm ({m['defocus_source']})", "defocus"),
        (m["dose"], lambda v: f"{v:.1f} e⁻/Å² before", "dose"),
    ):
        if value is None:
            missing.append(name)
        else:
            numbers.append(shown(value))
    if m["shift"] is not None:
        numbers.append(f"alignment shift {m['shift']:.0f} Å")
    if numbers:
        lines.append(" · ".join(numbers))
    if missing:
        lines.append("Not recorded: " + ", ".join(missing) + ".")
    return "\n".join(lines)


def _tick_title(s: TiltState) -> str:
    if s.drop is not None:
        what = "dropped by the tilt filter"
    elif s.in_tomogram is False:
        what = "not in alignment's output"
    elif s.flagged:
        what = "marked bad, not approved yet"
    elif s.is_dark:
        what = f"dark exposure, {_pct_fine(s.exposure_ratio)} of the series' median counts"
    elif s.in_tomogram:
        what = "in the tomogram"
    else:
        what = "not aligned yet"
    return f"{_signed(s.angle, 1)}° · tilt {s.number}: {what}"


# ── HTML ──────────────────────────────────────────────────────────────────────


def _look(s: TiltState, *, review: bool = True) -> str:
    """The one state class a card (``review``) or a mosaic cell takes, strongest first: the
    stripes, then the review's border, then amber for a dark exposure."""
    if s.drop is not None:
        return "cb-ts-drop"
    if s.in_tomogram is False:
        return "cb-ts-out"
    if review and s.review == "human_bad":
        return "cb-ts-hbad"
    if review and s.review == "model_bad":
        return "cb-ts-mbad"
    if s.is_dark:
        return "cb-ts-dark"
    return ""


def _card_caption(s: TiltState, threshold: float | None) -> str:
    """The angle, the dark token and P(bad) where they apply, the tilt number. The row clips
    from the right, so the angle and the state survive the smallest card."""
    parts = [f"<b>{_signed(s.angle, 1)}°</b>"]
    if s.is_dark:
        parts.append(f'<span style="color:{_AMBER_TEXT};font-weight:600;">dark {_pct_short(s.exposure_ratio)}</span>')
    if s.p_bad is not None:
        hot = threshold is not None and s.p_bad >= threshold
        parts.append(f'<span style="color:{_RED_TEXT if hot else _MUTED};">p{s.p_bad:.2f}</span>')
    parts.append(f"<span>#{s.number}</span>")
    return "".join(parts)


def _image(tilt: dict) -> str:
    """The tilt's PNG as a square filling its box's width, or a dark square where it has none."""
    if tilt["png"]:
        url = f"/api/tilt-thumb?path={urllib.parse.quote(tilt['png'], safe='')}"
        return (
            f'<img src="{url}" loading="lazy" alt="" style="width:100%;aspect-ratio:1;object-fit:cover;display:block;">'
        )
    return '<div style="width:100%;aspect-ratio:1;background:#1e293b;"></div>'


def card_grid_html(tilts: list[dict], card_px: int) -> str:
    """One series' tilts as cards for the Tilts tab: the preview, with the tilt's state as its
    border and stripes, over a caption row."""
    cards = "".join(
        f'<div class="cb-tp-card {t["look"]}" data-key="{html.escape(t["key"])}" title="{html.escape(t["tip"])}">'
        f'<div class="cb-tp-img">{_image(t)}</div>'
        f'<div class="cb-tp-cap">{t["cap"]}</div>'
        "</div>"
        for t in tilts
    )
    return (
        f'<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax({card_px}px,1fr));gap:4px;">'
        f"{cards}</div>"
    )


def mosaic_layout(n: int, aspect: float) -> tuple[int, int]:
    """(cols, rows) giving n square cells the largest size in a frame of aspect W/H. Ties go
    to more columns: 41 tilts in a square frame are 7 × 6."""
    best, best_side = (1, n), -1.0
    for cols in range(1, n + 1):
        rows = math.ceil(n / cols)
        side = min(aspect / cols, 1 / rows)  # in units of the frame height
        if side >= best_side:
            best, best_side = (cols, rows), side
    return best


def mosaic_html(tilts: list[dict], aspect: float | None) -> tuple[str, str | None]:
    """A series' tilts as square cells fitted into a tile's frame of aspect W/H, centred both
    ways, in angle order from the top left; a cell shows what went into the tomogram (the
    stripes) and whether something dark did (amber). Returns the HTML and, for a frame with
    no known extent (``aspect`` None), the CSS aspect-ratio the frame takes so the grid fills
    it.

    The grid sits in a box placed absolutely over the frame, so its size is a percentage of
    the frame and follows the tile size with no re-render. Each cell's 1 px padding is the
    gutter; a CSS gap would add to the percentages and overflow the frame."""
    cols, rows = mosaic_layout(len(tilts), aspect or 1.0)
    frame_aspect = aspect or cols / rows
    width_pct = cols * min(100 / cols, 100 / (frame_aspect * rows))
    cells = "".join(
        f'<div class="cb-tp-cell {t["cell_look"]}" data-key="{html.escape(t["key"])}" '
        f'title="{html.escape(t["tip"])}" style="padding:1px;box-sizing:border-box;min-width:0;">'
        f'<div class="cb-tp-img">{_image(t)}</div></div>'
        for t in tilts
    )
    grid = (
        '<div style="position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);'
        f'width:{width_pct:.3f}%;display:grid;grid-template-columns:repeat({cols},minmax(0,1fr));">'
        f"{cells}</div>"
    )
    return grid, (None if aspect else f"{cols}/{rows}")


def _strip_html(ticks: list[tuple[TiltState, str]], span: tuple[float, float]) -> str:
    """One tick per tilt at its stage tilt, on the axis every header shares, with a faint 0°
    mark. A tick takes its card's look; its title names the tilt."""
    lo, hi = span
    width = (hi - lo) or 1.0

    def x(angle: float) -> str:
        return f"{(angle - lo) / width * 100:.2f}%"

    zero = (
        f'<div style="position:absolute;left:{x(0.0)};top:0;bottom:0;width:1px;background:#e2e8f0;"></div>'
        if lo < 0 < hi
        else ""
    )
    marks = "".join(
        f'<span class="cb-tp-tick {_TICK_CLASS[_look(s)]}" data-key="{html.escape(s.frame_id)}" '
        f'title="{html.escape(title)}" style="left:{x(s.angle)};"></span>'
        for s, title in ticks
    )
    legend = (
        f"Every tilt at its stage tilt, {_signed(lo, 0)}° to {_signed(hi, 0)}° across the project. Red: dropped "
        "by the tilt filter. Pale grey: not in alignment's output. Red outline: marked bad, not approved yet. "
        "Amber: dark exposure (a marker, never a drop). Click a tick to view the tilt."
    )
    return (
        f'<div class="cb-tp-strip" title="{html.escape(legend)}" '
        f'style="position:relative;width:{STRIP_PX}px;height:12px;flex:0 0 auto;">{zero}{marks}</div>'
    )


def group_header_html(group: dict, span: tuple[float, float] | None) -> str:
    """What a group's tilts are, after its tilt count: how many are in the tomogram, how many
    the uncommitted review flags, how many are dark with nothing dropping them; then the strip.
    Collapsed, the headers read as the project's tilt scheme: where each series loses tilts,
    and its missing wedge."""
    s: SeriesSummary = group["summary"]
    words: list[tuple[str, str, str]] = []
    if s.used:
        words.append((f"{s.used} in the tomogram", "#64748b", _used_title(s)))
    if s.flagged:
        n_model = sum(1 for t in s.flagged if t.review == "model_bad")
        words.append(
            (
                f"{len(s.flagged)} flagged",
                _RED_TEXT,
                f"Marked bad in the review, not approved yet: {n_model} by the model, "
                f"{len(s.flagged) - n_model} by your labels. Approve drops them. {_angles(s.flagged)}.",
            )
        )
    if s.dark_unflagged:
        words.append(
            (
                f"{len(s.dark_unflagged)} dark",
                _AMBER_TEXT,
                f"{len(s.dark_unflagged)} dark exposures that nothing drops and the review does not flag: "
                f"{_dark_list(s.dark_unflagged)}.\n{DARK_RULE}",
            )
        )
    text = ' <span style="color:#cbd5e1;">·</span> '.join(
        f'<span style="color:{color};" title="{html.escape(title)}">{html.escape(word)}</span>'
        for word, color, title in words
    )
    strip = _strip_html(group["ticks"], span) if span else ""
    return (
        '<div style="display:flex;align-items:center;gap:10px;font-family:ui-monospace,monospace;font-size:9px;'
        f'white-space:nowrap;"><span>{text}</span>{strip}</div>'
    )


def _dark_list(states: Sequence[TiltState]) -> str:
    return ", ".join(f"{_signed(t.angle, 0)}° ({_pct_fine(t.exposure_ratio)})" for t in states)


def _used_title(s: SeriesSummary) -> str:
    lines = [f"{s.used} of {s.total} tilts are in the tomogram (alignment's output)."]
    if s.dropped:
        lines.append(f"{len(s.dropped)} dropped by the tilt filter: {_angles(s.dropped)}.")
    if s.left_out:
        lines.append(
            f"{len(s.left_out)} not in alignment's output with no verdict against them (Warp's import drops dark "
            f"tilts at the end of a series): {_angles(s.left_out)}."
        )
    return "\n".join(lines)


def tomogram_caption_html(group: dict, *, compact: bool) -> str:
    """A tomogram tile's caption numbers: the tilts used of the series' total; unless
    ``compact``, the stage-tilt range in use and the series' CTF fit; in amber, the dark
    exposures in use. The tooltip breaks them down."""
    s: SeriesSummary = group["summary"]
    parts = [f"{s.used}/{s.total}" if s.used is not None else f"{s.total} tilts"]
    if not compact:
        if s.kept_range is not None:
            parts.append(f"{_signed(s.kept_range[0], 0)}…{_signed(s.kept_range[1], 0)}°")
        if group["ctf_fit"] is not None:
            parts.append(f"{group['ctf_fit']:.1f} Å")
    text = html.escape(" · ".join(parts))
    if s.dark_in_use:
        text += f' · <span style="color:{_AMBER_TEXT};font-weight:600;">{len(s.dark_in_use)} dark</span>'
    return (
        f'<span title="{html.escape(_caption_title(group))}" style="font-family:ui-monospace,monospace;'
        f'font-size:9px;color:#64748b;white-space:nowrap;flex:0 0 auto;">{text}</span>'
    )


def _caption_title(group: dict) -> str:
    s: SeriesSummary = group["summary"]
    if s.used is None:
        lines = [
            f"{s.total} tilts. Which of them are in the tomogram is not recorded: the registry holds no per-tilt "
            "list from alignment."
        ]
    else:
        lines = [_used_title(s)]
    if s.kept_range is not None:
        lines.append(f"Stage tilts in use: {_signed(s.kept_range[0], 1)}° to {_signed(s.kept_range[1], 1)}°.")
    if group["ctf_fit"] is not None:
        lines.append(f"CTF fit {group['ctf_fit']:.1f} Å: Warp's estimate for the whole tilt series.")
    if s.dark_in_use:
        lines.append(f"{len(s.dark_in_use)} dark exposures in use: {_dark_list(s.dark_in_use)}.")
        lines.append(DARK_RULE)
    elif not group["counts_known"]:
        lines.append("Dark exposures cannot be marked: the registry holds no mdoc counts for this series.")
    return "\n".join(lines)


def legend_html(looks: set[str]) -> str:
    """One line naming each look the project's tilts take, each with its explanation as a
    tooltip."""
    items = "".join(
        f'<span title="{html.escape(title)}" style="display:inline-flex;align-items:center;gap:4px;">'
        f'<span style="width:10px;height:10px;box-sizing:border-box;border:{border};background:{bg};'
        f'border-radius:2px;"></span>{html.escape(text)}</span>'
        for look, text, title, bg, border in _LEGEND
        if look in looks
    )
    return f'<div style="display:flex;flex-wrap:wrap;gap:4px 14px;font-size:9px;color:#64748b;">{items}</div>'


# ── The viewer ────────────────────────────────────────────────────────────────


def open_tilt_viewer(tilt: dict, project_path: Path) -> None:
    """The tilt filter's full-size viewer on one tilt's averaged MRC (512 px to full size),
    parented at the page layout so a re-render of the tile or group that opened it cannot
    delete it mid-load."""
    from ui.tilt_filter_panel import _show_upsample

    with dialog_host():
        _show_upsample(tilt["mrc"], tilt["key"], Path(project_path))

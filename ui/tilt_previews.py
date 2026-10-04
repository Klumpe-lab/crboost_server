"""Tilt previews: every tilt of the project as the PNG the tilt filter makes of its
motion-corrected average, grouped by tilt series, each showing what it is.

Two hosts show them. The Tomograms view (``ui/tomo_gallery.py``) has a Tilts tab, and a tile on
its tomogram wall can swap its slice for a mosaic of its own series' tilts, in the box the slice
occupied. The tilt filter's job page shows the same gallery in review mode. Both come from
here: the collection (the registry plus one listing of the PNG directory), the gallery
(``TiltGallery``), the card, mosaic, header and caption HTML, the mosaic's fit, and the click
scripts.

Every tilt carries its state (``services/tilt_series/tilt_state.py``): whether it is in the
tomogram and, if not, why; what the uncommitted review says; the model's confidence score in
the DL modes; whether its exposure was dark. The action and the information look different:
a red border is the one exclusion, whoever set it (a manual label, the confidence score at the
threshold, the committed verdict), and what only informs sits over the image as small icons
(dark exposure, not in alignment's output, an outlier in a ticked metric), each explained on
hover. A mosaic cell shows the committed exclusion and dots for the same information, a header
strip one tick per tilt, and a tomogram's caption sums its series up. A card's caption carries
the tilt's recorded numbers, shown as the gallery's metrics menu says, violet where the number
is an outlier among the project's tilts at the same stage tilt. A dark exposure and an outlier
are markers only: nothing here excludes a tilt.

A project holds thousands of tilts, so a series is one HTML string with one delegated click
handler rather than an element per tilt. In the review a click on a card toggles the tilt's
exclusion; its magnifier, and a card click elsewhere, open the tilt filter's full-size viewer,
which re-renders the averaged MRC.
"""

from __future__ import annotations

import asyncio
import base64
import html
import json
import logging
import math
import statistics
import urllib.parse
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nicegui import ui

from services.dashboard_data import find_job_by_type, position_label
from services.jobs.tilt_filter import DL_MODES
from services.models_base import JobStatus, JobType
from services.tilt_series import get_registry_for
from services.tilt_series.tilt_state import (
    BLANK_EXPOSURE_FRACTION,
    DIM_EXPOSURE_FRACTION,
    OUTLIER_BAND_DEG,
    OUTLIER_MIN_BAND,
    OUTLIER_ROBUST_SDS,
    BandStat,
    SeriesSummary,
    TiltState,
    band_outliers,
    series_summary,
    tilt_states,
)
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.segmented import render_segmented
from ui.styles import MONO, SANS

logger = logging.getLogger(__name__)

# One delegated handler per series: the tilt under the click, by its frame id.
TILT_CLICK_JS = """(event) => {
    const tilt = event.target.closest('[data-key]');
    if (tilt) emit({key: tilt.dataset.key});
}"""

# The strip sits in a group header, whose click opens and closes the group: a tick keeps its
# click to itself, anywhere else on the header still toggles. A tick opens the viewer.
STRIP_CLICK_JS = """(event) => {
    const tick = event.target.closest('[data-key]');
    if (!tick) return;
    event.stopPropagation();
    emit({key: tick.dataset.key, zoom: true});
}"""

# A card grid's handlers. The popover is the gallery's own (a direct child of its root), found
# from the grid, never by a document-wide query: the Tilts tab and the job page can both be in
# the page, holding the same tilts. Its anchor is a caption or an info icon, each carrying the
# popover's content as a hidden child. Fixed-positioned at the anchor, so a group's
# `overflow: hidden` cannot clip it. Hover and pinning run in the browser alone.
_POPOVER_AT_ANCHOR = """
    const root = event.currentTarget.closest('.cb-tg-root');
    const pop = root && root.querySelector(':scope > .cb-tp-popover');
    const src = anchor.querySelector(':scope > .cb-tp-popsrc');
    if (!pop || !src) return;
    if (pinning && pop._unpin) pop._unpin();
    if (pop.dataset.pinned && !pinning) return;
    pop.innerHTML = src.innerHTML;
    pop.style.display = 'block';
    const r = anchor.getBoundingClientRect();
    const w = pop.offsetWidth, h = pop.offsetHeight;
    const below = r.bottom + 4 + h <= window.innerHeight - 8;
    pop.style.left = Math.max(8, Math.min(r.left, window.innerWidth - w - 8)) + 'px';
    pop.style.top = (below ? r.bottom + 4 : Math.max(8, r.top - h - 4)) + 'px';
"""

GRID_HOVER_JS = (
    """(event) => {
    const anchor = event.target.closest('.cb-tp-cap, .cb-ti');
    if (!anchor) return;
    const pinning = false;"""
    + _POPOVER_AT_ANCHOR
    + "}"
)

GRID_OUT_JS = """(event) => {
    const anchor = event.target.closest('.cb-tp-cap, .cb-ti');
    if (!anchor || anchor.contains(event.relatedTarget)) return;
    const root = event.currentTarget.closest('.cb-tg-root');
    const pop = root && root.querySelector(':scope > .cb-tp-popover');
    if (pop && !pop.dataset.pinned) pop.style.display = 'none';
}"""

# A strip tick (a beam's line, grouped by position) and the magnifier open the viewer; a caption
# or an info icon pins its popover until the next click elsewhere or Escape; the rest of a card
# is the card's own click (in the review it toggles the tilt's exclusion, elsewhere it opens the
# viewer).
GRID_CLICK_JS = (
    """(event) => {
    const tick = event.target.closest('.cb-tp-tick');
    if (tick) { emit({key: tick.dataset.key, zoom: true}); return; }
    const card = event.target.closest('.cb-tp-card');
    if (!card) return;
    if (event.target.closest('.cb-tp-zoom')) { emit({key: card.dataset.key, zoom: true}); return; }
    const anchor = event.target.closest('.cb-tp-cap, .cb-ti');
    if (!anchor) { emit({key: card.dataset.key}); return; }
    const pinning = true;"""
    + _POPOVER_AT_ANCHOR
    + """
    pop.dataset.pinned = '1';
    const unpin = () => {
        delete pop.dataset.pinned;
        pop.style.display = 'none';
        document.removeEventListener('mousedown', onDown, true);
        document.removeEventListener('keydown', onKey, true);
        window.removeEventListener('scroll', unpin, true);
        pop._unpin = null;
    };
    const onDown = (e) => { if (!pop.contains(e.target)) unpin(); };
    const onKey = (e) => { if (e.key === 'Escape') unpin(); };
    document.addEventListener('mousedown', onDown, true);
    document.addEventListener('keydown', onKey, true);
    window.addEventListener('scroll', unpin, true);
    pop._unpin = unpin;
}"""
)

STRIP_PX = 200
# Card column width per size step.
TILT_CARD_PX = {"s": 90, "m": 130, "l": 190}
_AMBER_TEXT = "#b45309"
_RED_TEXT = "#be4343"
_VIOLET_TEXT = "#6d28d9"
_INDIGO_TEXT = "#4a3aa7"
_MUTED = "#94a3b8"
_LABEL = "#64748b"

CONFIDENCE_TIP = "Confidence that this particular tilt should be excluded."

EXCLUDED_RULE = (
    "Red border: excluded from alignment, CTF and reconstruction, by a manual label or by the confidence score at "
    "the threshold; Approve labels on the tilt filter's page makes it final. On that page a click on a tilt "
    "toggles it."
)

DARK_RULE = (
    f"Dark exposure: the tilt's mdoc mean counts are under {DIM_EXPOSURE_FRACTION:.0%} of its tilt series' "
    f"median (dim); under {BLANK_EXPOSURE_FRACTION:.0%} it is blank (the beam was blocked, by a grid bar or the "
    "lamella edge). A blank exposure in a tomogram back-projects as straight streaks. A marker only: it excludes "
    "nothing; the tilt filter's review decides."
)

OUT_RULE = (
    "Not in alignment's output, and no verdict of the tilt filter excluded it: Warp's import leaves dark tilts at "
    "the end of a series out. A label does not bring it back."
)

OUTLIER_RULE = (
    f"Outlier: more than {OUTLIER_ROBUST_SDS:g} robust SDs (1.4826 × the median absolute deviation) worse than the "
    f"project's tilts in the same {OUTLIER_BAND_DEG:g}° stage-tilt band, both signs together. A band with fewer "
    f"than {OUTLIER_MIN_BAND} values, or no spread, marks none. Marked only for the metrics ticked in the metrics "
    "menu. A marker only: it excludes nothing."
)

# The gallery's legend, icons only: (mark, swatch class, explanation).
_LEGEND = (
    ("excluded", "cb-lg-x", EXCLUDED_RULE),
    ("dark", "cb-ti cb-ti-dim", DARK_RULE),
    ("out", "cb-ti cb-ti-out", OUT_RULE),
    ("outlier", "cb-ti cb-ti-outl-lg", OUTLIER_RULE),
)


# ── Metrics ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Metric:
    key: str
    label: str  # the metrics menu, the sort option, the popover's row
    tip: str  # the menu's tooltip: what the number tells
    short: str  # the caption's word before the value ("#" for the tilt number)
    fmt: Callable[[float], str]  # the value with its unit
    judged: bool = False  # band outliers apply
    two_sided: bool = False  # judged and sorted on its absolute value
    low_is_worse: bool = False  # worst first sorts ascending

    def token(self, value: float) -> str:
        """The caption's form: `#13`, `CTF 7.1 Å`."""
        return f"#{self.fmt(value)}" if self.short == "#" else f"{self.short} {self.fmt(value)}"


def _signed(value: float, digits: int) -> str:
    """A signed number with a true minus sign: +42.0, −12.0."""
    return f"{value:+.{digits}f}".replace("-", "−")


def _pct_fine(ratio: float) -> str:
    pct = ratio * 100
    if pct < 0.001:
        return "<0.001 %"
    if pct < 1:
        return f"{pct:.2g} %"
    return f"{pct:.1f} %" if pct < 10 else f"{pct:.0f} %"


_METRICS = (
    _Metric(
        "num", "Tilt number", "The tilt's place in the acquisition, i.e. the dose order.", "#", lambda v: f"{v:.0f}"
    ),
    _Metric("pbad", "Confidence score", CONFIDENCE_TIP, "score", lambda v: f"{v:.2f}"),
    _Metric(
        "exp",
        "Exposure",
        "The tilt's mdoc mean counts as a share of its tilt series' median.",
        "exposure",
        _pct_fine,
        low_is_worse=True,
    ),
    _Metric(
        "ctf",
        "CTF fit",
        "How far out Warp fit the Thon rings (CTFResolutionEstimate); worse with tilt and thickness.",
        "CTF",
        lambda v: f"{v:.1f} Å",
        judged=True,
    ),
    _Metric(
        "mot",
        "Motion",
        "Beam-induced motion during the exposure (MeanFrameMovement, Warp's units).",
        "motion",
        lambda v: f"{v:.2f}",
        judged=True,
    ),
    _Metric(
        "ddf",
        "Defocus vs series median",
        "The tilt's fitted defocus minus its tilt series' median: a collapsed or diverged CTF fit stands out.",
        "defocus",
        lambda v: f"{_signed(v, 2)} µm",
        judged=True,
        two_sided=True,
    ),
    _Metric(
        "ast",
        "Astigmatism",
        "The difference between the fitted defocus along the two axes.",
        "astigmatism",
        lambda v: f"{v:.2f} µm",
        judged=True,
    ),
    _Metric(
        "dose",
        "Dose before",
        "Electron dose accumulated before this tilt, from the scope's calibration.",
        "dose",
        lambda v: f"{v:.0f} e⁻/Å²",
    ),
    _Metric(
        "shift",
        "Alignment shift",
        "How far alignment moved the tilt: a proxy for how hard it was to align.",
        "shift",
        lambda v: f"{v:.0f} Å",
        judged=True,
    ),
)
_METRIC = {m.key: m for m in _METRICS}
METRIC_LABELS = {m.key: m.label for m in _METRICS}
JUDGED_METRICS = tuple(m.key for m in _METRICS if m.judged)
DEFAULT_METRICS = frozenset({"num", "pbad"})
# The tilt-metrics dict's name for each judged metric's number.
_METRIC_FIELD = {"ctf": "ctf_res", "mot": "motion", "ddf": "ddefocus", "ast": "astig", "shift": "shift"}


def _values(s: TiltState, m: dict) -> dict[str, float | None]:
    """Every metric's number for one tilt, by metric key."""
    return {
        "num": float(s.number),
        "pbad": s.p_bad,
        "exp": s.exposure_ratio,
        "dose": m["dose"],
        **{key: m[field] for key, field in _METRIC_FIELD.items()},
    }


def _project_bands(per_series: Sequence[tuple[Any, dict[str, dict]]]) -> dict[str, tuple[dict[str, BandStat], set]]:
    """Per judged metric, every tilt's |stage tilt| band over the whole project, and the
    outliers (band_outliers)."""
    bands = {}
    for metric in _METRICS:
        if not metric.judged:
            continue
        field = _METRIC_FIELD[metric.key]
        points = [
            (f.id, f.nominal_tilt_angle_deg, abs(v) if metric.two_sided else v)
            for ts, metrics in per_series
            for f in ts.frames
            if (v := metrics[f.id][field]) is not None
        ]
        bands[metric.key] = band_outliers(points)
    return bands


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
    the facts about the whole project its views state once.

    A group is ``{ts, label, order, tilts, n_png, summary, ticks, ctf_fit, counts_known,
    series, metrics}``: ``tilts`` are the tilts that show (see ``_tilt``); ``ticks`` is every
    tilt of the series as ``(state, title)`` for the header strip; ``summary`` the series'
    ``SeriesSummary``; ``ctf_fit`` its tsCtf fit resolution (Å) or None; ``series`` and
    ``metrics`` what ``restate_group`` re-derives from. The facts are ``{span, marks, lacking,
    n_series, present, dl, any_outlier, bands}``: the range of every stage tilt, the marks the
    legend shows (excluded, dark, out, outlier), per field family the number of series without
    it, the metrics some tilt has, whether the confidence score shows and any number is an
    outlier, and the project's outlier bands.

    A tilt shows when it has a preview PNG or a motion-corrected average, the full-size
    viewer's source. Its PNG is named after that average (the thumbnail pass converts the
    averages), else after the frame id. Tilts run by angle, most negative first. Outliers are
    judged over every tilt of the project. Disk: one listing of the PNG directory. Safe in a
    thread."""
    pngs = {p.stem: p for p in png_dir.glob("*.png")}
    series = list(tilt_series)
    per_series = [(ts, _tilt_metrics(ts, ctx)) for ts in series]
    bands = _project_bands(per_series)
    review = {k: ctx[k] for k in ("alignment_instance", "committed", "labels", "threshold", "mode")}
    groups: list[dict] = []
    angles: list[float] = []
    marks: set[str] = set()
    present: set[str] = set()
    lacking = {"counts": 0, "qc": 0, "alignment": 0}
    for ts, metrics in per_series:
        states = {s.frame_id: s for s in tilt_states(ts, **review)}
        tilts = []
        for frame in ts.frames:
            avg = next((o.averaged_mrc for o in frame.outputs.values() if o.output_type == "fs_motion_ctf"), None)
            png = (pngs.get(Path(avg).stem) if avg is not None else None) or pngs.get(frame.id)
            if avg is None and png is None:
                continue
            tilt = _tilt(frame.id, states[frame.id], metrics[frame.id], bands, ctx)
            tilt["png"] = str(png) if png is not None else None
            tilt["mrc"] = str(avg) if avg is not None else ""
            present.update(k for k, v in tilt["vals"].items() if v is not None)
            tilts.append(tilt)
        if not tilts:
            continue
        tilts.sort(key=lambda t: (t["angle"], t["number"]))
        ordered = sorted(states.values(), key=lambda s: (s.angle, s.number))
        angles.extend(s.angle for s in ordered)
        marks.update(m for s in ordered for m in _marks(s))
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
                # For re-deriving the states after a label or threshold change (restate_group).
                "series": ts,
                "metrics": metrics,
            }
        )
    groups.sort(key=lambda g: (g["order"], g["ts"]))
    any_outlier = any(t["outliers"] for g in groups for t in g["tilts"])
    if any_outlier:
        marks.add("outlier")
    facts = {
        "span": (min(angles), max(angles)) if angles else None,
        "marks": marks,
        "lacking": lacking,
        "n_series": len(groups),
        "present": present,
        "dl": ctx["mode"] in DL_MODES and "pbad" in present,
        "any_outlier": any_outlier,
        "bands": bands,
    }
    return groups, facts


def _tilt(key: str, s: TiltState, m: dict, bands: dict, ctx: dict) -> dict:
    """One tilt as the views show it: ``{key, angle, number, state, label, vals, outliers,
    disagree, tip, look, cell_look, tokens, pop, icons}``. ``label`` is a manual label;
    ``vals`` every metric's number; ``outliers`` the metrics whose number is an outlier;
    ``disagree`` whether a manual label and the model's call differ; ``tip`` the mosaic cell's
    multi-line title; ``tokens`` the card caption; ``pop`` its popover; ``icons`` the info icons
    over the card's image."""
    threshold = ctx["threshold"]
    vals = _values(s, m)
    outliers = {k for k, (_stats, out) in bands.items() if key in out}
    label = ctx["labels"].get(key)
    disagree = (
        label is not None
        and s.p_bad is not None
        and threshold is not None
        and (label == "bad") != (s.p_bad >= threshold)
    )
    return {
        "key": key,
        "angle": s.angle,
        "number": s.number,
        "state": s,
        "label": label,
        "vals": vals,
        "outliers": outliers,
        "disagree": disagree,
        "tip": tilt_tip(s, m, threshold),
        "look": _look(s),
        "cell_look": _look(s, review=False),
        "tokens": _tokens_html(s, vals, outliers),
        "pop": _popover_html(s, m, vals, outliers, bands, threshold),
        "icons": _icons_html(s, m, vals, outliers, bands),
    }


def restate_group(group: dict, ctx: dict, bands: dict) -> list[dict]:
    """Re-derive a group's tilts after the review changed (a label, the threshold, cleared
    labels): their states, looks, captions and popovers, its summary and strip ticks. The
    metrics and outliers stay as collected. Returns the tilts whose card changed."""
    review = {k: ctx[k] for k in ("alignment_instance", "committed", "labels", "threshold", "mode")}
    states = {s.frame_id: s for s in tilt_states(group["series"], **review)}
    changed = []
    for i, old in enumerate(group["tilts"]):
        new = _tilt(old["key"], states[old["key"]], group["metrics"][old["key"]], bands, ctx)
        new["png"], new["mrc"] = old["png"], old["mrc"]
        if (new["look"], new["label"], new["tokens"], new["pop"]) != (
            old["look"],
            old["label"],
            old["tokens"],
            old["pop"],
        ):
            changed.append(new)
        group["tilts"][i] = new
    ordered = sorted(states.values(), key=lambda s: (s.angle, s.number))
    group["summary"] = series_summary(ordered)
    group["ticks"] = [(s, _tick_title(s)) for s in ordered]
    return changed


def _lacks_qc(ts, fs_motion_instance: str | None) -> bool:
    """fsMotion has outputs for the series but none of them carries the per-tilt CTF fit or
    motion (an ingest older than those fields)."""
    outs = [f.outputs[fs_motion_instance] for f in ts.frames if fs_motion_instance in f.outputs]
    return bool(outs) and all(o.ctf_resolution is None and o.mean_frame_movement is None for o in outs)


def _tilt_metrics(ts, ctx: dict) -> dict[str, dict]:
    """Each frame's recorded numbers, by frame id. Δ defocus is against the series' median, and
    it and the astigmatism come from CTF after alignment for the tilts it fit, else from Motion &
    CTF; the dose before a tilt counts as unknown when the mdoc recorded none (every tilt reads
    0)."""
    fsm, ctf, aln = ctx["fs_motion_instance"], ctx["ts_ctf_instance"], ctx["alignment_instance"]
    fs_outs = {f.id: f.outputs[fsm] for f in ts.frames if fsm and fsm in f.outputs}
    fs_defocus = {fid: (o.defocus_u_angstrom + o.defocus_v_angstrom) / 2e4 for fid, o in fs_outs.items()}
    fs_astig = {fid: abs(o.defocus_u_angstrom - o.defocus_v_angstrom) / 1e4 for fid, o in fs_outs.items()}
    ctf_out = ts.outputs.get(ctf) if ctf else None
    ts_fits = ctf_out.per_frame if ctf_out is not None and ctf_out.output_type == "ts_ctf" else []
    ts_defocus = {e.frame_id: (e.defocus_u_angstrom + e.defocus_v_angstrom) / 2e4 for e in ts_fits}
    ts_astig = {e.frame_id: abs(e.defocus_u_angstrom - e.defocus_v_angstrom) / 1e4 for e in ts_fits}
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
            ddefocus, astig, source = ts_defocus[f.id] - ts_median, ts_astig[f.id], "CTF after alignment"
        elif f.id in fs_defocus:
            ddefocus, astig, source = fs_defocus[f.id] - fs_median, fs_astig[f.id], "Motion & CTF"
        else:
            ddefocus, astig, source = None, None, None
        out[f.id] = {
            "ctf_res": o.ctf_resolution if o is not None else None,
            "motion": o.mean_frame_movement if o is not None else None,
            "ddefocus": ddefocus,
            "astig": astig,
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


def _angles(states: Sequence[TiltState], limit: int = 8) -> str:
    shown = ", ".join(f"{_signed(s.angle, 0)}°" for s in sorted(states, key=lambda s: s.angle)[:limit])
    return shown + (f" and {len(states) - limit} more" if len(states) > limit else "")


def _band_label(angle: float) -> str:
    """The stage-tilt band a tilt is judged in, both signs together: `±40–50°`."""
    lo = int(abs(angle) // OUTLIER_BAND_DEG) * OUTLIER_BAND_DEG
    return f"±{lo:g}–{lo + OUTLIER_BAND_DEG:g}°"


def where_sentence(s: TiltState) -> str:
    """Whether the tilt is in the tomogram and, if not, why, as one sentence."""
    if s.drop is not None:
        why = "" if s.drop == "tilt-filter" else f" ({s.drop})"
        if s.in_tomogram:
            return f"Excluded by the approved verdict{why}, yet in alignment's output: alignment ran before it."
        if s.in_tomogram is None:
            return f"Excluded by the approved verdict{why}; alignment has not run for this tilt series."
        return f"Excluded by the approved verdict{why}: not in the tomogram."
    if s.in_tomogram is False:
        return "Not in alignment's output: Warp's import left it out (it drops dark tilts at the end of a series)."
    if s.in_tomogram:
        return "In the tomogram."
    return "Alignment has not run for this tilt series."


def _status_line(s: TiltState, threshold: float | None) -> tuple[str, str] | None:
    """What the open review does with the tilt, and the line's colour: excluded and by what, or
    kept by a manual label. None for a tilt the review leaves alone."""
    if s.review == "human_bad":
        return "Excluded: manual label", _RED_TEXT
    if s.review == "model_bad":
        cut = f", threshold {threshold:.2f}" if threshold is not None else ""
        return f"Excluded: confidence score {s.p_bad:.2f}{cut}", _RED_TEXT
    if s.review == "human_good":
        over = s.p_bad is not None and threshold is not None and s.p_bad >= threshold
        return "Included: manual label" + (f", confidence score {s.p_bad:.2f}" if over else ""), _LABEL
    return None


def _missing(s: TiltState, m: dict) -> list[str]:
    """The numbers a tilt has none of, by name. Never the shift: a tilt not aligned has none."""
    missing = []
    if s.exposure_ratio is None:
        missing.append("mdoc counts" if m["counts"] is None else "an exposure ratio (the series' median counts are 0)")
    for value, name in (
        (m["ctf_res"], "CTF fit"),
        (m["motion"], "motion"),
        (m["ddefocus"], "defocus"),
        (m["dose"], "dose"),
    ):
        if value is None:
            missing.append(name)
    return missing


def _exposure_ratio_words(s: TiltState, m: dict) -> str:
    return (
        f"{_pct_fine(s.exposure_ratio)} of its tilt series' median mdoc counts "
        f"({m['counts']:.3g} against {m['median_counts']:.3g})"
    )


def tilt_tip(s: TiltState, m: dict, threshold: float | None) -> str:
    """A tilt's native tooltip (a mosaic cell's), one fact per line: what it is, what the review
    does with it, its exposure, then its recorded numbers and what is not recorded."""
    lines = [f"{_signed(s.angle, 1)}° · tilt {s.number} · {s.frame_id}", where_sentence(s)]
    status = _status_line(s, threshold)
    if status is not None:
        lines.append(status[0])
    if s.exposure_ratio is not None:
        if s.is_dark:
            lines.append(f"Dark exposure ({s.exposure}): {_exposure_ratio_words(s, m)}. A marker only.")
        else:
            lines.append(f"Exposure: {_exposure_ratio_words(s, m)}.")
    vals = _values(s, m)
    numbers = []
    for metric in _METRICS:
        value = vals[metric.key]
        if metric.key in ("num", "exp") or value is None:
            continue
        source = f" ({m['defocus_source']})" if metric.key == "ddf" and m["defocus_source"] else ""
        numbers.append(f"{metric.label} {metric.fmt(value)}{source}")
    if numbers:
        lines.append(" · ".join(numbers))
    missing = _missing(s, m)
    if missing:
        lines.append("Not recorded: " + ", ".join(missing) + ".")
    return "\n".join(lines)


def _tick_title(s: TiltState) -> str:
    if s.excluded:
        what = "excluded"
    elif s.in_tomogram is False:
        what = "not in alignment's output (Warp's import)"
    elif s.is_dark:
        what = f"dark exposure, {_pct_fine(s.exposure_ratio)} of its tilt series' median counts"
    elif s.in_tomogram:
        what = "in the tomogram"
    else:
        what = "not aligned yet"
    return f"{_signed(s.angle, 1)}° · tilt {s.number}: {what}"


def _typical(metric: _Metric, value: float) -> str:
    """A band's median or cut as the popovers state it; a two-sided metric's is of the absolute value."""
    return "±" + metric.fmt(value).lstrip("+") if metric.two_sided else metric.fmt(value)


_TH = f"padding:0 0 2px 10px;font-weight:400;color:{_MUTED};text-align:right;white-space:nowrap;"


def _popover_html(s: TiltState, m: dict, vals: dict, outliers: set, bands: dict, threshold: float | None) -> str:
    """Everything recorded about a tilt, for its caption's popover, in one type scale: the tilt,
    what the review does with it and where it is, then a table of each metric's value against
    the typical value at its stage tilt (violet where it is an outlier in a ticked metric), then
    what is not recorded. The rules live in the icons' and the legend's hovers."""
    esc = html.escape
    rows = [
        f'<div style="font-weight:600;color:#0f172a;">{_signed(s.angle, 1)}° · tilt {s.number} '
        f'<span style="{MONO} font-weight:400;color:{_MUTED};">{esc(s.frame_id)}</span></div>'
    ]
    status = _status_line(s, threshold)
    if status is not None:
        rows.append(f'<div style="color:{status[1]};">{esc(status[0])}</div>')
    rows.append(f'<div style="color:{_LABEL};">{esc(where_sentence(s))}</div>')
    body = []
    for metric in _METRICS:
        value = vals[metric.key]
        if metric.key == "num" or value is None:
            continue
        stat = bands[metric.key][0].get(s.frame_id) if metric.judged else None
        typical = _typical(metric, stat.median) if stat is not None else ""
        value_cls = f' class="cb-to-v-{metric.key}"' if metric.key in outliers else ""
        dark = f"color:{_AMBER_TEXT};font-weight:600;" if metric.key == "exp" and s.is_dark else ""
        body.append(
            f'<tr><td style="padding:1px 0;color:{_LABEL};white-space:nowrap;">{esc(metric.label)}</td>'
            f'<td{value_cls} style="padding:1px 0 1px 10px;{MONO} text-align:right;white-space:nowrap;{dark}">'
            f"{esc(metric.fmt(value))}</td>"
            f'<td style="padding:1px 0 1px 10px;{MONO} color:{_MUTED};text-align:right;white-space:nowrap;">'
            f"{esc(typical)}</td></tr>"
        )
    if body:
        head = f'<tr><th></th><th style="{_TH}">value</th><th style="{_TH}">typical at {_band_label(s.angle)}</th></tr>'
        rows.append(f'<table style="border-collapse:collapse;margin-top:4px;">{head}{"".join(body)}</table>')
    missing = _missing(s, m)
    if missing:
        rows.append(f'<div style="color:{_MUTED};margin-top:3px;">Not recorded: {esc(", ".join(missing))}.</div>')
    return "".join(rows)


def _icon(cls: str, pop: str) -> str:
    return f'<span class="cb-ti {cls}"><span class="cb-tp-popsrc" style="display:none;">{pop}</span></span>'


def _icons_html(s: TiltState, m: dict, vals: dict, outliers: set, bands: dict) -> str:
    """The info icons over a card's image, bottom left: a dark exposure, a tilt alignment's
    output lacks with no verdict against it, an outlier (drawn only while one of its metrics is
    ticked). Each carries its popover: what it means, the value against the cut, the source."""
    esc = html.escape
    muted = f'<div style="color:{_MUTED};margin-top:3px;">'
    icons = []
    if s.is_dark:
        icons.append(
            _icon(
                "cb-ti-blank" if s.exposure == "blank" else "cb-ti-dim",
                f'<div style="font-weight:600;color:{_AMBER_TEXT};">Dark exposure ({s.exposure})</div>'
                f"<div>{esc(_exposure_ratio_words(s, m))}, from the mdoc's MinMaxMean.</div>"
                f"{muted}{esc(DARK_RULE)}</div>",
            )
        )
    if s.in_tomogram is False and s.drop is None:
        icons.append(
            _icon(
                "cb-ti-out",
                f'<div style="font-weight:600;color:{_INDIGO_TEXT};">Not in alignment\'s output</div>'
                f"<div>{esc(OUT_RULE)}</div>",
            )
        )
    if outliers:
        keys = [k for k in _METRIC if k in outliers]
        rows = [f'<div style="font-weight:600;color:{_VIOLET_TEXT};">Outlier</div>']
        for key in keys:
            metric, stat = _METRIC[key], bands[key][0][s.frame_id]
            rows.append(
                f'<div class="cb-to-row cb-to-{key}">{esc(metric.label)} {esc(metric.fmt(vals[key]))}: typical at '
                f"{_band_label(s.angle)} {esc(_typical(metric, stat.median))}, cut {esc(_typical(metric, stat.cut))}."
                "</div>"
            )
        rows.append(f"{muted}{esc(OUTLIER_RULE)}</div>")
        icons.append(_icon("cb-ti-outl " + " ".join(f"cb-to-{k}" for k in keys), "".join(rows)))
    return f'<span class="cb-ti-row">{"".join(icons)}</span>' if icons else ""


# ── HTML ──────────────────────────────────────────────────────────────────────


def _marks(s: TiltState) -> list[str]:
    """The legend's marks a tilt carries; the outlier mark comes from the metrics."""
    marks = []
    if s.excluded:
        marks.append("excluded")
    if s.is_dark:
        marks.append("dark")
    if s.in_tomogram is False and s.drop is None:
        marks.append("out")
    return marks


def _look(s: TiltState, *, review: bool = True) -> str:
    """The exclusion's class on a card (``review``: the open review's call, else the committed
    verdict) or a mosaic cell (the committed verdict alone: what went into the tomogram)."""
    return "cb-ts-x" if s.drop is not None or (review and s.flagged) else ""


def _tick_class(s: TiltState) -> str:
    """A strip tick's colour: red excluded, indigo not in alignment's output, amber dark and kept."""
    if s.excluded:
        return "cb-tk-x"
    if s.in_tomogram is False:
        return "cb-tk-out"
    if s.is_dark:
        return "cb-tk-dark"
    return ""


def _tokens_html(s: TiltState, vals: dict, outliers: set) -> str:
    """A card's caption: the angle, then every metric's token, which the gallery's metrics menu
    shows or hides by a class on its root; violet where the number is an outlier. The row clips
    from the right, so the angle survives the smallest card."""
    parts = [f"<b>{_signed(s.angle, 1)}°</b>"]
    for metric in _METRICS:
        value = vals[metric.key]
        if value is None:
            continue
        cls = f"cb-tm cb-tm-{metric.key}" + (" cb-tm-out" if metric.key in outliers else "")
        parts.append(f'<span class="{cls}">{html.escape(metric.token(value))}</span>')
    return "".join(parts)


def _image(tilt: dict) -> str:
    """The tilt's PNG as a square filling its box's width, or a dark square where it has none."""
    if tilt["png"]:
        url = f"/api/tilt-thumb?path={urllib.parse.quote(tilt['png'], safe='')}"
        return (
            f'<img src="{url}" loading="lazy" alt="" style="width:100%;aspect-ratio:1;object-fit:cover;display:block;">'
        )
    return '<div style="width:100%;aspect-ratio:1;background:#1e293b;"></div>'


# The magnifier over a card's image, drawn on hover: the full-size viewer.
_ZOOM = '<span class="cb-tp-zoom" title="Open full size"></span>'


def _caption_html(t: dict) -> str:
    """A card caption's inside: its tokens, and the hidden block its popover copies."""
    return f'{t["tokens"]}<div class="cb-tp-popsrc" style="display:none;">{t["pop"]}</div>'


def card_html(t: dict) -> str:
    """One tilt as a card: the preview with its info icons and the magnifier over it, the
    exclusion as its border; under it the caption, carrying the hidden block its popover
    copies."""
    return (
        f'<div class="cb-tp-card {t["look"]}" data-key="{html.escape(t["key"])}">'
        f'<div class="cb-tp-img">{_image(t)}{t["icons"]}{_ZOOM}</div>'
        f'<div class="cb-tp-cap">{_caption_html(t)}</div>'
        "</div>"
    )


def grid_html(tilts: Sequence[dict]) -> str:
    """A series' cards. The column width is the gallery root's `--cb-tp-card`, so a size switch
    changes one variable and re-renders nothing."""
    cards = "".join(card_html(t) for t in tilts)
    return (
        '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(var(--cb-tp-card,130px),1fr));'
        f'gap:4px;">{cards}</div>'
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


def _cell_dots(s: TiltState) -> str:
    """A mosaic cell's information, as 4 px dots at its bottom left: amber for a dark exposure,
    indigo for a tilt alignment's output lacks with no verdict against it."""
    dots = []
    if s.is_dark:
        dots.append('<span class="cb-tc-dot cb-tc-dark"></span>')
    if s.in_tomogram is False and s.drop is None:
        dots.append('<span class="cb-tc-dot cb-tc-out"></span>')
    return f'<span class="cb-tc-dots">{"".join(dots)}</span>' if dots else ""


def mosaic_html(tilts: list[dict], aspect: float | None) -> tuple[str, str | None]:
    """A series' tilts as square cells fitted into a tile's frame of aspect W/H, centred both
    ways, in angle order from the top left; a cell shows what the committed verdict excluded (a
    red outline) and, as dots, a dark exposure or a tilt Warp's import left out. Returns the HTML
    and, for a frame with no known extent (``aspect`` None), the CSS aspect-ratio the frame takes
    so the grid fills it.

    The grid sits in a box placed absolutely over the frame, so its size is a percentage of
    the frame and follows the tile size with no re-render. Each cell's 1 px padding is the
    gutter; a CSS gap would add to the percentages and overflow the frame."""
    cols, rows = mosaic_layout(len(tilts), aspect or 1.0)
    frame_aspect = aspect or cols / rows
    width_pct = cols * min(100 / cols, 100 / (frame_aspect * rows))
    cells = "".join(
        f'<div class="cb-tp-cell {t["cell_look"]}" data-key="{html.escape(t["key"])}" '
        f'title="{html.escape(t["tip"])}" style="padding:1px;box-sizing:border-box;min-width:0;">'
        f'<div class="cb-tp-img">{_image(t)}{_cell_dots(t["state"])}</div></div>'
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
    mark. A tick takes its tilt's colour (_tick_class); its title names the tilt."""
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
        f'<span class="cb-tp-tick {_tick_class(s)}" data-key="{html.escape(s.frame_id)}" '
        f'title="{html.escape(title)}" style="left:{x(s.angle)};"></span>'
        for s, title in ticks
    )
    legend = (
        f"Every tilt at its stage tilt, {_signed(lo, 0)}° to {_signed(hi, 0)}° across the project. Red: excluded. "
        "Indigo: not in alignment's output (Warp's import). Amber: dark exposure (a marker only). Click a tick to view "
        "the tilt."
    )
    return (
        f'<div class="cb-tp-strip" title="{html.escape(legend)}" '
        f'style="position:relative;width:{STRIP_PX}px;height:12px;flex:0 0 auto;">{zero}{marks}</div>'
    )


def _words_html(words: Sequence[tuple[str, str, str]]) -> str:
    """(word, colour, title) parts, dot-separated."""
    return ' <span style="color:#cbd5e1;">·</span> '.join(
        f'<span style="color:{color};" title="{html.escape(title)}">{html.escape(word)}</span>'
        for word, color, title in words
    )


def _excluded_title(states: Sequence[TiltState]) -> str:
    n_model = sum(1 for t in states if t.review == "model_bad")
    n_manual = sum(1 for t in states if t.review == "human_bad")
    if n_model or n_manual:
        text = (
            f"{len(states)} excluded, not approved yet: {n_model} by the confidence score at the threshold, "
            f"{n_manual} by a manual label. Approve labels makes it final."
        )
    else:
        text = f"{len(states)} excluded by the approved verdict."
    return f"{text} {_angles(states)}."


def group_header_html(group: dict, span: tuple[float, float] | None) -> str:
    """What a group's tilts are, after its tilt count: how many are in the tomogram, how many
    the filter excludes, how many are dark with nothing excluding them; then the strip.
    Collapsed, the headers read as the project's tilt scheme: where each series loses tilts,
    and its missing wedge."""
    s: SeriesSummary = group["summary"]
    words: list[tuple[str, str, str]] = []
    if s.used:
        words.append((f"{s.used} in the tomogram", _LABEL, _used_title(s)))
    if s.excluded:
        words.append((f"{len(s.excluded)} excluded", _RED_TEXT, _excluded_title(s.excluded)))
    if s.dark_kept:
        words.append(
            (
                f"{len(s.dark_kept)} dark",
                _AMBER_TEXT,
                f"{len(s.dark_kept)} dark exposures nothing excludes: {_dark_list(s.dark_kept)}.\n{DARK_RULE}",
            )
        )
    strip = _strip_html(group["ticks"], span) if span else ""
    return (
        '<div style="display:flex;align-items:center;gap:10px;font-family:ui-monospace,monospace;font-size:9px;'
        f'white-space:nowrap;"><span>{_words_html(words)}</span>{strip}</div>'
    )


def _position_key(group: dict) -> str:
    """The position a group belongs to: its stage number, or the series itself when its name
    carries none."""
    stage = group["order"][0]
    return f"pos:{stage}" if stage else f"ts:{group['ts']}"


def positions(groups: Sequence[dict]) -> list[dict]:
    """The groups by stage position (Pos 13 holds 13, 13_2, 13_3, …), in their order:
    ``{key, label, groups}``."""
    out: list[dict] = []
    by_key: dict[str, dict] = {}
    for g in groups:
        key = _position_key(g)
        pos = by_key.get(key)
        if pos is None:
            stage = g["order"][0]
            pos = by_key[key] = {"key": key, "label": f"Pos {stage}" if stage else g["label"], "groups": []}
            out.append(pos)
        pos["groups"].append(g)
    return out


def position_header_html(pos: dict) -> str:
    """A position box's header words: its beams, tilts, the tilts excluded and the dark ones kept."""
    summaries = [g["summary"] for g in pos["groups"]]
    n = len(summaries)
    n_excluded = sum(len(s.excluded) for s in summaries)
    n_dark = sum(len(s.dark_kept) for s in summaries)
    words = [
        (f"{n} beam" if n == 1 else f"{n} beams", _LABEL, "Tilt series acquired at this stage position"),
        (f"{sum(s.total for s in summaries)} tilts", _LABEL, "Every tilt of these tilt series"),
    ]
    if n_excluded:
        words.append((f"{n_excluded} excluded", _RED_TEXT, EXCLUDED_RULE))
    if n_dark:
        words.append((f"{n_dark} dark", _AMBER_TEXT, f"{n_dark} dark exposures nothing excludes.\n{DARK_RULE}"))
    return (
        f'<div style="font-family:ui-monospace,monospace;font-size:9px;white-space:nowrap;">{_words_html(words)}</div>'
    )


def beam_line_html(group: dict, span: tuple[float, float] | None) -> str:
    """A beam's slim line inside its position's box: the beam, the series id, its tilt count,
    then its header words and strip."""
    stage, beam = group["order"]
    name = f"Beam {beam}" if stage else group["label"]
    return (
        f'<span class="cb-tp-name">{html.escape(name)}</span>'
        f'<span class="cb-tp-ts">{html.escape(group["ts"])}</span>'
        f'<span class="cb-tp-n" title="Tilts with a preview or an average">{len(group["tilts"])}</span>'
        f"{group_header_html(group, span)}"
    )


def _dark_list(states: Sequence[TiltState]) -> str:
    return ", ".join(f"{_signed(t.angle, 0)}° ({_pct_fine(t.exposure_ratio)})" for t in states)


def _used_title(s: SeriesSummary) -> str:
    lines = [f"{s.used} of {s.total} tilts are in the tomogram (alignment's output)."]
    approved = [t for t in s.excluded if t.drop is not None]
    if approved:
        lines.append(f"{len(approved)} excluded by the approved verdict: {_angles(approved)}.")
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


# ── The gallery ───────────────────────────────────────────────────────────────

_SHOW = (
    ("all", "all"),
    ("excluded", "excluded"),
    ("out", "not in the tomogram"),
    ("dark", "dark"),
    ("outliers", "outliers"),
)
_GROUPINGS = (("series", "series"), ("position", "position"))
_POPOVER_STYLE = (
    "position: fixed; z-index: 6000; display: none; max-width: 400px; background: #ffffff; "
    "border: 1px solid #e2e8f0; border-radius: 5px; box-shadow: 0 6px 18px rgba(15,23,42,0.16); "
    "padding: 6px 8px; font-family: 'IBM Plex Sans', sans-serif; font-size: 10px; line-height: 1.45; "
    "color: #334155; white-space: normal;"
)


class TiltGallery:
    """Every tilt of the project as a card, grouped by tilt series or by stage position: the
    Tilts tab's gallery and the tilt-filter job page's, one component.

    The host collects (``collect_tilt_groups``), hands the result over with ``set_data`` and
    places the two parts: ``render_controls`` (S/M/L when the host has none, sort, show, group,
    the metrics menu, expand and collapse, the legend) and ``render`` (notes, the groups). Groups
    start collapsed and a group's cards are built on its first expand, one HTML string with one
    delegated click (per series, or per position when grouped by position). Sort and show
    re-render the open grids only; the size and the metrics menu set a CSS variable and classes
    on the gallery's root, so neither re-renders.

    Review mode (the job page): a click on a card calls ``on_toggle(key)`` and the magnifier
    opens the full-size viewer; elsewhere a card click opens the viewer too. Hovering a caption or
    an info icon shows its popover, and a click pins it. ``on_metrics`` hears the metrics menu."""

    def __init__(
        self,
        project_path: Path,
        *,
        review: bool = False,
        on_toggle: Callable[[str], Any] | None = None,
        on_metrics: Callable[[], Any] | None = None,
        size: str = "m",
        size_control: bool = False,
    ) -> None:
        self.project_path = Path(project_path)
        self.review = review
        self.on_toggle = on_toggle
        self.on_metrics = on_metrics
        self.size = size
        self.size_control = size_control
        self.groups: list[dict] = []
        self.facts: dict = {}
        # Kept on the gallery, so a re-render, a Refresh or a host's poll keeps them.
        self.grouping = "series"
        self.expanded: set[str] = set()  # series open, grouped by series
        self.expanded_pos: set[str] = set()  # positions open, grouped by position
        self.sort = "angle"
        self.show = "all"
        self.metrics_on: set[str] = set(DEFAULT_METRICS)
        self.root: Any = None
        self._list: Any = None
        self._groups_ui: dict[str, dict] = {}
        self._pos_ui: dict[str, dict] = {}
        self._tilt_by_key: dict[str, dict] = {}
        self._frame_keys: set[str] = set()
        self._sort_select: Any = None
        self._show_seg: Any = None
        self._size_seg: Any = None
        self._group_seg: Any = None

    # ── Data ──

    def set_data(self, groups: list[dict], facts: dict) -> None:
        self.groups, self.facts = groups, facts
        self._tilt_by_key = {t["key"]: t for g in groups for t in g["tilts"]}
        self._frame_keys = {s.frame_id for g in groups for s, _title in g["ticks"]}
        if self.sort not in self._sort_options():
            self.sort = "angle"
        if self.show not in dict(self._show_options()):
            self.show = "all"

    def _metric_keys(self) -> list[str]:
        """The metrics the menu offers: those some tilt has, the confidence score only where it shows."""
        present = self.facts.get("present") or set()
        return [
            m.key
            for m in _METRICS
            if m.key == "num" or (m.key in present and (m.key != "pbad" or self.facts.get("dl")))
        ]

    def _sort_options(self) -> dict[str, str]:
        opts = {"angle": "stage tilt", "order": "acquisition order"}
        offered = self._metric_keys()
        if "pbad" in offered:
            opts["pbad"] = "confidence score, worst first"
        for key in offered:
            if key not in ("num", "pbad") and key in self.metrics_on:
                opts[key] = f"{_METRIC[key].label}, worst first"
        return opts

    def _show_options(self) -> list[tuple[str, str]]:
        return [*_SHOW, ("disagree", "disagree")] if self.facts.get("dl") else list(_SHOW)

    # ── Controls ──

    def render_controls(self) -> None:
        """S/M/L (when the host has none), sort, show, group, the metrics menu, Expand all and
        Collapse all, then the legend, in the current slot."""
        with ui.element("div").style("display: flex; align-items: center; gap: 8px; flex-wrap: wrap; min-width: 0;"):
            if self.size_control:
                ui.label("size").classes("cb-gal-toolbar-label")
                self._size_seg = render_segmented([("s", "S"), ("m", "M"), ("l", "L")], self.size, self._select_size)
            ui.label("sort").classes("cb-gal-toolbar-label")
            self._sort_select = (
                ui.select(self._sort_options(), value=self.sort, on_change=self._on_sort)
                .props('dense popup-content-class="cb-select-popup"')
                .classes("cb-field w-36")
            )
            ui.label("show").classes("cb-gal-toolbar-label")
            self._show_seg = render_segmented(self._show_options(), self.show, self._select_show)
            ui.label("group").classes("cb-gal-toolbar-label")
            self._group_seg = render_segmented(list(_GROUPINGS), self.grouping, self._select_grouping)
            offered = self._metric_keys()
            if offered:
                self._render_metrics_menu(offered)
            house_button("Expand all", lambda: self.expand_all(True))
            house_button("Collapse all", lambda: self.expand_all(False))
            self._render_legend()

    def _render_metrics_menu(self, offered: list[str]) -> None:
        """The metrics every caption shows, and the only ones whose outliers are marked: a menu of
        checkboxes."""
        tip = "The numbers every caption shows; outliers are marked only in a ticked metric."
        with house_button("metrics ▾", tooltip=tip):
            with ui.menu().classes("cb-tg-menu"):
                for key in offered:
                    metric = _METRIC[key]
                    ui.checkbox(
                        metric.label,
                        value=key in self.metrics_on,
                        on_change=lambda e, k=key: self._set_metric(k, bool(e.value)),
                    ).props("dense size=xs").tooltip(metric.tip)

    def _render_legend(self) -> None:
        """The legend, icons only, each explained on hover: the marks the project's tilts carry,
        and in the review always the red border, which a click sets."""
        marks = set(self.facts.get("marks") or ())
        if self.review:
            marks.add("excluded")
        items = [(cls, tip) for mark, cls, tip in _LEGEND if mark in marks]
        if not items:
            return
        ui.label("legend").classes("cb-gal-toolbar-label")
        for cls, tip in items:
            ui.html(
                f'<span class="{cls}" style="display:inline-block;vertical-align:middle;"></span>', sanitize=False
            ).tooltip(tip)

    def _select_size(self, key: str) -> None:
        if self._size_seg is not None:
            self._size_seg.set_active(key)
        self.set_size(key)

    def set_size(self, key: str) -> None:
        self.size = key
        if self.root is not None and not self.root.is_deleted:
            self.root.style(f"--cb-tp-card: {TILT_CARD_PX[key]}px;")

    def _on_sort(self, e) -> None:
        if e.value and e.value != self.sort:
            self.sort = e.value
            self._rerender_open()

    def _select_show(self, key: str) -> None:
        self.show = key
        if self._show_seg is not None:
            self._show_seg.set_active(key)
        self._rerender_open()

    def _select_grouping(self, key: str) -> None:
        self.grouping = key
        if self._group_seg is not None:
            self._group_seg.set_active(key)
        if self._list is not None and not self._list.is_deleted:
            self._list.clear()
            with self._list:
                self._render_list()

    def _set_metric(self, key: str, on: bool) -> None:
        if on == (key in self.metrics_on):
            return
        if on:
            self.metrics_on.add(key)
        else:
            self.metrics_on.discard(key)
        if self.root is not None and not self.root.is_deleted:
            if on:
                self.root.classes(add=f"cb-tg-m-{key}")
            else:
                self.root.classes(remove=f"cb-tg-m-{key}")
        opts = self._sort_options()
        resort = self.sort not in opts
        if resort:
            self.sort = "angle"
        if self._sort_select is not None and not self._sort_select.is_deleted:
            self._sort_select.set_options(opts, value=self.sort)
        # show · outliers follows the ticked metrics.
        if resort or self.show == "outliers":
            self._rerender_open()
        if self.on_metrics is not None:
            self.on_metrics()

    # ── Body ──

    def render(self, notes: Sequence[tuple[str, str, bool]] = ()) -> None:
        """The gallery in the current slot: `notes` (text, colour, spinner) from the host, the
        project's own notes and the groups."""
        classes = ["cb-tg-root", *(f"cb-tg-m-{k}" for k in sorted(self.metrics_on))]
        if self.review:
            classes.append("cb-tg-review")
        self.root = (
            ui.element("div")
            .classes(" ".join(classes))
            .style(f"--cb-tp-card: {TILT_CARD_PX[self.size]}px; min-width: 0;")
        )
        self._groups_ui = {}
        self._pos_ui = {}
        self._list = None
        with self.root:
            ui.element("div").classes("cb-tp-popover").style(_POPOVER_STYLE)
            for text, color, spinner in notes:
                self._note(text, color=color, spinner=spinner)
            if not self.groups:
                if not notes:
                    with ui.element("div").classes("cb-empty"):
                        ui.icon("collections", size="40px").classes("text-gray-400")
                        ui.label("No tilts yet.").classes("text-sm text-gray-500")
                        ui.label("A tilt appears here once motion correction (fsMotion) has made its average.").classes(
                            "text-[11px] italic text-gray-400 text-center"
                        ).style("max-width: 460px;")
                return
            for text in self._lacking_notes():
                self._note(text)
            self._list = ui.element("div").style("display: flex; flex-direction: column; gap: 6px; padding: 10px;")
            with self._list:
                self._render_list()

    def _render_list(self) -> None:
        self._groups_ui = {}
        self._pos_ui = {}
        if self.grouping == "position":
            for pos in positions(self.groups):
                self._render_position(pos)
        else:
            for group in self.groups:
                self._render_group(group)

    def _note(self, text: str, *, color: str = "#94a3b8", spinner: bool = False) -> None:
        with ui.element("div").style("display: flex; align-items: center; gap: 6px; padding: 8px 10px 0;"):
            if spinner:
                ui.spinner(size="14px", color="indigo-400")
            ui.label(text).style(f"font-size: 10px; color: {color};")

    def _lacking_notes(self) -> list[str]:
        """One line per field family some tilt series lack."""
        lacking = self.facts.get("lacking") or {}
        n = self.facts.get("n_series", 0)
        notes = []
        if lacking.get("counts"):
            notes.append(
                f"{lacking['counts']} of {n} tilt series have no mdoc exposure counts recorded, so their dark "
                "exposures are not marked."
            )
        if lacking.get("alignment"):
            notes.append(
                f"{lacking['alignment']} of {n} tilt series have an alignment run without a per-tilt list recorded, "
                "so which of their tilts are in the tomogram is unknown."
            )
        if lacking.get("qc"):
            notes.append(f"{lacking['qc']} of {n} tilt series have no per-tilt CTF fit or motion recorded.")
        return notes

    def _render_group(self, group: dict) -> None:
        ts = group["ts"]
        box = ui.element("div").classes("cb-tp-group")
        with box:
            head = ui.element("div").classes("cb-tp-head")
            with head:
                chevron = ui.label("▸").classes("cb-tp-chev")
                ui.label(group["label"]).classes("cb-tp-name")
                ui.label(ts).classes("cb-tp-ts")
                ui.label(str(len(group["tilts"]))).classes("cb-tp-n").tooltip("Tilts with a preview or an average")
                # What the tilts are, and the strip: a tick opens its tilt, the rest of the header toggles.
                summary = ui.html(group_header_html(group, self.facts.get("span")), sanitize=False)
                summary.style("flex: 0 0 auto;").on("click", handler=self._on_click, js_handler=STRIP_CLICK_JS)
            body = ui.element("div").style("padding: 4px; display: none;")
        ref = {"box": box, "body": body, "chevron": chevron, "summary": summary, "group": group, "grid": None}
        self._groups_ui[ts] = ref
        head.on("click", lambda _e, t=ts: self._set_group_open(t, t not in self.expanded))
        if not self._shown(group):
            box.set_visibility(False)
        if ts in self.expanded:
            self._show_body(ref, True, lambda: self._grid(group))

    def _render_position(self, pos: dict) -> None:
        """One box per stage position: the header, and when open each beam's slim line followed
        by its cards, flush with the box's left edge."""
        key = pos["key"]
        box = ui.element("div").classes("cb-tp-group")
        with box:
            head = ui.element("div").classes("cb-tp-head")
            with head:
                chevron = ui.label("▸").classes("cb-tp-chev")
                ui.label(pos["label"]).classes("cb-tp-name")
                summary = ui.html(position_header_html(pos), sanitize=False).style("flex: 0 0 auto;")
            body = ui.element("div").style("padding: 4px; display: none;")
        ref = {"box": box, "body": body, "chevron": chevron, "summary": summary, "pos": pos, "grid": None}
        self._pos_ui[key] = ref
        head.on("click", lambda _e, k=key: self._set_pos_open(k, k not in self.expanded_pos))
        if not self._pos_shown(pos):
            box.set_visibility(False)
        if key in self.expanded_pos:
            self._show_body(ref, True, lambda: self._pos_html(pos))

    def _set_group_open(self, ts: str, on: bool) -> None:
        if on:
            self.expanded.add(ts)
        else:
            self.expanded.discard(ts)
        ref = self._groups_ui.get(ts)
        if ref is not None:
            self._show_body(ref, on, lambda: self._grid(ref["group"]))

    def _set_pos_open(self, key: str, on: bool) -> None:
        if on:
            self.expanded_pos.add(key)
        else:
            self.expanded_pos.discard(key)
        ref = self._pos_ui.get(key)
        if ref is not None:
            self._show_body(ref, on, lambda: self._pos_html(ref["pos"]))

    def _show_body(self, ref: dict, on: bool, content: Callable[[], str]) -> None:
        # Display written in full each time: .style() merges, and a left-out display would
        # keep the earlier `none`.
        ref["body"].style(f"padding: 4px; display: {'block' if on else 'none'};")
        if on:
            ref["chevron"].classes(add="open")
        else:
            ref["chevron"].classes(remove="open")
        if on and ref["grid"] is None:
            with ref["body"]:
                grid = ui.html(content(), sanitize=False)
            grid.on("click", handler=self._on_click, js_handler=GRID_CLICK_JS)
            grid.on("mouseover", js_handler=GRID_HOVER_JS)
            grid.on("mouseout", js_handler=GRID_OUT_JS)
            ref["grid"] = grid

    def expand_all(self, on: bool) -> None:
        if self.grouping == "position":
            for key, ref in self._pos_ui.items():
                if not on or self._pos_shown(ref["pos"]):
                    self._set_pos_open(key, on)
            return
        for ts, ref in self._groups_ui.items():
            if not on or self._shown(ref["group"]):
                self._set_group_open(ts, on)

    def _grid(self, group: dict) -> str:
        return grid_html(self._ordered([t for t in group["tilts"] if self._passes(t)]))

    def _pos_html(self, pos: dict) -> str:
        """A position's body: per beam with something to show, its slim line, then its cards."""
        span = self.facts.get("span")
        parts = []
        for g in pos["groups"]:
            if not self._shown(g):
                continue
            parts.append(f'<div class="cb-tp-beam" data-ts="{html.escape(g["ts"])}">{beam_line_html(g, span)}</div>')
            parts.append(self._grid(g))
        return "".join(parts)

    def _rerender_open(self) -> None:
        """After a sort or show switch: the open grids re-render, and a group (a position) with
        nothing to show hides. Header counts stay whole-series."""
        if self.grouping == "position":
            for ref in self._pos_ui.values():
                ref["box"].set_visibility(self._pos_shown(ref["pos"]))
                if ref["grid"] is not None:
                    ref["grid"].set_content(self._pos_html(ref["pos"]))
            return
        for ref in self._groups_ui.values():
            ref["box"].set_visibility(self._shown(ref["group"]))
            if ref["grid"] is not None:
                ref["grid"].set_content(self._grid(ref["group"]))

    def _shown(self, group: dict) -> bool:
        return self.show == "all" or any(self._passes(t) for t in group["tilts"])

    def _pos_shown(self, pos: dict) -> bool:
        return any(self._shown(g) for g in pos["groups"])

    def _passes(self, t: dict) -> bool:
        s: TiltState = t["state"]
        if self.show == "excluded":
            return s.excluded
        if self.show == "out":
            return s.drop is not None or s.in_tomogram is False
        if self.show == "dark":
            return s.is_dark
        if self.show == "outliers":
            return bool(t["outliers"] & self.metrics_on)
        if self.show == "disagree":
            return t["disagree"]
        return True

    def _ordered(self, tilts: list[dict]) -> list[dict]:
        if self.sort == "angle":
            return sorted(tilts, key=lambda t: (t["angle"], t["number"]))
        if self.sort == "order":
            return sorted(tilts, key=lambda t: t["number"])
        metric = _METRIC[self.sort]

        def worst_first(t: dict) -> tuple:
            value = t["vals"].get(metric.key)
            if value is None:
                return (1, 0.0, t["angle"])
            value = abs(value) if metric.two_sided else value
            return (0, value if metric.low_is_worse else -value, t["angle"])

        return sorted(tilts, key=worst_first)

    # ── Live updates (review mode) ──

    def tilt(self, key: str) -> dict | None:
        return self._tilt_by_key.get(key)

    def restate(self, ctx: dict, ts_ids: Iterable[str] | None = None) -> None:
        """After a label or threshold change: re-derive the tilts of `ts_ids` (every series when
        None, restate_group), replace their headers (a beam's line and its position's header,
        grouped by position), and patch the changed cards in place with one JavaScript call scoped
        to the gallery's root: the look and the caption. A grid whose membership depends on the
        review (show · excluded or disagree) re-renders instead."""
        bands = self.facts.get("bands") or {}
        span = self.facts.get("span")
        wanted = None if ts_ids is None else set(ts_ids)
        rebuild = self.show in ("excluded", "disagree")
        patches: list[dict] = []
        beams: list[dict] = []
        touched: dict[str, dict] = {}
        for group in self.groups:
            if wanted is not None and group["ts"] not in wanted:
                continue
            changed = restate_group(group, ctx, bands)
            for t in group["tilts"]:
                self._tilt_by_key[t["key"]] = t
            if self.grouping == "position":
                ref = self._pos_ui.get(_position_key(group))
                if ref is None:
                    continue
                touched[ref["pos"]["key"]] = ref
                if ref["grid"] is not None and not rebuild and self._shown(group):
                    beams.append({"ts": group["ts"], "html": beam_line_html(group, span)})
                    patches.extend(_card_patch(t) for t in changed)
                continue
            ref = self._groups_ui.get(group["ts"])
            if ref is None:
                continue
            ref["summary"].set_content(group_header_html(group, span))
            ref["box"].set_visibility(self._shown(group))
            grid = ref["grid"]
            if grid is None or not changed:
                continue
            if rebuild:
                grid.set_content(self._grid(group))
                continue
            # The cards are patched in the browser; the element holds the same HTML on the
            # server, so a later update of it cannot bring the old looks back.
            grid._props[grid.CONTENT_PROP] = self._grid(group)
            patches.extend(_card_patch(t) for t in changed)
        for ref in touched.values():
            ref["summary"].set_content(position_header_html(ref["pos"]))
            ref["box"].set_visibility(self._pos_shown(ref["pos"]))
            grid = ref["grid"]
            if grid is None:
                continue
            if rebuild:
                grid.set_content(self._pos_html(ref["pos"]))
            else:
                grid._props[grid.CONTENT_PROP] = self._pos_html(ref["pos"])
        if (patches or beams) and self.root is not None and not self.root.is_deleted:
            ui.run_javascript(
                _PATCH_JS.replace("__ROOT__", str(self.root.id))
                .replace("__PATCHES__", json.dumps(patches))
                .replace("__BEAMS__", json.dumps(beams))
            )

    # ── Clicks ──

    def _on_click(self, e) -> None:
        args = e.args or {}
        key = args.get("key", "")
        if self.review and not args.get("zoom"):
            return self.on_toggle(key) if self.on_toggle is not None else None
        tilt = self._tilt_by_key.get(key)
        if tilt is None:
            if key in self._frame_keys:
                ui.notify("That tilt has no motion-corrected average to show yet.", type="warning")
            else:
                ui.notify("That tilt is no longer in the registry; Refresh the view.", type="warning")
            return None
        open_tilt_viewer(tilt, self.project_path)
        return None


# The changed cards' look and caption, and grouped by position the beams' lines, as restate sends them.
_PATCH_JS = """(() => {
    const root = getHtmlElement(__ROOT__);
    if (!root) return;
    for (const p of __PATCHES__) {
        const card = root.querySelector('.cb-tp-card[data-key="' + CSS.escape(p.k) + '"]');
        if (!card) continue;
        card.className = p.look;
        const cap = card.querySelector('.cb-tp-cap');
        if (cap) cap.innerHTML = p.cap;
    }
    for (const b of __BEAMS__) {
        const line = root.querySelector('.cb-tp-beam[data-ts="' + CSS.escape(b.ts) + '"]');
        if (line) line.innerHTML = b.html;
    }
})()"""


def _card_patch(t: dict) -> dict:
    return {"k": t["key"], "look": f"cb-tp-card {t['look']}".strip(), "cap": _caption_html(t)}


# ── The viewer ────────────────────────────────────────────────────────────────


def open_tilt_viewer(tilt: dict, project_path: Path) -> None:
    """The full-size viewer on one tilt's averaged MRC (512 px to full size), parented at the
    page layout so a re-render of the tile or group that opened it cannot delete it mid-load."""
    with dialog_host():
        _show_upsample(tilt["mrc"], tilt["key"], Path(project_path))


def _render_mrc_preview(mrc_path: str, target_size: int = 1024) -> tuple[str, tuple]:
    """Read an MRC, Fourier-crop it to target_size, normalise it for display and return it as
    a base64 PNG with the source's shape. target_size 0 keeps the full resolution."""
    import io

    import mrcfile
    import numpy as np
    from PIL import Image
    from scipy.fft import fft2, fftshift, ifft2, ifftshift
    from scipy.ndimage import gaussian_filter

    with mrcfile.open(mrc_path, permissive=True) as mrc:
        data = mrc.data.astype(np.float32)

    orig_shape = data.shape

    # Fourier crop if requested and needed
    if target_size > 0 and (data.shape[0] > target_size or data.shape[1] > target_size):
        ft = fftshift(fft2(data))
        new = np.zeros((target_size, target_size), dtype=ft.dtype)
        cy, cx = [d // 2 for d in data.shape]
        ny, nx = target_size // 2, target_size // 2
        sy = slice(cy - min(cy, ny), cy + min(cy, ny))
        sx = slice(cx - min(cx, nx), cx + min(cx, nx))
        dy = slice(ny - min(cy, ny), ny + min(cy, ny))
        dx = slice(nx - min(cx, nx), nx + min(cx, nx))
        new[dy, dx] = ft[sy, sx]
        data = ifft2(ifftshift(new)).real

    # Gentle denoise — scale sigma with resolution
    sigma = 0.4 if data.shape[0] <= 1024 else 0.7
    data = gaussian_filter(data, sigma=sigma)

    # Percentile-based contrast (robust to hot/dead pixels)
    p_lo, p_hi = np.percentile(data, [1.0, 99.0])
    data = np.clip(data, p_lo, p_hi)
    rng = p_hi - p_lo
    if rng > 1e-9:
        data = (data - p_lo) / rng
    else:
        data = np.zeros_like(data)

    data = (data * 255).astype(np.uint8)
    img = Image.fromarray(data, mode="L")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode(), orig_shape


def _show_upsample(mrc_path: str, key: str, project_path: Path) -> None:
    """Open a dialog with resolution selector and live re-rendering."""
    if not mrc_path:
        ui.notify("No MRC path available", type="warning")
        return

    abs_mrc = Path(mrc_path) if Path(mrc_path).is_absolute() else project_path / mrc_path

    if not abs_mrc.exists():
        ui.notify(f"MRC not found: {abs_mrc}", type="warning")
        return

    current = {"size": 1024}

    dlg = ui.dialog().props("maximized")
    with dlg:
        with ui.column().style("width: 100%; height: 100%; background: #0a0a0a; padding: 0; position: relative;"):
            # ── Top bar ──
            with ui.row().style(
                "position: absolute; top: 0; left: 0; right: 0; z-index: 10; padding: 8px 12px; "
                "background: rgba(0,0,0,0.7); align-items: center; gap: 8px; justify-content: space-between;"
            ):
                with ui.row().style("align-items: center; gap: 6px;"):
                    ui.label(key).style(f"{MONO} font-size: 10px; color: #cbd5e1;")
                    size_label = ui.label("").style(f"{MONO} font-size: 9px; color: {_MUTED};")

                res_buttons: dict = {}
                btn_base = f"{MONO} font-size: 9px; padding: 1px 8px; border-radius: 3px; min-height: 0; "
                active = btn_base + "color: white; background: #334155;"
                inactive = btn_base + "color: #cbd5e1; background: transparent;"

                def _select_res(s):
                    current["size"] = s
                    for sz_key, b in res_buttons.items():
                        b.style(active if sz_key == s else inactive)
                    _load_at_size(s)

                with ui.row().style("align-items: center; gap: 4px;"):
                    for sz, label in [(512, "512"), (1024, "1K"), (2048, "2K"), (0, "Full")]:
                        btn = (
                            ui.button(label, on_click=lambda _, s=sz: _select_res(s))
                            .props("dense no-caps flat")
                            .style(active if sz == 1024 else inactive)
                        )
                        res_buttons[sz] = btn

                    ui.button(icon="close", on_click=dlg.close).props("flat round dense").style("color: white;")

            # ── Image area ──
            img_holder = ui.column().style(
                "width: 100%; height: 100%; align-items: center; justify-content: center; padding-top: 40px;"
            )
            with img_holder:
                ui.spinner(size="lg").style("color: white;")
                ui.label("Generating preview...").style(f"{SANS} font-size: 10px; color: #cbd5e1; margin-top: 8px;")

    def _load_at_size(sz):
        img_holder.clear()
        with img_holder:
            ui.spinner(size="lg").style("color: white;")
            sz_txt = "full resolution" if sz == 0 else f"{sz}px"
            ui.label(f"Rendering at {sz_txt}...").style(f"{SANS} font-size: 10px; color: #cbd5e1; margin-top: 8px;")

        async def _do():
            try:
                b64, orig = await asyncio.to_thread(_render_mrc_preview, str(abs_mrc), sz)
                out_px = orig[0] if sz == 0 else min(sz, orig[0])
                size_label.text = f"{out_px}px (source: {orig[0]}×{orig[1]})"
                img_holder.clear()
                with img_holder:
                    ui.html(
                        f'<img src="data:image/png;base64,{b64}" '
                        'style="max-width: 95vw; max-height: calc(100vh - 50px); object-fit: contain;" />',
                        sanitize=False,
                    )
            except Exception as e:
                logger.exception("Upsample failed")
                img_holder.clear()
                with img_holder:
                    ui.label(f"Error: {e}").style(f"color: {_RED_TEXT};")

        ui.timer(0.05, _do, once=True)

    dlg.open()
    ui.timer(0.1, lambda: _load_at_size(1024), once=True)

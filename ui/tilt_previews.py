"""Tilt previews: every tilt of the project as the PNG the tilt filter makes of its
motion-corrected average, grouped by tilt series, each showing what it is.

Two hosts show them. The Tomograms view (``ui/tomo_gallery.py``) has a Tilts tab, and a tile on
its tomogram wall can swap its slice for a mosaic of its own series' tilts, in the box the slice
occupied. The tilt filter's job page shows the same gallery in review mode. Both come from
here: the collection (the registry plus one listing of the PNG directory), the gallery
(``TiltGallery``), the card, mosaic, header and caption HTML, the mosaic's fit, and the click
scripts.

Every tilt carries its state (``services/tilt_series/tilt_state.py``): whether it is in the
tomogram and, if not, why; what the uncommitted review says; the model's P(bad) in the DL
modes; whether its exposure was dark. A card shows it as its border, stripes and caption, a
mosaic cell as stripes and an amber border, a header strip as one tick per tilt, and a
tomogram's caption sums its series up. A card's caption also carries the tilt's recorded
numbers, shown as the gallery's metric chooser says, red where the number is an outlier among
the project's tilts at the same |stage tilt|. A dark exposure and an outlier are markers only:
nothing here drops a tilt.

A project holds thousands of tilts, so a series is one HTML string with one delegated click
handler rather than an element per tilt. A tilt opens the tilt filter's full-size viewer,
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
# click to itself, anywhere else on the header still toggles.
STRIP_CLICK_JS = """(event) => {
    const tick = event.target.closest('[data-key]');
    if (!tick) return;
    event.stopPropagation();
    emit({key: tick.dataset.key});
}"""

# A card grid's handlers. The popover is the gallery's own (a direct child of its root), found
# from the grid, never by a document-wide query: the Tilts tab and the job page can both be in
# the page, holding the same tilts. Fixed-positioned at the caption, so a group's
# `overflow: hidden` cannot clip it. Hover and pinning run in the browser alone.
_POPOVER_AT_CAPTION = """
    const root = event.currentTarget.closest('.cb-tg-root');
    const pop = root && root.querySelector(':scope > .cb-tp-popover');
    const src = cap.querySelector('.cb-tp-popsrc');
    if (!pop || !src) return;
    if (pinning && pop._unpin) pop._unpin();
    if (pop.dataset.pinned && !pinning) return;
    pop.innerHTML = src.innerHTML;
    pop.style.display = 'block';
    const r = cap.getBoundingClientRect();
    const w = pop.offsetWidth, h = pop.offsetHeight;
    const below = r.bottom + 4 + h <= window.innerHeight - 8;
    pop.style.left = Math.max(8, Math.min(r.left, window.innerWidth - w - 8)) + 'px';
    pop.style.top = (below ? r.bottom + 4 : Math.max(8, r.top - h - 4)) + 'px';
"""

GRID_HOVER_JS = (
    """(event) => {
    const cap = event.target.closest('.cb-tp-cap');
    if (!cap) return;
    const pinning = false;"""
    + _POPOVER_AT_CAPTION
    + "}"
)

GRID_OUT_JS = """(event) => {
    const cap = event.target.closest('.cb-tp-cap');
    if (!cap || cap.contains(event.relatedTarget)) return;
    const root = event.currentTarget.closest('.cb-tg-root');
    const pop = root && root.querySelector(':scope > .cb-tp-popover');
    if (pop && !pop.dataset.pinned) pop.style.display = 'none';
}"""

# The flag labels (review mode), the caption pins its popover until the next click elsewhere or
# Escape, the rest of the card opens the viewer.
GRID_CLICK_JS = (
    """(event) => {
    const card = event.target.closest('[data-key]');
    if (!card) return;
    if (event.target.closest('.cb-tp-flag')) { emit({key: card.dataset.key, flag: true}); return; }
    const cap = event.target.closest('.cb-tp-cap');
    if (!cap) { emit({key: card.dataset.key}); return; }
    const pinning = true;"""
    + _POPOVER_AT_CAPTION
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
_MUTED = "#94a3b8"

DARK_RULE = (
    f"Dark exposure: the tilt's mdoc mean counts are under {DIM_EXPOSURE_FRACTION:.0%} of its tilt series' "
    f"median; under {BLANK_EXPOSURE_FRACTION:.0%} it is blank (the beam was blocked, by a grid bar or the "
    "lamella edge). A blank exposure in a tomogram back-projects as straight streaks. This is a marker "
    "only: nothing is dropped because of it. The tilt filter's review decides."
)

OUTLIER_RULE = (
    f"Red number: more than {OUTLIER_ROBUST_SDS:g} robust SDs (1.4826 × the median absolute deviation) worse than "
    f"the project's tilts in the same {OUTLIER_BAND_DEG:g}° band of |stage tilt|. A band with fewer than "
    f"{OUTLIER_MIN_BAND} values, or no spread, marks nothing. A marker, never a drop."
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


# ── Metrics ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Metric:
    key: str
    label: str  # the chooser's chip, the sort option, the popover's row
    tip: str  # the chip's tooltip: what the number tells
    token: Callable[[float], str]  # the caption's short form
    unit: str = ""
    judged: bool = False  # band outliers apply
    two_sided: bool = False  # judged and sorted on |value|
    low_is_worse: bool = False  # worst first sorts ascending


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


_METRICS = (
    _Metric(
        "num", "#N", "The tilt's number: its place in the acquisition, i.e. the dose order.", lambda v: f"#{v:.0f}"
    ),
    _Metric(
        "pbad",
        "P(bad)",
        "The model's P(bad) from the latest DL run: a ranking, not a calibrated probability. Red at or above the "
        "threshold.",
        lambda v: f"p{v:.2f}",
    ),
    _Metric(
        "exp",
        "exposure",
        "The tilt's mdoc mean counts as a share of its series' median. A dark tilt shows its amber token anyway.",
        lambda v: f"exp {_pct_short(v)}",
        low_is_worse=True,
    ),
    _Metric(
        "ctf",
        "CTF fit",
        "How far out Warp fit the Thon rings (CTFResolutionEstimate); worse with tilt and thickness.",
        lambda v: f"ctf {v:.1f}Å",
        unit=" Å",
        judged=True,
    ),
    _Metric(
        "mot",
        "motion",
        "Beam-induced motion during the exposure (MeanFrameMovement, Warp's units).",
        lambda v: f"mot {v:.2f}",
        judged=True,
    ),
    _Metric(
        "ddf",
        "Δ defocus",
        "The tilt's fitted defocus minus its series' median: a collapsed or diverged CTF fit stands out.",
        lambda v: f"Δdf {_signed(v, 2)}",
        unit=" µm",
        judged=True,
        two_sided=True,
    ),
    _Metric(
        "ast",
        "astigmatism",
        "The difference between the fitted defocus along the two axes.",
        lambda v: f"ast {v:.2f}",
        unit=" µm",
        judged=True,
    ),
    _Metric(
        "dose",
        "dose before",
        "Electron dose accumulated before this tilt, from the scope's calibration.",
        lambda v: f"dose {v:.0f}",
        unit=" e⁻/Å²",
    ),
    _Metric(
        "shift",
        "alignment shift",
        "How far alignment moved the tilt: a proxy for how hard it was to align.",
        lambda v: f"sh {v:.0f}Å",
        unit=" Å",
        judged=True,
    ),
)
_METRIC = {m.key: m for m in _METRICS}
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
    ``metrics`` what ``restate_group`` re-derives from. The facts are ``{span, looks, lacking,
    n_series, present, dl, any_outlier, bands}``: the range of every stage tilt, the card looks
    present, per field family the number of series without it, the metrics some tilt has,
    whether P(bad) shows and any number is an outlier, and the project's outlier bands.

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
    looks: set[str] = set()
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
                # For re-deriving the states after a label or threshold change (restate_group).
                "series": ts,
                "metrics": metrics,
            }
        )
    groups.sort(key=lambda g: (g["order"], g["ts"]))
    looks.discard("")
    facts = {
        "span": (min(angles), max(angles)) if angles else None,
        "looks": looks,
        "lacking": lacking,
        "n_series": len(groups),
        "present": present,
        "dl": ctx["mode"] in DL_MODES and "pbad" in present,
        "any_outlier": any(t["outliers"] for g in groups for t in g["tilts"]),
        "bands": bands,
    }
    return groups, facts


def _tilt(key: str, s: TiltState, m: dict, bands: dict, ctx: dict) -> dict:
    """One tilt as the views show it: ``{key, angle, number, state, label, vals, outliers,
    disagree, tip, title, look, cell_look, tokens, pop}``. ``label`` is a human's label (the
    review mode's flag); ``vals`` every metric's number; ``outliers`` the metrics whose number is
    an outlier; ``disagree`` whether a human's label and the model's call differ; ``tip`` the
    mosaic cell's multi-line title; ``title`` the card image's one line; ``tokens`` the card
    caption; ``pop`` its popover."""
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
        "title": f"{_signed(s.angle, 1)}° · tilt {s.number} · click to view",
        "look": _look(s),
        "cell_look": _look(s, review=False),
        "tokens": _tokens_html(s, vals, outliers, threshold),
        "pop": _popover_html(s, m, vals, outliers, bands, threshold),
    }


def restate_group(group: dict, ctx: dict, bands: dict) -> list[dict]:
    """Re-derive a group's tilts after the review changed (a label, the threshold, cleared
    labels): their states, looks, flags, captions and popovers, its summary and strip ticks.
    The metrics and outliers stay as collected. Returns the tilts whose card changed."""
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


def _exposure_line(s: TiltState, m: dict) -> str | None:
    if s.exposure_ratio is None:
        return None
    counts = f"{m['counts']:.3g} against {m['median_counts']:.3g}"
    ratio = f"{_pct_fine(s.exposure_ratio)} of the series' median mdoc counts ({counts})"
    return f"Dark exposure ({s.exposure}): {ratio}." if s.is_dark else f"Exposure: {ratio}."


def tilt_tip(s: TiltState, m: dict, threshold: float | None) -> str:
    """A tilt's native tooltip (a mosaic cell's), one fact per line: what it is, then its
    recorded numbers, then what is not recorded."""
    lines = [f"{_signed(s.angle, 1)}° · tilt {s.number} · {s.frame_id}", where_sentence(s)]
    review = _review_sentence(s)
    if review:
        lines.append(review)
    if s.p_bad is not None:
        cut = f" against the threshold {threshold:.2f}" if threshold is not None else ""
        lines.append(f"P(bad) {s.p_bad:.2f}{cut}: the model's ranking, not a calibrated probability.")
    exposure = _exposure_line(s, m)
    if exposure:
        lines.append(exposure)
        if s.is_dark:
            lines.append(DARK_RULE)
    numbers: list[str] = []
    for value, shown in (
        (m["ctf_res"], lambda v: f"CTF fit {v:.1f} Å"),
        (m["motion"], lambda v: f"motion {v:.2f}"),
        (m["ddefocus"], lambda v: f"Δ defocus {v:+.2f} µm ({m['defocus_source']})"),
        (m["dose"], lambda v: f"{v:.1f} e⁻/Å² before"),
        (m["shift"], lambda v: f"alignment shift {v:.0f} Å"),
    ):
        if value is not None:
            numbers.append(shown(value))
    if numbers:
        lines.append(" · ".join(numbers))
    missing = _missing(s, m)
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


def _band_words(metric: _Metric, stat: BandStat) -> str:
    unit = metric.unit
    what = f"|{metric.label}|" if metric.two_sided else metric.label
    band = f"{stat.lo:.0f}–{stat.hi:.0f}° |tilt|"
    if stat.n < OUTLIER_MIN_BAND:
        return f"{band}: {stat.n} tilts with a {what}, too few to judge"
    if stat.robust_sd <= 0:
        return f"{band}: {stat.n} tilts, no spread in {what}, nothing judged"
    return f"{band}: median {stat.median:.3g}{unit}, robust SD {stat.robust_sd:.2g}{unit}, {stat.n} tilts"


def _popover_html(s: TiltState, m: dict, vals: dict, outliers: set, bands: dict, threshold: float | None) -> str:
    """Everything recorded about a tilt, for its caption's popover: what it is, the review, then
    every metric with its band's median and robust SD (red where it is an outlier, with the
    rule), and what is not recorded."""
    esc = html.escape
    rows = [
        f"<div><b>{_signed(s.angle, 1)}° · tilt {s.number}</b> "
        f'<span style="color:{_MUTED};font-family:ui-monospace,monospace;font-size:9px;">'
        f"{esc(s.frame_id)}</span></div>",
        f"<div>{esc(where_sentence(s))}</div>",
    ]
    review = _review_sentence(s)
    if review:
        rows.append(f"<div>{esc(review)}</div>")
    if s.p_bad is not None:
        hot = threshold is not None and s.p_bad >= threshold
        cut = f" against the threshold {threshold:.2f}" if threshold is not None else ""
        color = _RED_TEXT if hot else "inherit"
        rows.append(
            f'<div><span style="color:{color};font-weight:600;">P(bad) {s.p_bad:.2f}</span>{cut}: the model\'s '
            "ranking, not a calibrated probability.</div>"
        )
    exposure = _exposure_line(s, m)
    if exposure:
        color = _AMBER_TEXT if s.is_dark else "inherit"
        rows.append(f'<div style="color:{color};">{esc(exposure)}</div>')
        if s.is_dark:
            rows.append(f'<div style="color:{_MUTED};">{esc(DARK_RULE)}</div>')
    table = []
    for metric in _METRICS:
        if metric.key in ("num", "pbad", "exp"):
            continue
        value = vals[metric.key]
        if value is None:
            continue
        shown = _signed(value, 2) if metric.key == "ddf" else f"{value:.3g}"
        out = metric.key in outliers
        cell = f'<span style="color:{_RED_TEXT if out else "inherit"};font-weight:{600 if out else 400};">'
        cell += f"{esc(shown)}{esc(metric.unit)}</span>"
        stat = bands[metric.key][0].get(s.frame_id) if metric.judged else None
        band = esc(_band_words(metric, stat)) if stat is not None else ""
        if metric.key == "ddf" and m["defocus_source"]:
            band = f"{esc(m['defocus_source'])}; {band}"
        table.append(
            f'<tr><td style="padding:0 8px 0 0;color:{_MUTED};white-space:nowrap;">{esc(metric.label)}</td>'
            f'<td style="padding:0 8px 0 0;white-space:nowrap;">{cell}</td>'
            f'<td style="color:{_MUTED};">{band}</td></tr>'
        )
    if table:
        rows.append(f'<table style="border-collapse:collapse;margin:3px 0;">{"".join(table)}</table>')
    for key in sorted(outliers, key=lambda k: [m_.key for m_ in _METRICS].index(k)):
        metric, stat = _METRIC[key], bands[key][0][s.frame_id]
        rows.append(
            f'<div style="color:{_RED_TEXT};">{esc(metric.label)}: more than {OUTLIER_ROBUST_SDS:g} robust SDs worse '
            f"than the project's {stat.n} tilts at {stat.lo:.0f}–{stat.hi:.0f}° |tilt| (cut "
            f"{stat.cut:.3g}{esc(metric.unit)}). A marker, never a drop.</div>"
        )
    missing = _missing(s, m)
    if missing:
        rows.append(f'<div style="color:{_MUTED};">Not recorded: {esc(", ".join(missing))}.</div>')
    return "".join(rows)


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


def _tokens_html(s: TiltState, vals: dict, outliers: set, threshold: float | None) -> str:
    """A card's caption: the angle, the dark token when dark (state, always shown), then every
    metric's token, which the gallery's chooser shows or hides by a class on its root. Red where
    the number is an outlier; P(bad) red at or above the threshold. The row clips from the right,
    so the angle and the state survive the smallest card."""
    parts = [f"<b>{_signed(s.angle, 1)}°</b>"]
    if s.is_dark:
        parts.append(f'<span style="color:{_AMBER_TEXT};font-weight:600;">dark {_pct_short(s.exposure_ratio)}</span>')
    for metric in _METRICS:
        value = vals[metric.key]
        if value is None or (metric.key == "exp" and s.is_dark):
            continue
        cls = f"cb-tm cb-tm-{metric.key}"
        if metric.key in outliers:
            cls += " cb-tm-out"
        if metric.key == "pbad" and threshold is not None and value >= threshold:
            cls += " cb-tm-hot"
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


def flag_class(label: str | None) -> str:
    """The label flag's class: filled red for your bad, filled slate for your good, else drawn
    only while the card is hovered."""
    return {"bad": "cb-tp-flag bad", "good": "cb-tp-flag good"}.get(label or "", "cb-tp-flag")


def _flag_title(label: str | None) -> str:
    if label == "bad":
        return "Your label: bad. Click for good."
    if label == "good":
        return "Your label: good. Click for bad."
    return "Label this tilt: a click sets the opposite of what the review says now."


def _caption_html(t: dict) -> str:
    """A card caption's inside: its tokens, and the hidden block its popover copies."""
    return f'{t["tokens"]}<div class="cb-tp-popsrc" style="display:none;">{t["pop"]}</div>'


def card_html(t: dict, *, review: bool) -> str:
    """One tilt as a card: the preview, with the tilt's state as its border and stripes and, in
    review mode, the label flag in its corner; under it the caption, carrying the hidden block
    its popover copies."""
    flag = ""
    if review:
        flag = f'<span class="{flag_class(t["label"])}" title="{html.escape(_flag_title(t["label"]))}"></span>'
    return (
        f'<div class="cb-tp-card {t["look"]}" data-key="{html.escape(t["key"])}">'
        f'<div class="cb-tp-img" title="{html.escape(t["title"])}">{_image(t)}{flag}</div>'
        f'<div class="cb-tp-cap">{_caption_html(t)}</div>'
        "</div>"
    )


def grid_html(tilts: Sequence[dict], *, review: bool) -> str:
    """A series' cards. The column width is the gallery root's `--cb-tp-card`, so a size switch
    changes one variable and re-renders nothing."""
    cards = "".join(card_html(t, review=review) for t in tilts)
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


def legend_html(looks: set[str], *, outliers: bool = False, review: bool = False) -> str:
    """One line naming each look the project's tilts take, each with its explanation as a
    tooltip; then the red number when any is red, and the slate flag in review mode."""
    item = (
        '<span title="{title}" style="display:inline-flex;align-items:center;gap:4px;">'
        '<span style="width:10px;height:10px;box-sizing:border-box;border:{border};background:{bg};'
        'border-radius:2px;"></span>{text}</span>'
    )
    items = [
        item.format(title=html.escape(title), border=border, bg=bg, text=html.escape(text))
        for look, text, title, bg, border in _LEGEND
        if look in looks
    ]
    if outliers:
        items.append(
            f'<span title="{html.escape(OUTLIER_RULE)}" style="display:inline-flex;align-items:center;gap:4px;">'
            f'<span style="color:{_RED_TEXT};font-weight:600;font-family:ui-monospace,monospace;">1.0</span>'
            "red number: more than 3 robust SDs worse than the project's tilts at the same |tilt|; a marker, "
            "never a drop</span>"
        )
    if review:
        items.append(
            item.format(
                title=html.escape("You labelled it good; Approve keeps it whatever the model says."),
                border="1.5px solid #ffffff",
                bg="#475569",
                text="your label: good",
            )
        )
    return f'<div style="display:flex;flex-wrap:wrap;gap:4px 14px;font-size:9px;color:#64748b;">{"".join(items)}</div>'


# ── The gallery ───────────────────────────────────────────────────────────────

_SHOW = (
    ("all", "all"),
    ("flagged", "flagged"),
    ("out", "not in the tomogram"),
    ("dark", "dark"),
    ("outliers", "outliers"),
)
_POPOVER_STYLE = (
    "position: fixed; z-index: 6000; display: none; max-width: 380px; background: #ffffff; "
    "border: 1px solid #e2e8f0; border-radius: 5px; box-shadow: 0 6px 18px rgba(15,23,42,0.16); "
    "padding: 6px 8px; font-size: 10px; line-height: 1.45; color: #334155; white-space: normal;"
)


class TiltGallery:
    """Every tilt of the project as a card, grouped by tilt series: the Tilts tab's gallery and
    the tilt-filter job page's, one component.

    The host collects (``collect_tilt_groups``), hands the result over with ``set_data`` and
    places the two parts: ``render_controls`` (S/M/L when the host has none, sort, show, the
    metric chooser, expand and collapse) and ``render`` (notes, the legend, the groups). Groups
    start collapsed and a group's cards are built on its first expand, one HTML string with one
    delegated click per series. Sort and show re-render the open grids only; the size and the
    metric chooser set a CSS variable and classes on the gallery's root, so neither re-renders.

    Review mode (the job page) draws a label flag in each image's corner, and a click on it calls
    ``on_flag(key)``. A click elsewhere on the image opens the full-size viewer; hovering a
    caption shows its popover of every metric, and a click on the caption pins it."""

    def __init__(
        self,
        project_path: Path,
        *,
        review: bool = False,
        on_flag: Callable[[str], Any] | None = None,
        size: str = "m",
        size_control: bool = False,
    ) -> None:
        self.project_path = Path(project_path)
        self.review = review
        self.on_flag = on_flag
        self.size = size
        self.size_control = size_control
        self.groups: list[dict] = []
        self.facts: dict = {}
        # Kept on the gallery, so a re-render, a Refresh or a host's poll keeps them.
        self.expanded: set[str] = set()
        self.sort = "angle"
        self.show = "all"
        self.metrics_on: set[str] = set(DEFAULT_METRICS)
        self.root: Any = None
        self._groups_ui: dict[str, dict] = {}
        self._tilt_by_key: dict[str, dict] = {}
        self._frame_keys: set[str] = set()
        self._sort_select: Any = None
        self._show_seg: Any = None
        self._size_seg: Any = None
        self._chips: dict[str, Any] = {}

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
        """The metrics the chooser offers: those some tilt has, P(bad) only where it shows."""
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
            opts["pbad"] = "P(bad), worst first"
        for key in offered:
            if key not in ("num", "pbad") and key in self.metrics_on:
                opts[key] = f"{_METRIC[key].label}, worst first"
        return opts

    def _show_options(self) -> list[tuple[str, str]]:
        return [*_SHOW, ("disagree", "disagree")] if self.facts.get("dl") else list(_SHOW)

    # ── Controls ──

    def render_controls(self) -> None:
        """S/M/L (when the host has none), sort, show, the metric chooser, Expand all and
        Collapse all, in the current slot."""
        self._chips = {}
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
            offered = self._metric_keys()
            if offered:
                ui.label("metrics").classes("cb-gal-toolbar-label")
                for key in offered:
                    self._render_chip(key)
            house_button("Expand all", lambda: self.expand_all(True))
            house_button("Collapse all", lambda: self.expand_all(False))

    def _render_chip(self, key: str) -> None:
        metric = _METRIC[key]
        chip = ui.element("div").classes("cb-gal-sp" + ("" if key in self.metrics_on else " off"))
        chip.on("click", lambda _e, k=key: self._toggle_metric(k))
        chip.tooltip(f"{metric.tip} Click to show or hide it on every card.")
        with chip:
            ui.element("div").classes("cb-gal-sp-dot").style("background: #64748b;")
            ui.label(metric.label).classes("cb-gal-sp-name")
        self._chips[key] = chip

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

    def _toggle_metric(self, key: str) -> None:
        on = key not in self.metrics_on
        if on:
            self.metrics_on.add(key)
        else:
            self.metrics_on.discard(key)
        if self.root is not None and not self.root.is_deleted:
            if on:
                self.root.classes(add=f"cb-tg-m-{key}")
            else:
                self.root.classes(remove=f"cb-tg-m-{key}")
        chip = self._chips.get(key)
        if chip is not None:
            if on:
                chip.classes(remove="off")
            else:
                chip.classes(add="off")
        opts = self._sort_options()
        resort = self.sort not in opts
        if resort:
            self.sort = "angle"
        if self._sort_select is not None and not self._sort_select.is_deleted:
            self._sort_select.set_options(opts, value=self.sort)
        if resort:
            self._rerender_open()

    # ── Body ──

    def render(self, notes: Sequence[tuple[str, str, bool]] = ()) -> None:
        """The gallery in the current slot: `notes` (text, colour, spinner) from the host, the
        project's own notes, the legend and the groups."""
        classes = " ".join(["cb-tg-root", *(f"cb-tg-m-{k}" for k in sorted(self.metrics_on))])
        self.root = (
            ui.element("div").classes(classes).style(f"--cb-tp-card: {TILT_CARD_PX[self.size]}px; min-width: 0;")
        )
        self._groups_ui = {}
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
            looks = self.facts.get("looks") or set()
            if looks or self.facts.get("any_outlier") or self.review:
                with ui.element("div").style("padding: 8px 10px 0;"):
                    ui.html(
                        legend_html(looks, outliers=bool(self.facts.get("any_outlier")), review=self.review),
                        sanitize=False,
                    )
            with ui.element("div").style("display: flex; flex-direction: column; gap: 6px; padding: 10px;"):
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
            self._show_group(ref, True)

    def _set_group_open(self, ts: str, on: bool) -> None:
        if on:
            self.expanded.add(ts)
        else:
            self.expanded.discard(ts)
        ref = self._groups_ui.get(ts)
        if ref is not None:
            self._show_group(ref, on)

    def _show_group(self, ref: dict, on: bool) -> None:
        # Display written in full each time: .style() merges, and a left-out display would
        # keep the earlier `none`.
        ref["body"].style(f"padding: 4px; display: {'block' if on else 'none'};")
        if on:
            ref["chevron"].classes(add="open")
        else:
            ref["chevron"].classes(remove="open")
        if on and ref["grid"] is None:
            with ref["body"]:
                grid = ui.html(self._grid(ref["group"]), sanitize=False)
            grid.on("click", handler=self._on_click, js_handler=GRID_CLICK_JS)
            grid.on("mouseover", js_handler=GRID_HOVER_JS)
            grid.on("mouseout", js_handler=GRID_OUT_JS)
            ref["grid"] = grid

    def expand_all(self, on: bool) -> None:
        for ts, ref in self._groups_ui.items():
            if not on or self._shown(ref["group"]):
                self._set_group_open(ts, on)

    def _grid(self, group: dict) -> str:
        return grid_html(self._ordered([t for t in group["tilts"] if self._passes(t)]), review=self.review)

    def _rerender_open(self) -> None:
        """After a sort or show switch: the open grids re-render, and a group with nothing to
        show hides. Header counts stay whole-series."""
        for ref in self._groups_ui.values():
            ref["box"].set_visibility(self._shown(ref["group"]))
            if ref["grid"] is not None:
                ref["grid"].set_content(self._grid(ref["group"]))

    def _shown(self, group: dict) -> bool:
        return self.show == "all" or any(self._passes(t) for t in group["tilts"])

    def _passes(self, t: dict) -> bool:
        s: TiltState = t["state"]
        if self.show == "flagged":
            return s.flagged
        if self.show == "out":
            return s.drop is not None or s.in_tomogram is False
        if self.show == "dark":
            return s.is_dark
        if self.show == "outliers":
            return bool(t["outliers"])
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
        None, restate_group), replace those groups' headers, and patch the changed cards in place
        with one JavaScript call scoped to the gallery's root: the look, the flag and the caption.
        A grid whose membership depends on the review (show · flagged or disagree) re-renders
        instead."""
        bands = self.facts.get("bands") or {}
        wanted = None if ts_ids is None else set(ts_ids)
        patches: list[dict] = []
        for group in self.groups:
            if wanted is not None and group["ts"] not in wanted:
                continue
            changed = restate_group(group, ctx, bands)
            for t in group["tilts"]:
                self._tilt_by_key[t["key"]] = t
            ref = self._groups_ui.get(group["ts"])
            if ref is None:
                continue
            ref["summary"].set_content(group_header_html(group, self.facts.get("span")))
            ref["box"].set_visibility(self._shown(group))
            grid = ref["grid"]
            if grid is None or not changed:
                continue
            if self.show in ("flagged", "disagree"):
                grid.set_content(self._grid(group))
                continue
            # The cards are patched in the browser; the element holds the same HTML on the
            # server, so a later update of it cannot bring the old looks back.
            grid._props[grid.CONTENT_PROP] = self._grid(group)
            patches.extend(_card_patch(t) for t in changed)
        if patches and self.root is not None and not self.root.is_deleted:
            ui.run_javascript(
                _PATCH_JS.replace("__ROOT__", str(self.root.id)).replace("__PATCHES__", json.dumps(patches))
            )

    # ── Clicks ──

    def _on_click(self, e) -> None:
        args = e.args or {}
        key = args.get("key", "")
        if args.get("flag"):
            if self.review and self.on_flag is not None:
                return self.on_flag(key)
            return None
        tilt = self._tilt_by_key.get(key)
        if tilt is None:
            if key in self._frame_keys:
                ui.notify("That tilt has no motion-corrected average to show yet.", type="warning")
            else:
                ui.notify("That tilt is no longer in the registry; Refresh the view.", type="warning")
            return None
        open_tilt_viewer(tilt, self.project_path)
        return None


# One card's new look, flag and caption, as restate sends it.
_PATCH_JS = """(() => {
    const root = getHtmlElement(__ROOT__);
    if (!root) return;
    for (const p of __PATCHES__) {
        const card = root.querySelector('.cb-tp-card[data-key="' + CSS.escape(p.k) + '"]');
        if (!card) continue;
        card.className = p.look;
        const flag = card.querySelector('.cb-tp-flag');
        if (flag) { flag.className = p.flag; flag.title = p.ftitle; }
        const cap = card.querySelector('.cb-tp-cap');
        if (cap) cap.innerHTML = p.cap;
    }
})()"""


def _card_patch(t: dict) -> dict:
    return {
        "k": t["key"],
        "look": f"cb-tp-card {t['look']}".strip(),
        "flag": flag_class(t["label"]),
        "ftitle": _flag_title(t["label"]),
        "cap": _caption_html(t),
    }


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

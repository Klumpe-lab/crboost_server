"""What a job made, read from what it wrote: the Results section of its page and the branch map's large card read the
same result, so a number reads the same in both.

`job_results` reads, in a thread, from the job's own files and the registry (as plain dicts, `registry_dicts`), and
returns a `JobResults`:
- `headline`: one computed line, what came out and its key numbers;
- `warn`: only for what stands out (outliers by name, failures, what was refused); `notes`: what else there is to
  know;
- `tiles`: headline numbers a job has of its own (M's round, CTF refine's moves);
- `wall`: one picture per tilt series, each with one number (a reconstruction, a denoised slab, membranes, picks);
- `maps`: a volume's three central slices (an average, a class, a mask), made by `map_slices_render`;
- `plot`: at most one, a value per tilt series or a histogram;
- `table`: one row per tilt series where a job has more to say of each than a number.

Nothing is drawn here. Pictures already on disk are named by path and asset URL; pictures still to make are named
by the volume they come from, and the page renders them (services/renders.py).

Outliers are marked by one rule on every page (`OUTLIER_RULE`), or by a job's own cut where it has one (template
matching's one false positive, a tomogram without picks); they are marked, never hidden.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
from collections import Counter
from urllib.parse import quote
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from services.array_tasks import manifest_items, scan_statuses, sort_ts_by_position, ts_pretty_name
from services.dashboard_data import vis_asset_url
from services.models_base import JobType
from services.renders import Render
from services.scheduling_and_orchestration import stop_after_iteration
from services.workers import WorkKind

logger = logging.getLogger(__name__)

THUMB = 192
NAMED = 3  # outliers named in a note; the rest are counted
OUTLIER_RULE = (
    "Marked: more than 3σ worse than this job's median, σ from the median absolute deviation (×1.4826); "
    "checked with five values or more."
)


# ── what a job's results are ────────────────────────────────────────────────


@dataclass
class WallTile:
    """One tilt series on a wall. `url` is the picture (served as a small JPEG); `before_url` its counterpart on a
    before/after wall; `overlays` (feature, mask URL) are tinted over it; `volume` and `slab` name the slab to render
    when the picture is not on disk yet. `value` is the caption's number, `flag` warn / bad, `why` its words."""

    ts: str
    label: str
    status: str = "ok"
    url: str | None = None
    before_url: str | None = None
    overlays: list[tuple[str, str, str]] = field(default_factory=list)  # (feature, mask URL, mask path)
    before_path: str | None = None  # the before picture on disk
    volume: str | None = None
    slab: str | None = None
    value: str = ""
    flag: str = ""
    why: str = ""
    note: str = ""  # what the frame says when it has no picture, and why
    dots: list[tuple[float, float]] = field(default_factory=list)  # picks, in % of the frame
    aspect: float | None = None  # this frame's width / height, when it must be its tomogram's (dots on it)
    tip: str = ""


@dataclass
class Wall:
    tiles: list[WallTile]
    value_title: str = ""  # what the caption's number is
    pair: tuple[str, str] | None = None  # a before/after wall's two words
    section: str | None = None  # the Journey's section for this job
    aspect: float | None = None  # the frames' width / height
    thumbs: bool = False  # the pictures are tilt thumbnails, not tomogram slices
    pass_missing: bool = False  # the pictures come from the job's preview pass, which has not run


@dataclass
class MapPic:
    """A volume's three central slices: `png` is where they are (made by `map_slices_render` when missing)."""

    volume: str
    png: str
    url: str | None
    label: str
    value: str = ""
    swatch: int | None = None  # a class's number: its colour
    tip: str = ""


@dataclass
class ResultPlot:
    """A value per tilt series (`dots` / `bars`, `labels` the tilt series), a histogram (`hist`, `labels` the bins'
    centres) or a curve (`curve`). `refs` are horizontal lines (value, words); `flags` index the marked values."""

    kind: str
    values: list[float]
    labels: list[str]
    y_title: str
    x_title: str = ""
    unit: str = ""
    flags: set[int] = field(default_factory=set)
    refs: list[tuple[float, str]] = field(default_factory=list)
    lo: float | None = None
    hi: float | None = None
    caption: str = ""  # the branch map's words under its small plot


@dataclass
class ResultTable:
    head: list[str]
    rows: list[list[str]]
    numeric: set[int] = field(default_factory=set)  # columns set right-aligned in the mono face


_ZERO = re.compile(r"[+\-−]?0(?:\.0+)?(?:\s*±\s*0(?:\.0+)?)?")


def telling(t: ResultTable) -> ResultTable | None:
    """The table without the columns that say nothing: every cell empty, zero, or (three rows or more) the same.
    None when only the tilt series and one more column would be left."""
    keep = [0]
    for i in range(1, len(t.head)):
        cells = [row[i] for row in t.rows]
        blank = all(c in ("—", "") or _ZERO.fullmatch(c.strip()) for c in cells)
        same = len(t.rows) >= 3 and len(set(cells)) == 1
        if not (blank or same):
            keep.append(i)
    if len(keep) <= 2:
        return None
    numeric = {k for k, i in enumerate(keep) if i in t.numeric}
    return ResultTable([t.head[i] for i in keep], [[row[i] for i in keep] for row in t.rows], numeric)


@dataclass
class JobResults:
    headline: str = ""
    warn: list[str] = field(default_factory=list)  # what stands out: outliers by name, failures, what was refused
    notes: list[str] = field(default_factory=list)  # what else there is to know
    tiles: list[tuple[str, str]] = field(default_factory=list)
    wall: Wall | None = None
    maps: list[MapPic] = field(default_factory=list)
    plot: ResultPlot | None = None
    table: ResultTable | None = None
    lines: list[str] = field(default_factory=list)  # what the branch map's card says besides (its old lines)

    @property
    def empty(self) -> bool:
        return not (
            self.headline or self.warn or self.notes or self.tiles or self.wall or self.maps or self.plot or self.table
        )


# ── helpers ──────────────────────────────────────────────────────────────────


def _median(vals: list[float]) -> float:
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def outliers(values: dict[str, float], *, worse: str) -> set[str]:
    """The keys whose value is more than 3σ worse than the median (σ from the MAD); `worse` high / low / both."""
    if len(values) < 5:
        return set()
    med = _median(list(values.values()))
    sigma = 1.4826 * _median([abs(v - med) for v in values.values()])
    if sigma <= 0:
        return set()
    if worse == "high":
        return {k for k, v in values.items() if v > med + 3 * sigma}
    if worse == "low":
        return {k for k, v in values.items() if v < med - 3 * sigma}
    return {k for k, v in values.items() if abs(v - med) > 3 * sigma}


def labels_for(names: list[str]) -> dict[str, str]:
    """Each tilt series by its house name (Pos 22 · Beam 2); where two of `names` would share one (a pooled set holds
    a Position_10 of every dataset), by what tells their datasets apart before it: the part of the name before the
    position, without what every dataset's name starts and ends with (20240204_Slot6 · Pos 10)."""
    pretty = {n: ts_pretty_name(n) for n in names}
    seen = Counter(pretty.values())
    clash = [n for n in names if seen[pretty[n]] > 1]
    if not clash:
        return pretty
    source = {n: n[: n.rfind("_Position_")] if "_Position_" in n else "" for n in clash}
    sources = sorted({v for v in source.values() if v})
    head = os.path.commonprefix(sources) if len(sources) > 1 else ""
    tail = os.path.commonprefix([x[::-1] for x in sources])[::-1] if len(sources) > 1 else ""
    head = head[: head.rfind("_") + 1]
    tail = tail[tail.find("_") :] if "_" in tail else ""
    out = dict(pretty)
    for n in clash:
        src = source[n]
        short = src[len(head) : len(src) - len(tail)] if src else ""
        out[n] = f"{short or src} · {pretty[n]}" if src else n
    return out


def count(n: int, word: str) -> str:
    """`n` and its noun, singular for one: 1 tomogram, 2,318 picks (a tilt series is the same either way)."""
    one = {"tomograms": "tomogram", "tilts": "tilt", "particles": "particle", "picks": "pick", "classes": "class"}
    return f"{n:,} {one.get(word, word) if n == 1 else word}"


def pct(share: float, digits: int = 0) -> str:
    """A share (0–1) as the house writes it: 37 %."""
    return f"{100 * share:.{digits}f} %"


def named(names: list[str], words: dict[str, str] | None = None, labels: dict[str, str] | None = None) -> str:
    """At most three tilt series by their house name, each with its words, then how many more."""
    labels = labels or labels_for(names)
    out = [labels.get(n, n) + (f" {words[n]}" if words and words.get(n) else "") for n in names[:NAMED]]
    more = len(names) - NAMED
    return ", ".join(out) + (f" +{more}" if more > 0 else "")


def words(v) -> str:
    """A parameter in words (an enum by its value)."""
    if isinstance(v, bool):
        return "yes" if v else "no"
    if hasattr(v, "value"):
        return str(v.value)
    if isinstance(v, float):
        return f"{v:g}"
    if isinstance(v, (list, tuple)):
        return ", ".join(str(x) for x in v) if v else "none"
    return "" if v is None else str(v)


def _url(path: Path | str | None, thumb: int = THUMB) -> str | None:
    if path is None:
        return None
    p = Path(path)
    return vis_asset_url(str(p), thumb=thumb) if p.is_file() else None


def _json(path: Path) -> dict:
    if not path.is_file():
        return {}
    with open(path) as f:
        return json.load(f)


def count_star_rows(star: Path) -> int:
    """The rows of a star file's last loop, without parsing it; 0 when it is not there."""
    if not star.exists():
        return 0
    n = 0
    in_loop = False
    with open(star, errors="replace") as f:
        for line in f:
            s = line.strip()
            if s.startswith("loop_"):
                in_loop = True
                n = 0
                continue
            if in_loop and s and not s.startswith("_"):
                n += 1
    return n


def _star(path: Path) -> dict:
    import starfile

    return starfile.read(path, always_dict=True)


def registry_dicts(series: list, names: set[str] | None = None) -> dict[str, dict]:
    """The registry's tilt series as plain dicts, the readers' input; only `names` when given. For a thread."""
    return {ts.id: ts.model_dump(mode="json") for ts in series if names is None or ts.id in names}


def _frames_out(reg: dict, ts: str, iid: str) -> list[tuple[dict, dict]]:
    """(frame, this job's output) for every frame of a tilt series that has one, in stage-angle order."""
    rows = [(fr, (fr.get("outputs") or {}).get(iid)) for fr in reg.get(ts, {}).get("frames", [])]
    return sorted([(fr, o) for fr, o in rows if o], key=lambda r: r[0].get("nominal_tilt_angle_deg") or 0.0)


def _ts_out(reg: dict, ts: str, iid: str) -> dict:
    return (reg.get(ts, {}).get("outputs") or {}).get(iid) or {}


def _tomo_out(reg: dict, ts: str, iid: str) -> dict:
    return ((reg.get(ts, {}).get("tomogram") or {}).get("outputs") or {}).get(iid) or {}


def _job_ts(reg: dict, iid: str, tasks: dict[str, str] | None) -> list[str]:
    """The tilt series this job ran on, in position order: its tasks, else those the registry holds its output for."""
    if tasks:
        return sort_ts_by_position(list(tasks))
    held = [
        t
        for t, d in reg.items()
        if iid in (d.get("outputs") or {})
        or iid in ((d.get("tomogram") or {}).get("outputs") or {})
        or any(iid in (fr.get("outputs") or {}) for fr in d.get("frames", []))
    ]
    return sort_ts_by_position(held)


def _status(tasks: dict[str, str] | None, ts: str) -> str:
    return (tasks or {}).get(ts, "ok")


def _failed_note(names: list[str], tasks: dict[str, str] | None) -> list[str]:
    bad = [t for t in names if _status(tasks, t) == "fail"]
    return [f"failed: {named(bad)}"] if bad else []


def _per_ts_plot(
    names: list[str], values: dict[str, float], title: str, unit: str, flags: set[str], *, kind: str = "dots", **kw
) -> ResultPlot | None:
    shown = [t for t in names if t in values]
    if not shown:
        return None
    vals = [values[t] for t in shown]
    med = _median(vals)
    labels = labels_for(shown)
    return ResultPlot(
        kind,
        vals,
        [labels[t] for t in shown],
        f"{title}, {unit}" if unit else title,
        "tilt series in position order",
        unit,
        flags={i for i, t in enumerate(shown) if t in flags},
        refs=kw.pop("refs", None) or [(med, f"median {med:.3g}")],
        **kw,
    )


def _mark(tiles: dict[str, WallTile], flags: set[str], why: str, flag: str = "warn") -> None:
    for t in flags:
        if t in tiles and not tiles[t].flag:
            tiles[t].flag, tiles[t].why = flag, why


def _wall_tiles(names: list[str], tasks: dict[str, str] | None) -> dict[str, WallTile]:
    out = {}
    labels = labels_for(names)
    for t in names:
        st = _status(tasks, t)
        tile = WallTile(t, labels[t], status=st, tip=t)
        if st == "fail":
            tile.flag, tile.why = "bad", "its task failed: the Tasks section has its log"
        out[t] = tile
    return out


def warp_png(mrc: Path | str | None) -> Path | None:
    """Warp's picture of a reconstruction: a PNG of the tomogram's stem beside it."""
    if not mrc:
        return None
    png = Path(mrc).with_suffix(".png")
    return png if png.is_file() else None


def _aspect(o: dict) -> float | None:
    x, y = o.get("size_x"), o.get("size_y")
    return min(2.5, max(0.5, x / y)) if x and y else None


# ── the maps ─────────────────────────────────────────────────────────────────


def slices_png(job_dir: Path, volume: Path) -> Path:
    return job_dir / "vis" / "results" / f"{volume.stem}_slices.png"


def map_slices_render(volume: Path | str, png: Path | str, project_path: Path | str | None = None) -> Render:
    """The three central slices of a volume (an average, a class map, a mask) side by side, in a worker."""
    from services.particles.class_readout import render_central_slices

    return Render(
        key=f"map-slices:{png}",
        title=f"Central slices · {Path(volume).name}",
        fn=render_central_slices,
        args=(Path(volume), Path(png)),
        outputs=(Path(png),),
        sources=(Path(volume),),
        kind=WorkKind.RENDER,
        quiet=True,
        project_path=str(project_path) if project_path else None,
    )


def _map(job_dir: Path, volume: Path, label: str, value: str = "", **kw) -> MapPic | None:
    if not volume.is_file():
        return None
    png = slices_png(job_dir, volume)
    fresh = png.is_file() and png.stat().st_mtime >= volume.stat().st_mtime
    return MapPic(str(volume), str(png), vis_asset_url(str(png)) if fresh else None, label, value, **kw)


# ── the readers ──────────────────────────────────────────────────────────────

# The reader of each job type, by the type's value; filled by `_reads`. A type without one has no results to show.
_READERS: dict[str, Callable] = {}


def _reads(job_type: JobType):
    def decorator(fn: Callable) -> Callable:
        _READERS[job_type.value] = fn
        return fn

    return decorator


def job_results(
    kind: str,
    jm,
    job_dir: Path | None,
    reg: dict[str, dict],
    iid: str,
    *,
    tasks: dict[str, str] | None = None,
    thumb_dir: Path | None = None,
    species_dims: bool = True,
) -> JobResults:
    """What `iid` (a job of `kind`) made, from what is on disk. `tasks`: its tasks' statuses by tilt series, when it
    is an array job (services/array_status_cache.py; read from its manifest when not given); `thumb_dir`: where the
    tilt thumbnails are. For a thread."""
    r = JobResults()
    if job_dir is None:
        return r
    reader = _READERS.get(kind)
    if reader is None:
        return r
    if tasks is None:
        # Not read from the status cache (the branch map's card): the job's own manifest and status files.
        items = manifest_items(job_dir)
        tasks = scan_statuses(job_dir, items) if items else None
    reader(r, jm, job_dir, reg, iid, tasks, thumb_dir)
    if r.table is not None:
        r.table = telling(r.table)
    return r


@_reads(JobType.IMPORT_MOVIES)
def _import_movies(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = sort_ts_by_position(list(reg))
    frames = [fr for t in names for fr in reg[t].get("frames", [])]
    if not names:
        return
    tilts = [fr.get("nominal_tilt_angle_deg") or 0.0 for fr in frames]
    dose = [fr["exposure_dose_e_per_a2"] for fr in frames if fr.get("exposure_dose_e_per_a2")]
    r.headline = (
        f"{len(names)} tilt series · {count(len(frames), 'tilts')} · {min(tilts):+.0f}° … {max(tilts):+.0f}°"
        + (f" · {_median(dose):.2f} e/Å² per tilt" if dose else "")
    )
    per = {t: float(len(reg[t].get("frames", []))) for t in names}
    few = sorted(outliers(per, worse="low"))
    if few:
        r.warn.append(f"fewer tilts than the rest: {named(few, {t: f'{per[t]:.0f}' for t in few})}")


@_reads(JobType.TS_IMPORT)
def _ts_import(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    n = len(list((job_dir / "tomostar").glob("*.tomostar")))
    r.headline = f"{n} tilt series as Warp tomostars · {words(getattr(jm, 'tomo_dimensions', ''))}"


@_reads(JobType.FS_MOTION_CTF)
def _fs_motion_ctf(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = _job_ts(reg, iid, tasks)
    tiles = _wall_tiles(names, tasks)
    fit: dict[str, float] = {}
    defocus, motion = [], []
    n_tilts = 0
    for t in names:
        rows = _frames_out(reg, t, iid)
        n_tilts += len(rows)
        res = [o["ctf_resolution"] for _, o in rows if o.get("ctf_resolution")]
        if res:
            fit[t] = _median(res)
            tiles[t].value = f"{fit[t]:.1f} Å"
        defocus += [((o.get("defocus_u_angstrom") or 0) + (o.get("defocus_v_angstrom") or 0)) / 2e4 for _, o in rows]
        motion += [o["mean_frame_movement"] for _, o in rows if o.get("mean_frame_movement")]
        if rows and thumb_dir is not None:
            fr, o = min(rows, key=lambda row: abs(row[0].get("nominal_tilt_angle_deg") or 0.0))
            png = thumb_dir / f"{Path(str(o.get('averaged_mrc') or fr.get('id'))).stem}.png"
            tiles[t].url = f"/api/tilt-thumb?path={quote(str(png), safe='')}" if png.is_file() else None
            tiles[t].tip = f"{t} · the tilt at {fr.get('nominal_tilt_angle_deg') or 0:+.0f}°"
    if not n_tilts:
        r.warn = _failed_note(names, tasks)
        return
    defocus = [d for d in defocus if d > 0]
    r.headline = f"{count(n_tilts, 'tilts')} in {len(fit) or len(names)} tilt series" + (
        f" · defocus {min(defocus):.1f}–{max(defocus):.1f} µm" if defocus else ""
    )
    if fit:
        r.headline += f" · CTF fit median {_median(list(fit.values())):.1f} Å"
    if motion:
        r.headline += f" · motion median {_median(motion):.2f}"
    worse = sorted(outliers(fit, worse="high"), key=lambda t: -fit[t])
    _mark(tiles, set(worse), f"CTF fit worse than the rest. {OUTLIER_RULE}")
    if worse:
        r.warn.append(f"CTF fit worse than the rest: {named(worse, {t: f'{fit[t]:.1f} Å' for t in worse})}")
    r.warn += _failed_note(names, tasks)
    r.wall = Wall(list(tiles.values()), "median CTF fit", section="fs_ctf", aspect=None, thumbs=True)
    r.plot = _per_ts_plot(
        names,
        fit,
        "CTF fit, median of the tilts",
        "Å",
        set(worse),
        lo=0,
        caption=f"CTF fit resolution per tilt series, Å · median {_median(list(fit.values())):.1f} Å" if fit else "",
    )


@_reads(JobType.TILT_FILTER)
def _tilt_filter(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = sort_ts_by_position(list(reg))
    per = [sum(1 for fr in reg[t].get("frames", []) if fr.get("is_filtered_out")) for t in names]
    tilts = sum(len(reg[t].get("frames", [])) for t in names)
    r.headline = f"{sum(per)} of {tilts:,} tilts taken out · threshold {words(getattr(jm, 'threshold', ''))}"
    r.plot = ResultPlot(
        "bars",
        [float(v) for v in per],
        [labels_for(names)[t] for t in names],
        "tilts taken out",
        caption=f"tilts taken out per tilt series · {sum(per)} of {tilts} · threshold "
        f"{words(getattr(jm, 'threshold', ''))}",
    )


@_reads(JobType.TS_ALIGNMENT)
def _ts_alignment(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = _job_ts(reg, iid, tasks)
    axis: dict[str, float] = {}
    shift: dict[str, float] = {}
    for t in names:
        pf = _ts_out(reg, t, iid).get("per_frame") or []
        if pf:
            axis[t] = _median([e.get("z_rot_deg") or 0.0 for e in pf])
            shift[t] = max(
                ((e.get("x_shift_angstrom") or 0) ** 2 + (e.get("y_shift_angstrom") or 0) ** 2) ** 0.5 for e in pf
            )
    method = words(getattr(jm, "alignment_method", "")) or "aligned"
    if axis:
        med = _median(list(axis.values()))
        far = max(shift, key=lambda t: shift[t])
        lo, hi = min(axis.values()), max(axis.values())
        spread = f" ({lo:.1f} … {hi:.1f})" if hi - lo >= 0.05 else ""
        r.headline = (
            f"{len(axis)} tilt series · {method} · tilt axis {med:.1f}°{spread} · largest shift {shift[far]:.0f} Å "
            f"({labels_for(names)[far]})"
        )
        off = sorted(outliers(axis, worse="both"), key=lambda t: -abs(axis[t] - med))
        if off:
            r.warn.append(f"tilt axis apart from the rest: {named(off, {t: f'{axis[t]:.1f}°' for t in off})}")
        if spread:
            r.plot = _per_ts_plot(
                names,
                axis,
                "tilt axis",
                "°",
                set(off),
                lo=lo - 1,
                hi=hi + 1,
                caption=f"tilt axis per tilt series, ° · {method}",
            )
        else:
            # One tilt axis for every tilt series: a flat plot says nothing, the shifts do.
            big = outliers(shift, worse="high")
            r.plot = _per_ts_plot(
                names, shift, "largest shift", "Å", big, lo=0, caption=f"largest shift per tilt series, Å · {method}"
            )
    r.warn += _failed_note(names, tasks)


@_reads(JobType.MISS_ALIGN)
def _miss_align(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = _job_ts(reg, iid, tasks)
    d = _json(job_dir / "missalign_changes.json")
    per = d.get("tilt_series") or {}
    rms: dict[str, float] = {}
    over: dict[str, int] = {}
    threshold = None
    for t in names:
        rec = per.get(t)
        if not rec:
            continue
        rms[t] = rec.get("residual_rms_angstrom") or 0.0
        over[t] = len(rec.get("tilts_over_threshold") or [])
        threshold = rec.get("threshold_angstrom") or threshold
    if rms:
        n_over = [t for t in names if over.get(t)]
        r.headline = f"{len(rms)} tilt series refined · residual median {_median(list(rms.values())):.1f} Å rms" + (
            f" · {len(n_over)} with tilts over {threshold:g} Å" if threshold else ""
        )
        # The job's threshold marks tilts, not tilt series: named only while they are a few; the ones far above the
        # rest by the outlier rule are named always.
        if n_over and len(n_over) <= len(rms) / 3:
            r.warn.append(f"tilts over the threshold: {named(n_over, {t: f'({over[t]})' for t in n_over})}")
        far = sorted(outliers(rms, worse="high"), key=lambda t: -rms[t])
        if far:
            r.warn.append(f"residual far above the rest: {named(far, {t: f'{rms[t]:.0f} Å' for t in far})}")
        r.plot = _per_ts_plot(
            names,
            rms,
            "residual after refinement",
            "Å rms",
            set(far),
            kind="bars",
            lo=0,
            caption=f"residual per tilt series after refinement, Å rms · {d.get('threshold', '')}",
        )
    r.warn += _failed_note(names, tasks)


@_reads(JobType.TS_CTF)
def _ts_ctf(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = _job_ts(reg, iid, tasks)
    fit: dict[str, float] = {}
    defocus: list[float] = []
    inverted: list[str] = []
    for t in names:
        o = _ts_out(reg, t, iid)
        if not o:
            continue
        if o.get("ctf_resolution"):
            fit[t] = o["ctf_resolution"]
        defocus += [
            ((e.get("defocus_u_angstrom") or 0) + (e.get("defocus_v_angstrom") or 0)) / 2e4
            for e in o.get("per_frame") or []
        ]
        if o.get("are_angles_inverted"):
            inverted.append(t)
    defocus = [v for v in defocus if v > 0]
    if fit or defocus:
        r.headline = f"{len(fit) or len(names)} tilt series" + (
            f" · defocus {min(defocus):.1f}–{max(defocus):.1f} µm" if defocus else ""
        )
        if fit:
            r.headline += f" · CTF fit median {_median(list(fit.values())):.1f} Å"
        fitted = [t for t in names if _ts_out(reg, t, iid)]
        if inverted and len(inverted) == len(fitted):
            r.headline += " · hand inverted"
        elif inverted:
            r.warn.append(f"hand inverted on {len(inverted)} of {len(fitted)}: {named(inverted)}")
        worse = sorted(outliers(fit, worse="high"), key=lambda t: -fit[t])
        if worse:
            r.warn.append(f"CTF fit worse than the rest: {named(worse, {t: f'{fit[t]:.1f} Å' for t in worse})}")
        r.plot = _per_ts_plot(
            names,
            fit,
            "CTF fit",
            "Å",
            set(worse),
            lo=0,
            caption=f"CTF fit resolution per tilt series, Å · median {_median(list(fit.values())):.1f} Å"
            if fit
            else "",
        )
    r.warn += _failed_note(names, tasks)


@_reads(JobType.TS_RECONSTRUCT)
def _ts_reconstruct(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = _job_ts(reg, iid, tasks)
    tiles = _wall_tiles(names, tasks)
    first: dict = {}
    for t in names:
        o = _tomo_out(reg, t, iid)
        if not o:
            continue
        first = first or o
        mrc = o.get("reconstructed_mrc")
        png = warp_png(mrc)
        tiles[t].url = _url(png)
        if png is None and mrc and Path(mrc).is_file():
            tiles[t].volume, tiles[t].slab = mrc, str(job_dir / "vis" / "slabs" / f"{t}_xy.png")
            tiles[t].url = _url(tiles[t].slab)
    done = sum(1 for t in names if _tomo_out(reg, t, iid))
    if first:
        r.headline = (
            f"{count(done, 'tomograms')} · {first.get('tomogram_pixel_size_angstrom', 0):g} Å/px · "
            f"{first.get('size_x')} × {first.get('size_y')} × {first.get('size_z')}"
            + (" · deconvolved" if getattr(jm, "deconv", 0) else "")
        )
    r.warn += _failed_note(names, tasks)
    if names:
        r.wall = Wall(list(tiles.values()), section="reconstruct", aspect=_aspect(first))


@_reads(JobType.DENOISE_TRAIN)
def _denoise_train(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    vals = _loss_curve(job_dir)
    method = words(getattr(jm, "denoise_method", ""))
    if vals:
        # A batch's loss is noisy: the start and the end are each an eighth of the log, averaged.
        k = max(1, len(vals) // 8)
        start, end = sum(vals[:k]) / k, sum(vals[-k:]) / k
        r.headline = f"{method} model trained · loss {start:.3g} → {end:.3g} (first and last eighth of the log)"
        r.plot = ResultPlot(
            "curve",
            vals,
            [str(i + 1) for i in range(len(vals))],
            "training loss",
            "over the log",
            caption=f"training loss over the log · {method}",
        )
    model = job_dir / "denoising_model.tar.gz"
    if model.is_file():
        r.notes.append(f"{model.name} · {model.stat().st_size / 1e6:.0f} MB")


def _source_picture(reg: dict, ts: str, mrc: str | None) -> Path | None:
    """The picture of the volume a job read for one tilt series: Warp's PNG of a reconstruction, or the slab the
    wall rendered of a denoised volume; None when neither is on disk."""
    if not mrc:
        return None
    png = warp_png(mrc)
    if png is not None:
        return png
    for o in ((reg.get(ts, {}).get("tomogram") or {}).get("outputs") or {}).values():
        if o.get("denoised_mrc") == mrc and o.get("job_dir"):
            slab = Path(o["job_dir"]) / "vis" / "slabs" / f"{ts}_xy.png"
            return slab if slab.is_file() else None
    return None


@_reads(JobType.DENOISE_PREDICT)
def _denoise_predict(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = _job_ts(reg, iid, tasks)
    tiles = _wall_tiles(names, tasks)
    first_recon: dict = {}
    for t in names:
        o = _tomo_out(reg, t, iid)
        if not o:
            continue
        slab = job_dir / "vis" / "slabs" / f"{t}_xy.png"
        tiles[t].url = _url(slab)
        tiles[t].volume, tiles[t].slab = o.get("denoised_mrc"), str(slab)
        recon = next(
            (v for v in ((reg[t].get("tomogram") or {}).get("outputs") or {}).values() if v.get("reconstructed_mrc")),
            {},
        )
        first_recon = first_recon or recon
        tiles[t].before_url = _url(warp_png(recon.get("reconstructed_mrc")))
    done = sum(1 for t in names if _tomo_out(reg, t, iid))
    # The method each volume was made with, as the registry recorded it, before the parameter's.
    ran = Counter(_tomo_out(reg, t, iid).get("denoise_method") for t in names if _tomo_out(reg, t, iid))
    ran.pop(None, None)
    method = " + ".join(m for m, _n in ran.most_common()) or words(getattr(jm, "denoise_method", ""))
    r.headline = f"{count(done, 'tomograms')} denoised · {method}" if done else ""
    r.warn += _failed_note(names, tasks)
    if names:
        r.wall = Wall(
            list(tiles.values()), pair=("before", "after"), section="reconstruct", aspect=_aspect(first_recon)
        )


@_reads(JobType.SEGMENT)
def _segment(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = _job_ts(reg, iid, tasks)
    tiles = _wall_tiles(names, tasks)
    share: dict[str, dict[str, float]] = {}
    first_feature = None
    shape = None
    for t in names:
        o = _tomo_out(reg, t, iid)
        if not o:
            continue
        vols = o.get("volumes") or {}
        shape = shape or o.get("shape_zyx")
        for feat, v in vols.items():
            first_feature = first_feature or feat
            if v.get("fraction_pct") is not None:
                share.setdefault(feat, {})[t] = float(v["fraction_pct"])
            if v.get("overlay_xy"):
                url = _url(v["overlay_xy"], thumb=0)
                if url:
                    tiles[t].overlays.append((feat, url, str(v["overlay_xy"])))
        src = o.get("source_mrc")
        base = _source_picture(reg, t, src)
        if base is None and src and Path(src).is_file():
            base = job_dir / "vis" / "slabs" / f"{t}_xy.png"
            tiles[t].volume, tiles[t].slab = src, str(base)
        tiles[t].url = tiles[t].before_url = _url(base)
        tiles[t].before_path = str(base) if base is not None and base.is_file() else None
        if first_feature and t in share.get(first_feature, {}):
            tiles[t].value = f"{share[first_feature][t]:.1f} %"
    done = sum(1 for t in names if _tomo_out(reg, t, iid))
    if done:
        medians = sorted(((_median(list(v.values())), feat) for feat, v in share.items()), reverse=True)
        parts = [f"{feat} {m:.1f} %" for m, feat in medians[:5]] + (
            [f"+{len(medians) - 5}"] if len(medians) > 5 else []
        )
        r.headline = f"{count(done, 'tomograms')} segmented" + (
            " · median share of the volume: " + ", ".join(parts) if parts else ""
        )
    r.warn += _failed_note(names, tasks)
    if names:
        aspect = min(2.5, max(0.5, shape[2] / shape[1])) if shape and shape[1] else None
        r.wall = Wall(
            list(tiles.values()),
            f"{first_feature}, % of the volume" if first_feature else "",
            pair=("tomogram", "segmented"),
            section="reconstruct",
            aspect=aspect,
        )


@_reads(JobType.TEMPLATE_MATCH_PYTOM)
def _template_match(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = _job_ts(reg, iid, tasks)
    top: dict[str, float] = {}
    fp1: dict[str, float] = {}
    for t in names:
        d = _json(job_dir / "analytics" / "tm_tomo" / f"{t}.json").get("data", {})
        scan = d.get("scan") or {}
        if scan.get("top_sigma") is not None:
            top[t] = float(scan["top_sigma"])
        if d.get("fp1_sigma"):
            fp1[t] = float(d["fp1_sigma"])
    if top:
        cut = _median(list(fp1.values())) if fp1 else None
        r.headline = (
            f"{count(len(top), 'tomograms')} matched · best peak median {_median(list(top.values())):.1f} σ"
            + (f" · one false positive at {cut:.1f} σ" if cut else "")
        )
        r.headline += f" · {words(getattr(jm, 'angular_search', ''))}° search"
        under = [t for t in names if t in top and t in fp1 and top[t] < fp1[t]]
        if under:
            r.warn.append(f"best peak under one false positive: {named(under, {t: f'{top[t]:.1f} σ' for t in under})}")
        r.plot = _per_ts_plot(
            names,
            top,
            "best peak",
            "σ",
            set(under),
            lo=0,
            hi=max(top.values()) * 1.1,
            refs=[(cut, f"one false positive {cut:.1f} σ")] if cut else None,
            caption=f"best peak per tomogram, σ · the line is one false positive · "
            f"{words(getattr(jm, 'angular_search', ''))}° search",
        )
    r.warn += _failed_note(names, tasks)


def _normalized_dots(picks: list, dims) -> list[tuple[float, float]]:
    """Picks as (x %, y %) of an X/Y slab of the tomogram, Y flipped as the pick canvas draws them; none without the
    tomogram's extent."""
    if not dims or len(dims) < 2:
        return []
    x_dim, y_dim = max(int(dims[0]), 1), max(int(dims[1]), 1)
    step = max(1, len(picks) // 260)
    out = []
    for p in picks[::step]:
        try:
            fx = min(max(float(p.get("x", 0)) / x_dim, 0.0), 1.0)
            fy = 1.0 - min(max(float(p.get("y", 0)) / y_dim, 0.0), 1.0)
        except (TypeError, ValueError):
            continue
        out.append((fx * 100.0, fy * 100.0))
    return out


@_reads(JobType.TEMPLATE_EXTRACT_PYTOM)
def _candidate_extract(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    names = _job_ts(reg, iid, tasks)
    tiles = _wall_tiles(names, tasks)
    d = _json(job_dir / "analytics" / "candidates.json").get("data", {})
    by_tomo = {t.get("tomo"): t for t in d.get("tomograms") or []}
    prev = job_dir / "vis" / "preview"
    missing = 0
    aspect = None
    for t in names:
        rec = by_tomo.get(t)
        if rec is not None:
            tiles[t].value = f"{rec.get('n', 0):,}"
            if not rec.get("n"):
                tiles[t].flag, tiles[t].why = "warn", "no picks on this tomogram"
                tiles[t].note = "no picks"  # the preview pass draws nothing for a tomogram without picks
        png = prev / t / "xy_slab_preview.png"
        tiles[t].url = _url(png)
        picks = _json(prev / t / "picks.json") if png.is_file() else {}
        dims = picks.get("tomo_dims_xyz_px")
        if dims and dims[1]:
            tiles[t].aspect = dims[0] / dims[1]
            aspect = aspect or min(2.5, max(0.5, tiles[t].aspect))
        tiles[t].dots = _normalized_dots(picks.get("picks") or [], dims)
        if tiles[t].url is None and tiles[t].status == "ok" and not tiles[t].note:
            missing += 1
    toms = list(by_tomo.values())
    scores = sorted(s for t in toms for s in (t.get("scores") or []))
    counts = [t.get("n", 0) for t in toms]
    if toms:
        picks = count(d.get("n_total", sum(counts)), "picks")
        tomos = count(d.get("n_tomograms") or 0, "tomograms")
        r.headline = f"{picks} in {d.get('n_with_picks')} of {tomos}" + (
            f" · σ {scores[0]:.1f}–{scores[-1]:.1f}" if scores else ""
        )
        if counts:
            r.headline += f" · median {_median([float(c) for c in counts]):.0f} per tomogram"
        none = [t for t in names if t in by_tomo and not by_tomo[t].get("n")]
        if none:
            r.warn.append(f"no picks: {named(none)}")
    if scores:
        lo, hi = scores[0], scores[-1]
        bins = 32
        width = (hi - lo) / bins or 1
        hist = [0.0] * bins
        for s in scores:
            hist[min(bins - 1, int((s - lo) / width))] += 1
        cut = min((t.get("cutoff_sigma") for t in toms if t.get("cutoff_sigma")), default=None)
        r.plot = ResultPlot(
            "hist",
            hist,
            [f"{lo + width * (i + 0.5):.1f}" for i in range(bins)],
            "picks",
            "score, σ",
            "σ",
            refs=[],
            lo=lo,
            hi=hi,
            caption=f"scores of all {d.get('n_total', len(scores))} picks, σ {lo:.1f}–{hi:.1f} · cut at one false "
            f"positive · {d.get('n_with_picks')} of {d.get('n_tomograms')} tomograms with picks",
        )
        if cut is not None:
            r.plot.refs = [(cut, f"cut {cut:.1f} σ")]
    r.warn += _failed_note(names, tasks)
    if names:
        r.wall = Wall(list(tiles.values()), "picks", section="particles", aspect=aspect, pass_missing=missing > 0)


@_reads(JobType.SUBTOMO_EXTRACTION)
def _subtomo_extraction(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    d = _json(job_dir / "analytics" / "subtomo.json").get("data", {})
    n = d.get("n_out")
    if n is None:
        n = count_star_rows(job_dir / "particles.star")
        if n:
            r.headline = f"{count(n, 'particles')} extracted"
        return
    crop = int(getattr(jm, "crop_size", -1) or -1)
    box = crop if crop > 0 else int(getattr(jm, "box_size", 0) or 0)
    toms = d.get("tomograms") or []
    r.headline = (
        f"{n:,} of {count(d['n_in'], 'particles')} extracted" if d.get("n_in") else f"{count(n, 'particles')} extracted"
    ) + f" · box {box} · bin {getattr(jm, 'binning', 0):g} · {count(len(toms), 'tomograms')}"
    lost = sorted((t for t in toms if t.get("lost")), key=lambda t: -t["lost"])
    if lost:
        r.notes.append(
            f"{sum(t['lost'] for t in lost):,} fell outside the tilt images, most on "
            + named(
                [t["tomo"] for t in lost],
                {t["tomo"]: f"({t['lost']})" for t in lost},
                labels_for([t["tomo"] for t in toms]),
            )
        )


@_reads(JobType.WARP_EXPORT)
def _warp_export(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    n = count_star_rows(job_dir / "particles.star")
    n_ts = len([p for p in (job_dir / "particleseries").glob("*") if p.is_dir()])
    r.headline = (
        f"{count(n, 'particles')} exported · box {words(getattr(jm, 'box_size', ''))} · "
        f"bin {getattr(jm, 'binning', 0):g} · Ø {getattr(jm, 'diameter_ang', 0):g} Å"
        + (f" · {n_ts} tilt series" if n_ts else "")
    )
    r.notes = _warp_export_model_lines(job_dir)
    r.warn += _failed_note(_job_ts(reg, iid, tasks), tasks)


def _warp_export_model_lines(job_dir: Path) -> list[str]:
    """Whose Warp model the stacks were cut with, when it is an M refine's, and what of it RELION's star leaves
    out, from the driver's `warp_export_model.json`."""
    d = _json(job_dir / "warp_export_model.json")
    m_jobs = d.get("m_jobs") or {}
    if not m_jobs:
        return []
    lines = [
        f"cut with M's model: {sum(m_jobs.values())} tilt series from "
        + ", ".join(f"{Path(j).name} ({n})" for j, n in sorted(m_jobs.items()))
        + " · M's per-tilt defocus and Cs in the set"
    ]
    mag, zern = d.get("magnified") or [], d.get("zernike") or []
    if mag or zern:
        lines.append(
            "left out (Warp applies them only where it back-projects; RELION's star holds neither): "
            + " · ".join(
                part
                for part in (
                    f"M's magnification on {len(mag)} tilt series" if mag else "",
                    f"its Zernike terms on {len(zern)}" if zern else "",
                )
                if part
            )
        )
    return lines


@_reads(JobType.RECONSTRUCT_PARTICLE)
def _reconstruct_particle(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    fsc = (_json(job_dir / "analytics" / "reconstruct.json").get("data") or {}).get("fsc") or {}
    res = ((fsc.get("unmasked") or {}).get("resolution") or {}).get("0.143")
    r.headline = f"box {words(getattr(jm, 'box_size', ''))} · {words(getattr(jm, 'symmetry', ''))} · bin " + (
        f"{getattr(jm, 'binning', 0):g}" + (f" · FSC 0.143 at {float(res):.1f} Å (unmasked half maps)" if res else "")
    )
    m = _map(job_dir, job_dir / "merged.mrc", "merged.mrc", "the average")
    if m is not None:
        r.maps.append(m)


@_reads(JobType.CLASS3D)
def _class3d(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    """The last iteration's classes: share and Class 3D's own resolution estimate (not a gold-standard FSC)."""
    from services.particles.class_readout import _model_classes, class3d_iterations

    its = class3d_iterations(job_dir)
    if not its:
        r.headline = "no iteration written yet"
        return
    final = its[-1]
    classes = _model_classes(job_dir / f"run_it{final:03d}_model.star")
    n = count_star_rows(job_dir / f"run_it{final:03d}_data.star")
    big = max(classes, key=lambda c: c["distribution"], default=None)
    r.headline = f"{count(len(classes), 'classes')} · iteration {final}" + (f" · {count(n, 'particles')}" if n else "")
    if big is not None:
        r.headline += f" · the largest, class {big['class']}, holds {pct(big['distribution'])}"
    for c in classes:
        est = (
            f" · {c['resolution_ang']:.1f} Å est."
            if c.get("resolution_ang") and math.isfinite(c["resolution_ang"])
            else ""
        )
        m = _map(
            job_dir,
            job_dir / f"run_it{final:03d}_class{c['class']:03d}.mrc",
            f"class {c['class']}",
            f"{pct(c['distribution'], 1)}{est}",
            swatch=int(c["class"]),
            tip="Class 3D's own estimate is a guide to which classes hold structure, never a resolution to report.",
        )
        if m is not None:
            r.maps.append(m)
        r.lines.append(f"class {c['class']} · {pct(c['distribution'])}{est}")


_ITERATION_MODEL_RE = re.compile(r"(run(?:_ct\d+)?_it(\d{3}))_half1_model\.star")


def _last_iteration(job_dir: Path) -> str | None:
    """The file prefix of the newest iteration relion_refine wrote: by number, never by name (run_it006 sorts after
    run_ct6_it016); of one number, the newest file, since a continuation writes the iteration it starts from again."""
    found = [
        (int(m.group(2)), p.stat().st_mtime, m.group(1))
        for p in job_dir.glob("run*_it*_half1_model.star")
        if (m := _ITERATION_MODEL_RE.fullmatch(p.name))
    ]
    return max(found)[2] if found else None


@_reads(JobType.REFINE3D)
def _refine3d(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    """The converged run's gold-standard FSC of the unmasked half maps, else the last iteration's resolution."""
    model = job_dir / "run_model.star"
    converged = model.exists()
    last = None
    if not converged:
        last = _last_iteration(job_dir)
        if last is None:
            r.headline = "no iteration written yet"
            return
        model = job_dir / f"{last}_half1_model.star"
    general = _star(model).get("model_general")
    res = general.get("rlnCurrentResolution") if isinstance(general, dict) else None
    ended = None if converged else stop_after_iteration.finished(job_dir)
    if converged:
        where = "converged"
    elif ended:
        where = f"ended at iteration {ended['iteration']}, not converged"
    else:
        where = f"so far, {last}"
    r.headline = f"{float(res):.2f} Å (gold-standard FSC, unmasked half maps) · {where}" if res else where
    volume = job_dir / "run_class001.mrc"
    if not volume.is_file() and last is not None:
        volume = job_dir / f"{last}_half1_class001.mrc"
    m = _map(job_dir, volume, volume.name, "the refined map")
    if m is not None:
        r.maps.append(m)


@_reads(JobType.MASK_CREATE)
def _mask_create(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    st = _json(job_dir / "mask_stats.json")
    if st:
        r.headline = (
            f"encloses {pct(st['enclosed_fraction'], 1)} of the box · threshold {st['threshold']:.4g} "
            f"({st['threshold_source']}) on the map low-passed at {st['lowpass_ang']:g} Å"
        )
        r.notes.append(f"grown {st['extend_px']} px, soft edge {st['soft_edge_px']} px")
        inner, outer = st.get("inner_radius_ang") or 0, st.get("outer_radius_ang") or 0
        if inner or outer:
            r.notes.append(
                f"kept between {inner:g} and {outer:g} Å from the centre" if outer else f"kept beyond {inner:g} Å"
            )
    for name, label in (("map_lowpass.mrc", "the low-passed map"), ("mask.mrc", "the mask")):
        m = _map(job_dir, job_dir / name, name, label)
        if m is not None:
            r.maps.append(m)


@_reads(JobType.POSTPROCESS)
def _postprocess(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    star = job_dir / "postprocess.star"
    if not star.exists():
        return
    general = _star(star).get("general")
    if isinstance(general, dict) and "rlnFinalResolution" in general:
        bfac = general.get("rlnBfactorUsedForSharpening")
        r.headline = f"{float(general['rlnFinalResolution']):.2f} Å (masked gold-standard FSC, 0.143)" + (
            f" · B {float(bfac):g} Å²" if bfac is not None else ""
        )
    m = _map(job_dir, job_dir / "postprocess.mrc", "postprocess.mrc", "the sharpened map")
    if m is not None:
        r.maps.append(m)


@_reads(JobType.CTF_REFINE)
def _ctf_refine(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    """What moved, per tilt series: the defocus (mean of U and V) and the astigmatism, medians over the tilts, from
    the driver's comparison of the refined tilt-series stars with those the job read."""
    d = _json(job_dir / "ctf_changes.json")
    series = d.get("tilt_series") or []
    if not d:
        return
    if not series:
        r.headline = "no tilt series compared"
        return
    worst = max(series, key=lambda s: abs(s["largest_move_um"]))
    labels = labels_for([s["name"] for s in series])
    n_tilts = sum(len(s["tilts"]) for s in series)
    moves = sorted(abs(t["mean_move_ang"]) / 1e4 for s in series for t in s["tilts"])
    r.tiles = [
        ("Moved", f"{_median(moves):.2f} µm"),
        ("Largest", f"{worst['largest_move_um']:+.2f} µm"),
        ("Box", f"{d.get('box')} px"),
    ]
    r.headline = (
        f"{len(series)} tilt series · {count(n_tilts, 'tilts')} · defocus moved median {_median(moves):.3f} µm, "
        f"largest {worst['largest_move_um']:+.3f} µm ({labels[worst['name']]}, {worst['largest_move_angle']:+.0f}°)"
    )
    if d.get("flagged"):
        r.warn.append(
            f"{len(d['flagged'])} tilts failed the checks (defocus ≤ 0, a move over {d.get('limit_um')} µm or a "
            "negative contrast scale)"
        )
    by_name = {s["name"]: s for s in series}
    rows = []
    for name in sort_ts_by_position(list(by_name)):
        s = by_name[name]
        scale = s.get("scale_range")
        rows.append(
            [
                labels[name],
                str(len(s["tilts"])),
                f"{s['mean_defocus_before_um']:.2f} → {s['mean_defocus_after_um']:.2f}",
                f"{s['median_move_um']:.2f}",
                f"{s['largest_move_um']:+.2f}",
                f"{s['astig_before_um']:.2f} → {s['astig_after_um']:.2f}",
                f"{scale[0]:.2f}–{scale[1]:.2f}" if scale else "—",
            ]
        )
    r.table = ResultTable(
        ["tilt series", "tilts", "defocus µm", "moved µm", "largest µm", "astigmatism µm", "scale"],
        rows,
        {1, 2, 3, 4, 5, 6},
    )


@_reads(JobType.FRAME_ALIGN)
def _frame_align(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    """What moved, per tilt series, against the model it came with: the tilt shifts and angles RELION refined, the
    particles' positions and their motion, from the driver's `frame_align_changes.json`."""
    d = _json(job_dir / "frame_align_changes.json")
    if not d:
        return
    series = d.get("tilt_series") or []
    refined = [s for s in series if not s.get("kept")]
    if not refined:
        r.headline = "no tilt series refined"
        return
    worst = max(refined, key=lambda s: s["shift_max_ang"])
    motion = [s["motion_rms_ang"] for s in refined if s.get("motion_rms_ang") is not None]
    r.tiles = [
        ("Refined", f"{len(refined)} of {len(series)}"),
        ("Largest shift", f"{worst['shift_max_ang']:.0f} Å"),
        ("Box", f"{d.get('box')} px"),
    ]
    if motion:
        r.tiles.insert(1, ("Motion", f"{max(motion):.1f} Å"))
    kept = [s for s in series if s.get("kept")]
    r.headline = f"{len(refined)} tilt series refined ({count(sum(s['particles'] for s in refined), 'particles')})" + (
        f", {len(kept)} kept as they came ({sum(s['particles'] for s in kept):,} particles, under "
        f"{d.get('min_particles')} each)"
        if kept
        else ""
    )
    r.notes.append("the set's particles name no images: Subtomo Extraction cuts them with this model")
    order = sort_ts_by_position([s["name"] for s in refined])
    labels = labels_for(order)
    by_name = {s["name"]: s for s in refined}
    rows = []
    for name in order:
        s = by_name[name]
        came = s.get("trajectories_in_rms_ang")
        rows.append(
            [
                labels[name],
                f"{s['particles']:,}",
                f"{s['shift_rms_ang']:.1f}",
                f"{s['shift_max_ang']:.1f}",
                f"{s['angle_max_deg']:.2f}",
                f"{s['position_rms_ang']:.1f}",
                f"{s['motion_rms_ang']:.1f}" if s.get("motion_rms_ang") is not None else "—",
                f"{came:.1f}" if came is not None else "—",
            ]
        )
    r.table = ResultTable(
        [
            "tilt series",
            "particles",
            "shift rms Å",
            "largest Å",
            "angles ≤ °",
            "positions Å",
            "motion Å",
            "came with Å",
        ],
        rows,
        {1, 2, 3, 4, 5, 6, 7},
    )


@_reads(JobType.M_REFINE)
def _m_refine(r, jm, job_dir, reg, iid, tasks, thumb_dir) -> None:
    """M's resolution before and after the round, what it moved per tilt series, and what of the model RELION's
    tilt-series star cannot hold, from the driver's `m_round.json` and `m_changes.json`."""
    rec = _json(job_dir / "m_round.json")
    if not rec:
        return
    series = _json(job_dir / "m_changes.json").get("tilt_series", [])

    def ang(v) -> str:
        return f"{v:.2f} Å" if isinstance(v, (int, float)) else "—"

    every = rec.get("all_species") or {}
    own = rec.get("job_species") or rec.get("species_name")
    r.tiles = [
        ("Round", f"R{rec.get('round')}"),
        ("M", ang(rec.get("resolution_after_ang"))),
        ("Particles", f"{rec.get('particles'):,}" if isinstance(rec.get("particles"), int) else "—"),
        ("Box", f"{rec.get('m_box')} → {rec.get('box')} px"),
    ]
    if len(every) > 1:
        r.tiles.append(("Species", str(len(every))))
    if rec.get("hand_back_of"):
        r.headline = (
            f"{own} handed back as round {rec.get('round')} left it, M at {ang(rec.get('resolution_after_ang'))} · "
            f"no M round ran ({Path(rec['hand_back_of']).name})"
        )
    else:
        r.headline = (
            f"M's own resolution {ang(rec.get('resolution_before_ang'))} → {ang(rec.get('resolution_after_ang'))} · "
            f"{rec.get('tilt_series')} tilt series in {rec.get('sources')} sources · "
            f"{rec.get('seconds', 0) / 60:.0f} min on {rec.get('gpus')} GPU"
        )
    r.notes.append("the half maps are in the Refine 3D's box: a PostProcess reads them in the trunk's masks")
    if len(every) > 1:
        for name in sorted(every, key=lambda n: (n != own, n)):
            e = every[name]
            count = e.get("particles")
            added = " · added this round" if e.get("added_round") == rec.get("round") and rec.get("round") else ""
            where = "this job's outputs" if name == own else f"species/{name}/"
            r.notes.append(
                f"{name}: {count:,} particles, M {ang(e.get('resolution_before_ang'))} → "
                f"{ang(e.get('resolution_after_ang'))}{added} ({where})"
                if isinstance(count, int)
                else f"{name}: M {ang(e.get('resolution_after_ang'))} ({where})"
            )
    for x in rec.get("expansions") or []:
        r.notes.append(
            f"{x.get('from')} expanded in round {x.get('round')}: {x.get('parents', 0):,} particles → "
            f"{x.get('copies', 0):,} copies ({x.get('per_parent')} each), the species now {x.get('to')}; each copy "
            "names its parent (cbParentUid, cbSubIndex)"
        )
    if (rec.get("temporal_samples") or 1) > 1:
        r.notes.append(
            f"M holds {rec['temporal_samples']} temporal samples of each particle's pose; the set carries their mean"
        )
    gate = rec.get("ctf_gate") or {}
    if gate.get("skipped"):
        r.warn.append(
            f"the CTF was not refined: M's species was at {ang(gate.get('species_resolution_ang'))}, coarser than "
            f"'CTF from' {gate.get('min_resolution_ang'):g} Å ({', '.join(gate.get('switches') or [])} did nothing)"
        )
    elif gate.get("not_fitted"):
        r.notes.append(
            f"the CTF was fitted from {', '.join(gate.get('fitted_on') or [])}; {', '.join(gate['not_fitted'])} "
            f"coarser than 'CTF from' {gate.get('min_resolution_ang'):g} Å took no part"
        )
    beyond = rec.get("beyond_star") or {}
    staged, mag = beyond.get("stage_angles") or {}, beyond.get("magnification") or {}
    if staged.get("tilt_series") or mag.get("tilt_series"):
        held = []
        if staged.get("tilt_series"):
            held.append(f"stage angles on {staged['tilt_series']} tilt series (up to {staged.get('max_deg', 0):.2f}°)")
        if mag.get("tilt_series"):
            held.append(
                f"a magnification on {mag['tilt_series']} (up to {mag.get('max_anisotropic_pct', 0):.2f} % anisotropic)"
            )
        consequences = []
        if staged.get("tilt_series"):
            consequences.append("Warp Export and RELION's tilt-image programs refuse the stage angles")
        if mag.get("tilt_series"):
            consequences.append("RELION reconstructs without the magnification")
        r.notes.append(f"RELION's tilt-series star cannot hold {' and '.join(held)}: {'; '.join(consequences)}")
    by_name = {s["name"]: s for s in series}
    labels = labels_for(list(by_name))
    rows = []
    for name in sort_ts_by_position(list(by_name)):
        s = by_name[name]
        scale = s.get("magnification_pct") or {}
        has_mag = abs(scale.get("anisotropic_pct", 0)) > 1e-4 or abs(scale.get("isotropic_pct", 0)) > 1e-4
        cs_moved = s.get("cs_after_mm") is not None and s.get("cs_after_mm") != s.get("cs_before_mm")
        rows.append(
            [
                labels[name],
                f"{s['particles']:,}",
                f"{s['defocus_shift_um']:+.3f} ± {s['defocus_shift_sd_um']:.3f}" if "defocus_shift_um" in s else "—",
                f"{s['warp_change_rms_ang']:.1f}" if s.get("warp_change_rms_ang") is not None else "—",
                f"{s['stage_angle_max_deg']:.2f}" if s.get("stage_angle_max_deg") else "—",
                f"{scale['x_pct']:+.2f} / {scale['y_pct']:+.2f}" if has_mag else "—",
                f"{s['cs_after_mm']:.2f}" if cs_moved else "—",
                f"{s['moved_rms_ang']:.1f}" if s.get("moved_rms_ang") is not None else "—",
                f"{s['turned_rms_deg']:.2f}" if s.get("turned_rms_deg") is not None else "—",
            ]
        )
    if rows:
        r.table = ResultTable(
            [
                "tilt series",
                "particles",
                "defocus µm",
                "warp Å",
                "stage ≤ °",
                "mag x / y %",
                "Cs mm",
                "moved Å",
                "turned °",
            ],
            rows,
            {1, 2, 3, 4, 5, 6, 7, 8},
        )
        if telling(r.table) is None:
            r.notes.append(f"M moved nothing in this round (R{rec.get('round')}): no particle, tilt or defocus changed")
    m = _map(job_dir, job_dir / "m_filtered.mrc", "m_filtered.mrc", "M's filtered map, in the Refine 3D's box")
    if m is not None:
        r.maps.append(m)


def _loss_curve(job_dir: Path) -> list[float]:
    """The training loss from the job's log: the value at the end of each epoch, else every log line's, thinned
    to eighty points."""
    log = job_dir / "run.out"
    if not log.exists():
        return []
    ends: list[float] = []
    every: list[float] = []
    with open(log, errors="replace") as f:
        for line in f:
            for piece in line.split("\r"):
                m = re.search(r"Loss: ([0-9.]+)", piece)
                if not m:
                    continue
                every.append(float(m.group(1)))
                if "100%" in piece:
                    ends.append(float(m.group(1)))
    vals = ends or every
    if len(vals) > 80:
        step = len(vals) / 80
        vals = [vals[int(i * step)] for i in range(80)]
    return vals

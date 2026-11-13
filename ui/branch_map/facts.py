"""What a large card shows of a job's results, read from what is on disk: Warp's own reconstruction PNGs, the
denoised slabs and segmentation overlays, the training log, template-matching and pick analytics, the registry's
numbers as plots. Every read runs off the event loop (`FactsStore` loads in a thread and the map draws again when
a load lands); pictures on disk are served by the app's own asset route, so only the composites (a mask tinted onto
a slice, the central slices of a map) are made here.
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import math
import re
import time
from collections.abc import Callable
from pathlib import Path

from services.dashboard_data import vis_asset_url
from ui.branch_map.model import Facts, Job, Plot, TiltPlot, TsFacts, param_words

logger = logging.getLogger(__name__)

THUMB = 192
# A running job's results change: its facts are read again when older than this.
_LIVE_MAX_AGE_S = 15.0
_thumbs: dict[tuple[str, float], str] = {}


# ── pictures ─────────────────────────────────────────────────────────────────


def picture_url(path: Path) -> str | None:
    """A picture on disk, as the asset route serves it at thumbnail size; None when it is not there."""
    if not path.exists():
        return None
    return vis_asset_url(str(path), thumb=THUMB)


def composite_url(path: Path, overlay: Path) -> str | None:
    """A slice with the segmentation overlay tinted onto it, as a small JPEG data URL."""
    from PIL import Image, ImageFilter, ImageOps

    try:
        key = (f"{path}+{overlay}", path.stat().st_mtime)
    except OSError:
        return None
    if key in _thumbs:
        return _thumbs[key]
    try:
        im = ImageOps.autocontrast(Image.open(path).convert("L"), cutoff=1)
        im.thumbnail((THUMB, THUMB))
        # The mask is thin at full size: thickened before the shrink, so a membrane stays a line.
        mask = Image.open(overlay).convert("LA").getchannel("A").filter(ImageFilter.MaxFilter(7))
        mask = mask.resize(im.size, Image.BILINEAR).point(lambda v: 255 if v > 40 else 0)
        base = im.convert("RGB")
        base.paste(Image.new("RGB", im.size, (56, 189, 248)), mask=mask)
        buf = io.BytesIO()
        base.save(buf, "JPEG", quality=72)
    except OSError:
        logger.exception("branch map: overlay of %s on %s", overlay, path)
        return None
    url = "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()
    _thumbs[key] = url
    return url


def map_slices_url(mrc: Path) -> str | None:
    """Three central slices of a small map (a particle average), side by side, as a data URL."""
    if not mrc.exists():
        return None
    try:
        import mrcfile
        import numpy as np
        from PIL import Image

        key = (str(mrc), mrc.stat().st_mtime)
        if key in _thumbs:
            return _thumbs[key]
        with mrcfile.mmap(mrc, mode="r", permissive=True) as m:
            v = np.asarray(m.data, dtype=np.float32)
        n = v.shape[0]
        slabs = [v[n // 2], v[:, n // 2], v[:, :, n // 2]]
        tiles = []
        for s in slabs:
            lo, hi = np.percentile(s, (1, 99))
            s = np.clip((s - lo) / ((hi - lo) or 1), 0, 1)
            tiles.append(Image.fromarray((s * 255).astype(np.uint8)).resize((THUMB, THUMB)))
        sheet = Image.new("L", (THUMB * 3 + 12, THUMB), 255)
        for i, t in enumerate(tiles):
            sheet.paste(t, (i * (THUMB + 6), 0))
        buf = io.BytesIO()
        sheet.save(buf, "JPEG", quality=72)
        url = "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()
        _thumbs[key] = url
        return url
    except (OSError, ValueError):
        logger.exception("branch map: slices of %s", mrc)
        return None


def _fullest_masks(masks: list[Path]) -> list[Path]:
    """The overlay masks with the most in them first, so the picture shows a tomogram with membranes."""
    from PIL import Image

    def coverage(p: Path) -> float:
        try:
            a = Image.open(p).convert("LA").getchannel("A").reduce(8)
            return sum(a.getdata()) / (a.size[0] * a.size[1])
        except OSError:
            return -1.0

    return sorted(masks, key=coverage, reverse=True)


def _recon_png(job_dir: Path, ts: str) -> Path | None:
    return next(iter(sorted((job_dir / "warp_tiltseries" / "reconstruction").glob(f"{ts}_*.png"))), None)


# ── readers ──────────────────────────────────────────────────────────────────


def _median(vals: list[float]) -> float:
    return sorted(vals)[len(vals) // 2]


def _plot_or_none(values: list[float], kind: str, caption: str, **kw) -> Plot | None:
    return Plot(kind, values, caption, **kw) if values else None


def _loss_curve(job_dir: Path) -> list[float]:
    """The training loss from the job's log: the value at the end of each epoch, else every log line's, thinned
    to eighty points."""
    log = job_dir / "run.out"
    if not log.exists():
        return []
    ends: list[float] = []
    every: list[float] = []
    try:
        with open(log, errors="replace") as f:
            for line in f:
                for piece in line.split("\r"):
                    m = re.search(r"Loss: ([0-9.]+)", piece)
                    if not m:
                        continue
                    every.append(float(m.group(1)))
                    if "100%" in piece:
                        ends.append(float(m.group(1)))
    except OSError:
        logger.exception("branch map: reading %s", log)
        return []
    vals = ends or every
    if len(vals) > 80:
        step = len(vals) / 80
        vals = [vals[int(i * step)] for i in range(80)]
    return vals


def _count_star_rows(star: Path) -> int:
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


def _frame_rows(reg: dict, ts: str, iid: str) -> list[tuple[float, dict, bool]]:
    """(tilt angle, this job's output, dropped by the filter) for every frame of a tilt series that has one."""
    out = []
    for fr in reg.get(ts, {}).get("frames", []):
        o = (fr.get("outputs") or {}).get(iid)
        if o:
            out.append((fr.get("nominal_tilt_angle_deg") or 0.0, o, bool(fr.get("is_filtered_out"))))
    return sorted(out, key=lambda r: r[0])


def _per_frame(reg: dict, ts: str, iid: str) -> list[tuple[float, dict]]:
    """(tilt angle, entry) for the per-frame entries of a tilt-series-level output, in angle order."""
    o = (reg.get(ts, {}).get("outputs") or {}).get(iid) or {}
    by_id = {fr["id"]: fr for fr in reg.get(ts, {}).get("frames", [])}
    rows = []
    for e in o.get("per_frame") or []:
        fr = by_id.get(e.get("frame_id"))
        rows.append(((fr or {}).get("nominal_tilt_angle_deg") or 0.0, e))
    return sorted(rows, key=lambda r: r[0])


def set_parent(project: Path, set_id: str) -> tuple[str | None, str]:
    """A merged-sources particle set: the job it pooled from (its recipe's first parent) and its name."""
    name = set_id.split("_", 1)[1] if "_" in set_id else set_id
    rec = project / "Analytics" / "sets" / set_id / "set.json"
    try:
        with open(rec) as f:
            d = json.load(f)
        parents = (d.get("recipe") or {}).get("parents") or []
        return (parents[0].get("instance_id") if parents else None), f"set {d.get('name', name)}"
    except (OSError, ValueError, AttributeError):
        return None, f"set {name}"


def job_facts(kind: str, jm, job_dir: Path | None, reg: dict[str, dict], iid: str, ts_order: list[str]) -> Facts:
    """What the large card shows of a finished job, from what is already on disk."""
    f = Facts()
    if job_dir is None:
        return f
    done = [t for t in ts_order if t in reg]
    if kind == "importmovies":
        tilts = sum(len(reg[t].get("frames", [])) for t in done)
        f.lines = [f"{len(done)} tilt series · {tilts} tilts · from the mdocs"]
    elif kind == "tsImport":
        f.lines = [f"{len(done)} tilt series as Warp tomostars · {param_words(jm, 'tomo_dimensions')}"]
    elif kind == "fsMotionAndCtf":
        vals = []
        for t in done:
            res = []
            for fr in reg[t].get("frames", []):
                o = (fr.get("outputs") or {}).get(iid) or {}
                if o.get("ctf_resolution"):
                    res.append(o["ctf_resolution"])
            if res:
                vals.append(sorted(res)[len(res) // 2])
        f.plot = _plot_or_none(
            vals,
            "dots",
            f"CTF fit resolution per tilt series, Å · median {_median(vals):.1f} Å" if vals else "",
            lo=min(vals, default=0) * 0.9,
            hi=max(vals, default=1) * 1.05,
        )
    elif kind == "tiltFilter":
        out = sum(1 for t in done for fr in reg[t].get("frames", []) if fr.get("is_filtered_out"))
        tilts = sum(len(reg[t].get("frames", [])) for t in done)
        per = [sum(1 for fr in reg[t].get("frames", []) if fr.get("is_filtered_out")) for t in done]
        f.plot = Plot(
            "bars",
            [float(v) for v in per],
            f"tilts taken out per tilt series · {out} of {tilts} · threshold {param_words(jm, 'threshold')}",
        )
    elif kind == "aligntiltsWarp":
        vals = []
        for t in done:
            o = (reg[t].get("outputs") or {}).get(iid) or {}
            pf = o.get("per_frame") or []
            if pf:
                vals.append(pf[0].get("z_rot_deg") or 0.0)
        f.plot = _plot_or_none(
            vals,
            "dots",
            f"tilt axis per tilt series, ° · {param_words(jm, 'alignment_method')}",
            lo=min(vals, default=0) - 1,
            hi=max(vals, default=1) + 1,
        )
    elif kind == "missAlign":
        ch = job_dir / "missalign_changes.json"
        vals, flags = [], set()
        if ch.exists():
            with open(ch) as fh:
                d = json.load(fh)
            per = d.get("tilt_series", {})
            for t in done:
                r = per.get(t)
                if r:
                    vals.append(r.get("residual_rms_angstrom") or 0.0)
                    if r.get("tilts_over_threshold"):
                        flags.add(len(vals) - 1)
            f.plot = _plot_or_none(
                vals,
                "bars",
                f"residual per tilt series after refinement, Å rms · {d.get('threshold', '')}",
                flags=flags,
            )
    elif kind == "tsCtf":
        vals = []
        for t in done:
            o = (reg[t].get("outputs") or {}).get(iid) or {}
            if o.get("ctf_resolution"):
                vals.append(o["ctf_resolution"])
        f.plot = _plot_or_none(
            vals,
            "dots",
            f"CTF fit resolution per tilt series, Å · median {_median(vals):.1f} Å" if vals else "",
            lo=min(vals, default=0) * 0.9,
            hi=max(vals, default=1) * 1.05,
        )
    elif kind == "tsReconstruct":
        apix = getattr(jm, "rescale_angpixs", 0)
        pngs = sorted((job_dir / "warp_tiltseries" / "reconstruction").glob("*.png"))
        for p in pngs[:3]:
            url = picture_url(p)
            if url:
                f.pictures.append((url, p.stem))
        dims = ""
        for t in done:
            o = ((reg[t].get("tomogram") or {}).get("outputs") or {}).get(iid)
            if o:
                dims = f" · {o.get('size_x')}×{o.get('size_y')}×{o.get('size_z')}"
                break
        f.picture_caption = f"{len(pngs)} tomograms at {apix:g} Å{dims}" + (
            " · deconvolved" if getattr(jm, "deconv", 0) else ""
        )
    elif kind == "denoisepredict":
        slabs = sorted((job_dir / "vis" / "slabs").glob("*_xy.png"))
        src = Path(jm.paths.get("input_star", "")).parent if jm.paths.get("input_star") else None
        for p in slabs:
            ts = p.name.removesuffix("_xy.png")
            before = _recon_png(src, ts) if src else None
            if before is not None:
                a, b = picture_url(before), picture_url(p)
                if a and b:
                    f.pictures = [(a, f"{ts} before"), (b, f"{ts} after")]
                    f.arrow = True
                    break
        f.picture_caption = f"a tomogram before and after · {param_words(jm, 'denoise_method')} · {len(slabs)} denoised"
    elif kind == "segment":
        feats = list(getattr(jm, "features", []) or [])
        masks = _fullest_masks(sorted((job_dir / "vis" / "segment").glob("*__membrane_xy.png")))
        src = Path(jm.paths.get("input_tomograms", "")).parent if jm.paths.get("input_tomograms") else None
        for m in masks:
            ts = m.name.removesuffix("__membrane_xy.png")
            base = (src / "vis" / "slabs" / f"{ts}_xy.png") if src else None
            if (base is None or not base.exists()) and src is not None:
                base = _recon_png(src, ts)
            if base is not None and base.exists():
                a, b = picture_url(base), composite_url(base, m)
                if a and b:
                    f.pictures = [(a, ts), (b, f"{ts} · membrane")]
                    f.arrow = True
                    break
        n_seg = len({p.name.split("__")[0] for p in (job_dir / "segmentations").glob("*.mrc")})
        f.picture_caption = f"membranes on a tomogram · {len(feats)} features: {', '.join(feats)} · {n_seg} tomograms"
    elif kind == "denoisetrain":
        vals = _loss_curve(job_dir)
        f.plot = _plot_or_none(vals, "curve", f"training loss over the log · {param_words(jm, 'denoise_method')}")
    elif kind == "templatematching":
        vals, cut = [], None
        for p in sorted((job_dir / "analytics" / "tm_tomo").glob("*.json")):
            with open(p) as fh:
                d = json.load(fh).get("data", {})
            top = (d.get("scan") or {}).get("top_sigma")
            if top is not None:
                vals.append(top)
                cut = d.get("fp1_sigma") or cut
        f.plot = _plot_or_none(
            vals,
            "dots",
            f"best peak per tomogram, σ · the line is one false positive · {param_words(jm, 'angular_search')}° search",
            lo=0,
            hi=max(vals, default=1) * 1.1,
            cut=cut,
        )
    elif kind == "tmextractcand":
        rec = job_dir / "analytics" / "candidates.json"
        if rec.exists():
            with open(rec) as fh:
                d = json.load(fh).get("data", {})
            toms = d.get("tomograms") or []
            scores = sorted(s for t in toms for s in (t.get("scores") or []))
            if scores:
                lo, hi = scores[0], scores[-1]
                bins = 32
                counts = [0] * bins
                for s in scores:
                    counts[min(bins - 1, int((s - lo) / ((hi - lo) or 1) * bins))] += 1
                cut = min((t.get("cutoff_sigma") for t in toms if t.get("cutoff_sigma")), default=None)
                cut_bin = int((cut - lo) / ((hi - lo) or 1) * bins) if cut is not None else None
                cap = (
                    f"scores of all {d.get('n_total', len(scores))} picks, σ {lo:.1f}–{hi:.1f} · cut at one false "
                    f"positive · {d.get('n_with_picks')} of {d.get('n_tomograms')} tomograms with picks"
                )
                f.plot = Plot("hist", [float(c) for c in counts], cap, cut=cut_bin)
        prev = job_dir / "vis" / "preview"
        atlas = next(iter(sorted(prev.glob("*/cutout_atlas.png"))), None)
        if atlas is not None:
            url = picture_url(atlas)
            if url:
                f.pictures = [(url, f"cut-outs · {atlas.parent.name}")]
                f.picture_caption = "the picks of one tomogram, cut out"
    elif kind == "warpExport":
        n = _count_star_rows(job_dir / "particles.star")
        f.lines = [
            f"{n} particles exported · box {param_words(jm, 'box_size')} · bin {getattr(jm, 'binning', 0):g} · "
            f"Ø {getattr(jm, 'diameter_ang', 0):g} Å"
        ]
    elif kind == "reconstructParticle":
        url = map_slices_url(job_dir / "merged.mrc")
        if url:
            f.pictures = [(url, "merged.mrc, three central slices")]
        f.picture_caption = (
            f"the average · box {param_words(jm, 'box_size')} · {param_words(jm, 'symmetry')} · "
            f"bin {getattr(jm, 'binning', 0):g}"
        )
    return f


def ts_facts(kind: str, jm, job_dir: Path | None, reg: dict, iid: str, ts: str) -> TsFacts | None:
    """The Journey's section of one job for one tilt series, from the registry and the job's own files."""
    if ts not in reg or (job_dir is None and kind != "tiltFilter"):
        return None  # the tilt filter runs inline: its verdicts are in the registry, not in a job dir
    f = TsFacts()
    frames = reg[ts].get("frames", [])
    dropped_idx = {
        i
        for i, fr in enumerate(sorted(frames, key=lambda x: x.get("nominal_tilt_angle_deg") or 0))
        if fr.get("is_filtered_out")
    }
    if kind == "importmovies":
        tilts = [fr.get("nominal_tilt_angle_deg") or 0.0 for fr in frames]
        dose = [fr.get("exposure_dose_e_per_a2") for fr in frames if fr.get("exposure_dose_e_per_a2")]
        f.tiles = [("Tilts", str(len(frames)))]
        if tilts:
            f.tiles.append(("Range", f"{min(tilts):+.0f}° … {max(tilts):+.0f}°"))
        if dose:
            f.tiles.append(("Dose per tilt", f"{_median(dose):.2f} e/Å²"))
    elif kind == "fsMotionAndCtf":
        rows = _frame_rows(reg, ts, iid)
        if not rows:
            return f
        tilts = [t for t, _, _ in rows]
        drop = {i for i, (_, _, d) in enumerate(rows) if d}
        defocus = [((o.get("defocus_u_angstrom") or 0) + (o.get("defocus_v_angstrom") or 0)) / 2e4 for _, o, _ in rows]
        res = [o.get("ctf_resolution") or 0.0 for _, o, _ in rows]
        motion = [o.get("mean_frame_movement") or 0.0 for _, o, _ in rows]
        f.tiles = [("Median defocus", f"{_median(defocus):.2f} µm")]
        if any(res):
            f.tiles += [("Median CTF fit", f"{_median(res):.1f} Å"), ("Worst CTF fit", f"{max(res):.1f} Å")]
        if any(motion):
            f.tiles.append(("Largest motion", f"{max(motion):.2f}"))
        f.plots = [TiltPlot("Defocus", tilts, defocus, "µm", dropped=drop, lo=0, hi=10)]
        if any(res):
            f.plots.append(TiltPlot("CTF fit resolution", tilts, res, "Å", dropped=drop, lo=0, hi=30))
        if any(motion):
            f.plots.append(TiltPlot("Beam-induced motion", tilts, motion, "", dropped=drop, lo=0))
    elif kind == "tiltFilter":
        rows = sorted(frames, key=lambda x: x.get("nominal_tilt_angle_deg") or 0)
        tilts = [fr.get("nominal_tilt_angle_deg") or 0.0 for fr in rows]
        p_bad = [fr.get("p_bad") for fr in rows]
        n_drop = sum(1 for fr in rows if fr.get("is_filtered_out"))
        f.tiles = [("Kept", f"{len(rows) - n_drop} of {len(rows)}"), ("Dropped", str(n_drop))]
        if any(v is not None for v in p_bad):
            f.plots = [
                TiltPlot(
                    "P(bad) per tilt",
                    tilts,
                    [v or 0.0 for v in p_bad],
                    "",
                    dropped=dropped_idx,
                    lo=0,
                    hi=1,
                    cut=float(getattr(jm, "threshold", 0) or 0) or None,
                )
            ]
        gone = [f"{fr.get('nominal_tilt_angle_deg') or 0:+.0f}°" for fr in rows if fr.get("is_filtered_out")]
        f.lines = [("dropped at " + ", ".join(gone)) if gone else "nothing dropped"]
    elif kind in ("aligntiltsWarp", "missAlign"):
        rows = _per_frame(reg, ts, iid)
        if rows:
            tilts = [t for t, _ in rows]
            shift = [
                ((e.get("x_shift_angstrom") or 0) ** 2 + (e.get("y_shift_angstrom") or 0) ** 2) ** 0.5 for _, e in rows
            ]
            offset = [-(e.get("tilt_y_deg") or 0) - t for t, e in rows]
            axis = [e.get("z_rot_deg") or 0.0 for _, e in rows]
            f.tiles = [("Largest shift", f"{max(shift):.1f} Å"), ("Median shift", f"{_median(shift):.1f} Å")]
            if axis:
                f.tiles.append(("Tilt axis", f"{_median(axis):.2f}°"))
            f.plots = [TiltPlot("Refined shift", tilts, shift, "Å", lo=0)]
            if any(abs(v) < 5 for v in offset):
                f.plots.append(TiltPlot("Tilt offset (refined − nominal)", tilts, offset, "°"))
            inten = [e.get("average_intensity") for _, e in rows]
            if any(inten):
                f.plots.append(TiltPlot("Mean tilt intensity", tilts, [v or 0.0 for v in inten], ""))
        if kind == "missAlign" and job_dir is not None:
            ch = job_dir / "missalign_changes.json"
            if ch.exists():
                with open(ch) as fh:
                    rec = (json.load(fh).get("tilt_series") or {}).get(ts)
                if rec:
                    per = sorted(rec.get("per_tilt") or [], key=lambda e: e.get("angle_deg") or 0)
                    thr = rec.get("threshold_angstrom")
                    f.tiles.append(("Residual rms", f"{rec.get('residual_rms_angstrom', 0):.1f} Å"))
                    f.tiles.append(("Over threshold", f"{len(rec.get('tilts_over_threshold') or [])} tilts"))
                    if per:
                        f.plots.append(
                            TiltPlot(
                                "Residual after refinement",
                                [e.get("angle_deg") or 0.0 for e in per],
                                [e.get("residual_angstrom") or 0.0 for e in per],
                                "Å",
                                kind="bars",
                                lo=0,
                                cut=thr,
                            )
                        )
    elif kind == "tsCtf":
        rows = _per_frame(reg, ts, iid)
        if not rows:
            return f
        tilts = [t for t, _ in rows]
        defocus = [((e.get("defocus_u_angstrom") or 0) + (e.get("defocus_v_angstrom") or 0)) / 2e4 for _, e in rows]
        astig = [e.get("ctf_astigmatism") or 0.0 for _, e in rows]
        out = (reg[ts].get("outputs") or {}).get(iid) or {}
        f.tiles = [
            ("Median defocus", f"{_median(defocus):.2f} µm"),
            ("Range", f"{min(defocus):.2f} – {max(defocus):.2f} µm"),
        ]
        if out.get("ctf_resolution"):
            f.tiles.append(("CTF fit resolution", f"{out['ctf_resolution']:.1f} Å"))
        normal = out.get("plane_normal")
        if normal:
            f.tiles.append(("Specimen plane tilt", f"{math.degrees(math.acos(min(1.0, abs(float(normal[2]))))):.1f}°"))
        f.plots = [TiltPlot("Defocus", tilts, defocus, "µm", lo=0, hi=10)]
        if any(astig):
            f.plots.append(TiltPlot("Astigmatism", tilts, astig, "Å", lo=0, hi=1500))
    elif kind == "tsReconstruct":
        png = _recon_png(job_dir, ts)
        if png is not None:
            url = picture_url(png)
            if url:
                f.pictures = [(url, f"{ts} · Warp's x/y slice")]
        o = ((reg[ts].get("tomogram") or {}).get("outputs") or {}).get(iid) or {}
        if o:
            f.tiles = [
                ("Pixel size", f"{o.get('tomogram_pixel_size_angstrom', 0):g} Å"),
                ("Size", f"{o.get('size_x')}×{o.get('size_y')}×{o.get('size_z')}"),
                ("Binning", f"{o.get('binning', 0):g}"),
            ]
    elif kind == "denoisepredict":
        after = job_dir / "vis" / "slabs" / f"{ts}_xy.png"
        src = Path(jm.paths.get("input_star", "")).parent if jm.paths.get("input_star") else None
        before = _recon_png(src, ts) if src else None
        a = picture_url(before) if before else None
        b = picture_url(after)
        if a and b:
            f.pictures, f.arrow = [(a, f"{ts} before"), (b, f"{ts} after")], True
        elif b:
            f.pictures = [(b, f"{ts} denoised")]
        f.tiles = [("Method", param_words(jm, "denoise_method"))]
        if not f.pictures:
            f.lines = ["no slab rendered for this tilt series"]
    elif kind == "segment":
        rec = job_dir / "analytics" / "segment" / f"{ts}.json"
        src = Path(jm.paths.get("input_tomograms", "")).parent if jm.paths.get("input_tomograms") else None
        base = (src / "vis" / "slabs" / f"{ts}_xy.png") if src else None
        if base is None or not base.exists():
            base = _recon_png(src, ts) if src else None
        if rec.exists():
            with open(rec) as fh:
                vols = json.load(fh).get("volumes") or {}
            f.tiles = [(name, f"{v.get('fraction_pct', 0):.2f} %") for name, v in vols.items()]
            mask = job_dir / "vis" / "segment" / f"{ts}__membrane_xy.png"
            if base is not None and base.exists():
                a = picture_url(base)
                b = composite_url(base, mask) if mask.exists() else None
                if a and b:
                    f.pictures, f.arrow = [(a, ts), (b, f"{ts} · membrane")], True
                elif a:
                    f.pictures = [(a, ts)]
        else:
            f.lines = ["no segmentation record for this tilt series"]
    elif kind == "templatematching":
        rec = job_dir / "analytics" / "tm_tomo" / f"{ts}.json"
        if not rec.exists():
            f.lines = ["no template-matching record for this tilt series"]
            return f
        with open(rec) as fh:
            d = json.load(fh).get("data", {})
        scan = d.get("scan") or {}
        peaks = scan.get("peaks_sigma") or []
        cut = d.get("fp1_sigma")
        f.tiles = [("Best peak", f"{scan.get('top_sigma', 0):.1f} σ")]
        if cut:
            f.tiles.append(("One false positive at", f"{cut:.1f} σ"))
        f.tiles += [
            ("Local maxima", str(scan.get("n_local_max", ""))),
            ("Run time", f"{d.get('runtime_min', 0):g} min"),
        ]
        if peaks:
            f.plots = [
                TiltPlot(
                    f"The {len(peaks)} strongest peaks, σ",
                    list(range(1, len(peaks) + 1)),
                    peaks,
                    "σ",
                    kind="bars",
                    lo=0,
                    cut=cut,
                    over_is_bad=False,
                )
            ]
    elif kind == "tmextractcand":
        rec = job_dir / "analytics" / "candidates.json"
        if rec.exists():
            with open(rec) as fh:
                toms = json.load(fh).get("data", {}).get("tomograms") or []
            t = next((x for x in toms if x.get("tomo") == ts), None)
            if t:
                scores = sorted(t.get("scores") or [])
                cut = t.get("cutoff_sigma")
                f.tiles = [("Picks", str(t.get("n", len(scores))))]
                if cut:
                    f.tiles.append(("Cut at", f"{cut:.1f} σ"))
                if scores:
                    f.tiles.append(("Scores", f"{scores[0]:.1f} – {scores[-1]:.1f} σ"))
                    bins = 16
                    lo, hi = scores[0], scores[-1]
                    counts = [0.0] * bins
                    for v in scores:
                        counts[min(bins - 1, int((v - lo) / ((hi - lo) or 1) * bins))] += 1
                    f.plots = [
                        TiltPlot(
                            "Pick scores",
                            [lo + (hi - lo) * (i + 0.5) / bins for i in range(bins)],
                            counts,
                            "σ",
                            kind="bars",
                            lo=0,
                            cut=cut,
                            over_is_bad=False,
                        )
                    ]
            else:
                f.lines = ["no picks on this tilt series"]
        atlas = job_dir / "vis" / "preview" / ts / "cutout_atlas.png"
        url = picture_url(atlas)
        if url:
            f.pictures = [(url, f"{ts} · the picks, cut out")]
    elif kind in ("warpExport", "subtomoExtraction"):
        star = job_dir / "particles.star"
        n = 0
        if star.exists():
            with open(star, errors="replace") as fh:
                n = sum(1 for line in fh if ts in line)
        f.tiles = [("Particles from this tilt series", str(n))]
    else:
        f.lines = ["a particle-level job: nothing per tilt series"]
    return f


# ── the store ────────────────────────────────────────────────────────────────


class FactsStore:
    """One project's results as read so far, shared by every tab's map. A reader asks (`job_facts`, `ts_facts`)
    and gets what is there; what is missing or stale is loaded in a thread (`load_job`, `load_ts`) and `version`
    moves when it lands, so a view whose signature carries it draws again."""

    def __init__(self, project_path: Path) -> None:
        self.path = project_path
        self.version = 0
        self._job: dict[str, tuple[float, str, Facts]] = {}
        self._ts: dict[tuple[str, str], tuple[float, str, TsFacts | None]] = {}
        self.set_parents: dict[str, tuple[str | None, str]] = {}
        self._loading: set = set()
        self._reg_dump: tuple[tuple, dict[str, dict]] | None = None

    # ── what is there ──
    def job_facts(self, j: Job) -> Facts | None:
        entry = self._job.get(j.id)
        return entry[2] if entry is not None else None

    def ts_facts(self, j: Job, ts: str) -> tuple[bool, TsFacts | None]:
        entry = self._ts.get((j.id, ts))
        return (True, entry[2]) if entry is not None else (False, None)

    def _stale(self, entry: tuple | None, status: str) -> bool:
        if entry is None:
            return True
        loaded_at, had, _ = entry
        if had != status:
            return True
        return status in ("running", "queued") and time.monotonic() - loaded_at > _LIVE_MAX_AGE_S

    def job_stale(self, j: Job) -> bool:
        return self._stale(self._job.get(j.id), j.status)

    def ts_stale(self, j: Job, ts: str) -> bool:
        return self._stale(self._ts.get((j.id, ts)), j.status)

    def loading(self, key) -> bool:
        return key in self._loading

    # ── reading ──
    def _registry_dict(self, series: list, stamp: tuple) -> dict[str, dict]:
        """Every tilt series as plain dicts (the readers above take JSON), made once per registry stamp."""
        if self._reg_dump is not None and self._reg_dump[0] == stamp:
            return self._reg_dump[1]
        reg = {ts.id: ts.model_dump(mode="json") for ts in series}
        self._reg_dump = (stamp, reg)
        return reg

    async def load_job(self, j: Job, jm, series: list, stamp: tuple, ts_order: list[str]) -> None:
        key = ("job", j.id)
        if key in self._loading:
            return
        self._loading.add(key)
        try:
            reg = await asyncio.to_thread(self._registry_dict, series, stamp)
            facts = await asyncio.to_thread(job_facts, j.kind, jm, j.job_dir, reg, j.id, ts_order)
        except Exception:  # the card then shows the magazine and the tally only
            logger.exception("branch map: facts of %s", j.id)
            facts = Facts()
        finally:
            self._loading.discard(key)
        self._job[j.id] = (time.monotonic(), j.status, facts)
        self.version += 1

    async def load_ts(self, j: Job, jm, ts_obj, ts: str) -> None:
        key = ("ts", j.id, ts)
        if key in self._loading:
            return
        self._loading.add(key)
        try:
            reg = await asyncio.to_thread(lambda: {ts: ts_obj.model_dump(mode="json")} if ts_obj is not None else {})
            facts = await asyncio.to_thread(ts_facts, j.kind, jm, j.job_dir, reg, j.id, ts)
        except Exception:  # the card then says nothing for this tilt series
            logger.exception("branch map: facts of %s for %s", j.id, ts)
            facts = None
        finally:
            self._loading.discard(key)
        self._ts[(j.id, ts)] = (time.monotonic(), j.status, facts)
        self.version += 1

    async def load_sets(self, set_ids: list[str]) -> None:
        wanted = [s for s in set_ids if s not in self.set_parents and ("set", s) not in self._loading]
        if not wanted:
            return
        for s in wanted:
            self._loading.add(("set", s))
        try:
            found = await asyncio.to_thread(lambda: {s: set_parent(self.path, s) for s in wanted})
        finally:
            for s in wanted:
                self._loading.discard(("set", s))
        self.set_parents.update(found)
        self.version += 1


_stores: dict[str, FactsStore] = {}


def facts_store_for(project_path: Path) -> FactsStore:
    key = str(project_path)
    if key not in _stores:
        _stores[key] = FactsStore(project_path)
    return _stores[key]


def kick(coro, after: Callable[[], None] | None = None) -> asyncio.Task:
    """Run a load in the background and call `after` (the view's refresh) once it has landed."""

    async def run() -> None:
        await coro
        if after is not None:
            after()

    return asyncio.create_task(run())

"""What a project snapshot shows, read from disk into one plain dict, and the pictures it needs copied.

The snapshot is the workspace's views read-only: the jobs and their parameters (the Jobs table), the
tomogram wall with its pick layers, each tilt series' tilts and per-tilt charts (the Journey), and per
species the particle readouts. Nothing that acts — run, stop, edit, add, protocols, other projects — is
in it. The readers are the views' own (`pipeline_table`, `tomo_gallery.collect_rows`,
`tilt_previews.collect_tilt_groups`, `dashboard_data`, `class_readout`), so the snapshot and the live
page can never disagree; the charts are the same ECharts option dicts (`ui/dashboard/figures.py`).

Pictures are not copied here: each one the data names is an `Img` (source PNG → path in the bundle),
which `build.py` converts in a worker. Every path in the data is relative to the bundle's index.html.

`collect` reads disk only and is safe in a thread; what must be read on the event loop (the registry,
the tilt filter's review context) is passed in.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from services.dashboard_data import (
    alignment_registry_df,
    filter_kept_dropped_from_registry,
    filter_verdicts_from_registry,
    fsm_motion_tracks,
    fsm_registry_df,
    job_dir_for,
    species_color,
    tilt_filter_committed,
    tsctf_registry_df,
)
from services.jobs.spec import instance_display_name
from services.models_base import JobStatus, JobType
from services.particles.class_readout import class_iteration_table, readout_record
from services.particles.species_overview import species_overview
from services.pipeline_table import pipeline_rows, run_times
from services.code_version import code_version
from ui.dashboard.figures import (
    JOURNEY_THEME,
    _is_meaningful_series,
    _safe_floats,
    _stats,
    build_class_convergence_chart,
    build_fsc_chart,
    build_motion_track_chart,
    build_per_tilt_chart,
)
from ui.pipeline_table_view import gutter_svg
from ui.status_indicator import _DOT_COLORS
from ui.tilt_previews import collect_tilt_groups
from ui.tomo_gallery import collect_rows

logger = logging.getLogger(__name__)

# Long edge of the pictures in the bundle (JPEG): a tomogram's full view (WarpTools writes 1024 px, which
# stays as it is), its wall tile, a tilt (shown 92 px wide; 160 keeps it sharp on a dense screen). A
# tomogram slice is mostly noise and stays ~450 KB at any useful quality; the tilts are the many.
TOMO_PX = 1600
TILE_PX = 320
TILT_PX = 160
# The last lines of a job's run.out / run.err the Jobs view shows.
LOG_TAIL_LINES = 80

_ALIGNERS = (JobType.TS_ALIGNMENT, JobType.MISS_ALIGN)


@dataclass(frozen=True)
class Img:
    """One picture to put in the bundle: `src` (a PNG on disk) converted to `dst` (relative to the
    bundle), its long edge at most `max_px`; `max_px` 0 copies it as it is."""

    src: str
    dst: str
    max_px: int


@dataclass
class Collected:
    data: dict[str, Any]
    images: list[Img] = field(default_factory=list)


def _slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(text)).strip("_") or "x"


def _plain(v: Any) -> Any:
    """A parameter value as JSON can carry it: enums by value, paths and the rest as text."""
    if v is None or isinstance(v, bool | int | float | str):
        return v
    if hasattr(v, "value"):
        return v.value
    if isinstance(v, list | tuple):
        return [_plain(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _plain(x) for k, x in v.items()}
    if hasattr(v, "model_dump"):
        return _plain(v.model_dump())
    return str(v)


def _tail(path: Path, n: int) -> str | None:
    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 64 * 1024))
            text = f.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return None
    return "\n".join(text.splitlines()[-n:])


class _Pictures:
    """The bundle's pictures, each source once."""

    def __init__(self) -> None:
        self.images: list[Img] = []
        self._seen: dict[tuple[str, int], str] = {}

    def add(self, src: Path | str | None, dst: str, max_px: int) -> str | None:
        if not src:
            return None
        key = (str(src), max_px)
        if key not in self._seen:
            self.images.append(Img(str(src), dst, max_px))
            self._seen[key] = dst
        return self._seen[key]


# ── Overview ──────────────────────────────────────────────────────────────────


def _overview(state, project_path: Path, n_series: int, n_tomos: int) -> dict:
    mic = state.microscope
    acq = state.acquisition
    facts = [
        ("Pixel size", f"{mic.pixel_size_angstrom:g} Å"),
        ("Voltage", f"{mic.acceleration_voltage_kv:g} kV"),
        ("Cs", f"{mic.spherical_aberration_mm:g} mm"),
        ("Amplitude contrast", f"{mic.amplitude_contrast:g}"),
        ("Dose per tilt", f"{acq.dose_per_tilt:g} e⁻/Å²"),
        ("Tilt axis", f"{acq.tilt_axis_degrees:g}°"),
        ("Detector", f"{acq.detector_dimensions[0]} × {acq.detector_dimensions[1]}"),
        ("Acquisition software", acq.acquisition_software),
    ]
    if acq.eer_fractions_per_frame:
        facts.append(("EER fractions per frame", str(acq.eer_fractions_per_frame)))
    origin = state.protocol_origin
    return {
        "project": state.project_name,
        "path": str(project_path),
        "created": state.created_at.isoformat(timespec="minutes") if state.created_at else None,
        "snapshot_at": datetime.now().isoformat(timespec="minutes"),
        "code_version": code_version(),
        "protocol": getattr(origin, "name", None) if origin is not None else None,
        "n_series": n_series,
        "n_tomograms": n_tomos,
        "facts": facts,
    }


# ── Jobs ──────────────────────────────────────────────────────────────────────


def _jobs(state, project_path: Path) -> list[dict]:
    """The Jobs table, read-only: its rows (with the lane graphic), each job's status, run times, its
    parameters, inputs, SLURM resources and the tail of its log."""
    rows = pipeline_rows(state)
    if not rows:
        return []
    times = run_times({r.instance_id: state.jobs[r.instance_id] for r in rows}, project_path, rows)
    lanes = 1 + max(lane for r in rows for lane in (r.lane, *r.through, *r.forks))
    out = []
    for row in rows:
        iid = row.instance_id
        jm = state.jobs[iid]
        status = jm.execution_status
        fill = "#f97316" if jm.is_orphaned else _DOT_COLORS.get(status, "#9ca3af")
        params = [(name, _plain(getattr(jm, name, None))) for name in sorted(type(jm).USER_PARAMS)]
        try:
            slurm = _plain(jm.get_effective_slurm_config())
        except (AttributeError, ValueError) as e:
            # A job model with no project state behind it has no merged resources to state.
            logger.warning("Snapshot: no SLURM settings for %s: %s", iid, e)
            slurm = None
        job_dir = job_dir_for(state, iid, jm, project_path)
        t = times.get(iid)
        out.append(
            {
                "iid": iid,
                "name": instance_display_name(iid, jm),
                "type": jm.job_type.value,
                "status": status.value,
                "color": fill,
                "gutter": gutter_svg(row, lanes, fill, "#ffffff", False),
                "job_dir": jm.relion_job_name.rstrip("/") if jm.relion_job_name else None,
                "start": t.start if t else None,
                "end": t.end if t else None,
                "params": params,
                "inputs": [
                    {"slot": e.slot, "producer": e.producer, "output": e.output, "explicit": e.explicit}
                    for e in row.inputs
                ],
                "slurm": slurm,
                "log": _tail(job_dir / "run.out", LOG_TAIL_LINES) if job_dir else None,
                "err": _tail(job_dir / "run.err", LOG_TAIL_LINES) if job_dir else None,
            }
        )
    return out


# ── Tomograms: the wall, the tilts, the per-tilt charts ──────────────────────


def _filter_marks(project_path: Path, ts_name: str, committed: bool) -> tuple[dict[str, str], list[float]]:
    """As the Journey's `_filter_marks`: the tilt filter's verdict per frame and its dropped angles."""
    verdicts = filter_verdicts_from_registry(project_path, ts_name, committed=committed)
    info = filter_kept_dropped_from_registry(project_path, ts_name, committed=committed)
    angles = [d["tilt_angle"] for d in (info or {}).get("dropped", []) if d["tilt_angle"] is not None]
    return verdicts, angles


def _customdata(df, verdicts: dict[str, str], thumbs: dict[str, str]) -> list[list]:
    """The Journey hover card's slots (`_per_tilt_customdata`), the thumbnails as bundle paths."""
    import pandas as pd

    bases = (
        [Path(str(v)).name for v in df["rlnMicrographMovieName"].tolist()]
        if "rlnMicrographMovieName" in df.columns
        else [""] * len(df)
    )
    numbers = (
        [None if pd.isna(v) else int(v) for v in df["cbTiltNumber"].tolist()]
        if "cbTiltNumber" in df.columns
        else [None] * len(df)
    )
    return [[n, b, verdicts.get(b, ""), thumbs.get(b, "")] for n, b in zip(numbers, bases, strict=True)]


def _chart(title: str, option: dict) -> dict:
    return {"title": title, "option": option}


def _motion_section(df, cd: list, tracks: list[dict], drops: list[float]) -> dict:
    tilts = _safe_floats(df["rlnTomoNominalStageTiltAngle"])
    ctf_res = _safe_floats(df["cbCtfResolution"]) if "cbCtfResolution" in df.columns else []
    motion = _safe_floats(df["cbMeanFrameMovement"]) if "cbMeanFrameMovement" in df.columns else []
    spread = _safe_floats(df["cbDefocusSpread"]) if "cbDefocusSpread" in df.columns else []
    charts, stats = _ctf_charts(df, tilts, cd, drops, ctf_res)
    if _is_meaningful_series(motion, threshold=1e-4):
        charts.append(
            _chart(
                "Beam-induced motion",
                build_per_tilt_chart(
                    tilts,
                    [{"name": "Mean frame motion", "y": motion}],
                    y_label="Mean frame motion",
                    customdata=cd,
                    dropped_tilts=drops,
                    theme=JOURNEY_THEME,
                ),
            )
        )
        m = _stats([v for v in motion if v is not None])
        stats.append(("Largest frame motion", f"{m['max']:.2f}"))
    if tracks:
        charts.append(
            _chart("Motion trajectories", build_motion_track_chart(tracks, dropped_tilts=drops, theme=JOURNEY_THEME))
        )
    if _is_meaningful_series(spread, threshold=1e-4):
        charts.append(
            _chart(
                "Defocus spread across image",
                build_per_tilt_chart(
                    tilts,
                    [{"name": "Defocus spread", "y": spread}],
                    y_label="Defocus spread (µm)",
                    customdata=cd,
                    y_unit=" µm",
                    y_range=(0.0, 0.5),
                    dropped_tilts=drops,
                    theme=JOURNEY_THEME,
                ),
            )
        )
    return {"title": "Motion & CTF", "stats": stats, "charts": charts}


def _ctf_charts(df, tilts: list, cd: list, drops: list[float], ctf_res: list) -> tuple[list[dict], list]:
    """Defocus, astigmatism and the CTF fit resolution, as `_render_ctf_motion_plots` draws them."""
    charts: list[dict] = []
    stats: list[tuple[str, str]] = []
    if "rlnDefocusU" in df.columns and "rlnDefocusV" in df.columns:
        du = [None if v is None else v / 1.0e4 for v in _safe_floats(df["rlnDefocusU"])]
        dv = [None if v is None else v / 1.0e4 for v in _safe_floats(df["rlnDefocusV"])]
        d = _stats([v for v in du if v is not None])
        if d["n"]:
            stats.append(("Median defocus", f"{d['median']:.2f} µm"))
            stats.append(("Defocus, middle half", f"{d['q1']:.2f} – {d['q3']:.2f} µm"))
        charts.append(
            _chart(
                "Defocus",
                build_per_tilt_chart(
                    tilts,
                    [{"name": "Defocus U", "y": du}, {"name": "Defocus V", "y": dv}],
                    y_label="Defocus (µm)",
                    customdata=cd,
                    y_unit=" µm",
                    y_range=(0.0, 10.0),
                    dropped_tilts=drops,
                    theme=JOURNEY_THEME,
                ),
            )
        )
    if "rlnCtfAstigmatism" in df.columns:
        astig = _safe_floats(df["rlnCtfAstigmatism"])
        if _is_meaningful_series(astig):
            charts.append(
                _chart(
                    "Astigmatism",
                    build_per_tilt_chart(
                        tilts,
                        [{"name": "Astigmatism", "y": astig}],
                        y_label="Astigmatism (Å)",
                        customdata=cd,
                        y_unit=" Å",
                        y_range=(0.0, 1500.0),
                        dropped_tilts=drops,
                        theme=JOURNEY_THEME,
                    ),
                )
            )
    if _is_meaningful_series(ctf_res):
        r = _stats([v for v in ctf_res if v is not None])
        stats.append(("Median CTF fit resolution", f"{r['median']:.1f} Å"))
        stats.append(("Worst CTF fit", f"{r['max']:.1f} Å"))
        charts.append(
            _chart(
                "CTF fit resolution",
                build_per_tilt_chart(
                    tilts,
                    [{"name": "CTF fit resolution", "y": ctf_res}],
                    y_label="CTF fit resolution (Å)",
                    customdata=cd,
                    y_unit=" Å",
                    y_range=(0.0, 30.0),
                    dropped_tilts=drops,
                    theme=JOURNEY_THEME,
                ),
            )
        )
    return charts, stats


def _alignment_section(title: str, df, cd: list, drops: list[float]) -> dict:
    """Refined shift and tilt offset, as `_render_alignment_plots` draws them."""
    tilts = _safe_floats(df["rlnTomoNominalStageTiltAngle"])
    charts: list[dict] = []
    if "rlnTomoXShiftAngst" in df.columns and "rlnTomoYShiftAngst" in df.columns:
        xs = _safe_floats(df["rlnTomoXShiftAngst"])
        ys = _safe_floats(df["rlnTomoYShiftAngst"])
        mag = [
            (x * x + y * y) ** 0.5 if x is not None and y is not None else None for x, y in zip(xs, ys, strict=False)
        ]
        if _is_meaningful_series(mag):
            charts.append(
                _chart(
                    "Refined shift",
                    build_per_tilt_chart(
                        tilts,
                        [
                            {"name": "Shift magnitude", "y": mag},
                            {"name": "X shift", "y": xs},
                            {"name": "Y shift", "y": ys},
                        ],
                        y_label="Shift (Å)",
                        customdata=cd,
                        y_unit=" Å",
                        dropped_tilts=drops,
                        theme=JOURNEY_THEME,
                    ),
                )
            )
    if "rlnTomoYTilt" in df.columns:
        yt = _safe_floats(df["rlnTomoYTilt"])
        # The alignment adapter stores the refined tilt negated in rlnTomoYTilt (Journey `_tilt_offsets`).
        offsets = [(-v) - t if v is not None and t is not None else None for v, t in zip(yt, tilts, strict=False)]
        if any(v is not None for v in offsets):
            charts.append(
                _chart(
                    "Tilt offset",
                    build_per_tilt_chart(
                        tilts,
                        [{"name": "Refined − nominal", "y": offsets}],
                        y_label="Tilt offset (°)",
                        customdata=cd,
                        y_unit="°",
                        dropped_tilts=drops,
                        theme=JOURNEY_THEME,
                    ),
                )
            )
    return {"title": title, "stats": [], "charts": charts}


def _ts_sections(state, project_path: Path, ts: str, thumbs: dict[str, str], committed: bool) -> list[dict]:
    """The Journey's per-tilt sections of one tilt series, one per job that measured it."""
    verdicts, drops = _filter_marks(project_path, ts, committed)
    sections: list[dict] = []
    for iid in state.pipeline_order:
        jm = state.jobs.get(iid)
        if jm is None or jm.execution_status != JobStatus.SUCCEEDED:
            continue
        name = instance_display_name(iid, jm)
        if jm.job_type == JobType.FS_MOTION_CTF:
            df = fsm_registry_df(project_path, iid, ts)
            if df is None:
                continue
            tracks = fsm_motion_tracks(project_path, iid, ts)
            for t in tracks:
                t["thumb"] = thumbs.get(Path(t["frame"]).name, "")
            sec = _motion_section(df, _customdata(df, verdicts, thumbs), tracks, drops)
        elif jm.job_type in _ALIGNERS:
            df = alignment_registry_df(project_path, iid, ts)
            if df is None:
                continue
            sec = _alignment_section("Alignment", df, _customdata(df, verdicts, thumbs), drops)
        elif jm.job_type == JobType.TS_CTF:
            df = tsctf_registry_df(project_path, iid, ts)
            if df is None:
                continue
            tilts = _safe_floats(df["rlnTomoNominalStageTiltAngle"])
            charts, stats = _ctf_charts(df, tilts, _customdata(df, verdicts, thumbs), drops, [])
            sec = {"title": "CTF after alignment", "stats": stats, "charts": charts}
        else:
            continue
        if sec["charts"] or sec["stats"]:
            sec["job"] = name
            sections.append(sec)
    return sections


def _tilt_entry(t: dict, pics: _Pictures) -> dict:
    s = t["state"]
    return {
        "n": s.number,
        "angle": s.angle,
        "frame": t["key"],
        "img": pics.add(t["png"], f"img/tilts/{_slug(Path(t['png']).stem)}.jpg", TILT_PX) if t["png"] else None,
        "excluded": s.excluded,
        "dark": s.is_dark,
        "tip": t["tip"] or t["key"],
    }


def _tomograms(
    state, project_path: Path, tilt_series: list, tilt_ctx: dict, pics: _Pictures
) -> tuple[list[dict], list[dict]]:
    """``(tomograms, pick layers)``: one entry per tilt series the wall or the tilt views know, in
    acquisition order — its pictures (one per reconstruction version), its pick dots, its tilts and
    its per-tilt chart sections."""
    rows, layers, _features = collect_rows(state, project_path, tilt_series)
    png_dir = Path(state.tilt_filter_png_dir) if state.tilt_filter_png_dir else project_path / "TiltFilter" / "png"
    groups, _facts = collect_tilt_groups(tilt_series, png_dir, tilt_ctx)
    by_ts_group = {g["ts"]: g for g in groups}
    committed = tilt_filter_committed(state)

    tomos: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        seen.add(row["ts"])
        tomos.append(_tomo_entry(state, project_path, row["ts"], row, by_ts_group.get(row["ts"]), committed, pics))
    # Tilt series without a tomogram (not reconstructed yet) still have their tilts and charts.
    for g in groups:
        if g["ts"] not in seen:
            tomos.append(_tomo_entry(state, project_path, g["ts"], None, g, committed, pics))
    tomos.sort(key=lambda t: (t["order"], t["ts"]))
    pick_layers = [
        {
            "id": la["id"],
            "species": la["species"],
            "label": la["label"],
            "color": la["color"],
            "n": la["n"],
            # The reconstruction the picks were made on: a layer draws over that version only, as on the
            # wall (`TomoGalleryPage._layer_shown`); None = not known, drawn over every version.
            "recon": la["recon"],
        }
        for la in layers
    ]
    return tomos, pick_layers


def _tomo_entry(
    state, project_path: Path, ts: str, row: dict | None, group: dict | None, committed: bool, pics: _Pictures
) -> dict:
    slug = _slug(ts)
    sources = []
    missing = []
    for key, src in (row or {}).get("sources", {}).items():
        if src["png"] is None:
            missing.append(key)
            continue
        k = _slug(key)
        sources.append(
            {
                "key": key,
                "caption": src["caption"],
                "recon": src.get("recon"),
                "img": pics.add(src["png"], f"img/tomo/{slug}__{k}.jpg", TOMO_PX),
                "thumb": pics.add(src["png"], f"img/tomo/{slug}__{k}_tile.jpg", TILE_PX),
            }
        )
    tilts = [_tilt_entry(t, pics) for t in (group or {}).get("tilts", [])]
    # Hover cards in the charts show the tilt's picture: by frame basename, as the Journey keys them.
    thumbs: dict[str, str] = {}
    for t, raw in zip(tilts, (group or {}).get("tilts", []), strict=True):
        if t["img"] and raw["mrc"]:
            thumbs[Path(raw["mrc"]).name] = t["img"]
        if t["img"]:
            thumbs[raw["key"]] = t["img"]
    try:
        sections = _ts_sections(state, project_path, ts, thumbs, committed)
        note = None
    except Exception:
        logger.exception("Snapshot: the per-tilt charts of %s could not be read", ts)
        sections, note = [], "The per-tilt charts could not be read when this snapshot was made."
    label = row["label"] if row else group["label"]
    order = row["order"] if row else group["order"]
    return {
        "ts": ts,
        "label": label,
        "order": list(order),
        "aspect": list(row["aspect"]) if row and row["aspect"] else None,
        "sources": sources,
        "missing_sources": missing,
        "picks": [{"layer": p["id"], "n": p["n"], "dots": p["dots"]} for p in (row or {}).get("picks", [])],
        "tilts": tilts,
        "n_excluded": sum(1 for t in tilts if t["excluded"]),
        "sections": sections,
        "note": note,
    }


# ── Particles ─────────────────────────────────────────────────────────────────


def _readout(project_path: Path, state, iid: str, pics: _Pictures) -> dict | None:
    """A Class 3D or Reconstruct particle job's readout, as its Results tab shows it."""
    jm = state.jobs[iid]
    try:
        rec = readout_record(project_path, state, iid)
    except (FileNotFoundError, ValueError) as e:
        return {"iid": iid, "name": instance_display_name(iid, jm), "note": str(e)}
    d = rec.data
    out: dict[str, Any] = {"iid": iid, "name": instance_display_name(iid, jm), "kind": jm.job_type.value}
    slug = _slug(iid)
    if jm.job_type == JobType.CLASS3D:
        out["facts"] = [
            ("Particles", f"{d.get('n_particles', 0):,}"),
            ("Classes", str(d.get("n_classes", 0))),
            ("Final iteration", str(d.get("final_iteration", ""))),
        ]
        sizes = {c["class"]: c["n"] for c in d.get("classes") or []}
        out["maps"] = [
            {
                "label": f"Class {m['class']} · {sizes.get(m['class'], 0):,} particles",
                "img": pics.add(m["png"], f"img/maps/{slug}_class{m['class']:03d}.png", 0),
            }
            for m in d.get("class_maps") or []
            if m.get("png")
        ]
        its, per = class_iteration_table(rec)
        convergence = build_class_convergence_chart(its, per)
        out["charts"] = [_chart("Class distribution by iteration", convergence)] if its else []
    else:
        fsc = d.get("fsc") or {}
        facts = []
        for name, c in fsc.items():
            for thr, res in (c.get("resolution") or {}).items():
                # None: the curve never falls below the threshold, the resolution is at Nyquist or better.
                facts.append((f"FSC {thr} ({name})", f"{res:.1f} Å" if res else "at Nyquist"))
        if d.get("voxel"):
            facts.append(("Voxel", f"{d['voxel']:.2f} Å"))
        out["facts"] = facts
        out["maps"] = (
            [{"label": "merged.mrc · XY / XZ / YZ", "img": pics.add(d["slices_png"], f"img/maps/{slug}_merged.png", 0)}]
            if d.get("slices_png")
            else []
        )
        out["charts"] = [_chart("Half-map FSC", build_fsc_chart(dict(fsc)))] if fsc else []
        if d.get("note"):
            out["note"] = d["note"]
    return out


def _species(state, project_path: Path, pics: _Pictures) -> list[dict]:
    out = []
    for idx, sp in enumerate(state.species_registry):
        ov = species_overview(state, project_path, sp.id)
        readouts = []
        for iid in state.pipeline_order:
            jm = state.jobs.get(iid)
            if jm is None or jm.job_type not in (JobType.CLASS3D, JobType.RECONSTRUCT_PARTICLE):
                continue
            if getattr(jm, "species_id", None) not in (sp.id, None, ""):
                continue
            if jm.execution_status != JobStatus.SUCCEEDED:
                continue
            readouts.append(_readout(project_path, state, iid, pics))
        out.append(
            {
                "id": sp.id,
                "name": sp.name,
                "color": species_color(sp, idx),
                "diameter": sp.diameter_ang,
                "symmetry": sp.symmetry,
                "notes": sp.notes,
                "n_tomos_with_picks": ov.n_tomos_with_picks,
                "n_picks": ov.n_picks,
                "n_kept": ov.n_kept,
                "lists": [
                    {"tomo": r.tomo_name, "label": r.label, "type": r.list_type, "count": r.count, "kept": r.kept}
                    for r in ov.rows
                ],
                "readouts": [r for r in readouts if r],
            }
        )
    # Particle jobs no species claims (a project without a species registry) still show.
    claimed = {r["iid"] for s in out for r in s["readouts"]}
    loose = [
        _readout(project_path, state, iid, pics)
        for iid in state.pipeline_order
        if iid not in claimed
        and (jm := state.jobs.get(iid)) is not None
        and jm.job_type in (JobType.CLASS3D, JobType.RECONSTRUCT_PARTICLE)
        and jm.execution_status == JobStatus.SUCCEEDED
    ]
    if loose:
        out.append(
            {"id": "", "name": "Particle jobs", "color": "#64748b", "lists": [], "readouts": [r for r in loose if r]}
        )
    return out


# ── The whole of it ───────────────────────────────────────────────────────────


def collect(state, project_path: Path, tilt_series: list, tilt_ctx: dict) -> Collected:
    """Everything the snapshot shows. `tilt_series` / `tilt_ctx`: `registry_tilt_series` and
    `tilt_context`, read on the event loop. Disk only; safe in a thread."""
    project_path = Path(project_path)
    pics = _Pictures()
    jobs = _jobs(state, project_path)
    tomos, layers = _tomograms(state, project_path, tilt_series, tilt_ctx, pics)
    species = _species(state, project_path, pics)
    n_tomos = sum(1 for t in tomos if t["sources"])
    data = {
        "overview": _overview(state, project_path, len(tomos), n_tomos),
        "jobs": jobs,
        "tomograms": tomos,
        "pick_layers": layers,
        "species": species,
    }
    return Collected(data=data, images=pics.images)


def missing_slabs(state, project_path: Path, tilt_series: list) -> list[tuple[str, Path, Path]]:
    """``[(tomogram, slab PNG, volume)]`` of every wall picture not rendered yet, which the snapshot
    renders first (the gallery's `xy_slab_render`, into the same file the wall would). Disk only."""
    from ui.particles.pick_viewer import _recon_slab_paths

    rows, _layers, _features = collect_rows(state, project_path, tilt_series)
    out = []
    for row in rows:
        for src in row["sources"].values():
            if src["png"] is None and src["kick"] is not None:
                cache_dir, mrc = src["kick"]
                out.append((row["ts"], _recon_slab_paths(cache_dir, row["ts"])[0], Path(mrc)))
    return out

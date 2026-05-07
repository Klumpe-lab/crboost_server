"""
Candidate-preview dialog v4 — interactive Plotly charts.

For each candidate-extract job we show:
  - Project-level pick-yield bar chart (Plotly bar) — sorted desc, click bars
    to navigate to that tomogram (nice-to-have, falls back to the sidebar).
  - Sidebar tomogram list.
  - Per-tomogram main view with three Plotly figures:
      * X/Y scatter with the WarpTools tomogram-preview PNG as a backdrop
        (so picks land *on* the volume render, not over an empty bbox).
      * X/Z scatter — exposes Z-stratification artifacts.
      * Score histogram — distribution + mean line.
  - 3dmod copy-command for hands-on inspection in the field-standard viewer.

All plots are interactive client-side: hover shows pick (idx, world_xyz, score),
zoom + pan + box-select are free with Plotly. No server-side image rendering.
"""

from __future__ import annotations

import json
import logging
import traceback
import urllib.parse
import uuid
from pathlib import Path
from typing import Optional

import pandas as pd
from nicegui import ui, run

from services.models_base import JobStatus, JobType
from services.project_state import get_project_state
from services.tilt_series.build import _infer_position
from services.visualization.imod_vis import generate_candidate_vis
from services.visualization.preview_orchestrator import generate_candidate_previews, read_preview_manifest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------


def _candidate_extract_instances(state) -> list[tuple[str, object]]:
    out: list[tuple[str, object]] = []
    for instance_id, job_model in state.jobs.items():
        if getattr(job_model, "job_type", None) == JobType.TEMPLATE_EXTRACT_PYTOM:
            out.append((instance_id, job_model))
    return sorted(out, key=lambda kv: kv[0])


def _job_dir_for(instance_id: str, job_model, project_path: Path) -> Optional[Path]:
    rjn = getattr(job_model, "relion_job_name", None)
    if rjn:
        d = project_path / rjn.rstrip("/")
        if d.is_dir():
            return d
    state = get_project_state()
    mapped = (state.job_path_mapping or {}).get(instance_id)
    if mapped:
        d = project_path / mapped.rstrip("/")
        if d.is_dir():
            return d
    return None


def _read_tomograms_table(tomograms_star: Path) -> Optional[pd.DataFrame]:
    if not tomograms_star.exists():
        return None
    try:
        import starfile

        data = starfile.read(tomograms_star, always_dict=True)
        for v in data.values():
            if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns:
                return v
    except Exception as e:
        logger.warning("Could not read %s: %s", tomograms_star, e)
    return None


def _resolve_volume_for_3dmod(tomo_row: pd.Series, project_path: Path) -> Optional[Path]:
    if "rlnTomoReconstructedTomogram" not in tomo_row.index:
        return None
    p = Path(str(tomo_row["rlnTomoReconstructedTomogram"]))
    if not p.is_absolute():
        p = project_path / p
    f32 = p.with_name(p.stem + "_f32.mrc")
    if f32.exists():
        return f32
    if p.exists():
        return p
    return None


def _vis_asset_url(asset_path: str) -> str:
    # mtime-keyed cache-buster: when the atlas/manifest regenerates, the URL
    # changes, so the browser doesn't keep serving a stale copy from disk
    # cache against an unchanged path. Same trick as static/main.css in main.py.
    try:
        v = int(Path(asset_path).stat().st_mtime)
    except OSError:
        v = 0
    return f"/api/vis-asset?path={urllib.parse.quote(asset_path, safe='')}&v={v}"


def _position_label(tomo_name: str) -> tuple[str, tuple[int, int]]:
    stage, beam = _infer_position(tomo_name)
    if stage == 0:
        return tomo_name.rsplit("_", 1)[-1], (stage, beam)
    return f"Pos {stage} · Beam {beam}", (stage, beam)


def has_any_extract_jobs() -> bool:
    state = get_project_state()
    return any(_candidate_extract_instances(state))


def has_any_previews_rendered() -> bool:
    state = get_project_state()
    if state.project_path is None:
        return False
    for instance_id, job_model in _candidate_extract_instances(state):
        job_dir = _job_dir_for(instance_id, job_model, state.project_path)
        if not job_dir:
            continue
        if (job_dir / "vis" / "preview" / "manifest.json").exists():
            return True
    return False


# ---------------------------------------------------------------------------
# Plotly figure builders — all return a dict (no plotly Python package needed,
# ui.plotly accepts the JSON spec directly).
# ---------------------------------------------------------------------------


def _empty_fig(message: str) -> dict:
    return {
        "data": [],
        "layout": {
            "annotations": [
                {
                    "text": message,
                    "showarrow": False,
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.5,
                    "y": 0.5,
                    "font": {"color": "#9ca3af", "size": 12},
                }
            ],
            "margin": {"t": 5, "b": 5, "l": 5, "r": 5},
            "paper_bgcolor": "#f8fafc",
            "plot_bgcolor": "#f8fafc",
            "xaxis": {"visible": False},
            "yaxis": {"visible": False},
        },
        "config": {"displaylogo": False, "responsive": True},
    }


def _build_xy_scatter_fig(
    picks: list, tomo_dims_xyz: tuple, score_field: Optional[str], warp_preview_url: Optional[str] = None
) -> dict:
    """Top-down X/Y scatter — clean dot plot, no image overlay.

    The tomogram preview lives in its own card (rendered via ui.image()
    just like the project-wide widget), so this scatter no longer tries to
    paint a Plotly image backdrop. That decoupling resolves the long-running
    Y-flip mismatch: every PNG renders the same way it does in any other
    `<img>` tag, and the scatter is purely about pick spatial distribution.
    """
    x_dim, y_dim, _z_dim = tomo_dims_xyz
    has_scores = picks and "score" in picks[0]
    xs = [p["x"] for p in picks]
    ys = [p["y"] for p in picks]
    custom = [[p["i"], p.get("z", 0), p.get("score")] for p in picks]
    marker: dict = {"size": 6, "line": {"width": 0}, "opacity": 0.85}
    if has_scores:
        marker["color"] = [p.get("score") for p in picks]
        marker["colorscale"] = "Viridis"
        marker["showscale"] = True
        marker["colorbar"] = {
            "title": {"text": score_field or "score", "font": {"size": 9}},
            "thickness": 8,
            "len": 0.7,
            "tickfont": {"size": 9},
            "outlinewidth": 0,
        }
    else:
        marker["color"] = "#fbbf24"

    trace = {
        "type": "scattergl",
        "x": xs,
        "y": ys,
        "mode": "markers",
        "marker": marker,
        "customdata": custom,
        "hovertemplate": (
            "pick #%{customdata[0]}<br>"
            "x=%{x}, y=%{y}, z=%{customdata[1]}"
            + ("<br>score=%{customdata[2]:.4f}" if has_scores else "")
            + "<extra></extra>"
        ),
        "name": "picks",
    }

    layout: dict = {
        "xaxis": {
            "title": {"text": "X (px)", "font": {"size": 10}},
            "range": [0, x_dim],
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 9},
        },
        "yaxis": {
            "title": {"text": "Y (px)", "font": {"size": 10}},
            "range": [0, y_dim],
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 9},
        },
        "margin": {"t": 8, "b": 38, "l": 50, "r": 8},
        "paper_bgcolor": "white",
        "plot_bgcolor": "#f8fafc",
        "showlegend": False,
        "shapes": [
            {
                "type": "rect",
                "xref": "x",
                "yref": "y",
                "x0": 0,
                "y0": 0,
                "x1": x_dim,
                "y1": y_dim,
                "line": {"color": "#cbd5e1", "width": 0.8, "dash": "dash"},
                "layer": "above",
            }
        ],
    }
    return {"data": [trace], "layout": layout, "config": {"displaylogo": False, "responsive": True}}


def _build_xz_scatter_fig(
    picks: list, tomo_dims_xyz: tuple, score_field: Optional[str], xz_preview_url: Optional[str] = None
) -> dict:
    """Side view: X/Z scatter (Z is short → wide-and-short layout). When the
    central-Y-slab preview PNG is available, draw it underneath as the data-
    area backdrop, mirroring the WarpTools-PNG treatment of the X/Y view."""
    x_dim, _y_dim, z_dim = tomo_dims_xyz
    has_scores = picks and "score" in picks[0]
    xs = [p["x"] for p in picks]
    zs = [p["z"] for p in picks]
    custom = [[p["i"], p.get("y", 0), p.get("score")] for p in picks]
    marker: dict = {"size": 5, "line": {"width": 0}, "opacity": 0.85}
    if has_scores:
        marker["color"] = [p.get("score") for p in picks]
        marker["colorscale"] = "Viridis"
        marker["showscale"] = False
    else:
        marker["color"] = "#fbbf24"

    trace = {
        "type": "scattergl",
        "x": xs,
        "y": zs,
        "mode": "markers",
        "marker": marker,
        "customdata": custom,
        "hovertemplate": (
            "pick #%{customdata[0]}<br>"
            "x=%{x}, z=%{y}, y=%{customdata[1]}"
            + ("<br>score=%{customdata[2]:.4f}" if has_scores else "")
            + "<extra></extra>"
        ),
        "name": "picks",
    }
    layout: dict = {
        "xaxis": {
            "title": {"text": "X (px)", "font": {"size": 10}},
            "range": [0, x_dim],
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 9},
        },
        "yaxis": {
            "title": {"text": "Z (px)", "font": {"size": 10}},
            "range": [0, z_dim],
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 9},
        },
        "margin": {"t": 8, "b": 38, "l": 50, "r": 8},
        "paper_bgcolor": "white",
        "plot_bgcolor": "#0f172a" if xz_preview_url else "white",
        "showlegend": False,
        "shapes": [
            {
                "type": "rect",
                "xref": "x",
                "yref": "y",
                "x0": 0,
                "y0": 0,
                "x1": x_dim,
                "y1": z_dim,
                "line": {"color": "#cbd5e1", "width": 0.8, "dash": "dash"},
            }
        ],
    }
    if xz_preview_url:
        # See _build_xy_scatter_fig — same anchor-at-(0, top) convention so
        # the PNG's top edge sits at z=z_dim. The PNG itself is flipud'd
        # server-side so its row 0 corresponds to z=z_dim (IMOD-up).
        layout["images"] = [
            {
                "source": xz_preview_url,
                "xref": "x",
                "yref": "y",
                "x": 0,
                "y": z_dim,
                "sizex": x_dim,
                "sizey": z_dim,
                "sizing": "stretch",
                "opacity": 0.85,
                "layer": "below",
            }
        ]
    return {"data": [trace], "layout": layout, "config": {"displaylogo": False, "responsive": True}}


def _build_score_hist_fig(picks: list, score_field: Optional[str]) -> dict:
    """Score distribution as a histogram with a dashed mean marker."""
    scores = [p.get("score") for p in picks if p.get("score") is not None]
    if not scores:
        return _empty_fig("no score column in candidates.star")
    mean_v = sum(scores) / len(scores)
    return {
        "data": [
            {
                "type": "histogram",
                "x": scores,
                "nbinsx": 30,
                "marker": {"color": "#4338ca"},
                "hovertemplate": "%{x}<br>%{y} picks<extra></extra>",
            }
        ],
        "layout": {
            "xaxis": {"title": {"text": score_field or "score", "font": {"size": 10}}, "tickfont": {"size": 9}},
            "yaxis": {"title": {"text": "count", "font": {"size": 10}}, "tickfont": {"size": 9}},
            # Top margin needs room for the inline mean label which sits inside
            # the plot area at y=0.95 (paper coords). With t:6 the label was
            # being clipped at the plot top edge.
            "margin": {"t": 8, "b": 38, "l": 50, "r": 8},
            "bargap": 0.05,
            "paper_bgcolor": "white",
            "plot_bgcolor": "white",
            "shapes": [
                {
                    "type": "line",
                    "xref": "x",
                    "yref": "paper",
                    "x0": mean_v,
                    "x1": mean_v,
                    "y0": 0,
                    "y1": 1,
                    "line": {"color": "#9ca3af", "width": 1.2, "dash": "dash"},
                }
            ],
            "annotations": [
                {
                    "text": f"mean {mean_v:.4f}",
                    "xref": "x",
                    "yref": "paper",
                    "x": mean_v,
                    "y": 0.96,
                    "showarrow": False,
                    "yanchor": "top",
                    "xanchor": "left",
                    "xshift": 4,
                    "font": {"size": 9, "color": "#6b7280"},
                    "bgcolor": "rgba(255,255,255,0.85)",
                }
            ],
        },
        "config": {"displaylogo": False, "responsive": True},
    }


def _read_picks_json(path: Path) -> dict:
    if not path or not Path(path).exists():
        return {"picks": [], "tomo_dims_xyz_px": [0, 0, 0], "score_field": None, "n": 0}
    try:
        return json.loads(Path(path).read_text())
    except Exception as e:
        logger.warning("Failed to load picks.json %s: %s", path, e)
        return {"picks": [], "tomo_dims_xyz_px": [0, 0, 0], "score_field": None, "n": 0}


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------


_CB_CSS = """
.cb-tomo-list { width: 240px; min-width: 240px; flex-shrink: 0; }
.cb-sidebar-toolbar {
    padding: 6px 8px;
    border-bottom: 1px solid #e5e7eb;
    background: #f8fafc;
    flex-shrink: 0;
}
.cb-tomo-row {
    display: flex; flex-direction: column; gap: 1px;
    padding: 6px 10px; border-bottom: 1px solid #f1f1f1;
    cursor: pointer; font-size: 12px;
}
.cb-tomo-row:hover { background: #f8fafc; }
.cb-tomo-row.selected { background: #eef2ff; border-left: 3px solid #6366f1; padding-left: 7px; }
.cb-tomo-row .cb-tomo-pos { font-weight: 600; color: #1f2937; }
.cb-tomo-row .cb-tomo-name { font-family: ui-monospace, monospace; font-size: 9.5px; color: #6b7280;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.cb-tomo-row .cb-tomo-meta { font-size: 10.5px; color: #4b5563; display: flex; gap: 8px; margin-top: 2px; }
.cb-tomo-row .cb-tomo-badge { background: #f1f5f9; padding: 0 5px; border-radius: 4px; font-family: ui-monospace, monospace; }
.cb-tomo-row.status-missing { color: #b45309; }
.cb-tomo-row.status-errored { color: #b91c1c; }
.cb-tomo-row.status-no-preview { color: #6b7280; font-style: italic; }
.cb-main { flex: 1; min-width: 0; padding: 12px; overflow: auto; }
.cb-empty { flex: 1; display: flex; align-items: center; justify-content: center;
    color: #9ca3af; font-size: 13px; padding: 40px; flex-direction: column; gap: 8px; }
.cb-section-title {
    font-size: 10px; text-transform: uppercase; font-weight: 700;
    color: #475569; letter-spacing: 0.4px;
}
.cb-section-card {
    background: #ffffff; border: 1px solid #e5e7eb; border-radius: 6px;
    padding: 10px 12px; margin-bottom: 8px;
}
.cb-section-card-header {
    display: flex; align-items: center; gap: 8px; margin-bottom: 6px;
}
.cb-aspect { width: 100%; }  /* aspect-ratio set inline per plot */
.cb-picks-right {
    border-left: 1px solid #eef2f7;
    padding-left: 14px;
}
@media (max-width: 900px) {
    .cb-picks-right {
        border-left: none; padding-left: 0;
        border-top: 1px solid #eef2f7; padding-top: 10px;
    }
}
.cb-hover-card {
    background: #f8fafc; border: 1px solid #e5e7eb; border-radius: 4px;
    padding: 8px 10px; font-family: ui-monospace, monospace;
    font-size: 11px; color: #374151;
    display: grid; grid-template-columns: max-content 1fr;
    gap: 4px 12px; align-items: baseline;
}
.cb-hover-card .cb-hover-key {
    color: #6b7280; text-transform: uppercase; font-size: 9px;
    font-weight: 700; letter-spacing: 0.4px;
}
.cb-hover-card .cb-hover-val { color: #1f2937; }
.cb-hover-card.cb-hover-empty { color: #9ca3af; font-style: italic; }
.cb-gallery-grid {
    display: grid;
    /* 96px display tiles. The atlas on disk is 192-px source per tile (for
       crisp Lanczos-resampled thumbnails) but we render at half scale here.
       The CSS sprite math (background-size + background-position) below
       uses 96-pixel offsets and a half-scale background-size so each
       display tile maps to exactly one source tile. */
    grid-template-columns: repeat(auto-fill, 96px);
    gap: 4px;
    padding: 6px 2px 6px 2px;
    justify-content: start;
}
.cb-gallery-tile {
    position: relative;
    width: 96px;
    height: 96px;
    background-color: #0f172a;
    background-repeat: no-repeat;
    border-radius: 3px;
    cursor: pointer;
    overflow: hidden;
    border: 2px solid transparent;
    transition: transform 0.06s ease;
}
.cb-gallery-tile:hover {
    transform: scale(1.04);
    border-color: #c7d2fe;
}
.cb-gallery-tile.selected {
    border-color: #4338ca;
    box-shadow: 0 0 0 1px #4338ca, 0 4px 10px rgba(67,56,202,0.25);
}
.cb-gallery-tile .cb-tile-score {
    position: absolute; bottom: 0; right: 0;
    padding: 1px 4px;
    background: rgba(15,23,42,0.72);
    font-family: ui-monospace, monospace;
    font-size: 9px; color: #f8fafc;
    border-top-left-radius: 3px;
}
.cb-gallery-tile .cb-tile-z {
    position: absolute; bottom: 0; left: 0;
    padding: 1px 4px;
    background: rgba(15,23,42,0.55);
    font-family: ui-monospace, monospace;
    font-size: 9px; color: #cbd5e1;
    border-top-right-radius: 3px;
}
.cb-gallery-tile .cb-tile-idx {
    position: absolute; top: 0; right: 0;
    padding: 0 4px;
    background: rgba(15,23,42,0.55);
    font-family: ui-monospace, monospace;
    font-size: 9px; color: #cbd5e1;
    border-bottom-left-radius: 3px;
}
.cb-gallery-tile .cb-tile-rank {
    position: absolute; top: 0; left: 0;
    padding: 0 4px;
    background: rgba(67,56,202,0.85);
    font-family: ui-monospace, monospace;
    font-size: 9px; color: white;
    border-bottom-right-radius: 3px;
}
.cb-gallery-empty {
    padding: 18px;
    text-align: center;
    font-size: 11px; color: #6b7280;
    background: #f8fafc; border-radius: 4px;
    border: 1px dashed #cbd5e1;
}
.cb-tomo-preview {
    /* Aspect-ratio set inline per slice (XY = x_dim/y_dim, XZ = x_dim/z_dim)
       so the box matches the underlying tomogram's projected proportions
       and the image fills it edge-to-edge — no `object-fit: contain`
       letterbox, which means percentage-based marker/ghost positioning is
       pixel-accurate against the data. */
    width: 100%;
    background: #0f172a;
    border-radius: 4px;
    overflow: hidden;
    position: relative;
}
.cb-tomo-preview img {
    width: 100%;
    height: 100%;
    object-fit: cover;
    display: block;
}
.cb-preview-stack {
    display: flex;
    flex-direction: column;
    gap: 6px;
    width: 100%;
}
.cb-pick-marker {
    position: absolute;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    border: 2px solid #fff;
    background: rgba(244, 114, 182, 0.95);
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.55), 0 0 8px rgba(244, 114, 182, 0.55);
    transform: translate(-50%, -50%);
    pointer-events: none;
    opacity: 0;
    transition: opacity 0.08s ease, left 0.05s linear, top 0.05s linear;
    z-index: 6;
    left: 0;
    top: 0;
}
.cb-pick-ghost {
    position: absolute;
    width: 5px;
    height: 5px;
    border-radius: 50%;
    background: rgba(67, 56, 202, 0.55);
    box-shadow: 0 0 0 0.5px rgba(255, 255, 255, 0.25);
    transform: translate(-50%, -50%);
    pointer-events: none;
    z-index: 4;
}
.cb-overlay-hide .cb-pick-ghost { display: none; }
.cb-preview-toolbar {
    display: flex; align-items: center; gap: 10px;
    font-size: 10px; color: #475569;
    padding: 2px 0 4px 0;
}
.cb-gallery-scroll {
    /* Self-scrolling so very many picks don't push the page tall — the dialog
       chrome stays in place and the gallery owns its own viewport. */
    overflow-y: auto;
    max-height: 75vh;
    padding-right: 4px;
}
.cb-failures-list {
    font-size: 10px; color: #6b7280;
    font-family: ui-monospace, monospace;
    max-height: 90px; overflow-y: auto;
    background: #f8fafc; border: 1px solid #e5e7eb;
    border-radius: 3px; padding: 6px 8px;
}
.cb-failures-list .cb-failure-row {
    display: flex; gap: 8px;
    padding: 1px 0;
    border-bottom: 1px dashed #e5e7eb;
}
.cb-failures-list .cb-failure-row:last-child { border-bottom: none; }
.cb-failures-list .cb-failure-i { color: #ef4444; min-width: 32px; }
"""


def _ensure_assets_loaded() -> None:
    ui.add_head_html(f"<style>{_CB_CSS}</style>")


# ---------------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------------


def open_candidate_preview_dialog() -> None:
    state = get_project_state()
    if state.project_path is None:
        ui.notify("No project loaded.", type="warning")
        return
    instances = _candidate_extract_instances(state)
    if not instances:
        ui.notify("No candidate-extract jobs in this project yet.", type="info")
        return

    project_path = Path(state.project_path)
    _ensure_assets_loaded()

    with (
        ui.dialog().props("maximized") as dlg,
        ui.card().classes("w-full h-full bg-gray-50 overflow-hidden flex flex-col p-0"),
    ):
        # Floating close button — no header bar. The previous "Candidate
        # Previews" + project label header was dead vertical space; the
        # close affordance is the only thing worth keeping at the top, and
        # an absolute-positioned button reclaims the row entirely.
        (
            ui.button(icon="close", on_click=dlg.close)
            .props("flat dense round size=sm")
            .classes("text-gray-500 absolute z-10")
            .style("top: 6px; right: 8px;")
        )

        body = ui.element("div").classes("w-full flex-1 overflow-hidden").style("min-height: 0;")
        with body:
            if len(instances) == 1:
                instance_id, job_model = instances[0]
                _render_instance_section(instance_id, job_model, project_path)
            else:
                with ui.tabs().classes("w-full bg-white border-b") as tabs:
                    for instance_id, _ in instances:
                        ui.tab(instance_id).classes("text-xs")
                with (
                    ui.tab_panels(tabs, value=instances[0][0])
                    .classes("w-full bg-gray-50 h-full")
                    .style("height: 100%;")
                ):
                    for instance_id, job_model in instances:
                        with ui.tab_panel(instance_id).classes("p-0 h-full"):
                            _render_instance_section(instance_id, job_model, project_path)

    dlg.open()


# ---------------------------------------------------------------------------
# Per-instance section
# ---------------------------------------------------------------------------


@ui.refreshable
def _render_instance_section(instance_id: str, job_model, project_path: Path) -> None:
    job_dir = _job_dir_for(instance_id, job_model, project_path)
    job_succeeded = job_model.execution_status == JobStatus.SUCCEEDED
    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))

    if not job_dir:
        ui.label("Job directory not found on disk.").classes("text-xs text-red-500 px-4 py-3")
        return

    if not job_succeeded and not (job_dir / "candidates.star").exists():
        ui.label(
            "Extraction hasn't produced candidates.star yet. Previews can be generated once the job completes."
        ).classes("text-xs text-gray-500 italic px-4 py-3")
        return

    rows = _collect_tomos_for_instance(job_dir, project_path)
    if not rows:
        ui.label("No tomograms found for this job.").classes("text-xs text-gray-500 italic px-4 py-3")
        return

    manifest = read_preview_manifest(job_dir) or {}
    has_imod_models = (job_dir / "vis" / "imodPartRad").exists() and any(
        (job_dir / "vis" / "imodPartRad").glob("*.mod")
    )

    with ui.column().classes("w-full h-full gap-0").style("height: 100%;"):
        # No top header bar — instance label, status badges, and the regen
        # buttons all moved into the sidebar to reclaim vertical space for
        # the per-tomogram dashboard.
        ok_rows = [r for r in rows if r["status"] == "ok"]
        selected_state = {"tomo": ok_rows[0]["tomo_name"] if ok_rows else rows[0]["tomo_name"]}

        with ui.row().classes("w-full flex-1 gap-0 overflow-hidden").style("min-height: 0;"):
            sidebar = ui.column().classes("cb-tomo-list bg-white border-r border-gray-200 gap-0 h-full")
            with sidebar:
                # Compact toolbar: instance label + tiny regen buttons. Tooltips
                # explain the difference between "render missing" (incremental,
                # uses the manifest cache) and "force" (re-renders every tomo,
                # bypasses cache).
                with ui.column().classes("cb-sidebar-toolbar gap-1"):
                    with ui.row().classes("w-full items-center gap-1"):
                        ui.label(instance_id).classes(
                            "text-[10px] font-bold text-gray-700 font-mono truncate"
                        ).style("flex: 1; min-width: 0;")
                        if not job_succeeded:
                            ui.label(str(job_model.execution_status)).classes(
                                "text-[9px] text-amber-600 font-mono"
                            )
                    if diameter:
                        ui.label(f"diameter {diameter:.0f} Å").classes("text-[9px] text-gray-500 font-mono")
                    with ui.row().classes("w-full items-center gap-1 flex-wrap"):
                        gen_missing_btn = (
                            ui.button(
                                "Render new",
                                icon="auto_fix_high",
                                on_click=lambda: _handle_generate_for_instance(
                                    instance_id, job_model, job_dir, project_path, False, gen_missing_btn
                                ),
                            )
                            .props("dense no-caps unelevated size=sm")
                            .classes("bg-purple-50 text-purple-700 border border-purple-200")
                            .style("padding: 0 8px; min-height: 22px;")
                            .tooltip("Render previews for tomograms without a fresh manifest entry. Uses cache.")
                        )
                        regen_btn = (
                            ui.button(
                                "Force",
                                icon="refresh",
                                on_click=lambda: _handle_generate_for_instance(
                                    instance_id, job_model, job_dir, project_path, True, regen_btn
                                ),
                            )
                            .props("dense no-caps flat size=sm")
                            .classes("text-gray-500")
                            .style("padding: 0 6px; min-height: 22px;")
                            .tooltip("Re-render every tomogram, bypass cache. Use after renderer changes.")
                        )
                        imod_btn = (
                            ui.button(
                                "IMOD" if has_imod_models else "IMOD",
                                icon="scatter_plot",
                                on_click=lambda: _handle_generate_imod_for_instance(
                                    instance_id, job_model, job_dir, project_path, imod_btn
                                ),
                            )
                            .props("dense no-caps unelevated size=sm")
                            .classes("bg-blue-50 text-blue-700 border border-blue-200")
                            .style("padding: 0 8px; min-height: 22px;")
                            .tooltip(
                                "Regenerate IMOD .mod overlays" if has_imod_models else "Generate IMOD .mod overlays"
                            )
                        )

                tomo_list = ui.element("div").classes("cb-tomo-list-rows w-full overflow-auto")
                tomo_list.style("flex: 1; min-height: 0;")
            main_area = ui.element("div").classes("cb-main").style("min-height: 0;")

            def render_main():
                main_area.clear()
                tomo_name = selected_state["tomo"]
                row = next((r for r in rows if r["tomo_name"] == tomo_name), None)
                if row is None:
                    return
                with main_area:
                    _render_main_panel(row, manifest)

            def render_list():
                tomo_list.clear()
                with tomo_list:
                    for r in rows:
                        _render_tomo_list_row(r, selected_state, render_list, render_main)

            render_list()
            render_main()


def _render_tomo_list_row(r: dict, selected_state: dict, render_list, render_main) -> None:
    cls = "cb-tomo-row"
    if selected_state["tomo"] == r["tomo_name"]:
        cls += " selected"
    if r["status"] == "missing-volume":
        cls += " status-missing"
    elif r["status"] == "errored":
        cls += " status-errored"
    elif r["status"] != "ok":
        cls += " status-no-preview"

    def on_click():
        selected_state["tomo"] = r["tomo_name"]
        render_list()
        render_main()

    with ui.element("div").classes(cls).on("click", on_click):
        ui.label(r["position_label"]).classes("cb-tomo-pos")
        ui.label(r["tomo_name"]).classes("cb-tomo-name")
        meta_parts = []
        if r.get("n_picks") is not None:
            meta_parts.append(f"N={r['n_picks']}")
        if r.get("score_range"):
            sr = r["score_range"]
            meta_parts.append(f"{sr[0]:.2f}–{sr[1]:.2f}")
        if r["status"] == "missing-volume":
            meta_parts.append("no volume")
        elif r["status"] == "errored":
            meta_parts.append("errored")
        elif r["status"] == "no-preview":
            meta_parts.append("not rendered")
        if meta_parts:
            with ui.element("div").classes("cb-tomo-meta"):
                for p in meta_parts:
                    ui.label(p).classes("cb-tomo-badge")


# ---------------------------------------------------------------------------
# Main panel for a single tomogram
# ---------------------------------------------------------------------------


def _render_main_panel(row: dict, manifest: dict) -> None:
    """Per-tomogram dashboard — the per-TS megawidget.

    Layout philosophy: one stack of cards per tomogram, each card owned by a
    pipeline stage. Cards render only when their backing data exists, so a
    partial pipeline shows a partial dashboard. The flagship card shows the
    tomogram preview alongside the subtomo gallery; future stages (tilt
    filter, CTF, template-match score volume, class3d, ...) plug in below
    as additional cards.
    """
    if row["status"] == "errored":
        with ui.element("div").classes("cb-empty"):
            ui.icon("error_outline", size="36px").classes("text-red-500")
            ui.label("Render error: " + (row.get("error") or "unknown")).classes("text-xs text-red-600")
        return
    if row["status"] not in ("ok", "missing-volume"):
        with ui.element("div").classes("cb-empty"):
            ui.icon("hourglass_empty", size="36px")
            ui.label("Preview not generated yet.").classes("text-xs")
            ui.label("Use Generate Missing Previews above.").classes("text-[11px] italic")
        return

    entry = (manifest.get("tomograms") or {}).get(row["tomo_name"]) or {}
    has_atlas = bool(entry.get("cutout_atlas") and entry.get("cutout_index"))

    with ui.column().classes("w-full gap-2"):
        _render_tomo_header_section(row, manifest)
        _render_3dmod_section(row)
        # Flagship card: preview + gallery side-by-side. When subtomo data
        # isn't available, the card renders preview-only (no gallery).
        _render_preview_and_gallery_section(row, entry, manifest)
        # Secondary spatial-context cards. Only show the picks-only scatter
        # views when there's NO gallery to look at — once the user has the
        # gallery, the scatter-with-no-backdrop is just clutter.
        if not has_atlas:
            _render_picks_scatter_section(row, entry, manifest)


def _render_tomo_header_section(row: dict, manifest: dict) -> None:
    entry = (manifest.get("tomograms") or {}).get(row["tomo_name"]) or {}
    score_field = manifest.get("score_field")
    with ui.row().classes("w-full items-baseline gap-3 flex-wrap"):
        ui.label(row["position_label"]).classes("text-sm font-bold text-gray-800")
        ui.label(row["tomo_name"]).classes("text-[10px] font-mono text-gray-500")
        ui.space()
        if row["score_range"]:
            ui.label(f"score {row['score_range'][0]:.3f}–{row['score_range'][1]:.3f}").classes(
                "text-[11px] text-gray-500 font-mono"
            )
        if entry.get("score_mean") is not None:
            ui.label(f"mean {entry['score_mean']:.3f}").classes("text-[11px] text-gray-500 font-mono")
        ui.label(f"N={row['n_picks']}").classes("text-[11px] font-mono")
        if score_field:
            ui.label(f"colored by {score_field}").classes("text-[10px] text-gray-500 italic")
        if entry.get("warp_tomo_preview"):
            ui.label("· Warp tomogram backdrop").classes("text-[10px] text-emerald-600 italic")
    if row["status"] == "missing-volume":
        ui.label("No reconstructed tomogram on disk for 3dmod — picks plot still works.").classes(
            "text-[11px] text-amber-700 italic"
        )


def _render_3dmod_section(row: dict) -> None:
    if not row.get("vol_path"):
        return
    mod = row.get("mod_path")
    cmd = f"3dmod {row['vol_path']} {mod}" if mod else f"3dmod {row['vol_path']}"
    with ui.row().classes("w-full items-center gap-1"):
        ui.label("3dmod").classes("text-[9px] uppercase font-bold text-gray-400 w-12")
        ui.input(value=cmd).props("dense outlined readonly hide-bottom-space").classes(
            "text-xs font-mono flex-1"
        ).style("min-width: 0;")
        if not mod:
            ui.icon("info", size="14px").classes("text-gray-400").tooltip(
                "No IMOD model overlay — use Generate IMOD Models above"
            )
        ui.button(
            icon="content_copy",
            on_click=lambda c=cmd: (ui.clipboard.write(c), ui.notify("Copied", type="positive", timeout=800)),
        ).props("flat dense round size=sm").classes("text-gray-500 hover:text-gray-800").tooltip("Copy 3dmod command")


def _render_preview_and_gallery_section(row: dict, entry: dict, manifest: dict) -> None:
    """Flagship section: tomogram preview (left, half-ish) + subtomo cutout
    gallery (right, scrollable, ~half). The preview side is a vertical stack
    of two slices — XY top-down on top, XZ side-view underneath — both with
    a faint always-on overlay of every pick (toggleable) and a bright hover
    marker that the gallery drives synchronously across both slices. The
    section renders gracefully without a Warp PNG (left side falls back to
    a placeholder; XZ stays empty) and without a subtomo atlas (gallery
    placeholder)."""
    warp_path = entry.get("warp_tomo_preview")
    warp_url = _vis_asset_url(warp_path) if warp_path else None
    xz_path = entry.get("xz_preview")
    xz_url = _vis_asset_url(xz_path) if xz_path else None
    index_path = entry.get("cutout_index")
    atlas_meta = _read_atlas_index(Path(index_path)) if index_path else None

    picks_json_path = entry.get("picks_json")
    picks_data = (
        _read_picks_json(Path(picks_json_path))
        if picks_json_path
        else {"picks": [], "tomo_dims_xyz_px": [1, 1, 1]}
    )
    picks = picks_data.get("picks", [])
    tomo_dims = picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1]
    x_dim = max(int(tomo_dims[0]), 1)
    y_dim = max(int(tomo_dims[1]), 1)
    z_dim = max(int(tomo_dims[2]), 1)

    # Per-render unique ids so the gallery can locate "its" preview markers
    # via JS without colliding with anything else on the page.
    nonce = uuid.uuid4().hex[:8]
    xy_host_id = f"cb-tomo-xy-{nonce}"
    xz_host_id = f"cb-tomo-xz-{nonce}"
    gallery_id = f"cb-gallery-{nonce}"

    # Aspect-ratios drive the slice container shapes so we can ditch
    # `object-fit: contain` and percentage-position picks pixel-accurately
    # against the underlying data. Cap aspects so a pathologically thin
    # XZ (z_dim ≪ x_dim) doesn't produce a 41-pixel-tall strip.
    xy_aspect = max(0.3, min(3.0, x_dim / y_dim))
    xz_aspect = max(1.5, min(8.0, x_dim / z_dim))

    # Pre-compute fractional positions for ALL picks, separately for each
    # slice. Both PNGs follow IMOD-up convention (origin at bottom), but DOM
    # tops grow downward — so for both slices we invert the vertical axis
    # (`1 - y/y_dim` for XY, `1 - z/z_dim` for XZ). This was the missing
    # flip that made markers/ghosts appear mirror-imaged on first wire-up.
    pick_xy_frac: list[tuple[int, float, float]] = []
    pick_xz_frac: list[tuple[int, float, float]] = []
    for p in picks:
        try:
            i = int(p.get("i"))
        except (TypeError, ValueError):
            continue
        fx = max(0.0, min(1.0, float(p.get("x", 0)) / x_dim))
        fy = max(0.0, min(1.0, float(p.get("y", 0)) / y_dim))
        fz = max(0.0, min(1.0, float(p.get("z", 0)) / z_dim))
        pick_xy_frac.append((i, fx, 1.0 - fy))
        pick_xz_frac.append((i, fx, 1.0 - fz))

    with ui.element("div").classes("cb-section-card w-full"):
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("photo_library", size="14px").classes("text-indigo-600")
            ui.label("Tomogram & picks").classes("cb-section-title")
            ui.space()
            n_ok = entry.get("cutout_n_ok") or 0
            n_pick = row.get("n_picks") or 0
            n_fail = len(entry.get("cutout_failures") or [])
            if atlas_meta is not None:
                badge = f"{n_ok}/{n_pick} cutouts"
                if n_fail:
                    badge += f" · {n_fail} failures"
                ui.label(badge).classes("text-[10px] text-gray-500 font-mono")

        # Total preview height = column_w / xy_aspect + column_w / xz_aspect.
        # Cap by a target total so XY+XZ together fit in viewport while still
        # leaving room for the gallery on the right.
        preview_target_total_h = 540
        col_max_w_calc = int(preview_target_total_h / max(0.05, (1.0 / xy_aspect + 1.0 / xz_aspect)))
        col_max_w = max(360, min(720, col_max_w_calc))

        with ui.row().classes("w-full gap-3 items-start flex-wrap"):
            # LEFT: stacked XY+XZ slices. The wrapper carries the overlay-
            # toggle class so the checkbox can show/hide every ghost in one
            # CSS-class flip without touching individual nodes.
            left_col = ui.column().classes("gap-1").style(
                f"flex: 1 1 360px; min-width: 320px; max-width: {col_max_w}px;"
            )
            with left_col:
                with ui.row().classes("cb-preview-toolbar"):
                    ui.label("Tomogram slices").classes("cb-section-title")
                    ui.space()
                    if pick_xy_frac:
                        show_all = ui.checkbox("Show all picks", value=True).props("dense").classes("text-[10px]")

                        def _toggle_overlay(e):
                            cls = "cb-overlay-hide"
                            if e.value:
                                left_col.classes(remove=cls)
                            else:
                                left_col.classes(add=cls)

                        show_all.on_value_change(_toggle_overlay)

                with ui.element("div").classes("cb-preview-stack"):
                    # XY top-down slice
                    xy_host = ui.element("div").classes("cb-tomo-preview")
                    xy_host._props["id"] = xy_host_id
                    xy_host.style(f"aspect-ratio: {x_dim}/{y_dim};")
                    with xy_host:
                        if warp_url:
                            ui.image(warp_url)
                            for i, fx, fy in pick_xy_frac:
                                g = ui.element("div").classes("cb-pick-ghost")
                                g._props["data-pick-idx"] = str(i)
                                g.style(f"left: {fx * 100:.3f}%; top: {fy * 100:.3f}%;")
                            ui.element("div").classes("cb-pick-marker")
                        else:
                            with ui.column().classes("absolute inset-0 items-center justify-center text-center"):
                                ui.icon("photo", size="36px").classes("text-gray-500")
                                ui.label("No WarpTools preview yet").classes("text-[11px] text-gray-400")
                                ui.label("(run ts_reconstruct first)").classes("text-[10px] text-gray-500 italic")

                    # XZ side-view slice
                    xz_host = ui.element("div").classes("cb-tomo-preview")
                    xz_host._props["id"] = xz_host_id
                    xz_host.style(f"aspect-ratio: {x_dim}/{z_dim};")
                    with xz_host:
                        if xz_url:
                            ui.image(xz_url)
                            for i, fx, fz_top in pick_xz_frac:
                                g = ui.element("div").classes("cb-pick-ghost")
                                g._props["data-pick-idx"] = str(i)
                                g.style(f"left: {fx * 100:.3f}%; top: {fz_top * 100:.3f}%;")
                            ui.element("div").classes("cb-pick-marker")
                        else:
                            with ui.column().classes("absolute inset-0 items-center justify-center text-center"):
                                ui.icon("layers", size="28px").classes("text-gray-500")
                                ui.label("No X/Z slab preview").classes("text-[10px] text-gray-500 italic")

            # RIGHT: subtomo gallery, scrollable so a high-pick-count tomogram
            # doesn't push the dialog tall and the previews stay visible.
            with ui.column().classes("gap-2 cb-picks-right").style("flex: 1 1 420px; min-width: 320px;"):
                if atlas_meta is None:
                    _render_gallery_placeholder(row)
                else:
                    _render_gallery_body(
                        row,
                        entry,
                        manifest,
                        atlas_meta,
                        xy_host_id,
                        xz_host_id,
                        gallery_id,
                        bool(warp_url),
                        bool(xz_url),
                    )


def _render_gallery_placeholder(row: dict) -> None:
    ui.label("Subtomo gallery").classes("cb-section-title")
    with ui.element("div").classes("cb-gallery-empty"):
        ui.icon("hourglass_empty", size="20px").classes("text-amber-500 block mx-auto mb-1")
        ui.html(
            "Subtomo cutout atlas not built — needs a <code>SUBTOMO_EXTRACTION</code> "
            "job whose particles match these picks by Å coordinates.",
            sanitize=False,
        )


def _render_gallery_body(
    row: dict,
    entry: dict,
    manifest: dict,
    atlas_meta: dict,
    xy_host_id: str,
    xz_host_id: str,
    gallery_id: str,
    has_xy: bool,
    has_xz: bool,
) -> None:
    """The scrollable subtomo gallery half of the flagship card.

    Sort dropdown drives the display order; each tile is a CSS-sprite of the
    atlas at its assigned (row, col); click a tile to mark it selected (the
    border lights up). The picks-data lookup is needed for sort-by-score and
    sort-by-Z because picks.json is the canonical post-sort metadata.

    `xy_host_id` / `xz_host_id` are the ids of the two preview slice wrappers
    whose `.cb-pick-marker` children get driven by tile hover. `gallery_id`
    is this gallery's grid container, used as the delegation root. `has_xy`
    / `has_xz` gate which slice gets a marker update — there's no point
    positioning a marker on a slice that isn't rendered.
    """
    picks_json_path = entry.get("picks_json")
    picks_data = (
        _read_picks_json(Path(picks_json_path)) if picks_json_path else {"picks": [], "tomo_dims_xyz_px": [0, 0, 0]}
    )
    picks = picks_data.get("picks", [])
    tomo_dims = picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1]
    pixel_size_ang = entry.get("pixel_size_ang")

    atlas_url = _vis_asset_url(entry["cutout_atlas"])
    cols = int(atlas_meta.get("cols", 8))
    rows = int(atlas_meta.get("rows", 1))
    cutout_index = atlas_meta.get("index", {})  # {"<i>": [r, c], ...}
    failures = entry.get("cutout_failures") or atlas_meta.get("failures") or []

    # Per-pick fractional positions for both slices, keyed by pick index.
    # Both axes inverted vertically (IMOD-up origin → DOM top-down) — must
    # match the convention used in `_render_preview_and_gallery_section` so
    # marker and ghost end up at the same spot.
    x_dim = max(int(tomo_dims[0]), 1)
    y_dim = max(int(tomo_dims[1]), 1)
    z_dim = max(int(tomo_dims[2]), 1)
    pick_xy_frac: dict[int, list[float]] = {}
    pick_xz_frac: dict[int, list[float]] = {}
    for k in cutout_index:
        try:
            i = int(k)
        except (TypeError, ValueError):
            continue
        if 0 <= i < len(picks):
            p = picks[i]
            fx = max(0.0, min(1.0, float(p.get("x", 0)) / x_dim))
            fy_top = max(0.0, min(1.0, 1.0 - float(p.get("y", 0)) / y_dim))
            fz_top = max(0.0, min(1.0, 1.0 - float(p.get("z", 0)) / z_dim))
            pick_xy_frac[i] = [fx, fy_top]
            pick_xz_frac[i] = [fx, fz_top]

    state = {"selected_idx": None, "sort_mode": "best"}

    with ui.row().classes("w-full items-center gap-2"):
        ui.label("Subtomo gallery").classes("cb-section-title")
        ui.space()
        sort_select = (
            ui.select(
                options={"best": "Best score", "worst": "Worst score", "z": "By Z (deep → shallow)"}, value="best"
            )
            .props("dense outlined")
            .classes("text-xs")
            .style("min-width: 180px;")
        )

    grid_container = ui.element("div").classes("cb-gallery-scroll w-full")
    grid_container._props["id"] = gallery_id

    # Hover-pick details — wired to the click on a tile (since there's no
    # scatter to hover anymore in the gallery view). Reusing the same labels
    # dict so the field names line up with the scatter-primary fallback.
    ui.label("Selected pick").classes("cb-section-title mt-2")
    hover_labels = _render_hover_card_skeleton()

    if failures:
        with ui.expansion(f"Why {len(failures)} tile(s) failed").classes("w-full text-[10px]"):
            with ui.element("div").classes("cb-failures-list"):
                for f in failures:
                    with ui.element("div").classes("cb-failure-row"):
                        ui.label(f"#{f.get('i', '?')}").classes("cb-failure-i")
                        ui.label(str(f.get("reason", "unknown")))

    def _sorted_pick_indices() -> list:
        mode = state["sort_mode"]
        available = [int(k) for k in cutout_index.keys()]
        if mode == "best":
            return sorted(available)
        if mode == "worst":
            return sorted(available, reverse=True)
        if mode == "z":

            def _z(i):
                return picks[i].get("z", 0) if 0 <= i < len(picks) else 0

            return sorted(available, key=_z, reverse=True)
        return sorted(available)

    def _on_tile_click(pick_idx: int):
        state["selected_idx"] = pick_idx
        # Synthesize a fake hover-event payload so the existing hover-card
        # update path can render the selected pick's details — keeps one
        # detail-rendering path instead of duplicating it for clicks.
        _update_hover_card(
            type("E", (), {"args": {"points": [{"customdata": [pick_idx]}]}})(), picks, pixel_size_ang, hover_labels
        )
        _refresh_grid()

    # CSS sprite math at 96px display. The atlas on disk has 192-px source
    # tiles, but we render the bg image at half scale so each 96-px display
    # tile maps to exactly one source tile. Pixel-exact math everywhere:
    #   bg-size  = (cols × 96) by (rows × 96)
    #   bg-pos   = (-c × 96) by (-r × 96)
    # Tile div size (96 × 96) is in the .cb-gallery-tile CSS rule.
    DISPLAY_TILE_PX = 96
    bg_w = cols * DISPLAY_TILE_PX
    bg_h = rows * DISPLAY_TILE_PX
    bg_size_css = f"{bg_w}px {bg_h}px"

    def _refresh_grid():
        grid_container.clear()
        with grid_container, ui.element("div").classes("cb-gallery-grid"):
            for rank, pick_idx in enumerate(_sorted_pick_indices()):
                pos = cutout_index.get(str(pick_idx))
                if not pos:
                    continue
                r, c = pos
                bg_x = -c * DISPLAY_TILE_PX
                bg_y = -r * DISPLAY_TILE_PX
                pick = picks[pick_idx] if 0 <= pick_idx < len(picks) else None
                cls = "cb-gallery-tile"
                if state["selected_idx"] == pick_idx:
                    cls += " selected"
                style = (
                    f"background-image: url({atlas_url}); "
                    f"background-size: {bg_size_css}; "
                    f"background-position: {bg_x}px {bg_y}px;"
                )
                tile = ui.element("div").classes(cls).style(style)
                tile._props["data-pick-idx"] = str(pick_idx)
                # title= gives a native browser tooltip with full per-pick
                # metadata on hover-pause; the corner labels below are the
                # at-a-glance read.
                if pick is not None:
                    parts = [f"#{pick_idx}"]
                    if pick.get("score") is not None:
                        parts.append(f"score={pick['score']:.4f}")
                    parts.append(f"x={int(pick.get('x', 0))}")
                    parts.append(f"y={int(pick.get('y', 0))}")
                    parts.append(f"z={int(pick.get('z', 0))}")
                    tile._props["title"] = "  ".join(parts)
                tile.on("click", lambda _e, i=pick_idx: _on_tile_click(i))
                with tile:
                    ui.html(f"#{rank + 1}", sanitize=False).classes("cb-tile-rank")
                    ui.html(f"{pick_idx}", sanitize=False).classes("cb-tile-idx")
                    if pick is not None and pick.get("z") is not None:
                        ui.html(f"z{int(pick['z'])}", sanitize=False).classes("cb-tile-z")
                    if pick and pick.get("score") is not None:
                        ui.html(f"{pick['score']:.3f}", sanitize=False).classes("cb-tile-score")

    sort_select.on_value_change(lambda e: (state.update(sort_mode=e.value or "best"), _refresh_grid()))
    _refresh_grid()

    # Wire client-side hover markers: tile mouseover → position both the XY
    # and XZ markers at the pick's fractional coords. Slice containers are
    # sized via `aspect-ratio` so the image fills exactly — percentage
    # positioning is pixel-accurate, no letterbox math needed. Delegation
    # on the grid container survives _refresh_grid() rebuilding tiles on
    # sort change.
    if (has_xy and pick_xy_frac) or (has_xz and pick_xz_frac):
        slices = []
        if has_xy and pick_xy_frac:
            slices.append({"id": xy_host_id, "picks": pick_xy_frac})
        if has_xz and pick_xz_frac:
            slices.append({"id": xz_host_id, "picks": pick_xz_frac})
        ui.run_javascript(
            "setTimeout(function(){"
            f"  const grid = document.getElementById({gallery_id!r});"
            "  if (!grid) return;"
            f"  const slices = {json.dumps(slices)};"
            "  const wired = slices.map(function(s){"
            "    const host = document.getElementById(s.id);"
            "    if (!host) return null;"
            "    return {marker: host.querySelector('.cb-pick-marker'), picks: s.picks};"
            "  }).filter(function(x){ return x && x.marker; });"
            "  if (!wired.length) return;"
            "  function place(idx) {"
            "    wired.forEach(function(w){"
            "      const xy = w.picks[idx];"
            "      if (!xy) { w.marker.style.opacity = '0'; return; }"
            "      w.marker.style.left = (xy[0] * 100).toFixed(3) + '%';"
            "      w.marker.style.top = (xy[1] * 100).toFixed(3) + '%';"
            "      w.marker.style.opacity = '1';"
            "    });"
            "  }"
            "  function hideAll() { wired.forEach(function(w){ w.marker.style.opacity = '0'; }); }"
            "  grid.addEventListener('mouseover', function(e){"
            "    const t = e.target.closest && e.target.closest('.cb-gallery-tile[data-pick-idx]');"
            "    if (!t || !grid.contains(t)) return;"
            "    place(t.getAttribute('data-pick-idx'));"
            "  });"
            "  grid.addEventListener('mouseout', function(e){"
            "    const t = e.target.closest && e.target.closest('.cb-gallery-tile[data-pick-idx]');"
            "    if (!t) return;"
            "    const next = e.relatedTarget && e.relatedTarget.closest "
            "      && e.relatedTarget.closest('.cb-gallery-tile[data-pick-idx]');"
            "    if (!next) hideAll();"
            "  });"
            "}, 80);"
        )


def _render_picks_scatter_section(row: dict, entry: dict, manifest: dict) -> None:
    """Scatter-only fallback: X/Y + X/Z + score histogram. Renders only when
    the gallery isn't available — once the gallery is showing, the scatters
    are clutter."""
    picks_json_path = entry.get("picks_json")
    picks_data = (
        _read_picks_json(Path(picks_json_path)) if picks_json_path else {"picks": [], "tomo_dims_xyz_px": [0, 0, 0]}
    )
    picks = picks_data.get("picks", [])
    tomo_dims = tuple(picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1])
    score_field = manifest.get("score_field")
    pixel_size_ang = entry.get("pixel_size_ang")
    xz_url = _vis_asset_url(entry["xz_preview"]) if entry.get("xz_preview") else None

    x_dim, y_dim, z_dim = (max(int(d), 1) for d in tomo_dims)
    xy_aspect = min(2.5, max(0.5, x_dim / y_dim))
    xz_aspect = min(5.0, max(1.5, x_dim / z_dim))
    xy_target_h = 380
    xy_max_w = min(640, max(280, int(xy_target_h * xy_aspect)))
    xz_target_h = 220
    xz_max_w = min(720, max(280, int(xz_target_h * xz_aspect)))

    with ui.element("div").classes("cb-section-card w-full"):
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("scatter_plot", size="14px").classes("text-indigo-600")
            ui.label("Pick distribution").classes("cb-section-title")
            ui.label(f"  ({row['n_picks']} picks)").classes("text-[10px] text-gray-400")
            ui.space()
            ui.label("· no subtomo extraction yet — gallery view unavailable").classes(
                "text-[10px] text-amber-700 italic"
            )

        with ui.row().classes("w-full gap-3 items-stretch flex-wrap"):
            with ui.column().classes("gap-1").style(f"flex: 1 1 320px; min-width: 280px; max-width: {xy_max_w}px;"):
                ui.label("X / Y top-down").classes("cb-section-title")
                with ui.element("div").classes("cb-aspect").style(f"aspect-ratio: {xy_aspect};"):
                    xy_plot = ui.plotly(_build_xy_scatter_fig(picks, tomo_dims, score_field)).style(
                        "width: 100%; height: 100%;"
                    )
            with (
                ui.column()
                .classes("gap-2 cb-picks-right")
                .style(f"flex: 1 1 320px; min-width: 280px; max-width: {xz_max_w}px;")
            ):
                ui.label("X / Z side").classes("cb-section-title")
                with ui.element("div").classes("cb-aspect").style(f"aspect-ratio: {xz_aspect};"):
                    xz_plot = ui.plotly(_build_xz_scatter_fig(picks, tomo_dims, score_field, xz_url)).style(
                        "width: 100%; height: 100%;"
                    )
                ui.label("Score distribution").classes("cb-section-title")
                ui.plotly(_build_score_hist_fig(picks, score_field)).style("width: 100%; height: 160px;")
                ui.label("Hovered pick").classes("cb-section-title")
                hover_labels = _render_hover_card_skeleton()

                def on_hover(e, _picks=picks, _ps=pixel_size_ang, _labels=hover_labels):
                    _update_hover_card(e, _picks, _ps, _labels)

                xy_plot.on("plotly_hover", on_hover, throttle=0.08)
                xz_plot.on("plotly_hover", on_hover, throttle=0.08)


def _read_atlas_index(index_path: Path) -> Optional[dict]:
    if not index_path or not Path(index_path).exists():
        return None
    try:
        meta = json.loads(Path(index_path).read_text())
        # Treat an atlas with no successful tiles as "not ready" — UI then
        # shows the placeholder + failure list instead of a blank grid.
        if not meta.get("index"):
            return None
        return meta
    except Exception as e:
        logger.warning("Could not parse cutout index %s: %s", index_path, e)
        return None


def _render_hover_card_skeleton() -> dict:
    """Build the static hover-details card; return the labels keyed by field
    so on-hover updates can drive them via .set_text()."""
    labels: dict = {}
    with ui.element("div").classes("cb-hover-card cb-hover-empty") as card:
        ui.label("idle").classes("cb-hover-key")
        ui.label("hover any pick to see details").classes("cb-hover-val")
    labels["__card"] = card
    # Replace the content lazily on first hover (keeps the empty state
    # compact). The on-hover handler clears `card` and rebuilds the rows.
    return labels


def _update_hover_card(e, picks: list, pixel_size_ang, labels: dict) -> None:
    """Populate the hover-details card from a plotly_hover event."""
    args = getattr(e, "args", None) or {}
    points = args.get("points") or []
    if not points:
        return
    p = points[0] or {}
    cd = p.get("customdata") or []
    if not cd:
        return
    try:
        idx = int(cd[0])
    except (TypeError, ValueError):
        return
    if idx < 0 or idx >= len(picks):
        return
    pick = picks[idx]

    card = labels.get("__card")
    if card is None:
        return
    # First-hover transition: drop the "idle" placeholder, render the rows.
    if not labels.get("__populated"):
        card.clear()
        card.classes(remove="cb-hover-empty")
        with card:
            for key in ("idx", "px", "ang", "score", "z%-tile", "nn"):
                ui.label(key).classes("cb-hover-key")
                val_label = ui.label("—").classes("cb-hover-val")
                labels[key] = val_label
        labels["__populated"] = True

    labels["idx"].set_text(f"#{pick['i']}")
    labels["px"].set_text(f"{pick['x']}, {pick['y']}, {pick['z']}")
    if pixel_size_ang:
        ax = pick["x"] * pixel_size_ang
        ay = pick["y"] * pixel_size_ang
        az = pick["z"] * pixel_size_ang
        labels["ang"].set_text(f"{ax:.0f}, {ay:.0f}, {az:.0f}")
    else:
        labels["ang"].set_text("(no pixel size)")
    if pick.get("score") is not None:
        labels["score"].set_text(f"{pick['score']:.4f}")
    else:
        labels["score"].set_text("—")
    if pick.get("z_pct") is not None:
        labels["z%-tile"].set_text(f"{pick['z_pct']:.0f}")
    else:
        labels["z%-tile"].set_text("—")
    if pick.get("nn_px") is not None:
        nn_px = pick["nn_px"]
        if pixel_size_ang:
            labels["nn"].set_text(f"{nn_px:.1f} px ({nn_px * pixel_size_ang:.0f} Å)")
        else:
            labels["nn"].set_text(f"{nn_px:.1f} px")
    else:
        labels["nn"].set_text("—")


# Future pipeline-stage sections plug in here, e.g.:
#   _render_tilt_filter_section(row, manifest)   -- tilt-quality stats
#   _render_ctf_section(row, manifest)           -- per-tilt CTF distribution
#   _render_reconstruct_section(row, manifest)   -- volume preview thumbnail
#   _render_template_match_section(row, manifest) -- pre-extract score map
#   _render_subtomo_extract_section(row, manifest) -- per-pick cutout gallery
# Each becomes a sibling cb-section-card under the main column.


# ---------------------------------------------------------------------------
# Tomogram list assembly
# ---------------------------------------------------------------------------


def _collect_tomos_for_instance(job_dir: Path, project_path: Path) -> list[dict]:
    tomograms_star = job_dir / "tomograms.star"
    tomo_df = _read_tomograms_table(tomograms_star)
    manifest = read_preview_manifest(job_dir) or {}
    tomo_entries = manifest.get("tomograms") or {}
    summary = manifest.get("summary") or {}
    missing_volume = set(summary.get("missing_volume") or [])
    errored_map = {e["tomo"]: e.get("error", "") for e in (summary.get("errored") or [])}

    rows: list[dict] = []
    if tomo_df is None:
        for tomo_name, entry in tomo_entries.items():
            label, (stage, beam) = _position_label(tomo_name)
            rows.append(
                {
                    "tomo_name": tomo_name,
                    "position_label": label,
                    "stage": stage,
                    "beam": beam,
                    "vol_path": entry.get("tomo_mrc"),
                    "mod_path": str(job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"),
                    "n_picks": entry.get("n_picks"),
                    "score_range": entry.get("score_range"),
                    "status": "ok" if entry.get("picks_json") else "no-preview",
                    "error": None,
                }
            )
    else:
        for _, tomo_row in tomo_df.iterrows():
            tomo_name = str(tomo_row["rlnTomoName"])
            label, (stage, beam) = _position_label(tomo_name)
            entry = tomo_entries.get(tomo_name) or {}
            vol_path = _resolve_volume_for_3dmod(tomo_row, project_path)
            mod_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
            if tomo_name in missing_volume and not entry.get("picks_json"):
                status = "missing-volume"
            elif tomo_name in errored_map:
                status = "errored"
            elif entry.get("picks_json"):
                status = "ok"
            else:
                status = "no-preview"
            rows.append(
                {
                    "tomo_name": tomo_name,
                    "position_label": label,
                    "stage": stage,
                    "beam": beam,
                    "vol_path": str(vol_path) if vol_path else None,
                    "mod_path": str(mod_path) if mod_path.exists() else None,
                    "n_picks": entry.get("n_picks"),
                    "score_range": entry.get("score_range"),
                    "status": status,
                    "error": errored_map.get(tomo_name),
                }
            )
    rows.sort(key=lambda r: (r["stage"], r["beam"], r["tomo_name"]))
    return rows


# ---------------------------------------------------------------------------
# Generation handlers
# ---------------------------------------------------------------------------


def _make_imod_command_runner():
    from services.computing.container_service import get_container_service

    container_service = get_container_service()

    def runner(cmd: str, cwd: Path) -> None:
        import subprocess

        wrapped = container_service.wrap_command_for_tool(cmd, cwd=cwd, tool_name="imod", additional_binds=[str(cwd)])
        result = subprocess.run(wrapped, shell=True, capture_output=True, text=True, cwd=cwd)
        if result.returncode != 0:
            raise RuntimeError(
                f"Container command failed (rc={result.returncode}): {result.stderr.strip() or result.stdout.strip()}"
            )

    return runner


def _generate_imod_sync(
    candidates_star: Path, tomograms_star: Path, diameter: float, job_dir: Path, project_path: Path
) -> None:
    generate_candidate_vis(
        candidates_star=candidates_star,
        tomograms_star=tomograms_star,
        particle_diameter_ang=diameter,
        output_dir=job_dir,
        command_runner=_make_imod_command_runner(),
        project_root=project_path,
    )


async def _handle_generate_imod_for_instance(
    instance_id: str, job_model, job_dir: Path, project_path: Path, btn
) -> None:
    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        ui.notify(
            "candidates.star or tomograms.star missing — cannot generate IMOD models", type="negative", timeout=4000
        )
        return
    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
    btn.props("loading")
    try:
        await run.io_bound(_generate_imod_sync, candidates_star, tomograms_star, diameter, job_dir, project_path)
        ui.notify("IMOD models generated — 3dmod commands now include the overlay", type="positive", timeout=3000)
    except Exception as e:
        traceback.print_exc()
        ui.notify(f"IMOD generation failed: {e}", type="negative", timeout=5000)
    finally:
        btn.props(remove="loading")
        _render_instance_section.refresh(instance_id, job_model, project_path)


async def _handle_generate_for_instance(
    instance_id: str, job_model, job_dir: Path, project_path: Path, force: bool, btn
) -> None:
    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        ui.notify("candidates.star or tomograms.star missing — cannot render previews", type="negative", timeout=4000)
        return
    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
    state = get_project_state()
    btn.props("loading")
    try:
        summary = await run.io_bound(
            generate_candidate_previews,
            candidates_star,
            tomograms_star,
            diameter,
            job_dir,
            project_path,
            None,
            force,
            state,
        )
        n_new = len(summary["ok"])
        n_cached = len(summary["skipped_cached"])
        n_missing = len(summary["missing_volume"])
        n_err = len(summary["errored"])
        msg = f"Previews: {n_new} rendered, {n_cached} cached"
        if n_missing:
            msg += f", {n_missing} missing volume"
        if n_err:
            msg += f", {n_err} errored"
        ui.notify(msg, type="positive" if not n_err else "warning", timeout=4000)
    except Exception as e:
        traceback.print_exc()
        ui.notify(f"Preview generation failed: {e}", type="negative", timeout=5000)
    finally:
        btn.props(remove="loading")
        _render_instance_section.refresh(instance_id, job_model, project_path)

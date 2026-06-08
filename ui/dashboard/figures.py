"""Journey dashboard — Plotly figure builders.

Pure dict builders for the dashboard's charts: the picks-only XY/XZ scatter +
score histogram (fallback when no subtomo cutout atlas exists) and the per-tilt
metric charts (defocus / motion / shifts / angles), plus the small numeric
helpers that prep series for them. ``ui.plotly()`` takes a JSON dict directly,
so these depend on no plotting package. Extracted from
``ui/tomo_dashboard_dialog.py`` (R0 refactor); py_compile + ruff only.
"""

from __future__ import annotations

from typing import Optional


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


def _build_xy_scatter_fig(picks: list, tomo_dims_xyz: tuple, score_field: Optional[str]) -> dict:
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


def _safe_floats(series) -> list[float]:
    """Coerce a pandas Series to a list of Python floats; non-finite stays as
    None so Plotly draws gaps instead of dropping to the floor."""
    import math

    out: list[float] = []
    for v in series:
        try:
            f = float(v)
        except (TypeError, ValueError):
            out.append(None)
            continue
        if not math.isfinite(f):
            out.append(None)
        else:
            out.append(f)
    return out


def _is_meaningful_series(values: list[float], *, threshold: float = 1e-3) -> bool:
    """True when the column carries real signal — at least one finite value
    AND max-abs above `threshold`. Filters out WarpTools placeholder columns
    (`1e-6` for AccumMotion / CtfMaxResolution; `None` for CtfFigureOfMerit)
    so we don't pollute the dashboard with flat-line plots. See memory
    `project_warp_relion_star_placeholders.md`."""
    finite = [v for v in values if v is not None]
    if not finite:
        return False
    return max(abs(v) for v in finite) >= threshold


def _stats(values: list[float]) -> dict:
    """Median / IQR / count over the non-None entries. Returns a dict with
    keys median, q1, q3, min, max, n."""
    import statistics

    finite = [v for v in values if v is not None]
    if not finite:
        return {"median": None, "q1": None, "q3": None, "min": None, "max": None, "n": 0}
    finite_sorted = sorted(finite)
    n = len(finite_sorted)
    median = statistics.median(finite_sorted)
    if n >= 4:
        q1 = statistics.median(finite_sorted[: n // 2])
        q3 = statistics.median(finite_sorted[(n + 1) // 2 :])
    else:
        q1 = q3 = median
    return {"median": median, "q1": q1, "q3": q3, "min": finite_sorted[0], "max": finite_sorted[-1], "n": n}


def _build_per_tilt_chart(
    x_tilts: list[float],
    series: list[dict],
    *,
    x_label: str = "tilt (°)",
    y_label: str = "",
    h_lines: Optional[list[dict]] = None,
    customdata: Optional[list[list]] = None,
    y_unit: str = "",
    y_range: Optional[tuple[float, float]] = None,
) -> dict:
    """Compact chart: x = tilt angle, y = one or more per-tilt metrics.

    `series`: list of {name, y, color, dash?, mode?} entries. Default mode is
    `markers` — discrete per-tilt estimates connect badly with lines (zigzag
    or tangled) so caller must opt-in via `mode='lines+markers'` when the
    metric varies continuously across tilts (e.g. shifts, refined angles).
    `customdata`: parallel list of [tilt_index, frame_basename, ...] pairs;
    surfaced in hover so users can identify which tilt a point belongs to.
    `y_unit`: short suffix appended to the y value in the hover string
    (e.g. " µm", " Å").
    `y_range`: fixed y-axis [min, max] — locks the axis across tilt-series
    so the same metric is visually comparable. Auto-extends if observed
    data exceeds the bounds (so we never clip outliers).
    """
    traces = []
    for s in series:
        marker_size = s.get("marker_size", 6)
        trace: dict = {
            "type": "scatter",
            "mode": s.get("mode", "markers"),
            "x": x_tilts,
            "y": s["y"],
            "line": {"color": s.get("color", "#4338ca"), "width": s.get("width", 1.4), "dash": s.get("dash", "solid")},
            "marker": {
                "size": marker_size,
                "color": s.get("color", "#4338ca"),
                "line": {"color": "#ffffff", "width": 0.6},
            },
            "name": s["name"],
        }
        if customdata is not None:
            trace["customdata"] = customdata
            trace["hovertemplate"] = (
                f"<b>{s['name']}</b>: %{{y:.3g}}{y_unit}"
                "<br>Tilt #%{customdata[0]} · %{x:.2f}°"
                "<br><span style='font-size:9px;color:#94a3b8'>%{customdata[1]}</span>"
                "<extra></extra>"
            )
        else:
            trace["hovertemplate"] = (
                f"<b>{s['name']}</b>: %{{y:.3g}}{y_unit}<br>Stage angle: %{{x:.2f}}°<extra></extra>"
            )
        traces.append(trace)
    layout: dict = {
        "xaxis": {"title": {"text": x_label, "font": {"size": 9}}, "tickfont": {"size": 8}, "zeroline": False},
        "yaxis": {"title": {"text": y_label, "font": {"size": 9}}, "tickfont": {"size": 8}, "zeroline": False},
        "margin": {"t": 6, "b": 32, "l": 50, "r": 12},
        "paper_bgcolor": "white",
        "plot_bgcolor": "#fafafa",
        "showlegend": len(series) > 1,
        "legend": {"orientation": "h", "x": 0, "y": 1.14, "font": {"size": 9}},
        "hovermode": "x unified",
    }
    if y_range:
        # Auto-extend the fixed range if observed data exceeds it — never
        # clip outliers; lock the axis only when data fits.
        ymin, ymax = float(y_range[0]), float(y_range[1])
        for s in series:
            for v in s.get("y") or []:
                if v is None:
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if fv < ymin:
                    ymin = fv
                if fv > ymax:
                    ymax = fv
        layout["yaxis"]["range"] = [ymin, ymax]
        layout["yaxis"]["autorange"] = False
    if h_lines:
        shapes = []
        annotations = []
        for h in h_lines:
            shapes.append(
                {
                    "type": "line",
                    "xref": "paper",
                    "yref": "y",
                    "x0": 0,
                    "x1": 1,
                    "y0": h["y"],
                    "y1": h["y"],
                    "line": {"color": h.get("color", "#9ca3af"), "width": 1.0, "dash": "dash"},
                }
            )
            if h.get("label"):
                annotations.append(
                    {
                        "text": h["label"],
                        "xref": "paper",
                        "yref": "y",
                        "x": 1,
                        "xanchor": "right",
                        "y": h["y"],
                        "yanchor": "bottom",
                        "showarrow": False,
                        "font": {"size": 8, "color": h.get("color", "#9ca3af")},
                        "bgcolor": "rgba(255,255,255,0.85)",
                    }
                )
        if shapes:
            layout["shapes"] = shapes
        if annotations:
            layout["annotations"] = annotations
    return {
        "data": traces,
        "layout": layout,
        "config": {"displaylogo": False, "responsive": True, "displayModeBar": False},
    }

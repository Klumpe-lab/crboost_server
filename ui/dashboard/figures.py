"""Journey dashboard — chart builders (Plotly for the pick scatters, ECharts for per-tilt).

Pure dict builders for the dashboard's charts: the picks-only XY/XZ scatter +
score histogram (fallback when no subtomo cutout atlas exists) and the per-tilt
metric charts (defocus / motion / shifts / angles), plus the small numeric
helpers that prep series for them. ``ui.plotly()`` takes a JSON dict directly,
so these depend on no plotting package. Extracted from
``ui/tomo_dashboard_dialog.py`` (R0 refactor); py_compile + ruff only.
"""

from __future__ import annotations


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
        "config": {"displaylogo": False, "responsive": True, "displayModeBar": False},
    }


def _build_xy_scatter_fig(picks: list, tomo_dims_xyz: tuple, score_field: str | None) -> dict:
    x_dim, y_dim, _z_dim = tomo_dims_xyz
    # Pad the axes ~2% of each dim so edge picks (x≈0 or x≈x_dim — common in
    # cryo-ET fields) draw their full marker inside the frame; the dashed
    # reference rect stays at the true [0,dim] so it reads as the tomogram bound.
    padx, pady = x_dim * 0.02, y_dim * 0.02
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
            "range": [-padx, x_dim + padx],
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 9},
        },
        "yaxis": {
            "title": {"text": "Y (px)", "font": {"size": 10}},
            "range": [-pady, y_dim + pady],
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
    return {
        "data": [trace],
        "layout": layout,
        "config": {"displaylogo": False, "responsive": True, "displayModeBar": False},
    }


def _build_xz_scatter_fig(
    picks: list, tomo_dims_xyz: tuple, score_field: str | None, xz_preview_url: str | None = None
) -> dict:
    x_dim, _y_dim, z_dim = tomo_dims_xyz
    # ~2% axis padding so edge picks draw their full marker inside the frame;
    # the dashed reference rect stays at the true [0,dim] (see XY builder).
    padx, padz = x_dim * 0.02, z_dim * 0.02
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
            "range": [-padx, x_dim + padx],
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 9},
        },
        "yaxis": {
            "title": {"text": "Z (px)", "font": {"size": 10}},
            "range": [-padz, z_dim + padz],
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
    return {
        "data": [trace],
        "layout": layout,
        "config": {"displaylogo": False, "responsive": True, "displayModeBar": False},
    }


def _build_score_hist_fig(picks: list, score_field: str | None) -> dict:
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
        "config": {"displaylogo": False, "responsive": True, "displayModeBar": False},
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


# ── Per-tilt metric charts: ECharts ──────────────────────────────────────────
# The Journey's per-tilt charts render with ``ui.echart`` (bundled with NiceGUI, no
# CDN): one typographic system — bold axis names, regular everything else, legend
# under the plot, `containLabel` so the plot area fills the tile instead of leaving
# Plotly's default margins empty. The pick scatter/histogram builders above stay on
# Plotly (image underlays + colorbars) and are the pick viewer's, not the Journey's.

# One palette for every chart on the page: accent, ink, and a muted slate for the
# third series (or a fit/trend line).
SERIES_PALETTE = ("#4f46e5", "#0f172a", "#94a3b8")
_SANS = "IBM Plex Sans, sans-serif"
_MONO = "IBM Plex Mono, monospace"

# Hover card: tilt index + stage angle, the frame it came from, the tilt-filter verdict
# when the caller supplied one (customdata[2]), then one line per series at that x.
# `__UNIT__` is substituted with the y unit. Under a ':' key NiceGUI evals it client-side.
_TOOLTIP_FORMATTER_JS = """
(ps) => {
  if (!Array.isArray(ps)) ps = [ps];
  const d = ps[0].data || [];
  const deg = (typeof d[0] === 'number') ? d[0].toFixed(2) + '°' : '';
  let h = '<div style="font-family:IBM Plex Sans,sans-serif;font-size:10px;line-height:1.5;color:#334155">';
  h += '<div style="font-weight:600">' + (d[2] ? 'Tilt ' + d[2] + ' · ' : '') + deg + '</div>';
  if (d[3]) h += '<div style="color:#94a3b8;font-family:IBM Plex Mono,monospace">' + d[3] + '</div>';
  if (d[4]) h += '<div style="color:#6366f1">tilt filter: ' + d[4] + '</div>';
  for (const p of ps) {
    const v = p.data ? p.data[1] : null;
    if (v === null || v === undefined) continue;
    h += '<div>' + p.marker + p.seriesName
      + ' <span style="font-family:IBM Plex Mono,monospace;font-weight:600">'
      + Number(v).toPrecision(3) + '__UNIT__</span></div>';
  }
  return h + '</div>';
}
"""


def build_per_tilt_chart(
    x_tilts: list[float],
    series: list[dict],
    *,
    x_label: str = "Stage tilt (°)",
    y_label: str = "",
    customdata: list[list] | None = None,
    y_unit: str = "",
    y_range: tuple[float, float] | None = None,
) -> dict:
    """ECharts options for a per-tilt chart: x = stage tilt angle, y = one or more
    per-tilt metrics.

    `series`: list of {name, y, color?, mode?, dash?, marker_size?, width?} entries.
    Default mode is `markers` — discrete per-tilt estimates connect badly with lines
    (zigzag or tangled), so a caller opts in with `mode='lines+markers'` / `'lines'`
    for metrics that vary continuously across tilts; line series are drawn in tilt
    order. `customdata`: parallel list of [tilt_index, frame_basename, (verdict)]
    rows surfaced in the hover card. `y_unit`: suffix for hover values (" µm", " Å").
    `y_range`: fixed y-axis [min, max] so the same metric is visually comparable
    across tilt series; auto-extended (never clipped) when the data exceeds it.
    """
    out: list[dict] = []
    for i, s in enumerate(series):
        color = s.get("color") or SERIES_PALETTE[i % len(SERIES_PALETTE)]
        mode = s.get("mode", "markers")
        is_line = "lines" in mode
        pts: list[list] = []
        for j, (x, y) in enumerate(zip(x_tilts, s["y"], strict=False)):
            if x is None:
                continue
            extra = list(customdata[j]) if customdata is not None and j < len(customdata) else []
            # None y stays None → ECharts draws a gap (line) / skips the point (scatter).
            pts.append([x, y, *extra])
        if is_line:
            pts.sort(key=lambda p: p[0])
        entry: dict = {
            "name": s["name"],
            "type": "line" if is_line else "scatter",
            "data": pts,
            "symbol": "circle",
            "symbolSize": s.get("marker_size", 6),
            "itemStyle": {"color": color},
            "emphasis": {"scale": 1.5},
        }
        if is_line:
            entry["showSymbol"] = "markers" in mode
            entry["connectNulls"] = False
            entry["lineStyle"] = {
                "color": color,
                "width": s.get("width", 1.4),
                "type": "dashed" if s.get("dash") in ("dash", "dot") else "solid",
            }
        out.append(entry)

    show_legend = len(series) > 1
    y_axis: dict = {
        "type": "value",
        "scale": True,
        "name": y_label,
        "nameLocation": "end",
        "nameGap": 8,
        "nameTextStyle": {
            "fontFamily": _SANS,
            "fontSize": 10,
            "fontWeight": "bold",
            "color": "#334155",
            "align": "left",
        },
        "axisLabel": {"fontFamily": _MONO, "fontSize": 9, "color": "#64748b"},
        "axisLine": {"show": False},
        "axisTick": {"show": False},
        "splitLine": {"lineStyle": {"color": "#f1f5f9"}},
    }
    if y_range:
        # Auto-extend the fixed range if observed data exceeds it — never clip
        # outliers; lock the axis only when data fits. Pad ~5% of the span so a
        # marker at an extreme doesn't spill past the plot rectangle.
        ymin, ymax = float(y_range[0]), float(y_range[1])
        for s in series:
            for v in s.get("y") or []:
                if v is None:
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                ymin = min(ymin, fv)
                ymax = max(ymax, fv)
        span = ymax - ymin
        pad = span * 0.05 if span > 0 else (abs(ymax) * 0.05 or 1.0)
        y_axis["min"] = round(ymin - pad, 6)
        y_axis["max"] = round(ymax + pad, 6)
        y_axis["scale"] = False

    return {
        "animation": False,
        "color": list(SERIES_PALETTE),
        "grid": {"left": 6, "right": 12, "top": 30, "bottom": 30 if show_legend else 8, "containLabel": True},
        "xAxis": {
            "type": "value",
            "name": x_label,
            "nameLocation": "middle",
            "nameGap": 20,
            "nameTextStyle": {"fontFamily": _SANS, "fontSize": 10, "fontWeight": "bold", "color": "#334155"},
            "axisLabel": {"fontFamily": _MONO, "fontSize": 9, "color": "#64748b"},
            "axisLine": {"lineStyle": {"color": "#cbd5e1"}},
            "axisTick": {"show": False},
            "splitLine": {"show": False},
        },
        "yAxis": y_axis,
        "legend": {
            "show": show_legend,
            "bottom": 0,
            "left": "center",
            "icon": "circle",
            "itemWidth": 8,
            "itemHeight": 8,
            "itemGap": 14,
            "textStyle": {"fontFamily": _SANS, "fontSize": 9, "color": "#475569"},
        },
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "line", "snap": True, "lineStyle": {"color": "#cbd5e1", "type": "dashed"}},
            "backgroundColor": "#ffffff",
            "borderColor": "#e2e8f0",
            "borderWidth": 1,
            "padding": [6, 8],
            "extraCssText": "box-shadow: 0 6px 18px rgba(15,23,42,0.10); border-radius: 5px;",
            ":formatter": _TOOLTIP_FORMATTER_JS.replace("__UNIT__", y_unit),
        },
        "series": out,
    }

"""Journey dashboard — chart builders (Plotly for the pick scatters, ECharts for per-tilt).

Pure dict builders for the dashboard's charts: the picks-only XY/XZ scatter +
score histogram (fallback when no subtomo cutout atlas exists) and the per-tilt
metric charts (defocus / motion / shifts / angles), plus the small numeric
helpers that prep series for them. ``ui.plotly()`` takes a JSON dict directly,
so these depend on no plotting package. The values that style them are a
`ChartTheme`.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, replace

# One palette for every chart on the page: accent, ink, and a muted slate for the
# third series (or a fit/trend line).
SERIES_PALETTE = ("#4f46e5", "#0f172a", "#94a3b8")
_SANS = "IBM Plex Sans, sans-serif"
_MONO = "IBM Plex Mono, monospace"


@dataclass(frozen=True)
class ChartTheme:
    """The values that style a chart. The defaults are the house look, which every builder
    keeps; the Journey's builders (per-tilt, motion tracks, CTF fit, and the pick scatters and
    score histogram) take a theme. Colours are `#rrggbb` (tilt_color mixes them); font
    families are CSS lists without quotes (the hover card splices them into JS strings)."""

    # Series in order (accent, ink, muted); a hollow series draws rings.
    series: tuple[str, ...] = SERIES_PALETTE
    hollow: tuple[bool, ...] = (False, False, False)
    marker_size: float = 6
    marker_opacity: float = 1.0
    # The hovered point: grown by hover_scale, and filled / ringed when those are set.
    hover_scale: float = 1.5
    hover_color: str | None = None
    hover_ring: str | None = None
    # Fit and reference lines (an ECharts line type), and the hover's axis pointer.
    ref_color: str = "#94a3b8"
    ref_type: str = "dashed"
    pointer_color: str = "#cbd5e1"
    # The CTF fit: Warp's measured spectrum and the CTF model.
    fit_measured: str = "#0f172a"
    fit_model: str = "#4f46e5"
    # Signed stage tilt (motion trajectories): the negative pole, 0°, the positive pole.
    tilt_neg: str = "#3730a3"
    tilt_mid: str = "#cbd5e1"
    tilt_pos: str = "#b45309"
    # Axes: tick labels, axis names, the x axis line and the y grid (None hides them), y ticks;
    # the plot rect's top, which holds the y axis name.
    tick_font: str = _MONO
    tick_size: int = 9
    tick_color: str = "#64748b"
    name_font: str = _SANS
    name_size: int = 10
    name_weight: str | int = "bold"
    name_color: str = "#334155"
    x_line: str | None = "#cbd5e1"
    y_grid: str | None = "#f1f5f9"
    y_ticks: int | None = None
    grid_top: int = 30
    # The legend and the hover card (its values in the mono face; the tilt filter's note).
    legend_color: str = "#475569"
    card_font: str = _SANS
    card_size: int = 10
    card_color: str = "#334155"
    card_mono: str = _MONO
    card_note: str = "#6366f1"
    # Plotly: layout font (None leaves Plotly's own), the pick scatters (scores on a colour
    # scale, else one colour; the _dark pair over the X/Z slab image) and the score histogram.
    plotly_font: str | None = None
    plotly_size: int = 12
    pick_scale: str | tuple = "Viridis"
    pick_color: str = "#fbbf24"
    pick_scale_dark: str | tuple = "Viridis"
    pick_color_dark: str = "#fbbf24"
    pick_opacity: float = 0.85
    hist_color: str = "#4338ca"


DEFAULT_THEME = ChartTheme()
# The Journey's: the house values in one font pair, no weight above 600, Plotly in Plex Sans.
JOURNEY_THEME = replace(DEFAULT_THEME, marker_size=7, name_weight=600, plotly_font=_SANS, plotly_size=10)

# ECharts line types → Plotly dash names.
_PLOTLY_DASH = {"dashed": "dash", "dotted": "dot", "solid": "solid"}


def _plotly_layout(layout: dict, theme: ChartTheme) -> dict:
    """`layout` in the theme's type when the theme sets a Plotly font: text and hover labels
    in it, tick numbers in the tick face."""
    if theme.plotly_font:
        layout["font"] = {"family": theme.plotly_font, "size": theme.plotly_size, "color": theme.name_color}
        layout["hoverlabel"] = {"font": {"family": theme.plotly_font, "size": theme.plotly_size}}
        for key in ("xaxis", "yaxis"):
            tick = layout.get(key, {}).get("tickfont")
            if tick is not None:
                tick["family"] = theme.tick_font
    return layout


def _pick_colors(marker: dict, picks: list, has_scores: bool, theme: ChartTheme, *, dark: bool = False) -> None:
    """Colour the pick markers: by score on the theme's scale, else the one pick colour; the
    `dark` pair over an image."""
    if has_scores:
        marker["color"] = [p.get("score") for p in picks]
        scale = theme.pick_scale_dark if dark else theme.pick_scale
        marker["colorscale"] = scale if isinstance(scale, str) else [list(stop) for stop in scale]
    else:
        marker["color"] = theme.pick_color_dark if dark else theme.pick_color


def _empty_fig(message: str, theme: ChartTheme = DEFAULT_THEME) -> dict:
    return {
        "data": [],
        "layout": _plotly_layout(
            {
                "annotations": [
                    {
                        "text": message,
                        "showarrow": False,
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0.5,
                        "y": 0.5,
                        "font": {"color": "#9ca3af", "size": theme.plotly_size},
                    }
                ],
                "margin": {"t": 5, "b": 5, "l": 5, "r": 5},
                "paper_bgcolor": "#f8fafc",
                "plot_bgcolor": "#f8fafc",
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
            },
            theme,
        ),
        "config": {"displaylogo": False, "responsive": True, "displayModeBar": False},
    }


def _build_xy_scatter_fig(
    picks: list, tomo_dims_xyz: tuple, score_field: str | None, theme: ChartTheme = DEFAULT_THEME
) -> dict:
    x_dim, y_dim, _z_dim = tomo_dims_xyz
    # Pad the axes ~2% of each dim so edge picks (x≈0 or x≈x_dim — common in
    # cryo-ET fields) draw their full marker inside the frame; the dashed
    # reference rect stays at the true [0,dim] so it reads as the tomogram bound.
    padx, pady = x_dim * 0.02, y_dim * 0.02
    has_scores = picks and "score" in picks[0]
    xs = [p["x"] for p in picks]
    ys = [p["y"] for p in picks]
    custom = [[p["i"], p.get("z", 0), p.get("score")] for p in picks]
    marker: dict = {"size": 6, "line": {"width": 0}, "opacity": theme.pick_opacity}
    _pick_colors(marker, picks, has_scores, theme)
    if has_scores:
        marker["showscale"] = True
        marker["colorbar"] = {
            "title": {"text": score_field or "score", "font": {"size": 9}},
            "thickness": 8,
            "len": 0.7,
            "tickfont": {"size": 9, **({"family": theme.tick_font} if theme.plotly_font else {})},
            "outlinewidth": 0,
        }

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
        "layout": _plotly_layout(layout, theme),
        "config": {"displaylogo": False, "responsive": True, "displayModeBar": False},
    }


def _build_xz_scatter_fig(
    picks: list,
    tomo_dims_xyz: tuple,
    score_field: str | None,
    xz_preview_url: str | None = None,
    theme: ChartTheme = DEFAULT_THEME,
) -> dict:
    x_dim, _y_dim, z_dim = tomo_dims_xyz
    # ~2% axis padding so edge picks draw their full marker inside the frame;
    # the dashed reference rect stays at the true [0,dim] (see XY builder).
    padx, padz = x_dim * 0.02, z_dim * 0.02
    has_scores = picks and "score" in picks[0]
    xs = [p["x"] for p in picks]
    zs = [p["z"] for p in picks]
    custom = [[p["i"], p.get("y", 0), p.get("score")] for p in picks]
    marker: dict = {"size": 5, "line": {"width": 0}, "opacity": theme.pick_opacity}
    _pick_colors(marker, picks, has_scores, theme, dark=bool(xz_preview_url))
    if has_scores:
        marker["showscale"] = False

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
        "layout": _plotly_layout(layout, theme),
        "config": {"displaylogo": False, "responsive": True, "displayModeBar": False},
    }


def _build_score_hist_fig(picks: list, score_field: str | None, theme: ChartTheme = DEFAULT_THEME) -> dict:
    scores = [p.get("score") for p in picks if p.get("score") is not None]
    if not scores:
        return _empty_fig("no score column in candidates.star", theme)
    mean_v = sum(scores) / len(scores)
    layout = {
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
                "line": {"color": theme.ref_color, "width": 1.2, "dash": _PLOTLY_DASH.get(theme.ref_type, "dash")},
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
    }
    return {
        "data": [
            {
                "type": "histogram",
                "x": scores,
                "nbinsx": 30,
                "marker": {"color": theme.hist_color},
                "hovertemplate": "%{x}<br>%{y} picks<extra></extra>",
            }
        ],
        "layout": _plotly_layout(layout, theme),
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
    so we don't pollute the dashboard with flat-line plots."""
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
# CDN): one typographic system — weighted axis names, regular everything else, legend
# under the plot, and an ECharts-computed plot rect (see `_grid`) so the plot area
# fills the tile instead of leaving Plotly's default margins empty. The pick
# scatter/histogram builders above stay on Plotly (image underlays + colorbars); the
# Particles section draws them, in the Journey and in the full-page pick viewer.

# Do NOT set `grid.containLabel`. It looks like the option that keeps axis decorations
# inside the canvas, and it is the option that crops the axis NAME. In the bundled
# ECharts 6 the grid layout branches on it (`Grid2.prototype.updateGridRect`):
#
#   if (optionContainLabel) layOutGridByOuterBounds(..., "axisLabel", ...)   // labels ONLY
#   else                    layOutGridByOuterBounds(..., outerBoundsContain, ...)
#
# — i.e. containLabel hard-codes the contain mode to "axisLabel", so the x-axis name is
# laid out relative to the axis line and whatever falls past the canvas edge is cropped.
# Omitting it takes the else branch, where `outerBoundsMode: "auto"` (outerBounds = the
# whole canvas) and `outerBoundsContain: "all"` make ECharts shrink the plot rect until
# labels AND names fit. That is exact, so grid left/top/right/bottom below are only a
# starting rect — no hand-computed margin arithmetic.
#
# nameGap is still ours: outerBounds prevents cropping, not collision, and the tick
# labels end ~20 px under the axis line (8 px margin + a 9 px line).
_X_NAME_GAP = 28
# The bottom legend is laid out independently of the grid and outerBounds knows nothing
# about it, so when one is shown the axis name is fenced above it via `outerBounds`.
_LEGEND_EXTENT = 22


def _grid(show_legend: bool, theme: ChartTheme = DEFAULT_THEME) -> dict:
    """Starting plot rect. ECharts shrinks it as needed to fit labels + axis names."""
    grid: dict = {"left": 6, "right": 12, "top": theme.grid_top, "bottom": 8}
    if show_legend:
        grid["outerBounds"] = {"left": 0, "right": 0, "top": 0, "bottom": _LEGEND_EXTENT}
    return grid


# Hover card for the per-tilt charts. Every data row is [x, y, tilt_number, frame_basename,
# verdict, thumb_url] (see tomo_dashboard_dialog._per_tilt_customdata): tilt number + stage
# angle (the frame name is carried for the select hook, not shown — it would set the card's
# width), the tilt-filter verdict when stamped, one line per series at that x,
# and — when fsMotion has run and its thumbnails exist — the motion-corrected tilt image
# itself. `__UNIT__` is substituted with the y unit. Under a ':' key NiceGUI evals it
# client-side. Raw string: the JS carries backslash escapes. `__OPEN__`, `__MONO__` and
# `__NOTE__` take the theme's card type (`_card_js`).
def _card_open(theme: ChartTheme) -> str:
    return (
        f"'<div style=\"font-family:{theme.card_font};font-size:{theme.card_size}px;line-height:1.5;"
        f"color:{theme.card_color}\">'"
    )


_TOOLTIP_CARD_OPEN = _card_open(DEFAULT_THEME)
_TOOLTIP_IMG_JS = (
    "'<img src=\"' + __SRC__ + '\" style=\"display:block;width:336px;height:336px;margin-top:5px;"
    "border-radius:3px;background:#f1f5f9;object-fit:contain\" onerror=\"this.style.display=\\'none\\'\">'"
)
_TOOLTIP_FORMATTER_JS = r"""
(ps) => {
  if (!Array.isArray(ps)) ps = [ps];
  const d = ps[0].data || [];
  const deg = (typeof d[0] === 'number') ? d[0].toFixed(2) + '°' : '';
  let h = __OPEN__;
  h += '<div style="font-weight:600">' + (d[2] ? 'Tilt ' + d[2] + ' · ' : '') + deg + '</div>';
  if (d[4]) h += '<div style="color:__NOTE__">tilt filter: ' + d[4] + '</div>';
  for (const p of ps) {
    const v = p.data ? p.data[1] : null;
    if (v === null || v === undefined) continue;
    h += '<div>' + p.marker + p.seriesName
      + ' <span style="font-family:__MONO__;font-weight:600">'
      + Number(v).toPrecision(3) + '__UNIT__</span></div>';
  }
  if (d[5]) h += __IMG__;
  return h + '</div>';
}
""".replace("__IMG__", _TOOLTIP_IMG_JS.replace("__SRC__", "d[5]"))

# Motion-track hover: rows are [x, y, tilt_number, frame_basename, tilt_deg, thumb_url]
# (frame at slot 3 like the per-tilt charts, so one select hook serves both).
_TRACK_TOOLTIP_JS = r"""
(p) => {
  const d = p.data || [];
  let h = __OPEN__;
  h += '<div style="font-weight:600">Tilt ' + d[2] + ' · ' + Number(d[4]).toFixed(2) + '° · step '
    + (p.dataIndex + 1) + '</div>';
  h += '<div>x <span style="font-family:__MONO__;font-weight:600">' + Number(d[0]).toFixed(2)
    + '</span> · y <span style="font-family:__MONO__;font-weight:600">' + Number(d[1]).toFixed(2)
    + '</span></div>';
  if (d[5]) h += __IMG__;
  return h + '</div>';
}
""".replace("__IMG__", _TOOLTIP_IMG_JS.replace("__SRC__", "d[5]"))

# CTF-fit hover: the hovered spatial frequency as a resolution, then measured vs model.
_FIT_TOOLTIP_JS = r"""
(ps) => {
  if (!Array.isArray(ps)) ps = [ps];
  const f = ps[0].data ? ps[0].data[0] : null;
  let h = __OPEN__;
  if (typeof f === 'number' && f > 0) h += '<div style="font-weight:600">' + (1 / f).toFixed(1)
    + ' Å <span style="color:#94a3b8;font-weight:400">(' + f.toFixed(3) + ' 1/Å)</span></div>';
  for (const p of ps) {
    const v = p.data ? p.data[1] : null;
    if (v === null || v === undefined) continue;
    h += '<div>' + p.marker + p.seriesName
      + ' <span style="font-family:__MONO__;font-weight:600">' + Number(v).toFixed(2) + '</span></div>';
  }
  return h + '</div>';
}
"""


def _card_js(js: str, theme: ChartTheme) -> str:
    """A hover-card formatter in the theme's card type."""
    return (
        js.replace("__OPEN__", _card_open(theme))
        .replace("__MONO__", theme.card_mono)
        .replace("__NOTE__", theme.card_note)
    )


# The card can be taller than its tile once it carries an image: place it beside the
# cursor, flip left near the right edge, clamp vertically to the tile. Paired with
# appendToBody so the page's scroll area can't clip it.
_TOOLTIP_POSITION_JS = """
(point, params, dom, rect, size) => {
  const w = size.contentSize[0], h = size.contentSize[1];
  const vw = size.viewSize[0], vh = size.viewSize[1];
  let x = point[0] + 16;
  if (x + w > vw) x = point[0] - w - 16;
  let y = point[1] - h / 2;
  if (y + h > vh) y = vh - h;
  if (y < 0) y = 0;
  return [x, y];
}
"""


def _tooltip(formatter_js: str, *, trigger: str = "axis", theme: ChartTheme = DEFAULT_THEME) -> dict:
    """The house hover card (white, hairline border, soft shadow) with a JS formatter."""
    out = {
        "trigger": trigger,
        "backgroundColor": "#ffffff",
        "borderColor": "#e2e8f0",
        "borderWidth": 1,
        "padding": [6, 8],
        "appendToBody": True,
        "extraCssText": "box-shadow: 0 6px 18px rgba(15,23,42,0.10); border-radius: 5px;",
        ":formatter": formatter_js,
        ":position": _TOOLTIP_POSITION_JS,
    }
    if trigger == "axis":
        out["axisPointer"] = {
            "type": "line",
            "snap": True,
            "lineStyle": {"color": theme.pointer_color, "type": "dashed"},
        }
    return out


def _legend(show: bool, theme: ChartTheme = DEFAULT_THEME) -> dict:
    return {
        "show": show,
        "bottom": 0,
        "left": "center",
        "icon": "circle",
        "itemWidth": 8,
        "itemHeight": 8,
        "itemGap": 14,
        "textStyle": {"fontFamily": _SANS, "fontSize": 9, "color": theme.legend_color},
    }


def _axis(name: str, *, horizontal: bool, theme: ChartTheme = DEFAULT_THEME) -> dict:
    """One value axis in the theme's style (the house one: bold name, mono labels, no ticks;
    the x axis keeps its line and the y axis keeps faint grid lines)."""
    label = {"fontFamily": theme.tick_font, "fontSize": theme.tick_size, "color": theme.tick_color}
    name_style = {
        "fontFamily": theme.name_font,
        "fontSize": theme.name_size,
        "fontWeight": theme.name_weight,
        "color": theme.name_color,
    }
    if horizontal:
        return {
            "type": "value",
            "name": name,
            "nameLocation": "middle",
            "nameGap": _X_NAME_GAP,
            "nameTextStyle": name_style,
            "axisLabel": label,
            "axisLine": {"lineStyle": {"color": theme.x_line}} if theme.x_line else {"show": False},
            "axisTick": {"show": False},
            "splitLine": {"show": False},
        }
    axis = {
        "type": "value",
        "scale": True,
        "name": name,
        "nameLocation": "end",
        "nameGap": 8,
        "nameTextStyle": {**name_style, "align": "left"},
        "axisLabel": label,
        "axisLine": {"show": False},
        "axisTick": {"show": False},
        "splitLine": {"lineStyle": {"color": theme.y_grid}} if theme.y_grid else {"show": False},
    }
    if theme.y_ticks:
        axis["splitNumber"] = theme.y_ticks
    return axis


def _mix(a: str, b: str, t: float) -> str:
    ra, ga, ba = (int(a[i : i + 2], 16) for i in (1, 3, 5))
    rb, gb, bb = (int(b[i : i + 2], 16) for i in (1, 3, 5))
    r, g, bl = round(ra + (rb - ra) * t), round(ga + (gb - ga) * t), round(ba + (bb - ba) * t)
    return f"#{r:02x}{g:02x}{bl:02x}"


def tilt_color(tilt_deg: float, max_abs_tilt: float, theme: ChartTheme = DEFAULT_THEME) -> str:
    """A signed stage tilt on the theme's diverging scale: its neutral at 0°, darkening toward
    the negative or positive pole with |tilt| (the house poles: indigo and amber, the canonical
    CVD-safe pair)."""
    t = 0.0 if max_abs_tilt <= 0 else min(1.0, abs(tilt_deg) / max_abs_tilt)
    return _mix(theme.tilt_mid, theme.tilt_neg if tilt_deg < 0 else theme.tilt_pos, t)


def _marker_style(color: str, hollow: bool, theme: ChartTheme) -> dict:
    """A scatter marker: a disc, or a ring for a hollow series; at the theme's opacity."""
    style: dict = {"color": "#ffffff", "borderColor": color, "borderWidth": 1} if hollow else {"color": color}
    if theme.marker_opacity < 1:
        style["opacity"] = theme.marker_opacity
    return style


def _emphasis(theme: ChartTheme) -> dict:
    """The hovered point: grown, and filled / ringed when the theme says so."""
    out: dict = {"scale": theme.hover_scale}
    style: dict = {}
    if theme.hover_color:
        style["color"] = theme.hover_color
    if theme.hover_ring:
        style.update({"borderColor": theme.hover_ring, "borderWidth": 1.5})
    if style:
        out["itemStyle"] = style
    return out


def build_empty_chart(message: str) -> dict:
    """A tile-sized chart that says why there is nothing to draw."""
    return {
        "animation": False,
        "title": {
            "text": message,
            "left": "center",
            "top": "middle",
            "textStyle": {"fontFamily": _SANS, "fontSize": 10, "fontWeight": "normal", "color": "#9ca3af"},
        },
        "xAxis": {"show": False},
        "yAxis": {"show": False},
        "series": [],
    }


def build_per_tilt_chart(
    x_tilts: list[float],
    series: list[dict],
    *,
    x_label: str = "Stage tilt (°)",
    y_label: str = "",
    customdata: list[list] | None = None,
    y_unit: str = "",
    y_range: tuple[float, float] | None = None,
    theme: ChartTheme = DEFAULT_THEME,
) -> dict:
    """ECharts options for a per-tilt chart: x = stage tilt angle, y = one or more
    per-tilt metrics.

    `series`: list of {name, y, color?, mode?, ref?, width?} entries. Default mode is
    `markers` — discrete per-tilt estimates connect badly with lines (zigzag or tangled),
    so a caller opts in with `mode='lines+markers'` / `'lines'` for metrics that vary
    continuously across tilts; line series are drawn in tilt order. `ref`: a fit or
    reference line, drawn as a line in the theme's reference style. Markers take the
    theme's size, opacity and hollow series. `customdata`: parallel list of
    [tilt_number, frame_basename, (verdict)] rows surfaced in the hover card. `y_unit`:
    suffix for hover values (" µm", " Å"). `y_range`: fixed y-axis [min, max] so the same
    metric is visually comparable across tilt series; auto-extended (never clipped) when
    the data exceeds it.
    """
    out: list[dict] = []
    for i, s in enumerate(series):
        ref = bool(s.get("ref"))
        color = theme.ref_color if ref else (s.get("color") or theme.series[i % len(theme.series)])
        mode = "lines" if ref else s.get("mode", "markers")
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
        hollow = not is_line and theme.hollow[i % len(theme.hollow)]
        entry: dict = {
            "name": s["name"],
            "type": "line" if is_line else "scatter",
            "data": pts,
            "symbol": "circle",
            "symbolSize": theme.marker_size,
            "itemStyle": _marker_style(color, hollow, theme),
            "emphasis": _emphasis(theme),
        }
        if is_line:
            entry["showSymbol"] = "markers" in mode
            entry["connectNulls"] = False
            entry["lineStyle"] = {
                "color": color,
                "width": s.get("width", 1.4),
                "type": theme.ref_type if ref else "solid",
            }
        out.append(entry)

    show_legend = len(series) > 1
    y_axis = _axis(y_label, horizontal=False, theme=theme)
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
        "color": list(theme.series),
        "grid": _grid(show_legend, theme),
        "xAxis": _axis(x_label, horizontal=True, theme=theme),
        "yAxis": y_axis,
        "legend": _legend(show_legend, theme),
        "tooltip": _tooltip(_card_js(_TOOLTIP_FORMATTER_JS, theme).replace("__UNIT__", y_unit), theme=theme),
        "series": out,
    }


def build_motion_track_chart(
    tracks: list[dict], *, unit_label: str = "Warp units, ≈ Å", theme: ChartTheme = DEFAULT_THEME
) -> dict:
    """ECharts options for the beam-induced motion tracks of every tilt in a tilt
    series: one thin polyline per tilt (x/y shift per time step of the movie, all
    starting near the origin) on the signed-tilt diverging scale (`tilt_color`),
    high tilts drawn last. `tracks`: [{tilt, index, frame, x, y, thumb?}]. Data rows
    are [x, y, tilt_number, frame_basename, tilt_deg, thumb_url] — frame at slot 3,
    like the per-tilt charts, so one select hook serves both. Both axes share one
    symmetric range so a path's direction reads true."""
    max_abs_tilt = max((abs(float(t["tilt"])) for t in tracks), default=0.0)
    lim = 1.0
    for t in tracks:
        for v in [*t["x"], *t["y"]]:
            lim = max(lim, abs(float(v)))
    lim = round(lim * 1.08, 3)
    series: list[dict] = []
    for t in sorted(tracks, key=lambda t: abs(float(t["tilt"]))):
        color = tilt_color(float(t["tilt"]), max_abs_tilt, theme)
        rows = [
            [x, y, t["index"], t["frame"], t["tilt"], t.get("thumb", "")] for x, y in zip(t["x"], t["y"], strict=True)
        ]
        series.append(
            {
                "name": f"{float(t['tilt']):+.1f}°",
                "type": "line",
                "data": rows,
                "symbol": "circle",
                "symbolSize": 3,
                "showSymbol": True,
                "lineStyle": {"color": color, "width": 1.3},
                "itemStyle": {"color": color},
                "emphasis": {"focus": "series", "lineStyle": {"width": 2.4}},
            }
        )
    x_axis = _axis(f"X shift ({unit_label})", horizontal=True, theme=theme)
    y_axis = _axis("Y shift", horizontal=False, theme=theme)
    x_axis.update({"min": -lim, "max": lim})
    y_axis.update({"min": -lim, "max": lim, "scale": False})
    return {
        "animation": False,
        "grid": _grid(False, theme),
        "xAxis": x_axis,
        "yAxis": y_axis,
        "legend": _legend(False, theme),
        "tooltip": _tooltip(_card_js(_TRACK_TOOLTIP_JS, theme), trigger="item", theme=theme),
        "series": series,
    }


def build_ctf_fit_chart(
    freq_inv_a: list[float],
    measured: list[float | None],
    model: list[float],
    *,
    fit_res_a: float | None = None,
    fit_window_inv_a: tuple[float, float] | None = None,
    theme: ChartTheme = DEFAULT_THEME,
) -> dict:
    """ECharts options for the CTF-fit overlay of ONE tilt: Warp's measured 1-D power
    spectrum (÷ its fitted envelope) against the CTF² model over spatial
    frequency in 1/Å, a reference line at Warp's fit-resolution estimate, and the band
    beyond the fitted frequency window shaded. y auto-ranges to the data."""
    accent, ink = theme.fit_model, theme.fit_measured
    model_series: dict = {
        "name": "CTF model",
        "type": "line",
        "data": [[f, v] for f, v in zip(freq_inv_a, model, strict=True)],
        "showSymbol": False,
        "lineStyle": {"color": accent, "width": 1.4},
        "itemStyle": {"color": accent},
    }
    if fit_res_a:
        model_series["markLine"] = {
            "silent": True,
            "symbol": "none",
            "lineStyle": {"color": theme.ref_color, "type": theme.ref_type, "width": 1},
            "label": {
                "formatter": f"fit to {fit_res_a:.1f} Å",
                "position": "insideEndTop",
                "fontFamily": theme.name_font,
                "fontSize": 9,
                "color": theme.tick_color,
            },
            "data": [{"xAxis": 1.0 / fit_res_a}],
        }
    if fit_window_inv_a and freq_inv_a and fit_window_inv_a[1] < freq_inv_a[-1]:
        model_series["markArea"] = {
            "silent": True,
            "itemStyle": {"color": "rgba(148, 163, 184, 0.12)"},
            "data": [[{"xAxis": fit_window_inv_a[1]}, {"xAxis": freq_inv_a[-1]}]],
        }
    series = [
        {
            "name": "Measured",
            "type": "line",
            "data": [[f, v] for f, v in zip(freq_inv_a, measured, strict=True)],
            "showSymbol": False,
            "connectNulls": False,
            "lineStyle": {"color": ink, "width": 1.1},
            "itemStyle": {"color": ink},
        },
        model_series,
    ]
    x_axis = _axis("Spatial frequency (1/Å)", horizontal=True, theme=theme)
    y_axis = _axis("Normalized power", horizontal=False, theme=theme)
    if freq_inv_a:
        x_axis.update({"min": round(freq_inv_a[0], 4), "max": round(freq_inv_a[-1], 4)})
    # Auto-ranged on purpose: measured ÷ envelope overshoots 1 at the ring maxima and
    # undershoots 0 between them, so a fixed 0…1 window would cut the curves off.
    return {
        "animation": False,
        "grid": _grid(True, theme),
        "xAxis": x_axis,
        "yAxis": y_axis,
        "legend": _legend(True, theme),
        "tooltip": _tooltip(_card_js(_FIT_TOOLTIP_JS, theme), theme=theme),
        "series": series,
    }


# ── The tilt filter's distributions (ui/tilt_filter_panel.py) ──

# What a tilt is, in every chart of the tilt filter's page: one colour per state and the same in
# each chart, so the page carries one legend above its charts. A tilt counts once, under the
# first state that applies. The four colours passed the palette validator as a set (every pair
# ΔE ≥ 13 under colour-vision deficiency, ≥ 16 with full colour vision); the amber sits under
# 3:1 against the surface, so the legend and the hover carry every count as text.
TILT_STATES = (
    ("excluded", "excluded", "#e34948"),
    ("out", "not in alignment's output", "#4a3aa7"),
    ("dark", "dark, kept", "#eda100"),
    ("kept", "kept", "#2a78d6"),
)
_STATE = {key: (name, color) for key, name, color in TILT_STATES}
# The charts' one height, so they line up side by side as small multiples.
SMALL_CHART_PX = 130
# The score histogram tops its axis at the second-largest bin when one bin outgrows it this much,
# and prints the clipped bin's count: a linear axis that the untouched majority would flatten.
_CLIP_RATIO = 5.0

# One hover card for every bar chart of the page: a title (`__TITLE__`, a JS expression over
# `ps`), then each state's count at that bar.
_COUNTS_TOOLTIP_JS = r"""
(ps) => {
  if (!Array.isArray(ps)) ps = [ps];
  let h = __OPEN__ + '<div style="font-weight:600">' + (__TITLE__) + '</div>';
  let total = 0;
  for (const p of ps) {
    const v = Array.isArray(p.value) ? p.value[1] : p.value;
    if (!v) continue;
    total += v;
    h += '<div>' + p.marker + '<span style="font-family:IBM Plex Mono,monospace;font-weight:600">' + v
      + '</span> ' + p.seriesName + '</div>';
  }
  if (!total) h += '<div style="color:#94a3b8">none</div>';
  return h + '</div>';
}
"""


def _count_axis(name: str) -> dict:
    """A count axis from zero, whole numbers only."""
    axis = _axis(name, horizontal=False)
    axis.update({"scale": False, "min": 0, "minInterval": 1})
    return axis


def _counts_tooltip(title_js: str) -> dict:
    tooltip = _tooltip(_COUNTS_TOOLTIP_JS.replace("__OPEN__", _TOOLTIP_CARD_OPEN).replace("__TITLE__", title_js))
    tooltip["axisPointer"] = {"type": "shadow", "shadowStyle": {"color": "rgba(148,163,184,0.12)"}}
    return tooltip


def _stacked_bars(keys: Sequence[str], counts: dict[str, list], *, xs: list | None = None, bar_px: int) -> list[dict]:
    """One stacked bar series per state in `keys` (bottom first), `counts` per key and per bar. Each
    bar's top segment gets the rounded end; a 1 px surface border on every segment makes the 2 px
    gap between them. `xs` puts the bars on a value axis instead of a category axis."""
    n = len(next(iter(counts.values())))
    tops = [next((k for k in reversed(keys) if counts[k][i]), None) for i in range(n)]
    series = []
    for key in keys:
        name, color = _STATE[key]
        data = []
        for i, value in enumerate(counts[key]):
            item: dict = {"value": [xs[i], value] if xs is not None else value}
            if value and tops[i] == key:
                item["itemStyle"] = {"borderRadius": [4, 4, 0, 0]}
            data.append(item)
        series.append(
            {
                "type": "bar",
                "name": name,
                "stack": "states",
                "data": data,
                "barMaxWidth": bar_px,
                "itemStyle": {"color": color, "borderColor": "#ffffff", "borderWidth": 1},
                "emphasis": {"focus": "series"},
            }
        )
    return series


def build_excluded_by_tilt_chart(bins: list[str], counts: dict[str, list[int]]) -> dict:
    """Tilts excluded, not in alignment's output, or dark and kept, per 10° bin of signed stage
    tilt (`bins` like "−60…−50", `counts` per state key, one per bin), stacked."""
    x_axis = _axis("stage tilt", horizontal=True)
    x_axis.update({"type": "category", "data": bins})
    x_axis["axisLabel"] = {**x_axis["axisLabel"], ":formatter": "(v) => v.split('…')[0] + '°'"}
    return {
        "animation": False,
        "grid": _grid(False),
        "tooltip": _counts_tooltip("'stage tilt ' + ps[0].axisValueLabel.replace('…', '° to ') + '°'"),
        "xAxis": x_axis,
        "yAxis": _count_axis("tilts"),
        "series": _stacked_bars(("excluded", "out", "dark"), counts, bar_px=16),
    }


def build_score_histogram(counts: dict[str, list[int]], threshold: float | None) -> dict:
    """The confidence score's histogram: equal bins over 0–1 (`counts` per state key, excluded
    and kept, one per bin), stacked, with the threshold as a dashed line. A linear count axis:
    when one bin holds more than _CLIP_RATIO times the next-largest, the axis tops out a little
    above that one and the clipped bin prints its count."""
    n_bins = len(counts["excluded"])
    half = 0.5 / n_bins
    centers = [round((i + 0.5) / n_bins, 4) for i in range(n_bins)]
    series = _stacked_bars(("excluded", "kept"), counts, xs=centers, bar_px=12)
    totals = [counts["excluded"][i] + counts["kept"][i] for i in range(n_bins)]
    ranked = sorted(range(n_bins), key=lambda i: totals[i], reverse=True)
    y_axis = _count_axis("tilts")
    top, second = totals[ranked[0]], totals[ranked[1]] if n_bins > 1 else 0
    if second and top > _CLIP_RATIO * second:
        y_max = math.ceil(second * 1.2)
        y_axis["max"] = y_max
        series[-1]["markPoint"] = {
            "silent": True,
            "symbolSize": 0,
            "label": {
                "show": True,
                "position": "top",
                "formatter": str(top),
                "fontFamily": _MONO,
                "fontSize": 9,
                "color": "#334155",
            },
            "data": [{"coord": [centers[ranked[0]], y_max]}],
        }
    if threshold is not None:
        series[0]["markLine"] = {
            "silent": True,
            "symbol": "none",
            "animation": False,
            "lineStyle": {"type": "dashed", "color": "#334155", "width": 1},
            "label": {
                "formatter": f"threshold {threshold:.2f}",
                "position": "insideEndTop",
                "fontFamily": _MONO,
                "fontSize": 9,
                "color": "#334155",
            },
            "data": [{"xAxis": round(threshold, 4)}],
        }
    x_axis = _axis("confidence score", horizontal=True)
    x_axis.update({"min": 0, "max": 1, "interval": 0.2})
    title = (
        f"'confidence score ' + (ps[0].value[0] - {half:.4f}).toFixed(2) + '–' + "
        f"(ps[0].value[0] + {half:.4f}).toFixed(2)"
    )
    tooltip = _counts_tooltip(title)
    tooltip["axisPointer"] = {"type": "line", "snap": True, "lineStyle": {"color": "#cbd5e1", "type": "dashed"}}
    return {
        "animation": False,
        "grid": _grid(False),
        "tooltip": tooltip,
        "xAxis": x_axis,
        "yAxis": y_axis,
        "series": series,
    }


def build_excluded_per_series_chart(names: list[str], counts: dict[str, list[int]]) -> dict:
    """Tilts excluded and not in alignment's output per tilt series (`names`, in position
    order), stacked, one thin bar each; the hover names the series."""
    x_axis = _axis("tilt series", horizontal=True)
    x_axis.update({"type": "category", "data": names})
    x_axis["axisLabel"] = {**x_axis["axisLabel"], "show": False}
    return {
        "animation": False,
        "grid": _grid(False),
        "tooltip": _counts_tooltip("ps[0].axisValueLabel"),
        "xAxis": x_axis,
        "yAxis": _count_axis("tilts"),
        "series": _stacked_bars(("excluded", "out"), counts, bar_px=6),
    }


# ── Particle analytics (Species page · Analytics tab) ──

# Hover card of the σ charts: the hovered σ (or bin), then each series' value at it. Values are
# counts; a fractional one (the Gaussian expectation) keeps two significant digits.
_SIGMA_TOOLTIP_JS = r"""
(ps) => {
  if (!Array.isArray(ps)) ps = [ps];
  const x = ps[0].value ? ps[0].value[0] : null;
  let h = __OPEN__ + '<div style="font-weight:600">' + (__TITLE__) + '</div>';
  for (const p of ps) {
    const v = Array.isArray(p.value) ? p.value[1] : p.value;
    if (v === null || v === undefined) continue;
    const t = Number.isInteger(v) ? v.toLocaleString()
      : (v >= 100 ? Math.round(v).toLocaleString() : Number(v).toPrecision(2));
    h += '<div>' + p.marker + p.seriesName
      + ' <span style="font-family:IBM Plex Mono,monospace;font-weight:600">' + t + '</span></div>';
  }
  return h + '</div>';
}
"""


def _sigma_tooltip(title_js: str) -> dict:
    return _tooltip(_SIGMA_TOOLTIP_JS.replace("__OPEN__", _TOOLTIP_CARD_OPEN).replace("__TITLE__", title_js))


def _sigma_mark_lines(lines: Sequence[tuple[float, str, str]]) -> dict:
    """Dashed vertical marks at σ positions: (x, label, color) each."""
    return {
        "silent": True,
        "symbol": "none",
        "animation": False,
        "data": [
            {
                "xAxis": round(float(x), 4),
                "lineStyle": {"type": "dashed", "color": color, "width": 1},
                "label": {
                    "formatter": label,
                    "position": "insideEndTop",
                    "fontFamily": _MONO,
                    "fontSize": 9,
                    "color": color,
                },
            }
            for x, label, color in lines
        ],
    }


def build_sigma_histogram(
    edges: Sequence[float], counts: Sequence[int], *, width: float, fp1: float | None = None, cut: float | None = None
) -> dict:
    """Picks per σ bin (`edges` = left edges, `width` σ wide), with the FP=1 cutoff (muted) and
    the chosen cut (ink) as dashed lines."""
    if not counts:
        return build_empty_chart("no scores")
    centers = [round(e + width / 2, 4) for e in edges]
    series: dict = {
        "type": "bar",
        "name": "picks",
        "data": [[c, n] for c, n in zip(centers, counts, strict=True)],
        "barMaxWidth": 10,
        "itemStyle": {"color": SERIES_PALETTE[0], "borderRadius": [2, 2, 0, 0]},
    }
    marks = []
    if fp1 is not None:
        marks.append((fp1, f"FP=1 {fp1:.2f}σ", SERIES_PALETTE[2]))
    if cut is not None:
        marks.append((cut, f"cut {cut:.1f}σ", SERIES_PALETTE[1]))
    if marks:
        series["markLine"] = _sigma_mark_lines(marks)
    x_axis = _axis("score (σ)", horizontal=True)
    x_axis.update({"min": round(edges[0], 4), "max": round(edges[-1] + width, 4)})
    half = width / 2
    tooltip = _sigma_tooltip(f"(x - {half}).toFixed(2) + '–' + (x + {half}).toFixed(2) + 'σ'")
    tooltip["axisPointer"] = {"type": "line", "snap": True, "lineStyle": {"color": "#cbd5e1", "type": "dashed"}}
    return {
        "animation": False,
        "grid": _grid(False),
        "tooltip": tooltip,
        "xAxis": x_axis,
        "yAxis": _count_axis("picks"),
        "series": [series],
    }


# The Gaussian null falls off a cliff past the cutoff; below this many expected noise peaks the
# curve stops (a log axis has no zero).
_NULL_FLOOR = 0.01


def build_survival_chart(
    z: Sequence[float],
    observed: Sequence[int],
    null: Sequence[float],
    *,
    cut: float | None = None,
    extra: Sequence[dict] = (),
) -> dict:
    """Picks scoring at or above z, against z, on a log count axis: the observed curve, the
    Gaussian expectation N·Q(z) summed over tomograms (dashed), and any `extra` curves (each
    {name, y, color}; a decoy run's survival curve). The gap between observed and the null is
    what the data hold beyond pure noise."""
    if not z:
        return build_empty_chart("no scores")

    def positive(values, floor: float = 0.0):
        return [
            [x, float(v)] if v is not None and float(v) > floor else [x, None] for x, v in zip(z, values, strict=True)
        ]

    series: list[dict] = [
        {
            "type": "line",
            "name": "observed",
            "data": positive(observed),
            "showSymbol": False,
            "connectNulls": False,
            "step": "end",
            "lineStyle": {"color": SERIES_PALETTE[0], "width": 1.4},
            "itemStyle": {"color": SERIES_PALETTE[0]},
        }
    ]
    if null:
        series.append(
            {
                "type": "line",
                "name": "Gaussian noise N·Q(z)",
                "data": positive(null, _NULL_FLOOR),
                "showSymbol": False,
                "connectNulls": False,
                "lineStyle": {"color": SERIES_PALETTE[2], "width": 1.2, "type": "dashed"},
                "itemStyle": {"color": SERIES_PALETTE[2]},
            }
        )
    for e in extra:
        color = e.get("color") or SERIES_PALETTE[1]
        series.append(
            {
                "type": "line",
                "name": e["name"],
                "data": positive(e["y"]),
                "showSymbol": False,
                "connectNulls": False,
                "step": "end",
                "lineStyle": {"color": color, "width": 1.2},
                "itemStyle": {"color": color},
            }
        )
    if cut is not None:
        series[0]["markLine"] = _sigma_mark_lines([(cut, f"cut {cut:.1f}σ", SERIES_PALETTE[1])])
    x_axis = _axis("score z (σ)", horizontal=True)
    x_axis.update({"min": round(z[0], 4), "max": round(z[-1], 4)})
    y_axis = _axis("picks ≥ z", horizontal=False)
    y_axis.update({"type": "log", "logBase": 10, "scale": False})
    show_legend = len(series) > 1
    return {
        "animation": False,
        "grid": _grid(show_legend),
        "legend": _legend(show_legend),
        "tooltip": _sigma_tooltip("'≥ ' + Number(x).toFixed(1) + 'σ'"),
        "xAxis": x_axis,
        "yAxis": y_axis,
        "series": series,
    }


def build_depth_profile_chart(n: Sequence[int], at_cut: Sequence[int], *, cut: float) -> dict:
    """Picks per depth bin through the pick slab (bars; bins between the slab's 5th and 95th
    percentile, the outer two holding everything beyond), and the share of each bin scoring at or
    above the cut (points, right axis). Real particles peak mid-slab; surface artefacts do not."""
    if not any(n):
        return build_empty_chart("no tomogram has enough picks for a pick plane")
    k = len(n)
    labels = [f"{round(100 * i / k)}–{round(100 * (i + 1) / k)}%" for i in range(k)]
    share = [round(100.0 * c / t, 1) if t else None for c, t in zip(at_cut, n, strict=True)]
    x_axis = _axis("depth through the pick slab", horizontal=True)
    x_axis.update({"type": "category", "data": labels})
    y_left = _count_axis("picks")
    y_right = _axis(f"% ≥ {cut:.1f}σ", horizontal=False)
    y_right.update({"min": 0, "max": 100, "scale": False, "position": "right", "splitLine": {"show": False}})
    y_right["nameTextStyle"] = {**y_right["nameTextStyle"], "align": "right"}
    tooltip = _counts_tooltip("'depth ' + ps[0].axisValueLabel")
    return {
        "animation": False,
        "grid": _grid(True),
        "legend": _legend(True),
        "tooltip": tooltip,
        "xAxis": x_axis,
        "yAxis": [y_left, y_right],
        "series": [
            {
                "type": "bar",
                "name": "picks",
                "data": list(n),
                "barMaxWidth": 18,
                "itemStyle": {"color": SERIES_PALETTE[2], "borderRadius": [2, 2, 0, 0]},
            },
            {
                "type": "scatter",
                "name": f"% ≥ {cut:.1f}σ",
                "yAxisIndex": 1,
                "data": share,
                "symbolSize": 7,
                "itemStyle": {"color": SERIES_PALETTE[0]},
            },
        ],
    }


# Hover card of the yield-against-quality scatter: the tomogram, then x and y.
_YIELD_TOOLTIP_JS = r"""
(p) => {
  const d = p.data || [];
  return __OPEN__ + '<div style="font-weight:600">' + d[2] + '</div>'
    + '<div>__X__ <span style="font-family:IBM Plex Mono,monospace;font-weight:600">' + d[0] + '</span></div>'
    + '<div>__Y__ <span style="font-family:IBM Plex Mono,monospace;font-weight:600">' + d[1] + '</span></div></div>';
}
"""


def build_yield_scatter(points: Sequence[tuple[float, float, str]], *, x_name: str, y_name: str) -> dict:
    """One point per tomogram: a data-quality fact (x) against its picks at or above the cut (y),
    the tomogram named on hover. Low yield at normal quality reads as little content, not bad data."""
    if not points:
        return build_empty_chart("no tomogram has both numbers")
    x_axis = _axis(x_name, horizontal=True)
    x_axis["scale"] = True
    y_axis = _count_axis(y_name)
    js = _YIELD_TOOLTIP_JS.replace("__OPEN__", _TOOLTIP_CARD_OPEN).replace("__X__", x_name).replace("__Y__", y_name)
    return {
        "animation": False,
        "grid": _grid(False),
        "tooltip": _tooltip(js, trigger="item"),
        "xAxis": x_axis,
        "yAxis": y_axis,
        "series": [
            {
                "type": "scatter",
                "data": [[x, y, name] for x, y, name in points],
                "symbolSize": 6,
                "itemStyle": {"color": SERIES_PALETTE[0], "opacity": 0.75},
                "emphasis": {"scale": 1.6},
            }
        ],
    }


def build_category_bars(labels: Sequence[str], counts: Sequence[int], *, x_name: str, y_name: str) -> dict:
    """A plain count per category (a histogram over whole numbers), one bar each."""
    if not counts:
        return build_empty_chart("nothing to count")
    x_axis = _axis(x_name, horizontal=True)
    x_axis.update({"type": "category", "data": list(labels)})
    return {
        "animation": False,
        "grid": _grid(False),
        "tooltip": _counts_tooltip(f"'{x_name} ' + ps[0].axisValueLabel"),
        "xAxis": x_axis,
        "yAxis": _count_axis(y_name),
        "series": [
            {
                "type": "bar",
                "name": y_name,
                "data": list(counts),
                "barMaxWidth": 12,
                "itemStyle": {"color": SERIES_PALETTE[0], "borderRadius": [2, 2, 0, 0]},
            }
        ],
    }


# A categorical set for classes: the house accent, ink and slate first, then distinct hues that hold
# up in both themes. Classes beyond its length repeat it.
CLASS_PALETTE = (
    "#4f46e5",
    "#0f172a",
    "#94a3b8",
    "#0e7490",
    "#b45309",
    "#be123c",
    "#15803d",
    "#7c3aed",
    "#a16207",
    "#475569",
)


def class_color(k: int) -> str:
    return CLASS_PALETTE[(int(k) - 1) % len(CLASS_PALETTE)]


def build_class_convergence_chart(iterations: Sequence[int], per_class: dict[int, Sequence[float]]) -> dict:
    """Class distribution at each iteration, one stacked bar per iteration summing to 100 %: a
    classification has converged when the stacks stop changing."""
    if not iterations or not per_class:
        return build_empty_chart("no model.star per iteration")
    x_axis = _axis("iteration", horizontal=True)
    x_axis.update({"type": "category", "data": [str(i) for i in iterations]})
    y_axis = _count_axis("% of particles")
    y_axis["max"] = 100
    y_axis.pop("minInterval", None)
    series = [
        {
            "type": "bar",
            "name": f"class {k}",
            "stack": "classes",
            "data": [round(100.0 * v, 1) for v in per_class[k]],
            "barMaxWidth": 14,
            "itemStyle": {"color": class_color(k), "borderColor": "#ffffff", "borderWidth": 1},
        }
        for k in sorted(per_class)
    ]
    return {
        "animation": False,
        "grid": _grid(len(series) > 1),
        "legend": _legend(len(series) > 1),
        "tooltip": _counts_tooltip("'iteration ' + ps[0].axisValueLabel"),
        "xAxis": x_axis,
        "yAxis": y_axis,
        "series": series,
    }


def build_fsc_chart(curves: dict[str, dict], *, thresholds: Sequence[float] = (0.143, 0.5)) -> dict:
    """Half-map FSC against spatial frequency (1/Å), one line per curve ({name: {freq, fsc}}), with
    the thresholds dashed. The FSC is a continuous function of frequency, so it is drawn as lines."""
    if not curves:
        return build_empty_chart("no half maps")
    series = []
    for i, (name, c) in enumerate(curves.items()):
        color = SERIES_PALETTE[i % len(SERIES_PALETTE)]
        series.append(
            {
                "type": "line",
                "name": name,
                "data": [[f, v] for f, v in zip(c["freq"], c["fsc"], strict=True)],
                "showSymbol": False,
                "lineStyle": {"color": color, "width": 1.4},
                "itemStyle": {"color": color},
            }
        )
    series[0]["markLine"] = {
        "silent": True,
        "symbol": "none",
        "lineStyle": {"type": "dashed", "color": "#94a3b8", "width": 1},
        "label": {"fontFamily": _MONO, "fontSize": 9, "color": "#64748b", "position": "insideEndTop"},
        "data": [{"yAxis": t, "label": {"formatter": f"{t:g}"}} for t in thresholds],
    }
    x_axis = _axis("spatial frequency (1/Å)", horizontal=True)
    x_axis.update({"min": 0})
    y_axis = _axis("FSC", horizontal=False)
    y_axis.update({"min": -0.1, "max": 1.0, "scale": False})
    return {
        "animation": False,
        "grid": _grid(len(series) > 1),
        "legend": _legend(len(series) > 1),
        "tooltip": _tooltip(_card_js(_FIT_TOOLTIP_JS, DEFAULT_THEME)),
        "xAxis": x_axis,
        "yAxis": y_axis,
        "series": series,
    }


def build_curves_chart(
    x: Sequence[float],
    series: Sequence[dict],
    *,
    x_name: str,
    y_name: str,
    y_max: float | None = None,
    cut: float | None = None,
) -> dict:
    """Continuous curves over one x (each {name, y, color?}; None leaves a gap), the cut dashed."""
    if not x or not series:
        return build_empty_chart("nothing to draw")
    out = []
    for i, s in enumerate(series):
        color = s.get("color") or SERIES_PALETTE[i % len(SERIES_PALETTE)]
        out.append(
            {
                "type": "line",
                "name": s["name"],
                "data": [[a, b] for a, b in zip(x, s["y"], strict=True)],
                "showSymbol": False,
                "connectNulls": False,
                "lineStyle": {"color": color, "width": 1.4},
                "itemStyle": {"color": color},
            }
        )
    if cut is not None:
        out[0]["markLine"] = _sigma_mark_lines([(cut, f"cut {cut:.1f}σ", SERIES_PALETTE[1])])
    x_axis = _axis(x_name, horizontal=True)
    x_axis.update({"min": round(x[0], 4), "max": round(x[-1], 4)})
    y_axis = _axis(y_name, horizontal=False)
    if y_max is not None:
        y_axis.update({"min": 0, "max": y_max, "scale": False})
    return {
        "animation": False,
        "grid": _grid(len(out) > 1),
        "legend": _legend(len(out) > 1),
        "tooltip": _sigma_tooltip("Number(x).toFixed(2)"),
        "xAxis": x_axis,
        "yAxis": y_axis,
        "series": out,
    }

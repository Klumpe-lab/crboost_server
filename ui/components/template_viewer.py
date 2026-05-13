"""Three-panel orthoslice viewer for templates and masks.

Replaces the broken molstar mount in the workbench. NiceGUI + plotly
heatmap-via-dict. Templates are 64–128 px so loading the full volume
into memory and rendering three slices is cheap (~1–8 MB per template).

Per project memory: do NOT use plotly's `scaleanchor` for cryo-ET-shaped
volumes; use container CSS. Use `ui.html(..., sanitize=False)` only when
strictly needed.

Public API: `render_template_viewer(template_path, mask_path=None, ...)`
mounts the viewer in the current NiceGUI parent. Call `update_paths(...)`
on the returned controller to swap volumes without re-mounting (workbench
swaps templates often).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from nicegui import ui

logger = logging.getLogger(__name__)


# Volume cache keyed by (path, mtime). Templates are tiny but we still
# avoid re-mmaping on every render — and on a slider drag we'd otherwise
# re-open the .mrc per slice change.
@dataclass
class _CachedVolume:
    data: object  # numpy array (typed loosely so this module imports without numpy)
    apix_ang: Optional[float]
    nx: int
    ny: int
    nz: int


_VOLUME_CACHE: dict[tuple[str, int], _CachedVolume] = {}


def _load_volume(path: str) -> Optional[_CachedVolume]:
    if not path:
        return None
    try:
        st = Path(path).stat()
    except OSError:
        return None
    key = (path, int(st.st_mtime))
    cached = _VOLUME_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        import mrcfile
        import numpy as np

        with mrcfile.open(path, mode="r", permissive=True) as m:
            # np.array(..., copy=True) so the data outlives the mmap.
            data = np.array(m.data, copy=True)
            vx = float(getattr(m.voxel_size, "x", 0.0) or 0.0)
            apix = vx if vx > 0 else None
            nx = int(m.header.nx)
            ny = int(m.header.ny)
            nz = int(m.header.nz)
        cached = _CachedVolume(data=data, apix_ang=apix, nx=nx, ny=ny, nz=nz)
        _VOLUME_CACHE[key] = cached
        return cached
    except Exception as e:
        logger.warning("Could not load volume %s: %s", path, e)
        return None


@dataclass
class TemplateViewerController:
    """Returned by `render_template_viewer`. Lets the caller swap paths
    or toggle the mask overlay without re-mounting the component."""

    update_paths: callable  # type: ignore[assignment]
    set_mask_visible: callable  # type: ignore[assignment]


def render_template_viewer(
    template_path: str = "",
    mask_path: Optional[str] = None,
    *,
    height_px: int = 280,
    show_mask_default: bool = True,
) -> TemplateViewerController:
    """Mount the orthoslice triplet (XY / XZ / YZ at center) in the
    current NiceGUI parent. Returns a controller for swap/refresh."""

    state = {
        "template_path": template_path,
        "mask_path": mask_path,
        "vol": None,  # _CachedVolume
        "mask": None,  # _CachedVolume or None
        "z": 0,
        "y": 0,
        "x": 0,
        "show_mask": show_mask_default,
    }

    refs: dict = {}

    def _reload_volumes() -> None:
        state["vol"] = _load_volume(state["template_path"])
        state["mask"] = _load_volume(state["mask_path"]) if state["mask_path"] else None
        # Default slice indices to centers
        if state["vol"] is not None:
            state["z"] = state["vol"].nz // 2
            state["y"] = state["vol"].ny // 2
            state["x"] = state["vol"].nx // 2

    def _slice_xy() -> Optional[object]:
        v = state["vol"]
        if v is None:
            return None
        return v.data[state["z"], :, :]

    def _slice_xz() -> Optional[object]:
        v = state["vol"]
        if v is None:
            return None
        return v.data[:, state["y"], :]

    def _slice_yz() -> Optional[object]:
        v = state["vol"]
        if v is None:
            return None
        return v.data[:, :, state["x"]]

    def _mask_xy() -> Optional[object]:
        if not state["show_mask"]:
            return None
        m = state["mask"]
        if m is None or state["vol"] is None:
            return None
        if m.data.shape != state["vol"].data.shape:
            return None
        return m.data[state["z"], :, :]

    def _mask_xz() -> Optional[object]:
        if not state["show_mask"]:
            return None
        m = state["mask"]
        if m is None or state["vol"] is None:
            return None
        if m.data.shape != state["vol"].data.shape:
            return None
        return m.data[:, state["y"], :]

    def _mask_yz() -> Optional[object]:
        if not state["show_mask"]:
            return None
        m = state["mask"]
        if m is None or state["vol"] is None:
            return None
        if m.data.shape != state["vol"].data.shape:
            return None
        return m.data[:, :, state["x"]]

    def _refresh_plots() -> None:
        if state["vol"] is None:
            return
        for axis_key, slice_fn, mask_fn, axis_labels in (
            ("xy", _slice_xy, _mask_xy, ("x", "y")),
            ("xz", _slice_xz, _mask_xz, ("x", "z")),
            ("yz", _slice_yz, _mask_yz, ("y", "z")),
        ):
            plot = refs.get(axis_key)
            if plot is None:
                continue
            plot.figure = _build_slice_fig(slice_fn(), mask_fn(), axis_labels[0], axis_labels[1])
            plot.update()

    def _refresh_full() -> None:
        # Re-mount the slider row (slider max depends on volume dims) and
        # all plots.
        _reload_volumes()
        body = refs.get("body")
        if body is None:
            return
        body.clear()
        with body:
            _build_body()

    def _build_body() -> None:
        v = state["vol"]
        if v is None:
            with ui.row().classes("w-full p-4 items-center gap-2"):
                ui.icon("info_outline", size="16px").classes("text-gray-400")
                ui.label("No template loaded").classes("text-sm text-gray-400 italic")
            return

        # Plot row — each cell is a square container (aspect-ratio: 1) so
        # cubic templates render visually cubic without using plotly's
        # scaleanchor (per project memory: aspect-ratio CSS, not scaleanchor).
        with ui.row().classes("w-full gap-2 flex-nowrap justify-center"):
            for axis_key, slice_fn, mask_fn, axis_labels in (
                ("xy", _slice_xy, _mask_xy, ("x", "y")),
                ("xz", _slice_xz, _mask_xz, ("x", "z")),
                ("yz", _slice_yz, _mask_yz, ("y", "z")),
            ):
                refs[axis_key] = (
                    ui.plotly(_build_slice_fig(slice_fn(), mask_fn(), axis_labels[0], axis_labels[1]))
                    .style(f"height: {height_px}px; width: {height_px}px; aspect-ratio: 1;")
                )

        # Slider row — one slider per axis, plus mask-overlay toggle if mask present
        with ui.row().classes("w-full gap-3 items-center px-2 py-2 bg-gray-50 border-t border-b"):
            for label_text, key, max_v in (("z", "z", v.nz - 1), ("y", "y", v.ny - 1), ("x", "x", v.nx - 1)):
                with ui.row().classes("items-center gap-1 flex-1"):
                    ui.label(label_text).classes("text-xs text-gray-500 w-3")
                    sl = (
                        ui.slider(min=0, max=max(0, max_v), value=state[key], step=1)
                        .classes("flex-1")
                        .props("dense")
                    )
                    val_label = ui.label(str(state[key])).classes("text-xs text-gray-700 font-mono w-10")

                    def _on_change(e, k=key, lbl=val_label):
                        state[k] = int(e.value or 0)
                        lbl.set_text(str(state[k]))
                        _refresh_plots()

                    sl.on_value_change(_on_change)

            if state["mask"] is not None:
                with ui.row().classes("items-center gap-1 ml-2"):
                    sw = ui.switch("mask", value=state["show_mask"]).props("dense")

                    def _on_mask(e):
                        state["show_mask"] = bool(e.value)
                        _refresh_plots()

                    sw.on_value_change(_on_mask)

        # Footer: tiny info strip about what's loaded
        info_parts: list[str] = []
        if v.apix_ang:
            info_parts.append(f"{v.apix_ang:.3g} Å/px")
        info_parts.append(f"{v.nx}×{v.ny}×{v.nz}")
        if state["mask"] is not None:
            mshape = state["mask"].data.shape
            if mshape == v.data.shape:
                info_parts.append("mask: shape match ✓")
            else:
                info_parts.append(f"mask: shape MISMATCH {mshape} vs {v.data.shape}")
        with ui.row().classes("w-full px-2 py-1"):
            ui.label(" • ".join(info_parts)).classes("text-[11px] text-gray-500 font-mono")

    # ---- Mount ---------------------------------------------------------------
    _reload_volumes()
    refs["body"] = ui.column().classes("w-full gap-0")
    with refs["body"]:
        _build_body()

    # ---- Controller ---------------------------------------------------------
    def _update_paths(template_path: str, mask_path: Optional[str] = None) -> None:
        state["template_path"] = template_path or ""
        state["mask_path"] = mask_path or None
        _refresh_full()

    def _set_mask_visible(visible: bool) -> None:
        state["show_mask"] = bool(visible)
        _refresh_plots()

    return TemplateViewerController(update_paths=_update_paths, set_mask_visible=_set_mask_visible)


# ─── Plotly figure builders ────────────────────────────────────────────────


def _build_slice_fig(slice_2d, mask_2d, x_label: str, y_label: str) -> dict:
    """Build a heatmap-via-dict for one orthoslice. `slice_2d` and
    `mask_2d` are numpy arrays (or None). Mask is overlaid as a contour
    line at value=0.5."""
    if slice_2d is None:
        return {
            "data": [],
            "layout": {
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
                "annotations": [
                    {
                        "text": "no data",
                        "showarrow": False,
                        "font": {"size": 12, "color": "#9ca3af"},
                        "x": 0.5,
                        "y": 0.5,
                        "xref": "paper",
                        "yref": "paper",
                    }
                ],
                "paper_bgcolor": "white",
            },
            "config": {"displaylogo": False, "responsive": True},
        }

    z_list = slice_2d.tolist()
    traces: list[dict] = [
        {
            "type": "heatmap",
            "z": z_list,
            "colorscale": "Greys_r",
            "showscale": False,
            "hovertemplate": (f"{x_label}=%{{x}}, {y_label}=%{{y}}<br>val=%{{z:.3g}}<extra></extra>"),
        }
    ]
    if mask_2d is not None:
        traces.append(
            {
                "type": "contour",
                "z": mask_2d.tolist(),
                "contours": {"start": 0.5, "end": 0.5, "size": 0.001, "coloring": "lines"},
                "line": {"color": "#fbbf24", "width": 1.4},
                "showscale": False,
                "hoverinfo": "skip",
            }
        )

    layout = {
        "xaxis": {
            "title": {"text": x_label, "font": {"size": 9}},
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 8},
        },
        "yaxis": {
            "title": {"text": y_label, "font": {"size": 9}},
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 8},
        },
        "margin": {"t": 6, "b": 28, "l": 28, "r": 6},
        "paper_bgcolor": "white",
        "plot_bgcolor": "#0a0a0a",
        "showlegend": False,
    }
    return {"data": traces, "layout": layout, "config": {"displaylogo": False, "responsive": True}}

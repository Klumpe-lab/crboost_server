"""Segmentation layers as every view draws them: the Tomograms wall and its zoom, the Journey's
Reconstruct card and the pick viewer's slabs.

A layer is one feature of one producer (a Segment job, or an import batch). It is drawn as a fill
in the feature's colour, cut out by the overlay mask the producer wrote (p ≥ 0.5 over the slab a
picture shows, `services/segmentation/volumes.py`), under the pick dots. In the layers menu it is a
row beside the pick layers (`ui/components/layer_menu.py`) and follows the version as they do: it
fills a picture of the reconstruction its density traces to (a denoised version counts as the one it
was denoised from) and not one of another, unless switched by hand. Which layers show and how opaque
the fills are is one setting per project for all these views (`services/segmentation/display.py`); a
feature's colour is kept with the project. Colours and the opacity are CSS variables on the element
holding the pictures (`css_vars`), so changing them repaints no picture.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from pathlib import Path

from nicegui import ui

from services.dashboard_data import vis_asset_url
from services.job_lineage import recon_of
from services.models_base import FeatureOrigin, species_palette_color
from services.project_state import get_project_state_for
from services.segmentation import display
from services.segmentation.ingest import batch_of
from ui.components.layer_menu import LayerRow, layers_tool
from ui.components.recon_compare import recon_version_labels

_ORIGIN_OF_TOOL = {"easymode": FeatureOrigin.EASYMODE, "ais": FeatureOrigin.AIS}


def collect_layers(state, tilt_series: Iterable, dirs: dict[Path, str]) -> tuple[list[dict], dict[str, list[dict]]]:
    """``(layers, {ts: [{id, feature, xy, xz, dims}, ...]})`` from the registry's `tilt_series`.

    ``layers`` = ``[{id, feature, producer, producer_label, shared, origin, recon, recon_label, n}]``
    by feature: two producers of one feature are two layers in the feature's one colour, named by
    producer (`shared`). ``recon`` is the reconstruct instance the segmented density traces to: as
    the producer recorded it, else followed through the job's current inputs (which may have been
    re-pointed since). ``xy`` / ``xz`` are the URLs of the overlay masks; a volume without an X/Y
    mask on disk draws nothing. ``dims`` is the volume's extent (x, y, z). Reads disk (a few stats
    per volume)."""
    recon_labels = recon_version_labels(state)
    layers: dict[str, dict] = {}
    per_ts: dict[str, list[dict]] = {}
    for ts in tilt_series:
        outputs = ts.tomogram.outputs if ts.tomogram is not None else {}
        for producer, out in outputs.items():
            if out.output_type != "segmentation":
                continue
            live = recon_of(state, producer, dirs) if producer in state.jobs else None
            recon = out.recon_instance or live
            for feature, vol in out.volumes.items():
                if vol.overlay_xy is None or not Path(vol.overlay_xy).is_file():
                    continue
                lid = display.layer_id(producer, feature)
                if lid not in layers:
                    tool = vol.model.tool if vol.model is not None else None
                    layers[lid] = {
                        "id": lid,
                        "feature": feature,
                        "producer": producer,
                        "producer_label": _producer_label(state, producer),
                        # A volume that names no tool was registered from elsewhere.
                        "origin": _ORIGIN_OF_TOOL.get(tool, FeatureOrigin.IMPORT),
                        "recon": recon,
                        "recon_label": recon_labels.get(recon, recon or "") if len(recon_labels) > 1 else "",
                        "n": 0,
                    }
                layers[lid]["n"] += 1
                xz = vol.overlay_xz if vol.overlay_xz is not None and Path(vol.overlay_xz).is_file() else None
                per_ts.setdefault(ts.id, []).append(
                    {
                        "id": lid,
                        "feature": feature,
                        "xy": vis_asset_url(str(vol.overlay_xy)),
                        "xz": vis_asset_url(str(xz)) if xz is not None else None,
                        "dims": [int(out.shape_zyx[2]), int(out.shape_zyx[1]), int(out.shape_zyx[0])],
                    }
                )
    ordered = sorted(layers.values(), key=lambda layer: (layer["feature"], layer["producer"]))
    producers_of: dict[str, int] = {}
    for layer in ordered:
        producers_of[layer["feature"]] = producers_of.get(layer["feature"], 0) + 1
    for layer in ordered:
        layer["shared"] = producers_of[layer["feature"]] > 1
    return ordered, per_ts


def _producer_label(state, producer: str) -> str:
    batch = batch_of(producer)
    if batch is None:
        return producer
    rec = state.get_segmentation_import(batch)
    return rec.label if rec is not None else f"import {batch}"


# ── Drawing ──────────────────────────────────────────────────────────────────


def feature_var(feature: str) -> str:
    """The CSS variable holding a feature's colour; feature ids are lower-case ASCII slugs."""
    return f"--cb-feat-{feature}"


def fill_style(feature: str, mask_url: str) -> str:
    """One feature's fill over a picture's box: the feature's colour, cut out by its overlay mask
    (white with the feature as alpha) stretched over the box, which is the tomogram's X/Y (or X/Z)
    extent. Colour and opacity are the variables `css_vars` sets."""
    mask = f'url("{mask_url}")'
    return (
        "position: absolute; inset: 0; pointer-events: none; "
        f"background-color: var({feature_var(feature)}); opacity: var(--cb-feat-opacity); "
        f"-webkit-mask-image: {mask}; mask-image: {mask}; "
        "-webkit-mask-size: 100% 100%; mask-size: 100% 100%; "
        "-webkit-mask-repeat: no-repeat; mask-repeat: no-repeat;"
    )


def feature_color(project_path, feature: str) -> str:
    """The feature's colour: as registered, else the palette slot registering it would give."""
    entry = get_project_state_for(Path(project_path)).get_feature(feature)
    return entry.color if entry is not None else species_palette_color(feature)


def feature_name(project_path, layer: dict) -> str:
    entry = get_project_state_for(Path(project_path)).get_feature(layer["feature"])
    name = entry.name if entry is not None else layer["feature"]
    return f"{name} · {layer['producer_label']}" if layer["shared"] else name


def css_vars(project_path, features: Iterable[str]) -> str:
    """The variables the fills and badges read: one colour per feature, and the fills' opacity."""
    variables = [f"{feature_var(f)}: {feature_color(project_path, f)};" for f in sorted(set(features))]
    return " ".join([*variables, f"--cb-feat-opacity: {display.opacity(project_path)};"])


def set_feature_color(project_path, layer: dict, color: str) -> None:
    """Recolour a feature everywhere it is drawn; the colour is the project's, so it is saved. A
    feature not registered yet (one that landed while this server was not watching) is registered."""
    from backend import get_backend

    state = get_project_state_for(Path(project_path))
    registered = state.get_feature(layer["feature"]) is not None
    feature = state.add_feature(layer["feature"], origin=layer["origin"])
    if feature.color != color or not registered:
        feature.color = color
        state.mark_dirty()
        backend = get_backend()
        if backend is not None:
            asyncio.create_task(backend.save_project(Path(project_path), debounce_s=1.0))


def feature_rows(
    project_path, layers: list[dict], count_of: Callable[[dict], str], on_change: Callable[[], None]
) -> list[LayerRow]:
    """The segmentation layers as rows of the layers list (`layer_menu`): beside each name,
    `count_of(layer)`; a colour picked recolours the feature, then `on_change` runs."""

    def _recolor(layer: dict, color: str) -> None:
        set_feature_color(project_path, layer, color)
        on_change()

    return [
        LayerRow(
            id=layer["id"],
            name=feature_name(project_path, layer),
            count=count_of(layer),
            color=feature_color(project_path, layer["feature"]),
            recon=layer["recon"],
            recon_label=layer["recon_label"],
            on_color=lambda c, lay=layer: _recolor(lay, c),
        )
        for layer in layers
    ]


# ── One tomogram's pictures: the Journey's Reconstruct card, the pick viewer's slabs ─────────────


def layers_for_ts(state, project_path, ts_name: str) -> tuple[list[dict], list[dict]]:
    """``(layers, entries)`` of one tomogram, as `collect_layers` gives them. On the event loop: the
    cached registry is shared with every reader."""
    from services.job_lineage import job_dirs
    from services.tilt_series import get_registry_for

    registry = get_registry_for(Path(project_path))
    if not registry.has_tilt_series(ts_name):
        return [], []
    layers, per_ts = collect_layers(state, [registry.get_tilt_series(ts_name)], job_dirs(state))
    return layers, per_ts.get(ts_name, [])


class FeatureFills:
    """The segmentation fills a view draws over one tomogram's pictures, kept by layer so that the
    view's layers menu shows or hides them in place (`apply`) instead of rebuilding the pictures.
    `holder` is the element around the pictures that carries the colour and opacity variables;
    `recon` the reconstruct instance the view's main picture shows, which the menu is read against."""

    def __init__(self, project_path, layers: list[dict], entries: list[dict], holder, recon: str | None) -> None:
        self.project_path = project_path
        self.layers = layers
        self.entries = {e["id"]: e for e in entries}
        self.holder = holder
        self.recon = recon
        self._els: list[tuple[str, str | None, ui.element]] = []  # (layer, picture's reconstruction, fill)
        holder.style(css_vars(project_path, (layer["feature"] for layer in layers)))

    @property
    def dims(self) -> list[int] | None:
        """The tomogram's extent (x, y, z), from its volumes' grid."""
        return next((e["dims"] for e in self.entries.values() if e.get("dims")), None)

    def _shown(self, layer: dict, recon: str | None) -> bool:
        return display.is_shown(self.project_path, layer["id"], display.on_source(layer["recon"], recon))

    def draw(self, plane: str, recon: str | None = None) -> None:
        """Every layer's fill for a picture of `plane` ("xy" or "xz") of reconstruction `recon` (the
        view's by default), into the current container, which must be the picture's box. A layer the
        picture does not show is drawn invisible, so the menu shows it without a rebuild."""
        recon = self.recon if recon is None else recon
        for layer in self.layers:
            entry = self.entries.get(layer["id"])
            url = entry.get(plane) if entry is not None else None
            if not url:
                continue
            el = ui.element("div").style(fill_style(layer["feature"], url))
            el.set_visibility(self._shown(layer, recon))
            self._els.append((layer["id"], recon, el))

    def apply(self) -> None:
        """After a change in the menu: each fill's visibility, and the colours and opacity."""
        by_id = {layer["id"]: layer for layer in self.layers}
        for lid, recon, el in self._els:
            if not el.is_deleted:
                el.set_visibility(self._shown(by_id[lid], recon))
        if not self.holder.is_deleted:
            self.holder.style(css_vars(self.project_path, (layer["feature"] for layer in self.layers)))

    def rows(self, on_change: Callable[[], None]) -> list[LayerRow]:
        """The layers as rows of a layers list."""
        return feature_rows(self.project_path, self.layers, lambda _layer: "", on_change)

    def menu(self) -> None:
        """The view's layers menu, of the segmentations only."""
        layers_tool(
            self.project_path, lambda changed: ([], self.rows(changed)), shows=lambda: self.recon, on_change=self.apply
        )

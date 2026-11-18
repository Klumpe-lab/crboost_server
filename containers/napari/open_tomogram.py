"""Open tomograms in napari for a crboost viewer session.

Runs with the Python of napari_GL.sif (container_defs/napari_GL.def) on a session's VNC desktop, started by
containers/chimerax_artiax/curation_session.sh with CB_APP=napari. It imports nothing from crboost: the image
holds napari (with dask), mrcfile and numpy, and that is all this file may use.

crboost asks for volumes by writing ``open.json`` into the session directory:

    {"seq": 2, "title": "Position_1", "layers": [{"name": "Position_1", "path": "/abs/Position_1.mrc"}]}

A request with a higher ``seq`` replaces the layers the previous request opened; layers the user made stay. The
outcome goes back into ``opened.json`` (``seq``, ``ok``, ``error``, ``layers`` with each one's voxel size and the
level 3-D draws, ``renderer``), so the server can say whether the window shows what it asked for.

Volumes are memory-mapped, so a 2-D slice reads one plane from disk. 3-D draws a whole volume as one texture, which
software rendering (Mesa's llvmpipe, a session on a CPU node) cannot allocate for a 1 GB tomogram and a 24 GB card
cannot for a bin-2 one. A volume larger than the renderer's budget therefore gets binned levels (napari draws the
coarsest level in 3-D and picks the level by zoom in 2-D); building them reads the whole volume once, on the first
switch to 3-D. Scale is the header's voxel size in Å. A header without one opens at 1 per voxel, and the layer name,
the window title and ``opened.json`` say the voxel size is unknown.

By hand, volumes can be given as arguments instead of a session directory; launch_napari_vnc.sh beside this file
does that on a GPU node.

With ``--companion <checkout>`` (conf.yaml ``napari.companion``) the window is the colleague's viewer instead
(``napari_mrc_lowpass_fast.py``: low-pass, per-tomogram comments, particle points saved as STAR files), and a
request opens its first volume there. That viewer saves into the folder its controls name; a request carries
``outputs``, the tomogram's project folder ``<project>/Curation/napari/``, and the viewer holds one project at a
time. Before it opens another tomogram, what it holds is saved (comments CSV, every particle layer's STAR), so the
STARs then read back from the folder are what was on screen; moving to another project saves into the old
folder, forgets the old project's tomograms, and points the controls at the new one. A request without
``outputs`` (a volume outside every project) leaves the folders where the viewer started (the user's home) and
says so. That viewer draws 3-D from binned copies of its volumes (its view3d.py), so 3-D works on software
rendering too. A request may name ``bundle``, a JSON file crboost wrote beside the request with the tomogram's picks
and segmentations (services/visualization/napari_bundle.py); the viewer shows it with the tomogram (its
crboost_bundle.py), afresh on every request, so asking again for the same tomogram picks up new results.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import dask.array as da
import mrcfile
import napari
import numpy as np
from napari.utils.notifications import show_error, show_warning
from qtpy.QtCore import QTimer
from qtpy.QtWidgets import QApplication

logger = logging.getLogger("open_tomogram")

REQUEST = "open.json"
ACK = "opened.json"
POLL_MS = 1000
# The most voxels a 3-D texture may hold: 256 MB as float32 in software, 2 GB on a GPU.
BUDGET_3D_VOXELS = {"software": 64 * 2**20, "gpu": 512 * 2**20}


def gl_renderer(viewer: napari.Viewer) -> str:
    """The OpenGL renderer the canvas draws with: "NVIDIA B200/PCIe/SSE2", "llvmpipe (LLVM 20.1.2, 256 bits)"."""
    from vispy.gloo import gl

    try:
        viewer.window._qt_viewer.canvas._scene_canvas.native.makeCurrent()
    except AttributeError:
        # napari moved its private canvas: the renderer is unknown, and 3-D takes the software budget, which
        # holds on any renderer.
        logger.warning("could not reach napari's GL canvas; the renderer is unknown")
        return "unknown"
    return str(gl.glGetParameter(gl.GL_RENDERER))


def renderer_kind(renderer: str) -> str:
    software = ("llvmpipe", "softpipe", "software", "unknown")
    return "software" if any(word in renderer.lower() for word in software) else "gpu"


def levels_for_3d(data: np.ndarray, budget: int) -> list:
    """`data` and, while the coarsest level is over `budget` voxels, 2 × 2 × 2 binned (mean) levels of it. Lazy:
    a level is computed when napari draws it."""
    levels = [data]
    level = da.from_array(data, chunks=(2, -1, -1))
    while int(np.prod(levels[-1].shape)) > budget and min(levels[-1].shape) >= 4:
        level = da.coarsen(np.mean, level.astype(np.float32), {0: 2, 1: 2, 2: 2}, trim_excess=True)
        level = level.rechunk((2, -1, -1))
        levels.append(level)
    return levels


def voxel_size_zyx(mrc) -> tuple[float, float, float] | None:
    """The header's voxel size (Å) in napari's axis order, or None when the header does not carry one."""
    vs = mrc.voxel_size
    zyx = (float(vs.z), float(vs.y), float(vs.x))
    return zyx if all(v > 0 for v in zyx) else None


def contrast(data: np.ndarray) -> tuple[tuple[float, float], tuple[float, float]]:
    """The starting contrast window and the slider's range: the 0.5–99.5 percentiles and the minimum and maximum
    of every 7th voxel of nine planes from the middle 60 % of Z, where the specimen is; the top and bottom of a
    tomogram are mostly empty."""
    nz = data.shape[0]
    planes = np.unique(np.linspace(nz * 0.2, max(nz * 0.8 - 1, 0), num=min(9, nz)).astype(int))
    sample = np.concatenate([np.asarray(data[i], dtype=np.float32).ravel()[::7] for i in planes])
    lo, hi = (float(x) for x in np.percentile(sample, [0.5, 99.5]))
    vmin, vmax = float(sample.min()), float(sample.max())
    if hi <= lo:
        # A constant volume: napari needs an interval, so widen the single value.
        lo, hi = lo - 0.5, hi + 0.5
    return (lo, hi), (min(vmin, lo), max(vmax, hi))


def write_ack(session_dir: Path, ack: dict) -> None:
    tmp = session_dir / (ACK + ".tmp")
    tmp.write_text(json.dumps(ack))
    os.replace(tmp, session_dir / ACK)


class Session:
    """The viewer plus what crboost asked it to show: the layers of the last request and their open files."""

    def __init__(self, viewer: napari.Viewer, session_dir: Path | None, renderer: str):
        self.viewer = viewer
        self.session_dir = session_dir
        self.renderer = renderer
        self.budget_3d = BUDGET_3D_VOXELS[renderer_kind(renderer)]
        self.seq = -1
        self.request_mtime = None
        self.layers: list = []
        self.files: list = []
        self.timer = None

    def watch(self) -> None:
        """Check the session directory for a new request every second. The first check runs once the event
        loop has started, when the window has its maximized size, so the view fits the volume to the whole
        canvas."""
        self.timer = QTimer()
        self.timer.timeout.connect(self.poll)
        self.timer.start(POLL_MS)

    def poll(self) -> None:
        path = self.session_dir / REQUEST
        try:
            mtime = path.stat().st_mtime_ns
        except FileNotFoundError:
            return
        if mtime == self.request_mtime:
            return
        self.request_mtime = mtime
        try:
            request = json.loads(path.read_text())
        except (OSError, ValueError) as e:
            # crboost writes the request with a rename, so a torn read is a bug on that side: say so and wait
            # for the next write.
            logger.error("unreadable %s: %s", path, e)
            show_error(f"crboost's request could not be read: {e}")
            return
        seq = int(request.get("seq", 0))
        if seq <= self.seq:
            return
        self.apply(request, seq)

    def apply(self, request: dict, seq: int) -> None:
        """Replace the previous request's layers with this request's volumes, then report what opened."""
        self.seq = seq
        for layer in self.layers:
            if layer in self.viewer.layers:
                self.viewer.layers.remove(layer)
        for mrc in self.files:
            mrc.close()
        self.layers, self.files = [], []

        title = str(request.get("title") or "")
        opened, errors, unknown_scale = [], [], False
        for spec in request.get("layers") or []:
            path = str(spec.get("path") or "")
            name = str(spec.get("name") or Path(path).stem)
            t0 = time.perf_counter()
            try:
                mrc = mrcfile.mmap(path, mode="r", permissive=True)
            except (OSError, ValueError) as e:
                logger.error("cannot open %s: %s", path, e)
                errors.append(f"{name}: {e}")
                continue
            data = mrc.data
            if data is None or data.ndim != 3:
                mrc.close()
                shape = None if data is None else data.shape
                errors.append(f"{name}: not a 3-D volume (shape {shape})")
                continue
            voxel = voxel_size_zyx(mrc)
            if voxel is None:
                unknown_scale = True
                name = f"{name} (voxel size unknown)"
            levels = levels_for_3d(data, self.budget_3d)
            window, value_range = contrast(data)
            layer = self.viewer.add_image(
                levels if len(levels) > 1 else data,
                multiscale=len(levels) > 1,
                name=name,
                scale=voxel or (1.0, 1.0, 1.0),
                units="angstrom" if voxel else "pixel",
                axis_labels=("z", "y", "x"),
                contrast_limits=window,
                colormap="gray",
            )
            layer.contrast_limits_range = value_range
            layer.contrast_limits = window
            self.layers.append(layer)
            self.files.append(mrc)
            opened.append(
                {
                    "name": name,
                    "path": path,
                    "shape": list(data.shape),
                    "voxel_size": voxel,
                    "shape_3d": list(levels[-1].shape),
                }
            )
            dt = time.perf_counter() - t0
            logger.info(
                "opened %s %s %s voxel %s, 3-D at %s, in %.2f s",
                path,
                data.shape,
                data.dtype,
                voxel,
                levels[-1].shape,
                dt,
            )

        if self.layers:
            z_mid = opened[0]["shape"][0] // 2
            self.viewer.dims.ndisplay = 2
            self.viewer.dims.set_point(0, self.layers[0].data_to_world((z_mid, 0, 0))[0])
            self.viewer.reset_view()
        self.viewer.scale_bar.visible = not unknown_scale and bool(self.layers)
        self.viewer.title = f"{title} (voxel size unknown)" if unknown_scale else (title or "napari")
        if unknown_scale:
            show_warning("The volume's header has no voxel size: it is shown at 1 per voxel, not in Å.")
        if errors:
            show_error("Not opened: " + "; ".join(errors))

        if self.session_dir is not None:
            write_ack(
                self.session_dir,
                {
                    "seq": seq,
                    "ok": not errors and bool(opened),
                    "error": "; ".join(errors) or None,
                    "layers": opened,
                    "renderer": self.renderer,
                },
            )


class CompanionSession(Session):
    """The colleague's viewer driven by crboost's requests (see the module docstring)."""

    def __init__(self, viewer: napari.Viewer, session_dir: Path | None, renderer: str, companion: Path):
        super().__init__(viewer, session_dir, renderer)
        sys.path.insert(0, str(companion))
        from napari_mrc_lowpass_fast import LowPassController, RuntimeProfile
        from napari_mrc_lowpass_hpc import SPECTRUM_CACHE_MIN_MEMORY_GB, allocated_cpus, allocated_memory_gb

        workers = allocated_cpus()
        memory_gb = allocated_memory_gb(workers)
        cache = memory_gb is None or memory_gb >= SPECTRUM_CACHE_MIN_MEMORY_GB
        self.controller = LowPassController(
            viewer, profile=RuntimeProfile(name="crboost", workers=workers, cache_spectrum=cache)
        )
        self.outputs: Path | None = None
        logger.info("companion viewer from %s: %d FFT threads, spectrum cache %s", companion, workers, cache)

    def _save(self) -> None:
        c = self.controller
        if c.current_path is None:
            return
        c.save_comments_csv(c._comments_csv_path())
        c.save_all_particle_layers()

    def _point_at(self, outputs: Path | None) -> None:
        """Move the viewer's saves to `outputs`, after saving what it holds into the folder it had. The tomograms
        of that project are forgotten; the one on screen goes once the next has loaded (`_forget`)."""
        c = self.controller
        if self.outputs is not None:
            self._save()
        for path in list(c.loaded_paths):
            if path != c.current_path:
                self._forget(path)
        base = outputs if outputs is not None else Path.cwd()
        c.default_star_dir = base / "particle_stars"
        c.default_comments_csv = base / "tomogram_comments.csv"
        c.default_png_dir = base / "tomogram_pngs"
        c.annotation_controls.star_dir.value = str(c.default_star_dir)
        c.comment_controls.save_comments.value = str(c.default_comments_csv)
        self.outputs = outputs

    def _centre(self) -> None:
        """The middle slice, the whole volume in view. The viewer sets the pixel size after it adds a layer, which
        napari 0.10 leaves at the raw midpoint read in Å: slice 41 of 511, near the top, where tomograms look
        alike."""
        layer = self.controller.filtered_layer or self.controller.source_layer
        if layer is None or layer.data.ndim != 3:
            return
        z_mid = layer.data.shape[0] // 2
        self.viewer.dims.ndisplay = 2
        self.viewer.dims.set_point(0, layer.data_to_world((z_mid, 0, 0))[0])
        self.viewer.reset_view()

    def _forget(self, path: Path) -> None:
        """Drop `path` from the viewer's file list, comments and picks, so no later save writes it elsewhere."""
        c = self.controller
        if path in c.loaded_paths:
            c.loaded_paths.remove(path)
        c.comments.pop(path, None)
        c.bundle.forget(path)
        for per_path in c.particle_coordinates.values():
            per_path.pop(path, None)
        c._refresh_selected_file_choices()

    def _bundle(self, raw: str | None, path: Path) -> dict | None:
        """Show the request's bundle with `path`'s tomogram: what it drew and what crboost left out, or None when the
        request has none. A bundle that cannot be read leaves the tomogram open and says why."""
        if not raw:
            return None
        try:
            bundle = self.controller.load_bundle(Path(raw))
        except (OSError, ValueError) as e:
            logger.exception("bundle %s not shown", raw)
            show_warning(f"crboost's picks and segmentations are not shown: {e}")
            return {"error": f"{type(e).__name__}: {e}"}
        if Path(bundle["tomogram"]).resolve() != path:
            # A later request rewrote bundle.json before this one was read: that request follows within a second.
            return {"error": f"the bundle is {bundle['tomogram']}'s"}
        return {**self.controller.bundle.summary(), "notes": bundle.get("notes") or []}

    def apply(self, request: dict, seq: int) -> None:
        self.seq = seq
        c = self.controller
        title = str(request.get("title") or "")
        specs = request.get("layers") or []
        raw = request.get("outputs")
        outputs = Path(raw) if raw else None
        error = None
        opened = []
        bundle = None
        loaded = False
        try:
            if not specs:
                raise ValueError("the request names no volume")
            path = Path(str(specs[0].get("path") or "")).resolve()
            previous = c.current_path
            new_project = outputs != self.outputs or previous is None
            if new_project or path != previous:
                if new_project:
                    self._point_at(outputs)
                elif self.outputs is not None:  # outside a project the viewer saves only when asked
                    self._save()
                for spec in specs:
                    c.register_path(Path(str(spec.get("path") or "")))
                # The load reads the whole volume and its FFT on the Qt thread (~20 s on 4 CPUs) and the window
                # freezes meanwhile: say what it is doing before it does.
                self.viewer.title = f"Opening {title or path.stem}…"
                QApplication.processEvents()
                c.load_mrc(path)
                loaded = True
                if new_project and previous is not None and previous != path:
                    self._forget(previous)
                if c.default_comments_csv.is_file():
                    c.load_comments_csv(c.default_comments_csv)
                if c.default_star_dir.is_dir():
                    c.load_particle_stars_from_dir(c.default_star_dir)
            bundle = self._bundle(request.get("bundle"), path)
            if loaded:
                self._centre()
            data = c.original_data
            voxel = float(c.voxel_size_angstrom)
            opened.append(
                {
                    "name": str(specs[0].get("name") or path.stem),
                    "path": str(path),
                    "shape": list(data.shape) if data is not None else None,
                    "voxel_size": [voxel, voxel, voxel],
                    "shape_3d": list(data.shape) if data is not None else None,
                }
            )
            self.viewer.title = title or path.stem
            if outputs is None:
                show_warning(
                    f"This tomogram is outside every crboost project: comments and particles save in {Path.cwd()}."
                )
        except Exception as e:  # the viewer is the user's window: report and keep it open
            logger.exception("companion viewer could not open %s", specs)
            error = f"{type(e).__name__}: {e}"
            show_error(f"Not opened: {error}")
        if self.session_dir is not None:
            write_ack(
                self.session_dir,
                {
                    "seq": seq,
                    "ok": error is None and bool(opened),
                    "error": error,
                    "layers": opened,
                    "renderer": self.renderer,
                    "viewer": "companion",
                    "outputs": str(outputs) if outputs else None,
                    "bundle": bundle,
                },
            )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("volumes", nargs="*", help="MRC volumes to open (by hand)")
    parser.add_argument("--session-dir", type=Path, help="crboost session directory holding open.json")
    parser.add_argument(
        "--companion", type=Path, help="checkout of the colleague's viewer to run instead of plain napari"
    )
    args = parser.parse_args(argv)
    if not args.volumes and args.session_dir is None:
        parser.error("give volumes to open or --session-dir")
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(name)s %(message)s")

    viewer = napari.Viewer(title="napari")
    qt_window = getattr(viewer.window, "_qt_window", None)
    if qt_window is not None:
        qt_window.showMaximized()
    renderer = gl_renderer(viewer)
    logger.info("OpenGL renderer: %s", renderer)
    if args.companion is not None:
        session = CompanionSession(viewer, args.session_dir, renderer, args.companion)
    else:
        session = Session(viewer, args.session_dir, renderer)
    if args.volumes:
        request = {
            "title": Path(args.volumes[0]).stem,
            "layers": [{"path": str(Path(v).resolve())} for v in args.volumes],
        }
        # After the window is maximized, as for a session's first request.
        QTimer.singleShot(POLL_MS, lambda: session.apply(request, seq=0))
    if args.session_dir is not None:
        session.watch()
    napari.run()


if __name__ == "__main__":
    main()

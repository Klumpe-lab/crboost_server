"""Open tomograms in napari for a crboost viewer session.

Runs with the Python of napari_GL.sif (container_defs/napari_GL.def) on a session's VNC desktop, started by
containers/chimerax_artiax/curation_session.sh with CB_APP=napari. It imports nothing from crboost: the image
holds napari, mrcfile and numpy, and that is all this file may use.

crboost asks for volumes by writing ``open.json`` into the session directory:

    {"seq": 2, "title": "Position_1", "layers": [{"name": "Position_1", "path": "/abs/Position_1.mrc"}]}

A request with a higher ``seq`` replaces the layers the previous request opened; layers the user made stay. The
outcome goes back into ``opened.json`` (``seq``, ``ok``, ``error``, ``layers`` with each one's voxel size), so the
server can say whether the window shows what it asked for.

Volumes are memory-mapped, so a 2-D slice reads one plane from disk; 3-D reads the whole volume once. Scale is
the header's voxel size in Å. A header without one opens at 1 per voxel, and the layer name, the window title and
``opened.json`` say the voxel size is unknown.

By hand, volumes can be given as arguments instead of a session directory; launch_napari_vnc.sh beside this file
does that on a GPU node.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import mrcfile
import napari
import numpy as np
from napari.utils.notifications import show_error, show_warning
from qtpy.QtCore import QTimer

logger = logging.getLogger("open_tomogram")

REQUEST = "open.json"
ACK = "opened.json"
POLL_MS = 1000


def voxel_size_zyx(mrc) -> tuple[float, float, float] | None:
    """The header's voxel size (Å) in napari's axis order, or None when the header does not carry one."""
    vs = mrc.voxel_size
    zyx = (float(vs.z), float(vs.y), float(vs.x))
    return zyx if all(v > 0 for v in zyx) else None


def contrast_limits(data: np.ndarray) -> tuple[float, float]:
    """0.5–99.5 percentiles of every 7th voxel of nine planes from the middle 60 % of Z, where the specimen is;
    the top and bottom of a tomogram are mostly empty."""
    nz = data.shape[0]
    planes = np.unique(np.linspace(nz * 0.2, max(nz * 0.8 - 1, 0), num=min(9, nz)).astype(int))
    sample = np.concatenate([np.asarray(data[i], dtype=np.float32).ravel()[::7] for i in planes])
    lo, hi = (float(x) for x in np.percentile(sample, [0.5, 99.5]))
    if hi <= lo:
        # A constant volume: napari needs an interval, so widen the single value.
        lo, hi = lo - 0.5, hi + 0.5
    return lo, hi


def write_ack(session_dir: Path, ack: dict) -> None:
    tmp = session_dir / (ACK + ".tmp")
    tmp.write_text(json.dumps(ack))
    os.replace(tmp, session_dir / ACK)


class Session:
    """The viewer plus what crboost asked it to show: the layers of the last request and their open files."""

    def __init__(self, viewer: napari.Viewer, session_dir: Path | None):
        self.viewer = viewer
        self.session_dir = session_dir
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
            layer = self.viewer.add_image(
                data,
                name=name,
                scale=voxel or (1.0, 1.0, 1.0),
                units="angstrom" if voxel else "pixel",
                axis_labels=("z", "y", "x"),
                contrast_limits=contrast_limits(data),
                colormap="gray",
            )
            self.layers.append(layer)
            self.files.append(mrc)
            opened.append({"name": name, "path": path, "shape": list(data.shape), "voxel_size": voxel})
            dt = time.perf_counter() - t0
            logger.info("opened %s %s %s voxel %s in %.2f s", path, data.shape, data.dtype, voxel, dt)

        if self.layers:
            first = self.layers[0]
            z_mid = first.data.shape[0] // 2
            self.viewer.dims.ndisplay = 2
            self.viewer.dims.set_point(0, first.data_to_world((z_mid, 0, 0))[0])
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
                {"seq": seq, "ok": not errors and bool(opened), "error": "; ".join(errors) or None, "layers": opened},
            )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("volumes", nargs="*", help="MRC volumes to open (by hand)")
    parser.add_argument("--session-dir", type=Path, help="crboost session directory holding open.json")
    args = parser.parse_args(argv)
    if not args.volumes and args.session_dir is None:
        parser.error("give volumes to open or --session-dir")
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(name)s %(message)s")

    viewer = napari.Viewer(title="napari")
    qt_window = getattr(viewer.window, "_qt_window", None)
    if qt_window is not None:
        qt_window.showMaximized()
    session = Session(viewer, args.session_dir)
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

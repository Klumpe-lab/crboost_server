"""The zip of a project snapshot: the static viewer, its data and its pictures, one folder inside.

Worker side (`services/workers.py`): `write_bundle` takes only paths, strings and tuples, converts
every picture to the size the viewer shows it at, and writes the zip beside its final name before
renaming it there, so a half-written bundle is never served. What goes in is decided by
`ui/snapshot/collect.py`; the viewer (HTML, JS, CSS) lives in `ui/snapshot/viewer/`.

The bundle opens from disk (file://): no fetch, no ES modules. The data is a classic script
(`data.js`, `window.SNAPSHOT = …`), and ECharts — which NiceGUI ships only as an ES module chunk — is
rewritten into a classic script that sets `window.echarts` (`echarts_classic_script`).
"""

from __future__ import annotations

import importlib.util
import io
import json
import logging
import os
import re
import zipfile
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_JPEG_QUALITY = 80
# The viewer's own files, copied as they are.
VIEWER_FILES = ("index.html", "viewer.js", "viewer.css")


def echarts_classic_script() -> str:
    """NiceGUI's ECharts build as a classic script that sets `window.echarts`.

    NiceGUI ships ECharts as an ES module: `dist/index.js` re-exports one chunk's export as
    `echarts`. The chunk imports nothing, so dropping its `export {…}` list and wrapping it in a
    function leaves plain JavaScript. Raises when the layout is not that, rather than shipping a
    bundle whose charts are blank."""
    # Found, not imported: a worker never loads the UI package.
    spec = importlib.util.find_spec("nicegui")
    if spec is None or spec.origin is None:
        raise RuntimeError("NiceGUI is not installed; its ECharts build is what the snapshot ships")
    dist = Path(spec.origin).parent / "elements" / "echart" / "dist"
    entry = (dist / "index.js").read_text()
    m = re.search(r'import\s*\{([^}]*)\}\s*from\s*"\./([^"]+)"', entry)
    alias = re.search(r"(\w+)\s+as\s+echarts\b", entry)
    if m is None or alias is None:
        raise RuntimeError(f"Unexpected NiceGUI ECharts entry {dist / 'index.js'}: no `as echarts` re-export")
    chunk = (dist / m.group(2)).read_text()
    if re.search(r"^\s*import\s", chunk, re.M):
        raise RuntimeError(f"NiceGUI's ECharts chunk {m.group(2)} imports other chunks; it cannot run as one script")
    exports = list(re.finditer(r"export\s*\{([^}]*)\};?", chunk))
    if not exports:
        raise RuntimeError(f"NiceGUI's ECharts chunk {m.group(2)} has no export list")
    last = exports[-1]
    local = re.search(rf"([\w$]+)\s+as\s+{re.escape(alias.group(1))}\b", last.group(1))
    if local is None:
        raise RuntimeError(f"NiceGUI's ECharts chunk {m.group(2)} does not export {alias.group(1)}")
    body = chunk[: last.start()] + chunk[last.end() :]
    body = re.sub(r"//# sourceMappingURL=\S+", "", body)
    return f"(function(){{\n{body}\nwindow.echarts = {local.group(1)};\n}})();\n"


def _entry(name: str) -> zipfile.ZipInfo:
    """A zip entry that unpacks readable by all (`writestr` with a bare name gives 0600), dated now."""
    info = zipfile.ZipInfo(name, date_time=datetime.now().timetuple()[:6])
    info.external_attr = 0o644 << 16
    return info


def _picture(src: str, max_px: int) -> bytes:
    """`src` as the bundle carries it: a JPEG with its long edge at most `max_px`, or the file as it
    is when `max_px` is 0."""
    if max_px == 0:
        return Path(src).read_bytes()
    from PIL import Image

    with Image.open(src) as im:
        im = im.convert("L") if im.mode in ("I", "I;16", "F", "LA", "L") else im.convert("RGB")
        im.thumbnail((max_px, max_px), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, "JPEG", quality=_JPEG_QUALITY, optimize=True)
        return buf.getvalue()


def write_bundle(
    zip_path: str,
    root: str,
    viewer_dir: str,
    data_js: str,
    parts: list[tuple[str, str]],
    images: list[tuple[str, str, int]],
    *,
    progress_cb: Callable[[int, int, str], None],
) -> dict:
    """Write the snapshot zip to `zip_path`, everything under the folder `root`: the viewer files from
    `viewer_dir`, `data.js` setting `window.SNAPSHOT` to `data_js` (a JavaScript literal), `parts`
    as ``(path in the bundle, script)``, ECharts, and `images` as ``(source PNG, path in the bundle,
    max edge px)``. Returns ``{size, n_images, missing}``: `missing` names the pictures that
    could not be read (the viewer shows their place empty)."""
    final = Path(zip_path)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(f".{final.name}.part")
    missing: list[str] = []
    total = len(images) + 1
    try:
        with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for name in VIEWER_FILES:
                zf.write(Path(viewer_dir) / name, f"{root}/{name}")
            text = zipfile.ZIP_DEFLATED
            zf.writestr(_entry(f"{root}/data.js"), f"window.SNAPSHOT = {data_js};\nwindow.SNAPSHOT_TS = {{}};\n", text)
            for path, script in parts:
                zf.writestr(_entry(f"{root}/{path}"), script, text)
            zf.writestr(_entry(f"{root}/echarts.js"), echarts_classic_script(), text)
            progress_cb(1, total, "viewer written")
            for i, (src, dst, max_px) in enumerate(images, start=2):
                try:
                    blob = _picture(src, max_px)
                except (OSError, ValueError) as e:
                    # A picture deleted or unreadable since the collection: its place shows empty.
                    logger.warning("Snapshot picture %s skipped: %s", src, e)
                    missing.append(dst)
                    continue
                # Pictures are compressed already; deflating them again only costs time.
                zf.writestr(_entry(f"{root}/{dst}"), blob, zipfile.ZIP_STORED)
                if i % 20 == 0 or i == total:
                    progress_cb(i, total, f"pictures {i - 1}/{len(images)}")
            if missing:
                zf.writestr(_entry(f"{root}/missing.json"), json.dumps(missing), text)
        os.replace(tmp, final)
    finally:
        if tmp.exists():
            tmp.unlink()
    return {"size": final.stat().st_size, "n_images": len(images) - len(missing), "missing": missing}

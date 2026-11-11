"""The zip of a project snapshot: every captured scene as a static page, with the pictures, fonts and
styles it names, under one folder.

A scene is what a browser froze of one workspace view (`ui/snapshot/capture.js`): the view's DOM,
the page's CSS and the options of its charts, each naming files by the URL the live page used —
`/api/vis-asset?path=…`, `/api/tilt-thumb?path=…`, `/_nicegui/<version>/static/…`, `/static/…`.
`write_bundle` resolves each URL to the file the server would have served (only inside the project,
NiceGUI's static files and the server's own), copies or shrinks it into `assets/`, and rewrites the
scenes to name the copies. A URL it cannot resolve is left out and listed in `missing.json`.

Worker side (`services/workers.py`): paths, strings and dicts in, a small dict out. The zip is written
beside its final name and renamed there, so a half-written one is never served. The pages open from
disk (file://): no fetch, no modules; ECharts, which NiceGUI ships only as an ES module chunk, is
rewritten into a classic script (`echarts_classic_script`).
"""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import logging
import os
import re
import shutil
import zipfile
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

logger = logging.getLogger(__name__)

_JPEG_QUALITY = 80
# A picture stored wider than this many times its widest use on a page is cut down to that.
_SHOWN_FACTOR = 1.5
# The server's own routes, the only URLs a snapshot resolves (the rest, an SVG namespace or a font CDN,
# are not files of this project).
_OWN_PATHS = ("/api/", "/_nicegui/", "/static/")
# CSS url(...) and HTML/JSON strings naming one of the server's URLs, absolute (as capture.js wrote them).
# Inside an HTML attribute a quote is an entity: `style="mask-image: url(&quot;http://…&quot;)"`, so the URL
# also ends where `&quot;` / `&#39;` begins.
_URL_IN_TEXT = re.compile(r"""https?://(?:(?!&quot;|&#39;)[^\s"'()<>\\])+""")


def nicegui_dir() -> Path:
    """NiceGUI's package directory, found without importing it: a worker never loads the UI package."""
    spec = importlib.util.find_spec("nicegui")
    if spec is None or spec.origin is None:
        raise RuntimeError("NiceGUI is not installed; the snapshot ships its styles and ECharts")
    return Path(spec.origin).parent


def echarts_classic_script() -> str:
    """NiceGUI's ECharts build as a classic script that sets `window.echarts`.

    NiceGUI ships ECharts as an ES module: `dist/index.js` re-exports one chunk's export as
    `echarts`. The chunk imports nothing, so dropping its `export {…}` list and wrapping it in a
    function leaves plain JavaScript. Raises when the layout is not that, rather than shipping a
    bundle whose charts are blank."""
    dist = nicegui_dir() / "elements" / "echart" / "dist"
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


def _inside(path: Path, roots: list[Path]) -> bool:
    return any(path == r or r in path.parents for r in roots)


def _shrunk(src: Path, max_px: int) -> bytes:
    """`src` as a JPEG with its long edge at most `max_px` (what `/api/vis-asset?thumb=` serves)."""
    from PIL import Image

    with Image.open(src) as im:
        im = im.convert("L") if im.mode in ("I", "I;16", "F", "LA", "L") else im.convert("RGB")
        im.thumbnail((max_px, max_px), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, "JPEG", quality=_JPEG_QUALITY, optimize=True)
        return buf.getvalue()


def _fitted(blob: bytes, ext: str, shown: int) -> tuple[bytes, str]:
    """A picture no wider than `_SHOWN_FACTOR` × `shown` (its widest use on a page, CSS px; 0 = not
    known): cut down and, without transparency, as a JPEG. Anything else stays as it is."""
    if not shown or ext not in (".png", ".jpg", ".jpeg"):
        return blob, ext
    from PIL import Image

    with Image.open(io.BytesIO(blob)) as im:
        target = int(shown * _SHOWN_FACTOR)
        if im.width <= target * 1.2:
            return blob, ext
        im = im.resize((target, max(1, round(im.height * target / im.width))), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        if im.mode in ("RGBA", "LA", "P"):
            im.save(buf, "PNG", optimize=True)
            return buf.getvalue(), ".png"
        im.convert("L" if im.mode in ("I", "I;16", "F", "L") else "RGB").save(
            buf, "JPEG", quality=_JPEG_QUALITY, optimize=True
        )
        return buf.getvalue(), ".jpg"


def resolve(url: str, *, project: Path, nicegui_static: Path, server_static: Path) -> tuple[bytes, str] | None:
    """The bytes the server would serve for `url`, and the file extension to store them under; None when
    the URL is not one of the server's files a snapshot may carry (outside the project, another route)."""
    parts = urlsplit(url)
    path, query = unquote(parts.path), parse_qs(parts.query)
    if path in ("/api/vis-asset", "/api/tilt-thumb", "/api/project-preview"):
        target = Path((query.get("path") or [""])[0])
        try:
            target = target.resolve(strict=True)
        except (FileNotFoundError, RuntimeError):
            return None
        if not _inside(target, [project]) or not target.is_file():
            return None
        thumb = int((query.get("thumb") or ["0"])[0] or 0)
        if thumb and target.suffix == ".png":
            return _shrunk(target, min(max(thumb, 64), 2048)), ".jpg"
        return target.read_bytes(), target.suffix
    m = re.fullmatch(r"/_nicegui/[^/]+/static/(.+)", path)
    base, rel = (nicegui_static, m.group(1)) if m else (server_static, path[len("/static/") :])
    if not m and not path.startswith("/static/"):
        return None
    target = (base / rel).resolve()
    if not _inside(target, [base.resolve()]) or not target.is_file():
        return None
    return target.read_bytes(), target.suffix


def _asset_name(url: str, ext: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()[:20] + ext


def _rewrite(text: str, names: dict[str, str], prefix: str) -> str:
    """Every server URL in `text` (also HTML-escaped) as the bundle's copy, `prefix` + its name; a URL
    the bundle has no copy of is left as it is (it shows as missing, which is the truth)."""

    def sub(m: re.Match) -> str:
        url = m.group(0)
        name = names.get(url) or names.get(url.replace("&amp;", "&"))
        return prefix + name if name else url

    return _URL_IN_TEXT.sub(sub, text)


def _page(scene: dict, css_name: str, data: dict) -> str:
    """One scene as a page: the frozen body under the captured styles, then the scripts that bring its
    charts back and wire its navigation (`frozen.js`)."""
    data_js = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    title = scene["title"].replace("<", "&lt;")
    return (
        "<!doctype html>\n"
        f'<html class="{scene.get("html_class", "")}"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{title}</title>"
        f'<link rel="stylesheet" href="assets/{css_name}"><link rel="stylesheet" href="frozen.css"></head>\n'
        f"{scene['html']}\n"
        f'<script src="echarts.js"></script><script>window.FROZEN = {data_js};</script>'
        '<script src="frozen.js"></script>\n</html>\n'
    )


def write_bundle(
    capture_dir: str,
    zip_path: str,
    root: str,
    project: str,
    frozen_dir: str,
    server_static: str,
    nav: dict,
    *,
    progress_cb: Callable[[int, int, str], None],
) -> dict:
    """Zip the scenes under `capture_dir` (``scenes/<key>.json``, ``css/<hash>.css``, written as they
    arrived) to `zip_path`, everything under the folder `root`. `nav` is what every page's navigation
    is wired from (`ui/snapshot/plan.py`): ``{files: {scene key: file}, index: key, …}``. Returns
    ``{size, n_scenes, n_assets, missing}``."""
    cap = Path(capture_dir)
    files: dict[str, str] = nav["files"]
    scenes = []
    for key in files:
        p = cap / "scenes" / f"{hashlib.sha1(key.encode()).hexdigest()[:16]}.json"
        if p.exists():
            scenes.append(json.loads(p.read_text()))
    if not scenes:
        raise RuntimeError("No view was captured; there is nothing to put in the snapshot")
    urls = sorted(
        u
        for u in {u for s in scenes for u in s.get("assets", [])} | _css_urls(cap)
        if urlsplit(u).path.startswith(_OWN_PATHS)
    )
    shown: dict[str, int] = {}
    for s in scenes:
        for u, w in (s.get("shown") or {}).items():
            shown[u] = max(shown.get(u, 0), int(w))
    total = len(urls) + len(scenes) + 1
    kw = {
        "project": Path(project).resolve(),
        "nicegui_static": nicegui_dir() / "static",
        "server_static": Path(server_static),
    }
    final = Path(zip_path)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(f".{final.name}.part")
    names: dict[str, str] = {}
    missing: list[str] = []
    try:
        with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            text = zipfile.ZIP_DEFLATED
            for i, url in enumerate(urls, start=1):
                try:
                    got = resolve(url, **kw)
                except (OSError, ValueError) as e:
                    # Deleted or unreadable since the capture: the page shows the place empty.
                    logger.warning("Snapshot asset %s skipped: %s", url, e)
                    got = None
                if got is None:
                    missing.append(url)
                    continue
                blob, ext = _fitted(*got, shown.get(url, 0))
                names[url] = _asset_name(url, ext)
                packed = text if ext in (".css", ".json", ".svg", ".txt") else zipfile.ZIP_STORED
                zf.writestr(_entry(f"{root}/assets/{names[url]}"), blob, packed)
                if i % 25 == 0:
                    progress_cb(i, total, f"pictures {i}/{len(urls)}")
            # Styles: their url()s name the copies beside them in assets/.
            for css in sorted((cap / "css").glob("*.css")):
                zf.writestr(_entry(f"{root}/assets/{css.name}"), _rewrite(css.read_text(), names, ""), text)
            zf.writestr(_entry(f"{root}/echarts.js"), echarts_classic_script(), text)
            for name in ("frozen.js", "frozen.css"):
                zf.write(Path(frozen_dir) / name, f"{root}/{name}")
            for j, scene in enumerate(scenes, start=1):
                scene["html"] = _rewrite(scene["html"], names, "assets/")
                charts = json.loads(_rewrite(json.dumps(scene.get("charts", {})), names, "assets/"))
                data = {**nav, "scene": scene["key"], "charts": charts, "solo": scene.get("solo", [])}
                page = _page(scene, f"{scene['css']}.css", data)
                zf.writestr(_entry(f"{root}/{files[scene['key']]}"), page, text)
                if scene["key"] == nav["index"]:
                    zf.writestr(_entry(f"{root}/index.html"), page, text)
                progress_cb(len(urls) + j, total, f"pages {j}/{len(scenes)}")
            if missing:
                zf.writestr(_entry(f"{root}/missing.json"), json.dumps(missing, indent=1), text)
        os.replace(tmp, final)
    finally:
        if tmp.exists():
            tmp.unlink()
    shutil.rmtree(cap, ignore_errors=True)
    if missing:
        logger.warning("Snapshot %s: %d file(s) could not be resolved, e.g. %s", final, len(missing), missing[0])
    return {"size": final.stat().st_size, "n_scenes": len(scenes), "n_assets": len(names), "missing": len(missing)}


def _css_urls(cap: Path) -> set[str]:
    return {m.group(0) for css in (cap / "css").glob("*.css") for m in _URL_IN_TEXT.finditer(css.read_text())}

"""Screenshot the app's pages with a headless Chromium, so a page can be looked at without a browser at hand.

Two roles in one file. Run as a tool (default) it starts a small NiceGUI server with the dev mock pages in the
project's own Python, opens each route in Playwright's Chromium and writes PNGs; `--url` points it at a server
already running instead (the real app, a project page). Run with `--serve PORT` it is that small server (the tool
starts it as a child process in `venv/bin/python3`).

The tool itself runs in a separate Python that has Playwright: ~/.local/share/crboost-dev/pw (made with
`uv venv`, `uv pip install playwright`, `playwright install chromium`; the browser lands in ~/.cache/ms-playwright).
Neither is part of the project's locked environment on purpose.

    ~/.local/share/crboost-dev/pw/bin/python scripts/dev_screenshot.py                 # both dev pages, full page
    ~/.local/share/crboost-dev/pw/bin/python scripts/dev_screenshot.py --panels         # one PNG per design panel
    ~/.local/share/crboost-dev/pw/bin/python scripts/dev_screenshot.py --url http://localhost:24300 /p/x/jobs

PNGs go to notes/screens/ (git-ignored) unless --out says otherwise.
"""

from __future__ import annotations

import argparse
import os
import re
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEV_ROUTES = ("/dev/io-ideas", "/dev/io-ideas/tree", "/dev/io-ideas/branch")
PANEL_CLASS = "cb-mock-panel"


# ── the small server (child process, project Python) ─────────────────────────


def serve(port: int) -> None:
    sys.path.insert(0, str(ROOT))
    os.chdir(ROOT)
    from nicegui import Client, ui

    from ui.dashboard.css import ensure_assets_loaded
    from ui.io_ideas_preview import render_io_ideas
    from ui.io_layouts_preview import render_io_layouts
    from ui.tree_ideas_preview import render_tree_ideas

    # The real page shell's head block (fonts, base styles), taken from ui/main_ui.py as written, so the mock
    # pages are shot the way the app serves them.
    src = (ROOT / "ui" / "main_ui.py").read_text()
    m = re.search(r'ui\.add_head_html\("""(.*?)"""\)', src, re.S)
    shell = m.group(1) if m else ""

    pages = {
        "/dev/io-layouts": render_io_layouts,
        "/dev/io-ideas": render_io_ideas,
        "/dev/io-ideas/tree": render_tree_ideas,
    }
    try:
        from ui.branch_map_preview import render_branch_map

        pages["/dev/io-ideas/branch"] = render_branch_map
    except ImportError:
        pass

    for route, render in pages.items():

        def make(render=render):
            async def page(client: Client):
                # Per page, not at module level: NiceGUI refuses ui.page beside UI defined in the global scope.
                ui.add_head_html(shell)
                await client.connected()
                ensure_assets_loaded()
                render()

            return page

        ui.page(route)(make())

    @ui.page("/")
    def index():
        for route in pages:
            ui.link(route, route)

    ui.run(host="127.0.0.1", port=port, show=False, reload=False, title="crboost dev pages", storage_secret="dev")


# ── the tool (parent process, Playwright Python) ─────────────────────────────


def free_port(start: int = 24390, end: int = 24399) -> int:
    for port in range(start, end + 1):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise SystemExit(f"no free port in {start}–{end}")


def wait_up(url: str, seconds: float = 60, child: subprocess.Popen | None = None) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if child is not None and child.poll() is not None:
            raise SystemExit(f"the dev-pages server exited with {child.returncode}")
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if r.status == 200:
                    return
        except Exception:  # not up yet: refused, reset, or a 5xx while starting
            time.sleep(0.3)
    raise SystemExit(f"{url} did not come up in {seconds:.0f}s")


def slug(route: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", route.lower()).strip("-") or "index"


def shoot(base: str, routes: list[str], out: Path, width: int, height: int, panels: bool, settle_ms: int) -> list[Path]:
    from playwright.sync_api import sync_playwright

    out.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": width, "height": height}, device_scale_factor=1)
        for route in routes:
            page.goto(base + route, wait_until="networkidle")
            page.wait_for_timeout(settle_ms)  # fonts, NiceGUI's post-connect CSS, Vue's first paint
            if panels:
                boxes = page.locator(f".{PANEL_CLASS}")
                n = boxes.count()
                titles = page.locator(f".{PANEL_CLASS}-title").all_inner_texts()
                for i in range(n):
                    name = slug(titles[i]) if i < len(titles) else f"panel-{i + 1}"
                    path = out / f"{slug(route)}--{i + 1}-{name}.png"
                    boxes.nth(i).scroll_into_view_if_needed()
                    boxes.nth(i).screenshot(path=str(path))
                    written.append(path)
                if n == 0:
                    print(f"{route}: no .{PANEL_CLASS} elements; shooting the page instead", file=sys.stderr)
            if not panels or not written:
                path = out / f"{slug(route)}.png"
                page.screenshot(path=str(path), full_page=True)
                written.append(path)
        browser.close()
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("routes", nargs="*", help=f"routes to shoot (default: {' '.join(DEV_ROUTES)})")
    ap.add_argument("--url", help="a running server to shoot instead of starting the dev-pages server")
    ap.add_argument("--out", type=Path, default=ROOT / "notes" / "screens")
    ap.add_argument("--width", type=int, default=1500)
    ap.add_argument("--height", type=int, default=900)
    ap.add_argument("--panels", action="store_true", help=f"one PNG per .{PANEL_CLASS} element")
    ap.add_argument("--settle", type=int, default=1200, help="ms to wait after network idle")
    ap.add_argument("--serve", type=int, metavar="PORT", help=argparse.SUPPRESS)
    args = ap.parse_args()
    if args.serve:
        serve(args.serve)
        return

    routes = args.routes or list(DEV_ROUTES)
    child = None
    if args.url:
        base = args.url.rstrip("/")
    else:
        port = free_port()
        base = f"http://127.0.0.1:{port}"
        python = ROOT / "venv" / "bin" / "python3"
        child = subprocess.Popen(
            [str(python), str(Path(__file__).resolve()), "--serve", str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_up(base + "/", child=child)
        except SystemExit:
            child.kill()
            print(child.stderr.read()[-3000:] if child.stderr else "", file=sys.stderr)
            raise
    try:
        for path in shoot(base, routes, args.out, args.width, args.height, args.panels, args.settle):
            print(path)
    finally:
        if child is not None:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()


if __name__ == "__main__":
    main()

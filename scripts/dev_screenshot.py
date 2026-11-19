"""Screenshot pages of a running crboost server with a headless Chromium, so a page can be looked at without a
browser at hand.

It runs in a separate Python that has Playwright: ~/.local/share/crboost-dev/pw (made with `uv venv`,
`uv pip install playwright`, `playwright install chromium`; the browser lands in ~/.cache/ms-playwright). Neither is
part of the project's locked environment on purpose. The first route should carry the server's token
(`?token=$(cat ~/.crboost/token)`); the cookie it sets covers the routes after it.

    ~/.local/share/crboost-dev/pw/bin/python scripts/dev_screenshot.py --url http://localhost:24300 "/?token=…" /p/x

PNGs go to notes/screens/ (git-ignored) unless --out says otherwise.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def slug(route: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", route.split("?", 1)[0].lower()).strip("-") or "index"


def shoot(base: str, routes: list[str], out: Path, width: int, height: int, settle_ms: int) -> list[Path]:
    from playwright.sync_api import sync_playwright

    out.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": width, "height": height}, device_scale_factor=1)
        for route in routes:
            page.goto(base + route, wait_until="networkidle")
            page.wait_for_timeout(settle_ms)  # fonts, NiceGUI's post-connect CSS, Vue's first paint
            path = out / f"{slug(route)}.png"
            page.screenshot(path=str(path), full_page=True)
            written.append(path)
        browser.close()
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("routes", nargs="+", help="routes to shoot, the first one with the token")
    ap.add_argument("--url", required=True, help="the running server, e.g. http://localhost:24300")
    ap.add_argument("--out", type=Path, default=ROOT / "notes" / "screens")
    ap.add_argument("--width", type=int, default=1500)
    ap.add_argument("--height", type=int, default=900)
    ap.add_argument("--settle", type=int, default=1200, help="ms to wait after network idle")
    args = ap.parse_args()
    for path in shoot(args.url.rstrip("/"), args.routes, args.out, args.width, args.height, args.settle):
        print(path)


if __name__ == "__main__":
    main()

"""Measure the branch map in a headless Chromium against a running server: element and DOM counts, the size of each
websocket update, how long a click takes to come back and settle, long tasks, and frame times while scrolling.

It opens a project's Jobs view, shows the map from the roster's map icon, and clicks view controls only (card sizes,
the flags on the Pipeline title line, a card's header); nothing in the project changes. It runs in the Playwright
Python of scripts/dev_screenshot.py:

    ~/.local/share/crboost-dev/pw/bin/python scripts/dev_map_probe.py http://localhost:24300 my_project notes/screens

The numbers print one line per action and land in <out>/probe.json, with a PNG of the map small and all large.
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

if len(sys.argv) < 3:
    sys.exit(__doc__)
BASE = sys.argv[1]
PROJECT = sys.argv[2]
OUT = Path(sys.argv[3]) if len(sys.argv) > 3 else Path(".")
TOKEN = (Path.home() / ".crboost" / "token").read_text().strip()

INIT = r"""
window.__lt = [];
try {
  new PerformanceObserver((l) => l.getEntries().forEach((e) => window.__lt.push([e.startTime, e.duration])))
    .observe({ entryTypes: ['longtask'] });
} catch (e) {}
window.__msgs = [];
window.__hook = () => {
  if (window.__hooked || !window.socket) return !!window.__hooked;
  window.socket.onAny((ev, ...args) => {
    let n = 0; try { n = JSON.stringify(args).length; } catch (e) {}
    window.__msgs.push([performance.now(), ev, n]);
  });
  window.__hooked = true; return true;
};
window.__mut = { last: 0 };
new MutationObserver(() => { window.__mut.last = performance.now(); })
  .observe(document, { childList: true, subtree: true, attributes: true, characterData: true });
"""

COUNT = r"""
(full) => {
  const els = (typeof mounted_app !== 'undefined' && mounted_app) ? mounted_app.elements : {};
  const tags = {};
  for (const e of Object.values(els)) { if (e) tags[e.tag] = (tags[e.tag] || 0) + 1; }
  const world = document.querySelector('.cb-bm-world');
  const cards = document.querySelectorAll('.cb-bm-card');
  const out = {
    nicegui_elements: Object.keys(els).length,
    dom_nodes: document.getElementsByTagName('*').length,
    map_dom_nodes: world ? world.getElementsByTagName('*').length : 0,
    cards: cards.length,
    large_cards: document.querySelectorAll('.cb-bm-node.large').length,
    world_px: world ? [parseFloat(world.style.width), parseFloat(world.style.height)] : null,
  };
  if (full) out.tags = Object.entries(tags).sort((a, b) => b[1] - a[1]).slice(0, 10);
  return out;
}
"""


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    report: dict = {}
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1700, "height": 1000}, device_scale_factor=1)
        page.add_init_script(INIT)
        page.goto(f"{BASE}/?token={TOKEN}", wait_until="networkidle")
        page.goto(f"{BASE}/p/{PROJECT}/pipeline", wait_until="networkidle")
        page.wait_for_selector('[title^="Show the branch map"]', timeout=60000)
        page.wait_for_function("window.__hook()", timeout=30000)
        settle(page)
        report["roster_only"] = page.evaluate(COUNT, False)

        def act(name: str, js: str, *, full: bool = False, shot: str | None = None) -> None:
            t0 = page.evaluate("performance.now()")
            ok = page.evaluate(js)
            if not ok:
                report[name] = {"skipped": "target not found"}
                return
            settle(page)
            msgs = [m for m in page.evaluate("window.__msgs") if m[0] >= t0 and m[1] == "update"]
            lt = [d for s, d in page.evaluate("window.__lt") if s >= t0]
            last_mut = page.evaluate("window.__mut.last")
            report[name] = {
                "server_reply_ms": round(msgs[0][0] - t0) if msgs else None,
                "updates": len(msgs),
                "update_kb": round(sum(m[2] for m in msgs) / 1024, 1),
                "settled_ms": round(last_mut - t0) if last_mut > t0 else None,
                "longtask_ms": round(sum(lt)),
                "longtask_max_ms": round(max(lt, default=0)),
                **page.evaluate(COUNT, full),
            }
            if shot:
                page.screenshot(path=str(OUT / shot))

        click_title = (
            "(() => { const b = document.querySelector('[title^=\"Show the branch map\"]'); "
            "if (!b) return false; b.click(); return true; })()"
        )
        flag = (
            "(() => { const b = [...document.querySelectorAll('.cb-head-tools button')]"
            ".find((x) => x.textContent.trim() === '{icon}'); if (!b) return false; b.click(); return true; })()"
        )
        act("open_map", click_title, full=True, shot="probe-open.png")
        act(
            "select_card",
            "(() => { const h = document.querySelectorAll('.cb-bm-card .cb-bm-head')[2]; "
            "if (!h) return false; h.click(); return true; })()",
        )
        act(
            "one_card_large",
            "(() => { const s = document.querySelectorAll('.cb-bm-size')[3]; "
            "if (!s) return false; s.click(); return true; })()",
        )
        act(
            "one_card_small",
            "(() => { const s = [...document.querySelectorAll('.cb-bm-size')]"
            ".find((x) => x.textContent.trim() === 'close_fullscreen');"
            " if (!s) return false; s.click(); return true; })()",
        )
        report["scroll_small"] = pan(page)
        act("all_large", flag.replace("{icon}", "unfold_more"), full=True, shot="probe-all-large.png")
        report["scroll_large"] = pan(page)
        act("all_small", flag.replace("{icon}", "unfold_less"))
        act("magazines_off", flag.replace("{icon}", "view_week"))
        act("magazines_on", flag.replace("{icon}", "view_week"))
        # A quiet minute: how often the timers rebuild with nothing happening.
        t0 = page.evaluate("performance.now()")
        page.wait_for_timeout(30000)
        msgs = [m for m in page.evaluate("window.__msgs") if m[0] >= t0 and m[1] == "update"]
        report["idle_30s"] = {"updates": len(msgs), "update_kb": round(sum(m[2] for m in msgs) / 1024, 1)}
        browser.close()
    (OUT / "probe.json").write_text(json.dumps(report, indent=1))
    for k, v in report.items():
        print(k, json.dumps(v))


def settle(page, quiet_ms: int = 1500, cap_ms: int = 40000) -> None:
    """Wait until the page has not changed for `quiet_ms`."""
    t = time.perf_counter()
    page.wait_for_timeout(300)
    while (time.perf_counter() - t) * 1000 < cap_ms:
        page.wait_for_timeout(250)
        if page.evaluate(f"performance.now() - window.__mut.last > {quiet_ms}"):
            return


def pan(page) -> dict:
    """Scroll the map down and back with the wheel; frame intervals from requestAnimationFrame."""
    box = page.locator(".cb-bm-scroll").bounding_box()
    if box is None:
        return {"skipped": "no map"}
    lt0 = page.evaluate("performance.now()")
    page.evaluate(
        "window.__fr = []; window.__frOn = true; (function loop(t0) { requestAnimationFrame((t) => { "
        "if (t0) window.__fr.push(t - t0); if (window.__frOn) loop(t); }); })(0);"
    )
    cx, cy = box["x"] + box["width"] * 0.5, box["y"] + box["height"] * 0.5
    page.mouse.move(cx, cy)
    for i in range(48):
        page.mouse.wheel(0, 80 if i < 24 else -80)
        page.wait_for_timeout(16)
    page.wait_for_timeout(300)
    page.evaluate("window.__frOn = false")
    fr = [f for f in page.evaluate("window.__fr") if f > 0]
    lt = [d for s, d in page.evaluate("window.__lt") if s >= lt0]
    return {
        "frames": len(fr),
        "frame_p50_ms": round(statistics.median(fr), 1) if fr else None,
        "frame_p95_ms": round(sorted(fr)[int(len(fr) * 0.95) - 1], 1) if len(fr) > 5 else None,
        "frame_max_ms": round(max(fr), 1) if fr else None,
        "longtask_ms": round(sum(lt)),
    }


if __name__ == "__main__":
    main()

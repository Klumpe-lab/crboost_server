"""Click through the particle map on a scratch copy of a project in a headless Chromium, and fail on any error.

Three roles in one file:

- `fixture SRC DEST` (the project's Python): a scratch copy of project SRC at DEST/<its name>: project_params.json
  with its project_path rewritten, the small folders copied, every job directory under External/ and the
  Snapshots symlinked (read only), and a class-1 set made of the first Class 3D, so the map has a set hanging
  off a class row. Nothing is written into SRC.
- `serve PORT HOME` (the project's Python): the app (`server_app.setup_app`) with the pipeline monitor and the
  curation watcher off (a second server must never reconcile the user's live projects), its own HOME (prefs,
  NiceGUI storage, logs), no token gate, on 127.0.0.1.
- default (the Playwright Python of `scripts/dev_screenshot.py`): makes the fixture, serves it, and steps through
  the view: it opens from the PARTICLES header; the class rows; the set hangs off its class row; the large card's
  blocks; the URL names the picked node; a job card's header opens the job page; ✕ and Esc come back onto the map
  with the Jobs view off as it was; Back. One PNG per step in notes/screens/particles/. Any page error, or an
  ERROR or Traceback line of the server, fails the run.

    ~/.local/share/crboost-dev/pw/bin/python scripts/dev_particle_map_check.py [--project SRC]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "notes" / "screens" / "particles"
DEFAULT_SRC = Path("/groups/<lab>/crboost_data/copia-tutorial-20261015-1551")
SYMLINKED = {"External", "Snapshots", "frames"}


# ── the fixture (project Python) ─────────────────────────────────────────────


def fixture(src: Path, dest_root: Path) -> None:
    dest = dest_root / src.name
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    for entry in src.iterdir():
        target = dest / entry.name
        if entry.name == "project_params.json":
            d = json.loads(entry.read_text())
            d["project_path"] = str(dest)
            target.write_text(json.dumps(d, indent=2))
        elif entry.name == "External":
            target.mkdir()
            for job in entry.iterdir():
                (target / job.name).symlink_to(job)
        elif entry.name in SYMLINKED:
            target.symlink_to(entry)
        elif entry.is_dir():
            shutil.copytree(entry, target, symlinks=True)
        else:
            shutil.copy2(entry, target)
    sys.path.insert(0, str(ROOT))
    from services.particles import set_ops
    from services.project_state import get_project_state_for

    state = get_project_state_for(dest)
    class3d = next(iid for iid, jm in state.jobs.items() if jm.job_type and jm.job_type.value == "class3d")
    species = state.jobs[class3d].species_id or ""
    ps = set_ops.create_class_subset_set(
        dest, state, class3d_instance=class3d, classes=[1], name="class 1", species_id=species
    )
    print(json.dumps({"project": str(dest), "class3d": class3d, "set": ps.id, "n": ps.n_particles}))


# ── the server (project Python) ──────────────────────────────────────────────


def serve(port: int, home: Path) -> None:
    os.environ["HOME"] = str(home)
    os.environ["CRBOOST_PORT"] = str(port)
    sys.path.insert(0, str(ROOT))
    os.chdir(ROOT)
    import uvicorn

    from services.log_setup import setup_logging

    # The server's own logging (its NiceGUI filters included), into the scratch HOME's log and the console.
    setup_logging(debug=False, port=port)

    from services.curation.watcher import CurationWatcher
    from services.scheduling_and_orchestration.pipeline_monitor import PipelineMonitor

    async def off(self) -> None:
        return None

    PipelineMonitor.start = off
    PipelineMonitor.stop = off
    CurationWatcher.start = off
    CurationWatcher.stop = off
    import server_app

    app = server_app.setup_app(port)
    uvicorn.run(app, host="127.0.0.1", port=port, log_config=None)


# ── the steps (Playwright Python) ────────────────────────────────────────────


def _project_python() -> str:
    return str(ROOT / "venv" / "bin" / "python3")


def check(src: Path) -> int:
    from playwright.sync_api import sync_playwright

    sys.path.insert(0, str(ROOT / "scripts"))
    from dev_screenshot import free_port, wait_up

    OUT.mkdir(parents=True, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix="crboost-pm-check-"))
    made = subprocess.run(
        [_project_python(), __file__, "fixture", str(src), str(scratch / "projects")],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if made.returncode != 0:
        print(made.stdout, made.stderr)
        return 1
    fx = json.loads(made.stdout.strip().splitlines()[-1])
    print("fixture", fx)
    project, class3d, set_node = Path(fx["project"]), fx["class3d"], "set:" + fx["set"]
    home = scratch / "home"
    home.mkdir()
    port = free_port(24390, 24399)
    child = subprocess.Popen(
        [_project_python(), __file__, "serve", str(port), str(home)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=ROOT,
    )
    base = f"http://127.0.0.1:{port}"
    errors: list[str] = []
    try:
        wait_up(base + "/", 120, child)
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={"width": 2400, "height": 1400})
            page.on("console", lambda m: errors.append("console: " + m.text) if m.type == "error" else None)
            page.on("pageerror", lambda e: errors.append("page: " + str(e)))
            page.goto(f"{base}/p/{project.name}?base={project.parent}", wait_until="networkidle")
            page.wait_for_timeout(2500)
            n = [0]

            def step(name: str) -> None:
                page.wait_for_timeout(700)
                n[0] += 1
                page.screenshot(path=str(OUT / f"{n[0]:02d}-{name}.png"))
                print("step", n[0], name, "·", page.url)

            def pm_card(nid: str):
                return page.locator(f'.cb-bm-canvas[data-map="particles"] .cb-bm-card[data-nid="{nid}"]')

            canvas = page.locator('.cb-bm-canvas[data-map="particles"]')
            page.locator(".cb-pm-open").first.click()
            canvas.wait_for(state="visible", timeout=20000)
            pm_card(set_node).wait_for(state="visible", timeout=30000)
            step("opened-from-the-header")
            assert "/particles" in page.url, page.url
            rows = pm_card(class3d).get_by_text("class 1 ·")
            assert rows.count() == 1, "no class row on the Class 3D card"
            assert "(Class 3D estimate)" in rows.inner_text(), rows.inner_text()
            cable = page.locator(f'.cb-bm-canvas[data-map="particles"] path[data-a="{class3d}"][data-b="{set_node}"]')
            assert cable.count() == 1, "the set does not hang off the Class 3D"
            ay = float(cable.get_attribute("data-ay"))
            h = pm_card(class3d).evaluate("el => parseFloat(el.style.height)")
            print("set cable leaves the class row at", ay, "of a card", h, "tall")
            assert ay < h - 22, "the set's cable leaves the footer, not the class row"
            # The set: its size toggle makes it large and picked.
            pm_card(set_node).locator(".cb-bm-size").click()
            page.wait_for_timeout(800)
            step("set-large")
            assert pm_card(set_node).get_by_text("RECIPE", exact=True).count() == 1
            assert pm_card(set_node).get_by_text("PARTICLES", exact=True).count() == 1
            assert "set%3A" in page.url or "set:" in page.url, page.url
            # The Class 3D large: its size toggle.
            pm_card(class3d).locator(".cb-bm-size").click()
            page.wait_for_timeout(2500)  # the facts load off the loop
            step("class3d-large")
            for block in ("PARAMETERS · as run", "RESULTS", "PARTICLES"):
                assert pm_card(class3d).get_by_text(block, exact=True).count() == 1, f"no {block} block"
            assert page.url.split("?")[0].endswith(f"/particles/{class3d}"), page.url
            # The Jobs view off, then a job page from a header, and its ✕ back onto the map.
            for _ in range(2):  # a job page open closes first, then the view
                if page.locator("#cb-view-pipeline").is_visible():
                    page.locator("#cb-view-pipeline .cb-view-close").last.click()
                    page.wait_for_timeout(800)
            assert not page.locator("#cb-view-pipeline").is_visible(), "the Jobs view did not close"
            step("jobs-view-off")
            pm_card(class3d).locator(".cb-bm-head").click()
            page.wait_for_timeout(1500)
            step("job-page-from-the-map")
            assert f"/job/{class3d}" in page.url, page.url
            assert not canvas.is_visible(), "the map still shows beside the job page"
            page.locator("#cb-view-pipeline .cb-view-close").last.click()
            page.wait_for_timeout(1000)
            step("closed-back-onto-the-map")
            assert canvas.is_visible(), "✕ did not bring the map back"
            assert not page.locator("#cb-view-pipeline").is_visible(), "the Jobs view stayed on"
            assert "/particles" in page.url, page.url
            # Esc does the same.
            pm_card(class3d).locator(".cb-bm-head").click()
            page.wait_for_timeout(1500)
            page.locator("#cb-view-pipeline .cb-view-close").last.hover()
            page.mouse.down()
            page.mouse.up()
            page.keyboard.press("Escape")
            page.wait_for_timeout(1000)
            step("esc-back-onto-the-map")
            assert canvas.is_visible(), "Esc did not bring the map back"
            # Back: the job page the map was left for.
            page.go_back()
            page.wait_for_timeout(1500)
            step("back")
            assert f"/job/{class3d}" in page.url, page.url
            # A drag on this map moves its card and nothing on the Jobs map.
            page.goto(f"{base}/p/{project.name}/particles/{class3d}?base={project.parent}", wait_until="networkidle")
            pm_card(class3d).wait_for(state="visible", timeout=30000)
            before = pm_card(set_node).evaluate("el => el.style.top")
            b = pm_card(set_node).locator(".cb-bm-head").bounding_box()
            page.mouse.move(b["x"] + 40, b["y"] + 8)
            page.mouse.down()
            for k in range(1, 7):
                page.mouse.move(b["x"] + 40, b["y"] + 8 + 120 * k / 6)
            page.mouse.up()
            page.wait_for_timeout(500)
            assert pm_card(set_node).evaluate("el => el.style.top") != before, "the drag did not move the set card"
            step("dragged-set-card")
            page.goto(f"{base}/p/{project.name}/particles/set:nothing?base={project.parent}", wait_until="networkidle")
            page.wait_for_timeout(3000)
            step("unknown-node")
            assert page.get_by_text("has no particle job or set").count() >= 1, "no toast for an unknown node"
            # The Jobs map, under its own map name, drags and zooms as before.
            page.goto(f"{base}/p/{project.name}?base={project.parent}", wait_until="networkidle")
            page.wait_for_timeout(2000)
            if (
                page.locator('.cb-bm-canvas[data-map="jobs"]').count() == 0
                or not page.locator('.cb-bm-canvas[data-map="jobs"]').is_visible()
            ):
                page.locator("#cb-view-pipeline .cb-view-close").last.click()  # close the open job page
                page.wait_for_timeout(1500)
            jobs = page.locator('.cb-bm-canvas[data-map="jobs"]')
            jobs.wait_for(state="visible", timeout=20000)
            label = page.locator('.cb-bm-zoom-label[data-map="jobs"]')
            z0 = label.inner_text()
            jb = jobs.bounding_box()
            page.mouse.move(jb["x"] + jb["width"] / 2, jb["y"] + jb["height"] / 2)
            page.mouse.wheel(0, -400)
            page.wait_for_timeout(600)
            assert label.inner_text() != z0, "the Jobs map's zoom label did not follow the wheel"
            jobs.locator("xpath=..").locator("button", has_text="fit").first.click()
            page.wait_for_timeout(600)
            jb = jobs.bounding_box()
            card, cb = None, None
            for i in range(jobs.locator(".cb-bm-card").count()):
                c = jobs.locator(".cb-bm-card").nth(i)
                hb = c.locator(".cb-bm-head").bounding_box()
                if hb and jb["y"] + 10 < hb["y"] < jb["y"] + jb["height"] - 150 and hb["x"] > jb["x"]:
                    card, cb = c, hb
                    break
            assert card is not None, "no Jobs map card on screen"
            top0 = card.evaluate("el => el.style.top")
            page.mouse.move(cb["x"] + 30, cb["y"] + 8)
            page.mouse.down()
            for k in range(1, 7):
                page.mouse.move(cb["x"] + 30, cb["y"] + 8 + 100 * k / 6)
            page.mouse.up()
            page.wait_for_timeout(500)
            assert card.evaluate("el => el.style.top") != top0, "a Jobs map card did not drag"
            step("jobs-map-as-before")
            browser.close()
    finally:
        child.terminate()
        out = child.communicate(timeout=20)[0]
        (OUT / "server.log").write_text(out)
    logs = sorted((home / ".crboost" / "logs").glob("*.log"))
    out += "".join(p.read_text(errors="replace") for p in logs)
    bad = [ln for ln in out.splitlines() if "ERROR" in ln or "Traceback" in ln]
    print(f"server lines flagged: {len(bad)}")
    print("\n".join(bad[:40]))
    print(f"page errors: {len(errors)}")
    print("\n".join(errors[:20]))
    return 1 if bad or errors else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")
    f = sub.add_parser("fixture")
    f.add_argument("src", type=Path)
    f.add_argument("dest", type=Path)
    s = sub.add_parser("serve")
    s.add_argument("port", type=int)
    s.add_argument("home", type=Path)
    ap.add_argument("--project", type=Path, default=DEFAULT_SRC)
    a = ap.parse_args()
    if a.cmd == "fixture":
        fixture(a.src, a.dest)
        return 0
    if a.cmd == "serve":
        serve(a.port, a.home)
        return 0
    return check(a.project)


if __name__ == "__main__":
    sys.exit(main())

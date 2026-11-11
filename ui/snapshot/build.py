"""Make a project snapshot: render the wall pictures not on disk yet, collect the views' data, write the
zip in a worker. Started from the workspace (Ctrl/⌘+Alt+S) as a BackgroundTask; the browser downloads
the zip when it lands (`/api/snapshot`).

The zip goes to `<project>/Snapshots/<project>_snapshot_<stamp>.zip`, so the snapshot also stays on
the cluster beside the project it shows.
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.parse
from datetime import datetime
from pathlib import Path

from nicegui import ui

from services import renders
from services.event_log import events
from services.project_state import get_project_state_for
from services.snapshot_bundle import write_bundle
from services.workers import WorkKind, run_in_worker
from ui.background_task import BackgroundTask
from ui.particles.pick_viewer import xy_slab_render
from ui.snapshot.collect import collect, missing_slabs
from ui.tilt_previews import registry_tilt_series, tilt_context

logger = logging.getLogger(__name__)

SNAPSHOT_DIRNAME = "Snapshots"
VIEWER_DIR = Path(__file__).parent / "viewer"


def _js(value) -> str:
    """`value` as a JavaScript literal inside a classic script: JSON (NaN stays NaN, which JavaScript
    reads), with `</` broken so no string can close the script."""
    return json.dumps(value, separators=(",", ":"), default=str).replace("</", "<\\/")


def snapshot_dir(project_path: Path) -> Path:
    return Path(project_path) / SNAPSHOT_DIRNAME


async def _render_missing(state, project_path: Path, tilt_series: list, progress) -> int:
    """Render every wall picture the project has a volume but no PNG for, as the wall would; returns how
    many could not be rendered (their tiles say so in the snapshot)."""
    todo = await asyncio.to_thread(missing_slabs, state, project_path, tilt_series)
    if not todo:
        return 0
    done = 0
    failed = 0

    async def _one(ts: str, png: Path, mrc: Path) -> None:
        nonlocal done, failed
        r = xy_slab_render(png, mrc, ts, project_path, tile=True)
        if await renders.ensure(r) == renders.RUNNING:
            await renders.wait(r.key)
        if renders.error(r.key):
            failed += 1
        done += 1
        progress(done, len(todo), f"tomogram pictures {done}/{len(todo)}")

    await asyncio.gather(*(_one(ts, png, mrc) for ts, png, mrc in todo))
    return failed


async def build_snapshot(project_path: Path, progress) -> Path:
    """The snapshot zip of `project_path`, written now. `progress(done, total, message)` is the
    BackgroundTask's."""
    project_path = Path(project_path)
    state = get_project_state_for(project_path)
    tilt_series, tilt_error = registry_tilt_series(project_path)
    if tilt_error:
        raise RuntimeError(tilt_error)
    progress(0, 0, "rendering tomogram pictures")
    unrendered = await _render_missing(state, project_path, tilt_series, progress)
    progress(0, 0, "reading the project")
    collected = await asyncio.to_thread(collect, state, project_path, tilt_series, tilt_context(state))
    if unrendered:
        collected.data["overview"]["note"] = f"{unrendered} tomogram picture(s) could not be rendered."

    # Each tomogram's chart sections are their own script, read when it is opened: a project of a
    # hundred and fifty tilt series has some 30 MB of them, which the first page need not parse.
    parts = []
    for i, tomo in enumerate(collected.data["tomograms"]):
        sections = tomo.pop("sections")
        tomo["part"] = f"data/ts{i:04d}.js" if sections else None
        if sections:
            parts.append((tomo["part"], f"window.SNAPSHOT_TS[{json.dumps(tomo['ts'])}] = ", _js(sections)))

    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    root = f"{state.project_name or project_path.name}_snapshot_{stamp}"
    zip_path = snapshot_dir(project_path) / f"{root}.zip"
    result = await run_in_worker(
        WorkKind.SNAPSHOT,
        write_bundle,
        str(zip_path),
        root,
        str(VIEWER_DIR),
        _js(collected.data),
        [(path, prefix + body + ";\n") for path, prefix, body in parts],
        [(i.src, i.dst, i.max_px) for i in collected.images],
        progress_cb=progress,
    )
    if result["missing"]:
        logger.warning("Snapshot %s: %d picture(s) could not be read", zip_path, len(result["missing"]))
    events.info("%s · snapshot written: %s (%.1f MB)", project_path.name, zip_path, result["size"] / 1e6)
    return zip_path


def start_snapshot(project_path: Path | None) -> None:
    """The snapshot of the workspace's project as a background task (in the tray); the browser that
    asked downloads the zip when it lands. A second ask while one runs joins it."""
    if project_path is None:
        ui.notify("Open a project first: a snapshot is of one project.", type="warning")
        return
    project_path = Path(project_path)
    made: dict[str, Path] = {}

    async def _work(progress) -> str:
        made["zip"] = await build_snapshot(project_path, progress)
        return f"{made['zip'].name} — downloading"

    def _landed(task) -> None:
        if task.status != "succeeded" or "zip" not in made:
            ui.notify(f"Snapshot failed: {task.error or task.status}", type="negative", timeout=8000)
            return
        ui.download(f"/api/snapshot?path={urllib.parse.quote(str(made['zip']), safe='')}", made["zip"].name)

    BackgroundTask(
        title="Project snapshot",
        subtitle=project_path.name,
        dedup_key=f"snapshot:{project_path}",
        project_path=str(project_path),
    ).submit(_work, on_complete=_landed)

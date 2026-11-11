"""A project snapshot's routes: the capture page, the plan it follows, the scenes it sends back, the zip.

Ctrl/⌘+Alt+S in a workspace opens `/snapshot/capture?from=<the workspace URL>` in a window of its own
(`ui/workspace_page.py`). That page (`capture.js`) asks for the plan, shows each scene's URL in an
iframe — a workspace of its own, the real UI — waits for the view to settle, freezes it and posts it
here; then asks for the zip (`services/snapshot/bundle.py`, in a worker) and downloads it. Its own
window, so its tab state (NiceGUI's per-tab storage, keyed by sessionStorage) is not the user's tab's.

Scenes land on disk as they arrive, in `<project>/Snapshots/.capture-<session>/`; the zip goes to
`<project>/Snapshots/<project>_snapshot_<stamp>.zip`, so it also stays beside the project it shows.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
import shutil
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse
from nicegui import app

from services.configs.config_service import REPO_ROOT
from services.event_log import events
from services.http_responses import NO_GZIP
from services.project_state import get_project_state_for, loaded_projects
from services.routes import route_of_url
from services.snapshot.bundle import write_bundle
from services.workers import WorkKind, run_in_worker
from ui.snapshot.plan import plan_for

logger = logging.getLogger(__name__)

SNAPSHOT_DIRNAME = "Snapshots"
_HERE = Path(__file__).parent
_CSS_ID = re.compile(r"[0-9a-z]{1,32}")

# session id -> {project, plan, dir}. A session lives from the plan to the zip; a capture window closed
# half-way leaves its directory, which the next snapshot of the project clears.
_sessions: dict[str, dict] = {}


def _bad(reason: str, status: int = 400) -> JSONResponse:
    return JSONResponse({"error": reason}, status_code=status)


def _project_of(from_url: str) -> tuple[Path, object] | str:
    """The loaded project the workspace URL `from_url` shows, and its route; else why not."""
    route = route_of_url(from_url)
    if route is None:
        return "Not a workspace URL: a snapshot is taken from a project's workspace."
    hits = [
        p for p in loaded_projects() if p.name == route.project and (not route.base or p.parent == Path(route.base))
    ]
    if len(hits) != 1:
        return f"The project '{route.project}' is not open in this server."
    return hits[0], route


@app.get("/snapshot/capture")
def capture_page() -> FileResponse:
    return FileResponse(_HERE / "capture.html", media_type="text/html", headers={"Cache-Control": "no-cache"})


@app.get("/snapshot/capture.js")
def capture_script() -> FileResponse:
    return FileResponse(_HERE / "capture.js", media_type="text/javascript", headers={"Cache-Control": "no-cache"})


@app.get("/api/snapshot/plan")
def snapshot_plan(source: str) -> JSONResponse:
    found = _project_of(source)
    if isinstance(found, str):
        return _bad(found, 404)
    project_path, route = found
    plan = plan_for(get_project_state_for(project_path), project_path, route)
    if not plan.scenes:
        return _bad("This project has no jobs and no tilt series yet: there is nothing to snapshot.", 409)
    snap_dir = project_path / SNAPSHOT_DIRNAME
    for old in snap_dir.glob(".capture-*"):
        # A capture window closed before its zip: nothing will finish it.
        shutil.rmtree(old, ignore_errors=True)
    session = secrets.token_hex(8)
    cap = snap_dir / f".capture-{session}"
    (cap / "scenes").mkdir(parents=True)
    (cap / "css").mkdir()
    _sessions[session] = {"project": project_path, "plan": plan, "dir": cap}
    return JSONResponse({"session": session, **plan.to_dict()})


@app.post("/api/snapshot/scene")
async def snapshot_scene(session: str, request: Request) -> JSONResponse:
    s = _sessions.get(session)
    if s is None:
        return _bad("No such snapshot session (the server restarted?)", 404)
    body = await request.json()
    key, css_id = body.get("key"), str(body.get("css") or "")
    if key not in {sc.key for sc in s["plan"].scenes} or not _CSS_ID.fullmatch(css_id):
        return _bad("Not a scene of this snapshot")
    css_text = body.pop("css_text", None)
    if css_text is not None:
        (s["dir"] / "css" / f"{css_id}.css").write_text(css_text)
    elif not (s["dir"] / "css" / f"{css_id}.css").exists():
        return JSONResponse({"need_css": True})
    (s["dir"] / "scenes" / f"{hashlib.sha1(key.encode()).hexdigest()[:16]}.json").write_text(json.dumps(body))
    return JSONResponse({"ok": True})


@app.post("/api/snapshot/finish")
async def snapshot_finish(session: str) -> JSONResponse:
    s = _sessions.pop(session, None)
    if s is None:
        return _bad("No such snapshot session (the server restarted?)", 404)
    project_path: Path = s["project"]
    plan = s["plan"]
    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    root = f"{plan.project}_snapshot_{stamp}"
    zip_path = project_path / SNAPSHOT_DIRNAME / f"{root}.zip"
    try:
        result = await run_in_worker(
            WorkKind.SNAPSHOT,
            write_bundle,
            str(s["dir"]),
            str(zip_path),
            root,
            str(project_path),
            str(_HERE),
            str(REPO_ROOT / "static"),
            plan.nav(),
            progress_cb=lambda *_a: None,
        )
    except Exception as e:
        logger.exception("Snapshot of %s failed", project_path)
        return _bad(f"The snapshot could not be written: {type(e).__name__}: {e}", 500)
    events.info(
        "%s · snapshot written: %s (%d views, %.1f MB)",
        project_path.name,
        zip_path,
        result["n_scenes"],
        result["size"] / 1e6,
    )
    return JSONResponse({**result, "name": zip_path.name, "url": f"/api/snapshot?path={quote(str(zip_path), safe='')}"})


@app.get("/api/snapshot")
def snapshot_download(path: str):
    # Only a .zip directly in a loaded project's Snapshots/ directory.
    try:
        resolved = Path(path).resolve(strict=True)
    except (FileNotFoundError, RuntimeError):
        return _bad("not found", 404)
    project = resolved.parent.parent
    if resolved.suffix != ".zip" or resolved.parent.name != SNAPSHOT_DIRNAME or project not in loaded_projects():
        return _bad("not a project snapshot", 404)
    return FileResponse(resolved, media_type="application/zip", filename=resolved.name, headers=NO_GZIP)

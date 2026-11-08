"""What every verb shares: the server it runs in, the project a verb names, the link to what it touched, and the
record of every write."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from services.event_log import events
from services.project_resolve import resolve_project
from services.project_state import get_project_state_for, preload_project_state
from services.result import err
from services.routes import View, workspace_path

logger = logging.getLogger(__name__)

# The project's record of what the agent API changed in it, one JSON object per line.
RECORD_FILE = "agent_api.jsonl"


@dataclass
class ApiContext:
    """The server the verbs run in. `results` keeps the outcome of the verbs that run as background tasks,
    by task id, for `get_task`."""

    backend: Any
    port: int
    host: str = "localhost"
    results: dict[str, dict[str, Any]] = field(default_factory=dict)

    def ui_url(self, project_path: Path | str, view: View = View.PIPELINE, a: str | None = None) -> str:
        """The link that opens what a verb touched in this server's UI."""
        return f"http://{self.host}:{self.port}{workspace_path(project_path, view, a)}"


async def open_project(project: str) -> tuple[Path | None, dict[str, Any] | None]:
    """`(path, None)` for the project a verb names (an absolute path, or a name the user's project bases
    resolve), loaded the way a tab loads it; `(None, why)` when it names none, or several."""
    path = Path(project).expanduser()
    if not path.is_absolute():
        found = await asyncio.to_thread(resolve_project, project)
        if not found["success"]:
            return None, found
        path = Path(found["path"])
    if not await asyncio.to_thread((path / "project_params.json").is_file):
        return None, err(f"No CryoBoost project at {path} (no project_params.json).")
    await preload_project_state(path)
    return get_project_state_for(path).project_path or path, None


def _what(verb: str, args: dict[str, Any]) -> str:
    shown = {k: v for k, v in args.items() if k != "project" and v not in (None, [], {}, False)}
    return verb + (" " + json.dumps(shown, default=str, sort_keys=True) if shown else "")


async def record_write(project_path: Path, verb: str, args: dict[str, Any], result: dict[str, Any]) -> None:
    """One line of the project's `Logs/agent_api.jsonl` per write verb, refused or not, and one console event per
    write made."""
    if result.get("success"):
        events.info("%s · agent: %s", project_path.name, _what(verb, args))
    line = json.dumps(
        {
            "at": datetime.now().isoformat(timespec="seconds"),
            "verb": verb,
            "args": {k: v for k, v in args.items() if k != "project"},
            "success": bool(result.get("success")),
            "error": result.get("error"),
        },
        default=str,
    )

    def _append() -> None:
        logs = project_path / "Logs"
        logs.mkdir(exist_ok=True)
        with open(logs / RECORD_FILE, "a") as f:
            f.write(line + "\n")

    try:
        await asyncio.to_thread(_append)
    except OSError:
        logger.exception("Could not record the agent write %s in %s", verb, project_path)

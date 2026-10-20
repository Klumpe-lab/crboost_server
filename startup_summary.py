"""The faint stats block printed under the startup links: who and where this server runs, the cluster at a
glance, the user's SLURM jobs and their most recently active projects. A snapshot taken once at startup,
stamped with its time; the landing page carries the live versions."""

from __future__ import annotations

import asyncio
import json
import re
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from backend import CryoBoostBackend, _is_pipeline_job_dict
from services.computing.slurm_service import SlurmNode, SlurmService, UserJob
from services.configs.config_service import get_config_service
from services.configs.user_prefs_service import UserPrefsService
from ui.routing import Route, route_to_path

N_PROJECTS = 5
N_JOB_GROUPS = 6
# Newest project_params.json files read while looking for the user's own projects.
_MAX_PARAMS_READS = 40
_PROBE_TIMEOUT_SEC = 8

# SGR 2 (faint) is as small as a terminal gets: there is no escape for a smaller font.
_DIM, _RESET = "\x1b[2m", "\x1b[0m"
# The project links stand out of the faint block in the banner links' bold cyan (main.py).
_LINK = "\x1b[1;36m"
_STATUS_COLOR = {
    "running": "\x1b[2;36m",
    "queued": "\x1b[2;34m",
    "review": "\x1b[2;35m",
    "failed": "\x1b[2;31m",
    "done": "\x1b[2;32m",
}
_RELION_JOB_DIR = re.compile(r"job\d{3,}")


def summary_lines(port: int, hostname: str, user: str, token: str, log_file: Path | None) -> list[str]:
    nodes, jobs, slurm_error = _probe_slurm(user)
    rows = [
        ("user", f"{user}   (snapshot {datetime.now():%H:%M})"),
        ("log", str(log_file).replace(str(Path.home()), "~", 1) if log_file else "none: console only"),
        ("cluster", " · ".join(x for x in (_cluster_name(), hostname) if x)),
        ("resources", slurm_error or _resources(nodes)),
        ("jobs", slurm_error or _job_counts(jobs)),
    ]
    rows += [("", line) for line in ([] if slurm_error else _job_groups(jobs))]
    rows.append(("", ""))
    projects = _recent_projects(user)
    if not projects:
        rows.append(("projects", "none of yours under the project base"))
    name_w = max((len(p["name"]) for p in projects), default=0)
    for i, p in enumerate(projects):
        color = _STATUS_COLOR.get(p["status"], _DIM)
        line = (
            f"{p['name']:<{name_w}}  {color}{p['status']:<7}{_RESET}{_DIM}  "
            f"{p['succeeded']:>3}/{p['total']:<3} jobs  {p['active']}"
        )
        rows.append(("projects" if i == 0 else "", line))
        rows.append(("", f"  Go to this project: {_RESET}{_LINK}{_link(port, token, p['path'])}{_RESET}"))
    # A blank line under the pinned links, and one above the projects, so the block breathes.
    return [""] + [f"{_DIM}{label:<10} {value}{_RESET}" for label, value in rows]


def _probe_slurm(user: str) -> tuple[list[SlurmNode], list[UserJob], str]:
    svc = SlurmService(user)

    async def probe():
        return await asyncio.wait_for(
            asyncio.gather(svc.get_nodes_info(), svc.get_user_jobs()), timeout=_PROBE_TIMEOUT_SEC
        )

    try:
        nodes, jobs = asyncio.run(probe())
    except TimeoutError:
        return [], [], f"SLURM did not answer within {_PROBE_TIMEOUT_SEC} s"
    if not nodes:
        return [], [], "SLURM unreachable (sinfo failed)"
    return nodes, jobs, ""


def _cluster_name() -> str:
    try:
        out = subprocess.run(
            ["scontrol", "show", "config"], capture_output=True, text=True, timeout=_PROBE_TIMEOUT_SEC
        ).stdout
    except (OSError, subprocess.TimeoutExpired):  # no scontrol here: the hostname row still says where
        return ""
    match = re.search(r"^ClusterName\s*=\s*(\S+)", out, re.MULTILINE)
    return match.group(1) if match else ""


def _resources(nodes: list[SlurmNode]) -> str:
    # sinfo -N lists a node once per partition it belongs to.
    unique = {n.name: n for n in nodes}.values()
    idle = sum(1 for n in unique if n.state.startswith("idle"))
    mixed = sum(1 for n in unique if n.state.startswith("mix"))
    gpus = sum(n.gpus for n in unique)
    partitions = len({n.partition for n in nodes})
    return f"{len(unique)} nodes ({idle} idle, {mixed} mixed) · {gpus} GPUs · {partitions} partitions"


def _job_counts(jobs: list[UserJob]) -> str:
    running = sum(1 for j in jobs if j.state == "RUNNING")
    pending = sum(1 for j in jobs if j.state == "PENDING")
    other = len(jobs) - running - pending
    return f"{running} running · {pending} pending" + (f" · {other} other" if other else "")


def _job_groups(jobs: list[UserJob]) -> list[str]:
    """One line per job directory, so a per-tilt-series array reads as one job, longest-running first."""
    groups: dict[str, list[UserJob]] = defaultdict(list)
    for j in jobs:
        job_dir = Path(j.stdout_path).parent if j.stdout_path else None
        if job_dir is not None and _RELION_JOB_DIR.fullmatch(job_dir.name):
            groups["/".join(job_dir.parts[-3:])].append(j)
        else:
            groups[j.name].append(j)
    ordered = sorted(groups.items(), key=lambda kv: -max(_elapsed_sec(j.time) for j in kv[1]))
    lines = []
    for where, members in ordered[:N_JOB_GROUPS]:
        states = defaultdict(int)
        for j in members:
            states[j.state.lower()] += 1
        counts = ", ".join(f"{n} {s}" if len(members) > 1 else s for s, n in states.items())
        longest = max(members, key=lambda j: _elapsed_sec(j.time)).time
        lines.append(f"  {longest:>11}  {where}  ({counts})")
    if len(ordered) > N_JOB_GROUPS:
        lines.append(f"  … {len(ordered) - N_JOB_GROUPS} more")
    return lines


def _elapsed_sec(t: str) -> int:
    """squeue's %M: M:SS, H:MM:SS or D-HH:MM:SS."""
    days, _, clock = t.rpartition("-")
    try:
        sec = sum(int(p) * 60**i for i, p in enumerate(reversed(clock.split(":"))))
        return sec + int(days or 0) * 86400
    except ValueError:  # "INVALID" / "N/A" for a job that has not started: sorts as not running
        return 0


def _recent_projects(user: str) -> list[dict]:
    # No browser storage at startup: this falls back to ~/.crboost/prefs.json.
    prefs = UserPrefsService().load_from_app_storage({})
    bases = {
        Path(b).expanduser().resolve()
        for b in (prefs.project_base_path, get_config_service().default_project_base)
        if b and b.strip()
    }
    candidates: dict[Path, float] = {}
    for base in bases:
        try:
            entries = list(base.iterdir())
        except OSError:  # base missing or unmounted: the other base may still answer
            continue
        for d in entries:
            try:
                candidates[d] = (d / "project_params.json").stat().st_mtime
            except OSError:  # not a project directory
                continue

    out = []
    for d, mtime in sorted(candidates.items(), key=lambda kv: -kv[1])[:_MAX_PARAMS_READS]:
        try:
            data = json.loads((d / "project_params.json").read_text())
        except (OSError, json.JSONDecodeError):  # mid-write or unreadable: the next project fills the slot
            continue
        if user not in (data.get("owner"), data.get("created_by")):
            continue
        jobs = {iid: j for iid, j in (data.get("jobs") or {}).items() if _is_pipeline_job_dict(j)}
        derived = CryoBoostBackend._derive_live_status(d, jobs, data.get("review_hold"))
        active = max(mtime, derived["last_activity_ts"])
        out.append(
            {
                "name": d.name,
                "path": d,
                "status": derived["live_status"],
                "succeeded": derived["succeeded"],
                "total": len(jobs),
                "active": f"{datetime.fromtimestamp(active):%b %d %H:%M}",
            }
        )
        if len(out) == N_PROJECTS:
            break
    return out


def _link(port: int, token: str, project_dir: Path) -> str:
    # The token rides along: the cookie it sets lasts only as long as the browser session.
    path = route_to_path(Route(project=project_dir.name, base=str(project_dir.parent)))
    return f"http://localhost:{port}{path}{'&' if '?' in path else '?'}token={token}"

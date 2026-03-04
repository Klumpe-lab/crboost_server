# services/computing/slurm_correlation_service.py
"""
Correlates running SLURM jobs back to RELION job paths by matching
the stdout file path that RELION sets in the sbatch submission script.

This module is deliberately isolated -- it has no imports from the rest
of the orchestration layer and can be removed or replaced without touching
anything else. The only coupling point is SlurmJobInfo and ProjectState.slurm_info.

Correlation strategy:
    RELION sets --output=<job_dir>/run.out in the sbatch script.
    squeue can report this path via the %o format token.
    Stripping the project root prefix and /run.out suffix gives us
    the relion_job_name (e.g. "External/job005/") for direct lookup.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from services.project_state import ProjectState, SlurmJobInfo


class SlurmCorrelationService:
    """
    Polls squeue every `interval` seconds and updates ProjectState.slurm_info
    with SLURM job IDs, states and elapsed times for any running RELION jobs.

    Lifecycle:
        start(project_dir, state)  -- call when pipeline becomes active
        stop()                     -- call when pipeline finishes/is killed
    """

    SQUEUE_FMT = "%i|%j|%T|%M|%N|%o"
    # Fields: job_id | name | state | elapsed | nodelist | stdout_path

    def __init__(self, username: str, interval: float = 5.0):
        self.username = username
        self.interval = interval
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, project_dir: Path, state: "ProjectState"):
        if self._task and not self._task.done():
            print("[SLURM_CORR] Already polling, ignoring start()")
            return
        print(f"[SLURM_CORR] Starting correlation loop (interval={self.interval}s)")
        self._task = asyncio.create_task(
            self._poll_loop(project_dir, state),
            name="slurm_correlation",
        )

    def stop(self):
        if self._task and not self._task.done():
            print("[SLURM_CORR] Stopping correlation loop")
            self._task.cancel()
        self._task = None

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _poll_loop(self, project_dir: Path, state: "ProjectState"):
        while True:
            try:
                await self._poll_once(project_dir, state)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[SLURM_CORR] Poll error (non-fatal): {e}")
            await asyncio.sleep(self.interval)

    async def _poll_once(self, project_dir: Path, state: "ProjectState"):
        rows = await self._run_squeue()
        if rows is None:
            return

        print(f"[SLURM_CORR] squeue returned {len(rows)} row(s)")
        for row in rows:
            print(f"[SLURM_CORR]   job_id={row['job_id']} state={row['state']} stdout={row['stdout_path']}")

        project_root = str(project_dir.resolve())
        print(f"[SLURM_CORR] project_root={project_root}")

        new_info: Dict[str, "SlurmJobInfo"] = {}

        for row in rows:
            relion_path = self._extract_relion_path(row["stdout_path"], project_root)
            print(f"[SLURM_CORR]   stdout={row['stdout_path']} -> relion_path={relion_path}")
            if relion_path is None:
                continue

            from services.project_state import SlurmJobInfo
            new_info[relion_path] = SlurmJobInfo(
                slurm_job_id=row["job_id"],
                slurm_state=row["state"],
                elapsed=row["elapsed"],
                node=row["node"],
            )

        state.slurm_info = new_info

        if new_info:
            print(f"[SLURM_CORR] Updated {len(new_info)} job(s): "
                + ", ".join(f"{p}={v.slurm_job_id}({v.slurm_state})"
                            for p, v in new_info.items()))

    async def _run_squeue(self) -> Optional[List[Dict[str, str]]]:
        try:
            proc = await asyncio.create_subprocess_exec(
                "squeue",
                "-u", self.username,
                "-o", self.SQUEUE_FMT,
                "--noheader",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10.0)
        except asyncio.TimeoutError:
            print("[SLURM_CORR] squeue timed out")
            return None
        except Exception as e:
            print(f"[SLURM_CORR] squeue failed: {e}")
            return None

        if proc.returncode != 0:
            print(f"[SLURM_CORR] squeue error: {stderr.decode().strip()}")
            return None

        rows = []
        for line in stdout.decode().splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 6:
                continue
            rows.append({
                "job_id":      parts[0],
                "name":        parts[1],
                "state":       parts[2],
                "elapsed":     parts[3],
                "node":        parts[4],
                "stdout_path": parts[5],
            })
        return rows

    def _extract_relion_path(self, stdout_path: str, project_root: str) -> Optional[str]:
        """
        Convert an absolute stdout path back to a relion_job_name.

        e.g. "/data/proj/myproject/External/job005/run.out"
             -> "External/job005/"
        """
        if not stdout_path or stdout_path == "N/A":
            return None

        # Normalize both to avoid trailing-slash mismatches
        try:
            rel = Path(stdout_path).relative_to(project_root)
        except ValueError:
            return None

        # RELION stdout is always <category>/jobNNN/run.out
        # We want the parent directory with trailing slash
        parent = rel.parent
        if not parent or parent == Path("."):
            return None

        return str(parent) + "/"
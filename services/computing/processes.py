"""Child processes of the server (squeue, sacct, sbatch, ssh, container commands), started off the event loop.

uvicorn runs the server on uvloop, whose `asyncio.create_subprocess_*` forks the whole server while holding the GIL
and then waits for the child's exec. Copying a large process's page tables stops every thread for as long: 80 ms a
spawn at a 2.3 GB heap, and seconds once the user's memory cgroup is reclaiming (the watchdog's 1-4 s stall lines in
`slurm_service._run_command`, one per squeue). `subprocess.run` in a thread spawns with vfork, which copies nothing,
and the loop waits on nothing.

Every child process the server starts goes through `run_process`.
"""

from __future__ import annotations

import asyncio
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path


async def run_process(
    cmd: Sequence[str] | str,
    *,
    shell: bool = False,
    cwd: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    input: bytes | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[bytes]:
    """Run `cmd` to its end in a worker thread, its output captured as bytes. `shell=True` takes a command line.
    On `timeout` the child is killed and `subprocess.TimeoutExpired` raised; `OSError` when it cannot be started."""
    return await asyncio.to_thread(
        subprocess.run,
        cmd,
        shell=shell,
        cwd=cwd,
        env=env,
        input=input,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )

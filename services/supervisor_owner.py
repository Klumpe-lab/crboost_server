"""Which supervisor owns a job dir: `<job_dir>/.supervisor.owner`.

A supervisor (the process qsub.sh starts in a job dir: an array job's dispatcher, or a job that runs
its tool itself) creates the file exclusively before it touches the dir, and removes it when it exits
(drivers/driver_base.start_supervisor). A second supervisor started in the same dir finds the file and
leaves the dir alone while SLURM still runs the first; one whose owner has ended (killed at its time
limit, its node lost) takes the dir over. O_EXCL rather than flock: job dirs live on the group's shared
mount, and an explicit SLURM check survives a stale file and names its owner. Run reads the file too
before it resubmits into a dir (pipeline_orchestrator_service._live_in_reused_dirs).
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from services.code_version import code_version
from services.computing.slurm_service import SLURM_ENDED_STATES
from services.result import err, ok

OWNER_FILENAME = ".supervisor.owner"

# Tries at taking over an owner that has ended before giving up: each one lost means another
# supervisor changed the file meanwhile.
_CLAIM_ATTEMPTS = 5


@dataclass(frozen=True)
class SupervisorOwner:
    slurm_job_id: str
    host: str
    started: str  # ISO time the supervisor started
    code_version: str | None  # the checkout it runs, services.code_version

    def describe(self) -> str:
        return f"slurm {self.slurm_job_id} on {self.host}, started {self.started}"


def read_owner(job_dir: Path) -> SupervisorOwner | None:
    """The supervisor the job dir's owner file names; None when it has none. Raises ValueError,
    naming the file, for one that does not parse (damaged: acquire never leaves one half written)."""
    return _read_owner_file(job_dir / OWNER_FILENAME)


def _read_owner_file(path: Path) -> SupervisorOwner | None:
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None
    try:
        data = json.loads(text)
        return SupervisorOwner(
            slurm_job_id=str(data["slurm_job_id"]),
            host=str(data["host"]),
            started=str(data["started"]),
            code_version=data.get("code_version"),
        )
    except (ValueError, KeyError, TypeError, AttributeError) as e:
        raise ValueError(f"{path} does not parse ({e!r})") from None


def acquire(job_dir: Path, slurm_job_id: str) -> dict[str, Any]:
    """Claim `job_dir` for the supervisor running as SLURM job `slurm_job_id`. A file naming a
    supervisor SLURM no longer runs is taken over, and so is one naming this same job (SLURM requeued
    it under its id). ok(owner=, took_over_from=); err naming the owner while it still runs, when
    squeue cannot tell, or when the file does not parse."""
    me = SupervisorOwner(
        slurm_job_id=slurm_job_id,
        host=socket.gethostname(),
        started=datetime.now().astimezone().isoformat(timespec="seconds"),
        code_version=code_version(),
    )
    path = job_dir / OWNER_FILENAME
    # Written whole under a name of this job's own, then linked into place: link() fails on an existing
    # name as O_EXCL does, and the owner file never exists empty or half written (a full quota, a kill).
    # A link whose reply NFS lost fails as if the name existed, and reads back as this very claim.
    tmp = job_dir / f"{OWNER_FILENAME}.{slurm_job_id}.tmp"
    took_over: SupervisorOwner | None = None
    try:
        tmp.write_text(json.dumps(asdict(me)))
        for _ in range(_CLAIM_ATTEMPTS):
            try:
                os.link(tmp, path)
            except FileExistsError:
                try:
                    holder = read_owner(job_dir)
                except ValueError as e:
                    return err(f"{e}; delete it if no supervisor runs in this job dir")
                if holder is None:
                    continue  # released meanwhile
                if holder == me:
                    return ok(owner=me, took_over_from=took_over)
                if holder.slurm_job_id != slurm_job_id:
                    live = _slurm_job_live(holder.slurm_job_id)
                    if live is None:
                        return err(
                            f"could not ask SLURM whether {holder.describe()}, which owns this job dir, still runs"
                        )
                    if live:
                        return err(f"another supervisor ({holder.describe()}) owns this job dir")
                if _take_over(job_dir, holder, slurm_job_id):
                    took_over = holder
                continue
            return ok(owner=me, took_over_from=took_over)
        return err(f"could not claim this job dir: {path} kept changing hands")
    finally:
        tmp.unlink(missing_ok=True)


def release(job_dir: Path, owner: SupervisorOwner) -> None:
    """Give the job dir back: remove its owner file if it still names `owner`."""
    try:
        _unlink_if_owned_by(job_dir, owner)
    except OSError:
        pass  # a file left behind is taken over by the next supervisor, once SLURM says this one ended


def _take_over(job_dir: Path, holder: SupervisorOwner, slurm_job_id: str) -> bool:
    """Remove the owner file naming `holder`, which has ended; True when this call removed it. The
    file is moved aside first, so of two supervisors taking it over at once only one gets it. One that
    moved the file the other has linked in since puts it back (if a third has claimed the dir in that
    instant, it cannot, and two supervisors run)."""
    path = job_dir / OWNER_FILENAME
    aside = job_dir / f"{OWNER_FILENAME}.{slurm_job_id}.ended"
    try:
        os.rename(path, aside)
    except FileNotFoundError:
        return False  # another supervisor moved it first
    try:
        moved = _read_owner_file(aside)
    except ValueError:
        moved = None  # not the file read a moment ago, which parsed
    if moved != holder:
        try:
            os.link(aside, path)
        except FileExistsError:
            pass  # a third supervisor claimed the dir meanwhile (see the docstring)
    aside.unlink()
    return moved == holder


def _unlink_if_owned_by(job_dir: Path, owner: SupervisorOwner) -> None:
    """Remove the owner file if it names `owner`; one another supervisor has written since stays."""
    try:
        current = read_owner(job_dir)
    except ValueError:
        return  # not one acquire wrote, so not this owner's
    if current == owner:
        (job_dir / OWNER_FILENAME).unlink(missing_ok=True)


def _slurm_job_live(slurm_job_id: str) -> bool | None:
    """Whether SLURM still has this job queued or running; None when squeue failed."""
    try:
        proc = subprocess.run(["squeue", "-h", "-j", slurm_job_id, "-o", "%T"], capture_output=True, text=True)
    except OSError:  # no squeue on this host
        return None
    if proc.returncode != 0:
        return False if "invalid job id specified" in proc.stderr.lower() else None
    states = [line.split()[0] for line in proc.stdout.splitlines() if line.strip()]
    return any(state not in SLURM_ENDED_STATES for state in states)

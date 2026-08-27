"""INFRA vs FAIL (roadmap 14 §verdicts): a failed stage whose logs carry a known cluster
signature is reported as infrastructure, with the node named — and is NEVER requeued
(2026-08-14 decision: node rot = static exclude + admin report, not retry machinery)."""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# (regex, label). First match wins; scanned over run.err/run.out/task_*.err/task_*.out tails.
SIGNATURES: tuple[tuple[str, str], ...] = (
    (r"transport endpoint is not connected", "mount rot (apptainer bind on the node)"),
    (r"QOSMaxWallDurationPerJobLimit", "QOS wall-time cap (job needs --qos)"),
    (r"DependencyNeverSatisfied", "upstream job failed (afterok)"),
    (r"CANCELLED AT .* DUE TO TIME LIMIT", "SLURM time limit"),
    (r"DUE TO NODE FAILURE|NODE_FAIL", "node failure"),
    (r"DUE TO PREEMPTION", "preempted"),
    (
        r"Failed to initialize NVML|cudaErrorNoDevice|no CUDA-capable device|CUDA_ERROR_NO_DEVICE|"
        r"CUDA driver version is insufficient|libcuda\.so.*cannot open shared object",
        "GPU / driver initialisation",
    ),
    (r"Stale file handle|Input/output error|No space left on device", "filesystem"),
)
_TAIL_BYTES = 256 * 1024


def classify_failure(job_dir: Path) -> dict | None:
    """`{"label", "pattern", "file", "line"}` for the first infrastructure signature found in
    the job's logs, else None (= a genuine failure of ours)."""
    for log in _log_files(job_dir):
        try:
            with open(log, "rb") as f:
                f.seek(max(0, log.stat().st_size - _TAIL_BYTES))
                text = f.read().decode("utf-8", errors="replace")
        except OSError:
            continue
        for pattern, label in SIGNATURES:
            m = re.search(pattern, text)
            if m:
                line = text[text.rfind("\n", 0, m.start()) + 1 : text.find("\n", m.end())].strip()
                return {"label": label, "pattern": pattern, "file": str(log), "line": line[:300]}
    return None


def _log_files(job_dir: Path) -> list[Path]:
    out = [job_dir / "run.err", job_dir / "run.out"]
    out += sorted(job_dir.glob("task_*.err")) + sorted(job_dir.glob("task_*.out"))
    return [p for p in out if p.is_file()]


async def node_for(slurm_job_id: str | None) -> str | None:
    """NodeList of a finished job via sacct, or None when sacct is unavailable / has no row."""
    if not slurm_job_id:
        return None
    jid = str(slurm_job_id).split("_", 1)[0]
    try:
        proc = await asyncio.create_subprocess_exec(
            "sacct",
            "-j",
            jid,
            "-o",
            "JobID,NodeList",
            "-n",
            "-P",
            "-X",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
    except (OSError, TimeoutError) as e:
        logger.info("sacct unavailable for node lookup of %s: %s", jid, e)
        return None
    for line in out.decode(errors="replace").splitlines():
        parts = line.split("|")
        if len(parts) >= 2 and parts[1].strip() not in ("", "None assigned"):
            return parts[1].strip()
    return None

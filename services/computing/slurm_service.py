# services/slurm_service.py
import asyncio
import logging
from enum import Enum
from pathlib import Path
import re
from typing import ClassVar, Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field
from services.configs.config_service import get_config_service

logger = logging.getLogger(__name__)


class SlurmPreset(str, Enum):
    CUSTOM = "Custom"
    SMALL = "1gpu:16GB"
    MEDIUM = "2gpu:32GB"
    LARGE = "4gpu:64gb"


# Descriptive mapping for UI pills and snapping values
SLURM_PRESET_MAP = {
    SlurmPreset.SMALL: {
        "label": "1 GPU · 16GB · 30m",
        "values": {"gres": "gpu:1", "mem": "16G", "cpus_per_task": 2, "time": "0:30:00", "nodes": 1},
    },
    SlurmPreset.MEDIUM: {
        "label": "2 GPUs · 32GB · 2h",
        "values": {"gres": "gpu:2", "mem": "32G", "cpus_per_task": 4, "time": "2:00:00", "nodes": 2},
    },
    SlurmPreset.LARGE: {
        "label": "4 GPUs · 64GB · 4h",
        "values": {"gres": "gpu:4", "mem": "64G", "cpus_per_task": 8, "time": "4:00:00", "nodes": 4},
    },
}


class SlurmConfig(BaseModel):
    """SLURM submission parameters for a job"""

    model_config = ConfigDict(validate_assignment=True)

    preset: SlurmPreset = Field(default=SlurmPreset.CUSTOM)
    partition: str = "g"
    constraint: str = "g2|g3|g4"
    nodes: int = Field(default=1, ge=1)
    ntasks_per_node: int = Field(default=1, ge=1)
    cpus_per_task: int = Field(default=4, ge=1)
    gres: str = "gpu:4"
    mem: str = "64G"
    time: str = "3:30:00"

    # Standard Relion Tomography aliases for XXXextra1XXX through XXXextra8XXX
    QSUB_EXTRA_MAPPING: ClassVar[Dict[str, str]] = {
        "partition": "qsub_extra1",
        "constraint": "qsub_extra2",
        "nodes": "qsub_extra3",
        "ntasks_per_node": "qsub_extra4",
        "cpus_per_task": "qsub_extra5",
        "gres": "qsub_extra6",
        "mem": "qsub_extra7",
        "time": "qsub_extra8",
    }

    def to_qsub_extra_dict(self) -> Dict[str, str]:
        return {self.QSUB_EXTRA_MAPPING[field]: str(getattr(self, field)) for field in self.QSUB_EXTRA_MAPPING}

    @classmethod
    def from_config_defaults(cls) -> "SlurmConfig":
        try:
            config_service = get_config_service()
            defaults = config_service.slurm_defaults
            return cls(**defaults.model_dump())
        except Exception as e:
            logger.info("Could not load config defaults, using built-in: %s", e)
            return cls()


@dataclass
class SlurmPartition:
    """Information about a SLURM partition"""

    name: str
    state: str
    nodes: int
    max_time: str
    max_nodes_per_job: int
    default_mem_per_cpu: str
    available_cpus: int
    available_gpus: int
    gpu_type: Optional[str] = None


@dataclass
class SlurmNode:
    """Information about a SLURM node"""

    name: str
    partition: str
    state: str
    cpus: int
    memory_mb: int
    gpus: int
    gpu_type: Optional[str] = None
    features: List[str] = None


@dataclass
class UserJob:
    job_id: str
    name: str
    partition: str
    state: str
    time: str
    nodes: int
    nodelist: str
    work_dir: str = ""
    stdout_path: str = ""  # %o -- path to stdout file, contains job dir


def normalize_slurm_ids(job_ids: List[str]) -> List[str]:
    """
    Deduplicate SLURM job IDs by normalizing array task IDs to their parent.

    Array tasks look like '28666490_1', '28666490_[3-14%8]', etc.
    Normalizing to '28666490' ensures a single scancel kills the entire array.
    Non-array IDs (e.g., '28666489') pass through unchanged.

    Example:
        ['28666489', '28666490_1', '28666490_2', '28666490_[3-14%8]']
        → ['28666489', '28666490']
    """
    result: set = set()
    for jid in job_ids:
        if "_" in jid:
            result.add(jid.split("_", 1)[0])
        else:
            result.add(jid)
    return sorted(result)


class SlurmService:
    def __init__(self, username: str):
        self.username = username
        self._cache = {}
        self._cache_timestamp = {}
        self._cache_ttl = 60

    async def _run_command(self, cmd: List[str]) -> tuple[bool, str, str]:
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            success = process.returncode == 0
            return success, stdout.decode(), stderr.decode()
        except Exception as e:
            return False, "", str(e)

    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._cache or key not in self._cache_timestamp:
            return False
        age = (datetime.now() - self._cache_timestamp[key]).total_seconds()
        return age < self._cache_ttl

    async def get_partitions_info(self, force_refresh: bool = False) -> List[SlurmPartition]:
        cache_key = "partitions"
        if not force_refresh and self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        success, stdout, stderr = await self._run_command(["sinfo", "-o", "%P|%a|%D|%l|%m|%c|%G", "--noheader"])

        if not success:
            logger.error("Failed to get partition info: %s", stderr)
            return []

        partitions_dict = {}
        for line in stdout.strip().split("\n"):
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 7:
                continue
            name = parts[0].rstrip("*")
            if name in partitions_dict:
                continue
            state = parts[1]
            nodes = int(parts[2]) if parts[2].isdigit() else 0
            max_time = parts[3]
            mem = parts[4]
            cpus = int(parts[5]) if parts[5].isdigit() else 0
            gres = parts[6]

            gpu_count = 0
            gpu_type = None
            if gres and gres != "(null)":
                gpu_match = re.search(r"gpu:(\w+):(\d+)", gres)
                if gpu_match:
                    gpu_type = gpu_match.group(1)
                    gpu_count = int(gpu_match.group(2))
                else:
                    gpu_match = re.search(r"gpu:(\d+)", gres)
                    if gpu_match:
                        gpu_count = int(gpu_match.group(1))

            partition = SlurmPartition(
                name=name,
                state=state,
                nodes=nodes,
                max_time=max_time,
                max_nodes_per_job=nodes,
                default_mem_per_cpu=mem,
                available_cpus=cpus,
                available_gpus=gpu_count,
                gpu_type=gpu_type,
            )
            partitions_dict[name] = partition

        partitions = list(partitions_dict.values())
        self._cache[cache_key] = partitions
        self._cache_timestamp[cache_key] = datetime.now()
        return partitions

    async def get_nodes_info(self, partition: Optional[str] = None, force_refresh: bool = False) -> List[SlurmNode]:
        cache_key = f"nodes_{partition or 'all'}"
        if not force_refresh and self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        cmd = ["sinfo", "-N", "-o", "%N|%P|%T|%c|%m|%G|%f", "--noheader"]
        if partition:
            cmd.extend(["-p", partition])

        success, stdout, stderr = await self._run_command(cmd)
        if not success:
            logger.error("Failed to get node info: %s", stderr)
            return []

        nodes = []
        for line in stdout.strip().split("\n"):
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 7:
                continue
            name = parts[0]
            part = parts[1].rstrip("*")
            state = parts[2]
            cpus = int(parts[3]) if parts[3].isdigit() else 0
            mem = int(parts[4]) if parts[4].isdigit() else 0
            gres = parts[5]
            features = parts[6].split(",") if parts[6] != "(null)" else []

            gpu_count = 0
            gpu_type = None
            if gres and gres != "(null)":
                gpu_match = re.search(r"gpu:(\w+):(\d+)", gres)
                if gpu_match:
                    gpu_type = gpu_match.group(1)
                    gpu_count = int(gpu_match.group(2))
                else:
                    gpu_match = re.search(r"gpu:(\d+)", gres)
                    if gpu_match:
                        gpu_count = int(gpu_match.group(1))

            node = SlurmNode(
                name=name,
                partition=part,
                state=state,
                cpus=cpus,
                memory_mb=mem,
                gpus=gpu_count,
                gpu_type=gpu_type,
                features=features,
            )
            nodes.append(node)

        self._cache[cache_key] = nodes
        self._cache_timestamp[cache_key] = datetime.now()
        return nodes

    async def get_user_jobs(self, force_refresh: bool = False) -> List[UserJob]:
        cache_key = "user_jobs"
        if not force_refresh and self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        logger.debug("Fetching jobs for user: %s", self.username)

        # %o = stdout file path -- parent dir is the job directory (e.g. External/job002)
        # %Z = submit working directory -- this is the project root, same for all jobs
        success, stdout, stderr = await self._run_command(
            ["squeue", "-u", self.username, "-o", "%i|%j|%P|%T|%M|%D|%N|%Z|%o", "--noheader"]
        )

        logger.debug("squeue success=%s", success)

        if not success:
            logger.error("Failed to get user jobs: %s", stderr)
            return []

        jobs = []
        for line in stdout.strip().split("\n"):
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 8:
                continue

            work_dir = parts[7].strip() if len(parts) > 7 else ""
            stdout_path = parts[8].strip() if len(parts) > 8 else ""

            # stdout_path may be relative to work_dir -- make it absolute
            if stdout_path and not stdout_path.startswith("/") and work_dir:
                stdout_path = str(Path(work_dir) / stdout_path)

            job = UserJob(
                job_id=parts[0],
                name=parts[1],
                partition=parts[2],
                state=parts[3],
                time=parts[4],
                nodes=int(parts[5]) if parts[5].isdigit() else 0,
                nodelist=parts[6],
                work_dir=work_dir,
                stdout_path=stdout_path,
            )
            jobs.append(job)

        logger.debug("Total jobs found: %d", len(jobs))
        self._cache[cache_key] = jobs
        self._cache_timestamp[cache_key] = datetime.now()
        return jobs

    async def find_slurm_job_for_directory(self, job_dir: Path) -> Optional[UserJob]:
        """
        Find the SLURM job whose stdout file lives inside the given job directory.
        RELION sets --output=<job_dir>/run.out, so parent of stdout_path == job_dir.
        Falls back to work_dir match for safety.
        """
        jobs = await self.get_user_jobs(force_refresh=True)
        target = job_dir.resolve()

        for job in jobs:
            # Primary: match via stdout file parent directory
            if job.stdout_path:
                try:
                    if Path(job.stdout_path).resolve().parent == target:
                        logger.info("Matched job %s via stdout path: %s", job.job_id, job.stdout_path)
                        return job
                except Exception as e:
                    logger.info("Error resolving stdout path for job %s: %s", job.job_id, e)

            # Fallback: work_dir match (only works if the job cd'd to job_dir)
            if job.work_dir:
                try:
                    if Path(job.work_dir).resolve() == target:
                        logger.info("Matched job %s via work_dir: %s", job.job_id, job.work_dir)
                        return job
                except Exception as e:
                    logger.info("Error resolving work_dir for job %s: %s", job.job_id, e)

        logger.info("No SLURM job found for %s", target)
        return None

    async def find_all_slurm_jobs_for_directory(self, job_dir: Path) -> List[UserJob]:
        """
        Find ALL SLURM jobs whose stdout file or work_dir matches the given directory.

        For array jobs, squeue returns each task as a separate row (28666490_1,
        28666490_2, ...) plus the supervisor job (28666489) — all with stdout paths
        inside the same directory.  Returns every match so the caller can collect
        all related IDs for a comprehensive scancel.
        """
        jobs = await self.get_user_jobs(force_refresh=True)
        target = job_dir.resolve()
        matches: List[UserJob] = []

        for job in jobs:
            matched = False
            if job.stdout_path:
                try:
                    if Path(job.stdout_path).resolve().parent == target:
                        matched = True
                except Exception:
                    pass
            if not matched and job.work_dir:
                try:
                    if Path(job.work_dir).resolve() == target:
                        matched = True
                except Exception:
                    pass
            if matched:
                matches.append(job)

        logger.info("Found %d SLURM job(s) for %s: %s", len(matches), target, [j.job_id for j in matches])
        return matches

    async def scancel_jobs(self, job_ids: List[str]) -> Dict[str, Any]:
        if not job_ids:
            return {"success": True, "cancelled": []}
        success, stdout, stderr = await self._run_command(["scancel"] + job_ids)
        if success:
            logger.info("Cancelled jobs: %s", job_ids)
            return {"success": True, "cancelled": job_ids}
        logger.info("scancel returned non-zero (may be already gone): %s", stderr.strip())
        return {"success": False, "error": stderr.strip(), "cancelled": job_ids}

    async def get_cluster_summary(self) -> Dict[str, Any]:
        partitions = await self.get_partitions_info()
        user_jobs = await self.get_user_jobs()
        total_nodes = sum(p.nodes for p in partitions)
        total_cpus = sum(p.available_cpus * p.nodes for p in partitions)
        total_gpus = sum(p.available_gpus * p.nodes for p in partitions)
        gpu_partitions = [p for p in partitions if p.available_gpus > 0]
        return {
            "total_partitions": len(partitions),
            "total_nodes": total_nodes,
            "total_cpus": total_cpus,
            "total_gpus": total_gpus,
            "gpu_partitions": len(gpu_partitions),
            "user_jobs": len(user_jobs),
            "running_jobs": len([j for j in user_jobs if j.state == "RUNNING"]),
            "pending_jobs": len([j for j in user_jobs if j.state == "SCHEDULED"]),
        }

    async def get_slurm_partitions(self) -> Dict[str, Any]:
        try:
            partitions = await self.get_partitions_info()
            return {
                "success": True,
                "partitions": [
                    {
                        "name": p.name,
                        "state": p.state,
                        "nodes": p.nodes,
                        "max_time": p.max_time,
                        "cpus": p.available_cpus,
                        "gpus": p.available_gpus,
                        "gpu_type": p.gpu_type,
                        "memory": p.default_mem_per_cpu,
                    }
                    for p in partitions
                ],
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def get_slurm_nodes(self, partition: str = None) -> Dict[str, Any]:
        try:
            nodes = await self.get_nodes_info(partition)
            return {
                "success": True,
                "nodes": [
                    {
                        "name": n.name,
                        "partition": n.partition,
                        "state": n.state,
                        "cpus": n.cpus,
                        "memory_mb": n.memory_mb,
                        "gpus": n.gpus,
                        "gpu_type": n.gpu_type,
                        "features": n.features or [],
                    }
                    for n in nodes
                ],
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def clear_cache(self):
        self._cache.clear()
        self._cache_timestamp.clear()

    async def get_user_slurm_jobs(self, force_refresh: bool = False) -> Dict[str, Any]:
        try:
            jobs = await self.get_user_jobs(force_refresh=force_refresh)
            logger.debug("Backend returning %d jobs", len(jobs))
            return {
                "success": True,
                "jobs": [
                    {
                        "job_id": j.job_id,
                        "name": j.name,
                        "partition": j.partition,
                        "state": j.state,
                        "time": j.time,
                        "nodes": j.nodes,
                        "nodelist": j.nodelist,
                        "work_dir": j.work_dir,
                        "stdout_path": j.stdout_path,
                    }
                    for j in jobs
                ],
            }
        except Exception as e:
            logger.error("Failed to get user jobs: %s", e)
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}

    async def get_slurm_summary(self, force_refresh: bool = False) -> Dict[str, Any]:
        try:
            summary = await self.get_cluster_summary()
            return {"success": True, "summary": summary}
        except Exception as e:
            return {"success": False, "error": str(e)}

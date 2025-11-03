# services/slurm_service.py
import asyncio
import re
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime


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
    """Information about a user's SLURM job"""
    job_id: str
    name: str
    partition: str
    state: str
    time: str
    nodes: int
    nodelist: str


class SlurmService:
    """Service for querying SLURM cluster information"""
    
    def __init__(self, username: str):
        self.username = username
        self._cache = {}
        self._cache_timestamp = {}
        self._cache_ttl = 60  # Cache for 60 seconds
    
    async def _run_command(self, cmd: List[str]) -> tuple[bool, str, str]:
        """Run a shell command and return success, stdout, stderr"""
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            success = process.returncode == 0
            return success, stdout.decode(), stderr.decode()
        except Exception as e:
            return False, "", str(e)
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached data is still valid"""
        if key not in self._cache or key not in self._cache_timestamp:
            return False
        age = (datetime.now() - self._cache_timestamp[key]).total_seconds()
        return age < self._cache_ttl
    
# In services/slurm_service.py, update the get_partitions_info method:

    async def get_partitions_info(self, force_refresh: bool = False) -> List[SlurmPartition]:
        """Get information about available partitions"""
        cache_key = "partitions"
        if not force_refresh and self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        # Query partition info
        success, stdout, stderr = await self._run_command([
            "sinfo", "-o", "%P|%a|%D|%l|%m|%c|%G", "--noheader"
        ])
        
        if not success:
            print(f"[ERROR] Failed to get partition info: {stderr}")
            return []
        
        # Use dict to deduplicate by partition name, keeping first occurrence
        partitions_dict = {}
        
        for line in stdout.strip().split('\n'):
            if not line:
                continue
            
            parts = line.split('|')
            if len(parts) < 7:
                continue
            
            name = parts[0].rstrip('*')  # Remove default marker
            
            # Skip if we already have this partition
            if name in partitions_dict:
                continue
            
            state = parts[1]
            nodes = int(parts[2]) if parts[2].isdigit() else 0
            max_time = parts[3]
            mem = parts[4]
            cpus = int(parts[5]) if parts[5].isdigit() else 0
            gres = parts[6]
            
            # Parse GPU info from GRES
            gpu_count = 0
            gpu_type = None
            if gres and gres != "(null)":
                gpu_match = re.search(r'gpu:(\w+):(\d+)', gres)
                if gpu_match:
                    gpu_type = gpu_match.group(1)
                    gpu_count = int(gpu_match.group(2))
                else:
                    gpu_match = re.search(r'gpu:(\d+)', gres)
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
                gpu_type=gpu_type
            )
            partitions_dict[name] = partition
        
        partitions = list(partitions_dict.values())
        
        self._cache[cache_key] = partitions
        self._cache_timestamp[cache_key] = datetime.now()
        return partitions
    
    async def get_nodes_info(self, partition: Optional[str] = None, force_refresh: bool = False) -> List[SlurmNode]:
        """Get information about nodes in a partition"""
        cache_key = f"nodes_{partition or 'all'}"
        if not force_refresh and self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        cmd = ["sinfo", "-N", "-o", "%N|%P|%T|%c|%m|%G|%f", "--noheader"]
        if partition:
            cmd.extend(["-p", partition])
        
        success, stdout, stderr = await self._run_command(cmd)
        
        if not success:
            print(f"[ERROR] Failed to get node info: {stderr}")
            return []
        
        nodes = []
        for line in stdout.strip().split('\n'):
            if not line:
                continue
            
            parts = line.split('|')
            if len(parts) < 7:
                continue
            
            name = parts[0]
            part = parts[1].rstrip('*')
            state = parts[2]
            cpus = int(parts[3]) if parts[3].isdigit() else 0
            mem = int(parts[4]) if parts[4].isdigit() else 0
            gres = parts[5]
            features = parts[6].split(',') if parts[6] != "(null)" else []
            
            # Parse GPU info
            gpu_count = 0
            gpu_type = None
            if gres and gres != "(null)":
                gpu_match = re.search(r'gpu:(\w+):(\d+)', gres)
                if gpu_match:
                    gpu_type = gpu_match.group(1)
                    gpu_count = int(gpu_match.group(2))
                else:
                    gpu_match = re.search(r'gpu:(\d+)', gres)
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
                features=features
            )
            nodes.append(node)
        
        self._cache[cache_key] = nodes
        self._cache_timestamp[cache_key] = datetime.now()
        return nodes
    
    async def get_user_jobs(self, force_refresh: bool = False) -> List[UserJob]:
        """Get current user's jobs"""
        cache_key = "user_jobs"
        if not force_refresh and self._is_cache_valid(cache_key):
            print(f"[DEBUG] Returning cached user jobs for {self.username}")
            return self._cache[cache_key]
        
        print(f"[DEBUG] Fetching jobs for user: {self.username}")
        
        success, stdout, stderr = await self._run_command([
            "squeue", "-u", self.username, "-o", "%i|%j|%P|%T|%M|%D|%N", "--noheader"
        ])
        
        print(f"[DEBUG] squeue success={success}")
        print(f"[DEBUG] squeue stdout: {repr(stdout)}")
        print(f"[DEBUG] squeue stderr: {repr(stderr)}")
        
        if not success:
            print(f"[ERROR] Failed to get user jobs: {stderr}")
            return []
        
        jobs = []
        for line in stdout.strip().split('\n'):
            if not line:
                continue
            
            print(f"[DEBUG] Parsing job line: {repr(line)}")
            parts = line.split('|')
            if len(parts) < 7:
                print(f"[WARN] Skipping malformed line with {len(parts)} parts")
                continue
            
            job = UserJob(
                job_id=parts[0],
                name=parts[1],
                partition=parts[2],
                state=parts[3],
                time=parts[4],
                nodes=int(parts[5]) if parts[5].isdigit() else 0,
                nodelist=parts[6]
            )
            jobs.append(job)
            print(f"[DEBUG] Parsed job: {job.job_id} - {job.name} - {job.state}")
        
        print(f"[DEBUG] Total jobs found: {len(jobs)}")
        
        self._cache[cache_key] = jobs
        self._cache_timestamp[cache_key] = datetime.now()
        return jobs
    
    async def get_cluster_summary(self) -> Dict[str, Any]:
        """Get a summary of cluster status"""
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
            "pending_jobs": len([j for j in user_jobs if j.state == "PENDING"]),
        }
    
    def clear_cache(self):
        """Clear all cached data"""
        self._cache.clear()
        self._cache_timestamp.clear()
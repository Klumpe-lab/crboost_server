# services/computing_service.py

from typing import Dict, Any, Optional
from .config_service import get_config_service

class ComputingService:
    def __init__(self):
        self.config_service = get_config_service()
        self.config = self.config_service.get_config()

    def get_computing_params(self, job_type: str, partition: str, do_node_sharing: bool = True) -> Dict[str, Any]:
        """Calculate computing parameters for a job type and partition"""
        conf_comp = self.config.computing
        
        # Find job type category
        job_category = None
        for category, jobs in conf_comp.JOBTypesCompute.items():
            if job_type in jobs:
                job_category = category
                break
        
        if not job_category:
            return {}

        # Get partition setup
        partition_setup = getattr(conf_comp, partition.replace('-', '_'), None)
        if not partition_setup:
            return {}

        # Get alias mappings
        part_name_alias = self._get_alias_reverse(job_type, "PartionName") or "qsub_extra3"
        nodes_alias = self._get_alias_reverse(job_type, "NrNodes") or "qsub_extra1"
        gpu_alias = self._get_alias_reverse(job_type, "NrGPU") or "qsub_extra4"
        memory_alias = self._get_alias_reverse(job_type, "MemoryRAM") or "qsub_extra5"
        mpi_per_node_alias = self._get_alias_reverse(job_type, "MPIperNode")

        comp_params = {}
        comp_params[part_name_alias] = partition
        
        # Handle node sharing
        node_sharing = conf_comp.NODE_Sharing
        memory_ram = partition_setup.RAM
        if do_node_sharing and partition in node_sharing.ApplyTo:
            memory_ram = str(round(int(partition_setup.RAM[:-1]) / 2)) + "G"
        
        comp_params[memory_alias] = memory_ram

        # Calculate parameters based on job category
        if job_category == "CPU-MPI":
            comp_params[mpi_per_node_alias] = partition_setup.NrCPU
            comp_params["nr_mpi"] = partition_setup.NrCPU * 1  # Default to 1 node
            comp_params[gpu_alias] = 0
            comp_params[nodes_alias] = 1
            comp_params["nr_threads"] = 1
            
        elif job_category in ["GPU-OneProcess", "GPU-OneProcessOneGPU"]:
            comp_params[mpi_per_node_alias] = 1
            comp_params["nr_mpi"] = 1
            comp_params[gpu_alias] = partition_setup.NrGPU
            comp_params[nodes_alias] = 1
            comp_params["nr_threads"] = partition_setup.NrGPU
            
            if job_category == "GPU-OneProcessOneGPU":
                comp_params[gpu_alias] = 1
                
        elif job_category == "GPU-MultProcess":
            comp_params[mpi_per_node_alias] = partition_setup.NrGPU
            comp_params[gpu_alias] = partition_setup.NrGPU
            comp_params["nr_mpi"] = partition_setup.NrGPU * 1  # Default to 1 node
            comp_params["nr_threads"] = 1
            comp_params[nodes_alias] = 1

        # Add jobs per device if specified
        if job_type in conf_comp.JOBsPerDevice:
            comp_params["param10_value"] = conf_comp.JOBsPerDevice[job_type].get(partition, 1)

        return comp_params

    def _get_alias_reverse(self, job: str, alias: str) -> Optional[str]:
        """Get parameter name from alias (reverse of get_alias)"""
        for entry in self.config.aliases:
            if (entry.Job == job or entry.Job == "all") and entry.Alias == alias:
                return entry.Parameter
        return None

    def get_default_partition(self, job_type: str) -> str:
        """Get default partition for a job type"""
        # You might want to make this configurable
        return "g"  # Default to GPU partition
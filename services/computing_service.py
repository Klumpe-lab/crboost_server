
from typing import Any, Dict


class ComputingService:
    def get_computing_params(self, job_type: str, partition: str, do_node_sharing: bool = True) -> Dict[str, Any]:
        """Calculate computing parameters for a job type and partition"""
        print(f"[COMPUTING DEBUG] Getting params for job_type={job_type}, partition={partition}")
        
        conf_comp = self.config.computing
        print(f"[COMPUTING DEBUG] Available JOBTypesCompute: {conf_comp.JOBTypesCompute}")
        
        # Find job type category
        job_category = None
        for category, jobs in conf_comp.JOBTypesCompute.items():
            if job_type in jobs:
                job_category = category
                break
        
        print(f"[COMPUTING DEBUG] Found job_category: {job_category}")
        
        if not job_category:
            print(f"[COMPUTING DEBUG] No job category found for {job_type}")
            return {}

        # Get partition setup
        partition_attr = partition.replace('-', '_')
        print(f"[COMPUTING DEBUG] Looking for partition: {partition_attr}")
        partition_setup = getattr(conf_comp, partition_attr, None)
        
        if not partition_setup:
            print(f"[COMPUTING DEBUG] Partition {partition} not found in config")
            # Try to find any partition
            available_partitions = [attr for attr in dir(conf_comp) if not attr.startswith('_') and attr not in [
                'QueSize', 'NODE_Sharing', 'JOBTypesCompute', 'JOBTypesApplication', 'JOBMaxNodes', 'JOBsPerDevice'
            ]]
            print(f"[COMPUTING DEBUG] Available partitions: {available_partitions}")
            if available_partitions:
                partition_setup = getattr(conf_comp, available_partitions[0])
                print(f"[COMPUTING DEBUG] Using first available partition: {available_partitions[0]}")
        
        if not partition_setup:
            return {}

        print(f"[COMPUTING DEBUG] Partition setup: {partition_setup}")
        
        part_name_alias = self._get_alias_reverse(job_type, "PartionName") or "qsub_extra3"
        nodes_alias = self._get_alias_reverse(job_type, "NrNodes") or "qsub_extra1"
        gpu_alias = self._get_alias_reverse(job_type, "NrGPU") or "qsub_extra4"
        memory_alias = self._get_alias_reverse(job_type, "MemoryRAM") or "qsub_extra5"
        mpi_per_node_alias = self._get_alias_reverse(job_type, "MPIperNode")

        print(f"[COMPUTING DEBUG] Aliases - part_name: {part_name_alias}, nodes: {nodes_alias}, gpu: {gpu_alias}, memory: {memory_alias}")
        
        comp_params = {}
        comp_params[part_name_alias] = partition
        
        node_sharing = conf_comp.NODE_Sharing
        memory_ram = partition_setup.RAM
        if do_node_sharing and partition in node_sharing.ApplyTo:
            memory_ram = str(round(int(partition_setup.RAM[:-1]) / 2)) + "G"
        
        comp_params[memory_alias] = memory_ram

        # Calculate parameters based on job category
        if job_category == "CPU-MPI":
            comp_params[mpi_per_node_alias] = partition_setup.NrCPU
            comp_params["nr_mpi"] = partition_setup.NrCPU * 1
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
            comp_params["nr_mpi"] = partition_setup.NrGPU * 1
            comp_params["nr_threads"] = 1
            comp_params[nodes_alias] = 1

        # Add jobs per device if specified
        if job_type in conf_comp.JOBsPerDevice:
            comp_params["param10_value"] = conf_comp.JOBsPerDevice[job_type].get(partition, 1)

        print(f"[COMPUTING DEBUG] Final comp_params: {comp_params}")
        return comp_params
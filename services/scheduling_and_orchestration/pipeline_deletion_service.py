# services/pipeline_deletion_service.py
"""
Proper job deletion following Relion conventions.
Relion has no CLI for deletion - only GUI. We implement equivalent logic.
"""

import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import pandas as pd
from services.configs.starfile_service import StarfileService
from services.project_state import JobType


@dataclass
class DeletionResult:
    success      : bool
    deleted_jobs : List[str] = field(default_factory=list)
    orphaned_jobs: List[str] = field(default_factory=list)
    error        : Optional[str] = None
    message      : str = ""


@dataclass
class PipelineGraph:
    """In-memory representation of the Relion pipeline graph."""
    job_counter: int
    processes: pd.DataFrame      # pipeline_processes
    nodes: pd.DataFrame          # pipeline_nodes
    input_edges: pd.DataFrame    # pipeline_input_edges
    output_edges: pd.DataFrame   # pipeline_output_edges
    
    def get_job_output_nodes(self, job_name: str) -> List[str]:
        """Get all output node names for a job."""
        if self.output_edges.empty:
            return []
        mask = self.output_edges["rlnPipeLineEdgeProcess"] == job_name
        return self.output_edges.loc[mask, "rlnPipeLineEdgeToNode"].tolist()
    
    def get_downstream_jobs(self, job_name: str) -> List[str]:
        """Find jobs that depend on this job's outputs."""
        output_nodes = self.get_job_output_nodes(job_name)
        if not output_nodes or self.input_edges.empty:
            return []
        
        downstream = set()
        for node in output_nodes:
            mask = self.input_edges["rlnPipeLineEdgeFromNode"] == node
            dependent_jobs = self.input_edges.loc[mask, "rlnPipeLineEdgeProcess"].tolist()
            downstream.update(dependent_jobs)
        
        return list(downstream)
    
    def get_job_status(self, job_name: str) -> Optional[str]:
        """Get the status of a job."""
        if self.processes.empty:
            return None
        mask = self.processes["rlnPipeLineProcessName"] == job_name
        matches = self.processes.loc[mask, "rlnPipeLineProcessStatusLabel"]
        return matches.iloc[0] if not matches.empty else None


class PipelineDeletionService:
    """
    Handles proper job deletion following Relion conventions.
    
    Relion's deletion logic (from pipeliner.cpp):
    1. Mark job and its output nodes for deletion
    2. Optionally cascade to downstream dependents (we don't do this)
    3. Write updated pipeline without deleted entries
    4. Move job directories to Trash/
    5. Write deleted_pipeline.star for audit trail
    """
    



    def __init__(self):
        self.star_handler = StarfileService()
    
    def load_pipeline_graph(self, project_dir: Path) -> Optional[PipelineGraph]:
        """Load the pipeline graph from default_pipeline.star."""
        pipeline_star = project_dir / "default_pipeline.star"
        if not pipeline_star.exists():
            return None
        
        try:
            data = self.star_handler.read(pipeline_star)
            
            # Extract job counter
            general = data.get("pipeline_general", {})
            if isinstance(general, pd.DataFrame) and not general.empty:
                job_counter = int(general["rlnPipeLineJobCounter"].iloc[0])
            elif isinstance(general, dict):
                job_counter = int(general.get("rlnPipeLineJobCounter", 0))
            else:
                job_counter = 0
            
            return PipelineGraph(
                job_counter=job_counter,
                processes=data.get("pipeline_processes", pd.DataFrame()),
                nodes=data.get("pipeline_nodes", pd.DataFrame()),
                input_edges=data.get("pipeline_input_edges", pd.DataFrame()),
                output_edges=data.get("pipeline_output_edges", pd.DataFrame()),
            )
        except Exception as e:
            print(f"[DELETION] Failed to load pipeline graph: {e}")
            return None
    
    def save_pipeline_graph(self, project_dir: Path, graph: PipelineGraph):
        """Save the pipeline graph back to default_pipeline.star."""
        pipeline_star = project_dir / "default_pipeline.star"
        
        # Reconstruct the data dict
        data = {
            "pipeline_general": pd.DataFrame({
                "rlnPipeLineJobCounter": [graph.job_counter]
            }),
            "pipeline_processes": graph.processes,
            "pipeline_nodes": graph.nodes,
            "pipeline_input_edges": graph.input_edges,
            "pipeline_output_edges": graph.output_edges,
        }
        
        self.star_handler.write(data, pipeline_star)
    
    def write_deleted_pipeline(
        self, 
        project_dir: Path, 
        deleted_processes: pd.DataFrame,
        deleted_nodes: pd.DataFrame,
        job_name: str
    ):
        """
        Write deleted entries to deleted_pipeline.star for audit trail.
        Follows Relion convention of appending to existing file.
        """
        deleted_star = project_dir / "Trash" / job_name.rstrip("/").replace("/", "_") / "deleted_pipeline.star"
        deleted_star.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            "pipeline_processes": deleted_processes,
            "pipeline_nodes": deleted_nodes,
        }
        
        self.star_handler.write(data, deleted_star)
    
    async def delete_job(
        self,
        project_dir: Path,
        job_path: str,  # e.g., "External/job005/" or "External/job005"
        recursive: bool = False,  # If True, also delete downstream dependents
    ) -> DeletionResult:
        """
        Delete a job from the pipeline.
        
        Args:
            project_dir: Path to the project root
            job_path: The job path as it appears in default_pipeline.star
            recursive: If True, cascade delete to downstream jobs (NOT IMPLEMENTED YET)
        
        Returns:
            DeletionResult with success status and orphaned job info
        """
        # Normalize job path (ensure trailing slash)
        job_name = job_path.rstrip("/") + "/"
        
        # 1. Load pipeline graph
        graph = self.load_pipeline_graph(project_dir)
        if graph is None:
            return DeletionResult(
                success=False,
                error="Could not load default_pipeline.star"
            )
        
        # 2. Verify job exists
        if graph.processes.empty:
            return DeletionResult(
                success=False,
                error="Pipeline has no jobs"
            )
        
        job_mask = graph.processes["rlnPipeLineProcessName"] == job_name
        if not job_mask.any():
            return DeletionResult(
                success=False,
                error=f"Job '{job_name}' not found in pipeline"
            )
        
        # 3. Find downstream dependents (for warning)
        downstream_jobs = graph.get_downstream_jobs(job_name)
        
        # 4. Find nodes to delete (output nodes of this job)
        output_nodes = graph.get_job_output_nodes(job_name)
        
        # 5. Store deleted entries for audit trail
        deleted_processes = graph.processes[job_mask].copy()
        node_mask = graph.nodes["rlnPipeLineNodeName"].isin(output_nodes) if not graph.nodes.empty else pd.Series(dtype=bool)
        deleted_nodes = graph.nodes[node_mask].copy() if node_mask.any() else pd.DataFrame()
        
        # 6. Remove job from processes
        graph.processes = graph.processes[~job_mask]
        
        # 7. Remove output nodes from nodes table
        if not graph.nodes.empty and output_nodes:
            graph.nodes = graph.nodes[~graph.nodes["rlnPipeLineNodeName"].isin(output_nodes)]
        
        # 8. Remove edges involving this job
        if not graph.input_edges.empty:
            # Remove edges where this job was the consumer
            graph.input_edges = graph.input_edges[
                graph.input_edges["rlnPipeLineEdgeProcess"] != job_name
            ]
            # Remove edges where deleted nodes were the source
            if output_nodes:
                graph.input_edges = graph.input_edges[
                    ~graph.input_edges["rlnPipeLineEdgeFromNode"].isin(output_nodes)
                ]
        
        if not graph.output_edges.empty:
            # Remove edges where this job was the producer
            graph.output_edges = graph.output_edges[
                graph.output_edges["rlnPipeLineEdgeProcess"] != job_name
            ]
        
        # 9. Move job directory to Trash
        job_dir = project_dir / job_name.rstrip("/")
        if job_dir.exists():
            trash_dir = project_dir / "Trash" / job_name.rstrip("/").replace("/", "_")
            trash_dir.parent.mkdir(parents=True, exist_ok=True)
            
            # Remove existing trash if present (Relion does this)
            if trash_dir.exists():
                shutil.rmtree(trash_dir)
            
            shutil.move(str(job_dir), str(trash_dir))
            print(f"[DELETION] Moved {job_dir} to {trash_dir}")
        
        # 10. Write deleted_pipeline.star for audit
        if not deleted_processes.empty:
            self.write_deleted_pipeline(
                project_dir, 
                deleted_processes, 
                deleted_nodes,
                job_name
            )
        
        # 11. Save updated pipeline
        self.save_pipeline_graph(project_dir, graph)
        
        # 12. Build result message
        orphan_msg = ""
        if downstream_jobs:
            orphan_msg = f" Warning: {len(downstream_jobs)} downstream job(s) are now orphaned: {downstream_jobs}"
        
        return DeletionResult(
            success=True,
            deleted_jobs=[job_name],
            orphaned_jobs=downstream_jobs,
            message=f"Deleted {job_name}.{orphan_msg}"
        )
    
    def find_jobs_by_type(
        self, 
        project_dir: Path, 
        job_type: JobType,
        job_resolver  # The JobTypeResolver from orchestrator
    ) -> List[str]:
        """Find all job paths matching a given JobType."""
        graph = self.load_pipeline_graph(project_dir)
        if graph is None or graph.processes.empty:
            return []
        
        matching_jobs = []
        for _, row in graph.processes.iterrows():
            job_path = row["rlnPipeLineProcessName"]
            detected_type = job_resolver.get_job_type_from_path(project_dir, job_path)
            if detected_type == job_type.value:
                matching_jobs.append(job_path)
        
        return matching_jobs
    
    def get_orphaned_jobs(self, project_dir: Path) -> List[Tuple[str, List[str]]]:
        """
        Find all jobs that have broken input references.
        Returns list of (job_path, [missing_input_nodes])
        """
        graph = self.load_pipeline_graph(project_dir)
        if graph is None:
            return []
        
        # Get set of all existing nodes
        existing_nodes = set(graph.nodes["rlnPipeLineNodeName"].tolist()) if not graph.nodes.empty else set()
        
        orphans = []
        if not graph.input_edges.empty:
            for job_name in graph.processes["rlnPipeLineProcessName"].unique():
                job_inputs = graph.input_edges[
                    graph.input_edges["rlnPipeLineEdgeProcess"] == job_name
                ]["rlnPipeLineEdgeFromNode"].tolist()
                
                missing = [node for node in job_inputs if node not in existing_nodes]
                if missing:
                    orphans.append((job_name, missing))
        
        return orphans

    # In services/pipeline_deletion_service.py

    # Add to PipelineDeletionService class:

    def preview_deletion(
        self,
        project_dir: Path,
        job_path: str,
        job_resolver=None,  # Optional: to get human-readable job types
    ) -> Dict[str, Any]:
        """
        Preview what would happen if we delete this job.
        Returns info about downstream jobs that would be orphaned.
        
        Does NOT modify anything.
        """
        job_name = job_path.rstrip("/") + "/"
        
        graph = self.load_pipeline_graph(project_dir)
        if graph is None:
            return {"success": False, "error": "Could not load pipeline"}
        
        # Check job exists
        if graph.processes.empty:
            return {"success": False, "error": "Pipeline has no jobs"}
        
        job_mask = graph.processes["rlnPipeLineProcessName"] == job_name
        if not job_mask.any():
            return {"success": False, "error": f"Job '{job_name}' not found"}
        
        # Get job status
        job_status = graph.get_job_status(job_name)
        
        # Find downstream dependents
        downstream_jobs = graph.get_downstream_jobs(job_name)
        
        # Enrich with job type info if resolver provided
        downstream_details = []
        for downstream_path in downstream_jobs:
            detail = {
                "path": downstream_path,
                "status": graph.get_job_status(downstream_path),
            }
            if job_resolver:
                job_type_str = job_resolver.get_job_type_from_path(project_dir, downstream_path)
                detail["type"] = job_type_str
            downstream_details.append(detail)
        
        return {
            "success": True,
            "job_path": job_name,
            "job_status": job_status,
            "downstream_count": len(downstream_jobs),
            "downstream_jobs": downstream_details,
            "warning": f"{len(downstream_jobs)} job(s) will become orphaned" if downstream_jobs else None,
        }

# Singleton instance
_deletion_service: Optional[PipelineDeletionService] = None

def get_deletion_service() -> PipelineDeletionService:
    global _deletion_service
    if _deletion_service is None:
        _deletion_service = PipelineDeletionService()
    return _deletion_service
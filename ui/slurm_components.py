# ui/slurm_components.py (NEW FILE)
"""Modular SLURM UI components"""
import asyncio
from nicegui import ui
from typing import Dict, Any, Callable


def build_slurm_job_config(backend, panel_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build SLURM job configuration inputs.
    Returns dict with input references.
    """
    
    slurm_inputs = {}
    
    with ui.column().classes("w-full gap-2"):
        # Each parameter on its own line with SBATCH prefix
        with ui.row().classes("w-full items-center gap-2"):
            ui.label("--partition").classes("text-xs font-mono text-gray-600 w-32")
            slurm_inputs["partition"] = ui.select(
                options=[],
                value=None
            ).props("dense outlined").classes("flex-grow")
        
        with ui.row().classes("w-full items-center gap-2"):
            ui.label("--constraint").classes("text-xs font-mono text-gray-600 w-32")
            slurm_inputs["constraint"] = ui.input(
                value="g2|g3|g4",
                placeholder="e.g. g2|g3|g4"
            ).props("dense outlined").classes("flex-grow")
        
        with ui.row().classes("w-full items-center gap-2"):
            ui.label("--nodes").classes("text-xs font-mono text-gray-600 w-32")
            slurm_inputs["nodes"] = ui.input(
                value="1"
            ).props("dense outlined type=number min=1").classes("flex-grow")
        
        with ui.row().classes("w-full items-center gap-2"):
            ui.label("--ntasks-per-node").classes("text-xs font-mono text-gray-600 w-32")
            slurm_inputs["ntasks"] = ui.input(
                value="1"
            ).props("dense outlined type=number min=1").classes("flex-grow")
        
        with ui.row().classes("w-full items-center gap-2"):
            ui.label("--cpus-per-task").classes("text-xs font-mono text-gray-600 w-32")
            slurm_inputs["cpus"] = ui.input(
                value="16"
            ).props("dense outlined type=number min=1").classes("flex-grow")
        
        with ui.row().classes("w-full items-center gap-2"):
            ui.label("--gres").classes("text-xs font-mono text-gray-600 w-32")
            slurm_inputs["gpus"] = ui.input(
                value="4"
            ).props("dense outlined type=number min=0").classes("flex-grow")
            ui.label("gpu:").classes("text-xs text-gray-500")
        
        with ui.row().classes("w-full items-center gap-2"):
            ui.label("--mem").classes("text-xs font-mono text-gray-600 w-32")
            slurm_inputs["memory"] = ui.input(
                value="96"
            ).props("dense outlined type=number min=1").classes("flex-grow")
            ui.label("GB").classes("text-xs text-gray-500")
        
        with ui.row().classes("w-full items-center gap-2"):
            ui.label("--time").classes("text-xs font-mono text-gray-600 w-32")
            slurm_inputs["time"] = ui.input(
                value="5:00:00",
                placeholder="HH:MM:SS"
            ).props("dense outlined").classes("flex-grow")
    
    return slurm_inputs


def build_cluster_overview(backend, panel_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build cluster overview section with jobs, partitions, and nodes.
    Returns dict with container references and refresh callback.
    """
    overview_state = {}
    
    with ui.column().classes("w-full gap-3"):
        # Header with refresh button
        with ui.row().classes("w-full items-center justify-between mb-2"):
            ui.label("Cluster Overview").classes("text-sm font-medium")
            refresh_btn = ui.button(
                icon="refresh",
                on_click=lambda: asyncio.create_task(refresh_cluster_data())
            ).props("dense flat round size=sm").classes("text-blue-600")
            refresh_btn.tooltip("Refresh cluster info")
        
        overview_state["refresh_btn"] = refresh_btn
        
        # MY JOBS - Always visible, first element
        ui.label("My Jobs").classes("text-xs font-semibold text-gray-700 uppercase tracking-wide mt-2")
        jobs_container = ui.column().classes("w-full gap-2 mb-3")
        overview_state["jobs_container"] = jobs_container
        
        # PARTITIONS - Button switcher
        ui.label("Partitions").classes("text-xs font-semibold text-gray-700 uppercase tracking-wide mt-2")
        partition_buttons_row = ui.row().classes("w-full gap-2 mb-2 flex-wrap")
        overview_state["partition_buttons_row"] = partition_buttons_row
        overview_state["partition_buttons"] = {}
        overview_state["selected_partition"] = None
        
        # NODES - Compact display
        nodes_container = ui.column().classes("w-full")
        overview_state["nodes_container"] = nodes_container
    
    async def refresh_cluster_data(force_refresh: bool = True):
        """Refresh all cluster overview data"""
        try:
            refresh_btn.props("loading")
            
            if force_refresh:
                backend.slurm_service.clear_cache()
            
            # Get partitions
            partitions_result = await backend.get_slurm_partitions()
            if partitions_result.get("success"):
                partitions = partitions_result["partitions"]
                unique_partitions = {}
                for p in partitions:
                    name = p["name"]
                    if name not in unique_partitions:
                        unique_partitions[name] = p
                
                partition_names = sorted(unique_partitions.keys())
                
                # Update partition buttons
                partition_buttons_row.clear()
                overview_state["partition_buttons"].clear()
                
                with partition_buttons_row:
                    for p_name in partition_names:
                        btn = ui.button(
                            p_name,
                            on_click=lambda pn=p_name: asyncio.create_task(switch_partition(pn))
                        ).props("dense size=sm")
                        
                        if overview_state["selected_partition"] == p_name:
                            btn.props("color=primary")
                        else:
                            btn.props("outline")
                        
                        overview_state["partition_buttons"][p_name] = btn
                
                # Load first partition by default
                if partition_names and not overview_state["selected_partition"]:
                    overview_state["selected_partition"] = partition_names[0]
                    await load_partition_nodes(partition_names[0])
            
            # Get user jobs
            jobs_result = await backend.get_user_slurm_jobs(force_refresh=force_refresh)
            if jobs_result.get("success"):
                jobs = jobs_result["jobs"]
                
                jobs_container.clear()
                with jobs_container:
                    if not jobs:
                        ui.label("No active jobs").classes("text-xs text-gray-500 italic")
                    else:
                        for job in jobs:
                            state_color = {
                                "RUNNING": "green",
                                "PENDING": "orange",
                                "COMPLETED": "blue",
                                "FAILED": "red"
                            }.get(job["state"], "gray")
                            
                            with ui.row().classes("w-full items-center gap-2 p-2 border rounded bg-white/50"):
                                ui.badge(job["state"], color=state_color).classes("text-xs")
                                ui.label(job["name"]).classes("text-xs font-medium flex-grow")
                                ui.label(f"ID: {job['job_id']}").classes("text-xs text-gray-500 font-mono")
                                ui.label(job["time"]).classes("text-xs text-gray-500")
            
            print("[INFO] Cluster overview refreshed")
            
        except Exception as e:
            print(f"[ERROR] Failed to refresh cluster overview: {e}")
            import traceback
            traceback.print_exc()
        finally:
            refresh_btn.props(remove="loading")
    
    async def switch_partition(partition_name: str):
        """Switch to viewing a different partition"""
        overview_state["selected_partition"] = partition_name
        
        # Update button styles
        for p_name, btn in overview_state["partition_buttons"].items():
            if p_name == partition_name:
                btn.props(remove="outline")
                btn.props("color=primary")
            else:
                btn.props(remove="color=primary")
                btn.props("outline")
        
        await load_partition_nodes(partition_name)
    
    async def load_partition_nodes(partition_name: str):
        """Load and display nodes for a partition"""
        if not partition_name:
            return
        
        try:
            nodes_container.clear()
            
            nodes_result = await backend.get_slurm_nodes(partition_name)
            if nodes_result.get("success"):
                nodes = nodes_result["nodes"]
                
                with nodes_container:
                    if not nodes:
                        ui.label("No nodes available").classes("text-xs text-gray-500 italic")
                    else:
                        # State summary badges
                        state_counts = {}
                        for node in nodes:
                            state = node["state"].lower()
                            state_counts[state] = state_counts.get(state, 0) + 1
                        
                        with ui.row().classes("w-full gap-2 mb-3"):
                            for state, count in sorted(state_counts.items()):
                                state_color = {
                                    "idle": "green",
                                    "allocated": "blue",
                                    "alloc": "blue",
                                    "mix": "orange",
                                    "down": "red",
                                    "drain": "red",
                                    "comp": "orange"
                                }.get(state, "gray")
                                ui.badge(f"{state}: {count}", color=state_color).classes("text-xs")
                        
                        # Compact node cards
                        with ui.grid(columns=3).classes("w-full gap-2"):
                            for node in nodes:
                                state_color = {
                                    "idle": "green",
                                    "allocated": "blue",
                                    "alloc": "blue",
                                    "mix": "orange",
                                    "down": "red",
                                    "drain": "red",
                                    "comp": "orange"
                                }.get(node["state"].lower(), "gray")
                                
                                with ui.card().classes("p-2 bg-white/30 backdrop-blur-sm border"):
                                    # Node name prominent with state badge
                                    with ui.row().classes("w-full items-center justify-between mb-1"):
                                        ui.label(node["name"]).classes("text-xs font-bold font-mono")
                                        ui.badge(node["state"], color=state_color).classes("text-xs")
                                    
                                    # Uniform stats list
                                    with ui.column().classes("gap-0"):
                                        ui.label(f"CPUs: {node['cpus']}").classes("text-xs text-gray-700")
                                        mem_gb = node['memory_mb'] / 1024
                                        ui.label(f"Memory: {mem_gb:.1f}G").classes("text-xs text-gray-700")
                                        if node["gpus"] > 0:
                                            gpu_label = f"GPUs: {node['gpus']} x {node['gpu_type'] or 'GPU'}"
                                            ui.label(gpu_label).classes("text-xs text-purple-700 font-medium")
        
        except Exception as e:
            print(f"[ERROR] Failed to load partition nodes: {e}")
            with nodes_container:
                ui.label(f"Error: {e}").classes("text-xs text-red-600")
    
    overview_state["refresh_cluster_data"] = refresh_cluster_data
    overview_state["load_partition_nodes"] = load_partition_nodes
    
    return overview_state
# ui/continuation_controls.py
"""
Pipeline continuation controls - delete/re-add jobs.
Only shown when loading an existing project.
"""
import asyncio
from backend import CryoBoostBackend
from nicegui import ui
from services.project_state import JobStatus, JobType
from typing import Dict, Any


def build_continuation_controls(backend, shared_state: Dict[str, Any], callbacks: Dict[str, Any]):
    """
    Build the continuation controls panel.
    Shows discovered jobs with delete buttons and add job functionality.
    """
    
    if not shared_state.get("continuation_mode"):
        return None
    
    ui.label("PIPELINE CONTINUATION").classes("text-xs font-semibold text-black uppercase tracking-wider mb-3")
    
    with ui.card().classes("w-full mb-4").style("padding: 12px;"):
        ui.label("Discovered Jobs").classes("text-sm font-medium mb-2")
        
        jobs_container = ui.column().classes("w-full").style("gap: 8px;")
        
        with jobs_container:
            for job_type in shared_state.get("selected_jobs", []):
                _render_job_row(job_type, backend, shared_state, callbacks)
        
        # Add job button (for future)
        with ui.row().classes("w-full mt-3").style("gap: 8px;"):
            ui.button("Add Job", icon="add").props("dense flat no-caps disable").style(
                "background: #f3f4f6; color: #6b7280; padding: 6px 16px; border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;"
            ).tooltip("Coming soon: Add jobs to pipeline")
    
    return {"jobs_container": jobs_container}


def _render_job_row(job_type: JobType, backend:CryoBoostBackend, shared_state, callbacks):
    """Render a single job row with status and delete button"""
    job_model = backend.state_service.state.jobs.get(job_type)
    if not job_model:
        return
    
    status = job_model.execution_status
    job_name = job_model.relion_job_name or "Not started"
    
    status_colors = {
        JobStatus.SCHEDULED: "#fbbf24",
        JobStatus.RUNNING: "#3b82f6",
        JobStatus.SUCCEEDED: "#10b981",
        JobStatus.FAILED: "#ef4444",
    }
    
    with ui.row().classes("w-full items-center p-2").style(
        "background: #fafafa; border-radius: 3px; gap: 12px;"
    ):
        # Status dot
        ui.element("div").style(
            f"width: 8px; height: 8px; border-radius: 50%; "
            f"background: {status_colors.get(status, '#6b7280')}; flex-shrink: 0;"
        )
        
        # Job info
        with ui.column().classes("flex-grow").style("gap: 2px;"):
            ui.label(job_type.value).classes("text-sm font-medium")
            ui.label(f"{status.value} · {job_name}").classes("text-xs text-gray-600")
        
        # Delete button (only for failed/succeeded jobs)
        if status in [JobStatus.FAILED, JobStatus.SUCCEEDED]:
            ui.button(
                icon="delete",
                on_click=lambda j=job_type: _handle_delete_job(j, backend, shared_state, callbacks)
            ).props("dense flat round").style(
                "background: #fef2f2; color: #dc2626; width: 32px; height: 32px;"
            ).tooltip(f"Delete and reset {job_type.value}")
        elif status == JobStatus.RUNNING:
            ui.label("Running").classes("text-xs text-gray-500 italic")
        else:
            ui.label("Scheduled").classes("text-xs text-gray-500 italic")


async def _handle_delete_job(job_type: JobType, backend, shared_state, callbacks):
    """Handle delete job with confirmation"""
    job_model = app_state.jobs.get(job_type.value)
    if not job_model or not job_model.relion_job_number:
        ui.notify("Cannot delete: job has no number", type="warning")
        return
    
    job_number = job_model.relion_job_number
    
    # Confirmation dialog
    with ui.dialog() as dialog, ui.card():
        ui.label(f"Delete job{job_number:03d} ({job_type.value})?").classes("text-lg font-semibold mb-4")
        ui.label("This will:").classes("text-sm mb-2")
        with ui.column().classes("text-sm text-gray-700 mb-4").style("gap: 4px;"):
            ui.label("• Remove the job from pipeline")
            ui.label("• Move job directory to Trash/")
            ui.label("• Reset job in scheme for re-running")
        
        with ui.row().classes("w-full justify-end").style("gap: 8px;"):
            ui.button("Cancel", on_click=dialog.close).props("flat")
            ui.button(
                "Delete",
                on_click=lambda: _confirm_delete_job(job_type, job_number, dialog, backend, shared_state, callbacks)
            ).props("flat").style("color: #dc2626;")
    
    dialog.open()


async def _confirm_delete_job(job_type: JobType, job_number, dialog, backend:CryoBoostBackend, shared_state, callbacks):
    """Actually delete the job"""
    dialog.close()
    ui.notify(f"Deleting job{job_number:03d}...", type="info")

    try:
        # Remove the await since it's a synchronous function
        result = backend.continuation.delete_and_reset_job(
            project_path=shared_state["current_project_path"],
            job_number=job_number,
            scheme_name=shared_state["current_scheme_name"]
        )

        if result.get("success"):
            ui.notify(result.get("message", "Job deleted successfully"), type="positive")
            
            # Sync statuses from pipeline.star
            await backend.pipeline_runner.status_sync.sync_all_jobs(shared_state["current_project_path"])
            
            # Remove the job from selected_jobs and job_cards
            if job_type in shared_state["selected_jobs"]:
                shared_state["selected_jobs"].remove(job_type)
            
            if job_type in shared_state["job_cards"]:
                # Stop any timers for this job
                job_state = shared_state["job_cards"][job_type]
                if job_state.get("logs_timer"):
                    job_state["logs_timer"].cancel()
                del shared_state["job_cards"][job_type]
            
            # Reset the job model
            job_model = backend.state_service.state.jobs.get(job_type)
            if job_model:
                job_model.execution_status = JobStatus.SCHEDULED
                job_model.relion_job_name = None
                job_model.relion_job_number = None
            
            # Rebuild UI
            if "rebuild_pipeline_ui" in callbacks:
                callbacks["rebuild_pipeline_ui"]()
        else:
            ui.notify(f"Failed to delete: {result.get('error')}", type="negative")
            
    except Exception as e:
        ui.notify(f"Error during deletion: {str(e)}", type="negative")
# In ui/status_indicator.py

from pathlib import Path
from nicegui import ui
from services.project_state import JobStatus, JobType, get_project_state


class ReactiveStatusDot:
    """Status dot that shows running/succeeded/failed/orphaned state."""

    def __init__(self, job_type: JobType):
        self.job_type = job_type
        self._render()

    def _render(self):
        state = get_project_state()
        job_model = state.jobs.get(self.job_type)

        if not job_model:
            status = JobStatus.SCHEDULED
            is_orphaned = False
        else:
            status = job_model.execution_status
            is_orphaned = job_model.is_orphaned

        # Orphaned jobs get a special indicator
        if is_orphaned:
            color = "#f97316"  # Orange
            css_class = "pulse-orphaned"
            tooltip = "Orphaned: missing input dependencies"
        else:
            color_map = {JobStatus.RUNNING: "#3b82f6", JobStatus.SUCCEEDED: "#10b981", JobStatus.FAILED: "#ef4444"}
            color = color_map.get(status, "#fbbf24")

            class_map = {
                JobStatus.RUNNING: "pulse-running",
                JobStatus.SUCCEEDED: "pulse-success",
                JobStatus.FAILED: "pulse-failed",
            }
            css_class = class_map.get(status, "pulse-scheduled")
            tooltip = status.value

        dot = (
            ui.element("div")
            .classes(f"status-dot {css_class}")
            .style(f"width: 8px; height: 8px; border-radius: 50%; display: inline-block; background-color: {color};")
        )
        dot.tooltip(tooltip)


class ReactiveStatusBadge:
    """Status badge that shows job status with orphan indicator."""
    
    def __init__(self, job_type: JobType):
        self.job_type = job_type
        self._render()
    
    def _render(self):
        state = get_project_state()
        job_model = state.jobs.get(self.job_type)
        
        if not job_model:
            status = JobStatus.SCHEDULED
            is_orphaned = False
            missing_inputs = []
        else:
            status = job_model.execution_status
            is_orphaned = job_model.is_orphaned
            missing_inputs = getattr(job_model, 'missing_inputs', [])
        
        with ui.row().classes("items-center gap-1"):
            # Main status badge
            colors = {
                JobStatus.SCHEDULED: ("bg-yellow-100", "text-yellow-800"),
                JobStatus.RUNNING: ("bg-blue-100", "text-blue-800"),
                JobStatus.SUCCEEDED: ("bg-green-100", "text-green-800"),
                JobStatus.FAILED: ("bg-red-100", "text-red-800"),
                JobStatus.UNKNOWN: ("bg-gray-100", "text-gray-800"),
            }
            bg, txt = colors.get(status, ("bg-gray-100", "text-gray-800"))
            
            ui.label(status.value).classes(f"text-xs font-bold px-2 py-0.5 rounded-full {bg} {txt}")
            
            # Orphan warning icon with detailed tooltip
            if is_orphaned:
                tooltip_text = "Orphaned: Missing inputs"
                if missing_inputs:
                    # Show first 3 missing items
                    items = missing_inputs[:3]
                    tooltip_text = "Missing:\n" + "\n".join(f"• {Path(p).name}" for p in items)
                    if len(missing_inputs) > 3:
                        tooltip_text += f"\n... and {len(missing_inputs) - 3} more"
                
                icon = ui.icon("link_off", size="16px").classes("text-orange-500 cursor-help")
                icon.tooltip(tooltip_text)
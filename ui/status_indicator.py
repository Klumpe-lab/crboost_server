# ui/status_indicator.py
from nicegui import ui
from services.project_state import JobStatus, JobType, get_project_state


class ReactiveStatusDot(ui.element):
    """
    A status dot that automatically polls the backend state for its job_type.
    """
    def __init__(self, job_type: JobType):
        super().__init__('div')
        self.job_type = job_type
        self._current_status = None
        self.style("width: 8px; height: 8px; border-radius: 50%; display: inline-block; transition: background-color 0.3s;")
        
        self.update_appearance()
        self.timer = ui.timer(1.0, self.update_appearance)

    def update_appearance(self):
        state = get_project_state()
        job = state.jobs.get(self.job_type)
        status = job.execution_status if job else JobStatus.SCHEDULED
        
        # Skip update if status hasn't changed
        if status == self._current_status:
            return
        self._current_status = status
        
        color_map = {
            JobStatus.RUNNING: "#3b82f6",
            JobStatus.SUCCEEDED: "#10b981",
            JobStatus.FAILED: "#ef4444",
            JobStatus.SCHEDULED: "#fbbf24",
            JobStatus.UNKNOWN: "#d1d5db"
        }
        color = color_map.get(status, "#d1d5db")
        
        # Handle pulse animation
        if status == JobStatus.RUNNING:
            self.classes(add="pulse-running")
        else:
            self.classes(remove="pulse-running")
            
        self.style(f"width: 8px; height: 8px; border-radius: 50%; display: inline-block; background-color: {color};")


class ReactiveStatusBadge(ui.label):
    """
    A status text badge that auto-updates.
    """
    def __init__(self, job_type: JobType):
        super().__init__()
        self.job_type = job_type
        self._current_status = None
        self.timer = ui.timer(1.0, self.update_appearance)
        self.update_appearance()

    def update_appearance(self):
        state = get_project_state()
        job = state.jobs.get(self.job_type)
        status = job.execution_status if job else JobStatus.SCHEDULED
        
        # Skip update if status hasn't changed
        if status == self._current_status:
            return
        self._current_status = status
        
        self.set_text(status.value)
        
        style_map = {
            JobStatus.SCHEDULED: ("bg-yellow-100", "text-yellow-800"),
            JobStatus.RUNNING:   ("bg-blue-100", "text-blue-800"),
            JobStatus.SUCCEEDED: ("bg-green-100", "text-green-800"),
            JobStatus.FAILED:    ("bg-red-100", "text-red-800"),
            JobStatus.UNKNOWN:   ("bg-gray-100", "text-gray-800"),
        }
        
        # Remove old status classes, add new ones
        for bg, txt in style_map.values():
            self.classes(remove=bg)
            self.classes(remove=txt)
        
        bg, txt = style_map.get(status, style_map[JobStatus.UNKNOWN])
        self.classes(add=f"text-xs font-bold px-2 py-0.5 rounded-full {bg} {txt}")
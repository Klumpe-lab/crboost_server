# ui/status_indicator.py
from pathlib import Path
from nicegui import ui
from services.project_state import JobStatus, get_project_state

_DOT_COLORS = {
    JobStatus.SCHEDULED: "#fbbf24",
    JobStatus.RUNNING: "#3b82f6",
    JobStatus.SUCCEEDED: "#10b981",
    JobStatus.FAILED: "#ef4444",
    JobStatus.UNKNOWN: "#9ca3af",
}

_DOT_PULSES = {
    JobStatus.RUNNING: "pulse-running",
    JobStatus.SUCCEEDED: "pulse-success",
    JobStatus.FAILED: "pulse-failed",
}

_BADGE_STYLES = {
    JobStatus.SCHEDULED: ("background:#fef3c7;", "color:#92400e;"),
    JobStatus.RUNNING: ("background:#dbeafe;", "color:#1e40af;"),
    JobStatus.SUCCEEDED: ("background:#d1fae5;", "color:#065f46;"),
    JobStatus.FAILED: ("background:#fee2e2;", "color:#991b1b;"),
    JobStatus.UNKNOWN: ("background:#f3f4f6;", "color:#1f2937;"),
}


def _dot_html(status: JobStatus, is_orphaned: bool = False) -> str:
    if is_orphaned:
        color = "#f97316"
        pulse = "pulse-orphaned"
        tip = "Orphaned: missing input dependencies"
    else:
        color = _DOT_COLORS.get(status, "#fbbf24")
        pulse = _DOT_PULSES.get(status, "pulse-scheduled")
        tip = status.value
    return (
        f'<span class="status-dot {pulse}" '
        f'style="width:8px;height:8px;border-radius:50%;display:inline-block;'
        f'background:{color};" title="{tip}"></span>'
    )


def _badge_html(status: JobStatus, is_orphaned: bool = False, missing_inputs: list = None) -> str:
    bg, txt = _BADGE_STYLES.get(status, ("background:#f3f4f6;", "color:#1f2937;"))
    html = (
        f'<span style="{bg}{txt}font-size:12px;font-weight:700;'
        f'padding:2px 8px;border-radius:9999px;white-space:nowrap;">'
        f"{status.value}</span>"
    )
    if is_orphaned:
        missing = missing_inputs or []
        if missing:
            items = [Path(p).name for p in missing[:3]]
            tip = "Missing: " + ", ".join(items)
            if len(missing) > 3:
                tip += f" +{len(missing) - 3} more"
        else:
            tip = "Orphaned: missing input dependencies"
        html += f' <span style="color:#f97316;cursor:help;font-size:14px;" title="{tip}">&#9888;</span>'
    return html


class BoundStatusDot:
    """Status dot bound to a job instance by instance_id."""

    def __init__(self, instance_id: str):
        state = get_project_state()
        job_model = state.jobs.get(instance_id)

        if not job_model:
            ui.html(_dot_html(JobStatus.SCHEDULED), sanitize=False, tag="span")
            return

        ui.html("", sanitize=False, tag="span").bind_content_from(
            job_model, "execution_status", backward=lambda s, jm=job_model: _dot_html(s, is_orphaned=jm.is_orphaned)
        )


class BoundStatusBadge:
    """Status badge bound to a job instance by instance_id."""

    def __init__(self, instance_id: str):
        state = get_project_state()
        job_model = state.jobs.get(instance_id)

        if not job_model:
            ui.html(_badge_html(JobStatus.SCHEDULED), sanitize=False, tag="span")
            return

        ui.html("", sanitize=False, tag="span").bind_content_from(
            job_model,
            "execution_status",
            backward=lambda s, jm=job_model: _badge_html(
                s, is_orphaned=jm.is_orphaned, missing_inputs=getattr(jm, "missing_inputs", [])
            ),
        )


ReactiveStatusDot = BoundStatusDot
ReactiveStatusBadge = BoundStatusBadge

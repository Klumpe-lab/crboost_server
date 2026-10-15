# ui/status_indicator.py
from html import escape

from nicegui import ui
from services.project_state import JobStatus
from ui.current_project import current_project_state

_DOT_COLORS = {
    JobStatus.SCHEDULED: "#fbbf24",
    JobStatus.QUEUED: "#60a5fa",
    JobStatus.RUNNING: "#3b82f6",
    JobStatus.SUCCEEDED: "#10b981",
    JobStatus.FAILED: "#ef4444",
    JobStatus.UNKNOWN: "#9ca3af",
}


_BADGE_STYLES = {
    JobStatus.SCHEDULED: ("background:#fef3c7;", "color:#92400e;"),
    JobStatus.QUEUED: ("background:#eff6ff;", "color:#2563eb;"),
    JobStatus.RUNNING: ("background:#dbeafe;", "color:#1e40af;"),
    JobStatus.SUCCEEDED: ("background:#d1fae5;", "color:#065f46;"),
    JobStatus.FAILED: ("background:#fee2e2;", "color:#991b1b;"),
    JobStatus.UNKNOWN: ("background:#f3f4f6;", "color:#1f2937;"),
}


def _running_spinner_html(size_px: int = 14, color: str = "#3b82f6") -> str:
    """A self-contained pulsating dot shown next to RUNNING jobs.

    Inline SVG with SMIL <animate>: the shape and the motion live in the
    markup, so it needs no @keyframes, no CSS class, no ::before — nothing
    from the Python-injected stylesheet, which can be served stale and does
    not reliably reach these v-html-injected spans. If SMIL is unavailable
    the circle still paints, just static.
    """
    return (
        f'<svg width="{size_px}" height="{size_px}" viewBox="0 0 24 24" '
        'style="display:inline-block;vertical-align:middle;flex-shrink:0;overflow:visible;">'
        f'<circle cx="12" cy="12" r="5" fill="{color}">'
        '<animate attributeName="r" values="4;9;4" dur="1.1s" repeatCount="indefinite"/>'
        '<animate attributeName="opacity" values="1;0.3;1" dur="1.1s" repeatCount="indefinite"/>'
        "</circle></svg>"
    )


def _halo(color: str) -> str:
    """The soft glow around a status light, in its own color (a box-shadow value)."""
    return f"0 0 2px {color}b3, 0 0 5px 1px {color}4d"


def _led_style(color: str, size_px: int, *, lit: bool = True) -> str:
    """Inline style of a status light: a flat dot and, when lit, its halo. Inline because
    the light is v-html content: no stylesheet involved."""
    glow = f"box-shadow:{_halo(color)};" if lit else ""
    return (
        f"width:{size_px}px;height:{size_px}px;border-radius:50%;display:inline-block;flex-shrink:0;"
        f"background:{color};{glow}"
    )


def _running_led_html(size_px: int = 8, color: str = _DOT_COLORS[JobStatus.RUNNING]) -> str:
    """The status light of a RUNNING job: the lit light with a halo behind it that swells and
    brightens. The halo is inline SVG/SMIL, so the motion needs no stylesheet either; without
    SMIL the light still paints, steady."""
    return (
        f'<span style="position:relative;display:inline-block;width:{size_px}px;height:{size_px}px;'
        'flex-shrink:0;vertical-align:middle;">'
        f'<svg width="{3 * size_px}" height="{3 * size_px}" viewBox="0 0 30 30" style="position:absolute;'
        f'left:-{size_px}px;top:-{size_px}px;overflow:visible;pointer-events:none;filter:blur(1px);">'
        f'<circle cx="15" cy="15" r="6" fill="{color}" opacity="0.3">'
        '<animate attributeName="r" values="6;10;6" dur="1.6s" repeatCount="indefinite"/>'
        '<animate attributeName="opacity" values="0.3;0.65;0.3" dur="1.6s" repeatCount="indefinite"/>'
        "</circle></svg>"
        f'<span style="position:absolute;left:0;top:0;{_led_style(color, size_px)}" '
        f'title="{JobStatus.RUNNING.value}"></span></span>'
    )


def _ring_html(color: str, size_px: int, tip: str) -> str:
    """A status light drawn as a ring, with its halo: a job that waits rather than works."""
    return (
        '<span class="status-dot" '
        f'style="width:{size_px}px;height:{size_px}px;border-radius:50%;display:inline-block;flex-shrink:0;'
        f"box-sizing:border-box;border:{size_px / 4:g}px solid {color};background:transparent;"
        f'box-shadow:{_halo(color)};" title="{tip}"></span>'
    )


def _dot_html(status: JobStatus, is_orphaned: bool = False, parked_since: str | None = None, size_px: int = 8) -> str:
    """The job's status light, for every status. RUNNING is the light with a breathing halo
    (_running_led_html). A ring is a job that waits: light blue while queued on the cluster,
    amber when a run parked it behind the tilt-filter review (`parked_since`, HH:MM). Grey
    (unknown) is the one light without a halo."""
    if status == JobStatus.RUNNING:
        return _running_led_html(size_px)
    if parked_since is not None and status == JobStatus.SCHEDULED and not is_orphaned:
        tip = f"Parked: waits for the tilt-filter review since {parked_since}; Approve submits it."
        return _ring_html("#f59e0b", size_px, tip)
    if status == JobStatus.QUEUED and not is_orphaned:
        return _ring_html(_DOT_COLORS[JobStatus.QUEUED], size_px, status.value)
    if is_orphaned:
        color = "#f97316"
        tip = "Orphaned: missing input dependencies"
    else:
        color = _DOT_COLORS.get(status, "#fbbf24")
        tip = status.value
    lit = is_orphaned or status != JobStatus.UNKNOWN
    return f'<span class="status-dot" style="{_led_style(color, size_px, lit=lit)}" title="{tip}"></span>'


def _badge_html(
    status: JobStatus, is_orphaned: bool = False, missing_inputs: list | None = None, slurm_job_id: str | None = None
) -> str:
    bg, txt = _BADGE_STYLES.get(status, ("background:#f3f4f6;", "color:#1f2937;"))
    label = status.value
    if status == JobStatus.QUEUED and slurm_job_id:
        label = f"Queued · {slurm_job_id}"
    html = (
        f'<span style="{bg}{txt}font-size:12px;font-weight:700;'
        f'padding:2px 8px;border-radius:9999px;white-space:nowrap;">'
        f"{label}</span>"
    )
    if is_orphaned:
        # The entries are path-resolution messages (which slot, and why), shown whole.
        missing = missing_inputs or []
        if missing:
            tip = "Missing: " + "; ".join(str(m) for m in missing[:3])
            if len(missing) > 3:
                tip += f" +{len(missing) - 3} more"
        else:
            tip = "Orphaned: missing input dependencies"
        html += f' <span style="color:#f97316;cursor:help;font-size:14px;" title="{escape(tip)}">&#9888;</span>'
    return html


class BoundStatusBadge:
    """Status badge bound to a job instance by instance_id."""

    def __init__(self, instance_id: str):
        state = current_project_state()
        job_model = state.jobs.get(instance_id)
        if not job_model:
            ui.html(_badge_html(JobStatus.SCHEDULED), sanitize=False, tag="span")
            return
        ui.html("", sanitize=False, tag="span").bind_content_from(
            job_model,
            "execution_status",
            backward=lambda s, jm=job_model: _badge_html(
                s,
                is_orphaned=jm.is_orphaned,
                missing_inputs=getattr(jm, "missing_inputs", []),
                slurm_job_id=getattr(jm, "slurm_job_id", None),
            ),
        )


class BoundStatusDot:
    """Status dot bound to a job instance by instance_id."""

    def __init__(self, instance_id: str):
        state = current_project_state()
        job_model = state.jobs.get(instance_id)

        if not job_model:
            ui.html(_dot_html(JobStatus.SCHEDULED), sanitize=False, tag="span")
            return

        ui.html("", sanitize=False, tag="span").bind_content_from(
            job_model, "execution_status", backward=lambda s, jm=job_model: _dot_html(s, is_orphaned=jm.is_orphaned)
        )


ReactiveStatusDot = BoundStatusDot
ReactiveStatusBadge = BoundStatusBadge

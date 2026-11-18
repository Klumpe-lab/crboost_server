"""A job type's glyph: the 12 x 12 line drawing its JobSpec row carries (`glyph`), as inline SVG."""

from __future__ import annotations

from services.jobs.spec import JOB_SPEC_BY_TYPE
from services.models_base import JobType

_FALLBACK = '<rect x="2" y="2" width="8" height="8"/>'


def job_glyph(job_type: JobType, *, size: int = 12, ink: str = "currentColor") -> str:
    spec = JOB_SPEC_BY_TYPE.get(job_type)
    body = (spec.glyph if spec is not None else "") or _FALLBACK
    return (
        f'<svg width="{size}" height="{size}" viewBox="0 0 12 12" fill="none" stroke="currentColor" '
        f'stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round" style="color:{ink};display:block;">'
        f"{body}</svg>"
    )

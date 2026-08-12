"""
Job plugin registry.

Register a custom parameter renderer and/or extra tabs for any JobType.

    from ui.job_plugins import register_params_renderer, register_extra_tab

    @register_params_renderer(JobType.TEMPLATE_MATCH_PYTOM)
    def render_tm_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None):
        ...

    @register_extra_tab(JobType.TEMPLATE_MATCH_PYTOM, key="workbench", label="Workbench", icon="build")
    def render_tm_workbench(job_type, instance_id, job_model, backend, ui_mgr):
        ...

Jobs without a registered plugin get the generic field-dump from default_renderer.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from collections.abc import Callable

from services.models_base import JobType

logger = logging.getLogger(__name__)


@dataclass
class ExtraTab:
    """An additional tab for a specific job type."""

    key: str  # unique ID stored in active_monitor_tab, e.g. "workbench"
    label: str  # display text
    icon: str  # material icon name
    render: Callable  # (job_type, job_model, backend, ui_mgr) -> None


@dataclass
class JobPlugin:
    # (job_type, job_model, is_frozen, save_handler, *, ui_mgr, backend) -> None
    render_params: Callable | None = None
    extra_tabs: list[ExtraTab] = field(default_factory=list)
    # Full-panel renderer replaces the entire tab chrome.
    # Signature: (job_type, instance_id, job_model, backend, ui_mgr, save_handler) -> None
    render_full_panel: Callable | None = None


_REGISTRY: dict[JobType, JobPlugin] = {}


def _ensure(job_type: JobType) -> JobPlugin:
    if job_type not in _REGISTRY:
        _REGISTRY[job_type] = JobPlugin()
    return _REGISTRY[job_type]


def register_params_renderer(job_type: JobType):
    """Decorator. fn(job_type, job_model, is_frozen, save_handler, *, ui_mgr, backend)"""

    def decorator(fn: Callable) -> Callable:
        _ensure(job_type).render_params = fn
        return fn

    return decorator


def register_extra_tab(job_type: JobType, *, key: str, label: str, icon: str = "dashboard"):
    """Decorator. fn(job_type, instance_id, job_model, backend, ui_mgr)"""

    def decorator(fn: Callable) -> Callable:
        _ensure(job_type).extra_tabs.append(ExtraTab(key=key, label=label, icon=icon, render=fn))
        return fn

    return decorator


def register_full_panel_renderer(job_type: JobType):
    """Decorator for interactive jobs that replace the entire tab chrome with a custom panel.
    fn(job_type, instance_id, job_model, backend, ui_mgr, save_handler)"""

    def decorator(fn: Callable) -> Callable:
        _ensure(job_type).render_full_panel = fn
        return fn

    return decorator


def get_full_panel_renderer(job_type: JobType) -> Callable | None:
    plugin = _REGISTRY.get(job_type)
    return plugin.render_full_panel if plugin else None


def get_params_renderer(job_type: JobType) -> Callable | None:
    plugin = _REGISTRY.get(job_type)
    return plugin.render_params if plugin else None


def get_extra_tabs(job_type: JobType) -> list[ExtraTab]:
    plugin = _REGISTRY.get(job_type)
    return plugin.extra_tabs if plugin else []


# ---------------------------------------------------------------------------
# Auto-import plugin modules so their decorators execute at import time.
# The module list is derived from JobSpec.plugins — when you create a plugin
# file, list its basename on the job type's row in services/jobs/spec.py.
# ---------------------------------------------------------------------------
def _load_plugins():
    import importlib

    from services.jobs.spec import JOB_SPECS

    _modules = [f"ui.job_plugins.{name}" for name in dict.fromkeys(m for s in JOB_SPECS for m in s.plugins)]
    for mod in _modules:
        try:
            importlib.import_module(mod)
        except Exception as e:
            # Loud (warning + traceback). The previous behaviour was to log
            # ImportError at INFO and swallow other exceptions silently — that
            # turned a typo or missing dependency in a plugin file into a mute
            # fallback to the default renderer, which is a real debugging
            # nightmare. Any failure here means the user sees the wrong UI.
            logger.warning("Plugin module %s failed to load: %s: %s", mod, type(e).__name__, e)
            logger.exception("Plugin load traceback:")


_load_plugins()

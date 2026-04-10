"""
Job plugin registry.

Register a custom parameter renderer and/or extra tabs for any JobType.

    from ui.job_plugins import register_params_renderer, register_extra_tab

    @register_params_renderer(JobType.TEMPLATE_MATCH_PYTOM)
    def render_tm_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None):
        ...

    @register_extra_tab(JobType.TEMPLATE_MATCH_PYTOM, key="workbench", label="Workbench", icon="build")
    def render_tm_workbench(job_type, job_model, backend, ui_mgr):
        ...

Jobs without a registered plugin get the generic field-dump from default_renderer.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

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
    render_params: Optional[Callable] = None
    extra_tabs: List[ExtraTab] = field(default_factory=list)
    # Full-panel renderer replaces the entire tab chrome.
    # Signature: (job_type, instance_id, job_model, backend, ui_mgr, save_handler) -> None
    render_full_panel: Optional[Callable] = None


_REGISTRY: Dict[JobType, JobPlugin] = {}


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
    """Decorator. fn(job_type, job_model, backend, ui_mgr)"""

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


def get_full_panel_renderer(job_type: JobType) -> Optional[Callable]:
    plugin = _REGISTRY.get(job_type)
    return plugin.render_full_panel if plugin else None


def get_params_renderer(job_type: JobType) -> Optional[Callable]:
    plugin = _REGISTRY.get(job_type)
    return plugin.render_params if plugin else None


def get_extra_tabs(job_type: JobType) -> List[ExtraTab]:
    plugin = _REGISTRY.get(job_type)
    return plugin.extra_tabs if plugin else []


# ---------------------------------------------------------------------------
# Auto-import plugin modules so their decorators execute at import time.
# Add new plugin filenames here when you create them.
# ---------------------------------------------------------------------------
def _load_plugins():
    import importlib

    _modules = [
        "ui.job_plugins.fs_motion_and_ctf",
        "ui.job_plugins.template_match",
        "ui.job_plugins.subtomo_extraction",
        "ui.job_plugins.candidate_extract",
        "ui.job_plugins.tilt_filter",
        "ui.job_plugins.ts_reconstruct",
        "ui.job_plugins.array_tasks",
    ]
    for mod in _modules:
        try:
            importlib.import_module(mod)
        except ImportError as e:
            logger.info("Skipped %s: %s", mod, e)


_load_plugins()

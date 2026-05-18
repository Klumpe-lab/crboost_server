"""
Per-tab UI refresher.

Reconciliation (reading exit markers, patching default_pipeline.star,
clearing pipeline_active on failure) is owned by the server-side
PipelineMonitor (services/.../pipeline_monitor.py). This class is purely
a UI tick: every 3 s it asks the roster to re-render from the in-memory
state and watches for `pipeline_active` transitions so it can flip the
per-tab `ui_mgr.is_running` flag, stop the spinner, and notify the user
when the pipeline finishes.

Multi-tab safety: two browser tabs on the same project share one
ProjectState (path-keyed registry). Both their refreshers observe the
same `pipeline_active` flag, so they both react in step when the monitor
flips it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from nicegui import ui

from services.project_state import get_project_state_for

if TYPE_CHECKING:
    from ui.pipeline_builder.pipeline_builder_panel import PipelineBuilderPanel

logger = logging.getLogger(__name__)


class StatusPoller:
    def __init__(self, panel: "PipelineBuilderPanel"):
        self.panel = panel
        # `None` on first tick means "no prior observation"; first tick
        # only records, doesn't fire transitions.
        self._last_active: Optional[bool] = None

    async def check_and_update_statuses(self):
        """One UI tick. Reads in-memory state (already kept fresh by
        the server-side PipelineMonitor), refreshes the roster, and
        watches for pipeline-active transitions."""
        panel = self.panel
        project_path = panel.ui_mgr.project_path
        if not project_path:
            return

        # Roster reads in-memory job_model.execution_status — cheap.
        panel.roster.refresh()

        overview = await panel.backend.get_pipeline_overview(str(project_path))
        panel.roster.update_status_label(overview)

        state = get_project_state_for(project_path)
        # `pipeline_active` is the monitor's source of truth; OR with
        # is_active() so a freshly-started pipeline (state already True,
        # in-memory dict already populated) is recognized before its
        # first tick.
        current = bool(state.pipeline_active) or panel.backend.pipeline_runner.is_active(project_path)

        if self._last_active is None:
            self._last_active = current
            return

        if self._last_active and not current:
            # Pipeline just finished or was stopped — flip per-tab UI flag.
            self._last_active = current
            panel.ui_mgr.set_pipeline_running(False)
            self.stop_all_timers()
            try:
                if overview.get("failed", 0) > 0:
                    ui.notify(f"Pipeline finished with {overview['failed']} failed job(s).", type="warning")
                else:
                    ui.notify("Pipeline execution finished.", type="positive")
            except RuntimeError:
                # Client gone — fine; nothing to notify.
                pass
            panel.rebuild_pipeline_ui()
            return

        if not self._last_active and current:
            # Pipeline started in another tab or via restart-recovery.
            self._last_active = current
            panel.ui_mgr.set_pipeline_running(True)
            panel.rebuild_pipeline_ui()

    async def safe_status_check(self):
        try:
            await self.check_and_update_statuses()
        except Exception as e:
            logger.info("Status refresh failed: %s", e)

    def stop_all_timers(self):
        self.panel.ui_mgr.cleanup_all_timers()

    async def startup_sync(self):
        """Fires when a workspace tab mounts. Records the initial
        pipeline_active state and starts the 3-s UI refresher. We don't
        call sync_all_jobs here — the server monitor already keeps state
        fresh."""
        panel = self.panel
        if not panel.ui_mgr.project_path:
            return

        project_path = panel.ui_mgr.project_path
        state = get_project_state_for(project_path)
        active = bool(state.pipeline_active) or panel.backend.pipeline_runner.is_active(project_path)

        if active and not panel.ui_mgr.is_running:
            panel.ui_mgr.set_pipeline_running(True)
            panel.rebuild_pipeline_ui()
        elif not active and panel.ui_mgr.is_running:
            # Recovery: another tab finished the pipeline since we last looked.
            panel.ui_mgr.set_pipeline_running(False)
            panel.rebuild_pipeline_ui()

        self._last_active = active

        # Always run the UI refresher, even when the pipeline isn't
        # currently active — that way a pipeline that gets started in
        # another tab (or by restart-recovery) shows up here without
        # needing a manual refresh.
        if panel.ui_mgr.status_timer is None:
            panel.ui_mgr.status_timer = ui.timer(3.0, self.safe_status_check)

from typing import TYPE_CHECKING
from nicegui import ui
from services.models_base import JobStatus
from services.project_state import get_project_state_for

if TYPE_CHECKING:
    from ui.pipeline_builder.pipeline_builder_panel import PipelineBuilderPanel


class StatusPoller:
    def __init__(self, panel: "PipelineBuilderPanel"):
        self.panel = panel

    async def check_and_update_statuses(self):
        panel = self.panel
        project_path = panel.ui_mgr.project_path
        if not project_path:
            return

        sbatch_errors = panel.backend.pipeline_runner.get_sbatch_errors(project_path)
        if sbatch_errors:
            await panel.backend.pipeline_runner.stop_pipeline(project_path)
            await panel.backend.pipeline_runner.reset_submission_failure(project_path)
            await panel.backend.pipeline_runner.sync_all_jobs(str(project_path))
            panel.ui_mgr.set_pipeline_running(False)
            self.stop_all_timers()
            panel.rebuild_pipeline_ui()
            ui.notify(f"SLURM submission failed: {sbatch_errors[0]}", type="negative", timeout=10000)
            return

        await panel.backend.pipeline_runner.sync_all_jobs(str(project_path))

        if not panel.ui_mgr.is_running:
            return

        overview = await panel.backend.get_pipeline_overview(str(project_path))
        panel.roster.update_status_label(overview)

        all_done = (
            overview.get("total", 0) > 0
            and overview.get("running", 0) == 0
            and overview.get("scheduled", 0) == 0
            and (overview.get("completed", 0) > 0 or overview.get("failed", 0) > 0)
            and not panel.backend.pipeline_runner.is_active(project_path)
        )

        if not all_done:
            return

        panel.ui_mgr.set_pipeline_running(False)
        self.stop_all_timers()

        try:
            if overview.get("failed", 0) > 0:
                ui.notify(f"Pipeline finished with {overview['failed']} failed job(s).", type="warning")
            else:
                ui.notify("Pipeline execution finished.", type="positive")
        except RuntimeError:
            pass

        panel.rebuild_pipeline_ui()

    async def safe_status_check(self):
        try:
            await self.check_and_update_statuses()
        except Exception as e:
            print(f"[UI] Status check failed: {e}")

    def stop_all_timers(self):
        self.panel.ui_mgr.cleanup_all_timers()
        self.panel.roster.stop_spinner_timer()

    async def startup_sync(self):
        panel = self.panel
        if not panel.ui_mgr.project_path:
            return

        project_path = panel.ui_mgr.project_path
        await panel.backend.pipeline_runner.sync_all_jobs(str(project_path))

        state = get_project_state_for(project_path)
        any_running = any(m.execution_status == JobStatus.RUNNING for m in state.jobs.values())

        if (any_running or state.pipeline_active) and not panel.ui_mgr.is_running:
            panel.ui_mgr.set_pipeline_running(True)
            panel.rebuild_pipeline_ui()
        elif panel.ui_mgr.is_running:
            panel.ui_mgr.status_timer = ui.timer(3.0, self.safe_status_check)
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Callable, List, Optional

from nicegui import ui

from backend import CryoBoostBackend
from services.models_base import JobStatus
from services.project_state import JobType, get_state_service

from ui.pipeline_builder.pipeline_constants import PHASE_JOBS, PHASE_PARTICLES, next_instance_id
from ui.pipeline_builder.pipeline_roster import RosterWidget
from ui.pipeline_builder.status_poller import StatusPoller
from ui.ui_state import get_ui_state_manager, get_job_display_name, instance_id_to_job_type
from ui.pipeline_builder.job_tab_component import render_job_tab

logger = logging.getLogger(__name__)


class PipelineBuilderPanel:
    def __init__(
        self,
        backend: CryoBoostBackend,
        callbacks: Dict[str, Callable],
        primary_sidebar=None,
        roster_panel=None,
        toggle_workbench: Optional[Callable] = None,
        ensure_pipeline_mode: Optional[Callable] = None,
    ):
        self.backend = backend
        self.callbacks = callbacks
        self.primary_sidebar = primary_sidebar
        self.roster_panel = roster_panel
        self.toggle_workbench = toggle_workbench
        self.ensure_pipeline_mode = ensure_pipeline_mode

        self.ui_mgr = get_ui_state_manager()
        self.state_service = get_state_service()

        self._job_content_containers: Dict[str, object] = {}
        self._content_wrapper_ref: Dict[str, object] = {}

        self.roster = RosterWidget(self)
        self.poller = StatusPoller(self)

    def build(self):
        self.ui_mgr.cleanup_all_timers()
        self.roster.build_sidebar()
        self.roster.refresh()

        self.ui_mgr.set_rebuild_callback(self.rebuild_pipeline_ui)
        self.callbacks["rebuild_pipeline_ui"] = self.rebuild_pipeline_ui
        self.callbacks["stop_all_timers"] = self.poller.stop_all_timers
        self.callbacks["check_and_update_statuses"] = self.poller.check_and_update_statuses
        self.callbacks["enable_run_button"] = self.roster.rebuild_run_slot
        self.callbacks["add_job_to_pipeline"] = lambda jt: self.add_instance_to_pipeline(jt)
        self.callbacks["add_instance_to_pipeline"] = self.add_instance_to_pipeline
        self.callbacks["remove_instance_from_pipeline"] = self.remove_instance_from_pipeline
        self.callbacks["invalidate_tm_tabs"] = self.invalidate_tm_tabs

        self.rebuild_pipeline_ui()

        if self.ui_mgr.is_project_created:
            ui.timer(0.3, self.poller.startup_sync, once=True)
        elif self.ui_mgr.is_running:
            ui.timer(0.2, self.poller.safe_status_check, once=True)

    # ── Species gate ──────────────────────────────────────────────────────────

    async def prompt_species_and_add(self, job_type: JobType):
        if self.ui_mgr.is_running:
            return

        if job_type not in PHASE_JOBS[PHASE_PARTICLES]:
            self.add_instance_to_pipeline(job_type)
            return

        project_path = self.ui_mgr.project_path
        if not project_path:
            return

        from services.project_state import get_project_state_for

        state = get_project_state_for(project_path)

        if not state.species_registry:
            ui.notify(
                "Register at least one particle species in the Template Workbench first.", type="warning", timeout=4000
            )
            return

        chosen = {"id": state.species_registry[0].id}

        with ui.dialog() as dialog, ui.card().style("min-width: 300px; padding: 16px;"):
            ui.label(f"Add {get_job_display_name(job_type)}").classes("text-base font-bold text-gray-800 mb-3")
            options = {s.id: s.name for s in state.species_registry}
            sel = (
                ui.select(options=options, value=chosen["id"], label="Particle species")
                .props("outlined dense")
                .classes("w-full")
            )

            def _on_change(e):
                chosen["id"] = e.value

            sel.on_value_change(_on_change)

            with ui.row().classes("w-full justify-end gap-2 mt-4"):
                ui.button("Cancel", on_click=lambda: dialog.submit(False)).props("flat dense no-caps")
                ui.button("Add", on_click=lambda: dialog.submit(True)).props("dense no-caps").style(
                    "background: #3b82f6; color: white; padding: 4px 16px; border-radius: 3px;"
                )

        confirmed = await dialog
        if not confirmed:
            return
        self.add_instance_to_pipeline(job_type, species_id=chosen["id"])

    # ── Tab management ────────────────────────────────────────────────────────

    def _ensure_job_rendered(self, instance_id: str):
        if instance_id in self._job_content_containers:
            return
        try:
            job_type = instance_id_to_job_type(instance_id)
        except ValueError:
            logger.info("Unknown job type for instance_id '%s'", instance_id)
            return

        wrapper = self._content_wrapper_ref.get("el")
        if wrapper is None:
            return

        with wrapper:
            container = ui.column().classes("w-full overflow-hidden").style("flex: 1 1 0%; min-height: 0;")
            container.set_visibility(False)
            self._job_content_containers[instance_id] = container
            with container:
                render_job_tab(
                    job_type=job_type,
                    instance_id=instance_id,
                    backend=self.backend,
                    ui_mgr=self.ui_mgr,
                    callbacks={
                        **self.callbacks,
                        "check_and_update_statuses": self.poller.check_and_update_statuses,
                        "rebuild_pipeline_ui": self.rebuild_pipeline_ui,
                        "remove_instance_from_pipeline": self.remove_instance_from_pipeline,
                    },
                )

    def invalidate_tm_tabs(self):
        tm_prefix = JobType.TEMPLATE_MATCH_PYTOM.value
        stale = [iid for iid in list(self._job_content_containers.keys()) if iid.split("__")[0] == tm_prefix]
        for iid in stale:
            container = self._job_content_containers.pop(iid, None)
            if container:
                try:
                    container.delete()
                except Exception:
                    pass

        active = self.ui_mgr.active_instance_id
        if active and active.split("__")[0] == tm_prefix:
            self._ensure_job_rendered(active)
            for iid, c in self._job_content_containers.items():
                c.set_visibility(iid == active)

    def switch_tab(self, instance_id: str):
        if self.ensure_pipeline_mode:
            self.ensure_pipeline_mode()
        self.ui_mgr.set_active_instance(instance_id)
        self._ensure_job_rendered(instance_id)
        for iid, c in self._job_content_containers.items():
            c.set_visibility(iid == instance_id)
        self.roster.refresh()

    def switch_to_job_subsection(self, instance_id: str, tab_key: str):
        """Switch to a specific job AND a specific subsection tab."""
        from ui.pipeline_builder.job_tab_component import _handle_tab_switch

        # Set the desired tab BEFORE ensuring the job is rendered,
        # so render_job_tab picks it up as the initial active tab.
        self.ui_mgr.set_job_monitor_tab(instance_id, tab_key, user_initiated=True)

        # Make the job active and visible (inline switch_tab without the
        # roster refresh so the heavy sidebar rebuild doesn't interleave
        # with the content update below).
        if self.ensure_pipeline_mode:
            self.ensure_pipeline_mode()
        self.ui_mgr.set_active_instance(instance_id)
        self._ensure_job_rendered(instance_id)
        for iid, c in self._job_content_containers.items():
            c.set_visibility(iid == instance_id)

        # Force a content re-render for the requested subsection.
        job_type = instance_id_to_job_type(instance_id)
        _handle_tab_switch(job_type, instance_id, tab_key, self.backend, self.ui_mgr, self.callbacks)

        # Refresh the roster *after* the content is updated so the
        # sidebar highlight reflects the new active job/tab.
        self.roster.refresh()

    # ── Job/instance management ───────────────────────────────────────────────

    def add_instance_to_pipeline(
        self, job_type: JobType, instance_id: Optional[str] = None, species_id: Optional[str] = None
    ):
        if self.ui_mgr.is_running:
            return

        state = self.state_service.state

        # Interactive jobs are singletons — if one already exists, just switch to it.
        if instance_id is None:
            existing = self._find_existing_interactive(job_type, state)
            if existing:
                if existing not in self.ui_mgr.selected_jobs:
                    self.ui_mgr.add_instance(existing, job_type)
                    self.rebuild_pipeline_ui()
                self.ui_mgr.set_active_instance(existing)
                self.rebuild_pipeline_ui()
                return

        if instance_id is None:
            instance_id = next_instance_id(job_type, self.ui_mgr.selected_jobs, list(state.jobs.keys()))

        # Auto-add prerequisite jobs that this job type depends on.
        # e.g. alignment requires tsImport to exist in the pipeline.
        self._ensure_prerequisites(job_type, state)

        if not self.ui_mgr.add_instance(instance_id, job_type):
            return

        if instance_id not in state.jobs:
            template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
            star = template_base / job_type.value / "job.star"
            state.ensure_job_initialized(
                job_type, instance_id=instance_id, template_path=star if star.exists() else None
            )
            # Restore saved labels for interactive filter jobs.
            self._restore_interactive_state(job_type, instance_id, state)

        if species_id is not None:
            job_model = state.jobs.get(instance_id)
            if job_model is not None:
                job_model.species_id = species_id
                if job_type == JobType.TEMPLATE_MATCH_PYTOM and self.ui_mgr.project_path:
                    from services.project_state import get_project_state_for

                    p_state = get_project_state_for(self.ui_mgr.project_path)
                    sp = p_state.get_species(species_id)
                    if sp:
                        job_model.template_path = sp.template_path or ""
                        job_model.mask_path = sp.mask_path or ""

        if self.ui_mgr.is_project_created:
            asyncio.create_task(self.state_service.save_project())

        self.ui_mgr.set_active_instance(instance_id)
        self.rebuild_pipeline_ui()

    def _cleanup_stale_overrides_for_instance(self, instance_id: str):
        state = self.state_service.state
        removed_model = state.jobs.get(instance_id)
        job_type_str = instance_id.split("__")[0]

        refs_to_clean: set = set()
        if removed_model:
            relion_name = getattr(removed_model, "relion_job_name", None)
            if relion_name:
                refs_to_clean.add(f"{job_type_str}:{relion_name.rstrip('/')}")
        refs_to_clean.add(f"{job_type_str}:External/pending_{instance_id}")

        for _, job_model in state.jobs.items():
            overrides = getattr(job_model, "source_overrides", None)
            if not overrides:
                continue
            stale = [k for k, v in overrides.items() if v in refs_to_clean]
            for k in stale:
                del overrides[k]

    def remove_instance_from_pipeline(self, instance_id: str):
        if self.ui_mgr.is_running:
            return
        if not self.ui_mgr.remove_instance(instance_id):
            return
        self._cleanup_stale_overrides_for_instance(instance_id)
        self._job_content_containers.pop(instance_id, None)

        state = self.state_service.state
        job_model = state.jobs.get(instance_id)
        if job_model and job_model.execution_status != JobStatus.SUCCEEDED:
            del state.jobs[instance_id]
            state.job_path_mapping.pop(instance_id, None)

        if self.ui_mgr.is_project_created:
            asyncio.create_task(self.state_service.save_project())
        self.rebuild_pipeline_ui()

    # Job types that require a prerequisite job to exist in the pipeline.
    # When adding the key job type, the value job type is auto-added if missing.
    _PREREQUISITES: Dict[JobType, JobType] = {JobType.TS_ALIGNMENT: JobType.TS_IMPORT}

    def _ensure_prerequisites(self, job_type: JobType, state):
        """Auto-add prerequisite jobs that this job type depends on."""
        prereq = self._PREREQUISITES.get(job_type)
        if prereq is None:
            return

        # Check if the prerequisite already exists in the pipeline
        prereq_id = prereq.value
        if prereq_id in state.jobs:
            # Also ensure it's in the UI selected jobs list
            if prereq_id not in self.ui_mgr.selected_jobs:
                self.ui_mgr.add_instance(prereq_id, prereq)
            return

        # Auto-add the prerequisite
        self.ui_mgr.add_instance(prereq_id, prereq)
        template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        star = template_base / prereq.value / "job.star"
        state.ensure_job_initialized(prereq, instance_id=prereq_id, template_path=star if star.exists() else None)

    @staticmethod
    def _find_existing_interactive(job_type: JobType, state) -> Optional[str]:
        """Return existing instance_id for a singleton interactive job, or None."""
        from services.jobs import jobtype_paramclass

        param_cls = jobtype_paramclass().get(job_type)
        if not param_cls or not getattr(param_cls, "IS_INTERACTIVE", False):
            return None
        for iid, jm in state.jobs.items():
            if jm.job_type == job_type:
                return iid
        return None

    @staticmethod
    def _restore_interactive_state(job_type: JobType, instance_id: str, state):
        """Restore persisted labels/state when re-creating an interactive job."""
        if job_type != JobType.TILT_FILTER:
            return
        job_model = state.jobs.get(instance_id)
        if not job_model:
            return
        if state.tilt_filter_labels:
            job_model.tilt_labels = dict(state.tilt_filter_labels)
            job_model.execution_status = JobStatus.SUCCEEDED
            # Restore output paths if the filtered star already exists on disk
            filtered_p = state.project_path / "TiltFilter" / "tiltseries_filtered.star"
            if filtered_p.exists():
                job_model.paths["output_star"] = str(filtered_p)

    # ── Full rebuild ──────────────────────────────────────────────────────────

    def rebuild_pipeline_ui(self):
        self._job_content_containers.clear()
        self._content_wrapper_ref.pop("el", None)

        self.roster.rebuild_run_slot()
        self.roster.refresh()

        tabs_container = self.ui_mgr.panel_refs.job_tabs_container
        if tabs_container is None:
            return
        tabs_container.clear()

        if not self.ui_mgr.is_project_created:
            with tabs_container:
                with ui.column().classes("w-full h-full items-center justify-center gap-3"):
                    ui.icon("create_new_folder", size="44px").classes("text-gray-300")
                    ui.label("Create a project to begin.").classes("text-sm text-gray-400")
            return

        selected = self.ui_mgr.selected_jobs
        if not selected:
            with tabs_container:
                ui.label("Select jobs from the left panel.").classes("text-xs text-gray-400 italic p-8")
            return

        if self.ui_mgr.active_instance_id not in selected:
            self.ui_mgr.set_active_instance(selected[0])

        with tabs_container:
            wrapper = ui.element("div").style(
                "display: flex; flex-direction: column; width: 100%; flex: 1 1 0%; min-height: 0; overflow: hidden;"
            )
            self._content_wrapper_ref["el"] = wrapper

        active = self.ui_mgr.active_instance_id
        if active:
            self._ensure_job_rendered(active)
            self._job_content_containers[active].set_visibility(True)

        if self.ui_mgr.is_running:
            self.roster.start_spinner_timer()
            try:
                self.ui_mgr.status_timer = ui.timer(3.0, self.poller.safe_status_check)
            except RuntimeError:
                pass

    # ── Run / Stop ────────────────────────────────────────────────────────────

    async def handle_run_pipeline(self):
        if not self.ui_mgr.is_project_created:
            ui.notify("Create a project first", type="warning")
            return

        await self.state_service.save_project(force=True)

        try:
            result = await self.backend.start_pipeline(
                project_path=str(self.ui_mgr.project_path),
                scheme_name=f"run_{datetime.now().strftime('%H%M%S')}",
                selected_jobs=self.ui_mgr.selected_jobs,
                required_paths=[],
            )
            if result.get("already_complete"):
                ui.notify("All selected jobs already completed.", type="info")
                return
            if result.get("success"):
                self.ui_mgr.set_pipeline_running(True)
                ui.notify(f"Pipeline started (PID: {result.get('pid')})", type="positive")
                self.ui_mgr.status_timer = ui.timer(3.0, self.poller.safe_status_check)
                self.rebuild_pipeline_ui()
            else:
                ui.notify(f"Failed to start: {result.get('error')}", type="negative")
        except Exception as e:
            ui.notify(f"Error: {e}", type="negative")

    async def handle_stop_pipeline(self):
        slurm_result = await self.backend.slurm_service.get_user_slurm_jobs(force_refresh=True)
        running_slurm = [j for j in slurm_result.get("jobs", []) if j["state"] in ("RUNNING", "PENDING")]

        with ui.dialog() as dialog, ui.card().style("min-width: 360px; padding: 16px;"):
            ui.label("Stop Pipeline?").classes("text-base font-bold text-gray-800")
            if running_slurm:
                ui.label(f"{len(running_slurm)} SLURM job(s) will be cancelled:").classes("text-sm text-gray-600 mt-2")
                for j in running_slurm:
                    ui.label(f"[{j['job_id']}]  {j['name']}  ({j['state']})").classes(
                        "text-xs font-mono text-gray-500 ml-2"
                    )
            else:
                ui.label("No active SLURM jobs found.").classes("text-sm text-gray-500 mt-2")
            ui.label("Running and queued jobs will be marked Failed.").classes("text-xs text-amber-600 mt-3")
            with ui.row().classes("mt-4 gap-2 justify-end w-full"):
                ui.button("Cancel", on_click=lambda: dialog.submit(False)).props("flat dense no-caps")
                ui.button("Stop Pipeline", on_click=lambda: dialog.submit(True)).props("dense no-caps").style(
                    "background: #ef4444; color: white; padding: 4px 16px; border-radius: 3px;"
                )

        confirmed = await dialog
        if not confirmed:
            return

        self.poller.stop_all_timers()
        self.ui_mgr.set_pipeline_running(False)
        slurm_ids = [j["job_id"] for j in running_slurm]
        result = await self.backend.pipeline_runner.stop_and_cleanup(self.ui_mgr.project_path, slurm_ids)
        await self.backend.pipeline_runner.sync_all_jobs(str(self.ui_mgr.project_path))
        self.rebuild_pipeline_ui()

        if result.get("success"):
            ui.notify("Pipeline stopped.", type="warning", timeout=4000)
        else:
            ui.notify(f"Stopped (with warnings: {'; '.join(result.get('errors', []))})", type="warning", timeout=6000)


def build_pipeline_builder_panel(
    backend: CryoBoostBackend,
    callbacks: Dict[str, Callable],
    primary_sidebar=None,
    roster_panel=None,
    toggle_workbench: Optional[Callable] = None,
    ensure_pipeline_mode: Optional[Callable] = None,
) -> None:
    panel = PipelineBuilderPanel(
        backend=backend,
        callbacks=callbacks,
        primary_sidebar=primary_sidebar,
        roster_panel=roster_panel,
        toggle_workbench=toggle_workbench,
        ensure_pipeline_mode=ensure_pipeline_mode,
    )

    # Must be created in the current NiceGUI rendering context before
    # panel.build() is called, since rebuild_pipeline_ui writes into it.
    tabs_container = ui.element("div").style(
        "display: flex; flex-direction: column; width: 100%; flex: 1 1 0%; min-height: 0; overflow: hidden;"
    )
    panel.ui_mgr.panel_refs.job_tabs_container = tabs_container

    panel.build()

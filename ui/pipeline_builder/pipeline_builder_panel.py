import asyncio
import logging
from datetime import datetime
from collections.abc import Callable

from nicegui import ui

from backend import CryoBoostBackend
from services.jobs.spec import JOB_SPEC_BY_TYPE
from services.models_base import JobStatus
from services.project_state import JobType

from ui.components.buttons import house_button
from ui.components.reactive import SingleFlight
from ui.current_project import current_project_state
from ui.pipeline_builder.pipeline_constants import PHASE_JOBS, PHASE_PARTICLES, missing_deps, next_instance_id
from ui.pipeline_builder.pipeline_roster import RosterWidget
from ui.pipeline_builder.status_poller import StatusPoller
from ui.routing import View
from services.models_base import InstanceId, instance_id_to_job_type
from ui.ui_state import get_ui_state_manager, get_job_display_name
from ui.pipeline_builder.job_tab_component import render_job_tab

logger = logging.getLogger(__name__)


def _safe_notify(message: str, *, type: str = "info") -> None:
    """ui.notify that swallows the "client has been deleted" RuntimeError.
    Happens when the browser tab closes / refreshes while a long-running
    backend call (e.g. relion container init) is still awaiting — there's
    no client left to notify, so we just log it."""
    try:
        ui.notify(message, type=type)
    except RuntimeError as e:
        logger.info("notify dropped (client gone): %s [%s]", message, e)


class PipelineBuilderPanel:
    def __init__(
        self,
        backend: CryoBoostBackend,
        callbacks: dict[str, Callable],
        primary_sidebar=None,
        roster_panel=None,
        toggle_workbench: Callable | None = None,
        ensure_pipeline_mode: Callable | None = None,
        toggle_journey: Callable | None = None,
        toggle_gallery: Callable | None = None,
        toggle_protocols: Callable | None = None,
    ):
        self.backend = backend
        self.callbacks = callbacks
        self.primary_sidebar = primary_sidebar
        self.roster_panel = roster_panel
        self.toggle_workbench = toggle_workbench
        self.ensure_pipeline_mode = ensure_pipeline_mode
        self.toggle_journey = toggle_journey
        self.toggle_gallery = toggle_gallery
        self.toggle_protocols = toggle_protocols

        self.ui_mgr = get_ui_state_manager()

        self._job_content_containers: dict[str, object] = {}
        self._content_wrapper_ref: dict[str, object] = {}

        self.roster = RosterWidget(self)
        self.poller = StatusPoller(self)
        # Guards async handlers that own a dialog (and other re-entrant work)
        # against rapid/double clicks. See ui/components/reactive.py.
        self.flight = SingleFlight()

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
        self.callbacks["set_active_mode"] = self.roster.set_active_mode
        # Species page (roadmap 10 S4): open a job in the pipeline view / add one for a species.
        self.callbacks["open_job"] = self.switch_tab
        self.callbacks["open_job_subsection"] = self.switch_to_job_subsection
        self.callbacks["add_instance_for_species"] = self.add_instance_for_species

        self.rebuild_pipeline_ui()

        if self.ui_mgr.is_project_created:
            ui.timer(0.3, self.poller.startup_sync, once=True)
        elif self.ui_mgr.is_running:
            ui.timer(0.2, self.poller.safe_status_check, once=True)

    # ── Curation session ──────────────────────────────────────────────────────

    # ── Species gate ──────────────────────────────────────────────────────────

    async def prompt_species_and_add(self, job_type: JobType):
        # SingleFlight: if a dialog for this job_type is already open, a
        # second click drops silently instead of stacking another dialog.
        # Required because the trigger button can be destroyed and rebuilt
        # mid-click by an unrelated refresh, so the user may click "add"
        # several times before one click lands.
        async with self.flight(f"species_dialog:{job_type.value}") as acquired:
            if not acquired:
                return

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
                    "No particle species yet — use “+” on the PARTICLES header to create one "
                    "(no template needed), or create one in the Particles registry.",
                    type="warning",
                    timeout=4000,
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
                    house_button("Cancel", lambda: dialog.submit(False))
                    house_button("Add", lambda: dialog.submit(True), kind="accent")

            confirmed = await dialog
            if not confirmed:
                return
            self.add_instance_to_pipeline(job_type, species_id=chosen["id"])

    async def add_instance_for_species(self, job_type: JobType, species_id: str) -> None:
        """One-click add from the Species page's Jobs tab: the species is known, so no
        chooser dialog — straight to `add_instance_to_pipeline(job_type, species_id=)`.
        SingleFlight-guarded like `prompt_species_and_add`; the roster's missing-
        dependency warning is repeated here since that surface isn't visible."""
        async with self.flight(f"add_for_species:{job_type.value}:{species_id}") as acquired:
            if not acquired:
                return
            if self.ui_mgr.is_running:
                ui.notify("Pipeline is running — stop it before adding jobs.", type="warning")
                return
            missing = missing_deps(job_type, set(self.ui_mgr.selected_jobs))
            if missing:
                ui.notify(
                    f"{get_job_display_name(job_type)} typically requires: "
                    + ", ".join(get_job_display_name(d) for d in missing),
                    type="warning",
                    timeout=3000,
                )
            self.add_instance_to_pipeline(job_type, species_id=species_id)
            ui.notify(f"Added {get_job_display_name(job_type)} for species '{species_id}'", type="positive")

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
        tm_type = JobType.TEMPLATE_MATCH_PYTOM
        stale = [iid for iid in list(self._job_content_containers.keys()) if InstanceId.matches(iid, tm_type)]
        for iid in stale:
            container = self._job_content_containers.pop(iid, None)
            if container:
                try:
                    container.delete()
                except Exception:
                    pass

        active = self.ui_mgr.active_instance_id
        if active and InstanceId.matches(active, tm_type):
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
        set_url = self.callbacks.get("set_url")
        if set_url:
            set_url(View.JOB, instance_id, self.ui_mgr.get_job_ui_state(instance_id).active_monitor_tab)
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
        self, job_type: JobType, instance_id: str | None = None, species_id: str | None = None
    ):
        if self.ui_mgr.is_running:
            return

        state = current_project_state()

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

        # A NEW per-particle job must name its species. Unattributable ones are not a
        # display wart: `species_render_plan` still emits them (parity contract), so they
        # draw a full Journey species tab, while the Species page's universe IS the
        # registry — which means every action 11-S3 moved there is unreachable for exactly
        # those instances. `prompt_species_and_add` asks for the species, but it is only
        # ONE caller; the invariant belongs here, where the two registered callbacks
        # (`add_job_to_pipeline`, `add_instance_to_pipeline`) also arrive. Re-selecting an
        # EXISTING instance is untouched — it already has one. `_ensure_prerequisites` does
        # NOT pass through here, so it would bypass this; today it cannot produce a particle
        # job, because no PARTICLES-phase spec declares a `prerequisite` (all point at
        # tsImport). Give one a particle prerequisite and it needs the same gate.
        if job_type in PHASE_JOBS[PHASE_PARTICLES] and species_id is None and instance_id not in state.jobs:
            ui.notify(
                f"{get_job_display_name(job_type)} needs a particle species — add it from the "
                "PARTICLES header “+” or the Particles registry's Jobs tab, which ask for one.",
                type="warning",
                timeout=4000,
            )
            return

        # Auto-add prerequisite jobs that this job type depends on.
        # e.g. alignment requires tsImport to exist in the pipeline.
        self._ensure_prerequisites(job_type, state)

        if not self.ui_mgr.add_instance(instance_id, job_type):
            return

        if instance_id not in state.jobs:
            state.ensure_job_initialized(job_type, instance_id=instance_id)
            # Restore saved labels for interactive filter jobs.
            self._restore_interactive_state(job_type, instance_id, state)

        if species_id is not None:
            job_model = state.jobs.get(instance_id)
            if job_model is not None:
                job_model.species_id = species_id
                if self.ui_mgr.project_path:
                    from services.project_state import get_project_state_for
                    from services.templating.template_metadata import (
                        get_effective_mask_path,
                        get_effective_template_path,
                    )

                    p_state = get_project_state_for(self.ui_mgr.project_path)
                    sp = p_state.get_species(species_id)
                    if sp:
                        # Default new TM jobs to the species's currently
                        # selected template + mask + symmetry. The v3
                        # helpers resolve template/mask via
                        # species.selected_*_id; symmetry is a top-level
                        # field on Species (defaults "C1").
                        #
                        # Previously this excluded symmetry — the rationale
                        # was that PyTOM's driver silently dropped non-Cn
                        # values, so a species declared as I1 would have
                        # silently run C1 anyway. That's no longer true:
                        # drivers/template_match_pytom.py now generates an
                        # asymmetric-unit angle list for D/T/O/I via
                        # services.templating.angle_lists and passes it as
                        # --angular-search <file>. With the driver honoring
                        # the value, inheriting from the species is the
                        # principle-of-least-surprise default; the user can
                        # still override in the TM-job dropdown.
                        if job_type == JobType.TEMPLATE_MATCH_PYTOM:
                            job_model.template_path = get_effective_template_path(sp)
                            job_model.mask_path = get_effective_mask_path(sp)
                            sp_sym = getattr(sp, "symmetry", None)
                            if sp_sym:
                                job_model.symmetry = sp_sym
                        # Default new candidate-extract jobs to the species's
                        # particle diameter. Job param defaults to 200 Å so
                        # this only overrides when species.diameter_ang is set.
                        elif job_type == JobType.TEMPLATE_EXTRACT_PYTOM:
                            if getattr(sp, "diameter_ang", None):
                                job_model.particle_diameter_ang = float(sp.diameter_ang)
                        # Extraction geometry the user committed once for this species
                        # (Species page → Overview, or the first per-list extract). Only
                        # when SET: the job's own box/bin/crop defaults are a legitimate
                        # job default, a species answer is not one to invent. Same
                        # snapshot-at-creation semantics as the two branches above —
                        # nothing propagates into existing jobs.
                        elif job_type == JobType.SUBTOMO_EXTRACTION:
                            ep = getattr(sp, "extraction_params", None)
                            if ep is not None:
                                job_model.box_size = ep.box_size
                                job_model.binning = ep.binning
                                job_model.crop_size = ep.crop_size

        # If a merge is active, wire this new consumer's input_optimisation slot to the
        # active merge's synthetic `mergedSources` producer (source_overrides key) so the
        # user doesn't have to configure the override by hand. No-ops when there is no
        # active merged optset, which is every project that has never merged.
        from services.aggregation.extraction import apply_aggregation_overrides

        apply_aggregation_overrides(state)

        if self.ui_mgr.is_project_created:
            asyncio.create_task(self.backend.save_project(self.ui_mgr.project_path))

        self.ui_mgr.set_active_instance(instance_id)
        self.rebuild_pipeline_ui()

    def _cleanup_stale_overrides_for_instance(self, instance_id: str):
        state = current_project_state()
        removed_model = state.jobs.get(instance_id)
        job_type_str = InstanceId.split(instance_id)[0]

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
        """Drop an instance from the pipeline and PERSIST that.

        The save is `force=True` and the state is marked dirty, both on purpose. A
        deletion mutates `jobs` / `job_path_mapping` / `pipeline_order` / other jobs'
        `source_overrides` directly, and none of those writes go through the USER_PARAMS
        setter that maintains `is_dirty` — `ensure_job_initialized` only calls
        `update_modified()`, which merely stamps `modified_at` (see `merge_card.persist`,
        which documents the same trap). A plain `save_project()` is gated on
        `force or state.is_dirty` (`StateService.save_project`), so before this the delete
        lived in memory only: the job vanished from the roster and came straight back on
        the next server restart, because `project_params.json` had never been rewritten.
        An ADD survived that only by accident — editing any parameter afterwards marks the
        state dirty and writes the whole thing out, the new job included.

        The state is resolved by EXPLICIT path when we have one, not `current_project_state()`:
        the save targets `ui_mgr.project_path`, and deleting out of one state while saving
        another is exactly how a deletion goes missing.
        """
        if self.ui_mgr.is_running:
            return
        if not self.ui_mgr.remove_instance(instance_id):
            return
        self._cleanup_stale_overrides_for_instance(instance_id)
        self._job_content_containers.pop(instance_id, None)

        project_path = self.ui_mgr.project_path
        if project_path is not None:
            from services.project_state import get_project_state_for

            state = get_project_state_for(project_path)
        else:
            state = current_project_state()

        job_model = state.jobs.get(instance_id)
        if job_model and job_model.execution_status != JobStatus.SUCCEEDED:
            del state.jobs[instance_id]
            state.job_path_mapping.pop(instance_id, None)
            # Persisted run membership must not keep naming a job that no longer exists.
            # Load filters it out (`project_state.py` builds pipeline_order from the ids
            # still in `jobs`), so this is belt-and-braces — but it keeps the file honest
            # between the delete and the next load.
            state.pipeline_order = [iid for iid in state.pipeline_order if iid != instance_id]
        state.mark_dirty()

        if self.ui_mgr.is_project_created:
            asyncio.create_task(self.backend.save_project(project_path, force=True))
        self.rebuild_pipeline_ui()

    def _ensure_prerequisites(self, job_type: JobType, state):
        """Auto-add prerequisite jobs that this job type depends on (JobSpec.prerequisite)."""
        spec = JOB_SPEC_BY_TYPE.get(job_type)
        prereq = spec.prerequisite if spec else None
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
        state.ensure_job_initialized(prereq, instance_id=prereq_id)

    @staticmethod
    def _find_existing_interactive(job_type: JobType, state) -> str | None:
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
        """Restore persisted labels/state when re-creating an interactive job.
        Labels come from the registry's per-frame filter verdicts (the last
        filter run stamped them) — the ProjectState tilt_filter_labels mirror
        is gone (roadmap 02 stage 4)."""
        if job_type != JobType.TILT_FILTER:
            return
        job_model = state.jobs.get(instance_id)
        if not job_model:
            return
        from services.tilt_series import get_registry_for

        try:
            reg = get_registry_for(state.project_path)
        except Exception:
            return
        labels = {
            f.id: ("bad" if f.is_filtered_out else "good")
            for ts in reg.all_tilt_series()
            for f in ts.frames
            if f.is_filtered_out or f.filter_probability is not None
        }
        if labels:
            job_model.tilt_labels = labels
            # The registry stamps ARE the committed output (the filter produces no
            # files of its own; alignment applies the cut when it snapshots the
            # tomostars). Frames carrying a verdict therefore mean the user has
            # committed, so the restored job may claim SUCCEEDED. Probability-only
            # stamps come from a DL pass the user never approved, so require at
            # least one actual drop before calling it committed.
            if any(f.is_filtered_out for ts in reg.all_tilt_series() for f in ts.frames):
                job_model.execution_status = JobStatus.SUCCEEDED

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
            try:
                self.ui_mgr.status_timer = ui.timer(3.0, self.poller.safe_status_check)
            except RuntimeError:
                pass

    # ── Run / Stop ────────────────────────────────────────────────────────────

    async def handle_run_pipeline(self):
        if not self.ui_mgr.is_project_created:
            _safe_notify("Create a project first", type="warning")
            return

        await self.backend.save_project(self.ui_mgr.project_path, force=True)

        try:
            result = await self.backend.start_pipeline(
                project_path=str(self.ui_mgr.project_path),
                scheme_name=f"run_{datetime.now().strftime('%H%M%S')}",
                selected_jobs=self.ui_mgr.selected_jobs,
                required_paths=[],
            )
            if result.get("already_complete"):
                _safe_notify("All selected jobs already completed.", type="info")
                return
            if result.get("success"):
                self.ui_mgr.set_pipeline_running(True)
                _safe_notify(f"Pipeline started (PID: {result.get('pid')})", type="positive")
                self.ui_mgr.status_timer = ui.timer(3.0, self.poller.safe_status_check)
                self.rebuild_pipeline_ui()
            else:
                logger.warning("start_pipeline failed: %s", result.get("error"))
                _safe_notify(f"Failed to start: {result.get('error')}", type="negative")
        except Exception as e:
            logger.exception("handle_run_pipeline error")
            _safe_notify(f"Error: {e}", type="negative")

    async def handle_stop_pipeline(self):
        project_path = self.ui_mgr.project_path
        project_prefix = str(project_path.resolve()) if project_path else None

        slurm_result = await self.backend.slurm_service.get_user_slurm_jobs(force_refresh=True)
        all_jobs = [j for j in slurm_result.get("jobs", []) if j["state"] in ("RUNNING", "PENDING")]

        # Filter to jobs that belong to THIS project (by stdout_path or work_dir)
        if project_prefix:
            running_slurm = [
                j
                for j in all_jobs
                if (j.get("stdout_path", "").startswith(project_prefix))
                or (j.get("work_dir", "").startswith(project_prefix))
            ]
        else:
            running_slurm = all_jobs

        with ui.dialog() as dialog, ui.card().style("min-width: 360px; padding: 16px;"):
            ui.label("Stop Pipeline?").classes("text-base font-bold text-gray-800")
            if project_path:
                ui.label(str(project_path)).classes("text-xs font-mono text-gray-400 mt-1")
            if running_slurm:
                ui.label(f"{len(running_slurm)} SLURM job(s) will be cancelled:").classes("text-sm text-gray-600 mt-2")
                for j in running_slurm:
                    ui.label(f"[{j['job_id']}]  {j['name']}  ({j['state']})").classes(
                        "text-xs font-mono text-gray-500 ml-2"
                    )
            else:
                ui.label("No active SLURM jobs found for this project.").classes("text-sm text-gray-500 mt-2")
            ui.label("Running and queued jobs will be marked Failed.").classes("text-xs text-amber-600 mt-3")
            with ui.row().classes("mt-4 gap-2 justify-end w-full"):
                house_button("Cancel", lambda: dialog.submit(False))
                house_button("Stop pipeline", lambda: dialog.submit(True), kind="accent")

        confirmed = await dialog
        if not confirmed:
            return

        self.poller.stop_all_timers()
        self.ui_mgr.set_pipeline_running(False)
        slurm_ids = [j["job_id"] for j in running_slurm]
        result = await self.backend.pipeline_runner.stop_and_cleanup(project_path, slurm_ids)
        await self.backend.pipeline_runner.sync_all_jobs(str(project_path))
        self.rebuild_pipeline_ui()

        if result.get("success"):
            ui.notify("Pipeline stopped.", type="warning", timeout=4000)
        else:
            ui.notify(f"Stopped (with warnings: {'; '.join(result.get('errors', []))})", type="warning", timeout=6000)


def build_pipeline_builder_panel(
    backend: CryoBoostBackend,
    callbacks: dict[str, Callable],
    primary_sidebar=None,
    roster_panel=None,
    toggle_workbench: Callable | None = None,
    ensure_pipeline_mode: Callable | None = None,
    toggle_journey: Callable | None = None,
    toggle_gallery: Callable | None = None,
    toggle_protocols: Callable | None = None,
) -> None:
    panel = PipelineBuilderPanel(
        backend=backend,
        callbacks=callbacks,
        primary_sidebar=primary_sidebar,
        roster_panel=roster_panel,
        toggle_workbench=toggle_workbench,
        ensure_pipeline_mode=ensure_pipeline_mode,
        toggle_journey=toggle_journey,
        toggle_gallery=toggle_gallery,
        toggle_protocols=toggle_protocols,
    )

    # The render-scoped self-heal of `apply_aggregation_overrides` was REMOVED here by
    # de-novo S6. It existed to retro-wire consumer jobs added before the merge hook was
    # wired, and it was safe only because `is_aggregation` kept it to a handful of projects.
    # With that flag deleted it would have become a mutator running on every workspace render
    # of every project — a behaviour change smuggled in as a cleanup. The wiring now happens
    # where the user acts: adding a consumer job (above), finishing a merge, and switching
    # the active merge (both in ui/aggregation/merge_card.py).

    # Must be created in the current NiceGUI rendering context before
    # panel.build() is called, since rebuild_pipeline_ui writes into it.
    tabs_container = ui.element("div").style(
        "display: flex; flex-direction: column; width: 100%; flex: 1 1 0%; min-height: 0; overflow: hidden;"
    )
    panel.ui_mgr.panel_refs.job_tabs_container = tabs_container

    panel.build()

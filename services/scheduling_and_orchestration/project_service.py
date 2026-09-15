import shutil
import logging
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any
import os
import glob
import asyncio
import json
from typing import TYPE_CHECKING

from services.configs.mdoc_service import acquisition_from_mdoc, get_mdoc_service, ts_name_from_mdoc
from services.configs.starfile_service import StarfileService
from services.stack_import import choose_source_layer, resolve_stack, split_stack
from services.models_base import InstanceId
from services.project_state import (
    JobType,
    get_state_service,
    jobtype_paramclass,
    set_project_state_for,
)
from services.result import err, ok
from services.scheduling_and_orchestration.pipeline_deletion_service import get_deletion_service

if TYPE_CHECKING:
    from backend import CryoBoostBackend

logger = logging.getLogger(__name__)


class DataImportService:
    def __init__(self):
        self.mdoc_service = get_mdoc_service()

    def _setup_project_data_sync(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str,
        selected_mdoc_paths: list[str] | None = None,
        progress_cb: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        """Synchronous core of data import — runs in thread pool to avoid blocking the event loop.

        Per mdoc, the source layer is re-derived from disk with the scan's rule (the
        overview is not passed down): movies are symlinked into `frames/` (looked
        for in the movies dir, then beside the mdoc), a SerialEM stack is split into one
        `.mrc` per tilt. The mdoc copy is `mdoc/<prefix><ts_name>.mdoc`. Returns
        `ok(layer=…, frame_extension=…, n_frames=…)`; `progress_cb` receives short status
        strings for the Create spinner.
        """
        try:
            frames_dir = project_dir / "frames"
            mdoc_dir = project_dir / "mdoc"
            frames_dir.mkdir(exist_ok=True, parents=True)
            mdoc_dir.mkdir(exist_ok=True, parents=True)

            if not movies_glob or not mdocs_glob:
                logger.info("Skipping data import - patterns are empty.")
                return ok(message="Skipped data import (empty patterns).")

            source_movie_dir = Path(movies_glob).parent

            # Import only selected mdocs when a selection is provided
            if selected_mdoc_paths is not None:
                mdoc_files = selected_mdoc_paths
            else:
                mdoc_files = glob.glob(mdocs_glob)

            if not mdoc_files:
                return err(f"No .mdoc files found with pattern: {mdocs_glob}")

            layers: Counter[str] = Counter()
            extensions: Counter[str] = Counter()
            n_frames = 0
            n_series = len(mdoc_files)
            for idx, mdoc_path_str in enumerate(sorted(mdoc_files)):
                mdoc_path = Path(mdoc_path_str)
                parsed_mdoc = self.mdoc_service.parse_mdoc_file(mdoc_path)
                if not parsed_mdoc["data"]:
                    logger.warning("Skipping %s: no [ZValue] sections", mdoc_path.name)
                    continue
                facts = acquisition_from_mdoc(parsed_mdoc)
                ts_name = ts_name_from_mdoc(mdoc_path.name)
                new_mdoc_path = mdoc_dir / f"{import_prefix}{ts_name}.mdoc"

                sources: list[tuple[dict, Path | None]] = []
                for section in parsed_mdoc["data"]:
                    if "SubFramePath" not in section:
                        continue
                    name = Path(section["SubFramePath"].replace("\\", "/")).name
                    found = next((d / name for d in (source_movie_dir, mdoc_path.parent) if (d / name).exists()), None)
                    sources.append((section, found))
                movies_complete = bool(sources) and all(p is not None for _, p in sources)
                movies_partial = any(p is not None for _, p in sources)
                stack = resolve_stack(mdoc_path, facts.image_file, facts.n_sections)
                layer = choose_source_layer(facts.software, movies_complete, movies_partial, stack is not None)

                if layer == "stack":
                    if progress_cb:
                        progress_cb(f"splitting stack {idx + 1}/{n_series}")
                    result = split_stack(mdoc_path, frames_dir, prefix=import_prefix, progress=progress_cb)
                    new_mdoc_path.write_text(result.mdoc_text)
                    layers["stacks"] += 1
                    extensions[".mrc"] += 1
                    n_frames += result.n_frames
                    continue

                if progress_cb:
                    progress_cb(f"linking movies {idx + 1}/{n_series}")
                for section, source_movie_path in sources:
                    original_movie_name = Path(section["SubFramePath"].replace("\\", "/")).name
                    prefixed_movie_name = f"{import_prefix}{original_movie_name}"
                    section["SubFramePath"] = prefixed_movie_name
                    if source_movie_path is None:
                        logger.warning("Source movie not found: %s", source_movie_dir / original_movie_name)
                        continue
                    link_path = frames_dir / prefixed_movie_name
                    if not link_path.exists():
                        os.symlink(source_movie_path.resolve(), link_path)
                    extensions[source_movie_path.suffix.lower()] += 1
                    n_frames += 1
                self.mdoc_service.write_mdoc_file(parsed_mdoc, new_mdoc_path)
                layers["movies"] += 1

            if not layers:
                return err("No tilt-series imported: every selected mdoc lacks [ZValue] sections.")
            if len(layers) > 1:
                logger.warning("Mixed source layers imported into %s: %s", project_dir, dict(layers))
            return ok(
                message=f"Imported {sum(layers.values())} tilt-series.",
                layer=layers.most_common(1)[0][0],
                frame_extension=extensions.most_common(1)[0][0] if extensions else "",
                n_frames=n_frames,
            )
        except Exception as e:
            logger.exception("Data import failed for %s", project_dir)
            return err(str(e))

    async def setup_project_data(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str,
        selected_mdoc_paths: list[str] | None = None,
        progress_cb: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        """Async wrapper — offloads blocking file I/O to a thread."""
        return await asyncio.to_thread(
            self._setup_project_data_sync,
            project_dir,
            movies_glob,
            mdocs_glob,
            import_prefix,
            selected_mdoc_paths,
            progress_cb,
        )


class ProjectService:
    def __init__(self, backend_instance: "CryoBoostBackend"):

        self.backend = backend_instance
        self.data_importer = DataImportService()
        self.star_handler = StarfileService()
        self.project_root: Path | None = None
        self.state_service = get_state_service()

    async def delete_job(self, job_name: str, project_path: Path, instance_id: str | None = None) -> dict[str, Any]:
        try:
            job_type = JobType(job_name)
            project_dir = Path(project_path)
            state = self.backend.state_service.state_for(project_dir)

            deletion_service = get_deletion_service()
            job_resolver = self.backend.pipeline_orchestrator.job_resolver

            # When a specific instance is requested, only delete that one job path.
            # Otherwise fall back to deleting all instances of the type (legacy behaviour).
            if instance_id:
                job_model = state.jobs.get(instance_id)
                if job_model and job_model.relion_job_name:
                    job_paths = [job_model.relion_job_name]
                else:
                    job_paths = []
            else:
                job_paths = deletion_service.find_jobs_by_type(project_dir, job_type, job_resolver)

            if not job_paths:
                instances_to_remove = (
                    [instance_id]
                    if instance_id
                    else [iid for iid in list(state.jobs.keys()) if InstanceId.matches(iid, job_type)]
                )
                for iid in instances_to_remove:
                    state.jobs.pop(iid, None)
                    state.job_path_mapping.pop(iid, None)
                await self.backend.state_service.save_project(project_path=project_dir, force=True)
                return ok(message=f"Job {job_name} removed from project state.")

            all_orphans = []
            deleted_count = 0
            errors = []

            for job_path in job_paths:
                result = await deletion_service.delete_job(project_dir, job_path, recursive=False)
                if result.success:
                    deleted_count += 1
                    all_orphans.extend(result.orphaned_jobs)
                else:
                    errors.append(f"{job_path}: {result.error}")

            # Remove only the affected instances from state
            if instance_id:
                instances_to_remove = [instance_id]
            else:
                instances_to_remove = [iid for iid in list(state.jobs.keys()) if InstanceId.matches(iid, job_type)]
            for iid in instances_to_remove:
                state.jobs.pop(iid, None)
                state.job_path_mapping.pop(iid, None)

            await self.backend.state_service.save_project(project_path=project_dir, force=True)
            await self.backend.pipeline_runner.sync_all_jobs(str(project_dir))

            if errors:
                return err(
                    f"Partial failure: {'; '.join(errors)}", deleted_count=deleted_count, orphaned_jobs=all_orphans
                )

            return ok(
                message=f"Deleted {deleted_count} job instance(s)."
                + (
                    f" Warning: {len(all_orphans)} downstream job(s) now have broken inputs: {all_orphans}"
                    if all_orphans
                    else ""
                ),
                deleted_count=deleted_count,
                orphaned_jobs=all_orphans,
            )

        except Exception as e:
            import traceback

            traceback.print_exc()
            return err(str(e))

    def set_project_root(self, project_dir: Path):
        """Set the project root for path resolution and update state."""
        self.project_root = project_dir.resolve()
        # Path-keyed registry, not tab-context state.
        if self.backend and self.backend.state_service:
            state = self.backend.state_service.state_for(self.project_root)
            state.project_path = self.project_root

    def get_job_dir(self, job_name: str, job_number: int) -> Path:
        if not self.project_root:
            raise ValueError("Project root not set. Call set_project_root() first.")

        job_type = JobType.from_string(job_name)
        param_classes = jobtype_paramclass()
        param_class = param_classes.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_name}")

        category = param_class.JOB_CATEGORY
        return self.project_root / category.value / f"job{job_number:03d}"

    def resolve_job_paths(self, job_name: str, job_number: int, selected_jobs: list[str]) -> dict[str, Path]:
        if not self.project_root:
            raise ValueError("Project root not set")

        job_type = JobType.from_string(job_name)
        param_classes = jobtype_paramclass()
        param_class = param_classes.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_name}")

        job_dir = self.get_job_dir(job_name, job_number)
        upstream_outputs = {}
        input_requirements = param_class.get_input_requirements()

        for _logical_name, upstream_job_type_str in input_requirements.items():
            try:
                upstream_idx = selected_jobs.index(upstream_job_type_str)
                upstream_job_num = upstream_idx + 1
            except ValueError:
                raise ValueError(f"{job_name} requires {upstream_job_type_str} but it's not in selected jobs") from None

            upstream_job_type = JobType.from_string(upstream_job_type_str)
            upstream_param_class = param_classes.get(upstream_job_type)
            if not upstream_param_class:
                raise ValueError(f"Unknown upstream job type: {upstream_job_type_str}")

            upstream_job_dir = self.get_job_dir(upstream_job_type_str, upstream_job_num)
            upstream_outputs[upstream_job_type_str] = upstream_param_class.get_output_assets(upstream_job_dir)

        input_paths = param_class.get_input_assets(job_dir, self.project_root, upstream_outputs)
        output_paths = param_class.get_output_assets(job_dir)
        all_paths = {**input_paths, **output_paths}

        return all_paths

    async def create_project_structure(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str,
        selected_mdoc_paths: list[str] | None = None,
        progress_cb: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        """Creates the project directory structure and imports the raw data. On success the
        import result is returned as is (it carries `layer` / `frame_extension` / `n_frames`)."""
        try:
            project_dir.mkdir(parents=True, exist_ok=True)
            self.set_project_root(project_dir)

            (project_dir / "Schemes").mkdir(exist_ok=True)
            (project_dir / "Logs").mkdir(exist_ok=True)

            await self._setup_qsub_templates(project_dir)

            return await self.data_importer.setup_project_data(
                project_dir, movies_glob, mdocs_glob, import_prefix, selected_mdoc_paths, progress_cb
            )
        except Exception as e:
            return err(f"Failed during directory setup: {e!s}")

    async def _setup_qsub_templates(self, project_dir: Path):
        """Copy qsub.sh to project root for relion_schemer to find."""
        source_qsub = Path.cwd() / "config" / "qsub.sh"
        dest_qsub = project_dir / "qsub.sh"

        if source_qsub.exists():
            shutil.copy(source_qsub, dest_qsub)
            logger.info("Copied qsub.sh to %s", dest_qsub)
        else:
            logger.warning("qsub.sh not found at %s", source_qsub)

    def _build_and_persist_registry(
        self, project_dir: Path, mdocs_glob: str, *, dose_per_tilt_fallback: float | None = None
    ) -> int:
        """Build TiltSeries registry from mdocs and save to {project}/registry/.

        `dose_per_tilt_fallback` (the project's dose) feeds the per-frame pre-exposure when
        the mdocs record no ExposureDose (SerialEM without dose calibration).

        Called from `initialize_new_project` after data import, and from
        `load_project_state` if the registry sidecar is missing or stale. Returns
        the number of TS added. Callers log exceptions and continue.

        Reads from `{project_dir}/mdoc/*.mdoc` — the post-import copies, which
        carry the `{project_name}_` prefix added by `DataImportService`. The
        TS IDs therefore match what `ts_import` writes into `tilt_series.star`.
        `mdocs_glob` (the raw source) is kept as a fallback for projects where
        the project mdoc dir is empty.
        """
        from services.tilt_series import get_registry_for, set_registry_for, TiltSeriesRegistry
        from services.tilt_series.build import build_from_mdocs

        frames_dir = project_dir / "frames"
        project_mdoc_glob = str(project_dir / "mdoc" / "*.mdoc")

        build_kwargs = {
            "frames_dir": frames_dir if frames_dir.exists() else None,
            "dose_per_tilt_fallback": dose_per_tilt_fallback,
        }
        ts_list = build_from_mdocs(project_mdoc_glob, **build_kwargs)
        # Fallback: if the project mdoc dir is empty (shouldn't happen for a
        # post-import project but can for hand-assembled test projects), fall
        # back to the source glob. Warn because this produces unprefixed TS
        # IDs that won't match downstream STARs.
        if not ts_list and mdocs_glob:
            logger.warning(
                "Registry: project mdoc dir empty (%s); falling back to source glob %s. "
                "TS IDs may not match ts_import output.",
                project_mdoc_glob,
                mdocs_glob,
            )
            ts_list = build_from_mdocs(mdocs_glob, **build_kwargs)
        if not ts_list:
            logger.info("Registry: no mdocs found for %s", project_dir)
            return 0

        # Fresh registry (overwrites any stale sidecars from a partial prior run).
        registry = TiltSeriesRegistry(project_dir)
        for ts in ts_list:
            registry.add_tilt_series(ts)
        problems = registry.sanity_check()
        if problems:
            logger.warning("Registry sanity issues for %s: %s", project_dir, problems)
        registry.save(force=True)
        set_registry_for(project_dir, registry)
        logger.info(
            "Registry: persisted %d tilt-series (%d frames) to %s",
            len(ts_list),
            registry.frame_count(),
            registry.registry_dir,
        )
        # Prime the path-keyed cache so backend.registry_for(project_dir) hits.
        _ = get_registry_for(project_dir)
        return len(ts_list)

    async def initialize_new_project(
        self,
        project_name: str,
        project_base_path: str,
        selected_jobs: list[str],
        movies_glob: str,
        mdocs_glob: str,
        selected_mdoc_paths: list[str] | None = None,
        import_summary: dict[str, Any] | None = None,
        detected_params: dict[str, Any] | None = None,
        shared: bool = False,
        progress_cb: Callable[[str], None] | None = None,
    ):
        try:
            project_dir = Path(project_base_path).expanduser() / project_name

            # 1. Standard Setup (Dirs, Data Import)
            if project_dir.exists():
                return err(f"Project directory '{project_dir}' already exists.")

            import getpass
            from services.project_nickname import nickname_for
            from services.project_state import ProjectState, SHARED_OWNER

            state = ProjectState()
            state.project_name = project_name
            # Persist a deterministic 3-word nickname so future loads keep the
            # same one. Seed on project_dir so it's stable across renames of
            # project_name (the dir is the durable identity).
            state.mnemonic = nickname_for(str(project_dir))
            state.project_path = project_dir
            state.movies_glob = movies_glob
            state.mdocs_glob = mdocs_glob
            state.created_by = getpass.getuser()
            # `created_by` is immutable provenance; `owner` drives sharing/grouping.
            state.owner = SHARED_OWNER if shared else None
            set_project_state_for(project_dir, state)

            # A project can be created with NO pipeline jobs (data-less): particles then
            # arrive by cross-project merge or tomogram import, and downstream jobs
            # (Reconstruct/Class3D/Refine3D) are added through the regular job roster. They
            # pick up the active merge's MergedSources/<slug>/optimisation_set.star through
            # the synthetic `mergedSources` producer the path resolver registers
            # (apply_aggregation_overrides wires it via a source_overrides key). Creation
            # does not depend on a project-type flag.

            # Apply microscope/acquisition params from the already-parsed dataset
            # overview (avoids re-parsing all mdocs from scratch). A data-less project has
            # no source mdocs, so skip the fallback re-parse.
            if detected_params:
                if "pixel_size_angstrom" in detected_params:
                    state.microscope.pixel_size_angstrom = detected_params["pixel_size_angstrom"]
                if "acceleration_voltage_kv" in detected_params:
                    state.microscope.acceleration_voltage_kv = detected_params["acceleration_voltage_kv"]
                if "dose_per_tilt" in detected_params:
                    state.acquisition.dose_per_tilt = detected_params["dose_per_tilt"]
                if "tilt_axis_degrees" in detected_params:
                    state.acquisition.tilt_axis_degrees = detected_params["tilt_axis_degrees"]
                # Gain reference is user-supplied at setup (not auto-detectable from
                # mdocs). Only set when non-empty so we never stamp a blank over a
                # value that could arrive later; the drivers read it via
                # job.gain_path -> acquisition.gain_reference_path.
                if detected_params.get("gain_reference_path"):
                    state.acquisition.gain_reference_path = detected_params["gain_reference_path"]
                if detected_params.get("dose_per_tilt_source"):
                    state.acquisition.dose_per_tilt_source = detected_params["dose_per_tilt_source"]
                if detected_params.get("acquisition_software"):
                    state.acquisition.acquisition_software = detected_params["acquisition_software"]
                if detected_params.get("detector_dimensions"):
                    state.acquisition.detector_dimensions = tuple(detected_params["detector_dimensions"])
                state.update_modified()
            elif mdocs_glob:
                # Fallback: re-parse mdocs (no overview available).
                # Gated on having mdocs at all — data-less projects have none, so
                # this is skipped without a flag check.
                await self.backend.state_service.update_from_mdoc(mdocs_glob, project_path=project_dir)

            # The dose per tilt is never left to the model default: every
            # creation path ends with a value and its provenance. Callers that pass a source
            # ("user", "estimated") are believed; otherwise the first mdoc decides — its
            # ExposureDose ("mdoc") or the zero-thickness estimate ("estimated") — and a
            # dataset that allows neither refuses to create rather than run on 3.0.
            if mdocs_glob:
                found = await asyncio.to_thread(self.data_importer.mdoc_service.first_mdoc_facts, mdocs_glob)
                facts = found[0] if found else None
                given = detected_params or {}
                if facts is not None:
                    if facts.software and not given.get("acquisition_software"):
                        state.acquisition.acquisition_software = facts.software
                    if facts.detector_dimensions and not given.get("detector_dimensions"):
                        state.acquisition.detector_dimensions = facts.detector_dimensions
                    if not state.acquisition.dose_per_tilt_source:
                        dose_given = "dose_per_tilt" in given
                        if dose_given or facts.dose_per_tilt is not None:
                            if not dose_given:
                                state.acquisition.dose_per_tilt = facts.dose_per_tilt
                            state.acquisition.dose_per_tilt_source = "mdoc"
                        elif facts.dose_estimate is not None:
                            state.acquisition.dose_per_tilt = facts.dose_estimate.value
                            state.acquisition.dose_per_tilt_source = "estimated"
                            logger.warning(
                                "%s: dose per tilt not in the mdocs — using the estimate %.2f e/Å² (%s)",
                                project_name,
                                facts.dose_estimate.value,
                                facts.dose_estimate.describe(),
                            )
                        else:
                            return err("dose per tilt: not in the mdocs and no estimate possible — enter it")
                    state.update_modified()

            # Apply dataset import summary so it's saved atomically with the project
            if import_summary:
                state.import_total_positions = import_summary.get("total_positions", 0)
                state.import_selected_positions = import_summary.get("selected_positions", 0)
                state.import_total_tilt_series = import_summary.get("total_tilt_series", 0)
                state.import_selected_tilt_series = import_summary.get("selected_tilt_series", 0)
                state.import_source_directory = import_summary.get("source_directory", "")
                state.import_frame_extension = import_summary.get("frame_extension", "")
                # Per-position/per-TS details + tilt_metadata live in the
                # TiltSeriesRegistry built below, not in ProjectState.

            # Create Dirs & Import Data (runs blocking I/O in thread pool)
            import_prefix = f"{project_name}_"
            structure_result = await self.create_project_structure(
                project_dir, movies_glob, mdocs_glob, import_prefix, selected_mdoc_paths, progress_cb
            )
            if not structure_result["success"]:
                return structure_result
            # What the import actually did — set before the jobs are
            # initialized so ensure_job_initialized's stamps see it.
            if structure_result.get("layer"):
                state.import_source_kind = structure_result["layer"]
            if structure_result.get("frame_extension"):
                state.import_frame_extension = structure_result["frame_extension"]

            # 2. Instantiate the selected jobs with their code defaults.
            if selected_jobs:
                logger.info("Initializing jobs: %s", selected_jobs)
                for job_str in selected_jobs:
                    try:
                        state.ensure_job_initialized(JobType(job_str))
                    except ValueError:
                        logger.warning("Skipping unknown job '%s'", job_str)

            # 3. Build the TiltSeries registry from the imported mdocs, persist to
            # sidecar JSON under {project}/registry/. Failure here is logged but
            # non-fatal. Data-less projects have no mdocs of their own to
            # register — gate on the mdocs glob, not a project-type flag.
            if mdocs_glob:
                try:
                    self._build_and_persist_registry(
                        project_dir, mdocs_glob, dose_per_tilt_fallback=state.acquisition.dose_per_tilt
                    )
                except Exception as e:
                    logger.warning("Registry construction failed for %s: %s", project_dir, e)

            # 4. Save Project State (project_params.json) — includes import summary
            await self.backend.state_service.save_project(project_path=project_dir, force=True)

            # 5. Initialize Relion (Create default_pipeline.star)
            logger.info("Initializing Relion project...")

            init_command = "unset DISPLAY && relion --tomo --do_projdir ."
            # Data-less projects have no raw-data parents to bind. Path("").parent
            # would resolve to the cwd, which is wrong and would clutter the binds.
            binds = [str(project_dir.resolve())]
            if movies_glob:
                binds.append(str(Path(movies_glob).parent.resolve()))
            if mdocs_glob:
                binds.append(str(Path(mdocs_glob).parent.resolve()))

            container_cmd = self.backend.container_service.wrap_command_for_tool(
                command=init_command, cwd=project_dir, tool_name="relion", additional_binds=binds
            )

            proc = await asyncio.create_subprocess_shell(
                container_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=project_dir
            )
            await proc.wait()

            return ok(message=f"Project '{project_name}' created.", project_path=str(project_dir))

        except Exception as e:
            import traceback

            traceback.print_exc()
            return err(str(e))

    async def load_project_state(self, project_path: str) -> dict[str, Any]:
        """
        Loads a project via StateService.
        """
        try:
            project_dir = Path(project_path)
            if not project_dir.exists():
                return err(f"Project path not found: {project_path}")

            params_file = project_dir / "project_params.json"
            if not params_file.exists():
                return err("No project_params.json found")

            # Manually read data_sources for compatibility
            movies_glob = ""
            mdocs_glob = ""
            try:
                with open(params_file) as f:
                    raw_params_data = json.load(f)

                data_sources = raw_params_data.get("data_sources", {})
                if data_sources:
                    movies_glob = data_sources.get("frames_glob", "")
                    mdocs_glob = data_sources.get("mdocs_glob", "")

                if not movies_glob and "acquisition" in raw_params_data:
                    movies_glob = raw_params_data["acquisition"].get("frames_glob", "")
                if not mdocs_glob and "acquisition" in raw_params_data:
                    mdocs_glob = raw_params_data["acquisition"].get("mdocs_glob", "")

            except Exception as e:
                logger.info("Warning: could not parse raw JSON for data_sources: %s", e)

            # Load via StateService (this registers into the path-keyed registry)
            load_success = await self.backend.state_service.load_project(params_file)

            if not load_success:
                return err(f"StateService failed to load project from {params_file}")

            # Explicit path: the state just loaded
            state = self.backend.state_service.state_for(project_dir)
            self.set_project_root(project_dir)

            # Rebuild TS registry if it's absent OR its TS IDs have drifted from
            # the project mdoc dir stems. Drift detection catches a registry
            # built from unprefixed source mdocs, whose TS IDs don't match what
            # ts_import writes; such a stale sidecar makes every ingest step
            # fail with "TS missing from registry".
            try:
                registry = self.backend.registry_for(project_dir)
                reg_ids = set(registry.tilt_series_ids())
                mdoc_stems = {p.stem for p in (project_dir / "mdoc").glob("*.mdoc")}
                drift = bool(reg_ids) and bool(mdoc_stems) and reg_ids.isdisjoint(mdoc_stems)
                if (not reg_ids or drift) and (mdocs_glob or mdoc_stems):
                    reason = "absent" if not reg_ids else "drifted from project mdocs"
                    logger.info("Registry %s for %s; rebuilding from mdocs", reason, project_dir)
                    if drift:
                        # Clear the stale in-memory registry so the rebuild
                        # starts from empty rather than merging.
                        from services.tilt_series import clear_registry

                        clear_registry(project_dir)
                    self._build_and_persist_registry(project_dir, mdocs_glob)
            except Exception as e:
                logger.warning("Registry backfill failed for %s: %s", project_dir, e)

            # Sync job statuses
            await self.backend.pipeline_runner.sync_all_jobs(str(project_path))

            project_name = state.project_name
            selected_jobs = list(state.jobs.keys())

            return ok(
                project_name=project_name, selected_jobs=selected_jobs, movies_glob=movies_glob, mdocs_glob=mdocs_glob
            )
        except Exception as e:
            import traceback

            traceback.print_exc()
            return err(str(e))

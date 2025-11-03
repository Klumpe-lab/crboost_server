You're right, that's a great adjustment. Keeping the command-building logic in `command_builders.py` is a cleaner separation of concerns. The parameter models should be data containers, not service classes.

We can still get the scalability you want by having the models provide *metadata* (like "am I a driver job?" or "which container tool do I use?") and then have the orchestrator use that metadata to decide *how* to build the command (either by calling a driver script or using the correct `CommandBuilder`).

Here is a phased plan designed to be implemented incrementally with minimal breakage.

-----

## Phase 1: State & Model Consolidation (Low Risk)

This phase cleans up internal file organization. It won't change any API behavior, so it's a safe place to start.

1.  **Move `PipelineState`:**

      * **Cut** the entire `PipelineState` class definition from `services/parameter_models.py`.
      * **Paste** it into `app_state.py`.
      * **Update Imports:** In `app_state.py`, add imports for `AbstractJobParams` and `jobtype_paramclass` from `parameter_models.py`.

2.  **Create `MdocService`:**

      * Create a new file: `services/mdoc_service.py`.
      * Move the `_parse_mdoc` logic from `app_state.py` into a method in this new service (e.g., `get_autodetect_params`).
      * Move the `_parse_mdoc` and `_write_mdoc` logic from `services/project_service.py` into methods in this new service (e.g., `parse_mdoc_file`, `write_mdoc_file`).
      * Create a singleton instance (like `get_config_service`).

3.  **Update Consumers:**

      * In `app_state.py`, change `update_from_mdoc` to call `get_mdoc_service().get_autodetect_params(...)` and remove the local `_parse_mdoc` function.
      * In `services/project_service.py`, change the `DataImportService` to use `get_mdoc_service()` for parsing and writing mdoc files.

-----

## Phase 2: Refactor Project Initialization (Low Risk)

This moves the "god method" from the `Backend` into the `ProjectService` where it belongs.

1.  **Move `create_project_and_scheme`:**

      * **Copy** the entire `create_project_and_scheme` method from `backend.py`.
      * **Paste** it into `services/project_service.py` and rename it (e.g., `initialize_new_project`).
      * This new method will need `from app_state import export_for_project`. It can access `self.backend.pipeline_orchestrator` and `self.backend.container_service` to perform all the steps it did before.

2.  **Delegate in `Backend`:**

      * **Replace** the entire implementation of `create_project_and_scheme` in `backend.py` with a single delegation call:
        ```python
        # backend.py
        async def create_project_and_scheme(
            self,
            project_name: str,
            # ... all other args
        ):
            return await self.project_service.initialize_new_project(
                project_name=project_name,
                project_base_path=project_base_path,
                selected_jobs=selected_jobs,
                movies_glob=movies_glob,
                mdocs_glob=mdocs_glob,
            )
        ```

-----

## Phase 3: Create `PipelineRunnerService` (Low Risk)

This moves all process-running and monitoring logic out of the `Backend`.

1.  **Create `PipelineRunnerService`:**

      * Create `services/pipeline_runner_service.py`.
      * Define a `PipelineRunnerService` class that takes `backend_instance` in its `__init__`.
      * **Move** the following methods from `backend.py` into this new class:
          * `start_pipeline`
          * `_run_relion_schemer`
          * `_monitor_schemer`
          * `get_pipeline_progress`
          * `get_pipeline_job_logs`
          * `monitor_pipeline_jobs`
      * Update these methods to access shared services via `self.backend` (e.g., `self.backend.container_service`, `self.backend.pipeline_orchestrator.star_handler`).

2.  **Update `Backend`:**

      * In `backend.py`'s `__init__`, add `self.pipeline_runner = PipelineRunnerService(self)`.
      * **Replace** the implementations of all moved methods with simple delegation calls (e.g., `async def start_pipeline(self, ...): return await self.pipeline_runner.start_pipeline(...)`).
      * For the `monitor_pipeline_jobs` generator, you'll need to `yield from` its results.

-----

## Phase 4: Scalable Command Building (Medium Risk)

This is the fix for the `if/elif` block, implementing your feedback.

1.  **Add Metadata to Models:**

      * In `services/parameter_models.py`, edit `AbstractJobParams`:
          * Add `def is_driver_job(self) -> bool: return False` (default to simple command).
          * Add `def get_tool_name(self) -> str: raise NotImplementedError("Must define tool name")`.
      * In `ImportMoviesParams`:
          * Implement `def get_tool_name(self) -> str: return "relion_import"`.
      * In `FsMotionCtfParams` and `TsAlignmentParams`:
          * Implement `def is_driver_job(self) -> bool: return True`.
          * Implement `def get_tool_name(self) -> str: return "driver"` (or "warptools", etc. - just needs to be consistent).

2.  **Refactor `pipeline_orchestrator_service.py`:**

      * In `__init__`, modify `self.job_builders` to map *only non-driver* jobs:
        ```python
        from services.commands_builder import ImportMoviesCommandBuilder # ... etc
        from services.parameter_models import JobType

        self.job_builders: Dict[str, BaseCommandBuilder] = {
            JobType.IMPORT_MOVIES.value: ImportMoviesCommandBuilder(),
            # Add other non-driver job builders here
        }
        ```
      * **Replace** the `_build_job_command` implementation with this new, scalable version:
        ```python
        # (JobType import needed at top of file)
        def _build_job_command(
            self, job_name: str, job_model: BaseModel, paths: Dict[str, Path], all_binds: List[str], server_dir: Path
        ) -> str:
            
            # 1. Ask the model if it's a driver job
            if job_model.is_driver_job():
                host_python_exe = server_dir / "venv" / "bin" / "python3"
                if not host_python_exe.exists():
                    host_python_exe = "python3" # Fallback
                
                env_setup = f"export PYTHONPATH={server_dir}:${{PYTHONPATH}};"
                
                # This 'if' block is small and only maps driver jobs to their scripts
                if job_name == JobType.FS_MOTION_CTF.value:
                    driver_script_path = server_dir / "drivers" / "fs_motion_and_ctf.py"
                elif job_name == JobType.TS_ALIGNMENT.value:
                    driver_script_path = server_dir / "drivers" / "ts_alignment.py"
                else:
                    return f"echo 'ERROR: Driver job {job_name} not recognized'; exit 1;"
                
                return f"{env_setup} {host_python_exe} {driver_script_path}"
            
            # 2. If not a driver, it's a simple command
            else:
                builder = self.job_builders.get(job_name)
                if not builder:
                    return f"echo 'ERROR: No command builder found for {job_name}'; exit 1;"
                
                # Build the raw command using the builder class
                raw_command = builder.build(job_model, paths)
                
                # Get the tool name from the model
                tool_name = job_model.get_tool_name() 
                
                container_svc = self.backend.container_service
                return container_svc.wrap_command_for_tool(
                    command=raw_command,
                    cwd=paths["job_dir"],
                    tool_name=tool_name,
                    additional_binds=all_binds
                )
        ```
      * In `create_custom_scheme`, make sure you are passing the `job_model` (not just `job_name`) to `_build_job_command`. You already fetch `job_model` from the app state, so this should be a simple change.

-----

## Phase 5: Legacy State Removal (High Risk)

This phase *will* break the UI until the UI code is updated simultaneously. Do this last, as one atomic commit.

1.  **Update UI Code:** Go through all UI files (`main_ui.py`, `projects_tab.py`, etc.) and change any code that relies on the flat structure from `get_ui_state_legacy()`. Update it to read from the hierarchical state (e.g., `state['microscope']['pixel_size_angstrom']`).
2.  **Update Backend:**
      * In `backend.py`, change `get_initial_parameters` and `autodetect_parameters` to return `app_state.model_dump()`.
3.  **Delete Debt:**
      * Delete the `get_ui_state_legacy` function from `app_state.py`.
      * Remove it from the `app_state` imports in `backend.py`.
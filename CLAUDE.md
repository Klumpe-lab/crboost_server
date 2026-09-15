# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

CryoBoost Server — a web-based cryo-electron tomography pipeline manager. It runs on an HPC cluster headnode, provides a NiceGUI/FastAPI browser UI, and orchestrates RELION-based processing pipelines via SLURM. External tools (RELION, Warp/AreTomo, PyTOM, CryoCARE, IMOD) run inside Apptainer containers or as native binaries, configured per-tool in `config/conf.yaml`.

## Commands

```bash
# Run the server
python main.py --port 8081 --host 0.0.0.0

# Validate environment setup
python preflight.py

# Lint / format
ruff check .
ruff format .

# Architecture-boundary lint (AST-based; catches function-local imports)
python check_boundaries.py
```

There is no test suite.

## Configuration

- `config/conf.yaml` — main config (created from `config/conf.template.yaml` by `preflight.py`). Contains: `crboost_root`, `crboost_python`, local paths, SLURM defaults, and per-tool execution config (container vs binary).
- `config/qsub.sh` — SLURM job submission template. Uses RELION-style `XXXextra1XXX`..`XXXextra8XXX` placeholders that get substituted with `SlurmConfig` fields at submission time.
- `config/protocols/<name>/` — Protocol bundles (`protocol.yaml` + `assets/`): the shape of a pipeline with its parameters, applied at project creation to make a REGULAR project (`services/protocols/`, roadmap 16). A project created from one carries `ProjectState.protocol_origin` and a frozen `<project>/protocol/protocol.yaml`; the workspace Protocols view (foot-of-rail light, `ui/protocols_view.py`) shows protocol vs current parameters. Nothing about results lives in protocols. How to write one: `docs/protocols.md`. Job instances take their code defaults; there are no per-job template files.

## Architecture

### Entry point and request flow

`main.py` → creates `FastAPI` app, mounts `/static`, initializes `CryoBoostBackend`, passes it to `ui/main_ui.py` which defines NiceGUI page routes (`/` landing, `/p/<project>/<view>/<target>` workspace, `/workspace` redirect).

### Backend (`backend.py`)

`CryoBoostBackend` is the central facade. It holds singleton service instances and exposes async methods consumed by the UI layer. All services receive a back-reference to `CryoBoostBackend` at construction.

### Services layer (`services/`)

- **`project_state.py`** — `ProjectState` (Pydantic model) is the single source of truth for a project. Serialized as `project_params.json` in each project directory. A path-keyed in-memory registry (`_project_states`) ensures one `ProjectState` per project across browser tabs. `StateService` wraps persistence with an asyncio lock.
- **`job_models.py`** — `AbstractJobParams` base class and concrete param classes per `JobType` (e.g., `FsMotionCtfParams`, `TsReconstructParams`). Each declares `USER_PARAMS` (user-editable fields), `JOB_CATEGORY`, `RELION_JOB_TYPE`. The `jobtype_paramclass()` function maps `JobType` enum → param class.
- **`models_base.py`** — shared enums (`JobType`, `JobStatus`, `JobCategory`, `AlignmentMethod`) and base Pydantic models (`MicroscopeParams`, `AcquisitionParams`).
- **`configs/config_service.py`** — loads `conf.yaml` into a typed `Config` model. Singleton via `get_config_service()`. Provides `get_tool_config(tool_name)` with legacy alias support.
- **`computing/slurm_service.py`** — `SlurmConfig` model, presets, and SLURM job submission/query.
- **`computing/container_service.py`** — wraps shell commands with `apptainer exec` invocations based on per-tool config.
- **`scheduling_and_orchestration/pipeline_orchestrator_service.py`** — `deploy_and_run_scheme()` creates a RELION Scheme directory, writes `job.star` files via `StarfileService`, and launches the RELION schemer process.
- **`scheduling_and_orchestration/pipeline_runner.py`** — owns schemer process lifecycle; `sync_all_jobs()` reconciles `default_pipeline.star` with in-memory job state.
- **`configs/starfile_service.py`** — reads/writes RELION `.star` files.
- **`io_slots.py` / `path_resolution_service.py`** — typed I/O slot system for inter-job data flow and path resolution.

### Drivers (`drivers/`)

Python scripts that execute on compute nodes inside SLURM jobs. Each driver calls `driver_base.get_driver_context()` which loads `project_params.json`, looks up the job by `--instance_id`, and returns `(ProjectState, job_model, context_data, job_dir, project_path, job_type)`. Drivers then build and run tool commands (often via container wrapping).

### UI (`ui/`)

NiceGUI-based. `main_ui.py` defines routes. Key components:
- `routing.py` — addressable URLs (roadmap 17). `View`/`Route`/`parse_route`/`route_to_path` are the pure URL↔(view, target) mapping; `apply_route` drives a route onto a built workspace *through the workspace `callbacks` dict*; `RouteWriter` (exposed as `callbacks["set_url"]`) writes the URL back. Deliberately imports nothing from `ui.*` at module level — half of `ui` imports it.
- `data_import_panel.py` — landing page: project creation/loading, data glob inputs.
- `workspace_page.py` — main workspace after project load.
- `pipeline_builder/` — pipeline configuration, job tabs, SLURM config, status polling.
- `job_plugins/` — per-job-type UI renderers (custom parameter forms).
- `ui_state.py` — `UIStateManager` per-tab state (stored in NiceGUI client storage).

### Key patterns

- **Instance IDs**: Jobs are keyed by `instance_id` string (e.g., `"tsReconstruct"`, `"templatematching__ribosome"`). The `__` separator denotes a species-specific instance. `JobType` enum holds the base type; instance_id may differ.
- **Singletons**: Services use module-level `_instance` + `get_*()` factory pattern (e.g., `get_config_service()`, `get_state_service()`).
- **Dirty tracking**: `ProjectState` has `mark_dirty()` / `is_dirty` / `save_if_dirty()` for deferred persistence.
- **RELION compatibility**: The system generates RELION-compatible directory structures, `default_pipeline.star`, and `job.star` files so projects can also be opened in RELION directly.

### UI reactivity patterns

NiceGUI's default refresh idiom — `container.clear()` followed by a full rebuild — destroys every child element along with its event handlers. If a user click is mid-flight against the old DOM, it gets silently dropped. If the cursor is hovering a row, :hover doesn't fire on the replacement until the cursor moves. Both compound when the rebuild is timer-driven and the cadence is short relative to typical click latency.

Two primitives in `ui/components/reactive.py` solve this:

- **`FingerprintedView`** — a container view whose `refresh()` is a no-op when nothing the render reads has changed. Subclasses implement `signature()` (cheap fingerprint of every state input the render touches) and `render()`. Polling timers tick on cadence but only cause a DOM rebuild when the signature actually moves. Used by `RosterWidget` (`ui/pipeline_builder/pipeline_roster.py`) and `_BackgroundTaskTray` (`ui/background_task_tray.py`).

- **`SingleFlight`** — guards an async handler against re-entry. `async with self.flight("key") as acquired: if not acquired: return ...` makes the second invocation a silent no-op while the first is in flight. Required for any handler that opens a dialog or does long async work — the trigger button can be destroyed and recreated mid-click by an unrelated refresh, so users may legitimately click "add" several times before one lands; without the guard each click queues another dialog. Used by `PipelineBuilderPanel.prompt_species_and_add`.

Conventions when writing new UI:

- **Polls observe state; they don't rebuild the DOM directly.** A timer's callback should mutate model fields (or trigger a `FingerprintedView.refresh()` which is signature-gated), never call `container.clear()` unconditionally.
- **Default poll cadence**: 3 s for cheap in-memory state observation, ~15 s for anything that touches disk. Sub-second timers must be replaceable by CSS animations or hard-justified.
- **Animations are CSS, never server ticks.** `@keyframes` in `ui/main_ui.py` drives the braille spinner (`.cb-braille-spin`) — there is no 0.17 s `ui.timer` advancing a glyph anymore.
- **Disk I/O off the event loop.** Render paths and event handlers that read files use `asyncio.to_thread` / `run.io_bound`. If a `signature()` does disk I/O, prefer reading mtimes/in-memory cache; the signature runs on every tick.
- **URL writes go through the History API, never `ui.navigate.to`.** A navigate rebuilds the page and throws away the lazily-built Journey / gallery / pick viewer, which is the entire cost the CSS-`display` view swap exists to avoid. `callbacks["set_url"](View.X, a, b)` (`ui/routing.py`) is the only way the address bar moves inside a workspace session. Order matters: `_switch_to` writes the bare view route (which `pushState`s, so Back walks views), then the view's own selection handler replaces it with the target-bearing one — and each such view must also write on *activation*, because re-entering an already-built view fires no selection handler.
- **Reactive bindings beat full rebuilds.** When a single attribute drives a single visual property, `bind_value_from(model, attr)` / `bind_content_from(model, attr)` updates only that property. `_status_widget` in the roster does this for the per-row status dot.
- **One control vocabulary.** Text buttons are `house_button()` (`ui/components/buttons.py`): slate outline, 10 px Plex Sans, 22 px tall, core look inline on the element; variants default / `accent` (the one primary action of a dialog/page) / `danger` (deletes). Form inputs in dialogs and registry-style pages are `house_field` / `house_number` / `house_text` / `house_select` (`ui/components/fields.py`): 9 px label left of a 16 px `.cb-field` box; `house_number` with `model=`/`attr=` binds through `numeric_forward` so int fields can't be corrupted by float values. Job-tab parameter forms stay on `ui/job_plugins/_field_styles.py` (`.cb-select` scale, 110 px label column) — one notch bigger by design; don't collapse the two scales. Never ad-hoc `color=primary` buttons or bare `ui.number`/`ui.input`/`ui.select` in dialogs. The `.cb-field`/`.cb-select` chrome and the app-wide spinner kill live ONLY in `ui/dashboard/css.py`; every page calls `ensure_assets_loaded()`. Actions sit WITH their inputs (`[button] [hint]`, no spacer-to-page-edge); dialog footers are a right-aligned `[Cancel] [accent action]` pair.
- **Mode-switch panels with different heights** get grid-stacked with inactive panels visibility-hidden (all inline styles) so the container holds the tallest panel's height and content below never jumps on a tab flip (`_stacked_panels` in `ui/template_workbench.py`).
- **Sections separate by whitespace, never underline rules.** Page-structure titles use `PAGE_SECTION_STYLE` (12 px, `_field_styles.py`) with a real vertical gap (`gap-5`) between blocks; no `section_rule()` under page-level headers, and no explanatory hint tail beside a title when the content below makes it obvious (fold it into the title's tooltip instead). Inputs must sit flush with surrounding 9–10 px text: `.cb-field` boxes are 16 px tall — if a control towers over its label, it's wrong.
- **CSS delivery has two channels and only one is trustworthy.** `ui.add_head_html` at module level (`ui/main_ui.py`) is baked into the page shell, and the shell is served stale on this deployment — new rules there may never reach the browser. `ui/dashboard/css.py` (`ensure_assets_loaded()`) injects per client after `client.connected()`, over the socket, and always arrives. Rule: structural/load-bearing styling goes inline on the element; hover states, pseudo-elements and shared polish go in `ui/dashboard/css.py`; never add new rules to the `main_ui.py` shell block.

### Job parameter tabs — species-derived fields

A particle-phase job tab (TM, Pick candidates, Subtomo extraction) shows three kinds of
field: the job's own parameters, its SLURM resources, and the handful of values it
inherited from the species when the instance was created (template, mask, symmetry, Ø,
extraction geometry). Those inherited values are still job parameters — snapshot at
creation, nothing auto-propagates — they just have a species-shaped default. So:

- **Weakly separated, not boxed.** `section_header(SPECIES_SECTION_TITLE)` + `field_grid()`
  inside the *same* Parameters card the other fields live in. Never a nested `ui.card()`
  with its own header bar, icon and font scale — that reads as a second application
  pasted into the form. Use `choice_row` (`ui/job_plugins/_field_styles.py`) when the
  options come from project state rather than a static `Enum`.
- **The tooltip names the species default**, so "why is this not what the species says"
  is answerable without leaving the tab. The Species page's Jobs-tab drift chips are the
  other half of that story and must keep agreeing with it.
- **Absent is stated, never defaulted.** No template registered means the row says so and
  points at the Species page; it never silently picks the first file it finds.

Full guideline of record: `docs/roadmaps/completed/picking_ui/roadmap_00-overview.md`.

## Results and exceptions — one idiom

Service-level outcomes are dicts built by `ok(...)` / `err(...)` from `services/result.py` — never
hand-built `{"success": ...}` literals. The only hard contract is `success` + `error` (+ optional
`code: ErrorCode` when a caller genuinely branches on the cause); payload keys stay ad-hoc. UI
consumption: render `result["error"]` on failure; branch on `result.get("code")` against the enum,
never on message text.

Exception policy — exactly three allowed forms:

1. **Can't handle → don't catch.** Let it propagate.
2. **Can report → catch, `logger.exception(...)` (full traceback) + `return err(...)`.**
3. **Genuinely expected-and-ignorable → catch the *narrow* type**, with a one-line comment saying why.

Bare `except Exception: pass` / `return <default>` fails review. Existing silent-swallow sites are
converted opportunistically whenever their file is touched.

## Surfacing uncertainty — never fail silently, never invent defaults

When a parameter is missing, ambiguous, or admits several viable interpretations, do NOT pick a
"reasonable default" and do NOT swallow it in a silent fallback. Inventing a value is not our job — it
hides a decision the domain expert needs to make, and silent fallbacks (e.g. apix → 1.0 when an MRC
header has no voxel size) produce wrong-but-plausible results that are painful to trace.

Instead, bubble it up to the UI where the value is used — a subtle tooltip, a red marker, or a
"WIP / unverified" badge — so the user sees the open decision while running the app and can supply or
override it. Keep it subtle and only where a real gap exists (don't litter the UI). Prefer raising or
visibly flagging over `except: return <default>` for any semantically load-bearing value.

## Code Style

- Line length: 120 (see `ruff.toml`)
- `ruff format` with `skip-magic-trailing-comma = true`
- `sys.dont_write_bytecode = True` is set in `main.py` — no `.pyc` files in dev

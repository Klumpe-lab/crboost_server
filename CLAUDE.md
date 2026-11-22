# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

CryoBoost Server — a web-based cryo-electron tomography pipeline manager. It runs on an HPC cluster headnode, provides a NiceGUI/FastAPI browser UI, and orchestrates RELION-based processing pipelines via SLURM. External tools (RELION, Warp/AreTomo, PyTOM, CryoCARE, IMOD) run inside Apptainer containers or as native binaries, configured per-tool in `config/conf.yaml`.

## Commands

```bash
# Run the server
./crboost                   # sh launcher: finds or builds this checkout's Python environment, runs crboost.py in it with a scrubbed env; picks a free port in 24300–24331; --port N to choose

# Validate environment setup
./crboost preflight

# Lint / format (ruff is pinned in requirements.txt)
./crboost python -m ruff check .
./crboost python -m ruff format .

# Architecture-boundary lint (AST-based; catches function-local imports)
./crboost python check_boundaries.py

# ui.html(..., sanitize=False) takes a constant or escaped content (html.escape); the rest is an allow-list that shrinks
./crboost python check_raw_html.py [--list]

# After editing requirements.in
./crboost lock
```

`tests/` is the install self-test, not unit tests: `./crboost test` checks the headnode setup (seconds),
`./crboost test -m cluster` submits one short SLURM job per configured tool (minutes).

The Python environment is a build product: `.uv/env/<python>-<hash of the lock's pins>`, built by `install.sh` when
it is missing and never modified afterwards, so a lock change builds a new one beside the old and a running server
keeps its own. `venv` is only a convenience link to the latest. `./crboost env` prints the path, `./crboost clean`
lists what is unused. Torch is not in it: the DL tilt filter's network runs in the `tilt_filter` container
(`filterTilts/deepLearning/predict.py`). Design: `docs/roadmaps/roadmap_39-python-environment-and-setup.md` S3 and S6.

## Configuration

- `config/conf.yaml` — main config (created from `config/conf.template.yaml` by `preflight.py`). Contains: `crboost_python` (leave empty: drivers run the interpreter running the server; preflight warns when it is set), local paths, SLURM defaults, `container_runtime` + `container_binds`, and per-tool execution config (container vs binary).
- `config/qsub.sh` — SLURM job submission template. Uses RELION-style `XXXextra1XXX`..`XXXextra8XXX` placeholders that get substituted with `SlurmConfig` fields at submission time.
- `config/protocols/<name>/` — Protocol bundles (`protocol.yaml` + `assets/`): the shape of a pipeline with its parameters, applied at project creation to make a REGULAR project (`services/protocols/`). A project created from one carries `ProjectState.protocol_origin` and a frozen `<project>/protocol/protocol.yaml`; the workspace Protocols view (foot-of-rail light, `ui/protocols_view.py`) shows protocol vs current parameters. Nothing about results lives in protocols. How to write one: `docs/protocols.md`. Job instances take their code defaults; there are no per-job template files.

## Architecture

### Entry point and request flow

`crboost` (sh launcher) → `crboost.py` (port, logging, banner) → `server_app.py` creates the `FastAPI` app, mounts `/static`, initializes `CryoBoostBackend`, passes it to `ui/main_ui.py` which defines NiceGUI page routes (`/` landing, `/p/<project>/<view>/<target>` workspace, `/workspace` redirect).

### Backend (`backend.py`)

`CryoBoostBackend` is the central facade. It holds singleton service instances and exposes async methods consumed by the UI layer. All services receive a back-reference to `CryoBoostBackend` at construction.

### Services layer (`services/`)

- **`project_state.py`** — `ProjectState` (Pydantic model) is the single source of truth for a project. Serialized as `project_params.json` in each project directory. A path-keyed in-memory registry (`_project_states`) ensures one `ProjectState` per project across browser tabs. `StateService` wraps persistence with an asyncio lock.
- **`job_models.py`** — `AbstractJobParams` base class and concrete param classes per `JobType` (e.g., `FsMotionCtfParams`, `TsReconstructParams`). Each declares `USER_PARAMS` (user-editable fields), `JOB_CATEGORY`, `RELION_JOB_TYPE`. The `jobtype_paramclass()` function maps `JobType` enum → param class.
- **`models_base.py`** — shared enums (`JobType`, `JobStatus`, `JobCategory`, `AlignmentMethod`) and base Pydantic models (`MicroscopeParams`, `AcquisitionParams`).
- **`configs/config_service.py`** — loads `conf.yaml` into a typed `Config` model. Singleton via `get_config_service()`. Provides `get_tool_config(tool_name)` with legacy alias support.
- **`computing/slurm_service.py`** — `SlurmConfig` model, presets, and SLURM job submission/query.
- **`computing/container_service.py`** — wraps shell commands with `apptainer exec` invocations based on per-tool config.
- **`scheduling_and_orchestration/pipeline_orchestrator_service.py`** — `deploy_and_run_scheme()` → `_submit_chain()`: allocates each job's `External/jobNNN` dir, writes its `job.star` and `run_submit.script`, and sbatches the jobs as a SLURM afterok chain.
- **`scheduling_and_orchestration/pipeline_runner.py`** — `reconcile_afterok()` (ticked by `pipeline_monitor.py`) reconciles job status from the exit markers, `squeue` and `sacct`; Stop and Cancel.
- **`configs/starfile_service.py`** — reads/writes RELION `.star` files.
- **`io_slots.py` / `path_resolution_service.py`** — typed I/O slot system for inter-job data flow and path resolution.

### Drivers (`drivers/`)

Python scripts that execute on compute nodes inside SLURM jobs. Each driver calls `driver_base.get_driver_context()` which loads `project_params.json`, looks up the job by `--instance_id`, and returns `(ProjectState, job_model, context_data, job_dir, project_path, job_type)`. Drivers then build and run tool commands (often via container wrapping).

### UI (`ui/`)

NiceGUI-based. `main_ui.py` defines routes. Key components:
- `routing.py` — addressable URLs. `View`/`Route`/`parse_route`/`route_to_path` are the pure URL↔(view, target) mapping; `apply_route` drives a route onto a built workspace *through the workspace `callbacks` dict*; `RouteWriter` (exposed as `callbacks["set_url"]`) writes the URL back. Deliberately imports nothing from `ui.*` at module level — half of `ui` imports it.
- `data_import_panel.py` — landing page: project creation/loading, data glob inputs.
- `workspace_page.py` — main workspace after project load.
- `pipeline_builder/` — pipeline configuration, job tabs, SLURM config, status polling.
- `job_plugins/` — per-job-type UI renderers (custom parameter forms).
- `ui_state.py` — `UIStateManager` per-tab state (stored in NiceGUI client storage).

### Key patterns

- **Instance IDs**: Jobs are keyed by `instance_id` string (e.g., `"tsReconstruct"`, `"templatematching__ribosome"`). The `__` separator denotes a species-specific instance. `JobType` enum holds the base type; instance_id may differ.
- **Singletons**: Services use module-level `_instance` + `get_*()` factory pattern (e.g., `get_config_service()`, `get_state_service()`).
- **Logs** (`services/log_setup.py`): `logger.info` lands in the log file only (`~/.crboost/logs/crboost-<port>.log`, 5 × 20 MB); what an operator watches the console for goes through `services.event_log.events`, project name first (`"%s · %s queued as SLURM %s"`). Code that runs on a timer logs at DEBUG, or the file fills with it.
- **Dirty tracking**: `ProjectState` has `mark_dirty()` / `is_dirty` / `save_if_dirty()` for deferred persistence.
- **RELION compatibility**: The system generates RELION-compatible directory structures, `default_pipeline.star`, and `job.star` files so projects can also be opened in RELION directly.
- **Warp's tilt-series model is more than the star.** A Warp XML holds what RELION's tilt-series star cannot: missAlign's local image warps (`GridMovementX/Y`), volume warps, level angles. Warp's own jobs (tsReconstruct, Warp export) apply them; a RELION program that cuts from the tilt images sees the star alone, 8–17 Å RMS off on the reruns (HOTFIXES H26). Every driver that hands RELION the tilt images calls `drivers/warp_bridge.carry_warp_geometry` first, which adds Warp's local geometry as RELION per-particle trajectories (`services/particles/warp_geometry.py`). Any new integration that reads tilt images or tilt-series geometry (M, an external tool) takes Warp's whole model, through the bridge or the XML, never the star alone. RELION's frame alignment (`frameAlign`, `drivers/frame_align.py`) replaces that model with RELION's whole one (refined tilt-series stars, its own motion) and marks the tomograms star (`cbGeometry`); the bridge hands a marked set on as it is, Warp Export refuses it, and its particles name no images until Subtomo Extraction cuts them anew (roadmap 53 §15.11). Warp's M (`mRefine`, `drivers/m_refine.py`) refines Warp's model in copies of the tilt series' Warp files in its own folder (`services/particles/m_population.py`); the set it hands back names those copies' per-tilt stars (M's per-tilt CTF in them, M's XML beside them). M also refines what neither the star nor the bridge can hold: per-tilt stage angles (the bridge and Warp Export refuse them) and a magnification and Zernike terms (no particle moves for them: Warp applies them where it back-projects, so RELION reconstructs without them, and its stacks carry none).

### UI reactivity patterns

NiceGUI's default refresh idiom — `container.clear()` followed by a full rebuild — destroys every child element along with its event handlers. If a user click is mid-flight against the old DOM, it gets silently dropped. If the cursor is hovering a row, :hover doesn't fire on the replacement until the cursor moves. Both compound when the rebuild is timer-driven and the cadence is short relative to typical click latency.

Two primitives in `ui/components/reactive.py` solve this:

- **`FingerprintedView`** — a container view whose `refresh()` is a no-op when nothing the render reads has changed. Subclasses implement `signature()` (cheap fingerprint of every state input the render touches) and `render()`. Polling timers tick on cadence but only cause a DOM rebuild when the signature actually moves. Used by `RosterWidget` (`ui/pipeline_builder/pipeline_roster.py`) and `_BackgroundTaskTray` (`ui/background_task_tray.py`).

- **`SingleFlight`** — guards an async handler against re-entry. `async with self.flight("key") as acquired: if not acquired: return ...` makes the second invocation a silent no-op while the first is in flight. Required for any handler that opens a dialog or does long async work — the trigger button can be destroyed and recreated mid-click by an unrelated refresh, so users may legitimately click "add" several times before one lands; without the guard each click queues another dialog. Used by `PipelineBuilderPanel.prompt_species_and_add`.

Conventions when writing new UI:

- **Polls observe state; they don't rebuild the DOM directly.** A timer's callback should mutate model fields (or trigger a `FingerprintedView.refresh()` which is signature-gated), never call `container.clear()` unconditionally.
- **Default poll cadence**: 3 s for cheap in-memory state observation, ~15 s for anything that touches disk. Sub-second timers must be replaceable by CSS animations or hard-justified.
- **Animations are CSS, never server ticks.** `@keyframes` in `ui/dashboard/css.py`, or an SVG carrying its own animation; never a `ui.timer` advancing a glyph.
- **Nothing heavy on the event loop.** One loop answers every click of every tab (`docs/architecture.md` "Where work runs"). Disk reads (stat, glob, star/JSON parsing) go to `asyncio.to_thread`; a render reads first, then builds. CPU-bound Python (renders, preview passes, filters, registration, thumbnails) goes to `run_in_worker(WorkKind.X, fn, *args)` (`services/workers.py`): a module-level fn, picklable args and result, never a `ProjectState`/job model/callback, state changes back in the server. A thread does not protect the loop from Python-level CPU work (the GIL). Array-job statuses come from `services/array_status_cache.py`, never a per-view scan. If a `signature()` does disk I/O, prefer mtimes or an in-memory cache; it runs on every tick. `services/loop_watchdog.py` logs any stall over 1 s as one line naming where it went, to the log file only; `CRBOOST_TRACE_STALLS=1` logs every stall over 250 ms with the loop's stack, on the console too.
- **Pictures rendered for display are `Render`s** (`services/renders.py`), shown in a `RenderSlot` (`ui/components/render_slot.py`): one render per key however many views ask, a skeleton with a restart button until it lands, filled in place. No module-level "already kicked" sets, no completion timers in a slot a rebuild can delete, no placeholder text, no "re-render" buttons.
- **Hidden views do nothing.** A view's pollers are `ViewScope.timer`s (`ui/components/reactive.py`): job pages use `ui_mgr.job_scope(instance_id)`, the roster `ui_mgr.view_scope("pipeline")`, whole views their `set_active` hook. Polled text is one element written only when it changed (`LiveLog`), never `ui.log` re-pushed per tick.
- **Edits update the model at once; what follows them waits for the typing to pause** (`Debounced`). A rebuild must never replace a field the user may be in (the SLURM section holds its rebuild while one of its fields has focus).
- **NiceGUI's slot stack is per asyncio task.** Work scheduled into a new task (`asyncio.create_task`, `Debounced`) that builds elements or submits a `BackgroundTask` captures `ui.context.slot` first and enters it.
- **Binary responses send `NO_GZIP`** (`services/http_responses.py`): NiceGUI's GZipMiddleware compresses everything else on the loop.
- **URL writes go through the History API, never `ui.navigate.to`.** A navigate rebuilds the page and throws away the lazily-built Journey / gallery / pick viewer, which is the entire cost the CSS-`display` view swap exists to avoid. `callbacks["set_url"](View.X, a, b)` (`ui/routing.py`) is the only way the address bar moves inside a workspace session. Order matters: the workspace's `_show` writes the bare route of the view just turned on (which `pushState`s, so Back walks views), then the view's own selection handler replaces it with the target-bearing one — and each such view must also write on *activation*, because re-entering an already-built view fires no selection handler.
- **Reactive bindings beat full rebuilds.** When a single attribute drives a single visual property, `bind_value_from(model, attr)` / `bind_content_from(model, attr)` updates only that property. `_status_widget` in the roster does this for the per-row status dot.
- **One control vocabulary.** Text buttons are `house_button()` (`ui/components/buttons.py`): slate outline, 10 px Plex Sans, 22 px tall, core look inline on the element; variants default / `accent` (the one primary action of a dialog/page) / `danger` (deletes). Form inputs in dialogs and registry-style pages are `house_field` / `house_number` / `house_text` / `house_select` (`ui/components/fields.py`): 9 px label left of a 16 px `.cb-field` box; `house_number` with `model=`/`attr=` binds through `numeric_forward` so int fields can't be corrupted by float values. Job-page parameter forms are the field geometry of `ui/job_plugins/_field_styles.py` (`.cb-select` scale): a 132 px label column in the slot voice, the box at S 64 / M 150 px / L to the column's edge (`box_style`), a 30 px unit column — one notch bigger than `.cb-field` by design; don't collapse the two scales. A path is `path_text(path, project)` (`ui/components/path_text.py`): cut in its middle to the width it has (the file name kept whole), a click copies the absolute path, hovering unrolls it; never an end-ellipsized label or a copy icon beside it. A checkbox is `house_check()` (`ui/components/check.py`: a 10 px box whose bottom edge is the label's baseline, `on_change` gets the event `ui.checkbox` sends), never `ui.checkbox`; a job's on/off parameter is a yes | no row (`toggle_field`). Never ad-hoc `color=primary` buttons or bare `ui.number`/`ui.input`/`ui.select` in dialogs. The `.cb-field`/`.cb-select` chrome and the app-wide spinner kill live ONLY in `ui/dashboard/css.py`; every page calls `ensure_assets_loaded()`. Actions sit WITH their inputs (`[button] [hint]`, no spacer-to-page-edge); dialog footers are a right-aligned `[Cancel] [accent action]` pair.
- **Mode-switch panels with different heights** get grid-stacked with inactive panels visibility-hidden (all inline styles) so the container holds the tallest panel's height and content below never jumps on a tab flip (`_stacked_panels` in `ui/template_workbench.py`).
- **Headers are the Rule ladder.** A section's head is `section_head(title, right=meta, accent=)` (or `section` / `config_section` / `panel_head`, `_field_styles.py`): the title in mono caps, a hairline running from it to the right edge, the meta (and any controls) at the far end; no fill, no corners, no rule *under* a title. The hairline is an element of the row (never in the stale shell); the Journey's class-built heads (`.cb-section-card-header`) get the same look from `ui/dashboard/css.py`. `accent=True` (a blue pip, title and rule) only for a section that asks to be looked at: a failed job's Log, Results. A group inside a section is `group_title` (sans 10.5 px semibold, no line). Blocks are still separated by a real vertical gap, and there is no explanatory hint tail beside a title when the content below makes it obvious (fold it into the title's tooltip instead). Inputs must sit flush with surrounding 9–10 px text: `.cb-field` boxes are 16 px tall — if a control towers over its label, it's wrong.
- **One box deep; the look is variables.** Inside a card (a view, the roster, a dialog) nothing draws a box again: a section is its Rule head and its content (`PANEL_STYLE` reads `--cb-section-*`). Colours, voices (`--cb-label-*`, `--cb-ctl-*`, `--cb-badge-*`, `--cb-th-*`), radii and states are `--cb-*` variables with today's value as the `var()` fallback; `_THEME_CSS` holds the defaults and `_HARBOR_MONO_CSS` the look (`ui/dashboard/css.py`, roadmap 51 §5–§6). New UI names a voice (`voice()` in `_field_styles.py`), never a font, case or hex.
- **A species is its mark; its colour is the canvases'.** `render_species_mark` (`ui/components/species_pill.py`)
  draws a species' glyph — the icosahedral capsid by default, or one it picked from `static/icons/species/` — in the
  theme's grey or a muted hue it chose (`SPECIES_GLYPHS`, `SPECIES_GLYPH_HUES`, `services/models_base.py`). The
  overlay colour (`SPECIES_OVERLAY_COLORS`) stays on the canvases it keys (tomogram overlays, the pick viewer,
  napari, the gallery) and the Species tab's one swatch, never on names, rows or chips.
- **CSS is delivered per client, never at setup.** `ui.add_head_html` outside a page (at module level or in `setup_app`) reaches no page: NiceGUI hands it to a client no browser gets, and 3.19 discards it with a startup warning (the two blocks that did so went in roadmap 63 S3; they had applied nowhere since NiceGUI 3.0). `ui/dashboard/css.py` (`ensure_assets_loaded()`) injects per client after `client.connected()`, over the socket, and always arrives. Rule: structural/load-bearing styling goes inline on the element; hover states, pseudo-elements and shared polish go in `ui/dashboard/css.py`. A component's script goes the same way: a constant run per client from `ensure_assets_loaded()` (`_TITLE_TIP_JS`, `PATH_JS`).

### Job page parameters — one renderer

Every user parameter declares where it stands on the job page in its Field: a `title`, and
`json_schema_extra=form(group, width, unit)` (`services/jobs/_base.py`; width S / M / L, none for
on/off); the class orders its groups (`PARAM_GROUPS`). `services/job_layout.layout_of` reads them;
`ui/job_plugins/params_renderer.render_params` draws every job from that. A plugin keeps only what
is its own, through hooks (`before`, `custom`, `replace`, `after`); a parameter that means nothing
under another's value says so in `shows_param` and its row is hidden, never rebuilt. A new user
parameter without a group lands in "Other" with a warning in the log.

### Job parameter tabs — species-derived fields

A particle-phase job tab (TM, Pick candidates, Subtomo extraction) shows three kinds of
field: the job's own parameters, its SLURM resources, and the handful of values it
inherited from the species when the instance was created (template, mask, symmetry, Ø,
extraction geometry). Those inherited values are still job parameters — snapshot at
creation, nothing auto-propagates — they just have a species-shaped default. So:

- **Weakly separated, not boxed.** The fields declare the species group on their Field
  (`form(SPECIES_GROUP, …)`, `services/jobs/_base.py`), so they are one more group of the
  *same* Parameters section, under its `group_title`. Never a nested `ui.card()` with its
  own header bar, icon and font scale — that reads as a second application pasted into the
  form. A plugin draws a row of its own through the renderer's hooks (`custom` /
  `replace`, `ui/job_plugins/params_renderer.py`), with `choice_row` when the options
  come from project state rather than a static `Enum`.
- **The tooltip names the species default**, so "why is this not what the species says"
  is answerable without leaving the tab. The notch on the roster's species tag (its hover
  lists the differences, `services/particles/species_jobs.job_drift`) is the other half of
  that story and must keep agreeing with it.
- **Absent is stated, never defaulted.** No template registered means the row says so and
  points at the Species page; it never silently picks the first file it finds.

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
- Bytecode: the server compiles into `~/.cache/crboost/pycache` (`pycache_setup.py`, imported first by
  `crboost`); drivers use the usual `__pycache__`; the venv's packages are compiled by `uv pip sync`
  (`compile-bytecode` in `uv.toml`)

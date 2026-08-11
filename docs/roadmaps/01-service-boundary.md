# Roadmap 01 — Service boundary: a real facade, and homes for QC/visualization logic

## The two questions this roadmap answers first

### Q1: Is there a downside to centralizing all services on the backend? Does it constrain us later?

Split the question in two, because "on the backend" means two different things:

- **Centralizing the *surface* (facade routes to services): no downside, actively helps later.**
  The facade's methods are the natural seam for everything the platform roadmap wants: a future
  control-plane API (`PLATFORM_ROADMAP.md` chapter 3's `ClusterBackend` split), a CLI, headless tests,
  the post-schemer reconciler. A method like `backend.extract_pick_list(project_path, ...)` maps 1:1
  onto a future HTTP endpoint; scattered `ui → services.*` imports map onto nothing. The one rule that
  keeps this future open: **services must never require the facade or any UI context** — they take
  explicit `project_path`/`ProjectState`/params. Drivers already prove this constraint is livable:
  they run on compute nodes importing services directly with no backend, and must keep doing so.
- **Centralizing the *implementation* (logic living in backend.py): real downside, already visible.**
  47% of `backend.py` (lines ~536-1412) is an unextracted curation-session subsystem. A facade that
  implements is a god object: untestable, merge-conflict-prone, and it makes the facade itself the
  thing you'd have to rewrite for the control-plane split. So: **facade = composition root + thin
  command methods; each domain gets a service; backend.py should trend toward ~400 lines of routing.**
  Corollary: stop exposing service objects as facade attributes (`backend.template_service.*` from UI
  is direct access with extra steps); expose operations.

The existing `services → backend` back-references (constructor injection of `self`) are a designed
cycle — acceptable for now, worth shrinking opportunistically (most services use only 1-2 things off
the backend), and the orchestrator replacement will naturally reshape it. Not this roadmap's fight.

### Q2: The services→ui inversion — load-bearing or safe to undo?

**Safe. It is ergonomic sugar, not architecture.** What's actually there
(`services/project_state.py:1165-1182`): the path-keyed registry `get_project_state_for(path)` is the
real mechanism and stays untouched. The zero-arg `get_project_state()` merely asks the browser tab's
`UIStateManager` for the active path (hence the `ui.ui_state` import), and **falls back to a blank
throwaway `ProjectState()`** when there's no client context. That fallback is the W2 ArtiaX bug class:
service/background code calling it silently operates on an empty project.
`services/aggregation_authoritative.py:11` already documents "never `get_project_state()`" as a
hard-won lesson.

Measured blast radius of the undo (grep, 2026-08-10): 61 call sites in 9 `ui/` files, **zero** in
services outside `project_state.py` itself; plus 4 uses of the equivalent `StateService.state`
property in `backend.py` (3) and `project_service.py` (1). So: move the zero-arg accessor to
`ui/current_project.py`, repoint the 9 UI files' imports (mechanical), convert the 4 backend/service
sites to `state_for(path)` — each of those is a latent blank-state bug today, so this is 4 bug fixes,
not 4 risks.

## Before → After

**Before:** 39/57 UI files import `services.*` directly; the facade is optional; persistence is
called from 28 UI sites with ad-hoc policy; dashboard aggregation, pixel-sanity math, and the
array-task status protocol live inside `ui/`; the curation subsystem lives inside the facade; the
core state accessor depends on a browser tab.

**After:** dependencies flow `ui → backend → services`, enforced by lint; QC/data-processing logic
(dashboard collectors, pixel chain, task status) lives in `services/` where drivers, tests, and any
future frontend can reach it; persistence policy has one owner; `backend.py` is thin routing.
**Gain:** UI files become what `ui/curation_session_dialog.py` already is (renderers over facade
calls); the scientific/QC logic becomes importable, testable, and reusable; the control-plane future
stays open.

## Stage 0 — gather first

- Enumerate the full `ui → services` import matrix (which UI file uses which service function) —
  the audit has counts; the migration needs the exact list per file. One grep session, recorded here.
- For each of the 28 `save_project()` UI call sites: classify *why it saves* (user action commit /
  post-mutation flush / defensive). The debounced saver in `ui/aggregation_merge_card.py:90` encodes a
  real perf constraint (full save too slow on hot paths) — that policy must survive the move into the
  facade, so document its trigger conditions before touching it.
- Confirm no NiceGUI dependency in `ui/dashboard/data.py` and `ui/dashboard/pixel_sanity.py:34-437`
  (audit says none; re-verify at move time — a stray `ui.notify` would need extracting first).

## Stage 0 record (gathered 2026-08-11, sandbox grep)

- **Zero-arg `get_project_state()` call sites** (post-roadmap-00 tree): 9 UI files —
  `aggregation_merge_card.py` ×15, `tomo_dashboard_dialog.py` ×11, `tilt_filter_panel.py` ×10,
  `io_config_component.py` ×11, `dashboard/data.py` ×4, `job_tab_component.py` ×3 (+1 local import),
  `status_indicator.py` ×2, `pipeline_roster.py` ×1 (+1 local import), `config_tab.py` ×1.
  Zero calls in `services/` outside `project_state.py` (the two `aggregation_authoritative.py`
  mentions are warning docstrings).
- **SURPRISE — 3 of the audit's 4 backend `.state` conversions are dead code:**
  `backend.get_job_parameters` (:1646), `backend.update_job_parameters` (:1674), and
  `backend.get_initial_parameters` (:1757) have **zero callers repo-wide** → deleted in stage 1
  instead of converted. Only `project_service.delete_job:120` is live (2 UI callers via
  `backend.delete_job`) → takes explicit `project_path` threaded from the UI callers.
- **SURPRISE — `set_project_state` (project_state.py:1169) has zero callers** → deleted in stage 1.
- **SURPRISE — 6 additional `.state` property uses in UI files** the audit didn't count:
  `pipeline_builder_panel.py` :260,:351,:378,:601 and `pipeline_roster.py` :221,:377 → repointed to
  the UI wrapper in stage 1.
- **`save_project` UI call sites: now 22, not 28** (tree moved since audit): tomo_dashboard_dialog ×7,
  tilt_filter_panel ×5, pipeline_builder_panel ×4, job_tab_component ×2, io_config_component ×2,
  template_workbench ×1, species_workbench_panel ×1. Classification deferred to stage 4.
- **NiceGUI check for stage 3:** `ui/dashboard/data.py` clean (no nicegui import; UI deps are only
  `task_utils` + the accessor stage 1 replaces). `ui/dashboard/pixel_sanity.py` imports nicegui at
  module top — the :34-437 pure band must be split from the renderers at move time as the audit said.
- **ui→services import matrix** (import statements per file, top offenders): tomo_dashboard_dialog 27,
  pipeline_roster 10, aggregation_merge_card 7, pipeline_builder_panel 6, tilt_filter_panel 5,
  dashboard/data.py 5, io_config_component 4, job_plugins/template_match 4,
  job_plugins/candidate_extract 4, data_import_panel 4; 39 files total import services directly.

## Stage 1 record (executed 2026-08-11)

Landed as planned (ui/current_project.py, 9 files repointed, 6 `.state` property uses repointed,
`delete_job` takes explicit `project_path` threaded from both UI callers, dead
`get_job_parameters`/`update_job_parameters`/`get_initial_parameters`/`set_project_state`/
`StateService.ensure_job_initialized` deleted, `get_project_state()` + `StateService.state` deleted).
Two deviations:

- **`StateService.save_project`'s no-path branch kept a contained tab-context resolve** (lazy
  `ui.ui_state` import inside the method, loudly commented): 22 UI sites still call bare
  `save_project()` and migrating them is stage 4's job. This is now the LAST services→ui inversion;
  stage 4 deletes it. Behavior change vs before: a path-less save with no client context now logs a
  warning instead of silently no-opping against a blank state (same net effect, visible).
- **Roadmap-00 fallout found during this stage:** ruff's F401 autofix had stripped load-bearing
  *re-exports* (`JobCategory`, `JobStatus`) from `services/project_state.py` — ImportError on boot.
  Restored with the `as X` redundant-alias idiom (autofix-proof). A repo-wide import-resolution sweep
  (scratchpad script: every internal `from X import name` checked against X's definitions) now passes;
  lesson for stage 7's import-linter: re-exports must use `as X` or `__all__`.

## Stage 2 record (executed 2026-08-11)

Landed as planned: the whole curation band (19 methods + `_CURATION_LIVE_STATES` + the three
`__init__` state fields, 876 lines) moved verbatim to `services/curation/session_service.py`;
`backend.py` keeps 15 same-named typed delegators (the 4 `_`-private helpers moved without
delegation — grep confirmed zero external callers, including of the state dicts). backend.py
1817 → 1064 lines. Notes:

- **The service takes explicit deps** (`server_dir`, `username`, `slurm_service` — the shared
  instance, preserving squeue-cache behavior; config via `get_config_service()`), NOT a backend
  back-reference: the band touched nothing else on `self`, so the new service is born conforming
  to Q1's "services never require the facade" rule. Lazy `services.visualization` imports kept
  lazy, verbatim.
- One line of pre-existing format drift rode along (`stop_curation_session`'s signature fits on
  one 120-char line); `ruff format` applied to both touched files only.
- One stale comment repointed (`ui/curation_session_dialog.py` referenced `backend._curation_loaded`);
  `artiax_bridge.py`'s `backend.send_chimerax_command` docstring mentions stay true via the delegator.
- Verification ceiling this session was ruff only (`/software` unmounted → venv python is a dangling
  symlink; lint + format + F821/F401 clean). Runtime check owed: boot + curation session launch +
  swap/save round-trip.

## Stage 3 scope record (gathered 2026-08-11, read-only)

Ordering is forced by the dependency chain `pixel_sanity → data → task_utils`: land as three commits
3a → 3b → 3c (each runnable).

**3a — `ui/components/task_utils.py` → `services/array_tasks.py`.** 153 lines, pure stdlib
(json/re/pathlib), zero nicegui — moves wholesale. Facts:

- Callers to repoint (5 files, 7 sites — 3 are pipeline_roster function-LOCAL imports, the
  lint-dodging kind): `aggregation_merge_card:35`, `array_task_tracker:27` (8 names, some `as _x`
  aliased), `pipeline_roster` :116 :133 :587, `dashboard/data.py:23`.
- Constants: task_utils HARDCODES `".task_manifest.json"` (:94) / `".task_status"` (:113) as
  literals; the named constants live only in `drivers/array_job_base.py:44-45`. Single-source:
  `array_tasks.py` defines `MANIFEST_FILENAME`/`STATUS_DIR_NAME` (and uses them);
  `array_job_base.py` replaces its literals with `from services.array_tasks import MANIFEST_FILENAME
  as MANIFEST_FILENAME, ...` — the as-X re-export is LOAD-BEARING (8 drivers import
  `STATUS_DIR_NAME` *from array_job_base*; stage-1 F401 lesson). Import direction is already proven:
  array_job_base sys.path-bootstraps and imports `services.*` on compute nodes today.
- Roadmap-04 coordination resolved: TaskStatusStore has NOT landed → stage 3 owns the constants.
  The `read_manifest`/`scan_statuses` duplication vs array_job_base's own manifest/status functions
  stays — that dedup IS TaskStatusStore, not this stage.

**3b — `ui/dashboard/data.py` → `services/dashboard_data.py`.** 709 lines; the one stage-3 move that
is not purely mechanical. Facts:

- Two UI deps: task_utils (fixed by 3a) and `current_project_state` ×4 — those 4 sites are the real
  work: `_job_dir_for` (:131, `state.job_path_mapping` fallback despite taking explicit
  `project_path`) gains an explicit `state` param → ~12 dialog call sites + 2 internal callers
  thread it; `has_any_previews_rendered()` gains `state` (sole caller `pipeline_roster:1535-1537`
  fetches and passes).
- **SURPRISE — `has_any_extract_jobs` + `has_any_dashboard_data` are DEAD** (repo-wide grep:
  definitions only) → delete, don't move (re-verify at execution).
- Public-API rename (drop `_`) covers exactly the cross-module surface: the dialog's 17-name import
  (:58) + strip's 2 (`_PREP_STAGES`, `_position_label`) + pixel_sanity's 5 (union ≈ 19 names;
  constants upcase → `SPECIES_OVERLAY_COLORS`, `PREP_STAGES`). Confirmed internal-only (keep `_`):
  `_PILL_STAGES`, `_PICK_LIST_GLYPH`, `_ARRAY_STAGE_OUTPUT_STAR` (stage-6 table candidate), and the
  per-TS status helpers.
- `aggregation_authoritative.py` :14 :55 :71 docstrings cite `ui.dashboard.data._job_dir_for` /
  `._resolve_species` → repoint the text to `services.dashboard_data`.
- `data.py`'s `services.tilt_series.build._infer_position` import becomes services→services (fine
  as-is; renaming that private is not this stage's fight).
- Shim: `ui/dashboard/data.py` re-exports the NEW names under the OLD `_names` for one release.

**3c — `ui/dashboard/pixel_sanity.py:33-437` → `services/pixel_chain.py`.** Facts:

- Pure band = 5 defs (`_read_template_apix_box`, `_parse_tomo_dimensions`, `_scale_tomo_dims`,
  `_compute_pixel_chain(project_state)` — state already explicit, `_apply_sanity_rules`); band greps
  CLEAN of nicegui/render-side names (`_fmt*`, `_UNIVERSAL_STAGE_KEYS`, `ui.`) → split line at
  :437/438 confirmed. Band's only cross-deps: 5 dashboard_data names (hence 3b first) +
  `services.templating.template_metadata` (already services).
- Renderers + `_fmt_*` + `_UNIVERSAL_STAGE_KEYS` + `_group_rows_by_species` stay in
  `ui/dashboard/pixel_sanity.py`; dialog repoint (:77): compute/apply from `services.pixel_chain`,
  de-prefixed `render_pixel_sanity_table` from `ui.dashboard.pixel_sanity`. Shim for moved names.

Runtime checklist (3a–3c together): boot; open an array-history project (`projects/pos9_10` or
`try2_after_pixShift`); roster per-TS chips (task_utils path); Journey dashboard sidebar strip +
pills (data path); pixel-sanity table with warnings (pixel_chain path); one array-job submit if
convenient (array_job_base import change).

## Stage 3 record (executed 2026-08-11)

Landed as scoped; repo-wide ruff clean, zero old-path imports left. Sizes: `services/array_tasks.py`
158, `services/dashboard_data.py` 682, `services/pixel_chain.py` 430; shims: `task_utils.py` 18,
`data.py` 50; `pixel_sanity.py` down to 267 (renderers only). Execution notes:

- **data.py had 2 MORE function-local task_utils imports (:455-456)** the scope's site count missed —
  caught by the post-edit grep sweep (9 sites, not 7). Reinforces stage 7's case: local imports dodge
  every static count.
- Dead `has_any_extract_jobs`/`has_any_dashboard_data` re-verified dead → deleted, not moved.
- All 12 dialog `job_dir_for` sites sat inside functions already holding `project_state` → threading
  was purely mechanical; no new accessor fetches anywhere. `has_any_previews_rendered(state)`'s one
  caller (roster) passes `current_project_state()` at the UI edge.
- `aggregation_authoritative`'s `_job_dir`/`_species_id_for_job` docstrings repointed; note their
  headless re-implementations could now collapse into `services.dashboard_data.job_dir_for`/
  `resolve_species` (both sides are headless now) — left for a deliberate later pass, not a move.
- Shims: `task_utils` (same-name `as X`), `data.py` (old `_names` via `__all__` re-export),
  `pixel_sanity` (re-exports + old-name alias for the renamed renderer). Delete after one release;
  stage 7's import-linter should ban importing them from new code.
- Pre-existing format drift observed (NOT formatted — not this change's lines): dialog (4 hunks),
  roster, merge_card, array_job_base. All new/rewritten files are format-clean.
- **Commit partition: 3a alone is committable; 3b+3c must land as ONE commit** — `job_dir_for`'s
  signature change breaks an unmodified dialog at runtime (shim can't paper over an arity change),
  and the dialog carries 3b and 3c imports together.

## Stages (each committable)

1. **Undo the inversion.** *(DONE 2026-08-11 — record above.)* Add `ui/current_project.py` with `current_project_state()` (tab-context
   resolve → `get_project_state_for`); repoint the 61 UI call sites; convert the 4 backend/service
   `.state` uses to explicit `state_for(path)`; delete `get_project_state()` and the
   `StateService.state` property from `services/`. Blank-state fallback lives on in the UI wrapper
   only (landing page legitimately has no project).
2. **Extract `CurationSessionService`** *(DONE 2026-08-11 — record above.)* (`backend.py:536-1412` →
   `services/curation/session_service.py`). Pure move: the facade keeps same-named delegating methods
   (UI callers unchanged). The session registry file, ssh/REST plumbing, and save/load logic move wholesale.
3. **Move the UI-resident services** *(DONE 2026-08-11 — scope + execution records above)* (pure moves
   with thin re-export shims for one release):
   - `ui/dashboard/data.py` → `services/dashboard_data.py` (the collectors; `ui/dashboard/` keeps
     rendering only). Drop the `_`-prefixes on what is now a public API.
   - `ui/dashboard/pixel_sanity.py:34-437` → `services/pixel_chain.py`; renderers stay.
   - `ui/components/task_utils.py` → `services/array_tasks.py`, single-sourcing
     `MANIFEST_FILENAME`/`STATUS_DIR_NAME` with `drivers/array_job_base.py` (coordinate with
     Roadmap 04's `TaskStatusStore` — whichever lands first owns the constants).
4. **Persistence through the facade.** Add `backend.save_project(project_path, *, force=False,
   debounce=False)` embodying the card's debounce policy; migrate the 28 UI call sites; make direct
   `save_project` imports from `ui/` a lint error (see enforcement below).
5. **Job-lifecycle strays out of UI:** `ui/tilt_filter_panel.py:185 _finalize_pipeline_output` →
   `services/jobs/tilt_filter.py` (the manual-label path is the job's real output producer);
   `ui/tomo_dashboard_dialog.py:2939 _handle_extract_list`'s poll-loop body → the existing
   `backend.extract_pick_list` path; `ui/aggregation_merge_card.py:117 apply_aggregation_overrides` →
   `services/aggregation_authoritative.py` (note: `path_resolution_service.py:590` has a comment
   depending on this function's behavior — read it first).
6. **One `JobSpec` table.** Frozen dataclass per job type (param class, display name, phase,
   dependencies, plugin, driver module) in `services/jobs/spec.py`, replacing the 8 unsynchronized
   tables (`jobtype_paramclass` + `PIPELINE_ORDER` + `JOB_DISPLAY_NAMES` + `PHASE_JOBS` +
   `JOB_DEPENDENCIES` + `_PREREQUISITES` + `_ARRAY_STAGE_OUTPUT_STAR` + plugin `_REGISTRY`).
   Build it additively: new table first, then convert readers one commit at a time, delete old tables
   last. Also stop rebuilding the mapping per call (`services/jobs/__init__.py:30`).
7. **Enforcement.** Add a tiny import-linter (a ~30-line AST check in `preflight.py` or a ruff
   `flake8-tidy-imports` banned-api config): `services/` may not import `ui.*`; `ui/` may not import
   `services.project_state.save_project` (list grows as stages land). Without this, the boundary
   erodes again — 51 of today's violations are function-local imports that dodged review.

## Modern-Python weave-in

- `JobSpec` = `@dataclass(frozen=True, slots=True)`; registry as `Final[Mapping[JobType, JobSpec]]`.
- The callback bags threaded through pipeline-builder components (10 signatures, 18 magic keys) →
  one `PanelCallbacks` Protocol (or frozen dataclass) when stage 4/5 touches those files.
- Facade methods gain return annotations as they're touched (they're the API surface; Roadmap 03's
  result type will thread through here).
- `functools.cached_property` for the facade's lazily-built service handles instead of init-time
  construction where cheap.

## Runtime checklist

Per stage: boot, open project hub, open workspace, run the moved feature (curation session launch for
stage 2; Journey dashboard for stage 3; save-heavy flows — species edit, merge card — for stage 4;
tilt-filter manual label + list extraction for stage 5). Stage 1 specifically: verify background tasks
(thumbnail generation, ArtiaX auto-ingest) still persist state — those were the historical blank-state
victims.

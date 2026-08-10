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

## Stages (each committable)

1. **Undo the inversion.** Add `ui/current_project.py` with `current_project_state()` (tab-context
   resolve → `get_project_state_for`); repoint the 61 UI call sites; convert the 4 backend/service
   `.state` uses to explicit `state_for(path)`; delete `get_project_state()` and the
   `StateService.state` property from `services/`. Blank-state fallback lives on in the UI wrapper
   only (landing page legitimately has no project).
2. **Extract `CurationSessionService`** (`backend.py:536-1412` → `services/curation/session_service.py`).
   Pure move: the facade keeps same-named delegating methods (UI callers unchanged). The session
   registry file, ssh/REST plumbing, and save/load logic move wholesale.
3. **Move the UI-resident services** (pure moves with thin re-export shims for one release):
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

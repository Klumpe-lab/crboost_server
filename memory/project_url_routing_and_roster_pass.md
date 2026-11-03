---
name: project_url_routing_and_roster_pass
description: "roadmap 17 addressable /p/<project>/<view>/<target> URLs S0-S5 CODE-COMPLETE 2026-09-08 (built on the 2026-09-07 projects-roster redesign); both PENDING RUNTIME"
metadata: 
  node_type: memory
  type: project
  originSessionId: 0af32c9b-7095-4e73-aad5-c620a73fba4c
  modified: 2026-09-08T10:36:10.713Z
---

**Branch `protocols`. Scoped 2026-09-07, built 2026-09-08. All PENDING RUNTIME**
(`ruff check .` + `ruff format --check` green on every touched file; `python -c "import main"`
and `python check_boundaries.py` OWED — no interpreter in the sandbox, see [[reference_hpc_env]]).

## Roadmap 17 — addressable URLs (S0–S5 code-complete 2026-09-08)

`docs/roadmaps/completed/roadmap_17-url-routing.md` §6b holds the full build log and §7 the 15-step runtime
checklist. New: `services/project_resolve.py`, `ui/routing.py`. The rest is wiring.

Design points that are NOT obvious from the code and cost real thought:

- **push vs replace**: `pushState` when the *view* changes, `replaceState` when a view refines its
  own target. Back walks views, not tilt-series clicks. Requires **coarse-then-fine** write order —
  `_switch_to` writes the bare view route, the view's own handler replaces it with the target one.
- Because of that ordering, a view must ALSO write its URL **on activation** (`on_journey_active`,
  the Species page's `_set_active`): re-entering an already-built view fires no selection handler,
  so the target would silently drop out of the URL.
- `apply_route` runs on a **live** workspace on every Back/Forward, not just a fresh one. The view
  entry points are toggles that switch *away* when re-entered → `callbacks["current_mode"]` +
  `_enter()`. This was the one thing the scoping doc got wrong.
- `ui/routing.py` imports nothing from `ui.*` at module level (half of `ui` imports it). Hence
  `SPECIES_TABS` / `JOURNEY_SECTIONS` are mirrored constants; job subsection keys are NOT mirrored
  (`_job_tab_keys` reads `MonitorTab` + `get_extra_tabs` at validation time, so plugin tabs stay
  addressable).
- **There is no `io` or `slurm` job tab** — those are `ui.expansion` blocks inside Config. Real keys:
  `config` / `logs` / `files` + `tasks` on the array jobs.
- NiceGUI's event modifiers do **not** include Vue's `.exact` (`nicegui/event_listener.py:32`), so
  "an `<a href>` whose plain click is intercepted but ⌘-click is not" is inexpressible. The landing
  chevron therefore became a plain `<a href>`; `ProjectsOverview._travel` deleted.
- `ui.clipboard.write` is secure-context-only and **silently `console.error`s over plain http** (how
  this server is reached). The rail copy-link is a menu showing the selectable URL + a copy icon.
- `ui.run_javascript` in nicegui 3.0.3 takes **no `respond=` kwarg** (only `timeout`); the two
  existing `respond=False` call sites (`ui/components/copyable.py:26`,
  `ui/pipeline_builder/io_config_component.py:146`) are pre-existing bugs in `except` fallbacks.

## 2026-09-07 roster/landing pass (unchanged, still pending runtime)

- `ui/projects_overview.py` row: name + full path + copy icon / `N TS · date · jobs · bar · species
  dots` / full-height chevron. Header sorter `ORDER [Last opened | Created]` + `SPECIES` filter,
  sorted INSIDE each owner section.
- "Last opened" = new per-user MRU `UserPreferences.recent_projects` + `add_recent_project` /
  `recent_project_rank`, written by `remember_project_opened` (`ui/open_project.py`).
- `backend._scan_for_projects_sync` emits `species: [{id,name,color}]`.
- **Landing h-scroll root cause**: `ui/dataset_overview_panel.py` `COL_WIDTHS` = 466 px of fixed grid
  columns inside `overflow: hidden`. Fixed with `overflow-x: auto` + `min-width: 0` / `max-content`.
- Landing type scale → `SZ_TITLE/SZ_BODY/SZ_META` 12/10/9; hub params pane 380 → 480 px.

See [[feedback_write_roadmaps_directly]], [[feedback_dont_gate_stages_on_runtime]],
[[reference_nicegui_flexstart_shrinkwrap]], [[feedback_ui_chrome_conventions]],
[[feedback_never_silent_defaults]].

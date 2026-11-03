---
name: project_landing_overview_rework_2026-10-19
description: "Landing = projects roster full-page + 'New project' view switch; compact roster is a table with more columns; roster render/patch + project-open speedups; BUILT 2026-10-19, NOT RUN"
metadata:
  type: project
---

Built 2026-10-19 on user request ("make projects overview the default view … 'new project' button … more
columns … switching into a project takes ~1.5 s, lazy loading"). Ruff-clean, NOT RUN.

- **Landing** (`ui/main_ui.py` root = `position: fixed; inset: 0` flex column; `ui/data_import_panel.py`): two
  views, each card's title is the same `cb-seg-title` segmented switch `Projects overview | New project`.
  Overview fills to the bottom edge (`ProjectsOverview(fill=True, title_slot=…)`); setup form unchanged inside,
  `max-width: 880px`. The hub dialog uses `fill=True` too.
- **Roster** (`ui/projects_overview.py`): compact view = table with a column header row (outside the scroll
  area) and columns Owner · Furthest job · Jobs (ok/total + red failed) · TS · Tomos · Picks · Species · Å/px ·
  kV · Dose · Activity · Size · Trash · Status; width tiers `cb-pc-t1..t3` hide columns via container queries
  on `.cb-proj-roster` (ui/dashboard/css.py). Full view line 4 got the same facts. Backend scan adds
  `furthest_done`, `n_picks`, `n_pick_lists`, `protocol`.
- **Render cost:** `_render_list` patches only rows whose fingerprint changed (keys equal → `_patch_list`,
  move+delete); hover card content built on first row hover (JS emits once per row); card anchored after the
  name (zero-width anchor) since the roster now spans the page; size breakdown = one `ui.html`; simple
  tooltips → native `title` (`_set_title`).
- **Project open:** "Opening <name>…" spinner right after connect; Species page built on first show
  (`_ensure_species_page` in workspace_page); hub preview's TS table loads the registry only when expanded
  (was a 1–3.6 s on-loop stall per preview); `TiltSeriesRegistry.load` reads sidecars with 16 threads and
  parses with `model_validate_json` (dict-path fallback, warns once).

**How to apply:** runtime check = landing look + `opened in` lines in the log file; if open is still slow,
the split (load vs build) says where. Related: [[project_ui_latency_roadmap34]],
[[project_projects_overview_disk_usage]], [[project_quiet_logs_staged_data_import]].

**2026-10-21 hub speed + sort memory (BUILT, NOT RUN):** in-project hub dialog kept per page (`RosterWidget._hub`,
reopen = `dialog.open()` + `ProjectsOverview.resume()`); any new roster renders from `backend.last_project_scan(base)`
then rescans; `resolved_path` computed in the scan thread (was ~5 `Path.resolve()` per row on the loop — watchdog
lines named `_is_path`). Sort persisted in prefs `projects_sort_mode` / `projects_column_sort`. A project hop is a new
page, so its first hub open still pays the row build (~1–2 s seen in watchdog lines for ~40 rows).

**2026-10-21 "Scanning… takes forever" (BUILT, NOT RUN):** watchdog lines showed 3.3 / 5.8 / 4.6 s loop stalls in
`user_prefs_service.py:<listcomp>` = the recent-path existence prune in `load_from_app_storage` (every page load and the
hub; ~35 paths on /groups mounts). Prune removed. Project scan now 16 threads per project. Tomogram wall logs
`<project> · tomograms shown in X s (read, render)` — next step for the slow wall depends on that split.

---
name: project-workspace-cards-closeable-job-2026-10-22
description: "Workspace macro pass 2026-10-22 — roster + views as rounded cards on a desk with 8 px gaps, closeable job page, active row shares the job title line look, rail avatar opens the projects hover card; NOT RUN"
metadata:
  node_type: memory
  type: project
  originSessionId: b56ac386-c241-4d27-a130-436fd47f0c55
  modified: 2026-10-22T09:20:15.005Z
---

Built 2026-10-22 on main, ruff-clean, NOT RUN in a browser:
- `ui/workspace_page.py`: outer row on `--cb-desk` (#e9eef4); roster + every view column are cards (`_CARD_EDGE`, radius 6); main area padding/gap `_PANEL_GAP_PX` 8; view splitter moved into the gap (css `.cb-view-splitter` left -8px); old `_VIEW_EDGE` border/shadow logic removed.
- Closeable job page: pipeline frame's × calls `callbacks["close_job"]` (panel `close_job` → `ui_mgr.clear_active_instance()`); frame shows only while `_job_open` (`set_job_open` callback from `_show_job` / rebuild). Rebuild leaves `active=None` closed. Empty area says "No job open."
- Active roster row = `--cb-head` fill + 2 px `--cb-accent` left tick, same as the job page's `_JOB_HEAD_STYLE` title line.
- Rail avatar: `.tooltip(name)` replaced by `project_card_menu` (ui/projects_overview.py), content from new `backend.scan_project()` (scan_one lifted to `_scan_project_sync`). Caret variant `.cb-proj-card--right`.
- Tomogram wall: skeleton tiles instead of the "Reading tomograms" spinner; `WAIT_SPINNER` SVG in ui/styles.py.

**Why:** user found side-by-side title bars "blocky and crammed", roster disconnected from the open job, avatar tooltip useless.
**How to apply:** if the runtime pass shows problems, these are the anchors. Related: [[project_style_themes_2026-10-22]].

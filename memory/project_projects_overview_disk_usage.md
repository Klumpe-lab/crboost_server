---
name: project-projects-overview-disk-usage
description: Roadmap 25 (Projects Overview rows + disk sizer) CLOSED 2026-10-15 — maintainer: done; doc in docs/roadmaps/completed/
metadata:
  node_type: memory
  type: project
  originSessionId: 62386a05-44b6-43e8-ade9-57330db75bb1
  modified: 2026-10-02T12:53:14.179Z
---

**CLOSED 2026-10-15** (maintainer: done). Doc moved to `docs/roadmaps/completed/roadmap_25-project-disk-usage.md`.

Roadmap 25 (`docs/roadmaps/roadmap_25-project-disk-usage.md`) built 2026-10-02, parts A + B, **NOT RUN**
(ruff only; no interpreter in the sandbox).

- A: ProjectsOverview rows = name+status / project path / raw-data path / footer (delete, TS, date, size).
  Avatars, species dots, progress bar, copy icon, transfer button removed; delete owned by the widget via
  `backend.delete_project`, so landing + in-workspace switcher behave identically; switcher dialog maximized.
- B: `services/project_disk_usage.py` — lstat walk (st_blocks, no symlink follow, = du), buckets by job phase,
  cached at `<project>/.disk_usage.json`, single background worker kicked from `backend.scan_for_projects`.
- Ownership transfer now has NO UI entry point (backend method kept) — user accepted the removal.

**Why:** the user found the roster "too colorful and motley" and wanted per-project disk visible for cleanup
(9.7 TB across the 412 projects; 2.4 TB is unreferenced fs_motion `.staging` copies — separate driver fix, not built).
**How to apply:** runtime check list is the roadmap's "User check"; related [[project-ownership-sharing]].

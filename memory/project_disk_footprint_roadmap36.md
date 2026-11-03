---
name: project_disk_footprint_roadmap36
description: "Roadmap 36: trash view (S1) + .staging move-not-copy and scripts/clear_staging_duplicates.py (S2) BUILT 2026-10-18 (not run); removing a job only moves it to Trash/; S3–S5 proposed"
metadata:
  node_type: memory
  type: project
  originSessionId: 9fbf128b-7fa2-43ef-b012-866c3b273ea5
  modified: 2026-10-18T15:05:17.750Z
---

Removing a finished job moves its dir to `<project>/Trash/<Top>_<jobNNN>` — space is NOT freed (SK held
1.17 TB after its particle jobs were removed 2026-10-18). S1 built 2026-10-18 (ruff-clean, NOT RUN):
`services/project_trash.py`, backend `list_project_trash` / `clear_project_trash`, roster trash chip + dialog
in `ui/projects_overview.py`, `DiskUsageService.trash_cleared`.

Measured: fs_motion and alignment `.staging/` keep full copies of collected outputs (Grid2 job002: 191 of
381 GB) — S2 proposes move-not-copy (zero UX cost, ~20 % of a project). S3 halves only with denoising, S4 TM
"free maps" / slab-only search (PyTOM output size unverified), S5 skip-Trash checkbox. Decisions owed in the
roadmap §4. Another session edited backend.py and projects_overview.py at the same time (acquisition
columns, queued status) — see [[reference_worktree_per_session]].

S2 built 2026-10-18 (ruff-clean, NOT RUN; no python in the sandbox): `collect_fs_outputs` and
`collect_per_ts_outputs` move instead of copy; `scripts/clear_staging_duplicates.py` (stdlib, dry run by
default, `--delete`) clears existing duplicates only for `.ok` TS with a same-size collected copy.

Follow-up built 2026-10-18 (ruff-clean, NOT RUN): roster deletes no longer block. Project delete =
background task in the existing tray (bottom-right; user asked "bottom left" — told them, offered to
move): `services/project_delete.py` renames to `.<name>.deleting-<hex>` (backend scan skips it), deletes
job dir by job dir, project_params.json last, renames back on failure/cancel. Trash Clear is live before
the sizing listing returns; with no listing it clears everything. Rows/chips hide via running task
dedup keys (`delete-project:` / `clear-trash:` + path); `snapshot_for_project` now includes
project-less tasks. Runtime check in roadmap 36 S1 follow-up bullet.

---
name: reference-afterok-skips-sync-side-effects
description: "Side effects hung off sync_all_jobs are DEAD on this deployment — conf.yaml sets use_afterok_orchestrator globally, so reconcile_afterok runs instead"
metadata: 
  node_type: memory
  type: reference
  originSessionId: ea97ec0a-49f5-4aed-a2e6-a5a8f80052a0
  modified: 2026-08-28T07:41:26.963Z
---

`config/conf.yaml` has `use_afterok_orchestrator: true` globally, so `PipelineMonitor`
dispatches every project to `reconcile_afterok` and `sync_all_jobs` returns `{}` on its
first line. Any job-status side effect wired into `sync_all_jobs` therefore never fires
on a real run — it looks alive in the code and is dead in production.

Found 2026-08-28 via the Journey's per-tilt hover cards losing their tilt images:
`_kickoff_tilt_thumbnails` (fsMotionAndCtf → SUCCEEDED renders `TiltFilter/png/*.png`)
existed only on the schemer path, so no project ever got thumbnails unless it had a
tilt-filter job AND someone opened that panel. Fixed by moving the logic to
`services/tilt_series_service.ensure_tilt_thumbnails()`, calling it from BOTH reconcilers,
and letting the Journey self-heal (`_tilt_thumbs` in `ui/tomo_dashboard_dialog.py`) for
runs whose SUCCEEDED edge no server was around to observe.

**Why:** the two reconcilers are not mirror images — `reconcile_afterok` was written as a
pure status reader and inherited none of `sync_all_jobs`'s side effects.

**How to apply:** when adding anything that reacts to a job status transition, wire it into
`reconcile_afterok` first (that's the live path) and add a consumer-side self-heal for the
already-finished-project case — a status EDGE is observed at most once, ever.

Related: [[project_pipeline_state_desync]], [[project_orchestrator_replacement]],
[[feedback_background_task_no_client_context]]

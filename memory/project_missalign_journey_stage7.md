---
name: project_missalign_journey_stage7
description: "Roadmap 21 stage 7a+7b (Journey Refinement section, warp strip, recon blink) built 2026-10-27, NOT RUN; warpylib sign convention"
metadata:
  node_type: memory
  type: project
  originSessionId: 11ab217d-d11d-4622-ba0f-e5b5b256a740
  modified: 2026-10-27T09:29:25.485Z
---

Built 2026-10-27 on branch `roadmaps`, not run: Journey section `ts_refine` (ui/tomo_dashboard_dialog.py), ui/dashboard/warp_strip.py, `refinement_changes`/`movement_grid` in services/analysis/alignment_changes.py, blink in `_render_recon_versions`. 7c (iteration scrubber) open. Same session: layer list's two tables share columns (ui/components/layer_menu.py `_Columns`).

Verified from warpylib source (raw.githubusercontent.com/warpem/warpylib/main/warpylib/tilt_series/positions.py, cubic_grid.py): position = projection + AxisOffset − GridMovement; nodes at margin + i(1−2·margin)/(N−1), X fastest, Z = XML tilt row. So whole-tilt shift = ΔAxisOffset − Δgrid mean (a handoff said "plus").

**Why:** the sign decides every arrow's direction; re-deriving it costs a source hunt (no warpylib locally, no python in the sandbox).
**How to apply:** runtime check owed on MyrGFP_Grid2 job022 Position_6; see [[project_agg412_missalign_rollout]].

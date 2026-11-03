---
name: reference-412-dataset-locations
description: "Canonical paths for the 412 viral-particle comparison (crboost_server vs cryoboost v1 / the PI's ground truth), including manual picks and template/mask files."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 21982a9c-b83c-43ad-906d-a06e1aa36cdc
  modified: 2026-10-04T16:52:32.812Z
---

For the 412 viral-particle comparison work:

**User's refactored project (crboost_server):**
- Root: `/groups/klumpe/crboost_data/GT/`
- Processed: 20 tilt-series, named `GT_Position_X_Y` (e.g. `GT_Position_10_2`)
- Pipeline: 9 jobs (Import + 8 External), all `Succeeded` as of 2026-05-15

**Ground-truth project (The PI, cryoboost v1):**
- Root: `/groups/klumpe/user/<pi>/Processing/412/try1/`
- Processed: 41 tilt-series, named `Position_X_Y_Z` (extra suffix vs user's naming)
- Pipeline: full RELION 4 + cryoboost v1 + downstream Class3D/Refine3D refinement
- **Manual picks**: `/groups/klumpe/user/<pi>/Processing/412/try1/ManualPicking/Position_*_6.20Apx.star` (21 files, ~507 picks total, at binning apix 6.2 Å — matches user's TM binning).

**Templates / masks:**
- Original full-res template: `/groups/klumpe/user/<pi>/Processing/412/try1/Class3D/job121/run_it015_class001.mrc` (apix 1.55, ~360 MB)
- User's downsampled templates: `/groups/klumpe/crboost_data/GT/templates/412/` — `run_it015_class001_apix6.20_box96_{black,white}.mrc` plus spherical mask `_sphere_d575_s5.mrc`
- the PI's reference templates: `/groups/klumpe/user/<pi>/Processing/412/try1/templates/ellipsoid_550_550_550_apix6.2*.mrc` (box128, isotropic-axis "ellipsoid" — actually spherical at 550 Å radius)

**Tilt-series cross-project mapping:** Source-of-truth is the mdoc `DateTime` + `SubFramePath` basename, NOT the TS name. the PI renames frames with a double-prefix (e.g. `Position_10_2_2Position_10_2_001_…`) so name-based mapping is unreliable. mdoc lines `DateTime = 17-Oct-2025 22:47:50` + `SubFramePath = …_20251017_224748_EER.eer` are identical between projects for shared TS — that's the comparison key.

All 20 of user's TS have ground-truth matches in the PI's 41 (by mdoc).

**Update 2026-10-04:** `/groups/klumpe/crboost_data/GT/` no longer exists. The prepared copies now live in
`/groups/klumpe/crboost_data/agg_20251113_412/templates/412/` (Class3D-derived, box128 @ 6.20, black/white +
`_sphere_d575_s5` mask) and `.../templates/412blob/` (ellipsoid, lp45). the PI's `try1/templates/` and
`Class3D/job121/` are readable from the sandbox. The full reprocessing (`AGG412_*`) has no templates/ dir yet.
See [[project_agg412_reprocessing_2026-10-04]].

NEVER write to /groups/. Read-only.

Related: [[project-412-tomohand-discrepancy]], [[reference-cryoboost-v1-location]].

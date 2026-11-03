---
name: project_blank_tilt_rays
description: "The 412 \"aliasing\" (agg_SK_20251017_412 Pos34_5) is back-projection rays from beam-blocked tilts that survive ts_import's min_intensity=0 coin flip — not aliasing; roadmap 22"
metadata:
  node_type: memory
  type: project
  originSessionId: 51985138-d697-4180-bfb9-5a80704abfa7
  modified: 2026-09-29T08:19:55.151Z
---

Investigated 2026-09-29 (read-only). Position_34_5's five top tilts (+56.89…+68.89°) are blank (mdoc mean
0.01–0.42 counts vs ~600–820). No gain/defect correction → they hold only fixed hot pixels → Warp normalises
them → each back-projects as a straight ray; in XY the ray cross-sections look like elongated bullseyes with
Gibbs rings ("aliasing" to the eye). No real-space binning anywhere; pixel sizes consistent.

**Why sites differ:** WarpTools ts_import keeps a tilt iff `mean >= min_intensity·cos·MaxAvg·0.999`; crboost
passes 0.0 → for blank tilts it is the sign of float16 noise. Same data/SIF on another CBE GPU node
(`post_handedness_fix`) kept 1 of 5. Munich's GPUs can keep none. Also the May code never applied the tilt
filter's verdict (all five were flagged bad). AreTomo tilt-offset search went NaN on 34_5 only.

**Why:** the user thought it was aliasing and cluster-dependent; it's data (blocked beam) + a threshold at 0.
**How to apply:** rays/bullseyes in one tomogram → check mdoc MinMaxMean for ~0-count tilts first. Fix plan =
deterministic blank-exposure exclusion (<1 % of series median) at registry ingest (roadmap 22). Side finding:
AreTomo tilt offsets −5…−15° at CC ≤ 0.013 on the other series, several pinned at the search edge.
Related: [[project_warp_ts_import_filter]].

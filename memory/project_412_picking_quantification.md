---
name: project-412-picking-quantification
description: "2026-05-16 — on the 412 dataset, crboost_server auto-picks ~16 per TS (mostly noise, tight CC distribution); cryoboost v1 the PI's manual ground truth is ~1-3 per TS. ~8x false-positive rate consistent with the divergence cascade."
metadata: 
  node_type: memory
  type: project
  originSessionId: 21982a9c-b83c-43ad-906d-a06e1aa36cdc
---

Quantitative picking outcome on the 412 viral-particle dataset (2026-05-16 comparison):

| Source | Total picks | Per TS | CC range | Interpretation |
|---|---|---|---|---|
| crboost_server auto (job008 extract) | ~326 across 20 TS | ~16 | 0.049 – 0.069 | Tight CC distribution — no clean signal/background separation; mostly noise above permissive cutoff |
| cryoboost v1 auto (job009, conservative cutoff) | ~2 visible in candidates.star | n/a | ~0.065 | `NumberOfFalsePositives=1, maxNum=10` — only the strongest match per TS |
| cryoboost v1 manual (ground truth, 21 .star files) | ~45 across 21 TS | **~1-3** | n/a | The PI manually validated; this is the trustable count |

**Verdict:** crboost_server is producing ~8x more picks per TS than the manual ground truth, almost certainly false positives. The fact that the *top* CCs are comparable (user 0.0686 vs the PI 0.0649) but the *distribution shape* is flat — user keeps picks all the way down to ~0.05 — is the signature of TM finding nothing real and the cutoff capturing noise.

This is consistent with the divergence cascade documented in [[project-412-divergence-cascade]]:
- Wrong tomogram chirality (TomoHand +1 vs -1) makes detail-matched template fail
- User uses Class3D-detail template (chirality-sensitive); the PI uses ellipsoid (chirality-insensitive)
- User's cutoff is permissive (catches noise)
- the PI's cutoff is conservative (`NumberOfFalsePositives=1, maxNum=10`)

**Suspicious coordinate convention in the PI's manual pick files:** `_rlnCenteredCoordinateXAngst` values are 21000-25000 Å, well outside the expected ±3174 Å range for a centered tomogram of 1024×1024×512 at apix 6.2. Either non-centered convention (origin at corner, plus offset), unbinned source-frame Å, or stage-position units. Need the PI to clarify before using as quantitative ground truth for spatial coincidence comparisons — but the **count** comparison stands regardless.

**Verification path:**
1. Apply P1 fixes (TomoHand flip + defocus set_flip) per [[project-412-divergence-cascade]]
2. Re-run from Import on 2-3 TS
3. Re-extract picks with conservative cutoff (NumberOfFalsePositives=1, maxNum=10)
4. Count picks per TS — should drop from ~16 toward the PI's ~1-3
5. If 1-3 picks land where the PI manually picked, we've recovered correctness

Related: [[project-412-divergence-cascade]], [[project-412-tomohand-discrepancy]], [[reference-412-dataset-locations]].

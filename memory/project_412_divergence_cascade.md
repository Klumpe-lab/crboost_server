---
name: project-412-divergence-cascade
description: "2026-05-16 — stage-by-stage comparison of crboost_server vs cryoboost v1 on the 412 dataset surfaced a 7-item cascade of divergences likely producing user's bad picks. TomoHand is just one layer. FIXED 2026-05-16 per user; keep memory for historical context."
metadata: 
  node_type: memory
  type: project
  originSessionId: 21982a9c-b83c-43ad-906d-a06e1aa36cdc
---

> **STATUS (2026-05-17):** User reports the cascade was fixed 2026-05-16. Keep this note as the historical record of the diagnosis; downstream symptoms (e.g., agg_20260122_412/job008 producing 0 picks on 2026-05-17) are no longer attributable to the cascade — investigate as a separate issue. Do not silently rely on these layers being unfixed.

The 412 viral-particle bad-picks investigation (2026-05-16) found cryoboost v1 and crboost_server produce different picks because of a stacked cascade, not a single bug. Each layer is real:

| Layer | crboost_server | cryoboost v1 | Effect |
|---|---|---|---|
| TomoHand (Import) | +1 (`flip_tiltseries_hand=No`) | -1 (`Yes`) | RELION reconstruction applies wrong sign to depth-dependent defocus correction (verified from `src/jaz/tomography/tomogram.cpp`); high-res info degraded across tomogram depth |
| Defocus handedness (tsCtf) | "auto" — actually DID apply set_flip in previous run (run.out evidence: "average correlation is negative") | "set_flip" explicit | NO real divergence here; auto-detection worked correctly. Switching default to "set_flip" is hardening only |
| Template style | detailed Class3D map | featureless ellipsoid | chirality-sensitive vs not |
| Mask style | spherical (`mask_is_spherical=true`) | non-spherical | different mask weighting |
| Symmetry | declared I1, **actually ran C1** in PyTOM | C1 | declared/applied mismatch (likely driver bug) |
| Box size | 96 | 128 | tighter context |
| Tilts used | 37 (TiltFilter dropped 2 high-tilts) | 39 | minor signal loss |

The first two are wrong-defaults bugs in crboost_server vs cryoboost v1 (silent during the refactor port). The middle four are strategic choices (the PI's "find blobs, refine downstream" vs user's "detail-match directly"). The symmetry-config bug is a driver-propagation issue separate from the rest.

**Path to verification:**
1. Flip `flip_tiltseries_hand = Yes` in the Import driver (or wherever its default lives in crboost_server)
2. Apply `set_flip` defocus handedness in the tsCtf driver (search for `invert_defocus_hand` usage)
3. Investigate why `project_params.json symmetry: "I1"` ends up as PyTOM `rotational_symmetry: 1`
4. Re-run from Import onwards on a couple of TS to validate before re-running all 20
5. If still bad: copy the PI's ellipsoid-template strategy as a second-pass

Per-tilt CTF/motion outputs themselves matched perfectly when comparing the same source frame (defocus 2.798 vs 2.809 µm, motion 1.184 vs 1.180) — so stage 1+2 driver logic itself is fine. The divergences are all upstream config or downstream strategy, not driver bugs.

Related: [[project-412-tomohand-discrepancy]], [[reference-412-dataset-locations]], [[reference-cryoboost-v1-location]].

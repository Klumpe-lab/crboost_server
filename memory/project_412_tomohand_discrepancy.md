---
name: project-412-tomohand-discrepancy
description: "2026-05-16 — on the 412 dataset comparison, found crboost_server's Import driver sets flip_tiltseries_hand=No (TomoHand=+1) while cryoboost v1 sets it Yes (-1). Likely root cause of bad picking quality on chiral particles."
metadata: 
  node_type: memory
  type: project
  originSessionId: 21982a9c-b83c-43ad-906d-a06e1aa36cdc
---

On the 412 viral-particle dataset comparison (2026-05-16), traced the user's "most picks look wrong" complaint to an Import-stage handedness mismatch.

**The discrepancy:**
- crboost_server `/groups/klumpe/crboost_data/GT/Import/job001/job.star`: `flip_tiltseries_hand = No` → `_rlnTomoHand = +1` in all tilt_series.star outputs
- cryoboost v1 `/groups/klumpe/user/<pi>/Processing/412/try1/Import/job001/job.star`: `flip_tiltseries_hand = Yes` → `_rlnTomoHand = -1` for same source data

Both call RELION's built-in `relion.importtomo` — same tool, opposite checkbox. So the divergence is in how the crboost_server `importmovies` driver / `project_params.json` defaults configure the job.

**Why:** `_rlnTomoHand` is the sign convention for depth-dependent defocus (verified from RELION source `src/jaz/tomography/tomogram.cpp` — `handedness` multiplies the `dz_pos` term in `getCtf`). Wrong sign → RELION's reconstruction applies the wrong depth-dependent defocus during CTF correction → high-resolution information degraded across the tomogram depth dimension → detailed templates (like the 412 Class3D map) fail to match → CC scores collapse to noise → user's permissive extract cutoff captures that noise as picks (~16/TS vs the PI's ground-truth ~1-3/TS).

NOTE: an earlier framing in conversation called this "tilt-axis chirality" and described picks as "mirror images" — that's WRONG. The effect is depth-dependent defocus mis-correction, not tomogram chirality flip.

**How to apply:** Before trusting any picking results on a new dataset:
1. Check `Import/job001/tilt_series.star` and confirm `_rlnTomoHand` matches the microscope's convention.
2. For Klumpe-lab Titan (TITAN52339720), the PI's `-1` is the established-correct value.
3. The fix is in the crboost_server Import driver / config — flip the default to mirror cryoboost v1.

Per-tilt CTF/motion outputs themselves matched between projects (Defocus 2.798 vs 2.809 µm, MeanFrameMovement 1.184 vs 1.180) — the driver does the same thing once Import is fixed. So fixing TomoHand and rerunning Import → tsImport → reconstruct → TM is likely the shortest path to validate.

Related: [[reference-cryoboost-v1-location]], [[project-per-ts-parallelization]].

---
name: tm-driver-reads-symmetry-from-job-tab-not-species
description: drivers/template_match_pytom.py reads params.symmetry exclusively; the linked ParticleSpecies.symmetry field is never consulted by TM/angle-list code
metadata: 
  node_type: memory
  type: project
  originSessionId: 5c809c59-075f-4d98-8bbf-40bcfe427892
---

**The job-tab symmetry dropdown is the sole input to the PyTOM TM invocation.** `drivers/template_match_pytom.py:221,356` does `sym = str(params.symmetry) if params.symmetry else "C1"` and feeds that into both the angle-list generator and the `--z-axis-rotational-symmetry` flag.

**The linked `ParticleSpecies.symmetry` is NEVER read by the TM driver, supervisor, or `services/templating/angle_lists.py`.** It's purely a UI label / metadata. Users have routinely run I1 TM jobs against species cards that still say "C1" — the job ran as I1 anyway, because the dropdown overrides.

**Why:** by design — a user might want to cross-validate by searching with C1 against an I1-trained template. The species symmetry is a description of the *trained map*, the job symmetry is the *search constraint*. Decoupling them is correct.

**How to apply:**
- If diagnosing "did this TM use the symmetry I expected?", read `project_params.json` → `jobs[<tm_instance>].symmetry`. Don't trust the species card.
- If a user asks "does it matter that my species says C1?", the answer is "no, for execution — the job-tab setting wins. But the species card is misleading UI and should be fixed for clarity."
- If improving UI: surface a discrepancy chip when `params.symmetry != species.symmetry` in the job tab. Doesn't block, just informs.

Confirmed on 2026-05-19 against the `agg_20260311_412_Grid3` run where species 412 was registered C1 but job014's TM ran I1 (angles_I1.txt was generated correctly). See also [[project_i1_angle_list_underperforms]].

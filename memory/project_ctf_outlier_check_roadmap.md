---
name: ctf-outlier-check-roadmap
description: "Roadmap 19 (renumbered from 18 on 2026-09-15; docs/roadmaps/roadmap_19-ctf-fit-outlier-check.md, scoped 2026-09-09, not built): per-tilt independent 1D defocus estimate from Warp's own PS1D/TiltPS1D vs the 2D fit; NO dataset numbers hard-coded (band/window read from the XML, cutoff-ladder stability instead of a fixed band); fail job at >= 25 % diverged; three on-disk projects are the regression test"
metadata: 
  node_type: memory
  type: project
  originSessionId: 57ad8f1b-b3be-4b38-8365-fd155a18d7ca
  modified: 2026-09-09T15:51:11.606Z
---

Follow-up to the copia CTF divergence ([[copia-ctf-bimodal-fits]], known_bugs #1). User's hard rule 2026-09-09: "why would we hardcode a range from a particular project into a verification check that's ostensibly general" — the check reads Warp's `RangeMin`/`DefocusMin`/`DefocusMax` from the XML it checks, picks the high-res cutoff by stability across a coarsening ladder, and keeps only physics/policy constants (`CTF_DIVERGENCE_UM` 0.5 = one Thon ring, `CTF_SCAN_MIN_CC` 0.3, `CTF_FAIL_FRACTION` 0.25), named with their reasoning. Never excludes tilts, never rewrites defoci; hand verdict marked unreliable when fits diverged.

**Why:** three days lost because nothing between fs_ctf and Refine3D flagged 17/28 wrong defoci; the same replay over projects A (30:6, must flag 17/28), C (30:10, must flag 0) and the PI's try2 (7 tilts) is the release regression test the user wants.
**How to apply:** build S1 (`services/tilt_series/ctf_check.py` + replay script) first — it is the whole value; S2–S5 are registry fields, driver tally hooks, UI markers, known_bugs flip. Inputs already parsed by `services/tilt_series/warp_curves.py` (`CtfFit1D`, extend with the defocus window).

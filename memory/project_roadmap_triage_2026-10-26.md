---
name: project-roadmap-triage-2026-10-26
description: "Maintainer's roadmap priorities 2026-10-26 — archived 20/22/24/29/31/34; 37–38 high; 32 up; 27 after 32; 36 waits on the .staging cleanup run; 23 = what Munich needs"
metadata:
  node_type: memory
  type: project
  originSessionId: 499044d5-ca1d-45b4-8b91-63f3165ac341
  modified: 2026-10-26T11:26:55.914Z
---

Maintainer's calls, 2026-10-26:
- Archived to `docs/roadmaps/completed/`: 20, 22 (fix never built), 24, 29, 31, 34.
- **37–38 segmentations: high priority.**
- **32 staging/subsets: higher priority**; its shape was unclear to the maintainer, so explain it plainly (cut point → subset → fork).
- **27 TS-axis: wanted ("cool feature"), but built after 32** ("the graphical pipeline builder" = 32's graphical_pipeline work; the drag-to-wire builder itself was dropped 2026-10-14).
- 36: archive once existing `.staging/` duplicates are cleared (new code moves; old projects still hold copies, e.g. Grid2 job002 ~3.9 GB per TS) — `scripts/clear_staging_duplicates.py`.
- 23: maintainer wants the minimum to make it bulletproof for the Munich handoff; the lease (stage 3) is multi-user CBE work.
- 39: think about the move to uv plus other speed/robustness gains.
- 33: maintainer asked whether it is still needed.

**Why:** triage after the 2026-10-24 overview. **How to apply:** order new work 37–38 → 32 → 27; don't re-propose archived roadmaps. Related [[project-segmentations-roadmap37]], [[project-branching-pipeline-survey]].

**Later 2026-10-26:** 36 (staging cleanup run by the maintainer), 37, 38 archived; 33 merged into 32 §14 (stages L1–L6);
32 §15 records the maintainer's UI brief (branched interactive roster + operational TS × job view; species/particles
deferred until the curation UI work). 27 holds the per-TS execution vision (§12) and is built after 32. README now has
a **Pending** table (32, 23 Munich slice, 39, 27) — each pending file carries a `**Pending (maintainer, <date>):**` line.
23 Munich slice BUILT (not run): `services/shared_files.py` make_shared (mkstemp 0600 fix), `services/code_version.py`,
`ProjectState.saved_by` (schema 3.12), version skew warns + one-time `.schema-x.y.bak`, never blocks jobs (maintainer's rule);
registry warns on any newer schema; open-project toast shows load_warnings + skew.

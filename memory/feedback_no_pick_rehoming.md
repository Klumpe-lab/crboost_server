---
name: feedback-no-pick-rehoming
description: "Never move picks between TM jobs or alignments (\"re-homing\" is dropped for good); particles in missAligned tomograms come from re-running TM"
metadata:
  node_type: memory
  type: feedback
  originSessionId: f2ccfb83-54d7-4366-8c8a-33aa5f0cfec3
  modified: 2026-10-10T21:01:51.364Z
---

Never propose moving or translating a pick set from one TM job / alignment to another — no "particle re-homing"
(was roadmap 29 C5 / roadmap 30 S8, removed from both on 2026-10-10), no fixed-particle designs that carry one pick
set across alignments. If tomograms were rebuilt from a different alignment (e.g. after missAlign), re-run template
matching on them.

**Why:** the user, 2026-10-10, emphatic: "fragile glue that will fall apart and complicates things in the interface
and reasoning". Cost of re-running TM (~440 GPU-h, ~7.5 TB maps for AGG412) was already on the table and accepted.

**How to apply:** every tomogram set gets its own TM run; comparisons between alignments use each side's own picks.
Also: the user didn't follow "A/B project" / "arm" — say "a project with a second preprocessing chain
(missAlign → tsCtf__2 → tsReconstruct__2)" instead. Related [[project-particle-analytics-roadmap30]],
[[project_warp_export_job]], [[feedback-no-verification-ladders]].

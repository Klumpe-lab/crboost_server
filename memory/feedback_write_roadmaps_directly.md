---
name: feedback_write_roadmaps_directly
description: "When the user asks for roadmaps/plans: write them straight into docs/roadmaps/ as progressive, committable-stage docs — no plan-approval ceremony (ExitPlanMode was rejected 2026-08-16); user switches git branches before any code deep-dive, so hold code until told"
metadata:
  type: feedback
  originSessionId: 6bbe5734-f68d-435f-91c3-e5f30b692080
  modified: 2026-09-15T08:23:09.800Z
---

2026-08-16, species-registry scoping: after the assessment + AskUserQuestion forks (which the user
answered happily), I called ExitPlanMode with a plan file; the user rejected it: "just write the
plan into the roadmaps, if need be construct multiple progressive roadmaps so we can work in
committable chunks. i want to switch branches as well before we deep dive."

**Why:** the user's unit of planning IS the roadmap doc in `docs/roadmaps/` (numbered, stage table in
README, stage records appended in-doc). A separate plan-approval step is ceremony; multiple
progressive roadmaps (08→09→10→11→12) that each land in committable stages are what they want.
Code work waits until they've switched branches.

**Re-confirmed 2026-09-04** (curation-seed scoping, plan mode was ON): after the forks were answered I
called ExitPlanMode; rejected again — "cool, just write this to disk and tell me how to pick it up in
the next session." Even inside plan mode the deliverable is the roadmap doc in `docs/roadmaps/` (+
index rows) and a pickup note, not just the plan file.

**Docs layout (user-set 2026-09-15, before first public share):** every roadmap file is named
`roadmap_<NN-slug>.md`; open ones live in `docs/roadmaps/`, code-complete or superseded ones move to
`docs/roadmaps/completed/` (the old `archive/` folder). Investigations / dataset case studies / session
handoffs / audits go in `docs/reports/<topic>/` (e.g. `412-capsid/`, `copia/`), never loose in `docs/`.
`docs/*.md` is reserved for reference documentation. Roadmap numbers must be unique (18 collided → CTF
check renumbered to 19).

**How to apply:** for scope/plan requests → gather facts, ask only concrete forks (with previews),
then WRITE the roadmap docs directly (one per progressive chunk, README row each, per-stage
verification, file:line anchors) and stop; do not open plan mode / ExitPlanMode for docs. Before
any code deep-dive after a scoping session, wait for the user to say the branch is ready. See
[[feedback_engage_concrete_goal]], [[feedback_never_commit]], [[project_species_registry_arc]].

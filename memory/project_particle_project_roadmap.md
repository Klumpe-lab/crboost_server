---
name: project_particle_project_roadmap
description: "Particle Project rework — kill is_aggregation, data-less projects + inline-import provider jobs; multi-session roadmap doc"
metadata: 
  node_type: memory
  type: project
  originSessionId: 0a0c0bb3-6c52-4691-91ea-bb6502a1839c
---

Reworking the "aggregation project" feature into a general **Particle Project** capability.
Living plan: `PARTICLE_PROJECT_ROADMAP.md` (repo root) — has a stage tracker + append-only
Stage log; update it every session.

**Thesis:** no "particle project" type and no `is_aggregation` flag. Just (a) a regular project
creatable **data-less** (no mdocs/frames), and (b) **entry-point provider jobs** + per-slot
input-acquisition UX so the first job a user picks asks for what it needs (the "inversion").

**Key insight:** providers are modeled as **inline-completed import jobs** — the afterok
`_submit_chain` already does this for `IMPORT_MOVIES` (writes star at submit, marks SUCCEEDED,
slurm=None, consumers get no afterok dep). So ImportTomograms + Aggregate become first-class
roster nodes, normally resolver-indexed, no orphaning, no `apply_aggregation_overrides` band-aid.

**Locked decisions (2026-06-24):** tomo input = BOTH reference-existing-star + synthesize-from-MRC-glob;
migration = RIP AND REPLACE is_aggregation; picking = DASHBOARD ACTIVITY→source (not a roster node,
unlike tomos/aggregate which ARE nodes); sequencing = BUILD NOW on afterok.

**Phases:** P0 data-less create → P1 ImportTomograms(→TOMOGRAMS_STAR) → P2 manual picking on imported
tomos (CASE 2; decouple dashboard from `_candidate_extract_instances`; wire authoritative-list→resolver
= S17 gap) → P3 Aggregate(→OPTIMISATION_SET_STAR, reuse `_MergeSelector`+`subtomo_merge`, CASE 1) → P4
Particles-header toolbar + per-slot acquire UX → P5 delete is_aggregation. Commit after each phase.

**Top risk:** the particle tail (subtomoExtraction/reconstructParticle/class3d + array jobs) has NOT
run through afterok yet — P1–P3 are its first real exercise.

**Status (2026-06-24):** P0 (particle-only data-less creation) + full P1 (ImportTomograms inline
provider: model + oracle-matched writer + UI plugin + roster wiring) landed CODE-CLEAN, both
runtime-verify PENDING (no auto-reload → user restart). Uncommitted. See PARTICLE_PROJECT_ROADMAP.md
"▶ NEXT SESSION" block for the exact pickup steps (commit → runtime-test P0/P1 → P2 picking).
Known P2 decision: TemplateMatch needs a tilt-series star (manual picking doesn't — proceed).

Supersedes [[project_picks_filter_aggregation]] and the q_aggregation_project.md approach. Builds on
[[project_orchestrator_replacement]] (afterok is MAIN as of 2026-06-24; schemer dormant-not-deleted).
User wants to progress this over multiple sessions: stop per stage, record done/bugs/blockers amply,
give commit hints.

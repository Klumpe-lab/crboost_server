---
name: project-particle-analytics-roadmap30
description: "Roadmap 30 — particle analytics in σ, controls, set tools; BUILT 2026-10-10 stages 1,2,3,4,5 (not run; S8 re-homing built then removed 2026-10-11), stage 6 UI owed; build record §11, checklist §12"
metadata:
  node_type: memory
  type: project
  originSessionId: 5a6d0a5c-bed2-4a6d-8f36-5e4911e696a4
  modified: 2026-10-10T16:43:06.447Z
---

`docs/roadmaps/roadmap_30-particle-analytics-and-set-tools.md`, scoped 2026-10-10, nothing built. Came out of
the AGG412 campaign, where every particle question was answered with notes/agg412_picking scripts; the user
wants those answers (and better controls) in the app so other users can do the same.

Core: σ-normalised score (LCC ÷ rlnSearchStd) as the shown unit; driver-written per-tomogram sidecars for heavy
stats; rules mark, never drop (filters make new sets with recipes). Stages 1–2 (σ first-class, rule filters →
FILTERED optimisation sets) serve roadmap 29 first. Decisions settled in its §10 (below).

The user did not know what "≥ 9σ" meant (2026-10-10) — explain σ cuts plainly (playbook §10 has the primer)
before using them in a recommendation.

**Why:** the particle stage has almost no at-a-glance view compared with tilts.
**How to apply:** build only on an explicit "build"; related [[project-agg412-reprocessing-2026-10-04]].

**2026-10-10 decisions:** σ default; drivers write anything computable once into sidecars (+ backfill reader);
new Species Analytics tab, nothing new in the Journey (its particle side is laggy at 500–1,000 picks and will be
revamped; keep parts reusable); no condition tags yet; cross-project views only via merged sets. The user wants a
data layer apart from the UI: every analytic and derived set carries provenance (sources, producer, code
version) and set operations are logged, so the UI can change without re-wiring. They will comment on UI/UX
once stage 1 is visible. missAlign rollout recipe for AGG412 = roadmap 29 §6.1 (refined tomograms are re-picked by TM; re-homing, once S8, dropped 2026-10-10).

**2026-10-10 build:** stages 1, 2, 3, 4, 5 code-complete (S8 re-homing code removed 2026-10-11 before commit) (ruff only, not run); stage 6 services done, UI owed
(split/algebra/replay actions, operation-log view, boundary gallery). Sets = directory store Analytics/sets/ (not
PickList); records under Analytics/records/; driver sidecars in <job>/analytics/. Snapshots notes/30-stage-snap/.

---
name: project_species_io_wiring_roadmap28
description: "Roadmap 28 species-aware IO wiring BUILT 2026-10-05 (stages 1–3, ruff-clean, NOT RUN) — stage/species filters, ties stated, instance-keyed overrides migrated on load, pin on ambiguity, 3-pass _submit_chain; doc in docs/roadmaps/completed/"
metadata:
  node_type: memory
  type: project
  originSessionId: cff85849-feff-49ba-824b-23bc16030660
  modified: 2026-10-05T11:24:42.930Z
---

Reproduced 2026-10-05 on the AGG412 pilot (TM ↔ TM afterok cycle, cross-species wiring); built the same day on
branch `species_driven_io`. Doc: `docs/roadmaps/completed/roadmap_28-species-aware-io-wiring.md` (§3 decisions
D1–D9, §5 user check owed, §8 build record). Per-stage snapshots `notes/28-stage-snap/s1..s3` (s3 = final tree);
read-only check `notes/28-stage-snap/dry_resolve.py <project>`.

Decided by the maintainer: **Q1 → pin on ambiguity** (adding a job that ties with an existing job's input pins
that input as an override, toast names it; NOT pin at creation). **Q2 → cross-species input marked in the I/O
section only** (amber tag), Start never asks.

Shape of the code: `_select_source` is the one per-slot answer (override else `_auto_select`) for resolve_inputs,
resolve_slot_edges/resolve_edges, validate_input_slot, _effective_input. `auto_exclusion` = not upstream in
PIPELINE_ORDER / other species. Rank (species, preferred type, job over synthetic, on roster = pipeline_order);
≥2 producer instances at the top = tie. The schemer (non-afterok) Start refuses on unresolved inputs too.
Hazard: older code (lab install on main, a revert) reads `instance:` keys as unresolvable → silent job-number
fallback; don't run a project saved by this code with older code until main has it. Override keys `jobtype:instance:<iid>` (`migrate_override_keys` in ProjectState.load rewrites
`External/jobNNN` and `pending_` keys); a dangling override is reported, never auto-replaced. `_submit_chain`:
allocate → resolve (refusal restores dirs, writes nothing) → write (counter committed in finally).

**Why:** the user hit the cycle mid-campaign; `_follow_refinements` (two missAlign runs) is the one place a job
number still decides.
**How to apply:** runtime check §5 + pilot dry-run owed; pre-existing uncommitted G2 leftovers (orchestrator
autostart calls, pipeline_runner, filterTilts/image_processor.py) sit in the same tree — see
[[reference_dev_tree_live_drivers]], [[project_agg412_reprocessing_2026-10-04]], [[feedback_never_commit]].

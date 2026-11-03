---
name: project-branching-pipeline-survey
description: "Roadmap 32 (from the 2026-10-12 branching survey): S1 fixed inputs, §8 table, S2 staging, S3 Subset job all BUILT 2026-10-15 in the graphical_pipeline worktree, NOT RUN; runtime round 1 + §11 stage 3 checks owed; S4 Fork after §9.5–9.6"
metadata:
  node_type: memory
  type: project
  originSessionId: 8f7e2753-360e-4d53-ac07-7a225abddb9f
  modified: 2026-10-15T11:23:59.511Z
---

Survey only (user asked "survey first and foremost"); no roadmap written, no code. Doc:
`docs/reports/pipeline-graph/branching-survey-2026-10-12.md`.

Core conclusions: the engine is already a DAG (typed slots, pins, `resolve_slot_edges`, afterok toposort,
per-job reconcile); the linear parts are the roster (grouped by type), Run (whole roster) and the project-wide
lock while anything runs. Recommended: stages × lanes graph view in plain NiceGUI (cards + one SVG, layout in
Python), branch verbs (add after / clone / clone from here), inputs fixed at submit, Run to here, read side
following the lane, TS-subset as an inline job. No node editor, no workspaces, protocols not branch templates.

Load-bearing facts found: drivers re-resolve inputs at start (`drivers/driver_base.py:117-139`), so edits while
queued change what runs; TS exclusion and tilt verdicts are project-wide registry state applied at run time;
~20 read-side views use `find_job_by_type` (first of type).

**How to apply:** if the user picks this up, the next step is a roadmap (number after 31) built from §5 of the
doc, starting with the decisions in §7. Related: [[project_species_io_wiring_roadmap28]],
[[project_orchestrator_replacement]], [[feedback-no-pick-rehoming]] (say "second chain", not "arm").

**2026-10-14:** user dropped the interactive graph builder (read-only overview later) and asked for a roadmap →
`docs/roadmaps/roadmap_32-staging-inputs-and-subsets.md`: S1 inputs fixed at submit, S2 staging/running while
live, S3 Subset job, S4 Fork (in-project or new project) unifying Import tomograms + aggregation sources as
"imported outputs". Per-TS streaming judged worthwhile but left in roadmap 27 stage 3, after its stage 2.

**2026-10-14 (pickup of S0+S1):** build happens in worktree `crboost_server_graphical_pipeline` (branch
graphical_pipeline, same commit as main); roadmap 32 + survey exist only as untracked files in the main checkout —
copy them in. Only §9.1 gates S0+S1. Found while re-reading: USER_PARAMS edits on QUEUED jobs are already silently
dropped by the model guard (`services/jobs/_base.py:421`, Scheduled/Failed only) while the UI shows them editable,
so S1.3 mostly makes the UI honest; the aggregate dialog's `_prepopulate_subtomo` writes only `paths`, which
`_submit_chain` overwrites at Run (proposed fold-in to S1.6). Proposed rule: a FAILED job retries in place only
when its inputs are unchanged, else a new dir. User likes the git-log "dots and arrows" DAG table (a post by
Hamilton Ulmer: rows = steps, lane gutter, metadata columns) for the §8 overview; proposed as a full-width view,
main-lineage edges only.

**2026-10-15:** user approved everything ("great plan, go ahead"). Stage 0 answered (§10) and stage 1 BUILT in
the graphical_pipeline worktree, NOT RUN (record §3.1: `submitted_inputs` + `inputs_fixed`, driver uses `paths`,
one dir = one input set, Reset inputs, queued read-only). Pipeline table moved up to right after stage 1 (§8).
Roadmap + survey copied into the worktree; the main checkout still has untracked originals.
Later 2026-10-15: §9.2 decided (live jobs + their upstream locked, upstream undeletable while downstream live;
rest open); §9.3 decided (no Run to here for now, one Run button; revisit with the table). Built
`services/pipeline_table.py` (rows + lanes, pure; view not built). Next session: the §8 view, then stage 2.

**2026-10-15 (third session):** BUILT, NOT RUN: the §8 view (`ui/pipeline_table_view.py`, `View.TABLE`, rail branch
icon; elapsed derived from run_submit.script/exit-marker mtimes, no recorded times) and stage 2 without S2.3
(record §4.1: `jobs_in_use` lock, Run skips live jobs + no pipeline_order rewrite, Clone icon via
`copy_settings_from`, registry O_EXCL lock + replay-on-save, delete preview via `consumers_of`). Next: stage 3 once
§9.4 (subset selection: list vs rules) is answered.

**2026-10-15 (fourth session):** commit `bf178a0d` on graphical_pipeline holds S1 + §8 + S2. Owned-timer fix for the
Tomograms wall / Protocols view handed as its own commit. Stage 3 (Subset job) BUILT, NOT RUN (record roadmap §6.1):
§9.4 = list only; cut points = aligned TS onward (alignment enumerates the registry, not its star); Subset made from
a finished job's tab, frozen once run (Clone to re-choose), opt-in (`auto_eligible=False`), pins to a removed Subset
dangle red; consumers via "Add <job>" on its tab (`JobSpec.items_from`, `add_after_pins`). Built by a background
fork while the user ran round 1 from the same tree; a server restarted mid-build loaded a partial (importable)
state, so the stage 3 restart is the one that counts. Commits on graphical_pipeline: bf178a0d (S1+§8+S2),
6c446f8a + 16b4bde4 (S3), 49dda3c6 (timer fix); tree clean after them. Next: user's round-1 findings, §11 stage 3 checks; S4 after
§9.5–9.6.

**2026-10-15 (fifth session, read-through review before round 1):** four read-only forks reviewed S1–S3 + §8 (drivers vs
a Subset star: fine). Fixed, ruff only, committed by the user as af3a132c..69a3a592 (5 commits, verified via reflog + tree diff): table rows = roster jobs (tsImport row took lane 0) + straight lane
to a consumer with consumers (tilt filter / denoise train fork off); `add_after_pins` pins inputs the cut job lacks to
what its chain read (`_read_up_chain`; TM beside a tomogram Subset tied tsCtf vs tsCtf__2); registry: a failed save
marks what it wrote clean, `_reindex_ts` adds before dropping (save thread vs loop lookups); aggregate dialog's Set up
extraction picks this species' job. Roadmap §4.1/§6.1/§8/§11 updated. Open, reported only: `apply_aggregation_overrides`
overwrites hand-set inputs on projects with an active merge (design call); × on a finished job opens Delete (the
handoff's "only off the roster" was wrong); KeyError if a run's Scheduled job is removed during the sbatch loop; lock
left 10 min if missAlign / DL predict is killed mid-save.

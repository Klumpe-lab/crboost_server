---
name: project_per_ts_top_up
description: "A TS that fails one stage is dropped from that stage's output star and vanishes from all downstream manifests; sparse re-dispatch exists but is unreachable because dir-reuse is gated on FAILED and the job exits SUCCESS"
metadata: 
  node_type: memory
  type: project
  originSessionId: 5d9bc108-a26e-4583-a84b-7d29b152fae3
  modified: 2026-08-11T09:15:22.606Z
---

**Spec:** `docs/roadmaps/roadmap_05-per-ts-top-up.md` (written 2026-08-11 for handoff to Fable, not
implemented). Diagnosed from `/groups/klumpe/crboost_data/deadcode_test`.

A tilt-series that fails one stage is **dropped from that stage's output STAR** — `ts_alignment.py:276`
`aligned_ts = results.ok`, deliberate policy ("AreTomo can't solve every TS; one bad TS must not halt
the pipeline"). Downstream manifests come from `read_tilt_series_names_from_input_star()`, so from
then on the TS is not *failed* downstream, it is **absent**. Nothing can retry what it never heard of.

**The asymmetry:** sparse re-dispatch already works (`array_job_base.py:608` skips `.ok`/`.skip`), but
in-place dir reuse — which preserves the markers it skips on — is gated on
`execution_status == JobStatus.FAILED` in BOTH submit paths
(`pipeline_orchestrator_service.py:126` schemer, `:308` afterok). A job that drops a TS exits
**SUCCESS**, so it gets a fresh dir and recomputes everything. *Partial success is representable in
`.task_status/` but not in `execution_status`.*

**Second bug found:** **nothing anywhere deletes a `.skip` marker.** `clean_status_dir` removes
`*.fail` (and `*.ok` if `keep_ok=False`) but never `*.skip`; `apply_exclusions`/`write_skip_status`
only add. So un-excluding a TS does NOT bring it back on in-place retry. Masked today only because
SUCCEEDED jobs get a fresh dir.

**Gotcha for whoever implements the cascade:** `ts_names` is `sorted()`, so re-adding a missing TS
**shifts array indices**. Markers are name-keyed (safe), but `task_{idx}.out` is index-keyed — so
`scan_statuses`' running inference and `_finalize_stopped_task_statuses` map to the WRONG TS after a
shift. Fix the RUNNING claim first ([[project_task_status_state_machine]] §5 phase 4).

**deadcode_test forensics:** job002 fsMotion 6/6 healthy; job004 tsAlignment SUCCESS but
`Position_1_3.fail`; job005 tsCtf has only 5 items. Do NOT confuse with the orphan-marker bug in the
same project (that was `Position_1_2` in job002, different cause).

See [[project_task_status_state_machine]], [[project_ts_exclude_from_processing]],
[[project_jobnum_offby1_abort_redeploy]].

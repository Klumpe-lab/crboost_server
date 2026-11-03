# Orchestrator roadmap — open issues & hypotheses

Scope: the deploy/numbering/scheme-advance path (`pipeline_orchestrator_service.py`,
`pipeline_runner.py`, `pipeline_monitor.py`). For the UI↔SLURM status-desync analysis and its
redesign tiers, see `PIPELINE_AND_ORCHESTRATOR_STATE.md` (repo root).

---

## H1 — Job-number off-by-one after a scheme abort + redeploy  ·  **hypothesis, needs reproduction**

### What is proven (certain, from on-disk artifacts)
On `/groups/klumpe/crboost_data/20260311_MyrGFP_Grid2` the alignment job died instantly with
`Tomostar directory not found: .../External/job003/tomostar`. Root facts:

- Scheme run 1 (`Schemes/run_20260623_102829`) did Import (job001) + motionctf (job002), then
  **aborted** (`RELION_JOB_EXIT_ABORTED`) before tsImport — leaving an **empty job003 stub**.
- Redeploy run 2 (`run_20260623_104928`) re-ran tsImport / alignment / tsCtf. Its
  `resolution_report.txt` shows crboost **predicted**: tsImport→job003, alignment→job004
  (with `tomostar_dir` → **job003**/tomostar), tsCtf→job005.
- RELION **actually** assigned: tsImport→job004 (real 50-TS tomostar), alignment→job005,
  tsCtf→job006 (`rlnPipeLineJobCounter` is now 6).
- So every prediction sits **one behind** RELION; alignment's `tomostar_dir` points at the
  empty stub → instant fail. A plain re-run hits the in-place retry path (re-sbatches the
  failed alignment with the same baked-in stale path) → **fails identically** ("stuck").

The failure mechanism (off-by-one → consumer points at an empty stub) is **certain**.

### What is NOT proven (the part needing reproduction)
*Which line* produced the 1-off. Two mechanisms yield the **identical** predicted numbers
(003/004/005), and the surviving artifacts cannot distinguish them (the root
`default_pipeline.star` at redeploy time has since been overwritten):

1. **Reuse branch** (`pipeline_orchestrator_service.py:117-125`): reuses a job's stale
   `relion_job_name` (the aborted `job003`) without consuming a counter slot, while RELION
   assigns the next free number (job004). Requires `rlnPipeLineJobCounter == 4` at redeploy.
2. **Stale counter read** (`_get_current_relion_counter` → `rlnPipeLineJobCounter`): if the
   counter read back `3` at redeploy (the aborted stub not yet counted), a *fresh* predict
   would also give job003. Requires counter == 3.

Self-consistency slightly favors (1) — job003's dir+job.star existed (so it was likely a
registered process → counter 4) — but this is **not conclusive**.

### Additional caveat
The failing project was built ~10–20 commits behind current `main`. The deploy code that ran
the redeploy is **not guaranteed identical** to what's in the tree now. Do not assume current
code reproduces it until shown.

### Existing mitigations in current code (why most projects work)
Two things in current `main` already attack parts of this — the failing project likely
predates the first:

1. **Drive-time re-resolution** (`drivers/driver_base.py` `get_driver_context`, ~lines 99-124):
   drivers no longer trust the deploy-time `job_model.paths` snapshot; they re-resolve via
   `PathResolutionService` against `Path.cwd()` at run time. The comment explicitly names this
   bug class: *"the schemer skipped a number … because of an orphan row in
   default_pipeline.star."* This fixes the **consumer-snapshot** half (a job whose own baked
   path is stale).
2. **Producer ranking** (`path_resolution_service.py:225-234`): candidates sort
   `SUCCEEDED < SCHEDULED` then **higher** `relion_job_number` first — so a real job004 output
   outranks a job003 stub *when both are distinct candidates*.

**Why these may still not be enough (the part to test):** job003 (Scheduled stub) and job004
(Succeeded) are the **same single `tsImport` instance** in `state.jobs`, not two candidates.
`_build_output_index` (`path_resolution_service.py:388-431`) emits **one** tsImport candidate
whose path comes from that instance's `relion_job_name` via `_get_instance_path` — and normal
output slots are **not** disk-existence-gated (only `prefer_if_exists` slots are, line 428). In
the failing project's *current* `project_params.json`, `tsImport.relion_job_name` and
`job_path_mapping["tsImport"]` are **still `External/job003/`** (the stub). So if current
`sync_all_jobs` likewise leaves the producer instance mapped to the stale stub (the
dual-mapping flap — both job003 Scheduled and job004 Succeeded map to one instance via the
type-count fallback in Pass 2), then drive-time re-resolution *still* yields job003 and
alignment still fails. The ranking doesn't help because there's only one tsImport instance to
rank.

### Reproduction plan (do this BEFORE any deploy-logic change)
On a throwaway project with the **current** code:
1. Start a multi-stage scheme (import → motionctf → tsImport → alignment).
2. Abort it partway (kill the schemer / stop the pipeline) right after motionctf succeeds and
   while tsImport is scheduled-but-not-run, so a stub `External/jobNNN` is left behind.
3. Redeploy the remaining jobs.
4. Read the new `Schemes/run_*/resolution_report.txt` and compare predicted `External/jobNNN`
   vs the dirs RELION actually creates. Off-by-one ⇒ confirmed.
5. If confirmed, capture the root `default_pipeline.star` `rlnPipeLineJobCounter` and each
   job's `relion_job_name` **at redeploy time** to decide mechanism (1) vs (2).
6. **Decisive check given the drive-time re-resolution mitigation:** let the redeployed
   tsImport actually run and succeed, then before alignment runs inspect the *producer*
   instance — does `project_params.json` `tsImport.relion_job_name` /
   `job_path_mapping["tsImport"]` point at the real (Succeeded, higher-numbered) dir or the
   stale stub? Equivalently, run `PathResolutionService(state).resolve_all_paths(TS_ALIGNMENT,
   …)` and see whether `tomostar_dir` lands on the real dir. If it lands on the stub, the
   producer-mapping/dual-mapping fix (sync must map an instance to its **Succeeded** row, not
   a superseded stub) is the real fix — not (1)/(2). If it lands on the real dir, current code
   is immune and only the transparent guard + stub cleanup remain.

### Candidate fixes (deferred until repro pins the mechanism)
- If (1): in the schemer deploy loop, stop reusing a stale `relion_job_name` for prediction;
  always fresh-predict from `_get_current_relion_counter` so crboost stays in lockstep with
  RELION. Genuine in-place reruns are already handled by `launch_retries` (the retry path), so
  this is surgical. Add the repro as a regression test.
- If (2): make `_get_current_relion_counter` robust to aborted stubs (count max existing
  `External/jobNNN` ∪ the star counter), or re-read it immediately before each prediction.
- Either way: a **deploy-time guard** that aborts/flags when a predicted `External/jobNNN`
  already exists on disk with content from a *different* run (collision) would catch the drift
  before submission.

### Secondary follow-up — orphaned stub cleanup
The aborted run leaves stub rows in `default_pipeline.star` as `Scheduled` (e.g. job003). In
`sync_all_jobs` Pass 2 these map to the *same* instance as the real dir via the type-count
fallback (`pipeline_runner.py` ~Pass 2), causing dual-mapping flap. A redeploy after abort
should prune orphaned stub processes (no output, superseded by a higher-numbered same-type
job). Risky (RELION pipeline surgery) — scope separately.

### Mitigation already landed — transparent guard  ·  ✅ (code-clean, pending runtime)
`drivers/driver_base.py`: `require_producer_input(path, label)` + `diagnose_stale_producer()`.
When a resolved upstream input is missing/empty, the raised error scans sibling
`External/job*/<name>` for a populated producer and names it, labelling the stale-job-number
cause explicitly instead of a cryptic "not found". Wired into the upstream-input checks of
`ts_alignment.py` (tomostar + settings), `ts_ctf.py` (input_processing + input_star), and
`ts_reconstruct.py` (input_star). This does **not** prevent the drift — it makes the failure
**legible** (turns a 20-min "why stuck" into an instant, actionable message). See
`[[project_jobnum_offby1_abort_redeploy]]`.

---

## Earmarked elsewhere
Tier-2 status-desync correctness bugs (B2 squeue-fail false RUNNING, B3 crashed-task stuck
"running", B4/B5 `pipeline_active` freeze + staleness banner) and Tiers 3–4 (SLURM dependency
chaining, S1 staging-count reconciliation + auto-retry) are tracked at the top of §6 in
`PIPELINE_AND_ORCHESTRATOR_STATE.md`.

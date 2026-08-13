# Roadmap 05 — Per-TS top-up: re-run one tilt-series without recomputing the job

**Status:** specified 2026-08-11, **not implemented**. Diagnosed from a live failure in
`/groups/klumpe/crboost_data/deadcode_test` (see §1). Companion to
[`../task-status-state-machine-roadmap.md`](../task-status-state-machine-roadmap.md), whose §5 record
migration this roadmap depends on for its phase 4 (the RUNNING claim) — see §6 for the coupling.

Unlike roadmaps 00–04 this one is **not** derived from the 2026-08-10 audit. It comes from a
reproduced production failure and fixes a capability gap, not a structural one.

## Before → After

**Before:** a tilt-series that fails one stage is silently dropped from that stage's output STAR and
therefore never appears in any downstream job's manifest. The array machinery to re-run *only* the
missing item already exists and is correct — but it is unreachable, because the code path that
preserves per-task status on re-run is gated on the job being `FAILED`, and a job that drops a TS
exits `SUCCESS`. The only available action is a full re-run of the job (and its whole downstream
chain) on a fresh job dir, recomputing every tilt-series that already succeeded.

**After:** "re-run tilt-series X through this job and everything after it" is a targeted action.
Partial success is a first-class outcome; the existing sparse-dispatch machinery is reachable;
un-excluding a muted TS actually brings it back. **Gain:** on a 60-TS dataset, recovering one bad
alignment costs one task instead of sixty, and the downstream chain tops up instead of recomputing.

---

## 1. The failure (reproduced, with evidence)

**Project:** `/groups/klumpe/crboost_data/deadcode_test` — 6 tilt-series, EER, `angpix 1.55`.

| Job | Type | Exit | `.task_status/` contents |
|---|---|---|---|
| `External/job002` | fsMotionAndCtf | `RELION_JOB_EXIT_SUCCESS` | 6 × `.ok` — all 6 TS in `fs_motion_and_ctf.star` |
| `External/job004` | tsAlignment | **`RELION_JOB_EXIT_SUCCESS`** | 5 × `.ok` + **`deadcode_test_Position_1_3.fail`** |
| `External/job005` | tsCtf | `RELION_JOB_EXIT_SUCCESS` | **5 items — `Position_1_3` absent entirely** |

`Position_1_3` failed to align. `job004` exited SUCCESS anyway, by design —
`drivers/ts_alignment.py:276-277`:

```python
aligned_ts = results.ok
excluded_ts = sorted(results.failed + results.missing)
```

with the rationale in the comment above it: *"Per-TS alignment failure is normal in cryo-ET — AreTomo
simply can't solve every tilt-series. One bad TS must NOT abort the whole job and halt the pipeline
… Only a total wipeout (nothing aligned) is fatal."* **That policy is correct and must be kept.**

The consequence is the part to fix. `job004` emits a 5-TS `aligned_tilt_series.star`. Every
downstream job derives its manifest from its input STAR via
`read_tilt_series_names_from_input_star()`, so from `job005` onward `Position_1_3` is not a *failed*
item — it does not exist. Nothing downstream can retry what it has never heard of.

> **Do not confuse this with the orphan-marker bug** fixed on 2026-08-11 in the same project.
> `Position_1_2` in `job002` carried both `.ok` and `.fail` because a superseded array's orphan task
> wrote status after the job had already completed. Different TS, different job, different cause —
> see `../task-status-state-machine-roadmap.md` §2. `job002` is healthy: 6/6.

## 2. Why it cannot be retried today

The sparse re-dispatch itself is **already built and correct** —
`drivers/array_job_base.py:608-609`:

```python
previously_done = get_previously_done(job_dir)          # {.ok} | {.skip}
indices_to_run = [i for i, ts in enumerate(ts_names) if ts not in previously_done]
```

Re-running `job004` in place would re-align `Position_1_3` and nothing else. The blocker is that
in-place dir reuse — the thing that keeps `.task_status/*.ok` alive for that call to skip — is gated
on `FAILED` in **both** submit paths:

| Path | Location | Gate |
|---|---|---|
| schemer retry | `pipeline_orchestrator_service.py:126` | `job_model.execution_status == JobStatus.FAILED` → `retry_ids` |
| afterok DAG | `pipeline_orchestrator_service.py:308-309` | `reuse_dir = (job_model.execution_status == JobStatus.FAILED and …)` |

`job004` is `SUCCEEDED`. Both paths therefore allocate a **fresh** `External/jobNNN` with an empty
status dir, so all 6 TS recompute and the downstream chain re-runs behind it.

**The root asymmetry: partial success is representable in `.task_status/` but not in
`execution_status`.** A job is either FAILED (retry in place, sparse) or SUCCEEDED (full recompute).
"Succeeded with 5 of 6" has no third state, so the machinery built for exactly this case is
unreachable.

## 3. The second bug: `.skip` is a one-way door

`clean_status_dir` (`array_job_base.py:184`) removes `*.fail`, and `*.ok` when `keep_ok=False`. It
**never** removes `*.skip`. `apply_exclusions` (`array_job_base.py:279`) and `write_skip_status`
(`:147`) only ever add them. A repo-wide search finds **no code path that deletes a `.skip` marker.**

Since `get_previously_done()` counts `.skip` as settled, **un-excluding a tilt-series in the registry
does not bring it back** on an in-place retry — the stale marker keeps it permanently undispatched.
This is currently masked only because a SUCCEEDED job gets a fresh dir where the marker is absent
anyway; fixing §2 without fixing this would *expose* it.

## 4. Stage 0 — gather first

> **PARTIAL RECORD 2026-08-13** (items 2 + 4 verified by code reading; 1 + 3 remain).
> **Item 2 — aggregation contract: rebuild-from-all-settled, confirmed for all four core drivers.**
> Every supervisor calls `adapter.ingest(results.ok)` where `results.ok` = ALL `.ok` stems in the
> status dir (prior submissions included), and every adapter's `emit_star` re-reads the full input
> STAR and rewrites the complete output STAR — nothing appends. Precondition it relies on: the
> shared output dir retains prior-run per-TS artifacts (XMLs / MRCs), which the ts_ctf #10
> copy-filter fix (`bb8cafe`) protects. Failure posture differs by driver and matches their
> supervisor policy: `fs_motion_ctf.py:161`, `ts_ctf.py:168`, `ts_reconstruct.py:117` RAISE on any
> non-excluded TS lacking an ingested output (all-or-nothing supervisors — emit only runs after
> `all_succeeded`); `ts_alignment.py:225-233` warns-and-drops per TS, fatal only on total wipeout
> (the tolerant tally — this is where the §2 drop policy lives). Consequence for stage 4: a top-up
> re-run at any stage emits a complete STAR covering old + new TS; the cascade only needs to
> re-trigger downstream, not to merge stars.
> **Item 4 — `is_excluded` round-trip exists.** `registry.set_excluded()`
> (`services/tilt_series/registry.py:184`) sets both ways and the dashboard toggles it
> (`ui/tomo_dashboard_dialog.py:468-470`); the one-way door is purely the on-disk `.skip` marker
> (`clean_status_dir` never removes `.skip`) → §3's `retry_items=` fix is sufficient, no UI work
> needed for un-muting itself.
> Also noted in passing: `ts_alignment.py` adapter `emit_star` falls back to `frame_angpix = 1.35`
> when no pixel-size column exists (`adapters/ts_alignment.py:177`) — another instance of the
> known 1.35-default trap, flagged for the never-silent-defaults policy, out of 05 scope.

1. **Confirm the downstream propagation shape.** For `deadcode_test`, enumerate which jobs after
   `job004` have manifests missing `Position_1_3` and which emit STARs that would need regenerating.
   Establishes how far a top-up must cascade.
2. **Decide the aggregation contract per job type.** After a partial re-run, does the supervisor's
   aggregation step rebuild its output STAR from *all* settled items or only the newly-run ones?
   `collect_task_results()` reads the whole status dir, so `results.ok` is all 6 and re-aggregation
   *should* be complete — **verify this per driver**, especially `ts_alignment`, `ts_ctf`,
   `ts_reconstruct`, and any driver that appends rather than rewrites.
3. **Inventory index-shift blast radius.** See §6 — list everything keyed by array index rather than
   item name.
4. **Check `is_excluded` round-trip.** Confirm the registry can un-exclude a TS at all, and what UI
   affordance exists; §3's fix is pointless if nothing can clear the flag.

## 5. Stages

1. **Reuse gate on work, not on status.** Replace the `execution_status == FAILED` predicate in both
   submit paths with "this job dir has unsettled manifest items" — i.e. reuse when
   `.task_manifest.json` exists and `get_previously_done()` does not cover every item. Behavior for
   genuinely-FAILED jobs is unchanged (they are a subset); the new reachable case is
   SUCCEEDED-with-gaps. Quarantine as its own commit.

2. **`clean_status_dir(..., retry_items: set[str] | None = None)`.** When given, clear `.fail` *and*
   `.skip` for exactly those names, so a caller can name the tilt-series to re-run. Default `None`
   preserves today's behavior. This is also the fix for §3: un-excluding a TS clears its `.skip`.

3. **A "re-run these tilt-series" action.** Plumb a set of TS names from the UI to the supervisor
   (manifest `extra` key, or a `.task_status/.retry_request` file the supervisor consumes and
   deletes). Surfaces in the roster's per-TS sub-rows, which already render `fail`/`skip` state via
   `scan_statuses`.

4. **Downstream cascade.** A top-up at stage N must re-run stages N+1… for that TS only. Each
   downstream job's manifest *grows* when it re-reads a now-larger input STAR — which is where §6
   bites. Do not start this stage before §6 is settled.

5. **Surface partial success.** Per the project's *"never fail silently"* rule, a job that dropped a
   TS must say so in the roster — a subtle badge (e.g. `5/6 ✓ · 1 dropped`) distinct from both green
   and the red failure count, with the reason on hover. Today `job004` looks identical to a clean
   6/6 success, which is what let this go unnoticed.

## 6. The index-shift hazard (read before stage 4)

Status markers are keyed by **item name**, so growing a manifest is safe for them:
`get_previously_done()` returns names and `indices_to_run` is recomputed against the new list.

But `ts_names` is `sorted()`, so inserting a previously-absent TS **shifts the array indices of every
item after it**. Anything keyed by *index* rather than name breaks:

- `task_{idx}.out` / `task_{idx}.err` — SLURM output paths. After a shift, `task_2.out` belongs to a
  different tilt-series than it did last run, and stale files from the previous submission are still
  on disk.
- `scan_statuses()` (`ui/components/task_utils.py`) infers `running` from `task_{idx}.out` existing —
  already unreliable across submissions, actively **wrong** after a shift.
- `_finalize_stopped_task_statuses` (`pipeline_runner.py:995`) maps `idx → name` through the **new**
  manifest while reading `task_{idx}.out` from the **old** run, and would write `.fail` against the
  wrong tilt-series.

**Mitigation:** implement the RUNNING claim from `../task-status-state-machine-roadmap.md` §5 phase 4
first. Once a task records `{"state": "running", "array_job_id": …, "task_idx": …}` under its own
name, nothing needs to infer state from an index-keyed filename and the hazard disappears. Cheaper
interim option if that migration is deferred: delete stale `task_*.out`/`task_*.err` in
`submit_array_job` for the indices about to be dispatched.

## 7. Modern-Python weave-in

- `StrEnum` for the task state (`ok|fail|skip|running|pending`) — it is currently a bare `str` in
  `scan_statuses` and a pair of booleans in `write_status_atomic(ok=...)`.
- A `TaskOutcome` frozen dataclass (state + `array_job_id` + `task_idx` + `attempt`) is the natural
  carrier for the §5-phase-2 JSON record; `match` on it in the aggregation branches.
- Replace `write_status_atomic(status_dir, name, ok: bool)` with an explicit state argument — the
  boolean cannot express `skip` or `running`, which is why those two grew separate writer functions
  with divergent semantics in the first place.

## 8. Runtime checklist

Sandbox ceiling is `ruff` + reading; the following is the maintainer's step.

1. Open `deadcode_test`. Confirm the roster shows `job004` as succeeded-with-1-dropped (stage 5) and
   `Position_1_3` as `fail` in its per-TS sub-rows.
2. Trigger "re-run `Position_1_3`" on `job004`. Expect: **no new `External/jobNNN`** — `job004` is
   reused; `run.out` reports `[SUPERVISOR] 5 tilt-series already succeeded; 1 to (re)run` and
   `--array=<idx of Position_1_3>%1`.
3. Confirm `aligned_tilt_series.star` now carries **6** rows, and that the 5 pre-existing TS were not
   recomputed (mtimes unchanged in `job004/tilt_series/`).
4. Cascade: `job005` re-runs and its `.task_status/` gains a 6th entry; verify no marker belongs to
   the wrong TS after the index shift (§6).
5. Exclude a TS, run, un-exclude it, re-run: confirm the `.skip` clears and it is dispatched (§3).

## 9. Open questions for the implementer

- **Should a dropped TS be `.fail` or a distinct `.dropped` state?** It failed *this* stage, but the
  job as a whole succeeded. Reusing `.fail` keeps the state set small; a distinct state makes stage
  5's badge trivial and avoids overloading the red count. Recommend deciding alongside §5 phase 1 of
  the state-machine roadmap, since both change the same enum.
- **Where does the cascade stop?** Re-running a TS through alignment invalidates every downstream
  product for that TS. Is the top-up bounded by the user's selection, or does it automatically walk
  the DAG to the leaves? The afterok orchestrator (`_submit_chain`) already knows the edges.
- **Does `ts_alignment`'s drop policy belong to other drivers?** It is currently local to alignment.
  If reconstruct/ctf grow the same tolerance, the dropped-TS concept needs to be shared rather than
  re-implemented.

# Per-task status — one record, one owner, one state

**Status:** Diagnosis + containment **LANDED 2026-08-11, PENDING RUNTIME**. Representation migration
(§5) **designed, not built** — user steer 2026-08-11: land containment, review the design before the
refactor. Companion to
[`ORCHESTRATOR_REPLACEMENT_PLAN.md`](../services/scheduling_and_orchestration/ORCHESTRATOR_REPLACEMENT_PLAN.md)
— this doc is the concrete fix for the marker-file layer of the status-desync tiers.

> The desync tiers themselves (S1–S4) live in `PIPELINE_AND_ORCHESTRATOR_STATE.md`, which
> `ORCHESTRATOR_REPLACEMENT_PLAN.md:24` expects at repo root but which is **not in the working tree**
> (branch `pipeline_state_expansion`?). Worth restoring — S1/S2 are cited from several places.

---

## 1. The thesis

Per-task status lives in `External/jobNNN/.task_status/` as **marker files whose presence is the
state**: `{item}.ok`, `{item}.fail`, `{item}.skip`. Nothing in that representation prevents an item
from holding two markers at once — "exactly one status" is not an invariant of the data, it is a
**convention** that every writer must independently honour and every reader must independently
reconstruct.

There are **5 writers** and **2 reader families**, and they did not agree. That disagreement is not a
bug that was introduced; it is the representation working as designed.

> The fix is to stop encoding state in the *presence of a file* and start encoding it in a *value*.

## 2. The incident that proved it

`/groups/klumpe/crboost_data/deadcode_test/External/job002` (fsMotionAndCtf, 6 TS):

| When | What |
|---|---|
| Aug 10 19:06 | `Position_1_2.ok` — the TS succeeded (41/41 frames in **2:24**) |
| Aug 11 09:23 | supervisor re-runs: *"5 tilt-series already succeeded; 1 to (re)run"* → `--array=5%1`, array job **987284** |
| Aug 11 09:26 | task 5 lands. `Status: 6 ok, 0 fail, 0 missing` → `RELION_JOB_EXIT_SUCCESS` |
| Aug 11 09:31 | **`Position_1_2.fail` appears — 5 minutes after the job finished successfully** |

A task from an **earlier, superseded** submission was still on the cluster. Nothing scancels the
previous array on re-run, so it kept computing a TS the new run had already settled, on a second GPU,
writing into the same staging tree. It was killed by `run_command`'s watchdog and wrote `.fail` over
a settled item.

`Position_1_2` then held **both** `.ok` and `.fail`. The roster counted marker *files* rather than
resolving one status per item — 6 ok + 1 fail — so the row showed `6/6` green and `1!` red at once,
on a job that had genuinely succeeded, with no rerun able to clear it.

**The slow task was node rot, not a code or parameter fault.** Same TS, same command, same container:
2:24 on the good run, 21/41 in ~26 min on the killed one — ~20× slower per frame on `clip-g3-0`. The
30-min per-task budget was already 12× headroom over real cost. Compare the Aug 10 apptainer
mount-rot sightings on `clip-g3-8` / `clip-g3-4` — same node family, same window.

## 3. Who touches the status dir

**Writers (5) — uncoordinated by construction:**

| Writer | Where | Notes |
|---|---|---|
| `write_status_atomic` | `drivers/array_job_base.py` | task mode, all 8 array drivers. The choke point. |
| `write_skip_status` | `drivers/array_job_base.py` | supervisor pre-marks "no work to do" |
| `apply_exclusions` | `drivers/array_job_base.py` | deletes `.ok`/`.fail`, writes `.skip` |
| `clean_status_dir` | `drivers/array_job_base.py` | deletes `.fail` (and `.ok` when `keep_ok=False`) |
| `_finalize_stopped_task_statuses` | `services/.../pipeline_runner.py:995` | **hand-rolls its own tmp+`os.replace`**, bypassing the choke point |

**Readers — two families:**

- **UI — already consolidated.** `scan_statuses` (`ui/components/task_utils.py`) is the single
  resolver, with correct `ok > fail > skip > running > pending` precedence. `array_task_tracker`,
  `ui/dashboard/data.py`, and (as of this change) `pipeline_roster` all route through it. **This side
  is healthy** and needs no migration beyond swapping its internals.
- **Driver / control plane — counts files.** `get_previously_succeeded`, `get_previously_done`,
  `collect_task_results` each glob the dir independently. These are what §5 has to move.

## 4. The state machine

Exactly one state per item at any time. This is genuinely simple — the difficulty was never the
machine, it was that the storage could not express it.

| From | To | Who may perform it |
|---|---|---|
| PENDING | SKIP | supervisor, pre-dispatch only |
| PENDING | RUNNING | the task, on claim |
| RUNNING | OK / FAIL | **only the task that claimed it** |
| RUNNING | FAIL | control plane, only once the owning array is confirmed dead |
| OK | PENDING | supervisor, explicit full reset only (`clean_status_dir(keep_ok=False)`) |
| FAIL / SKIP | PENDING | supervisor, on resubmit |
| *any* | *any* | **REJECTED when the writer's `array_job_id` ≠ the manifest's** |

That last row is the anti-tampering rule and the one that closes §2.

## 5. Target representation — one JSON record per item

```
.task_status/deadcode_test_Position_1_2.json
{
  "state": "ok",              # ok | fail | skip | running
  "array_job_id": "987284",   # owner — makes orphan writes detectable
  "task_idx": 1,
  "attempt": 2
}
```

Replaced wholesale via tmp + `os.replace`. **No illegal combination is representable**, so exclusivity
stops being a convention and becomes a property of the store. Precedence logic disappears from every
reader, because there is nothing left to disambiguate.

**Migration phases:**

1. **`read_status(job_dir, items) -> {item: state}`** — one resolver, with a back-compat shim that
   falls back to legacy `.ok`/`.fail`/`.skip` markers when no `.json` exists. Existing projects
   (`deadcode_test`, `try2_after_pixShift`, …) keep working untouched.
2. **Move the 3 driver-side readers onto it** (`get_previously_succeeded`, `get_previously_done`,
   `collect_task_results`) and re-point `scan_statuses` at it, so the UI inherits it for free.
3. **Single writer.** All 5 writers route through `write_status(job_dir, item, state, ...)`. Fold
   `_finalize_stopped_task_statuses` in — note it currently duplicates the atomic-write logic in
   `services/`, so a `drivers/` import there is a layering question to settle (likely: move the
   status module out of `drivers/` into `services/`).
4. **RUNNING claim.** The task writes `state: running` with its `array_job_id` on start. This retires
   the `task_{idx}.out`-existence inference, which is stale by construction — those files persist
   across submissions, so a re-dispatched-but-still-queued task currently reads as "running" — the
   queued-but-shown-running desync tier.

## 6. What landed 2026-08-11 (containment)

Behaviour-preserving except where noted; all lint-clean.

- **`write_status_atomic`** — success now clears any prior `.fail`/`.skip`, so a successful retry can
  no longer leave the red badge lit. Failure deliberately does **not** clear an existing `.ok`:
  normal dispatch never re-runs an item that has `.ok`, so that combination means the writer is an
  orphan and the recorded success is the trustworthy record.
- **`is_superseded_task`** — compares `SLURM_ARRAY_JOB_ID` against the manifest's `array_job_id` and
  refuses the write when the task's id is **strictly older**. SLURM ids are monotonic, so this
  identifies orphans unambiguously. Only *strictly* older is rejected, because `submit_array_job`
  writes `array_job_id` back **after** `sbatch` returns — a current task racing that write sees a
  *newer* id and must not be rejected. Missing env / unreadable manifest / non-numeric id all fail
  open.
- **`cancel_previous_array`** — `submit_array_job` now probes `squeue` for the manifest's prior
  `array_job_id` and `scancel`s it before writing the new manifest. Stops the superseded array from
  burning a GPU and racing the new run's outputs.
- **`_get_array_progress`** (roster) — resolves one status **per manifest item** via `scan_statuses`
  instead of counting marker files. This heals already-settled jobs on disk with no rerun.
- **`run_command` idle watchdog** — see §7.

## 7. The watchdog, corrected

The total-runtime watchdog was **not** the fault here, and is worth keeping. A run that trips it was
going to exceed `--time` anyway; what the 90% margin buys is that *we* kill it and still write a
`.fail` with a reason, instead of SLURM killing it mid-line with no marker.

Its real flaw was narrower: **it only measured total runtime, which is the wrong axis for the hazard
it was written for.** The documented hazard is a crashed tool whose orphaned child holds the stdout
pipe open, so `iter(process.stdout.readline, "")` blocks on a pipe that will never EOF. Total-time
scales with the allocation — under the 14-day `g_long` QOS a process hung in its first minute would
hold a GPU for ~12.6 days before the 90% mark tripped.

So inactivity was added as a **second, independent** watchdog (`IDLE_TIMEOUT_DEFAULT = 45 min` of
zero output, capped at the total budget, `idle_timeout=0` opts out for a legitimately silent tool).
The two catch different failures and the kill message now names which one fired.

## 8. Open items

- **`_finalize_stopped_task_statuses` keys off `task_{idx}.out`**, which is stale across reruns: on a
  stopped pipeline it can mark `.fail` for items never dispatched in the current submission. Low harm
  today (the outcome is "not ok", which is true), retired by §5 phase 4.
- **Scancel → status race.** Between `scancel` and the new `update_manifest`, the manifest still holds
  the old id, so a dying orphan would not be rejected. Negligible in practice — scancel'd tasks are
  SIGKILLed without running Python cleanup, and task mode installs no SIGTERM handler — but it closes
  properly once the record carries its own owner (§5).
- **Roster does not count `.skip` as settled**, so a user-muted TS reads `5/6` forever, while
  `collect_task_results` (the authority on job completion) counts it as settled. Same
  confusing-counter family; deliberately left alone as out of scope for the reported bug.

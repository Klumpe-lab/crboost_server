# Roadmap 15 — Job-dir ownership: no two supervisors in one directory

**Status:** scoped 2026-08-28 from a reproduced production failure in
`/groups/klumpe/crboost_data/YakoutRayyan_Aug27` (Rayyan's 114-TS run). **Not implemented.**

Not from the 2026-08-10 audit. It closes a **safety** gap: an `External/jobNNN/` directory is shared
mutable state with no owner, so a second supervisor can be launched into a live one and destroy the
first one's working set mid-run. The user-visible symptom is a per-TS `FileNotFoundError` that blames
the producer ("producer drift") for damage the consumer did to itself.

## Before → After

**Before:** every "is this job already running?" guard lives in one server process's RAM
(`PipelineRunner._active_processes`, `._retry_monitors`, `ProjectState.pipeline_active`). A server
restart, a second server process, or a `stop_and_cleanup` clears them while the SLURM supervisor is
still alive. `launch_retries` then deletes the exit markers and re-`sbatch`es the same
`run_submit.script` into the same directory. Two supervisors share one job dir, one `run.out`, one
`tomostar/`, one `.task_status/`. Nothing detects it; the damage surfaces as a data error.

**After:** the job dir has exactly one live owner, recorded on disk and validated against SLURM. A
second supervisor exits immediately and says why. Rebuilds of shared inputs are atomic, so even a
stale reader never sees a half-populated directory. **Gain:** the whole class — silent partial
input, scrambled logs, marker files written by the wrong process — stops being reachable.

---

## 1. The incident (with evidence)

Project `/groups/klumpe/crboost_data/YakoutRayyan_Aug27`, job `External/job004`
(`aligntiltsWarp`, 114 tilt-series). 62 of 114 array tasks failed with:

```
FATAL ERROR for ts=YakoutRayyan_Aug27_Position_1_2: Cannot align '...':
registry TS '...' missing from: tomostar dir (producer drift)
```

The producer was innocent: `External/job003/tomostar/` holds all 114 files. The consumer's own copy,
`External/job004/tomostar/`, held 52 — and the 62 missing names are **byte-for-byte the first 62 in
sort order**, which is the signature of a directory tree being deleted underneath a sorted rebuild
loop.

Two supervisors ran in that dir concurrently:

| evidence | where |
|---|---|
| Two `--- SLURM JOB BEGAN ---` / `--- ts_alignment: SUPERVISOR mode ---` blocks, interleaved at conflicting file offsets (each SLURM job re-opens `run.out` with `O_TRUNC` while the older process keeps writing at its old offset — this is why the log reads scrambled) | `job004/run.out` |
| Two array submissions from one job dir: `1548806`, then `1548927` | `job004/run.out` |
| The second supervisor took the resume path: `52 tilt-series already succeeded; 62 to (re)run` | `job004/run.out` |
| The loser died mid-`rmtree`: `OSError: [Errno 39] Directory not empty: '.../job004/tomostar'` | `job004/run.err` |

### The two defects that combine

**(a) The launcher has no cross-process guard.** `PipelineRunner.launch_retries`
(`services/scheduling_and_orchestration/pipeline_runner.py:866`) clears
`RELION_JOB_EXIT_{SUCCESS,FAILURE}`, patches the star row to Running, and re-`sbatch`es
`External/jobNNN/run_submit.script` in place. Its only precondition is

```python
if state.pipeline_active or self.is_active(project_dir):
    return err("Pipeline is already running. Wait for it to complete or cancel it first.")
```

`is_active` (`pipeline_runner.py:75`) is `resolved in self._active_processes or resolved in
self._retry_monitors` — process-local dicts. `pipeline_active` is a `ProjectState` bool that
`sync_all_jobs` **deliberately self-heals to False** when `is_active()` is False
(`pipeline_runner.py:213-215`), and that `stop_and_cleanup` clears on STOP-ON-FAIL
(`pipeline_runner.py:170`). In this incident the duplicate supervisor's own
`RELION_JOB_EXIT_FAILURE` triggered STOP-ON-FAIL, which re-armed the Run button while a real
supervisor was still polling its array.

**(b) The supervisor destroys shared state in place.** `TsAlignmentDriver.enumerate_items`
(`drivers/ts_alignment.py:178-182`):

```python
local_tomostar_dir = ctx.job_dir / "tomostar"
if local_tomostar_dir.exists():
    shutil.rmtree(str(local_tomostar_dir))
...
kept, dropped = drop_tilts_from_tomostar(tomostar_dir, local_tomostar_dir, drop_frames)
```

`drop_tilts_from_tomostar` (`services/tilt_series_service.py:247`) iterates
`sorted(src_dir.glob("*.tomostar"))`, so the window between `rmtree` and the last write is a
directory that is *legitimately* missing an ever-shrinking prefix. Any concurrent reader — including
the same driver's own drift check twenty lines later — sees a partial set and cannot tell it from
genuine producer drift.

### The class

1. A job dir is shared mutable state with **no owner**.
2. `RELION_JOB_EXIT_*` markers are a shared status channel **any** writer can assert on, including a
   duplicate that has no business speaking for the job.
3. Every liveness guard is **in-memory, single-process**, so it is wrong after a restart, wrong with
   two server processes, and clearable by an unrelated code path.

Note the amplifier: with the UI stalling for seconds (see roadmap 16 / the 2026-08-28 latency fixes),
a user re-clicking Run is the normal case, not the exotic one.

---

## 2. Stages

Each stage is a self-contained commit. S1 alone closes the class; S2–S4 remove the ways it can still
hurt and the ways it stayed invisible.

### S1 — Job-dir ownership lock in the supervisor (**the load-bearing stage**)

`drivers/array_job_base.py`, at the top of `run_supervisor`, before any directory mutation:

- `O_CREAT|O_EXCL` create `job_dir/.supervisor.owner` containing `SLURM_JOB_ID`, hostname, and an
  ISO timestamp.
- On `FileExistsError`: read it, and ask SLURM whether that job id is still live
  (`squeue -h -j <id>` — the supervisor already shells out to SLURM, and it runs on a compute node
  where `squeue` is available).
  - **live** → log `another supervisor (slurm <id> on <host>, started <ts>) owns this job dir` and
    exit non-zero *without touching anything*. It must not write `RELION_JOB_EXIT_FAILURE` — it does
    not speak for this job; see S3.
  - **dead** → steal the lock (rewrite it with our own id), logging that we did and whose it was.
- Release on normal exit and in the fatal-error path.

Do it in `array_job_base` rather than per driver, so every array job inherits it. Non-array
supervisors get the same treatment via the shared entry point.

**Why O_EXCL and not `flock`:** the job dirs live on the group's shared mount; `O_EXCL` + an explicit
SLURM liveness check is the pattern that survives NFS semantics and stale locks, and it names the
owner in the error, which `flock` cannot.

**Done when:** launching two supervisors into one job dir (`sbatch run_submit.script` twice) leaves
the first untouched and the second exits with the owner named in `run.out`.

### S2 — Atomic rebuilds; never mutate a shared input in place

`drivers/ts_alignment.py:178-182` is the instance; the rule is general.

- Build into `job_dir/tomostar.tmp.<slurm_job_id>/`, then `os.replace()` onto `tomostar/` (rename an
  old one aside and `rmtree` it after the swap).
- Sweep every other `rmtree`-then-rebuild of a job-local input across `drivers/` and give them the
  same treatment.

**Done when:** there is no window in which `job_dir/tomostar/` exists and is incomplete.

### S3 — Exit markers stop being an open channel

- The supervisor writes `RELION_JOB_EXIT_*` **only if it holds the lock**. `run_submit.script` writes
  the marker from the shell today (`if [ $EXIT_CODE -eq 0 ]`), so the marker decision moves into the
  driver, or the script consults the lock file before touching a marker.
- `sync_all_jobs` treats a marker as authority only when it does not contradict SLURM: a marker for a
  job dir whose `.supervisor.owner` names a *live* SLURM job is stale/foreign and must not flip the
  job to Failed. This is the specific step that stops STOP-ON-FAIL from re-arming Run under a live
  run.

**Done when:** a duplicate supervisor's failure cannot change the job's status.

### S4 — Submitter-side pre-flight

In `launch_retries` and `_submit_chain`, before `sbatch`: resolve live SLURM jobs' `stdout_path` into
their job dirs (`sync_all_jobs` already builds exactly this map, `pipeline_runner.py:187-206` — lift
it into a helper) and refuse the launch if the target dir already has one, naming the job id in the
error. Belt to S1's braces: it gives the user a UI-level "already running as slurm 1548806" instead
of a cluster-side exit.

**Done when:** the Run/Retry button refuses rather than trampling, with the live job id in the
message.

---

## 3. Adjacent findings from the same incident

Same project, same failure window, **different class** — but they were the reason the run's damage
went unnoticed, and one of them is a live data-loss route. They are recorded here because the
evidence is here; they can be split into their own roadmap if they grow.

### A1 — The registry silently drops tilt-series it cannot parse (**data loss**)

`TiltSeriesRegistry.load()` (`services/tilt_series/registry.py:299-304`):

```python
except Exception as e:
    logger.warning("Failed to load TS sidecar %s: %s", ts_path, e)
```

A sidecar that fails validation is dropped and the run continues on a **partial registry**. In this
incident the driver logged `PREFLIGHT: registry has 62 TS` for a 114-TS project and then
`WARNING: 52 tilt-series on disk are unknown to the registry (stale files?)` — the registry was
missing the very TS whose sidecars had just been enriched. `save()` (`registry.py:325`) then writes
`"tilt_series": sorted(self._tilt_series)`, i.e. **an index rebuilt from the survivors**, orphaning
the dropped sidecars.

This directly violates the standing rule in `CLAUDE.md` ("never fail silently, never invent
defaults") — a partial registry is an invented default for "the registry".

Fix: fail loud on an unparseable sidecar (or quarantine it and refuse to `save()` a shrunken index);
`save()` must never write an index smaller than the one it loaded without an explicit deletion.

### A2 — Minor schema skew is silent

`load()` warns only when the **major** version is ahead:

```python
if version[0] > REGISTRY_SCHEMA_VERSION[0]:
```

Here the on-disk registry was 1.4 (per-tilt QC: `average_intensity` / `masked_fraction` /
`fov_fraction`, added 2026-08-27) and the code reading it predated those fields, with
`model_config = ConfigDict(extra="forbid")`. `1.4 > 1.3` on the minor is invisible to that check, so
the mismatch surfaced only as A1's silent drop — plus **5.3 MB / 22,198 lines** of pydantic
`extra_forbidden` dumps in `job004/run.err`.

Fix: refuse (not warn) when the on-disk minor is ahead of the code's, with a message naming both
versions and the fix.

### A3 — Two installs, nine days apart

`job002` ran from `/users/artem.kushner/dev/crboost_server`; `job003+` ran from
`/groups/klumpe/software/crboost_server`, whose `services/tilt_series/models.py` is dated
**2026-08-18** against the dev tree's **2026-08-27**. The UI wrote 1.4 sidecars; the cluster drivers
read them with 1.3 models. A1 and A2 are only *reachable* because of this.

There is no operational answer in the repo for "which install is authoritative and when does the
shared one get updated". At minimum: a startup log line naming the install path and registry schema
version, so a skew is visible in `run.out` on line one rather than inferred from a 5 MB stderr.

---

## 4. Not in scope

- **Fixing the 62 failed tilt-series in that project.** The maintainer explicitly does not want
  recovery work; the array is idempotent (`task_already_done` skips real alignment output) and one
  clean re-run handles it once S1 is in.
- **The orchestrator replacement** (`ORCHESTRATOR_REPLACEMENT_PLAN.md`). S1–S4 must hold under both
  the schemer path and the afterok DAG; neither is being replaced here.
- **The latency work.** Fixed directly on 2026-08-28 (log tail read, registry reload cost, task-status
  scan) — same incident, unrelated cause.

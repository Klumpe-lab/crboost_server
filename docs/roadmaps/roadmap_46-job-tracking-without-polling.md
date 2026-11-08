# Roadmap 46 — Tracking jobs without polling Slurm

**Status:** scoped 2026-11-08, no code. Everything in §2 was read from the code the same day; nothing is measured yet
(S0 does that). Open: D1 first (what IT will accept), then S0–S6 and D2–D7. Pickup: S0 needs one small change (a call
counter) and two reads of `sdiag`.

**In one line.** The server asks Slurm about every unfinished job every 3 seconds, per project, whether or not anyone
has a tab open, and every array supervisor does the same every 30 seconds from its compute node. The cluster docs ask
users to query all their jobs at once, every few minutes, to use dependencies instead of waiting scripts, and say Slurm
delays the requests of users who send too many. This roadmap makes files the source of truth for what a job is doing,
leaves Slurm only the questions a file cannot answer (is it queued, why is it waiting, did it die without telling
us), asks those rarely and in one batch, and counts every call so that the rate is a number we can show IT.

**Not in this roadmap:** what the pages show (the utilisation badge of roadmap 45 S5 is read from files too, but it is
that roadmap's); the representation of per-task status (`roadmap_task-status-state-machine.md`); anything the cluster
owns, such as Slurm's rate limits, which are asked of IT (D1) and not built around.

## 1. Goal

**Use case.** The maintainer starts a pipeline, closes the laptop, and opens the UI hours later. Every job shows
queued, running, done or failed, and every array shows how many tasks are done, as before. During the pipeline the
server's Slurm requests are a small, known number per hour, and the same count is what `sdiag` shows IT for our user.
A job that died without writing its exit marker (time limit, out of memory, node failure, `scancel`) is still
settled, and settled by an event, not by a poll that happened to catch it.

**What "fewer calls" means.** Three numbers, because one average hides which path is expensive:

1. *Server requests per hour*, by command (`squeue`, `sacct`, `sinfo`, `sacctmgr`, `scancel`), with a tab open and with
   none.
2. *Supervisor requests per hour*, from the compute nodes (they run in their own processes, outside the server's count).
3. *What IT sees*: our row in `sdiag`'s "Remote Procedure Call statistics by user", before and after a run.

The targets are D2; the proposal there is an order of magnitude below what §2 adds up to.

## 2. What exists (checked 2026-11-08)

**Every path that asks Slurm.**

| Caller | Cadence | What it asks | Could a file answer it? |
|---|---|---|---|
| `PipelineMonitor._tick_once` → `reconcile_afterok` pass 2 (`pipeline_runner.py:206`) | every 3 s (`pipeline_monitor.py:46`), once per project with `pipeline_active` and a job without an exit marker | `squeue -j <ids>`, uncached on purpose (`slurm_service.py:599`); `sacct` (`query_terminal_states`, `:661`) for a job that left the queue without a marker | queued against running: yes (§4 L1). Dead without a marker: an event (L3), else Slurm |
| `reconcile_tilt_filter_predict` (`pipeline_runner.py:396`) | the same tick | the same two queries, for a DL prediction run | the same |
| Array supervisor `wait_for_array_completion` (`drivers/array_job_base.py:622`) | every 30 s, per running supervisor, from a compute node | `squeue -j <array id>` until nothing is left | yes: the supervisor writes the manifest and the tasks write `.task_status/` |
| `_slurm_job_live` (`supervisor_owner.py:161`) | when a supervisor looks for a previous owner | `squeue -j` | partly |
| Superseded-array cancel (`array_job_base.py:749`) | once per restart | `squeue -j`, then `scancel` | no, but rare |
| `live_task_counts` (`slurm_service.py:635`) from `backend.py:1156,1166`, `pipeline_orchestrator_service.py:331` | on a user's Stop or restart | `squeue -r` | no, but rare |
| `get_user_jobs` (`slurm_service.py:547`), cached 60 s (`:337`) | `force_refresh=True` in `session_service.py:443,476,559` and `slurm_service.py:696,730` | `squeue -u` | no for the pending reason; yes for the rest |
| Pick-list extraction reconcile (`backend.py:1249`) and `session_service.py:376` | on a tick and on a click | `squeue -j` | the same as the first row |
| Landing status strip (`ui/landing_status_strip.py:159,162,201`) | when the page is open | `sinfo`, `sacctmgr` | no, cached 60 s |

**What files already say.**

- The exit markers `RELION_JOB_EXIT_SUCCESS` / `RELION_JOB_EXIT_FAILURE` are read first, without Slurm
  (`pipeline_runner.py`, pass 1: "disk sentinels (authoritative, cheapest, no SLURM call)").
- Array tasks: `.task_status/<item>.ok|fail|skip`, and `task_<idx>.out`, which Slurm creates when the task starts, is
  already what makes a task "running" and its absence "pending" (`array_tasks.py:156`, `scan_statuses`). The
  per-process scan is shared by every tab (`array_status_cache.py`, 2.5 s).
- Slurm opens `--output` (`run.out`) when a job starts, and `qsub.sh` prints `--- SLURM JOB BEGAN ---` into it
  (`config/qsub.sh:36`; the script is written with `--open-mode=append`, `slurm_service.py:183`).

**What files cannot say today.** (a) For a job that is not an array, whether it is queued or running, unless
`run.out` is read; (b) a job that died without running the trailer: a `TIMEOUT`, an out-of-memory kill, a node
failure, `scancel`; none of them runs the trailer, so none writes a marker, and today they are found by absence from
`squeue` plus a grace window plus `sacct` (`pipeline_runner.py:63`); (c) the pending reason.

**Arithmetic from the code, not a measurement.** The reconciler alone is up to 20 `squeue` calls a minute per project
with an unfinished job: 1,200 an hour, 29,000 over a 24-hour pipeline, multiplied by the number of active projects.
Each running array supervisor adds two a minute. The docs ask for "every few minutes".

**One reading of `sdiag`.** On 2026-11-08, with no crboost pipeline running, our user had 640 requests since the
counters reset at 01:00 (they reset at midnight; "Data since" says when), almost all of them this session's probes.
That is the baseline for "a person at a terminal"; it says nothing about crboost yet.

## 3. Principles

- **Files first, Slurm last.** Slurm answers three questions only: is it queued, why is it waiting, did it die
  silently. Everything else is read from the job's own files.
- **Never conclude "done" from an error or an absence.** The existing invariant (`pipeline_runner.py`, "Never concludes
  'done' from an errored squeue") stays.
- **Events over polls.** An exit marker, a dependency job, a `--signal`: the job or the scheduler tells us.
- **One counted door.** Every Slurm call goes through one function that counts it by command; every cadence is a
  named constant; the count is visible (log, and later the UI).
- **A hidden view does nothing** (CLAUDE.md): no Slurm call because a tab might want the answer.
- **The exit-marker block of `config/qsub.sh` stays byte-identical** (crboost refuses a template without it).

## 4. Design

**L1 — Started is a file.** A job that is submitted and has no `run.out` newer than its submit time is queued; with one
it is running; a marker settles it. Array tasks already work this way. This removes the queued-against-running reason
for asking Slurm. The pending *reason* ("QOSMaxCpuPerUserLimit", "Dependency") is shown on demand, when the job's
page is open, from the shared snapshot of L4.

**L2 — A heartbeat.** The job touches `<job_dir>/.alive` (content: the epoch) every 60 s. A job with no marker and a
heartbeat older than three intervals is the only case that earns a targeted Slurm query. Driver-run jobs get it from
a thread in `run_command` (`drivers/driver_base.py:348`, the single choke point for tool calls); RELION-native jobs,
which run no code of ours, get a background shell loop in the generated script (`write_sbatch_script`,
`slurm_service.py:166`) that the trailer stops. Content is a job-side epoch, so clock differences between the nodes and
the server do not matter.

**L3 — Terminal truth by event.** For each submission, a companion job with `--dependency=afterany:<id>`
(`--kill-on-invalid-dep=yes`), 1 CPU, 64 MB, a minute, on partition `c`, runs one `sacct -j <id>` and writes the final
state to `<job_dir>/.slurm_final`. It runs whatever happened to the job, so it settles `TIMEOUT`, out of memory, node
failure and `scancel`. Alongside it, `--signal=B:TERM@60` and a `trap` in the generated script write a failure marker
with its reason when the time limit is near. The existing grace window and `sacct` path stay as the fallback. The cost
is one tiny job per submission; whether IT prefers that to polling is D1.

**L4 — One shared, budgeted snapshot.** One `squeue --me -h -o "%i|%T|%r"` for the whole server, not `-r` (arrays stay
one line), read by every consumer: the reconciler's pass 2, the pending-reason display, the curation and
pick-list sites that now force a refresh. Cadence: 60 s while a view that shows it is open or a job lacks a marker and
a fresh heartbeat; 300 s otherwise; a user action (Stop, restart) may force one. The snapshot has a maximum age that
callers can read.

**L5 — Supervisors poll files.** `wait_for_array_completion` becomes: every 30 s, count `.task_status/` entries against
the manifest and look at the heartbeat of the tasks Slurm started (`task_<idx>.out` mtime); ask Slurm every 5 minutes at
most, only to catch an array that vanished with tasks neither done nor failed.

**L6 — Counted and budgeted.** `_run_command` (`slurm_service.py:339`) counts calls by command and logs an hourly line
through `services.event_log`; a ceiling per minute raises a visible warning. The supervisors' calls are counted by
their own log lines (each `[ARRAY] n task(s) still in queue` line is one `squeue`).

## 5. Decisions

- **D1 — What IT will accept** (open, asked first). Whether Slurm applies a per-user request limit and its value, what
  our user looks like to them, whether they prefer a companion job per submission to polling, and the right cadence
  for a status check. The docs say only that the limit exists.
- **D2 — Targets** (proposed, to be set after S0). Server, steady state: at most 2 requests a minute with a tab open
  and 0.2 without; supervisors: none per 30 s. The budget ceiling of L6 is set from the measured peak.
- **D3 — Heartbeat** every 60 s, stale after three missed.
- **D4 — Companion job** yes or no; follows D1.
- **D5 — Where the heartbeat comes from** for RELION-native jobs: the shell loop of L2, or none (their `run.out`
  growth is a weaker signal and may be enough).
- **D6 — When the server is down.** Files persist; on restart one snapshot pass settles everything, as the restart
  recovery already does (`pipeline_monitor.py`, startup scan).
- **D7 — Pending reasons** shown only on a job's open page, not in the roster.

## 6. Stages

**S0 — Measure (one small change).**
1. Ask IT D1.
2. Read `sdiag` before and after one full pipeline run (`sdiag | sed -n '/by user/,/Pending RPC/p'`, our row only).
3. Add the counter of L6 to `_run_command` and log it hourly; no other behaviour changes.
4. Count the supervisors' `[ARRAY] ... still in queue` lines in the `run.out` of past array jobs, which gives their
   calls per hour retroactively.
Output: a table of calls per hour by path, in this file. Everything after S0 is sized from it.

**S1 — The shared snapshot (L4).** One poller; the reconciler's pass 2, the tilt-filter and pick-list reconcilers and
the forced refreshes of `session_service.py` and `slurm_service.py` read it. Touches `pipeline_runner.py:206,396`,
`backend.py:1249`, `session_service.py:376,443,476,559`, `slurm_service.py:696,730`. The "errored squeue holds the
status" behaviour moves with it unchanged.

**S2 — Started and heartbeat (L1, L2).** `run.out` against the submit time for a queued job; `.alive` for driver jobs
and the shell loop for RELION-native jobs; a stale heartbeat triggers one targeted query. Check first that a resubmit
does not leave an old `run.out` that reads as "started" (`pipeline_orchestrator_service.py:642` clears the markers;
whether it clears the log is not yet checked).

**S3 — Supervisors poll files (L5).**

**S4 — Terminal events (L3).** The companion job and the `--signal` trap, if D4 says yes. The grace window and `sacct`
stay.

**S5 — Re-measure and write the numbers in.** `sdiag` before and after a 24-hour pipeline, the counter's table, the
supervisors' lines; against S0. Set D2 from it.

## 7. Risks

- A heartbeat or `run.out` check on `/groups` is a metadata operation: 100 running tasks at one a minute is far below
  the 3-second scans that exist, but the scan of `task_*.out` over the shared folder should be watched
  (`array_tasks.py:156` already lists the directory once per tick).
- A companion job that cannot start (the dependency never satisfied) must not hold a project open:
  `--kill-on-invalid-dep` and a timeout in the reconciler.
- The snapshot's age must be visible to the code that settles jobs, so that "absent from a 5-minute-old snapshot" is
  never read as "gone".
- Clock skew: mtimes compare against the server's clock; the heartbeat carries the job's own epoch for this reason.

## 8. Runtime checklist (after S1 and S2)

1. Start the server, open a project with a small array, submit it; the log shows the hourly Slurm count by command.
2. Close every tab; the count falls to the idle rate of D2 and the job still settles.
3. `scancel` a running job from a terminal: the project settles within the heartbeat window, with its reason.
4. Let a job hit its time limit (`--time=0:02:00` on a sleep): a failure marker with the reason appears.
5. Compare `sdiag`'s row for our user before and after.

## 9. Build record

- **2026-11-08, scoped.** Read: `pipeline_monitor.py`, `pipeline_runner.py` (pass 1 and 2), `slurm_service.py`
  (`_run_command`, `get_user_jobs`, `query_jobs_by_ids`, `live_task_counts`, `query_terminal_states`),
  `supervisor_owner.py`, `drivers/array_job_base.py`, `array_tasks.py`, `config/qsub.sh`, the cluster docs on Slurm
  jobs and scheduling. One `sdiag` reading (§2). No call rate is measured and no code changed.

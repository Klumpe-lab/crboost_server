# Roadmap 46 — Tracking and orchestrating jobs without polling Slurm

**Status:** Part A (tracking) scoped 2026-11-08; S0 measured on the 412 pilot the same day (§9): the server makes about
30 Slurm calls a minute, nearly all the reconciler's `squeue -j`, and the supervisors few. Part B (orchestration
without supervisor jobs) scoped 2026-11-09 from a full `c` that held a GPU pipeline while `g` stood idle, with an
audit of every driver's supervisor steps (§2). Open: D1 (what IT will accept, now including light work on the login
node), S1–S5, S6–S11, D2–D12. Pickup: S1, the shared snapshot, which Part B needs too; then D8 (whether a pipeline may
depend on the server being up), which decides Part B's shape.

**In one line.** The server asks Slurm about every unfinished job every 3 seconds, per project, whether or not anyone
has a tab open, and every array supervisor does the same every 30 seconds from its compute node. The cluster docs ask
users to query all their jobs at once, every few minutes, to use dependencies instead of waiting scripts, and say Slurm
delays the requests of users who send too many. This roadmap makes files the source of truth for what a job is doing,
leaves Slurm only the questions a file cannot answer (is it queued, why is it waiting, did it die without telling
us), asks those rarely and in one batch, and counts every call so that the rate is a number we can show IT.

**Part B in one line.** Every stage of a pipeline is a supervisor: a 1-CPU Slurm job on `c` that lists the stage's
items, submits their array, waits on `squeue` every 30 seconds, then merges the results. Chained by afterok, each
becomes eligible only when the stage before ends, so each joins `c`'s queue afresh, with no age: on 2026-11-09 the
anglestest pilot's first supervisor waited on a full `c` with a worst-case start two days out while `g` was idle.
Part B moves that glue out of Slurm: the tasks keep doing all per-item work and leave files; the server, which already
reads those files, merges a finished stage and submits the next one's array itself; Slurm carries the dependencies
between arrays. A GPU pipeline then queues only for GPUs (and for the CPU stages it really has, tsImport).

**Not in this roadmap:** roadmap 45's levers (chunks, lanes, card types), which Part B only has to keep working; what
the pages show (the utilisation badge of roadmap 45 S5 is read from files too, but it is
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

**Part B's use case.** The maintainer starts a GPU pipeline while `c` is full. Its GPU stages start as soon as `g`
has cards; nothing of the pipeline waits in `c`'s queue but tsImport; the job pages, logs, markers and RELION files
read as they do today; a failed item is rerun alone, as now.

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

**What a supervisor does (audited 2026-11-09).** `run_supervisor` (`drivers/array_job_base.py:1069-1156`): bootstrap,
`supervisor_owner.acquire` (an exclusive claim on the job dir, `services/supervisor_owner.py`), enumerate, preflight
(a registry load), pre-dispatch, exclusions, write the manifest, sbatch the array, `squeue` every 30 s, tally,
aggregate, exit 0 or 1, and `config/qsub.sh` turns the exit code into `RELION_JOB_EXIT_*` (array scripts have that
block removed, `slurm_service.py:214-220`, so only the supervisor writes markers); the pipeline's afterok edges point
at supervisor job ids. Cost of the steps around the array, by driver (LIGHT: small files, seconds; MEDIUM: a pass
over every tilt series' or frame's files, up to a minute; HEAVY: a container, volume data or images):

| Driver | Before the array | After the array | Barrier |
|---|---|---|---|
| `fs_motion_and_ctf` | LIGHT | MEDIUM: one Warp XML per frame, all tilt series (`fs_motion_and_ctf.py:278-298`) | – |
| `ts_alignment` | MEDIUM: registry, rebuilds the tomostar dir with the tilt cut (`:224`) | MEDIUM: .aln/.xf/XML per tilt series, one MRC header (`:291-300`) | – |
| `ts_ctf` | **HEAVY**: `WarpTools ts_defocus_hand` in the container over all tilt series (`ts_ctf.py:50-103`, `:264`) | MEDIUM: XML per tilt series (`:281`) | **yes**: the hand is one decision for all |
| `ts_reconstruct` | LIGHT | LIGHT–MEDIUM: MRC headers, star, registry (`:150-166`) | – |
| `denoise_predict` | MEDIUM: untars the IsoNet model (`:93-112`) | LIGHT | – |
| `segment` | **HEAVY**: starts the easymode container for tool versions (`segment.py:274-290`) | LIGHT | – |
| `template_match_pytom` | LIGHT–MEDIUM: the angle list (scipy over pytom's list) | LIGHT | – |
| `extract_candidates_pytom` | LIGHT | **HEAVY**: IMOD `point2model`, previews from tomogram and template data, deletes the angle maps (`:265-361`) | – |
| `subtomo_extraction` | MEDIUM: star merges, per-tilt-series staging | MEDIUM: concat, moves | – |
| `warp_export` | MEDIUM: per-tilt-series staging | MEDIUM: per-tilt-series transforms | – |

The single-job drivers (tsImport, tilt filter, missAlign, denoise train, filament pick, particle reconstruct, Class3D,
Refine3D, pick-list extraction) are the tool run itself and stay Slurm jobs. Two server-side precedents exist:
`RUNS_INLINE` jobs (import, Subset) write their output in the server at submit (`pipeline_orchestrator_service.py:
670-690`), and the reconciler already settles jobs from files. The registry has its own cross-process lock
(`services/tilt_series/registry.py`), which a server-side merge must take. The orchestrator says what Part B gives
up, in its own words: "The chain executes in SLURM regardless of the headnode process" (`:422`).

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
- **Per-item work runs in the item's task; the server only merges.** A task leaves what its item contributes
  (registry fields, star rows) in a small file of its own; the step after a stage concatenates. Parsing every XML of a
  stage is never the server's job.
- **Nothing heavy on the login node.** A container, volume data or an image render runs in a Slurm job, never in the
  server; what the server runs is star/JSON merging in a worker process (`run_in_worker`), seconds long.
- **Slurm carries the dependencies; the server carries the glue.** Arrays wait on each other in Slurm, never in a
  polling loop; the server reacts to files.
- **Every glue step is idempotent and resumable from files**, so a server restart picks a pipeline up where it was.

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

**Part B.**

**L7 — One driver, three entry points.** `ArrayDriver` splits into `plan` (enumerate, preflight, pre-dispatch,
manifest), `task` (as now) and `finish` (tally, aggregate, exit marker). The supervisor keeps calling all three, so this
step changes no behaviour; the server can then call `plan` and `finish` itself.

**L8 — Per-item contributions.** Each driver whose `finish` is MEDIUM moves its per-item parsing into `collect`
(`fs_motion_and_ctf`, `ts_alignment`, `ts_ctf`, `ts_reconstruct`, `subtomo_extraction`, `warp_export`): the task writes
`.contrib/<item>.json` (registry fields and star rows), `finish` reads those and writes the star and the registry under
its lock. `finish` becomes LIGHT; it also makes item flow possible, where a stage has no single end.

**L9 — The server runs the glue.** When a stage's items are all settled (markers, plus S1's snapshot for tasks that
died without one), the pipeline monitor runs its `finish` in a worker, writes `RELION_JOB_EXIT_*` itself, and for each
consumer whose producers are all done runs `plan` and sbatches its array. The supervisor's owner claim becomes a
per-job lock in the server plus the same file, so a second server cannot double-submit. Stop and Cancel `scancel` the
arrays. `job.star`, `default_pipeline.star` and the markers stay as RELION expects them.

**L10 — Heavy steps get a job of their own, where they belong.** `ts_ctf`'s defocus hand runs as a one-task array of
its own before the CTF array (on a slice in `g`, which is idle when `c` is full); `segment`'s tool versions are read
once per image and cached beside the image (the kernel-cache folder); extraction's previews and IMOD models move into
the per-tomogram tasks (`collect`), and the angle-map deletion with them. None of them then runs on `c` or on the login
node.

**L11 — Item flow.** On top of L9: every stage's array is submitted at the start, each task waiting on its own
upstream task (`--dependency=aftercorr`, roadmap 45 P12), so a tilt series moves on when its own work is done (the
replay: 1:37 to 0:59, 0:39 with tsImport split). Needs one array index per tilt series across all stages (sparse
`--array` lists keep indices), barriers (the defocus hand) as whole-array dependencies, and a reconciler that reads
"skipped: upstream failed" from the upstream `.fail`, because `kill_invalid_depend` cancels such a task without a
record. Chunks (roadmap 45 L5) and item flow are at odds where a chunk's items finish upstream at different times;
item flow wins for the short stages, chunks stay for template matching, which follows a barrier anyway.

**L12 — Supervisor mode stays.** `orchestration: supervisor | server` in `conf.yaml`, default `supervisor` until S8
has run; a site where the server cannot stay up keeps the chain in Slurm.

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
- **D8 — May a pipeline depend on the server?** With L9 a stage's arrays run whatever the server does, but the next
  stage is submitted only while it runs: a server down overnight stalls a pipeline between stages (it resumes on
  restart, from files). Item flow (L11) narrows this to the barriers and the final merges. The alternatives: accept it
  (and keep the server under a supervisor of its own, e.g. a user systemd unit or tmux, roadmap 39); or keep supervisor
  mode for unattended runs. The maintainer's call.
- **D9 — Where glue may run.** Proposed: star/JSON merges in the server's worker processes; containers, volumes and
  images never on the login node. Asked of IT together with D1: whether a long-running server doing seconds of file
  merging per stage on a login node is acceptable, or whether they prefer it elsewhere (an OpenOnDemand session, a
  service node).
- **D10 — Heavy steps' placement** (L10): the defocus hand on a slice in `g`, or a CPU job on `c` (it would then queue
  there again, but once per pipeline, not once per stage).
- **D11 — Index per tilt series for item flow**: the registry's order at import, fixed for the project's life, or a
  per-pipeline map written at submit.
- **D12 — Default orchestration** after S8: `server` for this lab's deployment, `supervisor` in the template.

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

**Part B.** S1 comes first: the server needs one shared snapshot to tell a task that died from one that is queued.

**S6 — Split the driver (L7).** `plan` / `task` / `finish`, called in turn by the supervisor; no behaviour change.
Checked by rerunning a finished pilot's stages in supervisor mode and comparing outputs.

**S7 — Per-item contributions (L8),** driver by driver, starting with `fs_motion_and_ctf` (the largest merge). The
supervisor's `finish` reads the contributions; outputs compared with today's.

**S8 — The server runs the glue (L9, L12),** stage by stage, behind `orchestration: server`. One 412 pilot to template
matching in each mode while `c` is busy: stage start delays, wall time, the server's Slurm calls (S0's counter), and
the outputs identical.

**S9 — Heavy steps (L10).** The defocus-hand job, the cached tool versions, extraction's previews in its tasks.

**S10 — Item flow (L11),** after D11; the replay's numbers checked on a pilot.

**S11 — tsImport faster, not per tilt series** (built 2026-11-09, roadmap 45 §11). Roadmap 45's P11: the 12 minutes
are CPU work on one CPU, with about 64 s of fixed cost per `ts_import` call, so a task per tilt series would mostly
start up. Instead the job asks for 4–8 CPUs and runs 4–8 calls at once (1.5 minutes, identical tomostars); it stays
the one job a GPU pipeline has on `c`.

## 7. Risks

- A heartbeat or `run.out` check on `/groups` is a metadata operation: 100 running tasks at one a minute is far below
  the 3-second scans that exist, but the scan of `task_*.out` over the shared folder should be watched
  (`array_tasks.py:156` already lists the directory once per tick).
- A companion job that cannot start (the dependency never satisfied) must not hold a project open:
  `--kill-on-invalid-dep` and a timeout in the reconciler.
- The snapshot's age must be visible to the code that settles jobs, so that "absent from a 5-minute-old snapshot" is
  never read as "gone".
- Clock skew: mtimes compare against the server's clock; the heartbeat carries the job's own epoch for this reason.
- Part B: two servers on one project (two login nodes) would both submit the next stage; the per-job lock of L9 must
  be a file that both see, not memory.
- Part B: a merge in the server that throws must fail the job visibly (a failure marker with the reason), never leave
  it "running"; and it must not block the event loop (`run_in_worker`, CLAUDE.md).
- Part B: more arrays queued at once under item flow (60–80 tasks running, roadmap 45's replay) is more for S1's
  snapshot to cover and more for the reconciler to read; the snapshot stays one `squeue --me`.
- Part B: per-item contribution files add a few small files per item on `/groups`; written once, read once.

## 8. Runtime checklist (after S1 and S2)

1. Start the server, open a project with a small array, submit it; the log shows the hourly Slurm count by command.
2. Close every tab; the count falls to the idle rate of D2 and the job still settles.
3. `scancel` a running job from a terminal: the project settles within the heartbeat window, with its reason.
4. Let a job hit its time limit (`--time=0:02:00` on a sleep): a failure marker with the reason appears.
5. Compare `sdiag`'s row for our user before and after.

## 9. Build record

- **2026-11-09, S11 built (not run).** tsImport takes its own profile (8 CPUs, 16G, partition `c` in the dev conf)
  instead of the supervisor's, and runs 4 `ts_import` calls at once, each into its own folder, landed in `tomostar/`
  by one rename (roadmap 45 §11). It stays the one job a GPU pipeline has on `c`.
- **2026-11-09, Part B scoped.** Trigger: the anglestest pilot (roadmap 45) queued behind its first supervisor on a
  full `c` (worst-case start 2026-11-11 19:00) with `g` idle; the maintainer asked whether supervisors can go and
  chose to fold it into this roadmap. Read: `pipeline_orchestrator_service.py` (`_submit_chain`, the inline writers),
  `pipeline_runner.py`, `pipeline_monitor.py`, `drivers/array_job_base.py`, and every driver's supervisor hooks (the
  audit table of §2, by a read-only sweep). No code changed.

- **2026-11-08, scoped.** Read: `pipeline_monitor.py`, `pipeline_runner.py` (pass 1 and 2), `slurm_service.py`
  (`_run_command`, `get_user_jobs`, `query_jobs_by_ids`, `live_task_counts`, `query_terminal_states`),
  `supervisor_owner.py`, `drivers/array_job_base.py`, `array_tasks.py`, `config/qsub.sh`, the cluster docs on Slurm
  jobs and scheduling. One `sdiag` reading (§2). No call rate is measured and no code changed.
- **2026-11-08, S0 step 3 built (not run).** `SlurmCallCounter` (`services/computing/slurm_service.py`) counts every
  command `SlurmService._run_command` runs, by kind (`squeue -j`, `squeue -r`, `squeue -u`, `sacct`, `sinfo`,
  `sacctmgr`, `scancel`), and writes one event line an hour: `Slurm calls in the last 60 min: N (n a minute): ...`.
  Not counted: the sbatch submits, `supervisor_owner`'s `squeue`, the constraint check's cached `sinfo`, and the array
  supervisors on the compute nodes (step 4 counts those from their logs). A second `sdiag` reading, 16:35 with no
  pipeline running: 677 requests since 01:00 for our user. Step 2 is the maintainer's: the same reading before and
  after a full pipeline.
- **2026-11-08, S0 step 2: the first pipeline under the counter.** `slurmtest_412_FCIsolation` (163 tilt series, motion
  to Warp export, server on cbe-login-1 port 24326, restarted on the new code just before). `sdiag` before the
  submit: 704 requests since 01:00. The chain was submitted at 16:59:49; the "after" reading and the server's hourly
  lines (`~/.crboost/logs/crboost-24326.log`) go here when it ends.
- **2026-11-08, every job's log names its Slurm ids.** `write_sbatch_script` puts one line after the directives of
  every script it renders (`SLURM_ID_LINE`): `[SLURM] job 1476473_17 = 1476491 cb-templatematching on cbe-g5-3 (g),
  GPU GPU-…, started …` for an array task, `[SLURM] job 1476464 cb-tsImport-sup on cbe-c3-2 (c), started …` for a
  single job or supervisor. It is the first line of every `run.out` and `task_N.out`, so a job's ids come from its
  files without a Slurm query.
- **2026-11-08, S0 measured on the pilots** (the 412 pilot plus the copia run, both active for most of the hour).
  The server's counter, first hourly line (18:06): 1,827 calls in 60 minutes, 30.4 a minute: `squeue -j` 1,802,
  `sacctmgr` 12, `sinfo` 10, `sacct` 3. The array supervisors' logs: 183 `still in queue` lines over the whole run,
  one `squeue` each. `sdiag` for our user: 704 before the submit (16:59), 7,682 at 18:39, so about 7,000 requests in
  100 minutes, most of them the reconciler's 3-second `squeue -j` (an squeue can cost the scheduler more than one
  request). So the server, not the supervisors, is the rate to cut: S1's shared snapshot first.
- **2026-11-09, what roadmap 45's pipeline-shape probes mean for tracking** (roadmap 45 §11, P12 and the full `c`).
  - Supervisors are Slurm jobs on `c`, one per stage, and each queues there when its turn comes. With `c` full (4 of
    2,112 cores idle at 10:41) a GPU pipeline waited on CPU capacity while `g` had idle nodes.
  - `--dependency=aftercorr` starts task i of a dependent array when task i upstream ends, and the cluster's
    `DependencyParameters=kill_invalid_depend` cancels a task whose counterpart failed or does not exist. Such a task
    never runs and leaves no `sacct` record, so a tracker must read it from the upstream failure, not look for it.
  - Item flow submitted by the server (every stage's array at once, chained per task) would remove the supervisor
    jobs and their `squeue` lines; it puts more jobs in the queue at once, which S1's shared snapshot has to cover.

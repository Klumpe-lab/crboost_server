# Roadmap 27 — The tilt-series axis: failure isolation, catch-up, streaming

**Status:** Stage 1 (failure isolation) LANDED 2026-10-04 as a temporary fix, **NOT RUN**. It has been read and
structure-checked only; there is no interpreter in the assistant sandbox. The AreTomo name-length fix (§4.3) also
landed 2026-10-04 and is not run. Stages 2 (catch-up) and 3 (streaming) were scoped 2026-10-04 and are **not
started**. **Supersedes roadmap 05 (per-ts-top-up):** stage 2 is 05's scope. The trigger was a production
incident on eight AGG412 projects (§2).

**In one line.** A tilt-series that fails leaves only itself behind (stage 1). It can catch up later without
recomputing anyone else (stage 2). Eventually no tilt-series waits for the slowest of the others at every stage
(stage 3).

## Before → After

**Before (until 2026-10-04):**
- One bad tilt-series (TS) stopped the whole dataset at the next job boundary.
- A TS that was fixed could rejoin only by recomputing its job, and every job after it, for every TS.

**After stage 1:**
- A bad TS is dropped with a reason, and the rest carry on.

**After stage 2:**
- A dropped or restored TS rejoins with one click.
- Only that TS is recomputed, in every job it missed.

**After stage 3:**
- Each TS moves to the next job as soon as it is done with the current one.
- The first tomograms exist after about one TS's worth of latency, not after the slowest TS of every stage.

---

## 1. The thesis: the pipeline is a grid

Processing is a grid of jobs × tilt series. The per-TS cells already exist:

- Every array job keeps one status marker per TS in `External/jobNNN/.task_status/`: `.ok`, `.fail` or `.skip`
  (`drivers/array_job_base.py:116`, `:163`).
- Every task stages exactly one TS (`stage_per_ts_environment`, `array_job_base.py:454`).
- Re-running a FAILED job reuses its directory. It dispatches only the TS without `.ok`/`.skip`
  (`submit_array_job`, `array_job_base.py:573-632`; the dir reuse is `_submit_chain`'s `reuse_dir`,
  `services/scheduling_and_orchestration/pipeline_orchestrator_service.py:505-515`).

What is coarse are the edges between jobs:

- **Start signal.** A job starts when its upstream *supervisor* exits 0 (`--dependency=afterok`,
  `pipeline_runner.py:1115-1120`). A non-zero exit leaves every dependent `DependencyNeverSatisfied`, and
  `reconcile_afterok` scancels them (`pipeline_runner.py:507`).
- **Which TS flow on.** They are the rows of the upstream job's output STAR
  (`read_tilt_series_names_from_input_star`, `array_job_base.py:245`).
- **The exit code.** It was strict for every array job except alignment: the old base `tally_acceptable` returned
  `results.all_succeeded`. One failed TS failed the job and, through afterok, everything after it.

The three stages change the edges, never the cells:

| | Edge between jobs | What one bad TS costs |
|---|---|---|
| before | per job, all-or-nothing | the dataset, from the next job on |
| stage 1 | per job, tolerant | itself, dropped downstream |
| stage 2 | per job, tolerant, can be re-opened | itself until retried; a retry recomputes only it |
| stage 3 | per TS | itself; nobody waits for it |

---

## 2. The incident (2026-10-04)

The eight projects are `/groups/klumpe/crboost_data/AGG412_*`. Each runs importmovies → fsMotionAndCtf → tsImport →
tiltFilter (DL review) → aligntiltsWarp → tsCtf → tsReconstruct, from `~/dev/crboost_server`, under the afterok
orchestrator.

| Project | Alignment ok / fail | What happened |
|---|---|---|
| `AGG412_SK_20251017_412` | 133 / 4 | tsCtf died; see SK below |
| `AGG412_20260311_MyrGFP_Grid2` | 49 / 1 | tsCtf died; see MyrGFP below |
| `AGG412_20251031_412_FCIsolation` | 58 / 105 | name-length bug, then tsCtf died; see below |
| `AGG412_20260205_412_FCIso_unmilled2` | 0 / 176 | name-length bug wiped out alignment; see below |
| `AGG412_20240204_412_Slot6` | 123 / 3 | ran on (tsCtf 123/123) |
| `AGG412_20260311_412_Grid3` | 95 / 1 | ran on |
| `AGG412_20251113_412` | 141 / 10 | ran on |
| `AGG412_20260204_412` | 23 / 0 | done |

- **SK.** `Position_22_5` had 0 tilts left after the tilt filter.
  - Alignment dropped it. tsCtf's `ts_defocus_hand` still parsed its empty tomostar.
  - Warp threw "Metadata must contain at least 3 values per tilt" and the step aborted three times.
  - tsCtf FAILED, and afterok cancelled tsReconstruct.
- **MyrGFP.** `Position_10_4` had 1 tilt left.
  - AreTomo "aligned" it, so it was in the STAR.
  - `--check` hit a NaN gradient: `Math.Sign(NaN)` in Warp's single-tilt branch (`DefocusHandTiltseries.cs:120`).
    The step aborted.
- **FCIsolation.**
  - 84 failures are `Could not find <ts>.tlt`: the name-length bug, known_bugs #3.
    - All 97 series with names of 45 characters failed: 84 `.tlt` missing, 12 with no `.xf`, 1 empty tomostar.
    - 58 of the 66 series at 42–44 characters aligned.
  - tsCtf then died on the empty tomostars of `Position_29` and `Position_40_2`.
- **unmilled2.**
  - All names are 46–49 characters: 164 `.tlt` missing, 8 with no `.xf`, 4 empty tomostars.
  - That is a total wipeout, so alignment FAILED.
- **Across all eight projects**, no series with a name of 45 or more characters aligned (0 of 273). The longest name
  that aligned has 44.

Alignment was already tolerant. Three other defects combined to stop the runs:

1. **A global step saw every TS, not the live ones.**
   - tsCtf's supervisor copied the alignment job's whole `tomostar/` dir (`shutil.copytree`). It did so only once,
     guarded by `if not local_tomostar.exists()`.
   - `ts_defocus_hand` parses every `*.tomostar` in the settings' DataFolder.
   - So a TS that alignment had dropped came back and aborted the step, and a retry reused the poisoned copy.
2. **User exclusions arrived after the global step.**
   - `ArrayDriver.run_supervisor` runs `pre_dispatch` (`array_job_base.py:880`) before `apply_exclusions` (`:893`).
   - So excluding the TS in the Journey would not have saved the run either.
3. **Every array job except alignment was strict.** Any per-TS failure from tsCtf on stopped the dataset.

The name-length bug is a separate class (known_bugs #3). It turned "a few TS fail" into "most TS fail" in two
projects. It also showed why tolerance has to be loud (§4.2, §4.4 item 7): FCIsolation's alignment "succeeded" with
58 of 163.

---

## 3. Three words for "not in this job's output"

The stage-1 code and every later stage use these:

- **excluded:** the user's decision.
  - Stored as `TiltSeries.is_excluded` in the registry and toggled in the Journey strip
    (`ui/tomo_dashboard_dialog.py:363-385`).
  - Forward-only: each job pre-marks the TS `.skip` at dispatch, with the reason `excluded from processing`
    (`apply_exclusions`, `array_job_base.py:328-359`).
- **skipped:** a `.skip` marker; the job deliberately did not run the TS.
  - Causes: the TS is excluded, or there is nothing to do (no candidates, filtered out by name).
  - It counts as settled, not as a failure.
- **dropped:** a `.fail` marker, or no marker at all once the array has left the queue (`ArrayResults.dropped`,
  `array_job_base.py:230`).
  - The TS ran and did not succeed.
  - The job leaves it out of its output, so it is absent from everything downstream.
  - It is re-evaluated on every run of that job.

A drop is never written back as an exclusion, for two reasons:
- An automatic exclusion would be sticky. A TS lost to a bad node would stay out forever.
- The UI could no longer tell "I muted it" from "it broke".

---

## 4. Stage 1: failure isolation (LANDED 2026-10-04, temporary, NOT RUN)

### 4.1 What changed

Seven driver files changed; nothing outside `drivers/` was touched.

**`drivers/array_job_base.py`**
- `write_status_atomic(..., reason="")` writes the reason into the marker (`:116`). Readers only look at the
  suffix, so the content is free.
- `failure_reason(exc)` gives the first line of the exception. For a `CalledProcessError` it gives
  `tool exited with status N`, because the exception message is just the container command line (`:152`).
- `ArrayResults.dropped` (`:230`).
- `collect_task_results` counts only markers of this run's items (`:262-280`). A stale `.ok` of a TS no longer in the
  input could otherwise pass the tolerant tally and be ingested.
- `MISSING_TASK_REASON` and `log_dropped` (`:282-306`): a grouped `DROPPED n/N tilt-series … By reason:` block in
  `run.out`.
- `tally_acceptable` is tolerant by default: `bool(results.ok) or not results.dropped` (`:786-792`).
- The supervisor marks missing items failed with a reason, prints the summary, then aggregates the survivors
  (`:913-936`). A task's `.fail` carries `failure_reason(e)` (`:984`).

**`drivers/ts_alignment.py`**
- Its own tolerant `tally_acceptable` is deleted, because the base default is now the same policy.

**`drivers/ts_ctf.py`**
- `pre_dispatch` scope is the items minus the excluded TS (`:212`).
- The job-local `tomostar/` is rebuilt on every run with only the in-scope TS, and fails loud if none is found
  (`:244-261`).
- With an explicit hand (`set_flip`/`set_noflip`), `--check` is only a diagnostic: `check || echo "[WARN] …"; set_*`
  (`:81-88`).
- `aggregate` drops `skipped ∪ dropped` (`:283`).

**The other four drivers**
- `drivers/ts_reconstruct.py`: `aggregate` drops `skipped ∪ dropped` (`:129`).
- `drivers/fs_motion_and_ctf.py`: the same (`:295`).
- `drivers/extract_candidates_pytom.py`: the merge ignores the per-TS particle files of skipped and dropped TS
  (`:244`).
- `drivers/subtomo_extraction.py`: merges only TS that have picks and were not dropped (`:412`).

**Already right, unchanged**
- `denoise_predict` keeps only `.ok` rows (`drivers/denoise_predict.py:109-145`).
- Alignment's `emit_star` already dropped non-aligned TS.
- Template matching copies its input STAR on purpose (§4.4 item 4).

**Deliberately not changed: the base order `pre_dispatch` → `apply_exclusions`.**
- Moving `apply_exclusions` first would break `subtomo_extraction`. Its `pre_dispatch` deletes every `.skip` and
  relies on the base to re-write the excluded ones afterwards (`drivers/subtomo_extraction.py:374-386`).
- tsCtf reads the exclusion set itself instead.

### 4.2 Semantics

- **When a job fails.** Only when nothing succeeded and something failed. An all-skipped job still succeeds, as it
  did under the strict rule.
- **A job that succeeded with drops:**
  - It exits 0 and writes `RELION_JOB_EXIT_SUCCESS`.
  - Its row is green with the red `n!` count the roster already draws from `.fail` markers
    (`ui/pipeline_builder/pipeline_roster.py:526-531`).
- **Why each drop happened:**
  - It is in `.task_status/<ts>.fail`, and grouped in `run.out`.
  - The grouping replaces the TS name with `<ts>`, so a systematic cause reads as one line, e.g.
    `84 × WarpTools marked <ts> unselected: …`.
  - That line is the only thing standing between tolerance and a silent loss like FCIsolation's.
- **When it takes effect:**
  - For every supervisor and array task that started after the swap (2026-10-04 16:13).
  - Drivers are run from disk, so no server restart is needed (nothing outside `drivers/` imports them).
  - Supervisors already running kept the code they had loaded.

### 4.3 The name-length fix (LANDED 2026-10-04, NOT RUN)

**Alignment now runs AreTomo under a short alias** (`crbalias`) in each per-TS staging dir, for AreTomo names over
44 characters only, and restores the real name when it collects the outputs (`drivers/ts_alignment.py`
`staged_ts_name`, `rename_alias_files`, the `stage` / `verify_outputs` / `collect` hooks). Same day:
- **The limit has one home:** `ARETOMO_TS_NAME_MAX = 44` and `describe_long_ts_names` in
  `services/configs/mdoc_service.py`. 44 is the longest name that aligned on the AGG412 data. This replaced
  `ARETOMO_TS_NAME_MAX_KNOWN_GOOD = 42` in `services/protocols/apply.py`; `long_tilt_series_name_warnings` there
  now wraps the shared helper, and series names come from `ts_name_from_mdoc` rather than `Path.stem`, which kept
  the `.mrc` of `X.mrc.mdoc`.
- **Project creation refuses a name that would need the alias:**
  - The landing form adds a "Shorter project name" requirement, with the reason on the red status line.
  - The protocol create dialog refuses before `apply_protocol` runs.
- **known_bugs #3** carries the details and the sightings.

**What reads the real names after collect, and so has to see them restored:**
- The alignment ingest adapter.
  - It needs `warp_tiltseries/tiltstack/<ts>/` (`services/tilt_series/adapters/ts_alignment.py:273`).
  - It checks that the tomostar, per-TS XML and tiltstack-dir sets agree (`:479`).
  - The files inside the dir it globs, so their names don't matter to it.
- miss_align reads `tiltstack/<ts>/<ts>.st` (`drivers/miss_align.py:131`, `:321`). The file name matters here.
- `has_alignment_output`, `warp_marked_unselected` and `task_already_done` in `drivers/ts_alignment.py`.

**What it means for the two affected projects:**
- **unmilled2:** alignment is FAILED, so Run retries it in place with all 176 items.
- **FCIsolation:** alignment SUCCEEDED with 58, so Run skips it (§4.4 item 1). Its 105 dropped series rejoin only in
  one of two ways:
  - Delete aligntiltsWarp and the jobs after it, then re-add them. The new dirs re-align all 163, including the 58.
  - Or wait for stage 2.

### 4.4 Known gaps and things found along the way

1. **A dropped TS stays dropped.**
   - Run skips SUCCEEDED jobs (`pipeline_orchestrator_service.py:176-185`).
   - So a TS lost to a passing cause (node rot, OOM) never comes back without stage 2.
   - The same holds for FCIsolation's 105 now that the name fix exists.
2. **`defocus_hand=auto` silently falls back to `set_noflip` when `--check` crashes.**
   - The auto branch, `drivers/ts_ctf.py:71-80`, is
     `hand_output=$(check 2>&1); if … grep -q "should be set to 'flip'"; then set_flip; else set_noflip; fi`.
   - It runs under `bash -c` with no `set -e` (`services/computing/container_service.py:91-101`; `run_command` uses
     `Popen(shell=True)`).
   - So a crashed check gives no verdict string, takes the `else` branch, runs `--set_noflip` and exits 0. The job
     succeeds with a hand nobody decided.
   - A wrong hand flips `AreAnglesInverted`, the sign of the tilt angles Warp uses from then on. Each tilt's defocus
     gradient is modelled with the wrong sign, and the tomogram comes out mirrored. Neither stops a job; both show
     up weeks later in STA.
   - **Fix:** `elif … grep -q "should be set to 'no flip'"; then set_noflip; else echo "no verdict" >&2; exit 1; fi`.
     Warp prints "…should be set to 'no flip'" for a positive average correlation; confirm both strings in stage 0.
   - Put it in its own commit, since it changes behavior.
   - The default hand is `set_flip` (`services/jobs/ts_ctf.py:53`), so auto is opt-in. All eight AGG412 projects use
     `set_flip`.
3. **No minimum-tilts gate.**
   - A TS the filter leaves with 0 tilts fails alignment, which is fine. One left with 1 tilt "aligns" and flows on.
   - Lowest tilt counts among aligned series on 2026-10-04:
     - MyrGFP: `Position_10_4` 1, `Position_10_2` 10.
     - FCIsolation: `Position_31` 12.
     - Slot6: `Position_3_7` 15.
     - 20251113: `Position_16_2` 17.
   - **Where the gate goes:**
     - Right after `drop_tilts_from_tomostar` in `TsAlignmentDriver.enumerate_items`: count each local tomostar's
       rows, and below N write a `.skip` with the count as its reason.
     - The same check in tsCtf, for projects that are already aligned.
   - N is the maintainer's call (§10). The code must not invent one.
4. **Template matching keeps the rows of TS it failed.**
   - `TemplateMatchPytomDriver.aggregate` copies its input STAR verbatim (`drivers/template_match_pytom.py:345-351`).
     This is deliberate: downstream consults the registry for mutedness.
   - Candidate extraction enumerates from TM's `*_job.json` files (`drivers/extract_candidates_pytom.py:193`).
   - So a TS whose TM task failed after writing its json is dispatched again and dropped a second time. That is
     noise, not damage.
   - **Fix:** candidate extraction pre-skips a TS whose TM marker is `.fail`.
5. **miss_align has no per-TS policy.**
   - It is one non-array GPU job that refines every TS in its input star jointly (`drivers/miss_align.py:1-20`).
     Its input already lacks the dropped TS (`:310`).
   - Its change report tolerates a series without a refined XML (`:519-525`).
   - What the tool itself does with a series it cannot refine is unchecked (stage 0 item 6).
6. **The job row has no "partial" state.** The roster shows `n!` but not why; the reasons are only in `run.out` and
   the `.fail` files (UI in stage 2, §6.5).
7. **No drop-rate guard.**
   - FCIsolation's alignment succeeded with 58 of 163, and with stage 1, tsCtf and tsReconstruct carry on with 58.
   - The grouped summary makes the cause readable, but nothing stops the run.
   - A guard (fail when more than X % are dropped) and the value of X are the maintainer's call.
8. **Missing tasks are not retried.**
   - A task that died without reporting (`MISSING_TASK_REASON`) is infrastructure by definition.
   - Option: re-dispatch missing items once, inside the same supervisor run, before tallying. `submit_array_job`
     already dispatches only unsettled items.
   - The cost is a second array round on a bad-node day; maintainer's call.
9. **The tsCtf tomostar rebuild mutates a shared input in place** (`rmtree` then copy, `drivers/ts_ctf.py:250-258`).
   - This is the hazard roadmap 15 S2 names for alignment's identical rebuild.
   - Fold it into 15 S2's atomic-swap sweep.
10. **Restoring an excluded TS does not bring it back** (§6.3).

---

## 5. Stage 0 for stages 2–3: facts to gather first

1. **Everything keyed by array index.** Known so far:
   - `task_%a` log names (`services/computing/slurm_service.py:123`).
   - The running/pending inference in `scan_statuses` (`services/array_tasks.py:177`).
   - `mark_stopped_tasks_failed` (`:237`).
   - The per-TS log links in the tracker and Journey.

   Then decide between an append-only manifest and a per-item record (§6.1).
2. **What a downstream supervisor does when its input STAR gained rows** since its last run. Expected: it dispatches
   only the new TS. Confirm per driver, including TM's verbatim copy and subtomo extraction's merge.
3. **Warp's `ts_defocus_hand --check` verdict strings** in the container's Warp (2.0.0dev36). Also: does `--check` on
   a handful of well-formed TS give a stable verdict? §7.3 depends on it.
4. **tsImport per batch.** Does `WarpTools ts_import` skip or fail a mdoc whose frame series are not processed yet?
   That decides whether tsImport can run per batch in stage 3, or stays a cheap barrier that is simply re-run.
5. **What denoise_train really needs.** The smallest set it trains on decides whether stage 3 can start it at
   "N tomograms exist" rather than "all exist".
   - IsoNet: the `tomograms_for_training` filter plus the `isonet_max_training_tomograms` cap
     (`drivers/denoise_train.py:66-100`).
   - cryoCARE: the same filter (`:415`).
6. **miss_align with a series it cannot refine** (§4.4 item 5).
7. **SLURM on this cluster:**
   - What `aftercorr` does when the corresponding task fails: pending forever, or cancelled
     (`SlurmctldParameters=kill_invalid_depend`).
   - `MaxArraySize`.
   - The per-user job limit, since a streaming supervisor holds a CPU slot for a whole run.
8. **What latency costs today.** For the AGG412 projects, wall time per stage and per TS, from the `task_N.out`
   timestamps. This is the number that says whether stage 3 is worth its price.

---

## 6. Stage 2: catch-up (per-TS top-up; absorbs 05)

**Goal.** A dropped TS, or one that was excluded and is now restored, rejoins the run:
- Its own job runs it again in place.
- Every downstream job that already succeeded runs it, and only it, in place.

Nothing that already succeeded is recomputed.

### 6.1 Per-TS state

- Build the task-status-state-machine §5 record: `{state, array_job_id, task_idx, attempt}` per item, plus a `reason`
  and the item's own log path. Today one item's state is spread over up to three marker files, and its log is keyed
  by array index.
- **Why it is a prerequisite: a catch-up grows downstream manifests.**
  - The manifest is the sorted upstream TS list (`write_manifest`, `array_job_base.py:618`, fed by the sorting
    `read_tilt_series_names_from_input_star`). So a recovered TS is inserted in the middle and shifts every later
    index.
  - The earlier run's `task_<idx>.out` files then belong to other TS.
  - `scan_statuses` then infers "running" from the wrong file.
- **Cheaper alternative for the index problem alone: an append-only manifest.**
  - An existing item keeps its index, and a new item goes at the end.
  - An item that is gone upstream is marked, not removed.
  - Decide in stage 0.

### 6.2 "Retry dropped": what must change

**Today:**
- `_deploy_locked` runs only jobs that are not SUCCEEDED (`pipeline_orchestrator_service.py:176-185`).
- `_submit_chain` reuses a dir only for a FAILED job (`reuse_dir`, `:505-515`). Any other job gets a fresh
  `External/jobNNN` from the counter, i.e. a full recompute.

Both are right for "run the pipeline". Neither can say "re-open a succeeded job for its dropped TS".

**Change:**
1. **A per-job action "Retry dropped" and a project action "Catch up".** Both compute the *re-open set*:
   - the job itself;
   - every succeeded job downstream of it in `pipeline_order` (`PathResolutionService.resolve_edges`,
     `services/path_resolution_service.py:291`).
2. **`_deploy_locked` takes the re-open set** into `instances_to_run`. This is a flag on the request, not on the job:
   plain Run keeps today's meaning.
3. **`_submit_chain`'s `reuse_dir` becomes "FAILED or re-opened"**, under the same conditions (`relion_job_number`
   set, dir exists).
   - Exit sentinels are cleared as today (`:526`).
   - `.ok` markers stay.
   - `.fail` markers are cleared by `clean_status_dir` at dispatch.
4. **Afterok edges between re-opened jobs are wired as today** (`:587-601`). A re-opened downstream job waits for its
   re-opened producer.
5. **Each re-opened supervisor**:
   - dispatches only items without `.ok`/`.skip` (already true, `get_previously_done`, `array_job_base.py:188-197`);
   - re-aggregates its output from all `.ok` items. Ingest overwrites per job instance, e.g.
     `TsCtfIngestAdapter.ingest`, `services/tilt_series/adapters/ts_ctf.py:53-56`.
6. **Global jobs downstream are not re-opened automatically:** `reconstructParticle`, `class3d`, `denoise_train`,
   `missAlign`. They are whole-dataset computations; the UI offers a rerun and does not run one.

### 6.3 Restoring an excluded TS takes the same path, and is broken today

**The problem:**
- The Journey toggle flips `is_excluded` and reports "restored to processing" (`ui/tomo_dashboard_dialog.py:383`).
- But every job that ran while the TS was excluded holds a `.skip` for it, written by `apply_exclusions` with the
  reason `excluded from processing` (`array_job_base.py:353`).
- `apply_exclusions` never removes the skip of a TS that is no longer excluded.
- `get_previously_done` treats `.skip` as settled, and `clean_status_dir` removes only `.fail` (`:200-216`).
- So even an in-place rerun never dispatches the restored TS.
- `subtomo_extraction` is the one exception: its `pre_dispatch` clears every `.skip`
  (`drivers/subtomo_extraction.py:383`).

**The fix:**
- `apply_exclusions` removes `.skip` markers whose content is `excluded from processing` for TS that are no longer
  excluded.
- "Restore" then means un-exclude plus the catch-up of §6.2, from the first job that skipped the TS.

### 6.4 Global steps during a catch-up

- **The defocus hand.**
  - A tsCtf top-up re-runs `ts_defocus_hand` over all in-scope TS, including those already CTF-fitted.
  - With an explicit hand that is harmless, because it is idempotent.
  - With `auto`, a verdict over a larger set can differ from the first run's, and the dataset would mix hands.
  - Rule: the hand is decided once per project and recorded (§7.3). A top-up applies the recorded hand and never
    re-decides it.
  - This is a stage-2 requirement, not only a stage-3 one.
- **Re-aggregated STARs.**
  - Every aggregate rewrites its STAR from all `.ok` items, and downstream supervisors re-read it.
  - `StarfileService.write` writes in place (`services/configs/starfile_service.py:17-27`). A consumer starting
    mid-write would read half a STAR, so writes must become tmp + `os.replace`.
- **Merges** (`candidates.star`, subtomo `particles.star`).
  - Their aggregates rebuild them from the per-TS files.
  - That is correct as long as earlier runs' per-TS outputs survive in place. They do, because staging is per TS.

### 6.5 UI

- **Job row:** `123/137 · 4 dropped` in place of the bare `4!`, plus:
  - a popover that groups the reasons the way `log_dropped` does;
  - **Retry dropped**.
- **Journey, per TS:** where the TS left and why (e.g. "dropped at tsCtf: tool exited with status 134"), next to the
  exclude toggle.
- **Project:** one line when a run left TS behind: "12 tilt-series dropped across 3 jobs — Catch up".

### 6.6 Interaction with roadmap 15

- Catch-up turns in-place reruns of succeeded jobs from an exception into a button.
- A second click while a top-up is running would put two supervisors in one dir: exactly roadmap 15's incident.
- So 15 S1 (the ownership lock) is a prerequisite of stage 2.
- 15 S4 (the submit-side pre-flight) gives the button its "already running as slurm N" refusal.

### 6.7 Commits

- **2a.** Clear stale exclusion `.skip` markers (§6.3). This is a behavior change, so it is its own commit.
- **2b.** The per-item state record, or the append-only manifest (§6.1).
- **2c.** Atomic STAR writes (§6.4).
- **2d.** Record the hand decision per project; tsCtf applies it (§6.4, §7.3).
- **2e.** The re-open set, the `reuse_dir` extension and "Retry dropped" (§6.2).
- **2f.** The UI (§6.5).

---

## 7. Stage 3: streaming

**What it buys:**
- A TS moves to the next job as soon as it finishes the current one.
- The first tomograms exist after about one TS's worth of latency per stage, not after the slowest TS of every stage.
- A slow or stuck TS stops holding the others.
- A wrong parameter shows on the first tomograms, not after the whole dataset has gone through.

### 7.1 Barrier inventory (each driver read 2026-10-04)

| Job | Shape | Barrier? |
|---|---|---|
| importmovies | written inline by the server, no SLURM job (`_write_import_stars_inline`, `pipeline_orchestrator_service.py:743`) | none |
| fsMotionAndCtf | array, per TS (`drivers/fs_motion_and_ctf.py`) | none |
| tsImport | single job: `WarpTools ts_import` + `create_settings` over all mdocs (`drivers/ts_import.py:1-9`) | soft: cheap metadata; per-batch feasibility is stage 0 item 4 |
| tiltFilter | DL prediction over every tilt; Manual / DL review parks alignment until Approve (`services/jobs/tilt_filter.py:401-433`; roadmaps 06, 26) | hard while a human reviews; DL auto could run per batch |
| aligntiltsWarp | array, per TS; the supervisor refreshes the tomostar snapshot | none |
| tsCtf | array, per TS; `ts_defocus_hand` over all TS in `pre_dispatch` | soft: removable (§7.3) |
| tsReconstruct | array, per TS | none |
| denoise_train | single global job over a filtered, capped set (`drivers/denoise_train.py:39`, `:66-100`) | soft: needs N tomograms, not all |
| denoise_predict | array, per TS; needs the trained model, staged once (`drivers/denoise_predict.py:235-250`) | none once trained |
| templatematching | array, per tomogram; `pre_dispatch` stages per-tomogram inputs (`drivers/template_match_pytom.py:271-301`) | none |
| extractCandidates | array, per tomogram; merges `candidates.star` | none (the merge is cheap) |
| subtomoExtraction | array, per TS; merges `particles.star` | none (the merge is cheap) |
| reconstructParticle, class3d | single global jobs (`drivers/reconstruct_particle.py:21`, `drivers/class3d.py:15`) | hard: whole-dataset by nature |
| missAlign | single global GPU job, joint refinement (`drivers/miss_align.py:1-20`) | hard |

From fsMotion to subtomo extraction, everything is per TS except:
- one human barrier: the review;
- one cheap metadata step: tsImport;
- two soft barriers: the hand and denoise training.

### 7.2 Mechanism

**(A) SLURM `aftercorr` between arrays that share one manifest.**

How it works:
- Every per-TS job's array is submitted at deploy time with `--dependency=aftercorr:<upstream array>`.
- Task i starts when upstream task i exits 0. SLURM does the gating; nobody polls.

What it costs:
- Every job in the chain must share one index space, fixed at deploy time.
- Supervisors stop deciding their item list at run time. Exclusions, skips and filter verdicts made after deploy
  could only act by cancelling tasks.
- A failed upstream task leaves its downstream task pending until someone cancels it. Today's
  `wait_for_array_completion` would wait on it.
- Global steps can no longer live in supervisors.

It is fragile around everything that makes our supervisors useful.

**(B) Streaming supervisors.**

How it works:
- A per-TS job's supervisor starts together with its upstream: `--dependency=after:<upstream supervisor>`, where
  `after` means "once it has started".
- It loops:
  1. Read the upstream's per-item states.
  2. Dispatch the newly ready items as a sparse array (`--array=<indices>`). That is the mechanism `submit_array_job`
     already uses for re-runs (`array_job_base.py:624-632`).
  3. Collect.
- It stops when the upstream is terminal and every item has settled, then aggregates.

What it gives:
- Everything stays in `ArrayDriver`, and the chain survives server restarts.
- A drop upstream is simply an item that never becomes ready.

What it costs:
- One CPU slot per running supervisor for the whole run (6–8 per project).
- Several arrays per job. That requires the per-item record from 2b, because `is_superseded_task` compares
  against a single manifest id today (`array_job_base.py:89-113`).
- Upstream readiness has to be read from per-item state, not from the STAR (§7.4).

**(C) Dispatch from the headnode.** The server's monitor submits tasks as items become ready.
- Rejected: the pipeline would stop moving whenever the server is down.
- That is the property the afterok DAG was built to remove: "the chain executes in SLURM regardless of the headnode
  process" (roadmap orchestrator-replacement).

**Recommendation: (B).**
- It extends machinery we already own instead of replacing it.
- It keeps run-time decisions (exclusions, skips, filter verdicts) in the supervisor, where they are today.
- A failure needs no special case.

(A) can come back later as an optimization inside (B), for chains with no global step.

### 7.3 The defocus hand as a project decision

- **Only `--check` needs many TS.** `ts_defocus_hand --set_flip/--set_noflip` only sets `AreAnglesInverted` on each
  series; Warp's set branch does no per-series computation.
- **The AGG412 projects already decide it twice:**
  - `acquisition.invert_defocus_hand: true` (`services/models_base.py:275`). It also feeds the inline import's
    `rlnTomoHand` (`pipeline_orchestrator_service.py:756`).
  - tsCtf `defocus_hand = set_flip`, the default (`services/jobs/ts_ctf.py:53`).
  - Stage 0 establishes whether these two fields are one decision before they are merged.
- **Proposal:**
  - The hand becomes a project value with a provenance: `explicit`, or `checked on N TS at <job>, correlation r`.
  - `auto` decides it once, as soon as K TS have frame-series CTF, records it, and never re-decides it (§6.4).
  - Each tsCtf task applies the recorded hand to its own staged XML before `ts_ctf`.
  - The global step leaves `pre_dispatch`, and tsCtf becomes per-TS like alignment.

### 7.4 Aggregation when TS arrive one by one

- **Inputs.**
  - Today each job writes its output STAR once, after every task has finished.
  - A streaming downstream job cannot wait for that. It needs per-TS readiness (the per-item state) and per-TS
    inputs (the registry's `ts.outputs[job_instance_id]`).
  - The prerequisite is registry-consolidation phase 4. It swaps the enumeration reads for registry queries:
    `read_tilt_series_names_from_input_star` in alignment, tsCtf and tsReconstruct, and `read_ts_frame_mapping` in
    fsMotion (`drivers/fs_motion_and_ctf.py:38`).
- **Ingest** moves from once-per-job `aggregate` to per batch.
- **The STAR** becomes an export: written atomically when the job settles, and optionally after each batch.
- **The particle stage stays STAR-fed** (the registry-consolidation hard stop: PyTOM and RELION read the stars).
  - TM and candidate extraction consume per-tomogram files.
  - The merged stars are written at the end, which is when the global particle jobs need them anyway.

### 7.5 Commits

- **3a.** The hand decision, if 2d did not already land it.
- **3b.** Registry-authoritative enumeration (registry-consolidation phase 4).
- **3c.** The streaming loop in `ArrayDriver`, behind a per-job flag.
- **3d.** Chain submission: `after:` instead of `afterok` for streaming jobs, and detection of "upstream terminal".
- **3e.** Start denoise_train when N tomograms are ready.
- **3f.** UI: per-TS progress across jobs.

---

## 8. Dependencies

- **task-status-state-machine §5:** stage 2b (or the append-only manifest), and stage 3c.
- **Roadmap 15:**
  - S1 (ownership lock) and S4 (submit-side pre-flight) before stage 2e.
  - S2 (atomic rebuilds) covers §4.4 item 9.
- **registry-consolidation phase 4:** stage 3b.
- **orchestrator-replacement:**
  - The afterok DAG stays the outer engine.
  - P1.C (the RELION export) must tolerate STARs that grow.
  - Its stateless principle rules out mechanism (C).
- **05:** absorbed as stage 2.
- **06 / 26:** the review barrier. Whether "approve what's been reviewed so far" ever exists decides how streaming
  crosses the tilt filter.
- **19 (CTF-fit outlier check):** a per-TS verdict at tsCtf. It should use the same dropped-with-reason channel.

## 9. Modern-Python weave-in

- **`class TaskState(StrEnum)`** with PENDING, RUNNING, OK, FAILED, SKIPPED. It replaces suffix strings like `".ok"`
  across `array_tasks` and `array_job_base`. Excluded is a registry fact, not a task state.
- **`TaskRecord`:** a frozen `dataclass`, or a pydantic model for `model_validate_json`. Fields: state,
  `array_job_id`, `task_idx`, `attempt`, `reason`, `log_path`. Written with tmp + `os.replace`, as the markers are
  today.
- **`match` over (current, proposed) state pairs** for the transition table in task-status-state-machine §4,
  replacing the conventions spread across five writers.
- **A small `typing.Protocol`** for a barrier policy (`ready(n_ok: int) -> bool`). The hand decision and denoise
  training each implement it, and the streaming loop asks it before dispatching the global step.

## 10. Decisions owed by the maintainer

1. **Minimum tilts N**, or a fraction of the acquired tilts, for the gate (§4.4 item 3).
2. **Drop-rate guard:** none, as today, or fail above X % dropped; and X (§4.4 item 7).
3. **Name length:** decided. The alias fix and the landing-page guard landed (§4.3). Still open for FCIsolation:
   delete and re-add alignment now, or wait for stage 2.
4. **Re-dispatch missing tasks once, automatically** (§4.4 item 8).
5. **`auto` hand fails loudly when there is no verdict** (§4.4 item 2). Recommended; needs confirmation.
6. **Stage-2 index strategy:** per-item record or append-only manifest (§6.1).
7. **Whether a catch-up offers to re-run global downstream jobs** (§6.2 item 6).
8. **Streaming mechanism:** (B) recommended (§7.2).

## 11. Runtime checklist

**Stage 1, owed now.** Open each project in the UI and press Run.

1. **SK.**
   - tsCtf reuses `External/job005`, and tsReconstruct reuses `External/job006`.
   - `job005/run.out` shows `Staged 133/133 in-scope tomostars from …/External/job004/tomostar`.
   - Then `ts_defocus_hand` completes, an array of 133 runs, and the log ends with `Job finished successfully`.
2. **MyrGFP.**
   - First exclude `Position_10_4` in the Journey, then Run.
   - Expect `Staged 48/48 in-scope tomostars`, an array of 48 and one `.skip`.
   - Without the exclusion: `Staged 49/49`, plus `[WARN] ts_defocus_hand --check failed; the explicit set_flip is
     applied anyway` if the check still trips on it.
3. **FCIsolation.** `Staged 58/58`; tsReconstruct runs 58.
4. **Any job with a per-TS failure.**
   - `run.out` shows `[SUPERVISOR] DROPPED n/N tilt-series … By reason:` with grouped lines.
   - `.task_status/<ts>.fail` holds the reason.
   - The job ends Succeeded with `n!` on its row.
   - The next job's input STAR lacks the dropped TS.
5. **A job where every dispatched TS fails** still ends Failed: `Marking job as FAILED (0 ok, …)`.
6. **The name fix (§4.3), on unmilled2.**
   - Run retries alignment in place.
   - Series of 45+ characters align, with no `Could not find <ts>.tlt`.
   - `warp_tiltseries/<ts>.xml` and `tiltstack/<ts>/` carry the real names.
   - tsCtf and tsReconstruct consume them.

**Stage 2 is done when:**
- In a project that ran through tsReconstruct, "Retry dropped" on a TS dropped at tsCtf dispatches exactly one tsCtf
  task and one tsReconstruct task.
- The TS appears in tsReconstruct's output STAR.
- A restored TS goes the same way from the first job that skipped it.

**Stage 3 is done when:**
- On a fresh project, the first tomogram is written while alignment is still running on other TS.
- A TS that fails alignment never blocks the TS behind it.

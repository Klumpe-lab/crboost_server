# Roadmap 32 — Staging while running, inputs fixed at submit, subsets and forks

**Status:** scoped 2026-10-14 from the branching survey
(`docs/reports/pipeline-graph/branching-survey-2026-10-12.md`). Stage 0 answered; stage 1, the §8 table and
stage 2 (without S2.3) built 2026-10-15, all NOT RUN (records §3.1, §8, §4.1; runtime checks §11). Next: stage 3
once §9.4 is answered. §9.4–9.7 open.

**In one line.** Jobs can be added and started while others run; a submitted job's inputs never move; and a
"fork" is one defined thing, a subset of some job's output, used either inside the project or as the start of a
new one, so in-project branches and "import tomograms from another project" are the same mechanism.

**Not in this roadmap:** an interactive graph builder (dropped: drag-to-wire is a lot of machinery for little
gain). A read-only pipeline table comes right after stage 1 (§8). Per-TS streaming stays in roadmap 27 stage 3;
§7 is the assessment asked for.

## Before → After

**Before:**
- While any job of a project is queued or running, nothing can be added, removed or started (§2.1).
- A queued job re-resolves its inputs when SLURM starts it, so adding a job in the meantime can re-route it or
  kill it on a new tie.
- Adding missAlign silently moves every un-pinned consumer of the alignment onto it.
- Which tilt series are in play is one project-wide answer; two chains cannot differ on it.
- "Import tomograms" is the only way to start from another project's results, and only at the tomogram level.

**After:**
- Add, clone, edit and start anything that is not live; Run starts what is unfinished, around what is running.
- What a job read is recorded when it is submitted, shown in its I/O tab, and is what the driver uses.
- A **Subset** job cuts any per-TS output (aligned tilt series, CTF'd tilt series, tomograms, …) down to a
  chosen set of tilt series; chains branch from it.
- **Fork…** on a job output either adds a Subset in this project or creates a new project that starts from that
  output (referenced, not copied), with its own exclusions and tilt verdicts. Import tomograms becomes one case
  of it.

---

## 1. How inputs are chosen today (the I/O slot system in one page)

Each job type declares its inputs and outputs by file type (`services/io_slots.py`; e.g. tsReconstruct reads a
`TS_CTF_TILT_SERIES_STAR` and a `WARP_TILTSERIES_DIR` from tsCtf and a `WARP_TILTSERIES_SETTINGS` from alignment,
`services/jobs/ts_reconstruct.py:27-41`). For one input of one job, `PathResolutionService._select_source`
(`services/path_resolution_service.py:707`) answers "which file":

1. **Set by hand** (`source_overrides[slot]`, keyed `jobtype:instance:<id>`) wins if it still resolves; if not,
   the input is reported broken, never replaced.
2. Otherwise **automatic** (`_auto_select`, `:1185`). Candidates are every job output of an accepted type, plus
   external inputs (imported tomograms, merges, pick lists, particle sets). Dropped: the job itself, producers
   of a later or equal job type (`PIPELINE_ORDER`, `:233-250`), producers of another species. Ranked: own species
   → the slot's preferred job type → a job over an external input → on the roster. One producer at the top:
   chosen. Two or more: a **tie**, nothing chosen, the I/O tab asks.
3. **Refinement following** (`_follow_refinements`, `:1240`): if the chosen output is refined by a job on the
   roster (missAlign `refines` the alignment's star and warp dir), the input moves to the refiner. Several
   refiners: the most recently deployed wins.
4. **Pin on ambiguity** (`pin_sources_made_ambiguous`, `:1389`): when a job is added and an existing job's input
   becomes a tie, that input is set by hand to what it read before.

The same answer is computed three times: when the I/O tab renders, at Run (`_submit_chain` pass 2,
`services/scheduling_and_orchestration/pipeline_orchestrator_service.py:560-581`, which also derives the afterok
edges from it), and **again when the driver starts** (`drivers/driver_base.py:117-139`). The third one is a
leftover from the schemer, which could allocate a different job number than predicted; under afterok the job
dir is fixed at submit, so the reason is gone.

What is good and stays: typed slots, automatic selection for the plain chain, ties instead of guesses, hand
settings that never silently move. What goes wrong is *when* the answer is taken (§3) and two rules that move
inputs behind the user's back (refinement following of already-submitted jobs, the aggregation merge
overwriting every consumer, `services/aggregation/extraction.py:255-330`).

---

## 2. Facts this rests on

### 2.1 The run lock is project-wide, and only the front needs it

- Run refuses while `pipeline_active` (`pipeline_orchestrator_service.py:175-176`).
- Add, remove and species-add return early on `ui_mgr.is_running` (`ui/pipeline_builder/pipeline_builder_panel.py:114`,
  `:171`, `:293`, `:465`); the roster hides `+`/`×` (`ui/pipeline_builder/pipeline_roster.py:597`, `:604`, `:718`,
  `:732`); the Species page says "Pipeline running — adding jobs is paused." (`ui/species/jobs_tab.py:135`).
- The back end does not need it: `reconcile_afterok` tracks every job separately and `pipeline_active` is only
  "some job is live" (`pipeline_runner.py:406`, `:528-537`); `_submit_chain` can already gate new jobs on live
  upstream ones by SLURM id (`live_after`, `pipeline_orchestrator_service.py:593-598`, used by Approve).
- What would break if the lock simply went:
  - Run would resubmit queued/running jobs (`instances_to_run` = not SUCCEEDED, `:188-195`; a non-FAILED job gets
    a fresh dir in pass 1).
  - Deploy rewrites `pipeline_order` with the run selection (`:183`) and clears the review hold (`:185`).
  - Queued jobs re-resolve at start (§1) and stay editable (`is_job_frozen` covers RUNNING/SUCCEEDED only,
    `ui/pipeline_builder/config_tab.py:11-16`; drivers reload `project_params.json` at start).
  - Two supervisors aggregating at once write whole registry sidecars, last writer wins
    (`services/tilt_series/registry.py:353-384`; the lock at `:74` is per process).

### 2.2 Selection is project state

- TS exclusion: `TiltSeries.is_excluded` (`services/tilt_series/models.py:323`), applied by every array supervisor
  at start (`apply_exclusions`, `drivers/array_job_base.py:328`) as `.skip` markers.
- Tilt verdict: `Frame.is_filtered_out`, from the one tilt filter per project, which has no output
  (`OUTPUT_SCHEMA = []`, `services/jobs/tilt_filter.py:119`); alignment applies it at start
  (`drivers/ts_alignment.py:220`). The registry stamp is deliberate: it keeps wiring independent of when the filter
  is committed (`:211-216`).
- Per-TS drivers take their item list from the rows of their input star (`read_tilt_series_names_from_input_star`,
  `drivers/array_job_base.py:245`), so a star with fewer rows is already a subset everywhere downstream.
- Warp jobs read upstream dirs through per-TS symlinks and write their own `output_processing`
  (`stage_per_ts_environment`, `array_job_base.py:454-512`): upstream outputs are read-only to consumers, so they
  can be referenced from a subset, or from another project.

### 2.3 Starting from another project's results exists in pieces

| Feature | What it references | Record |
|---|---|---|
| Import tomograms, reference mode | another `tomograms.star` | `ImportedTomograms` (`services/project_state.py:628`), one per project, external input `importedTomograms` |
| Import tomograms, synthesize | loose `.mrc`/`.rec` files | same |
| Aggregation sources | other projects' optimisation sets, per species, optional tomo list | `AggregationSource` (`project_state.py:459`) → a merge, external input `mergedSources` |
| Particle sets | job outputs / sets / merges, with parents and mtimes | `services/particles/particle_sets.py:66-91` |
| Data-less project | none; created from the landing page | `is_dataless()`, `ui/data_import_panel.py:301` |

Nothing references a tilt-series-level output (aligned or CTF'd tilt series) of another project.

---

## 3. Stage 1 — Inputs fixed at submit

Do first: stage 2 makes edits while jobs are queued normal, and today those edits reach queued jobs.

**S1.1 Record what was resolved.** `_submit_chain` pass 2 already resolves every job of the run from one snapshot.
Store the answer on the job as `submitted_sources: dict[slot, source_key]` (next to `paths`). `_select_source`
reads it for a job that has one, before the hand setting, so the I/O tab, the edges and any re-render all agree
with what was submitted.

**S1.2 The driver uses the submitted paths.** Under afterok, `get_driver_context` takes `job_model.paths`
(written at `pipeline_orchestrator_service.py:628`) instead of re-resolving (`drivers/driver_base.py:117-139`).
The schemer path keeps re-resolving until it is decommissioned.

**S1.3 Queued jobs are frozen** like running ones (`is_job_frozen`).

**S1.4 Re-running.** A FAILED job retried in place keeps its recorded inputs. A **Reset inputs** action in the I/O
tab clears them (the job then re-resolves at its next Run). A job reset to fresh (new dir) starts unrecorded.

**S1.5 The I/O tab says which of three it is:** *automatic* (not submitted yet: may still change), *set by hand*,
*fixed at submit*. For an automatic input that a refiner currently redirects, say so ("follows missAlign").

**S1.6 Two rules that move inputs stop touching submitted jobs:**
- refinement following applies only to jobs without a record (falls out of S1.1);
- `apply_aggregation_overrides` skips jobs with a record and consumers of another species
  (`services/aggregation/extraction.py:255-330`).

**S1.7 Lineage check (warning only).** A job whose inputs come from two chains (tsReconstruct's star from
tsCtf__2 but settings from an alignment that is not upstream of tsCtf__2) gets an amber note in its I/O tab. A
Subset job (stage 3) counts as transparent.

**S1.8 Fold-in:** missAlign's walltime estimate reads its own input alignment, not the first alignment on the
roster (`services/jobs/miss_align.py:527-539`).

Size: ~200 lines (resolver, orchestrator, driver base, I/O tab). Risk: medium — every job's start path changes.

### 3.1 Stage 1 record — built 2026-10-15, NOT RUN

Decided with the maintainer before building (2026-10-15): §9.1 accepted, with the rules below.

- **What a job read stays what it reads.** In the build the problem showed up in four places, each fixed by
  the record. (1) A FAILED job retried in place kept its finished TS but chose its inputs again: add a
  missAlign, Run, and its failed TS ran on missAlign while the rest stayed on the old alignment, in one dir.
  (2) Editing a queued job's I/O reached the driver while its SLURM dependency stayed on the old producer.
  (3) Finished jobs' I/O tabs chose again and could show a source they never read. (4) A finished or switched
  aggregation merge rewired every optimisation-set consumer, finished or queued.
- **The record** (S1.1): `AbstractJobParams.submitted_inputs: dict[slot, SubmittedInput] | None`
  (`services/jobs/_base.py`), `SubmittedInput(source_key, output_key)`. `output_key` pins which output of the
  producer: without it a curated `optimisation_set_filtered.star` saved later would replace the raw file a
  job read. Written in `_submit_chain` pass 3 from the pass-2 resolver (`PathResolutionService.record_inputs`).
  `inputs_fixed` = a record and status not Scheduled: a job put back to Scheduled chooses afresh.
  `_select_source` reads the record first (`_keyed_source`, shared with overrides); a recorded source that no
  longer resolves is reported ("fixed at submit, but …"), never replaced. Recorded edges count as `explicit`.
- **The driver** (S1.2): `get_driver_context` takes `job_model.paths` when `inputs_fixed` and the recorded
  `job_dir` is the cwd; any other launch (schemer, tilt-filter predict in its own run dir) resolves as before
  and still honours the record. The schemer deploy clears records of what it runs.
- **One job dir holds one set of inputs** (S1.4, amended). A FAILED job is retried in place only when the inputs
  resolved now equal its last attempt's (`_inputs_moved`, input slots only). Otherwise it gets a new dir and
  redoes every TS; that moves its consumers' inputs, so passes 1–2 repeat until no reused dir sees new inputs.
- **Reset inputs** (S1.4): a button in the I/O tab of a FAILED job with a record. Finished jobs are not reset;
  "same job, other input" is Clone (stage 2).
- **Queued is read-only** (S1.3). Parameter edits on queued jobs were already dropped by the model guard
  (`services/jobs/_base.py`, `__setattr__` accepts USER_PARAMS only on Scheduled/Failed) while the tab showed
  editable fields; now `is_job_frozen` says so. Lost: editing a queued job's SLURM settings and I/O.
- **Not frozen: which TS.** Exclusions and tilt verdicts still reach queued jobs at their start.
- **No backfill.** Jobs submitted before this change have no record and keep choosing their inputs as before.
- **I/O tab** (S1.5): a quiet state word per input (`InputState`: automatic / set by hand / fixed at submit,
  or "follows <refiner>" from `AutoChoice.refined_by`); a fixed job's inputs render read-only.
- **S1.6:** `apply_aggregation_overrides` skips jobs whose inputs are fixed. The "consumers of another species"
  half was dropped: a merge carries no species id of this project. Fold-in bug: the aggregate dialog's "Set up
  extraction" (`_prepopulate_subtomo`, `ui/aggregation/aggregate_dialog.py`) wrote only `paths`, which every Run
  overwrites, so the wiring never reached the job; it now sets `manual:<optset>` as a hand-set input, on the
  first Subtomo extraction job whose inputs are not fixed.
- **S1.7:** `PathResolutionService.mixed_chains` — two direct producers neither upstream of the other → an amber
  line under Inputs. Synthetic producers are left out.
- **S1.8:** `planned_stack_apix` reads the alignment this missAlign reads (`source_of`), not the roster's first.
- Checked: `ruff check` / `ruff format` on the touched files. Not run: Python is absent in the sandbox, so no
  import check; runtime checks §11.

---

## 4. Stage 2 — Staging while running

**S2.1 Per-job liveness instead of a project lock.** A job is *live* when QUEUED or RUNNING. Everything that is
not live, and not upstream of a live job, can be added, cloned, edited, removed. Live jobs and their upstream are
read-only (their outputs or inputs are in use). The `ui_mgr.is_running` gates of §2.1 become this test; the run
slot shows Run and Stop side by side while something is live.

**S2.2 Run around what is running.**
- `instances_to_run` drops QUEUED/RUNNING jobs; their consumers wait on them through `live_after`.
- Deploy stops rewriting `pipeline_order` (add/remove already keep it, `pipeline_builder_panel.py:341`, `:486`).
- The review hold is cleared only by a run that contains the held alignment.
- The submit lock stays (`_submit_lock`, `pipeline_orchestrator_service.py:155`): two clicks never submit twice.

**S2.3 Run to here.** DEFERRED (§9.3): not in stage 2; one Run button. On a job row (and the job tab): the job and its unfinished upstream, nothing else. Run keeps
meaning "everything unfinished on the roster". Stop stays "cancel everything"; cancel on a job stops it and,
through afterok, what waits on it.

**S2.4 Clone.** On a job row: same type, USER_PARAMS + SLURM settings + species copied through the typed setters
(`numeric_forward` / `enum_forward`), same hand-set inputs. The A/B primitive; today every variant is retyped.

**S2.5 Registry writes from concurrent jobs.** A lock file (O_EXCL, as roadmap 15 S1) around a supervisor's
load → ingest → save, and the save re-reads a sidecar that changed on disk since load before writing it. Server
writes (exclusion toggles, verdicts) take the same lock.

**S2.6 Fold-in: the delete preview** uses `resolve_slot_edges` instead of `default_pipeline.star`
(`pipeline_deletion_service.py:323-370`), so "N jobs read this one" is true under afterok — needed once deleting
while others run is normal.

Size: ~250 lines. Risk: highest of the roadmap (the run path). Runtime check in §11.

### 4.1 Stage 2 record — built 2026-10-15, NOT RUN

Built under §9.2 (live jobs and their upstream locked) and §9.3 (one Run button, no S2.3).

- **S2.1** `jobs_in_use(state)` (`services/path_resolution_service.py`): the queued and running jobs and every job
  upstream of one (`resolve_slot_edges`), each mapped to a live job that holds it. Add, species-add and clone are
  open while jobs run: the `ui_mgr.is_running` gates are gone from the panel, the roster and the Species page's
  Jobs tab. Remove and delete refuse a job in use and say why (`in_use_notice`,
  `ui/pipeline_builder/config_tab.py`): the roster hides its ×, and the job tab's Delete and
  `remove_instance_from_pipeline` (the backstop) refuse it. Editing stays status-based (`is_job_frozen`): a job
  upstream of a live one is Succeeded or live itself, except a Failed producer in the seconds before the
  reconciler cancels its queued consumer. The run slot shows Run above Stop while anything is live.
- **S2.2** `_deploy_locked`: the `pipeline_active` refusal stays for the schemer path only (one scheme at a
  time). `instances_to_run` skips Queued and Running jobs; a job of the run that reads one waits on its SLURM id
  through `live_after`; when only live jobs are unfinished, Run answers "Nothing to start". `pipeline_order` is no
  longer rewritten: a run only appends jobs it was given and lacks (the tab's roster comes from `state.jobs` at
  load, so a finished job taken off the roster reappears there after a reload; pre-existing). The review hold is
  cleared only by a run that includes a parked job, which then decides afresh as before. Submit lock unchanged.
- **S2.3** not built (§9.3).
- **S2.4 Clone**: a copy icon on every roster row except the tilt filter and Import →
  `add_instance_to_pipeline(type, species_id=, clone_of=)` → `AbstractJobParams.copy_settings_from`: the USER_PARAMS
  (deep copies of the source's values, same model type, so no typed setters are needed), `slurm_overrides`,
  `source_overrides`. Not copied: the inputs recorded at the source's submit; the clone's automatic inputs are
  chosen afresh, so it may follow a refiner its source did not. The A/B pin (`pin_sources_made_ambiguous`) runs as
  for any add.
- **S2.5 Registry** (`services/tilt_series/registry.py`): `registry_lock`, the file `registry/.lock` created with
  O_EXCL and naming its holder (host, pid, SLURM job, time); taken over when its process is gone on this host or it
  is older than 10 min; a waiter gives up after 15 min, naming the holder. Every mutation is also kept as a change
  to replay. `save()` takes the lock, re-reads each sidecar it writes, replays this copy's changes onto it and writes
  it: re-read always, not when the mtime moved, because stat() on the shared mount lags up to a minute while open()
  does not. The index lists the union with what another process added (except `force`, the mdoc rebuild); when
  another process wrote meanwhile, the stale mark is kept so the server re-reads. This covers every writer without a
  call-site change: the supervisors' ingests, exclusion toggles, verdict commits, DL predictions.
- **S2.6** `preview_deletion(state, instance_id)` lists `consumers_of` (the resolver's wiring); `delete_job` reports
  the same list as orphaned when it deletes one instance.
- After a read-through review, same day:
  - A DL-auto tilt filter that is already queued or running now holds back the alignments of a later run too
    (`_submit_chain` builds the verdict edges over the run plus such filters, so they land in `live_after`).
    Before, a cloned alignment started without waiting for the verdict.
  - Registry: takeovers of a dead lock go one at a time through a second O_EXCL file (`_take_over`); without it,
    two waiters crossing the stale age together could both remove the dead lock and both hold the new one.
    `save()` works on a snapshot of the dirty set and pops each tilt series' changes before re-reading it, so a
    change made while a save runs stays for the next save. The verdict commit saves through `save_async`, under
    the same asyncio lock as the exclusion toggles.
  - `project_service.delete_job` and `species_admin.delete_species` refuse jobs in use themselves; the dialogs'
    checks alone missed a Run landing while a dialog was open.
  - Schemer path: while a scheme runs every job counts as in use (`jobs_in_use`), since the jobs it has yet to
    start are Scheduled.
  - Table: the rows are worked out on the event loop (where every edit of the jobs runs); only the disk reads go
    to a thread, on a copy of the rows' jobs. A row whose job was removed since the read is skipped.
  - Cancelling one job (job tab) no longer stops the tab's polling while other jobs run; the dialog says what
    happens under afterok (the job and what waits on it fail, the rest runs on).
  - Left: the roster's render works out `jobs_in_use` (a resolver pass, a few file checks) on the event loop on
    every rebuild while anything is live.
- Found, not fixed: the Tomograms wall and the Protocols view create their poll timers inside their own container
  and clear it on the first build, which deletes the timer (NiceGUI timers are elements), so their 15 s polls never
  run. The table uses `owned_timer`.
- Checked: ruff on the touched files. Not run (no Python in the sandbox).

---

## 5. What a subset is, and what a fork is

**A subset** is three things:
1. a **cut point**: one output of one job (the alignment's aligned tilt series, tsCtf's tilt series, a
   reconstruction's tomograms, a denoise run's tomograms, a candidate set, …);
2. a **TS selection**: which tilt series of that output, as a list (picked in a table with the Journey's per-TS
   readouts) or a saved rule over registry fields;
3. optionally a **tilt selection** — only in a new project (below), until the tilt verdict is an output.

Particle-level subsets already exist as particle sets (roadmap 30); they are the same idea one level down and
are not redone here.

**A fork** is a subset that new jobs read. Where those jobs live is the only real difference between "branch in
this project" and "new project, import from the old one":

| | Branch in this project | New project from the old one |
|---|---|---|
| Data read | the cut point's files, by path | the same files, by path |
| TS exclusion, tilt verdict | shared with every chain | its own; starts as a copy |
| Registry (per-TS metadata, Journey history) | shared | its own; seeded with the selected TS |
| Species, templates | shared | copied if the cut is in the particle half |
| Microscope / acquisition | shared | copied |
| Comparison with the original | same project: reconstruction compare, Journey with a chain chooser | across projects: aggregation-style references |
| Disk | only new outputs | only new outputs (references, no copies) |
| Lifetime | dies with the project | independent; can be shared, moved, deleted on its own |

So the data side is one mechanism and the destination is a choice. Recommended rule of thumb, shown in the Fork
dialog: **in this project** for parameter variations downstream of the cut and for TS subsets; **new project**
when the tilt verdict or other project-wide state must differ, or when the fork should outlive or leave the
original.

**Unification.** "Import tomograms (reference mode)" is a new-project fork cut at a reconstruction's tomograms.
"Import tomograms (synthesize)" is the same external input with files that came from no project. Aggregation
sources are forks of several projects' optimisation sets, merged. They become one record — an **imported output**:
`{file type, paths, source project, source instance, source output, TS selection, created_at}` — with three
producers of it (fork, file import, merge) and one external-input kind in the resolver. `ImportedTomograms`
migrates into it on load.

---

## 6. Stages 3–4 — Subset job and Fork

### Stage 3 — the Subset job (in project)

- New job type **Subset**: runs inline at submit like import (`RUNS_INLINE`, `_submit_chain` pass 3,
  `pipeline_orchestrator_service.py:645`); no SLURM job, no afterok edge of its own (its consumers depend on its
  producer).
- Input: one per-TS star of any level (`FS_MOTION_CTF_STAR`, `ALIGNED_TILT_SERIES_STAR`, `TS_CTF_TILT_SERIES_STAR`,
  `TOMOGRAMS_STAR`, `DENOISED_TOMOGRAMS_STAR`). Output: the same file type, rows restricted to the selection.
  Companion directories (warp dirs, settings, tomostars) keep coming from the original producer; per-TS drivers
  only process rows of their input star (§2.2).
- The output type follows the input: the resolver reads the output schema from the job instance rather than the
  class (`_get_output_schema`, `path_resolution_service.py:1337`) — a small change, needed only for this job.
- Selection UI: a table of the cut point's tilt series with the per-TS readouts the Journey already has, plus
  "save as rule". The Subset job's star is rewritten at each submit, so an edited selection applies to the next Run
  of its consumers (consumers that already ran keep what they read — stage 1).
- Consumers are added with **Add after this job** (pinned to the Subset); existing consumers of the cut point are
  untouched.
- Size: ~250 lines.

### Stage 4 — Fork… (either destination) and the imported output

- **Fork…** on any job's output (job tab, roster row): pick the output, the TS selection, the destination.
  - *This project*: adds a Subset (stage 3) on that output.
  - *New project*: creates a data-less project next to the original; copies microscope/acquisition, the selected
    TS registry sidecars (identity, frames, mdoc metadata, the source jobs' outputs as read-only history),
    exclusions and tilt verdicts as starting values, species if the cut is in the particle half; records an
    imported output pointing at the cut point's files; opens it.
- Per cut-point feasibility, to confirm in stage 0:

  | Cut point | References needed | Open question |
  |---|---|---|
  | Tomograms (reconstruct / denoise) | `tomograms.star` + volumes | none — today's reference import |
  | Particles (candidates, optimisation set) | star + volumes/stacks | the aggregation source, single project |
  | CTF'd tilt series | tsCtf star, `warp_tiltseries/`, alignment settings + tomostars | Warp reads settings paths relative to the settings file |
  | Aligned tilt series | alignment star, `warp_tiltseries/`, settings, tomostars | same; plus a tilt verdict in the new project means re-running alignment there |
  | Motion-corrected frames | fsMotion star, `warp_frameseries/` | rarely useful; a new project on the raw data is the same |

- Container binds: `project_binds` binds the project, its raw-data roots and the import source dir
  (`drivers/driver_base.py:28-40`); an imported output from another project adds its source project root.
- Import tomograms keeps its dialog, now writing an imported output. Aggregation keeps its merge card; its sources
  become imported outputs of the merge.
- Size: ~400 lines. The registry seeding is the delicate part.

---

## 7. Per-TS pacing (streaming) — assessment

Not a stupid feature, and it is already scoped: roadmap 27 stage 3, mechanism (B), streaming supervisors (§7.2
there), with a barrier inventory (§7.1).

What it buys, honestly:
- **Time to the first tomograms** drops from "the slowest TS of every stage, plus a queue wait per stage" to
  about one TS through the chain. This is the real gain: wrong parameters show in minutes, not after a day.
- **Total time** drops less than it feels. Arrays already run in throttle-sized waves; the GPUs are the limit.
  The saving is the tail of each stage overlapping the head of the next and one queue wait per stage — a modest
  fraction of a long run.
- **A stuck TS stops holding the others** — but roadmap 27 stage 1 (drop with a reason) and stage 2 (catch-up)
  already give most of this.

What it costs: registry-authoritative enumeration (registry-consolidation phase 4), per-item task records
(task-status-state-machine §5), one supervisor slot per streaming job for the whole run, and four barriers to
handle (tsImport, the defocus hand, denoise training, the tilt-filter review — the last one is human and blocks
streaming through alignment whenever a review is on).

Recommendation: keep it after roadmap 27 stage 2. The cheap 80 %: the Subset job of stage 3 gives "first
tomograms early" without new machinery — fork a handful of TS, run the chain on them, then Run the full set with
the same parameters. That is an option the user can take, not a step the pipeline imposes.

---

## 8. Next after stage 1: the read-only pipeline table

Moved up 2026-10-15 (was "after this roadmap"); it needs only stage 1's recorded edges. Replaces the survey's
stages × lanes card grid (§5 stage 1 there) with a git-log style table, after a design the maintainer liked (a public
post, 2026-10-13: in a pipeline the nodes form a list, so each can be a table row with metadata
beside it; parallel branches cost lanes, and horizontal space is cheap).

- **Shape.** One row per job, in dependency order. A narrow gutter on the left draws one lane per open chain: a
  dot on the job's row, lines down to its consumers, a lane looping out and back for a branch that rejoins. Our
  graph is mostly a list (preprocessing is one chain; branches are per-species particle chains, A/B copies such
  as tsCtf__2, and refiners such as missAlign), so 1–4 lanes is the usual width.
- **Edges.** Only each job's main lineage: the input its per-TS rows come from (the star). Side inputs (the
  alignment settings tsReconstruct reads) go in the row's hover and the I/O tab; drawn, they turn the gutter into
  spaghetti. Solid when fixed at submit or set by hand, dashed when automatic (they can still move). Subset jobs
  and imported outputs (stages 3–4) are ordinary rows.
- **Columns to start:** job + species pill, state, TS ok/total (the roster's `_get_array_progress`), elapsed.
  Later: a timeline column (queue wait vs run per job), which shows where a long run's day went; it needs
  submit/start/end times that no job records yet.
- **Where.** A full-width workspace view. The roster sidebar stays the job catalog (grouped by type, with the
  add rows); it is too narrow for lanes plus columns.
- **Build.** Order + lane assignment in Python (topological order, lanes allocated as `git log --graph` does),
  one inline SVG per row for the gutter, ordinary NiceGUI rows for the rest, a `FingerprintedView` keyed on
  statuses + edges. Click opens the job. No editing; the verbs live on the job (Run to here, Clone, Add after,
  Fork…).

**§8 record — 2026-10-15, layout only, NOT RUN.** `services/pipeline_table.py`: `pipeline_rows(state)` → one
`TableRow` per roster job (dependency order over all inputs, roster order between independent jobs; main lineage
by `MAIN_SLOTS`), and the pure `assign_lanes(order, parent, explicit)`: a job's first consumer continues straight
down its lane, each further consumer gets a lane forked off at the job's row (`forks`), a lane frees once its
consumer has its dot. Per row: `lane`, `arrives`, `through`, `forks`, `continues`, `explicit`. ruff clean; no
Python in the sandbox, so not executed. Owed: the view (`ui/pipeline_table_view.py`: workspace `View` +
route, per-row inline SVG gutter drawn from `TableRow`, columns, `FingerprintedView` signature = statuses +
rows), a rail entry, and a check of `assign_lanes` on a real roster (e.g. print `pipeline_rows` for a copia
project with two TM species and a missAlign).

**§8 view — built 2026-10-15, NOT RUN.** `ui/pipeline_table_view.py`: `View.TABLE` (`/p/<project>/table`), rail
icon under Jobs (`static/icons/pipeline_table.svg`), lazily built like the Tomograms wall. One inline SVG per row
(`gutter_svg`): lanes passing through, the line arriving at the dot, the line continuing down, forks as a curve
into their lane, the dot in the status colour (running pulses, missing inputs orange, parked hollow amber). Dashed
= automatic; the dash period divides the row height, so a lane runs on unbroken across rows. Columns: job +
species pill (hover: every job-fed input with its source and how it was set), state (hover: SLURM id, and what a
queued job waits for), TS ok/total (`_get_array_progress`), elapsed. Click opens the job.
- `assign_lanes` changed: a lane holds the consumer it leads to, so a line keeps one style from producer to
  consumer (`TableRow.solid`); forks take the first free lane right of the dot, so a branch never crosses the
  lines to its left; a consumer ahead of its producer (a loop) no longer leaves a dangling lane.
  `TableRow.inputs`: all job-fed inputs. Traced with a JS port on a 17-job roster (two species, missAlign).
- Elapsed (`run_times`): from the later of the job's submit (`run_submit.script` mtime) and the end of the last
  input job that ended before it, to its exit marker; it includes queue wait. No job records times yet; the
  timeline column still needs them.
- Polling: statuses in memory every 3 s; rows, TS tallies and markers re-read off the loop when those move, else
  every 15 s; the elapsed labels are set in place, the rest is a `FingerprintedView`.

Also later: read-side views follow a chosen chain instead of "the first job of each type" (survey §1.5,
`find_job_by_type`, `services/dashboard_data.py:59-67`, ~20 readers incl. the Journey).

---

## 9. Decisions owed by the maintainer

1. **Fixed inputs at submit (S1.1–S1.4).** DECIDED 2026-10-15: yes, with the amendments in §3.1 (one job dir =
   one set of inputs; Reset inputs on Failed jobs only; queued read-only; TS selection stays live; no backfill).
2. **What stays editable next to a live job (S2.1).** DECIDED 2026-10-15: live jobs and everything upstream of them
   are locked, and upstream jobs cannot be removed or deleted while a downstream job is queued or running;
   everything else (add, edit Scheduled/Failed, remove, Run) is open while a chain runs.
3. **Run verbs (S2.3).** DECIDED 2026-10-15: no Run to here / Run from here for now; one Run button (everything
   unfinished, around what is live). Revisit once the pipeline table (§8) shows the branches, where a per-row
   verb reads more obviously.
4. **Subset selection:** list only, or list + saved rules (recommended: list first, rules once the per-TS readouts
   it would filter on are settled).
5. **Fork destination rule of thumb (§5):** accept as the dialog's default hint?
6. **Unify Import tomograms and aggregation sources into imported outputs (§5)** — recommended; it is a migration
   of two records.
7. **Streaming (§7):** keep it in roadmap 27 after stage 2.

---

## 10. Stage 0 — facts (answered 2026-10-15, by reading)

1. **Warp settings paths are relative.** We write `DataFolder="tomostar"`, `ProcessingFolder="warp_tiltseries"`
   (`drivers/ts_import.py:51-54`, `drivers/fs_motion_and_ctf.py:105-108`), resolved against the settings file's
   own dir. Every Warp consumer stages its own copy (`stage_per_ts_environment`, `drivers/array_job_base.py:454`:
   settings copied, tomostar copied with absolute MoviePaths, the TS's XML symlinked), and tsCtf copies upstream
   XMLs into its own `output_processing` (`drivers/ts_ctf.py:223-229`). So a CTF'd/aligned cut point can be read
   from another project unchanged, given its root is bound in the container (§6 stage 4, `project_binds`).
2. **Registry paths are absolute and keyed by instance id.** Every `outputs` entry carries `job_instance_id`,
   `job_dir` and absolute file paths (`services/tilt_series/models.py:52-205`; `adapters/*` pass the supervisor's
   job dir). A seeded copy can keep them as read-only history without rewriting, but must re-key them: the new
   project's own `tsReconstruct` would otherwise overwrite the source project's entry of the same id.
3. **Readers of `paths` after `get_driver_context`:** drivers read `ctx.paths[...]`; in-process readers of
   `job_model.paths` are the job.star writers (`_get_job_specific_options`, written at submit after pass 3 sets
   `paths`), `DenoisePredictParams.inherited_from_train` (matches the train job by the model path, so it now
   matches the recorded one) and `local_warps_upstream` (`services/jobs/miss_align.py`, walks recorded paths). No
   driver looks another job up by type at run time. Writers outside the orchestrator: `backend.py:355` (tilt-filter
   predict, its own run dir), `backend.py:599` (per-list extraction, `job_dir` only), `apply_aggregation_overrides`
   and the aggregate dialog (both fixed in S1.6).
4. **No driver mutates its upstream's files found.** Upstream XMLs are symlinked read-only into staging or copied
   (tsCtf); outputs go to the job's own `output_processing`; `.skip` markers and exclusions live in the job's
   own dir and the registry.
5. **`ui_mgr.is_running` gates.** Editing: `pipeline_builder_panel.py:114`, `:171`, `:293`, `:465` (add, species
   add, add instance, remove); `pipeline_roster.py:597`, `:604`, `:718`, `:732` (+/× and add rows);
   `ui/species/jobs_tab.py:95/135`; `ui/data_import_panel.py:446`, `:652`, `:912` (landing data inputs, project
   creation — unaffected by stage 2). Chrome: `pipeline_builder_panel.py:97`, `:592` (poller start),
   `pipeline_roster.py:317` (signature), `:1601` (Run/Stop slot), `status_poller.py:128-131` (flag sync),
   `logs_tab.py` (log tail timers).

---

## 11. Runtime checklist (after `python main.py` restart)

- Stage 1 (adding jobs while a chain runs comes with stage 2, so these use what is possible now):
  - Run a chain. A queued job's tab is read-only; its I/O tab says "fixed at submit" per input; its driver log
    has `[DRIVER_BASE] Inputs fixed at submit`.
  - With the chain finished, add a missAlign on the alignment. The finished tsCtf still shows the alignment it
    read ("fixed at submit"); a new tsCtf__2 says "follows missAlign".
  - Make tsCtf fail on a few TS (cancel it mid-run), add a missAlign, Run: tsCtf resumes in its own dir on the
    old alignment. Then Reset inputs on it and Run: it says "follows missAlign" before the Run and starts over
    in a new job dir (events log: "inputs differ from its last attempt").
  - Aggregate dialog → Set up extraction: the Subtomo extraction job's input reads "set by hand" with the
    aggregate's optimisation_set, and Run keeps it.
- §8 table: the branch icon under Jobs on a project with two TM species and a missAlign. Rows in dependency
  order, one lane per chain, solid lines into jobs that ran, dashed into Scheduled ones; the address bar reads
  `/p/<project>/table` and Back returns; a row click opens the job; a running job's dot pulses and its elapsed
  time counts up; TS ok/total matches the roster.
- Stage 2 (no Run to here, §9.3): while a long job runs, add a job and clone one (the copy icon), Run. Only the
  unfinished jobs that are not live are submitted; one that reads the running job waits for it (its SLURM
  dependency, events log "afterok=[…]"); the running job is untouched. The running job and its upstream have no ×,
  and Delete in their job tab refuses with the reason. Stop still cancels everything. Approve a tilt filter while
  another chain runs.
- Stage 2 registry: two supervisors aggregating together (tsCtf and a clone tsCtf__2 finishing at once) both land
  in the registry (Journey shows both); toggle a TS exclusion during an aggregation; `registry/.lock` is gone
  after.
- Stage 3: Subset of 5 TS on a tsCtf output, tsReconstruct after it; only those 5 are reconstructed; the original
  tsReconstruct is untouched.
- Stage 4: Fork the same tsCtf output to a new project; it opens with 5 TS in its Journey; reconstruct there;
  exclude a TS in the new project and check the original still has it.

## 12. Modern-Python weave-in

- `class InputState(StrEnum)`: AUTOMATIC, SET_BY_HAND, FIXED_AT_SUBMIT — the I/O tab's three states, from one
  function next to `_select_source`.
- `ImportedOutput` as a pydantic model with a `Literal` `origin` ("fork" | "files" | "merge") — a discriminated
  union if the three grow different fields.
- A frozen `dataclass` `Subset(cut_instance, cut_output, ts_ids)` shared by the Subset job and the Fork dialog.

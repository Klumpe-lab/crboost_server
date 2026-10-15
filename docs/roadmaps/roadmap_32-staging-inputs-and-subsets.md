# Roadmap 32 — Staging while running, inputs fixed at submit, subsets and forks

**Status:** scoped 2026-10-14 from the branching survey
(`docs/reports/pipeline-graph/branching-survey-2026-10-12.md`); nothing built. Decisions owed in §9.

**In one line.** Jobs can be added and started while others run; a submitted job's inputs never move; and a
"fork" is one defined thing, a subset of some job's output, used either inside the project or as the start of a
new one, so in-project branches and "import tomograms from another project" are the same mechanism.

**Not in this roadmap:** an interactive graph builder (dropped: drag-to-wire is a lot of machinery for little
gain). A read-only graph overview comes after the stages here (§8). Per-TS streaming stays in roadmap 27 stage 3;
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
- Prerequisites: roadmap 33 S1 + S4 (with the project-wide lock gone, nothing else stops a resubmit into a dir
  whose supervisor still runs; the submit lock is in-process only) and 33 S5 (staged jobs then sit beside live ones).

**S2.3 Run to here.** On a job row (and the job tab): the job and its unfinished upstream, nothing else. Run keeps
meaning "everything unfinished on the roster". Stop stays "cancel everything"; cancel on a job stops it and,
through afterok, what waits on it.

**S2.4 Clone.** On a job row: same type, USER_PARAMS + SLURM settings + species copied through the typed setters
(`numeric_forward` / `enum_forward`), same hand-set inputs. The A/B primitive; today every variant is retyped.

**S2.5 Registry writes from concurrent jobs.** A lock file (O_EXCL, as roadmap 33 S1) around a supervisor's
load → ingest → save, and the save re-reads a sidecar that changed on disk since load before writing it. Server
writes (exclusion toggles, verdicts) take the same lock.

**S2.6 Fold-in: the delete preview** uses `resolve_slot_edges` instead of `default_pipeline.star`
(`pipeline_deletion_service.py:323-370`), so "N jobs read this one" is true under afterok — needed once deleting
while others run is normal.

Size: ~250 lines. Risk: highest of the roadmap (the run path). Runtime check in §10.

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

## 8. After this roadmap: the read-only graph overview

Spec in the survey (§5 stage 1): a seventh workspace view, rows = depth, columns = chains, cards with status and
per-TS counts, edges solid when set by hand / fixed at submit and dashed when automatic, Subset and imported
outputs as their own nodes. Click opens the job. No editing in the graph; the verbs live on the job (Run to here,
Clone, Add after, Fork…). It gets much simpler once stage 1 makes every submitted edge explicit.

Also later: read-side views follow a chosen chain instead of "the first job of each type" (survey §1.5,
`find_job_by_type`, `services/dashboard_data.py:59-67`, ~20 readers incl. the Journey).

---

## 9. Decisions owed by the maintainer

1. **Fixed inputs at submit (S1.1–S1.4).** Recommended. Cost: a retried job keeps its inputs even if a refiner was
   added since; Reset inputs re-opens them.
2. **What stays editable next to a live job (S2.1):** "not live and not upstream of live" (recommended), or also
   allow editing upstream jobs that are already finished (they are frozen anyway).
3. **Run to here** as the only new run verb (recommended), or also "Run from here" (the job and its downstream).
4. **Subset selection:** list only, or list + saved rules (recommended: list first, rules once the per-TS readouts
   it would filter on are settled).
5. **Fork destination rule of thumb (§5):** accept as the dialog's default hint?
6. **Unify Import tomograms and aggregation sources into imported outputs (§5)** — recommended; it is a migration
   of two records.
7. **Streaming (§7):** keep it in roadmap 27 after stage 2.

---

## 10. Stage 0 — facts to gather first

1. Warp settings files: are paths inside `warp_tiltseries.settings` relative to the settings file or absolute?
   Decides whether a CTF'd/aligned cut point can be referenced from another project unchanged.
2. Registry seeding: which sidecar fields name the source project's paths (job dirs in `outputs`), and whether a
   seeded copy needs them rewritten or only marked read-only.
3. Every reader of `job_model.paths` in drivers after `get_driver_context` (S1.2 makes them the submitted values).
4. Whether any driver mutates its upstream's files (the subset/fork model assumes none does).
5. Which `ui_mgr.is_running` gates are about editing and which about chrome (§2.1 lists the editing ones).

---

## 11. Runtime checklist (after `python main.py` restart)

- Stage 1: Run a chain; while tsCtf is queued, add a missAlign on the alignment; tsCtf starts on the alignment,
  its I/O tab says "fixed at submit". Add a tsCtf__2 afterwards: it says "follows missAlign".
- Stage 2: while a long job runs, add and clone jobs, Run to here on one of them; it waits on the running
  producer and starts after it. Cancel it; the running job is unaffected. Approve a tilt filter while another
  chain runs.
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

# Roadmap 07 — extract_pick_list becomes a real, UI-visible job (census #68)

**Status:** **ALL STAGES CODE-COMPLETE 2026-08-17** — S0 territory map, S1, S2, the #71 ALIGN, S3, S4
(§2/§3 amended in place as each stage found things; see the stage log). Census **#68 and #73 are
closed**, and so is roadmap-04 stage-5 item 3. ONE runtime pass owed at the end with the rest of the
arc. NOTE the S1/S2 re-sequencing: the switch to `driver_invocation()` moved from S1 into S2, so it
lands with the driver that can read it (S1 alone would have left the tree knowingly broken).

**Decision of record (2026-08-13, reaffirmed 2026-08-14):** make per-list extraction a bona fide
job — proper `JobType` + param class + one per-list instance id (~~`extractPickList__<list-slug>`~~
→ the `(species, tomo, slug)` triple, corrected 2026-08-17; see §2)
in `project_params.json`, bootstrapped via `get_driver_context`, status surfaced in the per-list
extraction UI. Explicitly **NO hidden/synthetic instances invisible to the UI** — which is why the
backend half must not land without its UI half. This roadmap is that whole.

**Timing (maintainer, 2026-08-14):** first slice of the de-novo/Journey UI pass. Hard
prerequisite for closing census **#73** (the driver's `tool_name="relion"` literal) and
roadmap-04 stage-5 item 3 (port onto `get_driver_context`) — both are gated on this and
deliberately left in place until it lands.

## 1. What you cannot see in the UI today (the "shadow zone")

`backend.extract_pick_list` (backend.py:226) hand-builds a CLI (`--box/--binning/--crop/...`) and
fires `drivers/extract_pick_list.py` as a **one-off sbatch outside the pipeline graph**. The job
has no entry in `project_state.jobs`, no `--instance_id`, no param class. Concretely, that means:

1. **While it runs:** the ONLY visibility is the dialog that launched it
   (`extract_pick_list_and_wait` awaits the out-dir). Close that dialog/tab and there is *no
   place in the UI* that shows an extraction is in flight. It is absent from the roster, the
   job-status polling, and the background-task tray. `squeue` on the headnode is the only truth.
2. **When it fails:** the driver writes `result.json {ok:false, error}` into the out dir. If the
   awaiting dialog is gone, the failure is invisible — the pick-list row simply never becomes
   "extracted", with no error surfaced anywhere. (The never-fail-silently policy is violated by
   omission here.)
3. **After it succeeds:** `PickList.mark_extracted` records optset + count, so the per-list row
   *does* show extracted state. ~~But the de-novo Journey rows hardcode
   `subtomo_status: "pending"` (`services/dashboard_data.py:832`), so extraction **never moves
   the journey strip** for a de-novo species.~~ **CLOSED 2026-08-17 by roadmap 11-S3** — the strip
   now derives it (see §3 stage 4 below). The rest of the shadow zone (1, 2, 4, 5) stands.
4. **Logs:** the sbatch stdout lands in the out dir, unreachable from any UI log viewer (those
   key off job instances).
5. **Parameters:** box/binning/crop used for a given extraction exist only in the launch command
   line — after the fact, nothing in the project records what geometry a list was cut with
   (only the resolved values inside the out dir's artifacts).

## 2. Target design

- `JobType.EXTRACT_PICK_LIST` in `services/models_base.py` + `ExtractPickListParams` in
  `services/jobs/` declaring: source (`candidate_optset` XOR `tomograms_star`), `list_star`,
  `tomo_name`, `species_id`, `list_slug`, and the geometry (`box_size`, `binning`, `crop_size`,
  `max_dose`, `min_frames`, `do_stack2d`, `do_float16`). `get_tool_name()` returns `"relion"` —
  closes census #73.
- **One instance per pick list**: ~~`extractPickList__<list-slug>`~~ **CORRECTED 2026-08-17 (S0
  territory map) — that scheme ALIASES.** `PickList.slug` is unique only within a
  `(species_id, tomo_name)` triple (`project_state.py:524`), and every hand-picked list is minted
  with the literal `slug="manual"` (`services/particles/ingest.py:36`, the sole producer for both
  ArtiaX saves and external imports), so ONE id would have served every manual list in the project;
  merged lists collide too whenever a merge name repeats on another tomogram (default `"Merged"` →
  `merged__Merged`). Verified against a real project: `agg_20260311_412_Grid3/project_params.json`
  registers two lists, both `slug="manual"`, both species `412`, tomograms `..._Position_13` and
  `..._Position_13_3` — the second submit would have overwritten the first's params, status and
  geometry. The id is therefore the same triple `pick_list_producer_id`
  (`path_resolution_service.py:88`) already keys on:
  **`extractPickList__{species_id}__{fs_slug(tomo_name)}__{slug}`**, built through
  `InstanceId(JobType.EXTRACT_PICK_LIST, suffix)` (`models_base.py:103` is the sole owner of the
  `__` separator; never f-string it). The tomogram is slugged with `list_ref.fs_slug` because the id
  goes unquoted into a shell command (`spec.py:253`) — so it is NOT string-equal to the resolver's
  producer id, which embeds the raw name; both are built from the triple, neither derives from the
  other. The on-disk layout never had this bug: `out_dir = Path(list_star).parent / slug` under
  `Curation/<species>/<tomo>/` was already triple-scoped.
  `ExtractPickListParams` **must also carry `species_id` as a real field**: `InstanceId.split` takes
  the FIRST `__` only, so a three-part suffix reports `species_id="412__..._13__manual"`, and
  `species_references` (`project_state.py:872`) compares that suffix by equality — without the field,
  deleting a species orphans every one of its per-list extraction instances.
  Re-extraction reuses the same instance (params updated, status reset) — no instance-per-attempt
  churn — but **status must be reset BEFORE the geometry is written**: `AbstractJobParams.__setattr__`
  silently drops writes to `USER_PARAMS` fields when `execution_status` is not SCHEDULED/FAILED
  (`_base.py:400-425`), so the reverse order re-extracts with stale box/binning/crop and no error.
- Driver bootstraps via `get_driver_context` (`--instance_id/--project_path` only); the
  hand-rolled argparse dies. KEEP as-is per census: `result.json` contract (#69), catch-all →
  structured error (#70), output-probe idempotency (#72). ALIGN while touching: `Command:`
  log format (#71).
- **Not a scheme/roster job**: it stays outside `default_pipeline.star` and the RELION scheme —
  its home is the per-list extraction UI, not the pipeline roster. (It still writes the RELION
  exit markers via qsub.sh as today.) **AMENDED 2026-08-17 — this is not free, and the S0 map found
  the mechanism the original spec left unstated.** Nothing filters `state.jobs` by "is this a
  pipeline job", so a bare instance would be swept by five separate sites. Two levers make it real:
  `IS_INTERACTIVE: ClassVar[bool] = True` on the param class (the `tilt_filter` precedent,
  `_base.py:72` "Interactive tools manage their own status") and `phase=None` on the JobSpec row.
  `IS_INTERACTIVE` is what stops (a) `sync_all_jobs`' orphan sweep forcing every instance absent from
  `default_pipeline.star` back to SCHEDULED and nulling `slurm_job_id` on each 3-s tick
  (`pipeline_runner.py:322`), (b) `pipeline_monitor` sweeping a SCHEDULED instance into
  `deploy_and_run_scheme` on crash recovery (`:159/:286`) — which would deploy per-list extraction as
  a real scheme job, precisely the §4 non-goal, (c) `get_pipeline_overview` counting it into the
  roster's done/total and holding `scheduled > 0` forever, the documented stuck-spinner bug
  (`:604`), and (d) the STOP-ON-FAIL `mem_has_live_job` heuristic (`:164`). `phase=None` keeps it out
  of the roster palette (`PHASE_JOBS`) and the Species page's Jobs tab (`PARTICLE_JOB_TYPES`).
  **Gap found, fixed in S1:** `stop_and_cleanup` (`pipeline_runner.py:1253`, and the afterok scancel
  loop at `:1156`) checks no such flag — it flips EVERY RUNNING/SCHEDULED instance to FAILED, so
  stopping the pipeline would scancel and mis-mark an unrelated in-flight extraction. Adding the
  guard there also fixes the same pre-existing bug for `tilt_filter`.
- **Param-class shape** (S0 map): declare **no `INPUT_SCHEMA`** — `get_driver_context` always
  re-resolves paths and `sys.exit(1)`s on `PathResolutionError` (`driver_base.py:111-120`), so the
  source stars ride as plain fields — and **no `OUTPUT_SCHEMA`**, or every per-list instance joins
  `_build_output_index` (`path_resolution_service.py:606-640`) as a candidate for other jobs' input
  slots. The JobSpec row is **mandatory, not cosmetic**: `jobtype_paramclass()` is derived from
  `JOB_SPECS`, so without a row `ProjectState.load` drops the instance with a load_warning and the
  persisted job vanishes on reload.
- Status surfacing is **derived, never a stored flag** (per the per-list extraction model):
  `ListExtractionState` derives from the instance's execution status + out-dir artifacts.

## 3. Stages

1. **S1 — identity:** `JobType.EXTRACT_PICK_LIST`, `ExtractPickListParams`, jobtype↔paramclass
   mapping, JobSpec table entry. `backend.extract_pick_list` creates/updates the per-list
   instance and submits via the standard `driver_invocation()` (instance form).
2. **S2 — driver port:** `drivers/extract_pick_list.py` onto `get_driver_context` +
   `DriverContext`; params from the instance; census #73 literal → `params.get_tool_name()`;
   #71 log format. `result.json` + markers unchanged (dashboard contract).
3. **S3 — status plumbing:** the awaiter (`extract_pick_list_and_wait` /
   `extract_authoritative_pending`) updates the instance's execution status; failure text from
   `result.json` lands on the instance so it survives dialog death.
   **AMENDED 2026-08-17 (S0 map).** Two things the original bullet assumed away:
   (a) **The awaiter is the ONLY refresher, by necessity.** Nothing else refreshes a one-off
   instance: `PipelineMonitor._tick_once` iterates only projects with `pipeline_active=True`
   (`pipeline_monitor.py:220`), and `reconcile_afterok` is additionally gated on
   `use_afterok_orchestrator`. Setting `pipeline_active` from an extraction submit is NOT an option —
   the roster would read as a live run. So the awaiter owns the transitions, and we additionally set
   `paths["job_dir"] = out_dir` + `slurm_job_id` at submit so that `reconcile_afterok` picks it up for
   free on afterok projects (its `_resolve_afterok_job_dir` already keys off exactly those).
   Residual, and NOT as benign as this bullet first claimed (corrected 2026-08-17 by the S3/S4
   review): if the awaiter dies — server restart, tray cancel, or `timeout_s` elapsing — nothing
   else moves the instance AND nothing runs `PickList.mark_extracted`, which only the two awaiter
   callers do. `extraction_state()` keys off the RECORDED `extracted_path`, so it cannot discover
   an output nobody recorded: a job that finishes afterwards leaves its optimisation set orphaned
   on disk with BOTH surfaces reading "not extracted". Re-extraction is the only recovery, which is
   why no path to it may be hard-blocked (see the S4 record and the open findings in
   notes/07-stage-snap/s3s4/HANDOFF.md).
   (b) **There is no failure-text field to land on.** `AbstractJobParams` carries only
   `missing_inputs`. S3 adds one, deliberately OUTSIDE `USER_PARAMS` so it stays writable on a
   non-SCHEDULED instance (the same immutability rule that forces the status-reset ordering above).
   Note also that `backend.extract_pick_list` deletes the exit markers and `result.json` and
   `rmtree`s `out/` before every submit (`backend.py:268-275`, a deliberate defeat of #72 so a user
   re-extract re-cuts): any status derivation that probes those markers must read "nothing there"
   right after a resubmit as QUEUED, never as failed.
4. **S4 — UI:** per-list row gets a live status chip (queued/running/ok/failed with the error on
   hover) + log access; ~~de-novo Journey rows derive `subtomo_status` from per-list extraction
   state instead of the hardcoded `"pending"` (dashboard_data.py:832)~~ **DONE 2026-08-17 in roadmap
   11-S3** — `services/dashboard_data.pick_list_subtomo_status`, landed TWO-state (any list of the
   (species, tomogram) with an extraction output → `ok`, else `pending`). The three-state rule this
   bullet originally specified was NOT built: telling STALE from EXTRACTED turns on
   `PickList.filtered_count`, a cache that needs `picks_filter.sync_filtered_count` (a pandas read
   per list) — unaffordable on a path that derives every tomogram, and wrong-but-plausible amber if
   left unsynced. Freshness stays on the Picks tab's per-list badge, which does sync. So NO `warn`
   token was added (`ui/dashboard/strip.py` `_STATUS_WORD` gained only `"skip"`, and there is no
   `.cb-strip-pickcell.warn` rule). Plus an `only_ts` scope on `collect_species_journey` so the main
   pane's signature does not pay the per-list stats for every tomogram. See roadmap 11's S3 stage record; pre-check the
   authoritative-list radio for single-list de-novo species while in this code. NOTE for whoever
   lands 07: the Journey's authoritative radio is DISPLAY-ONLY since 11-S3 — the pre-check belongs on
   `ui/species/picks_tab.py`, and the Journey's `_main_signature` already folds the choice in
   (`auth_sig`) so it repaints.
   **AMENDED 2026-08-17 (S0 map)** — three mechanics the bullet has to respect:
   the Picks table is an 8-column CSS grid (`.cb-ptable-row`, `ui/dashboard/css.py`) and `_render_row`
   emits exactly 8 children, so a live chip is either a 9th column (header cell + a
   `grid-template-columns` edit) or it rides inside the existing 26 px `ext` cell, which fits a dot
   and not a chip; `_PicksView.signature()` does NOT read `execution_status`, so the status must be
   folded into `_Computed`/`ListRow` or into the signature, else `_recompute` runs and the view still
   will not repaint; and the only log viewer (`ui/pipeline_builder/logs_tab.py`) is bound to per-tab
   widget refs and `relion_job_name`, which this job never gets — use `backend.get_job_logs`, which is
   reusable and tolerates the absolute out dir where qsub already writes `run.out`/`run.err`.

## 4. Non-goals

- No scheme/schemer involvement, no roster row, no afterok wiring — this job stays a one-off.
- No change to the extraction math or `relion_tomo_subtomo` invocation.
- No stored extraction-state flags on `PickList` beyond the existing `mark_extracted` record.

## Stage log (append-only)

- 2026-08-14 — scoped (incl. the shadow-zone inventory). No code.
- 2026-08-17 — **S0 territory map** (6-agent read of the extraction path, job-identity machinery,
  driver port, status/UI surface, census ledger, and the instance-id key). No code. §2 and §3 above
  are amended in place with what it found; the four corrections that change the build, in order of
  cost if missed:
  (1) **The instance id in §2 aliased.** `extractPickList__<list-slug>` collapses every manual list
  in a project onto one instance — CONFIRMED against a real project with two colliding `manual`
  lists. Corrected to the `(species, tomo, slug)` triple that `pick_list_producer_id` already uses,
  plus a real `species_id` field so `remove_species` can still find these instances through
  `species_references` (which compares the FIRST `__` suffix by equality).
  (2) **"Not a scheme/roster job" had no mechanism.** Nothing filters `state.jobs` by pipeline
  membership, and five sweeps would have picked the instance up — one of which
  (`pipeline_monitor`'s crash recovery) would have DEPLOYED per-list extraction as a scheme job, the
  exact §4 non-goal. `IS_INTERACTIVE = True` + `phase=None` are the levers; `stop_and_cleanup` is
  missing the guard entirely and gets it in S1 (which also fixes `tilt_filter`).
  (3) **S3 had no refresher and nowhere to put the error.** Only the awaiter can move a one-off
  instance's status (the monitor tick is gated on `pipeline_active`), and `AbstractJobParams` has no
  failure-text field — S3 adds one outside `USER_PARAMS`, because that same immutability rule also
  forces "reset status, THEN write geometry" on re-extraction.
  (4) **Census bookkeeping.** #73 is confirmed a one-file fix (the only other `tool_name=` literals
  in `drivers/` are the two deliberately-kept IsoNet closures). #71's ALIGN is now cosmetic — the
  fleet-wide `[run_command] $ …` echo subsumes it — and `extract_pick_list` has NO recorded
  transcript, so the usual byte-parity acceptance for an ALIGN commit is unavailable here; the
  S2 record must say so rather than claim a diff it cannot run. Per roadmap 04's prime directive the
  #71/#73 alignments land as their own flagged commit, not folded into the mechanical port.
- 2026-08-17 — **S1 CODE-COMPLETE** (`ruff check .` clean; `ruff format` clean on all 8 touched
  files; `py_compile` + `check_boundaries.py` owed with the runtime pass — no python in the sandbox).
  NEW `JobType.EXTRACT_PICK_LIST = "extractPickList"` (`models_base.py`), NEW
  `services/jobs/extract_pick_list.py` (`ExtractPickListParams`: `IS_INTERACTIVE = True`,
  `JOB_CATEGORY.EXTERNAL`, USER_PARAMS = the seven geometry fields, identity fields `tomo_name` /
  `list_slug`, source fields `candidate_optset` / `tomograms_star` / `list_star`, `last_error`
  outside USER_PARAMS, `get_tool_name() -> "relion"`, NO INPUT_SCHEMA / OUTPUT_SCHEMA), exported
  through `services/jobs/__init__.py` + the `services/job_models.py` shim, and registered as ONE
  `JobSpec` row with `phase=None`, `driver="extract_pick_list.py"`. NEW
  `list_ref.extract_pick_list_instance_id(species_id, tomo_name, slug)` — the corrected triple key,
  built through `InstanceId`, documented against its `pick_list_producer_id` twin.
  `backend.extract_pick_list` now creates/updates the instance before sbatch (status reset FIRST,
  then geometry, then `paths["job_dir"] = out_dir`), and on the way out sets `slurm_job_id` +
  QUEUED, or FAILED + `last_error` on either failure path; it returns `instance_id` alongside
  `slurm_job_id`/`out_dir`. `extract_authoritative_pending` needs no change — it submits through
  `extract_pick_list`, so the batch path gets instances for free.
  **Deviations / findings:**
  (1) **S1/S2 re-sequenced.** §3 has S1 submitting through `driver_invocation()` (instance form)
  and S2 porting the driver. Done in that order the tree is knowingly broken in between — the
  driver still parses the 11-flag CLI. So S1 creates the instance and leaves the CLI invocation
  exactly as it was (extraction keeps working), and the switch to `driver_invocation` moves into S2
  where it lands together with the driver that can read it. Nothing else about the split changes.
  (2) `species_id` did NOT need adding — it is already a field on `AbstractJobParams` (`_base.py:67`),
  so the `species_references` / `remove_species` hazard is closed by simply setting it. §2's wording
  ("must carry `species_id` as a real field") stands as a requirement, not as new schema.
  (3) **`stop_and_cleanup` guard landed here** (both sites: the afterok scancel list and the
  RUNNING/SCHEDULED → FAILED sweep). It is the one sweep that never learned about `IS_INTERACTIVE`,
  so stopping the pipeline would scancel an in-flight per-list extraction and leave it reading
  Failed. This also fixes the identical pre-existing bug for `tilt_filter` — an in-flight DL filter
  was equally exposed.
  (4) Verified the row lands where intended: `PARTICLE_JOB_TYPES` (Species Jobs tab) filters
  `phase == PHASE_PARTICLES` and `PHASE_JOBS` (roster palette) is built from the phased rows, so
  `phase=None` keeps it out of both. It does join `PIPELINE_ORDER`, which is consumed only by
  `ui_state._JOB_ORDER` (ordering) and `get_ordered_jobs()` — the latter has ZERO callers
  (dead-code candidate, left in place per the surgical-diff rule).
  (5) **Known bounded edge, NOT fixed:** `ProjectState.load` backfills an empty `pipeline_order` as
  `list(jobs.keys())`, so a project that has ONLY ever run per-list extractions (never built a
  pipeline) would take its extraction instances into `pipeline_order`. Reachable only in that
  never-built-a-pipeline case, and bounded everywhere it matters by `IS_INTERACTIVE` (the
  orchestrator, both monitor sweeps, the overview count and the STOP-ON-FAIL heuristic all skip it).
  Not special-cased because neither `IS_INTERACTIVE` nor `phase is None` actually means "not a
  pipeline job" — `tilt_filter` is interactive AND a scheme job, `tsImport` is phase-less AND a
  scheme job — and inventing a third flag for one row is worse than the edge.
- 2026-08-17 — **S2 CODE-COMPLETE** (`ruff check .` clean, `ruff format` clean on both touched
  files; `py_compile` + `check_boundaries.py` owed with the runtime pass). `drivers/extract_pick_list.py`
  is now bootstrapped like every other driver: the 11-flag argparse is gone,
  `get_driver_context(ExtractPickListParams)` returns the instance, and every value the CLI used to
  carry (`--candidate-optset`/`--tomograms-star`/`--list-star`/`--tomo`/`--box`/`--binning`/`--crop`/
  `--max-dose`/`--min-frames`/`--stack2d`/`--float16`, plus `--out-dir` and `--project-root`) is read
  off `params` or off the context. `backend.extract_pick_list` builds its launch with
  `driver_invocation(...)` instead of `driver_launch_prefix(...)` + hand-built flags, so the whole
  fleet now has ONE launch shape; `shlex` fell out of `backend.py` with the last hand-quoted flag,
  and `driver_launch_prefix`'s docstring lost the "third caller" paragraph that named this driver.
  **Census dispositions, re-verified at current line numbers** (the ledger's are stale by ~+9..+13
  since the ToolCommand migration):
  · **#68 CLOSED** — the job has an identity (S1) and the driver bootstraps from it.
  · **#73 CLOSED** — `tool_name="relion"` → `params.get_tool_name()`, and the in-code comment that
  named the census item went with it. Verified repo-wide: the only `tool_name=` literals left in
  `drivers/` are the two deliberately-kept IsoNet closures (`denoise_train.py:49`,
  `denoise_predict.py:411`), each carrying its own justification. So #73 was indeed a one-file fix.
  · **#69/#70 KEPT and strengthened.** `result.json` and the catch-all → structured error survive
  verbatim, but the write now wraps the BOOTSTRAP too: `get_driver_context` `sys.exit(1)`s on a
  missing/mistyped instance, which would have produced an exit marker and NO `result.json` — the
  awaiter's error text would have been empty. `SystemExit` is caught explicitly (it is not an
  `Exception`), `Path.cwd()` is resolved before the bootstrap to know where to write, and
  `_write_result` narrows its own swallow to `OSError` per the exception policy.
  · **#72 KEPT** — the `out/particles.star` + `out/Subtomograms/` probe is untouched, and so is
  `backend.extract_pick_list`'s deliberate defeat of it (markers + `result.json` unlinked and `out/`
  rmtree'd before every submit) so a user re-extract re-cuts.
  · **#71 NOT in this commit** — see the next entry; roadmap 04's prime directive keeps an ALIGN out
  of the mechanical migration.
  Also closes **roadmap-04 stage-5 item 3** (port onto `get_driver_context`), which was left open
  precisely for this.
  **Deviations / findings:**
  (1) The driver does NOT touch `RELION_JOB_EXIT_*`, unlike the `reconstruct_particle` template it
  otherwise mirrors: `config/qsub.sh` already touches them for this job, and two writers would
  contradict each other. Exit code stays the signal.
  (2) The old argparse made the source XOR structurally impossible to violate
  (`add_mutually_exclusive_group(required=True)`); on an instance it is just two fields. The driver
  now raises a named error when neither is set rather than silently building from whichever it finds
  — the submit site still guarantees the XOR, but the driver no longer trusts it silently.
  (3) `get_driver_context` overwrites `job_model.paths` in-process with freshly resolved paths, so
  the `paths["job_dir"]` S1 persists is a server-side record for `reconcile_afterok`, not something
  the driver reads back. With no INPUT_SCHEMA the resolver returns an empty slot map, which is legal.
- 2026-08-17 — **07 ALIGN (census #71)**, its own flagged commit per roadmap 04's directive
  ("ALIGN lands as its own flagged commit after the pure migration of that driver"). One line:
  `[extract-list] command: {cmd}` → `[DRIVER] Command: {cmd}`, matching `reconstruct_particle.py:101`
  / `ts_import.py:102` / `class3d.py:103`. Recorded honestly: the census's original rationale for
  this ALIGN ("a transcript grep would silently drop this driver") no longer holds — the fleet-wide
  `[run_command] $ …` echo in `driver_base` subsumes it — and `extract_pick_list` has NO recorded
  transcript in `04-stage2-transcripts.md`, so the usual acceptance test for an ALIGN commit
  (byte-identical `[run_command]` lines against the transcript corpus) is UNAVAILABLE here. It lands
  for fleet consistency only, which is why it is separable and separately labelled.
- 2026-08-17 — **S3 CODE-COMPLETE** (`ruff check .` clean; `ruff format` clean on every touched
  file; `py_compile` + `check_boundaries.py` owed with the runtime pass — no python in the sandbox).
  The awaiter is now the refresher §3 says it must be. `_await_extraction_outdirs` takes
  `{out_dir: (instance_id, slurm_job_id)}` + an explicit `project_path`; its off-loop scan gained a
  third branch — `run.out` exists ⇒ SLURM started this job ⇒ RUNNING — and it calls the new
  `_record_extraction_progress` once per tick, which writes RUNNING / SUCCEEDED / FAILED +
  `last_error` (the driver's own `result.json` text, else a pointer to `run.err`) onto the instances,
  one forced save per tick that changed something. `last_error` needed no schema change: S1 landed
  the field.
  **Deviations / findings:**
  (1) **The RUNNING probe would have lied on every re-extract.** `extract_pick_list` unlinked the
  exit markers and `result.json` but NOT `run.out`/`run.err`, so the first tick after a resubmit read
  the previous run's stdout file and reported RUNNING while SLURM still had the job PENDING. Both
  files now go with the markers — which also stops the log dialog showing the last run's output.
  (2) **The submit had a transient lie of its own.** It destroyed the output BEFORE resetting the
  instance, so a render landing in between paired the freshly-absent output (`extraction_state()` →
  NOT_EXTRACTED) with the PREVIOUS run's `Succeeded`/`Failed` and its stale error text — on the one
  shared in-memory state every tab reads. The instance update now precedes the cleanup; the only
  visible transient is "not extracted · submitting", which is true.
  (3) **Two awaiters can watch one instance** (a user-forced re-extract; the batch path racing the
  per-list button), and the older could stamp its own outcome over the newer run. Hence the SLURM id
  in `targets`: `_record_extraction_progress` writes only while the instance still tracks the id THIS
  awaiter submitted, and logs when it declines.
  (4) **The batch path was the real double-submit vector, closed at the source.** The gate calls a
  list "pending" the moment a re-extract clears its output, so "Extract all pending" during a single
  in-flight re-extract resubmitted THE SAME list into the dir the first job was writing.
  `extract_authoritative_pending` now skips any list whose instance is SCHEDULED/QUEUED/RUNNING and
  reports it under `still_running`. **This skip has no force path — see the open findings.**
  (5) **Two surfaces outside the Picks tab were showing this job — both regressions introduced by 07
  itself** (S1 gave the instance a status, S3 gives it Running/Failed), both fixed here:
  `_derive_live_status` walks the RAW `project_params.json` jobs dict with no `IS_INTERACTIVE` filter
  (it cannot see ClassVars), so ONE failed list extraction left the whole project reading "failed" on
  the hub indefinitely and a running one made an idle project read "running"; the same dict feeds
  `total_jobs_planned`. Both now filter through `_is_pipeline_job_dict` (job-type based; `tiltFilter`
  stays counted, it IS pipeline work). And `reconcile_afterok`'s Pass-4 `any_live` vote had no
  `IS_INTERACTIVE` guard, so on an afterok project a live per-list extraction held
  `state.pipeline_active = True` — the roster reading as a live run, the exact §4 non-goal, and
  pinned there forever if the extraction died with the server.
  (6) Latent NameError fixed while in the hub scan: `jobs_dict` was only bound inside the try, so an
  unreadable `project_params.json` skipped the project entirely instead of reaching the fallback path
  that exists so "the UI always has something to show".
- 2026-08-17 — **S4 CODE-COMPLETE** (same lint status; same two checks owed). The Picks table gained a
  9th `job` column beside `ext`. `services/particles/species_overview` grew a frozen `ExtractJob`
  (status, error, job_dir, slurm id, box/bin/crop + an `is_live` property) and `ListRow.extract_job`,
  so the live half rides the read model the tab already computes off-loop: `PicksTab._memory_key`
  already folds every job's `execution_status`, and `_PicksView.signature()` already folds
  `c.overview`, so the repaint chain moves with no new signature input and no new disk I/O.
  `_render_job_chip` always emits exactly one grid child (empty for the auto row and for a list never
  submitted); RUNNING is a `ui.spinner`, the rest are glyphs in the roster's own status colours; the
  hover carries SLURM id, the geometry the list was cut with (closing shadow-zone item 5 — that used
  to exist only in the launch command line) and the failure text; clicking opens
  `list_actions.open_extraction_logs`, a dialog over `backend.get_job_logs` on the recorded absolute
  out dir, since the pipeline log viewer keys off a `relion_job_name` this job never gets.
  **Deviations / findings:**
  (1) **Not built as §3-S4 (and picks_tab's own docstring) promised** — "the badge's hover gains the
  instance's execution status" was an Option-B design. Fusing the live and derived halves into one
  badge is what forces the amber-lie 11-S3 already rejected, so they are two columns, each true on
  its own: `ext` = is there a recorded, current extraction output; `job` = what did the last attempt
  do. The docstring is rewritten to say that.
  (2) **The roadmap's "pre-check the authoritative radio for single-list de-novo species" is
  deliberately NOT built.** Unpersisted it lies (the gate reads the STORED choice, so a checked radio
  would sit next to a BLOCKED roll-up); persisted it decides a downstream-affecting question with no
  user click, which is the invented-default the project bans. Instead the per-tomogram group header
  says "no authoritative list" with the fixing click named in the tooltip. **Its condition is
  species-scoped and the fact is per-tomogram — see the open findings.**
  (3) `BoundStatusDot`/`BoundStatusBadge` deliberately not reused: they resolve the model through
  `current_project_state()` (client context) while this tab is explicitly path-scoped, and they
  render a yellow "Scheduled" dot on a resolution miss — a wrong-but-plausible default.
  (4) **The task tray was reporting failed extractions as green successes** (the registry marks a
  task succeeded on any non-exception return, and the body returned an `err()` dict) — the very
  invisible failure this roadmap exists to end. The body now raises with the error text and returns a
  particle-count summary on success.
  (5) `list_admin.delete_pick_list` now pops the list's extraction instance: a `manual` list is
  re-minted under the same slug by the curation watcher, and would otherwise inherit the deleted
  list's status and failure text. **No live-extraction check — see the open findings.**
  (6) Known caveat, untouched: `pipeline_runner.get_job_logs` reads both files with blocking `open()`
  on the event loop (pre-existing; the pipeline log tab hits it every 3 s). This dialog reads once on
  open and on an explicit Reload.
- 2026-08-17 — **S3/S4 adversarial review: 15 findings filed, 0 verified.** A 4-lens review fleet ran
  over the whole change; the 7-agent verify phase then died on a session limit, so `confirmed: 0` in
  its output means NOTHING WAS CHECKED, not "clean". One finding was verified by hand and fixed
  immediately (the confirmed re-extract was a silent no-op: `BackgroundTaskRegistry.submit` dedupes by
  returning the running task id WITHOUT calling the coroutine, so the user's consent produced no
  submit while the toast said "Extraction submitted"; the replace path now cancels the old awaiter and
  submits without the dedup key, and the non-replacing path says what actually happened). The other 14
  are triaged but UNVERIFIED in **notes/07-stage-snap/s3s4/HANDOFF.md** — read that before the runtime
  pass. Snapshot of every touched file: `notes/07-stage-snap/s3s4/`.
- 2026-08-18 — **S5 (review follow-up): the 8 open findings closed.** Written against the triage in
  `notes/07-stage-snap/s3s4/HANDOFF.md` §4 (still UNVERIFIED at runtime — this stage does not change
  that; `ruff check .` / `ruff format --check` clean).
  (A+B, the big one) **A stranded instance no longer bricks the list.** `_await_extraction_outdirs`
  dies with its BackgroundTask, and nothing else moved these instances (`IS_INTERACTIVE` skips every
  sweep; `PipelineMonitor` ticks only `pipeline_active` projects, which a one-off extraction never
  sets) — so a server restart, a tray Cancel or `timeout_s` elapsing left the chip asserting
  Queued/Running forever AND made `extract_authoritative_pending` report that list as "still running"
  permanently, with the per-row confirm as the only escape. New `backend.reconcile_pick_list_extractions`
  asks SLURM instead of trusting the stored status: id still queued → live (promote to RUNNING once
  `run.out` exists); `query_jobs_by_ids` → `None` → squeue itself failed, conclude NOTHING; id gone →
  the out dir decides, and a `result.json` with `ok` also runs `PickList.mark_extracted`, which
  recovers an output whose awaiter died instead of re-cutting it (that was finding B — the roadmap §3
  text calling this residual "cosmetic" was corrected in the same pass). An instance with no recorded
  SLURM id is reported `unknown` and left alone: that is also the sub-second window inside
  `extract_pick_list` between the status reset and sbatch returning. Called from the batch path
  (before `enumerate_authoritative`, so a recovered list drops out of `pending` rather than being cut
  twice) and from `PicksTab._recompute` (so the chip settles; no squeue call at all when nothing is
  non-terminal). The batch skip now votes on that SLURM-proven `live` set, and an instance with no id
  is deliberately resubmitted — there is provably no job to clash with.
  (C) The two comments that outlived their truth: `reconcile_afterok`'s Pass-4 said interactive jobs
  "stay in `tracked` so passes 1-3 still reconcile them" (true only while a REAL pipeline job is live)
  and `extract_pick_list` claimed "afterok projects reconcile free". Both now say what actually holds
  and name the reconciler as the thing that covers the rest.
  (D) The "no authoritative list" hint was species-scoped for a per-tomogram fact — a species WITH a
  candidate-extract job still has no auto row on a tomogram where the CE found nothing, which is the
  same dead end, and the hint was suppressed there. Condition is now simply "no row in this group is
  authoritative", which also catches a choice left dangling by a deleted list, and is shorter than
  what it replaced.
  (E) `open_extraction_logs` leaked its whole element tree on every open (`dialog.close()` at a
  layout slot nothing clears; each log line is an element). Now `await dialog` + `dialog.delete()` —
  which also makes the caller's SingleFlight cover the dialog's lifetime as its docstring claims. The
  `if dialog.value` guard before the await is load-bearing: `Dialog.__await__` OPENS the dialog, so a
  user who dismissed it while `_load()` was reading Lustre would have seen it pop back.
  (F) `_LOG_MAX_LINES` was both the truncation threshold and `ui.log(max_lines=…)`, and `ui.log` drops
  from the FRONT — so the "[… truncated N lines …]" marker was the line it evicted. Split into
  `_LOG_MAX_LINES` (what we push) and `_LOG_WIDGET_LINES` (what the widget holds), as `logs_tab.py`
  already does.
  (G) `_refresh_status` bailed silently when the instance had gone (reachable: another tab deletes the
  list, whose instance S4 pops), leaving the header asserting a stale status beside a "job directory
  not found" pane. It now says "gone" and why.
  (H) `delete_pick_list` rmtree'd a running extraction's out dir and popped the instance holding its
  SLURM id — the job then re-created the directory with nothing left in the project able to stop it or
  explain it. The confirm now names the in-flight job, and the delete calls new
  `backend.cancel_pick_list_extraction` (scancel + FAILED with a reason) first. `list_admin` documents
  that as the caller's precondition rather than doing it — it is UI-free and holds no SlurmService.
  (I, first half) The pre-submit `rmtree` of `out/` (one 2D stack per particle, on Lustre, ×N in the
  batch prologue) moved off the event loop. The second half — `state.jobs` mutated from the loop while
  a thread iterates it — stays reported-not-fixed: pre-existing in kind and self-healing on the next
  tick.

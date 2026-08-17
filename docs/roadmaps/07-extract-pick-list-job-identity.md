# Roadmap 07 — extract_pick_list becomes a real, UI-visible job (census #68)

**Decision of record (2026-08-13, reaffirmed 2026-08-14):** make per-list extraction a bona fide
job — proper `JobType` + param class + one per-list instance id (`extractPickList__<list-slug>`)
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
- **One instance per pick list**: `extractPickList__<list-slug>`, created/updated by
  `backend.extract_pick_list` at submit time, persisted in `project_params.json`. Re-extraction
  reuses the same instance (params updated, status reset) — no instance-per-attempt churn.
- Driver bootstraps via `get_driver_context` (`--instance_id/--project_path` only); the
  hand-rolled argparse dies. KEEP as-is per census: `result.json` contract (#69), catch-all →
  structured error (#70), output-probe idempotency (#72). ALIGN while touching: `Command:`
  log format (#71).
- **Not a scheme/roster job**: it stays outside `default_pipeline.star` and the RELION scheme —
  its home is the per-list extraction UI, not the pipeline roster. (It still writes the RELION
  exit markers via qsub.sh as today.)
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

## 4. Non-goals

- No scheme/schemer involvement, no roster row, no afterok wiring — this job stays a one-off.
- No change to the extraction math or `relion_tomo_subtomo` invocation.
- No stored extraction-state flags on `PickList` beyond the existing `mark_extracted` record.

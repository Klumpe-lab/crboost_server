# Roadmap 28 — Species-aware input wiring: a species' jobs read their own species' upstream

**Status:** scoped 2026-10-05 from a reproduced failure in the pilot project `AGG412_20260204_412`;
Q1/Q2 decided and stages 1–3 built the same day (ruff-clean, **not run**). The user check (§5) is owed.
Takes ownership of the side finding in roadmap 21 §6 and the "found along the way" note in `README.md`
(species match counted only for succeeded candidates).

## Before → After

**Before:** when a job's input slot has no override, the resolver picks a producer by
(species match, counted only if the producer already SUCCEEDED) → preferred job type → highest job
number. In a freshly built two-species chain nothing has succeeded, so species plays no part and the
highest job number wins. Template matching declares its pass-through `tomograms.star` as an output, so
each TM job is also a candidate input for every other TM job. The result is either a refused Start (a
TM ↔ TM cycle) or, when no cycle forms, species A's extraction silently reading species B's TM.

**After:** an automatically chosen producer is always upstream of its consumer by stage, never bound to
another species, and of the consumer's own species whether or not it has run yet. Two equally good
same-species producers are reported as ambiguous, not guessed. Auto-selection can no longer form a
cycle, and cross-species wiring exists only as an explicit, visible override. **Gain:** adding a
second species, or an A/B copy of a job, stops scrambling the chain.

---

## 1. The incident (with evidence)

Pilot `AGG412_20260204_412`. The preprocessing chain had run up to tsReconstruct (`External/job006`).
Six picking jobs were added, all Scheduled: TM, candidate extraction and subtomo extraction for
`412_particle` and for `dm_ovary_ribosome`. Start refused:

```
start_pipeline failed: Pipeline DAG is not acyclic (cycle among ['subtomoExtraction',
'subtomoExtraction__2', 'templatematching', 'templatematching__2', 'tmextractcand',
'tmextractcand__2']); cannot submit afterok chain.
```

`paths` as the failed Start left them in `project_params.json`:

| Job (species) | Slot | Wired to | Should be |
|---|---|---|---|
| `templatematching` (412) | input_tomograms | job006 tsReconstruct | job006 ✓ |
| `templatematching__2` (ribosome) | input_tomograms | **job007**, the 412 TM's `tomograms.star` | job006 |
| `tmextractcand` (412) | input_tm_job | **job008**, the ribosome TM (override to job007 set by hand afterwards) | job007 |
| `tmextractcand__2` (ribosome) | input_tm_job | job008 ✓ (highest number, right by luck) | job008 |
| `subtomoExtraction` (412) | input_optimisation | **job010**, the ribosome extraction | job009 |
| `subtomoExtraction__2` (ribosome) | input_optimisation | job010 ✓ | job010 |

**The mechanism.** Start allocates job numbers and resolves `paths` in one loop
(`pipeline_orchestrator_service.py:483`). When `templatematching` was resolved, its sibling had no
number yet, so tsReconstruct (job006) won. When `templatematching__2` was resolved, job007 already
existed and outranked job006. The DAG is then derived again, after every number exists
(`resolve_edges`, `:588`). This time `templatematching` picks job008 (> job006) and
`templatematching__2` picks job007, which closes a TM ↔ TM cycle. The other four jobs hang off that
cycle, so all six are listed. The `paths` written moments earlier and the edges the DAG used disagree.

## 2. Defects

**(a) Species only counts after a producer has run.** `_choose_candidate_for_slot`
(`services/path_resolution_service.py:924`) computes
`species_match = 1 if (consumer_species_id and c.species_id == consumer_species_id and succeeded)` (`:964`).
The gate was added so a job's own pending output couldn't win over a real upstream; self-exclusion
(`:950`) now covers that case. A candidate bound to another species is merely unranked, never excluded.
Ties fall to `relion_job_number` (`:972`), so "most recently allocated" decides between species.

**(b) No stage-order constraint.** TM accepts `TOMOGRAMS_STAR` (`services/jobs/template_match.py:47`)
and produces one (`:57`, a copy of its input for the extraction job). Subtomo extraction accepts and
produces `OPTIMISATION_SET_STAR` (`services/jobs/subtomo_extraction.py:50`, `:57`, `:67`). Any job of
the same or a later stage that produces an accepted type is therefore a candidate, and so is a back-edge.

**(c) Two different resolutions at Start.** Covered in §1: `paths` are resolved while numbers are still
being handed out; edges are resolved after. A job's choice can depend on where it sits in that loop.

**(d) The IO section can show a different default than the resolver uses.** `get_candidates_for_slot`
(`:360`) sorts same-species candidates first with no succeeded gate (`:386–395`), while the resolver
applies the gate. The dropdown can therefore show the right job while Start uses another.

**(e) The cycle message doesn't say which wire is wrong.** `:603` reports the strongly connected set.
It names neither the slot edges nor where to fix them.

**(f) Overrides name job directories, and a failed Start hands out directories it doesn't keep.**
IO-section overrides are stored as `jobtype:External/jobNNN` (`_resolve_override`, `:589`). A refused
Start leaves `relion_job_name` / `job_path_mapping` set on every job but does not commit the allocator
(`:605`). The next Start reuses the numbers only if the job set and its order are unchanged. Adding or
removing a job in between shifts the numbers, so an override can silently point at a different
instance's directory, or at none and fall back to auto-selection. Verify in stage 0; the failure mode
follows from the code but hasn't been observed.

## 3. Decisions (maintainer, 2026-10-05)

Q1 and Q2 answered in the build session. D3 and D5 widened during the build; D7–D10 are the build's own
(D10 from the review pass), stated here so the record shows them.

- **D1 — Stage order is a hard filter for automatic selection.** A producer must sit strictly upstream
  of the consumer in `PIPELINE_ORDER` (`services/jobs/spec.py:239`). Synthetic producers (merge, imported
  tomograms, pick lists) have no stage. User overrides are exempt but cycle-checked (D5).
- **D2 — Species is a hard filter, whatever the status.** For a species-bound consumer, a producer bound
  to a different species is never selected automatically. A same-species producer wins over a
  species-neutral one, pending or not. Producers with no species (tsReconstruct, tsCtf) stay eligible.
- **D3 — Ambiguity is stated, not guessed.** Automatic selection ranks by species match, then the slot's
  preferred source type, then a pipeline job over a synthetic producer (D9), then a roster job over a
  finished one taken off the roster (D10). When the best rank holds two
  or more producer *instances*, whatever their type or species, the slot is ambiguous: amber in the I/O
  section, and Start refuses, naming the slot and the tied jobs. This covers preprocessing A/B copies (two
  tsReconstructs) as well as the species A/B case. Neither job number nor status ranks anything.
- **D4 — Pin on ambiguity (Q1).** Not pin at creation. When adding a job would leave an existing job's
  input ambiguous (the new job ties with that input's current source), the current source is written as
  an explicit override on the existing job, and a toast names each pin. Fresh jobs stay on automatic
  wiring, so there is no override clutter and nothing extra in exported protocols, and old or
  protocol-made projects are covered too.
- **D5 — Overrides name the producer instance, not its directory.** New key form:
  `jobtype:instance:<instance_id>`. The old `jobtype:External/jobNNN` form is still read, and rewritten on
  load through the directory each job holds at that moment. Saving an override that would close a cycle
  is refused in the I/O section, with the cycle spelled out.
- **D6 — One resolution, one snapshot.** At Start, allocate every number first, then resolve `paths` and
  edges from the same resolver. The I/O section's default is the same selection. A cycle error lists the
  slot edges, e.g. `templatematching.input_tomograms ← templatematching__2 (tomograms.star, set by hand)`,
  and points at the I/O section. A refused Start hands every job back its previous directory and writes
  nothing to disk.
- **D7 — A cross-species input is marked in the I/O section only (Q2).** The chosen source carries an
  amber `other species` tag (or `not upstream` for a same- or later-stage job); Start runs it without
  asking. Such sources stay in the menu, dimmed, with the reason.
- **D8 — An override that stops resolving is reported, never replaced.** A deleted job, a stale pick
  list, a removed merge or import: red in the I/O section, and Start refuses. Before, only synthetic
  producers were reported; a job-directory key that matched nothing fell back silently to automatic
  selection (the second half of defect f).
- **D9 — A pipeline job outranks a synthetic producer at equal species and preference.** Otherwise a
  project holding imported tomograms would leave every TM ambiguous between them and tsReconstruct.
- **D10 — A job on the roster outranks a finished one taken off it.** Removing a SUCCEEDED job from the
  roster keeps it in `state.jobs`, so it stays a candidate, and removal drops the pins that name it.
  Without this rank, keeping the winner of an A/B pair and removing the loser would leave its consumers
  ambiguous. `state.pipeline_order` is the roster; when it is empty every job counts as on it.

## 4. Stages

| Stage | Content | Files | Status |
|---|---|---|---|
| 0 | Reproduce on a two-species test project; confirm (f). | — | folded into §5: §1 is the before, §5 steps 1–3 the after |
| 1 | D1–D3, D7, D9, D10: one selection for every caller; menu ordered like it; "ambiguous" in the I/O section. | `services/path_resolution_service.py`, `ui/pipeline_builder/io_config_component.py`, `ui/status_indicator.py` | built 2026-10-05, not run |
| 2 | D6: allocate, resolve, write; cycle message with slot edges; the schemer path refuses too. | `services/path_resolution_service.py`, `services/scheduling_and_orchestration/pipeline_orchestrator_service.py` | built 2026-10-05, not run |
| 3 | D5, D8, D4 (pin on ambiguity). | `services/path_resolution_service.py`, `services/project_state.py`, `ui/pipeline_builder/io_config_component.py`, `ui/pipeline_builder/pipeline_builder_panel.py`, `services/protocols/export.py`, `services/protocols/apply.py`, `services/jobs/_base.py` | built 2026-10-05, not run |

Per-stage file snapshots: `notes/28-stage-snap/s1`, `s2`, `s3` (s3 = the final tree).

## 5. User check (runtime)

1. On a two-species project, add TM → extraction → subtomo for both species without touching any I/O
   section. Each job's I/O shows its own species' upstream, and both TMs read tsReconstruct. The other
   species' jobs are in each menu, dimmed and tagged `other species`; the sibling TM is tagged
   `not upstream`.
2. Start submits. In the afterok chain, both TMs depend only on tsReconstruct, each extraction on its
   own TM, each subtomo job on its own extraction.
3. Add a third TM for one species (an A/B copy). A toast reports the existing extraction's two inputs
   pinned to its TM; its I/O shows them with "Reset to auto". Add an extraction for the new TM: its I/O
   shows amber "Ambiguous — pick a source · 2 candidates" with both TMs named, and a second toast pins the
   existing subtomo job to the existing extraction. Start before picking is refused with
   `Path resolution failed for tmextractcand__3: … ambiguous between …`, and no new `External/jobNNN`
   directory appears. Pick the new TM in both slots; Start submits. Once both TMs have run, take one off
   the roster: inputs pinned to it lose the pin and follow the remaining TM, and nothing is ambiguous
   (D10).
4. Set a deliberate cross-species override: accepted, the face carries the amber `other species` tag,
   Start runs without asking. Point TM A's `input_tomograms` at TM B, then TM B's at TM A: the second is
   refused with a toast spelling out both wires.
5. Pilot `AGG412_20260204_412` after the restart: the server log has `Rewrote N input override(s) in
   AGG412_20260204_412 to name job instances`, every hand-set W18 input shows the same source as before,
   and Start submits.

## 6. Open questions

None. Q1 → D4 (pin on ambiguity), Q2 → D7 (I/O marker only), both 2026-10-05.

## 7. Workaround until the server runs this code

Set every species job's inputs by hand in its **I/O** section: TMs → tsReconstruct, extraction → its own
species' TM (both slots), subtomo → its own species' extraction. Don't add or remove jobs between a
refused Start and the next one (defect f). Overrides set this way are rewritten to instance keys on the
first load by the new code (§5 step 5).

## 8. Build record (2026-10-05, ruff-clean, not run)

### Stage 1 — one selection, stated ties
- `_choose_candidate_for_slot` is replaced by `_auto_select` (`path_resolution_service.py:1130`), which
  returns an `AutoChoice` (a candidate, or the tied producers). The filters are `auto_exclusion` (`:231`),
  public so the I/O section dims what automatic wiring skips. The rank is (species match, preferred
  type, job over synthetic, on the roster); within the one winning producer the curated
  `prefer_if_exists` output still wins over the raw one. The roster component (D10) came from the review
  pass.
- **Deviation:** rather than adding D1–D3 to each of the four callers' copies of the override logic, one
  per-slot primitive `_select_source` (`:703`, returning a `SlotSource`) now serves `resolve_inputs`,
  `resolve_edges`, `validate_input_slot` and `_effective_input`, so the four cannot drift again.
- Pick lists: only the latest extraction per (species, tomogram) competes (`OutputCandidate.auto_eligible`);
  lists on different tomograms tie. Its label reads `[latest on this tomogram]`; the old
  `[auto-selected: …]` would be false when it ties.
- `get_candidates_for_slot` (`:497`) orders the menu like `_auto_select`, skipped sources last; the I/O
  section passes the consumer's species (it passed none before, so species never ordered the menu).
- I/O section (`io_config_component.py`): amber dot and "Ambiguous — pick a source" face with a note
  naming the tied jobs; a red note when an override stopped resolving; menu rows of skipped sources
  dimmed with the reason; the chosen source's amber tag (D7) in `_render_current_face` (`:445`).
- The roster badge tooltip (`ui/status_indicator.py:80`) showed `Path(message).name` of each missing-input
  message, which cut "…in the job's I/O section" down to "O section". It shows the messages whole, escaped.

### Stage 2 — allocate, resolve, write
- `_submit_chain` (`pipeline_orchestrator_service.py:474`) runs three passes. Pass 1 holds every job's
  directory; pass 2 resolves every job's paths and the DAG's wires from one resolver and reports **all**
  unresolved or ambiguous inputs in one message; a refusal in pass 1 or 2 calls `_restore_allocation`
  (`:66`) and nothing is on disk yet. Pass 3 writes the job dirs, job.star and scripts, and commits the
  counter in a `finally`: a failure while writing (an inline Import star, a bad qsub template) used to
  leave the counter uncommitted, so the next Start could reuse a number already written to disk.
- `resolve_slot_edges` (`path_resolution_service.py:394`) returns `SlotEdge`s; `resolve_edges` is their
  deduplicated pairs, so `_parked_behind_review` and `submit_parked` are unchanged. A loop is reported
  through `find_cycle` (`:139`): `These inputs form a loop, so the chain cannot be ordered: <wires>.
  Change one of them in its job's I/O section.`
- The schemer path (`_deploy_locked`, non-afterok) keeps its per-job loop: selection no longer reads
  job numbers, so resolving mid-allocation gives the same answer, and that path builds no DAG. It used
  to launch a scheme that listed a job whose inputs did not resolve (no job.star, so the scheme stopped
  there); it now refuses with the same message as the afterok path and hands back the predicted dirs
  (`:377`). From the review pass: D3 and D8 make such failures reachable on a two-species project.

### Stage 3 — instance keys, reported dangling overrides, pin on ambiguity
- `OutputCandidate.source_key` (`:58`) is `jobtype:instance:<id>` for jobs; synthetic keys are unchanged
  (merge, imported tomograms, pick lists keep their instance-path keys, so `apply_aggregation_overrides`,
  `species_references` and the pick-list slug migration are untouched). `_resolve_override` reads both.
- `migrate_override_keys` (`:1295`), called from `ProjectState.load`, rewrites job-directory and
  `pending_` keys through the directory each job holds at load and logs the count. It runs in drivers'
  loads too, where it only changes the in-memory copy.
- D8 in `_select_source` with `_dangling_job_message` (`:748`).
- Cycle check on save: `cycle_with_override` (`:447`) evaluates the new key against every job's wires and
  restores the overrides; `_handle_source_change` refuses with a toast naming the loop.
- D4: `add_instance_to_pipeline` takes `automatic_sources()` (`:473`) before the addition and calls
  `pin_sources_made_ambiguous` (`:1334`) after it (`pipeline_builder_panel.py:331`, `:404`).
- Removing a job from the roster also drops instance keys naming it (`_cleanup_stale_overrides_for_instance`,
  `pipeline_builder_panel.py:436`).
- Protocol export reads instance keys (`services/protocols/export.py`); apply still reports explicit
  wiring as not applied (comment corrected: the reason is the instance-id mismatch below, not the key form).

### Found along the way (not changed)
- Two refiners of one input (two missAlign runs of one alignment) still go to the most recently deployed,
  by job number, with only a log warning (`_follow_refinements`) — the one place left where a job number
  decides between instances.
- Species come from the job model's `species_id`; a legacy job bound only by its instance-id suffix counts
  as species-neutral.
- `project_service.delete_job` (the species-delete cascade) leaves overrides naming the deleted jobs;
  under D8 they now show as "no longer in the project" instead of falling back silently.
- Protocol export writes a stage's `inputs` as the source project's instance ids (`templatematching__2`),
  while apply creates `{job}__{species}` ids; apply can honour explicit wiring only after a mapping.
- The read-only I/O of a finished job re-resolves instead of showing the paths it ran with. Pins keep
  that display stable for additions from now on; an older project with an A/B pair and no overrides shows
  its finished jobs amber.

### Older code reading a project saved by this code
Code without this change reads a `jobtype:instance:<id>` key as matching nothing and falls back silently
to its job-number selection, losing the hand-picked source. Once a server on this code has saved a
project, don't run that project with older code (a revert, or the lab install at
`/groups/klumpe/software/crboost_server` before main has this change).

### Review pass (2026-10-05)
An independent read-through of the diff found no runtime defects (call signatures, dataclass fields,
imports, the overrides dict mutated in place, no `await` between allocation and writing). It walked the
pilot's six picking jobs through the new code: every input resolves to the intended job, with no tie.
Its two logic findings are fixed above (D10, the schemer refusal); its third is the compatibility note.

## Not in scope

The cross-project species catalog (roadmap 12, picking_ui/07), job-dir ownership (15), and the
orchestrator replacement.

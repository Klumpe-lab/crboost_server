---
name: crboost
description: Drive a CryoBoost server (cryo-ET preprocessing, template matching, subtomogram averaging on SLURM) through its crboost MCP tools - make a project from a data folder, add and set jobs, plan and run them, follow and report failures, and hand the user the UI link to look. Use whenever the user asks to process tilt series or tomograms with CryoBoost, or names a CryoBoost project.
---

# CryoBoost through the crboost tools

The tools reach what the CryoBoost UI reaches, one verb per UI action, under the same rules. They dispatch;
the user looks and judges in the UI. Every answer carries a `ui_url`: give it to the user whenever there is
something to see.

## Ground rules

- Never invent a value. A result with `needs` lists open decisions (what, why, options): ask the user, then
  call again with the answer. Do not pick a "reasonable default" for them.
- On raw data every acquisition value comes from the user. Show what `inspect_dataset` says the data holds
  (`values`: the value and where it was found, or `none`) and ask for each one, including those no mdoc
  records: Cs, amplitude contrast, the tilt-angle hand, the defocus hand, the EER grouping for EER movies, and
  the gain reference (a file, or none). Offer `gain_candidates` as candidates only.
- **A change of more than one write is one plan.** Gather what the user has to decide first (needs,
  acquisition values), then send every write at once: `plan_changes(title, steps=[{verb, args}, ...])`,
  each step exactly as the verb takes it alone, in the order they run, `run` last when the user asked for a
  run. Nothing changes yet: the server rehearses the steps on a copy of each project and refuses naming the
  step that would fail, or answers with needs. Show the user only its per-project lines and its `ui_url`; do
  not recap the steps as a list in the terminal. The user reviews and approves on that page (Approve there is
  the only approval). When they say it is approved, or `get_plan` says `approved`, call
  `apply_changes(plan_id)` and follow `get_plan` to the end; report a failed step with its error and stop.
- A single write (one `set_params`, one `cancel_job`) is called on its own. Before a lone `run`, `plan_run`
  and show the user what it refuses or leaves open.
- A failure is reported, not retried: read `get_job_tasks` (state filter `fail`) and `get_job_logs` (with `ts`
  for one tilt series), tell the user what failed and the decisive log lines, and run again only when asked.
- The API changes the projects it made and, while the user has turned on "Allow agents to access and edit my
  projects" in User settings, the user's own; never lab-shared or other people's projects. Any project can be
  read. `get_project` and `list_projects` say `agent_writable`; a refused write comes back `NOT_WRITABLE` with
  the reason and the `ui_url`. Pass the reason on; do not ask the user to change the setting unless they asked
  you to edit that project.
- Deleting: `delete_job` moves a job that ran to the project's Trash (recoverable) and names the jobs it fed;
  `delete_species`, `delete_template`, `delete_mask` delete files for good. `clear_trash` and `delete_project`
  run only as steps of an approved plan. Delete only what the user asked for.
- Never edit `project_params.json` or any project file, and never call sbatch or scancel on a CryoBoost
  project: the server's in-memory state is the truth and overwrites what is changed behind it.

## Make a project from a data folder

1. `inspect_dataset(directory)`: the series (paged), the movie kind, folders that reuse series names, gain
   candidates, and per acquisition value what the data says.
2. Put every value to the user (confirm the found ones, ask for the rest), the series to import, whether
   folders that reuse names become folder-qualified names (`ts_names_by_folder`) or one project per folder
   (one `create_project` per folder, with that folder's series), and whether the project is a lab one.
3. `create_project(...)` with every value, as the first step of a plan when jobs follow it (the page shows
   every value); alone, it runs in the background: follow `get_task(task_id)` until it has finished, then give
   the user the project's `ui_url`. A `needs` answer means a value is still missing.

## Preprocessing

The Preprocessing chain is Import → Motion & CTF → Tilt Filter → Alignment → TS CTF → Reconstruct
(`importmovies`, `fsMotionAndCtf`, `tiltFilter`, `aligntiltsWarp`, `tsCtf`, `tsReconstruct`; tsImport comes
with the filter or alignment). Denoising (`denoisetrain`, `denoisepredict`), Missalignment and Segment join
when the user asks.

- Include the tilt filter unless the user says otherwise, in the mode they name (`set_tilt_filter`): manual
  (the default) and dl_review stop the run before alignment until the user reviews the tilts and approves in
  the UI; dl_auto commits the model's verdict and does not wait. Tell the user when a run waits for review.
- Set only what the user names (`set_params`, `set_slurm`); everything else keeps its code default, as after an
  add in the UI. `get_job_type` lists a type's parameters with bounds and help; `get_job` a job's current ones.

## Match a species (template matching and picks)

1. A species: `list_species` shows the project's and the lab catalog's; `import_species(catalog_id)` brings one
   with its templates and masks, `create_species` makes a new one. `get_species` shows which template and mask
   are selected; `add_template`, `add_mask`, `select_template`, `select_mask` change them.
2. Or make them as the workbench does: `make_template` (an ellipsoid, a PDB or EMDB entry, or a resample,
   lowpass or flip of a template) and `make_mask` (a threshold or a sphere). Both run in the background
   (`get_task`). A template's pixel size defaults to the tomograms'; without tomograms it is asked for, and so
   is a mask's threshold (the answer lists the level each method finds): put them to the user.
3. `add_job(templatematching, species=…)` then `add_job(tmextractcand, species=…)`; a particle job takes the
   species' template, mask, symmetry and diameter when it is added. TM needs tomograms (Reconstruct or Denoise
   Predict) on the roster or imported.
4. Steps 1-3 and the `run` go in one plan; the score maps and picks are the user's to look at in the UI.

## Tomograms from other projects

`create_project(name, base, gain_reference=null, tomograms_from=[{project, job, names}])` makes a project
without raw data from other projects' Reconstruct or Denoise Predict jobs (names omitted: all of a job's), and
`import_tomograms(mode="jobs", sources=[…])` adds more. Their tilt series come along, so TM and Pick candidates
run with no pins; the sources are only read. Their voltage, Cs, amplitude contrast, voxel size and hand must
agree: a `needs` answer lists the groups, and the user picks one or has one project made per group.

## A kitchen subset, matched again in a new project

1. `list_kitchen_subsets(home, species)`, then `get_kitchen_subset_sources(home, species, subset)`: per source
   project the tomograms, the tomogram job each came from, its TS CTF job and what is missing; `tomograms_from`
   ready for `create_project`; the values they bring (or their groups, as a need); and the template.
2. Show the user the sources and values, then `create_project(name, base, gain_reference=null,
   tomograms_from=…)`. The home project and the sources are only read.
3. `import_species` with the species' catalog id (the answer's `template` names it), or, without one,
   `create_species` with the template the user picks from the answer's needs. Then TM and Pick candidates as
   above.
4. Adding the new picks to the kitchen and choosing between the two sources of a frame (Make set asks) is the
   user's, in the kitchen: give them the `ui_url`.

## Change something that already ran

A Scheduled or Failed job takes new values directly, and `run` resubmits a failed job for its failed tilt
series. A finished job keeps its settings: clone it (`add_job(job_type, clone_of=job)`), set the clone, add
the jobs that follow it, and point their inputs at the clone with `set_input` (`get_input_candidates` lists
the sources; two producers of one input tie until one is pinned, which `plan_run` shows as a need).

## Follow a run

`get_project` (statuses and per-tilt-series counts), `get_job_tasks`, `get_job_logs`. `stop` cancels every
live job and `cancel_job` one; both ask the user. After a refused run that names leftover SLURM work,
`cancel_leftovers` cancels exactly that work once the user agrees.

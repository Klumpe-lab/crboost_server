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
  step that would fail, or answers with needs.
  - **Confirm in the terminal (the default).** Call `apply_changes_now(plan_id, revision)` straight away: the
    crboost server opens a dialog in the terminal with the whole plan, a table per project (its values, the jobs
    it adds, every setting now → after, deletions, the run), and applies it only on their Accept. You cannot
    answer it for them. Edits are made on the page. Don't print the plan yourself first; the dialog is it. If the answer says the dialog could not be
    shown, print `overview` exactly as it is and give the page (`ui_url`). If they decline, ask what to change
    and plan again.
  - **On the page.** A plan that deletes something for good (`permanent` is not empty) is approved on its page
    (`ui_url`), and so is any plan the user prefers to read or edit there: give the link and wait. The page can
    change parameters, SLURM values and drop deletions; each edit moves the plan's `revision`. When the user
    says it is approved, call `get_plan`; if the revision moved since you showed it, print the new `overview`
    first, then `apply_changes(plan_id, revision)`.
  - Follow `get_plan` to the end; report a failed step with its error and stop.
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
  run only as steps of a plan approved on its page. Delete only what the user asked for. `restore_from_trash`
  puts trashed jobs back on the roster (a taken instance id comes back as the next free one).
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

## Picks from a segmentation

Pick candidates also picks a species from one feature's segmentations (a Segment job's, or imported ones), with
no template and on CPUs only. Its `source` is `segmentation`.

1. `add_job(tmextractcand, species=…, reading={"input_segmentations": <source>})`, the source as
   `get_input_candidates` gives it: a job made reading segmentations and no template-matching output starts with
   the segmentation source. Or `set_params(source="segmentation")` on one that has not run.
2. The `feature` is the species' segmentation feature when the species names one (`get_species`). Otherwise put
   the segmentations' features to the user (Segment jobs name theirs `<feature>_ais`, imports keep theirs);
   never choose one by its name. Run refuses the job without one.
3. Detection: `p_threshold` (a voxel is the feature where p ≥ it), `min_piece` (pieces smaller than this fraction
   of a sphere of the diameter are left out), `one_per_piece` (one pick at each piece's centre, instead of the
   deepest points a diameter apart), `binning`. `orientation` (random, long axis, short axis; the axes only with
   one pick per piece) sets the starting angles: random ones are uniform over all orientations, so the first
   refinement of these picks searches globally. Set only what the user names.
4. Each pick carries its score (`rlnAutopickFigureOfMerit`, the mean p near it), its piece's volume and pick
   count, where its angles came from (`cbAngleSource`), and `cbMarkReason` `edge` within half a diameter of the
   volume's edge. Judging them is the user's, in the UI.

**Context, from either source.** Every pick also records what the job's segmentations input says around it:
per feature, `cbP_<feature>` (the mean p inside the particle) and `cbDistAngst_<feature>` (the distance to the
feature's surface, negative inside), e.g. `cbP_nucleus_ais` for nuclear ribosomes. The segmentation source takes
it from the features it does not pick. A template-matching job reads segmentations while its `context` is on (the
default for new jobs) and records none when the project has none. Run refuses segmentations of another
reconstruction than its picks, naming both: put that to the user, who chooses others (`set_input`) or turns
`context` off.

## Tomograms from other projects

`create_project(name, base, gain_reference=null, tomograms_from=[{project, job, names}])` makes a project
without raw data from other projects' Reconstruct or Denoise Predict jobs (names omitted: all of a job's), and
`import_tomograms(mode="jobs", sources=[…])` adds more. Their tilt series come along, so TM and Pick candidates
run with no pins; the sources are only read. Their voltage, Cs, amplitude contrast, voxel size and hand must
agree: a `needs` answer lists the groups, and the user picks one or has one project made per group.

## Sets in the STA kitchen

A species' kitchen in a home project pools picks from any projects (read, never written); subsets name groups
of its picks; Make set turns what a recipe keeps into a particle set of the home project that particle jobs read
(`set_input` on their particle input).

1. `list_kitchen_sources(home, species)`: per project its template-match picks (Pick candidates jobs) and pick
   lists of the species with the same catalog id. `add_kitchen_sources(home, species, sources=[{project, job}])`
   makes the kitchen when it has none and builds its table.
2. `get_kitchen(home, species)`: sources, subsets, groups, the sets made, the columns a recipe filters on (number
   columns like `sigma`, categories like `project` with their values), picks per project. Give it a recipe to
   see what it keeps, per project.
3. A recipe is `filters` (`{kind: range, col, lo, hi}`, `{kind: category, col, allowed}`, `{kind: set, set_id,
   mode}`), `in_subsets` / `not_in_subsets`, and `min_distance_a` / `top_n_per_tomogram`. The hand verdicts
   apply as in the kitchen; the user's filter strip is never changed.
4. `create_kitchen_group` and `create_kitchen_subset(name, group, recipe)` (a snapshot of what the recipe keeps
   now; in a group a pick sits in one subset only). `plan_kitchen_set` shows a Make set without writing;
   `make_kitchen_set` (from a recipe, a `subset`, or one set per subset of a `group`) runs in the background:
   `get_task`.
5. Every recipe, subset and set comes from the user's words (which σ, which projects, which classes): never
   pick a cut for them. Several kitchen writes are one plan, like any other.
6. `make_class_set(project, job, classes, name)`: a set of a Class 3D's classes, as its Results tab makes it.
   Which classes to keep is the user's call, from what they saw.
7. Filaments: after a helical Refine 3D, `get_polarity(project, job)` says which way each filament points by its
   segments' vote, and whether the vote means anything (`pieces`: about half agree when the map cannot tell a
   filament's ends apart). Show it; `make_polarity_set(project, job, name)` makes the set the next Refine 3D
   reads with the directions held, on the user's word.
8. Sub-particles (capsomers of a capsid, the monomers of a filament): after a Refine 3D or an M refine,
   `get_subparticles(project, job)` says whether its particles hold copies of a unit, the axes of its point group
   a unit can sit on (or a helical refinement's own twist and rise) and the species a sub-unit can be. The
   sub-unit is a species of its own (`create_species` when it is missing: its symmetry and Ø are what jobs on the
   set start from). Where the unit sits (`axis` and `radius_ang`; for a filament `twist_deg`, `rise_ang`,
   `radius_ang`, and `axial_repeat_ang` when the lattice has several starts) is the user's, read off their map:
   never fill one in. `plan_subparticle_set` counts without writing; `make_subparticle_set(project, job, species,
   name, …)` writes a coordinates set, which a Warp Export or a Reconstruct Particle cuts anew.
   An expansion keeps the images instead: `plan_expansion_set` / `make_expansion_set(project, job, name, …)` hold
   each particle in every pose its symmetry makes equal (a point group's operators; for filament segments the
   registrations of their lattice, `twist_deg`, `rise_ang`, `axial_repeat_ang` and `units_per_repeat`, all the
   user's). A Class 3D with Skip alignment on that set scores every copy against one reference.
9. The seam and register of filaments: over such a Class 3D of a lattice's registrations (First iteration CC
   off, so the scores are log-likelihoods), `get_seam(project, job, radius_ang)` says which registration each
   piece of filament is on and whether the vote means anything: `checks.blocks` (blocks of a piece that share no
   data against blocks of other filaments) and `checks.by_length` (the share of sites a piece's two halves label
   alike, a half when nothing is known, by the least length kept). Show it; `radius_ang` (where the units sit
   from the axis) and the length cut `min_segments` are the user's. `make_seam_set(project, job, name,
   radius_ang, min_segments, held_out)` writes the segments on their registration; `held_out` (the default)
   gives each segment the registration the rest of its filament voted, so an average of the set shows whether
   the units are told apart without the segments' own noise in it. The units of the true repeat (a tubulin
   dimer) are then a sub-particle set of that set: `make_subparticle_set(project, job, species, name,
   from_set=<the set>, twist_deg, rise_ang, radius_ang, axial_repeat_ang=<the true repeat>, azimuth_deg,
   axial_ang)`, the site read in the scoring reference's frame; its helix covers one turn and wraps opposite
   `azimuth_deg`, which is where the seam must be.

Verdicts on single picks, the review sample, and the choice between two reconstructions or sources of one tilt
series stay in the kitchen: `get_kitchen` says when such a choice stops Make set; give the user the `ui_url`.

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
live job and `cancel_job` one; both ask the user. `stop_after_iteration` ends a running Refine 3D or Class 3D
once its current iteration has landed, so run again it goes on from there; it asks the user too. After a refused run that names leftover SLURM work,
`cancel_leftovers` cancels exactly that work once the user agrees.

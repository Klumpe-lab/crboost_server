# Roadmap 42 — Agent API: talk to your data from a Claude Code session

**Status:** scoped 2026-11-04, no code. Reframed the same day after the maintainer's correction: the API reaches what
the UI reaches, action by action (D2); it is not a set of recipes. The anchors in §0.1–§0.12 come from four code reads
and a check of the Claude Code docs at `70ce4db`; the rest, and the UI inventory behind §4.3, from reads at `273f13d`.
Every decision is made (§8), the last of them on 2026-11-05. Part A (§3) stands on its own: it fixes today's multi-tab
roster whether or not an agent ever drives anything. **Pickup: S1, in its own worktree (§9).**

**Ask (maintainer, 2026-11-04).** "A really differentiated API so I can just talk to my data." From a Claude Code
session in a terminal:
- make a project from **any data folder** the landing page can scan and create from;
- put **any combination of jobs** the UI allows into it, with **any parameters**, and run them;
- then come into the UI and **look at the pictures and graphs**, as in a project built by hand.

Things one would say:
- **U1** "initiate the data in dir X to a project and run it through preprocessing with reasonable/default params";
- **U2** "take subset Y of species Z globally and run it through template matching / picks". Y is an STA-kitchen
  subset, matched again in a new project (decided, §7);
- **U3** and then look at the results in the UI, and carry on there.

**In one line.** One verb for each thing the UI lets you do to compose, set up and run a project. Each verb goes through
the same service function as its button, under the same rules. Any client with the token can call them (a script, curl);
Claude Code calls them through a stdio MCP forwarder. No chat or model runs inside the server. The API changes only
projects it made, and looking and judging stay in the UI.

## 0. What is there today (S0 facts, 2026-11-04)

1. **Nothing over HTTP can change anything.**
   - `/api/tilt-thumb`, `/api/vis-asset` and `/api/project-preview` (`server_app.py:63-125`), and `/api/file`
     (`ui/template_workbench.py:120`), only serve files.
   - `TokenGate` (`services/server_auth.py:63`) takes `?token=` or the per-port cookie. It has no header form.
2. **Creating a project.**
   - Landing → New project (`handle_create_project`, `ui/data_import_panel.py:726`) → `create_project_and_scheme`
     (`backend.py:2162`) → `ProjectService.initialize_new_project` (`project_service.py:464`). This makes a project with
     **no jobs**; they are added from the roster.
   - The form has three inputs: name, dose and gain (`data_import_panel.py:1326, 1464, 1513`). Pixel size, voltage and
     tilt axis come from the first value the mdocs give (`_import_payload`, `:872`), else from the code defaults. Cs and
     amplitude contrast always come from the code. None of them can be typed, at creation or later.
   - Much of what Create needs lives only in `ui/data_import_panel.py`:
     - the directory turned into a glob (`ui/glob_directory_input.py:173`), the mdocs found beside the frames or
       elsewhere (`:655-665`, `:1405`), the `*/` retry (`:279-289`), and the frame kind the scan switches to (`:1088`);
     - series selection (`ui/dataset_overview_panel.py`), and the folder split for folders that reuse series names:
       folder-qualified names, or one project per folder made in a UI loop (`ui/dataset_split_panel.py:33`, `:758-791`);
     - the dose row, the "shared" switch (owner `@lab`, `project_service.py:508`), and "no raw data" (`:313`, `:673`);
     - the refusal of a half-filled form (`get_missing_requirements`, `:322`); without it the backend skips the import
       and reports success;
     - the gain reference, which is dropped when Create runs without a scan (`:797-799`).
   - The scan itself is a backend call: `parse_dataset_overview` (`backend.py:2278`) → `DatasetOverview`
     (`services/tilt_series/preimport.py:217`).
3. **Acquisition values default silently** (`services/models_base.py:314-341`).
   - The defaults: pixel size 1.35 Å, 300 kV, Cs 2.7 mm, amplitude contrast 0.10, tilt axis −95°, detector 4096².
   - A project takes them without a word when the mdocs lack the value. Only the dose is guarded: there must be a value
     and a recorded source, or creation refuses (`project_service.py:550-582`).
   - The scan bar shows chips only for values it found, so a defaulted pixel size looks like a detected one.
   - Detected `invert_tilt_angles` and EER fractions are never applied.
4. **Adding a job is UI code, and its rules are lenient.**
   - `PipelineBuilderPanel.add_instance_to_pipeline` (`ui/pipeline_builder/pipeline_builder_panel.py:359-536`) does
     all of it: the single instance of an interactive job (`:381`, `:653`), the instance id (`next_instance_id`,
     `pipeline_constants.py:49`), the species requirement (`:404`), the tsImport prerequisite (`:633`), the copy of the
     species' values (`:433-488`), the tilt filter's saved labels (`:666`), clone and merge overrides, input pins,
     `pipeline_order`, and the save.
   - Missing dependencies are only a warning in the roster (`missing_deps`, `pipeline_constants.py:31`); protocols
     refuse on them (`services/protocols/apply.py:79`). Only Subset has a refusal in services (`add_refusal`,
     `services/subsets.py:66`).
   - The roster, the species Jobs tab, Subset's "Add <job>" and the I/O section's "+ Subtomo / + Warp export"
     (`add_job_reading`, `:538`) all add jobs.
   - `protocols.apply` keeps a private, partial copy of the species step (`_apply_species_defaults`, `apply.py:359`)
     that lacks the Warp-export branch.
5. **Setting a parameter is widget code.** There is no service function for "set X of job Y to V".
   - `AbstractJobParams.__setattr__` (`services/jobs/_base.py:418-451`) drops a write to a user parameter on a job that
     is not Scheduled or Failed, leaving only an INFO log line.
   - There is no `validate_assignment`. Bounds are checked only in `numeric_forward` (`ui/job_plugins/_field_styles.py:287`).
   - SLURM's "write only if changed" lives in `slurm_tab.py:203-215`. `apply_slurm_preset` (`_base.py:210`) exists, but
     the UI never calls it.
6. **Run is sound, and runs the whole roster.**
   - `deploy_and_run_scheme` holds a per-project lock (`pipeline_orchestrator_service.py:112-122`).
   - `_submit_chain` (`:398`) refuses with its reason: `SET_CHECKS`, `still_live`, ambiguous inputs, or a cycle. On
     success it returns the SLURM ids. Two Runs at once submit once.
   - Run submits the tab's whole roster; the UI has no "run these". It resubmits a failed job for its failed tilt series
     only (`:84`, about `:490-497`), in a fresh folder when its inputs moved (`:99`). A job that finished cannot run
     again: it is cloned, or deleted and added again.
   - Stop and Cancel live on `pipeline_runner` (`stop_and_cleanup` `:632`, `cancel_job` `:661`) with no backend method.
   - `get_job_logs` takes a job directory name, not an instance id (`:524`).
7. **The roster belongs to the browser tab.**
   - `UIState.selected_jobs` (`ui/ui_state.py:70`) lives in `app.storage.tab` (`:586`). It is filled from
     `state.roster()` once, when the tab opens the project (`:450-464`).
   - So a job added by anything else gets no row in that tab.
   - That tab's next add writes `state.pipeline_order = list(selected_jobs)` (`pipeline_builder_panel.py:386, 425`),
     which drops the job.
   - Its Run submits its own list (`:763`).
   - Two tabs on one project already diverge this way today.
8. **Some fields copy their value instead of binding to it.**
   - Most parameter fields bind to the job model, so a change made elsewhere shows within 0.1 s.
   - **Species-derived choice rows** (template, mask, symmetry; `_field_styles.py:459-521`) keep the value they were
     drawn with.
   - **SLURM fields** are copies taken at render time. Tabbing through a stale one writes its old text back as an
     override (`slurm_tab.py:203-209`).
   - **The I/O section** does not poll.
   - **Run/Stop buttons:** after a Stop, the tab's status poller is not restarted (`pipeline_builder_panel.py:890-898`).
     A later Run from somewhere else leaves those buttons stale.
9. **Status needs no tab.** `PipelineMonitor` reconciles every loaded project with `pipeline_active` set every 3 s
   (`docs/architecture.md`). After a restart it reloads only the projects under `default_project_base`.
10. **The tilt filter can hold the chain.**
    - Three modes (`services/jobs/tilt_filter.py:20-26`). Manual (the default, `:121`) and DL review wait for someone's
      Approve. DL auto runs the model as a chain job that commits its own verdict.
    - While a review is open, Run holds alignment and everything after it; the rest runs. Approve submits the held jobs
      (`pipeline_orchestrator_service.py:174-190`, `:238`).
    - Its `USER_PARAMS` is empty (`:99`). The mode has its own setter (`backend.set_tilt_filter_mode`, `backend.py:498`);
      model and threshold are written by the widgets (`ui/pipeline_builder/tilt_filter_row.py:378, 387`).
11. **Kitchen subsets.**
    - A subset is a named list of pool uids kept in the home project (`services/sta_kitchen/service.py:436`).
    - Each uid's row names its frame (project, tomogram, reconstruction; `pool.py:95-108`) and its member (project,
      local species id, the Pick candidates instance or pick list it came from; `models.py:48-60`).
    - Make set's `plan` already groups a selection by member and tomogram (`services/sta_kitchen/export.py:118`,
      `:167-200`).
    - `members.offered_members` / `matching_species` (`members.py:72-79`) and `service.add_members` (`service.py:303`)
      put a project's job into the pool.
    - Nothing runs a job across projects.
12. **In-memory state is the truth.**
    - The process holds one `ProjectState` per path (`project_state.py:1515-1555`). Disk is read only when nothing is
      loaded yet.
    - A second process that edits a loaded project is overwritten by the server's next save. So the API has to live
      inside the server.
13. **Tomograms can come into a project without raw data, but TM cannot run on them.**
    - `services/tomogram_import.py` writes the project's `Tomograms/tomograms.star`, either from `.mrc` files or by
      referencing an existing `tomograms.star`, and records it on `ProjectState.imported_tomograms`
      (`project_state.py:657`). The resolver offers that star as a tomograms producer
      (`path_resolution_service.py:1194`).
    - TM also reads a TS CTF tilt-series star (`input_tiltseries`, `services/jobs/template_match.py:69`) and stages
      each tilt series' star from it (`drivers/template_match_pytom.py:305-337`). An import brings none, as the
      resolver's own comment says (`path_resolution_service.py:1197-1203`).
    - TM takes voltage, Cs and amplitude contrast from the project, not from the tomogram
      (`template_match_pytom.py:238-240`).
14. **The job table knows the phases.**
    - `JOB_SPECS` (`services/jobs/spec.py:54-79`) gives each job type its roster phase, the types that must be in the
      roster before it, and the prerequisite added with it.
    - Preprocessing holds Import, Motion & CTF, Tilt Filter, Alignment, Missalignment, TS CTF, Reconstruct, Denoise
      Train, Denoise Predict, Segment and Subset. tsImport is added with Tilt Filter or Alignment.
15. **Nothing records how a project was made.** `created_by` and `owner` (`project_state.py:738-743`) say who made it,
    not whether by hand.
16. **Many UI writes skip dirty tracking** and force a save after themselves: input overrides, facet choices, submitted
    inputs, the gain reference, tilt labels, and the tilt filter's model and threshold.

## 1. How it connects

```
 terminal on the headnode                            crboost server (one process, runs as you)
 ┌─────────────────────────────┐                    ┌────────────────────────────────────────────────────┐
 │ claude                      │                    │ TokenGate: ?token= · cookie · Authorization: Bearer │
 │  └ crboost MCP (stdio)      │── HTTP + Bearer ──►│   POST /api/v1/<verb> ──► api/ verb table           │
 │     agent/crboost_mcp.py    │  127.0.0.1:<port>  │                              │                      │
 │     · finds your server     │                    │                              ▼ the same service      │
 │     · checks the port is    │                    │   services: pipeline_edit · job_params ·  doors as   │
 │       yours (uid)           │                    │             project_create · deploy_and_run  the     │
 │     · reads ~/.crboost/token│                    │                              │           buttons    │
 └─────────────────────────────┘                    │                              ▼                      │
                                                    │   _project_states: one ProjectState per project     │
 browser tab(s) ── cookie ── NiceGUI ───────────────│──► roster, job pages, Journey read the same objects │
                                                    │                              │                      │
                                                    └──────────────────────────────┼──────────────────────┘
                                                                                   ▼ sbatch afterok chain
                                                                                 SLURM
```

Talking to your data:
```
 you:    make a project from /groups/…/run12; align with AreTomo at 200 nm, reconstruct at 10 Å, denoise,
         and match ribosomes from the lab catalog at 7°
 agent:  inspect_dataset(dir)   → 41 tilt series, EER. The mdocs give 0.93 Å, 300 kV, dose 3.1, tilt axis −95.2;
                                   not given: Cs, amplitude contrast, sample thickness, hands, EER fractions.
         ← asks you for those, and for the gain reference                                        (D3)
         create_project(…, every value)                → Claude Code shows the call; you approve → ui_url
         list_job_types · get_job_type(aligntiltsWarp) → the parameters: value, bounds, choices, help
         add_job importmovies, fsMotionAndCtf, tiltFilter, aligntiltsWarp, tsCtf, tsReconstruct,
                 denoisetrain, denoisepredict                                                    (D4: filter in)
         set_params aligntiltsWarp {alignment_method: AreTomo, sample_thickness_nm: 200}
         set_params tsReconstruct {rescale_angpixs: 10}
         import_species(ribosome) · add_job templatematching, tmextractcand (species ribosome)
         set_params templatematching {angular_search: "7"}
         plan_run               → 10 jobs in submit order; alignment and the 6 after it wait for your tilt review
         run                    → Claude Code asks you → SLURM ids
 you:    open the ui_url: review the tilts and approve; later the tomograms, the denoised ones, the score maps
```

## 2. The contract: rules for every verb

1. **The UI's reach, no more.** Anything the UI lets you do to compose, set up or run a project, the API can do, and
   nothing the UI refuses. §4.3 is the list of record. The rules live in the doors (Part A), so both paths share them:
   a UI warning (a missing dependency) comes back as a `warning`, and a UI refusal as an `err`.
2. **One door per action.**
   - A verb calls the service function that the matching button calls.
   - Where that logic sits in `ui/` today, it moves to `services/` first (Part A), and the button calls it from there
     too.
   - Nothing is implemented twice.
3. **Looking and judging stay in the UI.** The API dispatches. It does not review tilts, curate picks or judge
   particles (§4.3 lists what stays). Every answer carries the `ui_url` of what it touched.
4. **Nothing is invented.** A value the call needs that nobody gave comes back as a `needs` item (what is missing, why,
   and the options), and the call refuses until it is given. Examples: an acquisition value for raw data (every one
   comes from you, D3), an input two producers could feed, a species with no template. The agent puts the question to
   you. This is CLAUDE.md *Surfacing uncertainty*, written as an API contract.
5. **Plan before acting.**
   - Reads and plans (`get_*`, `list_*`, `inspect_*`, `plan_*`) never change anything. Writes are separate verbs.
   - `run` submits only what `plan_run` shows.
6. **Failures are reported, not retried.** The agent reads the log and tells you what failed. It re-runs only when you
   ask (as in `feedback_no_cluster_workaround_automation`).
7. **Results are `ok()` / `err()`** (`services/result.py`).
   - Each result also carries `needs`, `warnings` and a `ui_url` for whatever the call touched.
   - `code` appears only where the agent has to branch. New `ErrorCode` members: `NEEDS_DECISION`, `JOB_FROZEN`,
     `NOT_WRITABLE`.
8. **Small answers.** Claude Code caps an MCP result at 25 000 tokens by default (`MAX_MCP_OUTPUT_TOKENS`) and saves
   anything longer to a file. Summaries carry counts; per-tilt-series detail has its own paged verb.
9. **Quick answers.**
   - Anything slower than a few seconds, such as an import or a template simulation, runs as a `BackgroundTask`
     (`services/background_tasks.py`), and the verb returns its id.
   - The task also shows in the tray of any tab on that project. The tray matches the resolved path string, so verbs
     always use the resolved path.
10. **The event loop stays free** (`docs/architecture.md`, *Where work runs*).
11. **Writes need the header, not the cookie.**
    - The per-port cookie rides along on any same-site request, and every `localhost` port counts as the same site.
    - So mutating verbs accept only `Authorization: Bearer`. Reads take either.
12. **The API changes only projects it made** (D7).
    - `create_project` marks the project (`ProjectState.made_by_api`, saved in `project_params.json`). It refuses a
      directory that already exists.
    - Every write verb checks the mark before its handler runs. A project made by hand comes back as `NOT_WRITABLE`,
      with its `ui_url`.
    - Reads work on any project.
    - In the UI, a project the API made is a project like any other.
13. **Every write is recorded.**
    - One console event: `events.info("%s · agent: %s", project, what)`.
    - One line in the project's `Logs/agent_api.jsonl` (D9).

## 3. Part A — one door per action, and a UI that shows changes from anywhere (prerequisite)

### 3.1 The roster belongs to the project
- `state.pipeline_order` (read through `state.roster()`) becomes the only roster.
- `UIState.selected_jobs` goes away. Its readers move to the project:
  - `get_instances_for_type` (`ui_state.py:323`)
  - `set_active_instance` (`:388`)
  - the I/O section's producer list (`io_tab.py:28`)
  - Run (`pipeline_builder_panel.py:763`)
- Adding and removing a job write `pipeline_order` through the door in §3.2.
- The roster's `signature()` includes `tuple(state.pipeline_order)`. A row then appears or disappears on the next 3 s
  tick, and an open job page whose job has left the roster closes.
- `StatusPoller` follows `pipeline_active` in both directions, including after a Stop.
- Behaviour change (its own commit): two tabs on one project show one roster.

### 3.2 One door for pipeline edits: `services/pipeline_edit.py`
- **`add_job(state, job_type, *, species_id=None, after=None, reading=None, clone_of=None)`** returns
  `ok(instance_id, added, pins, warnings)` or `err(..., code=...)`. In order, it:
  1. checks Subset's refusals (`add_refusal`) and the single instance of an interactive job;
  2. picks the id with `next_instance_id` (moves from `ui/pipeline_builder/pipeline_constants.py:49`);
  3. requires a species where the job type needs one;
  4. puts missing dependencies in `warnings`, as the roster does (`missing_deps` moves from `pipeline_constants.py:31`);
  5. adds the tsImport prerequisite;
  6. runs `ensure_job_initialized`, copies the species' values through one `species_defaults(job_type, species)` shared
     with `protocols.apply`, and restores the tilt filter's saved labels;
  7. copies settings when cloning (`copy_settings_from`, `_base.py:463`);
  8. pins inputs: `after=` to that job (so an older full-set job of the same type does not tie), `reading=` one input to
     a named source (the I/O section's "+ Subtomo / + Warp export"); then `apply_aggregation_overrides`
     (`services/aggregation/extraction.py:255`) and `pin_sources_made_ambiguous`;
  9. updates `pipeline_order`, marks the state dirty and saves.
- **`remove_job(state, iid)`** removes a job that has not run, unless a queued or running job reads it (`jobs_in_use`,
  `path_resolution_service.py:1762`). It cleans `pipeline_order`, and any other job's `source_overrides` that named it
  (`pipeline_builder_panel.py:562, 586`). Deleting a job that has run, into Trash, stays the UI's.
- **`exclude_tilt_series(state, ts_id, excluded)`** wraps `registry.set_excluded` (`services/tilt_series/registry.py:246`)
  with the "not the last one" guard that is UI code today (`ui/tomo_dashboard_dialog.py:407`).
- Every way of adding in the UI calls `add_job`. `add_instance_to_pipeline` keeps the `SingleFlight` guard, the species
  dialog and the toasts.
- Behaviour change (its own commit): `protocols.apply` gains the Warp-export branch.

### 3.3 The setting doors: `services/job_params.py`
- **`set_params(state, iid, values)`** returns `ok(applied, unchanged)` or `err(rejected={name: why}, code=...)`. It:
  - refuses a frozen job with `JOB_FROZEN` (the model's INFO-only drop stays as a backstop);
  - checks each value against the field's annotation and its metadata (`ge`/`le`, enum members); `apply._coerce`
    (`apply.py:327`) does the type part today, without the bounds;
  - applies coupled writes, such as Denoise Predict's Denoiser, which sets the method, `isonet_deconv` and the model
    input together (`ui/job_plugins/default_renderer.py:162-176`, moves here);
  - reads the values back.
- **`set_tilt_filter(state, iid, mode=, model=, threshold=)`**: the mode through `set_tilt_filter_mode`; model and
  threshold move from the widgets (§0.10).
- **`set_slurm(state, iid, values | reset)`**: validated values, written only where they change (moves from
  `slurm_tab.py:207`); `reset` is `clear_slurm_overrides` (`_base.py:248`). No presets: the UI has none.
- **`set_input(state, iid, slot, source | path | auto, facet=None)`**: the input-loop check (`cycle_with_override`,
  `path_resolution_service.py:543`), then the writes the I/O section makes today (`io_config_component.py:692`,
  `:981-1036`). **`reset_inputs(state, iid)`** for a failed job (`:309`). **`set_gain(state, path | None)`** (`:380`).
- Each door marks the state dirty itself (§0.16), so none depends on a widget's forced save.
- The widgets call the same doors (its own commit).

### 3.4 Fields that copy their value (UI)
- Choice rows, the SLURM section and the I/O section refresh by signature, as the rest of the page does.
- A SLURM field writes an override only if its text changed while it had focus.

### 3.5 Creation: everything the landing page can do
- **`services/dataset_scan.py`** takes a directory or globs and returns a `DatasetScan`. The UI-only parts of §0.2 move
  here: the glob from a directory, the mdocs beside the frames or elsewhere, the `*/` retry, and the frame kind. It
  returns:
  - the series and the movie kind (EER, TIFF or stacks);
  - the mdocs, gain-reference candidates, and folders that reuse series names;
  - for each acquisition value, what the data says: `{value, found_in: mdoc | header | none, mixed}`.
- **`ProjectSpec` + `create_project(spec)`** is the one function behind landing Create and the API. It:
  - takes raw data (a scan) or no raw data, and later tomograms from other projects (§3.8);
  - takes every choice the form offers: name, base, series selection, folder split (folder-qualified names; one
    project per folder is one call per folder), dose, gain reference, and "shared";
  - holds every acquisition value as a required field:
    - the landing page fills the values it does not let you type from the model defaults, in its own code, as today
      (D3);
    - the API takes every one from the request (D3): pixel size, voltage, Cs, amplitude contrast, dose per tilt, tilt
      axis, sample thickness, the tilt-angle and defocus hands, EER fractions for EER movies, and the gain reference
      (a path, or none);
  - checks the spec as `get_missing_requirements` checks the form today (moves here);
  - names in `warnings` each value that differs from what the data says;
  - refuses a directory that exists (§2.12);
  - keeps the gain reference whether or not a scan ran;
  - builds the tilt-series registry off the event loop; today `_build_and_persist_registry` parses every mdoc on it;
  - reads the RELION init's output so the subprocess cannot block on a full pipe.
- **UI:** unchanged. The silent defaults of §0.3 stay on the landing page for now (D3).

### 3.6 Species and templates: `services/species_edit.py`
- **`create_species(state, name, …)`**: the orchestration in `ui/species/prompt.py:228` moves here (`add_species`,
  `project_state.py:955`, then the edits, the template import, `register_mask`, and one save).
- **`set_species(state, sid, …)`**: identity edits (`ui/species/overview_tab.py:106`) and extraction geometry
  (`commit_extraction_geometry`, `ui/particles/list_actions.py:354`).
- **Template and mask files** register through `species_admin` (`register_template` `:135`, `register_mask` `:177`,
  `select_template` `:198`, `select_mask` `:215`). The import dialog's inline copy of that logic goes
  (`ui/template_workbench.py:2001-2013`), and so does its "register the white/black pair, select the white" step
  (`:1963`).
- **The workbench's generators** become `make_template(kind=shape | pdb | emdb | resample | lowpass | flip, …)` and
  `make_mask(kind=threshold | sphere, …)` over `template_service` (`:70, 73, 89, 157, 213, 289`) and `pdb_service`
  (`:218`). What only the workbench does today moves with them: flip polarity (`:2114, 2188`), the mask file names
  (`:2251, 2328`), the auto box size (`:1806`), and the default pixel size taken from the project (`:431`).

### 3.7 Small facade gaps
- `backend.stop_pipeline` and `backend.cancel_job`, wrapping `pipeline_runner`.
- `get_job_logs` by instance id, and the tail of a single tilt series' task log.
- `ui/routing.py`'s pure half (`View`, `Route`, `route_to_path`, `parse_route`) moves to `services/routes.py`, so the API
  builds the same links. `ui/routing.py` keeps `apply_route` and `RouteWriter`.

### 3.8 Tomograms from other projects, ready for TM (for U2)
- `services/tomogram_import.py` gains a third mode beside synthesize and reference: **from CryoBoost jobs**. For each
  source it takes the project, its tomogram job (Reconstruct or Denoise Predict), the TS CTF job behind it, and the
  tomogram names.
- It writes `Tomograms/tomograms.star` and `Tomograms/tilt_series.star` in the new project: the sources' rows for those
  names, with absolute paths. Nothing is copied, and nothing is written in a source.
- The resolver offers the second star as a TS CTF tilt-series producer beside the first
  (`path_resolution_service.py:1194`), so both TM inputs resolve without pins.
- The sources must agree on the values TM takes from the project (§0.13), and on pixel size. When they do not, the call
  returns a `needs` item listing the groups: you pick one, or have one project made per group.
- These values come with the tomograms: the new project takes the sources' recorded values, and the answer lists them.
  D3 covers raw data, where nobody has recorded them yet.
- The import dialog offers the same mode, so the UI can do it too (§2.1). Both go through
  `backend.commit_imported_tomograms` (`backend.py:820`).

## 4. Part B — the API inside the server

### 4.1 Auth
- `TokenGate` also accepts `Authorization: Bearer <token>`, compared with `hmac.compare_digest` as the cookie is.
  Mutating verbs require it (§2.11).
- `get_server_info` serves as the API's health check. It returns the owner, host, port, checkout (`code_version`),
  default project base and conf warnings.

### 4.2 One verb table, two readers
- A new top-level package `api/` sits beside `ui/`, over the same backend. `check_boundaries.py` R1 grows to cover it:
  no `ui.*` imports.
- `api/verbs.py` has one entry per verb: name, a description written for the model, a pydantic input model,
  `read | write`, and the handler.
- Two readers use that table:
  1. FastAPI routes `POST /api/v1/<verb>`, which bring an OpenAPI schema for free;
  2. `GET /api/v1/tools`, which serves the MCP tool list (name, description, JSON schema, read/write) and the
     instructions text.
- The dispatcher enforces §2.12: a write verb's project is checked before its handler runs.

### 4.3 The verbs: the UI, action by action

This is the list of record for §2.1. Every action the UI has for composing, setting up or running a project is here
with its verb, or in the lists after the tables with the reason it has none. A new action of that kind in the UI comes
with its verb or a line in those lists.

**Landing page**

| UI action | Verb | Door | Stage |
|---|---|---|---|
| Point at a directory or globs; mdocs beside the frames or elsewhere; the `*/` retry; the frame kind | `inspect_dataset` (read) | `dataset_scan` over `parse_dataset_overview` | S4 |
| Create: name, base, series, folder split, dose, gain, shared, no raw data | `create_project` (+ every acquisition value, D3) | `create_project(spec)` | S4 |
| New project from a protocol (Protocols dialog) | `create_project(protocol=…)` | `apply_protocol` (`apply.py:120`) | S7 |

**Roster**

| UI action | Verb | Door | Stage |
|---|---|---|---|
| Add a job: row, row "+", header "+", the species Jobs tab, Subset's "Add <job>", the I/O "+ Subtomo / + Warp export" | `add_job(type, species, after, reading)` | `pipeline_edit.add_job` | S2 |
| Clone a job | `add_job(clone_of=…)` | the same | S2 |
| Remove a job that has not run | `remove_job` | `pipeline_edit.remove_job` | S2 |
| Exclude a tilt series (Journey) | `exclude_tilt_series` | `pipeline_edit.exclude_tilt_series` | S2 |
| New species (PARTICLES header) | `create_species` | `species_edit.create_species` | S7 |
| Import tomograms (PARTICLES header) | `import_tomograms` | `commit_imported_tomograms` (`backend.py:820`); its "not beside a Reconstruct job" rule moves from `pipeline_roster.py:1375` | S7 |

**Job pages, SLURM and I/O**

| UI action | Verb | Door | Stage |
|---|---|---|---|
| Parameter fields, with the job-specific widgets that write parameters (TM's template, mask, symmetry and search region; Subset's tilt-series table; Denoise Predict's Denoiser) | `set_params` | `job_params.set_params` | S3 |
| Tilt filter: mode, model, threshold | `set_tilt_filter` | `job_params.set_tilt_filter` | S3 |
| Tilt filter: run or cancel the DL prediction | `predict_tilts`, `cancel_predict_tilts` | `backend.submit_tilt_filter_predict` (`:339`), `cancel_tilt_filter_predict` (`:481`) | S5 |
| SLURM: a field; reset to the profile | `set_slurm` | `job_params.set_slurm` | S3 |
| I/O: pin a source, a path or a facet; back to automatic | `set_input` | `job_params.set_input` | S3 |
| I/O: reset a failed job's inputs; the project's gain reference | `reset_inputs`, `set_gain` | `job_params` | S3 |
| Cancel this job | `cancel_job` | `pipeline_runner.cancel_job` | S5 |

**Species and templates**

| UI action | Verb | Door | Stage |
|---|---|---|---|
| Import a species from the lab catalog | `import_species` | `catalog.import_species` (`catalog.py:300`) | S5 |
| Register a template or mask file; select one | `add_template`, `add_mask`, `select_template`, `select_mask` | `species_admin` | S5 |
| Edit a species: identity, extraction geometry | `set_species` | `species_edit.set_species` | S7 |
| Workbench: shape, PDB, EMDB, resample, low-pass, flip; masks by threshold or sphere | `make_template`, `make_mask` | §3.6 | S7 |

**Run controls**

| UI action | Verb | Door | Stage |
|---|---|---|---|
| Run (it also resubmits failed jobs, for their failed tilt series) | `run` | `deploy_and_run_scheme` | S5 |
| Stop | `stop` | `backend.stop_pipeline` | S5 |
| After a refused Run, cancel the leftover SLURM jobs it names | `cancel_leftovers` | `slurm_service.scancel_jobs`, as `pipeline_builder_panel.py:823` offers | S5 |

**Reads**

| Verb | What it answers | Through |
|---|---|---|
| `get_server_info` | §4.1 | |
| `list_projects` | the projects under a base, with their status and whether the API made them | `backend.scan_for_projects` |
| `get_project` | jobs (instance, type, status, SLURM id, per-TS ok/fail/running counts), species, review hold, `ui_url` | in-memory state + `array_status_cache` |
| `get_job` | parameters (value, default, bounds, help, edited?), SLURM, each input's chosen source and why, outputs (stars, preview PNGs that Claude Code can open), `ui_url` | |
| `get_job_tasks` | per-tilt-series state and reason, filtered and paged | `array_tasks` |
| `get_job_logs` | run.out/err tail, or one tilt series' task log | §3.7 |
| `list_job_types` | each type's phase, what must come before it, the prerequisite added with it, whether it takes a species, its inputs and outputs | `JOB_SPECS` + each param class's slots |
| `get_job_type` | each parameter of a type: kind, default, bounds, choices, help | the param class |
| `get_input_candidates` | what the I/O section lists for an input, and why any is unreadable | `get_candidates_for_slot` (`path_resolution_service.py:660`), `unreadable_reason` (`:290`) |
| `list_species`, `get_species` | the project's species and the lab catalog's; templates, masks, selections | `services/particles/catalog.py`, the registry |
| `plan_run` | jobs in submit order, inputs, resources, held jobs, refusals, needs | `_submit_chain`'s checks, run without allocating anything |
| `get_task` | a background task's progress and result | `background_tasks` |
| `list_kitchen_subsets`, `get_kitchen_subset_sources` | §7 | `services/sta_kitchen/` |

**`plan_run` matters most.**
- Today `_submit_chain` checks inputs and allocates job directories in one pass (`:398-624`), and resolving inputs needs
  the allocated directories.
- The dry pass reports, for each input slot, the source job that `_select_source` would pick and why: pinned, recorded
  at submit, or automatic (with the reason). It does not report paths.
- So the inputs that are still picked silently show up in the plan, without being fixed here:
  - with several refiners, the most recent one wins;
  - per-list extraction takes the first one;
  - aggregation overrides are applied to every species;
  - a job with no species falls back to the project's only one.

**Kept in the UI: looking and judging** (§2.3).
- The tilt-filter review: labels, Approve, Reopen. Approve submits the jobs held behind it.
- Curation sessions, pick lists (merge, dedup, import, assign, per-list extraction), keep/drop in the pick viewer,
  particle sets in analytics.
- The STA kitchen: verdicts, subsets, membership, Make set.
- Aggregation and merges, which combine sets you judged.
- What these produce becomes a source that any job can read, through `set_input`.

**Not in v1:**
- Deleting: a job into Trash, a species, a template or mask, a project, the Trash.
- Publishing a species to the lab catalog: it writes outside the project (D7).
- Exporting a project as a protocol.

**Not offered, because the UI does not offer it:** running part of the roster, SLURM presets, reordering the roster,
running a finished job again (clone it), transferring ownership.

### 4.4 Projects the server has not loaded
- A verb on a project path loads it the way a tab would (`preload_project_state`, `project_state.py:1541`). That also
  puts the project under the monitor.
- Links carry `?base=` (`/p/<name>?base=<parent>`). A name alone is looked up under the prefs base, the recent bases
  and the config default (`services/project_resolve.py:52-73`), and a project the API made elsewhere may be in none of
  them.

## 5. Part C — the Claude Code side

### 5.1 MCP over stdio, as a thin forwarder (D1)
`agent/crboost_mcp.py` imports only `mcp` and `httpx`, so it starts well within Claude Code's 30 s `MCP_TIMEOUT`. It runs
with `uv run --script` and inline dependencies, which keeps `mcp` out of the server's lock (roadmap 39). It:
- **finds your server:** from `CRBOOST_URL`; else this checkout's port in `~/.crboost/ports.json`
  (`services/port_survey.py:36`); else the one CryoBoost server running as you. If there are several, it errors and names
  them.
- **sends the token only to a socket you own:**
  - Before the first call, it reads `/proc/net/tcp` (as `port_survey.listeners`, `:129`, does) to check that the port's
    listener runs under your uid.
  - The reason: every user of the headnode shares `localhost`, and the port can move or be taken by someone else between
    restarts. An MCP entry with a fixed URL and a Bearer header would hand your token to whoever holds that port.
  - The script must be self-contained (stdlib + `mcp` + `httpx`), so it does this itself rather than importing it.
- **runs on the server's login node.**
  - CBE.next has two login nodes, and the `cbe.next` alias lands on either (`crboost:32`). `~/.crboost/ports.json`
    is in the shared home, but it holds each checkout's preferred port, not the node a server runs on.
  - The uid check reads this node's sockets, so the forwarder does not reach across nodes: that would send the token
    to a port whose owner it cannot check.
  - Finding no server of yours here, it says so and names this node. The tunnel line of the server's startup banner
    names the server's node.
- **reads `~/.crboost/token` itself.** The token never appears in a config file or a tool result.
- **lists the tools from `GET /api/v1/tools`.** If the server is down when the session starts, it offers only
  `get_server_info`, which says how to start it. `/mcp` reconnects once the server runs.
- **forwards each tool call** as `POST /api/v1/<verb>`.

The alternative is an `http` MCP endpoint inside the server, with a `headersHelper` script reading the token. It saves
the forwarder but carries the port problem above. Keep it for a session on a laptop through the tunnel (L3).

### 5.2 Instructions and the skill
- **Server instructions** (at most 4 096 characters, loaded when the session starts) cover:
  - what CryoBoost is, and the read/write split;
  - the API reaches what the UI reaches; looking and judging are the user's, in the UI, so hand over the `ui_url`;
  - the §2 rules in the agent's words: never invent a value; a `needs` item is a question for the user; `plan_run` comes
    before `run`; a failure is reported, not retried;
  - on raw data, every acquisition value comes from the user: show what the data says, ask, never fill one in;
  - a preprocessing chain gets the tilt filter unless the user says otherwise, in the mode the user names (D4);
  - the API changes only the projects it made;
  - never edit `project_params.json`, and never call `sbatch` or `scancel` on a CryoBoost project.
- **A skill** (`agent/skills/crboost/SKILL.md`) holds the recipes: preprocessing, matching a species, changing something
  that already ran, U2, reading a failure, and what each job does. The docs' pattern: "MCP provides the connection; a
  skill teaches Claude how to use it well."

### 5.3 Permissions
- Allow the read verbs: `mcp__crboost__get_*`, `mcp__crboost__list_*`, `mcp__crboost__inspect_*`,
  `mcp__crboost__plan_*`. Every other verb prompts by default.
- `create_project` and the verbs that submit or cancel SLURM work (`run`, `predict_tilts`, `stop`, `cancel_job`,
  `cancel_leftovers`) set `anthropic/requiresUserInteraction` in their tool `_meta`. They then ask even when a rule
  allows them: `create_project` so you see every value before the project exists, the others since they spend GPU
  hours or end them.
- Deny `Edit` and `Write` of `**/project_params.json`.

### 5.4 Packaging (D6)
- **Now, for the maintainer:** a user-scope entry,
  `claude mcp add --scope user crboost -- uv run --script <checkout>/agent/crboost_mcp.py`. It works from any directory.
- **Later, for the lab:** a plugin in this repo (`agent/` with `.mcp.json`, `skills/` and `.claude-plugin/plugin.json`,
  plus `.claude-plugin/marketplace.json` at the root). Install it with
  `claude plugin marketplace add <repo>` and then `claude plugin install crboost@<marketplace>`.
- A short `docs/agent.md` covers setup, the permission snippet, and what the agent will and will not do.

## 6. Talking to your data: how a session goes

1. **What is there.** `inspect_dataset` gives the series, the movie kind, what the data says for each acquisition value,
   and the gain-reference candidates.
2. **The project.** The agent shows you every value (D3): those the data gives, for you to confirm, and those it does
   not, for you to give. `create_project` carries them, with the landing page's choices (series, folder split, dose,
   gain, shared). Claude Code shows the call before it runs (§5.3).
   - A landing page lists the project within 15 s if it sits under that tab's base (`projects_overview.py:400`).
   - The link always opens it.
3. **The jobs.** From `list_job_types` and `get_job_type`, the agent adds what you ask for and sets what you name;
   everything else keeps its code default, as after an add in the roster. "Preprocessing" is the Preprocessing phase's
   chain: Import → Motion & CTF → Tilt Filter → Alignment → TS CTF → Reconstruct, the filter included unless you say
   otherwise (D4). Denoising, Missalignment and Segment join when you ask.
4. **Plan and run.** `plan_run` shows the jobs, their inputs and resources, and what waits for a review. You approve
   `run`.
5. **Watch.** `get_project`, `get_job_tasks` and `get_job_logs`. A failure is reported with its log lines, not retried.
6. **Look.** You open the `ui_url`: the tilts, the alignments, the tomograms, the score maps, the picks, the graphs. The
   tilt review is yours.
7. **Change.** A Scheduled or Failed job takes new values directly, and Run resubmits a failed job for its failed tilt
   series. A job that finished is cloned (`add_job(clone_of=…)`), set and run, and the jobs after it are added
   `after=` the clone, as you would in the roster.

**The tilt filter (D4).** It is in unless you leave it out, and its mode is yours to name. Without a word from you it
keeps its default, Manual: Run holds alignment and everything after it until you review the tilts in the UI and
approve. DL review scores the tilts for you and still waits for Approve. DL auto commits the model's verdict and does
not wait. The DL modes need `tilt_filter.models` in conf.yaml.

## 7. U2: a kitchen subset, matched again in a new project

**Decided (maintainer, 2026-11-04):**
- Y is an STA-kitchen subset: Z's particles pooled from several projects.
- Its source tomograms are matched again in a new project that the API makes. The source projects are only read (D7).
- This keeps "re-run TM, never re-home picks": the new picks stay with the new project's TM job.

```
 home project H · kitchen of Z · subset Y (uids)
        │ get_kitchen_subset_sources(H, Z, Y)                                      read only
        ▼
 P1  {TS_03, TS_07}  tomograms of tsReconstruct, tilt series of tsCtf    300 kV · Cs 2.7 · AC 0.10 · 1.35 Å
 P2  {TS_11}         tomograms of denoisepredict, tilt series of tsCtf   300 kV · Cs 2.7 · AC 0.10 · 1.35 Å
        │ the values agree → one project        (else a needs item: one group, or one project per group)
        ▼
 create_project(N, tomograms_from=[P1 …, P2 …])        N is new; P1 and P2 are not written
 import_species(N, Z from the lab catalog)              the catalog id ties N's species to Z
 add_job(N, templatematching, species) · add_job(N, tmextractcand, species) · set_params(…)
 plan_run(N) · run(N)
        ▼
 you, in the UI: N's matches and picks; then in Z's kitchen, Add members → N's Pick candidates
```

- **`list_kitchen_subsets(home, species)`** (read): the subsets with their names and counts.
- **`get_kitchen_subset_sources(home, species, subset)`** (read). It is built on the per-member grouping in
  `export.plan`. For each source project it returns:
  - path and label;
  - the tomograms;
  - the tomogram job each came from and the TS CTF job behind it. A hand-curated pick list has no TM; its tomogram job
    is the one whose directory holds the frame's reconstruction;
  - the values of §3.8 (voltage, Cs, amplitude contrast, pixel size), and the groups they form;
  - anything missing, such as a reconstruction that is gone.
- **The new project is made from those sources** (§3.8), then built with the same verbs as any other.
- **The template (D8):** the one the catalog's Z carries. With several, or none, it is a `needs` item listing them and
  the templates the sources' TM jobs used.
- **The pool is not written.** It lives in the home project, which a person made, and membership is judging (§2.3). You
  add N's Pick candidates in the kitchen: Add members offers them because N's species came from the same catalog entry
  (`members.matching_species`, `members.py:72`; `offered_members`, `:79`).
- **Old and new picks side by side** needs N's frames to carry the sources' acquisition labels, since the kitchen
  groups sources per acquisition (`pool.acq_key`, `pool.py:331`). S9 checks this first.

## 8. Decisions

| # | Question | Recommendation or decision |
|---|---|---|
| D1 | What the agent talks to | **Decided 2026-11-05:** the server gains the verbs and nothing else, a small API that any outside client with the token can use to compose and queue work: Claude Code through the stdio forwarder (§5.1), a script, curl. No chat or model inside the server |
| D2 | The API's scope | **Decided 2026-11-04:** what the UI reaches (§2.1, §4.3): any data folder the landing page can scan and create from, any combination of jobs the UI allows, any parameter. No protocol and no default set; recipes are the skill's (§6) |
| D3 | Where acquisition values come from | **Decided 2026-11-04:** through the API, every one from you (§3.5). The landing page keeps its defaults for now |
| D4 | The tilt filter | **Decided 2026-11-04:** in a preprocessing chain unless you say otherwise; its mode as you name it, else Manual, which waits for your review in the UI (§6) |
| D5 | Protocol pins vs values from the data | **Dropped:** the API applies a protocol only as the dialog does (§4.3). The copia pin it came from is in §10 |
| D6 | Packaging | **Decided 2026-11-05:** a user-scope entry for the maintainer now; the in-repo plugin for the lab later (§5.4) |
| D7 | Which projects the API may change | **Decided 2026-11-04:** only those it made (§2.12), for now. Any project can be read |
| D8 | The template for U2 | **Decided 2026-11-05:** the one the catalog's Z carries; several or none → a `needs` item (§7) |
| D9 | The record of agent writes | **Decided 2026-11-05:** console events + `Logs/agent_api.jsonl`. Nothing extra in the UI |
| D10 | Picking again without matching again | **Dropped with D7:** a new project has no score maps to pick from again |
| U2 | "Subset Y of species Z globally" | **Decided 2026-11-04:** an STA-kitchen subset, matched again in a new project (§7) |

## 9. Stages

Every stage is one commit or a short series, and leaves the app runnable. The checks are run once at the end (§11),
as the standing decision says.

The arc is built in its own worktree (maintainer, 2026-11-05), S1 to S9 back to back. It stops for the maintainer only
where a stage's facts-first check changes the plan. S8 and S9 are the likely ones: whether TM takes the pixel size per
tomogram, and whether N's frames carry the sources' acquisition labels.

### S1 — The roster belongs to the project (§3.1)
UI. Behaviour change in its own commit.
- *Success:*
  - Tab A adds a job; within 3 s it is in tab B's roster.
  - B removes it; it is gone from A.
  - Run in either tab submits the same set.
  - A reload changes nothing.

### S2 — The pipeline-edit door (§3.2)
A move. The Warp-export branch for protocols is its own flagged commit.
- *Success:*
  - Every way of adding in the UI (§4.3) behaves as before: ids, prerequisites, species fields, pins, warnings, toasts.
  - A TM made from a protocol gets the same species fields as one added from the roster.
  - Excluding the last tilt series is refused by the door, not only by the Journey.

### S3 — The setting doors, and fields that refresh (§3.3, §3.4)
- *Success:*
  - A value set through a door shows on an open job page within 3 s. This includes template, symmetry, SLURM, input
    and tilt-filter values.
  - Tabbing through a SLURM field without typing writes nothing.
  - A bad value comes back naming the bound it broke.
  - A write to a running job comes back as `JOB_FROZEN`.
  - After a restart, every door's write is still there (none relied on a widget's forced save).

### S4 — Creation, everything the landing page can do (§3.5)
- *Facts first:* which acquisition values the drivers and the job.star writers read, so the API's required list is
  exact.
- *Success:*
  - Landing Create makes the same project as before, defaults included, for raw data, split folders and no raw data.
  - A spec missing any value is refused, naming each one.
  - A pixel size that differs from the mdocs' comes back as a warning naming both.
  - A gain reference survives a Create without a scan.
  - Creating 300 tilt series leaves no loop stall over 1 s in the log.

### S5 — The API: reads, creation, composition, runs (§4, §3.7)
Every verb in §4.3 marked S2–S5.
- *Success:*
  - `curl -s -X POST -H "Authorization: Bearer $(cat ~/.crboost/token)" localhost:P/api/v1/get_server_info` answers.
  - A write verb sent with only the cookie is refused.
  - A write verb on a project made by hand comes back as `NOT_WRITABLE`, and the project is unchanged.
  - `create_project` on a directory that exists is refused.
  - `/api/v1/tools` lists every verb with its schema.
  - A write shows in an open tab within 3 s.
  - **Parity:** from one dataset, a project built by hand and one built through the API with the same choices have the
    same jobs, parameters, input pins and SLURM values (`project_params.json` compared without paths and times).

### S6 — The Claude Code side (§5)
- *Facts first:*
  - Checked 2026-11-05 on cbe-login-0: `uv` is `/usr/local/bin/uv`, and PyPI answers, so `uv run --script` can
    fetch `mcp` and `httpx`.
  - Does Claude Code re-list tools after `/mcp` reconnects?
- *Success:*
  - `claude mcp list` shows `crboost` connected.
  - Read verbs run without a prompt.
  - `create_project` and `run` always ask; the `create_project` prompt shows every value.
  - Pointed at a port held by another uid, the forwarder refuses and sends nothing.
  - Started on the other login node, the forwarder names this node and sends nothing.

### S7 — Species, templates, tomogram import, protocols (§3.6, the rest of §4.3)
Every verb in §4.3 marked S7.
- *Success:*
  - A species made through the API, with a template simulated from a PDB entry and a threshold mask, matches one made
    in the workbench from the same inputs: same files, same selections.
  - Importing tomograms beside a Reconstruct job is refused, as in the roster.
  - A project created from a protocol through the API equals one from the dialog with the same inputs.

### S8 — Tomograms from other projects (§3.8)
- *Facts first:*
  - Which per-tilt-series stars does a Reconstruct job's `tomograms.star` point at, and are they its TS CTF input's?
  - Does TM take the pixel size per tomogram (from the star or the header) or from the project?
  - What else do Pick candidates and extraction read from the project?
- *Success:*
  - A project made from two projects' Reconstruct and TS CTF jobs runs TM and Pick candidates with no pins, from the
    API and from the import dialog alike.
  - The source projects' files and `project_params.json` keep their mtimes.
  - Sources with different voltages come back as a `needs` item naming the groups.

### S9 — The kitchen reads (§7)
- *Facts first:* do N's frames carry the sources' acquisition labels (`pool.acq_key`)? If not, the kitchen takes N's
  tomograms for new acquisitions, and old and new picks of one tilt series are not grouped together.
- *Success:*
  - On a home project whose pool spans two projects, `get_kitchen_subset_sources` lists both, with their tomograms,
    jobs and values.
  - A project made from them runs (S8), and the kitchen's Add members offers its Pick candidates.

## 10. Not doing
- A chat, model or agent session inside the server or the UI (dropped 2026-11-04; D1), and with it the per-person
  Claude credentials.
- Through the API: changing a project made by hand (D7, for now); looking and judging (§2.3); the deletions and the
  catalog publish of §4.3.
- What the UI does not offer either (§4.3).
- The landing page's silent defaults (§0.3): they stay for now (D3).
- Several people per server (roadmap 23).
- Jobs that span projects.
- Parameter sweeps or grids (protocols are shapes, not harnesses).
- New scientific defaults.
- Fixing the silent input picks that `plan_run` reveals (§4.3); they get their own roadmap if they bite.
- Found along the way, not fixed here:
  - copia-tutorial pins `tomo_dimensions 4096x4096x2048`, which is wrong for K3 stacks;
  - the jobs list passed at Create is always empty, and its `j.value` would fail on string ids
    (`data_import_panel.py:803`);
  - the "shared" tooltip promises an ownership transfer that no UI offers (`data_import_panel.py:1381`;
    `backend.transfer_project_ownership`, `backend.py:2236`);
  - the roster names a second instance `{job}__2`, protocols `{job}__{species}` (`services/protocols/schema.py:93`).

## 11. Runtime checklist (the maintainer's step, after the arc)
1. **Two tabs on one project.** Add, remove and Run from either tab; both show one roster (S1).
2. **Landing Create** on a dataset without PixelSpacing behaves as before (S4).
3. **The API with curl** (the S5 checks). The cookie alone on a write → refused. A write on a project made by hand →
   `NOT_WRITABLE`.
4. **Parity.** Build a small pipeline by hand; have the agent build the same one from the same data; compare the two
   `project_params.json` (S5).
5. **In Claude Code on the server's login node, from a data directory**, with no repo files open:
   - "make a project from here and run preprocessing; align with AreTomo at 200 nm";
   - it shows every acquisition value and asks for those the data lacks; approve `create_project`, then `run`;
   - in the `ui_url`: review the tilts, approve; the roster, job pages and pictures are those of a hand-made project.
6. **While it runs:**
   - Edit a SLURM value in the browser, then ask the agent for that job's SLURM; it shows your edit.
   - Have the agent set a parameter on a Scheduled job; the open page shows it within 3 s.
7. **Change something that ran:** "redo the alignment at 250 nm". The agent clones it, adds what follows after the
   clone, and runs; the UI shows both.
8. **Force a failure** (an impossible walltime). The agent reports the failing log lines and does not resubmit.
9. **Run from the browser and the agent at the same moment.** One submission.
10. **U2 on a home project whose Z pool spans two of your projects:**
    - "take subset Y and match it again";
    - the agent makes one new project from both and names its sources; approve `run`;
    - the source projects are unchanged; in the kitchen, Add members offers the new picks.
11. **Ask the agent to add a job to a project you made by hand.** It says it cannot, and gives you the link.

## 12. Risks
1. **The agent also has Bash.** It could edit project files or call `sbatch` directly. The instructions say not to and
   the deny rules stop the edits. Under the in-memory rule, such an edit is lost on the next save anyway.
2. **Two servers writing one project** (dev and release). This is not new; roadmap 23 stage 3 (the lease) fixes it.
   Keep one server running while the agent works.
3. **GPU hours.** The API can now set anything. `run` and `predict_tilts` always ask, and `plan_run` names the
   resources.
4. **The API falls behind the UI.** A new composing action in the UI without a verb breaks §2.1 quietly. §4.3 is the
   list of record, and adding a UI action means adding its verb or its line there.
5. **Large projects.** Answers carry counts, and per-tilt-series detail is paged.
6. **A server older than the checkout.** The forwarder reads the tool list from the running server, so the newer verbs
   are simply absent until a restart.
7. **The token leaving the machine.** It is never in a tool result or a config file. The uid check keeps it off other
   users' ports.
8. **The mark is one field in `project_params.json`.** A hand edit could open a hand-made project to the API. The deny
   rule (§5.3) keeps the agent itself from making that edit.

**Modern-Python weave-in.**
- The verb table as frozen `dataclass`es with `kind: Literal["read", "write"]`, and pydantic input models (FastAPI turns
  them into the schemas).
- `needs` items as a discriminated union (`kind: Literal["value", "choice", "pin"]`).
- `ProjectSpec` (every acquisition value required) and `DatasetScan` as pydantic models; what the data says about one
  value as a `DataValue` (`value`, `found_in: StrEnum`, `mixed`).
- `get_job_type` built from each param class's `model_fields`, so a new parameter needs no API change.
- The new `ErrorCode` members join the existing `StrEnum`.
- A static self-test (`tests/`): every verb's schema builds, and `/api/v1/tools` matches the table.

## 13. Stage log

*(empty)*

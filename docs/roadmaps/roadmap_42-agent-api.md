# Roadmap 42 — Agent API: drive CryoBoost from a Claude Code session

**Status:** scoped 2026-11-04, no code. Every anchor comes from four code reads and a check of the Claude Code
docs, all on 2026-11-04 at `70ce4db`. The maintainer decided what U2 means (§7); D1–D10 are open (§9).
Parts A (§3) and B (§4) stand on their own: they fix today's multi-tab roster and silent acquisition defaults
whether or not an agent ever drives anything. **Pickup: S1.**

**Ask (maintainer, 2026-11-04).** A Claude Code session, first in a terminal outside the server and later inside
the UI, to which one can say:
- **U1** "initiate the data in dir X to a project and run it through preprocessing with reasonable/default params";
- **U2** "take subset Y of species Z globally and run it through template matching / picks". Y is an STA-kitchen
  subset (decided, §7);
- **U3** and then look at the results in the UI as if it were a regular, hand-built project, and carry on there.

The session inside the UI waits (§8): it needs Claude credentials per person.

**In one line.** Put a small, typed, token-gated verb API inside the running server. Every verb goes through the same
service function as the matching button. Claude Code calls it through a stdio MCP forwarder. Whatever the agent builds
is then a regular project that every open browser tab shows.

## 0. What is there today (S0 facts, 2026-11-04)

1. **Nothing over HTTP can change anything.**
   - `/api/tilt-thumb`, `/api/vis-asset` and `/api/project-preview` (`server_app.py:63-125`), and `/api/file`
     (`ui/template_workbench.py:120`), only serve files.
   - `TokenGate` (`services/server_auth.py:63`) takes `?token=` or the per-port cookie. It has no header form.
2. **Creating a project.**
   - Landing → New project → `create_project_and_scheme` (`backend.py:2162`) → `ProjectService.initialize_new_project`
     (`project_service.py:464`). This makes a project with **no jobs**.
   - `apply_protocol` (`services/protocols/apply.py:120`) is the only path that creates stages. The only bundle is
     `config/protocols/copia-tutorial` (importmovies → class3d).
   - Much of what Create needs lives only in `ui/data_import_panel.py`:
     - glob checks and the `*/` retry, and the refusal of a half-filled form (`:333-345`); without it the backend skips
       the import and reports success;
     - series selection and the folder-split choice;
     - acquisition values taken from the selected series, and the dose row;
     - the gain reference, which is dropped when Create runs without a scan (`:797-799`).
3. **Acquisition values default silently** (`services/models_base.py:314-341`).
   - The defaults: pixel size 1.35 Å, 300 kV, Cs 2.7 mm, amplitude contrast 0.10, tilt axis −95°, detector 4096².
   - A project takes them without a word when the mdocs lack the value. Only the dose is guarded: there must be a value
     and a recorded source, or creation refuses (`project_service.py:550-582`).
   - The scan bar shows chips only for values it found, so a defaulted pixel size looks like a detected one.
   - Detected `invert_tilt_angles` and EER fractions are never applied.
4. **Adding a job is UI code:** `PipelineBuilderPanel.add_instance_to_pipeline`
   (`ui/pipeline_builder/pipeline_builder_panel.py:359-536`).
   - It does all of this: refusals, the single instance of an interactive job, the instance id (`next_instance_id`,
     `pipeline_constants.py:49`), the species requirement, the tsImport prerequisite (`:633`), the copy of the species'
     values (`:433-488`), clone and merge overrides, input pins, `pipeline_order`, and the save.
   - No service function does this. `protocols.apply` keeps a private, partial copy (`_apply_species_defaults`,
     `apply.py:359`) that lacks the Warp-export branch.
5. **Setting a parameter is widget code.** There is no service function for "set X of job Y to V".
   - `AbstractJobParams.__setattr__` (`services/jobs/_base.py:418-451`) drops a write to a user parameter on a job that
     is not Scheduled or Failed, leaving only an INFO log line.
   - There is no `validate_assignment`. Bounds are checked only in `numeric_forward` (`ui/job_plugins/_field_styles.py:287`).
   - SLURM's "write only if changed" lives in `slurm_tab.py:203-215`.
6. **Run is sound.**
   - `deploy_and_run_scheme` holds a per-project lock (`pipeline_orchestrator_service.py:112-122`).
   - `_submit_chain` (`:398`) refuses with its reason: `SET_CHECKS`, `still_live`, ambiguous inputs, or a cycle. On
     success it returns the SLURM ids. Two Runs at once submit once.
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
10. **The tilt filter parks the chain.**
    - Its default mode is Manual (`services/jobs/tilt_filter.py:121`), so the chain stops at alignment until someone
      approves the review.
    - Its `USER_PARAMS` is empty (`:99`), so a protocol cannot choose the mode.
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

U1 as a conversation:
```
 you:    make a project from /groups/…/run12 and run preprocessing with defaults
 agent:  inspect_dataset(dir)       → 41 tilt series, EER; pixel 0.93 Å (mdoc), dose 3.1 (mdoc), Cs 2.7 (site),
                                       needs: gain reference — 2 candidates in the directory
         ← asks you which gain reference                    (a `needs` item: never guessed)
         get_protocol("preprocessing")
         create_project(name, base, dataset, gain, protocol="preprocessing")  → task t1 (the tray shows it too)
         get_task(t1)                → done: project + ui_url
         plan_run(project)           → 7 jobs in submit order, their inputs and resources, nothing parks
         run(project)                → Claude Code asks you → SLURM 81234…81240
 you:    open the ui_url → the project's roster, running, like any project you built by hand
```

## 2. The contract: rules for every verb

1. **One door per action.**
   - A verb calls the service function that the matching button calls.
   - Where that logic sits in `ui/` today, it moves to `services/` first (Part A), and the button calls it from there
     too.
   - Nothing is implemented twice.
2. **Nothing is invented.** A value the call needs that nobody gave comes back as a `needs` item (what is missing, why,
   and the options), and the call refuses until it is given. Examples: a pixel size absent from the headers, an input two
   producers could feed, a species with no template. The agent puts the question to you. This is CLAUDE.md *Surfacing
   uncertainty*, written as an API contract.
3. **Plan before acting.**
   - Reads and plans (`get_*`, `list_*`, `inspect_*`, `plan_*`) never change anything. Writes are separate verbs.
   - `run` submits only what `plan_run` shows.
4. **Failures are reported, not retried.** The agent reads the log and tells you what failed. It re-runs only when you
   ask (as in `feedback_no_cluster_workaround_automation`).
5. **Results are `ok()` / `err()`** (`services/result.py`).
   - Each result also carries `needs`, `warnings` and a `ui_url` for whatever the call touched.
   - `code` appears only where the agent has to branch. New `ErrorCode` members: `NEEDS_DECISION`, `JOB_FROZEN`,
     `NOT_WRITABLE`.
6. **Small answers.** Claude Code caps an MCP result at 25 000 tokens by default (`MAX_MCP_OUTPUT_TOKENS`) and saves
   anything longer to a file. Summaries carry counts; per-tilt-series detail has its own paged verb.
7. **Quick answers.**
   - Anything slower than a few seconds, such as an import, runs as a `BackgroundTask` (`services/background_tasks.py`),
     and the verb returns its id.
   - The task also shows in the tray of any tab on that project. The tray matches the resolved path string, so verbs
     always use the resolved path.
8. **The event loop stays free** (`docs/architecture.md`, *Where work runs*).
9. **Writes need the header, not the cookie.**
   - The per-port cookie rides along on any same-site request, and every `localhost` port counts as the same site.
   - So mutating verbs accept only `Authorization: Bearer`. Reads take either.
10. **Every write is recorded.**
    - One console event: `events.info("%s · agent: %s", project, what)`.
    - One line in the project's `Logs/agent_api.jsonl` (D9).

## 3. Part A — the UI shows changes from anywhere (prerequisite; useful without the agent)

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
- **`add_job(state, job_type, *, species_id=None, after=None, clone_of=None)`** returns
  `ok(instance_id, added, pins, warnings)` or `err(..., code=...)`. In order, it:
  1. checks `add_refusal`;
  2. enforces the single instance of an interactive job;
  3. picks the id with `next_instance_id` (moves from `ui/pipeline_builder/pipeline_constants.py:49`);
  4. requires a species where the job type needs one;
  5. adds the tsImport prerequisite;
  6. runs `ensure_job_initialized`;
  7. copies the species' values through one `species_defaults(job_type, species)`, shared with `protocols.apply`;
  8. copies settings when cloning;
  9. runs `apply_aggregation_overrides`, `add_after_pins` (`after=` pins the new job's inputs to that job, so an
     older full-set job of the same type does not tie) and `pin_sources_made_ambiguous`;
  10. updates `pipeline_order` and marks the state dirty.
- **`remove_job(state, iid)`** only removes Scheduled jobs. It also cleans `pipeline_order`, and any other job's
  `source_overrides` that named the removed job. Deleting a job that has run, into Trash, stays the UI's `delete_job`
  flow.
- `add_instance_to_pipeline` keeps the `SingleFlight` guard, the species dialog and the toasts, and calls `add_job`.
- Behaviour change (its own commit): `protocols.apply` gains the Warp-export branch.

### 3.3 One validated setter: `services/job_params.py`
- **`set_params(state, iid, values)`** returns `ok(applied, unchanged)` or `err(rejected={name: why}, code=...)`. It:
  - refuses a frozen job with `JOB_FROZEN` (the model's INFO-only drop stays as a backstop);
  - checks each value against the field's annotation and its metadata (`ge`/`le`, enum members); `apply._coerce`
    (`apply.py:327`) does the type part today, without the bounds;
  - applies coupled writes, such as the denoise plugin's "method from the train job"
    (`ui/job_plugins/default_renderer.py:162-176`), which moves here;
  - reads the values back.
- **`set_slurm(state, iid, values | preset | reset)`.**
- **`set_input(state, iid, slot, source)`** runs the input-loop check (`cycle_with_override`,
  `io_config_component.py:989`, which moves here).
- The widgets' forwarders call the same validators (its own commit).

### 3.4 Fields that copy their value (UI)
- Choice rows, the SLURM section and the I/O section refresh by signature, as the rest of the page does.
- A SLURM field writes an override only if its text changed while it had focus.

### 3.5 Creation with a source for every acquisition value
- **`services/dataset_scan.py`** takes a directory or globs and returns a `DatasetScan`:
  - the series and the movie kind (EER, TIFF or stacks);
  - the mdocs, gain-reference candidates, and folders that reuse series names;
  - each acquisition value as `{value, source: mdoc | estimated | site | user | missing, mixed}`.
  - The UI-only logic listed in §0.2 moves here, out of `data_import_panel.py`.
- **`ProjectSpec` + `create_project(spec)`** is the one function behind landing Create, the protocols dialog,
  `crboost_protocol.py apply` and the API. It:
  - refuses while a load-bearing value is `missing` (D3);
  - takes `site` values from conf.yaml, never from code;
  - keeps the gain reference whether or not a scan ran;
  - builds the tilt-series registry off the event loop; today `_build_and_persist_registry` parses every mdoc on it;
  - reads the RELION init's output so the subprocess cannot block on a full pipe.
- **UI:** the scan bar names each value's source. Create waits for missing values the way it waits for the dose today
  (a behaviour change, in its own commit).
- **Protocols:**
  - A stage may set the tilt filter's mode (D4).
  - Fields that depend on the data (`tomo_dimensions`, `out_average_halves`, `halfmap_*`) cannot be pinned (D5).
    Copia pins `tomo_dimensions 4096x4096x2048`, which is wrong for K3 stacks.

### 3.6 Small facade gaps
- `backend.stop_pipeline` and `backend.cancel_job`, wrapping `pipeline_runner`.
- `get_job_logs` by instance id, and the tail of a single tilt series' task log.
- `ui/routing.py`'s pure half (`View`, `Route`, `route_to_path`, `parse_route`) moves to `services/routes.py`, so the API
  builds the same links. `ui/routing.py` keeps `apply_route` and `RouteWriter`.

## 4. Part B — the API inside the server

### 4.1 Auth
- `TokenGate` also accepts `Authorization: Bearer <token>`, compared with `hmac.compare_digest` as the cookie is.
  Mutating verbs require it (§2.9).
- `get_server_info` serves as the API's health check. It returns the owner, host, port, checkout (`code_version`),
  default project base, protocols and conf warnings.

### 4.2 One verb table, three readers
- A new top-level package `api/` sits beside `ui/`, over the same backend. `check_boundaries.py` R1 grows to cover it:
  no `ui.*` imports.
- `api/verbs.py` has one entry per verb: name, a description written for the model, a pydantic input model,
  `read | write`, and the handler.
- Three readers use that table:
  1. FastAPI routes `POST /api/v1/<verb>`, which bring an OpenAPI schema for free;
  2. `GET /api/v1/tools`, which serves the MCP tool list (name, description, JSON schema, read/write) and the
     instructions text;
  3. later, the session inside the UI (§8).

### 4.3 Verbs (v1)

| Verb | Kind | What it does | Through |
|---|---|---|---|
| `get_server_info` | read | §4.1 | |
| `list_projects` | read | the projects under a base, with their status | `backend.scan_for_projects` |
| `get_project` | read | jobs (instance, type, status, SLURM id, per-TS ok/fail/running counts), species, review hold, `ui_url` | in-memory state + `array_status_cache` |
| `get_job` | read | parameters (value, default, bounds, help, edited?), SLURM, each input's chosen source and why, outputs (stars, preview PNGs that Claude Code can open), `ui_url` | |
| `get_job_tasks` | read | per-tilt-series state and reason, filtered and paged | `array_tasks` |
| `get_job_logs` | read | run.out/err tail, or one tilt series' task log | §3.6 |
| `list_protocols`, `get_protocol` | read | stages, parameters, species | `services/protocols/discovery.py` |
| `inspect_dataset` | read | §3.5 | `dataset_scan` |
| `list_species` | read | the project's species and the lab catalog's | `services/particles/catalog.py` |
| `plan_run` | read | jobs in submit order, inputs, resources, parks, refusals, needs | `_submit_chain`'s checks, run without allocating anything |
| `get_task` | read | a background task's progress and result | `background_tasks` |
| `create_project` | write | §3.5, as a task | `create_project` |
| `add_job`, `remove_job` | write | §3.2 | `pipeline_edit` |
| `set_params`, `set_slurm`, `set_input` | write | §3.3 | `job_params` |
| `set_tilt_filter_mode` | write | before Run | `backend.set_tilt_filter_mode` (`backend.py:498`) |
| `import_species` | write | from the lab catalog | `catalog.import_species` (`catalog.py:300`) |
| `add_template` | write | register a template or mask with a species, and select it | `species_admin.register_template` / `register_mask` |
| `run` | write | the roster (like the button), or the jobs named | `deploy_and_run_scheme` |
| `stop`, `cancel_job` | write | | §3.6 |
| kitchen verbs | | §7 | |

**`plan_run` is the verb that matters most.**
- Today `_submit_chain` checks inputs and allocates job directories in one pass (`:398-624`), and resolving inputs needs
  the allocated directories.
- The dry pass reports, for each input slot, the source job that `_select_source` would pick and why: pinned, recorded
  at submit, or automatic (with the reason). It does not report paths.
- So the inputs that are still picked silently show up in the plan, without being fixed here:
  - with several refiners, the most recent one wins;
  - per-list extraction takes the first one;
  - aggregation overrides are applied to every species;
  - a job with no species falls back to the project's only one.

**Not in v1:**
- Deleting a job into Trash.
- Approving the tilt-filter review. The review is yours; the agent hands you its `ui_url`.
- Clone, curation sessions, segmentation imports, aggregation merges.

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
- **reads `~/.crboost/token` itself.** The token never appears in a config file or a tool result.
- **lists the tools from `GET /api/v1/tools`.** If the server is down when the session starts, it offers only
  `get_server_info`, which says how to start it. `/mcp` reconnects once the server runs.
- **forwards each tool call** as `POST /api/v1/<verb>`.

The alternative is an `http` MCP endpoint inside the server, with a `headersHelper` script reading the token. It saves
the forwarder but carries the port problem above. Keep it for a session on a laptop through the tunnel (L3).

### 5.2 Instructions and the skill
- **Server instructions** (at most 4 096 characters, loaded when the session starts) cover:
  - what CryoBoost is, and the read/write split;
  - the §2 rules in the agent's words: never invent a value; a `needs` item is a question for the user; `plan_run` comes
    before `run`; a failure is reported, not retried;
  - that the tilt-filter review and curation are the user's;
  - always give the `ui_url`;
  - never edit `project_params.json`, and never call `sbatch` or `scancel` on a CryoBoost project.
- **A skill** (`agent/skills/crboost/SKILL.md`) holds the recipes: U1, U2, reading a failure, and what each job does.
  The docs' pattern: "MCP provides the connection; a skill teaches Claude how to use it well."

### 5.3 Permissions
- Allow the read verbs: `mcp__crboost__get_*`, `mcp__crboost__list_*`, `mcp__crboost__inspect_*`,
  `mcp__crboost__plan_*`. Every other verb prompts by default.
- `run`, `stop` and `cancel_job` set `anthropic/requiresUserInteraction` in their tool `_meta`. They then ask even when a
  rule allows them, since they spend GPU hours or end them.
- Deny `Edit` and `Write` of `**/project_params.json`.

### 5.4 Packaging (D6)
- **Now, for the maintainer:** a user-scope entry,
  `claude mcp add --scope user crboost -- uv run --script <checkout>/agent/crboost_mcp.py`. It works from any directory.
- **Later, for the lab:** a plugin in this repo (`agent/` with `.mcp.json`, `skills/` and `.claude-plugin/plugin.json`,
  plus `.claude-plugin/marketplace.json` at the root). Install it with
  `claude plugin marketplace add <repo>` and then `claude plugin install crboost@<marketplace>`.
- A short `docs/agent.md` covers setup, the permission snippet, and what the agent will and will not do.

## 6. U1: data in a directory → a preprocessed project

1. `inspect_dataset("/groups/…/X")` returns the series, the movie kind, the values with their sources, and the `needs`
   (for example, "pixel size: not in the mdocs or the headers").
2. The agent asks you each `needs` question; you answer.
3. `get_protocol("preprocessing")` (D2). This is a bundle the maintainer exports from a project that worked
   (`docs/protocols.md`, *The easy way*). The parameters are the maintainer's, not the agent's.
4. `create_project(...)` returns a task, then a project with its `ui_url`.
   - A landing page lists the project within 15 s if it sits under that tab's base (`projects_overview.py:400`).
   - The link always opens it.
5. `plan_run` shows the plan; you approve `run`; it returns the SLURM ids.
6. Later, `get_project` gives progress per job and per tilt series. On a failure, the agent reads `get_job_logs` and
   tells you.

An unattended run needs the tilt filter left out, or in DL auto (D4). In Manual it parks at alignment, and the agent
tells you where the review is.

## 7. U2: a kitchen subset, matched again in its source projects

**Decided (maintainer, 2026-11-04):**
- Y is an STA-kitchen subset: Z's particles pooled from several projects.
- Its source tomograms are matched or picked again, and the work lands in each source project.
- This matches "re-run TM, never re-home picks": picks stay with the TM job that made them.

```
 home project H · kitchen of Z · subset Y (uids)
        │ get_kitchen_subset_sources(H, Z, Y)
        ▼
 P1  tomograms {TS_03, TS_07}   tmextractcand ← templatematching ← tsReconstruct      species z1   yours  ✓
 P2  tomograms {TS_11}          tmextractcand__2 ← templatematching__2 ← denoisepredict  species z  @lab   (D7)
 P3  tomograms {…}              owner: a colleague                                     refused (NOT_WRITABLE)
        │ in each writable project:
        │   add_job(subset, after=<the member TM's tomogram source>) · set_params(ts_names=…)
        │   [add_template(...) if one template for all, D8]
        │   add_job(templatematching, species=<local id>, after=<new subset>) · set_params(…)
        │   add_job(tmextractcand, species=<local id>, after=<new TM>)
        │   plan_run(P, [subset, TM, candidates]) · run(P, [same])
        ▼
 add_kitchen_members(H, Z, [(P1, tmextractcand__3), (P2, …)])  → the new picks join the pool beside the old ones
```

- **`list_kitchen_subsets(home, species)`** (read, new): the subsets with their names and counts.
- **`get_kitchen_subset_sources(home, species, subset)`** (read, new). It is built on the per-member grouping in
  `export.plan`. For each source project it returns:
  - path, label, owner, and whether it is writable;
  - the local species id and its selected template;
  - the tomograms;
  - the member's chain: Pick candidates ← TM ← the TM's tomogram source. A hand-curated pick list has no TM; its
    tomogram source is the job whose directory holds the frame's reconstruction;
  - anything missing, such as a reconstruction that is gone.
- **The per-project steps use the U1 verbs.** Only the read and the pool update cross projects.
  - `run` names its jobs, so jobs someone else staged by hand on that roster are not submitted along with them.
  - Each project's chain stands alone, so one refusal does not stop the others. Call `plan_run` everywhere first, then
    `run`.
- **`add_kitchen_members(home, species, [(project, instance)])`** (write, new; `members.offered_members` +
  `service.add_members`). The new Pick candidates join the pool, and the kitchen shows old and new picks of the same
  tomograms side by side.
- **Two servers.** Several projects are written in one go, so the two-servers risk (§13.2) applies to each of them.

## 8. Later: the session inside CryoBoost

- **What it needs:**
  - the same verb table, called in-process;
  - a chat panel;
  - approvals shown in the page;
  - a transcript kept per project.
- **Credentials:**
  - An Anthropic API key per person, or a lab key billed to the lab, read by that person's own server (roadmap 23:
    one server per person, running as them).
  - Claude Code's own login cannot be used. The Claude Code legal page: "Unless previously approved, Anthropic does not
    allow third party developers to offer claude.ai login or rate limits for their products, including agents built on
    the Claude Agent SDK."
  - These are the credentials there would be to keep track of.
- **Two ways to build it.** Choose when this is scheduled.
  - **The Claude Agent SDK** (Claude Code's loop as a library), restricted to the CryoBoost tools: `strict_mcp_config`,
    `setting_sources=[]`, `allowed_tools=["mcp__crboost__*"]`, built-in tools disallowed, and `can_use_tool` routing
    approvals to the page.
  - **The Messages API's tool runner** over the verb table: smaller, with no built-in tools to strip.

## 9. Decisions

| # | Question | Recommendation |
|---|---|---|
| D1 | What the agent talks to | A stdio MCP forwarder in front of the REST verbs in the server (§5.1) |
| D2 | What "preprocessing with defaults" runs | A `preprocessing` protocol the maintainer exports from a project that worked: importmovies → fsMotionAndCtf → aligntiltsWarp → tsCtf → tsReconstruct (denoise?). Not parameters chosen here |
| D3 | Which acquisition values must come from the data or from you, and which may be a site value | Must: pixel size, dose, voltage, tilt axis. Site (conf.yaml): Cs, amplitude contrast. Code defaults: never |
| D4 | The tilt filter in an unattended run | Not in `preprocessing`. DL auto when it is wanted (needs `tilt_filter.models` in conf.yaml) |
| D5 | Protocol pins vs values from the data | Fields that depend on the data cannot be pinned, and the validator says why |
| D6 | Packaging | A user-scope entry for the maintainer now; the in-repo plugin for the lab later |
| D7 | Which source projects U2 may write | Yours (`owner`, or `created_by` when there is no owner) and `@lab`. Others are refused |
| D8 | The template for U2 | Each project keeps its own selection, unless you name one template to push to all of them |
| D9 | The record of agent writes | Console events + `Logs/agent_api.jsonl`. Nothing extra in the UI |
| D10 | Picking again without matching again | Not now (Pick candidates cannot take a tomogram list); match again through a Subset |
| U2 | "Subset Y of species Z globally" | **Decided 2026-11-04:** an STA-kitchen subset; the work lands in the source projects |

## 10. Stages

Every stage is one commit or a short series, and leaves the app runnable. The checks are run once at the end (§12),
as the standing decision says.

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
  - Adding each job type from the roster behaves as before: ids, prerequisites, species fields, pins, toasts.
  - A TM made from a protocol gets the same species fields as one added from the roster.

### S3 — The parameter door, and fields that refresh (§3.3, §3.4)
- *Success:*
  - A value set through the door shows on an open job page within 3 s. This includes template, symmetry and SLURM
    values.
  - Tabbing through a SLURM field without typing writes nothing.
  - A bad value comes back naming the bound it broke.
  - A write to a running job comes back as `JOB_FROZEN`.

### S4 — Creation with sources (§3.5)
The UI behaviour change is its own commit.
- *Success:*
  - Mdocs without PixelSpacing → Create says "pixel size: not in the data — enter it".
  - With a value typed in, the project shows the source "user".
  - Cs shows "site".
  - A gain reference survives a Create without a scan.
  - Creating 300 tilt series leaves no loop stall over 1 s in the log.
  - The protocol validator refuses a pinned `tomo_dimensions`.

### S5 — The API (§4, §3.6)
- *Success:*
  - `curl -s -X POST -H "Authorization: Bearer $(cat ~/.crboost/token)" localhost:P/api/v1/get_server_info` answers.
  - A write verb sent with only the cookie is refused.
  - `/api/v1/tools` lists every verb with its schema.
  - A write shows in an open tab within 3 s.

### S6 — The Claude Code side (§5)
- *Facts first:*
  - Is `uv` on the PATH that Claude Code sees?
  - Does Claude Code re-list tools after `/mcp` reconnects?
- *Success:*
  - `claude mcp list` shows `crboost` connected.
  - Read verbs run without a prompt.
  - `run` always asks.
  - Pointed at a port held by another uid, the forwarder refuses and sends nothing.

### S7 — The preprocessing protocol (D2)
The maintainer's step: export it from a project that worked, edit what it should pin, then
`crboost_protocol.py validate preprocessing`.

### S8 — The kitchen verbs (§7)
- *Success:*
  - On a home project whose pool spans two projects, `get_kitchen_subset_sources` lists both, with their tomograms and
    chains.
  - After the per-project runs and `add_kitchen_members`, the kitchen shows the new members.

## 11. Not doing
- The session inside the UI (§8).
- Several people per server (roadmap 23).
- Jobs that span projects.
- Parameter sweeps or grids (protocols are shapes, not harnesses).
- New scientific defaults.
- Through the API: deleting into Trash, approving the tilt-filter review, curation.
- Fixing the silent input picks that `plan_run` reveals (§4.3); they get their own roadmap if they bite.

## 12. Runtime checklist (the maintainer's step, after the arc)
1. **Two tabs on one project.** Add, remove and Run from either tab; both show one roster (S1).
2. **A dataset without PixelSpacing.** The landing page refuses Create until the value is entered, and the source chips
   read mdoc / site / user (S4).
3. **The API with curl** (the S5 checks), then with the cookie only on a write → refused.
4. **In Claude Code, from a data directory**, with no repo files open:
   - "make a project from here and run preprocessing";
   - answer its questions; approve `run`;
   - open the `ui_url`. The roster, job pages and status are the same as for a hand-made project.
5. **While it runs:**
   - Edit a SLURM value in the browser, then ask the agent for that job's SLURM; it shows your edit.
   - Have the agent set a parameter on a Scheduled job; the open page shows it within 3 s.
6. **Force a failure** (an impossible walltime). The agent reports the failing log lines and does not resubmit.
7. **Run from the browser and the agent at the same moment.** One submission.
8. **U2 on a home project whose Z pool spans two of your projects:**
   - "take subset Y and match it again";
   - the plan names both projects; approve;
   - the kitchen later shows the new picks beside the old ones.

## 13. Risks
1. **The agent also has Bash.** It could edit project files or call `sbatch` directly. The instructions say not to and
   the deny rules stop the edits. Under the in-memory rule, such an edit is lost on the next save anyway.
2. **Two servers writing one project** (dev and release). This is not new; roadmap 23 stage 3 (the lease) fixes it. U2
   writes several projects at once, so keep one server running while the agent works.
3. **GPU hours.** `run` always asks, and `plan_run` names the resources.
4. **Large projects.** Answers carry counts, and per-tilt-series detail is paged.
5. **A server older than the checkout.** The forwarder reads the tool list from the running server, so the newer verbs
   are simply absent until a restart.
6. **The token leaving the machine.** It is never in a tool result or a config file. The uid check keeps it off other
   users' ports.

**Modern-Python weave-in.**
- The verb table as frozen `dataclass`es with `kind: Literal["read", "write"]`, and pydantic input models (FastAPI turns
  them into the schemas).
- `needs` items as a discriminated union (`kind: Literal["value", "choice", "pin"]`).
- `ProjectSpec` and `DatasetScan` as pydantic models with an `AcqValue` (`value`, `source: StrEnum`, `mixed`).
- The new `ErrorCode` members join the existing `StrEnum`.
- A static self-test (`tests/`): every verb's schema builds, and `/api/v1/tools` matches the table.

## 14. Stage log

*(empty)*

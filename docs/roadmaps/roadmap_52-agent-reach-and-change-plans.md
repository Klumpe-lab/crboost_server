# Roadmap 52 — Agent reach and change plans: a user setting for hand-made projects, deletions, one approval per plan

**Status:** scoped and decided 2026-11-16; S1–S5 built the same day (§12), checked on toys and one read-only
proposal on the live server, not run in a browser. Follows roadmap 42 (the agent API) and changes two of its
decisions: D7 (the API changes only projects it made) and the per-call approval of §5.3. Group-writable
projects are the default (D5). Another person stopping or adding to a running project you made for them (§13) is
diagnosed, not built.
Plan review in the terminal and the page as an editor (§14, S6) scoped and built the same day (D6, D7 confirmed),
checked on real projects read only and in NiceGUI's user simulation, not yet in a browser.
**Pickup: the S6 check in a browser and one terminal-confirmed plan; §13's decision (ask HPC for SLURM coordinator
rights, or self-stopping jobs).**

**Ask (maintainer, 2026-11-16).**
1. Through the API, an agent may also **delete** (jobs, files) and **edit projects it did not make**, the user's own
   hand-made ones.
2. That reach is **the user's choice**, a setting ("Allow agents to access and edit my projects") in a new
   **User settings** button on the toolbar, and every API write respects it.
3. What the agent puts in front of the user before acting is **one summary per project of everything it will do**,
   visual and scrollable, approved once, not a long list of single calls each approved on its own.
4. Can a project's ownership be transferred completely to another user, given that nobody here is an admin? (§6)

## 0. What is there today

- **The gate.** `api/router.py:91` refuses every write verb on a project whose `ProjectState.made_by_api`
  (`services/project_state.py:824`) is false, with `NOT_WRITABLE` and its `ui_url`. Reads work on any project
  (roadmap 42 §2.12, D7).
- **Approvals.** Claude Code allows the read verbs by rule (`mcp__crboost__get_*`, `list_*`, `inspect_*`, `plan_*` in
  `~/.claude*/settings.json`). Every other write verb prompts once per call. `create_project`, `run`, `predict_tilts`,
  `stop`, `cancel_job`, `cancel_leftovers` carry `asks=True` (`api/verbs.py:29`), which becomes
  `_meta["anthropic/requiresUserInteraction"]` (`api/router.py:54`): they prompt on every call even under an allow
  rule. So a preprocessing chain is ~15–25 prompts (eight `add_job`, a few `set_params`, `set_slurm`, `run`), each
  showing one tool call's raw arguments, and nothing ever shows the whole of what is about to happen.
- **Deletions are not in the API** (roadmap 42 §4.3 "Not in v1"). The UI's deletions and their doors:
  - a job that ran → its directory to `Trash/`: `backend.delete_job` (`backend.py:300`), then the roster takes it off
    (`ui/pipeline_builder/pipeline_roster.py:866-879`, and `job_tab_component.py:521`). The second half lives in
    `ui/`. A job that has not run: `pipeline_edit.remove_job` (`services/pipeline_edit.py:312`), already a verb;
    it refuses a job that ran (`:324`);
  - a species: `species_admin.delete_species` (`services/species_admin.py:49`); a template or mask file:
    `template_service.delete_file_async` (`services/templating/template_service.py:82`);
  - Trash entries, permanently: `backend.clear_project_trash` (`backend.py:2256`);
  - a project, permanently: `backend.delete_project` (`backend.py:2232`);
  - pick lists, particle sets, segmentation imports, kitchen subsets: judging, kept in the UI (roadmap 42 §2.3).
- **User preferences** are `UserPreferences` (`services/configs/user_prefs_service.py:31`), one per server process
  (the server runs as its user), kept in `app.storage.user` and mirrored to `~/.crboost/prefs.json`. Gotcha: the
  service's `prefs` property returns fresh defaults until a tab has loaded them (`:241-244`), so a server nobody
  has opened a tab on reads every preference as its default.
- **The toolbar** is the landing status strip: `house_button("Protocols")` and the gear `icon_button("settings")`
  for the server configuration (`ui/landing_status_strip.py:298-308`). The workspace has no toolbar of its own.
- **Ownership** is metadata: `created_by` (immutable) and `owner` (mutable, `SHARED_OWNER = "@lab"`),
  `effective_owner` (`project_state.py:915`); `backend.transfer_project_ownership` (`backend.py:2278`) and the
  roster's Transfer… dialog (`ui/projects_overview.py`) change it. Files on disk keep their Unix owner.

## 1. The setting

- `UserPreferences.agents_may_edit_my_projects: bool = False`. Off by default: turning it on is the user's act.
- **User settings** button on the status strip, beside the gear: `icon_button("person", tooltip="User settings")`.
  It opens a small dialog in the house vocabulary (`house_field`/a toggle row, `[Cancel] [Save]` footer). One row
  today: **Allow agents to access and edit my projects**, with the tooltip "Agents (the CryoBoost API, e.g. Claude
  Code) may change and delete in projects you own, not only those they made. Off: they change only projects they
  made. Reading is open either way." The dialog is the home for later per-user switches; the gear stays the server
  configuration.
- The API reads the flag from the server's singleton, loaded from `~/.crboost/prefs.json` when no tab has loaded it
  (fix the gotcha above in `get_prefs_service()`: load from the file on first use). The dialog saves through
  `save_to_app_storage`, which writes the file too. No verb sets the flag, ever: it is the user's switch, not the
  agent's.
- The workspace shows the state where it matters: on a hand-made project the agent can now change, the project
  title line carries a small `agent` chip (tooltip: "Agents may edit this project — User settings"). Nothing when
  off.

## 2. The gate: who may write where

One function, `api/reach.py: can_write(state) -> str | None` (None = yes, else the reason), used by `dispatch` in
place of the `made_by_api` test at `router.py:91`:

| Project | Flag off | Flag on |
|---|---|---|
| made by the API | write | write |
| hand-made, `effective_owner` is me | `NOT_WRITABLE`: "made by hand; turn on *Allow agents…* in User settings, or make the change in the UI" | write |
| shared (`@lab`) | `NOT_WRITABLE` | per D2 |
| someone else's | `NOT_WRITABLE`: "belongs to X" | `NOT_WRITABLE`: "belongs to X" |

- "Me" is the server's user (`getpass.getuser()`), as in the roster's *Only mine*.
- Read on every call, so switching the flag off stops the next write, including the next step of a plan (§4).
- `get_project` and `list_projects` report `agent_writable` and, when false, why. `list_projects` keeps
  `made_by_api`.
- The instructions and the skill: "The API changes the projects it made and, when the user has allowed it in
  User settings, the user's own; `NOT_WRITABLE` says which. Never ask the user to change the setting for you
  unless they asked you to edit that project."
- `agent_api.jsonl` records keep working as today; a write into a hand-made project also says so in its console
  event (`"%s · agent (hand-made): %s"`), so the maintainer sees agent edits to their own work at a glance.

## 3. Deletions through the API

The UI's deletions get verbs, through the same doors, under the gate. Parity, not more: no free path deletes.

| Verb | Door | Kind |
|---|---|---|
| `delete_job(instance_id)` | new `pipeline_edit.delete_job`: `backend.delete_job` + the roster's removal, moved out of `ui/` (both UI callers use it) | to Trash, recoverable |
| `delete_species(species_id)` | `species_admin.delete_species` | removes the species' registered files |
| `delete_template(species_id, path)`, `delete_mask(…)` | `template_service.delete_file_async` + the registry entry, as the species page does | file + sidecar |
| `clear_trash(names=None)` | `backend.clear_project_trash` | permanent |
| `delete_project(project)` | `backend.delete_project` | permanent |

- `delete_job` answers the orphans the roster dialog warns about (`pipeline_roster.py:840-857`): the downstream
  jobs whose inputs break.
- The permanent ones (`clear_trash`, `delete_project`) run **only as a step of an approved plan** (§4): called alone
  they refuse with "put it in a plan". D3.
- Not offered: pick lists, particle sets, segmentation imports, kitchen subsets and groups (judging, roadmap 42
  §2.3), and anything outside a project.

## 4. Change plans: one summary, one approval

The agent stops making writes one by one. It sends the whole change as a plan; the server checks it, renders it
per project on a page, the user approves there once, and the server carries it out.

```
 agent                                 server                                    user
 plan_changes(steps=[…])  ───────►  check every step (input model, gate)
                                       dry-run the state doors on a copy
                                       store plan p-7f3a (1 h)
                             ◄───────  per-project lines + ui_url  ─────────►   terminal: 3 lines + link
                                                                                 opens /agent/plan/p-7f3a
                                                                                 scrolls, [Approve]
 apply_changes(p-7f3a)       ───────►  approved? unchanged since? gate per step
                                       run the steps in order (a background task)
                             ◄───────  task id → get_task: step by step
```

### 4.1 The verbs
- `plan_changes(steps: list[{verb, args}], title: str)` — a read in the §2.5 sense (it changes no project), so
  it falls under no prompt. Every step is any write verb with its arguments as it would be called alone.
  - Each step's input model is validated and its project passed through the gate now; a refused step makes the
    whole plan come back refused, naming the step.
  - The state doors are pure on a `ProjectState` (`pipeline_edit.add_job/remove_job`, `job_params.set_params`,
    `set_slurm`, `set_input`, `set_tilt_filter`, `exclude_tilt_series` minus its registry save). The plan runs them,
    in order, on a deep copy of each project's state (`model_copy(deep=True)`), so later steps see earlier ones
    (`set_params` on a job the plan adds), `needs` and refusals surface before anything happens, and the diff is
    exact. Steps that touch disk or SLURM (`create_project`, species imports and templates, deletions, `run`) are
    not simulated; they are described, with what can be read now: sizes from the disk-usage service, the orphans,
    and for `run` the dry pass of `plan_run` over the copied state.
  - A `needs` item from any step comes back as the plan's need; the agent asks the user and proposes again.
  - Answer: `plan_id`, `ui_url`, and one line per project (`copia_api_20261110 · +3 jobs, 7 parameters, run 6 jobs
    (≈14 GPU-h)`; `412_SK_… · delete 2 jobs (38 GB to Trash)`; `old_test · DELETE PROJECT (1.2 TB, permanent)`).
- `get_plan(plan_id)` — read: its state (proposed, approved, running, done, failed, expired) and, once applied, each
  step's result.
- `apply_changes(plan_id)` — write. Refuses unless the plan was approved on its page; refuses when a project in it
  changed since the proposal (its state's `modified` stamp, or roster/params fingerprint) and asks for a new
  proposal; refuses after 1 h. It runs the steps in order through the same dispatch as single calls (each writes its
  `agent_api.jsonl` line with the `plan_id`), as a background task (`get_task`). The gate is checked again per step.
  The first failed step stops the plan; the answer names what ran and what did not. Nothing is rolled back: each
  step is a door call that stands on its own, as in the UI.
- Plans live in server memory (and `~/.crboost/agent_plans/<id>.json` for the page after a restart; an approval does
  not survive a restart).

### 4.2 The page: `/agent/plan/<id>`
One scrollable page, the house look (Harbor Mono, one box deep):
- **Header:** the plan's title, who proposed it (the agent, via the API), when, expiry, totals over all projects.
- **One section per project** (title stripe + content), in step order:
  - *Roster strip:* the project's jobs in pipeline order as small chips; existing grey, added green, deleted red
    struck through, cloned with an arrow from their source. The same visual as the roster, read-only.
  - *Parameters:* one table per job, `parameter | now | after`, only the changed rows; SLURM likewise; pinned inputs
    as `input | now | after`.
  - *Species and templates:* what is imported, made, selected, deleted.
  - *Deletions:* each with its size and kind (Trash = recoverable, permanent = red), and the orphans it leaves.
  - *Run:* the jobs that will be submitted in order, their resources and the plan's GPU-hour estimate, where the
    tilt-filter review will hold it.
  - Hand-made projects carry the `agent` chip so it is plain the agent is editing the user's own work.
- **Footer:** `[Reject] [Approve]`, Approve as `danger` when anything permanent is in it, with the permanent items
  repeated beside it. Approve and Reject show in the page and in `get_plan`.

### 4.3 Claude Code side
- Allow rules gain `mcp__crboost__plan_changes` (a read already matches no rule; add it explicitly) and
  `mcp__crboost__apply_changes`: the approval is the page's, so the terminal does not ask again (D4). Single write
  verbs keep prompting as today, `asks` included.
- Instructions and skill: "Any change of more than one write goes through `plan_changes`. Show the user the
  per-project lines and the `ui_url`, nothing else; wait for them to say it is approved (or call `get_plan`), then
  `apply_changes` and follow the task." The long recaps the skill asks for today (every value before
  `create_project`, every job before `run`) move to the page: `create_project` as a plan step shows every
  acquisition value in the page's section for the new project, with where each came from (`found_in`) and which the
  user gave (roadmap 42 D3 still holds: the agent still asks for the missing ones before proposing).

## 5. Boundaries
- `api/reach.py` and the plan store (`api/plans.py`) stay in `api/` (no `nicegui`). The page is `ui/agent_plan_page.py`,
  reading the plan store through the backend; Approve calls a backend method, not the API.
- The page is a NiceGUI route under the TokenGate like the others; Approve is a user action in the browser, the only
  way a plan becomes approved.

## 6. Transferring a project to another user completely (the question)

**Confirmed: not possible in place without admin rights.** Changing a file's Unix owner needs root
(`CAP_CHOWN`); a normal user cannot give files away, only change their group among groups they belong to. So the
files of a project stay owned by whoever wrote them (and count against their quota) however the metadata says.

What is possible without an admin:
1. **What exists:** the `owner` field and Transfer… in the roster. The project shows under the new owner and their
   *Only mine*; with this roadmap, their agent may edit it when their flag is on (it is theirs by `effective_owner`).
   On this filesystem that is already enough to work on it: the project directories are `drwxrwxrwx` under a
   `klumpe.grp` base (checked on `/groups/klumpe/_crboost_data_api`), so the new owner's server can add jobs and write
   new job directories, which are then owned by them. Files written before stay the old owner's; the umask (022)
   leaves them read-only to others, so the new owner cannot rewrite those in place. `project_params.json` is saved by
   temp file + rename (`project_state.py:1218-1223`), which a writable directory allows, so it passes to the new owner
   on their first save; star files and job outputs written in place are what to check (S5).
2. **A real handover by copy:** the receiver copies the project, and every copied file is theirs. The project's
   absolute paths (project_params.json, star files, `job.star`, driver args) tie it to its directory, so the copy has
   to land at the same path: the giver renames the directory aside, the receiver copies it back to the original name,
   the giver deletes the aside copy. Two people, a pause with nothing running, the disk twice for the time of the copy,
   and as long as a copy of the whole tree takes (TBs for a large project). Could be a guided action later ("Hand over…"
   in the roster: the giver's server parks it, the receiver's server copies it in, each a background task); not
   scoped here unless asked.
3. **Group-writable instead of handed over:** the giver runs `chmod -R g+rwX` on the project (and `g+s` on its
   directories). Then everyone in the group can edit everything, the old files included. Ownership and quota stay
   the giver's. The cheapest and probably the most useful for "lab members fix each other's projects".

Decided (D5): group-writable by default for what is made from now on. On `/groups/klumpe` the existing project
trees already read `rwxrwxrwx` to everyone (Isilon ACLs over NFS), so no action for existing projects was built.

## 7. Decisions

| # | Question | Recommendation |
|---|---|---|
| D1 | Does the flag also gate **reads** of hand-made projects ("access")? | **Decided 2026-11-16: no**, reads stay open (U2's kitchen sources and `tomograms_from` read hand-made projects); the setting's description says so |
| D2 | Shared (`@lab`) projects under the flag? | **Decided: no**: a shared project is everyone's; one member's switch does not open it |
| D3 | Permanent deletions (project, Trash) | **Decided:** only as a plan step, never as a single call; Approve turns red for them |
| D4 | Where a plan is approved | **Decided:** on its page only; `apply_changes` is allow-listed and refuses before the approval |
| D5 | Ownership transfer | **Decided:** no chown without root (§6); every project group-writable by default instead: the launcher's `umask 002` (the server's files and, through sbatch, its jobs'), new project folders `g+rwxs`. The maintainer's real need is §13 |

## 8. Stages

### S1 — The setting and the gate (§1, §2)
- `UserPreferences.agents_may_edit_my_projects`; the prefs service loads from the file on first use.
- User settings button + dialog on the status strip; the `agent` chip on the title line.
- `api/reach.py:can_write`; `dispatch` uses it; `agent_writable` in `get_project`/`list_projects`; instructions and
  skill text; the console event for hand-made writes.
- *Success:* flag off → a write on a hand-made project of mine is `NOT_WRITABLE` naming the setting; on → it lands
  and the open tab shows it within 3 s; someone else's project refused either way; reads unchanged.

### S2 — Deletion doors and verbs (§3)
- `pipeline_edit.delete_job` (both UI callers move to it); verbs `delete_job`, `delete_species`, `delete_template`,
  `delete_mask`, `clear_trash`, `delete_project`; the last two refuse outside a plan.
- *Success:* a job that ran, deleted through the API, is in `Trash/` and off the roster in the open tab, orphans
  named; the roster's Delete behaves as before.

### S3 — Plans: store, dry run, verbs (§4.1)
- `api/plans.py` (store, expiry, fingerprints); dry run on deep-copied states; `plan_changes`, `get_plan`,
  `apply_changes` (background task, stop at first failure, gate per step, `plan_id` in the records).
- *Success:* a plan of add_job ×8 + set_params on two of them + run, on a toy project, comes back with exact
  per-project lines and no state change; a plan with an invalid parameter is refused naming the step;
  `apply_changes` before Approve refuses; after it, the steps run and the project matches the same calls made one by
  one.

### S4 — The page (§4.2)
- `/agent/plan/<id>`: header, per-project sections (roster strip, parameter and SLURM tables, species, deletions,
  run), footer Approve/Reject; refresh while running (FingerprintedView on the plan's step states).
- *Success:* a three-project plan scrolls as one page; Approve flips `get_plan`; a permanent deletion makes Approve red.

### S5 — Claude Code side and ownership (§4.3, §6)
- Allow rules (`docs/agent.md` snippet and the maintainer's settings), skill and instructions rewritten around
  plans; remove the long recaps.
- "Make group-writable" beside Transfer… (D5), as a background task; check what the new owner can and cannot
  rewrite in a transferred project without it.

## 9. Not doing
- Any verb that sets the flag; any write to someone else's project.
- Deleting picks, particle sets, segmentation imports, kitchen subsets (judging stays in the UI).
- Rollback of a half-applied plan.
- Changing Unix ownership (§6); the copy handover (§6.2) unless asked.

## 10. Runtime checklist
1. Flag off: the agent is refused on a hand-made project with the setting named; on: the same request lands; off
   again mid-plan: the next step is refused.
2. "Redo the alignment of <hand-made project> at 250 nm and delete the old one": one plan, one page, Approve, done;
   the old job in Trash; the roster agrees in an open tab.
3. A preprocessing request on fresh data: questions for the missing values in the terminal, then one page with every
   value and the whole chain; one approval instead of ~20 prompts.
4. `delete_project` alone → refused; in a plan → red Approve; after it the project is gone from the roster.
5. A transferred project: the new owner's agent with the flag on edits it; which old files it cannot rewrite.

## 11. Risks
1. **The agent also has Bash** (roadmap 42 §12.1): with the flag on it is no more able to edit files behind the
   server than before; the deny rule on `project_params.json` stays. A deny rule on `//**/.crboost/prefs.json` keeps
   it from flipping the flag in the file (the server would only see that at its next start anyway).
2. **The dry run drifts from the real run** for steps it only describes; the page marks those as "not simulated".
3. **A stale plan:** the fingerprint check makes the user propose again rather than apply over a project that moved.
4. **A server without an open tab** read the flag as off until the S1 prefs fix; after it, from the file.

## 12. Stage log

**S1–S5, 2026-11-16 (built; toys and one live read-only proposal).**
- *The setting.* Not in the browser-synced preferences: `app.storage.user` is reloaded from whichever browser
  opens a page last, and a permission switch must not flip back with an old tab. It is
  `services/configs/user_settings.py` (`~/.crboost/user_settings.json`, written 0600, re-read when its mtime
  moves, ignored unless owned by the user). The person icon on the status strip opens `ui/user_settings_dialog.py`.
  The title-line `agent` chip of §1 was not built (the workspace has no project title line to carry it).
- *The gate.* `api/reach.py`: `refusal_for` (the rule on what a project records) and `write_refusal` (per call,
  in a thread). The owner is `effective_owner`, else the Unix owner of `project_params.json`, as the roster has
  it. `router.run_verb` is the one runner for single calls and plan steps (gate, handler, save, record);
  `get_project`/`list_projects` carry `agent_writable` and `not_writable_because`. A write into a hand-made
  project says so in its console event; plan steps carry `plan_id` in `agent_api.jsonl`.
- *Deletions.* `pipeline_edit.delete_job` (backend's Trash move + the roster removal); the workbench's template
  and mask deletes moved to `species_admin.delete_template` / `delete_mask`, which the workbench now calls. The
  roster's own Delete still calls `backend.delete_job` + `remove_instance_from_pipeline` (its UI follow-ups need
  the panel). Verbs `delete_job`, `delete_species`, `delete_template`, `delete_mask`, `clear_trash`,
  `delete_project`; the last two are `plan_only`. `delete_project` refuses while a job is live and records only
  the console event (nothing is left to record in).
- *Plans.* `propose_changes` was renamed **`plan_changes`**: the self-test holds every read to the read
  prefixes, and the maintainer's `plan_*` allow rule then covers it. `ProjectState.from_data` split out of
  `load`, and `detached_copy()` (a save + load in memory) is what a plan rehearses on;
  `pipeline_orchestrator.plan_run(project_dir, state=…)` plans on it. Rehearsed: add_job, remove_job,
  delete_job, set_params, set_slurm, set_input, reset_inputs, run; the rest described. The fingerprint covers
  the roster, every job's user parameters, SLURM and input overrides, and the species' files and selections,
  not statuses, so a running pipeline does not stale a plan. `apply_changes` runs the steps as a background
  task, waits for background steps (`create_project`), stops at the first failure. Store and page:
  `services/agent_plans.py`, `ui/agent_plan_page.py` (`/agent/plan/<id>`, a `FingerprintedView` on status and
  step states, 3 s).
- *Claude Code side.* `api/instructions.md` (3.1 kB of 4 kB), the skill and `docs/agent.md` say: more than one
  write is one plan; show only its project lines and link. Allow rule to add: `mcp__crboost__apply_changes`;
  deny rule: `Edit(//**/.crboost/user_settings.json)`.
- *Group-writable.* `crboost` launcher: `umask 002`. `shared_files.make_group_writable_dir` on a new project
  folder (`g+rwx`, setgid).
- *Checks.* ruff clean on the touched files (the tree's six old errors remain), boundaries clean, self-test 4
  passed. Toy: the rule in its five cases, the settings file 0600 and read back, a detached copy of a real
  project independent of it with its jobs' back-references on the copy, the fingerprint stable across a copy,
  file mode 664 under the new umask. Live (port 24326, read only): `list_projects` on `_crboost_data_api` gives
  `agent_writable`; a four-step plan on `AGG412_20260311_MyrGFP_Grid2_rerun` (clone Reconstruct, 12 Å on the
  clone, delete `warpExport__2`, run) answered `+1 job, 1 parameter, delete 1 to Trash (65.0 GB), run 1 job`,
  and the project on disk was unchanged.
- *Not checked:* the page and the dialog in a browser; `apply_changes` end to end; a hand-made project with the
  switch on.

## 13. Another person working in a running project you made for them (diagnosed 2026-11-16, not built)

The maintainer's real need behind §6: someone cannot queue, stop or delete in a project made for them while it
runs. File modes are not the cause on `/groups/klumpe` (everything reads `rwxrwxrwx`), and SLURM lets a job
depend on another user's job (`sbatch` man page: dependent jobs "may even belong to different users"). Two causes
are left:
1. **Stop and cancel.** The running chain's SLURM jobs belong to whoever submitted them. Only the owner, an
   operator, an admin or a coordinator of the account may `scancel` them; the maintainer is no coordinator
   (`sacctmgr show user withcoord`). So the other person's Stop is refused by SLURM.
2. **Two servers, one project.** Each person's server holds its own in-memory `ProjectState` and saves it whole.
   What the second person queues is written by their server, then overwritten by the first server's next save
   (a status reconcile marks it dirty), and both servers reconcile the project when it is under their base. This
   is roadmap 23's single writer (stage 3, the lease), not built.

Ways out, none built:
- **A. SLURM coordinator rights for the lab** (`sacctmgr add coordinator account=<lab account> names=<members>`):
  one request to HPC; then any member's server can stop any member's jobs. Simplest by far for cause 1.
- **B. Self-stopping jobs** (no admin): `qsub.sh` checks a stop marker in the project at start and from a watcher
  while it runs, and ends itself; this cluster sets `kill_invalid_depend`, so the afterok jobs behind it go with
  it. A stop from anyone's server writes the marker. It reaches queued heads only when they start (seconds).
- **C. The lease** (roadmap 23 stage 3) for cause 2: one server writes a project at a time; "Take over editing"
  hands it to the other person's server, whose jobs from then on are theirs.

Recommendation: A now (ask HPC), C next; B only if A is refused.

## 14. Reviewing a plan: in the terminal by default, the page as an editor (scoped 2026-11-16, not built)

The maintainer approved the first real plan (`p-449dfe46`, a demo) and judged the page: it reads like a JSON dump.
Ask: review in the terminal by default (a long scrollable overview, a choice to confirm), and keep the page but make
it a structured editor.

### 14.1 Why it reads as a dump
- Every step a plan does not rehearse is shown as `verb {json args}` (`api/plans.py:_what`), and a new project's
  steps are never rehearsed (the project does not exist), so a new-project plan is nothing but such lines.
- "Made with" prints the raw `create_project` input (`tomograms_from` as a list of dicts).
- The settings table names fields by attribute (`sample_thickness_nm`, `preset`) with raw values; a SLURM change
  shows the side effect `preset → Custom` as its own row.
- The steps list repeats all of it a second time.

### 14.2 One plan view, two renderings
`services/agent_plan_view.py` turns a `Plan` into a structured view once, in words, and both the terminal and the
page render that, so they never disagree:
- **Steps in words**, one sentence per verb: "Add Template Match for 412_particle after Reconstruct", "Import the
  species 412_particle from the lab catalog", "Make a project from 23 tomograms of AGG412_20260204_412_rerun
  (Reconstruct, job008)", "Delete Alignment (job005) to Trash, 4.1 GB". A verb without a sentence keeps its name
  and lists its arguments as `name: value` lines, never JSON.
- **Fields by their titles** (`model_fields[name].title`, the job tab's labels) with units, under the job's display
  name; booleans as on/off; a value equal to the code default says "default". SLURM side effects (`preset` going
  Custom) fold into the row that caused them.
- **New projects rehearsed too:** a blank `ProjectState` seeded with the spec's values (or the `tomograms_from`
  sources' values, as `check_job_sources` reads them) takes the roster and parameter steps, so a new project shows
  its roster and parameters like an existing one. Steps that need the project's files (imports, templates) are
  sentences.

### 14.3 The terminal, by default
- `plan_changes` answers with `overview`: markdown the server renders from the view, per project a heading, the
  roster as one line (`Import · Motion & CTF · +Template Match (412_particle) · −Alignment`), a settings table
  (`Job | Field | Now | After`), deletions (`What | Size | Trash or for good`), the run (`Job | Partition | GPU | Time`),
  and the new project's values. Claude Code renders markdown tables; the overview is as long as the plan and
  scrolls with the conversation.
- The agent prints `overview` verbatim (the skill says so: no summary of its own, nothing left out) and then calls
  a new verb, `apply_changes_now(plan_id)`, which carries `asks`: Claude Code's permission prompt (a select: yes /
  no, tell Claude what to change) is the one confirmation, and the harness enforces it, so the agent cannot
  approve on the user's behalf. "No" plus a word ("drop the deletion", "250 nm") makes the agent propose again;
  "edit it on the page" gives the link.
- `apply_changes` (page-approved, allow-listed) stays for the page path; `apply_changes_now` refuses a plan with a
  permanent deletion (D3: those are approved on the page, where Approve is red).

### 14.4 The page, an editor
Laid out as the app's own job views, not as tables of a report:
- **Left, the plan's projects**, each with its roster as the roster rail draws it: one row per job in pipeline order,
  status dot, the mark (added, cloned from, removed, to Trash) as the row's tint and a small badge; a click selects
  the job.
- **Right, the selected job** as its job tab draws its parameters: the job-tab field grid (`_field_styles.py`, the
  110 px label column), each changed field showing the old value struck beside the new one, and **editable**
  (`house_number` / `house_select` with the param class's bounds and choices, through `job_params.check_value`).
  Then its SLURM section, likewise. A job the plan does not touch is shown read-only.
- **Deletions** as a list with a keep switch per item, size, Trash or for good, and what it leaves without input.
- **A new project:** its values as the landing page's acquisition grid, each with where it came from.
- **An edit rewrites the plan:** a changed value merges into the plan's `set_params` / `set_slurm` step for that
  job (adds one when there is none), a kept item drops its deletion step; the plan is rehearsed again and its
  `revision` moves. `get_plan` returns the revision and the overview, so an agent that is waiting shows the user
  the edited plan before applying. An edit after Approve sends the plan back to proposed.
- Footer as today: `[Reject] [Approve]`, red when something goes for good.
- Not on the page: adding jobs or species (the agent's or the roster's), and the run's job selection.

### 14.5 Decisions
| # | Question | Recommendation |
|---|---|---|
| D6 | The terminal confirmation | **Decided 2026-11-16:** `apply_changes_now` with `asks` (the harness's prompt, which the agent cannot fake) rather than the agent's own multiple-choice question, which the server cannot tell from the agent's word |
| D7 | What the page edits | **Decided 2026-11-16:** parameters, SLURM values, keeping or dropping a deletion or a step; not adding jobs |

### S6 — Plan review in the terminal and as an editor (§14)
- `services/agent_plan_view.py` (sentences, titled fields, folded side effects, new-project rehearsal); `overview`
  markdown in `plan_changes` and `get_plan`; `apply_changes_now` (asks, refuses permanent steps); the page rebuilt
  as rail + job editor with plan revisions; skill and instructions: print the overview, then `apply_changes_now`.
- *Success:* the demo plan (new project from tomograms, species, TM, Pick candidates) reads in the terminal as a
  heading, a roster line, a values table and the TM parameters, with no JSON anywhere; approving it is one prompt; on
  the page, changing TM's angular search and approving applies the edited value.

**S6 — built 2026-11-16.**
- `services/agent_plans.py`: the plan as the user reads it: per project `JobView`s (roster order, marks, the
  ones taken off where they stood) with `FieldView`s (title, kind, now → after, type, choices, bounds, help,
  editable), deletions carrying their step, `also` sentences, a new project's values; `Plan.revision`;
  `steps_with_value` / `steps_without` turn a page edit into new steps; `PlanStore.revise` rehearses them through
  the rebuilder the API registers (`api/router.py`), moves the revision and proposes the plan again.
- `services/agent_plan_view.py`: `overview(plan, url)`, the markdown the terminal shows; `show()` formats values
  (on/off, dashes, lists) so no JSON reaches the user.
- `api/plans.py`: `build()` (checks and rehearsal, shared by `plan_changes` and page edits); every step a sentence
  (`sentence()`); a new project rehearsed on a blank state seeded with its values (or the `tomograms_from`
  source's), a catalog species rehearsed from the catalog without copying files; a step that cannot be previewed
  on a new project is noted, not refused. `plan_changes` and `get_plan` answer `overview` and `revision`.
  `apply_changes_now` (asks; refused for permanent steps) and `apply_changes` share `_carry_out`; both take the
  revision the user saw and refuse an edited plan.
- `ui/agent_plan_page.py`: rail of projects and their rosters (marks, status dots, "n set"), the selected job as a
  field grid (job-tab label column, house fields, old value struck), project overview with values, settings,
  deletions with keep switches, the run, steps. Edits commit on blur, Enter or change, and the view adopts the
  new state instead of rebuilding under the field.
- Instructions, skill and `docs/agent.md`: print the overview as it is, then `apply_changes_now`; permanent or
  edited plans on the page.
- Checked: on real projects read only, the demo plan (new project from the 20260204 rerun's tomograms, 412
  species, TM with angular search 7, Pick candidates) renders as values, roster and the TM setting, all five
  steps rehearsed; a clone of the alignment at 250 nm with 96G and a Warp Export deletion shows the roster with
  the clone and the struck job, two settings and 47.7 GB to Trash. NiceGUI simulation of the page: overview,
  a job's pane after a rail click, Approve by click; an edit through `revise` merges into the plan's set_params
  step (revision 2, proposed again) and setting the value back drops the step. `apply_changes_now` refuses a
  plan with a permanent step, `apply_changes` refuses before approval and on a stale revision.
- Not checked: the page in a browser (blur commits, the adopt-instead-of-rebuild), a terminal-confirmed plan
  through Claude Code.

### S7 — the terminal confirmation shows the plan (built 2026-11-16)

Tried through Claude Code, S6's confirmation was the permission prompt: a bare yes/no naming the tool and its
arguments. The overview the agent printed before it is not part of that prompt, and in auto mode the prompt may
not appear at all. So the confirmation moved into the MCP server.

- `apply_changes_now` changes nothing on its first call: it answers `confirm`, the plan as plain text with aligned
  columns (`agent_plan_view.dialog_text`: per project the values, jobs +/−, settings now/after grouped by job,
  deletions, the run) and a one-time token bound to the plan's revision.
- The forwarder (`agent/crboost_mcp.py`) strips `confirm` from what the agent sees and shows the text in Claude
  Code's own dialog (MCP form elicitation, Claude Code 2.1.76+): one choice, "Apply the plan" or "Do nothing",
  defaulting to Do nothing. On Apply it calls again with the token (`ApplyNowIn.confirmation`, kept out of the
  tool's schema); anything else answers "declined, nothing changed". The token is spent on any try, and a page
  edit (new revision) voids it.
- No `asks` on `apply_changes_now` any more, and it joins the allow list: the dialog is the confirmation, and
  neither an allow rule nor auto mode answers it. An Elicitation hook could; `docs/agent.md` says not to write
  one for `crboost`. A client without elicitation (print mode) gets a refusal and the page.
- The agent no longer prints the overview before applying (instructions, skill); it prints it only when the
  dialog could not be shown.
- Checked: the dialog text for the demo plan and the alignment-clone plan; the token flow on the real plan code
  (wrong token, replay, right token, reuse); the forwarder over stdio with a stand-in server and an MCP client
  answering apply, keep, decline, and one without elicitation.
- Not checked: how Claude Code lays the dialog out (the docs don't say whether `message` is markdown; it is plain
  text on purpose), and a long plan's scrolling outside fullscreen.

# Roadmap 16 — Protocols workbench: introspection, and the run-is-a-project inversion

**Status:** S0–S5 **CODE-COMPLETE 2026-09-03**, built back-to-back on code-review confidence
(standing decision 2026-08-17: verify ONCE at the end); `ruff check .` clean, `check_boundaries.py` +
`import main` + everything in §9 owed by the maintainer. Continues roadmap 14
(`14-protocols-regression-harness.md`), whose services and CLIs it keeps; design of record for the
protocol format stays `FEATURE_recipes.md`.

## Prompt log (verbatim, 2026-09-03 — `q_test_harness.md`)

> Ok good find regarding the pydantic thing, but what i have a general grudge with is how poorly
> supported the introspection into this (that is, in the UX there is no way to view the parameters
> with which this will be dispathced, the running status, the results, the errors, the logs, the
> failures). Right now all the user sees is the ltitle "protocols" drawer on the main page..
>
> Let's work out an interface for each of these things and im thinking in general we should clip
> this protocols thing on any given project's tools drawer (next to the control cetner for artiax)
> so the user can see the progress/what's running/ switch between protocols etc...
>
> Any other good ideas on this front?
>
> This is just for the ease of use and introspection for me right now so we can contineu
> prototyping, but our main goal is still reproducing the copia workflow earnestly...

Decisions taken with the maintainer in the same session: **foot light → full-width view** (not a
control-center dialog); **UI-launched runs are created under the project base** as ordinary
projects (CLI runs stay under `local_data/runs/`).

## Before → After

**Before:** a protocol run is one coroutine (`run_protocol`) that applies the protocol into a
project three levels under `local_data/`, submits the afterok chain, polls `reconcile_afterok`
itself, evaluates, writes `run.json` / `report.md` beside the project and prunes older run dirs —
project included. The only UI is the landing-page dialog: a row per bundle, Run/Record into a
`BackgroundTask` whose tray card shows a progress string, and a "Report" list that prints report
paths as text. Nothing in the workspace knows a protocol exists; a project created from one is
indistinguishable from a hand-built project.

**After:** a protocol run **is a project**. `ProjectState.protocol_origin` says which protocol,
version and mode it came from; `<project>/protocol/` holds the frozen `protocol.yaml` and the
harness's `run.json` / `metrics.json` / `report.md`. The harness is three operations over that
project — `launch_run` (apply + provenance + deploy; seconds), `evaluate_run` (pure over disk and
in-memory status; idempotent; callable any time) and, for the CLI only, `wait_for_settle`. The
server's `PipelineMonitor` is the single reconciler of a UI-launched run; the workspace shows it
like any project, and a **Protocols view** (opened from a foot-of-rail status light beside the
ArtiaX one) shows the protocol's stages with dispatch params, live status, verdicts, metrics,
checks, failure signatures, per-stage logs and files, an "edited" chip wherever the job's value
no longer matches the protocol's, the run history of every protocol, and the launch / stop /
evaluate / promote-to-baseline / save-as-protocol actions. **Gain:** every question in the prompt
log has a place on screen, and the harness stops re-implementing observation, cancellation and
retention that the app already owns.

---

## 1. Verified facts (2026-09-03)

- `run_protocol` (`services/regress/runner.py:158-204`) owns apply → deploy → `_wait_for_chain`
  → `_evaluate_stages` → outputs → `prune_runs`; the project is `local_data/runs/<protocol>-
  <stamp>/project`, invisible to the hub scan and to `PipelineMonitor._discover_and_recover`
  (both walk one level of the base: `backend.py:1144-1164`, `pipeline_monitor.py:83-125`).
- **Two reconcilers.** `apply_protocol` registers the state (`services/protocols/apply.py:168`,
  `state_service.state_for`); `_submit_chain` sets `pipeline_active`; the monitor's tick iterates
  the in-memory registry (`pipeline_monitor.py:215-233`) and calls `reconcile_afterok` every 3 s
  while `_wait_for_chain` calls it every 15 s. `reconcile_afterok` has no lock and rebuilds
  `_afterok_absent_since` from each call's own `pending` (`pipeline_runner.py:493`). The
  `local_data` isolation (`config_service.py:206-213`) protects the startup scan only.
- `_attach_log` (`runner.py:432-437`) adds a FileHandler to the **root** logger for the run's
  lifetime: in-server that is every server log line for hours, and two runs cross-write.
- Tray cancel raises `CancelledError`, which `except Exception` (`runner.py:194`) does not catch;
  `finally` still writes outputs and prunes; **nothing scancels the chain.**
- `BackgroundTaskRecord` has no log channel (`services/background_tasks.py:56-86`); the dialog
  passes no `project_path` and never re-attaches (`BackgroundTask.existing/attach` exist).
- `ProjectState` has no protocol field; `load()` restores fields one by one (`project_state.py:1127`,
  see the P1.A restore at :1313-1322) — an unrestored field saves fine and vanishes on reload.
- The headless CLI backend constructs a `PipelineMonitor` but never starts it (`backend.py:100-104`,
  `main.py:158-168`); there was no `is_running` flag.
- `stop_and_cleanup` (`pipeline_runner.py:1133`) on the afterok branch ignores the ids argument,
  scancels every tracked live supervisor, marks them FAILED and clears `pipeline_active`.
- The workspace can open any absolute path (`backend.load_existing_project` does no base check);
  the only URL surface is tab storage (`ui/main_ui.py:175-193`).
- The rail has no drawer: the "tools drawer" is `RosterWidget.build_sidebar`
  (`ui/pipeline_builder/pipeline_roster.py:952`); the ArtiaX control center is a foot-pinned
  indicator (`_build_curation_session_btn` :1793) opening a dialog. Views are siblings toggled by
  `_switch_to` (`ui/workspace_page.py:78`), lazily built like the gallery (:188).
- The tutorial's copia arm continues past class3d (MaskCreate → Refine3D → Polish → CtfRefine,
  7.5 Å); none of those job types exist. The 26S arm is multi-species. The protocol of record ends
  at class3d (reference run it015: 13.77 Å) — as decided in `FEATURE_recipes.md:21`.

## 2. The critique

Every gap in the prompt log has one cause: **the run is a coroutine that owns a hidden project.**
Status lives in a progress string because the project is not on screen; results are files nobody
reads back because nothing knows where they are; logs have no channel because the task was the
unit, not the project; errors are `str(e)` because the coroutine is the only witness. The defects
in §1 — second observer, root-logger handler, orphaned chain on cancel, prune deleting a project,
no provenance — are the same shape: the harness re-implements observation, cancellation, retention
and identity that the application already owns for every project.

The inversion (D1) makes the harness a *reader and a writer of one folder* inside a project the
app already displays. What the harness keeps is exactly what the app does not have: the frozen
protocol, the metric extractors, the bands and the baseline records.

Three vocabularies called "drift" now exist in the repo — species-vs-job chips (`ui/species/
jobs_tab.py`), the metric verdict `DRIFT` (`services/regress/bands.py`), and roadmap 15's
"producer drift". A job param that no longer matches the protocol is a fourth thing; it is called
**edited** here (D7).

## 3. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | A protocol run IS a project. `<project>/protocol/` carries `protocol.yaml` (frozen copy), `run.json`, `metrics.json`, `report.md`, `runner.log` (CLI only). | §2. The workspace, monitor, roster, logs tab and stop path already exist per project. |
| D2 | Harness = `launch_run` + `evaluate_run` + `wait_for_settle`. Evaluation is pure, idempotent and lazy (panel button, rail auto-trigger on settle, CLI). | No long-lived coroutine in the server; a re-evaluate after a fix or a later bless is free. |
| D3 | UI-launched runs are created under the project base (`<base>/<protocol>-<stamp>`); CLI runs stay under `local_data/runs/<protocol>-<stamp>/`. | Base = hub-visible, restart-recovered, deleted through the hub, single reconciler (the server). `local_data` = the CLI process is the only reconciler, never adopted by a server. |
| D4 | `ProjectState.protocol_origin` (schema 3.7) + the frozen yaml in the project. | Identity survives reload; "edited" compares against what was applied, not against a bundle that may have changed since. |
| D5 | Single observer: `wait_for_settle` calls `reconcile_afterok` only when `PipelineMonitor.is_running` is False (the CLI). | The monitor docstring's contract ("the single observer") had been reintroduced through the back door. |
| D6 | Foot-of-rail status light beside the ArtiaX one (off / live-breathing / ok / bad) opens a full-width Protocols view with two levels: **Protocol** (bundle, stages, input/baseline state, runs, launch/export) and **Run** (this project's stages with status, verdict, metrics, checks, infra, edited chips, params diff, logs, files, actions). | A dialog cannot hold a 13-stage table with log panes; the light answers "is a run up and how is it doing" at a glance. |
| D7 | Job ≠ protocol param is **edited** (chip, tooltip names both values). `DRIFT` stays the metric verdict. UI says **baseline**, never "bless". | Three "drift"s already; "bless" was called unclear on 2026-08-27 (`q_important_ui_fixes.md:32`). |
| D8 | The UI never prunes. `prune_runs` (CLI) skips any run dir whose `project_params.json` says `pipeline_active` (an on-disk, cross-process check). | A retention policy must never delete a project a person or another process is looking at. |
| D9 | Stop from the view = `stop_and_cleanup`. No Retry from the view until roadmap 15-S4's submitter pre-flight exists. | The only guard against a second supervisor in a live job dir is `pipeline_active`, which 15 shows is clearable. |
| D10 | Copia STA tail (MaskCreate / Refine3D / Polish / CtfRefine) and the 26S arm are not job types; deferred, named. The protocol stays "through class3d ≈ 13.8 Å". | Reproducing the tutorial to 7.5 Å is a job-type roadmap, not an introspection one. |

## 4. Layout

```
services/project_state.py       ProtocolOrigin · ProjectState.protocol_origin · explicit restore in load() · SCHEMA_VERSION (3, 7)
services/protocols/apply.py     apply_protocol stamps protocol_origin + dump_protocol(<project>/protocol) · stage_edits()
services/regress/runner.py      launch_run · evaluate_run · wait_for_settle · run_protocol (CLI composition) · bless_protocol
services/regress/report.py      RUN_SUBDIR · read_run_json · list_runs(roots) · prune_runs (guarded) · stage_files
services/scheduling_and_orchestration/pipeline_monitor.py   PipelineMonitor.is_running
ui/protocols_view.py            ProtocolsPage (switcher · _ProtocolView · _RunView)
ui/protocols_blocks.py          shared stage/species/kv rendering (from protocols_dialog.py)
ui/components/log_pane.py       log_pane (hoisted from array_task_tracker._log_pane)
ui/open_project.py              open_project_in_workspace (hoisted from the hub's _switch_project)
ui/pipeline_builder/pipeline_roster.py   _build_protocol_btn (foot light) · _paint_protocol · _open_protocols
ui/workspace_page.py            protocols_container · _show_protocols · toggle_protocols
ui/protocols_dialog.py          launcher refit: launch_run, Open toast, runs from both roots, baseline wording
```

On disk:

```
<base>/<protocol>-<YYYYmmdd-HHMMSS>/protocol/{protocol.yaml, run.json, metrics.json, report.md}     UI-launched run
local_data/runs/<protocol>-<stamp>/protocol/{…, runner.log}                                          CLI run (legacy runs/<x>/run.json + project/ still listed)
local_data/baseline/<protocol>/records/<applied_at stamp>/{metrics.json, run.json, site.yaml, artifacts/}
local_data/baseline/<protocol>/blessed/
```

`run.json` = `RunRecord` (`asdict`): `protocol, protocol_version, mode, started_at, run_dir,
finished_at, project_dir, verdict, stages[], annotations[], input{}, site{}, driver{}`. Stage
verdicts: `PASS · DRIFT · FAIL · INFRA · UNBANDED · SKIPPED · PENDING`; overall adds `LIVE`
while the pipeline is active. `driver` = `{"mode": "server"|"cli", "pid", "host"}` — the view
shows a CLI-driven run read-only.

## 5. Stages

| # | Stage | Record |
|---|---|---|
| S0 | this doc · README row · 14 §9 + 15:96 cross-refs · FEATURE_recipes decisions · CLAUDE.md line | **done 2026-09-03** |
| S1 | `ProtocolOrigin` + field + restore + 3.7 · apply stamps origin + freezes yaml | **done 2026-09-03** — `services/project_state.py` (class before `ProjectState`, field beside `use_afterok_orchestrator`, restore before `return project_state`), `services/protocols/schema.py` (`PROJECT_PROTOCOL_DIRNAME`), `services/protocols/apply.py` (origin + frozen copy written by hand — `dump_protocol` would rebind the live object's bundle dir). Snap `notes/16-stage-snap/s1/` |
| S2 | runner split (`launch_run` / `evaluate_run` / `wait_for_settle`), `is_running`, report roots + guarded prune, CLI composition | **done 2026-09-03** — `runner.py` rewritten (`_execute`/`_wait_for_chain` deleted; `RunRecord` +`driver`, `evaluated_at`, `from_dict`, `is_settled`; stage `PENDING`, overall `LIVE`; `_classify_failed` names "no run.out / run.err"); `report.py` (`run_dir_of`, `read_run_json`, `run_json_mtime`, `list_runs(roots)` over both layouts, `prune_runs` skips on-disk `pipeline_active`, `stage_files`); `PipelineMonitor.is_running`; CLI prints the project and lists both roots. Snap `s2/` |
| S3 | `stage_edits` · `stage_files` · `read_run_json` | **done 2026-09-03** — folded into S2's files (`apply.stage_edits` beside `_coerce`; the two readers in `report.py`) |
| S4 | Protocols view + foot light + hoists (`log_pane`, blocks, `open_project_in_workspace`) | **done 2026-09-03** — new `ui/protocols_view.py` (`ProtocolsPage` + `_RunView` + `_ProtocolView`, `protocol_light`, `evaluate_if_settled`, `run_evaluation`, `launch_project_base`, `VERDICT_STATUS`), `ui/protocols_blocks.py` (+ `launched_dialog`), `ui/open_project.py`, `ui/components/log_pane.py`; `workspace_page.py` 6th container + `_show_protocols`; `pipeline_builder_panel.py` threads `toggle_protocols`; roster `_build_protocol_btn` / `_tick_protocol` / `_paint_protocol` / `_open_protocols` + `set_active_mode` tuple; `css.py` `.cb-protocol-live`; the tracker imports the hoisted pane. Snap `s4/` |
| S5 | landing dialog refit | **done 2026-09-03** — `ui/protocols_dialog.py` rewritten: facts off-loop, `launch_run` under the tab's project base, completion dialog with "Open run project", **Runs** (both roots, verdict chips, open), "baseline" wording. The optional roster header chip was NOT built (the view's chips cover it). Snap `s5/` |
| S6 | plugin loading off the package-import path (the S4 circular import) | **done 2026-09-03** — `ui/job_plugins/__init__.py`: `_load_plugins()` at import → `load_plugins()` (idempotent, `_loaded` set before the imports so a plugin querying the registry at import time cannot recurse), called by the three registry getters on first query. The package import has no side effect any more, so no import order can leave `ui.components.fields` half-initialised when a plugin panel pulls it. One file; no snap. |

Stage snapshots: `notes/16-stage-snap/<stage>/` holds a verbatim copy of every file the stage
touches, taken before the edit (the 07/11 convention; 14 skipped it).

## 6. Modern-Python weave-in

`match` on the stage's `JobStatus` when mapping to `PENDING` / evaluation (one dispatch, no
JobType table — R4 clean); `dataclass` `RunRecord` gains `driver` and a `from_dict` classmethod
for the seeded re-evaluate; `Path.glob` roots list rather than a hard-coded `runs/`;
`contextlib.suppress` nowhere — every swallow stays a narrow `except` with a reason.

## 7. Behaviour changes (quarantined, explicit)

- CLI run projects move up one level (`local_data/runs/<protocol>-<stamp>/` IS the project;
  no `project/` child). Old run dirs stay readable in listings.
- `run_protocol` no longer prunes a run whose project is `pipeline_active` on disk.
- `runner.log` exists for CLI runs only; UI runs have no runner log (their log is the server's).
- `SCHEMA_VERSION` 3.6 → 3.7 (additive).

## 8. Risks

- `evaluate_run` from a page that does not have the project in memory uses a detached load
  (`backend.read_project_state_detached`) — status may lag the registry by one tick; the view
  is built on the registry copy, so this only affects the runs list.
- A CLI-driven run opened in the server workspace is still double-reconciled across processes
  (roadmap 15's class); the view flags it and disables actions, nothing more.
- The rail light's auto-evaluate runs in the client's timer; two tabs on the same project may
  both call `evaluate_run` — it is idempotent and writes the same file.

## 9. Runtime checklist (the user's step)

```
venv/bin/ruff check . && venv/bin/ruff format --check .
venv/bin/python3 -c "import main"; venv/bin/python3 check_boundaries.py
venv/bin/python3 crboost_protocol.py validate copia-empiar12580
# provenance survives reload
venv/bin/python3 crboost_protocol.py apply copia-empiar12580 --name provtest --base /tmp/crb --movies '…/*.eer' --mdocs '…/*.mdoc'
grep -c protocol_origin /tmp/crb/provtest/project_params.json; ls /tmp/crb/provtest/protocol/
# UI: landing → Protocols → Run → toast "Open" → workspace: foot light breathing; view = 13 rows,
#     RUNNING spinner, expand a stage → params (protocol | current | edited) · run.err tail · files
#     Stop chain → light red, verdict FAIL; Evaluate now twice → run.json identical
# settle without the view open: light turns green/red on its own (rail tick auto-evaluates)
# Promote to baseline → baseline/copia-empiar12580/records/<applied_at>/ exactly once
# CLI: venv/bin/python3 crboost_regress.py run copia-empiar12580 → local_data/runs/copia-…-<stamp>/protocol/
#     report lists both roots; restart the server mid-UI-run → hub lists the run, monitor re-observes
# then roadmap 14 §8: record ×3 → bless → banded run → PASS
```

## 10. Deferred, and ideas kept

- **Copia STA tail** as job types (MaskCreate, Refine3D, Polish, CtfRefine) + protocol v2 to the
  tutorial's 7.5 Å; **26S** as the multi-species case. Own roadmap.
- **Retry-from-stage** in the view — after 15-S4 (submitter pre-flight). Until then: fix the
  param in the job tab, redeploy from the builder, Evaluate again (the run being a project makes
  that free).
- **Cross-process reconciler lock** (roadmap 15, one level up: `<project>/.crboost/reconciler`).
- **Explicit `inputs` wiring at apply** — instance-id keyed resolver overrides (14 §9).
- **v0 bands from the reference run** — hand-authored from observed values (28 tilts, 9000
  rotations, 816 → 783 particles, 13.77 Å), `provenance: reference_run`, so the first real run is
  judged against something. Optional runtime step; bless overwrites it.
- **`crboost_protocol.py validate` in the static gates** (CLAUDE.md commands).
- **Roster header chip** `Protocol: <name> v<n> · k edited` beside the project avatar (S5 optional).
- **Compare two runs** side by side (metrics.json diff) — the runs list carries the verdict
  history; a diff view is a later convenience.

### Stage log — runtime findings 2026-09-03 (after S5)

- `check_boundaries.py`: clean. `ruff check .`: clean.
- **`import main` → circular import (S4 regression, app still boots, tilt-filter plugin missing):**
  `ui/job_plugins/__init__.py:129` runs `_load_plugins()` at import time. The new module-level
  `from ui.protocols_view import evaluate_if_settled, protocol_light` in
  `ui/pipeline_builder/pipeline_roster.py:18` makes `ui.protocols_view` (→ `ui.components.fields:51`)
  the FIRST importer of `ui.components.fields`; its line 31 `from ui.job_plugins._field_styles import …`
  initialises the `ui.job_plugins` package, whose loader imports `ui.job_plugins.tilt_filter` →
  `ui.tilt_filter_panel:24` → `from ui.components.fields import house_number` while `fields` is still
  half-initialised → `ImportError` (caught by the loader: "Plugin module ui.job_plugins.tilt_filter
  failed to load"). Before S4 the roster never touched `fields`, so `ui.job_plugins` was always
  initialised before `fields` was.
  - **Minimal fix (one line, S4 file):** make the roster's import lazy — move the two names into
    `_tick_protocol` / `_paint_protocol` as function-local imports (the roster already imports
    `services.project_counts` that way) — OR drop `house_text` from `ui/protocols_view.py`'s module
    imports (lazy inside `_ProtocolView.render`).
  - **Structural fix (the real hazard, its own small stage):** a shared component
    (`ui/components/fields.py`) must not depend on the plugin package's import side effect. Either
    hoist `_numeric_kind` / `numeric_forward` / `PAGE_SECTION_STYLE` out of
    `ui/job_plugins/_field_styles.py` into `ui/components/field_styles.py` (and re-export from
    `_field_styles`), or make plugin loading explicit (`load_plugins()` called once by the pipeline
    builder) instead of `_load_plugins()` at import.
- `crboost_protocol.py validate copia-empiar12580`: output not captured in the session log (only
  NumExpr notes were pasted) — re-run and read the last line.
- **Fixed — S6 (2026-09-03), the structural form.** `ui/job_plugins/__init__.py` no longer imports
  the plugin modules when the package is imported; `load_plugins()` runs once on the first
  `get_params_renderer` / `get_extra_tabs` / `get_full_panel_renderer` call (all three are render-time,
  pipeline builder only — `config_tab`, `job_tab_component`, the roster's extra-tab loop). The hoist
  was not done: it would only have cured `fields`, and any other shared module a plugin panel pulls
  would carry the same trap. The one-line lazy import in the roster was not done either: it hides
  the hazard and the next module-level import of `fields` brings it back.
  Gate for this class (add to the static gates whenever `import main` is run):
  `venv/bin/python3 -c "import main, ui.job_plugins as p; p.load_plugins(); print(sorted(j.value for j in p._REGISTRY))"`
  — must print a list containing `tiltFilter` and no "Plugin module … failed to load" warning.

# Roadmap 16 — Protocols: the shape of a pipeline with its parameters

**Status:** re-scoped and cut back **2026-09-04**; code complete, runtime checklist §6 owed.
Supersedes roadmap 14's harness (deleted the same day) and the first version of this document
(the "protocols workbench" with a regression harness inside it).

## 1. Context

The standing wish was simple: **a way to record the shape of a pipeline — which jobs, in what order,
with which parameters and species — untethered from any particular project, and to spawn a regular
project from it that runs through the regular pipeline.** Whether the result is good is judged by a
person looking at the spawned project (the class3d resolution, the job tabs, the Journey), exactly
as for any other project.

Roadmaps 14 and 16 (first version) built that, and then a regression harness around it: frozen inputs
with checksums, run.json verdicts, records, bless, bands, evaluate, baseline, INFRA classification,
site snapshots, Tier A snapshots, two CLIs, a six-state rail light. None of it was asked for, and it
brought a vocabulary of its own. On 2026-09-04 the maintainer cut it: *"a protocol is just a way to
record a bunch of params spread over jobs … nothing about the results obtained with it should be
here."* This document is the record of what remains.

## 2. What a protocol is

A bundle directory under `config/protocols/<name>/` (shared, checked in) or `~/.crboost/protocols/<name>/`
(personal):

```
protocol.yaml    name · version · description · provenance (free-form) · species[] · stages[]
assets/          the template / mask volumes the species entries point at
```

`species[]`: id, name, diameter, symmetry, template + mask assets, extraction geometry.
`stages[]`: `job` (a JobType value), optional `species` (→ instance id `{job}__{species}`), `params`
(a FULL `USER_PARAMS` snapshot, never a diff against defaults), optional `inputs` wiring.
No absolute path anywhere in a bundle. Dataset facts (pixel size, dose, tilt axis) are not in the
protocol: they come from the mdocs of whatever data a project is created on.

`services/protocols/`: `schema.py` (the models + yaml load/dump), `discovery.py` (the two roots;
a bundle that fails to load is LISTED with its error), `export.py` (project → protocol, the
authoring tool), `apply.py` (`validate_protocol` + `apply_protocol`: the one engine that turns a
protocol into a new, regular project — species registered with their assets, every stage
instantiated with coerced params, unknown/rejected fields REPORTED as warnings, never silently
substituted; plus `stage_edits`, protocol vs current per stage).

A project created from a protocol carries `ProjectState.protocol_origin` (`name`, `version`,
`bundle_dir`, `applied_at`; schema 3.7, explicit restore in `load()`) and a frozen copy of the
bundle's yaml at `<project>/protocol/protocol.yaml` — what "protocol | current" compares against,
whatever happens to the bundle later.

## 3. The two surfaces

**Landing → Protocols** (`ui/protocols_dialog.py`, the "Protocols" action on the status strip). One
row per bundle: name (click → every species and stage with its parameters), version / stages /
species / description, **Create project**. Create = the regular project creation with the protocol
applied: name, base, movies glob, mdocs glob (pre-filled from the config defaults), optional gain
ref → `apply_protocol` → `open_project_in_workspace`. The workspace opens with the roster listing
the protocol's stages as `ui_mgr.selected_jobs`; the normal Run button submits exactly those. No
special runner: the chain, the monitor, stop, logs are the regular pipeline's.

**Workspace → Protocols view** (`ui/protocols_view.py`, the foot-of-rail light). Switcher header
(the Journey's caret idiom: selected protocol, version, description; every bundle behind the caret).
Two levels:
- *This project* — only when this project's origin is the selected protocol: one row per stage,
  expand → the parameter table protocol | current, with the parameters a person changed in the job
  tabs since highlighted and counted as **edited**. Nothing else: no statuses, logs, files, results.
- *Protocol as declared* — bundle path, version, provenance, species and stages with their
  parameters; **Save this project as a protocol** (export to `~/.crboost/protocols/<name>/`).

**The light** (`pipeline_roster._build_protocol_btn`): dim (not from a protocol) · lit (created from
one) · blue, breathing while `pipeline_active`. Its hover names the protocol and what is running.

**CLI** `crboost_protocol.py list | validate | export | apply`.

## 4. Layout

```
services/protocols/{schema,discovery,export,apply}.py
services/project_state.py          ProtocolOrigin · ProjectState.protocol_origin · restore in load()
crboost_protocol.py                list · validate · export · apply
config/protocols/copia-empiar12580/{protocol.yaml, assets/}    the CryoBoost v1 tutorial, importmovies → class3d
ui/protocols_dialog.py             landing: list · detail · Create project
ui/protocols_view.py               workspace: switcher · this project (params) · protocol as declared · save-as
ui/protocols_blocks.py             shared species / stage / kv rendering
ui/open_project.py                 open a project in this tab's workspace (also used by the hub)
ui/pipeline_builder/pipeline_roster.py   the foot-of-rail light; ui/workspace_page.py _show_protocols
```

## 5. Deleted on 2026-09-04 (for the record)

`services/regress/` (inputs, checks, bands, infra, report, runner, snapshot), `crboost_regress.py`,
`services/protocols/scheme_export.py` and the `scheme` subcommand, `config/protocols/*/test/`,
`Expectation`/`expects`, `schema_fingerprint`, `ProtocolOrigin.mode`/`stage_fingerprints`,
`PipelineMonitor.is_running`, `LocalDataConfig`/`local_data.root`, run.json / report.md / verdict /
record / bless / bands / baseline / evaluate / Stop-from-the-view / log tails in the view, the
six-state light. Existing run projects may still carry inert `<project>/protocol/{run.json,
metrics.json, report.md}` files and a frozen copy with the old `expects:` / `schema_fingerprint:` keys
— the view then shows "frozen copy unreadable"; delete `<project>/protocol/` or re-create the project.

Deferred and named: the copia STA tail (MaskCreate / Refine3D / Polish / CtfRefine) and the 26S
multi-species case are not job types; explicit `inputs` wiring at apply is reported, not applied
(the resolver's override keys embed job directories).

## 6. Runtime checklist (the user's step)

```
venv/bin/ruff check .
venv/bin/python3 check_boundaries.py
venv/bin/python3 -c "import main, ui.job_plugins as p; p.load_plugins(); print(sorted(j.value for j in p._REGISTRY))"
venv/bin/python3 crboost_protocol.py list
venv/bin/python3 crboost_protocol.py validate copia-empiar12580
# landing → Protocols → copia → Create project (movies/mdocs globs of the EMPIAR-12580 data) → workspace:
#   13 staged rows in the roster, the foot light lit → Run → light breathes → chain to class3d
#   (denoise predict → template matching now passes: the star fix of 2026-09-04)
# workspace → light → Protocols view: this project's parameter tables; edit a param in a job tab → "edited" chip
# resolution: the class3d job tab of the spawned project
```

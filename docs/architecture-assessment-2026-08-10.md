# Architecture & Code-Style Assessment — 2026-08-10

Scope: whole repo (`services/` 21.4k lines, `ui/` 28.6k, `drivers/` 6.9k, `backend.py` 1.9k; 62.5k total).
Method: manual read of the load-bearing spine (`models_base`, `tilt_series/`, `jobs/_base`, `io_slots`,
`project_state`, `path_resolution_service`, `backend.py`) plus four exhaustive audit sweeps
(UI↔backend coupling, driver duplication, type coherence, naming/module organization).
All claims carry file:line citations from those sweeps.

---

## 1. Verdict

The architecture **on paper is right** — services / drivers / UI with a facade, a typed IO-slot system,
and a first-class TiltSeries registry. The newest subsystem (`services/tilt_series/`) is genuinely
excellent and proves the team knows exactly what good looks like. The problems are not design problems;
they are **enforcement and history** problems:

1. **The boundary is a suggestion.** 39 of 57 `ui/*.py` files import `services.*` directly; only 5 go
   through `backend.py`. One true inversion exists: `services/project_state.py:1174` imports `ui.ui_state`.
2. **Five coexisting models of "tilt series".** The registry is the best of them and the least consulted —
   the Journey dashboard re-reads emitted STAR files for facts the registry already holds.
3. **Stringly-typed arteries.** `{"success": bool, ...}` dicts (~180 construction sites, with a live bug),
   `paths: Dict[str, str]` with 22 undeclared magic keys, `instance_id.split("__")` in 19 places,
   `JobType` values in four casing styles used as raw literals ~180 times.
4. **~475 lines of copy-pasted driver lifecycle** across 8 array drivers, plus 3 independent
   implementations of the `.task_status` protocol (drivers, pipeline_runner, UI).
5. **Silent-failure culture** that directly contradicts the repo's own CLAUDE.md rule: 213 `except
   Exception` in services+backend, 164 swallow-and-return-default sites repo-wide.
6. **Modern Python unused**: zero `Protocol`, `TypedDict`, `StrEnum`, `match`, `abc.ABC`,
   `cached_property` anywhere — on Python 3.11, where all of them are available.
7. **~2,400 lines of dead code** kept on the books (`MetadataTranslator`, most of `filterTilts/`,
   two zero-byte modules, a shim that lies about being legacy).

None of this is a rewrite case. The exemplar patterns already exist in-repo; the work is to make the
rest of the codebase converge on them and to delete what's been superseded.

---

## 2. What is already right (build on these)

- **`services/tilt_series/models.py` is the house style to converge on.** `extra="forbid"` on every
  model, `Path` (never `str`) for every path field, Pydantic discriminated unions for heterogeneous
  per-job outputs (`:76`, `:130`, `:167`), units in field names (`defocus_u_angstrom`,
  `pre_exposure_e_per_a2`), identity rules documented as a module-docstring contract and enforced by
  `registry.sanity_check()`. Its `outputs: Dict[instance_id, FrameOutput]` solves the *same problem*
  as `ProjectState.jobs: Dict[str, SerializeAsAny[AbstractJobParams]]` — heterogeneous typed payloads
  keyed by instance id — with a discriminator instead of `SerializeAsAny` plus a 90-line hand-rolled
  loader. Strongest single argument in this document.
- **`services/io_slots.py`** — well-designed slot/manifest types, `JobFileType` with perfectly
  consistent snake_case values. (Its output is then flattened to a stringly dict; see §5.2.)
- **`drivers/array_job_base.py`** — a genuinely good verb library (manifest schema, atomic status
  writes, sparse re-run array specs, exclusion pre-marking, registry preflight). What it lacks is a
  template method; see §7.
- **`ui/components/reactive.py`** (`FingerprintedView`, `SingleFlight`) and the CLAUDE.md UI-reactivity
  conventions — real, documented, followed by new code.
- **`ui/curation_session_dialog.py`** — the model UI file: zero services imports, zero file I/O,
  everything through the facade. Also clean: `ui/dashboard/strip.py`, `ui/dashboard/figures.py`,
  `ui/components/reactive.py`.
- **Suffix conventions that already hold**: `*Service` (16), `*Params` (15), `*Config` (7), `*Output` (5),
  `*IngestAdapter` (4), `*Registry` (2). §9 builds on these instead of inventing new ones.

---

## 3. The boundary problem: UI ↔ services

### 3.1 The facade is optional

- 39/57 UI files import `services.*` directly (120 import statements over 29 service modules; 51 of
  them are function-local "lazy" imports — the tell that authors knew they were crossing a line).
- `backend.py:48-61` exposes 11 service objects as public attributes, so even facade users do
  `self.backend.template_service.*` — direct access with extra steps.
- **The one hard inversion:** `services/project_state.py:1174` does `from ui.ui_state import
  get_ui_state_manager` inside `get_project_state()`. The core domain accessor depends on a browser
  tab, and returns a **blank throwaway `ProjectState()`** when there's no client context (`:1180-1183`)
  — called from 66 sites. This caused the W2 ArtiaX bug; `services/aggregation_authoritative.py:11-17`
  documents working around it rather than fixing it.

### 3.2 Whole services living inside `ui/`

| Location | What it is | Belongs in |
|---|---|---|
| `ui/dashboard/data.py` (710 L) | The dashboard aggregation service: star reads, job-dir resolution, per-TS status derivation, journey model. Zero NiceGUI. | `services/dashboard_service.py` |
| `ui/dashboard/pixel_sanity.py` (676 L) | Pixel/binning propagation engine + domain sanity rules (box/Ø thresholds 1.5×/2×/3×). Rendering only from `:487`. | `services/pixel_chain.py` |
| `ui/components/task_utils.py` (154 L) | Third implementation of the `.task_status` protocol + job-dir resolution. | `services/array_tasks.py`, single-sourced with `drivers/array_job_base.py:45-46` |
| `backend.py:536-1412` (~876 L, 47% of the facade) | The entire ChimeraX/ArtiaX curation-session subsystem. | `services/curation_session_service.py` |

### 3.3 Business logic executed from UI event handlers

- `ui/tilt_filter_panel.py:185` `_finalize_pipeline_output` — **produces the tilt-filter job's actual
  pipeline output** (trims tomostar, mutates `job_model.paths`, stamps registry verdicts, saves) from
  the UI, because the manual-label path never dispatches the driver.
- `ui/tomo_dashboard_dialog.py:2939` `_handle_extract_list` — full job lifecycle in a dialog: param
  assembly, submission, a 360-iteration poll loop on `RELION_JOB_EXIT_*` sentinels, `mark_extracted`,
  force-save.
- `ui/aggregation_merge_card.py:117` `apply_aggregation_overrides` — rewrites `source_overrides`,
  `paths`, `is_orphaned` across *every job in the project*, on every workspace render — and
  `services/path_resolution_service.py:590` has a comment **depending on this UI card's behavior**.
- Scientific math in render paths: `_compute_tomogram_polarity` (`tomo_dashboard_dialog.py:1927`),
  hand-rolled OLS (`:1641`), RELION binning arithmetic (`:3626-3644`), defocus-hand reconciliation
  (`:895`), FFT bandpass + MRC I/O (`tilt_filter_panel.py:1077`, duplicating
  `filterTilts/image_processor.py`), MRC volume negation + box-size heuristics
  (`template_workbench.py:2083`, `:1694`).
- **28 `save_project()`/`state.save()` call sites across 8 UI files**, including a bespoke debounced
  persistence scheduler inside a card widget (`aggregation_merge_card.py:90`). Persistence policy
  should have exactly one owner.

### 3.4 Fix direction

1. Break the `services → ui` import: `get_project_state()` requires an explicit path (or dies);
   UI gets a thin `ui/current_project.py` wrapper that resolves the tab context. This is the unblocken
   for everything else.
2. Move the four embedded services (§3.2) behind the facade.
3. Make `save_project` unreachable from `ui/` — persistence goes through backend command methods.
4. Stop exporting service objects as facade attributes; expose *operations*.

---

## 4. TiltSeries: five models of one concept

The user's instinct that TiltSeries is the load-bearing class is exactly right. Today there are five:

| # | Class | Where | Role |
|---|---|---|---|
| 1 | `TiltSeries`/`Frame`/`Tomogram` | `services/tilt_series/models.py` | canonical registry (the keeper) |
| 2 | `TiltSeriesInfo`/`TiltInfo`/`StagePositionInfo` | `services/dataset_models.py` | import-time mdoc parse |
| 3 | `ImportTiltSeriesSummary` + `ProjectState.tilt_metadata` + `tilt_filter_labels` | `services/project_state.py:248,691,694` | persisted mirror — **every field duplicates a `TiltSeries`/`Frame` field** |
| 4 | `TiltSeriesData` | `services/tilt_series_service.py:31` | pandas view of STARs |
| 5 | `TomoFrame` | `services/visualization/coords.py:100` | tomogram geometry ("Frame" here means coordinate frame — name collision with entity #1's Frame) |

### 4.1 The registry is bypassed by its biggest would-be customer

The Journey dashboard uses the registry **only** for `excluded_ids()` (`tomo_dashboard_dialog.py:450`)
and otherwise re-reads the emitted STARs and does filename surgery for facts the registry holds typed:

- per-tilt defocus/CTF-res/motion (`:1373`) → registry has `FsMotionCtfFrameOutput.ctf_resolution`,
  `.mean_frame_movement`
- alignment per-frame (`:1465`) → `TsAlignmentPerFrame`
- tilt-filter verdicts re-read from `tilt_series_labeled/*.star` (`:1781`) → `Frame.is_filtered_out`/
  `filter_probability` — which `ui/tilt_filter_panel.py:215-232` *writes to the registry in the same commit*
- denoised volume found by stem-suffix surgery (`:2319`) → `DenoisePredictTomogramOutput.denoised_mrc`
- TomoHand re-read from import star (`:895`) → `TsCtfTiltSeriesOutput.are_angles_inverted`

Plus four independent `Position_{stage}[_{beam}]` regex parsers (`tilt_series/build.py:275` canonical;
`dataset_parsing_service.py:22`, `ui/components/task_utils.py:13`, `ui/tilt_filter_panel.py:53`), and
`ui/dashboard/data.py:22` importing the *private* `_infer_position` across the layer boundary.

This is precisely Phase 2+3 of the already-greenlit `docs/registry-consolidation-roadmap.md` — the
audit quantifies the gap. Landing dashboard-on-registry retires model #3 wholesale
(`tilt_metadata`, `tilt_filter_labels`, `Import*Summary`) and most of #4.

### 4.2 Identity caveats worth an explicit decision

- IDs are canonicalized name strings (mdoc/frame stems) with a `project_prefix` mutation knob
  (`build.py:37,58,152`) — fine, but it's a contract, and `adapters/ts_ctf.py:259-274` `_resolve_frame`
  quietly breaks the "never via string heuristics" claim with a 3-way fallback incl. `_EER`-strip.
  Either bless the fallback in the identity contract or remove it.
- Adapter default `job_instance_id`s are dead **and wrong**: `"tsCTF"` (`adapters/ts_ctf.py:58`) and
  `"tsAlignment"` (`adapters/ts_alignment.py:61`) match no `JobType` value. They key registry entries;
  only explicit-passing callers have kept this from biting. Remove the defaults (make the kwarg required).

---

## 5. Stringly-typed arteries

### 5.1 The `{"success": bool, ...}` pseudo-Result — ~180 construction sites

No shared Result type. Failure key disagreement: `"error"` ×129, `"message"` ×33, `"reason"` ×6,
`"detail"` ×2, `"errors"` ×2. **Live bug:** `deploy_and_run_scheme` returns failures under `"message"`
(`pipeline_orchestrator_service.py:69,290,339,373,394`) but the only consumer reads `"error"`
(`ui/pipeline_builder/pipeline_builder_panel.py:515-516`) — a failed pipeline start notifies
**"Failed to start: None"**. Same class returns `"error"` at `:722,742` and `"message"` at `:744`.

Fix: one `ServiceResult` (or `Ok[T]`/`Err` with `code: ErrorCode` StrEnum), migrating
`deploy_and_run_scheme → start_pipeline → pipeline_builder_panel` first since it's a live bug on the
app's primary action. A typed `Err` cannot be `.get()`-ed into `None` — this is also the enforcement
point for the CLAUDE.md "never fail silently" rule.

### 5.2 `job_model.paths: Dict[str, str]` — a typed system voluntarily downgraded

`io_slots.py` builds a typed `ResolvedManifest`, then `as_paths_dict()` (`:179`) flattens it to
`{str: str}`, synthesizing collision keys by string concat (`:200` `f"output_{key}"`). Downstream:
**78 subscript sites across 22 undeclared magic keys** (`input_star` ×24, … `tomostar_dir` vs
`input_tomostar` vs `output_tomostar` — three keys, one concept). Two independently-authored key
namespaces are merged flat with dict-spread precedence at `driver_base.py:112`
(`{**context_paths, **io_paths}`), where `get_context_paths()` (`path_resolution_service.py:795-852`)
is a hand-written per-jobtype if-chain injecting keys that also appear in `OUTPUT_SCHEMA`s.

Fix: `SlotKey` StrEnum (or per-schema `Final` constants); `paths: dict[SlotKey, Path]`; fold
`get_context_paths` into the schemas; delete the `f"output_{...}"` synthesis; split
`resolve_all_paths`'s bool-switched union return (`:105`) into two functions.

### 5.3 `get_driver_context()` — 6-positional-tuple with a bare `dict` inside

`driver_base.py:44` returns `Tuple[ProjectState, T, dict, Path, Path, JobType]`; 17 call sites unpack
it under three different names for position 2 (`local_params_data`/`context`/`context_data`). The dict
re-derives 4 fields that exist typed on the models (including `job_type.value` — enum → str).
Fix: a frozen `DriverContext` dataclass. Mechanical, touches every driver once, immediately pays off.

### 5.4 `instance_id` grammar — 19 hand parsings, canonical decoder in the wrong layer

`{jobtype}__{species}` is re-split inline in 19 places; the closest thing to a canonical parser
(`instance_id_to_job_type`) lives at `ui/ui_state.py:202`. The species-resolution fallback chain is
written three times, and two of the copies say so in comments (`template_metadata.py:148`,
`aggregation_authoritative.py:54`, `ui/dashboard/data.py:77`).
Fix: frozen `InstanceId(job_type, species_id)` value object in `services/models_base.py` with
`.parse()`/`__str__`; one species-resolution function.

### 5.5 Enum/Literal/string triple-representation

- `JobType` bypassed by `.value == "tsCtf"` string comparisons (5 sites: `pipeline_runner.py:314`,
  `ui/tilt_filter_panel.py:114,136,152,172`).
- `AlignmentMethod` ("AreTomo"/"IMOD"/"Relion") vs `Literal["aretomo","imod"]` on the output model —
  bridged by a ternary + `# type: ignore` that silently maps RELION→"imod"
  (`adapters/ts_alignment.py:335-339`).
- `PolarityGuess = Literal["white","black","ambiguous"]` narrowed to `Literal["white","black"]` in
  storage by **silently recording ambiguous as "black"** (`ui/template_workbench.py:343`) — a direct
  violation of the repo's no-invented-defaults rule.
- RELION status vocabulary handled as raw strings in 27 places; `JobStatus` has no `PENDING`, so
  `pipeline_runner.py:269-302` does ad-hoc if/elif with a bare-constructor fallback. Fix:
  `RelionProcessStatus` + `SlurmState` StrEnums with explicit `match`-based mappings.
- Tool names are free-form strings (~30 sites); on a miss, `config_service.py:331` **invents**
  `ToolConfig(exec_mode="binary", bin_path=<typo>)` — a typo becomes "run a bare binary named <typo>".
  Fix: `ToolName` StrEnum; raise on miss.

---

## 6. Silent failure vs the project's own rule

CLAUDE.md's "never fail silently, never invent defaults" is the most-violated rule in the codebase:

- 213 `except Exception` in `services/` + `backend.py` (45 in `backend.py`, 39 in `pipeline_runner.py`);
  164 swallow-and-return-default sites repo-wide; 157 `return None` in services+backend.
- `starfile_service.py:25-27`: the except branch **repeats the identical failing write call** — the
  escape hatch is dead code producing an unhandled raise plus a misleading log line. Every STAR write
  goes through here.
- `ProjectState.load()` (`project_state.py:~1030-1115`): six `except Exception → log → set to []`
  blocks that silently discard pick lists, aggregation sources, import details, or whole jobs on
  schema drift.
- `jobs/_base.py:149-150,176-177`: malformed `job_resource_profiles` in conf.yaml silently drops every
  per-jobtype SLURM override (`except Exception: pass`).
- `preview_render.py`: 27 `return None`s collapsing six distinct causes (missing Pillow vs corrupt MRC
  vs zero slab…) into indistinguishable blank UI.
- `backend.py:1442-1568` `_scan_for_projects_sync`: nested `except: pass` renders a corrupt project as
  a plausible blank card.

The structural fix is §5.1's Result type plus lint enforcement (§8.4) — cultural rules don't hold
against 213 existing counterexamples; types and lints do.

---

## 7. Drivers: the duplication mass

Full detail in the drivers sweep; the three levers:

1. **`ArrayDriver` template class** in `array_job_base.py` with `enumerate_items/prepare/run_item/
   aggregate` hooks, owning mode dispatch, bootstrap try/excepts, manifest index lookup, results tally,
   exit markers, failure handler. Deletes ~475 duplicated lines across 8 drivers and structurally
   closes real divergences: `subtomo_extraction`'s `manifest["items"]` vs everyone's
   `manifest["ts_names"]`, its reimplemented `apply_exclusions` (`:202-230`), `ts_alignment`'s
   glob-based TS enumeration (`:54-57`, contradicting `array_job_base.py:176-183`'s own warning),
   `denoise_predict`'s missing `preflight_registry`.
2. **`ToolCommand` builder + `run_tool()`** in `driver_base.py` — folds 12 hand-rolled command
   builders, 5 bind-dedup blocks, inconsistent `shlex.quote` (6/18 files), and 11 hardcoded tool-name
   strings. The `subtomo_extraction.py:413-434` / `extract_pick_list.py:36-61` near-verbatim twin
   collapses outright.
3. **`TaskStatusStore` + `BaseIngestAdapter`** — one owner for the `.task_manifest.json`/`.task_status`
   protocol (currently 3 implementations: `array_job_base`, `pipeline_runner.py:1013-1034`,
   `ui/components/task_utils.py:104-126`); a base class for the 4 adapters (identical `__init__`s,
   3× duplicated `_read_only_block`, 2× `_resolve_per_ts_path`, 4× copied exclusion filter); real
   adapters for `denoise_predict` (`:283-327`) and `tilt_filter` (`:134-157`) instead of hand-rolled
   registry stamps. Removes the standing hazard that UI's and runner's notion of "done" drifts from
   the drivers'.

Cheap adjacent wins: move `subtomo_merge.py` (a library, no `main()`) from `drivers/` to `services/`
— `services/visualization/list_extraction.py:38-90`'s docstrings admit they mirror it solely to avoid
a services→drivers import. Single-source the driver-invocation command string built independently at
`pipeline_orchestrator_service.py:529-541` and `array_job_base.py:445-454`. Port `extract_pick_list.py`
onto `get_driver_context` (it hand-rolls argparse and bypasses the entire bootstrap).

Also: drivers print with hand-typed `[SUPERVISOR]`/`[TASK n]` prefixes (`FATAL BOOTSTRAP ERROR` ×18)
while services use `logging` — acceptable for SLURM logs, but the prefix discipline should come from
the template class, not 8 copies.

---

## 8. Modern Python: what's absent and where it would pay

Census (repo-wide, excl. venv): `Protocol` 0, `TypedDict` 0, `NewType` 0, `StrEnum` 0 (24 hand-rolled
`str, Enum`), `match` 0, `abc.ABC` 0, `cached_property` 0, `dataclass(slots=True)` 0. Discriminated
unions: 3, all in `tilt_series/models.py`. Return-annotation coverage in services+backend: 76%.

### 8.1 Protocols (the user asked specifically)

- **`IngestAdapter(Protocol)`** — the 4 adapters are structurally identical but their `emit_star`
  signatures diverge silently (one lacks `excluded_ids`). A Protocol + base class makes the contract
  checkable.
- **Job-plugin renderer Protocols** — `ui/job_plugins/__init__.py:37,42,46` documents three `Callable`
  contracts in comments, one of which is already stale (says 4 args; every implementation takes 5).
  Three `Protocol`s with `__call__` turn the comments into types.
- **`ArrayJob` capability** — "is this an array job?" is currently
  `"array_throttle" in getattr(job_model, "USER_PARAMS", set())` in 5 UI files; renaming one field
  silently disables all array-task UI. Use `IS_ARRAY_JOB: ClassVar[bool]` or a Protocol.
- **`PanelCallbacks`** — the `Dict[str, Callable]` bags (10 signatures, 18 magic keys, half `.get()`
  half `[]`) become one frozen dataclass/Protocol.

### 8.2 Fix the pseudo-abstract base

`AbstractJobParams` hooks are `raise NotImplementedError` or **silent `return None`**
(`from_job_star`, `_base.py:461-463`) — a subclass forgetting one gets `None` at runtime instead of a
definition-time error. Use `abc.abstractmethod`. Declare `INPUT_SCHEMA`/`OUTPUT_SCHEMA` as `ClassVar`
on the base (today they exist on all 15 subclasses but not the base, so the resolver reaches them via
`getattr(cls, "INPUT_SCHEMA", None)` and a missing schema silently resolves to no inputs). Then replace
the ~80 `getattr(model, "<declared attr>", default)` sites with plain attribute access.

`AbstractJobParams` also carries six responsibilities (fields, SLURM 3-layer merge ×2 near-identical
methods, RELION job.star generation **with pandas** — the reason every param import drags pandas in —
15 state-proxy properties, `__setattr__` mutation policy, hooks). Extract `JobStarWriter` and
`SlurmResolver` as composition; keep the param class a data model. Add
`model_config = ConfigDict(validate_assignment=True)` to the base — that alone fixes the unvalidated
`setattr` loop at `backend.py:1685-1687`.

### 8.3 One `JobSpec` registry instead of 8 satellite tables

Adding a `JobType` today means editing 8 unsynchronized tables: `jobtype_paramclass`
(`services/jobs/__init__.py:32` — rebuilt as a fresh dict **on every call**, including inside
`ProjectState.load()`'s loop and the per-slot schema getters), `PIPELINE_ORDER` + `JOB_DISPLAY_NAMES`
(`ui/ui_state.py:146,164`), `PHASE_JOBS` + `JOB_DEPENDENCIES` (`ui/pipeline_builder/
pipeline_constants.py:7,42`), `_PREREQUISITES` (`pipeline_builder_panel.py:389`),
`_ARRAY_STAGE_OUTPUT_STAR` (`ui/dashboard/data.py:245`), plugin `_REGISTRY`
(`ui/job_plugins/__init__.py:50`). One frozen `JobSpec` dataclass per job type (param class, display
name, phase, dependencies, driver module, adapter) collapses all of them and gives `ProjectState.load`
its discriminator for free.

### 8.4 Toolchain: the cheapest high-leverage change in this document

`ruff.toml` selects only `["E","F"]` with no `target-version`. Set `target-version = "py311"` and add
`UP` (pyupgrade), `B` (bugbear), `RUF`; consider `ANN` on `services/` only. Every implicit-Optional
(`backend.py:1785` has three in one signature; `slurm_service.py:108` annotates `List[str]` and
defaults `None`), legacy `Dict/List/Optional` spelling, and missing return annotation becomes a lint
error instead of a reading exercise. This is the enforcement mechanism for everything else here.

### 8.5 `ProjectState.load()` → discriminated union

Give each param class `job_type: Literal[JobType.X]`; `jobs: dict[str, Annotated[Union[...],
Field(discriminator="job_type")]]`; delete the ~90-line hand deserializer and its six silent-discard
blocks. The pattern is proven three times in `tilt_series/models.py` on the identical problem shape.

---

## 9. Naming and module organization

### 9.1 `JobType` values are the fault line

Four styles in one enum (`models_base.py:69-91`): `importmovies` / `fsMotionAndCtf` / `aligntiltsWarp`
(verb-first, tool-suffixed, unrelated to member) / `tmextractcand` (abbreviated). These leak as ~180
hardcoded string literals: every `preferred_source=` in `services/jobs/*`, every `legacy_source_map`,
UI comparisons. `"importedTomograms"` (`path_resolution_service.py:266`) is a producer id that is not
a JobType member at all. Meanwhile `JobFileType` (`io_slots.py:14-48`) does it perfectly.

**Migration** (the one breaking change worth making): values → `lower_snake` derived from the member
(`ts_reconstruct`, `fs_motion_ctf`, …). Persisted in `project_params.json` and RELION dir names, but
`JobType.from_string` (`models_base.py:93-99`) is already the single chokepoint — a
`_LEGACY_JOBTYPE_VALUES` dict makes it a ~20-line change. Payoff: the value equals the module basename
across all four per-job layers (today: `fs_motion_and_ctf.py` / `fs_motion_ctf.py` /
`fs_motion_ctf.py` / `fs_motion_and_ctf.py`; `extract_candidates_pytom.py` vs `candidate_extract.py`),
`preferred_source` becomes `JobType.X`, and adapter instance-id defaults become derivable.

### 9.2 Names that lie (fix opportunistically)

`container_service.py:15 class Colors` (pretty-prints apptainer commands; zero colors) vs
`preflight.py:25 class C` (the actual colors) — effectively swapped. `ui/tomo_dashboard_dialog.py` —
6428 lines, zero classes, not a dialog, renamed surface ("Journey") per its own docstring.
`services/job_models.py` calls itself a legacy shim while being the mainline import path (14 importers
vs 1 for canonical `services.jobs`) **and omits `TiltFilterParams`** — the advertised import path
raises `ImportError`. `TomoFrame` collides with the Frame entity. `local_file_picker` is the repo's
only snake_case class. Seven files still carry pre-move path headers on line 1.

### 9.3 Convention table (grounded in existing majority practice)

| Entity kind | Convention | Status |
|---|---|---|
| Domain entity | bare noun (`TiltSeries`, `PickList`) | ✅ keep |
| Job params | `*Params` in `services/jobs/{jobtype_value}.py` | ✅ keep |
| Per-job artifact | `*Output` (`{Job}{Scope}Output`) | ✅ keep |
| Stateful injected collaborator | `*Service`, factory `get_{full_snake_name}()` | mostly ✅; fix `get_deletion_service`→`get_pipeline_deletion_service`, `get_prefs_service`→`get_user_prefs_service`; `PipelineRunnerService`→`PipelineRunner` (doubled agentive suffix) |
| Stateless format I/O | module functions (or `*Codec`); **no** `Service` suffix for method-bags | `StarfileService`, `MdocService` → `services/formats/starfile.py`, `mdoc.py` |
| Authoritative store | `*Registry`, `get_{x}_for(key)` | ✅ keep |
| Format→domain ingester | `*IngestAdapter` in `services/{domain}/adapters/` | ✅ keep |
| Value object / typed id | bare noun (`InstanceId`) | new |
| UI region w/ lifecycle | `*Panel` | standardize (`IOConfigComponent`→`IOConfigPanel`) |
| UI reusable leaf | `*View` (matches `FingerprintedView` base) | standardize (`RosterWidget`) |
| UI modal | `*Dialog`, must actually wrap `ui.dialog` | standardize |
| Per-tab state | `*UIState` | mostly ✅ |
| Banned | `*Manager`, `*Helper`, `*Util`, `*Handler`, `*Data` | `UIStateManager`→`UIStateStore`; `TiltSeriesData`→`TiltSeriesTable` |
| Enum values | `lower_snake` derived from member (the `JobFileType` rule) | migrate `JobType` (§9.1) |
| Casing | snake_case identifiers, no exceptions; acronyms title-cased in classes (`TsCtf` not `TsCTF`) | fixes adapter defaults, `IOConfigComponent` |
| Cross-module imports | no `_`-prefixed name may be imported across modules (26 sites today; `ui/dashboard/`'s whole API is private-by-name after R0) | rename on next touch |

### 9.4 Package taxonomy moves

- `services/configs/` → keep only actual config (`config_service`, `user_prefs_service`,
  `dataset_selection_cache`); move `starfile/mdoc/dataset_parsing/metadata` parsers to
  `services/formats/`.
- `services/visualization/` → split: `services/particles/` (coords, pick_merge, picks_filter,
  list_extraction, subtomo_link — the load-bearing half) + `services/rendering/` (preview_render,
  recon_cutouts, cutout_filters, preview_orchestrator, imod_vis, artiax_bridge).
- `services/` root: `project_state.py` → `services/project/` (state / models / migrations / registry
  accessors — it is 3+ modules today, incl. ~200 lines of migrations); `path_resolution_service.py` +
  `io_slots.py` → one `services/io/` package (they are one system); `dataset_models.py` +
  `tomogram_import.py` → `services/tilt_series/`; `aggregation_*` → `services/aggregation/`;
  delete the two zero-byte modules.
- `scheduling_and_orchestration` → `orchestration` (only `X_and_Y` package name).
- Move the 292 KB of in-package `.md` plans to `docs/`. Rename/relocate `filterTilts/` (mixed-case
  dir) after the deletion pass leaves only `image_processor.py` + 2 DL modules.
- `ui/dashboard/css.py` (916 lines, one CSS string) → a real `.css` static asset.
- 179 intra-project function-local imports: after the `services→ui` break and the
  `path_resolution ↔ job_models` cycle fix (via the JobSpec registry), most become promotable to
  module level; require a cycle-naming comment for any that remain. Note
  `pipeline_monitor.py:216` imports the **private module global** `_project_states` across packages —
  give the registry a real accessor.

---

## 10. Dead code inventory (delete first, ~2,400 lines, zero risk)

| Item | Lines | Evidence |
|---|---|---|
| `MetadataTranslator` (`services/configs/metadata_service.py:153-922`) | ~770 | never instantiated; superseded by the 4 adapters (their docstrings say so). Keep `WarpXmlParser` (`:21-152`) → promote to `services/formats/warp_xml.py`. Contains the `pixS = 1.35 # Fallback` rule-violation (`:517-519`), which dies with it. |
| `filterTilts/`: `filterTiltsInt.py`, `filterPipeline_orchestrator.py`, `deepLearning_orchestrator.py`, `plotter.py`, `star_handler.py`, `warpProjectHandler.py`, `filterTiltsRule.py` | ~1,600 | imported only by each other; live set is `image_processor.py`, `deepLearning/model_loader.py`, `statistics_calculator.py`, `model_architectures.py` (via model_loader) |
| `services/parameters_service.py`, `services/container_service.py` | 0-byte | no importers |
| `services/job_models.py` shim | 30 | after repointing 14 importers to `services.jobs` (also fixes the `TiltFilterParams` `ImportError`) |
| Adapter default `job_instance_id`s | — | dead and wrong (`"tsCTF"`, `"tsAlignment"`); make the kwarg required |

Also housekeeping: `transient_scripts/` (590 tracked lines announcing their own disposability),
root-level `roadmap_*.md` → `docs/`.

---

## 11. Bugs found during the audit (actionable independently of any refactor)

1. **Pipeline-start failures display "None"** — `"message"` produced
   (`pipeline_orchestrator_service.py:69,290,339,373,394`) vs `"error"` consumed
   (`pipeline_builder_panel.py:515-516`).
2. **`from services.job_models import TiltFilterParams` raises `ImportError`** — shim omits it
   (`services/job_models.py:10-30`).
3. **`starfile_service.py:25-27`** — except branch repeats the identical failing write; misleading log
   then unhandled raise.
4. **Ambiguous polarity silently stored as `"black"`** (`ui/template_workbench.py:343`).
5. **Unknown tool name silently becomes `ToolConfig(exec_mode="binary", bin_path=<typo>)`**
   (`config_service.py:331`).
6. **`ui/job_plugins/__init__.py:37` renderer signature comment is stale** (4 args documented, 5 passed).
7. **`ts_alignment` array driver enumerates TS from a directory glob** (`drivers/ts_alignment.py:54-57`)
   against `array_job_base.py:176-183`'s own warning — divergence risk vs the input STAR.
8. **`error == "no_coords_found"` string-typed error code** (`ui/tomo_dashboard_dialog.py:4242`) —
   fragile against message rewording.

---

## 12. Recommended sequencing

Composes with the two standing roadmaps (`docs/registry-consolidation-roadmap.md`, orchestrator
replacement plan). Notably: **don't invest in splitting `pipeline_runner.py`** (1,480 lines, five
services in one class) — the orchestrator replacement rips out the schemer half anyway; splitting now
is wasted motion.

- **Phase 0 — Delete + lint (days).** §10 deletions; `ruff.toml` target-version + `UP`/`B`/`RUF`;
  fix bugs §11.1-3 (tiny diffs).
- **Phase 1 — Typed spine (1-2 weeks, mechanical).** `ServiceResult`; `DriverContext` dataclass;
  `InstanceId` value object; `SlotKey`/`paths: dict[SlotKey, Path]`; `JobSpec` registry replacing the
  8 tables; abstractmethods + declared `INPUT_SCHEMA`/`OUTPUT_SCHEMA` on the base; StrEnums for
  RELION/SLURM status + tool names. Each item is independently landable.
- **Phase 2 — Boundary (per-slice).** Break `services→ui`; extract `CurationSessionService` from
  `backend.py`; relocate `ui/dashboard/data.py`, `pixel_sanity.py`, `task_utils.py` into `services/`;
  funnel `save_project` through the facade; move `_finalize_pipeline_output` into
  `services/jobs/tilt_filter.py`.
- **Phase 3 — Drivers (1 week).** `ArrayDriver` template; `ToolCommand`/`run_tool`;
  `TaskStatusStore`; `BaseIngestAdapter` + real adapters for denoise_predict/tilt_filter;
  `subtomo_merge` → services.
- **Phase 4 — Registry consolidation (already greenlit).** Dashboard-on-registry; delete the
  ProjectState TS mirror; `ProjectState.load` → discriminated union; then the `JobType` value
  migration behind `from_string`, which unlocks the four-layer module-name unification.
- **Continuous.** Naming convention table (§9.3) enforced on touched code only — no big-bang rename.

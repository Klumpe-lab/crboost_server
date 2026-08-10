# Roadmap 04 — Drivers: consolidate without changing what works

## Prime directive (from maintainer feedback)

The drivers **work**, ugly or not, and they run unattended on compute nodes where debugging is
expensive. Therefore: every stage here is **behavior-preserving by construction** — pure code motion
whose correctness is verifiable by diff reading — and every *known divergence* between drivers is
treated as a flagged decision, never silently "fixed" during a move. Rollout is one driver per
commit, runtime-verified on a sandbox project before the next.

## Before → After

**Before:** eight array drivers each hand-assemble the same ~60-line supervisor/task lifecycle from
`array_job_base`'s verbs (~475 duplicated lines); 12 hand-rolled command builders with inconsistent
quoting (6/18 files use `shlex.quote`) and 11 hardcoded tool-name strings; the
`.task_manifest.json`/`.task_status` protocol implemented three times (drivers, `pipeline_runner`,
UI) with drift risk in what "done" means; four registry ingest adapters with copy-pasted internals
and two drivers stamping the registry by hand instead; `subtomo_merge.py` — a library — living in
`drivers/` and mirrored in `services/` to dodge the import direction.

**After:** one `ArrayDriver` template class owns the lifecycle; one `ToolCommand`/`run_tool` owns
command construction and container wrapping; one `TaskStatusStore` owns the status protocol for all
three consumers; one `BaseIngestAdapter` owns adapter internals. A new driver becomes ~100 lines of
tool-specific logic. **Gain:** the next per-TS job type (IsoNet split, missAlign P2) is cheap and
consistent; protocol drift between UI/runner/drivers becomes impossible; quoting/naming
inconsistencies (live injection-hazard class) disappear.

## Stage 0 — gather first

1. **Divergence ledger.** Table of every known behavioral difference, each with a decision
   (KEEP as intentional / ALIGN in an isolated flagged commit):
   - `subtomo_extraction` reads `manifest["items"]` vs everyone's `manifest["ts_names"]`, and
     `sys.exit(1)`s where others raise (`drivers/subtomo_extraction.py:382-387`).
   - `subtomo_extraction.py:202-230` reimplements `apply_exclusions` (same reason string, hand-copied).
   - `ts_alignment.py:54-57` enumerates TS via `tomostar_dir.glob("*.tomostar")` against
     `array_job_base.py:176-183`'s own written warning (input-star is canonical). **Functional
     question**: does any real project depend on glob picking up TS absent from the star? Decide with
     the maintainer before aligning.
   - `denoise_predict` skips `preflight_registry` and stamps the registry by hand (`:283-327`);
     `tilt_filter.py:134-157` likewise.
   - `ts_ctf` copies (not symlinks) the per-TS XML during staging — documented as deliberate
     (`ts_ctf.py:121-123`); KEEP, encode as a template-hook parameter.
   - `ts_alignment`'s tolerant results tally (`:266-289`) vs everyone's strict one.
2. **Command transcripts.** Before any builder migration, capture the exact command string each
   driver currently builds for the sandbox projects (add a temporary `CRBOOST_PRINT_CMD=1` env check
   that prints-and-continues, or have the user run one array job per driver and save the
   `[TASK n] Command:` log lines). These transcripts are the acceptance test for `ToolCommand`:
   **byte-identical commands** (modulo agreed quoting fixes, which go in their own commit).
3. **Confirm the memory correction:** `denoise_predict` *is* array-migrated; `denoise_train` is
   legitimately a global reduction (no array). No migration work needed there beyond adapter/status
   adoption; update `DENOISE_PER_TS_SPLIT_PLAN.md` if it still says otherwise.

## Stages

1. **`TaskStatusStore`** (new module, e.g. `services/array_tasks.py` — shared home with Roadmap 01
   stage 3): constants + read/write/atomic-replace for `.task_manifest.json`, `.task_status/*`,
   `task_*.out`. Convert the three implementations (`array_job_base`, `pipeline_runner.py:1013-1034`,
   `ui/components/task_utils.py:104-126`) to it. Pure consolidation; the on-disk format is frozen —
   any format change would orphan running jobs mid-flight.
2. **`ToolCommand` + `run_tool()`** in `driver_base.py`: builder (`.arg()/.flag()/.paths()`) +
   one runner folding tool-name resolution (`params.get_tool_name()`), bind dedup, consistent
   quoting, the `[TASK n] Command:` log line, container wrapping, retries. Migrate one driver per
   commit against the Stage-0 transcripts. The 11 hardcoded tool-name strings become
   `get_tool_name()` calls in the same commits (transcript-visible, safe). Quoting *changes* (the
   12 builders disagree) are collected into one final flagged commit, not sprinkled.
3. **`ArrayDriver` template class** in `array_job_base.py`: hooks
   `enumerate_items() / stage(item) / build_command(item) / collect(item) / aggregate()`; the base
   owns mode dispatch, both bootstrap try/excepts, manifest index lookup, exclusion application,
   results tally, `RELION_JOB_EXIT_*` markers, fail-status handler, and the `[SUPERVISOR]`/`[TASK n]`
   print helpers. Migrate in risk order: `ts_reconstruct` (simplest) → `ts_ctf` → `fs_motion_and_ctf`
   → `template_match` → `extract_candidates` → `denoise_predict` → `ts_alignment` (divergences) →
   `subtomo_extraction` (most divergences). One driver per commit; each divergence resolves per the
   Stage-0 ledger — ALIGN items land as separate flagged commits *after* the pure migration of that
   driver.
4. **`BaseIngestAdapter`** in `services/tilt_series/adapters/_base.py`: shared `__init__`,
   `_read_only_block`, `_resolve_per_ts_path`, excluded-ids filtering; the four adapters shrink to
   their parsing cores; `denoise_predict` and `tilt_filter` get real adapters replacing hand-rolled
   registry stamps. Coordinates with Roadmap 02 stage 5 (required `job_instance_id`).
5. **Library relocations:** `drivers/subtomo_merge.py` → `services/` (then
   `services/visualization/list_extraction.py:38-90` imports it instead of mirroring — its docstrings
   already apologize for the copy); single-source the driver-invocation command string
   (`pipeline_orchestrator_service.py:529-541` ≡ `array_job_base.py:445-454`); port
   `extract_pick_list.py` onto `get_driver_context` (today it hand-rolls argparse and bypasses the
   bootstrap entirely).

## Modern-Python weave-in

- `ArrayDriver` hooks as `abc.abstractmethod` — forgetting one fails at class definition, not at
  3 a.m. on a compute node.
- `DriverContext` frozen dataclass replacing `get_driver_context()`'s 6-positional-tuple +
  bare-dict (17 unpack sites under three different names). Do it as part of stage 3's per-driver
  migrations so each driver is touched once, not twice.
- `IngestAdapter` Protocol (Roadmap 02) + the stage-4 base class: Protocol states the contract,
  base class shares the code.
- `ToolName(StrEnum)` for the tool registry, with `config_service.get_tool_config` raising on
  unknown instead of inventing `ToolConfig(bin_path=<typo>)` — this is a *behavior* change
  (flagged commit) but a pure win: today a typo silently tries to exec a binary named after itself.
- `match` on the driver `--mode` dispatch in the template class.

## Runtime checklist

Per migrated driver: run that job type on `projects/try2_after_pixShift` (or `pos9_10` for
tilt-filter-adjacent), confirm: same task count dispatched, `.skip` honored for an excluded TS,
`.task_status` settles identically, `RELION_JOB_EXIT_SUCCESS` lands, registry entries appear
(compare a per-TS JSON before/after), emitted stars byte-comparable to a pre-refactor run of the
same inputs. After stage 1: also open the roster UI and the runner's status view mid-run — all three
protocol consumers must agree on task states in real time.

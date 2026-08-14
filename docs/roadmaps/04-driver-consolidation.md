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

> **DONE 2026-08-13** → `04-stage0-census.md`: 75-entry divergence ledger (32 KEEP / 30 ALIGN /
> 13 ASK), corrected builder counts (18 tool-command builders, 6/18 shlex, 12 hardcoded tool_name=
> sites + 13 hardcoded binary names), 10-item status-protocol drift list, adapter duplication map,
> transcript harvest. **Transcript caveat:** all harvested logs predate the Aug-11/12 driver code
> AND the per-TS array refactor (array-mode exemplars exist only in `demo` for fsMotion/tsAlign/
> tsCtf) — fresh transcripts are required before stage 2 migrates each builder.
> **Resolved 2026-08-13:** post-echo transcripts harvested byte-exact into
> `04-stage2-transcripts.md` (fsMotionAndCtf, tsImport, tsAlignment, tsCtf, tsReconstruct from
> `stage4refactor_demo`); remaining job types still need runs.

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
   adoption. (Confirmed against code 2026-08-13, see `04-stage0-census.md`;
   `DENOISE_PER_TS_SPLIT_PLAN.md` no longer exists in the repo — nothing to update there.)

## Stages

1. **`TaskStatusStore`** (new module, e.g. `services/array_tasks.py` — shared home with Roadmap 01
   stage 3): constants + read/write/atomic-replace for `.task_manifest.json`, `.task_status/*`,
   `task_*.out`. Convert the three implementations (`array_job_base`, `pipeline_runner.py:1013-1034`,
   `ui/components/task_utils.py:104-126`) to it. Pure consolidation; the on-disk format is frozen —
   any format change would orphan running jobs mid-flight.

   > **DONE 2026-08-13** (code + first runtime session). Commits: `69b9843` (`services/array_tasks.py`
   > grows `TaskProgress` — single settledness arithmetic, skip=settled — plus `manifest_items` /
   > `manifest_array_job_id` / `any_task_started` / `mark_stopped_tasks_failed`), `db6813a`
   > (`pipeline_runner` converted; commit message carries a stale roadmap-03 label — content is this
   > conversion), `c030c29` (roster tallies via `TaskProgress`, skip-counts-as-settled fix, gray ⊘n
   > chip), `7593368` (task tracker via `TaskProgress.from_statuses`). Related landed in the same
   > session: `670a835` (all 13 census ASK decisions recorded), `bb8cafe` (ts_ctf supervisor copies
   > alignment XMLs only for non-`.ok` TS — ledger #10), `71a66ff` (`run_command` echoes
   > `[run_command] $ <cmd>` into every job log — feeds stage 2 transcripts), `3056ba3`
   > (ts_alignment adapter identity check requires ingested TS present in all sources; extras
   > downgrade to warnings — found at runtime, see below).
   >
   > **Runtime verification** on `/groups/klumpe/crboost_data/stage4refactor_demo/` (5 TS,
   > `Position_1` muted in Journey): mute worked end-to-end — alignment array pre-marked the muted TS
   > `.skip`, ran 4 tasks. First run went RED because the adapter's identity check demanded full
   > set-equality across tomostar/XML/tiltstack and the muted TS legitimately has only a tomostar →
   > fixed in `3056ba3`. Redeployed chain all green: job006 tsAlignment (manifest enumerates all 5,
   > status 4×`.ok`+1×`.skip`), job007 tsCtf (manifest enumerates the 4 live TS, 4×`.ok`, supervisor
   > `ts_defocus_hand --check`/`--set_flip` ran once), job008 tsReconstruct dispatched 4 tasks.
   > Note the enumeration asymmetry (muted TS in-manifest-but-skipped at alignment vs
   > dropped-from-manifest downstream) — feeds ledger #38/#39 (registry as enumeration authority).
2. **`ToolCommand` + `run_tool()`** in `driver_base.py`: builder (`.arg()/.flag()/.paths()`) +
   one runner folding tool-name resolution (`params.get_tool_name()`), bind dedup, consistent
   quoting, the `[TASK n] Command:` log line, container wrapping, retries. Migrate one driver per
   commit against the Stage-0 transcripts. The 11 hardcoded tool-name strings become
   `get_tool_name()` calls in the same commits (transcript-visible, safe). Quoting *changes* (the
   12 builders disagree) are collected into one final flagged commit, not sprinkled.

   > Design approved 2026-08-13 — `04-stage2-toolcommand-design.md`, all five §7 decisions resolved
   > as recommended (D1 tagged-verb builder with mandatory `quote=`; D2 compound shell stays literal
   > f-strings; D3 `crboost` deferred to the flagged StrEnum commit; D4 `--print-cmd` snapshots gate
   > the 9 uncovered types; D5 `run_tool` takes `str | ToolCommand`). The sketch's `.arg()/.paths()`
   > verbs were dropped: the census showed per-site quoting policy differs and is part of the bytes,
   > so `opt_path(..., quote=)` transcribes each site's existing policy instead of unifying it.
   >
   > **CODE-COMPLETE 2026-08-13** for the 5 transcript-covered drivers, byte-parity pending runtime.
   > `driver_base` grew `ToolCommand` (insertion-order `" ".join`; `flag/opt/opt_path/raw`; no
   > reordering, dedup, validation, or implicit quoting) and `run_tool()` (renders, wraps via
   > `wrap_command_for_tool`, dispatches to `run_command` or `run_command_with_retries`). Migrated in
   > transcript order: `ts_reconstruct`, `ts_ctf` (incl. the `ts_defocus_hand` mini-script — three
   > near-identical commands via a local helper, compound `$()`/`if` shell left literal),
   > `ts_alignment` (method dispatch returns `ToolCommand`; the unimplemented-method error shim stays
   > a `str`), `fs_motion_and_ctf`, `ts_import`. Tool-name literals converted: `"warptools"` × 2
   > (fs_motion, ts_import) → `params.get_tool_name()`, verified to return the same string;
   > `extract_pick_list` untouched per census #73. fs_motion's build-then-`.replace("'*.eer'", ...)`
   > patch died — the extension is now a builder argument.
   >
   > **`--print-cmd` snapshot mode** (D4) landed with it, as `CRBOOST_PRINT_CMD=1`. The gate lives in
   > `run_command`, NOT in `run_tool`, on purpose: every driver funnels through `run_command` whether
   > migrated or not, so a snapshot taken on pre-migration code is directly diffable against one
   > taken after. Prints the usual `[run_command] $ ...` line, fully container-wrapped, and returns
   > without executing. Caveats: the driver's Python-side work (staging, manifest/star writes) still
   > runs, so snapshot against a scratch copy; and a driver that validates tool output right after
   > the call aborts there, giving a partial-but-deterministic (hence still diffable) transcript.
   >
   > **BYTE-PARITY VERIFIED 2026-08-13** — all 5 transcript-covered types, live post-migration run
   > on `/groups/klumpe/crboost_data/deadcode_stage5_pre/` (5 TS, none muted, drivers edited 16:02
   > vs jobs run 16:15+). Every `[run_command]` line diffs clean against `04-stage2-transcripts.md`
   > after normalizing only project name, job number (this project has no TiltFilter, so alignment
   > is job004 not job006) and TS name: fs_motion (job002), ts_import (job003), ts_alignment
   > (job004), ts_ctf task + the `ts_defocus_hand --check && --set_flip` supervisor mini-script
   > (job005), ts_reconstruct (job006). Full chain green, 5×`.ok` per array job,
   > `RELION_JOB_EXIT_SUCCESS` on all five.
   >
   > **Remaining 9 builders migrated the same day** (no transcripts; parity by transcription
   > discipline + `--print-cmd` available as the gate): reconstruct_particle, class3d,
   > subtomo_extraction, extract_pick_list (RELION group); template_match, extract_candidates
   > (PyTOM group, incl. the `-g 0 1 2` / `-s` splats as `.raw()`); denoise_predict, denoise_train,
   > miss_align (env-prefix group — `TF_FORCE_GPU_ALLOW_GROWTH=... cryoCARE_predict.py` and
   > `env HOME=... miss-alignment train` become the ToolCommand exe, which is where the shell needs
   > them). `subtomo_merge` has no tool execution — nothing to migrate. After this pass
   > `wrap_command_for_tool` appears in `drivers/` ONLY inside `run_tool`.
   >
   > Tool-name literals: 12 of 17 converted. Five deliberately kept, each with an in-code reason —
   > `extract_pick_list` (census #73, no param class until #68) and the four calls inside the
   > IsoNet-only helpers `run_isonet_predict_task` / `run_isonet_train`. In those helpers the
   > *branch*, not `params`, is what makes the tool IsoNet; `params.get_tool_name()` is
   > method-conditional and would answer `"cryocare"` if the helper were ever called off the ISONET
   > branch, so the literal is strictly safer than the indirection. The two cryoCARE-branch calls in
   > `denoise_train.main()` WERE converted — the IsoNet branch `sys.exit(0)`s before them, so
   > `get_tool_name()` is unambiguous there. The two native `tar` calls stay on bare `run_command`
   > (design non-goal — they are not container-wrapped).
3. **`ArrayDriver` template class** in `array_job_base.py`: hooks
   `enumerate_items() / stage(item) / build_command(item) / collect(item) / aggregate()`; the base
   owns mode dispatch, both bootstrap try/excepts, manifest index lookup, exclusion application,
   results tally, `RELION_JOB_EXIT_*` markers, fail-status handler, and the `[SUPERVISOR]`/`[TASK n]`
   print helpers. Migrate in risk order: `ts_reconstruct` (simplest) → `ts_ctf` → `fs_motion_and_ctf`
   → `template_match` → `extract_candidates` → `denoise_predict` → `ts_alignment` (divergences) →
   `subtomo_extraction` (most divergences). One driver per commit; each divergence resolves per the
   Stage-0 ledger — ALIGN items land as separate flagged commits *after* the pure migration of that
   driver.

   > **BASE + 2 DRIVERS 2026-08-13, PENDING RUNTIME.** `ArrayDriver(ABC)` in `array_job_base.py`
   > owns mode dispatch, both bootstrap try/excepts, manifest index lookup, exclusion pre-marking,
   > the results tally, `RELION_JOB_EXIT_*`, the fail-status handler, and a `self.log()` that
   > stamps `[SUPERVISOR]` / `[TASK n]` so a hook's line reads the same whichever mode emitted it
   > (absorbs the dim-12 log-format ALIGNs). Abstract: `enumerate_items`, `build_command`,
   > `aggregate`. Optional, each defaulting to a no-op so a driver that doesn't need one doesn't
   > mention it: `item_metadata`, `manifest_extras`, `pre_dispatch`, `task_already_done`, `stage`,
   > `task_cwd`, `verify_outputs`, `collect`. `retry_attempts` class attr replaces the
   > run_command/run_command_with_retries choice.
   >
   > `DriverContext` (frozen, generic over the param class) landed in `driver_base.py` with it:
   > `DriverContext.load(TsReconstructParams)` returns a `DriverContext[TsReconstructParams]` and
   > does the two conversions every call site was re-doing by hand (`paths` → `Path`, binds → list).
   > Since ArrayDriver owns the bootstrap, subclasses never call `get_driver_context` at all.
   >
   > **ts_reconstruct** (pilot) 279 → 131 lines. Ledger #0 applied (dropped the duplicated local
   > `read_tilt_series_names_from_input_star` + its lone-purpose `StarfileService` import for the
   > base one). #1 (in-task idempotency) → `task_already_done`; #2 (fail-loud MRC check) →
   > `verify_outputs` — both promoted from "documented deviation" to named base hooks.
   >
   > **ts_ctf** 396 → 275 lines, and the reason it was migrated second: it stresses the hook design
   > hardest. #4 (supervisor runs real compute before dispatch — the global `ts_defocus_hand`) is
   > exactly `pre_dispatch`; the task runs with cwd = its staging dir, which is `task_cwd`; the
   > XML copy-back is `collect`. Nothing about ts_ctf needed a new hook, which is the evidence the
   > hook set is right.
   >
   > ONE deliberate behavioral delta in the ts_ctf move: `preflight_registry` now runs BEFORE
   > `pre_dispatch` instead of after the copy+defocus_hand compute. It is read-only (validates,
   > prints, raises) and its own docstring asks to run early, so this is the fail-fast direction —
   > but it is a change, not a pure move, hence flagged here.
   >
   > ts_ctf ALIGNs landed as a separate follow-up commit per the prime directive: #5 (defocus_hand
   > gets `attempts=3` + the `[run_command]` echo it now inherits from `run_tool`), #7 (missing
   > tomostar raises instead of staging a no-op that green-ticks — the maintainer's ASK decision),
   > #9 (a vanished staged XML raises instead of silently skipping copy-back and writing `.ok`).
   > **#8 CLOSED 2026-08-13, PENDING RUNTIME.** The precondition first: `TsCtfParams.OUTPUT_SCHEMA`
   > declares `output_processing` with `path_template="warp_tiltseries/"`, and `resolve_outputs`
   > renders it as `(job_dir / "warp_tiltseries/").resolve()` — byte-identical to the hardcoded
   > default, and `as_paths_dict` has no key collision for tsCtf, so the key is always present when
   > the bootstrap succeeded. The `paths.get(..., default)` was therefore dead and is now
   > `ctx.paths["output_processing"]`, in the supervisor AND in `stage`/`collect`.
   >
   > The other two literals are NOT resolver paths and the ledger entry mis-stated them: the task
   > reads `job_dir/warp_tiltseries.settings` and `job_dir/tomostar`, which are **job-local copies
   > the supervisor's `pre_dispatch` makes** — nothing declares them as OutputSlots, and the
   > resolver's `warp_tiltseries_settings` INPUT points at the upstream alignment file, not at our
   > copy. So they are a supervisor↔task contract, now named once as `LOCAL_SETTINGS_NAME` /
   > `LOCAL_TOMOSTAR_NAME` with the reason in-code. This closes the exact drift the ledger warned
   > about: the supervisor used to name its copy after the *upstream* file (`settings_file.name`)
   > while the task looked for the literal `"warp_tiltseries.settings"` — identical today, since all
   > four `WARP_TILTSERIES_SETTINGS` producers use that `path_template`, but a fail-hard mismatch the
   > moment one didn't. The staged-dir literals inside `.staging/task_*/` are left alone on purpose:
   > those are dictated by the settings file's own relative `DataFolder`/`ProcessingFolder` keys.
4. **`BaseIngestAdapter`** in `services/tilt_series/adapters/_base.py`: shared `__init__`,
   `_read_only_block`, `_resolve_per_ts_path`, excluded-ids filtering; the four adapters shrink to
   their parsing cores; `denoise_predict` and `tilt_filter` get real adapters replacing hand-rolled
   registry stamps. Coordinates with Roadmap 02 stage 5 (required `job_instance_id`).

   > **BASE CLASS DONE 2026-08-13, PENDING RUNTIME.** `BaseIngestAdapter` owns the constructor head
   > (`registry` / `job_dir` / `job_instance_id` / `warp_folder` / `warp_dir` / `starfile_service`),
   > `_read_only_block` (was copied 3×), `_resolve_per_ts_path` (2×), and the exclusion pair
   > `_excluded_set` / `_drop_excluded` (3×). Subclasses declare `DEFAULT_WARP_FOLDER` instead of a
   > per-`__init__` default; `fs_motion_ctf` and `ts_ctf` now have NO `__init__` at all,
   > `ts_reconstruct` keeps one for `rec_dir` and `ts_alignment` for `tiltstack_dir`/`tomostar_dir`.
   > Deliberately not an ABC: `ingest`/`emit_star` have genuinely different signatures per job type
   > (pixel sizes, alignment method, project root), and unifying them would mean `**kwargs` soup.
   > Two incidental attribute additions, both write-only and unread: `ts_ctf` now also sets
   > `warp_folder`, `ts_reconstruct` also sets `warp_dir` (`rec_dir` derives from it identically).
   >
   > **NOT DONE — needs a maintainer decision, not code motion.** The `denoise_predict` /
   > `tilt_filter` adapters do not fit this base, and forcing them would change failure semantics:
   > both stamps are **best-effort by explicit design** (`stamp_denoise_registry`,
   > `drivers/denoise_predict.py`; the frame-verdict stamp, `drivers/tilt_filter.py:134-157`) —
   > wrapped in a broad catch that only warns, on the stated grounds that a registry failure must
   > never fail a job whose real outputs already exist. Every adapter in this family is fail-loud.
   > `tilt_filter` also has no adapter *shape*: it calls `set_frame_filtered` per frame stem, with no
   > typed output attachment and no `emit_star`. **ASK:** should a registry-stamp failure fail those
   > two jobs (adopt the family's fail-loud contract) or keep warning (and stay outside the family)?
5. **Library relocations:** `drivers/subtomo_merge.py` → `services/` (then
   `services/visualization/list_extraction.py:38-90` imports it instead of mirroring — its docstrings
   already apologize for the copy); single-source the driver-invocation command string
   (`pipeline_orchestrator_service.py:529-541` ≡ `array_job_base.py:445-454`); port
   `extract_pick_list.py` onto `get_driver_context` (today it hand-rolls argparse and bypasses the
   bootstrap entirely).

   > **DONE 2026-08-13 (2 of 3), PENDING RUNTIME.**
   >
   > **subtomo_merge → `services/subtomo_merge.py`.** Pure move (shebang dropped — it has no
   > `__main__`). Repointed: `drivers/subtomo_extraction.py` (6 private names),
   > `drivers/extract_candidates_pytom.py`, and `ui/aggregation_merge_card.py` — which was a
   > **`ui/` → `drivers/` import**, a real boundary violation `check_boundaries.py` does not yet
   > catch (R1 only bans the reverse direction). `list_extraction.py`'s two hand-mirrored copies
   > (`_parse_optimisation_set`, `_write_optimisation_set`) are deleted in favour of the originals;
   > the only behavioural delta is that relative paths in a parsed optset now come back `.resolve()`d,
   > which changes no file we write (both writers `.resolve()` at write time). Names left private
   > (`_parse_optimisation_set` and friends) on purpose: 7 cross-module call sites already import them
   > that way, and renaming would churn a driver this stage is not otherwise touching.
   >
   > **Driver-invocation string → `services/jobs/spec.py`.** `driver_launch_prefix()` (the
   > `export PYTHONPATH=…; <python> <script>` head) + `driver_invocation()` (that plus
   > `--instance_id/--project_path`). Byte-identical at every site: the orchestrator's
   > `python_exe = "python3"` fallback was a `str` and array_job_base's a `Path`, which render the
   > same. The census named two call sites; there was a **third** — `backend.extract_pick_list`
   > (`backend.py:263-283`) carries the same prefix with a different argument set, which is why the
   > prefix is split out rather than folded into one function.
   >
   > **`extract_pick_list` → `get_driver_context`: NOT APPLICABLE, and the roadmap item is wrong.**
   > This driver is not a pipeline job: `backend.extract_pick_list` submits it as a one-off sbatch
   > with `--candidate-optset/--list-star/--tomo/--out-dir/--project-root`, it has no `--instance_id`,
   > no entry in `project_state.jobs`, and no param class (census #73). `get_driver_context` requires
   > all three — it looks the instance up in `project_state.jobs` and `sys.exit(1)`s on a miss, then
   > runs full IO-slot path resolution. Everything this driver *can* share it already does
   > (`ToolCommand`, `run_tool`, and now the launch prefix). Genuinely blocked on census #68 giving it
   > a job identity; re-open this item there.

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

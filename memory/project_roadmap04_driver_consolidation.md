---
name: project_roadmap04_driver_consolidation
description: Roadmap 04 CODE-COMPLETE 2026-08-14 — ALL 8 array drivers on ArrayDriver (6 migrated in one session, PENDING RUNTIME), both remaining ASKs decided (registry-stamp = FAIL LOUD landed in denoise_predict+tilt_filter; #68 → roadmap 07 doc, first slice of denovo UI pass); ts_alignment carries the #38/#39 registry-authority pilot
metadata: 
  node_type: memory
  type: project
  originSessionId: 9e94b14c-f7d1-4054-a4d6-6edfe7541613
  modified: 2026-08-14T09:23:54.866Z
---

Roadmap 04 (docs/roadmaps/04-driver-consolidation.md) started 2026-08-13, chosen over orchestrator
replacement. Stage 0 census COMPLETE → **docs/roadmaps/04-stage0-census.md** (75-entry divergence
ledger 32 KEEP / 30 ALIGN / 13 ASK, command-builder census, status drift list, adapter map,
transcript harvest, denoise facts). Produced by 12-agent workflow; adversarial verify pass never ran
(session limits) — each entry re-verified at its driver's migration commit instead.

Key census corrections vs the roadmap text:
- **18** tool-command builders (not 12); 6/18 shlex confirmed; **12** hardcoded tool_name= sites
  (not 11) + 13 hardcoded binary names inside command strings.
- `services/array_tasks.py` ALREADY exists (roadmap-01 stage 3) — stage 1 is folding the remaining
  hand-rolled sites onto it (pipeline_runner: 3 inline manifest-read variants + hand-rolled atomic
  write at ~:1015; roster + tracker tallies), not creating a module. 10-item drift list in census
  §status; 3 incompatible "settled" tallies, both-markers precedence contradiction, sticky .skip.
- **Harvested transcripts are NOT usable for stage-2 byte-identity**: all predate Aug-11/12 code and
  mostly predate the per-TS array refactor; array-mode exemplars only in projects/demo for
  fsMotion/tsAlign/tsCtf; no transcript at all for denoise_*, miss_align, tilt_filter. Fresh
  transcripts needed (one sandbox run per driver, or CRBOOST_PRINT_CMD print-and-continue hook ≈
  [[project_protocols_regression_harness]] Tier A).
- wrap_command_for_tool is string-in/string-out; compound shell (guards, $() capture, env prefixes,
  literal '*.eer' tokens, post-build .replace patching) is load-bearing → ToolCommand must emit flat
  strings, not argv.

All 13 ASK entries DECIDED 2026-08-13 — recorded in census §"ASK decisions (maintainer, 2026-08-13)".
Highlights: blanket never-guess/hard-fail policy with per-TS reason badge (containment: broken per-TS
input pre-marks THAT TS .fail with reason, others proceed); #10 fix = copy alignment XMLs only for
non-.ok TS (recompute-all rejected: defeats sparse re-dispatch); #38/#39 = registry becomes
enumeration authority + dispatch-time set-consistency drift check, no stale snapshots (pilot at
ts_alignment migration); #25 = coordinate list is the fundamental object, threaded through species
registry (interim: loud star-vs-TM-set diff); #19 = never edit primary star files, mutedness lives
in project state/registry; #56 deferred; #30 keep; #68 = real UI-visible per-list job identity
(extractPickList__<slug>), NO hidden instances — blocks stage 5 only.

Stage 1 COMMITTED 2026-08-13 (py_compile + check_boundaries clean; pipeline_runner commit db6813a
carries a stale roadmap-03 label — content is the roadmap-04 conversion, cosmetic only). ts_ctf #10
fix also COMMITTED (bb8cafe). Both PENDING RUNTIME. Stage-1 detail (on-disk format frozen): array_tasks.py grew
TaskProgress (single settledness arithmetic, skip=settled), manifest_items/manifest_array_job_id/
any_task_started, and mark_stopped_tasks_failed (moved verbatim from pipeline_runner's
_finalize_stopped_task_statuses; still bypasses superseded-guard by post-scancel convention —
phase-4 territory per docs/roadmaps/roadmap_task-status-state-machine.md). pipeline_runner consumes the
store (3 inline manifest parses + hand-rolled .fail writer deleted; zero protocol literals left);
roster _get_array_progress returns TaskProgress — carries the ONE flagged behavior change: skipped
(muted) TS count as settled (fixes 5/6-forever roster pin) + new gray ⊘n chip; tracker tallies via
TaskProgress.from_statuses. Verification ceiling hit: ruff check/format clean, venv python dead →
user owes py_compile + check_boundaries.py + the stage-1 runtime check (roster chip, tracker tab,
runner QUEUED→RUNNING flip, and supervisor tally must agree mid-run on a job with a muted TS).

Runtime session 2026-08-13 (demo project /groups/klumpe/crboost_data/stage4refactor_demo):
mute chain VERIFIED END-TO-END GREEN — after the adapter identity-check fix (commit 3056ba3:
per-expected-TS presence, extras warn) the user redeployed; final state External/job002 fsMotion
5×.ok, job003 tsImport ok, job006 tsAlignment 4×.ok+1×.skip (manifest keeps all 5, muted TS
skip-marked), job007 tsCtf 4×.ok + supervisor defocus-hand ran once (manifest drops muted TS —
enumeration asymmetry feeds ASK #38/#39), job008 tsReconstruct 4×.ok. Stage-1 DONE record written
into 04-driver-consolidation.md. All owed commits landed (71a66ff echo, 3056ba3 adapter fix).

Stage-2 prep DONE 2026-08-13: **docs/roadmaps/04-stage2-transcripts.md** = 19 byte-exact
[run_command] lines (verified by cmp against logs) for fsMotion/tsImport/tsAlign/tsCtf/tsReconstruct;
9 job types still uncovered (denoise_*, TM, extract*, subtomo*, reconstruct_particle, class3d,
miss_align). **docs/roadmaps/04-stage2-toolcommand-design.md** = full design AWAITING MAINTAINER
REVIEW, decisions D1-D5 (§7): D1 builder shape (tagged-verb ToolCommand w/ mandatory quote= on
opt_path vs plain helpers), D2 compound shapes stay literal f-strings (rec), D3 crboost under
raise-on-unknown (verified DEAD at wrap layer today — services/jobs/tilt_filter.py:55, only drivers
call get_tool_name, tilt_filter driver never wraps), D4 parity gate for uncovered types (rec:
Tier-A --print-cmd snapshots per [[project_protocols_regression_harness]]), D5 run_tool accepts
str|ToolCommand (rec: both). Design facts verified against code: wrapper sorts binds internally
(caller dedup idiom byte-neutral), run_command_with_retries doesn't forward idle_timeout,
19 driver wrap sites (not 21). Roadmap-05 stage-0 items 2+4 also verified + recorded in
05-per-ts-top-up.md (aggregation = rebuild-from-all-settled everywhere; is_excluded round-trips;
found another 1.35 apix silent default at adapters/ts_alignment.py:177).

**Why:** prime directive = behavior-preserving by construction, one driver per commit,
runtime-verified before the next; ALIGN items land as separate flagged commits after the pure move.
**How to apply:** stage-2 design APPROVED by maintainer 2026-08-13 ("designs look good") — D1-D5 as
recommended (tagged-verb ToolCommand, literal compound shapes, crboost deferred to flagged commit,
Tier-A --print-cmd as parity gate for uncovered types, run_tool accepts str|ToolCommand). Maintainer
priority: FINISH THE REFACTOR FAST, cut non-essentials — then de-novo picking
([[project_denovo_picking]]) + regression harness ([[project_protocols_regression_harness]]; Tier-A
--print-cmd built during stage 2 seeds it). Next = stage-2 migration commits in transcript order
(ts_reconstruct → ts_ctf → ts_alignment → fs_motion → ts_import) → stage 3 ArrayDriver in census
risk order (ts_reconstruct pilot, DriverContext folded per driver). Stages 4-5 + quoting-unification
+ roadmap-05 decision list = deferrable, don't gold-plate.

**STAGE 2 + PARTIAL STAGE 3 LANDED + COMMITTED 2026-08-13.**
Stage 2 DONE: `ToolCommand` + `run_tool()` + `CRBOOST_PRINT_CMD` snapshot gate in driver_base
(gate lives in `run_command`, NOT run_tool, so pre/post-migration snapshots are diffable). All 14
builders migrated. **BYTE-PARITY VERIFIED on a live post-migration chain run** at
/groups/klumpe/crboost_data/deadcode_stage5_pre — fs_motion(job002), ts_import(job003),
ts_alignment(job004), ts_ctf task + defocus_hand supervisor(job005), ts_reconstruct(job006) all
diff clean vs 04-stage2-transcripts.md after normalizing only project name / job number (that
project has no TiltFilter so alignment is job004 not job006) / TS name. 12 of 17 tool_name literals
converted; 5 kept deliberately with in-code reasons: extract_pick_list (census #73) + the 4 calls
inside the IsoNet-only helpers (branch, not params, makes them IsoNet; get_tool_name() is
method-conditional and would answer "cryocare" off-branch).
Stage 3 PARTIAL: `ArrayDriver(ABC)` in array_job_base (owns dispatch, both bootstraps, manifest
lookup, exclusions, tally, EXIT markers, fail handler, `self.log()` prefixing) + frozen generic
`DriverContext` in driver_base (subclasses never call get_driver_context at all). ts_reconstruct
279→131 lines (pilot), ts_ctf 396→275 — ts_ctf needed NO new hooks, which is the evidence the hook
set is right. ts_ctf ALIGNs #5/#7/#9 landed. **6 drivers still on the old shape** (fs_motion,
template_match, extract_candidates, denoise_predict, ts_alignment, subtomo_extraction) —
RECOMMENDED CUT: migrate opportunistically when touched; two idioms coexist meanwhile.
OPEN: ts_ctf ledger #8 (task mode hardcodes job_dir/warp_tiltseries{,.settings}+tomostar instead of
the resolver; supervisor has a silent paths.get default) — needs resolver output verified first,
supervisor/task disagreement fails staging outright. One flagged behavior delta: preflight_registry
now runs BEFORE pre_dispatch (was after the defocus-hand compute). CRBOOST_PRINT_CMD never
exercised at runtime yet.

**STAGES 4 + 5 DONE 2026-08-13 + ledger #8 CLOSED (all PENDING RUNTIME).**
- **Stage 4** `BaseIngestAdapter` (`services/tilt_series/adapters/_base.py`): ctor head +
  `_read_only_block` (3 copies) + `_resolve_per_ts_path` (2) + `_excluded_set`/`_drop_excluded` (3).
  Subclasses set `DEFAULT_WARP_FOLDER`; fs_motion_ctf + ts_ctf lost their `__init__` entirely.
  NOT an ABC on purpose (ingest/emit_star signatures genuinely differ per job type).
  **OPEN ASK**: denoise_predict + tilt_filter registry stamps do NOT fit — both are best-effort by
  explicit design (broad catch, warn only, "must never fail a job whose outputs exist") while every
  adapter here is fail-loud; tilt_filter also has no adapter shape (set_frame_filtered per frame, no
  typed output, no emit_star). Decision needed: adopt fail-loud, or stay outside the family?
- **Stage 5** subtomo_merge → `services/subtomo_merge.py` (this ALSO killed a real `ui/`→`drivers/`
  import in aggregation_merge_card that check_boundaries R1 doesn't catch — R1 only bans the reverse
  direction; consider adding the rule). list_extraction's 2 mirrored fns deleted. Driver-invocation
  string → `services/jobs/spec.py` as `driver_launch_prefix()` + `driver_invocation()`; the census
  named 2 call sites but there is a **THIRD** (backend.extract_pick_list), which is why the prefix is
  split out. **`extract_pick_list` → get_driver_context is NOT APPLICABLE** (no instance_id, not in
  project_state.jobs, no param class) — genuinely blocked on census #68, roadmap item was wrong.
- **Ledger #8 closed.** Precondition verified: TsCtfParams OUTPUT_SCHEMA `output_processing`
  path_template "warp_tiltseries/" renders byte-identically to the hardcoded default, so the silent
  `.get(..., default)` was dead → now `ctx.paths["output_processing"]` in supervisor + stage + collect.
  The other two literals were mis-stated in the ledger: `job_dir/warp_tiltseries.settings` and
  `job_dir/tomostar` are **supervisor-made job-local copies**, not resolver outputs — named once as
  LOCAL_SETTINGS_NAME/LOCAL_TOMOSTAR_NAME. That closes the real drift: the supervisor named its copy
  after the UPSTREAM file (`settings_file.name`) while the task looked for a literal.

**STAGE 3 COMPLETE (code) 2026-08-14 — all 8 array drivers migrated, PENDING RUNTIME + COMMIT.**
Session decisions (maintainer, via AskUserQuestion): ① registry-stamp failure = FAIL LOUD for
denoise_predict AND tilt_filter (landed: raises on empty registry, KeyError swallows removed,
denoise now also gets base preflight_registry — supersedes #30's tolerated omission); ② #68 =
NOT implemented blind — full design + shadow-zone inventory written to
docs/roadmaps/07-extract-pick-list-job-identity.md, lands as FIRST slice of denovo/Journey UI pass
(#73 stays gated); ③ ts_alignment migration includes the #38/#39 pilot.

The 6 migrations (fs_motion, template_match, extract_candidates, denoise_predict, ts_alignment,
subtomo_extraction) + decided ALIGNs (#12-#17, #21, #24, #25-interim, #28, #33, #36, #42-#44,
#46, #48; #44 closes the stale .ok+.skip double-marking defect) + tilt_filter #49/#51. Base grew
6 hooks (post_bootstrap, whole_job_short_circuit, preflight_scope, per_task_slurm_config,
task_binds, execute) + poll_secs attr + tally_acceptable policy hook (ts_alignment tolerant tally
#41 is the only override) + stage_per_ts_environment(input_processing=None) for alignment.
run_task now routes through self.execute (default = old build_command→run_tool path, log-identical).
ts_alignment pilot: registry enumerates (glob dead), snapshot refreshed EVERY supervisor run
(guards dropped — was reused stale), dispatch drift check registry-vs-tomostar-vs-star → live TS
missing from a source recorded in manifest "drift_ts" + its task fails with reason (fs_motion #12
uses the same manifest pattern via "unresolved_ts"); on-disk extras unknown to registry warned,
never dispatched. NOTE: ts_alignment's pure-migration-then-pilot 2-commit discipline was collapsed
into one commit (single final file, no git in sandbox).

Verification ceiling this session: ruff check+format only (venv python symlinks → /software,
UNMOUNTED; no system python at all). USER OWES: py_compile all drivers, python check_boundaries.py,
and per-driver runtime per the roadmap checklist (esp. ts_alignment drift check + tolerant tally,
denoise fail-loud stamp, subtomo merge_only/zero-picks short-circuits, template/extract full chain).
4 pre-existing unformatted files (class3d, denoise_train, miss_align, reconstruct_particle) left
alone on purpose. Roadmap docs updated: 04 stage records, census "ASK decisions round 2", README
row 04 + new row 07.

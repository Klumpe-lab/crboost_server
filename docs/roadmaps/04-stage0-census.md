# Roadmap 04 — Stage 0 census

Census run 2026-08-12/13 on branch `isonet_auto_and_missalignment_fix` at `05d9395`, by a 12-agent
fan-out (6 driver deep-reads, command-builder census, status-protocol census, adapter census,
transcript harvest, denoise fact-check, base-lifecycle reference). **Caveat:** the adversarial
verify pass and completeness critic did not run (session limits) — every ledger entry is a
single-agent read. That is acceptable because the roadmap's prime directive already requires each
driver's migration commit to be a diff-read of exactly these behaviors: **each entry gets
re-verified at the moment its driver is migrated**, and a wrong entry blocks nothing before then.

Decision key: **KEEP** = deliberate/documented, encode as a template hook or leave; **ALIGN** =
safe to unify, lands as its own flagged commit after the pure migration of that driver; **ASK** =
needs a maintainer decision (could change which data gets processed or how failures surface).

Totals: 75 divergence entries — 32 KEEP, 30 ALIGN, 13 ASK.

## ASK index — maintainer decisions needed before stages 2–3

- **[#7] ts_ctf** — 6-staging (tolerant missing tomostar -> silent no-op success risk) (`drivers/ts_ctf.py:116-119 (task staging); 240-243 (supervisor copytree)`)
- **[#10] ts_ctf** — 6-staging / re-run idempotency (supervisor XML re-copy clobbers CTF-updated XMLs) (`drivers/ts_ctf.py:223-225 (interacts with copy-back 362-365 and .ok-skip dispatch in array_job_base.py:607-624)`)
- **[#12] fs_motion_and_ctf** — 5 item/TS enumeration source (`drivers/fs_motion_and_ctf.py:56-101 (drop at 87-92), ts_names built at 306`)
- **[#13] fs_motion_and_ctf** — 6 staging behavior (`drivers/fs_motion_and_ctf.py:195-219 (warn at 217)`)
- **[#19] template_match_pytom** — 4 exclusion application (propagation into aggregation) (`drivers/template_match_pytom.py:391 (return discarded), 422-424 (verbatim copy)`)
- **[#25] extract_candidates_pytom** — 5 item/TS enumeration source (`drivers/extract_candidates_pytom.py:221-229`)
- **[#28] extract_candidates_pytom** — 11 error handling style (`drivers/extract_candidates_pytom.py:75-101`)
- **[#30] denoise_predict** — 10 registry preflight (`drivers/denoise_predict.py:38-48, 395-408`)
- **[#38] ts_alignment** — 5 item/TS enumeration source (`drivers/ts_alignment.py:53-56 (glob at 55), invoked at 234`)
- **[#39] ts_alignment** — 6 staging behavior (input snapshotting) (`drivers/ts_alignment.py:225-232 (guards at 228, 231)`)
- **[#53] denoise_train** — 5 item/TS enumeration (block + column resolution) (`drivers/denoise_train.py:264-279`)
- **[#56] miss_align** — 5 item/TS enumeration source (`drivers/miss_align.py:217-241, 46-63`)
- **[#68] extract_pick_list** — 1+2 bootstrap bypass / argparse dispatch (`drivers/extract_pick_list.py:10-11, 31, 108-122`)

## Divergence ledger (by driver, in stage-3 migration risk order)

### ts_reconstruct (4)

#### [#0] 5-enumeration (helper duplication) — **ALIGN**

`drivers/ts_reconstruct.py:57-63 (import gap at 30-41; StarfileService import at 44)`

- **Behavior:** Defines a private read_tilt_series_names_from_input_star() duplicating the base helper logic-for-logic (StarfileService().read, 'global' block, sorted rlnTomoName). Does not import the base version; imports StarfileService solely for this.
- **Canonical:** array_job_base.py:225-239 exports read_tilt_series_names_from_input_star with the authoritative-source docstring; ts_ctf imports and uses it (ts_ctf.py:33, 212).
- **Evidence:**
```
def read_tilt_series_names_from_input_star(input_star: Path) -> list[str]:
```
- **Rationale:** Bodies are semantically identical (same read, same 'global' lookup, same sorted list). Replace the local def with the base import and drop the now-unused StarfileService import; zero behavior change.

#### [#1] 8-status-writes (task-level idempotency short-circuit) — **KEEP**

`drivers/ts_reconstruct.py:232-238 (helper at 66-69)`

- **Behavior:** Task mode pre-checks the output MRC (exists and size>0) and writes .ok + exit 0 WITHOUT running WarpTools.
- **Canonical:** Base handles re-run skipping only at supervisor level: submit_array_job omits items with .ok/.skip from the array spec (array_job_base.py:607-624). No base verb for in-task idempotency.
- **Evidence:**
```
if out_mrc.exists() and out_mrc.stat().st_size > 0:
```
- **Rationale:** Documented deliberate in the module docstring (lines 14-15: 'idempotently skips if the reconstruction MRC already exists'). Covers the window where the artifact exists but no .ok was recorded (orphan/superseded runs). Consolidation could generalize it as an optional task_already_done() hook. Note: the skip path bypasses write_status_atomic's superseded-task guard only in the sense that it still goes through write_status_atomic, so it is safe.

#### [#2] 9-output-verification (extra fail-loud check vs base) — **KEEP**

`drivers/ts_reconstruct.py:259-262`

- **Behavior:** After the tool exits 0, raises FileNotFoundError if the expected reconstruction MRC is absent, so the TS gets .fail despite a zero exit code.
- **Canonical:** Base pattern trusts the tool exit code; ts_ctf writes .ok with no artifact verification at all (ts_ctf.py:362-367).
- **Evidence:**
```
raise FileNotFoundError(f"WarpTools reported success but expected output MRC missing: {out_mrc}")
```
- **Rationale:** Fail-loud matches project policy ('never fail silently'). Candidate to promote into the consolidated base as an expected-outputs hook rather than remove.

#### [#3] 7-note hardcoded tool-name string (dim 7 owned elsewhere) — **KEEP**

`drivers/ts_reconstruct.py:76 (container wrap keyed by get_tool_name() at 253-254)`

- **Behavior:** Command literal hardcodes 'WarpTools ts_reconstruct' while container selection uses params.get_tool_name().
- **Canonical:** Same pattern in ts_ctf (and presumably all warp drivers); noted per census instruction for the dimension-7 agent.
- **Evidence:**
```
f"WarpTools ts_reconstruct "
```
- **Rationale:** Matches the majority pattern; only recorded so the command-construction agent has the file:line inventory.

### ts_ctf (8)

#### [#4] 1-mode-shape (supervisor runs a global tool step before dispatch) — **KEEP**

`drivers/ts_ctf.py:54-90 (invoked at 248-251); pre-dispatch XML/settings/tomostar copy at 217-243; preflight_registry after it at 253`

- **Behavior:** Supervisor executes real compute before the array: copies in-scope alignment XMLs + settings + tomostar into the job dir, then runs WarpTools ts_defocus_hand globally (auto mode composes a shell if/grep on the --check output; forced modes chain check && set_flip/set_noflip). preflight_registry runs only AFTER this compute.
- **Canonical:** Canonical supervisor does no tool execution: enumerate -> preflight_registry -> apply_exclusions -> submit_array_job -> wait -> collect -> aggregate (array_job_base.py:7-12 docstring; ts_reconstruct.py:109-195). Tool runs live in task mode.
- **Evidence:**
```
Run ts_defocus_hand on ALL tilt-series at once.
```
- **Rationale:** Documented deliberate (module docstring lines 8-10: handedness decision needs statistics across all TS). Consolidation should model it as a pre-dispatch global-step hook. Minor ALIGN-able sub-point: preflight_registry (line 253) could move before run_defocus_hand_globally (line 250) so a bad registry fails before burning GPU time.

#### [#5] 12-log-format / 11-retry-style (global step) — **ALIGN**

`drivers/ts_ctf.py:86-90`

- **Behavior:** The defocus-hand wrapped command is executed via run_command (no retries) and is never echoed with a '[SUPERVISOR] Command:' line before running.
- **Canonical:** Tool invocations echo 'Command:' then use run_command_with_retries with a label (ts_ctf.py:355+360 task mode; ts_reconstruct.py:251+257).
- **Evidence:**
```
run_command(wrapped, cwd=job_dir)
```
- **Rationale:** Adding the echo is pure logging. Retries are also safe: --check plus set_flip/set_noflip are idempotent, and every other tool invocation in these drivers gets run_command_with_retries.

#### [#6] 6-staging (hand-rolled stage_ctf_environment; XML copy vs symlink; copy-back) — **KEEP**

`drivers/ts_ctf.py:93-134 (comment 121-123, copy2 at 132); copy-back at 362-365; cwd=stage_root at 357-360`

- **Behavior:** Implements its own stage_ctf_environment: stages FROM the job-local output_processing (post-defocus-hand XMLs, not upstream input_processing), COPIES the per-TS XML instead of symlinking, returns a single stage_root (not the base's (settings, processing) tuple), runs the tool with cwd=stage_root, then copies the mutated XML back to the shared dir.
- **Canonical:** stage_per_ts_environment (array_job_base.py:411-465) symlinks the XML ('dst_xml.symlink_to(src_xml.resolve())', line 463), stages from input_processing, returns (staged_settings, staged_processing); ts_reconstruct consumes it directly with no copy-back (ts_reconstruct.py:240-242).
- **Evidence:**
```
# A symlink would cause WarpTools to write through to the shared file,
```
- **Rationale:** The copy is documented deliberate (121-123): ts_ctf MUTATES the XML, defocus_hand already updated the shared copy, and a symlink would write through and make the final copy-back a SameFileError. Consolidation path: grow the base helper a link_mode='symlink'|'copy' (+ optional copy-back) parameter instead of keeping a parallel implementation.

#### [#7] 6-staging (tolerant missing tomostar -> silent no-op success risk) — **ASK**

`drivers/ts_ctf.py:116-119 (task staging); 240-243 (supervisor copytree)`

- **Behavior:** Task staging silently proceeds when {ts}.tomostar is absent; supervisor's tomostar copytree is likewise conditional and also never refreshes a stale local copy on re-run ('if not local_tomostar.exists()'). Since WarpTools enumerates items from the settings DataFolder (= tomostar), a missing tomostar means the tool sees zero items, the staged XML stays unchanged, copy-back succeeds, and .ok is written — a silent false success.
- **Canonical:** stage_per_ts_environment raises FileNotFoundError on a missing tomostar (array_job_base.py:446-447: raise FileNotFoundError(f"Tomostar not found: {src_tomostar}")).
- **Evidence:**
```
if src_tomostar.exists():
```
- **Rationale:** Aligning to the base raise is the fail-loud direction the project mandates, but it converts currently-green runs with absent tomostars into hard failures — maintainer must confirm no legitimate missing-tomostar case exists (e.g. TS present in alignment STAR but tomostar pruned by tilt filter).

#### [#8] 6-staging / path resolution (task mode hardcodes locations instead of resolved paths) — **ALIGN**

`drivers/ts_ctf.py:345-347 (supervisor fallback at 199; relative --settings in command at 141; supervisor derives name at 235)`

- **Behavior:** Task mode hardcodes output_processing=job_dir/'warp_tiltseries', settings=job_dir/'warp_tiltseries.settings', tomostar=job_dir/'tomostar' rather than reading local_params_data['paths']. Supervisor uses paths.get('output_processing', job_dir/'warp_tiltseries') as a silent fallback and copies the settings under its RESOLVED name (settings_file.name, line 235) — a resolver value differing from the hardcoded convention desyncs supervisor vs task (staging would raise FileNotFoundError on the settings copy).
- **Canonical:** ts_reconstruct task mode reads every location from local_params_data['paths'] (ts_reconstruct.py:229, 240-241, 249); base verbs take paths as explicit arguments.
- **Evidence:**
```
local_settings = job_dir / "warp_tiltseries.settings"
```
- **Rationale:** Pass resolved paths into task mode (or via manifest extras). Behavior-preserving as long as the resolver currently returns exactly these conventional locations — verify resolver output for tsCtf first and flag in the commit. Also removes the silent .get() default at 199, per the no-invented-defaults policy.

#### [#9] 9-output-verification (.ok written with no artifact check; guarded copy-back) — **ALIGN**

`drivers/ts_ctf.py:362-367`

- **Behavior:** After the tool run, the XML copy-back happens only 'if staged_xml.exists()' and .ok is then written unconditionally — a vanished staged XML silently skips the copy-back yet still marks the TS ok; nothing verifies ts_ctf actually updated CTF fields.
- **Canonical:** ts_reconstruct raises when the expected output artifact is missing before writing .ok (ts_reconstruct.py:259-262).
- **Evidence:**
```
if staged_xml.exists():
```
- **Rationale:** The staged XML was copied in at staging time (raise at 127-128 guarantees the source existed), so absence after the run means tool damage — inverting the guard to raise is safe and fail-loud. Deeper 'did CTF fields actually change' verification would need domain input (ASK-grade), but the missing-file raise is a flagged-commit unification.

#### [#10] 6-staging / re-run idempotency (supervisor XML re-copy clobbers CTF-updated XMLs) — **ASK**

`drivers/ts_ctf.py:223-225 (interacts with copy-back 362-365 and .ok-skip dispatch in array_job_base.py:607-624)`

- **Behavior:** Every supervisor run unconditionally copies in-scope ALIGNMENT XMLs over output_processing. On a re-run after partial failure, this overwrites the CTF-updated XMLs that already-.ok tasks copied back; those TS are then NOT re-dispatched (submit_array_job skips .ok items), so at aggregation time their XMLs contain alignment+defocus-hand data but the task-written CTF results were just clobbered.
- **Canonical:** ts_reconstruct's supervisor mutates no prior outputs before dispatch; re-runs preserve previously produced artifacts (its idempotency check even re-affirms them, ts_reconstruct.py:234-238).
- **Evidence:**
```
shutil.copy2(str(xml_file), str(output_processing / xml_file.name))
```
- **Rationale:** Reachable-looking correctness hazard, not just style: the .ok-preserving re-run path exists precisely to be used. Needs maintainer confirmation that TsCtfIngestAdapter.ingest reads CTF values from these job-dir XMLs, and a fix choice (skip the copy for TS holding .ok, or clear .ok when overwriting) — either choice changes which data is processed on re-runs.

#### [#11] 7-note hardcoded tool-name strings (dim 7 owned elsewhere) — **KEEP**

`drivers/ts_ctf.py:64, 66, 68, 140 (container wrap keyed by get_tool_name() at 87-88 and 357-358)`

- **Behavior:** Four 'WarpTools ...' literals: ts_defocus_hand check/set_flip/set_noflip commands (64, 66, 68) and the ts_ctf task command (140).
- **Canonical:** Same pattern as ts_reconstruct; noted per census instruction for the dimension-7 agent.
- **Evidence:**
```
f"WarpTools ts_ctf "
```
- **Rationale:** Matches the majority pattern; recorded only as file:line inventory for the command-construction agent.

### fs_motion_and_ctf (6)

#### [#12] 5 item/TS enumeration source — **ASK**

`drivers/fs_motion_and_ctf.py:56-101 (drop at 87-92), ts_names built at 306`

- **Behavior:** Hand-rolled read_ts_frame_mapping() enumerates from the import STAR's global block but SILENTLY DROPS any TS whose per-TS star file cannot be resolved (tries project_root then input_star_dir, then prints a [WARN] and `continue`s). The dropped TS never enters ts_names, gets no .skip marker, and the job can green-tick without it.
- **Canonical:** Base read_tilt_series_names_from_input_star (array_job_base.py:225-239) returns EVERY rlnTomoName row; base staging raises FileNotFoundError on missing per-TS inputs (array_job_base.py:446-447, 457-458); project policy is never-fail-silently. (A custom parser is legitimately needed here for the frame mapping — only the drop semantics diverge.)
- **Evidence:**
```
f"[WARN] Per-TS star not found: tried {project_root / ts_star_rel} and {input_star_dir / ts_star_rel}" ... continue
```
- **Rationale:** Silently narrows which tilt-series get processed. Maintainer must choose: hard raise (matches base staging + never-fail-silently policy) vs pre-marking the TS .skip so it stays visible in the per-TS strip. Either change alters observable behavior on malformed imports.

#### [#13] 6 staging behavior — **ASK**

`drivers/fs_motion_and_ctf.py:195-219 (warn at 217)`

- **Behavior:** stage_fs_environment() symlinks each frame into .staging/task_{ts}/frames/; a missing source frame produces an indented '[WARN] Frame not found' and is skipped — the TS is then processed on a PARTIAL frame set and can still be marked .ok.
- **Canonical:** Base stage_per_ts_environment raises FileNotFoundError when a staged input is missing (array_job_base.py:446-447 'Tomostar not found', 457-458 'Per-TS XML not found'). Staging layout itself (.staging/task_{ts}, symlinks) matches the base convention.
- **Evidence:**
```
print(f"  [WARN] Frame not found: {src}", flush=True)
```
- **Rationale:** A TS with missing frames on disk gets motion/CTF-corrected from whatever subset staged, and green-ticks. Raising instead would fail those TS — changes which data is processed, so needs the maintainer's call (cryo-ET expert may prefer hard fail per never-silent-defaults policy).

#### [#14] 8 status writes / output verification before .ok — **ALIGN**

`drivers/fs_motion_and_ctf.py:234-235 (silent return), 447-453 (.ok without check)`

- **Behavior:** Task writes .ok immediately after collect_fs_outputs() with no expected-output existence check; collect_fs_outputs itself silently returns when the staged warp_frameseries dir is absent — so a WarpTools run that exited 0 but produced nothing still records .ok. (write_status_atomic mechanics themselves are canonical.)
- **Canonical:** Majority of array drivers verify the per-item product and raise before writing .ok: template_match_pytom.py:530-533, ts_reconstruct.py:260, denoise_predict.py:536, extract_candidates_pytom.py:436 all use the 'reported success but expected output missing' idiom.
- **Evidence:**
```
if not staged_warp.exists():
        return
```
- **Rationale:** Adding a check that per-frame XMLs (or the staged warp_frameseries dir) exist before write_status_atomic(ok=True) matches the majority idiom; only degenerate zero-output runs change from silent-.ok to .fail, which is the documented intent of the idiom. Flagged commit.

#### [#15] input validation (dims 5/11 adjunct) — require_producer_input not used — **ALIGN**

`drivers/fs_motion_and_ctf.py:297-299`

- **Behavior:** Supervisor validates the resolved input STAR with a bare exists() check and plain FileNotFoundError — no stale-producer diagnosis.
- **Canonical:** Majority uses driver_base.require_producer_input (driver_base.py:379-399), which appends diagnose_stale_producer's actionable hint: ts_reconstruct.py:127, ts_ctf.py:202-205, ts_alignment.py:220-223, miss_align.py:202-204, denoise_predict.py:365-366.
- **Evidence:**
```
raise FileNotFoundError(f"Required input STAR file not found: {input_star_path}")
```
- **Rationale:** Same failure condition, richer error message on the known job-number-off-by-one failure mode. Pure diagnostics; no data-flow change.

#### [#16] tool-name sourcing (hardcoded string; dim-7-adjacent, noted per instructions) — **ALIGN**

`drivers/fs_motion_and_ctf.py:442-445 (literal at 444); also hardcoded command names 'WarpTools create_settings' at 118, 'WarpTools fs_motion_and_ctf' at 137`

- **Behavior:** Container wrap passes literal tool_name="warptools" instead of params.get_tool_name().
- **Canonical:** Array-driver majority passes params.get_tool_name(): ts_alignment.py:387, ts_ctf.py:88+358, ts_reconstruct.py:254, template_match_pytom.py:526, extract_candidates_pytom.py:430, denoise_predict.py:531. FsMotionCtfParams.get_tool_name() returns "warptools" (services/jobs/fs_motion_ctf.py:97-98), so the swap is behavior-identical.
- **Evidence:**
```
tool_name="warptools"
```
- **Rationale:** Provably identical value via the params class; unifying removes one place where a container remap in conf.yaml could silently miss this driver.

#### [#17] 12 log line format — **ALIGN**

`drivers/fs_motion_and_ctf.py:440 (truncation); unprefixed [WARN] at 88-91 and 217`

- **Behavior:** Task-mode 'Command:' echo is truncated to the first 300 chars of the UNwrapped WarpTools command ('...{warp_command[:300]}...'); helper functions emit bare '[WARN]' lines without the [SUPERVISOR]/[TASK n] prefix.
- **Canonical:** Majority echoes the full command line, e.g. template_match_pytom.py:523 print(f"[TASK {array_idx}] Command: {cmd_str}"); all lifecycle logs carry [SUPERVISOR]/[TASK n] prefixes.
- **Evidence:**
```
print(f"[TASK {array_idx}] Command: {warp_command[:300]}...", flush=True)
```
- **Rationale:** Cosmetic; full-command echo also matters for the planned Tier-A --print-cmd regression snapshots (protocols harness), where a truncated echo hides parameter drift.

### template_match_pytom (7)

#### [#18] 6 staging behavior — **KEEP**

`drivers/template_match_pytom.py:479-500 (shared tmResults, symlink at 498-500), run cwd=job_dir at 528`

- **Behavior:** No per-TS staging isolation at all: every array task runs with cwd=job_dir, shares one tmResults/ output dir, and merely symlinks its input tomogram into tmResults/{name}{suffix}. No .staging/task_{name} dirs exist.
- **Canonical:** Base stage_per_ts_environment builds an isolated .staging/task_{ts}/ per task (array_job_base.py:411-465) because WarpTools enumerates items from the settings DataFolder; fs_motion_and_ctf stages per-TS frames the same way (fs_motion_and_ctf.py:195-219).
- **Evidence:**
```
local_tomo = tm_results_dir / f"{tomo_name}{tomo_path.suffix or '.mrc'}"
```
- **Rationale:** Deliberate and documented in the module docstring (lines 15-20): pytom takes explicit per-tomogram args (-v plus per-tomo text files), so the DataFolder-enumeration hazard that forced staging doesn't apply; outputs are name-keyed so tasks cannot collide. Forcing base staging here would add copies for no isolation benefit.

#### [#19] 4 exclusion application (propagation into aggregation) — **ASK**

`drivers/template_match_pytom.py:391 (return discarded), 422-424 (verbatim copy)`

- **Behavior:** apply_exclusions() is called but its returned excluded-name list is discarded, and the success-path output star is a VERBATIM shutil.copy2 of the input tomograms star — so user-excluded (.skip) tomograms remain rows in the output tomograms.star despite having no {name}_scores.mrc.
- **Canonical:** apply_exclusions docstring instructs callers to also drop excluded names from aggregation (array_job_base.py:292-295 'Returns the excluded names ... so callers can also drop them from glob/merge-based aggregation'); registry-emitting drivers do so via excluded_ids=set(results.skipped): fs_motion_and_ctf.py:375-377, ts_ctf.py:307, ts_reconstruct.py:190.
- **Evidence:**
```
shutil.copy2(input_star_tomos, output_tomograms)
```
- **Rationale:** Dropping excluded rows would change which tomograms downstream (extract_candidates) sees — currently downstream must tolerate score-less rows. Note extract_candidates_pytom.py:322 shares the identical verbatim-copy pattern, so any fix must be coordinated across the pick stage, and the maintainer may prefer the current pass-through so excluded TS stay visible downstream.

#### [#20] 3 manifest as supervisor-time path snapshot (vs drive-time resolver) — **KEEP**

`drivers/template_match_pytom.py:377-385 (write), 462-471 + 503 (task reads), 473 (dead expression)`

- **Behavior:** Task mode ignores the drive-time resolver output entirely — line 473 builds the paths dict and discards it unassigned — and instead trusts paths frozen into the manifest at supervisor time (input_tomograms_star, raw_tomo_paths, template_path, mask_path, legacy_text_input, patched_tomograms_star, angle_list_path).
- **Canonical:** driver_base re-resolves paths at drive time precisely because schedule-time snapshots drift (driver_base.py:100-106 comment); fs_motion_and_ctf's task consumes local_params_data["paths"] (fs_motion_and_ctf.py:421-423).
- **Evidence:**
```
{k: Path(v) for k, v in context["paths"].items()}
```
- **Rationale:** Freezing inputs at supervisor time guarantees all array tasks of one submission see identical, already-validated template/mask/star inputs even if project state changes mid-array — a defensible consistency property for a multi-hour array. The unassigned dict-comprehension at 473 is dead code and can be deleted in any commit (no behavior).

#### [#21] 5 enumeration mechanics (STAR reading path) — **ALIGN**

`drivers/template_match_pytom.py:66-71 (_get_df_from_star), 301-307 (enumeration)`

- **Behavior:** Enumerates from the input tomograms STAR (canonical source) but via a hand-rolled first-DataFrame-block-of-any-name heuristic over the raw `starfile` library, not via StarfileService and a named block.
- **Canonical:** Base read_tilt_series_names_from_input_star uses StarfileService().read + star_data.get("global") + sorted rlnTomoName (array_job_base.py:235-239).
- **Evidence:**
```
if isinstance(v, pd.DataFrame):
            return v
```
- **Rationale:** Behavior-identical whenever the star's data block is named 'global' — the convention this pipeline itself writes (line 161: starfile.write({"global": tomo_df}, ...)). Unify in a flagged commit after confirming block naming on a real ts_reconstruct output tomograms.star; the driver still needs the full df for rlnTomoReconstructedTomogram, so only the name-extraction/read path aligns.

#### [#22] 1/behavior toggle — module-level hardcoded input-pathway switch — **KEEP**

`drivers/template_match_pytom.py:54-56 (toggle), 313-340 (supervisor branch), 470 + 513-520 (task branch), 154-158 (rlnTomoHand:=1 override in the dead branch)`

- **Behavior:** The entire pytom input pathway (0.10-style per-tomo text files vs --relion5-tomograms-star) is selected by a module constant LEGACY_TEXT_INPUT = True, editable only in source; the non-legacy branch is currently dead and additionally contains a [TEMP-DEBUG] override forcing rlnTomoHand to 1 (unprefixed print, no flush).
- **Canonical:** No other array driver gates behavior on an edit-the-source module constant; behavior switches come from the params model (USER_PARAMS). The flag is also duplicated into the manifest (legacy_text_input, line 380/470) so tasks follow the supervisor's choice.
- **Evidence:**
```
LEGACY_TEXT_INPUT = True
```
- **Rationale:** Behavior-preserving consolidation must NOT flip or delete either branch: the toggle is explicitly commented 'TEMPORARY ... to replicate GT pipeline behavior for score comparison' (lines 54-55) and ties into the open 412 handedness/score investigation. Promotion to a param or deletion of the dead branch is a separate maintainer decision to schedule after that investigation closes.

#### [#23] 8 status writes — in-task idempotent .ok short-circuit — **KEEP**

`drivers/template_match_pytom.py:482-486`

- **Behavior:** Task pre-checks tmResults/{name}_scores.mrc; if present and non-empty it writes .ok and exits 0 without running pytom.
- **Canonical:** Base has no in-task idempotency verb; sparse re-dispatch normally relies solely on .ok files (get_previously_done, array_job_base.py:174-183). fs_motion_and_ctf always re-runs its tool on re-dispatch.
- **Evidence:**
```
if out_scores.exists() and out_scores.stat().st_size > 0:
```
- **Rationale:** Documented in the module docstring (lines 15-17 'idempotently skips if tmResults/{name}_scores.mrc already exists'); covers the crash-after-output-before-status window for the most expensive per-item tool in the pipeline. Could later be promoted into the base as an optional verb.

#### [#24] input validation — require_producer_input not used — **ALIGN**

`drivers/template_match_pytom.py:289-296`

- **Behavior:** Four bare FileNotFoundError checks (tomograms star, tiltseries star, template, mask) with no stale-producer diagnosis.
- **Canonical:** Majority uses driver_base.require_producer_input (driver_base.py:379-399) for upstream-resolved inputs: ts_reconstruct.py:127, ts_ctf.py:202-205, ts_alignment.py:220-223, miss_align.py:202-204, denoise_predict.py:365-366.
- **Evidence:**
```
raise FileNotFoundError(f"Input tomograms STAR missing: {input_star_tomos}")
```
- **Rationale:** Same failure condition, adds the job-number-off-by-one diagnostic on the two star inputs (template/mask are workbench-side paths where the diagnosis is inapplicable but harmless). Pure diagnostics.

### extract_candidates_pytom (5)

#### [#25] 5 item/TS enumeration source — **ASK**

`drivers/extract_candidates_pytom.py:221-229`

- **Behavior:** Supervisor enumerates tomograms by globbing the STAGED tmResults dir for *_job.json files (one per tomogram that TM actually processed), not from the input tomograms.star. input_tomograms is validated to exist (:199-200) but only used for apix and passthrough copy.
- **Canonical:** read_tilt_series_names_from_input_star (array_job_base.py:225-239): "The input STAR is the data contract between jobs — use it"; majority per-TS drivers enumerate from the input star, and the base docstring explicitly warns that glob enumeration can pull excluded items back into scope.
- **Evidence:**
```
job_jsons = sorted(local_tm_results.glob("*_job.json"))
```
- **Rationale:** Switching to star enumeration could change which data is processed: job.jsons exist only for tomograms TM ran, so on TM-partial runs the glob set and the star set differ. The glob-hazard is partially mitigated because apply_exclusions runs afterward (:259) and its return value filters the merge (:295-297), but only the maintainer can say whether the TM-output set or the star is the intended authority for extraction.

#### [#26] 6 staging behavior — **KEEP**

`drivers/extract_candidates_pytom.py:125-142, 215-219`

- **Behavior:** One-time SUPERVISOR-level staging of the entire upstream tmResults dir into a single SHARED job-local dir: JSONs are copied with output_dir patched to the local dir, everything else (score/angle MRCs) is symlinked. Tasks all read from and write *_particles.star into this shared dir — no per-task .staging/task_{ts} isolation. `if target.exists(): continue` (:131-132) means a re-run never re-patches a changed upstream file.
- **Canonical:** stage_per_ts_environment (array_job_base.py:411-465) builds an isolated .staging/task_{ts_name}/ per task (settings copy + tomostar copy with absolute paths + XML symlink).
- **Evidence:**
```
data["output_dir"] = str(local)
```
- **Rationale:** Deliberate and documented in the module docstring ("Stages the upstream tmResults dir ... ONCE"). The canonical staging is WarpTools-shaped (settings/tomostar); pytom's job.json embeds output_dir, and tasks write distinct {tomo}_particles.star filenames so the shared dir is collision-free. The exists-skip staleness on re-run is worth a comment during consolidation.

#### [#27] 9 results tally strictness — **KEEP**

`drivers/extract_candidates_pytom.py:290-299`

- **Behavior:** After the canonical all_succeeded gate passes, the supervisor ADDITIONALLY requires at least one per-tomogram *_particles.star to survive exclusion filtering, else raises -> RELION_JOB_EXIT_FAILURE. A run where every tomogram is .skip (e.g. all user-excluded) fails despite collect_task_results reporting all_succeeded=True.
- **Canonical:** collect_task_results (array_job_base.py:242-257) counts .skip toward all_succeeded; denoise_predict honors this by succeeding with an empty output STAR (denoise_predict.py:428-432).
- **Evidence:**
```
raise RuntimeError("No *_particles.star files produced by tasks")
```
- **Rationale:** An empty candidates.star would be a poisoned contract for subtomo extraction/refinement downstream; failing loud matches the project's never-fail-silently policy. Document the intentional strictness when consolidating so it is not "unified away" into denoise's tolerant behavior.

#### [#28] 11 error handling style — **ASK**

`drivers/extract_candidates_pytom.py:75-101`

- **Behavior:** cleanup_tomo_names catches ANY exception, prints a [WARN], and returns 0 — the supervisor then proceeds to RELION_JOB_EXIT_SUCCESS with the _X.XXApx suffix still embedded in rlnTomoName in the merged candidates.star.
- **Canonical:** Supervisor steps that shape the output contract raise inside the guarded main try -> FAILURE marker + sys.exit(1) (same file :378-382); CLAUDE.md forbids silent tolerant fallbacks for load-bearing values.
- **Evidence:**
```
print(f"[WARN] Could not clean tomo names: {e}")
```
- **Rationale:** Suffixed tomo names silently break downstream rlnTomoName joins (subtomo/refine would see zero matches), which is exactly the wrong-but-plausible failure class CLAUDE.md targets. But hardening it could fail otherwise-complete jobs on a transient starfile read hiccup — maintainer call on fail-hard vs warn.

#### [#29] 7 (noted only) hardcoded tool-name strings — **KEEP**

`drivers/extract_candidates_pytom.py:104-111`

- **Behavior:** Executable name "pytom_extract_candidates.py" hardcoded in build_extract_base_cmd (:106). Container tool selection itself is NOT hardcoded — task mode uses params.get_tool_name() (:430).
- **Canonical:** Container wrap tool via params.get_tool_name(); executable names are per-driver constants across all drivers.
- **Evidence:**
```
"pytom_extract_candidates.py",
```
- **Rationale:** Note for the dimension-7 owner only; this is the normal per-driver executable constant, not a container-tool-selection hardcode.

### denoise_predict (8)

#### [#30] 10 registry preflight — **ASK**

`drivers/denoise_predict.py:38-48, 395-408`

- **Behavior:** preflight_registry is neither imported nor called anywhere in the file; the supervisor goes straight from apply_exclusions (:398) to submit_array_job (:399) with no pre-dispatch registry coverage validation.
- **Canonical:** preflight_registry(project_path, names, job_name=...) before submit_array_job (array_job_base.py:320-361); extract_candidates_pytom does this at :231.
- **Evidence:**
```
install_cancel_handler,
    read_manifest,
```
- **Rationale:** Adding preflight would turn currently-tolerated registry gaps into hard pre-dispatch failures for denoise, but denoise's own registry write is explicitly best-effort and skips an empty registry (:301-303, "Registry empty — skipped denoise output stamp") — the omission may be intentional so registry-less projects can still denoise. Aligning changes failure semantics; maintainer decides.

#### [#31] 10 registry stamping — **KEEP**

`drivers/denoise_predict.py:282-326, 433-442`

- **Behavior:** Hand-rolled best-effort registry stamping in the supervisor: get_registry_for + registry.attach_tomogram_output(DenoisePredictTomogramOutput(...)) + registry.save(), with unknown TS swallowed via bare `except KeyError: pass` (:321-322) and a broad outer except that only warns (:325-326) — a stamp failure never fails the job.
- **Canonical:** No base verb exists for driver-side registry mutation; the base offers only preflight_registry (validation). Other array drivers do not write the registry from the supervisor; per the roadmap, registry writes go through the services.tilt_series ingest-adapter path.
- **Evidence:**
```
WARNING: denoise registry stamp skipped ({e})
```
- **Rationale:** Deliberate and documented: stamping runs only in the single-threaded supervisor to avoid parallel-task races on the shared registry JSON, and "must never fail a job that already produced denoised tomograms" (docstring :292-296). Minor policy cleanup when touched: the `except KeyError: pass` lacks its inline one-line justification (it lives in the docstring).

#### [#32] 4 exclusion application — **KEEP**

`drivers/denoise_predict.py:375-385`

- **Behavior:** A second, driver-specific pre-skip source in addition to apply_exclusions: the denoising_tomo_name substring filter pre-writes .skip for every non-matching tomogram before dispatch, so filtered tomograms never spawn a GPU task and count as settled. apply_exclusions itself is still called canonically at :398 (return value unused — safe, since aggregation keys on results.ok, not globs).
- **Canonical:** Majority pattern: apply_exclusions (array_job_base.py:281-312) is the only pre-skip source; write_skip_status is documented for "upstream produced nothing for this item" cases (array_job_base.py:149-158).
- **Evidence:**
```
write_skip_status(status_dir, ts, reason=f"filtered out by denoising_tomo_name={flt!r}")
```
- **Rationale:** User-facing job parameter implemented with the sanctioned skip verb; collect_task_results treats the filtered items correctly as settled-not-failed. This is a feature, not drift.

#### [#33] 5 item/TS enumeration source — **ALIGN**

`drivers/denoise_predict.py:67-89, 368`

- **Behavior:** Enumeration source matches canonical (the reconstruct input star, sorted), but via a local parser: _global_block PREFERS the 'global' block and silently FALLS BACK to the first block when 'global' is absent, and read_tomo_map re-implements name reading because it also needs the {ts: tomogram-basename} map. Base helper would return [] (-> hard 'No tomograms found' error) on a star without a 'global' block.
- **Canonical:** read_tilt_series_names_from_input_star (array_job_base.py:225-239) reads strictly star_data.get("global") via StarfileService and returns sorted names only.
- **Evidence:**
```
name = next(iter(data))
```
- **Rationale:** Fold into a base helper that returns the global-block DataFrame (denoise needs basenames too). The behavioral delta is only the tolerant first-block fallback on malformed/renamed-block stars, where strictness is preferable per project policy; safe to unify in a flagged commit.

#### [#34] 6 staging behavior — **KEEP**

`drivers/denoise_predict.py:148-167, 193-203, 390-393, 492-533`

- **Behavior:** Method-split staging: the cryoCARE path stages NOTHING (reads upstream even/odd halves in place from reconstruct_base/reconstruction/{even,odd}/, writes per-task predict_{idx}.json into the shared job_dir); the IsoNet path hand-rolls per-task .staging/task_{ts_name} containing three one-file symlink dirs (full/even/odd) plus a per-task corrected/ output dir, and the supervisor untars the model ONCE into job_dir/.isonet_model (:390-393) sharing the .pt via the manifest.
- **Canonical:** stage_per_ts_environment (array_job_base.py:411-465): per-task settings copy + tomostar copy with absolute paths + XML symlink — WarpTools-shaped isolation.
- **Evidence:**
```
stage = job_dir / ".staging" / f"task_{ts_name}"
```
- **Rationale:** The canonical staging exists to constrain WarpTools' DataFolder enumeration; cryoCARE/IsoNet take explicit file paths so there is no enumeration hazard. The IsoNet path deliberately reuses the .staging/task_{ts} naming convention and its per-task output dir works around IsoNet's output-filename convention (documented ISONET-ASSUMPTION, :183-191).

#### [#35] 2 bootstrap behavior — **KEEP**

`drivers/denoise_predict.py:329-345, 359, 470`

- **Behavior:** After the canonical bootstrap, BOTH modes mutate the loaded params in place via _apply_inherited_method: denoise_method and isonet_deconv are overridden from the denoise-train job that produced the model (best-effort, WARN on failure). In supervisor mode the call sits OUTSIDE the guarded main try (between the bootstrap except and the `try:` at :361), relying on the helper's internal broad catch to never raise.
- **Canonical:** Bootstrap = get_driver_context only (driver_base.py:45-137); params come from project_params.json unmodified and drivers run what the job model says.
- **Evidence:**
```
params.denoise_method = method
```
- **Rationale:** Documented rationale: "so predict always runs what the model was trained as (never a stale/default that disagrees)"; prepare_isonet_model's tar-structure check remains the hard guard for a method/model mismatch. Aligning it away would reintroduce the stale-method bug class.

#### [#36] 12 log line format — **ALIGN**

`drivers/denoise_predict.py:99, 108, 206-226, 529`

- **Behavior:** Three format drifts: (a) calculate_memory_aware_tiles logs with a `[DRIVER]` prefix and no flush=True even though it runs in TASK mode (:99); (b) bare `[WARN]` prefix (:108); (c) the IsoNet path's three isonet.py invocations (:212-226, via the isonet() closure :206-210) are never echoed with a `Command:` line — only the cryoCARE path echoes `[TASK {idx}] Command:` (:529).
- **Canonical:** [SUPERVISOR] / [TASK n] prefixes with flush=True throughout; the task echoes `Command:` before executing (extract_candidates_pytom.py:427, denoise_predict.py:529).
- **Evidence:**
```
print(f"[DRIVER] Tomogram dimensions: {dims}")
```
- **Rationale:** Cosmetic and log-parsing consistency only; safe to unify in a flagged commit. The missing Command: echo on the IsoNet path also costs debuggability parity with every other task-mode tool invocation.

#### [#37] 7 (noted only) hardcoded tool-name strings — **KEEP**

`drivers/denoise_predict.py:145, 208, 212-226, 531`

- **Behavior:** Container tool name hardcoded as tool_name="isonet" in wrap_command_for_tool for all IsoNet subcommands (:208), while the cryoCARE path uses params.get_tool_name() (:531). Executable strings hardcoded: cryoCARE_predict.py (:145) and isonet.py prepare_star/deconv/predict (:213, :220, :224).
- **Canonical:** Container wrap tool via params.get_tool_name() (the cryoCARE branch and other drivers).
- **Evidence:**
```
tool_name="isonet"
```
- **Rationale:** The "isonet" hardcode is deliberate method dispatch — a DenoiseMethod.ISONET job must use the isonet container regardless of the job's configured tool. Noted with file:lines for the dimension-7 owner as requested.

### ts_alignment (4)

#### [#38] 5 item/TS enumeration source — **ASK**

`drivers/ts_alignment.py:53-56 (glob at 55), invoked at 234`

- **Behavior:** Supervisor enumerates TS by globbing *.tomostar in job_dir/tomostar (a local snapshot of the producer's tomostar dir) via enumerate_tomostar_names(); the filesystem is the source of truth for which TS get dispatched.
- **Canonical:** read_tilt_series_names_from_input_star() (array_job_base.py:225-239) — 'The input STAR is the data contract between jobs — use it.' Used by ts_ctf.py:212 and ts_reconstruct.py:129 (local copy of the helper). ts_alignment has an input_star available (required to exist at ts_alignment.py:298-301 for aggregation) but only uses it there, not for enumeration.
- **Evidence:**
```
files = sorted(tomostar_dir.glob("*.tomostar"))
```
- **Rationale:** Functional set difference — glob picks up but star would not: (a) stale .tomostar files the producer's current star no longer lists (e.g. a TS the tilt filter dropped entirely on a re-run, or leftovers in a reused producer dir) get aligned anyway and re-enter downstream scope via the aggregation star; (b) because the glob runs on a once-copied snapshot (see the dim-6 entry), a supervisor re-run still enumerates TS deleted upstream since the first run. Star lists but glob misses: (a) a TS present in the producer star whose .tomostar file is missing (partial producer write) is silently omitted and the job green-ticks without it — star-driven dispatch would instead fail loud at staging (stage_alignment_environment raises FileNotFoundError, ts_alignment.py:83-84); (b) on re-run, TS newly added upstream never enter the stale snapshot and are silently never aligned. Unaffected either way: user exclusions (apply_exclusions at :247 runs on whichever list is produced). Caveat for the maintainer: the base warning's stated rationale (WarpTools writes {ts}.xml even for failed TS, so an xml-glob resurrects excluded TS) targets consumers of warp XMLs DOWNSTREAM of alignment; ts_alignment globs tomostars upstream of any XML, so the applicable hazard here is the stale-file/stale-snapshot one, not the xml one. Whether alignment's input_star block layout matches the helper's star_data.get("global") lookup for the tsImport/tiltFilter producer output also needs confirming before switching.

#### [#39] 6 staging behavior (input snapshotting) — **ASK**

`drivers/ts_alignment.py:225-232 (guards at 228, 231)`

- **Behavior:** Supervisor copies the ENTIRE upstream tomostar dir plus the settings file into job_dir exactly once, guarded by `if not ...exists()` — re-runs of the supervisor reuse the stale snapshot; tasks then stage from job_dir/tomostar and job_dir/warp_tiltseries.settings (:365-366, :379).
- **Canonical:** Base stage_per_ts_environment (array_job_base.py:411-465) stages per-task directly from the producer's live dirs (original_tomostar_dir = settings_file.parent / 'tomostar', :441) fresh on every task run; no whole-dir job-local snapshot exists in the base or in ts_ctf/ts_reconstruct.
- **Evidence:**
```
if not local_tomostar_dir.exists():
            shutil.copytree(str(tomostar_dir), str(local_tomostar_dir))
```
- **Rationale:** Could change which data is processed: after the upstream tilt filter is re-run (tomostars re-trimmed, TS added/removed), a supervisor re-run aligns the OLD tomostar content from the snapshot — stale tilt trims, deleted TS still processed, new TS invisible (couples with the dim-5 enumeration entry). The snapshot may be deliberate (stable paths for staged envs per the comment at :225-226, and insulation from producer-dir churn mid-array), so the maintainer must decide between refresh-on-rerun, staging direct from producer like the base, or keeping the snapshot semantics documented.

#### [#40] 6 staging behavior (helper reimplementation) — **ALIGN**

`drivers/ts_alignment.py:59-92`

- **Behavior:** Local stage_alignment_environment() duplicates the base helper's step 1 (copy settings, :75-76) and step 2 (copy tomostar with absolute movie paths, :78-87) verbatim, but replaces step 3 with creating an EMPTY warp_tiltseries output dir (:90) instead of symlinking an input XML, and returns stage_root (a single Path) instead of the base's (staged_settings, staged_processing) tuple.
- **Canonical:** stage_per_ts_environment (array_job_base.py:411-465): same steps 1-2, then symlinks the per-TS input XML from input_processing (:456-463) and returns a 2-tuple.
- **Evidence:**
```
def stage_alignment_environment(job_dir: Path, ts_name: str, source_tomostar_dir: Path, source_settings: Path) -> Path:
```
- **Rationale:** The deviation is forced by the job (alignment PRODUCES the warp XMLs, it has none to stage in), but the shared two-thirds is hand-copied. Safe to unify by giving the base helper an optional input-XML leg (e.g. input_processing=None → create empty output dir); behavior-preserving. Hardcoded tool-name strings noted for the dim-7 owner: "WarpTools ts_aretomo" at :99 and "WarpTools ts_etomo_patches" at :119 (container tool_name itself is NOT hardcoded — params.get_tool_name() at :387).

#### [#41] 9 results tally strictness — **KEEP**

`drivers/ts_alignment.py:265-288 (fail-only-on-wipeout at 278-281)`

- **Behavior:** Tolerant tally: job fails (RELION_JOB_EXIT_FAILURE + sys.exit(1)) ONLY if results.ok is empty; otherwise failed+missing TS are warned about, dropped from aggregation (aligned_ts = results.ok at :276 feeds adapter.ingest at :313), and the job exits SUCCESS.
- **Canonical:** Strict tally in all 7 other array drivers: `if not results.all_succeeded:` → touch RELION_JOB_EXIT_FAILURE + sys.exit(1) (subtomo_extraction.py:274-277, ts_reconstruct.py:167, ts_ctf.py:285, fs_motion_and_ctf.py:350, template_match_pytom.py:417, extract_candidates_pytom.py:285, denoise_predict.py:423).
- **Evidence:**
```
# solve every tilt-series. One bad TS must NOT abort the whole job and
```
- **Rationale:** Deliberate and documented in-code (:272-275): per-TS alignment failure is normal in cryo-ET (AreTomo cannot solve every TS) and one bad TS must not halt the pipeline. The known consequence — failed TS silently absent from the output star and unreachable for sparse re-dispatch downstream — is owned by roadmap docs/roadmaps/05-per-ts-top-up.md, not by this consolidation. Consolidation should model tally strictness as an explicit per-driver policy knob, defaulting strict.

### subtomo_extraction (7)

#### [#42] 3 manifest read key — **ALIGN**

`drivers/subtomo_extraction.py:381`

- **Behavior:** Task mode reads manifest.get("items") or [] — tolerant .get with a silent empty-list fallback, so a manifest missing the key degrades to an 'index out of range' exit instead of a KeyError naming the real problem.
- **Canonical:** All 7 other array drivers read manifest["ts_names"] (ts_alignment.py:357, ts_reconstruct.py:223, ts_ctf.py:338, fs_motion_and_ctf.py:412, template_match_pytom.py:456, extract_candidates_pytom.py:404, denoise_predict.py:475). write_manifest (array_job_base.py:59) writes BOTH keys with identical content.
- **Evidence:**
```
ts_names = manifest.get("items") or []
```
- **Rationale:** Since write_manifest always emits both "items" and "ts_names" with the same list, switching the read to manifest["ts_names"] is behavior-identical for every manifest this codebase writes, and removes the only consumer keeping the duplicate "items" key alive.

#### [#43] 3+11 manifest error handling (sys.exit outside the status-writing try) — **ALIGN**

`drivers/subtomo_extraction.py:375-384`

- **Behavior:** Manifest-missing (FileNotFoundError → :377-379) and index-out-of-range (:382-384) each print and sys.exit(1) BEFORE the try block that writes .fail status (:390). No status marker is written, so the TS shows up as 'missing' in the supervisor tally and the per-TS UI strip, not 'failed'.
- **Canonical:** Other drivers raise inside the status-writing try — e.g. `raise IndexError(f"SLURM_ARRAY_TASK_ID {array_idx} out of range (manifest has {len(ts_names)})")` (ts_alignment.py:359, same idiom in ts_reconstruct.py:225, ts_ctf.py:340, fs_motion_and_ctf.py:416, template_match_pytom.py:458, extract_candidates_pytom.py:406, denoise_predict.py:478) — so the generic handler records a .fail (under a `_unknown_idx{n}` fallback label when ts_name is unknown).
- **Evidence:**
```
print(f"[TASK {array_idx}] Array index out of range ({len(ts_names)} items)", file=sys.stderr, flush=True)
        sys.exit(1)
```
- **Rationale:** Terminal outcome is equivalent for the supervisor (strict tally fails on 'missing' as well as 'fail'), so unifying to the raise-inside-try shape is safe; the observable delta is only which bucket the item lands in (missing → failed) plus a recorded marker. Note the fallback-label side effect either way: a pre-ts_name failure writes a `_unknown_idxN.fail` that is not a manifest item.

#### [#44] 4 exclusion application — **ALIGN**

`drivers/subtomo_extraction.py:198-229 (load at 201, filter 202-203, skip-writes 228-229)`

- **Behavior:** Hand-rolls exclusions: calls load_excluded_ts() directly, filters ts_with_picks itself, clears ALL *.skip files (:221-223), then write_skip_status per excluded TS with the reason string "excluded from processing" — but does NOT clear stale .ok/.fail markers for excluded TS.
- **Canonical:** apply_exclusions (array_job_base.py:281-312), called by the other 7 array drivers, which FIRST unlinks stale .ok/.fail for each excluded TS (:301-305) precisely because 'double-marking would break the ok + skip == len(ts_names) tally' (:290), then writes the same reason string (:306) and returns the excluded subset for callers to drop from aggregation.
- **Evidence:**
```
write_skip_status(status_dir, ts, reason="excluded from processing")
```
- **Rationale:** The omission is a live defect vector, not just duplication: a TS that succeeded in a previous run (.ok on disk) and is excluded afterwards ends up carrying .ok + .skip, making len(ok)+len(skip) == len(ts_names)+1, so collect_task_results.all_succeeded goes False and the strict check at :274 fails the whole job. Replace :201-203/:228-229 with `excluded = apply_exclusions(job_dir, project_path, ts_names)` plus the existing ts_with_picks filter; keep the subtomo-specific parts that base does not cover — the *.skip refresh (:221-223, needed because empty-TS pick status changes between runs) and the empty-TS skip markers (:224-227).

#### [#45] 6 staging behavior (supervisor-side staging) — **KEEP**

`drivers/subtomo_extraction.py:233-237 (loop), 309-359 (_stage_per_ts)`

- **Behavior:** The SUPERVISOR stages every picked TS up front, before dispatch: _stage_per_ts slices the upstream particles/tomograms DataFrames by rlnTomoName and writes per-TS particles.star + tomograms.star + optimisation_set.star into .staging/task_<ts>/ (idempotent overwrite each supervisor run). Tasks only read their staged optset (:391-394); task outputs go to .staging/task_<ts>/out/ and the supervisor merges with shutil.move (:515-523).
- **Canonical:** Base pattern (stage_per_ts_environment, array_job_base.py:411-465, used task-side by warp drivers): each TASK stages its own environment at run time from the producer's files (copy settings + tomostar, symlink XML).
- **Evidence:**
```
_stage_per_ts(staging_root, ts_name, optics_df, particles_df, tomograms_df, general_kv)
```
- **Rationale:** Inherent to the job's input format: slicing a RELION optimisation set requires the full particles/tomograms tables in memory, which only the supervisor parses — per-task re-parsing of the whole upstream star N times would be waste, and the base helper is Warp-shaped (settings/tomostar/xml) and unusable here. Documented in the module docstring (:7-17). Consolidation should model 'who stages' (supervisor vs task) as an explicit variation point. Hardcoded tool-name strings noted for the dim-7 owner: command "relion_tomo_subtomo" at :413 and tool_name="relion" at :447.

#### [#46] 8 status writes (silent swallow on fail-status write) — **ALIGN**

`drivers/subtomo_extraction.py:464-467`

- **Behavior:** The guard around the .fail write in the task-mode error handler is a bare `except Exception: pass` — a failed status write (e.g. unwritable status dir) leaves no trace in the task log.
- **Canonical:** The other 7 array drivers log it: `except Exception as inner: print(f"[TASK {array_idx}] Could not write fail status: {inner}", file=sys.stderr, flush=True)` (ts_alignment.py:417-418, ts_reconstruct.py:272-273, ts_ctf.py:377-378, fs_motion_and_ctf.py:463-464, template_match_pytom.py:545-546, extract_candidates_pytom.py:449-450, denoise_predict.py:548-549).
- **Evidence:**
```
except Exception:
            pass
```
- **Rationale:** Pure logging addition, zero behavior risk, and the bare swallow violates the project's exception policy (CLAUDE.md: bare `except Exception: pass` fails review; existing sites converted opportunistically when the file is touched).

#### [#47] 8+9 whole-job zero-picks skip branch — **KEEP**

`drivers/subtomo_extraction.py:153-189`

- **Behavior:** If upstream produced 0 picks across ALL tomograms, the supervisor writes a .skip marker for every upstream TS, a .skipped_no_candidates.json sentinel for the UI, touches RELION_JOB_EXIT_SUCCESS, and returns — no manifest is ever written (submit_array_job unreached) and no array runs.
- **Canonical:** Base has only the PER-ITEM skip verb (write_skip_status, array_job_base.py:149-163, documented for 'template matching yielded zero candidates for a tilt-series'); no other array driver has a whole-job zero-work SUCCESS path, and every other SUCCESS exit follows a submit_array_job call that wrote a manifest.
- **Evidence:**
```
# Whole-job equivalent of the per-TS .skip path: upstream picking
```
- **Rationale:** Deliberate and documented in-code (:158-166): exiting FAILURE would halt the schemer on an upstream-data condition that is diagnostic, not a job bug, and it mirrors per-TS skip semantics ('succeeded but did no work'). Consolidation note: this SUCCESS path leaves .skip files with no .task_manifest.json beside them — any manifest-reading UI tracker sees markers without a manifest, so the future base class needs a first-class 'job-level skip' verb that also records the item list.

#### [#48] 12 log line format — **ALIGN**

`drivers/subtomo_extraction.py:85, 121, 188, 371`

- **Behavior:** Keeps the legacy one-shot banners: opens with '--- SLURM JOB START (Subtomogram Extraction) ---' (:85), prints '--- SLURM JOB END (Exit Code: 0) ---' only on the merge_only (:121) and zero-picks (:188) branches (the normal path says '[SUPERVISOR] Job finished successfully.' :294), and the task bootstrap message drops 'FATAL' (:371 'BOOTSTRAP ERROR').
- **Canonical:** The other array drivers open with `print("Python", sys.version, flush=True)` (9 drivers, e.g. ts_alignment.py:186) plus the '--- <driver>: SUPERVISOR/TASK mode ---' lines, and use '[TASK {n}] FATAL BOOTSTRAP ERROR' (ts_alignment.py:349 and 6 siblings); the 'SLURM JOB START' banner survives only in the non-array one-shot drivers (class3d, reconstruct_particle, miss_align, denoise_train).
- **Evidence:**
```
print("--- SLURM JOB START (Subtomogram Extraction) ---", flush=True)
```
- **Rationale:** Cosmetic only — [SUPERVISOR]/[TASK n] prefixes and 'Command:' echo lines (:435) already match the convention; unifying the banners is free once the mode-dispatch main() moves into the base class.

### tilt_filter (3)

#### [#49] 2 bootstrap try/except — **ALIGN**

`drivers/tilt_filter.py:26-29`

- **Behavior:** get_driver_context(TiltFilterParams) is called bare in main() with no try/except and no '[DRIVER] FATAL BOOTSTRAP ERROR' handler; the RELION marker paths are only defined afterwards (lines 28-29). Any bootstrap exception get_driver_context does not itself sys.exit(1) on (e.g. a non-PathResolutionError from the resolver) escapes as a raw uncaught traceback instead of a labeled one-line FATAL. (No plain driver writes a RELION marker on bootstrap failure, so the marker behavior itself is not a delta — only the log shape and catch are.)
- **Canonical:** Majority plain-driver pattern wraps get_driver_context in try/except, prints '[DRIVER] FATAL BOOTSTRAP ERROR: {e}' to stderr, and sys.exit(1) (denoise_train.py:233-239, miss_align.py:180-186).
- **Evidence:**
```
_project_state, job_model, _context_data, job_dir, project_path, _job_type = get_driver_context(TiltFilterParams)
```
- **Rationale:** Pure error-path hardening; success path byte-identical. Note the heavy DL/service imports are deliberately INSIDE the work try (lines 46-57) so an import failure does write RELION_JOB_EXIT_FAILURE — that placement should be preserved when aligning.

#### [#50] 10 registry stamping — **KEEP**

`drivers/tilt_filter.py:134-159`

- **Behavior:** Hand-rolled per-frame registry write: get_registry_for(project_path) (137), loop of registry.set_frame_filtered(stem, is_filt, reason, probability) (143-153) with inner 'except KeyError: pass' for unknown stems (152-153), registry.save() (154) — all wrapped in a best-effort broad 'except Exception' that only prints a WARNING (158-159), so a registry failure can never fail the job.
- **Canonical:** Base-class drivers touch the registry only via preflight_registry() (fail-loud BEFORE dispatch, array_job_base.py:320-361) and route writes through ingest adapters; no other driver writes registry state by hand from inside job execution.
- **Evidence:**
```
registry.set_frame_filtered(
```
- **Rationale:** Explicitly documented as registry-consolidation Stage 2 (comment lines 128-133: 'Best-effort ... any registry failure only warns — it must never fail a filter that already trimmed the tomostar'; Stage 3 migrates consumers). Consolidating this into an ingest adapter belongs to roadmap 02's staged migration, not a behavior-preserving driver-consolidation commit. The broad except and KeyError-pass are both commented and deliberate.

#### [#51] 12 log line format — **ALIGN**

`drivers/tilt_filter.py:64, 162, 164-168`

- **Behavior:** No '--- SLURM JOB START ---' banner and no Python-version/Node/CWD echo at entry; failure path prints '[DRIVER] FATAL: {e}' + traceback and exits WITHOUT a '--- SLURM JOB END (Exit Code: 1) ---' trailer. Only the success-side END banner exists (line 162). Uses the standard '[DRIVER]' prefix otherwise.
- **Canonical:** Peer plain drivers print 'Python <ver>' + '--- SLURM JOB START ---' + Node + CWD at entry and an END trailer on BOTH exit paths (denoise_train.py:230-231, 241-242, 410, 418; miss_align.py:177-178, 188-189, 352, 360).
- **Evidence:**
```
print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
```
- **Rationale:** Log-only change; makes task_*.out/driver logs greppable with the same START/END markers across all drivers and gives node attribution for node-rot triage (see run_command watchdog reference).

### denoise_train (5)

#### [#52] 2 bootstrap import order/guard — **ALIGN**

`drivers/denoise_train.py:7-8, 21-30`

- **Behavior:** 'from services.computing.container_service import get_container_service' (line 7) and 'import starfile' (line 8) execute BEFORE the sys.path.insert(0, project_root) at lines 21-22 and OUTSIDE the guarded try at 24-30 — without an externally-set PYTHONPATH the services import dies as an unhandled ImportError traceback, bypassing the FATAL message. The guarded except also drops the exception detail ('except ImportError:' with no 'as e', printing only 'FATAL: Could not import services.').
- **Canonical:** path-insert first, then ALL services imports inside the guarded try, printing the underlying error (tilt_filter.py:13-22, miss_align.py:30-39, driver_base.py:17-26 all print the ImportError detail).
- **Evidence:**
```
from services.computing.container_service import get_container_service
```
- **Rationale:** Works today only because qsub.sh/the array script export PYTHONPATH before invoking drivers; moving line 7 under the guard (after the path insert) and adding 'as e' is behavior-preserving on every success path and only improves the failure diagnostic.

#### [#53] 5 item/TS enumeration (block + column resolution) — **ASK**

`drivers/denoise_train.py:264-279`

- **Behavior:** cryoCARE path takes the FIRST star block ('next(iter(tomo_df.values()))', line 266) rather than requiring 'global', and when 'rlnTomoReconstructedTomogram' is absent it GUESSES the first column whose name contains 'Name' or 'Tomogram' (lines 273-277), raising only if no such column exists (279). The IsoNet path is stricter (lines 55-57 prefer the 'global' block; halves columns required at 58-62).
- **Canonical:** read_tilt_series_names_from_input_star reads the 'global' block's rlnTomoName strictly via StarfileService (array_job_base.py:225-239); project policy (CLAUDE.md 'Surfacing uncertainty') forbids guessing semantically load-bearing values.
- **Evidence:**
```
possible_cols = [c for c in tomo_df.columns if "Name" in c or "Tomogram" in c]
```
- **Rationale:** A guessed column could silently change WHICH tomograms are matched by the tomograms_for_training substring filter (line 291 matches against the guessed column's value). Maintainer should say whether this fallback ever fires on real tsReconstruct outputs or can be tightened to a hard error.

#### [#54] 11 error handling (soft dependency gate on validation) — **KEEP**

`drivers/denoise_train.py:10-19, 161-162, 189-190`

- **Behavior:** numpy/mrcfile/cryocare are imported at module top inside a try; on ImportError HAS_DEPS=False with a stderr '[WARN]' and every validate_input_mrc/validate_extracted_data call returns immediately — the NaN/Inf/flat-input and patch-corruption validation is skipped entirely when the driver runs where cryocare deps are absent.
- **Canonical:** No other driver has environment-conditional validation; the exception policy allows narrow expected catches, but this gate silently (beyond one WARN line) disables checks the file's own comments describe as load-bearing (line 202: 'this is what caused your IndexError previously').
- **Evidence:**
```
print("[WARN] Could not import cryocare/mrcfile/numpy. Validation will be skipped.", file=sys.stderr)
```
- **Rationale:** Deliberate and commented ('Direct imports for validation since we are inside the container', line 10) and non-silent. Caveat for the maintainer: that comment contradicts the driver wrapping its tool commands via container_service (lines 369-371, 385-387), which implies the driver runs NATIVELY and validation is therefore always skipped unless the venv has these deps — worth a runtime confirmation, but not a consolidation-commit change.

#### [#55] 12 log line format (command echo) — **ALIGN**

`drivers/denoise_train.py:366-372, 382-388, 100-127`

- **Behavior:** Builds and runs container-wrapped commands without ever echoing the command line — only stage descriptions ('[DRIVER] Extracting training data...', '[DRIVER] Training model...'); the IsoNet chain likewise runs its four isonet.py commands (101-127) with no echo.
- **Canonical:** Peer echoes the exact command before running: miss_align.py:327 prints '[DRIVER] Train command: {train_cmd}'. (Full cross-driver echo convention is owned by the dimension-7 agent; noting only the observable delta among assigned files.)
- **Evidence:**
```
print("[DRIVER] Extracting training data...", flush=True)
```
- **Rationale:** Echoing the pre-wrap command is log-only and is what makes the Tier-A '--print-cmd' snapshot idea (protocols harness) and post-mortem debugging possible from task logs alone.

#### [#58] 7-NOTE hardcoded tool-name strings (for the dim-7 agent; not a divergence) — **KEEP**

`drivers/denoise_train.py:51, 101, 110, 114, 125, 137, 367, 370, 383, 386, 403`

- **Behavior:** Hardcoded tool identifiers: tool_name="isonet" (51); 'isonet.py prepare_star' (101), 'isonet.py deconv' (110), 'isonet.py make_mask' (114), 'isonet.py refine' (125); 'tar -czf denoising_model.tar.gz isonet_maps' (137); 'cryoCARE_extract_train_data.py' (367); tool_name="cryocare" (370, 386); 'cryoCARE_train.py' (383); 'tar -czf ../denoising_model.tar.gz denoising_model' (403).
- **Canonical:** n/a — command construction/quoting is owned by the dedicated dimension-7 agent; recorded here per instruction only.
- **Evidence:**
```
extract_cmd = f"cryoCARE_extract_train_data.py --conf {config_json_path.name}"
```
- **Rationale:** Inventory note only; no action proposed from this census. tilt_filter.py has NO external tool commands and no tool-name strings.

### miss_align (3)

#### [#56] 5 item/TS enumeration source — **ASK**

`drivers/miss_align.py:217-241, 46-63`

- **Behavior:** Consumes the ENTIRE upstream warp_tiltseries/ directory via shutil.copytree (line 239); the in-container stamp script enumerates 'glob.glob("warp_tiltseries/*.xml")' (line 52) and the tool is pointed at the whole staged dir (build_config training_directory, lines 136-138). The staged aligned_tilt_series.star (line 241) is never used by the driver to select TS — every XML upstream wrote, including alignment-failed/'unselected' and registry-excluded TS, gets stamped and refined. No apply_exclusions/registry consultation anywhere.
- **Canonical:** 'The input STAR is the data contract between jobs' — array_job_base.py:225-239 explicitly warns that a '*.xml'-glob enumeration 'would silently pull excluded TS back into scope'; array drivers also pre-mark registry-excluded TS via apply_exclusions (array_job_base.py:281-312).
- **Evidence:**
```
shutil.copytree(upstream_processing, staged_processing)
```
- **Rationale:** Could change which data is processed: restricting the copy to the star's TS list (or honoring registry exclusions) would remove TS from training. For a global refinement model extra TS may be intended (more training signal) or harmful (garbage alignments polluting the learned model + refined verdicts written for muted TS) — maintainer call, not a flagged-commit unification.

#### [#57] 6 staging behavior — **KEEP**

`drivers/miss_align.py:217-241, 249-259`

- **Behavior:** Stages a full COPY (copytree) of upstream warp_tiltseries/ plus .settings and the aligned star into the job dir, then mutates the copies (in-container dim-stamping, then in-place XML refinement by the tool). Adds resume logic unique among drivers: if iter*/ snapshots AND model.ckpt exist in the staged dir, keep it and resume at the next iteration (224-235); otherwise rmtree + fresh copy (236-241), with stamping skipped on resume (249).
- **Canonical:** Base staging is per-TS .staging/task_{ts} with COPIED small files (settings, tomostar) and a SYMLINKED per-TS XML (array_job_base.py:411-465) — upstream data is linked, never duplicated or mutated.
- **Evidence:**
```
miss-alignment refines the XMLs IN PLACE and writes an
```
- **Rationale:** Documented invariant (docstring lines 10-12: 'never mutate the upstream aligntiltsWarp output'); symlinking the XMLs would let the tool corrupt the upstream aligntiltsWarp output, so the copy is load-bearing. Resume-from-checkpoint is deliberate with an explicit PENDING-RUNTIME note (lines 222-223).

#### [#59] 7-NOTE hardcoded tool-name strings (for the dim-7 agent; not a divergence) — **KEEP**

`drivers/miss_align.py:254, 257, 316, 329`

- **Behavior:** Hardcoded tool identifiers: 'python stamp_dims.py ...' (254); tool_name="miss_alignment" (257, 329); 'miss-alignment train' (316).
- **Canonical:** n/a — command construction/quoting is owned by the dedicated dimension-7 agent; recorded here per instruction only.
- **Evidence:**
```
"miss-alignment train",
```
- **Rationale:** Inventory note only; no action proposed from this census.

### ts_import (2)

#### [#60] 1 mode-dispatch (array lifecycle absent: dims 3/4/5/6/8/10) — **KEEP**

`drivers/ts_import.py:8, 97-153`

- **Behavior:** Plain single-mode main(): never inspects SLURM_ARRAY_TASK_ID, no supervisor/task split, no manifest, no .task_status, no apply_exclusions, no preflight_registry/ingest adapter. Relationship to canon elsewhere: bootstrap MATCHES the supervisor canon (marker touch + traceback + exit(1), lines 100-109); uses the non-array '[DRIVER]' log dialect with a '[DRIVER] Command:' echo (line 121); strict output verification raising FileNotFoundError (131-139); touches RELION_JOB_EXIT_SUCCESS + sys.exit(0) / FAILURE + sys.exit(1) itself (143-153).
- **Canonical:** main() branches on os.environ.get('SLURM_ARRAY_TASK_ID') into run_supervisor_mode()/run_task_mode(); supervisor runs enumerate -> apply_exclusions -> preflight_registry -> submit_array_job -> collect_task_results -> adapter ingest (ts_ctf.py:164-172, 253-307; array_job_base.py:586-670).
- **Evidence:**
```
Runs as a single SLURM job (not an array).
```
- **Rationale:** WarpTools ts_import + create_settings is one cheap whole-dataset metadata command (no GPU); per-TS decomposition has no payoff and the single-job shape is documented in the module docstring. Exclusions are defined as pre-dispatch array machinery (apply_exclusions docstring: 'Call this right before submit_array_job'), so their absence here is by design, not drift.

#### [#61] 7-note hardcoded-tool-name — **ALIGN**

`drivers/ts_import.py:125 (also builder literals at 46, 69)`

- **Behavior:** wrap_command_for_tool called with literal tool_name="warptools"; command builder hardcodes 'WarpTools ts_import' (line 46) and 'WarpTools create_settings' (line 69). Noted for the dimension-7 agent per instructions.
- **Canonical:** tool_name=params.get_tool_name() (ts_ctf.py:88, 358); roadmap 04 stage 2 converts the 11 hardcoded tool-name strings to get_tool_name() calls.
- **Evidence:**
```
tool_name="warptools"
```
- **Rationale:** Roadmap stage 2 already schedules this exact conversion; transcript-visible and safe (same resolved tool config) provided TsImportParams exposes get_tool_name() like the array param classes do.

### class3d (3)

#### [#62] 1 mode-dispatch (array lifecycle absent: dims 3/4/5/6/8/10) — **KEEP**

`drivers/class3d.py:19-30`

- **Behavior:** Single-mode main() goes straight to get_driver_context, no SLURM_ARRAY_TASK_ID branch; no manifest/.task_status/exclusions/registry (particle-stage single relion_refine process; input is one optimisation_set.star from the resolver, lines 35-41). Relationship elsewhere: '[DRIVER]' dialect with '--- SLURM JOB START/END ---' banners (20, 159) and '[DRIVER] Command:' echo (118); strict completion check on the numbered run_it{N}_optimisation_set.star with mirror-copy to the bare name (137-146), tolerant WARN-only on class volumes (148-156); work-loop error style matches canon: raise -> catch -> traceback -> FAILURE marker -> sys.exit(1) (161-165); success marker touched with implicit exit 0 (158-159).
- **Canonical:** Env-var mode dispatch plus the write_manifest/submit_array_job/collect_task_results lifecycle (ts_ctf.py:164-172; array_job_base.py:586-670).
- **Evidence:**
```
print("--- SLURM JOB START (Class3D) ---", flush=True)
```
- **Rationale:** relion_refine is inherently one multi-threaded/GPU process over the whole particle set; there is no per-TS boundary to parallelize over. Single-job drivers are a legitimate second shape the consolidation template should acknowledge rather than erase.

#### [#63] 2 bootstrap — **ALIGN**

`drivers/class3d.py:22-26`

- **Behavior:** Bootstrap except catches Exception, prints '[DRIVER] BOOTSTRAP ERROR' to stderr WITHOUT traceback.print_exc() and WITHOUT touching RELION_JOB_EXIT_FAILURE, then sys.exit(1).
- **Canonical:** Bootstrap failure touches RELION_JOB_EXIT_FAILURE at Path.cwd(), prints 'FATAL BOOTSTRAP ERROR' AND traceback.print_exc(), then sys.exit(1) (ts_ctf.py:183-188; ts_import.py:104-109 follows the same shape).
- **Evidence:**
```
print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
```
- **Rationale:** Diagnostics-only divergence: qsub.sh's wrapper still touches the failure marker on any nonzero exit (the block array_job_base.py:532-541 strips for array tasks proves it exists in the template), so adding the in-driver marker + traceback changes no data flow and restores lost stack traces. Note most get_driver_context failures sys.exit(1) internally (SystemExit bypasses 'except Exception' in every driver), so this except only fires on exceptions that escape it — exactly the cases where the missing traceback hurts.

#### [#64] 7-note hardcoded-tool-name — **ALIGN**

`drivers/class3d.py:126 (builder literal at 50)`

- **Behavior:** wrap_command_for_tool called with literal tool_name="relion"; builder hardcodes 'relion_refine' (line 50). Noted for the dimension-7 agent.
- **Canonical:** tool_name=params.get_tool_name() (ts_ctf.py:88, 358).
- **Evidence:**
```
tool_name="relion"
```
- **Rationale:** Roadmap stage 2 conversion; transcript-visible and safe.

### reconstruct_particle (3)

#### [#65] 1 mode-dispatch (array lifecycle absent: dims 3/4/5/6/8/10) — **KEEP**

`drivers/reconstruct_particle.py:25-36`

- **Behavior:** Single-mode main(), no SLURM_ARRAY_TASK_ID branch; no manifest/.task_status/exclusions/registry (single relion_tomo_reconstruct_particle process over one optimisation_set.star, lines 41-43). Relationship elsewhere: '[DRIVER]' dialect with START/END banners (26, 134) and '[DRIVER] Command:' echo (103); strict check on merged.mrc (116-121), tolerant WARN-only on half1/half2.mrc (126-131); work-loop error style matches canon (136-140); success marker + implicit exit 0 (133-134). Near-identical twin of class3d.py.
- **Canonical:** Env-var mode dispatch plus the array lifecycle (ts_ctf.py:164-172; array_job_base.py:586-670).
- **Evidence:**
```
print("--- SLURM JOB START (Reconstruct Particle) ---", flush=True)
```
- **Rationale:** Whole-particle-set reduction with no per-TS boundary; single-job shape is correct for it.

#### [#66] 2 bootstrap — **ALIGN**

`drivers/reconstruct_particle.py:28-32`

- **Behavior:** Identical to class3d's divergence: bootstrap except prints '[DRIVER] BOOTSTRAP ERROR' without traceback and without touching RELION_JOB_EXIT_FAILURE, then sys.exit(1).
- **Canonical:** Marker touch + FATAL print + traceback.print_exc() + sys.exit(1) (ts_ctf.py:183-188; ts_import.py:104-109).
- **Evidence:**
```
print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
```
- **Rationale:** Same as class3d: qsub wrapper covers the marker on nonzero exit, so aligning is diagnostics-only; fix both twins in the same flagged commit.

#### [#67] 7-note hardcoded-tool-name — **ALIGN**

`drivers/reconstruct_particle.py:110 (builder literal at 50)`

- **Behavior:** wrap_command_for_tool called with literal tool_name="relion"; builder hardcodes 'relion_tomo_reconstruct_particle' (line 50). Noted for the dimension-7 agent (that agent also owns the unescaped other_args passthrough at 97-100).
- **Canonical:** tool_name=params.get_tool_name() (ts_ctf.py:88, 358).
- **Evidence:**
```
tool_name="relion"
```
- **Rationale:** Roadmap stage 2 conversion; transcript-visible and safe.

### extract_pick_list (6)

#### [#68] 1+2 bootstrap bypass / argparse dispatch — **ASK**

`drivers/extract_pick_list.py:10-11, 31, 108-122`

- **Behavior:** No get_driver_context at all: hand-rolled argparse (--candidate-optset/--list-star/--tomo/--out-dir/--project-root plus box/bin/crop/dose flags, lines 109-121); never loads ProjectState, has no --instance_id and no job model; the only driver_base reuse is 'from drivers.driver_base import run_command' (line 31). Submitted as a one-off SLURM job by backend.extract_pick_list, outside the pipeline graph.
- **Canonical:** get_driver_context(ParamClass) parses --instance_id/--project_path, loads project_params.json, resolves the job model and fresh paths (driver_base.py:45-137); every other driver bootstraps through it.
- **Evidence:**
```
Submitted as a one-off SLURM job by ``backend.extract_pick_list`` (NOT an array, NOT a pipeline job
```
- **Rationale:** Roadmap stage 5 says 'port extract_pick_list.py onto get_driver_context', but a mechanical port is impossible today: the job has no instance in project_params.json and its per-list params (box/binning/crop) arrive as CLI args chosen by backend.extract_pick_list per list, not from a param class. Maintainer must first decide where a non-pipeline one-off gets its identity/params (synthetic instance? param snapshot file?) — that decision could change the backend invocation contract.

#### [#69] 8+9 status writes / RELION markers — **KEEP**

`drivers/extract_pick_list.py:12-14, 124-135`

- **Behavior:** Driver never touches RELION_JOB_EXIT_* markers itself and writes no .task_status files; its success protocol is <out-dir>/result.json ({ok, optimisation_set, particles, count} or {ok: false, error}) plus process exit code; the qsub wrapper touches the RELION markers based on that exit code. The result.json write is best-effort (its own except at 131-134 logs traceback and continues to sys.exit).
- **Canonical:** The driver itself touches RELION_JOB_EXIT_SUCCESS/FAILURE (ts_import.py:143/151; ts_ctf.py:310/317); per-item outcomes go through write_status_atomic .ok/.fail with tmp+os.replace atomicity (array_job_base.py:116-146).
- **Evidence:**
```
The qsub wrapper touches ``RELION_JOB_EXIT_SUCCESS/FAILURE`` in the out-dir; this driver ALSO writes ``<out-dir>/result.json``
```
- **Rationale:** Documented, load-bearing contract: the dashboard consumes result.json to record PickList.mark_extracted; markers still land via the wrapper, and duplicating them in-driver would add nothing for a non-pipeline job that relion_schemer never watches.

#### [#70] 11 error-style — **KEEP**

`drivers/extract_pick_list.py:125-135`

- **Behavior:** Never lets an exception propagate and never touches a failure marker: catch-all converts any Exception to traceback + {ok: false, error: str(e)}, then exits via 'sys.exit(0 if result.get("ok") else 1)'. Failure detail travels in result.json rather than a marker file.
- **Canonical:** Raise inside the work try; outer catch prints FATAL + traceback, touches RELION_JOB_EXIT_FAILURE, sys.exit(1) (ts_ctf.py:314-318; ts_import.py:147-153).
- **Evidence:**
```
sys.exit(0 if result.get("ok") else 1)
```
- **Rationale:** The structured-result contract requires catching everything to serialize the error for the dashboard; exit code still carries the outcome for the qsub wrapper, so nothing is silently swallowed (traceback goes to stderr per policy form 2).

#### [#71] 12 log-format — **ALIGN**

`drivers/extract_pick_list.py:73, 79, 82, 104`

- **Behavior:** Unique '[extract-list]' prefix (neither '[SUPERVISOR]/[TASK n]' nor the single-job majority '[DRIVER]'), and the command echo is lowercase 'command:' — '[extract-list] command: {cmd_str}'.
- **Canonical:** Array canon: '[TASK {array_idx}] Command: {cmd}' (ts_ctf.py:355); single-job majority: '[DRIVER] Command: ...' (ts_import.py:121, class3d.py:118, reconstruct_particle.py:103) — capital-C 'Command:' everywhere else.
- **Evidence:**
```
print(f"[extract-list] command: {cmd_str}", flush=True)
```
- **Rationale:** Log-only change, but load-bearing for roadmap stage 0: the command-transcript capture greps 'Command:' log lines, and this lowercase variant would silently drop the driver from the acceptance corpus.

#### [#72] 8 re-run/skip semantics — **KEEP**

`drivers/extract_pick_list.py:75-79`

- **Behavior:** Idempotency via output-presence probe: skips relion_tomo_subtomo entirely when out/particles.star AND out/Subtomograms/ already exist (but still rebuilds the input optset and rewrites the final optimisation_set.star each run).
- **Canonical:** Settled work is skipped via .ok/.skip status files, not output probes (get_previously_done, array_job_base.py:174-183; submit_array_job:607-624).
- **Evidence:**
```
if (out_run / "particles.star").exists() and (out_run / "Subtomograms").exists():
```
- **Rationale:** Single-item one-off job with its own out-dir per list; a .task_status dir would be protocol overhead with no UI consumer. Worth recording in the ledger so the ArrayDriver template is not force-fitted onto it.

#### [#73] 7-note hardcoded-tool-name — **ALIGN**

`drivers/extract_pick_list.py:94 (builder literal at 41)`

- **Behavior:** wrap_command_for_tool called with literal tool_name="relion"; builder hardcodes 'relion_tomo_subtomo' (line 41). Noted for the dimension-7 agent.
- **Canonical:** tool_name=params.get_tool_name() (ts_ctf.py:88, 358) — though this driver currently has no params object at all (see its bootstrap-bypass entry).
- **Evidence:**
```
tool_name="relion"
```
- **Rationale:** Roadmap stage 2 conversion, but here it is gated on the ASK about giving this driver a param/identity story first — until then the literal must stay.

### subtomo_merge (1)

#### [#74] library-not-a-driver (dims 1,2,8,11,12 inapplicable) — **ALIGN**

`drivers/subtomo_merge.py:1-27, 587 (no __main__ anywhere)`

- **Behavior:** Pure importable library in drivers/: no main(), no argparse, no bootstrap, no sys.exit, no RELION markers; error style is raise-only (FileNotFoundError/ValueError/KeyError); logs with bare print '[MERGE]'/'[MERGE WARN]' without flush (398, 491, 524, 582); contains one broad 'except Exception: pass' fallback in _parse_optimisation_set (75-76). Imported by drivers/subtomo_extraction.py:63, drivers/extract_candidates_pytom.py:46, AND ui/aggregation_merge_card.py:728 (a UI->drivers import); services/visualization/list_extraction.py:37-89 mirrors it instead of importing, explicitly to avoid a services->drivers import.
- **Canonical:** drivers/ files are executable entry points (bootstrap + markers + exit codes); shared logic lives in services/ so UI and services import downward, never from drivers/.
- **Evidence:**
```
Auxiliary merge logic for STA subtomo extraction outputs.
```
- **Rationale:** Roadmap stage 5 relocation to services/ is pure code motion plus three import rewrites (subtomo_extraction.py:63, extract_candidates_pytom.py:46, aggregation_merge_card.py:728) and dissolving the list_extraction mirror; while touching it, the except-pass at 75-76 qualifies for the opportunistic narrow-catch conversion per CLAUDE.md policy.

## Roadmap claim checks

Verdicts against the claims written in 04-driver-consolidation.md (line numbers there predate the Aug 11–12 edits).

- **HOLDS** — ts_ctf copies (not symlinks) the per-TS XML during staging - documented as deliberate (ts_ctf.py:121-123)
  - current lines: `ts_ctf.py:121-123 (comment), 126-132 (guard + unlink + shutil.copy2 at 132)` — Line numbers still exact: the three-line comment at 121-123 reads 'Stage the XML as a real copy (defocus_hand already updated it). A symlink would cause WarpTools to write through to the shared file, making the final copy-back a SameFileError.' The actual copy is shutil.copy2 at line 132, with the base-contrast symlink at array_job_base.py:463.
- **HOLDS** — ts_reconstruct is the SIMPLEST array driver (closest to canonical)
  - current lines: `ts_reconstruct.py:30-41 (imports 10 base verbs), 240-242 (uses base stage_per_ts_environment directly); residual divergences at 57-63, 76, 234-238, 259-262` — Confirmed relative to ts_ctf, the only other driver in my assignment scope: ts_reconstruct uses every base verb including stage_per_ts_environment unchanged, has no supervisor-side tool step, no hand-rolled staging, no copy-back, and reads all locations from resolved paths. What still diverges from pure canonical: (1) local duplicate of read_tilt_series_names_from_input_star (57-63, ALIGN — base helper at array_job_base.py:225-239 is logic-identical); (2) task-level idempotency short-circuit writing .ok when the output MRC already exists (234-238, documented in docstring lines 14-15, KEEP); (3) post-run output-MRC verification raise (259-262, extra fail-loud check the base lacks, KEEP); (4) hardcoded 'WarpTools' literal (76, pattern-wide). A full 8-driver 'simplest' ranking is outside my two-file scope.
- **HOLDS** — fs_motion_and_ctf manifest protocol: filename .task_manifest.json; base keys items/ts_names/item_count/item_label plus job-specific extra key 'ts_frames' (ts_name -> [frame filenames]); task indexes manifest['ts_names'][array_idx] with an explicit out-of-range IndexError, then manifest['ts_frames'][ts_name] (hard KeyError if absent).
  - current lines: `write: fs_motion_and_ctf.py:333 via array_job_base.py:50-67 (keys at 59) and services/array_tasks.py:16 (MANIFEST_FILENAME='.task_manifest.json'); read: fs_motion_and_ctf.py:411-418` — Uses the majority 'ts_names' key (7 of 8 array drivers; only subtomo_extraction.py:381 uses 'items'). Nobody passes write_manifest's ts_metadata= parameter — it is currently a dead parameter of the base.
- **HOLDS** — template_match_pytom manifest protocol: base keys plus extras input_tomograms_star, raw_tomo_paths, legacy_text_input, patched_tomograms_star, template_path, mask_path, angle_list_path; task indexes manifest['ts_names'][array_idx] with IndexError guard; raw_tomo_paths read tolerantly (.get or {}) with an explicit KeyError, input_tomograms_star/template_path/mask_path are hard keys, legacy_text_input defaults True, angle_list_path/patched_tomograms_star via .get.
  - current lines: `write: template_match_pytom.py:377-385; read: 455-471, 503-504` — Task also extends container binds with template/mask parent dirs (474-477), sets TQDM_DISABLE=1 in main() (250), and contains a dead unassigned dict-comprehension over context paths (473).
- **HOLDS** — fs_motion_and_ctf results tally is STRICT: any .fail or missing item -> RELION_JOB_EXIT_FAILURE + sys.exit(1); .skip counts as settled toward all_succeeded.
  - current lines: `fs_motion_and_ctf.py:343-353 (strict gate 350-353), via array_job_base.py:242-257 (all_ok at 254)` — Success path touches RELION_JOB_EXIT_SUCCESS at 382; bootstrap failure touches FAILURE at Path.cwd() (284-289). Matches canonical.
- **HOLDS** — template_match_pytom results tally is STRICT: any .fail or missing tomogram -> RELION_JOB_EXIT_FAILURE + sys.exit(1); .skip counts as settled.
  - current lines: `template_match_pytom.py:410-420 (strict gate 417-420); SUCCESS marker 426; bootstrap FAILURE 271-276` — Identical shape to fs_motion_and_ctf and the base pattern.
- **HOLDS** — fs_motion_and_ctf registry stamping path: preflight_registry before dispatch, then post-success FsMotionCtfIngestAdapter — get_registry_for(project_path), empty-registry hard fail, adapter.ingest(results.ok), adapter.emit_star(input, output, project_root, excluded_ids=set(results.skipped)), registry.save().
  - current lines: `preflight: fs_motion_and_ctf.py:316; ingest: 364-378 (empty re-check 365-370, adapter 371-374, emit_star 375-377, save 378)` — The post-run empty-registry re-check duplicates preflight's check but matches ts_alignment.py:305-309's pattern, so it is majority style, not a divergence. excluded_ids uses results.skipped rather than apply_exclusions' discarded return — equivalent since exclusions are the only .skip writers for this job.
- **HOLDS** — template_match_pytom registry stamping path: preflight_registry only (on tomo names, 1:1 with TS ids per docstring); NO post-run ingest adapter and NO registry writes — aggregation is a verbatim copy of the input tomograms star.
  - current lines: `preflight: template_match_pytom.py:311; aggregation: 422-424; no IngestAdapter import/use anywhere in the file` — Matches its pick-stage siblings — extract_candidates_pytom and subtomo_extraction also use preflight_registry without ingest adapters (registry has no TM-score entity yet). Deliberate scope, but worth a roadmap note since fs/ts_alignment/ts_ctf/ts_reconstruct all stamp the registry.
- **HOLDS** — Hardcoded tool-name strings present (noted per census instructions, dim 7 owned elsewhere): fs_motion_and_ctf hardcodes container tool_name="warptools" and command names "WarpTools create_settings" / "WarpTools fs_motion_and_ctf"; template_match_pytom hardcodes the binary name "pytom_match_template.py" but sources its container tool name from params.get_tool_name().
  - current lines: `fs_motion_and_ctf.py:444 (tool_name), 118, 137 (command names); template_match_pytom.py:209 (binary), 526 (params-driven tool name)` — FsMotionCtfParams.get_tool_name() returns "warptools" (services/jobs/fs_motion_ctf.py:97-98), so aligning fs to params.get_tool_name() is behavior-identical.
- **HOLDS** — Both drivers match the canonical lifecycle on dims 1, 2, 8(mechanics), 9(markers), and 11: mode dispatch purely via SLURM_ARRAY_TASK_ID env (no --mode argparse anywhere in drivers/); supervisor bootstrap except -> touch RELION_JOB_EXIT_FAILURE at Path.cwd() + traceback + exit(1); task bootstrap except -> traceback + exit(1) with no marker and no .fail (item unknown); status via write_status_atomic to job_dir/.task_status with nested-try fail-write in the outer except; RELION markers supervisor-only (base strips them from array scripts); error style = raise inside try, outer except prints + marker + sys.exit(1).
  - current lines: `fs_motion_and_ctf.py:263-271, 280-289, 386-390, 399-406, 408, 453, 457-465; template_match_pytom.py:249-258, 267-276, 430-434, 443-450, 452, 535, 539-547; base marker-strip array_job_base.py:527-541; STATUS_DIR_NAME='.task_status' services/array_tasks.py:17` — No divergence entries emitted for these dimensions — both drivers are canonical there. Both also call apply_exclusions immediately before submit_array_job (fs:323, tm:391) per the base docstring's placement rule.
- **HOLDS** — denoise_predict skips preflight_registry and stamps the registry by hand (:283-327)
  - current lines: `/users/artem.kushner/dev/crboost_server/drivers/denoise_predict.py:282-326 (stamp_denoise_registry), :38-48 (import block lacks preflight_registry), :433-442 (call site)` — Both halves confirmed. preflight_registry is neither imported nor called anywhere in the file — the supervisor goes apply_exclusions (:398) straight to submit_array_job (:399). stamp_denoise_registry hand-writes the registry in the supervisor only (get_registry_for -> attach_tomogram_output(DenoisePredictTomogramOutput) -> registry.save()), best-effort: empty registry skipped (:301-303), unknown TS swallowed via `except KeyError: pass` (:321-322), any failure only warns (:325-326). Roadmap line range has drifted by one: the function now spans :282-326, not :283-327.
- **HOLDS** — extract_candidates handles per-species instance_ids in-driver (report how, if visible)
  - current lines: `/users/artem.kushner/dev/crboost_server/drivers/extract_candidates_pytom.py:179-181, :192, :261-270, :346-355` — The driver is fully species-agnostic: instance_id is taken as an opaque string from get_driver_context's context dict (:192, `instance_id = context["instance_id"]`) — there is NO `__` suffix parsing, no species_id logic, and no per-species branching anywhere in the file. Species specificity arrives entirely through resolved paths (paths["input_tm_job"] :194 points at the species-specific TM job, resolved upstream by PathResolutionService in driver_base). The instance_id is passed through verbatim to submit_array_job (:264 — array tasks re-invoke the driver with the same --instance_id via build_array_sbatch_script, array_job_base.py:500-505) and to generate_candidate_previews (:353) for species attribution downstream.
- **HOLDS** — subtomo_extraction reads manifest["items"] vs everyone's manifest["ts_names"], and sys.exit(1)s where others raise (subtomo_extraction.py:382-387)
  - current lines: `subtomo_extraction.py:375-384 (get("items") at 381; out-of-range sys.exit(1) at 382-384; missing-manifest sys.exit(1) at 377-379)` — Fully true; cited lines drifted by a few (382-387 → 381-384 core). All 7 other array drivers read manifest["ts_names"] and raise IndexError inside their status-writing try (ts_alignment.py:357-359, ts_reconstruct.py:223-225, ts_ctf.py:338-340, fs_motion_and_ctf.py:412-416, template_match_pytom.py:456-458, extract_candidates_pytom.py:404-406, denoise_predict.py:475-478). Extra nuance: subtomo's exits happen BEFORE its .fail-writing try (:390), so a manifest problem records no status marker (item counts as 'missing'), whereas siblings record a .fail under a fallback label. write_manifest (array_job_base.py:59) writes both keys, so aligning the key is free.
- **HOLDS** — subtomo_extraction.py:202-230 reimplements apply_exclusions (same reason string, hand-copied)
  - current lines: `subtomo_extraction.py:198-229 (load_excluded_ts at 201, filter 202-203, skip-writes 228-229; reason string at 229)` — Reason string "excluded from processing" at :229 is byte-identical to array_job_base.py:306. But it is a PARTIAL copy, not verbatim: the base's apply_exclusions first unlinks stale .ok/.fail for each excluded TS (array_job_base.py:301-305, guarding the ok+skip==len(ts_names) tally); the subtomo copy omits that, so a previously-succeeded TS excluded later carries .ok + .skip, over-counts the tally, flips all_succeeded to False, and the strict check at :274 fails the whole job. The copy also does extra work base doesn't (clears all *.skip at :221-223 to refresh empty-TS markers; filters ts_with_picks at :203) which must survive an ALIGN.
- **DRIFTED** — ts_alignment.py:54-57 enumerates TS via tomostar_dir.glob("*.tomostar") against array_job_base.py:176-183's own written warning (input-star is canonical)
  - current lines: `ts_alignment.py:53-56 (glob at 55, call site 234; snapshot copy 227-229); warning now at array_job_base.py:225-239 (the cited 176-183 is now get_previously_done, :174-183)` — Substance true, both cited ranges moved (driver by 1 line; base ref points at a different function). Functional analysis — glob picks up but the input star would not: (a) stale .tomostar files the producer's current star no longer lists (TS fully dropped by a tiltFilter re-run, or leftovers in a reused producer dir) get aligned and re-enter downstream scope via the aggregation star; (b) the glob runs on job_dir/tomostar, copied from upstream ONCE behind `if not local_tomostar_dir.exists()` (:228-229), so supervisor re-runs enumerate the stale snapshot — TS deleted upstream still processed. Star would list but glob misses: (a) a star-listed TS whose .tomostar file is missing (partial producer write) is silently omitted and the job green-ticks without it, where star-driven dispatch would fail loud at staging (FileNotFoundError, ts_alignment.py:83-84); (b) on re-run, TS newly added upstream never enter the snapshot — silently never aligned. Unaffected: user exclusions (apply_exclusions :247 runs on either list). Caveat: the base warning's stated rationale (WarpTools writes {ts}.xml even for failed TS) targets xml-globbing consumers DOWNSTREAM of alignment; ts_alignment globs tomostars upstream of any XML, so the applicable hazard is stale-file/stale-snapshot, not xml resurrection. An input_star does exist for this job (required at :298-301 for aggregation) but whether the tsImport/tiltFilter producer star's block layout matches read_tilt_series_names_from_input_star's `global`-block lookup needs maintainer confirmation. Decision proposed in divergences: ASK.
- **HOLDS** — ts_alignment's tolerant results tally (:266-289) vs everyone's strict one
  - current lines: `ts_alignment.py:265-288 (collect at 265; fail-only-if-nothing-aligned at 278-281; warn-and-continue at 282-288)` — One line of drift. Job fails only when results.ok is empty; failed+missing TS are warned about and dropped from aggregation (aligned_ts = results.ok at :276 → adapter.ingest at :313), then the job exits SUCCESS. All 7 other array drivers are strict: `if not results.all_succeeded:` → RELION_JOB_EXIT_FAILURE + sys.exit(1) (subtomo_extraction.py:274-277, ts_reconstruct.py:167, ts_ctf.py:285, fs_motion_and_ctf.py:350, template_match_pytom.py:417, extract_candidates_pytom.py:285, denoise_predict.py:423). Deliberate and documented in-code (:272-275, 'One bad TS must NOT abort the whole job'); dropped-TS downstream consequences are owned by docs/roadmaps/05-per-ts-top-up.md — proposed KEEP.
- **HOLDS** — tilt_filter.py:134-157 [stamps the registry by hand]
  - current lines: `134-159` — Accurate. try opens at 134, get_registry_for at 137, set_frame_filtered loop 143-153 (inner 'except KeyError: pass' 152-153), registry.save() 154, stamped-count print 155; the enclosing best-effort 'except Exception' → WARNING sits at 158-159, so the full hand-stamp block is 134-159 (roadmap's 134-157 misses only the two-line swallow). Deliberate per the Stage-2 comment at 128-133 ('must never fail a filter that already trimmed the tomostar'; Stage 3 migrates consumers).
- **HOLDS** — denoise_train is legitimately a global reduction (no array)
  - current lines: `33-35, 244-245, 255-262` — Confirmed: zero array/supervisor machinery — no array_job_base import (imports are only driver_base + job_models/models_base + container_service, lines 7-8 and 24-27), no SLURM_ARRAY_TASK_ID read, no --mode/supervisor-vs-task split, no manifest, no .task_status/.ok/.fail/.skip anywhere. Single main() using plain RELION_JOB_EXIT_SUCCESS/FAILURE markers (244-245, 409, 417). Explicitly documented: 'Training is a single global reduction for both methods (one model from many tomograms)' (255-257) and the IsoNet path docstring 'single global job (one model from many tomograms)' (34).
- **HOLDS** — miss_align.py does not participate in the array lifecycle (report its status protocol)
  - current lines: `4, 191-192, 202-204, 351-361` — No array lifecycle participation at all: no array_job_base import, no manifest, no .task_status dir, no .ok/.fail/.skip writes, no SLURM_ARRAY_TASK_ID, no supervisor/task dispatch — docstring line 4 declares 'Single (non-array) multi-GPU-node job.' Status protocol = plain-driver RELION markers only: RELION_JOB_EXIT_SUCCESS/FAILURE Paths defined at 191-192, success touch + '--- SLURM JOB END (Exit Code: 0) ---' + sys.exit(0) at 351-353, failure touch + END(1) + sys.exit(1) at 355-361; upstream inputs validated fail-loud via require_producer_input (202-204); bootstrap failures print '[DRIVER] FATAL BOOTSTRAP ERROR' + sys.exit(1) with no marker (180-186).
- **HOLDS** — extract_pick_list.py hand-rolls argparse and bypasses the bootstrap entirely (does not use get_driver_context)
  - current lines: `drivers/extract_pick_list.py:109-122 (hand-rolled argparse), :31 (only driver_base import), get_driver_context absent from the file` — Confirmed: main() builds its own ArgumentParser with --candidate-optset/--list-star/--tomo/--out-dir/--project-root + extraction flags; no --instance_id/--project_path, no ProjectState load, no job model. The bypass is of the bootstrap specifically, not of all driver infra — line 31 is 'from drivers.driver_base import run_command', and it also reuses get_container_service (line 32) and services/visualization/list_extraction helpers (line 33). Its identity/status contract is CLI args + <out-dir>/result.json + exit code (124-135), with RELION markers delegated to the qsub wrapper (docstring 12-13). A port onto get_driver_context is blocked on an identity/params decision: no instance exists in project_params.json for this one-off (see ASK divergence).
- **HOLDS** — subtomo_merge.py is a LIBRARY living in drivers/ and mirrored in services/visualization/list_extraction.py:38-90 - its docstrings already apologize for the copy
  - current lines: `drivers/subtomo_merge.py:1-587 (no __main__, no argparse, no exits — library confirmed); mirror now spans services/visualization/list_extraction.py:37-89 (roadmap's 38-90 is off by one); apologies at list_extraction.py:39-40 and :74-76` — Apology quotes (verbatim): '_parse_optimisation_set' docstring — 'Mirrors drivers/subtomo_merge.py so we read the candidate optset the same way the driver does.' (39-40); '_write_optimisation_set' docstring — '(mirrors drivers/subtomo_merge.write_optimisation_set — kept here to avoid a services→drivers import)' (74-76). Extent of the mirror: 2 functions, ~51 lines in list_extraction (parse 37-70 = 34 lines; write 73-89 = 17 lines) duplicating ~62 lines of subtomo_merge (:52-99 parse incl. the starfile-first/except-pass/manual-fallback structure; :189-202 write, byte-identical output format '# version 50001 / data_ / two _rlnTomo*File keys with resolved absolute paths'). The copy has already drifted twice: list_extraction's _resolve does NOT .resolve() relative paths (line 45) while subtomo_merge's _resolve_path does (line 39), and list_extraction's line-by-line fallback does not skip comment/data_/loop_ lines (subtomo_merge.py:83-84 does). Library consumers confirming the import-direction dodge: drivers/subtomo_extraction.py:63, drivers/extract_candidates_pytom.py:46, and ui/aggregation_merge_card.py:728 (function-local UI->drivers import).

## Command-builder census (stage 2 input)

Totals: 18 files scanned, 18 distinct builders, 6 shlex users, 12 hardcoded tool-name strings.

| file | function | lines | quoting | tool name | hardcoded |
|---|---|---|---|---|---|
| drivers/class3d.py | main | 49-128 (cmd_parts 49-116, join 117, wrap 125-127) | none | hardcoded | relion_refine (binary, L50); tool_name="relion" (L126) |
| drivers/denoise_predict.py | build_cryocare_predict_command | 117-145 (wrapped in run_task_mode 528-533) | fstring | get_tool_name | cryoCARE_predict.py (binary, in f-string L145); container tool_name = params.get_tool_name() (L531) |
| drivers/denoise_predict.py | run_isonet_predict_task | 170-236 (isonet() closure 206-210; commands 212-216, 219-221, 223-225) | shlex | hardcoded | isonet.py (binary, x3); tool_name="isonet" (L208) |
| drivers/denoise_train.py | run_isonet_train | 33-137 (isonet() closure 49-53; prepare_star 100-104, deconv 110, make_mask 114, refine 124-127, tar 137) | mixed | hardcoded | isonet.py (binary, x4) + native tar (L137); tool_name="isonet" (L51) |
| drivers/denoise_train.py | main (cryoCARE branch) | 337-404 (extract_cmd 367, wrap 369-372; train_cmd 383, wrap 385-388; tar_cmd 403-404) | fstring | hardcoded | cryoCARE_extract_train_data.py + cryoCARE_train.py (binaries); tool_name="cryocare" (L370 and L386) |
| drivers/extract_candidates_pytom.py | build_extract_base_cmd (+ run_task_mode completion) | 104-122, completed/joined/wrapped 424-432 | none | get_tool_name | pytom_extract_candidates.py (binary, L106); container tool_name = params.get_tool_name() (L430) |
| drivers/extract_pick_list.py | _build_subtomo_cmd | 36-61 (wrapped 93-96) | none | hardcoded | relion_tomo_subtomo (binary, L41); tool_name="relion" (L94) |
| drivers/fs_motion_and_ctf.py | build_warp_commands (+ post-build patch in run_task_mode) | 104-184; extension patch 437-438; wrap 442-445 | mixed | hardcoded | WarpTools create_settings (L118) + WarpTools fs_motion_and_ctf (L138); tool_name="warptools" (L444) |
| drivers/miss_align.py | main (stamp block) | 245-259 (stamp_cmd 253-255, wrap 256-258) | fstring | hardcoded | python stamp_dims.py (in-container python running the STAMP_SCRIPT file written at L251-252); tool_name="miss_alignment" (L257) |
| drivers/miss_align.py | main (train block) | 269-331 (train_parts 313-323, join 326, wrap 328-330) | fstring | hardcoded | miss-alignment train (binary+subcommand, L316); tool_name="miss_alignment" (L329) |
| drivers/reconstruct_particle.py | main | 49-113 (cmd_parts 49-100, join 102, wrap 109-111) | none | hardcoded | relion_tomo_reconstruct_particle (binary, L50); tool_name="relion" (L110) |
| drivers/subtomo_extraction.py | run_task_mode | 412-449 (cmd_parts 412-432, join 434, wrap 446-448) | none | hardcoded | relion_tomo_subtomo (binary, L413); tool_name="relion" (L447) |
| drivers/template_match_pytom.py | build_pytom_base_cmd (+ run_task_mode completion) | 188-241; per-tomo completion/join/wrap 505-528 | none | get_tool_name | pytom_match_template.py (binary, L209); container tool_name = params.get_tool_name() (L526) |
| drivers/ts_alignment.py | build_alignment_command | 95-132 (wrap 386-388) | none | get_tool_name | WarpTools ts_aretomo (L99) / WarpTools ts_etomo_patches (L119); container tool_name = params.get_tool_name() (L387) |
| drivers/ts_ctf.py | run_defocus_hand_globally | 54-90 (base cmds 61-69, auto script 71-84, wrap 86-90) | mixed | get_tool_name | WarpTools ts_defocus_hand (x3 variants: --check/--set_flip/--set_noflip, L64-68); container tool_name = params.get_tool_name() (L88) |
| drivers/ts_ctf.py | build_ctf_command | 137-156 (wrap 357-359, run via run_command_with_retries 360) | fstring | get_tool_name | WarpTools ts_ctf (L140); container tool_name = params.get_tool_name() (L358) |
| drivers/ts_import.py | build_ts_import_commands | 25-94 (wrap 123-126) | mixed | hardcoded | WarpTools ts_import (L46) + WarpTools create_settings (L69); tool_name="warptools" (L125) |
| drivers/ts_reconstruct.py | build_reconstruct_command | 72-85 (wrap 253-255, run via run_command_with_retries 257) | shlex | get_tool_name | WarpTools ts_reconstruct (L76); container tool_name = params.get_tool_name() (L254) |
| drivers/array_job_base.py | build_array_sbatch_script (SLURM plumbing, not a tool command) | 473-546 (driver_cmd 500-505, placeholder replace 510-525, marker-block strip 532-541) | fstring | n/a | venv python3 driver re-invocation embedded in config/qsub.sh template; sbatch script generation |
| drivers/array_job_base.py | submit_array_sbatch (SLURM plumbing) | 549-562 | list-argv | n/a | sbatch |
| drivers/array_job_base.py | wait_for_array_completion (SLURM plumbing) | 565-578 | list-argv | n/a | squeue |
| drivers/array_job_base.py | cancel_previous_array (SLURM plumbing) | 673-703 | list-argv | n/a | squeue + scancel |
| drivers/array_job_base.py | install_cancel_handler (SLURM plumbing) | 706-718 | list-argv | n/a | scancel |

### Injection-hazard notes

- `drivers/class3d.py` main: List assembled then " ".join with zero escaping. str(Path) unquoted for input_optimisation, input_reference, output_root; user-supplied params.solvent_mask_path interpolated raw (L106-111) — any space in a path breaks tokenization. symmetry emitted via .value-or-str(). ~20 conditional flags.
- `drivers/denoise_predict.py` build_cryocare_predict_command: Weird case: bare env-prefix embedded in the command string: 'TF_FORCE_GPU_ALLOW_GROWTH=true TF_GPU_ALLOCATOR=cuda_malloc_async cryoCARE_predict.py --conf predict_{idx}.json'. All real params (model tar, even/odd paths, tiles) go through a JSON config file written at L143-144 — CLI carries only a driver-generated relative filename, so cwd is semantic.
- `drivers/denoise_predict.py` run_isonet_predict_task: Three sequential f-string commands (prepare_star / optional deconv / predict) each wrapped+run via a shared closure. All path args shlex.quote()d; --pixel_size auto and --input_column {input_col} are unquoted safe constants. input_col switches rlnTomoName->rlnDeconvTomoName when params.isonet_deconv.
- `drivers/denoise_train.py` run_isonet_train: shlex.quote on staged dirs only; --star_name {prep} uses unquoted relative literal 'isonet_prep.star'; cs/voltage/ac are floats read from the STAR first row interpolated via str() (L94-96,103) — float repr must be byte-preserved; refine method from enum .value. Final 'tar -czf denoising_model.tar.gz isonet_maps' is a raw constant run via run_command WITHOUT container wrapping (native tar). shlex imported function-locally at L44.
- `drivers/denoise_train.py` main (cryoCARE branch): Both commands are f-strings interpolating only config_json_path.name ('train_config.json'); all real params flow through the JSON config (L342-364). tar_cmd 'tar -czf ../denoising_model.tar.gz denoising_model' is a raw constant, NOT container-wrapped, run with cwd=job_dir/'train_data' — relies on ../ relative output plus cwd.
- `drivers/extract_candidates_pytom.py` build_extract_base_cmd (+ run_task_mode completion): List built in one function, '-j <job.json>' appended and " ".join'd in another (L425-426), zero quoting. Weird cases: --particle-diameter value is str(int(params.particle_diameter_ang / 2.0 / apix) * apix) — Python float repr in the command (L108); user string params.score_filter_value split on ':' into two raw args --tophat-connectivity/--tophat-bins (L119-121). job_json path unquoted.
- `drivers/extract_pick_list.py` _build_subtomo_cmd: List->join, no quoting. --o value is str(out_run) + "/" trailing-slash convention (L43). Paths come from argparse args (backend-submitted one-off job, not pipeline paths). Mirrors subtomo_extraction's per-TS command by design — the two must stay in lockstep.
- `drivers/fs_motion_and_ctf.py` build_warp_commands (+ post-build patch in run_task_mode): Two commands fused into one compound shell line: f"test -f warp_frameseries.settings || ({create_cmd}) && {run_cmd}" (L184) — idempotency guard with subshell. shlex.quote ONLY on gain_path (L108); params.gain_operations is a raw user string joined as one token (L109,135); m_grid/c_grid raw user strings. Literal nested quotes to defeat glob expansion: "'*.eer'" (L124), then patched post-build via warp_command.replace("'*.eer'", f"'{ext}'") (L438). Semantic sign-prefix f"-{params.eer_ngroups}" (L130) reinterprets the value as EER fractions. round(float(voltage)) (L162).
- `drivers/miss_align.py` main (stamp block): f-string interpolates six numeric dims, all validated positive by read_settings_dims before use (never invented). Script and filename are relative to job_dir — cwd-dependent. The helper script itself is a module-level Python source string (L46-63) written to disk, a pattern a unified builder must accommodate (auxiliary file + command referencing it).
- `drivers/miss_align.py` main (train block): Weird case: first list element is an 'env HOME={jobtmp} MPLCONFIGDIR=... USER={username} LOGNAME={username} TORCHINDUCTOR_CACHE_DIR=... OMP_NUM_THREADS=1 MKL_NUM_THREADS=1' prefix (L314-315) — env vars set inline in the command string (getpwuid/cleanenv workaround), job-dir path and username interpolated unquoted. Unlike other drivers, each element fuses flag+value ('--pool-size {n}'), joined with " ". Relative 'config.yaml' is cwd-semantic.
- `drivers/reconstruct_particle.py` main: List->join, no quoting; --o is str(job_dir) + "/" (L52). THE raw-passthrough case: params.other_args appended VERBATIM, explicitly 'not shell-escaped' per comment (L97-100) — a unified builder needs a deliberate escape hatch for raw fragments. Enum-or-str for symmetry (L54) and theme (L77-81); helical block of conditional numeric flags.
- `drivers/subtomo_extraction.py` run_task_mode: List->join, no quoting. --o = str(out_dir) + "/" (L415); staged optset path unquoted; paths embed ts_name (a space-containing TS name would break every joined command in this family). int(params.binning) truncation (L421). Near-duplicate of extract_pick_list._build_subtomo_cmd — consolidation target.
- `drivers/template_match_pytom.py` build_pytom_base_cmd (+ run_task_mode completion): List->join, zero quoting on template/mask/tomo/angle-file paths. Weird cases: multi-value flags splatted after one flag — '-g', *gpu_ids (L218, from CUDA_VISIBLE_DEVICES.split(',') L502) and '-s', *get_gpu_split(...) (L230); user string params.bandpass_filter split on ':' into --low-pass/--high-pass raw args (L237-239); --angular-search is polymorphic (file path for D/T/O/I symmetry via generated angle list, float for Cn/C1, plus --z-axis-rotational-symmetry sym[1:] slice L227). Microscope floats via str().
- `drivers/ts_alignment.py` build_alignment_command: Method-dispatched builder (AlignmentMethod.ARETOMO vs IMOD) with fused binary+subcommand as first list element. All paths RELATIVE ('warp_tiltseries.settings') — cwd=stage_root is part of the command's meaning. Composite value f"{patch_x}x{patch_y}" (L112); unit conversions str(int(nm*10)) (L107,127); min(axis_batch,1) clamp (L115). Fallback branch returns a literal shell error shim: "echo 'ERROR: ...'; exit 1;" (L130) — a command that is a script, not a tool invocation.
- `drivers/ts_ctf.py` run_defocus_hand_globally: THE weirdest case: defocus_hand=='auto' builds a mini shell SCRIPT — hand_output=$({check_cmd} 2>&1); echo "$hand_output"; if echo "$hand_output" | grep -q "should be set to 'flip'"; then {set_flip}; else {set_noflip}; fi (L72-79) — command substitution capturing tool stdout, pipe to grep on a magic tool-output phrase, branch selecting the follow-up command, with an embedded single quote in the grep pattern that survives only because the container wrapper shlex.quotes the whole inner string. Non-auto modes: ' && '.join of two commands (L82,84). settings/output paths shlex.quote()d (L61-62).
- `drivers/ts_ctf.py` build_ctf_command: Single multi-line f-string; relative staged paths unquoted (cwd=stage_root); round(params.voltage) (L149); conditional flag via string append cmd += " --fit_phase" (L154-155). Executed through run_command_with_retries (3 attempts) — retry semantics attach to the wrapped string.
- `drivers/ts_import.py` build_ts_import_commands: shlex.quote on mdoc_dir, mdoc_pattern, os.path.relpath(frameseries), gain_path (L33-41); gain_operations raw user string (L42,87). Fused flag+quoted-glob in ONE list element: "--extension '*.tomostar'" (L72) with literal nested quotes. Compound idempotency chain joined ' && ': f"test -d tomostar && ls tomostar/*.tomostar >/dev/null 2>&1 || ({import_cmd})" && f"test -f warp_tiltseries.settings || ({settings_cmd})" (L89-94) — includes an in-shell glob + redirect probe; relies on ||/&& precedence. relpath makes the frameseries arg cwd-relative on purpose (tomostar _wrpMovieName relativity).
- `drivers/ts_reconstruct.py` build_reconstruct_command: f-string with shlex.quote on all three path args (settings/input_processing/output_processing, L77-79); numeric params (rescale_angpixs, halfmap_frames, deconv, perdevice) interpolated bare; fixed --dont_invert. The cleanest builder in the fleet — closest to the target pattern.
- `drivers/array_job_base.py` build_array_sbatch_script (SLURM plumbing, not a tool command): driver_cmd = f"export PYTHONPATH={server_dir}:$PYTHONPATH; {python_exe} {driver_script} --instance_id {instance_id} --project_path {project_path}" — instance_id and project_path interpolated UNQUOTED into a bash script; spaces in a project path break every array job. XXXextraNXXX raw string replacement of SlurmConfig fields incl. constraint.strip("'\"") (L494); '#SBATCH --array=' injected by string surgery anchored on '#SBATCH --output=XXXoutfileXXX' (L491-492); RELION marker block removed by exact-multiline-string replace (L532-541) — silently no-ops if qsub.sh drifts.
- `drivers/array_job_base.py` submit_array_sbatch (SLURM plumbing): True subprocess argv list — no shell, safe. Runs with env stripped of SLURM_/SBATCH_ vars (L555); job id parsed as out.split()[-1] from 'Submitted batch job N' (L562).
- `drivers/array_job_base.py` wait_for_array_completion (SLURM plumbing): argv list ['squeue','-j',id,'--noheader','-h'] — safe; poll loop keying off returncode/empty stdout.
- `drivers/array_job_base.py` cancel_previous_array (SLURM plumbing): prev_id read from manifest JSON and passed as its own argv element — safe. squeue probe (L693) then scancel (L699).
- `drivers/array_job_base.py` install_cancel_handler (SLURM plumbing): argv list inside a signal handler; check=False; safe.

### What ToolCommand must support (byte-identical reproduction)

CORRECTED TOTALS vs roadmap claims (12 builders / 6 shlex / 11 hardcoded names): 18 hand-rolled TOOL-command builder sites (roadmap said 12), spread over 14 of the 18 driver files; PLUS 5 SLURM-plumbing sites in array_job_base.py (sbatch-script generation + 4 safe subprocess-argv sites), listed in builders[] but excluded from distinct_builders since the roadmap's number is about tool commands — 23 entries total. shlex.quote users: exactly 6/18 files (denoise_predict, denoise_train [function-local import at L44], fs_motion_and_ctf, ts_ctf, ts_import, ts_reconstruct) — roadmap's 6/18 CONFIRMED. Hardcoded tool_name= literals passed to wrap_command_for_tool: 12 call sites (roadmap said 11): relion x4 (class3d:126, extract_pick_list:94, reconstruct_particle:110, subtomo_extraction:447), warptools x2 (fs_motion_and_ctf:444, ts_import:125), isonet x2 (denoise_predict:208, denoise_train:51), cryocare x2 (denoise_train:370,386), miss_alignment x2 (miss_align:257,329); versus 7 params.get_tool_name() sites in 6 files (denoise_predict:531, extract_candidates:430, template_match:526, ts_alignment:387, ts_ctf:88+358, ts_reconstruct:254). Separately, 13 distinct tool BINARY names are hardcoded inside command strings (relion_refine, relion_tomo_reconstruct_particle, relion_tomo_subtomo, cryoCARE_predict.py, cryoCARE_extract_train_data.py, cryoCARE_train.py, isonet.py, pytom_extract_candidates.py, pytom_match_template.py, WarpTools [8 subcommands], miss-alignment, tar, python). Files with ZERO builders: tilt_filter.py (pure in-process DL), subtomo_merge.py (pure STAR merge), driver_base.py (executor only).

WRAPPER CONTRACT (container_service.py, read fully): wrap_command_for_tool (L162-179) is string-in/string-out — no argv path exists anywhere. Container mode shlex.quotes the ENTIRE inner command as the single bash -c argument (L232/235), so it does NOT re-join an argv (it never receives one); inner shell semantics survive and are re-parsed by the in-container bash, which several builders depend on, and driver-level shlex.quote nests correctly under it. Binary exec_mode returns the string UNCHANGED (L170-176), so every command must be valid both wrapped and native. tool_name is load-bearing beyond config lookup: substring test '\"relion\" in tool_name.lower()' gates a /usr/bin bind (L203), RELION_QSUB_EXTRA* exports prepended INSIDE the quoted inner command (L220-231), and DISPLAY/XAUTHORITY unsets (L262). The OUTER apptainer string is itself \" \".join'd with unquoted bind paths and sif path (L246) and executed via Popen(shell=True) (driver_base.py L234-243) — a space in any bind/project path already breaks execution today. additional_binds that don't exist at wrap time are silently dropped (L196-199).

WHAT A UNIFIED ToolCommand BUILDER MUST SUPPORT FOR BYTE-IDENTICAL OUTPUT: (1) flat-string emission, not argv-only — the wrapper and run_command(shell=True) consume strings, and compound forms are load-bearing; (2) compound shell inside one command: idempotency guards f\"test -f X || ({A}) && {B}\" (fs_motion:184) and the double-guard chain with in-shell glob probe \"ls tomostar/*.tomostar >/dev/null 2>&1\" (ts_import:89-94); ' && '.join sequencing (ts_ctf:82,84); the auto defocus-hand mini-script with $() capture, echo, pipe-to-grep on a magic output phrase containing an embedded single quote, and if/else command selection (ts_ctf:72-79); the error shim \"echo 'ERROR...'; exit 1;\" (ts_alignment:130); (3) env prefixes in-band: bare 'K=V K=V tool' (denoise_predict:145) and 'env HOME=.. USER=.. tool' (miss_align:314-315); (4) per-site quoting policy preservation — sites range from shlex-everything (ts_reconstruct) through shlex-some (fs_motion: only gain_path; ts_import: paths but not gain_operations) to none (all relion + pytom builders); normalizing quoting changes bytes; (5) literal nested quotes as glob-defense tokens: \"'*.eer'\" / \"--extension '*.tomostar'\" including POST-BUILD string .replace() patching of that token (fs_motion:438); (6) formatting quirks: str() of computed Python floats (extract_candidates:108 diameter arithmetic; denoise_train cs/voltage/ac from STAR), round() (fs_motion:162, ts_ctf:149), int() truncation, sign-prefix f\"-{eer_ngroups}\" with changed semantics (fs_motion:130), composite \"{x}x{y}\" values (ts_alignment:112), trailing-slash output dirs str(dir)+\"/\" (reconstruct_particle:52, subtomo_extraction:415, extract_pick_list:43), multi-value flags '-g a b c' / '-s 2 2 1' via list splat (template_match:218,230), user strings split on ':' into flag pairs (extract_candidates:119-121, template_match:237-239), fused flag+value tokens (ts_import:72, all miss_align train elements), fused binary+subcommand names ('WarpTools ts_aretomo', 'miss-alignment train'); (7) a deliberate raw-passthrough escape hatch: params.other_args appended verbatim, documented as not-shell-escaped (reconstruct_particle:98-100); (8) config-file indirection as a first-class alternative to CLI args (cryoCARE --conf JSON, pytom -j job.json) plus auxiliary-file patterns (miss_align STAMP_SCRIPT source written to job_dir then invoked); (9) cwd-relativity as semantics: staged commands use bare relative names ('warp_frameseries.settings', 'config.yaml', relpath'd frameseries dir) where cwd choice is part of correctness, alongside shlex-quoted absolute paths elsewhere; (10) split builder/completion sites: base command built in one function, per-item args appended and joined in the task function (extract_candidates 104-122 + 424-432; template_match 188-241 + 505-528); (11) native un-wrapped commands (tar x3: denoise_train:137,403, run directly through run_command); (12) method-dispatched builders returning different tools from one function (ts_alignment ARETOMO/IMOD); (13) retry-wrapper interop (run_command_with_retries reuses the exact wrapped string: ts_ctf:360, ts_reconstruct:257). Injection posture to carry forward knowingly: the dominant hazard class is unquoted user/derived paths in the quoting=none relion/pytom builders and in build_array_sbatch_script's driver_cmd (array_job_base:500-505, unquoted --project_path in a generated bash script), compounded by the wrapper's own unquoted outer join (container_service:246); raw user strings gain_operations, m_grid/c_grid, score_filter_value, bandpass_filter enter commands unescaped by design and would need explicit policy in the unified builder.

## Status protocol — three implementations (stage 1 input)

### Driver writer + supervisor-side reader (task mode writes terminal markers; supervisor dispatches sparse arrays and tallies results). Re-exports protocol constants to 8 drivers: ts_ctf, fs_motion_and_ctf, denoise_predict, ts_alignment, ts_reconstruct, subtomo_extraction, extract_candidates_pytom, template_match_pytom. — `drivers/array_job_base.py:1-719 (protocol core: 44-47 constants imported FROM services.array_tasks; 50-81 manifest write/read/update; 89-163 status writes + superseded guard; 166-202 previously-done readers + clean; 242-257 collect_task_results; 265-312 exclusions->skip; 491-546 sbatch script wiring task_%a.out; 586-703 submit_array_job/cancel_previous_array)`

- **Reads:** .task_manifest.json via its own read_manifest (70-73: raises FileNotFoundError if missing, propagates JSON errors); manifest key array_job_id in is_superseded_task (106-113, compared to env SLURM_ARRAY_JOB_ID) and cancel_previous_array (684-689, swallow->None); .task_status/*.ok stems in get_previously_succeeded (166-171); *.ok union *.skip in get_previously_done (174-183); *.ok|*.fail|*.skip stems in collect_task_results (248-252). Task-mode drivers read manifest["ts_names"][SLURM_ARRAY_TASK_ID] (e.g. fs_motion_and_ctf.py:412, ts_reconstruct.py:223).
- **Writes:** .task_manifest.json NON-atomically (write_text, 65-66) with keys items + ts_names (duplicate lists), item_count, item_label='Tilt Series', optional ts_metadata + arbitrary extra (59-63); update_manifest non-atomic read-modify-write adds array_job_id after sbatch (76-81, called at 668). .task_status/{item}.ok|.fail via write_status_atomic (116-146): tmp '.{item}.{suffix}.tmp' + os.replace, empty content; ok-write unlinks stale .fail and .skip first (139-142); fail-write preserves an existing .ok; whole write refused when task's SLURM_ARRAY_JOB_ID is strictly older than manifest array_job_id (129-135, fails open). .task_status/{item}.skip via write_skip_status (149-163): tmp+os.replace, content = reason text, no superseded guard. apply_exclusions (296-306): unlinks .ok/.fail for user-excluded TS then writes .skip(reason='excluded from processing'). clean_status_dir (186-202): always deletes *.fail, deletes *.ok only when keep_ok=False, NEVER deletes *.skip. run_array.sh with injected #SBATCH --array and task_%a.out/err as SLURM stdout/stderr (491-546); RELION_JOB_EXIT_* markers stripped from array tasks (532-541) so only the supervisor writes them.
- **"Done" semantics:** Dispatch-level done = get_previously_done = stems(.ok) UNION stems(.skip); those indices are excluded from the sparse --array (610-624), so .skip is sticky-settled. Job-level (collect_task_results 242-257): ok/fail/skip lists are INDEPENDENT per-suffix stem globs; missing = ts_names minus the union; all_succeeded = (|ok|+|skip| == |ts_names|) AND zero .fail files AND zero missing. An item holding BOTH .ok and .fail appears in both lists and its .fail blocks all_succeeded -> at job level FAIL WINS over ok. .skip counts as success. Write-side invariants: success clears fail/skip; failure never clears ok (an ok present at fail time is treated as the trustworthy record from the current run, the failing writer presumed orphan); orphan writes additionally suppressed by the strictly-older array_job_id check, which fails open with no SLURM env or unreadable manifest.

### Control plane (PipelineRunner): job-level RUNNING/QUEUED refinement for the roster spinner, afterok reconciler, stop/cancel paths that scancel arrays via manifest and force-fail interrupted tasks. — `services/scheduling_and_orchestration/pipeline_runner.py:282-297 (sync_all_jobs Running refinement), 371-378 (_afterok_refine_running), 841-851 (retry-path contract comment), 984-1024 (_finalize_stopped_task_statuses), 1246-1292 (stop_and_cleanup manifest scancel + finalize call), 1409-1423 (cancel_job manifest scancel)`

- **Reads:** Existence of .task_manifest.json + glob('task_*.out') for job-level status (291-297, 376-378) - hardcoded literal filenames, not the shared constants; manifest 'items' with blanket except -> return 0 (990-999); .task_status stems union of *.ok|*.fail|*.skip (1003-1007); per-INDEX task_{idx}.out existence (1013); manifest 'array_job_id' for scancel, parsed inline with json.loads + swallow, in stop_and_cleanup (1254-1264) and cancel_job (1412-1423).
- **Writes:** Only .task_status/{name}.fail in _finalize_stopped_task_statuses (1009-1024): hand-rolled '.{name}.fail.tmp' + os.replace, empty content, tmp unlinked on error; bypasses write_status_atomic and its superseded guard; written only for manifest items with NO existing marker AND task_{idx}.out present; invoked from stop_and_cleanup after scancel for every RUNNING/SCHEDULED job (1281-1292).
- **"Done" semantics:** Job-level only, no per-item resolution: manifest present AND any task_*.out -> RUNNING, manifest present AND none -> QUEUED, no manifest -> supervisor SLURM state (297) or RUNNING (378). task_*.out files persist across submissions, so a re-dispatched-but-queued rerun reads RUNNING (known desync, roadmap section 5 phase 4). On stop: started-but-unsettled items (stale-index .out present, no marker) are forced .fail so the tracker flips Running->Failed; never-started items stay markerless (= pending/missing, picked up by the next sparse retry). Any existing marker wins - the forced .fail never overwrites .ok/.fail/.skip, so ok beats the forced fail here (consistent with write_status_atomic, inconsistent with nothing). Index-keyed .out check can fail items never dispatched in the CURRENT submission (roadmap section 9, accepted as low-harm).

### UI read side: services/array_tasks.py is the single resolver; ui/components/task_utils.py is an 18-line re-export shim (roadmap-01 stage 3a, 'delete after one release'). Downstream: ui/components/array_task_tracker.py (per-TS chip tab, 5 s poll), ui/pipeline_builder/pipeline_roster.py (_get_array_progress 101-127, _get_array_ts_statuses 130-153), services/dashboard_data.py (17, 566-567), ui/aggregation_merge_card.py (name helpers only). — `ui/components/task_utils.py 1-18):1-155 (constants 16-17: MANIFEST_FILENAME='.task_manifest.json', STATUS_DIR_NAME='.task_status'; read_manifest 94-102; scan_statuses 105-140; resolve_job_dir 143-155; read_tail 76-87; TS display-name helpers 20-73)`

- **Reads:** read_manifest: returns None on missing OR unparseable (blanket except); scan_statuses: single iterdir of .task_status bucketing stems by suffix .ok/.fail/.skip (119-126), then per manifest item checks task_{idx}.out existence by INDEX for running-vs-pending (136-139); read_tail reads task_{idx}.out/.err content for the tracker's log expander (array_task_tracker.py:254-255). resolve_job_dir: job_model.paths['job_dir'] falling back to project_path/relion_job_name.
- **Writes:** None - pure reader (module docstring lines 1-8 declares it owns the protocol NAMES while the writer side lives in drivers/array_job_base.py, which re-imports them so the ends cannot drift).
- **"Done" semantics:** Exactly one status per item with precedence ok > fail > skip > running (task_{idx}.out exists) > pending (129-139): an item with both .ok and .fail resolves 'ok' - orphan-write forgiveness, the OPPOSITE job-level outcome from collect_task_results. Downstream tallies then diverge: array_task_tracker._update_progress counts ok+fail+skip as done (bar reaches 100%, skip settled); roster _get_array_progress counts settled = ok+fail only (skip is neither settled nor failed -> muted TS reads 5/6 forever, roadmap section 9) and computes n_running = count(task_*.out) - n_settled by glob not index; running/pending inference is stale across submissions since .out files are never removed.

### Drift between the three

1. Both-markers precedence contradiction: scan_statuses resolves an item with .ok AND .fail as 'ok' (services/array_tasks.py:129-133); collect_task_results globs per-suffix so the same item lands in BOTH lists and any .fail blocks all_succeeded (drivers/array_job_base.py:248-254) - the UI can show every item green while the supervisor declares the job failed and RELION_JOB_EXIT_FAILURE is written.
2. read_manifest exists in 4 variants with 3 error contracts: raising (array_job_base.py:70-73), None-on-any-error (array_tasks.py:94-102), inline json.loads swallow->return 0 (pipeline_runner.py:990-996), inline json.loads swallow->pass twice (pipeline_runner.py:1256-1264 and 1414-1423). pipeline_runner also hardcodes the literal filenames '.task_manifest.json'/'.task_status' instead of the shared constants (990, 1001, 1254, 1412, 291, 376).
3. _finalize_stopped_task_statuses hand-rolls the tmp+os.replace atomic write (pipeline_runner.py:1015-1023) instead of calling write_status_atomic - duplicating the '.{name}.fail.tmp' naming convention and silently bypassing the is_superseded_task guard (only safe because it runs post-scancel by convention; nothing enforces it). Roadmap section 3 flags this as the 5th uncoordinated writer.
4. Three incompatible 'settled/progress' tallies over the same statuses: collect_task_results settled = ok+skip with fail blocking (array_job_base.py:254); roster _get_array_progress settled = ok+fail with skip counted neither settled nor failed (pipeline_roster.py:120-127) so a muted TS shows 5/6 forever (roadmap section 9); tracker _update_progress done = ok+fail+skip so its bar does reach 100% (array_task_tracker.py _update_progress).
5. Running-inference split, index-keyed vs glob-based: scan_statuses (array_tasks.py:136) and _finalize_stopped_task_statuses (pipeline_runner.py:1013) key task_{idx}.out by manifest index; sync_all_jobs (pipeline_runner.py:292), _afterok_refine_running (377) and roster (pipeline_roster.py:124) use glob('task_*.out') any/count. All are stale across submissions (.out files are never cleaned - a re-queued rerun reads RUNNING); index-keyed additionally mismaps whole items if a manifest ever grows or reorders (the 05-per-ts-top-up blocker, roadmap section 8).
6. Sticky .skip contradicts the designed state machine: clean_status_dir never deletes *.skip (array_job_base.py:194-201), apply_exclusions never un-skips a no-longer-excluded TS (281-312), and get_previously_done treats .skip as settled (174-183) - so un-muting a TS never re-dispatches it. Roadmap section 4 specifies 'FAIL/SKIP -> PENDING, supervisor, on resubmit'; only the success path clears .skip (139-142) but a skipped item is never dispatched to produce that write.
7. Manifest writes are non-atomic (write_text at array_job_base.py:66; read-modify-write update_manifest 79-81) while status markers are atomic - a poll can observe truncated JSON, and the four reader variants then diverge (raise vs None vs 0 vs pass). is_superseded_task reading a mid-write manifest fails open, i.e. exactly when the guard is needed it may not fire.
8. Split readership of the duplicated manifest item list: UI + control plane read 'items' (array_tasks.py:105-140, pipeline_runner.py:997, pipeline_roster.py:108, array_task_tracker.py:80); task-mode drivers read 'ts_names' (7 drivers: fs_motion_and_ctf.py:412, ts_reconstruct.py:223, ts_ctf.py:338, ts_alignment.py:357, extract_candidates_pytom.py:404, denoise_predict.py:475, template_match_pytom.py:456). Nothing enforces the two lists stay identical; if they ever diverge, the task computes a different TS than every status display attributes to that index.
9. Marker content conventions differ and are write-only: write_skip_status stores a human reason in the .skip file (array_job_base.py:162); .ok/.fail are written empty by both writers (145, 1019); no reader anywhere reads marker content, so failure reasons have no home in the current format.
10. Superseded-writer protection is asymmetric: only write_status_atomic checks is_superseded_task (129); write_skip_status and pipeline_runner's forced-.fail writer are unguarded, and the scancel->update_manifest window still holds the OLD array_job_id so a dying orphan in that window would not be rejected (roadmap section 9, accepted as negligible).

### Frozen on-disk format spec

SCOPE: {job_dir} = the array-dispatching job's directory (External/jobNNN). Protocol names are owned by services/array_tasks.py:16-17 and must be imported, never re-typed.

1. {job_dir}/.task_manifest.json - single JSON object, written by the SUPERVISOR only.
   Frozen keys:
   - "items": list[str], ordered item (tilt-series) names. Index i IS the SLURM array task id (--array uses these indices) and keys task_{i}.out/.err. The full list is rewritten on every (re)submission and its order/content must be identical across re-submissions of the same job dir (sparse retries rely on stable index->name mapping).
   - "ts_names": list[str], REQUIRED duplicate of "items" - load-bearing legacy key: task-mode drivers index it by SLURM_ARRAY_TASK_ID. Must always equal "items" byte-for-byte.
   - "item_count": int = len(items).
   - "item_label": str, UI noun (currently always "Tilt Series"; tracker defaults to "Item" if absent).
   - "ts_metadata": optional dict[item_name -> dict] (stage/beam positions etc.).
   - "array_job_id": str, SLURM parent id of the CURRENT submission. Added by a second write AFTER sbatch returns, so a legitimate window exists where it is absent or one submission stale; consumers of it (superseded-writer rejection, scancel) must tolerate absence. SLURM ids are monotonically increasing - that ordering is part of the contract (strictly-older = orphan).
   - Arbitrary extra keys allowed (manifest_extra passthrough); readers must ignore unknown keys.
   Atomicity: REQUIRED going forward = write to a dot-prefixed tmp in the same dir + os.replace (current code violates this; readers must meanwhile treat missing/unparseable as 'no manifest yet', never as failure).

2. {job_dir}/.task_status/ - flat dir, mkdir-on-demand, one terminal marker file per item:
   - "{item}.ok"   : success. Content: empty (reserved). Writer MUST first unlink "{item}.fail" and "{item}.skip", then create atomically.
   - "{item}.fail" : failure. Content: empty today (reserved for a reason string). Writer MUST NOT remove an existing "{item}.ok". A task writer whose SLURM_ARRAY_JOB_ID is strictly older than manifest array_job_id MUST NOT write at all (fail-open when env/manifest unreadable).
   - "{item}.skip" : intentional non-run, SUPERVISOR-only, written pre-dispatch. Content: free-text reason (may be empty). Writer clears stale "{item}.ok"/"{item}.fail" first (apply_exclusions order).
   Atomicity (all three): create ".{item}.{suffix}.tmp" in the SAME directory, then os.replace to the final name. The tmp name is dot-prefixed precisely so suffix globs (*.ok etc.) never match it - any consolidation must preserve both the dot prefix and same-dir rename.
   Invariant (goal, not guaranteed by the representation): at most one marker per item. Legacy dirs can hold ok+fail simultaneously; every reader must therefore resolve with precedence ok > fail > skip. Item identity = filename stem; item names must therefore stay glob/suffix-safe (no '.ok'/'.fail'/'.skip'-colliding dots; current TS names satisfy this).

3. {job_dir}/task_{idx}.out and task_{idx}.err - SLURM stdout/stderr per array task ("task_%a" in run_array.sh). Created by SLURM the moment task idx STARTS. NEVER deleted by the protocol, hence semantics = 'index idx started at least once in SOME submission' - a weak signal only, valid for running-vs-pending inference solely within the first submission. idx maps to items[idx].

4. {job_dir}/run_array.sh - generated array sbatch script. The RELION_JOB_EXIT_SUCCESS/FAILURE trailer is stripped from it: those markers are written ONLY by the supervisor after collect_task_results (a child writing them makes relion_schemer conclude the whole job early).

5. Lifecycle order (frozen): supervisor computes previously_done = stems(.ok) UNION stems(.skip) -> cancel_previous_array(manifest.array_job_id via squeue probe + scancel) -> write_manifest(ALL items) -> clean_status_dir(keep_ok=True) [deletes *.fail only] -> [drivers call apply_exclusions before submit: unlink .ok/.fail for excluded, write .skip] -> sbatch sparse --array over indices not in previously_done, '%throttle' suffix -> update_manifest({array_job_id}) -> tasks write .ok/.fail -> supervisor collect_task_results: all_succeeded iff |ok|+|skip| == |items| AND no .fail AND no missing -> supervisor writes RELION_JOB_EXIT_{SUCCESS,FAILURE}.

6. Control-plane overlay (must stay true): job-level QUEUED-vs-RUNNING = manifest exists ? (any task_*.out ? RUNNING : QUEUED) : supervisor SLURM state. On pipeline stop, post-scancel only: items with task_{idx}.out and no marker get a forced .fail; markerless never-started items stay markerless.

### TaskStatusStore API sketch (services/array_tasks.py)

```
TARGET MODULE: services/array_tasks.py - it ALREADY exists as the read-side owner of the protocol names (task_utils.py is a deletable shim over it, and drivers/array_job_base.py already imports the constants from it at line 47), so the layering question in roadmap section 5 phase 3 ('move the status module out of drivers/ into services/') is settled: extend array_tasks.py in place; array_job_base.py keeps only SLURM mechanics (staging, sbatch build/submit, squeue wait) and re-exports for the 8 drivers during transition.

TaskState = str-enum: OK | FAIL | SKIP | RUNNING | PENDING (RUNNING/PENDING are derived-only until roadmap section 5 phase 4 adds claim records).

# -- manifest (single parse point; kills the 4 variants) --
write_manifest(job_dir: Path, items: list[str], *, item_label: str = "Tilt Series", ts_metadata: dict | None = None, extra: dict | None = None) -> Path
    Atomic tmp+os.replace; writes items AND the legacy ts_names duplicate + item_count. [driver supervisor]
update_manifest(job_dir: Path, updates: dict) -> None
    Atomic read-modify-replace; sole use today: {'array_job_id': id} after sbatch. [driver supervisor]
read_manifest(job_dir: Path) -> dict | None
    None on missing/unparseable ('no manifest yet'). [all three: drivers drop the raising copy; pipeline_runner drops its 3 inline json.loads; UI unchanged]
manifest_items(job_dir: Path) -> list[str]
    read_manifest()['items'] or [] - convenience so callers stop touching raw keys. [pipeline_runner._finalize replacement, UI]
manifest_array_job_id(job_dir: Path) -> str | None
    For scancel collection and orphan detection. [pipeline_runner.stop_and_cleanup + cancel_job; driver cancel_previous_array + is_superseded_task]

# -- writes (ALL atomic dot-tmp+replace; single choke point = roadmap section 5 phase 3) --
write_status(job_dir: Path, item: str, state: TaskState, *, reason: str = "", guard_superseded: bool = True) -> bool
    OK clears fail+skip; FAIL preserves ok; SKIP clears ok+fail (folds apply_exclusions' unlink+write pair); returns False when rejected as a superseded writer (SLURM_ARRAY_JOB_ID strictly < manifest array_job_id; fail-open). guard_superseded=False for supervisor/control-plane callers that own the dir by construction. [tasks in 8 drivers (ok/fail); supervisor (skip); replaces pipeline_runner's hand-rolled writer]
mark_stopped_tasks_failed(job_dir: Path) -> int
    Current _finalize_stopped_task_statuses semantics verbatim: manifest items with no marker but a started task_{idx}.out -> FAIL; call only after scancel (docstring-enforced until phase 4 gives owner records). [pipeline_runner.stop_and_cleanup]
reset_for_dispatch(job_dir: Path, *, keep_ok: bool = True, current_skips: set[str] | None = None) -> Path
    Today's clean_status_dir (+ mkdir); when current_skips is given, also deletes stale .skip not in it - the one deliberate behavior CHANGE candidate, fixing the sticky-skip drift per roadmap section 4's SKIP->PENDING transition; ship default = today's behavior. [driver supervisor]

# -- reads (ONE resolver; kills the 3 divergent tallies) --
read_statuses(job_dir: Path, items: list[str]) -> dict[str, TaskState]
    scan_statuses semantics frozen: ok > fail > skip > running(task_{idx}.out) > pending; becomes the back-compat shim over per-item JSON in phase 1. [UI tracker + roster sub-rows + dashboard_data; keep scan_statuses as alias one release]
settled_items(job_dir: Path) -> set[str]
    stems(.ok) UNION stems(.skip) - the dispatch skip-list. [driver get_previously_done]
succeeded_items(job_dir: Path) -> set[str]
    stems(.ok). [driver get_previously_succeeded / retry messaging]
collect_results(job_dir: Path, items: list[str]) -> ArrayResults
    Job-level tally REBUILT ON read_statuses so per-item precedence matches the UI (resolves drift 1: an ok-resolved item no longer both shows green and fails the job); all_succeeded = every item in {OK, SKIP}. [driver supervisor final tally -> RELION markers]
progress(job_dir: Path, items: list[str]) -> TaskProgress  # (n_ok, n_fail, n_skip, n_running, n_pending, total)
    The single settledness arithmetic; roster and tracker both consume it, which is where the skip-not-settled 5/6-forever roster bug gets fixed once. [pipeline_roster._get_array_progress; array_task_tracker summary/progress]
any_task_started(job_dir: Path) -> bool | None
    None when no manifest (single-shot job); else any(task_*.out) - the QUEUED-vs-RUNNING refinement, one implementation for sync_all_jobs + _afterok_refine_running, and the ONLY glob-side touchpoint of task_*.out so phase 4 retires the inference in exactly two functions (this + read_statuses). [pipeline_runner]
resolve_job_dir(job_model, project_path=None) -> Path | None   # already present, unchanged. [UI]

CONSUMER -> METHOD MAP: drivers/array_job_base.py (supervisor): write_manifest, update_manifest, read_manifest, manifest_array_job_id, reset_for_dispatch, settled_items, succeeded_items, collect_results, write_status(SKIP); drivers task mode: read_manifest (ts_names until items-only migration), write_status(OK/FAIL); pipeline_runner.py: read_manifest/manifest_array_job_id (scancel), any_task_started, mark_stopped_tasks_failed; UI/dashboard: read_statuses, progress, read_manifest, resolve_job_dir, read_tail.

HOW docs/task-status-state-machine-roadmap.md CONSTRAINS THIS CONSOLIDATION:
- Status: containment LANDED 2026-08-11 (pending runtime), the JSON representation migration is DESIGNED NOT BUILT, and the user's steer is to review the design before refactoring - so the consolidation must land as marker-file-backed and behavior-preserving first (phases 1-3), with the representation swap (one JSON per item: {state, array_job_id, task_idx, attempt}, replaced wholesale via tmp+os.replace) a separate later change hidden entirely behind read_statuses/write_status.
- Phase 1 mandates the resolver ship with a legacy fallback: read_statuses must keep reading .ok/.fail/.skip markers forever for existing projects (deadcode_test, try2_after_pixShift).
- Phase 2 names exactly the three driver-side readers to move (get_previously_succeeded, get_previously_done, collect_task_results) and says scan_statuses re-points so the UI inherits for free - matching settled_items/succeeded_items/collect_results above.
- Phase 3 names _finalize_stopped_task_statuses as the writer to fold in and pre-answers the layering (module in services/) - satisfied by extending services/array_tasks.py.
- Section 4's state machine constrains write_status: RUNNING->OK/FAIL only by the claiming task; control-plane FAIL only once the owning array is confirmed dead (mark_stopped_tasks_failed post-scancel only); the anti-tamper rule generalizes to 'reject when writer array_job_id != manifest's' ONLY after phase-4 claim records exist - until then the guard must stay STRICTLY-OLDER, because submit_array_job writes array_job_id back after sbatch returns and a current task racing that write sees a newer id (section 6).
- Phase 4 (RUNNING claim records) retires task_{idx}.out inference; the API above deliberately funnels every task_*.out touch through read_statuses + any_task_started so phase 4 is a two-site change. Roadmap 05-per-ts-top-up depends on this (growing manifests shift indices) and additionally needs the TaskState enum to decide whether a dropped TS is FAIL or a new distinct state - so the enum should be extensible.
- Section 9 pre-declares the known consolidation-adjacent fixes: index-stale forced-fails (retired by phase 4), the scancel->update_manifest orphan window (closed when records carry their owner), and roster-vs-collect skip settledness (fix once inside progress()).
```

## Registry ingest adapters (stage 4 input)

### `services/tilt_series/adapters/fs_motion_ctf.py (FsMotionCtfIngestAdapter, 291 lines)`

- **Duplicated blocks:** __init__ L57-72: registry/job_dir/required-kw job_instance_id/warp_folder/StarfileService-default assembly (structurally identical in all four; this one keeps both self.warp_folder string and self.warp_dir); ingest guard boilerplate L79-89: expected=sorted(set(...)) + empty-check ValueError 'ingest called with no expected TS ids' + missing_in_registry RuntimeError with the shared 'Reload the project to backfill the registry from mdocs' message (4 copies across adapters); emit_star scaffolding L121-131: output mkdir + tilt_series/ subdir mkdir, starfile read, require 'global' block else ValueError, in_star_dir, out_ts_df=in_ts_df.copy(); excluded-ids filtering L133 (excluded={str(t) for t in (excluded_ids or ())}) + L137-140 (per-row skip, verbatim 'Muted TS: intentionally not ingested' comment) + L167-168 (final global-block ~isin filter) — same three-part pattern in ts_ctf and ts_reconstruct; ABSENT in ts_alignment; per-TS emit loop skeleton L135-148: iterate global rows, ts_id=str(row['rlnTomoName']), registry.has_tilt_series check with 'not in registry' problem string; unresolved aggregation raise L161-165; rlnTomoTiltSeriesStarFile global rewrite to f'tilt_series/{name}.star' L170-173 (aln L240-242 identical; ctf L128-130 variant lambda); final starfile_service.write({'global': out_ts_df}) + log L175-176; _resolve_per_ts_path L222-232: try (in_star_dir/rel) then (project_root/rel), return first existing — byte-identical body to ts_alignment L484-491 (this copy has the docstring); _read_only_block L234-236: identical 3-liner in fs/aln/ctf
- **Parsing core:** L41-54 module constants/helpers (_LEGACY_MOTION_PLACEHOLDER=0.000001, _pos_float); ingest per-frame loop body L91-109 (XML lookup at warp_dir/{frame.id}.xml per Frame, attach_frame_output, missing-XML collection); _build_frame_output L180-220 (per-movie Warp XML <CTF> parse + root CTFResolutionEstimate/MeanFrameMovement QC attrs, Warp fixed output layout paths average/even/odd/powerspectrum, legacy U==V + delta-as-astig quirk, builds FsMotionCtfFrameOutput); _apply_motion_ctf_to_tilt_df L238-290 (rlnMicrographMovieName -> ts.frame_by_filename -> frame.outputs[job_instance_id], overlays 8 real columns + 6 legacy placeholder columns, duplicate-frame guard). Registry write verb: attach_frame_output (registry.py L159-163), Frame-scoped FsMotionCtfFrameOutput (models.py L64-87). Driver wiring: drivers/fs_motion_and_ctf.py L364-378 (adapter at L371, excluded_ids=set(results.skipped) at L376, registry.save() at L378).

### `services/tilt_series/adapters/ts_alignment.py (TsAlignmentIngestAdapter, 569 lines)`

- **Duplicated blocks:** __init__ L55-72: same assembly plus extra tomostar_folder param and derived tiltstack_dir/tomostar_dir; ingest guard boilerplate L95-97 (empty check) + L107-112 (missing_in_registry, same 'Reload the project' message); emit_star scaffolding L155-165 (identical to fs L121-131); per-TS emit loop skeleton L182-206 incl. has_tilt_series 'not in registry' at L198-200 — VARIANT: collects into problems dict and tolerates per-TS failure (warn + drop, only total wipeout raises L225-233) vs the strict raise in the other three; roadmap 04 stage-0 ledger flags this divergence (':266-289 tolerant results tally'); NO excluded_ids parameter anywhere — exclusion handled by ingest returning the ingested-ids list and emit_star dropping non-emitted TS from the global block (L237); the sibling three-part excluded-ids pattern is absent (divergence to flag in the base-class move); rlnTomoTiltSeriesStarFile rewrite L240-242; final global write + log L257, L265; _resolve_per_ts_path L484-491: docstring-less byte-identical copy of fs_motion's L222-232; _read_only_block L493-495: identical 3-liner
- **Parsing core:** _ALN_COL_* index constants L48-52; ingest specifics L99-105 (calls _assert_ts_identity_consistency, shift-angpix explicit-or-inferred) + tolerant drop policy L114-142; emit-side unique logic: pixel-size 3-column cascade L167-177 (silent 1.35 fallback at L177), strict rlnTomoName==star-stem identity check L184-192, all_tilts.star sidecar accumulation L179+L216-220+L259-263, duplicate-rlnTomoName guard L244-246, tomo_dimensions 'WxHxD' -> rlnTomoSizeX/Y/Z + rlnTomoTiltSeriesPixelSize L248-255; _build_ts_output L269-345 (tiltstack dir per TS, aln parse, sort by Index col, tomostar wrpMovieName -> frame_by_filename resolution, tomostar-vs-aln row-count agreement, per-frame shift*angpix conversion, builds TsAlignmentTiltSeriesOutput); _parse_alignment_files L347-374 (AreTomo .st.aln vs IMOD .xf/.tlt dispatch, >1-file guard); _read_aretomo_aln L376-392; _read_imod_xf_tlt L394-417 (2x2 matrix inversion math); _infer_alignment_angpix L419-442 (.st MRC header, local mrcfile import); _assert_ts_identity_consistency L444-482 (tomostar ∩ per-TS XML ∩ tiltstack dir set agreement — ingest precondition); _apply_alignment_to_tilt_df L497-568 (5 rlnTomo* alignment columns with NaN default L523-525, is_filtered_out row DROP L537-543 + L556-561, ts_import-dropped rows left NaN with log L562-566). Registry write verb: attach_ts_output (registry.py L165-168), TS-scoped TsAlignmentTiltSeriesOutput (models.py L130-140). Driver wiring: drivers/ts_alignment.py L303-324 (adapter L310, no excluded_ids, registry.save() L324).

### `services/tilt_series/adapters/ts_ctf.py (TsCtfIngestAdapter, 344 lines)`

- **Duplicated blocks:** __init__ L51-65: same assembly (warp_dir only); ingest guard boilerplate L74-87: empty check + combined missing_in_registry/missing-XML hard-fail (variant: also asserts per-TS XML presence in the same raise); emit_star scaffolding L114-124; rlnTomoTiltSeriesStarFile rewrite L128-130 — VARIANT: f'{preserve_subfolder}/{Path(x).name}' applied BEFORE the loop, configurable preserve_subfolder kwarg; excluded-ids filtering L132 + L136-139 (verbatim 'Muted TS' comment) + L184-185 (global ~isin filter); per-TS emit loop skeleton L134-153 incl. 'not in registry' L145-147; unresolved aggregation raise L168-172; inline per-TS star resolution L140-143: (in_star_dir / per_ts_rel).resolve() ONLY — a degenerate single-base _resolve_per_ts_path that never tries project_root (divergence vs fs/aln two-base version; flag for the base-class move); _read_only_block L258-260: identical 3-liner; final global write + log L187-188
- **Parsing core:** CTF_COLUMNS tuple L42-48; _build_ts_output L192-256 (WarpXmlParser on per-TS XML, cryoBoostKey -> ts.frame_by_warp_key via the stage-5-blessed _EER drift rule L200-211, defocus U/V = (value±delta)*10000 + astig math L215-217, are_angles_inverted L206, missing/ambiguous fail-loud L229-239, duplicate-frame dedup guard L242-248, builds TsCtfTiltSeriesOutput); rlnTomoHand global-block stamp L174-182 (unique emit-side registry READ: hand_map over registry.all_tilt_series() from are_angles_inverted, .fillna(1)); _apply_ctf_to_tilt_df L262-327 (5 CTF columns overlay incl. per-row rlnTomoHand, is_filtered_out row DROP L300-302 + L315-320, ts_import-dropped rows retain fs_motion defocus with log L321-325); dead module-level helper legacy_copy_per_ts_stars L330-343 ('kept here in case a future adapter needs it'). Registry write verb: attach_ts_output, TS-scoped TsCtfTiltSeriesOutput (models.py L107-114). Driver wiring: drivers/ts_ctf.py L296-308 (adapter L303, excluded_ids=set(results.skipped) L307, registry.save() L308).

### `services/tilt_series/adapters/ts_reconstruct.py (TsReconstructIngestAdapter, 199 lines)`

- **Duplicated blocks:** __init__ L33-48: same assembly plus derived rec_dir = job_dir/warp_folder/reconstruction; ingest guard boilerplate L64-66 (empty check) + L73-78 (missing_in_registry, same 'Reload the project' message); emit_star scaffolding L131-137 — PARTIAL: read star/'global' check/copy only; no tilt_dir (emits a single-block global tomograms.star); output mkdir deferred to L186 (order divergence); excluded-ids filtering L139 + L143-146 (verbatim 'Muted TS' comment) + L183-184 (global ~isin filter); per-TS emit loop skeleton L141-157 incl. 'not in registry' L147-149; problems aggregation raise L177-181; final global write + log L186-188; NO _read_only_block and NO _resolve_per_ts_path (never reads per-TS stars); instead inline ABSOLUTE-path repointing of rlnTomoTiltSeriesStarFile L170-175 with existence fail-loud (deliberate: matches pre-refactor writer; opposite direction from the tilt_series/ rewrite in the other three)
- **Parsing core:** ingest body L68-121: rescale/frame pixel-size >0 validation L68-71, rec_res f'{rescale_angpixs:.2f}' + binning=rescale/frame L80-81, '{ts_id}_{rec_res}Apx.mrc' + even/odd half paths L85-87, _read_mrc_dims header read L94, builds TsReconstructTomogramOutput L99-110, per-TS problems dict with STRICT raise-if-any L117-121 (unlike ts_alignment's tolerant drop); emit_star overlays L159-165 (3 absolute tomogram-path columns, frame_angpix = tomogram_pixel_size/binning back-derivation L162-164, rlnTomoTomogramBinning); _read_mrc_dims L192-198 (local mrcfile import, header nx/ny/nz). Registry write verb: attach_tomogram_output (registry.py L170-175 — auto-creates the Tomogram entity), Tomogram-scoped TsReconstructTomogramOutput (models.py L152-164). Driver wiring: drivers/ts_reconstruct.py L175-191 (adapter L182, excluded_ids=set(results.skipped) L190, registry.save() L191). Note: adapters/__init__.py L10-20 exports the four classes; the shared write target is services/tilt_series/registry.py (TiltSeriesRegistry; adapters mutate in memory via attach_* verbs, drivers persist via registry.save() L310-335).

### Hand-rolled registry stamp sites (need real adapters)

- drivers/denoise_predict.py L282-326 stamp_denoise_registry() (invoked from run_supervisor_mode L433-442): imports get_registry_for + DenoisePredictTomogramOutput directly (L298), skips on empty registry (L301-303), loops ok_ts_names calling registry.attach_tomogram_output(ts, DenoisePredictTomogramOutput(...)) (L310-319) stamping fields job_instance_id=instance_id, job_dir, denoised_mrc=output_dir/basename, denoise_method=params.denoise_method.value, model_path; registry.save() L323. Best-effort by design: per-TS KeyError pass L321-322, broad except-Exception warn-only L325-326 (violates the adapters' fail-loud contract). Its STAR emission is ALSO hand-rolled separately: aggregate_output_star L244-279 (bare starfile lib, not StarfileService; keeps only .ok rows, repoints rlnTomoReconstructedTomogram to output_dir/<basename> project-relative, drops Half1/Half2 columns). A real DenoisePredictIngestAdapter needs: the shared __init__ (registry, job_dir, required-kw job_instance_id); ingest(ok_ts_ids, *, denoise_method, model_path, output_dir, tomo_basenames) that fail-louds on missing registry TS / missing denoised MRC instead of silently passing, attaching DenoisePredictTomogramOutput (model already exists, models.py L167-177, schema 1.3); emit_star absorbing aggregate_output_star with the standard excluded-ids handling; driver keeps calling registry.save() after, like the other four.
- drivers/tilt_filter.py L134-159 (Step 9, inline in main()): imports get_registry_for L135, skips on empty registry L138+L156-157, builds verdicts from df cryoBoostKey / (cryoBoostDlLabel != 'good') / cryoBoostDlProbability L139-141, loops calling registry.set_frame_filtered(str(stem), bool(is_filt), reason='DL tilt-filter' if is_filt else None, probability=...) L145-150 — stamping Frame.is_filtered_out, Frame.filter_reason, Frame.filter_probability (registry.py L192-210); KeyError pass L152-153; registry.save() L154; broad except-Exception warn-only L158-159. LATENT BUG the census surfaced: set_frame_filtered keys on Frame.id, but the driver passes cryoBoostKey; per models.py L20-24 + frame_id_to_warp_key (L40-44) EER movies have Frame.id='<stem>_EER' vs cryoBoostKey='<stem>', so on EER data every stamp KeyErrors and is silently swallowed — the L131 comment 'Frame.id is the raw-movie stem == cryoBoostKey' holds only for non-EER. A real TiltFilterIngestAdapter needs: shared __init__ plumbing; ingest resolving cryoBoostKey via TiltSeries.frame_by_warp_key (the stage-5 blessed drift rule) then set_frame_filtered by true Frame.id, fail-loud on unresolved keys; note this job stamps a per-frame VERDICT (re-stamped each run, not keyed by job_instance_id) — no TiltFilter output model exists in models.py, so the adapter design must decide verdict-verb vs typed-output; its functional emit is the trimmed tomostar (drop_tilts_from_tomostar L125) + labeled/filtered stars L95-102.
- CONTEXT (roadmap-02-cited twin of site 2, not in the two named drivers): services/jobs/tilt_filter.py L118-137 — the manual/interactive finalize path duplicates the same set_frame_filtered stamp (reason='tilt-filter', probability, per-stem KeyError pass, asyncio.to_thread(registry.save), broad except warn L136-137). It shares the cryoBoostKey-vs-Frame.id EER hazard. A real TiltFilter adapter must be shared by BOTH the DL driver and this manual path (roadmap 02 L102-104 lists them as the two stamping paths).

### BaseIngestAdapter plan

Per roadmap 04 stage 4 (docs/roadmaps/04-driver-consolidation.md L75-78): BaseIngestAdapter in services/tilt_series/adapters/_base.py owns "shared __init__, _read_only_block, _resolve_per_ts_path, excluded-ids filtering"; the four adapters shrink to their parsing cores; denoise_predict + tilt_filter get real adapters replacing the hand stamps. Concretely from the code census, the base should own: (1) __init__(registry, job_dir, *, job_instance_id, warp_folder=..., starfile_service=None) — all four are structurally identical (fs L57-72, aln L55-72, ctf L51-65, rec L33-48); job_instance_id MUST stay a required keyword with no default (Roadmap 02 stage-5 contract); derived dirs (tomostar_dir, tiltstack_dir, rec_dir) via a subclass hook or class attrs. (2) The ingest preamble: sorted(set(expected)) + 'ingest called with no expected TS ids' ValueError + missing_in_registry RuntimeError with the shared 'Reload the project to backfill the registry from mdocs' message (4 copies: fs L79-89, aln L95-112, ctf L74-87, rec L64-78). (3) _read_only_block (3 identical copies: fs L234-236, aln L493-495, ctf L258-260). (4) _resolve_per_ts_path two-base resolution (fs L222-232 ≡ aln L484-491; ts_ctf's inline single-base variant at ctf L140-143 is a flagged divergence — adopt or keep as documented override). (5) Excluded-ids machinery: normalize excluded set, per-row 'Muted TS' skip, final global ~isin filter (fs L133/137-140/167-168, ctf L132/136-139/184-185, rec L139/143-146/183-184); ts_alignment takes no excluded_ids (drops via emitted-list, L237) — per the roadmap's prime directive that divergence is KEPT as a subclass policy knob, not silently aligned. (6) emit_star template skeleton: read input star + require 'global' block + out_ts_df copy + tilt_dir mkdir + per-TS loop with has_tilt_series/'not in registry' checks + problems aggregation + tilt_series/{name}.star global rewrite + final write/log — with the strict-vs-tolerant error policy (fs/ctf/rec raise on any problem; aln warns+drops, raises only on total wipeout, L225-233) as an explicit subclass hook. (7) Optionally a shared MRC-header helper (aln _infer_alignment_angpix L434-442 and rec _read_mrc_dims L192-198 both locally import mrcfile). The base must NOT call registry.save() — persistence stays in the drivers (fs_motion_and_ctf.py L378, ts_alignment.py L324, ts_ctf.py L308, ts_reconstruct.py L191). Each subclass keeps: its parser (_build_frame_output fs L180-220; _build_ts_output + aln/xf/tlt readers + identity assert aln L269-482; _build_ts_output ctf L192-256; ingest-body MRC-dims/binning rec L68-121), its _apply_*_to_tilt_df overlay (fs L238-290, aln L497-568, ctf L262-327), its typed output model + attach verb (frame- vs TS- vs tomogram-scoped), and unique emit extras (fs legacy placeholders; aln all_tilts.star sidecar + tomo_dimensions + pixel cascade; ctf hand_map/rlnTomoHand + preserve_subfolder; rec absolute-path repointing). Composition note from both roadmaps (02 L281-283, 04 L93-94): an IngestAdapter Protocol states the contract (ingest(expected_ts_ids)/emit_star(...)), BaseIngestAdapter shares the code — Protocol + base class compose, not compete.

### Roadmap 02 stage 5 coordination (job_instance_id)

Roadmap 02 stage 5 (docs/roadmaps/02-tiltseries-single-source.md: spec L266-270, record L210-222, code-complete 2026-08-11) requires of adapters: (a) job_instance_id is a REQUIRED keyword on all four ingest adapters — the deleted defaults ('tsCTF'/'tsAlignment') were dead (every driver passes instance_id explicitly) and wrong ('tsCTF' is not even the enum value, which is 'tsCtf'; outputs written under a default key would have been read by nothing); (b) the _EER Warp-key drift is BLESSED into the identity contract — encoded exactly once as frame_id_to_warp_key() (services/tilt_series/models.py L40-44) and resolved via TiltSeries.frame_by_warp_key() (models.py L316-322); ts_ctf's old inline three-branch _resolve_frame fallback chain was deleted, and the other adapters resolve via star movie names (frame_by_filename), needing no change. Verified in current code: all four adapter __init__s declare '*, job_instance_id: str' with no default (fs_motion_ctf.py L63, ts_alignment.py L61, ts_ctf.py L57, ts_reconstruct.py L39), and ts_ctf resolves through frame_by_warp_key (ts_ctf.py L200-211). Consequence for roadmap 04 stage 4 ('Coordinates with Roadmap 02 stage 5 (required job_instance_id)', 04 L78): BaseIngestAdapter's shared __init__ must carry job_instance_id forward as required-kw and must not reintroduce any default; the two new adapters must honor the same contract — denoise_predict's hand stamp already passes instance_id (drivers/denoise_predict.py L314), but tilt_filter's stamp uses registry.set_frame_filtered, which is a verdict verb carrying NO job_instance_id at all (re-stamped per run, not output-keyed) — so the tilt_filter adapter design must explicitly decide verdict-verb vs job_instance_id-keyed typed output, and per stage-5's blessed drift rule it must resolve cryoBoostKey via frame_by_warp_key instead of the current Frame.id==cryoBoostKey assumption that silently fails on EER data.

## Command transcripts (stage 2 acceptance references)

PROVENANCE CAVEAT - all transcripts predate the Aug 11-12 driver edits. Driver file mtimes (ls -l --time-style=full-iso /users/artem.kushner/dev/crboost_server/drivers/*.py): all drivers 2026-08-11 10:53-15:29 (+0200), driver_base.py 2026-08-12 12:00:05. Log vintages: 3dclass_aln_pixShiftBug 2026-03-23, try2_after_pixShift and pos9_10_after_pixShift 2026-03-24, demo 2026-06-08. So NO transcript was produced by current driver code; the March sets additionally predate the per-TS array refactor (their fs_motion/ts_alignment/ts_ctf/etc. commands are whole-job monolithic: ts_alignment fuses ts_import+create_settings+ts_aretomo into one bash chain; demo's June array-model splits ts_import into its own job003 and runs alignment/ctf per-TS from .staging/task_<ts> CWDs with relative --settings paths). Demonstrated command drift March->June: fs_motion --folder_data ../../frames -> frames (staged), ts_import gained --min_intensity 0.0, ts_aretomo lost --axis_iter 3 --axis_batch 1, ts_ctf switched from absolute job003/job004 paths + defocus-hand chain to relative staged paths with no defocus-hand step. Drift March->current code: current ts_ctf.py:75 builds grep -q \"should be set to 'flip'\" while the March log shows the wrapper-escaped rendering grep -q should be set to '\"flip\"' - quoting differs, so ts_ctf transcripts are NOT byte-usable for the defocus-hand fragment. DRIVER ATTRIBUTION: mapped via each job dir's job.star fn_exe line (e.g. demo/External/job002/job.star line 19: drivers/fs_motion_and_ctf.py --instance_id fsMotionAndCtf), not guessed from commands; per-project mapping: demo job002=fsMotionAndCtf job003=tsImport job004=aligntiltsWarp job005=tsCtf; 3dclass+pos9_10 job002-010 = fsMotionAndCtf, aligntiltsWarp, tsCtf, tsReconstruct, templatematching, tmextractcand, subtomoExtraction, reconstructParticle, class3d; try2 job002-008 same minus job009/010. TRANSCRIPT FIDELITY: commands marked verbatim came from untruncated single-line echoes - ts_import demo run.out:12 ([DRIVER] Command:), ts_alignment monolithic run.out:70, ts_reconstruct run.out:18, subtomo run.out:13, reconstruct_particle run.out:13, class3d run.out:14, demo task_0.out:14 ([TASK 0] Command:) for ts_alignment/ts_ctf. The one-line echoes for fs_motion_and_ctf are truncated IN-LOG with literal '...' (demo task_0.out:14 at ~300 chars; March run.out:15 [DRIVER] Built inner command: at ~500 chars), and monolithic ts_ctf + both pytom drivers echo no single line at all; for those the command was reconstructed by deterministically joining the pretty-printed 'bash -c' payload in the [ CONTAINER EXECUTION ] block (strip indent, drop trailing backslash, single-space join; block-displayed \"'*.eer'\" corresponds to built '*.eer', verified against files having both forms, e.g. demo job003 run.out:12 vs :46). Only the ts_ctf grep fragment is quoting-ambiguous (flagged above); everything else in reconstructed commands is exact. WRAPPER CONTEXT (also in every log): warptools jobs run via apptainer run --nv --cleanenv with fixed -B set + image warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif; pytom via pytom_match_pick_0.10.0.sif (TM additionally binds templates/copia); relion jobs via relion5.0_tomo.sif with 'export RELION_QSUB_EXTRA_COUNT=8; export RELION_QSUB_EXTRA1..8=...' prefixed inside bash -c and extra unsets DISPLAY XAUTHORITY. Multi-TS monolithic pytom jobs (pos9_10 job006/job007, 2 tilt-series) emit one CONTAINER EXECUTION block per tomogram sequentially; recorded exemplar = first (Position_10). TM command differs between projects: 3dclass has --non-spherical-mask, pos9_10/try2 do not. demo fs_motion uses --perdevice 2, all March runs --perdevice 1. Supervisor-side (non-tool) array submission lines live in demo run.out ([SUPERVISOR] Submitting array sbatch: .../run_array.sh, run.out:18). note.txt files contain only the RELION fn_exe driver invocation, run.err files are empty - neither holds tool commands. newp project has no External jobs. Full driver list checked against drivers/ dir; drivers/ also contains subtomo_merge.py and extract_pick_list.py which were not in the requested list and have no transcripts either.

### fs_motion_and_ctf

**demo** — `projects/demo/External/job002` (log mtime 2026-06-08 15:45:13.614871762 +0200)

```
test -f warp_frameseries.settings || (WarpTools create_settings --folder_data frames --extension '*.eer' --folder_processing warp_frameseries --output warp_frameseries.settings --angpix 1.55 --eer_ngroups -32) && WarpTools fs_motion_and_ctf --settings warp_frameseries.settings --m_grid 1x1x3 --m_range_min 500 --m_range_max 10 --m_bfac -500 --c_grid 2x2x1 --c_window 512 --c_range_min 30.0 --c_range_max 6.0 --c_defocus_min 1.1 --c_defocus_max 8.0 --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --perdevice 2 --out_averages --out_skip_first 0 --out_skip_last 0 --out_average_halves
```
source: `projects/demo/External/job002/task_0.out`

**3dclass_aln_pixShiftBug** — `projects/3dclass_aln_pixShiftBug/External/job002` (log mtime 2026-03-23 17:04:01.895837000 +0100)

```
test -f warp_frameseries.settings || (WarpTools create_settings --folder_data ../../frames --extension '*.eer' --folder_processing warp_frameseries --output warp_frameseries.settings --angpix 1.55 --eer_ngroups -32) && WarpTools fs_motion_and_ctf --settings warp_frameseries.settings --m_grid 1x1x3 --m_range_min 500 --m_range_max 10 --m_bfac -500 --c_grid 2x2x1 --c_window 512 --c_range_min 30.0 --c_range_max 6.0 --c_defocus_min 1.1 --c_defocus_max 8.0 --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --perdevice 1 --out_averages --out_skip_first 0 --out_skip_last 0 --out_average_halves
```
source: `projects/3dclass_aln_pixShiftBug/External/job002/run.out`

**pos9_10_after_pixShift** — `projects/pos9_10_after_pixShift/External/job002` (log mtime 2026-03-24 09:33:46.856632000 +0100)

```
test -f warp_frameseries.settings || (WarpTools create_settings --folder_data ../../frames --extension '*.eer' --folder_processing warp_frameseries --output warp_frameseries.settings --angpix 1.55 --eer_ngroups -32) && WarpTools fs_motion_and_ctf --settings warp_frameseries.settings --m_grid 1x1x3 --m_range_min 500 --m_range_max 10 --m_bfac -500 --c_grid 2x2x1 --c_window 512 --c_range_min 30.0 --c_range_max 6.0 --c_defocus_min 1.1 --c_defocus_max 8.0 --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --perdevice 1 --out_averages --out_skip_first 0 --out_skip_last 0 --out_average_halves
```
source: `projects/pos9_10_after_pixShift/External/job002/run.out`

**try2_after_pixShift** — `projects/try2_after_pixShift/External/job002` (log mtime 2026-03-24 09:37:00.624564000 +0100)

```
test -f warp_frameseries.settings || (WarpTools create_settings --folder_data ../../frames --extension '*.eer' --folder_processing warp_frameseries --output warp_frameseries.settings --angpix 1.55 --eer_ngroups -32) && WarpTools fs_motion_and_ctf --settings warp_frameseries.settings --m_grid 1x1x3 --m_range_min 500 --m_range_max 10 --m_bfac -500 --c_grid 2x2x1 --c_window 512 --c_range_min 30.0 --c_range_max 6.0 --c_defocus_min 1.1 --c_defocus_max 8.0 --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --perdevice 1 --out_averages --out_skip_first 0 --out_skip_last 0 --out_average_halves
```
source: `projects/try2_after_pixShift/External/job002/run.out`

### ts_import

**demo** — `projects/demo/External/job003` (log mtime 2026-06-08 15:46:13.145884385 +0200)

```
test -d tomostar && ls tomostar/*.tomostar >/dev/null 2>&1 || (WarpTools ts_import --mdocs /users/artem.kushner/dev/crboost_server/projects/demo/mdoc --pattern '*.mdoc' --frameseries ../job002/warp_frameseries --output tomostar --tilt_exposure 3.0 --override_axis 84.4 --min_intensity 0.0 --dont_invert) && test -f warp_tiltseries.settings || (WarpTools create_settings --folder_data tomostar --extension '*.tomostar' --folder_processing warp_tiltseries --output warp_tiltseries.settings --angpix 1.55 --exposure 3.0 --tomo_dimensions 4096x4096x2048)
```
source: `projects/demo/External/job003/run.out`

### ts_alignment

**demo** — `projects/demo/External/job004` (log mtime 2026-06-08 15:47:32.594731676 +0200)

```
WarpTools ts_aretomo --settings warp_tiltseries.settings --output_processing warp_tiltseries --angpix 6.2 --alignz 1800 --perdevice 1
```
source: `projects/demo/External/job004/task_0.out`

**3dclass_aln_pixShiftBug** — `projects/3dclass_aln_pixShiftBug/External/job003` (log mtime 2026-03-23 17:07:07.474800000 +0100)

```
test -d tomostar && ls tomostar/*.tomostar >/dev/null 2>&1 || (WarpTools ts_import --mdocs /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/mdoc --pattern '*.mdoc' --frameseries ../job002/warp_frameseries --output tomostar --tilt_exposure 4.8 --override_axis 84.36 --dont_invert) && test -f warp_tiltseries.settings || (WarpTools create_settings --folder_data tomostar --extension '*.tomostar' --folder_processing warp_tiltseries --output warp_tiltseries.settings --angpix 1.55 --exposure 4.8 --tomo_dimensions 4096x4096x2048) && WarpTools ts_aretomo --settings warp_tiltseries.settings --output_processing warp_tiltseries --angpix 6.2 --alignz 1800 --perdevice 1 --axis_iter 3 --axis_batch 1
```
source: `projects/3dclass_aln_pixShiftBug/External/job003/run.out`

**pos9_10_after_pixShift** — `projects/pos9_10_after_pixShift/External/job003` (log mtime 2026-03-24 09:40:06.546538000 +0100)

```
test -d tomostar && ls tomostar/*.tomostar >/dev/null 2>&1 || (WarpTools ts_import --mdocs /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/mdoc --pattern '*.mdoc' --frameseries ../job002/warp_frameseries --output tomostar --tilt_exposure 4.47 --override_axis 84.36 --dont_invert) && test -f warp_tiltseries.settings || (WarpTools create_settings --folder_data tomostar --extension '*.tomostar' --folder_processing warp_tiltseries --output warp_tiltseries.settings --angpix 1.55 --exposure 4.47 --tomo_dimensions 4096x4096x2048) && WarpTools ts_aretomo --settings warp_tiltseries.settings --output_processing warp_tiltseries --angpix 6.2 --alignz 1800 --perdevice 1 --axis_iter 3 --axis_batch 1
```
source: `projects/pos9_10_after_pixShift/External/job003/run.out`

**try2_after_pixShift** — `projects/try2_after_pixShift/External/job003` (log mtime 2026-03-24 09:40:23.868426000 +0100)

```
test -d tomostar && ls tomostar/*.tomostar >/dev/null 2>&1 || (WarpTools ts_import --mdocs /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/mdoc --pattern '*.mdoc' --frameseries ../job002/warp_frameseries --output tomostar --tilt_exposure 4.8 --override_axis 84.36 --dont_invert) && test -f warp_tiltseries.settings || (WarpTools create_settings --folder_data tomostar --extension '*.tomostar' --folder_processing warp_tiltseries --output warp_tiltseries.settings --angpix 1.55 --exposure 4.8 --tomo_dimensions 4096x4096x2048) && WarpTools ts_aretomo --settings warp_tiltseries.settings --output_processing warp_tiltseries --angpix 6.2 --alignz 1800 --perdevice 1 --axis_iter 3 --axis_batch 1
```
source: `projects/try2_after_pixShift/External/job003/run.out`

### ts_ctf

**demo** — `projects/demo/External/job005` (log mtime 2026-06-08 15:49:33.398739968 +0200)

```
WarpTools ts_ctf --settings warp_tiltseries.settings --input_processing warp_tiltseries --output_processing warp_tiltseries --window 512 --range_low 30.0 --range_high 6.0 --defocus_min 1.1 --defocus_max 8.0 --voltage 300 --cs 2.7 --amplitude 0.1 --perdevice 1
```
source: `projects/demo/External/job005/task_0.out`

**3dclass_aln_pixShiftBug** — `projects/3dclass_aln_pixShiftBug/External/job004` (log mtime 2026-03-23 17:08:22.556673000 +0100)

```
mkdir -p /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job004/warp_tiltseries && cp /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job003/warp_tiltseries/*.xml /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job004/warp_tiltseries/ && hand_output=$(WarpTools ts_defocus_hand --settings /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job003/warp_tiltseries.settings --output_processing /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job004/warp_tiltseries --check 2>&1) ; echo $hand_output ; if echo $hand_output | grep -q should be set to '"flip"' ; then WarpTools ts_defocus_hand --settings /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job003/warp_tiltseries.settings --output_processing /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job004/warp_tiltseries --set_flip ; else WarpTools ts_defocus_hand --settings /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job003/warp_tiltseries.settings --output_processing /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job004/warp_tiltseries --set_noflip ; fi && WarpTools ts_ctf --settings /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job003/warp_tiltseries.settings --input_processing /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job004/warp_tiltseries --output_processing /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job004/warp_tiltseries --window 512 --range_low 30.0 --range_high 6.0 --defocus_min 1.1 --defocus_max 8.0 --voltage 300 --cs 2.7 --amplitude 0.1 --perdevice 1
```
source: `projects/3dclass_aln_pixShiftBug/External/job004/run.out`

**pos9_10_after_pixShift** — `projects/pos9_10_after_pixShift/External/job004` (log mtime 2026-03-24 09:41:52.356743000 +0100)

```
mkdir -p /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job004/warp_tiltseries && cp /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job003/warp_tiltseries/*.xml /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job004/warp_tiltseries/ && hand_output=$(WarpTools ts_defocus_hand --settings /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job003/warp_tiltseries.settings --output_processing /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job004/warp_tiltseries --check 2>&1) ; echo $hand_output ; if echo $hand_output | grep -q should be set to '"flip"' ; then WarpTools ts_defocus_hand --settings /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job003/warp_tiltseries.settings --output_processing /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job004/warp_tiltseries --set_flip ; else WarpTools ts_defocus_hand --settings /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job003/warp_tiltseries.settings --output_processing /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job004/warp_tiltseries --set_noflip ; fi && WarpTools ts_ctf --settings /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job003/warp_tiltseries.settings --input_processing /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job004/warp_tiltseries --output_processing /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job004/warp_tiltseries --window 512 --range_low 30.0 --range_high 6.0 --defocus_min 1.1 --defocus_max 8.0 --voltage 300 --cs 2.7 --amplitude 0.1 --perdevice 1
```
source: `projects/pos9_10_after_pixShift/External/job004/run.out`

**try2_after_pixShift** — `projects/try2_after_pixShift/External/job004` (log mtime 2026-03-24 09:41:29.373441000 +0100)

```
mkdir -p /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job004/warp_tiltseries && cp /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job003/warp_tiltseries/*.xml /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job004/warp_tiltseries/ && hand_output=$(WarpTools ts_defocus_hand --settings /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job003/warp_tiltseries.settings --output_processing /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job004/warp_tiltseries --check 2>&1) ; echo $hand_output ; if echo $hand_output | grep -q should be set to '"flip"' ; then WarpTools ts_defocus_hand --settings /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job003/warp_tiltseries.settings --output_processing /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job004/warp_tiltseries --set_flip ; else WarpTools ts_defocus_hand --settings /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job003/warp_tiltseries.settings --output_processing /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job004/warp_tiltseries --set_noflip ; fi && WarpTools ts_ctf --settings /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job003/warp_tiltseries.settings --input_processing /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job004/warp_tiltseries --output_processing /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job004/warp_tiltseries --window 512 --range_low 30.0 --range_high 6.0 --defocus_min 1.1 --defocus_max 8.0 --voltage 300 --cs 2.7 --amplitude 0.1 --perdevice 1
```
source: `projects/try2_after_pixShift/External/job004/run.out`

### ts_reconstruct

**3dclass_aln_pixShiftBug** — `projects/3dclass_aln_pixShiftBug/External/job005` (log mtime 2026-03-23 17:14:27.490640000 +0100)

```
WarpTools ts_reconstruct --settings /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job003/warp_tiltseries.settings --input_processing /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job004/warp_tiltseries --output_processing /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job005/warp_tiltseries --angpix 6.2 --halfmap_frames 1 --deconv 1 --perdevice 1 --dont_invert
```
source: `projects/3dclass_aln_pixShiftBug/External/job005/run.out`

**pos9_10_after_pixShift** — `projects/pos9_10_after_pixShift/External/job005` (log mtime 2026-03-24 09:50:40.300452000 +0100)

```
WarpTools ts_reconstruct --settings /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job003/warp_tiltseries.settings --input_processing /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job004/warp_tiltseries --output_processing /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job005/warp_tiltseries --angpix 6.2 --halfmap_frames 1 --deconv 1 --perdevice 1 --dont_invert
```
source: `projects/pos9_10_after_pixShift/External/job005/run.out`

**try2_after_pixShift** — `projects/try2_after_pixShift/External/job005` (log mtime 2026-03-24 09:47:34.272415000 +0100)

```
WarpTools ts_reconstruct --settings /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job003/warp_tiltseries.settings --input_processing /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job004/warp_tiltseries --output_processing /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job005/warp_tiltseries --angpix 6.2 --halfmap_frames 1 --deconv 1 --perdevice 1 --dont_invert
```
source: `projects/try2_after_pixShift/External/job005/run.out`

### template_match_pytom

**3dclass_aln_pixShiftBug** — `projects/3dclass_aln_pixShiftBug/External/job006` (log mtime 2026-03-23 17:21:08.264374000 +0100)

```
pytom_match_template.py -t /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/templates/copia/ellipsoid_550_550_550_apix6.20_box128_lp45_black.mrc -d /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job006/tmResults -m /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/templates/copia/ellipsoid_550_550_550_apix6.20_box128_lp45_mask.mrc --angular-search 90.0 --voltage 300.0 --spherical-aberration 2.7 --amplitude-contrast 0.1 --per-tilt-weighting --log debug -g 0 -s 2 2 1 --non-spherical-mask -v /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job006/tmResults/3dclass_aln_pixShiftBug_Position_11_2.mrc --tilt-angles /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job006/tiltAngleFiles/3dclass_aln_pixShiftBug_Position_11_2.tlt --defocus /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job006/defocusFiles/3dclass_aln_pixShiftBug_Position_11_2.txt --dose-accumulation /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job006/doseFiles/3dclass_aln_pixShiftBug_Position_11_2.txt
```
source: `projects/3dclass_aln_pixShiftBug/External/job006/run.out`

**pos9_10_after_pixShift** — `projects/pos9_10_after_pixShift/External/job006` (log mtime 2026-03-24 10:04:30.476220000 +0100)

```
pytom_match_template.py -t /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/templates/copia/ellipsoid_550_550_550_apix6.20_box128_lp45_black.mrc -d /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job006/tmResults -m /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/templates/copia/ellipsoid_550_550_550_apix6.20_box128_lp45_mask.mrc --angular-search 90.0 --voltage 300.0 --spherical-aberration 2.7 --amplitude-contrast 0.1 --per-tilt-weighting --log debug -g 0 -s 2 2 1 -v /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job006/tmResults/pos9_10_after_pixShift_Position_10.mrc --tilt-angles /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job006/tiltAngleFiles/pos9_10_after_pixShift_Position_10.tlt --defocus /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job006/defocusFiles/pos9_10_after_pixShift_Position_10.txt --dose-accumulation /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job006/doseFiles/pos9_10_after_pixShift_Position_10.txt
```
source: `projects/pos9_10_after_pixShift/External/job006/run.out`

**try2_after_pixShift** — `projects/try2_after_pixShift/External/job006` (log mtime 2026-03-24 09:53:39.012475000 +0100)

```
pytom_match_template.py -t /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/templates/copia/ellipsoid_550_550_550_apix6.20_box128_lp45_black.mrc -d /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job006/tmResults -m /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/templates/copia/ellipsoid_550_550_550_apix6.20_box128_lp45_mask.mrc --angular-search 90.0 --voltage 300.0 --spherical-aberration 2.7 --amplitude-contrast 0.1 --per-tilt-weighting --log debug -g 0 -s 2 2 1 -v /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job006/tmResults/try2_after_pixShift_Position_11_2.mrc --tilt-angles /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job006/tiltAngleFiles/try2_after_pixShift_Position_11_2.tlt --defocus /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job006/defocusFiles/try2_after_pixShift_Position_11_2.txt --dose-accumulation /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job006/doseFiles/try2_after_pixShift_Position_11_2.txt
```
source: `projects/try2_after_pixShift/External/job006/run.out`

### extract_candidates_pytom

**3dclass_aln_pixShiftBug** — `projects/3dclass_aln_pixShiftBug/External/job007` (log mtime 2026-03-23 17:22:32.442646000 +0100)

```
pytom_extract_candidates.py -n 1500 --particle-diameter 285.2 --relion5-compat --log debug --number-of-false-positives 1.0 -j /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job007/tmResults/3dclass_aln_pixShiftBug_Position_11_2_job.json
```
source: `projects/3dclass_aln_pixShiftBug/External/job007/run.out`

**pos9_10_after_pixShift** — `projects/pos9_10_after_pixShift/External/job007` (log mtime 2026-03-24 10:07:13.521375000 +0100)

```
pytom_extract_candidates.py -n 1500 --particle-diameter 285.2 --relion5-compat --log debug --number-of-false-positives 1.0 -j /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job007/tmResults/pos9_10_after_pixShift_Position_10_job.json
```
source: `projects/pos9_10_after_pixShift/External/job007/run.out`

**try2_after_pixShift** — `projects/try2_after_pixShift/External/job007` (log mtime 2026-03-24 09:57:59.323416000 +0100)

```
pytom_extract_candidates.py -n 1500 --particle-diameter 285.2 --relion5-compat --log debug --number-of-false-positives 1.0 -j /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job007/tmResults/try2_after_pixShift_Position_11_2_job.json
```
source: `projects/try2_after_pixShift/External/job007/run.out`

### subtomo_extraction

**3dclass_aln_pixShiftBug** — `projects/3dclass_aln_pixShiftBug/External/job008` (log mtime 2026-03-23 17:24:39.441578000 +0100)

```
relion_tomo_subtomo --o /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job008/ --i /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job007/optimisation_set.star --b 786 --bin 1 --crop 448 --stack2d --float16
```
source: `projects/3dclass_aln_pixShiftBug/External/job008/run.out`

**pos9_10_after_pixShift** — `projects/pos9_10_after_pixShift/External/job008` (log mtime 2026-03-24 10:12:10.245468000 +0100)

```
relion_tomo_subtomo --o /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job008/ --i /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job007/optimisation_set.star --b 786 --bin 1 --crop 448 --stack2d --float16
```
source: `projects/pos9_10_after_pixShift/External/job008/run.out`

**try2_after_pixShift** — `projects/try2_after_pixShift/External/job008` (log mtime 2026-03-24 09:59:59.624401000 +0100)

```
relion_tomo_subtomo --o /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job008/ --i /users/artem.kushner/dev/crboost_server/projects/try2_after_pixShift/External/job007/optimisation_set.star --b 786 --bin 1 --crop 448 --stack2d --float16
```
source: `projects/try2_after_pixShift/External/job008/run.out`

### reconstruct_particle

**3dclass_aln_pixShiftBug** — `projects/3dclass_aln_pixShiftBug/External/job009` (log mtime 2026-03-23 18:22:39.541266000 +0100)

```
relion_tomo_reconstruct_particle --i /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job008/optimisation_set.star --o /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job009/ --b 786 --sym I1 --j 1 --j_in 1 --j_out 1 --crop 448
```
source: `projects/3dclass_aln_pixShiftBug/External/job009/run.out`

**pos9_10_after_pixShift** — `projects/pos9_10_after_pixShift/External/job009` (log mtime 2026-03-24 11:58:43.642927000 +0100)

```
relion_tomo_reconstruct_particle --i /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job008/optimisation_set.star --o /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job009/ --b 786 --sym I1 --j 1 --j_in 1 --j_out 1 --crop 448
```
source: `projects/pos9_10_after_pixShift/External/job009/run.out`

### class3d

**3dclass_aln_pixShiftBug** — `projects/3dclass_aln_pixShiftBug/External/job010` (log mtime 2026-03-23 18:37:50.071238000 +0100)

```
relion_refine --ios /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job008/optimisation_set.star --ref /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job009/merged.mrc --o /users/artem.kushner/dev/crboost_server/projects/3dclass_aln_pixShiftBug/External/job010/run --K 1 --iter 15 --healpix_order 3 --offset_range 5 --offset_step 2 --oversampling 1 --sym I1 --j 4 --pool 30 --pad 2 --trust_ref_size --ini_high 45.0 --particle_diameter 575.0 --tau2_fudge 1.0 --ctf --norm --scale --zero_mask --dont_combine_weights_via_disc --flatten_solvent --firstiter_cc --preread_images --gpu
```
source: `projects/3dclass_aln_pixShiftBug/External/job010/run.out`

**pos9_10_after_pixShift** — `projects/pos9_10_after_pixShift/External/job010` (log mtime 2026-03-24 12:44:00.650624000 +0100)

```
relion_refine --ios /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job008/optimisation_set.star --ref /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job009/merged.mrc --o /users/artem.kushner/dev/crboost_server/projects/pos9_10_after_pixShift/External/job010/run --K 1 --iter 15 --healpix_order 3 --offset_range 5 --offset_step 2 --oversampling 1 --sym I1 --j 4 --pool 30 --pad 2 --trust_ref_size --ini_high 45.0 --particle_diameter 575.0 --tau2_fudge 1.0 --ctf --norm --scale --zero_mask --dont_combine_weights_via_disc --flatten_solvent --firstiter_cc --preread_images --gpu
```
source: `projects/pos9_10_after_pixShift/External/job010/run.out`

### Coverage gaps (no transcript exists — need one sandbox run before migrating these)

- denoise_predict: no job instance in any project (no job.star references it; no logs)
- denoise_train: no job instance in any project
- miss_align: no job instance in any project (P1 wiring landed but never run in these sandbox projects)
- tilt_filter: no tool-command transcript anywhere; demo/TiltFilter/ and pos9_10_after_pixShift/TiltFilter/ contain only png/ and tilt_series_*/star outputs (interactive job, no External job dir, no run log)
- ARRAY-MODE nuance: TASK-mode (per-TS array) transcripts exist ONLY in demo and only for fs_motion_and_ctf, ts_alignment, ts_ctf (task_0.out). ts_reconstruct, template_match_pytom, extract_candidates_pytom, subtomo_extraction have transcripts only from the pre-array monolithic driver code path (March logs) - no array-task exemplar exists for them in any project

## Stage-0 fact checks (denoise)

- denoise_predict array-migrated: **true**
- denoise_train is a global (non-array) reduction: **true**
- DENOISE_PER_TS_SPLIT_PLAN.md: **NOT_FOUND** (stale: false)

(1a) denoise_predict IS fully migrated to the SLURM-array supervisor/task model. Evidence from /users/artem.kushner/dev/crboost_server/drivers/denoise_predict.py (566 lines, read fully): module docstring L2-22 ("supervisor + per-tomogram SLURM array task ... dispatches on the SLURM_ARRAY_TASK_ID env var"; TASK mode "atomically writes .task_status/{ts}.{ok|fail}"); L38-48 imports the array_job_base verbs verbatim (apply_exclusions, collect_task_results, install_cancel_handler, read_manifest, submit_array_job, wait_for_array_completion, write_skip_status, write_status_atomic, STATUS_DIR_NAME); supervisor mode L348-452 calls apply_exclusions (L398), submit_array_job with per_task_cfg/array_throttle/manifest_extra (L399-408), install_cancel_handler + wait_for_array_completion (L410-412), collect_task_results (L416), writes RELION_JOB_EXIT_SUCCESS/FAILURE (L424, L444, L451); task mode L460-550 reads the manifest via read_manifest (L474), maps array_idx -> ts_names[array_idx] (L477-479, one tomogram per task), writes per-task status via write_status_atomic (L500, L538, L547); main() L553-561 dispatches on SLURM_ARRAY_TASK_ID (unset=supervisor, set=task). The manifest file is .task_manifest.json: services/array_tasks.py:16 MANIFEST_FILENAME = ".task_manifest.json" (and :17 STATUS_DIR_NAME = ".task_status"), re-exported by drivers/array_job_base.py:47. Side note confirming roadmap 04's divergence-ledger item: denoise_predict stamps the registry by hand in stamp_denoise_registry at L282-327 (roadmap cites :283-327 — matches).

(1b) denoise_train IS a single global (non-array) job. Evidence from /users/artem.kushner/dev/crboost_server/drivers/denoise_train.py (424 lines, read fully): zero imports from array_job_base and zero occurrences of SLURM_ARRAY_TASK_ID anywhere in the file; explicit comment L255-257 "Training is a single global reduction for both methods (one model from many tomograms); only the tool chain differs"; run_isonet_train docstring L33-34 "single global job (one model from many tomograms)"; the cryoCARE path builds ONE train_config.json aggregating all matching even/odd pairs (L342-364) and runs one extract + one train command (L367-388); it writes RELION_JOB_EXIT_SUCCESS/FAILURE itself in a single linear main() (L244-245, L409, L417).

(2) DENOISE_PER_TS_SPLIT_PLAN.md DOES NOT EXIST anywhere in the repo, so plan_doc_stale=false in the literal sense: there are no stale passages to quote because there is no document. Search evidence: `find /users/artem.kushner/dev/crboost_server -name 'DENOISE*' -not -path '*/.git/*'` -> empty; case-insensitive `-iname '*denoise*'` (excluding .git/venv/projects) -> only drivers/denoise_{predict,train}.py and services/jobs/denoise_{predict,train}.py (+ pycache); listings of docs/, docs/roadmaps/, docs/attic/ contain no such doc; `grep -rn "DENOISE_PER_TS_SPLIT_PLAN"` across all *.md/*.py -> exactly one hit, docs/roadmaps/04-driver-consolidation.md:51. Bounded search of /users/artem.kushner/dev (maxdepth 3) also found nothing. Related: ISONET_INTEGRATION_PLAN.md, referenced from denoise_predict.py:191 and denoise_train.py:42, is ALSO absent from the working tree (only container_defs/isonet2.def matches '*ISONET*') — both denoise plan docs were removed from the repo. git is unavailable in this sandbox, so deletion history could not be confirmed.

The stale claim the roadmap's "memory correction" targets lives OUTSIDE the repo, in session auto-memory: /users/artem.kushner/.claude/projects/-users-artem-kushner-dev-crboost-server/memory/project_denoise_per_ts_split.md — frontmatter description L3 still says "train/predict not migrated to per-TS array model", and L137 says "Plan written to repo root DENOISE_PER_TS_SPLIT_PLAN.md"; that memory's own body already records the rewrite (L15-23: "drivers/denoise_predict.py REWRITTEN to supervisor+per-tomogram SLURM array ... denoise_train stays SINGLE (reduction)"). MEMORY.md's one-line index entry carries the same stale phrasing ("train/predict never migrated to array model").

Exact edit that resolves stage 0 item 3: in /users/artem.kushner/dev/crboost_server/docs/roadmaps/04-driver-consolidation.md lines 49-51, the item currently ends "...No migration work needed there beyond adapter/status adoption; update `DENOISE_PER_TS_SPLIT_PLAN.md` if it still says otherwise." Both roadmap claims are hereby CONFIRMED against current code; the only correction needed is to the dangling reference itself — replace the final clause with a resolution note, e.g.: "adoption. (Confirmed against code 2026-08-12; `DENOISE_PER_TS_SPLIT_PLAN.md` no longer exists in the repo — this reference was its last mention — so there is nothing to update; the stale 'never migrated' claim survives only in session auto-memory, which should be corrected instead.)"

---

# Appendix — canonical array-driver lifecycle reference

# Array-driver lifecycle reference — `drivers/array_job_base.py` + `drivers/driver_base.py`

Census date 2026-08-12, branch `isonet_auto_and_missalignment_fix`. `array_job_base.py` = 719 lines; `driver_base.py` = 400 lines. Both read in full.

## 1. Public API inventory

### `drivers/array_job_base.py` (719 lines)

| Name | Lines | Purpose |
|---|---|---|
| `MANIFEST_FILENAME`, `STATUS_DIR_NAME` (re-export) | 47 | `from services.array_tasks import ... as ...` re-export. Comment at 44–46 marks it **load-bearing**: "8 drivers import these FROM this module". Literal values (owned by `services/array_tasks.py:16-17`): `".task_manifest.json"` and `".task_status"`. |
| `write_manifest(job_dir, ts_names, *, ts_metadata=None, extra=None)` | 50–67 | Writes the array-index→TS-name manifest JSON to `job_dir/.task_manifest.json`. |
| `read_manifest(job_dir)` | 70–73 | Loads manifest; raises `FileNotFoundError` if absent. |
| `update_manifest(job_dir, updates)` | 76–81 | Read-modify-write merge of keys into an existing manifest (used to stamp `array_job_id` post-submission). |
| `is_superseded_task(job_dir)` | 89–113 | True iff `SLURM_ARRAY_JOB_ID` env < manifest's `array_job_id` (SLURM ids are monotonic) — identifies an orphan task from an older submission. Fails open (`False`) on any unreadable/non-numeric input. |
| `write_status_atomic(status_dir, item_name, ok)` | 116–146 | Atomic per-item terminal marker: tmp-file + `os.replace` → `{item}.ok` or `{item}.fail`. Success clears stale `.fail`/`.skip` (139–142); failure does **not** clear an existing `.ok` (an `.ok` at failure time means the writer is a superseded orphan). Refuses to write at all if `is_superseded_task` (129–135). |
| `write_skip_status(status_dir, item_name, reason="")` | 149–163 | Atomic `{item}.skip` marker = intentionally-not-run (reason text stored as file content). Keeps the item visible in the per-TS UI strip as deliberately blank, not failed/missing. |
| `get_previously_succeeded(job_dir)` | 166–171 | Set of stems with `.ok` in the status dir. |
| `get_previously_done(job_dir)` | 174–183 | Set of stems with `.ok` OR `.skip` — "settled" items for sparse-array dispatch. |
| `clean_status_dir(job_dir, keep_ok=False)` | 186–202 | Deletes `.fail` files (they get retried); deletes `.ok` too unless `keep_ok=True`; mkdirs and returns the status dir. |
| `ArrayResults` (dataclass) | 205–222 | `ok / failed / missing / skipped: list[str]`, `all_succeeded: bool`; `summary` property (215–222) renders "N ok, N skip, N fail, N missing". |
| `read_tilt_series_names_from_input_star(input_star)` | 225–239 | Sorted `rlnTomoName` list from the input STAR's `global` block via `StarfileService` — the authoritative TS list (a `*.xml` glob would resurrect alignment-excluded TS). |
| `collect_task_results(job_dir, ts_names)` | 242–257 | Tallies status dir → `ArrayResults`. `all_succeeded = (ok + skip == len(ts_names)) and no fail and no missing` (line 254). |
| `load_excluded_ts(project_path)` | 265–278 | User "exclude from processing" set from the TiltSeries registry (`excluded_ids()`); late import; never raises — degrades to empty set with a WARN. |
| `apply_exclusions(job_dir, project_path, ts_names)` | 281–312 | Pre-marks user-excluded TS as `.skip` (clearing stale `.ok`/`.fail` first so each item carries exactly one marker), so the array never dispatches them. Returns the excluded subset for glob/merge-based aggregation to drop. |
| `preflight_registry(project_path, expected_ts_names, job_name)` | 320–361 | Verifies the registry covers every TS about to be dispatched, BEFORE `submit_array_job`; raises with a diagnostic dump (empty registry → "reload project"; name mismatch → prefix-drift guidance). |
| `copy_tomostar_with_absolute_paths(src, dst, original_dir)` | 369–403 | Copies a `.tomostar`, rewriting relative `_wrpMovieName` first-tokens to absolute paths resolved against the original dir (WarpTools resolves them relative to the tomostar's location). |
| `stage_per_ts_environment(job_dir, ts_name, input_processing, settings_file)` | 411–465 | Builds `job_dir/.staging/task_{ts}/` containing: copy of the settings XML, `tomostar/{ts}.tomostar` (absolute movie paths), `warp_tiltseries/{ts}.xml` symlink — so WarpTools sees exactly ONE tilt-series. Returns `(staged_settings_file, staged_input_processing)`. |
| `build_array_sbatch_script(template_path, job_dir, project_path, instance_id, per_task_cfg, array_spec, driver_script)` | 473–546 | Reads `config/qsub.sh`, injects `#SBATCH --array=...` before the `--output` line, substitutes `XXXextra1XXX`..`XXXextra8XXX` from the per-task `SlurmConfig` + `XXXcommandXXX` with the driver re-invocation, strips the RELION marker block (see §7), writes `job_dir/run_array.sh` mode 0755. |
| `submit_array_sbatch(script_path, cwd)` | 549–562 | `sbatch` in an env stripped of `SLURM_`/`SBATCH_` vars; returns the job id (last token of "Submitted batch job NNNN"); raises `RuntimeError` on non-zero rc. |
| `wait_for_array_completion(array_job_id, poll_secs=30)` | 565–578 | Blocking `squeue -j` poll loop until the array leaves the queue. |
| `submit_array_job(job_dir, project_path, instance_id, ts_names, per_task_cfg, array_throttle, driver_script, *, ts_metadata=None, manifest_extra=None)` | 586–670 | The one-call supervisor dispatch (see §2). Returns the SLURM array job id, or `None` if all items already settled. |
| `cancel_previous_array(job_dir)` | 673–703 | `scancel` the manifest's recorded `array_job_id` if `squeue` says it's still live (prevents duplicate-GPU work and `.ok`+`.fail` double-marking). Returns cancelled id or None. |
| `install_cancel_handler(array_job_id, job_dir)` | 706–718 | SIGTERM/SIGINT handlers: `scancel` the array, `touch job_dir/RELION_JOB_EXIT_FAILURE` (714), `sys.exit(130)`. |

### `drivers/driver_base.py` (400 lines)

| Name | Lines | Purpose |
|---|---|---|
| `load_project_state(project_path)` | 29–39 | `ProjectState.load(project_path/"project_params.json")`; raises `FileNotFoundError` if absent. |
| `get_driver_context(expected_type=None)` | 45–137 | Primary bootstrap for all drivers (see §8). |
| `IDLE_TIMEOUT_DEFAULT` | 143 | `45 * 60` s of total silence before `run_command` treats a tool as hung. |
| `_derive_watchdog_timeout()` | 146–200 | (private, load-bearing) Wall-clock budget: `SLURM_JOB_END_TIME` → `SLURM_JOB_TIME_LIMIT` (parses both integer-minutes and `[DD-]HH:MM:SS`) → 8 h fallback; ×0.9 safety margin so the driver dies before SLURM's SIGTERM and can still write a failure marker. |
| `run_command(command, cwd, timeout=None, idle_timeout=IDLE_TIMEOUT_DEFAULT)` | 203–281 | `shell=True` Popen in a new session, streams merged stdout/stderr, dual watchdog (total-time + zero-output idle) killing the whole process group via `os.killpg` SIGKILL; raises `CalledProcessError` on non-zero rc. |
| `run_command_with_retries(command, cwd, attempts=3, retry_delay=10, label="command", timeout=None)` | 284–323 | Bounded fixed-delay retry wrapper around `run_command`; re-raises the final `CalledProcessError`. |
| `diagnose_stale_producer(path)` | 326–376 | Builds the "a populated copy exists elsewhere" hint by scanning sibling `External/job*/<name>` — distinguishes stale job-number drift from never-deployed `pending_*` placeholders. Returns `""` when nothing useful. |
| `require_producer_input(path, label, *, require_nonempty=True)` | 379–399 | Validates a resolved upstream input; raises `FileNotFoundError` whose message embeds `diagnose_stale_producer` output. |

## 2. Supervisor flow (canonical)

Per the module docstring (lines 5–17) and `submit_array_job` (586–670):

1. Driver bootstraps via `get_driver_context(ParamsClass)` in `run_supervisor_mode()`; on bootstrap failure touches `RELION_JOB_EXIT_FAILURE` in cwd (e.g. `ts_ctf.py:184-185`).
2. Enumerate TS (job-specific; canonically `read_tilt_series_names_from_input_star`, 225–239).
3. Optionally `preflight_registry` (320–361) — fail loud at dispatch, not after wasted subjobs.
4. `apply_exclusions` (281–312) — pre-mark user-muted TS as `.skip` "right before `submit_array_job`".
5. `submit_array_job` (586–670), internally:
   1. `get_previously_done` → `indices_to_run` = indices not yet `.ok`/`.skip` (610–611); return `None` if nothing to run (622–624).
   2. `cancel_previous_array` (628) — scancel any still-live older array on this job dir.
   3. `write_manifest` with **ALL** ts_names — keeps index→name mapping stable across re-runs (630–631).
   4. `clean_status_dir(keep_ok=True)` — drop `.fail`, keep `.ok` (633–634).
   5. Compute array spec: fresh run → `0-{N-1}%throttle`; re-run → comma-list of failed/missing indices `%throttle` (637–645).
   6. `build_array_sbatch_script` from `config/qsub.sh` (653–661) then `submit_array_sbatch` (664).
   7. `update_manifest(job_dir, {"array_job_id": ...})` (668) for UI cross-reference and orphan detection.
6. (Driver-side) `install_cancel_handler` (706–718), `wait_for_array_completion` (565–578).
7. `collect_task_results` (242–257) → job-specific metadata aggregation → supervisor writes the RELION exit marker.

## 3. Task flow (canonical)

Per docstring lines 13–17; task mode is detected by the driver via `SLURM_ARRAY_TASK_ID` in the array env (comment at 485–486):

1. Bootstrap again via `get_driver_context(ParamsClass)` in `run_task_mode(array_idx)`.
2. `read_manifest` (70–73); index `manifest["items"][SLURM_ARRAY_TASK_ID]` → this task's TS.
3. `stage_per_ts_environment` (411–465) to isolate one TS for WarpTools.
4. Run the job-specific tool command (`run_command` / `run_command_with_retries`).
5. `write_status_atomic` (116–146) to report `.ok`/`.fail` — silently suppressed if the task is superseded (129–135).

## 4. Manifest format

Written by `write_manifest` (lines 59–66) to `job_dir/.task_manifest.json`:

```json
{
  "items": ["TS_01", "TS_02", ...],        // array index → TS name (positional)
  "ts_names": ["TS_01", "TS_02", ...],     // duplicate of items (legacy/UI alias)
  "item_count": N,
  "item_label": "Tilt Series",
  "ts_metadata": { "TS_01": {...}, ... },  // optional (stage_position, beam_position, ...)
  ...extra keys merged at root...,          // caller-supplied manifest_extra
  "array_job_id": "12345"                   // added AFTER sbatch via update_manifest (line 668)
}
```

Protocol names (`.task_manifest.json`, `.task_status`) are owned by `services/array_tasks.py:16-17` and re-exported at `array_job_base.py:47`.

## 5. Exclusion (`.skip`) mechanism

- Two producers of `.skip`: (a) job-specific "no upstream work" pre-marks via `write_skip_status` (149–163), (b) user forward-only mute via `apply_exclusions` (281–312) reading `registry.excluded_ids()` through `load_excluded_ts` (265–278).
- Excluded items get stale `.ok`/`.fail` cleared first so each carries exactly one marker (301–306); the manifest still lists ALL names so the UI strip shows a deliberate skip, not a hole (288–291).
- `.skip` counts as "settled": not dispatched (`get_previously_done`, 174–183), not failed/missing, and counted toward `all_succeeded` (247, 254).
- On-disk STARs are untouched ("forward-only", 292); callers must also drop excluded names from glob/merge aggregation using the returned list.

## 6. Status-file writes

- Dir: `job_dir/.task_status/`; one marker per item: `{item}.ok` | `{item}.fail` | `{item}.skip`.
- Atomicity: write empty tmp `.{item}.{suffix}.tmp` then `os.replace` (143–146, 160–163). `.skip` files carry the reason string as content (162).
- Single-terminal-marker invariant: success clears prior `.fail`/`.skip` (139–142); failure never clears `.ok` (124–127); superseded orphan tasks (older `SLURM_ARRAY_JOB_ID` than the manifest's) write nothing (129–135, 89–113).

## 7. Results tally + RELION_JOB_EXIT_* markers

- `collect_task_results` (242–257): `ok`/`fail`/`skip` from globs, `missing = ts_names − accounted`, `all_succeeded = (|ok|+|skip| == |ts_names|) ∧ ¬fail ∧ ¬missing`.
- **Only the supervisor writes `RELION_JOB_EXIT_{SUCCESS,FAILURE}`.** `build_array_sbatch_script` strips qsub.sh's `$EXIT_CODE`-conditional marker block from the array script, replacing it with `"# [array task] RELION markers suppressed — supervisor writes them after all tasks finish"` (527–541): a child writing the marker first makes relion_schemer think the whole job is done while tasks are still queued (528–531).
- Failure markers touched directly: `install_cancel_handler` on signal (714); supervisor bootstrap `except` blocks touch `Path.cwd()/RELION_JOB_EXIT_FAILURE` (e.g. `ts_ctf.py:185`, `fs_motion_and_ctf.py:286`, `denoise_predict.py:354`, `subtomo_extraction.py:104`, `extract_candidates_pytom.py:183`, `ts_import.py:106`; `tilt_filter.py:28-29` binds both marker paths).
- The supervisor's own qsub.sh (not built here) retains the `$EXIT_CODE` block, so supervisor success/failure markers come from the unmodified template.

## 8. `get_driver_context()` exact return tuple

Signature (`driver_base.py:45`): `get_driver_context(expected_type: type[T] | None = None) -> tuple[ProjectState, T, dict, Path, Path, JobType]`.

Return statement (line 137): `return (project_state, job_model, context_data, job_dir, project_path, job_type)` where:

1. `project_state: ProjectState` — loaded from `project_params.json`, with `.project_path` set (69–70).
2. `job_model: T` — `project_state.jobs[instance_id]`, runtime-isinstance-checked against `expected_type` (76–92); its `.paths` is overwritten in-memory with fresh resolver output, not persisted (118–122).
3. `context_data: dict` — `{"instance_id", "job_type" (str value), "paths" (str-valued dict), "additional_binds"}` (124–129).
4. `job_dir: Path` — `Path.cwd().resolve()` (64); authoritative because qsub.sh cd's into the allocated dir (105–106).
5. `project_path: Path` — resolved `--project_path` arg (63).
6. `job_type: JobType` — derived from the model, fatal if unset (95–98).

All failure paths are `sys.exit(1)` with a FATAL stderr message (72–73, 78–83, 87–92, 97–98, 115–116). Args parsed: `--instance_id` (required), `--project_path` (required); `parse_known_args` with `allow_abbrev=False` (54–62). Paths are re-resolved at drive time via `PathResolutionService.resolve_all_paths` + `get_context_paths` because the schedule-time snapshot can drift when the schemer skips a job number (100–116).

## Fact checks

### (a) Driver-invocation command duplication — CONFIRMED parallel implementations; roadmap line numbers are stale in BOTH files

`services/scheduling_and_orchestration/pipeline_orchestrator_service.py`, inside `_build_fn_exe` (method spans 492–514; the claimed ~529–541 now lands inside `_get_current_relion_counter`). Current lines **503–514**:

```python
        python_exe = server_dir / "venv" / "bin" / "python3"
        if not python_exe.exists():
            python_exe = "python3"

        script_path = server_dir / "drivers" / script

        return (
            f"export PYTHONPATH={server_dir}:${{PYTHONPATH}}; "
            f"{python_exe} {script_path} "
            f"--instance_id {instance_id} "
            f"--project_path {project_dir}"
        )
```

`drivers/array_job_base.py`, inside `build_array_sbatch_script` (the claimed ~445–454 now lands inside `stage_per_ts_environment`'s tomostar staging). Current lines **496–505**:

```python
    python_exe = server_dir / "venv" / "bin" / "python3"
    if not python_exe.exists():
        python_exe = Path("python3")

    driver_cmd = (
        f"export PYTHONPATH={server_dir}:${{PYTHONPATH}}; "
        f"{python_exe} {driver_script} "
        f"--instance_id {instance_id} "
        f"--project_path {project_path}"
    )
```

Verdict: **Yes — truly parallel implementations** of the same command string: identical `export PYTHONPATH={server_dir}:${PYTHONPATH}; <python> <driver> --instance_id <id> --project_path <path>` template, including the same venv-python-with-fallback probe. Differences are cosmetic/contextual only: fallback typed `"python3"` (str) vs `Path("python3")`; the orchestrator derives the script from `JOB_SPEC_BY_TYPE[job_type].driver` (498–507) while the array builder takes `driver_script` as a parameter; orchestrator returns the string as RELION `fn_exe` (supervisor launch), array builder injects it as `XXXcommandXXX` (task re-invocation). Any change to the driver CLI contract must be made in both places. Roadmap line refs should be updated to 503–514 and 496–505 (or 500–505 for the `driver_cmd` literal alone).

### (b) `get_driver_context()` call sites — 22 unpack sites across 14 driver files, NOT 17; "three naming conventions" holds at the family level

Grep over `drivers/` + `services/`, each site confirmed by reading. Excluded: import lines, the definition (`driver_base.py:45`), its docstring example (`driver_base.py:52`), and a docstring mention (`miss_align.py:9`). `drivers/extract_pick_list.py` and `drivers/subtomo_merge.py` do NOT call it (extract_pick_list uses its own argparse + `run_command`; subtomo_merge imports nothing from driver_base).

| # | File:line | Mode | Unpacked variable names |
|---|---|---|---|
| 1 | `drivers/ts_ctf.py:182` | supervisor | `_project_state, params, local_params_data, job_dir, project_path, _job_type` |
| 2 | `drivers/ts_ctf.py:328` | task | `_project_state, params, local_params_data, job_dir, _project_path, _job_type` |
| 3 | `drivers/ts_alignment.py:203` | supervisor | `_project_state, params, local_params_data, job_dir, project_path, _job_type` |
| 4 | `drivers/ts_alignment.py:345` | task | `_project_state, params, local_params_data, job_dir, _project_path, _job_type` |
| 5 | `drivers/denoise_predict.py:350` | supervisor | `project_state, params, local_params_data, job_dir, project_path, _job_type` |
| 6 | `drivers/denoise_predict.py:462` | task | `project_state, params, local_params_data, job_dir, _project_path, _job_type` |
| 7 | `drivers/denoise_train.py:234` | single | `_project_state, params, local_params_data, job_dir, project_path, _job_type` |
| 8 | `drivers/fs_motion_and_ctf.py:281` | supervisor | `_project_state, params, local_params_data, job_dir, project_path, _job_type` |
| 9 | `drivers/fs_motion_and_ctf.py:400` | task | `_project_state, params, local_params_data, job_dir, project_path, _job_type` |
| 10 | `drivers/ts_reconstruct.py:111` | supervisor | `_project_state, params, local_params_data, job_dir, project_path, _job_type` |
| 11 | `drivers/ts_reconstruct.py:211` | task | `_project_state, params, local_params_data, job_dir, _project_path, _job_type` |
| 12 | `drivers/ts_import.py:101` | single | `_project_state, params, local_params_data, job_dir, _project_path, job_type` |
| 13 | `drivers/miss_align.py:181` | single | `_project_state, params, local_params_data, job_dir, _project_path, _job_type` |
| 14 | `drivers/template_match_pytom.py:268` | supervisor | `_state, params, context, job_dir, project_path, _job_type` |
| 15 | `drivers/template_match_pytom.py:444` | task | `state, params, context, job_dir, project_path, _job_type` |
| 16 | `drivers/subtomo_extraction.py:102` | supervisor | `_state, params, context, job_dir, project_path, _job_type` |
| 17 | `drivers/subtomo_extraction.py:369` | task | `_state, params, context, job_dir, _project_path, _job_type` |
| 18 | `drivers/extract_candidates_pytom.py:179` | supervisor | `state, params, context, job_dir, project_path, _job_type` |
| 19 | `drivers/extract_candidates_pytom.py:392` | task | `_state, params, context, job_dir, _project_path, _job_type` |
| 20 | `drivers/class3d.py:23` | single | `_state, params, context, job_dir, _project_path, _job_type` |
| 21 | `drivers/reconstruct_particle.py:29` | single | `_state, params, context, job_dir, _project_path, _job_type` |
| 22 | `drivers/tilt_filter.py:26` | single | `_project_state, job_model, _context_data, job_dir, project_path, _job_type` (no parens) |

Naming conventions — three families, matching the roadmap's "three different naming conventions" claim:

- **Family A** — `(…project_state, params, local_params_data, …)`: 13 sites (rows 1–13; `denoise_predict` un-underscores `project_state`; `ts_import` un-underscores `job_type`).
- **Family B** — `(…state, params, context, …)`: 8 sites (rows 14–21; `state` vs `_state` varies).
- **Family C** — `(_project_state, job_model, _context_data, …)`: 1 site (row 22, `tilt_filter.py`).

Underscore-prefix usage is inconsistent *within* each family (`_project_state`/`project_state`, `_state`/`state`, `project_path`/`_project_path`, `_job_type`/`job_type`), so a mechanical rename must treat each site individually. **The roadmap's count of 17 is stale/wrong: the current count is 22** (likely drift — e.g. the denoise per-TS split added supervisor+task pairs after the roadmap census; `drivers/denoise_predict.py` and `drivers/denoise_train.py` carry Aug 11 mtimes).
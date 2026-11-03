---
name: project-dl-tilt-filter
description: DL tilt-filter integration (roadmap 06) CLOSED 2026-10-15 — maintainer: works well enough, in use; doc in docs/roadmaps/completed/roadmap_06-dl-tilt-filter.md
metadata: 
  node_type: memory
  type: project
  originSessionId: 56bc6b8f-7d30-409b-9d35-39858bdeae36
  modified: 2026-08-11T14:10:54.413Z
---

**CLOSED 2026-10-15** (maintainer: works well enough). Doc moved to `docs/roadmaps/completed/roadmap_06-dl-tilt-filter.md`; unbuilt/unrun items dropped. Weights still travel by hand (`filterTilts/*.pth` is git-ignored).

Scoped 2026-08-11, design revised same day by user (no code yet). Full plan: `docs/roadmaps/roadmap_06-dl-tilt-filter.md`.

**User's core decision:** TiltFilter is ALWAYS interactive — pipeline hard-stops at it, user MUST approve the tilt selection, only then does downstream proceed. NO unattended mode. The only switch is `filter_method: manual | deep_learning` (flipped when queuing the job): manual = hand-label gallery; DL = inference auto-dispatched to cluster, user reviews/overrides predictions, approves. This KILLED the per-instance IS_INTERACTIVE migration (ClassVar stays, no call-site audit). OOD (out-of-distribution) check = review-time warning banner only, never an orchestration gate.

**Assets:** weights `/groups/klumpe/software/Models/model260212.pth` — declares `model_architecture: SmallSimpleCNN`, keys match the **already-vendored** `filterTilts/` package exactly (536 MB = optimizer state included; weights ~180 MB). the model author's prototype next to it is mostly ported already; napari viewer/mdoc handler/plotter not worth porting.

**Key findings:**
- DL path fully scaffolded (drivers/tilt_filter.py, backend.submit_tilt_filter_dl, UI button) but NEVER ran: model dropdown default/binary/oneclass → nonexistent paths. Stage 0 = conf.yaml `tilt_filter.models` registry + red-marker when unconfigured.
- **Silent-fallback hazard (the barrier's raison d'être):** pending interactive TiltFilter is dropped from the resolver producer pool (path_resolution_service.py:531) → downstream deployed while filter pending resolves alignment against tsImport's UNTRIMMED tomostar, no warning. Fix = deploy truncates at first pending interactive job (schemer + afterok paths).
- Approve & Continue = one shared commit fn (driver + panel currently duplicate trim/stamp/stars; output locations diverge: TiltFilter/ vs dl_run/) + SUCCEEDED + auto-resume rest of state.pipeline_order.
- Manual-wins merge invariant: job_model.tilt_labels always overrides DL (driver step 6 already does this).
- venv has torch 2.6.0 but GPU-node execution unverified; fallback = miss-align container pattern.
- Two assumptions pending user confirmation: (a) approval auto-resumes downstream; (b) DL auto-dispatches when inputs land (not on panel open).

Related: [[project-pipeline-reshape-2026-07-29]] (tiltFilter placement + tomostar trim), [[project-per-ts-top-up]] (re-approval downstream staleness owned there).

**2026-08-18 — the silent-fallback hazard is FIXED, but NOT by the barrier.** User hit it live in
`/groups/klumpe/crboost_data/denovo_picking_testdrive`: labelled tilts and saved while tsImport was
still queued → "Cannot finalize: tsImport tomostar not found"; the panel stamped SUCCEEDED anyway,
alignment ran 3 min later on the untrimmed tomostar. Root fix chosen instead of the deploy-truncation
barrier: **TiltFilter no longer produces a tomostar dir at all** (`OUTPUT_SCHEMA = []`, `input_tomostar`
slot gone). The verdict is the registry's per-frame `is_filtered_out`; `drivers/ts_alignment.py`
`enumerate_items` applies it via `drop_tilts_from_tomostar` when it snapshots the tomostars, and
CTF/reconstruct inherit that snapshot. tsImport is the sole TOMOSTAR_DIR producer → no `pending_tiltFilter`
phantom in the IO menu, no ordering dependency, filter-off vs filter-on = empty vs non-empty drop set.
Commit is gated on success now (was: SUCCEEDED unconditionally, both DL and manual paths). Roadmap 06's
Stage-3.3 barrier section is obsolete; the rest (model registry, filter_method switch) still stands.
Ruff-clean, PENDING RUNTIME.

**2026-09-29 — rev 2 of roadmap 06 (user asked for "interactive/automatic").** The 2026-08-11 "no unattended
mode" decision is lifted: `review: always | never` (`on_ood` later), interactive default. The 08-18 fix removed
the phantom path but NOT the ordering hazard — nothing makes alignment wait, so an uncommitted filter = all
tilts silently. Barrier redesigned as a held SLURM gate job (hold → Approve = commit + `scontrol release`).
New defects: prob_threshold 0.1 is a no-op (v1 used 0.70), image_size must be 384, manual-wins broken (post-DL
reload writes every DL label into tilt_labels), probabilities never reach the gallery, cluster install venv has
no torch. Threshold/normalisation must come from the model author.

**2026-09-29 rev 3 — THE SHIPPED WEIGHTS ARE DEAD.** model260212.pth = epoch 49 of a run that collapsed
(recorded acc peak 0.925 @ epoch 11, val loss 14.78 @ 12 after an LR blow-up (max_lr 0.0196), then constant
0.5294 / loss ≈ ln 2). Every input → P(good) 0.557. Any threshold ≤ 0.557 keeps everything silently.
Plan: v1's fastai model (3-ch, ImageNet norm, val 0.955) converted to a state dict + a liveness gate in the
driver; calibrate thresholds on OUR labels (22 projects → 219 TS, 9,201 tilts, 1,078 bad; gallery PNGs are
byte-exact model inputs); store P(bad); dim-exposure rule (<10 % of series mean counts) = 99.3 % of those
labelled bad ≈ half of all bad labels. Barrier = SEGMENT SUBMISSION (park downstream in ProjectState,
Approve submits the rest), NOT a held SLURM gate (that keeps pipeline_active true for days). K3 out of scope.
**Rev 4 (same day, approved):** modes Manual / DL review / DL auto on the job row. Manual + DL review park the
pipeline (review_hold; Approve → submit_parked); DL review's DL run is on request, predict-only; DL auto is a
chain job that commits in-job. Maintainer evaluates the current weights and asks the author for upgraded ones;
v1 conversion / retrain are fallbacks. Stage 1 = DL review without parking (the evaluation path).

**2026-09-29 — implementation started (branch `dl_filter`, local clone `~/dev/crboost_server_dl_filter`).**
Stage 0 done (conf.yaml registry + tiltFilter profile; weights load with `weights_only=True`; GPU check True on
V100; torch 2.6.0+cu124 needs ≤ GLIBC_2.16; cluster install venv still lacks torch). Stage 1 commits 1–3 in
(config registry · `Frame.p_bad` schema 1.5 · predict_run in `TiltFilter/dl_run/NNN` settled by the monitor).
Chunks 4 (predict-only inference) and 5 (review gallery) are specced in roadmap 06 §12 — the log of record.
The rev-4 roadmap was never committed on `predep_roadmaps`; the clone now carries it + §12 (the main tree
still holds an uncommitted rev-4 copy — superseded). Chosen refinements: predicted-bad derived from `p_bad`
and the job threshold (not stored); predict_run has no threshold; driver predicts by default, stage 3 adds
`--commit`; liveness = every series with ≥2 predictions has std(P(bad)) < 0.05.

**2026-09-29 (second session) — chunk 4 code-complete, ruff-clean, handed over for commit.** Params now
`model` (None → default) / `threshold` 0.5 on P(bad) / `dl_batch_size`; `resolve_model` + `prediction_liveness`
live in `services/jobs/tilt_filter.py`; driver = predict-only (gallery PNGs, converts the rest, registry
`set_frame_prediction`, never the verdict); `statistics_calculator.py` deleted. Refinements are logged in roadmap
06 §12. Next = chunk 5 (review gallery). The duplicate chunk-3 commit (7ecc924 / b1bd2e7, 13 s apart) could not be
inspected from the sandbox (no git, no zlib in perl) — the user checks it with `git log --stat -3`.
Chunk 4 committed (77d8534). **Chunk 5 code-complete same day → stage 1 CODE-COMPLETE, runtime pass owed** (roadmap
06 §9 stage-1 success check). Threshold field moved into the gallery row (live relabel). Next = stage 2
(modes on the row + parking), the heaviest remaining piece; long pole overall = working weights (§7).
Chunk 5 committed (281f5f4). **First runtime pass 2026-09-29 on `agg_20260311_412_Grid3`** (697 tilts): driver clean on
a Quadro RTX 6000, P(bad) = 0.443 for EVERY tilt → dead weights confirmed on real data. The committed filter
pinned the threshold at 0.50 (USER_PARAMS freeze on SUCCEEDED = defect 2) → commit 6 empties
`TiltFilterParams.USER_PARAMS` (SUCCEEDED here = verdict committed, params steer the next run/review).
Commit 6 fix user-confirmed (threshold relabel, clicks solid, re-run keeps clicks). Weights diagnostic for the model
author: `docs/reports/dl-tilt-filter/scripts/check_tiltnet.py` (standalone, CPU); shipped file sha256 88cada42…62bd.
Its real output is still owed (Claude's sandbox cannot run python/torch) — the user runs it and pastes it.
**check_tiltnet.py run 2026-09-29 — definitive:** best epoch 12 (acc 0.925), val loss 14.78 at epoch 13 (one-cycle
max_lr 0.0196, Adam-type, wd 0.078), dead from epoch 15; file = after epoch 50. All weights ~1e-4, fc3+relu 100 %
zeros → logits == fc4.bias [−0.115, +0.115] → P(bad) 0.4427 for any input; batch-stat BN identical → not a BN-stat
problem, needs epoch-12 weights or a retrain. Roadmap 06 §7 rewritten with 1-based epochs (the old text was 0-based).
**End of session 2026-09-29 (handoff written).** Commits 4, 5, 6 in (77d8534, 281f5f4, 03644d5); commit 7 (diagnostic
script + output + roadmap §7.1–7.3 rewrite) handed over. Roadmap 06 is the log of record: §7.1 diagnosis table,
§7.2 requests to the model author, §12 runtime pass (still to check: tab closed mid-run, Approve on a COPY with
threshold > 0.443). Next = stage 2, split into commit-sized chunks in §12 first.

**2026-09-30 (session start).** Commit 7 was still uncommitted (script + output untracked; roadmap modified) —
re-handed. Chunk-3 "duplicate" resolved from raw objects: a split, not a duplicate (7ecc924 = backend.py,
services/jobs/tilt_filter.py, pipeline_runner.py; b1bd2e7 = pipeline_monitor.py, ui/tilt_filter_panel.py);
roadmap branch note updated. Stage 2 has ZERO code (no FilterMode/review_hold/submit_parked); deploy still skips
the interactive filter and submits alignment immediately (`pipeline_orchestrator_service.py:164-166`). Drafted a
concise proof note for the model author (in chat, not the repo): stored record + last-epoch evidence + fc3 dead.
**2026-09-30 — stage 2 split into commits 8 (row controls + one commit path + Re-open) and 9 (parking); specs +
refinements in roadmap 06 §12.** Key calls: default mode `manual`; only alignment + its downstream park (tsImport
runs during review); parked jobs get dirs at Approve; parking afterok-only (schemer refuses); commit refused once
alignment RUNNING/SUCCEEDED (so Grid3 Approve is refused now), Re-open also refused while QUEUED. Chunk 8
code-complete (ruff-clean, not run): `commit_verdict`/`reopen_review`/`effective_label`/`predictions_for` in
services/jobs/tilt_filter.py, `fs_motion_star` in tilt_series_service, backend approve/reopen/cancel_predict,
row controls = ui/pipeline_builder/tilt_filter_row.py (own FingerprintedView + 3 s timer), `rerender_job` on the
builder panel. Next = chunk 9 after the user commits 8.
Commit 8 landed 2026-09-30. Session ended at 43 % context with a handoff; roadmap 06 §12 now also holds
file:line anchors for chunk 9 (uncommitted, rides with commit 9). Next session = chunk 9 (parking).

**2026-09-30 (third session) — chunk 9 built and committed (as two same-subject commits, all files staged).**
review_hold schema 3.9; `_parked_behind_review` + `submit_parked` + per-project submit lock in the orchestrator
(hold recorded BEFORE the submit awaits, so a mid-submit Approve finds it); refresher bug found: the status timer
was created inside the run slot and died with its rebuild → `StatusPoller.start()` in the builder's own slot.
**Route-1 weights arrived the same day** (`new_weights/`: the author's ResNet-18 `run_best.pth` + training/predict
scripts; untracked, keep the .pth out of git). Static decode: alive (normal magnitudes, best epoch 8). Contract:
224 px resize of our 384 gallery PNGs, per-image std clipped ±7.2146σ, 8-fold D4 averaging, temperature 0.739,
cut P(bad) ≥ 0.301. Validation leaked (split_mode=provided → within-dataset 0.976/0.998). **Commit 10 built**
(ruff-clean, not run): ResNet18Gray, `convert_checkpoint.py` (user ran it → Models/tiltnet_resnet18_o1034.pth),
ModelLoader per-model transform/TTA/temperature, registry `threshold` (job takes a model's cut on its first
Run DL, not at the select). Grid3 check command in roadmap 06 §7.4 (not yet run). Chunks 11 (DL auto), 12
(refresher on Stop/Cancel), 13 (calibration script + weights decision) are specced in §12.
**Commit 10 landed 2026-09-30; ResNet check ran (docs/reports/dl-tilt-filter/check_resnet_run_best.txt):** alive
(synthetic spread 0.989, blank → P(bad) 1.0); on Grid3 flags 47 tilts, ALL at |angle| ≥ 60°. GOTCHA: Grid3's registry
has NO committed drops (filter committed pre-registry-verdict; old tiltseries_filtered.star in TiltFilter/) — its
human labels are tiltFilter.tilt_labels in project_params.json (56 bad, 2 good: 53 at ≥60°, 3 at 40–49°). Per-tilt
comparison still owed. Maintainer's call: if it holds, the ResNet replaces the dead CNN outright (chunk 10b: default
model, drop tiltnet_260212 + SmallSimpleCNN + "half"). Roadmap edits + report file uncommitted, ride with 10b.
**2026-09-30 (fourth session) — per-tilt check PASSED; 10b built (ruff-clean, handed over).** In-app run 003
(Grid3, ResNet): precision 1.000 / recall 0.839 at 0.3006 (47 flagged, all human-bad; 9 misses at 42–46° and 60–70°
with P(bad) ≤ 0.071), 51/56 in top 56, AUROC 0.995; bimodal output (nothing in 0.1–0.5). Threshold snapped 0.40 → 0.301.
10b = ResNet the only model + default; SmallSimpleCNN/"half" gone; dev conf.yaml edited. Found two defects, logged in
roadmap 06 §8: **j** gallery showed the old run until a page reload — NFSv3 attr cache (see
[[reference-nfs-attr-cache-registry]]); fix specced as **10c** (`reload_registry` in reconcile before `settle`).
**k** liveness (std < 0.05 in every series) calls a live decisive model dead on clean data (Grid3's 3 clean series:
0.0004–0.015) → false banner, and DL auto would fail; proposed dead = spread < 1e-6; USER DECISION OWED before chunk 11.
Chunk 11's failure-path check needs a dead model: ResNet copy with fc.1.weight zeroed. Comparison script (perl,
scratchpad only): joins registry frame `id`/`p_bad` (6-space indent) with project_params tilt_labels.
**2026-10-01 — "no flags anywhere" = two UI causes, model fine.** "Set all good" (panel) hand-labels EVERY tilt good →
hides all predictions; it wiped Grid3's 56 bad labels and agg_20251113_412's labels (defect l → "Clear labels" +
confirm). First runs on bullseye/FCIso hit defect j (reload shows them). Lost labels are recoverable from the May
stars (`tilt_series_labeled/` minus `tilt_series_filtered/` = the committed drops; equals Grid3's 56 exactly) —
restore recipe in roadmap 06 §13.8, NOT applied (needs server stopped + user go-ahead). ResNet vs May verdicts:
0.87/0.80, 0.87/0.75, but FCIso_unmilled2 0.245/0.64 (over-flags unmilled). User then opened UI/UX design
(buttons, barrier flags, card annotations, label sets "per set", stats, Journey) → recorded as roadmap 06 §13 for the
next session, nothing built. PNG answer: model needs fsMotion averages (training parity), but per-TS PNGs inside
fsMotion's array tasks (§13.1 a) would end the whole-job wait + headnode NFS pass.
**2026-10-01 (later):** PNGs stay post-fsMotion (decided). U1 scoped in roadmap 06 §13.2, NOT built: Tilts tab beside
Tomograms in `ui/tomo_gallery.py` + per-tile toggle swapping the slice for a tilt mosaic in the same box (fit math in
spec), zoom via `_show_upsample` under `dialog_host()`, caption rows become metric rows (Journey link dropped; zoom
keeps "Journey ↗"), registry-based collection, one ui.html per series + delegated click; also remove the rail's
"Link to this view" button. §13 subsections renumbered (13.3 buttons … 13.9 state).
**2026-10-01 (merge session):** weights path now relative to the checkout (`TiltFilterModelConfig.weights_path`
= REPO_ROOT / path; template `filterTilts/tiltnet_resnet18_o1034.pth`; `.gitignore` `*.pth`); converted file copied
into the clone's filterTilts/, clone conf.yaml switched to the relative path. Merge plan handed: main INTO dl_filter in
the clone (`git -c merge.conflictstyle=diff3 merge origin/main`), then push dl_filter + `--ff-only` in the main tree.
Dry-run (base 60ea956, main 601d8c6, dl 98a57c6): conflicts = backend.py (submit_tilt_filter_predict → main's
`write_sbatch_script(run_dir/"run_tilt_filter.sh", cfg, cmd)` + `driver_invocation` FileNotFoundError guard),
drivers/tilt_filter.py (take dl's; drop BOTH marker touches — qsub wrapper writes them once), roadmaps README row 06,
roadmap_06 (take dl's; main committed the stale rev-4 copy). Config: main renamed `_REPO_ROOT`→`REPO_ROOT`, dropped
`crboost_root`/`venv_path` (clone conf.yaml still sets crboost_root → unknown-key warning). Main tree had uncommitted
WIP (denoise/IsoNet halves-by-tilt, project_state.py ~l.1063, data_import_panel.py) → stash or commit before the ff.
Merge RESOLVED in the clone 2026-10-01 (main tip 0052536 incl. the user's Landing fix): exactly the 4 predicted
conflicts; plus `_REPO_ROOT`→`REPO_ROOT` in weights_path; clone conf.yaml := main tree's conf.yaml + tilt_filter block
+ tiltFilter profile (same file is what the main tree needs after the ff). Ruff: all remaining findings pre-existing on
main or in untracked new_weights/. Handed: commit merge → pytest → clone server on 8082 → push → stash/ff/pop in main.
`tatus` file in the clone = git-log output saved from less (typo) — rm it.
Merge committed (91c24b4); boundaries clean; DL run 005 on Grid3 loaded the clone's filterTilts/ weights, wrapper wrote
the marker. pytest's one failure = `pytest` resolves to ~/.local (module python, no torch) — use `python -m pytest`.
BLOCKER found before the ff: main moved to e9dc2d4 "isonet fixes" (ALREADY PUSHED to GitHub Klumpe-lab/crboost_server)
and it swept in filterTilts/new_weights/ (author's raw 45 MB run_best.pth + training scripts). Asked the user: amend +
force-push vs untrack vs ship weights. NEVER hand `git stash`/`stash pop` when nothing is modified: the main repo
holds an ancient stash (0483445, "WIP on parameter_service") that `pop` would apply.
**2026-10-01 — MERGED INTO MAIN (13111a1 = 8d0fb09 "isonet fixes" amended without new_weights + dl_filter 91c24b4).**
Main tree has conf.yaml + filterTilts/tiltnet_resnet18_o1034.pth. The force push to GitHub had NOT run at handoff (less ate
it again → stray file `h --force origin main`); GitHub main was still e9dc2d4 WITH the 45 MB run_best.pth — handed again
with --no-pager. Clone being retired: work from ~/dev/crboost_server from now on; author's raw files (clone new_weights/)
to /groups/klumpe/software/Models/tiltnet_resnet18_o1034_source. Next session: U1 (roadmap 06 §13.2) in the main tree.
Force push DONE 2026-10-01: GitHub main e9dc2d4 → 13111a1 (forced); the 45 MB checkpoint is off main's history (the old
commit may stay reachable on GitHub by SHA). Remaining: user retires the clone after this session (mv new_weights,
squeue check, optional `git branch -d dl_filter`, rm -rf clone).
**2026-10-01 (later) — 10c + 10d built, ruff-clean, NOT RUN (handed as two commits).** 10c (defect j): async
`reload_registry` in services/tilt_series/registry.py (parse in a thread, swap the cache entry on the loop; a dirty
cached instance is kept) awaited in the predict reconcile's `settle` on ANY → SUCCEEDED (marker, squeue or sacct).
10d: "Set all good" → "Clear labels" behind a confirm dialog (defect l). Defect k DECIDED by the user: dead = P(bad)
pstdev < 1e-6 in every series (`LIVENESS_MIN_STD`), banner/driver text "the same P(bad)". The 10c check needs a project
whose filter never ran DL (a Grid3 re-run writes identical numbers). Next session: U1 (roadmap 06 §13.2).
**2026-10-01 (same session, after 10c/10d were committed) — U1 BUILT, ruff-clean, NOT RUN.** New `ui/tilt_previews.py`
(registry collection, card/mosaic HTML, fit, click JS, `open_tilt_viewer` = tilt filter's `_show_upsample` under
`dialog_host()`); `ui/tomo_gallery.py` gets the Tomograms · Tilts segmented tabs, per-tile mosaic toggle (re-renders one
tile), Tilts tab groups filled on first expand; caption row no longer a Journey link; rail link button deleted. Refinements
logged in roadmap 06 §13.2 "Built". Next: the rest of §13 topic by topic (13.3 buttons … 13.8 Journey), or chunk 11.
**2026-10-02:** 10c, 10d and U1 all committed on main (U1 = "tomograms view: Tilts tab beside Tomograms, and a tile's
tilts in place of its slice"); none run yet. Handoff: the next session starts with DESIGN (no code until the user says
build): which metrics go on tilt cards / mosaic cells / tomogram caption rows (§13.5, §13.7) and how they thread into
the Journey (§13.8); record the decisions in roadmap 06 §13 or split §13 into its own roadmap.
**2026-10-02 (design session):** §13.1–13.8 moved to roadmap 26 (tilt views + Journey), see
[[project-tilt-views-roadmap26]]; 06 keeps §13.9 (status + label restores). Grid3's tilt_labels on disk are now 697 good /
0 bad (restore target of 62 unchanged). Nothing built.

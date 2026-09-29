# Roadmap 06 — DL tilt filter: three modes on the job row, a parked pipeline, and working weights

**Status:** rev 4, approved 2026-09-29 (rev 1 2026-08-11). **In progress on branch `dl_filter`:** stage 0
done; stage 1 chunks 1–3 committed, chunks 4–5 open. Next session: read §12 (implementation log) and
start at chunk 4; the decisions below are settled.

**Maintainer's decisions (2026-09-29):**
1. The filter has three modes, chosen **on the tilt-filter job row**: **Manual**, **DL review**, **DL auto**.
2. Manual and DL review **park** the pipeline at the filter until a human approves. DL auto does **not**
   park: the DL job runs inside the chain and the pipeline continues with its verdict.
3. In DL review the DL run is dispatched **on the user's request**, as a quick SLURM job — not
   automatically.
4. Build it assuming the weights work. The maintainer evaluates the current weights and is asking the
   model author for upgraded ones (§7). A dead model is **flagged**, not blocked, in review mode.
5. K3 (non-square) data is out of scope.

## 1. The modes

| Mode | Pipeline at the filter | DL dispatch | Who commits the verdict | Downstream starts |
|---|---|---|---|---|
| *(filter not in the pipeline)* | passes through | — | — | right away; only roadmap 22's blank-exposure rule applies |
| **Manual** | parks | — | a human: **Approve** | on Approve |
| **DL review** | parks | on request: **Run DL** (row or panel), predict-only | a human: **Approve** — predictions pre-filled, edits win | on Approve |
| **DL auto** | does not park | automatically, as a chain job after its inputs | the DL job itself | when the DL job succeeds |

## 2. The state machine

```
                                 Run
                                  │
            mode = Manual or DL review                     mode = DL auto
                     │                                           │
   submit upstream of the filter; park the rest       submit the whole chain; the DL job is a
   (ProjectState.review_hold)                         chain node:  fsMotion ─► DL ─► alignment ─► …
                     │                                           │
                     ▼                                           ▼
          ┌─ WAITING FOR REVIEW ─────────────────┐         QUEUED ─► RUNNING
          │  inputs pending (fsMotion not done)  │               │
          │         │ fsMotion succeeds          │               ├─ commits in-job ─► SUCCEEDED ─► alignment starts
          │         ▼                            │               └─ error, or output constant ─► FAILED
          │  ready ── Run DL (DL review) ──┐     │                    ─► downstream cancelled, reason shown
          │    ▲                           ▼     │
          │    │                      PREDICTING │
          │    └── prediction failed ◄─────┤     │
          │  predictions ready ◄───────────┘     │
          │  (labels editable in every sub-state)│
          └──────────────────────────────────────┘
             │ Approve                │ Switch to DL auto             │ Stop
             ▼                        ▼                               ▼
   commit ─► SUCCEEDED ─►   submit the DL job (afterok on anything    hold cleared; parked jobs
   submit parked jobs       still running) + parked jobs behind it    stay SCHEDULED
   (afterok on anything     ─► right-hand path
   still running)

   After commit: "Re-open review" is allowed only while alignment has not started.
```

**What each row state looks like:**

| Row chip | `execution_status` | Review sub-state | Row actions |
|---|---|---|---|
| — | SCHEDULED | not reached | mode selector |
| Waiting for inputs | SCHEDULED | inputs pending | mode selector |
| Waiting for review | SCHEDULED | ready | Review · Approve · Run DL (DL review) · mode → auto |
| Predicting… | SCHEDULED | predicting (predict run QUEUED/RUNNING) | Review · Cancel DL |
| Predictions ready · N flagged | SCHEDULED | predictions ready | Review · Approve · Run DL again |
| Prediction failed | SCHEDULED | prediction failed (reason in the tooltip) | Run DL · Review · Approve |
| Approved · D of T dropped | SUCCEEDED | committed | Re-open review (until alignment starts) |
| Auto · queued / running | QUEUED / RUNNING | — | — (pipeline Stop) |
| Auto · failed | FAILED | — | mode selector · Run again |

In the review modes `execution_status` answers one question — *is the verdict committed?* — and the predict
run is tracked separately (§3). In DL auto the filter is an ordinary chain job with ordinary statuses.

## 3. Data model
- `TiltFilterParams.mode: FilterMode` (`StrEnum`: `manual`, `dl_review`, `dl_auto`; UI writes through
  `enum_forward`). Editable whenever no predict run or chain job is in flight — including after a commit,
  via Re-open (fixes defect 2).
- `model` (registry key) and `threshold` on **P(bad)**. Default 0.5 (the model's own decision boundary),
  shown with an **"uncalibrated"** marker until stage 5 records a calibrated value.
- `tilt_labels`: only tilts a human touched.
- `predict_run`: `{job_dir: TiltFilter/dl_run/NNN, model, slurm_job_id, status, submitted_at, error}` — it
  has a job dir with exit sentinels, and the PipelineMonitor settles it (`reconcile_tilt_filter_predict`)
  with or without an active pipeline (fixes defect 1). No threshold: the threshold applies at review time.
- `committed`: `{at, source: manual|dl_review|dl_auto, kept, dropped, model, threshold}` — the verdict
  marker, separate from `execution_status`.
- Registry per frame: **prediction** (`p_bad`) kept apart from the **verdict** (`is_filtered_out`,
  `filter_reason`). Predicted-bad is derived as `p_bad ≥ threshold` with the job's current threshold, not
  stored, so moving the threshold during review needs no re-run. Only the commit core writes the verdict:
  `finalize_pipeline_output`, split so the driver can call it too (DL auto).
- `ProjectState.review_hold: {barrier, parked: [...]}` (schema bump).

## 4. Pipeline wiring
- **Deploy** (`pipeline_orchestrator_service.py:139-179`, `_submit_chain`):
  - filter already committed → nothing to wait for; the chain runs through;
  - Manual / DL review, not committed → submit upstream of the filter, park the rest in `review_hold`;
    parked jobs keep their allocated dirs and stay SCHEDULED;
  - DL auto → the filter is a chain job with a **synthetic edge filter → aligntiltsWarp** (alignment has no
    data edge to it: the filter has no output slots and the verdict lives in the registry).
- **`submit_parked`** (Approve): per-project lock → refuse if not committed or any producer FAILED → re-resolve
  paths/job.star/script in the allocated dirs (edits made during review apply) → afterok on producers still
  QUEUED/RUNNING → reuse `_submit_chain`'s submit/persist tail → clear `review_hold`.
- **Switch to DL auto while parked:** submit the DL chain job (afterok on in-flight upstream) and the parked
  jobs behind it; clear `review_hold`.
- **Per-project submit lock**, shared by deploy, `submit_parked` and Run DL (fixes defect 4: deploy flips
  `pipeline_active` only after the sbatch awaits, so a double Run may submit twice).
- **Commit refuses** once alignment is RUNNING or SUCCEEDED; Re-open is the explicit path before that.
- The interactive carve-outs (excluded from deploy, the reconciler's `pipeline_active` vote, Stop, recovery)
  apply only in the review modes; in DL auto the filter is a normal chain job.
- **Restart:** `review_hold`, `predict_run` and `committed` are persisted; the reconciler re-attaches to a
  running predict job; Approve works after a restart.

## 5. The job row and the panel
- **Row** (roster, `ui/pipeline_builder/pipeline_roster.py`): a compact mode selector
  `Manual · DL review · DL auto` in the house control vocabulary (no material tabs), the status chip from
  §2, and `house_button`s: Run DL, Review (opens the panel), Approve (`accent`), Re-open. Run DL is disabled
  with a tooltip until fsMotion has succeeded. The roster's `signature()` includes mode, review sub-state,
  `predict_run.status` and `committed`.
- **Panel** (`ui/tilt_filter_panel.py`): the same actions; the gallery shows P(bad), worst first, with
  predicted vs overridden styling. **Liveness banner** when predictions do not vary (std of P(bad) < 0.05
  within series): "the model gives every tilt P(bad) ≈ x — its verdicts are meaningless".

## 6. The DL job (assuming the weights work)
- **Plumbing:** torch in the cluster install's venv (the dev venv's torch 2.6.0+cu124 needs glibc ≤ 2.16,
  so it runs on the CentOS 7 nodes):
  `/groups/klumpe/software/crboost_server/venv/bin/pip install torch==2.6.0 torchvision==0.21.0 --index-url https://download.pytorch.org/whl/cu124`,
  checked with `srun -p g --gres=gpu:1 --time=0:05:00 <venv>/bin/python -c "import torch; print(torch.cuda.is_available())"`.
  Explicit `job_resource_profiles.tiltFilter` (gpu:1, 16G, 4 CPUs, 0:30:00).
- **Model registry** in conf.yaml, starting with the current file:
  ```yaml
  tilt_filter:
    models:
      tiltnet_260212:                 # current weights, under evaluation (§7)
        path: /groups/klumpe/software/Models/model260212.pth
        arch: SmallSimpleCNN
        normalisation: half          # mean 0.5 / std 0.5, 1 channel
    default_model: tiltnet_260212
  ```
  Dropdown from the registry; missing file → red marker + disabled Run DL; delete the phantom `'default'`
  path (`filterTilts/deepLearning/model_loader.py:46-47`).
- **Driver** (`drivers/tilt_filter.py`): predicting is its base behaviour (DL review) — it writes predictions
  and never touches the verdict; stage 3 adds `--commit` (DL auto), which calls the commit core afterwards.
  Store P(bad). `image_size` fixed at 384 (the network's only size). Check the model file before any conversion. Reuse the gallery PNGs (`TiltFilter/png/` — exactly
  the model's input) when present instead of re-converting MRCs. An unreadable tilt fails the run with the
  tilt named; missing CUDA fails loudly (no silent CPU).
- **Liveness in the driver:** constant output → in DL review the predictions still land and the panel shows
  the banner; in DL auto the job FAILS with that message instead of committing a meaningless verdict.

## 7. Weights and calibration (parallel track)
**The current file is a collapsed training run.** Its own record: accuracy peaks at 0.925 in epoch 11, the
learning rate blows up in epoch 12 (val loss 14.78, peak LR 0.0196), and from epoch 14 accuracy sits at
exactly 0.5294 with both losses ≈ ln 2. The saved weights are epoch 49 (BN `num_batches_tracked` = 15,500 =
50 × 310). A forward pass outside torch gave P(good) = 0.557 for every input tried — black, white, noise, a
human-"bad" −50° tilt and the 0° tilt. Expect the liveness banner (§5) until new weights arrive.

Routes, in order: **(1)** upgraded or best-epoch weights from the model author (the maintainer is asking);
**(2)** v1's model — 3 channels, ImageNet normalisation, recorded val accuracy 0.955 — converted once from
its fastai pickle (`CryoBoost/data/models/model.pkl`) into a plain state dict, with its architecture class
added; **(3)** retrain on our own labels. The registry's per-model `arch`/`normalisation` lets them coexist.

**Calibration, for whichever weights are live** (runs offline from the gallery PNGs):
- Labels: manual gallery labels in 22 projects; deduplicated **219 tilt series, 9,201 tilts, 1,078 bad**
  (412 in vitro 6,794 / 605; S2 lamellae 797 / 234; C. elegans lamellae 1,000 / 178; small sets from MyrGFP,
  MysteryLattice, Vermicelli; copia unlabelled). Bad labels sit mostly at |angle| ≥ 50°.
- Dim exposures are half the story: 595 of 599 tilts with mdoc counts below 10 % of their series mean are
  labelled bad (99.3 %) — a physics pre-label that runs regardless of the model (roadmap 22 holds the
  always-on 1 % blank rule).
- Pick one cut `t` on P(bad), cross-validated by session, on the non-dim remainder (~600 bad labels):
  **DL auto** — smallest `t` with bad-precision ≥ 0.90 pooled and ≥ 0.8 per sample type; **DL review** — `t`
  with bad-recall ≥ 0.9. Never maximise accuracy (88 % of tilts are good).
- Sanity per run: 0° tilt predicted good in ≥ 95 % of series; predicted-bad tilts sit at higher |angle|
  than predicted-good; per-session bad fraction within 4–30 %.

## 8. Defects fixed along the way

| # | Defect | Where |
|---|---|---|
| a | No barrier: alignment applies whatever verdict exists when it starts (`drivers/ts_alignment.py:188-194`) | §4 |
| b | Phantom model path; v1 method names in the dropdown | `model_loader.py:46-47`, `ui/tilt_filter_panel.py:299` |
| c | Cluster venv has no torch; no resource profile | §6 |
| d | Stored probability is the winning class (always ≥ 0.5); threshold 0.1 is a no-op | `services/jobs/tilt_filter.py:47`, `services/tilt_series/models.py:262-265` |
| e | `image_size` editable though the network only works at 384 | `services/jobs/tilt_filter.py:46` |
| f | Manual-wins broken: the post-DL reload writes every DL label into `tilt_labels` | `ui/tilt_filter_panel.py:389-397` |
| g | Probabilities never reach the gallery; Save overwrites them with 1.0 | `tilt_series_service.py:154-155` |
| h | DL dispatch polled from the click handler (dies with the tab) | `ui/tilt_filter_panel.py:357-376` |
| i | Unreadable tilt silently dropped; CUDA missing → silent CPU | `image_processor.py:172`, `model_loader.py:68-74` |
| 1 | A DL run whose tab closed stays RUNNING forever | `pipeline_runner.py:372-381, 427-429` |
| 2 | Re-running DL after a commit keeps the old threshold/model (USER_PARAMS writes dropped on SUCCEEDED) | `ui/tilt_filter_panel.py:316-318`, `services/jobs/_base.py:417-419` |
| 3 | A run started in another tab goes unseen until reload | `status_poller.py:66-79` |
| 4 | Deploy has no re-entrancy lock — a double Run may submit the chain twice (general, not filter-specific; check the Run handler's own guard first) | `pipeline_orchestrator_service.py:149` vs `:489` |
| 5 | Per-job Cancel stops the tab's timers while the rest of the chain runs | `job_tab_component.py:365-372` |

## 9. Stages

### Stage 0 — Plumbing (user-run, ~15 min)
- §6 torch install + GPU check; registry entry + resource profile in the cluster `conf.yaml`.
- *Success:* the GPU check prints `True`.

### Stage 1 — DL review without parking (the maintainer's evaluation path)
- Predict-only driver mode; `predict_run` with its own job dir, tracked by the reconciler; P(bad) in the
  gallery; `tilt_labels` = touched only; Approve = commit; liveness banner. Defects b, d, e, f, g, h, i, 1.
- Built as five commits (log and specs in §12): 1 config registry ✓ · 2 registry `p_bad` ✓ · 3 run
  lifecycle ✓ · 4 predict-only inference · 5 review gallery.
- *Success:* Run DL on a labelled 412 project → predictions for every tilt, worst first; edits survive a
  re-run; closing the tab mid-run changes nothing; with the current weights the banner appears.

### Stage 2 — Modes on the row + parking
- §3 mode + `review_hold`; §4 deploy/`submit_parked`/lock/Stop/Re-open; §5 row controls. Defects a, 2, 4.
- *Success:* Manual → Run submits upstream only, the row says "Waiting for review", the builder stays usable;
  Approve → alignment starts and logs "K kept, D dropped"; a server restart before Approve changes nothing;
  a double Run submits once.

### Stage 3 — DL auto in the chain
- Synthetic edge, `--commit` driver mode, liveness failure fails the job, Switch to DL auto from waiting.
- *Success:* with a live model the chain reaches tsReconstruct with the browser closed; with the current
  weights the DL job fails with the liveness message and downstream is cancelled, visibly.

### Stage 4 — Remaining UI defects
- Defects 3, 5.

### Stage 5 — Calibration + weights decision
- After new weights (route 1) or the fallback routes: §7 calibration; thresholds recorded here and in the
  registry; the "uncalibrated" marker goes.

### Later — out-of-distribution banner / `on_ood` auto mode
Only after DL auto has run on real data.

## 10. Runtime checklist
1. Stage 1: labelled 412 project, DL review, Run DL → gallery pre-filled; current weights → banner.
2. Stage 2: Manual → Run → segment 1 only; label; Approve → alignment starts; restart-then-Approve works.
3. Stage 3: DL auto with a live model, tab closed → chain completes; current weights → DL job FAILED with the
   reason, downstream cancelled.
4. Waiting in DL review → Switch to DL auto → DL job + remainder submitted.

## 11. Risks
1. Weights: until route 1, 2 or 3 lands the filter can only be exercised, not trusted.
2. Preprocessing parity with training rests on one comment in the author's code; calibration would expose a
   mismatch as poor agreement.
3. Labels: only 16.5 Å/px Falcon data; lamellae thin; implicit-good labels contain misses; annotators
   disagree (repeat-label 'bad' Jaccard 0.46–0.88).

## 12. Implementation log

### Stage 0 — done 2026-09-29
- **Config:** the `tilt_filter` registry and the `tiltFilter` resource profile are in `config/conf.template.yaml`;
  the dev server's `conf.yaml` has them (`tiltnet_260212` → `/groups/klumpe/software/Models/model260212.pth`,
  `SmallSimpleCNN`, `half`). The cluster install's `conf.yaml` does not yet.
- **torch:** pinned in `requirements.txt` (2.6.0 / torchvision 0.21.0). The installed 2.6.0+cu124 libraries
  require at most `GLIBC_2.16`, so they run on the CentOS 7 nodes. The dev venv has them; the cluster install's
  venv does not — `venv/bin/pip install --no-cache-dir torch==2.6.0 torchvision==0.21.0` before deploying.
- **Checks:** `torch.load(model260212.pth, map_location="cpu", weights_only=True)` works; top-level keys
  `accuracies, model_architecture, model_state_dict, optimizer_state_dict, train_losses, val_losses`. GPU check
  on `-p g --constraint="g2|g3|g4"`: `2.6.0+cu124 True Tesla V100-PCIE-32GB`.

### Stage 1 — commits
| # | Commit subject | What |
|---|---|---|
| 1 | tilt filter: model registry in conf.yaml (tilt_filter.models, default_model) and tiltFilter resource profile | `TiltFilterConfig` / `TiltFilterModelConfig`, `ConfigService.tilt_filter`, template sections |
| 2 | tilt filter: per-frame P(bad) in the registry, kept apart from the verdict | `Frame.p_bad` (registry schema 1.5); `filter_probability` dropped on load; `set_frame_filtered` carries no probability; dashboard kept/dropped readers take `committed=tilt_filter_committed(state)`; re-adding the job restores only the drops |
| — | requirements: note why torch stays at 2.6.0 | comment only |
| 3 | tilt filter: DL prediction runs in their own job dir, settled by the monitor | `TiltFilterPredictRun`, `predict_run`, `predict_in_flight`, `next_predict_run_dir`; `backend.submit_tilt_filter_predict` replaces `submit_tilt_filter_dl` (leaves execution_status alone, refuses while a run is in flight); `PipelineRunnerService.reconcile_tilt_filter_predict`; the monitor tick covers projects with an in-flight run; panel DL section above the gallery with a 3 s observer; the unused standalone panel entry is gone |
| 4 | open | predict-only inference — spec below |
| 5 | open | review gallery — spec below |

Defects fixed so far: h and 1 (commit 3); g in part (commits no longer stamp a probability, commit 2).

### Refinements to §3 / §6 made while building
- Predicted-bad is derived from `p_bad` and the job's current threshold, not stored (§3).
- `predict_run` records no threshold (§3).
- The driver predicts by default; stage 3 adds `--commit` (§6) instead of a `--predict-only` flag.
- The dashboard's kept/dropped readers need the job's committed state: a committed filter that dropped
  nothing from a TS leaves no per-frame trace. (Manual commits used to stamp `filter_probability = 1.0` on
  every tilt, which is what marked such a TS as filtered.)

### Chunk 4 — predict-only inference (next)
Files: `filterTilts/deepLearning/{model_loader,model_architectures,statistics_calculator}.py`,
`drivers/tilt_filter.py`, `services/jobs/tilt_filter.py`, `services/tilt_series/registry.py`, `backend.py`,
`ui/tilt_filter_panel.py` (DL section inputs only), `ui/tomo_dashboard_dialog.py` (param rows).
- **Params:** `model: str | None = None` (registry key; None → `default_model`), `threshold: float = 0.5`
  (`ge=0, le=1`, on P(bad)), keep `dl_batch_size`; remove `model_name`, `image_size`, `prob_threshold`,
  `prob_action`. The renames are deliberate: old keys are ignored on load, so a stored `prob_threshold: 0.1`
  (winning-class semantics) can never be read as a P(bad) cut. `USER_PARAMS = {model, threshold,
  dl_batch_size}`. One resolver (key → `TiltFilterModelConfig`; error when unconfigured, unknown or the file
  is missing) shared by the submit, the driver and the panel.
- **Loader:** `ModelLoader(model_path, arch, normalisation, gpu=0)`; normalisations `{"half":
  Normalize([0.5], [0.5])}` (unknown → error naming the known ones); CUDA required, no CPU fallback (including
  the OOM → CPU path); `torch.load(..., map_location="cpu", weights_only=True)`; the checkpoint's own
  `model_architecture` must equal the registry `arch`; `predict_p_bad(images, batch_size) -> list[float]` =
  softmax column 0 (vocab bad = 0, good = 1). Delete the phantom `'default'` path and the commented-out
  duplicate `load_model`.
- **Input size:** `SmallSimpleCNN.input_size = 384` (fc1 expects 6×6 after six 2× pools); the driver checks
  every input against it.
- **Driver:** `predict_run.model` → registry entry → load the model first (fails fast on a missing file or
  CUDA) → tilts from the fs-motion star → per tilt, the gallery PNG `<png_dir>/<Path(rlnMicrographName).stem>.png`
  (`png_dir` = `state.tilt_filter_png_dir` or `TiltFilter/png`; the thumbnails are made with
  `ImageProcessor(target_size=384)`, i.e. byte-exact model input). Tilts without one are converted into
  `<run_dir>/png/` with `ImageProcessor.batch_convert`; afterwards every tilt must have an L-mode
  input_size² PNG or the run fails naming the tilts (`batch_convert` swallows per-file errors and drops the
  Nones, so its return value must never be zipped against the tilt list). Then
  `registry.set_frame_prediction(key, p_bad)` (new; KeyError fails naming the tilt), `registry.save()`, and a
  liveness summary in the log. Never writes the verdict, never saves `project_params.json`. Worker count from
  `os.sched_getaffinity(0)`.
- **`statistics_calculator.py`:** `PredictionThresholder` / `FilterStatistics` are unused after the rewrite →
  delete (grep first).
- **Submit:** `predict_run.model` = the resolved key; refuse when unconfigured.
- **Panel DL section:** model select over the registry keys (default preselected; red marker and disabled
  Run DL when no model is configured or its file is missing); threshold on P(bad) with an "uncalibrated"
  marker, written through `numeric_forward`; batch size stays off the form.
- **Dashboard param rows** (`_render_tilt_filter_section`): model / threshold.
- **Liveness** (one function for driver and panel): live unless every series with ≥ 2 predictions has
  std(P(bad)) < 0.05; also returns the mean P(bad) for the banner. Reading "within series" as "within every
  series" keeps a clean series under a live model from raising the banner.

### Chunk 5 — review gallery (after 4)
Files: `ui/tilt_filter_panel.py`, `services/tilt_series_service.py`.
- The gallery reads `p_bad` per tilt from the registry; predicted = `p_bad ≥ job.threshold`; effective label =
  `tilt_labels.get(key)` → predicted → "good".
- Cards show P(bad); default sort "P(bad), worst first" when predictions exist; touched = solid border and
  filled dot, predicted-only = dashed border and ring dot.
- A click writes straight into `job_model.tilt_labels` (touched only) and saves with `debounce_s=1.0`, so edits
  survive a re-run, a gallery rebuild and a closed tab (defect f).
- "Save labels" becomes **Approve** (accent): `finalize_pipeline_output` on the effective labels → SUCCEEDED.
  Drop `apply_labels`' `cryoBoostDlProbability = 1.0` default once nothing reads it (defect g). Check the
  readers of `TiltFilter/tiltseries_{labeled,filtered}.star` (`pipeline_roster.py` matches the name) before
  dropping those writes.
- Liveness banner above the gallery: "The model gives every tilt P(bad) ≈ x — its verdicts are meaningless."
- Then the stage-1 success check (§9) is the user's runtime pass.

### Branch notes
- Registries saved from `dl_filter` carry `Frame.p_bad`; code without the field (`Frame` is
  `extra="forbid"`) skips those tilt-series sidecars with a warning. Until the merge, don't open one project
  from a `dl_filter` server and a server on another branch.
- The merge with `bindmounts_and_auth` (roadmap 20) meets in `services/configs/config_service.py` (`Config`
  gains `tilt_filter` here, `container_runtime` / `container_binds` there), `config/conf.template.yaml`, and
  possibly `services/scheduling_and_orchestration/pipeline_runner.py`. The DL submit launches through
  `driver_invocation`, so roadmap 20's interpreter change reaches it unchanged.
- Noticed, not fixed: `_hdr` in `ui/tilt_filter_panel.py` is dead; the dashboard's registry-gap marker reads
  "Job is running" for an unapproved (SCHEDULED) filter; defect 2 (params frozen after Approve) waits for
  stage 2.

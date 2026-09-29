# Roadmap 06 — DL tilt filter: three modes on the job row, a parked pipeline, and working weights

**Status:** rev 4, approved 2026-09-29 (rev 1 2026-08-11), **not implemented**. Next session: start at
stage 0; the decisions below are settled.

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
- `predict_run`: `{job_dir: TiltFilter/dl_run/<n>, slurm_job_id, status, model, threshold, submitted_at}` —
  it has a job dir with exit sentinels, so the reconciler tracks it like any job (fixes defect 1).
- `committed`: `{at, source: manual|dl_review|dl_auto, kept, dropped, model, threshold}` — the verdict
  marker, separate from `execution_status`.
- Registry per frame: **prediction** (`p_bad`, `predicted_bad`) kept apart from the **verdict**
  (`is_filtered_out`, `filter_reason`). Only the commit core writes the verdict: `finalize_pipeline_output`,
  split so the driver can call it too (DL auto).
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
- **Driver** (`drivers/tilt_filter.py`): `--predict-only` (DL review) writes predictions and never touches the
  verdict; `--commit` (DL auto) calls the commit core. Store P(bad). `image_size` fixed at 384 (the network's
  only size). Check the model file before any conversion. Reuse the gallery PNGs (`TiltFilter/png/` — exactly
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

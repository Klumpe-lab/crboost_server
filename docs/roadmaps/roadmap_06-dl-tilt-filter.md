# Roadmap 06 — DL tilt filter: three modes on the job row, a parked pipeline, and working weights

**Status:** rev 4, approved 2026-09-29 (rev 1 2026-08-11). **In progress on branch `dl_filter`:** stage 0
done; stage 1 done (commits 1–7) and runtime-checked except two items (§12 "Stage 1 runtime pass"). The shipped
weights are verified dead (§7.1); the requests to the model author are in §7.2 (sent 2026-09-30). Stage 2 is
built as commits 8–9 (§12) and has not run. Route 1 weights arrived 2026-09-30 — the model author's ResNet-18
(§7.4), converted and registered by commit 10, not yet checked on our data. Commits 11–13 (stages 3–5) are
specced in §12. The decisions below are settled.

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

### 7.1 Diagnosis of `model260212.pth` — verified 2026-09-29
**The file is the last epoch of a training run that diverged; it answers P(bad) 0.4427 for any input, and no
threshold, recalibration or BatchNorm recomputation can recover it.** A different checkpoint is the only fix.

Evidence: `docs/reports/dl-tilt-filter/scripts/check_tiltnet.py` (standalone, torch only, CPU so two machines
print the same numbers); full output of the run in `docs/reports/dl-tilt-filter/check_tiltnet_model260212.txt`.
Epochs are counted from 1 (an earlier version of this section counted from 0).

| What | Finding |
|---|---|
| File | 536,430,306 bytes, sha256 `88cada4274fc5cb8cb0b07b4bae06ebf9c646d58072e8fbdc11e3067096362bd`; keys `accuracies, model_architecture, model_state_dict, optimizer_state_dict, train_losses, val_losses`; `SmallSimpleCNN`, 44,698,050 parameters; no NaN/Inf |
| Optimizer | Adam-type param group on a one-cycle schedule: max_lr 0.01957 (start 7.8e-4 = max/25, end 7.9e-8), momentum 0.85–0.95, weight decay 0.0781 |
| Training record | epochs 1–12 healthy (val accuracy 0.85–0.925; best **0.925 at epoch 12**, val loss 0.34) · **epoch 13: val loss 14.78**, accuracy 0.70 · epoch 14: train loss 0.697 · **from epoch 15: accuracy exactly 0.5294** (epoch 21 alone reads 0.64) **and both losses ≈ ln 2** (0.691–0.694; train loss 0.79, 0.81, 1.20 at epochs 15, 20, 21) · epochs 22–50 identical |
| Which weights | 15,500 optimizer steps = 50 epochs × 310: the weights after epoch 50, not the best epoch |
| Weight magnitudes | every tensor tiny: conv/fc weights mean \|w\| 1.5e-5 to 6e-4, BN gammas 1e-4 to 8e-4, BN running variances 1.2e-5 (bn1) down to 6e-11 (bn3, bn4); only `fc4.bias` = [−0.1151, +0.1151] is sizeable |
| Forward pass (eval) | black, grey, white, uniform noise, Gaussian noise, a 32 px checkerboard, a ramp, and two gallery tilts of `agg_20260311_412_Grid3` (Position 9 at 0° and a near-blank −70°): every one gives logits exactly `fc4.bias` → P(bad) 0.4427, P(good) 0.5573; spread 0 |
| Where it dies | spread across inputs falls layer by layer — 3.8e-4 after conv1, 6.5e-7 after conv3, 2.3e-12 after conv6 — and **fc3+ReLU is 100 % zeros for every input**, so the logits are `fc4.bias` |
| BatchNorm | with batch statistics (train mode, dropout off) the output is identical: the learned weights are the cause, not the stored running statistics |
| In our pipeline | DL run 001 on the same project (697 tilts, Quadro RTX 6000): P(bad) 0.443 for every tilt, liveness banner shown |

Reading (inference from the above, not recorded in the file): the one-cycle learning rate, climbing toward
0.0196, blew the run up at epoch 13; the ReLUs died, so no gradient reached the weights, and weight decay then
shrank them through the remaining 36 epochs — which fits the ~1e-4 magnitudes (the checkpoint cannot tell AdamW from
Adam + L2). What is left is a bias-only classifier: `fc4.bias` encodes a class prior of P(good) 0.557, and 0.5294
is what answering "good" for everything scores on the validation set. Expect the liveness banner (§5) until
other weights are registered.

### 7.2 Requests to the model author
Drafted 2026-09-29, to go with the script and its output:
1. Run `check_tiltnet.py` on their copy of the file (same sha256?) and send the output.
2. The epoch-12 checkpoint if it was kept; otherwise one from a run that did not diverge (lower max_lr, saved at
   the best validation loss), checked with the script first — its verdict must read "The output depends on the
   input."
3. Confirm the inference contract: 384×384 Fourier-cropped, min-max scaled to 8 bits, one channel, ToTensor then
   Normalize(0.5, 0.5), class 0 = bad and 1 = good.
4. How the validation split was made (whole tilt series held out, or random tilts), and the training data:
   pixel size, detector, sample types, bad/good ratio.

**Accepting new weights:** run `check_tiltnet.py` on the file (verdict "The output depends on the input."),
register it in `conf.yaml` (`tilt_filter.models`, §6), Run DL on `agg_20260311_412_Grid3` — the banner must be
gone and its 56 human-bad tilts should sit near the top of the worst-first sort — then stage 5 calibration.
A checkpoint from the author's `trainTiltCNN_ResNet18` scripts is converted first
(`python -m filterTilts.deepLearning.convert_checkpoint <in> <out>`, which prints the conf.yaml entry) and checked
with `check_resnet_tiltnet.py`, which reproduces the author's inference, instead of `check_tiltnet.py`.

### 7.3 Routes and calibration
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

### 7.4 Route 1: the model author's ResNet-18 — received 2026-09-30
In `new_weights/` (untracked; the checkpoint stays out of git): the author's `trainTiltCNN_ResNet18_Optuna1034.py`,
`predictTiltCNN.py`, `submit_inference_ResNet_optuna1034.sh` and `run_best.pth` (sha256 `52a860e9…3cf25d`).

| What | Finding |
|---|---|
| Weights (decoded, not run) | ResNet-18 with a one-channel stem and a two-class head, 11.18 M parameters. Magnitudes of a trained network: first conv mean \|w\| 0.17, BatchNorm scales 0.26–1.85, running variances 0.02–2.6; the final layer's weights matter as much as its bias; 8 of 64 first-layer channels dead. The file is the best epoch (8), not the last. None of §7.1's signs of a dead run |
| Inference contract (stored in the checkpoint) | grayscale PNG resized to 224 px, standardised per image and clipped at ±7.2146σ; logits averaged over the 8 rotations/flips and divided by temperature 0.739; softmax, bad = 0. The author's cut: P(good) ≥ 0.699 is good, i.e. P(bad) ≥ 0.301 is bad |
| Validation | `split_mode=provided`: the validation datasets are also training datasets, and the script's own docstring says only a dataset-level holdout answers "does it work on the next grid". Model choice, cut and temperature were fitted on that same set. Reported balanced accuracy 0.976 and AUROC 0.998 are within-dataset numbers |
| Training data | 10 datasets (GroEL, two C. elegans, Chlamy, Drosophila, five K3), none of ours; 93 % of the validation bad tilts come from two of them; bad-tilt recall 0.42 on one K3 set and 0.69 on Drosophila |

Converted with `convert_checkpoint.py` to `/groups/klumpe/software/Models/tiltnet_resnet18_o1034.pth` and
registered as `tiltnet_resnet18_o1034` (`threshold: 0.301`) in the dev `conf.yaml` (chunk 10). The check on our
data, on a GPU node:
```
srun -p g --gres=gpu:1 --constraint="g2|g3|g4" --cpus-per-task=4 --mem=16G --time=0:20:00 venv/bin/python docs/reports/dl-tilt-filter/scripts/check_resnet_tiltnet.py new_weights/run_best.pth --project /groups/klumpe/crboost_data/agg_20260311_412_Grid3 --device cuda --csv docs/reports/dl-tilt-filter/grid3_resnet_run_best.csv > docs/reports/dl-tilt-filter/check_resnet_run_best.txt
```
It passes when it prints "The output depends on the input.", ranks Grid3's 56 committed-bad tilts near the top (high
AUROC), gives P(bad) rising with |angle| and the 0° tilts good. Then the in-app check under chunk 10 in §12.

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
  lifecycle ✓ · 4 predict-only inference ✓ · 5 review gallery ✓.
- *Success:* Run DL on a labelled 412 project → predictions for every tilt, worst first; edits survive a
  re-run; closing the tab mid-run changes nothing; with the current weights the banner appears.
- *Status 2026-09-29:* met on `agg_20260311_412_Grid3` except "closing the tab mid-run" and an Approve, both
  still to run (§12).

### Stage 2 — Modes on the row + parking
- §3 mode + `review_hold`; §4 deploy/`submit_parked`/lock/Stop/Re-open; §5 row controls. Defects a, 4
  (defect 2 was fixed in stage 1, §12 commit 6; `mode` stays editable after a commit the same way).
- Built as two commits (specs and refinements in §12): 8 row controls + one commit path · 9 parking.
- *Success:* Manual → Run submits upstream only, the row says "Waiting for review", the builder stays usable;
  Approve → alignment starts and logs "K kept, D dropped"; a server restart before Approve changes nothing;
  a double Run submits once.
- *Status 2026-09-30:* built (commits 8–9, ruff-clean), not run; the runtime pass is in §12 under chunk 9.

### Stage 3 — DL auto in the chain
- Synthetic edge, `--commit` driver mode, liveness failure fails the job, Switch to DL auto from waiting.
- One commit (11), specced in §12.
- *Success:* with a live model the chain reaches tsReconstruct with the browser closed; with the current
  weights the DL job fails with the liveness message and downstream is cancelled, visibly.

### Stage 4 — Remaining UI defects
- Defects 3, 5. One commit (12), specced in §12. Chunk 9 already keeps the refresher alive past a pipeline's end.

### Stage 5 — Calibration + weights decision
- After new weights (route 1) or the fallback routes: §7 calibration; thresholds recorded here and in the
  registry; the "uncalibrated" marker goes.
- Route 1 arrived (§7.4) and runs since commit 10; the calibration is one commit (13), specced in §12.

### Later — out-of-distribution banner / `on_ood` auto mode
Only after DL auto has run on real data.

## 10. Runtime checklist
1. Stage 1: labelled 412 project, DL review, Run DL → gallery pre-filled; current weights → banner.
2. Stage 2: Manual → Run → segment 1 only; label; Approve → alignment starts; restart-then-Approve works.
3. Stage 3: DL auto with a live model, tab closed → chain completes; current weights → DL job FAILED with the
   reason, downstream cancelled.
4. Waiting in DL review → Switch to DL auto → DL job + remainder submitted.

## 11. Risks
1. Weights: until route 1, 2 or 3 lands the filter can only be exercised, not trusted — the shipped file is
   verified dead (§7.1).
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
| 4 | tilt filter: predict-only DL runs write P(bad) per tilt from the registered model | params `model` / `threshold` (on P(bad), 0.5) / `dl_batch_size`; `resolve_model` + `prediction_liveness` in `services/jobs/tilt_filter.py`; `ModelLoader(path, arch, normalisation)` GPU-only with `predict_p_bad`; `SmallSimpleCNN.input_size = 384`; `registry.set_frame_prediction`; driver predicts from the gallery PNGs, converts the rest, never writes the verdict; `statistics_calculator.py` deleted; panel model select from the registry with a red marker, threshold with an "uncalibrated" marker; dashboard rows model / threshold |
| 5 | tilt filter: review gallery shows P(bad), keeps only the labels a human set, and commits on Approve | gallery reads `p_bad` from the registry; effective label = human label → prediction at the job's threshold → good; worst-first sort, dashed/ring = predicted, solid/filled = human; clicks write `tilt_labels` with a 1 s debounced save; threshold field in the gallery row, labels re-derive live; Approve (refused while a run is in flight) replaces Save; liveness banner; `apply_labels` / `filter_good_tilts` / `write_tilt_series` deleted |
| 6 | tilt filter: model, threshold and batch size stay editable after a commit | `TiltFilterParams.USER_PARAMS` is empty: that set freezes a field once the job leaves SCHEDULED/FAILED, and this job's SUCCEEDED only means a verdict is committed; the model select and the threshold force their own debounced saves |
| 7 | tilt filter: checkpoint diagnostic script and the verified weights diagnosis | `docs/reports/dl-tilt-filter/scripts/check_tiltnet.py` + its output on `model260212.pth`; §7 rewritten around it |

Defects fixed so far: h and 1 (commit 3); g in part (commits no longer stamp a probability, commit 2);
b, d, e, i (commit 4); f, g (commit 5); 2 (commit 6). Stage 1's defect list is closed.

### Stage 1 runtime pass
- **2026-09-29, `agg_20260311_412_Grid3`** (17 TS, 697 tilts, 57 human labels, filter committed earlier):
  run 001 on a Quadro RTX 6000 read the gallery PNGs and wrote P(bad) = 0.443 for every tilt; the log and
  the panel both raised the liveness warning. The dead-weights diagnosis (§7) holds on real data. With the
  threshold at 0.5 nothing is predicted bad, and the committed filter pinned the threshold field at 0.50
  (defect 2) → commit 6. After it (user-confirmed): threshold 0.40 turns every card dashed, 0.50 restores
  them; a click turns a card solid; a DL re-run keeps the clicks.
- **Still to check:** closing the tab mid-run (the run lands and the gallery shows it on reopen), and Approve —
  on a copy of a project, since Approve re-commits the verdict. With the current weights keep the threshold
  above 0.443: below it every untouched tilt is predicted bad, and Approve would commit all of them as bad.
  From commit 8 on, Approve is refused once alignment has run (§4), so the copy needs alignment not yet run
  (or its alignment job deleted); Grid3 itself now answers Approve with that reason.
- **Weights diagnostic for the model author:** `docs/reports/dl-tilt-filter/scripts/check_tiltnet.py` —
  standalone (torch, plus Pillow for PNGs), CPU by default so two machines print the same numbers. Prints the
  checkpoint's sha256 and stored training record, P(bad) for synthetic images and given gallery PNGs, the layer
  where different inputs stop differing, and the same pass with BatchNorm on batch statistics (running stats
  vs weights). Run 2026-09-29 (CPU, torch 2.6.0) on `model260212.pth` with the 0° and −70° Position 9 tilts of
  `agg_20260311_412_Grid3`: output in `docs/reports/dl-tilt-filter/check_tiltnet_model260212.txt`, diagnosis
  in §7.1, requests to the model author in §7.2. Usage:
  `python check_tiltnet.py <weights.pth> [384x384 grayscale tilt PNGs …] > out.txt`.

### Refinements to §3 / §6 made while building
- Predicted-bad is derived from `p_bad` and the job's current threshold, not stored (§3).
- `predict_run` records no threshold (§3).
- The driver predicts by default; stage 3 adds `--commit` (§6) instead of a `--predict-only` flag.
- The dashboard's kept/dropped readers need the job's committed state: a committed filter that dropped
  nothing from a TS leaves no per-frame trace. (Manual commits used to stamp `filter_probability = 1.0` on
  every tilt, which is what marked such a TS as filtered.)
- Chunk 4: `predict_p_bad` takes PNG paths that a `DataLoader` streams, not PIL images, so a 9,000-tilt
  project never holds every input in memory.
- Chunk 4: a gallery PNG that is not an L-mode 384² image counts as absent and is converted like a missing one;
  only a tilt that still has no usable input after conversion fails the run.
- Chunk 4: tilts unknown to the registry are collected and named together, and nothing is saved when any is
  unknown (a partial set of predictions would read as a complete run).
- Chunk 4: the checkpoint must be a dict holding `model_architecture` and `model_state_dict`; a bare state
  dict is refused because its architecture cannot be checked. Route 2's conversion (§7) writes that format.
- Chunk 4: a model picked in the panel is stored on the job; an untouched job keeps `model = None` and runs
  the site's `default_model` of the moment. `predict_run.model` records the key that actually ran.
- Chunk 4: `prediction_liveness` returns None (not assessed) when no series has two predictions.
- Chunk 5: the threshold field sits in the gallery's action row (shown once predictions exist), not in the
  DL section: it moves the gallery's labels, which re-derive as it changes, and the DL section is collapsed
  by default.
- Chunk 5: Approve refuses while a prediction run is in flight — it would commit against predictions about to
  change, and its registry save would race the driver's.
- Chunk 5: `TiltFilter/tiltseries_{labeled,filtered}.star` are no longer written. Their only reader was the
  roster's name match in `_remove_interactive_job`, which finds nothing now that the job has no output slots.
- Chunk 5: a human's good label shows a filled grey dot; an untouched good tilt shows none (every card used to
  carry a white ring, which would read as the predicted-bad ring).
- Chunk 5, fixed on the way: the first gallery build landed outside its container, so the reload after a DL
  run appended a second gallery; a group's "−N" count counted a stale copy and, once hidden at zero, never
  came back (`.style()` merges, so the display had to be written explicitly).

### Chunk 4 — predict-only inference (done)
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

### Chunk 5 — review gallery (done)
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

### Stage 2 — commits
| # | Commit subject | What |
|---|---|---|
| 8 | tilt filter: Manual / DL review switch on the job row, one Approve path, Re-open | chunk 8 below (committed 2026-09-30, ruff-clean, not run) |
| 9 | tilt filter: the pipeline parks at an unapproved filter; Approve submits the rest | chunk 9 below (2026-09-30, ruff-clean, not run) |

### Chunk 8 — the switch on the row, one commit path, Re-open (no parking yet)
Files: `services/jobs/tilt_filter.py`, `services/tilt_series_service.py`, `backend.py`, `ui/tilt_filter_panel.py`,
`ui/pipeline_builder/pipeline_roster.py`, `ui/pipeline_builder/pipeline_builder_panel.py`.
- **Params:** `FilterMode` (`StrEnum`: `manual`, `dl_review`; stage 3 adds `dl_auto`); `TiltFilterParams.mode`,
  default `manual`. `last_commit: TiltFilterCommit | None` = `{at, mode, kept, dropped, model, threshold}`, the
  record behind the row's "Approved · D of T dropped"; SUCCEEDED stays the one committed flag (the dashboard
  reads it).
- **One commit path:** `commit_verdict(state, project_path, instance_id)` in `services/jobs/tilt_filter.py`.
  Refuses while a prediction run is in flight, and once an alignment job is RUNNING or SUCCEEDED (it has
  consumed the verdict). Loads the fs-motion star (`fs_motion_star` in `tilt_series_service`, which the panel
  uses too), derives each tilt's label with `effective_label` (a human label wins; in DL review the prediction
  at the threshold; else good; manual ignores predictions), runs `finalize_pipeline_output`, sets SUCCEEDED and
  `last_commit`. `backend.approve_tilt_filter` = that + save; the panel's and the row's Approve both call it.
- **Re-open:** `reopen_review` / `backend.reopen_tilt_filter`: SUCCEEDED → SCHEDULED and `last_commit` cleared;
  the registry keeps the verdict until the next Approve re-stamps it. Refused once alignment is QUEUED, RUNNING
  or SUCCEEDED.
- **Cancel DL:** `backend.cancel_tilt_filter_predict` scancels the in-flight run; the monitor settles it FAILED
  from sacct ("SLURM ended the job: CANCELLED").
- **Row:** under the tilt-filter row, a controls line that is its own `FingerprintedView` with a 3 s observer
  (the roster only repaints on the status poller's tick, which stops when a pipeline finishes; the chip must
  still move when a DL run lands). `Segmented` Manual · DL review, the §2 chip, and `house_button`s: Run DL
  (DL review, fsMotion done, no run in flight), Cancel DL (run in flight), Approve (accent; fsMotion done, not
  committed, no run in flight), Re-open (committed, alignment not started). A mode switch is refused while a run
  is in flight and re-renders the open panel (`PipelineBuilderPanel.rerender_job`). The handlers'
  `SingleFlight` lives on the roster, which outlives the controls line.
- **Panel:** manual hides the DL section and ignores predictions (no P(bad) sort, threshold field, dashed cards
  or liveness banner); Approve goes through the backend.
- *Check (user):* Grid3 → DL review brings its predictions back; Approve there is refused with the reason
  (alignment SUCCEEDED); on a project whose alignment has not run, Approve → "Approved · D of T dropped",
  Re-open → "Waiting for review"; Run DL, then Cancel DL → "Prediction failed" with the reason.
- *Built:* the controls line is `ui/pipeline_builder/tilt_filter_row.py`. The panel's own star lookup and
  P(bad) read moved into the service (`fs_motion_star`, `predictions_for`), so the gallery and Approve read the
  same star and predictions. Run DL on the row reports an unconfigured model when clicked (the panel's DL
  section keeps its red marker). The "Approved" chip's tooltip says whether alignment has taken the verdict.

### Chunk 9 — parking
Files: `services/project_state.py`, `services/scheduling_and_orchestration/pipeline_orchestrator_service.py`,
`services/scheduling_and_orchestration/pipeline_runner.py`, `backend.py`,
`ui/pipeline_builder/pipeline_builder_panel.py`, `ui/pipeline_builder/status_poller.py`,
`ui/pipeline_builder/pipeline_roster.py`.
- **State:** `ProjectState.review_hold: ReviewHold | None` = `{barrier, parked, held_at}`; schema 3.9; restored
  explicitly in `load()`.
- **Deploy (afterok):** clears an old hold. When the run holds an unapproved tilt filter and includes alignment,
  alignment and everything downstream of it in the run (closure over `resolve_edges`) park; the rest is
  submitted; the hold is recorded. Nothing left to submit → ok with `waiting_for_review`, no chain. The schemer
  path refuses such a run with the reason.
- **`submit_parked`** (Approve): refuses unless the barrier is SUCCEEDED; drops parked ids that left the pipeline
  or already ran; refuses, keeping the hold, when a producer of a parked job is neither done, live nor parked,
  and names it; clears the hold; runs `_submit_chain` on the parked set. `_submit_chain` gains afterok on
  producers outside the submitted set that are still QUEUED/RUNNING.
- **Lock:** one `asyncio.Lock` per project around deploy and `submit_parked` (defect 4); the Run handler gets a
  `SingleFlight`.
- **Stop** clears the hold (both branches of `stop_and_cleanup`).
- **Approve** calls `submit_parked` when the hold's barrier is this filter. The commit stands when the resume
  is refused; the UI reports both.
- **UI:** the Run handler reports a parked run; the poller says the pipeline waits for the review instead of
  "finished", and keeps its refresher running once a pipeline winds down (it stopped, so a resume from Approve
  went unseen until reload); the row chip adds "K waiting".
- *Check (user):* the stage-2 success check in §9, on an afterok project whose alignment has not run, filter in
  Manual and unapproved:
  1. Run → "Pipeline started …; K job(s) wait for the tilt-filter review" (or, with everything upstream done,
     "Nothing to start: K job(s) wait …" and nothing submitted); the row reads "… · K waiting"; alignment and
     everything after it stay pending with no job dir. When the upstream ends: "The pipeline waits for the
     tilt-filter review", and the builder is editable again.
  2. Approve on the row → "Approved: …" and "K parked job(s) submitted"; within 3 s, without a reload, the run
     slot shows Stop and alignment reads Queued. Approving while tsImport still runs: the event log's
     "Queued <alignment> -> SLURM … (afterok=[…])" names tsImport's SLURM id.
  3. Restart the server between Run and Approve: the chip still says "K waiting", and Approve submits them.
  4. Double-click Run: one submit.
  5. Stop while the upstream runs: "K waiting" goes; Approve then only commits.
  6. A schemer project (`use_afterok_orchestrator` off) with an unapproved filter: Run is refused with the reason.
- *Anchors (as of commit 8):*
  - `pipeline_orchestrator_service.py`: `deploy_and_run_scheme` :139 (guard :149, interactive skip :164, afterok
    branch :178). Wrap it as `async with lock: return await self._deploy_locked(...)` rather than re-indenting
    it. `_submit_chain` :311 drops every edge whose producer is outside the submitted set (:449). That is where
    the in-flight producers get their afterok (`after_ids` :465). It sets `pipeline_active` and saves (:489).
  - `pipeline_runner.py`: `reconcile_afterok` :404 tracks every non-terminal job that has a `slurm_job_id`, so
    jobs `submit_parked` sends are tracked with no change there; the `pipeline_active` vote is at :523.
    `stop_and_cleanup` :1203 has two branches: the afterok one ends at :1239, the schemer one at :1331.
  - `status_poller.py:66`: the active→inactive branch stops every timer and says "Pipeline execution finished."
    (:75). `rebuild_pipeline_ui` (`pipeline_builder_panel.py:524`) restarts the status timer only while running
    (:563). `handle_run_pipeline` :571 treats any success as a started run.
  - `project_state.py`: `SCHEMA_VERSION` :51; `pipeline_active` :746; `load()` restores field by field; the
    `protocol_origin` restore at :1342 is the pattern for `review_hold`.
  - `backend.approve_tilt_filter` :285 gets the `submit_parked` call. `resolve_edges`
    (`path_resolution_service.py:285`) gives the closure. A job that has not run still enters the producer index
    through `relion_job_name` / `job_path_mapping`, so a parked alignment resolves tsImport's output while
    tsImport runs.
  - Row: `tilt_filter_row.py` `_review_status` :58 and `signature` :104 take the hold. The interactive-job
    header tooltip (`job_tab_component.py:256`) still says "commit, then run/resubmit".
  - Recovery (`pipeline_monitor._recover_one`) skips afterok projects, and the hold lives in ProjectState, so a
    restart needs nothing new.
- *Built:* `ReviewHold` in `project_state.py`; `review_barrier` (the unapproved filter of a run) in
  `services/jobs/tilt_filter.py`; `_parked_behind_review` and `submit_parked` in the orchestrator.
  - The deploy records the hold before the submit awaits anything, so an Approve that lands mid-submit finds it
    and its `submit_parked` waits on the lock; the hold is dropped again when nothing reached SLURM. With no hold
    (a second tab approving), `submit_parked` is ok with nothing submitted.
  - Approve's result carries `resume` (the `submit_parked` outcome); the row and the panel report it through
    `notify_resume` (`tilt_filter_row.py`).
  - The refresher: the Run handler made the status timer inside the run slot, so the next run-slot rebuild
    deleted it, and the wind-down branch never made another. `StatusPoller.start()` makes it in the slot the
    builder is built in; the wind-down branch, the Run handler and the rebuild tail use it.
  - The chip's "· K waiting" tooltip says since when. After a refused resume the filter reads "Approved · … · K
    waiting", and the tooltip says to Run.
  - Stop leaves parked jobs as they are: SCHEDULED with no SLURM id, outside the afterok branch's live set.
  - The interactive-job header tooltip says a Run holds alignment until Approve.
  - Not covered: with the pipeline idle and a hold in place there is no Stop button. Run (a fresh deploy),
    Approve, or removing the parked jobs from the pipeline ends the hold. Stop and per-job Cancel still stop the
    refresher (defect 5, stage 4).

### Refinements to §2–§5 made while planning stage 2
- The mode switch lives on the row only; the panel follows it.
- Default mode `manual`: it needs no model, and the shipped one is dead. DL review is one click on the row.
- Only alignment and what depends on it park; jobs off that path (tsImport) run during the review. §2 said
  "submit upstream of the filter; park the rest".
- Parked jobs get their job dirs when Approve submits them, not at deploy (§4): nothing needs the numbers
  earlier, and the roster shows a job with a dir as `name (jobNNN)`, which would read as a job that ran.
- Parking is afterok-only; the schemer path refuses a run that would park (the shipped conf.yaml runs afterok).
- Re-open is also refused while alignment is QUEUED: a queued alignment job starts without waiting for a review.
- The submit lock covers deploy and `submit_parked`. Run DL keeps its own guard (the run is recorded before the
  first await).
- `last_commit` is a record for the chip; SUCCEEDED stays the committed flag.

### Stages 3–5 and the route-1 weights — commits
| # | Commit subject | What |
|---|---|---|
| 10 | tilt filter: run the model author's ResNet-18 (converter, per-model input transform and threshold) | chunk 10 below (2026-09-30, ruff-clean, not run) |
| 11 | tilt filter: DL auto runs the filter as a chain job that commits its own verdict | chunk 11 below (spec) |
| 12 | pipeline builder: Stop and per-job Cancel keep the status refresher | chunk 12 below (spec) |
| 13 | tilt filter: calibrate the P(bad) cuts on our labelled projects | chunk 13 below (spec) |

### Chunk 10 — the route-1 ResNet weights
Files: `filterTilts/deepLearning/{model_architectures,model_loader,convert_checkpoint}.py`, `drivers/tilt_filter.py`,
`services/configs/config_service.py`, `config/conf.template.yaml`, `services/jobs/tilt_filter.py`, `backend.py`,
`ui/tilt_filter_panel.py`.
- `ResNet18Gray`: torchvision's ResNet-18 with a one-channel stem and `fc = Sequential(Dropout, Linear(512, 2))`,
  the author's layout, so the state dict loads unchanged; `input_size = 224`.
- `convert_checkpoint.py`: the author's checkpoint → ours: `model_architecture`, `model_state_dict`, `clip_sigma`,
  `temperature`, `tta`, and `source` {file, sha256, threshold_p_good}. It loads the source with
  `weights_only=False` (its config holds numpy scalars), refuses another architecture, input size or a missing
  clip, re-loads its output with `weights_only=True`, and prints the conf.yaml entry with the cut as P(bad).
- `ModelLoader`: every model reads the 384² gallery PNG (`GALLERY_PNG_SIZE`), resized when the network was trained
  at another size; normalisation `half` or `per_image` (clipped at the checkpoint's `clip_sigma`); temperature and
  the 8-fold rotation/flip averaging come from the checkpoint (defaults 1 and off, so `model260212.pth` runs as
  before).
- Driver: its inputs are the gallery PNGs, and a missing one is converted at `GALLERY_PNG_SIZE`; the log names the
  input size, temperature and averaging.
- `TiltFilterModelConfig.threshold`: the optional P(bad) cut of a model. A new job starts from the default model's
  cut. The first run of another model moves the job's threshold to that model's cut, at Run DL rather than at the
  select, because picking a model leaves the old model's predictions on screen; re-running the same model keeps a
  tuned threshold. The gallery's "uncalibrated" marker shows only when the predictions' model records no cut; the
  field's hint names a recorded one.
- Done by hand 2026-09-30: `run_best.pth` converted to `/groups/klumpe/software/Models/tiltnet_resnet18_o1034.pth`
  (clip 7.2146, temperature 0.739, averaging on, cut 0.301); the dev `conf.yaml` registers it. `default_model`
  stays `tiltnet_260212` until chunk 13 decides.
- *Check (user):* the §7.4 command. Then Grid3 → DL review → model `tiltnet_resnet18_o1034` → Run DL: the threshold
  reads 0.30 after the run is submitted, the liveness banner is gone, and the human-bad tilts sort to the top.

### Chunk 11 — DL auto (stage 3), spec
Files: `services/jobs/tilt_filter.py`, `drivers/tilt_filter.py`,
`services/scheduling_and_orchestration/{pipeline_orchestrator_service,pipeline_runner}.py`,
`ui/pipeline_builder/tilt_filter_row.py`, `ui/tilt_filter_panel.py`.
- `FilterMode.DL_AUTO`; the row's switch gains "DL auto"; `effective_label` takes predictions in DL auto as in DL
  review (human labels still win).
- The filter is a chain job only in DL auto. Make `IS_INTERACTIVE` mode-dependent on `TiltFilterParams` (an instance
  property, False in DL auto), so the deploy skip, reconcile_afterok's Pass-4 vote and Stop treat a DL-auto filter
  as a chain job with no per-site change; the class-level read in `_find_existing_interactive` keeps the singleton.
  Check first that every other read is instance-level.
- Deploy (afterok only; the schemer path refuses DL auto with the reason): an unapproved DL-auto filter is
  submitted like any job (`External/jobNNN`, supervisor `drivers/tilt_filter.py --commit`, afterok on fsMotion
  through its data edge), plus a synthetic edge filter → every alignment in the submit set, since no data edge
  exists (the verdict lives in the registry). Nothing parks in DL auto.
- Driver `--commit`: the model is `job_model.model` or the default (no `predict_run`); it predicts into the
  registry as now. A dead model (the liveness test) fails the job with the §6 message and writes no verdict.
  Otherwise it commits: effective labels at the job's threshold → `finalize_pipeline_output` (asyncio.run) →
  `commit.json` {kept, dropped, model, threshold} in the job dir → the SUCCESS marker. It still never writes
  project_params.json.
- reconcile_afterok: on a tilt filter's SUCCEEDED transition, `last_commit` from its `commit.json` (a side effect
  like Pass 5's thumbnails).
- Switch to DL auto while parked: with a hold on this filter, the switch calls `submit_parked` with the filter
  included (the synthetic edge puts it ahead of the parked jobs; afterok on in-flight upstream).
  `submit_parked` accepts a DL-auto filter as the barrier.
- Row: "Auto · queued / running / failed (reason in the tooltip) / done · D of T dropped"; no Approve in DL auto;
  Re-open as now, while alignment has not started.
- *Check (user):* §9 stage 3's success line; with `model260212.pth` the failure path, with the ResNet the chain.

### Chunk 12 — the refresher survives Stop and per-job Cancel (stage 4, defects 3 and 5), spec
Files: `ui/pipeline_builder/pipeline_builder_panel.py`, `ui/pipeline_builder/job_tab_component.py`.
- Stop: `poller.start()` after the rebuild, so a Run in another tab or an Approve shows here.
- Per-job Cancel (`job_tab_component.py` `_handle_cancel`): no `stop_all_timers()` and no
  `set_pipeline_running(False)`. On the afterok path the rest of the chain goes on (`cancel_job` leaves
  `pipeline_active` to reconcile_afterok), and the poller's wind-down branch flips the tab when the pipeline
  really ends; on the schemer path `cancel_job` clears `pipeline_active` itself, which the poller sees within 3 s.
- *Check (user):* cancel one job of a running chain → the rest keeps updating; Stop, then Run in another tab →
  this tab shows it running without a reload.

### Chunk 13 — calibration and the weights decision (stage 5), spec
Files: `docs/reports/dl-tilt-filter/scripts/calibrate_tiltnet.py` (new), `config/conf.template.yaml`, this roadmap.
- Offline on a GPU node, for one registered model: find the labelled projects under a base dir (a committed
  verdict in `registry/tilt_series/*.json`), dedupe tilt series across projects by the mdoc key, set dim exposures
  aside (mdoc counts below 10 % of the series mean: a physics label, not the model's job), predict through
  `ModelLoader` (production preprocessing, byte for byte), and pick the cuts per §7.3, cross-validated by session:
  DL review, bad-recall ≥ 0.9; DL auto, the smallest cut with bad-precision ≥ 0.90 pooled and ≥ 0.8 per sample
  type. Print §7.3's sanity checks; write `docs/reports/dl-tilt-filter/calibration_<model>.txt` with the project
  list (the inventory behind §7.3's counts was never recorded).
- Registry: one cut per mode (`threshold` for review, `threshold_auto` for DL auto), decided with the numbers in
  hand.
- The weights decision: if the ResNet holds up, `default_model: tiltnet_resnet18_o1034` in the template and the
  cluster conf.yaml, where the model and torch also need installing (§12 stage 0).

### Branch notes
- Registries saved from `dl_filter` carry `Frame.p_bad`; code without the field (`Frame` is
  `extra="forbid"`) skips those tilt-series sidecars with a warning. Until the merge, don't open one project
  from a `dl_filter` server and a server on another branch. `agg_20260311_412_Grid3` already carries `p_bad`
  (DL run 001), so a server on another branch skips all 17 of its tilt series until the merge.
- History: chunk 3 landed as two commits with the same subject, 13 s apart — a split, not a duplicate:
  `7ecc924` (`backend.py`, `services/jobs/tilt_filter.py`, `pipeline_runner.py`) and `b1bd2e7`
  (`pipeline_monitor.py`, `ui/tilt_filter_panel.py`). `7ecc924` alone is an incomplete state, which matters only
  when bisecting. Commit 9 landed the same way: two commits with the same subject, 11 s apart; every chunk-9 file
  was staged.
- The merge with `bindmounts_and_auth` (roadmap 20) meets in `services/configs/config_service.py` (`Config`
  gains `tilt_filter` here, `container_runtime` / `container_binds` there), `config/conf.template.yaml`, and
  possibly `services/scheduling_and_orchestration/pipeline_runner.py`. `SCHEMA_VERSION` is 3.9 here
  (`review_hold`); if that branch bumps it as well, the merge renumbers one of them. Code without the field ignores
  the `review_hold` key on load (field-by-field `load()`). The DL submit launches through
  `driver_invocation`, so roadmap 20's interpreter change reaches it unchanged.
- Noticed, not fixed: `_hdr` in `ui/tilt_filter_panel.py` is dead; the dashboard's registry-gap marker reads
  "Job is running" for an unapproved (SCHEDULED) filter; `TiltFilterParams.get_output_assets` names
  `filtered/tiltseries_*.star`, which no run writes any more (its only caller, `ProjectService.resolve_job_paths`, is itself uncalled); the roster's
  downstream check in `_remove_interactive_job` (a job path containing `tiltseries_filtered`) can no longer
  match — stage 2's Re-open/commit rules are where "who consumed this verdict" gets answered (alignment).

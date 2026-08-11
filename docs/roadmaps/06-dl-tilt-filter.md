# Roadmap 06 — DL tilt filter: real model, method switch, and the approval barrier

**Status:** scoped 2026-08-11, revised same day after design review, **not implemented**.
Trigger: new trained weights at `/groups/klumpe/software/Models/model260212.pth` (536 MB
checkpoint, `SmallSimpleCNN`, `model_state_dict` + `optimizer_state_dict`), next to Michael's
prototype (`filterTilts copy/`, written against legacy CryoBoost).

**Core design decision (user, 2026-08-11):** the TiltFilter job is **always interactive** — the
pipeline stops at it and the user MUST approve the tilt selection before anything downstream
runs. There is no unattended mode. The only switch is *what the user approves*:

- **`manual`** — user labels tilts by hand in the gallery (today's runtime-verified flow).
- **`deep_learning`** — DL inference is dispatched to the cluster; the user reviews the
  predicted labels (overriding freely) and approves them.

Either way: pipeline holds → user approves → pipeline proceeds. This kills the previously
scoped per-instance `IS_INTERACTIVE` migration and the standalone "auto"/"auto-gated"
orchestration modes entirely. The OOD (out-of-distribution) check demotes from an orchestration
gate to a **warning banner** shown during review.

## Before → After

**Before:** the manual gallery path works, but the DL path — though scaffolded end-to-end
(`drivers/tilt_filter.py`, `backend.submit_tilt_filter_dl`, vendored `filterTilts/`) — has
never run: the model dropdown (`default`/`binary`/`oneclass`) resolves to paths that don't
exist. Worse, the "hold" is advisory only: an uncommitted TiltFilter is silently dropped from
the resolver's producer pool (`path_resolution_service.py:531`), so downstream jobs deployed
while the filter is pending **fall back to tsImport's untrimmed tomostar and run on the
unfiltered tilt set** — no error, no warning.

**After:** a real, config-registered model powers DL filtering; a `filter_method` switch on the
job decides manual vs DL review; the pipeline **hard-stops** at a pending TiltFilter (downstream
is never deployed against the unfiltered tomostar); approval commits the cut and resumes the
remaining pipeline.

---

## 1. Inventory — what already exists (do not rebuild)

Surveyed 2026-08-11:

| Piece | State |
|---|---|
| `filterTilts/` vendored package (ImageProcessor, ModelLoader, model_architectures, PredictionThresholder, FilterStatistics + OOD check) | **in repo**, only cosmetically diverged from Michael's copy (ruff formatting) |
| `drivers/tilt_filter.py` | **complete DL driver**: MRC→PNG (Fourier crop 384px) → inference → threshold → labeled/filtered stars → tomostar trim → registry `set_frame_filtered` stamp → exit markers |
| `services/jobs/tilt_filter.py` (`TiltFilterParams`) | exists; `IS_INTERACTIVE=True`; USER_PARAMS = model_name, image_size, dl_batch_size, prob_threshold, prob_action; IO slots wired (fsMotion star in, tsImport tomostar in, trimmed tomostar out) |
| `backend.submit_tilt_filter_dl` | bespoke sbatch submission of the driver (outside the schemer), UI polls exit markers |
| `ui/tilt_filter_panel.py` | manual gallery (runtime-verified), DL config expansion + "Run DL Filter" button, sort-by-probability, show-only-removed, per-tilt CTF/motion chips from Warp XMLs |
| Pipeline placement | tiltFilter sits **before alignment**, trims the tomostar all downstream WarpTools steps read (runtime-verified 2026-07-29) |
| torch in venv | torch 2.6.0 + torchvision 0.21.0 installed |

**New checkpoint:** `model260212.pth` declares `model_architecture: SmallSimpleCNN`; its
state-dict keys (conv1–6, bn1–6, fc1–4) match the vendored class exactly — **no architecture
code changes needed**. The 536 MB includes `optimizer_state_dict` (Adam moments); weights alone
are ~180 MB.

**From Michael's prototype, NOT worth porting:** the napari/Qt batch viewer (our gallery covers
it), `warpProjectHandler`/mdoc filtering (obsolete — tomostar trim replaced it), `plotter.py`
(dashboard covers it), `filterTiltsRule.py` (empty stub).

**Worth porting (small):** manual-verdicts-always-win merge semantics; the
`FilterStatistics.evaluate_distribution()` OOD heuristics as a review-time banner;
probability-sorted "review worst first" as the default post-DL ordering.

---

## 2. UX use cases

**U1 — DL-assisted session.** User queues the pipeline with TiltFilter set to `deep_learning`.
When upstream inputs land, inference auto-dispatches to SLURM; the pipeline holds at the filter.
User opens the panel (or was watching it), sees predictions sorted worst-first, flips a few
labels, hits **Approve & Continue** → tomostar trimmed, downstream resumes. *Success looks
like:* user only adjudicates the low-confidence band, never hand-labels obvious junk.

**U2 — Manual session.** Same hold, `manual` method: gallery starts all-good (or metric-chip
informed), user labels by hand, approves, pipeline resumes. This is today's flow plus the hard
barrier and the resume.

**U3 — OOD warning.** New grid type / magnification → the review panel shows a prominent
distribution-check banner (mean prob < 0.95, good-at-high-angles inversion, >25 % cut) telling
the user the model's verdicts are suspect for this data. Purely informational; the user still
approves whatever they decide.

**U4 — Label while you wait.** In DL mode the user can start labeling while inference queues;
on completion DL fills in **only tilts the user hasn't touched** — manual verdicts win.

**U5 — Re-filter.** User tightens the threshold and re-runs DL, or re-labels manually;
re-approval re-trims the tomostar. Downstream staleness is owned by the forward-only convention
and roadmap 05, not by this feature.

---

## 3. Design decisions

### 3.1 Model registry in conf.yaml — no invented defaults

```yaml
tilt_filter:
  models:
    michaelNet_260212: /groups/klumpe/software/Models/model260212.pth
  default_model: michaelNet_260212
```

- UI dropdown lists registry entries (+ manual-path picker via the project file explorer).
  `job_model.model_name` stores the registry key or an absolute path; the driver resolves
  key→path via config at run time so weights can move without touching project state.
- **No model configured / path missing → red marker + disabled run** with a tooltip naming the
  missing config key (CLAUDE.md "surfacing uncertainty"). Delete the phantom bundled-path
  `'default'` fallback in `ModelLoader` rather than leaving a silent wrong default.
- Optional: publish a stripped copy (drop optimizer state, ~180 MB) to cut node-load time.

### 3.2 `filter_method` switch — a job param, not an orchestration mode

`filter_method: Literal["manual", "deep_learning"] = "manual"` on `TiltFilterParams`
(USER_PARAMS member; rendered as the headline toggle when queuing the job and at the top of the
panel). It changes only what the panel does during the hold:

- `manual` → gallery-first; DL expansion hidden or collapsed.
- `deep_learning` → DL config front and center; inference auto-dispatches (§3.4); gallery
  becomes a review surface pre-populated with predictions.

`IS_INTERACTIVE` stays a ClassVar, stays `True`, and none of its ~12 call sites change.

### 3.3 The approval barrier — fix the silent fallback

Today `deploy_and_run_scheme` skips interactive jobs and deploys everything else in the
selected set; the resolver drops the pending TiltFilter from the producer pool, so alignment
resolves its `tomostar_dir` against tsImport's **untrimmed** tomostar. A user who queues the
whole chain gets an unfiltered pipeline with no warning.

Fix: **deploy splits at the first pending interactive job.** Jobs upstream of it dispatch
normally; the interactive job and everything after it stay SCHEDULED. (The "absent job" fallback
in the param-class comment remains valid — only *present-but-pending* becomes a barrier.)
Implementation sketch: `deploy_and_run_scheme` walks `selected_instance_ids` in order and
truncates `instances_to_run` at the first non-SUCCEEDED interactive instance; applies to both
the schemer path and the afterok `_submit_chain`.

### 3.4 Approve → commit → resume

One **Approve & Continue** action (replaces/absorbs "Save Labels" when the job is part of a
run) that:

1. Commits: trim tomostar + registry stamp + labeled/filtered stars — extracted into ONE
   service function shared by the driver and the panel (today the UI's
   `_finalize_pipeline_output` and the driver duplicate this; also reconciles the two output
   locations, fixed `TiltFilter/` vs the DL run's `dl_run/`).
2. Flips the job SUCCEEDED.
3. **Auto-resumes the remainder**: re-invokes deploy for the rest of the persisted
   `state.pipeline_order` (P1.0 gives us membership without UIState, so resume works from any
   tab). *Assumption to confirm: resume is automatic on approval, not a second manual "run"
   click.*

### 3.5 DL auto-dispatch during the hold

When `filter_method == "deep_learning"` and the job's inputs become resolvable (fsMotion +
tsImport SUCCEEDED), the pipeline runner fires `backend.submit_tilt_filter_dl` automatically —
same pattern as the tsCtf auto-thumbnail kickoff (dedup-keyed so a manual click can't double
up). The panel keeps its manual "Run DL Filter" button for re-runs with different
threshold/model. *Assumption to confirm: dispatch-on-inputs-ready, so results are usually
waiting when the user comes to review.*

Merge invariant everywhere: `job_model.tilt_labels` (manual) always overrides DL predictions;
DL fills only untouched keys (the driver's step 6 already does this — keep it, and apply the
same rule when the panel reloads DL results mid-session, U4).

### 3.6 OOD = banner, not gate

Driver writes `distribution_check.txt` (vendored `FilterStatistics`); the review panel renders
it as a warning banner when tripped. No orchestration involvement — the user is already
required to review in every mode. Thresholds (0.95 / 25 % / angle inversion) come from
Michael's tuning on old data → make them config, not constants, so real datasets can
recalibrate without a code change.

### 3.7 Execution environment — verify before building

The driver runs `venv/bin/python3` directly on the compute node. torch 2.6 + CUDA via the venv
on partition-g nodes is **unverified** (the DL path has never run). Stage 0 includes a smoke
run; fallback is the miss-align container pattern (ships torch; remember the `--cleanenv`
getpwuid fix). Inference is light (SmallSimpleCNN @ 384 px) — `gres=gpu:1`, short walltime;
verify the TiltFilter SLURM preset actually carries it.

---

## 4. Stages

### Stage 0 — Make the DL path real (small, unblocker)
- conf.yaml `tilt_filter.models` registry + `default_model`; template + structured config
  editor entry; UI dropdown ← registry (+ manual path picker); red-marker when unconfigured.
- Driver resolves registry key → path; delete `ModelLoader`'s phantom `'default'` fallback.
- SLURM preset check (`gres=gpu:1`).
- **Runtime verification (user-run):** interactive *Run DL Filter* end-to-end on a repo-local
  project — checkpoint loads on a GPU node, predictions land in the gallery, commit trims the
  tomostar. Retires the long-standing "DL path PENDING RUNTIME".
- *Success:* DL filtering works with the real model; a missing model is visibly flagged, never
  defaulted.

### Stage 1 — `filter_method` switch + approval as the single commit path
- `filter_method` param + queue-time toggle + panel layout switch (§3.2).
- Extract the commit into one service function (driver + panel callers); reconcile output
  locations.
- **Approve & Continue** action (commit + SUCCEEDED); OOD banner in the review panel (§3.6).
- *Success:* both methods end in the same approval action producing byte-identical outputs.

### Stage 2 — The barrier + resume (the real orchestration work)
- Deploy truncates at the first pending interactive job — schemer AND afterok paths (§3.3).
- Approval auto-resumes the remaining `pipeline_order` (§3.4).
- Roster: pending TiltFilter shows a "waiting for review" badge; downstream shows why it's held.
- *Success:* queueing the full chain with a pending TiltFilter can NEVER run alignment on the
  untrimmed tomostar; approving resumes without re-configuring anything.

### Stage 3 — DL auto-dispatch + review polish
- Runner auto-fires `submit_tilt_filter_dl` when method=DL and inputs land (§3.5), dedup-keyed.
- Dispatch status survives tab reload (BackgroundTask + SingleFlight, not a click-handler poll
  loop); manual-wins merge on reload (U4).
- Post-DL gallery defaults to ascending-probability sort; low-confidence band visually distinct.
- *Success:* U1 — by the time the user opens the panel, predictions are typically already
  there; closing/reopening the tab never loses dispatch state.

### Stage 4 — Feedback loop (future, unscoped)
- Manual overrides are already registry-stamped with probabilities — export (key, label, png)
  tuples as a retraining set for Michael; registry keys model versions so projects record
  *which* model filtered them. Don't build until a second model exists.

---

## 5. Risks / open items

1. **Barrier semantics on the afterok path** — `_submit_chain` builds dependency edges; the
   truncation must not leave dangling afterok references. Audit at Stage 2 start.
2. **Resume correctness** — resuming via `pipeline_order` must survive tab-less operation
   (BackgroundTask has no client context — resolve state by explicit path) and the
   `pipeline_active` guard. Reuse the recovery-tick machinery rather than a new entry point if
   possible.
3. **venv torch on GPU nodes** — unverified; container fallback known-good. Resolve empirically
   in Stage 0.
4. **Downstream staleness after re-approval** (U5) — out of scope, owned by roadmap 05.
5. **Two assumptions to confirm with the user:** (a) approval auto-resumes downstream (no second
   "run" click); (b) DL inference auto-dispatches as soon as inputs are ready, not on first
   panel open.

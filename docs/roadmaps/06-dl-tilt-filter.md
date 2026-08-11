# Roadmap 06 — DL tilt filter: real model, interactive dispatch, and pipeline-auto mode

**Status:** scoped 2026-08-11, **not implemented**. Trigger: new trained weights landed at
`/groups/klumpe/software/Models/model260212.pth` (536 MB checkpoint, `SmallSimpleCNN`,
`model_state_dict` + `optimizer_state_dict`), next to Michael's prototype
(`filterTilts copy/`, written against legacy CryoBoost). This roadmap wires those weights into
the existing TiltFilter job and adds an **auto (unattended) mode** so the filter can run as a
normal pipeline stage, not only as a hands-on gallery session.

## Before → After

**Before:** the TiltFilter job is interactive-only. The manual gallery path is runtime-verified
(labels → trimmed tomostar → registry stamp). The DL path is scaffolded end-to-end
(`drivers/tilt_filter.py`, `backend.submit_tilt_filter_dl`, vendored `filterTilts/` package) but
**cannot actually run**: the model dropdown offers `default`/`binary`/`oneclass`, where `default`
resolves to `filterTilts/data/models/michaelNet_0.6/model.pth` — a path that does not exist in the
repo — and the other two are placeholders that resolve to nothing. There are no real weights
anywhere in the system.

**After:** one real, config-registered model powers three ways of filtering:

1. **Interactive** — user dispatches DL inference to the cluster from the gallery, waits with live
   status, reviews predictions sorted by confidence, overrides, commits (exists today minus the
   model + polish).
2. **Auto** — TiltFilter is queued in the scheme like any driver job; inference + threshold +
   tomostar trim + registry stamp happen unattended and downstream proceeds.
3. **Auto with review gate** (Michael's `onFailure` mode) — auto, but if the prediction
   distribution looks out-of-distribution the pipeline holds at the filter and asks for a human.

---

## 1. Inventory — what already exists (do not rebuild)

Surveyed 2026-08-11. Much more is in place than the "prototype" framing suggests:

| Piece | State |
|---|---|
| `filterTilts/` vendored package (ImageProcessor, ModelLoader, model_architectures, PredictionThresholder, FilterStatistics + OOD check) | **in repo**, only cosmetically diverged from Michael's copy (ruff formatting) |
| `drivers/tilt_filter.py` | **complete DL driver**: MRC→PNG (Fourier crop 384px) → inference → threshold → labeled/filtered stars → tomostar trim → registry `set_frame_filtered` stamp → exit markers |
| `services/jobs/tilt_filter.py` (`TiltFilterParams`) | exists; `IS_INTERACTIVE=True` ClassVar; USER_PARAMS = model_name, image_size, dl_batch_size, prob_threshold, prob_action; IO slots wired (fsMotion star in, tsImport tomostar in, trimmed tomostar out) |
| `backend.submit_tilt_filter_dl` | bespoke sbatch submission of the driver (outside the schemer), UI polls exit markers |
| `ui/tilt_filter_panel.py` | manual gallery (runtime-verified), DL config expansion + "Run DL Filter" button, sort-by-probability, show-only-removed, per-tilt CTF/motion chips from Warp XMLs |
| Pipeline placement | tiltFilter sits **before alignment**, trims the tomostar all downstream WarpTools steps read (runtime-verified 2026-07-29) |
| torch in venv | torch 2.6.0 + torchvision 0.21.0 installed |

**New checkpoint:** `model260212.pth` declares `model_architecture: SmallSimpleCNN` and its
state-dict keys (conv1–6, bn1–6, fc1–4) match the vendored class exactly — **no architecture code
changes needed**. The 536 MB includes `optimizer_state_dict` (Adam moments); weights alone are
~180 MB.

**From Michael's prototype, NOT worth porting:**
- `filterTiltsInt.py` (napari/Qt batch viewer) — our NiceGUI gallery already covers click-to-toggle,
  probability sort, only-removed filter, per-TS grouping, IMOD-style zoom (via the upsample dialog).
- `warpProjectHandler` / mdoc filtering — obsolete; the tomostar trim replaced mdoc-level filtering.
- `plotter.py` — the per-TS dashboard + tilt-filter chips cover this.
- `filterTiltsRule.py` — rule-based filtering was an empty stub even in the prototype.

**Worth porting (small):**
- `merge_with_existing` semantics: DL predictions must never clobber a manual verdict (see §3.3).
- `FilterStatistics.evaluate_distribution()` + the `distribution_check.txt` / DATA_IN_DISTRIBUTION
  marker idea → becomes the Stage 3 review gate.
- Probability-sorted "review worst first" as the *default* post-DL gallery ordering.

---

## 2. UX use cases

**U1 — Attended session (interactive dispatch).** Preprocessing is done; user opens the TiltFilter
job tab, picks the model + threshold, clicks *Run DL Filter*. The job goes to SLURM; the panel
shows queue→running→done (survives tab reload). On completion the gallery reloads sorted by
ascending probability with predicted-bad tilts flagged; user flips a handful, hits *Save Labels*
→ tomostar trimmed, pipeline continues. *Success looks like:* zero manual labeling of obvious
junk; user only adjudicates the low-confidence band.

**U2 — Overnight unattended (auto mode).** User builds the full scheme with TiltFilter set to
`auto`, launches, goes home. The filter runs as a normal pipeline stage between fsMotion/tsImport
and alignment; downstream reconstructs on the trimmed tilt set. Next morning the gallery shows
what was cut (retro-review), with labels editable + forward-only re-run if the user disagrees.

**U3 — Unattended with a safety net (auto + gate).** Same as U2, but on a new grid type /
magnification the model may be out of distribution. The driver's distribution check (mean prob
< 0.95, good-tilts-at-high-angles inversion, >25 % cut) trips → pipeline holds at TiltFilter in a
"needs review" state, the roster badges it, user reviews exactly like U1 and commits to resume.

**U4 — Label while you wait.** User dispatches DL (U1), and labels obvious tilts by hand while the
job queues. On completion DL fills in **only tilts the user hasn't touched** — manual verdicts win.

**U5 — Re-filter.** After seeing results, user tightens the threshold and re-runs DL, or just
re-labels manually. Re-commit re-trims the tomostar. Downstream staleness is handled by the
existing forward-only convention (and eventually roadmap 05's targeted top-up), not by this
feature.

---

## 3. Design decisions

### 3.1 Model registry in conf.yaml — no invented defaults

Replace the fake `default`/`binary`/`oneclass` dropdown with a config-driven registry:

```yaml
tilt_filter:
  models:
    michaelNet_260212: /groups/klumpe/software/Models/model260212.pth
  default_model: michaelNet_260212
```

- The UI dropdown lists registry entries (+ a manual-path picker, reusing the project file
  explorer per UI conventions). `job_model.model_name` stores the registry key or an absolute path;
  the driver resolves key→path via config at run time so weights can move without touching
  project state.
- **No model configured / path missing → red marker on the DL expansion + disabled run button**
  with a tooltip naming the missing config key (CLAUDE.md "surfacing uncertainty" rule). Never
  fall back to the nonexistent bundled path.
- Optional hygiene: publish a stripped copy (drop `optimizer_state_dict`, ~180 MB) next to the
  original to cut node-load time; keep the original as the training artifact.

### 3.2 Per-instance run mode (the real architectural change)

`IS_INTERACTIVE` is a ClassVar consulted at ~12 call sites (orchestrator skip, monitor recovery ×2,
runner sync/overview/stop-on-fail, roster ×3, job tab, resolver, builder). Auto mode needs the
*same job type* to be dispatchable, so interactivity must become **per-instance**:

- Add `run_mode: Literal["interactive", "auto", "auto_gated"] = "interactive"` to
  `TiltFilterParams` (USER_PARAMS member, rendered as a 3-way toggle at the top of the job tab).
- Replace ClassVar reads with an instance method `is_interactive()` (default implementation
  returns the ClassVar, so every other job type is untouched; TiltFilter overrides with
  `run_mode == "interactive"`). One call site uses the *class*
  (`pipeline_builder_panel.py:422` gates the roster affordance on `param_cls`) — that one keeps
  the ClassVar meaning "can be interactive".
- Audit every `getattr(x, "IS_INTERACTIVE", False)` site and decide skip-vs-dispatch per mode.
  This is the riskiest slice: a missed site either double-dispatches the job or deadlocks the
  pipeline-complete counter (see the `get_pipeline_overview` comment about interactive jobs and
  `scheduled > 0` forever).

### 3.3 One commit path, manual-wins merge

Today there are **two divergent output flows**: interactive commits write to a fixed
`<project>/TiltFilter/` dir via `_finalize_pipeline_output` (UI-side), while a scheme-dispatched
driver run would write to `External/jobNNN/`. Consolidate:

- Extract the commit (trim tomostar + registry stamp + labeled/filtered stars) into one service
  function used by both the driver and the UI panel. The driver already implements it —
  the service becomes its home; the panel calls the same code.
- Merge policy everywhere: `job_model.tilt_labels` (manual) **always** overrides DL predictions;
  DL fills only keys absent from `tilt_labels` (this is what the driver's step 6 already does —
  keep it, and apply the same rule when the interactive panel reloads DL results, U4).
- The resolver already falls back to producer `paths` for SUCCEEDED interactive jobs
  (`path_resolution_service.py:531`); auto instances get normal jobNNN resolution. Verify both
  routes end at the same `output_tomostar` key.

### 3.4 The review gate (auto_gated)

Driver computes `FilterStatistics.evaluate_distribution()` and writes `distribution_check.txt`
(already-vendored logic). Semantics:

- **In distribution** → behave exactly like `auto` (commit + SUCCESS).
- **OOD** → commit *nothing downstream-visible* (write labeled star only, not the tomostar trim),
  exit with a distinct marker, and let the job surface as **needs-review** rather than
  FAILED-red. Recommendation: reuse the interactive-hold machinery — on OOD the job flips itself
  to interactive-style "waiting for user" and the scheme stops cleanly after it (stop-on-fail path
  minus the red badge), rather than inventing a new JobStatus. The task-status JSON-record
  migration (task-status roadmap §5) is the natural place for the marker if it lands first.
- Roster: amber "review" badge + one-click jump to the gallery; commit from the gallery resumes
  downstream (same redeploy the user does today after manual filtering).

### 3.5 Execution environment — verify before building

The driver runs `venv/bin/python3` directly on the compute node (no container). torch 2.6 +
CUDA on CBE GPU nodes via the venv is **unverified** for this driver (the DL path has never run).
Stage 0 includes a smoke run; if the venv torch can't see the GPU on partition g, fall back to the
miss-align container pattern (it already ships torch; remember the `--cleanenv` getpwuid fix).
Inference is light (SmallSimpleCNN @ 384 px, ~1–2 k tilts) — `gpu:1`, short walltime, default QOS
is fine; make sure the TiltFilter SLURM preset actually carries `gres=gpu:1`.

### 3.6 PNG unification (minor)

Two PNG pipelines exist: gallery thumbnails (`generate_tilt_thumbnails`, auto-kicked on tsCtf
success) and the driver's inference PNGs (`ImageProcessor.batch_convert` at 384 px into
`filtered/png/`). Don't merge them in this roadmap (different sizes/normalizations, and inference
PNGs are a model input — display prettification must not leak in). Just make the driver skip
`save_pngs` when the gallery already has thumbnails, or keep both and ignore; cost is disk only.

---

## 4. Stages

### Stage 0 — Make the DL path real (small, unblocker)
- conf.yaml `tilt_filter.models` registry + `default_model`; template + structured config editor
  entry.
- UI dropdown ← registry (+ manual path picker); red-marker/disabled state when unconfigured.
- Driver resolves registry key → path; delete the phantom bundled-path fallback in
  `ModelLoader` (`'default'` branch) rather than leaving a silent wrong default.
- SLURM preset check (`gres=gpu:1`).
- **Runtime verification (user-run):** interactive *Run DL Filter* end-to-end on a repo-local
  project — venv torch loads the checkpoint on a GPU node, predictions land in the gallery,
  commit trims the tomostar. This also retires the long-standing "DL path PENDING RUNTIME".
- *Success:* U1 works with the real model; a missing model is visibly flagged, never defaulted.

### Stage 1 — Interactive polish (attended UX)
- Post-DL gallery defaults to ascending-probability sort with the low-confidence band visually
  distinct; stats strip gains mean-prob + OOD banner (FilterStatistics is already vendored).
- Dispatch status that survives tab reload: move the 5 s marker-poll loop out of the click handler
  into a BackgroundTask (dedup-keyed like the thumbnail task), SingleFlight on the run button.
- Manual-wins merge on DL reload (U4).
- *Success:* dispatch → close tab → reopen shows correct state; a manual label set before DL
  completion survives it.

### Stage 2 — Auto mode (pipeline dispatch)
- `run_mode` param + instance-level `is_interactive()`; audit all ClassVar call sites
  (orchestrator, runner ×3, monitor ×2, roster ×3, job tab, resolver, builder).
- Auto instances flow through the scheme *and* the afterok chain like any driver job (driver map
  entry already exists at `pipeline_orchestrator_service.py:515`).
- Unify the commit path (§3.3) so jobNNN-run and TiltFilter/-run produce identical
  `output_tomostar` wiring; gallery reads results from whichever ran.
- Roster/job tab: mode toggle; auto instances show normal status dots, interactive keep the
  current hand-icon treatment.
- *Success:* U2 — a scheme containing an auto TiltFilter runs start-to-finish unattended and
  alignment consumes the trimmed tomostar; flipping the same instance to interactive restores
  today's behavior exactly.

### Stage 3 — Review gate (`auto_gated`)
- Driver: OOD evaluation + marker; no tomostar trim on OOD.
- Orchestration: needs-review hold state per §3.4; clean scheme stop; roster amber badge → gallery;
  commit resumes downstream.
- *Success:* U3 — an in-distribution dataset flows through untouched; a deliberately-OOD one
  (e.g. wrong mag) halts at the filter with a review prompt instead of silently cutting 40 % of
  tilts.

### Stage 4 — Feedback loop (future, unscoped)
- Manual overrides are already stamped into the registry with probabilities — export
  (key, label, png) tuples as a training set for Michael's next retrain; registry keys model
  versions so projects record *which* model filtered them. Do not build until a second model
  exists.

---

## 5. Risks / open questions

1. **ClassVar→instance interactivity** is the highest-blast-radius change; a missed call site
   deadlocks pipeline-complete detection or double-runs the job. Mitigate with a full grep audit
   in the Stage 2 PR description, site by site.
2. **venv torch on GPU nodes** — unverified; container fallback is the known-good escape hatch
   (miss-align pattern). Resolve empirically in Stage 0 before building anything on top.
3. **OOD hold representation** — recommendation is "flip to interactive-hold", but if the
   task-status JSON migration lands first, prefer its record format. Decide at Stage 3 start.
4. **Downstream staleness after re-label** (U5) is explicitly out of scope — owned by roadmap 05.
5. **Model trust bootstrapping** — thresholds in `evaluate_distribution` (0.95 / 25 % / angle
   inversion) come from Michael's tuning on the old data; treat them as config, not constants,
   so the first real datasets can recalibrate them without a code change.

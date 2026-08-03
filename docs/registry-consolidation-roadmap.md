# Registry consolidation — make the TiltSeriesRegistry the store

**Status:** SCOPING (recon complete 2026-07-31; scope narrowed by user steer 2026-07-31). Companion to
[`tilt-filter-tomostar-relion-split-roadmap.md`](tilt-filter-tomostar-relion-split-roadmap.md)
(the §5 "collapse the RELION track" question this doc answers concretely) and
[`preprocessing-metrics-inventory.md`](preprocessing-metrics-inventory.md) (the placeholder-trap catalog).

> **SCOPE (user steer 2026-07-31).** This effort finishes the **preprocessing / TS-and-tomogram**
> registry in good shape. It does **NOT** try to stuff the particle stage into the `TiltSeriesRegistry`.
> The particle stage is a *different shape of problem* — its primary entity is the **particle** (species,
> counts, qualities, locations), not the tilt-series — and it deserves its **own registry concept**
> later, which **backlinks** to the TS registry for source tomogram + acquisition metadata. The clean
> conceptual boundary: the TS registry owns everything up to and including the **tomogram** (so
> **denoise**, which is tomogram-scoped, is in scope); **template-matching onward is out of scope here**
> and belongs to the future particle registry. See §3 Tier C.

---

## 1. The thesis

A tilt-series accumulates metadata at every stage of the pipeline. Today that metadata is
**scattered** across mdocs, `project_params.json`, RELION `.star` files, WarpTools XMLs, and loose
MRC/JSON files — and several pieces are **parsed then thrown away**, or written to stars as
**placeholders that lie** while the real value hides in an XML. We want one **indexed, in-memory,
method-bearing object** — the `TiltSeriesRegistry` — that holds everything a TS accumulates, so any
job, the dashboard, and QC can refer to it directly instead of re-parsing files.

**Stars stay producible** (RELION interop, "open the project in RELION" goal, and — crucially — the
particle-stage tools *consume* them). But through the **preprocessing chunk (import → reconstruct)**
we stop *depending* on them: the registry becomes authoritative, stars become an emit-only
projection.

### The natural cut line (from the de-star recon)

| Segment | Star reads today | Verdict |
|---|---|---|
| **Preprocessing** (fsMotion → tsImport → tiltFilter → align → tsCtf → reconstruct) | **enumeration-only** — the driver reads the star only for the list of `rlnTomoName` / frame names, then hands the *tomostar/XML* to WarpTools. The compute never reads the star. | **Registry-authoritative is feasible.** Swap the enum reads for `registry.tilt_series_ids()`; stars become emit-only. |
| **Particle stage** (templateMatch → extractCandidates → subtomo → reconstructParticle → merge) | **compute-critical** — PyTOM and RELION are *handed the star* and read its contents. | **Hard stop.** Stars remain load-bearing until the external tools change. Registry *records* the outputs but does not replace the star handoff. |

This is the whole strategy in one table: consolidate everything into the registry; retire star
*dependence* in preprocessing; keep star *production* everywhere and star *dependence* only at the
particle-stage tool boundary.

---

## 2. What's already in the registry (the foundation is real)

- **Identity / structure:** `TiltSeries` (id, mdoc, stage/beam position), `Frame` (id, raw_filename,
  tilt_index, nominal_tilt_angle_deg, pre_exposure), `Tomogram`.
- **Per-job outputs** (keyed by job instance_id, so per-species instances like
  `templatematching__ribosome` are expressible): `FsMotionCtfFrameOutput`, `TsAlignmentTiltSeriesOutput`,
  `TsCtfTiltSeriesOutput`, `TsReconstructTomogramOutput`.
- **Selection state:** `is_selected`, `is_excluded` (user mute, **main-process-owned**),
  `is_filtered_out` + `filter_reason` (tilt-filter verdict, driver-written).
- **Preprocessing `emit_star` is already registry-driven** (the four ingest adapters), and as of
  2026-07-31 `emit_star` correctly honors `is_filtered_out` (verified end-to-end). So the registry is
  *already* the de-facto producer of the preprocessing stars — we just don't yet *read* from it.

---

## 3. The complete gap inventory (three tiers)

### Tier A — FRONT: acquisition metadata parsed then dropped

The mdoc parser extracts these into a transient `TiltInfo.mdoc_stats` dict at import
(`services/configs/dataset_parsing_service.py`), but `services/tilt_series/build.py` copies only tilt
angle + prior dose onto the `Frame`; the rest is **discarded** when the registry persists.

| Field | Scope | mdoc source | Status |
|---|---|---|---|
| ExposureDose | per-tilt | `ExposureDose` | **dropped** |
| DoseRate | per-tilt | `DoseRate` | **dropped** |
| ExposureTime | per-tilt | `ExposureTime` | **dropped** |
| Defocus (measured) | per-tilt | `Defocus` | **dropped** (distinct from `TargetDefocus`, which the import-star writer re-reads from the mdoc on the fly — a workaround that disappears once the registry holds it) |
| Intensity min/mean/max | per-tilt | `MinMaxMean` | **dropped** (the dark-tilt detector QC signal) |
| ImageShift X/Y | per-tilt | `ImageShift` | **dropped** |
| DateTime | per-tilt | `DateTime` | **dropped + latent bug** — `build.py` reads `mdoc_stats["DateTime"]` into `Frame.acquisition_time`, but the parser never extracts it, so it is silently always `None` |
| Magnification, SpotSize, Binning | per-TS | header | not threaded to `AcquisitionParams` |

**Cost to close:** cheap — the values are already in hand at import. Add fields to `Frame`, populate
them in `build.py`, add one parser line for `DateTime`. No new I/O.

### Tier B — MIDDLE: real QC values that live only in the Warp XML (the placeholder-trap)

The RELION-star export writes **hard placeholders** for the metrics a QC view most wants; the real
values sit in the WarpTools XMLs and are read live by the dashboard (`frameseries_quality.py`), never
persisted. See `preprocessing-metrics-inventory.md` §0/§4.

| Real value | XML field | Star column (lies) |
|---|---|---|
| CTF fit resolution | `CTFResolutionEstimate` | `rlnCtfMaxResolution` = `1e-6` |
| Beam-induced motion | `MeanFrameMovement` | `rlnAccumMotionTotal/Early/Late` = `1e-6` |
| CTF figure of merit | (XML) | `rlnCtfFigureOfMerit` = `"None"` |
| Motion track | `GridMovementX/Y` | — (not in star) |
| CTF-fit curve | `PS1D` radial power spectrum | — |
| AreTomo per-tilt shift magnitude | AreTomo `.aln` | — |
| Dark-tilt detector signal | tomostar `AverageIntensity` | — |

**Cost to close:** small-medium — add fields to `FsMotionCtfFrameOutput` (and CTF-res to the per-frame
CTF), populate in the ingest adapter (which already opens the XML). Curves (PS1D, motion track) are
optional/heavier and can stay file-referenced.

### Tier C — BACK: the particle stage (MOSTLY OUT OF SCOPE — future particle registry)

The registry ends at `ts_reconstruct`. Everything past it lives in loose files/stars with no registry
representation. **Per the scope steer, we do NOT model the particle stage in the `TiltSeriesRegistry`.**
The particle is a different entity (species / counts / qualities / locations) and gets its own registry
later. Recorded here only so the future work is scoped, not lost.

| Job | Produces (unmodeled) | Disposition |
|---|---|---|
| denoise **predict** | per-TS denoised MRC path, method, model origin | **IN SCOPE (tomogram-scoped)** → `DenoisePredictTomogramOutput`, attaches via `attach_tomogram_output`. Denoise is the last TS/tomogram-centric step. |
| denoise **train** | global model tar, method, trained-on-TS set | **borderline** — job-scoped singleton, not per-TS. Minimal: a project-scope pointer (path + method) so predict can resolve it; not a per-TS output. (open Q 1) |
| **missAlign** | refined per-TS geometry (in XML), iters, checkpoint | tomogram/TS-scoped but geometry already lives in the Warp XML the alignment adapter reads; **defer** — no strong pull to model it now. |
| **templateMatch** | score/angle MRCs, per-TS CC stats, candidate count | **FUTURE particle registry** — per-species picking output. |
| **extractCandidates** | per-TS picks + counts, threshold, particles.star | **FUTURE particle registry** — this is where "particle" becomes the entity. |
| **subtomoExtraction** | per-TS subtomo dir + counts, box/crop/binning | **FUTURE particle registry.** |

**The future particle registry (not now):** a sibling store keyed by particle/species — holding per-species
picks, counts, correlation qualities, coordinates, orientations, extraction params, and subtomo paths —
that **backlinks** to `TiltSeries`/`Tomogram` by id for the source tomogram + acquisition metadata. Its
shape is genuinely different (per-particle rows, per-species aggregation across TS) and should not be
forced into the tilt-series model. Template-match → extract → subtomo → reconstruct-particle → merge all
belong to it.

---

## 4. The two structural gates (cross-cutting)

### Gate 1 — Main-process registry freshness (designed)

Drivers run in separate SLURM processes and mutate the *on-disk* registry (`registry.save()`); the
main server holds a cached `get_registry_for()` singleton that is never refreshed. Boundary map
(from recon): the **only** field the main process both reads and mutates is `is_excluded` /
`exclusion_reason` (dashboard mute). Everything a driver writes — outputs, `is_filtered_out`, and the
new Tier-A/B/C fields — the main process does **not** currently mutate.

**Design:** `registry.refresh_from_disk()` that, per TS whose sidecar mtime changed, reloads the
sidecar and **merges**: overwrite driver-owned fields, **preserve** `is_excluded`/`exclusion_reason`.
Triggered on job-completion and/or dashboard render (not a timer). mtime-gating mirrors the
star-read invalidation the dashboard gets for free today, and avoids clobbering (cf.
[[feedback_inmemory_state_authoritative]] — the 2026-05-13 stuck-yellow bug was exactly an
unconditional disk re-read clobbering live state).

This gate is **non-speculative only once a consumer exists** — the dashboard migration (Phase 3) is
that consumer. Do not build it earlier.

### Gate 2 — IO-graph re-key (smaller than feared)

The recon revises this down: for preprocessing the input-star reads are enumeration-only, so
`path_resolution_service` coupling is **weak** — swapping to registry queries is localized. The
particle-stage stars stay physically required (external tools), so the IO graph keeps its
star-typed slots *there*. We are not re-keying the whole graph, only relieving preprocessing of
star *dependence*.

---

## 5. Staged plan

Each phase is independently shippable and leaves the system working.

**Phase 0 — done (2026-07-31):** `emit_star` respects `is_filtered_out`; filter propagates cleanly to
the particle stage. Verified on `copiatest_postfilter`.

**Phase 1 — Make the registry a complete superset (backfill).** Purely additive; no read-path change,
no behavior change. Bumps `REGISTRY_SCHEMA_VERSION` minor per sub-step.
- **1a — Front acquisition (Tier A). ✅ BUILT 2026-07-31 (code-clean ruff+py_compile, PENDING RUNTIME).**
  Added 9 per-tilt acquisition fields to `Frame` (`exposure_dose_e_per_a2`, `dose_rate`,
  `exposure_time_s`, `defocus_um`, `min/mean/max_intensity`, `image_shift_x/y`; all `Optional`).
  Threaded `DateTime` through `TiltInfo.date_time` (parser extract in `dataset_parsing_service`) →
  **fixes the latent bug where `Frame.acquisition_time` was always `None`**. Populated BOTH build paths
  (`_build_one_ts` preferred + `build_from_mdocs` legacy) via `_acq_kwargs_from_stats` /
  `_acq_kwargs_from_raw_section` helpers. Schema 1.1→1.2. **Also fixed (user-decided against old
  CryoBoost):** `_build_one_ts` looked up `mdoc_stats.get("PriorRecordDose")` but the parser stores it
  under `"prior_dose"` → the preferred path silently always used the cumulative sum; now reads the right
  key so `pre_exposure` prefers the mdoc's `PriorRecordDose` (authoritative for dose-symmetric/variable
  dose), matching the legacy path + the code's stated intent + old CryoBoost's deference to the mdoc
  dose record. *(Not done: the import-star `TargetDefocus` mdoc re-read is now redundant but left for a
  later read-path pass — see Phase 4.)*
- **1b — XML QC values (Tier B). ✅ BUILT 2026-07-31 (code-clean, PENDING RUNTIME).** Added
  `ctf_resolution` + `mean_frame_movement` to `FsMotionCtfFrameOutput`; the fs-motion ingest adapter
  populates them from the Warp `<Movie>` root attributes (`CTFResolutionEstimate`, `MeanFrameMovement`)
  via a local positive-float coercion, right where it already parses the XML. Schema note folded into
  1.2. *(Deferred: per-tilt-series CTF-res from the tsCtf XML, and the PS1D/motion-track curves — kept
  file-referenced per open Q 5.)*
- **1c — Tilt-filter probability:** persist `cryoBoostDlProbability` alongside the existing boolean
  verdict (Frame field or a small filter output).
- **1d — Denoise (tomogram-scoped, the last preprocessing step):** add `DenoisePredictTomogramOutput`
  (denoised MRC path + method + model origin) via `attach_tomogram_output`, and a minimal project-scope
  pointer for the denoise-train model. **Stop here** — template-matching onward is the future particle
  registry, out of scope for this effort.

**Phase 2 — Freshness (Gate 1).** Build `refresh_from_disk()` + merge + mtime-gating + triggers. Its
first consumer is Phase 3.

**Phase 3 — Dashboard onto the registry.** Add a `registry.per_tilt_view(ts_id)` accessor returning the
shape `figures.py` already consumes; migrate **panel-by-panel**, keeping the star-read path as a
fallback and runtime-verifying each panel renders identically. Start with TS-align (100% in registry
already). Delete the star-read path only after all panels are migrated.

**Phase 4 — De-star preprocessing compute.** Replace the enumeration-only input-star reads with
registry queries at the ~4 sites (`read_tilt_series_names_from_input_star` in ts_alignment/ts_ctf/
ts_reconstruct; `read_ts_frame_mapping` in fs_motion). Preprocessing input stars stop being
load-bearing for execution.

**Phase 5 — Preprocessing stars become emit-only.** Produced for RELION-interop + the particle-stage
handoff, but nothing upstream depends on them. Optionally make emission lazy. **Particle-stage stars
stay as they are** (documented hard stop: PyTOM/RELION consume them).

**Sequencing:** 1a+1b first (front + QC backfill) — smallest, unblocks the dashboard win, low risk.
Then 2+3 as a pair (freshness earns itself via the dashboard). 1c/1d and 4/5 can proceed in parallel
tracks once the pattern is set.

---

## 6. Open decisions (product / judgment)

1. **Job-scoped outputs.** The registry is TS-scoped, but denoise-**train** produces a *single global
   model*. Where does it live — a new project/job-scope output store, or replicated as a pointer on
   every trained-on TS? (Leaning: a small `ProjectState`/registry project-scope `job_outputs` map.)
2. **Particle registry shape (future, decided-in-principle 2026-07-31).** The particle stage gets its
   own store keyed by particle/species, backlinking to the TS registry. Not designed here; flagged so
   Tier-C work isn't accidentally folded into the TS registry. When we get there: decide entity
   granularity (per-particle rows vs per-(species,TS) aggregates) and how it references tomograms.
3. **Non-WarpTools aligner ever?** Decides how tool-agnostic the Tier-B models must be, and whether
   the eventual full collapse (§5 option ii vs iii in the sibling doc) is worth it. Doesn't block
   Phases 1–3.
4. **Migration for existing on-disk projects.** New Tier-A fields need the mdoc re-read for old
   projects ("reload to backfill from mdocs"); Tier-B backfill on old projects needs a re-ingest pass
   or is forward-only. Decide forward-only vs backfill-on-load.
5. **Curves (PS1D, motion track).** Persist into the registry (heavier JSON) or keep file-referenced
   and load on demand? (Leaning: file-referenced; the registry holds the path + scalars.)

---

## 7. First slice

**Phase 1a + 1b:** extend `Frame` + the fs-motion output models with the dropped acquisition fields
and the two XML QC scalars, populate them at import/ingest, and fix the `DateTime` parser bug. Purely
additive, no consumer switched yet, verifiable by inspecting a freshly-built registry JSON. This makes
the registry a complete per-tilt superset — the precondition for the dashboard migration and
everything after. **✅ DONE 2026-07-31 (code-clean, PENDING RUNTIME) — see §8.**

---

## 8. Next-session handoff (2026-07-31)

**State:** Phase 0 verified. Phase 1a + 1b + the `pre_exposure` key-fix are **BUILT + RUNTIME-VERIFIED
2026-07-31** on `copiatest_postfilter` (16 TS; acq fields sane min≤mean≤max, `acquisition_time` now
populated = DateTime bug fixed; `ctf_resolution`=6.9Å + `mean_frame_movement`=1.23 real from XML).
Only un-exercised bit: `pre_exposure` on a mid-stack frame (frame[0]=0 for both code paths) — eyeball a
mid frame vs the mdoc `PriorRecordDose` if you want ocular proof. 7 files touched:
`services/tilt_series/models.py` (Frame acq fields + `FsMotionCtfFrameOutput.ctf_resolution`/
`mean_frame_movement`), `services/dataset_models.py` (`TiltInfo.date_time`),
`services/configs/dataset_parsing_service.py` (extract `DateTime`),
`services/tilt_series/build.py` (populate both paths + `pre_exposure` key-fix + 2 helpers),
`services/tilt_series/registry.py` (schema 1.1→1.2),
`services/tilt_series/adapters/fs_motion_ctf.py` (populate XML QC).

**FIRST THING NEXT SESSION — runtime-verify 1a/1b.** No auto-reload + no pandas in the Claude sandbox,
so the user runs it. Harness: `/users/artem.kushner/verify_registry_backfill.py` (build a fresh
registry from `copiatest_postfilter` mdocs → assert acq fields + non-null `acquisition_time`; re-ingest
fs-motion → assert `ctf_resolution`/`mean_frame_movement` non-null). OR: reload a project in the UI and
grep `registry/tilt_series/*.json` for `exposure_dose_e_per_a2` + `acquisition_time`. **Watch the
`pre_exposure` change** — on dose-symmetric data `pre_exposure` will now differ from the old cumulative
(it prefers `PriorRecordDose`); confirm that's the intended, more-correct value on a real dataset.

**THEN, remaining Phase 1:**
- **1c — tilt-filter probability:** persist `cryoBoostDlProbability` (currently only the boolean
  `is_filtered_out` is in the registry). Add a `Frame.filter_probability: Optional[float]` (or a small
  filter output) and stamp it in `drivers/tilt_filter.py` + `ui/tilt_filter_panel.py` right where
  `set_frame_filtered` is already called.
- **1d — denoise-predict tomogram output (LAST preprocessing step; particle stage is OUT — future
  particle registry):** add `DenoisePredictTomogramOutput` (denoised MRC path + method + model origin),
  attach via `attach_tomogram_output` from `drivers/denoise_predict.py`. A minimal project-scope pointer
  for the denoise-train global model (path + method) so predict can resolve it.

**THEN Phases 2→5** (see §5): freshness (`refresh_from_disk` merge, preserve `is_excluded`, mtime-gated)
→ dashboard `per_tilt_view` accessor + panel-by-panel migration (start TS-align) → swap preprocessing
enum-reads to `registry.tilt_series_ids()` → preprocessing stars emit-only.

**Open decisions still to make** (see §6): job-scoped output home for denoise-train; migration
(forward-only vs backfill-on-load) for old on-disk projects that predate 1a/1b; curves persist-vs-file-ref.

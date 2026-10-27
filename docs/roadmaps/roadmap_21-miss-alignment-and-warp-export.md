# Roadmap 21 — Miss-alignment as a transparent refinement stage, and what Warp export is for

**Status:** rev 2, approved 2026-09-29. Stages 1–3 **code-complete, NOT RUN** (branch `missalignmnet`,
2026-09-29); first run = `bullseye_artifact_two` (7 TS). Stages 4–6 open. Stage 7 (§7, what a run changed, shown in
the Journey): 7a and 7b built 2026-10-27, NOT RUN; 7c open. Tool reference: `docs/miss-alignment.md`.

**Progress 2026-09-29 (read before the first run):**
- **Every earlier run trained on mis-projected data.** The dim stamp took the image size from the settings'
  `HeaderlessWidth/Height` (7676×7420 — Warp's value for headerless raw formats, meaningless for EER/TIFF/MRC);
  the real images were 4096×4096 (stack cell 6348.8 Å), so warpylib's image centre was off by ~2700 Å. The
  Vermicelli logs show `aligned == misaligned` on every batch. The stamp now reads each stack's MRC cell.
- The container wrapper exports `TQDM_DISABLE=1` (roadmap 20); the tool's progress bars are its only output
  during training, so the 45-min idle watchdog would have killed it. The train command unsets it.
- An IO-tab override to a producer that has not run yet is stored as `type:External/pending_<id>`; at deploy the
  producer gets `External/jobNNN`, the key no longer matches, and the consumer silently falls back to automatic
  selection (`resolve_inputs`). Manual routing to an unrun missAlign therefore never worked; stage 3 replaces it.
  The latent override bug itself is not fixed (it affects any override to an unrun producer).
- Stage 1: scope = input star's TS; XMLs copied, stacks symlinked (`link_tree`); resume from the highest
  `iterN/model.ckpt`; success = `iter{len(schedule)}/model.ckpt`; unconfigured tool already raises (roadmap 20).
- Stage 2: `services/tilt_series/adapters/miss_align.py` (XML → `TsAlignmentTiltSeriesOutput`, inherited
  `emit_star`). The mapping reproduces aligntiltsWarp's star on `bullseye_artifact_two` job004 (69 tilts, 2 TS):
  YTilt exact, ZRot ≤ 0.025°, shifts ≤ 1.2 Å (checked with awk against the XMLs; the driver re-checks every run
  before training and also runs the full ingest + emit into a scratch dir). Upstream source confirms `global` /
  `anchoring` write `AxisOffsetX/Y`, `[N,N]` writes `GridMovementX/Y`; Angles/AxisAngle are not refined.
  missAlign's `warp_tiltseries_settings` output slot is gone (its copy has no `tomostar/` sibling).
- Stage 3: `JobSpec.refines` + `PathResolutionService._follow_refinements`; `pipeline_order` now tracks the
  roster on add/remove. Not done: the IO-tab label "(refines aligntiltsWarp)".
- A/B in one project: add `tsCtf__2` / `tsReconstruct__2` next to the finished baseline pair.
- First run started 20:47 on clip-g4-2 (2× A100, fast, 15 epochs): stamp 6348.8 Å, XML → star check over 252
  tilts (ZRot ≤ 0.025°, shifts ≤ 1.6 Å), pre-flight emit 7/7, ~1.2 s/step.

**Open after the first run (review findings, none blocking):**
- Registry across checkouts: a checkout without `"miss_alignment"` in `TsAlignmentTiltSeriesOutput.alignment_method`
  skips those TS sidecars on load and drops them from `index.json` on its next save (sidecars survive). Keep
  older checkouts (cluster install, `dl_filter` clone) off missAlign projects until this branch is everywhere.
- Diff `iter0/` against the final XMLs: confirm `anchoring` writes only `AxisOffsetX/Y` (not `GridAngle*` or a
  1×1×N `GridMovement`); `global` and `[N,N]` are confirmed from source.
- Resume: `iterN/model.ckpt` is copied last in the tool's per-iteration snapshot, so it does mark a finished
  iteration; still missing is a stage manifest (input star + mtime + TS list) that forces a restage when the
  upstream alignment changed.
- The finished baseline tsCtf now displays missAlign as its source in the IO tab (it ran on job004, and would
  follow missAlign if re-run). Overriding only one of `input_star` / `input_processing` pairs a star from one
  alignment with XMLs from the other, unwarned.
- Nits: `_follow_refinements` should take the consumer type from its job model (`InstanceId.parse` raises
  on unknown ids); `report_local_warps` should skip a missing XML instead of failing the finished run; the
  `MissAlignParams` docstring still describes manual routing; IO-tab label "(refines aligntiltsWarp)".

**Maintainer's requirements (2026-09-29):**
1. Miss-alignment is optional. **No other job may be affected by it, assume it runs, or cater to it in
   its outputs.**
2. Schedules/iterations/defaults are the maintainer's call after reading the paper; so far no
   schedule and project have carried it. The job must make trying a set of defaults across several
   projects cheap.
3. Set up the plumbing (container, config) now.
4. Explain the "Warp extraction" discussion: the collaborators' point was that *local deformations* are
   carried by one path and not the other.

## 1. Where it stands

- Fully coded, never finished a macro-iteration inside crboost. Cluster-install runs died at the stamp
  step: the cluster `conf.yaml` has no `tools.miss_alignment`, so `get_tool_config` silently fell back to a
  bare binary on the host python, which has no torch (`config_service.py:363`).
- Driver defects: the success check passes on the `iter0/` baseline upstream writes before training
  (`drivers/miss_align.py:334-339`); resume counts `iter0/` and asks for one iteration too many (`:224-227`).
- Its output is consumed by nothing: tsCtf binds `aligntiltsWarp` by `preferred_source`
  (`services/jobs/ts_ctf.py:29-30`).
- Its output is **not a drop-in** for aligntiltsWarp's:
  - the star is copied verbatim (`drivers/miss_align.py:240`) — its geometry disagrees with the refined
    XMLs, and the relative `tilt_series/<ts>.star` files it references are never written, so tsCtf's emit
    (`adapters/ts_ctf.py:123-127`) would fail only after every CTF task had run;
  - it refines every `*.xml` it finds (the tool globs the dir), including tilt series whose alignment
    failed and muted ones, instead of the tilt series in its input star;
  - it copies the whole `warp_tiltseries/` including the tilt stacks (duplicate data on disk).
- Needs from upstream, all from aligntiltsWarp: settings (dims stamped into the XMLs), the XMLs +
  `tiltstack/<ts>/<ts>.st`, the star. Not needed: tomostars, frames, gain (only for `--prepare-stacks`,
  which the driver refuses).

## 2. Design: a declared refinement, followed by the resolver

### 2.1 The rule
Automatic input selection stays exactly as it is. After it picks producer C of type T for a consumer:
if a job R **in the pipeline**, **before the consumer in JobSpec order**, **declares `refines ∋ T`**, and
**R's own effective input for T is exactly C**, the consumer is bound to R's T output instead. Repeat, so
refinements chain. User overrides bypass the rule.

```
   aligntiltsWarp ──(star, xml dir)──► tsCtf              missAlign absent: unchanged
   aligntiltsWarp ──► missAlign ──(star, xml dir)──► tsCtf  missAlign present: tsCtf follows the chain
                  └──(settings, tomostar/)───────► tsCtf  settings are not refined: still aligntiltsWarp
```

tsCtf keeps its `preferred_source="aligntiltsWarp"` and never learns missAlign exists. Resolution runs
in one function (`_choose_candidate_for_slot`, `services/path_resolution_service.py:904-954`) that
`resolve_inputs`, `resolve_edges`, the IO tab's validation and the drivers' on-node re-resolution
(`drivers/driver_base.py:103-125`) all share, so paths, afterok edges and the UI move together.

### 2.2 Why not "nearest upstream producer wins"
It would change 11 of the 28 input slots. Beyond the wanted tsCtf binding it would move tsCtf's and
tsReconstruct's **settings** to missAlign (no `tomostar/` sibling → every task fails "Tomostar not
found", `drivers/ts_ctf.py:243-246`, `drivers/array_job_base.py:439-445`), chain templatematching #2 onto
#1's pass-through copy, and cross-wire multi-instance extraction/class3d branches. And refinement must be
**declared**, never inferred from "consumes T, produces T": TM, subtomoExtraction, class3d, tsCtf and
tsReconstruct all have that shape.

### 2.3 Touch points
1. `services/jobs/spec.py:44-71`: `JobSpec.refines: tuple[JobFileType, ...] = ()`; the MISS_ALIGN row
   (`:126-133`) sets `(ALIGNED_TILT_SERIES_STAR, WARP_TILTSERIES_DIR)`.
2. `path_resolution_service.py:904-954`: wrap the choice in `_follow_refinements(candidate, consumer)`;
   membership from `state.pipeline_order`, order from `PIPELINE_ORDER` (`spec.py:228`).
3. `pipeline_order` tracks the roster: today adding a job does not update it and removing a SUCCEEDED
   job does not prune it (`ui/pipeline_builder/pipeline_builder_panel.py:317-321, 436-442`).
4. The IO tab shows the binding as "missAlign (refines aligntiltsWarp)".

**Must not change behaviour:** aligntiltsWarp, tsReconstruct, templatematching, candidate extraction,
subtomoExtraction, reconstructParticle, class3d, the denoise jobs — and tsCtf whenever missAlign is absent.

### 2.4 Drop-in output
missAlign must emit exactly what aligntiltsWarp emits, so consumers cannot tell the difference:
- **Star + `tilt_series/` with the refined rigid geometry,** via an ingest adapter: refined XMLs →
  `TsAlignmentTiltSeriesOutput` under missAlign's instance id → reuse `TsAlignmentIngestAdapter.emit_star`
  and `_apply_alignment_to_tilt_df` (`services/tilt_series/adapters/ts_alignment.py:144-265, 524-594`),
  which also brings identity resolution and the tilt-filter drops. Widen
  `alignment_method: Literal["aretomo","imod"]` (`services/tilt_series/models.py:163`).
- **Mapping,** checked read-only on an existing aligntiltsWarp job (2 TS × 41 tilts): rlnTomoYTilt =
  −`Angles` exactly; rlnTomoZRot vs `AxisAngle` ≤ 0.015°; X/YShiftAngst vs `AxisOffsetX/Y` ≤ 1.0 Å, same
  sign; rlnTomoXTilt = 0. So **aligntiltsWarp's own XML/star pair is the regression fixture**: the
  converter run on its XMLs must reproduce its star.
- Refine only the tilt series in the input star; symlink the tilt stacks instead of copying (verify the
  tool never writes them).
- Unverified: whether miss-alignment writes rigid changes into `Angles`/`AxisAngle`/`AxisOffset` or into
  single-node `Grid*` values — the converter must combine both. The paper/code answers it.

## 3. Local deformations — what "Warp extraction" is about

**Two geometry channels leave alignment:**
```
                      ┌─► WarpTools: ts_ctf, ts_reconstruct ─► tomograms ─► TM ─► picks
  Warp XML  (rigid + grids) ─┤
                      └─► WarpTools ts_export_particles ─► particles        (not in crboost)
  RELION star (rigid only) ─────► relion_tomo_subtomo ─► particles ─► Refine3D / Class3D
```

A Warp tilt-series XML holds the rigid per-tilt model (`Angles`, `AxisAngle`, `AxisOffsetX/Y`) **plus
grids**: `GridMovementX/Y` (per-tilt 2D image warps — the "local deformations"), `GridAngleX/Y/Z`,
`GridVolumeWarpX/Y/Z`. In every project so far they are 1×1 zeros (checked on a missAlign job's XMLs).
Miss-alignment's default and thorough schedules end with `[3,3]` "image-warping grid" iterations, which
presumably write real 3×3 `GridMovement` grids.

- **Warp applies the grids** — `ts_reconstruct` (so tomograms, TM and picks see them) and
  `ts_export_particles`.
- **The RELION star cannot hold them** — five rigid columns per tilt. RELION has its own image-deformation
  model (estimated by its own tomo frame alignment), a different parameterisation that crboost never
  writes and nothing converts to.
- **Consequence** after a `[3,3]` run: picks come from deformation-corrected tomograms, and RELION
  extraction cuts particles with rigid geometry only — off by the local deformation at each tilt.

**So a Warp export job would be an additional producer**, not a replacement: same outputs
(`particles.star`, `tomograms.star`, `optimisation_set.star`) as subtomoExtraction, so reconstructParticle
and Class3D take either. With rigid-only geometry — every project today — both paths cut the same
particles and there is nothing to gain. It only earns its place once alignments carry non-trivial grids,
i.e. once miss-alignment's grid iterations (or later M) prove useful. *(Corrected 2026-10-11: the all-zero
map was relion_tomo_reconstruct_particle re-extracting from Warp's dummy tilt series, not a star-format
problem. What dev36 does get wrong is fixed upstream in Warp 2.0.0dev40; the job runs dev41. See roadmap 24
stage 4.)*

**Until then:** when a project's refined XMLs carry non-trivial grids and RELION particle jobs are
downstream, those jobs show a warning ("local deformations present — RELION extraction ignores them"),
with the measured grid magnitude. Whether to run grid iterations at all becomes a visible choice, not an
accident.

## 4. Plumbing — what to install and configure

Nothing to install on the host: the job runs entirely inside its container (torch 2.8, torch-projectors,
miss-alignment), including the stamp step.

1. Cluster install `config/conf.yaml` (`/groups/klumpe/software/crboost_server/config/conf.yaml`) — add
   what the dev `conf.yaml` already has:
   ```yaml
   tools:
     miss_alignment:
       exec_mode: "container"
       container_path: /groups/klumpe/software/containers/sifs/miss_alignment_torch2.8.0_cuda12.9.sif
   job_resource_profiles:
     missAlign:
       gres: gpu:1
       mem: 48G
       cpus_per_task: 8
       time: '2:00:00'   # a floor; the job scales it by n_ts × macro-iterations, clamped to the QOS
   ```
2. GPU check, on a GPU node:
   `srun -p g --gres=gpu:1 --time=0:05:00 apptainer exec --nv <sif> python -c "import torch, miss_alignment; print(torch.__version__, torch.cuda.is_available())"`
3. Record the tool version for the def: `apptainer exec <sif> pip show miss-alignment`.

## 5. Stages

### Stage 0 — Plumbing (user-run, §4)
- *Success:* GPU check prints `True`; version recorded here.

### Stage 1 — Driver correctness
- Success check requires `iter{len(schedule)}/model.ckpt`; resume from the highest N ≥ 1 with a
  checkpoint; refine only the input star's tilt series; symlink stacks; unconfigured tool fails loudly
  (roadmap 20 stage 1 — land the one-liner here if 20 has not).
- *Success:* a fast schedule on `412FCIso_Pos28_4` (one TS) writes `iter1/`, `iter2/`; a job cancelled
  after iter1 resumes at the next iteration.

### Stage 2 — Drop-in output (§2.4)
- *Success:* the converter reproduces aligntiltsWarp's star from its XMLs within the tolerances above;
  missAlign's star + `tilt_series/` pass tsCtf's emit.

### Stage 3 — Transparent binding (§2.1–2.3)
- *Success:* with missAlign in the roster, tsCtf's IO tab shows "missAlign (refines aligntiltsWarp)" and
  the afterok edge follows; remove it and tsCtf binds aligntiltsWarp again; no other job's resolved inputs
  change (diff every job's resolved paths with and without missAlign on a full copia roster).

### Stage 4 — Local-deformation visibility (§3)
- Measure `GridMovementX/Y` magnitudes (Å) in the refined XMLs; warn on downstream RELION particle jobs
  when they are non-trivial.
- *Success:* after a schedule with a `[3,3]` iteration the warning appears with a number; after a
  rigid-only schedule it does not.

### Stage 5 — The maintainer's defaults across projects
- The maintainer picks the schedule/steps/epochs after reading the paper; the job exposes them as a
  preset plus overrides. Runs on 412 and copia; tomograms with and without refinement compared in the
  gallery.
- *Success:* at least one project where the refined tomogram is visibly better, with its settings recorded.

### Stage 6 — Warp export job (only if stage 5 keeps grid iterations and stage 4 shows non-trivial grids)
- New producer of the optimisation-set outputs next to subtomoExtraction; `ts_export_particles --2d` +
  a RELION 5 star built around Warp's particle stacks; compared with RELION extraction on the same particles
  (gold-standard FSC). **Built 2026-10-11 as roadmap 24 stage 4 (NOT RUN).**

### Stage 7 — What a run changed, in the Journey (§7)
- 7a: the Refinement section: stat tiles, the change-per-tilt chart, the warp strip and its zoom.
- 7b: blink / wipe between the registered reconstructions in the Reconstruct card.
- 7c: the iteration scrubber (only if 7a reads well).
- *Success:* on AGG412 Grid2 (job022) Position_6 shows its 121.8 Å tilt ringed and its 3×3 arrows; a series
  refined without `[3,3]` rounds says "no local warp" and shows no arrows; the blink makes the refined slab's
  sharper features visible without reading a number.
- **Progress 2026-10-27 — 7a + 7b built, NOT RUN** (ruff only). Decisions: one exaggeration per project (from
  the largest `local_warp.max_angstrom` over every missAlign report of the project), blink (no wipe), 7c later.
  - Checked against the source and job022's XMLs: `<Node>` carries `X`/`Y`/`Z` (Z = the XML's tilt row, X
    fastest); node i of N sits at `margin + i·(1 − 2·margin)/(N − 1)`, margins 0 here, so a 3×3 grid has nodes
    on the image's corners, edge midpoints and centre; `iter0/` holds a 1×1×1 zero grid. warpylib
    `get_position_in_all_tilts` **adds** `AxisOffset` and **subtracts** the movement grid, so the whole-tilt
    shift drawn is ΔAxisOffset **minus** the change of the grid's mean (the handoff said plus), and the warp
    arrows are minus the deformation: every arrow shows where image content moved. Thumbnails are not flipped
    (filterTilts `ImageProcessor`), so grid Y runs down the tile.
  - `services/analysis/alignment_changes.py`: `movement_grid`, `tilt_geometry` (parse cached by mtime),
    `refinement_changes` (rigid fit over the total whole-tilt shift), `interpolated_field` (natural cubic
    splines through the nodes; Warp's B-spline ends may differ — display only).
  - `ui/tomo_dashboard_dialog.py`: section `ts_refine` after Alignment, one card per missAlign job with a
    registry alignment output for the series; renders a placeholder and fills from a thread. Tiles: change
    rms, largest change and its tilt, re-centring, local warp, tilts over 2 px. Chart: shift change and warp
    rms, 2-px `ref` line. Because the grid mean is included, the numbers can differ slightly from
    `missalign_changes.json`'s (AxisOffset only).
  - `ui/dashboard/warp_strip.py`: the strip (64 px thumbnails, warm node arrows, gray-blue hollow whole-tilt
    arrow stopped at the tile's edge, ringed over the threshold) and the zoom (12×12 field from a worker,
    ← →).
  - 7b: in compare mode each non-reference pane stacks the reference slab under its own; holding the
    pointer, or Space with the box focused, shows the reference.
  - Owed at runtime: the success checks above; whether the arrow lengths read at ×N on a 64 px tile.

## 6. Side finding (outside this roadmap — verify)
Species match in candidate scoring only counts for **succeeded** candidates
(`path_resolution_service.py:944`). In a fresh multi-species afterok run nothing has succeeded yet, so
e.g. `tmextractcand__A` could bind `templatematching__B` when B has the higher job number — unless
species instances pin their inputs by override at creation. Worth one check on a two-species project.
**Reproduced 2026-10-05 and fixed by roadmap 28** (`completed/roadmap_28-species-aware-io-wiring.md`):
species and stage order are hard filters, ties are stated, and job numbers no longer rank.

**Modern-Python weave-in:** `refines` as a `tuple[JobFileType, ...]` on the frozen `JobSpec`;
`_follow_refinements` as a small pure function (candidate, pipeline, specs) → candidate, trivially testable.

## 7. What a run changed, shown in the Journey (scoped 2026-10-27; 7a + 7b built, not run)

The maintainer asked for a per-tilt-series view, in the Journey, of how missAlign changed an alignment: the local
deformations and the finer per-tilt corrections, visual rather than a table of numbers.

**Data — all on disk after every run, nothing new for the job to write:**
- The missAlign job's `warp_tiltseries/iter0/<ts>.xml` (the alignment as the tool got it), `iter<k>/<ts>.xml`
  (after macro-iteration k) and `<ts>.xml` (final). Per tilt: the rigid `AxisOffsetX/Y` (Å) and, after `[N,N]`
  rounds, `GridMovementX/Y`: W×H×tilts nodes (Å), one 2D B-spline image warp per tilt on a W×H lattice over
  the image. `[N,N]` rounds do not touch `AxisOffset`, so any whole-tilt shift they want lands in the grid's mean.
- `missalign_changes.json`: per tilt, the shift change with the rigid volume shift removed
  (`services/analysis/alignment_changes.py`), the 2-px threshold, the warp's rms / max.
- The tilt thumbnails the Alignment section's hover cards already use (`tilt_thumb_urls`).
- Both reconstructions (baseline tsReconstruct, tsReconstruct__2), which the Reconstruct card's compare mode
  already registers onto one plane (`matched_slabs`, `vis/compare/*.png`).
- Scale, from the Grid2 run (roadmap 29 §6.2): rigid-removed per-tilt change median 16.7 Å rms, max 121.8 Å;
  3×3 warps 8–44 Å rms per series on 12.4 Å stacks, i.e. 1–3.5 px. Big enough to see.

**Look — a "Refinement" section after Alignment, on tilt series a missAlign job refined** (the sketch's
numbers are made up):
```
 Refinement · missAlign job022 · PAPER                                                   ⓘ
 [change 16.7 Å rms] [max 121.8 Å @ +54°] [re-centring 140 Å] [warp 16 Å rms, 44 max] [4 tilts > 2 px]

 Change per tilt (Å)       ● shift change, rigid removed    ▲ warp rms     - - 2 px
  120 ┤                                                             ●
   40 ┤ ●                                                        ▲  ●
   20 ┤ ▲ ●  ▲ ●    ●  ▲    ▲  ●   ▲ ●   ●  ▲   ●  ▲  ● ▲  ●  ▲
    0 ┼──────────────────────────────────────────────────────────────
      -60          -30            0            +30           +60

 Local warp per tilt (arrows ×20; ⟶ = 20 Å)
 ┌─────┐┌─────┐┌─────┐┌─────┐┌─────┐┌─────┐┌─────┐ ...   one tile per tilt, angle order
 │↗ → ↘││→ → ↘││↗ ↗ →││  ·  ││· · ·││· → ·││↘ ↓ ↓│       tilt thumbnail faint underneath
 │↗ ⇢ →││→ ⇢ ↘││→ ⇢ →││· ⇢ ·││· · ·││· · ·││↘ ↓ ↙│       ⇢ = the tile's whole-tilt shift
 │↑ ↗ →││→ ↘ ↓││→ → →││  ·  ││· · ·││· · ·││↓ ↓ ↙│       ringed = over 2 px
 └─────┘└─────┘└─────┘└─────┘└─────┘└─────┘└─────┘
  -60°   -57°   -54°   ...     0°            +54°
```
1. Stat tiles: rigid-removed change rms / max (and its tilt), the re-centring (gauge) rms, warp rms / max, tilts
   over 2 px; the job's schedule in the ⓘ.
2. Change per tilt, markers only (dashboard plot rules): x = tilt angle, y = Å; the rigid-removed shift change and
   the warp's rms over its nodes; dashed 2-px line; hover = tilt #, movie, both numbers.
3. The warp strip: one ~72 px tile per tilt in angle order, wrapping to 2–3 rows; the thumbnail faint under an
   arrow per grid node (the node's displacement with the tile's mean removed), one exaggeration for the whole
   tomogram stated once with a scale arrow, one sequential colour ramp by magnitude; the tile's whole-tilt shift
   (rigid change + grid mean) as one hollow arrow from the centre; tilts over the threshold ringed. A click opens
   a zoom: the tilt at full size with the field interpolated from the B-spline (a ~12×12 quiver, or a magnitude
   map), ← → stepping the tilts.
4. Blink in the Reconstruct card: the registered baseline and refined slabs stacked in one box; holding the
   pointer on it (or a key) shows the other, or a wipe handle drags between them. Uses the PNGs `matched_slabs`
   already writes.
5. Iteration scrubber (7c): a slider over the macro-iterations redraws 2 and 3 from `iter0 → iter<k>`, so the
   anchoring / global rounds moving tilts and the `[3,3]` rounds bending them read apart.

**Rules:** the rigid volume shift is removed before anything is drawn (it only moves the tomogram in its box);
the grid's per-tilt mean is shown apart from its deformation; the exaggeration is on screen; 2D only, no volume
rendering. Arrow direction follows Warp's sign convention, checked once against a tilt whose `AxisOffset` moved.

**Where it runs:** the XML parse (iter0 + final, k+1 with the scrubber; a few kB each) off the loop
(`asyncio.to_thread`), cached by XML mtime; arrows as inline SVG over the thumbnails in the browser (~41 tiles ×
9 arrows); the zoom's interpolation in `run_in_worker(WorkKind.RENDER, …)`.

**Files:** `services/analysis/alignment_changes.py` (`movement_grid(xml)` → W, H and per-tilt node vectors; the
mean / deformation split), `ui/tomo_dashboard_dialog.py` (`_render_ts_refine_section`, section key `ts_refine`),
`ui/dashboard/figures.py` (the chart), a new `ui/dashboard/warp_strip.py` (strip + zoom), the blink in
`_render_recon_versions`.

**Not in scope:** `GridVolumeWarp` / M's 4D warps; changing the job; Warp export (§3, stage 6).

**Decisions owed:** (a) exaggeration per project (tilt series comparable) or per tilt series (tiles filled) —
recommend per project; (b) blink or wipe — recommend blink, it shows a change best; (c) 7c now or later.

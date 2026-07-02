# Preprocessing metrics inventory

**What this is.** A grounded catalog of *every* per-tilt / per-tilt-series / per-tomogram readout
that our WarpTools + AreTomo preprocessing jobs actually produce — what each one means (in plain
language, with ready-to-paste tooltip text), where it physically lives, whether it is a **real fitted
value or a placeholder that lies**, whether we currently show it, and how it should be plotted (with
the CryoSPARC analog where one exists).

**Why it exists.** The journey view currently plots a handful of per-tilt metrics and several of its
panels render empty because they read placeholder columns. We want to (a) understand what each number
means, (b) surface the rich data we're not reading, and (c) make the journey more informative *and*
decluttered. This doc is the reference for that work; the UI-integration and tilt-preview-overlay
work builds on it.

**How it was built.** Every metric below was read out of real completed runs
(`projects/try2_after_pixShift`, `projects/pos9_10_after_pixShift`) — RELION per-tilt stars, WarpTools
frameseries/tiltseries XML, AreTomo `.aln`, IMOD `.xf/.tlt`, tomostar, `tmResults` JSON, and
`particles.star` — then a second adversarial pass re-opened the files to catch wrong meanings, units,
and placeholder-mislabels. Corrections from that pass are folded in. Sample values are from
`try2_after_pixShift_Position_11_2` (39 tilts, 1.55 Å/px frame, 6.2 Å/px recon, 300 kV, dose-symmetric)
unless noted.

> ⚠️ **Single-TS ground truth.** Most samples come from one tilt-series. Cross-TS *distributions*
> (max-shift spread, resolution ranking, handedness spread) — which is exactly what a curation
> overview needs — must be validated on a multi-TS project (`pos9_10`, `External/job002..010`) before
> those aggregates are trusted.

---

## 0. The three things to take away first

1. **The RELION star lies about motion and CTF quality.** WarpTools' RELION-star export writes
   **hard placeholders** into four of the columns a dashboard most wants:
   `rlnAccumMotionTotal/Early/Late` and `rlnCtfMaxResolution` come out as **`1e-6`**, and
   `rlnCtfFigureOfMerit` is the literal string **`None`**. Three of our current journey panels
   (CTF-max-res, accumulated-motion, CTF-FoM) read these columns and therefore **render empty every
   time**. The *real* values live only in the WarpTools XML. → [§4](#4-the-placeholder-trap).

2. **The highest-value data is XML-only and currently unread.** `CTFResolutionEstimate` (real
   per-tilt CTF fit resolution), `MeanFrameMovement` (real per-tilt motion), the `GridMovementX/Y`
   motion track, the `PS1D` radial power spectrum (the CryoSPARC-style CTF-fit curve), the per-tilt
   `AreTomo shift` magnitude, and the tomostar `AverageIntensity` (dark-tilt detector) are all sitting
   on disk, fully real, and surfaced *nowhere*.

3. **A tilt-series is not a micrograph.** Almost every CryoSPARC "one value per exposure" readout
   becomes, for us, a **curve vs tilt angle within one TS** *plus* a **per-TS aggregate across the
   dataset**. The single most useful thing to build is CryoSPARC's **exposure-curation scatter +
   threshold sliders**, transposed to one-point-per-tilt-series. → [§5](#5-cryosparc-visualization-mapping).

---

## 1. How to read the catalog

Each metric is tagged:

- **Scope** — `per-frame` (sub-tilt), `per-tilt`, `per-TS` (one value for the whole tilt-series),
  `per-tomo`, `per-particle`, or `input` (a user-set parameter, not a readout).
- **Shape** — `scalar`, `curve` (values vs tilt/frequency), `grid` (2-D/3-D field), `vector`.
- **Real?** — ✅ real fitted/measured value · 🟡 real but constant/redundant (low plotting value) ·
  ⛔ **placeholder / not populated** (do not plot) · ❓ present but semantics unconfirmed (needs a
  domain call — see [§8](#8-open-questions-for-the-domain-expert)).
- **Shown?** — ✅ plotted now · ◐ partial (read but not charted, or available via handoff) ·
  ❌ not surfaced.

**Legend for "Source" paths** (per External job dir):

| Source | Path | What it is |
|---|---|---|
| per-tilt star | `<job>/tilt_series/<TS>.star` | one row per tilt; RELION columns |
| top star | `<job>/{fs_motion_and_ctf,aligned_tilt_series,ts_ctf_tilt_series,tomograms}.star` | per-TS + `data_global` optics |
| frameseries XML | `job002/warp_frameseries/*.xml` | one per tilt-movie; **real motion + CTF fit + PS1D** |
| tiltseries XML | `job003/job004 warp_tiltseries/*.xml` | TS-level CTF grids, per-tilt `TiltPS1D`, geometry |
| AreTomo aln | `job003/warp_tiltseries/tiltstack/<TS>/<TS>.st.aln` | alignment shifts / rotation / refined tilt |
| IMOD | `.../<TS>_Imod/<TS>_st.{xf,tlt}` | same solution in IMOD form |
| tomostar | `job003/tomostar/*.tomostar` | `_wrpDose`, `_wrpAverageIntensity`, `_wrpMaskedFraction` |

---

## 2. Data-source map by job

| Job (this project) | Type | Primary outputs | Real readouts live in… |
|---|---|---|---|
| job002 | **FS Motion + CTF** | `fs_motion_and_ctf.star`, `warp_frameseries/*.xml`, per-tilt star | **frameseries XML** (motion, CTF-res, PS1D); star has real defocus but placeholder motion/res |
| job003 | **TS Alignment** (AreTomo) | `aligned_tilt_series.star`, `.aln`, `tomostar`, tiltseries XML, per-tilt star | **`.aln`** (shifts, refined tilt), **tomostar** (intensity), per-tilt star (real shifts/angles) |
| job004 | **TS CTF** | `ts_ctf_tilt_series.star`, `warp_tiltseries/*.xml`, per-tilt star | **tiltseries XML** `GridCTF` (per-tilt defocus curve), `CTFResolutionEstimate` (per-TS) |
| job005 | Reconstruct | `tomograms.star`, `reconstruction/{even,odd}` | star (voxel size, dims, binning, handedness, half-maps) |
| job006 | Template match | `tmResults/*.{mrc,json}`, `candidates.star` | `job.json job_stats` (background σ, search space), scores.mrc |
| job007 | Extract candidates | `candidates.star`, `vis/preview/*` | star (`rlnLCCmax`, cutoff, coords, orientations), picks.json (nn, z-depth) |
| job008 | Subtomo extract | `particles.star` | star (visible-frames mask, box/crop, 2D-stack flags) |
| — | **Tilt Filter** | `TiltFilter/tilt_series_labeled/<TS>.star`, thumbnails | star (real defocus/dose/shifts); **labels live in `project_params.json`, not the star** |
| — | Denoise (train/predict) | *(none persisted)* | **log-only** — no SNR/PSNR metric exists |

---

## 3. Per-job metric catalogs

### 3.1 FS Motion + CTF (job002)

Per-movie (= per-tilt here; 1 EER file per tilt) motion correction + per-movie CTF fit. The EER is
internally fractionated into **7 frame-groups** (`EERGroupFrames=32`, `Dimensions Z=7`) — the star's
`rlnTomoTiltMovieFrameCount=1` hides that; the true sub-frame count is XML-only.

| Metric | Source | Scope · Shape · Units | Real? | Shown? | Meaning / tooltip |
|---|---|---|:--:|:--:|---|
| **rlnDefocusU / V** | per-tilt star #10/#11 | per-tilt · scalar · Å | ✅ | ✅ | Fitted defocus. **WarpTools ties U == V** (both = mean defocus); astigmatism is *not* U−V here. *Tooltip: "Fitted defocus for this tilt (Å); larger = further from focus."* Sample 45 725 Å, dips to ~12 000 Å at ±45–69° where the fit degrades. |
| **rlnCtfAstigmatism** | per-tilt star #12 | per-tilt · scalar · Å | ✅ | ✅ | Astigmatism magnitude (= `DefocusDelta`×1e4). **This** is where real astig lives. Near-zero on low-SNR tilts = collapsed fit. *"Astigmatism magnitude (Å); near-zero/huge values on weak tilts are unreliable."* |
| **rlnDefocusAngle** | per-tilt star #13 | per-tilt · scalar · ° | ✅ | ❌ | Astigmatism axis orientation. Only meaningful where magnitude is non-trivial. *"Astigmatism axis angle (°)."* |
| **CTFResolutionEstimate** ⭐ | frameseries XML `Movie@` | per-tilt · scalar · Å | ✅ | ❌ | **The real per-tilt CTF fit quality** — resolution to which Thon rings were reliably fit (lower = better). This is what `rlnCtfMaxResolution` *should* hold. 5.7 Å @ 0° → 10 Å @ −45°. *"Resolution the CTF was fit to for this tilt (Å) — lower is better."* |
| **MeanFrameMovement** ⭐ | frameseries XML `Movie@` | per-tilt · scalar · ❓Å (not px) | ✅ | ❌ | **The real per-tilt beam-induced motion.** Rises with tilt (1.6 → 4.4). Units unlabeled in the file — Warp convention is **Ångström**, not pixels (see [§8](#8-open-questions-for-the-domain-expert)). *"Average beam-induced motion for this tilt — higher = more drift/charging."* |
| **GridMovementX / Y** ⭐ | frameseries XML | per-frame · vector · ❓Å | ✅ | ❌ | The intra-tilt drift **track**: X/Y at each of 3 temporal motion-spline control nodes (`m_grid` Depth=3) spanning the 7 EER frame-groups. Sample X `[2.57,−1.27,−1.30]`. *"Beam-induced drift path within a tilt."* → the classic motion-trajectory plot. |
| **GridCTF (2×2)** | frameseries XML | per-tilt · grid · µm | ✅ | ❌ | Per-tile defocus across the field (`c_grid` 2×2) — the defocus gradient; only its mean reaches the star. ~0.17 µm spread. *"Defocus across the image (2×2) — the defocus ramp."* |
| **PS1D** ⭐ | frameseries XML `<PS1D>` | per-tilt · curve · power vs freq | ✅ | ❌ | Radial (rotationally-averaged) power spectrum — the Thon-ring profile the CTF was fit against, ~256 points 0→Nyquist. Pairs with `SimulatedBackground`/`SimulatedScale`. *"Radial power spectrum used for the CTF fit."* → CryoSPARC's CTF-fit overlay. |
| **rlnMicrographPreExposure** | per-tilt star #5 | per-tilt · scalar · e⁻/Å² | ✅ | ◐ | Accumulated dose *before* this tilt (dose-order, not tilt-order). 0 → 182.4. *"Cumulative dose before this tilt (e⁻/Å²) — drives dose weighting."* Best used as an **alternate x-axis / color** for other metrics. |
| **rlnTomoNominalStageTiltAngle** | per-tilt star #3 | per-tilt · scalar · ° | ✅ | ✅ | Nominal stage tilt (from mdoc), pre-refinement. **The canonical x-axis.** Span −45.01° → **+68.99°**. |
| rlnTomoNominalTiltAxisAngle | per-tilt star #4 | per-TS · scalar · ° | ✅ | ❌ | Nominal tilt-axis; refined by alignment. 84.36°. |
| rlnTomoNominalDefocus | per-tilt star #6 | per-TS · scalar · µm | ✅ | ❌ | Requested defocus at the scope (−5 µm). Use as a reference line under fitted defocus. |
| Global optics | top star `data_global` | per-TS · scalar | ✅ | ❌ | Voltage 300 kV · Cs 2.7 mm · ampl 0.10 · orig px 1.55 Å · TomoHand +1. Provenance strip, not a chart. |
| rlnCtfImage / MicrographName(Even/Odd) | per-tilt star #14/#7-9 | per-tilt · path | ✅ | ◐ | 2-D power-spectrum MRC + motion-corrected average + even/odd half-sets. Presence = completeness check; click-to-open. |
| **rlnCtfMaxResolution** | per-tilt star #18 | per-tilt · scalar | ⛔ | ◐(empty) | **Placeholder `1e-6`.** Use `CTFResolutionEstimate`. |
| **rlnAccumMotionTotal/Early/Late** | per-tilt star #15-17 | per-tilt · scalar | ⛔ | ◐(empty) | **Placeholder `1e-6`.** Use `MeanFrameMovement` + `GridMovement`. |
| **rlnCtfFigureOfMerit** | per-tilt star #20 | per-tilt · scalar | ⛔ | ◐(empty) | **Literal `None`.** No FoM emitted; nearest proxy is `CTFResolutionEstimate`. |
| SimulatedBackground / Scale | frameseries XML | per-tilt · curve | ❓ | ❌ | Fitted background floor + amplitude envelope for the PS1D fit. **Background is all-zero in this run** — confirm it's pre-subtracted, not a bug, before charting ([§8](#8-open-questions-for-the-domain-expert)). |
| Weight · MaskPercentage · PhaseShift · Bfactors · Unselect* | frameseries XML | per-tilt · scalar | 🟡 | ❌ | Constants / settings echoes in this run: `Weight=1`, `MaskPercentage=−1` (sentinel), `PhaseShift=0` (do_phase off), CTF `Bfactor=0`, `GridDoseBfacs=0` (the `−500` is the `m_bfac` *setting*, not a fitted value). `Unselect*` = per-tilt reject flags (all false here). **Not quality metrics** — surface only as provenance. |

**Traps.** ① `|DefocusU − DefocusV|` is always 0 — never compute astigmatism that way; use
`rlnCtfAstigmatism`. ② The three placeholder columns are the reason three journey panels are blank.

---

### 3.2 TS Alignment (job003, AreTomo)

Refines per-tilt shifts + tilt/rotation and writes them back into the per-tilt star (real) plus the
`.aln`. **Big honest finding:** AreTomo ran in **global mode** (`.aln NumPatches=0`), so its per-tilt
score columns `SMEAN/SFIT/SCALE` are hardcoded `1.00` and `BASE=0.00` — **there is no etomo/CryoSPARC
per-tilt alignment *residual* in this pipeline as configured.** The usable quality proxy is the
**refined shift magnitude**.

| Metric | Source | Scope · Shape · Units | Real? | Shown? | Meaning / tooltip |
|---|---|---|:--:|:--:|---|
| **Refined shift X/Y (rlnTomoXShiftAngst/YShiftAngst)** ⭐ | per-tilt star #24/#25 | per-tilt · vector · Å | ✅ | ✅ | The alignment solution in RELION convention = `.aln TX/TY × shift_angpix (6.2)` — pure scale, no sign flip. Zero-shift row = the alignment **reference tilt** (nominal ≈ 9°, *not* 0°). *"Refined per-tilt image shift (Å)."* |
| **Shift magnitude √(X²+Y²)** ⭐ | derived | per-tilt · curve · Å | ✅ | ✅ | **Best per-tilt alignment-quality proxy in global mode.** ~1–6 px mid-tilt, spiking to 60–90 px on the worst tilts. *"Total alignment shift; spikes flag hard-to-align / bad tilts."* Per-TS max = a headline QC number. |
| **Refined tilt (AreTomo TILT)** | `.aln` col 10 / `_st.tlt` | per-tilt · vector · ° | ✅ | ❌ | Tilt angle AreTomo actually used after its offset correction. Here `refined = nominal − 9°` uniformly. *"AreTomo-refined tilt angle."* |
| **Global tilt-offset** | derived `mean(refined − nominal)` | per-TS · scalar · ° | ✅ | ❌ | The stage-tilt zero-point / pretilt AreTomo fit for the whole TS (**−9.0°** here, uniform). A large value flags a mis-set stage or real pretilt. *"Per-TS stage-tilt offset AreTomo corrected for (°)."* |
| **rlnTomoYTilt** | per-tilt star #22 | per-tilt · vector · ° | ✅ | ✅ | **Convention confirmed:** `= −(AreTomo refined tilt) = offset − nominal` (adapter `ts_alignment.py:316`, `tilt_y_deg = −1·aln_TILT`). The applied projection tilt. |
| rlnTomoXTilt / ZRot | per-tilt star #21/#23 | per-tilt · vector · ° | 🟡 | ❌ | X-tilt = 0 (single-axis); Z-rot = tilt-axis 84.36° (constant). Scalar badges. |
| **Tilt-axis rotation (ROT / AxisAngle)** | `.aln` col 2 / XML | per-TS · scalar · ° | ✅ | ◐ | `.aln ROT` & star ZRot = nominal 84.36; XML `AxisAngle` = refined 84.375 (Δ 0.015°). run.out shows the axis stayed at 84.360 through refinement. *"Tilt-axis angle (nominal vs refined)."* |
| **Per-tilt average intensity** ⭐ | tomostar `_wrpAverageIntensity` | per-tilt · vector · a.u. | ✅ | ❌ | Mean image intensity. **Skewed** falloff — peaks slightly positive (~+3..+12°, ~1.50), asymmetric (−45°=1.09 vs +69°=0.85). A practical **dark-tilt detector**. *"Mean tilt intensity; low = dark/obstructed tilt."* |
| Per-tilt masked fraction | tomostar `_wrpMaskedFraction` | per-tilt · vector · 0–1 | ✅ | ❌ | Fraction masked (bad pixels / obstruction). 0 here; non-zero flags detector defects / beam edges. |
| Accumulated dose | tomostar `_wrpDose` / XML `Dose` | per-tilt · vector · e⁻/Å² | ✅ | ❌ | Cumulative dose (dose-symmetric; max **182.4**). Context axis / color, not quality. |
| **UseTilt (inclusion mask)** | tiltseries XML | per-tilt · vector · bool | ✅ | ❌ | WarpTools per-tilt keep/drop flag. **All True ×39 — no tilts dropped** (the earlier "37 vs 39 drop" was a parse artifact; corrected). The authoritative per-tilt selection signal. *"Whether this tilt is kept for reconstruction."* |
| FOVFraction | tiltseries XML | per-tilt · vector · 0–1 | ❓ | ❌ | Usable field-of-view fraction after shifts. **Non-monotonic** — minimum near zero tilt (~0.945), higher at extremes (not a simple high-tilt drop). Meaning unconfirmed ([§8](#8-open-questions-for-the-domain-expert)). |
| AreAnglesInverted | tiltseries XML `TiltSeries@` | per-TS · bool | ✅ | ❌ | Tilt-angle inversion / handedness flag (False here). Feeds the TomoHand chirality chain. |
| **rlnTomoHand** | `aligned_tilt_series.star` #7 | per-TS · scalar · ±1 | ✅ | ❌ | Handedness **exported into the pipeline star** (=+1 here), distinct from XML `AreAnglesInverted`. Load-bearing for the 412 chirality cascade. |
| SMEAN/SFIT/SCALE/BASE/GMAG | `.aln` cols 3,6-9 | per-tilt · vector | ⛔ | ❌ | **Unpopulated in global mode** (all 1.00/0.00). Only meaningful with patch tracking (`patch_x/y > 0`). Do **not** present as quality. |
| AxisOffsetX / Y | tiltseries XML | per-tilt · vector · ❓ | ❓ | ❌ | Large, erratic per-tilt values (X: +1.4 → −257). Semantics unconfirmed — hold ([§8](#8-open-questions-for-the-domain-expert)). |
| IMOD `_st.xf` | IMOD | per-tilt · matrix+shift | ✅ | ❌ | Same solution for IMOD/newstack; redundant with `.aln` shifts. |

**To get a real alignment residual** you'd need patch tracking (`patch_x/patch_y > 0`) or IMOD patch
alignment — that would populate `SMEAN/SFIT` with actual per-tilt error. Flag this to the user as a
config choice, not something we can recover from the current output.

---

### 3.3 TS CTF (job004)

Re-fits the CTF at the tilt-series level. What it genuinely computes anew: the **per-tilt defocus
curve** (`GridCTF`, 39 nodes), a **single per-TS astigmatism** (broadcast to every tilt), the **global
defocus handedness**, and a **per-TS CTF fit resolution**.

| Metric | Source | Scope · Shape · Units | Real? | Shown? | Meaning / tooltip |
|---|---|---|:--:|:--:|---|
| **Per-tilt defocus U / V** ⭐ | per-tilt star #10/#11 (from XML GridCTF ± DefocusDelta) | per-tilt · scalar · Å | ✅ | ❌ | TS-CTF-refined defocus; **supersedes** the FS-motion per-movie fit. U/V straddle the mean by the astigmatism. Forms the defocus-vs-tilt curve. Sample U 50 492 / V 40 827 Å. |
| **Per-tilt mean defocus (GridCTF)** ⭐ | tiltseries XML `<GridCTF Depth=39>` | per-tilt · curve · µm | ✅ | ❌ | The raw fitted defocus curve — **the primary new quantity.** ⚠️ **Nodes are in tilt-angle-ascending order (= tomostar / XML `Dose` order), NOT the star's acquisition order — join by movie filename, never by row index.** Collapses to ~1.2 µm at extreme high tilts = poor-fit signature (extreme \|tilt\| **and** high accumulated dose), not real defocus. |
| **CTF fit resolution (per-TS)** ⭐ | tiltseries XML `TiltSeries@CTFResolutionEstimate` | per-TS · scalar · Å | ✅ | ❌ | Best resolution the Thon rings were fit to for the whole TS (5.3 Å). **The real analog of the placeholder `rlnCtfMaxResolution`.** Per-TS only — no per-tilt scalar exists here (would need `TiltPS1D` overlap). *"CTF fit resolution for this tilt-series (Å, lower = better)."* |
| **Defocus handedness (TomoHand)** ⭐ | top star #7 / XML `AreAnglesInverted` / `ts_defocus_hand` | per-TS · scalar · ±1 | ✅ | ❌ | −1 = "flip" (`AreAnglesInverted=True`). **Decided globally across ALL tilt-series** by `ts_defocus_hand --check` (needs many TS for the correlation), then stored per TS. **Safety-critical** — a known root cause of bad picks on chiral templates. *"Defocus handedness (−1 flip / +1 no-flip), set globally."* Surface the global decision + its correlation sign, not per-TS in isolation. |
| **CTF astigmatism (mag + angle)** | per-tilt star #12/#13 / XML | per-TS · scalar · Å / ° | ✅ | ❌ | ⚠️ **Fit ONCE per tilt-series and broadcast constant** to all 39 tilts (9665 Å, 51°). A "per-tilt astigmatism" plot would be misleadingly flat — show as a scalar badge unless Warp is configured for per-tilt astig. |
| Per-tilt `TiltPS1D` + `TiltSimulatedScale` | tiltseries XML | per-tilt · curve | ✅ | ❌ | Measured vs simulated 1-D power spectrum per tilt (39 curves). **Overlap = the only per-tilt CTF-fit-quality diagnostic we have** (compensates for the missing FoM). → per-tilt CTF-fit overlay with a tilt selector. |
| Per-tilt accumulated dose | XML `Dose` / tomostar / star #5 | per-tilt · vector · e⁻/Å² | ✅ | ❌ | Same dose series; color the defocus-vs-tilt scatter by it. |
| Tilt-axis (AxisAngle) · PlaneNormal | tiltseries XML | per-TS · scalar / vector | ✅/❓ | ❌ | AxisAngle 84.375° (defocus-gradient direction). `PlaneNormal` (−0.026, 0.030, 0.999) ≈ specimen plane normal — report as a single "plane tilt from z" QC number, not the raw vector. |
| Tomo dims · optics | top star `data_global` | per-TS · scalar | ✅ | ◐ | 4096×4096×2048; 300 kV / Cs 2.7 / ampl 0.1 / 1.55 Å/px. Sanity-panel material. |
| Sample/ice **Thickness** | XML `<CTF> Thickness` | per-TS · scalar | ⛔ | ❌ | **= 0, not estimated** (driver passes no thickness-fit option). **Do not present 0 as a measured thickness.** Enabling thickness fitting is a config choice ([§8](#8-open-questions-for-the-domain-expert)). |
| Phase shift (GridCTFPhase) | XML | per-tilt · curve | ⛔ | ❌ | All-zero (`do_phase=False`). Only meaningful with phase-plate fitting. |
| **rlnCtfMaxResolution / FigureOfMerit / AccumMotion*** | per-tilt star | per-tilt · scalar | ⛔ | ❌ | Placeholders passed through untouched (`1e-6` / `None`). TS-CTF doesn't touch motion; real motion is job002's. |

---

### 3.4 Reconstruct + downstream (job005–008)

Downstream of the 3 core preprocessing jobs. The Warp placeholder trap **does not** apply here — PyTOM
picking scores are real. Enumerated for completeness; secondary priority for the journey view.

| Metric | Source | Scope · Units | Real? | Meaning / tooltip |
|---|---|---|:--:|---|
| **Reconstruction voxel size** | job005 star (`TiltSeriesPixelSize 1.55 × Binning 4`) / MRC `_6.20Apx` | per-tomo · Å/px | ✅ | **6.20 Å/px** — every downstream coordinate anchors to this. ⚠️ `rlnTomoTiltSeriesPixelSize` is the *frame* apix (1.55); it's **reused with a different meaning (6.2) in candidates/particles stars** — a real footgun (pixel arithmetic is our #1 bad-pick source). Flag mismatches. |
| Tomogram dims / binning | job005 star #9-11/#16 | per-tomo · px | ✅ | Unbinned 4096×4096×2048 → recon 1024×1024×512 (size / binning 4). |
| Half-maps (even/odd) | job005 star #14/#15 | per-tomo · path | ✅ | Present → enables cryoCARE denoise + FSC. Presence = a readout. |
| **rlnTomoHand** | job005 star #7 | per-tomo · ±1 | ✅ | −1 in the star, **but the TM driver forces +1** (`template_match_pytom.py:154-158`, TEMP-DEBUG) — the stored value is *not* what picking used. |
| **`job.json defocus_handedness`** | job006 `tmResults/<TS>_job.json` | per-run · int | ✅ | =0 (PyTOM default, unset). A **distinct** knob from `rlnTomoHand` — the defocus-gradient handedness PyTOM actually applied. Arguably the most load-bearing handedness record for the 412 issue. |
| **rlnLCCmax** ⭐ | job007 candidates.star #7 | per-particle · LCC | ✅ | Template-match score per pick (the ranking metric). Here max 0.184 / mean 0.086 / min 0.064 — tight & low = mostly noise (consistent with 412 over-picking). → score histogram + cutoff line. |
| rlnCutOff | job007 candidates.star #8 | per-tomo · LCC | ✅ | Extraction threshold = `mean + k·σ` of the background. 0.0636. → vertical line on the score histogram. |
| Background σ (rlnSearchStd / job_stats.std) | job006/07 | per-tomo · LCC | ✅ | Noise σ of the score map (drives the cutoff). 0.0098. |
| Search space / n_rotations | job006 job.json | per-tomo · count | ✅ | 25.7e9 correlations; **48 orientations** (coarse → quantized pick angles). |
| Candidate count / tomo | job007 | per-tomo · count | ✅ | 120 picks (capped at `max_num_particles`). → picks-per-tomogram bar. |
| Pick coords / orientations | job007 candidates.star #1-6 | per-particle · Å / ° | ✅ | Centered-Å + Warp-voxel coords; ZYZ Euler (heavily quantized here). |
| Pick nn distance / z-depth % | job007 picks.json | per-particle · px / % | ✅/❓ | Nearest-neighbor spacing (double-pick detector) + depth through the tomogram (surface-clustered picks = fiducials/artifacts). |
| `dose_accumulation` / `tilt_angles` | job006 job.json | per-tilt · e⁻·Å² / ° | ✅ | Per-tilt cumulative dose (0 → ~187) + tilt angles driving PyTOM's dose/CTF model. |
| Template/mask geometry | job006 job.json | per-run · px / Å | ✅ | Template box 128³, mask ellipsoid 550³ (spherical), lowpass 45 Å, particle Ø 575 Å — the matching kernel. |
| Subtomo count (extracted vs candidates) | job008 vs job007 | per-TS · count | ✅ | 119 / 120 (one out-of-bounds). The gap is a QC readout. |
| **rlnTomoVisibleFrames** | job008 particles.star #14 | per-particle · bit-mask | ✅ | Which tilts contribute to each particle's 2-D stack (len = 39). Many 0s = fewer views → weaker SNR (e.g. particle 3 = 11 contributing tilts / 28 dropped). → visible-frame-count histogram. |
| Subtomo box / crop / bin · 2D-stack flags | job008 particles.star | per-TS · vox / bool | ✅ | Box 786 → crop 448 @ 1.55 Å (694 Å); CTF-premultiplied 2-D stacks (RELION 4.1+). Sanity + format-contract checks. |
| ~~Spectral whitening filter~~ | — | — | ⛔ | **Removed** — `whiten_spectrum=false`, the `.npy` is never written (declared in job.json but absent on disk). |

---

### 3.5 Tilt Filter + Denoise

**Tilt Filter** — the gallery already overlays four things on each thumbnail card
(`ui/tilt_filter_panel.py`): tilt angle (real), defocus `rlnDefocusU/1e4` (real), **a `0.0px` motion
chip that is meaningless** (reads the `1e-6` placeholder), and a DL probability that **never renders**
(it's 1.0 until the DL model runs). This is exactly the "minor data already there" the request mentions
— and the clearest quick win.

| Metric | Source | Scope · Units | Real? | Shown? | Meaning / tooltip |
|---|---|---|:--:|:--:|---|
| **cryoBoostDlLabel (good/bad)** | **`project_params.json` `state.tilt_filter_labels`** (NOT a star column) | per-tilt · categorical | ✅ | ✅ | Keep/discard decision per tilt (manual click or DL). `write_tilt_series` explicitly drops it from the star (`tilt_series_service.py:218-220`); the df column is rebuilt in-memory at load. *"Keep (good) / discard (bad) this tilt."* |
| **cryoBoostDlProbability** | in-memory only (transient) | per-tilt · 0–1 | 🟡 | ◐ | DL confidence; **never persisted to any star or json** — 1.0 until the DL model runs. Shown as `p0.NN` only when < 1.0 (so: never, in manual runs). |
| rlnDefocusU | labeled star #10 | per-tilt · µm | ✅ | ✅ | Real per-tilt defocus; card shows `5.6µ`. XML frameseries `Defocus` corroborates the **U/V midpoint**, not U directly. |
| **CTFResolutionEstimate (XML)** ⭐ | `job002/warp_frameseries/*.xml` | per-tilt · Å | ✅ | ❌ | The real per-tilt CTF resolution (6.9 Å) — **the single highest-value overlay to add** to the previews. The panel reads only the star, so it never sees this. |
| **MeanFrameMovement (XML)** ⭐ | `job002/warp_frameseries/*.xml` | per-tilt · ❓Å | ✅ | ❌ | The real per-tilt motion (1.23) that should replace the dead `0.0px` chip. |
| rlnAccumMotionTotal (card "motion") | labeled star #15 | per-tilt · — | ⛔ | ⚠️shown-wrong | `1e-6` placeholder → every card shows a meaningless `0.0px`. **Replace with XML `MeanFrameMovement`.** |
| rlnCtfMaxResolution / FigureOfMerit | labeled star #18/#20 | per-tilt · — | ⛔ | ❌ | Placeholders (`1e-6` / `None`). Real value = XML `CTFResolutionEstimate`. |
| rlnMicrographPreExposure · shifts · YTilt #22 | labeled star | per-tilt | ✅ | ❌ | Real, unused: dose, per-tilt alignment shift (X,Y), applied tilt angle. Available in the merged df — free overlays. |
| Label summary (Total/Good/Bad/Removed%) | `get_label_summary()` | per-TS · counts | ✅ | ✅ | The headline roll-up; `>20%` removed flagged red. |
| Tilt thumbnail PNG | `tilt_filter_thumbnails/*.png` | per-tilt · image | ✅ | ✅ | The thing the human inspects. Tiny file size at extreme tilt (~5 KB vs ~130 KB) is itself a crude quality signal. |
| DL FilterStatistics roll-up | `statistics_calculator.py` | per-TS · mixed | ✅ | ❌ | mean/std/min/max prob, bad-fraction, mean-angle good-vs-bad, **out-of-distribution flag** — computed only when DL runs, **printed to stdout, never shown**. |

**Denoise** — essentially **no persisted quality metric exists.** `denoise_train` logs input-MRC
Range/Mean/Std + NaN/flat guards and the cryoCARE **train/val loss curve** (10 epochs) to stdout only;
`denoise_predict` emits per-tomogram ok/fail status + a tile grid — **no SNR/PSNR anywhere.** Neither
ground-truth project has a denoise run, so all of this is from source, not observed. If we want a
denoise quality readout we have to add one (capture the Keras loss history).

---

## 4. The placeholder trap

WarpTools' RELION-star export writes fixed sentinels into columns whose real values it keeps only in
its own XML. **These are the columns to stop reading and the sources to read instead:**

| Star column (⛔ do not plot) | Sentinel | Real value ✅ | Real source |
|---|---|---|---|
| `rlnCtfMaxResolution` | `1e-6` | `CTFResolutionEstimate` | frameseries XML `Movie@` (per-tilt) / tiltseries XML `TiltSeries@` (per-TS) |
| `rlnAccumMotionTotal/Early/Late` | `1e-6` | `MeanFrameMovement` (+ `GridMovementX/Y` track) | frameseries XML |
| `rlnCtfFigureOfMerit` | `None` | *(none exported)* — proxy via `CTFResolutionEstimate` or `TiltPS1D`↔`SimulatedScale` overlap | frameseries / tiltseries XML |
| `|DefocusU − DefocusV|` | `0` (U≡V) | `rlnCtfAstigmatism` | per-tilt star #12 |

The fix is one small XML reader keyed on the movie basename (`Path(rlnMicrographMovieName).stem` — the
same `cryoBoostKey` the tilt-filter panel already joins on). Whether that ingest happens at the
**adapter** layer (write real values into the registry / a sidecar) or at **render** time (read XML on
demand) is an implementation choice — see the build-order note below.

---

## 5. CryoSPARC visualization mapping

CryoSPARC's preprocessing UI is a few reusable diagnostic plots. Transposition rule: **a per-micrograph
value → for us a curve vs tilt within one TS *and* a per-TS aggregate across the dataset.**

| CryoSPARC view | What it shows | Our feed | Transposes? |
|---|---|---|:--:|
| **Manually Curate Exposures** ⭐ | Scatter with **dropdown X/Y axes** over per-exposure stats + **low/high threshold sliders** + split accepted/rejected histograms + linked table. The declutter/cull UI. | Per-TS aggregates: median `CTFResolutionEstimate`, mean `DefocusU`, `CtfAstigmatism`, mean `MeanFrameMovement`, `AverageIntensity` (ice proxy), tilt count, index. | **Yes** — best of all; one point = one tilt-series. Build axis-agnostic. |
| **CTF fit — 1-D PS overlay** ⭐ | Black measured PS1D, red fitted CTF, cyan cross-correlation, green line at fit resolution; DF1/DF2/ANGAST/FIT text. | `PS1D` + `SimulatedBackground/Scale` (→ curves); `DefocusU/V`→DF1/DF2; astig→ANGAST; `CTFResolutionEstimate`→FIT. | **Yes** — default to 0° tilt + a **tilt scrubber** across the stack. |
| **Motion trajectory** | X-shift vs Y-shift path, color-graded by frame/dose. | `GridMovementX/Y` (path) + `MeanFrameMovement` (magnitude); companion `motion vs tilt` curve. | **Partial** — one path per tilt; show current tilt + a per-TS "motion vs tilt" summary. |
| **Relative ice thickness** | Background ratio in the 0.265 Å⁻¹ band; a sortable table column. | `AverageIntensity` / masked-fraction (proxy); or compute the band ratio from our `PS1D`. | **Partial** — fold in as a curation **axis**, not a standalone plot (raw thickness rises as 1/cos(tilt)). |
| **Defocus-vs-tilt + handedness** ⭐ | Defocus vs tilt (smooth through-focus curve; slope sign = handedness) + dose-weight curve + ±1 handedness flag. | `DefocusU/V` vs refined tilt; `dose_accumulation`; `ts_defocus_hand` / `AreAnglesInverted`. | **Yes — tomo-only win.** No SPA analog; directly targets the 412 handedness failure mode. |
| **Tilt-series alignment residual / shift trajectory** ⭐ | Residual-vs-tilt bar (spikes = bad tilts) + refined shift path colored by tilt order. | `.aln` shift magnitude (we have this); true residual only if patch tracking is enabled. | **Yes — tomo-only win**, but "residual" here = shift magnitude until patch tracking is on. |

**Priority to build first (from the CryoSPARC pass):** ① the exposure-curation scatter (the declutter
pattern — general, axis-agnostic), ② the PS1D CTF-fit overlay (highest trust-building diagnostic),
③ defocus-vs-tilt + alignment shift (tomo-native wins), ④ the motion trajectory.

---

## 6. Gap analysis & recommended build order

**Currently in the journey (real & working):** per-tilt defocus U/V, astigmatism, refined shift
magnitude, refined tilt/angle deltas; FS-motion / align / CTF stat strips.

**Currently in the journey but BROKEN (read placeholders → render empty):** CTF-max-resolution panel,
accumulated-motion panel, CTF-figure-of-merit panel. **Decluttering starts here:** either wire these to
their real XML sources or remove them.

**High-value, real, on disk, surfaced nowhere:** `CTFResolutionEstimate`, `MeanFrameMovement` +
`GridMovement` track, `PS1D` fit overlay, per-tilt `AreTomo shift` magnitude, tomostar
`AverageIntensity`, per-TS `GridCTF` defocus curve, global defocus-handedness decision, `UseTilt`
inclusion, `rlnTomoVisibleFrames`, PyTOM `rlnLCCmax` score distributions.

**Suggested order** (each self-contained; ships value on its own):

1. **Fix the three dead panels** — add the small frameseries-XML reader and repoint CTF-res / motion to
   `CTFResolutionEstimate` / `MeanFrameMovement`. Immediate declutter: blank panels become real curves.
2. **Tilt-filter preview overlay** — badge each thumbnail with real CTF-res + motion (the "minor data"
   ask); replace the meaningless `0.0px` chip. Small, high-visibility.
3. **Defocus-vs-tilt + shift-magnitude** panel (tomo-native, all-real, no new plumbing).
4. **PS1D CTF-fit overlay** with a tilt scrubber (needs the XML curve parser).
5. **Exposure-curation scatter** across tilt-series (the declutter/cull centerpiece; needs multi-TS
   aggregates — validate on `pos9_10` first).

---

## 7. Tooltip text bank

Ready-to-paste one-liners (extends the existing `_HINT_*` constants in `tomo_dashboard_dialog.py`):

- **Defocus (U/V)** — "Fitted defocus for this tilt (Å); larger = further from focus. WarpTools ties U=V; astigmatism is reported separately."
- **Astigmatism** — "Difference between the two CTF defocus axes (Å); near-zero or huge values on weak tilts are unreliable fits."
- **Astig angle** — "Orientation of the astigmatism major axis (°); only meaningful when the magnitude is non-trivial."
- **CTF fit resolution** — "Resolution the CTF (Thon rings) was reliably fit to for this tilt (Å); lower is better. Degrades toward high tilt."
- **Beam-induced motion** — "Average beam-induced motion during this tilt's exposure; higher = more drift/charging. Rises with tilt and dose."
- **Motion track** — "The drift path within a single tilt's exposure (X vs Y over time)."
- **Accumulated dose** — "Cumulative electron dose received before this tilt (e⁻/Å²); drives dose weighting. Dose-symmetric order ≠ tilt order."
- **Refined shift** — "Per-tilt translation applied to register this tilt to the tilt-axis frame (Å); spikes flag hard-to-align / bad tilts."
- **Refined tilt / offset** — "Tilt angle after AreTomo's fitted stage-tilt offset; the offset is a per-TS pretilt / zero-point correction."
- **Tilt-axis angle** — "In-plane tilt-axis azimuth (°); defines the direction the defocus ramps across the image."
- **Average intensity** — "Mean image intensity for this tilt; anomalously low = a dark or obstructed tilt."
- **UseTilt** — "Whether this tilt is kept for reconstruction; excluded tilts are dropped downstream."
- **Defocus handedness** — "Defocus/tilt geometry handedness (−1 flip / +1 no-flip), decided globally across all tilt-series. Wrong handedness = bad picks on chiral templates."
- **CTF fit resolution (TS)** — "Best resolution the CTF was fit to for the whole tilt-series (Å); a top-line data-quality ranking number."
- **Template-match score (LCCmax)** — "Cross-correlation of each pick against the template; higher = stronger match. The cutoff line is the extraction threshold."
- **Visible frames** — "How many tilt images contribute to this particle's stack; fewer = weaker signal."
- **Voxel size** — "Physical size of one reconstructed voxel (Å/px) = frame pixel size × binning; every coordinate is anchored to this."

---

## 8. Open questions for the domain expert

These are the `❓` items — real fields whose meaning/units couldn't be settled from the files alone.
Each is a decision only you can make; flag them in the UI rather than guessing a value:

1. **`MeanFrameMovement` / `GridMovement` units — pixels or Ångström?** The XML doesn't state units;
   Warp convention is Å. At 1.55 Å/px this is a ~1.55× difference on a motion-QC axis. **Confirm before
   these drive a threshold.**
2. **`SimulatedBackground` is all-zero** in this run — pre-subtracted, not modeled, or a bug? Affects
   whether we can draw the CTF-fit background curve.
3. **`FOVFraction` meaning** — usable field-of-view fraction? It's non-monotonic (min near 0° tilt),
   which is counter-intuitive if it's about high-tilt frame overrun.
4. **`AxisOffsetX/Y` semantics/units** — large and erratic per tilt; unclear if it's a sample-position
   offset relative to the tilt axis.
5. **Per-tilt vs per-TS astigmatism** — Warp fit astig once per TS here (constant across tilts). Do you
   want per-tilt astig fitting enabled? It changes whether an astig-vs-tilt plot is meaningful.
6. **Ice/sample thickness fitting** — the `<CTF> Thickness` field exists but is 0 (not requested). Worth
   enabling in `ts_ctf` so we get a real thickness readout (and the CryoSPARC ice-thickness axis)?
7. **Alignment residual** — global AreTomo emits none. Enable patch tracking (`patch_x/patch_y > 0`) to
   get a true per-tilt alignment error, or is refined-shift-magnitude enough?
8. **Handedness overrides** — `rlnTomoHand` is force-set to +1 in the TM driver (TEMP-DEBUG) while
   `job.json defocus_handedness` stays 0. Both should be reconciled and surfaced given the 412 history.

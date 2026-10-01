# Roadmap 22 — Blank tilts leak into reconstructions (the "aliasing" artifact)

**Status:** investigated 2026-09-29, fix **not implemented**; the maintainer is confirming stages 0–1 on a separate branch. Release item: "aliasing artifacts, e.g.
`agg_SK_20251017_412` Pos 34 Beam 5 — the Munich cluster, same distribution and SIFs, gets none."

## 1. Finding — it is not aliasing

The artifact is a set of **straight back-projection rays**: bright lines crossing the whole volume at the
angles of five tilts where the beam was blocked. In XY slices each ray's cross-section appears as an
X-elongated "bullseye" with Gibbs rings, which reads as aliasing to the eye. There is no real-space binning
anywhere in the chain (Warp and AreTomo both Fourier-crop), and pixel sizes agree everywhere (1.55 Å raw,
6.2 Å tomogram).

**The chain of causes, Position_34_5:**

1. **Five blank exposures.** mdoc `MinMaxMean` means at +56.89 / 59.89 / 62.89 / 65.89 / 68.89° are
   0.42 / 0.01 / 0.01 / 0.01 / 0.01 counts, against ~600–820 for every other tilt
   (`mdoc/agg_SK_20251017_412_Position_34_5.mdoc`). Tomostar `_wrpAverageIntensity` = 0.000 for all five.
   They are the only zero-intensity tilts among the project's 20 series.
2. **They hold only hot pixels.** No gain or defect correction ran (`gain_path`/`defects_path` null,
   `External/job002/task_19.out`); the 62.9° average is 85 % exact zeros plus fixed 2×2 hot pixels (e.g.
   detector (2275, 776) reads 10–14 in every blank tilt, ~1.4 in normal ones). Per-tilt normalisation
   turns those into the brightest features of the tilt, and back-projection turns each into a ray.
3. **Whether they survive import is a coin flip.** WarpTools `ts_import` walks outward from 0° and
   truncates everything past the first tilt failing
   `AverageIntensity >= min_intensity · cos(angle) · MaxAverage · 0.999` (`ImportTiltseries.cs:335-350`).
   crboost passes `--min_intensity 0.0` (`services/jobs/ts_import.py:57`), so for a blank tilt the test is
   `mean >= 0` — decided by float16 rounding noise. This run kept all five. A second local run on the same
   raw data, same SIF, identical parameters, different GPU node (`post_handedness_fix`) truncated after
   56.89° and kept one: 2 bullseyes instead of 10+. **Another site's hardware can just as easily keep
   none** — the most likely reason Munich sees a clean tomogram.
4. **The tilt filter flagged all five, and the verdict never reached Warp.** `TiltFilter/` marks them bad
   (plus −42.1°, −45.1°), but the May code never applied the cut: the staged reconstruct tomostar has 39
   tilts vs 32 in `tilt_series_filtered/…34_5.star`, every `UseTilt` True. Today's code trims at alignment
   input (`drivers/ts_alignment.py:188-194`), but only if a verdict was committed before alignment starts —
   there is no barrier (roadmap 06).
5. **AreTomo then diverges on this series only:** `-nan` tilt-offset search (31/31 offsets), final offset 0
   at CC 0, 130–444 px shifts on three blank tilts. 34_5 is the only tomogram whose lamella is not levelled.

**Blast radius:** tilts with intensity < 0.05 also exist in agg_20251113_412 (8), agg_20260122_412 (9),
agg_20260204_412 (7) and agg_20260311_412_Grid3 (18). Wherever one survives import, the same rays appear.
Merely *dim* tilts (70–95 counts) reconstruct cleanly (agg_20260122_412 Position_3) — the artifact needs
≈ 0 counts.

**Side finding (out of scope, recorded):** the 19 other series got AreTomo tilt offsets of −5 to −15° at
CC ≤ 0.013, several pinned at the −15° search edge, and Warp applies them (34_3: −45.11 → −50.11). Weak
fits steering every lamella's levelling deserve their own look.

## 2. Before → After

**Before:** whether a beam-blocked exposure enters the reconstruction depends on the sign of float16 noise
and on whether someone committed the tilt filter before alignment started. Nothing says it happened.

**After:** a blank exposure is excluded deterministically, before alignment, from physics the mdoc already
records; the exclusion is visible per tilt with its reason; the user can overrule it; and tilts Warp drops
at import are counted in the open instead of silently skipped.

## 3. Design

### 3.1 Blank-exposure check at registry ingest
- Per tilt series, compare each tilt's mdoc mean counts with the series median. Below
  `BLANK_EXPOSURE_FRACTION = 0.01` → `is_filtered_out=True`, `filter_reason="blank exposure: <mean> vs
  series median <median> counts"`. The constant is policy, named and justified where it lives: blank tilts
  sit at ~1e-5 of the median, the dimmest real tilts seen at ~0.1, so 1 % separates them by 10× on one side
  and 1000× on the other. No dataset numbers.
- No `MinMaxMean` in the mdoc → fall back to Warp's `_wrpAverageIntensity`; neither → the series shows
  "blank check unavailable" (surfaced, never assumed clean).
- The tilt-filter gallery shows these pre-labelled bad with the reason; a manual "good" wins. The
  tilt-filter commit preserves a blank verdict unless the user flipped it.
- Alignment's tomostar trim already drops every `is_filtered_out` frame, so the check works even in
  pipelines without a tilt-filter job.

### 3.2 Warp's import drops become visible
`ts_import`'s walk also truncates *good* tilts beyond a blank one (a grid bar mid-series takes the rest of
the branch with it), and it cannot be disabled. The adapters currently skip un-matched rows silently. Record
per series "Warp dropped N tilts at import (±x°…)" in the registry and show it in the Journey and the
tsImport tab.

### 3.3 Gain and defects (contributory — confirm first)
No project under `/groups/klumpe/crboost_data` uses a gain reference or defect map. The rays originate in
uncorrected hot pixels. Find out whether these EER sessions came with a gain file; if so, the project-level
gain reference already exists as a setup field. Not a fix on its own — blank tilts must go regardless.

### 3.4 AreTomo divergence flagged
A `-nan` or CC-0 tilt-offset result is written to the registry as an alignment warning on that series
instead of passing through to Warp as offset 0.

## 4. Stages

### Stage 0 — Close the Munich question (optional; no compute)
- Ask the reviewer for Munich's `Position_34_5.tomostar` and the reconstruct XML: tilt count,
  `_wrpAverageIntensity`, `UseTilt`; the "Tilt filter applied from registry" line in their alignment
  `run.out`; `GainPath`/`CorrectGain` in their `warp_frameseries.settings`; md5 of the 34_5 mdoc.
- *Success:* the site difference is explained (expected: their import or their filter dropped the blank tilts).

### Stage 1 — Confirm the mechanism (one ~4 min GPU job, user-run)
- Re-run `ts_reconstruct` for 34_5 from a staged tomostar without the five blank rows.
- *Success:* the rays and bullseyes are gone (0 pixels beyond 8σ in the central slice, as in the other series).

### Stage 2 — §3.1 blank check + §3.4 AreTomo flag
- *Success:* re-running the 412 chain excludes the five tilts before alignment with the reason visible in
  the gallery; AreTomo's offset search is finite; 34_5's lamella is levelled; no rays.

### Stage 3 — §3.2 import-drop visibility
- *Success:* `post_handedness_fix` Position_34_5 shows "Warp dropped 4 tilts at import (59.9°…68.9°)".

### Stage 4 — Sweep existing data (optional)
- A one-off script lists reconstructed tomograms whose tilt series contain a blank tilt that survived
  import, so affected users know which tomograms to re-run.

## 5. Runtime checklist
1. Stage 1 recon viewed next to the original (XZ at y=559: the 33° ray must be gone).
2. After stage 2, the tilt-filter gallery for 34_5 shows five tilts pre-labelled "blank exposure" with counts.
3. Flip one back to good, commit → that tilt is used again (manual wins).

**Modern-Python weave-in:** the check as a pure function over a tilt series' `(angle, mean)` pairs
returning per-tilt verdicts — trivially testable against the 34_5 numbers above.

# Roadmap 19 — Per-tilt CTF-fit outlier check: fail loudly when Warp's defocus fit diverges

**Status:** scoped 2026-09-09, **not implemented**. Trigger: `docs/known_bugs.md` #1 — the copia protocol
ran with a frame-series fit band that reached into noise (`c_range_min_max 30:6` on 2 e/Å² tilts), Warp's
2D fit diverged on 17–22 of 28 tilts, nothing in the pipeline noticed, and the first symptom was Refine3D
stalling at 15 Å three jobs and two days later. Full investigation: `docs/reports/copia/copia-ctf-fit-investigation.md`.

**Core design decision (user, 2026-09-09):** the check carries **no dataset numbers**. No fit band, no
defocus range, no dose or pixel-size assumption is hard-coded. Everything the check needs about the data it
reads from the same Warp XML it is checking; the only constants are physics (one Thon ring) and policy
(when to stop the job), named and justified where they live.

## Before → After

**Before:** `fsMotionAndCtf` and `tsCtf` accept whatever `fs_ctf` / `ts_ctf` return. `ts_import` copies the
frame-series defoci into the tilt-series XML and `ts_ctf` only refines them locally, so a diverged
frame-series fit propagates untouched into RELION's CTF-premultiplied 2D stacks at extraction. The
`ts_defocus_hand --check` verdict is computed on the same garbage and reported as a number. Warp's own
placeholders (`_rlnCtfMaxResolution` 1e-6, `_rlnCtfFigureOfMerit` None) mean RELION sees nothing wrong
either (`reference_warp_relion_star_placeholders`).

**After:** after ingest, each CTF job re-estimates every tilt's defocus independently of Warp's 2D fit
from Warp's own stored 1D spectrum, stores both numbers and a verdict per tilt in the registry, logs the
flagged tilts with the job's own band in the message, fails the job when the series is mostly wrong, and
marks the hand verdict unreliable when the fits it was computed from are. The Journey and the CTF job tabs
show the flag per tilt. The same code, replayed over three projects already on disk, is the regression
test for future releases.

---

## 1. Inventory — what already exists (do not rebuild)

| Piece | State |
|---|---|
| `services/tilt_series/warp_curves.py` | parses `PS1D` (frame series) and `TiltPS1D ID=z` (tilt series) into `CtfFit1D`: frequency axis in cycles/px, background-subtracted power, Warp's defocus, Cs, kV, amplitude contrast, phase shift, pixel size, **`fit_range_nyq` (Warp's RangeMin/RangeMax)**, `ctf_resolution_a`. Memoised by mtime. |
| `services/tilt_series/adapters/ts_ctf.py` | registry ingest for tsCtf; already parses `PlaneNormal` and per-tilt defocus |
| frame-series ingest adapter + registry 1.4 per-tilt QC (`project_tilt_metrics_wave`) | per-tilt `ctf_resolution`, motion, thumbnails reach the Journey hovers and the tilt-metrics panel |
| `drivers/fs_motion_and_ctf.py`, `drivers/ts_ctf.py` | `ArrayDriver` subclasses; supervisor `tally_acceptable` runs after all tasks, the natural home for a per-series verdict |
| the awk scan in `docs/reports/copia/copia-ctf-fit-investigation.md` | the prototype of the 1D estimate: ±20-sample running-mean subtraction, correlation with CTF²(Δf), argmax over Δf; recovered 5.05–5.53 µm on all 28 copia tilts in every band while the 2D fit was off |
| `docs/known_bugs.md` #1 | symptoms, where to look, what the code should do |

## 2. The check

Two layers, cheapest first. Both run per tilt series, on the XML the job just wrote, inside the
supervisor after registry ingest.

### 2a. Series consistency (no spectral fit)

Defoci across a tilt series follow the microscope's nominal trend within a fraction of a micron. Compute
the median absolute deviation of Warp's per-tilt defoci from a robust trend (the median; a linear fit
against the mdoc's nominal defocus when the registry has it). A series whose fits scatter by microns is
inconsistent whatever the dose, band or pixel size. This layer alone flags copia (17 tilts at 1.3–4.2 µm
around 6 at 5.3), but when most tilts are wrong it cannot say which, so it never names tilts — it only
escalates to 2b and, on its own, warns.

### 2b. Independent 1D defocus estimate

For every tilt (or movie), from `CtfFit1D`:

1. Flatten the stored 1D power with a running-mean subtraction (window ~20 samples of 256).
2. Correlate the residual with CTF²(Δf) computed from the XML's Cs, kV, amplitude contrast and pixel size.
3. **Scan range** = the job's own defocus window, `OptionsCTF` `DefocusMin`/`DefocusMax` from the same XML
   (extend `CtfFit1D` by these two fields). Step: one hundredth of the window.
4. **Band**: lower edge = Warp's `RangeMin`. Upper edge is not a number: run the scan for a ladder of
   high-resolution cutoffs starting at Warp's `RangeMax` and coarsening (each step halves the number of
   Nyquist fractions), and take the estimate from the finest cutoff at which the argmax is stable across
   two neighbouring rungs. Stability across cutoffs is the criterion. A fit whose estimate never
   stabilises is **unverifiable** (no usable rings), which is a distinct verdict from **diverged**.
5. Record per tilt: `defocus_1d_um`, `defocus_1d_cc`, `defocus_1d_band_a` (the cutoff used), and the
   verdict `ok | diverged | unverifiable`.

**Diverged** when |Warp − 1D| exceeds `CTF_DIVERGENCE_UM` and the 1D correlation exceeds
`CTF_SCAN_MIN_CC`. Both are constants with their reasoning beside them:

- `CTF_DIVERGENCE_UM = 0.5` — about one Thon-ring period at the resolutions these fits work at
  (ΔΔf per ring = 2/(λk²): 1.0 µm at 10 Å, 0.35 µm at 6 Å, 300 kV); a disagreement of one ring is a
  different minimum, not noise.
- `CTF_SCAN_MIN_CC = 0.3` — below this the 1D scan has nothing to say and the tilt is unverifiable.

Nothing else is tunable and nothing is a job parameter. If a dataset needs different numbers, that is a
finding to write down here, not a knob.

## 3. Policy — what the job does with the verdict

Per the never-fail-silently rule and without inventing decisions for the user:

| Verdict per series | Action |
|---|---|
| no tilt diverged | nothing; the fields are still stored |
| some tilts diverged, below `CTF_FAIL_FRACTION` | job log WARNING naming the tilts with both defoci and the 1D band; registry flag; series continues |
| diverged tilts ≥ `CTF_FAIL_FRACTION` (= 0.25) of the series | **job fails** with the same list and a one-line hint quoting the job's own band: "fit band `<c_range_min_max as used>` reaches into noise on these tilts; a coarser high-resolution limit fixed the same failure on EMPIAR-12580 (known_bugs #1)". Continuing costs days; the fix is one parameter. |
| unverifiable tilts | registry flag only, counted in the log; they are neither excluded nor trusted |

`CTF_FAIL_FRACTION` is policy: below a quarter the downstream can survive with the other tilts (the
per-TS mute in `project_ts_exclude_from_processing` is the user's tool to drop them); above it the
series is not usable and the user must refit.

The check never excludes tilts and never rewrites defoci. Auto-excluding would be a silent default;
rewriting Warp's fit with our 1D number would put an unvalidated estimator into the data path.

### `tsCtf` specifics

- `ts_ctf` inherits the frame-series defoci and refines locally, so the check runs again on its XML; a
  series that failed in `fsMotionAndCtf` never reaches `tsCtf`.
- `ts_defocus_hand --check` reads the frame-series 2×2 gradients. When the series has any diverged tilt,
  the driver still runs the check but stores the verdict as `unreliable` with the reason, and the tsCtf
  tab shows it that way instead of a correlation number. The copia values (−0.035 / +0.159 on garbage,
  −0.994 on good fits) are the reference for why.

## 4. Surfacing

- Registry per-tilt fields as in 2b; the ingest adapters write them.
- Journey per-tilt hover and the tilt-metrics panel: a red marker on diverged tilts with the tooltip
  "Warp 3.6 µm · 1D scan 5.3 µm (cc 0.71, band to 12 Å)"; a grey marker on unverifiable ones.
- `fsMotionAndCtf` and `tsCtf` job tabs: one status line, "3 of 28 tilts diverged" / "series failed:
  17 of 28 diverged", linking to the job log.
- tsCtf tab: the hand verdict shows `unreliable (fits diverged)` when applicable.

Subtle and only where a real gap exists; nothing is shown on a clean series.

## 5. Regression test — the release check

Three projects on disk, each with the XMLs the check reads, run in seconds with no GPU:

| project | expected |
|---|---|
| `/groups/klumpe/crboost_data/copia-empiar12580-20260906-1319` (band 30:6) | job002 and job005: 17 of 28 diverged; series fails |
| `/groups/klumpe/crboost_data/copia-empiar12580-20260908-1447` (band 30:10) | 0 diverged in both jobs; hand verdict −0.994 kept |
| `/groups/klumpe/user/<pi>/Processing/Copia/try2` (1.55 Å/px, Warp dev31) | the 7 last-acquired tilts of Position_11_2 diverged (1.1–2.2 µm against ~4.8); below the fail fraction, warning only |

A script under `tools/` (or a `python -m services.tilt_series.ctf_check <project>` entry point) prints
the per-tilt table and the verdict for a project; the three expectations above are the assertions. This
is the test the copia protocol run exists for (`feedback_protocols_scope_no_harness`: a regular project,
run the regular way, checked afterwards).

## 6. Stages

- **S1 — `services/tilt_series/ctf_check.py`**: `scan_defocus_1d(fit: CtfFit1D) -> Scan1D`, the cutoff
  ladder, `check_series(fits) -> SeriesVerdict`; extend `CtfFit1D` with the defocus window. Pure, no I/O
  beyond `warp_curves`. Replay script over the three projects; expectations in section 5 met.
- **S2 — registry fields + adapters**: per-tilt `defocus_1d_um`, `defocus_1d_cc`, `defocus_1d_band_a`,
  `ctf_verdict`; per-series `ctf_hand_verdict_reliable`. Schema bump.
- **S3 — driver policy**: `tally_acceptable` hooks in `drivers/fs_motion_and_ctf.py` and `drivers/ts_ctf.py`
  per section 3; the hand-check gating in tsCtf.
- **S4 — UI markers** per section 4.
- **S5 — `docs/known_bugs.md` #1** flipped to FIXED with the commit; this roadmap's status updated.

S1 is the whole value; S2–S4 are wiring. Build S1–S5 back to back and verify once at the end on the three
projects (`feedback_dont_gate_stages_on_runtime`).

## 7. Out of scope

- Changing Warp's fit (band presets per dose, `--auto_hand`): protocol authors own the band; the check
  tells them when it is wrong.
- Replacing Warp's defocus with the 1D estimate in any output.
- Auto-excluding tilts (see section 3).
- A per-tilt outlier check for other Warp quantities (motion, astigmatism); same mechanism, later.

# Recipe: Diagnosing TM underpicking

## Root cause analysis

The GT (run12) used old CryoBoost + pytom 0.10 with the **text-file interface**
(`--defocus`, `--tilt-angles`, `--dose-accumulation`). This interface does NOT
read `rlnTomoHand` from STAR files, so pytom defaults to `defocus_handedness=0`.

Your current pipeline uses pytom 0.12 with `--relion5-tomograms-star`, which
reads `rlnTomoHand=-1` → `defocus_handedness=-1`. This is the suspected cause
of the score compression (std ratio 0.58 vs GT).

### What pytom actually does with defocus_handedness (from source)

In `tmjob.py:911`: when `defocus_handedness != 0` AND `per_tilt_weighting` is on,
pytom computes **per-patch defocus offsets** — it adjusts the CTF defocus value
for each sub-volume based on its position in the tomogram relative to the tilt axis.

In `tmjob.py:150` (`get_defocus_offsets()`):
```python
if ts_metadata.defocus_handedness < 0:
    ta_array *= -1  # flip tilt angle signs
z_offsets = z_centers * cos(ta_array) + x_centers * sin(ta_array)
```

Then in the matching loop:
```python
for ctf, defocus_shift in zip(self.ts_metadata.ctf_data, defocus_offsets):
    ctf.defocus = ctf.defocus + defocus_shift * 1e-10
```

So: **`defocus_handedness=0` skips this entire block** — uniform defocus per tilt,
no position-dependent CTF correction. This is what the GT used, and it works fine
for a spherical/ellipsoidal template at 11.8 A/px because the within-tilt defocus
gradient barely matters at this resolution.

With `defocus_handedness=-1`, pytom shifts the defocus per-patch with flipped tilt
angles. Whether the flip direction is actually correct for your data is the question —
if it's wrong, the per-patch CTF correction actively degrades the scores.

## Container

```
/groups/klumpe/software/PyTom_tm/PyTom_tm.sif          # pytom 0.10 (GT used this)
/groups/klumpe/software/Setup/cryoboost_containers/pytom_match_pick.sif      # pytom 0.12 (current)
/groups/klumpe/software/Setup/cryoboost_containers/pytom_match_pick_0.13.0.sif  # pytom 0.13
```

## How the old CryoBoost produced defocus/tilt/dose text files

Source: `/groups/klumpe/software/Setup/CryoBoost/src/templateMatching/pytomTm.py`

It reads the per-tilt STAR data and writes one value per line:

| File | STAR column | Divisor | Extension |
|------|-------------|---------|-----------|
| `defocusFiles/{tomoName}.txt` | `rlnDefocusU` | 10000 (Å → µm) | `.txt` |
| `tiltAngleFiles/{tomoName}.tlt` | `rlnTomoNominalStageTiltAngle` | 1 | `.tlt` |
| `doseFiles/{tomoName}.txt` | `rlnMicrographPreExposure` | 1 | `.txt` |

## Step 1: Generate the text files from your STAR data

Run this from your project root (adjust paths as needed):

```python
#!/usr/bin/env python3
"""Generate pytom 0.10 text-file inputs from per-tilt STAR data."""

import starfile
import os
from pathlib import Path

# --- CONFIGURE THESE ---
PROJECT = Path("projects/auto_flip_cusesum")
CTF_STAR_DIR = PROJECT / "External/job004/tilt_series"  # per-tilt STARs from CTF step
OUTPUT_DIR = PROJECT / "External/job006"                # TM job dir
# -----------------------

for subdir in ["defocusFiles", "tiltAngleFiles", "doseFiles"]:
    (OUTPUT_DIR / subdir).mkdir(parents=True, exist_ok=True)

for star_file in sorted(CTF_STAR_DIR.glob("*.star")):
    data = starfile.read(star_file, always_dict=True)
    df = list(data.values())[0]
    tomo_name = star_file.stem  # e.g. "auto_flip_cusesum_Position_1"

    # Defocus: rlnDefocusU in Angstroms, pytom 0.10 expects micrometers
    defocus_um = df["rlnDefocusU"] / 10000.0
    defocus_um.to_csv(OUTPUT_DIR / f"defocusFiles/{tomo_name}.txt", index=False, header=False)

    # Tilt angles: degrees, no conversion
    df["rlnTomoNominalStageTiltAngle"].to_csv(
        OUTPUT_DIR / f"tiltAngleFiles/{tomo_name}.tlt", index=False, header=False
    )

    # Dose accumulation: e/A^2, no conversion
    df["rlnMicrographPreExposure"].to_csv(
        OUTPUT_DIR / f"doseFiles/{tomo_name}.txt", index=False, header=False
    )

    print(f"Wrote {tomo_name}: {len(df)} tilts")
```

## Step 2: Run pytom 0.10

```bash
TOMO_NAME="auto_flip_cusesum_Position_1"
JOB_DIR="External/job006"
TOMO_VOL="${JOB_DIR}/tmResults/${TOMO_NAME}.mrc"  # symlink to reconstruction

apptainer run --nv /groups/klumpe/software/PyTom_tm/PyTom_tm.sif \
  "pytom_match_template.py \
    -v ${TOMO_VOL} \
    --tilt-angles ${JOB_DIR}/tiltAngleFiles/${TOMO_NAME}.tlt \
    --defocus ${JOB_DIR}/defocusFiles/${TOMO_NAME}.txt \
    --dose-accumulation ${JOB_DIR}/doseFiles/${TOMO_NAME}.txt \
    -t templates/ellipsoid_550_550_550_apix11.80_box96_lp45_black.mrc \
    -d ${JOB_DIR}/tmResults \
    -m templates/ellipsoid_550_550_550_apix11.80_box96_lp45_mask.mrc \
    --angular-search 90 \
    --voltage 300.0 \
    --spherical-aberration 2.7 \
    --amplitude-contrast 0.1 \
    --per-tilt-weighting \
    --log debug \
    -g 0 \
    -s 4 4 2 \
    --non-spherical-mask"
```

Note: the GT wrapped the entire pytom command as a single string argument to
`apptainer run` (see run12/External/job006/run.out). Adjust the quoting for
your submission system.

## What to compare

After running, compare the resulting `_job.json`:
- `defocus_handedness` should be `0` (pytom 0.10 default)
- `job_stats.std` should be closer to GT's `0.01733` (vs current `0.01002`)

Then run extraction and compare pick counts against GT's 1154.

## Temporary code change already applied

In `drivers/template_match_pytom.py`, `make_pytom_tomograms_star()` now
temporarily overrides `rlnTomoHand` to `1` before writing the patched STAR.
This tests the same hypothesis using pytom 0.12 + RELION5 STAR interface.
Look for `[TEMP-DEBUG]` in the TM run.out to confirm it fired.

---

## Adjusting extraction threshold (pytom 0.12 or 0.13)

### Why the threshold matters

The auto cutoff uses the Rickgauer et al. (2017) FDR formula:
```
cutoff = erfcinv(2 * n_false_positives / search_space) * sqrt(2) * sigma
```

Where `sigma = job_stats["std"]` from the score volume. When the score std is
compressed (0.010 vs GT's 0.017), the cutoff scales down proportionally, BUT the
peaks are also compressed. The net effect depends on whether peaks compress more
or less than the noise — in your case, the elevated mean (5.3x GT) suggests the
noise model assumptions (zero-mean Gaussian) are violated, making the FDR cutoff
too aggressive.

Current config (`project_params.json`):
```json
"cutoff_method": "NumberOfFalsePositives",
"cutoff_value": 1.0
```
This gives cutoff=0.065 and 733 picks. GT got 1154 picks.

### Option A: Increase number of false positives (recommended first test)

The `--number-of-false-positives` parameter (default 1.0) directly controls how
permissive the FDR threshold is. Increasing it lowers the cutoff and admits more
particles (at the cost of more false positives).

In `project_params.json`, under the `candidateextraction` job:
```json
"cutoff_method": "NumberOfFalsePositives",
"cutoff_value": 10.0
```

Or via CLI (standalone test without rerunning the pipeline):
```bash
JOB_DIR="projects/auto_flip_cusesum/External/job006"

# Using pytom 0.13 container:
apptainer run --nv /groups/klumpe/software/Setup/cryoboost_containers/pytom_match_pick_0.13.0.sif \
  "pytom_extract_candidates.py \
    -j ${JOB_DIR}/tmResults/auto_flip_cusesum_Position_1_job.json \
    -n 1500 \
    --number-of-false-positives 10 \
    --particle-diameter 550 \
    --relion5-compat \
    --log debug"

# Or using pytom 0.12:
apptainer run --nv /groups/klumpe/software/Setup/cryoboost_containers/pytom_match_pick.sif \
  "pytom_extract_candidates.py \
    -j ${JOB_DIR}/tmResults/auto_flip_cusesum_Position_1_job.json \
    -n 1500 \
    --number-of-false-positives 10 \
    --particle-diameter 550 \
    --relion5-compat \
    --log debug"
```

Values to try: 5, 10, 50, 100. The relationship is logarithmic so you need
large jumps to see meaningful threshold changes.

### Option B: Manual cutoff

Bypass the FDR model entirely and set a hard score threshold:

```json
"cutoff_method": "ManualCutOff",
"cutoff_value": 0.035
```

Or via CLI: `--cut-off 0.035`

To pick a value: your p99.5 is 0.034, so a manual cutoff around 0.03-0.035
should recover most particles that are distinguishable from background. Compare
against GT's p99.5 of 0.059 — the ratio is ~0.58 (matching the std compression),
so scaling GT's implicit cutoff by 0.58 is a reasonable starting point.

### Option C: Tophat filter (pytom's alternative to FDR)

The tophat filter uses morphological filtering + its own Gaussian fit to find
peaks, independent of the raw score distribution. This can be more robust when
the score distribution is non-Gaussian (e.g., elevated mean):

```json
"score_filter_method": "tophat",
"score_filter_value": "1:50"
```

CLI: `--tophat-filter --tophat-connectivity 1 --tophat-bins 50`

This can be combined with `--number-of-false-positives`.

### Important: extraction does NOT require re-running TM

The extraction step reads from `*_job.json` + `*_scores.mrc` + `*_angles.mrc`
in `tmResults/`. You can re-run extraction with different thresholds as many
times as you want without re-running the (expensive) template matching step.

### What to expect

If the rlnTomoHand override fixes the score compression, extraction with default
`n_false_positives=1` should already recover ~1100+ picks. If scores are still
compressed (pytom version difference), you'll need to manually compensate with
a higher `n_false_positives` or a manual cutoff.




Done. The recipe (PYTOM_010_RECIPE.md) now has three sections:

  1. Root cause analysis — what pytom actually does with defocus_handedness (from the source: defocus_handedness=0 skips per-patch CTF correction entirely, -1 flips tilt angles for the gradient calculation)
  2. Pytom 0.10 text-file recipe — how to generate defocus/tilt/dose files and run manually
  3. Threshold adjustment for 0.12/0.13 — three options:
    - Option A: Bump cutoff_value (n_false_positives) from 1.0 to 10/50/100 in project_params.json
    - Option B: Switch to ManualCutOff with a value around 0.035 (scaled from GT)
    - Option C: Tophat filter as an FDR alternative

  Key point: extraction is cheap — you can re-run it against the existing score maps without re-running TM.

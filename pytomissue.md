We observed a ~2x compression in score map statistics when upgrading from pytom-match-pick 0.10.0 to 0.12.1 (or 0.13.0). This occurs on the same reconstructed tomogram with the same parameters, leading to significantly different extraction behavior between versions.

## Setup

* Tomogram: Single reconstruction, 28 tilts, Warp 2.0 (dev36) at 11.8 A/px.
* Dimensions: 512 x 1024 x 1024 voxels.
* Template: 96^3 ellipsoid (550 A diameter, normalized, black contrast) -- copia virus particle.
* Weighting: Per-tilt weighting enabled.

## Comparison

Both runs used identical:

* Reconstructed tomogram (bitwise same .mrc).
* Template and mask.
* Angular search (90 deg, 48 rotations).
* No spectral whitening or random phase correction.
* No bandpass filter.

### v0.10.0 (Text file input)

```bash
pytom_match_template.py \
    -v tomo.mrc -t template.mrc -m mask.mrc \
        --tilt-angles angles.tlt --defocus defocus.txt --dose-accumulation dose.txt \
            --tomogram-ctf-model phase-flip \
                --angular-search 90 --per-tilt-weighting \
                    --voltage 300 --spherical-aberration 2.7 --amplitude-contrast 0.07 \
                        -g 0 -s 2 2 1

```

### v0.13.0 (RELION5 STAR input)

```bash
pytom_match_template.py \
    -v tomo.mrc -t template.mrc -m mask.mrc \
        --relion5-tomograms-star tomograms.star \
            --tomogram-ctf-model phase-flip \
                --angular-search 90 --per-tilt-weighting \
                    --voltage 300 --spherical-aberration 2.7 --amplitude-contrast 0.07 \
                        -g 0 -s 2 2 1

```

## Score Map Statistics

| Metric | v0.10.0 | v0.13.0 | v0.13.0 (hand=1) |
| --- | --- | --- | --- |
| std | 0.01579 | 0.00884 | 0.00886 |
| max | 0.293 | 0.188 | 0.186 |
| min | -0.153 | -0.063 | -0.064 |
| mean | 0.00961 | 0.00961 | 0.00962 |
| defocus_handedness | 0 | -1 | 1 |

The mean remains stable, but the dynamic range (std, min, max) is compressed by roughly a factor of 2. Overriding rlnTomoHand to 1 or toggling the CTF model did not change the distribution.

## Impact

The compression changes where extraction thresholds land, affecting pick counts substantially:

* v0.10.0: 1085 picks.
* v0.13.0: 737 picks.

## Questions

1. Is this an expected change in score normalization between versions? If so what can this be related to?
2. Could this be related to how 0.13 reads metadata from RELION5 STAR files vs. the old text interface? The distributions are identical regardless of handedness, suggesting something different, but i looked through the diffs of matching.py and other files between 0.10.0 and 0.12.1 and didn't really catch any significant change in the logic...

## Environment

* Software: pytom-match-pick 0.10.0 and 0.13.0.
* Stack: CUDA 12.1, Python 3.11, CuPy, PyTorch.
* Tomograms: Reconstructed with WarpTools 2.0 (dev36).

I also read through PR #334 but I don't think this patch is the reason. If this doesn't seem obvious and someone is willing to give me a hand/deeper look into this -- what kind of files/diagnostics would you wanna see?

<details>
<summary>This particular project looks like this... (v0.13.0)</summary>

```text
CBE-login [projects/pytom_phaseflip_on] tree -L 6 --charset=ascii  -I 'projects|venv|*__pycache__*|Class3D|*.eer|*.log|*_motion.json|*.mrcs|2025080*|run_it*|node_modules|GEVEv2*'
.
|-- default_pipeline.star
|-- External
|   |-- job002
|   |   |-- default_pipeline.star
|   |   |-- fs_motion_and_ctf.star
|   |   |-- job_pipeline.star
|   |   |-- job.star
|   |   |-- note.txt
|   |   |-- RELION_JOB_EXIT_SUCCESS
|   |   |-- run.err
|   |   |-- run.out
|   |   |-- run_submit.script
|   |   |-- tilt_series
|   |   |   `-- pytom_phaseflip_on_Position_1.star
|   |   |-- warp_frameseries
|   |   |   |-- align_and_ctf_frameseries.settings
|   |   |   |-- average
|   |   |   |   |-- even
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_001[10.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_002[13.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_003[7.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_004[4.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_005[16.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_006[19.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_007[1.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_008[-2.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_009[22.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_010[25.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_011[-5.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_012[-8.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_013[28.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_014[31.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_015[-11.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_016[-14.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_017[34.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_018[37.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_019[-17.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_020[-20.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_021[40.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_022[43.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_023[-23.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_024[-26.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_025[46.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_026[49.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_027[-29.00]_EER.mrc
|   |   |   |   |   `-- pytom_phaseflip_on_Position_1_028[-32.00]_EER.mrc
|   |   |   |   |-- odd
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_001[10.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_002[13.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_003[7.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_004[4.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_005[16.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_006[19.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_007[1.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_008[-2.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_009[22.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_010[25.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_011[-5.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_012[-8.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_013[28.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_014[31.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_015[-11.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_016[-14.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_017[34.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_018[37.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_019[-17.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_020[-20.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_021[40.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_022[43.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_023[-23.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_024[-26.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_025[46.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_026[49.00]_EER.mrc
|   |   |   |   |   |-- pytom_phaseflip_on_Position_1_027[-29.00]_EER.mrc
|   |   |   |   |   `-- pytom_phaseflip_on_Position_1_028[-32.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_001[10.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_002[13.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_003[7.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_004[4.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_005[16.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_006[19.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_007[1.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_008[-2.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_009[22.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_010[25.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_011[-5.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_012[-8.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_013[28.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_014[31.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_015[-11.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_016[-14.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_017[34.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_018[37.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_019[-17.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_020[-20.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_021[40.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_022[43.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_023[-23.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_024[-26.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_025[46.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_026[49.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_027[-29.00]_EER.mrc
|   |   |   |   `-- pytom_phaseflip_on_Position_1_028[-32.00]_EER.mrc
|   |   |   |-- logs
|   |   |   |-- powerspectrum
|   |   |   |   |-- pytom_phaseflip_on_Position_1_001[10.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_002[13.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_003[7.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_004[4.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_005[16.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_006[19.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_007[1.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_008[-2.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_009[22.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_010[25.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_011[-5.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_012[-8.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_013[28.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_014[31.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_015[-11.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_016[-14.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_017[34.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_018[37.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_019[-17.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_020[-20.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_021[40.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_022[43.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_023[-23.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_024[-26.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_025[46.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_026[49.00]_EER.mrc
|   |   |   |   |-- pytom_phaseflip_on_Position_1_027[-29.00]_EER.mrc
|   |   |   |   `-- pytom_phaseflip_on_Position_1_028[-32.00]_EER.mrc
|   |   |   |-- processed_items.json
|   |   |   |-- pytom_phaseflip_on_Position_1_001[10.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_002[13.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_003[7.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_004[4.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_005[16.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_006[19.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_007[1.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_008[-2.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_009[22.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_010[25.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_011[-5.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_012[-8.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_013[28.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_014[31.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_015[-11.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_016[-14.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_017[34.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_018[37.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_019[-17.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_020[-20.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_021[40.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_022[43.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_023[-23.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_024[-26.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_025[46.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_026[49.00]_EER.xml
|   |   |   |-- pytom_phaseflip_on_Position_1_027[-29.00]_EER.xml
|   |   |   `-- pytom_phaseflip_on_Position_1_028[-32.00]_EER.xml
|   |   `-- warp_frameseries.settings
|   |-- job003
|   |   |-- aligned_tilt_series.star
|   |   |-- all_tilts.star
|   |   |-- default_pipeline.star
|   |   |-- job_pipeline.star
|   |   |-- job.star
|   |   |-- note.txt
|   |   |-- RELION_JOB_EXIT_SUCCESS
|   |   |-- run.err
|   |   |-- run.out
|   |   |-- run_submit.script
|   |   |-- tilt_series
|   |   |   `-- pytom_phaseflip_on_Position_1.star
|   |   |-- tomostar
|   |   |   |-- processed_items.json
|   |   |   `-- pytom_phaseflip_on_Position_1.tomostar
|   |   |-- warp_tiltseries
|   |   |   |-- logs
|   |   |   |-- processed_items.json
|   |   |   |-- pytom_phaseflip_on_Position_1.xml
|   |   |   `-- tiltstack
|   |   |       `-- pytom_phaseflip_on_Position_1
|   |   |           |-- pytom_phaseflip_on_Position_1_aligned.mrc
|   |   |           |-- pytom_phaseflip_on_Position_1_Imod
|   |   |           |-- pytom_phaseflip_on_Position_1.rawtlt
|   |   |           |-- pytom_phaseflip_on_Position_1.st
|   |   |           |-- pytom_phaseflip_on_Position_1.st.aln
|   |   |           `-- thumbnails
|   |   `-- warp_tiltseries.settings
|   |-- job004
|   |   |-- default_pipeline.star
|   |   |-- job_pipeline.star
|   |   |-- job.star
|   |   |-- note.txt
|   |   |-- RELION_JOB_EXIT_SUCCESS
|   |   |-- run.err
|   |   |-- run.out
|   |   |-- run_submit.script
|   |   |-- tilt_series
|   |   |   `-- pytom_phaseflip_on_Position_1.star
|   |   |-- ts_ctf_tilt_series.star
|   |   `-- warp_tiltseries
|   |       |-- ctf_tiltseries.settings
|   |       |-- logs
|   |       |-- powerspectrum
|   |       |   `-- pytom_phaseflip_on_Position_1.mrc
|   |       |-- processed_items.json
|   |       `-- pytom_phaseflip_on_Position_1.xml
|   |-- job005
|   |   |-- default_pipeline.star
|   |   |-- job_pipeline.star
|   |   |-- job.star
|   |   |-- note.txt
|   |   |-- RELION_JOB_EXIT_SUCCESS
|   |   |-- run.err
|   |   |-- run.out
|   |   |-- run_submit.script
|   |   |-- tomograms.star
|   |   `-- warp_tiltseries
|   |       |-- logs
|   |       |-- processed_items.json
|   |       |-- pytom_phaseflip_on_Position_1.xml
|   |       `-- reconstruction
|   |           |-- ctf
|   |           |   `-- pytom_phaseflip_on_Position_1_11.80Apx.mrc
|   |           |-- deconv
|   |           |   `-- pytom_phaseflip_on_Position_1_11.80Apx.mrc
|   |           |-- even
|   |           |   `-- pytom_phaseflip_on_Position_1_11.80Apx.mrc
|   |           |-- odd
|   |           |   `-- pytom_phaseflip_on_Position_1_11.80Apx.mrc
|   |           |-- pytom_phaseflip_on_Position_1_11.80Apx_f32.mrc
|   |           |-- pytom_phaseflip_on_Position_1_11.80Apx.mrc
|   |           `-- pytom_phaseflip_on_Position_1_11.80Apx.png
|   |-- job006
|   |   |-- default_pipeline.star
|   |   |-- job_pipeline.star
|   |   |-- job.star
|   |   |-- note.txt
|   |   |-- RELION_JOB_EXIT_SUCCESS
|   |   |-- run.err
|   |   |-- run.out
|   |   |-- run_submit.script
|   |   |-- tilt_series
|   |   |   `-- pytom_phaseflip_on_Position_1.star -> /users/artem.kushner/dev/crboost_server/projects/pytom_phaseflip_on/External/job004/tilt_series/pytom_phaseflip_on_Position_1.star
|   |   |-- tmResults
|   |   |   |-- pytom_phaseflip_on_Position_1_angles.mrc
|   |   |   |-- pytom_phaseflip_on_Position_1_job.json
|   |   |   |-- pytom_phaseflip_on_Position_1.mrc -> /users/artem.kushner/dev/crboost_server/projects/pytom_phaseflip_on/External/job005/warp_tiltseries/reconstruction/pytom_phaseflip_on_Position_1_11.80Apx.mrc
|   |   |   |-- pytom_phaseflip_on_Position_1_scores.mrc
|   |   |   |-- template_convolved.mrc
|   |   |   `-- template_psf.mrc
|   |   |-- tomograms_for_pytom.star
|   |   `-- tomograms.star
|   `-- job007
|       |-- candidates.star
|       |-- candidatesWarp
|       |   `-- pytom_phaseflip_on_Position_1.star
|       |-- default_pipeline.star
|       |-- job_pipeline.star
|       |-- job.star
|       |-- note.txt
|       |-- optimisation_set.star
|       |-- RELION_JOB_EXIT_SUCCESS
|       |-- run.err
|       |-- run.out
|       |-- run_submit.script
|       |-- tmResults
|       |   |-- pytom_phaseflip_on_Position_1_angles.mrc -> /users/artem.kushner/dev/crboost_server/projects/pytom_phaseflip_on/External/job006/tmResults/pytom_phaseflip_on_Position_1_angles.mrc
|       |   |-- pytom_phaseflip_on_Position_1_job.json
|       |   |-- pytom_phaseflip_on_Position_1.mrc -> /users/artem.kushner/dev/crboost_server/projects/pytom_phaseflip_on/External/job005/warp_tiltseries/reconstruction/pytom_phaseflip_on_Position_1_11.80Apx.mrc
|       |   |-- pytom_phaseflip_on_Position_1_particles.star
|       |   |-- pytom_phaseflip_on_Position_1_scores.mrc -> /users/artem.kushner/dev/crboost_server/projects/pytom_phaseflip_on/External/job006/tmResults/pytom_phaseflip_on_Position_1_scores.mrc
|       |   |-- template_convolved.mrc -> /users/artem.kushner/dev/crboost_server/projects/pytom_phaseflip_on/External/job006/tmResults/template_convolved.mrc
|       |   `-- template_psf.mrc -> /users/artem.kushner/dev/crboost_server/projects/pytom_phaseflip_on/External/job006/tmResults/template_psf.mrc
|       |-- tomograms.star
|       `-- vis
|           |-- imodCenter
|           |   `-- coords_pytom_phaseflip_on_Position_1.txt
|           `-- imodPartRad
|               `-- coords_pytom_phaseflip_on_Position_1.txt
|-- frames
|-- Import
|   `-- job001
|       |-- default_pipeline.star
|       |-- job_pipeline.star
|       |-- job.star
|       |-- log.html
|       |-- log.txt
|       |-- note.txt
|       |-- RELION_JOB_EXIT_SUCCESS
|       |-- run.err
|       |-- run.out
|       |-- tilt_series
|       |   `-- pytom_phaseflip_on_Position_1.star
|       `-- tilt_series.star
|-- Logs
|-- mdoc
|   `-- pytom_phaseflip_on_Position_1.mdoc
|-- project_params.json
|-- qsub.sh
|-- Schemes
|   `-- run_20260303_135908
|       |-- aligntiltsWarp
|       |   `-- job.star
|       |-- fsMotionAndCtf
|       |   `-- job.star
|       |-- importmovies
|       |   `-- job.star
|       |-- RELION_JOB_EXIT_SUCCESS
|       |-- resolution_report.txt
|       |-- schemer.err
|       |-- schemer.out
|       |-- scheme.star
|       |-- templatematching
|       |   `-- job.star
|       |-- tmextractcand
|       |   `-- job.star
|       |-- tsCtf
|       |   `-- job.star
|       `-- tsReconstruct
|           `-- job.star
`-- templates
    |-- ellipsoid_550_550_550_apix11.80_box96_lp45_black.mrc
    |-- ellipsoid_550_550_550_apix11.80_box96_lp45_mask.mrc
    |-- ellipsoid_550_550_550_apix11.80_box96_lp45_white.mrc
    `-- ellipsoid_550_550_550_apix11.80_seed.mrc

56 directories, 274 files
```

</details>

<details>
<summary>And the EXACT same data in a "ground truth" project (with version 0.10.0) yields 1150 picks</summary>

```text
ᢹ CBE-login [run12/External] tree -L 6 --charset=ascii  -I 'projects|venv|*__pycache__*|Class3D|*.eer|*.log|*_motion.json|*.mrcs|2025080*|run_it*|node_modules|GEVEv2*'
.
|-- job002
|   |-- default_pipeline.star
|   |-- fs_motion_and_ctf.star
|   |-- job_pipeline.star
|   |-- job.star
|   |-- note.txt
|   |-- RELION_JOB_EXIT_SUCCESS
|   |-- run.err
|   |-- run.out
|   |-- run_submit.script
|   |-- tilt_series
|   |   `-- Position_1.star
|   |-- warp_frameseries
|   |   |-- align_and_ctf_frameseries.settings
|   |   |-- average
|   |   |   |-- even
|   |   |   |   |-- Position_1_001[10.00]_EER.mrc
|   |   |   |   |-- Position_1_002[13.00]_EER.mrc
|   |   |   |   |-- Position_1_003[7.00]_EER.mrc
|   |   |   |   |-- Position_1_004[4.00]_EER.mrc
|   |   |   |   |-- Position_1_005[16.00]_EER.mrc
|   |   |   |   |-- Position_1_006[19.00]_EER.mrc
|   |   |   |   |-- Position_1_007[1.00]_EER.mrc
|   |   |   |   |-- Position_1_008[-2.00]_EER.mrc
|   |   |   |   |-- Position_1_009[22.00]_EER.mrc
|   |   |   |   |-- Position_1_010[25.00]_EER.mrc
|   |   |   |   |-- Position_1_011[-5.00]_EER.mrc
|   |   |   |   |-- Position_1_012[-8.00]_EER.mrc
|   |   |   |   |-- Position_1_013[28.00]_EER.mrc
|   |   |   |   |-- Position_1_014[31.00]_EER.mrc
|   |   |   |   |-- Position_1_015[-11.00]_EER.mrc
|   |   |   |   |-- Position_1_016[-14.00]_EER.mrc
|   |   |   |   |-- Position_1_017[34.00]_EER.mrc
|   |   |   |   |-- Position_1_018[37.00]_EER.mrc
|   |   |   |   |-- Position_1_019[-17.00]_EER.mrc
|   |   |   |   |-- Position_1_020[-20.00]_EER.mrc
|   |   |   |   |-- Position_1_021[40.00]_EER.mrc
|   |   |   |   |-- Position_1_022[43.00]_EER.mrc
|   |   |   |   |-- Position_1_023[-23.00]_EER.mrc
|   |   |   |   |-- Position_1_024[-26.00]_EER.mrc
|   |   |   |   |-- Position_1_025[46.00]_EER.mrc
|   |   |   |   |-- Position_1_026[49.00]_EER.mrc
|   |   |   |   |-- Position_1_027[-29.00]_EER.mrc
|   |   |   |   `-- Position_1_028[-32.00]_EER.mrc
|   |   |   |-- odd
|   |   |   |   |-- Position_1_001[10.00]_EER.mrc
|   |   |   |   |-- Position_1_002[13.00]_EER.mrc
|   |   |   |   |-- Position_1_003[7.00]_EER.mrc
|   |   |   |   |-- Position_1_004[4.00]_EER.mrc
|   |   |   |   |-- Position_1_005[16.00]_EER.mrc
|   |   |   |   |-- Position_1_006[19.00]_EER.mrc
|   |   |   |   |-- Position_1_007[1.00]_EER.mrc
|   |   |   |   |-- Position_1_008[-2.00]_EER.mrc
|   |   |   |   |-- Position_1_009[22.00]_EER.mrc
|   |   |   |   |-- Position_1_010[25.00]_EER.mrc
|   |   |   |   |-- Position_1_011[-5.00]_EER.mrc
|   |   |   |   |-- Position_1_012[-8.00]_EER.mrc
|   |   |   |   |-- Position_1_013[28.00]_EER.mrc
|   |   |   |   |-- Position_1_014[31.00]_EER.mrc
|   |   |   |   |-- Position_1_015[-11.00]_EER.mrc
|   |   |   |   |-- Position_1_016[-14.00]_EER.mrc
|   |   |   |   |-- Position_1_017[34.00]_EER.mrc
|   |   |   |   |-- Position_1_018[37.00]_EER.mrc
|   |   |   |   |-- Position_1_019[-17.00]_EER.mrc
|   |   |   |   |-- Position_1_020[-20.00]_EER.mrc
|   |   |   |   |-- Position_1_021[40.00]_EER.mrc
|   |   |   |   |-- Position_1_022[43.00]_EER.mrc
|   |   |   |   |-- Position_1_023[-23.00]_EER.mrc
|   |   |   |   |-- Position_1_024[-26.00]_EER.mrc
|   |   |   |   |-- Position_1_025[46.00]_EER.mrc
|   |   |   |   |-- Position_1_026[49.00]_EER.mrc
|   |   |   |   |-- Position_1_027[-29.00]_EER.mrc
|   |   |   |   `-- Position_1_028[-32.00]_EER.mrc
|   |   |   |-- Position_1_001[10.00]_EER.mrc
|   |   |   |-- Position_1_002[13.00]_EER.mrc
|   |   |   |-- Position_1_003[7.00]_EER.mrc
|   |   |   |-- Position_1_004[4.00]_EER.mrc
|   |   |   |-- Position_1_005[16.00]_EER.mrc
|   |   |   |-- Position_1_006[19.00]_EER.mrc
|   |   |   |-- Position_1_007[1.00]_EER.mrc
|   |   |   |-- Position_1_008[-2.00]_EER.mrc
|   |   |   |-- Position_1_009[22.00]_EER.mrc
|   |   |   |-- Position_1_010[25.00]_EER.mrc
|   |   |   |-- Position_1_011[-5.00]_EER.mrc
|   |   |   |-- Position_1_012[-8.00]_EER.mrc
|   |   |   |-- Position_1_013[28.00]_EER.mrc
|   |   |   |-- Position_1_014[31.00]_EER.mrc
|   |   |   |-- Position_1_015[-11.00]_EER.mrc
|   |   |   |-- Position_1_016[-14.00]_EER.mrc
|   |   |   |-- Position_1_017[34.00]_EER.mrc
|   |   |   |-- Position_1_018[37.00]_EER.mrc
|   |   |   |-- Position_1_019[-17.00]_EER.mrc
|   |   |   |-- Position_1_020[-20.00]_EER.mrc
|   |   |   |-- Position_1_021[40.00]_EER.mrc
|   |   |   |-- Position_1_022[43.00]_EER.mrc
|   |   |   |-- Position_1_023[-23.00]_EER.mrc
|   |   |   |-- Position_1_024[-26.00]_EER.mrc
|   |   |   |-- Position_1_025[46.00]_EER.mrc
|   |   |   |-- Position_1_026[49.00]_EER.mrc
|   |   |   |-- Position_1_027[-29.00]_EER.mrc
|   |   |   `-- Position_1_028[-32.00]_EER.mrc
|   |   |-- logs
|   |   |-- Position_1_001[10.00]_EER.xml
|   |   |-- Position_1_002[13.00]_EER.xml
|   |   |-- Position_1_003[7.00]_EER.xml
|   |   |-- Position_1_004[4.00]_EER.xml
|   |   |-- Position_1_005[16.00]_EER.xml
|   |   |-- Position_1_006[19.00]_EER.xml
|   |   |-- Position_1_007[1.00]_EER.xml
|   |   |-- Position_1_008[-2.00]_EER.xml
|   |   |-- Position_1_009[22.00]_EER.xml
|   |   |-- Position_1_010[25.00]_EER.xml
|   |   |-- Position_1_011[-5.00]_EER.xml
|   |   |-- Position_1_012[-8.00]_EER.xml
|   |   |-- Position_1_013[28.00]_EER.xml
|   |   |-- Position_1_014[31.00]_EER.xml
|   |   |-- Position_1_015[-11.00]_EER.xml
|   |   |-- Position_1_016[-14.00]_EER.xml
|   |   |-- Position_1_017[34.00]_EER.xml
|   |   |-- Position_1_018[37.00]_EER.xml
|   |   |-- Position_1_019[-17.00]_EER.xml
|   |   |-- Position_1_020[-20.00]_EER.xml
|   |   |-- Position_1_021[40.00]_EER.xml
|   |   |-- Position_1_022[43.00]_EER.xml
|   |   |-- Position_1_023[-23.00]_EER.xml
|   |   |-- Position_1_024[-26.00]_EER.xml
|   |   |-- Position_1_025[46.00]_EER.xml
|   |   |-- Position_1_026[49.00]_EER.xml
|   |   |-- Position_1_027[-29.00]_EER.xml
|   |   |-- Position_1_028[-32.00]_EER.xml
|   |   |-- powerspectrum
|   |   |   |-- Position_1_001[10.00]_EER.mrc
|   |   |   |-- Position_1_002[13.00]_EER.mrc
|   |   |   |-- Position_1_003[7.00]_EER.mrc
|   |   |   |-- Position_1_004[4.00]_EER.mrc
|   |   |   |-- Position_1_005[16.00]_EER.mrc
|   |   |   |-- Position_1_006[19.00]_EER.mrc
|   |   |   |-- Position_1_007[1.00]_EER.mrc
|   |   |   |-- Position_1_008[-2.00]_EER.mrc
|   |   |   |-- Position_1_009[22.00]_EER.mrc
|   |   |   |-- Position_1_010[25.00]_EER.mrc
|   |   |   |-- Position_1_011[-5.00]_EER.mrc
|   |   |   |-- Position_1_012[-8.00]_EER.mrc
|   |   |   |-- Position_1_013[28.00]_EER.mrc
|   |   |   |-- Position_1_014[31.00]_EER.mrc
|   |   |   |-- Position_1_015[-11.00]_EER.mrc
|   |   |   |-- Position_1_016[-14.00]_EER.mrc
|   |   |   |-- Position_1_017[34.00]_EER.mrc
|   |   |   |-- Position_1_018[37.00]_EER.mrc
|   |   |   |-- Position_1_019[-17.00]_EER.mrc
|   |   |   |-- Position_1_020[-20.00]_EER.mrc
|   |   |   |-- Position_1_021[40.00]_EER.mrc
|   |   |   |-- Position_1_022[43.00]_EER.mrc
|   |   |   |-- Position_1_023[-23.00]_EER.mrc
|   |   |   |-- Position_1_024[-26.00]_EER.mrc
|   |   |   |-- Position_1_025[46.00]_EER.mrc
|   |   |   |-- Position_1_026[49.00]_EER.mrc
|   |   |   |-- Position_1_027[-29.00]_EER.mrc
|   |   |   `-- Position_1_028[-32.00]_EER.mrc
|   |   `-- processed_items.json
|   `-- warp_frameseries.settings
|-- job003
|   |-- aligned_tilt_series.star
|   |-- default_pipeline.star
|   |-- job_pipeline.star
|   |-- job.star
|   |-- note.txt
|   |-- RELION_JOB_EXIT_SUCCESS
|   |-- run.err
|   |-- run.out
|   |-- run_submit.script
|   |-- tilt_series
|   |   `-- Position_1.star
|   |-- tomostar
|   |   |-- Position_1.tomostar
|   |   `-- processed_items.json
|   |-- warp_tiltseries
|   |   |-- logs
|   |   |-- Position_1.xml
|   |   |-- processed_items.json
|   |   `-- tiltstack
|   |       `-- Position_1
|   |           |-- Position_1_aligned.mrc
|   |           |-- Position_1_Imod
|   |           |   |-- newst.com
|   |           |   |-- Position_1_st.tlt
|   |           |   |-- Position_1_st.xf
|   |           |   |-- Position_1_st.xtilt
|   |           |   `-- tilt.com
|   |           |-- Position_1.rawtlt
|   |           |-- Position_1.st
|   |           |-- Position_1.st.aln
|   |           `-- thumbnails
|   |               |-- Position_1_001[10.00]_EER.png
|   |               |-- Position_1_002[13.00]_EER.png
|   |               |-- Position_1_003[7.00]_EER.png
|   |               |-- Position_1_004[4.00]_EER.png
|   |               |-- Position_1_005[16.00]_EER.png
|   |               |-- Position_1_006[19.00]_EER.png
|   |               |-- Position_1_007[1.00]_EER.png
|   |               |-- Position_1_008[-2.00]_EER.png
|   |               |-- Position_1_009[22.00]_EER.png
|   |               |-- Position_1_010[25.00]_EER.png
|   |               |-- Position_1_011[-5.00]_EER.png
|   |               |-- Position_1_012[-8.00]_EER.png
|   |               |-- Position_1_013[28.00]_EER.png
|   |               |-- Position_1_014[31.00]_EER.png
|   |               |-- Position_1_015[-11.00]_EER.png
|   |               |-- Position_1_016[-14.00]_EER.png
|   |               |-- Position_1_017[34.00]_EER.png
|   |               |-- Position_1_018[37.00]_EER.png
|   |               |-- Position_1_019[-17.00]_EER.png
|   |               |-- Position_1_020[-20.00]_EER.png
|   |               |-- Position_1_021[40.00]_EER.png
|   |               |-- Position_1_022[43.00]_EER.png
|   |               |-- Position_1_023[-23.00]_EER.png
|   |               |-- Position_1_024[-26.00]_EER.png
|   |               |-- Position_1_025[46.00]_EER.png
|   |               |-- Position_1_026[49.00]_EER.png
|   |               |-- Position_1_027[-29.00]_EER.png
|   |               `-- Position_1_028[-32.00]_EER.png
|   `-- warp_tiltseries.settings
|-- job004
|   |-- default_pipeline.star
|   |-- job_pipeline.star
|   |-- job.star
|   |-- note.txt
|   |-- RELION_JOB_EXIT_SUCCESS
|   |-- run.err
|   |-- run.out
|   |-- run_submit.script
|   |-- tilt_series
|   |   `-- Position_1.star
|   |-- tomostar
|   |   |-- Position_1.tomostar
|   |   `-- processed_items.json
|   |-- ts_ctf_tilt_series.star
|   |-- warp_tiltseries
|   |   |-- ctf_tiltseries.settings
|   |   |-- logs
|   |   |-- Position_1.xml
|   |   |-- powerspectrum
|   |   |   `-- Position_1.mrc
|   |   `-- processed_items.json
|   `-- warp_tiltseries.settings
|-- job005
|   |-- default_pipeline.star
|   |-- job_pipeline.star
|   |-- job.star
|   |-- note.txt
|   |-- RELION_JOB_EXIT_SUCCESS
|   |-- run.err
|   |-- run.out
|   |-- run_submit.script
|   |-- tilt_series
|   |   `-- Position_1.star
|   |-- tomograms.star
|   |-- tomostar
|   |   |-- Position_1.tomostar
|   |   `-- processed_items.json
|   |-- warp_tiltseries
|   |   |-- logs
|   |   |-- Position_1.xml
|   |   |-- processed_items.json
|   |   `-- reconstruction
|   |       |-- ctf
|   |       |   `-- Position_1_11.80Apx.mrc
|   |       |-- deconv
|   |       |   `-- Position_1_11.80Apx.mrc
|   |       |-- even
|   |       |   `-- Position_1_11.80Apx.mrc
|   |       |-- odd
|   |       |   `-- Position_1_11.80Apx.mrc
|   |       |-- Position_1_11.80Apx_f32.mrc
|   |       |-- Position_1_11.80Apx.mrc
|   |       `-- Position_1_11.80Apx.png
|   `-- warp_tiltseries.settings
|-- job006
|   |-- default_pipeline.star
|   |-- defocusFiles
|   |   `-- Position_1.txt
|   |-- doseFiles
|   |   `-- Position_1.txt
|   |-- job_pipeline.star
|   |-- job.star
|   |-- note.txt
|   |-- RELION_JOB_EXIT_SUCCESS
|   |-- run.err
|   |-- run.out
|   |-- run_submit.script
|   |-- tiltAngleFiles
|   |   `-- Position_1.tlt
|   |-- tilt_series
|   |   `-- Position_1.star
|   |-- tmResults
|   |   |-- Position_1_11.80Apx_angles.mrc
|   |   |-- Position_1_11.80Apx_job.json
|   |   |-- Position_1_11.80Apx_scores.mrc
|   |   |-- template_convolved.mrc
|   |   `-- template_psf.mrc
|   `-- tomograms.star
`-- job007
    |-- candidates.star
    |-- candidatesWarp
    |   `-- Position_1.star
    |-- default_pipeline.star
    |-- job_pipeline.star
    |-- job.star
    |-- note.txt
    |-- optimisation_set.star
    |-- RELION_JOB_EXIT_SUCCESS
    |-- run.err
    |-- run.out
    |-- run_submit.script
    |-- tilt_series
    |   `-- Position_1.star
    |-- tmResults
    |   |-- Position_1_11.80Apx_angles.mrc -> /groups/klumpe/software/Setup/Testing/test1/run12/External/job006/tmResults/Position_1_11.80Apx_angles.mrc
    |   |-- Position_1_11.80Apx_extraction_graph.svg
    |   |-- Position_1_11.80Apx_job.json
    |   |-- Position_1_11.80Apx_particles.star
    |   |-- Position_1_11.80Apx_scores.mrc -> /groups/klumpe/software/Setup/Testing/test1/run12/External/job006/tmResults/Position_1_11.80Apx_scores.mrc
    |   |-- template_convolved.mrc -> /groups/klumpe/software/Setup/Testing/test1/run12/External/job006/tmResults/template_convolved.mrc
    |   `-- template_psf.mrc -> /groups/klumpe/software/Setup/Testing/test1/run12/External/job006/tmResults/template_psf.mrc
    |-- tomograms.star
    `-- vis
        |-- imodCenter
        |   |-- coords_Position_1.mod
        |   `-- coords_Position_1.txt
        `-- imodPartRad
            |-- coords_Position_1.mod
            `-- coords_Position_1.txt

46 directories, 286 files

```
</details>



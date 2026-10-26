# Roadmap 38 — Segmentation quality: segment at the training voxel size, more TTA, Z smoothing

**Status:** scoped and built 2026-10-20 on `segmentations` (§4; ruff-clean, not run). Follows the first run of
roadmap 37's Segment job (AGG412_20260311_MyrGFP_Grid2, job038). Next: the runtime check (§5).

**In one line.** easymode's networks were trained on 10 Å Warp reconstructions. The Segment job now gives them each
tomogram as a 10 Å reconstruction would hold it and maps every result back onto the tomogram's grid. It also
averages 8 test-time augmentations by default, and smooths the features decided plane by plane along Z to the
resolution they have in X/Y.

## 1. What the first run showed

job038: 48 tomograms, 1024 × 1024 × 512 at 6.2 Å (`tsReconstruct__2`); features cytoplasm, nucleus and ribosome;
TTA 2.

- **The display is not at fault.** The fills are drawn where the volumes put them: the overlays line up with the
  WarpTools pictures (correlation 0.98 against our slab in the overlays' orientation, 0.02 flipped).
- **Bands along Z.** cytoplasm and nucleus (2.5D Ais networks at 50 Å) come out in bands. On Position_10,
  cytoplasm covers 11 %, 57 % and 8 % of the plane at z = 192, 208 and 224 (100 Å apart).
- **Blocks within a plane.** The regions are blocks on the network's 50 Å grid. nucleus fills the lower half of a
  vesicle cluster with an edge that follows nothing in the density, and cytoplasm lights a frame along the
  volume's edges. 30 % of the nucleus voxels inside the lamella lie between p 0.1 and 0.9.
- **How the wall turns that into cut-offs.** The fill is p ≥ 0.5 anywhere in the central 10 planes (62 Å), so the
  bands show as hard-edged blocks that stop where the feature goes on.

## 2. What the tools do with the voxel size

From the easymode v1.2.5 and Ais v1.2.38 sources, and the models' metadata in the weights cache:

| | easymode's own networks (`.h5`: ribosome, microtubule, TRiC) | Ais-engine networks (`.scnm`: every other feature) |
|---|---|---|
| Resampling | X, Y and Z to the network's 10 Å (skimage `resize`, order 3, anti-aliased); back with order 1 | X/Y only, to the network's pixel (cytoplasm and nucleus 50 Å; void and mitochondrion 30 Å; membrane 10 Å); Z as it comes. Ais builds its training boxes the same way, so a network learned Z at the training tomograms' 10 Å |
| Z context | 3D tiles at 10 Å | 2.5D: 16 consecutive planes, 160 Å in training, 99 Å at 6.2 Å. 3D (membrane): slabs 16 planes deep, same ratio |
| Normalization | `global_mad`: the mean, and 1.4826 × the MAD, over 32 planes spread through the volume; raw voxels, no low-pass, before any resampling | same |
| TTA | up to 16 | 8 transforms in its lists; 9–16 raise an IndexError, the tomogram keeps its placeholder, and our task fails |

- **Contrast.** On a 6.2 Å tomogram the voxels' MAD is 1.40× that of the same tomogram binned to 12.4 Å. The
  ~50 Å picture a 2.5D network gets therefore arrives at 0.72× the normalized contrast (Position_1 and Position_10
  alike).
- **In short.** The header voxel size is read correctly. What differs from training is the Z context and the
  contrast.

## 3. Decisions

- **Segment at 10 Å** (easymode method, on by default).
  - Each tomogram's spectrum is cropped to 10 Å: what a 10 Å reconstruction holds (band limit 20 Å), with sample 0
    and the box's length kept. easymode segments that.
  - Each volume is mapped back onto the tomogram's grid by linear interpolation, the inverse of that sampling.
    easymode maps its own networks' output back with order 1 too.
  - Every volume keeps the tomogram's shape, so the wall, the Journey, the pick viewer, template matching's search
    region and ChimeraX are unchanged.
  - A tomogram within 5 % of 10 Å, or coarser, is segmented as it is (the tools would not resample it either).
  - Off: the old behaviour, for comparison.
- **The Ais-model method runs on the tomogram as it is.** A user's model was trained on the tomograms they
  annotated, and the job does not know their voxel size.
- **Z smoothing of plane-by-plane networks** (on by default).
  - A Gaussian along Z with FWHM = the network's X/Y pixel (its `.scnm` `apix`), so its Z resolution matches its
    X/Y resolution.
  - A network counts as plane-by-plane unless its `arch` says 3D.
  - Done on the grid it segmented, before mapping back.
- **TTA 8 by default, at most 8** (Ais's limit). The networks take four times as long as at TTA 2. The template's
  per-tomogram time limit goes from 1 h to 2 h.
- **Re-runs.** The processing voxel size and the smoothing are part of a feature's identity. Running a job made
  before this change again therefore recomputes every volume.
- **Each task asks the tools only for the volumes it lacks.** Results now land in the task's staging directory
  first, so the tools no longer see the finished volumes and skip them by themselves.
- **Not here:**
  - a soft (p-weighted) fill on the wall;
  - denoised input as the default (the job accepts denoised input already; easymode's training included its own n2n and iso
    denoisings, and Denoise predict method easymode now produces those flavours);
  - masking the frame along the volume's edges;
  - training Ais networks for samples unlike easymode's training set (roadmap 37 §T.2).

## 4. Build record (2026-10-20, ruff-clean, not run)

- **`services/segmentation/grid.py` (new).**
  - `processing_shape`, `write_resampled` (spectrum crop with `scipy.fft`, threads from the task's CPUs, float32
    MRC with the new voxel size per axis).
  - `finish_volume`: the tool's volume as p, smoothed along Z when asked, mapped back
    (`scipy.ndimage.affine_transform`, order 1, the inverse sampling), and written in the tool's encoding (int8
    p × 127, uint16 p × 255, float32 p) with the tomogram's voxel size, in one rename. A volume needing neither
    step is moved.
- **`services/segmentation/easymode.py`.** `ais_model_metadata` (`ais_model_title` reads through it) and
  `plane_pixel` (the X/Y pixel of a plane-by-plane network, None for a 3D one or one that states none).
- **`services/jobs/segment.py`.**
  - TTA default 8, at most 8.
  - `match_training_voxel` and `smooth_z` (both on).
  - The preamble and the SLURM note now carry job038's timings.
- **`drivers/segment.py`.**
  - Each target carries `plane_angstrom`, and the run's settings carry `segment_at` and `smooth_z`. Both are in
    `segmentations.json`'s parameters and in each feature's identity.
  - `stage` writes the 10 Å copy into the task's `in/` (in print-command mode it only says so) and keeps a
    complete one from an earlier attempt. It also lists the volumes the task still lacks; only those are
    passed to easymode.
  - The tools write into the task's `out/`. `verify_outputs` checks them on the grid they were made on.
  - `collect` finishes each volume into `segmentations/`, checks it on the tomogram's grid, writes the overlays
    and fractions as before, and removes the 10 Å copy (so does a task whose volumes were all there).
  - A re-run with other settings also removes the tools' leftovers in `out/`.
  - The supervisor's log names the voxel size, the TTA and the features smoothed with their pixel.
- **Open at runtime.** Whether FWHM = the X/Y pixel damps the bands enough; if not, the next knob is a wider
  Gaussian (a factor on that FWHM), not a fixed number of Å.
- **`ui/job_plugins/segment.py`.** The easymode method gets "Segment at 10 Å" and "Smooth 2.5D features along Z";
  the Ais-model method gets the smoothing.
- **`config/conf.template.yaml`.** The `segment` profile's time goes from 1:00:00 to 2:00:00.

## 5. Runtime check

1. **A fresh run.** Run Segment again with cytoplasm, nucleus and ribosome. A new Segment job keeps job038 for
   comparison; a job's TTA stays what it was saved with, so set 8 on an existing one.
   - Task log: "Segmenting at 10 Å: 635 × 635 × 317, resampling (spectrum cropped)".
   - easymode's lines: "at 50.0 A/px" and "inference at 10.0 Å/px".
   - Per volume: "smoothed along Z (FWHM 50 Å), mapped back onto 1024 × 1024 × 512" (ribosome: "mapped back"
     only).
2. **Position_10 against job038.** Compare the bands in X/Z and the blocks on the wall.
3. **Task time.** Check it against the per-tomogram limit (`job_resource_profiles.segment.time` in
   `config/conf.yaml`, 1 h there today).
4. **Settings.** The TTA field stops at 8. Switching "Segment at 10 Å" off and running again recomputes every
   volume.

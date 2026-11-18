# Roadmap 56 — napari 3-D that keeps up

3-D in the napari sessions (roadmap 54, the colleague's viewer of roadmap 55 S6) drew at 2–3 frames a second on
*Start now*. This roadmap measures where the time goes and takes the cheap wins first.

## 1. Where the time goes (measured 2026-11-18)

One 3-D frame (draw + read back, median of 5 at rotating angles) of MyrGFP Position_10 (1024 × 1024 × 512 at
6.2 Å): the tomogram, its five segmentations and two pick layers, on a 1632 × 1029 canvas. Plain napari 0.10 with
the layers the viewer's `view3d.py` draws, then the alternatives (`~/.crboost/napari_bundletest/bench3d.py`, jobs
1776351–1776354). Milliseconds:

| What is drawn | software, 8 CPUs | software, 32 CPUs | B200 (VirtualGL) | RTX 6000 (VirtualGL) |
|---|---|---|---|---|
| tomogram bin 2 + 5 segmentation volumes + picks (as built) | 380 | 120 | 62 | 14 |
| tomogram bin 4 + 5 segmentation volumes + picks | 381 | 117 | 62 | 24 |
| 5 segmentation volumes + picks, no tomogram | 340 | 107 | 57 | 25 |
| tomogram bin 2 + **one labels volume** + picks | **62** | **22** | 12 | 9.5 |
| tomogram bin 2 + segmentation meshes (720 k triangles) + picks | 156 | 231 | 29 | 11 |
| tomogram as one plane + meshes + picks | 154 | 257 | 12 | 14 |
| as built, ray step 2.0 instead of 0.8 | 194 | 64 | 46 | 10.5 |

Software is Mesa's llvmpipe on a B200 MIG slice; `LP_NUM_THREADS` changed nothing (llvmpipe already uses the job's
CPUs).

- napari ray-traces every volume on screen. The five segmentation volumes are almost all of a frame; the tomogram
  costs ~10 ms, and binning it further gains nothing.
- One labels volume (a label per feature) does the same work in one pass: 6× in software, 5× on a B200.
- llvmpipe scales with CPUs: 3× from 8 to 32.
- Meshes lose in software: marching cubes on the cytoplasm and nucleus gives 720 k noisy triangles. Only worth it
  decimated.
- The B200 is the slowest full card here and the dearest to hold; the RTX 6000 the fastest.
- Not measured: VNC's encoding and the network to the browser. Below ~60 ms a frame they likely set the pace; a
  smaller browser window helps both.

## 2. Stages

**S1 — one labels volume for the segmentations in 3-D.** Built 2026-11-18 in the colleague's viewer (`view3d.py`,
in the clone conf `napari.companion` names, for him to take with roadmap 55 S6's files). Layers that name the same
`metadata["merge_3d"]` group draw in 3-D as one labels volume, a label per visible layer where it passes
`metadata["level"]`, in its `metadata["color"]`. The layer covering most goes in first, so a membrane inside the
cytoplasm stays visible. `crboost_bundle.py` puts every crboost segmentation in group `crboost`. The cost: which
features show in 3-D is chosen in 2-D (the visible ones go in), not per feature by an eye in 3-D.

**S1b — smooth surfaces, the default (2026-11-18).** The merged labels volume was fast but blocky (whole bin-4
voxels) and all or nothing, so the user could not look at one feature. Now `view3d.py` draws each segmentation as
its own smooth mesh (`seg_mode = "surfaces"`):
- marching cubes on the bin-4 volume, lightly smoothed (Gaussian σ 0.7), specks under 500 voxels dropped;
- the step coarsened (up to 4) until a feature's mesh fits 40 k triangles (`NAPARI_3D_TRIANGLES`);
- Taubin smoothing.

The cytoplasm and nucleus blobs come down from ~300 k triangles each to 15–37 k, and membrane keeps step 1
(5 k triangles). Every feature gets a surface layer, hidden as it was in 2-D, so any can be turned on in 3-D. The
"crboost" dock's *3-D segmentations* switches between surfaces and merged.

Measured on 8 software CPUs, 1632 × 1029 canvas, tomogram bin 2 behind (jobs 1776726, 1776745):

| segmentations in 3-D | ms a frame | triangles |
|---|---|---|
| merged labels volume | 58 | — |
| meshes, step 1 | 145 | 660 k |
| meshes, 60 k budget per feature | 65 | 73 k |
| meshes, 25 k budget per feature | 54 | 37 k |

Bin 2 looked the same as bin 4 and took 30 s more to bin. In his viewer (job 1777126, its 488 × 949 canvas), a frame
took 32 ms with surfaces and 31 ms merged. Switching to 3-D took 6 s; changing mode 1.8 s, or 1.1 s back to the
cached surfaces.

**S2 — 32 CPUs for *Start now*.** Built 2026-11-18: conf `napari.software.cpus: 32` (lab conf; the template keeps 4).
The interactive QOS allows 48 CPUs per user, so a ChimeraX session still fits beside one napari session.

**S3 — RTX 6000 first for a full GPU.** Built 2026-11-18: conf `napari.prefer: rtx6000` (napari's own placement;
ChimeraX keeps `curation.prefer: b200`).

**S4 — decimated meshes (not started).** Segmentation surfaces as meshes of ~50 k triangles would let 3-D rotate on
any renderer at mesh speed, and the per-feature eye would come back. Only if S1–S3 are not enough.

**S5 — the transport (not started).** noVNC's quality and compression in the link, and a desktop size cap
(`resize=scale` with a fixed geometry instead of `resize=remote`), once someone measures frames reaching the
browser.

## 3. Checks

- **R1:** his viewer on a MIG slice, 3-D with the bundle: frame time at 8 and 32 CPUs, and a screenshot of the merged
  segmentations (§4).
- **R2:** the user rotates a tomogram in 3-D in a *Start now* session and says whether it keeps up.

## 4. Results

- **R1 passed 2026-11-18** (jobs 1776421 and 1776422, his viewer + MyrGFP Position_10's bundle on a MIG slice, its
  524 × 949 canvas between the docks): the five segmentations draw as one labels layer, `crboost segmentations
  (3-D, bin 4)`, beside the bin-2 tomogram and both pick layers. A frame takes 38 ms on 8 CPUs and 15.7 ms on 32,
  and the switch to 3-D 5 s. The labels surfaces are opaque, so the cytoplasm hides much; hide it in 2-D before
  turning to 3-D.
- R2 owed.

# 02 · What actually happens to a tomogram

This is the cryo-ET content of the talk. Everything else is plumbing around this.

## The flow

@figure. let's drop ts import, it's too specific. Otherwise let's use this to create a figure that looks like this: each stage is represented by the picture we generated, some modern tools are listed on the left (open source that take care of this conceptual stage) like relion,aretomo,warp for fsmotion, the milltiom template matching tools (pick the most used ones) with the one we use at each stage highlighted/bolded. (that's on the left of the box) (inisde the box that the image we produced form our ui heads -- the inputs/ outputs -- focus on the conceptual, not necessarily the fiel format, but please don't put whole fucking sentences in there) (on the right of each box -- the "what we did/what got better" from this page... )


@ui. produce ui elements from our job roster(like literal little rows with green "done" dots) and put them in this directory as high resolution pngs without any borders.

```
  RAW DATA on the microscope's dump
  ┌──────────────────────────────────────────────────────────────┐
  │  movies/  *.eer | *.tiff | *.mrc     one movie per TILT      │
  │  mdoc/    *.mdoc                     the acquisition log     │
  └───────────────────────────┬──────────────────────────────────┘
                              │  Import
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  tilt_series.star — frames grouped into tilt-series, with     │
  │  angle, dose order, optics. Nothing has moved yet.            │
  └───────────────────────────┬──────────────────────────────────┘
                              │  Motion & CTF      (per movie, GPU)
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  ONE 2-D AVERAGE PER TILT  + per-movie CTF fit                │
  │  beam-induced motion removed, dose-weighted                   │
  └───────────────────────────┬──────────────────────────────────┘
                              │  TS Import   (metadata only, no GPU)
                              ▼k
  ┌──────────────────────────────────────────────────────────────┐
  │  one .tomostar per tilt-series = "this is the stack, in this  │
  │  order, at these angles, with this accumulated dose"          │
  └───────────────────────────┬──────────────────────────────────┘
                              │  ▸ Tilt Filter  (optional, interactive)
                              │    drop the tilts that are junk
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  ALIGNMENT — AreTomo or IMOD patch tracking                   │
  │  per-tilt X/Y shifts, tilt-axis angle, refined tilt angles    │
  │  ▸ Miss Align (optional): learned refinement of the above     │
  └───────────────────────────┬──────────────────────────────────┘
                              │  TS CTF
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  per-tilt defocus + the TILT-SERIES HANDEDNESS decision       │
  │  (decided globally over all TS, then applied per tilt)        │
  └───────────────────────────┬──────────────────────────────────┘
                              │  Reconstruct
                              ▼
  ╔══════════════════════════════════════════════════════════════╗
  ║  A TOMOGRAM. A 3-D volume, at a chosen binned pixel size      ║
  ║  (default ~12 Å/px), optionally CTF-deconvolved for display.  ║
  ║  Plus EVEN/ODD half-tomograms, for the denoiser.              ║
  ╚═══════════════════════════┬══════════════════════════════════╝
                              │  ▸ Denoise train → predict  (optional)
                              │    cryoCARE (noise2noise on even/odd)
                              │    or IsoNet (+ missing-wedge correction)
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  a tomogram you can actually look at                          │
  └───────────────────────────┬──────────────────────────────────┘
                              │
        ═══════════════ PARTICLE PHASE ═══════════════
                              │
                              │  Template Match  (PyTOM, per tomogram)
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  NOT PICKS — two VOLUMES per tomogram:                        │
  │    scores.mrc  best cross-correlation at each voxel           │
  │    angles.mrc  which orientation achieved it                  │
  └───────────────────────────┬──────────────────────────────────┘
                              │  Pick candidates  (threshold + NMS)
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  candidates.star — coordinates + orientations + a score       │
  │  ▸ curate: hand-pick in ArtiaX, filter by CC, merge lists     │
  └───────────────────────────┬──────────────────────────────────┘
                              │  Subtomo Extraction  (RELION)
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  PSEUDO-SUBTOMOGRAMS — per-particle 2-D tilt stacks carrying   │
  │  their own CTF, NOT cut-out 3-D boxes                         │
  └───────────────────────────┬──────────────────────────────────┘
                              │  Reconstruct Particle
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  merged.mrc + half1/half2 — your first average                │
  └───────────────────────────┬──────────────────────────────────┘
                              │  Class 3D
                              ▼
                    classes, and a decision to make
```

---

## What changed at each step — the table version

| Stage | Domain before | Domain after | The thing that got better |
|---|---|---|---|
| Import | files on disk | a *tilt-series* | frames are grouped and ordered; dose is known |
| Motion & CTF | N movies per tilt | 1 image per tilt | beam-induced motion removed, dose-weighted, CTF fit |
| TS Import | loose images | a `.tomostar` stack | the tilt-series exists as an object |
| Tilt Filter | all tilts | kept tilts | junk tilts (ice, blank, blurred) no longer poison the alignment |
| Alignment | 2-D images | *registered* 2-D images | the projections agree on a common origin and tilt axis |
| Miss Align | coarse alignment | refined alignment | learned model closes residual misalignment |
| TS CTF | per-movie CTF | per-tilt-series CTF + handedness | the defocus gradient across the specimen is right |
| **Reconstruct** | registered projections | **a 3-D volume** | you now have a tomogram |
| Denoise | noisy volume | interpretable volume | SNR up; IsoNet also fills the missing wedge |
| Template Match | volume | score + angle volumes | every voxel has "how well does my particle fit here" |
| Pick candidates | score volume | a coordinate list | peaks become putative particles |
| Subtomo extraction | coordinates | per-particle 2-D stacks | each particle carries its own CTF model |
| Reconstruct Particle | particles | one map | signal averaged; half-maps for FSC |
| Class 3D | one map | classes | heterogeneity separated |

---

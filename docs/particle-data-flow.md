# Particle data flow — what each job does, what each file holds

**What this is:** the reference figure for the picking half of the pipeline. It answers
three questions that keep coming back: what is the actual difference between Template
matching, Pick candidates and Subtomo extraction; which STAR file carries which fact; and
what has to agree before two sets of picks may be united.

Written 2026-08-22 alongside `docs/roadmaps/picking_ui/12-aggregate-candidates.md`, which
depends on every fact below. Update the two together.

---

## 1. The frame of reference

Nothing in this document can be read without a `tomograms.star`. Coordinates are numbers;
`tomograms.star` is what makes them mean a place.

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │  tomograms.star            ONE ROW PER TOMOGRAM                      │
  ├──────────────────────────────────────────────────────────────────────┤
  │                                                                      │
  │   rlnTomoName                    the label, e.g. "TS_01"             │
  │                                  UNIQUE within a project.            │
  │                                  NOT unique across projects.         │
  │                                                                      │
  │   rlnTomoReconstructedTomogram   absolute path to the .mrc volume    │
  │                                  this IS unique, everywhere.         │
  │                                                                      │
  │   rlnTomoTiltSeriesPixelSize     unbinned Å/px                       │
  │   rlnTomoTomogramBinning         reconstruction binning factor       │
  │        pixel_size = the two multiplied  → binned recon Å/px          │
  │                                                                      │
  │   rlnTomoSizeX / Y / Z           UNBINNED volume dimensions          │
  │                                                                      │
  │   rlnTomoTiltSeriesStarFile      → the per-TS tilt star (geometry,   │
  │                                    per-tilt CTF, dose)               │
  │                                                                      │
  │   handedness                     which way Z runs                    │
  │                                  (set by flip_tiltseries_hand)       │
  │                                                                      │
  └──────────────────────────────────────────────────────────────────────┘
```

---

## 2. The two paths into a pick

There are exactly two ways a coordinate comes into existence, and they meet.

```
                        ┌───────────────────────┐
                        │    tomograms.star     │
                        │  (the frame, above)   │
                        └───────────┬───────────┘
                                    │
                ┌───────────────────┴───────────────────┐
                │                                       │
       ALGORITHMIC PATH                          DE-NOVO PATH
       (a template exists)                       (nothing exists yet)
                │                                       │
                ▼                                       ▼
```

### ① Template matching — `templatematching` · PyTOM · GPU

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                                                                    │
  │  CONSUMES                                                          │
  │      tomograms.star                  which volumes to search       │
  │      ts_ctf_tilt_series.star         per-tilt CTF for weighting    │
  │      template.mrc                    the species' reference        │
  │      mask.mrc                        where on the reference to     │
  │                                      score                         │
  │      Ø, symmetry, angular step       from the species / job tab    │
  │                                                                    │
  │  DOES                                                              │
  │      Slides the template over EVERY voxel of the tomogram, at      │
  │      EVERY orientation in the angle list, and records how well     │
  │      it correlates.                                                │
  │                                                                    │
  │      This is the expensive one. Cost scales with                   │
  │      (voxels) x (orientations).                                    │
  │                                                                    │
  │  PRODUCES     tmResults/                                           │
  │                                                                    │
  │      {tomo}_scores.mrc     a 3D VOLUME. At each voxel: the best    │
  │                            correlation any orientation achieved    │
  │                            with the template centred there.        │
  │                                                                    │
  │      {tomo}_angles.mrc     a 3D VOLUME. At each voxel: WHICH       │
  │                            orientation achieved it.                │
  │                                                                    │
  │      {tomo}_job.json       what the run was configured with.       │
  │                            Also the enumeration key downstream:    │
  │                            one file per tomogram TM actually       │
  │                            processed.                              │
  │                                                                    │
  │      tomograms.star        a copy, so the output is self-contained │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘

        ⚠  AFTER TEMPLATE MATCHING THERE ARE STILL NO PICKS.
           The output is three volumes per tomogram. Not a list.
           Nothing here knows how many particles there are.
```

### ①ʹ Manual picking — ArtiaX / import

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                                                                    │
  │  CONSUMES                                                          │
  │      the reconstructed tomogram, opened in a viewer                │
  │      a human's eyes                                                │
  │                                                                    │
  │  PRODUCES     one PickList, its star under Curation/               │
  │                                                                    │
  │      rlnTomoName                                                   │
  │      rlnCenteredCoordinateXAngst                                   │
  │      rlnCenteredCoordinateYAngst                                   │
  │      rlnCenteredCoordinateZAngst                                   │
  │                                                                    │
  │      ─────── that is the ENTIRE column set ───────                 │
  │                                                                    │
  │      No score. No orientation. No optics. Positions only.          │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘
```

### ② Pick candidates — `tmextractcand` · `pytom_extract_candidates.py`

Named "Template extract" until 2026-08-16. It extracts nothing; it picks. The rename was
peeve P-03.

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                                                                    │
  │  CONSUMES                                                          │
  │      tmResults/          the score + angle VOLUMES from ①          │
  │      tomograms.star                                                │
  │                                                                    │
  │  DOES     peak-finding on the score volume:                        │
  │                                                                    │
  │      1.  find local maxima                                         │
  │      2.  non-maximum suppression at the particle radius, so two    │
  │          peaks cannot be closer than one particle                  │
  │      3.  apply a cutoff, ONE of two incomparable strategies:       │
  │                                                                    │
  │            cc_threshold           raw LCC value, keep >= this      │
  │                                   typical 0.05 - 0.20              │
  │                                                                    │
  │            expected_false_positives                                │
  │                                   fit the noise distribution and   │
  │                                   pick the threshold that admits   │
  │                                   this many FPs per tomogram.      │
  │                                   strict=1, moderate=10, loose=100 │
  │                                                                    │
  │      4.  cap at max_num_particles                                  │
  │      5.  for each survivor, read its orientation out of the        │
  │          angle volume                                              │
  │                                                                    │
  │  PRODUCES                                                          │
  │      candidates.star                                               │
  │          rlnTomoName                                               │
  │          rlnCenteredCoordinateX/Y/ZAngst                           │
  │          rlnAngleRot / rlnAngleTilt / rlnAnglePsi   ◄ from ①       │
  │          rlnLCCmax                                 ◄ the score     │
  │          rlnTomoParticleName                                       │
  │                                                                    │
  │      optimisation_set.star     (see §3)                            │
  │                                                                    │
  │      NO optics block. Nothing here has pixels.                     │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘

        ⚠  ② IS A PEAK-FINDER OVER VOLUMES.
           Its input is a directory of .mrc score maps. It cannot be
           handed a coordinate list — there would be nothing for it
           to do. This is why "aggregate candidates" does not spawn
           a Pick candidates job. See roadmap 12.
```

### The plateau where both paths meet

```
     ┌──────────────────────────────────────────────────────────────┐
     │                                                              │
     │                THE PRE-EXTRACTION PLATEAU                    │
     │                                                              │
     │   Everything that reaches here is the same KIND of thing:    │
     │                                                              │
     │        rows of positions inside a named tomogram             │
     │                                                              │
     │   A manual list and a candidates.star differ only in how     │
     │   many columns they carry. Neither has pixels. Both can be   │
     │   united, filtered, deduplicated and handed to ③.            │
     │                                                              │
     │   THIS IS THE LAYER THE AGGREGATION UI OPERATES ON.          │
     │                                                              │
     └──────────────────────────────────────────────────────────────┘
```

### ③ Subtomo extraction — `subtomoExtraction` · `relion_tomo_subtomo`

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                                                                    │
  │  CONSUMES                                                          │
  │      optimisation_set.star  ──┬──►  particles.star                 │
  │                               │       who, and where               │
  │                               │                                    │
  │                               └──►  tomograms.star                 │
  │                                       in what frame, and how to    │
  │                                       get back to the tilt series  │
  │                                                                    │
  │  DOES     for each particle:                                       │
  │                                                                    │
  │      Goes back to the TILT SERIES — not the tomogram. Projects     │
  │      the 3D position into every tilt image, crops a 2D box there,  │
  │      weights it by accumulated dose and that tilt's CTF, and       │
  │      stacks the crops.                                             │
  │                                                                    │
  │      That stack is the "pseudo-subtomogram". It is not a cutout    │
  │      of the reconstruction.                                        │
  │                                                                    │
  │      GEOMETRY, all counted in BINNED voxels:                       │
  │          binning     output voxel = unbinned Å/px x this           │
  │          box_size    the reconstruction box. Must hold the         │
  │                      particle PLUS the CTF-delocalized signal      │
  │                      that high defocus smears outward.             │
  │                      Aim 2-3x particle diameter.                   │
  │          crop_size   what is kept after reconstruction — this is   │
  │                      what Refine3D loads, so it sets file size     │
  │                      and memory. <= box_size.                      │
  │                                                                    │
  │  PRODUCES                                                          │
  │      Subtomograms/*.mrcs        ◄══ PIXELS ON DISK, first time     │
  │                                                                    │
  │      particles.star  +  an OPTICS BLOCK:                           │
  │          rlnImageName                ◄ points at those pixels      │
  │          rlnOpticsGroup              ◄ join key to the optics      │
  │          rlnVoltage                                                │
  │          rlnSphericalAberration                                    │
  │          rlnAmplitudeContrast                                      │
  │          rlnTomoTiltSeriesPixelSize                                │
  │          rlnImageSize                ◄ the box                     │
  │          rlnImagePixelSize                                         │
  │          rlnTomoSubtomogramBinning                                 │
  │                                                                    │
  │      optimisation_set.star                                         │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
                   Reconstruct particle · Class3D · Refine3D
```

---

## 3. `optimisation_set.star` — the envelope

This file is why one aggregation UI can serve both stages.

```
  ┌────────────────────────────────────────────────────────────────────┐
  │  optimisation_set.star  is NOT data. It is a two-line pointer:     │
  │                                                                    │
  │      rlnTomoParticlesFile   ──►  particles.star                    │
  │      rlnTomoTomogramsFile   ──►  tomograms.star                    │
  │                                                                    │
  │  Both ② and ③ emit one. SAME ENVELOPE, TWO PAYLOAD GRADES:         │
  │                                                                    │
  │  ┌──────────────────────────┬───────────────────────────────────┐  │
  │  │  COORDINATE GRADE        │  PIXEL GRADE                      │  │
  │  │  (② and manual lists)    │  (③)                              │  │
  │  ├──────────────────────────┼───────────────────────────────────┤  │
  │  │  positions               │  positions                        │  │
  │  │  maybe orientations      │  orientations                     │  │
  │  │  maybe a score           │  maybe a score                    │  │
  │  │  ─                       │  rlnImageName ──► actual pixels   │  │
  │  │  ─                       │  an optics block                  │  │
  │  └──────────────────────────┴───────────────────────────────────┘  │
  │                                                                    │
  │  The grade determines what a merged set can be handed to:          │
  │                                                                    │
  │      coordinate grade   ──►  ③ Subtomo extraction                  │
  │      pixel grade        ──►  Reconstruct / Class3D / Refine3D      │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘
```

`relion_tomo_subtomo`'s reader is already lenient enough for this:
`subtomo_merge._read_input_particles_lenient` accepts a coordinate-grade star with no
optics block at all — *"Only `rlnTomoName` is required."* Feeding ③ a merged coordinate
set needs no driver change.

---

## 4. Where merging can happen

One species throughout. There is no workflow that unites two species into one particle set.

```
  L1   LISTS within ONE TOMOGRAM
       ────────────────────────────────────────────────────────────────
       auto (from ②)  ∪  manual (from ①ʹ)  ∪  imported

       What must agree:   nothing. One frame, one tomogram, one project.
       Output:            a merged PickList — it has a legal
                          (species_id, tomo_name, slug) key
       Code:              services/particles/pick_merge.py           ✔
       UI:                DELETED by 09-S3, nothing replaced it       ✘


  L2   TOMOGRAMS within ONE PROJECT
       ────────────────────────────────────────────────────────────────
       TS_01  ∪  TS_02  ∪  TS_07

       What must agree:   nothing. rlnTomoName is unique in a project.
       Output:            NOT a PickList — that key is per-tomogram.
                          A MergedSources/<slug>/ directory.
       Code:              none                                        ✘


  L3   ACROSS PROJECTS
       ────────────────────────────────────────────────────────────────
       projA/TS_01  ∪  projB/TS_04

       What must agree:   see §5. This is the whole difficulty.
       Output:            MergedSources/<slug>/
       Code:              merge_card.py + subtomo_merge.py
                          PIXEL GRADE ONLY today                      ✔
                          coordinate grade                            ✘
```

All four cases produce the same directory shape:

```
       MergedSources/<slug>/
           particles.star          coordinate grade OR pixel grade
           tomograms.star          the union of contributing frames
           optimisation_set.star   the envelope
           merge_summary.json      counts, sources, column coverage
```

L1 additionally registers a `PickList` so the merge gets a chip on the picks surface.

---

## 5. What must agree across projects

### 5a. Coordinate grade

```
  ✔  SAFE, and better than it looks
     ────────────────────────────────────────────────────────────────
     rlnCenteredCoordinate*Angst is measured in ANGSTROM from the
     tomogram CENTRE. It is binning-independent.

     Project A at bin 4 and project B at bin 6 describe the same
     physical place with the same numbers. Binning is a non-issue at
     this grade — which is the opposite of the usual expectation.


  ✘  rlnTomoName IS A LABEL, NOT AN IDENTITY
     ────────────────────────────────────────────────────────────────
     "TS_01" exists in both projects and means different tilt series.

     TWO SEPARATE KEYS ARE NEEDED, and they answer different questions:

       DISTINGUISHING — "are these two different things?"
           absolute rlnTomoReconstructedTomogram
           already the guard in subtomo_merge

       EQUATING — "are these the same acquisition?"
           mdoc SubFramePath + DateTime, i.e.
           TiltSeries.frames[0].raw_filename
           TiltSeries.frames[0].acquisition_time
           both already stored in the registry


  ✘  SAME ACQUISITION =/= TRANSFERABLE COORDINATES
     ────────────────────────────────────────────────────────────────
     Two projects can reconstruct the same tilt series with a
     different alignment, handedness or Z-height. Same physical
     object; different volume centre. Centred-Angstrom coordinates
     are measured from that centre, so identical numbers then point
     at different physical places.

     The acquisition key says "same TS".
     A SECOND GATE says "coordinates are transferable":

         handedness                    must match
         rlnTomoTiltSeriesPixelSize    must match
         rlnTomoTomogramBinning        must match
         rlnTomoSizeX / Y / Z          must match

     Any mismatch RAISES. Re-mapping coordinates between two
     different reconstructions is a separate problem and is not
     attempted.


  ✘  HANDEDNESS specifically
     ────────────────────────────────────────────────────────────────
     flip_tiltseries_hand differing between projects mirrors Z.
     Every imported pick lands on the wrong side of the section.
     This is the 412 chirality cascade. It is silent. It must raise.
```

### 5b. Pixel grade

```
  ✔  ALREADY GUARDED — subtomo_merge raises on:
     ────────────────────────────────────────────────────────────────
     · optics mismatch in the CRITICAL columns
           rlnVoltage
           rlnSphericalAberration
           rlnAmplitudeContrast
           rlnTomoTiltSeriesPixelSize
     · same rlnTomoName pointing at different reconstruction paths
     · particles referencing a tomogram absent from the merged star


  ✘  A REAL HOLE
     ────────────────────────────────────────────────────────────────
     rlnImageSize, rlnImagePixelSize and rlnTomoSubtomogramBinning
     sit in OPTIONAL_OPTICS_COLS. A mismatch only prints

         [MERGE WARN] Optional optics column '...' varies across
                      sources -- using primary value.

     to a server log nobody reads, and merges anyway.

     Two projects extracted at different box sizes then produce one
     particles.star claiming a single box for stacks that are
     physically two different sizes.

     These are promoted to CRITICAL. Roadmap 12, stage S0.
```

---

## 6. Ragged columns

Sources contribute different column sets. The union carries everything it can, but a
placeholder must never be readable as a measurement.

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │                                                                      │
  │  ORIENTATIONS  rlnAngleRot / rlnAngleTilt / rlnAnglePsi              │
  │  ──────────────────────────────────────────────────────────────────  │
  │      0, 0, 0 is RELION's own encoding of "no orientation prior".     │
  │      It is NOT an invented value; it is the correct one, and         │
  │      services/particles/list_extraction.py already writes it for     │
  │      manual picks.                                                   │
  │                                                                      │
  │      → Write 0,0,0. Record coverage in merge_summary.json.           │
  │                                                                      │
  │                                                                      │
  │  SCORE  rlnLCCmax  (or rlnAutopickFigureOfMerit /                    │
  │                     rlnMaxValueProbDistribution)                     │
  │  ──────────────────────────────────────────────────────────────────  │
  │      0 IS A LEGAL LCC VALUE. This is the trap.                       │
  │                                                                      │
  │          a "score >= 0.05" filter silently drops every manual pick   │
  │          a "score >= 0"    filter silently keeps everything          │
  │                                                                      │
  │      → A ragged score column is DROPPED from the RELION-facing       │
  │        particles.star.                                               │
  │      → Per-row values are preserved in a sidecar keyed by            │
  │        rlnTomoParticleName.                                          │
  │      → The UI states the coverage wherever a score filter is         │
  │        offered:  "applies to 1,204 of 3,890 picks".                  │
  │                                                                      │
  │                                                                      │
  │  ANY OTHER RAGGED COLUMN                                             │
  │  ──────────────────────────────────────────────────────────────────  │
  │      → Preserved in the sidecar, coverage in merge_summary.json.     │
  │      → Promoted into the main star only when EVERY source            │
  │        carries it.                                                   │
  │                                                                      │
  └──────────────────────────────────────────────────────────────────────┘
```

The general rule, and the reason for all of the above:

> **A placeholder must never be indistinguishable from a measurement.**
> Where no honest placeholder exists, the column leaves the machine-readable file and
> becomes a stated fact in the UI instead.

This is the same policy as CLAUDE.md's *"Surfacing uncertainty — never fail silently,
never invent defaults"*, applied to a column rather than a parameter.

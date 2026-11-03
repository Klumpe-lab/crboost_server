# Survey — picking, segmentation and multi-particle refinement tools (2026-10)

A snapshot of what exists beyond crboost's current particle stage (PyTOM template matching → RELION), written
while planning AGG412 (roadmaps 29, 30) and the Warp export job (roadmap 24 stage 4). Not a plan. Literature
and community sources only; nothing here was installed or run.

**Decision (maintainer, 2026-10-10):** stay with PyTOM for picking; add easymode later for segmentation. M and
the other pickers are noted for reference.

## 1. Why RELION sits at the tail of the pipeline

Warp's intended pipeline is Warp → RELION → M:
- **Warp** preprocesses and exports particles.
- **RELION** classifies and runs the first refinement; Warp has neither.
- **M** runs the final multi-particle refinement.

crboost implements the first two steps with RELION's own extraction (`relion_tomo_subtomo`). That extraction
only knows rigid per-tilt geometry, which is why Warp's particle export (`ts_export_particles`) is needed once
alignments carry local grids (miss-alignment's `[N,N]` rounds; M). See roadmap 21 §3.

## 2. M (Warp's multi-particle refinement)

Not SPA-only: M treats frame series and tilt series in one framework. Its headline result was in situ, a
ribosome–antibiotic complex at ~3.5–3.7 Å inside bacterial cells via Warp → RELION → M.

What it refines, per tilt series:
- per-tilt image warps;
- a volume deformation over the exposure;
- per-tilt CTF and defocus, and optical aberrations;
- particle poses, for any number of species at once, each at its own resolution.

What that would mean for crboost:
- **Better tomograms for everything.** M's refined geometry goes back into the Warp XMLs, so a re-run of
  Warp's CTF and reconstruction gives sharper tomograms for every downstream use: other species, membranes,
  organelles. Community practice: "ribosomes as fiducials".
- **Same plumbing as miss-alignment.** M would be another refiner of the alignment XMLs, through the
  `JobSpec.refines` mechanism, so tsCtf and tsReconstruct follow it unchanged.
- **Final per-species maps.** It needs Warp-side particle data, i.e. the Warp export job.
- **Limit:** an abundant species has to drive the geometry. Community advice is that a rare species stuck at
  ~20 Å or worse gains little on its own. For datasets without particles, miss-alignment is the tool instead.

## 3. Pickers compared with template matching

Reported F1 scores for ribosomes (F1 combines missed and false picks; 1.0 = perfect). Datasets and protocols
differ, so the numbers are not directly comparable:

| Method | Reported F1 | Source |
|---|---|---|
| PyTOM high-confidence TM | 0.77 | TM paper, Nat Commun 2024 |
| DeepFinder, trained on experimental data | 0.83 | both papers |
| DeePiCt | 0.79 | both papers |
| DeepFinder, trained on simulations only | 0.83 | Template Learning, Nat Commun 2025 (EMPIAR-10988) |
| TM in that benchmark | 0.49 | Template Learning |

**TomoTwin** (Nature Methods 2023)
- A pretrained network turns every subvolume into a fingerprint (an embedding). Give it a few examples or a
  reference and it finds similar ones; clustering the fingerprints finds unknown species. No per-dataset
  training. Median validation F1 0.88.
- Limits: globular complexes above ~150 kDa; no membrane, filament or small proteins (listed as future work);
  positions only, no orientations; tomograms rescaled to its fixed pixel size (believed 10 Å, unverified).

**DeepFinder** (Nature Methods 2021)
- A 3D U-Net labels voxels per species, and the labelled blobs become picks. Trained per target, from curated
  TM picks or, per the Template Learning paper, from simulations alone.

**CZII Kaggle challenge (2025)**
- More than 1,000 participants built pickers for five species over hundreds of experimental tomograms. The
  write-up ("Lessons learned from a Kaggle challenge for particle picking in cryo-electron tomography",
  bioRxiv Nov 2025, reported as published in Nature Methods) says the winners beat existing tools.
- Winners were mostly 2.5D/3D U-Net heatmap detectors with heavy augmentation; the released 4th-place write-up
  is one example.
- Finding: averaging tolerates moderate over-picking, not severe.
- The tomograms and winning annotations are on the CryoET Data Portal as a benchmark. How well the winning code
  is packaged is unknown.

**Maturity:** all are published and open source, but they are research software: installation work, format
conversion, GPUs.

**Where networks beat TM:**
- speed (seconds to minutes per tomogram, against 12–35 min per species per tomogram for our TM);
- targets without a good template (pleomorphic particles, vesicles, organelles, membranes, filaments);
- rejecting structured background (membranes, edges, granules) that TM scores high;
- many species in one pass.

**Where TM stays ahead:** known rigid structures, no training needed, orientations included, and its σ
statistics are what roadmap 30's controls are built on.

## 4. Segmentation (membranes, organelles, filaments)

- **easymode** (preprint 2026): a library of pretrained general networks for cellular cryo-ET, trained on more
  than 4,000 tilt series; no per-dataset training. The authors determined in situ IMPDH filaments at 4.0 Å
  from its output. Already used once by the lab for an actin project. **The planned addition.**
- **MemBrain-seg:** pretrained membrane instance segmentation, widely used, e.g. a 2026 J Cell Biol study of
  membrane thickness across organelles.
- **Research-stage:** ETSAM (Segment Anything 2 fine-tuned for membranes), SABER (automatic organelle
  segmentation, 2026), unsupervised segmentation from Stable Diffusion features (2025), and the POPSICLE
  benchmark (2026).
- **Synergy with TM:** a segmentation mask can restrict PyTOM's search (cytosol only, membrane only). That is
  faster and removes much of the structured background behind TM false positives.

## Sources

- [TomoTwin, Nature Methods 2023](https://www.nature.com/articles/s41592-023-01878-z)
- [TomoTwin release note](https://www.mpi-dortmund.mpg.de/news/tomotwin)
- [Template Learning, Nat Commun 2025](https://www.nature.com/articles/s41467-025-63895-0)
- [High-confidence 3D template matching, Nat Commun 2024](https://www.nature.com/articles/s41467-024-47839-8)
- [Tomogram exploration through template matching and deep learning (review, 2026)](https://www.sciencedirect.com/science/article/pii/S0959440X26001016)
- [Kaggle challenge lessons](https://www.biorxiv.org/content/10.1101/2025.11.03.686153.full.pdf)
- [2.5D U-Net, Kaggle 4th place](https://ar5iv.labs.arxiv.org/html/2502.13484)
- [M preprint](https://www.biorxiv.org/content/10.1101/2020.06.05.136341.full.pdf)
- [M, PMC](https://pmc.ncbi.nlm.nih.gov/articles/PMC7611018)
- [Dynamo ↔ Warp/M framework](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC8389456/)
- [warp-em: ribosomes as fiducials](https://groups.google.com/g/warp-em/c/0rdmC9CU1L4)
- [warp-em: co-refining species in M](https://groups.google.com/g/warp-em/c/1RAxrKRbYZ0/m/uxKjoSbhBwAJ)
- [easymode preprint](https://sciety.org/articles/activity/10.64898/2026.05.19.726344)
- [easymode on SBGrid](https://sbgrid.org/software/detail/easymode)
- [Membrane thickness across organelles, JCB](https://doi.org/10.1083/jcb.202504053)
- [ETSAM](https://pmc.ncbi.nlm.nih.gov/articles/PMC12697732)
- [POPSICLE benchmark](https://arxiv.org/pdf/2606.10255)
- [Segmentation review, 2025](https://www.sciencedirect.com/science/article/pii/S0959440X25001320)

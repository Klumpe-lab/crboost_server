# Survey — segmentation and non-particle annotations in cryo-ET (2026-10)

What the segmentation tools (membranes, organelles, filaments, vesicles, granules, artefact classes) read and
write, how the ecosystem represents annotations that are not particles, how other pipelines integrate them, and
what crboost has today. It follows `picking-segmentation-refinement-2026-10.md` (which chose easymode as the next
tool). It is not a plan. Sources: papers, docs and the tools' source code read on GitHub (2026-10-19). Nothing was
installed or run; the only local reads were the lab's container definitions, the easymode model cache listing and
two MRC headers. UNVERIFIED marks what neither source nor docs confirmed.

## 1. What the tools have in common

### 1.1 The frame (where an annotation lives)

- **CZ portal:** every annotation file is pinned to an `alignment_id` and a tomogram voxel spacing, not to a
  tomogram. Tomograms carry structured variant fields (`reconstruction_method`, `processing` raw/filtered/denoised,
  `ctf_corrected`, `tomogram_version`) and offsets.
- **copick:** Run → VoxelSpacing → Tomogram (`tomo_type` string). Picks, meshes, filaments and segmentations belong
  to the run; segmentations also to a voxel spacing. There is no alignment entity, so one run means one alignment.
- **RELION 5:** the `tomograms.star` row (name, sizes, pixel size, binning, hand); particles in centred Å.
- **The tools themselves** (Ais, easymode, membrain-seg, TARDIS, tracET) have no frame: they write back on the
  input grid, copy (or lose) the header voxel size, never set an origin, and identify a tomogram by its file stem.

So the frame exists only in whatever manages the project. Densities computed from one reconstruction (raw,
even/odd, cryoCARE, IsoNet, N2N) share its frame; a different alignment is a different frame.

### 1.2 Object identity

- copick `PickableObject`: `name` (slug, no `_`), `is_particle`, `label` (int ≥ 1, 0 = background), RGBA
  `color`, `radius` (Å), `identifier` (GO / UniProtKB / CHEBI / PDB / UBERON / CL / CDPO), `pdb_id`, `emdb_id`,
  `map_threshold`, `metadata` (filaments: polar, helical rise, twist).
- Portal: mandatory `annotation_object_name` + `annotation_object_id` (regex-validated GO, UniProtKB, …),
  optional `object_state`. Non-biological classes (carbon, ice contamination, vacuum, tomogram edge, artefacts) use
  the portal's own CDPO namespace; GO covers membranes, organelles, filaments.
- GO cellular-component ids checked: membrane 0016020, plasma membrane 0005886, mitochondrion 0005739, crista
  0030061, ER 0005783, nuclear envelope 0005635, nuclear pore 0005643, cytosol 0005829, vesicle 0031982, lipid
  droplet 0005811, thylakoid 0009579, microtubule 0005874, actin filament 0005884, intermediate filament 0005882,
  ribosome 0005840, stress granule 0010494.
- The tools use bare strings: easymode registry keys (`atp_synthase`), the Ais `.scnm` title (which becomes the
  output suffix), Pom ontology names. Scipion, nextPYP and RELION type segmentations by geometry only (mask, open
  or closed surface, filament, point set), with no biological identity.

### 1.3 Representations

| Representation | Written by | Format, units |
|---|---|---|
| Probability volume | Ais (p×255, uint8 stored as MRC mode 6), easymode (int8 p×127 default; uint16 p×255; float32 p), membrain-seg `--store-probabilities` (logits, f16/f32), TARDIS `-ct 0` (float tif) | MRC on the input grid |
| Binary mask | membrain-seg (int8), TARDIS `_semantic` (uint8), Pom `create_mask` (int8 0/127), portal SegmentationMask (int8) | MRC / OME-Zarr |
| Instance labels | membrain-seg `--store-connected-components` (replaces the mask in the same file), TARDIS `_instance`, tracET `_labeled.mrc`, portal InstanceSegmentationMask (uint16), copick instance / panoptic | MRC / OME-Zarr |
| Instance point cloud | TARDIS `_instances.csv` (Å, corner origin), tracET (voxel indices, no voxel size) | CSV |
| Triangle mesh | membrain-pick (`.obj`, pixels), Surface Morphometrics (VTP, nm), Mosaic (OBJ/PLY/STL), TARDIS (STL, membranes), copick and portal (GLB, Å) | |
| Polyline / spline per instance | RELION 5 filaments (`rlnTomoManifoldIndex`), TARDIS (sorted polylines, Amira spatial graph), tracET (`_skel_graph.csv`, VTP), Ais filament pick (sampled points + `aisFilamentID`), copick Filaments (Å; linear / catmull-rom / bspline), Dynamo, ArtiaX CurvedLine | |
| Primitive | RELION spheres (`rlnSphereRadius`), Dynamo vesicles, Mosaic sphere/ellipsoid/cylinder, ArtiaX sphere | |
| Oriented points | RELION (ZYZ; RELION 5 adds `rlnTomoSubtomogram*` and priors), Dynamo (ZXZ), pyTME TSV, membrain-pick STAR (non-centred Å + angles), copick (location in Å + 4×4) | |
| Numbers per tomogram / particle | Pom `summary.star` (% per feature), Pom `contextualize` columns, membrain-stats, Surface Morphometrics CSV | |

### 1.4 Provenance

- copick: user + session per entity (session `0` = tool output). copick-pipeliner (CCP-EM Pipeliner jobs for
  copick, 2026) sets session = pipeliner job number, so reruns never overwrite, and writes a manifest per job with
  exact URIs, per-run geometry, provenance and orientation trust (`measured`, `identity_initialisation`,
  `filament_frame`).
- Portal: mandatory method text and `method_type` (manual / automated / hybrid / simulated), recommended software
  and ground-truth flag, optional precision/recall and model-weight links. copick drops `method_type` and
  ground-truth status on import.
- The tools record nothing in their outputs: an easymode volume does not say which model made it.

### 1.5 Conversions (what each tool actually is)

| Operation | Implementations |
|---|---|
| Density → volume | easymode, Ais, membrain-seg, TARDIS, SABER, ETSAM, CryoSiam, CryoViT, DeePiCt |
| Threshold / components | membrain-seg `thresholds` / `components`, Pom `create_mask`, copick-utils `process` |
| Volume → points | Ais / easymode `pick` (EDT peaks or centroids), tracET `get_cluster`, copick-utils `seg2picks` |
| Volume → mesh | membrain-pick `convert_*`, Surface Morphometrics (screened Poisson), Surforama (marching cubes + remesh), Mosaic, copick-utils `seg2mesh` |
| Volume → skeleton / polylines | tracET, TARDIS, Ais filament pick, membrain-seg `skeletonize`, copick-utils `seg2fil` |
| Mesh or curve → oriented points | membrain-pick, RELION `relion_tomo_get_particle_poses`, Mosaic sample, ArtiaX, Dynamo models, Surforama, nextPYP |
| Oriented points → search constraint | pyTME `--orientations` (cone + positional uncertainty), RELION `--sigma_tilt` / `--sigma_psi` priors |
| Volumes → TM search restriction | pytom `--tomogram-mask`, Pom 1.1 ASTM (own engine), Pom 1.2 `create_mask` |
| Points × volumes → context | Pom `contextualize` (fraction in a sphere, signed distance), Pom 1.1 CAPP (cube mean), Scipion "mask 3D coordinates" (drops), tomoviz "filter by normal" |
| Statistics | Pom `summarize`, membrain-stats, Surface Morphometrics, cryoCAT memthick, TARDIS analysis |

## 2. Tool I/O contracts

### 2.1 easymode
Pretrained general networks, no per-dataset training (bioRxiv 2026). Trained on 4,100–4,700 tilt series (the
number grows across preprint, README and docs), aligned with AreTomo3 and reconstructed by Warp at 10 Å/px as
raw, even/odd and N2N flavours; missing-wedge-corrected volumes were not in the training data. GPL-3.

- **Features (24):** actin, atp_synthase, chromatin, cytoplasm, cytoplasmic_granule, ferritin, hfimpdh,
  ice_particle, intermediate_filament, lipid_droplet, membrane, microtubule, mip, mitochondrial_granule,
  mitochondrion, npc, nuclear_envelope, nucleus, prohibitin, proteasome, ribosome, tric, vault, void. Also
  `n2n_direct`, `ddw_direct`, `iso_direct` (denoising) and `tilt`.
- **Two engines.** `.h5` models run natively in TF (3D residual U-Nets, MemBrain-seg style, 160³ tiles);
  `.scnm` models shell out to `ais segment` (2D / 2.5D, organelles and void at up to 50 Å). Variant tags:
  `sv2`/`sv3` (2D/3D training labels) × `2d`/`2.5d`/`3d` (network).
- **Input:** any voxel size; resampled to the model's apix when > 5 % off. A header apix ≤ 1.0 is silently
  replaced by 10 Å, and 10 is written into the output header.
- **Output:** `<out>/<tomo>__<feature>.mrc` at the original shape, header voxel size copied, origin unset.
  Encoding: native engine `--format int8` (default, p×127), `uint16` (p×255), `float32` (p); Ais engine always
  p×255 (uint8 written as mode 6). One output directory can hold both encodings.
- **Claim files:** a 10³ volume of −1 claims a tomogram so several nodes can share a directory; existing outputs
  are skipped. A killed run leaves placeholders that look like outputs.
- **Pick:** always `ais pick`; the lab-era code hard-codes Warp's `_10.00Apx` naming to rewrite
  `rlnMicrographName` to `.tomostar`; documented export is `WarpTools ts_export_particles --coords_angpix 10`.
  Spawns `cpu_count()` processes regardless of the allocation (now capped at 32).
- **Distribution:** Hugging Face. Lab-era code (≈ v1.0.0, May 2026) reads `mgflast/easymode`; ≥ 1.2.3 reads
  `mgflast/easymode-v2` with a `registry.json` and `--version <tag>`. When online, both re-download whenever
  the remote timestamp is newer. Between 16 and 20 Sep 2026 the legacy repo changed under the old code: 3D
  `.h5` files for actin, membrane, npc, prohibitin, mitochondrial_granule, vault and void were deleted and
  ribosome, microtubule and tric were replaced by new weights, so an old container with network access now runs
  different models than in May.
- **Current release:** 1.2.5 (17 Sep 2026) requires `ais-cryoet ≥ 1.2.36` and `pom-cryoet ≥ 1.2.0`. It also
  wraps WarpTools + AreTomo3 reconstruction, denoising and an experimental tilt selection.
- **Speed:** 25–90 s per 10 Å volume per GPU for 3D networks, ≈ 2 s for the 50 Å 2D ones; 1,034 tomograms in
  ≈ 90 min on 8 consumer GPUs. TRiC: 16 s segmentation vs 370 s template matching per tomogram.
- **Stated limits:** not for arbitrary targets; false negatives unquantified; filaments along Z missed; nuclear
  envelope missed when in-plane; training data biased to human FIB lamellae.
- **In-situ IMPDH at 4.0 Å:** a dedicated network found filaments in 19 of 1,034 tomograms; globular picking at
  110 Å spacing inside the segmentation gave ≈ 10,000 particles → WarpTools export → RELION 5 → M (D4).

### 2.2 Ais
eLife 2024; GPL-3. Annotate slices, train one network per feature, apply. 38 public models on aiscryoet.org
(10–27.2 Å/px, numeric ids, no versioning).

- **`ais segment`:** `<out>/<tomostem>__<model title>.mrc`; XY resampled to the model apix when > 5 % off, Z never;
  per-slice z-scoring; output zoomed back to the input shape, header voxel size copied, origin unset; p×255 uint8
  (mode 6 on disk). Same claim-file mechanism as easymode. `main()` uses `parse_known_args`, so a misspelt flag is
  silently ignored.
- **`ais pick`:** threshold on the 0–255 scale with strict `>` (128 ≈ p 0.5). Blob mode: connected components,
  size filter, EDT peaks with `min_distance = max(3, spacing/px)`; `-centroid` gives one point per component.
  Filament mode: skeleton, branch pruning by `-length`, spline, sampling every `-spacing`; tilt and psi from the
  tangent, rot synthetic (k × twist), polarity arbitrary. Output `<tomo>__<target>_coords.star`, one
  `data_particles` block, no optics, no pixel size; **coordinates are 0-based voxel indices of the segmentation
  grid (corner origin)**. Filament rows carry `aisFilamentID`, `aisFilamentConnectivity`, curvature; no
  `rlnHelicalTubeID`, no prior columns. Current master adds `-orient normal|long-axis` (PCA) for blobs.
- **Files:** `.scnm` (Keras `.h5` + metadata: title, colour, apix, box size, threshold, …), `.scns` (annotated
  scene), `.scnt` (training stack). Model interactions (colocalise / avoid) live only in `.scnmgroup`; their use
  in the CLI is UNVERIFIED.
- **Versions:** lab commit `3a1d006` (6 Apr 2026, reports 1.0.47); current 1.2.39 (Sep 2026). TF ≤ 2.11, numpy
  < 2, protobuf ≤ 3.20.3. ≈ 1 min per volume per feature (2D networks).

### 2.3 Pom
Preprint 2025 ("Scaling data analyses in cellular cryoET using comprehensive segmentation"): 25 GO-tagged
features on 1,829 *Chlamydomonas* tomograms, deposited on the portal. 1.2 is a rewrite of 1.1.

- **1.1.1 (lab):** `project_configuration.json` in the working directory; `summarize` → `summary.xlsx` (soft
  volume % after a Z-margin crop); **ASTM** = threshold/shell masks from segmentations, include/exclude logic, and
  its own OpenGL 2D real-space matcher (Pommie), output TSV without recoverable angles; **CAPP** = per-particle
  cube means of context features, filtered with sliders → STAR.
- **1.2.1 (current):** `pom/config.json` lists tomogram and segmentation sources as plain paths; tomograms are
  identified by stem, segmentations by `<stem>__<feature>.mrc`. `summarize` → `summary.star` (% per feature, no
  threshold, no Z crop, so not comparable with 1.1). `projections` / `render` (headless via EGL) → PNG/GIF per
  tomogram. `create_mask --samplers feature:sigmaÅ:threshold[:dust] [!feature:…]` → int8 0/127 at the tomogram
  voxel size. `contextualize`: sphere sampler `pom<Feature><R>A` (mean value in a sphere, 0–1) and distance
  sampler `pomDist<Feature>T<thr>` (signed EDT, Å, negative inside), offsets along the particle axis. It reads
  `rlnCoordinate*` as tomogram voxels (or Warp `wrpCoordinate*` in Å), not `rlnCenteredCoordinate*Angst`;
  overwrites the input STAR by default; silently drops particles whose tomogram it cannot find.

### 2.4 MemBrain v2
Nat Methods, Sep 2026. The de facto membrane tool; wrapped by Scipion and nextPYP.

- **membrain-seg** (0.0.10): `membrain segment --tomogram-path --ckpt-path [--rescale-patches] [--in-pixel-size]
  [--out-pixel-size 10] [--store-probabilities] [--store-connected-components] [--connected-component-thres N]
  [--store-uncertainty-map] [--segmentation-threshold 0.0] [--test-time-augmentation]`. Output
  `<base>_<ckpt>_segmented.mrc` (int8 binary, or instance labels 1..N with 26-connectivity when components are
  stored); `_scores.mrc` are averaged logits (threshold 0 = p 0.5). Trained at 10 Å; `--rescale-patches` keeps
  the output on the input grid. Header voxel size kept, origin lost. `tomo_preprocessing match_pixel_size` for
  explicit rescaling.
- **membrain-pick** (0.0.9): one binary MRC per membrane → `.obj` mesh (pixels) + `.h5`; a mesh network predicts
  centres; STAR with `rlnCoordinate*` in non-centred Å and `rlnAngle*` (normal = particle Z; in-plane angle set
  to 0 in the code). Suggests refining with `--sigma_tilt 3 --sigma_psi 3`.
- **membrain-stats:** protein concentration per area (edge exclusion 50 Å), concentration vs distance or a
  per-vertex property, geodesic nearest neighbours, Ripley statistics.

### 2.5 TARDIS-em
Preprint; lab container 0.3.10, latest 0.3.36 (Oct 2026). `tardis_mem`, `tardis_mt`, `tardis_actin` (beta) on a
folder. Rescales internally to 15 Å (membranes) or 25 Å (MT, actin) and writes back on the input grid. Output
selector `-out <semantic>_<instance>`: semantic MRC/TIF/…, instance masks, `_instances.csv` (`IDs, X [A], Y [A],
Z [A]`, corner origin), Amira spatial graph, STL. Membranes come out as unordered point clouds per instance,
filaments as sorted polylines. No polarity or orientation. Run on > 13k portal tomograms.

### 2.6 tracET
PLOS One 2025; 2.2.2 in the lab. Saliency (smoothed distance transform), non-max suppression to centre surfaces /
lines / blobs, graph tracing (`_skel_graph.csv` Component, Branch, X, Y, Z; VTP), clustering to centres. All
coordinates are voxel indices and no voxel size is written. Use the short flags (long options are broken).

### 2.7 SABER
CZ (MIT; `saber-em` 1.0.1, Jun 2026). SAM2 automatic masks on slabs, propagated through Z with the video
predictor; optional light classifier on frozen embeddings trained from a few expert clicks in its GUI; SAM3 text
prompts for micrographs. **3D needs a copick project** (Zarr copies of the tomograms, 10 Å and denoised
recommended); writes uint8 multilabel Zarr segmentations under user `saber`. SAM2.1 checkpoints download into the
package directory and SAM3 weights come from a gated Hugging Face repo, so a read-only container needs them baked
in. CUDA 12. Used for 25,210 organelle segmentations in a 2026 portal deposition (with MemBrain-seg).

### 2.8 RELION 5 "Pick tomograms"
napari-based; modes particles, spheres, filaments (surfaces are documented as not functional and absent from the
source). Writes `annotations/<ts>_{particles,spheres,filaments}.star` (binned-tomogram voxel coordinates; spheres
add `rlnSphereRadius`; filaments add `rlnTomoManifoldIndex` with ordered control points).
`relion_tomo_get_particle_poses` samples them at a spacing: Z along the sphere normal or filament axis, written as
`rlnTomoSubtomogramRot/Tilt/Psi`, with constant `rlnAngleRot/Tilt/Psi = 0/90/0` and priors tilt 90, psi 0;
filaments add `rlnHelicalTubeID`, `rlnHelicalTrackLengthAngst` (divided by the pixel size in the source) and
`rlnAnglePsiFlipRatio 0.5` when polarity is unknown. No segmentation anywhere in RELION 5/5.1.

### 2.9 pytom-match-pick (lab 0.10.0; latest 0.14.0)
`--tomogram-mask` must be an MRC of exactly the tomogram's shape (never resampled). During matching it only skips
split sub-volumes whose mask is entirely ≤ 0 (the score map is not masked); `pytom_extract_candidates.py` sets
`score[mask <= 0] = 0` (job mask, an override `--tomogram-mask`, or `--ignore_tomogram_mask`). No per-position
orientation priors; nothing membrane-specific through 0.14.

### 2.10 Mosaic and pyTME
Mosaic (`mosaic-gui` 1.3.2, also headless `mosaic-pipeline`): imports segmentations, meshes and points; fits
spheres, ellipsoids, cylinders, RBF height fields, meshes and splines; samples seeds by count or distance with an
offset along the normal; exports RELION-4-style STAR, pyTME TSV, meshes, label volumes. pyTME 0.3.6:
`--orientations` (seeds or a mesh) with `--orientations-cone` (degrees around the normal) and
`--orientations-uncertainty`; it reads `rlnCenteredCoordinate*Angst` as voxels without conversion, so RELION 5
stars must be converted first. ColabSeg is superseded by Mosaic.

### 2.11 Dynamo and ArtiaX models
Dynamo models (filaments with torsion, membranes by levels, vesicles, dipoles) all end in crop points + crop
angles (tables: ZXZ, voxels). ArtiaX models: sphere, line, curved line (spline; particles every `spacing`, Z =
tangent, optional twist), surface (height field; Z = normal), boundary, triangulated and arbitrary surfaces; saved
as `np.savez`. chimerax-copick builds on ArtiaX.

### 2.12 Analyses
Surface Morphometrics (JCB 2023): label MRC → screened-Poisson mesh → curvature, verticality, intra- and
inter-membrane distances and orientations, thickness; per-triangle CSV (nm by default) + VTP. Membrane thickness
across organelles (JCB 2026): MemBrain-seg instances → surface points with refined normals → paired leaflets →
thickness (cryoCAT `memthick`). A second JCB 2026 paper fits head-group peaks along Surface Morphometrics normals.
PyCurv underlies curvature work; Surforama (napari) picks on meshes with Z = normal.

## 3. How other pipelines integrate segmentation

- **ScipionTomo:** `TomoMask` (a `Tomogram` subclass, label or probability volume) linked to its tomogram only by
  tsId and path; `SetOfMeshes` is really a point set with a `groupId` per object. Protocols: meshes from masks
  (random 5 % of voxels), "mask 3D coordinates" (drops outside picks), "filter by normal" (keeps particles whose Z
  matches the nearest vesicle normal). Plugins wrap membrain, tardis, tomosegmemtv, pyseg, deepfinder, dynamo.
- **nextPYP** (the closest analogue): energy-based closed-surface segmentation feeding surface picking;
  MemBrain-seg (May 2025) and TARDIS (Aug 2025) for open surfaces and filaments; since Mar 2026 segmentation
  normals seed membrane-protein orientations and picks are restricted to a slab.
- **RELION 5/5.1:** annotation-based picking only (§2.8). **Warp / WarpTools / M:** no 3D segmentation (2D BoxNet
  masks feed tilt exclusion); easymode and Pom are the de facto companions. **TomoBEAR, CryoSPARC:** none.
- **CZ portal:** portal-wide MemBrain-seg (13,275 tomograms in 3.5 days) and TARDIS runs; comprehensive
  *Chlamydomonas* segmentation (Pom); SABER + MemBrain-seg organelles (2026).
- **copick-pipeliner** (2026): CCP-EM Pipeliner jobs `copick.project` (from a RELION `tomograms.star` or the
  portal), `copick.membrain`, `copick.easymode`, `copick.filaments.*`; session = job number; manifests with URIs,
  geometry and provenance; writes `rlnCenteredCoordinate*Angst` centred on the picked tomogram.
- **Pattern:** pipelines type segmentations by representation, portals and copick by biological identity; only
  Pom does both. Every framework surfaces thresholds as a user choice (nextPYP per virion, MemBrain, Pom).

## 4. What gets segmented and measured

- **Targets:** generic membrane; mitochondria (OMM, IMM, cristae, granules); ER; nuclear envelope and NPC;
  nucleus, cytoplasm; vesicles, endosomes, lipid droplets; microtubules, actin, intermediate filaments; ribosomes
  and other large complexes as density classes; granules and condensates; artefact classes (void / outside the
  lamella, ice, carbon edge, gold).
- **Read-outs:** membrane thickness (JCB 2026 ×2), curvature and cristae morphology (Surface Morphometrics,
  EMBO J 2024), contact-site distances (Nat Commun 2025), particle context (Pom: ≈ 300k ribosomes split into
  cytosolic, nuclear, NE- and ER-bound by context vectors; DeePiCt ER/mitochondria ribosomes), protein density
  and spacing on membranes (membrain-stats), filament length, curvature, polarity, network organisation (TARDIS,
  PNAS 2025), vesicle positions and tethers, condensate inside/outside composition, per-tomogram volume census
  (Pom: void ≈ 43 %, cytoplasm 18 %). Distance to the lamella surface matters because FIB damage reaches ≈ 60 nm.
- **Benchmarks:** POPSICLE (arXiv 2606.10255, 2,993 portal tomograms): no architecture wins everywhere,
  segmentation rankings do not carry over to localisation, thin and low-contrast classes defeat every model,
  label noise limits generalisation. Kaggle lessons (Nat Methods 2026): augmentation wins; averaging tolerates
  moderate over-picking. Dice misjudges thin structures (tracET proposes a skeleton Dice).

## 5. Coordinate and unit pitfalls (all observed in the sources)

- **Units:** Å (copick, TARDIS CSV, membrain-pick positions), voxels (Ais pick, tracET, membrain-pick meshes,
  RELION annotations), nm (Surface Morphometrics). pyTME assumes points are voxels and meshes Å.
- **Origin:** corner (Ais, TARDIS, membrain-pick, copick) vs centre (RELION 5). The centre itself differs:
  `size/2 − 1` in RELION's Python tools and pytom's RELION-5 export, `size/2` in RELION's `particles-from-star`
  and crboost's `services/particles/coords.py:72-81`. Verify any new source by a round trip.
- **Grid:** pytom masks must match the tomogram shape exactly; segmenters resample internally (10, 15, 25, 50 Å)
  and write back on the input grid.
- **Headers:** origin never written (Ais, easymode, membrain-seg); voxel size dropped (tracET); silent 10 Å
  fallbacks (easymode apix ≤ 1, Ais pick header 0).
- **Encodings:** full scale 127 (easymode int8), 255 (Ais, easymode uint16), 1 (float32), logits (membrain).
- **Euler conventions:** RELION ZYZ (matrix inverted), Dynamo ZXZ, pytom ZXZ negated, pyTME intrinsic ZYZ; the
  in-plane angle from normals is 0 or random; normal sign (inward/outward) is ambiguous; filament polarity is
  arbitrary unless annotated.
- **Placeholders:** Ais/easymode claim files (10³ volumes) are indistinguishable from outputs by existence alone.

## 6. crboost today against this model

- **Frame:** `services/visualization/tomo_geometry.py` (star, recon MRC, binned dims, binned apix with
  provenance; never invented). Several reconstructions per tilt series since branching; `recon_of`
  (`ui/components/recon_compare.py:46`) walks any job back to its reconstruct instance, and the gallery draws a
  pick layer over the reconstruction its picks were made on (`ui/tomo_gallery.py:158`). Cross-project identity:
  `services/particles/tomo_identity.py`. Imported tomograms are star-only.
- **Registry:** `Tomogram.outputs` keyed by job instance (`services/tilt_series/models.py:180-277`):
  `TsReconstructTomogramOutput`, `DenoisePredictTomogramOutput`. No segmentation output type.
- **Identity:** `ParticleSpecies` (`services/project_state.py:193`) — particles only (templates, masks,
  symmetry, extraction geometry); lab catalog in `services/particles/catalog.py`.
- **Shapes:** points and oriented points as RELION stars, `PickList` (`project_state.py:500`) and particle sets
  with recipes (`services/particles/particle_sets.py`). Volumes: tomograms, TM score maps, templates and masks
  only. No meshes, curves or primitives; the lamella slab fit (`services/analysis/lamella_slab.py`) is the one
  geometric context object.
- **Context:** rule filters (`services/particles/particle_sets.py:93`, applied in `set_ops.py`): σ, LCC, top-N,
  depth band (plane fitted to picks), face distance, nearest-neighbour, include/exclude tomograms, away from
  another set. Analytics records with provenance and fingerprints (`services/particles/records.py`).
- **Pipeline:** `JobSpec` table (`services/jobs/spec.py`), `JobFileType` (`services/io_slots.py:15`, no
  segmentation type), per-TS `ArrayDriver` (`drivers/array_job_base.py`), one job type with a method enum as the
  precedent for interchangeable tools (`DenoiseMethod`, `services/models_base.py:31`). Template matching has no
  tomogram-mask input (`services/jobs/template_match.py:58`; per-tomogram command at
  `drivers/template_match_pytom.py:419`). Refine 3D / Class 3D expose `sigma_ang` only, no tilt/psi priors.
- **Display:** central-slab PNGs (`services/visualization/preview_render.py`) with SVG dot layers per species
  and list; ChimeraX + ArtiaX session (`services/visualization/artiax_bridge.py:426` builds the load commands);
  3dmod hand-off.

## 7. Lab state (2026-10-19)

- Containers under `/groups/klumpe/software/containers/` (with runscripts on PATH): `ais.sif`, `easymode.sif`,
  `pom_cryoet.sif` (all Ais commit `3a1d006`; Pom 1.1.1; shared Ais cache), plus separate `membrain-seg.sif`,
  `saber.sif` (saber-em + sam3 0.1.4, torch 2.7, CUDA 11.8 base — SABER documents CUDA 12), `tardis-em.sif`
  (0.3.10) and `tracet.sif` (2.2.2).
- easymode model cache: the lab-era file layout (`<feature>.h5` / `.json`, `<feature>.scnm` / `_2d.json`), all
  at `apix 10.0` with timestamps (e.g. membrane 2025-11-24, actin 2D 2026-04-06), plus the denoising models.
- The only file in `cache_and_artifacts/segmented/` is a 10×10×10 float volume of −1: an easymode claim
  placeholder from a run that did not finish.
- The one hand-run input on record is a WarpTools reconstruction stored as float16 (MRC mode 12) at 12 Å; the
  name of its segmentation (`…_12.00Apx_f32__actin.mrc`) shows a float32 copy was segmented.

## Sources

- easymode: [preprint](https://doi.org/10.64898/2026.05.19.726344), [docs](https://mgflast.github.io/easymode/),
  [repo](https://github.com/mgflast/easymode), [PyPI](https://pypi.org/project/easymode/),
  [registry.json](https://huggingface.co/mgflast/easymode-v2/raw/main/registry.json),
  [legacy HF commits](https://huggingface.co/mgflast/easymode/commits/main)
- Ais: [eLife 2024](https://elifesciences.org/articles/98552), [repo](https://github.com/bionanopatterning/Ais),
  [CLI reference](https://mgflast.github.io/Ais/cli/reference/), [model repository](https://aiscryoet.org/)
- Pom: [preprint](https://www.biorxiv.org/content/10.1101/2025.01.16.633326v1),
  [repo](https://github.com/bionanopatterning/Pom)
- MemBrain v2: [Nat Methods 2026](https://doi.org/10.1038/s41592-026-03178-8),
  [membrain-seg](https://github.com/teamtomo/membrain-seg), [membrain-pick](https://github.com/CellArchLab/membrain-pick),
  [membrain-stats](https://github.com/CellArchLab/membrain-stats)
- TARDIS: [preprint](https://www.biorxiv.org/content/10.1101/2024.12.19.629196v2),
  [repo](https://github.com/SMLC-NYSBC/TARDIS); tracET: [PLOS One 2025](https://doi.org/10.1371/journal.pone.0338502),
  [repo](https://github.com/PelayoAlvarezBrecht/tracET)
- SABER: [repo](https://github.com/czi-ai/saber), [docs](http://chanzuckerberg.github.io/saber/)
- copick: [data model](https://copick.github.io/copick/datamodel/), [geometry](https://copick.github.io/copick/geometry/),
  [repo](https://github.com/copick/copick), [copick-pipeliner](https://github.com/copick/copick-pipeliner),
  [copick-easymode](https://github.com/copick/copick-easymode)
- CZ portal: [schema](https://github.com/chanzuckerberg/cryoet-data-portal-backend/tree/main/schema/core/v2.0.0),
  [data docs](https://chanzuckerberg.github.io/cryoet-data-portal/stable/cryoet_data_portal_docsite_data.html),
  [CDPO](https://raw.githubusercontent.com/chanzuckerberg/cryoet-data-portal/main/docs/cdpo.md)
- RELION 5: [picking](https://relion.readthedocs.io/en/latest/STA_tutorial/ParticlePicking.html),
  [source](https://github.com/3dem/relion/tree/ver5.0/src/tomography_python_programs)
- pytom-match-pick: [repo](https://github.com/SBC-Utrecht/pytom-match-pick),
  [usage](https://sbc-utrecht.github.io/pytom-match-pick/Usage/)
- Mosaic: [docs](https://kosinskilab.github.io/mosaic/); pyTME: [repo](https://github.com/KosinskiLab/pyTME)
- Dynamo: [models](https://www.dynamo-em.org/w/index.php?title=Model); ArtiaX: [repo](https://github.com/FrangakisLab/ArtiaX)
- Surface Morphometrics: [JCB 2023](https://rupress.org/jcb/article/222/4/e202204093/213869),
  [repo](https://github.com/GrotjahnLab/surface_morphometrics); membrane thickness:
  [JCB 2026](https://rupress.org/jcb/article/225/1/e202504053/278443),
  [JCB 2026](https://rupress.org/jcb/article/225/3/e202505059/278755)
- Pipelines: [ScipionTomo objects](https://github.com/scipion-em/scipion-em-tomo/blob/devel/tomo/objects.py),
  [nextPYP segmentation](https://nextpyp.app/files/pyp/latest/docs/guide/segmentation.html),
  [nextPYP changelog](https://nextpyp.app/files/pyp/latest/docs/changelog.html)
- Benchmarks and reviews: [POPSICLE](https://arxiv.org/abs/2606.10255),
  [Kaggle lessons](https://doi.org/10.1038/s41592-026-03198-4), [COSB 2025](https://doi.org/10.1016/j.sbi.2025.103114),
  [COSB 2026](https://doi.org/10.1016/j.sbi.2026.103319)
- Other methods: [ETSAM](https://www.biorxiv.org/content/10.1101/2025.11.23.689996v3.full),
  [CryoSiam](https://www.biorxiv.org/content/10.1101/2025.11.11.687379v2),
  [CryoViT](https://www.biorxiv.org/content/10.1101/2024.06.26.600701v1),
  [DeePiCt](https://pmc.ncbi.nlm.nih.gov/articles/PMC9911354/)
- OME-Zarr / NGFF: [specifications](https://ngff.openmicroscopy.org/specifications/index.html)

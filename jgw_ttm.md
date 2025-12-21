# Research Plan: GW-Based Template Matching for CryoET

## Part 1: Current Methods Overview

### 1.1 Fourier Cross-Correlation (FCC) Methods

**Core Algorithm:**
The standard approach computes normalized cross-correlation (NCC) between template and tomogram. This is achieved by expressing the cross-correlation in the spatial domain as multiplication in the Fourier domain through the cross-correlation theorem.

**Key Implementations:**

| Tool | Key Features |
|------|--------------|
| **pyTME** | Runs up to ten times faster without loss in accuracy compared to existing software with multiple CPUs and GPUs, enabling template matching of even unbinned cryo-ET data in hours, which was previously nearly impossible due to technical constraints. |
| **pytom-match-pick** | GPU-accelerated, open-source command line interface for enhanced TM in cryo-ET. Using pytom-match-pick, we first quantify the effects of point spread function (PSF) weighting and show that a tilt-weighted PSF outperforms a binary wedge with a single defocus estimate. |

**How FCC handles challenges:**

| Challenge | FCC Solution |
|-----------|--------------|
| **Translations** | FFT convolution theorem: O(N³ log N) instead of O(N⁶) |
| **Rotations** | Exhaustive sampling of SO(3). In application domains such as cryo-electron microscopy, there are required more than ten thousand rotations for achieving an angular precision of a few degrees. |
| **Missing wedge** | PSF weighting, tilt-weighted wedge masks, phase randomization |
| **Low SNR** | Band-pass filtering, Gaussian smoothing, local normalization |
| **Scale** | GPU acceleration, caching Fourier transforms, pre-allocating and sharing arrays in memory across processes as well as using in-place operations |

**Computational bottleneck:** The rotation search. For degree-level precision in 3D, you need ~10,000+ rotations, each requiring a full FFT correlation.

---

### 1.2 Tensorial Template Matching (TTM)

**Core Insight:**
This work uses tensors to encode information over all template rotations, an idea highly related with spherical harmonics. It has been shown that symmetric tensors can be represented using spherical harmonics and vice-versa.

**Key difference from FCC:**
Contrary to standard template matching, the computational complexity of the presented algorithm is independent of the rotation accuracy.

**How it works:**

1. Convert template into a symmetric tensor field (computed once)
2. Compute correlations between tomogram and tensor components
3. Recover position AND rotation analytically from tensor correlations

TTM does not need to explicitly consider multiple concentric shells, so it allows computing each correlation using the fast Fourier transform.

**Complexity comparison:**
- FCC: O(num_rotations × FFT) 
- TTM: O(num_tensor_components × FFT)

For high angular precision, tensor components << rotations needed

**Current limitations:**
- Approximation quality depends on tensor degree
- Due to numerical reasons, high frequencies may be altered during rotation transformation.
- Still relatively new - less battle-tested than FCC

---

### 1.3 Deep Learning Methods

**Three main architectures:**

#### DeepFinder (Semantic Segmentation)
DeepFinder is a computational procedure that uses artificial neural networks to simultaneously localize multiple classes of macromolecules. Once trained, the inference stage of DeepFinder is faster than template matching.

Architecture: 3D CNN for voxel-wise segmentation
Training: Requires manually annotated tomograms

#### TomoTwin (Metric Learning / Embeddings)
By embedding tomograms in an information-rich, high-dimensional space that separates macromolecules according to their three-dimensional structure, TomoTwin allows users to identify proteins in tomograms de novo without manually creating training data or retraining the network each time a new protein is to be located.

Architecture: 3D CNN embedding into 32-dim space, triplet loss training
Key advantage: Pre-trained general model, no per-protein retraining needed

#### Common Blindspots of DL Methods

| Limitation | Details |
|------------|---------|
| **No rotation output** | Unlike TM and TTM, none is able to determine the rotations for every detected instance. |
| **Training data hungry** | These methods require extensive annotations for training and are less effective in detecting low-abundance particles. |
| **Domain shift** | Models trained on one dataset often fail on different sample types |
| **Small particles** | Struggle with proteins < 100-200 kDa due to low contrast |

---

## Part 2: Computational Challenges Summary

| Challenge | FCC Solution | TTM Solution | DL Solution | GW Potential |
|-----------|--------------|--------------|-------------|--------------|
| **6-DOF search** | Exhaustive rotation sampling | Tensor encoding (rotation-free) | Learned features | **Intrinsically rotation-invariant** |
| **Missing wedge** | PSF weighting, phase randomization | Same as FCC | Learned implicitly | Could modify distance metric |
| **Low SNR** | Filtering, normalization | Same as FCC | Learned denoising | Point cloud sparsification filters noise |
| **Crowded environment** | Template-specific masks | Same as FCC | Multi-class segmentation | Partial matching handles occlusion |
| **Computational cost** | GPU parallelization | Rotation-free = faster | Fast inference after training | **Unknown - needs prototyping** |
| **Partial matches** | Not well supported | Not well supported | Not well supported | **Native capability (UGW/JGW)** |

---

## Part 3: EMPOT/JGW Adaptation Strategy

### 3.1 What EMPOT/JGW Bring to the Table

From the papers, the key advantages are:

1. **Rotation invariance built-in**: GW uses pairwise distances within each point cloud, which are invariant to rigid transformations. No rotation sampling needed.

2. **Partial matching native**: UGW explicitly handles mass imbalance. JGW extends this to multiple clusters simultaneously.

3. **Noise robustness**: The JGW spiral experiment showed 99.1% correct mass transport vs 51-67% for competitors.

4. **Multi-object simultaneous matching**: JGW can match {template1, template2, template3} to tomogram in one shot.

5. **Speed for multi-chain**: JGW was 7x faster than sequential UGW (805s vs 5680s for PDB:1I3Q).

### 3.2 Challenges for CryoET Adaptation

**Scale Problem:**
- EMPOT used 500-2000 points
- Tomograms are 500³-1000³ voxels = 125M-1B voxels
- Even sparse point clouds would be 10k-100k points
- GW has O(n²) memory and O(n³) time complexity

**Multiple Instance Problem:**
- Template matching finds MANY copies of the same protein
- EMPOT/JGW find ONE best alignment
- Need to either: (a) tile the tomogram, or (b) fundamentally change the approach

**Rotation Recovery:**
- GW gives you the coupling matrix, not directly R and T
- EMPOT uses Kabsch algorithm on correspondences
- Works for single instances, unclear for multiple

### 3.3 Proposed Architecture: Hierarchical GW Matching

```
┌─────────────────────────────────────────────────────────────┐
│                    PROPOSED PIPELINE                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. COARSE LOCALIZATION (Rotation-invariant features)       │
│     ├─ Option A: Low-res TTM tensor correlation             │
│     ├─ Option B: GW-derived rotation-invariant descriptors  │
│     └─ Output: Candidate regions + rough positions          │
│                                                             │
│  2. CANDIDATE EXTRACTION                                    │
│     ├─ Extract subtomograms at candidate positions          │
│     ├─ Convert to sparse point clouds (TRN or thresholding) │
│     └─ Typical size: 500-2000 points per subtomogram        │
│                                                             │
│  3. JGW REFINEMENT                                          │
│     ├─ Input: {template point cloud} vs {candidate clouds}  │
│     ├─ JGW finds best partial matches simultaneously        │
│     ├─ Reject false positives via transport plan sparsity   │
│     └─ Output: Refined positions + coupling matrices        │
│                                                             │
│  4. ROTATION RECOVERY                                       │
│     ├─ Extract correspondences from transport plan          │
│     ├─ Kabsch algorithm per candidate                       │
│     └─ Output: Full 6-DOF pose per particle                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 3.4 Specific Research Directions

#### Direction A: GW as Rotation-Invariant Descriptor

**Idea:** Use GW distance itself as a rotation-invariant similarity score for coarse picking.

**Implementation:**
1. Precompute template's internal distance matrix D_template
2. For each position in tomogram, extract local patch → point cloud → distance matrix D_local  
3. Compute lightweight GW approximation (e.g., linear lower bound from Mémoli 2011)
4. Threshold on GW distance for candidate selection

**Pros:** Truly rotation-free coarse search
**Cons:** Still O(n²) per position; needs aggressive downsampling

#### Direction B: JGW for Multi-Template Simultaneous Matching

**Idea:** Match multiple different templates to a tomogram region simultaneously using JGW's cluster structure.

**Application scenario:** You know ribosomes, proteasomes, and fatty acid synthases are in the tomogram. Instead of three separate searches, do one JGW optimization with 3 source clusters.

**Implementation:**
1. Source: {ribosome_cloud, proteasome_cloud, FAS_cloud} with appropriate mass weights
2. Target: Tomogram region as single point cloud
3. JGW naturally assigns which source cluster maps to which target region
4. Extract per-cluster correspondences → Kabsch for each

**Pros:** 
- Natural handling of crowded environments
- Built-in discrimination between similar structures
- Potential speedup over sequential matching

**Cons:**
- Still needs candidate extraction first (can't do whole tomogram)
- Scaling to many templates unclear

#### Direction C: TTM + GW Hybrid

**Idea:** Use TTM's tensor representation for coarse localization, GW for refinement and partial matching.

**Why this might work:**
- TTM gives you fast rotation-free candidate detection
- But TTM assumes complete templates; fails for partial/fragmented structures
- GW refinement could handle partial matches that TTM misses

**Implementation:**
1. Run TTM at coarse resolution → candidate positions
2. For each candidate, extract subtomogram
3. Run JGW matching (template clusters vs subtomogram)
4. Accept candidates where JGW transport plan shows good structure

**Potential advantage:** Could find partial/occluded particles that TTM alone misses

#### Direction D: Modified Distance Metric for Missing Wedge

**Idea:** The missing wedge creates anisotropic resolution - distances along Z are less reliable. Modify the GW distance metric to account for this.

**Implementation options:**
1. **Weighted Euclidean:** d(a,b) = sqrt(w_x(a_x-b_x)² + w_y(a_y-b_y)² + w_z(a_z-b_z)²) with w_z < w_x, w_y
2. **Learned metric:** Train a small network to predict "true" distances from missing-wedge-corrupted distances
3. **Fourier-space weighting:** Sample point clouds preferentially from well-resolved directions

---

## Part 4: Concrete Prototyping Plan

### Phase 1: Feasibility (2-4 weeks)

**Goal:** Determine if GW-based matching can achieve comparable accuracy to FCC on simple cases.

**Experiments:**
1. Take SHREC benchmark tomograms (simulated, ground truth available)
2. Run standard pytom-match-pick as baseline
3. Implement naive GW matching:
   - Extract ground truth subtomograms
   - Convert to point clouds
   - Compute JGW between template and subtomograms
   - Measure: accuracy, runtime, memory

**Key metrics:**
- Precision/recall vs FCC baseline
- Runtime scaling with point cloud size
- Memory footprint

### Phase 2: Scale Solutions (4-8 weeks)

**Goal:** Make GW matching tractable for realistic tomogram sizes.

**Approaches to try:**
1. **Hierarchical coarse-to-fine:** Low-res GW for candidates, high-res for refinement
2. **Sliced GW:** Use 1D projections to approximate full GW (much faster)
3. **Sparse transport plans:** Exploit sparsity in entropic-regularized solutions
4. **GPU implementation:** Port core GW computation to CUDA

### Phase 3: Novel Capabilities (8-12 weeks)

**Goal:** Demonstrate advantages over FCC that justify the new approach.

**Target applications:**
1. **Partial particle detection:** Find incomplete/fragmented structures
2. **Multi-template discrimination:** Simultaneous matching in crowded regions  
3. **Small particle detection:** Test on <200 kDa targets where FCC struggles

### Phase 4: Integration (12-16 weeks)

**Goal:** Package as usable tool, benchmark comprehensively.

**Deliverables:**
- Python package with clean API
- Integration with common formats (MRC, STAR)
- Comprehensive benchmarks on public datasets
- Paper draft

---

## Part 5: Key Technical Questions to Resolve

1. **Point cloud generation:** TRN (as in EMPOT) vs intensity thresholding vs learned downsampling?

2. **GW solver choice:** Entropic regularization (smooth but diffuse) vs Frank-Wolfe (sparse but slower)?

3. **Mass distribution:** Uniform weights or intensity-weighted? How does this affect partial matching?

4. **Missing wedge handling:** Pre-correct tomogram, modify metric, or learn to ignore?

5. **Multi-instance handling:** Tile tomogram, iterative subtraction, or fundamentally new formulation?

---

## Summary: Why This Could Work

| Strength | Exploitation Strategy |
|----------|----------------------|
| Rotation invariance | Skip exhaustive rotation sampling entirely |
| Partial matching | Find fragmented/occluded particles missed by FCC |
| Multi-object (JGW) | Simultaneous multi-template search |
| Noise robustness | Better discrimination in crowded environments |

| Weakness | Mitigation Strategy |
|----------|---------------------|
| O(n²/n³) complexity | Hierarchical approach, sliced approximations |
| No direct rotation output | Kabsch on transport correspondences |
| New/unproven for this domain | Careful benchmarking against established baselines |

The most promising initial direction is probably **Direction C (TTM + GW Hybrid)** because it leverages the proven speed of TTM for coarse localization while using GW's unique partial matching capability for refinement and edge cases.

Would you like me to dive deeper into any of these directions, or sketch out actual code for the feasibility prototype?
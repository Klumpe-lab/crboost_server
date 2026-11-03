---
name: i1-angle-list-reduction-underperforms-by-15x
description: "services/templating/angle_lists.py:_reduce_to_asymmetric_unit produces ~3398 angles for I1@12° (expected ~225); search is correct but ~15× slower than advertised \"60× speedup\""
metadata: 
  node_type: memory
  type: project
  originSessionId: 5c809c59-075f-4d98-8bbf-40bcfe427892
---

The asymmetric-unit angle-list generator in `services/templating/angle_lists.py` is **conceptually correct but produces a 15× oversampled file** for icosahedral / non-cyclic point groups.

**Concrete numbers from the 2026-05-19 `agg_20260311_412_Grid3` run with I1@12°:**
- Uniform Euler grid: 30 × 15 × 30 = 13500 angles
- Expected after I-coset dedup: 13500 / |I| = 13500 / 60 = **225**
- Actual file written (`External/job014/angles_I1.txt`): **3398**
- PyTOM evaluated 3398 angles per TS → ~1:35 per TS instead of the ~6s a true 225-angle file would give. ~15× slowdown vs the advertised 60× speedup. Effective speedup against C1: ~4×.

**Why:** `_reduce_to_asymmetric_unit` canonicalizes each grid point's full G-orbit to a lex-min quaternion and dedupes by that fingerprint. Two grid points are in the same coset iff R₂·R₁⁻¹ ∈ G *exactly* — and on a uniform Euler grid that condition essentially never holds (the icosahedral group doesn't preserve the grid except for accidental axis alignments like the 5-fold around z giving 72° = 6×12° matches). So most "geometrically equivalent" grid points have slightly different orbit point-sets and fail to dedupe.

**Correctness impact:** none. PyTOM's CC at any orientation in the file is a valid score. Oversampling just wastes GPU cycles; picks are unaffected.

**Why: design choice was to generate a coset-rep file because PyTOM has no built-in icosahedral flag — `--z-axis-rotational-symmetry` only handles Cn, and the file IS the symmetry for D/T/O/I. The intent (file = one rep per coset) is right; the dedup implementation is wrong.

**How to apply:** if the user complains about TM runtime for I1/T/O templates, or if you're touching the angle-list code, fix it with one of:
1. Quaternion-distance-based dedup with tolerance + kd-tree (O(N log N), correct).
2. Sample directly in a fundamental domain of SO(3)/G (mathematically cleaner but more code).

PyTOM internal reference for angle-list format (from raw `src/pytom_tm/angles.py`): ZXZ Euler triples in radians, lex-sorted by (Z1, X, Z2), one per line. Our `write_angle_list_file` preserves the meshgrid's "ij" iteration order which is already (Z1, X, Z2)-sorted; PyTOM's `load_angle_list` would also re-sort defensively.

User on 2026-05-19 chose not to fix yet — preferred to just re-run with the correct cc_threshold (the actual 0-picks cause that triggered this investigation). Bookmark for later: see [[project_412_picking_quantification]].

**Coverage verified 2026-10-04:** the 3398-angle list (agg_20251113_412/External/job015/angles_I1.txt) composed
with the 60 icosahedral rotations covers SO(3) within max 6.4° (median ~2.5°) for both k·S and S·k and both
2-folds-on-xyz embeddings (I1/I2). The dedup only collapses the D2 subgroup (13500/4 ≈ 3398) — slow, not lossy.

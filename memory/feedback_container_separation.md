---
name: Separate containers when pinnings differ
description: Guidance on when to split vs bundle multiple tools into Apptainer containers — user prefers separation when Python/TF/CUDA pinnings diverge, to enable independent upgrade cycles
type: feedback
originSessionId: 5f30a0d5-2f00-44ab-887e-d4dfe083d5e0
---
When packaging multiple cryo-ET tools into Apptainer containers for this project, build **one container per tool** whenever their Python / TensorFlow / PyTorch / CUDA / cuDNN pinnings differ — do not bundle them into a single image with multiple conda envs just because they're functionally related.

**Why:** The user explicitly overrode a proposed bundled container (`easymode_ais.sif` with two conda envs) in favor of separate images. Reasoning:
1. Tools with different envelopes will diverge further over time — bundling couples their release cadences.
2. Rebuilding one shouldn't force rebuilding the other (HPC container builds are slow and flaky).
3. Isolated build failures: upstream breakage in one tool doesn't block the others.
4. Smaller per-image footprints.

**How to apply:** Before proposing a bundled container, check each tool's documented Python/TF/CUDA pinning. If any diverge, default to one-tool-per-container. Only bundle when the tools genuinely share the same envelope AND have coupled release cycles (e.g., AreTomo compiled against a specific Warp build). Even then, flag the trade-off — user would rather pay the container-count tax than the upgrade-coupling tax.

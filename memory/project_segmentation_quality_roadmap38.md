---
name: project-segmentation-quality-roadmap38
description: Roadmap 38 BUILT 2026-10-20, on main by 2026-10-24; first run = MyrGFP_Grid2 job039 (2026-10-22) — Segment job resamples to 10 Å (spectrum crop) + maps back, TTA default/max 8, Z smoothing of 2.5D nets
metadata:
  type: project
---

`docs/roadmaps/roadmap_38-segmentation-quality.md`. New `services/segmentation/grid.py` (processing_shape,
write_resampled = scipy.fft spectrum crop, origin-aligned; finish_volume = smooth Z (FWHM = net's X/Y pixel from
.scnm `apix`, only when `arch` lacks "3d") → map_back (affine_transform order 1, inverse sampling) → re-encode in the
tool's dtype, atomic). Driver: tools write to `.staging/task_<ts>/out/`, finals to `segmentations/`; easymode gets
only the features segmentations/ lacks; settings `segment_at`/`smooth_z` + target `plane_angstrom` are in the
re-run identity (old job038 manifest → full recompute). Params `match_training_voxel`, `smooth_z`; TTA le=8 (Ais
lists hold 8 → 9–16 crashed). Template segment time 1 h → 2 h; user's config/conf.yaml still says 1 h.

**Why:** first run (job038, 6.2 Å) banded along Z and blocked in XY; Ais resamples XY only and global_mad works on raw
voxels (0.72× contrast at 50 Å vs 12.4 Å bin) — see [[project-segmentations-roadmap37]].
**How to apply:** runtime check §5 of the roadmap (new Segment job for A/B vs job038); if bands persist, add a
factor on the smoothing FWHM, not fixed Å. Ais-model method is never resampled (training voxel unknown).

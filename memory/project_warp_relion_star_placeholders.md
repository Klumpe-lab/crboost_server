---
name: WarpTools RELION star export has placeholder values for motion/CTF-fit metrics
description: WarpTools-emitted RELION star files write 1e-6 / None for AccumMotion, CtfMaxResolution, CtfFigureOfMerit even when the underlying values exist
type: project
originSessionId: 90bd9c4a-9d17-41fe-875c-80baba426451
---
WarpTools-emitted RELION-format per-tilt stars (e.g. `External/job002/tilt_series/<TS>.star`) have **real** values for:
- `rlnDefocusU`, `rlnDefocusV` (Å)
- `rlnCtfAstigmatism`, `rlnDefocusAngle`
- `rlnTomoNominalStageTiltAngle`, `rlnMicrographPreExposure`
- (post-alignment) `rlnTomoXTilt/YTilt/ZRot`, `rlnTomoXShiftAngst/YShiftAngst`

But these columns are written with **placeholder `0.000001` (or `None`)** even when WarpTools internally has the values:
- `rlnAccumMotionTotal / Early / Late`
- `rlnCtfMaxResolution`
- `rlnCtfFigureOfMerit`

**Why:** WarpTools tracks motion / CTF fit quality in its own XML files (`warp_frameseries/*.xml`), not in the RELION star export. The export carries only the fields downstream RELION jobs need.

**How to apply:** For dashboard plots that depend on these fields, gate on a magnitude threshold (~1e-3) and skip the plot with a note when values are placeholder. To get the real motion/CTF-fit values we'd need to parse WarpTools' XML metadata — defer until requested.

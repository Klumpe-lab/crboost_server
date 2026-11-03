---
name: Don't render cryo-ET volumes in the browser
description: For pick / particle sanity-check UIs in CryoBoost, prefer star-file metadata plots over any 2D projection of the tomogram volume.
type: feedback
originSessionId: 8bb3142b-c215-446c-a10e-3493578913ec
---
Do not build browser-side volumetric previews (Z-MIP / orthoslab triptych / per-pick stamp cutouts) for cryo-ET pick inspection.

**Why:**
1. **Missing-wedge streak artifacts dominate any 2D projection.** Cryo-ET reconstructions have Fourier-space gaps at high tilt. A Z-MIP through a 200-slice slab has bright reconstruction streaks at +0.117 while a real ribosome contributes +0.05 to the projection — the streaks always win, particles are mathematically invisible. The "white streaks across the slices" the user reported in 2026-05 was exactly this; not a tuning issue, a fundamental property of cryo-ET data.
2. **Per-pick 2D cutouts also fail** — single Z-slice cutouts are pure noise; the only way to see particles is to scrub through Z, which only a real volumetric viewer (3dmod, napari, neuroglancer) can do.
3. **Performance ceiling** — reading a 2 GB float32 tomogram from Lustre is ~30s no matter what. With 100s of tomograms per project, any per-tomo volume read is unscalable.
4. **The field doesn't do this.** crYOLO, Topaz, RELION 4 all skip volumetric previews and lean on 3dmod for visual inspection. UI shows score/spatial stats only.

**How to apply:**
- For any "show me the picks for this tomogram" feature: render small PNGs of star-file-derived stats (score histogram, X/Y pick scatter, X/Z pick scatter, pick-count badge). matplotlib Agg backend, ~50 ms per plot, no volume reads.
- For "look at this specific pick" → hand off to 3dmod with the .mod overlay. The existing copy-command UX is the right primitive; just make it smooth.
- Volumetric viewing is a separate, deferred concern — Neuroglancer + OME-Zarr is the path if we ever need it, not browser-side MIPs.
- This rule is for the *picking-sanity-check* UI specifically. Tilt-image thumbnails are still fine (those are 2D micrographs, not volumes).

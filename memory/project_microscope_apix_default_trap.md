---
name: project_microscope_apix_default_trap
description: "MicroscopeParams.pixel_size_angstrom defaults to 1.35 (ge=0.5, never 0) — the `... or 0.0` \"unset\" idiom is broken; for particle-only projects it's always the garbage default"
metadata: 
  node_type: memory
  type: project
  originSessionId: b3df5f19-e3ea-49c7-b7a6-a26be8fac4c9
---

`MicroscopeParams.pixel_size_angstrom` (services/models_base.py:~118) is `Field(default=1.35, ge=0.5, le=10.0)`.
Because of `ge=0.5` it can **never legitimately be 0**, so the common idiom
`float(getattr(state.microscope, "pixel_size_angstrom", 0.0) or 0.0)` (expecting `0.0` to mean "unset") is
**broken** — it always returns a positive number. For a **particle-only / data-less project** the microscope
apix is never populated (no mdocs/frames to detect it; `project_service` skips the detection branches), so it
**stays at the model default 1.35** — a meaningless value for an arbitrary imported recon.

**Why it bites:** any code that prefills/uses this value as if it were real will silently mis-scale. Concretely,
the tomogram-import dialog used to prefill the apix-override field from it (1.35), and that 1.35 then **won over
each recon MRC's real header voxel size** in `tomogram_import._synthesize_rows` (`if pixel_size_angstrom > 0: ts_px = override` beats `recon_apix/binning`), so every imported tomogram got apix=1.35 — the exact apix→default
anti-pattern the working directive forbids. Fixed 2026-06-25 by leaving the override field **blank by default**.

**How to apply:** never treat `microscope.pixel_size_angstrom` as "0 ⇒ unset". The recon MRC header voxel size is
authoritative for imported tomograms; the project apix is only a real signal when it was actually detected
(standard projects with mdocs). When you need a true override, default the input BLANK and let a present header
win; surface a genuinely-missing header (warn chip + raise), never silently default. See
[[project_pixel_arithmetic_pain_point]], [[feedback_never_silent_defaults]], [[project_particle_project_roadmap]].

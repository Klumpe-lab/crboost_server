---
name: project_tilt_metrics_wave_2026-08-27
description: "2026-08-27 Journey per-tilt charts wave — registry schema 1.4 (motion tracks, defocus spread, intensity/FOV/masked, TS fit-res/plane normal), warp_curves.py PS1D-vs-CTF² panel with tilt scrubber, tilt image in every hover card, thumbnails on fsMotion completion, crboost_reingest.py; PENDING RUNTIME"
metadata: 
  node_type: memory
  type: project
  originSessionId: 83cdcacf-fca0-455a-9066-492e677a0300
  modified: 2026-08-27T17:38:02.008Z
---

**Ask (2026-08-27):** "what other graphs can we add to Journey that Warp/AreTomo log" (from
`docs/preprocessing-metrics-inventory.md`) + "hover a dot → show the tilt's image if fsMotion ran".
No back-compat wanted. Code-complete, ruff-clean, **NOT runtime-verified** (no python in the sandbox).

**Landed (status block in `docs/roadmaps/roadmap_preprocessing-metrics.md` is the doc of record):**
- Registry **schema 1.4** — adapters ingest: fsMotion `defocus_spread_um` + `motion_track_x/y/source`
  (Warp's `average/<frame>_motion.json` single cell, else XML `<GridMovementX/Y>` nodes); alignment per-frame
  `average_intensity`/`masked_fraction` (tomostar) + `fov_fraction` (per-TS XML, joined by `<MoviePath>`);
  tsCtf `ctf_resolution` + `plane_normal` (XML root). Shared helpers `pos_float/opt_float/xml_text_lines`
  in `adapters/_base.py`.
- `services/tilt_series/warp_curves.py` — PS1D/SimulatedScale (+ `TiltPS1D` per Z) reader, memoized; CTF²
  model from the XML's own `<CTF>` (λ relativistic, χ = πλf²Δz − ½πCsλ³f⁴ + φ, PhaseShift in units of π);
  `normalized_overlay` = (PS1D−bg)/scale vs CTF². Zeros checked against try2 data (first 3 CTF zeros match
  the PS1D dips within a bin).
- Journey: `_CtfFitPanel` (slider + readout; `select_frame` hook), `_plot_cell(on_select=)` wires
  `on_point_click` + `chart:mouseover` (throttle 0.15 s, `args=["data"]`, frame at data slot 3) →
  hover/click on any chart in the section moves the fit panel. Hover cards: customdata is now ALWAYS
  `[idx, frame, verdict, thumb]` (slots d[2..5] in the JS); image via `/api/tilt-thumb` from
  `TiltFilter/png/<frame_id>.png`; tooltip `appendToBody` + custom `:position` JS (card taller than the tile).
  Alignment "X tilt − nominal" chart was bogus (X tilt ≡ 0 → −nominal) → replaced by Tilt offset
  (refined − nominal, refined = −rlnTomoYTilt). CTF-FoM chart + star-fallback branches deleted.
- Thumbnails auto-kick moved tsCtf → **fsMotionAndCtf** completion (`pipeline_runner._kickoff_tilt_thumbnails`,
  reads `fs_motion_and_ctf.star`).
- `crboost_reingest.py <project> [--job …]` — re-runs the three adapters' `ingest()` over `.task_status/*.ok`
  items of SUCCEEDED jobs, saves the registry; the way existing projects get the new fields without GPU time.

**Runtime checklist owed:** restart server; run reingest on a finished project; Journey → Motion & CTF shows
Motion trajectories / Defocus spread / CTF fit (slider moves, hover on Defocus dots moves it); Alignment shows
Tilt offset / Mean tilt intensity / FOV; CTF-after-alignment tiles + its CTF fit; hover card shows the tilt
image (needs PNGs — Generate thumbnails in the tilt filter if the project predates the new trigger). Risk
points if something is dead: `chart:mouseover` event args, `:position` JS, `appendToBody`, slider
`on_change` re-entry (guarded by `_set` pos check).

**Not built:** AreTomo patch residuals (global mode logs none), AxisOffsetX/Y (semantics unconfirmed), ice
thickness (not fit), mdoc acquisition metadata charts (in registry since 1.2, still un-plotted), ⑤ curation
scatter. Relates to [[project_preproc_metrics_inventory]], [[feedback_dashboard_plot_principles]],
[[project_ui_pass_2026-08-27]].

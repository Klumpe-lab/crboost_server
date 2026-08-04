# Preprocessing metrics — roadmap & handoff

Companion to **`docs/preprocessing-metrics-inventory.md`** (the grounded catalog of every readout).
This is the *forward* plan: what's built, what's next, the open questions, and the data/env gaps that
block full validation. Written as a cold-start handoff — a fresh session should be able to pick up from
here without prior context.

**Goal (user, 2026-07-01):** make the journey view more informative *and* decluttered — surface the rich
per-tilt/per-TS metrics we already produce, explain each one, plot them (CryoSPARC-style where they
transpose), and eventually overlay them on the tilt-filter previews. The user is a cryo-ET domain expert
who explicitly wanted honesty about what each number means and what's real vs placeholder.

---

## Status

| # | Item | State |
|---|---|---|
| — | **Inventory doc** — every readout, meaning, tooltip, real-vs-placeholder, viz | ✅ landed (`docs/preprocessing-metrics-inventory.md`) |
| ① | **Fix 3 dead panels** — CTF-res + motion sourced from frameseries XML, not placeholders | ✅ code-clean (ruff E+F), **runtime-pending** |
| ② | **Tilt-filter preview overlay** — real CTF-res + motion chips (replaced the `0.0px` placeholder) | ✅ code-clean, **runtime-pending** |
| ③ | Defocus-vs-tilt + shift-magnitude panel | ✅ code-clean (ruff E+F, format), **runtime-pending** (2026-08-03) |
| ④ | PS1D CTF-fit overlay + tilt scrubber | ⬜ next |
| ⑤ | Exposure-curation scatter across tilt-series (the declutter centerpiece) | ⬜ (needs multi-TS data) |

**Landed code (① + ②):**
- `services/tilt_series/frameseries_quality.py` *(new, pandas-free)* — `read_frame_quality` / `quality_series`,
  memoized by mtime; parses `<Movie>` root attrs `CTFResolutionEstimate` + `MeanFrameMovement`.
- `ui/tomo_dashboard_dialog.py` — `_render_ctf_motion_plots(..., frameseries_dir=)`; FS-motion CTF-res +
  motion panels and stat strip now read real XML values. `ts_ctf` call left unchanged.
- `ui/tilt_filter_panel.py` — `_find_fs_motion_warp_dir` + df enrich `_xmlRes`/`_xmlMotion` → card chips.

> **Runtime-pending means:** verified only by `ruff check` (E+F). There is **no Python interpreter in the
> assistant sandbox** (the `venv/bin/python3` symlink points at an unmounted cluster path) and **the server
> does not auto-reload** — the user must restart `python main.py` + hard-reload the browser to see ①/②, and
> should eyeball that the FS-motion CTF-res/motion panels now render (were blank) and the tilt-filter cards
> show `Å` + motion chips instead of `0.0px`.

---

## Next build items (detail)

### ③ Defocus-vs-tilt + shift-magnitude panel  ✅ *BUILT 2026-08-03 (code-clean, runtime-pending)*
A tomo-native panel with no SPA analog. New per-TS section **"Tilt QC"** (panel key `tilt_qc`, toggleable,
renders after TS-CTF and before Reconstruct) in `ui/tomo_dashboard_dialog.py`:
- **Defocus vs tilt angle** — mean per-tilt defocus `((U+V)/2)` from the per-tilt star, **sorted by tilt
  angle** so the through-focus trend reads as a curve, with a **numpy-free OLS fit line** (dotted). The fit
  **slope sign is surfaced in the stat strip** (`defocus trend +x.xxx µm/° (rises/falls with +tilt)`) as the
  handedness cue (ties to the 412 / TomoHand issue). Source preference: **tsCTF per-tilt star → fsMotion**
  fallback (`_defocus_source_df`). *Reads the per-tilt star directly (already tilt-angle-paired per row), so
  the GridCTF filename-join caveat doesn't apply — no new plumbing.*
- **Shift magnitude vs tilt** — `√(rlnTomoXShiftAngst² + YShiftAngst²)` from the alignment per-tilt star;
  per-TS **max / median** in the stat strip as the headline alignment-difficulty number.
- No-op (returns False) when neither CTF nor alignment has run for the TS. Helpers: `_linear_slope_intercept`,
  `_defocus_source_df`, `_render_tilt_qc_section`; hint `_HINT_THROUGHFOCUS`.
- **Runtime-pending:** user restarts `python main.py` + hard-reloads; eyeball the Tilt QC panel on a TS that
  has both CTF + alignment (e.g. `try2_after_pixShift`), confirm the through-focus curve + slope readout and
  the shift plot render, and that the `Tilt-QC` checkbox in the Panels row toggles it.

### ④ PS1D CTF-fit overlay + tilt scrubber  *(needs a curve parser)*
CryoSPARC's signature diagnostic. Extend `frameseries_quality.py` (or a sibling) to parse the `<PS1D>`,
`<SimulatedBackground>`, `<SimulatedScale>` text curves (frameseries) and/or `<TiltPS1D>` +
`<TiltSimulatedScale>` (tiltseries, per-tilt). Plot measured (black) vs model (red) vs cross-correlation
(cyan) with a green line at the fit resolution; default to the 0° tilt with a tilt slider. This is the
highest trust-building plot but the heaviest (curve parsing + a scrubber control).

### ⑤ Exposure-curation scatter across tilt-series  *(the declutter/cull centerpiece — needs multi-TS data)*
CryoSPARC's "Manually Curate Exposures", transposed to **one point per tilt-series**. Scatter with
dropdown X/Y axes over per-TS aggregates (median `CTFResolutionEstimate`, mean `DefocusU`, mean
`MeanFrameMovement`, `AverageIntensity`/ice proxy, tilt count, index), low/high threshold sliders, split
accepted/rejected histograms, linked table. Build axis-agnostic so any metric plugs in. **This is the
"10 projects → cull → aggregate" flow.** Requires per-TS aggregation across a dataset — validate on a
multi-TS project first (see data gap below).

---

## Structural / correctness follow-ups (independent of ③–⑤)

1. **Adapter-ingest of the real values (make the star honest).** ①/② read the XML at *render* time so
   existing runs light up. The permanent fix is to also write `CTFResolutionEstimate` → `rlnCtfMaxResolution`
   and `MeanFrameMovement` → `rlnAccumMotionTotal` in `services/tilt_series/adapters/fs_motion_ctf.py`
   `_build_frame_output` + `_apply_motion_ctf_to_tilt_df` (parse the `<Movie>` root attrs alongside the
   existing `<CTF>` parse). Then downstream consumers + future runs are honest, not just the dashboard.
   Caveat: existing projects need a re-emit to backfill. Keep the render-time reader as the fallback.
2. **`rlnTomoHand` +1 TEMP-DEBUG override** in `drivers/template_match_pytom.py:154-158` — picking uses +1
   regardless of the stored handedness; `job.json defocus_handedness=0` is a separate knob. Reconcile +
   surface a handedness panel (412-critical — see `project_412_tomohand_discrepancy`).
3. **Motion units (§8 Q1)** — `MeanFrameMovement` is unlabeled; Warp convention is Å. Currently the axis is
   labeled without asserting a unit. Confirm px-vs-Å with the user, then lock the label + any threshold.
4. **ts_ctf per-TS CTF-res badge** — the tsCTF section's CTF-res panel is still empty (no per-tilt value
   exists there). Add the per-TS `TiltSeries@CTFResolutionEstimate` scalar as a header badge from the
   `warp_tiltseries` XML.
5. **Denoise convergence readout** — capture the cryoCARE Keras train/val loss history (currently stdout-only)
   into the job dir and surface a loss-vs-epoch plot. No SNR/PSNR metric exists today.
6. **Tilt-filter: more overlays** — dose, refined shift, and a per-TS "CTF-res / motion vs tilt" sparkline
   on the group header would make the filter decision better-informed.

---

## Open questions for the domain expert (carried from inventory §8)

1. `MeanFrameMovement` / `GridMovement` units — pixels or Ångström? (blocks a motion QC threshold)
2. `SimulatedBackground` is all-zero in the sampled run — pre-subtracted, not modeled, or a bug?
3. `FOVFraction` meaning — usable field-of-view fraction? (non-monotonic; min near 0° tilt)
4. `AxisOffsetX/Y` semantics/units (large, erratic per tilt)
5. Per-tilt vs per-TS astigmatism — enable per-tilt astig fitting? (constant per TS in the sampled run)
6. Ice/sample thickness fitting — enable it in `ts_ctf`? (`<CTF> Thickness` field exists but = 0)
7. Alignment residual — enable AreTomo patch tracking (`patch_x/patch_y > 0`) for a true per-tilt error?
   (global mode emits none; only shift magnitude is available)
8. `rlnTomoHand` override reconciliation (see follow-up #2)

---

## ⚠️ Data & environment access gaps (what blocked full validation)

**`/groups/klumpe/crboost_data` is NOT accessible from the assistant sandbox.** `config/conf.yaml`
`DefaultProjectBase` points there, but `/groups/klumpe/` doesn't exist in this environment
(`ls: cannot access '/groups/klumpe/': No such file or directory`). **I could not enumerate or read a
single project in `crboost_data`** — I don't know which datasets live there. The user's request to "pick a
project from `/groups/klumpe/crboost_data`" could not be honored directly.

**What I used instead** — the completed runs checked into the repo under `projects/`:
- `try2_after_pixShift` — primary ground truth (single TS `Position_11_2`, 39 tilts, WarpTools + AreTomo,
  full job chain job002–008). Most inventory samples are from here.
- `pos9_10_after_pixShift` — secondary (has `TiltFilter/`, `tilt_filter_thumbnails/`, Class3D/Refine3D);
  used for the tilt-filter findings.
- `demo`, `newp`, `3dclass_aln_pixShiftBug` — present but not deeply sampled.

**Why this matters — what a multi-TS `crboost_data` project would unblock:**
- ⑤ exposure-curation scatter needs **cross-TS aggregates/distributions** (max-shift spread, resolution
  ranking, handedness spread) — un-observable from one tilt-series.
- Confirm whether **astigmatism** and **defocus angle** are ever truly per-tilt (constant in `try2`).
- Confirm the **placeholder trap** and **motion units** hold on a second, independently-acquired dataset.
- Observe a run where **thickness fitting** / **patch tracking** were enabled (both off in `try2`).

**Action for next session:** ask the user to either (a) confirm a mounted path the assistant *can* read
(the local `projects/` copies, or a bind-mount of a `crboost_data` project), or (b) run a listing of
`crboost_data` and point at 1–2 multi-TS projects to copy/symlink somewhere readable. Until then, ③/④ can
proceed on `try2`/`pos9_10`; ⑤ should wait for multi-TS ground truth.

**Sandbox env:** no working Python interpreter (`venv/bin/python3` → unmounted cluster path); `ruff` (E+F)
is the static-check ceiling. Server has no auto-reload — the user runs `python main.py` to runtime-verify.
See `reference_hpc_env` / `reference_crboost_data_access` memories.

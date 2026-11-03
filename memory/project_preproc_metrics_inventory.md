---
name: project_preproc_metrics_inventory
description: Grounded metrics inventory for preprocessing jobs (fsMotion/align/tsCTF/+) → docs/preprocessing-metrics-inventory.md; build order for making the journey more informative + decluttered
metadata: 
  node_type: memory
  type: project
  originSessionId: c411690d-19bf-453b-8950-6ece86784e5f
---

**Goal (2026-07-01):** make the journey view show more, better-explained per-tilt/per-TS metrics from
preprocessing outputs, decluttered, with tooltips; then integrate as graphs (CryoSPARC-style where they
transpose), then overlay params on tilt-filter previews. User is a cryo-ET domain expert who wanted an
overview explaining every readout because "we're not sure what each means."

**Deliverable landed:** `docs/preprocessing-metrics-inventory.md` — full grounded catalog (read from real
runs in `projects/try2_after_pixShift` + `pos9_10`, then adversarially verified). ~122 metrics across
fsMotion / alignment / tsCTF / recon+downstream / tiltFilter+denoise, each with meaning + paste-ready
tooltip + source + real/placeholder flag + current-display status + recommended viz + CryoSPARC analog.

**Key facts (verified against files):**
- **Placeholder trap** (extends [[project_warp_relion_star_placeholders]]): star `rlnCtfMaxResolution` &
  `rlnAccumMotion*` = `1e-6`, `rlnCtfFigureOfMerit` = `None`. → **3 journey panels render EMPTY.** Real
  values are XML-only: `CTFResolutionEstimate` + `MeanFrameMovement` (frameseries XML `Movie@`),
  `GridMovementX/Y` track, `PS1D` fit curve. `|DefocusU−DefocusV|` is always 0 (Warp ties U=V; astig is
  in `rlnCtfAstigmatism`).
- **Alignment:** AreTomo ran GLOBAL (`.aln NumPatches=0`) → SMEAN/SFIT/SCALE all 1.00, **no per-tilt
  residual exists**; proxy = refined shift magnitude √(TX²+TY²). `rlnTomoYTilt = −1·aln_TILT` CONFIRMED
  (adapter ts_alignment.py:316). tomostar `_wrpAverageIntensity` = real dark-tilt detector. UseTilt all
  True (NO tilts dropped — the "37 vs 39" was a workflow-agent parser artifact, corrected).
- **tsCTF:** `GridCTF` (39 nodes) = per-tilt defocus curve, **tilt-angle order NOT star acquisition
  order — join by movie filename**. Astig fit ONCE per TS (constant). Handedness decided GLOBALLY by
  `ts_defocus_hand`. Ice `Thickness`=0 (not fit). TM driver forces `rlnTomoHand=+1` (TEMP-DEBUG) while
  job.json `defocus_handedness=0` — two distinct knobs (412-relevant, [[project_412_tomohand_discrepancy]]).
- **Tilt filter:** card "motion" chip shows meaningless `0.0px` (reads 1e-6 placeholder); DL prob never
  renders (1.0 until DL runs). Labels live in `project_params.json state.tilt_filter_labels`, NOT any
  star. Highest quick win = overlay real XML CTFResolutionEstimate/MeanFrameMovement per tilt.
- **Denoise:** NO persisted quality metric (loss curve/SNR are stdout-only).

**CryoSPARC mapping:** exposure-curation scatter + threshold sliders (the declutter/cull centerpiece,
one point = one TS) · PS1D CTF-fit overlay + tilt scrubber · defocus-vs-tilt + handedness (tomo-only) ·
motion trajectory. Rule: per-micrograph value → per-tilt curve within a TS + per-TS aggregate across data.

**Recommended build order (in doc §6):** ① fix the 3 dead panels via a small frameseries-XML reader →
② tilt-filter preview overlay → ③ defocus-vs-tilt + shift panel → ④ PS1D overlay → ⑤ exposure-curation
scatter (needs multi-TS aggregates; validate on pos9_10 first).

**① + ② LANDED 2026-07-01 (code-clean E+F, PENDING RUNTIME — user must restart `python main.py`, no
auto-reload [[reference_hpc_env]]).** New pandas-free reader `services/tilt_series/frameseries_quality.py`
(`read_frame_quality`/`quality_series`, memoized by mtime) parses the `<Movie>` root attrs
`CTFResolutionEstimate` + `MeanFrameMovement`. Wired into: (a) `ui/tomo_dashboard_dialog.py`
`_render_ctf_motion_plots(..., frameseries_dir=)` — CTF-res + motion panels now source real XML values
(cryoBoostKey == frame stem == XML basename), fsMotion stat-strip too; ts_ctf call left unchanged; motion
axis labeled without asserting Å (units §8-Q1 unresolved). (b) `ui/tilt_filter_panel.py`
`_find_fs_motion_warp_dir` + df enrich `_xmlRes`/`_xmlMotion` → card chips show real CTF-res (`6.9Å`) +
motion, replacing the meaningless `0.0px` placeholder chip. NOT done: adapter-ingest of these into the
star (would make downstream consumers honest for FUTURE runs; render-time read chosen so EXISTING runs
light up now). Next = ③ defocus-vs-tilt + shift panel. §8 lists open domain questions (motion units px-vs-Å, FOVFraction/AxisOffset semantics,
enable thickness/patch-tracking?). Current UI code: `ui/tomo_dashboard_dialog.py` (_render_ctf_motion_plots
/_render_alignment_plots + _HINT_* tooltips), `ui/dashboard/figures.py`, `ui/tilt_filter_panel.py`.
Relates to [[project_journey_rework_dashboard_refactor]], [[feedback_dashboard_plot_principles]],
[[feedback_no_browser_volume_rendering]].

**2026-08-27 follow-up:** ③ landed 2026-08-03; ④ + the rest of the per-tilt Warp/AreTomo readouts landed 2026-08-27 → [[project_tilt_metrics_wave_2026-08-27]].

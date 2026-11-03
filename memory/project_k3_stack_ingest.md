---
name: project_k3_stack_ingest
description: "K3/SerialEM (PACEtomo) tilt stacks ingested FROM THE LANDING PAGE — roadmap 18 S0–S4 CODE-COMPLETE 2026-09-09 (uncommitted), runtime pass owed; split at Create, per-mdoc layer inference, dose ESTIMATED from DoseRate (4.3 e/Å² on the sample), no protocol bundle"
metadata: 
  node_type: memory
  type: project
  originSessionId: 48662cf7-7848-41db-8cbf-680ef788ad68
  modified: 2026-09-09T11:51:26.411Z
---

Plan of record: `docs/roadmaps/completed/roadmap_18-k3-stack-ingest.md` (S0–S4, runtime checklist §6, 9 steps). Analysis +
dry run + decisions + dose estimate: `notes/k3_ingest/00-plan.md`. Sample: `/groups/klumpe/crboost_data/
area_5_a_ts_00{2,3,4,7}/` (nested, one folder per TS; 4 × 39 tilts, 5760x4092, 1.382 Å, axis 85.0 in a
`[T = …]` line, `ExposureDose = 0`); flat split layout in `_k3_dryrun/split/`. Branch `k3_implementation`.

**State 2026-09-09 (end of the build session): S0–S4 written, ruff-clean on every touched file, UNCOMMITTED,
NOT RUN.** One commit per stage is the maintainer's; S0 (`drivers/ts_reconstruct.py` switch fix + halves
pre-flight, `metadata_service.py` `.mrc` key fix) changes every project and is its own commit.
`check_boundaries.py` could not run in the sandbox (no interpreter) — owed. Smoke scripts for the maintainer:
`notes/k3_ingest/check_s1_scan.py` (both dialects + the three §6 scan targets) and `check_s2_split.py`
(splits ts_002 into a scratch dir, times it, idempotence). Pre-existing ruff debt elsewhere (53 errors in
`services/parameters_service.py` + `services/container_service.py`, ~46 unformatted files) is NOT ours.

**Where things landed:** `mdoc_service.py` = `MdocFacts` / `acquisition_from_mdoc` / `parse_header_facts`
(`[T = k = v, …]` lines) / `estimate_incident_dose` / `ts_name_from_mdoc` / `MdocService.first_mdoc_facts`;
`stack_import.py` = `resolve_stack` (header-only nz probe), `choose_source_layer` (the D1 rule, shared by
scan and import), `split_stack` (mrcfile mmap, nz == sections invariant, idempotent by size, verbatim mdoc
with SubFramePath rewritten); `preimport.py` = `ts_name`, `source_kind`, dose gaps, `source_line()`,
`AcquisitionSummary.from_tilt_series`; `project_service.py` = per-mdoc import branch + `progress_cb`
(spinner shows "splitting stack 2/4"), dose provenance resolved for EVERY creation path (mdoc → estimate →
`err`), `import_source_kind`/`import_frame_extension` set BEFORE `ensure_job_initialized` (stamps:
`tomo_dimensions` from `detector_dimensions` on all projects; `out_average_halves=False`, `halfmap_frames=0`
on stacks); landing page = depth-flip of the derived mdocs glob via the hidden input's extension
(`*.mdoc` ⇄ `*/*.mdoc`), frames verdict from the overview, dose row (`dose_per_tilt_override` +
`dose_per_tilt_source` in `DataImportFormState`), estimated-dose chip + source line; protocol dialog =
prefilled dose field; `apply_protocol(dose_per_tilt=, dose_per_tilt_source=)`.

**Behaviour changes to watch in the runtime pass:** (1) `ts_label` is now the mdoc name — a Tomo5 mdoc named
`prefix_Position_10.mdoc` gets TS id `<proj>_prefix_Position_10` (was `<proj>_Position_10`); bare
`Position_N.mdoc` is unchanged. (2) `halfmap_frames = 0` / `deconv = 0` now really omit the Warp switches on
every project. (3) `get_autodetect_params` skips `[ZValue]`-less mdocs and no longer returns `dose_per_tilt`
for `ExposureDose = 0`; it returns `dose_estimate` (a `DoseEstimate`).

**Decisions D1–D6 (roadmap §3), unchanged:** D1 infer layer per mdoc (SerialEM prefers stack, Tomo5 movies);
D2 split at Create in the threaded import; D3 `import_source_kind` + `acquisition.dose_per_tilt_source`,
schema 3.8; D4 missing dose estimated + flagged, never blocks; D5 no protocol bundle, facts stamped at job
init; D6 ts id = mdoc name minus `.mdoc`/`.mrc`, frame = `<prefix><SubFramePath basename>.mrc`.

**Dose estimate:** zero-thickness fit of `ln(DoseRate×ExposureTime/px²)` vs `1/cos(θ−θ₀)`: incident 4.21–4.32
on all four series, pretilt −8.5°, rms < 1 % → working value 4.3 e/Å²/tilt (transmitted 2.2 is NOT it).
Gates in code: every section has the rates, ≥10 tilts, ≥30° span, rms < 0.05, slope < 0; else transmitted only.

**Open (not code):** acquirer's dose (replaces 4.3), handedness answer (Warp said no flip), delivery layout.
Related: [[feedback_never_silent_defaults]], [[feedback_protocols_scope_no_harness]],
[[feedback_dont_gate_stages_on_runtime]], [[project_copia_ctf_bimodal_fits]], [[reference_hpc_env]].

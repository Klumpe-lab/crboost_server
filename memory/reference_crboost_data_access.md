---
name: reference_crboost_data_access
description: "/groups/klumpe/crboost_data (DefaultProjectBase) — mountable via the claudeenv apptainer --bind (added 2026-07-02); registry/tilt_series/*.json is the rich displayable data model; 9 multi-TS agg_* projects. Also no python interpreter"
metadata: 
  node_type: memory
  type: reference
  originSessionId: c411690d-19bf-453b-8950-6ece86784e5f
---

**Why it was invisible + the fix (2026-07-02).** Claude Code runs INSIDE an apptainer/singularity
container (`~/node_20-slim.sif`) launched by the user's `claudeenv` zsh alias. The container only sees
paths passed via `--bind`; the alias originally bound only `/users/artem.kushner,/tmp`, so `/groups`
didn't exist in the namespace at all (NOT Claude's inner sandbox — confirmed: absent even with sandbox
disabled). A repo-local symlink into `/groups/...` does NOT help (target not in namespace). **Fix: add
`--bind /groups/klumpe/crboost_data` to the claudeenv alias and RELAUNCH** (binds are fixed at container
launch — an already-running session can't gain one). User did this 2026-07-02 → crboost_data now readable.
If a future session can't see it, the alias/relaunch is the reason. Alt (no relaunch): copy metadata into
`/users/artem.kushner/...` (home is bound). READ-ONLY: never write under /groups.

**crboost_data contents (2026-07-02):** ~26 projects. The 9 `agg_*` are complete multi-TS 412 projects
(4–29 TS each: agg_20260205_FCIso_unmilled2=29, agg_SK=20, agg_20251113=18, agg_20260311_Grid3=17, …),
all the same 13-job chain (import→fsMotionCtf→align→tsCtf→tiltFilter→recon→templatematching→tmextractcand
→subtomoExtraction), all 2 species (`412` real, I1 sym + blob ellipsoid, C1). All `is_aggregation:false`
— "agg_" is just naming; aggregation is the downstream STA intent, not a flag. Also full pipelines:
Copia_Aaron_Tutorial, 20260609_vreteno_around_cells, S2_cells_lamellae_LG, post_handedness_fix, etc.

**Rich displayable metadata = the REGISTRY** (`registry/tilt_series/<TS>.json`, TiltSeries-Registry refactor
schema v1.0; see [[project_ts_registry_refactor]]). Per-TS, structured, processing outputs attached per
frame: acquisition (`nominal_tilt_angle_deg`, `pre_exposure_e_per_a2` dose, `acquisition_time`), CTF
(`defocus_u/v_angstrom`, `defocus_angle`, `ctf_astigmatism` under outputs.fsMotionAndCtf), alignment
(`z_rot_deg`, `x/y_shift_angstrom`, `tilt_x/y_deg` under tomogram.per_frame), recon (per-TS
`tomogram_pixel_size_angstrom`, `size_x/y/z`, `binning`, `reconstructed_mrc`, `are_angles_inverted`=
handedness, `alignment_method`), selection (`is_selected`, `is_filtered_out`, `filter_reason`). GOTCHAS:
(1) registry `defocus_u == defocus_v` (stores MEAN + separate `ctf_astigmatism`) → NOT a real U/V split;
true per-axis + CTF-fit-res + motion are ONLY in the WarpTools XML (`frameseries_quality.py`). (2)
`species_id` inconsistent across projects: `412_blob` vs `412blob`. mdoc-raw per-tilt acquisition
(`min/mean/max_intensity`, `dose_rate`, `exposure_dose`, `prior_dose`, `image_shift_x/y`, `exposure_time`,
nominal `defocus` µm) lives in `project_params.json`, NOT the registry. See [[project_preproc_metrics_inventory]]
— this closes the multi-TS validation gap and makes ⑤ (exposure-curation cull view) real.

**Ground truth I CAN read = repo-local `projects/`:**
- `projects/try2_after_pixShift` — best single dataset: one TS `Position_11_2`, 39 tilts, WarpTools +
  AreTomo, full job chain `External/job002`(fsMotionCtf) `job003`(align) `job004`(tsCtf) `job005`(recon)
  `job006`(templatematch) `job007`(extract) `job008`(subtomo). Most metrics work sampled here.
- `projects/pos9_10_after_pixShift` — has `TiltFilter/`, `tilt_filter_thumbnails/`, Class3D/Refine3D.
- `projects/demo`, `projects/newp`, `projects/3dclass_aln_pixShiftBug` — present, not deeply sampled.

The repo-local `projects/` are mostly SINGLE-tilt-series (fine for quick checks). For cross-TS
distributions (exposure-curation scatter, per-tilt-vs-per-TS astigmatism, handedness spread) use the
multi-TS `crboost_data` agg_* projects above now that the mount works.

**Python runs but has NO pandas/numpy (re-verified 2026-07-30, corrects the old "no interpreter" claim):**
`venv/bin/python3` IS a working Python 3.11.5 — `py_compile` works, non-pandas modules import. BUT
`pandas` and `numpy` are NOT installed in the venv (physically absent from site-packages), and `module load`
isn't available in-sandbox. Since basically every service goes through `StarfileService` (pandas) /
`mrcfile` / `numpy`, you CANNOT execute the adapters, drivers, `main.py`, or any harness that reads a star
file. Static-check ceiling = `./venv/bin/ruff check` (select E,F) + `ruff format` + `py_compile`. **You CAN
read star/registry files as plain TEXT** (awk/grep row-counting works fine — used it 2026-07-30 to measure
the emit_star ghost-tilt gap on real files). To actually run pandas code the user must use their real
server python (module-loaded env). `node` v20 available. Server has no auto-reload → user restarts `python
main.py` to runtime-verify. Extends [[reference_hpc_env]].

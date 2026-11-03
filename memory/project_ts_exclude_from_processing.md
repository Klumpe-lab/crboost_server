---
name: project_ts_exclude_from_processing
description: "Per-TS \"exclude from processing\" mute feature — registry flag + .skip enforcement across all array drivers + strip toggle"
metadata: 
  node_type: memory
  type: project
  originSessionId: a7317799-b6aa-4182-8640-96be59ab82b8
---

Per-tilt-series **exclude-from-processing** mute (landed 2026-07-23, code-clean/ruff, PENDING RUNTIME — sandbox has no python).

**Problem it solves:** one malformed TS that can never succeed at template matching deadlocked the whole dataset (all-or-nothing job status → stop-on-fail cascade). The earlier partial-success driver relaxation was REVERTED per user; exclusion is now the *only* sanctioned way past a bad TS.

**User decisions (AskUserQuestion):** (1) **Forward-only + render-time honor** — never rewrite existing on-disk STARs; drivers pre-skip excluded TS on next run; dashboard subtracts them from "expected". (2) **Reverted all-or-nothing** — a TS that fails *without* being excluded still fails the job loudly.

**Design (3 layers, all reuse existing primitives):**
- **Model:** `TiltSeries.is_excluded` + `exclusion_reason` (services/tilt_series/models.py). NOT the dormant `is_filtered_out` (that's the frame-level tilt-filter concept).
- **Registry:** `set_excluded(ts_id, bool, reason=)` + `excluded_ids()` (registry.py); persist via `_dirty_ts` + `save_async()`.
- **Enforcement:** `apply_exclusions(job_dir, project_path, ts_names)` + `load_excluded_ts(project_path)` in drivers/array_job_base.py. `apply_exclusions` clears stale `.ok`/`.fail` then writes `.skip` for excluded TS (exactly one marker each, or the `ok+skip==len` tally breaks) BEFORE submit_array_job → not dispatched, counts as settled success. Manifest keeps ALL names so excluded TS stays visible in the strip (mute≠hide).
- **Aggregation:** registry-adapter drivers (fs_motion_and_ctf, ts_ctf, ts_reconstruct) now `ingest(results.ok)` + `emit_star(..., excluded_ids=set(results.skipped))`; the 3 adapters' emit_star DROP excluded rows from the fresh output global block (raise still fires on *non*-excluded unresolved = loud). ts_alignment already tolerant (aligned_ts=results.ok, emit drops unresolved). template_match: plain copy (forward-only; benign tomograms-lists-N-but-particles-cover-N-1 mismatch — extract enumerates from `*_job.json` glob so malformed TS never enters). extract_candidates: filters `*_particles.star` glob against excluded. subtomo_extraction: folded into its existing empty_ts/.skip machinery + filters ts_with_picks.
- **UI:** per-column exclude/restore toggle on the dashboard strip (ui/dashboard/strip.py `on_toggle_exclude`/`excluded_ids`; `click.stop` so it doesn't select). Muted visual = hatched bg + strikethrough header + dimmed data cells (header+restore icon stay crisp so undo is always reachable). Wired in tomo_dashboard_dialog.py render_strip (`toggle_exclude` handler → registry.set_excluded + save_async + render_strip; excluded set folded into `_strip_sig` so a toggle forces rebuild). CSS in ui/dashboard/css.py (`.cb-strip-col.excluded`, `.cb-strip-excl`).

**Deadlock/trap guards:** UI refuses to exclude the last active TS (empty-set → adapters `raise "ingest called with no expected TS ids"`). Exclusion applies on NEXT run only (can't stop in-flight tasks). Undo = clear flag; next run's `.skip`-clear re-dispatches. Re-running TM after exclude is INSTANT (no compute — malformed→skip, 24 kept .ok → submit returns None → supervisor re-aggregates → SUCCESS).

**Open runtime caveat (unresolved):** "TM summary still blocked" after the partial fix is EITHER (A) a dataset-level expected=N count gate (exclusion fixes at render) OR (B) extract-candidates job never re-dispatched (was left FAILED). Ask colleague: does `TemplateExtract/jobNNN/` exist with ~N-1 rows? The Particles section (`_collect_species_data_for_ts`, tomo_dashboard_dialog.py:3220) gates on the extract job dir existing + a per-TS row — NOT on job status. Related: [[project_per_ts_parallelization]] [[project_ts_registry_refactor]] [[project_pipeline_state_desync]]

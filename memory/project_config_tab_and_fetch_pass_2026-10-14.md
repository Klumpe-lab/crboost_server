---
name: project-config-tab-and-fetch-pass-2026-10-14
description: "UI pass on main 2026-10-14: Config tab sections open (no expansions), I/O restyled, run-dot text → tooltip, image routes skip gzip + immutable cache, off-loop first project load; PENDING RUNTIME"
metadata:
  node_type: memory
  type: project
  originSessionId: 8f7e2753-360e-4d53-ac07-7a225abddb9f
  modified: 2026-10-14T18:08:01.449Z
---

Built on `main` (user asked "while we are here on main"), lint-only verified (no python in sandbox).

- Run dot: status text moved into its tooltip (`pipeline_roster.py` rebuild_run_slot / update_status_label).
- Config tab: Parameters / Inputs / Outputs / SLURM resources as plain `config_section` blocks
  (`ui/job_plugins/_field_styles.py`); SLURM `field_group` lost its box. I/O rows share LABEL_STYLE
  column, one dot per input (in the source box), 9 px secondary text; `.cb-io-*` hover moved from the
  main_ui shell block to `ui/dashboard/css.py`.
- Fetch: `Content-Encoding: identity` on image routes (skips NiceGUI gzip-9), vis-asset PNG with `v`
  = immutable cache, `v` now st_mtime_ns; gallery tiles lazy; Journey select_ts collects once;
  particles.star parse memoized on (mtime, size); first project open parses state + registry in a
  thread (`preload_project_state`, `preload_registry`); projects overview tick re-renders only on change
  (`refresh(force=True)` for explicit calls); Files-tab zoom scales a wrapper (no base64 resend).

Not done (reported): roster signature disk I/O every 3 s, gallery thumbnail variant, tilt viewer
base64, tilt-thumb URLs unversioned, `/api/tilt-thumb` + `/api/file` have no project-root guard.
Related: [[reference-nicegui-head-html-not-shared]], [[project_ui_pass_2026-08-27]].

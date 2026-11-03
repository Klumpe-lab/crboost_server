---
name: munich-shipprep-ui-pass
description: "Munich ship-prep UI pass (2026-07-24) — gain-ref at setup, IO tab rework, config-select restyle, roster row cleanup; all code-clean PENDING RUNTIME (server restart needed)"
metadata: 
  node_type: memory
  type: project
  originSessionId: c003b0d4-3eef-4f7a-8751-53ab489b70fe
---

Ship-prep UI pass for the Munich collaborators. All landed 2026-07-24, code-clean
(py_compile + ruff), **PENDING RUNTIME** — server has no auto-reload, user must
restart `python main.py` + hard-reload browser (CSS is injected from Python). See
[[reference_hpc_env]].

**Gain reference = PROJECT-LEVEL (decided).** `acquisition` is a shared singleton on
ProjectState (`services/models_base.py` AcquisitionParams.gain_reference_path); every
job's `acquisition` property returns the same ref (`services/jobs/_base.py:317` +
`gain_path` @355). Data plumbing to the two consumers (fsMotionAndCtf, tsImport
drivers, via `params.gain_path`) already existed — only the SETUP UI + a per-job
surface were missing. Added: (1) gain file field in data_import_panel raw-data section
(reuses `local_file_picker`, mode=file) → `DataImportFormState.gain_reference_path` →
`detected_params["gain_reference_path"]` → `project_service.py:~432`; (2) an
auto-populated, file-picker-overridable "Gain Reference" row at the top of Inputs in
`io_config_component` for `GAIN_CONSUMERS = {FS_MOTION_CTF, TS_IMPORT}` (writes the
project-level value). Did NOT add a resolver-wired JobFileType for gain (would need
invasive path_resolution changes for a producer-less external file).

**IO tab rework** (`ui/pipeline_builder/io_config_component.py` — mostly rewritten):
custom status-dot source **menu** (`ui.menu()` with `no-parent-event`, opened via
`selector.on("click", menu.open)` since the anchor is a div not a button); options read
like sidebar job rows (dot + disambiguated name + status word — fixes "[scheduled]
scheduled" double-word and two-queued-Alignments-look-identical via `_candidate_name`
using `producer_instance_id` suffix / job folder); fixed-width name column + flex
selector fixes the horizontal dropdown stagger (the variable "(+1)" accept hint moved
OFF the row into the hover); Manual-path file picker implemented (was "not yet
implemented"); Inputs/Outputs split into titled sections; rich dark hover panels
(`with name, ui.tooltip():`) explain artifact types (`_FILETYPE_INFO`) / predicted abs
output paths. **BUG FIXED same session:** `_save_and_refresh` now
`save_project(force=True)` — source-override handlers mutate `source_overrides` in
place (bypasses dirty tracking) so a plain save skipped the write and picks vanished on
reload (this was a PRE-EXISTING latent bug).

**Config restyle:** `.cb-select` class + themed popup CSS in `ui/main_ui.py` (clean 1px
slate border, not Material) applied via `enum_field` in `_field_styles.py`; tighter
rows (ROW_STYLE min-height 22→19, padding→0, field_grid gap 2→1).

**Roster** (`ui/pipeline_builder/pipeline_roster.py`): removed the redundant per-row
config/logs/files icons (reachable from the job top-bar tab switcher); removed the
always-rendered chevron row UNDER array jobs; single on-row chevron toggle now renders
for EVERY array job (gated on `array_throttle`, NOT on having a `tasks` extra tab —
subtomo_extraction has array_throttle but no tasks tab, so tying the chevron to the tab
dropped its toggle: **BUG FIXED same session**). Empty expansion shows a muted "tasks
appear once running" hint.

Adversarial review (subagent) found the two bugs above; both fixed + re-verified. Files
touched: pipeline_roster.py, io_config_component.py, _field_styles.py, main_ui.py,
project_service.py, ui_state.py, data_import_panel.py.

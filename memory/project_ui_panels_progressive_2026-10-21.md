---
name: project_ui_panels_progressive_2026-10-21
description: "2026-10-21 UI pass: view title line + expand icon, job page = one scroll page (no tabs), gray-blue PANEL_STYLE sweep, divider removal, progressive projects overview + tomogram wall; BUILT, NOT RUN"
metadata:
  type: project
---

Built 2026-10-21 on branch `segmentations` (user chose "build now, stay on segmentations"). Ruff-clean, NOT RUN.

- **View frame** (`ui/workspace_page.py` `_view_frame`): every view has a title line (12 px 600 #2c4a6b,
  `_VIEW_TITLES`), an expand icon (`_solo`: only this view on; hidden when alone) and the close cross.
  `callbacks["set_view_title"](name, text, tooltip)`; the pipeline view's title = the shown job's name
  (`PipelineBuilderPanel._show_job`), "Jobs" with none.
- **Job page** (`ui/pipeline_builder/job_tab_component.py`): no Config/Logs/Files/Tasks switcher. One scroll page:
  Parameters, Inputs/Outputs, SLURM panels, then folded lazy panels (extra tabs e.g. Tasks/Results, Log, Files; built
  on first open, 360 px resizable box). Log opens by default once the job has run. URL tab segment / roster links →
  `_handle_tab_switch` opens + scrolls (`widget_refs.section_openers`).
- **Panels**: `PANEL_STYLE` / `PANEL_TITLE_STYLE` in `ui/job_plugins/_field_styles.py` (#eef3f8, radius 6, title
  #2c4a6b); `config_section` renders one. Forks converted species/workbench/analytics, protocols/landing/dialogs,
  job internals/roster/tilt filter; header-bar rules dropped in css.py (.cb-jhead, .cb-species-header, .cb-gal-toolbar,
  .cb-tp-group). `section_rule()` deleted.
- **SLURM wall estimate removed** (user: "horrendously off"). NiceGUI removed from the startup spinner text.
- **Progressive**: `scan_for_projects(base, on_partial=)` streams rows (first scan only); `stable_index` removed.
  Tomo wall: tiles at geometry time, sources/picks/tilts fill in batches; `all_geometries` 16 threads; log line
  `tomograms: wall shown in X s, filled in Y s`.

**Watch at runtime:** config_settings_dialog.py was rebuilt after a bad perl edit by a fork — check its diff.
Related: [[project_landing_overview_rework_2026-10-19]], [[feedback_ui_chrome_conventions]],
[[feedback_journey_dont_overcorrect]].

Later same day: loading look = gray pulse (`cb-img-wait`, css.py) on image frames while an <img> is unmarked; `_IMG_STATE_JS` marks data-cb-img ok/err; atlas tiles use a hidden `.cb-img-probe` img (`_atlas_probe`, pick_viewer.py). Panel title stripe `PANEL_HEAD_STYLE` (#dfe7f0, bleeds via negative margins, first child of PANEL_STYLE); view title lines + roster "Jobs" line striped; view/roster borders 2px #b4c2d3. NOT RUN.

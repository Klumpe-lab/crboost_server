---
name: ui-pass-2026-08-27
description: "2026-08-27 UI pass — server-config/protocols width fix, protocol detail dialog, Journey header line + switcher dropdown + single-select section selector + ECharts per-tilt charts, full-width logs with copy, hidden scrollbars; PENDING RUNTIME"
metadata: 
  node_type: memory
  type: project
  originSessionId: f8dc850d-2bfe-4bd2-9c81-01db35592828
  modified: 2026-08-27T15:28:13.448Z
---

Code-complete 2026-08-27, not yet seen in a browser (no python in the sandbox: ruff only):

- Server config + Protocols dialogs: rows claim full width (`_ROW_BOX`, `cb-scroll-tight`); SLURM fields in a uniform
  auto-fill grid; Download is an icon; no "bless" wording in the UI (the CLI verb `crboost_regress.py bless` stays);
  protocol name opens a detail dialog (species assets + every stage's full params + input wiring).
- Journey: `ui/dashboard/strip.py` is now ONE thin header line (position · i/N · 4 labelled stage dots · species
  counts · ⓘ · exclude) whose name block drops a full switcher table (`ui.menu`); the section selector is a
  `Segmented` "All | …" at the right (pref `dashboard_panel`, replaced `dashboard_hidden_panels`). Per-tilt charts
  moved Plotly → `ui.echart` (`build_per_tilt_chart`, palette `SERIES_PALETTE`), bold axis names, legend below,
  stats as `_stat_tiles`, params behind a ⓘ tooltip (`_section_header`), no section icons, no "Not shown" notes.
  Pick scatters in the Particles section stay on Plotly (pick_viewer's).
- Logs: Tasks-tab per-task panes full width + copy-full-file button; Logs tab wraps + copy button.
- Scrollbars hidden app-wide (css.py, end of `_CB_CSS`).

**Why:** user's UI review (compressed panels, cluttered Journey, unreadable logs).
**How to apply:** runtime check owed — journey header/dropdown, ECharts tooltips (`:formatter`), `cb-params-tt`
tooltip grid, `:has()` collapsed-header rule. See [[nicegui-flexstart-shrinkwrap]].

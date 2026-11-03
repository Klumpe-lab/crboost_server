---
name: ux-jank-refactor
description: "Audit + refactor in progress (started 2026-05-18) to fix workspace UI jank — dropped clicks, hover lag, dialog stacking — by introducing reactive primitives and converting key views"
metadata: 
  node_type: memory
  type: project
  originSessionId: bc6ea342-a620-4e92-bd69-2d7c3406add0
  modified: 2026-08-13T15:47:39.279Z
---

User reported (2026-05-18) accumulating workspace jank: tomo roster rows not reacting to hover for seconds, 2-3s click-to-switch on tomos, job rows dropping first few clicks on add, particle dialog "Add" clicks not registering then the dialog suddenly disappearing replaced by an identical one.

**Audit root causes** (file:line citations in conversation):
1. `status_poller.py:126` ticked `panel.roster.refresh()` every 3 s unconditionally; `pipeline_roster.py:183` did `panel.roster_panel.clear()` + full rebuild → all buttons / handlers destroyed every 3 s.
2. `prompt_species_and_add` had no re-entry guard → rapid clicks (because of #1) queued multiple dialog coroutines, producing the "dialog disappears, identical one appears" symptom.
3. `pipeline_roster.py:1338` ran a 0.17 s `ui.timer` + `ui.run_javascript("document.querySelectorAll('.cb-row-spinner').forEach(...)")` broadcast — server-driven spinner animation; ~12 websocket messages/sec/tab while running.
4. `tomo_dashboard_dialog.py` click handler does heavy sync disk I/O on the event loop + full sidebar rebuild on every click → 2-3s switch delay. **User explicitly said don't touch this — "it's quite workable."**

**Why**: Months of incremental NiceGUI development gradually settled into a clear()+rebuild idiom that's tolerable at low complexity but compounds destructively as polling timers + DOM size + re-entry surface grow.

**How to apply**: Future UI work follows the [[reactive-ui-patterns]] feedback memory. The new primitives live in `ui/components/reactive.py` and are documented in `CLAUDE.md` under "UI reactivity patterns."

**Status (2026-05-18)**: Stage 1 landed — RosterWidget converted, dialog guarded, spinner CSS-ified, tray converted, docs written. Stage 2 (if user pursues): potentially convert dashboard, logs_tab, projects_overview, data_import auto-refresh. Open question: whether the 4-second tomo dashboard live_timer can also benefit from FingerprintedView without changing the section-card rebuild contract — user wants the dashboard left alone for now.

**Follow-up intent (2026-08-13)**: user, mid-de-novo-picking arc — "the ui is so bad right
now. I want to differentiate it a bit when we are done with this." Wants a UI *differentiation /
visual-identity* pass (distinct from this jank work, which was about dropped clicks and rebuild
churn) once the de-novo picking arc lands. Concretely triggered by not being able to runtime-verify
denovo S2 because the surface is hard to work through. Not scoped yet; when it is, it must respect
[[ui-chrome-conventions]] (compact `cb-*` chrome, no chunky material widgets) rather than replacing them.

**Files touched 2026-05-18**:
- new: `ui/components/reactive.py`
- edited: `ui/main_ui.py` (CSS keyframes for `.cb-braille-spin`)
- edited: `ui/pipeline_builder/pipeline_roster.py` (RosterWidget → FingerprintedView; spinner timer removed)
- edited: `ui/pipeline_builder/pipeline_builder_panel.py` (SingleFlight wrapper on species dialog)
- edited: `ui/pipeline_builder/status_poller.py` (drop stop_spinner_timer call)
- edited: `ui/background_task_tray.py` (tray → FingerprintedView)
- edited: `CLAUDE.md` (new "UI reactivity patterns" section)

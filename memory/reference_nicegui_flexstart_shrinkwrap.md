---
name: nicegui-flexstart-shrinkwrap
description: "Why panels render \"compressed to the left\" in NiceGUI 3 — scroll_area/expansion/column content is align-items:flex-start, so width-less children shrink-wrap; fix with cb-scroll-tight or explicit width"
metadata: 
  node_type: memory
  type: reference
  originSessionId: f8dc850d-2bfe-4bd2-9c81-01db35592828
  modified: 2026-08-27T15:28:02.122Z
---

NiceGUI 3.0.3 (`nicegui/static/nicegui.css`) lays out `.nicegui-column`, `.nicegui-scroll-area .q-scrollarea__content`
and `.nicegui-expansion .q-expansion-item__content` as `display:flex; flex-direction:column; align-items:flex-start;
gap:1rem; padding:1rem` (columns: padding 0). Any child without an explicit width (a bare `ui.element("div")`, `ui.html`)
shrink-wraps to its content → the "everything crammed to the left, empty space on the right" symptom (server-config
dialog, protocols table, per-task logs, 2026-08-27).

**How to apply:** give row containers `width: 100%; box-sizing: border-box` (or `w-full`), or put `cb-scroll-tight`
(ui/dashboard/css.py) on the scroll area / an ancestor: it sets `align-items: stretch; padding: 0; gap: 0` on the
scroll-area and expansion content. `ui.log` is a q-scrollarea with `white-space: pre` — class `cb-log` wraps lines.
`ui.echart` is bundled (no CDN) and takes `":formatter"`-style JS keys via convertDynamicProperties.
Related: [[ui-chrome-conventions]], [[reactive-ui-patterns]].

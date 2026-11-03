---
name: reference_nicegui_client_deleted_warning
description: "NiceGUI 'Client has been deleted but is still being used' is warn_once — the log shows only the FIRST hit per server process; cause = UI write to a client deleted mid-await or between timer check and callback"
metadata:
  node_type: memory
  type: reference
  originSessionId: 75e22313-469d-437b-8d21-38cfe78659d8
  modified: 2026-10-05T13:44:13.639Z
---

`nicegui/client.py check_existence` → `helpers.warn_once(..., stack_info=True)`: the message is logged once per process,
so one traceback in the log can stand for many silent repeats. Two sources seen 2026-10-05 (NiceGUI 3.0.3):
`ProjectsOverview.refresh` (async — the client was deleted while `scan_for_projects` was awaited, then `_render_list`
cleared the list) and `FingerprintedView.refresh` driven straight by a `ui.timer` (the tray's) — a client deleted
between the timer loop's `_should_stop()` check and its scheduled callback.

Client deletion marks every element `_deleted`, so `element.is_deleted` is the guard; both sites now return early on it.
Harmless when it fires (writes to a dead client are dropped), but a render after the guard point would also leak
elements into the dead client. Related: [[reference-nicegui-timer-parent-slot]], [[feedback-reactive-ui-patterns]].

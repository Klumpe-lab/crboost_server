---
name: reference-nicegui-timer-parent-slot
description: "NiceGUI \"The parent slot of the element has been deleted\" traceback from timer.py = a ui.timer inside a container rebuilt before its first tick; use owned_timer"
metadata:
  node_type: memory
  type: reference
  originSessionId: a973482d-80d7-4356-bae5-8795fa723342
  modified: 2026-10-15T08:47:18.560Z
---

Server log traceback ending in `nicegui/elements/timer.py _get_context → element.py parent_slot → RuntimeError: The
parent slot of the element has been deleted.` (NiceGUI 3.0.3). Cause: a `ui.timer` created INSIDE a container that our
own refresh rebuilds (FingerprintedView render, roster row, job page) is deleted before its first tick — typically a page
load that renders the roster twice before the browser connects. NiceGUI reads the timer's parent slot before checking
whether the timer was deleted, so it logs instead of exiting quietly. Harmless but noisy.

Fix (2026-10-04): `owned_timer(interval, callback, owner, *, once=False)` in `ui/components/reactive.py` — parents the
timer at the page layout (`dialog_host()`) and cancels it on the first tick after `owner.is_deleted`. Applied to the
tilt-filter row views and job page. Remaining old-pattern sites if the traceback returns: `ui/components/array_task_tracker.py`
(5 s poll inside job content), `ui/pipeline_builder/logs_tab.py` timers.

Silent variant (found 2026-10-15, not fixed): a page class that makes its timer in `__init__` inside its container
and then `container.clear()`s on first build deletes the timer before it ever runs — no traceback, the poll just never
happens. `TomoGalleryPage` (15 s pending-preview poll) and `ProtocolsPage` (15 s poll) do this; the pipeline table
(`ui/pipeline_table_view.py`) uses `owned_timer` instead.

Related: [[feedback-reactive-ui-patterns]], [[project-tilt-views-roadmap26]].

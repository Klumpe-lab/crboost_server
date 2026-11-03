---
name: feedback_background_task_no_client_context
description: "Inside a BackgroundTask work coroutine, bare get_project_state()/save_project() silently no-op (no client context) — resolve ProjectState by explicit path"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d6aae61d-6bb0-4298-a371-7d5dbef3d647
  modified: 2026-10-05T13:44:08.414Z
---

`get_project_state()` (`services/project_state.py:926`) resolves the project from the **current browser tab's** `app.storage.tab` (via `get_ui_state_manager()`). A `BackgroundTask` work coroutine runs detached (`services/background_tasks.py` → bare `asyncio.create_task`), so it has **NO client/tab context**: `get_project_state()` hits its `except RuntimeError` and returns a **blank throwaway `ProjectState()`**. Any `.add_pick_list()/.mark_dirty()/setattr` on it is discarded, and a path-less `await get_state_service().save_project()` no-ops because the blank state's `project_path is None` (early `return` at `project_state.py:1042`).

**Why:** This is silent — no error, no log. The work *appears* to run (files written via explicit-path backend calls still land), but the in-memory registry and `project_params.json` never update, so the UI never reflects the change. It caused the ArtiaX auto-import W2 bug (manual list written to disk but chip never appeared) and a sibling in `tilt_filter_panel.py` (`tilt_filter_png_dir` never persisted). Same FAMILY as the 2026-05-13 stuck-yellow bug ([[feedback_inmemory_state_authoritative]]) — background state mutation not reaching the authoritative singleton.

**How to apply:** In ANY `BackgroundTask` work coroutine (or anything that may run without a client connection), resolve state by **explicit path**: `get_state_service().state_for(project_path).<mutate>()` then `await get_state_service().save_project(project_path=project_path, force=True)`. `state_for(path)` → `get_project_state_for(path)` is path-keyed and context-independent, and returns the SAME singleton a client-context `get_project_state()` captured, so a UI builder that captured `state` earlier sees the mutation on `refresh()`.

The SAFE alternative (used by the dashboard preview/IMOD kicks, `_auto_kick_preview_generation` / `_handle_generate_for_instance`): capture `state = get_project_state()` BEFORE the coroutine (in client/render context) and pass it in as an argument — but that only fixes reads/in-memory mutation; a `save_project()` inside the coroutine STILL must pass `project_path=` or it no-ops. NiceGUI click/value-change/timer handlers DO have client context, so bare `get_project_state()` is fine there — but NOT in an `asyncio.create_task(...)` spawned from one: NiceGUI slot stacks are keyed per asyncio task (`nicegui/slot.py`), so the new task starts with none. 2026-10-05 instance: the I/O tab's `_save_and_refresh` task called `on_change` = the job-tab save handler, whose call-time `current_project_state()` came back blank → 9× `backend.save_project called without a project_path — nothing saved` (that warning is the tell). Fixed by `create_save_handler(ui_mgr)` reading `ui_mgr.project_path`. Related: [[project_artiax_bridge]], [[feedback_reactive_ui_patterns]].

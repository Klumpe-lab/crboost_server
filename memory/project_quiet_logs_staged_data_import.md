---
name: project_quiet_logs_staged_data_import
description: "2026-08-27 pass — terminal log quieting (root WARNING + crboost.events whitelist, sticky banner) and staged data-import path flow (commit-gated mdoc scan, threaded completion, recents-in-input); PENDING RUNTIME"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7f4ad016-b554-4b6b-becc-40f00f747cdf
  modified: 2026-08-27T15:59:59.624Z
---

2026-08-27: two long-standing irritations addressed, CODE-COMPLETE, PENDING RUNTIME.

**Logs.** Root logger is WARNING unless `--debug`; the only INFO channel is
`services/event_log.py::events` (`crboost.events`) — pipeline started/finished/failed,
job status transitions, "Queued X -> SLURM N". `uvicorn.run(log_config=None)` is what
actually kills the access log (uvicorn reinstalled its own config after
`setup_logging()`). `app.add_event_handler` replaces the deprecated `@app.on_event`.
Banner (URL + ssh tunnel line, bold/cyan) is pinned via a DECSTBM scroll region when
stdout is a TTY; `CRBOOST_PLAIN_LOG=1` opts out.

**Data import.** `GlobDirectoryInput` now: threaded+debounced `os.scandir` completion,
Tab-complete, Enter = `on_commit`, recent paths rendered inside the dropdown
(`recent_provider` / `on_forget_recent`), `extension=""` mode used for the project
base path too. Panel: typing only runs a debounced glob count; mdoc parse + autodetect
run only on commit (Enter / picker / recent / "Scan dataset" button). `parse_gen`
generation guard fixes the progress bar jumping (overlapping parses shared one dict).
Recent Locations / Recent Data side panels deleted.

**Why:** user request — 200 lines of logs per navigation; every keystroke into a
5000-file tomogram dir triggered a full mdoc parse on the event loop.

**How to apply:** new "operator-visible" log lines go through `events.info`, never
`logger.info`. Expensive path work belongs in `on_commit`, never `on_change`.
Related: [[feedback_reactive_ui_patterns]], [[reference_hpc_env]].

**Round 2 (same day):** picker `_scan_dir` → os.scandir, no size stats, directory
mode lists dirs only + "N files" count, file mode capped at 400 rows, "Loading…"
shown before the thread scan; completion cache `_SUBDIR_CACHE` keyed by parent mtime;
picker no longer commits (sets path only); parse warnings folded (3 + "+N more…"
toggle); Create always enabled, gaps redden the fields (`_mark_missing`, cleared in
`update_create_button_state`); blur-commit guarded by `committed_base_path` +
client `_deleted` (fixes "Client has been deleted" warning on navigate-away).

**2026-10-19 logging rework (BUILT, NOT RUN; supersedes the "root WARNING" part above).** User asked for a
readable live console + a capped log file. `services/log_setup.py`: console (stderr) = events + own
WARNING/ERROR only, compose-style `emitter │ HH:MM:SS  ▲ message`, modest ANSI (TTY only; NO_COLOR /
CRBOOST_PLAIN_LOG off), blank line after a >3 s gap, exceptions as one `↳` line pointing at the file. File =
`~/.crboost/logs/crboost-<port>.log`, RotatingFileHandler 20 MB × 5, own code at INFO, libraries at WARNING.
Loop watchdog lines go to the file only (CRBOOST_TRACE_STALLS=1 = console + stacks). Events now read
`<project> · …` (orchestrator, runner, backend tilt-filter lines, project created/deleted). Rule: timer-driven
code logs at DEBUG. Project opens log `<name> · opened in X s (load, build)` to the file (ui/main_ui.py).

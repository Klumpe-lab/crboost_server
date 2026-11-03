---
name: project_ui_latency_roadmap34
description: "UI lag (~2 s click-to-focus, mid-Oct 2026) — roadmap 34; decisions taken + S0–S6 BUILT 2026-10-17, NOT RUN; S7 + leftovers open"
metadata:
  node_type: memory
  type: project
  originSessionId: 7f0101a0-21c3-454b-8ae4-ae2ee3269f0b
  modified: 2026-10-17T11:18:39.862Z
---

User reported (2026-10-17) the whole UI got very laggy over the previous week (~2 s before a click into a job Config field takes focus). Roadmap: `docs/roadmaps/roadmap_34-ui-latency-and-workers.md`.

Decisions (§3): (1) job pages — "unload" = delete hidden job pages; built as keep shown + 2 before (`_KEPT_JOB_PAGES`), taken as default because the user asked what it meant — revisit if they object; (2) server-owned spawn process pool (user's choice); (3) fields update on change (user: "think about user experience") — only derived views debounced 0.3 s; (4) "previews" = candidate-extract preview passes (pick overlay, cutout gallery, histograms); auto pass now gated by `previews_up_to_date` (manifest `complete` + newer than sources).

Built patterns (also in CLAUDE.md "Nothing heavy on the event loop" and docs/architecture.md "Where work runs"): `services/loop_watchdog.py`, `services/workers.py` (`run_in_worker`, `WorkKind`, progress file), `services/array_status_cache.py`, `services/http_responses.py` (`NO_GZIP`), `ViewScope` + `Debounced` in `ui/components/reactive.py`, `LiveLog` in `ui/components/log_pane.py`, `UIStateManager.view_scope/job_scope`. main.py guard is `__name__ == "__main__"` only (spawned workers import it as `__mp_main__`).

**2026-10-17 first run:** watchdog caught a startup recovery scan (fixed, thread) and lazy-import compiles — main.py had `sys.dont_write_bytecode = True`, pycaches stale since Aug; now `pycache_setup.py` → ~/.cache/crboost/pycache. Open: §8 job + sub-tab switches still 1–2 s; user starts a fresh session from the roadmap.

**2026-10-17 second/third runs:** fixed (NOT RUN) scipy.signal imported on the loop by the first Journey/Tomograms open (recon_register; now imported in the worker), the roster's double render on workspace build, the wall's full re-render on a size switch. Open: Journey first paint ~0.8 s. Watchdog end line now names where a stall went (`file:function ×n`). `sys.pycache_prefix` ignores the cluster's own `__pycache__`, so each third-party module compiles once per user; pyc mtimes under ~/.cache/crboost/pycache date each module's first import — useful for finding lazy imports.

**How to apply:** first runtime question is what the watchdog logs; leftovers in roadmap §7 (analytics computes still take live ProjectState in threads; Journey pane render reads; registry re-sync on loop; template_viewer volume load; S7 bindings). See [[feedback_scope_design_talk_into_roadmap]].

**2026-10-17 later:** user: the watchdog "logging a lot and i dont know if any of that matters"; the app "DID get significantly quicker". Made quiet by default (NOT RUN): only the end line, only for stalls > 1 s; `CRBOOST_TRACE_STALLS=1` = the old 250 ms threshold + stack dumps (use it for §8.1 captures). Default mode silent = nothing worth chasing.

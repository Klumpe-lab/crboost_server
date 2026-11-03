---
name: project-render-registry-2026-10-28
description: "2026-10-28 on main (uncommitted): services/renders.py + RenderSlot skeletons replace every auto-kick; Journey header cleanup; layers list one table; registry responsive pass; NOT RUN in a browser"
metadata:
  type: project
---

Built 2026-10-28 on main, ruff-clean, imports + worker + NiceGUI-simulation tests pass; NOT RUN in a browser.

- `services/renders.py`: `Render(key, fn, args, outputs, sources, current, after, kind, in_thread, quiet, progress)`;
  `ensure` / `wait` / `restart` / `error` / `ensure_soon`. In-flight = real tasks only; failures remembered until
  restart (no auto-retry); `_made_from` source signature so a compute-node clock skew can't make a fresh picture
  stale. Tray via BackgroundTaskRegistry unless `quiet` (wall tiles, filter variants). `WorkKind.TILE` (cap 2).
- `ui/components/render_slot.py`: `RenderSlot` (dashed skeleton, breathes while running, red on failure, ↻ restart
  with `click.stop`, fills in place; `on_landed` for views that must re-collect; `failure=`; `fail()`),
  `keep_current(render, owner, on_landed, on_failed=)`.
- Converted: Particles canvas slabs, list cutouts, filter variants, CE preview pass (+ red header mark on failure),
  IMOD overlays (stamp `.made_from.json`, failure on the 3dmod command icon), Reconstruct denoised pane + compare
  (`matched_render`), wall tiles + zoom (`xy_slab_render(tile=True)`), tilt thumbnails (`thumbnail_render_key`),
  template viewer volume reads (thread), full-size tilt viewer (worker). Render previews / Re-render all / IMOD
  buttons and every `_AUTO_KICKED_*` set + `reset_auto_kick_state` deleted.
- Journey: full viewer ↗ link + polarity (Particles header switch AND Reconstruct card chips) removed — the
  latter was my reading of "the header"; "N cutouts" only when it differs from the list count.
- Layers list = one HTML `<table>` (all rows share columns). Registry responsive pass by a subagent:
  `_REGISTRY_CSS` + container queries (rail stacks ≤820 px; tables scroll sideways <~600 px).
- Foreign edit seen: backend.py + check_boundaries.py got `=`-aligned at 12:14 by something else; told the user.

**Why:** user: placeholder text hangs forever, kicks re-run for existing previews, three operational buttons.
**How to apply:** runtime check owed; the user announced per-tab layout comments for the registry next.
Related: [[project_ui_latency_roadmap34]], [[project_pane_focus_particles_tools_2026-10-27]].

---
name: project_delete_and_spinner_fix_2026-07-30
description: "Fixed delete-fails-first (\"Pipeline has no jobs\") + non-animating braille spinner; both code-clean PENDING RUNTIME"
metadata: 
  node_type: memory
  type: project
  originSessionId: 5cfff75c-4889-4769-8284-9a0a23de639b
---

Two bugs fixed 2026-07-30 (both code-clean, PENDING RUNTIME — server has no auto-reload, user must restart `python main.py` + hard-reload browser; see [[reference_hpc_env]]).

## Delete-fails-first ("Delete failed: Partial failure: External/jobNNN/: Pipeline has no jobs")
Root cause = `conf.yaml use_afterok_orchestrator: true` is GLOBAL now, so EVERY project's jobs live only in ProjectState and are NEVER written into `default_pipeline.star` (its process table is absent/empty). `pipeline_deletion_service.delete_job` hit the `graph.processes.empty` guard and hard-failed BEFORE reaching the afterok branch (`not job_mask.any()`) that trashes the dir + returns success. First delete always failed; retry "worked" only because `project_service.delete_job` had already popped the instance from state (unconditionally, even on error), so the 2nd call took the empty-`job_paths` early return (→ the ghost yellow/queued flash).
**Fix (pipeline_deletion_service.py ~186-215):** collapsed graph-None / processes-empty / job-not-in-star into one `job_in_star` check; when false → move dir to Trash + return success. Pruning path unchanged. No regression for schemer projects (matches old afterok branch semantics). Left `project_service.delete_job`'s pop-on-error and `preview_deletion`'s empty-guard untouched (surgical; preview failure is handled gracefully by the UI = "No downstream jobs").
**Recurring gotcha:** the afterok global toggle makes ALL star-reading code (also `sync_all_jobs`, which early-returns `{}` for afterok) unreliable — jobs are ProjectState-only.

## Running "spinner" → FINAL = inline SVG/SMIL pulsating dot (after 3 CSS dead ends)
THREE CSS approaches ALL rendered blank/static at runtime for the user: (1) inline `animation:cb-braille-rotate` glyph — static; (2) `.cb-braille-spin::before` animated `content` — blank; (3) `.cb-dots` = real `<i>` children sized+animated by a CSS class — blank. **Decisive clue:** the ORIGINAL inline `⠋` TEXT *did* show (proves the status binding + v-html rendering pipeline works and reaches RUNNING) — only the CSS-dependent visuals vanished. Root cause = the Python-injected `<style>` (ui/main_ui.py `ui.add_head_html`) is served CACHE-STALE, so ANY newly-added CSS rule (`::before` content, `.cb-dots > i` sizing) never applies → elements exist with zero size/no glyph → invisible. (Do NOT trust "the .pulse-* dots animate" — never runtime-confirmed; likely also stale.)
**FIX = `ui/status_indicator.py::_running_spinner_html(size_px, color)`** — a self-contained inline `<svg><circle><animate attributeName=r .../><animate attributeName=opacity .../></circle></svg>`. SMIL carries the shape AND motion IN THE MARKUP → zero stylesheet dependency → immune to the cache problem, and renders through the same v-html path the `⠋` used. Worst case (SMIL disabled) = a visible static dot, never blank. Wired at all 3 sites: pipeline_roster `_status_widget` RUNNING branch (14px) + sidebar run-slot (20px) + array_task_tracker placeholder (40px). Dead `.cb-dots`/braille CSS removed from main_ui.py.
**LESSON:** for a moving indicator inside a NiceGUI `ui.html(...)` / `bind_content_from` (v-html) span, use INLINE SVG+SMIL, not any newly-added CSS `@keyframes`/class/`::before` — the injected stylesheet caches stale and new rules silently no-op. Raw markup (text, SVG) always renders; CSS rules may not.
**Also removed** the `⏳` hourglass from sidebar SLURM-queue text (pipeline_roster `update_status_label`).
**Runtime note:** still needs server restart to pick up the Python change; SVG needs no stylesheet so a hard reload isn't required for IT, but other edits do.

## Index snapshot (detail moved out of MEMORY.md, 2026-08-10)

"Pipeline has no jobs" delete-fails-first = global afterok toggle → jobs never in default_pipeline.star → deletion empty-guard hard-failed before the trash-and-succeed branch. Spinner: 3 CSS approaches (rotate-glyph, ::before/content, .cb-dots class) ALL rendered blank — root cause = Python-injected <style> served CACHE-STALE so new CSS rules silently no-op (original inline ⠋ TEXT showed → pipeline works). FINAL FIX = status_indicator._running_spinner_html = inline SVG+SMIL pulsating dot (animation IN the markup, zero stylesheet dep) at row/sidebar/tasks-placeholder. Removed ⏳ hourglass. LESSON: moving indicator in a v-html span = inline SVG/SMIL, NEVER newly-added CSS keyframes/class/::before.

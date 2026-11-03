---
name: reference-nicegui-head-html-not-shared
description: "NiceGUI 3.0.3 — module-level ui.add_head_html without shared=True goes to a never-served pseudo client; likely the real cause of the \"stale shell\" CSS (code-read only, unverified)"
metadata:
  node_type: memory
  type: reference
  originSessionId: 8f7e2753-360e-4d53-ac07-7a225abddb9f
  modified: 2026-10-12T19:50:17.918Z
---

In NiceGUI 3.0.3, `ui.add_head_html(code)` called before the app starts and outside a page lands in a script-mode
pseudo client (`nicegui/context.py:18-25`, `nicegui/functions/html.py:11-17`); real page clients start with an
empty `_head_html` (`nicegui/client.py:93`, `:131`), so the HTML never reaches served pages. `main.py:187` (main.css
link, fonts) and `ui/main_ui.py:26` (the `<style>` block) do this. CLAUDE.md attributes the missing rules to a
stale page shell; this is the more likely cause. Fix would be `shared=True`.

**Unverified at runtime** (found 2026-10-12 while surveying branching). Check: page source contains
`main.css?v=` / `cb-pulse`? If not, confirmed. Don't change CLAUDE.md's rule until the user confirms.
Related: [[project-branching-pipeline-survey]].

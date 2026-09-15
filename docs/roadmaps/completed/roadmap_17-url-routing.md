# Roadmap 17 — Addressable URLs: `/p/<project>/<view>/<target>`

**Status:** scoped 2026-09-07. **S0–S5 code-complete 2026-09-08 — PENDING RUNTIME (§7).**
**Risk:** low-medium. No model change, no migration, no new persistence. Everything it needs to
*navigate* already exists as callbacks; what is missing is the address bar.

## 1. The concrete goal

> "I would like to be able to reproduce a link to a given project and tomo and navigate to this
> tuple's particular job type, journey, etc."

One sentence: **paste a URL → land on that project, that view, that target.** And the inverse:
**click around → the address bar always shows a URL that reproduces where you are.**

Success looks like this, end to end:

1. Open a project, switch to the Journey, click `TS_042` → the address bar reads
   `/p/HIV_Tomo_Batch1/journey/TS_042`.
2. Copy that, send it to a colleague on the same server, they paste it → the project loads, the
   Journey builds, `TS_042` is selected.
3. Same for a job tab (`/p/<proj>/job/tsReconstruct/logs`), a species tab, the pick viewer, the
   gallery and the Protocols view.

Explicit non-goals: no per-plot / per-scroll-position state, no URL state for dialogs, no
server-side session sharing, no permalinks that survive a project being moved on disk.

## 2. Why this is not hard (and where the actual work is)

The workspace is **one page** (`ui/main_ui.py:180` `@ui.page("/workspace")`) that builds six sibling
containers and swaps them with CSS `display` (`ui/workspace_page.py:78` `_switch_to`). Every view
already has a *programmatic entry point*, registered into one `callbacks` dict:

| Target | Entry point | Registered at |
|---|---|---|
| pipeline view | `ensure_pipeline_mode()` | `ui/workspace_page.py:276` |
| a job tab | `panel.switch_tab(instance_id)` | `ui/pipeline_builder/pipeline_builder_panel.py:232` |
| a job subsection | `panel.switch_to_job_subsection(instance_id, tab_key)` | `…panel.py:241` |
| species page | `open_species(species_id)` | `ui/workspace_page.py:275` |
| species page tab | `species_select_tab(key)` | `ui/species/page.py:145` |
| journey | `toggle_journey()` | `ui/workspace_page.py:277` |
| journey on a TS | `journey_show_ts(ts, section)` | `ui/tomo_dashboard_dialog.py:575` |
| tomogram gallery | `toggle_gallery()` | `ui/workspace_page.py:278` |
| pick viewer | `open_pick_viewer(species_id, tomo_name)` | `ui/workspace_page.py:280` |
| protocols view | `toggle_protocols()` | `ui/workspace_page.py:279` |

So the deep-*navigation* machinery is done. The three missing pieces are:

1. **Routes that carry a project.** `/workspace` reads the project out of `app.storage.tab`
   (`ui/ui_state.py:550`) — the URL says nothing. A URL-addressed page must resolve a project from
   its path segment and load it (`ui/open_project.py:15` `open_project_in_workspace` already does
   the load; it just ends in a `navigate.to`, which the routed version must skip).
2. **A parser/serialiser** between a URL and a `(view, target…)` tuple.
3. **Writing the URL back** as the user clicks. This must go through the History API
   (`history.replaceState`), **never** `ui.navigate.to` — a navigate rebuilds the page and throws
   away the lazily-built Journey/gallery/viewer panels (`_journey_built`, `_refs["gallery_page"]`,
   `_refs["viewer_page"]`), which is exactly the cost the CSS-display design exists to avoid.

## 3. The URL scheme (decided 2026-09-07)

**Project identity = the project directory name.** No new id, no schema change, no index file.
Ambiguity (two bases holding a same-named project) is resolved by an optional `?base=` query param,
and is *reported*, never guessed.

```
/                                          landing (unchanged)
/p/<project>                               workspace · pipeline · last-active job
/p/<project>/job/<instance_id>             pipeline · that job's tab
/p/<project>/job/<instance_id>/<tab>       …its subsection: config | logs | files (+ tasks on array jobs)
/p/<project>/journey                       journey · first TS
/p/<project>/journey/<ts_name>             journey on that tilt-series      (?s=<section> scrolls)
/p/<project>/tomograms                     tomogram gallery
/p/<project>/species                       species page · last species
/p/<project>/species/<species_id>          species page on that species
/p/<project>/species/<species_id>/<tab>    …its tab: overview | templates | picks | jobs
/p/<project>/picks/<species_id>/<ts_name>  full-page pick viewer
/p/<project>/protocols                     protocols view
```

Query params (all optional): `?base=<abs dir>` — disambiguate/locate the project;
`?s=<section>` — journey section to scroll into view.

`/workspace` **stays** and becomes a redirect to `/p/<dirname>` for the project in tab storage (or
to `/` when there is none), so every existing `ui.navigate.to("/workspace")` in the codebase keeps
working through the whole migration. There are 6 of them
(`ui/main_ui.py`, `ui/open_project.py`, `ui/data_import_panel.py`, `ui/pipeline_builder/pipeline_roster.py`).

### Project resolution order

`services/project_resolve.py :: resolve_project(name, base=None) -> ResolveResult`

1. `base` given → `Path(base)/name`.
2. The tab's currently-loaded project, if its directory name matches (a no-op re-entry).
3. `prefs.project_base_path / name`.
4. Each `prefs.recent_project_roots[*].path / name`.
5. The config default project base.

First candidate holding a `project_params.json` wins. **Two or more distinct hits** → do not guess
(CLAUDE.md, *Surfacing uncertainty*): render a small chooser listing the candidate absolute paths,
each linking to `/p/<name>?base=<its base>`. **Zero hits** → land on `/` with a toast naming the
name and the bases that were searched.

Every segment is percent-encoded on write and unquoted on read: project directory names may contain
spaces; `instance_id` carries `__` (`templatematching__ribosome`) which is already URL-safe.

## 4. Stages

### S0 — Inventory (no code) — DONE 2026-09-08

Grepped, not guessed. Two corrections to the scoping draft:

- **job subsection keys** = `config`, `logs`, `files` (`MonitorTab`, `ui/ui_state.py:17-20`) plus the
  extras a plugin registered for that job type (`get_extra_tabs`). The only registered extra today is
  `tasks`, on the seven array jobs (`ui/job_plugins/array_tasks.py`, `ui/job_plugins/ts_reconstruct.py`).
  There is **no `io` or `slurm` tab** — I/O and SLURM Resources are `ui.expansion` blocks *inside* the
  Config tab (`job_tab_component.py:107-118`), so they are not addressable and the §3 line was wrong.
  `workbench` in `ui/job_plugins/__init__.py:12` is a docstring example, not a registration.
  → `ui/routing.py :: _job_tab_keys()` reads both sources at validation time rather than listing them,
  so a plugin registering a new tab is addressable the day it lands.
- **species tab keys** = `overview`, `templates`, `picks`, `jobs` (`ui/species/page.py:44-49`, `TABS`).
- **journey section keys** = `dataset`, `fs_ctf`, `tilt_filter`, `ts_align`, `ts_ctf`, `tilt_qc`,
  `reconstruct`, `particles` (`_DASHBOARD_PANEL_KEYS`, `ui/tomo_dashboard_dialog.py:93-102`; the same
  keys are the `data-section` attributes `_scroll_section_into_view` queries).
- **async callbacks**: `toggle_journey`, `toggle_gallery`, `toggle_protocols`, `open_pick_viewer` —
  **and `journey_show_ts`**, which the draft listed as sync. Sync: `ensure_pipeline_mode`,
  `toggle_workbench`, `open_species`, `species_select_tab`, `open_job`, `open_job_subsection`.
- `switch_to_job_subsection` was **not** in the `callbacks` dict (only `open_job` was); registered as
  `open_job_subsection` (`pipeline_builder_panel.py`).

### S1 — Route model + project resolver (pure, no UI)

- `services/project_resolve.py` — `resolve_project()` per §3, returning
  `ok(path=…)` / `err(...)` with `code` for *not found* vs *ambiguous* plus the candidate list
  (`services/result.py` idiom).
- `ui/routing.py` — a frozen `Route` dataclass (`project`, `base`, `view`, `a`, `b`, `section`),
  `parse_route(project, segments, query) -> Route` and `route_to_path(route) -> str`.
  Round-trip is the contract: `parse_route(route_to_path(r)) == r` for every view.

**Verify:** `ruff check .`; read the round-trip by hand for all 11 URL shapes in §3.

### S2 — The routes exist (deep links work; the address bar is still static)

- `ui/main_ui.py`: four page handlers — `/p/{project}`, `/p/{project}/{view}`,
  `/p/{project}/{view}/{a}`, `/p/{project}/{view}/{a}/{b}` — all delegating to one
  `_open_routed_workspace(client, route)`:
  1. `await client.connected()`; resolve the project; on failure render the chooser / redirect to `/`.
  2. Load it into this tab (the body of `open_project_in_workspace` minus its `navigate.to` —
     factor that out rather than duplicating it).
  3. `build_workspace_page(backend)`.
  4. `ui.timer(0.05, lambda: apply_route(callbacks, route), once=True)` — `build_workspace_page`
     must return (or stash) its `callbacks` dict for this; today it returns `None`.
- `ui/routing.py :: apply_route(callbacks, route)` — one `match route.view` dispatching onto the
  table in §2. Unknown view → pipeline + a toast naming the bad segment.
- `/workspace` becomes the redirect described in §3.

**Verify (runtime):** paste each of the 11 URLs into a fresh tab; each lands on the right view with
the right target selected. A bad project name lands on `/` with a named toast. A name present under
two bases renders the chooser.

### S3 — The URL follows the UI

One helper, `ui/routing.py :: set_url(route)`:

```python
ui.run_javascript(f"history.replaceState(null,'',{json.dumps(route_to_path(route))})", respond=False)
```

Call it from the places that already own a view change — `_switch_to` (`ui/workspace_page.py:78`),
`switch_tab` / `switch_to_job_subsection`, the journey's `select_ts`, the species page's
`select_species` / `select_tab`, and `PickViewerPage.show`. Each of those knows its own target, so
each builds its own `Route`; nothing has to reach across views.

`replaceState`, not `pushState`: the browser Back button keeps meaning "leave the workspace" until
S4 makes it mean something better.

**Verify (runtime):** click through every view and target; the address bar tracks. Copy the URL
mid-session, open it in a new tab, land in the same place. Reload (F5) at any point → same place.

### S4 — Back/forward, and a copy-link affordance

- Swap `replaceState` for `pushState` on *user-initiated* view changes (keep `replaceState` for the
  route the page was opened with, so the history doesn't gain a duplicate entry on load).
- A `popstate` listener that re-dispatches `apply_route` for the popped URL, so Back/Forward move
  between views without a page rebuild.
- A copy-link button in the workspace header (`ui/components/copyable.py :: copy_button`) that copies
  the absolute URL of the current route.

**Verify (runtime):** Back after three view switches walks back through them without re-building the
workspace (the Journey does not re-show its "Loading journey…" spinner).

### S5 — Landing rows become real links (optional, small)

Each project row's chevron in `ui/projects_overview.py` becomes an `<a href="/p/<name>?base=…">`
wrapping the click handler, so ⌘-click / middle-click opens a project in a new browser tab. Pure
addition — the existing click handler stays for the plain-click path.

## 5. Risks and gotchas

- **`ui.navigate.to` rebuilds the page.** Any URL write that goes through it destroys the lazily
  built Journey/gallery/viewer. S3/S4 must use History API only. Grep for `navigate.to` before and
  after.
- **`app.storage.tab` needs a connected client.** Every routed page handler must `await
  client.connected()` first — same as the existing two pages.
- **Lazy views double-build.** `_show_journey` / `_show_gallery` / `_show_protocols` /
  `_open_pick_viewer` are each `SingleFlight`-guarded and idempotent; `apply_route` must go through
  them, never around them.
- **`_switch_to` toggles.** Calling `toggle_journey()` when already on the journey switches *back* to
  the pipeline (`ui/workspace_page.py:93`). Safe on load, **not** safe on a `popstate` — see §6b,
  `_enter()`. S3's `set_url` is a bare `history.*State` call and never re-enters the toggle.
- **A route can name something that doesn't exist yet** — a TS with no journey column, a species that
  was deleted, an `instance_id` not in this project. Every one of those must land on the view with a
  named toast, never a blank pane and never a silent fallback to "the first one"
  (CLAUDE.md, *Surfacing uncertainty*). `journey_show_ts` already does exactly this
  (`ui/tomo_dashboard_dialog.py:565`) — follow that pattern.
- **Project renames break links.** Accepted, and stated in §3. The `?base=` param does not help. If
  this becomes painful, the fallback is the `mnemonic` (already persisted, already stable across
  renames) as an *alternative* accepted identity — additive, no migration.

## 6. Modern-Python weave-in

`Route` as a frozen `@dataclass(slots=True)`; view names as a `StrEnum` so `route_to_path` and
`apply_route` cannot drift; `match route.view:` for the dispatch.

`ResolveResult` was scoped as a discriminated union but shipped as the house `ok()`/`err()` dict:
CLAUDE.md makes that the one idiom for a service-level outcome, and the two branches a caller
actually takes are already expressible as `ErrorCode.PROJECT_NOT_FOUND` / `PROJECT_AMBIGUOUS` — which
is exactly the documented bar for adding a code ("only when a caller genuinely branches on the
cause"). A second result shape for one function was not worth the exception.

## 6b. What was built (2026-09-08)

**New:** `services/project_resolve.py` (name → path, §3 order), `ui/routing.py` (`View` StrEnum,
`Route`, `parse_route` / `route_to_path` / `route_of_url`, `apply_route`, `RouteWriter`).
**Changed:** `ui/main_ui.py` (four `/p/...` handlers + chooser + `/workspace` redirect),
`ui/open_project.py` (`load_project_into_tab` split out of `open_project_in_workspace`,
`disambiguating_base`, `workspace_url`), `ui/workspace_page.py` (returns `callbacks`; owns the
writer, `_write_mode_url`, popstate), `ui/species/page.py`, `ui/tomo_dashboard_dialog.py`,
`ui/pipeline_builder/{pipeline_builder_panel,job_tab_component,pipeline_roster}.py`,
`ui/particles/pick_viewer.py`, `ui/projects_overview.py`, `services/result.py` (+2 `ErrorCode`).

Decisions taken while building, beyond the scoping:

- **push vs replace.** `pushState` when the *view* changes, `replaceState` when a view refines its
  own target and for the route the page opened with. So Back walks views (§7.12) and does not walk
  every tilt-series click. Consequence, stated rather than hidden: Back does **not** step through
  selections inside one view.
- **Coarse-then-fine URL writes.** `_switch_to` writes the bare view route; the view's own handlers
  (`select_ts`, `select_species` / `select_tab`, `switch_tab`, the viewer's species tabs) then
  replace it with the full one. That ordering is what makes push/replace come out right, and it is
  why the journey and Species page also write on *activation* — re-entering an already-built view
  fires no selection handler, so the target would otherwise be dropped from the URL.
- **`apply_route` is re-entrant.** The scoping assumed it only ever runs on a fresh workspace; S4's
  `popstate` runs it on a live one, where `toggle_journey()` would switch *away*. `callbacks[
  "current_mode"]` + `_enter()` make entering a view idempotent.
- **In-viewer species tabs.** `render_particles_section` gained `on_species_change`, so switching
  species inside the full-page viewer moves the address bar. Without it the URL would name the
  species the viewer was *opened* on while showing another — a link that lies.
- **`?s=` stays scroll-only.** It is not wired to the Journey's section *selector*: that is a
  persisted user pref (`prefs.dashboard_panel`), a different thing from "scroll this card into view".
- **Copy-link is a menu, not a bare copy button** (`RosterWidget._build_link_btn`, rail, above
  *Close project*). `navigator.clipboard` is secure-context only and this server is reached over
  plain http, where `ui.clipboard.write` `console.error`s and does nothing visible — so the menu
  shows the URL (selectable) with the copy icon as the fast path.
- **S5 chevron is a plain `<a href>`**, not an anchor wrapping the old handler. NiceGUI's event
  modifiers do not include Vue's `.exact` (`nicegui/event_listener.py:32`), so "prevent default on an
  unmodified click only" is not expressible; and with the routed page now doing the load, the browser
  navigation is the same code path the handler took. `ProjectsOverview._travel` went with it. The
  href always pins `?base=` (the roster lists whatever directory it was pointed at); the workspace
  drops the parameter on its first write when the name resolves on its own.

## 7. Runtime checklist (owed after S2–S4)

1. `/p/<name>` for a project under the default base → workspace opens on the pipeline.
2. `/p/<name>?base=<other base>` → the *other* project of that name opens.
3. `/p/<name>/job/<iid>/logs` → that job, Logs tab.
4. `/p/<name>/journey/<ts>?s=ctf` → journey, that TS, scrolled to the CTF card.
5. `/p/<name>/picks/<species>/<ts>` → pick viewer on that pair.
6. `/p/<name>/species/<species>/picks` → species page, Picks & curation.
7. `/p/<name>/tomograms`, `/p/<name>/protocols`.
8. A nonexistent project name → `/` + toast naming the searched bases.
9. A name under two bases → chooser with both absolute paths.
10. Click through all six views → address bar tracks; copy + paste into a new tab reproduces.
11. F5 on each of the above → same place, no console errors.
12. Back/forward (S4) → walks views without rebuilding the workspace.
13. The rail's link button (above *Close project*) → menu shows the absolute URL of the current
    route; it changes as you move between views; the copy icon works (or the text is selectable when
    the browser refuses the clipboard on plain http).
14. Landing roster: ⌘-click / middle-click a row's chevron → the project opens in a NEW browser tab;
    plain click still opens it in place.
15. `/p/<name>/job/<iid>/nosuchtab` and `/p/<name>/species/deleted-id` → the view opens with a named
    toast, never a blank pane.

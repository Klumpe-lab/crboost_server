# Roadmap 03 — Stage 0 census: `{"success": ...}` producers and consumers

Gathered 2026-08-12 (full-tree sweep). Companion to `03-errors-and-results.md`; this is the
migration worklist for stages 1/3/4. Line numbers are as of commit `4dc9013`.

## Totals

- **171 dict-literal producer sites** (57 True · 110 False · 4 variable) + 2 embedded-script
  sites (pdb_service PyMOL strings) + 2 `DeletionResult(success=…)` dataclass constructions.
- `main.py` and `drivers/` produce zero `"success"` dicts. Drivers use a parallel `{"ok": ...}`
  idiom (`drivers/extract_pick_list.py:105,130`) that `backend.py:359` re-wraps — fold into the
  same migration when those files are touched.
- A third failure idiom exists: per-item `{"reason": "..."}` records with no `success` key
  (`preview_render.py:319,324`, `recon_cutouts.py:122`, `cutout_filters.py:282`, `backend.py:556`).

## Failure-key aggregate (114 failure/variable sites)

| key | count | where |
|---|---|---|
| `error` | 107 | everywhere else — the de-facto standard |
| `message` | 5 | ALL in `pipeline_runner.launch_retries` (:873,:881,:884,:888,:904) — **the live bug** |
| none (`errors` list) | 2 | `pipeline_runner.py:1230,:1327` (`stop_and_cleanup`) |
| `detail` | 2 | `session_service.get_curation_session_info` :186,:222 — failure text under `success: True` |

## Producer sites per module (migration order, stage 3)

| module | sites | notes |
|---|---|---|
| `pipeline_orchestrator_service.py` | 12 | clean (all `error`); delegates to runner → inherits its shapes |
| `pipeline_runner.py` | 24 | worst offender: 3 shapes in one file (`error`/`message`/`errors`) |
| `backend.py` | 20 | `:1136` returns `{"error": ...}` with NO `success` key (`debug_pipeline_status`) |
| `project_service.py` | 18 | clean |
| `slurm_service.py` | 11 | clean |
| `template_service.py` | 20 | clean; `:206` success path returns mutated upstream dict |
| `pdb_service.py` | 21 (+2 in PyMOL script strings) | clean |
| `session_service.py` | 39 | largest; variable-success pass-throughs :616,:691 can yield `error: None`; `**info` spreads :750,:888,:900,:913 can shadow `success` |
| `pipeline_deletion_service.py` | 4 (+2 dataclass) | `DeletionResult.error` is a dead field (never populated) |
| `services/jobs/tilt_filter.py` | 2 | explicit `error: None` on success |

## Confirmed live/latent bugs (fix in stages 1/3)

1. **"Failed to start: None"** (live): `launch_retries` `message`-keyed failures returned verbatim
   via `pipeline_orchestrator_service.py:135` → `backend.py:78 start_pipeline` →
   `ui/pipeline_builder/pipeline_builder_panel.py:533` `result.get('error')` → renders `None`.
   → **fixed by stage-1 slice conversion.**
2. `stop_and_cleanup` failures carry only an `errors` list — any `.get("error")` consumer shows None.
3. `backend.debug_pipeline_status:1136` omits `success` entirely in one branch.
4. `session_service.py:616,:691` pass through `res.get("error")` (may be None) — upstream
   `send_chimerax_command:506` success dict has no `error` key, so the default path is live.
5. `get_curation_session_info` :186,:222 put failure text under `detail` with `success: True` —
   `ui/curation_session_dialog.py:423` reads `error` and can never show it.
6. Payload spreads `{"success": True, **info}` can be shadowed by keys inside `info`.

## Machine-readable codes found (ErrorCode seeds)

| wire string | producer | consumer branch | ErrorCode |
|---|---|---|---|
| `"no_coords_found"` (smuggled in `error` text) | `session_service.py:819` | `ui/tomo_dashboard_dialog.py:4106` `error == "no_coords_found"` | `NO_COORDS_FOUND` |
| `"No jobs selected."` (prose only) | `pipeline_orchestrator_service.py:71` | none today | `NO_JOBS_SELECTED` |
| `"upstream_particles_empty"` | `drivers/subtomo_extraction.py:171` sentinel JSON (not a success dict) | task-status readers | out of scope here (task-status roadmap) |

## Consumer census (72 `success`-key reads, 68 `error`-key reads — all accounted for)

### Cause-branching: exactly ONE literal error-string comparison in the repo

- `ui/tomo_dashboard_dialog.py:4106` (`_handle_import_curation_picks`):
  `result.get("error") == "no_coords_found"` → opens the manual-coords-path dialog.
  Producer: `session_service.py:819` (code smuggled in the `error` text field). The roadmap's
  `:4242` reference was stale — `no_coords_found` appears exactly twice in the codebase.
  **→ `ErrorCode.NO_COORDS_FOUND` (the only enum member stage 0 justifies).**
- Candidates that did NOT earn a code: "No jobs selected." (`pipeline_orchestrator_service.py:71`) —
  only consumer renders it verbatim, nobody branches; curation "nothing open" — that flow is
  `save_curation_picks` returning `success: True, count: 0`, branched at
  `ui/curation_session_dialog.py:547` on `count`, not an error at all.
- The only substring match on error text: `session_service.py:519-530` classifying the **ChimeraX
  REST envelope** (`cx_err.get("type") != "UserError"`, traceback sniffing) — external payload,
  out of scope.
- Zero reads or writes of a `"code"` key existed before `services/result.py`.

### Other branch-on-payload sites (legitimate, not error codes)

`status` strings from `get_curation_session_info` (`ready/pending/starting/ended`,
`curation_session_dialog._poll`); `already_complete` bool (`pipeline_builder_panel:523`, checked
before `success`); `downstream_count` on deletion previews (`job_tab_component:413`,
`pipeline_roster:700` — NOTE: a *failed* preview renders as "No downstream jobs will be affected",
indistinguishable from success-with-zero); manifest `row["status"]` literals in the dashboard.

### Hard-index `result["success"]` consumers (KeyError if producer omits the key)

`backend.py:1116` · `pipeline_orchestrator_service.py:718` · `pipeline_runner.py:1281,1437` ·
`project_service.py:279,443` · `template_service.py:135,147,199,310` · `pdb_service.py:276`
(relevant given `backend.py:1136` omits `success` — see bug 3 above).

### Silent-drop consumers (error text discarded; stage-4 targets)

- `ui/tomo_dashboard_dialog.py:3068` — dedup/clash stats failure → static "overlap check
  unavailable", error text dropped.
- `backend.py:1116` `get_eer_frames_per_tilt` — failure → bare `None`.
- `pipeline_runner.py:1383,1437` `cancel_job` — scancel failures logged at info, outer result stays
  `success: True` (UI never learns).
- `pdb_service.py:251` — replaces the underlying metadata error with a generic message.
- `session_service.py:576` — a per-list save failure is silently dropped from `saved`; outer
  result still `success: True`.

### "Failed: None"/blank render risks beyond M1

`ui/template_workbench.py` `_run` status strings (9 sites, e.g. :1728,:1767,:1934) render
`res.get('error')` with no fallback — `run_shell_command` failures with empty stderr render blank.
The paired `_on_complete` handlers already guard (`or task.error or "unknown"`).
Sites that already guard correctly are listed in the consumer sweep and need no change.

## Stage-1 slice conversions applied (2026-08-12)

- `pipeline_runner.launch_retries`: 5 `message`-keyed failures → `err(...)`, success → `ok(...)`.
  **Kills the live "Failed to start: None" bug at the producer.**
- `pipeline_orchestrator_service.deploy_and_run_scheme`: 3 sites → `ok()`/`err()`.
- `backend.start_pipeline` is a pure pass-through (no dicts of its own);
  `pipeline_builder_panel:533` already renders `result["error"]` — with the producers converted the
  contract now holds end-to-end on the start-pipeline path.

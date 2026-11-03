---
name: project_dashboard_renderer_twitch_fix
description: "Journey dashboard graphs \"twitchy/ugly/out-of-range\" — root cause is render_main full-rebuild (NOT Plotly); fixed in place with FingerprintedView gate + axis padding; renderer swap deferred"
metadata: 
  node_type: memory
  type: project
  originSessionId: 275d0325-408a-42b9-a950-c8809408b8a3
---

User (2026-07-01) said the dashboard graphs are "terrible" — twitchy, points fall outside the range, unappealing — and asked whether to swap renderer.

**Finding (multi-agent eval, workflow graph-renderer-eval): ~85% of the pain is architecture, NOT the Plotly library.** A renderer swap alone fixes almost none of it. Scored ranking: fix-in-place 8.0 > ECharts 7.4 > matplotlib-static 6.95 > uPlot 6.0. **User chose "fix in place, then reassess the look."** ECharts (bundled with NiceGUI, would drop the *undeclared* plotly dep) is the fallback if the de-twitched Plotly still looks bad. Full eval in the workflow result; renderer facts also in [[feedback_visualization_patterns]].

**Root cause of twitch:** `render_main()` in `ui/tomo_dashboard_dialog.py` did an unconditional `main_area.clear()` + full rebuild on every `refresh_all()`, which the 4 s `_maybe_refresh` timer fired whenever a *project-wide* signal moved (any bg-task start/finish anywhere, curation-save, session flip). Its sibling `render_strip()` was already signature-gated (`_strip_sig`); the main pane was not — so unrelated-TS activity tore down every Plotly chart (+ churned 2 `scattergl` WebGL contexts).

**Fix landed 2026-07-01 (code-clean ruff, PENDING RUNTIME — user must restart `python main.py`, no auto-reload per [[hpc-env-setup-for-running-python-ruff-in-this-project]]):**
1. **Axis padding** (`ui/dashboard/figures.py`): per-tilt locked y-range padded 5% of span; XY/XZ scatters padded 2% of dim (dashed reference rect kept at true `[0,dim]`). Fixes "points outside range" — library-agnostic. Also `displayModeBar:False` on scatter/hist/empty configs (per-tilt already had it).
2. **`render_main` FingerprintedView gate**: added `_main_signature()` = `(selected_ts, _journey_signature(...,[ts]) slice, ALL job execution_statuses, _tilt_filter_sig_for_ts(ts), hidden_panels, _CURATION_SESSION_LIVE, _curation_sig_for_ts(ts))`. `render_main(force=False)` early-returns when sig unchanged. Key design: **force/timer split** — 4 s timer calls `request_refresh(force_main=False)` (let pane self-gate); auto-kick `on_complete`, user toggles, and TS-selection use `force_main=True` (default) → always rebuild. This means landed artifacts (previews/slabs/manifests) do NOT need fingerprinting — the completion's `on_complete=refresh()` forces the rebuild. `_refresh_req` carries a coalesced `force_main` flag flushed in `_flush_refresh`.

**Adversarial review caught a real regression (fixed same session):** the first cut gated only on `_journey_signature`, which covers ONLY the 4 prep pill stages (`_PILL_STAGES[:4]` = fs_ctf/align/ctf/recon) + per-species picks. The **tilt_filter** section (and the standalone `<project>/TiltFilter/` tool, and secondarily the dataset/Import section) render from job state the journey never sees → a tilt-filter re-run could stay stale on the timer path. Fix = fold **all job execution_statuses** (in-memory, section-agnostic, catches any job-backed section incl. Import + future) + **per-ts tilt-filter labeled/filtered star mtimes** (the one section with a job-less standalone path) into the sig. LESSON: adding inputs to a FingerprintedView sig only ever causes MORE rebuilds, never more staleness — so a too-narrow gate is a staleness bug, and the safe direction when unsure is to over-include cheap in-memory fingerprints. job_states moves only on infrequent SLURM transitions (NOT background-task churn, which is UI-render tasks not job models), so no twitch returns.

**Why the gate can't be built from task ids** (Explore agent verdict): `BackgroundTaskRecord` has no ts field (id is a uuid; only `dedup_key` substrings), and the two most frequent auto-kicks — `render-previews:{job_dir}` and `imod-models:{job_dir}` — are job-scoped with NO ts. So gate on per-ts OUTPUTS, not tasks. The force/timer split sidesteps needing to fingerprint every artifact.

**Staleness risk to verify at runtime:** confirm on a LIVE/processing project that recon slabs / cutout galleries / previews still appear on completion, TS-switch still renders, and an external ArtiaX `.coords` save still auto-ingests (the `_curation_sig_for_ts` fold is what enables that on the timer path — see [[feedback_background_task_no_client_context]] lineage). No regression expected: silent SLURM job completion updates via journey-sig in the gate, same cadence as before (task-sig move or remount).

**Deliberately untouched:** the flex first-paint reflow (`.cb-plot-cell { flex: 1 1 320px }` → Plotly renders against intermediate width then reflows). Impact drops now that rebuilds are rare; changing the responsive layout risks regressions. Revisit only if still visible.

Next: after restart, reassess the Plotly LOOK. If still unappealing → ECharts swap (per-ts metric charts near-mechanical; XY/XZ scatter + score visualMap + XZ tomogram-PNG underlay is the real port risk). See [[project_preproc_metrics_inventory]] for the metric catalog these charts render.

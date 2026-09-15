# Arc runtime checklist — roadmaps 07, 08–12 and de-novo S5/S6

**One pass, at the end.** The maintainer lifted per-stage runtime gating on 2026-08-17, so every
stage in this arc landed on code-review confidence and this is where all of it gets exercised. Each
roadmap keeps its own longer checklist; this file is the ordered script that covers the arc without
repeating itself, newest and riskiest first.

Assistant-side verification ceiling for everything below: `ruff check .` and `ruff format --check`
on the touched files (both clean). There is no Python interpreter on the assistant's mounted path,
so **`python -m py_compile` and `python check_boundaries.py` are yours** and should run before the
app is started at all.

## 0. Static, before starting the app — ✅ DONE 2026-08-18

The maintainer ran all three on the cluster and they are green:

```
python -c "import main; print('import graph OK')"    # import graph OK
python check_boundaries.py                            # Boundaries clean (R1 R2 R3 R4)
ruff check .                                          # All checks passed!
```

`python -c "import main"` replaced `py_compile`: ruff parses all 192 files, so syntax was already
proven, and the module move's real hazard was import-time (no shims, 13 repointed sites). The uvicorn
call sits behind `if __name__ in {"__main__", "__mp_main__"}`, so importing walks the whole graph
without binding a port.

New cross-package imports `check_boundaries.py` should be asked about specifically:

- `services/particles/{species_overview,list_admin}.py` → `services.particles.list_ref`
- `services/particles/catalog.py` → `services.configs.config_service`, `services.project_state`
- `ui/particles/list_actions.py` → `services.background_tasks` (same idiom as `ui/background_task_tray.py`)
- `ui/species/{rail,overview_tab,page}.py` → `services.particles.catalog`
- `ui/aggregation/merge_card.py` → `services.aggregation.*` (the moved package)

Also confirm the app boots at all: the module move (`services/aggregation_*.py` →
`services/aggregation/`, `ui/aggregation_merge_card.py` → `ui/aggregation/merge_card.py`) repointed
13 import sites with **no shims**, so a missed one is an ImportError at startup, not a subtle bug.

## 1. Per-list extraction is a real job (roadmap 07 + its review follow-up)

Open a project with a species that has pick lists → Species page → **Picks** tab.

1. Extract one list. The `job` chip walks ◌ Queued → spinner → ✓ and the `ext` badge follows.
2. `scancel` the extraction so the driver fails: chip goes red ✕, the hover carries the driver's
   own `result.json` error, the tray card is RED (not green), clicking the chip opens run.out/run.err.
3. Re-extract a list while its own extraction is queued → confirm dialog → **a real second sbatch**
   (`squeue` shows a new id). This path produced no job at all before the fix.
4. **Stranded-instance recovery (the review's finding A — the highest-value check here).** Start an
   extraction, then restart the server (or Cancel the tray card) so its awaiter dies. Reopen the
   Picks tab: within ~15 s the chip must settle from Running to ✓/✕ by asking SLURM, and
   **"Extract all pending" must act on that list again** instead of reporting it as "still running"
   forever.
5. **Finding B:** let an extraction finish *after* its awaiter is gone. The reconciler must record
   `mark_extracted` — `ext` reads EXTRACTED without re-cutting anything.
6. Delete a list whose extraction is in flight: the confirm names the running job, and the SLURM job
   is cancelled before the directory goes (`squeue` empty afterwards).
7. Open the logs dialog ~15 times, then reload the page — no growing element tree / slow reconnect
   (finding E). With a >400-line run.out the `[… truncated N lines …]` marker must be VISIBLE at the
   top (finding F).
8. A tomogram where the CE found nothing must show the orange "no authoritative list" hint (finding
   D — it was suppressed there before).
9. Project hub: a FAILED per-list extraction must NOT make the project read "failed" and must not
   inflate its planned-job count.

## 2. Species page + Picks/Curation (roadmaps 08–11)

10. Change a species' colour → it changes in the roster chip, Journey tabs, strip and workbench (P-01).
11. Create a species from the roster "+" → it appears in the Species rail without a reload (P-05) and
    survives a server restart (P-06).
12. Overview identity editor: typing notes/Ø/symmetry does not save on every keystroke (P-02).
13. Picks tab: per-row extract / dedup / delete, the per-tomo merge bar, the auth radio, and
    "Extract all pending" behind its gate-report pre-flight.
14. Curation tab: live/off/**unknown** session status, the watcher log, unattributed saves.
15. Journey: merge ticks + inline merge bar, the extraction bar, clash/dedup panel, ⚡, and the
    "manage in Species ↗" route. The toolbox Curate / Import-all buttons should be GONE (11-S3).
16. ArtiaX: save a `.coords` with the Journey CLOSED — the watcher must still ingest it (P-07).
17. Job tabs: the pick-candidates form shows `array_throttle` exactly ONCE, in SLURM Resources (P-09).

**The 2026-08-18 (e)/(f)/(g) fixes** (11-S3's reported-not-fixed list; details in that stage log):

17a. **(e) invariant.** Every route that adds a PARTICLES-phase job must name a species. The
    PARTICLES "+" and the Species page's Jobs tab already ask; the gate now lives in
    `add_instance_to_pipeline`, so confirm a particle job cannot be created without one and that
    re-selecting an EXISTING particle job (clicking it back into the roster) still works untouched.
17b. **(f) Curation tab universe.** On a project whose tomograms come from a MERGE (or only from a
    CE job's own `tomograms.star`) rather than a local reconstruct job: the Curation tab must list
    the same tomograms as the Journey. It listed nothing there before.
17c. **(f) honest `has_geometry`.** A tomogram the species' CE star does NOT describe must not offer
    ⚡ / Curate / import as if it had geometry — the failure used to land later inside
    `prepare_curation_bundle`. Either the geometry provider supplies a star, or the row is inert.
17d. **(g) merge tick.** A species with a subtomo job, NO committed `particles_filtered.star` and no
    candidate-extract job: its `auto` row's merge tick must be drawn disabled (dashed, not-allowed)
    with the reason in the tooltip. Ticking it used to raise `ValueError` out of the click handler.
17e. **(a)–(d) perf.** Nothing to click — these are invisible when right. Watch for a REGRESSION
    instead: the Journey strip and main pane must still update on the 4 s tick, on a keep/drop Save,
    on a TS selection change and on an exclude toggle. `refresh_all` now collects once and hands the
    result to both renderers, so a stale strip or a pane that stops self-gating is the failure mode.

## 3. Tomogram import (de-novo S5)

18. Import a directory of `.rec` files — they must be found (the glob used to be `*.mrc`-only).
19. Import a second batch from a different source project. Both coexist in one
    `Tomograms/tomograms.star`, the dialog lists both batches with dates and counts, and the union
    shows in the Journey strip.
20. Force a collision (a tomogram name that already exists, and a file already imported): the toast
    must NAME the rename (`<name>` → `<name>__2`) and the skip. The EARLIER tomogram keeps its name —
    verify any picks made on it still resolve.
21. "Replace all" drops the prior batches (say so in the dialog, then confirm the star shrinks).
22. Re-open the dialog on a large recon directory: the status line counts up while probing, the table
    caps at 300 rows and SAYS how many it is not drawing, and the UI stays responsive.
23. Delete the imported star, then look at a job whose input was overridden to it: the error must name
    the imported tomograms, not "merged-sources optimisation_set not found".

## 4. Aggregation (de-novo S6)

24. **Regression, the important one:** reproduce an old aggregation-project merge on a REGULAR
    project. Same `MergedSources/<slug>/` output, same overrides written to the consumers.
25. The merge button is on the PARTICLES phase header of every project (not the sidebar), with a green
    dot when a merge exists.
26. Two browser tabs on the same project, both with the dialog open: selection, registry expansion and
    "add manual path" must not cross-talk.
27. Pre-flight: with a stale/un-extracted authoritative list selected, merging shows the blocker list
    and lets you proceed anyway.
28. Old projects that still have `"is_aggregation": true` in `project_params.json` open normally and
    behave like any other project.
29. `grep -rn is_aggregation --include=*.py .` → only comments/docstrings.

## 5. Species catalog (roadmap 12)

30. With `species_catalog_root` UNSET: no "From catalog" row, no "Publish to catalog" button, no
    errors anywhere. This is the state every existing install is in.
31. Set it to a shared directory. Publish a species that has templates + masks → `<root>/<id>/v1/`
    with `species.json`, `templates/`, `masks/` and the `.meta.json` sidecars, plus `catalog.json`.
32. Import it into a DIFFERENT project: files land in `templates/<sid>/`, the selected template is
    still selected (that only works because the sidecars travelled), and the Overview provenance line
    reads `catalog <id> v1`.
33. Publish again from the second project → **v2** appears; v1 is untouched byte-for-byte.
34. Try to publish a species whose template file you deleted → Publish is disabled and the missing
    file is named.
35. Merge card: both projects' copies show the `⌗ <catalog_id>` chip; clicking it selects every
    project holding that species; typing the catalog id in the filter finds them all.

## What to do with failures

Record them in the failing stage's own roadmap file (each has a stage log), not here — this file is
the script, not the record.

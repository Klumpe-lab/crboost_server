# S5 — Tomogram import hardening

Independent of S3/S4 (S2's geometry provider already reads the imported star). ~500 lines.
Goal: import is a real feature, not a dead end. Locked decision D-10: imported tomograms stay
**star-only** in this roadmap — no entity-registry entry; see `00-overview.md` §Registry interop
for the DataRegistry follow-up direction and its honest assessment.

## Current state (verified 2026-08-11)

Import was un-job-ified 2026-06-25 (`JobType.IMPORT_TOMOGRAMS` deliberately deleted). What exists:
`services/tomogram_import.py` (header probe; 13-column star synthesis; **raises** on missing voxel
size — correct), `backend.commit_imported_tomograms` (`backend.py:426-479`) →
`<project>/Tomograms/tomograms.star` + `ProjectState.ImportedTomograms`,
`ui/tomogram_import_dialog.py` (mrcs-glob + reference-star tabs), PARTICLES-header entry button
(`ui/pipeline_builder/pipeline_roster.py:1444`, disabled iff a TS_RECONSTRUCT job exists).

Known broken/stubbed: never runtime-verified end-to-end (dialog previously "laggy, terrible";
rework landed, never re-run); synthetic resolver candidate masquerades as
`producer_job_type=JobType.MERGED_SOURCES` (`services/path_resolution_service.py:633`); single-slot
(`set_imported_tomograms` replaces; no batch provenance); `.mrc`-only glob (`backend.py:404`,
dialog :278); candidate scores at `relion_job_number=0` and loses to any real producer; tomo-name
dedup only within one import call; no half-maps; TM-on-imported blocked (no
`rlnTomoTiltSeriesStarFile` — deliberate, stays out of scope).

## Work items

1. **Dedicated producer identity** (D-8): a dedicated sentinel for imported tomograms (constant or
   non-job `JobType`) ending the MERGED_SOURCES masquerade at `path_resolution_service.py:633` and
   in the override-key branches (:178, :400). Dangling imported candidates must surface like
   dangling merged ones (:174-181) instead of silently vanishing (:626-629).
2. **Multi-import batches**: `ImportedTomograms` → list of batches (source path, date, count);
   commit rebuilds the merged `Tomograms/tomograms.star` with cross-batch tomo-name dedup and an
   **explicit rename report** (never a silent merge — same bug class as
   `drivers/subtomo_merge.py:521`'s cross-project collision guard). Dialog shows per-batch
   provenance.
3. **Formats**: accept `.mrc` and `.rec` in the glob tab.
4. **Perf shake** (the "laggy, terrible" report): MRC header probes via `asyncio.to_thread`,
   batched; `FingerprintedView` on the preview table; re-run the dialog on Lustre-sized dirs.
5. **Deferred (D-9, out)**: half-map import slots for denoise-on-imported — separate follow-up,
   needs its own star columns and denoise-slot plumbing.

## Verification (user, runtime)

1. Two successive imports from different source projects coexist; union shows in the journey strip;
   rename report on collision.
2. Re-open the dialog on a large directory — responsive.
3. `grep` the resolver debug/report output: imported candidates carry the dedicated sentinel, and a
   deleted imported star surfaces as a dangling source instead of silently disappearing.

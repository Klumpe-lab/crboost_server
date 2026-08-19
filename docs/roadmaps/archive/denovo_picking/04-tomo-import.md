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

## Stage record

- 2026-08-18 — **S5 CODE-COMPLETE** (`ruff check .` / `ruff format --check` clean; `py_compile`,
  `check_boundaries.py` and the runtime pass above are owed, like the rest of the arc).

  **1. Dedicated producer identity (D-8) — done.** `JobType.IMPORTED_TOMOGRAMS` ("importedTomograms")
  joins `MERGED_SOURCES` as a synthetic, spec-less type (`_SYNTHETIC_DISPLAY_NAMES` gives it a name;
  nothing iterates `JobType` expecting a `JobSpec`). `_add_imported_tomograms_candidates` stops
  claiming to be MERGED_SOURCES and carries a real label ("Imported tomograms — N tomogram(s)"), so
  the resolver dropdown names the artifact instead of showing a merge that does not exist. A dangling
  imported override now gets `_dangling_imported_message()` in BOTH surfacing paths (`resolve_inputs`
  and `validate_input_slot`) instead of "merged-sources optimisation_set not found" — which named the
  wrong artifact and the wrong file type. Compatibility is read-time, not a migration: the legacy
  `mergedSources:Tomograms` override key is still recognised by `_synthetic_override_target` and
  rewritten inside `_resolve_override`, because an override lives on every job model that holds one
  and a load-time migration would miss any project that is only ever read.

  **2. Multi-import batches — done.** `ImportBatch` (source, geometry, count, `imported_at`, plus its
  own `renamed`/`skipped` report) is the new unit; `ImportedTomograms.batches` is the source of truth
  and `effective_batches()` folds a pre-S5 record into one synthetic batch, so an additional import
  cannot silently drop what was already there. `write_tomograms_star` now takes the batch LIST and
  rebuilds the whole star from it — a pure function of the batches, so dropping one later needs no
  in-place surgery. Cross-batch collisions are resolved and REPORTED, never merged silently:
  same recon file ⇒ the row is dropped ("already imported"); same tomo name, different file ⇒ the new
  row is renamed `<name>__2` and **the incumbent keeps its name**, because picks, curation saves and
  pick lists are keyed on it. `commit_imported_tomograms(replace=…)` keeps the old
  replace-everything behaviour as an explicit choice. A PRIOR batch that has become unreadable (its
  source directory moved) contributes zero rows and its error is returned under `batch_errors` rather
  than failing the commit — but the batch being added right now still raises, because that is the one
  the user can fix, and the "no silently defaulted apix" contract lives there.

  **3. Formats — done.** `TOMOGRAM_SUFFIXES = (".mrc", ".rec")`; `list_tomogram_candidates` filters
  both the directory and the glob branch through it. The directory widget composes `*` rather than
  `*.mrc` (a glob expresses one suffix; an etomo `.rec` directory used to read as empty), and
  `local_file_picker` learned comma-separated glob patterns (`"*.mrc,*.rec"`) since fnmatch has no
  alternation.

  **4. Perf shake — done, one deviation.** Header probes were ALREADY off the event loop
  (`asyncio.to_thread` in `probe_tomogram_metadata`), so the "laggy, terrible" report was the other
  two halves: one 3000-file await behind a motionless "Probing…" (now chunked at 100/await with a
  live count), and one DOM row per file (now capped at 300 rendered, with an explicit line saying how
  many are not drawn and that they are still selected and still imported — a silent cap would read as
  "that is all of them"). **`FingerprintedView` on the preview table was NOT built**: it gates
  repaints against a signature on a POLL, and nothing polls this table — it is rebuilt only by an
  explicit scan/browse/toggle. Adding one would have been ceremony around a rebuild that already
  happens exactly as often as it must.

  **5. Half-map slots (D-9)** — still deliberately out of scope.

- 2026-08-18 (same session, self-review) — **one defect found and fixed in the batch rebuild.**
  `write_tomograms_star` wrote the merged star and *then* the backend raised on the new batch's
  error, so a failed import had already rewritten the committed star from the prior batches —
  quietly dropping the rows of any prior batch that had since become unreadable. The
  fail-on-last-batch guard (`require_last=True`) now runs BEFORE the write, and the backend's
  duplicate raise is gone: a failed import leaves both the file and the recorded state untouched.

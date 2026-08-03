# Tilt-filter placement, tomostar/RELION split, and the three-representation duplication

**▶ NEXT SESSION (Stage 3) START HERE:** the tilt-filter move is **RUNTIME-VERIFIED**
(the compute honors the cut) and **the `emit_star` ghost-tilt gap is now CLOSED and
RUNTIME-VERIFIED** (2026-07-31 on `copiatest_postfilter`, all 16 TS — see §4 "THE GAP →
FIXED"): the `tsAlignment` + `tsCtf` ingest adapters drop `is_filtered_out` frames when
emitting per-tilt STARs, so the RELION stars now match the trimmed tomostar (per-TS counts
30/34/36/39/40 all matched, zero NaN-alignment rows) and the particle stage no longer
ingests ghost rows. The remaining work is the broader §5 collapse (registry-authoritative +
lazy star emit), whose two gates are (a) de-star-ify the IO graph and (b) main-process
registry freshness for the dashboard.

**Status:** Stage 1 **BUILT + RUNTIME-VERIFIED 2026-07-30** (`copiatest_postfilter`; compute
honors the cut, one `emit_star` gap open — see §4). Stage 2A built. Stages 2B/3 + §5 remain. Originally written 2026-07-29 from a
read-through of the preprocessing drivers + `services/tilt_series/`; the mechanics
below were re-verified against the live source before building (see §4/§6). Companion
to [`docs/architecture.md`](architecture.md) and the TS-registry refactor notes.

The immediate goal is to **move the tilt-filter as far upstream as possible so it
actually filters alignment/CTF/reconstruct, not just the display copy.** Getting
there forces us to confront a structural issue: the preprocessing pipeline carries
**three parallel representations of the same tilts**, and the tilt-filter currently
edits the one nobody upstream reads. This doc lays out the mechanics, the options
with tradeoffs, and the bigger de-duplication question to resolve.

---

## 1. The three representations

Every tilt exists, simultaneously, in three forms during preprocessing:

| # | Representation | On disk | Produced by | Consumed by | Authoritative for |
|---|---|---|---|---|---|
| 1 | **WarpTools native** | `tomostar/*.tomostar` + `warp_tiltseries/*.xml` (with `UseTilt` flags) | `tsImport` (tomostar, from **mdocs**); `aligntiltsWarp`/`tsCtf` (XMLs) | `ts_aretomo`, `ts_ctf`, `ts_reconstruct` — **the actual compute** | **which tilts get aligned/CTF'd/reconstructed** |
| 2 | **TiltSeriesRegistry** | `registry/tilt_series/{ts_id}.json` | `services/tilt_series/build.py` at import (from `DatasetOverview`/mdocs); adapters `ingest()` per-job outputs | adapters' `emit_star()`; dashboard; exclude-from-processing | *intended* single source of truth (only partially wired) |
| 3 | **RELION stars** | `fs_motion_and_ctf.star`, `aligned_tilt_series.star`, `ts_ctf_tilt_series.star`, `filtered/tiltseries_filtered.star`, `tomograms.star` | adapters' `emit_star()` (from the registry) | dashboard per-tilt plots; **IO-slot graph** (`JobFileType`); tilt-filter; **particle stage** (PyTOM/RELION/M) | RELION interop + typed inter-job wiring + display |

### The load-bearing fact

`tsImport` builds the tomostar **straight from the mdocs** (`WarpTools ts_import
--mdocs … --frameseries …`, see `drivers/ts_import.py:44-66`). It never reads a
`.star`. The registry is *also* built from mdocs at import (`build.py`). So repr. #1
and #2 are **parallel derivations from the same source**, and #3 is emitted from #2.

```
                         raw movies + mdocs
                                 │
                  ┌──────────────┼───────────────┐
                  │              │               │
         (WarpTools ts_import)   │        (build.py @ import)
                  ▼              │               ▼
          tomostar/*.tomostar    │        TiltSeriesRegistry
          warp_tiltseries/*.xml  │        (Frame / TiltSeries /
          [UseTilt flags]        │         Tomogram entities;
                  │              │         is_selected / is_filtered_out /
                  │              │         is_excluded)
   ┌──────────────┘              │               │
   │  alignment / CTF / recon    │   adapters.ingest(xml, tomostar)
   │  READ the tomostar + XML     │  adapters.emit_star()
   │  (the real compute)          │               │
   ▼                              │               ▼
Warp XMLs updated in place ───────┘        RELION .star files
                                           (fs_motion / aligned / ts_ctf / …)
                                                   │
                                    dashboard · IO-graph · tilt-filter · particle stage
```

**The tilt-filter today edits repr. #3 only** (`drivers/tilt_filter.py` reads a
`.star`, drops bad-tilt rows, writes `tiltseries_filtered.star`). Alignment reads
repr. #1. **The two never meet until the particle stage.** So a tilt-filter placed
before alignment would be silently ignored — alignment would process every tilt.

---

## 2. What actually consumes the RELION star, and when

This is the answer to "why do we even have a RELION track if nothing relies on it
until the particle stage?" — it's *mostly* right, with three real exceptions.

Within **preprocessing** (`fsMotionAndCtf → tsImport → aligntiltsWarp → tsCtf →
tsReconstruct`), the WarpTools tools do **not** consume the RELION tilt-series star
for their compute — they read the tomostar/XML. The star is threaded through for:

1. **The IO-slot graph.** `JobFileType` is defined in terms of stars
   (`FS_MOTION_CTF_STAR`, `ALIGNED_TILT_SERIES_STAR`, `TS_CTF_TILT_SERIES_STAR`,
   `FILTERED_TILT_SERIES_STAR`, …; see `services/io_slots.py`). Path resolution,
   `preferred_source`, `source_override`, orphan detection — the entire inter-job
   wiring — keys off these star artifacts. It is the pipeline's typed "currency."
2. **The dashboard / Journey.** Per-tilt metric plots read the per-tilt stars
   (`_read_per_tilt_df` → `tilt_series/<ts>.star`). All the defocus/CTF-res/motion
   panels, and now the tilt-filter datapoints, come from stars.
3. **Enumeration.** Several drivers read the star only to get the *list of TS names*
   (`ts_reconstruct.py:read_tilt_series_names_from_input_star`), then hand off to a
   WarpTools command that reads the tomostar/XML.

It becomes **genuinely load-bearing at the particle stage**: PyTOM template matching
consumes the per-tilt CTF+geometry star (`template_match.py` accepts
`[FILTERED_TILT_SERIES_STAR, TS_CTF_TILT_SERIES_STAR]`), denoise consumes
`TOMOGRAMS_STAR`, and RELION/M refinement is star/optimisation-set native. Plus the
standing "open the project in RELION directly" compatibility goal (`CLAUDE.md`).

**So the duplication is real: through preprocessing the RELION star is a shadow copy
maintained for wiring + display + a downstream handoff, while WarpTools does the work
off the tomostar/XML.** Section 5 weighs whether that's worth collapsing.

---

## 3. The tilt-filter move — options and tradeoffs

Goal: the filter runs as early as possible and its cut is honored by
alignment/CTF/reconstruct. It must stay **optional** (queue it = filter; don't =
run to completion — no orchestrator pause; decided 2026-07-29). The DL classifier
itself (`filterTilts.*`) is unchanged; only its I/O and position move.

Bridge that makes all options tractable: **every tilt is keyed by the same movie
file in all three reprs** — tomostar `_wrpMovieName`, star `rlnMicrographMovieName`,
registry `Frame.raw_filename`. Match by basename (the newly-shipped dashboard
datapoints already do exactly this).

### Option A — filter edits the tomostar directly (pragmatic, minimal)

- Position: **after `tsImport`, before `aligntiltsWarp`.**
- Input slots: `TOMOSTAR_DIR` (to edit) + `FS_MOTION_CTF_STAR` (to resolve the
  motion-corrected averages the DL reads).
- Action: classify tilts → **delete flagged tilts' rows from each `*.tomostar`**
  (keyed by movie basename) → write a trimmed `tomostar/` as output.
- Downstream: `aligntiltsWarp` and `tsImport`-consumers prefer the filtered
  tomostar (`preferred_source="tiltFilter"`, single-fallback → no regression when
  the filter is absent, same safe pattern as `tsReconstruct`'s dual-accept).

**Pros:** small, self-contained (~a tomostar text-editing util + a slot re-point);
effective immediately; no registry dependency.
**Cons:** introduces a *fourth* place selection state lives (the trimmed tomostar),
deepening the duplication rather than reducing it; the registry and the RELION stars
stay ignorant of the cut unless separately updated (dashboard "dropped tilts" and the
emitted stars would need to re-derive it); the `UseTilt` XML flag is bypassed (we
delete rows instead of flagging — fine for alignment, but two different "exclude"
mechanisms now coexist).

### Option B — filter sets registry state; tomostar + stars derive from it (correct)

- Add `Frame.is_filtered_out: bool` (+ `filter_reason`) to
  `services/tilt_series/models.py`. (The model *comments* already reference a
  "frame-level tilt-filter concept" as distinct from TS-level `is_excluded`, but the
  Frame flag doesn't exist yet — this is the missing piece.)
- The filter writes its verdict into the **registry**, not a star.
- **tomostar generation honors it.** This is the hard part: today the tomostar is
  emitted by `WarpTools ts_import` from mdocs. To make the registry authoritative we
  either (i) post-process the tomostar to drop `is_filtered_out` frames (same text
  edit as Option A, but driven by the registry flag), or (ii) longer-term, emit the
  tomostar *from the registry* instead of calling `ts_import` (see §5).
- Star emission (`adapters.emit_star`) and the dashboard both read the flag → one
  source of truth, no drift; exclude/filter/select all live together.

**Pros:** collapses selection state to one place; aligns with the TS-registry
refactor endgame; the dashboard, stars, and tomostar can't disagree; `is_excluded`
(TS-level mute) and `is_filtered_out` (tilt-level) become siblings under one model.
**Cons:** bigger; touches the registry model + adapters + the tomostar path; the
registry is currently *downstream* of tomostar (ingested from it), so making it
*drive* tomostar is a directional change; higher blast radius, needs runtime testing.

### Recommendation

Ship **Option A first** (unblocks the actual goal with contained risk), but implement
its tomostar-trim as a small reusable util and have the filter **also stamp
`Frame.is_filtered_out`** in the registry even in Option A — so the data is already
centralized and Option B becomes "make the emitters read the flag" rather than a
rewrite. Explicitly a two-stage path toward §5.

---

## 4. Staged roadmap

**Stage 0 — done (2026-07-29):** tilt-filter per-tilt datapoints on the TS-CTF plots
(hover keep/drop + probability, "DL keep N/M" strip); establishes the movie-basename
bridge in the dashboard. Position/behaviour unchanged.

**Stage 1 — tomostar-trim util + Option A move. ✅ BUILT 2026-07-30** (code-clean,
PENDING RUNTIME). The 9 edits:
- **New util** `drop_tilts_from_tomostar(src_dir, out_dir, bad_movie_stems)` in
  `services/tilt_series_service.py` — a *text-based* row filter (copies kept rows
  verbatim so relative `_wrpMovieName` paths + whitespace survive; the only tomostar
  consumer, alignment, re-parses to absolute paths anyway). Match is by `Path(name).stem`
  so a `foo_EER.eer` movie matches the `foo_EER` key the DL emits.
- **`services/jobs/tilt_filter.py`** — INPUT `[FS_MOTION_CTF_STAR@fsMotionAndCtf,
  TOMOSTAR_DIR@tsImport]`, OUTPUT trimmed `TOMOSTAR_DIR` (`tomostar/`). Dropped the
  post-CTF `input_processing`/`output_processing` warp slots and the
  `FILTERED_TILT_SERIES_STAR` output.
- **`drivers/tilt_filter.py`** — DL still reads the per-tilt averages via the star
  (now the fsMotion star; identical `rlnMicrographName` averages, so the classifier
  sees the same images); still writes the labeled + filtered stars **for the dashboard**;
  NEW step trims the tomostar by the DL verdict (`cryoBoostDlLabel != "good"` → stems);
  removed the warp-symlink step.
- **`services/jobs/ts_alignment.py`** — `tomostar_dir` slot `preferred_source` →
  `tiltFilter`. Verified safe: the resolver (`path_resolution_service._choose_candidate_for_slot`)
  scores `pref=1` for a tiltFilter `TOMOSTAR_DIR` candidate when present, else all
  candidates score `pref=0` and the sole producer (tsImport) wins → **no-filter pipeline
  byte-identical**. No manual `source_override` needed (unlike missAlign, whose
  `WARP_TILTSERIES_DIR` is multi-producer/ambiguous — `TOMOSTAR_DIR` has only two
  producers).
- **Order:** `PIPELINE_ORDER` (`ui/ui_state.py`) + `PHASE_JOBS` moved `TILT_FILTER`
  before `TS_ALIGNMENT`. Gating is **positional** — the RELION scheme is a linear
  chain built in PIPELINE_ORDER sequence (`_write_scheme_star` emits `job[i]→job[i+1]`
  edges), so alignment runs after tiltFilter when present.
- **`JOB_DEPENDENCIES`** `TILT_FILTER: [FS_MOTION_CTF]` (the DL's real input); tsImport
  rides in via **`_PREREQUISITES`** (`pipeline_builder_panel.py`) — I added
  `TILT_FILTER: TS_IMPORT` there, mirroring how `TS_ALIGNMENT` auto-injects the hidden
  tsImport rather than declaring it as a JOB_DEPENDENCIES edge.
- **`ts_reconstruct.py` / `template_match.py`** — reverted `input_star`/`input_tiltseries`
  to `[TS_CTF_TILT_SERIES_STAR]@tsCtf` (the cut now propagates natively; the old
  `FILTERED_TILT_SERIES_STAR` source is dead).
- **Verify at runtime (unchanged asks):** (a) with a queued tiltFilter, alignment/CTF/
  reconstruct process only kept tilts; (b) a no-filter pipeline is byte-identical to
  today; (c) the dashboard tilt-filter keep/drop panel still renders (it reads the
  labeled/filtered stars the driver still writes); (d) an empty tomostar (all tilts
  dropped for a TS) — alignment already tolerates per-TS failure, but confirm it degrades
  cleanly.

**Stage 1 RUNTIME FIX 1 — the interactive UI panel (2026-07-30, code-clean PENDING
RUNTIME).** First real-project run (`copiatest`) surfaced that the backend rewiring
above wasn't enough: `tiltFilter` is `IS_INTERACTIVE`, so it does NOT run via the SLURM
driver in the normal flow — the **UI panel** (`ui/tilt_filter_panel.py`) is the executor
(gallery → manual/DL labelling → commit). That panel was still 100% wired to the old
post-CTF design and stayed stuck on "Run the pipeline through TS CTF first." Fixes:
- **Accessibility:** both panel entry points now resolve the **fsMotion star**
  (`_find_fs_motion_star`) instead of `_find_ts_ctf_star`; the DL sees identical averages.
  Waiting message updated.
- **Commit produces the real output:** a shared `_finalize_pipeline_output()` runs at
  BOTH commit paths (DL-assisted + manual `_save`). It trims the tsImport tomostar
  (`_find_tsimport_tomostar_dir`) → `TiltFilter/tomostar/`, sets
  `job_model.paths["output_tomostar"]`, stamps the registry, and drops the stale
  `output_star`/`output_processing` sets. The manual path never dispatches the driver, so
  the trim MUST live in the UI too.
- **Resolver wiring for a job with no deployed dir:** the committed interactive job has
  `relion_job_name=None`, so alignment resolves its tomostar via the producer's cached
  `paths["output_tomostar"]` — confirmed at `path_resolution_service._get_producer_output_path`
  (cached-paths fallback) + the SUCCEEDED-only gate for interactive producers.
- **Absolute movie paths (`drop_tilts_from_tomostar`):** the trimmed tomostar is written
  with absolute `_wrpMovieName` — the interactive filter writes to `TiltFilter/` (depth 2)
  vs the driver's `External/jobNNN/` (depth 3), so verbatim `../../` paths would break.
  Validated on `copiatest`: the absolute form is byte-identical to what alignment's
  existing `copy_tomostar_with_absolute_paths` already computes (`_wrpMovieName` is a
  WarpTools logical ref — the real `.eer` lives in `frames/`, not `warp_frameseries/`).

**Stage 1 RUNTIME VERIFIED + one real gap found (2026-07-30, project `copiatest_postfilter`).**
End-to-end run import→fsMotion→tsImport→**tiltFilter (interactive, committed)**→align→
tsCtf→reconstruct, all SUCCEEDED. Confirmed working:
- tsImport auto-injected; alignment's `tomostar_dir` **auto-resolved to `TiltFilter/tomostar`**
  (the trimmed set) with **no source_override** — the preferred-source + cached-`paths`
  fallback works exactly as designed.
- The **compute honored the cut**: full tomostar 652 tilts → trimmed 613; alignment
  *staged and processed* the trimmed set (job004/tomostar = 39 for Position_10 vs 41 full;
  the per-TS Warp XML has 39 tilts), and the 16 reconstructed tomograms are built from it.
  Registry stamped 43 `is_filtered_out` frames (656 total → 613 kept).
- Absolute movie paths in the staged tomostar resolved correctly.

**THE GAP (matters for the particle stage): the `emit_star` adapters serialize *every*
registry frame regardless of `is_filtered_out`, so `aligned_tilt_series.star` and
`ts_ctf_tilt_series.star` carry the dropped tilts as "ghost" rows** (real CTF from
fsMotion, but `<NA>` for every alignment column — XTilt/YTilt/ZRot/shifts — since they
were never aligned; they are the highest-tilt frames). Because Stage 1 reverted
tsReconstruct/templateMatch to read the tsCtf star, **the particle stage would now ingest
these ghost NA-alignment rows** — a partial regression vs the old design, which fed a
clean filtered star. The tomograms themselves are fine (built from the XMLs). Fix =
make the alignment + ctf `emit_star` exclude `is_filtered_out` frames.

**→ FIXED + RUNTIME-VERIFIED (2026-07-31 on `copiatest_postfilter`, all 16 TS via a
non-destructive emit_star re-run: every emitted per-TS star matched its trimmed-tomostar
row count — 30/34/36/39/40, varying per TS — with 0 `<NA>` alignment rows, on BOTH the
`aligned` and `ts_ctf` outputs. The tsCtf pass was fed the OLD un-trimmed 41-row aligned
star as input, so it also proves the defensive drop works on an unfiltered input.)** Both
`_apply_alignment_to_tilt_df`
(`adapters/ts_alignment.py`) and `_apply_ctf_to_tilt_df` (`adapters/ts_ctf.py`) now drop
the per-tilt row when the resolved `Frame.is_filtered_out` is `True` (collecting the row
indices during the existing resolve loop, then `tilt_df.drop(index=...).reset_index()`
before the DataFrame is written). Placed *before* the alignment/CTF overlay so filtered
rows never receive columns. The `all_tilts.star` sidecar and per-TS `tilt_series/*.star`
inherit the trim (they're built from the post-drop DataFrame). The `tsCtf` drop is
defensive — its input is `tsAlignment`'s already-trimmed output — but correct if `tsCtf`
ever reads an unfiltered star. **No-filter pipelines are unaffected:** `is_filtered_out`
defaults `False`, so `filtered_idx` is empty and output is byte-identical. Not blocked by
the freshness issue: the adapter runs in-driver with a registry loaded fresh from disk.
`ruff check` clean, `py_compile` clean. **Runtime check:** on `copiatest_postfilter`,
confirm `aligned_tilt_series.star`/`ts_ctf_tilt_series.star` per-TS blocks now carry 613
(not 656/652) rows with no `<NA>` alignment rows, and the particle stage sees a clean list.

**IMPORTANT correction to the §5 freshness note:** making `emit_star` respect the flag is
**NOT blocked** by main-process registry staleness — adapters run *inside the driver
process*, which loads the registry fresh from disk (the filter, running earlier, already
persisted the flags). Only the **dashboard reading the flag live** is blocked by the
cached-singleton staleness. So Stage 2B splits: (i) `emit_star` respects the flag —
feasible now, needed for particle-stage correctness; (ii) dashboard-reads-flag — still
gated on freshness, cosmetic.

**Stage 2 — centralize in the registry (Option B groundwork). ✅ PART A BUILT
2026-07-30** (code-clean, PENDING RUNTIME); part B deliberately deferred to Stage 3
(see the freshness blocker below).
- **Built (A) — the registry is now the authoritative record of the filter verdict:**
  - `Frame.is_filtered_out` + `filter_reason` added to `services/tilt_series/models.py`
    (additive; old registry JSONs load fine via defaults; `REGISTRY_SCHEMA_VERSION`
    bumped `1.0 → 1.1`). This is the correctly-scoped **per-tilt** flag. **Gotcha
    confirmed:** `TiltSeries` *already* had an `is_filtered_out` field whose comment
    calls it "the frame-level tilt-filter concept" but sits at TS scope — a mislabeled
    placeholder; we did NOT reuse it, the real flag lives on `Frame`.
  - `registry.set_frame_filtered(frame_id, filtered, reason=)` +
    `filtered_out_frame_ids()` added (mirror `set_excluded`/`excluded_ids`).
  - `drivers/tilt_filter.py` stamps every processed tilt's verdict (good→False,
    bad→True, so re-runs are idempotent), keyed by `Frame.id == cryoBoostKey` (raw-movie
    stem), then `registry.save()`. Best-effort: unknown stems / empty registry are
    skipped and any registry error only warns — it must never fail a filter that already
    trimmed the tomostar.
- **Deferred (B) — consumers read the flag instead of diffing labeled/filtered stars.**
  NOT built, and this is the honest reason: the dashboard reads all *driver-produced*
  per-tilt data from **stars fresh off disk**, never from the registry singleton —
  because the tilt-filter (like every driver) runs in a **separate process**, so after
  it `save()`s the registry the main server's cached `get_registry_for()` instance is
  **stale until reload**. (`excluded_ids` is safe only because the *main* process owns
  it.) Wiring the dashboard to read a driver-written flag live would show stale data.
  So part B is folded into Stage 3, which must solve main-process registry freshness
  holistically. Until then the dashboard keeps its (working, fresh) labeled/filtered
  star diff, and the flag is a correct on-disk record + the Stage 3 prerequisite.

**Stage 3 — registry-authoritative tomostar (optional, gated on §5).**
- Emit the tomostar from the registry (or always post-trim it by the flag), retiring
  the parallel selection state. Only worth it if §5 lands.

**Parked / WIP (tracked elsewhere):** the interactive hard-stop gate (dropped —
"put it on the user"); missAlign chained-SLURM + train/infer (preamble shipped,
resume shipped PENDING RUNTIME; see `docs/miss-alignment.md`).

---

## 5. The bigger question — collapse the RELION track? (next-session)

**Complaint (valid):** through preprocessing we maintain a full RELION `.star` track
that no compute step reads, purely to (a) type the IO graph, (b) feed the dashboard,
(c) hand off to the particle stage. Meanwhile the registry — introduced to *be* the
single source of truth — is only half-wired (it ingests WarpTools outputs and emits
stars, but doesn't drive the tomostar and is bypassed by the tilt-filter). So we have
up to three copies of the same per-tilt truth that can drift.

**Options to weigh next session:**

- **(i) Do nothing / accept it.** The star track is the RELION-interop contract and
  the typed IO currency. Cheap to keep, and the particle stage needs it anyway. The
  cost is ongoing drift risk + adapter code.
- **(ii) Registry as single truth; stars emitted lazily.** Keep the registry
  authoritative for all per-tilt/TS state (selection, filter, exclude, per-job
  outputs). Emit RELION stars **only where actually consumed** — at the particle-stage
  handoff and on-demand for the dashboard/RELION-open. Delete the always-on
  per-preprocessing-job star track.
  - **Pro:** one truth; no drift; filter/exclude/select unified; less shadow I/O.
  - **Con:** the **IO-slot graph is star-typed** — `JobFileType`, `preferred_source`,
    `source_override`, orphan detection all assume star artifacts on disk. Re-keying
    the graph off registry entities (or registry-backed virtual artifacts) is a deep
    refactor touching every job's schema + `path_resolution_service`. High risk.
- **(iii) Tomostar as single truth for preprocessing; registry+stars derived.**
  Since WarpTools already treats tomostar/XML as authoritative, lean into it: registry
  and stars become pure projections. **Pro:** matches reality. **Con:** tomostar is a
  WarpTools-specific format; the registry gives us a tool-agnostic model (useful if a
  non-Warp aligner is ever added), and the mdoc→tomostar step is inside WarpTools
  (opaque, e.g. the hardcoded intensity walk — see
  `project_warp_ts_import_filter`).

**Open questions — three now answered from the source (2026-07-30):**

1. *Does anything write the per-tilt stars that isn't an `emit_star` adapter?*
   **Almost no.** The 4 preprocessing stars have exactly one producer each — the
   `emit_star` of `FsMotionCtf`/`TsAlignment`/`TsCtf`/`TsReconstruct` adapters in
   `services/tilt_series/adapters/`, all driven by the registry. The **one exception**
   is the Import job, which writes `tilt_series.star` **inline** via
   `PipelineOrchestratorService._write_import_stars_inline()` (reads the registry
   directly, bypassing adapters — its own code comment calls it "Option B"). Notably
   that inline writer is already a working proof of the "read registry → emit star"
   pattern option (ii) needs.
2. *How star-path-specific is the IO graph?* **Artifact-abstract, path-concrete —
   less coupled than this doc originally feared.** `JobFileType` is a semantic enum;
   `OutputSlot.path_template` is just physical layout; `_choose_candidate_for_slot`
   scores by producer identity, not file contents. The only disk-existence check is
   the optional `prefer_if_exists` gate. Re-keying to registry-backed artifacts is
   **localized to `path_resolution_service` (~a couple of methods, the
   `_get_producer_output_path`/`_build_output_index` path)** plus the adapter write
   sites — **not** "every job's schema." Revises the §5(ii) "High risk" note down to
   moderate/localized.
3. *Can the dashboard read the registry directly?* **Yes for alignment + CTF (fully in
   the registry), but blocked on ONE gap for fs-motion:** beam-induced motion +
   `rlnCtfMaxResolution` are **not persisted** — the star columns are `1e-6`
   placeholders and the dashboard reads the real values live from the Warp XMLs
   (`frameseries_quality.quality_series()`). Backfilling `mean_frame_movement` +
   `ctf_resolution` into `FsMotionCtfFrameOutput` (~50 lines: model field + adapter
   populate) makes the registry self-contained; then the dashboard's
   `_read_per_tilt_df(path)` sites can switch to a registry accessor. RELION's
   downstream stages don't read those two columns, so they're QC-only.

*Still open (judgment / product calls, not code-answerable):*
4. Is a non-WarpTools aligner ever plausible? (Decides ii vs iii.)
5. Migration story for existing on-disk projects (stars present, registry maybe
   absent → "reload to backfill from mdocs").

**Second blocker found while building Stage 2 (2026-07-30) — main-process registry
freshness.** Drivers run in separate SLURM processes and mutate the *on-disk* registry
(`registry.save()`); the main server holds a **cached** `get_registry_for()` singleton
that is not refreshed when a driver writes. That's exactly why the dashboard reads all
driver-produced per-tilt data from **stars** (fresh off disk) and only touches the
registry for **main-process-owned** state (`excluded_ids`). So before *any* consumer
(dashboard, lazy star emit) can trust the registry as the live source, Stage 3 must add
a refresh path — reload the affected TS sidecars from disk when a job completes / on
render — being careful not to clobber live in-memory state (cf.
[[feedback_inmemory_state_authoritative]]). This is a real second gate alongside the
IO-graph re-key; neither is huge, but the collapse needs both.

**Recommendation to open with:** option (ii) is the principled end state and now looks
**cheaper than the original writeup implied** — the IO-graph re-key is localized to
`path_resolution_service`, and Q1/Q3 show the registry is already the de-facto producer
(one motion-data backfill is the only real data gap). The two gates are (a) de-star-ify
the IO graph and (b) solve main-process registry freshness (above). Scope it as its own
investigation, but it is no longer gated on a scary "touch every schema" refactor.
Stage 2A (built) already put the filter verdict in the registry as the down-payment.

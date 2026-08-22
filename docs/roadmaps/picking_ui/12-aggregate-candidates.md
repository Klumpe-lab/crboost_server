# 12 — Aggregate: unite one species' picks at any granularity

**Status:** scoped 2026-08-22 with the maintainer. Was a stub deferring a design decision;
the decision was taken in conversation and is recorded in §1. Not yet built.

**Read `docs/particle-data-flow.md` first.** Every fact this roadmap leans on — what ①/②/③
consume and produce, what `optimisation_set.star` is, which columns are ragged, what must
agree across projects — is drawn there with the figures. This document is the plan; that
one is the reference.

## 0. Origin

The maintainer, `q_important_ui_fixes.md:34`:

> *"none of the aggregation controls belong [on the picks surface]… the merge-sources icon
> on the Particles is enough except it should probably be called something like 'aggregate
> candidates' and just spawn a 'Pick Candidates' job… so the coordinate lists (whether
> produced by manual picking, TM or something else) can be united and directly initiated in
> a pick coordinates job prepopulated with the combined list."*

and, expanding it 2026-08-22:

> *"let's follow a single species' journey: it might be found in one or multiple tomograms,
> it might be found across multiple coordinate files associated with one tomogram even (some
> produced by the user denovo picking, some via picking algos), it might be then found across
> multiple projects. We want to be able to merge/collate-filter at all of these levels at any
> level of granularity… Whether to create a job that consumes the output of these
> mergers/collations can just be a toggle button at the end of this workflow."*

## 1. Decisions taken (2026-08-22)

**D1 — the spawned job follows the payload GRADE, not the user's choice.**
② Pick candidates is a peak-finder over `tmResults/*.mrc`; handing it a coordinate list
leaves it nothing to do (`docs/particle-data-flow.md` §2). A union of coordinate lists *is*
what ② would have produced, so the terminal action is to skip ②:

| what was merged | terminal action |
|---|---|
| coordinate grade, one tomogram (L1) | the merged `PickList`'s per-list Extract (`EXTRACT_PICK_LIST`) |
| coordinate grade, many tomograms (L2/L3) | a `subtomoExtraction` job prepopulated with the merged optset |
| pixel grade (any level) | wire as `mergedSources` producer; offer Reconstruct / Class3D |

No new job type, no new driver mode, and `#68` stays archived.
`subtomo_merge._read_input_particles_lenient` already accepts an optics-less coordinate star
(*"Only `rlnTomoName` is required"*), so ③ needs no change to consume a coordinate merge.

**D2 — one species per merge, always.** The maintainer: *"there is no workflow where we want
to merge/extract/collate multiple species in one set of files, that just never happens."*
This removes the geometry ambiguity (whose box? whose Ø?) that made the original stub hard.
Enforced in the dialog, not merely conventional.

**D3 — tomogram identity uses two keys, not one.**
Distinguishing (*are these different?*) → absolute `rlnTomoReconstructedTomogram`, already
`subtomo_merge`'s guard. Equating (*are these the same acquisition?*) → mdoc `SubFramePath` +
`DateTime`, already stored as `TiltSeries.frames[0].raw_filename` /
`.acquisition_time`. A matching acquisition key is necessary but **not sufficient**: a second
gate requires handedness, `rlnTomoTiltSeriesPixelSize`, `rlnTomoTomogramBinning` and
`rlnTomoSizeX/Y/Z` to agree before coordinates may be transferred. Mismatch raises.
Re-mapping coordinates between two different reconstructions of one TS is out of scope.

**D4 — collisions are detected automatically and reported; dedup is one button.**
At every level, after the union is computed, clashes at the species Ø radius are counted and
shown. If any exist the report offers **Deduplicate**, which runs `pick_merge`'s existing
greedy keep-first walk (priority `merged > manual > imported > filtered > auto`). Never
automatic, never silent. Finer-grained filters (per-source score bands, per-tomogram caps)
are explicitly deferred — see §3.

**D5 — incongruity raises; ragged columns degrade honestly.**
Optics, binning, box size and handedness disagreements raise rather than warn (S0). Ragged
columns carry through as far as they honestly can:

- **orientations** — `0,0,0` is RELION's own "no prior", not an invented value. Write it,
  record coverage. `list_extraction` already does this.
- **score** — `0` is a legal LCC value, so it can never be a placeholder. A ragged score
  column leaves the RELION-facing `particles.star`, is preserved per-row in a sidecar keyed
  by `rlnTomoParticleName`, and every score filter states its coverage
  (*"applies to 1,204 of 3,890 picks"*).
- **anything else ragged** — sidecar + coverage in `merge_summary.json`; promoted into the
  main star only when every source carries it.

The rule: *a placeholder must never be indistinguishable from a measurement.*

## 2. End state

One dialog, opened from the roster PARTICLES-header icon (`pipeline_roster.py:1508`,
tooltip → **"Aggregate candidates"**), that unites one species' picks at whichever level the
user selects, and optionally launches what comes next.

```
   Aggregate candidates · <species>
   ┌──────────────────────────────────────────────────────────────┐
   │  GRADE   ( ) coordinates — picks not yet extracted           │
   │          ( ) extracted particles                             │
   ├──────────────────────────────────────────────────────────────┤
   │  SELECT                                                      │
   │    ▾ this project                                            │
   │        ▾ TS_01        auto (312)  ·  manual__sven (14)  ☑ ☑  │
   │        ▾ TS_04        auto (287)                        ☑    │
   │    ▾ /other/project                                          │
   │        ▾ TS_02        auto (301)                        ☑    │
   ├──────────────────────────────────────────────────────────────┤
   │  613 picks · 3 tomograms · 2 projects                        │
   │  ⚠ 41 clashes within 180 Å           [ Deduplicate ]         │
   │  score present on 600 / 613 — filter would apply to 600      │
   ├──────────────────────────────────────────────────────────────┤
   │  ☑ and extract subtomograms when done                        │
   │                              [ Cancel ]  [ Aggregate ]       │
   └──────────────────────────────────────────────────────────────┘
```

Output, identical in shape at every level:

```
   MergedSources/<slug>/
       particles.star          coordinate grade OR pixel grade
       tomograms.star          union of contributing frames
       optimisation_set.star   the envelope
       merge_summary.json      counts, sources, column coverage
       provenance.star         per-row ragged values (sidecar)
```

L1 additionally registers a `PickList` so the merge appears as a chip on the picks surface —
which is also how the 09-S3 regression closes (see §0 of S6).

## 3. Stages

**S0 — close the silent-merge hole.** CODE-COMPLETE 2026-08-22. No UI. Promote `rlnImageSize`, `rlnImagePixelSize`,
`rlnTomoSubtomogramBinning` from `OPTIONAL_OPTICS_COLS` to `CRITICAL_OPTICS_COLS` in
`services/subtomo_merge.py`, so a box-size mismatch raises instead of printing
`[MERGE WARN] … using primary value` to a log nobody reads. Add handedness to the
tomogram-conflict check. Independently committable, fixes today's behaviour.

**S1 — tomogram identity.** CODE-COMPLETE 2026-08-22. New `services/particles/tomo_identity.py`:
`acquisition_key(ts) -> (raw_filename, acquisition_time)` and
`assert_transferable(row_a, row_b)` implementing D3's second gate. Pure functions, no UI, no
new parsing — the registry already holds both fields.

**S2 — the coordinate-grade merge engine.** CODE-COMPLETE 2026-08-22. New `services/particles/coord_merge.py`: union N
coordinate sources spanning tomograms and projects into a `MergedSources/<slug>/` directory
at coordinate grade. Reuses `pick_merge.type_priority` for row order, `list_extraction`'s
optics-synthesis policy (raise, never invent) and `subtomo_merge.write_optimisation_set` for
the envelope. Implements D5's ragged-column handling and writes `provenance.star`.

**S3 — collision report** CODE-COMPLETE 2026-08-22 (service half; the Deduplicate button is S4). Clash counts at the species Ø radius over the whole union,
per tomogram and total; the **Deduplicate** action calls `pick_merge.deduplicate_star`.
Report is computed on every selection change, shown only when non-zero.

**S4 — the dialog.** CODE-COMPLETE 2026-08-22. Rename the roster icon and dialog. Add the grade switch. Extend the
existing `Project → Species → Tomogram` tree (`merge_card._MergeSelector`) down to the LIST
level for coordinate grade. Enforce one species. Pixel grade keeps today's behaviour
unchanged behind the same chrome.

**S5 — the terminal action.** CODE-COMPLETE 2026-08-22. `☑ and <verb> when done`, verb per D1's table. Spawns with
paths prepopulated; does not run anything the user did not tick.

**S6 — L1 back on the picks surface.** CODE-COMPLETE 2026-08-22. A one-tomogram coordinate merge registers its
`PickList` (`list_actions.merge_lists` already does exactly this and currently has **zero
call sites** — 09-S3 removed the merge bar and nothing replaced it, so the app has no way to
create a merged pick list at all today). This stage is the replacement 09-S3 promised.

## 4. Non-goals

- **Multi-species merges.** D2.
- **Re-mapping coordinates between two different reconstructions of the same TS.** D3's second
  gate raises instead. A real feature, a different roadmap.
- **Fine-grained collate-filters** — per-source score bands, per-tomogram caps, orientation
  filters. Maintainer, 2026-08-22: *"we can think about providing some more fine-grained
  filters… but just make a note of this for the next roadmap. we really need to finish the
  near-term minimal implementation."* Recorded here as the intended successor.
- **A list-input mode for ② Pick candidates.** Dissolved by D1, not deferred. `#68` stays
  archived.
- **Renaming the roster icon on its own.** It would advertise a capability that does not
  exist; it ships with S4 or not at all.

## 5. Runtime checklist (maintainer)

To be filled as stages land. Minimum, once S0–S6 are code-complete:

1. Two projects, same species, same physical TS imported into both → aggregate at coordinate
   grade. Expect the acquisition key to equate them and the transferability gate to pass.
2. Same, but with `flip_tiltseries_hand` differing → expect a RAISE naming handedness, not a
   merged file.
3. Aggregate a manual list with an auto list on one tomogram → expect the clash report, a
   working Deduplicate, and manual picks surviving the clashes.
4. Check the merged `particles.star` carries no `rlnLCCmax` column when sources are ragged,
   and that `provenance.star` holds the scores that existed.
5. Tick the terminal toggle at each level → expect per-list Extract (L1), a `subtomoExtraction`
   instance (L2/L3), and nothing spawned when unticked.
6. Merge two pixel-grade sources extracted at different box sizes → expect a RAISE (S0), where
   today it silently merges.

## 6. Log

- 2026-08-21 — stub written; deliberately unscheduled while the de-novo picking interface
  (roadmaps 08–11) was the focus. Deferred one decision: spawn a Pick-candidates job with a
  list-input mode, or a Subtomo extraction prepopulated with the merged list.
- 2026-08-22 — scoped with the maintainer. The deferred decision dissolved rather than
  resolved: ② cannot consume a coordinate list at all, so the merge produces what ② would
  have produced and the terminal action skips it (D1). Six further decisions taken (D2–D5),
  stages S0–S6 written, `docs/particle-data-flow.md` written as the reference figure.
  Discovered en route: `rlnImageSize` / `rlnTomoSubtomogramBinning` mismatches merge silently
  today (→ S0), and `list_actions.merge_lists` has had zero call sites since 09-S3 (→ S6).
- 2026-08-22 — **S0 + S1 code-complete, uncommitted.** Both are pure service-layer, no UI, and
  commit clean on their own ahead of the 08+09+10+11 wall.
  - S0 (`services/subtomo_merge.py`): a third optics tier, `GEOMETRY_OPTICS_COLS` =
    `rlnImageSize` / `rlnImagePixelSize` / `rlnTomoSubtomogramBinning`. Deviation from §3 as
    written: they were NOT promoted into `CRITICAL_OPTICS_COLS`, because `strict` treats a missing
    critical column as a hard `KeyError` and an older or hand-built star that simply omits the box
    is not a mismatch. The new tier separates the two — **absence tolerated, divergence fatal** —
    compared on the string form over non-null values only, the same idiom the critical check uses.
    `OPTIONAL_OPTICS_COLS` keeps `rlnImageDimensionality` alone and keeps its advisory print.
  - S0 handedness: **not** added to the tomogram-conflict check, and this is not a shortcut.
    `rlnTomoHand` does not live in `tomograms.star`; it is written into the Import job's
    `tilt_series.star` and reached via `rlnTomoTiltSeriesStarFile`. Reading it is one extra file
    per source, so it landed in S1 (`read_tomo_hand`) where the gate that needs it lives.
  - S1 (`services/particles/tomo_identity.py`, new): `distinguishing_key` (absolute reconstruction
    path), `acquisition_key` (mdoc `SubFramePath` + `DateTime` off `TiltSeries.frames[0]`, None when
    either half is absent — a partial key is not a key), `read_tomo_hand`, `check_transferable` /
    `assert_transferable` over `TRANSFERABLE_COLS`. The report deliberately splits **blocking**
    (stated facts that disagree → raise) from **unverified** (facts neither side states → the caller
    must SHOW them, per the never-invent-defaults policy). Handedness is the usual unverified line.
  - `services/particles/__init__.py` docstring names the new module. `ruff check .` green
    repo-wide; `ruff format` clean. `check_boundaries.py` and `import main` still owed — no runnable
    python in the agent sandbox.
- 2026-08-22 — **S2 code-complete + S3's service half, uncommitted.** One new file,
  `services/particles/coord_merge.py`, plus a one-line docstring update in the package `__init__`.
  Still pure service layer — nothing in `ui/` imports it yet, so it commits with S0/S1 ahead of the
  08–11 wall.
  - `CoordSource` (one resolved contributing list) → `plan_merge` → `merge_coordinate_sources`.
    Planning is separated from writing on purpose: the dialog (S4) needs to show what a merge WOULD
    do — renames, unverified facts, clash counts — before anything lands on disk.
  - **Tomogram identity is conservative by default.** `plan_merge(registry_lookup=None)` never
    equates two tomograms across projects; same-named ones are kept APART under a
    `<name>__<project>` disambiguation and the fact is reported as `unverified`. Pooling only
    happens when the caller supplies a `registry_lookup` that yields a `TiltSeries` whose
    `acquisition_key` matches — and then S1's transferability gate runs, and a stated disagreement
    is blocking. Never silently pooling unrelated picks is worth more than the convenience.
  - Two defects found while writing it, both now guarded rather than noted:
    (a) a source project's `tomograms.star` may hold paths RELATIVE to its own root, and copying
    them verbatim into a merged star elsewhere yields a reconstruction path that resolves to
    nothing — which extraction skips per-TS without complaining. `_absolutize_paths` resolves every
    `*File` / `*Dir` / `rlnTomoReconstructedTomogram` value against the project that wrote it.
    (b) contributing projects can write different `tomograms.star` schemas; stacking them fills the
    gaps with NaN, and a NaN tilt-star reference is the same silent skip. A column stated for some
    tomograms and not others now RAISES, naming the columns.
  - Column policy per D5 is in `_classify_columns`: angles filled with 0 (RELION's real "no prior",
    matching what `list_extraction` already writes), every other column in the main star only at
    100% row coverage, everything ragged into `provenance.star` with per-row coverage in
    `merge_summary.json`. `SCORE_COLS` exists so the caller can NAME the score column in the UI
    ("filter applies to N of M"), not because score has a separate rule.
  - `rlnTomoParticleName` is reassigned merged-wide (per-tomogram `<tomo>/<n>`), because the
    sources' own names are unique only within their own set and the merged set needs one join key.
    The originals ride in the sidecar as `cbSourceParticleName` rather than being lost.
  - S3's `clash_report` gives per-tomogram + total counts at a radius, reusing
    `pick_merge.clash_stats_coords`, so what it reports is exactly what
    `pick_merge.deduplicate_star` would drop. Nothing is applied — D4.
  - Headless CLI (`python -m services.particles.coord_merge --source … --out …`), same precedent
    and same reason as `list_extraction`'s: build the artifacts without SLURM so the star format is
    eyeballable, and so this is exercisable in the module env. `ruff check .` green repo-wide.
- 2026-08-22 — **S4 + S5 + S6 code-complete. Roadmap 12 is code-complete, PENDING RUNTIME.**
  New `ui/aggregation/aggregate_dialog.py` (648 lines) + `discover_pick_list_projects` in
  `services/aggregation/discovery.py`; `pipeline_roster._build_aggregation_merge_btn` now opens it
  and says "Aggregate candidates". **These are the first files in this roadmap that touch `ui/`, so
  from here it folds into the 08–11 commit wall.**
  - Deviation from §3 as written, and the significant one: S4 is a NEW module, not a grade switch
    inside `merge_card`. That dialog is built entirely around `AggregationSource` — one entry per
    optimisation set — and a coordinate source is a LIST, several per tomogram, most with no optset
    anywhere. Threading a second identity model through the same tree would have made both harder
    to read than either is. The two are linked instead: the coordinate dialog carries an
    "extracted particles instead ↗" link that closes it and opens `merge_card`'s, and `merge_card`
    is otherwise untouched. Its own §2 sketch (one dialog, a GRADE radio) is therefore not what
    shipped; the user-facing effect is the same door and the same two grades.
  - Cross-project reach needed a second discovery path. `discover_subtomo_optimisation_sets` finds
    projects that have EXTRACTED something, which is the wrong filter one stage earlier — a de-novo
    project whose only particles are hand-placed has no optset at all and was invisible to it.
    `discover_pick_list_projects` keys on `project_params.json` instead, and foreign lists are read
    straight off `get_project_state_for(other_path).pick_lists`.
  - S5's verb is chosen by shape, not by the user: a single-tomogram aggregate in THIS project
    offers **Extract this list** and submits `backend.extract_pick_list` for real; anything wider
    offers **Set up extraction**, which points an existing `subtomoExtraction` instance's
    `input_optimisation` at the aggregate and STOPS. It deliberately does not submit — box / crop /
    binning are the species' decision and the job tab states them; launching with whatever numbers
    happened to be on the instance is exactly the invented default this codebase refuses. Same
    reason the Extract path refuses when the species has no committed `ExtractionParams`: it says
    so and names the panel that owns the decision, rather than picking a box.
  - S3's user half lives in the result dialog: clash counts at the species Ø with an explicit
    statement that NOTHING was deduplicated and why the union keeps clashers. When the species
    states no Ø the report is not computed and says so — no invented particle size.
  - Blocking vs unverified are two different dialogs on purpose. Blocking is a wall (a stated
    reconstruction disagreement between the same acquisition; no user intent makes pooling those
    coordinates correct). Unverified is a question listing what nothing on disk states — handedness,
    and any tomogram name that had to be disambiguated.
  - `ruff check .` green repo-wide; the four new/changed files are `ruff format` clean.
    `check_boundaries.py` and `import main` still owed.
  - **Incident, for the record:** a `perl -0pi` rewrite with a wide-character replacement
    double-encoded every non-ASCII byte in `pipeline_roster.py`. Caught by `ruff` (E902, then
    RUF001 on a mojibake'd `·`), repaired by reversing one UTF-8 encoding pass
    (`iconv -f UTF-8 -t ISO-8859-1`) and re-inserting the em dash as raw bytes. The file now
    validates as UTF-8 and its 55 non-ASCII lines read correctly (`—`, `Å`, `▸`, `·`, `°`). No
    other file was affected — the corruption needs a wide char in the replacement, which only that
    one run had. Worth a spot-check of the roster's chips at runtime anyway.

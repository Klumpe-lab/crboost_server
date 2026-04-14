# TiltSeries Registry Refactor — Plan & Status

_Last updated: 2026-04-14. Session: Diagnosed post-refactor TM underpicking on `gt_pos9_10_newinfra` (170 picks vs 210 pre-refactor) — root cause was `fs_motion_and_ctf` driver dropping the negative sign on `--eer_ngroups` during the per-TS parallelization refactor, collapsing CTF fits on ~30% of tilts (esp. high-tilt) into the defocus-search floor. Fixed at `drivers/fs_motion_and_ctf.py:113-129`. Verified every other Warp invocation byte-matches original CryoBoost (`fsMotionAndCtf.py`, `tsCtf.py` incl. `ts_defocus_hand`, `tsAlignment.py` incl. `ts_import` flags, `tsReconstruct.py`). Added **Stage 4B** for per-tomogram array jobs on TM/extract/subtomo (currently monolithic)._

## The problem this plan solves

### Immediate symptom (what triggered the plan)

`tsCTF` metadata aggregation was failing with 57 unresolved keys, e.g.

```
CTF metadata merge has identity conflicts — 57 unresolved key(s):
  'agg11_..._Position_11_029_52.00_20251113_193630_EER'
    (tried [..._193630_EER, ..._193630, Position_11_029_52])
  ...
```

**Root causes — both in `services/configs/metadata_service.py`:**

1. **`_parse_tilt_series_xml`'s `[:num_entries]` slice silently dropped tilts.**
   When WarpTools writes fewer `<GridCTF>` nodes than `<MoviePath>` entries
   (e.g. CTF fit skipped on extreme tilts), the slice took the first-N
   MoviePath entries — but GridCTF Z-index doesn't correspond to MoviePath
   position-order after exclusions. The late-dose tilts fell off the end of
   `warp_df`, then the merge couldn't resolve them.

2. **`Path(key).stem` corrupted filenames with tilt-angle dots.** For a
   filename like `..._Position_11_029_52.00_20251113_193630_EER`, Python's
   `Path.stem` treats `.00_20251113_193630_EER` as the "suffix" and returns
   `..._Position_11_029_52`. The `base_key` candidate was therefore useless
   and could match adjacent tilts by prefix.

### The deeper problem the plan addresses

The symptom is a surface of a structural issue: **tilt-series and frame
identity is a recomputed accident at every metadata-merge site**. Three
different `cryoBoostKey` derivations exist in `metadata_service.py`, each
with slightly different `replace()` chains. Merges are best-effort string
heuristics keyed on ephemeral filename transformations rather than on
identity established at import time.

What the system implicitly knows but doesn't use:

- `ProjectState.import_tilt_series_details` (per-TS: mdoc filename, stage
  position, tilt count)
- `ProjectState.tilt_metadata` (per-tilt mdoc stats, keyed by frame stem)
- `services/dataset_models.py` `DatasetOverview` / `TiltSeriesInfo` /
  `TiltInfo` — already parses mdocs into a rich structure at import time

What we lose by not using them:

- Identity is re-inferred from STAR filename fields at every job boundary
- `cryoBoostKey = Path(rlnMicrographMovieName).stem` is computed at 3+ sites
  with different normalization rules; mismatches cause silent cross-TS
  contamination
- No way to declaratively ask "does this TS have all upstream artifacts?"
  before dispatching a job
- No scalable path to 100s of TS per project — every STAR parse is O(N)
  and happens many times per job

### Principles the plan establishes

1. **Identity is assigned once, at import, and never recomputed.**
   `TiltSeries.id` = mdoc filename stem; `Frame.id` = raw frame filename stem.
   These are persisted in a registry on disk and referenced everywhere
   downstream.

2. **Metadata merges are typed joins keyed by `Frame.id` via
   `(tilt_series_id, Z-index)`.** No string matching, no fallback candidate
   chains. Silent corruption becomes structurally impossible.

3. **STARs are serialization, not source of truth.** On-disk STARs remain
   in RELION format so external tools continue to work, but they are
   regenerated from registry state; the registry is authoritative.

4. **Ship working code at every stage.** No big-bang cutover. Each stage is
   additive or swaps exactly one code path.

---

## Entity model (`services/tilt_series/models.py`)

```
Frame
├── id: str                    # canonical, immutable, = Path(raw_filename).stem
├── tilt_series_id: str
├── raw_path: Path             # absolute path to source EER/tif
├── raw_filename: str          # cached basename
├── tilt_index: int            # 0-based acquisition order (matches Warp's <Node Z>)
├── nominal_tilt_angle_deg: float
├── pre_exposure_e_per_a2: float
├── acquisition_time: Optional[datetime]
└── outputs: Dict[str, FrameOutput]   # keyed by job instance_id

TiltSeries
├── id: str                    # canonical, = mdoc filename stem
├── mdoc_path, mdoc_filename
├── stage_position, beam_position
├── frames: List[Frame]        # ordered by tilt_index
├── tomogram: Optional[Tomogram]
├── outputs: Dict[str, TiltSeriesOutput]
├── is_selected, is_filtered_out, filter_reason
└── frame_by_index/id/filename — identity-resolution helpers

Tomogram (1:1 with TS for v1)
├── id: str                    # = tilt_series_id
├── tilt_series_id: str
└── outputs: Dict[str, TomogramOutput]
```

Output types are Pydantic discriminated unions via `output_type` literal:

| Scope     | Type                           | Produced by         |
|-----------|--------------------------------|---------------------|
| Frame     | `FsMotionCtfFrameOutput`       | fs_motion_ctf       |
| TS        | `TsCtfTiltSeriesOutput`        | tsCTF (adapter live)|
| TS        | `TsAlignmentTiltSeriesOutput`  | tsAlignment         |
| Tomogram  | `TsReconstructTomogramOutput`  | tsReconstruct       |

Each carries `job_instance_id`, `job_dir`, `attached_at` for provenance.

---

## Persistence layout

Per project:

```
{project}/registry/
├── index.json                         # {schema_version, [ts_id, ...], frame_count}
└── tilt_series/
    ├── {ts_id_1}.json                 # full TiltSeries serialized
    ├── {ts_id_2}.json
    └── ...
```

Sidecar per TS (not one monolithic file) anticipates **Stage 6** — lazy
per-TS load at 100+ TS scale. Atomic writes via tempfile+rename. Index
schema versioned so forward-compat is explicit.

Writes go through `TiltSeriesRegistry.save()` (sync) or `save_async()`
(asyncio lock). Dirty-tracking at the TS level means attaching one output
rewrites one sidecar, not the full project state.

---

## Staged plan + status

### Stage 0 — Bug fixes ✅ DONE

`services/configs/metadata_service.py`:

- `_parse_tilt_series_xml` (lines ~73–147): rewrote to use Z-index lookup
  into `MoviePath`, fail loud on grid Z-set divergence or out-of-range Z,
  warn on Z values missing from any grid. Replaces the `[:num_entries]`
  slice that was dropping late-dose tilts.
- `_merge_ctf_metadata` (line ~694): dropped the broken `base_key =
  Path(key).stem.replace(...)` candidate. Only `key` + `clean_key` cascade
  remain.

### Stage 1 — Registry scaffolding ✅ DONE

New files:

- `services/tilt_series/__init__.py` — public re-exports
- `services/tilt_series/models.py` — entity Pydantic models + output unions
- `services/tilt_series/registry.py` — `TiltSeriesRegistry` class, path-keyed
  singleton (`get_registry_for`, `set_registry_for`, `clear_registry`)
- `services/tilt_series/build.py`:
  - `build_from_dataset_overview(overview, project_prefix)` — preferred, no
    I/O
  - `build_from_mdocs(mdocs_glob, frames_dir, project_prefix)` — fallback
    that re-parses mdocs via `MdocService`

Registry API (all implemented):

- Lookup: `get_tilt_series`, `has_tilt_series`, `all_tilt_series`,
  `tilt_series_ids`, `get_frame`, `frame_by_filename`, `frame_count`
- Mutation: `add_tilt_series`, `remove_tilt_series`, `attach_frame_output`,
  `attach_ts_output`, `attach_tomogram_output`
- Validation: `assert_complete(job_instance_id, expected_ts_ids)`,
  `sanity_check()`
- Persistence: `load()`, `save(force)`, `save_async(force)`

Wiring:

- `backend.py` — `CryoBoostBackend.registry_for(project_path)` exposes the
  registry to the rest of the codebase
- `services/scheduling_and_orchestration/project_service.py`:
  - `_build_and_persist_registry(project_dir, mdocs_glob)` helper
  - Called after data import in `initialize_new_project` (new projects)
  - Called in `load_project_state` if the sidecar is missing (legacy
    backfill path)

Key design decisions pinned:

- **Frame ID format**: `Path(raw_filename).stem` (e.g.
  `..._Position_10_001_12.00_..._EER` for an `.eer` file). Raw-ish but
  unambiguous and matches what RELION STARs use via
  `Path(rlnMicrographMovieName).stem`.
- **Tomogram cardinality**: 1:1 with TS for v1. Multi-binning = multiple
  `TsReconstructTomogramOutput` records under the same `Tomogram`, not
  separate entities.
- **Re-run semantics**: overwrite on re-run (same `job_instance_id` replaces
  previous output). Matches current STAR behavior.
- **Migration strategy**: on legacy-project load, `load_project_state`
  backfills the registry from mdocs. No manual migration step.

Verified on `projects/pos9_10_after_pixShift`:
2 TS (`pos9_10_after_pixShift_Position_9`, `..._Position_10`), 82 frames
total, correct tilt-angle + pre-exposure + acquisition-order ordering,
round-trip through persistence preserves discriminated unions.

### Stage 2 — `TsCtfIngestAdapter` ✅ DONE

New files:

- `services/tilt_series/adapters/__init__.py`
- `services/tilt_series/adapters/ts_ctf.py` — `TsCtfIngestAdapter`

Flow:

1. **`adapter.ingest(expected_ts_ids)`**
   - Asserts every expected TS has a registry entry AND a per-TS XML
     (`{job_dir}/warp_tiltseries/{ts_id}.xml`).
   - For each TS: reuses the (fixed) `WarpXmlParser` to read `<GridCTF>`
     rows, each already keyed by `cryoBoostKey` = `MoviePath[Z]` after the
     `_EER.eer`/`.tif`/`.eer` strip. Resolves each row to a `Frame` via
     `TS.frame_by_filename` or stem-match. Computes
     `defocus_u = (val + delta) * 10000`, `defocus_v = (val - delta) *
     10000`, astigmatism, angle.
   - Attaches a typed `TsCtfTiltSeriesOutput` (with `per_frame`:
     `List[TsCtfPerFrameCtf]` and `are_angles_inverted`) to the registry
     via `registry.attach_ts_output(ts_id, ...)`.
   - Fails loud on unresolved or duplicated `frame_id` keys.

2. **`adapter.emit_star(input_star, output_star)`**
   - Reads `input_star` (the `aligned_tilt_series.star` from
     `ts_alignment`) — keeps every column RELION downstream needs.
   - For each TS: reads per-TS tilt STAR, resolves every row's
     `rlnMicrographMovieName` → `Frame` via `TS.frame_by_filename`, looks
     up the `TsCtfPerFrameCtf` by `frame.id`, overlays
     `rlnDefocusU/V/Angle`, `rlnCtfAstigmatism`, `rlnTomoHand`.
   - Writes the hierarchical `{output_dir}/tilt_series/{ts_id}.star` +
     main `{output_star}` with global block. Same shape as legacy.
   - `rlnTomoHand` on the global block is mapped per-TS from each TS's
     `are_angles_inverted`.

Driver wiring (`drivers/ts_ctf.py:262`):

Old `MetadataTranslator.update_ts_ctf_metadata(...)` call replaced with:

```python
registry = get_registry_for(project_path)
if not registry.tilt_series_ids():
    raise RuntimeError("TiltSeries registry is empty for project ... reload ...")
adapter = TsCtfIngestAdapter(
    registry=registry, job_dir=job_dir,
    job_instance_id=instance_id, warp_folder="warp_tiltseries",
)
adapter.ingest(ts_names)
adapter.emit_star(paths["input_star"], paths["output_star"])
registry.save()
```

**No silent fallback to the legacy path** — empty registry = fail loud
asking user to reload project. That's the fix for the original
silent-corruption class.

Verified on `pos9_10_after_pixShift/External/job004`:
- Registry built from mdocs (41 frames × 2 TS = 82 frames).
- Adapter ingested real CTF XMLs: 41/41 frames resolved per TS.
- `emit_star` into tmpdir: per-TS `rlnDefocusU` values match the legacy
  on-disk STAR byte-for-byte on all sampled tilts (55920 vs 55920, 55610
  vs 55610, 55474 vs 55474 for the first 3 tilts of Position_10).

---

### Stage 3 — Port other mergers (3 adapters) ✅ DONE

All three adapters follow the `TsCtfIngestAdapter` pattern.

#### `services/tilt_series/adapters/fs_motion_ctf.py` — `FsMotionCtfIngestAdapter`

- Reads per-movie XMLs at `{job_dir}/warp_frameseries/{frame.id}.xml`
  (XML stem == `Path(raw_frame_filename).stem` == `Frame.id`).
- Builds `FsMotionCtfFrameOutput` per Frame with averaged/even/odd MRC paths
  under `warp_frameseries/average/{,even/,odd/}{frame.id}.mrc` and the
  powerspectrum MRC; preserves the legacy U==V quirk (delta stored in
  `rlnCtfAstigmatism`) for byte-for-byte STAR compat.
- `emit_star(input, output, project_root)` — per-TS path resolution tries
  `(in_star_dir / rel)` then `(project_root / rel)` to handle both the
  project-root-relative ts_import STAR and later job-relative STARs.
- Swapped in `drivers/fs_motion_and_ctf.py:347`.

#### `services/tilt_series/adapters/ts_alignment.py` — `TsAlignmentIngestAdapter`

- Per-TS: reads `{job_dir}/warp_tiltseries/tiltstack/{ts_id}/*.st.aln`
  (AreTomo) or `.xf`/`.tlt` (IMOD); resolves `tomostar[wrpMovieName]` →
  `Frame` via registry; builds `TsAlignmentTiltSeriesOutput` with
  `per_frame: List[TsAlignmentPerFrame]`.
- `_assert_ts_identity_consistency` (tomostar ∩ per-TS XML ∩ tiltstack dir
  sets must agree) runs as an `ingest` precondition.
- `alignment_angpix` parameter controls shift→angstrom conversion; if 0,
  auto-infers from the first `.st` MRC header.
- `emit_star` overlays the five alignment columns and writes the
  `all_tilts.star` sidecar the legacy writer produced.
- Swapped in `drivers/ts_alignment.py:263`.

#### `services/tilt_series/adapters/ts_reconstruct.py` — `TsReconstructIngestAdapter`

- Per-TS: locates `{job_dir}/warp_tiltseries/reconstruction/{ts_id}_{rec_res}Apx.mrc`
  plus halves under `even/` and `odd/`; reads dims from the MRC header
  into `TsReconstructTomogramOutput`. Attached via
  `registry.attach_tomogram_output` — creates the `Tomogram` lazily.
- `emit_star` writes `tomograms.star` with absolute paths for reconstructed
  MRCs + resolved per-TS STAR paths + `rlnTomoTiltSeriesPixelSize` +
  `rlnTomoTomogramBinning`. Fails loud (vs. legacy's silent warn) when a
  per-TS STAR can't be resolved to an absolute path.
- Swapped in `drivers/ts_reconstruct.py:192`.

#### Stage 3 verification (pos9_10_after_pixShift)

Smoke tested against the on-disk reference STARs:

| Adapter           | Rows × cols compared | Result |
|-------------------|----------------------|--------|
| fs_motion_and_ctf | 10 × 8 (per-TS × 2 TS) | byte-for-byte match |
| ts_alignment      | 10 × 5 (per-TS × 2 TS) | byte-for-byte match |
| ts_ctf (regression)| 10 × 4 (per-TS × 2 TS)| byte-for-byte match |
| ts_reconstruct    | 2 × 6 (global block) | byte-for-byte match |
| Registry round-trip| all TS + outputs    | preserved |

The ts_ctf regression compare excludes `rlnTomoHand` because the adapter
writes it per-tilt while the legacy STAR carries it only on the global
block — pre-existing Stage 2 deviation, flagged but not owned by Stage 3.

Cleanup of `MetadataTranslator._merge_warp_metadata`, `_merge_ctf_metadata`,
`_load_all_tilt_series`, etc. is deferred to Stage 7 after a real-project
run exercises the swapped drivers.

### Stage 3 hotfixes (discovered on first real-project run) ✅ DONE

First run of the Stage-3-swapped drivers on `test_20251113_412` (151 TS,
18 selected) surfaced a latent registry-build bug. The subjobs all
succeeded; the supervisor then failed at `adapter.ingest` with "TS missing
from registry" on all 18 TS.

**Root cause**: `_build_and_persist_registry` was given the **source**
mdocs glob (`/groups/klumpe/001_Data/412/20251113_412/*.mdoc` — bare names
like `Position_11.mdoc`). But `DataImportService` copies mdocs into
`{project}/mdoc/` with a `{project_name}_` prefix, and `ts_import`
downstream writes TS names with that same prefix into `tilt_series.star`.
So registry stored bare `Position_11` IDs while the STAR used
`test_20251113_412_Position_11` → every adapter lookup missed.

The `pos9_10_after_pixShift` smoke test masked this because its source
mdocs happened to already be named `pos9_10_after_pixShift_Position_*.mdoc`
— source == prefixed by coincidence.

**Fixes** (in `services/scheduling_and_orchestration/project_service.py`):

1. `_build_and_persist_registry` now reads from `{project_dir}/mdoc/*.mdoc`
   (the post-import prefixed copies). Falls back to the source glob only
   if the project mdoc dir is empty.
2. `load_project_state` now detects drift: if `registry.tilt_series_ids()`
   is disjoint from the project mdoc stems, `clear_registry()` + rebuild.
   Stale-but-non-empty registries no longer silently persist.

**Fail-fast preflight** (`drivers/array_job_base.py`):

Added `preflight_registry(project_path, expected_ts_names, job_name)`.
Called by all four supervisors (`drivers/{fs_motion_and_ctf,ts_alignment,
ts_ctf,ts_reconstruct}.py`) **before `submit_array_job`**. If the registry
is empty or doesn't cover the expected TS set, the supervisor dies in <1s
with a diagnostic dump (registry TS count + head, expected head, missing
list) — converting "burn an hour on subjobs then fail aggregation" into
"fail at dispatch before wasting cluster time." Happy-path logs one line
per supervisor so "registry was fine at dispatch" is grep-able.

**Performance defaults bumped** (unrelated to the bug, driven by the
~1hr/18-TS fs_motion wall time observed):

- `FsMotionCtfParams.perdevice`: 1 → 2 (`services/jobs/fs_motion_ctf.py`)
- `job_resource_profiles.fsMotionAndCtf.mem`: 16G → 24G
  (`config/conf.yaml` + `config/conf.template.yaml`)

24G pairs with perdevice=2 because two concurrent EER movies per GPU need
~12–18GB plus host-side frame-staging buffer. Note in the template
explains the pairing; if someone pushes perdevice higher, they need to
bump mem in lockstep.

**UI hotfix** (unrelated, but surfaced during testing):

`ui/data_import_panel.py:do_delete` — NiceGUI race where `dialog.close()`
tore down the dialog's client before `ui.notify` ran after `await
shutil.rmtree`. Wrapped notifies in `_safe_notify` that swallows the
"client gone" RuntimeError; `scan_and_display_projects` refresh now runs
unconditionally.

---

## Stages remaining

### `ts_import` filter: resolved via legacy-compatible silent-skip (2026-04-13)

**Symptom (test3 then test4_20251113_412)**: 4 jobs run, all subjobs
succeed, supervisor dies at `TsAlignmentIngestAdapter.emit_star` with
"frame X has no alignment in registry output (tomostar did not include
this movie)" on ~6-8 frames per TS.

**Root cause — hardcoded WarpTools filter, not our code**:

`WarpTools/Commands/Tiltseries/ImportTiltseries.cs:335-350` contains a
contiguous-run intensity filter that walks outward from the 0-tilt
index, sorts tilts by angle, and truncates everything past the first
tilt whose `AverageIntensity >= MinIntensity * cos(angle) * MaxAverage *
0.999f` check fails. Verified via the WarpTools deepwiki:

- `AverageIntensity = median(every 10th pixel of the aligned-average
  MRC)`, raw pixel values, no normalization.
- Zero-mean motion-corrected MRCs (inherent to WarpTools' alignment
  algorithm) can produce ≤ 0 medians at high tilt.
- With `--min_intensity 0.0`, the threshold is 0, but `AverageIntensity
  < 0` STILL fails the `>=` check. CLI validation rejects negative
  `--min_intensity`, so the filter cannot be fully disabled.
- No CLI flag, env var, or settings field turns the walk off. No log
  message identifies dropped tilts.

Three other per-tilt drop conditions exist too (`UnselectManual`,
`|angle| > MaxTilt + 0.05`, `MaskedFraction > MaxMask`), with the same
silent behavior.

All downstream WarpTools commands (`ts_aretomo`, `ts_ctf`,
`ts_reconstruct`) treat the post-filter tomostar as the authoritative
frame set — confirmed via `TiltSeries.InitializeFromTomoStar` in
`WarpLib/TiltSeries/TiltSeries.cs:311-359`. Nothing downstream re-reads
`warp_frameseries/*.xml` for filtered frames.

**Resolution — match legacy CryoBoost silent-skip**:

Checked `CryoBoost/src/warp/tsAlignment.py:132-140` — the authoritative
prior implementation iterates over the tomostar (the smaller, filtered
set) and overlays columns onto the per-TS STAR via position lookup.
Rows in the STAR without a matching tomostar entry are simply left with
their prior values (alignment columns absent / NaN). Output STAR retains
all N rows; the k filtered rows are cosmetic because downstream reads
the tomostar.

Applied the same pattern to our adapters:

- `services/tilt_series/adapters/ts_alignment.py
  :_apply_alignment_to_tilt_df` — on a frame with no alignment entry,
  `continue` instead of `errors.append`. Initialize the five
  `rlnTomo{X,Y}Tilt/ZRot/{X,Y}ShiftAngst` columns to NaN at top of loop
  so the schema stays consistent across rows.

- `services/tilt_series/adapters/ts_ctf.py:_apply_ctf_to_tilt_df` —
  same: skip missing-CTF rows, retain the per-frame defocus values that
  `fs_motion_and_ctf` wrote. No hard error.

`ts_reconstruct` iterates per-TS (not per-frame) and is unaffected.

Both adapters log a per-TS INFO line with the skip count, so the gap
is observable without being a failure.

**Rejected alternative — Stage 4.5 registry-aware filter tracking**

Originally proposed adding `Frame.is_filtered_at_import = True` set by
the ts_import driver via mdoc-vs-tomostar diff, with adapters iterating
over `ts.active_frames()`. Rejected: the legacy idiom is simpler,
battle-tested, and achieves the same downstream result (reconstructions
use only the aligned subset, because the tomostar is authoritative).
If future work needs to query "which frames did WarpTools drop and
why", that's a registry enhancement on its own merits — not tied to
this failure mode.

**Defaults unchanged**: `TsImportParams.min_intensity` stays at 0.0
(bumped from 0.3 yesterday). Inline DANGER comment on that field now
accurate — `min_intensity=0` does NOT fully disable the filter, but we
tolerate the drops via legacy-compatible skipping rather than relying
on the flag.

### Stage 4 — Pre-submit STAR emission from registry

- `services/tilt_series/star_serializer.py` — one serializer per job type.
  Takes `(registry, ts_ids_in_scope, job_dir)`, writes the required STAR
  layout the driver expects as INPUT.
- `services/scheduling_and_orchestration/pipeline_orchestrator_service.py`
  `deploy_and_run_scheme()` — currently shuffles existing STARs; change
  to invoke serializers.
- `services/io_slots.py` / `services/path_resolution_service.py`: shift
  from "find upstream STAR" to `registry.outputs_for(upstream_instance_id,
  ts_ids)`.
- **Done check**: `rm -rf {project}/External/*/.*.star && restart server
  && open project` — registry rewrites every STAR, RELION/WarpTools see
  no difference.

### Stage 4B — Per-tomogram array jobs for downstream consumers

**Problem.** The per-TS parallelization refactor (Stages 0–3) covered jobs
from `ts_import` through `ts_reconstruct`. Downstream tomogram consumers
— `template_match_pytom`, `extract_candidates_pytom`,
`subtomo_extraction` — are still **monolithic**: one SLURM job iterates
sequentially over every tomogram in `tomograms.star`. Confirmed on
`gt_pos9_10_newinfra/External/job007`: no `.staging/`, no
`.task_manifest.json`, no `task_*.out`; the run.out shows
`[DRIVER] Processing ..._Position_10...` → `..._Position_9...` inside a
single process.

This does not scale past ~20 tomograms. TM alone at 12° angular sampling
runs ≥9000 angles × tomogram volume, which is tens of minutes of GPU
time per tomogram; a 100-tomogram project would need >10h of walltime in
one job, exceeding every QOS cap the cluster advertises. It also
serializes what is embarrassingly parallel work — each tomogram is
independent, so every queued GPU on the cluster could be consumed at
once.

**Goal.** Apply the existing supervisor/array-task pattern
(`drivers/array_job_base.py` — `read_manifest`, `submit_array_job`,
`wait_for_array_completion`, `collect_task_results`,
`preflight_registry`) to the three downstream drivers. Iterate over
**tomograms, not tilt-series**; for v1, tomogram IDs are 1:1 with TS IDs
so they inherit straight through via `Tomogram.tilt_series_id`. Each
task processes exactly one tomogram.

**Ordering.** Ship in this order so the biggest pain is relieved first:

1. **`template_match_pytom`** — highest impact. GPU-bound, per-tomogram
   independent, currently the longest-running job in the pipeline. This
   is what makes the 100-TS target feasible at all.
2. **`extract_candidates_pytom`** — uniformity. Work per tomogram is
   light (seconds to a minute) so arrayification is pure overhead at
   small scale, but doing it the same way as the others is worth more
   than the microseconds saved. Request modest SLURM resources per task
   (e.g. `mem=4G`, `time=0:15:00`, no GPU).
3. **`subtomo_extraction`** — same pattern. Already reads
   `particles.star` grouped by tomogram; task-per-tomogram is a
   mechanical refactor. (Verify `subtomo_merge.py` still runs as a
   monolithic post-aggregation step — it consumes all per-tomogram
   outputs to produce `merged.mrc` + `particles.star`, that's the
   supervisor's aggregation phase.)

**Design points — same across all three drivers:**

- **Manifest source = registry, not STAR.** Supervisor does
  `registry.tilt_series_ids()` → `[tomogram_id, ...]` (1:1 for v1) and
  writes `.task_manifest.json` with `{ts_name, tomogram_path,
  per_tilt_files...}`. This eliminates the "read tomograms.star, parse
  rows, guess IDs" dance in the current drivers.
- **Per-task SLURM shape differs per driver.** TM wants one GPU per
  task and `%M` concurrency throttle matching available GPUs (TM
  already uses ~32G on a single A100; `--array=0-N%4` is a reasonable
  default for a 4-GPU allocation). extract/subtomo want small CPU-only
  tasks. Add a per-driver `supervisor_slurm` profile next to
  `fsMotionAndCtf.mem` in `config/conf.yaml` so the shape is declarative.
- **Staging strategy mirrors ts_ctf.** Each task gets a
  `.staging/task_{tomogram_id}/` with symlinks to the single tomogram
  MRC + per-tilt `.tlt`/`.defocus`/`.dose` txt files + template/mask.
  Isolates concurrent Warp/pytom processes, keeps failure blast radius
  to one tomogram.
- **Preflight.** Supervisors call `preflight_registry(project_path,
  expected_tomogram_ids, job_name)` before `submit_array_job`. Fails
  in <1s if a tomogram is missing its reconstruction output, instead of
  burning GPU time then failing aggregation.
- **Aggregation phase.** Each supervisor's post-array step collects
  per-tomogram artifacts:
  - TM: merge per-tomogram `tmResults/{ts_id}*.mrc` into the job's
    `tmResults/` dir and emit `tomograms.star` with TM result paths
    attached via `registry.attach_tomogram_output`.
  - extract: concatenate per-tomogram `candidates.star` rows into the
    job's `candidates.star`.
  - subtomo: write `optimisation_set.star` + global `particles.star`
    from per-task `Subtomograms/` outputs (already the pattern, just
    dispatched differently).

**Dispatcher extraction (opportunistic).** Four supervisors
(`fs_motion_and_ctf`, `ts_alignment`, `ts_ctf`, `ts_reconstruct`) now
have near-identical supervisor bodies. The new three will duplicate the
same 150 lines. Good moment to extract `ArrayJobDispatcher` from
`ts_reconstruct.py` (the blueprint) into `drivers/array_job_base.py` so
all seven supervisors call `dispatcher.run(ts_ids, build_task_command,
aggregate_fn)` instead of re-implementing manifest-write →
submit-array → poll → aggregate. This was noted in the original plan
memory but deferred; Stage 4B is the right time because the need is
now concrete.

**Files to touch:**

```
drivers/template_match_pytom.py          — split into supervisor/task
drivers/extract_candidates_pytom.py      — split into supervisor/task
drivers/subtomo_extraction.py            — split into supervisor/task
drivers/array_job_base.py                — extract ArrayJobDispatcher
services/jobs/template_match.py          — add supervisor_slurm profile
services/jobs/candidate_extract.py       — add supervisor_slurm profile
services/jobs/subtomo_extraction.py      — add supervisor_slurm profile
config/conf.yaml, conf.template.yaml     — new per-driver SLURM blocks
```

**Done check.** Run `gt_pos9_10_newinfra` (2 tomograms) post-refactor
and verify TM/extract/subtomo each show `task_0.out` + `task_1.out`
files with independent SLURM job IDs, and total wall time ≈ max per-task
time + supervisor overhead (rather than sum). Then run on a ≥20-TS
project and verify `--array=0-N%M` concurrency actually dispatches in
parallel.

**Non-goals for this stage.** 3D classification (`class3D`), refinement
(`Refine3D`), CTF refinement are all RELION-native multi-GPU/MPI jobs
with their own parallelization model — not part of this stage. Leave
them alone.

### Stage 5 — Validation gates

- `registry.can_run(JobType.TsCtf, ts_ids)` → `(ok: bool, missing: Dict[ts_id, List[frame_id]])`
- UI disables Run buttons + shows per-TS "missing motion correction on
  frames X, Y".
- Drivers call `registry.assert_complete(job_instance_id, expected_ts_ids)`
  as preflight. Catches the "job claims success but is missing outputs"
  failure mode at dispatch time.

### Stage 6 — Scale hardening (for 100+ TS projects)

- `TiltSeriesRegistry` — lazy per-TS load: `get_tilt_series(id)` reads
  `registry/tilt_series/{id}.json` on demand; `all_tilt_series()` walks
  the index.
- Keep the current in-memory cache but add LRU bounds.
- Per-TS atomic writes already in place. Benchmark: 200-TS project load
  in <1s cold cache; attach one output = 1 file touch + <5ms.

### Stage 7 — Retire the old machinery

- `WarpXmlParser` → private helper inside the tsCTF / fs_motion_ctf
  adapters only.
- `MetadataTranslator` → deleted.
- `cryoBoostKey` as a concept → gone. `Frame.id` is the only identity.
- `services/tilt_series_service.py` (flat module) merges into the
  `services/tilt_series/` package; `filter_good_tilts` becomes a registry
  method.
- Grep check: no `cryoBoostKey`, no `Path(x).stem` as an identity source,
  no `_EER` replace-chains outside the `WarpXmlParser` internals.

---

## Things to watch out for

1. **Driver environment.** Drivers are standalone Python scripts launched
   by SLURM. They load the registry via `get_registry_for(project_path)`,
   which reads the sidecar off disk. If the SLURM node can't read
   `{project}/registry/`, the driver fails. Should be fine because the
   project dir is always a shared filesystem path on the HPC side, but
   verify on the first real run.

2. **Registry is a new on-disk artifact.** Backing up a project now means
   backing up `project_params.json` PLUS `registry/`. Worth mentioning in
   any project-archive tooling.

3. **Species-scoped instances** (the `__` separator in instance_ids) are
   orthogonal to TS identity. Each species-scoped job has a distinct
   `job_instance_id`, so its outputs coexist under the same `Frame` /
   `TiltSeries` without collision.

4. **Filter tilts state.** Currently `ProjectState.tilt_filter_labels`
   (keyed by `cryoBoostKey`). Stage 3 or 5 should migrate this to
   `Frame.is_filtered: bool` on the registry side; `tilt_filter_panel.py`
   becomes a registry writer. Note: WarpTools `ts_import` also drops
   frames silently (see "`ts_import` filter: resolved…" section above).
   Our ts_alignment/ts_ctf adapters now tolerate this by skipping
   un-matched rows, matching legacy CryoBoost behavior. If a future
   feature needs to query "which frames did WarpTools drop and why",
   wire `Frame.is_filtered_at_import` as a separate registry field then.

5. **Testing.** No test suite exists. Recommended minimum tests before
   Stage 7:
   - `test_warp_xml_parser.py`: synthetic XMLs with missing/skewed Z
     nodes, divergent grid sets
   - `test_tsctf_ingest_adapter.py`: synthetic registry + XML → every
     frame's CTF lands on the right frame
   - `test_registry_migration.py`: load a real pos9-style project, verify
     registry shape matches hand-enumerated truth

---

## File map (what's been added this refactor)

```
services/tilt_series/
├── __init__.py                        # re-exports
├── models.py                          # Frame, TiltSeries, Tomogram, output unions
├── registry.py                        # TiltSeriesRegistry + singleton
├── build.py                           # build_from_{dataset_overview, mdocs}
└── adapters/
    ├── __init__.py                    # re-exports (all 4 adapters)
    ├── ts_ctf.py                      # TsCtfIngestAdapter (Stage 2)
    ├── fs_motion_ctf.py               # FsMotionCtfIngestAdapter (Stage 3)
    ├── ts_alignment.py                # TsAlignmentIngestAdapter (Stage 3)
    └── ts_reconstruct.py              # TsReconstructIngestAdapter (Stage 3)

TS_REGISTRY_REFACTOR_PLAN.md           # this file
```

Modified:

- `services/configs/metadata_service.py` — Stage 0 bug fixes
- `backend.py` — `registry_for(project_path)` accessor
- `services/scheduling_and_orchestration/project_service.py` —
  `_build_and_persist_registry` (read from project mdoc dir, not source
  glob) + `load_project_state` drift detector (Stage 3 hotfix)
- `drivers/array_job_base.py` — `preflight_registry` helper (Stage 3
  hotfix)
- `drivers/ts_ctf.py` — swapped to adapter (Stage 2) + preflight call
- `drivers/fs_motion_and_ctf.py` — swapped to adapter (Stage 3) + preflight
- `drivers/ts_alignment.py` — swapped to adapter (Stage 3) + preflight
- `drivers/ts_reconstruct.py` — swapped to adapter (Stage 3) + preflight
- `services/jobs/fs_motion_ctf.py` — `perdevice` default 1 → 2
- `config/conf.yaml`, `config/conf.template.yaml` — fsMotionAndCtf
  `mem` 16G → 24G
- `ui/data_import_panel.py` — `_safe_notify` to swallow NiceGUI
  client-gone race in the delete-project dialog

---

## Quick reference for the next session

Run the smoke test end-to-end:

```python
# from project root, using project's venv python
from pathlib import Path
from services.tilt_series import TiltSeriesRegistry, clear_registry
from services.tilt_series.build import build_from_mdocs
from services.tilt_series.adapters import TsCtfIngestAdapter

proj = Path("projects/pos9_10_after_pixShift")
clear_registry(proj)

ts_list = build_from_mdocs(str(proj / "mdoc" / "*.mdoc"),
                           frames_dir=proj / "frames")
registry = TiltSeriesRegistry(proj)
for ts in ts_list:
    registry.add_tilt_series(ts)

adapter = TsCtfIngestAdapter(registry, proj / "External" / "job004")
adapter.ingest(registry.tilt_series_ids())
adapter.emit_star(
    proj / "External" / "job003" / "aligned_tilt_series.star",
    Path("/tmp/ts_ctf_out.star"),
)
```

Lint across refactor files (E402 on drivers is pre-existing
`sys.path.insert` boilerplate; ignore):

```bash
ruff check services/tilt_series/ backend.py \
  services/scheduling_and_orchestration/project_service.py \
  drivers/array_job_base.py drivers/ts_ctf.py drivers/fs_motion_and_ctf.py \
  drivers/ts_alignment.py drivers/ts_reconstruct.py
```

## How to validate on a real project

If the 4 pipeline jobs fail at aggregation with "TS missing from
registry", reload the project once. The drift detector in
`load_project_state` will log `"Registry drifted from project mdocs;
rebuilding from mdocs"` and fix it, then retry the job. On next-run
supervisor start you'll see:

```
[<job_name>] PREFLIGHT: registry has N TS, M frames total
[<job_name>] PREFLIGHT: registry TS head: [...]
```

If PREFLIGHT fires the failure path instead, the message dumps the
expected head, registry head, and first 10 missing TS — straight to the
diagnosis without a round-trip to the subjob logs.

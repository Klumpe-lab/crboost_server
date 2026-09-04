# Roadmap 14 — Protocols + the copia regression harness

> **Superseded 2026-09-04 — see roadmap 16.** The harness described here (frozen input, run.json
> verdicts, record / bless / bands, INFRA classification, site snapshots, Tier A snapshots,
> `crboost_regress.py`, the RELION scheme export) was DELETED on 2026-09-04: a protocol is the shape of
> a pipeline with its parameters, nothing about results. What survives from this roadmap is
> `services/protocols/{schema,discovery,export,apply}.py`, `crboost_protocol.py` and the copia bundle.
> Sections §3 "RELION scheme derivation" / "Runner + verdicts" / "Per-stage checks" / "Tier A", §4
> S4–S5 / S7 and §8 describe deleted code and are kept as history only.

**Status:** S0–S7 **CODE-COMPLETE 2026-08-27**, built back-to-back on code-review confidence
(maintainer standing decision 2026-08-17: verify ONCE at the end). Design of record is
`FEATURE_recipes.md` (2026-08-11); decisions of 2026-08-26/27 are folded in below. Runtime owed:
§8. Not derived from the 2026-08-10 audit — it comes from the standing wish for a whole-system
regression test and from "Protocols" as a sharing primitive.

## Before → After

**Before:** the repo has zero tests. The only way to know whether a change broke the pipeline is
to run a dataset by hand and look. A workflow that worked (the v1 copia tutorial, a colleague's
412 chain) lives in one project's `project_params.json` and in people's heads; there is no
portable form of it. `config/Schemes/warp_tomo_prep/` is dead code (a lone `scheme.star`, no
`job.star` templates; every loader guarded on files that never existed).

**After:** a **Protocol** is a bundle directory (`protocol.yaml` + `assets/` + optional `test/`)
that any project can be exported to and any dataset can be created from. The copia bundle
(`config/protocols/copia-empiar12580/`) is the first: apply it to the frozen EMPIAR-12580 input,
and `crboost_regress.py run copia-empiar12580` — or the **Protocols** button on the landing strip
— submits the 13-stage chain, waits, extracts per-stage metrics and returns PASS / DRIFT / FAIL /
INFRA per stage with a report. `record` ×3 then `bless` produce the bands from observed values
(never invented). A protocol also derives a vanilla RELION `Schemes/<name>/` on demand
(`crboost_protocol.py scheme`), which is the v1-compatibility instrument the group asked to keep
around — derivable, not first-class. **Gain:** every further change to drivers, resolver, registry
or orchestrator has a net under it; a workflow becomes a file you can hand someone.

---

## 1. Verified facts (2026-08-26/27)

- Headless path exists end to end: `backend.create_project_and_scheme` → species mutators →
  `state.ensure_job_initialized` + `setattr` (gated by `USER_PARAMS` / status) →
  `pipeline_orchestrator.deploy_and_run_scheme` (afterok DAG when `use_afterok_orchestrator`)
  → `pipeline_runner.reconcile_afterok` until `pipeline_active` clears. `services/`,
  `drivers/`, `backend.py` import no `ui`.
- `CRBOOST_PRINT_CMD=1` dry-run existed in `run_command` only — a supervisor honoured it in its
  tasks and still sbatched a real array (fixed in S7).
- Reference run `/groups/klumpe/crboost_data/Copia_Aaron_Tutorial`: 11 stages, 28/28 tilts kept
  by tsImport, TM @12° = 9000 rotations, 816 candidates → 783 particles, class3d it015
  `rlnCurrentResolution` 13.77 Å (its "failure" was the since-fixed numbered-optset check). Its
  template/mask assets are the copia bundle's `assets/`. Its frames are dangling symlinks into an
  unmounted `001_Data` — hence EMPIAR is the source of the frozen input.
- EMPIAR-12580 `data/Copia_TiltSeries/` = `Position_1.mdoc` + 28 EER (~11 GB), basenames
  identical to the reference project's (listing verified 2026-08-27).
- No RNG seed existed anywhere but `drivers/miss_align.py`; `Class3DParams` had no
  `random_seed` (added in S2).
- The resolver's `source_overrides` keys embed the producer's job directory
  (`tsReconstruct:External/job005`); for a producer that has not run, the key is
  `External/pending_<iid>` and stops matching the moment the producer is deployed. That is why
  a protocol's explicit `inputs` wiring is REPORTED, not applied (§3 apply contract).
- The healpix corruption trap is real in the reference project (`"healpix_order": 4.0`); apply
  coerces every value through the field's own type, so a protocol can never re-plant it.

## 2. Layout

```
services/protocols/
  schema.py        Protocol / ProtocolSpecies / ProtocolStage (+ Expectation, assets) · yaml load/dump · schema_fingerprint
  discovery.py     config/protocols/ + ~/.crboost/protocols/ → ProtocolInfo rows (broken bundles listed with their error)
  export.py        project → Protocol (authoring tool; copies species assets into assets/)
  apply.py         validate → create project → species + assets → stages (coerced) → save; warnings, never silent
  scheme_export.py Protocol + applied project → Schemes/<name>/ (scheme.star + per-stage job.star), relion_schemer-runnable
services/regress/
  inputs.py        test/input.yaml: presence · size · sha256; freeze-input; EMPIAR download commands
  checks.py        per-stage metric extractors (registry / star / MRC-header readers; overlap + map CC net-new)
  bands.py         bands.yaml load · evaluate (PASS/FAIL/DRIFT) · propose (bless)
  infra.py         INFRA signature table · sacct node lookup (NO requeue, 2026-08-14)
  report.py        run.json · metrics.json · report.md · run listing · retention (last N run dirs)
  runner.py        run_protocol() · record · bless · site snapshot
  snapshot.py      Tier A: materialized scheme, normalized, diffed against test/snapshots/
crboost_regress.py  fetch-input | freeze-input | run | record | bless | report | snapshot
crboost_protocol.py list | validate | export | apply | scheme
ui/protocols_dialog.py + the "Protocols" house_button on ui/landing_status_strip.py
config/protocols/copia-empiar12580/{protocol.yaml, assets/copia_{template,mask}.mrc, test/input.yaml}
services/configs/config_service.py  LocalDataConfig(root) · local_data_root · is_tool_configured
```

On disk, outside the repo (`local_data.root`, default `<DefaultProjectBase>/local_data` —
deliberately not under the project base so the server's monitor never adopts a CLI run):

```
local_data/input/<protocol>/            the frozen dataset (read-only after download)
local_data/runs/<protocol>-<stamp>/     project/ · run.json · metrics.json · report.md · runner.log   (last 3 kept)
local_data/baseline/<protocol>/records/<stamp>/{metrics.json, run.json, site.yaml, artifacts/}   (forever)
local_data/baseline/<protocol>/blessed/{candidates.star, merged.mrc, class3d.mrc}               (promoted by bless)
local_data/snapshots/<protocol>/project  scratch apply for Tier A
```

## 3. Design

### Protocol YAML (copia, abridged — full file in the bundle)

```yaml
name: copia-empiar12580
version: 1
provenance: {empiar: EMPIAR-12580, empiar_path: data/Copia_TiltSeries, doi: …, tutorial: …}
expects: {pixel_size_angstrom: {about: 2.95, tol: 0.05}, tilt_series_count: {about: 1, tol: 0}}
species:
  - {id: copia, name: Copia, diameter_ang: 550, symmetry: I1,
     template: {asset: assets/copia_template.mrc, polarity: black, source: "basic_shape:550:550:550", lowpass_ang: 45},
     mask:     {asset: assets/copia_mask.mrc, method: relion, threshold: 1.85, extend_pixels: 5, soft_edge_pixels: 5, lowpass_ang: 20},
     extraction: {box_size: 384, crop_size: 224, binning: 1.0}}
stages:                      # order = pipeline_order; params = FULL USER_PARAMS snapshot; schema_fingerprint per stage
  - {job: importmovies, params: {…}}
  - …
  - {job: templatematching, species: copia, params: {angular_search: "90.0", symmetry: C1, …}}   # template/mask from the species assets
  - {job: class3d, species: copia, params: {…, healpix_order: 4, random_seed: 1}}
```

Three layers (FEATURE_recipes §1): dataset facts come from the mdocs at apply and are only
*expected* (warn, never block); workflow decisions are the payload; site config never appears —
`test/site.yaml` pins it separately. `schema_fingerprint` = sha1[:12] over the sorted
`USER_PARAMS` names of the param class. No absolute path anywhere in a bundle.

### Apply contract (`apply_protocol(backend, protocol, *, project_name, project_base_path, movies_glob, mdocs_glob, gain_reference_path=None, shared=False)`)

1. `validate_protocol` before any disk write: job types, duplicate instances, declared species,
   every stage's tool configured (`ConfigService.is_tool_configured` — the bare-binary fallback of
   `get_tool_config` is NOT counted), `JobSpec.prerequisite`/`dependencies` present as stages
   (nothing is auto-added), assets on disk, template apix == `tsReconstruct.rescale_angpixs`.
2. mdoc autodetect → `create_project_and_scheme(selected_jobs=[])` (registry built, RELION
   projdir initialised, `qsub.sh` copied) with an import summary derived from the mdoc glob.
3. `expects` vs microscope/registry → warnings.
4. Species: `add_species` (id forced to the protocol's id) → diameter/symmetry/notes/extraction
   → assets copied by `species_admin.ingest_template_file` (lifted from the import dialog, which
   now calls it) → `register_template` / `register_mask`.
5. Stages: `ensure_job_initialized(job_type, instance_id)`; each param coerced through
   `TypeAdapter(field.annotation)` (enums from strings, floats never into int fields); unknown
   fields → warning, NOT applied; rejected values → warning, default kept; fields the protocol does
   not cover → listed. Species-shaped defaults exactly as the pipeline builder snapshots them (TM
   template/mask/symmetry, extract diameter, subtomo geometry) only where the stage does not pin
   them. `inputs` wiring → warning (§1, resolver limitation), automatic selection used.
6. `pipeline_order = stage ids`, one forced save. Returns `ok(project_path, stages, warnings,
   load_warnings)`.

### RELION scheme derivation (`export_relion_scheme(protocol, project_dir=…)`)

Works on a detached `ProjectState.load` of an APPLIED project (never registered, never saved),
predicts job dirs the way the schemer path does (existing `relion_job_name` kept, fresh stages
numbered after the highest known), resolves paths per stage, writes `Schemes/<name>/<iid>/job.star`
via the job model's own `generate_job_star` and `scheme.star` via `write_scheme_star` — the
function the deploy path now shares (extracted from `_write_scheme_star`). Runnable with
`relion_schemer --scheme <name> --run` in that project; site-flavoured (absolute server dir,
container wrapping inside the drivers). Pure-native RELION job.stars and the v1 wrapper-executable
form stay deferred (FEATURE_recipes §6 tiers 2–3).

### Runner + verdicts (`run_protocol(backend, protocol, *, mode="run"|"record", progress_cb, keep_runs=3, poll_secs=15, max_wall_hours=14)`)

0. **Assertion #0** — `test/input.yaml` vs `input/<protocol>/`: presence always; size + sha256
   once `freeze-input` wrote them (until then the report says "input not frozen"). Mismatch → INFRA.
1. Site snapshot (tool path/size/mtime) vs `test/site.yaml` → annotations ("container changed");
   no digests (10 GB sifs).
2. `apply_protocol` into `runs/<protocol>-<stamp>/project`; `use_afterok_orchestrator` forced on
   (the chain must live in SLURM, not in the CLI process); apply warnings + any `load_warnings`
   annotated (a green run has none).
3. `deploy_and_run_scheme`; poll `reconcile_afterok` every 15 s with `progress_cb(done, total,
   "running: …")`; wall-clock cap cancels the chain.
4. Per stage, in order: SUCCEEDED → metrics (`checks.py`) → `bands.yaml` → PASS / DRIFT (in band,
   > 3σ from the recorded mean) / FAIL / UNBANDED; first non-succeeded stage → INFRA when
   `infra.py` finds a signature in `run.err`/`run.out`/`task_*.{err,out}` (node named via `sacct`)
   else FAIL; every later stage SKIPPED. Cross-stage: `n_particles_le_candidates`,
   `particles_conserved`. Overall = worst of INFRA > FAIL > DRIFT > PASS.
5. `run.json` + `metrics.json` + `report.md` + `runner.log`; retention prunes run dirs beyond the
   newest 3 (only dirs carrying our `run.json`).

`record` = run + `baseline/<protocol>/records/<stamp>/{metrics.json, run.json, site.yaml,
artifacts/}` (candidates.star, merged.mrc, class3d.mrc). `bless` = `propose_bands` over the
records (observed min/max; exact where every record agrees; mean/std from n ≥ 3 — no widening,
no invented tolerance) → `test/bands.yaml` (or `bands.proposed.yaml` beside an existing one) for
the human to edit; newest record's artifacts → `blessed/`; its site → `test/site.yaml`.
Overlap / CC metrics compare against `blessed/`, so records taken before the first bless carry
none: record ×3 → bless → record ×3 → bless again to band them.

### Per-stage checks (copia)

| Stage | Metrics | Reader |
|---|---|---|
| importmovies | `n_tilt_series`, `n_tilts` (28) | `Import/jobNNN/tilt_series{,/*.star}` |
| fsMotionAndCtf | `n_frames`, defocus µm mean/min/max, `ctf_res_ang_median/max`, `motion_median` | registry `FsMotionCtfFrameOutput` |
| tsImport | **`tilt_ids` exact set**, `n_tilts`, `n_tomostars` | `tomostar/*.tomostar` `_wrpMovieName` stems (guards WarpTools' hardcoded high-tilt filter drifting) |
| aligntiltsWarp | `shift_ang_max/median`, `tilt_y_span_deg`, `tilt_axis_deg_mean` | registry `TsAlignmentTiltSeriesOutput.per_frame` |
| tsCtf | defocus band, `are_angles_inverted` exact | registry `TsCtfTiltSeriesOutput` |
| tsReconstruct | `size_x/y/z`, `pixel_size_ang` exact; `intensity_mean/rms` | registry `TsReconstructTomogramOutput` + MRC header |
| denoisetrain | `model_exists`, `model_bytes` | job dir |
| denoisepredict | `n_denoised`, header mean/rms | registry `DenoisePredictTomogramOutput` |
| templatematching | `n_rotations` exact, `score_max`, `score_rms_mean` | `tmResults/*_job.json`, scores MRC header |
| tmextractcand | `n_candidates`, `lcc_max/median`, **`overlap_frac`** vs blessed within r = Ø/2 | `candidates.star` |
| subtomoExtraction | `n_particles`, `n_particles_le_candidates` | `particles.star` |
| reconstructParticle | `merged_exists`, `cc_vs_blessed` | `merged.mrc` |
| class3d | `completed`, `resolution_ang` (~13.8), `accuracy_rot_deg`, `accuracy_trans_ang`, `n_particles`, `particles_conserved`, `cc_vs_blessed` | `run_it015_{model,data}.star`, class map |

Never asserted: `rlnCtfMaxResolution`, `rlnAccumMotion*` (WarpTools placeholders).

### Tier A (`crboost_regress.py snapshot`)

Materialize the scheme of an applied project (scratch apply under `local_data/snapshots/`),
normalize (`$PROJECT`, `$SERVER`, `$HOME`, starfile timestamps, `$SCHEME`), diff against
`test/snapshots/`; `--update` records the golden copy. Plus the `ArrayDriver.run_supervisor`
gate: under `CRBOOST_PRINT_CMD` every per-item command is rendered through `run_command`'s echo
and nothing is submitted. Driver transcripts against a completed run dir stay deferred (they would
overwrite that run's manifest).

## 4. Stages (each a self-contained commit)

| # | Stage | Record |
|---|---|---|
| S0 | this doc + README row; download commands to the user | **done 2026-08-27** |
| S1 | `LocalDataConfig` + template key · `services/protocols/{schema,discovery,export,apply(validate only),scheme_export}` · copia bundle (hand-authored from the reference run + tutorial pins, fingerprints computed) · `write_scheme_star` extracted · **rider:** `config/Schemes/` deleted, `backend.get_available_jobs` deleted, `ensure_job_initialized(template_path=)` dropped at all 5 call sites, `preflight.py` Schemes check + `CLAUDE.md` line replaced | **done** |
| S2 | `Class3DParams.random_seed` (−1 = RELION time seed; ≥ 0 → `--random_seed`) in USER_PARAMS → auto-rendered in the job tab | **done** (behaviour-preserving) |
| S3 | apply engine + `species_admin.ingest_template_file` (dialog repointed) | **done** |
| S4 | `services/regress/*` + `crboost_regress.py` + `crboost_protocol.py` | **done** |
| S5 | record → bless (code); the three record runs are runtime | **code done**; runs owed |
| S6 | `ui/protocols_dialog.py` + strip button (Run/Record → `BackgroundTask`, tray progress, toast) | **done** |
| S7 | supervisor print-cmd gate · `snapshot.py` · `snapshot` command | **done** |

## 5. Modern-Python weave-in

`match` over `JobType` in `checks.collect_stage_metrics` (a per-type dispatch that is not a
module-level JobType table — R4 stays clean); `pydantic.TypeAdapter` for field-typed coercion at
apply; `dataclass` records (`StageResult`, `RunRecord`) serialised with `asdict`; `StrEnum` values
already in place for origins.

## 6. Behaviour changes (quarantined, explicit)

- `config/Schemes/` and every `template_path` plumbing is gone. Nothing read it (`job.star`
  templates never existed); job instances always took their code defaults and still do.
- `CRBOOST_PRINT_CMD=1` on an array supervisor now prints and exits 0 instead of sbatching.
- `Class3DParams` gains `random_seed` (default −1 → identical command line to before).

## 7. Risks

- Denoise on the critical path makes TM picks stochastic → the first bless may propose wide
  `n_candidates` / `overlap_frac` bands; record mode measures it. Fallback = a `tm_on_denoised:
  false` variant protocol.
- Single-TS coverage: aggregation / merged-optset paths untested (accepted).
- Run cost ≈ 15–25 GB, 3–4 h wall (denoise train dominates); QOS default 8 h per job suffices.
- The CLI must run under the qsub.sh module environment; the button path inherits the server's.
- `relion --do_projdir` runs in the relion container at apply (seconds; needs the container).

## 8. Runtime checklist (the user's step — nothing here can run from the assistant sandbox)

```
# static gates
venv/bin/ruff check .
venv/bin/python3 check_boundaries.py
venv/bin/python3 -c "import main"
venv/bin/python3 crboost_protocol.py list
venv/bin/python3 crboost_protocol.py validate copia-empiar12580

# S0: the frozen input (~11 GB, once) — EITHER the landing page Protocols → Download button (tray progress)
venv/bin/python3 crboost_regress.py fetch-input copia-empiar12580        # OR this: resumable HTTPS fetch + freeze; --commands prints a wget script
# both write sizes + sha256 into test/input.yaml and lock the dir read-only — commit test/input.yaml afterwards

# S3/S1 sanity without the cluster
venv/bin/python3 crboost_protocol.py apply copia-empiar12580 --name copia_apply_test --base /tmp/crb --movies '/groups/klumpe/crboost_data/local_data/input/copia-empiar12580/*.eer' --mdocs '/groups/klumpe/crboost_data/local_data/input/copia-empiar12580/*.mdoc'
venv/bin/python3 crboost_protocol.py scheme copia-empiar12580 --project /tmp/crb/copia_apply_test
venv/bin/python3 crboost_regress.py snapshot copia-empiar12580 --project /tmp/crb/copia_apply_test --update

# S4: the first end-to-end run (hours)
venv/bin/python3 crboost_regress.py run copia-empiar12580                # expect every stage UNBANDED, none FAIL/INFRA
# S5: three records, then bless; edit test/bands.yaml; commit
venv/bin/python3 crboost_regress.py record copia-empiar12580   # ×3
venv/bin/python3 crboost_regress.py bless  copia-empiar12580
venv/bin/python3 crboost_regress.py run    copia-empiar12580             # first banded run → PASS
# S6: restart python main.py, landing page → Protocols → Run → tray progress → toast with the report path
```

Confirm `relion_refine --random_seed` is honoured under `--ios` by comparing two `record` runs'
class3d `resolution_ang` (O4 of the design doc).

## 9. Deferred

Mid-project protocol grafting · explicit `inputs` wiring at apply (needs instance-id override keys
in the resolver) · native RELION job.stars / v1 wrapper form of the scheme export · 26S
multi-species variant · driver transcripts in Tier A · cadence/cron.

**Continued in roadmap 16 (2026-09-03):** the runner inversion (a run IS a project; `launch_run` /
`evaluate_run` / CLI-only `wait_for_settle`), `ProjectState.protocol_origin`, the workspace
Protocols view with the builder "edited" chips FEATURE_recipes §7 asked for, and the fixes for the
second-observer, root-logger, cancel-orphans-chain and prune-deletes-project defects found in
the S4–S6 code here.

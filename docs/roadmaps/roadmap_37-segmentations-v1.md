# Roadmap 37 — Segmentations v1: produce, ingest, show, and restrict template matching

**Status:** scoped 2026-10-20. S0 done except S0.5 (container built, weights frozen, one-tomogram run recorded in
the S0 record). S1 → S3 built 2026-10-20 on `segmentations` (ruff-clean, not run; build record §6); stopped
for the maintainer's runtime check (§6.4). Next: S4 → S7. Background survey:
`docs/reports/tool-survey/segmentation-and-annotation-models-2026-10.md` (tool I/O contracts, pitfalls, sources).

**In one line.** easymode (and Ais models) segment a project's tomograms as a per-tomogram job; segmentations
made outside crboost can be registered; every segmentation is tied to a feature, a tomogram and the
reconstruction it was computed on; the gallery (first), then the Journey and pick viewer, show them as
transparent fills over the slabs we already draw; ChimeraX opens them as surfaces; template matching can be
restricted to a feature region.

**Not in v1:** meshes, sparse geometry (curves, spheres, oriented points from surfaces), copick, skeletonization,
context rules on particle sets, picking from segmentations, MemBrain/TARDIS/SABER as jobs (MemBrain outputs come in
through ingest), Z scrubbing, pre-rendered 3D pictures, a Features page.

## Decisions (maintainer, 2026-10-20)

- Basic, reliable functionality in the UI we have; no new paradigms.
- **Transparent fill for every feature** (membranes too), one colour per feature.
- **Gallery first**: the first visible milestone is membranes over the gallery tiles.
- **Template-matching search region is in v1.**
- **Tools:** easymode + Ais models as the job; MemBrain etc. only via ingest.
- **Containers:** Claude writes the `.def` files and the build commands; the maintainer builds and pastes the output.
- Each tool gets a tutorial for using it outside crboost (§T).
- Z scrubbing comes after v1.

## 1. Data model

### 1.1 Feature (new, beside Species)
`FeatureClass` on `ProjectState.feature_registry` (next to `species_registry`, `services/project_state.py:733`):
`id` (slug; the `<feature>` suffix of the files), `name`, `color`, `ontology_id: str | None` (GO term, optional,
never guessed), `origin` (`easymode` / `ais` / `import`), `created_at`, `notes`. Created implicitly the first time a
Segment job or an import produces that feature (for the job: by the server when it lands it, §6.2); colour from the
species palette function
(`services/models_base.py` `species_palette_color`, same stability rule). Colour edits from the gallery layer menu.
Display threshold is not a field in v1: overlays use p ≥ 0.5 (the easymode/Pom/MemBrain convention), stated in
the layer tooltip.

### 1.2 Segmentation volume (registry)
New tomogram-scoped output in `services/tilt_series/models.py` (union at `:208`, attached by
`registry.attach_tomogram_output`, `services/tilt_series/registry.py:217`):

```
SegmentationTomogramOutput(output_type="segmentation", job_instance_id, job_dir,
    source_mrc: Path,            # the density segmented (recon or a denoised variant)
    recon_instance: str | None,  # the reconstruct instance it traces to = the frame
    shape_zyx: tuple[int, int, int], voxel_angstrom: float,
    volumes: dict[feature_id, SegVolume])
SegVolume(path, full_scale: float,  # 127 int8 / 255 uint8-as-mode-6 or uint16 / 1.0 float32
          engine: "easymode-native" | "ais" | None, model: ModelRef | None,  # None: an import that did not say
          fraction_pct: float | None, overlay_xy: Path | None, overlay_xz: Path | None)
ModelRef(tool, tool_version, feature, variant, timestamp, weights_sha256)
```

As built (§6.2): `fraction_pct` and `overlay_xz` are in already, and `engine`/`model` may be None, so S4 and S5 need
no second schema change.

`extra="forbid"`: older code reading a registry that holds this output fails (see the cross-branch registry trap) —
the union member must land on main before any branch writes it.

### 1.3 Manifest (job and import alike)
`<dir>/segmentations.json`: one entry per (tomogram, feature) with path, shape, voxel size, encoding/full scale,
engine, model ref, parameters (tta, tile, format, apix override), source mrc, recon instance. The job writes it
in `aggregate`; an import writes it at ingest. The registry entries and the resolver both read from it.

### 1.4 Wiring
`JobFileType.SEGMENTATIONS` (`services/io_slots.py:15`) = the manifest. Producers: the Segment job, and imports as a
synthetic producer `importedSegmentations__<batch>` (same pattern as `IMPORTED_TOMOGRAMS`,
`services/path_resolution_service.py:1142`).

## 2. Stages

### S0 — Containers and facts (maintainer runs, Claude reads the output)
1. Build `easymode_1.2.5.sif` (§T.3). One image for easymode 1.2.5 + Ais 1.2.38 + Pom 1.2.1: easymode 1.2.5
   requires `ais-cryoet>=1.2.36` and `pom-cryoet>=1.2.0`, so the three are one coupled envelope (Py 3.10 /
   TF 2.11 / CUDA 11.2).
2. Fill and freeze the weights; paste the cache tree and `models.sha256`. This fixes where a feature's weight files
   live (needed for `ModelRef.weights_sha256`).
3. Segment one real tomogram by hand (membrane + void + cytoplasm) with the exact command the driver will use;
   paste `ls -la`, and the header (`header` from the IMOD container, or `python -c` with mrcfile) of input and
   outputs. Facts to record here: output dtype per engine, shape = input shape, voxel size copied, whether Warp's
   float16 (mode 12) input is accepted as is or needs a float32 copy, wall time and GPU memory.
4. Run the same command with `HF_HUB_OFFLINE=1` on a node without network and confirm no download attempt.
5. `apptainer exec … ais` over `srun --x11` once: does the GUI open (§T.2)?

**S0 record.**
- 2026-10-20 — S0.1 built: `sifs/easymode_1.2.5.sif`; TF 2.11.0 with CUDA, easymode 1.2.5, ais-cryoet 1.2.38,
  pom-cryoet 1.2.1, numpy 1.26.4, protobuf 3.19.6; both cache patches and all three CLIs verified at build.
- 2026-10-20 — S0.2 weights frozen: `cache_and_artifacts/easymode_1.2.5/models/` (4.0 GB, 127 files, read-only,
  `models.sha256` beside it). Layout: `models/registry.json`, root models (`n2n_direct`, `tilt`, …) as
  `models/<name>.{h5,json}`, features as `models/models/<feature>_<tag>.{h5|scnm,json}`. `easymode list` works with
  `HF_HUB_OFFLINE=1` (the image default). Defaults: `sv3-3d` (`.h5`, native engine) for microtubule, ribosome, tric;
  `sv2-2.5d` (`.scnm`) for cytoplasm, lipid_droplet, mitochondrion, npc, nuclear_envelope, nucleus, void; `sv2-3d`
  (`.scnm`) for the rest, membrane included — so most features run through the Ais engine (p × 255 output).
  `ModelRef.weights_sha256` = the line of `models.sha256` for `models/models/<feature>_<tag>.<ext>`.
- 2026-10-20 — S0.3 one tomogram (`FilamentTracing1_Position_29_5_12.00Apx.mrc`, WarpTools, MRC mode 12 float16,
  264×530×530 at 12 Å) on one Quadro RTX 6000 (23 GB), `easymode segment membrane void cytoplasm ribosome --tta 2`
  (test script `/groups/klumpe/software/containers/tests/easymode_1.2.5_s0.sh`):
  - float16 input is read as is — **no float32 copy** in `stage`.
  - Every output has the input's shape and the input's voxel size (12 Å) in the header.
  - Ais-engine features (membrane, void, cytoplasm): MRC mode 6, uint16, max 254–255 → full scale 255; 148 MB each.
    Native-engine feature (ribosome, `sv3-3d`): mode 0, int8, max 126 → full scale 127; 74 MB.
  - Time: 7 m 46 s for the four (membrane ≈ 3 min incl. start-up, void and cytoplasm ≈ 1 min each, ribosome 4 m 07 s).
    Peak GPU memory 22.2 GB — the job's GPU default needs a card with ≥ 24 GB, or a smaller `--tile`.
  - Runs offline from the frozen cache (image default `HF_HUB_OFFLINE=1`); no download attempted.
- 2026-10-20 — Viewing outside crboost: the cluster's IMOD module is 4.11.5, which cannot open MRC mode 12
  (WarpTools' float16). `/groups/klumpe/software/containers/tests/view_seg_3dmod.sh` (native `3dmod`, X forwarding,
  no GPU) converts any mode-12 input once to a float32 copy under `$WORK/f32/` and opens it with the segmentations.
  crboost itself is unaffected (mrcfile reads float16).
- 2026-10-20 — Visual check (maintainer, 3dmod): the output is weak on this tomogram, partly following filament
  (actin) density. The tomogram is from a months-old crboost run with known reconstruction artefacts, so this is no
  verdict on easymode; judge quality on a fresh, current project (runtime checklist §5).
- Open: S0.5 (Ais GUI over X11) — not blocking S1–S3.
- 2026-10-20 — Found while building S2 (easymode 1.2.5 source + the cache's mtimes): **HF_HUB_OFFLINE=1 does not
  make easymode offline.** `distribution.is_online()` fetches `https://huggingface.co` itself; online,
  `get_registry()` overwrites `models/registry.json` with the remote copy and `get_model()` downloads any weights
  whose remote timestamp is newer (under HF_HUB_OFFLINE that download fails, so the run dies instead). The S0.3
  run did go online: `models/registry.json` was rewritten at 14:55:31, byte-identical to the frozen copy (its
  sha256 still matches `models.sha256`). Offline for real = a proxy that refuses every connection
  (`https_proxy=HTTPS_PROXY=http://127.0.0.1:9`): the check fails at once, nothing is fetched or written. The
  Segment job sets it; manual runs need it too (§T.1).
- 2026-10-20 — `chmod -R a-w models` did not hold: every file under `cache_and_artifacts/` reads `rwxrwxrwx` and
  is writable (the /groups mount ignores mode bits). Nothing protects the weights but the runs staying offline.
- 2026-10-20 — easymode rewrites `settings.txt` in `$EASYMODE_CACHE_DIR` at every import, and on a read error
  copies its defaults over it and leaves `settings = None` (a crash later); Ais does the same in
  `$AIS_CONFIG_DIR`. Concurrent tasks sharing one directory race on these files, so each task gets its own
  (§6.2).

### S1 — Model: Feature registry, registry output, manifest, file type
`FeatureClass` + `feature_registry` + add/get helpers (`ProjectState`), `SegmentationTomogramOutput` and friends,
`JobFileType.SEGMENTATIONS`, manifest read/write helpers in a new `services/segmentation/` package (pure, no UI
imports; the driver imports it on compute nodes). `config/conf.template.yaml:224`: `easymode` tool key
(container). No behaviour change.

### S2 — Segment job (the first produce path)
- `JobType.SEGMENT = "segment"`, `SegmentParams` (`services/jobs/segment.py`), `JobSpec` row after denoise
  predict in `services/jobs/spec.py` (phase preprocessing; items_from `input_tomograms`; plugins `segment`,
  `array_tasks`). `SegmentMethod` enum like `DenoiseMethod`: `EASYMODE`, `AIS_MODEL`.
- Params: `features: list[str]` (easymode: chosen from the 24 names read from the frozen cache's registry, never
  a hard-coded list), `ais_model_path` (AIS_MODEL), `tta` (easymode default 2), `output_format` (`int8`),
  `array_throttle`, GPU resources.
- Input slot `input_tomograms` accepts `[TOMOGRAMS_STAR, DENOISED_TOMOGRAMS_STAR]`, preferred `tsReconstruct`:
  easymode was trained on raw / even-odd / N2N Warp reconstructions, not missing-wedge-corrected ones. Choosing
  an IsoNet input shows a warning marker (not a block).
- Driver `drivers/segment.py` on `ArrayDriver` (`drivers/array_job_base.py:714`):
  - `stage`: symlink the tomogram as `<tomo>.mrc` into a per-task input dir (Warp's float16 is read as is, S0.3).
  - Resources: one GPU per task; the default must ask for a ≥ 24 GB card (peak 22.2 GB at 530×530×264, S0.3).
  - `build_command`: `easymode segment <features…> --data <in> --output <job>/segmentations --format int8 --tta N
    --gpu 0` or `ais segment -m <scnm> -d <in> -ou <job>/segmentations -gpu 0`; ~~env `HF_HUB_OFFLINE=1`; binds the
    frozen cache read-only and a per-job Ais config dir~~ — as built: the offline proxy and per-task settings
    directories (§6.2).
  - `verify_outputs`: each expected `<tomo>__<feature>.mrc` exists, has the input's shape and a known dtype, and is
    not a claim placeholder (a 10³ volume). Failure names the file.
  - `collect` (volume already mapped): per feature, the overlay PNG (§3.1) and the fraction (mean p ×100 over the
    volume) into a small sidecar `<job>/analytics/segment/<tomo>.json`.
  - `aggregate`: manifest; registry attach (`SegmentationTomogramOutput`); ~~features registered~~ — as built, the
    server registers the features when it lands the job (§6.2).
- Job tab: method, feature picker, input, resources; per-TS task rows come with `array_tasks`.

### S3 — Gallery overlay (first visible milestone)
- `ui/tomo_gallery.py`: a **features** layer menu beside the picks menu (`_pick_layers`, `:158`), one entry per
  feature with an eye and a colour swatch. Layers follow the source exactly like pick layers: a feature shows over
  the version whose reconstruction it traces to (`recon_of`, now `services/job_lineage.py`; denoised
  variants of that reconstruction included); over another reconstruction it is hidden with a note.
- Each overlay is one element per (tile, feature): a `div` filling the frame with `background-color: <feature
  colour>; opacity: .4; mask-image: url(<overlay png>)` (+ `-webkit-mask-image`), inline. Colour and opacity change
  without re-rendering; one PNG per (tomogram, feature) serves every tile size.
- Tile caption: the fraction of each visible feature (from the sidecar), as small coloured badges.
- Zoom view: same layers.

### S4 — Ingest segmentations made elsewhere
- Dialog (gallery toolbar "Import segmentations…"): a directory or glob of `<stem>__<feature>.mrc`.
- Matching: `<stem>` against each tomogram's reconstructed and denoised MRC stems (`_sources_for`,
  `ui/tomo_gallery.py:299`, knows them); unmatched files and grid mismatches (shape ≠ that volume's shape) are
  listed and skipped — never resampled, never guessed.
- Encoding from dtype + observed maximum (MemBrain binary int8 → full scale 1; easymode int8 → 127; Ais mode 6 →
  255); ambiguous cases are asked in the dialog.
- Files are referenced, not copied; manifest under `<project>/Segmentations/imports/<batch>/`; overlays and
  fractions rendered in a worker (`run_in_worker(WorkKind.RENDER, …)`) by the same function the driver uses.

### S5 — Journey and pick viewer overlays
The same feature layers over the X/Y and X/Z slabs of the pick viewer (`ui/particles/pick_viewer.py`, beside
`_render_pick_layer` `:1294`) and the Journey's reconstruct card. X/Z overlay PNG is written by the driver too.

### S6 — Template matching restricted to a feature region
- `TemplateMatchPytomParams` (`services/jobs/template_match.py`): optional input slot `input_segmentations`
  (accepts `SEGMENTATIONS`, `required=False`) and `search_region`: include features and exclude features
  (e.g. include `cytoplasm`, exclude `void`, `ice_particle`), threshold p ≥ 0.5, an optional dilation in Å.
- Driver (`drivers/template_match_pytom.py`, `stage` `:385`, `build_command` `:419`): per tomogram, build the mask
  from the manifest volumes (union of includes minus union of excludes), write
  `tmResults/<tomo>_search_mask.mrc` (int8 0/1) and pass `--tomogram-mask`. pytom requires exactly the tomogram's
  shape; a different shape or a segmentation from another reconstruction fails the task with that reason.
  pytom skips sub-volumes that are entirely masked while matching and zeroes scores outside the mask at
  extraction (the extraction driver reads the mask from `job.json`, `drivers/extract_candidates_pytom.py:99`).
- Submit-time check: every tomogram the job will match has the named features; missing ones are listed before
  submission.
- The TM analytics sidecar records the searched fraction so σ statistics are read against the right volume.

### S7 — ChimeraX
`session_chimerax_commands` (`services/visualization/artiax_bridge.py:426`) gains the visible features of the
tomogram: `open <volume>`, `volume #N level <0.5 × full scale> color <hex> transparency 0.6`. 3dmod stays the plain
volume hand-off.

## 3. Rendering

### 3.1 Overlay PNG
For a feature volume of shape (Z, Y, X): the same Z range the tomogram slab uses, computed from the **tomogram's**
shape and itemsize (factor the range computation out of `render_xy_slab_preview`,
`services/visualization/preview_render.py:205`, into a pure helper both call — the byte budget depends on dtype,
so the int8 volume would otherwise get a different slab). Within the range, max over Z (thin membranes survive);
alpha = 255 where p ≥ 0.5 else 0; `flipud` (same Y-up convention); same down-scaling as the slab (`max_dim`).
Grey-alpha PNG; colour comes from CSS. X/Z overlay the same way over the central Y range.

### 3.2 Which picture it sits on
The overlay matches our X/Y slab exactly. Over the WarpTools PNG (the gallery's first choice when present) the
central plane is the same but the slice thickness is Warp's; runtime check S3.

## 4. Disk
int8 on the reconstruction grid ≈ 74 MB per feature per tomogram at 530×530×264 (Ais-engine features are
written as mode 6 uint16, ≈ 148 MB). The Segment job's directory follows the trash/clear path of roadmap 36 like
any job.

## §T. Tutorials: the tools outside crboost

### T.1 easymode (headless; what the Segment job runs)
Paths: image `/groups/klumpe/software/containers/sifs/easymode_1.2.5.sif`; weights
`/groups/klumpe/software/cache_and_artifacts/easymode_1.2.5/` (read-only `models/`); Ais state
`/groups/klumpe/software/cache_and_artifacts/ais_1.2/`.

```bash
SIF=/groups/klumpe/software/containers/sifs/easymode_1.2.5.sif
EM=/groups/klumpe/software/cache_and_artifacts/easymode_1.2.5
AIS=/groups/klumpe/software/cache_and_artifacts/ais_1.2
OFF="--env https_proxy=http://127.0.0.1:9 --env HTTPS_PROXY=http://127.0.0.1:9"
srun --pty --partition=g --constraint="g3|g4" --gres=gpu:1 --cpus-per-task=8 --mem=32G --time=2:00:00 bash
cd /scratch-cbe/users/$USER/<workdir>
apptainer exec --nv --cleanenv $OFF -B $EM:/opt/easymode_cache -B $AIS:/opt/ais_cache -B $PWD $SIF easymode list
apptainer exec --nv --cleanenv $OFF -B $EM:/opt/easymode_cache -B $AIS:/opt/ais_cache -B $PWD -B /groups/klumpe $SIF easymode segment membrane mitochondrion void --data /path/to/tomos --output segmented --tta 2
apptainer exec --nv --cleanenv $OFF -B $EM:/opt/easymode_cache -B $AIS:/opt/ais_cache -B $PWD -B /groups/klumpe $SIF easymode pick ribosome --data segmented --output coords/ribosome --spacing 150 --no_tomostar
```

- `$OFF` keeps the run offline (S0 record): without it easymode checks huggingface.co, rewrites the shared
  cache's `registry.json` from the remote copy and tries to fetch newer weights. Only `easymode download` goes
  without it.
- One run at a time per `$EM` / `$AIS`: both tools rewrite their settings files at start. Parallel runs need
  directories of their own, as the Segment job gives each task
  (`/groups/klumpe/software/containers/tests/easymode_1.2.5_task_layout.sh` shows that layout).

- Input: one MRC per tomogram, any voxel size (rescaled to the model's 10 Å internally); output
  `segmented/<tomo>__<feature>.mrc` on the input grid. Default encoding int8 = p × 127; `.scnm` (Ais-engine)
  features come out p × 255 as uint16 — `easymode list` shows which engine a feature uses.
- A killed run leaves 10³ placeholder files that block reruns: delete them (or `--overwrite`).
- Picks are corner-origin voxel indices of the segmentation grid, no pixel size in the STAR. `--no_tomostar` unless
  the tomograms follow Warp's `_10.00Apx` naming.
- Look at results: `apptainer exec <imod.sif> 3dmod tomo.mrc segmented/tomo__membrane.mrc`, or ChimeraX
  (`volume #2 level 63 color cornflowerblue transparency 0.6` for int8).

### T.2 Ais (GUI: annotate and train a feature easymode lacks)
```bash
srun --pty --x11 --partition=g --gres=gpu:1 --cpus-per-task=8 --mem=32G --time=4:00:00 bash
cd /scratch-cbe/users/$USER/<workdir>
apptainer exec --nv --cleanenv --env DISPLAY=$DISPLAY --env XAUTHORITY=$HOME/.Xauthority -B $HOME/.Xauthority -B $AIS:/opt/ais_cache -B $PWD -B /groups/klumpe $SIF ais
```
- Workflow: open a tomogram (10–20 Å/px works best), make a feature, brush a handful of slices (positives and a
  few negatives), train (box/epochs in the Models panel), preview on the whole volume, save the model (`.scnm`).
- Headless application of the saved model:
  `apptainer exec --nv --cleanenv -B … $SIF ais segment -m mymodel.scnm -d tomos/ -ou segmented/ -gpu 0`
  → `segmented/<tomo>__<model title>.mrc` (p × 255). The model title becomes the feature id in crboost.
- In crboost: Segment job, method *Ais model*, point it at the `.scnm`.
- If X11 forwarding is too slow (software GL), the fallback is the ChimeraX VNC route (GPU node + VirtualGL);
  recorded as an S0.5 outcome.

### T.3 Building the image
The definition is `/groups/klumpe/software/containers/defs/easymode_1.2.5.def` (one environment, Ais and Pom
config paths patched to the bound cache directories, weights outside the image, `HF_HUB_OFFLINE=1` by default).
Build on a node with fakeroot and enough temporary space:

```bash
cd /groups/klumpe/software/containers
export APPTAINER_TMPDIR=/scratch-cbe/users/$USER/apptainer_tmp
export APPTAINER_CACHEDIR=/scratch-cbe/users/$USER/apptainer_cache
mkdir -p $APPTAINER_TMPDIR $APPTAINER_CACHEDIR
apptainer build --fakeroot sifs/easymode_1.2.5.sif defs/easymode_1.2.5.def 2>&1 | tee /scratch-cbe/users/$USER/easymode_1.2.5_build.log
mkdir -p /groups/klumpe/software/cache_and_artifacts/easymode_1.2.5/models /groups/klumpe/software/cache_and_artifacts/ais_1.2
apptainer exec --cleanenv --env HF_HUB_OFFLINE=0 -B /groups/klumpe/software/cache_and_artifacts/easymode_1.2.5:/opt/easymode_cache -B /groups/klumpe/software/cache_and_artifacts/ais_1.2:/opt/ais_cache sifs/easymode_1.2.5.sif easymode download --all
cd /groups/klumpe/software/cache_and_artifacts/easymode_1.2.5
find . -maxdepth 3 | sort
find models -type f -exec sha256sum {} + | sort -k2 > models.sha256
chmod -R a-w models
apptainer exec --cleanenv -B /groups/klumpe/software/cache_and_artifacts/easymode_1.2.5:/opt/easymode_cache -B /groups/klumpe/software/cache_and_artifacts/ais_1.2:/opt/ais_cache /groups/klumpe/software/containers/sifs/easymode_1.2.5.sif easymode list
```

The older `easymode.sif`, `ais.sif` and `pom_cryoet.sif` (Ais `3a1d006`, Pom 1.1.1) and their runscripts stay
untouched; switching the lab runscripts to the new image is a separate decision once S0 passes.

## 5. Runtime checklist (once, at the end)
1. Project with a reconstruct job: add Segment (easymode: membrane, void, cytoplasm), run; task rows go green;
   `segmentations.json` lists every (tomogram, feature).
2. Gallery: features menu shows the three; membranes appear as a transparent fill on the tiles and line up with
   the membranes in the slab (also on the WarpTools picture); badges show fractions; colour change is instant.
3. A second reconstruct job (another alignment): its tiles hide the features with the note.
4. Import a folder of MemBrain outputs: matched files register; a wrong-grid file is listed and skipped.
5. Journey and pick viewer show the same layers on X/Y and X/Z.
6. TM with search region cytoplasm − void: `tmResults/<tomo>_search_mask.mrc` exists, the pytom log shows skipped
   sub-volumes, candidates lie inside the region; a tomogram without the features is listed before submit.
7. "Open in ChimeraX" shows the features as transparent surfaces with the tomogram and picks.

## 6. Build record: S1–S3 (2026-10-20, ruff-clean, not run)

### 6.1 What was built
- **S1.** `FeatureClass`, `ProjectState.feature_registry`, `get_feature` / `add_feature` (project schema 3.10,
  restored explicitly in `load()`); `ModelRef`, `SegVolume`, `SegmentationTomogramOutput` in the registry's tomogram
  union (registry schema 1.6); `JobFileType.SEGMENTATIONS`; `services/segmentation/manifest.py` (entries, manifest,
  atomic write, `tomogram_outputs` = the registry entries built from a manifest, `feature_slug`);
  `ToolConfig.cache_dir` and the `tools.easymode` template entry.
- **S2.** `JobType.SEGMENT`, `SegmentMethod`, `SegmentFormat`, `SegmentParams` (`array_throttle` 8), the JobSpec row
  after Denoise Predict (depends on Reconstruct). `drivers/segment.py`; `services/segmentation/easymode.py` (cache
  from config, catalog from the frozen `registry.json` + `models.sha256`, Ais model title, sha256) and
  `volumes.py` (encoding from dtype, finished-volume check, overlay masks + fraction). `central_slab` factored out
  of the slab previews (`preview_render.py`), used by both previews and the overlays. The server registers the
  features when it lands the job (`PipelineRunnerService._land_segment_job`). Job tab (`ui/job_plugins/segment.py`,
  `multi_choice_row` in `_field_styles.py`) and the Tasks tab. `segment` resource profile in the template.
  `job_dirs` / `recon_of` moved to `services/job_lineage.py`: the driver needs them and services may not import ui.
- **S3.** Gallery (`ui/tomo_gallery.py`): a features menu in the toolbar and the zoom (eye, colour swatch saved to
  the feature registry, tomograms covered or the fraction, an eye-off note over another reconstruction, opacity
  25/40/60 %); one masked fill per (tile, layer) under the dots; fractions as caption badges. Colours and the
  opacity are CSS variables on the page and the zoom, so changing them repaints no tile.

### 6.2 Decisions taken in the build
- **Features are registered by the server**, not the driver: the server owns `project_params.json`, and a driver
  write would be lost under its in-memory state. A feature that landed while no server watched draws in its
  palette colour and is registered at its first colour edit.
- **Run layout.** Each task has its own `EASYMODE_CACHE_DIR` (`settings.txt` + a `models/` link to the frozen
  cache's) and `AIS_CONFIG_DIR`; `https_proxy`/`HTTPS_PROXY` point at a closed port so easymode's online check
  fails (S0 record). The cache is bound at its own path by `container_binds`, not at `/opt/easymode_cache`, and not
  read-only: crboost binds are same-path, and nothing writes under the proxy.
- **Volume names.** Ais-engine volumes are named after the `.scnm` title; the supervisor reads each title instead of
  assuming title = feature.
- **Registry schema covers S4 and S5**: `fraction_pct` and `overlay_xz` exist (the driver writes the X/Z mask
  now), `engine`/`model` may be None. The per-tomogram sidecar is the task → supervisor channel; the gallery reads
  the registry only.
- **Lineage.** The driver records `recon_instance`, which the gallery uses; the job's current inputs (`recon_of`)
  only when nothing was recorded, since they may have been re-pointed after the volumes were made. Layers are per
  (producer, feature): two jobs' membranes are two rows in one colour.
- **Provenance.** Tool versions are read from the image (`importlib.metadata`) in one container start at supervisor
  time, which also fails the job before any GPU is asked for when the image does not start.
- **Re-runs** (a failed job runs again in its directory, and its parameters can change in between). Complete
  volumes are kept; placeholders and truncated files go before the tool runs. The supervisor compares each
  feature's identity (volume name, engine, variant, weights checksum, TTA, format) with the previous run's: a
  changed feature's volumes and overlays are removed, and on any change or added feature no tomogram counts as
  done. The registry is made to match `segmentations.json`: a tomogram this run did not segment loses the job's
  earlier entry (`TiltSeriesRegistry.detach_tomogram_output`).
- **Self-test.** `tests/test_cluster.py` has an `easymode` probe (the image starts, `ais` is there, TensorFlow sees
  the GPU); without one, `test_every_tool_has_a_probe` fails once `tools.easymode` is configured.
- **Not handled in v1:** a header voxel size ≤ 1 Å fails the task with that reason (easymode would use 10 Å); no
  apix override. Imported tomograms are not segmented by the job (no registry tilt series; S4 ingests volumes made
  elsewhere). `output_format` affects only easymode's own 3D networks.

### 6.3 Cross-branch hazard
Registry 1.6: a server on older code (the lab install on main, another branch) that reads a project after a Segment
job attached its outputs fails to validate those sidecars and skips the tilt series (the project looks empty there;
a save with a dirty index drops them from `index.json`). Merge to main before running Segment on projects other
servers open. Project schema 3.10: older code ignores `feature_registry` and drops it on save (colours fall back to
the palette).

### 6.4 Runtime check (the maintainer's, S1–S3)
1. Dev-tree `config/conf.yaml` has `tools.easymode` (image + `cache_dir`) and `job_resource_profiles.segment`
   (`constraint: "g3|g4"`).
2. Fresh project with a Reconstruct job: add Segment (easymode: membrane, void, cytoplasm), Run. Task rows go green;
   `segmentations.json` lists every (tomogram, feature); `run.out` has the versions line; the cache's
   `models/registry.json` keeps its 14:55 mtime.
3. Tomograms view → Refresh: the features menu lists the three; membranes fill the tiles and sit on the membranes
   (WarpTools picture and X/Y slab source); badges show fractions; a colour change is instant and survives a
   restart; the opacity switch works.
4. With a second Reconstruct job: its tiles show no fill and the menu row shows the eye-off note.
5. A task that fails inside the tool: `/groups/klumpe/software/containers/tests/easymode_1.2.5_task_layout.sh`
   runs the same call outside crboost.

Per-stage file snapshots: `notes/37-stage-snap/s1`, `s2`, `s3` (git-ignored).

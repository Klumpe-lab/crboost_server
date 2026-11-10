# Roadmap 48 — K3 / SerialEM / PACEtomo deliveries, from either layer

Status: scoped 2026-11-10; S1–S5 built the same day, NOT RUN (§9). Builds on [roadmap 18](completed/roadmap_18-k3-stack-ingest.md) (stack split at
Create, per-mdoc layer inference, dose estimate). Anchors at `a22decd`.

Goal: point the landing page at an outside lab's SerialEM/PACEtomo folder as it was shared (maps, previews, IMOD
leftovers and all) and get **one** project of its tilt series. The user chooses whether they are processed from the
**frames** (TIFF + gain reference) or from SerialEM's **stacks**, with the right tilt axis, hand, gain and dose for
that choice and every guess shown as a guess.

## 1. The two deliveries

Both from the same collaborating lab: Krios D3781, K3 behind an energy filter (slit 10 eV), SerialEM 4.1.10.

| | first share (Sept) | second share (Nov) |
|---|---|---|
| where | `/groups/klumpe/crboost_data/olena_k3` | `/groups/klumpe/001_Data/OLENA_pooled_tomos` |
| what | 4 tilt series | `experiment2_20260615`: 23 series (`tomo/` 16, `tomo2/` 7) + `maps/`, `maps2/`; `experiment1_20251126`: 3 series + `frames/` + `maps/`; `settingup/` |
| driver | PACEtomo 1.8.1 | experiment2: PACEtomo 1.8.1; experiment1: SerialEM's own dose-symmetric series (`dosym = 12.0` header) |
| per series | stack `.mrc` + stack mdoc + 39 TIFFs with `.tif.mdoc` | same, but IMOD has touched many folders (below) |

**Continuity.** The first share is `experiment2_20260615/tomo/area_5_a_ts_00{2,3,4,7}` of the second. Their mdocs,
TIFFs and PACEtomo files are byte-identical. The stacks are not where they were. In the meantime the series were
processed in IMOD's etomo (`*_project.log`: an etomo session on a laptop, July 2026), and etomo's "Use fixed stack"
renames the raw stack `<ts>.mrc` to `<ts>_orig.mrc` and writes the X-ray-erased stack under the old name:

- `_orig.mrc` only, no `<ts>.mrc`: `area_4-A_1_ts_002/003`, `area_4-A_2_ts_001`, `area_5-A_ts_002/004`.
- both: `area_5-A_ts_005`, `area_5-A_ts_007`. `<ts>.mrc` is the erased one: checked on ts_007, where the new
  `_orig.mrc` matches the first share's `<ts>.mrc` and the new `<ts>.mrc` does not (md5 of the first 200 MB).
- experiment1: all three stacks are `ts_0N_orig.mrc`, plus `ts_02.mrc` and a Finder duplicate `ts_02 2.mrc`.
- IMOD leftovers beside the data: `*.com`, `.edf`, `dfltcoms/`, `origcoms/`, `_preali.mrc`, `_full_rec.mrc`,
  `top/mid/bot_rec.mrc`, `.xf`, `.tlt`, `.seed`, `*~` backups.

So nothing in a share can be assumed to be what SerialEM wrote, except the mdocs and the frames.

One folder, `tomo/area_6_a_shift_ts_005`, is mode `drwx------`: readable by the server's user only.

## 2. What wrote which file

| file | written by | what it is |
|---|---|---|
| `<area>_ts_NNN.mrc` + `.mrc.mdoc` | SerialEM, driven by PACEtomo | the tilt series as SerialEM shows it: frame-aligned sums (the PACEtomo log reports "Frame alignment results" per tilt), gain-normalised, rotated/flipped by the camera's `RotationAndFlip`, int16 ×16 counts per electron (`CountsPerElectron = 16`, `DividedBy2 = 1`) |
| `<area>_ts_NNN_<area>_ts_NNN_001_000_<angle>.tif` + `.tif.mdoc` | SerialEM's K3 plugin | the 8 raw frames of one tilt: LZW, uint8, **gain-unnormalised, camera-native orientation** (§3); one `[FrameSet = 0]` section, no `[ZValue]` |
| `CountRef_<first frame file>.mrc` | SerialEM | the counting-mode gain reference (float32, mean 1.0, multiply), copied once per folder; named in every `.tif.mdoc`'s `GainReference =` |
| `SuperRef_<…>.dm4` | SerialEM / DigitalMicrograph | the super-resolution gain reference (experiment1) |
| `defects_<…>.txt` | SerialEM | K3 defect list: here only `BadRows 0–5, 8176–8183` (super-res rows), i.e. the 3 edge rows top and bottom |
| `<area>_tgts.txt`, `_tgts_run01.txt`, `_tgts_settings.txt`, `<area>.log` | PACEtomo | targets per area (`_tgt = NNN`, `tsfile`, stage X/Y, specimen shift SSX/SSY), run state, settings (start −10°, −64…+50°, 3° steps, groups of 2), the acquisition log |
| `<area>_tgt_NNN.mrc`, `_tgt_NNN_view.mrc` (+ mdocs) | PACEtomo | one-image previews of each target at Record (1.382 Å) and View (13.76 Å) magnification |
| `SA_map_NN.mrc`, `gridmap.mrc`, `gm1.mrc`, `nav*.nav` | SerialEM Navigator | montage maps (13.76 / 660.8 Å/px) and navigator files |
| `*.com`, `.edf`, `dfltcoms/`, `origcoms/`, `_orig.mrc` | IMOD etomo / batchruntomo | the acquirer's own processing |

Naming: the PACEtomo **area** (`area_5-A`, a navigator point = one stage position) and the **target** number
(`_ts_NNN`, one beam-shift position around it) are the stage/beam grammar of this data. Tomo5's `Position_N[_M]` does
not occur, which is why the overview's stage/beam columns are empty (`services/tilt_series/build.py:286`).

Documentation to cite (not re-read in this session; the background research did not finish): SerialEM help, file
formats page `https://bio3d.colorado.edu/SerialEM/hlp/html/about_formats.htm` (mdoc keys) and the K2/K3 frame-saving
pages; PACEtomo `https://github.com/eisfabian/PACEtomo`; IMOD etomo tutorial
`https://bio3d.colorado.edu/imod/doc/tomoguide.html`; Warp `https://warpem.github.io/warp/`.

## 3. Measured facts (experiment2)

- **Frames vs stack orientation.** The sum of a tilt's 8 frames correlates with the matching stack section only
  after SerialEM operation 6 (flip about Y, then rotate 180° = a vertical mirror): NCC 0.64 at bin 8, against
  0.03–0.04 for the other three operations (`area_5-A_ts_002`, z 0). The TIFF's own `ImageDescription` says
  `SerialEMCCD: Frames …  r/f 6`. The frames are **native**, and the stack is the mirrored view.
- **Gain vs frames orientation.** Over all 312 frames of `area_5-A_ts_002`, the high-passed frame sum correlates with
  the high-passed `1/CountRef` as stored (+0.117) and not with any flipped version (+0.009). The gain is native like
  the frames, is used **as-is** (no flip, no transpose), and is **multiplied** (it lowers the residual pattern; mean
  1.0005, range 0.54–1.56).
- **Counts.** Raw frames 3.48 counts/px per tilt at −10°, stack 55.73 = 3.48 × 16. SerialEM's `DoseRate × ExposureTime`
  for the same tilt is 4.36 e/px, 25 % more than counted. A coincidence-loss correction would explain it; not checked
  against the documentation.
- **Tilt axis.** The mdoc's `Tilt axis angle = 85.0` belongs to the stack's (mirrored) orientation. A vertical
  mirror maps an axis at α to −α, so for the native frames the axis is **−85.0°**. The mirror also reverses the
  tilt-angle hand: a project built from the frames needs the opposite hand to one built from the stacks.
- **Pixel size.** 1.382 Å for every series, also in the TIFF resolution tag (72358896 px/cm).
- **Frames complete.** All 23 stack mdocs name existing TIFFs for every section. Repeated tilts: `area_4-A_1_ts_003`
  (5°) and `area_6-A_shift_ts_001` (−52°, −55°).
- **Dose.** `ExposureDose = 0` everywhere (dose never calibrated in SerialEM). Per series, the zero-thickness fit of
  roadmap 18 (`services/configs/mdoc_service.py:241`) gives an **incident 4.2–4.6 e/Å² per tilt in all 17 series where
  it fits**: 14 of 23 (t/λ from 0.30 to 1.60, pretilt −4.5° to −9.5°), although the transmitted dose varies
  fivefold (0.9–3.4). The eight `area_6-A` series (both grids) and `area_4-A_1_ts_001` do not fit (rms 0.05–0.56)
  and only have transmitted values (1.0–3.3). A constant beam of ≈ 4.3 e/Å² per tilt, ≈ 165 e/Å² for 39 tilts,
  explains all fitted series.

experiment1: frames are 8-bit **super-resolution** MRCs (11520×8184, `Binning = 0.5`, 1.6855 Å), stack mdocs say
3.371 Å at 26 kx. The acquirer's mail says 0.261 nm, which matches neither. The gain reference these frames name
(`SuperRef_lam_ts_ts_01_45_000_12.0.dm4`) is **not in the share**; the one in `frames/` belongs to other
acquisitions in that folder.

## 4. Why the scan of `experiment2_20260615` gave four projects

Reproduced read-only through `DatasetParsingService.parse_dataset` with the glob the landing page derived,
`experiment2_20260615/*/*.mdoc` (`ui/data_import_panel.py:651-663`):

1. **The glob is one level too shallow.** It matches `maps/*.mdoc`, `maps2/*.mdoc` and the loose PACEtomo previews in
   `tomo/` and `tomo2/` (`*_tgt_NNN*.mrc.mdoc`), and none of the 23 tilt series, which sit one folder deeper.
2. **Anything with a `[ZValue]` section counts as a tilt series** (`services/configs/dataset_parsing_service.py:83-91`).
   Montage maps (36–42 sections at one tilt) and the one-image previews pass. Their sections give no `TiltInfo`
   (`_build_tilt_info` returns None without `SubFramePath`, `:257-259`), so they become series with **0 tilts**, angle
   range (0, 0) (`services/tilt_series/preimport.py:79-83`), and `kind=stack`, because `resolve_stack` finds an MRC
   whose nz equals the section count (`services/stack_import.py:39`).
3. **Four projects.** `maps/` and `maps2/` both hold `SA_map_01…04`, so `folder_name_collisions()`
   (`preimport.py:309`) asks the folder-split question, and per-folder mode makes one project per mdoc folder: maps,
   maps2, tomo, tomo2.
4. **13.76 and 660.8 Å** are the maps (6500× and 135×) and the View previews.
5. **Dose 0.49** is `DatasetOverview.dose_estimate()` (`preimport.py:241`), the median of 42 junk estimates: maps
   0.0–0.24, View previews ~0.15, Record previews 0.7–1.6. Per-folder overviews give 0.0, 0.14, 0.54 and 0.64, the
   0 / 0.5 / 0.6 in the project panels.
6. **Stage/beam "-"**: no `Position_N` (§2).

With the right depth (`*/*/*.mdoc`) the same parser finds the 23 series, no collisions, dose 4.22, and shows the next
problems:

7. **Mixed layers, silently.** 18 series come out `stack` and 5 `movies` (the five whose stack is `_orig.mrc` only),
   with one project-wide tilt axis of 85° (`stack_import.py:57-66`, `dataset_parsing_service.py:203-218`). The five
   would be processed from native frames, with no gain (the gain field is empty unless typed), under the stack's axis
   and hand: mirrored, the axis 10° off. Filed as **HOTFIXES H8**.
8. **`area_5-A_ts_005/007` take the IMOD-erased `<ts>.mrc`**, not the raw `_orig.mrc`: `resolve_stack` only knows
   `ImageFile`.
9. **The source line says "movies (.mrc …)"** for TIFF movies: `_frame_extension` returns `.mrc` whenever stacks are
   the majority (`dataset_parsing_service.py:217`).
10. **The dose median mixes incident and transmitted values** (`preimport.py:241-245`, `AcquisitionSummary`
    `dose_estimates`). It lands on 4.22 here only because 17 of 23 fit.

## 5. Both layers, as a choice

Nothing fundamental stands in the way of offering both for experiment2. Each needs:

| | stacks | frames (TIFF/MRC + gain) |
|---|---|---|
| files | stack mdoc + the **raw** stack: `<stem>_orig.mrc` if present, else `ImageFile`; nz == sections | stack mdoc + every `SubFramePath` basename found beside it + the gain reference named in the frame mdocs' `GainReference` |
| tilt axis | mdoc header (85.0) | mdoc header mirrored by the frames' `RotationAndFlip` (−85.0 for op 6) |
| tilt-angle hand | as roadmap 18 ran it (`--dont_invert`; Warp's defocus-hand check said "no flip") | the opposite of the stacks (needs H2: the hand is not settable today) |
| gain | none (already normalised) | the CountRef as-is, multiplied; `--gain_path`, no flips |
| defects | none | edge rows only here, so none (stated on screen); a real defect list needs conversion (open) |
| super-res | n/a | `Binning = 0.5`: angpix = frame pixel, Warp bins by 2 |
| motion | SerialEM's whole-frame alignment at acquisition; no per-frame dose weighting | Warp's motion and CTF on the 8 frames |
| dose | estimated (§3) | the same estimate |

What only the acquirer can settle: the **true dose**; experiment1's **pixel size** (0.261 nm vs 3.371 Å) and its
**missing gain reference**. The second blocks experiment1's frames path: the stacks path works without it. What the
data cannot settle on its own: the **absolute hand**. Warp's defocus-hand check and one chiral feature decide it, as
for every dataset; the rule above only keeps the frames and stacks consistent with each other.

## 6. Stages

**S1 — Find tilt series at any depth, and only tilt series.**
- An mdoc is a tilt series when it has ≥ 3 `[ZValue]` sections with `SubFramePath`, a tilt span ≥ 10° and no
  montage keys (`PieceCoordinates`, `[MontSection]`). Per-frame mdocs (`[FrameSet]`), one-image previews, montage
  maps and `*~` backups are skipped and counted on one line: "ignored: 10 montage maps, 18 preview images, 889 frame
  mdocs".
- The landing page searches below the chosen folder (depth ≤ 3) instead of the `*.mdoc` ⇄ `*/*.mdoc` flip.
- Anchors: `dataset_parsing_service.py:83-91`, `mdoc_service.py:292` (`first_mdoc_facts` has the same rule),
  `ui/data_import_panel.py:651-663`.
- Check: experiment2 scans to 23 series, one project, no split question; experiment1 to 3; the first share to 4.

**S2 — One layer per project, chosen by the user.**
- Per series, the scan records what each layer has: `stack_path` (raw: `<stem>_orig.mrc` beats `ImageFile`, and an
  `.edf`/`eraser.com` beside a stack without `_orig` is flagged as possibly modified) and the frames with their gain.
- The overview shows a segmented choice, Frames / Stacks, with how many series each layer can serve. The default is the
  layer every selected series has; when neither covers all, nothing is chosen and Create says why. Series without the
  chosen layer are listed, not silently switched.
- `choose_source_layer` (`stack_import.py:57`) becomes the availability rule; the choice lives in
  `DataImportFormState` and `import_source_kind`.
- Fixes H8, §4 items 7–9.

**S3 — The frames layer's facts.**
- Gain: `GainReference` of the frame mdocs, found beside the mdoc, then up to the scanned root; one gain per project
  (here two, `tomo/` and `tomo2/` each copied their own: same reference or not is checked by content, and two
  different ones are shown as an open item). Into `acquisition.gain_reference_path` with no gain operations.
- Orientation: `RotationAndFlip` of the frame mdocs (and the TIFF `r/f N`) gives the axis for the frames and flags the
  hand as opposite to the stacks; shown beside the axis field as derived, overridable.
- Super-resolution frames (`Binning = 0.5`): angpix and Warp's binning.
- `.dm4` gain: whether WarpTools reads it is unverified; if not, convert once at Create (IMOD `dm2mrc`).
- Depends on H2 (settable hand) and H5 (gain operations on the project).

**S4 — Dose per series.**
- The project's dose is the median of **incident fits only**; series without a fit are named, with their transmitted
  value, and assumed to share the beam. The estimated-dose chip says "estimated (14/23 fitted, 4.2–4.6)".
- Anchors: `preimport.py:241-252`, `AcquisitionSummary.from_tilt_series`.

**S5 — PACEtomo positions.**
- Stage group = PACEtomo area (`<area>_ts_NNN` → `<area>`), beam = target `NNN`; from `_tgts.txt` (`tsfile`) when
  present, else the name. `TiltSeriesInfo.stage_position` is an int today (`preimport.py:56`): it gets a label
  alongside, and `parse_position` (`build.py:286`) stays the Tomo5 grammar.

**S6 — Check on the real data.** experiment2 into two projects, one per layer, five series each (one per area);
motion/CTF → alignment → Warp's defocus-hand check → reconstruction; the two reconstructions of one series must match
after the mirror. Then all 23 in the chosen layer.

## 7. Decisions

- **D1 — layer for experiment2.** Open: the user picks per project (S2); which one to run first is the maintainer's
  call after S6.
- **D2 — dose.** Decided 2026-11-10: the estimated incident dose (≈ 4.3 e/Å² per tilt) until the acquirer answers;
  shown as estimated. Question drafted (§8).
- **D3 — experiment1.** Stacks only until the gain reference and pixel size are answered.

## 8. Question for the acquirer

Drafted for the maintainer to send; names left out here.

Thanks for the data! We've been through the files and have three questions:

1. **Dose.** The mdocs don't record a dose (`ExposureDose = 0`, so the dose wasn't calibrated in SerialEM), and we
   need it for dose weighting. What we can see is the dose rate SerialEM measured on the camera (`DoseRate`), which
   counts only the electrons that made it through the lamella and the energy filter, so it drops at high tilt and on
   thick areas. We fitted that drop across each tilt series (thickness grows as 1/cos of the tilt) and extrapolated to
   zero thickness. That gives about **4.3 e/Å² per tilt at the specimen (≈ 165 e/Å² over 39 tilts)**, consistently in
   the 14 of 23 series where the fit works. The area_6-A series (both grids) don't fit cleanly. Does ~4.3 e/Å² per tilt match what you set, and was
   the beam the same for area_6-A (both grids)? If you measured the dose over vacuum or have the planned total, that
   number would replace ours.
2. **experiment1 pixel size.** Your mail says 0.261 nm, but the mdocs say 3.371 Å (26 kx; 1.6855 Å for the
   super-resolution frames). Which one should we use?
3. **experiment1 gain reference.** The frames name `SuperRef_lam_ts_ts_01_45_000_12.0.dm4`, which isn't in the
   folder (only `SuperRef_lam_2A_16_SAmap26kx_Lam_poly_21_21_000_0.0.dm4` is). Could you send it?

## 9. Stage log

**2026-11-10 — S1–S5 built, NOT RUN (S6 owed).** ruff and `check_boundaries.py` clean. Read-only run of the new scan
(`DatasetParsingService.parse_dataset` + `find_tilt_series_depth`) on the three folders:

| folder | depth | series | ignored | positions | stack / frames | dose |
|---|---|---|---|---|---|---|
| experiment2 | 2 | 23, one project | 889 frame mdocs | 6 PACEtomo areas | 23/23 · 23/23 | 4.29 (14/23 fitted) |
| experiment1 | 2 | 3 | 109 frame mdocs | flat (`ts_0N`) | 3/3 · 3/3, frames blocked: gain missing, super-res | 2.89 (2/3 fitted) |
| first share | 1 | 4 | 156 frame mdocs | `area_5-A` | 4/4 · 4/4, frames blocked: gain not in that share | 4.28 (4/4) |

Stacks: the seven `_orig.mrc` series take the raw stack. Frames: `.tif`, axis −85.0, tilt-angle hand inverted, the
`tomo/` and `tomo2/` CountRefs hold the same image (headers differ in 19 bytes), so one gain serves the project.

What changed:
- S1. `classify_mdoc` / `describe_ignored_mdocs` / `find_tilt_series_depth` (`services/configs/mdoc_service.py`);
  the scan, `first_mdoc_facts` and the import skip anything that is not a tilt series. The landing page adopts the
  depth with the most tilt series (0–3 below the data folder) instead of the `*.mdoc` ⇄ `*/*.mdoc` flip, and the scan
  line says "23 tilt series 2 folders down (ignored: 889 frame mdocs)".
- S2. Per series the scan records both layers (`stack_path`, `movies_complete`); `DatasetOverview.layer` /
  `apply_layer` / `default_layer` / `layer_coverage`; a Stacks/Frames segmented row in the overview with a line
  saying what the choice implies; Create is held (frames field) while a selected series lacks the layer.
  `resolve_stack` and `split_stack` take `<stem>_orig.mrc` first (`stack_candidates`). The import gets the choice
  (`import_summary["source_layer"]` → `choose_source_layer(chosen=…)`) and fails on a series without it.
- S3. `read_frame_facts` (per-movie mdoc: gain, defects, RotationAndFlip, Binning), the gain found beside the mdoc or
  above it, compared by content when several (`same_image_data`), prefilled into the gain field for frames and taken
  out for stacks (a typed value is kept); `frames_tilt_axis` (ops 0 and 6 only; others leave the axis open and block
  Create); `invert_tilt_angles=True` for mirrored frames (written at creation — the first writer of that field, see
  H2). Super-resolution frames block the frames layer.
- S4. `AcquisitionSummary.dose_estimates` holds incident fits only, `dose_transmitted_only` the rest; the hint says
  "incident fit in 14/23 series (4.2–4.6); 9 without a fit (transmitted 1.0–3.3), assumed the same beam".
- S5. `<area>_ts_<NNN>` → `area_label` / `target_label`; `aggregate_to_positions` groups by area; the POS column
  shows the area, BEAM the target.

Not done: defect lists (here only edge rows); super-resolution binning; `.dm4` gains; frame-series `--exposure`
(not passed for any input today, EER included — check whether Warp's frame dose weighting needs it).

S6 runtime check, owed: experiment2 into two projects (Stacks, Frames), one series per area; motion/CTF → alignment →
`ts_defocus_hand --check` → reconstruction; the frames reconstruction must match the stack one; then all 23.

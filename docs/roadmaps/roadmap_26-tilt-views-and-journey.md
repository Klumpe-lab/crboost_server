# Roadmap 26 — Tilts, Tomograms and the Journey: what each tilt is, everywhere

**Status:** scoped 2026-10-02; proposed, nothing built beyond U1. Split out of roadmap 06 §13, which anticipated it:
the design recorded there on 2026-10-01 is appendix A, verbatim (A.n = 06 §13.n). A.2 is U1, built 2026-10-01 and not
run. The body answers A.5 (annotations), A.7 (statistics) and A.8 (the Journey); A.3 (buttons), A.4 (the barrier's
flags) and A.6 (label sets) are not designed here. The maintainer's decisions are owed in §10; the recommended first
build is V1 (§9).

**In one line.** Every surface that shows a tilt (a Tilts card, a cell of a tomogram's mosaic, a Journey point) says
the same three things about it, from one derivation: whether it is in the tomogram and, if not, why; what the review
says; whether its exposure was dark. Numbers ride along where they fit, and a tomogram's caption row sums its tilts up.

## 1. What it is for: two cases on our data
1. **Diagnosis: Grid3's blank tilts.** `agg_20260311_412_Grid3` holds 48 blank exposures (mdoc mean counts at most
   0.01 % of their series' median, all between −60° and −70°). Warp's import walk dropped 30 of them. The other 18
   (−66°, −69°, −70° in Position_13_4, 14_4, 15_3, 15_4, 16 and 16_4) are in the alignment output, so they are in those
   six tomograms: roadmap 22's streak mechanism, tilt by tilt. Nothing in the app says so. *Done when* those six tiles
   read `41/41 · −70…+46° · 3 dark`, their mosaics show three amber cells, and the Journey marks the same tilts.
2. **Calibration: FCIso_unmilled2.** The model flags 196 tilts where the May review dropped 75 (06 §7.4). Beside the
   mdoc counts: 41 of the 196 are dark exposures (under 10 % of the series median), 155 are normal exposures flagged
   in every 10° band from −60° to +60°, and 5 blank exposures get P(bad) 0.015–0.19, below the cut. *Done when* a Tilts
   card shows the model's number next to the physics, so 06 chunk 13's question (misses of the review, or of the
   model?) is answered by looking.

## 2. What the registry holds
**Coverage** (36 project registries under `/groups/klumpe/crboost_data`, scanned 2026-10-02): P(bad) in 6, mdoc counts
in 13, per-tilt CTF fit resolution in 6, motion in 5, alignment intensity in 4, a committed verdict with drops in 4;
at least 22 carry alignment's per-frame list. Grid3 and agg_20251113_412, the projects under review, hold P(bad) and,
per tilt, nothing else beyond angle, order, dose, the fsMotion / tsCtf defocus and alignment's shifts.

**Backfills, no recompute:**
- The registry-1.4 QC fields (CTF fit resolution, motion, defocus spread, motion tracks, alignment intensity / FOV /
  masked fraction, the series' CTF fit resolution, plane normal): `venv/bin/python3 crboost_reingest.py <project>`
  (exists).
- The mdoc acquisition fields (counts, exposure dose and time, defocus, image shift): `crboost_reingest.py <project>
  --mdoc` (new in V1). The mdocs are on disk (`TiltSeries.mdoc_path`); Grid3's carry `MinMaxMean` for every tilt.

A view states a missing field once, in a note line that names the backfill; never per card, never as 0.

## 3. One tilt, one identity
- **Key:** `Frame.id` on every surface. A PNG's stem resolves through its average's stem (U1); a Journey row's frame
  basename is the frame's `raw_filename`.
- **Number, today four ways.** U1 cards show `tilt_index` (0-based). Journey hovers number the rows of each section's
  table (`_per_tilt_customdata`, `ui/tomo_dashboard_dialog.py:1086`): in Motion & CTF that is `tilt_index + 1` (while
  every tilt has an fsMotion output); in Alignment, the rank among the tilts alignment kept; in CTF after alignment,
  Warp's Z, which runs in angle order (Grid3 Position_13: Z 31–37 for `tilt_index` 28–40). The CTF-fit readout follows
  the Motion & CTF rows.
- **Proposed:** "tilt N" = `tilt_index + 1` everywhere. SerialEM's file counter reads that way
  (`…_Position_13_001_-12.00_…` is `tilt_index` 0), and so do the Journey's motion hovers already. The alternative is
  0-based, like the mdoc's ZValue and Warp's Z (§10.1).

## 4. A tilt's state
One derivation, in `services/tilt_series/tilt_state.py` (new, pure), for the views (V1), the Journey (V2) and roadmap
22's exclusion, which reuses its exposure constants and ratio (the exclusion policy stays roadmap 22's).

| Field | Value | From |
|---|---|---|
| `in_tomogram` | yes / no / unknown | the frame's entry in the alignment output's `per_frame`, built from the tomostar alignment ran on; CTF and reconstruction use the same tilts. Unknown until alignment has run |
| `drop` | the committed verdict's reason, or none | `Frame.is_filtered_out` / `filter_reason`, where a verdict covers the series (`_verdict_covers`, `services/dashboard_data.py:364`: the filter is committed, or the series carries a drop) |
| `review` | human bad / human good / model bad / none | uncommitted only: the job's `tilt_labels`, then in DL review the prediction at the job's threshold (`effective_label`, `services/jobs/tilt_filter.py:286`, the commit's own rule) |
| `p_bad` | number, or none | `Frame.p_bad`; shown in DL review only, as the filter panel does |
| `exposure` | the ratio, and blank (< 1 %) / dim (< 10 %) / normal / unknown | the mdoc mean counts ÷ the series' median of them; unknown without counts, or when the median is 0 |

**Look** (cards and mosaic cells), in the filter gallery's and the Journey's dropped-cutout vocabulary (red border and
thin red stripes, never dimmed):

| State | Look |
|---|---|
| dropped by the committed filter | red border, red stripes |
| not in the alignment output, no verdict (Warp's import walk, or a verdict older than the registry) | grey stripes |
| human bad, uncommitted | red solid border |
| model bad, uncommitted | red dashed border |
| dark (blank or dim), and nothing drops it | amber border |
| in the tomogram, or nothing known against it | none |

Stripes win over a review border, a review border over amber. Amber is the warning: a dark exposure that is in the
tomogram, or will be.

## 5. The metrics
Per tilt:

| Metric | What it tells | Source | How far to trust it | Where |
|---|---|---|---|---|
| Stage tilt | where the tilt sits | `nominal_tilt_angle_deg` (mdoc) | exact, nominal: alignment's refined angle differs by the series' tilt offset | card caption; strip position; every tooltip |
| Tilt N | acquisition (= dose) order | `tilt_index + 1` | exact | card caption; tooltips; Journey hovers |
| In the tomogram, or why not | what was reconstructed | §4 | exact once alignment has run | stripes; tomogram caption |
| P(bad) | the model's opinion | `Frame.p_bad`, latest run | a ranking that holds on 412 (Grid3 AUROC 0.995; precision / recall at the cut 1.00 / 0.84, 0.87 / 0.80, 0.87 / 0.75); over-flags unmilled data (0.245 / 0.64) and passes some blank exposures (§1.2); one cut (0.301) from the author, uncalibrated until 06 chunk 13. A rank, not a probability | card caption; tooltip; Journey (V2) |
| Exposure | a beam-blocked or dim exposure | mdoc `MinMaxMean` mean ÷ the series median | high for that question: the 1 % cut sits 10× under the dimmest real tilt seen and 1000× over the blanks (roadmap 22); under 10 % was labelled bad 595 times in 599 (06 §7.3, against the mean). Raw counts: a longer high-tilt exposure raises them, which can hide dimness, never invent it | amber caption token; amber border; tooltip; Journey (V2) |
| CTF fit resolution | how far out the Thon rings were fit | `ctf_resolution` (Warp `CTFResolutionEstimate`) | Warp's own estimate; worsens with tilt and thickness; a diverged fit still reports one (roadmap 19) | tooltip; metric slot (V3) |
| Motion | drift or charging during the exposure | `mean_frame_movement` | real; unit unconfirmed (Å assumed); missing on some frames | tooltip; slot (V3) |
| Δ defocus | a collapsed or diverged CTF fit | tsCtf `per_frame` defocus (else fsMotion) minus the series median | exact arithmetic, crude reading (it ignores the trend with tilt); roadmap 19's 1D scan is the verdict. Grid3: 43 tilts more than 0.5 µm off (§8.8); blank tilts fit anywhere (6 of the 18 in use land within 0.5 µm) | tooltip; slot (V3) |
| Dose before | accumulated damage | `pre_exposure_e_per_a2` | nominal, from the scope's calibration; a series whose mdoc carried no dose reads 0 throughout, which the tooltip calls unknown | tooltip |
| Alignment shift | how far the aligner moved the tilt | alignment `per_frame` x / y | a difficulty proxy (global AreTomo writes no residual); spikes on blank tilts (roadmap 22: 130–444 px) | tooltip; slot (V3) |

Per tomogram:

| Metric | What it tells | Source | How far to trust it | Where |
|---|---|---|---|---|
| Used / total | how many tilts were reconstructed | alignment `per_frame` count / frames | exact | caption |
| Kept range | the missing wedge | min…max stage tilt of the tilts used | exact, nominal | caption (M, L) |
| Dark in use | streaks to expect | tilts in use whose exposure is blank or dim | high | amber caption token |
| CTF fit (series) | a ranking across tomograms | tsCtf `ctf_resolution` | Warp's estimate | caption (M, L), when recorded |
| Stage-tilt offset, largest shift, plane tilt | alignment health | alignment `per_frame`; tsCtf `plane_normal` | the offset fit is weak on 412 (CC ≤ 0.013, some at the search edge; roadmap 22 §1) | caption tooltip |

## 6. The surfaces
### 6.1 Tilt card (Tilts tab)
- **Caption:** `+42.0°`, then `dark 0.4%` (amber, only when dark; one decimal, `<0.1%` below that), `p0.98` (DL
  review; red at or above the threshold), `#13`. The row clips from the right, so the angle and the state survive the
  S size.
- **Border and stripes** per §4.
- **Tooltip** (native title, one line per fact): angle · tilt N · frame id; the state as a sentence ("not in the
  alignment output: Warp's import walk drops dark tilts, or the verdict predates the registry"); P(bad) and the
  threshold; the exposure ratio; CTF fit, motion, Δ defocus, dose and shift where recorded; "not recorded: …" for the
  rest.
- This answers A.5's Grid3 note: a human-labelled card still carries the model's number, so agreement (a red `p` on
  a solid red card) and disagreement (a grey one) show without another glyph.

### 6.2 Mosaic cell (a tomogram tile's tilts)
No text: stripes and the amber border only, i.e. what went into this tomogram and whether something dark did. The
card's tooltip.

### 6.3 Tomogram caption row
Left, the position label (as now). Right, mono 9 px: `41/41 · −70…+46° · 7.1 Å` and, amber, `3 dark`. S keeps
used / total and the dark token; M and L add the range and the series' CTF fit. The tooltip breaks used / total down
by cause and lists the dark tilts: "3 tilts in use recorded under 1 % of their series' median counts (−66°, −69°,
−70°); a beam-blocked exposure back-projects as straight streaks." An imported tomogram (no tilts) keeps the bare
label.

### 6.4 Tilts-tab group header and its strip
After the tilt count: `38 in the tomogram · 2 flagged · 3 dark` (flagged: the model's call is bad and uncommitted;
dark: nothing drops it), each part only when nonzero. Then the **strip**: one 2 px tick per tilt at its stage tilt, on
an axis shared by every header (the project's angle span), with a faint 0° mark. A tick takes its §4 state: red for a
drop, pale grey for "not in the alignment output", a red outline for an uncommitted bad, amber for dark, slate for the
rest. Collapsed, the list of headers is the project's tilt scheme: where every series loses tilts, and each series'
missing wedge. That is A.7's "distribution of excluded tilts … per tilt series" without a chart. A tick's title names
its tilt; a click opens the viewer (the cards' delegated click). At 200 px, ticks 1° apart touch.

### 6.5 Summary (V3)
Top of the Tilts tab, collapsible:
- Tilts not in the tomograms by |stage tilt| in 10° bands, stacked by cause (the filter, by a human or the model;
  Warp's import), with dark-in-use as an amber overlay. Bars, for binned counts.
- The P(bad) histogram (log counts) with the threshold, stacked by human label, and one line: "the model flags 47 ·
  your labels say 56 bad · both 47 · only yours 9 · only the model's 0 (untouched tilts count as good)".
- No third chart: the strips (6.4) are the per-series view.

## 7. The Journey
Every per-tilt metric of §5 but two already has a Journey chart: CTF fit, motion and defocus in Motion & CTF and Tilt
QC, shift in Alignment. V2 adds the two (P(bad), exposure) to the Tilt filter section and threads the state through
every chart.
1. **Identity in every hover.** "Tilt N" per §3 in every section, from the registry (`_per_tilt_customdata`,
   `ui/tomo_dashboard_dialog.py:1073`), the CTF-fit readout included; and the §4 state line in every section's hover
   (today only CTF after alignment passes the verdict, `:1662`).
2. **Motion & CTF** plots every tilt: tilts not in the tomogram as open markers, dark ones in use with an amber ring
   (per-point `itemStyle` in `build_per_tilt_chart`, `ui/dashboard/figures.py:538`; series and legends unchanged). An
   outlier then reads as "not used" or "dark" rather than as a bad fit.
3. **Alignment and CTF after alignment** have no row for a dropped tilt: a rug of short ticks under the x axis at the
   angles of the tilts missing from the output (red = the filter, grey = Warp's import), named on hover, and a "38 of
   41 tilts" tile.
4. **Tilt filter section** (`:1815`): two per-tilt charts, markers only. **P(bad)**, with the threshold as a dashed
   line, human labels filled and predictions open. **Exposure**, counts ÷ series median on a log axis, with lines at
   1 % and 10 %. Between them they explain every drop. The stat tiles and the drop list stay; each dropped row names
   its cause.
5. **Links.** A double-click on a per-tilt point opens the full-size tilt viewer (in Motion & CTF, hover and click
   drive the CTF-fit panel, so a single click is taken). The viewer's top bar (`_show_upsample`,
   `ui/tilt_filter_panel.py:941`) gains **Journey ↗** (the series, Motion & CTF, the tilt selected in the CTF-fit
   panel; `journey_show_ts` takes a frame) and **Tilts ↗** (Tomograms view, Tilts tab, the group open, the card
   scrolled into view and outlined for a second), each shown when the viewer was opened from the other side. The
   tomogram zoom's Journey ↗ stays.
6. **Routes** (roadmap 17): `/p/<project>/tomograms/tilts[/<ts>]` (`_TARGETS[View.TOMOGRAMS] = 2`,
   `ui/routing.py:51`: the tab, then the series) and `?t=<frame id>` there and on `/journey/<ts>` (outline the card;
   select the tilt in the CTF-fit panel). Written with replaceState on a tab switch and a group expand. The viewer, a
   modal, gets no route.

## 8. Found on the way
1. The same tilt reads as up to four numbers (§3).
2. `?s=fs_ctf` and `?s=ts_align` scroll nowhere: the section cards carry `data-section` `fs_motion_ctf` and
   `ts_alignment` (`ui/tomo_dashboard_dialog.py:1394, 1405, 1525, 1536`), while `JOURNEY_SECTIONS`
   (`ui/routing.py:60`) and the panel keys say `fs_ctf` and `ts_align`. Fixed with 7.6.
3. Only CTF after alignment puts the filter verdict in its hovers (`ui/tomo_dashboard_dialog.py:1662`).
4. "Beam-induced motion" connects per-tilt points (`"mode": "lines+markers"`, `:1467`), against the plot rule; it is
   on the maintainer's "don't touch yet" list (roadmaps README). 7.2 restyles its markers either way.
5. The tilt filter panel reads CTF fit and motion from the Warp XMLs at render (`ui/tilt_filter_panel.py:434-443`);
   the Journey reads the registry. They can disagree after a re-run until a re-ingest.
6. **Grid3's tomograms hold 26 of the May review's 56 drops.** The registry has no verdict, and alignment's output
   holds 667 of 697 tilts. The 30 missing are blank exposures at the end of the negative branch, all among the May
   drops: Warp's import walk truncated them in eight series and kept them in six (roadmap 22 §1, item 3). The other 26
   are the 18 blank tilts of §1.1 and 8 normal exposures (Position_16_5 one, 1_5 one, 2 two, 20_3 four). The May
   verdict never reached alignment here, as on agg_SK_20251017_412 (roadmap 22 §1, item 4); this is roadmap 22's
   "Grid3 (18)", tilt by tilt.
7. Grid3's `tilt_labels` on disk hold 697 "good" and no "bad" (06 §13.9 recorded 687 good and 10 bad). The restore's
   target, 62 labels, is unchanged.
8. Grid3's CTF after alignment: 43 tilts sit more than 0.5 µm from their series' median defocus. Position_13 at +45°
   and +46° reads 1.53 and 1.36 µm against 4.98; Position_2 has 17, from 1.07 to 7.31 µm against 4.71. Roadmap 19's
   question, on a second dataset.

## 9. Stages
| Stage | What | State |
|---|---|---|
| V1 | What each tilt is, read-only: the state, caption rows, strips, one numbering, the mdoc backfill | proposed first build |
| V2 | The Journey: hovers, marked charts, the filter section's charts, links, routes | outline |
| V3 | Summary charts, the metric slot, sort and show switches | outline |
| V4 | The review moves into the Tilts view | direction; decide before V3 |

### V1 — what each tilt is (recommended first build)
Read-only, touches no job, and shows §1.1 on data we have. V2 and roadmap 22 build on its derivation.
- **`services/tilt_series/tilt_state.py`** (new, pure): `BLANK_EXPOSURE_FRACTION = 0.01` and
  `DIM_EXPOSURE_FRACTION = 0.10`, each with its reason; `exposure_ratios(ts)`; `TiltState` (frozen: number, angle,
  in_tomogram, drop, review, p_bad, exposure ratio and class); `tilt_states(ts, *, alignment_instance, committed,
  labels, threshold, mode)`; `series_summary(states)` (used, total, kept range, dark in use, flagged). Run
  `check_boundaries.py`: `services.jobs.tilt_filter` imports the registry package.
- **`crboost_reingest.py --mdoc`**, alone or with `--job`: per series, parse `ts.mdoc_path`
  (`get_mdoc_service().parse_mdoc_file`, `services/configs/mdoc_service.py:375`), match sections to frames by the
  SubFramePath basename, write `_acq_kwargs_from_raw_section` (`services/tilt_series/build.py:243`) onto each frame;
  name every frame without a section and every missing mdoc; save. The fields are import's own, and this writes what
  import would have.
- **`ui/tilt_previews.py`:** the collection carries each tilt's `TiltState` and recorded metrics, each group's
  summary, the project's angle span and the field families the project lacks. Card per 6.1, cell per 6.2, header and
  strip per 6.4 (one HTML string per header, ticks with `data-key`, `TILT_CLICK_JS`). `_tip` becomes the multi-line
  tooltip; the number is `tilt_index + 1`.
- **`ui/tomo_gallery.py`:** the caption row per 6.3 (`_fill_tile`, `:551`); the header per 6.4
  (`_render_tilt_group`, `:697`); a strip click opens the viewer; one note line per missing field family in the Tilts
  tab, naming the backfill. `_collect` (`:346`) snapshots the filter job's labels, threshold, mode and committed flag
  on the loop together with the registry read; the derivation runs in the thread pass. A commit or DL run made while
  the view is open shows on Refresh.
- **`ui/dashboard/css.py`:** the state classes on `.cb-tp-card` and `.cb-tp-cell` (the stripes as
  `.cb-tile-dropped::after`, `:860`, plus a grey variant), and the strip.
- **Journey hovers:** `_per_tilt_customdata` takes "Tilt N" from the registry in every section, and so does the
  CTF-fit readout, so a card's `#N` and a hover's "Tilt N" agree from V1 on.
- **Not in V1:** the Journey's charts and links, routes (V2); summary charts, the metric slot, the switches (V3);
  labelling in the Tilts tab (V4).
- **Check (user):**
  1. Server stopped: `venv/bin/python3 crboost_reingest.py /groups/klumpe/crboost_data/agg_20260311_412_Grid3 --mdoc`
     names no unmatched frame. Start the server.
  2. Grid3's wall: 13_4, 14_4, 15_3, 15_4, 16 and 16_4 read `41/41 · −70…+46° · 3 dark`; 13, 13_3, 14_3, 14_5 and
     16_5 read `38/41 · −63…+46°`; 8_2, 9 and 9_3 read `36/41 · −57…+46°`; 1_5, 2 and 20_3 read `41/41 · −70…+46°`.
  3. 13_4's mosaic opens with three amber cells; 13's with three grey-striped ones.
  4. The Tilts tab, collapsed: the strips show the same; a tick's title names its tilt and a click opens the viewer. A
     card's `#N` equals the counter in its frame name and the Journey's "Tilt N" in all three sections.
  5. FCIso_unmilled2 with its filter row in DL review: dashed red cards with a red `p`;
     `Position_19_040_-58.00_…` reads `dark <0.1%` beside a grey `p0.19`.
  6. A project without counts (agg_20251113_412) shows one note naming `--mdoc`, and no card reads `dark`.

### V2 — the Journey (outline)
§7.1–7.6, with §8.2's `data-section` fix. Files: `ui/tomo_dashboard_dialog.py`, `ui/dashboard/figures.py`,
`ui/tilt_filter_panel.py` (the viewer's links), `ui/tilt_previews.py`, `ui/tomo_gallery.py`, `ui/routing.py`,
`ui/workspace_page.py` (a `tilts_show(ts, frame)` callback beside `toggle_gallery`). *Check:* on Grid3, 13_4's Motion &
CTF charts ring −66°, −69° and −70° in amber; 13's draw them open, and its Alignment charts carry three grey rug ticks;
a double-click opens the viewer, whose Tilts ↗ lands on the outlined card;
`/p/agg_20260311_412_Grid3/tomograms/tilts/agg_20260311_412_Grid3_Position_13_4?t=<frame id>` opens there.

### V3 — statistics and the metric slot (outline)
6.5, plus a metric switch in the Tilts toolbar, `P(bad) · exposure · CTF fit · motion · Δ defocus · shift` (only those
with data), which sets the card's last caption token and a worst-first sort; and a show switch, `all · not in the
tomogram · flagged · disagree · dark`. Where the switches live depends on V4.

### V4 — one gallery (direction)
The Tilts tab already has the cards, the state, the viewer and (with V2) the routes. A click that labels there, as in
the filter gallery, leaves the filter panel its DL section and Approve, and answers A.3's "one home per action":
labelling in Tilts; Approve and Run DL on the job row. Not scheduled; decide before V3, whose switches belong to the
gallery that reviews. Label sets (A.6) are designed with it.

## 10. Decisions owed
1. **Tilt number:** `tilt_index + 1` (proposed), or 0-based like ZValue and Warp's Z.
2. **"In the tomogram"** = in the alignment output (proposed).
3. **Dark exposure:** blank under 1 % and dim under 10 % of the series' median mdoc counts, constants shared with
   roadmap 22; shown here, never excluding here (proposed). 06 chunk 13 restates §7.3's 595 of 599 against the median.
4. **Backfill:** `crboost_reingest.py --mdoc` (proposed), or the registry fills missing acquisition fields from the
   mdoc when it loads.
5. **P(bad) on Tilts cards:** in DL review only, as the panel (proposed), or whenever recorded.
6. **The strips as the project's tilt scheme** (proposed), or a separate chart in the summary.
7. **V4:** does the review move into the Tilts view?

## Appendix A — roadmap 06 §13.1–13.8, as recorded 2026-10-01
Moved verbatim on 2026-10-02. A.n is 06 §13.n; every other § reference below is roadmap 06's. §§5–7 above answer A.5,
A.7 and A.8; A.1 is decided and A.2 built; A.3, A.4 and A.6 stay open.

### A.1 Where the PNGs come from, and when — the opening question
*Do we have to wait for fsMotion to make the tilt PNGs?* For the model's input, yes; for the whole job, no.
- **Source.** The PNG is fsMotion's motion-corrected average of one tilt
  (`External/jobNNN/warp_frameseries/average/*.mrc`; 4096², 33.5 MB each on Grid3) Fourier-cropped to 384² and
  min-max scaled to 8 bits (`filterTilts/image_processor.py`). The raw tilts are EER movies (event streams, no gain
  correction applied); fsMotion is the first step that gives a 2D image per tilt.
- **Why the model needs that source.** The ResNet was trained on v1 CryoBoost's `cryoBoostPNG` files (the columns
  the author's `makeTrainSet.py` reads), which v1 made from motion-corrected averages with the same `filterTilts`
  image processor. A preview summed from raw frames (no gain or motion correction) is a different input; the model's
  numbers on it would need their own calibration.
- **When, today.** One background task on the headnode for the whole project (`ensure_tilt_thumbnails`,
  `services/tilt_series_service.py:292`, 16 processes), started on the fsMotion → SUCCEEDED edge by both reconcilers,
  by the Journey's self-heal, or by the panel's Generate button. It reads every average over NFS (~23 GB for Grid3's
  697 tilts). fsMotion itself runs one SLURM array task per tilt series, so the first series' averages exist long
  before the PNG pass starts. The gallery (`fs_motion_star`) and Run DL also wait for fsMotion SUCCEEDED.
- **Readers.** The gallery, the DL driver (model input, byte for byte), the Journey's per-tilt hover cards
  (`/api/tilt-thumb`, `services/dashboard_data.py:277-291`).
- **Options.**
  - (a) *Per tilt series, inside fsMotion's array task* (recommended): the task writes its series' PNGs right after
    Warp writes the averages, on the compute node. The gallery and Run DL can then open series by series while
    fsMotion still runs, and the headnode stops reading the averages. The headnode pass stays as the fallback for
    projects processed before. Needs: per-series readiness instead of fsMotion SUCCEEDED (the registry knows which
    series carry fsMotion outputs); a Run DL on a partial set (predict what has landed, or wait).
  - (b) *Before fsMotion, from the mdoc* (no images): the per-tilt counts give the dim/blank physics label at import
    (§7.3: 595 of 599 dim tilts are labelled bad; roadmap 22 holds the 1 % blank rule). It marks the worst tilts
    before any processing; it is not a gallery.
  - (c) *Before fsMotion, from raw frames*: needs an EER/TIFF decoder and the gain reference in our stack (or a
    Warp/RELION call per tilt), and gives the model an input it was not trained on. At most a human-only preview.
- Dropping a tilt before fsMotion saves little compute (fsMotion per tilt is cheap); the gain of (a)–(c) is time to
  the first review.
- **Decided (2026-10-01):** the PNGs stay a post-fsMotion step, made as they are now. (a) is parked.

### A.2 U1 — Tilts beside Tomograms, and each tomogram's tilts in place (spec; build first)
The maintainer's design (2026-10-01): tilt previews become a view of the project, not only the tilt filter's gallery.
It is the canvas the later interactions, annotations, sorting and statistics (A.3–A.8) build on, so it comes first
and must be robust and navigable.
- **Where.** The Tomograms view (`ui/tomo_gallery.py`, `TomoGalleryPage`) gets a tab switch in its toolbar in place of
  the "Tomograms" title: `Tomograms · Tilts`, the house `render_segmented` with each tab's count in its badge (no
  material tabs). Tomograms keeps today's wall. Tilts shows every tilt preview, grouped by tilt series, as the
  tilt-filter gallery lays them out now. It is there for any project whose fsMotion has run; before that the tab says
  so. A project with tilts and no tomograms opens on Tilts. The toolbar's tab-specific controls (source and picks for
  Tomograms; Expand all / Collapse all for Tilts) sit in their own container, so a tab switch rebuilds them and not the
  strip that was clicked.
- **Each tomogram's tilts, in place.** A small toggle in a tile frame's top-left corner (the pick badge holds the
  top-right) replaces the slice with a mosaic of that tilt series' tilts, inside exactly the box the slice occupied;
  toggled back, the slice returns. Per tile and independent, so one or two tomograms can be open at once; the open set
  lives on the page, so a size switch, a refresh or the pending poll keeps it. Only a tile whose tomogram has tilts
  gets the toggle (an imported tomogram has none). Re-render only the toggled tile (keep a ref per tile), not the wall.
  - *Fit:* for n square cells in a frame of aspect a = W/H, take the cols that maximise
    min(a/cols, 1/ceil(n/cols)); cell width = min(100/cols, 100/(a·rows)) % of W, the grid centred both ways, and a
    1 px padding inside each cell as the gutter (a CSS gap would overflow the percentages). A frame with no known extent
    takes the aspect cols/rows. 41 tilts in a square frame: 7 × 6.
  - *Order:* by tilt angle, most negative first, left to right and top to bottom (acquisition order becomes a sort
    option later). Each cell's native tooltip: angle, acquisition index, frame id.
  - *In mosaic mode* the frame drops its own zoom click, the pick dots and the pick badge; the toggle uses
    `click.stop`, since in slice mode it sits inside the frame's zoom click.
- **Zoom.** A tilt, in the mosaic or on a card, opens the tilt filter's full-size viewer `_show_upsample`
  (`ui/tilt_filter_panel.py:905`): the averaged MRC re-rendered at 512 / 1K / 2K / full, maximised, on black. Open it
  under `dialog_host()` (`ui/components/dialogs.py`), or a re-render of the tile destroys it mid-load. The tomogram
  zoom stays as it is; its "Journey ↗" button is this view's way to the Journey. Later: ← / → through the series'
  tilts inside the viewer.
- **The rows under the images** (the maintainer likes them; they are reserved for metrics, A.5 / A.7). The tomogram
  tile's caption row stops being a Journey link: no click, no "↗", no hover tint (`.cb-gal-cap`, `.cb-gal-go` in
  `ui/dashboard/css.py`); it keeps the position label with the tilt-series name in its tooltip, and the rest of the
  row waits for the metrics. Tilt cards get the same row: angle and acquisition index now. The tilt-filter gallery's
  card rows stay as they are.
- **Data.** From the tilt-series registry, not the fs-motion star: per frame `id` (the PNG's stem),
  `nominal_tilt_angle_deg`, `tilt_index`, and its fsMotion output's `averaged_mrc` (the zoom's source); the PNG dir from
  `tilt_thumb_dir` (`services/dashboard_data.py:274`), listed once rather than one stat per tilt. A frame shows when it
  has a PNG or an fsMotion output. Collected in the same thread pass as `collect_rows`. A registry failure is logged
  and stated in the Tilts tab while the wall still renders. Groups ordered like the tiles (`position_label`).
- **Rendering.** One `ui.html` per tilt series (its mosaic or its card grid) with one delegated click handler: each
  tilt carries `data-key`, and a `js_handler` emits the key, like the tilt filter's `_CARD_CLICK_JS`
  (`ui/tilt_filter_panel.py:647`). A project holds thousands of tilts; an element per tilt would be thousands of
  NiceGUI elements. Tilt groups start collapsed and fill on first expand (the tilt filter's pattern), with Expand all /
  Collapse all; images `loading="lazy"`. The wall's S / M / L switch sizes the cards too (about 90 / 130 / 190 px).
- **Thumbnails not there yet.** The page calls `ensure_tilt_thumbnails` (`services/tilt_series_service.py:292`), as
  the Journey does. Its 15 s pending poll also counts tilts that have an fsMotion output but no PNG, while the
  thumbnail task runs (`BackgroundTask.existing` on its dedup key, `tilt-filter-thumbnails:<project>:<png dir>`), and
  its signature carries the PNG count per series, so a landed batch re-renders.
- **Also U1:** remove the rail's "Link to this view" button (`_build_link_btn`,
  `ui/pipeline_builder/pipeline_roster.py:1880`, called at :1015). The address bar still carries the route.
- **Not in U1:** the tab in the URL (`/p/<project>/tomograms/tilts`: `_TARGETS` and `apply_route` in `ui/routing.py`);
  annotations, sorting, statistics (A.5–A.7); labels and the review (the tilt filter keeps its own gallery for now).
- **Files:** `ui/tilt_previews.py` (new: the registry collection, mosaic and card HTML, the fit, the click JS),
  `ui/tomo_gallery.py`, `ui/dashboard/css.py`, `ui/pipeline_builder/pipeline_roster.py`.
- **Check (user):** on a project with tomograms, the Tilts tab lists every series and a card opens the full-size
  viewer; on the wall, one tile's toggle shows its tilts in the same box while its neighbours keep their slices, and
  toggling back restores the slice; a size switch and Refresh keep the open mosaics; a tilt in a mosaic opens the
  viewer; a project without tomograms opens on Tilts; the rail has no link button.
- **Built 2026-10-01** (ruff-clean, not run), as specced, with these refinements:
  - A tilt's PNG is looked up by its fsMotion average's stem, which is how the thumbnail pass names it, then by the
    frame id.
  - The registry is read on the event loop (`registry_tilt_series`, a list of its tilt series), since a re-sync started
    from a worker thread could change the shared cached registry under another reader. The PNG listing runs in the
    same thread pass as `collect_rows`.
  - The mosaic is a CSS grid of `cols` equal columns inside a box whose width is the fitted percentage, not flex cells
    with percentage widths: rounding those up can push a row's last cell onto the next row.
  - The pending poll skips tiles that show their tilts, since they kick no slab render and would otherwise keep it
    polling. It counts tilts waiting for a preview while the thumbnail pass runs, and once more after it ends, so the
    last batch lands. `ensure_tilt_thumbnails` is called only when no tilt has a preview: it does nothing otherwise.
  - The tab is chosen once, at the first collection: Tilts when there are tilts and no tomograms.
  - A tilt reads "tilt N" with the registry's 0-based `tilt_index`, as in the Journey's CTF readout, and its angle
    with a sign (`+12.0°`).
  - `_build_link_btn` is deleted with its call; the caption row keeps the tilt-series name as its tooltip; `.cb-gal-go`
    and the caption hover rules are gone from `ui/dashboard/css.py`.

### A.3 Buttons and controls
- *Now.* Row (`ui/pipeline_builder/tilt_filter_row.py`): `Segmented` Manual · DL review, the §2 chip, `house_button`s
  Run DL / Cancel DL / Approve (accent) / Re-open, "· K waiting". Panel (`ui/tilt_filter_panel.py`): a
  `ui.expansion` "Deep Learning Auto-Filter" with a material icon (model select, Run DL), stat chips, then Approve /
  Set all good / Expand all / Collapse all / sort (a bare `ui.select`) / threshold (`house_number`) / "Show only
  removed" (a bare `ui.checkbox`).
- *To decide.* One home per action (Run DL and Approve exist on the row and in the panel); the expansion with its
  icon and the bare select/checkbox are outside CLAUDE.md's control vocabulary; DL auto's controls (chunk 11).
- *Defect l (fixed in 10d).* "Clear labels" replaces "Set all good", behind a confirmation that names the count: in
  Manual every tilt then reads good, in DL review every tilt goes back to its prediction. Every bulk action gets a
  confirmation or an undo (A.6).

### A.4 The barrier's flags
- *Now.* The §2 chips, "· K waiting" with its since-when tooltip, the poller's "The pipeline waits for the
  tilt-filter review", the interactive-job header tooltip. Parked jobs read SCHEDULED with no job dir.
- *To decide.* A distinct roster state for a parked job; a project-level "waiting for review" marker (rail badge,
  landing hub), since a parked project is otherwise idle; how DL auto reads when it fails on liveness and when it is
  done ("D of T dropped"); how a refused Re-open reads once alignment has run.

### A.5 Annotations on the tilt previews
- *Now.* A card shows the PNG, `pX.XX` (red at or above the threshold), a dashed border for predicted-bad, a solid
  border and filled dot for a human label, and a zoom into the MRC; each group header counts its bad tilts.
- *Seen on Grid3.* The 47 tilts the model flagged were all human-bad, so all 47 cards were solid: the model's calls
  were invisible, which may be why "Set all good" got clicked. A human-labelled card could show whether the model
  agrees.
- *Candidates.* Angle, acquisition order and accumulated dose; CTF resolution and motion from the Warp XML (the
  panel already loads `_xmlRes` / `_xmlMotion`); the mdoc dim flag (A.1 b); P(bad) as a bar rather than text.

### A.6 Label sets and their caching
The maintainer's phrase is "caching of labels per set"; two readings, possibly both:
- (i) *Sets that never overwrite each other*: the human edits, each DL run's predictions (only the latest survives,
  in `Frame.p_bad`), each committed verdict (only the latest, in the registry), kept as named sets with history, so a
  bulk action can be undone and runs compared. On 2026-10-01, Grid3's labels came back only because an analysis
  printout and the May `tilt_series_labeled/` / `tilt_series_filtered/` stars still held them.
- (ii) *Labels per data set*: keyed by the raw tilt (mdoc key + frame), so every project that processes the same
  grid reuses them (the 412 grids appear in several projects; chunk 13's dedupe needs the same key).
- Where: per project `TiltFilter/labels/<set>.json` (small, diffable), the registry (one value per frame), or a
  lab-level store beside the species catalog (for ii).

### A.7 Statistics
- *Now.* Total / Good / Bad / Removed % chips in the panel; the dashboard's per-series Kept / Dropped tiles and
  dropped list (`ui/tomo_dashboard_dialog.py:1813`).
- *Wanted.* The distribution of excluded tilts: by |angle| band (as in §7.4's tables), by acquisition order or
  accumulated dose, per tilt series (kept count, and the angular range left, i.e. the missing-wedge cost); a P(bad)
  histogram with the threshold marked; agreement between the model and the human labels (the precision and recall this
  roadmap computed by hand). The dashboard plot rules apply (no lines between discrete points; hovers name the tilt
  and frame).

### A.8 The Journey
- *Now.* Per-tilt hover cards show the gallery PNGs; the per-frame verdict string "keep/drop (P(bad) x)"
  (`services/dashboard_data.py:378`); the dashboard's per-series filter section.
- *To decide.* Dropped tilts marked in the per-tilt QC charts (defocus, resolution, motion against angle), so a
  dropped tilt reads as dropped rather than as an outlier; P(bad) as a per-tilt track; links between a Journey tilt
  and its gallery card in both directions; an addressable gallery route (`/p/<project>/…`, roadmap 17).

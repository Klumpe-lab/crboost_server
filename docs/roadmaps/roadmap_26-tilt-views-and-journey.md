# Roadmap 26 — Tilts, Tomograms and the Journey: what each tilt is, everywhere

**Status:** scoped 2026-10-02; §10 decided the same day; V1 built 2026-10-02 (ruff-clean, not run). Split out of
roadmap 06 §13, which anticipated it: the design recorded there on 2026-10-01 is appendix A, verbatim (A.n = 06
§13.n). A.2 is U1, built 2026-10-01 and not run. The body answers A.5 (annotations), A.7 (statistics) and A.8 (the
Journey); stage F answers A.3 (buttons) and A.4 (the barrier's flags); A.6 (label sets) is not designed here. Stage F
(§9), specced 2026-10-02 from the maintainer's answers (§10.8–10.14), was built the same day as commits F1–F5
(ruff-clean, not run; each F subsection lists its refinements). Next: F's runtime pass (the checks under F1–F5, with
V1's), then V2. `crboost_reingest.py`, the mdoc backfill included, was a prototyping stopgap; F4 deleted it (§2).
Stage G (§9), scoped 2026-10-04 from the maintainer's review of F in use (DL review on 8 projects of one dataset;
decisions §10.15–10.23), revises F's annotations, clicks, page and row: built the same day as G1–G5 (ruff-clean,
not run).

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

**The backfills are a prototyping stopgap (maintainer, 2026-10-02).** They exist only so the new views can be tried
on registries written by older code. A project created and run with the current code records the mdoc fields at
creation and the QC fields as each job finishes, so it never needs them. Test data is re-run through the current code
instead (the copia protocol makes a fresh project), and the stopgap goes: `--mdoc`
(`crboost_reingest.py`, `TiltSeriesRegistry.fill_frame_acquisition`) and the Tilts tab's note lines that print the
command (`TomoGalleryPage._lacking_notes`). From then on a field an old registry lacks reads "not recorded", with no
command. Users are never pointed at a shell command. The rest of `crboost_reingest.py` (the 1.4 QC re-ingest) goes
with it (decided 2026-10-02, §10.13): F4 deletes the script, and an old registry reads "not recorded" for the QC fields
too.

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
9. **The outlier rule marks 31 % of the tilts of a 126-series project** (the page's `outliers` tile: 1555 of 5007,
   2026-10-04). Three robust SDs on the bad side, over five metrics, should mark about 1 %. Either a metric's values
   cluster so tightly within a band that its MAD, and with it the cut, collapses (`BandStat.judges` refuses only a
   spread of exactly 0, `services/tilt_series/tilt_state.py:198`), or a heavy-tailed metric (alignment shift, motion)
   needs a log scale or a higher cut. G5's per-metric breakdown names the metric; the fix follows it. Until then
   outliers mark only the metrics ticked in the metrics menu (§10.21).
10. **After Re-open, the old verdict shows through the review.** `tilt_states` reads `is_filtered_out` whether or not
    the filter is committed (`services/tilt_series/tilt_state.py:162`) and `_look` puts the drop first
    (`ui/tilt_previews.py:784`), so a tilt the previous commit excluded keeps its stripes after it is labelled
    included again, until the next Approve re-stamps it. G3 takes the border from the review while it is uncommitted.
11. **"The parent slot of the element has been deleted" (fixed 2026-10-04).** The server log printed this traceback
    from NiceGUI's timer loop. `render_tilt_filter_status` made a 3 s `ui.timer` inside every roster render, and a page
    load renders the roster twice before the browser connects, so the first render's timer was deleted before its
    first tick; NiceGUI 3.0.3 reads a timer's parent slot before it checks whether the timer was deleted. The
    filter's views and page now use `owned_timer` (`ui/components/reactive.py`): parented at the page layout,
    cancelled on its first tick after its view's container is deleted. Other timers made inside rebuilt containers
    (the array-task tracker, the logs tab) follow the old pattern; they are the next suspects if the traceback
    returns.

## 9. Stages
| Stage | What | State |
|---|---|---|
| V1 | What each tilt is, read-only: the state, caption rows, strips, one numbering, the mdoc backfill | built 2026-10-02, not run |
| F | The tilt-filter job: the row as a dropdown of settings, the job page as a review panel over the shared gallery, one card with a metric chooser, a popover, outliers and a label flag; DL auto (06 chunk 11) | built 2026-10-02 as F1–F5; the review page in use on 8 projects 2026-10-04 (no formal runtime pass); G revises it |
| G | One exclusion signal (red border) for the model and manual labels, info as icons, a click toggles and a magnifier zooms, a table on hover, a metrics menu, group by position, a quieter page (stats first, small charts tucked away), a flat row, Approve labels; DL review predicts by itself in a Run | scoped 2026-10-04 (§10.15–10.23); built 2026-10-04 as G1–G5, not run |
| V2 | The Journey: hovers, marked charts, the filter section's charts, links, routes | outline; after F |
| V3 | Summary charts, the metric slot, sort and show switches | mostly built by F; what is left is outlined below |
| V4 | The review moves into the Tilts view | decided 2026-10-02: yes, after V2 |

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
- **Built 2026-10-02** (ruff-clean, not run; `check_boundaries.py` not run either, the sandbox has no Python; no new
  import crosses R1–R4 by reading), as specced, with these refinements:
  - The dark rule is explained wherever a dark tilt is marked (§10.3): a card's tooltip, the header's and the
    caption's `dark` tooltips, and a legend line at the top of the Tilts tab naming each look the project's tilts take
    (`dark exposure (under 10% of its series' median counts): a marker, never a drop`), each with its explanation as a
    tooltip. One text, `DARK_RULE` in `ui/tilt_previews.py`, built from the two constants.
  - "Flagged" in a header counts every tilt the uncommitted review marks bad, a human's or the model's (§6.4 said the
    model's only); its tooltip splits the two. Approve drops both, so both are what the header should warn about.
  - Two dark counts: a header's `N dark` = dark tilts nothing drops and the review does not flag (the cards' amber);
    a caption's `N dark` = dark tilts in alignment's output (the streaks to expect). On Grid3's six they agree.
  - A mosaic cell shows no review, so its amber marks any dark tilt nothing drops, flagged or not.
  - `--mdoc` fills only unset fields and never replaces a recorded value; `TiltSeriesRegistry.fill_frame_acquisition`
    marks the series dirty. Alone it does the mdoc pass only; with `--job` it does both.
  - The strip draws every tilt of the series, including one without a preview or average; clicking that tick says
    so. Ticks are 4 px hit boxes around a 2 px mark; the flagged tick is a 4 px red outline (2 px reads solid).
  - Δ defocus: CTF after alignment for the tilts it fit, else Motion & CTF, each against its own median; the tooltip
    names the source. The tooltip lists what is not recorded (counts, CTF fit, motion, defocus, dose), never the
    shift: a tilt not aligned has none by definition.
  - Missing families are stated as note lines at the top of the Tilts tab, one per family with its series count and
    the exact command: mdoc counts (`--mdoc`), alignment's per-tilt list (`--job aligntiltsWarp`), the 1.4 QC fields
    (plain re-ingest).
  - The Journey's numbering: the registry frames carry `cbTiltNumber` (= `tilt_index + 1`) into `fsm_registry_df` and
    the tsCtf / alignment frames; `_per_tilt_customdata`, the CTF-fit readout and the motion-track hover read it.
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

### F — the tilt-filter job: its row and its page on the shared components (built 2026-10-02, not run)
The maintainer's design (2026-10-02), from the answers to the five questions and four forks recorded as §10.8–10.14.
Three parts: the job row becomes a dropdown holding the filter's settings; the job page becomes a review panel over the
Tilts tab's gallery; the card gains a metric chooser, a caption popover, red outliers and a label flag. Five commits,
F1–F5, built back to back, one runtime pass at the end.

**Where it stands.** Roadmap 06 chunks 8–9 (the row's switch, the parked pipeline), 10c, 10d, U1 and V1 are committed
and have not run. F builds on them. 06 chunk 11 (DL auto) is F2.

**Decided (§10.8–10.14).**
- The row is a dropdown, like an array job's tilt-series list, holding the settings: `Manual · DL`; for DL,
  `Stop for review · Apply automatically`; the model; the threshold when applying automatically. Manual always stops.
  Collapsed, the row says where the filter is.
- Every action lives once, on the page: Run DL, Cancel DL, Clear labels, Approve, Re-open. The dropdown links to the
  page and carries no other action.
- The page: a top panel (the review's state and actions, the DL run, the numbers, the distributions of excluded
  tilts), then the gallery with its own controls row (S/M/L, sort, show, metrics).
- A click on the image zooms, a corner flag labels, and the caption's hover or click shows every metric. Red marks an
  outlier: more than 3 robust SDs worse than the project's tilts in the same 10° |tilt| band.
- `crboost_reingest.py` is deleted entirely (§2).

#### F1 — Approve reads the registry
Files: `services/jobs/tilt_filter.py`.
- `commit_verdict` (`:318`) labels every registry frame with `effective_label(f.id, f.p_bad, …)` (`:286`) instead of
  reading the fs-motion star, so the page, the Tilts tab and the commit read one source. It refuses until fsMotion has
  succeeded; the star's existence was the gate.
- `finalize_pipeline_output` (`:203`) becomes `stamp_verdict(registry, labels)` → `ok(kept, dropped)`: stamp and
  save. The old-slot cleanup stays in `commit_verdict`. A label naming a frame the registry lacks still refuses the
  commit and names the tilts. F2's DL-auto job commits through the same function.
- Counts run over the registry's frames, so a tilt series fsMotion skipped counts as kept (the star left it out).
- *Check (user):* on a project whose alignment has not run, label two tilts bad and Approve: "2 dropped"; Re-open and
  Approve again: the same. Before fsMotion has succeeded, Approve is refused with the reason.
- **Built 2026-10-02** (ruff-clean, not run), as specced, with these refinements:
  - `stamp_verdict` checks every label against the registry before it stamps anything. The old function stamped as it
    went and refused at the end, which left the refused commit's stamps in memory, unsaved, for the next save to write.
  - `commit_verdict` passes the job's human labels for tilts the registry lacks along with the registry frames' labels,
    so the refusal names them, as before.
  - `predictions_for` stays until F5: the panel reads it.

#### F2 — DL auto (roadmap 06 chunk 11)
As specced in roadmap 06 §12 chunk 11, with these additions:
- The driver's `--commit` loads the registry on the node and commits through F1's `stamp_verdict`, then writes
  `commit.json`.
- The mode write moves into the backend (`set_tilt_filter_mode`): it refuses while a prediction run or an auto job is
  in flight, and a switch to DL auto while a hold names this filter submits it with the parked jobs (chunk 11's
  "switch while parked"). The row (now, and F3's dropdown) calls it.
- `tilt_states` (`services/tilt_series/tilt_state.py:113`) shows P(bad) and the model's call in DL auto as in DL
  review.
- Until F3, the row's switch gains a third segment, `DL auto`, and `_review_status`
  (`ui/pipeline_builder/tilt_filter_row.py:58`) the auto states: `Auto · queued`, `running`, `failed` (the reason in
  the tooltip), `done · D of T dropped`.
- *Check (user):* chunk 11's.
- **Built 2026-10-02** (ruff-clean, not run), as specced, with these refinements:
  - `IS_INTERACTIVE` is a property of `TiltFilterParams`. Every read but one is instance-level (the deploy skip, the
    reconcilers' votes and counts, Stop, recovery, the producer pool, the roster, the delete flows); the builder's
    singleton check reads the class and gets the property object, which is truthy.
  - `verdict_labels(registry, job)` builds the labels a commit stamps; Approve and the driver's `--commit` share it.
    `--commit` is appended to the filter's command in `_build_fn_exe`: the filter reaches the chain only in DL auto.
    A run whose predictions cannot be assessed for liveness (no series with two predictions) commits.
  - Approve is refused in DL auto, where the job commits; the panel hides its Approve there.
  - Run DL is refused while the DL-auto job is in flight, and a Run while a prediction run of a DL-auto filter is: each
    job rewrites whole registry sidecars, so the later save would undo the other's predictions or verdict.
  - The reconciler settles a DL-auto job's side effects *before* its status flips, in Pass 1 and Pass 2 alike
    (`_land_tilt_filter_job`): on success `reload_registry` and `last_commit` from `commit.json`; on failure the
    driver's FATAL line into `TiltFilterParams.auto_error` (new), which the row's tooltip shows.
  - Re-open, and leaving DL auto after a failed job, clear the job's SLURM id: the reconciler tracks a non-terminal job
    that has one and would read its old exit marker back.
  - `Segmented` passes a coroutine callback's result on to NiceGUI, which awaits it in the click's slot, so the row's
    switch awaits `set_tilt_filter_mode` and can report a refusal.
  - The job page's header pill reads "Automatic" in DL auto.
  - No schema bump: older code repairs a stored `dl_auto` to `manual` with a load warning.
  - Left for F3: the roster still draws its "single-shot job" hint under a deployed DL-auto filter.

#### F3 — The row: a dropdown with the settings, the parked look, the waiting markers
Files: `ui/pipeline_builder/pipeline_roster.py`, `ui/pipeline_builder/tilt_filter_row.py`, `ui/status_indicator.py`,
`ui/projects_overview.py`, `backend.py`.
- **Chevron.** The tilt filter's row gets the array jobs' chevron (`pipeline_roster.py:531-543`, state in
  `_expanded_instances` `:213`, toggled by `_toggle_ts_expansion` `:688`). Expanded, the line under the row
  (`:610-612`) shows the dropdown; collapsed, nothing. Collapsed by default.
- **The collapsed row** says where the filter is, in mono 9 px where an array job shows its progress (`:480-515`),
  coloured and tooltipped as today's chip (`_review_status`, `_waiting_tip` `:93`): `manual`, `DL review` or
  `DL auto 0.30` before anything ran; `waiting for fsMotion`; `review`; `predicting…`; `47 flagged`; `DL failed`
  (red); `12/697 dropped` once committed; `auto · queued`, `auto · running`, `auto · failed` (red); `· K parked`
  appended while a hold names this filter.
- **The dropdown** is `TiltFilterControls` (`tilt_filter_row.py:112`), still its own FingerprintedView on a 3 s
  tick, with a label column as in `house_field`:
  - `method`: `render_segmented` Manual · DL.
  - `when DL` (DL only): Stop for review · Apply automatically. Manual → DL selects Stop for review.
  - `model` (DL only): `house_select` over conf.yaml's `tilt_filter.models`, with the red marker (the
    `resolve_model` reason) when the model cannot run. It moves here from the page's DL section
    (`ui/tilt_filter_panel.py:223`).
  - `threshold` (Apply automatically only): `house_number` through `numeric_forward`, 0–1 in steps of 0.05,
    "uncalibrated" when conf.yaml records no cut for the model. DL review's threshold stays on the page, where it
    moves the labels live.
  - The state line (today's chip and tooltip) and `Review →` (`panel.switch_tab`).
  - Writes go through F2's `set_tilt_filter_mode`, saved with `force=True` (none is a USER_PARAMS field); an open
    page re-renders (`rerender_job`).
  - No actions: `_actions` (`:168`) and its handlers move to the page (F5).
- **Parked.** A job in `review_hold.parked` (`services/project_state.py:685`) draws an amber ring instead of the
  SCHEDULED dot (`_dot_html` `ui/status_indicator.py:55` gets the variant; `_status_widget` `pipeline_roster.py:225`
  passes it), with the tooltip "Parked: waits for the tilt-filter review since HH:MM; Approve submits it." The
  roster's `signature()` (`:244`) takes the hold. Roadmap 13 splits SCHEDULED into staged and queued from
  `pipeline_active` and `pipeline_order`; parked is a third derived state, drawn the same way.
- **Project-level marker.** Rail: an amber dot on the Jobs button (`:976`) beside its count badge while a hold exists,
  with the tooltip "The pipeline waits for the tilt-filter review: K jobs parked since HH:MM", painted on the rail's
  count tick (`_paint_counts` `:1063`). Landing: a `review` pill (amber) in `ProjectsOverview` (`_STATUS_STYLES`
  `ui/projects_overview.py:66`) when the project file carries a `review_hold` and nothing runs;
  `_derive_live_status` (`backend.py:1330`) reads the hold beside `jobs`. Roadmap 25 restyles the rows later and
  keeps the state.
- *Check (user):*
  1. A new tilt filter's row reads `manual`, collapsed; the chevron opens the settings; DL adds `when DL` and
     `model`; Apply automatically adds `threshold`.
  2. Run with Manual: the row reads `review · K parked`; alignment onward draw amber rings; the rail's Jobs button
     carries the amber dot; the landing lists the project as `review`.
  3. A mode switch while a DL run is in flight is refused with the reason.
- **Built 2026-10-02** (ruff-clean, not run), as specced, with these refinements:
  - **The actions reach the page in F3, not F5**, so no commit lacks Re-open or Cancel DL: `TiltFilterReview` (the
    state line and Run DL / Cancel DL / Approve / Re-open, its own 3 s tick) sits at the top of the old page, whose own
    Approve, Run DL and model select go (`_notify_finalize` with them: nothing called it any more). F5 builds its top
    panel around the same block.
  - `tilt_filter_row.py` holds three views: `TiltFilterStatus` (the words, on its own 3 s tick, so the collapsed row
    moves when a DL run lands with no pipeline running), `TiltFilterControls` (the dropdown) and `TiltFilterReview`.
    `filter_status` is the one derivation of the words and tooltip (`_review_status` plus `· K parked`), shared by
    all three; `status_signature` is their fingerprint.
  - The words before fsMotion has succeeded: the mode's name (`manual`, `DL review`) while fsMotion has not started,
    `waiting for fsMotion` once it has (or failed: the tooltip says so) or while a hold names the filter. Committed
    reads `D/T dropped` whoever committed; the tooltip says Approved or the DL-auto job, at which threshold.
  - The dropdown's signature leaves out the threshold: its box is bound to the job, and a repaint would take the box
    from under the cursor. The model select and the threshold box are disabled while the DL-auto job runs. A model
    pick re-renders the dropdown (the red marker), not the page.
  - The threshold's note: `uncalibrated` (amber) when conf.yaml records no cut for the model, else `cut 0.30` (grey).
  - The chevron reuses `_expanded_instances` / `_toggle_ts_expansion`; the roster's signature takes every chevron's
    state (a collapsed filter's toggle changed nothing it read before) and the hold. The "single-shot job" hint no
    longer draws under the filter.
  - The parked ring's binding captures the project's state when the row is built: the binding runs outside the tab's
    context, where the tab accessor can hand back a blank state.
  - The landing's `review` outranks `failed`, as specced (a hold and nothing running); its tooltip leads with the
    parked count and since when.
  - `ruff format` reflowed `ui/status_indicator.py`'s aligned dicts (formatting only).

#### F4 — The card: the gallery component, metrics, the popover, outliers, the flag; the stopgap goes
Files: `ui/tilt_previews.py`, `services/tilt_series/tilt_state.py`, `ui/tomo_gallery.py`, `ui/dashboard/css.py`;
`crboost_reingest.py` (deleted), `services/tilt_series/registry.py`, `services/dashboard_data.py`,
`docs/roadmaps/roadmap_preprocessing-metrics.md`.
- **`TiltGallery`** (`ui/tilt_previews.py`): the Tilts tab's group code moves into one class that both hosts mount
  (`_render_tilts`, `_render_tilt_group`, `_set_group_open`, `_show_group`, `_expand_all`, `_on_tilt_click`;
  `ui/tomo_gallery.py:679-831`). Groups start collapsed and fill on first expand, one `ui.html` per series with one
  delegated click. It owns the controls row and a review mode that only the job page passes (F5). The Tilts tab
  keeps its toolbar (tabs, S/M/L, Refresh) and puts the gallery's other controls in its tab-controls slot
  (`_render_tab_controls` `:504`).
- **Controls row:**
  - S/M/L (90 / 130 / 190 px, `_TILT_CARD_PX` `ui/tomo_gallery.py:78`), on the job page; the Tilts tab has it in
    its toolbar.
  - `sort` (`house_select`): angle (default), acquisition order, P(bad) worst first where shown, and each chosen
    metric worst first.
  - `show` (`render_segmented`): all · flagged · not in the tomogram · dark · outliers, plus disagree in DL review
    (your label against the model's call). A group with nothing to show is hidden; header counts stay whole-series.
  - `metrics`: toggle chips in the wall's species-chip look (`.cb-gal-sp`).
  - Expand all · Collapse all.
  - Sort and show re-render the open grids only.
- **Metrics** per tilt (`_tilt_metrics` `:253` gains astigmatism): #N, P(bad) where the mode shows it, exposure (% of
  the series' median counts), CTF fit, motion, Δ defocus, astigmatism (fsMotion, or CTF after alignment where it fit,
  as Δ defocus), dose before, alignment shift. The angle is always on; #N and P(bad) are on by default. Every card
  carries every token and the chooser toggles a class on the gallery's root, so CSS shows the chosen tokens and a
  toggle never re-renders. The dark token stays as now, amber, whatever is chosen: it is state.
- **Popover.** A card's caption carries a hidden block with every metric (the value, its band's median and robust
  SD, red when an outlier), `tilt_tip`'s state sentences (`:359`), `DARK_RULE` where dark, and "Not recorded: …".
  A client-side handler on the grid (NiceGUI 3.0.3's `.on(..., js_handler=...)` without a Python handler, so no
  server round trip) copies it into one fixed-position popover per gallery, placed at the caption
  (`getBoundingClientRect`, so a group's `overflow: hidden` cannot clip it). Hover shows it; a caption click pins
  it until the next click elsewhere or Escape. The card's multi-line native title goes; the image keeps a one-line
  title (`+42.0° · tilt 13 · click to view`). Mosaic cells and strip ticks keep their titles.
- **Outliers** (`services/tilt_series/tilt_state.py`, pure): `OUTLIER_ROBUST_SDS = 3.0`, `OUTLIER_BAND_DEG = 10.0`
  and `OUTLIER_MIN_BAND = 10`, each with its reason, and `band_outliers(points, worse)`. Per metric, over every tilt
  of the project that has a value, banded by |stage tilt|: robust SD = 1.4826 × MAD, and a value is red when it lies
  past the band's median by more than 3 robust SDs on its bad side (higher for CTF fit, motion, astigmatism and
  shift; larger |value| for Δ defocus). No rule for the angle, #N and dose; exposure keeps the dark rule (amber) and
  P(bad) the threshold (red, as now). A band with fewer than 10 values, or no spread, marks nothing, and the popover
  says so. The popover's red line names the rule, the band, its median, robust SD and tilt count. The legend gains
  one line: "red number: more than 3 robust SDs worse than the project's tilts at the same |tilt|; a marker, never a
  drop".
- **The flag** (review mode only): a 14 px box in the image's top-right corner (`.cb-tp-flag`), drawn on hover for an
  untouched tilt and always for a labelled one: filled red = your bad, filled slate = your good. A click toggles bad ↔
  good as the page's card click does now (the effective label flips and the tilt counts as labelled). Elsewhere, the
  image zooms and the caption opens the popover. The legend gains "your label: good" (the slate flag).
- **The stopgap goes** (§10.13): `crboost_reingest.py`; `TiltSeriesRegistry.fill_frame_acquisition`
  (`services/tilt_series/registry.py:223`); the commands in the Tilts tab's notes (`_lacking_notes`
  `ui/tomo_gallery.py:709`), which then read, e.g., "4 of 17 tilt series have no mdoc exposure counts recorded, so
  their dark exposures are not marked."; the script's mentions in the registry's schema comment (`:51`),
  `fsm_motion_tracks`' docstring (`services/dashboard_data.py:238`) and `roadmap_preprocessing-metrics.md:33`. V1's
  check step 1 is void.
- *Check (user):*
  1. Tilts tab: its controls row; turning metrics on and off changes every caption at once, with no flicker; sort by
     CTF fit puts the red values first; show · outliers hides the rest.
  2. Hover a caption: the popover lists every metric; on Grid3 a red value names its band, median and the rule; a
     click pins it; a group's edge does not clip it.
  3. No note names a command; `crboost_reingest.py` is gone.
- **Built 2026-10-02** (ruff-clean, not run), as specced, with these refinements:
  - `TiltGallery` takes its data from the host (`set_data(groups, facts)`, from `collect_tilt_groups`) and draws
    in two parts the host places: `render_controls()` (S/M/L only with `size_control=True`, the job page's case)
    and `render(notes)`, the host's own notes (the registry error, previews still being made) above the
    project's. It keeps its open groups, sort, show and chosen metrics across renders and Refreshes.
  - The size switch changes no HTML either: a card grid's column width is the root's `--cb-tp-card`
    (`TILT_CARD_PX`, moved from `tomo_gallery.py`). The Tilts tab's S/M/L re-renders only the wall.
  - Tokens: `#13`, `p0.98`, `exp 52%`, `ctf 7.1Å`, `mot 1.20`, `Δdf +0.31`, `ast 0.12`, `dose 42`, `sh 130Å`.
    A dark tilt shows no `exp` token beside its amber `dark` one. The chooser offers only the metrics some tilt
    has, P(bad) only where it shows; `sort` offers stage tilt, acquisition order, P(bad) where it shows, and each
    chosen metric worst first (turning the sorted metric off falls back to stage tilt).
  - Outliers are judged over every tilt of the project, at collection, per metric: CTF fit, motion, Δ defocus
    (on |Δ|), astigmatism and alignment shift. Astigmatism is |defocus U − V|, from CTF after alignment where it
    fit, else Motion & CTF, as Δ defocus.
  - The popover: the tilt's line, where it is, the review, P(bad), the exposure (and the dark rule), then a
    table of every metric with its band (median, robust SD, tilt count, or why the band does not judge), a red
    line per outlier naming the rule and the cut, and "Not recorded: …". A pinned popover also lets go on a
    scroll: it is fixed-positioned and would stay behind while its caption scrolls away.
  - The legend's outlier line shows when any number is red; the slate flag's line in review mode.
  - `open_tilt_viewer` still reaches the old page's viewer lazily; F5 moves the viewer here.

#### F5 — The job page: a review panel over the shared gallery
Files: `ui/tilt_filter_panel.py` (rewritten), `ui/tilt_previews.py`, `ui/dashboard/figures.py`, `ui/dashboard/css.py`,
`ui/pipeline_builder/tilt_filter_row.py`.
- **Data** as the Tilts tab: `registry_tilt_series` and `tilt_context` on the loop, `collect_tilt_groups` in a
  thread. No fs-motion star, no Warp XMLs: `_find_fs_motion_warp_dir`, `load_tilt_series` and `get_label_summary`
  leave the page, and `get_label_summary` (`services/tilt_series_service.py:217`) goes with its last caller. The page
  observes `predict_run` and the job's status on today's 3 s tick (`_observe` `:283`); a landed run re-collects and
  re-renders the groups, keeping their open state. Previews as the Tilts tab: `ensure_tilt_thumbnails` when none
  exist, and the "N tilts have no preview yet" note with a 15 s poll. The Generate screen (`_render_generate`
  `:324`) goes. Before fsMotion: the Tilts tab's empty state.
- **Top panel**, its blocks separated by whitespace:
  1. *Review:* the state line (shared with the row through `_review_status`) and the actions as `house_button`s:
     Run DL / Run DL again / Cancel DL (DL modes); Clear labels, behind its confirmation (10d); Approve (accent,
     with `notify_resume`; not in DL auto, whose job commits); Re-open, drawn disabled with
     `_verdict_locked_reason` (`services/jobs/tilt_filter.py:310`) as its tooltip once alignment has started. In DL
     review the threshold sits here (`house_number`, live relabel, the "uncalibrated" marker); in DL auto it reads
     "threshold 0.30, set on the job row".
  2. *DL run* (DL modes): a card with the model as chosen on the row, run NNN, its state (the braille spinner while
     queued or running), SLURM id, submitted at and elapsed, then "N predictions · K at or above 0.30" or the failure
     reason; the liveness banner under it when the model is dead. It replaces `_render_dl_config` and
     `_render_predict_status` (`:223`, `:301`).
  3. *Numbers:* `.cb-stats` tiles as in the Journey (`_stat_tiles` `ui/tomo_dashboard_dialog.py:998`): tilts (and
     series), in the tomogram (where alignment ran), flagged (yours · the model's), dropped (committed), dark,
     outliers. In DL review, the agreement line: "the model flags 47 · your labels say 56 bad · both 47 · only yours
     9 · only the model's 0 (untouched tilts count as good)".
  4. *Distributions* (`ui.echart`; builders beside `build_per_tilt_chart` in `ui/dashboard/figures.py`, on its
     `_grid` / `_tooltip` / `_axis` helpers; read the dataviz skill before writing them): tilts out of the tomograms
     or flagged, by |stage tilt| in 10° bands, stacked by cause (dropped by the verdict · flagged by you · flagged by
     the model · left out by Warp's import), with dark tilts in use as an amber segment; in DL modes, the P(bad)
     histogram (20 bins, log counts) stacked by your label (bad · good · untouched), with the threshold as a dashed
     line.
- **Gallery:** `TiltGallery` in review mode under the panel, with its controls row (S/M/L included), the legend and
  the group headers with their strips.
- **Live updates, no re-render.** A flag click writes the job's `tilt_labels` (saved debounced, as now), recomputes
  that series' states and summary, and runs one JavaScript call scoped to the gallery's root element that swaps the
  card's look class, flag class and caption tokens. (The Tilts tab can hold the same tilt in the same page, so the
  call never queries the whole document.) The group's header HTML is replaced, and the tiles and charts update
  (`chart.update()`). A threshold move or Clear labels recomputes every series and sends every changed card in one
  call.
- **What goes** from `ui/tilt_filter_panel.py`: `_hdr`, `_chip`, `_parse_pos_beam`, `_find_ts_ctf_star`,
  `_find_fs_motion_warp_dir`, `_render_dl_config`, `_render_generate`, `_build_gallery`, `_render_gallery_content`,
  `_CARD_CLICK_JS`, `_card_look`, `_js_apply_look`, `_set_bad_count`, `_build_cards_html`, `_render_ts_group` and the
  palette they use. The viewer (`_render_mrc_preview` `:889`, `_show_upsample` `:941`) moves beside
  `open_tilt_viewer` in `ui/tilt_previews.py`, its only other caller, which ends that module's lazy import of the
  panel. `_confirm_clear_labels` and `_notify_finalize` stay.
- *Check (user):*
  1. Grid3 in DL review: the panel shows the state, Run DL again, Clear labels, Approve (refused with the reason:
     alignment has run), the DL card naming its run and model, the tiles and both charts; the gallery below.
  2. A flag click restyles the card at once; its header counts, its strip tick, the tiles and the bar chart follow;
     nothing flickers; a reload keeps the label.
  3. Threshold 0.30 → 0.50: dashed borders and red `p` tokens change in place; the histogram's line moves; the
     agreement line follows.
  4. A project whose alignment has not run: Approve → the dropped cards take red stripes and the row reads
     `D/T dropped`; Re-open → the review again.
  5. A label set on the page shows in the Tilts tab after its Refresh.
- **Built 2026-10-02** (ruff-clean, not run), as specced, with these refinements:
  - The review block is F3's `TiltFilterReview`. Clear labels and the threshold (DL review) sit beside it, outside
    its view: the view repaints on the threshold, and would take the box from under the cursor. Re-open stays
    drawn, disabled, with the reason as its tooltip (`reopen_lock` in `services/jobs/tilt_filter.py`, which
    `reopen_review` uses too). In DL auto the threshold line is bound to the job, so the row's edits show.
  - **The P(bad) histogram's bars stand side by side in each bin, not stacked** (the spec said stacked): on a log
    count axis a stacked segment's length does not encode its count. Left to right: your good, untouched, your
    bad. The log axis starts at 0.6, so a bin of one tilt has height; its labels stay on whole numbers.
  - The band chart's top segment is the dark tilts *kept* (in the tomogram, or to be once alignment runs), so it
    shows before alignment has run; a tilt counts once, under the first of dropped, the model's flag, your flag,
    Warp's import, dark. Both palettes passed the dataviz validator in their stack order (dropped `#e34948`,
    model `#4a3aa7`, yours `#e87ba4`, Warp `#2a78d6`, dark `#eda100`; good `#1baf7a`, untouched `#2a78d6`, bad
    `#e34948`); the contrast warning on the light hues is relieved by the legend and the per-bar hover.
  - The braille spinner no longer exists; the DL card uses the roster's moving dot (`_running_spinner_html`). The
    card shows the latest prediction run; in DL auto without one it says the chain job predicts on Run.
  - Tiles: tilts · series, in the tomogram (over the series alignment ran on), flagged (all · yours · the
    model's), dropped, dark, outliers.
  - `TiltGallery.restate` re-derives a series (`restate_group`), replaces its header and sends the changed cards'
    look, flag and caption in one call scoped by `getHtmlElement(root id)`; the grid element's HTML is updated on
    the server too, so a reconnect cannot bring old looks back. With show · flagged or disagree the series' grid
    re-renders instead, since its membership changes.
  - A flag click records the label in any state; the card's look follows it only while the review is uncommitted,
    as before.
  - Gone with their last caller: `get_label_summary` and `fs_motion_star` (`services/tilt_series_service.py`) and
    `predictions_for` (`services/jobs/tilt_filter.py`); `_notify_finalize` went in F3. The page scrolls in a plain
    overflow box, not `ui.scroll_area`.

**Not in F:** the Journey (V2); labelling in the Tilts tab (V4); label sets (A.6); 06 chunks 12–13; roadmap 22's
blank-exposure exclusion.

### G — one exclusion signal, info as icons, a quieter page (scoped and built 2026-10-04, not run)
The maintainer's review of F in use: DL review run on 8 projects of one dataset, the page read on a 126-series,
5007-tilt project. Decisions §10.15–10.23. Five commits, G1–G5, built back to back, one runtime pass at the end.

**Why.** F's annotations blur information with the action. A card edge can say six things: a solid red border (a
manual bad), a dashed one (the model's call), red stripes (committed), grey stripes (Warp's import), amber (dark), and
red numbers mark outliers. The page top spells the manual / DL split out on every line, and the outlier marker covers a
third of the tilts (§8.9).

**Approve, confirmed (2026-10-04).** At Approve every registry frame takes `effective_label`
(`services/jobs/tilt_filter.py:273`): a manual label, else in the DL modes "bad" at a confidence score at or above the
threshold. `stamp_verdict` (`:227`) writes the same `is_filtered_out` for both and keeps no source. Alignment drops
the stamped frames from its tomostar snapshot (`drivers/ts_alignment.py:188`), tsCtf and reconstruction stage from
that snapshot, and the alignment and CTF star writers skip them (`services/tilt_series/adapters/ts_alignment.py:567`,
`services/tilt_series/adapters/ts_ctf.py:286`). A tilt the model flags is already the same citizen as a manually
labelled one; G removes the visual difference.

**Decided** (§10.15–10.23). Also assumed, stated in the session and not objected to: Warp's left-outs become an
icon, not grey stripes; the amber `dark` caption token goes (the icon carries it); outliers take a violet accent and
the confidence score is never coloured; the metrics chooser becomes a menu of checkboxes and the legend icons only at
the end of the controls row; the DL run card shrinks to one line; a click on an approved filter's card is refused
with a pointer to Re-open; the model select shows only when conf.yaml registers more than one model.

#### G1 — The row: flat settings, Approve labels
Files: `ui/pipeline_builder/tilt_filter_row.py`, `services/jobs/tilt_filter.py` (messages), `drivers/tilt_filter.py`
(messages).
- `TiltFilterControls.render` (`:256`) always shows three lines: `method` (Manual · DL); `when DL` (Stop for review ·
  Apply automatically), drawn disabled in Manual; `threshold` (`house_number`, bound as now), editable only in Apply
  automatically, otherwise disabled with the tooltip "Set on the job page while reviewing". The amber `uncalibrated`
  stays; the grey `cut 0.30` note moves into the tooltip. `model` (`_render_model` `:279`) shows only when
  conf.yaml's `tilt_filter.models` holds more than one entry; with one, its name is in the method switch's tooltip,
  and `resolve_model`'s red marker sits beside the method switch whenever DL is chosen.
- The dropdown's state line and `Review →` go (`:275-277`); the collapsed row's words (`TiltFilterStatus`) are the one
  state readout, and a click on the row opens the page as for any job.
- Words (`_committed_status` `:79`, `_auto_status` `:100`, `_review_status` `:114`): `N flagged` → `N to exclude`,
  `D/T dropped` → `D/T excluded`, "P(bad) ≥ 0.30" → "confidence score ≥ 0.30", "your labels win" → "manual labels
  win".
- `TiltFilterReview._actions` (`:393`): `Approve` → **`Approve labels`**, tooltip "Exclude the N red tilts from
  alignment, CTF and reconstruction, and start the K parked jobs" (N from the review as it stands, K from the hold).
  The notify: "Approved: K kept, D excluded."
- The driver's liveness FATAL (`drivers/tilt_filter.py:207`), which the row shows through `auto_error`, and its commit
  line (`:179`) say "confidence score".
- *Check (user):* a new filter's dropdown shows method, when DL (greyed in Manual) and threshold (greyed unless Apply
  automatically); no Review →; one registered model, no model select; the page's button reads Approve labels and its
  tooltip counts the red tilts and the parked jobs.
- **Built 2026-10-04** (ruff-clean, not run), as specced, with these refinements:
  - The row's `N to exclude` counts what Approve labels would exclude (`_to_exclude`: manual labels and the model's
    calls, by `effective_label`) in either review mode, not only after a DL run; the status fingerprint takes the
    labels, so a toggle on the page moves the row.
  - The greyed `when DL` strip sits in a wrapper that carries its "Only for DL" tooltip (a strip without pointer events
    shows none).
  - The three views' 3 s observers are `owned_timer`s (§8.11).

#### G2 — DL review predicts by itself when the filter is in a Run
Files: `backend.py`, `services/scheduling_and_orchestration/pipeline_orchestrator_service.py`,
`services/scheduling_and_orchestration/pipeline_runner.py`, `filterTilts/image_processor.py`,
`services/jobs/tilt_filter.py`, `ui/pipeline_builder/tilt_filter_row.py`.
- **When.** A filter in DL review that a Run includes, i.e. the run's hold names it (`state.review_hold.barrier`; the
  maintainer's "only if the job is queued"), not committed, no prediction in flight:
  1. at deploy, once the hold is recorded (`_deploy_locked`, `pipeline_orchestrator_service.py:205-246`, both the
     parked-only return and the chain branch), when fsMotion has already succeeded and the filter has no successful
     prediction run;
  2. on fsMotion's SUCCEEDED edge in `reconcile_afterok` (pass 5, `pipeline_runner.py:545`, beside
     `_kickoff_tilt_thumbnails`): fresh averages, fresh predictions, whatever ran before;
  3. on a switch to DL review (`backend.set_tilt_filter_mode`, `backend.py:336`) while a hold names the filter and
     fsMotion has succeeded, with no successful prediction run.

  A filter that only sits in the roster never predicts by itself; Run DL on the page stays for re-runs. One backend
  helper holds the conditions and calls `submit_tilt_filter_predict` (`backend.py:216`); the three sites call it. The
  schemer path parks nothing (it refuses a parked run), so it never auto-predicts.
- **A refused auto-submit is stated, never swallowed.** The reason (no model, weights missing, no fs-motion star) goes
  to `auto_error` (`services/jobs/tilt_filter.py:143`, from now on "why the last automatic DL step failed") and the row
  reads `DL failed`, red, with the reason as its tooltip; `events.warning` logs it.
- **The PNG race.** The thumbnail pass starts on the same fsMotion edge and writes the gallery PNGs the driver reads
  (`drivers/tilt_filter.py:97-119`). `_is_model_input` (`:47`) checks the header only, so a half-written PNG passes.
  `filterTilts/image_processor.py:96` writes each PNG under a temporary name outside the `*.png` glob (with
  `format="PNG"`) and `os.replace`s it, so the driver sees a complete PNG or none and converts the missing ones itself,
  as now.
- *Check (user):* a fresh copia project with the filter in DL · Stop for review, Run: the row reads `waiting for
  fsMotion · K parked`, then `predicting…`, then `N to exclude`, with no click. The same filter in the roster but left
  out of the Run predicts nothing. With the weights file renamed, the row reads `DL failed` with the reason.
- **Built 2026-10-04** (ruff-clean, not run), as specced, with these refinements:
  - The helper is `backend.autostart_tilt_filter_predict(project_path, *, fresh=False)`; the deploy hook runs after
    the chain submit whenever a hold stands (fsMotion already done: at once; else pass 5 does it).
  - A run that starts clears `auto_error` (`submit_tilt_filter_predict`), and so does any mode switch: the reason
    belonged to the mode left behind.
  - The temporary PNG is `<name>.png.part`, saved with `format='PNG'` and `os.replace`d.

#### G3 — The card: one exclusion signal, info as icons, a table on hover
Files: `ui/tilt_previews.py`, `ui/dashboard/css.py`, `services/tilt_series/tilt_state.py`, `ui/tomo_gallery.py`.
- **One exclusion signal.** `_look` (`ui/tilt_previews.py:781`) gives one look or none: excluded = a 1.5 px solid red
  border on a card, a red outline on a mosaic cell. While the review is uncommitted, excluded is what the review marks
  bad (a manual label or the model's call); otherwise it is the committed stamp. The dashed and striped variants, the
  grey stripes and the amber border go (`ui/dashboard/css.py:1287-1301`), and an old commit's stamp no longer shows
  through a review (§8.10).
- **Info icons**, bottom-left inside the image, side by side: 12 px translucent discs (`rgba(15,23,42,0.55)`) with a
  small glyph (inline SVG), each only when it applies:
  - dark exposure: an amber glyph, filled when blank, outlined when dim;
  - not in alignment's output while nothing excludes it (Warp's import left it out): a grey slashed circle;
  - outlier: a violet glyph, for the metrics ticked in the metrics menu only (§10.21).

  Hovering an icon shows the gallery's popover: what the icon means, the tilt's value against the cut, where the
  number comes from (the mdoc's `MinMaxMean`, Warp's XML, alignment's per-tilt output) and "A marker only; it excludes
  nothing." Red is never an information colour: outlier numbers turn violet bold (`.cb-tm-out`, `css.py:1322`), and
  the confidence score is never coloured.
- **Clicks** (`GRID_CLICK_JS` `:120`, `_on_click` `:1445`). In review mode a click on a card toggles exclusion
  (`on_flag` becomes `on_toggle`; the page's handler keeps its logic); a magnifier, 16 px in the image's top-right
  corner and drawn on hover, opens the viewer; a strip tick or an icon never toggles. Approved: a toggle is refused
  with "Approved: Re-open to change labels" (F5 recorded the label silently). The Tilts tab has no review, so its click
  still zooms. The flag goes (`flag_class` `:828`, `.cb-tp-flag` `css.py:1326-1334`).
- **The popover** (`_popover_html` `:715`) becomes a table in one type scale (10 px IBM Plex Sans; numbers in 10 px
  IBM Plex Mono):
  - a title line, `−42.0° · tilt 31`, the frame id muted;
  - a status line: "Excluded: confidence score 0.92, threshold 0.30" / "Excluded: manual label" / "Included: manual
    label, confidence score 0.92" / "Included"; then where it is: "In the tomogram" / "Not in alignment's output
    (Warp's import)" / "Alignment has not run";
  - rows of `metric · value · typical at ±40–50°` (the band's median; violet bold for an outlier): confidence score,
    exposure, CTF fit, motion, defocus vs series median, astigmatism, dose before, alignment shift;
  - one muted line, "Not recorded: …".

  No prose rules inside it: they live in the icon and legend hovers. Full metric names: `Δ defocus` becomes "defocus
  vs series median", `P(bad)` "confidence score".
- **Captions** (`_tokens_html` `:797`): `−42.0°`, `#31`, `score 0.92`, `exposure 3.2 %`, `CTF 7.1 Å`, `motion 1.20`,
  `defocus +0.31 µm`, `astigmatism 0.12 µm`, `dose 42 e⁻/Å²`, `shift 130 Å`. The amber `dark` token goes.
- **Controls row** (`render_controls` `:1165`). The metric chips (`_render_chip` `:1189`) become a `metrics ▾` menu
  of checkboxes, one per offered metric ("Confidence score" with its tooltip, §10.18), toggling through
  `_toggle_metric` (`:1220`) as now. The legend moves to the end of the row as the word `legend` and icons only (a
  red-bordered square and the icons the data holds), each explained on hover; the legend line above the groups goes
  (`legend_html` `:1037`, `render` `:1271-1277`). Show: `flagged` becomes `excluded`.
- **Headers, strips, cells.** Group header words: `N in the tomogram · K excluded · D dark` (`group_header_html`
  `:941`). Strip ticks: red = excluded, grey = not in alignment's output, amber = dark and kept, slate otherwise; the
  red-outline tick goes. Mosaic cells (`mosaic_html` `:884`): the red outline, plus a 4 px amber or grey dot at the
  bottom left.
- **Texts:** `DARK_RULE` and `OUTLIER_RULE` (`:155`, `:162`), `_LEGEND` (`:169`), `where_sentence`, `_review_sentence`,
  `_tick_title`, `_used_title`: no "you / your", no "P(bad)", no `|tilt|` ("at ±40–50°", "the same stage-tilt band").
- *Check (user):*
  1. Job page in DL review with predictions: every tilt the review excludes has the same red border, whether the model
     or a manual label excluded it; no dashes, no stripes. A dark tilt carries an amber icon and a tilt Warp left out
     a grey one; hovering an icon explains it.
  2. A click on a card toggles red ↔ none at once; the magnifier opens the viewer; after Approve a click points to
     Re-open.
  3. A caption's hover: a table in one font, with no `|tilt|`, `Δ`, `sh`, `mot`, `P(bad)` or "you".
  4. Ticking CTF fit in the metrics menu adds its token to every caption, and violet icons where it is an outlier;
     unticking removes both.
  5. The Tilts tab: the same borders and icons; a click zooms.
- **Built 2026-10-04** (ruff-clean, not run), as specced, with these refinements:
  - `TiltState.excluded` (`flagged or drop`) is the one exclusion; `tilt_states` leaves `drop` unset while a review is
    open, and `SeriesSummary` trades `flagged` / `dropped` / `dark_unflagged` for `excluded` / `dark_kept`.
  - The icons and the magnifier are CSS background images (data-URI SVGs in `ui/dashboard/css.py`), so a card carries
    one short span per icon; each icon holds its popover's content as a hidden child, and the caption and the icons
    share one popover handler (`_POPOVER_AT_ANCHOR`).
  - **Not in alignment's output is indigo, not grey**, on the icon, the mosaic dot and the strip tick: G5's charts
    need a validated hue for it (grey fails the palette validator's chroma floor), and the card and the charts keep
    one colour per state.
  - The outlier marks follow the ticked metrics through CSS alone: the icon carries a `cb-to-<metric>` class per
    outlying metric, the popover's rows and values likewise, toggled by the root's `cb-tg-m-<metric>`; nothing
    re-renders on a tick. The legend's outlier icon shows whenever any metric has outliers (the legend sits outside
    the root); its hover says only ticked metrics are marked.
  - A mosaic cell outlines what the committed verdict excluded only (what went into the tomogram), with the dots.
  - The image's native title goes; the magnifier carries "Open full size". `show · outliers` counts ticked metrics.

#### G4 — Group by position
Files: `ui/tilt_previews.py`, `ui/dashboard/css.py`.
- A `group: series · position` switch in the controls row; series (today's view) is the default.
- Position mode: one box per position, keyed by the stage number of `position_label`
  (`services/dashboard_data.py:146`); a series whose name carries none is a box of its own. The header holds the
  chevron, `Pos 13` and `4 beams · 160 tilts · 12 excluded · 9 dark`. Expanded, each beam is a slim line (`Beam 1`, the
  series id muted, its header words and strip) followed by its cards, all flush with the box's left edge: no nested
  boxes, no indent. One `ui.html` per position, filled on first expand, with one delegated click.
- Show hides the beams, then the positions, with nothing to show; sort applies within a beam; Expand / Collapse all
  act on positions; the open state is kept per mode.
- Live updates: a beam line carries `data-ts`, and `restate` patches it in the same JavaScript call as the cards
  (`_PATCH_JS` `:1464`).
- *Check (user):* on a multi-beam dataset, group · position shows one box per position; Pos 13 opens on Beam 1's line
  and cards, then Beam 2's, flush left. show · excluded hides the positions with nothing excluded; a toggle updates its
  beam line's counts.
- **Built 2026-10-04** (ruff-clean, not run), as specced. A grouping switch rebuilds the gallery's list (collapsed,
  but for what was open in that mode); a toggle also replaces the position's header words, and with show · excluded or
  disagree re-renders the open position.

#### G5 — The job page: stats first, actions, charts tucked away
Files: `ui/tilt_filter_panel.py`, `ui/dashboard/figures.py`, `ui/dashboard/css.py`, `services/dashboard_data.py`,
`ui/tomo_dashboard_dialog.py`.
- **Order:** the stats, the actions line, `charts ▸` (collapsed), the gallery. The top section's text drops a notch:
  the stats in `.cb-stats-inline` (9 px labels, 10 px values, `css.py:588`), the state words at 10 px (11 today,
  `ui/pipeline_builder/tilt_filter_row.py:382`), chart text at 9 px.
- **Stats** (`_render_numbers` `:200`), the same in every mode: `tilts`, `series`, `to exclude` (`excluded` once
  approved), `in tomograms` (alignment's output, where it has run), `dark`, `outliers`. The manual / DL split lives in
  the hovers only: `to exclude` → "346 by confidence score ≥ 0.30 · 0 by manual label · 2 manual labels keep a tilt the
  score would exclude". `outliers` counts tilts with an outlier in a ticked metric ("—" while none is ticked), so the
  page agrees with the cards; its hover gives the count per metric over every metric (§8.9). The agreement line
  (`_agreement_text` `:394`) moves into the charts.
- **Actions line:** the state words, the threshold (DL review), Run DL / Cancel DL, Clear labels, Approve labels,
  Re-open. The DL run card (`_DlRunCard` `:461`) shrinks to one muted line after them (`DL run 003 · done 14:02`;
  while running, the moving dot and the minutes), with the model, SLURM id, submit time, the tilts scored and how many
  sit at or above the threshold, or a failure's reason, in its tooltip. The liveness warning stays a red line.
- **Charts**, collapsed by default (`ui/dashboard/figures.py`; load the dataviz skill first): small multiples of one
  height (about 120 px), one grid, axis and tooltip style, linear tilt counts, colours from the card vocabulary (red
  excluded, grey not in alignment's output, amber dark):
  1. *Excluded by stage tilt*: signed 10° bins (−70…+70), stacked excluded · not in alignment's output · dark and
     kept. Replaces `build_tilt_band_chart`'s cause split (`TILT_CAUSES` `:780`: no model / manual split).
  2. *Confidence score* (DL modes): 20 bins, stacked excluded · kept by the effective label (so manual overrides
     show), the threshold as a dashed line; when one bin outnumbers the next-largest more than 5×, the axis tops out at
     1.2× the next-largest and the clipped bar prints its count. Replaces the log histogram (`build_p_bad_histogram`
     `:871`, `P_BAD_LABELS` `:788`).
  3. *Excluded per tilt series*: one thin bar per series in position order, excluded · not in alignment's output
     stacked; the hover names the series.
  4. *Manual labels vs DL calls* (only when both exist): a 2 × 3 count table, rows DL exclude / keep, columns manual
     exclude / keep / none.
- **Toggles** (`_on_flag` `:324`): refused once approved (G3), otherwise as now: flip the effective label, save
  debounced, restate.
- **The Journey's words:** the hover suffix `(P(bad) 0.83)` (`services/dashboard_data.py:379`) → `(confidence 0.83)`;
  `threshold on P(bad)` (`ui/tomo_dashboard_dialog.py:1851`) → `confidence threshold`.
- *Check (user):*
  1. The 126-series project: the stats on one line at the top; the `to exclude` hover splits the sources; `outliers`
     reads "—" until a metric is ticked, and its hover names the metric behind the 1555 (§8.9).
  2. `charts ▸` opens four small blocks with matching axes; the score histogram's clipped bin prints its count;
     threshold 0.30 → 0.50 moves the line and the excluded / kept split.
  3. No "P(bad)", "you" or "your" anywhere on the page, the row or the Tilts tab.
- **Built 2026-10-04** (ruff-clean, not run), as specced, with these refinements:
  - The charts share one height (`SMALL_CHART_PX` = 130) and one legend line above them; their palette passed the
    validator as a set of four, every pair: excluded `#e34948`, not in alignment's output `#4a3aa7`, dark kept
    `#eda100`, kept `#2a78d6` (the amber's contrast warning is relieved by the legend and the hover counts). The charts
    are built on the first open and resized on a re-open.
  - The DL run's line also carries the dead-model warning, in red, instead of a banner of its own.
  - The page's timers are `owned_timer`s (§8.11).

**Not in G:** fixing the outlier rule itself (§8.9, after G5's breakdown); labelling in the Tilts tab (V4); the
Journey's charts (V2).

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
tomogram · flagged · disagree · dark`. The switches live in the Tilts view, which reviews from V4 on (§10.7).
**Mostly built by F (2026-10-02):** F4's controls row gives both hosts the sort, the show switch (with outliers) and a
metric chooser, which replaces the single metric slot; F5 puts 6.5's charts on the job page's panel. What is left is
6.5 at the top of the Tilts tab, decided with V4.

### V4 — one gallery (decided: after V2)
The Tilts tab already has the cards, the state, the viewer and (with V2) the routes. A click that labels there, as in
the filter gallery, leaves the filter panel its DL section and Approve, and answers A.3's "one home per action":
labelling in Tilts; Approve and Run DL on the job row. Scheduled after V2 (§10.7). Label sets (A.6) are designed with
it. **After F:** both hosts mount one `TiltGallery`, so V4 is the Tilts tab passing the review mode (the flag). The
actions stay on the job page's panel (§10.9), not on the row; whether the page keeps its own gallery then is decided
with V4.

## 10. Decisions (maintainer, 2026-10-02)
1. **Tilt number:** `tilt_index + 1` everywhere.
2. **"In the tomogram"** = in the alignment output.
3. **Dark exposure:** blank under 1 % and dim under 10 % of the series' median mdoc counts, constants shared with
   roadmap 22; shown here, never excluding here. The maintainer is wary of dropping anything by a rule: every surface
   that marks a dark tilt says what the rule is and that it drops nothing (V1's tooltips and legend line). 06 chunk 13
   restates §7.3's 595 of 599 against the median.
4. **Backfill:** `crboost_reingest.py --mdoc`, run by hand; the registry does not fill itself on load. Amended the
   same day: a prototyping stopgap for registries written by older code, removed once test data is re-run through
   the current code (§2); users are never pointed at a command.
5. **P(bad) on Tilts cards:** in DL review only, as the panel. With F2, in DL auto too.
6. **The strips are the project's tilt scheme**; no separate chart.
7. **V4:** yes, the review moves into the Tilts view, after V2. V3's switches belong there.

Stage F (maintainer, 2026-10-02; the answers to F's questions and forks):

8. **The row is a dropdown** (the array jobs' chevron) holding the settings: Manual, which always stops the pipeline,
   or DL; for DL, stop for review or apply automatically, with the threshold for the latter; the model. Collapsed,
   the row says where the filter is.
9. **Every action lives on the job page only:** Run DL, Cancel DL, Clear labels, Approve, Re-open. The dropdown links
   to the page.
10. **The page** is a top panel (the review's state and actions, the DL run "dressed up", the numbers, the
    distributions of excluded tilts) over the shared gallery with its own controls row: S/M/L, sort, show filters and
    a chooser of the metrics every caption shows.
11. **Clicks:** a click on the image zooms; a corner flag labels (red = your bad, slate = your good, which also marks
    "your label: good"); the caption's hover or click reveals every metric.
12. **Outliers are red:** more than 3 robust SDs (1.4826 × MAD) worse than the project's tilts in the same 10° |tilt|
    band. A marker, never a drop.
13. **`crboost_reingest.py` is deleted entirely**, the 1.4 QC re-ingest with the mdoc backfill.
14. **DL auto (06 chunk 11) is built in F** as F2, since the row offers it (in the plan of 2026-10-02, not objected
    to). Approve reads the registry first (F1), so the DL-auto job commits through the same function.

Stage G (maintainer, 2026-10-04; the review of F in use and the answers to four questions):

15. **Approve labels.** A tilt the model flags and a manually labelled one are the same citizen: the commit stamps
    both identically (§9 G, "Approve, confirmed") and the UI shows them identically. The button reads "Approve labels"
    and means only this: exclude every red tilt from alignment onward and start the parked jobs. Manual and DL · Stop
    for review never advance without it; DL · Apply automatically never waits for it.
16. **Action versus information.** The border is the action: red = excluded (the model or a manual label, pending or
    approved), none = included. Whatever only informs (dark exposure, not in alignment's output, metric outliers) is a
    small translucent icon at the image's bottom left, several side by side, explained on hover (meaning, value,
    source). Red marks exclusion only.
17. **Clicks** (supersedes 11): in the review a click on the image toggles exclusion; a magnifier in the top-right
    corner, drawn on hover, zooms; the caption's hover shows a table and a click pins it.
18. **Words.** "P(bad)" becomes **"Confidence score"**, with the tooltip "Confidence that this particular tilt should
    be excluded" (the maintainer's wording). No "you / your" anywhere: "manual label". No shorthand in a table or a
    tooltip (`|tilt|`, `Δdf`, `sh`, `mot`): full metric names, one type scale.
19. **DL · Stop for review predicts by itself, only when the filter is in a Run** ("only if the job is queued"): after
    fsMotion, or at once when fsMotion has run. A filter that only sits in the roster never does. Supersedes roadmap
    06 rev 4's "on request" for this case; Run DL stays for re-runs.
20. **Group by position:** one box per position, its beams as slim lines each followed by its cards at full width,
    never indented; in addition to the per-series view, not instead of it.
21. **Outliers mark only the metrics ticked in the metrics menu** (none by default) until §8.9 is explained.
22. **The page:** stats first and the same in every mode, the manual / DL split in hovers only; charts small, in one
    style, on linear tilt counts, collapsed by default; the top section's text one notch smaller.
23. **The row:** method and "when DL" always shown; the threshold always shown, editable only for Apply automatically;
    no Review → button.

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

# Roadmap 30 — Particle-stage analytics, controls and set tools

**Status:** scoped 2026-10-10; approved for build the same day with the decisions in §10; nothing built yet.
Origin: the AGG412 campaign (roadmap 29). Every particle question
there was answered with throwaway scripts (`cutoff.js`, `tmpeek.js`, `ribohealth.js`, the σ ladders, the cap
survey). This roadmap moves those answers into the app, adds the statistical controls the scripts lacked, and
gives users the tools to cut, split and merge particle sets. It absorbs roadmap 29's C3 (σ subset) and C4
(class subset), and the "collate-filters" successor named in `roadmap_particles-ux-peeves.md`. The particle
readouts already catalogued in `docs/preprocessing-metrics-inventory.md` §3.4 are its raw material.

## 1. Goal

A user with a fresh multi-project dataset (test case: AGG412, 8 projects, 867 tomograms, two species) can do
everything below in the app, without scripts:

1. **TM health:** see whether template matching ran properly on every tomogram, and which tomograms are odd.
2. **Where signal stops:** see, with evidence, the score below which candidates are mostly background.
3. **Content or quality:** tell whether a tomogram with few picks is empty or just bad data.
4. **Make the subset:** apply a σ cut, a class choice, a depth band or a tomogram exclusion; see the count
   funnel; and feed the result downstream as an optimisation set.
5. **Compare:** split a set (halves, conditions, projects, σ bins, A/B arms) and merge sets across projects.

The tilt stage already has this kind of at-a-glance view: the Journey's per-tilt sections, the PS1D panel,
the tilt-filter histogram with its threshold, and review/approve. The particle stage has almost none.

## 2. Score unit: σ

PyTOM correlates the template with the tomogram at every voxel and every rotation (local cross-correlation,
LCC). It records the spread of those raw values, σ, which is the noise level of one correlation in that
tomogram. σ lives in `job.json` and in each candidate's `rlnSearchStd`.

Raw LCC is not comparable between tomograms or templates. In AGG412, σ is 0.007–0.009 for the 412 template
against 0.032–0.038 for the ribosome, and it varies with thickness and contrast. **LCC ÷ σ** is: 9σ means
the same thing everywhere.

PyTOM's "false positives = 1" cutoff (7.1σ for the 412 search) is the score that pure Gaussian noise would
beat once per tomogram. Real background is not Gaussian noise: membranes, edges, granules and gold correlate
with templates. On the AGG412 pilot, background reached 9.6σ, against ~13 peaks per tomogram above 7.1σ where
the formula expects 1. **The cutoff is a gate, not an error rate. The real line has to be measured, which is
what §5 is for.**

## 3. What exists (inventory 2026-10-10)

- **Shown today:**
  - pick positions on slabs, per-list counts (kept/total) and the LCC min–max–mean in a tooltip;
  - cutout galleries sorted by score, display filters, and the H1–H4 / L1–L4 calibration anchors;
  - keep/drop curation (box and lasso), with per-pick score, z-percentile and nearest-neighbour distance on
    hover;
  - a plain 30-bin LCC histogram, only in the no-atlas fallback;
  - pick-count dots in the Journey strip, and the pixel-chain sanity table.
- **Parsed:**
  - `rlnLCCmax`, through `preview_orchestrator.SCORE_COL_PRIORITY`;
  - empty `*_particles.star` files, read as "ran, nothing above cutoff".
- **Not read anywhere:** `rlnSearchStd`, `rlnCutOff`, `job.json` stats, score maps, Class3D `model.star` /
  `data.star`, half-map FSC.
- **Set tools:**
  - keep/drop filtered lists (`services/particles/picks_filter.py`);
  - priority-union merge plus radius dedup (`pick_merge.py`, `coord_merge.py`);
  - extracted-set merge (`subtomo_merge.py`, Aggregation merge card);
  - per-list extraction;
  - ArtiaX curation;
  - the species catalog.
  `PickListType.FILTERED` ("derived from a parent list") exists, but nothing creates it.
- **Missing:** σ anywhere in the UI; score or σ subsets; top-N or per-tomogram caps; splits; class subsets;
  any Class3D or ReconstructParticle readout; any control.

## 4. Principles

- **σ is the score users see.** LCC moves to the tooltip.
- **Rules mark; they never drop.** A filter makes a new set with its parent and recipe recorded; the parent
  is untouched. Flags (cap bound, edge pick, σ outlier) are markers that a human acts on.
- **Heavy numbers come from drivers.** Score maps are 2 GB each. Whatever can be computed when a job runs and
  never needs recomputing (σ, cutoff, peaks, ladders, slab fits) is computed on the compute node by that job's
  driver and written to a small sidecar. The UI reads only sidecars and stars; a backfill reader covers jobs
  that finished before the sidecars existed, from `job.json` and star columns. Anything that depends on a
  user choice (the cut, a class pick) is computed on demand from those records, never stored as truth.
- **Data layer apart from the representation.** Every analytic is a record with its provenance: source files
  (path + mtime), the job and instance that produced them, the code that computed it (module + schema
  version), and when. Every derived set is a recipe (parent + operation + parameters) with the same
  provenance, and every set operation is logged as an event. The UI only renders records. Changing a chart,
  moving a panel or replacing the Journey must never require re-wiring or recomputing, and nothing is lost when
  a view goes away.
- **Not in the Journey.** The Journey's particle side (cutout galleries streamed per pick) is already slow and
  cluttered at 500–1,000 picks and will be revamped. New features live in a Species **Analytics** tab and
  read the same records the revamped Journey will; at most the Journey links to it. One style for the tab, using
  the house controls and `ui/dashboard/figures.py`, so a later Journey revamp can reuse its parts.
- **Volumes are images, not 3D.** Class and reconstruction maps are shown as central slices plus "open in
  ChimeraX / 3dmod".
- **Stats first, small charts.** Discrete points get no connecting lines.

## 5. Analytics, by stage

### A. TM health (per tomogram, plus a project roll-up)

- **Columns:** σ, search space, the FP=1 cutoff, top peak in σ, number of distinct peaks above the cutoff,
  runtime and node.
- **Markers:**
  - σ far from the project median (thickness or contrast outlier);
  - top peak within 1σ of the cutoff (nothing found);
  - runtime outliers per node family.
- **Prototype:** `tmpeek.js` / `cutoff.js`.

### B. Candidate set

- **σ ladder:** counts ≥ 7, 8, 9, 10, 12σ, per tomogram and pooled.
- **Survival curve:** count ≥ z against z, with the Gaussian expectation N·Q(z) drawn over it (free), plus
  the decoy curve when one exists (§6 K2). The gap between the curves is the signal.
- **σ histogram:** with the cutoff line and the chosen-cut line.
- **Cap report:** did the cap bind, the σ of the last pick, and a flag when all capped picks sit above the
  cut. AGG412 had six such tomograms: a dense field or an artefact.
- **Spatial:**
  - lamella slab fit (thickness, tilt);
  - pick depth and score against depth (real signal peaks mid-slab, artefacts at surfaces);
  - edge picks;
  - nearest-neighbour distances (duplicates);
  - cross-species overlap (one position claimed by two species).
  Prototype: `ribohealth.js`.
- **Yield against data quality:** picks ≥ cut per tomogram plotted against the tilt metrics the Journey
  already has (CTF resolution, tilts used, blank tilts). Low yield with normal quality means low content.
  Shown as a scatter with tomogram hovers.

### C. Subtomo

- Extracted vs candidate counts (out-of-bounds losses).
- Histogram of visible-frame counts per particle (`rlnTomoVisibleFrames`).

### D. Class3D and ReconstructParticle (results panel on the job page)

- **Class3D:**
  - class distribution per iteration (convergence);
  - class × σ-bin table;
  - class × tomogram and class × project tables (a class concentrated in a few tomograms is a batch
    artefact);
  - member gallery per class, best and worst;
  - central slices of each class map.
- **ReconstructParticle:** central slices, and an FSC computed from the half maps it already writes.

### E. Funnel (Species page)

TM tomograms → candidates → ≥ cut → curated → class-kept → refined. One row per project per species, and each
number links to its set.

## 6. Controls

These are measured on the user's own data rather than taken from the Gaussian formula. They are where crboost
can do better than a script.

| # | Control | What it answers | Cost |
|---|---|---|---|
| K1 | Gaussian null curve N·Q(z) next to the observed survival curve | how far the real background exceeds the noise model | free |
| K2 | **Decoy template.** TM with a phase-randomised template (same amplitude spectrum and mask, no shape) on a subset of tomograms | empirical background in σ → an FDR estimate per σ cut: decoy ≥ z ÷ target ≥ z | one TM on ~10 % of tomograms |
| K2′ | PyTOM's random-phase correction (the TM job's "random phase" toggle, off in AGG412), which subtracts a phase-randomised run's map during TM | whether built-in suppression of structured background beats a σ cut | TM time roughly ×2 (to be measured) |
| K3 | **Random-position decoys.** Extract N random in-slab positions, kept ≥ Ø from any pick, and tag them; send them through Class3D with the real candidates | the classifier's false-acceptance rate: the share of decoys landing in the "good" classes | one subtomo pass on N extra positions |
| K4 | **σ-bin reconstructions.** ReconstructParticle per σ bin with the TM angles, plus one from random positions using the angle map's angles | where the structure fades; and template bias: if the random positions rebuild the template, scores at that level mean nothing | minutes per bin |
| K5 | **Recall against a reference list** (manual picks, a previous run), matched within a radius | recall and precision at each σ cut (e.g. AGG412's SK manual picks) | free |
| K6 | **Half-set independence.** No two particles closer than Ø in opposite halves; or halves split by tomogram | FSC inflated by duplicates | free |
| K7 | Mirror template (hand) | the absolute hand of the chain (roadmap 29 R1) | one TM |
| K8 | Seed repeat of Class3D | stability of class membership (overlap between runs) | one Class3D |

## 7. Set tools

| # | Tool | Notes |
|---|---|---|
| S1 | **Rule filter → FILTERED set.** σ ≥ x, LCC ≥ x, top-N per tomogram, depth band, distance from the volume faces, NN ≥ d, tomogram include/exclude, away from another species' picks | Live count preview before commit. If the parent is extracted, the output is a row subset of its optimisation set and needs no re-extraction (roadmap 29 C3). |
| S2 | **Class subset → set.** Choose classes from a Class3D iteration | Class table alongside: counts, σ mix, tomogram spread (roadmap 29 C4). |
| S3 | **Split.** Random halves; by tomogram; by project; by σ bin; by class | Writes sibling sets. Condition tags (milled/unmilled, grid, date) deferred (decision 5). |
| S4 | **Merge across projects** | Exists for extracted sets (merge card); check optics groups and pixel-size agreement on 8 projects. |
| S5 | **Set algebra.** A − B; A ∩ B by proximity | Reuses the clash statistics from `pick_merge`. |
| S6 | **Boundary curation.** Gallery of the picks nearest the cut (sorted by \|σ − cut\|) | Puts human time where the decision is uncertain; the labels feed K5's precision estimate at the cut. A learned scorer over σ, depth and NN is parked. |
| S7 | **Lineage.** A set tree with counts and recipes | The funnel (§5 E) reads from it; a recipe can be replayed, or stored in a protocol. |

## 8. Where it shows

- **Journey:** nothing new for now (decision 4); a link to the tomogram's row in the Analytics tab at most.
- **Species page, new Analytics tab (per project, per species):**
  - TM-health table;
  - pooled survival curve with the null and decoy curves;
  - yield-against-quality scatter;
  - funnel;
  - a σ-cut slider with live counts and "Make subset";
  - the markers (cap bound, σ outlier, surface-heavy, no peak) as a sortable list.
- **Job pages:** a results panel for Class3D and ReconstructParticle (§5 D), with the class-subset action.

## 9. Stages

| # | Stage | Success on AGG412 |
|---|---|---|
| 0 | Gather: `rlnSearchStd` / `rlnCutOff` present in every AGG412 candidates star; `job.json` fields; the sidecar format; the cost of K2′ on two pilot tomograms | a table of what each job dir holds |
| 1 | σ first-class: analytics records with provenance (driver sidecars + backfill); the Species Analytics tab with the TM-health table, σ ladder, histogram, survival curve with K1, and cap flags; σ in the existing score tooltips | numbers match the 2026-10-08 survey (412 ≥ 10σ: 929 in 255 tomograms; six all-≥ 9σ capped tomograms) |
| 2 | S1 filters → FILTERED sets as optimisation sets, plus the funnel | a ≥ 9σ 412 set per project without re-extraction; Class3D accepts it |
| 3 | Spatial QC and yield against quality | reproduces `ribohealth.js` on the pilot (slab fit, mid-slab score peak, NN median ~280 Å) |
| 4 | Class3D / ReconstructParticle readouts; S2; class × σ table; half-map FSC | roadmap 29 R1's class-by-σ table comes from the app |
| 5 | Controls K2–K6 | an FDR-at-cut number for the ribosome pilot; decoy share in the good classes |
| 6 | S3 splits, S5 algebra, S6 boundary curation, S7 lineage | the miss-alignment A/B particle sets (roadmap 29 §6) built in the app |

Build order (2026-10-10): stages 1–2, then 3–6. Re-homing (moving a set between alignments, once S8) is dropped: picks are never moved between alignments; refined tomograms are re-picked by template matching. Stages 1–2 serve roadmap 29 directly. The Warp export job is built separately by the maintainer (roadmap 24 stage 4).

## 10. Decisions (maintainer, 2026-10-10)

1. **σ is the default score** in every view; LCC in the tooltip.
2. **Drivers compute** whatever can be computed when the job runs and never needs recomputing, into
   sidecars; jobs that finished earlier get a backfill reader.
3. **Decoy control:** left to the build. K2 (separate decoy TM on a subset) is the default, because it leaves
   the scores alone and gives an FDR; K2′ stays a TM toggle the user can try.
4. **A separate Species Analytics tab.** The Journey gets a revamp later that will either link here or reuse
   these parts; nothing new goes into it now.
5. **Condition tags: not now.** Too early and too experimental.
6. **Cross-project views come with merged sets:** when a set spans projects (an Aggregation merge), its
   analytics show per-project breakdowns. No multi-project page.

The maintainer will comment on the UI/UX once stage 1 is visible; the Journey's particle side is known to be
laggy and cluttered at 500–1,000 picks (§4).

**Modern-Python weave-in:** statistics as pure functions over star DataFrames in `services/particles/`
(`analytics.py`, `set_ops.py`), sidecar records as frozen dataclasses, and set recipes as Pydantic models
stored on `PickList`.

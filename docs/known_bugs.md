# Known bugs and pitfalls

Things that have bitten more than once. One entry per problem: how it shows, where to look, why, what to do,
and whether the code still lets it happen. Add the project path and date each time it is seen again, so the
frequency is visible. Fixed entries stay, marked FIXED with the commit, until nobody remembers the symptom.

---

## 1. Warp per-tilt CTF fits diverge on low-dose tilts (fsMotionAndCtf `fs_ctf`, tsCtf `ts_ctf`)

**Status:** ROOT CAUSE FOUND 2026-09-08 (experiment `<project>/tutorial_sta/ctf_fit_test.sbatch`; full log and
decision tree in `docs/copia-ctf-fit-investigation.md`): the frame-series fit band `c_range_min_max 30:6` is too greedy for
2 e/Å² tilts; `30:10` fits all 28 images at 5.10–5.53 µm. Protocol fixed; driver-side guard still open. The first
workaround tried, a tight defocus window (`4.0:6.5`), made it WORSE: see below.
**Seen:** `copia-empiar12580-20260906-1319` (17 of 28 tilts wrong), `copia-empiar12580-20260908-1407` (22 of 28);
also in v1 with Warp `2.0.0dev31`: `/groups/klumpe/user/sven.klumpe/Processing/Copia/try2` (Position_11_2, 1.55 Å/px,
3.2 e/Å² per tilt, same `30:6.0`/`1.1:8`): the 7 last-acquired tilts (|tilt| ≥ 27°) at 1.1–2.2 µm instead of ~5,
carried unchanged into `ts_ctf`; only 7 of 39, so it still refined to 9.8 Å. Severity scales with per-tilt SNR.
**Fix verified:** `copia-empiar12580-20260908-1447` (protocol with `30:10`): all 28 tilts 5.15–5.58 µm, CTF fit resolution
6.1–7.6 Å on every tilt, `ts_defocus_hand --check` −0.994 (a real verdict for the first time; = tutorial's flip).

**Symptom.** STA stalls at 15–20 Å although RELION reports angular accuracy < 1°.
`WarpTools ts_defocus_hand --check` reports a correlation near 0 (copia: −0.035, +0.159), i.e. it cannot tell
the handedness. Both are downstream of the same defect.

**Where to look.** Per-tilt `_rlnDefocusU` in `External/<tsCtf job>/tilt_series/<ts>.star`, the `<GridCTF>`
nodes in `External/<tsCtf job>/warp_tiltseries/<ts>.xml`, or `defocus_u_angstrom` in
`registry/tilt_series/<ts>.json`. One tilt series must show one defocus ± ~0.5 µm. Quick look (Warp's export,
columns 3 and 10):

    awk '!/^_|^#|^$|^data|^loop/{printf "%6s %8.0f\n", $3, $10}' External/job005/tilt_series/*.star | sort -n

Copia, window `1.1:8` (v1 default): six lowest-dose tilts at 5.3 µm, the rest 1.3–4.2 µm. Window `4.0:6.5`:
the same six at 5.3 µm, the rest anywhere from −18 to +60 µm, and the frame-series fits of the same images
carry astigmatism of 25–64 µm. So the window only bounds Warp's initial grid search; the subsequent per-tilt
(and per-quadrant, `c_grid 2x2x1`) refinement is unbounded and runs away on tilts with weak Thon rings.
`_rlnCtfMaxResolution` / `_rlnCtfFigureOfMerit` in the star are Warp placeholders (1e-6 / None), never evidence.

**What the data actually contain.** A 1D correlation scan of Warp's own `<TiltPS1D>` curves (rotationally
averaged, geometry-corrected power spectra per tilt) against CTF² finds 5.05–5.53 µm on all 28 tilts with
correlation 0.5–0.9, in every band from 30–12 Å to 30–6 Å. The rings are there; the 2D fit fails anyway.
The mdoc `Defocus` readout (−10.4 µm ± 0.5) is not the true defocus but is flat, consistent with this.

**Cause (established by the experiment).** (a) Fit band `30:6` Å at 2.95 Å/px: beyond ~10 Å most tilts are noise,
and that part of the band holds two thirds of the Fourier pixels, so the objective is noise-dominated on
weak tilts (WarpTools' own default `--range_high` is 4 Å, so this is not exotic). Standalone `fs_ctf` with `30:10`
fits every image (5.10–5.53 µm, astigmatism ≤ 0.09 µm). And `ts_ctf` does not search at all: it starts from the
frame-series defoci that `ts_import` copied into the tilt-series XML and refines them locally (five variants,
any hand, any band, reproduced job005's values within 0.1 µm in ~20 s). Ruled out: (b) `ts_ctf` only: the
driver runs `ts_defocus_hand --check` + `--set_flip` BEFORE `ts_ctf` (same order as v1 and as Warp's quick
start); the check reads the `2x2x1` frame-series defocus gradients, which were garbage, so its verdict was
noise, and `ts_ctf --auto_hand` documents that the fit is done "with the correct handedness", i.e. the hand
is an input to the fit — but the experiment showed the hand does not move the fit. Not the cause: motion (frame shifts of a few Å), illumination, EER grouping (32).
Why the v1 tutorial run on the same data did not fail is unexplained (Warp `2.0.0dev36` here vs `dev29/31`).

**Consequence.** `relion_tomo_subtomo` bakes the per-tilt defoci into the CTF-premultiplied 2D stacks: a 2 µm
error flips the CTF phase beyond ~20 Å, and those tilts cancel signal in every later refinement. RELION's
tomo CTF refinement cannot rescue it (search range ±3000–6000 Å, errors up to 40 000 Å). Everything from
extraction on must be re-run once the fits are right.

**What the code should do (open).** Scoped as `docs/roadmaps/18-ctf-fit-outlier-check.md` (2026-09-09): an
independent 1D defocus estimate per tilt from Warp's own `PS1D` / `TiltPS1D`, with the scan window and band
read from the same XML (no dataset numbers in the check), a `diverged | unverifiable | ok` verdict per tilt in
the registry, Journey and job tabs, job failure when a quarter of the series diverged, and the hand verdict
marked unreliable on diverged fits. Projects A / C / Sven's try2 are its regression test. Do not ship
`1.1:8` / `30:6` as dataset-independent defaults.

---

## 2. `healpix_order` description in `services/jobs/class3d.py` is one order off from the RELION GUI / tutorials

**Status:** OPEN. Cosmetic in code, but it produced a wrong protocol value.
**Seen:** `config/protocols/copia-empiar12580/protocol.yaml` (`healpix_order: 4  # 3.75 deg`, 2026-09-07).

The field description says `2=15deg, 3=7.5deg, 4=3.75deg`. Those are the base HEALPix steps. Every value quoted
by the RELION GUI, its docs and tutorials is the *oversampled* step, because the GUI always adds
`--oversampling 1`: GUI 3.75° is `--healpix_order 3`, GUI 1.8° is `--healpix_order 4` (check
`_rlnPsiStep` in `run_it*_sampling.star`: 3.75 at order 4). Same convention for `offset_step`: GUI step 1 px is
`--offset_step 2`. Anyone translating a tutorial into a protocol picks one order too fine. Fix: rewrite the
description and the UI label in GUI terms (order + 1 at oversampling 1); correct the copia protocol comment.

---

## 3. Long tilt-series names break AreTomo's IMOD output; Warp drops the series and the pipeline runs on unaligned angles

**Status:** DRIVER GUARD + CREATION WARNING added 2026-09-09 (`drivers/ts_alignment.py` `warp_marked_unselected`,
`services/protocols/apply.py` `long_tilt_series_name_warnings`). The AreTomo bug itself is not ours.
**Seen:** `/groups/klumpe/crboost_data/copia-empiar12580-tutorial-20260909-1414` (2026-09-09), tilt-series name
`copia-empiar12580-tutorial-20260909-1414_Position_1` (51 chars). The 42-char name of
`copia-empiar12580-20260908-1447_Position_1` works.

**How it shows.** The tomogram preview (Warp's `<ts>_11.80Apx.png`, Journey, Tomograms view) shows the particle
layer as a diagonal band instead of filling the field: the sample plane is inclined in the volume. Template
matching still finds ~1000 picks, so nothing downstream complains. The alignment job reports success.

**Where to look.** `External/<align job>/task_0.out`: `Failed to process <ts>.tomostar, marked as unselected` /
`Could not find <ts>.tlt` / `1/1, 1 failed`. `External/<align job>/warp_tiltseries/<ts>.xml`:
`UnselectManual="True"`, `<Angles>` = the nominal stage angles (the good run has AreTomo's tilt-offset applied,
−44.01 for nominal −32), `<AxisAngle>` unrefined. `tiltstack/<ts>/<ts>_Imod/`: the `.tlt` is there under a
mangled name (`20260909-1414_Position_1.st`, 252 bytes = the 28 angles) instead of `<ts>_st.tlt`; `.xf` and
`.xtilt` are fine. tsCtf then fits `PlaneNormal` ≈ 12° off z to absorb the missing tilt offset.

**Why.** Our tilt-series id is `<project dirname>_<mdoc stem>`, and protocol projects are named
`<protocol>-<YYYYMMDD-HHMM>`, so a 9-char longer protocol name pushed the basename from 42 to 51. AreTomo 1.0's
`-OutImod` writer mangles the `.tlt` file name at that length (exact limit unknown: 42 good, 51 bad); WarpTools
`ts_aretomo` needs that `.tlt` to import the alignment, gives up on the series, flags it `UnselectManual`, and
exits 0. Our driver's success check only looked for the `.st.aln`, which AreTomo had written.

**What to do.** Keep project names short (`copia-tutorial`, not `copia-empiar12580-tutorial`). A project that hit
this must be recreated: every job from alignment on ran with the wrong geometry. The driver now fails the series
when the XML says `UnselectManual="True"`, and protocol creation warns when a tilt-series name would exceed 42
characters. Open: the same warning for non-protocol project creation, and pinning the exact AreTomo limit.

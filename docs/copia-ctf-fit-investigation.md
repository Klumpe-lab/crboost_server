# Copia protocol run: CTF-fit divergence investigation (2026-09-07/08)

Working log for `docs/known_bugs.md` entry 1. Read the **Handoff** first if you are picking this up.

## Result (2026-09-08, 14:45)

`ctf_fit_test.sbatch` ran in 9 min. **Cause = frame-series fit band.** `fs_ctf` with `30:10` Å fits all 28 images at
5.10–5.53 µm, astigmatism ≤ 0.09 µm (with `30:6` the same images diverged). `ts_ctf` does not search: all five
variants (noflip/flip × 30-6/30-10, `--auto_hand 1`) reproduced job005's garbage within 0.1 µm in ~20 s, because
`ts_import` copies the frame-series defoci into the tilt-series XML and `ts_ctf` only refines them locally. The
handedness therefore never touched the fit; its verdict is still open (every `--check` so far read garbage gradients)
and `hand_check.sbatch` re-runs the check on the good fits. Protocol updated: `c_range_min_max`/`range_min_max`
`"30:10"`, both defocus windows back to `"1.1:8"`.

## Handoff (state at the end of the 2026-09-08 session)

- DONE: `ctf_fit_test.sbatch` (see Result above). Project C = `copia-empiar12580-20260908-1447` created from the fixed
  protocol 14:47: 28 defoci 5.15–5.58 µm, hand check −0.994 (flip, = tutorial). Pipeline running; STA chain copied to
  `C/tutorial_sta/` (paths rewritten). `hand_check.sbatch` did NOT work (tomostar rewrite ignored, still 0.159); moot. Old text:
  `tutorial_sta/logs/hand_check.out` (handedness verdict on good fits; protocol currently `set_flip` as the tutorial).
- Superseded description of the experiment: `/groups/klumpe/crboost_data/copia-empiar12580-20260908-1407/tutorial_sta/ctf_fit_test.sbatch`
  → `tutorial_sta/logs/ctf_fit_test.out` (+ `.err`). It answers which of two suspects breaks Warp's per-tilt CTF fit:
  the fit band (30–6 Å vs 30–10 Å) or the defocus handedness being set before `ts_ctf`. Read it with the
  "How to read the experiment" section below, then apply the decision tree.
- Two projects exist. A = `copia-empiar12580-20260906-1319` (protocol as authored, CTF windows `1.1:8`; full pipeline
  done; hand-run STA chain in `A/tutorial_sta/`; Refine3D plateaued ~15 Å). B = `copia-empiar12580-20260908-1407`
  (protocol with windows `4.0:6.5`, the failed workaround; pipeline re-run from scratch, tsCtf output already known bad).
  Both are throwaways kept for comparison. The next real run should be a project C from the corrected protocol.
- Uncommitted repo edits: `config/protocols/copia-empiar12580/protocol.yaml` lines 43/44/76/78 (band `30:10`, windows `1.1:8`;
  done), `docs/known_bugs.md` (new), this file (new). Verify with
  `git diff -- config/protocols/copia-empiar12580/protocol.yaml` and `git status --short docs/`.
- After the CTF fix: re-run the pipeline (project C), check the 28 per-tilt defoci (must all be 4.4–5.7 µm), then the
  STA chain from `A/tutorial_sta/` (copy the folder, change `PROJ` in `_env.sh`; stage 01 mask threshold rule is
  40 % of the lowpassed max; stage 02 is `mpirun -n 3 relion_refine_mpi`). Targets: Refine3D+PostProcess < 10 Å,
  final PostProcess (B = −75) < 8.5 Å.

## What we are trying to find out

Why does the copia protocol (a faithful copy of the CryoBoost v1 tutorial, verified parameter by parameter) refine to
~15 Å where the tutorial reaches 8.5 Å at the same stage? Established answer: 17–22 of the 28 tilts carry a wrong
defocus out of Warp (`fs_ctf` / `ts_ctf`), RELION bakes those into the CTF-premultiplied 2D stacks at extraction, and
the affected tilts cancel signal beyond ~20 Å. Open question: *why* Warp's fit diverges when the data plainly contain
a clean 5.3 µm CTF on every tilt, and which protocol/driver change prevents it.

## Facts established (all on `…_Position_1`, 28 tilts −32°…+49°, 2.07 e/Å² per tilt, 2.95 Å/px, 300 kV)

| item | value | source |
|---|---|---|
| true defocus, every tilt | 5.05–5.53 µm, CTF² correlation 0.5–0.9 in bands 30–12 … 30–6 Å | 1D scan of Warp's `<TiltPS1D>` (below), both projects |
| microscope readout | mdoc `Defocus` −10.4 ± 0.5 µm, flat; ~0.9 µm less at 40–49° | `mdoc/*.mdoc` (not the true defocus, but flat) |
| Warp `ts_ctf`, window 1.1:8 (A) | six first-acquired tilts (4°–19°) 5.32–5.39 µm, rest 1.3–5.7 µm; 17 wrong | `A/External/job005/tilt_series/*.star` col 10, `GridCTF` in the XML |
| Warp `ts_ctf`, window 4.0:6.5 (B) | same six fine; rest −18 … +60 µm; 22 wrong | `B/External/job005/…` |
| Warp `fs_ctf`, window 4.0:6.5 (B) | same pattern; astigmatism (`DefocusDelta`) 25–64 µm on failed images; per-quadrant `GridCTF` −125 … +98 µm | `B/External/job002/warp_frameseries/*.xml` |
| Warp `fs_ctf`, window 1.1:8 (A) | failed images cluster at 3.5–3.7 µm (aliased minimum), fit resolution ~10 Å vs 6.1 Å on the six good ones | `A/External/job002/…`, registry `ctf_resolution` |
| `ts_defocus_hand --check` | −0.035 (A), +0.159 (B): noise; it reads the 2×2 frame-series gradients, which were garbage | job005 `run.out` |
| driver order | `--check` → `--set_flip` (protocol: `set_flip`) → `ts_ctf`; identical in v1 (`CryoBoost/src/warp/tsCtf.py`) and Warp's quick start | `drivers/ts_ctf.py` |
| Warp defaults | `--range_high 4`, `--defocus_max 5`; `ts_ctf --auto_hand N` = "estimate handedness, then estimate CTF with the correct handedness" | WarpTools API reference |
| not the cause | motion (frame shifts of a few Å), illumination (mean counts flat), EER grouping (32), column mix-up (col 10 is `_rlnDefocusU`) | job002 XML, mdoc `MinMaxMean` |
| unexplained | v1 tutorial run on the same data reached 8.5 Å with the same `30:6` / `1.1:8` (Warp `2.0.0dev36` here vs dev29/31 there) | tutorial.rst, v1 scheme |

Refine3D on A's stacks (MPI, 3 procs, local from 1.8°): 16.1 → 17.4 → 20.0 → 17.4 → 17.4 → 15.4 Å by iteration 7,
angular accuracy 0.7–0.9°. The refinement is healthy; the input is not.

## Process (in order, so the next reader does not redo it)

1. Protocol vs tutorial verified (`tutorial.rst` + v1 `config/Schemes/warp_tomo_prep`): preprocessing identical;
   deviations only in Class3D `healpix_order` 4 (GUI 1.8°, tutorial 3.75° = order 3), `tau_fudge` 1 (GUI default 4),
   peak-extraction Ø 575 (tutorial 550), TM mask sphere d575 (tutorial relion_mask_create on the template).
2. Hand-written RELION STA chain (`A/tutorial_sta/`, 12 sbatch stages + README). Learned on the way: tutorial mask
   threshold 0.15 is absolute and does not transfer (our map max 0.064); `relion_refine` refuses
   `--split_random_halves` without MPI; `relion5.0_tomo.sif` has Open MPI 4.0.3 + `relion_refine_mpi`.
3. Refine3D plateau → per-tilt defocus check → hypothesis 1 "window too wide → aliased minima" → protocol windows
   set to `4.0:6.5` → project B → **worse** (unbounded refinement after the bounded grid search).
4. Direct measurement: scan of `<TiltPS1D>` and `<PS1D>` curves vs CTF² (awk, below) → 5.3 µm on all tilts, both
   projects, every band. Hypothesis 1 dead as a root cause.
5. Hypotheses 2 (fit band) and 3 (handedness before fit) → `ctf_fit_test.sbatch`.

## The instrument: 1D CTF scan of Warp's stored spectra

Warp writes, per tilt, `<TiltPS1D ID=z>` (tilt-series XML, geometry-corrected) and `<PS1D>` (frame-series XML, whole
image) as `freq_cyc_px|value;…` (256 samples). This scan subtracts a ±20-sample running mean and correlates with
CTF²(Δf) over 30–`hires` Å for Δf = 2–7 µm. It is independent of Warp's fit and robust where the fit is not.
`services/tilt_series/warp_curves.py` already parses these curves for the Journey; a driver-side outlier check can
reuse it.

    scan='BEGIN{apix=2.95; lam=0.019688; cs=2.7e7; q0=0.1; pi=3.141592653589793}
    NF==2{f[n]=$1+0; p[n]=$2+0; n++}
    END{
      for(i=0;i<n;i++){s=0;c=0;for(j=i-20;j<=i+20;j++){if(j>=1&&j<n){s+=p[j];c++}} bg[i]=s/c; r[i]=p[i]-bg[i]}
      best=-2
      for(D=20000; D<=70000; D+=250){
        sx=0;sy=0;sxx=0;syy=0;sxy=0;m=0
        for(i=1;i<n;i++){k=f[i]/apix; if(k<1/30||k>1/hires) continue
          chi=pi*lam*D*k*k - 0.5*pi*cs*lam*lam*lam*k*k*k*k
          ctf=-(sqrt(1-q0*q0)*sin(chi)+q0*cos(chi)); y=ctf*ctf
          sx+=r[i];sy+=y;sxx+=r[i]*r[i];syy+=y*y;sxy+=r[i]*y;m++}
        cov=sxy/m-(sx/m)*(sy/m); vx=sxx/m-(sx/m)^2; vy=syy/m-(sy/m)^2; cc=cov/sqrt(vx*vy)
        if(cc>best){best=cc;bestD=D}
      }
      printf "%.2f(%.2f) ", bestD/1e4, best
    }'
    y=External/job005/warp_tiltseries/<ts>.xml   # TiltPS1D ID = rank of the tilt by angle (0 = most negative)
    for i in $(seq 0 27); do grep -o "<TiltPS1D ID=\"$i\">[^<]*" "$y" | sed 's/<TiltPS1D ID="[0-9]*">//' | tr ';' '\n' | awk -F'|' -v hires=10 "$scan"; done; echo

Quick per-tilt defocus check of what RELION will use (columns 3 and 10 of Warp's export):

    awk '!/^_|^#|^$|^data|^loop/{printf "%6s %8.0f\n", $3, $10}' External/job005/tilt_series/*.star | sort -n

## The experiment (`B/tutorial_sta/ctf_fit_test.sbatch`)

Everything is copied into `B/tutorial_sta/ctf_fit_test/`; no project job dir is touched.

A. `ts_ctf` from the job004 alignment XML (no CTF, `AreAnglesInverted=False`), window `1.1:8`:
`ts_noflip_30-6`, `ts_flip_30-6`, `ts_noflip_30-10`, `ts_flip_30-10`, `ts_autohand_30-6` (`--auto_hand 1`).
Each block prints: the `--check` correlation computed *after* the fit, `AreAnglesInverted`, and the 28 `GridCTF`
defoci in angle order (−32 … 49). Last line: job005's values for reference.

B. standalone `fs_ctf` on a copy of the job002 staging (28 images, grid 2x2x1, window `1.1:8`): `fs_30-10`,
`fs_30-8`. Prints `tilt:defocus/astig` per image.

### How to read it / decision tree

- A block with all 28 defoci in 5.0–5.6 µm is a working configuration. If `30-10` works for both hands and `30-6`
  works for neither → cause is the band: protocol `range_min_max` → `"30:10"` (both tsCtf and fsMotionAndCtf
  `c_range_min_max`), windows back to `"1.1:8"` (or keep a sane prior like `"3.0:7.0"`; the window was not the
  cause, so tutorial fidelity wins). Add the band to `docs/known_bugs.md` entry 1 as the fix.
- If only one hand works regardless of band → cause is handedness-before-fit: the driver must fit first (or use
  `--auto_hand`), and the hand that works tells whether the tutorial's `set_flip` is right for this data. The
  after-fit `--check` correlations then become the authoritative hand verdict (positive = no flip, negative = flip).
- If `autohand` works → simplest driver change: `defocus_hand: auto` → `ts_ctf --auto_hand N` instead of
  check/set before the fit.
- If nothing works → remaining suspects: Warp dev36 itself (try the frame-series `--use_sum`, window 256/1024, or
  another Warp build); ask what the tutorial author's Warp build was.
- B tells whether the frame-series fits (input to `--check`, and to the registry QC) are fixed by the band alone.

Whatever wins: fix the protocol, create project C, verify the 28 defoci, continue with the STA chain.

## Pending / open items

- Protocol: FIXED (band `30:10` on both CTF jobs, windows back to `1.1:8`). Was: lines 44 and 78 `"4.0:6.5"` (failed
  workaround); set from the experiment. Tutorial-fidelity fields left deliberately untouched for a clean comparison:
  `healpix_order: 4 # 3.75 deg` (comment wrong, value = GUI 1.8°; tutorial → 3), `tau_fudge: 1.0` (tutorial → 4),
  `tmextractcand.particle_diameter_ang: 575` (tutorial 550).
- `drivers/ts_ctf.py`: per-tilt defocus outlier flag (median ± 1 µm, or the 1D scan), surfaced in log/registry/tab;
  run `--check` only on sane fits; consider `--auto_hand`. `services/jobs/class3d.py` `healpix_order` description
  (`docs/known_bugs.md` entry 2).
- Handedness for this dataset is genuinely undetermined: tutorial says flip, B's (garbage-input) check said no flip.
  The after-fit checks in the experiment decide.
- STA chain scripts: `A/tutorial_sta/` (README is stage-by-stage). Stage 05/09 flags verified against
  `relion_help.txt`. Stage 02 needs `mpirun -n 3 relion_refine_mpi`.
- Memory: `project_copia_ctf_bimodal_fits.md`, `project_copia_tutorial_sta_chain.md` in the Claude memory dir hold the
  same state in short form.

## Cross-check against Sven's v1 run (`/groups/klumpe/user/sven.klumpe/Processing/Copia/try2`, 2025-10, Warp dev31)

Different data: `Position_11_2`, 39 tilts (−45°…+69°, dose-symmetric from 12°), 1.55 Å/px, 3.2 e/Å² per tilt
(mdoc; his Import says 4.8), ~4.5–5.1 µm. Same v1 CTF parameters (`30:6.0`, `1.1:8`, window 512, `2x2x1`).

- Same failure, milder: `fs_ctf` put the 7 last-acquired images (−27°, 57°, 60°, −39°, 66°, 69°, −45°) at
  1.1–1.75 µm; `ts_ctf` carried them (1.6–2.2 µm in `tilt_series/Position_11_2.star`, column 15) — the third
  dataset/version confirming that `ts_ctf` only refines the frame-series value. 32 good tilts sufficed for a
  meaningful hand check (−0.694, flip) and for 9.78 Å (PostProcess/job024, no cleaning) / 8.79 Å (job050, after
  manual cleaning `Refinejob28_manuallyCleaned.star`, Select, CtfRefine). His per-tilt SNR is ~4× ours (dose ×1.5,
  pixel area ×3.6), which is why 6 Å was a usable band limit for him and not for EMPIAR Position_1.
- His RELION commands (note.txt) validate the hand-written chain in `tutorial_sta/`: Refine3D job037 is flag-for-flag
  our stage 02 except he kept `--firstiter_cc` (GUI "absolute greyscale: No"; tutorial says Yes) and `--trust_ref_size`;
  Reconstruct `--b 384 --crop 224 --bin 2` at 1.55 Å/px = our unbinned 2.95; PostProcess `--auto_bfac --autob_lowres 10`
  then `--adhoc_bfac -75`; Polish `relion_tomo_align --b 512 --r 7 --motion --s_vel 0.2 --s_div 5000 --it 30000`
  (GUI default `s_vel` is 0.2, CLI default 0.5 → added to stage 05); CtfRefine `--b 512 --do_defocus --d0 -6000
  --d1 6000 --do_scale --per_frame_scale` (no defocus regularisation; the tutorial's lambda 0.2 is kept in stage 09);
  Extract `--min_frames 1 --float16 --stack2d` (added `--min_frames 1`). Box sizes are 2× ours because of the pixel size.
- He also had to drop the mask threshold: MaskCreate job012 at 0.15 (tutorial) was redone as job013 at 0.01; inner
  radius 146 px × 1.55 Å = 226 Å (ours 73 px × 2.95 = 215 Å).

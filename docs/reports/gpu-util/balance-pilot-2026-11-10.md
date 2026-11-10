# The balance pilot (roadmap 47 S3), 2026-11-10

`orchestrator_test_FCIsolation`: the 163 tilt series of the 2025-10-31 FCIsolation data with the anglestest settings
(I1 at 5°, 6.2 Å, 20 candidates at 575 Å), on the dev server with the S1 conf of the same day: supervisors, tsImport
and the CPU steps on partition `g` with any RTX 6000 or A100 as the ticket; motion, alignment, CTF and export on any
card (`gpu:1`, no constraint); reconstruct and template matching on full B200s; throttles still 20 and 16 (the
project was created before S2). Compared with `anglestest_FCIsolation` of 2026-11-09 (B200 cards and slices only).
Times from the task headers, the `.task_status` mtimes and the exit markers; billing from one `sacct` over the 13
job ids; the per-tilt-series comparison with `scripts/compare_runs_by_device.py`.

## The five numbers (roadmap 47 §1)

| | Balance pilot | anglestest (2026-11-09) |
|---|---|---|
| Run | 12:33:28 | 13:00 |
| Wait before the first task (Run to the first motion task) | 2 min 37 s (supervisor queued 98 s) | about 27 min (supervisor queued 26 min) |
| Sum of the stage-boundary waits (last task of a stage to the first task of the next) | 9 min 5 s over 6 boundaries, of which about 3 min are the supervisors' merges | not recorded per boundary; each supervisor queued on `c` afresh |
| Wall, Run to extraction done | 85 min 33 s (to the end of template matching 72 min 34 s) | about 105 min to the end of template matching |
| Billing-h (696 jobs and tasks) | 211.3 | 205.6 |
| Outputs | 158 of 158 from CTF on, the same five alignment failures; 1,412 candidates against 1,409; but see below | |

Per stage (first task to last task; the marker follows after the supervisor's merge):

| Stage | Supervisor started | First task | Last task | Marker | Tasks |
|---|---|---|---|---|---|
| Motion | 12:35:06 | 12:36:05 | 12:51:24 | 12:53:14 | 163 at 20 |
| tsImport (one job, 4 calls) | 12:54:07 | | | 12:56:58 | |
| Alignment | 12:57:07 | 12:58:08 | 13:06:53 | 13:07:27 | 163 at 20 |
| CTF | 13:08:08 | 13:09:06 | 13:15:10 | 13:15:32 | 158 at 20 |
| Reconstruct | 13:16:07 | 13:16:47 | 13:27:51 | 13:28:15 | 20 chunks of 8 |
| Template matching | 13:28:19 | 13:28:33 | 13:45:52 | 13:46:02 | 27 lanes of 6 at 16 |
| Extraction | 13:46:06 | 13:46:32 | 13:54:57 | 13:59:01 | 158 at 16 |

Every supervisor started on `cbe-g3-1` within 4 to 60 s of becoming eligible. Motion's pending tasks' reason at 12:46
was `JobArrayTaskLimit` with devices free: the throttle, not the queue, set the stage's length (roadmap 47 D3, now 40).

## Where the tasks landed

| Stage | B200 card | B200 slice | A100 | RTX 6000 |
|---|---:|---:|---:|---:|
| Motion | 98 | 53 | 0 | 7 |
| Alignment | 36 | 122 | 0 | 0 |
| CTF | 57 | 81 | 2 | 18 |
| Reconstruct | 20 chunks | | | |

## Outputs against anglestest, per tilt series, by the device of today's task

Motion (frame XMLs) and alignment (`.st.aln`):

| Motion ran on | Series | Motion fits differ (any frame > 0.05 µm or > 1 px) | Alignment differs (> 1 px) |
|---|---:|---:|---:|
| B200 card | 98 | 4 (one frame each, 0.05–0.30 µm) | 0 |
| B200 slice | 53 | 0 | 0 |
| RTX 6000 | 7 | 7 | 6 |

The seven RTX 6000 series: Position_17_2 (7 frames differ, alignment off by 885 px, tomogram centre correlation with
yesterday's 0.31), Position_18 (every tilt shifted 13 px, correlation −0.01), Position_17_3 (correlation 0.42),
Positions 16_3, 16_4 and 17_4 (alignment kept 41 tilts against 40, correlation 0.98–0.99), Position_17 (0.6 px,
0.996). The alignment of all seven ran on B200 slices: the damage came in through the motion-corrected averages.

Tilt-series CTF, on the 151 series whose motion and alignment are identical to yesterday's (alignment XMLs and
tomostars bitwise the same, both runs' CTF tasks on B200 devices but for the 18 RTX 6000 and 2 A100 below):

| CTF ran on | Series | Identical (< 0.001 µm) | > 0.05 µm | > 0.3 µm | Max |
|---|---:|---:|---:|---:|---:|
| B200 card | 57 | 23 | 13 | 3 | 0.51 µm |
| B200 slice | 81 | 32 | 8 | 2 | 0.49 µm |
| RTX 6000 (motion on a B200) | 11 | 7 | 0 | 0 | 0.004 µm |
| A100 | 2 | 0 | 1 | 0 | 0.06 µm |

So Warp's tilt-series CTF fit is not reproducible from run to run on the same architecture with the same inputs for
about a quarter of the series, mostly the short ones (21–37 tilts). It is what moves 16 tomograms with identical
alignments to correlations of 0.83–0.98. (P13 found it identical on four good series.) Tomogram centres: median
correlation 1.0000 over the 151, 71 of them above 0.99999.

## Reading

- The stall is gone: 98 s to the first supervisor, boundaries of seconds to two minutes, 85 minutes to extraction
  against 105 to the end of template matching before, at the same billing.
- The RTX 6000 is unfit for Warp's motion correction here: 7 of 7 series changed, 3 destroyed, 3 more lost a tilt.
  The P13 reading ("marginal frames only") was too mild; a whole series with signal can go wrong when its averages
  change. The A100 carried too few tasks (2 CTF) to judge today.
- Run-to-run variance of the tilt-series CTF on one architecture is a separate finding, for the issue to Warp's
  author and for anyone comparing runs of this pipeline.

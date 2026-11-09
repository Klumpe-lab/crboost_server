#!/bin/bash
# P12 (roadmap 45, pipeline shape): does Slurm here start task i of a dependent array as soon as task i of the array
# it depends on ends (--dependency=aftercorr), how long after, and what becomes of a task whose counterpart failed
# or does not exist. Run it on the login node; it submits four tiny jobs to partition c (one CPU, 1 GB, minutes):
#
#   bash /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/probe_aftercorr.sh
#
# A: an array 0-5 at 3 at a time; task i sleeps 20 + 10 i seconds; task 3 fails. B: an array 0-6 with
# aftercorr:A, under the cluster's default for a dependency that can never be met. C: the same with
# --kill-on-invalid-dep=yes. Task 6 of B and C has no counterpart in A. R: once A has ended (afterany), waits 150 s,
# records squeue and sacct for A, B and C, and cancels whatever of B and C is still pending. Results in
# /scratch/users/artem.kushner/gpu_probe/aftercorr_<date-time>.
set -euo pipefail
RES=/scratch/users/artem.kushner/gpu_probe/aftercorr_$(date +%Y%m%d-%H%M%S)
mkdir -p "$RES"
common=(-p c -c 1 --mem=1G -t 00:10:00)
A=$(sbatch --parsable "${common[@]}" -J cb-probe-corrA --array=0-5%3 -o "$RES/A-%a.out" \
  --wrap='date +%s.%N; sleep $((20 + 10 * SLURM_ARRAY_TASK_ID)); [ "$SLURM_ARRAY_TASK_ID" != 3 ]')
B=$(sbatch --parsable "${common[@]}" -J cb-probe-corrB --array=0-6 --dependency="aftercorr:$A" \
  -o "$RES/B-%a.out" --wrap='date +%s.%N')
C=$(sbatch --parsable "${common[@]}" -J cb-probe-corrC --array=0-6 --dependency="aftercorr:$A" \
  --kill-on-invalid-dep=yes -o "$RES/C-%a.out" --wrap='date +%s.%N')
R=$(sbatch --parsable "${common[@]}" -J cb-probe-corrR --dependency="afterany:$A" -o "$RES/R.out" \
  --wrap="sleep 150; squeue -j $B,$C -r -o '%i %T %r' >$RES/squeue.txt; sacct -j $A,$B,$C -X -P -n -o JobID,JobName,State,Submit,Start,End,Reason >$RES/sacct.txt; scancel $B $C")
echo "A=$A B=$B C=$C R=$R"
echo "results in $RES in about 5 minutes"

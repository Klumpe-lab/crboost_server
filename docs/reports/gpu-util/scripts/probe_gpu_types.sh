#!/bin/bash
#SBATCH -J cb-probe-gputypes
#SBATCH -p g
#SBATCH --gres=gpu:quadro_rtx_6000:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 8
#SBATCH --mem=48G
#SBATCH -t 02:30:00
# P13 (roadmap 45): every card type of partition g through the pipeline's GPU stages. Besides the B200s (billing 24)
# and their 1g.23gb slices (3.43), g has Quadro RTX 6000 (sm_75, 24 GB, billing 2), A100 SXM4 40 GB (sm_80, 6) and
# A100 3g.20gb MIG slices (20 GB, 3), none of them tried with our images. One job per card type; each runs, on tilt
# series of the first 412 pilot (read only), every GPU stage's own production command from that pilot's inputs, on
# the CPUs production gives the stage, and compares the outputs with the pilot's (probe_gpu_types.py says what and
# how). The B200 and B200-slice jobs are the uncrowded references for the same tilt series.
#
#   sbatch --gres=gpu:quadro_rtx_6000:1 -o /scratch/users/artem.kushner/gpu_probe/probe_gpu_types-%j.out /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/probe_gpu_types.sh 4
#   sbatch --gres=gpu:nvidia_a100-sxm4-40gb:1 -o ... (the same, one line per card type: also nvidia_a100_3g.20gb,
#   nvidia_b200, nvidia_b200_1g.23gb)
#
# Results in /scratch/users/artem.kushner/gpu_probe/gpu_types_<jobid> (runs.tsv, compare.tsv, checks.tsv,
# card.json, logs/); nothing is written under /groups. Then:
#   python3 -B /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/summarise_gpu_types.py /scratch/users/artem.kushner/gpu_probe/gpu_types_*
set -uo pipefail
N=${1:-4}
REPO=/users/artem.kushner/dev/crboost_server
RES=/scratch/users/artem.kushner/gpu_probe/gpu_types_${SLURM_JOB_ID}
mkdir -p "$RES"
echo "[SLURM] job ${SLURM_JOB_ID} on $(hostname -s), GPU ${CUDA_VISIBLE_DEVICES:-none}, started $(date '+%F %T')"
PYTHONPATH=$REPO PYTHONNOUSERSITE=1 "$("$REPO/crboost" env)/bin/python3" -B \
  "$REPO/docs/reports/gpu-util/scripts/probe_gpu_types.py" "$RES" "$N"

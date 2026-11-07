#!/bin/bash
# Probe P2 of roadmap 45: what can a node show us? Five minutes, no real work, everything is printed.
# Submit the same script to a full B200, a MIG slice and a CPU node, from any directory:
#
#   sbatch -J cb-cap -p g --gres=gpu:nvidia_b200:1 -N1 -n1 -c4 --mem=16G -t 0:05:00 \
#       -o cap-b200.out capability.sh
#   sbatch -J cb-cap -p g --gres=gpu:nvidia_b200_1g.23gb:1 -N1 -n1 -c4 --mem=16G -t 0:05:00 \
#       -o cap-slice.out capability.sh
#   sbatch -J cb-cap -p c -N1 -n1 -c4 --mem=16G -t 0:05:00 -o cap-c.out capability.sh
#
# The CPU job's GPU lines fail by design. Paste the three outputs into docs/reports/gpu-util/capability-<date>.md.
# No `set -e`: a missing tool is an answer, not a reason to stop.

sif=${SIF:-/groups/klumpe/software/containers/sifs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.sif}
big=${BIGFILE:-/groups/klumpe/software/containers/sifs/imod.sif} # about 1.2 GB on /groups, read once below
show='echo "inside: CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}"; nvidia-smi -L'

echo "== host"
hostname
date
echo "CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}  SLURM_GPUS_ON_NODE=${SLURM_GPUS_ON_NODE:-unset}"

echo "== profiling and monitoring tools on PATH"
command -v dcgmi nsys ncu nvidia-cuda-mps-control iostat pidstat perf py-spy strace

echo "== GPUs as the host sees them"
nvidia-smi -L
nvidia-smi --query-gpu=name,driver_version,compute_mode,mig.mode.current --format=csv
nvidia-smi dmon -s umt -c 3 -d 1
nvidia-smi --query-gpu=utilization.gpu,utilization.memory,memory.used,power.draw --format=csv

echo "== storage the job sees (what backs /tmp, how big)"
df -hT /tmp /dev/shm /scratch /groups
findmnt -T /tmp
mount | grep -E " /groups | /scratch | /users "

echo "== cgroup files a job can read"
cg=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)
echo "$cg"
ls "$cg"
cat "$cg/cpu.max" "$cg/memory.max"

echo "== which counters in /proc/self/io see a read from /groups (rchar, read_bytes)"
python3 - "$big" <<'EOF'
import sys

with open(sys.argv[1], "rb") as f:
    while f.read(1 << 24):
        pass
print(open("/proc/self/io").read())
EOF

echo "== the container as crboost runs it (--nv --cleanenv): which GPUs does it see?"
time apptainer exec --nv --cleanenv -B /groups "$sif" bash -c "$show"

echo "== the same without --cleanenv (the site docs advise against --cleanenv on GPU nodes)"
time apptainer exec --nv -B /groups "$sif" bash -c "$show"

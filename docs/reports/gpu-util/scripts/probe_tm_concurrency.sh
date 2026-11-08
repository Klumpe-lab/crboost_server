#!/bin/bash
# Probe P6 of roadmap 45: how many template-matching processes fill one B200?
#
# P5 found a template-matching process spends about 30 s on one CPU core before its first kernel, with 30 GB of the
# card already held; one process uses 31 GB of the card's ~180 GB. This runs 1, 2, 3 and then 4 processes at once on
# the one allocated GPU, each the full angular search of the production command on the 412 tomogram, outputs on the
# job's /tmp, kernel cache warm. If one process's single-core preamble overlaps another's search, N processes finish
# in much less than N times the time of one. A once-a-second nvidia-smi trace runs throughout; dcgmi (SM, tensor and
# DRAM activity) is tried as well and may not work in a user job: that is also an answer.
#
#   sbatch -o probe_tm_conc-%j.out docs/reports/gpu-util/scripts/probe_tm_concurrency.sh
#
# Nothing is written to /groups, which is only read. PROBE_DRY=1 runs stand-ins (no GPU needed).
#SBATCH -J cb-probe-tmconc
#SBATCH -p g
#SBATCH --gres=gpu:nvidia_b200:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 16
#SBATCH --mem=120G
#SBATCH -t 0:50:00
set -euo pipefail

SCRATCHDIR=${SCRATCHDIR:-/scratch/users/$(id -un)}
id=${SLURM_JOB_ID:-manual}
RES=$SCRATCHDIR/gpu_probe/tm_conc_$id # results: runs.tsv, dmon.log, dcgm.log, one log per process
T=${TMPDIR:-/tmp}/tm_conc_$id         # outputs and caches on the job's private /tmp
mkdir -p "$RES" "$T"

sif=${SIF:-/groups/klumpe/software/containers/sifs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.sif}
cuda=/groups/klumpe/software/cache_and_artifacts/cuda_kernels/$(basename "$sif" .sif)

# The inputs and flags of probe_tm_ab.sh: the 412 tomogram and the command crboost runs, with the full angle list.
p=${PROJECT:-/groups/klumpe/crboost_data/AGG412_20251031_412_FCIsolation}
j=$p/External/job019
ts=AGG412_20251031_412_FCIsolation_Position_1
vol=$(readlink -f "$j/tmResults/$ts.mrc")
tpl=$p/templates/412_particle/run_it015_class001_apix6.20_box128
common="-t ${tpl}_black.mrc -m ${tpl}_sphere_d575_s5.mrc --voltage 300.0 --spherical-aberration 2.7"
common="$common --amplitude-contrast 0.1 --tomogram-ctf-model phase-flip --per-tilt-weighting --log debug"
common="$common --tilt-angles $j/tiltAngleFiles/$ts.tlt --defocus $j/defocusFiles/$ts.txt"
common="$common --dose-accumulation $j/doseFiles/$ts.txt -v $vol -g 0 -s 1 1 1"
short=$RES/angles_short.txt
head -n 48 "$j/angles_I1.txt" >"$short"
full=$j/angles_I1.txt

standin='import time; time.sleep(3); end = time.time() + 3; [None for _ in iter(lambda: time.time() < end, False)]'
binds="-B /groups -B /programs -B /scratch -B /software -B /tmp"
one() { # label, angle file: one process, its own output folder and kernel-cache directory (shared, warm)
    local label=$1 angles=$2 t0 t1
    mkdir -p "$T/out_$label"
    t0=$(date +%s.%N)
    if [ -n "${PROBE_DRY:-}" ]; then
        python3 -c "$standin"
    else
        apptainer exec --nv --cleanenv $binds "$sif" bash -c \
            "export XDG_CACHE_HOME=$T/xdg CUPY_CACHE_DIR=$T/cupy CUDA_CACHE_PATH=$cuda CUDA_CACHE_MAXSIZE=4294967296 \
             MPLCONFIGDIR=$T/mpl TORCHINDUCTOR_CACHE_DIR=$T/inductor TRITON_CACHE_DIR=$T/triton \
             NUMBA_CACHE_DIR=$T/numba TQDM_DISABLE=1; \
             pytom_match_template.py $common --angular-search $angles -d $T/out_$label" >"$RES/$label.log" 2>&1 ||
            echo "FAILED $label (see $label.log)" | tee -a "$RES/failed.txt"
    fi
    t1=$(date +%s.%N)
    printf '%s\t%s\t%s\n' "$label" "$t0" "$t1" >>"$RES/procs.tsv"
    rm -rf "$T/out_$label"
}

batch() { # N: N processes at once; the line in runs.tsv spans from the first start to the last end
    local n=$1 t0 t1 i
    t0=$(date +%s.%N)
    for i in $(seq 1 "$n"); do one "n${n}_p$i" "$full" & done
    wait
    t1=$(date +%s.%N)
    printf '%s\t%s\t%s\n' "$n" "$t0" "$t1" >>"$RES/runs.tsv"
}

sampler=""
dcgm=""
if [ -z "${PROBE_DRY:-}" ]; then
    stdbuf -oL nvidia-smi dmon -s um -d 1 -o T >"$RES/dmon.log" 2>&1 &
    sampler=$!
    # Best effort: SM active, SM occupancy, tensor pipe active, DRAM active, once a second, if a user job may.
    dcgmi discovery -l >"$RES/dcgm_discovery.txt" 2>&1 || true
    date +%s.%N >"$RES/dcgm_start.txt"
    dcgmi dmon -e 1002,1003,1004,1005 -d 1000 >"$RES/dcgm.log" 2>&1 &
    dcgm=$!
    cat "$vol" "${tpl}_black.mrc" "${tpl}_sphere_d575_s5.mrc" >/dev/null # the same page-cache state for every run
fi
trap 'kill $sampler $dcgm 2>/dev/null || true' EXIT

one prime "$short" # fills the kernel caches; not part of the table
batch 1
batch 2
batch 3
batch 4
batch 1 # last, to see whether the first one was typical

echo "done: $RES"
echo "next: python3 docs/reports/gpu-util/scripts/summarise_tm_conc.py $RES"

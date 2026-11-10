#!/bin/bash
# Probe P5 of roadmap 45: what does a template-matching task do in the minute before the card starts to work?
#
# P4b found the card holding 31 GB but with its SMs idle for 58-72 s at the start of every run, whatever the kernel
# cache and the output folder. This repeats one run (short angular search, outputs on the job's /tmp, kernel cache
# warm) with 4 and with 16 CPUs, in mirrored order, and samples the busiest python process once a second: CPU time,
# threads, resident memory, bytes read. If the minute shrinks with more CPUs it is CPU work; if it does not and the
# process reads nothing it is waiting; reads would point at storage. summarise_tm_pre.py prints the table.
#
#   sbatch -o probe_tm_pre-%j.out docs/reports/gpu-util/scripts/probe_tm_prestart.sh
#   python3 docs/reports/gpu-util/scripts/summarise_tm_pre.py $SCRATCHDIR/gpu_probe/tm_pre_1474966
#
# Nothing is written to /groups, which is only read. PROBE_DRY=1 runs a stand-in (3 s asleep, 3 s busy) in place of
# the container, so the sampler and the summary can be tried without a GPU.
#SBATCH -J cb-probe-tmpre
#SBATCH -p g
#SBATCH --gres=gpu:nvidia_b200:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 16
#SBATCH --mem=32G
#SBATCH -t 0:30:00
set -euo pipefail

SCRATCHDIR=${SCRATCHDIR:-/scratch/users/$(id -un)}
me=$(id -un)
id=${SLURM_JOB_ID:-manual}
RES=$SCRATCHDIR/gpu_probe/tm_pre_$id # results: runs.tsv, dmon.log, ps.log, one log per run
T=${TMPDIR:-/tmp}/tm_pre_$id         # outputs and caches on the job's private /tmp
mkdir -p "$RES" "$T"

sif=${SIF:-/groups/klumpe/software/containers/sifs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.sif}
cuda=/groups/klumpe/software/cache_and_artifacts/cuda_kernels/$(basename "$sif" .sif)

# The inputs and flags of probe_tm_ab.sh: the 412 tomogram and the command crboost runs.
p=${PROJECT:-/groups/klumpe/crboost_data/AGG412_20251031_412_FCIsolation}
j=$p/External/job019
ts=AGG412_20251031_412_FCIsolation_Position_1
vol=$(readlink -f "$j/tmResults/$ts.mrc")
tpl=$p/templates/412_particle/run_it015_class001_apix6.20_box128
head -n 48 "$j/angles_I1.txt" >"$RES/angles_short.txt"
common="-t ${tpl}_black.mrc -m ${tpl}_sphere_d575_s5.mrc --voltage 300.0 --spherical-aberration 2.7"
common="$common --amplitude-contrast 0.1 --tomogram-ctf-model phase-flip --per-tilt-weighting --log debug"
common="$common --tilt-angles $j/tiltAngleFiles/$ts.tlt --defocus $j/defocusFiles/$ts.txt"
common="$common --dose-accumulation $j/doseFiles/$ts.txt -v $vol -g 0 -s 1 1 1"

first_n() { # the first N cpus of this job's affinity list, as a comma list (sed, not head: head would SIGPIPE awk)
    taskset -pc $$ | sed 's/.*: //' | tr ',' '\n' |
        awk -F- '{ if (NF == 2) for (i = $1; i <= $2; i++) print i; else print $1 }' | sed -n "1,${1}p" | paste -sd,
}

pick='$4 ~ /^(python|pytom)/ { print $1, $2, $3; exit }' # busiest python (or script-named) process
standin='import time; time.sleep(3); end = time.time() + 3; [None for _ in iter(lambda: time.time() < end, False)]'
sample() { # once a second: epoch, pid, cpu ticks (1/100 s), threads, RSS kB, bytes read, of the busiest python process
    local line pid rch
    while sleep 1; do
        line=$(ps -u "$me" -o pid=,nlwp=,rss=,comm= --sort=-time | awk "$pick") || true
        [ -n "$line" ] || continue
        set -- $line
        pid=$1
        rch=$(awk '/^rchar/ { print $2 }' "/proc/$pid/io" 2>/dev/null) || rch=0
        awk -v t="$(date +%s.%N)" -v pid="$pid" -v n="$2" -v rss="$3" -v rch="${rch:-0}" \
            '{ print t, pid, $14 + $15, n, rss, rch }' "/proc/$pid/stat" 2>/dev/null || true
    done
}

binds="-B /groups -B /programs -B /scratch -B /software -B /tmp"
run() { # label, cpu list
    local label=$1 cpus=$2 t0 t1
    mkdir -p "$T/out_$label"
    t0=$(date +%s.%N)
    if [ -n "${PROBE_DRY:-}" ]; then
        taskset -c "$cpus" python3 -c "$standin"
    else
        taskset -c "$cpus" apptainer exec --nv --cleanenv $binds "$sif" bash -c \
            "export XDG_CACHE_HOME=$T/xdg CUPY_CACHE_DIR=$T/cupy CUDA_CACHE_PATH=$cuda CUDA_CACHE_MAXSIZE=4294967296 \
             MPLCONFIGDIR=$T/mpl TORCHINDUCTOR_CACHE_DIR=$T/inductor TRITON_CACHE_DIR=$T/triton \
             NUMBA_CACHE_DIR=$T/numba TQDM_DISABLE=1; \
             pytom_match_template.py $common --angular-search $RES/angles_short.txt \
             -d $T/out_$label" >"$RES/$label.log" 2>&1 ||
            echo "FAILED $label (see $label.log)" | tee -a "$RES/failed.txt"
    fi
    t1=$(date +%s.%N)
    printf '%s\t%s\t%s\t%s\n' "$label" "$cpus" "$t0" "$t1" >>"$RES/runs.tsv"
    rm -rf "$T/out_$label"
}

c4=$(first_n 4)
c16=$(first_n 16)
echo "cpus: 4 = $c4   16 = $c16" | tee "$RES/cpus.txt"

sampler=""
if [ -z "${PROBE_DRY:-}" ]; then
    stdbuf -oL nvidia-smi dmon -s um -d 1 -o T >"$RES/dmon.log" 2>&1 &
    sampler=$!
    cat "$vol" "${tpl}_black.mrc" "${tpl}_sphere_d575_s5.mrc" >/dev/null # the same page-cache state for every run
fi
sample >"$RES/ps.log" &
ps_sampler=$!
trap 'kill $ps_sampler $sampler 2>/dev/null || true' EXIT

run prime "$c16" # fills the kernel caches; the summary leaves it out
run cpu4_a "$c4"
run cpu16_a "$c16"
run cpu16_b "$c16"
run cpu4_b "$c4"

echo "done: $RES"
echo "next: python3 docs/reports/gpu-util/scripts/summarise_tm_pre.py $RES"

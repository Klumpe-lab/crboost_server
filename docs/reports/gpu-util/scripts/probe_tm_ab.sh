#!/bin/bash
# Probe P4b of roadmap 45: where does a template-matching task's non-search time go?
#
# One 412 tomogram and the crboost command (the inputs of roadmap 43 S1, read only), run in a 2 x 2 design twice
# over, in mirrored order, with a short angular search so that the fixed costs stand out:
#   compiled-kernel cache (cupy): cold = an empty directory; warm = the directory the previous run filled
#   output folder: /groups (as in production) or the job's private /tmp (the BeeGFS behind /scratch)
# then two runs with the full angular search: cold + /groups (as in production) and warm + /tmp (the best case).
# A once-a-second nvidia-smi trace runs throughout; summarise_tm_ab.py turns it and runs.tsv into one table.
#
#   sbatch -o probe_tm_ab-%j.out docs/reports/gpu-util/scripts/probe_tm_ab.sh /groups/klumpe/user/$USER/_gpu_probe
#   python3 docs/reports/gpu-util/scripts/summarise_tm_ab.py $SCRATCHDIR/gpu_probe/tm_ab_1474636
#
# The argument is a folder you own on /groups; about 20 GiB are written there. It is an argument, not an exported
# variable, because sbatch on CBE.next passes a job only $SCRATCHDIR (the CBE module sets SBATCH_EXPORT=SCRATCHDIR).
# Run alone: probe_io.sh writes 16 GiB at once and would slow the writes this one times.
#
# It deletes the folder it makes under $GROUPS_OUT and only that one, and only if it holds its marker file.
# PROBE_DRY=1 prints what it would run instead of running it (no GPU needed).
#SBATCH -J cb-probe-tm
#SBATCH -p g
#SBATCH --gres=gpu:nvidia_b200:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 4
#SBATCH --mem=32G
#SBATCH -t 0:40:00
set -euo pipefail

GROUPS_OUT=${1:-${GROUPS_OUT:-}}
: "${GROUPS_OUT:?pass a folder on /groups that you own as the first argument}"
SCRATCHDIR=${SCRATCHDIR:-/scratch/users/$USER}
id=${SLURM_JOB_ID:-manual}
RES=$SCRATCHDIR/gpu_probe/tm_ab_$id # results: runs.tsv, dmon.log, one log per run
G=$GROUPS_OUT/tm_ab_$id             # outputs on /groups
T=${TMPDIR:-/tmp}/tm_ab_$id         # outputs, caches and scratch space on the job's private /tmp
mkdir -p "$RES" "$G" "$T"
: >"$G/.gpu_probe_marker"

sif=${SIF:-/groups/klumpe/software/containers/sifs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.sif}
cuda=/groups/klumpe/software/cache_and_artifacts/cuda_kernels/$(basename "$sif" .sif)

# The tomogram, template, mask and metadata run_case.sh of roadmap 43 S1 used; every flag the crboost driver passes.
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

run() { # label, cupy cache dir, output dir, angle file
    local label=$1 cupy=$2 out=$3 angles=$4 t0 t1
    mkdir -p "$out" "$cupy"
    t0=$(date +%s.%N)
    if [ -n "${PROBE_DRY:-}" ]; then
        echo "DRY $label cupy=$cupy out=$out angles=$angles"
    else
        apptainer exec --nv --cleanenv -B /groups -B /programs -B /scratch -B /software -B /tmp "$sif" bash -c \
            "export XDG_CACHE_HOME=$T/xdg CUPY_CACHE_DIR=$cupy CUDA_CACHE_PATH=$cuda CUDA_CACHE_MAXSIZE=4294967296 \
             MPLCONFIGDIR=$T/mpl TORCHINDUCTOR_CACHE_DIR=$T/inductor TRITON_CACHE_DIR=$T/triton \
             NUMBA_CACHE_DIR=$T/numba TQDM_DISABLE=1; \
             pytom_match_template.py $common --angular-search $angles -d $out" >"$RES/$label.log" 2>&1 ||
            echo "FAILED $label (see $label.log)" | tee -a "$RES/failed.txt"
    fi
    t1=$(date +%s.%N)
    printf '%s\t%s\t%s\t%s\n' "$label" "$t0" "$t1" "$(ls "$cupy" 2>/dev/null | wc -l)" >>"$RES/runs.tsv"
    rm -f "$out"/*_scores.mrc "$out"/*_angles.mrc # this run's maps only, in the probe's own folders
}

n=0
pair() { # nfs|tmp: a cold run, then a warm run that reuses the directory the cold one filled
    local where=$1 out
    n=$((n + 1))
    if [ "$where" = nfs ]; then out=$G/out_$n; else out=$T/out_$n; fi
    run "cold_$where" "$T/cupy_$n" "$out" "$RES/angles_short.txt"
    run "warm_$where" "$T/cupy_$n" "$out" "$RES/angles_short.txt"
}

if [ -z "${PROBE_DRY:-}" ]; then
    stdbuf -oL nvidia-smi dmon -s um -d 1 -o T >"$RES/dmon.log" 2>&1 &
    sampler=$!
    trap 'kill $sampler 2>/dev/null || true' EXIT
    # The same page-cache state for every run (production reads the tomogram cold, which is a second or two).
    cat "$vol" "${tpl}_black.mrc" "${tpl}_sphere_d575_s5.mrc" >/dev/null
fi

echo "== first container start on this node, and which GPUs a --cleanenv container sees"
show='echo "inside: CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}"; nvidia-smi -L'
c0=$(date +%s.%N)
if [ -z "${PROBE_DRY:-}" ]; then
    apptainer exec --nv --cleanenv -B /groups "$sif" bash -c "$show" | tee "$RES/container_check.txt"
fi
c1=$(date +%s.%N)
echo "host: CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}" | tee -a "$RES/container_check.txt"
awk -v a="$c0" -v b="$c1" 'BEGIN { printf "container start: %.1f s\n", b - a }' | tee -a "$RES/container_check.txt"

pair nfs
pair tmp
pair tmp
pair nfs

# The full angular search: the search time itself, in the production setting and in the best one.
n=$((n + 1))
run full_cold_nfs "$T/cupy_$n" "$G/out_full" "$j/angles_I1.txt"
run full_warm_tmp "$T/cupy_$n" "$T/out_full" "$j/angles_I1.txt"

[ -e "$G/.gpu_probe_marker" ] && rm -rf "$G"
echo "done: $RES"
echo "next: python3 docs/reports/gpu-util/scripts/summarise_tm_ab.py $RES"

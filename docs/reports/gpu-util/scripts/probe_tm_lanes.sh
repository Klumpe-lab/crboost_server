#!/bin/bash
# Probe P7b of roadmap 45: how many template-matching lanes should one B200 run, and does it need MPS?
#
# P7 ran four processes at once from the same instant: 1.24x one process, 1.45x under MPS, against a ceiling of
# 1.75x (the search alone, 63.5 s a tomogram). In production a task would run lanes: each lane takes tomograms one
# after another, so after the first the set-ups of different lanes fall apart in time. This runs the production
# command (1 FFT worker, as crboost runs it) on the 412 tomogram in lanes, each lane on its own 4 CPUs, later lanes
# starting 30 s apart (one set-up):
#   seq     1 lane x 2 tomograms, no MPS (the baseline, and the run-to-run check of the maps)
#   l2      2 lanes x 3, no MPS
#   l2_mps  2 lanes x 3, under MPS
#   l3_mps  3 lanes x 2, under MPS
#   l4_mps  4 lanes x 2, under MPS
# The maps of the first baseline tomogram are compared byte for byte with the second baseline's and with one made by
# the fourth lane under MPS (identity.txt): concurrency must not change results.
#
#   sbatch -o /scratch/users/artem.kushner/gpu_probe/probe_tm_lanes-%j.out \
#       /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/probe_tm_lanes.sh
#
# Nothing is written to /groups, which is only read. A process that runs over 15 minutes is stopped and reported.
# PROBE_DRY=1 runs stand-ins (no GPU, no container).
#SBATCH -J cb-probe-tmlanes
#SBATCH -p g
#SBATCH --gres=gpu:nvidia_b200:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 16
#SBATCH --mem=120G
#SBATCH -t 1:10:00
set -euo pipefail

SCRATCHDIR=${SCRATCHDIR:-/scratch/users/$(id -un)}
id=${SLURM_JOB_ID:-manual}
RES=$SCRATCHDIR/gpu_probe/tm_lanes_$id # results: runs.tsv, procs.tsv, one log and one phase log per tomogram, traces
T=${TMPDIR:-/tmp}/tm_lanes_$id         # outputs and caches on the job's private /tmp
mkdir -p "$RES" "$T"

sif=${SIF:-/groups/klumpe/software/containers/sifs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.sif}
cuda=/groups/klumpe/software/cache_and_artifacts/cuda_kernels/$(basename "$sif" .sif)

# The inputs and flags of probe_tm_fft_mps.sh: the 412 tomogram and the command crboost runs.
p=${PROJECT:-/groups/klumpe/crboost_data/AGG412_20251031_412_FCIsolation}
j=$p/External/job019
ts=AGG412_20251031_412_FCIsolation_Position_1
vol=$(readlink -f "$j/tmResults/$ts.mrc")
tid=$(basename "$vol" .mrc) # pytom names its maps after the volume file: <tid>_scores.mrc
tpl=$p/templates/412_particle/run_it015_class001_apix6.20_box128
common="-t ${tpl}_black.mrc -m ${tpl}_sphere_d575_s5.mrc --voltage 300.0 --spherical-aberration 2.7"
common="$common --amplitude-contrast 0.1 --tomogram-ctf-model phase-flip --per-tilt-weighting --log debug"
common="$common --tilt-angles $j/tiltAngleFiles/$ts.tlt --defocus $j/defocusFiles/$ts.txt"
common="$common --dose-accumulation $j/doseFiles/$ts.txt -v $vol -g 0 -s 1 1 1"
short=$RES/angles_short.txt
head -n 48 "$j/angles_I1.txt" >"$short"
full=$j/angles_I1.txt

# pytom_match_template.py with scipy.fft's default worker count set, logging its phases (name, detail, start, end).
cat >"$RES/tm_timed.py" <<'EOF'
import functools
import os
import sys
import time

T_START = time.time()
import scipy.fft  # noqa: E402

LOG = os.environ["TM_PHASE_LOG"]
WORKERS = int(os.environ.get("TM_FFT_WORKERS", "1"))


def mark(name, detail, t0, t1):
    with open(LOG, "a") as f:
        f.write(f"{name}\t{detail}\t{t0:.3f}\t{t1:.3f}\n")


def timed(owner, attr, name):
    fn = getattr(owner, attr)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        t0 = time.time()
        try:
            return fn(*args, **kwargs)
        finally:
            first = args[0] if args else None
            if isinstance(first, tuple):
                detail = "x".join(map(str, first))
            elif hasattr(first, "shape"):
                detail = "x".join(map(str, first.shape))
            elif isinstance(first, (str, os.PathLike)):
                detail = os.path.basename(str(first))
            else:
                detail = ""
            mark(name, detail, t0, time.time())

    setattr(owner, attr, wrapper)


from pytom_tm import entry_points, tmjob  # noqa: E402

mark("imports", "", T_START, time.time())
timed(tmjob, "read_mrc", "read_mrc")
timed(tmjob, "create_wedge", "create_wedge")
timed(tmjob, "rfftn", "rfftn")
timed(tmjob, "irfftn", "irfftn")
timed(tmjob.TMJob, "__init__", "tmjob_init")
timed(tmjob.TemplateMatchingGPU, "__init__", "gpu_plan")
timed(tmjob.TemplateMatchingGPU, "run", "gpu_search")
timed(entry_points, "write_mrc", "write_mrc")
timed(tmjob.TMJob, "write_to_json", "write_json")
sys.argv[0] = "pytom_match_template.py"
with scipy.fft.set_workers(WORKERS):
    entry_points.match_template()
mark("total", WORKERS, T_START, time.time())
EOF

# Stand-in for a run: 1 s of start-up, 2 s busy, with the phase lines a real run writes.
standin='import os, time
t = time.time(); time.sleep(1); a = time.time(); end = a + 2
while time.time() < end: pass
with open(os.environ["TM_PHASE_LOG"], "a") as f:
    f.write(f"imports\t\t{t:.3f}\t{a:.3f}\ngpu_search\t\t{a:.3f}\t{end:.3f}\ntotal\t1\t{t:.3f}\t{end:.3f}\n")'

# The job's CPUs in four groups of four; lane i runs on group i.
mapfile -t cpus < <(taskset -pc $$ | sed 's/.*: //' | tr ',' '\n' |
    awk -F- '{ if (NF == 2) for (i = $1; i <= $2; i++) print i; else print $1 }')
if [ "${#cpus[@]}" -lt 16 ]; then
    echo "needs 16 CPUs, has ${#cpus[@]}" >&2
    exit 1
fi
group=()
for g in 1 2 3 4; do group[g]=$(printf '%s\n' "${cpus[@]:$(((g - 1) * 4)):4}" | paste -sd,); done
printf 'group %s: %s\n' 1 "${group[1]}" 2 "${group[2]}" 3 "${group[3]}" 4 "${group[4]}" | tee "$RES/cpus.txt"

binds="-B /groups -B /programs -B /scratch -B /software -B /tmp"
mps_env=""
one() { # label, angle file, cpu list, keep: one tomogram with its own output folder
    local label=$1 angles=$2 cpus=$3 keep=${4:-} t0 t1
    mkdir -p "$T/out_$label"
    t0=$(date +%s.%N)
    if [ -n "${PROBE_DRY:-}" ]; then
        TM_PHASE_LOG=$RES/$label.phases.tsv taskset -c "$cpus" python3 -c "$standin"
    else
        timeout 900 taskset -c "$cpus" apptainer exec --nv --cleanenv $binds "$sif" bash -c \
            "export XDG_CACHE_HOME=$T/xdg CUPY_CACHE_DIR=$T/cupy CUDA_CACHE_PATH=$cuda CUDA_CACHE_MAXSIZE=4294967296 \
             MPLCONFIGDIR=$T/mpl TORCHINDUCTOR_CACHE_DIR=$T/inductor TRITON_CACHE_DIR=$T/triton \
             NUMBA_CACHE_DIR=$T/numba TQDM_DISABLE=1 TM_PHASE_LOG=$RES/$label.phases.tsv TM_FFT_WORKERS=1 \
             $mps_env; \
             python3 $RES/tm_timed.py $common --angular-search $angles -d $T/out_$label" >"$RES/$label.log" 2>&1 ||
            echo "FAILED $label (see $label.log)" | tee -a "$RES/failed.txt"
    fi
    t1=$(date +%s.%N)
    printf '%s\t%s\t%s\t%s\n' "$label" "$cpus" "$t0" "$t1" >>"$RES/procs.tsv"
    if [ -n "$keep" ] && [ -z "${PROBE_DRY:-}" ]; then
        mv "$T/out_$label/${tid}_scores.mrc" "$T/keep_${label}_scores.mrc" || true
        mv "$T/out_$label/${tid}_angles.mrc" "$T/keep_${label}_angles.mrc" || true
    fi
    rm -rf "$T/out_$label"
}

keep_list=" seq_l1_t1 seq_l1_t2 l4_mps_l4_t2 " # the maps identity() compares
lane() { # arm, lane, tomograms, cpu list, delay s: one lane runs its tomograms one after another
    local arm=$1 i=$2 n=$3 cpus=$4 delay=$5 k label
    sleep "$delay"
    for k in $(seq 1 "$n"); do
        label=${arm}_l${i}_t$k
        if [[ $keep_list == *" $label "* ]]; then one "$label" "$full" "$cpus" keep; else one "$label" "$full" "$cpus"; fi
    done
}

arm() { # label, lanes, tomograms per lane: the line in runs.tsv spans the first start to the last end
    local label=$1 lanes=$2 per=$3 t0 t1 i pids=()
    t0=$(date +%s.%N)
    for i in $(seq 1 "$lanes"); do
        lane "$label" "$i" "$per" "${group[i]}" $(((i - 1) * 30)) &
        pids+=($!)
    done
    wait "${pids[@]}" # these only: a bare wait would also wait for the samplers, which never end
    t1=$(date +%s.%N)
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$((lanes * per))" "$lanes" "$t0" "$t1" \
        "$(cut -d' ' -f1-3 /proc/loadavg)" >>"$RES/runs.tsv"
}

identity() { # byte for byte, then the largest difference if they differ; run against run is the baseline
    local pair kind a b
    for pair in seq_l1_t1:seq_l1_t2 seq_l1_t1:l4_mps_l4_t2; do
        for kind in scores angles; do
            a=$T/keep_${pair%:*}_$kind.mrc
            b=$T/keep_${pair#*:}_$kind.mrc
            if [ ! -e "$a" ] || [ ! -e "$b" ]; then
                echo "$pair $kind: not compared (a map is missing)"
            elif cmp -s "$a" "$b"; then
                echo "$pair $kind: identical byte for byte"
            else
                apptainer exec --cleanenv $binds "$sif" python3 -c \
                    'import sys, mrcfile, numpy as np
a, b = mrcfile.read(sys.argv[1]), mrcfile.read(sys.argv[2])
d = np.abs(a.astype(np.float64) - b)
print(f"{sys.argv[3]}: differ in {int((a != b).sum())} voxels, largest difference {d.max():.3g} (range {a.min():.3g} to {a.max():.3g})")' \
                    "$a" "$b" "$pair $kind"
            fi
        done
    done
    rm -f "$T"/keep_*.mrc
}

mps_dir=/dev/shm/mps_$id # the daemon's pipes and logs: node-local, as MPS needs
mps_on() {
    export CUDA_MPS_PIPE_DIRECTORY=$mps_dir/pipe CUDA_MPS_LOG_DIRECTORY=$mps_dir/log
    mkdir -p "$CUDA_MPS_PIPE_DIRECTORY" "$CUDA_MPS_LOG_DIRECTORY"
    if ! nvidia-cuda-mps-control -d; then
        echo "FAILED mps: the MPS control daemon did not start" | tee -a "$RES/failed.txt"
        return 1
    fi
    mps_env="CUDA_MPS_PIPE_DIRECTORY=$CUDA_MPS_PIPE_DIRECTORY CUDA_MPS_LOG_DIRECTORY=$CUDA_MPS_LOG_DIRECTORY"
}
mps_off() {
    [ -n "${CUDA_MPS_PIPE_DIRECTORY:-}" ] || return 0
    echo quit | nvidia-cuda-mps-control || true
    sleep 2
    cp -r "$mps_dir/log" "$RES/mps_log" 2>/dev/null || true
    rm -rf "$mps_dir"
    unset CUDA_MPS_PIPE_DIRECTORY CUDA_MPS_LOG_DIRECTORY
    mps_env=""
}

pick_gpu='/^\| [0-9]+ / { id = $2 } /Device UUID/ && index($0, u) { print id }' # the GPU ID row, then its UUID row
sampler=""
dcgm=""
if [ -z "${PROBE_DRY:-}" ]; then
    stdbuf -oL nvidia-smi dmon -s um -d 1 -o T >"$RES/dmon.log" 2>&1 &
    sampler=$!
    # SM active, SM occupancy, tensor pipe active, DRAM active, once a second. dcgmi sees all 8 GPUs of the node;
    # -i keeps ours, found by the UUID in CUDA_VISIBLE_DEVICES.
    env -u CUDA_VISIBLE_DEVICES dcgmi discovery -l >"$RES/dcgm_discovery.txt" 2>&1 || true
    gid=$(awk -v u="${CUDA_VISIBLE_DEVICES:-none}" "$pick_gpu" "$RES/dcgm_discovery.txt")
    echo "${gid:-all}" >"$RES/dcgm_gpu.txt"
    date +%s.%N >"$RES/dcgm_start.txt"
    env -u CUDA_VISIBLE_DEVICES dcgmi dmon ${gid:+-i "$gid"} -e 1002,1003,1004,1005 -d 1000 >"$RES/dcgm.log" 2>&1 &
    dcgm=$!
    cat "$vol" "${tpl}_black.mrc" "${tpl}_sphere_d575_s5.mrc" >/dev/null # the same page-cache state for every run
else
    sleep 600 & # stands in for the samplers, so that a wait which also waits for them would show up as a hang
    sampler=$!
fi
trap 'mps_off; kill $sampler $dcgm 2>/dev/null || true' EXIT

one prime "$short" "${group[1]}" # fills the kernel caches; not part of the table
arm seq 1 2
arm l2 2 3
if [ -n "${PROBE_DRY:-}" ] || mps_on; then
    arm l2_mps 2 3
    arm l3_mps 3 2
    arm l4_mps 4 2
    mps_off
fi
identity | tee "$RES/identity.txt" || true

echo "done: $RES"
echo "next: python3 -B /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/summarise_tm_lanes.py $RES"

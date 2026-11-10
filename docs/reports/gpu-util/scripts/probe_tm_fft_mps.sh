#!/bin/bash
# Probe P7 of roadmap 45: is the template-matching preamble pytom's CPU filter of the whole tomogram, how much does
# it vary from run to run, and what do MPS and one FFT thread per CPU give four processes on one B200?
#
# P5 and P6 found a template-matching process on one core for 30-70 s before its first kernel, the same with 4 or 16
# CPUs. pytom 0.14's TMJob.start_job filters the whole padded tomogram on the CPU before the search (create_wedge at
# full size, then scipy.fft.rfftn and irfftn of 1024 x 1024 x 512), and scipy.fft uses one thread unless told
# otherwise. Each run here is the production command through tm_timed.py, which sets scipy.fft's worker count for
# the process and logs when each of those calls, the GPU plan, the search and the writes start and end:
#   one process with 1 FFT worker (today), one with 4, 4, 1, in mirrored order, each pinned to 4 CPUs;
#   four processes at once, 4 FFT workers and 4 CPUs each;
#   with the argument mps, the same four under an MPS daemon on the job's GPU, so kernels of different processes can
#   run side by side.
# The maps of the first 1-worker run are compared byte for byte with the first 4-worker run's and, as the baseline,
# with the second 1-worker run's (identity.txt).
#
#   sbatch -o /scratch/users/artem.kushner/gpu_probe/probe_tm_fft-%j.out \
#       /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/probe_tm_fft_mps.sh mps
#
# Nothing is written to /groups, which is only read. A process that runs over 15 minutes is stopped and reported.
# PROBE_DRY=1 runs stand-ins (no GPU, no container).
#SBATCH -J cb-probe-tmfft
#SBATCH -p g
#SBATCH --gres=gpu:nvidia_b200:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 16
#SBATCH --mem=120G
#SBATCH -t 1:00:00
set -euo pipefail

arms=${1:-} # "mps" adds the MPS batch
SCRATCHDIR=${SCRATCHDIR:-/scratch/users/$(id -un)}
id=${SLURM_JOB_ID:-manual}
RES=$SCRATCHDIR/gpu_probe/tm_fft_$id # results: runs.tsv, procs.tsv, one log and one phase log per process, traces
T=${TMPDIR:-/tmp}/tm_fft_$id         # outputs and caches on the job's private /tmp
mkdir -p "$RES" "$T"

sif=${SIF:-/groups/klumpe/software/containers/sifs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.sif}
cuda=/groups/klumpe/software/cache_and_artifacts/cuda_kernels/$(basename "$sif" .sif)

# The inputs and flags of probe_tm_concurrency.sh: the 412 tomogram and the command crboost runs.
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
    f.write(f"imports\t\t{t:.3f}\t{a:.3f}\nrfftn\t1024x1024x512\t{a:.3f}\t{a + 1:.3f}\n")
    f.write(f"gpu_search\t\t{a + 1:.3f}\t{end:.3f}\ntotal\t1\t{t:.3f}\t{end:.3f}\n")'

# The job's CPUs in four groups of four; process i of a batch runs on group i.
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
one() { # label, angle file, FFT workers, cpu list, keep: one process with its own output folder
    local label=$1 angles=$2 workers=$3 cpus=$4 keep=${5:-} t0 t1
    mkdir -p "$T/out_$label"
    t0=$(date +%s.%N)
    if [ -n "${PROBE_DRY:-}" ]; then
        TM_PHASE_LOG=$RES/$label.phases.tsv taskset -c "$cpus" python3 -c "$standin"
    else
        timeout 900 taskset -c "$cpus" apptainer exec --nv --cleanenv $binds "$sif" bash -c \
            "export XDG_CACHE_HOME=$T/xdg CUPY_CACHE_DIR=$T/cupy CUDA_CACHE_PATH=$cuda CUDA_CACHE_MAXSIZE=4294967296 \
             MPLCONFIGDIR=$T/mpl TORCHINDUCTOR_CACHE_DIR=$T/inductor TRITON_CACHE_DIR=$T/triton \
             NUMBA_CACHE_DIR=$T/numba TQDM_DISABLE=1 TM_PHASE_LOG=$RES/$label.phases.tsv TM_FFT_WORKERS=$workers \
             $mps_env; \
             python3 $RES/tm_timed.py $common --angular-search $angles -d $T/out_$label" >"$RES/$label.log" 2>&1 ||
            echo "FAILED $label (see $label.log)" | tee -a "$RES/failed.txt"
    fi
    t1=$(date +%s.%N)
    printf '%s\t%s\t%s\t%s\t%s\n' "$label" "$workers" "$cpus" "$t0" "$t1" >>"$RES/procs.tsv"
    if [ -n "$keep" ] && [ -z "${PROBE_DRY:-}" ]; then
        mv "$T/out_$label/${tid}_scores.mrc" "$T/keep_${label}_scores.mrc" || true
        mv "$T/out_$label/${tid}_angles.mrc" "$T/keep_${label}_angles.mrc" || true
    fi
    rm -rf "$T/out_$label"
}

batch() { # label, N, FFT workers, keep: N processes at once; the line in runs.tsv spans first start to last end
    local label=$1 n=$2 workers=$3 keep=${4:-} t0 t1 i pids=()
    t0=$(date +%s.%N)
    for i in $(seq 1 "$n"); do
        one "${label}_p$i" "$full" "$workers" "${group[i]}" "$keep" &
        pids+=($!)
    done
    wait "${pids[@]}" # these only: a bare wait would also wait for the samplers, which never end
    t1=$(date +%s.%N)
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$n" "$workers" "$t0" "$t1" "$(cut -d' ' -f1-3 /proc/loadavg)" \
        >>"$RES/runs.tsv"
}

identity() { # byte for byte, then the largest difference if they differ; run against run is the baseline
    local pair kind a b
    for pair in w1_a:w4_a w1_a:w1_b; do
        for kind in scores angles; do
            a=$T/keep_${pair%:*}_p1_$kind.mrc
            b=$T/keep_${pair#*:}_p1_$kind.mrc
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
mps_check() { # after the four have started: the MPS server and which processes hold the card
    sleep 60
    {
        echo "== get_server_list"
        echo get_server_list | nvidia-cuda-mps-control
        echo "== nvidia-smi compute apps"
        nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv
    } >"$RES/mps_check.txt" 2>&1 || true
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

one prime "$short" 4 "${group[1]}" # fills the kernel caches; not part of the table
batch w1_a 1 1 keep
batch w4_a 1 4 keep
batch w4_b 1 4
batch w1_b 1 1 keep
identity | tee "$RES/identity.txt" || true
batch n4 4 4
if [ "$arms" = mps ] && [ -z "${PROBE_DRY:-}" ] && mps_on; then
    mps_check &
    checker=$!
    batch n4_mps 4 4
    wait "$checker"
    mps_off
fi

echo "done: $RES"
echo "next: python3 -B /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/summarise_tm_fft.py $RES"

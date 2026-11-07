#!/bin/bash
# Probe P4 of roadmap 45: what does a node get when sixteen tasks write and read at once, the way an array of
# template-matching tasks does? CPU only, no GPU. Each task writes one file of PROBE_MB (default 1024 MiB) to
#   the same /groups folder as the other tasks, its own /groups subfolder, the job's private /tmp, and $SCRATCHDIR,
# reads each back with O_DIRECT (no client cache), and reads one real score map of a production job out of the
# folder where all of them sit. A barrier before each phase makes the sixteen tasks do the same thing together.
#
#   export GROUPS_OUT=/groups/klumpe/user/<you>/_gpu_probe      # a folder you own; ~16 GiB are written there
#   sbatch -o probe_io-%A_%a.out docs/reports/gpu-util/scripts/probe_io.sh
#   python3 docs/reports/gpu-util/scripts/summarise_io.py .      # in the folder holding the probe_io-*.out files
#
# Task 0 deletes the folder the run made under $GROUPS_OUT, and only that one, only if it holds its marker file.
# PROBE_SMOKE=1 runs one tiny task in the foreground (a 16 MiB file) to check the script itself.
#SBATCH -J cb-probe-io
#SBATCH -p g
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 2
#SBATCH --mem=8G
#SBATCH -t 0:20:00
#SBATCH --array=0-15
set -euo pipefail

: "${GROUPS_OUT:?set GROUPS_OUT to a folder on /groups that you own}"
SCRATCHDIR=${SCRATCHDIR:-/scratch/users/$USER}
if [ -n "${PROBE_SMOKE:-}" ]; then
    N=1 I=0 MB=16 run=smoke
else
    N=16 I=$SLURM_ARRAY_TASK_ID MB=${PROBE_MB:-1024} run=$SLURM_ARRAY_JOB_ID
fi
G=$GROUPS_OUT/io_$run                  # /groups
S=$SCRATCHDIR/gpu_probe/io_$run        # $SCRATCHDIR; the barrier files live here too
T=${TMPDIR:-/tmp}                      # the job's private /tmp
maps=${MAPS:-/groups/klumpe/crboost_data/AGG412_20251113_412/External/job019/tmResults}
mkdir -p "$G/shared" "$G/sub_$I" "$S/barrier" "$S/f_$I"
: >"$G/.gpu_probe_marker"

barrier() { # wait until all N tasks have reached <name>, for at most 300 s
    : >"$S/barrier/$1.$I"
    local waited=0
    while [ "$(ls "$S/barrier" | grep -c "^$1\.")" -lt "$N" ] && [ "$waited" -lt 300 ]; do
        sleep 1
        waited=$((waited + 1))
    done
    [ "$waited" -ge 300 ] && echo "NOTE barrier $1 timed out: not all $N tasks ran at once" >&2
    return 0
}

timed() { # phase name, then the command: one RESULT line with start, end and MB/s
    local name=$1 t0 t1
    shift
    t0=$(date +%s.%N)
    "$@" >/dev/null 2>&1 || echo "FAILED $name" >&2
    t1=$(date +%s.%N)
    awk -v n="$name" -v i="$I" -v h="$(hostname -s)" -v a="$t0" -v b="$t1" -v mb="$MB" \
        'BEGIN { printf "RESULT\t%s\t%d\t%s\t%.3f\t%.3f\t%.1f\n", n, i, h, a, b, mb / (b - a) }'
}

src=$T/probe_src_$I
head -c $((MB * 1048576)) /dev/urandom >"$src" # incompressible bytes, made on the node's own /tmp
write() { dd if="$src" of="$1" bs=16M conv=fsync; }
fetch() { dd if="$1" of=/dev/null bs=16M iflag=direct; }
real=$(ls "$maps"/*_scores.mrc | sed -n "$((I + 1))p")

echo "task $I of $N on $(hostname -s), $MB MiB per file, run $run"
barrier w1; timed write_groups_shared write "$G/shared/f_$I"
barrier w2; timed write_groups_own write "$G/sub_$I/f"
barrier w3; timed write_tmp write "$T/probe_dst_$I"
barrier w4; timed write_scratch write "$S/f_$I/f"
barrier r1; timed read_groups_shared fetch "$G/shared/f_$I"
barrier r2; timed read_groups_own fetch "$G/sub_$I/f"
barrier r3; timed read_tmp fetch "$T/probe_dst_$I"
barrier r4; timed read_scratch fetch "$S/f_$I/f"
barrier r5; timed read_real_maps_one_folder dd if="$real" of=/dev/null bs=16M count=$((MB / 16)) iflag=direct
barrier done

rm -f "$src" "$T/probe_dst_$I" "$S/f_$I/f" "$G/sub_$I/f" "$G/shared/f_$I"
if [ "$I" -eq 0 ] && [ -e "$G/.gpu_probe_marker" ]; then
    [ -n "${PROBE_SMOKE:-}" ] || sleep 20 # the other tasks are done with $G by then
    rm -rf "$G"
fi

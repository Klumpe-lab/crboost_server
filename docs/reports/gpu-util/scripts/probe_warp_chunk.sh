#!/bin/bash
#SBATCH -J cb-probe-warpchunk
#SBATCH -p g
#SBATCH --gres=gpu:nvidia_b200:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 8
#SBATCH --mem=96G
#SBATCH -t 01:00:00
# P10 (roadmap 45 L5; P5 for Warp): Warp reconstruct in chunks. N tilt series of the 412 pilot (read only) through
# WarpTools ts_reconstruct one call each, as production runs them, then all N in one call with 1, 2 and 4 workers on
# the card (--perdevice); on a full B200 and on a MIG slice.
#
#   sbatch -o /scratch/users/artem.kushner/gpu_probe/probe_warp_chunk-%j.out /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/probe_warp_chunk.sh 8 one,1,2,4
#   sbatch --gres=gpu:nvidia_b200_1g.23gb:1 -t 01:30:00 -o /scratch/users/artem.kushner/gpu_probe/probe_warp_chunk-%j.out /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/probe_warp_chunk.sh 4 one,1,2,4
#
# Each arm stages its own folder in scratch the way production stages a task (drivers/array_job_base.py,
# stage_per_ts_environment): job003's tilt-series settings, each tilt series' tomostar with absolute movie paths
# (production's own copy_tomostar_with_absolute_paths), and its CTF-job XML, copied rather than linked. The command
# is the pilot's own (job006's task log) with only --settings, --input_processing, --output_processing, --task_dir
# and --perdevice changed, so Warp writes only into the arm's folders. Before any arm, every frame-series average the
# N tilt series read is read once (timed: what a cold read from /groups costs), so all arms read from the page cache.
# "one" runs first and is the reference: each later arm's tomograms are compared with it (centre crop) and deleted.
# On a B200 a 1 Hz nvidia-smi dmon runs beside it. Results in
# /scratch/users/artem.kushner/gpu_probe/warp_chunk_<jobid>; then python3 -B summarise_warp_chunk.py <that folder>.
set -uo pipefail
N=${1:?how many tilt series}
ARMS=${2:?arms, e.g. one,1,2,4}
REPO=/users/artem.kushner/dev/crboost_server
PROJ=/groups/klumpe/crboost_data/slurmtest_412_FCIsolation
J3=$PROJ/External/job003
J5=$PROJ/External/job005
J6=$PROJ/External/job006
SIF=/groups/klumpe/software/containers/sifs/warp-2.0.0dev41_aretomo2-1.0.0_cuda12.9_sm100.sif
RES=/scratch/users/artem.kushner/gpu_probe/warp_chunk_${SLURM_JOB_ID}
CACHE=${TMPDIR:-/tmp}/probe_cache_${SLURM_JOB_ID}
CRB_PY=$("$REPO/crboost" env)/bin/python3
mkdir -p "$RES/logs" "$RES/out" "$RES/stage" "$CACHE"
echo "[SLURM] job ${SLURM_JOB_ID} on $(hostname -s), GPU ${CUDA_VISIBLE_DEVICES:-none}, started $(date '+%F %T')"
nvidia-smi -L | tee "$RES/gpu.txt"

run() {  # $1: a command line in the Warp container with production's environment, caches in this job's /tmp
  apptainer exec --nv --cleanenv -B /groups -B /scratch -B /tmp "$SIF" bash -c \
    "export XDG_CACHE_HOME=$CACHE CUDA_CACHE_PATH=$CACHE/cuda CUDA_CACHE_MAXSIZE=4294967296 TQDM_DISABLE=1; $1" </dev/null
}
crb_py() {  # production's Python with the repo importable
  PYTHONPATH=$REPO PYTHONNOUSERSITE=1 "$CRB_PY" -B "$@"
}
stage() {  # $1: stage root, then tilt-series names
  local root=$1
  shift
  mkdir -p "$root/tomostar" "$root/warp_tiltseries"
  cp "$J3/warp_tiltseries.settings" "$root/"
  crb_py -c '
import sys
from pathlib import Path
from drivers.array_job_base import copy_tomostar_with_absolute_paths
src, root = Path(sys.argv[1]), Path(sys.argv[2])
for ts in sys.argv[3:]:
    copy_tomostar_with_absolute_paths(src / f"{ts}.tomostar", root / "tomostar" / f"{ts}.tomostar", src)
' "$J3/tomostar" "$root" "$@"
  for ts in "$@"; do cp "$J5/warp_tiltseries/$ts.xml" "$root/warp_tiltseries/"; done
}
command_for() {  # $1: stage root, $2: output dir, $3: workers per card
  sed -E "s# --settings [^ ]+# --settings $1/warp_tiltseries.settings#; s# --input_processing [^ ]+# --input_processing $1/warp_tiltseries#; s# --output_processing [^ ]+# --output_processing $2#; s# --task_dir [^ ]+# --task_dir $1/tasks#; s# --perdevice [0-9]+# --perdevice $3#" <<<"$PILOT"
}
timed() {  # $1: arm, $2: label, $3: stage root, $4: command line; appends a row to runs.tsv
  local t0 rc
  t0=$(date +%s.%N)
  (cd "$3" && run "$4") >"$RES/logs/$1_$2.log" 2>&1
  rc=$?
  printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$t0" "$(date +%s.%N)" "$rc" >>"$RES/runs.tsv"
  echo "$(date '+%T') $1 $2: exit $rc"
}

PILOT=$(grep -h -m1 -E '^\[TASK [0-9]+\] Command: WarpTools ts_reconstruct ' "$J6/task_0.out" | sed -E 's/^\[TASK [0-9]+\] Command: //')
[ -n "$PILOT" ] || { echo "no ts_reconstruct command in $J6/task_0.out" >&2; exit 1; }
mapfile -t TS < <(ls "$J6/warp_tiltseries/reconstruction/" | sed -n -E 's/_[0-9.]+Apx\.mrc$//p' | sort | head -n "$N")
[ "${#TS[@]}" -eq "$N" ] || { echo "found ${#TS[@]} reconstructed tilt series, wanted $N" >&2; exit 1; }
printf '%s\n' "${TS[@]}" >"$RES/tilt_series.txt"
echo "tilt series: ${TS[*]}"
printf 'arm\tlabel\tstart\tend\texit\n' >"$RES/runs.tsv"

# Every frame-series average (and its even/odd half) these tilt series name, read once, timed.
stage "$RES/stage/files" "${TS[@]}"
crb_py -c '
import sys
from pathlib import Path
for tomostar in Path(sys.argv[1]).glob("*.tomostar"):
    for line in tomostar.read_text().splitlines():
        s = line.strip()
        if s and not s.startswith(("data_", "loop_", "_", "#")):
            movie = Path(s.split()[0])
            for sub in ("", "even/", "odd/"):
                f = movie.parent / "average" / f"{sub}{movie.stem}.mrc"
                if f.exists():
                    print(f)
' "$RES/stage/files/tomostar" >"$RES/inputs.txt"
t0=$(date +%s.%N)
xargs -a "$RES/inputs.txt" cat >/dev/null
printf 'read\t%s files, %s bytes\t%s\t%s\t0\n' "$(wc -l <"$RES/inputs.txt")" \
  "$(xargs -a "$RES/inputs.txt" stat -c %s | awk '{ s += $1 } END { print s }')" "$t0" "$(date +%s.%N)" >>"$RES/runs.tsv"
echo "pre-read: $(wc -l <"$RES/inputs.txt") files"

stage "$RES/stage/warmup" "${TS[0]}"
timed warmup "${TS[0]}" "$RES/stage/warmup" "$(command_for "$RES/stage/warmup" "$RES/out/warmup" 1)"
rm -rf "$RES/out/warmup"

DMON=
if ! grep -q MIG "$RES/gpu.txt"; then
  nvidia-smi dmon -s u -d 1 -o T >"$RES/dmon.log" 2>&1 &
  DMON=$!
fi
IFS=, read -ra ARMLIST <<<"$ARMS"
for arm in "${ARMLIST[@]}"; do
  if [ "$arm" = one ]; then
    for ts in "${TS[@]}"; do
      stage "$RES/stage/one_$ts" "$ts"
      timed one "$ts" "$RES/stage/one_$ts" "$(command_for "$RES/stage/one_$ts" "$RES/out/one" 1)"
    done
    rm -rf "$RES/out/one/reconstruction/even" "$RES/out/one/reconstruction/odd"
    continue
  fi
  stage "$RES/stage/p$arm" "${TS[@]}"
  timed "p$arm" all "$RES/stage/p$arm" "$(command_for "$RES/stage/p$arm" "$RES/out/p$arm" "$arm")"
  if [ -d "$RES/out/one/reconstruction" ]; then
    crb_py - "$RES/out/one/reconstruction" "$RES/out/p$arm/reconstruction" "p$arm" >>"$RES/equivalence.tsv" <<'EOF'
import sys
from pathlib import Path
import mrcfile
import numpy as np
ref_dir, arm_dir, arm = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
for ref in sorted(ref_dir.glob("*.mrc")):
    other = arm_dir / ref.name
    if not other.exists():
        print(f"{arm}\t{ref.name}\tmissing\t\t")
        continue
    with mrcfile.mmap(ref, permissive=True) as a, mrcfile.mmap(other, permissive=True) as b:
        z, y, x = (s // 2 for s in a.data.shape)
        ca = np.asarray(a.data[z - 64 : z + 64, y - 128 : y + 128, x - 128 : x + 128], dtype=np.float64)
        cb = np.asarray(b.data[z - 64 : z + 64, y - 128 : y + 128, x - 128 : x + 128], dtype=np.float64)
        same = "identical" if np.array_equal(ca, cb) else "differs"
        print(f"{arm}\t{ref.name}\t{same}\t{np.corrcoef(ca.ravel(), cb.ravel())[0, 1]:.6f}\t{np.abs(ca - cb).max():.3g}")
EOF
  fi
  rm -rf "$RES/out/p$arm/reconstruction"
done
[ -n "$DMON" ] && kill "$DMON"
rm -rf "$RES/out/one/reconstruction"
echo "done: $RES"
echo "next: python3 -B $REPO/docs/reports/gpu-util/scripts/summarise_warp_chunk.py $RES"

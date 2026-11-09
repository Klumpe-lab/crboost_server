#!/bin/bash
#SBATCH -J cb-probe-tmangles
#SBATCH -p g
#SBATCH --gres=gpu:nvidia_b200:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 01:30:00
# P9 (roadmap 45; HOTFIXES H7): template matching with correct symmetry-reduced angle lists against the list
# production writes today, on tomograms of the 412 pilot (read only): search time, and what the candidates do.
#
#   sbatch -o /scratch/users/artem.kushner/gpu_probe/probe_tm_angles-%j.out /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/probe_tm_angles.sh b200 4 old,12,8,5
#   sbatch --gres=gpu:nvidia_b200_1g.23gb:1 -t 00:45:00 -o /scratch/users/artem.kushner/gpu_probe/probe_tm_angles-%j.out /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/probe_tm_angles.sh slice 2 12,8
#
# Arguments: where it runs (b200: pytom's -s 1 1 1, as production; slice: -s 2 2 1, the split roadmap 43 measured
# for 412 on a slice); how many tomograms (those with the most candidates in the pilot); the arms, in order: "old"
# is the pilot's own list (job007/angles_I1.txt), a number is a correct I list at that increment written by
# tm_angle_lists.py, which first checks with pytom's own rotation which axes the template has and on which side
# its symmetry composes. Each arm runs the pilot's own pytom command for the tomogram (its task log's Command line)
# with only -d, --angular-search and -s changed, then the pilot's own candidate extraction on the result; the arm
# order turns by one per tomogram. A one-rotation run first warms the kernel caches. Results in
# /scratch/users/artem.kushner/gpu_probe/tm_angles_<jobid> (score maps kept, angle maps deleted once extracted);
# nothing is written under /groups. Then: python3 -B summarise_tm_angles.py <that folder>
set -uo pipefail
WHERE=${1:?b200 or slice}
NTOMO=${2:?how many tomograms}
ARMS=${3:?arms, e.g. old,12,8,5}
SCRIPTS=/users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts
PROJ=/groups/klumpe/crboost_data/slurmtest_412_FCIsolation
J7=$PROJ/External/job007
J8=$PROJ/External/job008
SIF=/groups/klumpe/software/containers/sifs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.sif
RES=/scratch/users/artem.kushner/gpu_probe/tm_angles_${SLURM_JOB_ID}
CACHE=${TMPDIR:-/tmp}/probe_cache_${SLURM_JOB_ID}
case $WHERE in
  b200) SPLIT="1 1 1" ;;
  slice) SPLIT="2 2 1" ;;
  *) echo "where: b200 or slice" >&2; exit 2 ;;
esac
mkdir -p "$RES/lists" "$RES/logs" "$RES/pilot" "$CACHE"
echo "[SLURM] job ${SLURM_JOB_ID} on $(hostname -s), GPU ${CUDA_VISIBLE_DEVICES:-none}, started $(date '+%F %T')"
nvidia-smi -L | tee "$RES/gpu.txt"

run() {  # $1: a command line, run in the pytom container with production's cache settings, caches in this job's /tmp
  apptainer exec --nv --cleanenv -B /groups -B /scratch -B /tmp -B "$SCRIPTS" "$SIF" bash -c \
    "export XDG_CACHE_HOME=$CACHE CUPY_CACHE_DIR=$CACHE/cupy CUDA_CACHE_PATH=$CACHE/cuda CUDA_CACHE_MAXSIZE=4294967296 MPLCONFIGDIR=$CACHE/mpl TQDM_DISABLE=1; $1" </dev/null
}
pilot_cmd() {  # $1: job dir, $2: tool, $3: a fixed string only this tomogram's command holds
  grep -h -E "^\[TASK [0-9]+\] Command: $2 " "$1"/task_*.out | grep -F -- "$3" | head -1 | sed -E 's/^\[TASK [0-9]+\] Command: //'
}
with_list() {  # $1: the pilot's pytom command, $2: output dir, $3: angle list
  local c
  c=$(sed -E "s# -d [^ ]+# -d $2#; s# --angular-search [^ ]+# --angular-search $3#; s# -s [0-9]+ [0-9]+ [0-9]+# -s $SPLIT#" <<<"$1")
  grep -q -E ' -s [0-9]+ [0-9]+ [0-9]+' <<<"$c" || c="$c -s $SPLIT"
  echo "$c"
}

python3 -I -B - "$J8/tmResults" "$NTOMO" >"$RES/tomos.txt" <<'EOF'
import glob, sys
counts = []
for f in glob.glob(sys.argv[1] + "/*_particles.star"):
    rows = [ln for ln in open(f) if ln.strip() and ln.lstrip()[0] in "-0123456789."]
    counts.append((len(rows), f.rsplit("/", 1)[1][: -len("_particles.star")]))
for _, name in sorted(counts, reverse=True)[: int(sys.argv[2])]:
    print(name)
EOF
mapfile -t TOMOS <"$RES/tomos.txt"
[ "${#TOMOS[@]}" -gt 0 ] || { echo "no tomograms found in $J8/tmResults" >&2; exit 1; }
echo "tomograms: ${TOMOS[*]}"
for t in "${TOMOS[@]}"; do cp "$J8/tmResults/${t}_particles.star" "$RES/pilot/"; done

first=$(pilot_cmd "$J7" pytom_match_template.py "/${TOMOS[0]}.mrc ")
[ -n "$first" ] || { echo "no pytom command for ${TOMOS[0]} in $J7" >&2; exit 1; }
TEMPLATE=$(grep -o -E ' -t [^ ]+' <<<"$first" | cut -d' ' -f3)
MASK=$(grep -o -E ' -m [^ ]+' <<<"$first" | cut -d' ' -f3)
OLD=$(grep -o -E ' --angular-search [^ ]+' <<<"$first" | cut -d' ' -f3)
INCS=$(tr ',' '\n' <<<"$ARMS" | grep -v -x old | tr '\n' ' ')
pilot_cmd "$J8" pytom_extract_candidates.py "/${TOMOS[0]}_job.json" | grep -o -E -- '--particle-diameter [^ ]+' |
  cut -d' ' -f2 >"$RES/diameter.txt"

echo "lists: template $TEMPLATE, old list $OLD, increments $INCS"
run "python3 -B $SCRIPTS/tm_angle_lists.py $TEMPLATE $MASK $OLD $RES/lists $INCS" 2>&1 | tee "$RES/logs/lists.log"
rc=${PIPESTATUS[0]}
[ "$rc" -eq 3 ] && echo "lists: the template did not say clearly which axes it has; see lists/check.txt" >>"$RES/failed.txt"
[ "$rc" -ne 0 ] && [ "$rc" -ne 3 ] && echo "lists: tm_angle_lists.py exit $rc; only the old list runs" >>"$RES/failed.txt"

echo "0.0 0.0 0.0" >"$RES/lists/one.txt"
mkdir -p "$RES/out/warmup"
t0=$(date +%s.%N)
run "$(with_list "$first" "$RES/out/warmup" "$RES/lists/one.txt")" >"$RES/logs/warmup.log" 2>&1
echo "warm-up (one rotation, cold caches): exit $?, $(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN { printf "%.0f", b - a }') s"
rm -f "$RES"/out/warmup/*.mrc

printf 'arm\ttomo\trotations\tstart\tend\texit\n' >"$RES/runs.tsv"
IFS=, read -ra ARMLIST <<<"$ARMS"
n=${#ARMLIST[@]}
pids=()
for i in "${!TOMOS[@]}"; do
  t=${TOMOS[$i]}
  tm=$(pilot_cmd "$J7" pytom_match_template.py "/$t.mrc ")
  ex=$(pilot_cmd "$J8" pytom_extract_candidates.py "/${t}_job.json")
  for k in $(seq 0 $((n - 1))); do
    arm=${ARMLIST[$(((k + i) % n))]}
    if [ "$arm" = old ]; then list=$RES/lists/angles_old.txt; else list=$RES/lists/angles_I_${arm}.txt; fi
    [ -s "$list" ] || { echo "$arm $t: no list" >>"$RES/failed.txt"; continue; }
    out=$RES/out/$arm
    mkdir -p "$out"
    t0=$(date +%s.%N)
    run "$(with_list "$tm" "$out" "$list")" >"$RES/logs/${arm}_$t.log" 2>&1
    rc=$?
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$arm" "$t" "$(wc -l <"$list")" "$t0" "$(date +%s.%N)" "$rc" >>"$RES/runs.tsv"
    echo "$(date '+%T') $arm $t: exit $rc"
    if [ "$rc" -ne 0 ]; then
      echo "$arm $t: pytom exit $rc" >>"$RES/failed.txt"
      continue
    fi
    (
      run "$(sed -E "s# -j [^ ]+# -j $out/${t}_job.json#" <<<"$ex")" >"$RES/logs/${arm}_${t}_extract.log" 2>&1
      echo $? >"$out/${t}_extract.rc"
      rm -f "$out/${t}_angles.mrc"
    ) &
    pids+=($!)
  done
done
for p in "${pids[@]}"; do wait "$p"; done
grep -L -x 0 "$RES"/out/*/*_extract.rc 2>/dev/null | sed 's/^/extraction failed: /' >>"$RES/failed.txt"
cat "$RES/lists/check.txt" 2>/dev/null
echo "done: $RES"
echo "next: python3 -B $SCRIPTS/summarise_tm_angles.py $RES"

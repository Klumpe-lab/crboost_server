#!/bin/bash
#SBATCH -J cb-probe-tsimport
#SBATCH -p c
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 16
#SBATCH --mem=32G
#SBATCH -t 01:30:00
# P11 (roadmap 45, pipeline shape): where tsImport's 12 minutes go, and whether splitting it helps. The 412 pilot's
# own WarpTools ts_import (job003's command line) over its 163 mdocs, read only: in one call as production runs it,
# then in 4 and in 8 calls at once over shares of the mdocs, each call's wall, user and system seconds; then
# create_settings alone, as production runs it after. Each split's tomostars are compared with the single call's.
#
#   sbatch -o /scratch/users/artem.kushner/gpu_probe/probe_ts_import-%j.out /users/artem.kushner/dev/crboost_server/docs/reports/gpu-util/scripts/probe_ts_import.sh 1,4,8
#
# Every call writes only under its own folder in /scratch/users/artem.kushner/gpu_probe/ts_import_<jobid> (the mdocs
# are copied there; the frame series are read where they are, as production does). The summary is printed at the end.
set -uo pipefail
SPLITS=${1:?splits, e.g. 1,4,8}
PROJ=/groups/klumpe/crboost_data/slurmtest_412_FCIsolation
J3=$PROJ/External/job003
FRAMESERIES=$PROJ/External/job002/warp_frameseries
SIF=/groups/klumpe/software/containers/sifs/warp-2.0.0dev41_aretomo2-1.0.0_cuda12.9_sm100.sif
RES=/scratch/users/artem.kushner/gpu_probe/ts_import_${SLURM_JOB_ID}
mkdir -p "$RES"
echo "[SLURM] job ${SLURM_JOB_ID} on $(hostname -s), $(nproc) CPUs, started $(date '+%F %T')"

run() {  # $1: a command line in the Warp container
  apptainer exec --cleanenv -B /groups -B /scratch -B /tmp "$SIF" bash -c "export TQDM_DISABLE=1; $1" </dev/null
}
timed() {  # $1: folder to run in, $2: command line; writes <folder>/time.txt (wall, user, system seconds)
  local TIMEFORMAT='%R %U %S'
  { time (cd "$1" && run "$2" >"$1/log.txt" 2>&1); } 2>"$1/time.txt"
}

IMPORT=$(grep -h -m1 -o -E "WarpTools ts_import [^)]*" "$J3/run.out")
[ -n "$IMPORT" ] || { echo "no ts_import command in $J3/run.out" >&2; exit 1; }
SETTINGS=$(grep -h -m1 -o -E "WarpTools create_settings --folder_data tomostar [^)]*" "$J3/run.out")
mapfile -t MDOCS < <(ls "$J3/mdoc"/*.mdoc)
echo "${#MDOCS[@]} mdocs; production's call: $IMPORT"
printf 'split\tcall\tmdocs\twall\tuser\tsystem\n' >"$RES/runs.tsv"

IFS=, read -ra SPLITLIST <<<"$SPLITS"
for k in "${SPLITLIST[@]}"; do
  pids=()
  t0=$(date +%s.%N)
  for c in $(seq 0 $((k - 1))); do
    dir=$RES/split$k/call$c
    mkdir -p "$dir/mdoc"
    for i in "${!MDOCS[@]}"; do
      [ $((i % k)) -eq "$c" ] && cp "${MDOCS[$i]}" "$dir/mdoc/"
    done
    cmd=$(sed -E "s# --mdocs [^ ]+# --mdocs $dir/mdoc#; s# --frameseries [^ ]+# --frameseries $FRAMESERIES#" <<<"$IMPORT")
    timed "$dir" "$cmd" &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p"; done
  printf '%s\tall\t%s\t%s\t\t\n' "$k" "${#MDOCS[@]}" "$(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN { printf "%.1f", b - a }')" >>"$RES/runs.tsv"
  for c in $(seq 0 $((k - 1))); do
    dir=$RES/split$k/call$c
    printf '%s\t%s\t%s\t%s\n' "$k" "$c" "$(ls "$dir/mdoc" | wc -l)" "$(tr ' ' '\t' <"$dir/time.txt")" >>"$RES/runs.tsv"
  done
  echo "$(date '+%T') split $k done"
done

first=$RES/split${SPLITLIST[0]}/call0
if [ -n "$SETTINGS" ]; then
  timed "$first" "$SETTINGS"
  printf 'settings\tcreate_settings\t\t%s\n' "$(tr ' ' '\t' <"$first/time.txt")" >>"$RES/runs.tsv"
fi

echo
echo "| Split | Call | Mdocs | Wall s | User s | System s |"
echo "|---|---|---:|---:|---:|---:|"
awk -F'\t' 'NR > 1 { printf "| %s | %s | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5, $6 }' "$RES/runs.tsv"
echo
ref=$RES/split${SPLITLIST[0]}
for k in "${SPLITLIST[@]:1}"; do
  same=0 differ=0 missing=0
  for f in "$ref"/call*/tomostar/*.tomostar; do
    other=$(ls "$RES/split$k"/call*/tomostar/"$(basename "$f")" 2>/dev/null | head -1)
    if [ -z "$other" ]; then missing=$((missing + 1))
    elif cmp -s "$f" "$other"; then same=$((same + 1))
    else differ=$((differ + 1)); fi
  done
  echo "split $k against split ${SPLITLIST[0]}: $same tomostars identical, $differ differ, $missing missing"
done
echo "tomostars from the single call: $(ls "$ref"/call*/tomostar/*.tomostar 2>/dev/null | wc -l)"
echo "done: $RES"

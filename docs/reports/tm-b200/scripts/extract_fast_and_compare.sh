#!/bin/bash
# extract_and_compare.sh with extract_fast.py in place of pytom_extract_candidates.py, four cases at a time:
# the original extracts at ~7 s per candidate (~2 h per copia case) and would reach compare.py only after
# its 4 h limit. Same production settings per project; a case that already has its star file is skipped.
#SBATCH -p g
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:nvidia_b200_1g.23gb:1
#SBATCH -c 8
#SBATCH --mem=96G
#SBATCH -t 2:00:00
#SBATCH -o /scratch/users/artem.kushner/tm_bench/logs/extract_fast_and_compare_%j.log
set -uo pipefail
root=/scratch/users/artem.kushner/tm_bench
repo=/users/artem.kushner/dev/crboost_server
here=$repo/docs/reports/tm-b200/scripts
sif=/groups/klumpe/software/containers/sifs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.sif
export APPTAINERENV_CUPY_CACHE_DIR=$root/.cupy
running=0
for tomo in copia 412; do
    case $tomo in
        copia) n=1500 d=550.0 ;;
        412) n=20 d=575.0 ;;
    esac
    for job in "$root"/$tomo/*/*_job.json; do
        dir=$(dirname "$job")
        ls "$dir"/*_particles.star > /dev/null 2>&1 && continue
        (
            apptainer exec --nv --cleanenv -B /groups -B /scratch "$sif" python "$here/extract_fast.py" -j "$job" \
                -n $n --particle-diameter $d --number-of-false-positives 1.0 --relion5-compat \
                > "$dir/extract_fast.log" 2>&1 || echo "extract failed: $dir"
        ) &
        running=$((running + 1))
        if [ "$running" -ge 4 ]; then
            wait -n
            running=$((running - 1))
        fi
    done
done
wait
"$repo/venv/bin/python" "$here/compare.py" "$root" "$repo/docs/reports/tm-b200"

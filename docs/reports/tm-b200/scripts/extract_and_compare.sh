#!/bin/bash
# After the S1 matrix: run pytom_extract_candidates with each project's production settings on every
# case, then compare.py. Submitted with --dependency=afterany on the benchmark jobs.
#SBATCH -p g
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:nvidia_b200_1g.23gb:1
#SBATCH -c 8
#SBATCH --mem=96G
#SBATCH -t 4:00:00
#SBATCH -o /scratch/users/artem.kushner/tm_bench/logs/extract_and_compare_%j.log
set -uo pipefail
root=/scratch/users/artem.kushner/tm_bench
repo=/users/artem.kushner/dev/crboost_server
sif=/groups/klumpe/software/containers/sifs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.sif
export APPTAINERENV_CUPY_CACHE_DIR=$root/.cupy
for tomo in copia 412; do
    case $tomo in
        copia) n=1500 d=550.0 ;;
        412) n=20 d=575.0 ;;
    esac
    for job in "$root"/$tomo/*/*_job.json; do
        dir=$(dirname "$job")
        ls "$dir"/*_particles.star > /dev/null 2>&1 && continue
        apptainer exec --nv --cleanenv -B /groups -B /scratch "$sif" pytom_extract_candidates.py -j "$job" \
            -n $n --particle-diameter $d --number-of-false-positives 1.0 --relion5-compat \
            > "$dir/extract.log" 2>&1 || echo "extract failed: $dir"
    done
done
"$repo/venv/bin/python" "$repo/docs/reports/tm-b200/scripts/compare.py" "$root" "$repo/docs/reports/tm-b200"

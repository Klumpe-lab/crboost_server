#!/bin/bash
# One roadmap-43 S1 benchmark case: pytom_match_template on one tomogram with one GPU type, volume
# split, precision and GPU count. Submitted by submit_all.sh, which sets the gres per case.
# Usage: run_case.sh <tomo: copia|412> <split: 221|111> <precision: fp32|half> <gpus: 1|2|4> <gputype> [rp|wh]
# The optional last argument adds --random-phase-correction (rp) or --spectral-whitening (wh): S4 cases.
#SBATCH -p g
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 4:00:00
set -euo pipefail
tomo=$1 split=$2 prec=$3 ngpu=$4 gputype=$5 extra=${6:-}
case_name="${tomo}_${gputype}_s${split}_${prec}_g${ngpu}${extra:+_$extra}"
out=/scratch/users/artem.kushner/tm_bench/$tomo/$case_name
sif=/groups/klumpe/software/containers/sifs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.sif
mkdir -p "$out"

if [ "$tomo" = copia ]; then
    p=/groups/klumpe/crboost_data/copia-tutorial-20261104-1732
    j=$p/External/job007
    ts=copia-tutorial-20261104-1732_Position_1
    vol=$(readlink -f $j/tmResults/$ts.mrc)
    args="-t $p/templates/copia/copia_template.mrc -m $p/templates/copia/copia_mask.mrc --angular-search 90.0"
else
    p=/groups/klumpe/crboost_data/AGG412_20251031_412_FCIsolation
    j=$p/External/job019
    ts=AGG412_20251031_412_FCIsolation_Position_1
    vol=$(readlink -f $j/tmResults/$ts.mrc)
    tpl=$p/templates/412_particle/run_it015_class001_apix6.20_box128
    args="-t ${tpl}_black.mrc -m ${tpl}_sphere_d575_s5.mrc --angular-search $j/angles_I1.txt"
fi
# Every flag the crboost TM driver passes, as in the job's own task log; only -g, -s and precision vary.
args="$args --voltage 300.0 --spherical-aberration 2.7 --amplitude-contrast 0.1"
args="$args --tomogram-ctf-model phase-flip --per-tilt-weighting --log debug"
args="$args --tilt-angles $j/tiltAngleFiles/$ts.tlt --defocus $j/defocusFiles/$ts.txt"
args="$args --dose-accumulation $j/doseFiles/$ts.txt -v $vol -d $out"
args="$args -g $(seq -s ' ' 0 $((ngpu - 1))) -s ${split:0:1} ${split:1:1} ${split:2:1}"
[ "$prec" = half ] && args="$args --half-precision"
[ "$extra" = rp ] && args="$args --random-phase-correction"
[ "$extra" = wh ] && args="$args --spectral-whitening"

echo "case $case_name on $(hostname)"
nvidia-smi --query-gpu=name,memory.total --format=csv,noheader
# Peak GPU memory: the largest used figure any visible device reports, sampled every 2 s.
( peak=0; while sleep 2; do
    m=$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits 2>/dev/null \
        | grep -oE '^[0-9]+' | sort -n | tail -1)
    [ -n "$m" ] && [ "$m" -gt "$peak" ] && peak=$m && echo "$peak" > "$out/peak_mib"
done ) &
sampler=$!

export APPTAINERENV_CUPY_CACHE_DIR=/scratch/users/artem.kushner/tm_bench/.cupy
export APPTAINERENV_CUDA_CACHE_PATH=/groups/klumpe/software/cache_and_artifacts/cuda_kernels/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100
export APPTAINERENV_TQDM_DISABLE=1
t0=$(date +%s)
apptainer exec --nv --cleanenv -B /groups -B /scratch "$sif" pytom_match_template.py $args > "$out/pytom.log" 2>&1
t1=$(date +%s)
kill $sampler 2>/dev/null || true
echo "$case_name wall_s=$((t1 - t0)) peak_mib=$(cat "$out/peak_mib" 2>/dev/null || echo NA)" | tee "$out/result.txt"

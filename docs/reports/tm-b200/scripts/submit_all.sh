#!/bin/bash
# Submit the roadmap-43 S1 matrix: per tomogram, full B200 and MIG 1g.23gb, split 2 2 1 vs 1 1 1,
# fp32 vs --half-precision, and 1/2/4 B200s on one tomogram. The reference is b200_s111_fp32_g1.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
logs=/scratch/users/artem.kushner/tm_bench/logs
mkdir -p "$logs"
sub() {  # tomo split prec ngpu gputype [rp|wh]
    local gres
    case $5 in
        b200) gres=gpu:nvidia_b200:$4 ;;
        mig) gres=gpu:nvidia_b200_1g.23gb:$4 ;;
    esac
    sbatch --parsable -J "tmb-$1-$5-s$2-$3-g$4${6:+-$6}" --gres="$gres" -o "$logs/%x_%j.log" \
        "$here/run_case.sh" "$1" "$2" "$3" "$4" "$5" ${6:-}
}
for tomo in copia 412; do
    for split in 221 111; do
        for prec in fp32 half; do sub $tomo $split $prec 1 b200; done
        sub $tomo $split fp32 1 mig
    done
    sub $tomo 111 half 1 mig
    sub $tomo 111 fp32 2 b200
    sub $tomo 111 fp32 4 b200
    # S4: the decoy and the whitening, against the same reference.
    sub $tomo 111 fp32 1 b200 rp
    sub $tomo 111 fp32 1 b200 wh
done

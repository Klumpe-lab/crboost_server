#!/bin/bash
# Build container_defs/<name>.def into <out_dir>/<name>.sif. Runs in this folder, where a def's %files (its locks)
# are found. Never overwrites an image: a running job may have it mounted, so a new image takes a new name.
#
#   container_defs/build.sh <name> <out_dir>
#
# By environment:
#   APPTAINER_TMPDIR   where the build unpacks; a dir under /dev/shm is removed afterwards. Not on BeeGFS, where apt
#                      fails now and then (EBUSY on renaming a .deb, Ubuntu 22.04 and 24.04): /dev/shm on a cluster
#                      node (it counts against the job's memory; a two-stage RELION build holds about 40 GB).
#   CRBOOST_FAKEROOT   "ignore-command" on a host with no subuid range for the user (CBE.next): --fakeroot
#                      --ignore-fakeroot-command, under which only root is mapped (the defs keep apt as root and divert
#                      chown/chgrp while packages install); "yes" for plain --fakeroot; unset to build as root.
#
# A failing %test does not fail an Apptainer build, so every def checks itself at the end of %post.
# On CBE.next, as a job on partition c (pytom: 6 min, relion and warp: 15 min with 16 CPUs); its sbatch passes no
# environment on, so the variables go inside the job's command:
#   sbatch -p c -c 16 --mem 160G -t 3:00:00 -o <log> --wrap 'CRBOOST_FAKEROOT=ignore-command \
#     APPTAINER_TMPDIR=/dev/shm/build-$SLURM_JOB_ID <checkout>/container_defs/build.sh <name> <out_dir>'
set -euo pipefail

if [ $# -ne 2 ]; then
    echo "usage: $0 <name> <out_dir>   (builds <name>.def from $(dirname "$0"))" >&2
    exit 2
fi
name=${1%.def}
mkdir -p "$2"
out_dir=$(cd "$2" && pwd)
cd "$(dirname "$0")"
def=$PWD/$name.def
if [ ! -f "$def" ]; then
    echo "no $def" >&2
    exit 1
fi
sif=$out_dir/$name.sif
if [ -e "$sif" ]; then
    echo "refusing to overwrite $sif" >&2
    exit 1
fi

case ${CRBOOST_FAKEROOT:-} in
    ignore-command) fakeroot=(--fakeroot --ignore-fakeroot-command) ;;
    yes) fakeroot=(--fakeroot) ;;
    "") fakeroot=() ;;
    *) echo "CRBOOST_FAKEROOT is ignore-command, yes or unset, not '$CRBOOST_FAKEROOT'" >&2; exit 2 ;;
esac
# Built under a temporary name and moved into place, so a failed build leaves no image behind.
partial=$sif.partial-$$
cleanup() {
    rm -f "$partial"
    case ${APPTAINER_TMPDIR:-} in /dev/shm/*) rm -rf "$APPTAINER_TMPDIR" ;; esac
}
trap cleanup EXIT
if [ -n "${APPTAINER_TMPDIR:-}" ]; then
    mkdir -p "$APPTAINER_TMPDIR"
fi
echo "building $sif from $def on $(hostname) at $(date)"
apptainer build "${fakeroot[@]}" "$partial" "$def"
mv "$partial" "$sif"
echo "built $sif at $(date): $(du -h "$sif" | cut -f1), sha256 $(sha256sum "$sif" | cut -d' ' -f1)"

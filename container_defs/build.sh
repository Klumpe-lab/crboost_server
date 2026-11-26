#!/bin/bash
# Build container_defs/<name>.def into <out_dir>/<name>.sif. Runs in this folder, where a def's %files (build_env.sh,
# its locks) are found. Never overwrites an image: a running job may have it mounted, so a new image takes a new name.
#
#   container_defs/build.sh <name> <out_dir>
#
# A site normally pulls the images and never builds them (roadmap 63 D20); this is for one that must, and for CI.
# As root it builds plainly. Otherwise always with --fakeroot --ignore-fakeroot-command: where the user has a subuid
# range that changes nothing, and where there is none (CBE.next, Munich) it keeps Apptainer from bringing the host's
# fakeroot binaries into the image, which fail on any image older than the host's glibc. The defs' apt_install
# (build_env.sh) makes apt work under the root-only mapping that is left.
#
# By environment:
#   APPTAINER_TMPDIR   where the build unpacks; a dir under /dev/shm is removed afterwards. Not on BeeGFS, where apt
#                      fails now and then (EBUSY on renaming a .deb): /dev/shm on a cluster node (it counts against the
#                      job's memory; a two-stage RELION build holds about 40 GB).
#   CRBOOST_BUILD_CA   the CA bundle (PEM) every download in the build is checked against; by default the host's own
#                      (/etc/ssl/certs/ca-certificates.crt, /etc/pki/tls/certs/ca-bundle.crt, ...). A base image has
#                      none until apt installs one, and apt reads its dated snapshot over HTTPS (build_env.sh). On a
#                      network that inspects TLS, the bundle that holds the proxy's CA. Bound into the build at /mnt,
#                      never copied into the image.
#
# On CBE.next, as a job on partition c (16 CPUs: pytom 4 min, relion and warp 15 min); its sbatch passes no
# environment on, so the variables go inside the job's command:
#   sbatch -p c -c 16 --mem 160G -t 3:00:00 -o <log> --wrap \
#     'APPTAINER_TMPDIR=/dev/shm/build-$SLURM_JOB_ID <checkout>/container_defs/build.sh <name> <out_dir>'
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
if [ "$(uname -m)" != x86_64 ]; then
    echo "the images are x86_64; this host is $(uname -m)" >&2
    exit 1
fi

args=()
if [ "$(id -u)" -ne 0 ]; then
    # Only a hint: Ubuntu 23.10 and later restrict user namespaces per program (AppArmor), and Apptainer's package
    # brings the profile that allows its own, so unshare may be refused where Apptainer is not.
    if ! unshare --user --map-root-user true 2>/dev/null; then
        echo "note: unshare cannot make a user namespace here; if Apptainer cannot either, build as root or pull" >&2
        echo "      the published images (docs/installation.md)" >&2
    fi
    args+=(--fakeroot --ignore-fakeroot-command)
fi

ca_dir=""
partial=$sif.partial-$$
cleanup() {
    rm -f "$partial"
    if [ -n "$ca_dir" ]; then rm -rf "$ca_dir"; fi
    case ${APPTAINER_TMPDIR:-} in /dev/shm/*) rm -rf "$APPTAINER_TMPDIR" ;; esac
}
trap cleanup EXIT
ca=${CRBOOST_BUILD_CA:-}
if [ -z "$ca" ]; then
    for f in /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt /etc/ssl/ca-bundle.pem \
        /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem; do
        if [ -f "$f" ]; then
            ca=$f
            break
        fi
    done
fi
if [ -z "$ca" ]; then
    echo "no CA bundle found on this host: set CRBOOST_BUILD_CA to one (PEM)" >&2
    exit 1
fi
if ! grep -q 'BEGIN CERTIFICATE' "$ca"; then
    echo "$ca is not a PEM bundle" >&2
    exit 1
fi
# A bind needs its target in the image: /mnt is in every base, empty, and left empty.
ca_dir=$(mktemp -d)
cp "$ca" "$ca_dir/build-ca.pem"
args+=(--bind "$ca_dir:/mnt:ro")
if [ -n "${APPTAINER_TMPDIR:-}" ]; then
    mkdir -p "$APPTAINER_TMPDIR"
fi

# Built under a temporary name and moved into place, so a failed build leaves no image behind.
echo "building $sif from $def on $(hostname) at $(date), $(apptainer --version), TLS checked against $ca," \
    "${args[*]}"
apptainer build "${args[@]}" "$partial" "$def"
mv "$partial" "$sif"
echo "built $sif at $(date): $(du -h "$sif" | cut -f1), sha256 $(sha256sum "$sif" | cut -d' ' -f1)"

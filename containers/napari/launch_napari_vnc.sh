#!/usr/bin/env bash
# By hand: napari on a VNC desktop on a GPU node, showing the volumes given. It runs the worker the napari chip
# in crboost submits (containers/chimerax_artiax/curation_session.sh with CB_APP=napari) under srun, and prints
# the SSH tunnel and the VNC password once the desktop is up. Ctrl-C (or quitting napari) ends the session.
#
#   NAPARI_SIF=/path/to/napari_GL.sif ./launch_napari_vnc.sh /path/to/tomogram.mrc [more.mrc ...]
#
# NAPARI_SIF   the image built from container_defs/napari_GL.def (required)
# NAPARI_GRES  the GPU to ask for (default gpu:1). It must be a full GPU that does OpenGL: on CBE.next MIG
#              slices and A100s do not, so ask for gpu:nvidia_b200:1 or gpu:quadro_rtx_6000:1 there.
# NAPARI_TIME  walltime (default 08:00:00; the GPU QOS rejects an unlimited one)
# CX_BINDS     colon-separated host paths the session may open besides /tmp and $HOME (default: the
#              volumes' directories)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${NAPARI_SIF:?Set NAPARI_SIF to the napari_GL.sif path, e.g. NAPARI_SIF=/path/to/napari_GL.sif $0 tomogram.mrc}"
[ "$#" -ge 1 ] || { echo "usage: NAPARI_SIF=/path/to/napari_GL.sif $0 tomogram.mrc [more.mrc ...]" >&2; exit 2; }

volumes=()
dirs=()
for v in "$@"; do
  [ -f "$v" ] || { echo "not a file: $v" >&2; exit 2; }
  v="$(realpath "$v")"
  volumes+=("$v")
  dirs+=("$(dirname "$v")")
done

export CB_APP=napari
export CX_SIF="$NAPARI_SIF"
export CX_VGL=1
export CB_NAPARI_LAUNCHER="$HERE/open_tomogram.py"
export CB_NAPARI_VOLUMES="$(IFS=:; echo "${volumes[*]}")"
export CX_BINDS="${CX_BINDS:-$(IFS=:; echo "${dirs[*]}")}"
# The worker runs on the compute node; the tunnel it prints goes through this login node.
export CX_LOGIN_HOST="${CX_LOGIN_HOST:-$(hostname -f)}"

echo "Queuing a GPU node on partition g (${NAPARI_GRES:-gpu:1}) for napari, Ctrl-C to stop waiting…"
exec srun -p g --nodes=1 --ntasks-per-node=1 --gres="${NAPARI_GRES:-gpu:1}" --cpus-per-task=4 --mem=16G \
  --time="${NAPARI_TIME:-08:00:00}" --pty "$HERE/../chimerax_artiax/curation_session.sh"

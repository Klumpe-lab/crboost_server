#!/usr/bin/env bash
# Manual launcher: allocate a CBE compute node and start a ChimeraX+ArtiaX VNC
# curation session inside it. The crboost UI button submits the same worker
# (curation_session.sh) via sbatch; this is the by-hand / debug entry point.
#
# Partition 'c' (CPU, software GL — the base chimerax_artiax.sif):
#   CX_SIF=/path/to/chimerax_artiax.sif    ./launch_curation_vnc.sh      # CPU node
# Partition 'g' (GPU + VirtualGL — the chimerax_artiax_GL.sif): passing 'g' turns on
# CX_VGL, requests a GPU, and the worker runs `vglrun -d egl chimerax` under --nv:
#   CX_SIF=/path/to/chimerax_artiax_GL.sif ./launch_curation_vnc.sh g    # GPU node
# Preload a tomogram + picks (kills the blank session): set CB_CXC to a .cxc from
# `python -m services.visualization.artiax_bridge bundle …`; srun --export=ALL carries it:
#   CX_SIF=… CB_CXC=/path/open_TS_01.cxc ./launch_curation_vnc.sh
set -euo pipefail

PARTITION="${1:-c}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${CX_SIF:?Set CX_SIF to the chimerax_artiax(_GL).sif path, e.g. CX_SIF=/groups/.../chimerax_artiax_GL.sif $0 g}"

# GPU node → request a GPU and switch the worker to the VirtualGL path (use the
# _GL.sif). srun propagates CX_VGL + CX_SIF to the worker via --export=ALL (default).
if [ "$PARTITION" = "g" ]; then
  export CX_VGL=1
  # The GPU QOS caps per-job walltime (QOSMaxWallDurationPerJobLimit), so `--time=0`
  # (unlimited — fine on partition c) is REJECTED here. Use a finite cap. If 8 h still
  # violates the QOS, lower it via CX_TIME; check the real limit with:
  #   sacctmgr show qos format=Name,MaxWall%20   (or `scontrol show partition g`)
  WALLTIME="${CX_TIME:-08:00:00}"
else
  WALLTIME="${CX_TIME:-0}"   # partition c is infinite
fi
GRES="${CX_VGL:+--gres=gpu:1}"

echo "Queuing an interactive node on partition '$PARTITION'${CX_VGL:+ (GPU + VirtualGL)}, walltime ${WALLTIME} (Ctrl-C to stop waiting)…"
exec srun -p "$PARTITION" ${GRES} --cpus-per-task=4 --mem=16G --time="$WALLTIME" --pty "$HERE/curation_session.sh"

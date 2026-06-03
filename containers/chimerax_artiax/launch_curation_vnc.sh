#!/usr/bin/env bash
# Manual launcher: allocate a CBE compute node and start a ChimeraX+ArtiaX VNC
# curation session inside it. The crboost UI button submits the same worker
# (curation_session.sh) via sbatch; this is the by-hand / debug entry point.
#
# Default partition 'c' (CPU, infinite walltime): software GL is enough for
# slice-based ArtiaX picking and leaves the scarce 'g' GPU nodes free.
#   CX_SIF=/path/to/chimerax_artiax.sif ./launch_curation_vnc.sh        # CPU node (recommended)
#   CX_SIF=/path/to/chimerax_artiax.sif ./launch_curation_vnc.sh g      # GPU node (then set up --nv; see README)
set -euo pipefail

PARTITION="${1:-c}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${CX_SIF:?Set CX_SIF to the chimerax_artiax.sif path, e.g. CX_SIF=/groups/.../chimerax_artiax.sif $0}"

echo "Queuing an interactive node on partition '$PARTITION' (Ctrl-C to stop waiting)…"
exec srun -p "$PARTITION" --cpus-per-task=4 --mem=16G --time=0 --pty "$HERE/curation_session.sh"

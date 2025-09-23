#!/bin/bash
#SBATCH --job-name=CryoBoost-Warp
#SBATCH --partition=XXXextra3XXX
#SBATCH --nodes=XXXextra1XXX
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=XXXthreadsXXX
#SBATCH --gres=gpu:XXXextra4XXX
#SBATCH --mem=XXXextra5XXX
#SBATCH --time=6:00:00
#SBATCH --output=XXXoutfileXXX
#SBATCH --error=XXXerrfileXXX

# --- SLURM JOB LOGIC ---
echo "--- SLURM JOB START ---"
echo "Job started on node: $(hostname) at $(date)"
echo "Job Directory: $(pwd)"
echo "--------------------------------------------------------"

# Define the container and necessary bind paths
CRYOBOOST_CONTAINER="/scratch-cbe/users/artem.kushner/crboost_with_relion.sif"
CRYOBOOST_PROJECTS_ROOT="/scratch-cbe/users/$USER/cryoboost_projects"
DATA_DIR="/users/artem.kushner/dev/001_CopiaTestSet"
ADAPTERS_DIR="/users/artem.kushner/dev/def_files/adapters_klumpelab"




# Change to the job's working directory. This is important for RELION.
cd "$(pwd)"

# CRITICAL FIX: The --containall flag.
# This flag tells Apptainer to NOT map in any host-system libraries.
# It prevents the host's old libstdc++.so.6 from being seen by the
# container's Python, which resolves the GLIBCXX version conflict.
# We then explicitly bind only the paths we absolutely need.
apptainer exec \
    --containall \
    --nv \
    -B "${CRYOBOOST_PROJECTS_ROOT}:${CRYOBOOST_PROJECTS_ROOT}" \
    -B "${DATA_DIR}:/data:ro" \
    -B "${ADAPTERS_DIR}/WarpTools:/opt/CryoBoost/bin/WarpTools" \
    --pwd "$(pwd)" \
    "${CRYOBOOST_CONTAINER}" \
    XXXcommandXXX

EXIT_CODE=$?
echo "--- SLURM JOB END ---"
echo "Job finished with exit code: $EXIT_CODE at $(date)"
exit $EXIT_CODE

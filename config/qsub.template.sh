#!/bin/bash
#SBATCH --job-name=CryoBoost
#SBATCH --partition=XXXextra1XXX
#SBATCH --constraint="XXXextra2XXX"
#SBATCH --nodes=XXXextra3XXX
#SBATCH --ntasks-per-node=XXXextra4XXX
#SBATCH --cpus-per-task=XXXextra5XXX
#SBATCH --gres=XXXextra6XXX
#SBATCH --mem=XXXextra7XXX
#SBATCH --time=XXXextra8XXX
#SBATCH --output=XXXoutfileXXX
#SBATCH --error=XXXerrfileXXX

# ==============================================================================
# CLUSTER-SPECIFIC MODULE LOADS
# Add your cluster's module commands here. Examples:
#
#   module purge
#   module load python/3.11
#   module load cuda/12.0
#
# ==============================================================================

# CryoBoost paths (filled by preflight.py)
export CRBOOST_SERVER_DIR="XXXcrboost_rootXXX"
export CRBOOST_PYTHON="XXXcrboost_pythonXXX"
export PYTHONPATH="${CRBOOST_SERVER_DIR}:${PYTHONPATH}"

echo "--- SLURM JOB BEGAN ---"
echo "Node: $(hostname)"
echo "Job ID: ${SLURM_JOB_ID}"

JOB_DIR=$(dirname "XXXoutfileXXX")
cd "${JOB_DIR}"

XXXcommandXXX

EXIT_CODE=$?
echo "--- SLURM JOB END (Exit Code: $EXIT_CODE) ---"

if [ $EXIT_CODE -eq 0 ]; then
    touch "./RELION_JOB_EXIT_SUCCESS"
else
    touch "./RELION_JOB_EXIT_FAILURE"
fi
#!/bin/bash
#SBATCH --job-name=CryoBoost
#SBATCH --partition=XXXpartitionXXX
#SBATCH --nodes=XXXnodesXXX
#SBATCH --ntasks-per-node=XXXntasksXXX
#SBATCH --constraint=XXXconstraintXXX
#SBATCH --cpus-per-task=XXXcpusXXX
#SBATCH --gres=XXXgresXXX
#SBATCH --mem=XXXmemXXX
#SBATCH --time=XXXtimeXXX
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


# CryoBoost paths (filled by setup.py)
export CRBOOST_SERVER_DIR="XXXcrboost_rootXXX" # <-- drivers must be on the python path
export CRBOOST_PYTHON="XXXpython_executableXXX"
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
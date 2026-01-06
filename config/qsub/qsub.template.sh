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
# ==============================================================================
# Uncomment and modify the following lines for your cluster's module system.
# You typically need: a recent Python (3.11+), and possibly CUDA/GCC toolchains.
#
# Example for modules:
#   export MODULEPATH=/software/system/modules/core
#   . /opt/ohpc/admin/lmod/lmod/init/bash
#   module load build-env/f2022
#   module load python/3.11.5-gcccore-13.2.0
#
# Example for a generic cluster:
#   module purge
#   module load python/3.11
#   module load cuda/12.0
#
# ==============================================================================

# CryoBoost paths - filled in by setup script
export CRBOOST_SERVER_DIR="XXXcraboroot_rootXXX"
export VENV_PYTHON="XXXvenv_pythonXXX"
export PYTHONPATH="${CRBOOST_SERVER_DIR}:${PYTHONPATH}"

echo "--- SLURM JOB BEGAN ---"
echo "Node: $(hostname)"
echo "Job ID: ${SLURM_JOB_ID}"

JOB_DIR=$(dirname "XXXoutfileXXX")

echo "CWD: $(pwd)"
echo "Job Directory: ${JOB_DIR}"

cd "${JOB_DIR}"

XXXcommandXXX

EXIT_CODE=$?
echo "--- SLURM JOB END (Exit Code: $EXIT_CODE) ---"

if [ $EXIT_CODE -eq 0 ]; then
    touch "./RELION_JOB_EXIT_SUCCESS"
else
    touch "./RELION_JOB_EXIT_FAILURE"
fi
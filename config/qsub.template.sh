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
##SBATCH --exclude=<node-1>,<node-2>   # optional: nodes to keep jobs off (remove one '#' to enable)
#SBATCH --output=XXXoutfileXXX
#SBATCH --error=XXXerrfileXXX
# The XXX...XXX placeholders are filled per job by crboost — leave them as they are.


# ------------ SLURM HEADER  -----------
# CLUSTER-SPECIFIC: make the compute node's environment match the one the venv was built with,
# so that <crboost_root>/venv/bin/python3 starts and finds its packages, and `apptainer` is on PATH.
# Ours, for reference (an Lmod cluster; the venv was built on the module python below):
#
#   export MODULEPATH=/software/system/modules/core
#   . /opt/ohpc/admin/lmod/lmod/init/bash
#   module load build-env/f2022
#   module load miniconda3/24.7.1-0
#   module load python/3.11.5-gcccore-13.2.0
#   module load gcccore/13.2.0
#   module load arrow/16.1.0-gfbf-2023b
#
which python3
python3 --version
# ------------ ------------  -----------

# ------------ ENV PATHS  --------------
# Filled by preflight.py when it creates config/qsub.sh from this template.
export CRBOOST_SERVER_DIR="XXXcrboost_rootXXX"
export CRBOOST_PYTHON="XXXcrboost_pythonXXX"
export PYTHONPATH="${CRBOOST_SERVER_DIR}:${PYTHONPATH}"
# ------------ ---------  --------------

echo "--- SLURM JOB BEGAN ---"
echo "Node: $(hostname)"

JOB_DIR=$(dirname "XXXoutfileXXX")

echo "Original CWD: $(pwd)"
echo "Target Job Directory: ${JOB_DIR}"

cd "${JOB_DIR}"
echo "New CWD: $(pwd)"

XXXcommandXXX

EXIT_CODE=$?
echo "--- SLURM JOB END (Exit Code: $EXIT_CODE) ---"

# Keep this block byte-for-byte: drivers/array_job_base.py finds it verbatim and strips it from
# array child tasks, so that only the supervisor writes the exit markers.
if [ $EXIT_CODE -eq 0 ]; then
    echo "Creating RELION_JOB_EXIT_SUCCESS"
    touch "./RELION_JOB_EXIT_SUCCESS"
else
    echo "Creating RELION_JOB_EXIT_FAILURE"
    touch "./RELION_JOB_EXIT_FAILURE"
fi

# Keep: the SLURM exit code is what --dependency=afterok chains on.
exit $EXIT_CODE

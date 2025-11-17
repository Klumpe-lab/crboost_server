#!/bin/bash
#SBATCH --job-name=CryoBoost-Warp
#SBATCH --partition=g
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1     
#SBATCH --constraint="g2|g3|g4"
#SBATCH --cpus-per-task=4
#SBATCH --gres=gpu:1             
#SBATCH --mem=16G
#SBATCH --time=0:15:00
#SBATCH --time=0:15:00
#SBATCH --output=XXXoutfileXXX
#SBATCH --error=XXXerrfileXXX


export MODULEPATH=/software/system/modules/core
. /opt/ohpc/admin/lmod/lmod/init/bash

module load build-env/f2022
module load miniconda3/24.7.1-0
module load python/3.11.5-gcccore-13.2.0 
module load gcccore/13.2.0 
module load arrow/16.1.0-gfbf-2023b
which python3
python3 --version

export CRBOOST_SERVER_DIR="/users/artem.kushner/dev/crboost_server/"
export VENV_PYTHON="/users/artem.kushner/dev/crboost_server/venv/bin/python3"
export PYTHONPATH="${VENV_PYTHON}:${PYTHONPATH}"

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

if [ $EXIT_CODE -eq 0 ]; then
    echo "Creating RELION_JOB_EXIT_SUCCESS"
    touch "./RELION_JOB_EXIT_SUCCESS"
else
    echo "Creating RELION_JOB_EXIT_FAILURE"
    touch "./RELION_JOB_EXIT_FAILURE"
fi

exit $EXIT_CODE

#!/bin/bash
#SBATCH --job-name=CryoBoost-Warp
#SBATCH --constraint="g2|g3|g4"
#SBATCH --partition=g
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1     
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:4             
#SBATCH --mem=96G                 
#SBATCH --time=5:00:00
#SBATCH --time=5:00:00
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
VENV_PYTHON="/users/artem.kushner/dev/crboost_server/venv/bin/python3"
HELPER_SCRIPT="/users/artem.kushner/dev/crboost_server/config/binAdapters/update_fs_metadata.py"
export PYTHONPATH="${VENV_PYTHON}:${PYTHONPATH}"

echo "--- SLURM JOB START ---"
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

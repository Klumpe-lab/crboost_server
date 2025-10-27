#!/bin/bash
#SBATCH --job-name=CryoBoost-Warp
#SBATCH --constraint=g4
#SBATCH --partition=XXXextra3XXX
#SBATCH --nodes=XXXextra1XXX
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=XXXthreadsXXX
#SBATCH --gres=gpu=XXXextra4XXX
#SBATCH --mem=XXXextra5XXX
#SBATCH --time=5:00:00
#SBATCH --output=XXXoutfileXXX
#SBATCH --error=XXXerrfileXXX

echo "--- SLURM JOB START ---"
echo "Node: $(hostname)"

JOB_DIR=$(dirname "XXXoutfileXXX")

echo "Original CWD: $(pwd)"
echo "Target Job Directory: ${JOB_DIR}"

cd "${JOB_DIR}"
echo "New CWD: $(pwd)"

echo "Executing Command..."
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

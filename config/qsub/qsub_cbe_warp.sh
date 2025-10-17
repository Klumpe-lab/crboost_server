#!/bin/bash
#SBATCH --job-name=CryoBoost-Warp
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

# Change to the job's specific output directory before doing anything else.
cd "${JOB_DIR}"
echo "New CWD: $(pwd)"

# # --- STANDARD ENVIRONMENT SETUP ---
# echo "Purging and loading modules..."
# module --force purge
# module load build-env/f2022
# module load cuda/11.8.0

echo "Executing Command..."
XXXcommandXXX

EXIT_CODE=$?
echo "--- SLURM JOB END (Exit Code: $EXIT_CODE) ---"
exit $EXIT_CODE

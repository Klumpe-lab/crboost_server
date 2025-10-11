#!/bin/bash
#SBATCH --job-name=CryoBoost-Warp
#SBATCH --partition=XXXextra3XXX
#SBATCH --nodes=XXXextra1XXX
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=XXXthreadsXXX
#SBATCH --gres=gpu:XXXextra4XXX
#SBATCH --mem=XXXextra5XXX
#SBATCH --time=5:00:00
#SBATCH --output=XXXoutfileXXX
#SBATCH --error=XXXerrfileXXX

echo "--- SLURM JOB START ---"
echo "Node: $(hostname)"


# #module load build-env/f2022
# module load miniconda3/24.7.1-0
# #module load gcc/11.3.0
# #module load cuda/12.3.0

# SATELLITE_ACTIVATE_SCRIPT="/groups/klumpe/software/Setup/cryoboost_satellite_repo/activate_satellite_repo.sh"
# echo "--- Sourcing Application Environment from ${SATELLITE_ACTIVATE_SCRIPT} ---"
# source "${SATELLITE_ACTIVATE_SCRIPT}" 

XXXcommandXXX

EXIT_CODE=$?
echo "--- SLURM JOB END (Exit Code: $EXIT_CODE) ---"
exit $EXIT_CODE

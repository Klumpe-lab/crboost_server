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

unset LD_PRELOAD
unset SINGULARITY_BINDPATH
unset APPTAINER_BINDPATH
unset SINGULARITY_NAME
unset APPTAINER_NAME
unset SINGULARITY_CONTAINER
unset APPTAINER_CONTAINER
unset FAKEROOTKEY
unset FAKEROOTDONTTRYCHOWN

XXXcommandXXX

EXIT_CODE=$?
echo "--- SLURM JOB END (Exit Code: $EXIT_CODE) ---"
exit $EXIT_CODE

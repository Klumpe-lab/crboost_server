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
echo "Working directory: $(pwd)"

# Clean container environment variables that might be inherited from parent container
unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH
unset SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER
unset SINGULARITYENV_APPEND_PATH APPTAINERENV_APPEND_PATH LD_PRELOAD
unset SINGULARITY_TMPDIR APPTAINER_TMPDIR XDG_RUNTIME_DIR
unset DISPLAY XAUTHORITY

# Execute the containerized command
XXXcommandXXX

EXIT_CODE=$?
echo "--- SLURM JOB END (Exit Code: $EXIT_CODE) ---"
exit $EXIT_CODE

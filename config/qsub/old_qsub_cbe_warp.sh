#!/bin/bash
#SBATCH --job-name=CryoBoost-Job
#SBATCH --partition=XXXextra3XXX
#SBATCH --nodes=XXXextra1XXX
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=XXXextra2XXX
#SBATCH --gres=gpu:XXXextra4XXX
#SBATCH --mem=XXXextra5XXX
#SBATCH --time=24:00:00
#SBATCH --output=XXXjob_dirXXX/run.out
#SBATCH --error=XXXjob_dirXXX/run.err

#####################################################################
# STAGE 1: SLURM ENVIRONMENT LOGGING
#####################################################################
echo "============================================================"
echo "          CryoBoost Job Log - SLURM Environment"
echo "============================================================"
echo "Job ID:         $SLURM_JOB_ID"
echo "Job Name:       $SLURM_JOB_NAME"
echo "Submit Host:    $SLURM_SUBMIT_HOST"
echo "Compute Node:   $(hostname)"
echo "Job Directory:  XXXjob_dirXXX"
echo "GPU(s) assigned: $CUDA_VISIBLE_DEVICES"
echo "Time:             $(date)"
echo "============================================================"
echo

#####################################################################
# STAGE 2: PAYLOAD EXECUTION
#####################################################################
echo "--- Preparing to execute Python translator ---"
echo "Changing directory to XXXjob_dirXXX"
cd XXXjob_dirXXX
echo

echo "Full command to be executed:"
echo "XXXcommandXXX"
echo "--------------------------------------------"
echo

# Execute the Python translator script passed by relion_pipeliner
XXXcommandXXX

# Capture the exit code of the command immediately after it runs
EXIT_CODE=$?

#####################################################################
# STAGE 3: FINAL STATUS LOGGING
#####################################################################
echo
echo "============================================================"
echo "                    Job Completion Status"
echo "============================================================"
echo "Python translator finished at: $(date)"
if [ $EXIT_CODE -eq 0 ]; then
  echo "Exit Code: $EXIT_CODE (SUCCESS)"
else
  echo "Exit Code: $EXIT_CODE (FAILURE)"
fi
echo "============================================================"

# Ensure the SLURM job status reflects the command's success/failure
exit $EXIT_CODE

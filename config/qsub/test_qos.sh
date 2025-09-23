#!/bin/bash
#SBATCH --job-name=qos-test
#SBATCH --output=qos-test-%j.out
#SBATCH --error=qos-test-%j.err

echo "Job ID: $SLURM_JOB_ID"
echo "---"
# This command asks Slurm to show all details for this specific job
scontrol show job $SLURM_JOB_ID

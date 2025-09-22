#!/bin/bash
#SBATCH --job-name=gpu_test_job
#SBATCH --output=gpu_test_job_%j.out
#SBATCH --error=gpu_test_job_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:05:00

echo "Starting GPU Test Job on node $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "---"

for i in {1..5}
do
    echo "Timestamp: $(date)"
    echo "Querying NVIDIA GPU status (Iteration $i/5):"
    nvidia-smi
    echo "---"
    sleep 10
done

echo "Good. GPU Test Job Finished."
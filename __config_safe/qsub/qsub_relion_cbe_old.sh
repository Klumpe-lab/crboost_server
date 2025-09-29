#!/bin/bash --norc 
# Standard output and error:
#SBATCH -e XXXerrfileXXX
#SBATCH -o XXXoutfileXXX
# Initial working directory:
#SBATCH -D ./
# Job Name:
#SBATCH -J Relion
# Queue (Partition):
# Number of nodes and MPI tasks per node:
#SBATCH --ntasks=XXXmpinodesXXX
#SBATCH --cpus-per-task=XXXthreadsXXX
#
#SBATCH --mail-type=none
#
# Wall clock limit:
#SBATCH --time=01:00:00


#clean up environment

#build up environment variables

module list
echo "submitting relion"
srun bash --norc -c 'XXXcommandXXX'
echo "done"


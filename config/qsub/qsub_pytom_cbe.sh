#!/bin/bash  
# Standard output and error:
#SBATCH -e XXXerrfileXXX
#SBATCH -o XXXoutfileXXX
# Initial working directory:
#SBATCH -D ./
# Job Name:
#SBATCH -J WarpEm
# Queue (Partition):
#SBATCH --partition=g
# Number of nodes and MPI tasks per node:
#SBATCH --nodes=1   #XXXextra1XXX
#SBATCH --ntasks=1 #XXXmpinodesXXX
##SBATCH --ntasks-per-node=XXXextra2XXX
##SBATCH --cpus-per-task=XXXthreadsXXX
##SBATCH --gres=gpu:XXXextra4XXX
#SBATCH --gres=gpu:1
#
#SBATCH --constraint="g2|g3|g4"
#SBATCH --mem 20G
#
# Wall clock limit:
#SBATCH --time=1:00:00


#clean up environment
#apptainer exec minimalContainerOS.sif
#source /users/sven.klumpe/software/activate_cb.sh
#source /programs/sbgrid.shrc
#ml load cuda/12.3.0
#nvidia-smi

#source /programs/sbgrid.shrc
#apptainer run --nv /resources/containers/pytom_tm.sif 'pytom_match_template.py -v Tomograms/job005/tomograms/rec_Position_1.mrc --tilt-angles External/job009//tiltAngleFiles/Position_1.tlt --defocus External/job009//defocusFiles/Position_1.txt --dose-accumulation External/job009//doseFiles/Position_1.txt -t templates/template_box96.0_apix11.8_black.mrc -d External/job009/tmResults -m templates/template_box96.0_apix11.8_mask.mrc --angular-search 180 --voltage 300 --amplitude-contrast 0.1  --per-tilt-weighting --log debug -g 0 -s 4 4 2 --non-spherical-mask'

#apptainer run --nv /resources/containers/pytom_tm.sif 'pytom_match_template.py --help'
#apptainer run --nv /resources/containers/pytom_tm.sif 'pytom_match_template.py -v Tomograms/job005/tomograms/rec_Position_1.mrc --tilt-angles External/job009//tiltAngleFiles/Position_1.tlt --defocus External/job009//defocusFiles/Position_1.txt --dose-accumulation External/job009//doseFiles/Position_1.txt -t templates/template_box96.0_apix11.8_black.mrc -d External/job009/tmResults -m templates/template_box96.0_apix11.8_mask.mrc --angular-search angles.txt --voltage 300.0 --spherical-abberation 2.7 --amplitude-contrast 0.1 --per-tilt-weighting --log debug -g 0 -s 1 1 1 --non-spherical-mask'

echo "submitting relion"
srun bash --norc -c 'XXXcommandXXX'
echo "done"

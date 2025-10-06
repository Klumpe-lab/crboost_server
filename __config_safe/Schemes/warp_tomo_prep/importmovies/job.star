
# version 50001

data_job

_rlnJobTypeLabel             relion.importtomo
_rlnJobIsContinue                       1
_rlnJobIsTomo                           0
 

# version 50001

data_joboptions_values

loop_ 
_rlnJobOptionVariable #1 
_rlnJobOptionValue #2 
movie_files ./frames/*.eer 
images_are_motion_corrected         No 
mdoc_files ./mdoc/*.mdoc 
optics_group_name         "" 
prefix         "" 
angpix       2.93 
kV        300 
Cs        2.7 
Q0        0.1 
dose_rate          3 
dose_is_per_movie_frame         No 
tilt_axis_angle        -95 
mtf_file         "" 
flip_tiltseries_hand         No 
do_queue         Yes 
queuename    openmpi 
qsub     sbatch 
qsubscript          qsub/qsub_cbe_warp.sh
min_dedicated          1 
other_args         "" 
qsub_extra1       auto 
qsub_extra2       170G 
qsub_extra3       auto 
qsub_extra4       auto 
qsub_extra5      
 

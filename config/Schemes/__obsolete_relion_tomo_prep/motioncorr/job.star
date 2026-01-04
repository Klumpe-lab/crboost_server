
# version 30001

data_job

_rlnJobTypeLabel             relion.motioncorr.own
_rlnJobIsContinue                       0
_rlnJobIsTomo                           1
 

# version 30001

data_joboptions_values

loop_ 
_rlnJobOptionVariable #1 
_rlnJobOptionValue #2 
input_star_mics Schemes/relion_tomo_prep/importmovies/tilt_series.star  
eer_grouping         39 
do_float16         No 
do_even_odd_split        Yes 
do_save_ps        Yes 
group_for_ps         10 
bfactor        150 
patch_x          1 
patch_y          1 
group_frames          1 
bin_factor          1 
fn_gain_ref         "" 
gain_flip "No flipping (0)" 
gain_rot "No rotation (0)" 
fn_defect         "" 
do_own_motioncor        Yes 
fn_motioncor2_exe /programs/x86_64-linux/system/sbgrid_bin/MotionCor2_1.6.4_Cuda118_Mar312023 
gpu_ids          0 
other_motioncor2_args         "" 
nr_mpi          3 
nr_threads          3 
do_queue        Yes 
queuename    auto 
qsub     sbatch 
    qsubscript          qsub/qsub_cbe_warp.sh
min_dedicated          1 
other_args         "" 
qsub_extra1        auto  # qos name
qsub_extra2        auto  # required memory
qsub_extra3        auto 
qsub_extra4        auto 
qsub_extra5        
 

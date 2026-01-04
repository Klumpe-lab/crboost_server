# version 50001

data_job

_rlnJobTypeLabel             relion.aligntiltseries
_rlnJobIsContinue                       0
_rlnJobIsTomo                           0
 

# version 50001

data_joboptions_values

loop_ 
_rlnJobOptionVariable #1 
_rlnJobOptionValue #2 
in_tiltseries Schemes/relion_tomo_prep/filtertiltsInter/tiltseries_filtered.star 
do_imod_fiducials         No 
fiducial_diameter         10 
do_imod_patchtrack         No 
patch_size        100 
patch_overlap         50 
do_aretomo2        Yes 
do_aretomo_ctf         No 
do_aretomo_phaseshift         No 
do_aretomo_tiltcorrect         Yes 
aretomo_tiltcorrect_angle        999 
tomogram_thickness        250 
other_aretomo_args         "" 
fn_aretomo_exe    /programs/x86_64-linux/system/sbgrid_bin/AreTomo_1.3.4_Cuda118_Feb22_2023
fn_batchtomo_exe /programs/x86_64-linux/system/sbgrid_bin/batchruntomo
min_dedicated          1 
gpu_ids     
do_queue        Yes 
other_args         "" 
queuename    auto 
qsub        sbatch 
    qsubscript          qsub/qsub_cbe_warp.sh
nr_mpi          6 
qsub_extra1       auto 
qsub_extra2       auto 
qsub_extra3       auto
qsub_extra4       auto 
qsub_extra5        

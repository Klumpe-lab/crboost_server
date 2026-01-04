
# version 50001

data_job

_rlnJobTypeLabel             relion.denoisetomo
_rlnJobIsContinue                       1
_rlnJobIsTomo                           0
 

# version 50001

data_joboptions_values

loop_ 
_rlnJobOptionVariable #1 
_rlnJobOptionValue #2 
in_tomoset  Schemes/relion_tomo_prep/reconstructionsplit/tomograms.star 
cryocare_path /programs/x86_64-linux/system/sbgrid_bin/ 
gpu_ids           "0" 
do_cryocare_train        Yes 
tomograms_for_training "Position_1" 
number_training_subvolumes        600 
subvolume_dimensions         64 
do_cryocare_predict         No 
care_denoising_model         "" 
ntiles_x          2 
ntiles_y          2 
ntiles_z          2 
denoising_tomo_name         "" 
do_queue        Yes 
queuename      auto 
qsub     sbatch 
    qsubscript          qsub/qsub.sh
min_dedicated          1 
other_args         "" 
qsub_extra1       auto 
qsub_extra2       auto 
qsub_extra3       auto 
qsub_extra4       auto 
qsub_extra5         

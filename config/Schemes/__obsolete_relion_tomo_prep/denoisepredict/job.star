
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
care_denoising_model  Schemes/relion_tomo_prep/denoisetrain/denoising_model.tar.gz 
cryocare_path /programs/x86_64-linux/system/sbgrid_bin/ 
gpu_ids          "0" 
do_cryocare_train         No 
tomograms_for_training Position_1 
number_training_subvolumes        600 
subvolume_dimensions         64 
do_cryocare_predict        Yes 
ntiles_x          4 
ntiles_y          4 
ntiles_z          4 
denoising_tomo_name         "" 
do_queue        Yes 
queuename      auto 
qsub     sbatch 
    qsubscript          qsub/qsub_cbe_warp.sh
min_dedicated          1 
other_args         "" 
qsub_extra1       auto 
qsub_extra2       auto 
qsub_extra3       auto 
qsub_extra4       auto 
qsub_extra5           
 

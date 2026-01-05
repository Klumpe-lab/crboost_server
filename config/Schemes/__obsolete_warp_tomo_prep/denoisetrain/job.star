# version 50001

data_job

_rlnJobTypeLabel             relion.external
_rlnJobIsContinue                       0
_rlnJobIsTomo                           1
 

# version 50001

data_joboptions_values

loop_ 
_rlnJobOptionVariable #1 
_rlnJobOptionValue #2 
fn_exe "echo PLACEHOLDER_WILL_BE_REPLACED_BY_ORCHESTRATOR"
in_tomoset Schemes/warp_tomo_prep/tsReconstruct/tomograms.star
other_args ""
do_queue Yes
queuename openmpi
qsub sbatch
qsubscript qsub/qsub.sh
min_dedicated 1

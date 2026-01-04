# version 50001

data_job

_rlnJobTypeLabel             relion.external
_rlnJobIsContinue                       0
_rlnJobIsTomo                           0


# version 50001

data_joboptions_values

loop_ 
_rlnJobOptionVariable #1 
_rlnJobOptionValue #2 
   binning          1 
  box_size        512 
 crop_size        256  
do_float16        Yes 
do_stack2d        Yes 
in_optimisation   "" 
  max_dose         -1 
min_frames          1 
    fn_exe         XXX_REPLACED_BY_ORCHESTRATOR_XXX
other_args         "" 
  do_queue        Yes 
      qsub     sbatch 
nr_threads          12 
qsub_extra1       auto 
qsub_extra2       auto 
qsub_extra3       auto
qsub_extra4       auto 
qsub_extra5       370G 
qsubscript       qsub/qsub.sh
 queuename       auto


# version 50001

data_job

_rlnJobTypeLabel             relion.pseudosubtomo
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
use_direct_entries        No 
in_optimisation   "Schemes/relion_tomo_prep/tmextractcand/optimisation_set.star" 
in_particles    "" 
in_tomograms    "" 
in_trajectories         "" 
  max_dose         -1 
min_dedicated          1 
min_frames          1 
    nr_mpi          2 
other_args         "" 
  do_queue        Yes 
      qsub     sbatch 
nr_threads          12 
qsub_extra1       auto 
qsub_extra2       auto 
qsub_extra3       auto
qsub_extra4       auto 
qsub_extra5        
    qsubscript          qsub/qsub_cbe_warp.sh
 queuename        auto
 

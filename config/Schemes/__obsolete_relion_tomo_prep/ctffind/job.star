
# version 50001

data_job

_rlnJobTypeLabel             relion.ctffind.ctffind4
_rlnJobIsContinue                       1
_rlnJobIsTomo                           1
 

# version 50001

data_joboptions_values

loop_ 
_rlnJobOptionVariable #1 
_rlnJobOptionValue #2 
input_star_mics Schemes/relion_tomo_prep/motioncorr/corrected_tilt_series.star 
do_phaseshift         No 
phase_min          0 
phase_max        180 
phase_step         10 
dast        100 
fn_ctffind_exe /programs/x86_64-linux/system/sbgrid_bin/ctffind4 
use_given_ps        Yes 
slow_search         No 
ctf_win         -1 
box        512 
dfmin       5000 
dfmax      70000 
resmin         30 
resmax          5 
dfstep        500 
localsearch_nominal_defocus      10000 
exp_factor_dose        185 
nr_mpi         16 
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
 

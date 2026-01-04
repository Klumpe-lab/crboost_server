
# version 30001

data_job

_rlnJobTypeLabel             relion.external
_rlnJobIsContinue                       0
_rlnJobIsTomo                           0
 

# version 30001

data_joboptions_values

loop_ 
_rlnJobOptionVariable #1 
_rlnJobOptionValue #2 
    fn_exe         crboost_filterTitlts.py
  in_3dref         "" 
 in_coords         "" 
   in_mask         "" 
    in_mic         Schemes/warp_tomo_prep/fsMotionAndCtf/fs_motion_and_ctf.star
    in_mov         "" 
   in_part         "" 
other_args         "" 
param1_label      model 
param1_value      default
param2_label      defocusInAng
param2_value      0,600000,-70,70 
param3_label      ctfMaxResolution
param3_value      0,50,-70,70 
param4_label      driftInAng
param4_value      0,90000,-70,70    
param5_label      probThreshold
param5_value         0.70 
param6_label      probThrAction 
param6_value         assignToGood 
param7_label      mdocWk 
param7_value      "mdoc/*.mdoc" 
param8_label         "" 
param8_value         "" 
param9_label         "" 
param9_value         "" 
param10_label         "" 
param10_value         "" 
nr_threads          24
do_queue         Yes 
queuename    openmpi 
qsub     sbatch 
qsubscript          qsub/qsub_cbe_warp.sh
min_dedicated          1 
qsub_extra1          1 
qsub_extra2          1 
qsub_extra3    g
qsub_extra4      2 
qsub_extra5      370G
 


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
    fn_exe         crboost_warp_ts_reconstruct.py
  in_3dref         "" 
 in_coords         "" 
   in_mask         "" 
    in_mic         Schemes/warp_tomo_prep/tsCtf/ts_ctf_tilt_series.star
    in_mov         "" 
   in_part         "" 
other_args         "\" 
param1_label      "rescale_angpixs" 
param1_value      "11.8" 
param2_label      "halfmap_frames"
param2_value      "1"
param3_label      "deconv" 
param3_value      "0" 
param4_label      "" 
param4_value      "" 
param5_label      "" 
param5_value      "" 
param6_label      "" 
param6_value      "" 
param7_label      "" 
param7_value      ""
param8_label      ""
param8_value      "" 
param9_label      ""
param9_value      ""    
param10_label     "perdevice" 
param10_value     "2" 
nr_threads        1
do_queue         Yes 
queuename    openmpi 
qsub     sbatch 
qsubscript          qsub/qsub.sh
min_dedicated          1
qsub_extra1          1 
qsub_extra2          8
qsub_extra3    g
qsub_extra4      1
qsub_extra5      32G

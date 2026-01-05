
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
    fn_exe         crboost_warp_ts_ctf.py
  in_3dref         "" 
 in_coords         "" 
   in_mask         "" 
    in_mic         Schemes/warp_tomo_prep/aligntiltsWarp/aligned_tilt_series.star
    in_mov         "" 
   in_part         "" 
other_args         "\" 
param1_label      "window" 
param1_value      "512" 
param2_label      "range_min_max"
param2_value      "30:6.0"
param3_label      "defocus_min_max" 
param3_value      "1.1:8" 
param4_label      "defocusHand" 
param4_value      "set_flip" 
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
qsub_extra2          1 
qsub_extra3    g
qsub_extra4      2 
qsub_extra5      32G
 

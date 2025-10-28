
# version 30001

data_job

_rlnJobTypeLabel             relion.external
_rlnJobIsContinue                       0
_rlnJobIsTomo                           1
 
# version 30001

data_joboptions_values

loop_ 
_rlnJobOptionVariable #1 
_rlnJobOptionValue #2 
    fn_exe         crboost_warp_fs_motion_and_ctf.py
  in_3dref         "" 
 in_coords         "" 
   in_mask         "" 
    in_mic         Schemes/warp_tomo_prep/importmovies/tilt_series.star
    in_mov         "" 
   in_part         "" 
other_args         "--c_window 512 --out_average_halves \" 
param1_label      "eer_fractions" 
param1_value      "32" 
param2_label      "gain_path" 
param2_value      "None" 
param3_label      "gain_operations" 
param3_value      "None" 
param4_label      "m_range_min_max" 
param4_value      "500:10"
param5_label      "m_bfac"
param5_value      "-500" 
param6_label      "m_grid"
param6_value      "1x1x3"    
param7_label      "c_range_min_max"
param7_value      "30:6.0"
param8_label      "c_defocus_min_max" 
param8_value      "1.1:8" 
param9_label      "c_grid" 
param9_value      "2x2x1" 
param10_label     "perdevice" 
param10_value     "1" 
nr_threads        8
do_queue         Yes 
queuename    openmpi 
qsub     sbatch 
qsubscript    qsub/qsub_cbe_warp.sh 
min_dedicated          1 
qsub_extra1          1 
qsub_extra2          8 
qsub_extra3    g
qsub_extra4      1 
qsub_extra5      16G
 

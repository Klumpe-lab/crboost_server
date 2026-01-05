
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
  do_queue         No 
    fn_exe ../../../../00-Other/CryoTheraPy/bin/parser_feature_analyser.py 
  in_3dref         "" 
 in_coords         "" 
   in_mask         "" 
    in_mic Schemes/relion_tomo_prep/ctffind/tilt_series_ctf.star 
    in_mov         "" 
   in_part         "" 
min_dedicated          1 
nr_threads          1 
other_args         "" 
param10_label         "" 
param10_value         "" 
param1_label         y1 
param1_value rlnCtfMaxResolution 
param2_label         "" 
param2_value         "" 
param3_label         "" 
param3_value         "" 
param4_label         "" 
param4_value         "" 
param5_label         "" 
param5_value         "" 
param6_label         "" 
param6_value         "" 
param7_label         "" 
param7_value         "" 
param8_label         "" 
param8_value         "" 
param9_label         "" 
param9_value         "" 
      qsub     sbatch 
qsub_extra1          3 
qsub_extra2          3 
qsub_extra3    g
qsub_extra4      2
    qsubscript          qsub/qsub.sh
 queuename    openmpi 
 


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
    in_mic         Schemes/relion_tomo_prep/ctffind/tilt_series_ctf.star
    in_mov         "" 
   in_part         "" 
other_args         "" 
param1_label      model 
param1_value      default
param2_label      defocusInAng
param2_value      2000,140000,-70,70 
param3_label      ctfMaxResolution
param3_value      0,50,-70,70 
param4_label      driftInAng
param4_value      1,90000,-70,70    
param5_label      probThreshold
param5_value         0.70 
param6_label      probThrAction 
param6_value         assignToGood 
param7_label      "mdocWk" 
param7_value      "mdoc/*.mdoc" 
param8_label      "" 
param8_value         "" 
param9_label         "" 
param9_value         "" 
param10_label         "" 
param10_value         "" 
nr_threads          24
do_queue         Yes 
queuename    auto 
qsub     sbatch 
    qsubscript          qsub/qsub.sh
min_dedicated          1 
qsub_extra1       auto 
qsub_extra2       auto 
qsub_extra3       auto 
qsub_extra4       auto
qsub_extra5       
 

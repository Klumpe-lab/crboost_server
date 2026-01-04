
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
    fn_exe         crboost_filterTitlts_Interactive.py
  in_3dref         "" 
 in_coords         "" 
   in_mask         "" 
    in_mic         Schemes/relion_tomo_prep/filtertilts/tiltseries_filtered.star
    in_mov         "" 
   in_part         "" 
other_args         "" 
param1_label      interActiveMode 
param1_value      "onFailure"
param2_label      ""
param2_value      ""
param3_label      ""
param3_value      ""
param4_label      ""
param4_value      ""  
param5_label      ""
param5_value      ""
param6_label      ""
param6_value      ""
param7_label      ""
param7_value      ""
param8_label      "" 
param8_value         "" 
param9_label         "" 
param9_value         "" 
param10_label         "" 
param10_value         "" 
nr_threads          12
do_queue         No 
queuename    auto 
qsub     sbatch 
    qsubscript          qsub/qsub_cbe_warp.sh
min_dedicated          1 
qsub_extra1       auto 
qsub_extra2       auto 
qsub_extra3       auto 
qsub_extra4       auto
qsub_extra5       
 

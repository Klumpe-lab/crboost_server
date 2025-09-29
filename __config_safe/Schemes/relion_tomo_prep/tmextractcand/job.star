
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
    fn_exe         crboost_extract_tm_candidates.py
  in_3dref         "" 
 in_coords         "" 
   in_mask         "" 
    in_mic         Schemes/relion_tomo_prep/templatematching/tomograms.star
    in_mov         "" 
   in_part         "" 
other_args         "--implementation Pytom \"  
param1_label      "cutOffMethod" 
param1_value      "NumberOfFalsePositives" 
param2_label      "cutOffValue" 
param2_value      "1" 
param3_label      "particleDiameterInAng" 
param3_value      "200"
param4_label      "maxNumParticles"
param4_value      "1500"    
param5_label      "apixScoreMap"
param5_value      "auto" 
param6_label      "scoreFilterMethod" 
param6_value      "None" 
param7_label      "scoreFilterValue"
param7_value      "None"
param8_label      "MaskFoldPath" 
param8_value      "None" 
param9_label      "" 
param9_value      "" 
param10_label     "" 
param10_value     "" 
nr_threads        1
do_queue         Yes 
queuename       auto 
qsub     sbatch 
    qsubscript          qsub/qsub_cbe_warp.sh
min_dedicated          1 
qsub_extra1       auto 
qsub_extra2        170 
qsub_extra3       auto 
qsub_extra4       auto
qsub_extra5      
 

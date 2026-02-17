# Ours:

(venv) ᢹ CBE-login [dev/crboost_server] # The scores distribution -- how many candidates before thresholding?
(venv) ᢹ CBE-login [dev/crboost_server] cat projects/post_tomo_correction/External/job012/run.out | tail -40
  3%|▎         | 38/1500 [00:09<05:55,  4.11it/s]
  3%|▎         | 39/1500 [00:09<05:56,  4.10it/s]
  3%|▎         | 40/1500 [00:10<06:00,  4.05it/s]
  3%|▎         | 41/1500 [00:10<05:55,  4.10it/s]
  3%|▎         | 42/1500 [00:10<06:15,  3.88it/s]
  3%|▎         | 43/1500 [00:10<06:28,  3.75it/s]
  3%|▎         | 44/1500 [00:11<06:23,  3.80it/s]
  3%|▎         | 45/1500 [00:11<06:35,  3.68it/s]
  3%|▎         | 46/1500 [00:11<06:49,  3.55it/s]
  3%|▎         | 47/1500 [00:12<06:57,  3.48it/s]
  3%|▎         | 48/1500 [00:12<06:59,  3.46it/s]
  3%|▎         | 49/1500 [00:12<07:00,  3.45it/s]
  3%|▎         | 50/1500 [00:12<06:47,  3.56it/s]
  3%|▎         | 51/1500 [00:13<06:29,  3.72it/s]
  3%|▎         | 52/1500 [00:13<06:16,  3.85it/s]
  4%|▎         | 53/1500 [00:13<06:08,  3.92it/s]
  4%|▎         | 54/1500 [00:13<06:05,  3.96it/s]
  4%|▎         | 55/1500 [00:14<06:02,  3.99it/s]
  4%|▎         | 56/1500 [00:14<05:56,  4.05it/s]
  4%|▍         | 57/1500 [00:14<05:51,  4.10it/s]
  4%|▍         | 58/1500 [00:14<05:47,  4.15it/s]
  4%|▍         | 59/1500 [00:15<05:46,  4.16it/s]
  4%|▍         | 60/1500 [00:15<05:48,  4.13it/s]
  4%|▍         | 61/1500 [00:15<05:50,  4.11it/s]
  4%|▍         | 62/1500 [00:15<05:58,  4.01it/s]
  4%|▍         | 63/1500 [00:16<06:21,  3.77it/s]
  4%|▍         | 64/1500 [00:16<06:35,  3.63it/s]
  4%|▍         | 65/1500 [00:16<06:46,  3.53it/s]
  4%|▍         | 66/1500 [00:17<06:48,  3.51it/s]
  4%|▍         | 67/1500 [00:17<06:56,  3.44it/s]
  5%|▍         | 68/1500 [00:17<07:15,  3.29it/s]
  5%|▍         | 68/1500 [00:17<06:17,  3.79it/s]
[DRIVER] Collecting particle lists...
[DRIVER] Single tomogram - copying post_tomo_correction_Position_1_particles.star
[DRIVER] Cleaned rlnTomoName suffix '_12.00Apx' from 68 particles
[DRIVER] Extracted 68 particles
[DRIVER] Created optimisation_set.star with absolute paths
--- SLURM JOB END (Exit Code: 0) ---
--- SLURM JOB END (Exit Code: 0) ---
Creating RELION_JOB_EXIT_SUCCESS
(venv) ᢹ CBE-login [dev/crboost_server]                            
(venv) ᢹ CBE-login [dev/crboost_server] # And the template matching output
(venv) ᢹ CBE-login [dev/crboost_server] cat projects/post_tomo_correction/External/job011/run.out | tail -40
 99%|█████████▉| 8904/9000 [06:55<00:04, 21.36it/s]
 99%|█████████▉| 8907/9000 [06:56<00:04, 21.37it/s]
 99%|█████████▉| 8910/9000 [06:56<00:04, 21.38it/s]
 99%|█████████▉| 8913/9000 [06:56<00:04, 21.38it/s]
 99%|█████████▉| 8916/9000 [06:56<00:03, 21.39it/s]
 99%|█████████▉| 8919/9000 [06:56<00:03, 21.39it/s]
 99%|█████████▉| 8922/9000 [06:56<00:03, 21.39it/s]
 99%|█████████▉| 8925/9000 [06:56<00:03, 21.39it/s]
 99%|█████████▉| 8928/9000 [06:57<00:03, 21.40it/s]
 99%|█████████▉| 8931/9000 [06:57<00:03, 21.40it/s]
 99%|█████████▉| 8934/9000 [06:57<00:03, 21.40it/s]
 99%|█████████▉| 8937/9000 [06:57<00:02, 21.40it/s]
 99%|█████████▉| 8940/9000 [06:57<00:02, 21.39it/s]
 99%|█████████▉| 8943/9000 [06:57<00:02, 21.39it/s]
 99%|█████████▉| 8946/9000 [06:57<00:02, 21.39it/s]
 99%|█████████▉| 8949/9000 [06:58<00:02, 21.39it/s]
 99%|█████████▉| 8952/9000 [06:58<00:02, 21.38it/s]
100%|█████████▉| 8955/9000 [06:58<00:02, 21.38it/s]
100%|█████████▉| 8958/9000 [06:58<00:01, 21.38it/s]
100%|█████████▉| 8961/9000 [06:58<00:01, 21.38it/s]
100%|█████████▉| 8964/9000 [06:58<00:01, 21.38it/s]
100%|█████████▉| 8967/9000 [06:58<00:01, 21.38it/s]
100%|█████████▉| 8970/9000 [06:59<00:01, 21.38it/s]
100%|█████████▉| 8973/9000 [06:59<00:01, 21.39it/s]
100%|█████████▉| 8976/9000 [06:59<00:01, 21.39it/s]
100%|█████████▉| 8979/9000 [06:59<00:00, 21.40it/s]
100%|█████████▉| 8982/9000 [06:59<00:00, 21.40it/s]
100%|█████████▉| 8985/9000 [06:59<00:00, 21.40it/s]
100%|█████████▉| 8988/9000 [06:59<00:00, 21.40it/s]
100%|█████████▉| 8991/9000 [07:00<00:00, 21.40it/s]
100%|█████████▉| 8994/9000 [07:00<00:00, 21.40it/s]
100%|█████████▉| 8997/9000 [07:00<00:00, 21.40it/s]
100%|██████████| 9000/9000 [07:00<00:00, 21.39it/s]
100%|██████████| 9000/9000 [07:00<00:00, 21.41it/s]
DEBUG:root:Got all results from the child processes
DEBUG:root:Terminated the processes
[DRIVER] Copied tomograms.star to /users/artem.kushner/dev/crboost_server/projects/post_tomo_correction/External/job011/tomograms.star
--- SLURM JOB END (Exit Code: 0) ---
--- SLURM JOB END (Exit Code: 0) ---
Creating RELION_JOB_EXIT_SUCCESS
(venv) ᢹ CBE-login [dev/crboost_server]                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                            
(venv) ᢹ CBE-login [dev/crboost_server]                                                                                                                                                                                                                                     
# Quick check -- what do the scores look like?                                                                                                                                                                                                                              
python3 -c "
import mrcfile, numpy as np
with mrcfile.open('projects/post_tomo_correction/External/job011/tmResults/post_tomo_correction_Position_1_scores.mrc') as m:
    d = m.data
    print(f'Shape: {d.shape}')
    print(f'Min: {d.min():.4f}, Max: {d.max():.4f}, Mean: {d.mean():.4f}, Std: {d.std():.4f}')
    # Top percentiles
    for p in [99, 99.5, 99.9, 99.95]:
        print(f'  {p}th percentile: {np.percentile(d, p):.4f}')
(venv) ᢹ CBE-login [dev/crboost_server] # Quick check -- what do the scores look like?                                                                                                                                                                                      
(venv) ᢹ CBE-login [dev/crboost_server] python3 -c "                                                                                                                                                                                                                        
dquote> import mrcfile, numpy as np
dquote> with mrcfile.open('projects/post_tomo_correction/External/job011/tmResults/post_tomo_correction_Position_1_scores.mrc') as m:
dquote>     d = m.data
dquote>     print(f'Shape: {d.shape}')
dquote>     print(f'Min: {d.min():.4f}, Max: {d.max():.4f}, Mean: {d.mean():.4f}, Std: {d.std():.4f}')
dquote>     # Top percentiles
dquote>     for p in [99, 99.5, 99.9, 99.95]:
dquote>         print(f'  {p}th percentile: {np.percentile(d, p):.4f}')
dquote> "



Shape: (504, 1006, 1006)
Min: -0.0040, Max: 0.0634, Mean: 0.0076, Std: 0.0025
  99th percentile: 0.0143
  99.5th percentile: 0.0152
  99.9th percentile: 0.0173
  99.95th percentile: 0.0182




# Vanilla crb:

(venv) ᢹ CBE-login [test1/run12] # 1. How many particles did the they extract?                                                                                                                                                                                              
(venv) ᢹ CBE-login [test1/run12] grep -c "Position_1" /groups/klumpe/software/Setup/Testing/test1/run12/External/job007/candidates.star                                                                                                                                     
1154
(venv) ᢹ CBE-login [test1/run12]                                                                                                                                                                                                                                            
                                                                                                                                                                                                                                                                            
(venv) ᢹ CBE-login [test1/run12] # 2. What were the they's TM job params?                                                                                                                                                                                                   
(venv) ᢹ CBE-login [test1/run12] cat /groups/klumpe/software/Setup/Testing/test1/run12/External/job006/job.star                                                                                                                                                             

# version 50001

data_job

_rlnJobTypeLabel             relion.external
_rlnJobIsContinue                       0
_rlnJobIsTomo                           0


# version 50001

data_joboptions_values

loop_
_rlnJobOptionVariable #1
_rlnJobOptionValue #2
  do_queue        Yes
    fn_exe crboost_match_template.py
  in_3dref /groups/klumpe/software/Setup/Testing/test1/run12/templates/ellipsoid_550_550_550_apix11.8_black.mrc
 in_coords         ""
   in_mask /groups/klumpe/software/Setup/Testing/test1/run12/templates/ellipsoid_550_550_550_apix11.8_mask.mrc
    in_mic External/job005/tomograms.star
    in_mov         ""
   in_part         ""
min_dedicated          1
nodespread       auto
nr_threads          1
other_args "--implementation pytom --volumeMaskFold None --gpu_ids auto \"
param10_label      split
param10_value      4:4:2
param1_label volumeColumn
param1_value rlnTomoReconstructedTomogram
param2_label templateSym
param2_value         C1
param3_label angularSearch
param3_value         90
param4_label nonSphericalMask
param4_value       True
param5_label bandPassFilter
param5_value       None
param6_label  ctfWeight
param6_value       True
param7_label  doseWeigh
param7_value       True
param8_label spectralWhitening
param8_value      False
param9_label randomPhaseCorrection
param9_value      False
qsub_extra1       auto
qsub_extra2         70
qsub_extra3       auto
qsub_extra4       auto
qsub_extra5         ""
qsubscript qsub/qsub_pytom.sh
 queuename       auto

(venv) ᢹ CBE-login [test1/run12]                                                                                                                                                                                                                                            
(venv) ᢹ CBE-login [test1/run12] # 3. What were the they's extraction params?                                                                                                                                                                                               
(venv) ᢹ CBE-login [test1/run12] cat /groups/klumpe/software/Setup/Testing/test1/run12/External/job007/job.star                                                                                                                                                             

# version 50001

data_job

_rlnJobTypeLabel             relion.external
_rlnJobIsContinue                       1
_rlnJobIsTomo                           0


# version 50001

data_joboptions_values

loop_
_rlnJobOptionVariable #1
_rlnJobOptionValue #2
  do_queue        Yes
    fn_exe crboost_extract_tm_candidates.py
  in_3dref         ""
 in_coords         ""
   in_mask         ""
    in_mic External/job006/tomograms.star
    in_mov         ""
   in_part         ""
min_dedicated          1
nodespread       auto
nr_threads          1
other_args "--implementation Pytom \"
param10_label         ""
param10_value         ""
param1_label cutOffMethod
param1_value NumberOfFalsePositives
param2_label cutOffValue
param2_value          1
param3_label particleDiameterInAng
param3_value        550
param4_label maxNumParticles
param4_value       1500
param5_label apixScoreMap
param5_value       auto
param6_label scoreFilterMethod
param6_value       None
param7_label scoreFilterValue
param7_value       None
param8_label MaskFoldPath
param8_value       None
param9_label         ""
param9_value         ""
qsub_extra1       auto
qsub_extra2         70
qsub_extra3       auto
qsub_extra4       auto
qsub_extra5         ""
qsubscript qsub/qsub_pytom.sh
 queuename       auto

(venv) ᢹ CBE-login [test1/run12]                                                                                                                                                                                                                                            
(venv) ᢹ CBE-login [test1/run12] # 4. Compare templates -- dimensions and pixel size                                                                                                                                                                                        
(venv) ᢹ CBE-login [test1/run12] python3 -c "                                                                                                                                                                                                                               
dquote> import mrcfile
dquote> for p in [
dquote>     '/groups/klumpe/software/Setup/Testing/test1/run12/templates/ellipsoid_550_550_550_apix11.8_black.mrc',
dquote>     'projects/post_tomo_correction/templates/ellipsoid_550_550_550_apix12.00_box96_lp40_black.mrc',
dquote> ]:
dquote>     with mrcfile.open(p) as m:
dquote>         print(f'{p.split(\"/\")[-1]}:')
dquote>         print(f'  shape={m.data.shape}, dtype={m.data.dtype}, voxel={float(m.voxel_size.x):.2f}')
dquote>         print(f'  min={m.data.min():.4f}, max={m.data.max():.4f}, mean={m.data.mean():.4f}')
dquote>         print()
dquote> "

ellipsoid_550_550_550_apix11.8_black.mrc:
  shape=(96, 96, 96), dtype=float32, voxel=11.80
  min=-4.2553, max=0.2817, mean=0.0000

Traceback (most recent call last):
  File "<string>", line 7, in <module>
  File "/users/artem.kushner/dev/crboost_server/venv/lib/python3.11/site-packages/mrcfile/load_functions.py", line 145, in open
    return NewMrc(name, mode=mode, permissive=permissive,
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/users/artem.kushner/dev/crboost_server/venv/lib/python3.11/site-packages/mrcfile/mrcfile.py", line 109, in __init__
    self._open_file(name)
  File "/users/artem.kushner/dev/crboost_server/venv/lib/python3.11/site-packages/mrcfile/mrcfile.py", line 126, in _open_file
    self._iostream = open(name, self._mode + 'b')
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: 'projects/post_tomo_correction/templates/ellipsoid_550_550_550_apix12.00_box96_lp40_black.mrc'
(venv) ᢹ CBE-login [test1/run12]                                                                                                                                                                                                                                            
(venv) ᢹ CBE-login [test1/run12] # 5. Same for masks                                                                                                                                                                                                                        
(venv) ᢹ CBE-login [test1/run12] python3 -c "                                                                                                                                                                                                                               
dquote> import mrcfile
dquote> for p in [
dquote>     '/groups/klumpe/software/Setup/Testing/test1/run12/templates/ellipsoid_550_550_550_apix11.8_mask.mrc',
dquote>     'projects/post_tomo_correction/templates/ellipsoid_550_550_550_apix12.00_box96_lp40_mask.mrc',
dquote> ]:
dquote>     with mrcfile.open(p) as m:
dquote>         print(f'{p.split(\"/\")[-1]}:')
dquote>         print(f'  shape={m.data.shape}, dtype={m.data.dtype}, voxel={float(m.voxel_size.x):.2f}')
dquote>         print(f'  nonzero={(m.data > 0.01).sum()} / {m.data.size}')
dquote>         print()
dquote> "
ellipsoid_550_550_550_apix11.8_mask.mrc:
  shape=(96, 96, 96), dtype=float32, voxel=11.80
  nonzero=134400 / 884736

Traceback (most recent call last):
  File "<string>", line 7, in <module>
  File "/users/artem.kushner/dev/crboost_server/venv/lib/python3.11/site-packages/mrcfile/load_functions.py", line 145, in open
    return NewMrc(name, mode=mode, permissive=permissive,
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/users/artem.kushner/dev/crboost_server/venv/lib/python3.11/site-packages/mrcfile/mrcfile.py", line 109, in __init__
    self._open_file(name)
  File "/users/artem.kushner/dev/crboost_server/venv/lib/python3.11/site-packages/mrcfile/mrcfile.py", line 126, in _open_file
    self._iostream = open(name, self._mode + 'b')
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: 'projects/post_tomo_correction/templates/ellipsoid_550_550_550_apix12.00_box96_lp40_mask.mrc'
(venv) ᢹ CBE-login [test1/run12]                                                                                                                                                                                                                                            
(venv) ᢹ CBE-login [test1/run12] # 6. they's score distribution for comparison                                                                                                                                                                                              
(venv) ᢹ CBE-login [test1/run12] python3 -c "                                                                                                                                                                                                                               
dquote> import mrcfile, numpy as np
dquote> with mrcfile.open('/groups/klumpe/software/Setup/Testing/test1/run12/External/job006/tmResults/Position_1_11.80Apx_scores.mrc') as m:
dquote>     d = m.data
dquote>     print(f'they scores: shape={d.shape}')
dquote>     print(f'  min={d.min():.4f}, max={d.max():.4f}, mean={d.mean():.4f}, std={d.std():.4f}')
dquote>     for p in [99, 99.5, 99.9, 99.95]:
dquote>         print(f'  {p}th: {np.percentile(d, p):.4f}')
dquote> "

their scores: shape=(512, 1024, 1024)
  min=-0.2025, max=0.2679, mean=0.0018, std=0.0173
  99th: 0.0486
  99.5th: 0.0589
  99.9th: 0.0908
  99.95th: 0.1075

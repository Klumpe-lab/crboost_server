# Roadmap 04 — Stage 2 transcripts: `[run_command]` ground truth

Harvested 2026-08-13 from the demo project `/groups/klumpe/crboost_data/stage4refactor_demo/`.
The `[run_command] $ <cmd>` echo was added in `drivers/driver_base.py` (commit `71a66ff`,
2026-08-13); every line below is the byte-exact string handed to the shell, captured from the
named log file. One echo per `run_command()` call; per-task commands land in `task_N.out`,
supervisor-level commands in `run.out`.

Run context: 5 tilt series (`stage4refactor_demo_Position_1` .. `_1_5`); `Position_1` was muted
in Journey before alignment, so alignment onward runs 4 TS. Task index → TS name follows
`.task_manifest.json` `items` order, reproduced per job below.

Job types still lacking transcripts after this harvest: denoise_train, denoise_predict,
templateMatching, extractCandidates, extract_pick_list, subtomo_extraction, subtomo_merge,
reconstruct_particle, class3d, miss_align, tilt_filter (interactive).

---

## fsMotionAndCtf — `External/job002` (`drivers/fs_motion_and_ctf.py`, SLURM array 1068362)

Task status: 5 × `.ok`. Manifest `items` (= task index order): `Position_1`, `_1_2`, `_1_3`, `_1_4`, `_1_5`.
Per-task command = `test -f warp_frameseries.settings || (WarpTools create_settings ...)` guard
chained with `&&` into `WarpTools fs_motion_and_ctf`. Note the `'"'"'` re-quoting around the
literal glob `*.eer`, the per-task `.staging/task_<TS>` bind mount, and the env-unset prefix
before `apptainer exec`. No supervisor-level command (`run.out` has no echo).

### task 0, TS stage4refactor_demo_Position_1 — `job002/task_0.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job002/.staging/task_stage4refactor_demo_Position_1 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'test -f warp_frameseries.settings || (WarpTools create_settings --folder_data frames --extension '"'"'*.eer'"'"' --folder_processing warp_frameseries --output warp_frameseries.settings --angpix 1.55 --eer_ngroups -32) && WarpTools fs_motion_and_ctf --settings warp_frameseries.settings --m_grid 1x1x3 --m_range_min 500 --m_range_max 10 --m_bfac -500 --c_grid 2x2x1 --c_window 512 --c_range_min 30.0 --c_range_max 6.0 --c_defocus_min 1.1 --c_defocus_max 8.0 --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --perdevice 2 --out_averages --out_skip_first 0 --out_skip_last 0 --out_average_halves'
```

### task 1, TS stage4refactor_demo_Position_1_2 — `job002/task_1.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job002/.staging/task_stage4refactor_demo_Position_1_2 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'test -f warp_frameseries.settings || (WarpTools create_settings --folder_data frames --extension '"'"'*.eer'"'"' --folder_processing warp_frameseries --output warp_frameseries.settings --angpix 1.55 --eer_ngroups -32) && WarpTools fs_motion_and_ctf --settings warp_frameseries.settings --m_grid 1x1x3 --m_range_min 500 --m_range_max 10 --m_bfac -500 --c_grid 2x2x1 --c_window 512 --c_range_min 30.0 --c_range_max 6.0 --c_defocus_min 1.1 --c_defocus_max 8.0 --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --perdevice 2 --out_averages --out_skip_first 0 --out_skip_last 0 --out_average_halves'
```

### task 2, TS stage4refactor_demo_Position_1_3 — `job002/task_2.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job002/.staging/task_stage4refactor_demo_Position_1_3 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'test -f warp_frameseries.settings || (WarpTools create_settings --folder_data frames --extension '"'"'*.eer'"'"' --folder_processing warp_frameseries --output warp_frameseries.settings --angpix 1.55 --eer_ngroups -32) && WarpTools fs_motion_and_ctf --settings warp_frameseries.settings --m_grid 1x1x3 --m_range_min 500 --m_range_max 10 --m_bfac -500 --c_grid 2x2x1 --c_window 512 --c_range_min 30.0 --c_range_max 6.0 --c_defocus_min 1.1 --c_defocus_max 8.0 --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --perdevice 2 --out_averages --out_skip_first 0 --out_skip_last 0 --out_average_halves'
```

### task 3, TS stage4refactor_demo_Position_1_4 — `job002/task_3.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job002/.staging/task_stage4refactor_demo_Position_1_4 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'test -f warp_frameseries.settings || (WarpTools create_settings --folder_data frames --extension '"'"'*.eer'"'"' --folder_processing warp_frameseries --output warp_frameseries.settings --angpix 1.55 --eer_ngroups -32) && WarpTools fs_motion_and_ctf --settings warp_frameseries.settings --m_grid 1x1x3 --m_range_min 500 --m_range_max 10 --m_bfac -500 --c_grid 2x2x1 --c_window 512 --c_range_min 30.0 --c_range_max 6.0 --c_defocus_min 1.1 --c_defocus_max 8.0 --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --perdevice 2 --out_averages --out_skip_first 0 --out_skip_last 0 --out_average_halves'
```

### task 4, TS stage4refactor_demo_Position_1_5 — `job002/task_4.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job002/.staging/task_stage4refactor_demo_Position_1_5 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'test -f warp_frameseries.settings || (WarpTools create_settings --folder_data frames --extension '"'"'*.eer'"'"' --folder_processing warp_frameseries --output warp_frameseries.settings --angpix 1.55 --eer_ngroups -32) && WarpTools fs_motion_and_ctf --settings warp_frameseries.settings --m_grid 1x1x3 --m_range_min 500 --m_range_max 10 --m_bfac -500 --c_grid 2x2x1 --c_window 512 --c_range_min 30.0 --c_range_max 6.0 --c_defocus_min 1.1 --c_defocus_max 8.0 --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --perdevice 2 --out_averages --out_skip_first 0 --out_skip_last 0 --out_average_halves'
```

---

## tsImport (post-TiltFilter) — `External/job003` (`drivers/ts_import.py`, single job, no array)

Single supervisor command in `run.out`: idempotence guard `test -d tomostar && ls
tomostar/*.tomostar >/dev/null 2>&1 || (WarpTools ts_import ...)` chained with `&&` into a
`test -f warp_tiltseries.settings ||` guard around `WarpTools create_settings`. Consumes the
TiltFilter-trimmed mdoc set; `--mdocs` points at the project-level `mdoc/` dir with a literal
`'*.mdoc'` pattern. Both globs use the `'"'"'` re-quote idiom.

### supervisor — `job003/run.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job003 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'test -d tomostar && ls tomostar/*.tomostar >/dev/null 2>&1 || (WarpTools ts_import --mdocs /groups/klumpe/crboost_data/stage4refactor_demo/mdoc --pattern '"'"'*.mdoc'"'"' --frameseries ../job002/warp_frameseries --output tomostar --tilt_exposure 3.0 --override_axis 84.4 --min_intensity 0.0 --dont_invert) && test -f warp_tiltseries.settings || (WarpTools create_settings --folder_data tomostar --extension '"'"'*.tomostar'"'"' --folder_processing warp_tiltseries --output warp_tiltseries.settings --angpix 1.55 --exposure 3.0 --tomo_dimensions 4096x4096x2048)'
```

---

## tsAlignment — `External/job006` (`drivers/ts_alignment.py`, SLURM array 1074446)

Task status: 4 × `.ok` + `Position_1.skip` (muted TS pre-marked → array task 0 never ran, no
`task_0.out`). Manifest `items` still enumerates all 5 TS — mute lives in task status, not in
enumeration. Per-task command is a single flat `WarpTools ts_aretomo` (no guard chain);
`--angpix 6.2` is the alignment binning (4× of raw 1.55). No supervisor command; aggregation
(`aligned_tilt_series.star`, `all_tilts.star`, adapter identity check) is Python-side.

### task 1, TS stage4refactor_demo_Position_1_2 — `job006/task_1.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job006/.staging/task_stage4refactor_demo_Position_1_2 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_aretomo --settings warp_tiltseries.settings --output_processing warp_tiltseries --angpix 6.2 --alignz 1800 --perdevice 1'
```

### task 2, TS stage4refactor_demo_Position_1_3 — `job006/task_2.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job006/.staging/task_stage4refactor_demo_Position_1_3 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_aretomo --settings warp_tiltseries.settings --output_processing warp_tiltseries --angpix 6.2 --alignz 1800 --perdevice 1'
```

### task 3, TS stage4refactor_demo_Position_1_4 — `job006/task_3.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job006/.staging/task_stage4refactor_demo_Position_1_4 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_aretomo --settings warp_tiltseries.settings --output_processing warp_tiltseries --angpix 6.2 --alignz 1800 --perdevice 1'
```

### task 4, TS stage4refactor_demo_Position_1_5 — `job006/task_4.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job006/.staging/task_stage4refactor_demo_Position_1_5 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_aretomo --settings warp_tiltseries.settings --output_processing warp_tiltseries --angpix 6.2 --alignz 1800 --perdevice 1'
```

---

## tsCtf — `External/job007` (`drivers/ts_ctf.py`, SLURM array 1074482)

Task status: 4 × `.ok`. Manifest `items`: `_1_2`, `_1_3`, `_1_4`, `_1_5` (muted TS dropped from
enumeration — contrast with job006 where it stayed in the manifest and was skip-marked).
Per-task command = flat `WarpTools ts_ctf`. Supervisor additionally runs `ts_defocus_hand
--check && ts_defocus_hand --set_flip` ONCE with absolute paths (per-task commands use
cwd-relative paths inside the task staging bind).

### task 0, TS stage4refactor_demo_Position_1_2 — `job007/task_0.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job007/.staging/task_stage4refactor_demo_Position_1_2 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_ctf --settings warp_tiltseries.settings --input_processing warp_tiltseries --output_processing warp_tiltseries --window 512 --range_low 30.0 --range_high 6.0 --defocus_min 1.1 --defocus_max 8.0 --voltage 300 --cs 2.7 --amplitude 0.1 --perdevice 1'
```

### task 1, TS stage4refactor_demo_Position_1_3 — `job007/task_1.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job007/.staging/task_stage4refactor_demo_Position_1_3 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_ctf --settings warp_tiltseries.settings --input_processing warp_tiltseries --output_processing warp_tiltseries --window 512 --range_low 30.0 --range_high 6.0 --defocus_min 1.1 --defocus_max 8.0 --voltage 300 --cs 2.7 --amplitude 0.1 --perdevice 1'
```

### task 2, TS stage4refactor_demo_Position_1_4 — `job007/task_2.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job007/.staging/task_stage4refactor_demo_Position_1_4 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_ctf --settings warp_tiltseries.settings --input_processing warp_tiltseries --output_processing warp_tiltseries --window 512 --range_low 30.0 --range_high 6.0 --defocus_min 1.1 --defocus_max 8.0 --voltage 300 --cs 2.7 --amplitude 0.1 --perdevice 1'
```

### task 3, TS stage4refactor_demo_Position_1_5 — `job007/task_3.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job007/.staging/task_stage4refactor_demo_Position_1_5 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_ctf --settings warp_tiltseries.settings --input_processing warp_tiltseries --output_processing warp_tiltseries --window 512 --range_low 30.0 --range_high 6.0 --defocus_min 1.1 --defocus_max 8.0 --voltage 300 --cs 2.7 --amplitude 0.1 --perdevice 1'
```

### supervisor (defocus handedness, runs once) — `job007/run.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job007 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_defocus_hand --settings /groups/klumpe/crboost_data/stage4refactor_demo/External/job007/warp_tiltseries.settings --output_processing /groups/klumpe/crboost_data/stage4refactor_demo/External/job007/warp_tiltseries --check && WarpTools ts_defocus_hand --settings /groups/klumpe/crboost_data/stage4refactor_demo/External/job007/warp_tiltseries.settings --output_processing /groups/klumpe/crboost_data/stage4refactor_demo/External/job007/warp_tiltseries --set_flip'
```

---

## tsReconstruct — `External/job008` (`drivers/ts_reconstruct.py`, SLURM array 1074519)

Task status (settled 2026-08-13 ~13:50): 4 × `.ok`, `RELION_JOB_EXIT_SUCCESS` written.
Manifest `items`: `_1_2`, `_1_3`, `_1_4`, `_1_5` (muted TS absent, same as job007). Per-task
command = flat `WarpTools ts_reconstruct`. Unlike ts_ctf tasks (cwd-relative paths), these use
ABSOLUTE paths into the per-task `.staging/task_<TS>/` copy for `--settings`/`--input_processing`,
while `--output_processing` points at the SHARED job-level `warp_tiltseries` — tasks write results
straight into the job dir.

### task 0, TS stage4refactor_demo_Position_1_2 — `job008/task_0.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job008 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_reconstruct --settings /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/.staging/task_stage4refactor_demo_Position_1_2/warp_tiltseries.settings --input_processing /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/.staging/task_stage4refactor_demo_Position_1_2/warp_tiltseries --output_processing /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/warp_tiltseries --angpix 6.2 --halfmap_frames 1 --deconv 1 --perdevice 1 --dont_invert'
```

### task 1, TS stage4refactor_demo_Position_1_3 — `job008/task_1.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job008 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_reconstruct --settings /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/.staging/task_stage4refactor_demo_Position_1_3/warp_tiltseries.settings --input_processing /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/.staging/task_stage4refactor_demo_Position_1_3/warp_tiltseries --output_processing /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/warp_tiltseries --angpix 6.2 --halfmap_frames 1 --deconv 1 --perdevice 1 --dont_invert'
```

### task 2, TS stage4refactor_demo_Position_1_4 — `job008/task_2.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job008 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_reconstruct --settings /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/.staging/task_stage4refactor_demo_Position_1_4/warp_tiltseries.settings --input_processing /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/.staging/task_stage4refactor_demo_Position_1_4/warp_tiltseries --output_processing /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/warp_tiltseries --angpix 6.2 --halfmap_frames 1 --deconv 1 --perdevice 1 --dont_invert'
```

### task 3, TS stage4refactor_demo_Position_1_5 — `job008/task_3.out`
```
[run_command] $ unset SINGULARITY_BIND APPTAINER_BIND SINGULARITY_BINDPATH APPTAINER_BINDPATH SINGULARITY_NAME APPTAINER_NAME SINGULARITY_CONTAINER APPTAINER_CONTAINER LD_PRELOAD XDG_RUNTIME_DIR CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER SLURM_JOBID SLURM_JOB_ID SLURM_NODELIST SLURM_STEP_NODELIST SLURM_NTASKS SLURM_PROCID SLURM_LOCALID SLURM_TASK_PID PMI_FD PMI_SIZE PMI_RANK PMIX_RANK OMPI_COMM_WORLD_SIZE OMPI_COMM_WORLD_RANK; apptainer exec --nv --cleanenv --no-home -B /etc/group:/etc/group:ro -B /etc/passwd:/etc/passwd:ro -B /groups -B /groups/klumpe/crboost_data/stage4refactor_demo/External/job008 -B /programs -B /run/munge -B /scratch -B /software -B /tmp -B /users/artem.kushner -B /usr/lib64/slurm /groups/klumpe/software/containers/sifs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif bash -c 'WarpTools ts_reconstruct --settings /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/.staging/task_stage4refactor_demo_Position_1_5/warp_tiltseries.settings --input_processing /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/.staging/task_stage4refactor_demo_Position_1_5/warp_tiltseries --output_processing /groups/klumpe/crboost_data/stage4refactor_demo/External/job008/warp_tiltseries --angpix 6.2 --halfmap_frames 1 --deconv 1 --perdevice 1 --dont_invert'
```


# Dose rate mystery
The mdoc says ExposureDose = 2.07. Your code does 2.07 * 1.5 = 3.105, which rounds to 3.1 -- exactly matching the ground truth. But your project_params.json shows 2.47. That does NOT match 2.07 * 1.5.
This means either: (a) the mdocs_glob /groups/klumpe/001_Data/.../mdoc/*.mdoc matched a different mdoc file first (one with ExposureDose = 1.647, since 1.647 * 1.5 = 2.47), or (b) the * 1.5 multiplier was added after you created this project and the original code just used the raw value with some other scaling.
Can you check: how many mdoc files are in that source directory? If there are multiple, the get_autodetect_params grabs whichever glob.glob returns first, which is filesystem-order-dependent. That would explain the inconsistency.
Also -- the * 1.5 comment says "Old code multiplied by 1.5 for some reason." Do you know the actual reason? For EER data with NumSubFrames = 1 and FrameDosesAndNumber = 2.07 1, the dose per tilt IS 2.07 e/A^2. The ground truth used 3.1, which happens to equal 2.07 * 1.5. But is 1.5 a correction factor specific to this detector/counting mode, or is it a general thing? If it's wrong for other datasets, it'll silently break every project.



```
The star file makes the source of 2.47 clear: `rlnMicrographPreExposure` steps are exactly 0.0, 2.47, 4.94, 7.41... -- a fixed 2.47 per tilt, computed by Warp from the mdoc. The GT dose file steps are 0.0, 3.1, 6.2... = 2.07 * 1.5 per tilt. So old CryoBoost deliberately overrides Warp's dose calculation with ExposureDose * 1.5, and pytom is sensitive to this difference.

The reason is probably that Warp is computing dose differently for EER -- possibly dividing by the number of internal EER sub-frames in some intermediate step. The exact mechanism doesn't matter much; the point is your star file has ~20% lower dose values than what pytom should see.

Before I give you the fix I need to know whether `rlnMicrographPreExposure` in that star file is written by Warp directly (i.e., it's the raw output of the ts_ctf job) or whether `metadata_service.py` is post-processing it. Can you show me `services/configs/metadata_service.py`, specifically the function(s) that build or update the tilt-series star file?

The fix I'm planning either goes in `metadata_service.py` (if it controls those values) or as a patching step in `template_match_pytom.py` right before pytom is called -- recomputing `rlnMicrographPreExposure` as `tilt_index * dose_per_tilt` using the mdoc service value (ExposureDose * 1.5 = 3.1). But I want to patch at the right place and not duplicate the logic.
```

# extract candidates???

"--particle-diameter", str(int(params.particle_diameter_ang / 2.0 / apix) * apix),
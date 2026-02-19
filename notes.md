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


# Phase flip (warp/pytom)

The confusion inthe ground truth is that the old code passes --tomogram-ctf-model phase-flip unconditionally, yet the resulting job.json shows flip_phase: false per tilt. These are different fields describing different things -- the old code was pairing phase-flip reconstruction model with non-flipped per-tilt CTF data, and that apparently worked. Whether this was correct or a coincidental cancellation is worth understanding. Your previous conversation conclusion that the ground truth has it "off" may have been conflating these two separate fields.





------ pytom 0.10 vs 0.12
## Summary
In pytom-match-pick 0.12, `--relion5-tomograms-star` reads tilt angles, dose, defocus, amplitude contrast, voltage, spherical aberration, and handedness from the STAR file and ignores CLI CTF flags. `flip_phase` in the output job.json comes from the `--tomogram-ctf-model phase-flip` flag and is stored in each `CtfData` entry.

## Details

### Fields read from RELION5 tomograms.star
`parse_relion5_star_data()` reads the following STAR columns into `RelionTiltSeriesMetaData` and `CtfData`:
- Tilt angles from `rlnTomoNominalStageTiltAngle` [1](#0-0) 
- Dose from `rlnMicrographPreExposure` [2](#0-1) 
- Defocus from the mean of `rlnDefocusU` and `rlnDefocusV` per tilt<citation repo="SBC-Utrecht/pytom-match-pick" path="src/pytom_tm/io.py" start="638" end="641" />
- Amplitude contrast from `rlnAmplitudeContrast` [3](#0-2) 
- Voltage from `rlnVoltage` (converted to eV) [4](#0-3) 
- Spherical aberration from `rlnSphericalAberration` (converted to m) [5](#0-4) 
- Defocus handedness from `rlnTomoHand` [6](#0-5) 
- Binning and tilt-series pixel size for coordinate conversion<citation repo="SBC-Utrecht/pytom-match-pick" path="src/pytom_tm/io.py" start="624" end="625" />

### CLI vs STAR precedence
When `--relion5-tomograms-star` is provided, the entry point calls `parse_relion5_star_data()` and uses its returned metadata, bypassing CLI CTF flags entirely<citation repo="SBC-Utrecht/pytom-match-pick" path="src/pytom_tm/entry_points.py" start="1029" end="1035" />. The CLI CTF flags are only used in manual mode (no RELION5/Warp)<citation repo="SBC-Utrecht/pytom-match-pick" path="src/pytom_tm/entry_points.py" start="1004" end="1027" />. Therefore, STAR values win; CLI `--amplitude-contrast`, `--voltage`, and `--spherical-aberration` are ignored in RELION5 mode.

### How flip_phase is set in job.json
`flip_phase` is set by the `--tomogram-ctf-model phase-flip` flag. In the entry point, this sets `phase_flip_correction = True` [7](#0-6) , which is passed to `parse_relion5_star_data()` and stored in each `CtfData` as `flip_phase=phase_flip_correction` [8](#0-7) . `CtfData` includes `flip_phase` as a field<citation repo="SBC-Utrecht/pytom-match-pick" path="src/pytom_tm/dataclass.py" start="21" end="22" />, and the job JSON serializes the entire `ts_metadata` including `ctf_data` [9](#0-8) .

## Notes
- In RELION5 mode, the job.json’s `ts_metadata.ctf_data[].amplitude_contrast` comes from the STAR file, not the CLI. This explains discrepancies if the STAR value differs from the passed CLI flag.
- `flip_phase` is not read from the STAR file; it is controlled solely by `--tomogram-ctf-model phase-flip` and applied uniformly across tilts in RELION5 mode.

Wiki pages you might want to explore:
- [pytom_match_template (SBC-Utrecht/pytom-match-pick)](/wiki/SBC-Utrecht/pytom-match-pick#2.3)
- [Core Concepts (SBC-Utrecht/pytom-match-pick)](/wiki/SBC-Utrecht/pytom-match-pick#3)

### Citations

**File:** src/pytom_tm/io.py (L617-617)
```python
    tilt_angles = list(tilt_series_star_data["rlnTomoNominalStageTiltAngle"])
```

**File:** src/pytom_tm/io.py (L618-618)
```python
    dose_accumulation = list(tilt_series_star_data["rlnMicrographPreExposure"])
```

**File:** src/pytom_tm/io.py (L627-627)
```python
    defocus_handedness = int(tomogram_meta_data["rlnTomoHand"])
```

**File:** src/pytom_tm/io.py (L632-632)
```python
            amplitude_contrast=tomogram_meta_data["rlnAmplitudeContrast"],
```

**File:** src/pytom_tm/io.py (L633-633)
```python
            voltage=tomogram_meta_data["rlnVoltage"] * 1e3,
```

**File:** src/pytom_tm/io.py (L634-634)
```python
            spherical_aberration=tomogram_meta_data["rlnSphericalAberration"] * 1e-3,
```

**File:** src/pytom_tm/io.py (L635-635)
```python
            flip_phase=phase_flip_correction,
```

**File:** src/pytom_tm/entry_points.py (L1000-1002)
```python
    phase_flip_correction = False
    if args.tomogram_ctf_model is not None and args.tomogram_ctf_model == "phase-flip":
        phase_flip_correction = True
```

**File:** src/pytom_tm/tmjob.py (L562-595)
```python
    @property
    def template_filter(self) -> npt.NDArray[float]:
        if self._template_filter is None:
            self._generate_filters()
        return self._template_filter

    def copy(self) -> TMJob:
        """Create a copy of the TMJob

        Returns
        -------
        job: TMJob
            copied TMJob instance
        """
        return copy.deepcopy(self)

    def write_to_json(self, file_name: pathlib.Path) -> None:
        """Write job to .json file.

        Note: This has to be run from the same cwd as where `self` was initiated
              otherwise the path resolving doesn't make sense

        Parameters
        ----------
        file_name: pathlib.Path
            path to the output file
        """
        d = self.__dict__.copy()
        d.pop("sub_jobs")
        d.pop("search_origin")
        d.pop("search_size")

        # pop cached numpy arrays that we don't want to dump
        for c in ["_tomogram_filter", "_template_filter"]:
```




----

# Warp The single global settings file
Using one global settings file is not wrong in principle, but it's dangerous here because DoPhase means different things in CTF estimation vs reconstruction context, and WarpTools uses the same XML field for both. If you want CTF estimation without phase-plate fitting and reconstruction with phase-flip correction, you need different values of DoPhase in different settings files. This is probably exactly what old CryoBoost did by copying/adapting the settings per step.


# handedness difference..

Defocus handedness in a tilt series refers to the geometric relationship between tilt angle and defocus. As you tilt the sample, one side of the sample gets closer to the objective lens and the other side gets farther away. Depending on the physical orientation of the sample relative to the lens, increasing tilt angle either increases or decreases the apparent defocus. This is what "hand" means.
WarpTools checks this empirically: it looks at the correlation between tilt angle and estimated defocus across your tilt series. If specimens tilted to higher angles consistently show higher defocus (further from the lens), correlation is negative and set_flip is the correct setting. If it's the other way around, correlation is positive and set_noflip is correct. The sign of the correlation tells you which physical orientation your sample is in.
This property is determined by the microscope hardware setup -- specifically the stage geometry and lens design -- so it should be constant across all datasets from the same instrument. It is not something that varies per-experiment.
Whether the discrepancy should worry you
The ground truth got -0.958 (strongly negative, clearly flip) and you got +0.394 (weakly positive, suggesting no-flip). These are opposite signs, which is surprising if both datasets came from the same microscope.
A few possible explanations, in rough order of likelihood:
The correlation in your check was computed on alignment results from job003 (the upstream AreTomo/Warp alignment job). If your alignment job produced lower quality results -- less precise per-tilt shifts and angles -- the correlation between tilt angle and defocus will be noisier and could flip sign spuriously. A value of 0.394 is not strong, while -0.958 is very decisive. A weak positive correlation on noisy alignment could easily be an artifact.
The two datasets could genuinely differ if the stage was remounted or the acquisition software settings changed between runs, but that's less likely.
The bottom line is: you should look at your alignment quality from job003 first. If the alignment was poor, the handedness check is unreliable and its output shouldn't be trusted blindly. The correct handedness for a given microscope is usually known -- it's a property of the instrument. Do you know which hand is correct for this scope? The ground truth used set_flip decisively, which is strong evidence that set_flip is right for this instrument. If that's confirmed, then auto mode is still useful as a sanity check, but the weak positive you got is likely noise from upstream alignment quality rather than a genuine reversal of hand.
So practically: for now I would set defocus_hand to "set_flip" explicitly as a project-level configuration, matching the ground truth, rather than relying on auto-detection. Change the auto default logic once you have more datasets with clean alignment to validate it against.

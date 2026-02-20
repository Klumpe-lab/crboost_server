I am building a cryoet orchestrator software (`crboost_server`) that basically runs the whole pipeline from the acquired data to reconstruction to template matching and subtomo averaging. It is a "modernized" port of the older system (`cryoboost`) that basically achieved all of these steps and implemented job submission to slurm, but was clanky and hard to extend (however for all its faults it was CORRECT).

Currently my problem is that somehow my template matching yields significantly lower number of picks than the groundtruth (GT) project run for this exact dataset. I need your help debugging this. My issue currently seems to be that no matter what i do my ctf values per tilt come out scrambled vis a vis the ground truth project. I want to really dig into my metadata processing and naming conventions etc, comapre with the way old cryoboost did it and verify that we are not doing anything silly. 

Let me show you the postmortem of yet another attempt to fix it with claude, then the two projects (the one that again faield to correctly estimate ctf and the grountruth project where all values are around -5.1u). Then i will show you the two repos -- crboost_server and cryoboost.


# Claude:

```bash
# 1. Check AreAnglesInverted is True in job004's XML
grep "AreAnglesInverted" /users/artem.kushner/dev/crboost_server/projects/ts_defocus_pre_ctf_fix/External/job004/warp_tiltseries/ts_defocus_pre_ctf_fix_Position_1.xml

# 2. Check raw CTF fits from the XML directly
python3 -c "
import xml.etree.ElementTree as ET
tree = ET.parse('/users/artem.kushner/dev/crboost_server/projects/ts_defocus_pre_ctf_fix/External/job004/warp_tiltseries/ts_defocus_pre_ctf_fix_Position_1.xml')
root = tree.getroot()
movies = [p.strip() for p in root.find('MoviePath').text.strip().split('\n') if p.strip()]
ctf_nodes = root.find('GridCTF').findall('Node')
for node, movie in zip(ctf_nodes, movies):
    print(f'Z={node.get(\"Z\")}  {float(node.get(\"Value\")):.4f} um  {movie.split(\"/\")[-1]}')
"

# 3. Check final star file defocus
python3 -c "
import starfile
d = starfile.read('/users/artem.kushner/dev/crboost_server/projects/ts_defocus_pre_ctf_fix/External/job004/tilt_series/ts_defocus_pre_ctf_fix_Position_1.star', always_dict=True)
df = list(d.values())[0]
print(df[['rlnTomoNominalStageTiltAngle', 'rlnDefocusU']].to_string())
print(f'\nDefocusU mean: {df.rlnDefocusU.mean():.1f} A = {df.rlnDefocusU.mean()/10000:.2f} um')
print(f'DefocusU std:  {df.rlnDefocusU.std():.1f} A')
"
```

We want: (1) `AreAnglesInverted="True"`, (2) all GridCTF values clustered around 5.3 µm, (3) mean ~53000 Å std under 2000 Å.

----
```
(venv) ᢹ CBE-login [dev/crboost_server] python3 -c "                       
dquote> import starfile
dquote> d = starfile.read('/users/artem.kushner/dev/crboost_server/projects/ts_defocus_pre_ctf_fix/Exte
rnal/job004/tilt_series/ts_defocus_pre_ctf_fix_Position_1.star', always_dict=True)
dquote> df = list(d.values())[0]
dquote> print(df[['rlnTomoNominalStageTiltAngle', 'rlnDefocusU']].to_string())
dquote> print(f'\nDefocusU mean: {df.rlnDefocusU.mean():.1f} A = {df.rlnDefocusU.mean()/10000:.2f} um')

dquote> print(f'DefocusU std:  {df.rlnDefocusU.std():.1f} A')
dquote> "
    rlnTomoNominalStageTiltAngle  rlnDefocusU
0                           9.98   53825.8136
1                          12.99   53696.2406
2                           6.99   53885.0946
3                           3.99   53512.6906
4                          15.99   53466.4456
5                          18.99   54030.5866
6                           1.00   27111.3146
7                          -2.01   52471.4606
8                          21.99   13028.6406
9                          24.99   57983.1406
10                         -5.00   51544.2806
11                         -8.01   33978.0446
12                         27.99   26386.2646
13                         30.99   32913.8676
14                        -11.01   42735.0606
15                        -14.00   42196.7706
16                         33.99   31602.8036
17                         36.99   28589.1356
18                        -17.01   34832.8086
19                        -20.00   34563.4006
20                         39.99   34702.3936
21                         42.99   28128.7946
22                        -23.01   52361.0136
23                        -26.01   36680.4666
24                         45.99   28465.4156
25                         48.99   34949.3606
26                        -29.01   22781.4106
27                        -32.01   50750.5956

DefocusU mean: 40041.9 A = 4.00 um
DefocusU std:  12296.6 A
```

---

## Summary of the defocus handedness investigation

### What the defocus hand check actually does

`ts_defocus_hand --check` correlates per-tilt defocus estimates from the frameseries XMLs (which already have CTF fitted from job002) against the expected tilt geometry. If the sample is tilted away from you, defocus should increase on the far side - a negative correlation means the sign convention is inverted and you need to flip. GT got -0.958 (decisive flip needed). Your runs kept returning near-zero correlations (0.185, 0.013), which we initially misread as a real signal. It was noise - the check was operating on wrong or mutated files and had no meaningful data to correlate.

### The in-place mutation problem

WarpTools `--input_processing` and `--output_processing` follow a single rule in `BaseOptions.Evaluate()`: if `--output_processing` is not specified, it falls back to `--input_processing`. This means any command that doesn't explicitly set `--output_processing` will write results back into whatever directory `--input_processing` points to.

Your original driver passed `--input_processing job003` to both `ts_defocus_hand` and `ts_ctf` without `--output_processing`. This caused `ts_defocus_hand` to write `AreAnglesInverted=True` directly into job003's XMLs, mutating upstream state. Confirmed by Deepwiki against WarpTools source.

### Why adding --output_processing to ts_defocus_hand alone didn't fix it

After the first fix attempt we gave both commands `--input_processing job003 --output_processing job004`. This correctly stopped mutating job003. But the sequence still broke:

1. `ts_defocus_hand --input_processing job003 --output_processing job004 --set_flip` writes a new XML into job004 with `AreAnglesInverted=True`
2. `ts_ctf --input_processing job003 --output_processing job004` reads from job003 (unflipped), fits CTF, and writes a fresh XML to job004 - overwriting the flip

The CTF-fitted XML that ts_ctf writes to job004 does not inherit `AreAnglesInverted` from the flip step. It starts fresh from whatever it reads at `--input_processing`. So the flip was silently discarded.

### What the GT workflow was actually doing right

GT copied job003's XMLs into job004 first, then ran all commands without any `--input_processing`. With no override, WarpTools resolves all paths from the settings file's `ProcessingFolder`, which GT had set to a relative `warp_tiltseries` - meaning job004's local copy. So:

1. `ts_defocus_hand --set_flip` wrote `AreAnglesInverted=True` into job004's XMLs
2. `ts_ctf` read from job004's XMLs (already flipped) and wrote CTF back to job004

Both commands operated on the same files. The flip survived into ts_ctf.

### The correct fix

Copy job003's XMLs into job004 first. Then:

- `ts_defocus_hand` uses only `--output_processing job004` (no `--input_processing`) - reads geometry from settings→job003, writes flip to job004
- `ts_ctf` uses `--input_processing job004 --output_processing job004` - reads the already-flipped XMLs and writes CTF results back to the same place

The copy step is load-bearing. Without it, `ts_defocus_hand` has no existing XML to modify in job004 and may do nothing or create an incomplete one.

### Why ts_ctf was fitting bad values

Even ignoring the flag issue, ts_ctf was producing scattered defocus (0.6-7.0 µm) instead of a clean ~5.3 µm. This is because fitting with the wrong handedness assumption distorts the geometric model ts_ctf uses to constrain per-tilt defocus. Tilts acquired early (lowest dose, near 0°) happened to fit cleanly because they don't rely heavily on the geometric model. High-tilt and late-acquisition tilts depend more on geometry and fell apart completely. This explained the puzzling pattern where rows 0-5 in the star were fine and everything else was garbage.

### The defocus_max issue

Separately, `defocus_max` was set to 6 µm while the actual defocus is ~5.3 µm. GT used 8 µm. A tight upper bound means any tilt where the fitter overshoots slightly has nowhere to land and returns garbage. This was changed to 8 µm as part of the fix and should stay at 8 µm as the default.

### The auto hand check reliability

The auto hand check returning near-zero correlation is most likely a consequence of the mutation problem - it was checking already-corrupted or wrong-location XMLs. Once the copy-first approach is in place and the check operates on clean job003 XMLs via the settings ProcessingFolder, it should return a decisive value like GT's -0.958. Whether to trust auto long-term depends on seeing a few more runs, but the current `set_flip` hardcode should be replaced with auto once we confirm the pipeline is working correctly, and exposed as a user-configurable param for edge cases.
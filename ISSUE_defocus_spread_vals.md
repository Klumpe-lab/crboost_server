

```
ᢹ CBE-login [projects/metadata_fixes] python3 -c "
dquote> import starfile, numpy as np
dquote>
dquote> for label, p in [
dquote>     ('GT',      '/groups/klumpe/software/Setup/Testing/test1/run12/External/job004/tilt_series/Position_1.star'),
dquote>     ('CURRENT', '/users/artem.kushner/dev/crboost_server/projects/metadata_fixes/External/job005/tilt_series/metadata_fixes_Positi
on_1.star'),
dquote> ]:
dquote>     d = starfile.read(p, always_dict=True)
dquote>     df = list(d.values())[0]
dquote>     u = np.array(df['rlnDefocusU']) / 10000  # convert Å to µm
dquote>     v = np.array(df['rlnDefocusV']) / 10000
dquote>     mean_def = (u + v) / 2
dquote>     print(f'{label}')
dquote>     print(f'  mean defocus range: {mean_def.min():.2f} - {mean_def.max():.2f} µm')
dquote>     print(f'  mean={mean_def.mean():.2f}  std={mean_def.std():.2f} µm')
dquote>     print(f'  all values: {np.round(mean_def, 2).tolist()}')
dquote> "
GT
  mean defocus range: 3.21 - 5.53 µm
  mean=5.21  std=0.39 µm
  all values: [5.32, 5.33, 5.29, 5.26, 5.31, 5.32, 5.27, 5.22, 5.34, 5.33, 5.19, 5.2, 5.4, 5.36, 5.18, 5.25, 5.37, 5.39, 5.19, 5.16, 5.36, 5.38, 5.17, 5.15, 3.21, 5.53, 5.2, 5.14]
CURRENT
  mean defocus range: 2.43 - 5.33 µm
  mean=4.02  std=0.88 µm
  all values: [5.33, 5.33, 5.3, 5.27, 5.31, 5.32, 3.45, 3.42, 2.9, 3.12, 5.16, 3.69, 3.42, 3.49, 3.62, 3.92, 3.48, 3.44, 3.57, 3.7, 3.66, 3.6, 5.14, 3.5, 3.33, 3.64, 2.43, 4.98]
ᢹ CBE-login [projects/metadata_fixes] python3 -c "
dquote> import starfile, numpy as np
dquote>
dquote> p = '/users/artem.kushner/dev/crboost_server/projects/metadata_fixes/External/job005/tilt_series/metadata_fixes_Position_1.star'
dquote> d = starfile.read(p, always_dict=True)
dquote> df = list(d.values())[0]
dquote> u = np.array(df['rlnDefocusU']) / 10000
dquote> v = np.array(df['rlnDefocusV']) / 10000
dquote> mean_def = (u + v) / 2
dquote> tilts = np.array(df['rlnTomoYTilt'])
dquote>
dquote> print('tilt_angle   defocus_um')
dquote> for t, def_ in sorted(zip(tilts, mean_def)):
dquote>     flag = '  <-- LOW' if def_ < 4.5 else ''
dquote>     print(f'  {t:+6.1f}     {def_:.2f}{flag}')
dquote> "
tilt_angle   defocus_um
   -37.0     3.64  <-- LOW
   -34.0     3.33  <-- LOW
   -31.0     3.60  <-- LOW
   -28.0     3.66  <-- LOW
   -25.0     3.44  <-- LOW
   -22.0     3.48  <-- LOW
   -19.0     3.49  <-- LOW
   -16.0     3.42  <-- LOW
   -13.0     3.12  <-- LOW
   -10.0     2.90  <-- LOW
    -7.0     5.32
    -4.0     5.31
    -1.0     5.33
    +2.0     5.33
    +5.0     5.30
    +8.0     5.27
   +11.0     3.45  <-- LOW
   +14.0     3.42  <-- LOW
   +17.0     5.16
   +20.0     3.69  <-- LOW
   +23.0     3.62  <-- LOW
   +26.0     3.92  <-- LOW
   +29.0     3.57  <-- LOW
   +32.0     3.70  <-- LOW
   +35.0     5.14
   +38.0     3.50  <-- LOW
   +41.0     2.43  <-- LOW
   +44.0     4.98

```

This is a very clear and systematic pattern, not random scatter. All negative tilts beyond ±10° are wrong, plus a scattered set of positive tilts. The near-zero tilts (-7 to +8) are correct at ~5.3 µm.

This is Warp's CTF fitter landing in a local minimum at ~3.4 µm for tilts with poor SNR. At high tilt angles the sample appears thicker, the signal is weaker, and the 1D CTF power spectrum becomes noisy enough that the fitter can't distinguish the correct ~5.3 µm solution from a spurious ~3.4 µm one. Your `defocus_min_max` is `"1.1:6"` -- plenty of room for this wrong minimum.

The GT result shows this is solvable because their values are uniformly ~5.3 µm even at high tilts. The fix is to constrain the search range so the wrong minimum is outside it. Since you know the true defocus is ~5.3 µm, use `"4.5:6.5"` instead of `"1.1:6"`.

Change this in your UI or directly in `project_params.json` under `tsCtf.defocus_min_max`, then re-run from job005 onward. Everything downstream (job006 reconstruct, job008 TM) needs to re-run after that since they all consume the CTF STAR.

While you're at it, also fix the template path in the `templatematching` job -- `project_params.json` currently has:

```
"template_path": ".../ellipsoid_550_550_550_apix11.80_seed.mrc",
"mask_path":     ".../ellipsoid_550_550_550_apix11.80_seed_mask.mrc",
```


Both `template_path` entries (there are two -- one under `paths` and one at the job level) need to point to `_black.mrc`. The mask is fine pointing to `seed_mask.mrc`.
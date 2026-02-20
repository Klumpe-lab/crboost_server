# Template comparison


```
python3 -c "
import mrcfile, numpy as np

pairs = [
    ('GT black',      '/groups/klumpe/software/Setup/Testing/test1/run12/templates/ellipsoid_550_550_550_apix11.8_black.mrc'),
    ('CURRENT black', '/users/artem.kushner/dev/crboost_server/projects/metadata_fixes/templates/ellipsoid_550_550_550_apix11.80_box96_lp45_black.mrc'),
    ('CURRENT seed',  '/users/artem.kushner/dev/crboost_server/projects/metadata_fixes/templates/ellipsoid_550_550_550_apix11.80_seed.mrc'),
]
for label, p in pairs:
    with mrcfile.open(p, permissive=True) as m:
        d = m.data
        nz = np.count_nonzero(d)
        print(f'{label}')
        print(f'  shape={d.shape}  apix={float(m.voxel_size.x):.2f}')
        print(f'  min={d.min():.4f}  max={d.max():.4f}  mean={d.mean():.4f}')
        print(f'  nonzero={nz}/{d.size} ({100*nz/d.size:.1f}%)')
"
```


# Scoremap comparison

```
python3 -c "
import mrcfile, numpy as np

pairs = [
    ('GT',      '/groups/klumpe/software/Setup/Testing/test1/run12/External/job006/tmResults/Position_1_11.80Apx_scores.mrc'),
    ('CURRENT', '/users/artem.kushner/dev/crboost_server/projects/metadata_fixes/External/job008/tmResults/metadata_fixes_Position_1_scores.mrc'),
]
for label, p in pairs:
    with mrcfile.open(p, permissive=True) as m:
        d = m.data
        print(f'{label}')
        print(f'  max={d.max():.4f}  mean={d.mean():.4f}  std={d.std():.4f}')
        print(f'  top-1pct threshold: {np.percentile(d, 99):.4f}')
"
```



python3 -c "
import mrcfile, numpy as np

pairs = [
    ('GT',      '/groups/klumpe/software/Setup/Testing/test1/run12/External/job006/tmResults/Position_1_11.80Apx_scores.mrc'),
    ('CURRENT', '/users/artem.kushner/dev/crboost_server/projects/tm_bench_fixes/External/job007/tmResults/tm_bench_fixes_Position_1_scores.mrc'),
]
for label, p in pairs:
    with mrcfile.open(p, permissive=True) as m:
        d = m.data
        print(f'{label}')
        print(f'  max={d.max():.4f}  mean={d.mean():.4f}  std={d.std():.4f}')
        print(f'  top-1pct threshold: {np.percentile(d, 99):.4f}')
"
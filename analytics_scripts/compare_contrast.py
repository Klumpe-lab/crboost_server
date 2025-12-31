import mrcfile
import numpy as np

projects = {
    "OLD (white template, LP=20)": "/users/artem.kushner/dev/crboost_server/projects/warp_dev36_11/External/job024/tmResults/warp_dev36_11_Position_1_12.00Apx_scores.mrc",
    "NEW (black template, LP=40)": "/users/artem.kushner/dev/crboost_server/projects/dec_26/External/job009/tmResults/dec_26_Position_1_12.00Apx_scores.mrc",
}

for name, path in projects.items():
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")
    
    with mrcfile.open(path) as m:
        d = m.data
        
    print(f"Shape: {d.shape}")
    print(f"Min:   {d.min():.5f}")
    print(f"Max:   {d.max():.5f}")
    print(f"Mean:  {d.mean():.5f}")
    print(f"Std:   {d.std():.5f}")
    print()
    print("Percentiles:")
    for p in [90, 95, 99, 99.5, 99.9]:
        print(f"  {p:5.1f}%: {np.percentile(d, p):.5f}")
    print()
    print("Voxels above threshold:")
    for t in [0.03, 0.04, 0.05, 0.07, 0.10, 0.15]:
        count = (d > t).sum()
        print(f"  > {t:.2f}: {count:>8,}")
#!/usr/bin/env python3
import json
import numpy as np
import mrcfile
from pathlib import Path

def load_mrc(p):
    with mrcfile.open(p, permissive=True) as m:
        v = np.asarray(m.data, dtype=np.float32)
    return v

def load_search_std(job_json):
    j = json.loads(Path(job_json).read_text())
    # Try a few common locations/keys (varies by version)
    for key in ["SearchStd", "search_std", "score_std", "std", "sigma"]:
        if key in j and isinstance(j[key], (int, float)):
            return float(j[key])
    # sometimes nested:
    for k1 in ["search", "match", "template_matching", "result", "statistics"]:
        if k1 in j and isinstance(j[k1], dict):
            for key in ["SearchStd", "search_std", "std", "sigma"]:
                if key in j[k1] and isinstance(j[k1][key], (int, float)):
                    return float(j[k1][key])
    return None

def robust_stats(x):
    x = x[np.isfinite(x)]
    return dict(
        n=x.size,
        mean=float(x.mean()),
        std=float(x.std()),
        p1=float(np.percentile(x, 1)),
        p5=float(np.percentile(x, 5)),
        p50=float(np.percentile(x, 50)),
        p95=float(np.percentile(x, 95)),
        p99=float(np.percentile(x, 99)),
        max=float(x.max()),
    )

def compare(a_scores, a_job, b_scores, b_job, sample=5_000_000):
    A = load_mrc(a_scores)
    B = load_mrc(b_scores)

    # Optional: sample to keep things fast
    rng = np.random.default_rng(0)
    def sample_flat(V):
        f = V.ravel()
        f = f[np.isfinite(f)]
        if f.size <= sample:
            return f
        idx = rng.choice(f.size, size=sample, replace=False)
        return f[idx]

    a_std = load_search_std(a_job)
    b_std = load_search_std(b_job)

    print("A scores:", a_scores)
    print("B scores:", b_scores)
    print("A SearchStd:", a_std)
    print("B SearchStd:", b_std)

    a = sample_flat(A)
    b = sample_flat(B)

    print("\nRaw score stats")
    print("A:", robust_stats(a))
    print("B:", robust_stats(b))

    if a_std and b_std:
        az = a / a_std
        bz = b / b_std
        print("\nZ~score stats (score/SearchStd)")
        print("A:", robust_stats(az))
        print("B:", robust_stats(bz))

        # Tail comparison: how many voxels exceed various sigma thresholds
        for t in [2, 3, 4, 5, 6]:
            fa = float((az > t).mean())
            fb = float((bz > t).mean())
            print(f"frac(Z>{t}): A={fa:.3e}  B={fb:.3e}  ratio(B/A)={(fb/fa if fa>0 else np.inf):.2f}")

if __name__ == "__main__":
    # EDIT THESE
    a_scores = "/users/artem.kushner/dev/crboost_server/projects/zval_fixes/External/job006/tmResults/zval_fixes_Position_1_scores.mrc"
    a_job    = "/users/artem.kushner/dev/crboost_server/projects/zval_fixes/External/job006/tmResults/zval_fixes_Position_1_job.json"

    b_scores = "/groups/klumpe/software/Setup/Testing/test1/run12/External/job006/tmResults/Position_1_11.80Apx_scores.mrc"
    b_job    = "/groups/klumpe/software/Setup/Testing/test1/run12/External/job006/tmResults/Position_1_11.80Apx_job.json"

    compare(a_scores, a_job, b_scores, b_job)


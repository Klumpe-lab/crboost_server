"""Two runs of the same tilt series compared per tilt series, joined with the GPU device each task of run A used.

    python3 -I compare_runs_by_device.py <project A> <project B> <out.json>

Motion (frame XMLs: Defocus, GridMovementX), alignment (AreTomo2 .st.aln rows), tilt-series CTF is left to the
reader, tomogram centres (64 x 256 x 256 at 6.20 A). Job numbers are the standard chain: job002 motion, job004
alignment, job006 reconstruct. The device comes from the task header line the rendered sbatch prints
("[SLURM] job ... on <node> (g), GPU <uuid>"): a MIG uuid is a slice; cbe-g3 RTX 6000, cbe-g4 A100, cbe-g5 B200.
Written for the balance pilot of roadmap 47 (orchestrator_test_FCIsolation against anglestest_FCIsolation).
"""

import collections
import glob
import json
import re
import sys
from pathlib import Path
import numpy as np
import mrcfile

_pa, _pb = Path(sys.argv[1]), Path(sys.argv[2])
A, B = _pa / "External", _pb / "External"
PA, PB = _pa.name + "_", _pb.name + "_"


def devices(job):
    """ts -> device class from the task headers: '[SLURM] job ... cb-x on <node> (g), GPU <uuid>'."""
    out = {}
    for f in glob.glob(str(A / job / "task_*.out")):
        head = open(f, errors="replace").read(4000)
        m = re.search(r"on (cbe-g\d)-\d+ \(g\), GPU (\S+)", head)
        ts = re.search(r"\[(?:TASK|DRIVER)\][^\n]*?(Position_\S+)", head) or re.search(r"(Position_[0-9_]+)", head)
        if not m or not ts:
            continue
        node, uuid = m.group(1), m.group(2)
        cls = {"cbe-g3": "RTX6000", "cbe-g4": "A100", "cbe-g5": "B200"}[node] + (
            "-slice" if uuid.startswith("MIG") else ""
        )
        out[ts.group(1).rstrip(".,:)")] = cls
    return out


def defocus(p):
    m = re.search(r'<Param Name="Defocus" Value="([^"]+)"', Path(p).read_text())
    return float(m.group(1)) if m else None


def grid(p, name):
    m = re.search(rf"<{name}[^>]*>(.*?)</{name}>", Path(p).read_text(), re.S)
    return [float(v) for v in re.findall(r'Value="([^"]+)"', m.group(1))] if m else None


def aln_rows(p):
    rows = []
    for line in Path(p).read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        try:
            rows.append([float(v) for v in line.split()])
        except ValueError:
            break
    return np.array(rows)


def centre_corr(a, b):
    with mrcfile.mmap(a, permissive=True) as ma, mrcfile.mmap(b, permissive=True) as mb:
        da, db = ma.data, mb.data
        if da.shape != db.shape:
            return float("nan")
        z, y, x = (s // 2 for s in da.shape)
        ca = np.asarray(da[z - 32 : z + 32, y - 128 : y + 128, x - 128 : x + 128], dtype=np.float64).ravel()
        cb = np.asarray(db[z - 32 : z + 32, y - 128 : y + 128, x - 128 : x + 128], dtype=np.float64).ravel()
        return float(np.corrcoef(ca, cb)[0, 1])


dev_motion, dev_align, dev_recon = devices("job002"), devices("job004"), devices("job006")
ts_list = sorted(
    p.name.replace(PA, "").replace(".st.aln", "") for p in (A / "job004/warp_tiltseries/tiltstack").glob("*/*.st.aln")
)
rows = []
for ts in ts_list:
    r = {
        "ts": ts,
        "dev_motion": dev_motion.get(ts, "?"),
        "dev_align": dev_align.get(ts, "?"),
        "dev_recon": dev_recon.get(ts, "?"),
    }
    # motion: frames of this tilt series
    fr_a = sorted((A / "job002/warp_frameseries").glob(f"{PA}{ts}_[0-9][0-9][0-9]_*.xml"))
    nd = nm = 0
    worst_d = worst_m = 0.0
    for xa in fr_a:
        xb = B / "job002/warp_frameseries" / xa.name.replace(PA, PB)
        if not xb.exists():
            continue
        da, db = defocus(xa), defocus(xb)
        if da is not None and db is not None:
            d = abs(da - db)
            worst_d = max(worst_d, d)
            nd += d > 0.05
        ga, gb = grid(xa, "GridMovementX"), grid(xb, "GridMovementX")
        if ga and gb and len(ga) == len(gb):
            mm = max(abs(p - q) for p, q in zip(ga, gb, strict=False))
            worst_m = max(worst_m, mm)
            nm += mm > 1
    r.update(frames=len(fr_a), fr_defocus_gt005=nd, fr_motion_gt1px=nm, worst_defocus=worst_d, worst_motion_px=worst_m)
    # alignment
    aa = A / "job004/warp_tiltseries/tiltstack" / f"{PA}{ts}" / f"{PA}{ts}.st.aln"
    ab = B / "job004/warp_tiltseries/tiltstack" / f"{PB}{ts}" / f"{PB}{ts}.st.aln"
    if aa.exists() and ab.exists():
        ra, rb = aln_rows(aa), aln_rows(ab)
        if len(ra) == len(rb) and len(ra):
            d = np.abs(ra - rb)
            r.update(
                tilts=len(ra),
                rot_diff=float(d[:, 1].max()),
                shift_diff=float(d[:, 3:5].max()),
                tilts_shift_gt1=int((d[:, 3:5].max(axis=1) > 1).sum()),
            )
        else:
            r.update(
                tilts=f"{len(ra)} vs {len(rb)}", rot_diff=float("nan"), shift_diff=float("nan"), tilts_shift_gt1=-1
            )
    # tomogram
    ta = A / "job006/warp_tiltseries/reconstruction" / f"{PA}{ts}_6.20Apx.mrc"
    tb = B / "job006/warp_tiltseries/reconstruction" / f"{PB}{ts}_6.20Apx.mrc"
    r["tomo_corr"] = centre_corr(ta, tb) if ta.exists() and tb.exists() else float("nan")
    rows.append(r)

print(f"tilt series compared: {len(rows)}")
print("\n== devices used today (per stage)")
for k in ("dev_motion", "dev_align", "dev_recon"):
    print(f"  {k:10s}", dict(collections.Counter(r[k] for r in rows)))


def summarize(key, pred, label):
    print(f"\n== {label}")
    by = collections.defaultdict(lambda: [0, 0])
    for r in rows:
        by[r[key]][1] += 1
        by[r[key]][0] += bool(pred(r))
    for d, (n, t) in sorted(by.items()):
        print(f"  {d:12s} {n:3d} of {t:3d}")


summarize(
    "dev_motion",
    lambda r: r["fr_defocus_gt005"] > 0 or r["fr_motion_gt1px"] > 0,
    "motion: tilt series with any frame off by >0.05 um defocus or >1 px motion, by motion device",
)
summarize(
    "dev_align",
    lambda r: isinstance(r.get("shift_diff"), float) and r["shift_diff"] > 1,
    "alignment: tilt series whose .st.aln differs by >1 px on any tilt, by alignment device",
)
summarize(
    "dev_align",
    lambda r: isinstance(r.get("shift_diff"), float) and r["shift_diff"] > 20,
    "alignment: tilt series differing by >20 px (a different solution), by alignment device",
)
summarize(
    "dev_recon",
    lambda r: not (r["tomo_corr"] > 0.99),
    "tomogram: centre correlation with yesterday's below 0.99, by reconstruct device",
)
print("\n== the tilt series that differ most (alignment shift > 1 px or tomogram corr < 0.99)")
print(
    f"  {'tilt series':16s} {'motion dev':11s} {'align dev':11s} {'frames>0.05um':>13s} {'frames>1px':>10s} "
    f"{'worst def':>9s} {'tilts':>5s} {'shift px':>8s} {'tilts>1px':>9s} {'tomo corr':>9s}"
)
for r in sorted(rows, key=lambda r: -(r.get("shift_diff") or 0)):
    if (isinstance(r.get("shift_diff"), float) and r["shift_diff"] > 1) or not (r["tomo_corr"] > 0.99):
        print(
            f"  {r['ts']:16s} {r['dev_motion']:11s} {r['dev_align']:11s} {r['fr_defocus_gt005']:13d} "
            f"{r['fr_motion_gt1px']:10d} {r['worst_defocus']:9.2f} {r.get('tilts')!s:>5s} "
            f"{r.get('shift_diff', float('nan')):8.1f} {r.get('tilts_shift_gt1', -1):9d} {r['tomo_corr']:9.4f}"
        )
json.dump(rows, open(sys.argv[3], "w"), indent=0, default=str)

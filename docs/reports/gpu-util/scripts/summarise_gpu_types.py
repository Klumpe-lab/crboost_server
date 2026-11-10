"""P13's tables from probe_gpu_types.sh result folders, one column per card type.

    python3 -B summarise_gpu_types.py /scratch/users/artem.kushner/gpu_probe/gpu_types_*

Per stage: median seconds per item, and what an item costs at production's request on that card: Slurm bills
max(CPUs, 0.134 per GB, the card's weight) per hour. The template-matching score maps are compared across cards
(against the full B200's) only where numpy imports: run it with the crboost env's python for that row.
"""

import json
import statistics
import sys
from pathlib import Path

# Partition g's TRESBillingWeights (scontrol show partition g, 2026-11-09).
WEIGHTS = {"b200_slice": 3.43, "b200": 24.0, "a100_slice": 3.0, "a100": 6.0, "rtx6000": 2.0}
LABELS = {
    "b200": "B200",
    "b200_slice": "B200 1g.23gb",
    "a100": "A100 40GB",
    "a100_slice": "A100 3g.20gb",
    "rtx6000": "RTX 6000",
}
# Production's request per task (config/conf.yaml): CPUs, memory in GB.
REQUEST = {"motion": (2, 24), "align": (2, 16), "ctf": (2, 16), "recon": (4, 24), "tm": (4, 32)}
STAGES = [
    ("motion", "Warp motion + CTF, a tilt series"),
    ("align", "AreTomo2 alignment, a tilt series"),
    ("ctf", "Warp CTF, a tilt series"),
    ("recon", "Warp reconstruct, a tilt series"),
    ("tm_warmup", "pytom, one rotation (start-up)"),
    ("tm", "pytom, a tomogram, I1 at 5°"),
]


def card_kind(gpu: str) -> str:
    g = gpu.upper()
    if "B200" in g:
        return "b200_slice" if "MIG" in g else "b200"
    if "A100" in g:
        return "a100_slice" if "MIG" in g else "a100"
    if "RTX 6000" in g:
        return "rtx6000"
    return gpu


def read_tsv(path: Path) -> list[list[str]]:
    return [line.rstrip("\n").split("\t") for line in path.read_text().splitlines()] if path.exists() else []


def main() -> int:
    runs = {}
    for res in map(Path, sys.argv[1:]):
        card = json.loads((res / "card.json").read_text()) if (res / "card.json").exists() else None
        if card is None:
            print(f"{res}: no card.json (not started, or failed before the card was read)")
            continue
        kind = card_kind(card["gpu"])
        if kind in runs:
            print(f"{res}: a second {kind} run, {runs[kind]['res']} kept")
            continue
        runs[kind] = {
            "res": res,
            "card": card,
            "runs": read_tsv(res / "runs.tsv"),
            "compare": read_tsv(res / "compare.tsv"),
            "checks": read_tsv(res / "checks.tsv"),
        }
    kinds = [k for k in LABELS if k in runs] + [k for k in runs if k not in LABELS]
    if not kinds:
        return 1

    head = "| | " + " | ".join(LABELS.get(k, k) for k in kinds) + " |"
    rule = "|---|" + "---:|" * len(kinds)
    print("## Cards\n")
    print(head)
    print(rule)
    for label, get in (
        ("node", lambda r: r["card"]["node"]),
        ("job", lambda r: r["card"]["slurm_job"]),
        ("compute capability", lambda r: r["card"]["compute_cap"]),
        ("memory GiB", lambda r: f"{r['card']['memory_bytes'] / 2**30:.1f}"),
        ("billing weight", lambda r: WEIGHTS.get(card_kind(r["card"]["gpu"]), "?")),
    ):
        print(f"| {label} | " + " | ".join(str(get(runs[k])) for k in kinds) + " |")
    tools = sorted({row[0] for k in kinds for row in runs[k]["checks"]})
    for tool in tools:
        cells = []
        for k in kinds:
            hit = [row for row in runs[k]["checks"] if row[0] == tool]
            cells.append(hit[0][1] if hit else "")
        print(f"| kernel: {tool} | " + " | ".join(cells) + " |")

    print("\n## Seconds per item (median over the items that succeeded; failures counted)\n")
    print(head)
    print(rule)
    secs = {}
    for stage, label in STAGES:
        cells = []
        for k in kinds:
            rows = [r for r in runs[k]["runs"] if r[0] == stage]
            good = [float(r[4]) for r in rows if r[5] == "0"]
            secs[(stage, k)] = statistics.median(good) if good else None
            bad = len(rows) - len(good)
            cell = f"{secs[(stage, k)]:.0f}" if good else "–"
            cells.append(cell + (f" ({bad} failed)" if bad else ""))
        print(f"| {label} | " + " | ".join(cells) + " |")

    print("\n## Billing-hours per 100 items at production's request (CPUs, GB as in config/conf.yaml)\n")
    print(head)
    print(rule)
    for stage, label in STAGES:
        if stage not in REQUEST:
            continue
        cpus, gb = REQUEST[stage]
        cells = []
        for k in kinds:
            s = secs.get((stage, k))
            w = WEIGHTS.get(k)
            cells.append(f"{100 * s / 3600 * max(cpus, 0.134 * gb, w):.2f}" if s is not None and w is not None else "–")
        print(f"| {label} ({cpus} CPUs, {gb}G) | " + " | ".join(cells) + " |")

    print("\n## Outputs against the pilot's (worst item)\n")
    print(head)
    print(rule)
    measures = sorted({(row[0], row[2]) for k in kinds for row in runs[k]["compare"]})
    for stage, what in measures:
        cells = []
        for k in kinds:
            vals = [row[3] for row in runs[k]["compare"] if row[0] == stage and row[2] == what]
            cells.append(_worst(what, vals))
        print(f"| {stage}: {what} | " + " | ".join(cells) + " |")

    try:
        import numpy as np
    except ImportError:
        print("\nScore maps across cards: needs numpy (run with the crboost env's python).")
        return 0
    if "b200" not in runs:
        print("\nScore maps across cards: no full-B200 run to compare with.")
        return 0
    ref_files = sorted((runs["b200"]["res"] / "crops").glob("tm_*.npy"))
    print("\n## Score-map centre against the full B200's\n")
    print(head)
    print(rule)
    for ref in ref_files:
        a = np.load(ref).astype(np.float64).ravel()
        cells = []
        for k in kinds:
            other = runs[k]["res"] / "crops" / ref.name
            if not other.exists():
                cells.append("–")
                continue
            b = np.load(other).astype(np.float64).ravel()
            cells.append(f"r {np.corrcoef(a, b)[0, 1]:.5f}, max |d| {np.abs(a - b).max():.2g}")
        print(f"| {ref.stem[3:]} | " + " | ".join(cells) + " |")
    return 0


def _worst(what: str, vals: list[str]) -> str:
    """The worst of an item's values: the largest difference, the smallest correlation, else all of them."""
    nums = []
    for v in vals:
        try:
            nums.append(float(v))
        except ValueError:
            return "; ".join(sorted(set(vals)))
    if not nums:
        return ""
    if "corr" in what:
        return f"{min(nums):.5f}"
    if "diff" in what:
        return f"{max(nums):.4g}"
    return "; ".join(sorted(set(vals)))


if __name__ == "__main__":
    sys.exit(main())

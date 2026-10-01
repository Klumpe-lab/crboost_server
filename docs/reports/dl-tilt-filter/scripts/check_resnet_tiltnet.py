#!/usr/bin/env python3
"""Does the model author's ResNet18 tilt classifier respond to its input, and how does it rank the
tilts a human marked bad in a crboost project?

For checkpoints written by trainTiltCNN_ResNet18_Optuna1034.py (keys model_state_dict, arch, img_size,
preprocessing, threshold, temperature, tta, ...). Standalone: torch, torchvision, Pillow. Runs on the
CPU by default, so two machines print the same numbers:

    python check_resnet_tiltnet.py run_best.pth [tilt.png ...] [--project DIR] [--png-dir DIR]
        [--device cuda] [--no-tta] [--csv out.csv]

Inference exactly as the author's predictTiltCNN.py: grayscale PNG -> Resize to the checkpoint's
img_size -> ToTensor -> per-image standardisation clipped at the checkpoint's clip_sigma -> ResNet18 ->
logits averaged over the 8 D4 rotations/flips when the checkpoint was evaluated with TTA -> divided by
the stored temperature -> softmax. Class order 0 = bad, 1 = good (the filterTilts vocab too). The
stored threshold is on P(good): a tilt is predicted bad when P(good) < threshold, i.e.
P(bad) > 1 - threshold.

--project reads the committed tilt-filter verdict (Frame.is_filtered_out) from
<project>/registry/tilt_series/*.json and the gallery PNGs from <project>/TiltFilter/png (or --png-dir),
then reports how the model ranks the tilts marked bad.
"""

import argparse
import csv
import hashlib
import importlib.util
import json
import statistics
import sys
from pathlib import Path

import torch
import torch.nn as nn
from PIL import Image
from torchvision import models, transforms

BAD, GOOD = 0, 1
SIZE = 384  # the filterTilts gallery PNG size, used for the synthetic inputs
LIVENESS_MIN_STD = 0.05  # services/jobs/tilt_filter.py

# predictTiltCNN.py, unchanged
D4_OPS = [
    lambda x: x,
    lambda x: torch.rot90(x, 1, (-2, -1)),
    lambda x: torch.rot90(x, 2, (-2, -1)),
    lambda x: torch.rot90(x, 3, (-2, -1)),
    lambda x: torch.flip(x, (-1,)),
    lambda x: torch.rot90(torch.flip(x, (-1,)), 1, (-2, -1)),
    lambda x: torch.rot90(torch.flip(x, (-1,)), 2, (-2, -1)),
    lambda x: torch.rot90(torch.flip(x, (-1,)), 3, (-2, -1)),
]


class PerImageStandardize:
    """predictTiltCNN.py, unchanged: zero mean / unit variance per image, clipped at +-clip_sigma."""

    def __init__(self, clip_sigma=6.0):
        self.clip_sigma = float(clip_sigma)

    def __call__(self, x):
        std = x.std()
        if std < 1e-6:
            std = torch.tensor(1.0)
        x = (x - x.mean()) / std
        if self.clip_sigma > 0:
            x = x.clamp(-self.clip_sigma, self.clip_sigma)
        return x


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def load_checkpoint(path: Path) -> dict:
    # The file was written under numpy 2, which pickles numpy scalars as numpy._core.*; numpy 1
    # names that package numpy.core.
    if importlib.util.find_spec("numpy._core") is None:
        import numpy.core
        import numpy.core.multiarray

        sys.modules.setdefault("numpy._core", numpy.core)
        sys.modules.setdefault("numpy._core.multiarray", numpy.core.multiarray)
    # weights_only=False: the metadata holds a numpy scalar, which torch's safe loader refuses.
    # Only for files you trust.
    return torch.load(path, map_location="cpu", weights_only=False)


def build_model(ckpt: dict) -> nn.Module:
    """predictTiltCNN.py's buildModelFromConfig for resnet18."""
    cfg = ckpt.get("config", {})
    arch = ckpt.get("arch", cfg.get("arch"))
    if arch != "resnet18":
        sys.exit(f"expected arch 'resnet18', the checkpoint says {arch!r}")
    net = models.resnet18(weights=None)
    old = net.conv1
    net.conv1 = nn.Conv2d(1, old.out_channels, old.kernel_size, old.stride, old.padding, bias=False)
    net.fc = nn.Sequential(nn.Dropout(cfg.get("dropout", 0.3)), nn.Linear(net.fc.in_features, 2))
    net.load_state_dict(ckpt["model_state_dict"])
    return net


def synthetic_images() -> dict[str, Image.Image]:
    """8-bit grayscale SIZE x SIZE images, like the gallery PNGs."""
    g = torch.Generator().manual_seed(0)
    yy, xx = torch.meshgrid(torch.arange(SIZE), torch.arange(SIZE), indexing="ij")
    arrays = {
        "blank (constant)": torch.full((SIZE, SIZE), 128.0),
        "uniform noise": torch.rand(SIZE, SIZE, generator=g) * 255,
        "gaussian noise": (128 + 38 * torch.randn(SIZE, SIZE, generator=g)).clamp(0, 255),
        "checkerboard 32 px": (((yy // 32) + (xx // 32)) % 2).float() * 255,
        "horizontal ramp": xx.float() / (SIZE - 1) * 255,
    }
    return {
        name: Image.frombytes("L", (SIZE, SIZE), bytes(a.round().to(torch.uint8).flatten().tolist()))
        for name, a in arrays.items()
    }


def open_png(path: Path) -> Image.Image:
    with Image.open(path) as img:
        return img.convert("L")


def logits_of(model: nn.Module, x: torch.Tensor, tta: bool) -> torch.Tensor:
    if tta:
        return torch.stack([model(op(x)).float() for op in D4_OPS]).mean(0)
    return model(x).float()


def p_bad_of(model, images, transform, device, tta, temperature, batch=64) -> torch.Tensor:
    """P(bad) per image (temperature applied), images as PIL images or PNG paths."""
    out = []
    for i in range(0, len(images), batch):
        chunk = [open_png(im) if isinstance(im, Path) else im for im in images[i : i + batch]]
        x = torch.stack([transform(im) for im in chunk]).to(device)
        out.append(logits_of(model, x, tta).cpu())
    return torch.softmax(torch.cat(out) / temperature, dim=1)[:, BAD]


def stages(model: nn.Module, x: torch.Tensor) -> dict[str, torch.Tensor]:
    """The torchvision ResNet forward pass with every stage's output kept (eval mode, no TTA)."""
    out = {}
    x = model.relu(model.bn1(model.conv1(x)))
    out["stem conv+bn+relu"] = x
    x = model.maxpool(x)
    for name in ("layer1", "layer2", "layer3", "layer4"):
        x = getattr(model, name)(x)
        out[name] = x
    x = torch.flatten(model.avgpool(x), 1)
    out["avgpool"] = x
    out["fc (logits)"] = model.fc(x)
    return out


def auroc(labels: list[bool], scores: list[float]) -> float:
    """Rank AUROC of `scores` for the True labels (ties averaged)."""
    order = sorted(range(len(scores)), key=scores.__getitem__)
    ranks = [0.0] * len(scores)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and scores[order[j + 1]] == scores[order[i]]:
            j += 1
        for k in range(i, j + 1):
            ranks[order[k]] = (i + j) / 2 + 1
        i = j + 1
    n_pos = sum(labels)
    n_neg = len(labels) - n_pos
    if not n_pos or not n_neg:
        return float("nan")
    return (sum(r for r, lab in zip(ranks, labels, strict=True) if lab) - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)


def project_tilts(project: Path, png_dir: Path) -> tuple[list[dict], int]:
    """Every registered tilt with a gallery PNG: series, key, nominal angle, committed verdict."""
    stems = {p.stem: p for p in png_dir.glob("*.png")}
    rows, missing = [], 0
    for sidecar in sorted((project / "registry" / "tilt_series").glob("*.json")):
        ts = json.loads(sidecar.read_text())
        for f in ts.get("frames", []):
            fid = f["id"]
            base = fid[: -len("_EER")] if fid.endswith("_EER") else fid
            png = stems.get(fid) or stems.get(base) or stems.get(f"{base}_EER")
            if png is None:
                missing += 1
                continue
            rows.append(
                {
                    "series": ts.get("id", sidecar.stem),
                    "key": fid,
                    "png": png,
                    "angle": f.get("nominal_tilt_angle_deg"),
                    "bad": bool(f.get("is_filtered_out")),
                }
            )
    return rows, missing


def report_project(rows, missing, p_bad, cut, png_dir, csv_path) -> None:
    labels = [r["bad"] for r in rows]
    scores = p_bad.tolist()
    n_bad = sum(labels)
    print(
        f"{len(rows)} tilts with a PNG in {png_dir} ({missing} registered tilts without one), "
        f"{len({r['series'] for r in rows})} series, {n_bad} committed bad (is_filtered_out)"
    )
    print(f"mean P(bad) {statistics.fmean(scores):.4f}, min {min(scores):.4f}, max {max(scores):.4f}")

    by_series: dict[str, list[float]] = {}
    for r, s in zip(rows, scores, strict=True):
        by_series.setdefault(r["series"], []).append(s)
    spreads = [statistics.pstdev(v) for v in by_series.values() if len(v) >= 2]
    live = sum(s >= LIVENESS_MIN_STD for s in spreads)
    print(
        f"crboost liveness: {live} of {len(spreads)} series have a P(bad) spread >= {LIVENESS_MIN_STD} "
        f"-> {'live' if live else 'DEAD (the panel would show the banner)'}"
    )

    if n_bad and n_bad < len(rows):
        print(f"AUROC, P(bad) against the committed verdict: {auroc(labels, scores):.4f}")
        ranked = sorted(range(len(rows)), key=lambda i: -scores[i])
        for k in (n_bad, 2 * n_bad):
            hit = sum(labels[i] for i in ranked[:k])
            print(f"committed-bad tilts among the {k} highest P(bad): {hit} of {n_bad}")
        flagged = [s > cut for s in scores]
        tp = sum(f and lab for f, lab in zip(flagged, labels, strict=True))
        print(
            f"at the checkpoint's cut P(bad) > {cut:.4f}: {sum(flagged)} flagged, "
            f"recall of committed bad {tp / n_bad:.3f}, precision {tp / max(1, sum(flagged)):.3f}"
        )

    print(f"\n{'|angle|':>9s} {'tilts':>6s} {'committed bad':>14s} {'mean P(bad)':>12s} {'flagged':>8s}")
    for lo, hi in ((0, 20), (20, 40), (40, 50), (50, 60), (60, 91)):
        idx = [i for i, r in enumerate(rows) if r["angle"] is not None and lo <= abs(r["angle"]) < hi]
        if idx:
            print(
                f"{lo:3d}-{hi - 1:<3d}  {len(idx):6d} {sum(labels[i] for i in idx):14d} "
                f"{statistics.fmean(scores[i] for i in idx):12.4f} {sum(scores[i] > cut for i in idx):8d}"
            )
    zero = []
    for series in by_series:
        idx = [i for i, r in enumerate(rows) if r["series"] == series and r["angle"] is not None]
        if idx:
            zero.append(scores[min(idx, key=lambda i: abs(rows[i]["angle"]))] <= cut)
    if zero:
        print(f"lowest-|angle| tilt predicted good in {sum(zero)} of {len(zero)} series")

    if csv_path:
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["series", "key", "angle", "committed_bad", "p_bad"])
            for r, s in zip(rows, scores, strict=True):
                w.writerow([r["series"], r["key"], r["angle"], int(r["bad"]), f"{s:.6f}"])
        print(f"per-tilt P(bad) -> {csv_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("checkpoint")
    ap.add_argument("pngs", nargs="*", help="grayscale tilt PNGs (any size; resized like at training)")
    ap.add_argument("--project", type=Path, help="crboost project with a committed tilt-filter verdict")
    ap.add_argument("--png-dir", type=Path, help="gallery PNGs (default <project>/TiltFilter/png)")
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--no-tta", action="store_true", help="skip the 8-fold D4 averaging")
    ap.add_argument("--csv", help="write per-tilt P(bad) of --project here")
    args = ap.parse_args()
    torch.set_grad_enabled(False)
    device = torch.device(args.device)
    path = Path(args.checkpoint)

    print("== environment")
    print(f"torch {torch.__version__}, python {sys.version.split()[0]}, device {device}")

    print("\n== checkpoint")
    print(f"{path}  {path.stat().st_size:,} bytes  sha256 {sha256(path)}")
    ckpt = load_checkpoint(path)
    cfg = ckpt.get("config", {})
    print("keys:", ", ".join(sorted(ckpt)))
    size = int(ckpt.get("img_size", cfg.get("img_size", 224)))
    clip = float(ckpt.get("preprocessing", {}).get("clip_sigma", cfg.get("clip_sigma", 6.0)))
    threshold = float(ckpt.get("threshold", 0.5))
    temperature = float(ckpt.get("temperature", 1.0))
    tta = bool(ckpt.get("tta", False)) and not args.no_tta
    cut = 1.0 - threshold
    print(f"arch {ckpt.get('arch')}, img_size {size}, preprocessing {ckpt.get('preprocessing')}")
    print(
        f"class_names {ckpt.get('class_names')}, best_epoch {ckpt.get('best_epoch')}, "
        f"split_mode {cfg.get('split_mode')}, select_on {cfg.get('select_on')}"
    )
    print(
        f"threshold {threshold:.4f} on P(good) -> predicted bad when P(bad) > {cut:.4f}; "
        f"temperature {temperature:.4f}; TTA {tta}"
    )
    print("val_datasets:", ", ".join(ckpt.get("val_datasets", [])))
    vm = ckpt.get("val_metrics", {})
    if vm:
        print("val_metrics:", ", ".join(f"{k} {float(v):.4g}" for k, v in vm.items()))
    for row in ckpt.get("per_dataset_metrics", []):
        bad = int(row.get("tn", 0)) + int(row.get("fp", 0))
        print(
            f"  {row['dataset']:32s} n {int(row['n']):5d}  bad {bad:4d}  balacc {float(row['balacc']):.3f}  "
            f"recall_bad {float(row['recall_bad']):.3f}  auroc {float(row['auroc']):.3f}"
        )

    model = build_model(ckpt).to(device).eval()
    state = ckpt["model_state_dict"]
    print("\n== weights")
    print(f"parameters: {sum(p.numel() for p in model.parameters()):,}")
    non_finite = [k for k, v in state.items() if v.is_floating_point() and not torch.isfinite(v).all()]
    print("tensors holding NaN/Inf:", ", ".join(non_finite) or "none")
    for name in (
        "conv1.weight",
        "bn1.weight",
        "bn1.running_var",
        "layer4.1.conv2.weight",
        "layer4.1.bn2.running_var",
        "fc.1.weight",
        "fc.1.bias",
    ):
        t = state[name]
        print(
            f"{name:26s} {tuple(t.shape)!s:18s} mean|x| {t.abs().mean().item():11.4g}  "
            f"max|x| {t.abs().max().item():11.4g}"
        )

    transform = transforms.Compose([transforms.Resize((size, size)), transforms.ToTensor(), PerImageStandardize(clip)])
    images = synthetic_images()
    for p in args.pngs:
        images[Path(p).name] = open_png(Path(p))
    names = list(images)

    print("\n== forward pass, eval mode" + (" with D4 TTA" if tta else ""))
    p_bad = p_bad_of(model, list(images.values()), transform, device, tta, temperature)
    width = max(len(n) for n in names)
    print(f"{'input':{width}s} {'P(bad)':>8s}")
    for n, p in zip(names, p_bad.tolist(), strict=True):
        print(f"{n:{width}s} {p:8.4f}")
    spread = (p_bad.max() - p_bad.min()).item()
    print(f"P(bad) spread over the {len(names)} inputs (max - min): {spread:.3g}")

    print("\n== where the inputs stop differing (eval mode, no TTA)")
    print("zeros = share of exact zeros in the stage's output; spread = std across the inputs, averaged over features")
    x = torch.stack([transform(images[n]) for n in names]).to(device)
    print(f"{'stage':20s} {'zeros':>7s} {'spread':>11s}")
    for name, t in stages(model, x).items():
        t = t.flatten(1).cpu()
        print(f"{name:20s} {(t == 0).float().mean().item():7.1%} {t.std(dim=0).mean().item():11.4g}")

    if args.project:
        png_dir = args.png_dir or args.project / "TiltFilter" / "png"
        print(f"\n== {args.project.name}: P(bad) against the committed verdict")
        rows, missing = project_tilts(args.project, png_dir)
        if not rows:
            print(f"no registered tilt has a PNG in {png_dir}")
        else:
            scores = p_bad_of(model, [r["png"] for r in rows], transform, device, tta, temperature)
            report_project(rows, missing, scores, cut, png_dir, args.csv)

    print("\n== verdict")
    if spread < 1e-3:
        print(f"The network gives every input the same answer: P(bad) {p_bad.mean().item():.4f}.")
    else:
        print("The output depends on the input.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Does a SmallSimpleCNN tilt-classifier checkpoint respond to its input?

Standalone: needs torch, plus Pillow when PNGs are given. Runs on the CPU by default, so two
machines print the same numbers for the same file:

    python check_tiltnet.py model260212.pth [tilt.png ...] [--device cuda]

PNGs must be 384x384 8-bit grayscale, the filterTilts thumbnail format (Fourier-cropped to
384 px, min-max scaled to 0-255). Every input goes through the filterTilts inference transform:
ToTensor, then Normalize(mean 0.5, std 0.5). Class order is the filterTilts vocab: 0 = bad,
1 = good.

Prints the checkpoint's own training record, P(bad)/P(good) for synthetic images and the
PNGs, and the layer at which different inputs stop producing different activations.
"""

import argparse
import hashlib
import math
import sys
from pathlib import Path

import torch
import torch.nn as nn

BAD, GOOD = 0, 1
SIZE = 384


class SmallSimpleCNN(nn.Module):
    """filterTilts/deepLearning/model_architectures.py, unchanged."""

    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 32, kernel_size=3, stride=1, padding=1)
        self.bn1 = nn.BatchNorm2d(32)
        self.conv2 = nn.Conv2d(32, 64, kernel_size=3, stride=1, padding=1)
        self.bn2 = nn.BatchNorm2d(64)
        self.conv3 = nn.Conv2d(64, 128, kernel_size=3, stride=1, padding=1)
        self.bn3 = nn.BatchNorm2d(128)
        self.conv4 = nn.Conv2d(128, 256, kernel_size=3, stride=1, padding=1)
        self.bn4 = nn.BatchNorm2d(256)
        self.conv5 = nn.Conv2d(256, 512, kernel_size=3, stride=1, padding=1)
        self.bn5 = nn.BatchNorm2d(512)
        self.conv6 = nn.Conv2d(512, 1024, kernel_size=3, stride=1, padding=1)
        self.bn6 = nn.BatchNorm2d(1024)

        self.fc1 = nn.Linear(1024 * 6 * 6, 1024)
        self.dropout1 = nn.Dropout(0.5)
        self.fc2 = nn.Linear(1024, 512)
        self.dropout2 = nn.Dropout(0.5)
        self.fc3 = nn.Linear(512, 256)
        self.dropout3 = nn.Dropout(0.5)
        self.fc4 = nn.Linear(256, 2)

        self.activationF = nn.ReLU()
        self.maxpool = nn.MaxPool2d(kernel_size=2, stride=2)

    def forward(self, x):
        x = self.activationF(self.bn1(self.conv1(x)))
        x = self.maxpool(x)
        x = self.activationF(self.bn2(self.conv2(x)))
        x = self.maxpool(x)
        x = self.activationF(self.bn3(self.conv3(x)))
        x = self.maxpool(x)
        x = self.activationF(self.bn4(self.conv4(x)))
        x = self.maxpool(x)
        x = self.activationF(self.bn5(self.conv5(x)))
        x = self.maxpool(x)
        x = self.activationF(self.bn6(self.conv6(x)))
        x = self.maxpool(x)
        x = x.view(x.size(0), -1)
        x = self.dropout1(self.activationF(self.fc1(x)))
        x = self.dropout2(self.activationF(self.fc2(x)))
        x = self.dropout3(self.activationF(self.fc3(x)))
        x = self.fc4(x)
        return x


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def synthetic_images() -> dict[str, torch.Tensor]:
    """1 x 384 x 384 images with values in [0, 1], i.e. before normalisation."""
    g = torch.Generator().manual_seed(0)
    yy, xx = torch.meshgrid(torch.arange(SIZE), torch.arange(SIZE), indexing="ij")
    return {
        "black": torch.zeros(1, SIZE, SIZE),
        "grey 0.5": torch.full((1, SIZE, SIZE), 0.5),
        "white": torch.ones(1, SIZE, SIZE),
        "uniform noise": torch.rand(1, SIZE, SIZE, generator=g),
        "gaussian noise": (0.5 + 0.15 * torch.randn(1, SIZE, SIZE, generator=g)).clamp(0, 1),
        "checkerboard 32 px": (((yy // 32) + (xx // 32)) % 2).float().unsqueeze(0),
        "horizontal ramp": (xx.float() / (SIZE - 1)).unsqueeze(0),
    }


def png_image(path: str) -> torch.Tensor:
    from PIL import Image

    with Image.open(path) as img:
        if img.mode != "L" or img.size != (SIZE, SIZE):
            sys.exit(f"{path}: need an 8-bit grayscale {SIZE}x{SIZE} PNG, got mode {img.mode}, size {img.size}")
        data = torch.frombuffer(bytearray(img.tobytes()), dtype=torch.uint8)
    return (data.float() / 255).reshape(1, SIZE, SIZE)


def stages(model: SmallSimpleCNN, x: torch.Tensor) -> dict[str, torch.Tensor]:
    """The forward pass with every stage's output kept (eval mode, so dropout is the identity)."""
    out = {}
    relu, pool = model.activationF, model.maxpool
    for i in range(1, 7):
        x = relu(getattr(model, f"bn{i}")(getattr(model, f"conv{i}")(x)))
        out[f"conv{i}+bn{i}+relu"] = x
        x = pool(x)
    x = x.flatten(1)
    for i in range(1, 4):
        x = relu(getattr(model, f"fc{i}")(x))
        out[f"fc{i}+relu"] = x
    out["fc4 (logits)"] = model.fc4(x)
    return out


def cell(values: list[float], i: int) -> str:
    return f"{values[i]:10.4f}" if i < len(values) else " " * 10


def print_probabilities(names: list[str], logits: torch.Tensor) -> torch.Tensor:
    probs = torch.softmax(logits, dim=1)
    width = max(len(n) for n in names)
    print(f"{'input':{width}s} {'logit bad':>10s} {'logit good':>10s} {'P(bad)':>8s} {'P(good)':>8s}")
    for name, lg, pr in zip(names, logits.tolist(), probs.tolist(), strict=True):
        print(f"{name:{width}s} {lg[BAD]:10.4f} {lg[GOOD]:10.4f} {pr[BAD]:8.4f} {pr[GOOD]:8.4f}")
    p_bad = probs[:, BAD]
    print(f"P(bad) spread over the {len(names)} inputs (max - min): {(p_bad.max() - p_bad.min()).item():.3g}")
    return p_bad


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("checkpoint")
    ap.add_argument("pngs", nargs="*", help="384x384 8-bit grayscale tilt PNGs")
    ap.add_argument("--device", default="cpu")
    args = ap.parse_args()
    torch.set_grad_enabled(False)
    device = torch.device(args.device)
    path = Path(args.checkpoint)

    print("== environment")
    print(f"torch {torch.__version__}, python {sys.version.split()[0]}, device {device}")

    print("\n== checkpoint")
    print(f"{path}  {path.stat().st_size:,} bytes  sha256 {sha256(path)}")
    ckpt = torch.load(path, map_location="cpu", weights_only=True)
    print("keys:", ", ".join(sorted(ckpt)))
    print("model_architecture:", ckpt.get("model_architecture"))
    state = ckpt["model_state_dict"]
    tracked = sorted({int(v) for k, v in state.items() if k.endswith("num_batches_tracked")})
    print("BatchNorm num_batches_tracked (optimizer steps taken):", tracked)
    for group in ckpt.get("optimizer_state_dict", {}).get("param_groups", []):
        print("optimizer param_group:", {k: v for k, v in group.items() if k != "params"})

    acc = [float(v) for v in ckpt.get("accuracies", [])]
    train = [float(v) for v in ckpt.get("train_losses", [])]
    val = [float(v) for v in ckpt.get("val_losses", [])]
    epochs = max(len(acc), len(val))
    print(f"\n== training record stored in the checkpoint (ln 2 = {math.log(2):.4f})")
    print(f"{len(acc)} accuracies, {len(train)} train losses, {len(val)} val losses; epochs numbered from 1")
    if epochs and len(train) > epochs and len(train) % epochs == 0:
        per = len(train) // epochs
        train = [sum(train[i * per : (i + 1) * per]) / per for i in range(epochs)]
        print(f"(train_losses holds {per} values per epoch; shown as each epoch's mean)")
    print(f"{'epoch':>6s} {'train_loss':>10s} {'val_loss':>10s} {'accuracy':>10s}")
    for i in range(max(epochs, len(train))):
        print(f"{i + 1:6d} {cell(train, i)} {cell(val, i)} {cell(acc, i)}")
    if acc:
        best = max(range(len(acc)), key=acc.__getitem__)
        stuck = len(acc) - next(i for i in range(len(acc)) if all(a == acc[-1] for a in acc[i:]))
        print(f"best accuracy {acc[best]:.4f} at epoch {best + 1}; the last {stuck} epochs all read {acc[-1]:.4f}")
    if tracked and epochs:
        print(
            f"{tracked[-1]} optimizer steps / {epochs} epochs = {tracked[-1] / epochs:g} per epoch: "
            "the saved weights are the ones after the last epoch"
        )

    model = SmallSimpleCNN()
    model.load_state_dict(state)
    model.to(device).eval()

    print("\n== weights")
    print(f"parameters: {sum(p.numel() for p in model.parameters()):,}")
    non_finite = [k for k, v in state.items() if v.is_floating_point() and not torch.isfinite(v).all()]
    print("tensors holding NaN/Inf:", ", ".join(non_finite) or "none")
    for name, t in state.items():
        if t.is_floating_point():
            mean_abs, max_abs = t.abs().mean().item(), t.abs().max().item()
            print(f"{name:20s} {tuple(t.shape)!s:22s} mean|x| {mean_abs:11.4g}  max|x| {max_abs:11.4g}")
    bias = model.fc4.bias.detach().cpu()
    p_bias = torch.softmax(bias, dim=0)
    print(
        f"fc4.bias = [{bias[BAD].item():.4f}, {bias[GOOD].item():.4f}] -> softmax: "
        f"P(bad) {p_bias[BAD].item():.4f}, P(good) {p_bias[GOOD].item():.4f}"
    )

    images = synthetic_images()
    for p in args.pngs:
        images[Path(p).name] = png_image(p)
    names = list(images)
    x = torch.stack([images[n] for n in names]).sub(0.5).div(0.5).to(device)

    print("\n== forward pass, eval mode (what inference uses)")
    logits = model(x).cpu()
    p_bad = print_probabilities(names, logits)
    print("logits equal fc4.bias for every input:", torch.allclose(logits, bias.expand_as(logits), atol=1e-5))

    print("\n== where the inputs stop differing (eval mode)")
    print("zeros = share of exact zeros in the stage's output; spread = std across the inputs, averaged over features")
    print(f"{'stage':22s} {'zeros':>7s} {'spread':>11s}")
    dead_at = None
    for name, t in stages(model, x).items():
        t = t.flatten(1).cpu()
        spread = t.std(dim=0).mean().item()
        print(f"{name:22s} {(t == 0).float().mean().item():7.1%} {spread:11.4g}")
        if dead_at is None and spread == 0.0:
            dead_at = name

    print("\n== same inputs, BatchNorm on the batch's own statistics (train mode, dropout off)")
    print(
        "If these vary while eval mode does not, the stored BatchNorm running statistics are the problem, "
        "not the learned weights."
    )
    model.train()
    for m in model.modules():
        if isinstance(m, nn.Dropout):
            m.eval()
    p_bad_batch_stats = print_probabilities(names, model(x).cpu())
    model.eval()

    print("\n== verdict")
    if (p_bad.max() - p_bad.min()).item() < 1e-3:
        print(
            f"The network gives every input the same answer: P(bad) {p_bad.mean().item():.4f}, "
            f"P(good) {1 - p_bad.mean().item():.4f}."
        )
        if dead_at:
            print(f"From '{dead_at}' on, every input produces identical activations.")
        if (p_bad_batch_stats.max() - p_bad_batch_stats.min()).item() < 1e-3:
            print(
                "BatchNorm on batch statistics changes nothing: the learned weights are the cause, "
                "not the stored statistics."
            )
    else:
        print("The output depends on the input.")


if __name__ == "__main__":
    main()

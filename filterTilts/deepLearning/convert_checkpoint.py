#!/usr/bin/env python3
"""Convert a checkpoint from the model author's trainTiltCNN_ResNet18 scripts into the format the
tilt filter loads.

The output is what ModelLoader reads with weights_only=True: model_architecture "ResNet18Gray",
model_state_dict, and the inference settings training recorded (clip_sigma, temperature, tta),
plus the source file's name, sha256 and decision threshold. The source is loaded with
weights_only=False because its config holds numpy scalars, so convert only files that come from
the model author. Prints the conf.yaml entry that registers the result. CPU only.

    python -m filterTilts.deepLearning.convert_checkpoint run_best.pth <Models dir>/tiltnet_resnet18_o1034.pth
"""

import argparse
import hashlib
from pathlib import Path

import torch

from filterTilts.deepLearning.model_architectures import ResNet18Gray


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def convert(source: Path, output: Path) -> dict:
    """Write `output` from `source` and return what was written. Refuses any architecture, input
    size or missing setting that ResNet18Gray and ModelLoader would not reproduce exactly."""
    ckpt = torch.load(source, map_location="cpu", weights_only=False)
    config = ckpt.get("config") or {}
    if not isinstance(config, dict):
        config = vars(config)  # an argparse Namespace
    arch = ckpt.get("arch", config.get("arch"))
    if arch != "resnet18":
        raise SystemExit(f"{source}: architecture {arch!r}; only the author's resnet18 converts to ResNet18Gray")
    img_size = int(ckpt.get("img_size", config.get("img_size", 0)))
    if img_size != ResNet18Gray.input_size:
        raise SystemExit(f"{source}: trained at {img_size} px, but ResNet18Gray takes {ResNet18Gray.input_size}")
    clip_sigma = (ckpt.get("preprocessing") or {}).get("clip_sigma", config.get("clip_sigma"))
    if clip_sigma is None:
        raise SystemExit(f"{source}: no clip_sigma recorded; the per-image standardisation needs it")

    model = ResNet18Gray()
    model.load_state_dict(ckpt["model_state_dict"])  # strict: every key and shape must match
    converted = {
        "model_architecture": "ResNet18Gray",
        "model_state_dict": model.state_dict(),
        "clip_sigma": float(clip_sigma),
        "temperature": float(ckpt.get("temperature", 1.0)),
        "tta": bool(ckpt.get("tta", False)),
        "source": {
            "file": source.name,
            "sha256": _sha256(source),
            "threshold_p_good": float(ckpt.get("threshold", 0.5)),
        },
    }
    torch.save(converted, output)
    # The load ModelLoader does, so a file it would refuse fails here rather than in a SLURM job.
    torch.load(output, map_location="cpu", weights_only=True)
    return converted


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("source", type=Path, help="checkpoint written by trainTiltCNN_ResNet18 (e.g. run_best.pth)")
    ap.add_argument("output", type=Path, help="converted checkpoint to write; must not exist")
    args = ap.parse_args()
    if args.output.exists():
        raise SystemExit(f"{args.output} exists; not overwriting it")

    out = convert(args.source, args.output)
    thr_good = out["source"]["threshold_p_good"]
    print(
        f"Wrote {args.output}: ResNet18Gray, clip_sigma {out['clip_sigma']:.4f}, "
        f"temperature {out['temperature']:.4f}, tta {out['tta']}, from {args.source.name} "
        f"(sha256 {out['source']['sha256']})"
    )
    print("\nRegister it in conf.yaml under tilt_filter.models:\n")
    print(f"    {args.output.stem}:")
    print(f'      path: "{args.output.resolve()}"')
    print('      arch: "ResNet18Gray"')
    print('      normalisation: "per_image"')
    print(
        f"      threshold: {1.0 - thr_good:.3f}   # the model author's cut, P(good) {thr_good:.3f} on their own "
        "validation set; not calibrated on our data"
    )


if __name__ == "__main__":
    main()

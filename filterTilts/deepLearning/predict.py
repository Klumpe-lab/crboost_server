"""The tilt classifier's command line: the part of the tilt-filter job that needs torch. It runs in the tilt_filter
container (drivers/tilt_filter.py calls it through run_tool), so the server's environment has no torch.

    python -m filterTilts.deepLearning.predict --weights W.pth --arch ResNet18Gray --normalisation per_image \\
        --pngs list.txt --out predictions.json [--batch-size 32] [--workers N] [--gpu 0]

`list.txt` holds one PNG path per line. `predictions.json` holds P(bad) for each PNG, in that order, and the facts the
driver prints about the model. GPU only, as ModelLoader: no CUDA means a broken node, which a CPU fallback would hide.
"""

import argparse
import json
import os
import sys
from pathlib import Path

import torch

from .model_loader import ModelLoader


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="P(bad) per gallery PNG from one tilt-classifier checkpoint")
    ap.add_argument("--weights", required=True, type=Path)
    ap.add_argument("--arch", required=True, help="network class name in model_architectures.py")
    ap.add_argument("--normalisation", required=True)
    ap.add_argument("--pngs", required=True, type=Path, help="text file with one PNG path per line")
    ap.add_argument("--out", required=True, type=Path, help="JSON file to write")
    ap.add_argument("--batch-size", type=int, default=32)
    ap.add_argument("--workers", type=int, default=len(os.sched_getaffinity(0)), help="DataLoader workers")
    ap.add_argument("--gpu", type=int, default=0)
    args = ap.parse_args(argv)

    paths = [line.strip() for line in args.pngs.read_text().splitlines() if line.strip()]
    if not paths:
        raise SystemExit(f"{args.pngs} lists no PNGs")

    loader = ModelLoader(args.weights, args.arch, args.normalisation, gpu=args.gpu, num_workers=args.workers)
    loader.load_model()
    p_bad = loader.predict_p_bad(paths, args.batch_size)
    if len(p_bad) != len(paths):
        raise RuntimeError(f"{len(p_bad)} predictions for {len(paths)} PNGs")

    result = {
        "p_bad": p_bad,
        "model": {
            "arch": args.arch,
            "input_size": loader.input_size,
            "normalisation": args.normalisation,
            "temperature": loader.temperature,
            "tta": loader.tta,
            "device": loader.device_name,
            "torch": torch.__version__,
        },
    }
    # Written whole or not at all: the driver reads this file back as the run's result.
    part = args.out.with_name(args.out.name + ".part")
    part.write_text(json.dumps(result))
    os.replace(part, args.out)
    print(f"predict: {len(p_bad)} tilts on {loader.device_name}, torch {torch.__version__}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

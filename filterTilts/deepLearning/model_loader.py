from pathlib import Path

import torch
from PIL import Image
from torch.utils.data import DataLoader, Dataset
from torchvision import transforms

from .model_architectures import get_model_class

# Every model reads the gallery PNGs (TiltFilter/png): ImageProcessor's Fourier crop to this size,
# min-max scaled to 8 bits. A network trained at another size gets them resized, as at training.
GALLERY_PNG_SIZE = 384

# The input normalisations weights are trained with, by the registry's `normalisation` key:
# "per_image" = PerImageStandardize at the checkpoint's clip_sigma.
NORMALISATIONS = ("per_image",)

# Output column of the 'bad' class (training vocab: bad = 0, good = 1).
BAD = 0

# The eight rotations and flips of a square image, for test-time averaging.
_D4 = (
    lambda x: x,
    lambda x: torch.rot90(x, 1, (-2, -1)),
    lambda x: torch.rot90(x, 2, (-2, -1)),
    lambda x: torch.rot90(x, 3, (-2, -1)),
    lambda x: torch.flip(x, (-1,)),
    lambda x: torch.rot90(torch.flip(x, (-1,)), 1, (-2, -1)),
    lambda x: torch.rot90(torch.flip(x, (-1,)), 2, (-2, -1)),
    lambda x: torch.rot90(torch.flip(x, (-1,)), 3, (-2, -1)),
)


class PerImageStandardize:
    """Zero mean and unit variance per image, clipped at +-clip_sigma. The same computation as the
    model author's PerImageStandardize, which the "per_image" weights were trained with."""

    def __init__(self, clip_sigma: float):
        self.clip_sigma = float(clip_sigma)

    def __call__(self, x):
        std = x.std()
        if std < 1e-6:
            std = torch.tensor(1.0)
        x = (x - x.mean()) / std
        return x.clamp(-self.clip_sigma, self.clip_sigma) if self.clip_sigma > 0 else x


class PngDataset(Dataset):
    """Model inputs read from PNG files: one 8-bit grayscale gallery image per tilt (the driver
    checks mode and size before inference)."""

    def __init__(self, paths, transform):
        self.paths = list(paths)
        self.transform = transform

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, idx):
        with Image.open(self.paths[idx]) as img:
            return self.transform(img)


class ModelLoader:
    """Loads one registered tilt classifier onto a GPU and returns P(bad) per tilt."""

    def __init__(self, model_path, arch, normalisation, gpu=0, num_workers=4):
        """
        Parameters:
        - model_path: .pth checkpoint holding 'model_architecture' and 'model_state_dict', and the
          inference settings its training recorded: 'clip_sigma' ("per_image" only), 'temperature'
          (default 1) and 'tta' (default False)
        - arch: network class name in model_architectures.py; must match the checkpoint's own
        - normalisation: one of NORMALISATIONS
        - gpu: CUDA device number. There is no CPU mode: the job asks SLURM for a GPU, so CUDA
          missing means a broken node or a CPU-only torch install, which a fallback would hide.
        - num_workers: DataLoader workers reading the PNGs
        """
        if normalisation not in NORMALISATIONS:
            raise ValueError(f"Unknown normalisation '{normalisation}'; known: {', '.join(NORMALISATIONS)}")
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA is not available on this node; the tilt classifier runs on a GPU only.")
        self.model_path = Path(model_path)
        self.arch = arch
        self.normalisation = normalisation
        self.model_class = get_model_class(arch)
        self.device = torch.device(f"cuda:{gpu}")
        self.num_workers = num_workers
        self.model = None
        # Set from the checkpoint by load_model.
        self.transform = None
        self.temperature = 1.0
        self.tta = False

    @property
    def input_size(self) -> int:
        """Side length in pixels of the square image the network takes."""
        return self.model_class.input_size

    @property
    def device_name(self) -> str:
        return torch.cuda.get_device_name(self.device)

    def load_model(self):
        """Build the network, load the checkpoint's weights and inference settings, and move it
        to the GPU."""
        if not self.model_path.is_file():
            raise FileNotFoundError(f"Model weights not found: {self.model_path}")
        checkpoint = torch.load(self.model_path, map_location="cpu", weights_only=True)
        if not isinstance(checkpoint, dict) or "model_state_dict" not in checkpoint:
            raise ValueError(
                f"{self.model_path} is not a checkpoint dict with 'model_architecture' and 'model_state_dict'"
            )
        saved_arch = checkpoint.get("model_architecture")
        if saved_arch != self.arch:
            raise ValueError(
                f"{self.model_path} was saved from '{saved_arch}', but conf.yaml registers it as '{self.arch}'"
            )
        model = self.model_class()
        model.load_state_dict(checkpoint["model_state_dict"])
        self.transform = self._input_transform(checkpoint)
        self.temperature = float(checkpoint.get("temperature", 1.0))
        self.tta = bool(checkpoint.get("tta", False))
        del checkpoint
        self.model = model.to(self.device).eval()
        return self.model

    def _input_transform(self, checkpoint) -> transforms.Compose:
        """Gallery PNG -> network input, as at training: resized when the network was trained at
        another size, then normalised."""
        steps = []
        if self.input_size != GALLERY_PNG_SIZE:
            steps.append(transforms.Resize((self.input_size, self.input_size)))
        steps.append(transforms.ToTensor())
        if "clip_sigma" not in checkpoint:
            raise ValueError(f"{self.model_path} records no 'clip_sigma', which 'per_image' normalisation needs")
        steps.append(PerImageStandardize(checkpoint["clip_sigma"]))
        return transforms.Compose(steps)

    def predict_p_bad(self, png_paths, batch_size=50) -> list[float]:
        """P(bad) for each image, in input order: the softmax weight of the 'bad' class, from the
        logits divided by the checkpoint's temperature. With 'tta' the logits are first averaged
        over the eight rotations and flips of the image."""
        if self.model is None:
            self.load_model()
        loader = DataLoader(
            PngDataset(png_paths, self.transform),
            batch_size=batch_size,
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=True,
        )
        p_bad: list[float] = []
        with torch.no_grad():
            for inputs in loader:
                x = inputs.to(self.device)
                if self.tta:
                    logits = torch.stack([self.model(op(x)).float() for op in _D4]).mean(0)
                else:
                    logits = self.model(x).float()
                p_bad.extend(torch.softmax(logits / self.temperature, dim=1)[:, BAD].cpu().tolist())
        return p_bad

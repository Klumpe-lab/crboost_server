from pathlib import Path

import torch
from PIL import Image
from torch.utils.data import DataLoader, Dataset
from torchvision import transforms

from .model_architectures import get_model_class

# The input transform each weights file was trained with, by the registry's `normalisation` key.
NORMALISATIONS = {"half": transforms.Normalize(mean=[0.5], std=[0.5])}

# Output column of the 'bad' class (training vocab: bad = 0, good = 1).
BAD = 0


class PngDataset(Dataset):
    """Model inputs read from PNG files: one 8-bit grayscale image per tilt, already at the
    network's input size (the driver checks both before inference)."""

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
        - model_path: .pth checkpoint holding 'model_architecture' and 'model_state_dict'
        - arch: network class name in model_architectures.py; must match the checkpoint's own
        - normalisation: key in NORMALISATIONS
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
        self.model_class = get_model_class(arch)
        self.transform = transforms.Compose([transforms.ToTensor(), NORMALISATIONS[normalisation]])
        self.device = torch.device(f"cuda:{gpu}")
        self.num_workers = num_workers
        self.model = None

    @property
    def input_size(self) -> int:
        """Side length in pixels of the square image the network takes."""
        return self.model_class.input_size

    @property
    def device_name(self) -> str:
        return torch.cuda.get_device_name(self.device)

    def load_model(self):
        """Build the network, load the checkpoint's weights and move it to the GPU."""
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
        del checkpoint
        self.model = model.to(self.device).eval()
        return self.model

    def predict_p_bad(self, png_paths, batch_size=50) -> list[float]:
        """P(bad) for each image, in input order: the softmax weight of the 'bad' class."""
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
                logits = self.model(inputs.to(self.device))
                p_bad.extend(torch.softmax(logits, dim=1)[:, BAD].cpu().tolist())
        return p_bad

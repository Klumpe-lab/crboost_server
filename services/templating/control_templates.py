"""Control templates for template matching.

- **Decoy**: the template with its Fourier phases randomised — the same amplitude spectrum (so the
  same contrast, resolution and correlation statistics) but no shape. Matched with the same mask on
  a subset of tomograms, it measures how much the real background scores against "a template of
  this size and spectrum": its picks at or above a cut, over the real template's on the same
  tomograms, estimate the false discovery rate at that cut.
- **Mirror**: the template flipped along Z, the other hand. The correct hand scores higher in
  nearly every tomogram, which settles the absolute hand of a processing chain.

Both are written beside the species' other templates and registered on the species, never selected
automatically; a template-matching job has to choose them.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np


def _read(path: Path) -> tuple[np.ndarray, float]:
    import mrcfile

    with mrcfile.open(str(path), permissive=True) as m:
        return np.asarray(m.data, dtype=np.float32).copy(), float(m.voxel_size.x)


def _write(path: Path, data: np.ndarray, voxel: float) -> Path:
    import mrcfile

    path.parent.mkdir(parents=True, exist_ok=True)
    with mrcfile.new(str(path), overwrite=True) as m:
        m.set_data(np.ascontiguousarray(data, dtype=np.float32))
        m.voxel_size = voxel
    return path


def phase_randomised(template: Path, out: Path, *, seed: int) -> Path:
    """The decoy: |F| of `template` with the phases of seeded white noise, rescaled to the template's
    mean and standard deviation."""
    vol, voxel = _read(template)
    amp = np.abs(np.fft.rfftn(vol))
    rng = np.random.default_rng(seed)
    phase = np.angle(np.fft.rfftn(rng.standard_normal(vol.shape).astype(np.float32)))
    dec = np.fft.irfftn(amp * np.exp(1j * phase), s=vol.shape).astype(np.float32)
    dec = (dec - dec.mean()) / (dec.std() or 1.0) * vol.std() + vol.mean()
    return _write(out, dec, voxel)


def mirrored(template: Path, out: Path) -> Path:
    """The other hand: `template` flipped along Z."""
    vol, voxel = _read(template)
    return _write(out, vol[::-1, :, :], voxel)

"""The segmentation manifest, ``<dir>/segmentations.json``: one entry per (tomogram, feature).

The Segment job writes it when it aggregates its tasks; an import of segmentations made elsewhere
writes it when it registers them. The tilt-series registry's entries are built from the same
entries (`tomogram_outputs`), so the file and the registry say the same thing.
"""

from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from services.tilt_series.models import ModelRef, SegmentationTomogramOutput, SegVolume

MANIFEST_NAME = "segmentations.json"
MANIFEST_SCHEMA = 1

# How a volume was computed: easymode's own TensorFlow networks (.h5 weights), or Ais (.scnm
# models, which easymode runs through `ais segment`).
ENGINE_NATIVE = "easymode-native"
ENGINE_AIS = "ais"


def feature_slug(name: str) -> str:
    """A feature id from a model title: lower-case ASCII letters, digits and underscores, the form
    easymode's own feature names have."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "feature"


class SegmentationEntry(BaseModel):
    """One feature's volume of one tomogram."""

    tomogram: str  # rlnTomoName, the tilt-series id
    feature: str
    path: Path
    shape_zyx: tuple[int, int, int]
    voxel_angstrom: float
    full_scale: float  # the stored value of p = 1
    engine: Literal["easymode-native", "ais"] | None = None
    model: ModelRef | None = None
    source_mrc: Path  # the density that was segmented
    recon_instance: str | None = None  # the reconstruct job that density traces to
    fraction_pct: float | None = None
    overlay_xy: Path | None = None
    overlay_xz: Path | None = None


class SegmentationManifest(BaseModel):
    schema_version: int = MANIFEST_SCHEMA
    producer: str  # the job's instance id, or the synthetic producer of an import
    method: str  # "easymode" | "ais" | "import"
    parameters: dict[str, Any] = Field(default_factory=dict)  # what the run was asked for
    written_at: datetime = Field(default_factory=datetime.now)
    entries: list[SegmentationEntry] = Field(default_factory=list)

    def features(self) -> list[str]:
        return sorted({e.feature for e in self.entries})


def manifest_path(directory: Path) -> Path:
    return Path(directory) / MANIFEST_NAME


def write_manifest(directory: Path, manifest: SegmentationManifest) -> Path:
    """Write `directory`/segmentations.json in one rename: the server reads it as soon as the job
    lands, and must never read half of it."""
    path = manifest_path(directory)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(manifest.model_dump_json(indent=2))
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return path


def read_manifest(directory: Path) -> SegmentationManifest:
    """The manifest in `directory`. Raises FileNotFoundError when there is none."""
    return SegmentationManifest.model_validate_json(manifest_path(directory).read_text())


def tomogram_outputs(manifest: SegmentationManifest, producer_dir: Path) -> dict[str, SegmentationTomogramOutput]:
    """The registry output of every tomogram the manifest lists, keyed by tomogram."""
    out: dict[str, SegmentationTomogramOutput] = {}
    for e in manifest.entries:
        output = out.get(e.tomogram)
        if output is None:
            output = out[e.tomogram] = SegmentationTomogramOutput(
                job_instance_id=manifest.producer,
                job_dir=producer_dir,
                source_mrc=e.source_mrc,
                recon_instance=e.recon_instance,
                shape_zyx=e.shape_zyx,
                voxel_angstrom=e.voxel_angstrom,
            )
        output.volumes[e.feature] = SegVolume(
            path=e.path,
            full_scale=e.full_scale,
            engine=e.engine,
            model=e.model,
            fraction_pct=e.fraction_pct,
            overlay_xy=e.overlay_xy,
            overlay_xz=e.overlay_xz,
        )
    return out

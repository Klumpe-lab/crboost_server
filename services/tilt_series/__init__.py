"""TiltSeries registry: first-class primitives for TS/Frame/Tomogram with stable IDs.

Stage 1 (this module): pure addition alongside the STAR-based pipeline. No job code
change — the registry is available on the backend but not yet the source of truth.

See project_ts_registry_refactor memory for the staged plan.
"""

from services.tilt_series.models import (
    Frame,
    FrameOutput,
    FsMotionCtfFrameOutput,
    TiltSeries,
    TiltSeriesOutput,
    Tomogram,
    TomogramOutput,
    TsAlignmentTiltSeriesOutput,
    TsCtfPerFrameCtf,
    TsCtfTiltSeriesOutput,
    TsReconstructTomogramOutput,
)
from services.tilt_series.registry import (
    TiltSeriesRegistry,
    clear_registry,
    get_registry_for,
    set_registry_for,
)

__all__ = [
    "Frame",
    "FrameOutput",
    "FsMotionCtfFrameOutput",
    "TiltSeries",
    "TiltSeriesOutput",
    "Tomogram",
    "TomogramOutput",
    "TsAlignmentTiltSeriesOutput",
    "TsCtfPerFrameCtf",
    "TsCtfTiltSeriesOutput",
    "TsReconstructTomogramOutput",
    "TiltSeriesRegistry",
    "clear_registry",
    "get_registry_for",
    "set_registry_for",
]

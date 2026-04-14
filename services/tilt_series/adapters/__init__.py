"""Per-job-type ingest adapters that populate the TiltSeriesRegistry from
driver-produced artifacts (Warp XMLs, tomostar files, alignment outputs).

Each adapter is a thin module: it reads job-dir state, resolves identity via
the registry (never via string heuristics), and attaches typed outputs. STAR
emission for the downstream job is a separate concern per adapter, kept in the
same module for cohesion.
"""

from services.tilt_series.adapters.fs_motion_ctf import FsMotionCtfIngestAdapter
from services.tilt_series.adapters.ts_alignment import TsAlignmentIngestAdapter
from services.tilt_series.adapters.ts_ctf import TsCtfIngestAdapter
from services.tilt_series.adapters.ts_reconstruct import TsReconstructIngestAdapter

__all__ = [
    "FsMotionCtfIngestAdapter",
    "TsAlignmentIngestAdapter",
    "TsCtfIngestAdapter",
    "TsReconstructIngestAdapter",
]

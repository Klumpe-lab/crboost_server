"""Rich MRC inspection for the template-import flow.

When a user drops in an .mrc file ("boss said use this"), we want to
extract everything we can from the file before asking the user to fill
out metadata fields by hand. This module is the building block for that
flow: open the volume, read the header + statistics, run a couple of
heuristics (polarity, mask-likeness, provenance hints from header
labels), and return one frozen `MrcInspection` record that the import
UI can pre-fill from.

This is heavier than `template_metadata.read_template_header` (which is
header-only and called on every render). Inspection loads the volume to
compute statistics and inference; call it once at import time.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Literal, Optional

logger = logging.getLogger(__name__)


# Polarity inference returns one of three states. "ambiguous" lets the UI
# show a hint that we couldn't tell, and the user should pick.
PolarityGuess = Literal["white", "black", "ambiguous"]


@dataclass(frozen=True)
class MrcInspection:
    """Everything we can pull out of an .mrc file at import time.

    All fields are populated best-effort. Heuristic / inferred values are
    suffixed `_inferred` and live alongside an explicit confidence so the
    UI can flag low-confidence guesses for the user to confirm.
    """

    # Basic file identity
    path: str
    file_size_bytes: int
    mtime: float

    # Header geometry
    nx: int
    ny: int
    nz: int
    apix_ang: Optional[float]
    is_cube: bool
    box_px: int  # min(nx, ny, nz) -- conservative box

    # Data type
    mode: int
    mode_name: str  # "float32", "int16", etc.

    # Statistics (computed from the full volume, not from header amin/amax/amean
    # which some tools fail to update)
    data_min: float
    data_max: float
    data_mean: float
    data_rms: float

    # Inference: polarity (white = positive density / protein-bright)
    inferred_polarity: PolarityGuess
    polarity_confidence: float  # 0..1

    # Inference: does this look like a (binary-ish) mask rather than a density?
    looks_like_mask: bool
    mask_confidence: float  # 0..1

    # Free-text labels from the MRC header (10 slots × 80 chars). Some tools
    # write provenance here ("RELION 16-Apr-2024", "PDB:6Z6J generated...").
    labels: List[str] = field(default_factory=list)

    # Best-effort regex pulls from labels. None = no match.
    inferred_pdb_id: Optional[str] = None
    inferred_emdb_id: Optional[str] = None
    inferred_tool: Optional[str] = None  # "relion", "pytom", "warptools", "chimera", "imod"


_MRC_MODE_NAMES = {
    0: "int8",
    1: "int16",
    2: "float32",
    3: "complex_int16",
    4: "complex_float32",
    6: "uint16",
    12: "float16",
    101: "uint4",
}


def inspect_mrc_for_import(path: str) -> Optional[MrcInspection]:
    """Open an .mrc, read header + volume, run heuristics, return inspection.

    Returns None if the file doesn't exist or can't be opened. Logs a
    warning in that case — the caller should surface a "couldn't read this
    file" message to the user rather than crash.
    """
    p = Path(path)
    try:
        st = p.stat()
    except OSError:
        return None

    try:
        import mrcfile
        import numpy as np

        with mrcfile.open(str(p), permissive=True) as m:
            # Header geometry
            nx = int(m.header.nx)
            ny = int(m.header.ny)
            nz = int(m.header.nz)
            vx = float(getattr(m.voxel_size, "x", 0.0) or 0.0)
            apix = vx if vx > 0 else None
            mode = int(m.header.mode)
            mode_name = _MRC_MODE_NAMES.get(mode, f"mode{mode}")
            box_px = min(d for d in (nx, ny, nz) if d > 0) if any((nx, ny, nz)) else 0
            is_cube = nx == ny == nz

            # Volume stats — we trust m.data over header amin/amax/amean.
            # m.data is a numpy array (or a memmap depending on mode).
            data = np.asarray(m.data)
            data_min = float(data.min())
            data_max = float(data.max())
            data_mean = float(data.mean())
            data_rms = float(np.sqrt(np.mean(np.square(data - data_mean))))

            # Polarity & mask inference
            polarity, p_conf = _infer_polarity(data)
            is_mask, m_conf = _infer_mask_likeness(data, data_min, data_max)

            labels = _read_labels(m.header)

        pdb_id, emdb_id, tool = _infer_provenance(labels)

        return MrcInspection(
            path=str(p),
            file_size_bytes=int(st.st_size),
            mtime=float(st.st_mtime),
            nx=nx,
            ny=ny,
            nz=nz,
            apix_ang=apix,
            is_cube=is_cube,
            box_px=box_px,
            mode=mode,
            mode_name=mode_name,
            data_min=data_min,
            data_max=data_max,
            data_mean=data_mean,
            data_rms=data_rms,
            inferred_polarity=polarity,
            polarity_confidence=p_conf,
            looks_like_mask=is_mask,
            mask_confidence=m_conf,
            labels=labels,
            inferred_pdb_id=pdb_id,
            inferred_emdb_id=emdb_id,
            inferred_tool=tool,
        )
    except Exception as e:
        logger.warning("Could not inspect MRC %s: %s", path, e)
        return None


def _infer_polarity(volume) -> tuple[PolarityGuess, float]:
    """Compare the central region's mean to the overall mean.

    Templates are typically centered: the protein density is near the
    middle, the periphery is solvent. If center is brighter than overall
    -> "white" (protein has positive density). If darker -> "black".

    Confidence is the magnitude of the difference normalized by the
    volume's RMS — so a flat or noisy volume returns low confidence and
    the UI can flag it.
    """
    import numpy as np

    nz, ny, nx = volume.shape
    # Take a central cube of side ~ min_dim/2
    r = max(1, min(nz, ny, nx) // 4)
    cz, cy, cx = nz // 2, ny // 2, nx // 2
    central = volume[max(0, cz - r): cz + r, max(0, cy - r): cy + r, max(0, cx - r): cx + r]
    if central.size == 0:
        return "ambiguous", 0.0
    central_mean = float(central.mean())
    overall_mean = float(volume.mean())
    overall_rms = float(np.sqrt(np.mean(np.square(volume - overall_mean)))) or 1.0
    diff = central_mean - overall_mean
    confidence = min(1.0, abs(diff) / overall_rms)
    if confidence < 0.1:
        return "ambiguous", confidence
    return ("white" if diff > 0 else "black"), confidence


def _infer_mask_likeness(volume, vmin: float, vmax: float) -> tuple[bool, float]:
    """A binary-ish mask has values mostly in [0, 1] with a bimodal
    distribution. Returns (is_mask, confidence)."""
    # Fast reject: anything outside [-0.01, 1.01] isn't a binary mask.
    if vmin < -0.01 or vmax > 1.01:
        return False, 1.0
    p_low = float((volume < 0.1).mean())
    p_high = float((volume > 0.9).mean())
    bimodal = p_low + p_high
    # 85%+ of voxels at the two extremes -> very likely a mask.
    return bimodal > 0.85, float(min(1.0, bimodal))


def _read_labels(header) -> List[str]:
    """Pull the 10 80-char label slots from an MRC header. Returns the
    non-empty ones with leading/trailing whitespace stripped."""
    labels: List[str] = []
    nlabl = int(getattr(header, "nlabl", 0) or 0)
    label_field = getattr(header, "label", None)
    if label_field is None:
        return labels
    for i in range(min(nlabl, 10)):
        try:
            raw = bytes(label_field[i])
            text = raw.decode("utf-8", errors="replace").strip("\x00 \t\n\r")
            if text:
                labels.append(text)
        except Exception:
            continue
    return labels


# RFC: a "good" PDB ID is 4 chars, must start with a digit. EMDB IDs are
# numeric (4-5 digits typically). We tolerate "PDB:6Z6J", "PDB 6z6j",
# "PDB-6Z6J" and friends.
_RE_PDB = re.compile(r"\bPDB[\s:_\-]*([0-9][a-z0-9]{3})\b", re.IGNORECASE)
_RE_EMDB = re.compile(r"\bEMD(?:B)?[\s:_\-]*([0-9]{3,5})\b", re.IGNORECASE)
_TOOL_KEYWORDS = (
    ("relion", "relion"),
    ("pytom", "pytom"),
    ("warptools", "warptools"),
    ("warp", "warptools"),
    ("chimera", "chimera"),
    ("imod", "imod"),
    ("eman", "eman2"),
    ("cryosparc", "cryosparc"),
)


def _infer_provenance(labels: List[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Best-effort regex over concatenated header labels. Looks for PDB id,
    EMDB id, and a tool name. Each is independently optional."""
    if not labels:
        return None, None, None
    text = " ".join(labels)
    pdb_match = _RE_PDB.search(text)
    emdb_match = _RE_EMDB.search(text)
    pdb_id = pdb_match.group(1).upper() if pdb_match else None
    emdb_id = emdb_match.group(1) if emdb_match else None
    lower = text.lower()
    tool: Optional[str] = None
    for keyword, name in _TOOL_KEYWORDS:
        if keyword in lower:
            tool = name
            break
    return pdb_id, emdb_id, tool

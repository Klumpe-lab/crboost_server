"""SerialEM tilt stacks as a landing-page source.

A SerialEM delivery ships one frame-aligned stack per tilt-series (`<ts>.mrc` +
`<ts>.mrc.mdoc`, one `[ZValue]` section per slice). The pipeline's input contract is
one image file per tilt in `frames/`, so the stack is split at Create — nothing after
the landing page knows stacks exist. This module holds the two stack-specific pieces:
the classification probe the scan and the import share, and the splitter.

`mrcfile` is imported lazily (precedent: `services/tomogram_import.py`) so importing
this module stays cheap on the UI thread.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


def stack_section_count(path: Path) -> int | None:
    """`nz` of an MRC file from its header alone; None when the file is missing or not
    an MRC. No data is read — this runs inside the landing-page scan."""
    import mrcfile

    if not path.is_file():
        return None
    try:
        with mrcfile.open(str(path), permissive=True, header_only=True) as mrc:
            return int(mrc.header.nz)
    except (ValueError, OSError) as e:
        # Not an MRC (or unreadable): the series simply has no usable stack layer.
        logger.warning("stack probe: %s is not a readable MRC (%s)", path, e)
        return None


def resolve_stack(mdoc_path: Path, image_file: str, n_sections: int) -> tuple[Path, int] | None:
    """`(stack_path, nz)` when `<mdoc dir>/<ImageFile>` is an MRC whose slice count equals
    the mdoc's `[ZValue]` sections — the invariant that makes slice z ↔ section z.
    None otherwise (absent file, not an MRC, or a count mismatch, which is logged)."""
    if not image_file or n_sections <= 0:
        return None
    candidate = mdoc_path.parent / Path(image_file.replace("\\", "/")).name
    nz = stack_section_count(candidate)
    if nz is None:
        return None
    if nz != n_sections:
        logger.warning(
            "stack probe: %s has %d slices but %s has %d sections", candidate, nz, mdoc_path.name, n_sections
        )
        return None
    return candidate, nz


def choose_source_layer(software: str, movies_complete: bool, movies_partial: bool, has_stack: bool) -> str:
    """The one rule for scan and import: `"stack"` when a valid stack
    exists and either the movies are incomplete or the dialect is SerialEM (whose
    stacks are the frame-aligned data its users mean; Tomo5 stacks are unaligned sums);
    `"movies"` when any movie resolved; `"missing"` otherwise."""
    if has_stack and (not movies_complete or software == "SerialEM"):
        return "stack"
    if movies_complete or movies_partial:
        return "movies"
    return "missing"


def frame_name_for_slice(sub_frame_path: str) -> str:
    """`Z:\\…\\area_5-A_ts_002_001_000_-10.0.tif` → `area_5-A_ts_002_001_000_-10.0.mrc`
    (the slice keeps the movie's basename, single suffix swapped)."""
    return Path(Path(sub_frame_path.replace("\\", "/")).name).with_suffix(".mrc").name


@dataclass(frozen=True)
class SplitResult:
    ts_name: str
    n_frames: int
    frame_names: list[str]
    mdoc_text: str  # original mdoc, only the SubFramePath lines rewritten


def split_stack(
    mdoc_path: Path, out_dir: Path, *, prefix: str, progress: Callable[[str], None] | None = None
) -> SplitResult:
    """Write slice z of `<mdoc dir>/<ImageFile>` as `<out_dir>/<prefix><movie basename>.mrc`
    for every `[ZValue = z]` section of `mdoc_path`, dtype kept, voxel size = PixelSpacing.

    Raises `ValueError` when the stack's slice count differs from the section count
    (Create fails loud, no half project). Idempotent: a slice whose file already exists
    with the right size is skipped. Returns the mdoc text with each section's
    `SubFramePath` rewritten to the new basename and every other line verbatim, so the
    `[T = …]` header survives for later scans."""
    import mrcfile
    import numpy as np

    from services.configs.mdoc_service import acquisition_from_mdoc, get_mdoc_service, ts_name_from_mdoc

    parsed = get_mdoc_service().parse_mdoc_file(mdoc_path)
    facts = acquisition_from_mdoc(parsed)
    sections = parsed["data"]
    ts_name = ts_name_from_mdoc(mdoc_path.name)
    if not facts.image_file:
        raise ValueError(f"{mdoc_path.name}: no ImageFile header — not a stack mdoc")
    stack_path = mdoc_path.parent / Path(facts.image_file.replace("\\", "/")).name

    names_by_z: dict[int, str] = {}
    for sec in sections:
        sub = sec.get("SubFramePath", "")
        if not sub:
            raise ValueError(f"{mdoc_path.name}: [ZValue = {sec.get('ZValue')}] has no SubFramePath")
        names_by_z[int(sec["ZValue"])] = f"{prefix}{frame_name_for_slice(sub)}"

    out_dir.mkdir(parents=True, exist_ok=True)
    frame_names: list[str] = []
    with mrcfile.mmap(str(stack_path), permissive=True) as stack:
        nz = int(stack.header.nz)
        if nz != len(sections):
            raise ValueError(
                f"{stack_path.name}: {nz} slices but {mdoc_path.name} has {len(sections)} [ZValue] sections — "
                f"the stack and its mdoc disagree; refusing to split"
            )
        apix = facts.pixel_size or float(stack.voxel_size.x)
        ny, nx = int(stack.header.ny), int(stack.header.nx)
        expected_size = 1024 + nx * ny * stack.data.dtype.itemsize
        for i, z in enumerate(sorted(names_by_z)):
            if z < 0 or z >= nz:
                raise ValueError(f"{mdoc_path.name}: [ZValue = {z}] outside the stack's {nz} slices")
            name = names_by_z[z]
            target = out_dir / name
            if progress:
                progress(f"{ts_name}: slice {i + 1}/{nz}")
            if not (target.exists() and target.stat().st_size == expected_size):
                with mrcfile.new(str(target), overwrite=True) as out:
                    out.set_data(np.ascontiguousarray(stack.data[z]))
                    out.voxel_size = apix
            frame_names.append(name)

    lines: list[str] = []
    current_z: int | None = None
    for raw in mdoc_path.read_text().splitlines():
        stripped = raw.strip()
        if stripped.startswith("[ZValue"):
            current_z = int(stripped.split("=", 1)[1].strip().rstrip("]").strip())
        elif stripped.startswith("SubFramePath") and current_z is not None:
            raw = f"SubFramePath = {names_by_z[current_z]}"
        lines.append(raw)
    return SplitResult(
        ts_name=ts_name, n_frames=len(frame_names), frame_names=frame_names, mdoc_text="\n".join(lines) + "\n"
    )

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


def stack_candidates(mdoc_path: Path, image_file: str) -> list[Path]:
    """Where the raw stack of `mdoc_path` may be, in order: `<stem>_orig<ext>` first, then
    `<ImageFile>`. IMOD's etomo renames the stack it was given to `<stem>_orig<ext>` when it
    puts an X-ray-erased stack in its place ("Use fixed stack"), so where both exist the
    `_orig` one is what SerialEM wrote."""
    if not image_file:
        return []
    named = mdoc_path.parent / Path(image_file.replace("\\", "/")).name
    return [named.with_name(f"{named.stem}_orig{named.suffix}"), named]


def resolve_stack(mdoc_path: Path, image_file: str, n_sections: int) -> tuple[Path, int] | None:
    """`(stack_path, nz)` for the first of `stack_candidates` that is an MRC whose slice count
    equals the mdoc's `[ZValue]` sections — the invariant that makes slice z ↔ section z.
    None otherwise (absent, not an MRC, or a count mismatch, which is logged)."""
    if n_sections <= 0:
        return None
    for candidate in stack_candidates(mdoc_path, image_file):
        if not candidate.is_file():
            continue
        nz = stack_section_count(candidate)
        if nz is None:
            continue
        if nz != n_sections:
            logger.warning(
                "stack probe: %s has %d slices but %s has %d sections", candidate, nz, mdoc_path.name, n_sections
            )
            continue
        return candidate, nz
    return None


def frames_tilt_axis(stack_axis: float, rotation_flip: int) -> float | None:
    """The tilt axis in the frames' orientation, from SerialEM's axis for the images it shows.

    SerialEM saves K2/K3 frames in the camera's native orientation and applies
    `RotationAndFlip` to the images (stack, mdoc axis). Operation 0 is the identity; 6 (flip
    about Y, then 180°) is a vertical mirror, which maps an axis angle a to −a — measured on a K3
    delivery (frame sum vs stack section). The other six are not derived (their sign
    convention is unchecked): None, and the axis stays for the user to give."""
    if rotation_flip == 0:
        return stack_axis
    if rotation_flip == 6:
        return -stack_axis
    return None


def frames_are_mirrored(rotation_flip: int) -> bool:
    """Operations 4–7 contain a flip: frames built into a tomogram need the other tilt-angle
    hand from the stack's."""
    return rotation_flip >= 4


@dataclass(frozen=True)
class FrameFacts:
    """What SerialEM's per-movie mdoc (`<movie>.mdoc`, one `[FrameSet]`) says about its movie."""

    gain_reference: str = ""  # basename, as SerialEM wrote it
    defect_file: str = ""
    rotation_flip: int | None = None
    binning: float | None = None  # 0.5 = super-resolution frames
    pixel_size: float | None = None


def read_frame_facts(movie_path: Path) -> FrameFacts | None:
    """`FrameFacts` from `<movie_path>.mdoc`; None when there is no such file."""
    mdoc = movie_path.with_name(movie_path.name + ".mdoc")
    if not mdoc.is_file():
        return None
    kv: dict[str, str] = {}
    for line in mdoc.read_text(errors="replace").splitlines():
        if "=" in line and not line.lstrip().startswith("["):
            k, v = line.split("=", 1)
            kv.setdefault(k.strip(), v.strip())

    def num(key: str) -> float | None:
        try:
            return float(kv[key]) if key in kv else None
        except ValueError:
            # A malformed number is an unknown fact; the caller shows it as missing.
            return None

    rf = num("RotationAndFlip")
    return FrameFacts(
        gain_reference=Path(kv.get("GainReference", "").replace("\\", "/")).name,
        defect_file=Path(kv.get("DefectFile", "").replace("\\", "/")).name,
        rotation_flip=int(rf) if rf is not None else None,
        binning=num("Binning"),
        pixel_size=num("PixelSpacing"),
    )


def same_image_data(paths: list[Path]) -> bool:
    """Whether the files hold the same image: equal MRC data arrays (headers may differ —
    SerialEM copies one reference per folder with its own labels), else equal bytes."""
    import mrcfile
    import numpy as np

    if len(paths) < 2:
        return True
    if all(p.suffix.lower() == ".mrc" for p in paths):
        with mrcfile.mmap(str(paths[0]), permissive=True) as first:
            for other in paths[1:]:
                with mrcfile.mmap(str(other), permissive=True) as o:
                    if first.data.shape != o.data.shape or not np.array_equal(first.data, o.data):
                        return False
        return True
    ref = paths[0].read_bytes()
    return all(p.read_bytes() == ref for p in paths[1:])


def find_beside_or_above(name: str, start: Path, root: Path | None) -> Path | None:
    """`name` in `start` or one of its parents, stopping at `root` (inclusive) or after 3 levels."""
    if not name:
        return None
    d = start
    for _ in range(4):
        if (d / name).is_file():
            return d / name
        if (root is not None and d == root) or d.parent == d:
            break
        d = d.parent
    return None


def choose_source_layer(
    software: str, movies_complete: bool, movies_partial: bool, has_stack: bool, chosen: str | None = None
) -> str:
    """The one rule for scan and import. With `chosen` (`"stack"` / `"movies"`, the user's
    pick for the whole project) that layer when the series has it, else `"missing"` — never
    the other layer. Without it: `"stack"` when a valid stack exists and either the movies
    are incomplete or the dialect is SerialEM (whose stacks are the frame-aligned data its
    users mean; Tomo5 stacks are unaligned sums); `"movies"` when any movie resolved;
    `"missing"` otherwise."""
    if chosen == "stack":
        return "stack" if has_stack else "missing"
    if chosen == "movies":
        return "movies" if movies_complete else "missing"
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
    resolved = resolve_stack(mdoc_path, facts.image_file, len(sections))
    if resolved is None:
        raise ValueError(f"{mdoc_path.name}: no stack with {len(sections)} slices beside it — refusing to split")
    stack_path = resolved[0]

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

"""Template matching restricted to a region of each tomogram, from its segmentations: the union of
the included features (p ≥ 0.5), grown by a distance, less the union of the excluded ones. pytom gets
it as `--tomogram-mask` (an int8 0/1 volume of exactly the tomogram's shape): while matching it skips
the sub-volumes entirely outside, and extraction sets the scores outside to 0.

The template-match driver builds each tomogram's mask on the compute node (`write_search_mask`);
the orchestrator checks a job's region against its segmentations before submitting it
(`search_region_problem`).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from services.segmentation.manifest import SegmentationEntry, SegmentationManifest
from services.segmentation.volumes import DISPLAY_THRESHOLD

MASK_SUFFIX = "_search_mask.mrc"
# Tomograms named per feature in a refusal before "and N more".
_LISTED = 6


def search_mask_path(tm_results: Path, tomo: str) -> Path:
    return Path(tm_results) / f"{tomo}{MASK_SUFFIX}"


def read_segmentations(path) -> SegmentationManifest:
    """The segmentations.json at `path` (the file a SEGMENTATIONS input names)."""
    return SegmentationManifest.model_validate_json(Path(path).read_text())


def describe(include: list[str], exclude: list[str], dilation_ang: float) -> str:
    """The region in words: "cytoplasm + 50 Å, less void"."""
    inside = " or ".join(include) if include else "the whole tomogram"
    if include and dilation_ang:
        inside += f" + {dilation_ang:g} Å"
    return inside + (f", less {' and '.join(exclude)}" if exclude else "")


def _feature_voxels(entry: SegmentationEntry) -> np.ndarray:
    """Where a feature's volume has p ≥ DISPLAY_THRESHOLD, as a bool array (z, y, x)."""
    import mrcfile

    with mrcfile.mmap(entry.path, mode="r", permissive=True) as m:
        if m.data is None or m.data.ndim != 3:
            raise ValueError(f"{entry.path} is not a complete 3D volume")
        return np.asarray(m.data) >= DISPLAY_THRESHOLD * entry.full_scale


def build_search_mask(
    entries: dict[str, SegmentationEntry], include: list[str], exclude: list[str], dilation_vox: float
) -> np.ndarray:
    """The region on one tomogram, as a bool array (z, y, x): the union of the `include` features
    (everything when there are none), grown by `dilation_vox` voxels, less the union of the `exclude`
    features. `entries` holds every named feature's volume of that tomogram, all on its grid."""
    region: np.ndarray | None = None
    for feature in include:
        voxels = _feature_voxels(entries[feature])
        region = voxels if region is None else region | voxels
    if region is None:
        region = np.ones(tuple(entries[exclude[0]].shape_zyx), dtype=bool)
    elif dilation_vox > 0:
        region = _dilate(region, dilation_vox)
    for feature in exclude:
        region &= ~_feature_voxels(entries[feature])
    return region


def _dilate(region: np.ndarray, radius_vox: float, chunk: int = 64) -> np.ndarray:
    """`region` grown by `radius_vox` voxels (Euclidean). The distance transform runs over Z slabs
    with a halo of the radius, which holds every region voxel within the radius of the slab, so
    memory stays at one slab's transform whatever the tomogram's size; a slab with no region voxel
    in reach stays empty."""
    from scipy.ndimage import distance_transform_edt

    nz = region.shape[0]
    halo = int(np.ceil(radius_vox))
    out = np.zeros_like(region)
    for z0 in range(0, nz, chunk):
        z1 = min(nz, z0 + chunk)
        lo, hi = max(0, z0 - halo), min(nz, z1 + halo)
        block = region[lo:hi]
        if block.any():
            out[z0:z1] = distance_transform_edt(~block)[z0 - lo : z1 - lo] <= radius_vox
    return out


def write_search_mask(path: Path, mask: np.ndarray, voxel_angstrom: float) -> None:
    """`mask` as the int8 0/1 MRC pytom reads, with the tomogram's voxel size."""
    import mrcfile

    path.parent.mkdir(parents=True, exist_ok=True)
    with mrcfile.new(str(path), data=mask.astype(np.int8), overwrite=True) as m:
        m.voxel_size = voxel_angstrom


def search_region_problem(
    params, paths: dict[str, str], *, planned: list[str] | None, excluded: set[str]
) -> str | None:
    """Why a template-match job (`params`) cannot run its search region on the inputs it was just
    resolved to (`paths`), or None. A feature the region names must be segmented for every tomogram
    the job will match (every Nth of its input, less the tilt series `excluded` from processing): read
    from the producer's segmentations.json once it exists, else from the features a Segment job is
    set to make (`planned`; None when the producer cannot say before it runs). Reads disk."""
    from services.visualization.tomo_geometry import read_tomo_table

    if not params.restricts_search:
        return None
    both = sorted(set(params.search_include) & set(params.search_exclude))
    if both:
        return f"its search region both searches in and leaves out {', '.join(both)}, so nothing is left"
    named = sorted(set(params.search_include) | set(params.search_exclude))
    source = paths.get("input_segmentations")
    if not source:
        return "its search region has no segmentations to read"
    if not Path(source).exists():
        lack = [f for f in named if planned is not None and f not in planned]
        if lack:
            return f"its search region names {', '.join(lack)}, which its segmentations job is not set to make"
        return None
    try:
        manifest = read_segmentations(source)
    except (OSError, ValueError) as e:
        return f"its segmentations could not be read ({source}): {e}"
    have = {(e.tomogram, e.feature) for e in manifest.entries}
    table = read_tomo_table(Path(paths["input_tomograms"])) if paths.get("input_tomograms") else None
    if table is None:
        # The tomograms are not written yet: only what the segmentations hold at all can be checked.
        lack = [f for f in named if f not in {e.feature for e in manifest.entries}]
        return f"its segmentations have no {', '.join(lack)}" if lack else None
    names = sorted(str(n) for n in table["rlnTomoName"])[:: max(1, int(params.every_nth_tomogram or 1))]
    missing = {t: [f for f in named if (t, f) not in have] for t in names if t not in excluded}
    missing = {t: fs for t, fs in missing.items() if fs}
    if not missing:
        return None
    listed = "; ".join(f"{t} ({', '.join(fs)})" for t, fs in list(missing.items())[:_LISTED])
    more = f" and {len(missing) - _LISTED} more" if len(missing) > _LISTED else ""
    return (
        f"{len(missing)} tomogram(s) it would match lack a feature of its search region in "
        f"{manifest.producer}: {listed}{more}. Segment them, or exclude them from processing"
    )

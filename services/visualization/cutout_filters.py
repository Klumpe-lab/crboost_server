"""Display lowpass/bandpass filters for pick-cutout sprite atlases.

The Journey tab's cutout tiles are eyeballed to curate picks. On non-denoised
tomograms the raw tiles are noisy and the particle is hard to see. This module
provides display-only filters (they NEVER touch the data, the picks, or any
star file) plus the shared atlas-assembly used by BOTH cutout pipelines:

  - subtomo-extracted cutouts  → services/visualization/preview_render.py
  - recon-sourced cutouts      → services/visualization/recon_cutouts.py

Both pipelines reduce to the same thing: a list of 2D float frames (one per
pick, in pick order, None where a pick had no usable cutout). `build_filtered_atlas`
takes that list, applies one filter preset, normalizes tomogram-wide, and lays
the tiles into a sprite-atlas PNG array + an index dict — byte-identical schema
to the previous per-module assembly, so the gallery consumes it unchanged.

The cutoff is specified in Ångström and converted to a pixel frequency with the
source file's own pixel size (`apix`, read from the MRC header by the caller) —
so recon cutouts (reconstruction bin) and subtomo cutouts (extraction bin), which
have DIFFERENT pixel sizes, each filter at the right scale automatically. The
resolved apix and its provenance ride along in the index JSON so the UI can show
the user exactly what value was used and where it came from.

numpy is a hard dep (already true for both pipelines); PIL is imported lazily.
No scipy — the lowpass is a numpy-FFT soft (Butterworth) filter.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


# ── Filter presets ──────────────────────────────────────────────────────────
# CONFIGURATION POINT. Edit this list to change the filters offered in the
# gallery. Each preset:
#   key          stable id (variant atlas filename + UI value)
#   type         implementation group for the TYPE dropdown
#   type_label   display name of the type
#   param_label  option label within the type (the PARAM dropdown)
#   kind         render dispatch: raw | lowpass | bandpass | denoise | clahe
#   + kind params: cutoff_ang/highpass_ang (freq) | weight (denoise) | clip_limit (clahe)
# Pure lowpass only blurs; the genuinely useful filters for noisy, non-denoised
# cutouts are edge-preserving DENOISE (TV) and LOCAL CONTRAST (CLAHE) — listed
# first so they're the obvious choices. Defaults tuned for large viral VLPs
# (Copia/412, ~40-60 nm). `raw` MUST stay first and is the default selection.
DEFAULT_FILTER_PRESETS: list[dict] = [
    {"key": "raw", "type": "raw", "type_label": "None (raw)", "param_label": "", "kind": "raw"},
    {
        "key": "denoise_lo",
        "type": "denoise",
        "type_label": "Denoise (edge-safe)",
        "param_label": "Light",
        "kind": "denoise",
        "weight": 0.04,
    },
    {
        "key": "denoise_hi",
        "type": "denoise",
        "type_label": "Denoise (edge-safe)",
        "param_label": "Strong",
        "kind": "denoise",
        "weight": 0.12,
    },
    {
        "key": "clahe_lo",
        "type": "clahe",
        "type_label": "Local contrast",
        "param_label": "Subtle",
        "kind": "clahe",
        "clip_limit": 0.01,
    },
    {
        "key": "clahe_hi",
        "type": "clahe",
        "type_label": "Local contrast",
        "param_label": "Punchy",
        "kind": "clahe",
        "clip_limit": 0.03,
    },
    {
        "key": "bp",
        "type": "bandpass",
        "type_label": "Bandpass",
        "param_label": "35-800 Å",
        "kind": "bandpass",
        "cutoff_ang": 35.0,
        "highpass_ang": 800.0,
    },
    {
        "key": "lp30",
        "type": "lowpass",
        "type_label": "Lowpass",
        "param_label": "30 Å",
        "kind": "lowpass",
        "cutoff_ang": 30.0,
    },
    {
        "key": "lp50",
        "type": "lowpass",
        "type_label": "Lowpass",
        "param_label": "50 Å",
        "kind": "lowpass",
        "cutoff_ang": 50.0,
    },
]

# Bumped whenever the preset SET changes, so cached atlases regenerate: the
# subtomo path keys off the manifest version, the recon/manual path off the
# `filter_schema` stamped into each index JSON.
FILTER_SCHEMA_VERSION = 2

# Butterworth order — higher = sharper edge. 4 is a soft rolloff with no
# meaningful Gibbs ringing at thumbnail scale.
_BUTTER_ORDER = 4
FREQ_KINDS = ("raw", "lowpass", "bandpass")


def get_filter_presets() -> list[dict]:
    """The active preset list. Single accessor so a future conf.yaml override is
    a one-function change; today it returns the module default."""
    return DEFAULT_FILTER_PRESETS


def is_raw(preset: Optional[dict]) -> bool:
    return preset is None or preset.get("kind", "raw") == "raw"


def resolve_apix(header_voxel_size: Optional[float], hint: Optional[float]) -> tuple[Optional[float], str]:
    """Resolve the pixel size (Å/px) to use for Å→px conversion, with provenance.

    Prefers the source MRC header's own value (so recon vs subtomo each self-report
    their true bin); falls back to a caller hint (e.g. job pixel size); else marks
    it unknown so the UI can warn rather than show a silently-wrong cutoff.
    """
    try:
        v = float(header_voxel_size) if header_voxel_size is not None else 0.0
    except (TypeError, ValueError):
        v = 0.0
    if v > 0:
        return v, "MRC header"
    try:
        h = float(hint) if hint is not None else 0.0
    except (TypeError, ValueError):
        h = 0.0
    if h > 0:
        return h, "job params"
    return None, "unknown"


def _butterworth(r: np.ndarray, fc: float, order: int) -> np.ndarray:
    """Lowpass transfer function, soft edge, no ringing. r, fc in cycles/px."""
    if fc <= 0:
        return np.ones_like(r)
    return 1.0 / np.sqrt(1.0 + (r / fc) ** (2 * order))


def apply_filter_2d(arr: np.ndarray, apix: Optional[float], preset: Optional[dict]) -> np.ndarray:
    """Apply a display filter to a 2D float frame. Returns a float32 array.

    `raw` (or a preset needing apix when apix is unknown) is a no-op passthrough.
    Cutoffs are Å; converted to cycles/px via f = apix / cutoff_ang.
    """
    if is_raw(preset) or apix is None or apix <= 0:
        return np.asarray(arr, dtype=np.float32)
    a = np.nan_to_num(np.asarray(arr, dtype=np.float32))
    ny, nx = a.shape
    fy = np.fft.fftfreq(ny)[:, None]
    fx = np.fft.fftfreq(nx)[None, :]
    r = np.sqrt(fy * fy + fx * fx)  # cycles/px, DC at [0,0] (fft2 layout)
    mask = np.ones_like(r)
    cutoff_ang = preset.get("cutoff_ang")
    highpass_ang = preset.get("highpass_ang")
    if cutoff_ang:
        mask *= _butterworth(r, apix / float(cutoff_ang), _BUTTER_ORDER)
    if highpass_ang:
        # Highpass = 1 - lowpass at the (low) highpass frequency: removes the
        # slow background below it while keeping the particle band.
        mask *= 1.0 - _butterworth(r, apix / float(highpass_ang), _BUTTER_ORDER)
    out = np.fft.ifft2(np.fft.fft2(a) * mask).real
    return out.astype(np.float32)


def _normalize01(arr: np.ndarray, lo: float, hi: float) -> np.ndarray:
    return np.clip((np.nan_to_num(np.asarray(arr, dtype=np.float32)) - lo) / (hi - lo), 0.0, 1.0)


def _denoise(n01: np.ndarray, weight: float) -> np.ndarray:
    """Edge-preserving total-variation denoise on a [0,1] image. Unlike a lowpass
    it suppresses noise while keeping the particle's boundary sharp. Degrades to a
    passthrough if scikit-image is unavailable."""
    try:
        from skimage.restoration import denoise_tv_chambolle
    except Exception:
        return n01
    return denoise_tv_chambolle(n01, weight=weight).astype(np.float32)


def _clahe(n01: np.ndarray, clip_limit: float) -> np.ndarray:
    """Contrast-limited adaptive histogram equalization on a [0,1] image — boosts
    LOCAL contrast so a faint particle pops out of an uneven background. Degrades
    to a passthrough if scikit-image is unavailable."""
    try:
        from skimage.exposure import equalize_adapthist
    except Exception:
        return n01
    return equalize_adapthist(np.clip(n01, 0.0, 1.0), clip_limit=clip_limit).astype(np.float32)


def _image_op(n01: np.ndarray, kind: str, preset: dict) -> np.ndarray:
    if kind == "denoise":
        return _denoise(n01, float(preset.get("weight", 0.08)))
    if kind == "clahe":
        return _clahe(n01, float(preset.get("clip_limit", 0.02)))
    return n01


def _pool_bounds(frames: list, sample_n: int = 12) -> tuple[float, float]:
    """Tomogram-wide (lo, hi) 1-99 percentile from a stratified sample of the
    (already-filtered) frames, so every tile shares one clip and pure-noise tiles
    stay uniformly grey. Falls back to (0, 1) if nothing is readable."""
    valid = [f for f in frames if f is not None]
    if not valid:
        return 0.0, 1.0
    if len(valid) <= sample_n:
        chosen = valid
    else:
        step = len(valid) / sample_n
        chosen = [valid[int(i * step)] for i in range(sample_n)]
    flat = np.concatenate([f.ravel() for f in chosen])
    lo = float(np.percentile(flat, 1.0))
    hi = float(np.percentile(flat, 99.0))
    if hi <= lo:
        hi = lo + 1.0
    return lo, hi


def build_filtered_atlas(
    frames: list, fail_info: Optional[list], *, apix: Optional[float], preset: dict, tile_px: int = 192, cols: int = 8
) -> dict:
    """Filter + normalize + assemble one sprite atlas from per-pick 2D float frames.

    `frames[i]` is the float (y, x) frame for pick i, or None if that pick has no
    usable cutout. `fail_info[i]` (optional, aligned) is a dict describing WHY pick
    i is None (e.g. {"reason": "no subtomo match", "mrcs": "..."}); used verbatim in
    the `failures` list so the UI keeps its precise diagnostics.

    Returns {atlas (uint8 2D), index {str(i): [r, c]}, failures, n_ok, rows, cols,
    tile_px, norm_lo, norm_hi}. Tile positions are independent of the preset, so all
    variants share one index layout (only the pixels and norm bounds differ).
    """
    from PIL import Image  # lazy; absent in the bare venv

    n = len(frames)
    kind = preset.get("kind", "raw")
    # Two normalization regimes:
    #  - freq (raw/lowpass/bandpass): FFT-filter the raw frame, then clip to a
    #    tomogram-wide percentile of the FILTERED frames (keeps a lowpass from
    #    washing the contrast out).
    #  - image ops (denoise/clahe): clip the RAW frame to a tomogram-wide
    #    percentile → [0,1], then run the edge-safe denoise / local-contrast op
    #    (which expect and return [0,1]). The display image is in [0,1] either way.
    if kind in ("denoise", "clahe"):
        lo, hi = _pool_bounds(frames)
        display = [(_image_op(_normalize01(f, lo, hi), kind, preset) if f is not None else None) for f in frames]
    else:
        filtered = [apply_filter_2d(f, apix, preset) if f is not None else None for f in frames]
        lo, hi = _pool_bounds(filtered)
        display = [(_normalize01(f, lo, hi) if f is not None else None) for f in filtered]

    rows = (n + cols - 1) // cols
    atlas = np.zeros((rows * tile_px, cols * tile_px), dtype=np.uint8)
    index: dict[str, list[int]] = {}
    failures: list[dict] = []
    n_ok = 0
    for i, d in enumerate(display):
        if d is None:
            info = fail_info[i] if (fail_info and i < len(fail_info) and fail_info[i]) else {"reason": "render failed"}
            failures.append({"i": i, **info})
            continue
        u8 = (np.clip(d, 0.0, 1.0) * 255.0).astype(np.uint8)
        img = Image.fromarray(u8, mode="L").resize((tile_px, tile_px), Image.LANCZOS)
        r, c = divmod(i, cols)
        atlas[r * tile_px : (r + 1) * tile_px, c * tile_px : (c + 1) * tile_px] = np.asarray(img, dtype=np.uint8)
        index[str(i)] = [r, c]
        n_ok += 1
    return {
        "atlas": atlas,
        "index": index,
        "failures": failures,
        "n_ok": n_ok,
        "rows": rows,
        "cols": cols,
        "tile_px": tile_px,
        "norm_lo": float(lo),
        "norm_hi": float(hi),
    }


def keyed_path(base_path: Path, key: str) -> Path:
    """Variant filename for a preset: `raw` keeps the base name (back-compat);
    others insert `__<key>` before the suffix (cutout_atlas.png → cutout_atlas__lp30.png)."""
    base_path = Path(base_path)
    if key == "raw":
        return base_path
    return base_path.with_name(f"{base_path.stem}__{key}{base_path.suffix}")


def emit_filtered_atlases(
    frames: list,
    fail_info: Optional[list],
    raw_atlas_path: Path,
    raw_index_path: Path,
    *,
    apix_header: Optional[float],
    apix_hint: Optional[float],
    filters: Optional[list],
    tile_px: int,
    cols: int,
    source: str,
) -> Optional[dict]:
    """Render one atlas per filter preset from preloaded 2D float `frames`.

    Shared by both cutout pipelines: the caller produces `frames` (+ aligned
    `fail_info`) and the source MRC's header pixel size; this resolves apix,
    builds + saves the `raw` atlas plus a variant per preset, and returns the
    raw atlas's metadata augmented with `apix`, `apix_source`, and a `variants`
    map {key: {atlas, index, n_ok}}. Returns None if every pick failed (a raw
    index JSON is still written so the caller can see why). When apix is unknown,
    only `raw` is produced (Å→px conversion would be meaningless).
    """
    import importlib.util

    if importlib.util.find_spec("PIL") is None:
        logger.warning("Cutout atlas deps unavailable: Pillow (PIL) not installed")
        return None

    apix, apix_source = resolve_apix(apix_header, apix_hint)
    presets = list(filters) if filters else get_filter_presets()
    if apix is None:
        # Only the frequency filters need apix; denoise/CLAHE are apix-independent,
        # so drop just the lowpass/bandpass presets when the pixel size is unknown.
        presets = [p for p in presets if p.get("kind") not in ("lowpass", "bandpass")]

    n = len(frames)
    variants: dict[str, dict] = {}
    raw_meta: Optional[dict] = None
    for preset in presets:
        key = preset["key"]
        built = build_filtered_atlas(frames, fail_info, apix=apix, preset=preset, tile_px=tile_px, cols=cols)
        atlas_p = keyed_path(raw_atlas_path, key)
        index_p = keyed_path(raw_index_path, key)
        extra = {
            "source": source,
            "apix": apix,
            "apix_source": apix_source,
            "filter_schema": FILTER_SCHEMA_VERSION,
            "filter": {
                k: preset.get(k)
                for k in (
                    "key",
                    "type",
                    "type_label",
                    "param_label",
                    "kind",
                    "cutoff_ang",
                    "highpass_ang",
                    "weight",
                    "clip_limit",
                )
            },
        }
        if key == "raw" and built["n_ok"] == 0:
            # All picks failed: still write the raw index so the caller surfaces why.
            save_atlas(built, atlas_p, index_p, extra=extra)
            return None
        if built["n_ok"] == 0:
            continue
        save_atlas(built, atlas_p, index_p, extra=extra)
        variants[key] = {"atlas": str(atlas_p), "index": str(index_p), "n_ok": built["n_ok"]}
        if key == "raw":
            raw_meta = {
                "atlas_path": str(atlas_p),
                "index_path": str(index_p),
                "n_ok": built["n_ok"],
                "n_total": n,
                "failures": built["failures"],
                "norm_bounds": (built["norm_lo"], built["norm_hi"]),
            }
    if raw_meta is None:
        return None
    raw_meta.update({"apix": apix, "apix_source": apix_source, "variants": variants})
    return raw_meta


def save_atlas(built: dict, atlas_path: Path, index_path: Path, *, extra: Optional[dict] = None) -> None:
    """Write the atlas PNG + index JSON. `extra` is merged into the index payload
    (apix, apix_source, filter block, source, etc.)."""
    import json

    from PIL import Image

    atlas_path = Path(atlas_path)
    index_path = Path(index_path)
    atlas_path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(built["atlas"], mode="L").save(str(atlas_path), format="PNG", optimize=True)
    payload = {
        "tile_px": built["tile_px"],
        "cols": built["cols"],
        "rows": built["rows"],
        "n_picks": len(built["index"]) + len(built["failures"]),
        "n_ok": built["n_ok"],
        "atlas_w": built["cols"] * built["tile_px"],
        "atlas_h": built["rows"] * built["tile_px"],
        "index": built["index"],
        "failures": built["failures"],
        "norm_lo": built["norm_lo"],
        "norm_hi": built["norm_hi"],
    }
    if extra:
        payload.update(extra)
    index_path.write_text(json.dumps(payload))

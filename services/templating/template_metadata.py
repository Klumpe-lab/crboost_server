"""Read MRC headers for templates (apix + dims) with an mtime-keyed cache.

Templates are tiny but we still don't want to re-open them on every render.
Cache invalidates whenever the file's mtime changes, so editing or
re-rendering a template is picked up on the next read without a manual
flush.

Disk is the single source of truth for template apix and box. Per the v2
plan we deliberately do NOT persist these on the ParticleTemplate model —
the model's job is identity, provenance, and editor state, not a stale
mirror of the MRC header.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import NamedTuple

logger = logging.getLogger(__name__)


class TemplateHeader(NamedTuple):
    """Minimal MRC header view used by template-introspection UI.

    box_px is the smallest of (nx, ny, nz) — the conservative box for
    sanity checks. nx/ny/nz exposed separately so callers can warn on
    non-cube templates.

    dmin/dmax/dmean/rms come straight from the MRC header — `mrcfile`
    updates these on close after `set_data`, so they reflect the actual
    file content without us re-reading the volume. Surfaces normalization
    state ("is this template at std≈1 like the ellipsoid, or near zero?").
    """

    apix_ang: float | None
    box_px: int | None
    nx: int | None
    ny: int | None
    nz: int | None
    dmin: float | None = None
    dmax: float | None = None
    dmean: float | None = None
    rms: float | None = None

    @classmethod
    def empty(cls) -> TemplateHeader:
        return cls(None, None, None, None, None, None, None, None, None)


_HEADER_CACHE: dict[tuple[str, int], TemplateHeader] = {}


def read_template_header(template_path: str) -> TemplateHeader:
    """Read voxel_size and dims from an MRC header. Returns an empty
    TemplateHeader on missing path / unreadable file / no voxel_size."""
    if not template_path:
        return TemplateHeader.empty()
    try:
        st = Path(template_path).stat()
    except OSError:
        return TemplateHeader.empty()
    key = (template_path, int(st.st_mtime))
    cached = _HEADER_CACHE.get(key)
    if cached is not None:
        return cached
    info = TemplateHeader.empty()
    try:
        import mrcfile

        with mrcfile.open(template_path, header_only=True, mode="r") as m:
            vx = float(getattr(m.voxel_size, "x", 0.0) or 0.0)
            apix = vx if vx > 0 else None
            nx = int(m.header.nx)
            ny = int(m.header.ny)
            nz = int(m.header.nz)
            d_min = min((d for d in (nx, ny, nz) if d > 0), default=0)
            box = d_min if d_min > 0 else None
            # dmin/dmax/dmean/rms in the header are RELION/mrcfile-written
            # statistics; cast through float() so we don't leak numpy
            # scalar types into the UI layer.
            dmin = float(m.header.dmin)
            dmax = float(m.header.dmax)
            dmean = float(m.header.dmean)
            rms = float(m.header.rms)
            info = TemplateHeader(
                apix_ang=apix,
                box_px=box,
                nx=nx or None,
                ny=ny or None,
                nz=nz or None,
                dmin=dmin,
                dmax=dmax,
                dmean=dmean,
                rms=rms,
            )
    except Exception as e:
        logger.warning("Could not read template header %s: %s", template_path, e)
    _HEADER_CACHE[key] = info
    return info


def get_effective_template_path(species) -> str:
    """Resolve the path of the species's selected template (v3 schema).

    Returns the path of the template whose id matches
    species.selected_template_id, or "" if no template is selected /
    the selected id no longer exists.
    """
    if species is None:
        return ""
    get_sel = getattr(species, "get_selected_template", None)
    if callable(get_sel):
        tpl = get_sel()
        if tpl is not None:
            return getattr(tpl, "template_path", "") or ""
    return ""


def get_effective_mask_path(species) -> str:
    """Resolve the path of the species's selected mask (v3 schema)."""
    if species is None:
        return ""
    get_sel = getattr(species, "get_selected_mask", None)
    if callable(get_sel):
        mask = get_sel()
        if mask is not None:
            return getattr(mask, "mask_path", "") or ""
    return ""


def get_selected_template(species):
    """Return the species's selected ParticleTemplate or None."""
    if species is None:
        return None
    get_sel = getattr(species, "get_selected_template", None)
    return get_sel() if callable(get_sel) else None


def get_selected_mask(species):
    """Return the species's selected TemplateMask or None."""
    if species is None:
        return None
    get_sel = getattr(species, "get_selected_mask", None)
    return get_sel() if callable(get_sel) else None

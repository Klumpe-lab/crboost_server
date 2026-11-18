"""One drawn glyph per job type for the roster's gutter: 12 × 12, a single 1.1 px line in the ink it is given, no
fill but the odd dot. Two sets: `drawn` (these) and `material` (the Material icon names the branch map uses today),
so the gutter can be looked at with either."""

from __future__ import annotations

from ui.branch_map.model import ICON

_P = {
    # tray and arrow down: data coming in
    "importmovies": '<path d="M6 1.5v6M3.6 5.2 6 7.6l2.4-2.4M1.5 8.2v2.3h9V8.2"/>',
    "tsImport": '<path d="M6 1.5v6M3.6 5.2 6 7.6l2.4-2.4M1.5 8.2v2.3h9V8.2"/>',
    # a stack of frames, the front one sharp
    "fsMotionAndCtf": '<rect x="1.5" y="4" width="6.5" height="6.5"/><path d="M3.5 4V2h6.5v6.5H8"/>',
    # a funnel
    "tiltFilter": '<path d="M1.5 2h9L7 6.4v3.4l-2 .9V6.4z"/>',
    # the fan of tilts from one axis
    "aligntiltsWarp": '<path d="M6 10.5 2 2.8M6 10.5V1.5M6 10.5l4-7.7M3.2 8.6a3.6 3.6 0 0 1 5.6 0"/>',
    # the fan, with the refined one marked
    "missAlign": '<path d="M6 10.5 2.2 3M6 10.5 9.8 3"/><circle cx="6" cy="3.6" r="1.4"/>',
    # Thon rings
    "tsCtf": (
        '<circle cx="6" cy="6" r="4.6"/>'
        '<circle cx="6" cy="6" r="2.6"/>'
        '<circle cx="6" cy="6" r=".7" fill="currentColor"/>'
    ),
    # a volume
    "tsReconstruct": '<path d="M6 1.5 10.5 4v4.8L6 11.3 1.5 8.8V4zM1.5 4 6 6.5 10.5 4M6 6.5v4.8"/>',
    # a model: nodes and their links
    "denoisetrain": (
        '<circle cx="2.6" cy="3" r="1.1"/>'
        '<circle cx="2.6" cy="9" r="1.1"/>'
        '<circle cx="9.4" cy="6" r="1.1"/>'
        '<path d="M3.6 3.5 8.4 5.6M3.6 8.5l4.8-2.1M2.6 4.1v3.8"/>'
    ),
    # the model applied: a wand
    "denoisepredict": '<path d="M1.8 10.2 7.4 4.6M8.6 1.4v2.2M7.5 2.5h2.2M10 5.2v1.4M9.3 5.9h1.4"/>',
    # a region, hatched
    "segment": (
        '<path d="M2 6.2C2 3.4 4 1.8 6.4 2.2c2.6.4 3.8 2.4 3.4 4.6-.4 2.2-2.4 3.4-4.6 3.2C3.2 9.8 2 8.4 2 6.2z"/>'
        '<path d="M4 8.6 8.2 4.4M3.2 6.2l3-3M6.2 9.4l3-3"/>'
    ),
    # a list cut short
    "subset": '<path d="M1.5 2.5h9M1.5 5h9M1.5 7.5h5M1.5 10h2.5"/>',
    # cross-hairs
    "templatematching": '<circle cx="6" cy="6" r="3.6"/><path d="M6 .8v2.6M6 8.6v2.6M.8 6h2.6M8.6 6h2.6"/>',
    # scattered candidates, one ringed
    "tmextractcand": (
        '<circle cx="3" cy="3.2" r=".9" fill="currentColor"/>'
        '<circle cx="8.8" cy="2.8" r=".9" fill="currentColor"/>'
        '<circle cx="2.8" cy="9" r=".9" fill="currentColor"/><circle cx="8.2" cy="8.4" r="2.2"/>'
        '<circle cx="8.2" cy="8.4" r=".7" fill="currentColor"/>'
    ),
    # points along a filament
    "filamentPick": (
        '<path d="M1.5 9.5C4 9.5 4 2.5 6.5 2.5s2.4 4 4 4"/>'
        '<circle cx="3.6" cy="7.4" r=".8" fill="currentColor"/>'
        '<circle cx="6.5" cy="2.5" r=".8" fill="currentColor"/><circle cx="9.2" cy="5.6" r=".8" fill="currentColor"/>'
    ),
    # boxes cut out
    "subtomoExtraction": (
        '<rect x="1.5" y="1.5" width="3.8" height="3.8"/>'
        '<rect x="6.7" y="1.5" width="3.8" height="3.8"/>'
        '<rect x="1.5" y="6.7" width="3.8" height="3.8"/><rect x="6.7" y="6.7" width="3.8" height="3.8"/>'
    ),
    # a box and an arrow out
    "warpExport": '<path d="M6.5 2H2v8.5h8.5V6M7.5 1.5h3v3M10.5 1.5 5.8 6.2"/>',
    # a sphere
    "reconstructParticle": '<circle cx="6" cy="6" r="4.6"/><ellipse cx="6" cy="6" rx="4.6" ry="1.7"/>',
    # classes: three apart
    "class3d": (
        '<circle cx="6" cy="2.9" r="1.6"/><circle cx="2.9" cy="8.5" r="1.6"/><circle cx="9.1" cy="8.5" r="1.6"/>'
    ),
    # a map sharpened: a ring around a solid core
    "refine3d": '<circle cx="6" cy="6" r="4.6"/><circle cx="6" cy="6" r="2" fill="currentColor"/>',
    # a soft mask: the outline dashed
    "maskCreate": '<circle cx="6" cy="6" r="4.6" stroke-dasharray="1.6 1.3"/><circle cx="6" cy="6" r="2.4"/>',
    # the FSC falling off
    "postprocess": '<path d="M1.5 1.5v9h9M2.6 2.8c2.6 0 2.8 6 7.4 6.6"/>',
}


def glyph(kind: str, *, size: int = 12, ink: str = "currentColor", icons: str = "drawn") -> str:
    """The job type's glyph as inline HTML."""
    if icons == "material":
        name = ICON.get(kind, "radio_button_unchecked")
        return (
            f'<i class="q-icon notranslate material-icons" aria-hidden="true" '
            f'style="font-size:{size + 1}px;color:{ink};line-height:1;">{name}</i>'
        )
    body = _P.get(kind, '<rect x="2" y="2" width="8" height="8"/>')
    return (
        f'<svg width="{size}" height="{size}" viewBox="0 0 12 12" fill="none" stroke="currentColor" '
        f'stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round" style="color:{ink};display:block;">'
        f"{body}</svg>"
    )

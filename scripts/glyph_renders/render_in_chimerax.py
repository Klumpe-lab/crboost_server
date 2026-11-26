"""Render a particle for the /dev/glyphs ideas, inside ChimeraX (roadmap 77); `make_glyphs.py` turns the images into
glyphs.

    ChimeraX --nogui --offscreen --exit --script "render_in_chimerax.py SPEC_JSON"

A spec is a list of components, each blurred into a map and drawn in its own grey, then placed and rendered:

- `pdb`: a model (mmCIF), blurred with `molmap` at `resolution`; only some `chains` (or chain-id `prefixes`) if
  given, so one model can give two components in two greys. Drawn as instances of that one map (never copies of the
  atoms): its biological `assembly`, extended along its own helix (`helix`: `copies` of the whole model, the rise and
  twist measured from its subunits), or tiled as a hexagonal `lattice` across its thinnest axis (`spacing` Å, `rings`).
- `map`: a density map, binned and blurred (`bin`, `blur` Å), optionally doubled along an axis by a copy shifted by its
  lattice repeat (`tile` Å, `tile_axis`), the two joined by their maximum.
- `membrane`: a lipid bilayer (two leaflets of head groups, 40 Å apart): `patch` (a waved sheet, `radius`, `wave`
  amplitude and wavelength, a strip if its `aspect` is under 1), `sphere` (a vesicle or a virus's envelope,
  `radius`), or `belt` (a round sheet around the first component, where its hydrophobic band is, with a hole where
  the protein is).
- `spikes`: a model's map (its narrow end down) standing on a sphere (`radius`, `count`).

`orient` places the particle: `principal` (longest axis up, thinnest toward the camera), `parts` (the first component
left of the second), `membrane` (the first component's hydrophobic band level, its normal up), or `none`. A view is
ChimeraX commands run after that; `after` ones run after the view is fitted (a clip, which fitting would undo). The
styles: `soft` (ambient occlusion, white) and `line` (flat white with silhouette edges); transparent PNGs.
"""

import itertools
import json
import sys

import numpy as np
from chimerax.atomic import AtomicStructure
from chimerax.core.commands import run
from chimerax.geometry import Place, Places, rotation, translation
from chimerax.graphics import OrthographicCamera
from chimerax.map import Volume, volume_from_grid_data
from chimerax.map_data import ArrayGridData
from chimerax.std_commands.sym import pdb_assemblies

session = session  # noqa: F821 (ChimeraX provides it)
spec = json.loads(open(sys.argv[1]).read())
HYDROPHOBIC = {"ALA", "VAL", "LEU", "ILE", "PHE", "MET", "TRP", "CYS"}
CHARGED = {"ASP", "GLU", "LYS", "ARG"}


def newest(kind):
    """The model of a kind opened or made last (a structure's own sub-models come after it in the list)."""
    return [m for m in session.models if isinstance(m, kind)][-1]


def ids(models) -> str:
    return ",".join(m.id_string for m in models)


def _place(r: np.ndarray, c: np.ndarray) -> Place:
    if np.linalg.det(r) < 0:
        r[2] = -r[2]
    m = np.zeros((3, 4))
    m[:, :3] = r
    m[:, 3] = -r @ c
    return Place(matrix=m)


def principal_axes(points: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """(centroid, axes as rows: longest, middle, thinnest)."""
    c = points.mean(axis=0)
    _, v = np.linalg.eigh(np.cov((points - c).T))
    return c, np.array([v[:, 2], v[:, 1], v[:, 0]])


def principal_place(points: np.ndarray) -> Place:
    """The longest axis along y, the next along x, the thinnest along z (toward the camera)."""
    c, (e1, e2, e3) = principal_axes(points)
    return _place(np.array([e2, e1, e3]), c)


def parts_place(points: np.ndarray, a: np.ndarray, b: np.ndarray) -> Place:
    """Part a on the left, part b on the right (x from a's centroid to b's), y the longest axis across that."""
    c = points.mean(axis=0)
    x = b.mean(axis=0) - a.mean(axis=0)
    x /= np.linalg.norm(x)
    flat = (points - c) - np.outer((points - c) @ x, x)
    _, v = np.linalg.eigh(np.cov(flat.T))
    y = v[:, 2] - (v[:, 2] @ x) * x
    y /= np.linalg.norm(y)
    return _place(np.array([x, y, np.cross(x, y)]), c)


def hydrophobic_band(structures) -> tuple[np.ndarray, float]:
    """The membrane's normal and level for a membrane protein: the 30 Å slab, over directions on a hemisphere, with
    the most hydrophobic residues and the fewest charged ones (a small OPM)."""
    ca = [m.atoms[m.atoms.names == "CA"] for m in structures]
    xyz = np.concatenate([a.scene_coords for a in ca])
    names = np.concatenate([a.residues.names for a in ca])
    score = np.where(np.isin(names, list(HYDROPHOBIC)), 1.0, np.where(np.isin(names, list(CHARGED)), -2.0, -0.3))
    best = (-np.inf, None, 0.0)
    k = np.arange(600)
    phi, cz = k * np.pi * (3 - np.sqrt(5)), 1 - k / 600  # a hemisphere's spiral
    for n in np.stack([np.sqrt(1 - cz**2) * np.cos(phi), np.sqrt(1 - cz**2) * np.sin(phi), cz], axis=1):
        proj = xyz @ n
        order = np.argsort(proj)
        p, cum = proj[order], np.concatenate([[0], np.cumsum(score[order])])
        for c in np.arange(p[0] + 15, p[-1] - 15, 2.0):
            s = cum[np.searchsorted(p, c + 15)] - cum[np.searchsorted(p, c - 15)]
            if s > best[0]:
                best = (s, n, c)
    return best[1], best[2]


def bilayer(d: np.ndarray) -> np.ndarray:
    """Head-group density across a bilayer at signed distance d (Å) from its middle."""
    return np.exp(-((d - 20) ** 2) / (2 * 5.0**2)) + np.exp(-((d + 20) ** 2) / (2 * 5.0**2))


def grid_volume(name: str, lo: np.ndarray, hi: np.ndarray, step: float, fn) -> Volume:
    """A map in scene coordinates from fn(x, y, z) on a grid over [lo, hi]."""
    axes = [np.arange(lo[i], hi[i] + step, step) for i in range(3)]
    z, y, x = np.meshgrid(axes[2], axes[1], axes[0], indexing="ij")
    data = fn(x, y, z).astype(np.float32)
    g = ArrayGridData(data, origin=tuple(lo), step=(step, step, step), name=name)
    return volume_from_grid_data(g, session, style="surface", show_dialog=False)


_opened: dict = {}


def open_model(path: str):
    """A model opened once however many components draw parts of it (each component blurs its own chains)."""
    if path not in _opened:
        run(session, f"open {path}")
        _opened[path] = newest(AtomicStructure)
        run(session, f"hide #{_opened[path].id_string} models")
    return _opened[path]


def instance_places(comp: dict, m, xyz: np.ndarray) -> list:
    """Where the component's one blurred map is drawn: its biological assembly, extended along its own helix, or
    tiled as a hexagonal lattice; drawn as instances of one map, never as copies of the atoms."""
    places = [Place()]
    if comp.get("assembly"):  # the assembly's operators, from the mmCIF (the `sym` command would copy the atoms)
        asm = next(x for x in pdb_assemblies(m) if x.id == str(comp["assembly"]))
        places = [op for _chains, _expr, ops in asm.chain_ops for op in ops]  # one entry per op, or one with all
    pts = np.concatenate([p.transform_points(xyz) for p in places])
    if h := comp.get("helix"):  # the model's own subunits (its chains) give the rise and twist
        c, axes = principal_axes(pts)
        a = axes[0]
        subs = [ch for ch in set(m.residues.chain_ids) if (m.atoms.residues.chain_ids == ch).sum() > 500]
        cent = sorted((m.atoms[m.atoms.residues.chain_ids == ch].scene_coords.mean(axis=0) for ch in subs),
                      key=lambda q: (q - c) @ a)  # fmt: skip
        rise = np.mean([(b - q) @ a for q, b in itertools.pairwise(cent)])
        u, w = axes[1], axes[2]
        ang = [np.arctan2((q - c) @ w, (q - c) @ u) for q in cent]
        twist = np.degrees(np.mean([(b - q + np.pi) % (2 * np.pi) - np.pi for q, b in itertools.pairwise(ang)]))
        n = len(cent)
        places = [
            translation(a * rise * n * k) * rotation(a, twist * n * k, center=c) * p
            for k in range(h["copies"])
            for p in places
        ]
    if lat := comp.get("lattice"):
        c, axes = principal_axes(pts)
        u, w = axes[0], np.cross(axes[2], axes[0])
        s, n = lat["spacing"], lat.get("rings", 1)
        cells = [(i, j) for i in range(-n, n + 1) for j in range(-n, n + 1) if abs(i + j) <= n]
        places = [translation(s * (i * u + j * (0.5 * u + np.sqrt(3) / 2 * w))) * p for i, j in cells for p in places]
    return places


def spike_places(model, radius: float, count: int) -> list:
    """Places that stand a model (its narrow end down) on a sphere of `radius` round the origin."""
    xyz = model.atoms.scene_coords
    c, axes = principal_axes(xyz)
    a = axes[0]
    proj = (xyz - c) @ a
    lo, hi = proj < np.percentile(proj, 15), proj > np.percentile(proj, 85)

    def spread(sel):
        q = xyz[sel] - c
        return np.linalg.norm(q - np.outer(q @ a, a), axis=1).mean()

    if spread(hi) < spread(lo):
        a, proj = -a, -proj  # a points from the narrow end to the wide one
    base = proj.min()
    k = np.arange(count) + 0.5
    phi, cz = k * np.pi * (3 - np.sqrt(5)), 1 - 2 * k / count
    rng = np.random.default_rng(7)
    out = []
    for d in np.stack([np.sqrt(1 - cz**2) * np.cos(phi), np.sqrt(1 - cz**2) * np.sin(phi), cz], axis=1):
        ax = np.cross(a, d)
        ang = np.degrees(np.arccos(np.clip(a @ d, -1, 1)))
        turn = rotation(ax if np.linalg.norm(ax) > 1e-6 else axes[1], ang, center=c)
        spin = rotation(d, rng.uniform(0, 360), center=c)
        out.append(translation(d * (radius - base) - c) * spin * turn)
    return out


# ── Build the components: one blurred map each, drawn at its instance places ─────────────────────────────────
comps = spec["components"]
built = []  # (component, points in scene coordinates, volume or None)
for comp in comps:
    if "pdb" in comp or "spikes" in comp:
        src = comp.get("pdb") or comp["spikes"]["pdb"]
        m = open_model(src)
        chains = sorted(set(m.residues.chain_ids))
        keep = comp.get("chains") or [c for c in chains if any(c.startswith(p) for p in comp.get("prefixes", [""]))]
        sel = np.isin(m.atoms.residues.chain_ids, keep)
        res = comp.get("resolution") or comp["spikes"]["resolution"]
        run(session, f"molmap #{m.id_string}/{','.join(keep)} {res} gridSpacing {res / 3:.2f}")
        vol = newest(Volume)
        xyz = m.atoms.scene_coords[sel]
        if "spikes" in comp:
            places = spike_places(m, comp["spikes"]["radius"], comp["spikes"]["count"])
        else:
            places = instance_places(comp, m, m.atoms.scene_coords[sel])
        vol.positions = Places(places)
        built.append((comp, np.concatenate([p.transform_points(xyz) for p in places]), vol))
    elif "map" in comp:
        run(session, f"open {comp['map']}")
        raw = newest(Volume)
        if comp.get("tile"):
            run(session, f"open {comp['map']}")
            copy = newest(Volume)
            axis = comp.get("tile_axis", "x")
            run(session, f"move {axis} {comp['tile']} models #{copy.id_string} coordinateSystem #{copy.id_string}")
            run(session, f"volume maximum #{raw.id_string},{copy.id_string} boundingGrid true")
            joined = newest(Volume)
            run(session, f"close #{raw.id_string},{copy.id_string}")
            raw = joined
        run(session, f"volume bin #{raw.id_string} binSize {comp.get('bin', 2)}")
        binned = newest(Volume)
        run(session, f"volume gaussian #{binned.id_string} sDev {comp['blur']}")
        vol = newest(Volume)
        run(session, f"close #{raw.id_string},{binned.id_string}")
        mat = vol.full_matrix()
        ijk = np.argwhere(mat > mat.mean() + comp["sd"] * mat.std())[:, ::-1].astype(float)
        built.append((comp, vol.data.ijk_to_xyz_transform.transform_points(ijk), vol))
    else:
        built.append((comp, np.zeros((0, 3)), None))  # a membrane: made once the particle is placed

# ── Place it ─────────────────────────────────────────────────────────────────────────────────────────────────
orient = spec.get("orient", "principal")
band_y = 0.0
if orient == "none":
    place = Place()
elif orient == "membrane":
    comp0 = built[0][0]
    m0 = open_model(comp0["pdb"])
    normal, level = hydrophobic_band([m0])
    xyz = built[0][1]
    c = xyz.mean(axis=0)
    flat = (xyz - c) - np.outer((xyz - c) @ normal, normal)
    _, v = np.linalg.eigh(np.cov(flat.T))
    x = v[:, 2] - (v[:, 2] @ normal) * normal
    x /= np.linalg.norm(x)
    place = _place(np.array([x, normal, np.cross(x, normal)]), c)
    band_y = level - normal @ c
elif orient == "parts":
    place = parts_place(np.concatenate([b[1] for b in built[:2]]), built[0][1], built[1][1])
else:
    place = principal_place(np.concatenate([b[1] for b in built if len(b[1])]))
for _comp, _pts, vol in built:
    if vol is not None:
        vol.positions = Places([place * p for p in vol.positions])


def set_level(comp: dict, vol: Volume) -> None:
    level = f"level {comp['level']}" if "level" in comp else f"sdLevel {comp.get('sd', 1.2)}"
    run(session, f"volume #{vol.id_string} {level} color {comp.get('color', 'white')} style surface capFaces false")


for comp, _structs, vol in built:
    if vol is not None:
        set_level(comp, vol)


# ── Membranes, in the placed frame ───────────────────────────────────────────────────────────────────────────
def membrane(comp: dict, protein: Volume | None, band_y: float) -> Volume:
    step, r = comp.get("step", 4.0), comp.get("radius", 200)
    kind = comp["membrane"]
    if kind == "sphere":
        lo, hi = np.full(3, -r - 40.0), np.full(3, r + 40.0)
        return grid_volume("membrane", lo, hi, step, lambda x, y, z: bilayer(np.sqrt(x**2 + y**2 + z**2) - r))
    if kind == "patch":
        amp, wave = comp.get("wave", (22.0, 420.0))

        depth = r * comp.get("aspect", 1.0)  # a strip when the aspect is under 1

        def patch(x, y, z):
            mid = amp * np.sin(2 * np.pi * x / wave) * np.cos(np.pi * z / wave)
            return bilayer(y - mid) * ((x / r) ** 2 + (z / depth) ** 2 < 1)

        h = amp + 45.0  # the crest and its outer leaflet stay on the grid
        return grid_volume("membrane", np.array([-r - 8, -h, -r - 8]), np.array([r + 8, h, r + 8]), step, patch)
    # belt: round the first component at its hydrophobic band, with a hole where the protein is
    thr = protein.surfaces[0].level

    def belt(x, y, z):
        pts = np.stack([x.ravel(), y.ravel(), z.ravel()], axis=1)
        dens = protein.interpolated_values(pts, point_xform=Place()).reshape(x.shape)  # scene points
        hole = np.clip(dens / (0.5 * thr), 0, 1)
        return bilayer(y - band_y) * (np.sqrt(x**2 + z**2) < r) * (1 - hole)

    return grid_volume("membrane", np.array([-r, band_y - 40, -r]), np.array([r, band_y + 40, r]), step, belt)


for i, (comp, _structs, _vol) in enumerate(built):
    if comp.get("membrane"):
        v = membrane(comp, built[0][2], band_y)
        set_level({"level": 0.5, **comp}, v)
        built[i] = (comp, [], v)

# ── Camera and views ─────────────────────────────────────────────────────────────────────────────────────────
centre = ids(v for _c, _s, v in built)
# Orthographic: `camera ortho` fails offscreen (it asks the context about stereo), so set the camera directly.
cam = OrthographicCamera()
cam.position = session.main_view.camera.position
session.main_view.camera = cam
run(session, "material dull")
for view in spec["views"]:
    run(session, "view orient")
    for cmd in view.get("commands", []):  # `#VOL` names the particle's maps
        run(session, cmd.replace("#VOL", f"#{centre}"))
    run(session, "view; view pad 0.06")
    for cmd in view.get("after", []):  # after `view`, which would undo a clip
        run(session, cmd.replace("#VOL", f"#{centre}"))
    for style in view.get("styles", ["soft", "line"]):
        if style == "soft":
            run(session, "lighting soft; lighting shadows false; graphics silhouettes false")
        else:
            run(session, "lighting flat; graphics silhouettes true width 3 color black depthJump 0.015")
        out = f"{spec['out']}/{spec['name']}_{view['name']}_{style}.png"
        px = spec.get("px", 600)
        run(session, f"save {out} width {px} height {px} supersample 3 transparentBackground true")
    run(session, "clip off")

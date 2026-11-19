"""What the registry prototypes show, read from real projects and never written.

`registry` reads one project: every species with its identity, templates and masks (their headers, sizes and small
pictures), its tomograms joined from what Picks & curation and Analytics read today (lists per tomogram, top σ, the
σ ladder, markers), and its particle jobs with their drift from the species. `overlay` takes one reconstruction of
the project with its candidates from every species, for judging the overlay colours on a real slice.

Both are module-level functions with plain arguments and plain results, so they run in a worker process
(`services.workers.run_in_worker`); pictures come back as PNG data URLs.
"""

from __future__ import annotations

import base64
import io
import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# key -> (project folder, what it shows)
PROJECTS: dict[str, tuple[str, str]] = {
    "grid2": (
        "/groups/klumpe/_crboost_data_api/AGG412_20260311_MyrGFP_Grid2_rerun",
        "48 tomograms with one candidates list each, two species",
    ),
    "sta": (
        "/groups/klumpe/_crboost_data_api/AGG412_STA",
        "the STA home project: three 412 templates, Class 3D jobs whose symmetry differs from the species",
    ),
    "mystery": (
        "/groups/klumpe/crboost_data/MysteryLattice",
        "one tomogram holding six lists (candidates and five hand-picked), no templates",
    ),
}

# Overlay palettes, slot for slot in the order of `SPECIES_OVERLAY_COLORS` (red, cyan, yellow, magenta, green, blue,
# orange, pink), so a species' colour maps by its slot. "today" is that palette; the others keep strong hues but
# come down from neon, after instrument panels: (title, what it is after, colours).
PALETTES: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "today": (
        "Today",
        "the saturated primaries the overlays use now",
        ("#ff1744", "#00e5ff", "#ffea00", "#d500f9", "#76ff03", "#2979ff", "#ff9100", "#f50057"),
    ),
    "cockpit": (
        "Cockpit",
        "a glass cockpit's display: magenta course, cyan, green, amber, warning red",
        ("#e8504a", "#2cc8dc", "#e3cf3e", "#d65ad6", "#4cc96a", "#4f8ff0", "#f0a030", "#ef7aa6"),
    ),
    "reactor": (
        "Reactor",
        "a control room's annunciators and mimic board, Cherenkov blue, the trefoil's magenta",
        ("#d8453c", "#46b8c6", "#e6b43c", "#b4509f", "#44a86a", "#3d9cf0", "#e2762e", "#d8708c"),
    ),
    "terminal": (
        "Terminal",
        "a terminal's ANSI accents, Solarized-like, the yellow and green lifted to read on grey",
        ("#dc322f", "#2aa198", "#d1a10c", "#d33682", "#93b316", "#268bd2", "#cb4b16", "#7c80d4"),
    ),
    "industrial": (
        "Industrial",
        "RAL signal and traffic colours: traffic red, turquoise blue, signal yellow, telemagenta, traffic green",
        ("#d4302a", "#1f9aa0", "#f2b100", "#c03f80", "#2f9a5a", "#2b7cc0", "#e45a1d", "#c96fa5"),
    ),
}


def slot(color: str) -> int:
    """A colour's slot in the overlay palette, -1 when it is not one of them."""
    today = PALETTES["today"][2]
    return today.index(color.lower()) if color.lower() in today else -1


def in_palette(color: str, palette: str) -> str:
    """A species' colour in another palette: the same slot, or the colour itself when it has none."""
    i = slot(color)
    return PALETTES[palette][2][i] if i >= 0 else color


@dataclass(frozen=True)
class Vol:
    """A template or a mask, as one table row shows it."""

    id: str
    kind: str  # "template" | "mask"
    path: str
    polarity: str  # templates: "black" | "white"
    method: str  # masks: how it was made
    knobs: str  # masks: the relion_mask_create values
    source: str  # templates: where it came from; masks: the template it was made from
    parent: str  # masks: the template id it was made from, "" when unknown
    apix: float | None
    box: int | None
    lowpass: float | None
    dmin: float | None
    dmax: float | None
    rms: float | None
    size: int | None  # bytes
    selected: bool
    pics: tuple[str, ...]  # projections along z, y, x as PNG data URLs; () when unreadable


@dataclass(frozen=True)
class PickList:
    slug: str
    label: str
    list_type: str  # auto | manual | imported | merged
    source_kind: str  # tm | artiax | import | merge | ""
    count: int
    kept: int | None
    state: str  # ListExtractionState value or "n/a"
    origin: str  # the template file the picks came from, "" when not recorded
    job_status: str  # the per-list extraction job's status, "" when never submitted
    star_path: str


@dataclass(frozen=True)
class Tomo:
    name: str
    short: str
    lists: tuple[PickList, ...]
    picks: int
    kept: int
    top: float | None
    ge10: int | None
    last: float | None
    extracted: str  # "", or "n_out/n_in" from the subtomo record
    markers: tuple[tuple[str, str], ...]  # (label, rule text)
    has_geometry: bool
    n_saves: int
    details: str  # the name's hover


@dataclass(frozen=True)
class Job:
    iid: str
    kind: str
    number: str
    status: str
    drift: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Species:
    id: str
    name: str
    color: str
    origin: str
    catalog: str
    created: str
    diameter: float | None
    symmetry: str
    notes: str
    extraction: tuple[int, float, int] | None
    n_picks: int
    n_kept: int
    n_tomos_with_picks: int
    n_extracted_lists: int
    source_label: str  # where the scores come from ("Pick candidates job011 ← Template Match job009")
    templates: tuple[Vol, ...]
    masks: tuple[Vol, ...]
    tomos: tuple[Tomo, ...]
    jobs: tuple[Job, ...]
    n_sets: int


@dataclass(frozen=True)
class Registry:
    key: str
    name: str
    path: str
    species: tuple[Species, ...]


@dataclass(frozen=True)
class Layer:
    """One species' picks on the picture."""

    species: str
    vivid: str  # its overlay colour
    index: int  # its slot in the palettes (`slot`), -1 when it has none
    dots: tuple[tuple[float, float], ...]  # x, y in % of the picture
    diameter: float | None  # Å, the species' own


@dataclass(frozen=True)
class Overlay:
    tomo: str
    short: str
    url: str
    width: int
    height: int
    layers: tuple[Layer, ...]
    apix: float  # of the reconstruction
    nx: int  # its width in voxels


# ── pictures ─────────────────────────────────────────────────────────────────


def _png(img: np.ndarray, px: int) -> str:
    from PIL import Image

    lo, hi = (float(x) for x in np.percentile(img, (0.5, 99.5)))
    u8 = (np.clip((img - lo) / (hi - lo if hi > lo else 1.0), 0, 1) * 255).astype(np.uint8)
    pic = Image.fromarray(u8, mode="L").resize((px, px), Image.LANCZOS)
    buf = io.BytesIO()
    pic.save(buf, "PNG", optimize=True)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()


def _projections(path: str, polarity: str, px: int = 112) -> tuple[str, ...]:
    """The volume summed along z, y and x, density white whatever the file's polarity."""
    import mrcfile

    try:
        with mrcfile.mmap(path, mode="r", permissive=True) as m:
            v = np.asarray(m.data, dtype=np.float32)
    except (OSError, ValueError) as e:
        logger.warning("Could not read %s: %s", path, e)
        return ()
    if v.ndim != 3:
        return ()
    if polarity == "black":
        v = -v
    return tuple(_png(v.sum(axis=a), px) for a in (0, 1, 2))


# ── one project ──────────────────────────────────────────────────────────────


def _vol_template(t, selected: str) -> Vol:
    from services.templating.template_metadata import read_template_header

    h = read_template_header(t.template_path)
    size = Path(t.template_path).stat().st_size if Path(t.template_path).exists() else None
    src = t.source or ""
    if src == "imported" and t.imported_from and Path(t.imported_from).name != Path(t.template_path).name:
        src = f"imported from {t.imported_from}"
    return Vol(
        id=t.id,
        kind="template",
        path=t.template_path,
        polarity=t.polarity,
        method="",
        knobs="",
        source=src,
        parent="",
        apix=h.apix_ang,
        box=h.box_px,
        lowpass=t.lowpass_resolution_ang,
        dmin=h.dmin,
        dmax=h.dmax,
        rms=h.rms,
        size=size,
        selected=t.id == selected,
        pics=_projections(t.template_path, t.polarity),
    )


def _vol_mask(m, selected: str, templates: dict[str, str]) -> Vol:
    from services.templating.template_metadata import read_template_header

    h = read_template_header(m.mask_path)
    size = Path(m.mask_path).stat().st_size if Path(m.mask_path).exists() else None
    knobs = " · ".join(
        f"{k} {v:g}"
        for k, v in (
            ("thr", m.threshold),
            ("ext", m.extend_pixels),
            ("soft", m.soft_edge_pixels),
            ("lp", m.lowpass_ang),
        )
        if v is not None
    )
    parent = m.derived_from_template_id or ""
    if parent:
        source = (
            f"from {Path(templates[parent]).name}" if parent in templates else "from a template no longer registered"
        )
    else:
        source = m.notes or ""
    return Vol(
        id=m.id,
        kind="mask",
        path=m.mask_path,
        polarity="white",
        method=m.method or "",
        knobs=knobs,
        source=source,
        parent=parent if parent in templates else "",
        apix=h.apix_ang,
        box=h.box_px,
        lowpass=None,
        dmin=h.dmin,
        dmax=h.dmax,
        rms=h.rms,
        size=size,
        selected=m.id == selected,
        pics=_projections(m.mask_path, "white"),
    )


def _tomos(state, project: Path, species_id: str) -> tuple[tuple[Tomo, ...], dict]:
    from services.array_tasks import ts_display_name, ts_position_sort_key
    from services.particles.sigma import LADDER_SIGMAS
    from ui.analytics.tomogram_table import MARKER_LABELS
    from ui.species import analytics_tab, picks_tab

    picks = picks_tab._compute(state, project, species_id)
    stats = analytics_tab._compute(state, project, species_id, None)
    by_tomo: dict[str, list] = {}
    for r in picks.overview.rows if picks.overview else ():
        by_tomo.setdefault(r.tomo_name, []).append(r)
    rows = {r["tomo"]: r for r in stats.rows}
    marks: dict[str, list[tuple[str, str]]] = {}
    for m in stats.markers:
        marks.setdefault(m.tomo, []).append((MARKER_LABELS.get(m.key, m.key), m.text))
    i10 = LADDER_SIGMAS.index(10.0) if 10.0 in LADDER_SIGMAS else None

    infos = {t.tomo_name: t for t in picks.tomos}
    names = sorted(set(infos) | set(by_tomo), key=ts_position_sort_key)
    out = []
    for name in names:
        lists = tuple(
            PickList(
                slug=r.slug,
                label=r.label,
                list_type=r.list_type,
                source_kind=r.source_kind,
                count=r.count,
                kept=r.kept,
                state=r.extraction_state,
                origin=Path(r.template_path).name if r.template_path else "",
                job_status=(r.extract_job.status if r.extract_job else ""),
                star_path=r.star_path or "",
            )
            for r in by_tomo.get(name, [])
        )
        a = rows.get(name) or {}
        c = a.get("cand") or {}
        ladder = c.get("ladder") or []
        sub = a.get("sub") or {}
        details = [name]
        if a.get("n_tilts"):
            details.append(f"{a['n_tilts']} tilts")
        if a.get("runtime_min"):
            details.append(f"matched in {a['runtime_min']:.0f} min on {(a.get('node') or '').split('.', 1)[0]}")
        info = infos.get(name)
        out.append(
            Tomo(
                name=name,
                short=ts_display_name(name),
                lists=lists,
                picks=sum(pl.count for pl in lists),
                kept=sum(pl.kept if pl.kept is not None else pl.count for pl in lists),
                top=a.get("top_sigma"),
                ge10=int(ladder[i10]) if c and i10 is not None and i10 < len(ladder) else None,
                last=c.get("last_sigma"),
                extracted=(f"{sub['n_out']}/{sub['n_in']}" if sub.get("n_in") is not None else str(sub["n_out"]))
                if sub
                else "",
                markers=tuple(marks.get(name, ())),
                has_geometry=bool(info.has_geometry) if info else False,
                n_saves=info.n_saves if info else 0,
                details=" · ".join(details),
            )
        )
    meta = {"source": stats.source.label if stats.source else "", "sets": len(stats.sets)}
    return tuple(out), meta


def _jobs(state, sp) -> tuple[Job, ...]:
    from services.jobs.spec import display_name
    from services.particles.species_jobs import jobs_for_species
    from ui.species.jobs_tab import _drift

    out = []
    for iid, jm in jobs_for_species(state, sp.id):
        name = getattr(jm, "relion_job_name", "") or ""
        out.append(
            Job(
                iid=iid,
                kind=display_name(jm.job_type),
                number=name.rstrip("/").split("/")[-1] if name else "",
                status=str(getattr(jm.execution_status, "value", jm.execution_status) or ""),
                drift=tuple(_drift(sp, jm)),
            )
        )
    return tuple(out)


def registry(key: str) -> Registry:
    """One project's registry, read once."""
    from services.particles.species_overview import species_overview
    from services.project_state import ProjectState

    folder = Path(PROJECTS[key][0])
    state = ProjectState.load(folder / "project_params.json", read_only=True)
    species = []
    for sp in state.species_registry:
        ov = species_overview(state, folder, sp.id)
        tomos, meta = _tomos(state, folder, sp.id)
        templates = {t.id: t.template_path for t in sp.templates}
        ep = sp.extraction_params
        species.append(
            Species(
                id=sp.id,
                name=sp.name,
                color=sp.color,
                origin=sp.origin or "workbench",
                catalog=f"{sp.catalog_id} v{sp.catalog_version}" if sp.catalog_id else "",
                created=sp.created_at.strftime("%Y-%m-%d %H:%M") if sp.created_at else "",
                diameter=sp.diameter_ang,
                symmetry=sp.symmetry or "C1",
                notes=sp.notes or "",
                extraction=(ep.box_size, ep.binning, ep.crop_size) if ep else None,
                n_picks=ov.n_picks,
                n_kept=ov.n_kept,
                n_tomos_with_picks=ov.n_tomos_with_picks,
                n_extracted_lists=ov.n_extracted_lists,
                source_label=meta["source"],
                templates=tuple(_vol_template(t, sp.selected_template_id) for t in sp.templates),
                masks=tuple(_vol_mask(m, sp.selected_mask_id, templates) for m in sp.masks),
                tomos=tomos,
                jobs=_jobs(state, sp),
                n_sets=meta["sets"],
            )
        )
    return Registry(key=key, name=folder.name, path=str(folder), species=tuple(species))


# ── the overlay picture ──────────────────────────────────────────────────────


def overlay(key: str, near: int | None = None, width: int = 560) -> Overlay | None:
    """One reconstruction as WarpTools drew its central slice, with every species' candidates on it: the one with the
    most picks of every species, or with `near`, the one whose picks of all species together come closest to that
    many. Dots follow the tomogram wall's convention (`ui/tomo_gallery._normalized_dots`: x right, y up). None when
    the project has no WarpTools picture or no candidates."""
    import mrcfile
    from PIL import Image

    from services.array_tasks import ts_display_name
    from services.particles.species_overview import species_overview
    from services.project_state import ProjectState

    folder = Path(PROJECTS[key][0])
    state = ProjectState.load(folder / "project_params.json", read_only=True)
    per_species: list[tuple[str, str, float | None, dict[str, np.ndarray]]] = []
    for sp in state.species_registry:
        ov = species_overview(state, folder, sp.id)
        star = next((r.star_path for r in ov.rows if r.list_type == "auto" and r.star_path), None)
        if not star or not Path(star).exists():
            continue
        import starfile

        df = starfile.read(star)
        cols = ("rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnTomoName")
        if not all(c in df.columns for c in cols):
            continue
        groups = {
            str(t): g[["rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst"]].to_numpy(float)
            for t, g in df.groupby("rlnTomoName")
        }
        per_species.append((sp.name, sp.color, sp.diameter_ang, groups))
    if not per_species:
        return None
    recon = sorted(folder.glob("External/*/warp_tiltseries/reconstruction/*.png"))
    by_name = {p.name.rsplit("_", 1)[0]: p for p in recon}

    def total(t: str) -> int:
        return sum(len(g.get(t, ())) for *_x, g in per_species)

    if near is None:
        best = max(by_name, key=lambda t: (min(len(g.get(t, ())) for *_x, g in per_species), total(t)), default=None)
    else:
        best = min(by_name, key=lambda t: abs(total(t) - near), default=None)
    if best is None:
        return None
    png = by_name[best]
    mrc = png.with_suffix(".mrc")
    with mrcfile.open(mrc, header_only=True, permissive=True) as m:
        nx, ny, apix = int(m.header.nx), int(m.header.ny), float(m.voxel_size.x)
    layers = []
    for name, color, diameter, groups in per_species:
        xy = groups.get(best)
        if xy is None or not len(xy):
            continue
        fx = np.clip((xy[:, 0] / apix + nx / 2) / nx, 0, 1) * 100
        fy = (1 - np.clip((xy[:, 1] / apix + ny / 2) / ny, 0, 1)) * 100
        dots = tuple((round(float(a), 2), round(float(b), 2)) for a, b in zip(fx, fy, strict=True))
        layers.append(Layer(name, color, slot(color), dots, diameter))
    pic = Image.open(png).convert("L")
    h = round(width * pic.size[1] / pic.size[0])
    pic = pic.resize((width, h), Image.LANCZOS)
    buf = io.BytesIO()
    pic.save(buf, "PNG", optimize=True)
    url = "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()
    return Overlay(best, ts_display_name(best), url, width, h, tuple(layers), apix, nx)

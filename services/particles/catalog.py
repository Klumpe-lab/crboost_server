"""Lab-level species catalog (roadmap 12) — cross-project species DEFINITIONS.

Two tiers, and the split is the whole design. A **definition** is cross-project by nature:
name, diameter, symmetry, notes, colour, templates and masks with their provenance. Everything
else a species accumulates — picks, keep/drop filters, merges, extractions — is project-bound,
because coordinates are relative to a project's tomograms and RELION-compat wants project-local
files. Cross-project reuse of *results* is a different feature that already exists (the
aggregation merge card).

Layout under ``species_catalog_root``::

    <root>/catalog.json                     index, regenerated on every write
    <root>/<catalog_id>/v1/species.json     one version, never edited in place
    <root>/<catalog_id>/v1/templates/*.mrc  (+ their .meta.json sidecars)
    <root>/<catalog_id>/v1/masks/*.mrc      (+ their .meta.json sidecars)
    <root>/<catalog_id>/v2/…                a later publish; v1 stays exactly as it was

**Snapshot semantics, both directions.** Import COPIES the definition and its files into the
project (``templates/<sid>/``) and records ``catalog_id`` + ``catalog_version`` as a backlink;
publish COPIES a project species up as a NEW version directory. Neither one syncs afterwards —
same rule as species values copied onto a job. A published version is immutable: republishing
writes v(N+1), so a project that imported v1 keeps describing exactly what it used.

Sidecars travel with the files, so a template's UUID is stable across every project that
imports it — which is also what keeps ``selected_template_id`` resolvable after an import.

The feature is OFF when ``species_catalog_root`` is unset: ``catalog_root()`` returns None and
every caller renders nothing. It is not an error state and must never be reported as one.
"""

from __future__ import annotations

import getpass
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from services.configs.config_service import get_config_service
from services.models_base import SpeciesOrigin
from services.project_state import ParticleSpecies, ParticleTemplate, TemplateMask, sidecar_ensure, slugify

logger = logging.getLogger(__name__)

INDEX_NAME = "catalog.json"
SPECIES_FILE = "species.json"
TEMPLATES_DIR = "templates"
MASKS_DIR = "masks"


class CatalogSpecies(BaseModel):
    """One published version of one catalog species.

    ``extra="forbid"`` on purpose: this file is written by one crboost version and read by
    another, possibly older, one. A key nobody understands is a fact worth failing on rather
    than dropping silently — the read path turns it into a load warning naming the file.

    ``templates`` / ``masks`` reuse the project models verbatim; their ``*_path`` field holds
    a BARE FILENAME relative to this version's ``templates/`` / ``masks/`` directory rather
    than an absolute path. Reusing the models means the catalog cannot drift from what a
    species actually is — there is no parallel schema to keep in step.
    """

    model_config = ConfigDict(extra="forbid")

    catalog_id: str
    name: str
    version: int
    color: str = "#3b82f6"
    diameter_ang: float | None = None
    symmetry: str = "C1"
    notes: str = ""
    templates: list[ParticleTemplate] = Field(default_factory=list)
    masks: list[TemplateMask] = Field(default_factory=list)
    selected_template_id: str = ""
    selected_mask_id: str = ""
    published_by: str = ""
    published_at: datetime = Field(default_factory=datetime.now)
    source_project: str = ""  # the project this version was published from, for provenance


class CatalogEntry(BaseModel):
    """One row of the index — enough to render the picker without opening every version."""

    catalog_id: str
    name: str
    latest_version: int
    n_templates: int
    n_masks: int
    diameter_ang: float | None = None
    symmetry: str = "C1"
    published_by: str = ""
    published_at: datetime | None = None


# ── Root + layout ─────────────────────────────────────────────────────────────


def catalog_root() -> Path | None:
    """The configured catalog directory, or None when the feature is off."""
    return get_config_service().species_catalog_root


def is_enabled() -> bool:
    """Whether ANY catalog affordance should render. The one check every caller makes."""
    return catalog_root() is not None


def _version_dirs(species_dir: Path) -> list[int]:
    """Version numbers present under one catalog id, ascending."""
    out: list[int] = []
    for child in species_dir.iterdir() if species_dir.is_dir() else []:
        if child.is_dir() and child.name.startswith("v") and child.name[1:].isdigit():
            out.append(int(child.name[1:]))
    return sorted(out)


def version_dir(catalog_id: str, version: int) -> Path | None:
    root = catalog_root()
    return root / catalog_id / f"v{version}" if root else None


# ── Read ──────────────────────────────────────────────────────────────────────


def read_species(catalog_id: str, version: int | None = None) -> CatalogSpecies:
    """One catalog species at ``version`` (default: the newest). Raises FileNotFoundError
    when the catalog is off, the id is unknown or that version was never published, and
    ValueError when species.json does not parse — all four are things the user must see,
    not silently-empty results. Blocking I/O: call off the event loop."""
    root = catalog_root()
    if root is None:
        raise FileNotFoundError("no species catalog is configured (species_catalog_root)")
    species_dir = root / catalog_id
    versions = _version_dirs(species_dir)
    if not versions:
        raise FileNotFoundError(f"catalog species '{catalog_id}' has no published versions under {species_dir}")
    v = versions[-1] if version is None else int(version)
    if v not in versions:
        raise FileNotFoundError(f"catalog species '{catalog_id}' has no v{v} (have: {versions})")
    path = species_dir / f"v{v}" / SPECIES_FILE
    try:
        return CatalogSpecies(**json.loads(path.read_text()))
    except (OSError, ValueError) as e:
        raise ValueError(f"{path}: {e}") from e


def list_catalog() -> list[CatalogEntry]:
    """Every catalog species, newest version each, name-sorted. Rebuilds from the version
    directories rather than trusting ``catalog.json`` — the index is a convenience for other
    readers, and a stale one must never hide a species somebody published. Returns [] when
    the feature is off or the root does not exist yet. Blocking I/O: call off the loop."""
    root = catalog_root()
    if root is None or not root.is_dir():
        return []
    entries: list[CatalogEntry] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        versions = _version_dirs(child)
        if not versions:
            continue
        try:
            sp = read_species(child.name, versions[-1])
        except (OSError, ValueError) as e:
            # Reported, not swallowed: one unreadable entry must not empty the picker.
            logger.warning("catalog: skipping %s — %s", child.name, e)
            continue
        entries.append(
            CatalogEntry(
                catalog_id=sp.catalog_id,
                name=sp.name,
                latest_version=sp.version,
                n_templates=len(sp.templates),
                n_masks=len(sp.masks),
                diameter_ang=sp.diameter_ang,
                symmetry=sp.symmetry,
                published_by=sp.published_by,
                published_at=sp.published_at,
            )
        )
    entries.sort(key=lambda e: e.name.lower())
    return entries


def write_index() -> Path | None:
    """Regenerate ``catalog.json`` from the version directories. Best-effort by design: the
    index is derived data (``list_catalog`` never reads it), so a read-only root costs a log
    line, not a failed publish."""
    root = catalog_root()
    if root is None or not root.is_dir():
        return None
    path = root / INDEX_NAME
    try:
        payload = {
            "generated_at": datetime.now().isoformat(),
            "species": [e.model_dump(mode="json") for e in list_catalog()],
        }
        path.write_text(json.dumps(payload, indent=2))
        return path
    except OSError as e:
        logger.warning("catalog: could not write index %s: %s", path, e)
        return None


# ── Publish (project → catalog) ───────────────────────────────────────────────


def _copy_with_sidecar(src: Path, dst_dir: Path) -> str:
    """Copy one file plus its ``.meta.json`` sidecar; returns the destination filename.
    The sidecar carries the UUID, so copying it is what keeps a template's identity stable
    across every project that imports this version."""
    dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst_dir / src.name)
    sidecar = src.with_name(src.name + ".meta.json")
    if sidecar.exists():
        shutil.copy2(sidecar, dst_dir / sidecar.name)
    return src.name


def publish_species(state, project_path: Path, species_id: str) -> CatalogSpecies:
    """Copy one project species up as a NEW catalog version and return what was written.

    Never overwrites: the version number is max(existing) + 1, so anything already published
    stays byte-identical and a project that imported v1 keeps describing what it used. The
    project species gets ``catalog_id`` / ``catalog_version`` written back as a backlink —
    the caller persists. Raises when the catalog is off or a registered file is missing on
    disk (publishing a definition whose template does not exist would produce an entry that
    cannot be imported). Blocking I/O: call off the event loop."""
    root = catalog_root()
    if root is None:
        raise FileNotFoundError("no species catalog is configured (species_catalog_root)")
    sp: ParticleSpecies | None = state.get_species(species_id)
    if sp is None:
        raise FileNotFoundError(f"no species '{species_id}' in this project")

    catalog_id = sp.catalog_id or _mint_catalog_id(root, sp.name)
    species_dir = root / catalog_id
    version = (_version_dirs(species_dir)[-1] if _version_dirs(species_dir) else 0) + 1
    vdir = species_dir / f"v{version}"
    if vdir.exists():
        raise FileExistsError(f"{vdir} already exists — refusing to overwrite a published version")

    templates: list[ParticleTemplate] = []
    for t in sp.templates:
        src = Path(t.template_path)
        if not src.is_file():
            raise FileNotFoundError(f"template file missing, cannot publish: {src}")
        name = _copy_with_sidecar(src, vdir / TEMPLATES_DIR)
        templates.append(t.model_copy(update={"template_path": name}))
    masks: list[TemplateMask] = []
    for m in sp.masks:
        src = Path(m.mask_path)
        if not src.is_file():
            raise FileNotFoundError(f"mask file missing, cannot publish: {src}")
        name = _copy_with_sidecar(src, vdir / MASKS_DIR)
        masks.append(m.model_copy(update={"mask_path": name}))

    record = CatalogSpecies(
        catalog_id=catalog_id,
        name=sp.name,
        version=version,
        color=sp.color,
        diameter_ang=sp.diameter_ang,
        symmetry=sp.symmetry,
        notes=sp.notes,
        templates=templates,
        masks=masks,
        selected_template_id=sp.selected_template_id,
        selected_mask_id=sp.selected_mask_id,
        published_by=getpass.getuser(),
        source_project=Path(project_path).name,
    )
    vdir.mkdir(parents=True, exist_ok=True)
    (vdir / SPECIES_FILE).write_text(record.model_dump_json(indent=2))
    sp.catalog_id = catalog_id
    sp.catalog_version = version
    write_index()
    logger.info("catalog: published %s v%d from %s", catalog_id, version, project_path)
    return record


def _mint_catalog_id(root: Path, name: str) -> str:
    """A fresh catalog id from a display name, unique across the whole catalog."""
    base = slugify(name)
    taken = {c.name for c in root.iterdir()} if root.is_dir() else set()
    cid, i = base, 2
    while cid in taken:
        cid = f"{base}_{i}"
        i += 1
    return cid


# ── Import (catalog → project) ────────────────────────────────────────────────


def import_species(state, project_path: Path, catalog_id: str, version: int | None = None) -> ParticleSpecies:
    """Instantiate a catalog species INTO this project and return the new registry entry.

    Copies the definition and its template / mask files into ``templates/<sid>/`` (with
    sidecars, so the UUIDs the version recorded keep resolving) and records
    ``catalog_id`` / ``catalog_version``. The project id is minted locally by
    ``add_species`` — two projects can hold the same catalog species under different local
    ids, and the catalog id is what ties them together downstream. The caller persists.
    Blocking I/O: call off the event loop."""
    record = read_species(catalog_id, version)
    vdir = version_dir(record.catalog_id, record.version)
    if vdir is None:
        raise FileNotFoundError("no species catalog is configured (species_catalog_root)")

    species = state.add_species(record.name, origin=SpeciesOrigin.IMPORTED, color=record.color)
    species.catalog_id = record.catalog_id
    species.catalog_version = record.version
    species.diameter_ang = record.diameter_ang
    species.symmetry = record.symmetry
    species.notes = record.notes

    dest = Path(project_path) / "templates" / species.id
    dest.mkdir(parents=True, exist_ok=True)
    for t in record.templates:
        src = vdir / TEMPLATES_DIR / t.template_path
        local = _copy_with_sidecar(src, dest)
        abs_path = str(dest / local)
        sidecar_ensure(abs_path, "template")
        species.templates.append(t.model_copy(update={"template_path": abs_path, "imported_from": str(src)}))
    for m in record.masks:
        src = vdir / MASKS_DIR / m.mask_path
        local = _copy_with_sidecar(src, dest)
        abs_path = str(dest / local)
        sidecar_ensure(abs_path, "mask")
        species.masks.append(m.model_copy(update={"mask_path": abs_path, "imported_from": str(src)}))

    # The recorded selections only resolve because the sidecars travelled with the files.
    # Fall back to the first of each rather than leaving a dangling id.
    ids_t = {t.id for t in species.templates}
    ids_m = {m.id for m in species.masks}
    species.selected_template_id = (
        record.selected_template_id
        if record.selected_template_id in ids_t
        else (species.templates[0].id if species.templates else "")
    )
    species.selected_mask_id = (
        record.selected_mask_id if record.selected_mask_id in ids_m else (species.masks[0].id if species.masks else "")
    )
    state.mark_dirty()
    state.bump_registry_rev()
    logger.info("catalog: imported %s v%d into %s as '%s'", record.catalog_id, record.version, project_path, species.id)
    return species

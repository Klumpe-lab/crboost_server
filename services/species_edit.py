"""Making and editing a particle species: one door for the species dialogs, the Particles registry's identity
editor and the template workbench, and for the agent API.

A species is a name, a colour, a diameter, a symmetry and notes, an optional extraction geometry, and the
templates and masks registered on it (`species_admin`). The workbench's generators make templates and masks in
two steps: a plan settles every value (or names what is open), then the make writes the files and registers them.
These doors mark the state dirty through `mutate_species`; the caller saves.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services import species_admin, tomogram_import
from services.jobs._base import SymmetryGroup
from services.models_base import JobType, SpeciesOrigin
from services.project_state import ExtractionParams, ProjectState, TemplateMask
from services.result import ErrorCode, err, need, ok
from services.templating.template_metadata import read_template_header

logger = logging.getLogger(__name__)

_SYMMETRIES = tuple(g.value for g in SymmetryGroup)


def _identity_problems(name, diameter_ang, symmetry) -> list[str]:
    problems = []
    if name is not None and not name.strip():
        problems.append("name: empty")
    if diameter_ang is not None and diameter_ang <= 0:
        problems.append(f"diameter_ang: must be > 0, not {diameter_ang:g}")
    if symmetry is not None and symmetry not in _SYMMETRIES:
        problems.append(f"symmetry: one of {', '.join(_SYMMETRIES)}")
    return problems


def create_species(
    state: ProjectState,
    name: str,
    *,
    origin: str = SpeciesOrigin.WORKBENCH.value,
    color: str = "",
    diameter_ang: float | None = None,
    symmetry: str = SymmetryGroup.C1.value,
    notes: str = "",
    mask_path: str | None = None,
) -> dict[str, Any]:
    """A new species with its facts, and a mask registered as it is (not copied into the project), as the
    species dialog makes it. A template comes after, through its import (inspected, its polarity confirmed).
    `ok(species_id, warnings)`."""
    problems = _identity_problems(name, diameter_ang, symmetry)
    if problems:
        return err("Not created: " + "; ".join(problems), problems=problems)
    species = state.add_species(name.strip(), origin=SpeciesOrigin(origin).value, color=color)

    def _apply(sp) -> None:
        sp.diameter_ang = diameter_ang
        sp.symmetry = symmetry
        sp.notes = notes

    state.mutate_species(species.id, _apply)
    warnings = []
    if mask_path:
        res = species_admin.register_mask(state, species.id, TemplateMask(mask_path=mask_path, method="imported"))
        if not res["success"]:
            warnings.append(res["error"])
    return ok(species_id=species.id, warnings=warnings)


# What an edit leaves as it is (None is a value: a diameter cleared).
KEEP: Any = object()


def set_species(
    state: ProjectState,
    species_id: str,
    *,
    name: Any = KEEP,
    color: Any = KEEP,
    diameter_ang: Any = KEEP,
    symmetry: Any = KEEP,
    notes: Any = KEEP,
    extraction: Any = KEEP,
) -> dict[str, Any]:
    """Edit a species: its identity (what the Particles registry's editor sets; a diameter of None clears it)
    and its extraction geometry (box, binning, crop: all three, which the jobs added for it later take). Only
    what is given changes; a job made earlier keeps the values it took. `ok(species)` as it now is."""
    if state.get_species(species_id) is None:
        listed = ", ".join(s.id for s in state.species_registry) or "none"
        return err(f"No species {species_id} in this project ({listed}).")
    problems = _identity_problems(
        None if name is KEEP else name,
        None if diameter_ang is KEEP else diameter_ang,
        None if symmetry is KEEP else symmetry,
    )
    geometry = KEEP
    if extraction is not KEEP:
        box, binning, crop = extraction
        if not box or not binning or not crop:
            problems.append("extraction: box size, binning and crop are all required")
        else:
            geometry = ExtractionParams(box_size=int(box), binning=float(binning), crop_size=int(crop))
    if problems:
        return err("Not set: " + "; ".join(problems), problems=problems)

    def _apply(sp) -> None:
        if name is not KEEP:
            sp.name = name.strip()
        if color is not KEEP:
            sp.color = color
        if diameter_ang is not KEEP:
            sp.diameter_ang = diameter_ang
        if symmetry is not KEEP:
            sp.symmetry = symmetry
        if notes is not KEEP:
            sp.notes = notes
        if geometry is not KEEP:
            sp.extraction_params = geometry

    state.mutate_species(species_id, _apply)
    sp = state.get_species(species_id)
    return ok(
        species={
            "id": sp.id,
            "name": sp.name,
            "color": sp.color,
            "diameter_ang": sp.diameter_ang,
            "symmetry": sp.symmetry,
            "notes": sp.notes,
            "extraction": sp.extraction_params.model_dump() if sp.extraction_params else None,
        }
    )


# ── Templates and masks made in the project ───────────────────────────────────

TEMPLATE_KINDS = ("shape", "pdb", "emdb", "resample", "lowpass", "flip")
MASK_KINDS = ("threshold", "sphere")
THRESHOLD_METHODS = ("flexible_bounds", "otsu", "isodata", "li", "yen")
SHAPE_MIN_BOX = 96
# A basic shape is always filtered: at this resolution (Å) when none is given (`generate_basic_shape_async`).
SHAPE_LOWPASS = 45.0
# The resolution (Å) a PDB entry's density is simulated at when none is given.
PDB_RESOLUTION = 10.0
# A threshold mask's extension and soft edge (pixels) and the lowpass (Å) it thresholds at, when not given; the
# soft edge is a sphere's too.
MASK_EXTEND = 5.0
MASK_SOFT_EDGE = 5.0
MASK_LOWPASS = 20.0


def auto_box(shape: str, pixel_size: float, minimum: int = SHAPE_MIN_BOX) -> int:
    """A basic shape's box: its longest axis plus 30 %, in pixels, rounded up to a multiple of 32, and at least
    `minimum`. Raises ValueError for a shape that is not numbers."""
    longest = max(float(x) for x in shape.split(":"))
    return max(int(((longest / pixel_size) * 1.3 + 31) // 32) * 32, minimum)


async def template_pixel_size(state: ProjectState) -> tuple[float | None, str]:
    """The voxel size of the tomograms this project's templates are matched against: what its Reconstruct jobs
    make, else what its imported tomograms have. `(size, where it comes from)`, or `(None, why there is none)`:
    no tomograms yet, or tomograms of several voxel sizes."""
    made = sorted({float(jm.rescale_angpixs) for jm in state.jobs.values() if jm.job_type == JobType.TS_RECONSTRUCT})
    if len(made) == 1:
        return made[0], "the Reconstruct job's pixel size"
    if made:
        return None, "the Reconstruct jobs make tomograms of " + ", ".join(f"{v:g}" for v in made) + " Å"
    imported = state.imported_tomograms
    if imported is not None and imported.star_path:
        star = Path(imported.star_path)
        if not star.is_absolute():
            star = Path(state.project_path) / star
        try:
            sizes = await asyncio.to_thread(tomogram_import.voxel_sizes, star)
        except (OSError, ValueError) as e:
            return None, f"the imported tomograms' star could not be read ({e})"
        if len(sizes) == 1:
            return sizes[0], "the imported tomograms' voxel size"
        if sizes:
            return None, "the imported tomograms have voxel sizes " + ", ".join(f"{v:g}" for v in sizes) + " Å"
    return None, "no tomograms yet: no Reconstruct job, none imported"


@dataclass
class TemplatePlan:
    """A template to make with every value settled (`plan_template`), for `make_template`."""

    kind: str
    species_id: str
    out_dir: str
    source: Any = None  # the ParticleTemplate a resample, lowpass or flip starts from
    entry_id: str = ""  # the PDB or EMDB entry
    shape: str = ""
    pixel_size: float | None = None
    pixel_size_from: str = ""
    box_size: int | None = None
    lowpass: float | None = None

    @property
    def key(self) -> str:
        """The same template made twice at once is made once."""
        start = self.source.template_path if self.source is not None else self.entry_id or self.shape
        return f"make-template:{self.out_dir}:{self.kind}:{start}:{self.pixel_size}:{self.box_size}:{self.lowpass}"

    def values(self) -> dict[str, Any]:
        out: dict[str, Any] = {"kind": self.kind}
        if self.source is not None:
            out["from_template"] = self.source.id
        for name in ("entry_id", "shape"):
            if getattr(self, name):
                out[name] = getattr(self, name)
        if self.kind != "flip":
            out.update(pixel_size=self.pixel_size, pixel_size_from=self.pixel_size_from, box_size=self.box_size)
            out["lowpass"] = SHAPE_LOWPASS if self.kind == "shape" and not self.lowpass else self.lowpass
        return out


def _source_template(sp, template_id: str | None):
    tid = template_id or sp.selected_template_id
    tpl = sp.get_template_by_id(tid) if tid else None
    if tpl is None:
        listed = ", ".join(t.id for t in sp.templates) or "none"
        return None, err(f"No template {tid or '(none selected)'} on {sp.id} to start from (registered: {listed}).")
    return tpl, None


async def plan_template(
    state: ProjectState,
    species_id: str,
    kind: str,
    *,
    shape: str | None = None,
    pdb_id: str | None = None,
    emdb_id: str | None = None,
    template_id: str | None = None,
    pixel_size: float | None = None,
    box_size: int | None = None,
    lowpass: float | None = None,
) -> dict[str, Any]:
    """Settle a template to make, as the workbench's generators take it:
    - `shape`: an ellipsoid of `shape` (`x:y:z`, Å) at `pixel_size`, in its own box (`auto_box`, at least
      `box_size`), filtered at `lowpass` (45 Å when not given);
    - `pdb`: the density simulated from PDB entry `pdb_id` at resolution `lowpass` (10 Å when not given), at
      `pixel_size` in a box of `box_size`;
    - `emdb`: EMDB map `emdb_id` resampled to `pixel_size` and `box_size`, filtered at `lowpass` when given;
    - `resample`: template `template_id` (the selected one when not given) resampled the same way;
    - `lowpass`: that template filtered at `lowpass`, at its own pixel size and box (from its header; `pixel_size`
      and `box_size` when its header has none);
    - `flip`: that template's other polarity.
    A pixel size not given is the tomograms' (`template_pixel_size`); without tomograms it is a `needs` item, as
    is any other value the kind needs and was not given. `ok(plan=TemplatePlan)`."""
    sp = state.get_species(species_id)
    if sp is None:
        listed = ", ".join(s.id for s in state.species_registry) or "none"
        return err(f"No species {species_id} in this project ({listed}).")
    if kind not in TEMPLATE_KINDS:
        return err(f"No template kind {kind!r}: one of {', '.join(TEMPLATE_KINDS)}.")
    plan = TemplatePlan(
        kind=kind,
        species_id=species_id,
        out_dir=os.path.join(str(state.project_path), "templates", species_id),
        box_size=box_size,
        lowpass=lowpass,
    )
    problems: list[str] = []
    needs: list[dict[str, Any]] = []
    for name, value in (("pixel_size", pixel_size), ("box_size", box_size), ("lowpass", lowpass)):
        if value is not None and value <= 0:
            problems.append(f"{name}: must be > 0, not {value:g}")

    if kind in ("resample", "lowpass", "flip"):
        plan.source, why = _source_template(sp, template_id)
        if why:
            return why
        if not await asyncio.to_thread(os.path.isfile, plan.source.template_path):
            return err(f"Template {plan.source.id}: {plan.source.template_path} is not on disk.")
    if kind == "shape":
        if not shape:
            needs.append(need("value", "shape", "the ellipsoid's axes, x:y:z in Å"))
        else:
            try:
                dims = [float(x) for x in shape.split(":")]
            except ValueError:
                dims = []
            if len(dims) != 3 or min(dims) <= 0:
                problems.append(f"shape: three axes x:y:z in Å, each > 0, not {shape!r}")
            plan.shape = shape
    elif kind == "pdb":
        if not (pdb_id or "").strip():
            needs.append(need("value", "pdb_id", "the PDB entry to simulate the density from"))
        plan.entry_id = (pdb_id or "").strip().lower()
        plan.lowpass = lowpass or PDB_RESOLUTION
    elif kind == "emdb":
        if not (emdb_id or "").strip():
            needs.append(need("value", "emdb_id", "the EMDB entry whose map is fetched"))
        plan.entry_id = (emdb_id or "").strip()
    elif kind == "lowpass" and lowpass is None:
        needs.append(need("value", "lowpass", "the resolution to filter the template at, Å"))

    if kind in ("shape", "pdb", "emdb", "resample"):
        if pixel_size is not None:
            plan.pixel_size, plan.pixel_size_from = pixel_size, "given"
        else:
            found, why = await template_pixel_size(state)
            if found is None:
                needs.append(need("value", "pixel_size", f"the template's pixel size in Å, the tomograms' ({why})"))
            plan.pixel_size, plan.pixel_size_from = found, why
    if kind in ("pdb", "emdb", "resample") and box_size is None:
        needs.append(need("value", "box_size", "the template's box edge in pixels, at that pixel size"))
    if kind == "lowpass":
        header = await asyncio.to_thread(read_template_header, plan.source.template_path)
        plan.pixel_size = header.apix_ang or pixel_size
        plan.pixel_size_from = "its header" if header.apix_ang else "given"
        plan.box_size = header.box_px or box_size
        if not plan.pixel_size:
            needs.append(need("value", "pixel_size", "the template's header has no pixel size"))
        if not plan.box_size:
            needs.append(need("value", "box_size", "the template's header has no box size"))

    if problems:
        return err("Not made: " + "; ".join(problems), problems=problems)
    if needs:
        return err(
            "Not made: " + ", ".join(n["what"] for n in needs) + " open.", code=ErrorCode.NEEDS_DECISION, needs=needs
        )
    if kind == "shape":
        plan.box_size = auto_box(shape, plan.pixel_size, box_size or SHAPE_MIN_BOX)
    return ok(plan=plan)


def _flipped_path(path: str, polarity: str) -> str:
    """Where a template's other polarity lives: its `_white`/`_black` sibling, else beside it under its name with
    the other polarity appended."""
    other = "black" if polarity == "white" else "white"
    if path.endswith(f"_{polarity}.mrc"):
        return path[: -len(f"_{polarity}.mrc")] + f"_{other}.mrc"
    stem = Path(path).stem
    for token in ("_white", "_black"):
        if stem.endswith(token):
            stem = stem[: -len(token)]
            break
    return os.path.join(os.path.dirname(path), f"{stem}_{other}.mrc")


def _negate(src: str, dst: str) -> None:
    """Write the negative of `src` to `dst`, with its voxel size. Never overwrites."""
    import mrcfile
    import numpy as np

    with mrcfile.open(src, mode="r", permissive=True) as m:
        data = np.array(m.data, copy=True)
        voxel = float(getattr(m.voxel_size, "x", 0.0) or 0.0)
    with mrcfile.new(dst, overwrite=False) as m:
        m.set_data((-data).astype(np.float32))
        if voxel > 0:
            m.voxel_size = voxel


async def _flip(state: ProjectState, plan: TemplatePlan) -> dict[str, Any]:
    """The other polarity of `plan.source`, selected: registered already, or on disk and registered now, or
    written as its negative."""
    tpl = plan.source
    other = "black" if tpl.polarity == "white" else "white"
    target = _flipped_path(tpl.template_path, tpl.polarity)
    name = os.path.basename(tpl.template_path)
    sp = state.get_species(plan.species_id)
    existing = next((t for t in sp.templates if t.template_path == target), None) if sp else None
    if existing is not None:
        species_admin.select_template(state, plan.species_id, existing.id)
        return ok(template_id=existing.id, paths=[target], values=plan.values(), message="registered already")
    if await asyncio.to_thread(os.path.exists, target):
        source = tpl.source or f"flipped from {name}"
    else:
        try:
            await asyncio.to_thread(_negate, tpl.template_path, target)
        except (OSError, ValueError) as e:
            logger.exception("Flipping the polarity of %s failed", tpl.template_path)
            return err(f"Polarity flip failed: {e}")
        source = f"flipped from {name}"
    res = species_admin.register_template(
        state, plan.species_id, target, polarity=other, source=source, lowpass=tpl.lowpass_resolution_ang
    )
    if not res["success"]:
        return res
    species_admin.select_template(state, plan.species_id, res["template_id"])
    return ok(template_id=res["template_id"], paths=[target], values=plan.values())


async def make_template(
    backend, state: ProjectState, plan: TemplatePlan, progress_cb: Callable[[str], None] | None = None
) -> dict[str, Any]:
    """Make the template `plan` settles, as the workbench's generators do: write its white and black volumes into
    `templates/<species>/`, register both, and select the white, the template the next step reads; a flip selects
    the flipped one. Marks the state dirty; the caller saves. `ok(template_id, templates, paths, values)`."""
    say = progress_cb or (lambda _text: None)
    await asyncio.to_thread(os.makedirs, plan.out_dir, exist_ok=True)
    if plan.kind == "flip":
        return await _flip(state, plan)
    service = backend.template_service
    if plan.kind == "shape":
        say(f"Building ellipsoid {plan.shape}…")
        res = await service.generate_basic_shape_async(
            plan.shape, plan.pixel_size, plan.out_dir, plan.box_size, plan.lowpass
        )
        source = f"basic_shape:{plan.shape}"
    elif plan.kind == "pdb":
        say(f"Fetching PDB {plan.entry_id}…")
        fetched = await service.fetch_pdb_async(plan.entry_id, plan.out_dir)
        if not fetched["success"]:
            return err(f"PDB fetch failed: {fetched['error']}")
        say(f"Simulating the density of {os.path.basename(fetched['path'])}…")
        res = await backend.pdb_service.simulate_map_from_pdb(
            pdb_path=fetched["path"],
            output_folder=plan.out_dir,
            target_apix=plan.pixel_size,
            target_box=plan.box_size,
            resolution=plan.lowpass,
        )
        source = f"PDB:{plan.entry_id}"
    elif plan.kind == "emdb":
        say(f"Fetching EMDB {plan.entry_id}…")
        fetched = await service.fetch_emdb_map_async(plan.entry_id, plan.out_dir)
        if not fetched["success"]:
            return err(f"EMDB fetch failed: {fetched['error']}")
        say(f"Resampling {os.path.basename(fetched['path'])}…")
        res = await service.process_volume_async(
            fetched["path"], plan.out_dir, plan.pixel_size, plan.box_size, plan.lowpass
        )
        source = f"EMDB-{plan.entry_id}"
    else:
        name = os.path.basename(plan.source.template_path)
        say(f"{'Resampling' if plan.kind == 'resample' else 'Filtering'} {name}…")
        res = await service.process_volume_async(
            plan.source.template_path, plan.out_dir, plan.pixel_size, plan.box_size, plan.lowpass
        )
        source = f"resampled from {name}" if plan.kind == "resample" else f"lowpass({plan.lowpass:g}Å) from {name}"
    if not res["success"]:
        return err(f"Making the {plan.kind} template failed: {res['error']}")

    pair = [(res.get("path_white"), "white"), (res.get("path_black"), "black")]
    on_disk = await asyncio.to_thread(lambda: {path for path, _ in pair if path and os.path.exists(path)})
    made: dict[str, str] = {}
    for path, polarity in pair:
        if path in on_disk:
            reg = species_admin.register_template(
                state, plan.species_id, path, polarity=polarity, source=source, lowpass=plan.lowpass
            )
            if not reg["success"]:
                return reg
            made[polarity] = reg["template_id"]
    if "white" in made:
        species_admin.select_template(state, plan.species_id, made["white"])
    return ok(
        template_id=made.get("white"),
        templates=made,
        paths=[path for path, _ in pair if path in on_disk],
        values=plan.values(),
    )


@dataclass
class MaskPlan:
    """A mask to make with every value settled (`plan_mask`), for `make_mask`."""

    kind: str
    species_id: str
    template: Any  # the ParticleTemplate the mask is made from (a threshold) or on the grid of (a sphere)
    input_path: str
    out_path: str
    threshold: float | None = None
    threshold_from: str = ""
    extend: float = MASK_EXTEND
    soft_edge: float = MASK_SOFT_EDGE
    lowpass: float = MASK_LOWPASS
    diameter_ang: float | None = None
    pixel_size: float | None = None
    box_size: int | None = None

    @property
    def key(self) -> str:
        return f"make-mask:{self.out_path}"

    def values(self) -> dict[str, Any]:
        out: dict[str, Any] = {"kind": self.kind, "from_template": self.template.id, "soft_edge": self.soft_edge}
        if self.kind == "threshold":
            out.update(
                threshold=self.threshold, threshold_from=self.threshold_from, extend=self.extend, lowpass=self.lowpass
            )
        else:
            out.update(diameter_ang=self.diameter_ang, pixel_size=self.pixel_size, box_size=self.box_size)
        return out


async def plan_mask(
    backend,
    state: ProjectState,
    species_id: str,
    kind: str,
    *,
    template_id: str | None = None,
    threshold: float | None = None,
    threshold_method: str | None = None,
    extend: float = MASK_EXTEND,
    soft_edge: float = MASK_SOFT_EDGE,
    lowpass: float = MASK_LOWPASS,
    diameter_ang: float | None = None,
) -> dict[str, Any]:
    """Settle a mask to make from template `template_id` (the selected one when not given), as the workbench's
    mask tools take it:
    - `threshold`: RELION's mask of the template's white volume at `threshold` (or at the level one of
      `THRESHOLD_METHODS` finds, as the workbench's method menu sets it), extended by `extend` and softened over
      `soft_edge` pixels, the volume filtered at `lowpass` Å first;
    - `sphere`: a sphere of `diameter_ang` (the species' when not given) on the template's grid, softened over
      `soft_edge` pixels.
    A value the kind needs and nobody gave is a `needs` item. `ok(plan=MaskPlan)`."""
    sp = state.get_species(species_id)
    if sp is None:
        listed = ", ".join(s.id for s in state.species_registry) or "none"
        return err(f"No species {species_id} in this project ({listed}).")
    if kind not in MASK_KINDS:
        return err(f"No mask kind {kind!r}: one of {', '.join(MASK_KINDS)}.")
    tpl, why = _source_template(sp, template_id)
    if why:
        return why
    if not await asyncio.to_thread(os.path.isfile, tpl.template_path):
        return err(f"Template {tpl.id}: {tpl.template_path} is not on disk.")
    out_dir = os.path.join(str(state.project_path), "templates", species_id)
    base = Path(tpl.template_path).stem.replace("_white", "").replace("_black", "")
    problems = [f"{n}: must be ≥ 0, not {v:g}" for n, v in (("extend", extend), ("soft_edge", soft_edge)) if v < 0]

    if kind == "threshold":
        if lowpass <= 0:
            problems.append(f"lowpass: must be > 0, not {lowpass:g}")
        if threshold_method is not None and threshold_method not in THRESHOLD_METHODS:
            problems.append(f"threshold_method: one of {', '.join(THRESHOLD_METHODS)}")
        if problems:
            return err("Not made: " + "; ".join(problems), problems=problems)
        source = tpl.template_path
        if source.endswith("_black.mrc"):
            white = source[: -len("_black.mrc")] + "_white.mrc"
            if await asyncio.to_thread(os.path.exists, white):
                source = white
        threshold_from = "given"
        if threshold is None:
            found = await backend.template_service.calculate_thresholds_async(source, lowpass)
            if not found:
                return err(f"The threshold levels of {os.path.basename(source)} could not be computed.")
            if threshold_method is None:
                options = [{"threshold_method": m, "threshold": round(found[m], 4)} for m in THRESHOLD_METHODS]
                return err(
                    "Not made: threshold open.",
                    code=ErrorCode.NEEDS_DECISION,
                    needs=[
                        need(
                            "choice",
                            "threshold",
                            "the density level the mask starts from: a number (threshold), or a method that finds "
                            f"it (threshold_method), here on {os.path.basename(source)} filtered at {lowpass:g} Å",
                            options=options,
                        )
                    ],
                )
            threshold, threshold_from = round(found[threshold_method], 4), threshold_method
        return ok(
            plan=MaskPlan(
                kind=kind,
                species_id=species_id,
                template=tpl,
                input_path=source,
                out_path=os.path.join(out_dir, f"{base}_mask_t{threshold:.3f}_e{int(extend)}_s{int(soft_edge)}.mrc"),
                threshold=threshold,
                threshold_from=threshold_from,
                extend=extend,
                soft_edge=soft_edge,
                lowpass=lowpass,
            )
        )

    diameter = diameter_ang if diameter_ang and diameter_ang > 0 else sp.diameter_ang
    if problems:
        return err("Not made: " + "; ".join(problems), problems=problems)
    if not diameter or diameter <= 0:
        return err(
            "Not made: diameter_ang open.",
            code=ErrorCode.NEEDS_DECISION,
            needs=[need("value", "diameter_ang", f"the sphere's diameter in Å; species {sp.id} has none")],
        )
    header = await asyncio.to_thread(read_template_header, tpl.template_path)
    if not header.apix_ang or not header.box_px:
        return err(f"Template {tpl.id} has no pixel size or box in its header: the sphere takes its grid.")
    return ok(
        plan=MaskPlan(
            kind=kind,
            species_id=species_id,
            template=tpl,
            input_path=tpl.template_path,
            out_path=os.path.join(out_dir, f"{base}_sphere_d{round(diameter)}_s{round(soft_edge)}.mrc"),
            soft_edge=soft_edge,
            diameter_ang=float(diameter),
            pixel_size=float(header.apix_ang),
            box_size=int(header.box_px),
        )
    )


async def make_mask(backend, state: ProjectState, plan: MaskPlan) -> dict[str, Any]:
    """Make the mask `plan` settles, as the workbench's mask tools do, and register it on the species (selected
    only when it has no mask yet). Marks the state dirty; the caller saves. `ok(mask_id, path, values)`."""
    service = backend.template_service
    if plan.kind == "threshold":
        res = await service.create_mask_relion(
            plan.input_path, plan.out_path, plan.threshold, plan.extend, plan.soft_edge, plan.lowpass
        )
        mask = TemplateMask(
            mask_path=plan.out_path,
            method="relion",
            threshold=plan.threshold,
            extend_pixels=plan.extend,
            soft_edge_pixels=plan.soft_edge,
            lowpass_ang=plan.lowpass,
            derived_from_template_id=plan.template.id,
        )
    else:
        res = await service.create_spherical_mask_async(
            output_path=plan.out_path,
            apix_ang=plan.pixel_size,
            box_px=plan.box_size,
            diameter_ang=plan.diameter_ang,
            soft_edge_pixels=plan.soft_edge,
        )
        mask = TemplateMask(
            mask_path=plan.out_path,
            method="spherical",
            soft_edge_pixels=plan.soft_edge,
            derived_from_template_id=plan.template.id,
        )
    if not res["success"]:
        return err(f"Making the {plan.kind} mask failed: {res['error']}")
    reg = species_admin.register_mask(state, plan.species_id, mask)
    if not reg["success"]:
        return reg
    return ok(mask_id=reg["mask_id"], path=plan.out_path, values=plan.values())

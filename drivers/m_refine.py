#!/usr/bin/env python3
"""
Driver for one round of Warp's M (MTools sets the population up, MCore runs the round).

A first round builds M's population in the job's folder (services/particles/m_population.py): the sources, copies of
the Warp files of the tilt series the particles sit on, each checked; the particles as M reads them; the Mask job's
surface made binary again. Then the species: the Refine 3D's unfiltered halves at their own pixel size, the
symmetry and diameter the job holds. A follow-up round copies an earlier round's population, every species and the
sources into its own folder; with *Add this job's species* it makes this job's species in that copy as a first
round makes one (its particles on the population's tilt series, or on its own tilt series too, which join as
sources). M's versioning is off (--dont_version): a round's state is its job folder, and no job writes another's
files.

Before the round, the job's own species can change in its copy: a new mask (MTools update_mask), its trajectories
resampled in time (resample_trajectories), its symmetry expanded into copies (expand_symmetry; each copy then named
after its parent, `m_population.label_copies`).

Then one MCore round with the job's switches: its work queue in the node's memory (/dev/shm), its REST API off, the
GPUs the job was given (renumbered 0..N-1 in the container). After it, for every species of the population (the
job's own in the job folder, every other in species/<name>/):
- M's half maps windowed to its Refine 3D's box (half1.mrc, half2.mrc) and its filtered map (m_filtered.mrc), so a
  PostProcess reads them in the masks the trunk used;
- M's particles as a RELION 5 set without images (optimisation_set.star), its tomograms naming the source copies'
  per-tilt stars, which hold M's per-tilt CTF;
then per tilt series what the round changed against the model it started from (m_changes.json), and the round's
record (m_round.json), which a follow-up reads.

*Hand back only* runs no round: the job's species' files as the round it continues left them become the job's own
outputs, and its record points a follow-up at that round's state.

M refines the CTF only from species finer than --ctf_minresolution and prints nothing when it skips one; the driver
says which species the fit used, in the log and in the record.
"""

import getpass
import json
import math
import os
import shlex
import shutil
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from drivers.driver_base import (
    ToolCommand,
    allocated_gpu_count,
    get_driver_context,
    require_producer_input,
    run_tool,
    start_supervisor,
)
from drivers.postprocess import geometry, second_half
from services.jobs.m_refine import CTF_FLAGS, MRefineParams
from services.models_base import resolve_species
from services.particles import m_population as mp
from services.subtomo_merge import (
    _parse_optimisation_set,
    _read_input_particles_lenient,
    _read_tomograms_star,
    write_optimisation_set,
)

ROUND_RECORD = "m_round.json"
RECORD_VERSION = 2
CHANGES_NAME = "m_changes.json"
HALF_NAMES = ("half1.mrc", "half2.mrc")
FILTERED_NAME = "m_filtered.mrc"
SET_NAME = "optimisation_set.star"
SET_FILES = ("particles.star", "tomograms.star")
SPECIES_OUT = "species"  # species/<name>/: the outputs of every species but the job's own
LDD_NAME = "torchsharp_ldd.txt"
OUTPUTS = (*HALF_NAMES, FILTERED_NAME, ROUND_RECORD, CHANGES_NAME, SET_NAME, *SET_FILES)
APIX_TOLERANCE = 1e-3
NO_MOVES = {
    "particles": 0,
    "moved_rms_ang": None,
    "moved_max_ang": None,
    "turned_rms_deg": None,
    "turned_max_deg": None,
}


def say(msg: str) -> None:
    print(f"[DRIVER] {msg}", flush=True)


def task_root() -> Path:
    """MCore's work queue, which its workers claim by renaming files: a folder of this job's own in the node's memory
    (/dev/shm, which the container shares and SLURM empties), never on NFS; the job's TMPDIR where /dev/shm is not
    writable."""
    job = os.environ.get("SLURM_JOB_ID") or str(os.getpid())
    base = Path("/dev/shm")
    if not (base.is_dir() and os.access(base, os.W_OK)):
        base = Path(os.environ.get("TMPDIR") or "/tmp")
        say(f"WARNING: /dev/shm is not writable here; M's task queue goes to {base}")
    return base / f"crboost-{getpass.getuser()}-{job}-m_tasks"


def check_torchsharp(params: MRefineParams, job_dir: Path) -> None:
    """M trains its denoiser and commits every round through TorchSharp. In an image whose PyTorch lacks a symbol its
    LibTorchSharp was built against, M dies there minutes in; `ldd -r` says so in a second."""
    out = job_dir / LDD_NAME
    out.unlink(missing_ok=True)
    q = shlex.quote(str(out))
    cmd = (
        'L="$(dirname "$(dirname "$(command -v MTools)")")/lib/libLibTorchSharp.so"; '
        f'if [ -e "$L" ]; then ldd -r "$L" > {q} 2>&1; else echo "absent: $L" > {q}; fi'
    )
    run_tool(cmd, tool_name=params.get_tool_name(), cwd=job_dir, binds=[str(job_dir)])
    if not out.is_file():
        say("LibTorchSharp not checked: ldd wrote nothing")
        return
    text = out.read_text()
    if text.startswith("absent:"):
        say(f"LibTorchSharp not checked, not where it was looked for ({text.strip()[8:]})")
        return
    bad = [line.strip() for line in text.splitlines() if "undefined symbol" in line or "not found" in line]
    if bad:
        raise RuntimeError(
            f"The Warp image's LibTorchSharp does not load ({bad[0]}; all of ldd's output in {out}). M dies in it "
            "when it makes a species and when it commits a round: the image's PyTorch must be the one this Warp's "
            "TorchSharp was built against (PyTorch 2.13 for Warp 2.0.0dev41)"
        )
    say("LibTorchSharp loads in the Warp image (ldd -r: no undefined symbol)")


def mtools(params: MRefineParams, cwd: Path, binds: set[str], command: ToolCommand) -> None:
    say(f"Command: {command}")
    run_tool(command, tool_name=params.get_tool_name(), cwd=cwd, binds=sorted(binds))


def species_name(state, params: MRefineParams, instance_id: str | None) -> str:
    species, sid = resolve_species(state, params, instance_id)
    return mp.safe_name(str(getattr(species, "id", None) or sid or "species"))


def own_species(sname: str, names) -> str:
    """The job's own species among a population's: its species' name, or the one species there is."""
    names = sorted(names)
    if sname in names:
        return sname
    if len(names) == 1:
        say(f"This job's species is {sname}; the population's one species is {names[0]}, which the job takes")
        return names[0]
    raise ValueError(
        f"This job's species {sname} is not in the population, which holds {', '.join(names)}: set the job's "
        "species to the one it continues or hands back"
    )


def ctf_gate(params: MRefineParams, resolutions: dict[str, float | None], own: str) -> dict:
    """Which species M fits the CTF from: those finer than 'CTF from'. Skipped when no species is."""
    switches = params.ctf_switches()
    limit = params.ctf_minresolution
    fitted = sorted(n for n, r in resolutions.items() if r is not None and r <= limit)
    coarse = sorted(n for n, r in resolutions.items() if r is not None and r > limit)
    return {
        "switches": [CTF_FLAGS[s] for s in switches],
        "min_resolution_ang": limit,
        "species_resolution_ang": resolutions.get(own),
        "resolutions_ang": resolutions,
        "fitted_on": fitted if switches else [],
        "not_fitted": coarse if switches else [],
        "skipped": bool(switches) and not fitted and bool(coarse),
    }


def resolution_of(params: dict[str, str]) -> float | None:
    try:
        value = float(params.get("GlobalResolution", ""))
    except ValueError:
        return None
    return value if math.isfinite(value) and value > 0 else None


# ── a species' inputs, and making it


@dataclass
class SpeciesInputs:
    set: Path
    half1: Path
    half2: Path
    mask: Path
    particles: pd.DataFrame
    tomograms: pd.DataFrame
    tomograms_star: Path
    box: int
    apix: float


def species_inputs(paths: dict[str, Path]) -> SpeciesInputs:
    input_set = paths["input_optimisation"]
    half1 = paths["input_half_map"]
    half2 = second_half(half1)
    mask = paths["input_mask"]
    for path, label in ((input_set, "Optimisation set"), (half1, "Half map 1"), (half2, "Half map 2"), (mask, "Mask")):
        require_producer_input(path, label)
    particles_star, tomograms_star = _parse_optimisation_set(input_set)
    particles, _, _ = _read_input_particles_lenient(particles_star)
    tomograms = _read_tomograms_star(tomograms_star)
    say(f"Set: {input_set} ({len(particles)} particles)")
    (b1, a1), (b2, a2) = geometry(half1), geometry(half2)
    if b1 != b2 or abs(a1 - a2) > APIX_TOLERANCE or len(set(b1)) != 1:
        raise ValueError(f"The half maps are {b1} px at {a1:g} Å and {b2} px at {a2:g} Å: two equal cubes expected")
    return SpeciesInputs(input_set, half1, half2, mask, particles, tomograms, tomograms_star, b1[0], a1)


def create_source(params: MRefineParams, m_dir: Path, binds: set[str], plan: mp.SourcePlan) -> None:
    d = m_dir / mp.SOURCES_DIR / plan.name
    mtools(
        params,
        m_dir,
        binds,
        ToolCommand("MTools create_source")
        .opt_path("--population", mp.population_path(m_dir.parent), quote=True)
        .opt_path("--processing_settings", d / mp.SETTINGS_NAME, quote=True)
        .opt("--name", plan.name)
        .opt_path("--files", d / mp.FILES_NAME, quote=True)
        .flag("--dont_version"),
    )


def make_species(
    params: MRefineParams,
    m_dir: Path,
    sname: str,
    inp: SpeciesInputs,
    import_star: Path,
    mask_name: str,
    binds: set[str],
) -> dict:
    """MTools create_species from the inputs; returns what its mask was made from."""
    mask_data, mask_apix, mask_info = mp.binary_mask(inp.mask)
    if mask_data.shape != (inp.box,) * 3 or abs(mask_apix - inp.apix) > APIX_TOLERANCE:
        raise ValueError(
            f"The Mask job's map is {mask_data.shape[::-1]} px at {mask_apix:g} Å, the half maps {inp.box} px at "
            f"{inp.apix:g} Å: make the mask on the Refine 3D's map"
        )
    mp.write_map(m_dir / mask_name, mask_data, inp.apix)
    say(
        f"M's mask for {sname}: {inp.mask.parent / mp.MASK_LOWPASS} at {mask_info['threshold']:.6g}, binary, "
        f"{mask_info['fraction']:.1%} of the box"
    )
    binds |= {str(inp.half1.parent.resolve()), str(inp.mask.parent.resolve())}
    species = (
        ToolCommand("MTools create_species")
        .opt_path("--population", mp.population_path(m_dir.parent), quote=True)
        .opt("--name", sname)
        .opt("--diameter", f"{params.diameter_ang:g}")
        .opt("--sym", shlex.quote(params.symmetry.strip()))
        .opt("--temporal_samples", params.temporal_samples or 1)
        .opt_path("--half1", inp.half1, quote=True)
        .opt_path("--half2", inp.half2, quote=True)
        .opt_path("--mask", m_dir / mask_name, quote=True)
        .opt_path("--particles_relion", import_star, quote=True)
        .opt("--angpix_coords", 1)
        .opt("--angpix_shifts", 1)
        .flag("--dont_version")
    )
    if not params.use_denoiser:
        species.flag("--dont_use_denoiser")
    mtools(params, m_dir, binds, species)
    return mask_info | {"from": str(inp.mask)}


def new_entry(sname: str, inp: SpeciesInputs, mask: dict, round_no: int, *, subset_ok: bool) -> dict:
    return {
        "name": sname,
        "refine3d_set": str(inp.set),
        "set_in": str(inp.set),
        "subset_ok": subset_ok,
        "box": inp.box,
        "pixel_size": inp.apix,
        "mask": mask,
        "added_round": round_no,
        "expansions": [],
    }


def first_round(params: MRefineParams, paths: dict[str, Path], job_dir: Path, sname: str, binds: set[str]) -> dict:
    """The population, sources and species of a first round; what the round's record takes from it."""
    inp = species_inputs(paths)
    plans = mp.plan_sources(inp.particles, inp.tomograms, inp.tomograms_star)
    m_dir = job_dir / mp.M_DIR
    m_dir.mkdir(parents=True)
    manifest = mp.write_sources(plans, m_dir)
    for p in plans:
        say(
            f"Source {p.name}: {len(p.series)} tilt series, {sum(s.particles for s in p.series)} particles, "
            f"{p.dims} px at {p.apix:g} Å, from {p.settings_from}"
        )
    mp.write_plain_star(mp.m_import_table(inp.particles, manifest), m_dir / mp.IMPORT_STAR)
    mtools(
        params,
        m_dir,
        binds,
        ToolCommand("MTools create_population").opt_path("--directory", m_dir, quote=True).opt("--name", mp.POPULATION),
    )
    for p in plans:
        create_source(params, m_dir, binds, p)
    mask = make_species(params, m_dir, sname, inp, m_dir / mp.IMPORT_STAR, mp.BINARY_MASK, binds)
    return {
        "round": 0,
        "parent": None,
        "manifest": manifest,
        "species": {sname: new_entry(sname, inp, mask, 0, subset_ok=False)},
    }


def species_entries(record: dict, record_dir: Path) -> dict[str, dict]:
    """The species a round's record names, by name; a record from before several species, its one."""
    if "all_species" in record:
        return {n: dict(e) for n, e in record["all_species"].items()}
    name = record.get("species_name") or Path(record["species"]).stem
    return {
        name: {
            "name": name,
            "refine3d_set": record.get("refine3d_set"),
            "set": record["set"],
            "box": int(record["box"]),
            "pixel_size": float(record["pixel_size"]),
            "mask": record.get("mask"),
            "out_dir": str(record_dir),
            "resolution_before_ang": record.get("resolution_before_ang"),
            "resolution_after_ang": record.get("resolution_after_ang"),
            "m_box": record.get("m_box"),
            "fsc": record.get("fsc"),
            "particles": record.get("particles"),
            "species_file": record.get("species"),
            "added_round": 0,
            "expansions": [],
        }
    }


def follow_up(paths: dict[str, Path], job_dir: Path) -> dict:
    record_path = paths["input_m"]
    require_producer_input(record_path, "The M round this one continues")
    parent = json.loads(record_path.read_text())
    parent_dir = record_path.parent
    state_dir = Path(parent.get("state_dir") or parent_dir)
    entries = species_entries(parent, parent_dir)
    say(
        f"Continuing round {parent['round']} of {parent_dir} (state in {state_dir}): "
        + ", ".join(f"{n} at {e.get('resolution_after_ang')} Å" for n, e in entries.items())
    )
    manifest = mp.copy_round(state_dir, job_dir)
    species = {
        n: {
            "name": n,
            "refine3d_set": e.get("refine3d_set"),
            "set_in": e["set"],
            "subset_ok": False,
            "box": int(e["box"]),
            "pixel_size": float(e["pixel_size"]),
            "mask": e.get("mask"),
            "added_round": e.get("added_round", 0),
            "expansions": list(e.get("expansions") or []),
        }
        for n, e in entries.items()
    }
    return {"round": int(parent["round"]) + 1, "parent": str(parent_dir), "manifest": manifest, "species": species}


def add_species(
    params: MRefineParams, paths: dict[str, Path], job_dir: Path, sname: str, start: dict, binds: set[str]
) -> None:
    """This job's species made in the population a follow-up copied: its particles on the population's tilt series
    (all of them, with *Only the population's tilt series* off, its other tilt series added as sources)."""
    m_dir = job_dir / mp.M_DIR
    found = mp.species_files(mp.population_path(job_dir))
    if sname in found:
        raise ValueError(
            f"The population holds a species {sname} already: continue the M job without adding one, or give this "
            "job another species"
        )
    inp = species_inputs(paths)
    off = {
        n: float(mp.species_params(p).get("PixelSize") or 0)
        for n, p in found.items()
        if abs(float(mp.species_params(p).get("PixelSize") or 0) - inp.apix) > APIX_TOLERANCE
    }
    if off:
        raise ValueError(
            f"The new species' half maps are at {inp.apix:g} Å and the population's species at "
            f"{', '.join(f'{n} {a:g} Å' for n, a in off.items())}: M would resample them, which is untried here. "
            "Make its Refine 3D at the population's pixel size"
        )
    manifest = start["manifest"]
    index = mp.series_index(manifest)
    tomo = inp.particles[mp.TOMO].astype(str)
    on = tomo.isin(set(index)).to_numpy()
    if not on.any() and params.population_series_only:
        raise ValueError(
            f"None of the {len(inp.particles)} particles of {inp.set} sit on the population's {len(index)} tilt "
            "series (matched by their name in the set)"
        )
    shared = inp.particles[on]
    if not shared.empty:
        for plan in mp.plan_sources(shared, inp.tomograms, inp.tomograms_star):
            for s in plan.series:
                pop = index[s.name][1]
                if Path(s.xml_from).resolve() != Path(pop["xml_from"]).resolve():
                    raise ValueError(
                        f"{s.name}: the new species' set names its Warp model {s.xml_from}, the population "
                        f"{pop['xml_from']}: one tilt series, two models"
                    )
    if params.population_series_only:
        chosen = inp.particles[on]
        say(
            f"{sname}: {len(chosen)} of {len(inp.particles)} particles sit on {tomo[on].nunique()} of the "
            f"population's {len(index)} tilt series; the other {int((~on).sum())} stay out"
        )
    else:
        chosen = inp.particles
        others = inp.particles[~on]
        if not others.empty:
            plans = mp.plan_sources(others, inp.tomograms, inp.tomograms_star)
            mp.rename_apart(plans, set(manifest["sources"]))
            items = {s["item"] for _, s in index.values()}
            clash = sorted(s.item for p in plans for s in p.series if s.item in items)
            if clash:
                raise ValueError(f"The new tilt series {clash[:3]} share a name with the population's: M matches by it")
            manifest = mp.write_sources(plans, m_dir, existing=manifest)
            for p in plans:
                n = sum(s.particles for s in p.series)
                say(f"Source {p.name} added: {len(p.series)} tilt series, {n} particles")
                create_source(params, m_dir, binds, p)
            start["manifest"] = manifest
    import_star = m_dir / f"particles_in_{sname}.star"
    mp.write_plain_star(mp.m_import_table(chosen, manifest), import_star)
    mask = make_species(params, m_dir, sname, inp, import_star, f"mask_binary_{sname}.mrc", binds)
    start["species"][sname] = new_entry(sname, inp, mask, start["round"], subset_ok=len(chosen) < len(inp.particles))


# ── before the round: the job's own species


def before_round(
    params: MRefineParams,
    m_dir: Path,
    species: Path,
    entry: dict,
    paths: dict[str, Path],
    binds: set[str],
    round_no: int,
) -> None:
    """A new mask, the trajectories resampled, the symmetry expanded: on the job's own species, in its copy."""
    name = entry["name"]
    population = mp.population_path(m_dir.parent)

    def tool(cmd: str) -> ToolCommand:
        return (
            ToolCommand(f"MTools {cmd}")
            .opt_path("--population", population, quote=True)
            .opt_path("--species", species, quote=True)
        )

    if params.changes_mask:
        mask = paths["input_mask"]
        require_producer_input(mask, "Mask")
        data, apix, info = mp.binary_mask(mask)
        own_apix = float(mp.species_params(species).get("PixelSize") or 0)
        if abs(apix - own_apix) > APIX_TOLERANCE:
            raise ValueError(f"The Mask job's map is at {apix:g} Å, the species {name} at {own_apix:g} Å")
        out = m_dir / f"mask_update_{name}.mrc"
        mp.write_map(out, data, apix)
        binds.add(str(mask.parent.resolve()))
        cmd = tool("update_mask").opt_path("--map", out, quote=True).opt("--threshold", 0.5)
        if params.mask_dilate:
            cmd.opt("--dilate", params.mask_dilate)
        if params.mask_center:
            cmd.flag("--center")
        mtools(params, m_dir, binds, cmd)
        entry["mask"] = info | {
            "from": str(mask),
            "round": round_no,
            "dilate_px": params.mask_dilate,
            "centred": params.mask_center,
        }
        say(f"{name}: a new mask from {mask}, binary, {info['fraction']:.1%} of its box")

    if params.temporal_samples and not params.makes_species:
        now = mp.temporal_samples(species)
        if params.temporal_samples != now:
            mtools(params, m_dir, binds, tool("resample_trajectories").opt("--samples", params.temporal_samples))
            say(f"{name}: trajectories resampled from {now} to {mp.temporal_samples(species)} temporal samples")

    if params.expand_symmetry:
        sym_before = mp.species_params(species).get("Symmetry") or ""
        args = params.expansion_args()
        cmd = tool("expand_symmetry")
        if args:
            cmd.raw(" ".join(shlex.quote(a) for a in args))
        mtools(params, m_dir, binds, cmd)
        sym_after = mp.species_params(species).get("Symmetry") or ""
        counts = mp.label_copies(mp.particles_star(species), f"r{round_no}|{sym_before}|{' '.join(args)}")
        entry["expansions"].append(
            {
                "round": round_no,
                "from": params.expand_from.strip() or sym_before,
                "to": sym_after,
                "helical": (
                    {"units": params.helical_units, "twist_deg": params.helical_twist, "rise_ang": params.helical_rise}
                    if params.helical_expansion
                    else None
                ),
                **counts,
            }
        )
        say(
            f"{name}: symmetry {sym_before} expanded, {counts['parents']} particles → {counts['copies']} copies "
            f"({counts['per_parent']} each), the species now {sym_after}"
        )


# ── after the round: every species handed out


def hand_out(entry: dict, species: Path, out_dir: Path, manifest: dict) -> None:
    """One species' half maps in its Refine 3D's box, its filtered map, its set; into the entry, where they went."""
    name = entry["name"]
    out_dir.mkdir(parents=True, exist_ok=True)
    box, apix = entry["box"], entry["pixel_size"]
    m_box = None
    for src, dst in zip((f"{name}_half1.mrc", f"{name}_half2.mrc"), HALF_NAMES, strict=True):
        data, m_apix = mp.read_map(species.parent / src)
        if abs(m_apix - apix) > APIX_TOLERANCE:
            raise ValueError(f"M's {src} is at {m_apix:g} Å, the Refine 3D's halves at {apix:g} Å")
        m_box = data.shape[0]
        mp.write_map(out_dir / dst, mp.window(data, box), apix)
    filtered = species.parent / f"{name}_filt.mrc"
    if filtered.is_file():
        mp.write_map(out_dir / FILTERED_NAME, mp.window(mp.read_map(filtered)[0], box), apix)
    else:
        say(f"WARNING: M wrote no {filtered.name}; {name} has no filtered map")
    base_particles, base_tomograms = _parse_optimisation_set(Path(entry["set_in"]))
    counts = mp.relion_set_from_m(
        mp.read_plain_star(mp.particles_star(species)),
        base_particles,
        base_tomograms,
        manifest,
        out_dir,
        subset_ok=bool(entry.get("subset_ok")),
    )
    sp = mp.species_params(species)
    entry.update(
        out_dir=str(out_dir),
        set=str(out_dir / SET_NAME),
        half1=str(out_dir / HALF_NAMES[0]),
        map=str(out_dir / FILTERED_NAME) if filtered.is_file() else None,
        m_box=m_box,
        fsc=str(species.parent / f"{name}_fsc.star"),
        species_file=str(species),
        particles=counts["particles"],
        tilt_series=counts["tilt_series"],
        symmetry=sp.get("Symmetry"),
        temporal_samples=mp.temporal_samples(species),
    )
    say(
        f"{name}: M's halves ({m_box} px) windowed to the Refine 3D's {box} px and its set "
        f"({counts['particles']} particles) in {out_dir}"
        + (f"; {counts['left_out']} of its set's particles were not given to M" if counts["left_out"] else "")
        + (
            f"; the set holds the mean of {entry['temporal_samples']} temporal samples"
            if entry["temporal_samples"] > 1
            else ""
        )
    )


def beyond_star_summary(series: list[dict]) -> dict:
    """What of the refined model RELION's tilt-series star cannot hold, over the tilt series."""
    staged = [s for s in series if s["stage_angle_max_deg"] > 0]
    magnified = [
        s for s in series if any(abs(a - b) > 1e-6 for a, b in zip(s["magnification"], (1, 0, 0, 1), strict=True))
    ]
    zernike = [s for s in series if any(s["zernike_odd"]) or any(s["zernike_even"])]
    return {
        "stage_angles": {
            "tilt_series": len(staged),
            "max_deg": max((s["stage_angle_max_deg"] for s in staged), default=0),
        },
        "magnification": {
            "tilt_series": len(magnified),
            "max_anisotropic_pct": max((s["magnification_pct"]["anisotropic_pct"] for s in magnified), default=0),
        },
        "zernike": {"tilt_series": len(zernike)},
    }


def print_series(series: list[dict]) -> None:
    say("Per tilt series, against the model the round started from:")
    for s in series:
        parts = [f"{s['particles']} particles"]
        if "defocus_shift_um" in s:
            parts.append(f"defocus {s['defocus_shift_um']:+.3f} ± {s['defocus_shift_sd_um']:.3f} µm")
        if s.get("warp_change_rms_ang") is not None:
            parts.append(f"image warp {'x'.join(map(str, s['warp_grid']))} moved {s['warp_change_rms_ang']:.1f} Å RMS")
        if s["stage_angle_max_deg"] > 0:
            parts.append(f"stage angles up to {s['stage_angle_max_deg']:.2f}°")
        mag = s["magnification_pct"]
        if abs(mag["isotropic_pct"]) > 1e-4 or abs(mag["anisotropic_pct"]) > 1e-4:
            parts.append(f"magnification {mag['isotropic_pct']:+.2f} % iso, {mag['anisotropic_pct']:.2f} % aniso")
        if s["cs_before_mm"] != s["cs_after_mm"]:
            parts.append(f"Cs {s['cs_before_mm']:g} → {s['cs_after_mm']:.3f} mm")
        if s.get("moved_rms_ang") is not None:
            parts.append(f"positions moved {s['moved_rms_ang']:.1f} Å RMS, turned {s['turned_rms_deg']:.2f}° RMS")
        others = {n: m["particles"] for n, m in (s.get("species") or {}).items()}
        if len(others) > 1:
            parts.append("species " + ", ".join(f"{n} {c}" for n, c in sorted(others.items())))
        say(f"  {s['name']}: " + " · ".join(parts))


# ── hand back only


def hand_back(paths: dict[str, Path], job_dir: Path, sname: str) -> None:
    """The job's species as the round it continues left it, as this job's outputs; no round."""
    record_path = paths["input_m"]
    require_producer_input(record_path, "The M round this one hands back")
    parent = json.loads(record_path.read_text())
    parent_dir = record_path.parent
    entries = species_entries(parent, parent_dir)
    own = own_species(sname, entries)
    e = entries[own]
    src = Path(e["out_dir"])
    for name in OUTPUTS:
        (job_dir / name).unlink(missing_ok=True)
    for name in (*HALF_NAMES, *SET_FILES):
        require_producer_input(src / name, f"{own}'s {name} from round {parent['round']}")
        shutil.copy2(src / name, job_dir / name)
    if (src / FILTERED_NAME).is_file():
        shutil.copy2(src / FILTERED_NAME, job_dir / FILTERED_NAME)
    write_optimisation_set(
        job_dir / SET_NAME, particles_star=job_dir / SET_FILES[0], tomograms_star=job_dir / SET_FILES[1]
    )
    e.update(
        out_dir=str(job_dir),
        set=str(job_dir / SET_NAME),
        half1=str(job_dir / HALF_NAMES[0]),
        map=str(job_dir / FILTERED_NAME) if (job_dir / FILTERED_NAME).is_file() else None,
    )
    changes = json.loads((parent_dir / CHANGES_NAME).read_text()) if (parent_dir / CHANGES_NAME).is_file() else {}
    for s in changes.get("tilt_series", []):
        if "species" in s:
            s.update((s["species"] or {}).get(own) or NO_MOVES)
    changes |= {
        "hand_back_of": str(parent_dir),
        "resolution_before_ang": e.get("resolution_before_ang"),
        "resolution_after_ang": e.get("resolution_after_ang"),
    }
    (job_dir / CHANGES_NAME).write_text(json.dumps(changes, indent=1))
    record = parent | {
        "version": RECORD_VERSION,
        "hand_back_of": str(parent_dir),
        "parent": str(parent_dir),
        "state_dir": str(parent.get("state_dir") or parent_dir),
        "job_species": own,
        "species_name": own,
        "species": e.get("species_file") or parent.get("species"),
        "refine3d_set": e.get("refine3d_set"),
        "set": e["set"],
        "box": e["box"],
        "pixel_size": e["pixel_size"],
        "m_box": e.get("m_box"),
        "resolution_before_ang": e.get("resolution_before_ang"),
        "resolution_after_ang": e.get("resolution_after_ang"),
        "fsc": e.get("fsc"),
        "mask": e.get("mask"),
        "particles": e.get("particles"),
        "gpus": 0,
        "seconds": 0,
        "all_species": entries,
    }
    (job_dir / ROUND_RECORD).write_text(json.dumps(record, indent=1))
    say(
        f"Handed back {own} as round {parent['round']} of {parent_dir} left it ({e.get('resolution_after_ang')} Å, "
        f"{e.get('particles')} particles), from {src}; no M round ran"
    )


def main():
    start_supervisor()
    print("--- SLURM JOB START (M refine) ---", flush=True)

    try:
        (state, params, context, job_dir, _project_path, _job_type) = get_driver_context(MRefineParams)
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        problems = params.run_problems()
        if problems:
            raise ValueError("; ".join(problems))
        paths = {k: Path(v) for k, v in context["paths"].items()}
        sname = species_name(state, params, context.get("instance_id"))
        if params.hand_back:
            hand_back(paths, job_dir, sname)
            print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
            return
        check_torchsharp(params, job_dir)

        m_dir = job_dir / mp.M_DIR
        for d in (m_dir, job_dir / SPECIES_OUT):
            if d.exists():
                say(f"{d} holds an earlier attempt of this job; it is built again")
                shutil.rmtree(d)
        for name in OUTPUTS:
            (job_dir / name).unlink(missing_ok=True)

        binds = {str(job_dir), *context.get("additional_binds", [])}
        if params.continue_from_m:
            start = follow_up(paths, job_dir)
        else:
            start = first_round(params, paths, job_dir, sname, binds)
        if params.adds_species:
            add_species(params, paths, job_dir, sname, start, binds)
        manifest = start["manifest"]
        index = mp.series_index(manifest)
        for source in manifest["sources"].values():
            binds.add(str(Path(source["project"]).resolve()))

        population = mp.population_path(job_dir)
        found = mp.species_files(population)
        entries = start["species"]
        if set(found) != set(entries):
            raise ValueError(
                f"The population holds the species {sorted(found)} and the rounds' records name {sorted(entries)}"
            )
        own = sname if params.makes_species else own_species(sname, found)
        before_round(params, m_dir, found[own], entries[own], paths, binds, start["round"])

        before = {n: mp.read_plain_star(mp.particles_star(p)) for n, p in found.items()}
        res_before = {n: resolution_of(mp.species_params(p)) for n, p in found.items()}
        gate = ctf_gate(params, res_before, own)
        for n, p in sorted(found.items()):
            say(f"Species {n}: {len(before[n])} particles, M's resolution {res_before[n]} Å ({p})")
        if gate["skipped"]:
            say(
                f"WARNING: M refines the CTF only from species finer than {params.ctf_minresolution:g} Å, and none "
                f"is ({', '.join(f'{n} {r:g} Å' for n, r in res_before.items() if r)}): "
                f"{', '.join(gate['switches'])} will refine nothing this round. Raise 'CTF from' to refine the CTF"
            )
        elif gate["not_fitted"]:
            say(f"The CTF is fitted from {', '.join(gate['fitted_on'])}; {', '.join(gate['not_fitted'])} take no part")

        n_gpus = allocated_gpu_count(1)
        tasks = task_root()
        cmd = (
            ToolCommand("MCore")
            .opt_path("--population", population, quote=True)
            .opt("--port", -1)
            .opt_path("--task_dir", tasks, quote=True)
            .raw("--devicelist " + " ".join(str(i) for i in range(n_gpus)))
            .opt("--perdevice", params.perdevice)
            .raw(" ".join(params.mcore_switches()))
        )
        if params.other_args.strip():
            cmd.raw(params.other_args.strip())
        say(f"Round {start['round']} on {n_gpus} GPU(s), {len(found)} species: {cmd}")
        tasks.mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        try:
            run_tool(cmd, tool_name=params.get_tool_name(), cwd=m_dir, binds=sorted(binds | {str(tasks)}))
        finally:
            shutil.rmtree(tasks, ignore_errors=True)
        seconds = time.time() - t0
        say(f"MCore finished after {seconds / 60:.0f} min")

        moves = {}
        for n, p in sorted(found.items()):
            e = entries[n]
            e["resolution_before_ang"] = res_before[n]
            e["resolution_after_ang"] = resolution_of(mp.species_params(p))
            say(f"{n}: M's resolution {res_before[n]} → {e['resolution_after_ang']} Å")
            hand_out(e, p, job_dir if n == own else job_dir / SPECIES_OUT / n, manifest)
            moves[n] = mp.m_moves(before[n], mp.read_plain_star(mp.particles_star(p)), manifest)

        series = []
        for name, (source, ser) in sorted(index.items()):
            after = Path(source["folder"]) / mp.PROCESSING_DIR / f"{ser['item']}.xml"
            entry = {"name": name, "item": ser["item"], "source": source["name"]}
            entry |= mp.model_changes(Path(ser["xml_before"]), after)
            entry |= moves[own].get(name) or NO_MOVES
            if len(found) > 1:
                entry["species"] = {n: m[name] for n, m in moves.items() if name in m}
            series.append(entry)
        beyond = beyond_star_summary(series)
        own_e = entries[own]
        per_species = {
            n: {k: e.get(k) for k in ("resolution_before_ang", "resolution_after_ang", "particles", "out_dir")}
            for n, e in entries.items()
        }
        changes = {
            "round": start["round"],
            "resolution_before_ang": own_e["resolution_before_ang"],
            "resolution_after_ang": own_e["resolution_after_ang"],
            "species": per_species,
            "beyond_star": beyond,
            "tilt_series": series,
        }
        (job_dir / CHANGES_NAME).write_text(json.dumps(changes, indent=1))
        record = {
            "version": RECORD_VERSION,
            "round": start["round"],
            "parent": start["parent"],
            "state_dir": str(job_dir),
            "job_species": own,
            "added_species": sname if params.adds_species else None,
            "refine3d_set": own_e["refine3d_set"],
            "set": own_e["set"],
            "population": str(population),
            "species": str(found[own]),
            "species_name": own,
            "box": own_e["box"],
            "pixel_size": own_e["pixel_size"],
            "m_box": own_e["m_box"],
            "resolution_before_ang": own_e["resolution_before_ang"],
            "resolution_after_ang": own_e["resolution_after_ang"],
            "fsc": own_e["fsc"],
            "switches": params.mcore_switches(),
            "command": str(cmd),
            "ctf_gate": gate,
            "beyond_star": beyond,
            "mask": own_e["mask"],
            "particles": own_e["particles"],
            "temporal_samples": own_e["temporal_samples"],
            "expansions": own_e["expansions"],
            "tilt_series": len(series),
            "sources": len(manifest["sources"]),
            "gpus": n_gpus,
            "seconds": round(seconds),
            "all_species": entries,
        }
        (job_dir / ROUND_RECORD).write_text(json.dumps(record, indent=1))
        print_series(series)
        if beyond["stage_angles"]["tilt_series"] or beyond["magnification"]["tilt_series"]:
            say(
                f"RELION's tilt-series star cannot hold this model: stage angles on "
                f"{beyond['stage_angles']['tilt_series']} tilt series (Warp Export and the bridge refuse them), "
                f"a magnification on {beyond['magnification']['tilt_series']} (RELION goes without it)"
            )
        say(f"Output: {job_dir / HALF_NAMES[0]} (for PostProcess), {job_dir / SET_NAME} (no images)")
        if len(found) > 1:
            say(f"The other species' outputs: {job_dir / SPECIES_OUT}/<name>/")
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

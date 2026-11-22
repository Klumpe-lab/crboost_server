#!/usr/bin/env python3
"""
Driver for one round of Warp's M (MTools sets the population up, MCore runs the round).

A first round builds M's population in the job's folder (services/particles/m_population.py): the sources, copies of
the Warp files of the tilt series the particles sit on, each checked; the particles as M reads them; the Mask job's
surface made binary again. Then the species: the Refine 3D's unfiltered halves at their own pixel size, the
symmetry and diameter the job holds, one temporal sample. A follow-up round copies an earlier round's population,
species and sources into its own folder. M's versioning is off (--dont_version): a round's state is its job folder,
and no job writes another's files.

Then one MCore round with the job's switches: its work queue in the node's memory (/dev/shm), its REST API off, the
GPUs the job was given (renumbered 0..N-1 in the container). After it:
- M's half maps windowed to the Refine 3D's box (half1.mrc, half2.mrc) and its filtered map (m_filtered.mrc), so a
  PostProcess reads them in the masks the trunk used;
- M's particles as a RELION 5 set without images (optimisation_set.star), its tomograms naming the source copies'
  per-tilt stars, which hold M's per-tilt CTF;
- per tilt series what the round changed against the model it started from (m_changes.json), and the round's
  record (m_round.json), which a follow-up reads.

M refines the CTF only of a species finer than --ctf_minresolution and prints nothing when it skips it; the driver
says it, in the log and in the record.
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
from pathlib import Path

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
from services.subtomo_merge import _parse_optimisation_set, _read_input_particles_lenient, _read_tomograms_star

ROUND_RECORD = "m_round.json"
CHANGES_NAME = "m_changes.json"
HALF_NAMES = ("half1.mrc", "half2.mrc")
FILTERED_NAME = "m_filtered.mrc"
SET_NAME = "optimisation_set.star"
LDD_NAME = "torchsharp_ldd.txt"
OUTPUTS = (*HALF_NAMES, FILTERED_NAME, ROUND_RECORD, CHANGES_NAME, SET_NAME, "particles.star", "tomograms.star")
APIX_TOLERANCE = 1e-3


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


def ctf_gate(params: MRefineParams, resolution: float | None) -> dict:
    switches = params.ctf_switches()
    skipped = bool(switches) and resolution is not None and resolution > params.ctf_minresolution
    return {
        "switches": [CTF_FLAGS[s] for s in switches],
        "min_resolution_ang": params.ctf_minresolution,
        "species_resolution_ang": resolution,
        "skipped": skipped,
    }


def resolution_of(params: dict[str, str]) -> float | None:
    try:
        value = float(params.get("GlobalResolution", ""))
    except ValueError:
        return None
    return value if math.isfinite(value) and value > 0 else None


def first_round(params: MRefineParams, paths: dict[str, Path], job_dir: Path, sname: str, binds: set[str]) -> dict:
    """The population, sources and species of a first round; what the round's record takes from it."""
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
    box, apix = b1[0], a1

    plans = mp.plan_sources(particles, tomograms, tomograms_star)
    m_dir = job_dir / mp.M_DIR
    m_dir.mkdir(parents=True)
    manifest = mp.write_sources(plans, m_dir)
    for p in plans:
        say(
            f"Source {p.name}: {len(p.series)} tilt series, {sum(s.particles for s in p.series)} particles, "
            f"{p.dims} px at {p.apix:g} Å, from {p.settings_from}"
        )
    mp.write_plain_star(mp.m_import_table(particles, manifest), m_dir / mp.IMPORT_STAR)

    mask_data, mask_apix, mask_info = mp.binary_mask(mask)
    if mask_data.shape != (box, box, box) or abs(mask_apix - apix) > APIX_TOLERANCE:
        raise ValueError(
            f"The Mask job's map is {mask_data.shape[::-1]} px at {mask_apix:g} Å, the half maps {box} px at "
            f"{apix:g} Å: make the mask on the Refine 3D's map"
        )
    mp.write_map(m_dir / mp.BINARY_MASK, mask_data, apix)
    say(
        f"M's mask: {mask.parent / mp.MASK_LOWPASS} at {mask_info['threshold']:.6g}, binary, "
        f"{mask_info['fraction']:.1%} of the box"
    )

    binds |= {str(half1.parent.resolve()), str(mask.parent.resolve())}
    population = mp.population_path(job_dir)
    mtools(
        params,
        m_dir,
        binds,
        ToolCommand("MTools create_population").opt_path("--directory", m_dir, quote=True).opt("--name", mp.POPULATION),
    )
    for p in plans:
        d = m_dir / mp.SOURCES_DIR / p.name
        mtools(
            params,
            m_dir,
            binds,
            ToolCommand("MTools create_source")
            .opt_path("--population", population, quote=True)
            .opt_path("--processing_settings", d / mp.SETTINGS_NAME, quote=True)
            .opt("--name", p.name)
            .opt_path("--files", d / mp.FILES_NAME, quote=True)
            .flag("--dont_version"),
        )
    species = (
        ToolCommand("MTools create_species")
        .opt_path("--population", population, quote=True)
        .opt("--name", sname)
        .opt("--diameter", f"{params.diameter_ang:g}")
        .opt("--sym", shlex.quote(params.symmetry.strip()))
        .opt("--temporal_samples", 1)
        .opt_path("--half1", half1, quote=True)
        .opt_path("--half2", half2, quote=True)
        .opt_path("--mask", m_dir / mp.BINARY_MASK, quote=True)
        .opt_path("--particles_relion", m_dir / mp.IMPORT_STAR, quote=True)
        .opt("--angpix_coords", 1)
        .opt("--angpix_shifts", 1)
        .flag("--dont_version")
    )
    if not params.use_denoiser:
        species.flag("--dont_use_denoiser")
    mtools(params, m_dir, binds, species)
    return {
        "round": 0,
        "parent": None,
        "refine3d_set": str(input_set),
        "base_particles": particles_star,
        "base_tomograms": tomograms_star,
        "box": box,
        "pixel_size": apix,
        "mask": mask_info | {"from": str(mask)},
        "manifest": manifest,
    }


def follow_up(paths: dict[str, Path], job_dir: Path) -> dict:
    record_path = paths["input_m"]
    require_producer_input(record_path, "The M round this one continues")
    parent = json.loads(record_path.read_text())
    parent_dir = record_path.parent
    say(f"Continuing round {parent['round']} of {parent_dir} (M at {parent.get('resolution_after_ang')} Å)")
    manifest = mp.copy_round(parent_dir, job_dir)
    base_particles, base_tomograms = _parse_optimisation_set(Path(parent["set"]))
    return {
        "round": int(parent["round"]) + 1,
        "parent": str(parent_dir),
        "refine3d_set": parent["refine3d_set"],
        "base_particles": base_particles,
        "base_tomograms": base_tomograms,
        "box": int(parent["box"]),
        "pixel_size": float(parent["pixel_size"]),
        "mask": parent.get("mask"),
        "manifest": manifest,
    }


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
        parts.append(f"positions moved {s['moved_rms_ang']:.1f} Å RMS, turned {s['turned_rms_deg']:.2f}° RMS")
        say(f"  {s['name']}: " + " · ".join(parts))


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
        check_torchsharp(params, job_dir)

        m_dir = job_dir / mp.M_DIR
        if m_dir.exists():
            say(f"{m_dir} holds an earlier attempt of this job; it is built again")
            shutil.rmtree(m_dir)
        for name in OUTPUTS:
            (job_dir / name).unlink(missing_ok=True)

        binds = {str(job_dir), *context.get("additional_binds", [])}
        if params.continue_from_m:
            start = follow_up(paths, job_dir)
        else:
            start = first_round(params, paths, job_dir, species_name(state, params, context.get("instance_id")), binds)
        manifest = start["manifest"]
        index = mp.series_index(manifest)
        for source in manifest["sources"].values():
            binds.add(str(Path(source["project"]).resolve()))

        population = mp.population_path(job_dir)
        species = mp.species_file(population)
        sname = mp.species_params(species).get("Name") or species.stem
        res_before = resolution_of(mp.species_params(species))
        gate = ctf_gate(params, res_before)
        say(f"Species {species} ({sname}), M's resolution {res_before} Å")
        if gate["skipped"]:
            say(
                f"WARNING: M refines the CTF only of a species finer than {params.ctf_minresolution:g} Å, and this one "
                f"is at {res_before:g} Å: {', '.join(gate['switches'])} will refine nothing this round. Raise 'CTF "
                "from' to refine the CTF"
            )

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
        say(f"Round {start['round']} on {n_gpus} GPU(s): {cmd}")
        tasks.mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        try:
            run_tool(cmd, tool_name=params.get_tool_name(), cwd=m_dir, binds=sorted(binds | {str(tasks)}))
        finally:
            shutil.rmtree(tasks, ignore_errors=True)
        seconds = time.time() - t0
        res_after = resolution_of(mp.species_params(species))
        say(f"MCore finished after {seconds / 60:.0f} min: M's resolution {res_before} → {res_after} Å")

        box, apix = start["box"], start["pixel_size"]
        m_box = None
        for src, dst in zip((f"{sname}_half1.mrc", f"{sname}_half2.mrc"), HALF_NAMES, strict=True):
            data, m_apix = mp.read_map(species.parent / src)
            if abs(m_apix - apix) > APIX_TOLERANCE:
                raise ValueError(f"M's {src} is at {m_apix:g} Å, the Refine 3D's halves at {apix:g} Å")
            m_box = data.shape[0]
            mp.write_map(job_dir / dst, mp.window(data, box), apix)
        say(f"M's halves ({m_box} px) windowed to the Refine 3D's {box} px: {job_dir / HALF_NAMES[0]}")
        filtered = species.parent / f"{sname}_filt.mrc"
        if filtered.is_file():
            mp.write_map(job_dir / FILTERED_NAME, mp.window(mp.read_map(filtered)[0], box), apix)
        else:
            say(f"WARNING: M wrote no {filtered.name}; the job has no filtered map")

        m_particles = mp.read_plain_star(species.parent / f"{sname}_particles.star")
        moved = mp.relion_set_from_m(m_particles, start["base_particles"], start["base_tomograms"], manifest, job_dir)
        series = []
        for name, (source, ser) in sorted(index.items()):
            after = Path(source["folder"]) / mp.PROCESSING_DIR / f"{ser['item']}.xml"
            entry = {"name": name, "item": ser["item"], "source": source["name"]}
            entry |= mp.model_changes(Path(ser["xml_before"]), after) | moved[name]
            series.append(entry)
        beyond = beyond_star_summary(series)
        changes = {
            "round": start["round"],
            "resolution_before_ang": res_before,
            "resolution_after_ang": res_after,
            "beyond_star": beyond,
            "tilt_series": series,
        }
        (job_dir / CHANGES_NAME).write_text(json.dumps(changes, indent=1))
        record = {
            "round": start["round"],
            "parent": start["parent"],
            "refine3d_set": start["refine3d_set"],
            "set": str(job_dir / SET_NAME),
            "population": str(population),
            "species": str(species),
            "species_name": sname,
            "box": box,
            "pixel_size": apix,
            "m_box": m_box,
            "resolution_before_ang": res_before,
            "resolution_after_ang": res_after,
            "fsc": str(species.parent / f"{sname}_fsc.star"),
            "switches": params.mcore_switches(),
            "command": str(cmd),
            "ctf_gate": gate,
            "beyond_star": beyond,
            "mask": start["mask"],
            "particles": sum(s["particles"] for s in series),
            "tilt_series": len(series),
            "sources": len(manifest["sources"]),
            "gpus": n_gpus,
            "seconds": round(seconds),
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
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

"""The optimisation set a RELION tilt-image program should read: the input's, or one that carries Warp's local
geometry as trajectories (services/particles/warp_geometry.py; HOTFIXES H26). A set marked as carrying RELION's own
model (frame alignment, roadmap 53 §15.11) is handed on as it is, once checked.

Called by every driver that hands RELION the tilt images (Reconstruct particle, CTF refine, Subtomo extraction,
pick-list extraction) before it builds its command. The computation runs in the miss_alignment container
(containers/miss_alignment/warp_trajectories.py: warpylib), as the tilt filter's network runs in its own.
"""

from __future__ import annotations

import shlex
from pathlib import Path

from drivers.driver_base import run_tool
from services.configs.config_service import REPO_ROOT
from services.particles import warp_geometry as wg

SCRIPT = REPO_ROOT / "containers" / "miss_alignment" / "warp_trajectories.py"


def _say(msg: str) -> None:
    print(f"[WARP GEOMETRY] {msg}", flush=True)


def carry_warp_geometry(input_set: Path, job_dir: Path, binds=()) -> Path:
    """`input_set` when its tilt series hold nothing beyond the rigid model the star holds, or when it already
    names this bridge's trajectories for all its particles; else a new optimisation set in `job_dir` naming the
    trajectories computed here (and, for particles without names, a named copy of the particles). Tilts that hold
    no alignment (WarpTools ts_import left them out) are first left out of a copy of their stars."""
    input_set, dropped = wg.drop_unaligned_tilts(input_set, job_dir)
    for name, movies in dropped.items():
        _say(
            f"{name}: {len(movies)} tilt(s) hold no alignment (WarpTools ts_import left them out; Warp's model has "
            f"no such tilt), so RELION reads its star without them: {', '.join(movies)}"
        )
    p = wg.plan(input_set)
    if p.unlocated:
        _say(
            f"WARN: no Warp XML found for {len(p.unlocated)} tilt series ({', '.join(p.unlocated[:5])}"
            f"{' …' if len(p.unlocated) > 5 else ''}): RELION places their particles by the tilt-series star alone"
        )
    if p.magnified:
        _say(
            f"WARN: {len(p.magnified)} tilt series hold a magnification correction (an M refine's): it moves no "
            "particle, Warp applies it where it back-projects, and RELION's star holds none, so RELION reconstructs "
            "them without it"
        )
    if p.relion_model or p.marked:
        wg.check_marked(p)
        if p.relion_model:
            _say(
                f"{len(p.relion_model)} tilt series carry RELION's own model (frame alignment): their stars and motion "
                "go to RELION as they are, without Warp's local geometry"
            )
        if p.series:
            _say(f"{len(p.series)} tilt series keep Warp's model, with the bridge's offsets the set carries")
        _say(f"the set is handed on as it is (trajectories: {p.trajectories or 'none'})")
        return input_set
    if not p.needed:
        if p.rigid_only:
            _say(f"{len(p.rigid_only)} tilt series: Warp's model is the star's rigid one; nothing to carry")
        return input_set
    _say(f"{len(p.series)} tilt series hold local geometry beyond the star's rigid model (Warp applies it)")
    if wg.carried(p):
        _say(f"the input set already carries it for every particle: {p.trajectories}")
        return input_set
    if p.trajectories is not None and p.trajectories.name != wg.TRAJECTORIES_NAME:
        raise ValueError(
            f"The input set names trajectories of its own ({p.trajectories}), and its tilt series hold local "
            "geometry Warp applies and RELION's star cannot (above). The two cannot be combined yet: RELION would "
            "place the particles by one or the other"
        )

    job_dir.mkdir(parents=True, exist_ok=True)
    for name in (wg.TRAJECTORIES_NAME, wg.REPORT_NAME):
        (job_dir / name).unlink(missing_ok=True)  # an earlier attempt's output must not pass for this one's
    particles_star = wg.named_particles(p, job_dir)
    if particles_star != p.particles_star:
        _say(f"the particles have no unique names; named <tomogram>/<n> in {particles_star}")
    manifest = wg.write_manifest(p, job_dir, particles_star)
    q = shlex.quote
    command = f"cd {q(str(job_dir))} && python {q(str(SCRIPT))} --manifest {q(str(manifest))}"
    folders = {SCRIPT.parent, job_dir, particles_star.parent, p.tomograms_star.parent}
    folders |= {s.xml.parent for s in p.series} | {s.tilt_star.parent for s in p.series}
    run_tool(command, tool_name=wg.WARP_GEOMETRY_TOOL, cwd=job_dir, binds=[*binds, *sorted(folders)])
    if not (job_dir / wg.TRAJECTORIES_NAME).is_file():
        raise RuntimeError(f"warp_trajectories.py wrote no {job_dir / wg.TRAJECTORIES_NAME}; see the log above")
    for line in wg.report_lines(job_dir, p):
        _say(line)
    out = wg.write_bridged_set(p, job_dir, particles_star)
    _say(f"RELION reads {out}")
    return out

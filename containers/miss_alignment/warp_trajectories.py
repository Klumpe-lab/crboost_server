#!/usr/bin/env python3
"""Warp's local tilt-series geometry as RELION per-particle trajectories (HOTFIXES H26).

Runs with the Python of the miss_alignment container (warpylib, torch, numpy, starfile) and imports nothing from
crboost. crboost (services/particles/warp_geometry.py, drivers/warp_bridge.py) finds each tilt series' Warp XML,
checks it against the tilt-series star RELION reads, and hands this script a manifest:

    {"particles": "/abs/particles.star",     # rlnTomoName, rlnTomoParticleName, rlnCenteredCoordinate{X,Y,Z}Angst,
                                             # rlnOrigin{X,Y,Z}Angst when refined
     "out": "/abs/warp_trajectories.star",
     "report": "/abs/warp_trajectories.json",
     "series": [{"name": "<rlnTomoName>", "xml": "/abs/<ts>.xml",
                 "movies": ["<movie of star row 0>", ...],   # the tilt-series star's rows: RELION's frame order
                 "image_dims_ang": [x, y], "volume_dims_ang": [x, y, z]}, ...]}

RELION places a particle in tilt f by the star's rigid model; Warp by the same model plus what the star cannot hold
(GridMovementX/Y, volume warps, level angles). Per particle and tilt this writes the shortest 3-D offset whose rigid
projection is Warp's full position minus its rigid one. RELION adds a particle's trajectory to its position before
it projects it, so the particle then lands where Warp's model puts it. The format is the one RELION's Polish writes
(`rlnTomoTrajectoriesFile`): data_general with rlnParticleNumber, then one block per particle, named after its
rlnTomoParticleName, with rlnOrigin{X,Y,Z}Angst per frame.
"""

import argparse
import copy
import json
from pathlib import Path

import numpy as np
import starfile
import torch
from warpylib.cubic_grid import CubicGrid
from warpylib.tilt_series.core import TiltSeries
from warpylib.tilt_series.io import load_meta
from warpylib.tilt_series.positions import get_position_in_all_tilts_single

CHUNK = 4096  # particles per warpylib call
STEP_ANG = 100.0  # finite-difference step for the rigid projection's linear part (it is affine: exact)


def particle_table(path: str):
    for block in starfile.read(path, always_dict=True).values():
        if hasattr(block, "columns") and "rlnTomoName" in block.columns:
            return block
    raise ValueError(f"{path}: no particle table with rlnTomoName")


def load_series(entry: dict) -> TiltSeries:
    ts = TiltSeries()
    load_meta(ts, entry["xml"])
    # crboost checked these against the XML's own and the tomogram row; set after load_meta, which would
    # otherwise keep whatever the XML says (a thin XML says zero).
    ts.image_dimensions_physical = torch.tensor(entry["image_dims_ang"], dtype=torch.float32)
    ts.volume_dimensions_physical = torch.tensor(entry["volume_dims_ang"], dtype=torch.float32)
    for axis in "xyz":
        if float(getattr(ts, f"grid_angle_{axis}").values.abs().max()) > 0:
            raise ValueError(
                f"{entry['name']}: the XML holds per-tilt angle corrections (GridAngle{axis.upper()}); a trajectory "
                "moves a particle but cannot turn it"
            )
    return ts


def rigid_copy(ts: TiltSeries) -> TiltSeries:
    """The model the RELION star holds: the same tilt series without its grids and level angles."""
    rigid = copy.deepcopy(ts)
    rigid.grid_movement_x = CubicGrid((1, 1, 1))
    rigid.grid_movement_y = CubicGrid((1, 1, 1))
    for axis in "xyz":
        setattr(rigid, f"grid_volume_warp_{axis}", CubicGrid((1, 1, 1, 1)))
    rigid.level_angle_x = 0.0
    rigid.level_angle_y = 0.0
    return rigid


def frame_order(ts: TiltSeries, movies: list[str], name: str) -> list[int]:
    """For each row of the tilt-series star, the XML's tilt index, matched by movie file name (stem as a fallback)."""
    xml_movies = [Path(p).name for p in ts.tilt_movie_paths]
    by_name = {m: i for i, m in enumerate(xml_movies)}
    by_stem = {Path(m).stem: i for i, m in enumerate(xml_movies)}
    order = []
    for movie in movies:
        base = Path(movie).name
        idx = by_name.get(base, by_stem.get(Path(base).stem))
        if idx is None:
            raise ValueError(f"{name}: the star's tilt {base} has no tilt in the XML")
        order.append(idx)
    return order


def least_norm_inverse(rigid: TiltSeries) -> np.ndarray:
    """(tilts, 3, 2): per tilt, the 3-D offset of least length that the rigid projection maps onto a 2-D one."""
    centre = rigid.volume_dimensions_physical.numpy().astype(np.float64) / 2
    points = torch.tensor(np.vstack([centre, centre + np.eye(3) * STEP_ANG]), dtype=torch.float32)
    projected = get_position_in_all_tilts_single(rigid, points).numpy()[..., :2].astype(np.float64)  # (4, T, 2)
    m = np.stack([(projected[k + 1] - projected[0]) / STEP_ANG for k in range(3)], axis=-1)  # (T, 2, 3)
    mt = m.transpose(0, 2, 1)
    return mt @ np.linalg.inv(m @ mt)


def offsets(entry: dict, particles) -> tuple[np.ndarray, dict]:
    """(particles, frames, 3) offsets in the star's frame order, and the series' line of the report."""
    ts = load_series(entry)
    rigid = rigid_copy(ts)
    order = frame_order(ts, entry["movies"], entry["name"])
    pinv = least_norm_inverse(rigid)[order]  # (F, 3, 2)

    centred = particles[[f"rlnCenteredCoordinate{a}Angst" for a in "XYZ"]].to_numpy(dtype=np.float64)
    origin_cols = [f"rlnOrigin{a}Angst" for a in "XYZ"]
    origin = particles[origin_cols].to_numpy(dtype=np.float64) if set(origin_cols) <= set(particles.columns) else 0.0
    positions = centred - origin + ts.volume_dimensions_physical.numpy().astype(np.float64) / 2  # Warp's frame, Å

    out = np.zeros((len(positions), len(order), 3))
    image = np.zeros((len(positions), len(order), 2))
    for start in range(0, len(positions), CHUNK):
        chunk = torch.tensor(positions[start : start + CHUNK], dtype=torch.float32)
        full = get_position_in_all_tilts_single(ts, chunk).numpy()[..., :2].astype(np.float64)
        flat = get_position_in_all_tilts_single(rigid, chunk).numpy()[..., :2].astype(np.float64)
        delta = (full - flat)[:, order, :]  # (n, F, 2), Å in the image
        image[start : start + CHUNK] = delta
        out[start : start + CHUNK] = np.einsum("fij,nfj->nfi", pinv, delta)
    size = np.linalg.norm(image, axis=-1)
    line = {
        "particles": len(positions),
        "frames": len(order),
        "offset_rms_ang": float(np.sqrt((size**2).mean())) if size.size else 0.0,
        "offset_max_ang": float(size.max()) if size.size else 0.0,
        "level_angles_deg": [float(ts.level_angle_x), float(ts.level_angle_y)],
        "movement_grid": list(ts.grid_movement_x.dimensions),
    }
    return out, line


def write_trajectories(path: Path, names: list[str], tracks: list[np.ndarray]) -> None:
    with open(path, "w") as f:
        f.write(f"# version 50001\n\ndata_general\n\n_rlnParticleNumber {len(names)}\n\n")
        for name, track in zip(names, tracks, strict=True):
            f.write(f"\n# version 50001\n\ndata_{name}\n\nloop_\n")
            f.write("_rlnOriginXAngst #1\n_rlnOriginYAngst #2\n_rlnOriginZAngst #3\n")
            f.write("".join(f"{x:.6f} {y:.6f} {z:.6f}\n" for x, y, z in track))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--manifest", required=True)
    manifest = json.loads(Path(ap.parse_args().manifest).read_text())

    particles = particle_table(manifest["particles"])
    names_all = particles["rlnTomoParticleName"].astype(str)
    if names_all.duplicated().any():
        repeats = names_all[names_all.duplicated()].tolist()[:3]
        raise ValueError(f"{manifest['particles']}: rlnTomoParticleName repeats ({repeats})")

    names: list[str] = []
    tracks: list[np.ndarray] = []
    report: dict = {"series": {}}
    tomo = particles["rlnTomoName"].astype(str)
    for entry in manifest["series"]:
        sub = particles[tomo == entry["name"]]
        if sub.empty:
            continue
        d, line = offsets(entry, sub)
        names += sub["rlnTomoParticleName"].astype(str).tolist()
        tracks += list(d)
        report["series"][entry["name"]] = line
    write_trajectories(Path(manifest["out"]), names, tracks)
    report["particles"] = len(names)
    Path(manifest["report"]).write_text(json.dumps(report, indent=1))
    print(f"{len(names)} particles over {len(report['series'])} tilt series -> {manifest['out']}", flush=True)


if __name__ == "__main__":
    main()

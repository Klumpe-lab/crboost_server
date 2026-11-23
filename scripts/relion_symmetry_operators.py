"""Read RELION's own symmetry operators into services/particles/data/relion_symmetry.json.

`relion_particle_symmetry_expand --sym G` on a star of one particle with zero angles writes one row per operator of
G, and each row's rlnAngleRot/Tilt/Psi is that operator in RELION's convention: its axes, its order, its setting
of the icosahedral groups (I1 … I4). Sub-particle sites (services/particles/sites.py) are built from these, never
from a table typed in, so a site's axis is where RELION put it in the map.

Run it again when a group is missing (a larger --max-n) or after a RELION upgrade, with RELION's command prefix:

    ./crboost python scripts/relion_symmetry_operators.py --prefix "apptainer exec /path/to/relion.sif"
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "services" / "particles" / "data" / "relion_symmetry.json"
FIXED = ("T", "O", "I", "I1", "I2", "I3", "I4")

IDENTITY_STAR = """
data_optics

loop_
_rlnOpticsGroup #1
_rlnOpticsGroupName #2
_rlnImagePixelSize #3
_rlnImageSize #4
_rlnImageDimensionality #5
_rlnVoltage #6
_rlnSphericalAberration #7
_rlnAmplitudeContrast #8
1 opticsGroup1 1.0 64 3 300 2.7 0.07

data_particles

loop_
_rlnAngleRot #1
_rlnAngleTilt #2
_rlnAnglePsi #3
_rlnOriginXAngst #4
_rlnOriginYAngst #5
_rlnOriginZAngst #6
_rlnOpticsGroup #7
_rlnImageName #8
0.0 0.0 0.0 0.0 0.0 0.0 1 one.mrcs
"""


def _angles(star: Path) -> list[list[float]]:
    """The three angle columns of the expanded star's particle rows, in RELION's order."""
    rows, columns, in_particles = [], [], False
    for line in star.read_text().splitlines():
        s = line.strip()
        if s.startswith("data_"):
            in_particles, columns = s == "data_particles", []
        elif in_particles and s.startswith("_rln"):
            columns.append(s.split()[0][1:])
        elif in_particles and s and not s.startswith(("loop_", "#")):
            cells = s.split()
            rows.append([float(cells[columns.index(c)]) for c in ("rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi")])
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prefix", default="", help='what runs RELION, e.g. "apptainer exec /path/to/relion.sif"')
    ap.add_argument("--max-n", type=int, default=40, help="the largest n of the Cn and Dn groups read")
    args = ap.parse_args()
    prefix = shlex.split(args.prefix)
    groups = [f"C{n}" for n in range(2, args.max_n + 1)] + [f"D{n}" for n in range(1, args.max_n + 1)] + list(FIXED)
    version = subprocess.run(
        [*prefix, "relion_particle_symmetry_expand", "--version"], capture_output=True, text=True, check=False
    )
    # C1 has the identity alone, and RELION refuses to expand it ("Nothing to do").
    out: dict[str, list[list[float]]] = {"C1": [[0.0, 0.0, 0.0]]}
    with tempfile.TemporaryDirectory(dir=Path.home()) as tmp:
        tmp = Path(tmp)
        (tmp / "one.star").write_text(IDENTITY_STAR)
        for g in groups:
            res = subprocess.run(
                [*prefix, "relion_particle_symmetry_expand", "--i", "one.star", "--o", f"{g}.star", "--sym", g],
                cwd=tmp,
                capture_output=True,
                text=True,
                check=False,
            )
            if res.returncode != 0 or not (tmp / f"{g}.star").exists():
                print(f"{g}: RELION failed\n{res.stdout[-400:]}\n{res.stderr[-400:]}", file=sys.stderr)
                return 1
            out[g] = _angles(tmp / f"{g}.star")
            print(f"{g}: {len(out[g])} operators")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "source": "relion_particle_symmetry_expand --sym <group> on one particle with zero angles",
        "relion": next((ln.strip() for ln in (version.stdout + version.stderr).splitlines() if "version" in ln), ""),
        "made_by": "scripts/relion_symmetry_operators.py",
        "groups": out,
    }
    body = ",\n".join(f"    {json.dumps(g)}: {json.dumps(ops, separators=(',', ':'))}" for g, ops in out.items())
    head = {k: v for k, v in record.items() if k != "groups"}
    text = json.dumps(head, indent=2)[:-2] + ',\n  "groups": {\n' + body + "\n  }\n}\n"
    OUT.write_text(text)
    json.loads(OUT.read_text())
    print(f"wrote {OUT.relative_to(REPO)} ({OUT.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

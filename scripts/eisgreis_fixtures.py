"""Write EisGreis's tests/fixtures/crboost/{open,bundle}.json with crboost's own writers, on a small fake project.

EisGreis's link tests read what crboost writes: when the request or the bundle changes here, run this and send the
new fixtures to EisGreis with the version its link reads (EisGreis's docs/crboost-protocol.md).

    ./crboost python scripts/eisgreis_fixtures.py <EisGreis checkout> [<scratch folder>]
"""

import json
import shutil
import sys
from pathlib import Path

import mrcfile
import numpy as np
import pandas as pd
import starfile

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.curation.session_service import CurationSessionService
from services.segmentation.volumes import DISPLAY_THRESHOLD
from services.visualization.napari_bundle import _build, _PickSource, _Plan

eisgreis = Path(sys.argv[1])
root = Path(sys.argv[2]) if len(sys.argv) > 2 else Path.home() / ".crboost" / "eisgreis_fixtures"
shutil.rmtree(root, ignore_errors=True)
data, session = root / "data", root / "session"
project = data / "demo_project"
recon = project / "External" / "job007" / "warp_tiltseries" / "reconstruction" / "TS_01_10.00Apx.mrc"
denoised = project / "External" / "job009" / "denoised" / "TS_01_10.00Apx.mrc"
seg = project / "External" / "job011" / "segmentations" / "TS_01__membrane.mrc"
star = project / "External" / "job013" / "candidates.star"
for p in (recon, denoised, seg, star):
    p.parent.mkdir(parents=True, exist_ok=True)
(project / "project_params.json").write_text("{}")
session.mkdir(parents=True)

shape, px = (24, 40, 36), 10.0
for p in (recon, denoised):
    with mrcfile.new(str(p), overwrite=True) as m:
        m.set_data(np.zeros(shape, dtype=np.float32))
        m.voxel_size = px
seg_shape, seg_px, full_scale = (12, 20, 18), 20.0, 127.0
with mrcfile.new(str(seg), overwrite=True) as m:
    m.set_data(np.zeros(seg_shape, dtype=np.int8))
    m.voxel_size = seg_px
# Centered Å, RELION 5: two picks of TS_01, one of another tomogram (left out by rlnTomoName).
starfile.write(
    {
        "particles": pd.DataFrame(
            {
                "rlnTomoName": ["TS_01", "TS_01", "TS_02"],
                "rlnCenteredCoordinateXAngst": [0.0, 50.0, 0.0],
                "rlnCenteredCoordinateYAngst": [0.0, -40.0, 0.0],
                "rlnCenteredCoordinateZAngst": [0.0, 30.0, 0.0],
            }
        )
    },
    star,
    overwrite=True,
)

plan = _Plan(project=project, volume=recon, tomo_name="TS_01")
plan.volumes = [
    {"name": "reconstruction (tsReconstruct)", "kind": "reconstruction", "path": str(recon)},
    {"name": "denoised · cryocare (denoisePredict)", "kind": "denoised", "path": str(denoised)},
]
plan.segmentations = [
    {
        "name": "membrane (segment)",
        "path": str(seg),
        "voxel_size": seg_px,
        "shape_zyx": list(seg_shape),
        "level": DISPLAY_THRESHOLD * full_scale,
        "full_scale": full_scale,
        "color": "#f50057",
        "visible": True,
    }
]
plan.picks = [
    _PickSource(
        name="ribosome · candidates (tmextractcand)",
        species="ribosome",
        source="candidates tmextractcand",
        color="#ff1744",
        box_size=300.0,
        star=star,
    )
]
plan.notes = ["tmextractcand__other: belongs to no registered species: not shown"]
bundle = _build(plan)
seq = CurationSessionService._write_napari_request(
    session, [{"name": "TS_01", "path": str(recon)}], "TS_01", bundle, protocol=1
)

out = eisgreis / "tests" / "fixtures" / "crboost"
out.mkdir(parents=True, exist_ok=True)
for name in ("open.json", "bundle.json"):
    text = (session / name).read_text().replace(str(data), "${DATA}").replace(str(session), "${SESSION}")
    (out / name).write_text(json.dumps(json.loads(text), indent=2) + "\n")
    print(name, "->", out / name)

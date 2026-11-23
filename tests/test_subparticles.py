"""Sub-particles (roadmap 71): the conventions against RELION's own numbers, and what a placement must hold.

No site and no data: `./crboost ci` runs it with the repo tests. The numbers marked RELION were written by RELION
5.0.1 on a two-row star (roadmap 71 §10): row b has angles (30, 40, 50) and origin (1, 2, 3) Å.
"""

import numpy as np
import pandas as pd
import pytest

from services.particles import sites, subparticles
from services.particles.coords import CENTERED_COLS
from services.particles.poses import compose, euler_angles, euler_matrices, frame_on, recentred_origin

pytestmark = pytest.mark.repo

ROW_B = euler_matrices([30.0], [40.0], [50.0])
ORIGIN_B = np.array([[1.0, 2.0, 3.0]])


def test_recentre_is_relions():
    """`relion_star_handler --center --center_X 10` at 1.55 Å/px moved row b's origin to these."""
    got = recentred_origin(ORIGIN_B, ROW_B, [15.5, 0.0, 0.0])
    assert np.allclose(got, [[0.327113, 14.858775, -5.62839]], atol=2e-6)


def test_a_copy_sits_where_the_recentre_puts_it():
    """centre + A·v is the coordinate minus the recentred origin: the two say the same point."""
    coord = np.array([[100.0, 200.0, 300.0]])
    v = np.array([[15.5, -3.0, 7.0]])
    _poses, centres = compose(ROW_B, coord - ORIGIN_B, np.eye(3)[None], v)
    assert np.allclose(centres, coord - recentred_origin(ORIGIN_B, ROW_B, v[0]))


def test_helix_steps_are_relions():
    """`relion_particle_symmetry_expand --helix --twist -27.69 --rise 9.55` gave row b these angles and X/Y
    origins. (It leaves the Z origin alone, which is why its expansion is not used on sub-tomograms.)"""
    site = sites.helix_site(twist_deg=-27.69, rise_ang=9.55, radius_ang=1e-9, half_window_ang=10.0)
    poses, centres = compose(ROW_B, -ORIGIN_B, site.rotations, site.offsets)
    rot, tilt, psi = euler_angles(poses)
    relion = {2.31: (-2.94583, 6.702457), 30.0: (1.0, 2.0), 57.69: (4.94583, -2.70246)}
    assert sorted(np.round(rot, 2)) == sorted(relion)
    for r, t, p, c in zip(rot, tilt, psi, centres, strict=True):
        assert np.allclose([t, p], [40.0, 50.0], atol=1e-6)
        assert np.allclose(-c[:2], relion[round(float(r), 2)], atol=2e-5)


def test_euler_round_trip():
    rng = np.random.default_rng(0)
    rot, psi = rng.uniform(-180, 180, 500), rng.uniform(-180, 180, 500)
    tilt = np.degrees(np.arccos(rng.uniform(-1, 1, 500)))
    m = euler_matrices(rot, tilt, psi)
    assert np.allclose(euler_matrices(*euler_angles(m)), m, atol=1e-9)


@pytest.mark.parametrize(
    "group,order", [("C1", 1), ("C6", 6), ("D7", 14), ("T", 12), ("O", 24), ("I1", 60), ("I2", 60), ("I4", 60)]
)
def test_operators_are_a_group(group, order):
    ops = sites.operators(group)
    assert len(ops) == order
    assert np.allclose(ops[0], np.eye(3), atol=1e-6)
    for a in ops[:6]:
        for b in ops:
            assert any(np.allclose(a @ b, c, atol=1e-5) for c in ops)


def test_axes_are_named_by_what_the_group_carries_onto_itself():
    assert [(c.label, c.copies) for c in sites.axis_classes("I1")] == [("5-fold", 12), ("3-fold", 20), ("2-fold", 30)]
    assert [(c.label, c.copies) for c in sites.axis_classes("O")] == [("4-fold", 6), ("3-fold", 8), ("2-fold", 12)]
    # A tetrahedron's 3-fold axis has two different ends, a D7's 2-folds are of two kinds, a C6's axis has two ends.
    assert [(c.label, c.copies) for c in sites.axis_classes("T")] == [("3-fold a", 4), ("3-fold b", 4), ("2-fold", 6)]
    assert [(c.label, c.copies) for c in sites.axis_classes("D7")] == [("7-fold", 2), ("2-fold a", 7), ("2-fold b", 7)]
    assert [(c.label, c.copies) for c in sites.axis_classes("C6")] == [("6-fold a", 1), ("6-fold b", 1)]
    assert sites.axis_classes("C1") == ()


@pytest.mark.parametrize("axis,n,own", [("5-fold", 12, "C5"), ("3-fold", 20, "C3"), ("2-fold", 30, "C2")])
def test_a_point_group_site_gives_the_distinct_positions(axis, n, own):
    site = sites.point_group_site("I1", axis=axis, radius_ang=238.0)
    assert (site.n, site.own_symmetry) == (n, own)
    assert np.allclose(np.linalg.norm(site.offsets, axis=1), 238.0)
    # Each copy's own Z is the line it sits on.
    assert np.allclose(site.rotations @ np.array([0.0, 0.0, 1.0]), site.offsets / 238.0, atol=1e-6)
    # Made again from what its description keeps, it is the same site to the digit.
    again = sites.site_of(site.description)
    assert again.description == site.description and again.digest == site.digest
    assert np.array_equal(again.offsets, site.offsets) and np.array_equal(again.rotations, site.rotations)


def test_a_direction_off_every_axis_takes_the_whole_group():
    site = sites.point_group_site("I1", direction=[0.3, 0.5, 0.8], radius_ang=100.0)
    assert (site.n, site.own_symmetry, site.description["axis"]) == (60, "C1", sites.DIRECTION)


def test_what_is_not_given_is_refused():
    with pytest.raises(ValueError, match="radius"):
        sites.point_group_site("I1", axis="5-fold", radius_ang=None)
    with pytest.raises(ValueError, match="no axis"):
        sites.point_group_site("I1", axis="7-fold", radius_ang=100)
    with pytest.raises(ValueError, match="not been read from RELION"):
        sites.operators("C99")
    with pytest.raises(ValueError, match="twist"):
        sites.helix_site(twist_deg=None, rise_ang=None, radius_ang=100, half_window_ang=50)


def test_frame_on_puts_z_on_the_direction():
    for d in ([0, 0, 1], [0, 0, -1], [1, 2, 3], [-0.2, 0.9, -0.4]):
        q = frame_on(d)
        assert np.allclose(q @ q.T, np.eye(3), atol=1e-12) and np.isclose(np.linalg.det(q), 1.0)
        assert np.allclose(q[:, 2], np.asarray(d, float) / np.linalg.norm(d), atol=1e-12)


def _capsids(n: int = 7, seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(
        {
            "rlnTomoName": ["t1"] * (n - 2) + ["t2"] * 2,
            "rlnTomoParticleName": [f"t/{i}" for i in range(n)],
            "rlnImageName": [f"{i}.mrcs" for i in range(n)],
            "rlnAngleRot": rng.uniform(-180, 180, n),
            "rlnAngleTilt": np.degrees(np.arccos(rng.uniform(-1, 1, n))),
            "rlnAnglePsi": rng.uniform(-180, 180, n),
            "rlnOriginXAngst": rng.normal(0, 2, n),
            "rlnOriginYAngst": rng.normal(0, 2, n),
            "rlnOriginZAngst": rng.normal(0, 2, n),
            "rlnRandomSubset": [1 + (i % 2) for i in range(n)],
            "rlnLCCmax": rng.uniform(0, 1, n),
            "cbPickUid": [f"u{i:012x}" for i in range(n)],
            "rlnClassNumber": 1,
        }
    )
    xyz = rng.uniform(-2000, 2000, (n, 3))
    for i, c in enumerate(CENTERED_COLS):
        df[c] = xyz[:, i]
    return df


PENTAMER = {"kind": "point_group", "group": "I1", "axis": "5-fold", "radius_ang": 238.0}


def test_placement_on_capsids():
    parents = _capsids()
    out = subparticles.place(parents, PENTAMER)
    rows = out.rows
    assert (out.n_parents, out.per_parent, len(rows)) == (7, 12, 84)
    # 238 Å from the parent's centre, which is its coordinate minus its origin.
    centre = (
        parents[CENTERED_COLS].to_numpy()
        - parents[["rlnOriginXAngst", "rlnOriginYAngst", "rlnOriginZAngst"]].to_numpy()
    )
    d = np.linalg.norm(rows[CENTERED_COLS].to_numpy() - np.repeat(centre, 12, axis=0), axis=1)
    assert np.allclose(d, 238.0)
    assert (rows[["rlnOriginXAngst", "rlnOriginYAngst", "rlnOriginZAngst"]] == 0).all().all()
    # Who it came from, and its half: the parent's.
    assert rows["cbPickUid"].is_unique and not set(rows["cbPickUid"]) & set(parents["cbPickUid"])
    assert (rows["cbParentUid"].to_numpy() == np.repeat(parents["cbPickUid"].to_numpy(), 12)).all()
    assert (rows["rlnRandomSubset"].to_numpy() == np.repeat(parents["rlnRandomSubset"].to_numpy(), 12)).all()
    assert (rows.groupby("cbAssembly")["rlnRandomSubset"].nunique() == 1).all()
    # The parent's images and what its refinement measured of it are not carried; its score is.
    assert not {"rlnImageName", "rlnTomoParticleName", "rlnClassNumber"} & set(rows.columns)
    assert "rlnLCCmax" in rows.columns
    assert subparticles.place(parents, PENTAMER).rows.equals(rows)


def test_copies_are_valid_parents():
    """A pentamer set expanded again (its five monomers): the operation takes its own output."""
    pentamers = subparticles.place(_capsids(), PENTAMER).rows
    monomer = {"kind": "point_group", "group": "C5", "direction": [1, 0, 0], "radius_ang": 40.0}
    out = subparticles.place(pentamers, monomer)
    assert (out.per_parent, len(out.rows)) == (5, 84 * 5)
    assert out.rows["cbPickUid"].is_unique
    assert (out.rows["cbParentUid"].to_numpy() == np.repeat(pentamers["cbPickUid"].to_numpy(), 5)).all()
    assert (out.rows["rlnRandomSubset"].to_numpy() == np.repeat(pentamers["rlnRandomSubset"].to_numpy(), 5)).all()


def test_copies_outside_their_tomogram_are_left_out():
    parents = _capsids()
    extents = {"t1": np.array([4400.0, 4400.0, 4400.0])}
    out = subparticles.place(parents, PENTAMER, extents=extents, margin_ang=80.0)
    xyz = out.rows.loc[out.rows["rlnTomoName"] == "t1", CENTERED_COLS].to_numpy()
    assert out.n_outside > 0 and len(out.rows) == 84 - out.n_outside
    assert (np.abs(xyz) <= 2200.0 - 80.0).all()
    assert any("missing from the tomograms star" in n for n in out.notes)  # t2 is not in `extents`


def test_parents_that_cannot_be_placed_are_refused():
    parents = _capsids()
    with pytest.raises(ValueError, match="rlnTomoSubtomogramRot"):
        subparticles.place(parents.assign(rlnTomoSubtomogramRot=0.0), PENTAMER)
    with pytest.raises(ValueError, match="rlnAngleRot"):
        subparticles.place(parents.drop(columns=["rlnAngleRot"]), PENTAMER)
    with pytest.raises(ValueError, match="twice"):
        subparticles.place(pd.concat([parents, parents.iloc[:1]]), PENTAMER)
    with pytest.raises(ValueError, match="rlnHelicalTubeID"):
        subparticles.place(parents, {"kind": "helix", "twist_deg": -27.69, "rise_ang": 9.55, "radius_ang": 100})


TWIST, RISE, REPEAT = -360.0 / 13, 9.55, 13 * 9.55 / 3
SPACING = 2 * REPEAT


def _filament(n_segments: int = 12, tube: int = 1, shift=(0.0, 0.0, 0.0)) -> pd.DataFrame:
    """Segments along Z, two monomer repeats apart: a step that carries the lattice onto itself, so every segment
    sees the same lattice with the same angles."""
    z = np.arange(n_segments) * SPACING
    df = pd.DataFrame(
        {
            "rlnTomoName": "t1",
            "rlnHelicalTubeID": tube,
            "rlnHelicalTrackLengthAngst": z,
            "rlnAngleRot": 25.0,
            "rlnAngleTilt": 0.0,
            "rlnAnglePsi": 0.0,
            "rlnRandomSubset": 1 + (tube % 2),
        }
    )
    df[CENTERED_COLS[0]], df[CENTERED_COLS[1]], df[CENTERED_COLS[2]] = shift[0], shift[1], z + shift[2]
    return df


def _lattice_site(**over) -> dict:
    return {
        "kind": "helix",
        "twist_deg": TWIST,
        "rise_ang": RISE,
        "radius_ang": 102.0,
        "axial_repeat_ang": REPEAT,
    } | over


def test_a_helix_alone_visits_a_third_of_a_three_start_lattice():
    alone = sites.helix_site(twist_deg=TWIST, rise_ang=RISE, radius_ang=102.0, half_window_ang=62.0)
    whole = sites.helix_site(
        twist_deg=TWIST, rise_ang=RISE, radius_ang=102.0, half_window_ang=62.0, axial_repeat_ang=REPEAT
    )
    assert (alone.n, whole.n) == (13, 39)
    assert np.isclose(whole.spacing_ang, REPEAT, atol=0.01)


def test_neighbouring_segments_give_each_unit_once():
    from scipy.spatial import cKDTree

    parents = pd.concat([_filament(tube=1), _filament(tube=2, shift=(600.0, 0.0, 0.0))], ignore_index=True)
    out = subparticles.place(parents, _lattice_site())
    rows = out.rows
    assert out.n_doubles > 0 and out.n_close == 0
    assert (rows.groupby("cbAssembly")["rlnRandomSubset"].nunique() == 1).all()
    assert rows["cbAssembly"].nunique() == 2
    for _, g in rows.groupby("cbAssembly"):
        xyz = g[CENTERED_COLS].to_numpy()
        d, _ = cKDTree(xyz).query(xyz, k=2)
        # No unit twice, and none missing inside the filament: every unit's nearest neighbour is a lattice step away.
        assert d[:, 1].min() > REPEAT - 0.5 and d[:, 1].max() < REPEAT + 0.5
        # 13 protofilaments, a monomer every repeat, over the segments' span and half a window at each end.
        span = 11 * SPACING + 2 * out.half_window_ang
        assert abs(len(g) - 13 * span / REPEAT) <= 13
        axis_x = 600.0 if xyz[:, 0].mean() > 300.0 else 0.0
        assert np.allclose(np.hypot(xyz[:, 0] - axis_x, xyz[:, 1]), 102.0, atol=1e-6)


def test_segments_that_disagree_about_the_lattice_are_said():
    a = _filament(n_segments=6)
    # Every other segment 0.4 of a monomer off along the axis: its copies land between the others', too far from
    # them to be the same unit and too near to be the next.
    a.loc[a.index % 2 == 1, CENTERED_COLS[2]] += 0.4 * REPEAT
    out = subparticles.place(a, _lattice_site())
    assert out.n_close > 0 and any("disagree" in n for n in out.notes)

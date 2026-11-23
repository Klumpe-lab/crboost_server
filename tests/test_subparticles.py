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


def _same_rows(a: pd.DataFrame, b: pd.DataFrame) -> None:
    """Made again, the same rows: numbers equal far below the six decimals a star keeps (numpy's sums can differ in
    the last bit from one call to the next, with the arrays' alignment in memory)."""
    pd.testing.assert_frame_equal(a, b, check_exact=False, rtol=0, atol=1e-9)


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
    _same_rows(subparticles.place(parents, PENTAMER).rows, rows)


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


# ── Expansions: the same images in every pose the symmetry makes equal ─────────────────────────


def _segments(n: int = 6) -> pd.DataFrame:
    """Refined segments with their images, origins and a refinement's per-particle scores."""
    rng = np.random.default_rng(3)
    df = _filament(n_segments=n).assign(
        rlnImageName=[f"seg{i}.mrcs" for i in range(n)],
        rlnTomoParticleName=[f"t1/{i}" for i in range(n)],
        rlnAngleRot=rng.uniform(-180, 180, n),
        rlnAngleTilt=rng.uniform(20, 160, n),
        rlnAnglePsi=rng.uniform(-180, 180, n),
        rlnAngleTiltPrior=90.0,
        rlnAnglePsiPrior=0.0,
        rlnOriginXAngst=rng.normal(0, 2, n),
        rlnOriginYAngst=rng.normal(0, 2, n),
        rlnOriginZAngst=rng.normal(0, 2, n),
        rlnLogLikeliContribution=1.0e6,
    )
    return df


DIMERS = {"kind": "lattice", "twist_deg": TWIST, "rise_ang": RISE, "axial_repeat_ang": REPEAT, "units_per_repeat": 2}


def _moves_lattice_onto_itself(q: np.ndarray, v: np.ndarray, lattice: np.ndarray) -> bool:
    """Every unit of `lattice` (away from its ends) lands on a unit after the move Q·p + v."""
    from scipy.spatial import cKDTree

    inner = lattice[np.abs(lattice[:, 2]) < 150.0]
    d, _ = cKDTree(lattice).query(inner @ q.T + v)
    return bool(d.max() < 1e-6)


def test_a_lattice_s_registrations():
    """26 for a 13_3 lattice of dimers: each carries the monomer lattice onto itself, no two are the same pose of
    the dimers (they differ by more than whole dimer repeats), label k is k steps of the helix, and none moves
    further than half a dimer repeat (the 26 steps in a row would move 124 Å)."""
    e = sites.expansion_of(DIMERS)
    assert e.n == 26 and e.by_steps and sorted(e.labels) == list(range(26))
    assert e.max_shift_ang <= REPEAT + 1e-6
    lattice = sites.helix_site(
        twist_deg=TWIST, rise_ang=RISE, radius_ang=102.0, half_window_ang=400.0, axial_repeat_ang=REPEAT
    ).offsets
    for q, v in zip(e.rotations, e.offsets, strict=True):
        assert _moves_lattice_onto_itself(q, v, lattice)
    for k, q, v in zip(e.labels, e.rotations, e.offsets, strict=True):
        # k steps of the helix, less whole dimer repeats along the axis, are this registration.
        steps_q, steps_z = sites.rot_z(k * TWIST), k * RISE
        assert np.allclose(steps_q, q, atol=1e-9)
        assert np.isclose(((steps_z - v[2]) / SPACING) % 1.0, 0.0, atol=1e-6) or np.isclose(
            ((steps_z - v[2]) / SPACING) % 1.0, 1.0, atol=1e-6
        )
    for i in range(26):
        for j in range(i + 1, 26):
            same_turn = np.allclose(e.rotations[i], e.rotations[j], atol=1e-9)
            dz = (e.offsets[i, 2] - e.offsets[j, 2]) / SPACING
            assert not (same_turn and np.isclose(dz, round(dz), atol=1e-6))
    assert sites.expansion_of(dict(DIMERS)).digest == e.digest


def test_an_expansion_keeps_the_images_and_moves_the_origins():
    parents = _segments()
    out = subparticles.expand(parents, DIMERS)
    rows, k = out.rows, 26
    assert (out.n_parents, out.per_parent, len(rows)) == (6, k, 6 * k)
    # The images, the coordinate and the half are the particle's; the refinement's score of the parent is not.
    for c in ("rlnImageName", "rlnTomoParticleName", *CENTERED_COLS, "rlnRandomSubset", "rlnHelicalTubeID"):
        assert (rows[c].to_numpy() == np.repeat(parents[c].to_numpy(), k)).all(), c
    assert "rlnLogLikeliContribution" not in rows.columns
    # The pose is the parent's followed by the registration; the centre (coordinate − origin) moves by A·v.
    a = euler_matrices(parents["rlnAngleRot"], parents["rlnAngleTilt"], parents["rlnAnglePsi"])
    e = out.expansion
    got = euler_matrices(rows["rlnAngleRot"], rows["rlnAngleTilt"], rows["rlnAnglePsi"]).reshape(6, k, 3, 3)
    assert np.allclose(got, np.einsum("nij,kjl->nkil", a, e.rotations), atol=1e-9)
    origin = ["rlnOriginXAngst", "rlnOriginYAngst", "rlnOriginZAngst"]
    centre = parents[CENTERED_COLS].to_numpy() - parents[origin].to_numpy()
    moved = (rows[CENTERED_COLS].to_numpy() - rows[origin].to_numpy()).reshape(6, k, 3)
    assert np.allclose(moved, centre[:, None, :] + np.einsum("nij,kj->nki", a, e.offsets), atol=1e-9)
    # A turn about the filament's own axis keeps its tilt and psi, so the priors still hold; the origin is its own
    # prior, so RELION's prior on translations weighs every copy alike.
    assert np.allclose(rows["rlnAngleTilt"], np.repeat(parents["rlnAngleTilt"], k), atol=1e-9)
    assert {"rlnAngleTiltPrior", "rlnAnglePsiPrior"} <= set(rows.columns)
    priors = ["rlnOriginXPriorAngst", "rlnOriginYPriorAngst", "rlnOriginZPriorAngst"]
    assert np.allclose(rows[priors].to_numpy(), rows[origin].to_numpy())
    # One uid per copy, its particle named, its label kept, one filament.
    assert rows["cbPickUid"].is_unique
    assert rows["cbParentUid"].nunique() == 6 and rows["cbAssembly"].nunique() == 1
    assert (rows["cbSubIndex"].to_numpy() == np.tile(e.labels, 6)).all()
    _same_rows(subparticles.expand(parents, DIMERS).rows, rows)


def test_a_point_group_expansion_takes_every_operator():
    parents = _capsids().assign(rlnAngleTiltPrior=0.0)
    out = subparticles.expand(parents, {"kind": "point_group", "group": "I1"})
    rows = out.rows
    assert len(rows) == 7 * 60
    origin = ["rlnOriginXAngst", "rlnOriginYAngst", "rlnOriginZAngst"]
    assert np.allclose(rows[origin].to_numpy(), np.repeat(parents[origin].to_numpy(), 60, axis=0))
    a = euler_matrices(parents["rlnAngleRot"], parents["rlnAngleTilt"], parents["rlnAnglePsi"])
    got = euler_matrices(rows["rlnAngleRot"], rows["rlnAngleTilt"], rows["rlnAnglePsi"]).reshape(7, 60, 3, 3)
    assert np.allclose(got, np.einsum("nij,kjl->nkil", a, sites.operators("I1")), atol=1e-9)
    assert "rlnAngleTiltPrior" not in rows.columns  # a point group's turn moves tilt and psi
    assert (rows.groupby("cbAssembly")["rlnRandomSubset"].nunique() == 1).all()


def test_what_cannot_be_expanded_is_refused():
    with pytest.raises(ValueError, match="rlnImageName"):
        subparticles.expand(_segments().drop(columns=["rlnImageName"]), DIMERS)
    with pytest.raises(ValueError, match="units per repeat"):
        sites.expansion_of(DIMERS | {"units_per_repeat": 1})
    with pytest.raises(ValueError, match="never come back"):
        sites.expansion_of(DIMERS | {"twist_deg": -166.6, "rise_ang": 27.6, "axial_repeat_ang": 27.6 * 1.37})
    with pytest.raises(ValueError, match="nothing to expand"):
        sites.point_group_expansion("C1")


def test_a_scaled_template_puts_the_structure_where_the_scale_says(tmp_path):
    """A blob off the centre, scaled by 0.9 across Z and 1.1 along it, lands where the scale puts it, on the grid
    asked for: the resample that brings a deposited filament to the data's lattice."""
    import mrcfile

    from services.templating import volume_prep

    n, apix = 64, 2.0
    ax = (np.arange(n) - n // 2) * apix
    z, y, x = np.meshgrid(ax, ax, ax, indexing="ij")
    blob = np.exp(-((x - 20.0) ** 2 + (y + 10.0) ** 2 + (z - 30.0) ** 2) / (2 * 4.0**2)).astype(np.float32)
    src = tmp_path / "src.mrc"
    with mrcfile.new(src) as m:
        m.set_data(blob)
        m.voxel_size = apix
    white, black = tmp_path / "w.mrc", tmp_path / "b.mrc"
    info = volume_prep.resample_scaled(str(src), str(white), str(black), 3.0, 48, 0.9, 1.1)
    out = mrcfile.read(white)
    assert out.shape == (48, 48, 48) and np.isclose(mrcfile.open(white).voxel_size.x, 3.0)
    assert np.allclose(mrcfile.read(black), -out)
    k, j, i = np.unravel_index(np.argmax(out), out.shape)
    where = (np.array([i, j, k]) - 24) * 3.0
    assert np.allclose(where, [20.0 * 0.9, -10.0 * 0.9, 30.0 * 1.1], atol=1.6)
    assert info.cutoff_ang == 6.0

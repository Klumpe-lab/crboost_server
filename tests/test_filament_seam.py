"""The seam vote (roadmap 71 S7): filaments made on a known lattice, their segments left on random registrations as
a refinement that cannot tell the units apart leaves them, scored as the units would score them.

No site and no data: `./crboost ci` runs it with the repo tests.
"""

import numpy as np
import pandas as pd
import pytest

from services.particles import filament_seam, sites, subparticles
from services.particles.coords import CENTERED_COLS
from services.particles.poses import euler_angles, euler_matrices
from services.particles.subparticles import ANGLE_COLS, INDEX_COL, ORIGIN_COLS, PARENT_COL

pytestmark = pytest.mark.repo

LATTICE = {
    "kind": sites.LATTICE,
    "twist_deg": -27.69,
    "rise_ang": 9.55,
    "axial_repeat_ang": 41.3833,
    "units_per_repeat": 2,
}
EXPANSION = sites.expansion_of(LATTICE)
N = EXPANSION.n
DIMER = 2 * 41.3833
RADIUS = 111.0


def _filament(rng, tomo, tube, n, *, k_true, strength=40.0, noise=1.0):
    """`n` segments a dimer repeat apart on one lattice, each on a random registration of it, and the score of
    every copy: the triangle about the filament's true registration, a pattern that follows the turn alone, noise.
    Returns the scored copies and each segment's registration (its label 0 seen from the filament)."""
    frame = euler_matrices([rng.uniform(-180, 180)], [rng.uniform(40, 140)], [rng.uniform(-180, 180)])[0]
    start = rng.uniform(-500, 500, 3)
    on = rng.integers(0, N, n)
    poses = np.einsum("ij,njk->nik", frame, EXPANSION.rotations[on])
    along = np.arange(n) * DIMER + EXPANSION.shifts_ang[on]
    centres = start + along[:, None] * frame[:, 2]
    rot, tilt, psi = euler_angles(poses)
    parents = pd.DataFrame(
        {
            "rlnTomoName": tomo,
            "rlnHelicalTubeID": tube,
            "rlnHelicalTrackLengthAngst": np.arange(n) * DIMER,
            "rlnImageName": [f"{tomo}_{tube}_{i}.mrcs" for i in range(n)],
            "rlnRandomSubset": 1 + tube % 2,
        }
    )
    for i, c in enumerate(CENTERED_COLS):
        parents[c] = centres[:, i]
    parents[ANGLE_COLS[0]], parents[ANGLE_COLS[1]], parents[ANGLE_COLS[2]] = rot, tilt, psi
    for c in ORIGIN_COLS:
        parents[c] = 0.0
    copies = subparticles.expand(parents, LATTICE).rows
    of_segment = np.repeat(np.arange(n), N)
    # Copy k of a segment on registration L is the filament's registration L + k.
    d = (on[of_segment] + copies[INDEX_COL].to_numpy() - k_true) % N
    triangle = 1.0 - 2.0 * np.minimum(d, N - d) / (N // 2)
    turn_only = 300.0 * np.cos(2 * np.pi * (on[of_segment] + copies[INDEX_COL].to_numpy()) / (N // 2))
    copies[filament_seam.SCORE_COL] = strength * triangle + turn_only + rng.normal(0, noise, len(copies))
    return copies, on


def _scored(rng, lengths, k_true, **kwargs):
    parts, on = [], {}
    for tube, (n, k) in enumerate(zip(lengths, k_true, strict=True), 1):
        copies, registrations = _filament(rng, "tomo_a", tube, n, k_true=k, **kwargs)
        parts.append(copies)
        on[tube] = registrations
    return pd.concat(parts, ignore_index=True), on


def test_each_filament_is_put_on_its_registration():
    """The vote finds every filament's registration whatever registration each segment was left on, and what
    follows the turn alone (ten times the units' part here) does not enter it."""
    rng = np.random.default_rng(1)
    k_true = [3, 17, 25]
    scored, on = _scored(rng, [40, 30, 55], k_true)
    v = filament_seam.vote(scored, EXPANSION, radius_ang=RADIUS, min_segments=30)
    assert v.n_left_out == 0 and len(v.pieces) == 3
    assert (v.pieces["verdict"] == filament_seam.DECIDED).all()
    # A piece's frame is its first segment's: the filament's registration less that segment's own.
    assert v.pieces["k"].tolist() == [(k - on[t][0]) % N for t, k in zip((1, 2, 3), k_true, strict=True)]
    assert (v.pieces["halves"] == 0).all()
    rows = filament_seam.registered_rows(v, scored, held_out=True)
    assert len(rows) == 125 and rows[PARENT_COL].is_unique
    assert filament_seam.SCORE_COL not in rows.columns
    for tube, k in zip((1, 2, 3), k_true, strict=True):
        got = rows[rows["rlnHelicalTubeID"] == tube].sort_values("rlnHelicalTrackLengthAngst")
        assert got[INDEX_COL].tolist() == [int(x) for x in (k - on[tube]) % N]
    assert "#" not in "".join(rows[filament_seam.PIECE_COL])
    assert v.checks["by_length"][0]["sites_alike"] == 1.0


def test_registered_segments_share_one_frame():
    """On their registrations, neighbouring segments are whole repeats apart along one axis with no turn between
    them: a unit of the true repeat sits at the same site in each."""
    rng = np.random.default_rng(2)
    scored, _on = _scored(rng, [30], [11])
    v = filament_seam.vote(scored, EXPANSION, radius_ang=RADIUS)
    rows = filament_seam.registered_rows(v, scored, held_out=False).sort_values("rlnHelicalTrackLengthAngst")
    a = euler_matrices(*(rows[c].to_numpy(float) for c in ANGLE_COLS))
    c = rows[CENTERED_COLS].to_numpy(float) - rows[ORIGIN_COLS].to_numpy(float)
    # Thirteen steps of −27.69° close the turn within 0.03°, so the frames agree to that and no better.
    assert np.allclose(a, a[0], atol=2e-3)
    steps = np.einsum("ij,nj->ni", a[0].T, np.diff(c, axis=0))
    assert np.allclose(steps[:, :2], 0.0, atol=1e-6)
    assert np.allclose(steps[:, 2] / DIMER, np.round(steps[:, 2] / DIMER), atol=1e-4)


def test_a_held_out_registration_ignores_the_segments_own_scores():
    """A block's held-out registration is voted by the blocks beyond its neighbours: scores of the block itself
    and of its neighbours, however wrong, do not move it; the whole piece's vote they can."""
    rng = np.random.default_rng(3)
    scored, _on = _scored(rng, [40], [5], strength=5.0)
    v = filament_seam.vote(scored, EXPANSION, radius_ang=RADIUS)
    seg = v.segments.sort_values("block", kind="stable")
    spoiled = scored.copy()
    in_block = spoiled[PARENT_COL].isin(seg.index[seg["block"].isin([2, 3, 4])])
    spoiled.loc[in_block, filament_seam.SCORE_COL] = rng.normal(0, 500.0, int(in_block.sum()))
    w = filament_seam.vote(spoiled, EXPANSION, radius_ang=RADIUS)
    before, after = v.segments.loc[seg.index], w.segments.loc[seg.index]
    assert (before.loc[before["block"] == 3, "k_held_out"] == after.loc[after["block"] == 3, "k_held_out"]).all()
    assert (before["k_held_out"] >= 0).all()


def test_a_segment_off_the_lattice_is_left_out_and_a_run_of_them_ends_the_piece():
    rng = np.random.default_rng(4)
    scored, _on = _scored(rng, [40], [9])
    uids = scored.drop_duplicates(PARENT_COL).sort_values("rlnHelicalTrackLengthAngst")[PARENT_COL].tolist()
    axis = euler_matrices(*(scored.loc[scored[PARENT_COL] == uids[0], c].to_numpy(float)[:1] for c in ANGLE_COLS))[0]

    def moved(which):
        out = scored.copy()
        sel = out[PARENT_COL].isin([uids[i] for i in which])
        # 20 Å along the axis is between two registrations (monomers are 41 Å apart, steps 9.55 Å).
        out.loc[sel, CENTERED_COLS] = out.loc[sel, CENTERED_COLS].to_numpy(float) + 20.0 * axis[:, 2]
        return out

    one = filament_seam.vote(moved([10]), EXPANSION, radius_ang=RADIUS)
    assert one.n_left_out == 1 and len(one.pieces) == 1 and one.pieces["segments"].iloc[0] == 39
    # Three in a row end the piece and the third starts the next; the two after it are off its lattice in turn,
    # and the third of those starts the piece that runs to the end.
    run = filament_seam.vote(moved([10, 11, 12]), EXPANSION, radius_ang=RADIUS, min_segments=20)
    assert run.pieces["segments"].tolist() == [10, 1, 25] and run.n_left_out == 4
    assert run.pieces["verdict"].tolist() == [filament_seam.UNDECIDED, filament_seam.UNDECIDED, filament_seam.DECIDED]
    assert "shorter than 20" in run.pieces["why"].iloc[0]


def test_what_cannot_be_voted_is_refused():
    rng = np.random.default_rng(5)
    scored, _on = _scored(rng, [20], [0])
    with pytest.raises(ValueError, match="least"):
        filament_seam.vote(scored, EXPANSION, radius_ang=RADIUS, min_segments=10)
    with pytest.raises(ValueError, match="radius"):
        filament_seam.vote(scored, EXPANSION, radius_ang=None)
    with pytest.raises(ValueError, match="point group"):
        filament_seam.vote(scored, sites.point_group_expansion("C5"), radius_ang=RADIUS)
    with pytest.raises(ValueError, match="lack some"):
        filament_seam.vote(scored[scored[INDEX_COL] != 7], EXPANSION, radius_ang=RADIUS)
    with pytest.raises(ValueError, match="carry no"):
        filament_seam.vote(scored.drop(columns=[filament_seam.SCORE_COL]), EXPANSION, radius_ang=RADIUS)


def test_noise_alone_is_seen_in_the_checks():
    """Without anything that tells the units apart, the halves of a piece label about half the sites alike and
    blocks of one piece agree no more than blocks of different filaments."""
    rng = np.random.default_rng(6)
    scored, _on = _scored(rng, [40] * 30, list(rng.integers(0, N, 30)), strength=0.0)
    v = filament_seam.vote(scored, EXPANSION, radius_ang=RADIUS)
    alike = v.checks["by_length"][0]["sites_alike"]
    assert abs(alike - 0.5) < 0.12
    blocks = v.checks["blocks"]
    assert abs(blocks["within_one_step"] - blocks["other_filaments_within_one_step"]) < 0.06

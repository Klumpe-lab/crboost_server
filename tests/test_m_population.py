"""M with several species, expansions and temporal samples (roadmap 73): the pure pieces, without M.

The copies' layout is MTools expand_symmetry's own, read 2026-11-25 on a scratch copy of roadmap 73 S0's population
(the capsid, I1 → C1: 105 particles → 6,300 rows, each particle's 60 copies one after another, every column the
parent's but the angles). `./crboost ci` runs it with the repo tests.
"""

import numpy as np
import pandas as pd
import pytest

from services.jobs.m_refine import MRefineParams
from services.particles import m_population as mp
from services.particles.poses import euler_angles, euler_matrices

pytestmark = pytest.mark.repo


def m_star(names: list[str], copies: int = 1, samples: int = 1) -> pd.DataFrame:
    """An M particle star as M writes one: every particle `copies` times in a row."""
    rows = []
    for i, name in enumerate(names):
        for k in range(copies):
            row = {}
            for t in range(1, samples + 1):
                row |= {f"wrpCoordinate{a}{t}": 100.0 * (i + 1) + t for a in "XYZ"}
            for t in range(1, samples + 1):
                row |= {f"wrpAngleRot{t}": 10.0 * k, f"wrpAngleTilt{t}": 30.0, f"wrpAnglePsi{t}": 0.0}
            row |= {mp.M_SUBSET: 1 + i % 2, mp.M_SOURCE: "TS1", mp.UID_COL: f"u{i:012x}", mp.NAME_COL: name}
            rows.append(row)
    return pd.DataFrame(rows)


def test_copies_are_named_after_their_parents(tmp_path):
    star = tmp_path / "x_particles.star"
    mp.write_plain_star(m_star(["TS1/1", "TS1/2"], copies=3), star)
    counts = mp.label_copies(star, "r1|I1|")
    df = mp.read_plain_star(star)
    assert counts == {"parents": 2, "copies": 6, "per_parent": 3}
    assert df[mp.NAME_COL].tolist() == ["TS1/1_0", "TS1/1_1", "TS1/1_2", "TS1/2_0", "TS1/2_1", "TS1/2_2"]
    assert df[mp.PARENT_NAME_COL].tolist() == ["TS1/1"] * 3 + ["TS1/2"] * 3
    assert df[mp.INDEX_COL].tolist() == [0, 1, 2, 0, 1, 2]
    assert df[mp.UID_COL].nunique() == 6
    assert df[mp.PARENT_UID_COL].tolist() == ["u000000000000"] * 3 + ["u000000000001"] * 3


def test_an_expansion_without_copies_is_refused(tmp_path):
    star = tmp_path / "x_particles.star"
    mp.write_plain_star(m_star(["TS1/1", "TS1/2"]), star)
    with pytest.raises(ValueError, match="no copy"):
        mp.label_copies(star, "t")


def test_one_sample_is_the_pose_as_written():
    df = m_star(["a"])
    pos, mats = mp.m_pose(df)
    assert np.allclose(pos, [[101.0, 101.0, 101.0]])
    assert np.allclose(mp.angles_of(mats), [[0.0, 30.0, 0.0]], atol=1e-9)


def test_two_samples_give_the_mean_pose():
    """Samples symmetric about a rotation (R·Rz(+θ), R·Rz(−θ)) average to it; positions to their mean."""
    middle = euler_matrices([40.0], [50.0], [60.0])[0]
    turns = [euler_matrices([0.0], [0.0], [s * 12.0])[0] for s in (1, -1)]
    df = pd.DataFrame({f"wrpCoordinate{a}{t}": [float(t * 10)] for t in (1, 2) for a in "XYZ"})
    for t, turn in zip((1, 2), turns, strict=True):
        rot, tilt, psi = euler_angles((middle @ turn)[None])
        df[f"wrpAngleRot{t}"], df[f"wrpAngleTilt{t}"], df[f"wrpAnglePsi{t}"] = rot, tilt, psi
    pos, mats = mp.m_pose(df)
    assert mp.m_samples(df) == 2
    assert np.allclose(pos, [[15.0, 15.0, 15.0]])
    assert np.allclose(mats[0], middle, atol=1e-9)


def test_the_set_holds_each_copy_under_its_parents_row(tmp_path):
    star = tmp_path / "x_particles.star"
    mp.write_plain_star(m_star(["TS1/1", "TS1/2"], copies=2), star)
    mp.label_copies(star, "t")
    m = mp.read_plain_star(star)
    base = pd.DataFrame({mp.NAME: ["TS1/1", "TS1/2"], "rlnTomoName": ["TS1", "TS1"], "rlnLCCmax": [0.4, 0.5]})
    out = mp.expand_base(base, m)
    assert out[mp.NAME].tolist() == ["TS1/1_0", "TS1/1_1", "TS1/2_0", "TS1/2_1"]
    assert out["rlnLCCmax"].tolist() == [0.4, 0.4, 0.5, 0.5]
    assert out[mp.INDEX_COL].tolist() == [0, 1, 0, 1]
    assert mp.expand_base(out, m) is out  # the next round's set holds the copies already


def test_moves_are_per_tilt_series():
    before = m_star(["TS1/1", "TS1/2"])
    after = before.copy()
    after.loc[0, "wrpCoordinateX1"] += 3.0
    after.loc[0, "wrpAngleRot1"] += 5.0
    manifest = {"sources": {"s": {"series": [{"name": "set_TS1", "item": "TS1"}]}}}
    moves = mp.m_moves(before, after, manifest)
    assert list(moves) == ["set_TS1"]
    m = moves["set_TS1"]
    assert m["particles"] == 2
    assert m["moved_max_ang"] == pytest.approx(3.0)
    assert m["turned_max_deg"] == pytest.approx(5.0, abs=1e-6)


def test_a_hand_back_shows_no_round_fields():
    p = MRefineParams(continue_from_m=True, hand_back_only=True)
    assert p.hand_back and not p.makes_species and not p.reads_mask
    for name in ("iterations", "refine_particles", "expand_symmetry", "temporal_samples", "ctf_defocus"):
        assert not p.shows_param(name), name
    assert not p.shows_param("add_species")


def test_adding_a_species_reads_a_refine3d_and_a_mask():
    p = MRefineParams(continue_from_m=True, add_species=True, diameter_ang=350.0)
    assert p.adds_species and p.makes_species and p.reads_mask and not p.hand_back
    for name in ("symmetry", "diameter_ang", "use_denoiser", "population_series_only"):
        assert p.shows_param(name), name
    assert not p.shows_param("update_mask") and not p.shows_param("hand_back_only")
    assert p.run_problems() == []


def test_a_new_mask_is_a_follow_ups_own():
    p = MRefineParams(continue_from_m=True, update_mask=True)
    assert p.changes_mask and p.reads_mask and not p.makes_species
    assert p.shows_param("mask_dilate") and not p.shows_param("symmetry")
    assert not MRefineParams(update_mask=True, diameter_ang=100.0).changes_mask


def test_switches_and_expansion_arguments():
    p = MRefineParams(refine_tiltmovies=True, expand_symmetry=True, expand_from="C5", expand_to="C1")
    assert "--refine_tiltmovies" in p.mcore_switches()
    assert p.expansion_args() == ["--expand_from", "C5", "--expand_to", "C1"]
    h = MRefineParams(expand_symmetry=True, helical_units=13, helical_twist=-27.69, helical_rise=9.55)
    assert h.expansion_args() == ["--helical_units", "13", "--helical_twist", "-27.69", "--helical_rise", "9.55"]
    assert h.shows_param("helical_twist") and not p.shows_param("helical_twist")


def test_expansions_that_cannot_work_are_said():
    c1 = MRefineParams(expand_symmetry=True, symmetry="C1", diameter_ang=100.0)
    assert any("C1 makes no copies" in s for s in c1.run_problems())
    helix = MRefineParams(expand_symmetry=True, helical_units=13, diameter_ang=100.0)
    assert any("twist and rise" in s for s in helix.run_problems())
    both = MRefineParams(continue_from_m=True, add_species=True, hand_back_only=True)
    assert any("exclude each other" in s for s in both.run_problems())

"""Which registration of its lattice a filament is on: a microtubule's seam and register, voted per filament.

A refinement against a map that blends a lattice's units (alpha and beta tubulin) leaves each segment on any of the
lattice's registrations (`sites.lattice_expansion`: 26 for a 13-protofilament microtubule). An expansion set holds
every segment in each of them, and a Class 3D that scores the set without aligning writes one score per copy. This
module reads those scores. Pure: tables in, tables out; `set_ops` reads the stars and publishes the set.

**One frame per filament.** A copy's label counts helical steps from its own segment's refined pose, and
neighbouring segments sit on different registrations. Each segment's labels are carried into its filament's frame
by the registration between it and its neighbour, chained along the filament (`chain`). A segment whose pose is on
no registration of its neighbour's is left out and its neighbours bridged; after a few in a row the filament is cut
into pieces, each voting alone.

**The part of the scores the units decide.** A step of the helix moves the seam over one protofilament, so
registrations k and k* label the same unit on 1 − |k − k*| / n of the sites (n steps to a turn), and k* + n on
none: what the units add to the scores is a triangle that changes sign under k → k + n. Whatever follows the turn
alone (the missing wedge against where the reference points) repeats every n labels and cannot enter
S(k) − S(k + n), which the vote matches to that triangle (`_profiles`).

**What is independent.** A copy's score reads the filament as far as the reference reaches, so neighbouring
segments share their data, and so do neighbouring blocks of segments. Blocks at least one block apart do not
(`block_segments` must span the reference's reach along the axis). Three things follow from that one rule:
the two halves of a piece, a block apart, vote alone and their distance says how far the piece's vote can be
trusted (`halves`; over many pieces, `checks`' share of sites the two halves label alike); a block's own vote is
compared with the vote of the blocks beyond its neighbours (`own_vs_rest`); and a segment can be given the
registration the rest of its filament voted (`held_out`), so an average of the registered segments holds nothing
the segments' own noise chose. Which pieces are kept is decided by their length alone, never by a score, for the
same reason.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from services.particles import sites
from services.particles.coords import CENTERED_COLS
from services.particles.poses import euler_matrices
from services.particles.subparticles import (
    ANGLE_COLS,
    INDEX_COL,
    ORIGIN_COLS,
    PARENT_COL,
    REFINED_SCORE_COLS,
    TOMO_COL,
    TUBE_COL,
)

SCORE_COL = "rlnLogLikeliContribution"
TRACK_COL = "rlnHelicalTrackLengthAngst"
PIECE_COL = "cbSeamPiece"  # the stretch of filament that voted together
REGISTRATION_COL = "cbSeamRegistration"  # the registration it was given, in its piece's frame

DECIDED = "decided"
UNDECIDED = "undecided"

# A segment further than this from every registration of the last segment kept is off the lattice.
BREAK_ANG = 8.0
# This many off the lattice in a row end a piece.
MAX_SKIP = 2
BLOCK_SEGMENTS = 5
# A held-out vote needs a block beyond each block's neighbours: four blocks at least.
MIN_BLOCKS = 4
# The lengths (in blocks) `checks` reports the halves' agreement from.
_LENGTH_CUTS = (4, 6, 8, 10, 12, 16)


@dataclass(frozen=True)
class Lattice:
    """An expansion's registrations as the vote uses them, labels in order."""

    n: int  # registrations
    half: int  # the steps that swap every unit
    turns_deg: np.ndarray
    shifts_ang: np.ndarray
    true_repeat_ang: float
    radius_ang: float  # where the units sit from the axis: turns a difference in angle into a distance


def lattice_of(expansion: sites.Expansion, radius_ang) -> Lattice:
    """The lattice a seam vote can read: two units to the true repeat, labels that count the helix's steps."""
    d = expansion.description
    if expansion.kind != sites.LATTICE:
        raise ValueError("a seam is voted on a lattice's registrations; this expansion is a point group's operators")
    if int(d.get("units_per_repeat") or 0) != 2 or not expansion.by_steps:
        raise ValueError(
            "the seam vote reads a lattice of two units to the true repeat whose registrations each are some number "
            f"of helical steps (a microtubule's alpha and beta); this one has {d.get('units_per_repeat')} units to a "
            "repeat"
        )
    labels = np.asarray(expansion.labels)
    if expansion.n % 2 or not np.array_equal(labels, np.arange(expansion.n)):
        raise ValueError("the expansion's labels are not 0 … n − 1 in order")
    try:
        radius = float(radius_ang)
    except (TypeError, ValueError):
        raise ValueError("the radius the units sit at from the axis is not given") from None
    if not np.isfinite(radius) or radius <= 0:
        raise ValueError(f"the radius the units sit at from the axis must be more than 0 (got {radius_ang!r})")
    return Lattice(
        n=expansion.n,
        half=expansion.n // 2,
        turns_deg=np.asarray(expansion.turns_deg, dtype=float),
        shifts_ang=np.asarray(expansion.shifts_ang, dtype=float),
        true_repeat_ang=2.0 * float(d["axial_repeat_ang"]),
        radius_ang=radius,
    )


def _wrap(x, period: float):
    return (x + period / 2.0) % period - period / 2.0


def circular_distance(a, b, n: int):
    """How many steps apart two registrations are, the short way round."""
    d = np.abs(np.asarray(a) - np.asarray(b)) % n
    return np.minimum(d, n - d)


def registration(r: np.ndarray, t: np.ndarray, lat: Lattice) -> tuple[int, float]:
    """The registration nearest to one segment's frame seen from another's (`r` the rotation, `t` the move, both
    in the first one's frame), and how far from it the frame is, in Å at the units' radius."""
    turn = np.degrees(np.arctan2(r[1, 0], r[0, 0]))
    arc = np.radians(_wrap(turn - lat.turns_deg, 360.0)) * lat.radius_ang
    along = _wrap(t[2] - lat.shifts_ang, lat.true_repeat_ang)
    d = np.hypot(arc, along)
    best = int(np.argmin(d))
    return best, float(d[best])


def score_table(scored: pd.DataFrame, n: int) -> tuple[np.ndarray, pd.DataFrame]:
    """`scores[i, k]`: the score of segment i's copy k, less the segment's own mean; and the segments in the same
    order, one row each: the filament, the place along it, the pose (a 3 × 3 matrix) and the centre of its own
    copy (label 0)."""
    needed = [SCORE_COL, INDEX_COL, PARENT_COL, TOMO_COL, TUBE_COL, TRACK_COL, *ANGLE_COLS, *CENTERED_COLS]
    missing = [c for c in needed if c not in scored.columns]
    if missing:
        raise ValueError(
            f"the scored particles carry no {', '.join(missing)}: a seam is voted on the scores of an expansion set "
            "of filament segments, as a Class 3D with Skip alignment writes them"
        )
    labels = pd.to_numeric(scored[INDEX_COL], errors="coerce")
    if labels.isna().any():
        raise ValueError(f"{INDEX_COL} is not a number on every row")
    df = scored.assign(**{INDEX_COL: labels.astype(int)})
    if df.duplicated([PARENT_COL, INDEX_COL]).any():
        raise ValueError("a segment holds one registration twice: the scores are not one expansion's")
    table = df.pivot(index=PARENT_COL, columns=INDEX_COL, values=SCORE_COL).reindex(columns=range(n))
    lacking = int(table.isna().any(axis=1).sum())
    if lacking:
        raise ValueError(f"{lacking:,} segment(s) lack some of their {n} scores")
    own = df[df[INDEX_COL] == 0].set_index(PARENT_COL).loc[table.index]
    centre = own[CENTERED_COLS].to_numpy(dtype=float)
    if all(c in own.columns for c in ORIGIN_COLS):
        centre = centre - own[ORIGIN_COLS].to_numpy(dtype=float)
    segments = pd.DataFrame(
        {
            TOMO_COL: own[TOMO_COL].astype(str).to_numpy(),
            TUBE_COL: own[TUBE_COL].to_numpy(),
            TRACK_COL: pd.to_numeric(own[TRACK_COL], errors="coerce").to_numpy(),
        },
        index=table.index,
    )
    if segments[TRACK_COL].isna().any():
        raise ValueError(f"{TRACK_COL} is not a number on every segment: their order along the filament is unknown")
    segments["pose"] = list(euler_matrices(*(own[c].to_numpy(dtype=float) for c in ANGLE_COLS)))
    segments["centre"] = list(centre)
    scores = table.to_numpy(dtype=float)
    return scores - scores.mean(axis=1, keepdims=True), segments


def chain(
    segments: pd.DataFrame, lat: Lattice, *, break_ang: float = BREAK_ANG, max_skip: int = MAX_SKIP
) -> tuple[pd.DataFrame, int]:
    """Each filament's segments in order along it, every one linked to the last one kept: (piece, place in the
    piece, shift), the shift being what is added to a label of the piece's frame to give the segment's own. A
    segment off the lattice from the last one kept is left out; after `max_skip` in a row a new piece starts.
    Returns the kept segments (indexed as `segments`) and how many were left out."""
    rows: list[tuple[Any, str, int, int]] = []
    left_out = 0
    keys = segments[TOMO_COL].astype(str) + "|" + segments[TUBE_COL].astype(str)
    for name, g in segments.groupby(keys.to_numpy(), sort=False):
        g = g.sort_values(TRACK_COL, kind="stable")
        uids = g.index.to_list()
        piece, place, anchor, shift, skipped = 0, 0, uids[0], 0, 0
        rows.append((anchor, f"{name}#0", 0, 0))
        for u in uids[1:]:
            a0, a1 = g.at[anchor, "pose"], g.at[u, "pose"]
            label, dist = registration(a0.T @ a1, a0.T @ (g.at[u, "centre"] - g.at[anchor, "centre"]), lat)
            if dist <= break_ang:
                shift, anchor, skipped, place = (shift + label) % lat.n, u, 0, place + 1
                rows.append((u, f"{name}#{piece}", place, shift))
                continue
            skipped += 1
            left_out += 1
            if skipped > max_skip:
                # The run of off-lattice segments ends the piece; the last of them starts the next.
                piece, place, anchor, shift, skipped = piece + 1, 0, u, 0, 0
                left_out -= 1
                rows.append((u, f"{name}#{piece}", 0, 0))
    out = pd.DataFrame(rows, columns=["uid", "piece", "place", "shift"]).set_index("uid")
    return out, left_out


def _triangle_matrix(lat: Lattice) -> np.ndarray:
    """m[k, d]: the triangle centred on registration k, read at d."""
    d = np.arange(lat.n)
    tri = 1.0 - 2.0 * np.abs(_wrap(d.astype(float), float(lat.n))) / lat.half
    return tri[(d[None, :] - d[:, None]) % lat.n]


def _profiles(mapped: np.ndarray, blocks: np.ndarray, n_blocks: int, lat: Lattice) -> np.ndarray:
    """Per block, how well each registration fits its segments' summed scores: the part that changes sign under
    k → k + half, matched to the triangle. Profiles add: a set of blocks votes with the sum of theirs."""
    summed = np.zeros((n_blocks, lat.n))
    np.add.at(summed, blocks, mapped)
    anti = summed - np.roll(summed, -lat.half, axis=1)
    return anti @ _triangle_matrix(lat).T


def _halves(profiles: np.ndarray) -> tuple[int, int] | None:
    """The votes of a piece's two halves, a block left out between them so they share no data; None when a half
    would be empty."""
    n_blocks = len(profiles)
    h = n_blocks // 2
    if h < 1 or n_blocks - h - 1 < 1:
        return None
    return int(np.argmax(profiles[:h].sum(axis=0))), int(np.argmax(profiles[h + 1 :].sum(axis=0)))


@dataclass
class SeamVote:
    """What the vote decided. `pieces`: one row per piece (its filament, segments, blocks, registration k, how
    many steps apart its two halves voted, how far its blocks' own votes lie from the rest's, the verdict and
    why). `segments`: one row per segment kept in a piece (the piece, its shift, its block, the piece's k and the
    k voted without the segment's own block and that block's neighbours, −1 when there is none)."""

    lattice: Lattice
    pieces: pd.DataFrame
    segments: pd.DataFrame
    n_segments: int
    n_left_out: int
    params: dict[str, Any]
    checks: dict[str, Any] = field(default_factory=dict)

    @property
    def counts(self) -> dict[str, int]:
        p = self.pieces
        decided = p[p["verdict"] == DECIDED]
        return {
            "segments": self.n_segments,
            "segments_left_out": self.n_left_out,
            "filaments": int(p["filament"].nunique()) if len(p) else 0,
            "pieces": len(p),
            "decided": len(decided),
            "undecided": len(p) - len(decided),
            "segments_decided": int(decided["segments"].sum()),
            "segments_undecided": int(p["segments"].sum() - decided["segments"].sum()),
        }


def sites_alike(steps, half: int):
    """The share of a lattice's sites two registrations `steps` apart label alike."""
    return 1.0 - np.asarray(steps, dtype=float) / half


def _checks(pieces: pd.DataFrame, blocks: pd.DataFrame, lat: Lattice, block_segments: int) -> dict[str, Any]:
    """Whether the vote means anything, from pieces that share no data. `blocks`: pairs of blocks of one piece at
    least one block apart, against blocks of different filaments. `by_length`: over the pieces at least so long,
    the share of sites their two halves label alike (a half where nothing is known; 1 where the halves agree)."""
    out: dict[str, Any] = {"chance_sites_alike": 0.5}
    same: list[np.ndarray] = []
    for _, g in blocks.groupby("piece", sort=False):
        k = g.sort_values("block")["k"].to_numpy()
        if len(k) < 3:
            continue
        i, j = np.triu_indices(len(k), k=2)
        same.append(circular_distance(k[i], k[j], lat.n))
    if same:
        within = np.concatenate(same)
        k_all, fil = blocks["k"].to_numpy(), blocks["filament"].to_numpy()
        between = circular_distance(k_all[:, None], k_all[None, :], lat.n)[fil[:, None] != fil[None, :]]
        out["blocks"] = {
            "pairs": len(within),
            "within_one_step": round(float((within <= 1).mean()), 4),
            "mean_steps": round(float(within.mean()), 3),
            "other_filaments_within_one_step": round(float((between <= 1).mean()), 4) if len(between) else None,
            "other_filaments_mean_steps": round(float(between.mean()), 3) if len(between) else None,
        }
    halved = pieces[pieces["halves"].notna()]
    by_length = []
    for cut in _LENGTH_CUTS:
        g = halved[halved["blocks"] >= cut]
        if g.empty:
            continue
        steps = g["halves"].to_numpy(dtype=float)
        by_length.append(
            {
                "min_segments": cut * block_segments,
                "pieces": len(g),
                "segments": int(g["segments"].sum()),
                "halves_mean_steps": round(float(steps.mean()), 2),
                "sites_alike": round(float(sites_alike(steps, lat.half).mean()), 3),
            }
        )
    out["by_length"] = by_length
    return out


def vote(
    scored: pd.DataFrame,
    expansion: sites.Expansion,
    *,
    radius_ang,
    block_segments: int = BLOCK_SEGMENTS,
    min_segments: int | None = None,
    break_ang: float = BREAK_ANG,
    max_skip: int = MAX_SKIP,
) -> SeamVote:
    """The seam vote over the scored copies of an expansion set (`scored`: one row per copy, with its score, its
    label and its segment's uid). A piece is decided when it has `min_segments` segments (at least `MIN_BLOCKS`
    blocks, which is also the least `min_segments` can ask): by its length alone, so that no score chooses which
    segments an average holds."""
    block_segments = int(block_segments)
    if block_segments < 1:
        raise ValueError("a block is one segment or more")
    least = MIN_BLOCKS * block_segments
    min_segments = least if min_segments is None else int(min_segments)
    if min_segments < least:
        raise ValueError(
            f"a piece needs {MIN_BLOCKS} blocks to be voted for by blocks that share none of its data: the least "
            f"length is {least} segments (got {min_segments})"
        )
    lat = lattice_of(expansion, radius_ang)
    scores, segs = score_table(scored, lat.n)
    row_of = {u: i for i, u in enumerate(segs.index)}
    kept, left_out = chain(segs, lat, break_ang=break_ang, max_skip=max_skip)
    labels = np.arange(lat.n)

    piece_rows: list[dict[str, Any]] = []
    block_rows: list[tuple[str, str, int, int]] = []
    seg_frames: list[pd.DataFrame] = []
    for piece, g in kept.groupby("piece", sort=False):
        g = g.sort_values("place", kind="stable")
        n = len(g)
        shifts = g["shift"].to_numpy()
        # Row j in its piece's frame: the piece's label k' is the segment's own label k' − shift.
        own = scores[[row_of[u] for u in g.index]]
        mapped = np.take_along_axis(own, (labels[None, :] - shifts[:, None]) % lat.n, axis=1)
        n_blocks = max(1, n // block_segments)
        blocks = np.minimum(np.arange(n) // block_segments, n_blocks - 1)
        prof = _profiles(mapped, blocks, n_blocks, lat)
        k_piece = int(np.argmax(prof.sum(axis=0)))
        k_own = prof.argmax(axis=1)
        far = np.abs(np.arange(n_blocks)[:, None] - np.arange(n_blocks)[None, :]) >= 2
        k_rest = np.array([int(np.argmax(prof[far[b]].sum(axis=0))) if far[b].any() else -1 for b in range(n_blocks)])
        tested = k_rest >= 0
        own_vs_rest = float(circular_distance(k_own[tested], k_rest[tested], lat.n).mean()) if tested.any() else None
        halves = _halves(prof)
        filament = piece.rsplit("#", 1)[0]
        long_enough = n >= min_segments and n_blocks >= MIN_BLOCKS
        tomo, _, tube = filament.rpartition("|")
        piece_rows.append(
            {
                "piece": piece,
                "filament": filament,
                TOMO_COL: tomo,
                TUBE_COL: tube,
                "segments": n,
                "blocks": n_blocks,
                "k": k_piece,
                "halves": None if halves is None else int(circular_distance(halves[0], halves[1], lat.n)),
                "own_vs_rest": None if own_vs_rest is None else round(own_vs_rest, 2),
                "verdict": DECIDED if long_enough else UNDECIDED,
                "why": "" if long_enough else f"shorter than {min_segments} segments",
            }
        )
        block_rows += [(piece, filament, b, int(k_own[b])) for b in range(n_blocks)]
        seg_frames.append(
            pd.DataFrame(
                {"piece": piece, "shift": shifts, "block": blocks, "k": k_piece, "k_held_out": k_rest[blocks]},
                index=g.index,
            )
        )
    pieces = pd.DataFrame(piece_rows)
    if len(pieces):
        # In the order a person reads them: by tomogram, then tube and piece as numbers.
        tube = pd.to_numeric(pieces[TUBE_COL], errors="coerce")
        part = pieces["piece"].str.rsplit("#", n=1).str[-1].astype(int)
        order = pd.DataFrame({"tomo": pieces[TOMO_COL], "tube": tube, "name": pieces[TUBE_COL], "part": part})
        pieces = pieces.loc[order.sort_values(["tomo", "tube", "name", "part"], kind="stable").index]
        pieces = pieces.reset_index(drop=True)
    block_table = pd.DataFrame(block_rows, columns=["piece", "filament", "block", "k"])
    return SeamVote(
        lattice=lat,
        pieces=pieces,
        segments=pd.concat(seg_frames) if seg_frames else pd.DataFrame(),
        n_segments=len(segs),
        n_left_out=left_out,
        params={
            "radius_ang": lat.radius_ang,
            "block_segments": block_segments,
            "min_segments": min_segments,
            "break_ang": float(break_ang),
            "max_skip": int(max_skip),
        },
        checks=_checks(pieces, block_table, lat, block_segments) if len(pieces) else {},
    )


def registered_rows(v: SeamVote, scored: pd.DataFrame, *, held_out: bool) -> pd.DataFrame:
    """One row per segment of the decided pieces: the copy on the registration its piece voted, or, with
    `held_out`, the one the piece voted without the segment's own block and that block's neighbours. The row is
    the scored copy's (its images, pose and origin), without the scores, with its piece and the registration."""
    decided = set(v.pieces.loc[v.pieces["verdict"] == DECIDED, "piece"])
    seg = v.segments[v.segments["piece"].isin(decided)]
    k = seg["k_held_out" if held_out else "k"].to_numpy()
    if held_out and (k < 0).any():
        raise ValueError("a decided piece has a block with no block beyond its neighbours to vote for it")
    wanted = pd.DataFrame(
        {
            PARENT_COL: seg.index.to_numpy(),
            INDEX_COL: (k - seg["shift"].to_numpy()) % v.lattice.n,
            # A star's value cannot hold "#" (a comment from there on): tomogram/tube/piece.
            PIECE_COL: seg["piece"].str.replace("|", "/", regex=False).str.replace("#", "/", regex=False).to_numpy(),
            REGISTRATION_COL: k,
        }
    )
    labelled = scored.assign(**{INDEX_COL: pd.to_numeric(scored[INDEX_COL]).astype(int)})
    rows = labelled.merge(wanted, on=[PARENT_COL, INDEX_COL], how="inner", validate="one_to_one")
    if len(rows) != len(wanted):
        raise ValueError(f"{len(wanted) - len(rows):,} segment(s) have no copy on the registration they were given")
    return rows.drop(columns=[c for c in REFINED_SCORE_COLS if c in rows.columns]).reset_index(drop=True)


__all__ = [
    "BLOCK_SEGMENTS",
    "DECIDED",
    "MIN_BLOCKS",
    "PIECE_COL",
    "REGISTRATION_COL",
    "SCORE_COL",
    "UNDECIDED",
    "Lattice",
    "SeamVote",
    "chain",
    "circular_distance",
    "lattice_of",
    "registered_rows",
    "registration",
    "score_table",
    "sites_alike",
    "vote",
]

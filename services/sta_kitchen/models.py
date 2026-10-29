"""The STA kitchen's records: one species' pool of picks in a home project, what feeds it, and the
recipe being drafted on it.

A pool lives at ``<home>/Analytics/sta_kitchen/<species>/pool.json`` (`services/sta_kitchen/store.py`). Its
members are read, never written: a member in another project is read through a snapshot
(`services/sta_kitchen/snapshots.py`). The draft is what the filter strip shows; only Make set turns it
into a particle set.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from services.particles.records import current_user, now_iso

POOL_SCHEMA = 1
# A cutout's Z slab, in Å: 5 px at 6.2 Å/px, the commonest reconstruction pixel size.
DEFAULT_SLAB_A = 30.0


class MemberKind(StrEnum):
    TM = "tm"  # a candidate-extract job: its candidates.star, with its subtomo extractions joined
    LIST = "list"  # a pick list: hand-picked, imported or merged, with its own extraction joined


class PickSource(StrEnum):
    """Where a pick came from; a hand pick wins a clash with any other at Make set."""

    HAND = "hand"
    IMPORTED = "imported"
    MERGED = "merged"
    TM = "tm"


# Dedup priority at Make set, lower wins.
SOURCE_PRIORITY = {PickSource.HAND: 0, PickSource.IMPORTED: 1, PickSource.MERGED: 2, PickSource.TM: 3}


class Verdict(StrEnum):
    KEEP = "keep"
    DROP = "drop"
    CLEAR = "clear"  # an event only: the particle goes back to no verdict


class PoolMember(BaseModel):
    key: str  # stable: project token + kind + instance or list id
    project_path: str
    project_label: str
    species_id: str  # the species in the member's own project
    kind: MemberKind
    ref: str  # kind tm: the candidate-extract instance; kind list: "<species>/<tomo>/<slug>"
    label: str = ""
    tomos: list[str] | None = None  # the tomograms taken; None = all
    # When its existing curation (a `_filtered.star`) was read into the pool's verdicts: once only.
    seeded_at: str | None = None
    added_at: str = Field(default_factory=now_iso)
    added_by: str = Field(default_factory=current_user)


class RangeFilter(BaseModel):
    """Passes `lo ≤ v ≤ hi` (an open end when None); a row with no value passes when `keep_missing`."""

    kind: Literal["range"] = "range"
    col: str
    lo: float | None = None
    hi: float | None = None
    keep_missing: bool = True


class CategoryFilter(BaseModel):
    """Passes the rows whose value is one of `allowed` (category names, so a rebuilt table keeps them)."""

    kind: Literal["category"] = "category"
    col: str
    allowed: list[str] = Field(default_factory=list)


class SetFilter(BaseModel):
    """Membership in a particle set of the home project, matched by uid."""

    kind: Literal["set"] = "set"
    set_id: str
    mode: Literal["in", "out"] = "in"


class TomogramFilter(BaseModel):
    """Every pick on one of the `excluded` reconstructions (frame keys) fails; a frame added to the pool
    later passes. The σ-by-tomogram strip draws and edits it."""

    kind: Literal["tomograms"] = "tomograms"
    excluded: list[str] = Field(default_factory=list)


Filter = Annotated[RangeFilter | CategoryFilter | SetFilter | TomogramFilter, Field(discriminator="kind")]


class PostOps(BaseModel):
    """Run in order on what passes the filters, per frame and tomogram, best σ first."""

    nn_a: float | None = None  # no two closer than this, the better kept
    top_n: int | None = None  # at most this many


class Scope(BaseModel):
    """Which subsets the draft is narrowed to: in at least one of `include` (none named: no limit) and in
    none of `exclude`. Subset ids."""

    include: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)


class Draft(BaseModel):
    filters: list[Filter] = Field(default_factory=list)
    post: PostOps = Field(default_factory=PostOps)
    scope: Scope = Field(default_factory=Scope)


class Subset(BaseModel):
    """A named, coloured list of pool uids (the uids live in ``subsets/<id>.txt``). Subsets may overlap,
    except those of one group."""

    id: str
    name: str
    color: str
    group: str | None = None  # a SubsetGroup id
    created_at: str = Field(default_factory=now_iso)
    created_by: str = Field(default_factory=current_user)


class SubsetGroup(BaseModel):
    """Subsets kept disjoint: moving a pick into one of them takes it out of the others."""

    id: str
    name: str
    created_at: str = Field(default_factory=now_iso)
    created_by: str = Field(default_factory=current_user)


class KitchenPool(BaseModel):
    schema_version: int = POOL_SCHEMA
    species_id: str
    members: list[PoolMember] = Field(default_factory=list)
    draft: Draft = Field(default_factory=Draft)
    box_a: float | None = None  # cutout edge in Å; None: CUTOUT_BOX_FACTOR × the species diameter
    slab_a: float = DEFAULT_SLAB_A
    # The user's choice of frame for an acquisition seen in frames that cannot be pooled
    # (acquisition label -> frame group); Make set waits for one per such acquisition.
    frame_choice: dict[str, str] = Field(default_factory=dict)
    # The one source (member key) taken for an acquisition fed by several (acquisition key -> member key);
    # Make set waits for one per such acquisition.
    source_choice: dict[str, str] = Field(default_factory=dict)
    subsets: list[Subset] = Field(default_factory=list)
    groups: list[SubsetGroup] = Field(default_factory=list)
    created_at: str = Field(default_factory=now_iso)
    created_by: str = Field(default_factory=current_user)

    def member(self, key: str) -> PoolMember | None:
        return next((m for m in self.members if m.key == key), None)

    def subset(self, sid: str) -> Subset | None:
        return next((x for x in self.subsets if x.id == sid), None)

    def group(self, gid: str) -> SubsetGroup | None:
        return next((g for g in self.groups if g.id == gid), None)

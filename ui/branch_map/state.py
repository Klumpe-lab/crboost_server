"""The map's page state: the job being added, what has been chosen for it, the input whose sources are being
chosen, the cards shown large, the tilt series picked, and how the map is drawn (magazines, the folded start, a
band per species). Per browser tab; what is worth keeping across reloads (the large cards, how it is drawn) is
written to the user's storage by the view."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class MapState:
    adding: str | None = None  # the kind of the job being added
    method: str | None = None  # its method parameter's value, raw
    species_id: str | None = None  # the species a particle job was given before its card appeared
    picks: dict[str, str] = field(default_factory=dict)  # slot key → job id, set by hand
    params: dict[str, object] = field(default_factory=dict)  # the new job's parameters typed on the card
    params_rev: int = 0
    open_slot: tuple[str, str] | None = None  # (node, slot key): the input whose sources are being chosen
    trunk: bool = True  # the shared start folded into one card
    strips: bool = True  # the tilt-series magazine above every card
    by_species: bool = False  # a band per species, each its own grid
    folded: set[str] = field(default_factory=set)  # the species bands folded, by species id ("" = none)
    all_large: bool = False
    large: set[str] = field(default_factory=set)  # the cards shown large, by node id
    # The tilt series picked.
    active_ts: str | None = None

    def signature(self) -> tuple:
        """What the render reads of this state."""
        return (
            self.adding,
            self.method,
            self.species_id,
            tuple(sorted(self.picks.items())),
            self.params_rev,
            self.open_slot,
            self.trunk,
            self.strips,
            self.by_species,
            tuple(sorted(self.folded)),
            self.all_large,
            tuple(sorted(self.large)),
            self.active_ts,
        )

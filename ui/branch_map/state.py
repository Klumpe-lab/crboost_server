"""The map's page state: the job being added, what has been chosen for it, the cards shown large, where dragged
cards were left, the view the browser last reported, the tilt series picked and the picker's state. Per browser
tab; what is worth keeping across reloads (the view, the dragged cards, the large cards) is written to the user's
storage by the view."""

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
    open_slot: tuple[str, str] | None = None  # (node, slot key): the input whose candidates are fanned out
    trunk: bool = True  # the shared start folded into one card
    strips: bool = True  # the tilt-series magazine above every card
    all_large: bool = False
    large: set[str] = field(default_factory=set)  # the cards shown large, by node id
    moved: dict[str, tuple[float, float]] = field(default_factory=dict)  # where a dragged card was left
    # The view, as the browser last reported it; None until the first fit.
    zoom: float | None = None
    tx: float = 0.0
    ty: float = 0.0
    # The tilt series picked, and the picker's state.
    active_ts: str | None = None
    picker_open: bool = False
    picker_pinned: bool = False

    def signature(self) -> tuple:
        """What the render reads of this state; not the view and the dragged positions, which the browser keeps
        and a redraw only repeats."""
        return (
            self.adding,
            self.method,
            self.species_id,
            tuple(sorted(self.picks.items())),
            self.open_slot,
            self.trunk,
            self.strips,
            self.all_large,
            tuple(sorted(self.large)),
            self.active_ts,
            self.picker_open,
            self.picker_pinned,
        )

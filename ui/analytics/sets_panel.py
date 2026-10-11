"""The particle sets of one species as a lineage: each set under the set it was made from, with its
count, recipe, parent, who made it and when, whether its parent changed since (stale), which jobs
read it, and its actions. Hover a name for the funnel, notes and provenance.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from nicegui import ui

from services.particles.particle_sets import ParticleSet, SetLevel
from ui.analytics.common import HINT_CLS, TOOLTIP_STYLE
from ui.components.buttons import house_button
from ui.components.chip import render_chip
from ui.styles import MONO, SANS

_LEVEL_TIP = {
    SetLevel.COORDINATES: "Positions only: run Subtomo extraction (or the Warp export) on it to get particles.",
    SetLevel.EXTRACTED: "Particles with images: Reconstruct particle and Class 3D read it directly.",
}


def lineage(sets: list[ParticleSet]) -> list[tuple[ParticleSet, int]]:
    """(set, depth) in tree order: every set right after its parent set, roots in creation order."""
    ids = {s.id for s in sets}
    children: dict[str, list[ParticleSet]] = {}
    roots: list[ParticleSet] = []
    for s in sets:
        parent_ids = [p.set_id for p in s.recipe.parents if p.kind == "set" and p.set_id in ids]
        if parent_ids:
            children.setdefault(parent_ids[0], []).append(s)
        else:
            roots.append(s)
    out: list[tuple[ParticleSet, int]] = []

    def walk(s: ParticleSet, depth: int) -> None:
        out.append((s, depth))
        for c in children.get(s.id, []):
            walk(c, depth + 1)

    for r in roots:
        walk(r, 0)
    return out


def _detail(s: ParticleSet, stale: list[str]) -> str:
    lines = [f"{s.name} — {s.n_particles:,} particles in {s.n_tomograms} tomograms ({s.level.value})"]
    lines.append(f"recipe: {s.recipe.op.value} · {s.recipe.describe()}")
    for p in s.recipe.parents:
        lines.append(f"parent: {p.label} ({p.particles_star})")
    if s.funnel:
        lines.append("funnel:")
        lines += [f"  {f.label}: {f.kept:,} kept" + (f" (−{f.removed:,})" if f.removed else "") for f in s.funnel]
    lines += [f"note: {n}" for n in s.notes]
    if stale:
        lines.append("stale: changed since this set was made — " + ", ".join(stale))
    lines.append(f"made by {s.created_by}, {s.created_at.replace('T', ' ')} · {s.module} v{s.schema_version}")
    lines.append(f"files: {s.optimisation_set}")
    return "\n".join(lines)


class SetsPanel:
    def __init__(
        self,
        sets: list[ParticleSet],
        *,
        readers: dict[str, list[str]],
        stale: dict[str, list[str]],
        on_delete: Callable[[ParticleSet], Any],
        actions: list[tuple[str, str, Callable[[ParticleSet], Any], Callable[[ParticleSet], bool]]] = (),
    ) -> None:
        self.sets = sets
        self.readers = readers
        self.stale = stale
        self._on_delete = on_delete
        # (label, tooltip, handler, applies-to) per extra action.
        self._actions = list(actions)

    def build(self) -> None:
        if not self.sets:
            ui.label(
                "No particle sets yet. Make one from the cut above (Make subset…); every set records its parent and "
                "rules, and the parent is never changed."
            ).classes(HINT_CLS)
            return
        for s, depth in lineage(self.sets):
            self._row(s, depth)

    def _row(self, s: ParticleSet, depth: int) -> None:
        stale = self.stale.get(s.id, [])
        readers = self.readers.get(s.id, [])
        with (
            ui.row()
            .classes("w-full items-center gap-2 no-wrap")
            .style(f"min-height: 24px; border-bottom: 1px solid #f1f5f9; padding-left: {8 + 14 * depth}px;")
        ):
            if depth:
                ui.label("└").style("color: #cbd5e1; font-size: 10px;")
            with ui.label(s.name).style(f"{SANS} font-size: 11px; font-weight: 600; color: #1e293b; cursor: help;"):
                ui.tooltip(_detail(s, stale)).style(TOOLTIP_STYLE)
            render_chip(
                "level",
                s.level.value,
                status="info" if s.level == SetLevel.EXTRACTED else "neutral",
                tooltip=_LEVEL_TIP[s.level],
            )
            ui.label(f"{s.n_particles:,} ({s.n_tomograms})").style(f"{MONO} font-size: 10px; color: #334155;").tooltip(
                "particles (tomograms)"
            )
            ui.label(s.recipe.describe()).style(
                f"{SANS} font-size: 10px; color: #475569; overflow: hidden; text-overflow: ellipsis; "
                "white-space: nowrap;"
            ).classes("flex-1 min-w-0")
            ui.label(s.created_at[:10]).classes(HINT_CLS).tooltip(f"{s.created_by}, {s.created_at.replace('T', ' ')}")
            if stale:
                render_chip(
                    "parent",
                    "changed",
                    status="warn",
                    tooltip="The file this set was made from changed after it was made: "
                    + ", ".join(stale)
                    + ". The set is unchanged; make it again from the new parent if that is what you want.",
                )
            if readers:
                render_chip("read by", ", ".join(readers), status="neutral", tooltip="Jobs whose input names this set.")
            for label, tip, handler, applies in self._actions:
                if applies(s):
                    house_button(label, lambda _e, s=s, h=handler: h(s), tooltip=tip)
            house_button(
                "Delete",
                lambda _e, s=s: self._on_delete(s),
                kind="danger",
                tooltip="Remove this set's files. The operation log keeps its recipe.",
            )

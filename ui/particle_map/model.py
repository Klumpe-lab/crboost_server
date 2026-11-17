"""The particle map's nodes: the project's particle jobs and its particle sets, and what each input row reads.

Which job reads which job comes from `ui/branch_map/model.project_from_state`, the Jobs view's own reading, so the
two maps never disagree; a row that reads a particle set is pointed at the set's node here. A set's rows are its
recipe's parents: a job of this project (from the class rows a class subset took, else from its footer), another
set, or something in another project, named without a cable. Nothing here touches the disk: the sets and the
class rows are what `services/particles/lineage.py` has read so far.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from services.particles.lineage import SET_NODE_PREFIX, JobParticles, LineageStore, SetNode, is_particle_job
from services.particles.lineage import set_of_input
from ui.branch_map.model import Job, Project, Slot

SET_SLOT_ACCEPTS = ("particle_set",)


@dataclass
class Row:
    """One input row of a node: the node it reads (`source`, cabled) and from which of its ports (`classes`: the
    class rows a cable leaves; empty for the footer), or a named value with no cable."""

    key: str
    label: str
    value: str
    tip: str
    state: str  # wired / named / explicit / ambiguous / missing / optional
    source: str | None = None
    classes: tuple[int, ...] = ()


@dataclass
class Node:
    id: str
    job: Job | None = None
    set: SetNode | None = None
    rows: list[Row] = field(default_factory=list)
    particles: JobParticles | None = None

    @property
    def is_set(self) -> bool:
        return self.set is not None

    @property
    def main_source(self) -> str | None:
        return self.rows[0].source if self.rows else None

    def sources(self) -> list[str]:
        return [r.source for r in self.rows if r.source]


@dataclass
class ParticleMap:
    pr: Project
    nodes: dict[str, Node]
    problems: list[str] = field(default_factory=list)

    def ident(self, nid: str) -> str:
        n = self.nodes.get(nid)
        if n is None:
            return nid
        if n.set is not None:
            return f"set {n.set.name}"
        return self.pr.ident(n.job)


def _job_value(pr: Project, src: Job) -> str:
    return f"{pr.names.get(src.kind, src.kind)} {src.number or src.id}"


def _job_rows(pm_pr: Project, j: Job, jm, sets: dict[str, SetNode], on_map: set[str]) -> list[Row]:
    rows = []
    main_src = j.inputs[0].source if j.inputs else None
    for i, r in enumerate(j.inputs):
        slot = r.slot
        gloss = pm_pr.slot_gloss.get(slot.label, "")
        set_id = set_of_input(jm, slot.key) if jm is not None else None
        if set_id is not None:
            s = sets.get(set_id)
            if s is not None:
                rows.append(
                    Row(
                        slot.key,
                        slot.label,
                        f"set {s.name}",
                        f"{slot.label} ← set {s.name} ({s.id}) · {gloss}",
                        "wired",
                        SET_NODE_PREFIX + s.id,
                    )
                )
            else:
                rows.append(
                    Row(
                        slot.key,
                        slot.label,
                        f"set {set_id}",
                        f"{slot.label} ← set {set_id}, not in this project's sets (deleted, or not read yet)",
                        "named",
                    )
                )
            continue
        if r.source is not None and r.source in pm_pr.by_id:
            src = pm_pr.by_id[r.source]
            if i > 0 and slot.bundle and r.source == main_src:
                rows.append(Row(slot.key, slot.label, _job_value(pm_pr, src), f"{slot.label}: {r.how}", "wired"))
                continue  # rides the main row's cable: no cable of its own
            state = "explicit" if r.how == "set by hand" else "wired"
            tip = f"{slot.label} ← {pm_pr.ident(src)} · {r.how} · {gloss}"
            if r.source in on_map:
                rows.append(Row(slot.key, slot.label, _job_value(pm_pr, src), tip, state, r.source))
            else:
                rows.append(Row(slot.key, slot.label, _job_value(pm_pr, src), tip + " · on the Jobs map", "named"))
        elif r.value:
            rows.append(Row(slot.key, slot.label, r.value, f"{slot.label}: {r.value} · {r.how} · {gloss}", "named"))
        elif r.how == "to choose":
            rows.append(Row(slot.key, slot.label, "to choose", f"{slot.label}: not chosen yet · {gloss}", "ambiguous"))
        elif slot.required:
            rows.append(
                Row(slot.key, slot.label, "nothing", f"{slot.label}: no job provides this · {gloss}", "missing")
            )
        else:
            rows.append(
                Row(slot.key, slot.label, "optional, none", f"{slot.label}: optional, not set · {gloss}", "optional")
            )
    return rows


def _class_words(classes: tuple[int, ...]) -> str:
    if len(classes) == 1:
        return f"class {classes[0]}"
    return "classes " + " + ".join(str(c) for c in classes)


def _set_rows(pr: Project, s: SetNode, on_map: set[str], sets: dict[str, SetNode]) -> list[Row]:
    rows = []
    for i, p in enumerate(s.parents):
        key = f"from_{i}"
        stale = " · changed since the set was made" if p.stale else ""
        if p.project:
            rows.append(Row(key, "from", f"{p.project} · {p.label}", f"from {p.label} in {p.project}{stale}", "named"))
            continue
        if p.kind == "job" and p.instance_id in on_map:
            src = pr.by_id[p.instance_id]
            what = f"{_class_words(p.classes)} of " if p.classes else ""
            rows.append(
                Row(
                    key,
                    "from",
                    what + _job_value(pr, src),
                    f"from {what}{pr.ident(src)}{stale}",
                    "explicit" if p.stale else "wired",
                    p.instance_id,
                    p.classes,
                )
            )
            continue
        if p.kind == "set" and p.set_id in sets:
            rows.append(
                Row(
                    key,
                    "from",
                    f"set {sets[p.set_id].name}",
                    f"from set {sets[p.set_id].name}{stale}",
                    "explicit" if p.stale else "wired",
                    SET_NODE_PREFIX + p.set_id,
                )
            )
            continue
        where = {"merge": "a cross-project merge", "list": "a pick list"}.get(p.kind, p.kind)
        rows.append(Row(key, "from", p.label or where, f"from {where}: {p.label}{stale}", "named"))
    if not rows:
        rows.append(Row("from_0", "from", "no parent recorded", "the set's recipe names no parent", "missing"))
    return rows


def particle_map(pr: Project, state, lineage: LineageStore) -> ParticleMap:
    """The particle jobs of `pr` and the project's sets as nodes, their rows pointed at each other."""
    jobs = [j for j in pr.jobs if is_particle_job(j.job_type)]
    sets = {s.id: s for s in lineage.sets or ()}
    on_map = {j.id for j in jobs}
    nodes: dict[str, Node] = {}
    for j in jobs:
        jm = state.jobs.get(j.id)
        nodes[j.id] = Node(j.id, job=j, rows=_job_rows(pr, j, jm, sets, on_map), particles=lineage.jobs.get(j.id))
    for s in sets.values():
        nodes[SET_NODE_PREFIX + s.id] = Node(SET_NODE_PREFIX + s.id, set=s, rows=_set_rows(pr, s, on_map, sets))
    problems = [lineage.set_problem] if lineage.set_problem else []
    return ParticleMap(pr, nodes, problems)


def set_slot(row: Row) -> Slot:
    """A row as the card's input slot."""
    return Slot(row.key, row.label, SET_SLOT_ACCEPTS)


__all__ = ["Node", "ParticleMap", "Row", "particle_map", "set_slot"]

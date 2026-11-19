"""What /dev/map-ideas shows: one real project, read and never written, as the branch map's `Project` model, plus
what the top-down layouts need on top of it: the sources no job of the project made (the kitchen's particle sets, map
files), every edge between jobs, and the words each job is found by."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

from services import array_status_cache
from services.project_resolve import resolve_project
from services.project_state import ProjectState
from services.tilt_series import get_registry_for, preload_registry
from ui.branch_map.facts import FactsStore, set_parent
from ui.branch_map.model import Job, Project, project_from_state

logger = logging.getLogger(__name__)

DEFAULT_PROJECT = "AGG412_STA"
API_BASE = Path("/groups/klumpe/_crboost_data_api")

# More words a job type is found by, besides its name and its kind.
ALIASES = {
    "importmovies": "movies frames",
    "fsMotionAndCtf": "motion ctf fs",
    "tsImport": "tilt series import",
    "tiltFilter": "filter tilts",
    "aligntiltsWarp": "align aretomo imod",
    "missAlign": "missalignment refine alignment",
    "tsCtf": "ctf defocus",
    "tsReconstruct": "reconstruct tomograms recon",
    "denoisetrain": "denoise train cryocare",
    "denoisepredict": "denoise predict",
    "segment": "segmentation membranes",
    "templatematching": "tm template matching pytom",
    "tmextractcand": "picks candidates pick",
    "filamentPick": "filament pick",
    "subtomoExtraction": "extract subtomograms",
    "warpExport": "export particles",
    "reconstructParticle": "reconstruct average map",
    "class3d": "classification classify classes",
    "refine3d": "refinement autorefine",
    "maskCreate": "mask",
    "postprocess": "pp sharpen fsc",
    "ctfRefine": "ctf refine",
}


@dataclass
class Source:
    """Something a job reads as its main input that no job of the project made: a particle set from the kitchen,
    a map file."""

    id: str  # set:<set id> / file:<name>
    kind: str  # set / file
    label: str
    detail: str
    readers: list[str] = field(default_factory=list)

    @property
    def shared(self) -> bool:
        return len(self.readers) > 1


@dataclass
class Edge:
    a: str  # what is read: a job, or a shared source
    b: str  # the job reading it
    kind: str  # main / side
    slot: str  # the input's key on b


@dataclass
class MapData:
    pr: Project
    path: Path
    state: ProjectState
    sources: dict[str, Source]
    source_of: dict[str, str]  # job id -> the source its main input reads
    edges: list[Edge]
    store: FactsStore
    series: list
    stamp: tuple

    def job(self, nid: str) -> Job:
        return self.pr.by_id[nid]

    def kids(self, nid: str) -> list[str]:
        """The jobs reading `nid` as their main input, in the order they were made."""
        if nid in self.sources:
            out = list(self.sources[nid].readers)
        else:
            out = [c.id for c in self.pr.main_children(self.pr.by_id[nid]) if c.id != nid]
        return sorted(out, key=self.order)

    def order(self, nid: str) -> tuple:
        if nid in self.sources:
            return min((self.order(r) for r in self.sources[nid].readers), default=(9, "", nid))
        j = self.pr.by_id[nid]
        return (0, j.number, nid) if j.number else (1, "", nid)

    def is_root(self, nid: str) -> bool:
        """A job no other job's main output feeds: it starts from raw data, a file, or a set."""
        j = self.pr.by_id[nid]
        return j.parent is None or j.parent not in self.pr.by_id or j.parent == nid

    def preds(self, nid: str) -> list[str]:
        """Everything a node reads that is on the map: its main parent or shared source, its side inputs."""
        return [e.a for e in self.edges if e.b == nid]

    def species_of(self, nid: str) -> str:
        if nid in self.sources:
            sp = {self.pr.by_id[r].species for r in self.sources[nid].readers}
            return next(iter(sp)) if len(sp) == 1 else ""
        return self.pr.by_id[nid].species or ""


def resolve_path(name: str) -> Path:
    """A project by absolute path, by name the way the app resolves names, or under the API's data folder."""
    if name.startswith("/"):
        return Path(name)
    res = resolve_project(name)
    if res["success"]:
        return Path(res["path"])
    candidate = API_BASE / name
    if (candidate / "project_params.json").is_file():
        return candidate
    raise FileNotFoundError(res["error"])


def _set_ids(state: ProjectState) -> set[str]:
    out = set()
    for jm in state.jobs.values():
        for override in (jm.source_overrides or {}).values():
            if override.startswith("mergedSources:"):
                out.add(override.split(":", 1)[1].removeprefix("particle_set__"))
        for recorded in (jm.paths or {}).values():
            if "/Analytics/sets/" in str(recorded):
                out.add(str(recorded).split("/Analytics/sets/", 1)[1].split("/", 1)[0])
    return out


def _set_counts(path: Path, set_ids: set[str]) -> dict[str, tuple[str, int, int]]:
    """Per set id: its name, particles and tomograms, from its set.json."""
    out = {}
    for s in set_ids:
        try:
            with open(path / "Analytics" / "sets" / s / "set.json") as f:
                d = json.load(f)
            out[s] = (str(d.get("name", s)), int(d.get("n_particles") or 0), int(d.get("n_tomograms") or 0))
        except (OSError, ValueError, TypeError):  # a set written by an older kitchen: its name only
            out[s] = (s.split("_", 1)[-1], 0, 0)
    return out


def _sources(pr: Project, set_info: dict[str, tuple[str, int, int]]) -> tuple[dict[str, Source], dict[str, str]]:
    by_name = {f"set {name}": (sid, name, n, t) for sid, (name, n, t) in set_info.items()}
    sources: dict[str, Source] = {}
    source_of: dict[str, str] = {}
    for j in pr.jobs:
        if not j.inputs:
            continue
        r = j.inputs[0]
        if r.source is not None and r.source in pr.by_id and r.source != j.id:
            continue
        if not r.value:
            continue
        if r.value.startswith("set "):
            sid, name, n, t = by_name.get(r.value, (r.value[4:], r.value[4:], 0, 0))
            key = f"set:{sid}"
            detail = "particle set" + (f" · {n:,} particles" if n else "") + (f" · {t} tomograms" if t else "")
            src = sources.setdefault(key, Source(key, "set", name, detail))
        else:
            key = f"file:{r.value}"
            src = sources.setdefault(key, Source(key, "file", r.value, f"{r.slot.label} file"))
        src.readers.append(j.id)
        source_of[j.id] = key
    return sources, source_of


def _edges(pr: Project, sources: dict[str, Source], source_of: dict[str, str]) -> list[Edge]:
    out: list[Edge] = []
    for j in pr.jobs:
        main_slot = j.inputs[0].slot.key if j.inputs else ""
        if j.parent is not None and j.parent in pr.by_id and j.parent != j.id:
            out.append(Edge(j.parent, j.id, "main", main_slot))
        elif j.id in source_of and sources[source_of[j.id]].shared:
            out.append(Edge(source_of[j.id], j.id, "main", main_slot))
        for r in j.inputs[1:]:
            if r.source is None or r.source not in pr.by_id or r.source in (j.parent, j.id) or r.slot.quiet:
                continue
            out.append(Edge(r.source, j.id, "side", r.slot.key))
    return out


async def load(name: str) -> MapData:
    """The project `name`, read-only: its state, its array jobs' task states, the sets its jobs read, the branch map's
    model of it, and an empty facts store of its own (the live map's is left alone)."""
    path = await asyncio.to_thread(resolve_path, name)
    state = await asyncio.to_thread(ProjectState.load, path / "project_params.json", read_only=True)
    await array_status_cache.refresh(path, state.jobs)
    set_ids = _set_ids(state)
    parents = await asyncio.to_thread(lambda: {s: set_parent(path, s) for s in set_ids})
    set_info = await asyncio.to_thread(_set_counts, path, set_ids)
    pr = await asyncio.to_thread(project_from_state, state, path, set_parents=parents)
    sources, source_of = _sources(pr, set_info)
    edges = _edges(pr, sources, source_of)
    try:
        await preload_registry(path)  # its first read, off the loop
        registry = get_registry_for(path)
        series = list(registry.all_tilt_series())
        stamp = (registry.loaded_stamp, len(series))
    except Exception:  # a project without tilt series: the large cards show the job's results only
        logger.exception("map ideas: the registry of %s", path)
        series, stamp = [], (0, 0)
    return MapData(pr, path, state, sources, source_of, edges, FactsStore(path), series, stamp)


# ── what the browser needs: the edges, and what each node is found by ────────


def _words(*parts: str) -> list[str]:
    out: list[str] = []
    for p in parts:
        for w in re.split(r"[^a-z0-9]+", (p or "").lower()):
            if w and w not in out:
                out.append(w)
    return out


def graph_json(d: MapData) -> str:
    pr = d.pr
    nodes = {}
    for j in pr.jobs:
        name = pr.names.get(j.kind, j.kind)
        sp = pr.species_names.get(j.species or "", j.species or "")
        src = d.sources.get(d.source_of.get(j.id, ""))
        nodes[j.id] = {
            "name": name,
            "num": j.number,
            "kind": j.kind.lower(),
            "status": j.status,
            "sp": sp,
            "first": _words(name)[0] if _words(name) else "",
            "nw": _words(name, j.kind, ALIASES.get(j.kind, "")),
            "meta": _words(sp, j.status, pr.method_of(j), " ".join(pr.chips(j))),
            "src": _words(src.label) if src is not None else [],
            "chips": " · ".join(pr.chips(j)),
        }
    for s in d.sources.values():
        nodes[s.id] = {
            "name": f"{s.kind} {s.label}",
            "num": "",
            "kind": s.kind,
            "status": "source",
            "sp": "",
            "first": s.kind,
            "nw": _words(s.kind, s.label),
            "meta": [],
            "src": [],
            "chips": s.detail,
        }
    edges = [{"a": e.a, "b": e.b, "k": e.kind, "s": e.slot} for e in d.edges]
    return json.dumps({"nodes": nodes, "edges": edges})

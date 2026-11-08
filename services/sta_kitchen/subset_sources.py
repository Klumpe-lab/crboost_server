"""What a kitchen subset is made of, to match its tomograms again in a new project: the pool's subsets, and per
source project of a subset its tomograms, the jobs they came from and the values a new project takes with them
(`tomogram_import.check_job_sources`). Read by the agent API; nothing here changes a pool's members or subsets.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from services.configs.config_service import get_config_service
from services.job_lineage import job_dirs
from services.particles.analytics_store import tm_instance_for_ce
from services.result import err, need, ok
from services.sta_kitchen import export, store
from services.sta_kitchen.models import MemberKind
from services.sta_kitchen.service import loaded, open_pool, rebuild, subset_mask
from services.sta_kitchen.snapshots import project_view
from services.tomogram_import import TOMOGRAM_JOB_TYPES, check_job_sources


def _has_pool(home: Path, species_id: str) -> bool:
    return (store.kitchen_dir(home, species_id) / store.POOL_FILE).is_file()


async def list_subsets(home: Path, species_id: str) -> dict[str, Any]:
    """The subsets of the kitchen of `species_id` in `home` (name, group, picks) and its members: the open
    pool's when this server has it open, else read from its files."""
    lp = loaded(home, species_id)
    if lp is not None:
        pool, held = lp.pool, lp.subset_uids
    else:
        if not await asyncio.to_thread(_has_pool, home, species_id):
            return err(f"{home.name} has no kitchen for species {species_id}.")
        pool = await asyncio.to_thread(store.load_pool, home, species_id)
        held = {s.id: await asyncio.to_thread(store.read_subset, home, species_id, s.id) for s in pool.subsets}
    groups = {g.id: g.name for g in pool.groups}
    return ok(
        subsets=[
            {"id": s.id, "name": s.name, "group": groups.get(s.group or ""), "picks": len(held.get(s.id, ()))}
            for s in pool.subsets
        ],
        members=[{"project": m.project_label, "label": m.label or m.ref, "kind": m.kind.value} for m in pool.members],
    )


async def subset_sources(home: Path, species_id: str, subset: str) -> dict[str, Any]:
    """The source projects of a subset's picks (`subset`: its id or name), from `export.plan`'s per-member
    grouping. Opens the pool as the kitchen does (its members' cached tables built), and never adds a member,
    changes a subset or records a verdict. See `_sources` for the answer."""
    if not await asyncio.to_thread(_has_pool, home, species_id):
        return err(f"{home.name} has no kitchen for species {species_id}.")
    lp = await open_pool(home, species_id)
    await rebuild(lp)
    sub = next((s for s in lp.pool.subsets if subset in (s.id, s.name)), None)
    if sub is None:
        listed = ", ".join(s.name for s in lp.pool.subsets) or "none"
        return err(f"No subset {subset} in the kitchen of {species_id} (its subsets: {listed}).")
    if lp.table is None:
        return err(f"The kitchen of {species_id} has no table: none of its members could be built.")
    mask = subset_mask(lp.table, lp.subset_uids.get(sub.id, set()))
    if not mask.any():
        return err(f"Subset {sub.name} holds no pick the pool still has.")
    sel = SimpleNamespace(final=mask, n=int(mask.sum()), kept_by_hand=0, dropped_by_hand=0)
    planned = await asyncio.to_thread(export.plan, lp, sel, name=sub.name, diameter_a=None)
    binds = list(get_config_service().config.container_binds)
    res = await asyncio.to_thread(_sources, lp, planned, binds)
    if res["success"]:
        res["subset"] = {"id": sub.id, "name": sub.name, "picks": int(mask.sum())}
    return res


def _job_of(path: str, dirs: dict[Path, str]) -> str | None:
    """The job whose directory holds `path`, or None."""
    if not path:
        return None
    p, resolved = Path(path), Path(path).resolve()
    for d, iid in dirs.items():
        if p.is_relative_to(d) or resolved.is_relative_to(d.resolve()):
            return iid
    return None


def _sources(lp, planned: dict[str, Any], binds: list[str]) -> dict[str, Any]:
    """Per source project: its tomograms with the job whose directory holds each one's reconstruction (for a
    template-matching member, the job its TM read; for a pick list, where the frame's reconstruction lives) and
    the job whose directory holds its tilt-series star; what is missing. Then `tomograms_from` for
    `create_project`, the values they bring and their groups (`check_job_sources`), and the template (D8): the
    one the species' catalog entry carries, else a `needs` item listing the candidates with the templates the
    sources' TM jobs used."""
    table = lp.table
    frame_of: dict[tuple[str, str], dict] = {}
    for fr in table.frames:
        for key in fr["members"]:
            frame_of.setdefault((key, fr["tomo"]), fr)
    projects: dict[str, dict[str, Any]] = {}
    picks: dict[tuple[str, str], list[str]] = {}
    used: list[dict[str, str]] = []
    for m in planned["members"]:
        view = project_view(Path(m["project"]))
        dirs = job_dirs(view.state)
        src = projects.setdefault(
            m["project"],
            {"project": m["project"], "label": m["project_label"], "members": [], "tomograms": [], "missing": []},
        )
        src["members"].append(m["label"])
        if m["kind"] == MemberKind.TM.value:
            tm = tm_instance_for_ce(view.state, m["ref"])
            path = getattr(view.state.jobs.get(tm), "template_path", "") if tm else ""
            if path and all(u["path"] != path for u in used):
                used.append({"project": m["project_label"], "job": tm, "path": path})
        for tomo in m["tomos"]:
            fr = frame_of.get((m["key"], tomo)) or {}
            recon, ts_star = fr.get("recon", ""), fr.get("ts_star", "")
            job = _job_of(recon, dirs)
            job_type = view.state.jobs[job].job_type.value if job in view.state.jobs else None
            src["tomograms"].append(
                {
                    "tomogram": tomo,
                    "reconstruction": recon,
                    "tomogram_job": job,
                    "tomogram_job_type": job_type,
                    "tilt_series_star": ts_star,
                    "tilt_series_job": _job_of(ts_star, dirs),
                    "picks": int(sum(1 for t in m["tomo_of_row"] if t == tomo)),
                }
            )
            if not fr.get("recon_exists"):
                src["missing"].append(f"{tomo}: its reconstruction {recon or '(none named)'} is gone")
            elif job_type not in TOMOGRAM_JOB_TYPES:
                src["missing"].append(f"{tomo}: {recon} is no Reconstruct or Denoise Predict job's tomogram")
            elif not ts_star:
                src["missing"].append(f"{tomo}: its row names no tilt-series star")
            else:
                names = picks.setdefault((m["project"], job), [])
                if tomo not in names:
                    names.append(tomo)
    tomograms_from = [{"project": p, "job": j, "names": sorted(n)} for (p, j), n in picks.items()]
    needs: list[dict[str, Any]] = []
    values, problems = None, []
    if tomograms_from:
        checked = check_job_sources(tomograms_from, binds)
        if checked["success"]:
            values = checked["values"]
        else:
            needs += checked.get("needs", [])
            problems = checked.get("problems") or ([] if checked.get("needs") else [checked["error"]])
    template, template_needs = _template(lp, used)
    return ok(
        sources=list(projects.values()),
        tomograms_from=tomograms_from,
        values=values,
        problems=problems,
        template=template,
        templates_used=used,
        needs=needs + template_needs,
        blockers=planned["blockers"],
        notes=planned["notes"],
    )


def _template(lp, used: list[dict[str, str]]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """The template a new project's species takes (D8): the one the home species' catalog entry carries; with
    several or none, or no catalog entry, a `needs` item listing the candidates and the templates the sources'
    TM jobs used."""
    from services.particles import catalog

    sp = project_view(lp.home).state.get_species(lp.species_id)
    if sp is None:
        return None, [need("choice", "species", f"the home project has no species {lp.species_id}", options=used)]
    if not sp.catalog_id or not catalog.is_enabled():
        options = [{"template_id": t.id, "path": t.template_path} for t in sp.templates] + used
        why = (
            f"species {sp.name} has no lab catalog entry: make the new project's species with create_species and "
            "one of these templates (make_template, add_template)"
        )
        return None, [need("choice", "template", why, options=options)]
    try:
        entry = catalog.read_species(sp.catalog_id)
    except (OSError, ValueError) as e:
        return None, [need("choice", "template", f"catalog entry {sp.catalog_id}: {e}", options=used)]
    found = {"catalog_id": entry.catalog_id, "version": entry.version}
    if len(entry.templates) == 1:
        return {**found, "file": entry.templates[0].template_path}, []
    options = [
        {"file": t.template_path, "selected": t.id == entry.selected_template_id} for t in entry.templates
    ] + used
    why = (
        f"catalog entry {entry.catalog_id} v{entry.version} carries {len(entry.templates) or 'no'} templates: "
        "which one the new project's TM takes"
    )
    return found, [need("choice", "template", why, options=options)]

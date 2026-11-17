"""The kitchen's actions for the agent API (`api/handlers.py`): the ones the kitchen tab does, on the same
`LoadedPool` an open tab holds, so the tab shows what they did.

Sources: what a pool can read (`offers`), `add_sources`, `remove_source`. Subsets and groups: `create_subset`
(the picks a recipe keeps now, as "+ kept" does), `edit_subset`, `delete_subset`, `create_group`,
`rename_group`, `delete_group`. Sets: `plan_set` (what Make set would write), `make_set`, `make_sets` (one per
subset of a group). `state` reads the pool.

A recipe (filters, post-ops, subsets in and out) is given with each selection and never written into the
pool's draft: the user's filter strip stays as they left it. The hand verdicts and the source and frame choices
apply as they do in the tab. Every function returns `ok(...)` or `err(...)`.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import numpy as np

from services.result import err, ok
from services.sta_kitchen import service, store
from services.sta_kitchen.members import matching_species, member_key, offered_members
from services.sta_kitchen.models import Draft, PoolMember, Scope, SubsetGroup
from services.sta_kitchen.pool import CAT_COLUMNS, NUM_COLUMNS
from services.sta_kitchen.snapshots import project_view

logger = logging.getLogger(__name__)

# Categories whose values `state` lists in full; the others (tomograms, reconstructions) are counted.
LISTED_CATEGORIES = ("project", "member", "source", "extracted", "tm_marker")


def _has_pool(home: Path, species_id: str) -> bool:
    return (store.kitchen_dir(home, species_id) / store.POOL_FILE).is_file()


async def _species_ok(home: Path, species_id: str) -> str:
    view = await asyncio.to_thread(project_view, home)
    if view.state.get_species(species_id) is None:
        listed = ", ".join(sp.id for sp in view.state.species_registry) or "none"
        return f"{home.name} has no species {species_id} (its species: {listed})."
    return ""


async def _open(home: Path, species_id: str, *, create: bool = False) -> tuple[service.LoadedPool | None, str]:
    """The pool, opened and its table built as the kitchen tab opens it; why not when there is none."""
    why = await _species_ok(home, species_id)
    if why:
        return None, why
    if (
        not create
        and service.loaded(home, species_id) is None
        and not await asyncio.to_thread(_has_pool, home, species_id)
    ):
        return None, f"{home.name} has no kitchen for {species_id} yet: add_kitchen_sources opens one."
    lp = await service.open_pool(home, species_id)
    await service.rebuild(lp)
    return lp, ""


def check_draft(draft: Draft) -> str:
    """Why a recipe cannot be read on a pool table, or ""."""
    for f in draft.filters:
        if f.kind == "range" and f.col not in NUM_COLUMNS:
            return f"No number column {f.col!r} to filter on (get_kitchen lists them)."
        if f.kind == "category" and f.col not in CAT_COLUMNS:
            return f"No category column {f.col!r} to filter on (get_kitchen lists them)."
    return ""


def _subset_id(lp: service.LoadedPool, ref: str) -> str | None:
    hit = next((x for x in lp.pool.subsets if ref in (x.id, x.name)), None)
    return hit.id if hit is not None else None


def _group(lp: service.LoadedPool, ref: str) -> SubsetGroup | None:
    return next((g for g in lp.pool.groups if ref in (g.id, g.name)), None)


def _resolve_scope(lp: service.LoadedPool, draft: Draft) -> tuple[Draft, str]:
    """The recipe with its subsets named by id; why not when one is not in the pool."""
    ids = {}
    for ref in [*draft.scope.include, *draft.scope.exclude]:
        sid = _subset_id(lp, ref)
        if sid is None:
            listed = ", ".join(x.name for x in lp.pool.subsets) or "none"
            return draft, f"No subset {ref!r} in this kitchen (its subsets: {listed})."
        ids[ref] = sid
    scope = Scope(include=[ids[r] for r in draft.scope.include], exclude=[ids[r] for r in draft.scope.exclude])
    return draft.model_copy(update={"scope": scope}, deep=True), ""


def _counts(lp: service.LoadedPool, mask: np.ndarray, col: str) -> dict[str, int]:
    table = lp.table
    codes, n = np.unique(table.cat[col][mask], return_counts=True)
    names = table.categories[col]
    return {names[int(c)] if 0 <= int(c) < len(names) else "(none)": int(k) for c, k in zip(codes, n, strict=True)}


async def _kept(lp: service.LoadedPool, draft: Draft) -> tuple[np.ndarray | None, Any]:
    await service.ensure_set_masks(lp, draft)
    sel = await asyncio.to_thread(service.current_selection, lp, draft)
    return (sel.final if sel is not None else None), sel


def _members(lp: service.LoadedPool) -> list[dict[str, Any]]:
    out = []
    for m in lp.pool.members:
        st = lp.members.get(m.key)
        out.append(
            {
                "key": m.key,
                "project": m.project_label,
                "project_path": m.project_path,
                "species_there": m.species_id,
                "kind": m.kind.value,
                "ref": m.ref,
                "label": m.label or m.ref,
                "state": st.state if st is not None else "pending",
                "picks": st.n if st is not None else None,
                "error": (st.error or None) if st is not None else None,
            }
        )
    return out


def _subsets(lp: service.LoadedPool) -> list[dict[str, Any]]:
    groups = {g.id: g.name for g in lp.pool.groups}
    return [
        {"id": x.id, "name": x.name, "group": groups.get(x.group or ""), "picks": len(lp.subset_uids.get(x.id, ()))}
        for x in lp.pool.subsets
    ]


def _sets_of(home: Path, species_id: str) -> list[dict[str, Any]]:
    """The particle sets of the home project made from this pool, oldest first. Blocking."""
    from services.particles.particle_sets import SetOp, list_sets, pool_scope

    out = []
    for ps in list_sets(home):
        if ps.recipe.op != SetOp.POOL or ps.species_id != species_id:
            continue
        out.append({"id": ps.id, "name": ps.name, "picks": ps.n_particles, "subsets": pool_scope(ps)[0]})
    return out


async def state(home: Path, species_id: str, draft: Draft) -> dict[str, Any]:
    """The pool: its members (with their picks and any error), its subsets and groups, the sets made from it,
    the columns a recipe can filter on (the categories' values listed for the short ones), the open choices
    that would stop Make set, and what `draft` keeps now, per project."""
    lp, why = await _open(home, species_id)
    if lp is None:
        return err(why)
    out: dict[str, Any] = {
        "members": _members(lp),
        "subsets": _subsets(lp),
        "groups": [g.name for g in lp.pool.groups],
        "sets": await asyncio.to_thread(_sets_of, home, species_id),
        "filter_strip": service.describe_draft(lp.pool.draft.model_dump(), {x.id: x.name for x in lp.pool.subsets}),
    }
    table = lp.table
    if table is None:
        out["table"] = "no table: the pool has no member with picks yet"
        return ok(**out)
    every = np.ones(table.n, dtype=bool)
    out["picks"] = int(table.n)
    out["number_columns"] = {k: f"{c.label}" + (f" ({c.unit})" if c.unit else "") for k, c in NUM_COLUMNS.items()}
    out["category_columns"] = {
        k: (
            sorted(table.categories.get(k, []))
            if k in LISTED_CATEGORIES
            else f"{len(table.categories.get(k, []))} values"
        )
        for k in CAT_COLUMNS
    }
    out["picks_per_project"] = _counts(lp, every, "project")
    conflicts = await asyncio.to_thread(service.frame_conflicts, lp)
    open_frames = sum(1 for c in conflicts if not c["chosen"])
    open_sources = sum(1 for acq in table.source_conflicts if acq not in lp.pool.source_choice)
    if open_frames or open_sources:
        out["open_choices"] = (
            f"{open_frames} tilt series reconstructed twice without a choice of reconstruction, {open_sources} "
            "acquisitions fed by several sources without a choice: Make set waits for them (the kitchen's Sources "
            "and Make set dialogs)"
        )
    draft, why = _resolve_scope(lp, draft)
    if why:
        return err(why)
    why = check_draft(draft)
    if why:
        return err(why)
    if draft.filters or draft.scope.include or draft.scope.exclude or draft.post.nn_a or draft.post.top_n:
        kept, sel = await _kept(lp, draft)
        names = {x.id: x.name for x in lp.pool.subsets}
        out["recipe"] = service.describe_draft(draft.model_dump(), names)
        out["kept"] = int(kept.sum()) if kept is not None else 0
        out["kept_per_project"] = _counts(lp, kept, "project") if kept is not None else {}
        if sel is not None and (sel.kept_by_hand or sel.dropped_by_hand):
            out["by_hand"] = {"kept": sel.kept_by_hand, "dropped": sel.dropped_by_hand}
    return ok(**out)


# ── Sources ──────────────────────────────────────────────────────────────────────────────────────


def _species_there(view, home_path: Path, species_id: str, catalog_id: str | None, given: str | None) -> str | None:
    if given:
        return given if view.state.get_species(given) is not None else None
    if view.path == home_path:
        return species_id
    return matching_species(view, catalog_id)


async def offers(home: Path, species_id: str, projects: list[Path]) -> dict[str, Any]:
    """Per project, what the kitchen could read: its species matched by the lab-catalog id (never by name), its
    template-match jobs and pick lists of that species with their picks, and whether each is in the pool."""
    why = await _species_ok(home, species_id)
    if why:
        return err(why)
    home_view = await asyncio.to_thread(project_view, home)
    catalog_id = getattr(home_view.state.get_species(species_id), "catalog_id", None)
    lp = service.loaded(home, species_id)
    if lp is None and await asyncio.to_thread(_has_pool, home, species_id):
        lp = await service.open_pool(home, species_id)
    in_pool = {m.key for m in lp.pool.members} if lp is not None else set()
    out = []
    for path in projects:
        try:
            view = await asyncio.to_thread(project_view, path)
        except (OSError, ValueError) as e:
            out.append({"project": str(path), "error": f"{type(e).__name__}: {e}"})
            continue
        sid = _species_there(view, home_view.path, species_id, catalog_id, None)
        row: dict[str, Any] = {"project": str(view.path), "species_there": sid}
        if sid is None:
            row["note"] = f"no species from the catalog entry {catalog_id}" if catalog_id else "no species matched"
            out.append(row)
            continue
        found = await asyncio.to_thread(offered_members, view, sid)
        row["sources"] = [
            {
                "job": o.ref,
                "label": o.label,
                "kind": o.kind.value,
                "picks": o.n_rows,
                "tomograms": o.n_tomos,
                "extracted": o.extracted,
                "in_pool": member_key(view.path, o.kind, o.ref) in in_pool,
            }
            for o in found
        ]
        out.append(row)
    return ok(catalog_id=catalog_id, projects=out)


async def add_sources(home: Path, species_id: str, sources: list[dict[str, Any]]) -> dict[str, Any]:
    """Add each source (`project`, `job`: a template-match job or a pick list as `offers` names it, and
    optionally `species` there) to the pool, as Add sources does: the pool is made when it has none, the
    table built, the sources' own curation read in once as drops."""
    why = await _species_ok(home, species_id)
    if why:
        return err(why)
    home_view = await asyncio.to_thread(project_view, home)
    catalog_id = getattr(home_view.state.get_species(species_id), "catalog_id", None)
    members: list[PoolMember] = []
    problems = []
    for src in sources:
        path = Path(src["project"]).expanduser()
        try:
            view = await asyncio.to_thread(project_view, path)
        except (OSError, ValueError) as e:
            problems.append(f"{path}: {type(e).__name__}: {e}")
            continue
        sid = _species_there(view, home_view.path, species_id, catalog_id, src.get("species"))
        if sid is None:
            problems.append(
                f"{view.path.name}: no species "
                + (f"{src['species']}" if src.get("species") else f"from the catalog entry {catalog_id}")
            )
            continue
        found = {o.ref: o for o in await asyncio.to_thread(offered_members, view, sid)}
        o = found.get(src["job"])
        if o is None:
            listed = ", ".join(found) or "none"
            problems.append(f"{view.path.name}: {src['job']} is not a source of {sid} there (its sources: {listed})")
            continue
        members.append(
            PoolMember(
                key=member_key(view.path, o.kind, o.ref),
                project_path=str(view.path),
                project_label=view.path.name,
                species_id=sid,
                kind=o.kind,
                ref=o.ref,
                label=o.label,
            )
        )
    if problems:
        return err("Nothing added: " + "; ".join(problems), problems=problems)
    lp, why = await _open(home, species_id, create=True)
    if lp is None:
        return err(why)
    before = {m.key for m in lp.pool.members}
    await service.add_members(lp, members)
    added = [m.key for m in members if m.key not in before]
    failed = {k: s.error for k, s in lp.members.items() if k in added and s.state == "error"}
    return ok(
        added=added,
        already=[m.key for m in members if m.key in before],
        failed=failed,
        picks=int(lp.table.n) if lp.table is not None else 0,
        members=_members(lp),
    )


async def remove_source(home: Path, species_id: str, source: str) -> dict[str, Any]:
    """Take a member out of the pool (by its key, or `<project folder>:<job>`); its picks leave the table, the
    verdicts and subsets that named them stay."""
    lp, why = await _open(home, species_id)
    if lp is None:
        return err(why)
    hit = next((m for m in lp.pool.members if source in (m.key, f"{Path(m.project_path).name}:{m.ref}", m.label)), None)
    if hit is None:
        listed = ", ".join(f"{Path(m.project_path).name}:{m.ref}" for m in lp.pool.members) or "none"
        return err(f"No source {source} in this kitchen (its sources: {listed}).")
    await service.remove_member(lp, hit.key)
    return ok(removed=hit.key, picks=int(lp.table.n) if lp.table is not None else 0)


# ── Subsets and groups ───────────────────────────────────────────────────────────────────────────


async def create_subset(home: Path, species_id: str, name: str, draft: Draft, group: str | None) -> dict[str, Any]:
    """A new subset of the picks `draft` keeps now (a snapshot, as "+ kept" makes), in `group` when named:
    inside a group they leave its other subsets."""
    lp, why = await _open(home, species_id)
    if lp is None:
        return err(why)
    if lp.table is None:
        return err("The pool has no table yet: add a source with picks first.")
    if _subset_id(lp, name) is not None:
        return err(f"This kitchen has a subset {name} already.")
    gid = None
    if group:
        g = _group(lp, group)
        if g is None:
            listed = ", ".join(x.name for x in lp.pool.groups) or "none"
            return err(f"No group {group} in this kitchen (its groups: {listed}); create_kitchen_group makes one.")
        gid = g.id
    draft, why = _resolve_scope(lp, draft)
    if why or (why := check_draft(draft)):
        return err(why)
    kept, _sel = await _kept(lp, draft)
    rows = np.flatnonzero(kept) if kept is not None else []
    uids = [lp.table.uid_hex(int(r)) for r in rows]
    sub = await service.create_subset(lp, name, uids, gid)
    return ok(
        subset=sub.id,
        name=sub.name,
        picks=len(lp.subset_uids.get(sub.id, ())),
        recipe=service.describe_draft(draft.model_dump(), {x.id: x.name for x in lp.pool.subsets}),
        per_project=_counts(lp, kept, "project") if kept is not None else {},
    )


async def edit_subset(
    home: Path, species_id: str, subset: str, name: str | None, group: str | None, ungroup: bool
) -> dict[str, Any]:
    lp, why = await _open(home, species_id)
    if lp is None:
        return err(why)
    sid = _subset_id(lp, subset)
    if sid is None:
        return err(f"No subset {subset} in this kitchen.")
    gid: str | None = "-"
    if ungroup:
        gid = None
    elif group:
        g = _group(lp, group)
        if g is None:
            return err(f"No group {group} in this kitchen.")
        gid = g.id
    problem = await service.update_subset(lp, sid, name=name, group=gid)
    if problem:
        return err(problem)
    return ok(subset=sid, subsets=_subsets(lp))


async def delete_subset(home: Path, species_id: str, subset: str) -> dict[str, Any]:
    """The subset and its membership go; the picks stay in the pool, and sets made from it keep their copy."""
    lp, why = await _open(home, species_id)
    if lp is None:
        return err(why)
    sid = _subset_id(lp, subset)
    if sid is None:
        return err(f"No subset {subset} in this kitchen.")
    n = len(lp.subset_uids.get(sid, ()))
    await service.delete_subset(lp, sid)
    return ok(deleted=sid, picks=n)


async def create_group(home: Path, species_id: str, name: str) -> dict[str, Any]:
    lp, why = await _open(home, species_id)
    if lp is None:
        return err(why)
    if _group(lp, name) is not None:
        return err(f"This kitchen has a group {name} already.")
    g = await service.create_group(lp, name)
    return ok(group=g.id, name=g.name)


async def rename_group(home: Path, species_id: str, group: str, name: str) -> dict[str, Any]:
    lp, why = await _open(home, species_id)
    if lp is None:
        return err(why)
    g = _group(lp, group)
    if g is None:
        return err(f"No group {group} in this kitchen.")
    await service.rename_group(lp, g.id, name)
    return ok(group=g.id, name=g.name)


async def delete_group(home: Path, species_id: str, group: str) -> dict[str, Any]:
    """The group goes; its subsets stay, ungrouped."""
    lp, why = await _open(home, species_id)
    if lp is None:
        return err(why)
    g = _group(lp, group)
    if g is None:
        return err(f"No group {group} in this kitchen.")
    await service.delete_group(lp, g.id)
    return ok(deleted=g.id, subsets=_subsets(lp))


# ── Sets ─────────────────────────────────────────────────────────────────────────────────────────


def _plan_summary(plan: dict) -> dict[str, Any]:
    return {
        "name": plan["name"],
        "picks": plan["n"],
        "tomograms": plan["n_frames"],
        "projects": plan["n_projects"],
        "per_project": plan["per_project"],
        "within_half_diameter_removed": plan["n_before_dedup"] - plan["n"],
        "by_hand": {"kept": plan["kept_by_hand"], "dropped": plan["dropped_by_hand"]},
        "renamed_tomograms": len(plan.get("renamed") or []),
        "blockers": plan["blockers"],
        "unverified": plan["unverified"] + plan["notes"],
    }


async def _plans(
    home: Path, species_id: str, name: str | None, draft: Draft, subset: str | None, group: str | None
) -> tuple[service.LoadedPool | None, list[dict], str]:
    """The Make set plans: one from the recipe (narrowed to `subset`), or one per subset of `group`."""
    lp, why = await _open(home, species_id)
    if lp is None:
        return None, [], why
    if lp.table is None:
        return None, [], "The pool has no table yet: add a source with picks first."
    draft, why = _resolve_scope(lp, draft)
    if why or (why := check_draft(draft)):
        return None, [], why
    if group:
        g = _group(lp, group)
        if g is None:
            return None, [], f"No group {group} in this kitchen."
        subs = [x for x in lp.pool.subsets if x.group == g.id]
        if not subs:
            return None, [], f"The group {g.name} holds no subset."
        plans = []
        for x in subs:
            scope = Scope(include=[x.id], exclude=list(draft.scope.exclude))
            plans.append(await service.plan_set(lp, x.name, scope=scope, draft=draft))
        return lp, plans, ""
    scope = None
    if subset:
        sid = _subset_id(lp, subset)
        if sid is None:
            return None, [], f"No subset {subset} in this kitchen."
        scope = Scope(include=[sid], exclude=list(draft.scope.exclude))
    if not name:
        name = (lp.pool.subset(scope.include[0]).name if scope else "") or "pool set"
    return lp, [await service.plan_set(lp, name, scope=scope, draft=draft)], ""


async def plan_sets(
    home: Path, species_id: str, name: str | None, draft: Draft, subset: str | None, group: str | None
) -> dict[str, Any]:
    """What Make set would write, writing nothing: picks, tomograms and projects, picks per project, what was
    removed and why, what stops it."""
    _lp, plans, why = await _plans(home, species_id, name, draft, subset, group)
    if why:
        return err(why)
    return ok(sets=[_plan_summary(p) for p in plans])


async def make_sets(
    home: Path, species_id: str, name: str | None, draft: Draft, subset: str | None, group: str | None
) -> dict[str, Any]:
    """Make the planned set(s), each a particle set of the home project fixed as it is now. With `group`, a set
    of each subset that has picks and nothing stopping it; the others are named. Without, refused when its plan
    names a problem."""
    lp, plans, why = await _plans(home, species_id, name, draft, subset, group)
    if why:
        return err(why)
    ready = [p for p in plans if not p["blockers"] and p["n"]]
    left = [
        {"name": p["name"], "why": "; ".join(p["blockers"]) or "no pick passes"}
        for p in plans
        if p["blockers"] or not p["n"]
    ]
    if not ready:
        return err("No set made: " + "; ".join(f"{x['name']}: {x['why']}" for x in left), left_out=left)
    made = []
    for p in ready:
        try:
            ps = await service.make_set(lp, p)
        except (OSError, ValueError, RuntimeError) as e:
            logger.exception("Kitchen: the set %s could not be made", p["name"])
            return err(f"The set {p['name']} could not be made: {e}", made=made, left_out=left)
        made.append({"set": ps["id"], "name": ps.get("name", p["name"]), "picks": ps["n_particles"]})
    return ok(made=made, left_out=left)

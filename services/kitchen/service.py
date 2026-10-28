"""The kitchen's pools in this server: opened once per (home project, species), shared by every tab
and by the table, tile and slab routes.

A pool opens from its files (pool.json, verdicts.jsonl, the members' cached tables) and rebuilds only
the members whose sources moved, in workers. Everything here runs on the event loop and sends disk
work to threads and CPU work to workers; nothing reads a member's files on the loop.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from services.kitchen import store
from services.kitchen.members import member_spec, tomo_facts
from services.kitchen.models import Draft, KitchenPool, PoolMember, Verdict
from services.kitchen.pool import PoolTable, Selection, assemble, read_chunk, select, verdict_codes
from services.kitchen.snapshots import ProjectView, project_view
from services.kitchen.table import build_member
from services.workers import WorkKind, run_in_worker

logger = logging.getLogger(__name__)


@dataclass
class MemberStatus:
    state: str = "pending"  # pending | building | ok | error
    error: str = ""
    fingerprint: str = ""
    n: int = 0
    build_s: float | None = None


@dataclass
class LoadedPool:
    home: Path
    species_id: str
    pool: KitchenPool
    table: PoolTable | None = None
    payload: bytes | None = None
    verdicts: dict[str, str] = field(default_factory=dict)
    tags: dict[str, int] = field(default_factory=dict)
    members: dict[str, MemberStatus] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    building: bool = False
    rev: int = 0  # moves whenever the table, the pool or the verdicts change: views compare it
    tile_specs: dict[tuple, Any] = field(default_factory=dict)
    chunks: dict[str, tuple[dict, dict]] = field(default_factory=dict)  # member key -> (meta, arrays)
    averages: dict | None = None  # the last kept / removed means (`averages`)
    assembled_from: list[tuple[str, str]] = field(default_factory=list)  # (member, fingerprint) of the table

    def bump(self) -> None:
        self.rev += 1


_POOLS: dict[tuple[str, str], LoadedPool] = {}
_FACTS: dict[tuple[Path, tuple], dict[str, dict[str, Any]]] = {}


def loaded(home: Path, species_id: str) -> LoadedPool | None:
    return _POOLS.get((str(Path(home).resolve()), species_id))


async def open_pool(home: Path, species_id: str) -> LoadedPool:
    """The pool of `species_id` in `home`, read from its files the first time."""
    key = (str(Path(home).resolve()), species_id)
    lp = _POOLS.get(key)
    if lp is None:
        home_path = Path(key[0])
        is_new = not (store.kitchen_dir(home_path, species_id) / store.POOL_FILE).exists()
        pool = await asyncio.to_thread(store.load_pool, home_path, species_id)
        if is_new:
            pool.members = await asyncio.to_thread(_home_members, home_path, species_id)
        lp = _POOLS.setdefault(key, LoadedPool(home_path, species_id, pool))
        lp.verdicts, lp.tags, _n = await asyncio.to_thread(store.read_verdicts, lp.home, species_id)
        if is_new and pool.members:
            await save_pool(lp)
    return lp


def _home_members(home: Path, species_id: str) -> list[PoolMember]:
    """A new pool starts with what the home project itself offers for the species."""
    from services.kitchen.members import member_key, offered_members

    view = project_view(home)
    return [
        PoolMember(
            key=member_key(view.path, o.kind, o.ref),
            project_path=str(view.path),
            project_label=view.path.name,
            species_id=species_id,
            kind=o.kind,
            ref=o.ref,
            label=o.label,
        )
        for o in offered_members(view, species_id)
    ]


async def _view(path: str) -> ProjectView:
    return await asyncio.to_thread(project_view, Path(path))


async def _facts(view: ProjectView) -> dict[str, dict[str, Any]]:
    key = (view.path, view.stamp if not view.installed else (view.registry.index_path.stat().st_mtime_ns,))
    hit = _FACTS.get(key)
    if hit is None:
        hit = _FACTS[key] = await asyncio.to_thread(tomo_facts, view)
    return hit


def _cache_paths(lp: LoadedPool, member_key: str) -> Path:
    return store.table_dir(lp.home, lp.species_id) / f"{store.safe_name(member_key)}.npz"


async def _member_chunk(lp: LoadedPool, member: PoolMember) -> tuple[dict, dict] | None:
    status = lp.members.setdefault(member.key, MemberStatus())
    try:
        view = await _view(member.project_path)
        spec = await asyncio.to_thread(member_spec, view, member, await _facts(view))
    except (OSError, ValueError, KeyError) as e:
        logger.exception("Kitchen member %s could not be read", member.key)
        status.state, status.error = "error", f"{type(e).__name__}: {e}"
        return None
    held = lp.chunks.get(member.key)
    if held is not None and held[0].get("fingerprint") == spec["fingerprint"]:
        status.state, status.error = "ok", ""
        return held
    npz = _cache_paths(lp, member.key)
    meta = None
    if npz.with_suffix(".json").exists():
        try:
            meta, arrays = await asyncio.to_thread(read_chunk, npz)
        except (OSError, ValueError, KeyError) as e:
            logger.warning("Kitchen table cache %s unreadable, rebuilding: %s", npz, e)
            meta = None
    if meta is None or meta.get("fingerprint") != spec["fingerprint"]:
        status.state = "building"
        lp.bump()
        try:
            await run_in_worker(WorkKind.KITCHEN, build_member, spec, str(npz))
            meta, arrays = await asyncio.to_thread(read_chunk, npz)
        except Exception as e:
            # A member that cannot be built (a damaged star, a dead worker) leaves the others usable.
            logger.exception("Kitchen member %s could not be built", member.key)
            status.state, status.error = "error", f"{type(e).__name__}: {e}"
            return None
    status.state, status.error = "ok", ""
    status.fingerprint, status.n, status.build_s = meta["fingerprint"], int(meta["n"]), meta.get("build_s")
    lp.chunks[member.key] = (meta, arrays)
    return meta, arrays


async def rebuild(lp: LoadedPool) -> bool:
    """Read or build every member's chunk and assemble the table. True when its version moved."""
    async with lp.lock:
        lp.building = True
        try:
            t0 = time.time()
            members = list(lp.pool.members)
            results = await asyncio.gather(*(_member_chunk(lp, m) for m in members))
            chunks = [(m.key, r[0], r[1]) for m, r in zip(members, results, strict=True) if r is not None]
            for key in list(lp.members):
                if lp.pool.member(key) is None:
                    lp.members.pop(key)
                    lp.chunks.pop(key, None)
            fingerprints = [(k, meta["fingerprint"]) for k, meta, _a in chunks]
            if lp.table is not None and fingerprints == lp.assembled_from:
                return False
            lp.assembled_from = fingerprints
            labels = {m.key: (m.project_label, m.label or m.ref) for m in members}
            table = await asyncio.to_thread(assemble, lp.pool, chunks, labels=lambda k: labels[k])
            changed = lp.table is None or table.version != lp.table.version
            if changed:
                lp.table, lp.payload = table, None
                lp.bump()
            logger.debug("Kitchen %s/%s assembled in %.2f s", lp.home.name, lp.species_id, time.time() - t0)
            return changed
        finally:
            lp.building = False


async def save_pool(lp: LoadedPool) -> None:
    async with lp.write_lock:
        await asyncio.to_thread(store.save_pool, lp.home, lp.pool)
    lp.bump()


async def set_draft(lp: LoadedPool, draft: Draft) -> None:
    lp.pool.draft = draft
    await save_pool(lp)


async def add_members(lp: LoadedPool, members: list[PoolMember]) -> None:
    for m in members:
        if lp.pool.member(m.key) is None:
            lp.pool.members.append(m)
    await save_pool(lp)
    await rebuild(lp)
    await seed_verdicts(lp)


async def seed_verdicts(lp: LoadedPool) -> None:
    """D6: a member's existing curation is read once into the pool's verdicts, as drops with their
    source named; a particle that already carries a verdict keeps it."""
    from services.kitchen.seeds import seed_drops
    from services.particles.records import now_iso

    table = lp.table
    if table is None:
        return
    todo = [m for m in lp.pool.members if m.seeded_at is None and m.key in table.member_slices]
    for m in todo:
        try:
            found = await asyncio.to_thread(seed_drops, table, m.key)
        except (OSError, ValueError, KeyError) as e:
            logger.exception("Kitchen: the curation of %s could not be read", m.key)
            lp.members.setdefault(m.key, MemberStatus()).error = f"its curation could not be read: {e}"
            continue
        for path, uids in found.items():
            fresh = [u for u in uids if u not in lp.verdicts]
            if fresh:
                await record_verdicts(lp, fresh, Verdict.DROP, source=f"seed:{path}")
                logger.info("Kitchen %s: %d drops seeded from %s", m.key, len(fresh), path)
        m.seeded_at = now_iso()
    if todo:
        await save_pool(lp)


async def remove_member(lp: LoadedPool, key: str) -> None:
    lp.pool.members = [m for m in lp.pool.members if m.key != key]
    await save_pool(lp)
    await rebuild(lp)


async def record_verdicts(lp: LoadedPool, uids: list[str], verdict: Verdict, source: str = "hand") -> None:
    events = [store.verdict_event(u, verdict, source) for u in uids]
    for u in uids:
        if verdict == Verdict.CLEAR:
            lp.verdicts.pop(u, None)
        else:
            lp.verdicts[u] = verdict.value
    async with lp.write_lock:
        await asyncio.to_thread(store.append_events, lp.home, lp.species_id, events)
        await asyncio.to_thread(store.compact_verdicts, lp.home, lp.species_id)
    lp.bump()


async def record_tags(lp: LoadedPool, uids: list[str], tag: int, on: bool) -> None:
    bit = 1 << int(tag)
    for u in uids:
        bits = (lp.tags.get(u, 0) | bit) if on else (lp.tags.get(u, 0) & ~bit)
        if bits:
            lp.tags[u] = bits
        else:
            lp.tags.pop(u, None)
    async with lp.write_lock:
        events = [store.tag_event(u, tag, on) for u in uids]
        await asyncio.to_thread(store.append_events, lp.home, lp.species_id, events)
    lp.bump()


def current_selection(lp: LoadedPool) -> Selection | None:
    """The authoritative reading of the draft (cheap: vectorised over the table)."""
    if lp.table is None:
        return None
    return select(lp.table, lp.pool.draft, verdict_codes(lp.table, lp.verdicts), lp.pool.frame_choice)


async def selection(lp: LoadedPool) -> Selection | None:
    if lp.table is None:
        return None
    return await asyncio.to_thread(current_selection, lp)


async def payload(lp: LoadedPool) -> bytes | None:
    """The table's binary payload, encoded once per version."""
    from services.kitchen.payload import encode

    if lp.table is None:
        return None
    if lp.payload is None:
        table = lp.table
        data = await asyncio.to_thread(encode, table)
        if lp.table is table:
            lp.payload = data
        return data
    return lp.payload


def verdict_map(lp: LoadedPool) -> dict[str, int]:
    """{uid: 1 keep | 2 drop} for the rows of the table."""
    return {u: (1 if v == "keep" else 2) for u, v in lp.verdicts.items()}


def stale_members(lp: LoadedPool) -> list[str]:
    return [k for k, s in lp.members.items() if s.state == "error"]


def per_frame_rows(table: PoolTable, mask: np.ndarray) -> dict[int, int]:
    codes, n = np.unique(table.cat["frame"][mask], return_counts=True)
    return {int(c): int(k) for c, k in zip(codes, n, strict=True)}


# ── Pictures ─────────────────────────────────────────────────────────────────────────────────────


def box_a(lp: LoadedPool) -> float | None:
    """The cutout edge in Å: the pool's setting, else twice the home species' diameter; None when the
    species has no diameter (tiles then fall back to a fixed window, no common scale)."""
    if lp.pool.box_a:
        return float(lp.pool.box_a)
    sp = project_view(lp.home).state.get_species(lp.species_id)
    d = getattr(sp, "diameter_ang", None) if sp is not None else None
    return 2.0 * float(d) if d else None


def _frame_sample(table: PoolTable, code: int) -> list[tuple[float, float, float]]:
    from services.kitchen.tiles import NORM_SAMPLE

    rows = np.flatnonzero(table.cat["frame"] == code)
    rows = rows[np.isfinite(table.num["x_px"][rows])]
    if len(rows) > NORM_SAMPLE:
        rows = rows[np.linspace(0, len(rows) - 1, NORM_SAMPLE).astype(int)]
    return [(float(table.num["x_px"][i]), float(table.num["y_px"][i]), float(table.num["z_px"][i])) for i in rows]


async def tile_spec(lp: LoadedPool, code: int):
    """The frame's TileSpec (or why it has no tiles) and its normalisation sample, cached per table."""
    from services.kitchen.tiles import tile_spec as make_spec

    table = lp.table
    key = (table.version, code, lp.pool.box_a, lp.pool.slab_a)
    hit = lp.tile_specs.get(key)
    if hit is None:
        frame = table.frames[code]
        root = store.tiles_dir(lp.home, lp.species_id, frame["key"])
        box = await asyncio.to_thread(box_a, lp)
        spec = await asyncio.to_thread(make_spec, frame, root, box, lp.pool.slab_a)
        hit = lp.tile_specs[key] = (spec, _frame_sample(table, code))
    return hit


async def tile(lp: LoadedPool, uid: str) -> tuple[Path | None, str]:
    """The cutout of `uid`: its file, or why there is none. Cut on demand, batched per frame."""
    from services.kitchen.tiles import BATCHER, cached_tile

    table = lp.table
    row = table.row_of(uid) if table is not None else None
    if row is None:
        return None, "not a pick of this pool"
    code = int(table.cat["frame"][row])
    if code < 0 or not np.isfinite(table.num["x_px"][row]):
        return None, "the pick has no voxel position (its tomogram has no geometry)"
    spec, sample = await tile_spec(lp, code)
    if isinstance(spec, str):
        return None, spec
    path, reason = await asyncio.to_thread(cached_tile, spec, uid)
    if path is None and reason is None:
        xyz = (float(table.num["x_px"][row]), float(table.num["y_px"][row]), float(table.num["z_px"][row]))
        await asyncio.shield(BATCHER.add(spec, uid, xyz, sample))
        path, reason = await asyncio.to_thread(cached_tile, spec, uid)
    return path, reason or ""


async def prefetch_tiles(lp: LoadedPool, uids: list[str]) -> None:
    """Queue the cutouts of `uids` that are not cut yet, so a screen of them is cut in one call per frame."""
    from services.kitchen.tiles import BATCHER, cached_tile

    table = lp.table
    if table is None:
        return
    todo = []
    for uid in uids:
        row = table.row_of(uid)
        if row is None or table.cat["frame"][row] < 0 or not np.isfinite(table.num["x_px"][row]):
            continue
        spec, sample = await tile_spec(lp, int(table.cat["frame"][row]))
        if not isinstance(spec, str):
            todo.append((spec, sample, uid, row))

    def missing() -> list[int]:
        return [k for k, (spec, _s, uid, _r) in enumerate(todo) if cached_tile(spec, uid) == (None, None)]

    for k in await asyncio.to_thread(missing):
        spec, sample, uid, row = todo[k]
        xyz = (float(table.num["x_px"][row]), float(table.num["y_px"][row]), float(table.num["z_px"][row]))
        BATCHER.add(spec, uid, xyz, sample)


async def slab(lp: LoadedPool, code: int, which: str) -> tuple[Path | None, str]:
    """The X/Y or X/Z slab of frame `code`, rendered once for every view that asks."""
    from services import renders
    from services.kitchen.slabs import slab_paths

    table = lp.table
    if table is None or not 0 <= code < len(table.frames):
        return None, "no such frame"
    frame = table.frames[code]
    if not frame.get("recon_exists"):
        return None, "its reconstruction is not on disk"
    xy, xz, render = await asyncio.to_thread(slab_paths, lp.home, lp.species_id, frame)
    if render is not None:
        state = await renders.ensure(render)
        if state == renders.RUNNING:
            await renders.wait(render.key)
            state = await renders.ensure(render)
        if state == renders.FAILED:
            return None, renders.error(render.key) or "the slab render failed"
    path = xy if which == "xy" else xz
    return (path, "") if path.exists() else (None, "the slab was not written")


# ── Doctor, Make set, Ledger ─────────────────────────────────────────────────────────────────────


async def diagnose(lp: LoadedPool) -> list:
    from services.kitchen.doctor import diagnose as run

    sel = await selection(lp)
    return await asyncio.to_thread(run, lp, sel)


async def choose_frame(lp: LoadedPool, acq: str, frame_key: str | None) -> None:
    """D3: the frame an acquisition's picks are taken from (None: no choice, Make set waits)."""
    if frame_key:
        lp.pool.frame_choice[acq] = frame_key
    else:
        lp.pool.frame_choice.pop(acq, None)
    await save_pool(lp)


def frame_source(frame: dict) -> str:
    """Where a frame comes from, as a user picks between frames: its project and reconstruction job."""
    recon = Path(frame.get("recon") or "")
    job = next((p.name for p in recon.parents if p.name.startswith("job") and p.parent.name == "External"), "")
    return f"{frame.get('project', '')} · {job}" if job else str(frame.get("project", ""))


async def prefer_source(lp: LoadedPool, source: str) -> int:
    """For every conflicted acquisition with a frame from `source` (`frame_source`), choose that frame."""
    table = lp.table
    n = 0
    for acq, groups in (table.conflicts if table else {}).items():
        hit = next((g for g in groups if frame_source(table.frames[g[0]]) == source), None)
        if hit is not None:
            lp.pool.frame_choice[acq] = table.frames[hit[0]]["key"]
            n += 1
    await save_pool(lp)
    return n


async def plan_set(lp: LoadedPool, name: str, level: str | None) -> dict:
    from services.kitchen.export import plan

    sel = await selection(lp)
    sp = (await asyncio.to_thread(project_view, lp.home)).state.get_species(lp.species_id)
    diameter = getattr(sp, "diameter_ang", None) if sp is not None else None
    return await asyncio.to_thread(plan, lp, sel, name=name, level=level, diameter_a=diameter)


async def make_set(lp: LoadedPool, plan: dict) -> dict:
    """Write the planned set in a worker; returns the published ParticleSet as JSON."""
    from services.kitchen.export import write_set

    if plan["blockers"]:
        raise ValueError("; ".join(plan["blockers"]))
    out = await run_in_worker(WorkKind.KITCHEN, write_set, plan)
    lp.bump()
    return out


def describe_draft(draft: dict) -> str:
    """The recipe in one line: "σ ≥ 9 · −300 ≤ depth ≤ 250 Å · project: 4 of 8 · NN ≥ 300 Å"."""
    from services.kitchen.pool import CAT_COLUMNS, NUM_COLUMNS

    parts = []
    for f in draft.get("filters") or []:
        if f.get("kind") == "range":
            info = NUM_COLUMNS.get(f["col"])
            label, unit = (
                (info.label, f" {info.unit}" if info.unit and info.unit != "σ" else "") if info else (f["col"], "")
            )
            lo, hi = f.get("lo"), f.get("hi")
            if lo is not None and hi is not None:
                parts.append(f"{lo:g} ≤ {label} ≤ {hi:g}{unit}")
            elif lo is not None:
                parts.append(f"{label} ≥ {lo:g}{unit}")
            elif hi is not None:
                parts.append(f"{label} ≤ {hi:g}{unit}")
        elif f.get("kind") == "category":
            info = CAT_COLUMNS.get(f["col"])
            parts.append(f"{info.label if info else f['col']}: {len(f.get('allowed') or [])} chosen")
    post = draft.get("post") or {}
    if post.get("nn_a"):
        parts.append(f"no two closer than {post['nn_a']:g} Å")
    if post.get("top_n"):
        parts.append(f"at most {post['top_n']} per tomogram")
    return " · ".join(parts) or "every row"


def _headline(project: Path, state, iid: str) -> str:
    """A finished reader's result where its driver wrote a readout: class sizes, or the FSC resolution."""
    import json

    from services.models_base import JobStatus
    from services.particles.analytics_store import job_dir_of
    from services.particles.class_readout import CLASS3D_KIND, RECONSTRUCT_KIND, sidecar_paths

    jm = state.jobs.get(iid)
    status = getattr(getattr(jm, "execution_status", None), "value", "") if jm else ""
    if jm is None or jm.execution_status != JobStatus.SUCCEEDED:
        return status
    jd = job_dir_of(state, project, iid)
    for kind in (CLASS3D_KIND, RECONSTRUCT_KIND):
        path = sidecar_paths(jd, kind)[0] if jd else None
        if path is None or not path.exists():
            continue
        try:
            data = json.loads(path.read_text()).get("data") or {}
        except (OSError, ValueError):  # a readout being rewritten: the status alone says enough
            return status
        if kind == CLASS3D_KIND and data.get("classes"):
            sizes = sorted((c.get("n", 0) for c in data["classes"]), reverse=True)
            total = max(1, sum(sizes))
            return "classes " + " / ".join(f"{100 * n / total:.0f}%" for n in sizes)
        res = ((data.get("fsc") or {}).get("resolution") or {}).get("0.143")
        if res:
            return f"FSC 0.143: {res:.1f} Å"
    return status


def ledger(lp: LoadedPool) -> list[dict]:
    """The sets made from this pool, newest first, with their readers. Blocking: call in a thread."""
    from services.particles.particle_sets import SetOp, jobs_reading_set, list_sets

    state = project_view(lp.home).state
    out = []
    for ps in reversed(list_sets(lp.home)):
        if ps.recipe.op != SetOp.POOL or ps.species_id != lp.species_id:
            continue
        readers = [(iid, _headline(lp.home, state, iid)) for iid in jobs_reading_set(state, ps.id)]
        out.append(
            {
                "id": ps.id,
                "name": ps.name,
                "created_at": ps.created_at,
                "level": ps.level.value,
                "n": ps.n_particles,
                "n_tomograms": ps.n_tomograms,
                "per_project": ps.details.get("per_project") or {},
                "recipe": describe_draft(ps.recipe.params.get("draft") or {}),
                "readers": readers,
                "stale": [p.label for p in ps.stale_parents()],
                "uids": str(Path(ps.particles_star).with_name(ps.details.get("uids_file") or "kitchen_uids.txt")),
            }
        )
    return out


def set_uids(path: str) -> list[str]:
    try:
        return [u for u in Path(path).read_text().split() if u]
    except FileNotFoundError:
        return []


# ── Kept vs removed averages (S6) ────────────────────────────────────────────────────────────────

AVERAGE_SAMPLE = 200


async def averages(lp: LoadedPool) -> dict | None:
    """Unaligned means of up to AVERAGE_SAMPLE kept and AVERAGE_SAMPLE removed cutouts (a fixed random
    sample, so the same recipe gives the same pictures), each cut on demand. None without a table."""
    import hashlib

    from services.kitchen.tiles import mean_image

    sel = await selection(lp)
    if sel is None:
        return None
    table = lp.table
    rng = np.random.default_rng(0)
    usable = (table.cat["frame"] >= 0) & np.isfinite(table.num["x_px"])
    out: dict = {}
    for which, mask in (("kept", sel.final & usable), ("removed", ~sel.final & usable)):
        rows = np.flatnonzero(mask)
        if len(rows) > AVERAGE_SAMPLE:
            rows = np.sort(rng.choice(rows, AVERAGE_SAMPLE, replace=False))
        uids = [table.uid_hex(int(r)) for r in rows]
        if not uids:
            out[which] = {"n": 0, "file": ""}
            continue
        digest = hashlib.sha1(("|".join(uids) + f"|{lp.pool.box_a}|{lp.pool.slab_a}").encode()).hexdigest()[:12]
        target = store.kitchen_dir(lp.home, lp.species_id) / "averages" / f"{which}-{digest}.png"
        if not await asyncio.to_thread(target.exists):
            tiles = await asyncio.gather(*(tile(lp, u) for u in uids))
            paths = [str(p) for p, _r in tiles if p is not None]
            if not paths:
                out[which] = {"n": 0, "file": ""}
                continue
            n = await run_in_worker(WorkKind.KITCHEN_TILE, mean_image, paths, str(target))
        else:
            n = len(uids)
        out[which] = {"n": int(n), "of": int(mask.sum()), "file": str(target), "digest": digest}
    return out

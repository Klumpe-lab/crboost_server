"""The kitchen's pools in this server: opened once per (home project, species), shared by every tab
and by the table, tile and slab routes.

A pool opens from its files (pool.json, verdicts.jsonl, the members' cached tables) and rebuilds only
the members whose sources moved, in workers. Everything here runs on the event loop and sends disk
work to threads and CPU work to workers; nothing reads a member's files on the loop.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from services.kitchen import store
from services.kitchen.members import member_spec, tomo_facts
from services.kitchen.models import Draft, KitchenPool, MemberKind, PoolMember, SetFilter, Verdict
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
    verdict_budget: int = 0  # verdict events appended before the log is read again for compaction
    members: dict[str, MemberStatus] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    building: bool = False
    rev: int = 0  # moves whenever the table, the pool or the verdicts change: views compare it
    tile_specs: dict[tuple, Any] = field(default_factory=dict)
    chunks: dict[str, tuple[dict, dict]] = field(default_factory=dict)  # member key -> (meta, arrays)
    averages: dict | None = None  # the last kept / removed means (`averages`)
    # set id -> (what it was matched from, its rows in the table, what the match found): set filters (D9)
    set_masks: dict[str, tuple[tuple, np.ndarray, dict]] = field(default_factory=dict)
    # member key -> (what it was joined from, {row of its star: (extraction star, subtomogram, visible tilts)})
    pictures: dict[str, tuple[tuple, dict]] = field(default_factory=dict)
    review: dict | None = None  # the review sample and its labels (`store.REVIEW_FILE`)
    # member key -> (its star's mtime, rot, tilt, psi per row of the star): the loupe's template angles
    angles: dict[str, tuple] = field(default_factory=dict)
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
        lp.verdicts, n_lines = await asyncio.to_thread(store.read_verdicts, lp.home, species_id)
        lp.verdict_budget = store.compaction_budget(n_lines, len(lp.verdicts))
        lp.review = await asyncio.to_thread(store.read_review, lp.home, species_id)
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
        spec = await asyncio.to_thread(member_spec, view, member)
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
            spec["facts"] = await _facts(view)
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
            await _check_pictures(lp)
            diameter = await asyncio.to_thread(species_diameter, lp)
            radius = diameter / 2.0 if diameter else None
            fingerprints = [(k, meta["fingerprint"]) for k, meta, _a in chunks] + [("agree_radius", str(radius))]
            if lp.table is not None and fingerprints == lp.assembled_from:
                return False
            lp.assembled_from = fingerprints
            labels = {m.key: (m.project_label, m.label or m.ref) for m in members}
            table = await asyncio.to_thread(assemble, lp.pool, chunks, labels=lambda k: labels[k], agree_radius=radius)
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
        lp.verdict_budget -= len(events)
        if lp.verdict_budget <= 0:
            lp.verdict_budget = await asyncio.to_thread(store.compact_verdicts, lp.home, lp.species_id)
    lp.bump()


async def save_review(lp: LoadedPool, review: dict) -> None:
    """Keep the review sample and its labels (the browser sends them whole, a few hundred uids)."""
    lp.review = review
    async with lp.write_lock:
        await asyncio.to_thread(store.save_review, lp.home, lp.species_id, review)
    lp.bump()


def current_selection(lp: LoadedPool) -> Selection | None:
    """The authoritative reading of the draft (cheap: vectorised over the table)."""
    if lp.table is None:
        return None
    masks = {sid: hit[1] for sid, hit in lp.set_masks.items() if hit[0][0] == lp.table.version}
    return select(lp.table, lp.pool.draft, verdict_codes(lp.table, lp.verdicts), lp.pool.frame_choice, masks)


# ── Set filters (D9) ─────────────────────────────────────────────────────────────────────────────


async def ensure_set_masks(lp: LoadedPool) -> bool:
    """Match every set the draft names against the table (`setmatch.set_rows`), again only when the table
    or the set moved. True when a match changed. A set that cannot be read holds nothing, and says why."""
    from services.kitchen import setmatch

    table = lp.table
    if table is None:
        return False
    home = [m.key for m in lp.pool.members if Path(m.project_path).resolve() == lp.home]
    moved = False
    for sid in dict.fromkeys(f.set_id for f in lp.pool.draft.filters if isinstance(f, SetFilter)):
        stamp = (table.version, await asyncio.to_thread(setmatch.set_stamp, lp.home, sid))
        hit = lp.set_masks.get(sid)
        if hit is not None and hit[0] == stamp:
            continue
        try:
            mask, info = await asyncio.to_thread(setmatch.set_rows, table, lp.home, sid, home)
        except (OSError, ValueError, KeyError) as e:
            logger.exception("Kitchen %s/%s: set %s could not be matched", lp.home.name, lp.species_id, sid)
            mask, info = np.zeros(table.n, dtype=bool), {"n_set": 0, "n_matched": 0, "error": f"{e}"}
        lp.set_masks[sid] = (stamp, mask, info)
        moved = True
    if moved:
        lp.bump()
    return moved


def set_mask_payload(lp: LoadedPool) -> dict[str, dict]:
    """The matched sets for the browser: per set id its rows (`setmatch.mask_payload`), under the table
    version they number, and what the match found."""
    from services.kitchen.setmatch import mask_payload

    if lp.table is None:
        return {}
    return {
        sid: {"v": stamp[0], "bits": mask_payload(mask), **info}
        for sid, (stamp, mask, info) in lp.set_masks.items()
        if stamp[0] == lp.table.version
    }


async def selection(lp: LoadedPool) -> Selection | None:
    if lp.table is None:
        return None
    await ensure_set_masks(lp)
    return await asyncio.to_thread(current_selection, lp)


async def payload(lp: LoadedPool) -> bytes | None:
    """The table's binary payload, gzip-compressed once per version: the columns shrink to about a third,
    and the browser undoes it as it reads (the route sends ``Content-Encoding: gzip``)."""
    if lp.table is None:
        return None
    if lp.payload is None:
        table = lp.table
        data = await asyncio.to_thread(_encode_gzip, table)
        if lp.table is table:
            lp.payload = data
        return data
    return lp.payload


def _encode_gzip(table: PoolTable) -> bytes:
    import gzip

    from services.kitchen.payload import encode

    # Level 1: a quarter of level 6's time for 6 % more bytes on these float columns.
    return gzip.compress(encode(table), compresslevel=1)


def verdict_map(lp: LoadedPool) -> dict[str, int]:
    """{uid: 1 keep | 2 drop} for the rows of the table."""
    return {u: (1 if v == "keep" else 2) for u, v in lp.verdicts.items()}


def stale_members(lp: LoadedPool) -> list[str]:
    return [k for k, s in lp.members.items() if s.state == "error"]


def per_frame_rows(table: PoolTable, mask: np.ndarray) -> dict[int, int]:
    codes, n = np.unique(table.cat["frame"][mask], return_counts=True)
    return {int(c): int(k) for c, k in zip(codes, n, strict=True)}


# ── Pictures ─────────────────────────────────────────────────────────────────────────────────────


def species_diameter(lp: LoadedPool) -> float | None:
    """The home species' diameter in Å; None when it has none."""
    sp = project_view(lp.home).state.get_species(lp.species_id)
    d = getattr(sp, "diameter_ang", None) if sp is not None else None
    return float(d) if d else None


def box_a(lp: LoadedPool) -> float | None:
    """The cutout edge in Å: the pool's setting, else CUTOUT_BOX_FACTOR × the home species' diameter (the
    scale of an extraction's box, as the Journey's extracted cutouts show it); None when the species has no
    diameter (tiles then fall back to a fixed window, no common scale)."""
    from services.visualization.recon_cutouts import CUTOUT_BOX_FACTOR

    if lp.pool.box_a:
        return float(lp.pool.box_a)
    d = species_diameter(lp)
    return CUTOUT_BOX_FACTOR * d if d else None


def tile_settings(lp: LoadedPool) -> str:
    """What every tile of the pool is cut with, as a short hash: the browser keeps its cutouts under it."""
    from services.kitchen.tiles import TILE_PX, TILE_QUALITY, TILE_SCHEMA

    raw = f"{TILE_SCHEMA}|{box_a(lp)}|{lp.pool.slab_a}|{TILE_PX}|{TILE_QUALITY}"
    return hashlib.sha1(raw.encode()).hexdigest()[:8]


def _frame_sample(table: PoolTable, code: int) -> list[tuple[float, float, float]]:
    from services.kitchen.tiles import NORM_SAMPLE

    rows = np.flatnonzero(table.cat["frame"] == code)
    rows = rows[np.isfinite(table.num["x_px"][rows])]
    if len(rows) > NORM_SAMPLE:
        rows = rows[np.linspace(0, len(rows) - 1, NORM_SAMPLE).astype(int)]
    return [(float(table.num["x_px"][i]), float(table.num["y_px"][i]), float(table.num["z_px"][i])) for i in rows]


async def tile_specs(lp: LoadedPool, codes: set[int]) -> dict[int, tuple]:
    """{frame code: (its TileSpec or why it has no tiles, its normalisation sample)}, cached per table and
    tile settings; the frames not seen yet in one thread call (a stat per volume)."""
    from services.kitchen.tiles import tile_spec as make_spec

    table, settings = lp.table, tile_settings(lp)
    out = {
        c: lp.tile_specs[(table.version, c, settings)] for c in codes if (table.version, c, settings) in lp.tile_specs
    }
    todo = [c for c in codes if c not in out]
    if todo:
        box, slab_a = box_a(lp), lp.pool.slab_a

        def build() -> dict[int, tuple]:
            return {
                c: (
                    make_spec(
                        table.frames[c], store.tiles_dir(lp.home, lp.species_id, table.frames[c]["key"]), box, slab_a
                    ),
                    _frame_sample(table, c),
                )
                for c in todo
            }

        for c, hit in (await asyncio.to_thread(build)).items():
            out[c] = lp.tile_specs[(table.version, c, settings)] = hit
    return out


async def _member_pictures(lp: LoadedPool, key: str) -> dict:
    """A member's extracted subtomograms by row of its star (`pictures.member_pictures`), joined once and
    again only when an extraction serving it moved (`_check_pictures`, on the disk tick)."""
    from services.kitchen.pictures import member_pictures, pictures_stamp

    hit = lp.pictures.get(key)
    if hit is not None:
        return hit[1]
    member = lp.pool.member(key)
    if member is None or lp.table is None or key not in lp.table.member_meta:
        return {}
    view = await _view(member.project_path)
    stamp = (_fingerprint(lp, key), await asyncio.to_thread(pictures_stamp, view, member))
    pics = await asyncio.to_thread(member_pictures, view, member, lp.table.member_meta[key]["star"])
    lp.pictures[key] = (stamp, pics)
    return pics


def _fingerprint(lp: LoadedPool, key: str) -> str:
    status = lp.members.get(key)
    return status.fingerprint if status is not None else ""


async def _check_pictures(lp: LoadedPool) -> None:
    """Forget the pictures of members whose picks or extractions moved, so the next ask joins them again."""
    from services.kitchen.pictures import pictures_stamp

    for key, (stamp, _pics) in list(lp.pictures.items()):
        member = lp.pool.member(key)
        if member is None:
            lp.pictures.pop(key, None)
            continue
        now = (_fingerprint(lp, key), await asyncio.to_thread(pictures_stamp, await _view(member.project_path), member))
        if now != stamp:
            lp.pictures.pop(key, None)


def _row_members(table: PoolTable) -> np.ndarray:
    """Per row, its member's index in `table.members`."""
    out = np.empty(table.n, dtype=np.int32)
    for i, key in enumerate(table.members):
        out[table.member_slices[key]] = i
    return out


async def _ext_jobs(lp: LoadedPool, rows: list[tuple[str, int, int]], now: list) -> dict[str, tuple]:
    """The tile jobs of `rows` (uid, row, frame code) as extracted subtomograms: one directory per frame and
    extraction; a pick with none gets its reason in `now`."""
    from services.kitchen.pictures import LIST_NOT_JOINED, NOT_EXTRACTED
    from services.kitchen.tiles import ext_spec
    from services.particles.records import file_mtime

    table = lp.table
    owner = _row_members(table)
    pics = {}
    for key in dict.fromkeys(table.members[owner[row]] for _u, row, _c in rows):
        member = lp.pool.member(key)
        pics[key] = await _member_pictures(lp, key) if member is not None and member.kind == MemberKind.TM else None
    found = {}
    for uid, row, code in rows:
        key = table.members[owner[row]]
        if pics[key] is None:
            now.append((uid, LIST_NOT_JOINED))
            continue
        hit = pics[key].get(int(table.src_row[row]))
        if hit is None:
            now.append((uid, NOT_EXTRACTED))
            continue
        found[uid] = (code, hit)
    stars = {hit[0] for _c, hit in found.values()}
    mtimes = await asyncio.to_thread(lambda: {s: file_mtime(s) for s in stars})
    jobs: dict[str, tuple] = {}
    for uid, (code, (star, mrcs, visible)) in found.items():
        if mtimes[star] is None:
            now.append((uid, "its extraction's particles.star is gone"))
            continue
        spec = ext_spec(store.tiles_dir(lp.home, lp.species_id, table.frames[code]["key"]), star, mtimes[star])
        jobs.setdefault(spec.out_dir, (spec, [], []))[2].append((uid, (mrcs, visible)))
    return jobs


async def _tiles(lp: LoadedPool, uids: list[str], source: str = "recon"):
    """Groups of (uid, JPEG | why there is none) for `uids`: first every tile that needs no cutting, then
    each worker call's frames as they land, the frames of the first uids first. `source` "ext": each
    pick's extracted subtomogram instead of a cutout of its reconstruction."""
    from services.kitchen.tiles import BATCHER, STORE

    table = lp.table
    if table is None:
        return
    now: list[tuple[str, bytes | str]] = []
    rows: list[tuple[str, int, int]] = []
    for uid in dict.fromkeys(uids):
        row = table.row_of(uid)
        if row is None:
            now.append((uid, "not a pick of this pool"))
            continue
        code = int(table.cat["frame"][row])
        if code < 0 or not np.isfinite(table.num["x_px"][row]):
            now.append((uid, "the pick has no voxel position (its tomogram has no geometry)"))
            continue
        rows.append((uid, row, code))
    jobs: dict[str, tuple] = {}  # tile directory -> (spec, sample, [(uid, xyz)]), in the order asked for
    if source == "ext":
        jobs = await _ext_jobs(lp, rows, now) if rows else {}
        rows = []
    specs = await tile_specs(lp, {code for _u, _r, code in rows}) if rows else {}
    for uid, row, code in rows:
        spec, sample = specs[code]
        if isinstance(spec, str):
            now.append((uid, spec))
            continue
        xyz = (float(table.num["x_px"][row]), float(table.num["y_px"][row]), float(table.num["z_px"][row]))
        jobs.setdefault(spec.out_dir, (spec, sample, []))[2].append((uid, xyz))
    wanted = [(d, u) for d, (_spec, _sample, items) in jobs.items() for u, _xyz in items]
    found = await asyncio.to_thread(STORE.lookup, wanted) if wanted else {}
    now += found.items()
    if now:
        yield now
    batches = []
    for spec, sample, items in jobs.values():
        todo = [(u, xyz) for u, xyz in items if u not in found]
        if todo:
            batches += BATCHER.request(spec, todo, sample)
    missing = {u for _d, u in wanted if u not in found}
    try:
        for landed in asyncio.as_completed(list({id(b.done): b.done for b in batches}.values())):
            group = [(u, data) for u, data in (await landed).items() if u in missing]
            missing.difference_update(u for u, _data in group)
            if group:
                yield group
    finally:
        BATCHER.release(batches)


async def tile_records(lp: LoadedPool, uids: list[str], source: str = "recon"):
    """The body of a tiles response (`tiles.record`): one chunk per group of `_tiles`, so the browser paints
    each frame's cutouts as they land."""
    from services.kitchen.tiles import record

    async for group in _tiles(lp, uids, source):
        yield b"".join(record(uid, data) for uid, data in group)


async def slab(lp: LoadedPool, code: int, which: str) -> tuple[Path | None, str]:
    """The display copy of frame `code`'s X/Y or X/Z slab, rendered once for every view that asks."""
    from services import renders
    from services.kitchen.slabs import slab_views

    table = lp.table
    if table is None or not 0 <= code < len(table.frames):
        return None, "no such frame"
    frame = table.frames[code]
    if not frame.get("recon_exists"):
        return None, "its reconstruction is not on disk"
    xy, xz, steps = await asyncio.to_thread(slab_views, lp.home, lp.species_id, frame)
    for render in steps:
        state = await renders.ensure(render)
        if state == renders.RUNNING:
            await renders.wait(render.key)
            state = await renders.ensure(render)
        if state == renders.FAILED:
            return None, renders.error(render.key) or "the slab render failed"
    path = xy if which == "xy" else xz
    return (path, "") if await asyncio.to_thread(path.exists) else (None, "the slab was not written")


# ── The loupe (S6.7) ─────────────────────────────────────────────────────────────────────────────

ANGLE_COLS = ("rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi")


def _star_angles(star: str) -> tuple:
    """(mtime, rot, tilt, psi) of every row of a member's star; the arrays are None without angles."""
    from services.kitchen.table import _particle_block

    mtime = Path(star).stat().st_mtime_ns
    df = _particle_block(Path(star))
    if not all(c in df.columns for c in ANGLE_COLS):
        return (mtime, None, None, None)
    return (mtime, *(df[c].to_numpy(dtype=np.float64) for c in ANGLE_COLS))


async def _template_for(lp: LoadedPool, member: PoolMember | None, src_row: int) -> tuple[str | None, tuple | None]:
    """The template its template match matched with, and the pick's matched angles (rot, tilt, psi)."""
    from services.particles.analytics_store import tm_instance_for_ce

    if member is None or member.kind != MemberKind.TM or lp.table is None:
        return None, None
    view = await _view(member.project_path)
    tm = tm_instance_for_ce(view.state, member.ref)
    path = str(getattr(view.state.jobs.get(tm), "template_path", "") or "") if tm else ""
    template = None
    if path:
        p = Path(path) if Path(path).is_absolute() else view.path / path
        template = str(p) if await asyncio.to_thread(p.exists) else None
    star = lp.table.member_meta[member.key]["star"]
    hit = lp.angles.get(member.key)
    mtime = await asyncio.to_thread(lambda: Path(star).stat().st_mtime_ns)
    if hit is None or hit[0] != mtime:
        hit = lp.angles[member.key] = await asyncio.to_thread(_star_angles, star)
    if hit[1] is None or not 0 <= src_row < len(hit[1]):
        return template, None
    return template, (float(hit[1][src_row]), float(hit[2][src_row]), float(hit[3][src_row]))


async def loupe(lp: LoadedPool, uid: str) -> dict[str, Any]:
    """The loupe of one pick (`services/kitchen/loupe.py`), rendered once in a worker and kept in the kitchen's
    ``loupe/``: {"name": its PNG, "template", "why", "angles", "template_name", "box_a", "slab_a"}, or
    {"error": why there is none}."""
    import json

    from services.kitchen.loupe import LOUPE_PX, render_loupe

    table = lp.table
    row = table.row_of(uid) if table is not None else None
    if row is None:
        return {"error": "not a pick of this pool"}
    code = int(table.cat["frame"][row])
    if code < 0 or not np.isfinite(table.num["x_px"][row]):
        return {"error": "the pick has no voxel position (its tomogram has no geometry)"}
    spec, _sample = (await tile_specs(lp, {code}))[code]
    if isinstance(spec, str):
        return {"error": spec}
    frame = table.frames[code]
    member = lp.pool.member(table.members[int(_row_members(table)[row])])
    template, angles = await _template_for(lp, member, int(table.src_row[row]))
    xyz = tuple(float(table.num[c][row]) for c in ("x_px", "y_px", "z_px"))
    tag = hashlib.sha1(f"{spec.out_dir}|{template}|{angles}|{LOUPE_PX}".encode()).hexdigest()[:8]
    out = store.kitchen_dir(lp.home, lp.species_id) / "loupe" / f"{uid}-{tag}.png"
    meta = out.with_suffix(".json")
    info = {
        "name": out.name,
        "angles": angles,
        "template_name": Path(template).name if template else None,
        "box_a": round(2 * spec.half_box * frame["apix"]),
        "slab_a": round((2 * spec.half_slab + 1) * frame["apix"]),
    }
    try:
        done = json.loads(await asyncio.to_thread(meta.read_text))
    except (OSError, ValueError):  # not rendered yet
        done = await run_in_worker(
            WorkKind.KITCHEN_TILE,
            render_loupe,
            spec.recon,
            xyz,
            spec.half_box,
            spec.half_slab,
            float(frame["apix"]),
            template,
            angles,
            str(out),
        )
        await asyncio.to_thread(meta.write_text, json.dumps(done))
    return {**info, **done}


# ── Make set ─────────────────────────────────────────────────────────────────────────────────────


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


def frame_conflicts(lp: LoadedPool) -> list[dict]:
    """Acquisitions seen in frames that cannot share picks (D3, D12), each with its frames: where each
    comes from, its rows, and whether it is the one chosen. Blocking: call in a thread."""
    table = lp.table
    out = []
    for acq, groups in (table.conflicts if table else {}).items():
        chosen = lp.pool.frame_choice.get(acq)
        items = [
            {
                "acq": acq,
                "frame": table.frames[g[0]]["key"],
                "source": frame_source(table.frames[g[0]]),
                "rows": int(np.isin(table.cat["frame"], g).sum()),
                "chosen": chosen == table.frames[g[0]]["key"],
            }
            for g in groups
        ]
        out.append({"acq": acq, "tomo": table.frames[groups[0][0]]["tomo"], "chosen": bool(chosen), "items": items})
    return out


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
        elif f.get("kind") == "tomograms" and f.get("excluded"):
            parts.append(f"without {len(f['excluded'])} tomogram(s)")
        elif f.get("kind") == "set":
            parts.append(f"{'in' if f.get('mode', 'in') == 'in' else 'not in'} set {f.get('set_id')}")
    post = draft.get("post") or {}
    if post.get("nn_a"):
        parts.append(f"no two closer than {post['nn_a']:g} Å")
    if post.get("top_n"):
        parts.append(f"at most {post['top_n']} per tomogram")
    return " · ".join(parts) or "every row"


# ── Kept vs removed means (S6) ───────────────────────────────────────────────────────────────────

AVERAGE_SAMPLE = 200


async def averages(lp: LoadedPool) -> dict | None:
    """Unaligned means of up to AVERAGE_SAMPLE kept and AVERAGE_SAMPLE removed cutouts (a fixed random
    sample, so the same recipe gives the same pictures), each cut on demand. Made only when the user asks:
    a cold pair cuts up to 2 × AVERAGE_SAMPLE cutouts. None without a table."""
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
        digest = hashlib.sha1(("|".join(uids) + f"|{tile_settings(lp)}").encode()).hexdigest()[:12]
        target = store.kitchen_dir(lp.home, lp.species_id) / "averages" / f"{which}-{digest}.png"
        if not await asyncio.to_thread(target.exists):
            blobs = [data async for group in _tiles(lp, uids) for _u, data in group if isinstance(data, bytes)]
            if not blobs:
                out[which] = {"n": 0, "file": ""}
                continue
            n = await run_in_worker(WorkKind.KITCHEN_TILE, mean_image, blobs, str(target))
        else:
            n = len(uids)
        out[which] = {"n": int(n), "of": int(mask.sum()), "file": str(target), "digest": digest}
    return out

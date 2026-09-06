"""Server-side curation watcher (roadmap 09-S4, hardened by 10-S2).

Detects ArtiaX ``.coords`` saves under ``<project>/Curation/<species>/<tomo>/`` and
registers each as a pick list — with no Journey tab open. Replaces the Journey's
render-path prescan (``_auto_kick_coords_ingest`` + the ``.coords`` mtime terms in its
refresh gates), which only ran while the Particles section of the *selected* tilt series
was being rendered.

Shape = the ``PipelineMonitor`` skeleton (start / stop / _loop / _tick_once). Per open
project, every tick scans the HOT dirs and every ``FULL_SWEEP_EVERY``-th tick the whole
``Curation/*/*/`` tree. Scanning, attribution and the geometry lookup run off the event
loop; ingest (``backend.import_curation_picks`` → ``register_manual_pick_list`` → save by
explicit path — no client context here) runs sequentially on it, one save at a time.

EVERY save, not the newest (10-S2). It used to ingest only the newest ``.coords`` per dir
and collapse every one of them onto a single ``manual`` slug, so of N lists a user saved in
a session, N−1 silently disappeared. Now each file is its own pick list, keyed by its stem
(``services.particles.ingest.manual_slug_for``) — which also makes re-saving under the same
name an update of that list rather than a new one.

ATTRIBUTION comes from the dir's ``manifest.json`` — written when crboost declared the
session's scope — and falls back to matching the registered species ids / known tomogram
names against the ``<species>/<tomo>`` directory slugs only when there is no manifest (a
pre-10 project). A dir that resolves to nothing, or to two things (slug collision), or
whose manifest names a species the registry no longer has, is reported through
``unattributed()`` and never guessed at; the Picks & curation tab turns each of those into
an explicit "assign to species + tomogram" action, which is the staging mechanism of
record. Dedup: ``_seen`` (project, coords file, int(mtime)) plus the list's
``created_at >= mtime`` guard, which makes a restart re-register nothing.

The HOT set is derived from the manifests too (their ``launched_at``), not from an
in-memory record of what a session was told to load: that record was empty after a crboost
restart while ArtiaX kept picking, which is fragility #2 of the roadmap's §1.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from services.particles.ingest import manual_slug_for, register_manual_pick_list
from services.particles.list_ref import tomograms_star_for
from services.visualization import artiax_bridge
from services.visualization.tomo_geometry import read_tomo_table, tomogram_star_sources

if TYPE_CHECKING:
    from backend import CryoBoostBackend

logger = logging.getLogger(__name__)

TICK_SEC = 5.0
FULL_SWEEP_EVERY = 6  # hot dirs every tick; the whole Curation/*/*/ tree every ~30 s
SETTLE_SEC = 2.0  # ArtiaX writes are not atomic — leave a just-written file for the next tick
EVENTS_PER_PROJECT = 200


@dataclass(frozen=True, slots=True)
class _Save:
    """One candidate ``.coords`` save from the off-loop scan, attributed (or not)."""

    dir: Path
    coords: Path
    mtime: float
    species_id: str | None
    species_name: str
    tomo_name: str | None
    tomograms_star: Path | None
    reason: str  # "" when attributable with geometry; else why the save cannot be ingested


def _key(project_path: Path) -> str:
    return str(Path(project_path).resolve())


class CurationWatcher:
    def __init__(self, backend: CryoBoostBackend):
        self._backend = backend
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        self._tick_n = 0
        self._seen: set[tuple[str, str, int]] = set()  # (project, coords, int(mtime)) handled: ingested or failed
        self._events: dict[str, deque] = defaultdict(lambda: deque(maxlen=EVENTS_PER_PROJECT))
        self._unattributed: dict[str, dict[str, dict]] = defaultdict(dict)  # project → {dir: {reason, files}}
        self._reported: set[tuple[str, str, str]] = set()  # (project, dir, reason) already logged once
        self._hot: dict[str, list[Path]] = {}  # project → most recently launched curation dir(s)
        # project → when the hot dir became the session's scope (epoch s). A user save in
        # ANOTHER dir that is newer than this is an off-scope save (2026-09-06: ChimeraX's save
        # dialog opens in the folder last saved to, so after a switch the previous scope's
        # seed is what the dialog lists — and gets overwritten).
        self._hot_at: dict[str, float] = {}
        # One project tick at a time: `sweep_now` (the Picks tab's Refresh) and the loop must
        # not ingest the same save twice — `_seen` is only written after an ingest completes.
        self._tick_lock = asyncio.Lock()
        self._with_saves: dict[str, set[str]] = {}  # project → dirs that held a user .coords at the last full sweep

    # ── lifecycle (PipelineMonitor shape) ─────────────────────────────────────────

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="curation-watcher")
        logger.info("CurationWatcher started (tick=%.0fs, full sweep every %d ticks)", TICK_SEC, FULL_SWEEP_EVERY)

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("CurationWatcher stop: task raised")
        self._task = None
        logger.info("CurationWatcher stopped")

    # ── read API (the Picks & curation tab's watcher footer, roadmap 11-S4 / 09-S1) ────

    def events(self, project_path: Path) -> list[dict]:
        """Newest-last ingest / error / unattributed events for one project (in-memory,
        capped at EVENTS_PER_PROJECT)."""
        return list(self._events.get(_key(project_path), ()))

    def unattributed(self, project_path: Path) -> list[dict]:
        """Curation dirs holding a user save that maps to no (registered species, known
        tomogram) — surfaced, never guessed. ``{"dir", "reason", "files"}`` per dir, sorted
        by dir. The reason is what the Picks & curation tab shows so the user can fix the
        name rather than wonder why a save did nothing, and ``files`` is what its "assign"
        action would move (10-S2)."""
        found = self._unattributed.get(_key(project_path), {})
        return [{"dir": d, "reason": found[d]["reason"], "files": list(found[d]["files"])} for d in sorted(found)]

    # ── write API: scope changes + the Picks tab's Refresh (2026-09-06) ─────────────

    def mark_hot(self, project_path: Path, curation_dir: Path) -> None:
        """Make ``curation_dir`` the hot dir NOW — called by the backend when a session is
        launched on it or switched to it. Until now the hot set moved only on the full sweep
        (every ``FULL_SWEEP_EVERY`` ticks), so the first save after a switch waited up to
        ~30 s to be seen; a hot dir is rescanned every tick (~7 s to a row update). The full
        sweep keeps re-deriving the same answer from the manifests, which is what survives a
        restart."""
        key = _key(project_path)
        self._hot[key] = [Path(curation_dir)]
        self._hot_at[key] = time.time()

    async def sweep_now(self, project_path: Path) -> None:
        """One immediate FULL sweep of a project — the Picks tab's Refresh button. Serialised
        with the loop's tick; a save younger than ``SETTLE_SEC`` still waits for the next
        pass (ArtiaX writes are not atomic)."""
        from services.project_state import get_project_state_for

        async with self._tick_lock:
            await self._tick_project(Path(project_path), get_project_state_for(Path(project_path)), True)

    # ── tick ──────────────────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._tick_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("CurationWatcher tick failed")
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=TICK_SEC)
            except TimeoutError:
                continue

    async def _tick_once(self) -> None:
        from services.project_state import _project_states

        self._tick_n += 1
        full = self._tick_n % FULL_SWEEP_EVERY == 1  # first tick sweeps everything (restart catch-up)
        for project_path, state in list(_project_states.items()):
            try:
                async with self._tick_lock:
                    await self._tick_project(Path(project_path), state, full)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("CurationWatcher: tick failed for %s", project_path)

    async def _tick_project(self, project_path: Path, state, full: bool) -> None:
        if not (project_path / "Curation").is_dir():
            return
        key = _key(project_path)
        hot = self._hot.get(key, [])
        if not hot and not full:
            return
        saves = await asyncio.to_thread(self._collect, key, project_path, state, hot, full)
        if full and self._unattributed.get(key):
            # Drop rows for dirs that no longer exist OR no longer hold a user save — an
            # `assign` moves the files out, and a row that lingers after that reads as an
            # unresolved problem the user already fixed.
            still = self._with_saves.get(key, set())
            for d in [d for d in self._unattributed[key] if d not in still]:
                self._unattributed[key].pop(d, None)
        for s in saves:
            dedup = (key, str(s.coords), int(s.mtime))
            if s.species_id is None or s.tomo_name is None:
                entry = self._unattributed[key].setdefault(str(s.dir), {"reason": s.reason, "files": []})
                entry["reason"] = s.reason
                if str(s.coords) not in entry["files"]:
                    entry["files"].append(str(s.coords))
                self._note(key, "unattributed", s, message=s.reason)
                continue
            self._unattributed[key].pop(str(s.dir), None)
            slug = manual_slug_for(s.coords)
            pl = state.get_pick_list(slug, s.species_id, s.tomo_name)
            if pl is not None and pl.created_at is not None and pl.created_at.timestamp() >= s.mtime:
                self._seen.add(dedup)  # already registered (this or a newer save) — the restart-safe guard
                continue
            # Off-scope save: newer than the moment the hot dir became the session's scope,
            # but not IN it. The mechanism still files it where it landed (that folder's
            # manifest says whose it is) — this is the never-silent half: the WATCHER footer
            # names it in red so a save into the previous scope's seed is seen, not swallowed.
            hot_at = self._hot_at.get(key)
            if hot and hot_at is not None and s.dir not in hot and s.mtime > hot_at:
                h = hot[0]
                self._note(
                    key,
                    "off-scope",
                    s,
                    message=f"saved while the session's scope was {h.parent.name}/{h.name} — ArtiaX's save "
                    "dialog opens in the folder last saved to; if this was meant for the scoped list, its previous "
                    "content is in imports/",
                )
            if s.tomograms_star is None:
                self._note(key, "no-geometry", s, message=s.reason)  # not `_seen`: geometry may appear later
                continue
            res = await self._backend.import_curation_picks(
                project_path, s.tomograms_star, s.tomo_name, s.species_name, s.species_id, coords_path=s.coords
            )
            self._seen.add(dedup)  # a failed import is not retried until the file changes (new mtime → new key)
            if not res.get("success"):
                self._note(key, "error", s, message=str(res.get("error")))
                continue
            register_manual_pick_list(state, res, s.species_id, s.tomo_name)
            await self._backend.save_project(project_path, force=True)
            self._note(key, "ingested", s, count=int(res.get("count", 0)))

    # ── off-loop scan + attribution ───────────────────────────────────────────────

    def _collect(self, key: str, project_path: Path, state, hot: list[Path], full: bool) -> list[_Save]:
        """Thread: EVERY settled user save in each scanned dir, attributed to (species,
        tomo) by manifest (else by slug match), with the tomograms.star the import needs.
        Saves already handled (``_seen``) are dropped here so a stale save costs one glob
        per tick, no more. A full sweep also refreshes the hot set from the manifests and
        records which dirs still hold a user save, which is how a resolved unattributed row
        stops being shown."""
        dirs: set[Path] = set(hot)
        if full:
            root = project_path / "Curation"
            try:
                for sp_dir in root.iterdir():
                    if sp_dir.is_dir():
                        dirs.update(d for d in sp_dir.iterdir() if d.is_dir())
            except OSError:
                logger.exception("CurationWatcher: cannot list %s", root)
        now = time.time()
        found: list[tuple[Path, Path, float]] = []
        manifests: dict[Path, dict] = {}
        launched: list[tuple[str, Path]] = []
        with_saves: set[str] = set()
        for d in sorted(dirs):
            m = artiax_bridge.read_manifest(d)
            if m is not None:
                manifests[d] = m
                if m.get("launched_at"):
                    launched.append((str(m["launched_at"]), d))
            for c in artiax_bridge.user_coords_saves(d):
                with_saves.add(str(d))
                try:
                    mtime = c.stat().st_mtime
                except OSError:
                    continue  # vanished between glob and stat — the next tick sees whatever is there
                if now - mtime < SETTLE_SEC or (key, str(c), int(mtime)) in self._seen:
                    continue
                found.append((d, c, mtime))
        if full:
            self._with_saves[key] = with_saves
            # Hot = the dir of the most recently LAUNCHED session — where the user is
            # picking, or last was. ISO-8601 timestamps sort lexicographically. It stays hot
            # after that session ends, which costs one glob per tick and is the honest
            # trade for surviving a crboost restart. Empty until some dir has been launched
            # on; a hot dir is rescanned every tick instead of every sixth, which is the
            # whole latency difference the save contract quotes.
            self._hot[key] = [max(launched)[1]] if launched else []
            if launched:
                try:
                    self._hot_at[key] = datetime.fromisoformat(max(launched)[0]).timestamp()
                except ValueError:
                    self._hot_at.pop(key, None)  # a hand-edited manifest; no off-scope check rather than a wrong one
        if not found:
            return []

        species_by_id = {sp.id: sp for sp in list(state.species_registry)}
        species_by_slug = _by_slug([(sp.id, sp) for sp in species_by_id.values()])
        tomo_names: set[str] = set()
        for _source, star in tomogram_star_sources(state, project_path):
            df = read_tomo_table(star)
            if df is not None:
                tomo_names.update(str(x) for x in df["rlnTomoName"].tolist())
        tomo_by_slug = _by_slug([(name, name) for name in tomo_names])

        out: list[_Save] = []
        for d, coords, mtime in found:
            sp, tomo, reasons = self._attribute(d, manifests.get(d), species_by_id, species_by_slug, tomo_by_slug)
            if reasons:
                out.append(_Save(d, coords, mtime, None, "", None, None, "; ".join(reasons)))
                continue
            star = tomograms_star_for(state, project_path, sp.id, tomo)
            reason = ""
            if star is None:
                reason = f"no tomograms.star carries {tomo} (nothing reconstructed or imported yet)"
            out.append(_Save(d, coords, mtime, sp.id, sp.name, tomo, star, reason))
        return out

    @staticmethod
    def _attribute(d: Path, manifest: dict | None, species_by_id: dict, species_by_slug: dict, tomo_by_slug: dict):
        """``(species, tomo_name, reasons)`` for one curation dir. The manifest crboost
        wrote when it declared the session's scope is authoritative — no name reversal, so
        a slug collision cannot misfile anything. Without one (a pre-10 dir, or one the
        user made by hand) the directory names are matched against the registered species
        ids / known tomogram names, exactly as before. A manifest naming a species the
        registry no longer holds is an error to report, NOT a reason to fall back to
        guessing at the directory name."""
        if manifest and manifest.get("species_id") and manifest.get("tomo_name"):
            sp = species_by_id.get(str(manifest["species_id"]))
            if sp is None:
                return None, None, [f"manifest names species '{manifest['species_id']}', which is not registered"]
            return sp, str(manifest["tomo_name"]), []
        sp = species_by_slug.get(d.parent.name)
        tomo = tomo_by_slug.get(d.name)
        reasons = []
        if sp is None:
            reasons.append(f"no registered species slugs to '{d.parent.name}'")
        if tomo is None:
            reasons.append(f"no known tomogram slugs to '{d.name}'")
        return sp, tomo, reasons

    # ── bookkeeping ───────────────────────────────────────────────────────────────

    def _note(self, key: str, kind: str, s: _Save, *, message: str = "", count: int | None = None) -> None:
        """Append an event + log line. Unattributed / no-geometry are recorded ONCE per
        (dir, reason) — a lingering dir must not flood the events or the log every sweep;
        ingests and errors are recorded every time."""
        if kind in ("unattributed", "no-geometry"):
            mark = (key, str(s.dir), message)
            if mark in self._reported:
                return
            self._reported.add(mark)
        self._events[key].append(
            {
                "ts": datetime.now().isoformat(timespec="seconds"),
                "kind": kind,
                "species_id": s.species_id or "",
                "tomo_name": s.tomo_name or "",
                "coords": str(s.coords),
                "count": count,
                "message": message,
            }
        )
        if kind in ("unattributed", "no-geometry"):
            logger.warning("CurationWatcher[%s]: %s — %s (%s)", Path(key).name, kind, message, s.coords)
        elif kind == "error":
            logger.warning(
                "CurationWatcher[%s]: import failed for %s/%s: %s", Path(key).name, s.species_id, s.tomo_name, message
            )
        else:
            logger.info(
                "CurationWatcher[%s]: ingested %s picks for %s/%s from %s",
                Path(key).name,
                count,
                s.species_id,
                s.tomo_name,
                s.coords.name,
            )


def _by_slug(items: list[tuple[str, object]]) -> dict[str, object]:
    """``{slug: value}`` over ``(name, value)`` pairs; a slug two names share maps to
    NOTHING (dropped) so a colliding dir is reported unattributed, never guessed."""
    out: dict[str, object] = {}
    clashes: set[str] = set()
    for name, value in items:
        slug = artiax_bridge._safe_slug(name)
        if slug in out or slug in clashes:
            clashes.add(slug)
            out.pop(slug, None)
            continue
        out[slug] = value
    return out

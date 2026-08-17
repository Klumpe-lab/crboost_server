"""Server-side curation watcher (roadmap 09-S4).

Detects ArtiaX ``.coords`` saves under ``<project>/Curation/<species>/<tomo>/`` and
registers each as that tomogram's ``manual`` pick list — with no Journey tab open.
Replaces the Journey's render-path prescan (``_auto_kick_coords_ingest`` + the
``.coords`` mtime terms in its refresh gates), which only ran while the Particles
section of the *selected* tilt series was being rendered.

Shape = the ``PipelineMonitor`` skeleton (start / stop / _loop / _tick_once). Per open
project, every tick scans the HOT dirs (what a live session has loaded,
``CurationSessionService.loaded_curation_dirs``) and every ``FULL_SWEEP_EVERY``-th tick
the whole ``Curation/*/*/`` tree. Scanning, attribution and the geometry lookup run off
the event loop; ingest (``backend.import_curation_picks`` → ``register_manual_pick_list``
→ save by explicit path — no client context here) runs sequentially on it, one save at a
time, so two saves never race on the same ``manual`` slug.

Attribution never reverses a directory name: the slugs of the registered species ids and
of the known tomogram names are matched against ``<species>/<tomo>`` (the same
``_safe_slug`` ``curation_dir`` used to create them); a dir matching nothing — or two
things (slug collision) — is reported through ``unattributed()`` and never guessed.
Dedup: ``_seen`` (project, dir, int(mtime)) plus the ``manual`` list's
``created_at >= mtime`` guard, which makes a restart re-register nothing.
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

from services.particles.ingest import register_manual_pick_list
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
MANUAL_SLUG = "manual"


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
        self._seen: set[tuple[str, str, int]] = set()  # (project, dir, int(mtime)) handled: ingested or failed
        self._events: dict[str, deque] = defaultdict(lambda: deque(maxlen=EVENTS_PER_PROJECT))
        self._unattributed: dict[str, dict[str, str]] = defaultdict(dict)  # project → {dir: reason}
        self._reported: set[tuple[str, str, str]] = set()  # (project, dir, reason) already logged once

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

    # ── read API (Curation tab, roadmap 11-S4; peeves triage) ─────────────────────

    def events(self, project_path: Path) -> list[dict]:
        """Newest-last ingest / error / unattributed events for one project (in-memory,
        capped at EVENTS_PER_PROJECT)."""
        return list(self._events.get(_key(project_path), ()))

    def unattributed(self, project_path: Path) -> list[Path]:
        """Curation dirs holding a user save that maps to no (registered species, known
        tomogram) — surfaced, never guessed."""
        return [Path(d) for d in sorted(self._unattributed.get(_key(project_path), {}))]

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
                await self._tick_project(Path(project_path), state, full)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("CurationWatcher: tick failed for %s", project_path)

    async def _tick_project(self, project_path: Path, state, full: bool) -> None:
        if not (project_path / "Curation").is_dir():
            return
        hot = self._backend.curation_service.loaded_curation_dirs(project_path)
        if not hot and not full:
            return
        key = _key(project_path)
        saves = await asyncio.to_thread(self._collect, key, project_path, state, hot, full)
        if full and self._unattributed.get(key):
            gone = [d for d in self._unattributed[key] if not Path(d).is_dir()]
            for d in gone:
                self._unattributed[key].pop(d, None)
        for s in saves:
            if s.species_id is None or s.tomo_name is None:
                self._unattributed[key][str(s.dir)] = s.reason
                self._note(key, "unattributed", s, message=s.reason)
                continue
            self._unattributed[key].pop(str(s.dir), None)
            dedup = (key, str(s.dir), int(s.mtime))
            pl = state.get_pick_list(MANUAL_SLUG, s.species_id, s.tomo_name)
            if pl is not None and pl.created_at is not None and pl.created_at.timestamp() >= s.mtime:
                self._seen.add(dedup)  # already registered (this or a newer save) — the restart-safe guard
                continue
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
        """Thread: newest settled user save per dir, attributed to (species, tomo) by
        slug match, with the tomograms.star the import needs. Saves already handled
        (``_seen``) are dropped here so a stale save costs one glob per tick, no more."""
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
        for d in sorted(dirs):
            saves = artiax_bridge.user_coords_saves(d)
            if not saves:
                continue
            try:
                mtime = saves[0].stat().st_mtime
            except OSError:
                continue  # vanished between glob and stat — the next tick sees whatever is there
            if now - mtime < SETTLE_SEC or (key, str(d), int(mtime)) in self._seen:
                continue
            found.append((d, saves[0], mtime))
        if not found:
            return []

        species_by_slug = _by_slug([(sp.id, sp) for sp in list(state.species_registry)])
        tomo_names: set[str] = set()
        for _source, star in tomogram_star_sources(state, project_path):
            df = read_tomo_table(star)
            if df is not None:
                tomo_names.update(str(x) for x in df["rlnTomoName"].tolist())
        tomo_by_slug = _by_slug([(name, name) for name in tomo_names])

        out: list[_Save] = []
        for d, coords, mtime in found:
            sp = species_by_slug.get(d.parent.name)
            tomo = tomo_by_slug.get(d.name)
            reasons = []
            if sp is None:
                reasons.append(f"no registered species slugs to '{d.parent.name}'")
            if tomo is None:
                reasons.append(f"no known tomogram slugs to '{d.name}'")
            if reasons:
                out.append(_Save(d, coords, mtime, None, "", None, None, "; ".join(reasons)))
                continue
            star = tomograms_star_for(state, project_path, sp.id, tomo)
            reason = ""
            if star is None:
                reason = f"no tomograms.star carries {tomo} (nothing reconstructed or imported yet)"
            out.append(_Save(d, coords, mtime, sp.id, sp.name, tomo, star, reason))
        return out

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

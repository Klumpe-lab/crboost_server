"""TiltSeriesRegistry — authoritative in-memory model of TS/Frame/Tomogram state.

Persistence layout (per project):

    {project_root}/registry/
        index.json                         # cheap listing: ts_id → frame_count
        tilt_series/{ts_id}.json           # full TiltSeries serialized

Per-TS sidecars anticipate Stage 6 (lazy load for 100s of TS projects). For now
the registry is loaded eagerly — all TS are read into memory at project load —
but the on-disk shape already supports partial loads and per-TS atomic writes.

Concurrency: a single asyncio.Lock per registry serializes saves. Mutations
are in-memory and lock-free; `save()` flushes dirty TS to disk in one pass.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Set

from services.tilt_series.models import (
    Frame,
    FrameOutput,
    TiltSeries,
    TiltSeriesOutput,
    Tomogram,
    TomogramOutput,
)

logger = logging.getLogger(__name__)


# Schema version for the on-disk registry files. Bump MINOR for additive
# changes, MAJOR for incompatible ones.
REGISTRY_SCHEMA_VERSION = (1, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Registry
# ─────────────────────────────────────────────────────────────────────────────


class TiltSeriesRegistry:
    """Project-scoped model of every tilt-series, frame, and tomogram.

    One registry per project directory. Obtained via `get_registry_for(path)`.
    """

    def __init__(self, project_path: Path):
        self.project_path = project_path.resolve()
        self._tilt_series: Dict[str, TiltSeries] = {}
        # Fast reverse indexes rebuilt on mutation/load. Kept in sync with
        # _tilt_series; never set directly from outside.
        self._frame_index: Dict[str, TiltSeries] = {}       # frame_id → TS
        self._filename_index: Dict[str, Frame] = {}         # raw_filename → Frame
        self._dirty_ts: Set[str] = set()
        self._dirty_index: bool = False
        self._save_lock = asyncio.Lock()

    # ── Paths ──────────────────────────────────────────────────────────────

    @property
    def registry_dir(self) -> Path:
        return self.project_path / "registry"

    @property
    def ts_dir(self) -> Path:
        return self.registry_dir / "tilt_series"

    @property
    def index_path(self) -> Path:
        return self.registry_dir / "index.json"

    def _ts_path(self, ts_id: str) -> Path:
        return self.ts_dir / f"{ts_id}.json"

    # ── Lookups ────────────────────────────────────────────────────────────

    def get_tilt_series(self, ts_id: str) -> TiltSeries:
        ts = self._tilt_series.get(ts_id)
        if ts is None:
            raise KeyError(f"No tilt-series with id {ts_id!r} in registry")
        return ts

    def has_tilt_series(self, ts_id: str) -> bool:
        return ts_id in self._tilt_series

    def all_tilt_series(self) -> Iterable[TiltSeries]:
        return self._tilt_series.values()

    def tilt_series_ids(self) -> List[str]:
        return sorted(self._tilt_series)

    def get_frame(self, frame_id: str) -> Frame:
        ts = self._frame_index.get(frame_id)
        if ts is None:
            raise KeyError(f"No frame with id {frame_id!r} in registry")
        return ts.frame_by_id(frame_id)

    def frame_by_filename(self, name: str) -> Frame:
        """Resolve a frame by raw filename (basename). Matches filename or stem."""
        target = Path(name).name
        hit = self._filename_index.get(target)
        if hit is not None:
            return hit
        # fall back to stem-keyed lookup
        stem = Path(target).stem
        for ts in self._tilt_series.values():
            try:
                return ts.frame_by_filename(stem)
            except KeyError:
                continue
        raise KeyError(f"No frame matching filename {name!r}")

    def frame_count(self) -> int:
        return sum(ts.frame_count for ts in self._tilt_series.values())

    # ── Mutations ──────────────────────────────────────────────────────────

    def add_tilt_series(self, ts: TiltSeries, *, overwrite: bool = False) -> None:
        """Register a new tilt-series. Refuses to overwrite unless explicit."""
        if ts.id in self._tilt_series and not overwrite:
            raise ValueError(f"Tilt-series {ts.id!r} already registered; pass overwrite=True to replace")
        self._tilt_series[ts.id] = ts
        self._reindex_ts(ts)
        self._dirty_ts.add(ts.id)
        self._dirty_index = True

    def remove_tilt_series(self, ts_id: str) -> None:
        ts = self._tilt_series.pop(ts_id, None)
        if ts is None:
            return
        for f in ts.frames:
            self._frame_index.pop(f.id, None)
            self._filename_index.pop(f.raw_filename, None)
        self._dirty_index = True
        path = self._ts_path(ts_id)
        if path.exists():
            path.unlink()

    def attach_frame_output(self, frame_id: str, output: FrameOutput) -> None:
        frame = self.get_frame(frame_id)
        frame.outputs[output.job_instance_id] = output
        ts = self._frame_index[frame_id]
        self._dirty_ts.add(ts.id)

    def attach_ts_output(self, ts_id: str, output: TiltSeriesOutput) -> None:
        ts = self.get_tilt_series(ts_id)
        ts.outputs[output.job_instance_id] = output
        self._dirty_ts.add(ts_id)

    def attach_tomogram_output(self, ts_id: str, output: TomogramOutput) -> None:
        ts = self.get_tilt_series(ts_id)
        if ts.tomogram is None:
            ts.tomogram = Tomogram(id=ts_id, tilt_series_id=ts_id)
        ts.tomogram.outputs[output.job_instance_id] = output
        self._dirty_ts.add(ts_id)

    # ── Validation ─────────────────────────────────────────────────────────

    def assert_complete(self, job_instance_id: str, expected_ts_ids: Set[str]) -> None:
        """Raise if any of `expected_ts_ids` lacks an output for `job_instance_id`
        at either TS or per-frame scope."""
        missing_ts: List[str] = []
        missing_frames: List[str] = []

        for ts_id in sorted(expected_ts_ids):
            if ts_id not in self._tilt_series:
                missing_ts.append(f"{ts_id} (not registered)")
                continue
            ts = self._tilt_series[ts_id]
            has_ts_output = job_instance_id in ts.outputs
            has_tomo_output = (
                ts.tomogram is not None and job_instance_id in ts.tomogram.outputs
            )
            if has_ts_output or has_tomo_output:
                continue
            # Otherwise every frame must carry the output
            missing = [f.id for f in ts.frames if job_instance_id not in f.outputs]
            if missing:
                missing_frames.append(f"{ts_id}: missing on frames {missing[:5]}{'...' if len(missing) > 5 else ''}")

        problems: List[str] = []
        if missing_ts:
            problems.append(f"TS not registered: {missing_ts}")
        if missing_frames:
            problems.append("Incomplete per-frame outputs:\n  - " + "\n  - ".join(missing_frames))
        if problems:
            raise RuntimeError(
                f"Registry job-completion check failed for job {job_instance_id!r}:\n  "
                + "\n  ".join(problems)
            )

    # ── Persistence ────────────────────────────────────────────────────────

    def load(self) -> None:
        """Load all TS from on-disk sidecars. Safe to call on a fresh registry."""
        if not self.index_path.exists():
            logger.debug("No registry index at %s; starting empty", self.index_path)
            return

        try:
            with open(self.index_path) as f:
                index = json.load(f)
        except Exception as e:
            logger.warning("Failed to read registry index %s: %s", self.index_path, e)
            return

        version = tuple(index.get("schema_version", (0, 0)))
        if version[0] > REGISTRY_SCHEMA_VERSION[0]:
            logger.warning(
                "Registry schema version %s is newer than code (%s); proceeding with caution",
                version, REGISTRY_SCHEMA_VERSION,
            )

        ts_ids = index.get("tilt_series", [])
        loaded = 0
        for ts_id in ts_ids:
            ts_path = self._ts_path(ts_id)
            if not ts_path.exists():
                logger.warning("Registry index references missing TS file: %s", ts_path)
                continue
            try:
                with open(ts_path) as f:
                    data = json.load(f)
                ts = TiltSeries.model_validate(data)
                self._tilt_series[ts.id] = ts
                self._reindex_ts(ts)
                loaded += 1
            except Exception as e:
                logger.warning("Failed to load TS sidecar %s: %s", ts_path, e)

        logger.info("Loaded %d tilt-series from %s", loaded, self.registry_dir)
        self._dirty_ts.clear()
        self._dirty_index = False

    def save(self, *, force: bool = False) -> None:
        """Flush dirty TS + index to disk atomically (per-file rename)."""
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        self.ts_dir.mkdir(parents=True, exist_ok=True)

        dirty = self._dirty_ts if not force else set(self._tilt_series)
        for ts_id in sorted(dirty):
            ts = self._tilt_series.get(ts_id)
            if ts is None:
                continue
            self._atomic_write(self._ts_path(ts_id), ts.model_dump_json(indent=2))

        if dirty or self._dirty_index or force:
            index = {
                "schema_version": list(REGISTRY_SCHEMA_VERSION),
                "tilt_series": sorted(self._tilt_series),
                "frame_count": self.frame_count(),
            }
            self._atomic_write(self.index_path, json.dumps(index, indent=2))

        self._dirty_ts.clear()
        self._dirty_index = False

    async def save_async(self, *, force: bool = False) -> None:
        async with self._save_lock:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: self.save(force=force))

    # ── Internals ──────────────────────────────────────────────────────────

    def _reindex_ts(self, ts: TiltSeries) -> None:
        # Wipe any stale entries for this TS, then rebuild.
        stale_frame_ids = [fid for fid, t in self._frame_index.items() if t.id == ts.id]
        for fid in stale_frame_ids:
            self._frame_index.pop(fid, None)
        stale_names = [name for name, f in self._filename_index.items() if f.tilt_series_id == ts.id]
        for name in stale_names:
            self._filename_index.pop(name, None)

        seen_ids: Set[str] = set()
        for f in ts.frames:
            if f.id in seen_ids:
                raise ValueError(f"Duplicate frame id {f.id!r} in TS {ts.id}")
            seen_ids.add(f.id)
            self._frame_index[f.id] = ts
            self._filename_index[f.raw_filename] = f

    @staticmethod
    def _atomic_write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=path.suffix)
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
            os.replace(tmp, str(path))
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # ── Sanity ─────────────────────────────────────────────────────────────

    def sanity_check(self) -> List[str]:
        """Return a list of integrity problems; empty means the registry is consistent."""
        problems: List[str] = []
        seen_frame_ids: Set[str] = set()
        for ts in self._tilt_series.values():
            if ts.id != ts.mdoc_filename.rsplit(".", 1)[0]:
                problems.append(
                    f"TS {ts.id!r} id does not match mdoc stem {ts.mdoc_filename!r}"
                )
            for i, f in enumerate(ts.frames):
                if f.tilt_index != i:
                    problems.append(
                        f"TS {ts.id!r}: frame at position {i} has tilt_index={f.tilt_index}"
                    )
                if f.tilt_series_id != ts.id:
                    problems.append(
                        f"Frame {f.id!r} tilt_series_id={f.tilt_series_id!r} != parent {ts.id!r}"
                    )
                if f.id in seen_frame_ids:
                    problems.append(f"Duplicate frame id across TS: {f.id!r}")
                seen_frame_ids.add(f.id)
            if ts.tomogram is not None and ts.tomogram.tilt_series_id != ts.id:
                problems.append(
                    f"Tomogram {ts.tomogram.id!r} tilt_series_id does not match parent TS {ts.id!r}"
                )
        return problems


# ─────────────────────────────────────────────────────────────────────────────
# Path-keyed singleton (mirrors ProjectState registry)
# ─────────────────────────────────────────────────────────────────────────────


_registries: Dict[Path, TiltSeriesRegistry] = {}


def get_registry_for(project_path: Path) -> TiltSeriesRegistry:
    """Get or lazily load the registry for a project directory.

    First access triggers a disk load (if registry/ exists) or returns an
    empty registry. Subsequent calls return the same in-memory instance.
    """
    resolved = project_path.resolve()
    reg = _registries.get(resolved)
    if reg is None:
        reg = TiltSeriesRegistry(resolved)
        reg.load()
        _registries[resolved] = reg
    return reg


def set_registry_for(project_path: Path, registry: TiltSeriesRegistry) -> None:
    """Install or replace a registry in the path-keyed cache. Used by the
    project-initialization flow when a registry is built from a fresh
    DatasetOverview and should be cached immediately."""
    _registries[project_path.resolve()] = registry


def clear_registry(project_path: Path) -> None:
    """Drop a registry from the cache (e.g. on project close)."""
    _registries.pop(project_path.resolve(), None)

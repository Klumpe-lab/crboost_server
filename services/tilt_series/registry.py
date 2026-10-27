"""TiltSeriesRegistry — authoritative in-memory model of TS/Frame/Tomogram state.

Persistence layout (per project):

    {project_root}/registry/
        index.json                         # cheap listing: ts_id → frame_count
        tilt_series/{ts_id}.json           # full TiltSeries serialized

The registry is loaded eagerly (all TS are read into memory at project load);
the per-TS sidecars allow partial loads and per-TS atomic writes.

Concurrency: several processes write one registry, each from its own copy:
supervisors ingesting on compute nodes (two chains can aggregate at once) and the
server (exclusion toggles, tilt verdicts). Mutations are in memory and also kept
as changes to replay; `save()` takes the project's registry lock (`registry_lock`),
re-reads each sidecar it writes and replays this copy's changes onto it, so nothing
another process wrote in the meantime is lost. Within the server, an asyncio.Lock
per registry serializes `save_async()`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

from pydantic import ValidationError

from services.shared_files import make_shared
from services.tilt_series.models import Frame, FrameOutput, TiltSeries, TiltSeriesOutput, Tomogram, TomogramOutput

logger = logging.getLogger(__name__)

# A full load reads this many sidecars at a time: on the shared mount every read is a round
# trip, and the files do not depend on each other.
_READ_THREADS = 16
_dict_path_warned = False


def _parse_tilt_series(raw: bytes) -> TiltSeries:
    """One sidecar's TiltSeries, validated from its JSON text directly, which skips building the
    intermediate dicts (most of a large project's load time). A file the JSON mode rejects is
    validated from the parsed dicts instead, so a difference between the two modes costs speed,
    never a tilt-series; invalid JSON raises from that second attempt."""
    global _dict_path_warned
    try:
        return TiltSeries.model_validate_json(raw)
    except ValidationError:
        ts = TiltSeries.model_validate(json.loads(raw))
        if not _dict_path_warned:
            _dict_path_warned = True
            logger.warning("A registry sidecar validated only from parsed dicts (%s); loads run slower", ts.id)
        return ts


# The registry lock (`registry/.lock`) is held for the length of one save, seconds. A lock older
# than _LOCK_STALE_S was left by a process that died holding it; a waiter gives up after
# _LOCK_WAIT_S, which leaves time to take a stale lock over first.
_LOCK_NAME = ".lock"
_LOCK_STALE_S = 600.0
_LOCK_WAIT_S = 900.0
_LOCK_POLL_S = 0.5


# Schema version for the on-disk registry files. Bump MINOR for additive
# changes, MAJOR for incompatible ones.
# (1, 1): additive Frame.is_filtered_out / filter_reason (tilt-filter verdict).
# (1, 2): additive Frame raw acquisition metadata (exposure_dose, dose_rate,
#         exposure_time, defocus, intensities, image_shift) captured from mdoc;
#         additive FsMotionCtfFrameOutput.ctf_resolution / mean_frame_movement
#         (real QC values from the Warp XML; the star columns are placeholders).
# (1, 3): additive Frame.filter_probability (DL tilt-filter score alongside the
#         boolean verdict); additive DenoisePredictTomogramOutput tomogram output.
# (1, 4): additive per-tilt QC the Journey charts: FsMotionCtfFrameOutput
#         defocus_spread_um + motion_track_x/y/source; TsAlignmentPerFrame
#         average_intensity / masked_fraction / fov_fraction; TsCtfTiltSeriesOutput
#         ctf_resolution / plane_normal. Older registries read fine (fields
#         default to None).
# (1, 5): additive Frame.p_bad (the DL classifier's P(bad), kept apart from the
#         verdict). Frame.filter_probability is dropped on load: it held the winning
#         class's probability (1.0 after a manual commit), not a P(bad).
# (1, 6): additive SegmentationTomogramOutput tomogram output. Code older than this
#         fails to validate a sidecar that holds one and skips that tilt series.
# (1, 7): additive "easymode" DenoisePredictTomogramOutput.denoise_method. Code older than
#         this fails to validate a sidecar that holds one and skips that tilt series.
REGISTRY_SCHEMA_VERSION = (1, 7)


# ─────────────────────────────────────────────────────────────────────────────
# Registry
# ─────────────────────────────────────────────────────────────────────────────


class TiltSeriesRegistry:
    """Project-scoped model of every tilt-series, frame, and tomogram.

    One registry per project directory. Obtained via `get_registry_for(path)`.
    """

    def __init__(self, project_path: Path):
        self.project_path = project_path.resolve()
        self._tilt_series: dict[str, TiltSeries] = {}
        # Fast reverse indexes rebuilt on mutation/load. Kept in sync with
        # _tilt_series; never set directly from outside.
        self._frame_index: dict[str, TiltSeries] = {}  # frame_id → TS
        self._filename_index: dict[str, Frame] = {}  # raw_filename → Frame
        self._dirty_ts: set[str] = set()
        self._dirty_index: bool = False
        # Per TS, the changes made since the last save, each a function that makes it on a TS;
        # save() replays them onto the sidecar as it is on disk then. A dirty TS without any
        # (one added here) is written as it is in memory.
        self._pending: dict[str, list[Callable[[TiltSeries], None]]] = {}
        # Sidecar mtime observed when each TS was last parsed, so refresh_from_disk() can
        # re-parse only what a driver actually rewrote instead of the whole project.
        self._ts_mtime_ns: dict[str, int] = {}
        self._save_lock = asyncio.Lock()
        # index.json mtime observed at load time. Drivers ingest on compute
        # nodes and save to disk; the server's cached instance detects that via
        # is_stale_on_disk() and get_registry_for() reloads. None = loaded with
        # no index on disk (empty/pre-registry project).
        self._loaded_index_mtime_ns: int | None = None
        # Set by load() when newer code wrote this registry: what to tell the user.
        self.version_skew: str | None = None
        # Set by load() and refresh_from_disk() when part of the registry could not be read: which
        # files and why. The tilt series they hold are missing from this copy.
        self.load_problem: str | None = None
        # The schema version the on-disk index was written with, None before a load found one.
        self.schema_on_disk: tuple[int, ...] | None = None

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

    def tilt_series_ids(self) -> list[str]:
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
        self._pending.pop(ts.id, None)  # written whole
        self._dirty_index = True

    def remove_tilt_series(self, ts_id: str) -> None:
        if ts_id not in self._tilt_series:
            return
        self._forget_tilt_series(ts_id)
        self._dirty_index = True
        path = self._ts_path(ts_id)
        if path.exists():
            path.unlink()

    def attach_frame_output(self, frame_id: str, output: FrameOutput) -> None:
        def attach(ts: TiltSeries) -> None:
            ts.frame_by_id(frame_id).outputs[output.job_instance_id] = output

        self._change(self._ts_of_frame(frame_id).id, attach)

    def attach_ts_output(self, ts_id: str, output: TiltSeriesOutput) -> None:
        def attach(ts: TiltSeries) -> None:
            ts.outputs[output.job_instance_id] = output

        self._change(ts_id, attach)

    def attach_tomogram_output(self, ts_id: str, output: TomogramOutput) -> None:
        def attach(ts: TiltSeries) -> None:
            if ts.tomogram is None:
                ts.tomogram = Tomogram(id=ts.id, tilt_series_id=ts.id)
            ts.tomogram.outputs[output.job_instance_id] = output

        self._change(ts_id, attach)

    def detach_tomogram_output(self, ts_id: str, job_instance_id: str) -> None:
        """Remove one job's output from a tomogram: a run of the job that no longer produced it."""

        def detach(ts: TiltSeries) -> None:
            if ts.tomogram is not None:
                ts.tomogram.outputs.pop(job_instance_id, None)

        self._change(ts_id, detach)

    def set_excluded(self, ts_id: str, excluded: bool, *, reason: str | None = None) -> None:
        """Mark/unmark a tilt-series as excluded from processing (forward-only).

        Mutates in memory + marks dirty; the caller persists via
        save()/save_async(). Undo is just `set_excluded(ts_id, False)`.
        """

        def mark(ts: TiltSeries) -> None:
            ts.is_excluded = excluded
            ts.exclusion_reason = reason if excluded else None

        self._change(ts_id, mark)

    def excluded_ids(self) -> set[str]:
        """The set of TS ids the user has excluded from processing."""
        return {ts.id for ts in self._tilt_series.values() if ts.is_excluded}

    def set_frame_filtered(self, frame_id: str, filtered: bool, *, reason: str | None = None) -> None:
        """Record the tilt-filter's per-frame verdict, which alignment applies when it
        snapshots the tomostars. Re-stamped on every commit, in both directions. A DL
        prediction (`Frame.p_bad`) is left as it is. Mutates in memory + marks the parent
        TS dirty; the caller persists via save()/save_async(). Raises KeyError if the
        frame is unknown (caller decides whether that's fatal)."""

        def stamp(ts: TiltSeries) -> None:
            frame = ts.frame_by_id(frame_id)
            frame.is_filtered_out = filtered
            frame.filter_reason = reason if filtered else None

        self._change(self._ts_of_frame(frame_id).id, stamp)

    def set_frame_prediction(self, frame_id: str, p_bad: float) -> None:
        """Record the DL classifier's P(bad) for one frame. A prediction only: the verdict
        (`is_filtered_out`) is left as it is. Mutates in memory + marks the parent TS dirty;
        the caller persists via save(). Raises KeyError if the frame is unknown."""

        def predict(ts: TiltSeries) -> None:
            ts.frame_by_id(frame_id).p_bad = p_bad

        self._change(self._ts_of_frame(frame_id).id, predict)

    def _ts_of_frame(self, frame_id: str) -> TiltSeries:
        ts = self._frame_index.get(frame_id)
        if ts is None:
            raise KeyError(f"No frame with id {frame_id!r} in registry")
        return ts

    def _change(self, ts_id: str, change: Callable[[TiltSeries], None]) -> None:
        """Make `change` on a TS now, and keep it for save() to replay onto the sidecar on disk."""
        change(self.get_tilt_series(ts_id))
        self._pending.setdefault(ts_id, []).append(change)
        self._dirty_ts.add(ts_id)

    def filtered_out_frame_ids(self) -> set[str]:
        """The set of frame ids the tilt-filter has marked filtered-out."""
        return {f.id for ts in self._tilt_series.values() for f in ts.frames if f.is_filtered_out}

    # ── Validation ─────────────────────────────────────────────────────────

    def assert_complete(self, job_instance_id: str, expected_ts_ids: set[str]) -> None:
        """Raise if any of `expected_ts_ids` lacks an output for `job_instance_id`
        at either TS or per-frame scope."""
        missing_ts: list[str] = []
        missing_frames: list[str] = []

        for ts_id in sorted(expected_ts_ids):
            if ts_id not in self._tilt_series:
                missing_ts.append(f"{ts_id} (not registered)")
                continue
            ts = self._tilt_series[ts_id]
            has_ts_output = job_instance_id in ts.outputs
            has_tomo_output = ts.tomogram is not None and job_instance_id in ts.tomogram.outputs
            if has_ts_output or has_tomo_output:
                continue
            # Otherwise every frame must carry the output
            missing = [f.id for f in ts.frames if job_instance_id not in f.outputs]
            if missing:
                missing_frames.append(f"{ts_id}: missing on frames {missing[:5]}{'...' if len(missing) > 5 else ''}")

        problems: list[str] = []
        if missing_ts:
            problems.append(f"TS not registered: {missing_ts}")
        if missing_frames:
            problems.append("Incomplete per-frame outputs:\n  - " + "\n  - ".join(missing_frames))
        if problems:
            raise RuntimeError(
                f"Registry job-completion check failed for job {job_instance_id!r}:\n  " + "\n  ".join(problems)
            )

    # ── Persistence ────────────────────────────────────────────────────────

    def has_unsaved_changes(self) -> bool:
        return bool(self._dirty_ts) or self._dirty_index

    def is_stale_on_disk(self) -> bool:
        """True when the on-disk index has changed since this instance loaded —
        i.e. another process (a driver's supervisor ingest) wrote the registry."""
        try:
            return self.index_path.stat().st_mtime_ns != self._loaded_index_mtime_ns
        except FileNotFoundError:
            return self._loaded_index_mtime_ns is not None

    def load(self) -> None:
        """Load all TS from on-disk sidecars. Safe to call on a fresh registry."""
        if not self.index_path.exists():
            logger.debug("No registry index at %s; starting empty", self.index_path)
            self._loaded_index_mtime_ns = None
            return

        try:
            with open(self.index_path) as f:
                index = json.load(f)
        except (OSError, ValueError) as e:
            self.load_problem = f"The registry index {self.index_path} could not be read ({e}): no tilt series loaded."
            logger.warning("%s", self.load_problem)
            return

        version = tuple(index.get("schema_version", (0, 0)))
        self.schema_on_disk = version
        newer_on_disk = version > REGISTRY_SCHEMA_VERSION

        ts_ids = index.get("tilt_series", [])
        with ThreadPoolExecutor(max_workers=_READ_THREADS, thread_name_prefix="registry-read") as pool:
            sidecars = list(pool.map(self._read_sidecar, ts_ids))
        loaded = 0
        unreadable: list[str] = []
        for ts_id, sidecar in zip(ts_ids, sidecars, strict=True):
            ts_path = self._ts_path(ts_id)
            if isinstance(sidecar, FileNotFoundError):
                logger.warning("Registry index references missing TS file: %s", ts_path)
                continue
            try:
                if isinstance(sidecar, OSError):
                    raise sidecar
                mtime_ns, raw = sidecar
                self._load_sidecar(ts_id, ts_path, replacing=False, mtime_ns=mtime_ns, raw=raw)
                loaded += 1
            except (OSError, ValueError) as e:
                logger.warning("Failed to load TS sidecar %s: %s", ts_path, e)
                unreadable.append(f"{ts_path.name}: {e}")

        logger.info("Loaded %d tilt-series from %s", loaded, self.registry_dir)
        self.load_problem = self._unreadable_problem(unreadable, len(ts_ids))
        self.version_skew = None
        if newer_on_disk:
            skipped = len(ts_ids) - loaded
            self.version_skew = (
                f"The tilt-series registry was written by newer code (schema {'.'.join(map(str, version))}) than this "
                f"server runs ({'.'.join(map(str, REGISTRY_SCHEMA_VERSION))}): {skipped} of {len(ts_ids)} tilt-series "
                "could not be read and are missing here. Update this checkout before running jobs on this project."
            )
            logger.warning("%s: %s", self.registry_dir, self.version_skew)
        try:
            self._loaded_index_mtime_ns = self.index_path.stat().st_mtime_ns
        except FileNotFoundError:
            self._loaded_index_mtime_ns = None
        self._dirty_ts.clear()
        self._pending.clear()
        self._dirty_index = False

    def refresh_from_disk(self) -> None:
        """Re-sync with the on-disk registry, re-parsing only the sidecars that changed.

        A driver ingesting on a compute node rewrites a handful of sidecars and touches the
        index. A full `load()` would re-validate every sidecar in the project (~19 MB of
        pydantic per poll on a 114-tilt-series project, on the event loop), so only sidecars
        whose own mtime moved are re-read; the rest keep their parsed objects.

        Caller must ensure there are no unsaved in-memory changes (see get_registry_for).
        """
        if not self.index_path.exists():
            return
        try:
            with open(self.index_path) as f:
                index = json.load(f)
        except (OSError, ValueError) as e:
            self.load_problem = f"The registry index {self.index_path} could not be read ({e})."
            logger.warning("%s", self.load_problem)
            return

        ts_ids = index.get("tilt_series", [])
        reparsed = 0
        unreadable: list[str] = []
        for ts_id in ts_ids:
            ts_path = self._ts_path(ts_id)
            try:
                mtime = ts_path.stat().st_mtime_ns
            except OSError:
                logger.warning("Registry index references missing TS file: %s", ts_path)
                continue
            if ts_id in self._tilt_series and self._ts_mtime_ns.get(ts_id) == mtime:
                continue
            try:
                self._load_sidecar(ts_id, ts_path, replacing=ts_id in self._tilt_series, mtime_ns=mtime)
                reparsed += 1
            except (OSError, ValueError) as e:
                logger.warning("Failed to load TS sidecar %s: %s", ts_path, e)
                unreadable.append(f"{ts_path.name}: {e}")
        self.load_problem = self._unreadable_problem(unreadable, len(ts_ids))

        for gone in set(self._tilt_series) - set(ts_ids):
            self._forget_tilt_series(gone)

        logger.info("Registry refresh: %d of %d tilt-series re-parsed", reparsed, len(ts_ids))
        try:
            self._loaded_index_mtime_ns = self.index_path.stat().st_mtime_ns
        except FileNotFoundError:
            self._loaded_index_mtime_ns = None

    def _unreadable_problem(self, unreadable: list[str], total: int) -> str | None:
        """load_problem for the sidecars that could not be read; None when every one could."""
        if not unreadable:
            return None
        shown = "; ".join(unreadable[:3]) + (f"; and {len(unreadable) - 3} more" if len(unreadable) > 3 else "")
        return (
            f"{len(unreadable)} of {total} tilt-series files in {self.ts_dir} could not be read, so those tilt "
            f"series are missing here: {shown}"
        )

    def raise_if_unreadable(self) -> None:
        """For a job about to work from this registry: raise, naming the files, when part of it could
        not be read. The job would otherwise run on the tilt series that could, and leave out the rest
        without a word."""
        if self.load_problem:
            raise RuntimeError(" ".join(m for m in (self.load_problem, self.version_skew) if m))

    def save(self, *, force: bool = False) -> None:
        """Flush dirty TS + index to disk atomically (per-file rename), under the registry lock.

        A sidecar this copy changed is read again under the lock and the changes replayed onto
        it, so what another process wrote since this copy loaded it is kept. Read, not compared
        by mtime: on the shared mount stat() can report the old mtime of a sidecar another node
        replaced for up to a minute, while open() reads what is there. The index lists every
        tilt-series this copy holds plus any another process added; `force` writes this copy as
        the whole registry (a rebuild from the mdocs)."""
        if not (self._dirty_ts or self._dirty_index or force):
            return
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        self.ts_dir.mkdir(parents=True, exist_ok=True)

        with registry_lock(self.registry_dir):
            # A snapshot: the server saves off the event loop, and a change made meanwhile stays
            # dirty and pending for the next save.
            dirty = set(self._tilt_series) if force else set(self._dirty_ts)
            written: set[str] = set()
            try:
                for ts_id in sorted(dirty):
                    if ts_id not in self._tilt_series:
                        continue
                    ts_path = self._ts_path(ts_id)
                    changes = self._pending.pop(ts_id, [])
                    try:
                        if changes and ts_path.exists():
                            self._load_sidecar(ts_id, ts_path, replacing=True)
                            for change in changes:
                                change(self._tilt_series[ts_id])
                        self._atomic_write(ts_path, self._tilt_series[ts_id].model_dump_json(indent=2))
                    except BaseException:
                        # Not written: the changes wait for the next save, ahead of any made since.
                        self._pending[ts_id] = changes + self._pending.get(ts_id, [])
                        raise
                    written.add(ts_id)
                    # Our own write is not a change to re-parse on the next refresh.
                    try:
                        self._ts_mtime_ns[ts_id] = ts_path.stat().st_mtime_ns
                    except OSError:
                        self._ts_mtime_ns.pop(ts_id, None)
            except BaseException:
                # What was written before the failure is clean: left dirty with its changes spent, the
                # next save would write it from memory, over what another process wrote since.
                self._dirty_ts -= {i for i in written if not self._pending.get(i)}
                raise

            on_disk, disk_mtime = self._read_index()
            listed = set(self._tilt_series)
            if not force:
                listed |= {i for i in on_disk if i not in self._tilt_series and self._ts_path(i).exists()}
            index = {
                "schema_version": list(REGISTRY_SCHEMA_VERSION),
                "tilt_series": sorted(listed),
                "frame_count": self.frame_count(),
            }
            self._atomic_write(self.index_path, json.dumps(index, indent=2))
            # When another process wrote since this copy loaded, the old mark stays, so the next
            # get_registry_for() re-reads the sidecars it changed.
            if disk_mtime == self._loaded_index_mtime_ns:
                try:
                    self._loaded_index_mtime_ns = self.index_path.stat().st_mtime_ns
                except FileNotFoundError:
                    pass

        # In place: a change made meanwhile on the event loop adds to this set concurrently.
        done = {i for i in dirty if i not in self._tilt_series or (i in written and not self._pending.get(i))}
        self._dirty_ts -= done
        self._dirty_index = False

    def _read_index(self) -> tuple[list[str], int | None]:
        """The on-disk index's tilt-series ids and mtime, read fresh (open, then fstat)."""
        try:
            with open(self.index_path) as f:
                mtime = os.fstat(f.fileno()).st_mtime_ns
                return json.load(f).get("tilt_series", []), mtime
        except FileNotFoundError:
            return [], None

    async def save_async(self, *, force: bool = False) -> None:
        async with self._save_lock:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: self.save(force=force))

    # ── Internals ──────────────────────────────────────────────────────────

    def _read_sidecar(self, ts_id: str) -> tuple[int, bytes] | OSError:
        """(mtime_ns, contents) of one sidecar, or the error reading it, for a pool thread. The
        mtime comes from the open file: on the shared mount stat() can lag what open() reads."""
        try:
            with open(self._ts_path(ts_id), "rb") as f:
                return os.fstat(f.fileno()).st_mtime_ns, f.read()
        except OSError as e:
            # Returned, not raised: load() reports each unreadable sidecar and loads the rest.
            return e

    def _load_sidecar(
        self, ts_id: str, ts_path: Path, *, replacing: bool, mtime_ns: int | None = None, raw: bytes | None = None
    ) -> None:
        """Parse one TS sidecar into the registry and record its mtime. Raises on bad JSON.
        `raw` is the file's contents when the caller already read them."""
        if mtime_ns is None:
            mtime_ns = ts_path.stat().st_mtime_ns
        if raw is None:
            raw = ts_path.read_bytes()
        ts = _parse_tilt_series(raw)
        self._tilt_series[ts.id] = ts
        self._reindex_ts(ts, replacing=replacing)
        self._ts_mtime_ns[ts.id] = mtime_ns

    def _forget_tilt_series(self, ts_id: str) -> None:
        """Drop a TS from memory without deleting its sidecar — the disk copy is already gone
        or was removed by another process. `remove_tilt_series` is the user-facing delete."""
        ts = self._tilt_series.pop(ts_id, None)
        self._ts_mtime_ns.pop(ts_id, None)
        self._pending.pop(ts_id, None)
        if ts is None:
            return
        for f in ts.frames:
            self._frame_index.pop(f.id, None)
            self._filename_index.pop(f.raw_filename, None)

    def _reindex_ts(self, ts: TiltSeries, *, replacing: bool = True) -> None:
        # Index this TS's frames, then drop the stale entries it no longer has. In that order: the
        # server re-reads a sidecar in its save thread while the event loop looks frames up, and a
        # lookup must never miss a frame the TS still has. The stale scan covers every frame in the
        # project, so it is skipped when this TS is being indexed for the first time (a fresh
        # load) — otherwise a full load is quadratic in total frame count.
        seen_ids: set[str] = set()
        for f in ts.frames:
            if f.id in seen_ids:
                raise ValueError(f"Duplicate frame id {f.id!r} in TS {ts.id}")
            seen_ids.add(f.id)
        for f in ts.frames:
            self._frame_index[f.id] = ts
            self._filename_index[f.raw_filename] = f

        if replacing:
            names = {f.raw_filename for f in ts.frames}
            stale_frame_ids = [fid for fid, t in self._frame_index.items() if t.id == ts.id and fid not in seen_ids]
            for fid in stale_frame_ids:
                self._frame_index.pop(fid, None)
            stale_names = [n for n, f in self._filename_index.items() if f.tilt_series_id == ts.id and n not in names]
            for name in stale_names:
                self._filename_index.pop(name, None)

    @staticmethod
    def _atomic_write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=path.suffix)
        try:
            make_shared(fd)
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

    def sanity_check(self) -> list[str]:
        """Return a list of integrity problems; empty means the registry is consistent."""
        problems: list[str] = []
        seen_frame_ids: set[str] = set()
        for ts in self._tilt_series.values():
            if ts.id != ts.mdoc_filename.rsplit(".", 1)[0]:
                problems.append(f"TS {ts.id!r} id does not match mdoc stem {ts.mdoc_filename!r}")
            for i, f in enumerate(ts.frames):
                if f.tilt_index != i:
                    problems.append(f"TS {ts.id!r}: frame at position {i} has tilt_index={f.tilt_index}")
                if f.tilt_series_id != ts.id:
                    problems.append(f"Frame {f.id!r} tilt_series_id={f.tilt_series_id!r} != parent {ts.id!r}")
                if f.id in seen_frame_ids:
                    problems.append(f"Duplicate frame id across TS: {f.id!r}")
                seen_frame_ids.add(f.id)
            if ts.tomogram is not None and ts.tomogram.tilt_series_id != ts.id:
                problems.append(f"Tomogram {ts.tomogram.id!r} tilt_series_id does not match parent TS {ts.id!r}")
        return problems


# ─────────────────────────────────────────────────────────────────────────────
# Cross-process lock
# ─────────────────────────────────────────────────────────────────────────────


@contextmanager
def registry_lock(registry_dir: Path) -> Iterator[None]:
    """Hold the project's registry lock for the block: `registry/.lock`, created with O_EXCL and
    naming its holder. O_EXCL rather than flock, which does not reach across the nodes of the
    shared mount. A lock whose holder died is taken over: on this host when its process is gone,
    anywhere once it is older than _LOCK_STALE_S. Raises TimeoutError, naming the holder, after
    _LOCK_WAIT_S."""
    path = registry_dir / _LOCK_NAME
    me = {
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "since": time.time(),
    }
    deadline = time.monotonic() + _LOCK_WAIT_S
    while True:
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o664)
        except FileExistsError:
            holder = _lock_holder(path)
            if holder is None:
                continue  # released meanwhile
            if _lock_is_stale(holder) and _take_over(path, holder):
                continue
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"The tilt-series registry is locked by {holder} (lock file {path}); gave up after "
                    f"{_LOCK_WAIT_S:.0f} s. Delete the lock file if that process is gone."
                ) from None
            time.sleep(_LOCK_POLL_S)
            continue
        with os.fdopen(fd, "w") as f:
            json.dump(me, f)
        break
    try:
        yield
    finally:
        _unlink_lock_of(path, me)


def _lock_holder(path: Path) -> dict | None:
    """Who holds the lock, as its file says; None when there is no lock file any more. A lock
    whose holder has not written its name yet reads as held since the file's mtime."""
    try:
        with open(path) as f:
            text = f.read()
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return None
    try:
        holder = json.loads(text)
    except json.JSONDecodeError:
        holder = {}
    holder.setdefault("since", mtime)
    return holder


def _lock_is_stale(holder: dict) -> bool:
    if holder.get("host") == socket.gethostname() and isinstance(holder.get("pid"), int):
        try:
            os.kill(holder["pid"], 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            pass  # alive, under another user
    return time.time() - float(holder.get("since", 0)) > _LOCK_STALE_S


def _unlink_lock_of(path: Path, holder: dict) -> None:
    """Remove the lock if it is still `holder`'s, not one another process took meanwhile."""
    current = _lock_holder(path)
    if current is not None and all(current.get(k) == holder.get(k) for k in ("host", "pid", "since")):
        path.unlink(missing_ok=True)


def _take_over(path: Path, holder: dict) -> bool:
    """Remove a dead holder's lock, one waiter at a time; False when another waiter is at it.
    Waiters cross the stale age together, and two that both read the dead lock would both remove
    it, the second removing the lock the first had just taken; a second O_EXCL file serializes the
    read-and-remove. One left behind by a waiter that died within those few lines goes by age."""
    guard = path.with_name(path.name + ".takeover")
    try:
        fd = os.open(guard, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o664)
    except FileExistsError:
        try:
            if time.time() - guard.stat().st_mtime > _LOCK_STALE_S:
                guard.unlink(missing_ok=True)
        except FileNotFoundError:
            pass  # the other waiter finished its takeover
        return False
    os.close(fd)
    try:
        logger.warning("Registry lock %s: taking it over from %s, which no longer holds it", path, holder)
        _unlink_lock_of(path, holder)
    finally:
        guard.unlink(missing_ok=True)
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Path-keyed singleton (mirrors ProjectState registry)
# ─────────────────────────────────────────────────────────────────────────────


_registries: dict[Path, TiltSeriesRegistry] = {}


def get_registry_for(project_path: Path) -> TiltSeriesRegistry:
    """Get or lazily load the registry for a project directory.

    First access triggers a disk load (if registry/ exists) or returns an
    empty registry. Subsequent calls return the same in-memory instance —
    unless the on-disk index changed under it (a driver's supervisor ingested
    outputs on a compute node), in which case the cached instance re-syncs in
    place, re-parsing only the sidecars whose own mtime moved. A cached instance
    with unsaved in-memory changes is never re-synced (its own save() lands
    first; the next call picks up both)."""
    resolved = project_path.resolve()
    reg = _registries.get(resolved)
    if reg is None:
        reg = TiltSeriesRegistry(resolved)
        reg.load()
        _registries[resolved] = reg
    elif reg.is_stale_on_disk() and not reg.has_unsaved_changes():
        reg.refresh_from_disk()
    return reg


async def preload_registry(project_path: Path) -> None:
    """First access to a project's registry: parse its sidecars off the event loop (tens of MB of
    pydantic on a large project) and install it, unless another caller cached one meanwhile. A no-op
    when the registry is already cached; get_registry_for keeps owning re-syncs."""
    resolved = project_path.resolve()
    if resolved in _registries:
        return
    fresh = TiltSeriesRegistry(resolved)
    await asyncio.to_thread(fresh.load)
    _registries.setdefault(resolved, fresh)


async def reload_registry(project_path: Path) -> TiltSeriesRegistry:
    """Re-read a project's registry from its files, whatever the index's mtime says, and
    install it in the cache.

    For a caller that knows another process has just replaced the registry files (a SLURM job
    that has landed). get_registry_for re-syncs only when the index's mtime moves, and the
    headnode's NFS client can report a file that a compute node replaced with its old mtime for
    up to a minute; opening the file revalidates it, so a fresh parse reads the new contents.
    The files are parsed off the event loop and the cache entry is swapped on it, so no reader
    sees a half-loaded registry. A cached instance with unsaved changes is kept, as in
    get_registry_for."""
    resolved = project_path.resolve()
    fresh = TiltSeriesRegistry(resolved)
    await asyncio.to_thread(fresh.load)
    cached = _registries.get(resolved)
    if cached is not None and cached.has_unsaved_changes():
        logger.warning("Registry %s has unsaved changes; kept instead of the copy re-read from disk", resolved)
        return cached
    _registries[resolved] = fresh
    return fresh


def set_registry_for(project_path: Path, registry: TiltSeriesRegistry) -> None:
    """Install or replace a registry in the path-keyed cache. Used by the
    project-initialization flow when a registry is built from a fresh
    DatasetOverview and should be cached immediately."""
    _registries[project_path.resolve()] = registry


def clear_registry(project_path: Path) -> None:
    """Drop a registry from the cache (e.g. on project close)."""
    _registries.pop(project_path.resolve(), None)

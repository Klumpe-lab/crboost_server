"""How far a running job has got, as its tool says in run.out.

Standalone: nothing in the pipeline depends on it. `read(job_type, job_dir)` returns a
`LogProgress`, or None when the log holds nothing it recognises; the projects roster's Running
column and a job page's header show it.

The tools print the same few shapes every time: tqdm bars (miss-alignment, IsoNet), keras bars
(cryoCARE), RELION's `elapsed/estimate sec ~~(,_,">` bar and its iteration lines. A per-job-type
parser keeps the few facts that say where a run is (iteration k of n, the phase, the current
bar); any other job type gets its last bar of any kind.

run.out is read incrementally: each file's offset and facts are kept between calls, so a tick
reads only what the job wrote since the last one. The first read of a file starts at most
`_FIRST_READ` bytes before its end. A file that shrank or was replaced (a resubmit truncates
run.out) is read afresh.
"""

from __future__ import annotations

import codecs
import logging
import os
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from services.jobs.spec import JOB_SPEC_BY_TYPE
from services.models_base import JobType

logger = logging.getLogger(__name__)

# Enough to hold a miss-alignment iteration header (~300 KB of bars per iteration) and an IsoNet
# epoch summary (~1 MB of bars per epoch).
_FIRST_READ = 2 * 1024 * 1024
# A run.out that has not grown for this long is called quiet (a hung tool keeps its GPUs).
_QUIET_AFTER_S = 600
# Files nobody asked about for this long are dropped from the cache.
_FORGET_AFTER_S = 3600
# An unterminated tail longer than this is not a progress line.
_MAX_CARRY = 4096


@dataclass(frozen=True)
class LogProgress:
    stage: str  # where the run is, in the tool's terms: "iteration 3/9 · training"
    short: str  # a few characters for a cramped cell: the overall percentage, else the stage's gist
    fraction: float | None  # of the whole job, when the log says how much there is in all
    eta: str | None  # the whole job's rest at its current pace, rounded: "14 h 20 min"
    quiet_min: int | None  # minutes since run.out last grew, once that is over 10
    # The last progress line as printed. Not compared: it moves on every tick, the rest rarely.
    last_line: str = field(default="", compare=False)


@dataclass(frozen=True)
class _Bar:
    kind: str  # tqdm / keras / relion
    desc: str
    done: float
    total: float | None
    elapsed_s: float | None
    remain_s: float | None
    post: str
    line: str

    @property
    def frac(self) -> float | None:
        return min(1.0, self.done / self.total) if self.total else None

    @property
    def pct(self) -> int | None:
        frac = self.frac
        return None if frac is None else int(frac * 100)

    @property
    def span_s(self) -> float | None:
        """How long the whole bar takes: elapsed + remaining, or the elapsed time of a finished one."""
        if self.elapsed_s is not None and self.remain_s is not None:
            return self.elapsed_s + self.remain_s
        if self.elapsed_s is not None and self.total and self.done >= self.total:
            return self.elapsed_s
        return None


_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]|\x08")
_SPLIT = re.compile(r"[\r\n]")
_TQDM = re.compile(
    r"(?P<desc>[^|]*?):?\s*\d{1,3}%\|[^|]*\|\s*(?P<n>\d+)/(?P<total>\d+)\s*"
    r"\[(?P<elapsed>[\d:]+)<(?P<remain>[\d:]+|\?)(?P<post>[^\]]*)\]"
)
# A running keras bar says `- ETA: 1:02`, a finished one `- 994s 5s/step`.
_KERAS = re.compile(r"^\s*(?P<n>\d+)/(?P<total>\d+) \[[=>.]*\](?: - ETA: (?P<remain>[\d:]+)| - (?P<took>\d+)s )?")
_RELION = re.compile(r"^\s*(?P<done>\d+(?:\.\d*)?)/\s*(?P<est>\d+(?:\.\d*)?|\?+) (?P<unit>sec|min|hrs) \.*~~\(,_,\">")
_UNIT_S = {"sec": 1.0, "min": 60.0, "hrs": 3600.0}


def _hms(text: str | None) -> float | None:
    """'2:21:02' / '05:53' → seconds; '?' → None."""
    if not text or not text[0].isdigit():
        return None
    s = 0.0
    for part in text.split(":"):
        s = s * 60 + float(part)
    return s


def _human(seconds: float | None) -> str | None:
    """Rounded to the minute below an hour, to ten minutes above."""
    if seconds is None:
        return None
    if seconds < 60:
        return "<1 min"
    if seconds < 3600:
        return f"{round(seconds / 60)} min"
    h, m = divmod(round(seconds / 600) * 10, 60)
    return f"{h} h {m} min" if m else f"{h} h"


def _parse_bar(line: str) -> _Bar | None:
    if "%|" in line:
        m = _TQDM.search(line)
        if m:
            return _Bar(
                "tqdm",
                m["desc"].strip(),
                float(m["n"]),
                float(m["total"]),
                _hms(m["elapsed"]),
                _hms(m["remain"]),
                m["post"],
                line,
            )
    if "~~(" in line:
        m = _RELION.match(line)
        if m:
            unit = _UNIT_S[m["unit"]]
            done = float(m["done"])
            est = float(m["est"]) if m["est"][0].isdigit() else None
            remain = (est - done) * unit if est is not None and est >= done else None
            return _Bar("relion", "", done, est or None, done * unit, remain, "", line)
    if "] - " in line or line.rstrip().endswith("]"):
        m = _KERAS.match(line)
        if m:
            took = float(m["took"]) if m["took"] else None
            return _Bar("keras", "", float(m["n"]), float(m["total"]), took, _hms(m["remain"]), "", line)
    return None


@dataclass(frozen=True)
class _Summary:
    stage: str
    short: str  # shown when there is no overall fraction
    fraction: float | None = None
    eta_s: float | None = None


Facts = dict
Feed = Callable[[str, _Bar | None, Facts], None]
Summarize = Callable[[Facts], _Summary | None]


def _rest(bar: _Bar | None, units_left: int, unit_s: float | None) -> float | None:
    """The current unit's remaining time plus `units_left` more like it; None when either is unknown."""
    if bar is None or bar.remain_s is None or unit_s is None:
        return None
    return bar.remain_s + units_left * unit_s


# ── any job: its last bar ──────────────────────────────────────────────────────


def _feed_generic(line: str, bar: _Bar | None, f: Facts) -> None:
    """Nothing beyond the last bar, which `_advance` keeps for every parser."""


def _sum_generic(f: Facts) -> _Summary | None:
    """The last bar alone. It may be one step of several, so it gives no overall fraction or estimate."""
    bar: _Bar | None = f.get("last_bar")
    if bar is None or bar.pct is None:
        return None
    count = f" ({int(bar.done)}/{int(bar.total)})" if bar.kind != "relion" else ""
    return _Summary(f"{bar.desc or 'step'} {bar.pct}%{count}", f"{bar.pct}%")


# ── miss-alignment: Iteration k/n headers, a training bar, then an alignment bar ─


_MA_ITER = re.compile(r"^Iteration (\d+)/(\d+) - Alignment: (.+?)\s*$")


def _feed_miss_align(line: str, bar: _Bar | None, f: Facts) -> None:
    if bar is not None:
        if bar.desc == "Training":
            f.update(phase="training", bar=bar)
            f["train_s"] = bar.span_s or f.get("train_s")
        elif bar.desc.startswith("Tilt series alignment"):
            f.update(phase="aligning", bar=bar)
            f["align_s"] = bar.span_s or f.get("align_s")
        return
    m = _MA_ITER.match(line)
    if m:
        f.update(it=int(m[1]), n_it=int(m[2]), phase="training", bar=None)
    elif line.startswith("Best model after iteration"):
        f.update(phase="aligning", bar=None)
    elif line.startswith("[DRIVER] Staging"):
        f.update(phase="staging", bar=None)
    elif line.startswith(("[DRIVER] changes", "[DRIVER] Refined")):
        f.update(phase="writing", bar=None)


def _sum_miss_align(f: Facts) -> _Summary | None:
    phase = f.get("phase")
    it, n = f.get("it"), f.get("n_it")
    bar: _Bar | None = f.get("bar")
    if phase == "staging":
        return _Summary("staging the tilt-series", "staging")
    if phase == "writing":
        return _Summary("writing the refined tilt-series", "writing", 1.0)
    if it is None or n is None:
        return _sum_generic(f)
    # Later iterations are taken to train and align as long as the last ones did; early stopping
    # ends training sooner, so the estimate errs long.
    train_s, align_s = f.get("train_s"), f.get("align_s") or 0.0
    per_iter = train_s + align_s if train_s is not None else None
    if phase == "aligning":
        return _Summary(f"iteration {it}/{n} · aligning", f"{it}/{n}", it / n, _rest(bar, n - it, per_iter))
    frac = (bar.frac or 0.0) if bar else 0.0
    eta = _rest(bar, n - it, per_iter)
    if eta is not None:
        eta += align_s
    return _Summary(f"iteration {it}/{n} · training", f"{it}/{n}", (it - 1 + frac) / n, eta)


# ── RELION Refine 3D / Class 3D: Expectation iteration k [of n], E-step bar ──────


_RL_ITER = re.compile(r"^\s*Expectation iteration (\d+)(?: of (\d+))?")
_RL_RES = re.compile(r"(?:CurrentResolution|Auto-refine: Resolution)= ?(\d+(?:\.\d+)?)")


def _feed_relion(line: str, bar: _Bar | None, f: Facts) -> None:
    if bar is not None:
        f["bar"] = bar
        if f.get("phase") == "E-step":
            f["iter_s"] = bar.span_s or f.get("iter_s")
        return
    m = _RL_ITER.match(line)
    if m:
        f.update(it=int(m[1]), phase="E-step", bar=None)
        if m[2]:
            f["n_it"] = int(m[2])
        return
    m = _RL_RES.search(line)
    if m:
        f["res"] = float(m[1])
    elif "Maximization" in line:
        f.update(phase="M-step", bar=None)
    elif "Estimating accuracies" in line:
        f.update(phase="estimating accuracies", bar=None)
    elif "has converged, entering last iteration" in line:
        f["final"] = True


def _sum_relion(f: Facts) -> _Summary | None:
    """Class 3D says how many iterations it runs; Refine 3D runs until it converges, so it gets
    no overall fraction or estimate."""
    it = f.get("it")
    if it is None:
        return _sum_generic(f)
    n = f.get("n_it")
    res = f" · {f['res']:.1f} Å" if f.get("res") else ""
    if not n:
        final = " (final)" if f.get("final") else ""
        return _Summary(f"iteration {it}{final}{res}", f"it {it}")
    bar: _Bar | None = f.get("bar") if f.get("phase") == "E-step" else None
    frac = (bar.frac or 0.0) if bar else 0.0
    iter_s = f.get("iter_s")
    eta = _rest(bar, n - it, iter_s) if bar else (iter_s * (n - it) if iter_s is not None else None)
    return _Summary(f"iteration {it}/{n}{res}", f"{it}/{n}", (it - 1 + frac) / n, eta)


# ── relion_tomo_reconstruct_particle: + Tomogram k / n ────────────────────────


_RP_TOMO = re.compile(r"^\s*\+ Tomogram (\d+) / (\d+)")


def _feed_reconstruct(line: str, bar: _Bar | None, f: Facts) -> None:
    if bar is not None:
        f["bar"] = bar
        f["tomo_s"] = bar.span_s or f.get("tomo_s")
        return
    m = _RP_TOMO.match(line)
    if m:
        f.update(tomo=int(m[1]), n_tomo=int(m[2]), phase="backprojecting", bar=None)
    elif line.strip().startswith("+ Reconstructing"):
        f.update(phase="reconstructing", bar=None)


def _sum_reconstruct(f: Facts) -> _Summary | None:
    if f.get("phase") == "reconstructing":
        return _Summary("reconstructing the map", "recon", 1.0)
    k, n = f.get("tomo"), f.get("n_tomo")
    if k is None:
        return _sum_generic(f)
    bar: _Bar | None = f.get("bar")
    frac = (bar.frac or 0.0) if bar else 0.0
    return _Summary(f"tomogram {k}/{n}", f"{k}/{n}", (k - 1 + frac) / n, _rest(bar, n - k, f.get("tomo_s")))


# ── denoise train: IsoNet `Epoch k:` tqdm + `Epoch [ k/ n]`, cryoCARE keras `Epoch k/n` ─


_DN_ISONET_N = re.compile(r"^\[ISONET\] (\d+) epochs")
_DN_ISONET_DONE = re.compile(r"^Epoch \[\s*(\d+)/\s*(\d+)\]")
_DN_KERAS_EPOCH = re.compile(r"^Epoch (\d+)/(\d+)\s*$")
_DN_TQDM_EPOCH = re.compile(r"^Epoch (\d+)$")


def _feed_denoise(line: str, bar: _Bar | None, f: Facts) -> None:
    if bar is not None:
        m = _DN_TQDM_EPOCH.match(bar.desc)
        if m:
            f.update(epoch=int(m[1]), stage="training", bar=bar)
        elif bar.kind == "keras" and f.get("stage") == "training":
            f["bar"] = bar
        else:
            f.update(stage=bar.desc or "working", bar=bar)
            return
        f["epoch_s"] = bar.span_s or f.get("epoch_s")
        return
    m = _DN_KERAS_EPOCH.match(line)
    if m:
        f.update(epoch=int(m[1]), n_epochs=int(m[2]), stage="training", bar=None)
        return
    m = _DN_ISONET_DONE.match(line)
    if m:
        f["n_epochs"] = int(m[2])
        return
    m = _DN_ISONET_N.match(line)
    if m:
        f["n_epochs"] = int(m[1])


def _sum_denoise(f: Facts) -> _Summary | None:
    bar: _Bar | None = f.get("bar")
    ep, n = f.get("epoch"), f.get("n_epochs")
    if f.get("stage") != "training" or ep is None:
        return _sum_generic(f)
    if not n:
        return _Summary(f"training · epoch {ep}", f"ep {ep}")
    frac = (bar.frac or 0.0) if bar else 0.0
    return _Summary(
        f"training · epoch {ep}/{n}", f"ep {ep}/{n}", (ep - 1 + frac) / n, _rest(bar, n - ep, f.get("epoch_s"))
    )


# Keyed by JobSpec.log_format.
_PARSERS: dict[str, tuple[Feed, Summarize]] = {
    "miss_align": (_feed_miss_align, _sum_miss_align),
    "relion": (_feed_relion, _sum_relion),
    "reconstruct": (_feed_reconstruct, _sum_reconstruct),
    "denoise": (_feed_denoise, _sum_denoise),
}


# ── the incremental reader ─────────────────────────────────────────────────────


@dataclass
class _Tail:
    job_type: str
    ino: int
    offset: int = 0
    carry: str = ""
    facts: Facts = field(default_factory=dict)
    decoder: codecs.IncrementalDecoder = field(default_factory=lambda: codecs.getincrementaldecoder("utf-8")("replace"))
    seen: float = 0.0


_tails: dict[str, _Tail] = {}
_failed: set[str] = set()
_lock = threading.Lock()


def _advance(tail: _Tail, path: Path, size: int, feed: Feed) -> None:
    start = tail.offset
    skip_first = False
    if start == 0 and size > _FIRST_READ:
        start, skip_first = size - _FIRST_READ, True
    with open(path, "rb") as fh:
        fh.seek(start)
        data = fh.read(size - start)
    tail.offset = start + len(data)
    segments = _SPLIT.split(tail.carry + tail.decoder.decode(data))
    if skip_first:
        segments = segments[1:]
    if not segments:
        return
    # The unterminated last segment is usually the bar the tool is redrawing: fed now, and again
    # with whatever completes it next time (every fact is last-wins, so a second feed is harmless).
    tail.carry = segments[-1][-_MAX_CARRY:]
    for seg in segments:
        line = _ANSI.sub("", seg).rstrip()
        if not line:
            continue
        bar = _parse_bar(line)
        if bar is not None:
            tail.facts["last_bar"] = bar
        feed(line, bar, tail.facts)


def read(job_type: JobType | str, job_dir: Path | str) -> LogProgress | None:
    """How far the job in `job_dir` has got, from its run.out. None without a run.out or without
    anything recognisable in it. Disk; for a thread."""
    jt = getattr(job_type, "value", job_type)
    path = Path(job_dir) / "run.out"
    key = str(path)
    try:
        st = os.stat(path)
    except FileNotFoundError:  # not started yet, or a job that writes no run.out
        return None
    try:
        log_format = JOB_SPEC_BY_TYPE[JobType(jt)].log_format
    except (ValueError, KeyError):  # not a job type with a spec row: the generic reader
        log_format = None
    feed, summarize = _PARSERS.get(log_format, (_feed_generic, _sum_generic))
    now = time.monotonic()
    with _lock:
        for k in [k for k, t in _tails.items() if now - t.seen > _FORGET_AFTER_S]:
            del _tails[k]
        tail = _tails.get(key)
        if tail is None or tail.ino != st.st_ino or st.st_size < tail.offset or tail.job_type != jt:
            tail = _tails[key] = _Tail(jt, st.st_ino)
        tail.seen = now
        try:
            if st.st_size > tail.offset:
                _advance(tail, path, st.st_size, feed)
            s = summarize(tail.facts)
        except OSError as e:  # the file went away or NFS dropped mid-read: no progress this tick
            logger.debug("log progress: %s unreadable: %s", path, e)
            return None
        except Exception:
            # A parser bug must not take the caller down with it; said once per file.
            del _tails[key]
            if key not in _failed:
                _failed.add(key)
                logger.exception("log progress: parsing %s failed", path)
            return None
        if s is None:
            return None
        last = tail.facts.get("last_bar")
    age = time.time() - st.st_mtime
    return LogProgress(
        stage=s.stage,
        short=f"{int(s.fraction * 100)}%" if s.fraction is not None else s.short,
        fraction=s.fraction,
        eta=_human(s.eta_s),
        quiet_min=int(age // 60) if age > _QUIET_AFTER_S else None,
        last_line=last.line.strip() if last is not None else "",
    )

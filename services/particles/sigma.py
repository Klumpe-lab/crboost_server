"""Template-matching scores in σ units.

PyTOM correlates the template with the tomogram at every voxel and every rotation (local
cross-correlation, LCC) and records the spread of those raw values, σ, in each tomogram's
``job.json`` (``job_stats.std``) and in every candidate (``rlnSearchStd``). Raw LCC is not
comparable between tomograms or templates; LCC ÷ σ is, so σ units are the score the app shows.

With N = voxels × rotations correlations (``job_stats.search_space``) of pure Gaussian noise,
about N·Q(z) of them beat z·σ, where Q is the Gaussian upper tail. PyTOM's "false positives = FP"
cutoff solves N·Q(z) = FP: z = erfcinv(2·FP/N)·√2. Real background (membranes, edges, granules,
gold) is not Gaussian noise, so that cutoff is a gate, not an error rate; `expected_noise_count`
is the null curve the observed counts are drawn against.

Standard library only: the drivers import this on compute nodes.
"""

from __future__ import annotations

import math
from collections.abc import Iterable

# The fixed σ levels of the per-tomogram ladder (counts at or above each).
LADDER_SIGMAS: tuple[float, ...] = (7.0, 8.0, 9.0, 10.0, 12.0)


def gaussian_tail(z: float) -> float:
    """Q(z): the probability that one standard-normal value exceeds z."""
    return 0.5 * math.erfc(z / math.sqrt(2.0))


def erfcinv(y: float) -> float:
    """Inverse of math.erfc on (0, 2), by bisection. Accurate for the tiny arguments a
    search space of ~1e12 correlations produces, where series expansions lose precision."""
    if not 0.0 < y < 2.0:
        raise ValueError(f"erfcinv is defined on (0, 2), got {y!r}")
    lo, hi = -10.0, 10.0
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        if math.erfc(mid) > y:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def fp_cutoff_sigma(search_space: float, false_positives: float = 1.0) -> float:
    """The score, in σ, that `false_positives` of `search_space` Gaussian-noise correlations
    would beat: PyTOM's extraction cutoff divided by σ."""
    if search_space <= 0 or false_positives <= 0:
        raise ValueError(f"need a positive search space and FP count, got {search_space!r}, {false_positives!r}")
    return erfcinv(2.0 * false_positives / search_space) * math.sqrt(2.0)


def expected_noise_count(z: float, search_spaces: Iterable[float]) -> float:
    """Σ N·Q(z) over tomograms: how many Gaussian-noise correlations would beat z·σ."""
    tail = gaussian_tail(z)
    return sum(float(n) * tail for n in search_spaces)


def score_sigma(lcc: float, sigma: float) -> float | None:
    """LCC ÷ σ, or None when σ is not a positive number."""
    if sigma is None or not sigma > 0:
        return None
    return float(lcc) / float(sigma)

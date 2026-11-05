"""Roadmap-43 S1: pytom_extract_candidates with a faster peak search, for the benchmark only.

pytom 0.14's extraction loop calls np.nanargmax on the transposed (Fortran-ordered) score map once per
candidate, which copies the whole volume twice each time: ~7 s per candidate on a 1024 x 1024 x 512 map
in the S1 comparison job, ~2 h per copia case. This runs pytom's own extraction (its cut-off, edge mask,
spherical exclusion and star output) on C-ordered copies of the maps with NaNs set to -inf, where
np.argmax finds the same peak as np.nanargmax.

Usage, inside the pytom image: python extract_fast.py <pytom_extract_candidates.py arguments>
"""

import sys

import numpy as np
import pytom_tm.extract as extract
from pytom_tm.entry_points import extract_candidates

_read_mrc = extract.read_mrc


def _read_c_ordered(*args, **kwargs):
    vol = np.ascontiguousarray(_read_mrc(*args, **kwargs))
    if np.issubdtype(vol.dtype, np.floating):
        vol[np.isnan(vol)] = -np.inf
    return vol


extract.read_mrc = _read_c_ordered
# No NaN is left in any map this process reads, so the plain argmax picks the same voxel.
np.nanargmax = np.argmax

if __name__ == "__main__":
    sys.argv[0] = "pytom_extract_candidates.py"
    sys.exit(extract_candidates())

"""
Deterministic three-word project nicknames.

Each project gets something like "amber-vagrant-fermi" — handy when the
operational folder name is a long hash/date and the user wants something
to actually latch onto. Same seed always yields the same nickname so a
project's mnemonic survives renames and reloads.

Mnemonic is stored on `ProjectState.mnemonic` (persisted in
`project_params.json`). For legacy projects without one, the dashboard /
project list derives one from the project path at display time, so the
user sees something stable without needing to re-save.

64 × 64 × 88 ≈ 360,000 combinations.
"""

from __future__ import annotations

import hashlib

_ADJ_1 = (
    "amber", "azure", "boreal", "bright", "brisk", "cobalt", "copper", "crimson",
    "crystal", "deft", "drifting", "dusty", "elated", "emerald", "endless", "fading",
    "faithful", "feral", "frosted", "fuzzy", "gentle", "gilded", "glossy", "golden",
    "graceful", "hazy", "icy", "indigo", "ivory", "jade", "jaunty", "keen",
    "lithe", "lively", "lonely", "lucid", "lunar", "lush", "magnetic", "marble",
    "merry", "misty", "mossy", "muted", "noble", "obsidian", "olden", "opal",
    "patient", "placid", "plucky", "polar", "quiet", "radiant", "raven", "rolling",
    "rosy", "rugged", "sable", "salty", "serene", "silent", "silver", "stormy",
)

_ADJ_2 = (
    "amiable", "ancient", "boisterous", "bold", "brave", "candid", "clever", "courteous",
    "curious", "daring", "dashing", "decisive", "dignified", "diligent", "earnest", "eager",
    "elegant", "energetic", "ethereal", "fanciful", "fierce", "gallant", "generous", "genial",
    "gleaming", "glowing", "graceful", "grand", "happy", "hardy", "honest", "humble",
    "industrious", "intrepid", "jolly", "joyful", "kind", "languid", "loyal", "magnificent",
    "majestic", "mellow", "mighty", "modest", "nimble", "noble", "obliging", "patient",
    "peaceful", "peculiar", "pensive", "plucky", "proud", "punctual", "quaint", "quiet",
    "regal", "resolute", "restless", "robust", "rumbling", "shrewd", "spirited", "valiant",
)

_NOUNS = (
    "bohr", "boyle", "brahe", "cassini", "chadwick", "copernicus", "curie", "dalton",
    "darwin", "dirac", "einstein", "euclid", "euler", "fermi", "feynman", "fourier",
    "franklin", "galileo", "gauss", "halley", "heisenberg", "herschel", "hooke", "hubble",
    "huygens", "kepler", "lavoisier", "leibniz", "linnaeus", "lorenz", "mach", "maxwell",
    "mendel", "millikan", "moseley", "newton", "oppenheimer", "pasteur", "pauli", "penrose",
    "planck", "rontgen", "rutherford", "sagan", "salk", "schroedinger", "tesla", "thomson",
    "tycho", "vesalius", "volta", "watson", "wegener", "wheeler", "wright", "yukawa",
    "atom", "aurora", "beacon", "cipher", "compass", "comet", "ember", "harbor",
    "helix", "horizon", "kernel", "lattice", "lens", "meadow", "monolith", "nebula",
    "node", "orbit", "pendulum", "prism", "quartz", "rune", "sequoia", "spire",
    "summit", "tundra", "valley", "vector", "vortex", "willow", "zephyr",
)


def nickname_for(seed: str) -> str:
    """Pick three words deterministically from `seed`. Stable across calls
    and across processes."""
    h = hashlib.sha256(seed.encode("utf-8")).digest()
    a = _ADJ_1[h[0] % len(_ADJ_1)]
    b = _ADJ_2[h[1] % len(_ADJ_2)]
    c = _NOUNS[h[2] % len(_NOUNS)]
    return f"{a}-{b}-{c}"

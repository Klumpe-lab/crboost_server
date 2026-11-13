"""The look stays variables (roadmap 51 §6): no new hex colour, `font-family` or `text-transform` literal in `ui/`.

A look is one block of `--cb-*` values in `ui/dashboard/css.py`; a component names a voice or a state and reads them.
Literals that predate the rule are counted per file in `check_look_literals.json`, the allow-list that shrinks as
files convert. A file over its count, or a new file with any, fails. A literal inside a `var(--cb-…, <fallback>)` is
the variable's fallback and is not counted.

    ./crboost python check_look_literals.py            # check
    ./crboost python check_look_literals.py --update   # write today's counts (after converting a file)
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ALLOW = ROOT / "check_look_literals.json"
# The theme block and the voice helpers are where the values live.
EXEMPT = {"ui/dashboard/css.py", "ui/job_plugins/_field_styles.py", "ui/branch_map/style.py"}
_FALLBACK = re.compile(r"var\(--cb-[\w-]+,[^()]*(?:\([^()]*\)[^()]*)*\)")
_LITERAL = re.compile(r"#[0-9a-fA-F]{3,8}\b|font-family\s*:|text-transform\s*:")


def counts() -> dict[str, int]:
    out = {}
    for path in sorted((ROOT / "ui").rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if rel in EXEMPT:
            continue
        n = len(_LITERAL.findall(_FALLBACK.sub("", path.read_text(encoding="utf-8"))))
        if n:
            out[rel] = n
    return out


def main() -> int:
    now = counts()
    if "--update" in sys.argv:
        ALLOW.write_text(json.dumps(now, indent=1, sort_keys=True) + "\n")
        print(f"{len(now)} files, {sum(now.values())} literals written to {ALLOW.name}")
        return 0
    allowed = json.loads(ALLOW.read_text()) if ALLOW.exists() else {}
    over = [(f, n, allowed.get(f, 0)) for f, n in now.items() if n > allowed.get(f, 0)]
    for f, n, a in over:
        print(f"{f}: {n} look literals, {a} allowed — name a voice or a --cb-* variable instead")
    if over:
        return 1
    print(f"Look literals within the allow-list ({sum(now.values())} left in {len(now)} files).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

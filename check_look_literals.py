"""The look stays variables (roadmap 51 §6): no new hex colour, `font-family` or `text-transform` literal in `ui/`.

A look is one block of `--cb-*` values in `ui/dashboard/theme.py`; a component names a voice or a state and reads
them. Literals that predate the rule are counted per file in `check_look_literals.json`, the allow-list that shrinks as
files convert. A file over its count, or a new file with any, fails. A literal inside a `var(--cb-…, <fallback>)` is
the variable's fallback and is not counted, and a `font-family` or `text-transform` whose value is a `--cb-*`
variable is not a literal.

The palette (`PALETTE` in ui/dashboard/theme.py) keeps the light look exact: every palette variable a file in `ui/`
names must be in it, and a fallback beside one must be the colour the palette gives it, or the light theme would
show something else than the code says.

    ./crboost python check_look_literals.py            # check
    ./crboost python check_look_literals.py --update   # write today's counts (after converting a file)
"""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ALLOW = ROOT / "check_look_literals.json"
# The theme block and the voice helpers are where the values live.
EXEMPT = {"ui/dashboard/css.py", "ui/dashboard/theme.py", "ui/job_plugins/_field_styles.py", "ui/branch_map/style.py"}
THEME = ROOT / "ui" / "dashboard" / "theme.py"
_PALETTE_USE = re.compile(
    r"var\(--cb-((?:white|black|(?:slate|gray|zinc|neutral|stone|red|orange|amber|yellow|lime|green|emerald|teal|"
    r"cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose)-\d+|x-[0-9a-f]{6,8}))\s*(?:,\s*([^)]*))?\)"
)
_FALLBACK = re.compile(r"var\(--cb-[\w-]+,[^()]*(?:\([^()]*\)[^()]*)*\)")
_LITERAL = re.compile(r"#[0-9a-fA-F]{3,8}\b|(?:font-family|text-transform)\s*:(?!\s*var\(--cb-)")


def counts() -> dict[str, int]:
    out = {}
    for path in sorted((ROOT / "ui").rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if rel in EXEMPT:
            continue
        n = len(_LITERAL.findall(_FALLBACK.sub("var(--cb-)", path.read_text(encoding="utf-8"))))
        if n:
            out[rel] = n
    return out


def palette() -> dict[str, str]:
    for node in ast.parse(THEME.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", "") == "PALETTE":
            return ast.literal_eval(node.value)
    raise SystemExit(f"no PALETTE in {THEME}")


def _hex6(colour: str) -> str:
    h = colour.strip().lstrip("#").lower()
    return "#" + ("".join(c * 2 for c in h) if len(h) == 3 else h)


def palette_problems() -> list[str]:
    known = palette()
    out = []
    for path in sorted((ROOT / "ui").rglob("*")):
        if path.suffix not in (".py", ".js") or path == THEME:
            continue
        rel = path.relative_to(ROOT).as_posix()
        for m in _PALETTE_USE.finditer(path.read_text(encoding="utf-8")):
            name, fallback = m.group(1), m.group(2)
            if name not in known:
                out.append(f"{rel}: --cb-{name} is not in PALETTE")
            elif fallback is not None and fallback.strip() != "white" and _hex6(fallback) != known[name]:
                out.append(f"{rel}: --cb-{name} falls back to {fallback.strip()}, PALETTE gives {known[name]}")
    return out


def main() -> int:
    bad = palette_problems()
    for line in bad:
        print(line)
    now = counts()
    if "--update" in sys.argv:
        ALLOW.write_text(json.dumps(now, indent=1, sort_keys=True) + "\n")
        print(f"{len(now)} files, {sum(now.values())} literals written to {ALLOW.name}")
        return 0
    allowed = json.loads(ALLOW.read_text()) if ALLOW.exists() else {}
    over = [(f, n, allowed.get(f, 0)) for f, n in now.items() if n > allowed.get(f, 0)]
    for f, n, a in over:
        print(f"{f}: {n} look literals, {a} allowed — name a voice or a --cb-* variable instead")
    if over or bad:
        return 1
    print(f"Look literals within the allow-list ({sum(now.values())} left in {len(now)} files).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Raw HTML is constant or escaped (roadmap 63 S3): `ui.html(content, sanitize=False)` skips NiceGUI's sanitizer, and
projects are shared group-writable, so a name or a note in one is data another member's browser renders.

A call's content passes when it is a string constant (a literal, a module-level name bound to one, or an f-string,
concatenation or conditional made of such), escaped (`html.escape(...)`), or a number (`int()`, `float()`, `len()`,
`round()`, a numeric format spec). Everything else, a helper's return value included, is counted per file in
`check_raw_html.json`, the allow-list that shrinks as calls are made constant or escaped. A file over its count, or a
new file with any, fails.

    ./crboost python check_raw_html.py            # check
    ./crboost python check_raw_html.py --list     # every counted call: file:line and its content
    ./crboost python check_raw_html.py --update   # write today's counts (after making a call pass)
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ALLOW = ROOT / "check_raw_html.json"
_ESCAPES = {"escape", "html.escape", "_html.escape"}
_NUMBERS = {"int", "float", "len", "round", "abs"}
_NUMERIC_SPEC = set("bcdeEfFgGnoxX%")


def _module_constants(tree: ast.Module) -> set[str]:
    """Names a module binds once, at its top level, to a string constant (in order, so one may build on another)."""
    bound: dict[str, int] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        for t in targets:
            if isinstance(t, ast.Name):
                bound[t.id] = bound.get(t.id, 0) + 1
    constants: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            target, value = node.target, node.value
        else:
            continue
        if isinstance(target, ast.Name) and bound[target.id] == 1 and _passes(value, constants):
            constants.add(target.id)
    return constants


def _numeric_spec(spec: ast.expr | None) -> bool:
    if not isinstance(spec, ast.JoinedStr):
        return False
    text = "".join(v.value for v in spec.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
    return bool(text) and text[-1] in _NUMERIC_SPEC


def _passes(node: ast.expr, constants: set[str]) -> bool:
    if isinstance(node, ast.Constant):
        return isinstance(node.value, (str, int, float))
    if isinstance(node, ast.Name):
        return node.id in constants
    if isinstance(node, ast.JoinedStr):
        # Its values are the literal pieces (Constant) and the interpolations (FormattedValue).
        return all(
            isinstance(v, ast.Constant) or _numeric_spec(v.format_spec) or _passes(v.value, constants)
            for v in node.values
        )
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _passes(node.left, constants) and _passes(node.right, constants)
    if isinstance(node, ast.IfExp):
        return _passes(node.body, constants) and _passes(node.orelse, constants)
    if isinstance(node, ast.Call):
        name = ast.unparse(node.func)
        return name in _ESCAPES or name in _NUMBERS
    return False


def _is_raw_html(call: ast.Call) -> bool:
    if ast.unparse(call.func) != "ui.html":
        return False
    return any(
        k.arg == "sanitize" and isinstance(k.value, ast.Constant) and k.value.value is False for k in call.keywords
    )


def findings() -> dict[str, list[tuple[int, str]]]:
    out: dict[str, list[tuple[int, str]]] = {}
    for path in sorted((ROOT / "ui").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
        constants = _module_constants(tree)
        rel = path.relative_to(ROOT).as_posix()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not _is_raw_html(node):
                continue
            content = node.args[0] if node.args else next((k.value for k in node.keywords if k.arg == "content"), None)
            if content is None or not _passes(content, constants):
                text = ast.unparse(content) if content is not None else "(none)"
                out.setdefault(rel, []).append((node.lineno, text))
    return out


def main() -> int:
    found = findings()
    now = {f: len(calls) for f, calls in found.items()}
    if "--list" in sys.argv:
        for f, calls in found.items():
            for line, text in calls:
                print(f"{f}:{line}: {text if len(text) <= 100 else text[:97] + '...'}")
        return 0
    if "--update" in sys.argv:
        ALLOW.write_text(json.dumps(now, indent=1, sort_keys=True) + "\n")
        print(f"{len(now)} files, {sum(now.values())} calls written to {ALLOW.name}")
        return 0
    allowed = json.loads(ALLOW.read_text()) if ALLOW.exists() else {}
    over = [(f, n, allowed.get(f, 0)) for f, n in now.items() if n > allowed.get(f, 0)]
    for f, n, a in over:
        lines = ", ".join(str(line) for line, _ in found[f])
        print(f"{f}: {n} ui.html(sanitize=False) calls neither constant nor escaped, {a} allowed (lines {lines})")
    if over:
        return 1
    n = sum(now.values())
    print(f"Raw HTML within the allow-list ({n} calls neither constant nor escaped, in {len(now)} files).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

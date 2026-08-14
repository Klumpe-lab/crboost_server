#!/usr/bin/env python3
"""Architecture-boundary linter (roadmap 01, stage 7).

Run alongside ruff:  python check_boundaries.py   (exit 1 on any violation)

AST-based on purpose: function-local imports dodge grep-style review (51 of the
boundary violations the 2026-08-10 audit found were function-local), and kwargs
hide on continuation lines. Rules are declarative lists below — grow them as
shims are deleted or new boundaries land.

Rules:
  R1  services/, drivers/, backend.py must not import ui.* (services are
      headless; drivers run on compute nodes; the facade serves the UI,
      never the reverse).
  R2  ui/ must not call StateService.save_project directly — persistence goes
      through backend.save_project (owns force/debounce policy). Detected as
      any `<receiver>.save_project(...)` whose receiver mentions
      get_state_service / state_service.
  R3  Stage-3 re-export shims are frozen: no NEW imports of them (existing
      code was repointed; delete the shims after one release).
  R4  No new module-level dict literals keyed by 3+ JobType members outside
      services/jobs/spec.py — that's a per-concern job table growing back.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).parent

NO_UI_TREES = ("services", "drivers", "backend.py")

# Pure re-export shims (module -> where to import from instead).
BANNED_SHIM_MODULES = {
    "ui.components.task_utils": "services.array_tasks",
    "ui.dashboard.data": "services.dashboard_data",
}
# Shimmed names inside modules that also hold live code ((module, name) -> replacement).
BANNED_SHIM_NAMES = {
    ("ui.dashboard.pixel_sanity", "_apply_sanity_rules"): "services.pixel_chain.apply_sanity_rules",
    ("ui.dashboard.pixel_sanity", "_compute_pixel_chain"): "services.pixel_chain.compute_pixel_chain",
    ("ui.dashboard.pixel_sanity", "_render_pixel_sanity_table"): "ui.dashboard.pixel_sanity.render_pixel_sanity_table",
}

JOBTYPE_TABLE_KEY_THRESHOLD = 3
SPEC_FILE = ROOT / "services" / "jobs" / "spec.py"


def iter_py(*tops: str):
    for top in tops:
        p = ROOT / top
        if p.is_file():
            yield p
        elif p.is_dir():
            yield from sorted(p.rglob("*.py"))


def parse(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as e:  # ruff reports these better; don't duplicate
        print(f"  (skipping unparsable {path}: {e})")
        return None


def imported_modules(node: ast.AST):
    """Yield (lineno, module, name) for every import, however deeply nested."""
    for n in ast.walk(node):
        if isinstance(n, ast.Import):
            for a in n.names:
                yield n.lineno, a.name, None
        elif isinstance(n, ast.ImportFrom) and n.level == 0 and n.module:
            for a in n.names:
                yield n.lineno, n.module, a.name


def check_no_ui_imports() -> list[str]:
    out = []
    for path in iter_py(*NO_UI_TREES):
        tree = parse(path)
        if tree is None:
            continue
        for lineno, mod, _name in imported_modules(tree):
            if mod == "ui" or mod.startswith("ui."):
                out.append(
                    f"{path.relative_to(ROOT)}:{lineno}: R1 imports {mod} — services/drivers/facade must stay UI-free"
                )
    return out


def check_ui_save_project() -> list[str]:
    out = []
    for path in iter_py("ui"):
        tree = parse(path)
        if tree is None:
            continue
        for n in ast.walk(tree):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "save_project":
                receiver = ast.unparse(n.func.value)
                if "state_service" in receiver:
                    out.append(
                        f"{path.relative_to(ROOT)}:{n.lineno}: R2 {receiver}.save_project(...) — "
                        f"use backend.save_project (owns force/debounce policy)"
                    )
    return out


def check_shim_imports() -> list[str]:
    out = []
    shim_files = {ROOT / Path(*m.split(".")).with_suffix(".py") for m in BANNED_SHIM_MODULES}
    for path in iter_py("ui", "services", "drivers", "backend.py", "main.py"):
        if path in shim_files:
            continue
        tree = parse(path)
        if tree is None:
            continue
        for lineno, mod, name in imported_modules(tree):
            rel = f"{path.relative_to(ROOT)}:{lineno}"
            if mod in BANNED_SHIM_MODULES:
                out.append(f"{rel}: R3 imports shim {mod} — import {BANNED_SHIM_MODULES[mod]} instead")
            elif name and (mod, name) in BANNED_SHIM_NAMES:
                out.append(f"{rel}: R3 imports shimmed {mod}.{name} — use {BANNED_SHIM_NAMES[(mod, name)]}")
    return out


def check_jobtype_tables() -> list[str]:
    out = []
    for path in iter_py("ui", "services", "drivers", "backend.py"):
        if path == SPEC_FILE:
            continue
        tree = parse(path)
        if tree is None:
            continue
        for stmt in tree.body:  # module level only — that's where tables live
            value = getattr(stmt, "value", None)
            if not isinstance(value, ast.Dict):
                continue
            jobtype_keys = sum(1 for k in value.keys if k is not None and ast.unparse(k).startswith("JobType."))
            if jobtype_keys >= JOBTYPE_TABLE_KEY_THRESHOLD:
                out.append(
                    f"{path.relative_to(ROOT)}:{stmt.lineno}: R4 module-level dict keyed by {jobtype_keys} JobType "
                    f"members — extend the JobSpec table in services/jobs/spec.py instead"
                )
    return out


def main() -> int:
    violations = check_no_ui_imports() + check_ui_save_project() + check_shim_imports() + check_jobtype_tables()
    for v in violations:
        print(v)
    if violations:
        print(f"\n{len(violations)} boundary violation(s).")
        return 1
    print(
        "Boundaries clean (R1 ui-free services/drivers/facade, R2 facade-only saves, "
        "R3 no shim imports, R4 no stray JobType tables)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

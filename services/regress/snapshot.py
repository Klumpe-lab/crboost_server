"""Tier A — materialized-scheme snapshots, no cluster (roadmap 14 S7).

`materialize()` derives the RELION scheme of an applied project (the same writer the
deploy path and `crboost_protocol.py scheme` use): one `job.star` per stage with every
resolved input/output path plus `scheme.star`. Normalized (project / server / home paths,
starfile timestamps) it is a byte-stable fingerprint of what the pipeline WOULD submit —
the apix mis-scale, the TomoHand flip and the filtered-star-not-consumed bugs all show up
here as a diff, in seconds. Golden copies live in `<bundle>/test/snapshots/`."""

from __future__ import annotations

import difflib
import re
import shutil
from pathlib import Path

from services.protocols.schema import Protocol, TEST_DIRNAME
from services.protocols.scheme_export import SERVER_DIR, export_relion_scheme

SNAPSHOT_SCHEME = "_snapshot"
SNAPSHOTS_DIRNAME = "snapshots"
_STARFILE_STAMP = re.compile(r"^# Created by the starfile Python package.*$", re.MULTILINE)


def materialize(protocol: Protocol, project_dir: Path) -> dict[str, str]:
    """{relative star path: normalized text} for the protocol's stages in `project_dir`."""
    res = export_relion_scheme(protocol, project_dir=project_dir, scheme_name=SNAPSHOT_SCHEME)
    if not res["success"]:
        raise RuntimeError(res["error"])
    scheme_dir = Path(res["scheme_dir"])
    files = {
        str(p.relative_to(scheme_dir)): normalize(p.read_text(), Path(project_dir))
        for p in sorted(scheme_dir.rglob("*.star"))
    }
    shutil.rmtree(scheme_dir)
    return files


def normalize(text: str, project_dir: Path) -> str:
    text = _STARFILE_STAMP.sub("# Created by the starfile Python package", text)
    for raw, token in (
        (str(Path(project_dir).resolve()), "$PROJECT"),
        (str(SERVER_DIR), "$SERVER"),
        (str(Path.home()), "$HOME"),
    ):
        text = text.replace(raw, token)
    return text.replace(f"Schemes/{SNAPSHOT_SCHEME}/", "Schemes/$SCHEME/")


def snapshots_dir(protocol: Protocol) -> Path:
    if protocol.bundle_dir is None:
        raise ValueError(f"protocol '{protocol.name}' is not bound to a bundle directory")
    return protocol.bundle_dir / TEST_DIRNAME / SNAPSHOTS_DIRNAME


def write_snapshot(protocol: Protocol, files: dict[str, str]) -> Path:
    root = snapshots_dir(protocol)
    if root.exists():
        shutil.rmtree(root)
    for rel, text in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    return root


def diff_snapshot(protocol: Protocol, files: dict[str, str]) -> list[str]:
    """Unified diffs (golden vs current) plus missing/extra files; [] when identical."""
    root = snapshots_dir(protocol)
    if not root.is_dir():
        return [f"no golden snapshot at {root} — run with --update to record one"]
    golden = {str(p.relative_to(root)): p.read_text() for p in sorted(root.rglob("*.star"))}
    out: list[str] = []
    for rel in sorted(set(golden) | set(files)):
        if rel not in golden:
            out.append(f"+ {rel}: new file (not in the golden snapshot)")
            continue
        if rel not in files:
            out.append(f"- {rel}: missing from the current materialization")
            continue
        if golden[rel] != files[rel]:
            diff = difflib.unified_diff(
                golden[rel].splitlines(),
                files[rel].splitlines(),
                fromfile=f"golden/{rel}",
                tofile=f"current/{rel}",
                lineterm="",
                n=2,
            )
            out.append("\n".join(diff))
    return out

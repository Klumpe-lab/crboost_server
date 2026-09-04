"""Where protocols live (FEATURE_recipes.md §7): the repo's `config/protocols/` (shared,
checked in) and the user's `~/.crboost/protocols/` (personal) — the same two-layer shape
as conf.yaml. A name resolves to the first bundle directory carrying it; a path resolves as
itself. A bundle that fails to load is LISTED with its error, never hidden."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from services.protocols.schema import PROTOCOL_FILENAME, Protocol, load_protocol

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
USER_PROTOCOLS_DIR = Path.home() / ".crboost" / "protocols"


def protocol_roots() -> list[Path]:
    return [REPO_ROOT / "config" / "protocols", USER_PROTOCOLS_DIR]


@dataclass(frozen=True)
class ProtocolInfo:
    name: str
    bundle_dir: Path
    protocol: Protocol | None
    error: str | None = None  # load failure text; shown on the row instead of hiding the bundle


def list_protocols() -> list[ProtocolInfo]:
    out: list[ProtocolInfo] = []
    seen: set[str] = set()
    for root in protocol_roots():
        if not root.is_dir():
            continue
        for bundle in sorted(root.iterdir()):
            if not (bundle / PROTOCOL_FILENAME).exists() or bundle.name in seen:
                continue
            seen.add(bundle.name)
            try:
                out.append(ProtocolInfo(bundle.name, bundle, load_protocol(bundle)))
            except Exception as e:
                logger.exception("Protocol bundle %s failed to load", bundle)
                out.append(ProtocolInfo(bundle.name, bundle, None, error=str(e)))
    return out


def find_bundle(name_or_path: str) -> Path | None:
    p = Path(name_or_path).expanduser()
    if (p / PROTOCOL_FILENAME).exists():
        return p.resolve()
    for root in protocol_roots():
        if (root / name_or_path / PROTOCOL_FILENAME).exists():
            return (root / name_or_path).resolve()
    return None


def load_named_protocol(name_or_path: str) -> Protocol:
    bundle = find_bundle(name_or_path)
    if bundle is None:
        roots = ", ".join(str(r) for r in protocol_roots())
        raise FileNotFoundError(f"No protocol '{name_or_path}' under {roots} (nor at that path)")
    return load_protocol(bundle)

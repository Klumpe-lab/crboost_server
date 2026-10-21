"""easymode's weights cache and Ais models: which features the cache holds, which network each one
runs, and the provenance the Segment job records for it.

The cache is the directory `easymode download --all` filled, named by tools.easymode.cache_dir in
config/conf.yaml: `models/registry.json` names each feature's default network, `models/models/`
holds the weights, and `models.sha256` beside `models/` lists their checksums as they were frozen.
"""

from __future__ import annotations

import hashlib
import json
import logging
import tarfile
from dataclasses import dataclass
from pathlib import Path

from services.configs.config_service import get_config_service
from services.segmentation.manifest import ENGINE_AIS, ENGINE_NATIVE

logger = logging.getLogger(__name__)

TOOL_NAME = "easymode"


def easymode_cache_dir() -> Path:
    """The configured weights cache. Raises LookupError naming the setting that is missing."""
    cfg = get_config_service()
    if not cfg.is_tool_configured(TOOL_NAME):
        raise LookupError("easymode is not configured: add tools.easymode to config/conf.yaml")
    raw = (cfg.get_tool_config(TOOL_NAME).cache_dir or "").strip()
    if not raw:
        raise LookupError(
            "tools.easymode.cache_dir is not set in config/conf.yaml (the directory `easymode download --all` filled)"
        )
    return Path(raw).expanduser()


@dataclass(frozen=True)
class EasymodeModel:
    """The network easymode runs for one feature: the default variant of its registry entry."""

    feature: str
    variant: str
    weights: Path
    timestamp: str | None
    weights_sha256: str | None  # None when models.sha256 does not list the file

    @property
    def engine(self) -> str:
        return ENGINE_AIS if self.weights.suffix == ".scnm" else ENGINE_NATIVE


def read_catalog(cache_dir: Path) -> dict[str, EasymodeModel]:
    """Every feature of the cache's registry, by name. Raises OSError / ValueError when the cache
    has no readable registry."""
    registry = json.loads((cache_dir / "models" / "registry.json").read_text())
    sums = _checksums(cache_dir / "models.sha256")
    out: dict[str, EasymodeModel] = {}
    for feature, entry in sorted(registry.items()):
        tag = entry.get("default") if isinstance(entry, dict) else None
        model = ((entry.get("models") or {}).get(tag) if tag else None) or {}
        if not model.get("weights"):
            logger.warning("easymode registry %s: %r names no default weights; not offered", cache_dir, feature)
            continue
        # registry paths are relative to models/; the checksums to the cache root
        rel = Path("models") / model["weights"]
        out[feature] = EasymodeModel(
            feature=feature,
            variant=tag,
            weights=cache_dir / rel,
            timestamp=model.get("timestamp"),
            weights_sha256=sums.get(rel.as_posix()),
        )
    return out


def _checksums(path: Path) -> dict[str, str]:
    """{relative path: sha256} from a `sha256sum` listing; empty when there is none."""
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        return {}
    out = {}
    for line in lines:
        digest, _, name = line.partition("  ")
        if digest and name:
            out[name.strip()] = digest.strip()
    return out


def ais_model_metadata(scnm: Path) -> dict:
    """The metadata an Ais model (.scnm, a tar archive) carries: its title, its network (`arch`), the
    X/Y pixel it segments at (`apix`), …. Raises ValueError for a file that is not an Ais model."""
    try:
        with tarfile.open(scnm, "r") as tar:
            member = next((m for m in tar.getmembers() if m.name.endswith("_metadata.json")), None)
            f = tar.extractfile(member) if member is not None else None
            if f is None:
                raise ValueError(f"{scnm.name} holds no *_metadata.json, so it is not an Ais model")
            meta = json.load(f)
    except tarfile.TarError as e:
        raise ValueError(f"{scnm.name} is not an Ais model ({e})") from e
    if not isinstance(meta, dict):
        raise ValueError(f"{scnm.name}: its metadata is not a JSON object")
    return meta


def ais_model_title(scnm: Path) -> str:
    """The title an Ais model carries in its metadata. `ais segment` names its output
    `<tomogram>__<title>.mrc`. Raises ValueError for a file that is not an Ais model."""
    title = ais_model_metadata(scnm).get("title")
    if not isinstance(title, str) or not title.strip():
        raise ValueError(f"{scnm.name} has no title in its metadata")
    return title


def plane_pixel(meta: dict) -> float | None:
    """The X/Y pixel (Å) of an Ais model that decides each Z plane on its own (a 2D or 2.5D network),
    or None for a 3D network (its `arch` says so) or a model that states no pixel."""
    if "3d" in str(meta.get("arch") or "").lower():
        return None
    apix = meta.get("apix")
    return float(apix) if isinstance(apix, (int, float)) and apix > 0 else None


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()

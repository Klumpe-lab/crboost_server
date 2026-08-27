"""Protocol — a portable, declarative capture of a workflow that worked (roadmap 14).

A protocol is a bundle directory:

    <bundle>/protocol.yaml     this schema
    <bundle>/assets/           frozen template / mask volumes the species entries point at
    <bundle>/test/             OPTIONAL regression bundle: input.yaml, bands.yaml, site.yaml,
                               snapshots/ — a protocol shares fine without it

Three parameter layers, and only the middle one lives here (FEATURE_recipes.md §1): dataset
facts (apix, dose, tilt scheme, TS count) come from the mdocs at apply time and are only
*expected* here; workflow decisions (stages, order, every USER_PARAM, species assets, IO
wiring) are the payload; site/execution config (partitions, walltimes, container paths)
never appears — the test bundle pins a site snapshot separately.

Stage params are FULL `USER_PARAMS` snapshots, not diffs against defaults: a code-side
default change silently altering a shared protocol is itself a regression class this exists
to catch. `schema_fingerprint` (hash of the param class's USER_PARAMS names) lets apply say
"captured against a different field set" instead of dropping fields silently. No absolute
path may appear anywhere in a bundle.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from services.models_base import InstanceId, JobType

PROTOCOL_FILENAME = "protocol.yaml"
ASSETS_DIRNAME = "assets"
TEST_DIRNAME = "test"
PROTOCOL_SCHEMA_VERSION = 1


class Expectation(BaseModel):
    """A dataset fact the protocol was validated on. Checked at apply time and surfaced as a
    warning, never a block — the facts come from the mdocs, the protocol only expects them."""

    model_config = ConfigDict(extra="forbid")

    about: float
    tol: float = 0.0

    def holds(self, value: float) -> bool:
        return abs(float(value) - self.about) <= self.tol + 1e-9


class ProtocolTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asset: str  # bundle-relative, e.g. assets/copia_template.mrc
    polarity: Literal["white", "black"] = "black"
    source: str | None = None
    lowpass_ang: float | None = None
    notes: str = ""


class ProtocolMask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asset: str
    method: Literal["spherical", "cylindrical", "relion", "manual", "imported"] | None = None
    threshold: float | None = None
    extend_pixels: float | None = None
    soft_edge_pixels: float | None = None
    lowpass_ang: float | None = None
    notes: str = ""


class ProtocolExtraction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    box_size: int
    crop_size: int
    binning: float


class ProtocolSpecies(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str  # becomes the project species id AND the instance-id suffix of its stages
    name: str
    diameter_ang: float | None = None
    symmetry: str = "C1"
    notes: str = ""
    template: ProtocolTemplate | None = None
    mask: ProtocolMask | None = None
    extraction: ProtocolExtraction | None = None


class ProtocolStage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job: str  # JobType value
    species: str | None = None  # ProtocolSpecies.id -> `{job}__{species}` instance id
    params: dict[str, Any] = Field(default_factory=dict)
    # Explicit IO wiring as STAGE references (input slot -> producer stage instance id).
    # Exported from `source_overrides`, whose stored form embeds job numbers and does not
    # travel; re-resolved against the producer's instance path at apply.
    inputs: dict[str, str] = Field(default_factory=dict)
    schema_fingerprint: str | None = None

    @property
    def job_type(self) -> JobType:
        return JobType.from_string(self.job)

    @property
    def instance_id(self) -> str:
        return str(InstanceId(self.job_type, self.species))


class Protocol(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    version: int = 1
    schema_version: int = PROTOCOL_SCHEMA_VERSION
    description: str = ""
    provenance: dict[str, Any] = Field(default_factory=dict)
    expects: dict[str, Expectation] = Field(default_factory=dict)
    species: list[ProtocolSpecies] = Field(default_factory=list)
    stages: list[ProtocolStage] = Field(default_factory=list)

    # Bound by load_protocol / dump_protocol; never serialized (the bundle is wherever it is).
    _bundle_dir: Path | None = PrivateAttr(default=None)

    @property
    def bundle_dir(self) -> Path | None:
        return self._bundle_dir

    @property
    def test_dir(self) -> Path | None:
        return self._bundle_dir / TEST_DIRNAME if self._bundle_dir else None

    def asset_path(self, rel: str) -> Path:
        if self._bundle_dir is None:
            raise ValueError(f"Protocol '{self.name}' is not bound to a bundle directory; cannot resolve '{rel}'")
        return self._bundle_dir / rel

    def stage_ids(self) -> list[str]:
        return [s.instance_id for s in self.stages]

    def get_species(self, species_id: str) -> ProtocolSpecies | None:
        return next((s for s in self.species if s.id == species_id), None)

    def get_stage(self, instance_id: str) -> ProtocolStage | None:
        return next((s for s in self.stages if s.instance_id == instance_id), None)


def schema_fingerprint(param_class: type) -> str:
    """Stable id of a param class's USER_PARAMS name set (12 hex chars of sha1 over the
    comma-joined sorted names). Written by export, compared at apply."""
    names = ",".join(sorted(getattr(param_class, "USER_PARAMS", set())))
    return hashlib.sha1(names.encode()).hexdigest()[:12]


def protocol_to_yaml(protocol: Protocol) -> str:
    data = protocol.model_dump(mode="json")
    return yaml.safe_dump(data, sort_keys=False, default_flow_style=False, allow_unicode=True, width=110)


def load_protocol(bundle_dir: Path | str) -> Protocol:
    bundle_dir = Path(bundle_dir).expanduser().resolve()
    path = bundle_dir / PROTOCOL_FILENAME
    if not path.exists():
        raise FileNotFoundError(f"No {PROTOCOL_FILENAME} in {bundle_dir}")
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top level must be a mapping")
    protocol = Protocol.model_validate(data)
    protocol._bundle_dir = bundle_dir
    return protocol


def dump_protocol(protocol: Protocol, bundle_dir: Path | str) -> Path:
    bundle_dir = Path(bundle_dir).expanduser().resolve()
    bundle_dir.mkdir(parents=True, exist_ok=True)
    path = bundle_dir / PROTOCOL_FILENAME
    path.write_text(protocol_to_yaml(protocol))
    protocol._bundle_dir = bundle_dir
    return path

"""Protocol — the shape of a pipeline with its parameters, as a portable bundle (roadmap 16).

A protocol is a bundle directory:

    <bundle>/protocol.yaml     this schema
    <bundle>/assets/           the template / mask volumes the species entries point at

Only workflow decisions live here: the stages in order, every USER_PARAM of each, the species
with their assets, the IO wiring. Dataset facts (apix, dose, tilt scheme) come from the mdocs
of whatever data the project is created on; site/execution config (partitions, walltimes,
container paths) never appears. Stage params are FULL `USER_PARAMS` snapshots, not diffs against
defaults, so a code-side default change cannot silently alter a shared protocol. No absolute
path may appear anywhere in a bundle.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from services.models_base import InstanceId, JobType

PROTOCOL_FILENAME = "protocol.yaml"
ASSETS_DIRNAME = "assets"
# Inside a project created from a protocol: the frozen copy of the protocol.yaml it was created
# from (what "protocol | current" compares against). Sibling of External/, Import/, Schemes/.
PROJECT_PROTOCOL_DIRNAME = "protocol"
PROTOCOL_SCHEMA_VERSION = 1


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
    species: list[ProtocolSpecies] = Field(default_factory=list)
    stages: list[ProtocolStage] = Field(default_factory=list)

    # Bound by load_protocol / dump_protocol; never serialized (the bundle is wherever it is).
    _bundle_dir: Path | None = PrivateAttr(default=None)

    @property
    def bundle_dir(self) -> Path | None:
        return self._bundle_dir

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

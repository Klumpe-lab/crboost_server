"""The job page's parameter layout, read from the param classes: each user parameter's group, label, box width class
and unit (`form(...)` in its Field's `json_schema_extra`, its `title`), the groups in the class's `PARAM_GROUPS`
order. One renderer draws every job's parameters from this (`ui/job_plugins/params_renderer.py`)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, get_args, get_origin

from services.jobs._base import SPECIES_GROUP, AbstractJobParams, L, M, S

logger = logging.getLogger(__name__)

# Parameters another section draws: the array throttle is SLURM's "At once".
OUTSIDE_PARAMETERS = frozenset({"array_throttle"})
OTHER_GROUP = "Other"

# Kinds of field, by what their row holds.
TOGGLE, NUMBER, CHOICE, TEXT, LIST = "toggle", "number", "choice", "text", "list"

_unplaced_logged: set[str] = set()


@dataclass(frozen=True)
class FieldLayout:
    name: str
    title: str
    width: str  # S, M or L; "" for a toggle
    unit: str
    kind: str
    hint: str | None
    enum: type[Enum] | None = None


@dataclass(frozen=True)
class GroupLayout:
    title: str
    fields: tuple[FieldLayout, ...]

    @property
    def values(self) -> tuple[FieldLayout, ...]:
        return tuple(f for f in self.fields if f.kind != TOGGLE)

    @property
    def toggles(self) -> tuple[FieldLayout, ...]:
        return tuple(f for f in self.fields if f.kind == TOGGLE)

    @property
    def is_species(self) -> bool:
        return self.title == SPECIES_GROUP


def _concrete(annotation: Any) -> Any:
    """`X | None` -> X; anything else as it is."""
    args = [a for a in get_args(annotation) if a is not type(None)]
    if args and get_origin(annotation) is not list and len(args) == 1:
        return args[0]
    return annotation


def _kind(annotation: Any) -> tuple[str, type[Enum] | None]:
    t = _concrete(annotation)
    if t is bool:
        return TOGGLE, None
    if isinstance(t, type) and issubclass(t, Enum):
        return CHOICE, t
    if t in (int, float):
        return NUMBER, None
    if get_origin(t) is list or t is list:
        return LIST, None
    return TEXT, None


def _title(name: str, info) -> str:
    if info.title:
        return info.title
    return name.replace("_", " ").capitalize()


def _default_width(kind: str) -> str:
    return {NUMBER: S, CHOICE: M, TEXT: M, LIST: L}.get(kind, "")


def field_layout(cls: type[AbstractJobParams], name: str) -> FieldLayout:
    """One user parameter's row: its label, width class, unit and kind."""
    info = cls.model_fields[name]
    extra = info.json_schema_extra if isinstance(info.json_schema_extra, dict) else {}
    kind, enum = _kind(info.annotation)
    width = "" if kind == TOGGLE else (str(extra.get("width") or "") or _default_width(kind))
    return FieldLayout(
        name=name,
        title=_title(name, info),
        width=width,
        unit=str(extra.get("unit") or ""),
        kind=kind,
        hint=info.description,
        enum=enum,
    )


def group_of(cls: type[AbstractJobParams], name: str) -> str:
    extra = cls.model_fields[name].json_schema_extra
    group = extra.get("group") if isinstance(extra, dict) else None
    return str(group) if group else OTHER_GROUP


def layout_of(job_model: AbstractJobParams | type[AbstractJobParams]) -> list[GroupLayout]:
    """The job's user parameters as groups, in the class's `PARAM_GROUPS` order, then any other group in the order
    its first field is declared, "Other" last. Within a group the fields keep their declaration order. A parameter
    without a group lands in "Other", logged once per class so the gap gets filled."""
    cls = job_model if isinstance(job_model, type) else type(job_model)
    names = [n for n in cls.model_fields if n in cls.USER_PARAMS and n not in OUTSIDE_PARAMETERS]
    by_group: dict[str, list[FieldLayout]] = {}
    for name in names:
        group = group_of(cls, name)
        if group == OTHER_GROUP and cls.__name__ not in _unplaced_logged:
            _unplaced_logged.add(cls.__name__)
            unplaced = [n for n in names if group_of(cls, n) == OTHER_GROUP]
            logger.warning("%s: user parameters with no group on the job page: %s", cls.__name__, unplaced)
        by_group.setdefault(group, []).append(field_layout(cls, name))
    order = [g for g in cls.PARAM_GROUPS if g in by_group]
    order += [g for g in by_group if g not in order and g != OTHER_GROUP]
    if OTHER_GROUP in by_group:
        order.append(OTHER_GROUP)
    return [GroupLayout(g, tuple(by_group[g])) for g in order]


__all__ = [
    "CHOICE",
    "LIST",
    "NUMBER",
    "OTHER_GROUP",
    "OUTSIDE_PARAMETERS",
    "SPECIES_GROUP",
    "TEXT",
    "TOGGLE",
    "FieldLayout",
    "GroupLayout",
    "L",
    "M",
    "S",
    "field_layout",
    "group_of",
    "layout_of",
]

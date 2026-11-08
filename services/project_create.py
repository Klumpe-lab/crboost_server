"""What a new project is made of: `ProjectSpec`, the one description behind the landing page's Create, the
Protocols dialog and the agent API, and the checks a spec passes before anything is written.

Every acquisition value the pipeline reads is a field of the spec, and none has a default: the caller says
each one. The landing page and the Protocols dialog fill the ones they do not ask for from the model defaults
in their own code (`model_defaults`); the API takes every one from the request.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, ValidationError

from services.models_base import AcquisitionParams, MicroscopeParams
from services.project_state import TomogramJobSource

# The acquisition values a project holds, by spec field: (model, attribute). Exactly what the pipeline reads:
# the drivers, the job.star writers, and job creation (detector size, `ensure_job_initialized`). The project's
# sample thickness is not among them: nothing reads it, and alignment's own `sample_thickness_nm` is a job
# parameter.
ACQUISITION_FIELDS: dict[str, tuple[str, str]] = {
    "pixel_size_angstrom": ("microscope", "pixel_size_angstrom"),
    "acceleration_voltage_kv": ("microscope", "acceleration_voltage_kv"),
    "spherical_aberration_mm": ("microscope", "spherical_aberration_mm"),
    "amplitude_contrast": ("microscope", "amplitude_contrast"),
    "dose_per_tilt": ("acquisition", "dose_per_tilt"),
    "tilt_axis_degrees": ("acquisition", "tilt_axis_degrees"),
    "detector_dimensions": ("acquisition", "detector_dimensions"),
    "invert_tilt_angles": ("acquisition", "invert_tilt_angles"),
    "invert_defocus_hand": ("acquisition", "invert_defocus_hand"),
    "eer_fractions_per_frame": ("acquisition", "eer_fractions_per_frame"),
}

# What a project with no raw data needs: the values imported tomograms and template matching read from the
# project (`backend.commit_imported_tomograms`, the TM driver). The rest describe tilt series it has none of.
NO_RAW_DATA_FIELDS = ("acceleration_voltage_kv", "spherical_aberration_mm", "amplitude_contrast", "invert_defocus_hand")

_DOSE_SOURCES = ("mdoc", "estimated", "user")


class ProjectSpec(BaseModel):
    """A new project: where it goes, its raw data and which of it, how its series are named, who owns it, and
    every acquisition value. Raw data is both globs, or neither for a project without raw data."""

    name: str
    base: str
    frames_glob: str = ""
    mdocs_glob: str = ""
    # The mdocs to import, out of those the glob matches; None imports every one.
    series: list[str] | None = None
    # Series named after their folder too: one project from folders that reuse series names.
    ts_names_by_folder: bool = False
    shared: bool = False
    # A path, or None for frames already gain-corrected: given either way.
    gain_reference: str | None = Field(...)

    pixel_size_angstrom: float | None = None
    acceleration_voltage_kv: float | None = None
    spherical_aberration_mm: float | None = None
    amplitude_contrast: float | None = None
    dose_per_tilt: float | None = None
    # Where the dose came from: "mdoc", "estimated" (the scan's fit of the mdoc DoseRate) or "user".
    dose_per_tilt_source: str = ""
    tilt_axis_degrees: float | None = None
    detector_dimensions: tuple[int, int] | None = None
    invert_tilt_angles: bool | None = None
    invert_defocus_hand: bool | None = None
    # Raw EER frames per fraction; for EER movies only.
    eer_fractions_per_frame: int | None = None

    # What the mdoc headers say about the delivery, kept on the project.
    acquisition_software: str = ""
    # Tomograms of other projects' jobs, imported once the project exists (a project without raw data).
    tomograms_from: list[TomogramJobSource] = Field(default_factory=list)

    @property
    def has_raw_data(self) -> bool:
        return bool(self.frames_glob or self.mdocs_glob)

    @property
    def eer_movies(self) -> bool:
        return self.frames_glob.lower().endswith(".eer")


# The EER grouping the motion job uses when a project sets none (`AbstractJobParams.eer_ngroups`).
_EER_FRACTIONS_UNSET = 32


def model_defaults() -> dict[str, Any]:
    """Every acquisition value at the model's default: what the landing page and the Protocols dialog fill in
    for the values they do not ask for. The API never uses these. The EER grouping, which the model leaves
    unset, is the one the motion job then uses, so an EER project states what it runs with."""
    m, a = MicroscopeParams(), AcquisitionParams()
    models = {"microscope": m, "acquisition": a}
    out = {name: getattr(models[model], attr) for name, (model, attr) in ACQUISITION_FIELDS.items()}
    out["eer_fractions_per_frame"] = _EER_FRACTIONS_UNSET
    return out


def required_values(spec: ProjectSpec) -> list[str]:
    """The acquisition values this spec must carry."""
    if not spec.has_raw_data:
        return list(NO_RAW_DATA_FIELDS)
    names = [n for n in ACQUISITION_FIELDS if n != "eer_fractions_per_frame"]
    if spec.eer_movies:
        names.append("eer_fractions_per_frame")
    return names


def spec_problems(spec: ProjectSpec) -> list[str]:
    """Why the spec cannot make a project, each named, or []: a name and a base; raw data as both globs or
    neither; a selection that is not empty; every acquisition value it must carry, within the model's bounds;
    a dose with its source."""
    problems: list[str] = []
    if not spec.name.strip():
        problems.append("name: missing")
    elif "/" in spec.name:
        problems.append(f"name: {spec.name!r} names a folder, not a project")
    if not spec.base.strip():
        problems.append("base: missing")
    if spec.has_raw_data:
        if not spec.frames_glob:
            problems.append("frames_glob: missing (raw data is both globs, or neither)")
        if not spec.mdocs_glob:
            problems.append("mdocs_glob: missing (raw data is both globs, or neither)")
        if spec.series is not None and not spec.series:
            problems.append("series: none selected")
        if spec.dose_per_tilt is not None and spec.dose_per_tilt_source not in _DOSE_SOURCES:
            problems.append(f"dose_per_tilt_source: one of {', '.join(_DOSE_SOURCES)}")
    elif spec.series:
        problems.append("series: a project without raw data has none")
    if spec.tomograms_from and spec.has_raw_data:
        problems.append("tomograms_from: a project with raw data makes its own tomograms")
    for name in required_values(spec):
        if getattr(spec, name) is None:
            problems.append(f"{name}: missing")
    if not spec.eer_movies and spec.eer_fractions_per_frame is not None and spec.has_raw_data:
        problems.append("eer_fractions_per_frame: only for EER movies")

    given = {n: getattr(spec, n) for n in ACQUISITION_FIELDS if getattr(spec, n) is not None}
    for model_cls, model in ((MicroscopeParams, "microscope"), (AcquisitionParams, "acquisition")):
        fields = {attr: given[n] for n, (m, attr) in ACQUISITION_FIELDS.items() if m == model and n in given}
        try:
            model_cls(**fields)
        except ValidationError as e:
            for error in e.errors():
                field = str(error["loc"][0]) if error.get("loc") else "?"
                problems.append(f"{field}: {error.get('msg', 'invalid')}")
    return problems


def apply_values(state, spec: ProjectSpec) -> None:
    """Write the spec's acquisition values, its dose source, software and gain reference onto a new project."""
    for name, (model, attr) in ACQUISITION_FIELDS.items():
        value = getattr(spec, name)
        if value is not None:
            setattr(getattr(state, model), attr, tuple(value) if name == "detector_dimensions" else value)
    if spec.dose_per_tilt is not None:
        state.acquisition.dose_per_tilt_source = spec.dose_per_tilt_source
    if spec.acquisition_software:
        state.acquisition.acquisition_software = spec.acquisition_software
    state.acquisition.gain_reference_path = spec.gain_reference or None

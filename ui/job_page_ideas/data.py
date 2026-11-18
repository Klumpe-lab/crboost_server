"""What the job-page prototypes show: a few real jobs, read and never written, flattened into what a page draws.

The parameter groups, units and widths below are this prototype's: a build would declare them on the param classes
(a group, a unit and a width class per field), so one renderer could lay out every job and the per-type plugins keep
only what is really theirs (choice rows fed from project state, warnings).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

from services.configs.config_service import get_config_service
from services.job_params import SLURM_FIELDS
from services.jobs.spec import display_name
from services.models_base import JobStatus, JobType
from services.particles.particle_sets import list_sets, set_lineage, set_read_by
from services.path_resolution_service import PathResolutionService
from services.project_state import ProjectState
from ui.branch_map.model import WORDS, project_from_state, value_words

logger = logging.getLogger(__name__)

ROOT = Path("/groups/<lab>/_crboost_data_api")
# (key, project folder, instance id): the jobs the page can switch between.
SAMPLES = (
    ("motion", "<project>", "fsMotionAndCtf"),
    ("tm", "<project>", "templatematching"),
    ("class3d", "<project>", "class3d__9"),
    ("recon", "<project>", "tsReconstruct"),
)

# Width classes: S a number, M a choice or a short code (500:10, 2x2x1), L a path or a list (to the column's edge).
S, M, L = "S", "M", "L"

# kind -> [(group title, [(field, label, width, unit)])]; a field left out of every group lands in "Other".
LAYOUT: dict[str, list[tuple[str, list[tuple[str, str, str, str]]]]] = {
    "fsMotionAndCtf": [
        ("Motion", [("m_range_min_max", "Range", M, "Å"), ("m_bfac", "B-factor", S, "Å²"), ("m_grid", "Grid", M, "")]),
        (
            "CTF",
            [
                ("c_range_min_max", "Resolution range", M, "Å"),
                ("c_defocus_min_max", "Defocus range", M, "µm"),
                ("c_grid", "Grid", M, ""),
                ("c_window", "Window", S, "px"),
                ("c_use_sum", "Use frame sum", "", ""),
                ("do_phase", "Estimate phase shifts", "", ""),
            ],
        ),
        (
            "Output",
            [
                ("out_skip_first", "Skip first", S, "tilts"),
                ("out_skip_last", "Skip last", S, "tilts"),
                ("out_average_halves", "Average halves", "", ""),
            ],
        ),
        (
            "Processing",
            [
                ("perdevice", "Per GPU", S, ""),
                ("do_at_most", "Max series", S, ""),
                ("gain_operations", "Gain ops", M, ""),
            ],
        ),
    ],
    "templatematching": [
        (
            "Search",
            [
                ("angular_search", "Angular step", S, "°"),
                ("symmetry", "Symmetry", M, ""),
                ("bandpass_filter", "Bandpass", M, "Å"),
                ("every_nth_tomogram", "Every nth tomogram", S, ""),
            ],
        ),
        (
            "From species",
            [
                ("template_path", "Template", L, ""),
                ("mask_path", "Mask", L, ""),
                ("non_spherical_mask", "Non-spherical mask", "", ""),
            ],
        ),
        (
            "Weighting",
            [
                ("defocus_weight", "Defocus weight", "", ""),
                ("dose_weight", "Dose weight", "", ""),
                ("spectral_whitening", "Spectral whitening", "", ""),
                ("random_phase_correction", "Random phase correction", "", ""),
            ],
        ),
        (
            "Search region",
            [
                ("search_include", "Search in", L, ""),
                ("search_exclude", "Leave out", L, ""),
                ("search_dilation_ang", "Grow by", S, "Å"),
            ],
        ),
        (
            "GPUs",
            [
                ("lanes", "Lanes", S, ""),
                ("perdevice", "Per device", S, ""),
                ("tomograms_per_lane", "Tomograms / lane", S, ""),
                ("gpu_split", "GPU split", S, ""),
                ("half_precision", "Half precision", "", ""),
            ],
        ),
    ],
    "class3d": [
        (
            "Classification",
            [
                ("n_classes", "Classes", S, ""),
                ("n_iterations", "Iterations", S, ""),
                ("tau_fudge", "T", S, ""),
                ("symmetry", "Symmetry", M, ""),
                ("random_seed", "Random seed", S, ""),
            ],
        ),
        (
            "Reference & mask",
            [
                ("ini_high", "Low-pass reference", S, "Å"),
                ("particle_diameter", "Mask diameter", S, "Å"),
                ("solvent_mask_path", "Solvent mask", L, ""),
                ("mask_from_job", "Mask from a job", "", ""),
                ("firstiter_cc", "First iteration CC", "", ""),
                ("flatten_solvent", "Flatten solvent", "", ""),
                ("zero_mask", "Zero mask", "", ""),
            ],
        ),
        (
            "Angles & offsets",
            [
                ("healpix_order", "Healpix order", S, ""),
                ("sigma_ang", "Sigma angle", S, "°"),
                ("offset_range", "Offset range", S, "px"),
                ("offset_step", "Offset step", S, "px"),
                ("oversampling", "Oversampling", S, ""),
                ("skip_align", "Skip alignment", "", ""),
            ],
        ),
        (
            "CTF & scale",
            [("do_ctf", "CTF correction", "", ""), ("do_norm", "Normalise", "", ""), ("do_scale", "Scale", "", "")],
        ),
        (
            "Compute",
            [
                ("mpi_ranks", "MPI ranks", S, ""),
                ("threads", "Threads", S, ""),
                ("pool", "Pool", S, ""),
                ("pad", "Pad", S, ""),
                ("use_gpu", "Use GPU", "", ""),
                ("preread_images", "Pre-read images", "", ""),
                ("dont_combine_weights_via_disc", "Weights in memory", "", ""),
            ],
        ),
    ],
    "tsReconstruct": [
        ("Reconstruction", [("rescale_angpixs", "Pixel size", S, "Å/px"), ("deconv", "Deconvolution", S, "")]),
        ("Half maps", [("halfmap_frames", "Frames", S, ""), ("halfmap_tilts", "Tilts", S, "")]),
        ("Processing", [("perdevice", "Per GPU", S, ""), ("tilt_series_per_task", "Series / task", S, "")]),
    ],
}


@dataclass
class Param:
    name: str
    label: str
    value: str
    toggle: bool
    on: bool
    width: str
    unit: str


@dataclass
class Group:
    title: str
    params: list[Param]

    @property
    def fields(self) -> list[Param]:
        return [p for p in self.params if not p.toggle]

    @property
    def toggles(self) -> list[Param]:
        return [p for p in self.params if p.toggle]


@dataclass
class Source:
    name: str  # the job's type name, or what stands in for one ("Manual path", "Pool set")
    number: str  # 008, or ""
    status: JobStatus | None


@dataclass
class In:
    word: str  # the slot word, as the builder's card says it
    required: bool
    source: Source | None
    path: str  # absolute, or ""
    note: str = ""  # a second line: a set's size, the images it reads
    detail: list[tuple[str, str]] = field(default_factory=list)  # a set's facets: (word, text)


@dataclass
class Out:
    word: str
    path: str


@dataclass
class Sample:
    key: str
    iid: str
    name: str
    number: str
    status: JobStatus
    tool: str
    species: str
    project: Path
    job_dir: Path | None
    fixed: bool
    groups: list[Group]
    inputs: list[In]
    outputs: list[Out]
    profile: str
    throttle: int | None
    slurm: list[tuple[str, str, str]]  # (field, label, value)
    supervisor: list[tuple[str, str]]

    def rel(self, path: str) -> str:
        """`path` from the project folder: External/job008/tomograms.star."""
        try:
            return str(Path(path).relative_to(self.project))
        except ValueError:
            return path


def _param(jm, name: str, label: str, width: str, unit: str) -> Param:
    v = getattr(jm, name)
    if isinstance(v, bool):
        return Param(name, label, "", True, v, "", "")
    text = value_words(v)
    return Param(name, label, text if text not in ("", "None") else "", False, False, width or S, unit)


def _groups(kind: str, jm) -> list[Group]:
    spec = LAYOUT.get(kind, [])
    placed = {f for _t, fs in spec for f, *_ in fs}
    out = [Group(t, [_param(jm, *f) for f in fs if f[0] in jm.USER_PARAMS]) for t, fs in spec]
    rest = sorted(set(jm.USER_PARAMS) - placed - {"array_throttle"})
    if rest:
        out.append(Group("Other", [_param(jm, n, n.replace("_", " ").capitalize(), S, "") for n in rest]))
    return [g for g in out if g.params]


def _facet_lines(state, jm, key: str) -> list[tuple[str, str]]:
    """What a job fixed at submit read of a set's picks: per facet, the job each project's came from."""
    rec = (jm.submitted_inputs or {}).get(key) if jm.inputs_fixed else None
    if rec is None or not rec.facets:
        return []
    by_ref: dict[str, int] = {}
    for ref in rec.facets.values():
        by_ref[ref] = by_ref.get(ref, 0) + 1
    out = []
    for ref, n in by_ref.items():
        proj, _, iid = ref.partition("|")
        here = state.jobs.get(iid) if proj == str(state.project_path) else None
        name = f"{display_name(here.job_type)} {_number(here)}" if here is not None else iid or "as picked"
        out.append(("images", f"{name} · {n} of {len(rec.facets)} projects"))
    return out


def _number(jm) -> str:
    name = jm.relion_job_name or ""
    return name.rstrip("/").rsplit("job", 1)[-1] if "job" in name else ""


@cache
def _state(folder: str) -> ProjectState:
    return ProjectState.load(ROOT / folder / "project_params.json", read_only=True)


@cache
def load(key: str) -> Sample:
    """One sample job, read once per server process."""
    _k, folder, iid = next(s for s in SAMPLES if s[0] == key)
    state = _state(folder)
    project = ROOT / folder
    jm = state.jobs[iid]
    pr = project_from_state(state, project, set_parents={})
    j = pr.by_id[iid]
    job_type = jm.job_type if hasattr(jm, "job_type") else JobType(iid.split("__")[0])
    resolver = PathResolutionService(state)

    inputs = []
    for row in j.inputs:
        path = str((jm.paths or {}).get(row.slot.key) or "")
        src = None
        detail: list[tuple[str, str]] = []
        note = ""
        set_id = set_read_by(jm, row.slot.key)
        if set_id is not None:
            pset = next((x for x in list_sets(project) if x.id == set_id), None)
            src = Source("Particle set", set_id.split("_", 1)[0], None)
            note = set_lineage(pset) if pset is not None else ""
            detail = _facet_lines(state, jm, row.slot.key)
        elif row.source and row.source in state.jobs:
            sj = state.jobs[row.source]
            src = Source(display_name(sj.job_type), _number(sj), sj.execution_status)
        elif path:
            src = Source("File", "", None)
        inputs.append(In(row.slot.label, row.slot.required, src, path, note, detail))

    outputs = []
    job_dir = project / "External" / f"job{_number(jm)}" if _number(jm) else None
    for slot in resolver.get_output_schema_for_job(job_type, jm):
        recorded = (jm.paths or {}).get(slot.key)
        path = str(recorded) if recorded else (str(job_dir / slot.path_template) if job_dir else "")
        word = WORDS.get(slot.key) or slot.key.removeprefix("output_").replace("_", " ")
        if not outputs and pr.makes.get(j.kind):
            word = pr.makes[j.kind]
        outputs.append(Out(word, path))

    eff = jm.get_effective_slurm_config()
    slurm = [(f, label, value_words(getattr(eff, f, ""))) for f, label in SLURM_FIELDS]
    is_array = "array_throttle" in jm.USER_PARAMS
    sup = get_config_service().supervisor_slurm_defaults if is_array else None
    supervisor = (
        [(label, value_words(getattr(sup, f, ""))) for f, label in SLURM_FIELDS if value_words(getattr(sup, f, ""))]
        if sup is not None
        else []
    )
    species = ""
    sid = getattr(jm, "species_id", None)
    if sid:
        sp = state.get_species(sid)
        species = sp.name if sp else sid
    return Sample(
        key=key,
        iid=iid,
        name=display_name(job_type),
        number=_number(jm),
        status=jm.execution_status,
        tool=j.tool,
        species=species,
        project=project,
        job_dir=job_dir,
        fixed=bool(getattr(jm, "inputs_fixed", False)),
        groups=_groups(j.kind, jm),
        inputs=inputs,
        outputs=outputs,
        profile=j.kind if jm.has_resource_profile() else "",
        throttle=getattr(jm, "array_throttle", None) if is_array else None,
        slurm=slurm,
        supervisor=supervisor,
    )

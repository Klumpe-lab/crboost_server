"""How far a species has come: its jobs as four stages, each job with the one number that says what it made.

    evidence  →  picks  →  extracted  →  maps

- evidence: the Template Match jobs of the species, and the Segment jobs (or imported segmentations) that hold
  the feature it is picked from;
- picks: its Pick candidates jobs (and Filament pick jobs made before those traced filaments), each with where
  its picks come from (`TM`, `SEG · <feature>`, `TRACE · <feature>`) and how many there are;
- extracted: its Warp Export and Subtomo Extraction jobs, with the particles cut and their grid;
- maps: its Reconstruct Particle, Class 3D, Refine 3D and PostProcess jobs, with a resolution where one is known.

And how the species is found at all (`Finding`): by template matching (a template selected, or none) and from a
segmentation (the feature it names, and whether this project holds it).

The read model of the Species page's strip and of `get_species`. Takes an explicit state; reads job outputs
(stars, manifests, sidecars), so callers run it off the event loop. Nothing here draws.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

from services.jobs.spec import JOB_SPEC_BY_TYPE, display_name
from services.models_base import JobType, PickSource, SegmentMethod, SpeciesKind
from services.particles.species_jobs import jobs_for_species
from services.segmentation.manifest import read_manifest, segment_feature_id

EVIDENCE, PICKS, EXTRACTED, MAPS = "evidence", "picks", "extracted", "maps"
STAGES = (EVIDENCE, PICKS, EXTRACTED, MAPS)
STAGE_TITLES = {EVIDENCE: "Evidence", PICKS: "Picks", EXTRACTED: "Extracted", MAPS: "Maps"}

# What a filament's resolution line has to say with it (roadmap 41, 2026-11-22).
FILAMENT_FSC_NOTE = (
    "A filament averaged without a search of its spin is a smooth tube, whose FSC has zeros: the first crossing "
    "of 0.143 can be one of them, not the resolution."
)


@dataclass(frozen=True, slots=True)
class Step:
    """One job of the species, as its stage shows it."""

    stage: str
    instance_id: str
    name: str  # the job type's name
    job: str  # "job005" once it has a directory, else ""
    status: str  # JobStatus value
    token: str  # where a pick job's picks come from; "" for the others
    fact: str  # what it made, in one number; "" when it has made nothing yet
    note: str  # the longer line for a tooltip


@dataclass(frozen=True, slots=True)
class Finding:
    """One way the species can be found, and whether this project has what it takes."""

    how: str  # "Template matching" | "Segmentation"
    ready: bool
    text: str


@dataclass(frozen=True, slots=True)
class SpeciesProgress:
    species_id: str
    kind: str
    finding: tuple[Finding, ...]
    steps: tuple[Step, ...]

    def stage(self, name: str) -> tuple[Step, ...]:
        return tuple(s for s in self.steps if s.stage == name)

    def to_dict(self) -> dict:
        return {
            "finding": [asdict(f) for f in self.finding],
            "stages": {name: [asdict(s) for s in self.stage(name)] for name in STAGES},
        }


def count_star_rows(star: Path) -> int | None:
    """The rows of a star file's last loop, without parsing it; None when the file is not there."""
    try:
        with open(star, errors="replace") as f:
            n, in_loop = 0, False
            for line in f:
                s = line.strip()
                if s.startswith("loop_"):
                    in_loop, n = True, 0
                elif in_loop and s and not s.startswith(("_", "#", "data_")):
                    n += 1
            return n
    except OSError:
        return None


def _job_dir(project_path: Path, jm) -> Path | None:
    name = (getattr(jm, "relion_job_name", None) or "").strip("/")
    return Path(project_path) / name if name else None


def _json(path: Path) -> dict:
    """A sidecar's content; {} when it is not there or not JSON (the step then shows no fact)."""
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _plural(n: int, word: str) -> str:
    return f"{n:,} {word}{'' if n == 1 else 's'}"


def pick_token(jm) -> str:
    """Where a pick job's picks come from: `TM`, `SEG · <feature>`, or `TRACE · <feature>` for a filament."""
    if jm.job_type == JobType.FILAMENT_PICK:
        return f"TRACE · {jm.feature}"
    if PickSource(jm.source) == PickSource.TEMPLATE_MATCHING:
        return "TM"
    return f"{'TRACE' if jm.traces_filaments else 'SEG'} · {jm.feature or 'no feature'}"


def _pick_fact(jm, job_dir: Path | None) -> tuple[str, str]:
    if job_dir is None:
        return "", ""
    n = count_star_rows(job_dir / "candidates.star")
    if n is None:
        return "", ""
    fact = _plural(n, "pick")
    summary = _json(job_dir / "summary.json")
    filaments = summary.get("filaments")
    if isinstance(filaments, int):
        fact += f" · {_plural(filaments, 'filament')}"
    if jm.job_type == JobType.FILAMENT_PICK or getattr(jm, "traces_filaments", False):
        angles = "along the filament; the spin about it and its direction are not known"
    elif PickSource(jm.source) == PickSource.TEMPLATE_MATCHING:
        angles = "template matching's"
    else:
        angles = jm.effective_orientation.value
    return fact, f"angles: {angles}"


def _extract_fact(state, jm, job_dir: Path | None) -> tuple[str, str]:
    if job_dir is None:
        return "", ""
    n = count_star_rows(job_dir / "particles.star")
    if n is None:
        return "", ""
    pixel = float(state.microscope.pixel_size_angstrom) * float(jm.binning)
    crop = int(getattr(jm, "crop_size", -1) or -1)
    box = crop if crop > 0 else int(jm.box_size)
    return f"{_plural(n, 'particle')} · {pixel:g} Å · box {box}", ""


def _map_fact(jm, job_dir: Path | None, filament: bool) -> tuple[str, str]:
    """A resolution where the job has one. Read from what the job wrote: the Reconstruct Particle sidecar's
    unmasked half-map FSC, relion_refine's own estimate, PostProcess's final resolution."""
    if job_dir is None:
        return "", ""
    note = FILAMENT_FSC_NOTE if filament else ""
    if jm.job_type == JobType.RECONSTRUCT_PARTICLE:
        fsc = (_json(job_dir / "analytics" / "reconstruct.json").get("data") or {}).get("fsc") or {}
        res = ((fsc.get("unmasked") or {}).get("resolution") or {}).get("0.143")
        return (f"{float(res):.1f} Å (unmasked half maps)", note) if res else ("", note)
    if jm.job_type == JobType.CLASS3D:
        return _plural(int(jm.n_classes), "class").replace("classs", "classes"), ""
    if jm.job_type in (JobType.REFINE3D, JobType.POSTPROCESS):
        import starfile

        star, block, column = (
            (job_dir / "run_model.star", "model_general", "rlnCurrentResolution")
            if jm.job_type == JobType.REFINE3D
            else (job_dir / "postprocess.star", "general", "rlnFinalResolution")
        )
        if not star.is_file():
            its = sorted(job_dir.glob("run_it*_half1_model.star")) if jm.job_type == JobType.REFINE3D else []
            return (f"iteration {its[-1].name.split('_it')[1][:3].lstrip('0') or '0'}", note) if its else ("", note)
        try:
            general = starfile.read(star, always_dict=True).get(block)
            res = float(general[column]) if isinstance(general, dict) else float(general[column].iloc[0])
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return "", note
        return f"{res:.1f} Å", note
    return "", ""


def _segment_steps(state, project_path: Path, feature: str | None) -> tuple[list[Step], Finding]:
    """The jobs holding segmentations of `feature`, and the Segmentation line of how the species is found."""
    if not feature:
        return [], Finding("Segmentation", False, "no feature named (the Feature row above)")
    steps: list[Step] = []
    for iid, jm in (state.jobs or {}).items():
        if jm.job_type not in (JobType.SEGMENT, JobType.IMPORTED_SEGMENTATIONS):
            continue
        job_dir = _job_dir(project_path, jm)
        fact, holds = "", False
        if job_dir is not None and (job_dir / "segmentations.json").is_file():
            try:
                manifest = read_manifest(job_dir)
            except (OSError, ValueError):
                manifest = None
            if manifest is not None:
                entries = [e for e in manifest.entries if e.feature == feature]
                holds = bool(entries)
                total = len({e.tomogram for e in manifest.entries})
                fact = f"{feature} · {len(entries)} of {_plural(total, 'tomogram')}"
        elif jm.job_type == JobType.SEGMENT and jm.method == SegmentMethod.EASYMODE:
            # Not run yet: what it is set to make, by the id it will write.
            holds = feature in {segment_feature_id(f) for f in jm.features}
            fact = f"{feature} · not run yet" if holds else ""
        if not holds:
            continue
        steps.append(
            Step(
                stage=EVIDENCE,
                instance_id=iid,
                name=display_name(jm.job_type),
                job=os.path.basename(str(job_dir)) if job_dir else "",
                status=jm.execution_status.value,
                token="",
                fact=fact,
                note="",
            )
        )
    if steps:
        return steps, Finding("Segmentation", True, " · ".join(s.fact for s in steps if s.fact) or feature)
    return [], Finding("Segmentation", False, f"{feature} · no segmentation of it in this project")


# The last progress computed per (project, species): what a view that must not read disk (the registry's rail)
# shows.
_LATEST: dict[tuple[str, str], SpeciesProgress] = {}


def latest_progress(project_path: Path, species_id: str) -> SpeciesProgress | None:
    """The last `species_progress` computed for this species in this process; None before the first."""
    return _LATEST.get((str(Path(project_path)), species_id))


def species_progress(state, project_path: Path, species_id: str) -> SpeciesProgress | None:
    """The species' jobs by stage with what each made, and how it is found; None for a species the project does
    not have."""
    sp = state.get_species(species_id)
    if sp is None:
        return None
    filament = SpeciesKind(sp.kind) == SpeciesKind.FILAMENT
    template = sp.get_selected_template()
    mask = sp.get_selected_mask()
    if template is None:
        by_template = "no template"
    else:
        mask_name = os.path.basename(mask.mask_path) if mask else "none"
        by_template = f"{os.path.basename(template.template_path)} · mask {mask_name}"
    finding = [Finding("Template matching", template is not None, by_template)]
    segment_steps, segmentation = _segment_steps(state, project_path, sp.segmentation_feature)
    finding.append(segmentation)

    steps: list[Step] = list(segment_steps)
    for iid, jm in jobs_for_species(state, species_id):
        spec = JOB_SPEC_BY_TYPE.get(jm.job_type)
        stage = spec.progress_stage if spec is not None else None
        if stage is None:
            continue
        job_dir = _job_dir(project_path, jm)
        token, fact, note = "", "", ""
        if stage == EVIDENCE:
            fact = os.path.basename(getattr(jm, "template_path", "") or "")
        elif stage == PICKS:
            token = pick_token(jm)
            fact, note = _pick_fact(jm, job_dir)
        elif stage == EXTRACTED:
            fact, note = _extract_fact(state, jm, job_dir)
        else:
            fact, note = _map_fact(jm, job_dir, filament)
        steps.append(
            Step(
                stage=stage,
                instance_id=iid,
                name=display_name(jm.job_type),
                job=os.path.basename(str(job_dir)) if job_dir else "",
                status=jm.execution_status.value,
                token=token,
                fact=fact,
                note=note,
            )
        )
    progress = SpeciesProgress(
        species_id=species_id, kind=SpeciesKind(sp.kind).value, finding=tuple(finding), steps=tuple(steps)
    )
    _LATEST[(str(Path(project_path)), species_id)] = progress
    return progress

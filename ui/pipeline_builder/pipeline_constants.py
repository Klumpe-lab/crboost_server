from typing import Dict, List, Set
from services.project_state import JobType

PHASE_PREPROCESSING = "preprocessing"
PHASE_PARTICLES = "particles"

PHASE_JOBS: Dict[str, List[JobType]] = {
    PHASE_PREPROCESSING: [
        JobType.IMPORT_MOVIES,
        JobType.FS_MOTION_CTF,
        JobType.TS_ALIGNMENT,
        JobType.TS_CTF,
        JobType.TS_RECONSTRUCT,
        JobType.DENOISE_TRAIN,
        JobType.DENOISE_PREDICT,
    ],
    PHASE_PARTICLES: [
        JobType.TEMPLATE_MATCH_PYTOM,
        JobType.TEMPLATE_EXTRACT_PYTOM,
        JobType.SUBTOMO_EXTRACTION,
        JobType.RECONSTRUCT_PARTICLE,
        JobType.CLASS3D,
    ],
}

PHASE_META: Dict[str, tuple] = {
    PHASE_PREPROCESSING: ("looks_one", "Preprocessing", "Import → Denoise"),
    PHASE_PARTICLES: ("looks_two", "Particles", "Template Match → Class3D"),
}

ROSTER_ANCHOR: Dict[str, str] = {
    PHASE_PREPROCESSING: "roster-anchor-preprocessing",
    PHASE_PARTICLES: "roster-anchor-particles",
}

JOB_DEPENDENCIES: Dict[JobType, List[JobType]] = {
    JobType.IMPORT_MOVIES: [],
    JobType.FS_MOTION_CTF: [JobType.IMPORT_MOVIES],
    JobType.TS_ALIGNMENT: [JobType.FS_MOTION_CTF],
    JobType.TS_CTF: [JobType.TS_ALIGNMENT],
    JobType.TS_RECONSTRUCT: [JobType.TS_CTF],
    JobType.DENOISE_TRAIN: [JobType.TS_RECONSTRUCT],
    JobType.DENOISE_PREDICT: [JobType.DENOISE_TRAIN, JobType.TS_RECONSTRUCT],
    JobType.TEMPLATE_MATCH_PYTOM: [JobType.TS_CTF],
    JobType.TEMPLATE_EXTRACT_PYTOM: [JobType.TEMPLATE_MATCH_PYTOM],
    JobType.SUBTOMO_EXTRACTION: [JobType.TEMPLATE_EXTRACT_PYTOM],
    JobType.RECONSTRUCT_PARTICLE: [JobType.SUBTOMO_EXTRACTION],
    JobType.CLASS3D: [JobType.RECONSTRUCT_PARTICLE],
}

SB_SEP = "#e2e8f0"
SB_MUTE = "#94a3b8"
SB_ACT = "#3b82f6"
SB_ABG = "#eff6ff"


def missing_deps(job_type: JobType, selected_instance_ids: Set[str]) -> List[JobType]:
    def type_present(jt: JobType) -> bool:
        prefix = jt.value
        return any(s == prefix or s.startswith(prefix + "__") for s in selected_instance_ids)

    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        if type_present(JobType.TS_RECONSTRUCT) or type_present(JobType.DENOISE_PREDICT):
            return []
        return [JobType.TS_RECONSTRUCT]

    return [d for d in JOB_DEPENDENCIES.get(job_type, []) if not type_present(d)]


def next_instance_id(job_type: JobType, existing_ui_ids: List[str], state_keys: List[str]) -> str:
    taken = set(existing_ui_ids) | set(state_keys)
    base = job_type.value
    if base not in taken:
        return base
    for n in range(2, 200):
        candidate = f"{base}__{n}"
        if candidate not in taken:
            return candidate
    return f"{base}__{len(taken) + 1}"


def fmt(v) -> str:
    if v is None:
        return "---"
    return f"{v:.4g}" if isinstance(v, float) else str(v)
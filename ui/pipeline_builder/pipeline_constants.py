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
    PHASE_PREPROCESSING: ("layers.svg", "Preprocessing", "Import → Denoise"),
    PHASE_PARTICLES: (
        '<svg width="15" height="15" viewBox="0 0 15 15" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M7.49991 0.877075C3.84222 0.877075 0.877075 3.84222 0.877075 7.49991C0.877075 11.1576 3.84222 14.1227 7.49991 14.1227C11.1576 14.1227 14.1227 11.1576 14.1227 7.49991C14.1227 3.84222 11.1576 0.877075 7.49991 0.877075ZM1.82708 7.49991C1.82708 4.36689 4.36689 1.82707 7.49991 1.82707C10.6329 1.82707 13.1727 4.36689 13.1727 7.49991C13.1727 10.6329 10.6329 13.1727 7.49991 13.1727C4.36689 13.1727 1.82708 10.6329 1.82708 7.49991ZM8.37287 7.50006C8.37287 7.98196 7.98221 8.37263 7.5003 8.37263C7.01839 8.37263 6.62773 7.98196 6.62773 7.50006C6.62773 7.01815 7.01839 6.62748 7.5003 6.62748C7.98221 6.62748 8.37287 7.01815 8.37287 7.50006ZM9.32287 7.50006C9.32287 8.50664 8.50688 9.32263 7.5003 9.32263C6.49372 9.32263 5.67773 8.50664 5.67773 7.50006C5.67773 6.49348 6.49372 5.67748 7.5003 5.67748C8.50688 5.67748 9.32287 6.49348 9.32287 7.50006Z" fill="currentColor" fill-rule="evenodd" clip-rule="evenodd"></path></svg>',
        "Particles", "Template Match → Class3D"
    ),
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

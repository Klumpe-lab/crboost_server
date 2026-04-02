# services/jobs/__init__.py
"""
Per-job-type parameter models.

Canonical import path:  from services.jobs import FsMotionCtfParams
Legacy shim:            from services.job_models import FsMotionCtfParams  (still works)
"""

from services.jobs._base import (
    AbstractJobParams,
    ExtractionCutoffMethod,
    SymmetryGroup,
    TemplateWorkbenchState,
)
from services.jobs.import_movies import ImportMoviesParams
from services.jobs.fs_motion_ctf import FsMotionCtfParams
from services.jobs.ts_alignment import TsAlignmentParams
from services.jobs.ts_ctf import TsCtfParams
from services.jobs.ts_reconstruct import TsReconstructParams
from services.jobs.denoise_train import DenoiseTrainParams
from services.jobs.denoise_predict import DenoisePredictParams
from services.jobs.template_match import TemplateMatchPytomParams
from services.jobs.candidate_extract import CandidateExtractPytomParams
from services.jobs.subtomo_extraction import SubtomoExtractionParams
from services.jobs.reconstruct_particle import ReconstructParticleParams
from services.jobs.class3d import Class3DParams

from services.models_base import JobType
from typing import Dict, Type


def jobtype_paramclass() -> Dict[JobType, Type[AbstractJobParams]]:
    """Registry mapping JobType to its parameter class."""
    return {
        JobType.IMPORT_MOVIES: ImportMoviesParams,
        JobType.FS_MOTION_CTF: FsMotionCtfParams,
        JobType.TS_ALIGNMENT: TsAlignmentParams,
        JobType.TS_CTF: TsCtfParams,
        JobType.TS_RECONSTRUCT: TsReconstructParams,
        JobType.DENOISE_TRAIN: DenoiseTrainParams,
        JobType.DENOISE_PREDICT: DenoisePredictParams,
        JobType.TEMPLATE_MATCH_PYTOM: TemplateMatchPytomParams,
        JobType.TEMPLATE_EXTRACT_PYTOM: CandidateExtractPytomParams,
        JobType.SUBTOMO_EXTRACTION: SubtomoExtractionParams,
        JobType.RECONSTRUCT_PARTICLE: ReconstructParticleParams,
        JobType.CLASS3D: Class3DParams,
    }


__all__ = [
    "AbstractJobParams",
    "ExtractionCutoffMethod",
    "SymmetryGroup",
    "TemplateWorkbenchState",
    "ImportMoviesParams",
    "FsMotionCtfParams",
    "TsAlignmentParams",
    "TsCtfParams",
    "TsReconstructParams",
    "DenoiseTrainParams",
    "DenoisePredictParams",
    "TemplateMatchPytomParams",
    "CandidateExtractPytomParams",
    "SubtomoExtractionParams",
    "ReconstructParticleParams",
    "Class3DParams",
    "jobtype_paramclass",
]

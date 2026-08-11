# services/jobs/__init__.py
"""
Per-job-type parameter models.

Canonical import path:  from services.jobs import FsMotionCtfParams
Legacy shim:            from services.job_models import FsMotionCtfParams  (still works)
"""

from services.jobs._base import AbstractJobParams, ExtractionCutoffMethod, SymmetryGroup, TemplateWorkbenchState
from services.jobs.import_movies import ImportMoviesParams
from services.jobs.fs_motion_ctf import FsMotionCtfParams
from services.jobs.ts_import import TsImportParams
from services.jobs.ts_alignment import TsAlignmentParams
from services.jobs.miss_align import MissAlignParams
from services.jobs.ts_ctf import TsCtfParams
from services.jobs.tilt_filter import TiltFilterParams
from services.jobs.ts_reconstruct import TsReconstructParams
from services.jobs.denoise_train import DenoiseTrainParams
from services.jobs.denoise_predict import DenoisePredictParams
from services.jobs.template_match import TemplateMatchPytomParams
from services.jobs.candidate_extract import CandidateExtractPytomParams
from services.jobs.subtomo_extraction import SubtomoExtractionParams
from services.jobs.reconstruct_particle import ReconstructParticleParams
from services.jobs.class3d import Class3DParams

from collections.abc import Mapping

from services.jobs.spec import PARAM_CLASS_BY_TYPE
from services.models_base import JobType


def jobtype_paramclass() -> Mapping[JobType, type[AbstractJobParams]]:
    """Registry mapping JobType to its parameter class (read-only view, built once in services.jobs.spec)."""
    return PARAM_CLASS_BY_TYPE


__all__ = [
    "AbstractJobParams",
    "CandidateExtractPytomParams",
    "Class3DParams",
    "DenoisePredictParams",
    "DenoiseTrainParams",
    "ExtractionCutoffMethod",
    "FsMotionCtfParams",
    "ImportMoviesParams",
    "MissAlignParams",
    "ReconstructParticleParams",
    "SubtomoExtractionParams",
    "SymmetryGroup",
    "TemplateMatchPytomParams",
    "TemplateWorkbenchState",
    "TiltFilterParams",
    "TsAlignmentParams",
    "TsCtfParams",
    "TsImportParams",
    "TsReconstructParams",
    "jobtype_paramclass",
]

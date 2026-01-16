# services/models_base.py
from __future__ import annotations
from enum import Enum
from typing import Tuple, Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator

class JobStatus(str, Enum):
    SUCCEEDED = "Succeeded"
    FAILED    = "Failed"
    RUNNING   = "Running"
    SCHEDULED = "Scheduled"
    UNKNOWN   = "Unknown"

class MicroscopeType(str, Enum):
    KRIOS_G3 = "Krios_G3"
    KRIOS_G4 = "Krios_G4"
    GLACIOS  = "Glacios"
    TALOS    = "Talos"
    CUSTOM   = "Custom"

class AlignmentMethod(str, Enum):
    ARETOMO = "AreTomo"
    IMOD    = "IMOD"
    RELION  = "Relion"

class JobCategory(str, Enum):
    IMPORT     = "Import"
    EXTERNAL   = "External"
    MOTIONCORR = "MotionCorr"
    CTFFIND    = "CtfFind"

class JobType(str, Enum):
    IMPORT_MOVIES          = "importmovies"
    FS_MOTION_CTF          = "fsMotionAndCtf"
    TS_ALIGNMENT           = "aligntiltsWarp"
    TS_CTF                 = "tsCtf"
    TS_RECONSTRUCT         = "tsReconstruct"
    DENOISE_TRAIN          = "denoisetrain"
    DENOISE_PREDICT        = "denoisepredict"
    TEMPLATE_MATCH_PYTOM   = "templatematching"
    TEMPLATE_EXTRACT_PYTOM = "tmextractcand"
    SUBTOMO_EXTRACTION     = "subtomoExtraction"

    @classmethod
    def from_string(cls, value: str) -> "JobType":
        try:
            return cls(value)
        except ValueError:
            valid = [e.value for e in cls]
            raise ValueError(f"Unknown job type '{value}'. Valid types: {valid}")

class MicroscopeParams(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    microscope_type         : MicroscopeType = MicroscopeType.CUSTOM
    pixel_size_angstrom     : float          = Field(default=1.35, ge=0.5, le=10.0)
    acceleration_voltage_kv : float          = Field(default=300.0)
    spherical_aberration_mm : float          = Field(default=2.7, ge=0.0, le=10.0)
    amplitude_contrast      : float          = Field(default=0.1, ge=0.0, le=1.0)

class AcquisitionParams(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    dose_per_tilt           : float           = Field(default=3.0, ge=0.1, le=9.0)
    detector_dimensions     : Tuple[int, int] = (4096, 4096)
    tilt_axis_degrees       : float           = Field(default=-95.0, ge=-180.0, le=180.0)
    eer_fractions_per_frame : Optional[int]   = Field(default=None, ge=1, le=100)
    sample_thickness_nm     : float           = Field(default=300.0, ge=50.0, le=2000.0)
    gain_reference_path     : Optional[str]   = None
    invert_tilt_angles      : bool            = False
    invert_defocus_hand     : bool            = False
    acquisition_software    : str             = Field(default="SerialEM")
    nominal_magnification   : Optional[int]   = None
    spot_size               : Optional[int]   = None
    camera_name             : Optional[str]   = None
    binning                 : Optional[int]   = Field(default=1, ge=1)
    frame_dose              : Optional[float] = None
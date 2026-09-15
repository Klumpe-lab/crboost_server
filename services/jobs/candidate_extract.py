from __future__ import annotations
from typing import Any, ClassVar
from pydantic import Field, model_validator

from services.computing.slurm_service import SlurmConfig
from services.configs.config_service import get_config_service
from services.jobs._base import AbstractJobParams, ExtractionCutoffMethod
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class CandidateExtractPytomParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TEMPLATE_EXTRACT_PYTOM)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[set[str]] = {
        "particle_diameter_ang",
        "max_num_particles",
        "cutoff_method",
        "cc_threshold",
        "expected_false_positives",
        "apix_score_map",
        "score_filter_method",
        "score_filter_value",
        "array_throttle",
    }

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_tm_job", accepts=[JobFileType.TM_RESULTS_DIR], preferred_source="templatematching"),
        InputSlot(
            key="input_tomograms",
            accepts=[JobFileType.TOMOGRAMS_STAR, JobFileType.DENOISED_TOMOGRAMS_STAR],
            preferred_source="templatematching",
            required=True,
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.CANDIDATES_STAR, path_template="candidates.star"),
        OutputSlot(
            key="optimisation_set", produces=JobFileType.OPTIMISATION_SET_STAR, path_template="optimisation_set.star"
        ),
    ]

    # Particle params
    particle_diameter_ang: float = Field(default=200.0)
    # ge=1: PyTOM's `-n` is a required, strictly-positive cap ("error: -n must be larger
    # than 0", argparse exit 2); without the bound a stored 0 only fails on the GPU node.
    # There is no "0 = uncapped" spelling; leave the cap high instead.
    max_num_particles: int = Field(
        default=1500, ge=1, description="Hard cap on candidates per tilt-series after the cutoff is applied."
    )

    # Thresholding strategy + per-strategy values.
    #
    # The two strategies have incomparable scales: under FALSE_POSITIVES the value is
    # "expected FPs per tomogram" (e.g. 1.0 = strict), under MANUAL it is a raw LCC
    # threshold (e.g. 0.1). A shared field would carry 1.0 over to MANUAL on a dropdown
    # flip (CC ≥ 1.0 → 0 picks), so each strategy has its own backing field and the UI
    # shows only the one matching `cutoff_method`. Older project files carry a single
    # `cutoff_value`; `_migrate_legacy_cutoff_value` below routes it on load.
    cutoff_method: ExtractionCutoffMethod = Field(default=ExtractionCutoffMethod.FALSE_POSITIVES)
    cc_threshold: float = Field(
        default=0.1,
        description="Manual LCC threshold; peaks with score ≥ this are kept. Typical 0.05–0.20.",
    )
    expected_false_positives: float = Field(
        default=1.0,
        description="Expected false positives per tomogram (FALSE_POSITIVES strategy). "
        "Strict=1, moderate=10, loose=100.",
    )

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_cutoff_value(cls, data: Any) -> Any:
        """Migrate the legacy `cutoff_value` field into the appropriate
        strategy-specific field. Runs before field validation, so the raw
        dict from JSON load is what we mutate.

        Behavior:
          - If the JSON has `cutoff_value` and neither `cc_threshold` nor
            `expected_false_positives` is set, route the value into the
            field that matches `cutoff_method`.
          - If both old and new keys are present, the explicit new field wins.
          - The legacy key is always removed from the dict (Pydantic v2
            BaseModel already defaults to extra='ignore').
        """
        if not isinstance(data, dict):
            return data
        if "cutoff_value" not in data:
            return data
        legacy = data.pop("cutoff_value", None)
        if legacy is None:
            return data
        try:
            legacy_f = float(legacy)
        except (TypeError, ValueError):
            return data
        method_raw = data.get("cutoff_method") or ExtractionCutoffMethod.FALSE_POSITIVES.value
        method_str = method_raw.value if hasattr(method_raw, "value") else str(method_raw)
        if method_str == ExtractionCutoffMethod.MANUAL.value:
            data.setdefault("cc_threshold", legacy_f)
        else:
            data.setdefault("expected_false_positives", legacy_f)
        return data

    # Score map pixel size
    apix_score_map: str = Field(default="auto")

    # Filtering
    score_filter_method: str = Field(default="None")
    score_filter_value: str = Field(default="None")

    array_throttle: int = Field(
        default=16, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tomogram candidate extraction"
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_tm_job = self.paths.get("input_tm_job", "")
        return [("in_mic", str(input_tm_job))]

    def _get_queue_options(self) -> list[tuple[str, str]]:
        """
        Override: extract's parent sbatch is a lightweight CPU-only supervisor
        that enumerates tomograms, submits a per-tomogram SLURM array, polls,
        merges per-tomogram particle lists. Per-task resources (also CPU-only,
        small) are consumed by the supervisor when it builds the array sbatch
        -- NOT by this supervisor's own sbatch.
        """
        sup = get_config_service().supervisor_slurm_defaults
        options = [
            ("do_queue", "Yes"),
            ("queuename", sup.partition),
            ("qsub", "sbatch"),
            ("qsubscript", "qsub.sh"),
            ("min_dedicated", "1"),
        ]
        for field_name, var_name in SlurmConfig.QSUB_EXTRA_MAPPING.items():
            options.append((var_name, str(getattr(sup, field_name))))
        return options

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "pytom"

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"tm_job": "templatematching"}

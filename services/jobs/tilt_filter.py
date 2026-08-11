from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import ClassVar
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobStatus, JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType

logger = logging.getLogger(__name__)


class TiltFilterParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TILT_FILTER)

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_INTERACTIVE: ClassVar[bool] = True

    USER_PARAMS: ClassVar[set[str]] = {"model_name", "image_size", "dl_batch_size", "prob_threshold", "prob_action"}

    # Runs after tsImport, before alignment, so the cut actually filters
    # alignment/CTF/reconstruct instead of only the display copy. The DL reads
    # the motion-corrected averages via the fs-motion star; the verdict is applied
    # by trimming the tomostar (drivers/tilt_filter.py), which every downstream
    # WarpTools step reads. When this job is absent, alignment's tomostar_dir slot
    # falls back to tsImport's tomostar and the pipeline is unchanged.
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.FS_MOTION_CTF_STAR], preferred_source="fsMotionAndCtf"),
        InputSlot(key="input_tomostar", accepts=[JobFileType.TOMOSTAR_DIR], preferred_source="tsImport"),
    ]

    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(key="output_tomostar", produces=JobFileType.TOMOSTAR_DIR, path_template="tomostar/", is_dir=True)
    ]

    model_name: str = Field(default="default", description="DL model name for tilt quality classification")
    image_size: int = Field(default=384, ge=128, le=1024, description="Target image size for DL inference")
    dl_batch_size: int = Field(default=32, ge=1, le=256, description="Batch size for DL inference")
    prob_threshold: float = Field(default=0.1, ge=0.0, le=1.0, description="Probability threshold for classification")
    prob_action: str = Field(default="assignToGood", description="Action for low-confidence predictions")
    tilt_labels: dict[str, str] = Field(default_factory=dict, description="Manual good/bad label overrides by tilt key")

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "crboost"

    @staticmethod
    def get_output_assets(job_dir: Path) -> dict[str, Path]:
        return {
            "filtered_star": job_dir / "filtered" / "tiltseries_filtered.star",
            "labeled_star": job_dir / "filtered" / "tiltseries_labeled.star",
        }

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"ctf": "tsCtf"}


# ── commit-time output production (shared by the DL and manual-label paths) ──


def _find_tsimport_tomostar_dir(state, project_path: Path) -> Path | None:
    """Locate the tsImport job's `tomostar/` directory — the source the tilt filter
    trims (dropping bad-tilt rows) so alignment/CTF/reconstruct inherit the cut."""
    for _iid, jm in state.jobs.items():
        if jm.job_type and jm.job_type.value == "tsImport" and jm.execution_status == JobStatus.SUCCEEDED:
            d = jm.paths.get("tomostar_dir")
            if d:
                p = Path(d) if Path(d).is_absolute() else project_path / d
                if p.is_dir():
                    return p
            if jm.relion_job_name:
                p = project_path / jm.relion_job_name.rstrip("/") / "tomostar"
                if p.is_dir():
                    return p
    return None


async def finalize_pipeline_output(state, job_model, ts_data, project_path: Path) -> dict:
    """Produce the tilt filter's real pipeline output — a trimmed tomostar with the
    dropped tilts removed — that alignment/CTF/reconstruct consume. Runs at commit for
    BOTH the DL-assisted and manual-labelling paths (the SLURM driver only runs for the
    DL pass; manual labelling never dispatches it, so the trim must live here too).

    Sets `job_model.paths['output_tomostar']` so the path resolver wires alignment to
    it even though this interactive job has no deployed job dir (the resolver falls back
    to the producer's cached paths for SUCCEEDED interactive jobs). Also stamps the
    per-tilt verdict into the registry (authoritative record). Returns
    {"success", "error", "kept", "dropped"}; the UI caller surfaces the outcome."""
    from services.tilt_series_service import drop_tilts_from_tomostar

    src_tomostar = _find_tsimport_tomostar_dir(state, project_path)
    if src_tomostar is None:
        return {
            "success": False,
            "error": "Cannot finalize: tsImport tomostar not found (run Import + TS Import first).",
        }

    df = ts_data.all_tilts_df
    has_labels = "cryoBoostDlLabel" in df.columns and "cryoBoostKey" in df.columns
    bad_stems = set(df.loc[df["cryoBoostDlLabel"] != "good", "cryoBoostKey"].tolist()) if has_labels else set()

    out_tomostar = project_path / "TiltFilter" / "tomostar"
    kept, dropped = await asyncio.to_thread(drop_tilts_from_tomostar, src_tomostar, out_tomostar, bad_stems)

    job_model.paths["output_tomostar"] = str(out_tomostar)
    # Drop stale slots from the pre-move design so the resolver never wires them.
    job_model.paths.pop("output_star", None)
    job_model.paths.pop("output_processing", None)

    # Registry stamp — authoritative record; best-effort, never blocks the commit.
    try:
        from services.tilt_series import get_registry_for

        registry = get_registry_for(project_path)
        if registry.tilt_series_ids() and has_labels:
            probs = df["cryoBoostDlProbability"] if "cryoBoostDlProbability" in df.columns else [None] * len(df)
            for stem, is_filt, prob in zip(df["cryoBoostKey"], (df["cryoBoostDlLabel"] != "good"), probs, strict=False):
                try:
                    registry.set_frame_filtered(
                        str(stem),
                        bool(is_filt),
                        reason="tilt-filter" if is_filt else None,
                        probability=float(prob) if prob is not None else None,
                    )
                except KeyError:
                    pass
            await asyncio.to_thread(registry.save)
    except Exception as e:
        logger.warning("tilt-filter registry stamp skipped: %s", e)

    return {"success": True, "error": None, "kept": kept, "dropped": dropped}

from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import ClassVar
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.result import err, ok

logger = logging.getLogger(__name__)


class TiltFilterParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TILT_FILTER)

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_INTERACTIVE: ClassVar[bool] = True

    USER_PARAMS: ClassVar[set[str]] = {"model_name", "image_size", "dl_batch_size", "prob_threshold", "prob_action"}

    # The DL reads the motion-corrected averages via the fs-motion star; that is this
    # job's only input. The verdict is a per-frame `is_filtered_out` stamp in the
    # TiltSeries registry, NOT a file this job produces: alignment applies the cut when
    # it snapshots the tomostar dir (drivers/ts_alignment.py), and CTF/reconstruct
    # inherit it from that snapshot.
    #
    # Deliberately NOT a TOMOSTAR_DIR producer. It used to consume tsImport's tomostar
    # and emit a trimmed copy, which forced an ordering this interactive job cannot
    # honour -- the user reaches the gallery as soon as fsMotion's PNGs exist, which is
    # routinely before tsImport has run -- and it put a phantom `pending_tiltFilter`
    # entry in alignment's tomostar source menu that a pending filter resolved to
    # silently, running alignment on the unfiltered tilt set. With the verdict in the
    # registry, tsImport is the sole tomostar producer, the commit has no upstream
    # dependency at all, and filter-off vs filter-on differ only by the drop set.
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.FS_MOTION_CTF_STAR], preferred_source="fsMotionAndCtf")
    ]

    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = []

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


# ── commit-time verdict (shared by the DL and manual-label paths) ──


async def finalize_pipeline_output(state, job_model, ts_data, project_path: Path) -> dict:
    """Commit the tilt-filter verdict: stamp every tilt's keep/drop decision into the
    TiltSeries registry, which is what alignment reads when it snapshots the tomostar
    dir. Runs at commit for BOTH the DL-assisted and manual-labelling paths (the SLURM
    driver only runs for the DL pass; manual labelling never dispatches it).

    Deliberately has no upstream dependency: the registry exists from import onward, so
    the user can commit before, after, or without tsImport having run. Re-stamps in both
    directions on every commit, so un-labelling a tilt restores it. Downstream jobs pick
    the new verdict up when they are (re)queued -- the forward-only staleness convention.

    Returns ok(kept=..., dropped=...) or err(...); the UI caller surfaces the outcome.
    A failure here means the cut was NOT recorded, so the caller must not mark the job
    succeeded."""
    from services.tilt_series import get_registry_for

    df = ts_data.all_tilts_df
    if "cryoBoostDlLabel" not in df.columns or "cryoBoostKey" not in df.columns:
        return err("Cannot commit: the tilt table has no labels (expected cryoBoostKey + cryoBoostDlLabel columns).")

    try:
        registry = get_registry_for(project_path)
    except Exception:
        logger.exception("tilt-filter commit: registry unavailable")
        return err("Cannot commit: the TiltSeries registry could not be loaded. Reload the project and retry.")

    if not registry.tilt_series_ids():
        return err("Cannot commit: the TiltSeries registry is empty. Reload the project to backfill it from mdocs.")

    probs = df["cryoBoostDlProbability"] if "cryoBoostDlProbability" in df.columns else [None] * len(df)
    kept = dropped = 0
    unknown: list[str] = []
    for stem, is_filt, prob in zip(df["cryoBoostKey"], (df["cryoBoostDlLabel"] != "good"), probs, strict=False):
        try:
            registry.set_frame_filtered(
                str(stem),
                bool(is_filt),
                reason="tilt-filter" if is_filt else None,
                probability=float(prob) if prob is not None else None,
            )
        except KeyError:
            # A labelled tilt the registry has never heard of means the verdict for it
            # would be lost silently -- report it rather than quietly under-filtering.
            unknown.append(str(stem))
            continue
        if is_filt:
            dropped += 1
        else:
            kept += 1

    if unknown:
        shown = ", ".join(unknown[:3]) + (f" (+{len(unknown) - 3} more)" if len(unknown) > 3 else "")
        return err(
            f"Cannot commit: {len(unknown)} labelled tilts are not in the registry ({shown}). Reload the project."
        )

    try:
        await asyncio.to_thread(registry.save)
    except Exception:
        logger.exception("tilt-filter commit: registry save failed")
        return err("Cannot commit: writing the tilt verdict to the registry failed. See the server log.")

    # Stale slots from the pre-registry design, when this job produced its own tomostar.
    for dead in ("output_tomostar", "output_star", "output_processing"):
        job_model.paths.pop(dead, None)

    return ok(kept=kept, dropped=dropped)

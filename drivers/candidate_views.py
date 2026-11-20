"""The views of a Pick candidates job's picks, from either source: the IMOD models and the candidate preview PNGs
the Journey shows. Convenience artifacts made once the picks are complete: a failure is reported and never fails a
job whose scientific outputs are written."""

from collections.abc import Callable
from pathlib import Path

from drivers.driver_base import DriverContext


def write_candidate_views(
    ctx: DriverContext, candidates_star: Path, tomograms_star: Path, log: Callable[[str], None]
) -> None:
    try:
        from services.visualization.imod_vis import generate_candidate_vis

        log("Generating IMOD visualization...")
        generate_candidate_vis(
            candidates_star=candidates_star,
            tomograms_star=tomograms_star,
            particle_diameter_ang=float(ctx.params.particle_diameter_ang),
            output_dir=ctx.job_dir,
            project_root=ctx.project_path,
        )
    except Exception as vis_err:
        # A convenience artifact: its failure must not fail a job whose outputs are complete.
        print(f"[SUPERVISOR WARN] Visualization generation failed (non-fatal): {vis_err}", flush=True)

    try:
        from services.visualization.preview_orchestrator import generate_candidate_previews

        log("Rendering candidate preview PNGs...")
        # `project_state` lets the orchestrator walk this project's SUBTOMO_EXTRACTION jobs and build a per-pick
        # cutout atlas; without it every entry gets cutout_atlas=None, and the dashboard reports the atlas as not
        # built even when the subtomo extraction completed and coordinates match.
        preview_summary = generate_candidate_previews(
            candidates_star=candidates_star,
            tomograms_star=tomograms_star,
            particle_diameter_ang=float(ctx.params.particle_diameter_ang),
            output_dir=ctx.job_dir,
            project_root=ctx.project_path,
            project_state=ctx.state,
            instance_id=ctx.instance_id,
            job_model=ctx.params,
        )
        log(
            "Previews: "
            f"{len(preview_summary['ok'])} rendered, "
            f"{len(preview_summary['skipped_cached'])} cached, "
            f"{len(preview_summary['missing_volume'])} missing volume, "
            f"{len(preview_summary['errored'])} errored"
        )
    except Exception as preview_err:
        # Same convenience-artifact reasoning as the IMOD visualization.
        print(f"[SUPERVISOR WARN] Preview rendering failed (non-fatal): {preview_err}", flush=True)

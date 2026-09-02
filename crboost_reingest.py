#!/usr/bin/env python3
"""crboost_reingest — re-run the registry ingest of completed preprocessing jobs
from the artifacts already in their job dirs. No recompute, no STAR re-emit.

    venv/bin/python3 crboost_reingest.py /path/to/project              # every succeeded fsMotion / tsAlignment / tsCtf
    venv/bin/python3 crboost_reingest.py /path/to/project --job tsCtf  # one job type (repeatable)

Why: the registry gains per-tilt QC fields over time (registry schema 1.4 — motion
tracks, defocus spread, tilt intensity / FOV / masked fraction, TS-level CTF fit
resolution + plane normal — the things the Journey charts). A project processed
before a field existed shows nothing for it until the adapters read the Warp XMLs,
tomostars and .aln files again; this does exactly that and saves the registry.
Run from the repo root in the server's environment (the module block of
config/qsub.sh). Exit 0 when every eligible job re-ingested, 1 otherwise.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO))

from services.array_tasks import STATUS_DIR_NAME  # noqa: E402
from services.models_base import JobStatus, JobType  # noqa: E402
from services.project_state import ProjectState  # noqa: E402
from services.tilt_series import get_registry_for  # noqa: E402
from services.tilt_series.adapters import (  # noqa: E402
    FsMotionCtfIngestAdapter,
    TsAlignmentIngestAdapter,
    TsCtfIngestAdapter,
)

REINGESTABLE = (JobType.FS_MOTION_CTF, JobType.TS_ALIGNMENT, JobType.TS_CTF)


def _succeeded_items(job_dir: Path) -> set[str]:
    """TS ids the array job finished (`.ok` status files) — the same set the
    driver's aggregate step handed to `ingest()`."""
    status_dir = job_dir / STATUS_DIR_NAME
    return {p.stem for p in status_dir.glob("*.ok")} if status_dir.is_dir() else set()


def _reingest(jt: JobType, jm, registry, job_dir: Path, instance_id: str, ts_ids: set[str]) -> None:
    if jt == JobType.FS_MOTION_CTF:
        FsMotionCtfIngestAdapter(
            registry=registry, job_dir=job_dir, job_instance_id=instance_id, warp_folder="warp_frameseries"
        ).ingest(ts_ids)
    elif jt == JobType.TS_ALIGNMENT:
        TsAlignmentIngestAdapter(registry=registry, job_dir=job_dir, job_instance_id=instance_id).ingest(
            ts_ids, alignment_method=jm.alignment_method, alignment_angpix=jm.rescale_angpixs
        )
    elif jt == JobType.TS_CTF:
        TsCtfIngestAdapter(
            registry=registry, job_dir=job_dir, job_instance_id=instance_id, warp_folder="warp_tiltseries"
        ).ingest(ts_ids)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("project", type=Path, help="project directory (holds project_params.json + registry/)")
    parser.add_argument(
        "--job",
        action="append",
        choices=[jt.value for jt in REINGESTABLE],
        help="restrict to one job type (repeatable); default: all three",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    project = args.project.resolve()
    state = ProjectState.load(project / "project_params.json")
    registry = get_registry_for(project)
    if not registry.tilt_series_ids():
        print(f"registry is empty for {project} — open the project in the UI once to build it from the mdocs")
        return 1
    wanted = {JobType(v) for v in args.job} if args.job else set(REINGESTABLE)

    failures = 0
    for instance_id, jm in state.jobs.items():
        jt = jm.job_type
        if jt not in wanted or jm.execution_status != JobStatus.SUCCEEDED or not jm.relion_job_name:
            continue
        job_dir = project / jm.relion_job_name
        ts_ids = _succeeded_items(job_dir)
        if not ts_ids:
            print(f"{instance_id}: no .ok items under {job_dir / STATUS_DIR_NAME} — skipped")
            continue
        try:
            _reingest(jt, jm, registry, job_dir, instance_id, ts_ids)
            print(f"{instance_id}: re-ingested {len(ts_ids)} tilt-series from {job_dir}")
        except Exception as e:  # report and carry on — one job's problem must not hide the others'
            failures += 1
            print(f"{instance_id}: FAILED — {e}")
    registry.save()
    print(f"registry saved: {registry.registry_dir}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

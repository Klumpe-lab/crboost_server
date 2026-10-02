#!/usr/bin/env python3
"""crboost_reingest — re-run the registry ingest of completed preprocessing jobs
from the artifacts already in their job dirs. No recompute, no STAR re-emit.

    venv/bin/python3 crboost_reingest.py /path/to/project              # every succeeded fsMotion / tsAlignment / tsCtf
    venv/bin/python3 crboost_reingest.py /path/to/project --job tsCtf  # one job type (repeatable)
    venv/bin/python3 crboost_reingest.py /path/to/project --mdoc       # mdoc acquisition fields only (+ --job: both)

Why: the registry gains per-tilt QC fields over time (registry schema 1.4 — motion
tracks, defocus spread, tilt intensity / FOV / masked fraction, TS-level CTF fit
resolution + plane normal — the things the Journey charts). A project processed
before a field existed shows nothing for it until the adapters read the Warp XMLs,
tomostars and .aln files again; this does exactly that and saves the registry.

`--mdoc` fills each frame's unset mdoc acquisition fields (mean counts, dose, exposure time,
defocus, image shift) from its tilt series' mdoc, as import writes them; a recorded value is
never replaced. Frames match mdoc sections by the SubFramePath basename; a frame without a
section and an unreadable mdoc are named. The Tomograms view's dark-exposure markers read
the counts. Run it with the server stopped: a server holding the registry can save a tilt
series over the filled one.
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
from services.configs.mdoc_service import get_mdoc_service  # noqa: E402
from services.models_base import JobStatus, JobType  # noqa: E402
from services.project_state import ProjectState  # noqa: E402
from services.tilt_series import get_registry_for  # noqa: E402
from services.tilt_series.adapters import (  # noqa: E402
    FsMotionCtfIngestAdapter,
    TsAlignmentIngestAdapter,
    TsCtfIngestAdapter,
)
from services.tilt_series.build import _acq_kwargs_from_raw_section  # noqa: E402

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


def _fill_from_mdocs(registry) -> int:
    """Fill every frame's unset acquisition fields from its tilt series' mdoc. Returns the
    number of tilt series with a problem: an unreadable mdoc, or frames it has no section for."""
    svc = get_mdoc_service()
    problems = 0
    for ts in sorted(registry.all_tilt_series(), key=lambda t: t.id):
        mdoc = Path(ts.mdoc_path)
        try:
            sections = svc.parse_mdoc_file(mdoc)["data"]
        except (OSError, ValueError, IndexError) as e:  # missing, undecodable or malformed: this series' problem
            problems += 1
            print(f"{ts.id}: mdoc not readable at {mdoc} — {e}")
            continue
        by_name = {
            Path(sec["SubFramePath"].replace("\\", "/")).name: sec for sec in sections if sec.get("SubFramePath")
        }
        filled = 0
        unmatched: list[str] = []
        for frame in ts.frames:
            sec = by_name.get(frame.raw_filename)
            if sec is None:
                unmatched.append(frame.raw_filename)
            elif registry.fill_frame_acquisition(frame.id, _acq_kwargs_from_raw_section(sec)):
                filled += 1
        print(f"{ts.id}: filled {filled} of {len(ts.frames)} frames from {mdoc.name}")
        if unmatched:
            problems += 1
            shown = ", ".join(unmatched[:3]) + (f" (+{len(unmatched) - 3} more)" if len(unmatched) > 3 else "")
            print(f"{ts.id}: {len(unmatched)} frames have no section in {mdoc.name}: {shown}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("project", type=Path, help="project directory (holds project_params.json + registry/)")
    parser.add_argument(
        "--job",
        action="append",
        choices=[jt.value for jt in REINGESTABLE],
        help="restrict to one job type (repeatable); default: all three, unless --mdoc is given alone",
    )
    parser.add_argument(
        "--mdoc",
        action="store_true",
        help="fill each frame's unset mdoc acquisition fields from its tilt series' mdoc; alone, or with --job",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    project = args.project.resolve()
    state = ProjectState.load(project / "project_params.json")
    registry = get_registry_for(project)
    if not registry.tilt_series_ids():
        print(f"registry is empty for {project} — open the project in the UI once to build it from the mdocs")
        return 1
    if args.job:
        wanted = {JobType(v) for v in args.job}
    else:
        wanted = set() if args.mdoc else set(REINGESTABLE)

    failures = _fill_from_mdocs(registry) if args.mdoc else 0
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

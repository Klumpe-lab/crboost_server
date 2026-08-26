"""Per-pick-list subtomogram extraction (roadmap 07).

Given ONE curation pick list's coordinate star, build a per-list ``optimisation_set``
and run ``relion_tomo_subtomo`` on it, landing the extracted particles + a final
``optimisation_set.star`` under ``<job-dir>/out/`` so each curated list extracts
independently and never overwrites another.

The optset is built one of two ways, and exactly one source is set on the instance:
``candidate_optset`` mirrors the species' candidate-extract schema
(``list_extraction.build_list_optset``); ``tomograms_star`` synthesizes the schema and
optics for a de-novo species that has no candidate-extract job
(``build_list_optset_from_tomograms``).

Bootstrapped like every other driver — ``get_driver_context(ExtractPickListParams)`` off
``--instance_id``/``--project_path`` — but it is NOT a scheme job: ``backend.extract_pick_list``
submits it as a one-off through ``config/qsub.sh`` (one tomogram, one list), and its instance
is kept out of the pipeline sweeps by ``IS_INTERACTIVE``. ``Path.cwd()`` is the job dir, which
qsub sets to the list's out dir, so ``job_dir`` IS ``Curation/<species>/<tomo>/<slug>/``.

The qsub wrapper touches ``RELION_JOB_EXIT_SUCCESS/FAILURE`` — this driver deliberately does
NOT (two writers would contradict each other); it signals through its exit code. It DOES write
``<job-dir>/result.json`` (``{ok, optimisation_set, particles, count}`` or ``{ok: false, error}``),
the contract the awaiter reads to record ``PickList.mark_extracted`` and to surface a failure
(census #69/#70) — including when the bootstrap itself fails, which is why the result.json write
wraps the bootstrap and not just the run.

The relion command + container wrapping mirror ``drivers/subtomo_extraction.py`` exactly.
"""

from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import ToolCommand, get_driver_context, run_tool
from services.job_models import ExtractPickListParams
from services.particles.list_extraction import (
    build_list_optset,
    build_list_optset_from_tomograms,
    write_extracted_optset,
)


def _build_subtomo_cmd(optset: Path, out_run: Path, params: ExtractPickListParams) -> ToolCommand:
    """relion_tomo_subtomo command for ONE list's optset — mirrors the per-TS command
    in drivers/subtomo_extraction.py so a list extracts with the SAME box/bin/crop as
    the species' auto extraction (downstream refinement can mix the two)."""
    cmd = (
        ToolCommand("relion_tomo_subtomo")
        .opt_path("--o", f"{out_run}/", quote=False)
        .opt_path("--i", optset, quote=False)
        .opt("--b", params.box_size)
        .opt("--bin", int(params.binning))
    )
    if params.crop_size and params.crop_size > 0:
        cmd.opt("--crop", params.crop_size)
    if params.max_dose and params.max_dose > 0:
        cmd.opt("--max_dose", params.max_dose)
    if params.min_frames and params.min_frames > 1:
        cmd.opt("--min_frames", params.min_frames)
    if params.do_stack2d:
        cmd.flag("--stack2d")
    if params.do_float16:
        cmd.flag("--float16")
    return cmd


def _run(params: ExtractPickListParams, job_dir: Path, project_path: Path) -> dict:
    # 1) Per-list INPUT optimisation_set: mirror the species' candidate schema, or —
    #    for a de-novo species with no candidate-extract job — synthesize it from the
    #    tomograms.star. The submit site guarantees exactly one source is set.
    if params.tomograms_star:
        source_dir = Path(params.tomograms_star).resolve().parent
        prep = build_list_optset_from_tomograms(
            Path(params.list_star), params.tomo_name, job_dir, Path(params.tomograms_star)
        )
    elif params.candidate_optset:
        source_dir = Path(params.candidate_optset).resolve().parent
        prep = build_list_optset(Path(params.candidate_optset), Path(params.list_star), params.tomo_name, job_dir)
    else:
        raise ValueError(
            f"Instance has neither candidate_optset nor tomograms_star set — nothing to build the "
            f"extraction schema from (list {params.species_id}/{params.tomo_name}/{params.list_slug})"
        )
    input_optset = Path(prep["optimisation_set"])
    tomograms_star = Path(prep["tomograms"])
    n = int(prep["count"])
    print(f"[extract-list] built input optset ({n} picks): {input_optset}", flush=True)

    # 2) relion_tomo_subtomo into out/ (idempotent: skip if already extracted).
    out_run = job_dir / "out"
    out_run.mkdir(parents=True, exist_ok=True)
    if (out_run / "particles.star").exists() and (out_run / "Subtomograms").exists():
        print("[extract-list] outputs already present — skipping relion_tomo_subtomo", flush=True)
    else:
        cmd = _build_subtomo_cmd(input_optset, out_run, params)
        print(f"[DRIVER] Command: {cmd}", flush=True)
        # Bind the project tree (tilt series / tomograms the optset points at), the
        # out dir, and the tomograms.star + schema-source dirs (may sit outside it).
        binds = sorted(
            {str(project_path.resolve()), str(job_dir.resolve()), str(tomograms_star.parent.resolve()), str(source_dir)}
        )
        run_tool(cmd, tool_name=params.get_tool_name(), cwd=out_run, binds=binds)
        if not (out_run / "particles.star").exists():
            raise RuntimeError(f"relion_tomo_subtomo produced no particles.star in {out_run}")
        if not (out_run / "Subtomograms").exists():
            raise RuntimeError(f"relion_tomo_subtomo produced no Subtomograms/ in {out_run}")

    # 3) Final optimisation_set the dashboard records as this list's extracted_path.
    final_optset = write_extracted_optset(out_run, tomograms_star)
    print(f"[extract-list] wrote extracted optset: {final_optset}", flush=True)
    return {"ok": True, "optimisation_set": str(final_optset), "particles": str(out_run / "particles.star"), "count": n}


def main() -> None:
    # qsub.sh cd's into the list's out dir before invoking us, so cwd IS the job dir.
    # Resolve it BEFORE the bootstrap: get_driver_context exits the process on a missing
    # or mistyped instance, and a failure with no result.json is exactly the silent hole
    # census #69/#70 exist to prevent — the awaiter would see only an exit marker.
    out_dir = Path.cwd().resolve()
    result: dict
    try:
        (_state, params, _context, job_dir, project_path, _job_type) = get_driver_context(ExtractPickListParams)
    except SystemExit as e:
        result = {"ok": False, "error": f"driver bootstrap failed (see run.err): exit {e.code}"}
        _write_result(out_dir, result)
        sys.exit(1)
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        result = {"ok": False, "error": f"driver bootstrap failed: {e}"}
        _write_result(out_dir, result)
        sys.exit(1)

    try:
        job_dir.mkdir(parents=True, exist_ok=True)
        result = _run(params, job_dir, project_path)
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        result = {"ok": False, "error": str(e)}
    _write_result(job_dir, result)
    sys.exit(0 if result.get("ok") else 1)


def _write_result(out_dir: Path, result: dict) -> None:
    """Best-effort result.json — the dashboard contract (census #69). Never raises: a
    failure to record the outcome must not mask the outcome itself (the exit code stands)."""
    try:
        (out_dir / "result.json").write_text(json.dumps(result))
    except OSError:
        traceback.print_exc(file=sys.stderr)


if __name__ == "__main__":
    main()

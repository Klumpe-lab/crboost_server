"""Standalone per-pick-list subtomogram extraction (Slice C).

Runs OUTSIDE the pipeline graph: given a curation pick list's coordinate star, build
a per-list ``optimisation_set`` (mirroring the species' candidate-extract schema via
``services.visualization.list_extraction.build_list_optset``) and run
``relion_tomo_subtomo`` on it, landing the extracted particles + a final
``optimisation_set.star`` under ``<out-dir>/out/`` so each curated list extracts
independently and never overwrites another.

Submitted as a one-off SLURM job by ``backend.extract_pick_list`` (NOT an array, NOT a
pipeline job — one tomogram, one list, via ``config/qsub.sh`` like ``drivers/tilt_filter.py``).
The qsub wrapper touches ``RELION_JOB_EXIT_SUCCESS/FAILURE`` in the out-dir; this driver
ALSO writes ``<out-dir>/result.json`` (``{ok, optimisation_set, particles, count}`` or
``{ok: false, error}``) so the dashboard can record ``PickList.mark_extracted`` on success.

The relion command + container wrapping mirror ``drivers/subtomo_extraction.py`` exactly.
py_compile + ruff in Claude's venv; runtime-tested by the user in the module env.
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import run_command  # noqa: E402
from services.computing.container_service import get_container_service  # noqa: E402
from services.visualization.list_extraction import build_list_optset, write_extracted_optset  # noqa: E402


def _build_subtomo_cmd(optset: Path, out_run: Path, args) -> str:
    """relion_tomo_subtomo command for ONE list's optset — mirrors the per-TS command
    in drivers/subtomo_extraction.py so a list extracts with the SAME box/bin/crop as
    the species' auto extraction (downstream refinement can mix the two)."""
    cmd = [
        "relion_tomo_subtomo",
        "--o",
        str(out_run) + "/",
        "--i",
        str(optset),
        "--b",
        str(args.box),
        "--bin",
        str(int(args.binning)),
    ]
    if args.crop and args.crop > 0:
        cmd += ["--crop", str(args.crop)]
    if args.max_dose and args.max_dose > 0:
        cmd += ["--max_dose", str(args.max_dose)]
    if args.min_frames and args.min_frames > 1:
        cmd += ["--min_frames", str(args.min_frames)]
    if args.stack2d:
        cmd.append("--stack2d")
    if args.float16:
        cmd.append("--float16")
    return " ".join(cmd)


def _run(args) -> dict:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Per-list INPUT optimisation_set (candidate schema + this list's coords).
    prep = build_list_optset(Path(args.candidate_optset), Path(args.list_star), args.tomo, out_dir)
    input_optset = Path(prep["optimisation_set"])
    tomograms_star = Path(prep["tomograms"])
    n = int(prep["count"])
    print(f"[extract-list] built input optset ({n} picks): {input_optset}", flush=True)

    # 2) relion_tomo_subtomo into out/ (idempotent: skip if already extracted).
    out_run = out_dir / "out"
    out_run.mkdir(parents=True, exist_ok=True)
    if (out_run / "particles.star").exists() and (out_run / "Subtomograms").exists():
        print("[extract-list] outputs already present — skipping relion_tomo_subtomo", flush=True)
    else:
        cmd_str = _build_subtomo_cmd(input_optset, out_run, args)
        print(f"[extract-list] command: {cmd_str}", flush=True)
        # Bind the project tree (tilt series / tomograms the optset points at), the
        # out dir, and the tomograms.star + candidate-optset dirs (may sit outside it).
        binds = sorted(
            {
                str(Path(args.project_root).resolve()),
                str(out_dir.resolve()),
                str(tomograms_star.parent.resolve()),
                str(Path(args.candidate_optset).resolve().parent),
            }
        )
        wrapped = get_container_service().wrap_command_for_tool(
            cmd_str, cwd=out_run, tool_name="relion", additional_binds=binds
        )
        run_command(wrapped, cwd=out_run)
        if not (out_run / "particles.star").exists():
            raise RuntimeError(f"relion_tomo_subtomo produced no particles.star in {out_run}")
        if not (out_run / "Subtomograms").exists():
            raise RuntimeError(f"relion_tomo_subtomo produced no Subtomograms/ in {out_run}")

    # 3) Final optimisation_set the dashboard records as this list's extracted_path.
    final_optset = write_extracted_optset(out_run, tomograms_star)
    print(f"[extract-list] wrote extracted optset: {final_optset}", flush=True)
    return {"ok": True, "optimisation_set": str(final_optset), "particles": str(out_run / "particles.star"), "count": n}


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract one curation pick list's subtomograms (relion_tomo_subtomo).")
    ap.add_argument("--candidate-optset", required=True, help="species' candidate-extract optimisation_set.star")
    ap.add_argument("--list-star", required=True, help="the pick list's coords star (prefer its _filtered.star)")
    ap.add_argument("--tomo", required=True, help="rlnTomoName to extract")
    ap.add_argument("--out-dir", required=True, help="Curation/<species>/<tomo>/<slug>/")
    ap.add_argument("--project-root", required=True, help="project root, bound into the container")
    ap.add_argument("--box", type=int, default=384)
    ap.add_argument("--binning", type=float, default=1.0)
    ap.add_argument("--crop", type=int, default=224)
    ap.add_argument("--max-dose", dest="max_dose", type=float, default=-1.0)
    ap.add_argument("--min-frames", dest="min_frames", type=int, default=1)
    ap.add_argument("--stack2d", action="store_true")
    ap.add_argument("--float16", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        result = _run(args)
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        result = {"ok": False, "error": str(e)}
    try:
        (out_dir / "result.json").write_text(json.dumps(result))
    except Exception:
        traceback.print_exc(file=sys.stderr)
    sys.exit(0 if result.get("ok") else 1)


if __name__ == "__main__":
    main()

"""What a Refine 3D or a PostProcess job measured, read from its RELION star files: the Fourier shell correlation
curves by spatial frequency (1/Å) and the resolution RELION reports, said with what it is (gold-standard of the
unmasked half maps, or masked and corrected after PostProcess). Plain functions over a job directory, for a thread.
"""

from __future__ import annotations

import logging
from pathlib import Path
from collections.abc import Sequence
from typing import Any

from services.result import ErrorCode, err, ok

logger = logging.getLogger(__name__)

# PostProcess's FSC table: the columns it writes, by the name the plot gives each curve.
_POSTPROCESS_CURVES = (
    ("masked, corrected", "rlnFourierShellCorrelationCorrected"),
    ("unmasked", "rlnFourierShellCorrelationUnmaskedMaps"),
    ("masked", "rlnFourierShellCorrelationMaskedMaps"),
    ("phase-randomised, masked", "rlnCorrectedFourierShellCorrelationPhaseRandomizedMaskedMaps"),
)


def crossing(freq: Sequence[float], fsc: Sequence[float], at: float) -> float | None:
    """The first spatial frequency (1/Å) where the curve drops below `at`, interpolated; None when it never does."""
    for i in range(1, len(fsc)):
        if freq[i] > 0 and fsc[i - 1] >= at > fsc[i]:
            t = (fsc[i - 1] - at) / ((fsc[i - 1] - fsc[i]) or 1)
            return freq[i - 1] + t * (freq[i] - freq[i - 1])
    return None


def _star(path: Path) -> dict:
    import starfile

    return starfile.read(path, always_dict=True)


def _num(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f and f not in (float("inf"), float("-inf")) else None


def _failure(job_dir: Path) -> str | None:
    """The last line of the job's error log when RELION marked it failed."""
    if not (job_dir / "RELION_JOB_EXIT_FAILURE").exists():
        return None
    err_log = job_dir / "run.err"
    lines = err_log.read_text(errors="replace").splitlines() if err_log.exists() else []
    return next((x.strip() for x in reversed(lines) if x.strip()), "RELION marked the job failed")


def _optimiser_facts(model: Path) -> dict[str, Any]:
    """From the optimiser star beside a model star: the resolution below which RELION averaged the two half maps
    (`--low_resol_join_halves`; their FSC is 1 there by construction) and whether it refined filaments."""
    name = model.name.replace("_half1_model.star", "_optimiser.star").replace("_model.star", "_optimiser.star")
    path = model.with_name(name)
    if not path.exists():
        return {"joined_below": None, "helical": False}
    try:
        general = _star(path).get("optimiser_general") or {}
    except (OSError, ValueError):
        logger.exception("refine3d readout: %s", path)
        return {"joined_below": None, "helical": False}
    joined = _num(general.get("rlnJoinHalvesUntilThisResolution"))
    return {
        "joined_below": joined if joined and joined > 0 else None,
        "helical": bool(general.get("rlnDoHelicalRefine")),
    }


def refine3d_fsc(job_dir: Path) -> dict[str, Any]:
    """The gold-standard FSC of a Refine 3D: of the converged run, else of its last iteration so far."""
    job_dir = Path(job_dir)
    model = job_dir / "run_model.star"
    converged = model.exists()
    if not converged:
        halves = sorted(job_dir.glob("run_it*_half1_model.star"))
        if not halves:
            failure = _failure(job_dir)
            if failure:
                return err(failure)
            return err("No iteration written yet.", code=ErrorCode.NOT_WRITTEN_YET)
        model = halves[-1]
    try:
        blocks = _star(model)
    except (OSError, ValueError) as e:
        logger.exception("refine3d readout: %s", model)
        return err(f"Could not read {model.name}: {e}")
    general = blocks.get("model_general") or {}
    curve = blocks.get("model_class_1")
    curves = {}
    if curve is not None and {"rlnResolution", "rlnGoldStandardFsc"} <= set(getattr(curve, "columns", ())):
        curves["gold-standard"] = {
            "freq": [float(x) for x in curve["rlnResolution"]],
            "fsc": [float(x) for x in curve["rlnGoldStandardFsc"]],
        }
    apix = _num(general.get("rlnPixelSize"))
    return ok(
        kind="refine3d",
        source=model.name,
        converged=converged,
        resolution=_num(general.get("rlnCurrentResolution")),
        what="gold-standard FSC of the unmasked half maps at 0.143",
        apix=apix,
        box=int(general.get("rlnOriginalImageSize") or 0),
        nyquist=2 * apix if apix else None,
        curves=curves,
        failure=_failure(job_dir),
        **_optimiser_facts(model),
    )


def postprocess_fsc(job_dir: Path) -> dict[str, Any]:
    """PostProcess's FSC curves and its final resolution and B-factor."""
    job_dir = Path(job_dir)
    star = job_dir / "postprocess.star"
    if not star.exists():
        failure = _failure(job_dir)
        if failure:
            return err(failure)
        return err("postprocess.star is not written yet.", code=ErrorCode.NOT_WRITTEN_YET)
    try:
        blocks = _star(star)
    except (OSError, ValueError) as e:
        logger.exception("postprocess readout: %s", star)
        return err(f"Could not read postprocess.star: {e}")
    general = blocks.get("general") or {}
    table = blocks.get("fsc")
    curves = {}
    cols = set(getattr(table, "columns", ()))
    if table is not None and "rlnResolution" in cols:
        freq = [float(x) for x in table["rlnResolution"]]
        for name, col in _POSTPROCESS_CURVES:
            if col in cols:
                curves[name] = {"freq": freq, "fsc": [float(x) for x in table[col]]}
    return ok(
        kind="postprocess",
        source=star.name,
        converged=True,
        resolution=_num(general.get("rlnFinalResolution")),
        what="masked, corrected FSC at 0.143",
        bfactor=_num(general.get("rlnBfactorUsedForSharpening")),
        apix=None,
        box=0,
        nyquist=None,
        curves=curves,
        failure=None,
    )


__all__ = ["crossing", "postprocess_fsc", "refine3d_fsc"]

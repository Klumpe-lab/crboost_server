"""Recorded-baseline bands (roadmap 14 §verdicts; FEATURE_recipes.md §4).

`test/bands.yaml`:

    version: 1
    stages:
      tsImport:
        n_tilts:  {exact: 28}
        tilt_ids: {exact: [..28 stems..]}
      tmextractcand:
        n_candidates: {min: 790, max: 840, mean: 816.3, std: 9.1}   # mean/std only when n >= 3

Verdicts per metric: PASS · FAIL (outside min/max, or ≠ exact, or the metric is MISSING) ·
DRIFT (inside the band but > DRIFT_K·std from the recorded mean — slow rot stays visible).
`propose_bands` writes observed min/max (no widening, no invented tolerances) and exact
values where every record agrees; the human edits and accepts."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import yaml

from services.protocols.schema import Protocol

BANDS_FILE = "bands.yaml"
DRIFT_K = 3.0


def load_bands(protocol: Protocol) -> dict[str, dict[str, dict]]:
    """{stage instance id: {metric: band}} or {} when the protocol has no bands yet."""
    test_dir = protocol.test_dir
    path = test_dir / BANDS_FILE if test_dir else None
    if path is None or not path.exists():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    stages = data.get("stages") if isinstance(data, dict) else None
    return stages if isinstance(stages, dict) else {}


def evaluate_stage(metrics: dict[str, Any], stage_bands: dict[str, dict]) -> list[dict]:
    """One check per banded metric: `{metric, value, verdict, band, detail}`."""
    checks: list[dict] = []
    for metric, band in stage_bands.items():
        value = metrics.get(metric)
        band = band if isinstance(band, dict) else {"exact": band}
        if value is None:
            checks.append(_check(metric, value, "FAIL", band, "metric missing from this run"))
            continue
        if "exact" in band:
            want = band["exact"]
            same = _canon(value) == _canon(want)
            checks.append(_check(metric, value, "PASS" if same else "FAIL", band, "" if same else f"expected {want!r}"))
            continue
        try:
            v = float(value)
        except (TypeError, ValueError):
            checks.append(_check(metric, value, "FAIL", band, f"non-numeric value {value!r} for a min/max band"))
            continue
        lo, hi = band.get("min"), band.get("max")
        if (lo is not None and v < float(lo)) or (hi is not None and v > float(hi)):
            checks.append(_check(metric, value, "FAIL", band, f"outside [{lo}, {hi}]"))
            continue
        mean, std = band.get("mean"), band.get("std")
        if mean is not None and std is not None and float(std) > 0 and abs(v - float(mean)) > DRIFT_K * float(std):
            checks.append(
                _check(
                    metric, value, "DRIFT", band, f"{abs(v - float(mean)) / float(std):.1f}σ from recorded mean {mean}"
                )
            )
            continue
        checks.append(_check(metric, value, "PASS", band, ""))
    return checks


def stage_verdict(checks: list[dict]) -> str:
    if not checks:
        return "UNBANDED"
    verdicts = {c["verdict"] for c in checks}
    for v in ("FAIL", "DRIFT"):
        if v in verdicts:
            return v
    return "PASS"


def propose_bands(records: list[dict[str, dict[str, Any]]]) -> tuple[dict, list[str]]:
    """`records` = per-run `{stage: {metric: value}}`. Returns `({stage: {metric: band}},
    notes)`: exact bands where every record agrees on a non-float value, observed min/max
    (plus mean/std from n >= 3 records) for numbers, nothing for metrics that vary in a way a
    band cannot express (listed in the notes instead of guessed)."""
    notes: list[str] = []
    bands: dict[str, dict[str, dict]] = {}
    stages = sorted({s for r in records for s in r})
    for stage in stages:
        per_metric: dict[str, list[Any]] = {}
        for r in records:
            for metric, value in (r.get(stage) or {}).items():
                per_metric.setdefault(metric, []).append(value)
        for metric, values in sorted(per_metric.items()):
            if len(values) < len(records) or any(v is None for v in values):
                notes.append(f"{stage}.{metric}: missing in some records — not banded")
                continue
            canon = {_canon(v) for v in values}
            if all(isinstance(v, bool | str | list) for v in values):
                if len(canon) == 1:
                    bands.setdefault(stage, {})[metric] = {"exact": values[0]}
                else:
                    notes.append(f"{stage}.{metric}: records disagree ({len(canon)} distinct values) — not banded")
                continue
            try:
                nums = [float(v) for v in values]
            except (TypeError, ValueError):
                notes.append(f"{stage}.{metric}: non-numeric — not banded")
                continue
            if len(canon) == 1 and all(isinstance(v, int) for v in values):
                bands.setdefault(stage, {})[metric] = {"exact": values[0]}
                continue
            band: dict[str, Any] = {"min": min(nums), "max": max(nums)}
            if len(nums) >= 3:
                mean = sum(nums) / len(nums)
                std = math.sqrt(sum((x - mean) ** 2 for x in nums) / (len(nums) - 1))
                band.update(mean=round(mean, 6), std=round(std, 6))
            bands.setdefault(stage, {})[metric] = band
    return bands, notes


def write_bands(path: Path, bands: dict, *, header: str = "") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = yaml.safe_dump({"version": 1, "stages": bands}, sort_keys=False, default_flow_style=None, width=110)
    path.write_text(header + body)
    return path


def _check(metric: str, value: Any, verdict: str, band: dict, detail: str) -> dict:
    return {"metric": metric, "value": value, "verdict": verdict, "band": band, "detail": detail}


def _canon(v: Any) -> Any:
    if isinstance(v, list):
        return tuple(sorted(str(x) for x in v))
    if isinstance(v, bool):
        return v
    if isinstance(v, int | float):
        return float(v)
    return v

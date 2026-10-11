"""Boundary curation: a human looks at the picks nearest the cut, and says particle or background.

A σ cut is uncertain only near itself. The picks whose score lies closest to it, gathered across
tomograms from the extraction's cutout atlases (the subtomo tiles the Journey shows), are where a
human's time decides the most; the share of them labelled "particle" estimates the precision at the
cut. The labels are a record of their own — who labelled what, when, at which cut — kept beside the
project's other analytics; nothing about the picks changes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from services.particles.records import Producer, Provenance, Record, analytics_root, now_iso, read_record, write_record

REVIEWS_SUBDIR = "reviews"
BOUNDARY_KIND = "boundary_labels"
BOUNDARY_SCHEMA = 1
LABELS = ("particle", "background")


def boundary_picks(ce_job_dir: Path, cut: float, *, window: float, limit: int) -> list[dict[str, Any]]:
    """Up to `limit` picks scoring within `window` σ of `cut`, closest first, each with the atlas tile
    the extraction's previews hold for it. Tomograms without previews or without a σ are skipped."""
    from services.particles.analytics_store import tomo_sigma_map
    from services.visualization.preview_orchestrator import read_preview_manifest

    manifest = read_preview_manifest(ce_job_dir) or {}
    sigmas = tomo_sigma_map(ce_job_dir)
    out: list[dict[str, Any]] = []
    for tomo, entry in (manifest.get("tomograms") or {}).items():
        sigma = sigmas.get(tomo)
        atlas, index, picks_json = entry.get("cutout_atlas"), entry.get("cutout_index"), entry.get("picks_json")
        if not (sigma and atlas and index and picks_json):
            continue
        picks = json.loads(Path(picks_json).read_text()).get("picks") or []
        layout, positions = None, {}
        for p in picks:
            if p.get("score") is None:
                continue
            s = float(p["score"]) / sigma
            if abs(s - cut) > window:
                continue
            if layout is None:
                meta = json.loads(Path(index).read_text())
                layout = {k: meta.get(k) for k in ("tile_px", "cols", "rows")}
                positions = meta.get("index") or {}
            pos = positions.get(str(p["i"]))
            if not pos:
                continue
            out.append(
                {
                    "key": f"{tomo}|{p['i']}",
                    "tomo": tomo,
                    "i": int(p["i"]),
                    "xyz_px": [p.get("x"), p.get("y"), p.get("z")],
                    "sigma": round(s, 3),
                    "atlas": atlas,
                    "rc": pos,
                    **layout,
                }
            )
    out.sort(key=lambda d: abs(d["sigma"] - cut))
    return out[:limit]


def labels_path(project_path: Path, ce_instance: str) -> Path:
    return analytics_root(project_path) / REVIEWS_SUBDIR / f"boundary_{ce_instance}.json"


def read_labels(project_path: Path, ce_instance: str) -> dict[str, Any]:
    """{"labels": {key: {"label", "sigma", "cut", "at", "by"}}} of one extraction job, empty when none."""
    rec = read_record(labels_path(project_path, ce_instance))
    return rec.data if rec is not None else {"labels": {}}


def save_label(
    project_path: Path,
    ce_instance: str,
    ce_job_dir: Path,
    pick: dict[str, Any],
    label: str | None,
    *,
    cut: float,
    user: str,
) -> dict[str, Any]:
    """Set (or clear, with None) one pick's label and rewrite the record."""
    if label is not None and label not in LABELS:
        raise ValueError(f"label must be one of {LABELS}")
    data = read_labels(project_path, ce_instance)
    labels = dict(data.get("labels") or {})
    if label is None:
        labels.pop(pick["key"], None)
    else:
        labels[pick["key"]] = {
            "label": label,
            "sigma": pick["sigma"],
            "tomo": pick["tomo"],
            "xyz_px": pick.get("xyz_px"),
            "cut": cut,
            "at": now_iso(),
            "by": user,
        }
    record = Record(
        Provenance(
            kind=BOUNDARY_KIND,
            schema=BOUNDARY_SCHEMA,
            module=__name__,
            computed_by="user",
            producer=Producer("tmextractcand", ce_instance, str(ce_job_dir)),
            note="labels a user gave picks near a σ cut in the Analytics tab",
        ),
        {"labels": labels},
    )
    write_record(labels_path(project_path, ce_instance), record)
    return record.data


def precision_near(labels: dict[str, Any], cut: float, window: float) -> tuple[int, int]:
    """(picks labelled particle, picks labelled) among labels within `window` σ of `cut`."""
    near = [v for v in labels.values() if abs(float(v.get("sigma", 1e9)) - cut) <= window]
    return sum(1 for v in near if v.get("label") == "particle"), len(near)

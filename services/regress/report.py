"""Run outputs (roadmap 14 §runner): `run.json` (the record), `metrics.json` (per-stage
metrics, what `record` keeps), `report.md` (the human table), the run listing and the
retention policy (last N run dirs; reports/records are never pruned)."""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

RUN_JSON = "run.json"
METRICS_JSON = "metrics.json"
REPORT_MD = "report.md"
RUNNER_LOG = "runner.log"


def write_run_outputs(record, run_dir: Path) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / RUN_JSON).write_text(json.dumps(record.to_dict(), indent=2, default=str))
    (run_dir / METRICS_JSON).write_text(json.dumps(metrics_by_stage(record), indent=2, default=str))
    (run_dir / REPORT_MD).write_text(render_markdown(record))
    return run_dir / REPORT_MD


def metrics_by_stage(record) -> dict[str, dict[str, Any]]:
    return {s.instance_id: dict(s.metrics) for s in record.stages}


def render_markdown(record) -> str:
    n = len(record.stages)
    n_ok = sum(1 for s in record.stages if s.verdict in ("PASS", "UNBANDED"))
    lines = [
        f"# {record.protocol} v{record.protocol_version} — {record.mode} — **{record.verdict}**",
        "",
        f"- started: {record.started_at}  ·  finished: {record.finished_at or '—'}",
        f"- run dir: `{record.run_dir}`",
        f"- project: `{record.project_dir or '—'}`",
        f"- stages: {n_ok}/{n} pass (PASS or unbanded)",
        "",
        "| stage | status | verdict | metrics | notes |",
        "|---|---|---|---|---|",
    ]
    for s in record.stages:
        shown = ", ".join(f"{k}={_fmt(v)}" for k, v in s.metrics.items() if not isinstance(v, list))
        notes = list(s.notes)
        for c in s.checks:
            if c["verdict"] != "PASS":
                notes.append(f"{c['metric']}: {c['verdict']} ({c['detail']})")
        if s.infra:
            notes.append(f"INFRA: {s.infra.get('label')} — {s.infra.get('line', '')}")
            if s.infra.get("node"):
                notes.append(f"node {s.infra['node']}")
        lines.append(f"| {s.instance_id} | {s.status} | {s.verdict} | {shown} | {'; '.join(notes)} |")
    if record.annotations:
        lines += ["", "## Annotations", ""] + [f"- {a}" for a in record.annotations]
    if record.input:
        lines += ["", "## Input", "", f"- dir: `{record.input.get('input_dir', '—')}`"]
        lines.append(f"- frozen: {record.input.get('frozen')}  ·  ok: {record.input.get('ok')}")
        lines += [f"- {p}" for p in record.input.get("problems", [])]
    if record.site:
        lines += ["", "## Site (tools at run time)", ""]
        for name, t in sorted(record.site.items()):
            lines.append(f"- {name}: `{t.get('path')}` size={t.get('size')} mtime={t.get('mtime')}")
    return "\n".join(lines) + "\n"


def _fmt(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:g}"
    return str(v)


def list_runs(input_root: Path, protocol_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    runs_root = Path(input_root) / "runs"
    if not runs_root.is_dir():
        return rows
    for d in sorted(runs_root.glob(f"{protocol_name}-*"), reverse=True):
        rj = d / RUN_JSON
        if not rj.exists():
            continue
        try:
            data = json.loads(rj.read_text())
        except (OSError, ValueError) as e:
            rows.append({"run_dir": str(d), "verdict": "?", "error": str(e)})
            continue
        stages = data.get("stages") or []
        rows.append(
            {
                "run_dir": str(d),
                "started_at": data.get("started_at"),
                "finished_at": data.get("finished_at"),
                "mode": data.get("mode"),
                "verdict": data.get("verdict"),
                "n_stages": len(stages),
                "n_pass": sum(1 for s in stages if s.get("verdict") in ("PASS", "UNBANDED")),
                "report": str(d / REPORT_MD),
            }
        )
    return rows


def prune_runs(input_root: Path, protocol_name: str, keep: int) -> list[Path]:
    """Delete all but the newest `keep` run directories of this protocol. Only directories
    carrying our own run.json are candidates; anything else under runs/ is left alone."""
    runs_root = Path(input_root) / "runs"
    if not runs_root.is_dir() or keep < 0:
        return []
    owned = sorted(d for d in runs_root.glob(f"{protocol_name}-*") if (d / RUN_JSON).exists())
    doomed = owned[: max(0, len(owned) - keep)] if keep else owned
    deleted: list[Path] = []
    for d in doomed:
        try:
            shutil.rmtree(d)
            deleted.append(d)
            logger.info("pruned run dir %s (retention keep=%d)", d, keep)
        except OSError as e:
            logger.warning("could not prune %s: %s", d, e)
    return deleted


def to_plain(obj: Any) -> Any:
    return asdict(obj) if is_dataclass(obj) else obj

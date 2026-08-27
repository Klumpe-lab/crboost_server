"""run_protocol(): apply -> deploy -> wait -> checks -> verdicts (roadmap 14 §runner).

One coroutine for both consumers — the CLI (crboost_regress.py) and the landing-page
Protocols dialog (through BackgroundTask). Ground truth for a stage is its own
RELION_JOB_EXIT_* sentinel + SLURM state as reconciled by `reconcile_afterok`; the UI status
layer is never consulted. Verdicts: PASS · DRIFT (in band, > k·σ from the recorded mean) ·
FAIL · INFRA (a known cluster signature in the logs, node named, NEVER requeued) · UNBANDED
(no bands yet) · SKIPPED (an upstream stage failed). `record` keeps the metrics + blessed
artifacts of a run as a baseline record; `bless` proposes bands from the records.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from services.configs.config_service import get_config_service
from services.models_base import JobStatus, JobType
from services.protocols.apply import apply_protocol
from services.protocols.schema import Protocol
from services.regress import checks as stage_checks
from services.regress.bands import BANDS_FILE, evaluate_stage, load_bands, propose_bands, stage_verdict, write_bands
from services.regress.infra import classify_failure, node_for
from services.regress.inputs import check_input
from services.regress.report import METRICS_JSON, RUN_JSON, RUNNER_LOG, metrics_by_stage, prune_runs, write_run_outputs
from services.result import err, ok

logger = logging.getLogger(__name__)

ProgressCb = Callable[[int, int, str], None]

SITE_FILE = "site.yaml"
RECORDS_DIRNAME = "records"
BLESSED_DIRNAME = "blessed"
ARTIFACTS_DIRNAME = "artifacts"
VERDICT_RANK = ("INFRA", "FAIL", "DRIFT", "PASS")


@dataclass
class StageResult:
    instance_id: str
    job_type: str
    status: str  # JobStatus value
    verdict: str  # PASS | DRIFT | FAIL | INFRA | UNBANDED | SKIPPED
    job_dir: str | None = None
    slurm_job_id: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    checks: list[dict] = field(default_factory=list)
    infra: dict | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class RunRecord:
    protocol: str
    protocol_version: int
    mode: str
    started_at: str
    run_dir: str
    finished_at: str | None = None
    project_dir: str | None = None
    verdict: str = "FAIL"
    stages: list[StageResult] = field(default_factory=list)
    annotations: list[str] = field(default_factory=list)
    input: dict[str, Any] = field(default_factory=dict)
    site: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def n_ok(self) -> int:
        return sum(1 for s in self.stages if s.verdict in ("PASS", "UNBANDED"))

    def summary(self) -> str:
        return f"{self.verdict} · {self.n_ok}/{len(self.stages)} stages · {self.run_dir}"


# ── locations ─────────────────────────────────────────────────────────────────


def local_data_root() -> Path:
    root = get_config_service().local_data_root
    if root is None:
        raise RuntimeError(
            "local_data.root is not configured and local.DefaultProjectBase is empty — set one in conf.yaml"
        )
    return root


def baseline_dir(root: Path, protocol: Protocol) -> Path:
    return Path(root) / "baseline" / protocol.name


def blessed_dir(root: Path, protocol: Protocol) -> Path:
    return baseline_dir(root, protocol) / BLESSED_DIRNAME


# ── site snapshot ─────────────────────────────────────────────────────────────


def snapshot_site() -> dict[str, dict[str, Any]]:
    """Per configured tool: exec mode, path, size, mtime. Cheap (no digests: sifs are ~10 GB);
    enough to say "the container changed under this run"."""
    out: dict[str, dict[str, Any]] = {}
    for name, tc in get_config_service().config.tools.items():
        path = tc.container_path if tc.exec_mode == "container" else tc.bin_path
        entry: dict[str, Any] = {"exec_mode": tc.exec_mode, "path": path or "", "size": None, "mtime": None}
        try:
            st = Path(path).stat() if path else None
        except OSError:
            st = None
        if st is not None:
            entry.update(size=st.st_size, mtime=int(st.st_mtime))
        out[name] = entry
    return out


def load_site(protocol: Protocol) -> dict[str, dict[str, Any]]:
    test_dir = protocol.test_dir
    path = test_dir / SITE_FILE if test_dir else None
    if path is None or not path.exists():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def compare_site(current: dict, recorded: dict) -> list[str]:
    """Annotations for every tool whose path/size/mtime differs from the blessed site."""
    if not recorded:
        return ["site not pinned yet (no test/site.yaml) — container drift cannot be attributed"]
    out: list[str] = []
    for name, want in sorted(recorded.items()):
        have = current.get(name)
        if have is None:
            out.append(f"site: tool '{name}' pinned in site.yaml is not configured now")
            continue
        for key in ("path", "size", "mtime"):
            if have.get(key) != want.get(key):
                out.append(f"site: {name} {key} changed ({want.get(key)} -> {have.get(key)})")
    return out


# ── the run ───────────────────────────────────────────────────────────────────


async def run_protocol(
    backend,
    protocol: Protocol,
    *,
    mode: str = "run",
    progress_cb: ProgressCb | None = None,
    keep_runs: int = 3,
    poll_secs: int = 15,
    max_wall_hours: float = 14.0,
) -> RunRecord:
    """Tier B, end to end. Always returns a RunRecord and always leaves `run.json` /
    `report.md` / `runner.log` in the run dir, whatever happened."""
    if mode not in ("run", "record"):
        raise ValueError(f"mode must be 'run' or 'record', not {mode!r}")
    root = local_data_root()
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = root / "runs" / f"{protocol.name}-{stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)
    rec = RunRecord(
        protocol=protocol.name,
        protocol_version=protocol.version,
        mode=mode,
        started_at=datetime.now().isoformat(timespec="seconds"),
        run_dir=str(run_dir),
    )
    n_stages = len(protocol.stages)

    def progress(done: int, msg: str) -> None:
        if progress_cb is not None:
            progress_cb(done, n_stages, msg)

    handler = _attach_log(run_dir / RUNNER_LOG)
    try:
        await _execute(backend, protocol, rec, root, run_dir, progress, poll_secs, max_wall_hours)
        if mode == "record" and rec.stages:
            _write_record(rec, root, protocol, stamp)
    except Exception as e:
        logger.exception("run_protocol(%s) crashed", protocol.name)
        rec.annotations.append(f"runner crashed: {e}")
        rec.verdict = "FAIL"
    finally:
        rec.finished_at = datetime.now().isoformat(timespec="seconds")
        write_run_outputs(rec, run_dir)
        logger.info("%s: %s", protocol.name, rec.summary())
        _detach_log(handler)
        prune_runs(root, protocol.name, keep_runs)
    return rec


async def _execute(backend, protocol, rec, root, run_dir, progress, poll_secs, max_wall_hours) -> None:
    progress(0, "checking the frozen input")
    chk = await asyncio.to_thread(check_input, protocol, root)
    rec.input = {
        "input_dir": str(chk.input_dir),
        "ok": chk.ok,
        "frozen": chk.frozen,
        "problems": chk.problems,
        "notes": chk.notes,
    }
    rec.annotations += [f"input: {n}" for n in chk.notes]
    if not chk.ok:
        rec.verdict = "INFRA"
        rec.annotations += [f"input: {p}" for p in chk.problems]
        return

    rec.site = snapshot_site()
    rec.annotations += compare_site(rec.site, load_site(protocol))

    progress(0, "applying the protocol")
    applied = await apply_protocol(
        backend,
        protocol,
        project_name="project",
        project_base_path=run_dir,
        movies_glob=str(chk.input_dir / chk.movies_glob),
        mdocs_glob=str(chk.input_dir / chk.mdocs_glob),
    )
    if not applied.get("success"):
        rec.verdict = "FAIL"
        rec.annotations.append(f"apply failed: {applied.get('error')}")
        return
    rec.project_dir = applied["project_path"]
    rec.annotations += [f"apply: {w}" for w in applied.get("warnings", [])]
    rec.annotations += [f"state load repair (a green run has none): {w}" for w in applied.get("load_warnings", [])]
    project_dir = Path(rec.project_dir)
    state = backend.state_service.state_for(project_dir)
    # The chain must live in SLURM, not in this process: a CLI run ends when the report is
    # written and must not be holding a schemer subprocess.
    state.use_afterok_orchestrator = True
    state.mark_dirty()

    progress(0, "submitting the afterok chain")
    ids = protocol.stage_ids()
    deployed = await backend.pipeline_orchestrator.deploy_and_run_scheme(project_dir, ids)
    if not deployed.get("success"):
        rec.verdict = "FAIL"
        rec.annotations.append(f"deploy failed: {deployed.get('error')}")
        return
    rec.annotations.append(f"submitted {len(deployed.get('submitted') or [])} SLURM job(s)")

    await _wait_for_chain(backend, state, project_dir, ids, progress, poll_secs, max_wall_hours, rec)

    progress(len(ids), "collecting metrics")
    bands = load_bands(protocol)
    if not bands:
        rec.annotations.append("no bands.yaml yet — every completed stage is UNBANDED (record ×3, then bless)")
    blessed = blessed_dir(root, protocol)
    rec.stages = await asyncio.to_thread(
        _evaluate_stages, state, project_dir, ids, bands, blessed if blessed.is_dir() else None
    )
    for sr in rec.stages:
        if sr.infra is not None:
            sr.infra["node"] = await node_for(sr.slurm_job_id)
    rec.verdict = overall_verdict(rec.stages)


async def _wait_for_chain(backend, state, project_dir, ids, progress, poll_secs, max_wall_hours, rec) -> None:
    deadline = time.monotonic() + max_wall_hours * 3600
    while True:
        await backend.pipeline_runner.reconcile_afterok(str(project_dir))
        done = sum(1 for i in ids if state.jobs[i].execution_status == JobStatus.SUCCEEDED)
        live = [i for i in ids if state.jobs[i].execution_status in (JobStatus.RUNNING, JobStatus.QUEUED)]
        progress(done, ("running: " + ", ".join(live)) if live else "settling")
        if not state.pipeline_active:
            return
        if time.monotonic() > deadline:
            rec.annotations.append(f"wall clock exceeded ({max_wall_hours:g} h) — cancelling the chain")
            slurm_ids = [str(state.jobs[i].slurm_job_id) for i in ids if state.jobs[i].slurm_job_id]
            try:
                await backend.slurm_service.scancel_jobs(slurm_ids)
            except Exception as e:
                logger.warning("scancel after wall-clock breach failed: %s", e)
            return
        await asyncio.sleep(poll_secs)


def _evaluate_stages(state, project_dir: Path, ids: list[str], bands: dict, blessed: Path | None) -> list[StageResult]:
    results: list[StageResult] = []
    upstream_failed = False
    for iid in ids:
        jm = state.jobs[iid]
        status = jm.execution_status
        job_dir = stage_checks.job_dir_of(project_dir, jm)
        sr = StageResult(
            instance_id=iid,
            job_type=jm.job_type.value,
            status=status.value,
            verdict="PASS",
            job_dir=str(job_dir) if job_dir else None,
            slurm_job_id=getattr(jm, "slurm_job_id", None),
        )
        if status == JobStatus.SUCCEEDED:
            sr.metrics, notes = stage_checks.collect_stage_metrics(project_dir, iid, jm, blessed_dir=blessed)
            sr.notes += notes
        elif upstream_failed:
            sr.verdict = "SKIPPED"
            sr.notes.append("not run: an upstream stage failed")
        else:
            infra = classify_failure(job_dir) if job_dir is not None and job_dir.is_dir() else None
            if infra:
                sr.infra = infra
                sr.verdict = "INFRA"
            else:
                sr.verdict = "FAIL"
                sr.notes.append("no infrastructure signature in the logs — a failure of ours")
            upstream_failed = True
        results.append(sr)

    _cross_stage_metrics(results)
    for sr in results:
        if sr.status == JobStatus.SUCCEEDED.value:
            sr.checks = evaluate_stage(sr.metrics, bands.get(sr.instance_id, {}))
            sr.verdict = stage_verdict(sr.checks)
    return results


def _cross_stage_metrics(results: list[StageResult]) -> None:
    """Conservation checks that need two stages: extracted particles never exceed the
    candidates, and class3d keeps every particle it was handed."""
    by_type = {sr.job_type: sr for sr in results if sr.status == JobStatus.SUCCEEDED.value}
    cand = by_type.get(JobType.TEMPLATE_EXTRACT_PYTOM.value)
    sub = by_type.get(JobType.SUBTOMO_EXTRACTION.value)
    cls = by_type.get(JobType.CLASS3D.value)
    if cand and sub and cand.metrics.get("n_candidates") is not None and sub.metrics.get("n_particles") is not None:
        sub.metrics["n_particles_le_candidates"] = bool(sub.metrics["n_particles"] <= cand.metrics["n_candidates"])
    if sub and cls and sub.metrics.get("n_particles") is not None and cls.metrics.get("n_particles") is not None:
        cls.metrics["particles_conserved"] = bool(cls.metrics["n_particles"] == sub.metrics["n_particles"])


def overall_verdict(stages: list[StageResult]) -> str:
    if not stages:
        return "FAIL"
    present = {s.verdict for s in stages}
    for v in VERDICT_RANK:
        if v in present:
            return v
    return "PASS"  # only UNBANDED / SKIPPED-free stages left


# ── record / bless ────────────────────────────────────────────────────────────


def _write_record(rec: RunRecord, root: Path, protocol: Protocol, stamp: str) -> None:
    rd = baseline_dir(root, protocol) / RECORDS_DIRNAME / stamp
    art = rd / ARTIFACTS_DIRNAME
    art.mkdir(parents=True, exist_ok=True)
    (rd / METRICS_JSON).write_text(json.dumps(metrics_by_stage(rec), indent=2, default=str))
    (rd / RUN_JSON).write_text(json.dumps(rec.to_dict(), indent=2, default=str))
    (rd / SITE_FILE).write_text(yaml.safe_dump(rec.site, sort_keys=True))
    for sr in rec.stages:
        if not sr.job_dir or sr.status != JobStatus.SUCCEEDED.value:
            continue
        jd = Path(sr.job_dir)
        if sr.job_type == JobType.TEMPLATE_EXTRACT_PYTOM.value and (jd / "candidates.star").exists():
            shutil.copy2(jd / "candidates.star", art / stage_checks.BLESSED_CANDIDATES)
        elif sr.job_type == JobType.RECONSTRUCT_PARTICLE.value and (jd / "merged.mrc").exists():
            shutil.copy2(jd / "merged.mrc", art / stage_checks.BLESSED_MERGED)
        elif sr.job_type == JobType.CLASS3D.value:
            maps = sorted(jd.glob("run_it*_class001.mrc"))
            if maps:
                shutil.copy2(maps[-1], art / stage_checks.BLESSED_CLASS)
    rec.annotations.append(f"recorded as baseline record {rd}")


def bless_protocol(protocol: Protocol, *, input_root: Path | None = None) -> dict[str, Any]:
    """Propose `test/bands.yaml` from every baseline record (observed min/max, exact where
    the records agree; the human widens and accepts), promote the newest record's artifacts
    to `baseline/<protocol>/blessed/` and pin its site snapshot as `test/site.yaml`."""
    root = input_root or local_data_root()
    records_root = baseline_dir(root, protocol) / RECORDS_DIRNAME
    dirs = sorted(d for d in records_root.glob("*") if (d / METRICS_JSON).exists()) if records_root.is_dir() else []
    if not dirs:
        return err(f"no baseline records under {records_root} — run `crboost_regress.py record {protocol.name}` first")
    test_dir = protocol.test_dir
    if test_dir is None:
        return err(f"protocol '{protocol.name}' is not bound to a bundle directory")
    records = [json.loads((d / METRICS_JSON).read_text()) for d in dirs]
    bands, notes = propose_bands(records)
    if not bands:
        return err("records carry no bandable metrics", notes=notes)

    test_dir.mkdir(parents=True, exist_ok=True)
    target = test_dir / BANDS_FILE
    if target.exists():
        target = test_dir / "bands.proposed.yaml"
        notes.append(
            "bands.yaml already exists — the proposal is written beside it as bands.proposed.yaml; merge by hand"
        )
    header = (
        f"# Proposed by `crboost_regress.py bless` from {len(dirs)} record(s) on "
        f"{datetime.now():%Y-%m-%d}: observed min/max, exact where every record agrees,\n"
        "# mean/std from n >= 3. Widen deliberately — nothing here is a guess, so nothing here is generous.\n"
    )
    write_bands(target, bands, header=header)

    dst = blessed_dir(root, protocol)
    src = dirs[-1] / ARTIFACTS_DIRNAME
    if src.is_dir():
        shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(src, dst)
    site_src = dirs[-1] / SITE_FILE
    if site_src.exists():
        shutil.copy2(site_src, test_dir / SITE_FILE)
    if len(dirs) < 3:
        notes.append(f"only {len(dirs)} record(s): no mean/std, so DRIFT cannot fire yet (3 recommended)")
    notes.append(
        "overlap / CC metrics compare against the blessed artifacts: records taken BEFORE this bless have none"
    )
    return ok(bands_path=str(target), n_records=len(dirs), notes=notes, blessed_dir=str(dst), stages=sorted(bands))


# ── logging ───────────────────────────────────────────────────────────────────


def _attach_log(path: Path) -> logging.Handler:
    handler = logging.FileHandler(path)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname).1s %(name)s:%(lineno)d %(message)s"))
    logging.getLogger().addHandler(handler)
    return handler


def _detach_log(handler: logging.Handler) -> None:
    logging.getLogger().removeHandler(handler)
    handler.close()

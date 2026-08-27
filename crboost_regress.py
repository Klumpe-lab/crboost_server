#!/usr/bin/env python3
"""crboost_regress — the protocol regression harness CLI (docs/roadmaps/14-protocols-regression-harness.md).

    venv/bin/python3 crboost_regress.py fetch-input  copia-empiar12580   # download from EMPIAR + freeze
    venv/bin/python3 crboost_regress.py freeze-input copia-empiar12580   # sizes + sha256 into <bundle>/test/input.yaml
    venv/bin/python3 crboost_regress.py run          copia-empiar12580   # apply -> deploy -> wait -> checks -> report
    venv/bin/python3 crboost_regress.py record       copia-empiar12580   # run + keep as a baseline record
    venv/bin/python3 crboost_regress.py bless        copia-empiar12580   # propose test/bands.yaml (you edit)
    venv/bin/python3 crboost_regress.py report       copia-empiar12580   # table of past runs
    venv/bin/python3 crboost_regress.py snapshot     copia-empiar12580   # Tier A: scheme vs test/snapshots/

Run from the repo root in the server's environment (the module block of config/qsub.sh);
`run` / `record` / `snapshot` build the backend headlessly (no UI, no pipeline monitor —
this process is the reconciler for the run it owns). Exit codes: 0 PASS/DRIFT, 1 FAIL,
2 INFRA, 3 usage / setup.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parent


def _backend():
    from backend import CryoBoostBackend

    return CryoBoostBackend(REPO)


def _protocol(name: str):
    from services.protocols.discovery import load_named_protocol

    return load_named_protocol(name)


def _progress(done: int, total: int, msg: str) -> None:
    print(f"[{done}/{total}] {msg}", flush=True)


def cmd_fetch_input(args) -> int:
    from services.regress.inputs import check_input, download_commands, download_input
    from services.regress.runner import local_data_root

    protocol = _protocol(args.protocol)
    root = local_data_root()
    if args.commands:
        for line in download_commands(protocol, root):
            print(line)
        return 0
    chk = check_input(protocol, root, verify_checksums=False)
    if chk.ok and chk.frozen:
        print(f"input present and frozen: {chk.input_dir}")
        return 0
    res = download_input(protocol, root, progress_cb=_progress)
    if not res["success"]:
        print(f"error: {res['error']}", file=sys.stderr)
        return 3
    print(f"{res['n_files']} files, {res['bytes'] / 2**30:.1f} GB in {res['input_dir']}")
    print(
        ("frozen: " if res["frozen_now"] else "verified against ")
        + res["manifest"]
        + ("  (commit it)" if res["frozen_now"] else "")
    )
    return 0


def cmd_freeze_input(args) -> int:
    from services.regress.inputs import freeze_input
    from services.regress.runner import local_data_root

    path = freeze_input(_protocol(args.protocol), local_data_root())
    print(f"frozen: {path}")
    return 0


def cmd_run(args, mode: str) -> int:
    from services.regress.runner import run_protocol

    protocol = _protocol(args.protocol)
    rec = asyncio.run(
        run_protocol(
            _backend(),
            protocol,
            mode=mode,
            progress_cb=_progress,
            keep_runs=args.keep,
            poll_secs=args.poll,
            max_wall_hours=args.max_hours,
        )
    )
    print(f"\n{rec.summary()}\nreport: {rec.run_dir}/report.md")
    for s in rec.stages:
        flag = "" if s.verdict in ("PASS", "UNBANDED") else " <--"
        print(f"  {s.instance_id:28s} {s.status:10s} {s.verdict}{flag}")
    for a in rec.annotations:
        print(f"  · {a}")
    return {"PASS": 0, "DRIFT": 0, "FAIL": 1, "INFRA": 2}.get(rec.verdict, 1)


def cmd_bless(args) -> int:
    from services.regress.runner import bless_protocol

    res = bless_protocol(_protocol(args.protocol))
    if not res["success"]:
        print(res["error"])
        return 3
    print(f"bands proposed from {res['n_records']} record(s): {res['bands_path']}")
    print(f"blessed artifacts: {res['blessed_dir']}")
    print(f"stages banded: {', '.join(res['stages'])}")
    for n in res["notes"]:
        print(f"  · {n}")
    return 0


def cmd_report(args) -> int:
    from services.regress.report import list_runs
    from services.regress.runner import local_data_root

    rows = list_runs(local_data_root(), _protocol(args.protocol).name)
    if not rows:
        print("no runs yet")
        return 0
    for r in rows:
        print(
            f"{r.get('started_at', '?'):20s} {r.get('mode', '?')!s:7s} {r.get('verdict', '?')!s:8s} "
            f"{r.get('n_pass', '?')}/{r.get('n_stages', '?')}  {r.get('report', r.get('run_dir'))}"
        )
    return 0


def cmd_snapshot(args) -> int:
    from services.protocols.apply import apply_protocol
    from services.regress.inputs import check_input
    from services.regress.runner import local_data_root
    from services.regress.snapshot import diff_snapshot, materialize, write_snapshot

    protocol = _protocol(args.protocol)
    if args.project:
        project_dir = Path(args.project).expanduser().resolve()
    else:
        root = local_data_root()
        project_dir = root / "snapshots" / protocol.name / "project"
        if not (project_dir / "project_params.json").exists():
            chk = check_input(protocol, root, verify_checksums=False)
            if not chk.ok:
                print("\n".join(chk.problems))
                return 3
            res = asyncio.run(
                apply_protocol(
                    _backend(),
                    protocol,
                    project_name="project",
                    project_base_path=project_dir.parent,
                    movies_glob=str(chk.input_dir / chk.movies_glob),
                    mdocs_glob=str(chk.input_dir / chk.mdocs_glob),
                )
            )
            if not res["success"]:
                print(res["error"])
                return 3
            for w in res.get("warnings", []):
                print(f"  · apply: {w}")
    files = materialize(protocol, project_dir)
    if args.update:
        print(f"golden snapshot written: {write_snapshot(protocol, files)} ({len(files)} files)")
        return 0
    diffs = diff_snapshot(protocol, files)
    if not diffs:
        print(f"snapshot identical ({len(files)} files)")
        return 0
    print("\n\n".join(diffs))
    return 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "record"):
        s = sub.add_parser(name)
        s.add_argument("protocol")
        s.add_argument("--keep", type=int, default=3, help="run dirs to retain (default 3)")
        s.add_argument("--poll", type=int, default=15, help="reconcile cadence in seconds")
        s.add_argument("--max-hours", type=float, default=14.0, help="wall-clock cap before the chain is cancelled")
    for name in ("bless", "report", "freeze-input"):
        sub.add_parser(name).add_argument("protocol")
    s = sub.add_parser("fetch-input")
    s.add_argument("protocol")
    s.add_argument("--commands", action="store_true", help="print the equivalent wget script instead of downloading")
    s = sub.add_parser("snapshot")
    s.add_argument("protocol")
    s.add_argument("--project", help="an applied project to materialize (default: a scratch apply under input_root)")
    s.add_argument("--update", action="store_true", help="record the current materialization as the golden snapshot")
    args = p.parse_args(argv)

    if Path.cwd().resolve() != REPO:
        print(f"run from the repo root ({REPO})", file=sys.stderr)
        return 3
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname).1s %(name)s:%(lineno)d %(message)s", datefmt="%H:%M:%S"
    )
    try:
        if args.cmd in ("run", "record"):
            return cmd_run(args, args.cmd)
        return {
            "bless": cmd_bless,
            "report": cmd_report,
            "fetch-input": cmd_fetch_input,
            "freeze-input": cmd_freeze_input,
            "snapshot": cmd_snapshot,
        }[args.cmd](args)
    except (FileNotFoundError, RuntimeError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""crboost_protocol — author, apply and export protocols (services/protocols/).

    venv/bin/python3 crboost_protocol.py list
    venv/bin/python3 crboost_protocol.py validate copia-empiar12580
    venv/bin/python3 crboost_protocol.py export   /path/to/project --name my-flow --out ~/.crboost/protocols/my-flow
    venv/bin/python3 crboost_protocol.py apply    copia-empiar12580 --name copia_test --base /path/to/projects \\
                                        --movies '/data/copia/*.eer' --mdocs '/data/copia/*.mdoc' [--gain gain.mrc]
    venv/bin/python3 crboost_protocol.py scheme   copia-empiar12580 --project /path/to/projects/copia_test

`scheme` derives a vanilla RELION `Schemes/<name>/` (scheme.star + per-stage job.star) from an
APPLIED project — runnable with `relion_schemer`; see services/protocols/scheme_export.py for
what does and does not survive the export. Run from the repo root.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parent


def cmd_list(_args) -> int:
    from services.protocols.discovery import list_protocols

    infos = list_protocols()
    if not infos:
        print("no protocols found")
        return 0
    for info in infos:
        if info.protocol is None:
            print(f"{info.name:28s} BROKEN  {info.bundle_dir}: {info.error}")
            continue
        p = info.protocol
        test = " +test" if info.has_test_bundle else ""
        print(
            f"{info.name:28s} v{p.version}  {len(p.stages)} stages  {len(p.species)} species{test}  {info.bundle_dir}"
        )
    return 0


def cmd_validate(args) -> int:
    from services.protocols.apply import validate_protocol
    from services.protocols.discovery import load_named_protocol

    problems = validate_protocol(load_named_protocol(args.protocol))
    if problems:
        print("\n".join(f"! {p}" for p in problems))
        return 1
    print("ok")
    return 0


def cmd_export(args) -> int:
    from services.project_state import ProjectState
    from services.protocols.export import export_protocol
    from services.protocols.schema import dump_protocol, protocol_to_yaml

    project_dir = Path(args.project).expanduser().resolve()
    state = ProjectState.load(project_dir / "project_params.json")
    state.project_path = project_dir
    protocol, warnings = export_protocol(state, name=args.name, bundle_dir=args.out, description=args.description or "")
    for w in warnings:
        print(f"  · {w}", file=sys.stderr)
    if args.out:
        print(f"written: {dump_protocol(protocol, args.out)}")
    else:
        print(protocol_to_yaml(protocol))
    return 0


def cmd_apply(args) -> int:
    from backend import CryoBoostBackend
    from services.protocols.apply import apply_protocol
    from services.protocols.discovery import load_named_protocol

    res = asyncio.run(
        apply_protocol(
            CryoBoostBackend(REPO),
            load_named_protocol(args.protocol),
            project_name=args.name,
            project_base_path=args.base,
            movies_glob=args.movies,
            mdocs_glob=args.mdocs,
            gain_reference_path=args.gain,
            shared=args.shared,
        )
    )
    if not res["success"]:
        print(res["error"], file=sys.stderr)
        return 1
    print(f"created: {res['project_path']}  stages: {', '.join(res['stages'])}")
    for w in res.get("warnings", []):
        print(f"  · {w}")
    return 0


def cmd_scheme(args) -> int:
    from services.protocols.discovery import load_named_protocol
    from services.protocols.scheme_export import export_relion_scheme

    res = export_relion_scheme(
        load_named_protocol(args.protocol), project_dir=args.project, scheme_name=args.scheme_name
    )
    if not res["success"]:
        print(res["error"], file=sys.stderr)
        return 1
    print(f"scheme: {res['scheme_dir']}  jobs: {', '.join(res['jobs'])}\nrun it with:\n  {res['command']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    sub.add_parser("validate").add_argument("protocol")
    s = sub.add_parser("export")
    s.add_argument("project")
    s.add_argument("--name", required=True)
    s.add_argument("--out", help="bundle directory to write protocol.yaml + assets/ into (default: print yaml)")
    s.add_argument("--description", default="")
    s = sub.add_parser("apply")
    s.add_argument("protocol")
    s.add_argument("--name", required=True, help="project name")
    s.add_argument("--base", required=True, help="project base directory")
    s.add_argument("--movies", required=True, help="movies glob")
    s.add_argument("--mdocs", required=True, help="mdocs glob")
    s.add_argument("--gain", default=None, help="gain reference path (optional)")
    s.add_argument("--shared", action="store_true")
    s = sub.add_parser("scheme")
    s.add_argument("protocol")
    s.add_argument("--project", required=True, help="an APPLIED project directory")
    s.add_argument("--scheme-name", default=None, help="Schemes/<name>/ (default: the protocol name)")
    args = p.parse_args(argv)

    if Path.cwd().resolve() != REPO:
        print(f"run from the repo root ({REPO})", file=sys.stderr)
        return 3
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname).1s %(name)s:%(lineno)d %(message)s", datefmt="%H:%M:%S"
    )
    try:
        return {
            "list": cmd_list,
            "validate": cmd_validate,
            "export": cmd_export,
            "apply": cmd_apply,
            "scheme": cmd_scheme,
        }[args.cmd](args)
    except (FileNotFoundError, RuntimeError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())

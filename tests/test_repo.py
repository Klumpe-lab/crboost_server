"""What every checkout ships, checked without a site: `./crboost ci` runs it (`pytest -m repo tests/test_repo.py`).

It reads the templates (config/conf.template.yaml, config/qsub.template.sh), never a site's own conf.yaml or qsub.sh,
and touches no project and no SLURM. `./crboost test` is the other half: the self-test of an install against its
site.
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from services.configs import config_service

REPO = Path(__file__).resolve().parents[1]
CONF_TEMPLATE = REPO / "config" / "conf.template.yaml"
QSUB_TEMPLATE = REPO / "config" / "qsub.template.sh"
# Jobs that run in crboost's own environment name this tool; it has no entry under `tools:`.
IN_HOUSE_TOOL = "crboost"

pytestmark = pytest.mark.repo


@pytest.fixture(autouse=True, scope="module")
def template_conf(tmp_path_factory):
    """Every get_config_service() in this module reads the conf template, with no personal override layered on."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(config_service, "DEFAULT_CONFIG_PATH", CONF_TEMPLATE)
        mp.setattr(config_service, "USER_OVERRIDE_PATH", tmp_path_factory.mktemp("home") / "conf.yaml")
        config_service.reset_config_service()
        yield
    config_service.reset_config_service()


def test_conf_template_has_no_unknown_key():
    warnings = config_service.get_config_service().load_warnings
    assert not warnings, "\n".join(warnings)


def test_qsub_template_renders(tmp_path, monkeypatch):
    """The template keeps the exit-marker block crboost needs and ends on the job's exit code, and renders with
    every placeholder filled, as a job and as an array."""
    from services.computing import slurm_service

    text = QSUB_TEMPLATE.read_text()
    assert slurm_service.QSUB_EXIT_MARKERS in text
    assert text.rstrip().endswith("exit $EXIT_CODE")
    placeholders = [f"XXXextra{n}XXX" for n in range(1, 9)] + ["XXXoutfileXXX", "XXXerrfileXXX", "XXXcommandXXX"]
    assert [p for p in placeholders if p not in text] == []

    monkeypatch.setattr(slurm_service, "QSUB_TEMPLATE", QSUB_TEMPLATE)
    cfg = slurm_service.SlurmConfig.from_config_defaults()
    for name, array in (("run.sh", None), ("run_array.sh", "0-1%1")):
        script = slurm_service.write_sbatch_script(tmp_path / name, cfg, "true", array=array).read_text()
        assert not re.search(r"XXX\w+XXX", script), script


def test_every_protocol_bundle_validates():
    from services.protocols.apply import validate_protocol
    from services.protocols.schema import PROTOCOL_FILENAME, load_protocol

    bundles = sorted(p.parent for p in (REPO / "config" / "protocols").glob(f"*/{PROTOCOL_FILENAME}"))
    assert bundles, "no protocol bundle in config/protocols"
    problems = {b.name: validate_protocol(load_protocol(b)) for b in bundles}
    assert not any(problems.values()), problems


def test_every_job_runs_a_tool_the_template_configures():
    from services.jobs import jobtype_paramclass

    tools = set(config_service.get_config_service().config.tools) | {IN_HOUSE_TOOL}
    named = {jt.value: cls().get_tool_name() for jt, cls in jobtype_paramclass().items()}
    assert {job: tool for job, tool in named.items() if tool not in tools} == {}


def test_agent_api_verb_table():
    """Every verb's input schema builds as an MCP tool's, reads are named as reads, the instructions fit Claude
    Code's limit, and /api/v1/tools serves exactly the table."""
    from types import SimpleNamespace

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api.router import INSTRUCTIONS_LIMIT, create_api_router, instructions, tool_entry
    from api.verbs import VERBS

    assert len(instructions()) <= INSTRUCTIONS_LIMIT
    read_prefixes = ("get_", "list_", "inspect_", "plan_")
    for verb in VERBS.values():
        entry = tool_entry(verb)
        assert entry["inputSchema"]["type"] == "object", verb.name
        assert (verb.kind == "read") == verb.name.startswith(read_prefixes), verb.name
        assert verb.kind == "write" or not verb.asks, verb.name

    app = FastAPI()
    app.include_router(create_api_router(SimpleNamespace(), 0))
    served = TestClient(app).get("/api/v1/tools").json()
    assert [t["name"] for t in served["tools"]] == list(VERBS)


_BUILD_APP = """
import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient
from starlette.routing import Mount

from services.configs import config_service

config_service.DEFAULT_CONFIG_PATH = Path(sys.argv[1])
import server_app

app = server_app.setup_app(24399)
ui = [r for r in app.routes if isinstance(r, Mount) and r.path == ""]
print(json.dumps({
    "paths": sorted({getattr(r, "path", "") for r in app.routes}),
    "pages": sorted({getattr(r, "path", "") for mount in ui for r in mount.app.routes}),
    "tools": TestClient(app).get("/api/v1/tools").status_code,
}))
"""


def test_app_builds(tmp_path):
    """The whole app (FastAPI, the NiceGUI pages, the backend and its services) constructs on the template conf, in
    a process of its own with an empty HOME, and the agent API answers. Constructing starts nothing: the lifespan,
    the pipeline monitor's recovery of a base's projects among it, runs only once a server serves the app (a
    TestClient outside a `with` block runs none). FastAPI keeps an included router as one route, so the API's paths
    are not among the app's own."""
    env = {**os.environ, "HOME": str(tmp_path)}
    run = subprocess.run(
        [sys.executable, "-c", _BUILD_APP, str(CONF_TEMPLATE)],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert run.returncode == 0, run.stderr[-4000:]
    built = json.loads(run.stdout.strip().splitlines()[-1])
    assert {"/static", "/api/vis-asset"} <= set(built["paths"]), built
    assert {"/", "/workspace", "/p/{project}/{view}"} <= set(built["pages"]), built
    assert built["tools"] == 200, built

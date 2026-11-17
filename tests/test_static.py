"""Headnode self-test, seconds: `pytest`.

Everything preflight checks (config, tools, container_binds, partitions, qsub.sh), plus: no conf key
goes unread, and qsub.sh renders into an array script with every placeholder filled.
"""

import re

import preflight
from services.computing.slurm_service import SlurmConfig, write_sbatch_script
from services.configs.config_service import get_config_service


def test_preflight_checks_pass():
    preflight.failures.clear()
    preflight.check_python()
    cfg = preflight.check_config()
    preflight.check_slurm(cfg)
    preflight.check_qsub()
    assert not preflight.failures, "preflight:\n" + "\n".join(preflight.failures)


def test_every_config_key_is_read():
    warnings = get_config_service().load_warnings
    assert not warnings, "\n".join(warnings)


def test_qsub_renders(tmp_path):
    script = write_sbatch_script(tmp_path / "run.sh", SlurmConfig.from_config_defaults(), "true", array="0-1%1")
    text = script.read_text()
    assert not re.search(r"XXX(extra\d|outfile|errfile|command)XXX", text), text


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


def test_ood_portal_answers():
    """With `ood:` set, the portal answers at its noVNC page: a redirect to its sign-in counts, an error does not.
    The sign-in stands before every path, so this shows the portal is up and reachable from here, not that the
    noVNC page is still at `novnc_path` after an upgrade (roadmap 55 §2)."""
    import urllib.error
    import urllib.request

    import pytest

    ood = get_config_service().ood
    if not ood.enabled:
        pytest.skip("no ood.url/novnc_path: viewer sessions use the SSH tunnel")

    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None

    url = ood.url.rstrip("/") + ood.novnc_path
    # A browser's Accept: the portal's OIDC sends a browser to its sign-in (302) and other clients a 401.
    request = urllib.request.Request(url, headers={"Accept": "text/html"})
    try:
        status = urllib.request.build_opener(_NoRedirect).open(request, timeout=10).status
    except urllib.error.HTTPError as e:  # a 3xx surfaces here once redirects are refused
        status = e.code
    assert status < 400 or status == 401, f"{url} answered {status}"

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


# --- Container recipes (roadmap 63 D17, D19) -------------------------------------------------------------------------

DEFS = REPO / "container_defs"
UBUNTU_2404 = "ubuntu@sha256:562111b88caa79c71391c0c4d3c2114a7d5c35915c20561fd893cb1e07929e6b"
CUDA_DEVEL_2404 = "nvidia/cuda@sha256:020bc241a628776338f4d4053fed4c38f6f7f3d7eb5919fecb8de313bb8ba47c"
# Images the curation worker starts with a bare command: no `tools:` entry names them.
VIEWER_IMAGES = {"napari", "chimerax"}
# Recipes of images that still run somewhere and were built before D19. Each goes when its image is replaced by one
# built from a def that follows it; the list only shrinks.
OLD_RECIPES = {
    "chimerax_artiax_GL",
    "cryocare",
    "isonet2-a266ff4_torch-2.8.0_cuda12.9_sm100",
    "miss_alignment_torch2.8.0_cuda12.9",
    "napari_GL",
    "pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100",
    "relion-5.0.1-b16a35b_torch-2.8.0_cuda12.9_sm100",
    "tilt_filter_torch2.8.0_cuda12.8",
    "warp-2.0.0dev41_aretomo2-1.0.0_torch2.13_cuda12.9_sm100",
}
# What no recipe does: a TLS check turned off, a moving download, Anaconda's channels, apt outside apt_install (which
# makes it work under any fakeroot), wget (downloads go through fetch, which checks a sha256, or a lock).
FORBIDDEN = {
    "--no-check-certificate": "a TLS check turned off: a site's CA goes in through CRBOOST_BUILD_CA",
    "ssl_verify": "a TLS check turned off: a site's CA goes in through CRBOOST_BUILD_CA",
    "sslVerify": "a TLS check turned off: a site's CA goes in through CRBOOST_BUILD_CA",
    "/latest/": "a moving download: pin the version and check its sha256 (fetch)",
    "repo.anaconda.com": "Anaconda's channels and terms: Miniforge, conda-forge only (install_miniforge)",
    "Miniconda": "Anaconda's installer and terms: Miniforge (install_miniforge)",
    "apt-get install": "apt outside apt_install, which works under a root-only fakeroot",
    "wget ": "a download without a checked hash: fetch <url> <sha256> <file>",
}


def _new_recipes() -> list[Path]:
    return sorted(d for d in DEFS.glob("*.def") if d.stem not in OLD_RECIPES)


def test_old_recipes_exist():
    """The allow-list names only defs that are there, so a replaced image's old recipe leaves it with the def."""
    missing = sorted(name for name in OLD_RECIPES if not (DEFS / f"{name}.def").exists())
    assert not missing, f"OLD_RECIPES names defs that are gone, take them out: {missing}"


@pytest.mark.parametrize("def_path", _new_recipes(), ids=lambda p: p.stem)
def test_every_def_follows_the_recipe(def_path):
    text = def_path.read_text()
    name = def_path.stem
    assert name.endswith("_ubuntu24.04"), f"{name}: an image built on D17's base says so in its name"
    stages = re.findall(r"^Bootstrap:", text, re.M)
    bases = re.findall(r"^From:\s*(\S+)", text, re.M)
    assert len(bases) == len(stages) >= 1, f"{name}: one From per stage"
    assert bases[-1] == UBUNTU_2404, f"{name}: the image ends on ubuntu:24.04 by digest, not {bases[-1]}"
    assert all(b in (UBUNTU_2404, CUDA_DEVEL_2404) for b in bases[:-1]), f"{name}: a build stage on {bases[:-1]}"
    for marker, field in (
        ("build_env.sh /opt/crboost/build_env.sh", "%files"),
        (". /opt/crboost/build_env.sh", "%post"),
    ):
        assert text.count(marker) == len(stages), f"{name}: every stage has `{marker}` in its {field}"
    # What the recipe does, without its comments (which may well name what it no longer does).
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    for word, why in FORBIDDEN.items():
        assert word not in code, f"{name}: `{word}`: {why}"
    for ref in re.findall(r"git checkout (\S+)", code):
        assert re.fullmatch(r"[0-9a-f]{40}", ref), f"{name}: git checkout {ref}: a source is checked out by commit"
    for source in re.findall(r"^%files\s*\n((?:[ \t]+\S.*\n)+)", text, re.M):
        for line in source.splitlines():
            src = line.split()[0]
            assert (DEFS / src).exists(), f"{name}: %files names {src}, which is not in container_defs/"
    labels = dict(re.findall(r"^\s+(crboost\.\w+)\s+(\S+)", text, re.M))
    assert labels.get("crboost.def") == name, f"{name}: %labels crboost.def {name}"
    tools = {*config_service.get_config_service().config.tools, *VIEWER_IMAGES}
    assert labels.get("crboost.tool") in tools, f"{name}: %labels crboost.tool names a tool of the conf template"


@pytest.mark.repo
def test_apt_reads_a_dated_snapshot():
    """apt_install reads Ubuntu's archive of one date (roadmap 63 S7): a rebuild gets the packages the image had."""
    build_env = (DEFS / "build_env.sh").read_text()
    date = re.search(r"^APT_SNAPSHOT=(\d{8}T\d{6}Z)$", build_env, re.M)
    assert date, "build_env.sh names APT_SNAPSHOT as a snapshot.ubuntu.com moment"
    assert 'APT::Snapshot \\"$APT_SNAPSHOT\\"' in build_env


def test_images_build_with_crboosts_uv():
    """An image's uv (build_env.sh's install_uv) is the one uv.toml pins for crboost's own environment."""
    pinned = re.search(r'required-version = "==([\d.]+)"', (REPO / "uv.toml").read_text()).group(1)
    pinned_sum = re.search(r'sha256 = "([0-9a-f]{64})"', (REPO / "uv.toml").read_text()).group(1)
    build_env = (DEFS / "build_env.sh").read_text()
    assert f"/releases/download/{pinned}/uv-x86_64-unknown-linux-gnu.tar.gz" in build_env, pinned
    assert pinned_sum in build_env, pinned_sum

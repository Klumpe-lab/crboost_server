"""Export the locks of a running image's environment, for a def to install from (roadmap 63 D19).

    ./crboost python container_defs/export_locks.py <image.sif> <env prefix in the image> <out stem> [--pypi-hashes]
        [--no-conda]

Writes `<out stem>.conda.lock` (`conda list --explicit --md5`: every conda package by URL and md5, which conda checks)
and, if the environment has packages pip installed, `<out stem>.pip.lock` (`name==version`, a git package as
`name @ git+<url>@<commit>`). --no-conda for an environment without conda (a venv, a Python image's own), which the
def rebuilds with uv: only the pip lock. With --pypi-hashes every version is looked up on PyPI and all its files'
sha256 written, for `--require-hashes`; a package PyPI does not have (a git one, a +local version from another index)
makes that impossible, and the export says so and writes none. A package pip installed from a local folder or archive
cannot be locked: it is listed on stderr, and its def installs it from its source (RELION's `.[vis]`).

Reads only: runs `apptainer exec` on the image, never writes into it. A lock is changed on purpose, never by a rebuild.
"""

import argparse
import datetime
import json
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

# Run inside the image's python: every installed distribution with its installer and where it came from.
_DISTRIBUTIONS = r"""
import json
from importlib import metadata
rows = []
for d in metadata.distributions():
    def text(name):
        try:
            return (d.read_text(name) or "").strip()
        except OSError:
            return ""
    rows.append({"name": d.metadata["Name"], "version": d.version, "installer": text("INSTALLER"),
                 "direct_url": text("direct_url.json")})
print(json.dumps(rows))
"""


def _in_image(sif: Path, *cmd: str) -> str:
    return subprocess.run(
        ["apptainer", "exec", "--cleanenv", "--no-home", str(sif), *cmd], check=True, capture_output=True, text=True
    ).stdout


def _pip_line(row: dict) -> tuple[str | None, str | None]:
    """The lock line of a pip-installed distribution, or the reason it has none."""
    if not row["direct_url"]:
        return f"{row['name']}=={row['version']}", None
    url = json.loads(row["direct_url"])
    vcs = url.get("vcs_info")
    if vcs:
        return f"{row['name']} @ {vcs['vcs']}+{url['url']}@{vcs['commit_id']}", None
    return None, f"{row['name']} {row['version']} came from {url['url']}"


def _pypi_hashes(name: str, version: str) -> list[str]:
    try:
        with urllib.request.urlopen(f"https://pypi.org/pypi/{name}/{version}/json", timeout=60) as response:
            files = json.load(response)["urls"]
    except urllib.error.HTTPError as e:
        # 404: PyPI does not have this version (another index's +local build).
        if e.code == 404:
            return []
        raise
    return [f["digests"]["sha256"] for f in files]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("sif", type=Path)
    ap.add_argument("prefix", help="the environment's prefix in the image, e.g. /opt/miniforge3/envs/easymode")
    ap.add_argument("out_stem", type=Path, help="container_defs/<def name>")
    ap.add_argument("--pypi-hashes", action="store_true")
    ap.add_argument("--no-conda", action="store_true", help="a venv or a Python image's own environment")
    a = ap.parse_args()

    stamp = f"exported {datetime.date.today()} from {a.sif.name} ({a.sif.stat().st_size / 1e9:.1f} GB), {a.prefix}"
    if not a.no_conda:
        conda = str(Path(a.prefix).parents[1] / "bin" / "conda")
        explicit = _in_image(a.sif, conda, "list", "-p", a.prefix, "--explicit", "--md5")
        packages = [line for line in explicit.splitlines() if line.startswith(("http://", "https://"))]
        conda_lock = a.out_stem.with_name(a.out_stem.name + ".conda.lock")
        conda_lock.write_text(
            f"# The conda packages of a running image's environment, {stamp}:\n"
            "# conda list --explicit --md5; conda checks each package's md5.\n@EXPLICIT\n" + "\n".join(packages) + "\n"
        )
        print(f"{conda_lock}: {len(packages)} conda packages")

    rows = json.loads(_in_image(a.sif, f"{a.prefix}/bin/python", "-c", _DISTRIBUTIONS))
    lines, unlockable = [], []
    for row in sorted(rows, key=lambda r: r["name"].lower()):
        if row["installer"] in ("conda", ""):
            continue
        line, why = _pip_line(row)
        if line:
            lines.append(line)
        else:
            unlockable.append(why)
    for why in unlockable:
        print(f"not locked, install it from its source in the def: {why}", file=sys.stderr)
    if not lines:
        return 0

    note = "Installed with --no-deps."
    if a.pypi_hashes:
        hashed, missing = [], []
        for line in lines:
            name, _, version = line.partition("==")
            digests = _pypi_hashes(name, version) if version else []
            if digests:
                hashed.append(line + "".join(f" --hash=sha256:{d}" for d in digests))
            else:
                missing.append(line)
        if missing:
            print(f"no hashes written: PyPI has no files for {', '.join(missing)}", file=sys.stderr)
        else:
            lines = hashed
            note = "Every file PyPI has for each version is hashed: installed with --no-deps --require-hashes."
    pip_lock = a.out_stem.with_name(a.out_stem.name + ".pip.lock")
    pip_lock.write_text(
        f"# The pip packages of a running image's environment, {stamp}.\n# {note}\n" + "\n".join(lines) + "\n"
    )
    print(f"{pip_lock}: {len(lines)} pip packages")
    return 0


if __name__ == "__main__":
    sys.exit(main())

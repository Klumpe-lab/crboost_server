"""Assertion #0 of every run: the frozen input (roadmap 14 §runner).

`test/input.yaml` lists the files the protocol was validated on. `freeze-input` writes
their sizes + sha256 after the download; until then only presence can be checked and the
run reports "input not frozen" instead of pretending the data was verified.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from services.protocols.schema import Protocol
from services.result import err, ok

logger = logging.getLogger(__name__)

INPUT_MANIFEST = "input.yaml"
EMPIAR_HTTPS_ROOT = "https://ftp.ebi.ac.uk/empiar/world_availability"


@dataclass
class InputCheck:
    input_dir: Path
    ok: bool
    frozen: bool
    movies_glob: str = "*.eer"
    mdocs_glob: str = "*.mdoc"
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def input_dir_for(protocol: Protocol, input_root: Path) -> Path:
    return Path(input_root) / "input" / protocol.name


def read_manifest(protocol: Protocol) -> dict:
    test_dir = protocol.test_dir
    path = test_dir / INPUT_MANIFEST if test_dir else None
    if path is None or not path.exists():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def check_input(protocol: Protocol, input_root: Path, *, verify_checksums: bool = True) -> InputCheck:
    """Presence of every manifest file; size + sha256 when the manifest carries them."""
    manifest = read_manifest(protocol)
    input_dir = input_dir_for(protocol, input_root)
    chk = InputCheck(
        input_dir=input_dir,
        ok=True,
        frozen=False,
        movies_glob=str(manifest.get("movies_glob") or "*.eer"),
        mdocs_glob=str(manifest.get("mdocs_glob") or "*.mdoc"),
    )
    files = manifest.get("files") or []
    if not files:
        chk.ok = False
        chk.problems.append(
            f"{protocol.name}: test/{INPUT_MANIFEST} lists no files — nothing to check the input against"
        )
        return chk
    if not input_dir.is_dir():
        chk.ok = False
        chk.problems.append(f"input directory missing: {input_dir} (see `crboost_regress.py fetch-input`)")
        return chk
    chk.frozen = all(entry.get("sha256") for entry in files)
    if not chk.frozen:
        chk.notes.append("input not frozen (no checksums in test/input.yaml) — presence checked only")
    for entry in files:
        name = str(entry.get("name", ""))
        p = input_dir / name
        if not p.exists():
            chk.problems.append(f"missing input file: {name}")
            continue
        size = entry.get("size")
        if size is not None and p.stat().st_size != int(size):
            chk.problems.append(f"size mismatch: {name} is {p.stat().st_size} B, manifest says {size} B")
            continue
        digest = entry.get("sha256")
        if digest and verify_checksums and sha256_of(p) != digest:
            chk.problems.append(f"checksum mismatch: {name}")
    chk.ok = not chk.problems
    return chk


def freeze_input(protocol: Protocol, input_root: Path) -> Path:
    """Write sizes + sha256 of every listed file into test/input.yaml. The list of names is
    the manifest's own (hand-authored); freezing never adds or removes files."""
    test_dir = protocol.test_dir
    if test_dir is None:
        raise ValueError(f"protocol '{protocol.name}' is not bound to a bundle directory")
    manifest = read_manifest(protocol)
    files = manifest.get("files") or []
    if not files:
        raise ValueError(f"test/{INPUT_MANIFEST} lists no files to freeze")
    input_dir = input_dir_for(protocol, input_root)
    for entry in files:
        p = input_dir / str(entry["name"])
        if not p.exists():
            raise FileNotFoundError(f"cannot freeze: {p} is missing")
        entry["size"] = p.stat().st_size
        entry["sha256"] = sha256_of(p)
    manifest["files"] = files
    path = test_dir / INPUT_MANIFEST
    header = (
        f"# Frozen input of {protocol.name}: written by `crboost_regress.py freeze-input`; the file list is\n"
        "# hand-authored, sizes + sha256 are measured. Lives under <local_data.root>/input/<protocol>/.\n"
    )
    path.write_text(header + yaml.safe_dump(manifest, sort_keys=False, default_flow_style=None, width=110))
    return path


def download_commands(protocol: Protocol, input_root: Path) -> list[str]:
    """The exact shell lines that fetch the frozen input from EMPIAR over HTTPS: one explicit
    `wget` per manifest file (no crawling — EBI's robots.txt stops a recursive wget after the
    index page; brackets in the names are percent-encoded for the URL, decoded on disk)."""
    empiar = str(protocol.provenance.get("empiar", "")).replace("EMPIAR-", "")
    sub = str(protocol.provenance.get("empiar_path", "")).strip("/")
    input_dir = input_dir_for(protocol, input_root)
    names = [str(e.get("name", "")) for e in (read_manifest(protocol).get("files") or []) if e.get("name")]
    if not empiar or not names:
        return [
            f"# protocol '{protocol.name}' has no provenance.empiar or input.yaml files; put the data under {input_dir}"
        ]
    url = f"{EMPIAR_HTTPS_ROOT}/{empiar}/{sub}" if sub else f"{EMPIAR_HTTPS_ROOT}/{empiar}"
    quoted = " ".join("'" + n.replace("'", "'\\''") + "'" for n in names)
    return [
        f'D="{input_dir}"',
        f'U="{url}"',
        'mkdir -p "$D"',
        'chmod -R u+w "$D"',
        f"for f in {quoted}; do",
        '  e="${f//\\[/%5B}"; e="${e//\\]/%5D}"',
        '  wget -c -q --show-progress -P "$D" "$U/$e" || { echo "FAILED: $f"; exit 1; }',
        "done",
        'chmod -R a-w "$D"',
        f"venv/bin/python3 crboost_regress.py freeze-input {protocol.name}",
    ]


# ── in-process download (the Protocols dialog's "Download" and `fetch-input`) ─────────────

ProgressCb = Callable[[int, int, str], None]
_CHUNK = 1 << 20
_ATTEMPTS = 3
_PROGRESS_EVERY = 8 << 20


def empiar_url(protocol: Protocol) -> str | None:
    empiar = str(protocol.provenance.get("empiar", "")).replace("EMPIAR-", "")
    sub = str(protocol.provenance.get("empiar_path", "")).strip("/")
    if not empiar:
        return None
    return f"{EMPIAR_HTTPS_ROOT}/{empiar}/{sub}" if sub else f"{EMPIAR_HTTPS_ROOT}/{empiar}"


def download_input(protocol: Protocol, input_root: Path, *, progress_cb: ProgressCb | None = None) -> dict:
    """Fetch every manifest file over HTTPS into `input/<protocol>/` (streamed to `.part`,
    resumed with a Range request, 3 attempts per file), then FREEZE the manifest (sizes +
    sha256 into test/input.yaml) — or verify it when it already is frozen — and lock the
    directory read-only. Blocking: run it in a thread. `ok(n_files, bytes, input_dir,
    manifest, frozen_now)` / `err(...)`."""
    manifest = read_manifest(protocol)
    files = [e for e in (manifest.get("files") or []) if e.get("name")]
    base = empiar_url(protocol)
    if not files or base is None:
        return err(f"protocol '{protocol.name}' has no provenance.empiar or no test/{INPUT_MANIFEST} file list")
    input_dir = input_dir_for(protocol, input_root)
    input_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(input_dir, 0o755)
    total = len(files)

    def report(i: int, msg: str) -> None:
        if progress_cb is not None:
            progress_cb(i, total, msg)

    total_bytes = 0
    for i, entry in enumerate(files):
        name = str(entry["name"])
        dst = input_dir / name
        want = entry.get("size")
        if dst.exists() and (want is None or dst.stat().st_size == int(want)):
            total_bytes += dst.stat().st_size
            report(i + 1, f"{name}: present")
            continue
        if dst.exists():
            dst.chmod(0o644)
            dst.unlink()  # wrong size: not this file
        url = f"{base}/{urllib.parse.quote(name)}"
        last_error: Exception | None = None
        for attempt in range(1, _ATTEMPTS + 1):
            try:
                total_bytes += _download_one(url, dst, lambda m, i=i, name=name: report(i, f"{name}: {m}"))
                last_error = None
                break
            except (urllib.error.URLError, OSError, TimeoutError) as e:
                last_error = e
                logger.warning("download %s attempt %d/%d failed: %s", name, attempt, _ATTEMPTS, e)
                time.sleep(5)
        if last_error is not None:
            return err(f"{name}: download failed after {_ATTEMPTS} attempts ({last_error})", input_dir=str(input_dir))
        report(i + 1, f"{name}: done")

    frozen_now = not all(e.get("sha256") for e in files)
    report(total, "hashing (sha256) …" if frozen_now else "verifying checksums …")
    if frozen_now:
        manifest_path = freeze_input(protocol, input_root)
    else:
        manifest_path = protocol.test_dir / INPUT_MANIFEST
        chk = check_input(protocol, input_root, verify_checksums=True)
        if not chk.ok:
            return err("downloaded files do not match the frozen manifest: " + "; ".join(chk.problems))
    for entry in files:
        (input_dir / str(entry["name"])).chmod(0o444)
    os.chmod(input_dir, 0o555)
    return ok(
        n_files=total, bytes=total_bytes, input_dir=str(input_dir), manifest=str(manifest_path), frozen_now=frozen_now
    )


# TLS trust. The venv's Python links an EasyBuild OpenSSL whose default cert store does not
# carry the cluster's TLS-interception CA, so a plain urlopen dies with
# CERTIFICATE_VERIFY_FAILED while the system wget (system bundle) succeeds. Verify against the
# system bundle; verification itself is never switched off.
_CA_BUNDLE_ENV = ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")
_CA_BUNDLE_PATHS = (
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/ssl/cert.pem",
)


def _ssl_context() -> ssl.SSLContext:
    for var in _CA_BUNDLE_ENV:
        p = os.environ.get(var, "")
        if p and Path(p).exists():
            return ssl.create_default_context(cafile=p)
    for p in _CA_BUNDLE_PATHS:
        if Path(p).exists():
            return ssl.create_default_context(cafile=p)
    return ssl.create_default_context()


def _download_one(url: str, dst: Path, report: Callable[[str], None]) -> int:
    """Stream `url` into `dst.part` (resuming a partial file with a Range request), then
    rename. Falls back to the system `wget` when Python cannot verify the server's
    certificate even with the system bundle (wget has already proven the trust chain on
    this host). Returns the final size in bytes."""
    try:
        return _download_urllib(url, dst, report)
    except urllib.error.URLError as e:
        if not isinstance(e.reason, ssl.SSLError) or shutil.which("wget") is None:
            raise
        logger.warning("TLS verification failed in Python (%s); falling back to the system wget", e.reason)
        report("TLS verify failed in Python — using system wget")
        return _download_wget(url, dst)


def _download_wget(url: str, dst: Path) -> int:
    part = dst.with_name(dst.name + ".part")
    proc = subprocess.run(["wget", "-c", "-q", "-O", str(part), url], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise OSError(f"wget rc={proc.returncode}: {proc.stderr.strip()[-300:]}")
    part.replace(dst)
    return dst.stat().st_size


def _download_urllib(url: str, dst: Path, report: Callable[[str], None]) -> int:
    part = dst.with_name(dst.name + ".part")
    have = part.stat().st_size if part.exists() else 0
    headers = {"Range": f"bytes={have}-"} if have else {}
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=120, context=_ssl_context())
    except urllib.error.HTTPError as e:
        if e.code == 416 and have:  # the .part already holds the whole file
            part.replace(dst)
            return dst.stat().st_size
        raise
    with resp:
        if have and resp.status != 206:
            have = 0  # server ignored the range: start over
        mode = "ab" if have else "wb"
        length = int(resp.headers.get("Content-Length") or 0)
        total = have + length
        done = have
        since = 0
        with open(part, mode) as f:
            while True:
                chunk = resp.read(_CHUNK)
                if not chunk:
                    break
                f.write(chunk)
                done += len(chunk)
                since += len(chunk)
                if since >= _PROGRESS_EVERY:
                    since = 0
                    report(f"{done / 2**20:.0f}/{total / 2**20:.0f} MB" if total else f"{done / 2**20:.0f} MB")
    if total and done != total:
        raise OSError(f"short read: {done} of {total} bytes")
    part.replace(dst)
    return done

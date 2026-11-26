# Container images

One Apptainer definition per image the tools run in (`config/conf.yaml`, `tools.<tool>.container_path`; napari and
ChimeraX are started by the curation worker). A site pulls the images and does not build them (roadmap 63 D20); the
defs are how they are made, and CI builds every one of them to show they still build.

## One recipe (roadmap 63 D17, D19)

Every def built after 2026-11-25 has the same shape, which `tests/test_repo.py` checks:

- **One base.** The image ends on `ubuntu:24.04`, pinned by digest. Where CUDA code is compiled (RELION, AreTomo2) a
  build stage on `nvidia/cuda:12.9.1-devel-ubuntu24.04`, also by digest, does it, and only what runs is copied over:
  no toolkit, no nvcc, no NVIDIA base in an image. Those compile with GCC 12: CUDA 12.9's libcu++ headers fail under
  24.04's GCC 13.
- **`build_env.sh`**, put in each stage by `%files` and sourced first in its `%post`: `apt_install` (apt under any
  fakeroot), `fetch` (a download kept only if its sha256 matches), `install_miniforge`, `install_uv`, `ldd_complete`,
  and a site's CA bundle for every downloader when the build is given one. It stays in the image at
  `/opt/crboost/build_env.sh`.
- **Environments from locks** exported from the image that ran before (`export_locks.py`):
  `<name>.conda.lock` (every conda package by URL and md5) and `<name>.pip.lock` (`name==version`, by sha256 wherever
  PyPI has every file). Conda images install through Miniforge, conda-forge only; pip-only images (tilt_filter,
  napari) through uv, the version `uv.toml` pins. A lock changes on purpose, never by a rebuild.
- **apt from one date.** `apt_install` reads Ubuntu's archive as it was at `APT_SNAPSHOT` (`build_env.sh`, through
  snapshot.ubuntu.com), so a rebuild gets the packages the image had; the date moves on purpose, like a lock.
  Sources are pinned the same way: a base by digest, a download by sha256, a git checkout by commit.
- **Checks at the end of `%post`**, since a failing `%test` fails nothing: versions, `pip check`, imports, that every
  binary loads its libraries, the GPU architectures compiled in (sm_100 for the B200s, PTX beyond).
- **Never** a TLS check turned off, a `latest` download, Anaconda's channels, `wget`, or apt outside `apt_install`.
- **Labels** `crboost.tool`, `crboost.def` (the file's name) and the OCI ones; `PYTHONNOUSERSITE=1`.
- **Names** `<tool>-<version>_<key libraries>_ubuntu24.04`. A changed recipe makes a new name: an image file is never
  overwritten, since running jobs have it mounted.

Defs of images that still run from an older recipe are listed in `OLD_RECIPES` (`tests/test_repo.py`); each leaves
with its image.

## Building

```sh
container_defs/build.sh <name> <out_dir>      # <name>.def -> <out_dir>/<name>.sif
```

As root it builds plainly; otherwise always with `--fakeroot --ignore-fakeroot-command`. Why: a build runs as root
inside the image, and without real root Apptainer fakes it in a user namespace whose size the admin sets in
`/etc/subuid`. With a range there, the flag changes nothing. Without one (CBE.next, Munich) only root is mapped, and
Apptainer would otherwise bring the host's `fakeroot` binaries into the image, which fail on any image whose glibc is
older than the host's ("GLIBC_2.38 not found"); `apt_install` makes apt work under the root-only mapping (apt stays
root; `chown`/`chgrp` ignore what cannot be mapped while packages install). A host that allows no user namespaces at
all cannot build without root; `build.sh` says so.

By environment:

- `APPTAINER_TMPDIR`: where the build unpacks. Not on BeeGFS (apt's EBUSY): a folder under `/dev/shm` on a cluster
  node, which counts against the job's memory (a two-stage RELION build holds about 40 GB) and is removed afterwards.
- `CRBOOST_BUILD_CA`: the CA bundle (PEM) every download is checked against. By default the host's own
  (`/etc/ssl/certs/ca-certificates.crt`, `/etc/pki/tls/certs/ca-bundle.crt`, ...): a base image has none until apt
  installs one, and apt reaches its snapshot over HTTPS. Behind a proxy that inspects TLS, the bundle that holds the
  proxy's CA. Bound into the build, never copied into the image.

On CBE.next a build is a job on partition `c` (16 CPUs: pytom 4 min, RELION and Warp about 15). Its `sbatch` passes no
environment on, so the setting goes inside the command:

```sh
sbatch -p c -c 16 --mem 160G -t 3:00:00 -o build_%j.log --wrap \
  'APPTAINER_TMPDIR=/dev/shm/build-$SLURM_JOB_ID <checkout>/container_defs/build.sh <name> <out_dir>'
```

## A new lock

```sh
./crboost python container_defs/export_locks.py <image.sif> <env prefix> container_defs/<name> --pypi-hashes
```

`--no-conda` for a venv or a Python image's own environment. It reads the image and writes the locks beside the defs.

## Checks before an image is used

1. Its build passes (the checks in `%post`).
2. `./crboost test -m cluster -k <tool>` from a checkout whose `config/conf.yaml` names the new image.
3. One real job's tool commands rerun with the old and the new image on the same inputs, outputs compared (roadmap 63
   §7 lists what was compared for each image).

## Bringing an image to a site

Copy it beside the old one under its new name and change that tool's `container_path`. Each job reads the conf when it
starts, so the change reaches every job that starts after it, queued ones included: swap an image between a project's
runs, not in the middle of one. The old file goes once nothing uses it.

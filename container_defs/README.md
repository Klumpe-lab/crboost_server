# Container images

One Apptainer definition per image the tools run in (`config/conf.yaml`, `tools.<tool>.container_path`). A def that
installs a Python environment carries the locks beside it, `<name>.conda.lock` (an explicit list: URL and md5 per
package) and `<name>.pip.lock` (version and sha256 per package), exported from the image that runs; the def installs
from them, so a rebuild resolves nothing and gets the same packages.

## Building

```sh
container_defs/build.sh <name> <out_dir>      # <name>.def -> <out_dir>/<name>.sif
```

It never overwrites an image (a running job may have it mounted): a new image gets a new name. Two settings, by
environment:

- `APPTAINER_TMPDIR`: where the build unpacks. Not on BeeGFS: apt fails there now and then (renaming a downloaded
  .deb gives EBUSY; seen with Ubuntu 22.04 and 24.04). A folder under `/dev/shm` works; it counts against the job's
  memory (a two-stage RELION build holds about 40 GB) and is removed afterwards.
- `CRBOOST_FAKEROOT=ignore-command` on a host with no subuid range for the user (CBE.next):
  `--fakeroot --ignore-fakeroot-command`, under which only root is mapped. The defs keep apt running as root and
  divert `chown`/`chgrp` while packages install; that block is harmless under real root.

On CBE.next a build runs as a job on partition `c` (16 CPUs: pytom 6 min, relion and warp 15 min). Its `sbatch` passes
no environment on, so the settings go inside the command:

```sh
sbatch -p c -c 16 --mem 160G -t 3:00:00 -o build_%j.log --wrap 'CRBOOST_FAKEROOT=ignore-command \
  APPTAINER_TMPDIR=/dev/shm/build-$SLURM_JOB_ID <checkout>/container_defs/build.sh <name> <out_dir>'
```

## Checks

A failing `%test` does not fail an Apptainer build, so each def checks itself at the end of `%post` (imports, versions,
what can be checked without a GPU). The GPU check is crboost's own: point `tools.<tool>.container_path` in a test
checkout's `config/conf.yaml` at the new image and run `./crboost test -m cluster -k <tool>`, then rerun one real job
on it and compare its output with the job's.

## Bringing an image to a site

Copy it beside the old one under its new name and change that tool's `container_path`. Each job reads the conf when it
starts, so the change reaches every job that starts after it, queued ones included: swap an image between a project's
runs, not in the middle of one. The old file goes once nothing uses it.

## Bases

Pinned by digest (Apptainer takes a tag or a digest, not both). No image ends on NVIDIA's CUDA devel base: where a tool
compiles CUDA code, it does so in a build stage, and the final stage carries only what runs (roadmap 63 S7).

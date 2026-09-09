# Protocols — how to make one

A protocol is the shape of a pipeline with its parameters: which jobs, in what order, with which
parameters and species. It is not tied to any project. From a protocol you create a **regular**
project (landing page → Protocols → Create project) and run it with the normal Run button.

## Where they live

```
config/protocols/<name>/        shared, checked into the repo
~/.crboost/protocols/<name>/    personal
```

A bundle is one directory:

```
protocol.yaml    the protocol
assets/          template / mask volumes the species entries point at (bundle-relative paths)
```

The same name in both roots resolves to the repo one first. A bundle that fails to load is still
listed, with its error.

## The easy way: export a project that worked

Workspace → foot-of-rail protocol light → Protocols view → **Save this project as a protocol**, or:

```
venv/bin/python3 crboost_protocol.py export /path/to/project --name my-flow --out ~/.crboost/protocols/my-flow
```

Export writes every stage of the project's pipeline with its FULL parameter set, the species each
stage is bound to, and copies the template / mask into `assets/`. It records what the project DID;
edit the yaml to say what the protocol should PIN (drop what was incidental, fix values to the
ones you mean). Then:

```
venv/bin/python3 crboost_protocol.py validate my-flow
venv/bin/python3 crboost_protocol.py list
```

## By hand

```yaml
name: my-flow
version: 1
description: one line
provenance: {tutorial: "https://…"}      # free-form, optional

species:                                 # optional; only for particle-phase stages
  - id: copia                            # becomes the project species id and the `{job}__copia` instance suffix
    name: Copia
    diameter_ang: 550.0
    symmetry: I1
    template: {asset: assets/copia_template.mrc, polarity: black, lowpass_ang: 45.0}
    mask: {asset: assets/copia_mask.mrc, method: relion, threshold: 1.85}
    extraction: {box_size: 384, crop_size: 224, binning: 1.0}

stages:                                  # in pipeline order
  - job: importmovies
    params: {optics_group_name: opticsGroup1, do_at_most: -1}
  - job: tsReconstruct
    params: {rescale_angpixs: 11.8, halfmap_frames: 1, deconv: 1, perdevice: 1, array_throttle: 20}
  - job: templatematching
    species: copia                       # instance id templatematching__copia
    params: {angular_search: "90.0", …}
```

Rules the validator enforces:

- `job` is a job type value (`importmovies`, `fsMotionAndCtf`, `tsImport`, `aligntiltsWarp`, `tsCtf`,
  `tsReconstruct`, `templatematching`, `tmextractcand`, `subtomoExtraction`, `reconstructParticle`,
  `class3d`, …); `params` keys are that job's user parameters — the fields of its job tab
  (`USER_PARAMS` of the param class in `services/jobs/`).
- Every prerequisite and dependency of a stage is itself a stage. Nothing is auto-added.
- Params are full snapshots, not diffs: a value the protocol does not name takes the code default,
  and apply reports it as "not covered by the protocol".
- No absolute path anywhere. Species assets are `assets/<file>` inside the bundle.
- A template's pixel size must equal the `tsReconstruct.rescale_angpixs` the protocol pins.

Unknown fields and rejected values are reported at create time and never silently substituted.

## Using one

Landing page → **Protocols** → click the name to read every stage → **Create project** (name, base,
movies and mdocs globs, optional gain reference). The project opens in the workspace with the
stages in the roster; press Run. In the workspace, the Protocols view shows each stage's parameters
as the protocol pinned them beside what the job holds now, with anything changed since marked
"edited". `crboost_protocol.py apply <name> --name … --base … --movies … --mdocs …` does the same
from a terminal.

Reference bundle: `config/protocols/copia-empiar12580/` — the CryoBoost v1 copia tutorial
(EMPIAR-12580), 11 stages importmovies → class3d.

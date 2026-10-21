#!/usr/bin/env python3
"""
segment driver — supervisor + one SLURM array task per tomogram.

- Supervisor (SLURM_ARRAY_TASK_ID unset): reads the input tomograms STAR, settles what runs for each
  feature (a network of easymode's weights cache, or the one Ais model), reads the tool versions
  from the image, submits the array with per-task GPU resources and, once it drains, writes
  segmentations.json and makes the tilt-series registry say the same. The server registers the
  features when it sees the job succeed.

- Task: links its tomogram into a directory of its own as <tomogram>.mrc, runs `easymode segment`
  (or `ais segment`) on it into segmentations/, checks every volume it promised, and leaves the
  overlay masks and fractions in analytics/segment/<tomogram>.json for the supervisor.

How a task runs the tools follows from three things they do:
- easymode tells online from offline by fetching huggingface.co itself; HF_HUB_OFFLINE does not
  reach that check. Online, it overwrites its registry with the remote copy and downloads any
  weights newer than the cached ones. The task points it at a proxy that refuses every connection,
  so the check fails at once and the run reads the cache as it was frozen.
- easymode and Ais rewrite their settings files at every start, in the directories that
  EASYMODE_CACHE_DIR and AIS_CONFIG_DIR name. Concurrent tasks sharing one would read each other's
  half-written files, so each task has its own, with the cache's models/ linked into it.
- Both claim a tomogram by writing a 10³ placeholder under the output name, and skip outputs that
  exist. A task removes what an interrupted attempt left before it runs; complete volumes are kept,
  so only the missing features are computed again. Volumes made with other settings are removed by
  the supervisor before a re-run (`_settle_previous_run`).
"""

import json
import os
import shlex
import subprocess
import tempfile
from pathlib import Path

from drivers.array_job_base import STATUS_DIR_NAME, ArrayDriver, ArrayResults, read_manifest
from drivers.driver_base import DriverContext, ToolCommand, print_cmd_only, require_producer_input
from drivers.template_match_pytom import read_global_block, resolve_tomogram_path
from services.computing.container_service import get_container_service
from services.job_lineage import recon_of
from services.jobs.segment import SegmentParams
from services.models_base import SegmentFormat, SegmentMethod
from services.segmentation.easymode import TOOL_NAME, ais_model_title, easymode_cache_dir, file_sha256, read_catalog
from services.segmentation.manifest import (
    ENGINE_AIS,
    ENGINE_NATIVE,
    SegmentationEntry,
    SegmentationManifest,
    feature_slug,
    tomogram_outputs,
    write_manifest,
)
from services.segmentation.volumes import summarize_volume, volume_problem
from services.tilt_series import ModelRef, get_registry_for

OUTPUT_DIR = "segmentations"
VIS_DIR = Path("vis") / "segment"
SIDECAR_DIR = Path("analytics") / "segment"
# Nothing listens there, so easymode's online check fails at once (see the module docstring).
OFFLINE_PROXY = "http://127.0.0.1:9"
# The distribution each tool's version is read from, in the image.
_DISTRIBUTIONS = {"easymode": "easymode", "ais": "ais-cryoet"}


def _volume_path(job_dir: Path, tomo: str, target: dict) -> Path:
    return job_dir / OUTPUT_DIR / f"{tomo}__{target['suffix']}.mrc"


def _overlay_paths(job_dir: Path, tomo: str, target: dict) -> tuple[Path, Path]:
    vis = job_dir / VIS_DIR
    return vis / f"{tomo}__{target['feature']}_xy.png", vis / f"{tomo}__{target['feature']}_xz.png"


def _sidecar_path(job_dir: Path, tomo: str) -> Path:
    return job_dir / SIDECAR_DIR / f"{tomo}.json"


def _ais_model(params: SegmentParams) -> Path:
    return Path((params.ais_model_path or "").strip())


def _settings(params: SegmentParams) -> dict:
    """The run's settings that shape every volume: TTA, and the storage format of easymode's own
    networks."""
    easymode = params.method == SegmentMethod.EASYMODE
    return {"tta": params.tta, "format": SegmentFormat(params.output_format).value if easymode else None}


def _identity(target: dict, settings: dict) -> tuple:
    """What one feature's volumes depend on besides the tomogram: the network that makes them and the
    run's settings (the format reaches only easymode's own networks)."""
    model = target["model"]
    fmt = settings.get("format") if target["engine"] == ENGINE_NATIVE else None
    return target["suffix"], target["engine"], model["variant"], model["weights_sha256"], settings.get("tta"), fmt


def _grid(mrc: Path) -> tuple[tuple[int, int, int], int, float]:
    """(shape (z, y, x), bytes per voxel, voxel size in Å) from an MRC header."""
    import mrcfile

    with mrcfile.open(mrc, header_only=True, permissive=True) as m:
        h = m.header
        itemsize = mrcfile.utils.data_dtype_from_header(h).itemsize
        return (int(h.nz), int(h.ny), int(h.nx)), itemsize, float(m.voxel_size.x)


def _relink(link: Path, target: Path) -> None:
    if link.is_symlink() or link.exists():
        link.unlink()
    link.symlink_to(target)


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def resolve_targets(params: SegmentParams) -> list[dict]:
    """What the job runs, one entry per feature: its id, the suffix of its volume file (an Ais model's
    title as it is, which `ais segment` puts in the name), the engine and the model reference.
    Raises naming what is missing."""
    if params.method == SegmentMethod.AIS_MODEL:
        scnm = _ais_model(params)
        if not scnm.name or not scnm.is_file():
            raise FileNotFoundError(f"Ais model not found: {params.ais_model_path!r}")
        title = ais_model_title(scnm)
        model = ModelRef(tool="ais", feature=feature_slug(title), weights_sha256=file_sha256(scnm))
        return [{"feature": model.feature, "suffix": title, "engine": ENGINE_AIS, "model": model.model_dump()}]

    if not params.features:
        raise ValueError("No features chosen: pick them in the job's Parameters")
    catalog = read_catalog(easymode_cache_dir())
    unknown = sorted(set(params.features) - set(catalog))
    if unknown:
        raise ValueError(f"Not in easymode's weights cache: {unknown}; it holds {sorted(catalog)}")
    targets = []
    for name in sorted(set(params.features)):
        entry = catalog[name]
        if not entry.weights.is_file():
            raise FileNotFoundError(f"The weights of {name} are not in the cache: {entry.weights}")
        model = ModelRef(
            tool="easymode",
            feature=name,
            variant=entry.variant,
            timestamp=entry.timestamp,
            weights_sha256=entry.weights_sha256,
        )
        # easymode hands an Ais-engine feature to `ais segment`, which names the volume after the
        # model's title, not after the feature.
        suffix = ais_model_title(entry.weights) if entry.engine == ENGINE_AIS else name
        targets.append({"feature": name, "suffix": suffix, "engine": entry.engine, "model": model.model_dump()})
    return targets


class SegmentDriver(ArrayDriver):
    params_class = SegmentParams
    job_name = "segment"
    driver_script = Path(__file__).resolve()

    # What the supervisor settled for this run, also written into the task manifest.
    _run: dict | None = None

    # ---------------- supervisor ----------------

    def enumerate_items(self, ctx: DriverContext[SegmentParams]) -> list[str]:
        star = ctx.paths["input_tomograms"]
        require_producer_input(star, "Input tomograms STAR")
        df = read_global_block(star)
        missing = {"rlnTomoName", "rlnTomoReconstructedTomogram"} - set(df.columns)
        if missing:
            raise KeyError(f"{star} lacks the columns {sorted(missing)}")
        self._tomo_paths = {
            str(row["rlnTomoName"]): str(
                resolve_tomogram_path(
                    str(row["rlnTomoReconstructedTomogram"]), tomograms_star=star, project_root=ctx.project_path
                )
            )
            for _, row in df.iterrows()
        }
        self._targets = resolve_targets(ctx.params)
        self.log(f"{len(self._tomo_paths)} tomogram(s); features: {', '.join(t['feature'] for t in self._targets)}")
        return sorted(self._tomo_paths)

    def pre_dispatch(self, ctx: DriverContext[SegmentParams], items: list[str]) -> None:
        (ctx.job_dir / OUTPUT_DIR).mkdir(exist_ok=True)
        recon = recon_of(ctx.state, ctx.instance_id)
        self.log(f"The input traces to reconstruct job: {recon or 'none of this project'}")
        versions = {} if print_cmd_only() else self._tool_versions(ctx)
        for t in self._targets:
            t["model"]["tool_version"] = versions.get(_DISTRIBUTIONS[t["model"]["tool"]])
        easymode = ctx.params.method == SegmentMethod.EASYMODE
        self._run = {
            "tomo_paths": self._tomo_paths,
            "targets": self._targets,
            "settings": _settings(ctx.params),
            "recon_instance": recon,
            "cache_dir": str(easymode_cache_dir()) if easymode else None,
        }
        if not print_cmd_only():
            self._settle_previous_run(ctx, items)

    def _tool_versions(self, ctx: DriverContext[SegmentParams]) -> dict[str, str]:
        """easymode's and Ais's versions in the image the tasks run, for every volume's provenance.
        One container start: an image that does not start fails the job before a GPU is asked for."""
        code = (
            "import importlib.metadata as m, json; "
            f"print(json.dumps({{p: m.version(p) for p in {tuple(_DISTRIBUTIONS.values())!r}}}))"
        )
        wrapped = get_container_service().wrap_command_for_tool(
            command=f"python -c {shlex.quote(code)}", cwd=ctx.job_dir, tool_name=TOOL_NAME
        )
        proc = subprocess.run(wrapped, shell=True, cwd=ctx.job_dir, capture_output=True, text=True, timeout=900)
        lines = proc.stdout.strip().splitlines()
        if proc.returncode != 0 or not lines:
            raise RuntimeError(f"The easymode image did not start (exit {proc.returncode}): {proc.stderr[-800:]}")
        versions = json.loads(lines[-1])
        self.log(f"Versions in the image: {versions}")
        return versions

    def _settle_previous_run(self, ctx: DriverContext[SegmentParams], items: list[str]) -> None:
        """A failed job can be changed and run again in its directory, where the tools skip the
        volumes that exist. Volumes made otherwise than this run would make them (another model under
        the same feature name, another TTA or format) are removed with their overlays, and when
        anything changed no tomogram counts as done, so each is segmented for what it now lacks;
        volumes of unchanged features are kept."""
        try:
            before = read_manifest(ctx.job_dir)
        except FileNotFoundError:
            return
        old_settings = before.get("settings") or {}
        old = {t["feature"]: t for t in before.get("targets") or []}
        changed = [
            (old[t["feature"]], t)
            for t in self._targets
            if t["feature"] in old and _identity(old[t["feature"]], old_settings) != _identity(t, self._run["settings"])
        ]
        added = [t["feature"] for t in self._targets if t["feature"] not in old]
        if not changed and not added:
            return
        for previous, current in changed:
            for item in items:
                for path in {_volume_path(ctx.job_dir, item, previous), _volume_path(ctx.job_dir, item, current)}:
                    path.unlink(missing_ok=True)
                for path in _overlay_paths(ctx.job_dir, item, current):
                    path.unlink(missing_ok=True)
        if changed:
            self.log(f"Made with other settings, removed: {', '.join(c['feature'] for _p, c in changed)}")
        if added:
            self.log(f"New since the last run: {', '.join(added)}")
        self.log("Every tomogram runs again for what it lacks")
        for marker in (ctx.job_dir / STATUS_DIR_NAME).glob("*.ok"):
            marker.unlink()

    def manifest_extras(self, ctx: DriverContext[SegmentParams], items: list[str]) -> dict:
        return self._run

    def aggregate(self, ctx: DriverContext[SegmentParams], results: ArrayResults) -> None:
        targets = {t["feature"]: t for t in self._targets}
        entries: list[SegmentationEntry] = []
        incomplete: list[str] = []
        for item in results.ok:
            try:
                record = json.loads(_sidecar_path(ctx.job_dir, item).read_text())
            except FileNotFoundError:
                incomplete.append(item)
                continue
            volumes = record["volumes"]
            if set(targets) - set(volumes):
                incomplete.append(item)
                continue
            for feature, t in targets.items():
                v = volumes[feature]
                entries.append(
                    SegmentationEntry(
                        tomogram=item,
                        feature=feature,
                        path=v["path"],
                        shape_zyx=record["shape_zyx"],
                        voxel_angstrom=record["voxel_angstrom"],
                        full_scale=v["full_scale"],
                        engine=t["engine"],
                        model=t["model"],
                        source_mrc=record["source_mrc"],
                        recon_instance=self._run["recon_instance"],
                        fraction_pct=v["fraction_pct"],
                        overlay_xy=v["overlay_xy"],
                        overlay_xz=v["overlay_xz"],
                    )
                )
        if incomplete:
            self.log(f"WARN: left out {len(incomplete)} tomogram(s) marked done without every feature: {incomplete}")

        p = ctx.params
        manifest = SegmentationManifest(
            producer=ctx.instance_id,
            method="ais" if p.method == SegmentMethod.AIS_MODEL else "easymode",
            parameters={
                "input_tomograms": str(ctx.paths["input_tomograms"]),
                **self._run["settings"],
                "ais_model_path": str(_ais_model(p)) if p.method == SegmentMethod.AIS_MODEL else None,
            },
            entries=entries,
        )
        write_manifest(ctx.job_dir, manifest)
        self.log(f"segmentations.json: {len(entries)} volume(s) of {len(results.ok) - len(incomplete)} tomogram(s)")

        # The registry says what the manifest says: a tomogram this run did not segment loses what an
        # earlier run of the job recorded for it.
        registry = get_registry_for(ctx.project_path)
        outputs = tomogram_outputs(manifest, ctx.job_dir)
        for ts, output in outputs.items():
            registry.attach_tomogram_output(ts, output)
        for ts in sorted(set(self._tomo_paths) - set(outputs)):
            if registry.has_tilt_series(ts):
                tomogram = registry.get_tilt_series(ts).tomogram
                if tomogram is not None and ctx.instance_id in tomogram.outputs:
                    registry.detach_tomogram_output(ts, ctx.instance_id)
        registry.save()

    # ---------------- task ----------------

    def _inputs(self, ctx: DriverContext[SegmentParams]) -> dict:
        """What the supervisor settled: the task reads it from the manifest; in print-command mode the
        supervisor stages the tasks itself, before any manifest is written."""
        return self._run if self._run is not None else read_manifest(ctx.job_dir)

    def task_already_done(self, ctx: DriverContext[SegmentParams], item: str) -> bool:
        run = self._inputs(ctx)
        tomo = Path(run["tomo_paths"][item])
        grid = _grid(tomo)
        if any(volume_problem(_volume_path(ctx.job_dir, item, t), grid[0]) for t in run["targets"]):
            return False
        self.log("Every volume is there from an earlier attempt")
        if not _sidecar_path(ctx.job_dir, item).exists():
            self._summarize(ctx, item, tomo, grid, run["targets"])
        return True

    def stage(self, ctx: DriverContext[SegmentParams], item: str) -> dict:
        run = self._inputs(ctx)
        tomo = Path(run["tomo_paths"][item])
        if not tomo.is_file():
            raise FileNotFoundError(f"Tomogram not found: {tomo}")
        shape, itemsize, apix = _grid(tomo)
        if apix <= 1.0:
            raise ValueError(
                f"{tomo.name} has a voxel size of {apix:g} Å in its header; easymode reads 1 Å or less as unset "
                "and would segment it as if it were 10 Å"
            )
        self.log(f"{tomo} · {shape[2]} × {shape[1]} × {shape[0]} at {apix:g} Å")

        task_dir = ctx.job_dir / ".staging" / f"task_{item}"
        in_dir, easymode_dir, ais_dir = task_dir / "in", task_dir / "easymode", task_dir / "ais"
        for d in (in_dir, easymode_dir, ais_dir):
            d.mkdir(parents=True, exist_ok=True)
        _relink(in_dir / f"{item}.mrc", tomo.resolve())
        cache_dir = Path(run["cache_dir"]) if run.get("cache_dir") else None
        if cache_dir is not None:
            _relink(easymode_dir / "models", cache_dir / "models")
        out_dir = ctx.job_dir / OUTPUT_DIR
        out_dir.mkdir(exist_ok=True)

        for t in run["targets"]:
            path = _volume_path(ctx.job_dir, item, t)
            problem = volume_problem(path, shape)
            if problem not in (None, "missing"):
                self.log(f"Removing {path.name}: {problem}")
                path.unlink()

        return {
            "tomo": tomo,
            "grid": (shape, itemsize, apix),
            "targets": run["targets"],
            "in_dir": in_dir,
            "easymode_dir": easymode_dir,
            "ais_dir": ais_dir,
            "out_dir": out_dir,
            "cache_dir": cache_dir,
        }

    def build_command(self, ctx: DriverContext[SegmentParams], item: str, staged: dict) -> ToolCommand:
        env = " ".join(
            f"{name}={shlex.quote(str(value))}"
            for name, value in (
                ("EASYMODE_CACHE_DIR", staged["easymode_dir"]),
                ("HF_HOME", staged["easymode_dir"] / "hf"),
                ("AIS_CONFIG_DIR", staged["ais_dir"]),
                ("https_proxy", OFFLINE_PROXY),
                ("HTTPS_PROXY", OFFLINE_PROXY),
            )
        )
        p = ctx.params
        if p.method == SegmentMethod.AIS_MODEL:
            return (
                ToolCommand(f"{env} ais segment")
                .opt_path("-m", _ais_model(p), quote=True)
                .opt_path("-d", staged["in_dir"], quote=True)
                .opt_path("-ou", staged["out_dir"], quote=True)
                .opt("-tta", p.tta)
                .opt("-p", 1)
                .opt("-gpu", 0)
            )
        return (
            ToolCommand(f"{env} easymode segment")
            .raw(" ".join(shlex.quote(t["feature"]) for t in staged["targets"]))
            .opt_path("--data", staged["in_dir"], quote=True)
            .opt_path("--output", staged["out_dir"], quote=True)
            .opt("--format", SegmentFormat(p.output_format).value)
            .opt("--tta", p.tta)
            .opt("--gpu", 0)
        )

    def task_binds(self, ctx: DriverContext[SegmentParams], item: str, staged: dict) -> list:
        binds = [staged["tomo"].resolve().parent]
        if staged["cache_dir"] is not None:
            binds.append(staged["cache_dir"])
        if ctx.params.method == SegmentMethod.AIS_MODEL:
            binds.append(_ais_model(ctx.params).resolve().parent)
        return binds

    def verify_outputs(self, ctx: DriverContext[SegmentParams], item: str, staged: dict) -> None:
        for t in staged["targets"]:
            path = _volume_path(ctx.job_dir, item, t)
            problem = volume_problem(path, staged["grid"][0])
            if problem:
                raise RuntimeError(f"{path.name}: {problem}")

    def collect(self, ctx: DriverContext[SegmentParams], item: str, staged: dict) -> None:
        self._summarize(ctx, item, staged["tomo"], staged["grid"], staged["targets"])

    def _summarize(self, ctx: DriverContext[SegmentParams], item: str, tomo: Path, grid, targets: list[dict]) -> None:
        """Each volume's overlay masks and fraction, into the tomogram's sidecar for the supervisor."""
        shape, itemsize, apix = grid
        volumes = {}
        for t in targets:
            path = _volume_path(ctx.job_dir, item, t)
            xy_png, xz_png = _overlay_paths(ctx.job_dir, item, t)
            summary = summarize_volume(path, slab_shape_zyx=shape, slab_itemsize=itemsize, xy_png=xy_png, xz_png=xz_png)
            volumes[t["feature"]] = {
                "path": str(path),
                "full_scale": summary["full_scale"],
                "fraction_pct": summary["fraction_pct"],
                "overlay_xy": str(xy_png),
                "overlay_xz": str(xz_png),
            }
            self.log(f"{t['feature']}: {summary['fraction_pct']:.2f} % of the volume")
        record = {
            "tomogram": item,
            "source_mrc": str(tomo),
            "shape_zyx": list(shape),
            "voxel_angstrom": apix,
            "volumes": volumes,
        }
        _write_json(_sidecar_path(ctx.job_dir, item), record)


if __name__ == "__main__":
    SegmentDriver().main()

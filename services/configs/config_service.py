# services/configs/config_service.py
"""
Pure configuration loader - reads conf.yaml and provides typed access.

Two layers are merged at load time:
  1. the shared server default  <repo>/config/conf.yaml  (read-only for users)
  2. a per-user override        ~/.crboost/conf.yaml     (this user only)
The override is a *partial* diff — only the keys a user changed via the
settings UI — deep-merged on top of the server default. This lets one user
retarget a container or a SLURM default for their own instance without ever
touching the shared file that everyone else's server reads.
"""

import copy
import logging
import shutil
import yaml
from pathlib import Path
from pydantic import BaseModel, Field
from typing import Any, Literal

logger = logging.getLogger(__name__)


def find_repo_root() -> Path:
    """
    Robustly find the repository root by looking for 'config/conf.yaml'
    starting from the current directory and moving up.
    """
    current = Path.cwd()
    # Check current directory first (most likely when running main.py)
    if (current / "config" / "conf.yaml").exists():
        return current

    # Fallback: check parents (in case we are running a script from a subfolder)
    for parent in current.parents:
        if (parent / "config" / "conf.yaml").exists():
            return parent

    # Last resort fallback to the old logic but corrected for the new depth
    # config_service.py is now in services/configs/, so we need .parent.parent
    return Path(__file__).resolve().parent.parent.parent


_REPO_ROOT = find_repo_root()
DEFAULT_CONFIG_PATH = _REPO_ROOT / "config" / "conf.yaml"

# Per-user override lives alongside the existing ~/.crboost/prefs.json
# (see services/configs/user_prefs_service.py). Home-scoped, so each user's
# server picks up only their own edits.
USER_OVERRIDE_PATH = Path.home() / ".crboost" / "conf.yaml"


def _deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively overlay `overlay` onto `base` (overlay wins on leaves).
    Nested dicts are merged, not replaced, so a partial override keeps the
    base's untouched siblings."""
    out = dict(base)
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _deep_diff(base: dict, new: dict) -> dict:
    """Return the subset of `new` whose leaves differ from `base`.

    Used to reduce a user's full edited value-set down to just the deltas
    from the server default, so the override file stays minimal and new
    server-default keys keep flowing through on untouched fields.
    """
    diff: dict = {}
    for k, v in new.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            sub = _deep_diff(base[k], v)
            if sub:
                diff[k] = sub
        elif base.get(k) != v:
            diff[k] = v
    return diff


def check_path_exists(path: str | None) -> bool:
    """True if a tool path resolves. Absolute paths are stat'd; a bare command
    name (e.g. ``pymol``) is looked up on PATH."""
    if not path or not str(path).strip():
        return False
    p = Path(path)
    if p.is_absolute():
        return p.exists()
    return shutil.which(path) is not None


class SlurmDefaultsConfig(BaseModel):
    """SLURM submission defaults from conf.yaml"""

    partition: str = "g"
    constraint: str = ""
    nodes: int = 1
    ntasks_per_node: int = 1
    cpus_per_task: int = 4
    gres: str = "gpu:1"
    mem: str = "32G"
    time: str = "2:00:00"


class SupervisorSlurmConfig(BaseModel):
    """
    Resources for the lightweight supervisor sbatch used by array-dispatching jobs.
    The supervisor counts tilt-series, submits a child SLURM array, polls squeue,
    and runs pure-Python metadata aggregation — no GPU work.
    The user-facing slurm config (slurm_defaults + per-job slurm_overrides) is consumed
    by the supervisor when it submits the child array job, NOT by the supervisor's own sbatch.
    """

    partition: str = "g"
    constraint: str = "g2|g3|g4"
    nodes: int = 1
    ntasks_per_node: int = 1
    cpus_per_task: int = 1
    gres: str = ""
    mem: str = "4G"
    time: str = "4:00:00"


class JobResourceProfile(BaseModel):
    """
    Per-job-type SLURM resource defaults.  All fields are optional — only the
    fields present in conf.yaml override the global slurm_defaults.
    Keys in conf.yaml must match JobType.value strings.
    """

    partition: str | None = None
    constraint: str | None = None
    nodes: int | None = None
    ntasks_per_node: int | None = None
    cpus_per_task: int | None = None
    gres: str | None = None
    mem: str | None = None
    time: str | None = None


# Backward compat alias
TsReconstructSupervisorSlurmConfig = SupervisorSlurmConfig


class LocalConfig(BaseModel):
    DefaultProjectBase: str | None = None
    DefaultMoviesGlob: str | None = None
    DefaultMdocsGlob: str | None = None


class ToolConfig(BaseModel):
    """Configuration for a specific external tool"""

    exec_mode: Literal["container", "binary"] = "container"
    container_path: str | None = None
    bin_path: str | None = None


class ProcessingDefaultsConfig(BaseModel):
    reconstruction_binning: int = 4


class CurationConfig(BaseModel):
    """ChimeraX+ArtiaX remote manual-picking session (VNC over a SLURM job).

    The SIF location is intentionally NOT hardcoded — every cluster keeps its
    containers somewhere different, so it is set here (or via the CX_SIF env var,
    which takes precedence). login_host defaults to the server's own FQDN, i.e.
    the headnode the user already SSHes into to reach the crboost UI.
    """

    sif_path: str | None = None
    partition: str = "c"  # CPU partition; software GL is enough for slice-based picking
    gres: str | None = None  # SLURM --gres, e.g. "gpu:1" on partition 'g'; None on CPU partitions
    vgl: bool = False  # render ChimeraX via VirtualGL (vglrun -d egl) on the GPU — needs the _GL.sif + gres
    cpus: int = 4
    mem: str = "16G"
    time: str = "08:00:00"  # must be <= the partition QOS MaxWall (0/unlimited is rejected by QOS)
    geometry: str = "1920x1080"
    chimerax_bin: str = "chimerax"
    login_host: str | None = None
    # VNC auth. Default ON: the worker mints a one-time random password (VncAuth).
    # True switches the desktop to `-SecurityTypes None` — no password at all, so ANYONE
    # who can reach that node's rfb port drives the session. Opt-in per site; the control
    # center states the risk beside the (empty) password field. Roadmap 10-S1.
    passwordless_vnc: bool = False
    # Open a block-binned display copy of the reconstruction instead of the full-res
    # volume (roadmap 10-S3). ArtiaX spends ~20 s COMPUTING on a 1 GB / 268 M-voxel recon;
    # 2 is ~8x fewer voxels, 4 is ~64x. Generated once, cached beside the recon. The
    # (N-1)/2·px corner shift this introduces is recorded in the dir's manifest and undone
    # exactly on ingest — see services/visualization/artiax_bridge.display_corner_offset.
    # 1 = off (open the full-res volume, as before 10-S3).
    display_bin: int = 2
    # The REST command channel (the worker starts `remotecontrol rest` on the node's
    # loopback; crboost reaches it via `ssh <node> curl`). QUARANTINED since roadmap 10-S1:
    # nothing may drive a session after launch (Model B), and this now gates only a
    # launch-time health check. It is NOT a switch for loading/saving from crboost — that
    # path is deleted, deliberately.
    rest_enabled: bool = True


class LocalDataConfig(BaseModel):
    """Local data of the protocol regression harness (roadmap 14). `root` holds the frozen dataset
    inputs (`input/<protocol>/`), the per-run throwaway projects (`runs/`) and the recorded
    baselines (`baseline/`). Empty = `<DefaultProjectBase>/local_data` — deliberately OUTSIDE
    the project base itself so the server's PipelineMonitor never adopts a CLI-driven run."""

    root: str = ""


class Config(BaseModel):
    """Root configuration model"""

    crboost_root: str = Field(default_factory=lambda: str(_REPO_ROOT))
    venv_path: str | None = None
    local: LocalConfig = Field(default_factory=LocalConfig)
    slurm_defaults: SlurmDefaultsConfig = Field(default_factory=SlurmDefaultsConfig)
    # Accepts both new key "supervisor_slurm" and legacy "tsreconstruct_supervisor_slurm"
    supervisor_slurm: SupervisorSlurmConfig = Field(default_factory=SupervisorSlurmConfig)
    tsreconstruct_supervisor_slurm: SupervisorSlurmConfig | None = None
    job_resource_profiles: dict[str, JobResourceProfile] = Field(default_factory=dict)
    processing_defaults: ProcessingDefaultsConfig = Field(default_factory=ProcessingDefaultsConfig)
    curation: CurationConfig = Field(default_factory=CurationConfig)
    tools: dict[str, ToolConfig] = Field(default_factory=dict)
    containers: dict[str, str] | None = None
    # Lab-level species catalog root (roadmap 12). Cross-project species DEFINITIONS live
    # here — name, diameter, symmetry, notes, templates + masks with their provenance;
    # picks, filters, merges and extractions stay project-bound. Empty or absent = the
    # feature is OFF: no catalog affordance is rendered anywhere, which is the state every
    # existing install is in until someone points this at a shared directory.
    species_catalog_root: str = ""
    local_data: LocalDataConfig = Field(default_factory=LocalDataConfig)

    # DEV TOGGLE (temporary): global override so every project uses the afterok orchestrator
    # (schemer-free submit + inline import) without per-project project_params.json edits. A
    # per-project `use_afterok_orchestrator: true` still wins on its own. Remove once validated.
    use_afterok_orchestrator: bool = False

    class Config:
        extra = "ignore"


class ConfigService:
    """Loads and provides access to static configuration"""

    def __init__(self, config_path: Path | None = None):
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH
        self._default_path = config_path
        self._override_path = USER_OVERRIDE_PATH

        if not config_path.exists():
            # Diagnostic info to help debug future path shifts
            raise FileNotFoundError(
                f"Configuration file not found at: {config_path}\n"
                f"Repo Root identified as: {_REPO_ROOT}\n"
                f"Run 'python preflight.py' to create one from the template."
            )

        with open(config_path) as f:
            base_data = yaml.safe_load(f) or {}
        self._base_data: dict = base_data

        # Layer 2: this user's personal override (may be absent).
        override: dict = {}
        if self._override_path.exists():
            try:
                with open(self._override_path) as f:
                    override = yaml.safe_load(f) or {}
                if not isinstance(override, dict):
                    logger.warning("User config override at %s is not a mapping — ignoring", self._override_path)
                    override = {}
            except Exception as e:
                logger.warning("Failed to read user config override %s: %s — using default", self._override_path, e)
                override = {}
        self._override_data: dict = override

        data = _deep_merge(base_data, override) if override else dict(base_data)

        # Migrate legacy key: tsreconstruct_supervisor_slurm → supervisor_slurm
        if "tsreconstruct_supervisor_slurm" in data and "supervisor_slurm" not in data:
            data["supervisor_slurm"] = data.pop("tsreconstruct_supervisor_slurm")

        self._effective_data: dict = data
        self._config = Config(**data)

    @property
    def config(self) -> Config:
        return self._config

    @property
    def crboost_root(self) -> Path:
        return Path(self._config.crboost_root)

    @property
    def processing_defaults(self) -> ProcessingDefaultsConfig:
        return self._config.processing_defaults

    @property
    def curation(self) -> CurationConfig:
        return self._config.curation

    @property
    def species_catalog_root(self) -> Path | None:
        """The lab species catalog directory, or None when the feature is off.

        None is the normal state, not an error: an install that never set
        `species_catalog_root` has no shared directory to publish to, and every catalog
        affordance checks this before rendering. A configured-but-missing path still
        returns the Path — the catalog service creates it on first publish, and a typo
        should surface there (as a real OSError naming the path) rather than here as a
        silent "feature off"."""
        raw = (self._config.species_catalog_root or "").strip()
        return Path(raw).expanduser() if raw else None

    @property
    def local_data_root(self) -> Path | None:
        """Root of the regression harness's on-disk area (roadmap 14): the configured
        `local_data.root`, else `<DefaultProjectBase>/local_data`, else None when neither
        is set (the harness then refuses to run and names the missing key)."""
        raw = (self._config.local_data.root or "").strip()
        if raw:
            return Path(raw).expanduser()
        base = (self._config.local.DefaultProjectBase or "").strip()
        return Path(base).expanduser() / "local_data" if base else None

    @property
    def venv_path(self) -> Path | None:
        if self._config.venv_path:
            return Path(self._config.venv_path)
        return None

    @property
    def venv_python(self) -> Path | None:
        if self.venv_path:
            return self.venv_path / "bin" / "python3"
        return None

    @property
    def slurm_defaults(self) -> SlurmDefaultsConfig:
        return self._config.slurm_defaults

    @property
    def supervisor_slurm_defaults(self) -> SupervisorSlurmConfig:
        return self._config.supervisor_slurm

    @property
    def tsreconstruct_supervisor_slurm_defaults(self) -> SupervisorSlurmConfig:
        """Backward compat alias."""
        return self.supervisor_slurm_defaults

    @property
    def default_project_base(self) -> str | None:
        return self._config.local.DefaultProjectBase

    @property
    def default_data_globs(self) -> tuple[str | None, str | None]:
        return (self._config.local.DefaultMoviesGlob, self._config.local.DefaultMdocsGlob)

    def get_job_resource_profile(self, job_type_value: str) -> JobResourceProfile | None:
        """Return the resource profile for a job type, or None if not configured."""
        return self._config.job_resource_profiles.get(job_type_value)

    def get_tool_config(self, tool_name: str) -> ToolConfig:
        if tool_name in self._config.tools:
            return self._config.tools[tool_name]

        legacy_mapping = {
            "warptools": "warp_aretomo",
            "aretomo": "warp_aretomo",
            "relion_import": "relion",
            "relion_schemer": "relion",
        }
        lookup_name = legacy_mapping.get(tool_name, tool_name)

        if lookup_name in self._config.tools:
            return self._config.tools[lookup_name]

        if self._config.containers and lookup_name in self._config.containers:
            return ToolConfig(exec_mode="container", container_path=self._config.containers[lookup_name])

        return ToolConfig(exec_mode="binary", bin_path=tool_name)

    def is_tool_configured(self, tool_name: str) -> bool:
        """True when `tool_name` (or its legacy alias) has an entry under `tools:` /
        `containers:`. `get_tool_config`'s last fallback — a bare-binary guess named after
        the tool — is deliberately NOT counted: a protocol pinning a tool must find it
        configured, not assumed (roadmap 14)."""
        legacy_mapping = {
            "warptools": "warp_aretomo",
            "aretomo": "warp_aretomo",
            "relion_import": "relion",
            "relion_schemer": "relion",
        }
        name = legacy_mapping.get(tool_name, tool_name)
        if tool_name in self._config.tools or name in self._config.tools:
            return True
        return bool(self._config.containers and name in self._config.containers)

    def get_tool_path(self, tool_name: str) -> str | None:
        config = self.get_tool_config(tool_name)
        if config.exec_mode == "container":
            return config.container_path
        return config.bin_path

    # ── Per-user override management (settings UI) ────────────────────────

    @property
    def has_user_override(self) -> bool:
        """True when a non-empty ~/.crboost/conf.yaml is currently layered on."""
        return bool(self._override_data)

    @property
    def user_override_path(self) -> Path:
        return self._override_path

    @property
    def default_config_path(self) -> Path:
        return self._default_path

    def base_dict(self) -> dict:
        """The shared server default, as a plain dict (a deep copy)."""
        return copy.deepcopy(self._base_data)

    def effective_dict(self) -> dict:
        """The merged (default + user override) config, as a plain dict."""
        return copy.deepcopy(self._effective_data)

    def save_user_override(self, new_values: dict) -> None:
        """Persist the user's edited values to ~/.crboost/conf.yaml.

        `new_values` is a nested dict shaped like conf.yaml holding the
        editable subset. Only leaves that differ from the SERVER DEFAULT are
        written, so the override stays a minimal diff and the shared file is
        never touched. Resets the singleton so the next get_config_service()
        reflects the change.
        """
        delta = _deep_diff(self._base_data, new_values)
        self._override_path.parent.mkdir(parents=True, exist_ok=True)
        if delta:
            with open(self._override_path, "w") as f:
                yaml.safe_dump(delta, f, default_flow_style=False, sort_keys=False)
            logger.info("Wrote user config override (%d top-level keys) to %s", len(delta), self._override_path)
        elif self._override_path.exists():
            # Edited values collapsed back to the defaults → drop the override.
            self._override_path.unlink()
            logger.info("User config override matched defaults — removed %s", self._override_path)
        reset_config_service()

    def revert_to_defaults(self) -> None:
        """Delete this user's override entirely, falling back to the shared
        server conf.yaml. Resets the singleton."""
        if self._override_path.exists():
            self._override_path.unlink()
            logger.info("Reverted to server defaults — removed %s", self._override_path)
        reset_config_service()

    def container_status(self) -> dict[str, Any]:
        """Existence check for every configured tool path. Powers the landing
        status dot (green when all resolve, amber otherwise) and the per-row
        markers in the settings panel."""
        tools: list[dict[str, Any]] = []
        for name, tc in self._config.tools.items():
            path = tc.container_path if tc.exec_mode == "container" else tc.bin_path
            tools.append(
                {"name": name, "exec_mode": tc.exec_mode, "path": path or "", "exists": check_path_exists(path)}
            )
        tools.sort(key=lambda t: t["name"])
        n_ok = sum(1 for t in tools if t["exists"])
        return {"all_ok": n_ok == len(tools) and len(tools) > 0, "n_ok": n_ok, "n_total": len(tools), "tools": tools}


_config_service_instance: ConfigService | None = None


def get_config_service() -> ConfigService:
    global _config_service_instance
    if _config_service_instance is None:
        _config_service_instance = ConfigService()
    return _config_service_instance


def reset_config_service():
    global _config_service_instance
    _config_service_instance = None

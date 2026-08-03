"""
CryoBoost landing-page status strip.

A thin band mounted above the project setup on the landing page. Left: the
CryoBoost wordmark. Middle: live status dots for the things that tell you at
a glance whether this server instance is healthy — SLURM reachability, the
user it runs as, host:port, and whether every configured container/tool
resolves on disk. Right: a gear that opens the config settings editor.

The live parts (SLURM probe, container existence) are refreshed on a 15 s
timer but only rebuild the DOM when a value actually changes (FingerprintedView),
per the project's UI-reactivity conventions.
"""

from __future__ import annotations

import asyncio
import getpass
import logging
import os
import re
import socket
from collections import defaultdict
from typing import Any, Optional

from nicegui import ui

from services.configs.config_service import get_config_service
from ui.components.copyable import copy_button
from ui.components.reactive import FingerprintedView
from ui.config_settings_dialog import open_config_settings
from ui.styles import MONO, SANS as FONT

logger = logging.getLogger(__name__)

CURRENT_USER = getpass.getuser()
HOSTNAME = socket.gethostname()

CLR_HEADING = "#0f172a"
CLR_LABEL = "#475569"
CLR_SUBLABEL = "#94a3b8"
CLR_BORDER = "#e2e8f0"
CLR_ACCENT = "#2563eb"
CLR_OK = "#10b981"
CLR_WARN = "#f59e0b"
CLR_BAD = "#ef4444"
CLR_NEUTRAL = "#94a3b8"

_REFRESH_SEC = 15.0

# Light popover (opens on hover, closes on leave) — same gray/blue palette as the rest of the UI.
_POPOVER_STYLE = (
    "background: #ffffff; color: #334155; border: 1px solid #e2e8f0; border-radius: 10px; "
    "padding: 12px 14px; box-shadow: 0 10px 30px rgba(15,23,42,0.12);"
)
_POP_TITLE = f"{FONT} font-size: 12px; font-weight: 700; color: {CLR_HEADING};"
_POP_LABEL = (
    f"{FONT} font-size: 9px; font-weight: 700; color: {CLR_SUBLABEL}; "
    "letter-spacing: 0.06em; text-transform: uppercase;"
)
_POP_DESC = f"{FONT} font-size: 10px; color: {CLR_LABEL}; line-height: 1.4;"
_POP_VAL = f"{MONO} font-size: 10.5px; color: {CLR_HEADING};"
_POP_CHIP = (
    f"{MONO} font-size: 9.5px; color: {CLR_LABEL}; background: #f1f5f9; "
    "border: 1px solid #e2e8f0; border-radius: 4px; padding: 1px 6px;"
)
_POP_PATH = f"{MONO} font-size: 10px; color: {CLR_HEADING}; word-break: break-all;"

# Exact commands behind each section, surfaced as an info tooltip so the data is auditable.
_CMD_PARTITIONS = 'sinfo -o "%P|%a|%D|%l|%m|%c|%G" --noheader'
_CMD_NODES = 'sinfo -N -o "%N|%P|%T|%c|%m|%G|%f" --noheader'
_CMD_QOS = (
    "sacctmgr -nP show assoc user=$USER format=QOS,DefaultQOS  |  "
    "sacctmgr -nP show qos format=Name,MaxWall,MaxTRESPerJob,MaxTRESPerUser"
)


def _pop_copy_row(text: str) -> None:
    """A mono command/path line with a copy icon, on the light popover."""
    with ui.row().classes("items-center").style(
        "gap: 6px; background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 5px; "
        "padding: 4px 6px; margin-top: 3px; width: 100%;"
    ):
        ui.label(text).style(_POP_PATH + " flex: 1 1 0; min-width: 0;")
        copy_button(text, tooltip="Copy", color=CLR_ACCENT)


def _section_header(title: str, command: str) -> None:
    """A section label with a right-aligned terminal icon whose tooltip is the exact command."""
    with ui.row().classes("items-center").style("gap: 6px; width: 100%; margin-top: 9px; margin-bottom: 2px;"):
        ui.label(title).style(_POP_LABEL)
        ui.element("div").style("flex: 1;")
        ui.icon("terminal", size="13px").style(f"color: {CLR_SUBLABEL}; cursor: help;").tooltip(command)


def _server_port() -> str:
    return os.environ.get("CRBOOST_PORT", "") or "?"


class _StatusState:
    """Plain holder mutated by the async probe; read by signature()/render()."""

    def __init__(self) -> None:
        self.checked = False
        self.slurm_ok = False
        self.slurm_partitions = 0
        self.cont_all_ok = False
        self.cont_n_ok = 0
        self.cont_n_total = 0
        self.cont_missing: list[str] = []
        self.has_override = False
        # Cluster layout (sinfo): per partition -> (name, max_time, nodes, cpu_only, gpu_groups)
        # where gpu_groups = ((gpus_per_node, gpu_type, feature, node_count), ...). Hashable for signature().
        self.cluster: tuple = ()
        # QOS (sacctmgr): per-QOS table rows (name, is_default, wall_human, gpus_job_human).
        self.qos_probed = False
        self.qos_rows: tuple = ()
        self.qos_default_wall = ""  # human wall for the default QOS, e.g. "8:00:00" / "no limit"


class LandingStatusStrip(FingerprintedView):
    def __init__(self, backend, container: Any) -> None:
        super().__init__(container)
        self.backend = backend
        self.state = _StatusState()

    def signature(self):
        s = self.state
        return (
            s.checked,
            s.slurm_ok,
            s.slurm_partitions,
            s.cluster,
            s.qos_probed,
            s.qos_rows,
            s.qos_default_wall,
            s.cont_all_ok,
            s.cont_n_ok,
            s.cont_n_total,
            s.has_override,
        )

    async def refresh_status(self) -> None:
        """Probe SLURM + container existence off the event loop, then re-render
        (signature-gated, so no DOM churn unless something moved)."""
        s = self.state
        # Partitions (cluster-wide) + per-node GPU inventory, aggregated into GPU-type groups so a
        # heterogeneous GPU partition (e.g. CBE's `g` = mixed V100/RTX/A100 nodes) reads clearly.
        try:
            parts = await self.backend.slurm_service.get_partitions_info()
            s.slurm_ok = len(parts) > 0
            s.slurm_partitions = len(parts)
            nodes = await self.backend.slurm_service.get_nodes_info()
            # sinfo's PARTITION summary emits one line per node-state, so its %D is a per-state
            # subset (that's the "g · 1 node" bug). Derive per-partition totals from the NODE list
            # (one row per node), which is authoritative: node count, GPU-type groups, and the modal
            # CPU/mem spec (so cpu-only partitions get details too).
            part_names: dict = defaultdict(set)
            gpu_by_part: dict = defaultdict(lambda: defaultdict(int))
            spec_by_part: dict = defaultdict(lambda: defaultdict(int))  # (cpus, mem_gb) -> node count
            for n in nodes:
                part_names[n.partition].add(n.name)
                mem_gb = round(n.memory_mb / 1024) if n.memory_mb else 0
                spec_by_part[n.partition][(n.cpus or 0, mem_gb)] += 1
                if n.gpus:  # include GPU nodes even when gres is untyped (gpu:N) — feature is the hint
                    feat = next((f for f in (n.features or []) if re.fullmatch(r"g\d+", f)), "")
                    gpu_by_part[n.partition][(n.gpus, n.gpu_type or "GPU", feat)] += 1
            cluster = []
            for p in parts:
                grp = gpu_by_part.get(p.name, {})
                gpu_groups = tuple(
                    sorted(((g, t, f, c) for (g, t, f), c in grp.items()), key=lambda x: (-x[3], x[1]))
                )
                total_nodes = len(part_names.get(p.name, ())) or (getattr(p, "nodes", 0) or 0)
                specs = spec_by_part.get(p.name, {})
                spec = max(specs.items(), key=lambda kv: kv[1])[0] if specs else (0, 0)  # modal (cpus, mem_gb)
                # Partition TimeLimit is often "infinite" with the real per-job wall enforced by QOS
                # (see the QOS section) — don't surface a misleading "max infinite".
                mx = (getattr(p, "max_time", "") or "").strip()
                if mx.lower() in ("infinite", "unlimited", "n/a", ""):
                    mx = ""
                cluster.append((p.name, mx, total_nodes, not gpu_groups, gpu_groups, spec))
            # GPU partitions first, then by node count.
            cluster.sort(key=lambda c: (c[3], -c[2]))
            s.cluster = tuple(cluster)
        except Exception as e:
            logger.info("SLURM probe failed: %s", e)
            s.slurm_ok = False
            s.slurm_partitions = 0
            s.cluster = ()

        # QOS: what the *user* is allowed per job (partitions are cluster-wide; QOS is per-user).
        try:
            qos = await self.backend.slurm_service.get_user_qos_limits()
            rows = []
            for q in qos:
                raw = (q.max_wall or "").strip()
                wall = "no limit" if (raw == "" or raw.upper() in ("UNLIMITED", "NONE")) else raw
                # A per-job GPU cap only means something when it's a real ceiling; a node here has
                # <=8 GPUs, so anything above ~16 is effectively "no per-job cap" and just confuses.
                gpus_job = str(q.max_gpus_per_job) if 0 < q.max_gpus_per_job <= 16 else "—"
                rows.append((q.name, q.is_default, wall, gpus_job))
                if q.is_default:
                    s.qos_default_wall = wall
            if not s.qos_default_wall and rows:  # no explicit default flagged: show the first
                s.qos_default_wall = rows[0][2]
            s.qos_rows = tuple(rows)
            s.qos_probed = True
        except Exception as e:
            logger.info("QOS probe failed: %s", e)
            s.qos_probed = False
            s.qos_rows = ()

        try:
            cs = get_config_service()
            stat = await asyncio.to_thread(cs.container_status)
            s.cont_all_ok = stat["all_ok"]
            s.cont_n_ok = stat["n_ok"]
            s.cont_n_total = stat["n_total"]
            s.cont_missing = [t["name"] for t in stat["tools"] if not t["exists"]]
            s.has_override = cs.has_user_override
        except Exception as e:
            logger.info("Container status probe failed: %s", e)

        s.checked = True
        try:
            self.refresh()
        except Exception as e:
            logger.info("Status strip refresh skipped: %s", e)

    # ── render ────────────────────────────────────────────────────────────

    def render(self) -> None:
        s = self.state
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 14px; width: 100%; "
            "padding: 7px 14px; background: #ffffff; border: 1px solid "
            f"{CLR_BORDER}; border-radius: 8px; box-shadow: 0 1px 3px rgba(15,23,42,0.05);"
        ):
            self._wordmark()
            ui.element("div").style(f"width: 1px; height: 20px; background: {CLR_BORDER}; flex-shrink: 0;")

            # SLURM (click for partitions + submission-template path)
            if not s.checked:
                self._dot_item(CLR_NEUTRAL, "SLURM", "checking…", "Checking cluster reachability…", pulse=False)
            elif s.slurm_ok:
                self._dot_item(
                    CLR_OK, "SLURM", f"{s.slurm_partitions} part", "", popover=self._slurm_popover
                )
            else:
                self._dot_item(CLR_BAD, "SLURM", "unreachable", "", popover=self._slurm_popover)

            # User
            self._dot_item(CLR_NEUTRAL, "user", CURRENT_USER, f"This server runs as {CURRENT_USER}", pulse=False)

            # Host:port (click for the SSH port-forward command to reach it)
            self._dot_item(
                CLR_NEUTRAL, "host", f"{HOSTNAME}:{_server_port()}", "", pulse=False, popover=self._host_popover
            )

            # Containers
            if not s.checked:
                self._dot_item(CLR_NEUTRAL, "containers", "…", "Checking container paths…", pulse=False)
            elif s.cont_all_ok:
                self._dot_item(
                    CLR_OK, "containers", f"{s.cont_n_ok}/{s.cont_n_total}", "All configured tool paths resolve"
                )
            else:
                missing = ", ".join(s.cont_missing) if s.cont_missing else "some paths"
                self._dot_item(
                    CLR_WARN,
                    "containers",
                    f"{s.cont_n_ok}/{s.cont_n_total}",
                    f"Missing on disk: {missing}",
                )

            ui.element("div").style("flex: 1;")

            if s.has_override:
                with ui.element("div").style(
                    "display: flex; align-items: center; gap: 4px; padding: 2px 8px; border-radius: 20px; "
                    "background: #eff6ff; border: 1px solid #bfdbfe; flex-shrink: 0;"
                ).tooltip("Your personal config override is active (~/.crboost/conf.yaml)"):
                    ui.element("div").style(f"width: 5px; height: 5px; border-radius: 50%; background: {CLR_ACCENT};")
                    ui.label("custom").style(f"{FONT} font-size: 9px; font-weight: 600; color: #1e40af;")

            (
                ui.button(icon="settings", on_click=lambda: open_config_settings(on_saved=self._on_config_saved))
                .props("flat dense round size=sm")
                .classes("text-slate-400 hover:text-blue-600")
                .tooltip("Server configuration & container paths")
            )

    def _on_config_saved(self) -> None:
        # Config singleton was reset; re-probe so dots + override badge update.
        ui.timer(0.05, self.refresh_status, once=True)

    def _wordmark(self) -> None:
        with ui.element("div").style("display: flex; align-items: center; gap: 7px; flex-shrink: 0;"):
            with ui.element("div").style(
                f"width: 18px; height: 18px; border-radius: 5px; background: {CLR_ACCENT}1a; "
                f"border: 1px solid {CLR_ACCENT}55; display: flex; align-items: center; justify-content: center;"
            ):
                ui.icon("bolt", size="13px").style(f"color: {CLR_ACCENT};")
            ui.label("CryoBoost").style(
                f"{FONT} font-size: 13px; font-weight: 700; color: {CLR_HEADING}; letter-spacing: -0.02em;"
            )

    def _dot_item(self, color: str, label: str, value: str, tip: str, pulse: bool = True, popover=None) -> None:
        pulse_cls = "status-dot pulse-success" if (pulse and color == CLR_OK) else "status-dot"
        cursor = "cursor: pointer;" if popover else ""
        item = ui.element("div").style(f"display: flex; align-items: center; gap: 5px; flex-shrink: 0; {cursor}")
        with item:
            ui.element("div").classes(pulse_cls).style(
                f"width: 7px; height: 7px; border-radius: 50%; background: {color}; flex-shrink: 0;"
            )
            ui.label(label).style(
                f"{FONT} font-size: 9px; font-weight: 700; color: {CLR_SUBLABEL}; "
                "letter-spacing: 0.05em; text-transform: uppercase;"
            )
            # A dotted underline hints the value is clickable for details/copy.
            val_extra = "border-bottom: 1px dotted #94a3b8;" if popover else ""
            ui.label(value).style(f"{MONO} font-size: 10px; color: {CLR_LABEL}; {val_extra}")
            if popover is not None:
                # Keep the QMenu itself transparent/chromeless and render the light card as a child
                # div — styling the QMenu directly leaves Quasar's white wrapper + its own shadow
                # showing behind the rounded corners.
                menu = (
                    ui.menu()
                    .props('no-parent-event anchor="bottom left" self="top left"')
                    .style("background: transparent; box-shadow: none; overflow: visible;")
                )
                with menu:
                    with ui.element("div").style(_POPOVER_STYLE):
                        popover()
                # Hover-card behaviour: open on hover/click, and close when the pointer leaves BOTH
                # the trigger and the popover (so it doesn't linger). Re-opening on the menu's own
                # mouseenter keeps it alive while you move into it to select/copy. (Esc / click-away
                # also close it — Quasar default.)
                item.on("mouseenter", lambda m=menu: m.open())
                item.on("click", lambda m=menu: m.open())
                item.on("mouseleave", lambda m=menu: m.close())
                menu.on("mouseenter", lambda m=menu: m.open())
                menu.on("mouseleave", lambda m=menu: m.close())
        if popover is None and tip:
            item.tooltip(tip)

    def _host_popover(self) -> None:
        port = _server_port()
        cmd = f"ssh -f -N -L {port}:localhost:{port} {CURRENT_USER}@{HOSTNAME}"
        with ui.column().style("gap: 3px; min-width: 340px; max-width: 560px;"):
            ui.label("Reach this server from your machine").style(_POP_TITLE)
            ui.label(f"Running as {CURRENT_USER} on {HOSTNAME}, port {port}.").style(_POP_DESC)
            ui.label(f"Run this in a LOCAL terminal, then open http://localhost:{port}").style(_POP_DESC)
            _pop_copy_row(cmd)
            ui.label("Already on the cluster network? Browse to the machine's address directly.").style(
                _POP_DESC + " margin-top: 4px;"
            )

    def _slurm_popover(self) -> None:
        s = self.state
        try:
            qsub = str(get_config_service().crboost_root / "config" / "qsub.sh")
        except Exception:
            qsub = "config/qsub.sh"
        divider = "height: 1px; background: #e2e8f0; margin: 8px 0 2px; width: 100%;"
        # Hide the GPUS/JOB column when no QOS actually caps GPUs per job (all "—") — else it's noise.
        show_gpu_col = any(r[3] != "—" for r in s.qos_rows)
        qos_cols = "1fr 104px 66px" if show_gpu_col else "1fr 104px"
        qos_grid = f"display: grid; grid-template-columns: {qos_cols}; gap: 8px; width: 100%; align-items: baseline;"
        gpu_grid = (
            "display: grid; grid-template-columns: 82px 66px 1fr; gap: 8px; width: 100%; "
            "padding-left: 10px; align-items: baseline;"
        )

        with ui.column().style("gap: 2px; min-width: 400px; max-width: 600px;"):
            ui.label(f"Cluster · {HOSTNAME}").style(_POP_TITLE)

            # ── Partitions & GPUs (cluster-wide) ──────────────────────────────
            _section_header("Partitions & GPUs", f"{_CMD_PARTITIONS}   |   {_CMD_NODES}")
            if not s.slurm_ok:
                ui.label("sinfo returned nothing — SLURM may be unavailable on this host.").style(_POP_DESC)
            elif not s.cluster:
                ui.label("No partitions reported.").style(_POP_DESC)
            else:
                for name, max_time, nodes, cpu_only, gpu_groups, spec in s.cluster[:12]:
                    with ui.column().style("gap: 2px; width: 100%; margin-bottom: 3px;"):
                        with ui.row().classes("items-baseline").style("gap: 6px; width: 100%;"):
                            ui.label(name).style(_POP_VAL + " font-weight: 700;")
                            cpus, _mem_gb = spec
                            meta = f"{nodes} node{'s' if nodes != 1 else ''}"
                            if cpus:
                                meta += f"  ·  {cpus} cores"
                            if max_time:
                                meta += f"  ·  max {max_time}"
                            if cpu_only:
                                meta += "  ·  cpu-only"
                            ui.label(meta).style(_POP_DESC)
                        for gpn, gtype, feat, cnt in gpu_groups:
                            with ui.element("div").style(gpu_grid):
                                ui.label(f"{gpn}× {gtype.upper()}").style(_POP_VAL)
                                ui.label(f"{cnt} node{'s' if cnt != 1 else ''}").style(_POP_DESC)
                                if feat:
                                    ui.label(feat).style(_POP_CHIP).tooltip(f"request with --constraint={feat}")
                                else:
                                    ui.label("").style(_POP_DESC)

            # ── Your QOS (per-user limits) ────────────────────────────────────
            ui.element("div").style(divider)
            _section_header("Your QOS  ·  ★ = default", _CMD_QOS)
            if not s.qos_probed:
                ui.label("probing…").style(_POP_DESC)
            elif not s.qos_rows:
                ui.label("sacctmgr returned no QOS for your user.").style(_POP_DESC)
            else:
                with ui.element("div").style(qos_grid):
                    ui.label("QOS").style(_POP_LABEL)
                    ui.label("MAX WALL/JOB").style(_POP_LABEL)
                    if show_gpu_col:
                        ui.label("GPUS/JOB").style(_POP_LABEL)
                for qname, is_def, wall, gpus_job in s.qos_rows[:16]:
                    with ui.element("div").style(qos_grid):
                        ui.label(f"★ {qname}" if is_def else qname).style(
                            _POP_VAL + (" font-weight: 700;" if is_def else "")
                        )
                        ui.label(wall).style(_POP_VAL)
                        if show_gpu_col:
                            ui.label(gpus_job).style(_POP_VAL)
                if s.qos_default_wall:
                    ui.label(f"Your default QOS (★) caps a single job at {s.qos_default_wall}.").style(
                        _POP_DESC + " margin-top: 4px;"
                    )

            # ── Submission template ───────────────────────────────────────────
            ui.element("div").style(divider)
            _section_header("Submission template", f"cat {qsub}")
            _pop_copy_row(qsub)


def mount_landing_status_strip(backend) -> Optional[LandingStatusStrip]:
    """Mount the strip at the current NiceGUI position. Returns the view (also
    kicks off the first async probe + the refresh timer)."""
    container = ui.element("div").classes("w-full").style("margin-bottom: 8px;")
    strip = LandingStatusStrip(backend, container)
    strip.refresh()  # initial (skeleton) paint
    ui.timer(0.05, strip.refresh_status, once=True)
    ui.timer(_REFRESH_SEC, strip.refresh_status)
    return strip

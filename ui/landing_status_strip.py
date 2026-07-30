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
import socket
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

# Dark interactive popover (click a dot to open) — matches the IO-tab hover panel.
_POPOVER_STYLE = (
    "background: #0f172a; color: #e2e8f0; border-radius: 8px; padding: 10px 12px; "
    "box-shadow: 0 8px 24px rgba(0,0,0,0.30);"
)
_POP_TITLE = f"{FONT} font-size: 11px; font-weight: 700; color: #f8fafc;"
_POP_DESC = f"{FONT} font-size: 10px; color: #94a3b8; line-height: 1.35;"
_POP_CHIP = f"{MONO} font-size: 9px; color: #cbd5e1; background: #1e293b; border-radius: 4px; padding: 1px 6px;"
_POP_PATH = f"{MONO} font-size: 10px; color: #cbd5e1; word-break: break-all;"


def _pop_copy_row(text: str) -> None:
    """A mono command/path line on the dark popover with a copy icon."""
    with ui.row().classes("items-center").style(
        "gap: 6px; background: #1e293b; border-radius: 5px; padding: 4px 6px; margin-top: 3px; width: 100%;"
    ):
        ui.label(text).style(_POP_PATH + " flex: 1 1 0; min-width: 0;")
        copy_button(text, tooltip="Copy", color="#93c5fd")


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
        self.slurm_part_details: list[str] = []  # e.g. ["g ×16 A100", "c ×40"]
        self.has_override = False


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
            tuple(s.slurm_part_details),
            s.cont_all_ok,
            s.cont_n_ok,
            s.cont_n_total,
            s.has_override,
        )

    async def refresh_status(self) -> None:
        """Probe SLURM + container existence off the event loop, then re-render
        (signature-gated, so no DOM churn unless something moved)."""
        s = self.state
        try:
            parts = await self.backend.slurm_service.get_partitions_info()
            s.slurm_ok = len(parts) > 0
            s.slurm_partitions = len(parts)
            details = []
            for p in parts:
                nodes = getattr(p, "nodes", 0) or 0
                gpu_n = getattr(p, "available_gpus", 0) or 0
                gpu_t = getattr(p, "gpu_type", None)
                tag = f"{p.name} ×{nodes}"
                if gpu_n:
                    tag += f" {gpu_t or 'gpu'}"
                details.append(tag)
            s.slurm_part_details = details
        except Exception as e:
            logger.info("SLURM probe failed: %s", e)
            s.slurm_ok = False
            s.slurm_partitions = 0
            s.slurm_part_details = []

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
                menu = ui.menu().props('no-parent-event anchor="bottom left" self="top left"').style(_POPOVER_STYLE)
                with menu:
                    popover()
                # Hover OR click opens it; it stays open (interactive — you can
                # select/copy inside) and closes on click-away.
                item.on("mouseenter", lambda m=menu: m.open())
                item.on("click", lambda m=menu: m.open())
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
        with ui.column().style("gap: 3px; min-width: 340px; max-width: 620px;"):
            ui.label("SLURM").style(_POP_TITLE)
            if s.slurm_ok:
                ui.label(f"{s.slurm_partitions} partition(s) reachable via sinfo:").style(_POP_DESC)
                with ui.row().style("flex-wrap: wrap; gap: 4px; margin: 3px 0;"):
                    for tag in s.slurm_part_details[:32]:
                        ui.label(tag).style(_POP_CHIP)
            else:
                ui.label("sinfo returned nothing — SLURM may be unavailable on this host.").style(_POP_DESC)
            ui.label("Submission template used for every job:").style(_POP_DESC + " margin-top: 4px;")
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

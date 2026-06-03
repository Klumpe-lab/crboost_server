#!/usr/bin/env bash
# Node-side worker for a ChimeraX+ArtiaX curation session.
#
# Runs ON a compute node — either submitted by crboost via sbatch (the UI
# "Launch ChimeraX/ArtiaX" button) or started by hand via launch_curation_vnc.sh
# (srun --pty). Starts an Xvnc desktop + ChimeraX(+ArtiaX) from the SIF, then:
#   - writes a machine-readable session.json into $CB_SESSION_DIR (so crboost can
#     read back the node/port/password and show the user the tunnel command), and
#   - prints the same connection info as a human banner (for the manual path).
# The session lives until ChimeraX quits; then the VNC server is torn down.
#
# Required env: CX_SIF (path to chimerax_artiax.sif).
# Optional env: CX_BIN (chimerax|ChimeraX), CX_DISPLAY, CX_GEOMETRY,
#               CX_LOGIN_HOST, CB_SESSION_DIR.
set -euo pipefail

SIF="${CX_SIF:?CX_SIF must be set to the chimerax_artiax.sif path (conf.yaml curation.sif_path)}"
CXBIN="${CX_BIN:-chimerax}"
DISPLAY_NUM="${CX_DISPLAY:-1}"
GEOMETRY="${CX_GEOMETRY:-1920x1080}"
# On a compute node `hostname -f` is the compute node, NOT the login host the
# user SSHes into — crboost always passes CX_LOGIN_HOST. The fallback is only
# for a bare manual run from the login node.
LOGIN_HOST="${CX_LOGIN_HOST:-$(hostname -f)}"
SESSION_DIR="${CB_SESSION_DIR:-}"
VNC_PORT=$(( 5900 + DISPLAY_NUM ))
NODE="$(hostname -s)"
USER_NAME="${USER:-$(id -un)}"

BINDS=(-B /tmp -B /groups -B /software -B /scratch -B "$HOME")

# apptainer may be module-provided on compute nodes; ignore if already on PATH.
module load Apptainer 2>/dev/null || module load apptainer 2>/dev/null || true

# ChimeraX must write its config/cache to a WRITABLE dir — the SIF's baked
# /opt/cx is read-only (only XDG_DATA_HOME, where ArtiaX lives, stays read-only).
# Unique per-run dirs so two sessions on one shared node don't collide.
CXCFG="$(mktemp -d /tmp/cx-config.XXXXXX)"
CXCACHE="$(mktemp -d /tmp/cx-cache.XXXXXX)"

# One-time VNC password for this session. TigerVNC 1.12 here ships the server but
# no vncpasswd, and there's no python in the image — so mint the standard 8-byte
# VncAuth file with x11vnc -storepasswd (added by chimerax_artiax_patch.def).
VNC_PASS="$(head -c 16 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 8)"
mkdir -p "$HOME/.vnc"
apptainer exec "${BINDS[@]}" "$SIF" x11vnc -storepasswd "$VNC_PASS" "$HOME/.vnc/passwd" >/dev/null 2>&1
chmod 600 "$HOME/.vnc/passwd"

TUNNEL_CMD="ssh -L ${VNC_PORT}:${NODE}:${VNC_PORT} ${USER_NAME}@${LOGIN_HOST}"

# Publish machine-readable connection info BEFORE blocking on ChimeraX so crboost
# can surface it as soon as the job starts running.
if [ -n "$SESSION_DIR" ]; then
  mkdir -p "$SESSION_DIR"
  cat > "$SESSION_DIR/session.json" <<JSON
{"status": "ready", "node": "${NODE}", "display": ${DISPLAY_NUM}, "port": ${VNC_PORT}, "password": "${VNC_PASS}", "login_host": "${LOGIN_HOST}", "user": "${USER_NAME}", "tunnel_cmd": "${TUNNEL_CMD}"}
JSON
fi

cat <<EOF

==================================================================
 CHIMERAX + ARTIAX CURATION SESSION
   node:      $NODE
   display:   :$DISPLAY_NUM   (VNC rfb port $VNC_PORT)
   password:  $VNC_PASS
 On your Mac, open ONE tunnel (reuses your ControlMaster auth):
   $TUNNEL_CMD
 then point the TurboVNC Viewer at:
   localhost:$VNC_PORT
==================================================================

EOF

# Start the VNC desktop + window manager + ChimeraX in a single container shell
# (so the X11 socket in /tmp is shared). Clearing a stale server on this display
# first avoids a confusing "display in use" failure from a previously crashed run.
# Session ends when ChimeraX quits; then tear the VNC server down.
cleanup() {
  apptainer exec "${BINDS[@]}" "$SIF" bash -lc \
    "\$(command -v vncserver || command -v tigervncserver) -kill :$DISPLAY_NUM" >/dev/null 2>&1 || true
  [ -n "$SESSION_DIR" ] && rm -f "$SESSION_DIR/session.json" 2>/dev/null || true
  rm -rf "$CXCFG" "$CXCACHE" 2>/dev/null || true
}
trap cleanup EXIT

apptainer exec "${BINDS[@]}" "$SIF" bash -lc "
  VS=\"\$(command -v vncserver || command -v tigervncserver)\"
  \"\$VS\" -kill :$DISPLAY_NUM >/dev/null 2>&1 || true
  \"\$VS\" :$DISPLAY_NUM -geometry $GEOMETRY -depth 24 -localhost no -rfbport $VNC_PORT -SecurityTypes VncAuth >/dev/null 2>&1
  export XDG_CONFIG_HOME='$CXCFG' XDG_CACHE_HOME='$CXCACHE'
  export DISPLAY=:$DISPLAY_NUM
  fluxbox >/dev/null 2>&1 &
  $CXBIN
"

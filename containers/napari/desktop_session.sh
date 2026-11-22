#!/usr/bin/env bash
# Node-side worker for a napari session: EisGreis on the site's remote desktop, the one its Open OnDemand app shows
# (the image's TurboVNC, and Xfce through its ood-desktop), started the way that app starts it. crboost submits it as
# a SLURM job from a tomogram's napari chip (services/curation/session_service.py).
#
# It writes session.json into CB_SESSION_DIR (node, display, VNC port, password, websocket port, the SSH tunnel) once
# the desktop is up, which crboost reads to hand the user the desktop, and lives until the viewer exits; then the
# desktop, the VNC server and the image's instance go.
#
# Required env: CX_SIF (the remote-desktop image, conf.yaml napari.sif_path: vncserver and vncpasswd from TurboVNC,
#               and ood-desktop), CB_EISGREIS (an EisGreis install, conf.yaml napari.eisgreis), CB_SESSION_DIR.
# Optional env: CX_VGL (a full GPU: the instance runs under --nv, and EisGreis's launcher draws through VirtualGL),
#               CX_GEOMETRY, CX_BINDS (colon-separated host paths to bind), CX_LOGIN_HOST (the host the tunnel goes
#               through), CX_VNC_NOPASS (no VNC password), CB_WS_SIF (an image with websockify: serve the desktop as a
#               websocket for the portal's noVNC, and keep the VNC server on the node's localhost).
set -euo pipefail
# rc is assigned at the start of the trap's own string, which ShellCheck does not count as an assignment.
# shellcheck disable=SC2154
trap 'rc=$?; echo "==== napari desktop FAILED (exit $rc) on $(hostname -s 2>/dev/null) at line ${LINENO}: ${BASH_COMMAND} ====" >&2' ERR
trap 'exit 143' TERM
trap 'exit 129' HUP

SIF="${CX_SIF:?CX_SIF must name the remote-desktop image (conf.yaml napari.sif_path)}"
EISGREIS="${CB_EISGREIS:?CB_EISGREIS must name an EisGreis install (conf.yaml napari.eisgreis)}"
SESSION_DIR="${CB_SESSION_DIR:?CB_SESSION_DIR must name the session folder}"
GEOMETRY="${CX_GEOMETRY:-1920x1080}"
LOGIN_HOST="${CX_LOGIN_HOST:-$(hostname -f)}"
WS_SIF="${CB_WS_SIF:-}"
NODE="$(hostname -s)"
USER_NAME="${USER:-$(id -un)}"
INSTANCE="cb-napari-$(basename "$SESSION_DIR")-$$"

# apptainer may be module-provided on compute nodes; ignore if already on PATH.
module load Apptainer 2>/dev/null || module load apptainer 2>/dev/null || true

# The session folder and the install, and the site's roots crboost passes; a path this node lacks is skipped.
BINDS=(-B "$SESSION_DIR" -B "$EISGREIS")
if [ -n "${CX_BINDS:-}" ]; then
  IFS=: read -ra EXTRA_BINDS <<< "$CX_BINDS"
  for p in "${EXTRA_BINDS[@]}"; do
    if [ -n "$p" ] && [ -e "$p" ]; then
      BINDS+=(-B "$p")
    fi
  done
fi
NV=()
if [ -n "${CX_VGL:-}" ]; then
  NV=(--nv)
fi

DISPLAY_NUM=""
DESKTOP_PID=""
VIEWER_PID=""
WS_PID=""
cleanup() {
  # A second signal must not cut this short: another Stop, or SLURM's own TERM when the job is cancelled outright.
  trap '' TERM HUP
  # The viewer first, with the display still up: on SIGTERM it closes its window, which saves the comments and the
  # particle STARs, and removes its desktop.json. The signal goes to the viewer's own process, found by its command
  # line (this session's folder ends it) inside the instance, where only this session's processes are: a search of
  # the whole node reads every process's command line and can stall behind someone else's busy process (50 s, once).
  # apptainer's wrapper (VIEWER_PID) does not pass a signal on, but lives exactly as long as the viewer. crboost
  # stops a session by signalling this script alone (scancel --batch), so the desktop is still up meanwhile.
  if [ -n "$VIEWER_PID" ] && kill -0 "$VIEWER_PID" 2>/dev/null &&
    apptainer exec "instance://$INSTANCE" pkill -TERM -f -- "eisgreis-crboost --session-dir $SESSION_DIR\$" 2>/dev/null; then
    for _ in $(seq 1 40); do
      kill -0 "$VIEWER_PID" 2>/dev/null || break
      sleep 0.5
    done
  fi
  [ -n "$VIEWER_PID" ] && kill "$VIEWER_PID" 2>/dev/null || true
  [ -n "$WS_PID" ] && kill "$WS_PID" 2>/dev/null || true
  [ -n "$DESKTOP_PID" ] && kill "$DESKTOP_PID" 2>/dev/null || true
  if [ -n "$DISPLAY_NUM" ]; then
    apptainer exec "instance://$INSTANCE" vncserver -kill ":$DISPLAY_NUM" >/dev/null 2>&1 || true
  fi
  apptainer instance stop "$INSTANCE" >/dev/null 2>&1 || true
  rm -f "$SESSION_DIR/session.json" "$SESSION_DIR/vnc.passwd" "$SESSION_DIR/desktop.json" "$SESSION_DIR/alive"
}
trap cleanup EXIT

apptainer instance start "${NV[@]}" "${BINDS[@]}" "$SIF" "$INSTANCE" >/dev/null

# The VNC password: TurboVNC's own file, readable by this user only.
if [ -n "${CX_VNC_NOPASS:-}" ]; then
  VNC_PASS=""
  SECURITY=(-securitytypes None)
  echo "NOTE: CX_VNC_NOPASS set: the desktop takes connections with no password." >&2
else
  VNC_PASS="$(head -c 16 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 8)"
  (umask 077 && printf '%s\n' "$VNC_PASS" | apptainer exec "instance://$INSTANCE" vncpasswd -f > "$SESSION_DIR/vnc.passwd")
  SECURITY=(-rfbauth "$SESSION_DIR/vnc.passwd" -securitytypes VNC)
fi

# The VNC server picks a free display. With the portal's websocket it listens on localhost only; without, on every
# interface, for the SSH tunnel.
LOCAL=()
if [ -n "$WS_SIF" ]; then
  LOCAL=(-localhost)
fi
HTTPD=()
if apptainer exec "instance://$INSTANCE" vncserver -help 2>&1 | grep -q nohttpd; then
  HTTPD=(-nohttpd)
fi
# -alr: half a second after the picture stops changing, what went out as JPEG goes again losslessly, so a still
# tomogram is exactly what the viewer drew. noVNC asks for JPEG quality level 6 (JPEG 79); a whole screen of a real
# tomogram is ~0.5 MB that way and ~1 MB lossless, and both still frames matched a lossless capture pixel for pixel
# (roadmap 61, 2026-11-20).
VNC_OUT="$(apptainer exec "instance://$INSTANCE" vncserver -log "$SESSION_DIR/vnc.log" "${SECURITY[@]}" \
  "${HTTPD[@]}" -noxstartup -alr 0.5 -geometry "$GEOMETRY" "${LOCAL[@]}" 2>&1)"
DISPLAY_NUM="$(printf '%s\n' "$VNC_OUT" | awk -F: '/^Desktop/{print $NF}')"
if [ -z "$DISPLAY_NUM" ]; then
  echo "The VNC server did not start: $VNC_OUT" >&2
  exit 1
fi
VNC_PORT=$((5900 + DISPLAY_NUM))

# With --nv apptainer puts the host's NVIDIA and glvnd libraries into /.singularity.d/libs, which the image's
# programs cannot load: its own glvnd libraries go in front, as the portal's desktop does it. The links point at image
# paths and resolve inside the instance; one the image lacks dangles, and the loader skips it.
GLVND="$SESSION_DIR/glvnd"
mkdir -p "$GLVND"
for lib in libGL.so.1 libGLX.so.0 libEGL.so.1 libGLdispatch.so.0 libOpenGL.so.0 libGLESv1_CM.so.1 libGLESv2.so.2; do
  ln -sfn "/usr/lib64/$lib" "$GLVND/$lib"
done
# EISGREIS_NO_AUTOSTART: the user's autostart entry runs in this desktop too, and stands down for the viewer below.
DESKTOP_ENV=(--env "DISPLAY=:$DISPLAY_NUM" --env "LD_LIBRARY_PATH=$GLVND:/.singularity.d/libs"
  --env "EISGREIS_NO_AUTOSTART=1")

cd "$HOME"
apptainer exec "${DESKTOP_ENV[@]}" "instance://$INSTANCE" ood-desktop > "$SESSION_DIR/desktop.log" 2>&1 &
DESKTOP_PID=$!

# websockify before session.json: crboost hands out the browser link once session.json exists.
WS_PORT=""
if [ -n "$WS_SIF" ]; then
  for p in $((25900 + DISPLAY_NUM)) $(shuf -i 26000-26999 -n 20); do
    if ! (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
      WS_PORT="$p"
      break
    fi
  done
  if [ -z "$WS_PORT" ]; then
    echo "No free port for websockify on $NODE." >&2
    exit 1
  fi
  apptainer exec "$WS_SIF" websockify "$WS_PORT" "127.0.0.1:$VNC_PORT" > "$SESSION_DIR/websockify.log" 2>&1 &
  WS_PID=$!
  sleep 2
  if ! kill -0 "$WS_PID" 2>/dev/null; then
    echo "websockify did not start on port $WS_PORT (image $WS_SIF):" >&2
    tail -5 "$SESSION_DIR/websockify.log" >&2 || true
    exit 1
  fi
fi

TUNNEL_CMD="kill \$(lsof -ti tcp:${VNC_PORT}) 2>/dev/null; ssh -f -N -o ExitOnForwardFailure=yes -L ${VNC_PORT}:${NODE}:${VNC_PORT} ${USER_NAME}@${LOGIN_HOST}"
cat > "$SESSION_DIR/session.json.tmp" <<JSON
{"status": "ready", "app": "napari", "desktop": "remote desktop", "node": "${NODE}", "display": ${DISPLAY_NUM}, "port": ${VNC_PORT}, "rest_port": null, "password": "${VNC_PASS}", "ws_port": ${WS_PORT:-null}, "login_host": "${LOGIN_HOST}", "user": "${USER_NAME}", "tunnel_cmd": "${TUNNEL_CMD}"}
JSON
mv "$SESSION_DIR/session.json.tmp" "$SESSION_DIR/session.json"

printf '\n%s\n NAPARI SESSION (EisGreis on the remote desktop)\n' "=================================================================="
printf '   node:      %s\n   display:   :%s   (VNC port %s)\n   password:  %s\n' \
  "$NODE" "$DISPLAY_NUM" "$VNC_PORT" "${VNC_PASS:-<none>}"
if [ -n "$WS_PORT" ]; then
  printf "   websocket: %s  (the portal's noVNC, through /rnode/%s/%s/websockify)\n" "$WS_PORT" "$NODE" "$WS_PORT"
fi
printf '%s\n\n' "=================================================================="

# The window manager first, so the viewer's window opens maximized. Looked for inside the instance, as the viewer is
# in cleanup(): a search of the whole node took 42 s on a busy one.
for _ in $(seq 1 40); do
  apptainer exec "instance://$INSTANCE" pgrep -x xfwm4 >/dev/null 2>&1 && break
  sleep 0.5
done

# The session lasts as long as the viewer: quitting it ends the job.
apptainer exec "${DESKTOP_ENV[@]}" "instance://$INSTANCE" "$EISGREIS/eisgreis-crboost" --session-dir "$SESSION_DIR" &
VIEWER_PID=$!
wait "$VIEWER_PID" || true
VIEWER_PID=""

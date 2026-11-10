#!/bin/sh
# This checkout's Python environment: found, or built first. Prints its path on stdout; progress goes to stderr.
#
# An environment is named by what goes into it, <python>-<12 hex of a sha256 over the lock's pin lines>, and is never
# modified once it exists. A lock change builds a new one beside the old (servers still running keep theirs), and
# going back to the old lock finds the old one again. It is built under a temporary name and renamed when complete,
# so a name that exists is a finished environment, whoever started it and however it was interrupted.
#
#   ./install.sh            find or build the environment, print its path
#   ./install.sh --path     print the path, never build
#   ./install.sh --lock     recompile requirements.txt from requirements.in with the pinned uv
#   ./install.sh --clean    list the environments this lock no longer needs; --yes removes them
#
# What goes in is data, not code: requirements.txt (what), .python-version (which Python), uv.toml (which uv, and
# how it behaves). To try something without touching those: CRBOOST_LOCK (another lock file), CRBOOST_PYTHON (another
# Python, as x.y.z), CRBOOST_ENV_ROOT (where environments live; the default is .uv/env in this checkout), CRBOOST_UV
# (a uv binary of your own). uv's own variables (UV_INDEX_URL, UV_PYTHON_INSTALL_MIRROR, ...) work as usual.
set -eu
here=$(cd "$(dirname "$0")" && pwd -P)

log() { printf 'install: %s\n' "$*" >&2; }
die() { log "$*"; exit 1; }

lock=${CRBOOST_LOCK:-$here/requirements.txt}
python=${CRBOOST_PYTHON:-$(sed -n '1p' "$here/.python-version" 2>/dev/null | tr -d '[:space:]')}
env_root=${CRBOOST_ENV_ROOT:-$here/.uv/env}
uv=${CRBOOST_UV:-$here/.uv/bin/uv}

printf '%s' "$python" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' ||
    die "Python must be an exact version x.y.z (.python-version or CRBOOST_PYTHON), got '$python'"
[ -f "$lock" ] || die "no lock file at $lock"

# The name: the Python, and a hash of the lock's pin lines (comments and blank lines ignored, order too).
pins=$(sed -e 's/[[:space:]]*#.*//' -e '/^[[:space:]]*$/d' "$lock" | LC_ALL=C sort)
[ -n "$pins" ] || die "$lock has no pins"
name=$python-$(printf '%s\n%s\n' "$python" "$pins" | sha256sum | cut -c1-12)
env=$env_root/$name

# Nothing the shell loaded may reach a build.
unset PYTHONPATH PYTHONHOME PYTHONSTARTUP PYTHONUSERBASE VIRTUAL_ENV CONDA_PREFIX

# uv, with this checkout's uv.toml and nobody else's configuration.
run_uv() {
    UV_NO_CONFIG=1 UV_CACHE_DIR="$here/.uv/cache" UV_PYTHON_INSTALL_DIR="$here/.uv/python" \
        "$uv" --config-file "$here/uv.toml" "$@"
}

ensure_uv() {
    [ -z "${CRBOOST_UV:-}" ] || return 0
    want=$(sed -n 's/^required-version *= *"==\([0-9][0-9.]*\)".*/\1/p' "$here/uv.toml")
    [ -n "$want" ] || die 'uv.toml has no required-version = "==x.y.z"'
    if [ "$("$uv" --version 2>/dev/null | cut -d' ' -f2)" != "$want" ]; then
        log "fetching uv $want into $here/.uv/bin"
        # Into a scratch directory and renamed, so two first starts never write the same binary.
        fetch=$here/.uv/fetch-$$
        rm -rf "$fetch"
        curl -LsSf "https://astral.sh/uv/$want/install.sh" | env UV_UNMANAGED_INSTALL="$fetch" sh >&2 || true
        if [ -x "$fetch/uv" ]; then
            mkdir -p "$here/.uv/bin"
            mv -f "$fetch/uv" "$uv"
        fi
        rm -rf "$fetch"
        [ "$("$uv" --version 2>/dev/null | cut -d' ' -f2)" = "$want" ] ||
            die "could not fetch uv $want from astral.sh; put a uv $want binary at $uv, or point CRBOOST_UV at one"
    fi
}

# venv -> the environment this checkout is on, for editors and habit. Nothing here depends on it, and a venv/ that
# is a real directory (an install from before environments were named) is left alone.
link_alias() {
    if [ -L "$here/venv" ] || [ ! -e "$here/venv" ]; then
        ln -sfn "$env" "$here/venv" 2>/dev/null || true
    fi
}

clean() {
    [ -z "${CRBOOST_ENV_ROOT:-}" ] ||
        die "$env_root may hold other checkouts' environments; remove what you do not need by hand"
    yes=; [ "${1:-}" = "--yes" ] && yes=1
    keep_other=$(ls -dt "$env_root"/*/ 2>/dev/null | sed 's|/$||' | grep -v -x -F "$env" | grep -v -F '.tmp-' |
        grep -v -F '.old-' | head -n 1 || true)
    n=0
    for d in "$env_root"/*/; do
        [ -d "$d" ] || continue
        d=${d%/}
        base=${d##*/}
        case $base in
            *.old-*) why="left over from an interrupted removal" ;;
            *.tmp-*)
                [ -n "$(find "$d" -maxdepth 0 -mmin +1440)" ] || { echo "building  $base"; continue; }
                why="an interrupted build" ;;
            *)
                [ "$d" != "$env" ] || { echo "current   $base"; continue; }
                [ "$d" != "$keep_other" ] || { echo "previous  $base (kept for the way back)"; continue; }
                # Servers are started as "<environment>/bin/python3 crboost.py": look for that on this node.
                if grep -l -a -F "$d/" /proc/[0-9]*/cmdline >/dev/null 2>&1; then
                    echo "in use    $base (a process on this node runs it)"
                    continue
                fi
                why="not this lock's, not the previous one" ;;
        esac
        size=$(du -sh "$d" 2>/dev/null | cut -f1)
        if [ -n "$yes" ]; then
            if mv -T "$d" "$d.old-$$" 2>/dev/null && rm -rf "$d.old-$$" 2>/dev/null; then
                echo "removed   $base ($size, $why)"
            else
                echo "busy      $base: something still has files open (NFS); the next clean retries"
            fi
        else
            echo "remove    $base ($size, $why)"
        fi
        n=$((n + 1))
    done
    if [ "$n" -gt 0 ] && [ -z "$yes" ]; then
        log "nothing removed; run './crboost clean --yes' to remove the lines marked remove."
    fi
    log "a server on another login node cannot be seen from here: stop those before removing what they run."
}

case ${1:-} in
    --path) printf '%s\n' "$env"; exit 0 ;;
    --clean) shift; clean "$@"; exit 0 ;;
    --lock)
        ensure_uv
        # Relative paths: uv records the command in the lock's header, and that file is committed.
        cd "$here"
        exec env UV_NO_CONFIG=1 UV_CACHE_DIR="$here/.uv/cache" "$uv" --config-file uv.toml \
            pip compile requirements.in -o requirements.txt
        ;;
    "") ;;
    *) die "unknown option $1 (see the top of this file)" ;;
esac

if [ -x "$env/bin/python3" ]; then
    link_alias
    printf '%s\n' "$env"
    exit 0
fi

# Build. A shared install is written by whoever starts it first, and cleaned up by anyone: group-writable.
umask 002
if ! mkdir -p "$env_root" 2>/dev/null || [ ! -w "$env_root" ]; then
    die "no environment for this lock ($name) and $env_root is not writable by you: ask whoever installed CryoBoost to run $here/install.sh"
fi
ensure_uv

tmp=$env_root/$name.tmp-$$
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

log "building environment $name (once per lock and Python; servers already running are not affected)"
# --no-bin: uv would otherwise put a python3.11 shim in ~/.local/bin, outside this checkout.
run_uv python install --no-bin "$python" >&2
run_uv venv --relocatable --python "$python" "$tmp" >&2
run_uv pip sync --python "$tmp/bin/python3" "$lock" >&2
"$tmp/bin/python3" -I -c "import sys; assert '.'.join(map(str, sys.version_info[:3])) == '$python', sys.version"

{
    echo "name      $name"
    echo "python    $python"
    echo "uv        $("$uv" --version)"
    echo "lock      $lock"
    echo "lock sha  $(sha256sum "$lock" | cut -d' ' -f1)"
    echo "built     $(date -u +%Y-%m-%dT%H:%M:%SZ) by $(id -un)@$(hostname)"
    echo "checkout  $(git -C "$here" rev-parse --short HEAD 2>/dev/null || echo unknown)"
} > "$tmp/BUILD"
chmod -R a+rX "$tmp"

# One rename publishes it. If another start finished the same environment first, theirs stands and ours goes.
if mv -T "$tmp" "$env" 2>/dev/null; then
    log "built $name"
elif [ -x "$env/bin/python3" ]; then
    log "another start finished $name first; using it"
else
    die "could not move the finished build to $env"
fi
link_alias
printf '%s\n' "$env"

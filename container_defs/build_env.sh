# What every image's build needs, whatever the host builds it (roadmap 63 D19, D20). Every def puts this file at
# /opt/crboost/build_env.sh through %files and sources it first thing in each stage's %post:
#
#   %files
#       build_env.sh /opt/crboost/build_env.sh
#   %post
#       set -e
#       . /opt/crboost/build_env.sh
#
# It stays in the image, so `apptainer exec <image> cat /opt/crboost/build_env.sh` shows what built it. POSIX sh:
# Apptainer runs %post with /bin/sh.

export DEBIAN_FRONTEND=noninteractive
export PYTHONNOUSERSITE=1
unset PYTHONPATH PYTHONHOME

# The CA bundle container_defs/build.sh binds here: the host's own, or a site's (CRBOOST_BUILD_CA) when its network
# inspects TLS. Every downloader checks against it; no image keeps it, and no check is ever turned off. A base image
# has no bundle of its own until apt installs one.
BUILD_CA=/mnt/build-ca.pem
if [ -f "$BUILD_CA" ]; then
    export SSL_CERT_FILE="$BUILD_CA" CURL_CA_BUNDLE="$BUILD_CA" REQUESTS_CA_BUNDLE="$BUILD_CA" PIP_CERT="$BUILD_CA" \
        CONDA_SSL_VERIFY="$BUILD_CA" GIT_SSL_CAINFO="$BUILD_CA"
    echo "build_env: TLS checked against the CA bundle the build was lent"
fi

# Ubuntu's archive as it was at this moment (snapshot.ubuntu.com): every image gets the apt packages of this date,
# whenever it is built. Moved on purpose, like a lock, never by a rebuild. A source without snapshots (NVIDIA's
# repository in the CUDA build stage, from which nothing is installed) is read as it is.
APT_SNAPSHOT=20261125T180000Z

# apt_install <package>...: apt-get install under any fakeroot, from the archive of APT_SNAPSHOT. A host without a
# subuid range for the user builds with --fakeroot --ignore-fakeroot-command, where only root is mapped: apt cannot
# drop to its _apt user (setgroups 65534 fails) and package scripts cannot chown to system users (EINVAL). apt stays
# root and chown/chgrp ignore that while packages install; both are put back afterwards. Harmless under real root or a
# full subuid range.
apt_install() {
    if [ ! -f "$BUILD_CA" ] && [ ! -f /etc/ssl/certs/ca-certificates.crt ]; then
        echo "apt_install: no CA bundle to reach snapshot.ubuntu.com with: build through container_defs/build.sh," >&2
        echo "             which lends the build the host's (or CRBOOST_BUILD_CA)" >&2
        return 1
    fi
    conf=/etc/apt/apt.conf.d/99-crboost-build
    echo 'APT::Sandbox::User "root";' > "$conf"
    echo "APT::Snapshot \"$APT_SNAPSHOT\";" >> "$conf"
    if [ -f "$BUILD_CA" ]; then
        echo "Acquire::https::CaInfo \"$BUILD_CA\";" >> "$conf"
    fi
    for c in chown chgrp; do
        dpkg-divert --local --rename --add /usr/bin/$c
        printf '#!/bin/sh\n/usr/bin/%s.distrib "$@" 2>/dev/null || true\n' $c > /usr/bin/$c
        chmod +x /usr/bin/$c
    done
    apt-get update
    apt-get install -y --no-install-recommends "$@"
    rm -rf /var/lib/apt/lists/*
    for c in chown chgrp; do
        rm /usr/bin/$c
        dpkg-divert --local --rename --remove /usr/bin/$c
    done
    rm "$conf"
    test -z "$(dpkg --audit)"
}

# fetch <url> <sha256> <file>: a download kept only if its sha256 is the one given. Needs curl (apt_install it).
fetch() {
    curl -fsSL --retry 5 -o "$3" "$1"
    echo "$2  $3" | sha256sum -c -
}

# install_miniforge: Miniforge 26.9.4-1 at /opt/miniforge3, conda-forge only; environments come from explicit locks.
install_miniforge() {
    fetch https://github.com/conda-forge/miniforge/releases/download/26.9.4-1/Miniforge3-26.9.4-1-Linux-x86_64.sh \
        571b410272cc434f28bc6c5c49ca8ffd4a4d1bcba9f57f1b6baccdb46446cfcc /tmp/miniforge.sh
    bash /tmp/miniforge.sh -b -p /opt/miniforge3
    rm /tmp/miniforge.sh
    export PATH="/opt/miniforge3/bin:$PATH"
}

# install_uv: uv at /usr/local/bin/uv, the version uv.toml pins for crboost's own environment (tests/test_repo.py keeps
# the two the same), for an image whose environment is pip packages only (roadmap 63 D19). The Pythons it installs go
# to /opt/python; the def removes uv once the environment is made.
install_uv() {
    fetch https://github.com/astral-sh/uv/releases/download/0.13.7/uv-x86_64-unknown-linux-gnu.tar.gz \
        c5221ff41b7892bfc223543883600125f6a47fe845093e0f5bdff4f359196d83 /tmp/uv.tar.gz
    # As root tar restores the archive's owner, which a root-only fakeroot cannot map.
    tar --no-same-owner -xzf /tmp/uv.tar.gz -C /tmp
    mv /tmp/uv-x86_64-unknown-linux-gnu/uv /usr/local/bin/uv
    rm -rf /tmp/uv.tar.gz /tmp/uv-x86_64-unknown-linux-gnu
    export UV_NO_CONFIG=1 UV_NO_CACHE=1 UV_PYTHON_INSTALL_DIR=/opt/python UV_PYTHON_PREFERENCE=only-managed
}

# ldd_complete <file>...: every ELF given loads every library it needs from the image as it is.
ldd_complete() {
    missing=$(for f in "$@"; do
        if [ -f "$f" ] && head -c 4 "$f" | grep -q ELF; then ldd "$f" 2>/dev/null | grep 'not found' | sed "s|^|$f: |"; fi
    done)
    if [ -n "$missing" ]; then
        echo "$missing"
        return 1
    fi
}

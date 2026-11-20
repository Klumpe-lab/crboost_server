#!/bin/sh
# ./crboost eisgreis-install [DIR] [--ref REF]: install EisGreis, the viewer behind the napari chip, and print the
# conf.yaml line that names it.
#
# DIR (default: EisGreis beside this crboost checkout) gets a clone of EisGreis at REF, a release tag or a branch
# (default: the newest release tag), and then EisGreis's own install.sh: its environment, its X libraries and its
# launchers are EisGreis's business, so a viewer fix needs no crboost change. Run again on the same DIR, it updates: a
# clone at a tag moves to REF, a clone on the branch REF pulls it. A clone with local changes, or on another branch,
# is left alone.
#
# EISGREIS_REPO names another repository to clone (default: the lab's, below).
set -eu
here=$(cd "$(dirname "$0")/.." && pwd -P)
repo=${EISGREIS_REPO:-https://github.com/Klumpe-lab/EisGreis.git}

die() { printf 'eisgreis-install: %s\n' "$*" >&2; exit 1; }

dir=
ref=
while [ $# -gt 0 ]; do
    case $1 in
        --ref) [ $# -ge 2 ] || die "--ref needs a tag or a branch"; ref=$2; shift 2 ;;
        --ref=*) ref=${1#--ref=}; shift ;;
        -h | --help) sed -n '2,11p' "$0"; exit 0 ;;
        -*) die "unknown option $1 (see ./crboost eisgreis-install --help)" ;;
        *) [ -z "$dir" ] || die "one folder only"; dir=$1; shift ;;
    esac
done
dir=${dir:-$(dirname "$here")/EisGreis}
command -v git >/dev/null 2>&1 || die "git is needed"
# A shared install is used by the whole lab: group-writable.
umask 002

if [ -z "$ref" ]; then
    ref=$(git ls-remote --tags --refs --sort=-v:refname "$repo" 2>/dev/null | head -n 1 | sed 's|.*refs/tags/||')
    [ -n "$ref" ] || die "$repo has no release tag yet: name a branch or a tag with --ref"
fi

if [ -d "$dir/.git" ]; then
    [ -z "$(git -C "$dir" status --porcelain --untracked-files=no)" ] ||
        die "$dir has local changes: commit or stash them there, or install into another folder"
    git -C "$dir" fetch -q --tags origin
    branch=$(git -C "$dir" symbolic-ref -q --short HEAD || true)
    if [ -z "$branch" ]; then
        git -C "$dir" checkout -q --detach "$ref" 2>/dev/null || git -C "$dir" checkout -q --detach "origin/$ref" ||
            die "$ref is neither a tag nor a branch of $repo"
    elif [ "$branch" = "$ref" ]; then
        git -C "$dir" pull -q --ff-only origin "$ref" || die "$dir could not be brought up to origin/$ref"
    else
        die "$dir is on the branch $branch, not $ref: switch it yourself, or install into another folder"
    fi
else
    if [ -e "$dir" ] && [ -n "$(ls -A "$dir" 2>/dev/null)" ]; then
        die "$dir exists and is no git clone: name an empty or new folder"
    fi
    git clone -q --branch "$ref" "$repo" "$dir" || die "could not clone $repo at $ref"
fi

[ -x "$dir/install.sh" ] || die "$dir at $ref has no install.sh: that EisGreis predates its cluster install"
"$dir/install.sh" >/dev/null
versions=$("$dir/eisgreis-crboost" --version) || die "$dir/eisgreis-crboost --version did not answer"
echo "EisGreis at $dir ($(git -C "$dir" describe --tags --always 2>/dev/null || echo "$ref")): $versions"
echo
echo "In config/conf.yaml (or ~/.crboost/conf.yaml):"
echo
echo "napari:"
echo "  eisgreis: \"$dir\""

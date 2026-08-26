#!/bin/sh
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Build an offline Linux tarball. This is a copy/checksum/archive operation;
# it never invokes pip, a compiler, or the network.

set -eu

EX_USAGE=64
EX_UNAVAILABLE=69
EX_SOFTWARE=70
EX_CANTCREAT=73

fail() {
    code=$1
    shift
    printf 'build_release.sh: %s\n' "$*" >&2
    exit "$code"
}

SCRIPT_DIR=$(CDPATH= cd -P -- "$(dirname -- "$0")" 2>/dev/null && pwd) \
    || fail "$EX_SOFTWARE" "cannot resolve scripts directory"
PROJECT_ROOT=$(CDPATH= cd -P -- "$SCRIPT_DIR/.." 2>/dev/null && pwd) \
    || fail "$EX_SOFTWARE" "cannot resolve project root"

force=0
output_dir=$PROJECT_ROOT/dist
output_seen=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --force) force=1 ;;
        -*) fail "$EX_USAGE" "unknown option: $1" ;;
        *)
            [ "$output_seen" -eq 0 ] || fail "$EX_USAGE" "usage: scripts/build_release.sh [--force] [OUTPUT_DIRECTORY]"
            output_dir=$1
            output_seen=1
            ;;
    esac
    shift
done

command -v tar >/dev/null 2>&1 || fail "$EX_UNAVAILABLE" "tar is required"
[ -r "$PROJECT_ROOT/VERSION" ] || fail "$EX_SOFTWARE" "VERSION is missing"
[ -r "$PROJECT_ROOT/MANIFEST.release" ] || fail "$EX_SOFTWARE" "MANIFEST.release is missing"

version=$(tr -d '\r\n' < "$PROJECT_ROOT/VERSION")
case "$version" in
    ''|*[!A-Za-z0-9._-]*) fail "$EX_SOFTWARE" "invalid VERSION: $version" ;;
esac
release_name=hcu-envcheck-$version
pyproject_version=$(sed -n 's/^version[[:space:]]*=[[:space:]]*"\([^"]*\)"[[:space:]]*$/\1/p' "$PROJECT_ROOT/pyproject.toml" | head -n 1)
module_version=$(sed -n 's/^__version__[[:space:]]*=[[:space:]]*"\([^"]*\)"[[:space:]]*$/\1/p' "$PROJECT_ROOT/hcu_envcheck/__init__.py" | head -n 1)
[ "$pyproject_version" = "$version" ] \
    || fail "$EX_SOFTWARE" "version mismatch: VERSION=$version pyproject.toml=$pyproject_version"
[ "$module_version" = "$version" ] \
    || fail "$EX_SOFTWARE" "version mismatch: VERSION=$version hcu_envcheck/__init__.py=$module_version"
archive=$output_dir/$release_name.tar.gz
if [ "$force" -ne 1 ] && { [ -e "$archive" ] || [ -e "$archive.sha256" ]; }; then
    fail "$EX_CANTCREAT" "release already exists: $archive (use --force to replace)"
fi

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/hcu-envcheck-release.XXXXXX") \
    || fail "$EX_CANTCREAT" "cannot create temporary directory"
stage=$work_dir/$release_name
cleanup() { rm -rf -- "$work_dir"; }
trap cleanup EXIT HUP INT TERM
mkdir -p -- "$stage" "$output_dir" \
    || fail "$EX_CANTCREAT" "cannot create staging or output directory"

while IFS=' ' read -r policy path rest; do
    case "$policy" in
        ''|'#'*) continue ;;
        required|optional) ;;
        *) fail "$EX_SOFTWARE" "invalid manifest policy: $policy" ;;
    esac
    [ -z "${rest-}" ] || fail "$EX_SOFTWARE" "manifest paths cannot contain spaces: $path $rest"
    source=$PROJECT_ROOT/$path
    if [ ! -e "$source" ]; then
        [ "$policy" = optional ] && continue
        fail "$EX_SOFTWARE" "required release input is missing: $path"
    fi
    cp -R -- "$source" "$stage/$path" \
        || fail "$EX_CANTCREAT" "cannot stage: $path"
done < "$PROJECT_ROOT/MANIFEST.release"

# Never publish interpreter caches or editor/test artifacts.
find "$stage" -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete
find "$stage" -type d -name '__pycache__' -exec rm -rf -- {} +
chmod 755 -- "$stage/hcu-envcheck.sh" "$stage/install.sh" "$stage"/bin/* "$stage"/examples/*.sh
find "$stage/hcu_envcheck" -type f -name '*.py' -exec chmod 644 -- {} +

vcs_commit=UNAVAILABLE
if command -v git >/dev/null 2>&1; then
    # PROJECT_ROOT may be a subdirectory of a monorepo and therefore have no
    # .git entry of its own. Let Git discover the enclosing work tree.
    detected_commit=$(git -C "$PROJECT_ROOT" rev-parse --verify HEAD 2>/dev/null || true)
    [ -z "$detected_commit" ] || vcs_commit=$detected_commit
fi
{
    printf 'version=%s\n' "$version"
    printf 'build_time_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'vcs_commit=%s\n' "$vcs_commit"
} > "$stage/RELEASE-INFO.txt"
chmod 644 -- "$stage/RELEASE-INFO.txt"

manifest=$stage/RELEASE-MANIFEST.sha256
if command -v sha256sum >/dev/null 2>&1; then
    (cd "$stage" && find . -type f ! -name 'RELEASE-MANIFEST.sha256' -print | LC_ALL=C sort | \
        while IFS= read -r file; do sha256sum "$file"; done) > "$manifest"
elif command -v shasum >/dev/null 2>&1; then
    (cd "$stage" && find . -type f ! -name 'RELEASE-MANIFEST.sha256' -print | LC_ALL=C sort | \
        while IFS= read -r file; do shasum -a 256 "$file"; done) > "$manifest"
else
    fail "$EX_UNAVAILABLE" "sha256sum or shasum is required"
fi

archive_tmp=$work_dir/$release_name.tar.gz
tar -C "$work_dir" -czf "$archive_tmp" "$release_name" \
    || fail "$EX_CANTCREAT" "cannot create archive"
mv -- "$archive_tmp" "$archive" \
    || fail "$EX_CANTCREAT" "cannot publish archive: $archive"

if command -v sha256sum >/dev/null 2>&1; then
    (cd "$output_dir" && sha256sum "$release_name.tar.gz") > "$archive.sha256"
else
    (cd "$output_dir" && shasum -a 256 "$release_name.tar.gz") > "$archive.sha256"
fi

printf 'Release: %s\n' "$archive"
printf 'Checksum: %s\n' "$archive.sha256"
printf 'The archive is offline and contains no third-party Python dependencies.\n'

#!/bin/sh
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Copy-only offline installer. It installs only checksum-listed release files;
# no pip, compiler, package manager, or network is used.

set -eu

EX_USAGE=64
EX_DATAERR=65
EX_SOFTWARE=70
EX_CANTCREAT=73
EX_TEMPFAIL=75

usage() {
    cat <<'EOF'
Usage: ./install.sh [--prefix DIR] [--force]

Options:
  --prefix DIR  Install below DIR (default: $HOME/.local)
  --force       Replace the same installed version and command links
  -h, --help    Show this help

This installer verifies the release and performs local file copies only.
EOF
}

fail() {
    code=$1
    shift
    printf 'install.sh: %s\n' "$*" >&2
    exit "$code"
}

resolve_script_dir() {
    target=$1
    while [ -h "$target" ]; do
        target_dir=$(CDPATH= cd -P -- "$(dirname -- "$target")" 2>/dev/null && pwd) || return 1
        link=$(readlink "$target") || return 1
        case "$link" in /*) target=$link ;; *) target=$target_dir/$link ;; esac
    done
    CDPATH= cd -P -- "$(dirname -- "$target")" 2>/dev/null && pwd
}

SOURCE_ROOT=$(resolve_script_dir "$0") || fail "$EX_SOFTWARE" "cannot resolve package directory"
prefix=${HOME:+$HOME/.local}
force=0

while [ "$#" -gt 0 ]; do
    case "$1" in
        --prefix)
            [ "$#" -ge 2 ] || fail "$EX_USAGE" "--prefix requires a directory"
            prefix=$2
            shift 2
            ;;
        --force) force=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) fail "$EX_USAGE" "unknown option: $1 (use --help)" ;;
    esac
done

[ -n "${prefix-}" ] || fail "$EX_USAGE" "HOME is unset; specify --prefix DIR"
[ -r "$SOURCE_ROOT/VERSION" ] || fail "$EX_SOFTWARE" "VERSION is missing"
[ -r "$SOURCE_ROOT/RELEASE-MANIFEST.sha256" ] \
    || fail "$EX_DATAERR" "RELEASE-MANIFEST.sha256 is missing; install from a built release archive"
[ -x "$SOURCE_ROOT/bin/hcu-envcheck-verify" ] \
    || fail "$EX_SOFTWARE" "bin/hcu-envcheck-verify is missing or not executable"

if ! "$SOURCE_ROOT/bin/hcu-envcheck-verify" >/dev/null; then
    fail "$EX_DATAERR" "release checksum verification failed"
fi

version=$(tr -d '\r\n' < "$SOURCE_ROOT/VERSION")
case "$version" in
    ''|*[!A-Za-z0-9._-]*) fail "$EX_SOFTWARE" "invalid VERSION: $version" ;;
esac

lib_dir=$prefix/lib
bin_dir=$prefix/bin
target=$lib_dir/hcu-envcheck-$version
stage=$lib_dir/.hcu-envcheck-$version.$$
backup=$lib_dir/.hcu-envcheck-$version.backup.$$
lock=$lib_dir/.hcu-envcheck.install.lock
transaction=$lib_dir/.hcu-envcheck-install-transaction.$$
lock_owned=0
transaction_active=0
install_complete=0

cleanup() {
    if [ "$transaction_active" -eq 1 ] && [ "$install_complete" -eq 0 ]; then
        for command_name in hcu-envcheck hcu-envcheck-doctor hcu-envcheck-verify; do
            link_path=$bin_dir/$command_name
            old_link=$transaction/$command_name.old
            absent_marker=$transaction/$command_name.absent
            if [ -e "$old_link" ] || [ -L "$old_link" ]; then
                rm -f -- "$link_path" 2>/dev/null || true
                mv -- "$old_link" "$link_path" 2>/dev/null || true
            elif [ -f "$absent_marker" ]; then
                rm -f -- "$link_path" 2>/dev/null || true
            fi
        done
        if [ -e "$backup" ] || [ -L "$backup" ]; then
            rm -rf -- "$target" 2>/dev/null || true
            mv -- "$backup" "$target" 2>/dev/null || true
        elif [ -f "$transaction/target.absent" ]; then
            rm -rf -- "$target" 2>/dev/null || true
        fi
    fi
    [ ! -e "$stage" ] || rm -rf -- "$stage"
    [ ! -e "$transaction" ] || rm -rf -- "$transaction"
    for command_name in hcu-envcheck hcu-envcheck-doctor hcu-envcheck-verify; do
        rm -f -- "$bin_dir/.${command_name}.new.$$" 2>/dev/null || true
    done
    if [ "$lock_owned" -eq 1 ] && [ -d "$lock" ]; then
        rm -f -- "$lock/pid" 2>/dev/null || true
        rmdir -- "$lock" 2>/dev/null || true
    fi
}
trap cleanup EXIT HUP INT TERM
mkdir -p -- "$lib_dir" "$bin_dir" || fail "$EX_CANTCREAT" "cannot create directories below: $prefix"
mkdir -- "$lock" 2>/dev/null \
    || fail "$EX_TEMPFAIL" "another hcu-envcheck install is active below this prefix: $lock"
lock_owned=1
printf '%s\n' "$$" > "$lock/pid" 2>/dev/null || true
mkdir -p -- "$stage" || fail "$EX_CANTCREAT" "cannot create staging directory"

# RELEASE-MANIFEST.sha256 contains only build-time whitelist paths. Validate
# every path before copying so a modified manifest cannot escape the stage.
while IFS=' ' read -r checksum listed_path extra; do
    [ -n "$checksum" ] || continue
    [ -z "${extra-}" ] || fail "$EX_DATAERR" "invalid checksum manifest entry: $listed_path $extra"
    case "$listed_path" in
        ./*) relative=${listed_path#./} ;;
        *) fail "$EX_DATAERR" "invalid checksum path: $listed_path" ;;
    esac
    case "/$relative/" in
        */../*|*/./*) fail "$EX_DATAERR" "unsafe checksum path: $listed_path" ;;
    esac
    source=$SOURCE_ROOT/$relative
    [ -f "$source" ] || fail "$EX_DATAERR" "release file is missing: $relative"
    destination=$stage/$relative
    mkdir -p -- "$(dirname -- "$destination")" || fail "$EX_CANTCREAT" "cannot stage: $relative"
    cp -- "$source" "$destination" || fail "$EX_CANTCREAT" "cannot stage: $relative"
done < "$SOURCE_ROOT/RELEASE-MANIFEST.sha256"
cp -- "$SOURCE_ROOT/RELEASE-MANIFEST.sha256" "$stage/RELEASE-MANIFEST.sha256" \
    || fail "$EX_CANTCREAT" "cannot stage checksum manifest"

for command_name in hcu-envcheck hcu-envcheck-doctor hcu-envcheck-verify; do
    [ -f "$stage/bin/$command_name" ] || fail "$EX_DATAERR" "release command is missing: $command_name"
    chmod 755 -- "$stage/bin/$command_name" || fail "$EX_CANTCREAT" "cannot chmod: $command_name"
done
[ ! -f "$stage/hcu-envcheck.sh" ] || chmod 755 -- "$stage/hcu-envcheck.sh"
[ ! -f "$stage/install.sh" ] || chmod 755 -- "$stage/install.sh"
for example in "$stage"/examples/*.sh; do [ ! -f "$example" ] || chmod 755 -- "$example"; done

if [ -e "$target" ] || [ -L "$target" ]; then
    [ "$force" -eq 1 ] || fail "$EX_CANTCREAT" "already installed: $target (use --force to replace)"
fi
for command_name in hcu-envcheck hcu-envcheck-doctor hcu-envcheck-verify; do
    link_path=$bin_dir/$command_name
    if [ -e "$link_path" ] || [ -L "$link_path" ]; then
        [ "$force" -eq 1 ] || fail "$EX_CANTCREAT" "command already exists: $link_path (use --force)"
        [ ! -d "$link_path" ] || [ -L "$link_path" ] \
            || fail "$EX_CANTCREAT" "refusing to replace command directory: $link_path"
    fi
done

# Prepare all command links before touching the active target. Renaming each
# prepared symlink into place later is atomic on the prefix filesystem.
for command_name in hcu-envcheck hcu-envcheck-doctor hcu-envcheck-verify; do
    temp_link=$bin_dir/.${command_name}.new.$$
    ln -s -- "../lib/hcu-envcheck-$version/bin/$command_name" "$temp_link" \
        || fail "$EX_CANTCREAT" "cannot prepare command link: $temp_link"
done

mkdir -- "$transaction" || fail "$EX_CANTCREAT" "cannot create install transaction directory"
transaction_active=1
for command_name in hcu-envcheck hcu-envcheck-doctor hcu-envcheck-verify; do
    link_path=$bin_dir/$command_name
    if [ -e "$link_path" ] || [ -L "$link_path" ]; then
        mv -- "$link_path" "$transaction/$command_name.old" \
            || fail "$EX_CANTCREAT" "cannot preserve command link: $link_path"
    else
        : > "$transaction/$command_name.absent" \
            || fail "$EX_CANTCREAT" "cannot record absent command: $link_path"
    fi
done

if [ -e "$target" ] || [ -L "$target" ]; then
    mv -- "$target" "$backup" || fail "$EX_CANTCREAT" "cannot preserve previous installation: $target"
else
    : > "$transaction/target.absent" \
        || fail "$EX_CANTCREAT" "cannot record absent installation target"
fi
mv -- "$stage" "$target" || fail "$EX_CANTCREAT" "cannot activate installation: $target"
for command_name in hcu-envcheck hcu-envcheck-doctor hcu-envcheck-verify; do
    temp_link=$bin_dir/.${command_name}.new.$$
    mv -- "$temp_link" "$bin_dir/$command_name" \
        || fail "$EX_CANTCREAT" "cannot activate command link: $bin_dir/$command_name"
done
install_complete=1
transaction_active=0
[ ! -e "$backup" ] || rm -rf -- "$backup" 2>/dev/null \
    || printf 'install.sh: warning: previous installation backup remains at %s\n' "$backup" >&2
[ ! -e "$transaction" ] || rm -rf -- "$transaction" 2>/dev/null \
    || printf 'install.sh: warning: transaction metadata remains at %s\n' "$transaction" >&2
trap - EXIT HUP INT TERM
rm -f -- "$lock/pid" 2>/dev/null || true
rmdir -- "$lock" 2>/dev/null || true

printf 'Installed hcu-envcheck %s\n' "$version"
printf 'Commands: %s/{hcu-envcheck,hcu-envcheck-doctor,hcu-envcheck-verify}\n' "$bin_dir"
case :$PATH: in *:"$bin_dir":*) ;; *) printf 'Add this directory to PATH: %s\n' "$bin_dir" ;; esac

#!/bin/sh
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Convenience entry point for running directly from an extracted release.
set -u

resolve_script_dir() {
    target=$1
    while [ -h "$target" ]; do
        target_dir=$(CDPATH= cd -P -- "$(dirname -- "$target")" 2>/dev/null && pwd) || return 1
        link=$(readlink "$target") || return 1
        case "$link" in /*) target=$link ;; *) target=$target_dir/$link ;; esac
    done
    CDPATH= cd -P -- "$(dirname -- "$target")" 2>/dev/null && pwd
}

ROOT=$(resolve_script_dir "$0") || {
    printf 'hcu-envcheck.sh: cannot resolve release directory\n' >&2
    exit 70
}
exec "$ROOT/bin/hcu-envcheck" "$@"

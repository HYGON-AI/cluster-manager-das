#!/bin/sh
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# SSH fallback when clush/Slurm node expansion is unavailable.
set -eu
: "${NODES_FILE:?set NODES_FILE to one hostname or address per line}"
EXPECTED_DEVICES=${EXPECTED_DEVICES:-8}
SOFTWARE_MODE=${SOFTWARE_MODE:-host-python}
REMOTE_PYTHON=${REMOTE_PYTHON:-python3}
OUTPUT_DIR=${OUTPUT_DIR:-"$PWD/hcu-envcheck-$(date +%Y%m%d-%H%M%S)-$$"}
[ ! -e "$OUTPUT_DIR" ] || { printf 'refusing to reuse output directory: %s\n' "$OUTPUT_DIR" >&2; exit 73; }
if command -v hcu-envcheck >/dev/null 2>&1; then
    TOOL=hcu-envcheck
else
    EXAMPLE_DIR=$(CDPATH= cd -P -- "$(dirname -- "$0")" && pwd)
    TOOL=$EXAMPLE_DIR/../bin/hcu-envcheck
fi
set -- baremetal-cluster --nodes-file "$NODES_FILE" --transport ssh \
    --concurrency "${CONCURRENCY:-32}" --expected-devices "$EXPECTED_DEVICES" \
    --software-mode "$SOFTWARE_MODE" --remote-python "$REMOTE_PYTHON" \
    --output-dir "$OUTPUT_DIR"
case "$SOFTWARE_MODE" in
    host-python) ;;
    conda)
        : "${CONDA_PREFIX:?set CONDA_PREFIX to the absolute training environment path}"
        : "${CONDA_STORAGE:?set CONDA_STORAGE to shared or node-local}"
        set -- "$@" --conda-prefix "$CONDA_PREFIX" --conda-storage "$CONDA_STORAGE"
        ;;
    docker)
        : "${DOCKER_IMAGE:?set DOCKER_IMAGE to the exact training image}"
        set -- "$@" --docker-image "$DOCKER_IMAGE" \
            --container-python "${CONTAINER_PYTHON:-python3}"
        ;;
    *) printf 'SOFTWARE_MODE must be host-python, conda or docker\n' >&2; exit 64 ;;
esac
[ -z "${SSH_USER-}" ] || set -- "$@" --ssh-user "$SSH_USER"
[ -z "${SSH_CONFIG_FILE-}" ] || set -- "$@" --ssh-config-file "$SSH_CONFIG_FILE"
[ -z "${IDENTITY_FILE-}" ] || set -- "$@" --identity-file "$IDENTITY_FILE"
"$TOOL" "$@"
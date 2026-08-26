#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

IMAGE="${1:-hygon/ft-controller:latest}"
DEFAULT_TAR_NAME="$(printf '%s' "$IMAGE" | tr '/:' '--').tar"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAR="${2:-${IMAGE_TAR:-$ROOT_DIR/dist/$DEFAULT_TAR_NAME}}"
TARGET_NODES="${TARGET_NODES:-}"
NODE_SELECTOR="${NODE_SELECTOR:-}"
SSH_OPTS="${SSH_OPTS:-}"
DOCKER_BIN="${DOCKER_BIN:-docker}"

fail() {
  echo "[load-image] ERROR: $*" >&2
  exit 1
}

command -v kubectl >/dev/null 2>&1 || fail "kubectl not found"
command -v ssh >/dev/null 2>&1 || fail "ssh not found"
[[ -f "$IMAGE_TAR" ]] || fail "image tar not found: $IMAGE_TAR"

if [[ -z "$TARGET_NODES" ]]; then
  if [[ -n "$NODE_SELECTOR" ]]; then
    TARGET_NODES="$(kubectl get nodes -l "$NODE_SELECTOR" -o name | sed 's|^node/||')"
  else
    TARGET_NODES="$(kubectl get nodes -o name | sed 's|^node/||')"
  fi
fi

[[ -n "$TARGET_NODES" ]] || fail "no target nodes found"

echo "[load-image] image: $IMAGE"
echo "[load-image] tar: $IMAGE_TAR"
echo "[load-image] target nodes:"
printf '  %s\n' $TARGET_NODES

for node in $TARGET_NODES; do
  echo "[load-image] loading image on $node"
  ssh $SSH_OPTS "$node" "PATH=/usr/bin:/bin:/usr/local/bin $DOCKER_BIN load -i '$IMAGE_TAR'"
done

echo "[load-image] verifying image on target nodes"
for node in $TARGET_NODES; do
  echo "[$node]"
  ssh $SSH_OPTS "$node" \
    "PATH=/usr/bin:/bin:/usr/local/bin $DOCKER_BIN images '$IMAGE' --format '{{.Repository}}:{{.Tag}} {{.ID}} {{.CreatedSince}}'"
done


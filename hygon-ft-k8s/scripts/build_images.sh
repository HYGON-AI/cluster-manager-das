#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPOSITORY_ROOT="$(cd "$MODULE_DIR/.." && pwd)"
IMAGE="${1:-hygon/ft-controller:latest}"
DEFAULT_TAR_NAME="$(printf '%s' "$IMAGE" | tr '/:' '--').tar"
IMAGE_TAR="${2:-${IMAGE_TAR:-$MODULE_DIR/dist/$DEFAULT_TAR_NAME}}"

docker build \
    -f "$MODULE_DIR/packaging/docker/Dockerfile" \
    -t "$IMAGE" \
    "$REPOSITORY_ROOT"

echo "Built image: $IMAGE"
mkdir -p "$(dirname "$IMAGE_TAR")"
docker save "$IMAGE" -o "$IMAGE_TAR"
echo "Saved image archive: $IMAGE_TAR"
echo "Load it on another node with: docker load -i $IMAGE_TAR"
echo "Load it on all Kubernetes nodes with: bash scripts/load_image_to_nodes.sh $IMAGE $IMAGE_TAR"
echo "If your Kubernetes nodes cannot see the local Docker image, push it to your registry and update manifests."

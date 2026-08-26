#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

NAMESPACE="${NAMESPACE:-hygon-ft}"
POD="${1:-}"

if [[ -z "$POD" ]]; then
  POD="$(kubectl -n "$NAMESPACE" get pods -l app=nodehealth-agent -o jsonpath='{.items[0].metadata.name}')"
fi

kubectl -n "$NAMESPACE" exec "$POD" -- bash -lc 'rm -f /host/tmp/hygon-ft-nhc-fail'

NODE="$(kubectl -n "$NAMESPACE" get pod "$POD" -o jsonpath='{.spec.nodeName}')"
kubectl taint node "$NODE" ft.hygon.io/node-unhealthy- || true

echo "Cleared simulated NHC failure and removed taint from node: $NODE"


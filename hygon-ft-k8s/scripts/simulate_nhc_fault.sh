#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

NAMESPACE="${NAMESPACE:-hygon-ft}"
POD="${1:-}"

if [[ -z "$POD" ]]; then
  POD="$(kubectl -n "$NAMESPACE" get pods -l app=nodehealth-agent -o jsonpath='{.items[0].metadata.name}')"
fi

kubectl -n "$NAMESPACE" exec "$POD" -- bash -lc 'echo fail > /host/tmp/hygon-ft-nhc-fail'

echo "Injected simulated NHC failure through nodehealth pod: $POD"
echo "Next checks:"
echo "kubectl -n $NAMESPACE logs ds/nodehealth-agent --tail=100 -f"
echo "kubectl -n $NAMESPACE get faultevents -w"
echo "kubectl get pods -A -l ft.hygon.io/enabled=true -o wide"


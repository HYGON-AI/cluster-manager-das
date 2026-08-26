#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

NODE="${1:?usage: simulate_node_notready_fault.sh <node-name>}"

kubectl patch node "$NODE" --type=merge -p '{"status":{"conditions":[{"type":"Ready","status":"Unknown","reason":"ManualSimulation","message":"manual NodeNotReady simulation"}]}}' || true

echo "If your apiserver blocks status patching from kubectl, simulate by stopping kubelet on a test node instead."
echo "Watch:"
echo "kubectl -n hygon-ft get faultevents -w"
echo "kubectl -n hygon-ft logs deploy/ft-operator -f"


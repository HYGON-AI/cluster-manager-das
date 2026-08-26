#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Edit these two variables before running.
# System nodes run ft-operator and ft-webhook.
# Training nodes run nodehealth-agent and training Pods.
SYSTEM_NODES="${SYSTEM_NODES:-system-node-1 system-node-2}"
TRAINING_NODES="${TRAINING_NODES:-train-node-1 train-node-2 train-node-3 train-node-4 train-node-5 train-node-6 train-node-7 train-node-8}"
APPLY_SYSTEM_TAINT="${APPLY_SYSTEM_TAINT:-true}"

for node in $SYSTEM_NODES; do
  kubectl label node "$node" node-role.hygon.io/system=true --overwrite
  if [[ "$APPLY_SYSTEM_TAINT" == "true" ]]; then
    kubectl taint node "$node" node-role.hygon.io/system=true:NoSchedule --overwrite 2>/dev/null \
      || kubectl taint node "$node" node-role.hygon.io/system=true:NoSchedule
  fi
done

for node in $TRAINING_NODES; do
  kubectl label node "$node" node-role.hygon.io/training=true --overwrite
  kubectl label node "$node" accelerator.hygon.io/enabled=true --overwrite
done

echo "System nodes:"
kubectl get nodes -l node-role.hygon.io/system=true -o wide

echo "Training nodes:"
kubectl get nodes -l node-role.hygon.io/training=true -o wide

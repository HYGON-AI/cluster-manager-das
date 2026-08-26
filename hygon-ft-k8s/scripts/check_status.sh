#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

FT_LOG_DIR="${FT_LOG_DIR:-}"

kubectl -n hygon-ft get pods -o wide
kubectl -n hygon-ft get lease ft-operator-leader -o yaml || true
kubectl -n hygon-ft get faultevents
kubectl get nodes -L node-role.hygon.io/system,node-role.hygon.io/training,accelerator.hygon.io/enabled -o wide
kubectl get nodes -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints
kubectl get pods -A -l ft.hygon.io/enabled=true -o wide

if kubectl get crd jobs.batch.volcano.sh >/dev/null 2>&1; then
  kubectl get jobs.batch.volcano.sh -A
fi

if [[ -n "$FT_LOG_DIR" ]]; then
  echo "Persistent training and launcher logs: $FT_LOG_DIR"
  if [[ -d "$FT_LOG_DIR" ]]; then
    find "$FT_LOG_DIR" -maxdepth 1 -type f -printf '%TY-%Tm-%Td %TH:%TM:%TS %10s %f\n' | sort
  else
    echo "Log directory does not exist yet."
  fi
fi
